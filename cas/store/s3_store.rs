// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::cmp;
use std::marker::Send;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use fast_async_mutex::mutex::Mutex;
use futures::stream::unfold;
use http::status::StatusCode;
use rand::{rngs::OsRng, Rng};
use rusoto_core::{region::Region, ByteStream, RusotoError};
use rusoto_s3::{
    AbortMultipartUploadRequest, CompleteMultipartUploadRequest, CreateMultipartUploadRequest, GetObjectRequest,
    HeadObjectError, HeadObjectRequest, PutObjectRequest, S3Client, UploadPartRequest, S3,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::time::sleep;
use tokio_util::io::ReaderStream;

use common::{log, DigestInfo};
use config;
use error::{make_err, make_input_err, Code, Error, ResultExt};
use retry::{ExponentialBackoff, Retrier, RetryResult};
use traits::{ResultFuture, StoreTrait};

use async_read_taker::AsyncReadTaker;

// S3 parts cannot be smaller than this number. See:
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
const MIN_MULTIPART_SIZE: i64 = 5_000_000; // 5mb.

pub struct S3Store {
    s3_client: S3Client,
    bucket: String,
    key_prefix: String,
    jitter_fn: Box<dyn Fn(Duration) -> Duration + Send + Sync>,
    retry: config::backends::Retry,
    retrier: Retrier,
}

impl S3Store {
    pub fn new(config: &config::backends::S3Store) -> Result<Self, Error> {
        let jitter_amt = config.retry.jitter;
        S3Store::new_with_client_and_jitter(
            config,
            S3Client::new(
                config
                    .region
                    .parse::<Region>()
                    .map_err(|e| make_input_err!("{}", e.to_string()))?,
            ),
            Box::new(move |delay: Duration| {
                let min = 1. - (jitter_amt / 2.);
                let max = 1. + (jitter_amt / 2.);
                delay.mul_f32(OsRng.gen_range(min..max))
            }),
        )
    }

    pub fn new_with_client_and_jitter(
        config: &config::backends::S3Store,
        s3_client: S3Client,
        jitter_fn: Box<dyn Fn(Duration) -> Duration + Send + Sync>,
    ) -> Result<Self, Error> {
        Ok(S3Store {
            s3_client: s3_client,
            bucket: config.bucket.to_owned(),
            key_prefix: config.key_prefix.as_ref().unwrap_or(&"".to_string()).to_owned(),
            jitter_fn: jitter_fn,
            retry: config.retry.to_owned(),
            retrier: Retrier::new(Box::new(|duration| Box::pin(sleep(duration)))),
        })
    }

    fn make_s3_path(&self, digest: &DigestInfo) -> String {
        format!("{}{}-{}", self.key_prefix, digest.str(), digest.size_bytes)
    }
}

#[async_trait]
impl StoreTrait for S3Store {
    fn has<'a>(self: Pin<&'a Self>, digest: DigestInfo) -> ResultFuture<'a, bool> {
        Box::pin(async move {
            let retry_config = ExponentialBackoff::new(Duration::from_millis(self.retry.delay as u64))
                .map(|d| (self.jitter_fn)(d))
                .take(self.retry.max_retries); // Remember this is number of retries, so will run max_retries + 1.
            let s3_path = &self.make_s3_path(&digest);
            self.retrier
                .retry(
                    retry_config,
                    unfold((), move |state| async move {
                        let head_req = HeadObjectRequest {
                            bucket: self.bucket.to_owned(),
                            key: s3_path.to_owned(),
                            ..Default::default()
                        };

                        let r = self.s3_client.head_object(head_req).await;
                        let (should_retry, err) = match r {
                            // Object found in S3.
                            Ok(_) => return Some((RetryResult::Ok(true), state)),

                            // Object not found in S3.
                            Err(RusotoError::Service(HeadObjectError::NoSuchKey(_))) => {
                                return Some((RetryResult::Ok(false), state))
                            }

                            // Timeout-like errors. This can retry.
                            Err(RusotoError::HttpDispatch(e)) => (true, e.to_string()),

                            // HTTP-level errors. Sometimes can retry.
                            Err(RusotoError::Unknown(e)) => match e.status {
                                StatusCode::NOT_FOUND => return Some((RetryResult::Ok(false), state)),
                                StatusCode::INTERNAL_SERVER_ERROR => (true, e.status.to_string()),
                                StatusCode::SERVICE_UNAVAILABLE => (true, e.status.to_string()),
                                StatusCode::CONFLICT => (true, e.status.to_string()),
                                other_err => (false, other_err.to_string()),
                            },

                            // Other errors (eg. Validation, Parse, Credentials). Never retry.
                            Err(other_err) => (false, other_err.to_string()),
                        };

                        if should_retry {
                            return Some((
                                RetryResult::Retry(make_err!(
                                    Code::Unavailable,
                                    "Error attempting to load s3 result, retried {} times. Error: {}",
                                    self.retry.max_retries + 1,
                                    err,
                                )),
                                state,
                            ));
                        }
                        Some((
                            RetryResult::Err(make_err!(
                                Code::Unavailable,
                                "Error attempting to load s3 result. This is not a retryable error: {}",
                                err
                            )),
                            state,
                        ))
                    }),
                )
                .await
        })
    }

    fn update<'a>(
        self: Pin<&'a Self>,
        digest: DigestInfo,
        reader: Box<dyn AsyncRead + Send + Unpin + Sync + 'static>,
    ) -> ResultFuture<'a, ()> {
        Box::pin(async move {
            let s3_path = &self.make_s3_path(&digest);

            // We make a major assumption here... If we cannot trust the size we assume it's an action cache.
            // in this case it is extremely unlikely the payload will be greater than 5gb. If it is a CAS item
            // it should be `trust_size = true` which we can upload in one chunk if small enough (and more efficient)
            // but only if the size is small enough. We could use MAX_UPLOAD_PART_SIZE (5gb), but I think it's fine
            // to use 5mb as a limit too.
            if !digest.trust_size || digest.size_bytes < MIN_MULTIPART_SIZE {
                let put_object_request = PutObjectRequest {
                    bucket: self.bucket.to_owned(),
                    key: s3_path.to_owned(),
                    content_length: if digest.trust_size {
                        Some(digest.size_bytes)
                    } else {
                        None
                    },
                    body: Some(ByteStream::new(ReaderStream::new(reader))),
                    ..Default::default()
                };
                return self
                    .s3_client
                    .put_object(put_object_request)
                    .await
                    .map_or_else(|e| Err(make_err!(Code::Unknown, "{:?}", e)), |_| Ok(()))
                    .err_tip(|| "Failed to upload file to s3 in single chunk");
            }

            // S3 requires us to upload in parts if the size is greater than 5GB. The part size must be at least
            // 5mb and can have up to 10,000 parts.
            let bytes_per_upload_part =
                cmp::max(MIN_MULTIPART_SIZE, digest.size_bytes / (MIN_MULTIPART_SIZE - 1)) as usize;

            let response = self
                .s3_client
                .create_multipart_upload(CreateMultipartUploadRequest {
                    bucket: self.bucket.to_owned(),
                    key: s3_path.to_owned(),
                    ..Default::default()
                })
                .await
                .map_err(|e| make_err!(Code::Unknown, "Failed to create multipart upload to s3: {:?}", e))?;

            let upload_id = response
                .upload_id
                .err_tip(|| "Expected upload_id to be set by s3 response")?;

            let complete_result = {
                let mut part_number = 1;

                let reader = Arc::new(Mutex::new(reader));
                let is_done = Arc::new(AtomicBool::new(false));
                loop {
                    let is_done_clone = is_done.clone();
                    // Wrap `AsyncRead` so we can hold a copy of it in this scope between iterations.
                    // This is quite difficult because we need to give full ownership of an AsyncRead
                    // to `ByteStream` which has an unknown lifetime.
                    // This wrapper will also ensure we only send `bytes_per_upload_part` then close the
                    // stream.
                    let taker = AsyncReadTaker::new(
                        reader.clone(),
                        Some(move || is_done_clone.store(true, Ordering::Relaxed)),
                        bytes_per_upload_part,
                    );
                    {
                        let body = Some(ByteStream::new(ReaderStream::new(taker)));
                        self.s3_client
                            .upload_part(UploadPartRequest {
                                bucket: self.bucket.to_owned(),
                                key: s3_path.to_owned(),
                                body,
                                part_number,
                                upload_id: upload_id.clone(),
                                ..Default::default()
                            })
                            .await
                            .map_err(|e| make_err!(Code::Unknown, "Failed to upload part: {:?}", e))?;
                    }
                    if is_done.load(Ordering::Relaxed) {
                        break;
                    }
                    part_number += 1;
                }

                self.s3_client
                    .complete_multipart_upload(CompleteMultipartUploadRequest {
                        bucket: self.bucket.to_owned(),
                        key: s3_path.to_owned(),
                        upload_id: upload_id.clone(),
                        ..Default::default()
                    })
                    .await
                    .map_or_else(|e| Err(make_err!(Code::Unknown, "{:?}", e)), |_| Ok(()))
                    .err_tip(|| "Failed to complete multipart to s3")?;
                Ok(())
            };
            if complete_result.is_err() {
                let abort_result = self
                    .s3_client
                    .abort_multipart_upload(AbortMultipartUploadRequest {
                        bucket: self.bucket.to_owned(),
                        key: s3_path.to_owned(),
                        upload_id: upload_id.clone(),
                        ..Default::default()
                    })
                    .await;
                if let Err(err) = abort_result {
                    log::info!("\x1b[0;31ms3_store\x1b[0m: Failed to abort_multipart_upload: {:?}", err);
                }
            }
            complete_result
        })
    }

    fn get_part<'a>(
        self: Pin<&'a Self>,
        digest: DigestInfo,
        mut writer: &'a mut (dyn AsyncWrite + Send + Unpin + Sync),
        offset: usize,
        length: Option<usize>,
    ) -> ResultFuture<'a, ()> {
        Box::pin(async move {
            let s3_path = &self.make_s3_path(&digest);
            let end_read_byte = length
                .map_or(Some(None), |length| Some(offset.checked_add(length)))
                .err_tip(|| "Integer overflow protection triggered")?;
            let get_req = GetObjectRequest {
                bucket: self.bucket.to_owned(),
                key: s3_path.to_owned(),
                range: Some(format!(
                    "bytes={}-{}",
                    offset,
                    end_read_byte.map_or_else(|| "".to_string(), |v| v.to_string())
                )),
                ..Default::default()
            };

            let get_object_output = self
                .s3_client
                .get_object(get_req)
                .await
                .map_err(|e| make_err!(Code::Unavailable, "Error uploading to S3: {:?}", e))?;
            let s3_in_stream = get_object_output
                .body
                .err_tip(|| "Expected body to be set in s3 get request")?;
            let mut s3_reader = s3_in_stream.into_async_read();
            // TODO(blaise.bruer) We do get a size return from this function, but it is unlikely
            // we can validate it is the proper size (other than if it was too large). Maybe we
            // could make a debug log or something to print out data in the case it does not match
            // expectations of some kind?
            tokio::io::copy(&mut s3_reader, &mut writer)
                .await
                .err_tip(|| "Failed to download from s3")?;
            let _ = writer.write(&[]).await; // At this point we really only care about if shutdown() fails.
            writer
                .shutdown()
                .await
                .err_tip(|| "Failed to shutdown write stream in S3Store::get_part")?;
            Ok(())
        })
    }
}
