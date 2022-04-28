// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::cmp;
use std::io::Cursor;
use std::marker::Send;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::{try_join_all, FutureExt};
use futures::stream::unfold;
use http::status::StatusCode;
use lazy_static::lazy_static;
use rand::{rngs::OsRng, Rng};
use rusoto_core::credential::DefaultCredentialsProvider;
use rusoto_core::request::{DispatchSignedRequest, DispatchSignedRequestFuture};
use rusoto_core::{region::Region, ByteStream, HttpClient, HttpDispatchError, RusotoError};
use rusoto_s3::{
    AbortMultipartUploadRequest, CompleteMultipartUploadRequest, CompletedMultipartUpload, CompletedPart,
    CreateMultipartUploadRequest, GetObjectError, GetObjectRequest, HeadObjectError, HeadObjectRequest,
    PutObjectRequest, S3Client, UploadPartRequest, S3,
};
use rusoto_signature::signature::SignedRequest;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tokio_util::io::ReaderStream;

use buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use common::{log, DigestInfo, JoinHandleDropGuard};
use config;
use error::{error_if, make_err, make_input_err, Code, Error, ResultExt};
use retry::{ExponentialBackoff, Retrier, RetryResult};
use traits::{StoreTrait, UploadSizeInfo};

// S3 parts cannot be smaller than this number. See:
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
const MIN_MULTIPART_SIZE: usize = 5 * 1024 * 1024; // 5mb.

// Size for the large vector pool if not specified.
// NOTE: If this value is changed also change comment in backends.rs.
const DEFAULT_ADDITIONAL_MAX_CONCURRENT_REQUESTS: usize = 20;

struct InnerThrottledDispatcher {
    client: HttpClient,
    active_request_semaphore: Semaphore,
}

struct ThrottledDispatcher {
    inner: Arc<InnerThrottledDispatcher>,
}

impl ThrottledDispatcher {
    fn new() -> Self {
        Self {
            inner: Arc::new(InnerThrottledDispatcher {
                client: HttpClient::new().expect("failed to create request dispatcher"),
                active_request_semaphore: Semaphore::new(0), // We will fill this later.
            }),
        }
    }

    fn add_permits(&self, count: usize) {
        self.inner.active_request_semaphore.add_permits(count);
    }
}

impl DispatchSignedRequest for ThrottledDispatcher {
    fn dispatch(&self, request: SignedRequest, timeout: Option<Duration>) -> DispatchSignedRequestFuture {
        let inner = self.inner.clone();
        Box::pin(async move {
            // This will limit the number of concurrent requests permitted at once.
            let _permit = inner
                .active_request_semaphore
                .acquire()
                .await
                .map_err(|_| HttpDispatchError::new("Semaphore closed".into()))?;
            inner.client.dispatch(request, timeout).await
        })
    }
}

lazy_static! {
    static ref SHARED_CLIENT: Mutex<Arc<ThrottledDispatcher>> = Mutex::new(Arc::new(ThrottledDispatcher::new()));
}

fn should_retry<T, E>(result: Result<T, RusotoError<E>>) -> RetryResult<T>
where
    RusotoError<E>: std::fmt::Display,
{
    match result {
        // Object found in S3.
        Ok(v) => RetryResult::Ok(v), // Success

        // Timeout-like errors. This can retry.
        Err(RusotoError::HttpDispatch(e)) => RetryResult::Retry(make_err!(Code::Unavailable, "{}", e.to_string())),

        // HTTP-level errors. Sometimes can retry.
        Err(RusotoError::Unknown(e)) => match e.status {
            StatusCode::NOT_FOUND => RetryResult::Err(make_err!(Code::NotFound, "{}", e.status.to_string())),
            StatusCode::INTERNAL_SERVER_ERROR => {
                RetryResult::Retry(make_err!(Code::Unavailable, "{}", e.status.to_string()))
            }
            StatusCode::SERVICE_UNAVAILABLE => {
                RetryResult::Retry(make_err!(Code::Unavailable, "{}", e.status.to_string()))
            }
            StatusCode::CONFLICT => RetryResult::Retry(make_err!(Code::Unavailable, "{}", e.status.to_string())),
            other_err => RetryResult::Err(make_err!(Code::Unavailable, "{}", other_err.to_string())),
        },

        // Other errors (eg. Validation, Parse, Credentials). Never retry.
        Err(other_err) => RetryResult::Err(make_err!(Code::Unavailable, "{}", other_err.to_string())),
    }
}

pub struct S3Store {
    s3_client: Arc<S3Client>,
    bucket: String,
    key_prefix: String,
    jitter_fn: Box<dyn Fn(Duration) -> Duration + Send + Sync>,
    retry: config::backends::Retry,
    retrier: Retrier,
}

impl S3Store {
    pub fn new(config: &config::backends::S3Store) -> Result<Self, Error> {
        let mut additional_max_concurrent_requests = config.additional_max_concurrent_requests;
        if additional_max_concurrent_requests == 0 {
            additional_max_concurrent_requests = DEFAULT_ADDITIONAL_MAX_CONCURRENT_REQUESTS;
        }
        let s3_client = {
            let dispatcher = SHARED_CLIENT.lock().unwrap().clone();
            dispatcher.add_permits(additional_max_concurrent_requests);
            let credentials_provider =
                DefaultCredentialsProvider::new().expect("failed to create credentials provider");
            let region = config
                .region
                .parse::<Region>()
                .map_err(|e| make_input_err!("{}", e.to_string()))?;
            S3Client::new_with(dispatcher, credentials_provider, region)
        };
        let jitter_amt = config.retry.jitter;
        S3Store::new_with_client_and_jitter(
            config,
            s3_client,
            Box::new(move |delay: Duration| {
                if jitter_amt == 0. {
                    return delay;
                }
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
            s3_client: Arc::new(s3_client),
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
    async fn has(self: Pin<&Self>, digest: DigestInfo) -> Result<Option<usize>, Error> {
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

                    let result = self.s3_client.head_object(head_req).await;
                    if let Err(RusotoError::Service(HeadObjectError::NoSuchKey(_))) = &result {
                        return Some((RetryResult::Ok(None), state));
                    }

                    match should_retry(result) {
                        RetryResult::Ok(result) => {
                            if let Some(sz) = result.content_length {
                                return Some((RetryResult::Ok(Some(sz as usize)), state));
                            }
                            Some((
                                RetryResult::Err(make_input_err!("Expected size to exist in s3 store has")),
                                state,
                            ))
                        }
                        RetryResult::Err(err) => {
                            if err.code == Code::NotFound {
                                return Some((RetryResult::Ok(None), state));
                            }
                            Some((RetryResult::Err(err), state))
                        }
                        RetryResult::Retry(err) => Some((
                            RetryResult::Retry(make_err!(
                                Code::Unavailable,
                                "Error attempting to load s3 result, retried {} times. Error: {}",
                                self.retry.max_retries + 1,
                                err,
                            )),
                            state,
                        )),
                    }
                }),
            )
            .await
    }

    async fn update(
        self: Pin<&Self>,
        digest: DigestInfo,
        mut reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        let s3_path = &self.make_s3_path(&digest);

        let max_size = match upload_size {
            UploadSizeInfo::ExactSize(sz) => sz,
            UploadSizeInfo::MaxSize(sz) => sz,
        };
        // NOTE(blaise.bruer) It might be more optimal to use a different heuristic here, but for
        // simplicity we use a hard codded value. Anything going down this if-statement will have
        // the advantage of only 1 network request for the upload instead of minimum of 3 required
        // for multipart upload requests.
        if max_size < MIN_MULTIPART_SIZE {
            let (body, content_length) = if let UploadSizeInfo::ExactSize(sz) = upload_size {
                reader.set_close_after_size(sz as u64);
                (Some(ByteStream::new(reader)), Some(sz as i64))
            } else {
                let write_buf = reader
                    .take(max_size + 1) // Just in case, we want to capture the EOF, so +1.
                    .await
                    .err_tip(|| "Failed to read file in upload to s3 in single chunk")?;
                error_if!(
                    write_buf.len() > max_size,
                    "More data than provided max_size in s3_store {}",
                    digest.str()
                );
                let content_length = write_buf.len();
                (
                    Some(ByteStream::new(ReaderStream::new(Cursor::new(write_buf)))),
                    Some(content_length as i64),
                )
            };

            let put_object_request = PutObjectRequest {
                bucket: self.bucket.to_owned(),
                key: s3_path.to_owned(),
                content_length,
                body,
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
        let bytes_per_upload_part = cmp::max(MIN_MULTIPART_SIZE, max_size / (MIN_MULTIPART_SIZE - 1));

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
            let mut part_number: i64 = 1;

            // We might end up with +1 capacity units than needed, but that is the worst case.
            let mut completed_part_futs = Vec::with_capacity((max_size / bytes_per_upload_part) + 1);
            loop {
                // TODO(blaise.bruer) We should limit the number of bytes or requests active
                // at a time here. Otherwise a client that has very high upload speed but
                // s3 has slow download speeds, we might end up using an insane amount of ram.
                let write_buf = reader
                    .take(bytes_per_upload_part as usize)
                    .await
                    .err_tip(|| "Failed to read chunk in s3_store")?;
                if write_buf.len() == 0 {
                    break; // Reached EOF.
                }

                let write_buf_len = write_buf.len() as i64;
                let body = Some(ByteStream::new(ReaderStream::new(Cursor::new(write_buf))));

                let request = UploadPartRequest {
                    bucket: self.bucket.to_owned(),
                    key: s3_path.to_owned(),
                    content_length: Some(write_buf_len),
                    body,
                    part_number,
                    upload_id: upload_id.clone(),
                    ..Default::default()
                };

                let s3_client = self.s3_client.clone();
                completed_part_futs.push(
                    JoinHandleDropGuard::new(tokio::spawn(async move {
                        let part_number = request.part_number;
                        let mut response = s3_client
                            .upload_part(request)
                            .await
                            .map_err(|e| make_err!(Code::Unknown, "Failed to upload part: {:?}", e))?;
                        let e_tag = response.e_tag.take();
                        Result::<CompletedPart, Error>::Ok(CompletedPart {
                            e_tag,
                            part_number: Some(part_number),
                        })
                    }))
                    .map(|r| match r.err_tip(|| "Failed to run s3 upload spawn") {
                        Ok(r2) => r2.err_tip(|| "S3 upload chunk failure"),
                        Err(e) => Err(e),
                    }),
                );
                part_number += 1;
            }

            let completed_parts = try_join_all(completed_part_futs).await?;
            self.s3_client
                .complete_multipart_upload(CompleteMultipartUploadRequest {
                    bucket: self.bucket.to_owned(),
                    key: s3_path.to_owned(),
                    upload_id: upload_id.clone(),
                    multipart_upload: Some(CompletedMultipartUpload {
                        parts: Some(completed_parts),
                    }),
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
    }

    async fn get_part(
        self: Pin<&Self>,
        digest: DigestInfo,
        writer: DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> Result<(), Error> {
        let s3_path = &self.make_s3_path(&digest);
        let end_read_byte = length
            .map_or(Some(None), |length| Some(offset.checked_add(length)))
            .err_tip(|| "Integer overflow protection triggered")?;

        let retry_config = ExponentialBackoff::new(Duration::from_millis(self.retry.delay as u64))
            .map(|d| (self.jitter_fn)(d))
            .take(self.retry.max_retries); // Remember this is number of retries, so will run max_retries + 1.

        self.retrier
            .retry(
                retry_config,
                unfold(writer, move |mut writer| async move {
                    let result = self
                        .s3_client
                        .get_object(GetObjectRequest {
                            bucket: self.bucket.to_owned(),
                            key: s3_path.to_owned(),
                            range: Some(format!(
                                "bytes={}-{}",
                                offset + writer.get_bytes_written() as usize,
                                end_read_byte.map_or_else(|| "".to_string(), |v| v.to_string())
                            )),
                            ..Default::default()
                        })
                        .await;

                    if let Err(RusotoError::Service(GetObjectError::NoSuchKey(err))) = &result {
                        return Some((
                            RetryResult::Err(make_err!(Code::NotFound, "File not found in S3: {:?}", err)),
                            writer,
                        ));
                    }

                    let s3_in_stream = match should_retry(result) {
                        RetryResult::Ok(get_object_output) => {
                            let body_result = get_object_output
                                .body
                                .err_tip(|| "Expected body to be set in s3 get request");
                            match body_result {
                                Ok(s3_in_stream) => s3_in_stream,
                                Err(err) => {
                                    return Some((
                                        RetryResult::Err(make_err!(
                                            Code::Unavailable,
                                            "Error attempting to get s3 result. This is not a retryable error: {}",
                                            err
                                        )),
                                        writer,
                                    ));
                                }
                            }
                        }
                        RetryResult::Err(err) => {
                            return Some((RetryResult::Err(err), writer));
                        }
                        RetryResult::Retry(err) => {
                            return Some((
                                RetryResult::Retry(make_err!(
                                    Code::Unavailable,
                                    "Error attempting to get s3 result, retried {} times. Error: {}",
                                    self.retry.max_retries + 1,
                                    err,
                                )),
                                writer,
                            ));
                        }
                    };

                    // Copy data from s3 input stream to the writer stream.
                    let result = writer
                        .forward(s3_in_stream, true /* Forward EOF */)
                        .await
                        .err_tip(|| "Failed to forward data in s3_store");
                    if let Err(e) = result {
                        // Prevent retry if pipe is gone.
                        if writer.is_pipe_broken() {
                            return Some((
                                RetryResult::Err(make_input_err!(
                                    "Output channel is shutdown. Nowhere to send data in s3_store"
                                )),
                                writer,
                            ));
                        }
                        // This looks like maybe S3 closed our connection, so we will retry.
                        return Some((RetryResult::Retry(e), writer));
                    }

                    Some((RetryResult::Ok(()), writer))
                }),
            )
            .await
    }

    fn as_any(self: Arc<Self>) -> Box<dyn std::any::Any> {
        Box::new(self)
    }
}
