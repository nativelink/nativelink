// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::pin::Pin;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::channel::mpsc;
use futures::executor::block_on;
use futures::future::ready;
use futures::stream::StreamExt;
use futures::FutureExt;
use http::status::StatusCode;
use rusoto_core::{
    request::{HttpDispatchError, HttpResponse},
    signature::{SignedRequest, SignedRequestPayload},
    ByteStream, DispatchSignedRequest, Region,
};
use rusoto_mock::{MockCredentialsProvider, MockRequestDispatcher, MultipleMockRequestDispatcher};
use rusoto_s3::S3Client;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::join;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio_util::io::ReaderStream;

use async_fixed_buffer::AsyncFixedBuf;
use common::DigestInfo;
use config;
use error::Error;
use error::ResultExt;
use s3_store::S3Store;
use traits::StoreTrait;

// Should match constant in s3_store.
const MIN_MULTIPART_SIZE: usize = 5 * 1024 * 1024; // 5mb.

// TODO(blaise.bruer) We should merge this functionality into StreamMockRequestDispatcher.
fn receive_request(sender: mpsc::Sender<(SignedRequest, Vec<u8>)>) -> impl Fn(SignedRequest) {
    move |mut request| {
        let mut sender = sender.clone();
        let mut raw_payload = Vec::new();
        match request.payload.take() {
            Some(SignedRequestPayload::Stream(stream)) => {
                let mut async_reader = stream.into_async_read();
                assert!(block_on(async_reader.read_to_end(&mut raw_payload)).is_ok());
            }
            Some(SignedRequestPayload::Buffer(buffer)) => raw_payload.extend_from_slice(&buffer[..]),
            None => {}
        }
        sender.try_send((request, raw_payload)).expect("Failed to send payload");
    }
}

type RequestWithWriter = (SignedRequest, Box<dyn AsyncWrite + Send + Unpin + Sync + 'static>);

/// Utility dispatcher that allows tests to receive the requests being performed and publish
/// data to the body stream using channels.
struct StreamMockRequestDispatcher {
    writer_sender: UnboundedSender<RequestWithWriter>,
}

impl StreamMockRequestDispatcher {
    pub fn new(writer_sender: UnboundedSender<RequestWithWriter>) -> Self {
        Self { writer_sender }
    }
}

impl DispatchSignedRequest for StreamMockRequestDispatcher {
    fn dispatch(
        &self,
        request: SignedRequest,
        _timeout: Option<Duration>,
    ) -> rusoto_core::request::DispatchSignedRequestFuture {
        let (rx, tx) = AsyncFixedBuf::new(vec![0u8; 1024]).split_into_reader_writer();
        let result = self.writer_sender.send((request, Box::new(tx)));
        if result.is_err() {
            panic!("Failed to send request and tx");
        }

        ready(Ok(HttpResponse {
            status: StatusCode::OK,
            body: ByteStream::new(ReaderStream::new(rx)),
            headers: Default::default(),
        }))
        .boxed()
    }
}

/// Takes a request and extracts out the `range` into the start and end ranges when requesting data.
fn get_range_from_request(request: &SignedRequest) -> Result<(usize, Option<usize>), Error> {
    let range_header = from_utf8(&request.headers["range"][0]).unwrap();
    assert!(
        range_header.starts_with("bytes="),
        "expected range header to start with 'bytes=', got {}",
        range_header
    );
    let (start, end) = range_header["bytes=".len()..]
        .split_once('-')
        .err_tip(|| "Expected '-' to be in 'range' header")?;
    let start = usize::from_str_radix(&start, 10)?;
    let end = if end.len() > 0 {
        Some(usize::from_str_radix(&end, 10)?)
    } else {
        None
    };
    Ok((start, end))
}

#[cfg(test)]
mod s3_store_tests {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    const BUCKET_NAME: &str = "dummy-bucket-name";
    const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

    #[tokio::test]
    async fn simple_has_object_found() -> Result<(), Error> {
        let s3_client = S3Client::new_with(
            MockRequestDispatcher::with_status(StatusCode::OK.into()).with_header("Content-Length", "512"),
            MockCredentialsProvider,
            Region::UsEast1,
        );
        let store = S3Store::new_with_client_and_jitter(
            &config::backends::S3Store {
                bucket: BUCKET_NAME.to_string(),
                ..Default::default()
            },
            s3_client,
            Box::new(move |_delay| Duration::from_secs(0)),
        )?;
        let store_pin = Pin::new(&store);

        let digest = DigestInfo::try_new(&VALID_HASH1, 100).unwrap();
        let result = store_pin.has(digest.clone()).await;
        assert_eq!(result, Ok(Some(512)), "Expected to find item, got: {:?}", result);
        Ok(())
    }

    #[tokio::test]
    async fn simple_has_object_not_found() -> Result<(), Error> {
        let s3_client = S3Client::new_with(
            MockRequestDispatcher::with_status(StatusCode::NOT_FOUND.into()).with_body(
                r#"
                <Error>
                    <Code>NoSuchKey</Code>
                    <Message>The specified key does not exist.</Message>
                    <Key>dummy_path</Key>
                    <RequestId>request_id</RequestId>
                    <HostId>foobar</HostId>
                </Error>"#,
            ),
            MockCredentialsProvider,
            Region::UsEast1,
        );
        let store = S3Store::new_with_client_and_jitter(
            &config::backends::S3Store {
                bucket: BUCKET_NAME.to_string(),
                ..Default::default()
            },
            s3_client,
            Box::new(move |_delay| Duration::from_secs(0)),
        )?;
        let store_pin = Pin::new(&store);

        let digest = DigestInfo::try_new(&VALID_HASH1, 100).unwrap();
        let result = store_pin.has(digest.clone()).await;
        assert_eq!(result, Ok(None), "Expected to not find item, got: {:?}", result);
        Ok(())
    }

    #[tokio::test]
    async fn simple_has_retries() -> Result<(), Error> {
        let s3_client = S3Client::new_with(
            MultipleMockRequestDispatcher::new(vec![
                // Common Status errors that will retry.
                MockRequestDispatcher::with_status(StatusCode::INTERNAL_SERVER_ERROR.into()),
                MockRequestDispatcher::with_status(StatusCode::SERVICE_UNAVAILABLE.into()),
                MockRequestDispatcher::with_status(StatusCode::CONFLICT.into()),
                // Dispatch errors are retried. Like timeouts.
                MockRequestDispatcher::with_dispatch_error(HttpDispatchError::new("maybe timeout?".to_string())),
                // Note: "OK" must always be last.
                MockRequestDispatcher::with_status(StatusCode::OK.into()).with_header("Content-Length", "111"),
            ]),
            MockCredentialsProvider,
            Region::UsEast1,
        );
        let store = S3Store::new_with_client_and_jitter(
            &config::backends::S3Store {
                bucket: BUCKET_NAME.to_string(),
                retry: config::backends::Retry {
                    max_retries: 1024,
                    delay: 0.,
                    jitter: 0.,
                },
                ..Default::default()
            },
            s3_client,
            Box::new(move |_delay| Duration::from_secs(0)),
        )?;
        let store_pin = Pin::new(&store);

        let digest = DigestInfo::try_new(&VALID_HASH1, 100).unwrap();
        let result = store_pin.has(digest.clone()).await;
        assert_eq!(result, Ok(Some(111)), "Expected to find item, got: {:?}", result);
        Ok(())
    }

    #[tokio::test]
    async fn simple_update_ac() -> Result<(), Error> {
        let (sender, mut receiver) = mpsc::channel::<(SignedRequest, Vec<u8>)>(100);
        let s3_client = S3Client::new_with(
            MultipleMockRequestDispatcher::new(vec![
                MockRequestDispatcher::with_status(StatusCode::OK.into())
                    .with_request_checker(receive_request(sender.clone())),
                MockRequestDispatcher::with_status(StatusCode::OK.into())
                    .with_request_checker(receive_request(sender.clone())),
            ]),
            MockCredentialsProvider,
            Region::UsEast1,
        );

        // Create dummy data.
        let mut send_data = Vec::with_capacity(50);
        for i in 0..send_data.capacity() {
            send_data.push(((i * 3) % 256) as u8);
        }

        {
            // Send payload.
            let store = S3Store::new_with_client_and_jitter(
                &config::backends::S3Store {
                    bucket: BUCKET_NAME.to_string(),
                    ..Default::default()
                },
                s3_client,
                Box::new(move |_delay| Duration::from_secs(0)),
            )?;
            let store_pin = Pin::new(&store);
            store_pin
                .update_oneshot(DigestInfo::try_new(&VALID_HASH1, 199)?, send_data.clone().into())
                .await?;
        }

        // Check requests.
        {
            let (request, rt_data) = receiver.next().await.err_tip(|| "Could not get next payload")?;

            assert_eq!(request.method, "PUT");
            assert_eq!(
                from_utf8(&request.headers["content-type"][0]).unwrap(),
                "application/octet-stream"
            );
            assert_eq!(
                from_utf8(&request.headers["host"][0]).unwrap(),
                "s3.us-east-1.amazonaws.com"
            );
            assert_eq!(request.canonical_query_string, "");
            assert_eq!(
                request.canonical_uri,
                "/dummy-bucket-name/0123456789abcdef000000000000000000010000000000000123456789abcdef-199"
            );
            assert_eq!(send_data, rt_data, "Expected sizes to match",);
        }

        Ok(())
    }

    #[tokio::test]
    async fn simple_get_ac() -> Result<(), Error> {
        const VALUE: &str = "23";
        const AC_ENTRY_SIZE: u64 = 1000; // Any size that is not VALUE.len().

        let s3_client = S3Client::new_with(
            MockRequestDispatcher::with_status(StatusCode::OK.into()).with_body(&VALUE),
            MockCredentialsProvider,
            Region::UsEast1,
        );
        let store = S3Store::new_with_client_and_jitter(
            &config::backends::S3Store {
                bucket: BUCKET_NAME.to_string(),
                ..Default::default()
            },
            s3_client,
            Box::new(move |_delay| Duration::from_secs(0)),
        )?;
        let store_pin = Pin::new(&store);

        let store_data = store_pin
            .get_part_unchunked(DigestInfo::try_new(&VALID_HASH1, AC_ENTRY_SIZE)?, 0, None, None)
            .await?;
        assert_eq!(
            store_data,
            VALUE.as_bytes(),
            "Hash for key: {} did not insert. Expected: {:#x?}, but got: {:#x?}",
            VALID_HASH1,
            VALUE,
            store_data
        );
        Ok(())
    }

    #[tokio::test]
    async fn smoke_test_get_part() -> Result<(), Error> {
        const AC_ENTRY_SIZE: u64 = 1000; // Any size that is not raw_send_data.len().

        let (sender, mut receiver) = mpsc::channel::<SignedRequest>(100);
        let sender = Arc::new(Mutex::new(sender));

        let s3_client = S3Client::new_with(
            MockRequestDispatcher::with_status(StatusCode::OK.into()).with_request_checker(move |request| {
                sender
                    .lock()
                    .unwrap()
                    .try_send(request)
                    .expect("Failed to send payload");
            }),
            MockCredentialsProvider,
            Region::UsEast1,
        );
        let store = S3Store::new_with_client_and_jitter(
            &config::backends::S3Store {
                bucket: BUCKET_NAME.to_string(),
                ..Default::default()
            },
            s3_client,
            Box::new(move |_delay| Duration::from_secs(0)),
        )?;
        let store_pin = Pin::new(&store);

        const OFFSET: usize = 105;
        const LENGTH: usize = 50_000; // Just a size that is not the same as the real data size.
        store_pin
            .get_part_unchunked(
                DigestInfo::try_new(&VALID_HASH1, AC_ENTRY_SIZE)?,
                OFFSET,
                Some(LENGTH),
                None,
            )
            .await?;

        let request = receiver.next().await.err_tip(|| "Could not get next SignedRequest")?;
        let (start, end) = get_range_from_request(&request)?;
        assert_eq!(start, OFFSET, "Expected start range to match");
        assert_eq!(end, Some(OFFSET + LENGTH), "Expected end range to match");
        Ok(())
    }

    /// This test will run `get_part` on the s3 store and then abruptly terminate the stream
    /// after sending a few bytes of data. We expect retries to happen automatically and
    /// test that it resumes downloading from where it left off.
    #[tokio::test]
    async fn get_part_retries_broken_pipe() -> Result<(), Error> {
        let (request_tx, mut request_rx) = unbounded_channel::<RequestWithWriter>();

        let store = S3Store::new_with_client_and_jitter(
            &config::backends::S3Store {
                bucket: BUCKET_NAME.to_string(),
                retry: config::backends::Retry {
                    max_retries: 2,
                    delay: 0.,
                    jitter: 0.,
                },
                ..Default::default()
            },
            S3Client::new_with(
                StreamMockRequestDispatcher::new(request_tx),
                MockCredentialsProvider,
                Region::UsEast1,
            ),
            Box::new(move |_delay| Duration::from_secs(0)),
        )?;

        const START_BYTE: usize = 1;
        const SHUTDOWN_AFTER_N_BYTES: usize = 2;
        const SEND_BYTES: [u8; 5] = [9u8, 8u8, 7u8, 6u8, 5u8];

        // This future represents the s3 side sending data and abruptly shutting down a few times.
        let send_data_fut = async move {
            let mut expected_current_start: usize = START_BYTE;
            {
                // Send a few bytes then abruptly shutdown.
                let (request, mut tx) = request_rx.recv().await.unwrap();
                let (start, _) = get_range_from_request(&request)?;
                assert_eq!(start, expected_current_start, "Expected start position to match");
                tx.write_all(&SEND_BYTES[start..start + SHUTDOWN_AFTER_N_BYTES]).await?;
                expected_current_start += SHUTDOWN_AFTER_N_BYTES;
                tx.shutdown().await.err_tip(|| "Failed shutdown")?; // Abruptly terminate the stream causing disconnect.
            }
            {
                // Send a few more bytes then abruptly shutdown.
                let (request, mut tx) = request_rx.recv().await.unwrap();
                let (start, _) = get_range_from_request(&request)?;
                assert_eq!(start, expected_current_start, "Expected start position to match");
                tx.write_all(&SEND_BYTES[start..start + SHUTDOWN_AFTER_N_BYTES]).await?;
                expected_current_start += SHUTDOWN_AFTER_N_BYTES;
                tx.shutdown().await.err_tip(|| "Failed shutdown")?; // Abruptly terminate the stream causing disconnect.
            }
            {
                // This time complete the request.
                let (request, mut tx) = request_rx.recv().await.unwrap();
                let (start, _) = get_range_from_request(&request)?;
                assert_eq!(start, expected_current_start, "Expected start position to match");
                tx.write_all(&SEND_BYTES[start..]).await?;
                tx.write(&[]).await.err_tip(|| "Failed to write EOF")?; // Send EOF to finish stream.
            }
            Result::<(), Error>::Ok(())
        };

        let digest = DigestInfo::try_new(&VALID_HASH1, 100).unwrap();
        let get_part_fut = Pin::new(&store).get_part_unchunked(digest, START_BYTE, None, None);

        // Now run our test.
        let (get_part_result, send_data_result) = join!(get_part_fut, send_data_fut);

        send_data_result?;
        // At this point we shutdown the stream 2 times, but sent a little bit each time. We need
        // to make sure it resumed the data properly and at the proper position.
        assert_eq!(get_part_result?, SEND_BYTES[START_BYTE..], "Expected data to match.");
        Ok(())
    }

    #[tokio::test]
    async fn get_part_simple_retries() -> Result<(), Error> {
        let s3_client = S3Client::new_with(
            MultipleMockRequestDispatcher::new(vec![
                // Common Status errors that will retry.
                MockRequestDispatcher::with_status(StatusCode::INTERNAL_SERVER_ERROR.into()),
                MockRequestDispatcher::with_status(StatusCode::SERVICE_UNAVAILABLE.into()),
                MockRequestDispatcher::with_status(StatusCode::CONFLICT.into()),
                // Dispatch errors are retried. Like timeouts.
                MockRequestDispatcher::with_dispatch_error(HttpDispatchError::new("maybe timeout?".to_string())),
                // Note: "OK" must always be last.
                MockRequestDispatcher::with_status(StatusCode::OK.into()),
            ]),
            MockCredentialsProvider,
            Region::UsEast1,
        );
        let store = S3Store::new_with_client_and_jitter(
            &config::backends::S3Store {
                bucket: BUCKET_NAME.to_string(),
                retry: config::backends::Retry {
                    max_retries: 1024,
                    delay: 0.,
                    jitter: 0.,
                },
                ..Default::default()
            },
            s3_client,
            Box::new(move |_delay| Duration::from_secs(0)),
        )?;

        let digest = DigestInfo::try_new(&VALID_HASH1, 100).unwrap();
        let result = Pin::new(&store).get_part_unchunked(digest, 0, None, None).await;
        assert!(result.is_ok(), "Expected to find item, got: {:?}", result);
        Ok(())
    }

    #[tokio::test]
    async fn multipart_update_large_cas() -> Result<(), Error> {
        let (sender, mut receiver) = mpsc::channel::<(SignedRequest, Vec<u8>)>(100);
        let s3_client = S3Client::new_with(
            MultipleMockRequestDispatcher::new(vec![
                MockRequestDispatcher::with_status(StatusCode::OK.into())
                    .with_request_checker(receive_request(sender.clone()))
                    .with_body(
                        r#"
                    <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                      <UploadId>Dummy-uploadid</UploadId>
                    </InitiateMultipartUploadResult>"#,
                    ),
                MockRequestDispatcher::with_status(StatusCode::OK.into())
                    .with_request_checker(receive_request(sender.clone())),
                MockRequestDispatcher::with_status(StatusCode::OK.into())
                    .with_request_checker(receive_request(sender.clone())),
                MockRequestDispatcher::with_status(StatusCode::OK.into())
                    .with_request_checker(receive_request(sender.clone())),
                MockRequestDispatcher::with_status(StatusCode::OK.into())
                    .with_request_checker(receive_request(sender.clone())),
            ]),
            MockCredentialsProvider,
            Region::UsEast1,
        );

        // Create dummy data.
        let mut send_data = Vec::with_capacity(MIN_MULTIPART_SIZE * 2 + 50);
        for i in 0..send_data.capacity() {
            send_data.push(((i * 3) % 256) as u8);
        }

        {
            // Send payload.
            let digest = DigestInfo::try_new(&VALID_HASH1, send_data.len())?;
            let store = S3Store::new_with_client_and_jitter(
                &config::backends::S3Store {
                    bucket: BUCKET_NAME.to_string(),
                    ..Default::default()
                },
                s3_client,
                Box::new(move |_delay| Duration::from_secs(0)),
            )?;
            let store_pin = Pin::new(&store);
            store_pin.update_oneshot(digest, send_data.clone().into()).await?;
        }

        // Check requests.

        {
            // First request is the create_multipart_upload request.
            let (request, rt_data) = receiver.next().await.err_tip(|| "Could not get next payload")?;
            assert_eq!(rt_data.len(), 0, "Expected first request payload to be empty");
            assert_eq!(request.method, "POST");
            assert_eq!(
                from_utf8(&request.headers["content-type"][0]).unwrap(),
                "application/octet-stream"
            );
            assert_eq!(
                from_utf8(&request.headers["host"][0]).unwrap(),
                "s3.us-east-1.amazonaws.com"
            );
            assert_eq!(
                from_utf8(&request.headers["content-length"][0]).unwrap(),
                format!("{}", rt_data.len())
            );
            assert_eq!(request.canonical_query_string, "uploads=");
            assert_eq!(
                request.canonical_uri,
                "/dummy-bucket-name/0123456789abcdef000000000000000000010000000000000123456789abcdef-10485810"
            );
        }
        {
            // Further requests are the actual data.
            let (request, rt_data) = receiver.next().await.err_tip(|| "Could not get next payload")?;
            assert_eq!(&send_data[0..MIN_MULTIPART_SIZE], rt_data, "Expected data to match");
            assert_eq!(request.method, "PUT");
            assert_eq!(
                from_utf8(&request.headers["content-type"][0]).unwrap(),
                "application/octet-stream"
            );
            assert_eq!(
                from_utf8(&request.headers["host"][0]).unwrap(),
                "s3.us-east-1.amazonaws.com"
            );
            assert_eq!(
                from_utf8(&request.headers["content-length"][0]).unwrap(),
                format!("{}", rt_data.len())
            );
            assert_eq!(request.canonical_query_string, "partNumber=1&uploadId=Dummy-uploadid");
            assert_eq!(
                request.canonical_uri,
                "/dummy-bucket-name/0123456789abcdef000000000000000000010000000000000123456789abcdef-10485810"
            );
        }
        {
            let (request, rt_data) = receiver.next().await.err_tip(|| "Could not get next payload")?;
            assert_eq!(
                &send_data[MIN_MULTIPART_SIZE..MIN_MULTIPART_SIZE * 2],
                rt_data,
                "Expected data to match"
            );

            assert_eq!(
                from_utf8(&request.headers["content-length"][0]).unwrap(),
                format!("{}", rt_data.len())
            );
            assert_eq!(request.canonical_query_string, "partNumber=2&uploadId=Dummy-uploadid");
            assert_eq!(
                request.canonical_uri,
                "/dummy-bucket-name/0123456789abcdef000000000000000000010000000000000123456789abcdef-10485810"
            );
        }
        {
            let (request, rt_data) = receiver.next().await.err_tip(|| "Could not get next payload")?;
            assert_eq!(
                &send_data[MIN_MULTIPART_SIZE * 2..MIN_MULTIPART_SIZE * 2 + 50],
                rt_data,
                "Expected data to match"
            );
            assert_eq!(
                from_utf8(&request.headers["content-length"][0]).unwrap(),
                format!("{}", rt_data.len())
            );
            assert_eq!(request.canonical_query_string, "partNumber=3&uploadId=Dummy-uploadid");
            assert_eq!(
                request.canonical_uri,
                "/dummy-bucket-name/0123456789abcdef000000000000000000010000000000000123456789abcdef-10485810"
            );
        }
        {
            // Final payload is the complete_multipart_upload request.
            let (request, rt_data) = receiver.next().await.err_tip(|| "Could not get next payload")?;
            const COMPLETE_MULTIPART_PAYLOAD_DATA: &str = concat!(
                r#"<?xml version="1.0" encoding="utf-8"?>"#,
                "<CompleteMultipartUpload>",
                "<Part><PartNumber>1</PartNumber></Part>",
                "<Part><PartNumber>2</PartNumber></Part>",
                "<Part><PartNumber>3</PartNumber></Part>",
                "</CompleteMultipartUpload>",
            );
            assert_eq!(
                from_utf8(&rt_data).unwrap(),
                COMPLETE_MULTIPART_PAYLOAD_DATA,
                "Expected last payload to be empty"
            );
            assert_eq!(request.method, "POST");
            assert_eq!(
                from_utf8(&request.headers["content-length"][0]).unwrap(),
                format!("{}", COMPLETE_MULTIPART_PAYLOAD_DATA.len())
            );
            assert_eq!(
                from_utf8(&request.headers["x-amz-content-sha256"][0]).unwrap(),
                "730f96c9a87580c7930b5bd4fd0457fbe01b34f2261dcdde877d09b06d937b5e"
            );
            assert_eq!(request.canonical_query_string, "uploadId=Dummy-uploadid");
            assert_eq!(
                request.canonical_uri,
                "/dummy-bucket-name/0123456789abcdef000000000000000000010000000000000123456789abcdef-10485810"
            );
        }

        Ok(())
    }
}
