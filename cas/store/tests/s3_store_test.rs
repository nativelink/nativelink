// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::io::Cursor;
use std::pin::Pin;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::channel::mpsc;
use futures::executor::block_on;
use futures::stream::StreamExt;
use http::status::StatusCode;
use rusoto_core::{request::HttpDispatchError, signature::SignedRequest, signature::SignedRequestPayload, Region};
use rusoto_mock::{MockCredentialsProvider, MockRequestDispatcher, MultipleMockRequestDispatcher};
use rusoto_s3::S3Client;
use tokio::io::AsyncReadExt;

use common::DigestInfo;
use config;
use error::Error;
use error::ResultExt;
use s3_store::S3Store;
use traits::StoreTrait;

// Should match constant in s3_store.
const MIN_MULTIPART_SIZE: usize = 5_000_000; // 5mb.

fn receive_request(sender: mpsc::Sender<(SignedRequest, Vec<u8>)>) -> impl Fn(SignedRequest) {
    move |mut request| {
        let mut sender = sender.clone();
        let mut raw_payload = Vec::new();
        match request.payload.take() {
            Some(SignedRequestPayload::Stream(stream)) => {
                let mut async_reader = stream.into_async_read();
                assert!(block_on(async_reader.read_to_end(&mut raw_payload)).is_ok());
            }
            Some(SignedRequestPayload::Buffer(buffer)) => raw_payload.copy_from_slice(&buffer[..]),
            None => {}
        }
        sender.try_send((request, raw_payload)).expect("Failed to send payload");
    }
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
            MockRequestDispatcher::with_status(StatusCode::OK.into()),
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
        assert_eq!(result, Ok(true), "Expected to find item, got: {:?}", result);
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
        assert_eq!(result, Ok(false), "Expected to not find item, got: {:?}", result);
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
        let store_pin = Pin::new(&store);

        let digest = DigestInfo::try_new(&VALID_HASH1, 100).unwrap();
        let result = store_pin.has(digest.clone()).await;
        assert_eq!(result, Ok(true), "Expected to find item, got: {:?}", result);
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
                .update(
                    DigestInfo::try_new(&VALID_HASH1, 199)?,
                    Box::new(Cursor::new(send_data.clone())),
                )
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

        let mut store_data = Vec::new();
        store_pin
            .get(
                DigestInfo::try_new(&VALID_HASH1, AC_ENTRY_SIZE)?,
                &mut Cursor::new(&mut store_data),
            )
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
            .get_part(
                DigestInfo::try_new(&VALID_HASH1, AC_ENTRY_SIZE)?,
                &mut Cursor::new(Vec::new()),
                OFFSET,
                Some(LENGTH),
            )
            .await?;

        let signed_request = receiver.next().await.err_tip(|| "Could not get next SignedRequest")?;
        let bytes_header = signed_request
            .headers
            .get("range")
            .err_tip(|| "Expected 'range' header")?;
        assert_eq!(bytes_header.len(), 1, "Expected only one 'range' header");
        assert_eq!(
            bytes_header[0],
            format!("bytes={}-{}", OFFSET, OFFSET + LENGTH).as_bytes(),
            "Got wrong 'range' header"
        );
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
            let mut digest = DigestInfo::try_new(&VALID_HASH1, send_data.len())?;
            digest.trust_size = true;
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
                .update(digest, Box::new(Cursor::new(send_data.clone())))
                .await?;
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
            assert_eq!(request.canonical_query_string, "uploads=");
            assert_eq!(
                request.canonical_uri,
                "/dummy-bucket-name/0123456789abcdef000000000000000000010000000000000123456789abcdef-10000050"
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
            assert_eq!(request.canonical_query_string, "partNumber=1&uploadId=Dummy-uploadid");
            assert_eq!(
                request.canonical_uri,
                "/dummy-bucket-name/0123456789abcdef000000000000000000010000000000000123456789abcdef-10000050"
            );
        }
        {
            let (request, rt_data) = receiver.next().await.err_tip(|| "Could not get next payload")?;
            assert_eq!(
                &send_data[MIN_MULTIPART_SIZE..MIN_MULTIPART_SIZE * 2],
                rt_data,
                "Expected data to match"
            );
            assert_eq!(request.canonical_query_string, "partNumber=2&uploadId=Dummy-uploadid");
            assert_eq!(
                request.canonical_uri,
                "/dummy-bucket-name/0123456789abcdef000000000000000000010000000000000123456789abcdef-10000050"
            );
        }
        {
            let (request, rt_data) = receiver.next().await.err_tip(|| "Could not get next payload")?;
            assert_eq!(
                &send_data[MIN_MULTIPART_SIZE * 2..MIN_MULTIPART_SIZE * 2 + 50],
                rt_data,
                "Expected data to match"
            );
            assert_eq!(request.canonical_query_string, "partNumber=3&uploadId=Dummy-uploadid");
            assert_eq!(
                request.canonical_uri,
                "/dummy-bucket-name/0123456789abcdef000000000000000000010000000000000123456789abcdef-10000050"
            );
        }
        {
            // Final payload is the complete_multipart_upload request.
            let (request, rt_data) = receiver.next().await.err_tip(|| "Could not get next payload")?;
            assert_eq!(&send_data[0..0], rt_data, "Expected data to match");
            assert_eq!(request.canonical_query_string, "uploadId=Dummy-uploadid");
            assert_eq!(
                request.canonical_uri,
                "/dummy-bucket-name/0123456789abcdef000000000000000000010000000000000123456789abcdef-10000050"
            );
        }

        Ok(())
    }
}
