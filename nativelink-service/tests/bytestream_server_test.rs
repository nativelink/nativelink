// Copyright 2023 The NativeLink Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::pin::Pin;
use std::sync::Arc;

use futures::poll;
use futures::task::Poll;
use hyper::body::Sender;
use maplit::hashmap;
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_proto::google::bytestream::WriteResponse;
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::common::{encode_stream_proto, DigestInfo};
use nativelink_util::spawn;
use nativelink_util::task::JoinHandleDropGuard;
use prometheus_client::registry::Registry;
use tokio::task::yield_now;
use tonic::{Request, Response};

const INSTANCE_NAME: &str = "foo_instance_name";
const HASH1: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";

async fn make_store_manager() -> Result<Arc<StoreManager>, Error> {
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store(
        "main_cas",
        store_factory(
            &nativelink_config::stores::StoreConfigOptions::memory(
                nativelink_config::stores::MemoryStore::default(),
            ),
            &store_manager,
            Some(&mut <Registry>::default()),
            None,
        )
        .await?,
    );
    Ok(store_manager)
}

fn make_bytestream_server(store_manager: &StoreManager) -> Result<ByteStreamServer, Error> {
    ByteStreamServer::new(
        &nativelink_config::cas_server::ByteStreamConfig {
            cas_stores: hashmap! {
                "foo_instance_name".to_string() => "main_cas".to_string(),
            },
            persist_stream_on_disconnect_timeout: 0,
            max_bytes_per_stream: 1024,
        },
        store_manager,
    )
}

#[cfg(test)]
pub mod write_tests {
    use nativelink_proto::google::bytestream::{
        byte_stream_server::ByteStream, // Needed to call .write().
        WriteRequest,
    };
    use pretty_assertions::assert_eq; // Must be declared in every module.
    use tonic::{
        codec::Codec, // Needed for .decoder().
        codec::CompressionEncoding,
        codec::ProstCodec,
        transport::Body,
        Streaming,
    };

    use super::*;

    #[nativelink_test]
    pub async fn chunked_stream_receives_all_data() -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let bs_server = make_bytestream_server(store_manager.as_ref())?;
        let store_owned = store_manager.get_store("main_cas").unwrap();

        let store = Pin::new(store_owned.as_ref());

        // Setup stream.
        let (mut tx, join_handle) = {
            let (tx, body) = Body::channel();
            let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
            // Note: This is an undocumented function.
            let stream = Streaming::new_request(
                codec.decoder(),
                body,
                Some(CompressionEncoding::Gzip),
                None,
            );

            let join_handle = spawn!(
                "chunked_stream_receives_all_data_write_stream",
                async move {
                    let response_future = bs_server.write(Request::new(stream));
                    response_future.await
                },
            );
            (tx, join_handle)
        };
        // Send data.
        let raw_data = {
            let raw_data = "12456789abcdefghijk".as_bytes();
            // Chunk our data into two chunks to simulate something a client
            // might do.
            const BYTE_SPLIT_OFFSET: usize = 8;

            let resource_name = format!(
                "{}/uploads/{}/blobs/{}/{}",
                INSTANCE_NAME,
                "4dcec57e-1389-4ab5-b188-4a59f22ceb4b", // Randomly generated.
                HASH1,
                raw_data.len()
            );
            let mut write_request = WriteRequest {
                resource_name,
                write_offset: 0,
                finish_write: false,
                data: vec![].into(),
            };
            // Write first chunk of data.
            write_request.write_offset = 0;
            write_request.data = raw_data[..BYTE_SPLIT_OFFSET].into();
            tx.send_data(encode_stream_proto(&write_request)?).await?;

            // Write empty set of data (clients are allowed to do this.
            write_request.write_offset = BYTE_SPLIT_OFFSET as i64;
            write_request.data = vec![].into();
            tx.send_data(encode_stream_proto(&write_request)?).await?;

            // Write final bit of data.
            write_request.write_offset = BYTE_SPLIT_OFFSET as i64;
            write_request.data = raw_data[BYTE_SPLIT_OFFSET..].into();
            write_request.finish_write = true;
            tx.send_data(encode_stream_proto(&write_request)?).await?;

            raw_data
        };
        // Check results of server.
        {
            // One for spawn() future and one for result.
            let server_result = join_handle.await??;
            let committed_size = usize::try_from(server_result.into_inner().committed_size)
                .or(Err("Cant convert i64 to usize"))?;
            assert_eq!(committed_size, raw_data.len());

            // Now lets check our store to ensure it was written with proper data.
            assert!(
                store
                    .has(DigestInfo::try_new(HASH1, raw_data.len())?)
                    .await?
                    .is_some(),
                "Not found in store",
            );
            let store_data = store
                .get_part_unchunked(DigestInfo::try_new(HASH1, raw_data.len())?, 0, None)
                .await?;
            assert_eq!(
                std::str::from_utf8(&store_data),
                std::str::from_utf8(raw_data),
                "Expected store to have been updated to new value"
            );
        }
        Ok(())
    }

    #[nativelink_test]
    pub async fn resume_write_success() -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let bs_server = make_bytestream_server(store_manager.as_ref())?;
        let store_owned = store_manager.get_store("main_cas").unwrap();

        let store = Pin::new(store_owned.as_ref());

        async fn setup_stream(
            bs_server: ByteStreamServer,
        ) -> Result<
            (
                Sender,
                JoinHandleDropGuard<(
                    Result<Response<WriteResponse>, tonic::Status>,
                    ByteStreamServer,
                )>,
            ),
            Error,
        > {
            let (tx, body) = Body::channel();
            let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
            // Note: This is an undocumented function.
            let stream = Streaming::new_request(
                codec.decoder(),
                body,
                Some(CompressionEncoding::Gzip),
                None,
            );

            let join_handle = spawn!("resume_write_success_write_stream", async move {
                let response_future = bs_server.write(Request::new(stream));
                (response_future.await, bs_server)
            });
            Ok((tx, join_handle))
        }
        let (mut tx, join_handle) = setup_stream(bs_server).await?;
        const WRITE_DATA: &str = "12456789abcdefghijk";

        // Chunk our data into two chunks to simulate something a client
        // might do.
        const BYTE_SPLIT_OFFSET: usize = 8;

        let resource_name = format!(
            "{}/uploads/{}/blobs/{}/{}",
            INSTANCE_NAME,
            "4dcec57e-1389-4ab5-b188-4a59f22ceb4b", // Randomly generated.
            HASH1,
            WRITE_DATA.len()
        );
        let mut write_request = WriteRequest {
            resource_name,
            write_offset: 0,
            finish_write: false,
            data: vec![].into(),
        };
        {
            // Write first chunk of data.
            write_request.write_offset = 0;
            write_request.data = WRITE_DATA[..BYTE_SPLIT_OFFSET].into();
            tx.send_data(encode_stream_proto(&write_request)?).await?;
        }
        let bs_server = {
            // Now disconnect our stream.
            drop(tx);
            let (result, bs_server) = join_handle.await?;
            assert_eq!(result.is_err(), true, "Expected error to be returned");
            bs_server
        };
        // Now reconnect.
        let (mut tx, join_handle) = setup_stream(bs_server).await?;
        {
            // Write the remainder of our data.
            write_request.write_offset = BYTE_SPLIT_OFFSET as i64;
            write_request.finish_write = true;
            write_request.data = WRITE_DATA[BYTE_SPLIT_OFFSET..].into();
            tx.send_data(encode_stream_proto(&write_request)?).await?;
        }
        {
            // Now disconnect our stream.
            drop(tx);
            let (result, _bs_server) = join_handle.await?;
            result?;
        }
        {
            // Check to make sure our store recorded the data properly.
            let digest = DigestInfo::try_new(HASH1, WRITE_DATA.len())?;
            assert_eq!(
                store.get_part_unchunked(digest, 0, None).await?,
                WRITE_DATA,
                "Data written to store did not match expected data",
            );
        }
        Ok(())
    }

    #[nativelink_test]
    pub async fn restart_write_success() -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let bs_server = make_bytestream_server(store_manager.as_ref())?;
        let store_owned = store_manager.get_store("main_cas").unwrap();

        let store = Pin::new(store_owned.as_ref());

        async fn setup_stream(
            bs_server: ByteStreamServer,
        ) -> Result<
            (
                Sender,
                JoinHandleDropGuard<(
                    Result<Response<WriteResponse>, tonic::Status>,
                    ByteStreamServer,
                )>,
            ),
            Error,
        > {
            let (tx, body) = Body::channel();
            let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
            // Note: This is an undocumented function.
            let stream = Streaming::new_request(
                codec.decoder(),
                body,
                Some(CompressionEncoding::Gzip),
                None,
            );

            let join_handle = spawn!("restart_write_success_write_stream", async move {
                let response_future = bs_server.write(Request::new(stream));
                (response_future.await, bs_server)
            });
            Ok((tx, join_handle))
        }
        let (mut tx, join_handle) = setup_stream(bs_server).await?;
        const WRITE_DATA: &str = "12456789abcdefghijk";

        // Chunk our data into two chunks to simulate something a client
        // might do.
        const BYTE_SPLIT_OFFSET: usize = 8;

        let resource_name = format!(
            "{}/uploads/{}/blobs/{}/{}",
            INSTANCE_NAME,
            "4dcec57e-1389-4ab5-b188-4a59f22ceb4b", // Randomly generated.
            HASH1,
            WRITE_DATA.len()
        );
        let mut write_request = WriteRequest {
            resource_name,
            write_offset: 0,
            finish_write: false,
            data: vec![].into(),
        };
        {
            // Write first chunk of data.
            write_request.write_offset = 0;
            write_request.data = WRITE_DATA[..BYTE_SPLIT_OFFSET].into();
            tx.send_data(encode_stream_proto(&write_request)?).await?;
        }
        let bs_server = {
            // Now disconnect our stream.
            drop(tx);
            let (result, bs_server) = join_handle.await?;
            assert_eq!(result.is_err(), true, "Expected error to be returned");
            bs_server
        };
        // Now reconnect.
        let (mut tx, join_handle) = setup_stream(bs_server).await?;
        {
            // Write first chunk of data again.
            write_request.write_offset = 0;
            write_request.data = WRITE_DATA[..BYTE_SPLIT_OFFSET].into();
            tx.send_data(encode_stream_proto(&write_request)?).await?;
        }
        {
            // Write the remainder of our data.
            write_request.write_offset = BYTE_SPLIT_OFFSET as i64;
            write_request.finish_write = true;
            write_request.data = WRITE_DATA[BYTE_SPLIT_OFFSET..].into();
            tx.send_data(encode_stream_proto(&write_request)?).await?;
        }
        {
            // Now disconnect our stream.
            drop(tx);
            let (result, _bs_server) = join_handle.await?;
            assert!(result.is_ok(), "Expected success to be returned");
        }
        {
            // Check to make sure our store recorded the data properly.
            let digest = DigestInfo::try_new(HASH1, WRITE_DATA.len())?;
            assert_eq!(
                store.get_part_unchunked(digest, 0, None).await?,
                WRITE_DATA,
                "Data written to store did not match expected data",
            );
        }
        Ok(())
    }

    #[nativelink_test]
    pub async fn restart_mid_stream_write_success() -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let bs_server = make_bytestream_server(store_manager.as_ref())?;
        let store_owned = store_manager.get_store("main_cas").unwrap();

        let store = Pin::new(store_owned.as_ref());

        async fn setup_stream(
            bs_server: ByteStreamServer,
        ) -> Result<
            (
                Sender,
                JoinHandleDropGuard<(
                    Result<Response<WriteResponse>, tonic::Status>,
                    ByteStreamServer,
                )>,
            ),
            Error,
        > {
            let (tx, body) = Body::channel();
            let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
            // Note: This is an undocumented function.
            let stream = Streaming::new_request(
                codec.decoder(),
                body,
                Some(CompressionEncoding::Gzip),
                None,
            );

            let join_handle = spawn!(
                "restart_mid_stream_write_success_write_stream",
                async move {
                    let response_future = bs_server.write(Request::new(stream));
                    (response_future.await, bs_server)
                },
            );
            Ok((tx, join_handle))
        }
        let (mut tx, join_handle) = setup_stream(bs_server).await?;
        const WRITE_DATA: &str = "12456789abcdefghijk";

        // Chunk our data into two chunks to simulate something a client
        // might do.
        const BYTE_SPLIT_OFFSET: usize = 8;

        let resource_name = format!(
            "{}/uploads/{}/blobs/{}/{}",
            INSTANCE_NAME,
            "4dcec57e-1389-4ab5-b188-4a59f22ceb4b", // Randomly generated.
            HASH1,
            WRITE_DATA.len()
        );
        let mut write_request = WriteRequest {
            resource_name,
            write_offset: 0,
            finish_write: false,
            data: vec![].into(),
        };
        {
            // Write first chunk of data.
            write_request.write_offset = 0;
            write_request.data = WRITE_DATA[..BYTE_SPLIT_OFFSET].into();
            tx.send_data(encode_stream_proto(&write_request)?).await?;
        }
        let bs_server = {
            // Now disconnect our stream.
            drop(tx);
            let (result, bs_server) = join_handle.await?;
            assert_eq!(result.is_err(), true, "Expected error to be returned");
            bs_server
        };
        // Now reconnect.
        let (mut tx, join_handle) = setup_stream(bs_server).await?;
        {
            // Write some of the first chunk of data again.
            write_request.write_offset = 2;
            write_request.data = WRITE_DATA[2..BYTE_SPLIT_OFFSET].into();
            tx.send_data(encode_stream_proto(&write_request)?).await?;
        }
        {
            // Write the remainder of our data.
            write_request.write_offset = BYTE_SPLIT_OFFSET as i64;
            write_request.finish_write = true;
            write_request.data = WRITE_DATA[BYTE_SPLIT_OFFSET..].into();
            tx.send_data(encode_stream_proto(&write_request)?).await?;
        }
        {
            // Now disconnect our stream.
            drop(tx);
            let (result, _bs_server) = join_handle.await?;
            assert!(result.is_ok(), "Expected success to be returned");
        }
        {
            // Check to make sure our store recorded the data properly.
            let digest = DigestInfo::try_new(HASH1, WRITE_DATA.len())?;
            assert_eq!(
                store.get_part_unchunked(digest, 0, None).await?,
                WRITE_DATA,
                "Data written to store did not match expected data",
            );
        }
        Ok(())
    }

    #[nativelink_test]
    pub async fn ensure_write_is_not_done_until_write_request_is_set(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let bs_server = make_bytestream_server(store_manager.as_ref())?;
        let store_owned = store_manager.get_store("main_cas").unwrap();

        let store = Pin::new(store_owned.as_ref());

        // Setup stream.
        let (mut tx, mut write_fut) = {
            let (tx, body) = Body::channel();
            let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
            // Note: This is an undocumented function.
            let stream = Streaming::new_request(
                codec.decoder(),
                body,
                Some(CompressionEncoding::Gzip),
                None,
            );

            (tx, bs_server.write(Request::new(stream)))
        };
        const WRITE_DATA: &str = "12456789abcdefghijk";
        let resource_name = format!(
            "{}/uploads/{}/blobs/{}/{}",
            INSTANCE_NAME,
            "4dcec57e-1389-4ab5-b188-4a59f22ceb4b", // Randomly generated.
            HASH1,
            WRITE_DATA.len()
        );
        let mut write_request = WriteRequest {
            resource_name,
            write_offset: 0,
            finish_write: false,
            data: vec![].into(),
        };
        {
            // Write our data.
            write_request.write_offset = 0;
            write_request.data = WRITE_DATA[..].into();
            tx.send_data(encode_stream_proto(&write_request)?).await?;
        }
        // Note: We have to pull multiple times because there are multiple futures
        // joined onto this one future and we need to ensure we run the state machine as
        // far as possible.
        for _ in 0..100 {
            assert!(
                poll!(&mut write_fut).is_pending(),
                "Expected the future to not be completed yet"
            );
        }
        {
            // Write our EOF.
            write_request.write_offset = WRITE_DATA.len() as i64;
            write_request.finish_write = true;
            write_request.data.clear();
            tx.send_data(encode_stream_proto(&write_request)?).await?;
        }
        let mut result = None;
        for _ in 0..100 {
            if let Poll::Ready(r) = poll!(&mut write_fut) {
                result = Some(r);
                break;
            }
        }
        {
            // Check our results.
            assert_eq!(
                result
                    .err_tip(|| "bs_server.write never returned a value")?
                    .err_tip(|| "bs_server.write returned an error")?
                    .into_inner(),
                WriteResponse {
                    committed_size: WRITE_DATA.len() as i64
                },
                "Expected Responses to match"
            );
        }
        {
            // Check to make sure our store recorded the data properly.
            let digest = DigestInfo::try_new(HASH1, WRITE_DATA.len())?;
            assert_eq!(
                store.get_part_unchunked(digest, 0, None).await?,
                WRITE_DATA,
                "Data written to store did not match expected data",
            );
        }
        Ok(())
    }

    #[nativelink_test]
    pub async fn out_of_order_data_fails() -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let bs_server = make_bytestream_server(store_manager.as_ref())?;

        async fn setup_stream(
            bs_server: ByteStreamServer,
        ) -> Result<
            (
                Sender,
                JoinHandleDropGuard<(
                    Result<Response<WriteResponse>, tonic::Status>,
                    ByteStreamServer,
                )>,
            ),
            Error,
        > {
            let (tx, body) = Body::channel();
            let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
            // Note: This is an undocumented function.
            let stream = Streaming::new_request(
                codec.decoder(),
                body,
                Some(CompressionEncoding::Gzip),
                None,
            );

            let join_handle = spawn!("out_of_order_data_fails_write_stream", async move {
                let response_future = bs_server.write(Request::new(stream));
                (response_future.await, bs_server)
            });
            Ok((tx, join_handle))
        }
        let (mut tx, join_handle) = setup_stream(bs_server).await?;
        const WRITE_DATA: &str = "12456789abcdefghijk";

        // Chunk our data into two chunks to simulate something a client
        // might do.
        const BYTE_SPLIT_OFFSET: usize = 8;

        let resource_name = format!(
            "{}/uploads/{}/blobs/{}/{}",
            INSTANCE_NAME,
            "4dcec57e-1389-4ab5-b188-4a59f22ceb4b", // Randomly generated.
            HASH1,
            WRITE_DATA.len()
        );
        let mut write_request = WriteRequest {
            resource_name,
            write_offset: 0,
            finish_write: false,
            data: vec![].into(),
        };
        {
            // Write first chunk of data.
            write_request.write_offset = 0;
            write_request.data = WRITE_DATA[..BYTE_SPLIT_OFFSET].into();
            tx.send_data(encode_stream_proto(&write_request)?).await?;
        }
        {
            // Write data it already has.
            write_request.write_offset = (BYTE_SPLIT_OFFSET - 1) as i64;
            write_request.data = WRITE_DATA[(BYTE_SPLIT_OFFSET - 1)..].into();
            tx.send_data(encode_stream_proto(&write_request)?).await?;
        }
        assert!(
            join_handle.await?.0.is_err(),
            "Expected error to be returned"
        );
        {
            // Make sure stream was closed.
            write_request.write_offset = (BYTE_SPLIT_OFFSET - 1) as i64;
            write_request.data = WRITE_DATA[(BYTE_SPLIT_OFFSET - 1)..].into();
            assert!(
                tx.send_data(encode_stream_proto(&write_request)?)
                    .await
                    .is_err(),
                "Expected error to be returned"
            );
        }
        Ok(())
    }

    #[nativelink_test]
    pub async fn upload_zero_byte_chunk() -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let bs_server = make_bytestream_server(store_manager.as_ref())?;
        let store_owned = store_manager.get_store("main_cas").unwrap();
        let store = Pin::new(store_owned.as_ref());

        async fn setup_stream(
            bs_server: ByteStreamServer,
        ) -> Result<
            (
                Sender,
                JoinHandleDropGuard<(
                    Result<Response<WriteResponse>, tonic::Status>,
                    ByteStreamServer,
                )>,
            ),
            Error,
        > {
            let (tx, body) = Body::channel();
            let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
            // Note: This is an undocumented function.
            let stream = Streaming::new_request(
                codec.decoder(),
                body,
                Some(CompressionEncoding::Gzip),
                None,
            );

            let join_handle = spawn!("upload_zero_byte_chunk_write_stream", async move {
                let response_future = bs_server.write(Request::new(stream));
                (response_future.await, bs_server)
            });
            Ok((tx, join_handle))
        }
        let (mut tx, join_handle) = setup_stream(bs_server).await?;

        let resource_name = format!(
            "{}/uploads/{}/blobs/{}/{}",
            INSTANCE_NAME,
            "4dcec57e-1389-4ab5-b188-4a59f22ceb4b", // Randomly generated.
            HASH1,
            0
        );
        let write_request = WriteRequest {
            resource_name,
            write_offset: 0,
            finish_write: true,
            data: vec![].into(),
        };

        {
            // Write our zero byte data.
            tx.send_data(encode_stream_proto(&write_request)?).await?;
            // Wait for stream to finish.
            join_handle.await?.0?;
        }
        {
            // Check to make sure our store recorded the data properly.
            let data = store
                .get_part_unchunked(DigestInfo::try_new(HASH1, 0)?, 0, None)
                .await?;
            assert_eq!(data, "", "Expected data to exist and be empty");
        }
        Ok(())
    }

    #[nativelink_test]
    pub async fn disallow_negative_write_offset() -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let bs_server = make_bytestream_server(store_manager.as_ref())?;

        async fn setup_stream(
            bs_server: ByteStreamServer,
        ) -> Result<
            (
                Sender,
                JoinHandleDropGuard<(
                    Result<Response<WriteResponse>, tonic::Status>,
                    ByteStreamServer,
                )>,
            ),
            Error,
        > {
            let (tx, body) = Body::channel();
            let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
            // Note: This is an undocumented function.
            let stream = Streaming::new_request(
                codec.decoder(),
                body,
                Some(CompressionEncoding::Gzip),
                None,
            );

            let join_handle = spawn!("disallow_negative_write_offset_write_stream", async move {
                let response_future = bs_server.write(Request::new(stream));
                (response_future.await, bs_server)
            });
            Ok((tx, join_handle))
        }
        let (mut tx, join_handle) = setup_stream(bs_server).await?;

        let resource_name = format!(
            "{}/uploads/{}/blobs/{}/{}",
            INSTANCE_NAME,
            "4dcec57e-1389-4ab5-b188-4a59f22ceb4b", // Randomly generated.
            HASH1,
            0
        );
        let write_request = WriteRequest {
            resource_name,
            write_offset: -1,
            finish_write: true,
            data: vec![].into(),
        };

        {
            // Write our zero byte data.
            tx.send_data(encode_stream_proto(&write_request)?).await?;
            // Expect the write command to fail.
            assert!(join_handle.await?.0.is_err());
        }
        Ok(())
    }

    #[nativelink_test]
    pub async fn out_of_sequence_write() -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let bs_server = make_bytestream_server(store_manager.as_ref())?;

        async fn setup_stream(
            bs_server: ByteStreamServer,
        ) -> Result<
            (
                Sender,
                JoinHandleDropGuard<(
                    Result<Response<WriteResponse>, tonic::Status>,
                    ByteStreamServer,
                )>,
            ),
            Error,
        > {
            let (tx, body) = Body::channel();
            let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
            // Note: This is an undocumented function.
            let stream = Streaming::new_request(
                codec.decoder(),
                body,
                Some(CompressionEncoding::Gzip),
                None,
            );

            let join_handle = spawn!("out_of_sequence_write_write_stream", async move {
                let response_future = bs_server.write(Request::new(stream));
                (response_future.await, bs_server)
            });
            Ok((tx, join_handle))
        }
        let (mut tx, join_handle) = setup_stream(bs_server).await?;

        let resource_name = format!(
            "{}/uploads/{}/blobs/{}/{}",
            INSTANCE_NAME,
            "4dcec57e-1389-4ab5-b188-4a59f22ceb4b", // Randomly generated.
            HASH1,
            100
        );
        let write_request = WriteRequest {
            resource_name,
            write_offset: 10,
            finish_write: false,
            data: "TEST".into(),
        };

        {
            // Write our zero byte data.
            tx.send_data(encode_stream_proto(&write_request)?).await?;
            // Expect the write command to fail.
            assert!(join_handle.await?.0.is_err());
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod read_tests {
    use nativelink_proto::google::bytestream::{
        byte_stream_server::ByteStream, // Needed to call .read().
        ReadRequest,
    };
    use pretty_assertions::assert_eq; // Must be declared in every module.
    use tokio_stream::StreamExt;

    use super::*;

    #[nativelink_test]
    pub async fn chunked_stream_reads_small_set_of_data() -> Result<(), Box<dyn std::error::Error>>
    {
        let store_manager = make_store_manager().await?;
        let bs_server = make_bytestream_server(store_manager.as_ref())?;
        let store_owned = store_manager.get_store("main_cas").unwrap();

        let store = Pin::new(store_owned.as_ref());

        const VALUE1: &str = "12456789abcdefghijk";

        let digest = DigestInfo::try_new(HASH1, VALUE1.len())?;
        store.update_oneshot(digest, VALUE1.into()).await?;

        let read_request = ReadRequest {
            resource_name: format!("{}/blobs/{}/{}", INSTANCE_NAME, HASH1, VALUE1.len()),
            read_offset: 0,
            read_limit: VALUE1.len() as i64,
        };
        let mut read_stream = bs_server
            .read(Request::new(read_request))
            .await?
            .into_inner();
        {
            let mut roundtrip_data = Vec::with_capacity(VALUE1.len());
            while let Some(result_read_response) = read_stream.next().await {
                roundtrip_data.append(&mut result_read_response?.data.to_vec());
            }
            assert_eq!(
                roundtrip_data,
                VALUE1.as_bytes(),
                "Expected response to match what is in store"
            );
        }
        Ok(())
    }

    #[nativelink_test]
    pub async fn chunked_stream_reads_10mb_of_data() -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let bs_server = make_bytestream_server(store_manager.as_ref())?;
        let store_owned = store_manager.get_store("main_cas").unwrap();

        let store = Pin::new(store_owned.as_ref());

        const DATA_SIZE: usize = 10_000_000;
        let mut raw_data = vec![41u8; DATA_SIZE];
        // Change just a few bits to ensure we don't get same packet
        // over and over.
        raw_data[5] = 42u8;
        raw_data[DATA_SIZE - 2] = 43u8;

        let data_len = raw_data.len();
        let digest = DigestInfo::try_new(HASH1, data_len)?;
        store
            .update_oneshot(digest, raw_data.clone().into())
            .await?;

        let read_request = ReadRequest {
            resource_name: format!("{}/blobs/{}/{}", INSTANCE_NAME, HASH1, raw_data.len()),
            read_offset: 0,
            read_limit: raw_data.len() as i64,
        };
        let mut read_stream = bs_server
            .read(Request::new(read_request))
            .await?
            .into_inner();
        {
            let mut roundtrip_data = Vec::with_capacity(raw_data.len());
            assert!(
                !raw_data.is_empty(),
                "Expected at least one byte to be sent"
            );
            while let Some(result_read_response) = read_stream.next().await {
                roundtrip_data.append(&mut result_read_response?.data.to_vec());
            }
            assert_eq!(
                roundtrip_data, raw_data,
                "Expected response to match what is in store"
            );
        }
        Ok(())
    }

    /// A bug was found in early development where we could deadlock when reading a stream if the
    /// store backend resulted in an error. This was because we were not shutting down the stream
    /// when on the backend store error which caused the AsyncReader to block forever because the
    /// stream was never shutdown.
    #[nativelink_test]
    pub async fn read_with_not_found_does_not_deadlock() -> Result<(), Error> {
        let store_manager = make_store_manager()
            .await
            .err_tip(|| "Couldn't get store manager")?;
        let mut read_stream = {
            let bs_server =
                make_bytestream_server(store_manager.as_ref()).err_tip(|| "Couldn't make store")?;
            let read_request = ReadRequest {
                resource_name: format!(
                    "{}/blobs/{}/{}",
                    INSTANCE_NAME,
                    HASH1,
                    55, // Dummy value
                ),
                read_offset: 0,
                read_limit: 55,
            };
            // This should fail because there's no data in the store yet.
            bs_server
                .read(Request::new(read_request))
                .await
                .err_tip(|| "Couldn't send read")?
                .into_inner()
        };
        // We need to give a chance for the other spawns to do some work before we poll.
        yield_now().await;
        {
            let result_fut = read_stream.next();

            let result = result_fut.await.err_tip(|| "Expected result to be ready")?;
            let expected_err_str = concat!(
                "status: NotFound, message: \"Hash 0123456789abcdef000000000000000000000000000000000123456789abcdef ",
                "not found\", details: [], metadata: MetadataMap { headers: {} }",
            );
            assert_eq!(
                Error::from(result.unwrap_err()),
                make_err!(Code::NotFound, "{}", expected_err_str),
                "Expected error data to match"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod query_tests {
    use nativelink_proto::google::bytestream::byte_stream_server::ByteStream;
    use nativelink_proto::google::bytestream::{
        QueryWriteStatusRequest, QueryWriteStatusResponse, WriteRequest,
    };
    use pretty_assertions::assert_eq; // Must be declared in every module.
    use tonic::codec::{Codec, CompressionEncoding, ProstCodec};
    use tonic::transport::Body;
    use tonic::Streaming;

    use super::*;

    #[nativelink_test]
    pub async fn test_query_write_status_smoke_test() -> Result<(), Box<dyn std::error::Error>> {
        let store_manager = make_store_manager().await?;
        let bs_server = Arc::new(make_bytestream_server(store_manager.as_ref())?);

        let raw_data = "12456789abcdefghijk".as_bytes();
        let resource_name = format!(
            "{}/uploads/{}/blobs/{}/{}",
            INSTANCE_NAME,
            "4dcec57e-1389-4ab5-b188-4a59f22ceb4b", // Randomly generated.
            HASH1,
            raw_data.len()
        );

        {
            let response = bs_server
                .query_write_status(Request::new(QueryWriteStatusRequest {
                    resource_name: resource_name.clone(),
                }))
                .await;
            assert_eq!(response.is_err(), true);
            let expected_err = make_err!(
                Code::NotFound,
                "{}{}",
                "status: NotFound, message: \"not found : Failed on query_write_status() ",
                "command\", details: [], metadata: MetadataMap { headers: {} }"
            );
            assert_eq!(Into::<Error>::into(response.unwrap_err()), expected_err);
        }

        // Setup stream.
        let (mut tx, join_handle) = {
            let (tx, body) = Body::channel();
            let mut codec = ProstCodec::<WriteRequest, WriteRequest>::default();
            // Note: This is an undocumented function.
            let stream = Streaming::new_request(
                codec.decoder(),
                body,
                Some(CompressionEncoding::Gzip),
                None,
            );

            let bs_server_clone = bs_server.clone();
            let join_handle = spawn!("query_write_status_smoke_test_write_stream", async move {
                let response_future = bs_server_clone.write(Request::new(stream));
                response_future.await
            });
            (tx, join_handle)
        };

        const BYTE_SPLIT_OFFSET: usize = 8;

        let mut write_request = WriteRequest {
            resource_name: resource_name.clone(),
            write_offset: 0,
            finish_write: false,
            data: vec![].into(),
        };

        // Write first chunk of data.
        write_request.write_offset = 0;
        write_request.data = raw_data[..BYTE_SPLIT_OFFSET].into();
        tx.send_data(encode_stream_proto(&write_request)?).await?;

        {
            // Check to see if our request is active.
            tokio::task::yield_now().await;
            let data = bs_server
                .query_write_status(Request::new(QueryWriteStatusRequest {
                    resource_name: resource_name.clone(),
                }))
                .await?;
            assert_eq!(
                data.into_inner(),
                QueryWriteStatusResponse {
                    committed_size: write_request.data.len() as i64,
                    complete: false,
                }
            );
        }

        // Finish writing our data.
        write_request.write_offset = BYTE_SPLIT_OFFSET as i64;
        write_request.data = raw_data[BYTE_SPLIT_OFFSET..].into();
        write_request.finish_write = true;
        tx.send_data(encode_stream_proto(&write_request)?).await?;

        {
            // Now that it's done uploading, ensure it returns a success when requested again.
            tokio::task::yield_now().await;
            let data = bs_server
                .query_write_status(Request::new(QueryWriteStatusRequest { resource_name }))
                .await?;
            assert_eq!(
                data.into_inner(),
                QueryWriteStatusResponse {
                    committed_size: raw_data.len() as i64,
                    complete: true,
                }
            );
        }
        join_handle
            .await
            .err_tip(|| "Failed to join")?
            .err_tip(|| "Failed write")?;
        Ok(())
    }
}
