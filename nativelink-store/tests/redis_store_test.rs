// Copyright 2024 The NativeLink Authors. All rights reserved.
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

use bytes::Bytes;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_store::cas_utils::ZERO_BYTE_DIGESTS;
use nativelink_store::redis_store::{LazyConnection, RedisStore};
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::Store;
use redis::{Pipeline, RedisError};
use redis_test::{MockCmd, MockRedisConnection};

const VALID_HASH1: &str = "3031323334353637383961626364656630303030303030303030303030303030";
const TEMP_UUID: &str = "temp-550e8400-e29b-41d4-a716-446655440000";

type Command = str;
type Arg = str;
type RedisResult<'a> = Result<&'a [redis::Value], RedisError>;

fn mock_uuid_generator() -> String {
    uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")
        .unwrap()
        .to_string()
}

#[cfg(test)]
mod redis_store_tests {
    use nativelink_util::buf_channel::make_buf_channel_pair;
    use nativelink_util::store_trait::UploadSizeInfo;

    use super::*;

    struct MockRedisConnectionBuilder {
        mock_cmds: Vec<MockCmd>,
    }

    impl MockRedisConnectionBuilder {
        fn new() -> Self {
            MockRedisConnectionBuilder { mock_cmds: vec![] }
        }

        fn pipe(mut self, inputs: &[(&Command, &[&Arg], RedisResult)]) -> Self {
            let mut pipe = Pipeline::new();
            pipe.atomic();
            let mut res_vec = vec![];
            for (cmd, args, result) in inputs {
                let mut command = redis::cmd(cmd);
                for arg in args.iter() {
                    command.arg(arg);
                }
                for res in result.as_ref().unwrap().iter() {
                    res_vec.push(res.clone());
                }
                pipe.add_command(command);
            }
            self.mock_cmds.push(MockCmd::with_values(pipe, Ok(res_vec)));
            self
        }

        fn cmd(mut self, cmd: &Command, args: &[&Arg], result: Result<&str, RedisError>) -> Self {
            let mut cmd = redis::cmd(cmd);
            for arg in args {
                cmd.arg(arg);
            }
            self.mock_cmds.push(MockCmd::new(cmd, result));
            self
        }

        fn build(self) -> MockRedisConnection {
            MockRedisConnection::new(self.mock_cmds)
        }
    }

    #[nativelink_test]
    async fn upload_and_get_data() -> Result<(), Error> {
        let data = Bytes::from_static(b"14");

        let digest = DigestInfo::try_new(VALID_HASH1, 2)?;
        let packed_hash_hex = format!("{}-{}", digest.hash_str(), digest.size_bytes);

        let chunk_data = "14";

        let redis_connection = MockRedisConnectionBuilder::new()
            .pipe(&[("APPEND", &[TEMP_UUID, chunk_data], Ok(&[redis::Value::Nil]))])
            .cmd("APPEND", &[&packed_hash_hex, ""], Ok(""))
            .pipe(&[(
                "RENAME",
                &[TEMP_UUID, &packed_hash_hex],
                Ok(&[redis::Value::Nil]),
            )])
            .cmd("STRLEN", &[&packed_hash_hex], Ok("1"))
            .cmd("GETRANGE", &[&packed_hash_hex, "0", "1"], Ok("14"))
            .build();

        let store = RedisStore::new_with_conn_and_name_generator(
            LazyConnection::Connection(Ok(redis_connection)),
            nativelink_config::stores::Retry {
                max_retries: 1024,
                delay: 0.,
                jitter: 0.,
                ..Default::default()
            },
            Some(1024 * 1024),
            mock_uuid_generator,
        );
        let pinned_store: Pin<&RedisStore<MockRedisConnection>> = Pin::new(&store);

        pinned_store.update_oneshot(digest, data.clone()).await?;

        let result = pinned_store.has(digest).await?;
        assert!(
            result.is_some(),
            "Expected redis store to have hash: {VALID_HASH1}",
        );

        let result = pinned_store
            .get_part_unchunked(digest, 0, Some(data.clone().len()))
            .await?;

        assert_eq!(result, data, "Expected redis store to have updated value",);

        Ok(())
    }

    #[nativelink_test]
    async fn upload_empty_data() -> Result<(), Error> {
        let data = Bytes::from_static(b"");

        let digest = ZERO_BYTE_DIGESTS[0];

        let redis_connection = MockRedisConnectionBuilder::new().build();

        let store = RedisStore::new_with_conn_and_name_generator(
            LazyConnection::Connection(Ok(redis_connection)),
            nativelink_config::stores::Retry {
                max_retries: 1024,
                delay: 0.,
                jitter: 0.,
                ..Default::default()
            },
            Some(1024 * 1024),
            mock_uuid_generator,
        );
        let pinned_store: Pin<&RedisStore<MockRedisConnection>> = Pin::new(&store);

        pinned_store.update_oneshot(digest, data).await?;

        let result = pinned_store.has(digest).await?;
        assert!(
            result.is_some(),
            "Expected redis store to have hash: {VALID_HASH1}",
        );

        Ok(())
    }

    #[nativelink_test]
    async fn test_uploading_large_data() -> Result<(), Error> {
        // Requires multiple chunks as data is larger than 64K
        let data: Bytes = Bytes::from(vec![0u8; 65 * 1024]);

        let digest = DigestInfo::try_new(VALID_HASH1, 1)?;
        let packed_hash_hex = format!("{}-{}", digest.hash_str(), digest.size_bytes);

        let chunk_data = std::str::from_utf8(&data).unwrap().to_string();

        let redis_connection = MockRedisConnectionBuilder::new()
            .pipe(&[(
                "APPEND",
                &[TEMP_UUID, &chunk_data],
                Ok(&[redis::Value::Nil]),
            )])
            .cmd(
                "APPEND",
                &[&packed_hash_hex, ""],
                Ok(&hex::encode(&data[..])),
            )
            .pipe(&[(
                "RENAME",
                &[TEMP_UUID, &packed_hash_hex],
                Ok(&[redis::Value::Nil]),
            )])
            .cmd("STRLEN", &[&packed_hash_hex], Ok("1"))
            .cmd(
                "GETRANGE",
                &[&packed_hash_hex, "0", "65535"],
                Ok(&hex::encode(&data[..])),
            )
            .cmd(
                "GETRANGE",
                &[&packed_hash_hex, "65535", "65560"],
                Ok(&hex::encode(&data[..])),
            )
            .build();

        let store = RedisStore::new_with_conn_and_name_generator(
            LazyConnection::Connection(Ok(redis_connection)),
            nativelink_config::stores::Retry {
                max_retries: 1024,
                delay: 0.,
                jitter: 0.,
                ..Default::default()
            },
            Some(1024 * 1024),
            mock_uuid_generator,
        );
        let pinned_store: Pin<&RedisStore<MockRedisConnection>> = Pin::new(&store);

        pinned_store.update_oneshot(digest, data.clone()).await?;

        let result = pinned_store.has(digest).await?;
        assert!(
            result.is_some(),
            "Expected redis store to have hash: {VALID_HASH1}",
        );

        let get_result: Bytes = pinned_store
            .get_part_unchunked(digest, 0, Some(data.clone().len()))
            .await?;

        assert_eq!(
            hex::encode(get_result).len(),
            hex::encode(data.clone()).len(),
            "Expected redis store to have updated value",
        );

        Ok(())
    }

    #[nativelink_test]
    async fn yield_between_sending_packets_in_update() -> Result<(), Error> {
        let data = Bytes::from(vec![0u8; 10 * 1024]);
        let data_p1 = Bytes::from(vec![0u8; 6 * 1024]);
        let data_p2 = Bytes::from(vec![0u8; 4 * 1024]);

        let digest = DigestInfo::try_new(VALID_HASH1, 2)?;
        let packed_hash_hex = format!("{}-{}", digest.hash_str(), digest.size_bytes);

        let redis_connection = MockRedisConnectionBuilder::new()
            .pipe(&[
                (
                    "APPEND",
                    &[TEMP_UUID, std::str::from_utf8(&data_p1).unwrap()],
                    Ok(&[redis::Value::Nil]),
                ),
                (
                    "APPEND",
                    &[TEMP_UUID, std::str::from_utf8(&data_p2).unwrap()],
                    Ok(&[redis::Value::Nil]),
                ),
            ])
            .cmd("APPEND", &[&packed_hash_hex, ""], Ok(""))
            .pipe(&[(
                "RENAME",
                &[TEMP_UUID, &packed_hash_hex],
                Ok(&[redis::Value::Nil]),
            )])
            .cmd("STRLEN", &[&packed_hash_hex], Ok("1"))
            .cmd(
                "GETRANGE",
                &[&packed_hash_hex, "0", "10239"],
                Ok(std::str::from_utf8(&data).unwrap()),
            )
            .build();

        let store = RedisStore::new_with_conn_and_name_generator(
            LazyConnection::Connection(Ok(redis_connection)),
            nativelink_config::stores::Retry {
                max_retries: 1024,
                delay: 0.,
                jitter: 0.,
                ..Default::default()
            },
            Some(1024 * 1024),
            mock_uuid_generator,
        );
        let pinned_store: Pin<&RedisStore<MockRedisConnection>> = Pin::new(&store);

        let (mut tx, rx) = make_buf_channel_pair();
        tx.send(data_p1).await?;
        tokio::task::yield_now().await;
        tx.send(data_p2).await?;
        tx.send_eof()?;
        pinned_store
            .update(digest, rx, UploadSizeInfo::ExactSize(data.len()))
            .await?;

        let result = pinned_store.has(digest).await?;
        assert!(
            result.is_some(),
            "Expected redis store to have hash: {VALID_HASH1}",
        );

        let result = pinned_store
            .get_part_unchunked(digest, 0, Some(data.clone().len()))
            .await?;

        assert_eq!(result, data, "Expected redis store to have updated value",);

        Ok(())
    }
}
