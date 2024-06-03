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

use futures::try_join;

#[cfg(test)]
mod verify_store_tests {
    use nativelink_error::{Error, ResultExt};
    use nativelink_macro::nativelink_test;
    use nativelink_store::memory_store::MemoryStore;
    use nativelink_store::verify_store::VerifyStore;
    use nativelink_util::buf_channel::make_buf_channel_pair;
    use nativelink_util::common::DigestInfo;
    use nativelink_util::digest_hasher::{make_ctx_for_hash_func, DigestHasherFunc};
    use nativelink_util::spawn;
    use nativelink_util::store_trait::{Store, UploadSizeInfo};
    use pretty_assertions::assert_eq; // Must be declared in every module.
    use tracing::info_span;

    use super::*;

    const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

    #[nativelink_test]
    async fn verify_size_false_passes_on_update() -> Result<(), Error> {
        let inner_store = Arc::new(MemoryStore::new(
            &nativelink_config::stores::MemoryStore::default(),
        ));
        let store_owned = VerifyStore::new(
            &nativelink_config::stores::VerifyStore {
                backend: nativelink_config::stores::StoreConfigOptions::memory(
                    nativelink_config::stores::MemoryStore::default(),
                ),
                verify_size: false,
                verify_hash: false,
            },
            inner_store.clone(),
        );
        let store = Pin::new(&store_owned);

        const VALUE1: &str = "123";
        let digest = DigestInfo::try_new(VALID_HASH1, 100).unwrap();
        let result = store.update_oneshot(digest, VALUE1.into()).await;
        assert_eq!(
            result,
            Ok(()),
            "Should have succeeded when verify_size = false, got: {:?}",
            result
        );
        assert_eq!(
            Pin::new(inner_store.as_ref()).has(digest).await,
            Ok(Some(VALUE1.len())),
            "Expected data to exist in store after update"
        );
        Ok(())
    }

    #[nativelink_test]
    async fn verify_size_true_fails_on_update() -> Result<(), Error> {
        let inner_store = Arc::new(MemoryStore::new(
            &nativelink_config::stores::MemoryStore::default(),
        ));
        let store_owned = VerifyStore::new(
            &nativelink_config::stores::VerifyStore {
                backend: nativelink_config::stores::StoreConfigOptions::memory(
                    nativelink_config::stores::MemoryStore::default(),
                ),
                verify_size: true,
                verify_hash: false,
            },
            inner_store.clone(),
        );
        let store = Pin::new(&store_owned);

        const VALUE1: &str = "123";
        let digest = DigestInfo::try_new(VALID_HASH1, 100).unwrap();
        let (mut tx, rx) = make_buf_channel_pair();
        let send_fut = async move {
            tx.send(VALUE1.into()).await?;
            tx.send_eof()
        };
        let result = try_join!(
            send_fut,
            store.update(digest, rx, UploadSizeInfo::ExactSize(100))
        );
        assert!(result.is_err(), "Expected error, got: {:?}", &result);
        const EXPECTED_ERR: &str = "Expected size 100 but got size 3 on insert";
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains(EXPECTED_ERR),
            "Error should contain '{EXPECTED_ERR}', got: {err:?}"
        );
        assert_eq!(
            Pin::new(inner_store.as_ref()).has(digest).await,
            Ok(None),
            "Expected data to not exist in store after update"
        );
        Ok(())
    }

    #[nativelink_test]
    async fn verify_size_true_suceeds_on_update() -> Result<(), Error> {
        let inner_store = Arc::new(MemoryStore::new(
            &nativelink_config::stores::MemoryStore::default(),
        ));
        let store_owned = VerifyStore::new(
            &nativelink_config::stores::VerifyStore {
                backend: nativelink_config::stores::StoreConfigOptions::memory(
                    nativelink_config::stores::MemoryStore::default(),
                ),
                verify_size: true,
                verify_hash: false,
            },
            inner_store.clone(),
        );
        let store = Pin::new(&store_owned);

        const VALUE1: &str = "123";
        let digest = DigestInfo::try_new(VALID_HASH1, 3).unwrap();
        let result = store.update_oneshot(digest, VALUE1.into()).await;
        assert_eq!(result, Ok(()), "Expected success, got: {:?}", result);
        assert_eq!(
            Pin::new(inner_store.as_ref()).has(digest).await,
            Ok(Some(VALUE1.len())),
            "Expected data to exist in store after update"
        );
        Ok(())
    }

    #[nativelink_test]
    async fn verify_size_true_suceeds_on_multi_chunk_stream_update() -> Result<(), Error> {
        let inner_store = Arc::new(MemoryStore::new(
            &nativelink_config::stores::MemoryStore::default(),
        ));
        let store_owned = VerifyStore::new(
            &nativelink_config::stores::VerifyStore {
                backend: nativelink_config::stores::StoreConfigOptions::memory(
                    nativelink_config::stores::MemoryStore::default(),
                ),
                verify_size: true,
                verify_hash: false,
            },
            inner_store.clone(),
        );

        let (mut tx, rx) = make_buf_channel_pair();

        let digest = DigestInfo::try_new(VALID_HASH1, 6).unwrap();
        let digest_clone = digest;
        let future = spawn!(
            "verify_size_true_suceeds_on_multi_chunk_stream_update",
            async move {
                Pin::new(&store_owned)
                    .update(digest_clone, rx, UploadSizeInfo::ExactSize(6))
                    .await
            },
        );
        tx.send("foo".into()).await?;
        tx.send("bar".into()).await?;
        tx.send_eof()?;
        let result = future.await.err_tip(|| "Failed to join spawn future")?;
        assert_eq!(result, Ok(()), "Expected success, got: {:?}", result);
        assert_eq!(
            Pin::new(inner_store.as_ref()).has(digest).await,
            Ok(Some(6)),
            "Expected data to exist in store after update"
        );
        Ok(())
    }

    #[nativelink_test]
    async fn verify_sha256_hash_true_suceeds_on_update() -> Result<(), Error> {
        let inner_store = Arc::new(MemoryStore::new(
            &nativelink_config::stores::MemoryStore::default(),
        ));
        let store_owned = VerifyStore::new(
            &nativelink_config::stores::VerifyStore {
                backend: nativelink_config::stores::StoreConfigOptions::memory(
                    nativelink_config::stores::MemoryStore::default(),
                ),
                verify_size: false,
                verify_hash: true,
            },
            inner_store.clone(),
        );
        let store = Pin::new(&store_owned);

        /// This value is sha256("123").
        const HASH: &str = "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3";
        const VALUE: &str = "123";
        let digest = DigestInfo::try_new(HASH, 3).unwrap();
        let result = store.update_oneshot(digest, VALUE.into()).await;
        assert_eq!(result, Ok(()), "Expected success, got: {:?}", result);
        assert_eq!(
            Pin::new(inner_store.as_ref()).has(digest).await,
            Ok(Some(VALUE.len())),
            "Expected data to exist in store after update"
        );
        Ok(())
    }

    #[nativelink_test]
    async fn verify_sha256_hash_true_fails_on_update() -> Result<(), Error> {
        let inner_store = Arc::new(MemoryStore::new(
            &nativelink_config::stores::MemoryStore::default(),
        ));
        let store_owned = VerifyStore::new(
            &nativelink_config::stores::VerifyStore {
                backend: nativelink_config::stores::StoreConfigOptions::memory(
                    nativelink_config::stores::MemoryStore::default(),
                ),
                verify_size: false,
                verify_hash: true,
            },
            inner_store.clone(),
        );
        let store = Pin::new(&store_owned);

        /// This value is sha256("12").
        const HASH: &str = "6b51d431df5d7f141cbececcf79edf3dd861c3b4069f0b11661a3eefacbba918";
        const VALUE: &str = "123";
        let digest = DigestInfo::try_new(HASH, 3).unwrap();
        let result = store.update_oneshot(digest, VALUE.into()).await;
        let err = result.unwrap_err().to_string();
        const ACTUAL_HASH: &str =
            "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3";
        let expected_err =
            format!("Hashes do not match, got: {HASH} but digest hash was {ACTUAL_HASH}");
        assert!(
            err.contains(&expected_err),
            "Error should contain '{expected_err}', got: {err:?}"
        );
        assert_eq!(
            Pin::new(inner_store.as_ref()).has(digest).await,
            Ok(None),
            "Expected data to not exist in store after update"
        );
        Ok(())
    }

    #[nativelink_test]
    async fn verify_blake3_hash_true_suceeds_on_update() -> Result<(), Error> {
        let inner_store = Arc::new(MemoryStore::new(
            &nativelink_config::stores::MemoryStore::default(),
        ));
        let store_owned = VerifyStore::new(
            &nativelink_config::stores::VerifyStore {
                backend: nativelink_config::stores::StoreConfigOptions::memory(
                    nativelink_config::stores::MemoryStore::default(),
                ),
                verify_size: false,
                verify_hash: true,
            },
            inner_store.clone(),
        );
        let store = Pin::new(&store_owned);

        /// This value is blake3("123").
        const HASH: &str = "b3d4f8803f7e24b8f389b072e75477cdbcfbe074080fb5e500e53e26e054158e";
        const VALUE: &str = "123";
        let digest = DigestInfo::try_new(HASH, 3).unwrap();
        let result = make_ctx_for_hash_func(DigestHasherFunc::Blake3)?
            .wrap_async(
                info_span!("update_oneshot"),
                store.update_oneshot(digest, VALUE.into()),
            )
            .await;

        assert_eq!(result, Ok(()), "Expected success, got: {:?}", result);
        assert_eq!(
            Pin::new(inner_store.as_ref()).has(digest).await,
            Ok(Some(VALUE.len())),
            "Expected data to exist in store after update"
        );
        Ok(())
    }

    #[nativelink_test]
    async fn verify_blake3_hash_true_fails_on_update() -> Result<(), Error> {
        let inner_store = Arc::new(MemoryStore::new(
            &nativelink_config::stores::MemoryStore::default(),
        ));
        let store_owned = VerifyStore::new(
            &nativelink_config::stores::VerifyStore {
                backend: nativelink_config::stores::StoreConfigOptions::memory(
                    nativelink_config::stores::MemoryStore::default(),
                ),
                verify_size: false,
                verify_hash: true,
            },
            inner_store.clone(),
        );
        let store = Pin::new(&store_owned);

        /// This value is blake3("12").
        const HASH: &str = "b944a0a3b20cf5927e594ff306d256d16cd5b0ba3e27b3285f40d7ef5e19695b";
        const VALUE: &str = "123";
        let digest = DigestInfo::try_new(HASH, 3).unwrap();

        let result = make_ctx_for_hash_func(DigestHasherFunc::Blake3)?
            .wrap_async(
                info_span!("update_oneshot"),
                store.update_oneshot(digest, VALUE.into()),
            )
            .await;

        // let result = store.update_oneshot(digest, VALUE.into()).await;
        let err = result.unwrap_err().to_string();
        const ACTUAL_HASH: &str =
            "b3d4f8803f7e24b8f389b072e75477cdbcfbe074080fb5e500e53e26e054158e";
        let expected_err =
            format!("Hashes do not match, got: {HASH} but digest hash was {ACTUAL_HASH}");
        assert!(
            err.contains(&expected_err),
            "Error should contain '{expected_err}', got: {err:?}"
        );
        assert_eq!(
            Pin::new(inner_store.as_ref()).has(digest).await,
            Ok(None),
            "Expected data to not exist in store after update"
        );
        Ok(())
    }
}
