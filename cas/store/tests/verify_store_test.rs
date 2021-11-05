// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;

use tokio::io::AsyncWriteExt;

#[cfg(test)]
mod verify_store_tests {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    use async_fixed_buffer::AsyncFixedBuf;
    use common::DigestInfo;
    use config;
    use error::{Error, ResultExt};
    use memory_store::MemoryStore;
    use traits::{StoreTrait, UploadSizeInfo};
    use verify_store::VerifyStore;

    const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

    #[tokio::test]
    async fn verify_size_false_passes_on_update() -> Result<(), Error> {
        let inner_store = Arc::new(MemoryStore::new(&config::backends::MemoryStore::default()));
        let store_owned = VerifyStore::new(
            &config::backends::VerifyStore {
                backend: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
                verify_size: false,
                verify_hash: false,
            },
            inner_store.clone(),
        );
        let store = Pin::new(&store_owned);

        const VALUE1: &str = "123";
        let digest = DigestInfo::try_new(&VALID_HASH1, 100).unwrap();
        let result = store
            .update(
                digest.clone(),
                Box::new(Cursor::new(VALUE1)),
                UploadSizeInfo::ExactSize(3),
            )
            .await;
        assert_eq!(
            result,
            Ok(()),
            "Should have succeeded when verify_size = false, got: {:?}",
            result
        );
        assert_eq!(
            Pin::new(inner_store.as_ref()).has(digest).await?,
            true,
            "Expected data to exist in store after update"
        );
        Ok(())
    }

    #[tokio::test]
    async fn verify_size_true_fails_on_update() -> Result<(), Error> {
        let inner_store = Arc::new(MemoryStore::new(&config::backends::MemoryStore::default()));
        let store_owned = VerifyStore::new(
            &config::backends::VerifyStore {
                backend: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
                verify_size: true,
                verify_hash: false,
            },
            inner_store.clone(),
        );
        let store = Pin::new(&store_owned);

        const VALUE1: &str = "123";
        let digest = DigestInfo::try_new(&VALID_HASH1, 100).unwrap();
        let result = store
            .update(
                digest.clone(),
                Box::new(Cursor::new(VALUE1)),
                UploadSizeInfo::ExactSize(100),
            )
            .await;
        assert!(result.is_err(), "Expected error, got: {:?}", &result);
        const EXPECTED_ERR: &str = "Expected size 100 but got size 3 on insert";
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains(EXPECTED_ERR),
            "Error should contain '{}', got: {:?}",
            EXPECTED_ERR,
            err
        );
        assert_eq!(
            Pin::new(inner_store.as_ref()).has(digest).await?,
            false,
            "Expected data to not exist in store after update"
        );
        Ok(())
    }

    #[tokio::test]
    async fn verify_size_true_suceeds_on_update() -> Result<(), Error> {
        let inner_store = Arc::new(MemoryStore::new(&config::backends::MemoryStore::default()));
        let store_owned = VerifyStore::new(
            &config::backends::VerifyStore {
                backend: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
                verify_size: true,
                verify_hash: false,
            },
            inner_store.clone(),
        );
        let store = Pin::new(&store_owned);

        const VALUE1: &str = "123";
        let digest = DigestInfo::try_new(&VALID_HASH1, 3).unwrap();
        let result = store
            .update(
                digest.clone(),
                Box::new(Cursor::new(VALUE1)),
                UploadSizeInfo::ExactSize(3),
            )
            .await;
        assert_eq!(result, Ok(()), "Expected success, got: {:?}", result);
        assert_eq!(
            Pin::new(inner_store.as_ref()).has(digest).await?,
            true,
            "Expected data to exist in store after update"
        );
        Ok(())
    }

    #[tokio::test]
    async fn verify_size_true_suceeds_on_multi_chunk_stream_update() -> Result<(), Error> {
        let inner_store = Arc::new(MemoryStore::new(&config::backends::MemoryStore::default()));
        let store_owned = VerifyStore::new(
            &config::backends::VerifyStore {
                backend: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
                verify_size: true,
                verify_hash: false,
            },
            inner_store.clone(),
        );

        let raw_fixed_buffer = AsyncFixedBuf::new(vec![0u8; 100].into_boxed_slice());
        let (rx, mut tx) = tokio::io::split(raw_fixed_buffer);

        let digest = DigestInfo::try_new(&VALID_HASH1, 6).unwrap();
        let digest_clone = digest.clone();
        let future = tokio::spawn(async move {
            Pin::new(&store_owned)
                .update(digest_clone, Box::new(rx), UploadSizeInfo::ExactSize(6))
                .await
        });
        tx.write_all("foo".as_bytes()).await?;
        tx.flush().await?;
        tx.write_all("bar".as_bytes()).await?;
        tx.write(&[]).await?;
        let result = future.await.err_tip(|| "Failed to join spawn future")?;
        assert_eq!(result, Ok(()), "Expected success, got: {:?}", result);
        assert_eq!(
            Pin::new(inner_store.as_ref()).has(digest).await?,
            true,
            "Expected data to exist in store after update"
        );
        Ok(())
    }

    #[tokio::test]
    async fn verify_hash_true_suceeds_on_update() -> Result<(), Error> {
        let inner_store = Arc::new(MemoryStore::new(&config::backends::MemoryStore::default()));
        let store_owned = VerifyStore::new(
            &config::backends::VerifyStore {
                backend: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
                verify_size: false,
                verify_hash: true,
            },
            inner_store.clone(),
        );
        let store = Pin::new(&store_owned);

        /// This value is sha256("123").
        const HASH: &str = "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3";
        const VALUE: &str = "123";
        let digest = DigestInfo::try_new(&HASH, 3).unwrap();
        let result = store
            .update(
                digest.clone(),
                Box::new(Cursor::new(VALUE)),
                UploadSizeInfo::ExactSize(3),
            )
            .await;
        assert_eq!(result, Ok(()), "Expected success, got: {:?}", result);
        assert_eq!(
            Pin::new(inner_store.as_ref()).has(digest).await?,
            true,
            "Expected data to exist in store after update"
        );
        Ok(())
    }

    #[tokio::test]
    async fn verify_hash_true_fails_on_update() -> Result<(), Error> {
        let inner_store = Arc::new(MemoryStore::new(&config::backends::MemoryStore::default()));
        let store_owned = VerifyStore::new(
            &config::backends::VerifyStore {
                backend: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
                verify_size: false,
                verify_hash: true,
            },
            inner_store.clone(),
        );
        let store = Pin::new(&store_owned);

        /// This value is sha256("12").
        const HASH: &str = "6b51d431df5d7f141cbececcf79edf3dd861c3b4069f0b11661a3eefacbba918";
        const VALUE: &str = "123";
        let digest = DigestInfo::try_new(&HASH, 3).unwrap();
        let result = store
            .update(
                digest.clone(),
                Box::new(Cursor::new(VALUE)),
                UploadSizeInfo::ExactSize(3),
            )
            .await;
        let err = result.unwrap_err().to_string();
        const ACTUAL_HASH: &str = "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3";
        let expected_err = format!("Hashes do not match, got: {} but digest hash was {}", HASH, ACTUAL_HASH);
        assert!(
            err.contains(&expected_err),
            "Error should contain '{}', got: {:?}",
            expected_err,
            err
        );
        assert_eq!(
            Pin::new(inner_store.as_ref()).has(digest).await?,
            false,
            "Expected data to not exist in store after update"
        );
        Ok(())
    }
}
