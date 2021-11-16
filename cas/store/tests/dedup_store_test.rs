// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::pin::Pin;
use std::sync::Arc;

use common::DigestInfo;
use config;
use dedup_store::DedupStore;
use error::{Code, Error, ResultExt};
use memory_store::MemoryStore;
use rand::{rngs::SmallRng, Rng, SeedableRng};
use traits::StoreTrait;

fn make_default_config() -> config::backends::DedupStore {
    config::backends::DedupStore {
        index_store: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
        content_store: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
        min_size: 8 * 1024,
        normal_size: 32 * 1024,
        max_size: 128 * 1024,
        max_concurrent_fetch_per_get: 10,
    }
}

fn make_random_data(sz: usize) -> Vec<u8> {
    let mut value = vec![0u8; sz];
    let mut rng = SmallRng::seed_from_u64(1);
    rng.fill(&mut value[..]);
    value
}

#[cfg(test)]
mod dedup_store_tests {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
    const MEGABYTE_SZ: usize = 1024 * 1024;

    #[tokio::test]
    async fn simple_round_trip_test() -> Result<(), Error> {
        let store_owned = DedupStore::new(
            &make_default_config(),
            Arc::new(MemoryStore::new(&config::backends::MemoryStore::default())), // Index store.
            Arc::new(MemoryStore::new(&config::backends::MemoryStore::default())), // Content store.
        );
        let store = Pin::new(&store_owned);

        let original_data = make_random_data(MEGABYTE_SZ);
        let digest = DigestInfo::try_new(&VALID_HASH1, MEGABYTE_SZ).unwrap();

        store
            .update_oneshot(digest.clone(), original_data.clone().into())
            .await
            .err_tip(|| "Failed to write data to dedup store")?;

        let rt_data = store
            .get_part_unchunked(digest.clone(), 0, None, Some(original_data.len()))
            .await
            .err_tip(|| "Failed to get_part from dedup store")?;

        assert_eq!(rt_data, original_data, "Expected round trip data to match");
        Ok(())
    }

    #[tokio::test]
    async fn check_missing_last_chunk_test() -> Result<(), Error> {
        let content_store = Arc::new(MemoryStore::new(&config::backends::MemoryStore::default()));
        let store_owned = DedupStore::new(
            &make_default_config(),
            Arc::new(MemoryStore::new(&config::backends::MemoryStore::default())), // Index store.
            content_store.clone(),
        );
        let store = Pin::new(&store_owned);

        let original_data = make_random_data(MEGABYTE_SZ);
        let digest = DigestInfo::try_new(&VALID_HASH1, MEGABYTE_SZ).unwrap();

        store
            .update_oneshot(digest.clone(), original_data.into())
            .await
            .err_tip(|| "Failed to write data to dedup store")?;

        // This is the hash & size of the last chunk item in the content_store.
        const LAST_CHUNK_HASH: &str = "9220cc441e3860a0a8f5ed984d5b2da69c09ca800dcfd7a93c755acf8561e7a5";
        const LAST_CHUNK_SIZE: usize = 25779;

        let did_delete = content_store
            .remove_entry(&DigestInfo::try_new(LAST_CHUNK_HASH, LAST_CHUNK_SIZE).unwrap())
            .await;

        assert_eq!(did_delete, true, "Expected item to exist in store");

        let result = store.get_part_unchunked(digest.clone(), 0, None, None).await;
        assert!(result.is_err(), "Expected result to be an error");
        assert_eq!(
            result.unwrap_err().code,
            Code::NotFound,
            "Expected result to not be found"
        );
        Ok(())
    }

    /// Test to ensure if we upload a bit of data then request just a slice of it, we get the
    /// proper data out. Internal to DedupStore we only download the slices that contain the
    /// requested data; this test covers that use case.
    #[tokio::test]
    async fn fetch_part_test() -> Result<(), Error> {
        let store_owned = DedupStore::new(
            &make_default_config(),
            Arc::new(MemoryStore::new(&config::backends::MemoryStore::default())), // Index store.
            Arc::new(MemoryStore::new(&config::backends::MemoryStore::default())), // Content store.
        );
        let store = Pin::new(&store_owned);

        const DATA_SIZE: usize = MEGABYTE_SZ / 4;
        let original_data = make_random_data(DATA_SIZE);
        let digest = DigestInfo::try_new(&VALID_HASH1, DATA_SIZE).unwrap();

        store
            .update_oneshot(digest.clone(), original_data.clone().into())
            .await
            .err_tip(|| "Failed to write data to dedup store")?;

        const ONE_THIRD_SZ: usize = DATA_SIZE / 3;
        let rt_data = store
            .get_part_unchunked(digest.clone(), ONE_THIRD_SZ, Some(ONE_THIRD_SZ), None)
            .await
            .err_tip(|| "Failed to get_part from dedup store")?;

        assert_eq!(rt_data.len(), ONE_THIRD_SZ, "Expected round trip sizes to match");
        assert_eq!(
            rt_data,
            original_data[ONE_THIRD_SZ..(ONE_THIRD_SZ * 2)],
            "Expected round trip data to match"
        );
        Ok(())
    }
}
