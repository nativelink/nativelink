// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::pin::Pin;
use std::sync::Arc;

use common::DigestInfo;
use config;
use error::Error;
use memory_store::MemoryStore;
use rand::{rngs::SmallRng, Rng, SeedableRng};
use traits::StoreTrait;

use fast_slow_store::FastSlowStore;

const MEGABYTE_SZ: usize = 1024 * 1024;

fn make_stores() -> (Arc<impl StoreTrait>, Arc<impl StoreTrait>, Arc<impl StoreTrait>) {
    let fast_store = Arc::new(MemoryStore::new(&config::backends::MemoryStore::default()));
    let slow_store = Arc::new(MemoryStore::new(&config::backends::MemoryStore::default()));
    let fast_slow_store = Arc::new(FastSlowStore::new(
        &config::backends::FastSlowStore {
            fast: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
            slow: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
        },
        fast_store.clone(),
        slow_store.clone(),
    ));
    (fast_slow_store, fast_store, slow_store)
}

fn make_random_data(sz: usize) -> Vec<u8> {
    let mut value = vec![0u8; sz];
    let mut rng = SmallRng::seed_from_u64(1);
    rng.fill(&mut value[..]);
    value
}

async fn check_data<S: StoreTrait>(
    check_store: Pin<&S>,
    digest: DigestInfo,
    original_data: &Vec<u8>,
    debug_name: &str,
) -> Result<(), Error> {
    assert!(
        check_store.has(digest.clone()).await?.is_some(),
        "Expected data to exist in {} store",
        debug_name
    );

    let store_data = check_store.get_part_unchunked(digest.clone(), 0, None, None).await?;
    assert_eq!(
        store_data, original_data,
        "Expected data to match in {} store",
        debug_name
    );
    Ok(())
}

#[cfg(test)]
mod fast_slow_store_tests {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    const VALID_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

    #[tokio::test]
    async fn write_large_amount_to_both_stores_test() -> Result<(), Error> {
        let (store, fast_store, slow_store) = make_stores();
        let store = Pin::new(store.as_ref());

        let original_data = make_random_data(20 * MEGABYTE_SZ);
        let digest = DigestInfo::try_new(&VALID_HASH, 100).unwrap();
        store
            .update_oneshot(digest.clone(), original_data.clone().into())
            .await?;

        check_data(store, digest.clone(), &original_data, "fast_slow").await?;
        check_data(Pin::new(fast_store.as_ref()), digest.clone(), &original_data, "fast").await?;
        check_data(Pin::new(slow_store.as_ref()), digest.clone(), &original_data, "slow").await?;

        Ok(())
    }

    #[tokio::test]
    async fn fetch_slow_store_puts_in_fast_store_test() -> Result<(), Error> {
        let (fast_slow_store, fast_store, slow_store) = make_stores();
        let fast_slow_store = Pin::new(fast_slow_store.as_ref());
        let fast_store = Pin::new(fast_store.as_ref());
        let slow_store = Pin::new(slow_store.as_ref());

        let original_data = make_random_data(MEGABYTE_SZ);
        let digest = DigestInfo::try_new(&VALID_HASH, 100).unwrap();
        slow_store
            .update_oneshot(digest.clone(), original_data.clone().into())
            .await?;

        assert_eq!(fast_slow_store.has(digest.clone()).await, Ok(Some(original_data.len())));
        assert_eq!(fast_store.has(digest.clone()).await, Ok(None));
        assert_eq!(slow_store.has(digest.clone()).await, Ok(Some(original_data.len())));

        // This get() request should place the data in fast_store too.
        fast_slow_store
            .get_part_unchunked(digest.clone(), 0, None, None)
            .await?;

        // Now the data should exist in all the stores.
        check_data(fast_store, digest.clone(), &original_data, "fast_store").await?;
        check_data(slow_store, digest.clone(), &original_data, "slow_store").await?;

        Ok(())
    }

    #[tokio::test]
    async fn partial_reads_do_not_copy_to_slow_store_test() -> Result<(), Error> {
        let (fast_slow_store, fast_store, slow_store) = make_stores();
        let fast_slow_store = Pin::new(fast_slow_store.as_ref());
        let fast_store = Pin::new(fast_store.as_ref());
        let slow_store = Pin::new(slow_store.as_ref());

        let original_data = make_random_data(MEGABYTE_SZ);
        let digest = DigestInfo::try_new(&VALID_HASH, 100).unwrap();
        slow_store
            .update_oneshot(digest.clone(), original_data.clone().into())
            .await?;

        // This get() request should place the data in fast_store too.
        fast_slow_store
            .get_part_unchunked(digest.clone(), 0, Some(50), None)
            .await?;

        // Data should not exist in fast store, but should exist in slow store because
        // it was a partial read.
        assert_eq!(fast_store.has(digest.clone()).await, Ok(None));
        check_data(slow_store, digest.clone(), &original_data, "slow_store").await?;

        Ok(())
    }
}
