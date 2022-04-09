// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::pin::Pin;
use std::sync::Arc;

#[cfg(test)]
mod ref_store_tests {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    use error::Error;

    use common::DigestInfo;
    use config;
    use memory_store::MemoryStore;
    use size_partitioning_store::SizePartitioningStore;
    use traits::StoreTrait;

    const BASE_SIZE_PART: u64 = 5;

    const SMALL_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
    const SMALL_VALUE: &str = "99";

    const BIG_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
    const BIG_VALUE: &str = "123456789";

    fn setup_stores(size: u64) -> (SizePartitioningStore, Arc<MemoryStore>, Arc<MemoryStore>) {
        let lower_memory_store = Arc::new(MemoryStore::new(&config::backends::MemoryStore::default()));
        let upper_memory_store = Arc::new(MemoryStore::new(&config::backends::MemoryStore::default()));

        let size_part_store = SizePartitioningStore::new(
            &config::backends::SizePartitioningStore {
                size,
                lower_store: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
                upper_store: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
            },
            lower_memory_store.clone(),
            upper_memory_store.clone(),
        );
        (size_part_store, lower_memory_store, upper_memory_store)
    }

    #[tokio::test]
    async fn has_test() -> Result<(), Error> {
        let (size_part_store, lower_memory_store, upper_memory_store) = setup_stores(BASE_SIZE_PART);

        {
            // Insert data into lower store.
            Pin::new(lower_memory_store.as_ref())
                .update_oneshot(DigestInfo::try_new(&SMALL_HASH, SMALL_VALUE.len())?, SMALL_VALUE.into())
                .await?;

            // Insert data into upper store.
            Pin::new(upper_memory_store.as_ref())
                .update_oneshot(DigestInfo::try_new(&BIG_HASH, BIG_VALUE.len())?, BIG_VALUE.into())
                .await?;
        }
        {
            // Check if our partition store has small data.
            let small_has_result = Pin::new(&size_part_store)
                .has(DigestInfo::try_new(&SMALL_HASH, SMALL_VALUE.len())?)
                .await;
            assert_eq!(
                small_has_result,
                Ok(Some(SMALL_VALUE.len())),
                "Expected size part store to have data in ref store : {}",
                SMALL_HASH
            );
        }
        {
            // Check if our partition store has big data.
            let small_has_result = Pin::new(&size_part_store)
                .has(DigestInfo::try_new(&BIG_HASH, BIG_VALUE.len())?)
                .await;
            assert_eq!(
                small_has_result,
                Ok(Some(BIG_VALUE.len())),
                "Expected size part store to have data in ref store : {}",
                BIG_HASH
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn get_test() -> Result<(), Error> {
        let (size_part_store, lower_memory_store, upper_memory_store) = setup_stores(BASE_SIZE_PART);

        {
            // Insert data into lower store.
            Pin::new(lower_memory_store.as_ref())
                .update_oneshot(DigestInfo::try_new(&SMALL_HASH, SMALL_VALUE.len())?, SMALL_VALUE.into())
                .await?;

            // Insert data into upper store.
            Pin::new(upper_memory_store.as_ref())
                .update_oneshot(DigestInfo::try_new(&BIG_HASH, BIG_VALUE.len())?, BIG_VALUE.into())
                .await?;
        }
        {
            // Read the partition store small data.
            let data = Pin::new(&size_part_store)
                .get_part_unchunked(DigestInfo::try_new(&SMALL_HASH, SMALL_VALUE.len())?, 0, None, None)
                .await
                .expect("Get should have succeeded");
            assert_eq!(
                data,
                SMALL_VALUE.as_bytes(),
                "Expected size part store to have data in ref store : {}",
                SMALL_HASH
            );
        }
        {
            // Read the partition store big data.
            let data = Pin::new(&size_part_store)
                .get_part_unchunked(DigestInfo::try_new(&BIG_HASH, BIG_VALUE.len())?, 0, None, None)
                .await
                .expect("Get should have succeeded");
            assert_eq!(
                data,
                BIG_VALUE.as_bytes(),
                "Expected size part store to have data in ref store : {}",
                BIG_HASH
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn update_test() -> Result<(), Error> {
        let (size_part_store, lower_memory_store, upper_memory_store) = setup_stores(BASE_SIZE_PART);

        {
            // Insert small data into ref_store.
            Pin::new(&size_part_store)
                .update_oneshot(DigestInfo::try_new(&SMALL_HASH, SMALL_VALUE.len())?, SMALL_VALUE.into())
                .await?;

            // Insert small data into ref_store.
            Pin::new(&size_part_store)
                .update_oneshot(DigestInfo::try_new(&BIG_HASH, BIG_VALUE.len())?, BIG_VALUE.into())
                .await?;
        }
        {
            // Check if we read small data from size_partition_store it has same data.
            let data = Pin::new(lower_memory_store.as_ref())
                .get_part_unchunked(DigestInfo::try_new(&SMALL_HASH, SMALL_VALUE.len())?, 0, None, None)
                .await
                .expect("Get should have succeeded");
            assert_eq!(
                data,
                SMALL_VALUE.as_bytes(),
                "Expected size part store to have data in memory store : {}",
                SMALL_HASH
            );
        }
        {
            // Check if we read big data from size_partition_store it has same data.
            let data = Pin::new(upper_memory_store.as_ref())
                .get_part_unchunked(DigestInfo::try_new(&BIG_HASH, BIG_VALUE.len())?, 0, None, None)
                .await
                .expect("Get should have succeeded");
            assert_eq!(
                data,
                BIG_VALUE.as_bytes(),
                "Expected size part store to have data in memory store : {}",
                BIG_HASH
            );
        }
        Ok(())
    }
}
