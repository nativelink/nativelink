// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::time::Duration;

use bytes::Bytes;
use mock_instant::{Instant as MockInstant, MockClock};

use common::DigestInfo;
use config::backends::EvictionPolicy;
use error::Error;
use evicting_map::{EvictingMap, InstantWrapper, LenEntry};

#[derive(Clone, PartialEq, Debug)]
pub struct BytesWrapper(Bytes);

impl LenEntry for BytesWrapper {
    #[inline]
    fn len(&self) -> usize {
        Bytes::len(&self.0)
    }
}

impl From<Bytes> for BytesWrapper {
    #[inline]
    fn from(bytes: Bytes) -> BytesWrapper {
        BytesWrapper(bytes)
    }
}

/// Our mocked out instant that we can pass to our EvictionMap.
struct MockInstantWrapped(MockInstant);

impl InstantWrapper for MockInstantWrapped {
    fn from_secs(_secs: u64) -> Self {
        MockInstantWrapped(MockInstant::now())
    }

    fn unix_timestamp(&self) -> u64 {
        100
    }

    fn elapsed(&self) -> Duration {
        self.0.elapsed()
    }
}

#[cfg(test)]
mod evicting_map_tests {
    use super::*;

    const HASH1: &str = "0123456789abcdef000000000000000000000000000000000123456789abcdef";
    const HASH2: &str = "123456789abcdef000000000000000000000000000000000123456789abcdef1";
    const HASH3: &str = "23456789abcdef000000000000000000000000000000000123456789abcdef12";
    const HASH4: &str = "3456789abcdef000000000000000000000000000000000123456789abcdef012";

    #[tokio::test]
    async fn insert_purges_at_max_count() -> Result<(), Error> {
        let evicting_map = EvictingMap::<BytesWrapper, MockInstantWrapped>::new(
            &EvictionPolicy {
                max_count: 3,
                max_seconds: 0,
                max_bytes: 0,
            },
            MockInstantWrapped(MockInstant::now()),
        );
        evicting_map
            .insert(DigestInfo::try_new(HASH1, 0)?, Bytes::new().into())
            .await;
        evicting_map
            .insert(DigestInfo::try_new(HASH2, 0)?, Bytes::new().into())
            .await;
        evicting_map
            .insert(DigestInfo::try_new(HASH3, 0)?, Bytes::new().into())
            .await;
        evicting_map
            .insert(DigestInfo::try_new(HASH4, 0)?, Bytes::new().into())
            .await;

        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH1, 0)?).await,
            None,
            "Expected map to not have item 1"
        );
        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH2, 0)?).await,
            Some(0),
            "Expected map to have item 2"
        );
        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH3, 0)?).await,
            Some(0),
            "Expected map to have item 3"
        );
        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH4, 0)?).await,
            Some(0),
            "Expected map to have item 4"
        );

        Ok(())
    }

    #[tokio::test]
    async fn insert_purges_at_max_bytes() -> Result<(), Error> {
        let evicting_map = EvictingMap::<BytesWrapper, MockInstantWrapped>::new(
            &EvictionPolicy {
                max_count: 0,
                max_seconds: 0,
                max_bytes: 17,
            },
            MockInstantWrapped(MockInstant::now()),
        );
        const DATA: &str = "12345678";
        evicting_map
            .insert(DigestInfo::try_new(HASH1, 0)?, Bytes::from(DATA).into())
            .await;
        evicting_map
            .insert(DigestInfo::try_new(HASH2, 0)?, Bytes::from(DATA).into())
            .await;
        evicting_map
            .insert(DigestInfo::try_new(HASH3, 0)?, Bytes::from(DATA).into())
            .await;
        evicting_map
            .insert(DigestInfo::try_new(HASH4, 0)?, Bytes::from(DATA).into())
            .await;

        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH1, 0)?).await,
            None,
            "Expected map to not have item 1"
        );
        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH2, 0)?).await,
            None,
            "Expected map to not have item 2"
        );
        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH3, 0)?).await,
            Some(DATA.len()),
            "Expected map to have item 3"
        );
        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH4, 0)?).await,
            Some(DATA.len()),
            "Expected map to have item 4"
        );

        Ok(())
    }

    #[tokio::test]
    async fn insert_purges_at_max_seconds() -> Result<(), Error> {
        let evicting_map = EvictingMap::<BytesWrapper, MockInstantWrapped>::new(
            &EvictionPolicy {
                max_count: 0,
                max_seconds: 5,
                max_bytes: 0,
            },
            MockInstantWrapped(MockInstant::now()),
        );

        const DATA: &str = "12345678";
        evicting_map
            .insert(DigestInfo::try_new(HASH1, 0)?, Bytes::from(DATA).into())
            .await;
        MockClock::advance(Duration::from_secs(2));
        evicting_map
            .insert(DigestInfo::try_new(HASH2, 0)?, Bytes::from(DATA).into())
            .await;
        MockClock::advance(Duration::from_secs(2));
        evicting_map
            .insert(DigestInfo::try_new(HASH3, 0)?, Bytes::from(DATA).into())
            .await;
        MockClock::advance(Duration::from_secs(2));
        evicting_map
            .insert(DigestInfo::try_new(HASH4, 0)?, Bytes::from(DATA).into())
            .await;

        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH1, 0)?).await,
            None,
            "Expected map to not have item 1"
        );
        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH2, 0)?).await,
            Some(DATA.len()),
            "Expected map to have item 2"
        );
        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH3, 0)?).await,
            Some(DATA.len()),
            "Expected map to have item 3"
        );
        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH4, 0)?).await,
            Some(DATA.len()),
            "Expected map to have item 4"
        );

        Ok(())
    }

    #[tokio::test]
    async fn get_refreshes_time() -> Result<(), Error> {
        let evicting_map = EvictingMap::<BytesWrapper, MockInstantWrapped>::new(
            &EvictionPolicy {
                max_count: 0,
                max_seconds: 3,
                max_bytes: 0,
            },
            MockInstantWrapped(MockInstant::now()),
        );

        const DATA: &str = "12345678";
        evicting_map
            .insert(DigestInfo::try_new(HASH1, 0)?, Bytes::from(DATA).into())
            .await;
        MockClock::advance(Duration::from_secs(2));
        evicting_map
            .insert(DigestInfo::try_new(HASH2, 0)?, Bytes::from(DATA).into())
            .await;
        MockClock::advance(Duration::from_secs(2));
        evicting_map.get(&DigestInfo::try_new(HASH1, 0)?).await; // HASH1 should now be last to be evicted.
        MockClock::advance(Duration::from_secs(2));
        evicting_map
            .insert(DigestInfo::try_new(HASH3, 0)?, Bytes::from(DATA).into())
            .await;

        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH1, 0)?).await,
            Some(DATA.len()),
            "Expected map to have item 1"
        );
        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH2, 0)?).await,
            None,
            "Expected map to not have item 2"
        );
        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH3, 0)?).await,
            Some(DATA.len()),
            "Expected map to have item 3"
        );

        Ok(())
    }

    #[tokio::test]
    async fn contains_key_refreshes_time() -> Result<(), Error> {
        let evicting_map = EvictingMap::<BytesWrapper, MockInstantWrapped>::new(
            &EvictionPolicy {
                max_count: 0,
                max_seconds: 3,
                max_bytes: 0,
            },
            MockInstantWrapped(MockInstant::now()),
        );

        const DATA: &str = "12345678";
        evicting_map
            .insert(DigestInfo::try_new(HASH1, 0)?, Bytes::from(DATA).into())
            .await;
        MockClock::advance(Duration::from_secs(2));
        evicting_map
            .insert(DigestInfo::try_new(HASH2, 0)?, Bytes::from(DATA).into())
            .await;
        MockClock::advance(Duration::from_secs(2));
        evicting_map.size_for_key(&DigestInfo::try_new(HASH1, 0)?).await; // HASH1 should now be last to be evicted.
        MockClock::advance(Duration::from_secs(2));
        evicting_map
            .insert(DigestInfo::try_new(HASH3, 0)?, Bytes::from(DATA).into())
            .await;

        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH1, 0)?).await,
            Some(8),
            "Expected map to have item 1"
        );
        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH2, 0)?).await,
            None,
            "Expected map to not have item 2"
        );
        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH3, 0)?).await,
            Some(8),
            "Expected map to have item 3"
        );

        Ok(())
    }

    #[tokio::test]
    async fn hashes_equal_sizes_different_doesnt_override() -> Result<(), Error> {
        let evicting_map = EvictingMap::<BytesWrapper, MockInstantWrapped>::new(
            &EvictionPolicy {
                max_count: 0,
                max_seconds: 0,
                max_bytes: 0,
            },
            MockInstantWrapped(MockInstant::now()),
        );

        let value1 = BytesWrapper(Bytes::from_static(b"12345678"));
        let value2 = BytesWrapper(Bytes::from_static(b"87654321"));
        evicting_map
            .insert(DigestInfo::try_new(HASH1, 0)?, value1.clone())
            .await;
        evicting_map
            .insert(DigestInfo::try_new(HASH1, 1)?, value2.clone())
            .await;
        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH1, 0)?).await,
            Some(value1.len()),
            "HASH1/0 should exist"
        );
        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH1, 1)?).await,
            Some(value2.len()),
            "HASH1/1 should exist"
        );

        assert_eq!(evicting_map.get(&DigestInfo::try_new(HASH1, 0)?).await.unwrap(), value1);
        assert_eq!(evicting_map.get(&DigestInfo::try_new(HASH1, 1)?).await.unwrap(), value2);

        Ok(())
    }

    #[tokio::test]
    async fn build_lru_index_and_reload() -> Result<(), Error> {
        let mut evicting_map = EvictingMap::<BytesWrapper, MockInstantWrapped>::new(
            &EvictionPolicy {
                max_count: 0,
                max_seconds: 0,
                max_bytes: 0,
            },
            MockInstantWrapped(MockInstant::now()),
        );

        let value1 = BytesWrapper(Bytes::new());
        let value2 = BytesWrapper(Bytes::new());
        evicting_map
            .insert(DigestInfo::try_new(HASH1, 0)?, value1.clone())
            .await;
        evicting_map
            .insert(DigestInfo::try_new(HASH2, 1)?, value2.clone())
            .await;

        let serialized_index = evicting_map.build_lru_index().await;

        {
            // Now insert another entry.
            let value3 = BytesWrapper(Bytes::new());
            evicting_map
                .insert(DigestInfo::try_new(HASH3, 3)?, value3.clone())
                .await;

            assert_eq!(
                evicting_map.size_for_key(&DigestInfo::try_new(HASH1, 0)?).await,
                Some(value1.len()),
                "HASH1/0 should exist"
            );
            assert_eq!(
                evicting_map.size_for_key(&DigestInfo::try_new(HASH2, 1)?).await,
                Some(value2.len()),
                "HASH2/1 should exist"
            );
            assert_eq!(
                evicting_map.size_for_key(&DigestInfo::try_new(HASH3, 3)?).await,
                Some(value3.len()),
                "HASH3/1 should exist"
            );
        }

        // Now reload from the serialized version.
        evicting_map
            .restore_lru(serialized_index, move |_digest| BytesWrapper(Bytes::new()))
            .await;

        // Data should now have the previously inserted data, but not the newly inserted data
        // that was inserted after the `build_lru_index` call.
        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH1, 0)?).await,
            Some(value1.len()),
            "HASH1/0 should exist"
        );
        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH2, 1)?).await,
            Some(value2.len()),
            "HASH2/1 should exist"
        );
        assert_eq!(
            evicting_map.size_for_key(&DigestInfo::try_new(HASH3, 3)?).await,
            None,
            "HASH3/2 should be empty"
        );

        Ok(())
    }
}
