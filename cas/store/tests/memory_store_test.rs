// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::pin::Pin;

#[cfg(test)]
mod memory_store_tests {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    use error::Error;
    use std::io::Cursor;

    use common::DigestInfo;
    use config;
    use memory_store::MemoryStore;
    use traits::{StoreTrait, UploadSizeInfo};

    const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

    #[tokio::test]
    async fn insert_one_item_then_update() -> Result<(), Error> {
        let store_owned = MemoryStore::new(&config::backends::MemoryStore::default());
        let store = Pin::new(&store_owned);

        {
            // Insert dummy value into store.
            const VALUE1: &str = "13";
            store
                .update(
                    DigestInfo::try_new(&VALID_HASH1, VALUE1.len())?,
                    Box::new(Cursor::new(VALUE1)),
                    UploadSizeInfo::ExactSize(VALUE1.len()),
                )
                .await?;
            assert!(
                store.has(DigestInfo::try_new(&VALID_HASH1, VALUE1.len())?).await?,
                "Expected memory store to have hash: {}",
                VALID_HASH1
            );
        }

        const VALUE2: &str = "23";
        let mut store_data = Vec::new();
        {
            // Now change value we just inserted.
            store
                .update(
                    DigestInfo::try_new(&VALID_HASH1, VALUE2.len())?,
                    Box::new(Cursor::new(VALUE2)),
                    UploadSizeInfo::ExactSize(VALUE2.len()),
                )
                .await?;
            store
                .get(
                    DigestInfo::try_new(&VALID_HASH1, VALUE2.len())?,
                    &mut Cursor::new(&mut store_data),
                )
                .await?;
        }

        assert_eq!(
            store_data,
            VALUE2.as_bytes(),
            "Hash for key: {} did not update. Expected: {:#x?}, but got: {:#x?}",
            VALID_HASH1,
            VALUE2,
            store_data
        );
        Ok(())
    }

    const TOO_LONG_HASH: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdefff";
    const TOO_SHORT_HASH: &str = "100000000000000000000000000000000000000000000000000000000000001";
    const INVALID_HASH: &str = "g111111111111111111111111111111111111111111111111111111111111111";

    #[tokio::test]
    async fn read_partial() -> Result<(), Error> {
        let store_owned = MemoryStore::new(&config::backends::MemoryStore::default());
        let store = Pin::new(&store_owned);

        const VALUE1: &str = "1234";
        let digest = DigestInfo::try_new(&VALID_HASH1, 4).unwrap();
        store
            .update(
                digest.clone(),
                Box::new(Cursor::new(VALUE1)),
                UploadSizeInfo::ExactSize(4),
            )
            .await?;

        let mut store_data = Vec::new();
        store
            .get_part(digest, &mut Cursor::new(&mut store_data), 1, Some(2))
            .await?;

        assert_eq!(
            VALUE1[1..3].as_bytes(),
            store_data,
            "Expected partial data to match, expected '{:#x?}' got: {:#x?}'",
            VALUE1[1..3].as_bytes(),
            store_data,
        );
        Ok(())
    }

    #[tokio::test]
    async fn errors_with_invalid_inputs() -> Result<(), Error> {
        let store_owned = MemoryStore::new(&config::backends::MemoryStore::default());
        let store = Pin::new(&store_owned);
        const VALUE1: &str = "123";
        {
            // .has() tests.
            async fn has_should_fail(store: Pin<&MemoryStore>, hash: &str, expected_size: usize) {
                let digest = DigestInfo::try_new(&hash, expected_size);
                assert!(
                    digest.is_err() || store.has(digest.unwrap()).await.is_err(),
                    ".has() should have failed: {} {}",
                    hash,
                    expected_size
                );
            }
            has_should_fail(store, &TOO_LONG_HASH, VALUE1.len()).await;
            has_should_fail(store, &TOO_SHORT_HASH, VALUE1.len()).await;
            has_should_fail(store, &INVALID_HASH, VALUE1.len()).await;
        }
        {
            // .update() tests.
            async fn update_should_fail<'a>(
                store: Pin<&'a MemoryStore>,
                hash: &'a str,
                expected_size: usize,
                value: &'static str,
            ) {
                let digest = DigestInfo::try_new(&hash, expected_size);
                assert!(
                    digest.is_err()
                        || store
                            .update(
                                digest.unwrap(),
                                Box::new(Cursor::new(value)),
                                UploadSizeInfo::ExactSize(expected_size)
                            )
                            .await
                            .is_err(),
                    ".has() should have failed: {} {} {}",
                    hash,
                    expected_size,
                    value
                );
            }
            update_should_fail(store, &TOO_LONG_HASH, VALUE1.len(), &VALUE1).await;
            update_should_fail(store, &TOO_SHORT_HASH, VALUE1.len(), &VALUE1).await;
            update_should_fail(store, &INVALID_HASH, VALUE1.len(), &VALUE1).await;
        }
        {
            // .update() tests.
            async fn get_should_fail<'a>(
                store: Pin<&'a MemoryStore>,
                hash: &'a str,
                expected_size: usize,
                out_data: &'a mut Vec<u8>,
            ) {
                let digest = DigestInfo::try_new(&hash, expected_size);
                assert!(
                    digest.is_err() || store.get(digest.unwrap(), &mut Cursor::new(out_data)).await.is_err(),
                    ".get() should have failed: {} {}",
                    hash,
                    expected_size
                );
            }
            let mut out_data: Vec<u8> = Vec::new();

            get_should_fail(store, &TOO_LONG_HASH, 1, &mut out_data).await;
            get_should_fail(store, &TOO_SHORT_HASH, 1, &mut out_data).await;
            get_should_fail(store, &INVALID_HASH, 1, &mut out_data).await;
            // With an empty store .get() should fail too.
            get_should_fail(store, &VALID_HASH1, 1, &mut out_data).await;
        }
        Ok(())
    }
}
