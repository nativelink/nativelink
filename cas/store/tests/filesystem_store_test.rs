// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use rand::{thread_rng, Rng};
use std::env;
use std::time::SystemTime;

use common::DigestInfo;
use config;
use error::Error;
use filesystem_store::FilesystemStore;
use filetime::{set_file_atime, FileTime};
use tokio::fs;
use traits::StoreTrait;

/// Get temporary path from either `TEST_TMPDIR` or best effort temp directory if
/// not set.
fn make_temp_path(data: &str) -> String {
    format!(
        "{}/{}/{}",
        env::var("TEST_TMPDIR").unwrap_or(env::temp_dir().to_str().unwrap().to_string()),
        thread_rng().gen::<u64>(),
        data
    )
}

#[cfg(test)]
mod filesystem_store_tests {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    const HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
    const VALUE1: &str = "123";

    #[tokio::test]
    async fn valid_results_after_shutdown_test() -> Result<(), Error> {
        let digest = DigestInfo::try_new(&HASH1, VALUE1.len())?;
        let content_path = make_temp_path("content_path");
        let temp_path = make_temp_path("temp_path");
        {
            let store = Box::pin(
                FilesystemStore::new(&config::backends::FilesystemStore {
                    content_path: content_path.clone(),
                    temp_path: temp_path.clone(),
                    eviction_policy: None,
                })
                .await?,
            );

            // Insert dummy value into store.
            store.as_ref().update_oneshot(digest.clone(), VALUE1.into()).await?;
            assert_eq!(
                store.as_ref().has(digest.clone()).await,
                Ok(Some(VALUE1.len())),
                "Expected filesystem store to have hash: {}",
                HASH1
            );
        }
        {
            // With a new store ensure content is still readable (ie: restores from shutdown).
            let store = Box::pin(
                FilesystemStore::new(&config::backends::FilesystemStore {
                    content_path,
                    temp_path,
                    eviction_policy: None,
                })
                .await?,
            );

            let content = store.as_ref().get_part_unchunked(digest, 0, None, None).await?;
            assert_eq!(content, VALUE1.as_bytes());
        }

        Ok(())
    }

    #[tokio::test]
    async fn atime_updates_on_has_test() -> Result<(), Error> {
        let digest1 = DigestInfo::try_new(&HASH1, VALUE1.len())?;

        let store = Box::pin(
            FilesystemStore::new(&config::backends::FilesystemStore {
                content_path: make_temp_path("content_path"),
                temp_path: make_temp_path("temp_path"),
                eviction_policy: None,
            })
            .await?,
        );
        // Insert data into store.
        store.as_ref().update_oneshot(digest1.clone(), VALUE1.into()).await?;

        let path = store.get_file_for_digest(&digest1);

        // Set atime to along time ago.
        set_file_atime(&path, FileTime::zero())?;

        // Check to ensure it was set to zero from previous command.
        assert_eq!(fs::metadata(&path).await?.accessed()?, SystemTime::UNIX_EPOCH);

        // Now touch digest1.
        store.as_ref().has(digest1.clone()).await?;

        // Ensure it was updated.
        assert!(fs::metadata(&path).await?.accessed()? > SystemTime::UNIX_EPOCH);

        Ok(())
    }

    #[tokio::test]
    async fn atime_updates_on_get_part_test() -> Result<(), Error> {
        let digest1 = DigestInfo::try_new(&HASH1, VALUE1.len())?;

        let store = Box::pin(
            FilesystemStore::new(&config::backends::FilesystemStore {
                content_path: make_temp_path("content_path"),
                temp_path: make_temp_path("temp_path"),
                eviction_policy: None,
            })
            .await?,
        );
        // Insert data into store.
        store.as_ref().update_oneshot(digest1.clone(), VALUE1.into()).await?;

        let path = store.get_file_for_digest(&digest1);

        // Set atime to along time ago.
        set_file_atime(&path, FileTime::zero())?;

        // Check to ensure it was set to zero from previous command.
        assert_eq!(fs::metadata(&path).await?.accessed()?, SystemTime::UNIX_EPOCH);

        // Now touch digest1.
        let data = store
            .as_ref()
            .get_part_unchunked(digest1.clone(), 0, None, None)
            .await?;
        assert_eq!(data, VALUE1.as_bytes());

        // Ensure it was updated.
        assert!(fs::metadata(&path).await?.accessed()? > SystemTime::UNIX_EPOCH);

        Ok(())
    }
}
