// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::env;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use buf_channel::{make_buf_channel_pair, DropCloserReadHalf};
use common::{fs, DigestInfo};
use config;
use error::{Error, ResultExt};
use filesystem_store::FilesystemStore;
use filetime::{set_file_atime, FileTime};
use rand::{thread_rng, Rng};
use tokio::io::AsyncReadExt;
use tokio_stream::{wrappers::ReadDirStream, StreamExt};
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

async fn read_file_contents(file_name: &str) -> Result<Vec<u8>, Error> {
    let mut file = fs::open_file(&file_name)
        .await
        .err_tip(|| format!("Failed to open file: {}", file_name))?;
    let mut data = vec![];
    file.read_to_end(&mut data)
        .await
        .err_tip(|| "Error reading file to end")?;
    Ok(data)
}

#[cfg(test)]
mod filesystem_store_tests {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    const HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";
    const HASH2: &str = "0123456789abcdef000000000000000000020000000000000123456789abcdef";
    const VALUE1: &str = "123";
    const VALUE2: &str = "321";

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
                    ..Default::default()
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
                    ..Default::default()
                })
                .await?,
            );

            let content = store.as_ref().get_part_unchunked(digest, 0, None, None).await?;
            assert_eq!(content, VALUE1.as_bytes());
        }

        Ok(())
    }

    #[tokio::test]
    async fn temp_files_get_deleted_on_replace_test() -> Result<(), Error> {
        let digest1 = DigestInfo::try_new(&HASH1, VALUE1.len())?;
        let content_path = make_temp_path("content_path");
        let temp_path = make_temp_path("temp_path");

        let store = Box::pin(
            FilesystemStore::new(&config::backends::FilesystemStore {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                eviction_policy: Some(config::backends::EvictionPolicy {
                    max_count: 3,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await?,
        );

        // Insert data into store.
        store.as_ref().update_oneshot(digest1.clone(), VALUE1.into()).await?;

        let expected_file_name = format!("{}/{}-{}", content_path, digest1.str(), digest1.size_bytes);
        {
            // Check to ensure our file exists where it should and content matches.
            let data = read_file_contents(&expected_file_name).await?;
            assert_eq!(&data[..], VALUE1.as_bytes(), "Expected file content to match");
        }

        // Replace content.
        store.as_ref().update_oneshot(digest1.clone(), VALUE2.into()).await?;

        {
            // Check to ensure our file now has new content.
            let data = read_file_contents(&expected_file_name).await?;
            assert_eq!(&data[..], VALUE2.as_bytes(), "Expected file content to match");
        }

        let (_permit, temp_dir_handle) = fs::read_dir(temp_path.clone())
            .await
            .err_tip(|| "Failed opening temp directory")?
            .into_inner();
        let mut read_dir_stream = ReadDirStream::new(temp_dir_handle);

        // Ensure we let any background tasks finish.
        tokio::task::yield_now().await;

        while let Some(temp_dir_entry) = read_dir_stream.next().await {
            let path = temp_dir_entry?.path();
            assert!(false, "No files should exist in temp directory, found: {:?}", path);
        }

        Ok(())
    }

    // This test ensures that if a file is overridden and an open stream to the file already
    // exists, the open stream will continue to work properly and when the stream is done the
    // temporary file (of the object that was deleted) is cleaned up.
    #[tokio::test]
    async fn file_continues_to_stream_on_content_replace_test() -> Result<(), Error> {
        let digest1 = DigestInfo::try_new(&HASH1, VALUE1.len())?;
        let content_path = make_temp_path("content_path");
        let temp_path = make_temp_path("temp_path");

        let store = Arc::new(
            FilesystemStore::new(&config::backends::FilesystemStore {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                eviction_policy: Some(config::backends::EvictionPolicy {
                    max_count: 3,
                    ..Default::default()
                }),
                read_buffer_size: 1,
            })
            .await?,
        );

        let store_pin = Pin::new(store.as_ref());
        // Insert data into store.
        store_pin
            .as_ref()
            .update_oneshot(digest1.clone(), VALUE1.into())
            .await?;

        let (writer, mut reader) = make_buf_channel_pair();
        let store_clone = store.clone();
        let digest1_clone = digest1.clone();
        tokio::spawn(async move { Pin::new(store_clone.as_ref()).get(digest1_clone, writer).await });

        {
            // Check to ensure our first byte has been received. The future should be stalled here.
            let first_byte = DropCloserReadHalf::take(&mut reader, 1)
                .await
                .err_tip(|| "Error reading first byte")?;
            assert_eq!(first_byte[0], VALUE1.as_bytes()[0], "Expected first byte to match");
        }

        // Replace content.
        store_pin
            .as_ref()
            .update_oneshot(digest1.clone(), VALUE2.into())
            .await?;

        // Ensure we let any background tasks finish.
        tokio::task::yield_now().await;

        {
            // Now ensure we only have 1 file in our temp path.
            let (_permit, temp_dir_handle) = fs::read_dir(temp_path.clone())
                .await
                .err_tip(|| "Failed opening temp directory")?
                .into_inner();
            let mut read_dir_stream = ReadDirStream::new(temp_dir_handle);
            let mut num_files = 0;
            while let Some(temp_dir_entry) = read_dir_stream.next().await {
                num_files += 1;
                let path = temp_dir_entry?.path();
                let data = read_file_contents(path.to_str().unwrap()).await?;
                assert_eq!(&data[..], VALUE1.as_bytes(), "Expected file content to match");
            }
            assert_eq!(num_files, 1, "There should only be one file in the temp directory");
        }

        let remaining_file_data = DropCloserReadHalf::take(&mut reader, 1024)
            .await
            .err_tip(|| "Error reading remaining bytes")?;

        assert_eq!(
            &remaining_file_data,
            VALUE1[1..].as_bytes(),
            "Expected file content to match"
        );

        // Ensure we let any background tasks finish.
        tokio::task::yield_now().await;

        {
            // Now ensure our temp file was cleaned up.
            let (_permit, temp_dir_handle) = fs::read_dir(temp_path.clone())
                .await
                .err_tip(|| "Failed opening temp directory")?
                .into_inner();
            let mut read_dir_stream = ReadDirStream::new(temp_dir_handle);
            while let Some(temp_dir_entry) = read_dir_stream.next().await {
                let path = temp_dir_entry?.path();
                assert!(false, "No files should exist in temp directory, found: {:?}", path);
            }
        }

        Ok(())
    }

    // Eviction has a different code path than a file replacement, so we check that if a
    // file is evicted and has an open stream on it, it will stay alive and eventually
    // get deleted.
    #[tokio::test]
    async fn file_gets_cleans_up_on_cache_eviction() -> Result<(), Error> {
        let digest1 = DigestInfo::try_new(&HASH1, VALUE1.len())?;
        let digest2 = DigestInfo::try_new(&HASH2, VALUE2.len())?;
        let content_path = make_temp_path("content_path");
        let temp_path = make_temp_path("temp_path");

        let store = Arc::new(
            FilesystemStore::new(&config::backends::FilesystemStore {
                content_path: content_path.clone(),
                temp_path: temp_path.clone(),
                eviction_policy: Some(config::backends::EvictionPolicy {
                    max_count: 1,
                    ..Default::default()
                }),
                read_buffer_size: 1,
            })
            .await?,
        );

        let store_pin = Pin::new(store.as_ref());
        // Insert data into store.
        store_pin
            .as_ref()
            .update_oneshot(digest1.clone(), VALUE1.into())
            .await?;

        let mut reader = {
            let (writer, reader) = make_buf_channel_pair();
            let store_clone = store.clone();
            tokio::spawn(async move { Pin::new(store_clone.as_ref()).get(digest1, writer).await });
            reader
        };
        // Ensure we have received 1 byte in our buffer. This will ensure we have a reference to
        // our file open.
        assert!(reader.peek().await.is_ok(), "Could not peek into reader");

        // Insert new content. This will evict the old item.
        store_pin
            .as_ref()
            .update_oneshot(digest2.clone(), VALUE2.into())
            .await?;

        // Ensure we let any background tasks finish.
        tokio::task::yield_now().await;

        {
            // Now ensure we only have 1 file in our temp path.
            let (_permit, temp_dir_handle) = fs::read_dir(temp_path.clone())
                .await
                .err_tip(|| "Failed opening temp directory")?
                .into_inner();
            let mut read_dir_stream = ReadDirStream::new(temp_dir_handle);
            let mut num_files = 0;
            while let Some(temp_dir_entry) = read_dir_stream.next().await {
                num_files += 1;
                let path = temp_dir_entry?.path();
                let data = read_file_contents(path.to_str().unwrap()).await?;
                assert_eq!(&data[..], VALUE1.as_bytes(), "Expected file content to match");
            }
            assert_eq!(num_files, 1, "There should only be one file in the temp directory");
        }

        let reader_data = DropCloserReadHalf::take(&mut reader, 1024)
            .await
            .err_tip(|| "Error reading remaining bytes")?;

        assert_eq!(&reader_data, VALUE1, "Expected file content to match");

        // Ensure we let any background tasks finish.
        tokio::task::yield_now().await;

        {
            // Now ensure our temp file was cleaned up.
            let (_permit, temp_dir_handle) = fs::read_dir(temp_path.clone())
                .await
                .err_tip(|| "Failed opening temp directory")?
                .into_inner();
            let mut read_dir_stream = ReadDirStream::new(temp_dir_handle);
            while let Some(temp_dir_entry) = read_dir_stream.next().await {
                let path = temp_dir_entry?.path();
                assert!(false, "No files should exist in temp directory, found: {:?}", path);
            }
        }

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
                ..Default::default()
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

    #[tokio::test]
    async fn eviction_deletes_file_test() -> Result<(), Error> {
        let digest1 = DigestInfo::try_new(&HASH1, VALUE1.len())?;

        let store = Box::pin(
            FilesystemStore::new(&config::backends::FilesystemStore {
                content_path: make_temp_path("content_path"),
                temp_path: make_temp_path("temp_path"),
                eviction_policy: None,
                ..Default::default()
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
