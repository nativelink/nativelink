// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::env;
use std::os::unix::fs::MetadataExt;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use rand::{thread_rng, Rng};

use common::{fs, DigestInfo};
use config;
use error::{Error, ResultExt};
use fast_slow_store::FastSlowStore;
use filesystem_store::FilesystemStore;
use memory_store::MemoryStore;
use prost::Message;
use proto::build::bazel::remote::execution::v2::{Directory, DirectoryNode, FileNode, NodeProperties, SymlinkNode};
use running_actions_manager::download_to_directory;
use store::Store;

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

async fn setup_stores() -> Result<
    (
        Pin<Arc<FilesystemStore>>,
        Pin<Arc<MemoryStore>>,
        Pin<Arc<FastSlowStore>>,
    ),
    Error,
> {
    let fast_config = config::backends::FilesystemStore {
        content_path: make_temp_path("content_path"),
        temp_path: make_temp_path("temp_path"),
        eviction_policy: None,
        ..Default::default()
    };
    let slow_config = config::backends::MemoryStore::default();
    let fast_store = Pin::new(Arc::new(FilesystemStore::new(&fast_config).await?));
    let slow_store = Pin::new(Arc::new(MemoryStore::new(&slow_config)));
    let cas_store = Pin::new(Arc::new(FastSlowStore::new(
        &config::backends::FastSlowStore {
            fast: config::backends::StoreConfig::filesystem(fast_config),
            slow: config::backends::StoreConfig::memory(slow_config),
        },
        Pin::into_inner(fast_store.clone()),
        Pin::into_inner(slow_store.clone()),
    )));
    Ok((fast_store, slow_store, cas_store))
}

#[cfg(test)]
mod running_actions_manager_tests {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    #[tokio::test]
    async fn download_to_directory_file_download_test() -> Result<(), Box<dyn std::error::Error>> {
        let (fast_store, slow_store, cas_store) = setup_stores().await?;

        const FILE1_NAME: &str = "file1.txt";
        const FILE1_CONTENT: &str = "HELLOFILE1";
        const FILE2_NAME: &str = "file2.exec";
        const FILE2_CONTENT: &str = "HELLOFILE2";
        const FILE2_MODE: u32 = 0o710;
        const FILE2_MTIME: u64 = 5;

        let root_directory_digest = {
            // Make and insert (into store) our digest info needed to create our directory & files.
            let file1_content_digest = DigestInfo::new([02u8; 32], 32);
            slow_store
                .as_ref()
                .update_oneshot(file1_content_digest.clone(), FILE1_CONTENT.into())
                .await?;
            let file2_content_digest = DigestInfo::new([03u8; 32], 32);
            slow_store
                .as_ref()
                .update_oneshot(file2_content_digest.clone(), FILE2_CONTENT.into())
                .await?;

            let root_directory_digest = DigestInfo::new([01u8; 32], 32);
            let root_directory = Directory {
                files: vec![
                    FileNode {
                        name: FILE1_NAME.to_string(),
                        digest: Some(file1_content_digest.into()),
                        is_executable: false,
                        node_properties: None,
                    },
                    FileNode {
                        name: FILE2_NAME.to_string(),
                        digest: Some(file2_content_digest.into()),
                        is_executable: true,
                        node_properties: Some(NodeProperties {
                            properties: vec![],
                            mtime: Some(
                                SystemTime::UNIX_EPOCH
                                    .checked_add(Duration::from_secs(FILE2_MTIME))
                                    .unwrap()
                                    .into(),
                            ),
                            unix_mode: Some(FILE2_MODE),
                        }),
                    },
                ],
                ..Default::default()
            };

            slow_store
                .as_ref()
                .update_oneshot(root_directory_digest.clone(), root_directory.encode_to_vec().into())
                .await?;
            root_directory_digest
        };

        let download_dir = {
            // Tell it to download the digest info to a directory.
            let download_dir = make_temp_path("download_dir");
            fs::create_dir_all(&download_dir)
                .await
                .err_tip(|| format!("Could not make download_dir : {}", download_dir))?;
            download_to_directory(
                cas_store.as_ref(),
                fast_store.as_ref(),
                &root_directory_digest,
                &download_dir,
            )
            .await?;
            download_dir
        };
        {
            // Now ensure that our download_dir has the files.
            let file1_content = fs::read(format!("{}/{}", download_dir, FILE1_NAME)).await?;
            assert_eq!(std::str::from_utf8(&file1_content)?, FILE1_CONTENT);

            let file2_path = format!("{}/{}", download_dir, FILE2_NAME);
            let file2_content = fs::read(&file2_path).await?;
            assert_eq!(std::str::from_utf8(&file2_content)?, FILE2_CONTENT);

            let file2_metadata = fs::metadata(&file2_path).await?;
            assert_eq!(file2_metadata.mode() & 0o777, FILE2_MODE);

            assert_eq!(file2_metadata.mtime() as u64, FILE2_MTIME);
        }
        Ok(())
    }

    #[tokio::test]
    async fn download_to_directory_folder_download_test() -> Result<(), Box<dyn std::error::Error>> {
        let (fast_store, slow_store, cas_store) = setup_stores().await?;

        const DIRECTORY1_NAME: &str = "folder1";
        const FILE1_NAME: &str = "file1.txt";
        const FILE1_CONTENT: &str = "HELLOFILE1";
        const DIRECTORY2_NAME: &str = "folder2";

        let root_directory_digest = {
            // Make and insert (into store) our digest info needed to create our directory & files.
            let directory1_digest = DigestInfo::new([01u8; 32], 32);
            {
                let file1_content_digest = DigestInfo::new([02u8; 32], 32);
                slow_store
                    .as_ref()
                    .update_oneshot(file1_content_digest.clone(), FILE1_CONTENT.into())
                    .await?;
                let directory1 = Directory {
                    files: vec![FileNode {
                        name: FILE1_NAME.to_string(),
                        digest: Some(file1_content_digest.into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                slow_store
                    .as_ref()
                    .update_oneshot(directory1_digest.clone(), directory1.encode_to_vec().into())
                    .await?;
            }
            let directory2_digest = DigestInfo::new([03u8; 32], 32);
            {
                // Now upload an empty directory.
                slow_store
                    .as_ref()
                    .update_oneshot(directory2_digest.clone(), Directory::default().encode_to_vec().into())
                    .await?;
            }
            let root_directory_digest = DigestInfo::new([05u8; 32], 32);
            {
                let root_directory = Directory {
                    directories: vec![
                        DirectoryNode {
                            name: DIRECTORY1_NAME.to_string(),
                            digest: Some(directory1_digest.into()),
                        },
                        DirectoryNode {
                            name: DIRECTORY2_NAME.to_string(),
                            digest: Some(directory2_digest.into()),
                        },
                    ],
                    ..Default::default()
                };
                slow_store
                    .as_ref()
                    .update_oneshot(root_directory_digest.clone(), root_directory.encode_to_vec().into())
                    .await?;
            }
            root_directory_digest
        };

        let download_dir = {
            // Tell it to download the digest info to a directory.
            let download_dir = make_temp_path("download_dir");
            fs::create_dir_all(&download_dir)
                .await
                .err_tip(|| format!("Could not make download_dir : {}", download_dir))?;
            download_to_directory(
                cas_store.as_ref(),
                fast_store.as_ref(),
                &root_directory_digest,
                &download_dir,
            )
            .await?;
            download_dir
        };
        {
            // Now ensure that our download_dir has the files.
            let file1_content = fs::read(format!("{}/{}/{}", download_dir, DIRECTORY1_NAME, FILE1_NAME))
                .await
                .err_tip(|| "On file_1 read")?;
            assert_eq!(std::str::from_utf8(&file1_content)?, FILE1_CONTENT);

            let folder2_path = format!("{}/{}", download_dir, DIRECTORY2_NAME);
            let folder2_metadata = fs::metadata(&folder2_path)
                .await
                .err_tip(|| "On folder2_metadata metadata")?;
            assert_eq!(folder2_metadata.is_dir(), true);
        }
        Ok(())
    }

    #[tokio::test]
    async fn download_to_directory_symlink_download_test() -> Result<(), Box<dyn std::error::Error>> {
        let (fast_store, slow_store, cas_store) = setup_stores().await?;

        const FILE_NAME: &str = "file.txt";
        const FILE_CONTENT: &str = "HELLOFILE";
        const SYMLINK_NAME: &str = "symlink_file.txt";
        const SYMLINK_TARGET: &str = "file.txt";

        let root_directory_digest = {
            // Make and insert (into store) our digest info needed to create our directory & files.
            let file_content_digest = DigestInfo::new([01u8; 32], 32);
            slow_store
                .as_ref()
                .update_oneshot(file_content_digest.clone(), FILE_CONTENT.into())
                .await?;

            let root_directory_digest = DigestInfo::new([02u8; 32], 32);
            let root_directory = Directory {
                files: vec![FileNode {
                    name: FILE_NAME.to_string(),
                    digest: Some(file_content_digest.into()),
                    is_executable: false,
                    node_properties: None,
                }],
                symlinks: vec![SymlinkNode {
                    name: SYMLINK_NAME.to_string(),
                    target: SYMLINK_TARGET.to_string(),
                    node_properties: None,
                }],
                ..Default::default()
            };

            slow_store
                .as_ref()
                .update_oneshot(root_directory_digest.clone(), root_directory.encode_to_vec().into())
                .await?;
            root_directory_digest
        };

        let download_dir = {
            // Tell it to download the digest info to a directory.
            let download_dir = make_temp_path("download_dir");
            fs::create_dir_all(&download_dir)
                .await
                .err_tip(|| format!("Could not make download_dir : {}", download_dir))?;
            download_to_directory(
                cas_store.as_ref(),
                fast_store.as_ref(),
                &root_directory_digest,
                &download_dir,
            )
            .await?;
            download_dir
        };
        {
            // Now ensure that our download_dir has the files.
            let symlink_path = format!("{}/{}", download_dir, SYMLINK_NAME);
            let symlink_content = fs::read(&symlink_path).await.err_tip(|| "On symlink read")?;
            assert_eq!(std::str::from_utf8(&symlink_content)?, FILE_CONTENT);

            let symlink_metadata = fs::symlink_metadata(&symlink_path)
                .await
                .err_tip(|| "On symlink symlink_metadata")?;
            assert_eq!(symlink_metadata.is_symlink(), true);
        }
        Ok(())
    }
}
