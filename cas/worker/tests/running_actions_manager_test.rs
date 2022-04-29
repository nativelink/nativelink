// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::env;
use std::os::unix::fs::MetadataExt;
use std::pin::Pin;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::{FutureExt, TryFutureExt};
use prost::Message;
use rand::{thread_rng, Rng};

use ac_utils::{get_and_decode_digest, serialize_and_upload_message};
use action_messages::{ActionResult, DirectoryInfo, ExecutionMetadata, FileInfo, NameOrPath, SymlinkInfo};
use common::{fs, DigestInfo};
use config;
use error::{Error, ResultExt};
use fast_slow_store::FastSlowStore;
use filesystem_store::FilesystemStore;
use memory_store::MemoryStore;
use proto::build::bazel::remote::execution::v2::{
    Action, Command, Directory, DirectoryNode, ExecuteRequest, FileNode, NodeProperties, SymlinkNode, Tree,
};
use proto::com::github::allada::turbo_cache::remote_execution::StartExecute;
use running_actions_manager::{download_to_directory, RunningAction, RunningActionsManager, RunningActionsManagerImpl};
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
            // Note: We sent 0o710, but because is_executable was set it turns into 0o711.
            assert_eq!(file2_metadata.mode() & 0o777, FILE2_MODE | 0o111);

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

    #[tokio::test]
    async fn upload_files_from_above_cwd_test() -> Result<(), Box<dyn std::error::Error>> {
        let (_, slow_store, cas_store) = setup_stores().await?;
        let root_work_directory = make_temp_path("root_work_directory");
        fs::create_dir_all(&root_work_directory).await?;
        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new(
            root_work_directory,
            Pin::into_inner(cas_store.clone()),
        )?);
        const WORKER_ID: &str = "foo_worker_id";
        let action_result = {
            const SALT: u64 = 55;
            let command = Command {
                arguments: vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "echo -n 123 > ../test.txt; echo -n foo-stdout; >&2 echo -n bar-stderr".to_string(),
                ],
                output_paths: vec!["test.txt".to_string()],
                working_directory: "some_cwd".to_string(),
                ..Default::default()
            };
            let command_digest = serialize_and_upload_message(&command, cas_store.as_ref()).await?;
            let input_root_digest = serialize_and_upload_message(
                &Directory {
                    directories: vec![DirectoryNode {
                        name: "some_cwd".to_string(),
                        digest: Some(
                            serialize_and_upload_message(&Directory::default(), cas_store.as_ref())
                                .await?
                                .into(),
                        ),
                    }],
                    ..Default::default()
                },
                cas_store.as_ref(),
            )
            .await?;
            let action = Action {
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input_root_digest.into()),
                ..Default::default()
            };
            let action_digest = serialize_and_upload_message(&action, cas_store.as_ref()).await?;

            let running_action_impl = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(ExecuteRequest {
                            action_digest: Some(action_digest.into()),
                            ..Default::default()
                        }),
                        salt: SALT,
                    },
                )
                .await?;

            running_action_impl
                .clone()
                .prepare_action()
                .and_then(|action| action.execute())
                .and_then(|action| action.upload_results())
                .and_then(|action| action.get_finished_result())
                .then(|result| async move {
                    running_action_impl.cleanup().await?;
                    result
                })
                .await?
        };
        let file_content = slow_store
            .as_ref()
            .get_part_unchunked(action_result.output_files[0].digest.clone(), 0, None, None)
            .await?;
        assert_eq!(from_utf8(&file_content)?, "123");
        let stdout_content = slow_store
            .as_ref()
            .get_part_unchunked(action_result.stdout_digest.clone(), 0, None, None)
            .await?;
        assert_eq!(from_utf8(&stdout_content)?, "foo-stdout");
        let stderr_content = slow_store
            .as_ref()
            .get_part_unchunked(action_result.stderr_digest.clone(), 0, None, None)
            .await?;
        assert_eq!(from_utf8(&stderr_content)?, "bar-stderr");
        assert_eq!(
            action_result,
            ActionResult {
                output_files: vec![FileInfo {
                    name_or_path: NameOrPath::Path("test.txt".to_string()),
                    digest: DigestInfo::try_new("a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3", 3)?,
                    is_executable: false,
                }],
                stdout_digest: DigestInfo::try_new(
                    "426afaf613d8cfdd9fa8addcc030ae6c95a7950ae0301164af1d5851012081d5",
                    10
                )?,
                stderr_digest: DigestInfo::try_new(
                    "7b2e400d08b8e334e3172d105be308b506c6036c62a9bde5c509d7808b28b213",
                    10
                )?,
                exit_code: 0,
                output_folders: vec![],
                output_file_symlinks: vec![],
                output_directory_symlinks: vec![],
                server_logs: HashMap::new(),
                execution_metadata: ExecutionMetadata {
                    worker: WORKER_ID.to_string(),
                    queued_timestamp: SystemTime::UNIX_EPOCH,
                    worker_start_timestamp: SystemTime::UNIX_EPOCH,
                    worker_completed_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_start_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_completed_timestamp: SystemTime::UNIX_EPOCH,
                    execution_start_timestamp: SystemTime::UNIX_EPOCH,
                    execution_completed_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_start_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_completed_timestamp: SystemTime::UNIX_EPOCH,
                }
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn upload_dir_and_symlink_test() -> Result<(), Box<dyn std::error::Error>> {
        let (_, slow_store, cas_store) = setup_stores().await?;
        let root_work_directory = make_temp_path("root_work_directory");
        fs::create_dir_all(&root_work_directory).await?;
        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new(
            root_work_directory,
            Pin::into_inner(cas_store.clone()),
        )?);
        const WORKER_ID: &str = "foo_worker_id";
        let action_result = {
            const SALT: u64 = 55;
            let command = Command {
                arguments: vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    concat!(
                        "mkdir -p dir1/dir2 && ",
                        "echo foo > dir1/file && ",
                        "touch dir1/file2 && ",
                        "ln -s ../file dir1/dir2/sym &&",
                        "ln -s /dev/null empty_sym",
                    )
                    .to_string(),
                ],
                output_paths: vec!["dir1".to_string(), "empty_sym".to_string()],
                working_directory: ".".to_string(),
                ..Default::default()
            };
            let command_digest = serialize_and_upload_message(&command, cas_store.as_ref()).await?;
            let input_root_digest = serialize_and_upload_message(&Directory::default(), cas_store.as_ref()).await?;
            let action = Action {
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input_root_digest.into()),
                ..Default::default()
            };
            let action_digest = serialize_and_upload_message(&action, cas_store.as_ref()).await?;

            let running_action_impl = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(ExecuteRequest {
                            action_digest: Some(action_digest.into()),
                            ..Default::default()
                        }),
                        salt: SALT,
                    },
                )
                .await?;

            running_action_impl
                .clone()
                .prepare_action()
                .and_then(|action| action.execute())
                .and_then(|action| action.upload_results())
                .and_then(|action| action.get_finished_result())
                .then(|result| async move {
                    running_action_impl.cleanup().await?;
                    result
                })
                .await?
        };
        let tree =
            get_and_decode_digest::<Tree>(slow_store.as_ref(), &action_result.output_folders[0].tree_digest).await?;
        let root_directory = Directory {
            files: vec![
                FileNode {
                    name: "file".to_string(),
                    digest: Some(
                        DigestInfo::try_new("b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c", 4)?
                            .into(),
                    ),
                    ..Default::default()
                },
                FileNode {
                    name: "file2".to_string(),
                    digest: Some(
                        DigestInfo::try_new("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)?
                            .into(),
                    ),
                    ..Default::default()
                },
            ],
            directories: vec![DirectoryNode {
                name: "dir2".to_string(),
                digest: Some(
                    DigestInfo::try_new("cce0098e0b0f1d785edb0da50beedb13e27dcd459b091b2f8f82543cb7cd0527", 16)?.into(),
                ),
            }],
            ..Default::default()
        };
        assert_eq!(
            tree,
            Tree {
                root: Some(root_directory.clone()),
                children: vec![
                    Directory {
                        symlinks: vec![SymlinkNode {
                            name: "sym".to_string(),
                            target: "../file".to_string(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    root_directory
                ],
                ..Default::default()
            }
        );
        assert_eq!(
            action_result,
            ActionResult {
                output_files: vec![],
                stdout_digest: DigestInfo::try_new(
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    0
                )?,
                stderr_digest: DigestInfo::try_new(
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    0
                )?,
                exit_code: 0,
                output_folders: vec![DirectoryInfo {
                    path: "dir1".to_string(),
                    tree_digest: DigestInfo::try_new(
                        "adbb04fa6e166e663c1310bbf8ba494e468b1b6c33e1e5346e2216b6904c9917",
                        490
                    )?,
                }],
                output_file_symlinks: vec![SymlinkInfo {
                    name_or_path: NameOrPath::Path("empty_sym".to_string()),
                    target: "/dev/null".to_string(),
                }],
                output_directory_symlinks: vec![],
                server_logs: HashMap::new(),
                execution_metadata: ExecutionMetadata {
                    worker: WORKER_ID.to_string(),
                    queued_timestamp: SystemTime::UNIX_EPOCH,
                    worker_start_timestamp: SystemTime::UNIX_EPOCH,
                    worker_completed_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_start_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_completed_timestamp: SystemTime::UNIX_EPOCH,
                    execution_start_timestamp: SystemTime::UNIX_EPOCH,
                    execution_completed_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_start_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_completed_timestamp: SystemTime::UNIX_EPOCH,
                }
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_happens_on_job_failure() -> Result<(), Box<dyn std::error::Error>> {
        let (_, _, cas_store) = setup_stores().await?;
        let root_work_directory = make_temp_path("root_work_directory");
        fs::create_dir_all(&root_work_directory).await?;
        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new(
            root_work_directory.clone(),
            Pin::into_inner(cas_store.clone()),
        )?);
        const WORKER_ID: &str = "foo_worker_id";
        let action_result = {
            const SALT: u64 = 55;
            let command = Command {
                arguments: vec!["sh".to_string(), "-c".to_string(), "exit 33".to_string()],
                output_paths: vec![],
                working_directory: ".".to_string(),
                ..Default::default()
            };
            let command_digest = serialize_and_upload_message(&command, cas_store.as_ref()).await?;
            let input_root_digest = serialize_and_upload_message(&Directory::default(), cas_store.as_ref()).await?;
            let action = Action {
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input_root_digest.into()),
                ..Default::default()
            };
            let action_digest = serialize_and_upload_message(&action, cas_store.as_ref()).await?;

            let running_action_impl = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(ExecuteRequest {
                            action_digest: Some(action_digest.into()),
                            ..Default::default()
                        }),
                        salt: SALT,
                    },
                )
                .await?;

            running_action_impl
                .clone()
                .prepare_action()
                .and_then(|action| action.execute())
                .and_then(|action| action.upload_results())
                .and_then(|action| action.get_finished_result())
                .then(|result| async move {
                    running_action_impl.cleanup().await?;
                    result
                })
                .await?
        };
        assert_eq!(
            action_result,
            ActionResult {
                output_files: vec![],
                stdout_digest: DigestInfo::try_new(
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    0
                )?,
                stderr_digest: DigestInfo::try_new(
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    0
                )?,
                exit_code: 33,
                output_folders: vec![],
                output_file_symlinks: vec![],
                output_directory_symlinks: vec![],
                server_logs: HashMap::new(),
                execution_metadata: ExecutionMetadata {
                    worker: WORKER_ID.to_string(),
                    queued_timestamp: SystemTime::UNIX_EPOCH,
                    worker_start_timestamp: SystemTime::UNIX_EPOCH,
                    worker_completed_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_start_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_completed_timestamp: SystemTime::UNIX_EPOCH,
                    execution_start_timestamp: SystemTime::UNIX_EPOCH,
                    execution_completed_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_start_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_completed_timestamp: SystemTime::UNIX_EPOCH,
                }
            }
        );
        let mut dir_stream = fs::read_dir(&root_work_directory).await?;
        assert!(
            dir_stream.as_mut().next_entry().await?.is_none(),
            "Expected empty directory at {}",
            root_work_directory
        );
        Ok(())
    }
}
