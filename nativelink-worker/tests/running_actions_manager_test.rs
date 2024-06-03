// Copyright 2023 The NativeLink Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::env;
use std::ffi::OsString;
#[cfg(target_family = "unix")]
use std::fs::Permissions;
use std::io::{Cursor, Write};
#[cfg(target_family = "unix")]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::pin::Pin;
use std::str::from_utf8;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use nativelink_config::cas_server::EnvironmentSource;
use nativelink_error::{make_input_err, Code, Error, ResultExt};
use nativelink_macro::nativelink_test;
#[cfg_attr(target_family = "windows", allow(unused_imports))]
use nativelink_proto::build::bazel::remote::execution::v2::{
    digest_function::Value as ProtoDigestFunction, platform::Property, Action,
    ActionResult as ProtoActionResult, Command, Directory, DirectoryNode, ExecuteRequest,
    ExecuteResponse, FileNode, NodeProperties, Platform, SymlinkNode, Tree,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    HistoricalExecuteResponse, StartExecute,
};
use nativelink_store::ac_utils::{get_and_decode_digest, serialize_and_upload_message};
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::memory_store::MemoryStore;
#[cfg_attr(target_family = "windows", allow(unused_imports))]
use nativelink_util::action_messages::{
    ActionResult, DirectoryInfo, ExecutionMetadata, FileInfo, NameOrPath, SymlinkInfo,
};
use nativelink_util::common::{fs, DigestInfo};
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::store_trait::Store;
use nativelink_worker::running_actions_manager::{
    download_to_directory, Callbacks, ExecutionConfiguration, RunningAction, RunningActionImpl,
    RunningActionsManager, RunningActionsManagerArgs, RunningActionsManagerImpl,
};
use once_cell::sync::Lazy;
use prost::Message;
use rand::{thread_rng, Rng};
use tokio::sync::oneshot;

/// Get temporary path from either `TEST_TMPDIR` or best effort temp directory if
/// not set.
fn make_temp_path(data: &str) -> String {
    #[cfg(target_family = "unix")]
    return format!(
        "{}/{}/{}",
        env::var("TEST_TMPDIR").unwrap_or(env::temp_dir().to_str().unwrap().to_string()),
        thread_rng().gen::<u64>(),
        data
    );
    #[cfg(target_family = "windows")]
    return format!(
        "{}\\{}\\{}",
        env::var("TEST_TMPDIR").unwrap_or(env::temp_dir().to_str().unwrap().to_string()),
        thread_rng().gen::<u64>(),
        data
    );
}

async fn setup_stores() -> Result<
    (
        Pin<Arc<FilesystemStore>>,
        Pin<Arc<MemoryStore>>,
        Pin<Arc<FastSlowStore>>,
        Pin<Arc<MemoryStore>>,
    ),
    Error,
> {
    let fast_config = nativelink_config::stores::FilesystemStore {
        content_path: make_temp_path("content_path"),
        temp_path: make_temp_path("temp_path"),
        eviction_policy: None,
        ..Default::default()
    };
    let slow_config = nativelink_config::stores::MemoryStore::default();
    let fast_store = Pin::new(Arc::new(FilesystemStore::new(&fast_config).await?));
    let slow_store = Pin::new(Arc::new(MemoryStore::new(&slow_config)));
    let ac_store = Pin::new(Arc::new(MemoryStore::new(&slow_config)));
    let cas_store = Pin::new(Arc::new(FastSlowStore::new(
        &nativelink_config::stores::FastSlowStore {
            fast: nativelink_config::stores::StoreConfigOptions::filesystem(fast_config),
            slow: nativelink_config::stores::StoreConfigOptions::memory(slow_config),
        },
        Pin::into_inner(fast_store.clone()),
        Pin::into_inner(slow_store.clone()),
    )));
    Ok((fast_store, slow_store, cas_store, ac_store))
}

async fn run_action(action: Arc<RunningActionImpl>) -> Result<ActionResult, Error> {
    action
        .clone()
        .prepare_action()
        .and_then(RunningAction::execute)
        .and_then(RunningAction::upload_results)
        .and_then(RunningAction::get_finished_result)
        .then(|result| async move {
            action.cleanup().await?;
            result
        })
        .await
}

const NOW_TIME: u64 = 10000;

fn make_system_time(add_time: u64) -> SystemTime {
    UNIX_EPOCH
        .checked_add(Duration::from_secs(NOW_TIME + add_time))
        .unwrap()
}

fn monotonic_clock(counter: &AtomicU64) -> SystemTime {
    let count = counter.fetch_add(1, Ordering::Relaxed);
    make_system_time(count)
}

fn increment_clock(time: &mut SystemTime) -> SystemTime {
    let previous_time = *time;
    *time = previous_time.checked_add(Duration::from_secs(1)).unwrap();
    previous_time
}

#[cfg(test)]
mod running_actions_manager_tests {
    use pretty_assertions::assert_eq;

    use super::*; // Must be declared in every module.

    #[nativelink_test]
    async fn download_to_directory_file_download_test() -> Result<(), Box<dyn std::error::Error>> {
        const FILE1_NAME: &str = "file1.txt";
        const FILE1_CONTENT: &str = "HELLOFILE1";
        const FILE2_NAME: &str = "file2.exec";
        const FILE2_CONTENT: &str = "HELLOFILE2";
        const FILE2_MODE: u32 = 0o710;
        const FILE2_MTIME: u64 = 5;

        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let root_directory_digest = {
            // Make and insert (into store) our digest info needed to create our directory & files.
            let file1_content_digest = DigestInfo::new([2u8; 32], 32);
            slow_store
                .as_ref()
                .update_oneshot(file1_content_digest, FILE1_CONTENT.into())
                .await?;
            let file2_content_digest = DigestInfo::new([3u8; 32], 32);
            slow_store
                .as_ref()
                .update_oneshot(file2_content_digest, FILE2_CONTENT.into())
                .await?;

            let root_directory_digest = DigestInfo::new([1u8; 32], 32);
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
                .update_oneshot(root_directory_digest, root_directory.encode_to_vec().into())
                .await?;
            root_directory_digest
        };

        let download_dir = {
            // Tell it to download the digest info to a directory.
            let download_dir = make_temp_path("download_dir");
            fs::create_dir_all(&download_dir)
                .await
                .err_tip(|| format!("Could not make download_dir : {download_dir}"))?;
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
            let file1_content = fs::read(format!("{download_dir}/{FILE1_NAME}")).await?;
            assert_eq!(std::str::from_utf8(&file1_content)?, FILE1_CONTENT);

            let file2_path = format!("{download_dir}/{FILE2_NAME}");
            let file2_content = fs::read(&file2_path).await?;
            assert_eq!(std::str::from_utf8(&file2_content)?, FILE2_CONTENT);

            let file2_metadata = fs::metadata(&file2_path).await?;
            // Note: We sent 0o710, but because is_executable was set it turns into 0o711.
            #[cfg(target_family = "unix")]
            assert_eq!(file2_metadata.mode() & 0o777, FILE2_MODE | 0o111);
            assert_eq!(
                file2_metadata
                    .modified()?
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_secs(),
                FILE2_MTIME
            );
        }
        Ok(())
    }

    #[nativelink_test]
    async fn download_to_directory_folder_download_test() -> Result<(), Box<dyn std::error::Error>>
    {
        const DIRECTORY1_NAME: &str = "folder1";
        const FILE1_NAME: &str = "file1.txt";
        const FILE1_CONTENT: &str = "HELLOFILE1";
        const DIRECTORY2_NAME: &str = "folder2";

        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let root_directory_digest = {
            // Make and insert (into store) our digest info needed to create our directory & files.
            let directory1_digest = DigestInfo::new([1u8; 32], 32);
            {
                let file1_content_digest = DigestInfo::new([2u8; 32], 32);
                slow_store
                    .as_ref()
                    .update_oneshot(file1_content_digest, FILE1_CONTENT.into())
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
                    .update_oneshot(directory1_digest, directory1.encode_to_vec().into())
                    .await?;
            }
            let directory2_digest = DigestInfo::new([3u8; 32], 32);
            {
                // Now upload an empty directory.
                slow_store
                    .as_ref()
                    .update_oneshot(
                        directory2_digest,
                        Directory::default().encode_to_vec().into(),
                    )
                    .await?;
            }
            let root_directory_digest = DigestInfo::new([5u8; 32], 32);
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
                    .update_oneshot(root_directory_digest, root_directory.encode_to_vec().into())
                    .await?;
            }
            root_directory_digest
        };

        let download_dir = {
            // Tell it to download the digest info to a directory.
            let download_dir = make_temp_path("download_dir");
            fs::create_dir_all(&download_dir)
                .await
                .err_tip(|| format!("Could not make download_dir : {download_dir}"))?;
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
            let file1_content = fs::read(format!("{download_dir}/{DIRECTORY1_NAME}/{FILE1_NAME}"))
                .await
                .err_tip(|| "On file_1 read")?;
            assert_eq!(std::str::from_utf8(&file1_content)?, FILE1_CONTENT);

            let folder2_path = format!("{download_dir}/{DIRECTORY2_NAME}");
            let folder2_metadata = fs::metadata(&folder2_path)
                .await
                .err_tip(|| "On folder2_metadata metadata")?;
            assert_eq!(folder2_metadata.is_dir(), true);
        }
        Ok(())
    }

    // Windows does not support symlinks.
    #[cfg(not(target_family = "windows"))]
    #[nativelink_test]
    async fn download_to_directory_symlink_download_test() -> Result<(), Box<dyn std::error::Error>>
    {
        const FILE_NAME: &str = "file.txt";
        const FILE_CONTENT: &str = "HELLOFILE";
        const SYMLINK_NAME: &str = "symlink_file.txt";
        const SYMLINK_TARGET: &str = "file.txt";

        let (fast_store, slow_store, cas_store, _ac_store) = setup_stores().await?;

        let root_directory_digest = {
            // Make and insert (into store) our digest info needed to create our directory & files.
            let file_content_digest = DigestInfo::new([1u8; 32], 32);
            slow_store
                .as_ref()
                .update_oneshot(file_content_digest, FILE_CONTENT.into())
                .await?;

            let root_directory_digest = DigestInfo::new([2u8; 32], 32);
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
                .update_oneshot(root_directory_digest, root_directory.encode_to_vec().into())
                .await?;
            root_directory_digest
        };

        let download_dir = {
            // Tell it to download the digest info to a directory.
            let download_dir = make_temp_path("download_dir");
            fs::create_dir_all(&download_dir)
                .await
                .err_tip(|| format!("Could not make download_dir : {download_dir}"))?;
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
            let symlink_path = format!("{download_dir}/{SYMLINK_NAME}");
            let symlink_content = fs::read(&symlink_path)
                .await
                .err_tip(|| "On symlink read")?;
            assert_eq!(std::str::from_utf8(&symlink_content)?, FILE_CONTENT);

            let symlink_metadata = fs::symlink_metadata(&symlink_path)
                .await
                .err_tip(|| "On symlink symlink_metadata")?;
            assert_eq!(symlink_metadata.is_symlink(), true);
        }
        Ok(())
    }

    #[nativelink_test]
    async fn ensure_output_files_full_directories_are_created_no_working_directory_test(
    ) -> Result<(), Box<dyn std::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
            RunningActionsManagerArgs {
                root_action_directory,
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(futures::future::pending()),
            },
        )?);
        {
            const SALT: u64 = 55;
            let command = Command {
                arguments: vec!["touch".to_string(), "./some/path/test.txt".to_string()],
                output_files: vec!["some/path/test.txt".to_string()],
                ..Default::default()
            };
            let command_digest = serialize_and_upload_message(
                &command,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            let input_root_digest = serialize_and_upload_message(
                &Directory {
                    directories: vec![DirectoryNode {
                        name: "some_cwd".to_string(),
                        digest: Some(
                            serialize_and_upload_message(
                                &Directory::default(),
                                cas_store.as_ref(),
                                &mut DigestHasherFunc::Sha256.hasher(),
                            )
                            .await?
                            .into(),
                        ),
                    }],
                    ..Default::default()
                },
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            let action = Action {
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input_root_digest.into()),
                ..Default::default()
            };
            let action_digest = serialize_and_upload_message(
                &action,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;

            let running_action = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(ExecuteRequest {
                            action_digest: Some(action_digest.into()),
                            digest_function: ProtoDigestFunction::Sha256.into(),
                            ..Default::default()
                        }),
                        salt: SALT,
                        queued_timestamp: None,
                    },
                )
                .await?;

            let running_action = running_action.clone().prepare_action().await?;

            // The folder should have been created for our output file.
            assert_eq!(
                fs::metadata(format!(
                    "{}/{}",
                    running_action.get_work_directory(),
                    "some/path"
                ))
                .await
                .is_ok(),
                true,
                "Expected path to exist"
            );

            running_action.cleanup().await?;
        };
        Ok(())
    }

    #[nativelink_test]
    async fn ensure_output_files_full_directories_are_created_test(
    ) -> Result<(), Box<dyn std::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
            RunningActionsManagerArgs {
                root_action_directory,
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(futures::future::pending()),
            },
        )?);
        {
            const SALT: u64 = 55;
            let working_directory = "some_cwd";
            let command = Command {
                arguments: vec!["touch".to_string(), "./some/path/test.txt".to_string()],
                output_files: vec!["some/path/test.txt".to_string()],
                working_directory: working_directory.to_string(),
                ..Default::default()
            };
            let command_digest = serialize_and_upload_message(
                &command,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            let input_root_digest = serialize_and_upload_message(
                &Directory {
                    directories: vec![DirectoryNode {
                        name: "some_cwd".to_string(),
                        digest: Some(
                            serialize_and_upload_message(
                                &Directory::default(),
                                cas_store.as_ref(),
                                &mut DigestHasherFunc::Sha256.hasher(),
                            )
                            .await?
                            .into(),
                        ),
                    }],
                    ..Default::default()
                },
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            let action = Action {
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input_root_digest.into()),
                ..Default::default()
            };
            let action_digest = serialize_and_upload_message(
                &action,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;

            let running_action = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(ExecuteRequest {
                            action_digest: Some(action_digest.into()),
                            digest_function: ProtoDigestFunction::Sha256.into(),
                            ..Default::default()
                        }),
                        salt: SALT,
                        queued_timestamp: None,
                    },
                )
                .await?;

            let running_action = running_action.clone().prepare_action().await?;

            // The folder should have been created for our output file.
            assert_eq!(
                fs::metadata(format!(
                    "{}/{}/{}",
                    running_action.get_work_directory(),
                    working_directory,
                    "some/path"
                ))
                .await
                .is_ok(),
                true,
                "Expected path to exist"
            );

            running_action.cleanup().await?;
        };
        Ok(())
    }

    #[nativelink_test]
    async fn blake3_upload_files() -> Result<(), Box<dyn std::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, slow_store, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
            RunningActionsManagerArgs {
                root_action_directory,
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(futures::future::pending()),
            },
        )?);
        let action_result = {
            const SALT: u64 = 55;
            #[cfg(target_family = "unix")]
            let arguments = vec![
                "sh".to_string(),
                "-c".to_string(),
                "printf '123 ' > ./test.txt; printf 'foo-stdout '; >&2 printf 'bar-stderr  '"
                    .to_string(),
            ];
            #[cfg(target_family = "windows")]
            let arguments = vec![
                "cmd".to_string(),
                "/C".to_string(),
                // Note: Windows adds two spaces after 'set /p=XXX'.
                "echo | set /p=123> ./test.txt & echo | set /p=foo-stdout & echo | set /p=bar-stderr 1>&2 & exit 0"
                    .to_string(),
            ];
            let working_directory = "some_cwd";
            let command = Command {
                arguments,
                output_paths: vec!["test.txt".to_string()],
                working_directory: working_directory.to_string(),
                ..Default::default()
            };
            let command_digest = serialize_and_upload_message(
                &command,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Blake3.hasher(),
            )
            .await?;
            let input_root_digest = serialize_and_upload_message(
                &Directory {
                    directories: vec![DirectoryNode {
                        name: working_directory.to_string(),
                        digest: Some(
                            serialize_and_upload_message(
                                &Directory::default(),
                                cas_store.as_ref(),
                                &mut DigestHasherFunc::Blake3.hasher(),
                            )
                            .await?
                            .into(),
                        ),
                    }],
                    ..Default::default()
                },
                cas_store.as_ref(),
                &mut DigestHasherFunc::Blake3.hasher(),
            )
            .await?;
            let action = Action {
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input_root_digest.into()),
                ..Default::default()
            };
            let action_digest = serialize_and_upload_message(
                &action,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Blake3.hasher(),
            )
            .await?;

            let running_action_impl = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(ExecuteRequest {
                            action_digest: Some(action_digest.into()),
                            digest_function: ProtoDigestFunction::Blake3.into(),
                            ..Default::default()
                        }),
                        salt: SALT,
                        queued_timestamp: None,
                    },
                )
                .await?;

            run_action(running_action_impl.clone()).await?
        };
        let file_content = slow_store
            .as_ref()
            .get_part_unchunked(action_result.output_files[0].digest, 0, None)
            .await?;
        assert_eq!(from_utf8(&file_content)?, "123 ");
        let stdout_content = slow_store
            .as_ref()
            .get_part_unchunked(action_result.stdout_digest, 0, None)
            .await?;
        assert_eq!(from_utf8(&stdout_content)?, "foo-stdout ");
        let stderr_content = slow_store
            .as_ref()
            .get_part_unchunked(action_result.stderr_digest, 0, None)
            .await?;
        assert_eq!(from_utf8(&stderr_content)?, "bar-stderr  ");
        let mut clock_time = make_system_time(0);
        assert_eq!(
            action_result,
            ActionResult {
                output_files: vec![FileInfo {
                    name_or_path: NameOrPath::Path("test.txt".to_string()),
                    digest: DigestInfo::try_new(
                        "3f488ba478fc6716c756922c9f34ebd7e84b85c3e03e33e22e7a3736cafdc6d8",
                        4
                    )?,
                    is_executable: false,
                }],
                stdout_digest: DigestInfo::try_new(
                    "af1720193ae81515067a3ef39f0dfda3ad54a1a9d216e55d32fe5c1e178c6a7d",
                    11
                )?,
                stderr_digest: DigestInfo::try_new(
                    "65e0abbae32a3aedaf040b654c6f02ace03c7690c17a8415a90fc2ec9c809a16",
                    12
                )?,
                exit_code: 0,
                output_folders: vec![],
                output_file_symlinks: vec![],
                output_directory_symlinks: vec![],
                server_logs: HashMap::new(),
                execution_metadata: ExecutionMetadata {
                    worker: WORKER_ID.to_string(),
                    queued_timestamp: SystemTime::UNIX_EPOCH,
                    worker_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_completed_timestamp: increment_clock(&mut clock_time),
                    execution_start_timestamp: increment_clock(&mut clock_time),
                    execution_completed_timestamp: increment_clock(&mut clock_time),
                    output_upload_start_timestamp: increment_clock(&mut clock_time),
                    output_upload_completed_timestamp: increment_clock(&mut clock_time),
                    worker_completed_timestamp: increment_clock(&mut clock_time),
                },
                error: None,
                message: String::new(),
            }
        );
        Ok(())
    }

    #[nativelink_test]
    async fn upload_files_from_above_cwd_test() -> Result<(), Box<dyn std::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, slow_store, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
            RunningActionsManagerArgs {
                root_action_directory,
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(futures::future::pending()),
            },
        )?);
        let action_result = {
            const SALT: u64 = 55;
            #[cfg(target_family = "unix")]
            let arguments = vec![
                "sh".to_string(),
                "-c".to_string(),
                "printf '123 ' > ./test.txt; printf 'foo-stdout '; >&2 printf 'bar-stderr  '"
                    .to_string(),
            ];
            #[cfg(target_family = "windows")]
            let arguments = vec![
                "cmd".to_string(),
                "/C".to_string(),
                // Note: Windows adds two spaces after 'set /p=XXX'.
                "echo | set /p=123> ./test.txt & echo | set /p=foo-stdout & echo | set /p=bar-stderr 1>&2 & exit 0"
                    .to_string(),
            ];
            let working_directory = "some_cwd";
            let command = Command {
                arguments,
                output_paths: vec!["test.txt".to_string()],
                working_directory: working_directory.to_string(),
                ..Default::default()
            };
            let command_digest = serialize_and_upload_message(
                &command,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            let input_root_digest = serialize_and_upload_message(
                &Directory {
                    directories: vec![DirectoryNode {
                        name: working_directory.to_string(),
                        digest: Some(
                            serialize_and_upload_message(
                                &Directory::default(),
                                cas_store.as_ref(),
                                &mut DigestHasherFunc::Sha256.hasher(),
                            )
                            .await?
                            .into(),
                        ),
                    }],
                    ..Default::default()
                },
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            let action = Action {
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input_root_digest.into()),
                ..Default::default()
            };
            let action_digest = serialize_and_upload_message(
                &action,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;

            let running_action_impl = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(ExecuteRequest {
                            action_digest: Some(action_digest.into()),
                            digest_function: ProtoDigestFunction::Sha256.into(),
                            ..Default::default()
                        }),
                        salt: SALT,
                        queued_timestamp: None,
                    },
                )
                .await?;

            run_action(running_action_impl.clone()).await?
        };
        let file_content = slow_store
            .as_ref()
            .get_part_unchunked(action_result.output_files[0].digest, 0, None)
            .await?;
        assert_eq!(from_utf8(&file_content)?, "123 ");
        let stdout_content = slow_store
            .as_ref()
            .get_part_unchunked(action_result.stdout_digest, 0, None)
            .await?;
        assert_eq!(from_utf8(&stdout_content)?, "foo-stdout ");
        let stderr_content = slow_store
            .as_ref()
            .get_part_unchunked(action_result.stderr_digest, 0, None)
            .await?;
        assert_eq!(from_utf8(&stderr_content)?, "bar-stderr  ");
        let mut clock_time = make_system_time(0);
        assert_eq!(
            action_result,
            ActionResult {
                output_files: vec![FileInfo {
                    name_or_path: NameOrPath::Path("test.txt".to_string()),
                    digest: DigestInfo::try_new(
                        "c69e10a5f54f4e28e33897fbd4f8701595443fa8c3004aeaa20dd4d9a463483b",
                        4
                    )?,
                    is_executable: false,
                }],
                stdout_digest: DigestInfo::try_new(
                    "15019a676f057d97d1ad3af86f3cc1e623cb33b18ff28422bbe3248d2471cc94",
                    11
                )?,
                stderr_digest: DigestInfo::try_new(
                    "2375ab8a01ca11e1ea7606dfb58756c153d49733cde1dbfb5a1e00f39afacf06",
                    12
                )?,
                exit_code: 0,
                output_folders: vec![],
                output_file_symlinks: vec![],
                output_directory_symlinks: vec![],
                server_logs: HashMap::new(),
                execution_metadata: ExecutionMetadata {
                    worker: WORKER_ID.to_string(),
                    queued_timestamp: SystemTime::UNIX_EPOCH,
                    worker_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_completed_timestamp: increment_clock(&mut clock_time),
                    execution_start_timestamp: increment_clock(&mut clock_time),
                    execution_completed_timestamp: increment_clock(&mut clock_time),
                    output_upload_start_timestamp: increment_clock(&mut clock_time),
                    output_upload_completed_timestamp: increment_clock(&mut clock_time),
                    worker_completed_timestamp: increment_clock(&mut clock_time),
                },
                error: None,
                message: String::new(),
            }
        );
        Ok(())
    }

    // Windows does not support symlinks.
    #[cfg(not(target_family = "windows"))]
    #[nativelink_test]
    async fn upload_dir_and_symlink_test() -> Result<(), Box<dyn std::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, slow_store, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
            RunningActionsManagerArgs {
                root_action_directory,
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(futures::future::pending()),
            },
        )?);
        let queued_timestamp = make_system_time(1000);
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
            let command_digest = serialize_and_upload_message(
                &command,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            let input_root_digest = serialize_and_upload_message(
                &Directory::default(),
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            let action = Action {
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input_root_digest.into()),
                ..Default::default()
            };
            let action_digest = serialize_and_upload_message(
                &action,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;

            let running_action_impl = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(ExecuteRequest {
                            action_digest: Some(action_digest.into()),
                            digest_function: ProtoDigestFunction::Sha256.into(),
                            ..Default::default()
                        }),
                        salt: SALT,
                        queued_timestamp: Some(queued_timestamp.into()),
                    },
                )
                .await?;

            run_action(running_action_impl.clone()).await?
        };
        let tree = get_and_decode_digest::<Tree>(
            slow_store.as_ref(),
            &action_result.output_folders[0].tree_digest,
        )
        .await?;
        let root_directory = Directory {
            files: vec![
                FileNode {
                    name: "file".to_string(),
                    digest: Some(
                        DigestInfo::try_new(
                            "b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c",
                            4,
                        )?
                        .into(),
                    ),
                    ..Default::default()
                },
                FileNode {
                    name: "file2".to_string(),
                    digest: Some(
                        DigestInfo::try_new(
                            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                            0,
                        )?
                        .into(),
                    ),
                    ..Default::default()
                },
            ],
            directories: vec![DirectoryNode {
                name: "dir2".to_string(),
                digest: Some(
                    DigestInfo::try_new(
                        "cce0098e0b0f1d785edb0da50beedb13e27dcd459b091b2f8f82543cb7cd0527",
                        16,
                    )?
                    .into(),
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
            }
        );
        let mut clock_time = make_system_time(0);
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
                    queued_timestamp,
                    worker_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_completed_timestamp: increment_clock(&mut clock_time),
                    execution_start_timestamp: increment_clock(&mut clock_time),
                    execution_completed_timestamp: increment_clock(&mut clock_time),
                    output_upload_start_timestamp: increment_clock(&mut clock_time),
                    output_upload_completed_timestamp: increment_clock(&mut clock_time),
                    worker_completed_timestamp: increment_clock(&mut clock_time),
                },
                error: None,
                message: String::new(),
            }
        );
        Ok(())
    }

    #[nativelink_test]
    async fn cleanup_happens_on_job_failure() -> Result<(), Box<dyn std::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
            RunningActionsManagerArgs {
                root_action_directory: root_action_directory.clone(),
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(futures::future::pending()),
            },
        )?);
        let queued_timestamp = make_system_time(1000);

        #[cfg(target_family = "unix")]
        let arguments = vec!["sh".to_string(), "-c".to_string(), "exit 33".to_string()];
        #[cfg(target_family = "windows")]
        let arguments = vec!["cmd".to_string(), "/C".to_string(), "exit 33".to_string()];

        let action_result = {
            const SALT: u64 = 55;
            let command = Command {
                arguments,
                output_paths: vec![],
                working_directory: ".".to_string(),
                ..Default::default()
            };
            let command_digest = serialize_and_upload_message(
                &command,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            let input_root_digest = serialize_and_upload_message(
                &Directory::default(),
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            let action = Action {
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input_root_digest.into()),
                ..Default::default()
            };
            let action_digest = serialize_and_upload_message(
                &action,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;

            let running_action_impl = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(ExecuteRequest {
                            action_digest: Some(action_digest.into()),
                            digest_function: ProtoDigestFunction::Sha256.into(),
                            ..Default::default()
                        }),
                        salt: SALT,
                        queued_timestamp: Some(queued_timestamp.into()),
                    },
                )
                .await?;

            run_action(running_action_impl.clone()).await?
        };
        let mut clock_time = make_system_time(0);
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
                    queued_timestamp,
                    worker_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_completed_timestamp: increment_clock(&mut clock_time),
                    execution_start_timestamp: increment_clock(&mut clock_time),
                    execution_completed_timestamp: increment_clock(&mut clock_time),
                    output_upload_start_timestamp: increment_clock(&mut clock_time),
                    output_upload_completed_timestamp: increment_clock(&mut clock_time),
                    worker_completed_timestamp: increment_clock(&mut clock_time),
                },
                error: None,
                message: String::new(),
            }
        );
        let mut dir_stream = fs::read_dir(&root_action_directory).await?;
        assert!(
            dir_stream.as_mut().next_entry().await?.is_none(),
            "Expected empty directory at {root_action_directory}"
        );
        Ok(())
    }

    #[nativelink_test]
    async fn kill_ends_action() -> Result<(), Box<dyn std::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";
        const SALT: u64 = 55;

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: root_action_directory.clone(),
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            })?);

        #[cfg(target_family = "unix")]
        let arguments = vec![
            "sh".to_string(),
            "-c".to_string(),
            "sleep infinity".to_string(),
        ];
        #[cfg(target_family = "windows")]
        // Windows is weird with timeout, so we use ping. See:
        // https://www.ibm.com/support/pages/timeout-command-run-batch-job-exits-immediately-and-returns-error-input-redirection-not-supported-exiting-process-immediately
        let arguments = vec![
            "cmd".to_string(),
            "/C".to_string(),
            "ping -n 99999 127.0.0.1".to_string(),
        ];

        let command = Command {
            arguments,
            output_paths: vec![],
            working_directory: ".".to_string(),
            ..Default::default()
        };
        let command_digest = serialize_and_upload_message(
            &command,
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let action = Action {
            command_digest: Some(command_digest.into()),
            input_root_digest: Some(input_root_digest.into()),
            ..Default::default()
        };
        let action_digest = serialize_and_upload_message(
            &action,
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        let running_action_impl = running_actions_manager
            .clone()
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(ExecuteRequest {
                        action_digest: Some(action_digest.into()),
                        digest_function: ProtoDigestFunction::Sha256.into(),
                        ..Default::default()
                    }),
                    salt: SALT,
                    queued_timestamp: Some(make_system_time(1000).into()),
                },
            )
            .await?;

        // Start the action and kill it at the same time.
        let result = futures::join!(
            run_action(running_action_impl),
            running_actions_manager.kill_all()
        )
        .0?;

        // Check that the action was killed.
        #[cfg(target_family = "unix")]
        assert_eq!(9, result.exit_code);
        // Note: Windows kill command returns exit code 1.
        #[cfg(target_family = "windows")]
        assert_eq!(1, result.exit_code);

        Ok(())
    }

    // This script runs a command under a wrapper script set in a config.
    // The wrapper script will print a constant string to stderr, and the test itself will
    // print to stdout. We then check the results of both to make sure the shell script was
    // invoked and the actual command was invoked under the shell script.
    #[nativelink_test]
    async fn entrypoint_does_invoke_if_set() -> Result<(), Box<dyn std::error::Error>> {
        #[cfg(target_family = "unix")]
        const TEST_WRAPPER_SCRIPT_CONTENT: &str = "\
#!/bin/bash
# Print some static text to stderr. This is what the test uses to
# make sure the script did run.
>&2 printf \"Wrapper script did run\"

# Now run the real command.
exec \"$@\"
";
        #[cfg(target_family = "windows")]
        const TEST_WRAPPER_SCRIPT_CONTENT: &str = "\
@echo off
:: Print some static text to stderr. This is what the test uses to
:: make sure the script did run.
echo | set /p=\"Wrapper script did run\" 1>&2

:: Run command, but morph the echo to ensure it doesn't
:: add a new line to the end of the output.
%1 | set /p=%2
exit 0
";
        const WORKER_ID: &str = "foo_worker_id";
        const SALT: u64 = 66;
        const EXPECTED_STDOUT: &str = "Action did run";

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let test_wrapper_script = {
            let test_wrapper_dir = make_temp_path("wrapper_dir");
            fs::create_dir_all(&test_wrapper_dir).await?;
            #[cfg(target_family = "unix")]
            let test_wrapper_script = OsString::from(test_wrapper_dir + "/test_wrapper_script.sh");
            #[cfg(target_family = "windows")]
            let test_wrapper_script =
                OsString::from(test_wrapper_dir + "\\test_wrapper_script.bat");

            // We use std::fs::File here because we sometimes get strange bugs here
            // that result in: "Text file busy (os error 26)" if it is an executeable.
            // It is likley because somewhere the file descriotor does not get closed
            // in tokio's async context.
            let mut test_wrapper_script_handle = std::fs::File::create(&test_wrapper_script)?;
            test_wrapper_script_handle.write_all(TEST_WRAPPER_SCRIPT_CONTENT.as_bytes())?;
            #[cfg(target_family = "unix")]
            test_wrapper_script_handle.set_permissions(Permissions::from_mode(0o777))?;
            test_wrapper_script_handle.sync_all()?;
            drop(test_wrapper_script_handle);

            test_wrapper_script
        };

        // TODO(#527) Sleep to reduce flakey chances.
        tokio::time::sleep(Duration::from_millis(250)).await;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: root_action_directory.clone(),
                execution_configuration: ExecutionConfiguration {
                    entrypoint: Some(test_wrapper_script.into_string().unwrap()),
                    additional_environment: None,
                },
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            })?);
        #[cfg(target_family = "unix")]
        let arguments = vec!["printf".to_string(), EXPECTED_STDOUT.to_string()];
        #[cfg(target_family = "windows")]
        let arguments = vec!["echo".to_string(), EXPECTED_STDOUT.to_string()];
        let command = Command {
            arguments,
            working_directory: ".".to_string(),
            ..Default::default()
        };
        let command_digest = serialize_and_upload_message(
            &command,
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let action = Action {
            command_digest: Some(command_digest.into()),
            input_root_digest: Some(input_root_digest.into()),
            ..Default::default()
        };
        let action_digest = serialize_and_upload_message(
            &action,
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        let running_action_impl = running_actions_manager
            .clone()
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(ExecuteRequest {
                        action_digest: Some(action_digest.into()),
                        digest_function: ProtoDigestFunction::Sha256.into(),
                        ..Default::default()
                    }),
                    salt: SALT,
                    queued_timestamp: Some(make_system_time(1000).into()),
                },
            )
            .await?;

        let result = run_action(running_action_impl).await?;
        assert_eq!(result.exit_code, 0, "Exit code should be 0");

        let expected_stdout = DigestHasherFunc::Sha256
            .hasher()
            .compute_from_reader(Cursor::new(EXPECTED_STDOUT))
            .await?;
        // Note: This string should match what is in worker_for_test.sh
        let expected_stderr = DigestHasherFunc::Sha256
            .hasher()
            .compute_from_reader(Cursor::new("Wrapper script did run"))
            .await?;
        assert_eq!(expected_stdout, result.stdout_digest);
        assert_eq!(expected_stderr, result.stderr_digest);

        Ok(())
    }

    #[nativelink_test]
    async fn entrypoint_injects_properties() -> Result<(), Box<dyn std::error::Error>> {
        #[cfg(target_family = "unix")]
        const TEST_WRAPPER_SCRIPT_CONTENT: &str = "\
#!/bin/bash
# Print some static text to stderr. This is what the test uses to
# make sure the script did run.
>&2 printf \"Wrapper script did run with property $PROPERTY $VALUE $INNER_TIMEOUT\"

# Now run the real command.
exec \"$@\"
";
        #[cfg(target_family = "windows")]
        const TEST_WRAPPER_SCRIPT_CONTENT: &str = "\
@echo off
:: Print some static text to stderr. This is what the test uses to
:: make sure the script did run.
echo | set /p=\"Wrapper script did run with property %PROPERTY% %VALUE% %INNER_TIMEOUT%\" 1>&2

:: Run command, but morph the echo to ensure it doesn't
:: add a new line to the end of the output.
%1 | set /p=%2
exit 0
";
        const WORKER_ID: &str = "foo_worker_id";
        const SALT: u64 = 66;
        const EXPECTED_STDOUT: &str = "Action did run";

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let test_wrapper_script = {
            let test_wrapper_dir = make_temp_path("wrapper_dir");
            fs::create_dir_all(&test_wrapper_dir).await?;
            #[cfg(target_family = "unix")]
            let test_wrapper_script = OsString::from(test_wrapper_dir + "/test_wrapper_script.sh");
            #[cfg(target_family = "windows")]
            let test_wrapper_script =
                OsString::from(test_wrapper_dir + "\\test_wrapper_script.bat");

            // We use std::fs::File here because we sometimes get strange bugs here
            // that result in: "Text file busy (os error 26)" if it is an executeable.
            // It is likley because somewhere the file descriotor does not get closed
            // in tokio's async context.
            let mut test_wrapper_script_handle = std::fs::File::create(&test_wrapper_script)?;
            test_wrapper_script_handle.write_all(TEST_WRAPPER_SCRIPT_CONTENT.as_bytes())?;
            #[cfg(target_family = "unix")]
            test_wrapper_script_handle.set_permissions(Permissions::from_mode(0o777))?;
            test_wrapper_script_handle.sync_all()?;
            drop(test_wrapper_script_handle);

            test_wrapper_script
        };

        // TODO(#527) Sleep to reduce flakey chances.
        tokio::time::sleep(Duration::from_millis(250)).await;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: root_action_directory.clone(),
                execution_configuration: ExecutionConfiguration {
                    entrypoint: Some(test_wrapper_script.into_string().unwrap()),
                    additional_environment: Some(HashMap::from([
                        (
                            "PROPERTY".to_string(),
                            EnvironmentSource::property("property_name".to_string()),
                        ),
                        (
                            "VALUE".to_string(),
                            EnvironmentSource::value("raw_value".to_string()),
                        ),
                        (
                            "INNER_TIMEOUT".to_string(),
                            EnvironmentSource::timeout_millis,
                        ),
                    ])),
                },
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            })?);
        #[cfg(target_family = "unix")]
        let arguments = vec!["printf".to_string(), EXPECTED_STDOUT.to_string()];
        #[cfg(target_family = "windows")]
        let arguments = vec!["echo".to_string(), EXPECTED_STDOUT.to_string()];
        let command = Command {
            arguments,
            working_directory: ".".to_string(),
            ..Default::default()
        };
        let command_digest = serialize_and_upload_message(
            &command,
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        const TASK_TIMEOUT: Duration = Duration::from_secs(122);
        let action = Action {
            command_digest: Some(command_digest.into()),
            input_root_digest: Some(input_root_digest.into()),
            platform: Some(Platform {
                properties: vec![Property {
                    name: "property_name".into(),
                    value: "property_value".into(),
                }],
            }),
            timeout: Some(prost_types::Duration {
                seconds: TASK_TIMEOUT.as_secs() as i64,
                nanos: 0,
            }),
            ..Default::default()
        };
        let action_digest = serialize_and_upload_message(
            &action,
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        let running_action_impl = running_actions_manager
            .clone()
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(ExecuteRequest {
                        action_digest: Some(action_digest.into()),
                        digest_function: ProtoDigestFunction::Sha256.into(),
                        ..Default::default()
                    }),
                    salt: SALT,
                    queued_timestamp: Some(make_system_time(1000).into()),
                },
            )
            .await?;

        let result = run_action(running_action_impl).await?;
        assert_eq!(result.exit_code, 0, "Exit code should be 0");

        let expected_stdout = DigestHasherFunc::Sha256
            .hasher()
            .compute_from_reader(Cursor::new(EXPECTED_STDOUT))
            .await?;
        // Note: This string should match what is in worker_for_test.sh
        let expected_stderr =
            "Wrapper script did run with property property_value raw_value 122000";
        let expected_stderr_digest = DigestHasherFunc::Sha256
            .hasher()
            .compute_from_reader(Cursor::new(expected_stderr))
            .await?;

        let actual_stderr: prost::bytes::Bytes = cas_store
            .as_ref()
            .get_part_unchunked(result.stderr_digest, 0, None)
            .await?;
        let actual_stderr_decoded = std::str::from_utf8(&actual_stderr)?;
        assert_eq!(expected_stderr, actual_stderr_decoded);
        assert_eq!(expected_stdout, result.stdout_digest);
        assert_eq!(expected_stderr_digest, result.stderr_digest);

        Ok(())
    }

    #[nativelink_test]
    async fn entrypoint_sends_timeout_via_side_channel() -> Result<(), Box<dyn std::error::Error>> {
        #[cfg(target_family = "unix")]
        const TEST_WRAPPER_SCRIPT_CONTENT: &str = "\
#!/bin/bash
echo '{\"failure\":\"timeout\"}' > \"$SIDE_CHANNEL_FILE\"
exit 1
";
        #[cfg(target_family = "windows")]
        const TEST_WRAPPER_SCRIPT_CONTENT: &str = "\
@echo off
echo | set /p={\"failure\":\"timeout\"} 1>&2 > %SIDE_CHANNEL_FILE%
exit 1
";
        const WORKER_ID: &str = "foo_worker_id";
        const SALT: u64 = 66;

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let test_wrapper_script = {
            let test_wrapper_dir = make_temp_path("wrapper_dir");
            fs::create_dir_all(&test_wrapper_dir).await?;
            #[cfg(target_family = "unix")]
            let test_wrapper_script = OsString::from(test_wrapper_dir + "/test_wrapper_script.sh");
            #[cfg(target_family = "windows")]
            let test_wrapper_script =
                OsString::from(test_wrapper_dir + "\\test_wrapper_script.bat");

            // We use std::fs::File here because we sometimes get strange bugs here
            // that result in: "Text file busy (os error 26)" if it is an executeable.
            // It is likley because somewhere the file descriotor does not get closed
            // in tokio's async context.
            let mut test_wrapper_script_handle = std::fs::File::create(&test_wrapper_script)?;
            test_wrapper_script_handle.write_all(TEST_WRAPPER_SCRIPT_CONTENT.as_bytes())?;
            #[cfg(target_family = "unix")]
            test_wrapper_script_handle.set_permissions(Permissions::from_mode(0o777))?;
            test_wrapper_script_handle.sync_all()?;
            drop(test_wrapper_script_handle);

            test_wrapper_script
        };

        // TODO(#527) Sleep to reduce flakey chances.
        tokio::time::sleep(Duration::from_millis(250)).await;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: root_action_directory.clone(),
                execution_configuration: ExecutionConfiguration {
                    entrypoint: Some(test_wrapper_script.into_string().unwrap()),
                    additional_environment: Some(HashMap::from([(
                        "SIDE_CHANNEL_FILE".to_string(),
                        EnvironmentSource::side_channel_file,
                    )])),
                },
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            })?);
        let arguments = vec!["true".to_string()];
        let command = Command {
            arguments,
            working_directory: ".".to_string(),
            ..Default::default()
        };
        let command_digest = serialize_and_upload_message(
            &command,
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let action = Action {
            command_digest: Some(command_digest.into()),
            input_root_digest: Some(input_root_digest.into()),
            ..Default::default()
        };
        let action_digest = serialize_and_upload_message(
            &action,
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        let running_action_impl = running_actions_manager
            .clone()
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(ExecuteRequest {
                        action_digest: Some(action_digest.into()),
                        digest_function: ProtoDigestFunction::Sha256.into(),
                        ..Default::default()
                    }),
                    salt: SALT,
                    queued_timestamp: Some(make_system_time(1000).into()),
                },
            )
            .await?;

        let result = run_action(running_action_impl).await?;
        assert_eq!(result.exit_code, 1, "Exit code should be 1");
        assert_eq!(
            result.error.err_tip(|| "Error should exist")?.code,
            Code::DeadlineExceeded
        );
        Ok(())
    }

    #[nativelink_test]
    async fn caches_results_in_action_cache_store() -> Result<(), Box<dyn std::error::Error>> {
        let (_, _, cas_store, ac_store) = setup_stores().await?;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: String::new(),
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::success_only,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            })?);

        let action_digest = DigestInfo::new([2u8; 32], 32);
        let mut action_result = ActionResult {
            output_files: vec![FileInfo {
                name_or_path: NameOrPath::Path("test.txt".to_string()),
                digest: DigestInfo::try_new(
                    "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3",
                    3,
                )?,
                is_executable: false,
            }],
            stdout_digest: DigestInfo::try_new(
                "426afaf613d8cfdd9fa8addcc030ae6c95a7950ae0301164af1d5851012081d5",
                10,
            )?,
            stderr_digest: DigestInfo::try_new(
                "7b2e400d08b8e334e3172d105be308b506c6036c62a9bde5c509d7808b28b213",
                10,
            )?,
            exit_code: 0,
            output_folders: vec![],
            output_file_symlinks: vec![],
            output_directory_symlinks: vec![],
            server_logs: HashMap::new(),
            execution_metadata: ExecutionMetadata {
                worker: "WORKER_ID".to_string(),
                queued_timestamp: SystemTime::UNIX_EPOCH,
                worker_start_timestamp: make_system_time(0),
                input_fetch_start_timestamp: make_system_time(1),
                input_fetch_completed_timestamp: make_system_time(2),
                execution_start_timestamp: make_system_time(3),
                execution_completed_timestamp: make_system_time(4),
                output_upload_start_timestamp: make_system_time(5),
                output_upload_completed_timestamp: make_system_time(6),
                worker_completed_timestamp: make_system_time(7),
            },
            error: None,
            message: String::new(),
        };
        running_actions_manager
            .cache_action_result(action_digest, &mut action_result, DigestHasherFunc::Sha256)
            .await?;

        let retrieved_result =
            get_and_decode_digest::<ProtoActionResult>(ac_store.as_ref(), &action_digest).await?;

        let proto_result: ProtoActionResult = action_result.into();
        assert_eq!(proto_result, retrieved_result);

        Ok(())
    }

    #[nativelink_test]
    async fn failed_action_does_not_cache_in_action_cache() -> Result<(), Box<dyn std::error::Error>>
    {
        let (_, _, cas_store, ac_store) = setup_stores().await?;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: String::new(),
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::everything,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            })?);

        let action_digest = DigestInfo::new([2u8; 32], 32);
        let mut action_result = ActionResult {
            output_files: vec![FileInfo {
                name_or_path: NameOrPath::Path("test.txt".to_string()),
                digest: DigestInfo::try_new(
                    "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3",
                    3,
                )?,
                is_executable: false,
            }],
            stdout_digest: DigestInfo::try_new(
                "426afaf613d8cfdd9fa8addcc030ae6c95a7950ae0301164af1d5851012081d5",
                10,
            )?,
            stderr_digest: DigestInfo::try_new(
                "7b2e400d08b8e334e3172d105be308b506c6036c62a9bde5c509d7808b28b213",
                10,
            )?,
            exit_code: 1,
            output_folders: vec![],
            output_file_symlinks: vec![],
            output_directory_symlinks: vec![],
            server_logs: HashMap::new(),
            execution_metadata: ExecutionMetadata {
                worker: "WORKER_ID".to_string(),
                queued_timestamp: SystemTime::UNIX_EPOCH,
                worker_start_timestamp: make_system_time(0),
                input_fetch_start_timestamp: make_system_time(1),
                input_fetch_completed_timestamp: make_system_time(2),
                execution_start_timestamp: make_system_time(3),
                execution_completed_timestamp: make_system_time(4),
                output_upload_start_timestamp: make_system_time(5),
                output_upload_completed_timestamp: make_system_time(6),
                worker_completed_timestamp: make_system_time(7),
            },
            error: None,
            message: String::new(),
        };
        running_actions_manager
            .cache_action_result(action_digest, &mut action_result, DigestHasherFunc::Sha256)
            .await?;

        let retrieved_result =
            get_and_decode_digest::<ProtoActionResult>(ac_store.as_ref(), &action_digest).await?;

        let proto_result: ProtoActionResult = action_result.into();
        assert_eq!(proto_result, retrieved_result);

        Ok(())
    }

    #[nativelink_test]
    async fn success_does_cache_in_historical_results() -> Result<(), Box<dyn std::error::Error>> {
        let (_, _, cas_store, ac_store) = setup_stores().await?;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: String::new(),
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_historical_results_strategy: Some(
                            nativelink_config::cas_server::UploadCacheResultsStrategy::success_only,
                        ),
                        success_message_template:
                            "{historical_results_hash}-{historical_results_size}".to_string(),
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            })?);

        let action_digest = DigestInfo::new([2u8; 32], 32);
        let mut action_result = ActionResult {
            output_files: vec![FileInfo {
                name_or_path: NameOrPath::Path("test.txt".to_string()),
                digest: DigestInfo::try_new(
                    "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3",
                    3,
                )?,
                is_executable: false,
            }],
            stdout_digest: DigestInfo::try_new(
                "426afaf613d8cfdd9fa8addcc030ae6c95a7950ae0301164af1d5851012081d5",
                10,
            )?,
            stderr_digest: DigestInfo::try_new(
                "7b2e400d08b8e334e3172d105be308b506c6036c62a9bde5c509d7808b28b213",
                10,
            )?,
            exit_code: 0,
            output_folders: vec![],
            output_file_symlinks: vec![],
            output_directory_symlinks: vec![],
            server_logs: HashMap::new(),
            execution_metadata: ExecutionMetadata {
                worker: "WORKER_ID".to_string(),
                queued_timestamp: SystemTime::UNIX_EPOCH,
                worker_start_timestamp: make_system_time(0),
                input_fetch_start_timestamp: make_system_time(1),
                input_fetch_completed_timestamp: make_system_time(2),
                execution_start_timestamp: make_system_time(3),
                execution_completed_timestamp: make_system_time(4),
                output_upload_start_timestamp: make_system_time(5),
                output_upload_completed_timestamp: make_system_time(6),
                worker_completed_timestamp: make_system_time(7),
            },
            error: None,
            message: String::new(),
        };
        running_actions_manager
            .cache_action_result(action_digest, &mut action_result, DigestHasherFunc::Sha256)
            .await?;

        assert!(!action_result.message.is_empty(), "Message should be set");

        let historical_digest = {
            let (historical_results_hash, historical_results_size) = action_result
                .message
                .split_once('-')
                .expect("Message should be in format {hash}-{size}");

            DigestInfo::try_new(
                historical_results_hash,
                historical_results_size.parse::<i64>()?,
            )?
        };
        let retrieved_result = get_and_decode_digest::<HistoricalExecuteResponse>(
            cas_store.as_ref(),
            &historical_digest,
        )
        .await?;

        assert_eq!(
            HistoricalExecuteResponse {
                action_digest: Some(action_digest.into()),
                execute_response: Some(ExecuteResponse {
                    result: Some(action_result.into()),
                    status: Some(Default::default()),
                    ..Default::default()
                }),
            },
            retrieved_result
        );

        Ok(())
    }

    #[nativelink_test]
    async fn failure_does_not_cache_in_historical_results() -> Result<(), Box<dyn std::error::Error>>
    {
        let (_, _, cas_store, ac_store) = setup_stores().await?;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: String::new(),
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_historical_results_strategy: Some(
                            nativelink_config::cas_server::UploadCacheResultsStrategy::success_only,
                        ),
                        success_message_template:
                            "{historical_results_hash}-{historical_results_size}".to_string(),
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            })?);

        let action_digest = DigestInfo::new([2u8; 32], 32);
        let mut action_result = ActionResult {
            exit_code: 1,
            ..Default::default()
        };
        running_actions_manager
            .cache_action_result(action_digest, &mut action_result, DigestHasherFunc::Sha256)
            .await?;

        assert!(
            action_result.message.is_empty(),
            "Message should not be set"
        );
        Ok(())
    }

    #[nativelink_test]
    async fn infra_failure_does_cache_in_historical_results(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_, _, cas_store, ac_store) = setup_stores().await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
            root_action_directory: String::new(),
            execution_configuration: ExecutionConfiguration::default(),
            cas_store: Pin::into_inner(cas_store.clone()),
            ac_store: Some(Pin::into_inner(ac_store.clone())),
            historical_store: Pin::into_inner(cas_store.clone()),
            upload_action_result_config: &nativelink_config::cas_server::UploadActionResultConfig {
                upload_historical_results_strategy: Some(
                    nativelink_config::cas_server::UploadCacheResultsStrategy::failures_only,
                ),
                failure_message_template: "{historical_results_hash}-{historical_results_size}".to_string(),
                ..Default::default()
            },
            max_action_timeout: Duration::MAX,
            timeout_handled_externally: false,
        })?);

        let action_digest = DigestInfo::new([2u8; 32], 32);
        let mut action_result = ActionResult {
            exit_code: 0,
            error: Some(make_input_err!("test error")),
            ..Default::default()
        };
        running_actions_manager
            .cache_action_result(action_digest, &mut action_result, DigestHasherFunc::Sha256)
            .await?;

        assert!(!action_result.message.is_empty(), "Message should be set");

        let historical_digest = {
            let (historical_results_hash, historical_results_size) = action_result
                .message
                .split_once('-')
                .expect("Message should be in format {hash}-{size}");

            DigestInfo::try_new(
                historical_results_hash,
                historical_results_size.parse::<i64>()?,
            )?
        };

        let retrieved_result = get_and_decode_digest::<HistoricalExecuteResponse>(
            cas_store.as_ref(),
            &historical_digest,
        )
        .await?;

        assert_eq!(
            HistoricalExecuteResponse {
                action_digest: Some(action_digest.into()),
                execute_response: Some(ExecuteResponse {
                    result: Some(action_result.into()),
                    status: Some(make_input_err!("test error").into()),
                    ..Default::default()
                }),
            },
            retrieved_result
        );
        Ok(())
    }

    #[nativelink_test]
    async fn action_result_has_used_in_message() -> Result<(), Box<dyn std::error::Error>> {
        let (_, _, cas_store, ac_store) = setup_stores().await?;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: String::new(),
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::success_only,
                        success_message_template: "{action_digest_hash}-{action_digest_size}"
                            .to_string(),
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            })?);

        let action_digest = DigestInfo::new([2u8; 32], 32);
        let mut action_result = ActionResult {
            exit_code: 0,
            ..Default::default()
        };
        running_actions_manager
            .cache_action_result(action_digest, &mut action_result, DigestHasherFunc::Sha256)
            .await?;

        assert!(!action_result.message.is_empty(), "Message should be set");

        let action_result_digest = {
            let (action_result_hash, action_result_size) = action_result
                .message
                .split_once('-')
                .expect("Message should be in format {hash}-{size}");

            DigestInfo::try_new(action_result_hash, action_result_size.parse::<i64>()?)?
        };

        let retrieved_result =
            get_and_decode_digest::<ProtoActionResult>(ac_store.as_ref(), &action_result_digest)
                .await?;

        let proto_result: ProtoActionResult = action_result.into();
        assert_eq!(proto_result, retrieved_result);
        Ok(())
    }

    #[nativelink_test]
    async fn ensure_worker_timeout_chooses_correct_values() -> Result<(), Box<dyn std::error::Error>>
    {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let (_, _, cas_store, ac_store) = setup_stores().await?;

        #[cfg(target_family = "unix")]
        let arguments = vec!["true".to_string()];
        #[cfg(target_family = "windows")]
        let arguments = vec![
            "cmd".to_string(),
            "/C".to_string(),
            "exit".to_string(),
            "0".to_string(),
        ];

        let command = Command {
            arguments,
            output_paths: vec![],
            working_directory: ".".to_string(),
            ..Default::default()
        };
        let command_digest = serialize_and_upload_message(
            &command,
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        {
            // Test to ensure that the task timeout is choosen if it is less than the max timeout.
            static SENT_TIMEOUT: AtomicI64 = AtomicI64::new(-1);
            const MAX_TIMEOUT_DURATION: Duration = Duration::from_secs(100);
            const TASK_TIMEOUT: Duration = Duration::from_secs(10);

            let action = Action {
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input_root_digest.into()),
                timeout: Some(prost_types::Duration {
                    seconds: TASK_TIMEOUT.as_secs() as i64,
                    nanos: 0,
                }),
                ..Default::default()
            };
            let action_digest = serialize_and_upload_message(
                &action,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;

            let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
                RunningActionsManagerArgs {
                    root_action_directory: root_action_directory.clone(),
                    execution_configuration: ExecutionConfiguration::default(),
                    cas_store: Pin::into_inner(cas_store.clone()),
                    ac_store: Some(Pin::into_inner(ac_store.clone())),
                    historical_store: Pin::into_inner(cas_store.clone()),
                    upload_action_result_config:
                        &nativelink_config::cas_server::UploadActionResultConfig {
                            upload_ac_results_strategy:
                                nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                            ..Default::default()
                        },
                    max_action_timeout: MAX_TIMEOUT_DURATION,
                    timeout_handled_externally: false,
                },
                Callbacks {
                    now_fn: test_monotonic_clock,
                    sleep_fn: |duration| {
                        SENT_TIMEOUT.store(duration.as_millis() as i64, Ordering::Relaxed);
                        Box::pin(futures::future::pending())
                    },
                },
            )?);

            running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(ExecuteRequest {
                            action_digest: Some(action_digest.into()),
                            digest_function: ProtoDigestFunction::Sha256.into(),
                            ..Default::default()
                        }),
                        salt: 0,
                        queued_timestamp: Some(make_system_time(1000).into()),
                    },
                )
                .and_then(|action| {
                    action
                        .clone()
                        .prepare_action()
                        .and_then(RunningAction::execute)
                        .then(|result| async move {
                            if let Err(e) = action.cleanup().await {
                                return Result::<ActionResult, Error>::Err(e).merge(result);
                            }
                            result
                        })
                })
                .await?;
            assert_eq!(
                SENT_TIMEOUT.load(Ordering::Relaxed),
                TASK_TIMEOUT.as_millis() as i64
            );
        }
        {
            // Ensure if no timeout is set use max timeout.
            static SENT_TIMEOUT: AtomicI64 = AtomicI64::new(-1);
            const MAX_TIMEOUT_DURATION: Duration = Duration::from_secs(100);
            const TASK_TIMEOUT: Duration = Duration::from_secs(0);

            let action = Action {
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input_root_digest.into()),
                timeout: Some(prost_types::Duration {
                    seconds: TASK_TIMEOUT.as_secs() as i64,
                    nanos: 0,
                }),
                ..Default::default()
            };
            let action_digest = serialize_and_upload_message(
                &action,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;

            let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
                RunningActionsManagerArgs {
                    root_action_directory: root_action_directory.clone(),
                    execution_configuration: ExecutionConfiguration::default(),
                    cas_store: Pin::into_inner(cas_store.clone()),
                    ac_store: Some(Pin::into_inner(ac_store.clone())),
                    historical_store: Pin::into_inner(cas_store.clone()),
                    upload_action_result_config:
                        &nativelink_config::cas_server::UploadActionResultConfig {
                            upload_ac_results_strategy:
                                nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                            ..Default::default()
                        },
                    max_action_timeout: MAX_TIMEOUT_DURATION,
                    timeout_handled_externally: false,
                },
                Callbacks {
                    now_fn: test_monotonic_clock,
                    sleep_fn: |duration| {
                        SENT_TIMEOUT.store(duration.as_millis() as i64, Ordering::Relaxed);
                        Box::pin(futures::future::pending())
                    },
                },
            )?);

            running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(ExecuteRequest {
                            action_digest: Some(action_digest.into()),
                            digest_function: ProtoDigestFunction::Sha256.into(),
                            ..Default::default()
                        }),
                        salt: 0,
                        queued_timestamp: Some(make_system_time(1000).into()),
                    },
                )
                .and_then(|action| {
                    action
                        .clone()
                        .prepare_action()
                        .and_then(RunningAction::execute)
                        .then(|result| async move {
                            if let Err(e) = action.cleanup().await {
                                return Result::<ActionResult, Error>::Err(e).merge(result);
                            }
                            result
                        })
                })
                .await?;
            assert_eq!(
                SENT_TIMEOUT.load(Ordering::Relaxed),
                MAX_TIMEOUT_DURATION.as_millis() as i64
            );
        }
        {
            // Ensure we reject tasks that have a timeout set too high.
            static SENT_TIMEOUT: AtomicI64 = AtomicI64::new(-1);
            const MAX_TIMEOUT_DURATION: Duration = Duration::from_secs(100);
            const TASK_TIMEOUT: Duration = Duration::from_secs(200);

            let action = Action {
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input_root_digest.into()),
                timeout: Some(prost_types::Duration {
                    seconds: TASK_TIMEOUT.as_secs() as i64,
                    nanos: 0,
                }),
                ..Default::default()
            };
            let action_digest = serialize_and_upload_message(
                &action,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;

            let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
                RunningActionsManagerArgs {
                    root_action_directory: root_action_directory.clone(),
                    execution_configuration: ExecutionConfiguration::default(),
                    cas_store: Pin::into_inner(cas_store.clone()),
                    ac_store: Some(Pin::into_inner(ac_store.clone())),
                    historical_store: Pin::into_inner(cas_store.clone()),
                    upload_action_result_config:
                        &nativelink_config::cas_server::UploadActionResultConfig {
                            upload_ac_results_strategy:
                                nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                            ..Default::default()
                        },
                    max_action_timeout: MAX_TIMEOUT_DURATION,
                    timeout_handled_externally: false,
                },
                Callbacks {
                    now_fn: test_monotonic_clock,
                    sleep_fn: |duration| {
                        SENT_TIMEOUT.store(duration.as_millis() as i64, Ordering::Relaxed);
                        Box::pin(futures::future::pending())
                    },
                },
            )?);

            let result = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(ExecuteRequest {
                            action_digest: Some(action_digest.into()),
                            digest_function: ProtoDigestFunction::Sha256.into(),
                            ..Default::default()
                        }),
                        salt: 0,
                        queued_timestamp: Some(make_system_time(1000).into()),
                    },
                )
                .and_then(|action| {
                    action
                        .clone()
                        .prepare_action()
                        .and_then(RunningAction::execute)
                        .then(|result| async move {
                            if let Err(e) = action.cleanup().await {
                                return Result::<ActionResult, Error>::Err(e).merge(result);
                            }
                            result
                        })
                })
                .await;
            assert_eq!(SENT_TIMEOUT.load(Ordering::Relaxed), -1);
            assert_eq!(result.err().unwrap().code, Code::InvalidArgument);
        }
        Ok(())
    }

    #[nativelink_test]
    async fn worker_times_out() -> Result<(), Box<dyn std::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        type StaticOneshotTuple =
            Mutex<(Option<oneshot::Sender<()>>, Option<oneshot::Receiver<()>>)>;
        static TIMEOUT_ONESHOT: Lazy<StaticOneshotTuple> = Lazy::new(|| {
            let (tx, rx) = oneshot::channel();
            Mutex::new((Some(tx), Some(rx)))
        });
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
            RunningActionsManagerArgs {
                root_action_directory: root_action_directory.clone(),
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| {
                    Box::pin(async move {
                        let rx = TIMEOUT_ONESHOT.lock().unwrap().1.take().unwrap();
                        rx.await.expect("Could not receive timeout signal");
                    })
                },
            },
        )?);

        #[cfg(target_family = "unix")]
        let arguments = vec![
            "sh".to_string(),
            "-c".to_string(),
            "sleep infinity".to_string(),
        ];
        #[cfg(target_family = "windows")]
        // Windows is weird with timeout, so we use ping. See:
        // https://www.ibm.com/support/pages/timeout-command-run-batch-job-exits-immediately-and-returns-error-input-redirection-not-supported-exiting-process-immediately
        let arguments = vec![
            "cmd".to_string(),
            "/C".to_string(),
            "ping -n 99999 127.0.0.1".to_string(),
        ];

        let command = Command {
            arguments,
            output_paths: vec![],
            working_directory: ".".to_string(),
            ..Default::default()
        };
        let command_digest = serialize_and_upload_message(
            &command,
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let action = Action {
            command_digest: Some(command_digest.into()),
            input_root_digest: Some(input_root_digest.into()),
            ..Default::default()
        };
        let action_digest = serialize_and_upload_message(
            &action,
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        let execute_results_fut = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(ExecuteRequest {
                        action_digest: Some(action_digest.into()),
                        digest_function: ProtoDigestFunction::Sha256.into(),
                        ..Default::default()
                    }),
                    salt: 0,
                    queued_timestamp: Some(make_system_time(1000).into()),
                },
            )
            .and_then(|action| {
                action
                    .clone()
                    .prepare_action()
                    .and_then(RunningAction::execute)
                    .and_then(RunningAction::upload_results)
                    .and_then(RunningAction::get_finished_result)
                    .then(|result| async move {
                        if let Err(e) = action.cleanup().await {
                            return Result::<ActionResult, Error>::Err(e).merge(result);
                        }
                        result
                    })
            });

        let (results, _) = tokio::join!(execute_results_fut, async move {
            tokio::task::yield_now().await;
            let tx = TIMEOUT_ONESHOT.lock().unwrap().0.take().unwrap();
            tx.send(()).expect("Could not send timeout signal");
        });
        assert_eq!(results?.error.unwrap().code, Code::DeadlineExceeded);

        Ok(())
    }

    #[nativelink_test]
    async fn kill_all_waits_for_all_tasks_to_finish() -> Result<(), Box<dyn std::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
            RunningActionsManagerArgs {
                root_action_directory: root_action_directory.clone(),
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(futures::future::pending()),
            },
        )?);

        #[cfg(target_family = "unix")]
        let arguments = vec![
            "sh".to_string(),
            "-c".to_string(),
            "sleep infinity".to_string(),
        ];
        #[cfg(target_family = "windows")]
        // Windows is weird with timeout, so we use ping. See:
        // https://www.ibm.com/support/pages/timeout-command-run-batch-job-exits-immediately-and-returns-error-input-redirection-not-supported-exiting-process-immediately
        let arguments = vec![
            "cmd".to_string(),
            "/C".to_string(),
            "ping -n 99999 127.0.0.1".to_string(),
        ];

        let command = Command {
            arguments,
            output_paths: vec![],
            working_directory: ".".to_string(),
            ..Default::default()
        };
        let command_digest = serialize_and_upload_message(
            &command,
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let action = Action {
            command_digest: Some(command_digest.into()),
            input_root_digest: Some(input_root_digest.into()),
            ..Default::default()
        };
        let action_digest = serialize_and_upload_message(
            &action,
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        let (cleanup_tx, cleanup_rx) = oneshot::channel();
        let cleanup_was_requested = AtomicBool::new(false);
        let action = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(ExecuteRequest {
                        action_digest: Some(action_digest.into()),
                        digest_function: ProtoDigestFunction::Sha256.into(),
                        ..Default::default()
                    }),
                    salt: 0,
                    queued_timestamp: Some(make_system_time(1000).into()),
                },
            )
            .await?;
        let execute_results_fut = action
            .clone()
            .prepare_action()
            .and_then(RunningAction::execute)
            .and_then(RunningAction::upload_results)
            .and_then(RunningAction::get_finished_result)
            .then(|result| async {
                cleanup_was_requested.store(true, Ordering::Release);
                cleanup_rx.await.expect("Could not receive cleanup signal");
                if let Err(e) = action.cleanup().await {
                    return Result::<ActionResult, Error>::Err(e).merge(result);
                }
                result
            });

        tokio::pin!(execute_results_fut);
        {
            // Advance the action as far as possible and ensure we are not waiting on cleanup.
            for _ in 0..100 {
                assert!(futures::poll!(&mut execute_results_fut).is_pending());
                tokio::task::yield_now().await;
            }
            assert_eq!(cleanup_was_requested.load(Ordering::Acquire), false);
        }

        let kill_all_fut = running_actions_manager.kill_all();
        tokio::pin!(kill_all_fut);

        {
            // * Advance the action as far as possible.
            // * Ensure we are now waiting on cleanup.
            // * Ensure our kill_action is still pending.
            while !cleanup_was_requested.load(Ordering::Acquire) {
                // Wait for cleanup to be triggered.
                tokio::task::yield_now().await;
                assert!(futures::poll!(&mut execute_results_fut).is_pending());
                assert!(futures::poll!(&mut kill_all_fut).is_pending());
            }
        }
        // Allow cleanup, which allows execute_results_fut to advance.
        cleanup_tx.send(()).expect("Could not send cleanup signal");
        // Advance our two futures to completion now.
        let result = execute_results_fut.await;
        kill_all_fut.await;
        {
            // Ensure our results are correct.
            let action_result = result?;
            let err = action_result
                .error
                .as_ref()
                .err_tip(|| format!("No error exists in result : {action_result:?}"))?;
            assert_eq!(
                err.code,
                Code::Aborted,
                "Expected Aborted : {action_result:?}"
            );
        }

        Ok(())
    }

    /// Regression Test for Issue #675
    #[cfg(target_family = "unix")]
    #[nativelink_test]
    async fn unix_executable_file_test() -> Result<(), Box<dyn std::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";
        const FILE_1_NAME: &str = "file1";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
            RunningActionsManagerArgs {
                root_action_directory,
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                execution_configuration: ExecutionConfiguration::default(),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(futures::future::pending()),
            },
        )?);
        // Create and run an action which
        // creates a file with owner executable permissions.
        let action_result = {
            let command = Command {
                arguments: vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    format!("touch {FILE_1_NAME} && chmod 700 {FILE_1_NAME}").to_string(),
                ],
                output_paths: vec![FILE_1_NAME.to_string()],
                working_directory: ".".to_string(),
                ..Default::default()
            };
            let command_digest = serialize_and_upload_message(
                &command,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            let input_root_digest = serialize_and_upload_message(
                &Directory::default(),
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            let action = Action {
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input_root_digest.into()),
                ..Default::default()
            };
            let action_digest = serialize_and_upload_message(
                &action,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;

            let running_action_impl = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(ExecuteRequest {
                            action_digest: Some(action_digest.into()),
                            digest_function: ProtoDigestFunction::Sha256.into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )
                .await?;

            run_action(running_action_impl.clone()).await?
        };
        // Ensure the file copied from worker to CAS is executable.
        assert!(
            action_result.output_files[0].is_executable,
            "Expected output file to be executable"
        );
        Ok(())
    }

    #[nativelink_test]
    async fn action_directory_contents_are_cleaned() -> Result<(), Box<dyn std::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;
        let temp_action_directory = make_temp_path("root_action_directory/temp");
        fs::create_dir_all(&temp_action_directory).await?;

        let running_actions_manager =
            Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: root_action_directory.clone(),
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            })?);
        let queued_timestamp = make_system_time(1000);

        #[cfg(target_family = "unix")]
        let arguments = vec!["sh".to_string(), "-c".to_string(), "exit 0".to_string()];
        #[cfg(target_family = "windows")]
        let arguments = vec!["cmd".to_string(), "/C".to_string(), "exit 0".to_string()];

        const SALT: u64 = 55;
        let command = Command {
            arguments,
            output_paths: vec![],
            working_directory: ".".to_string(),
            ..Default::default()
        };
        let command_digest = serialize_and_upload_message(
            &command,
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let action = Action {
            command_digest: Some(command_digest.into()),
            input_root_digest: Some(input_root_digest.into()),
            ..Default::default()
        };
        let action_digest = serialize_and_upload_message(
            &action,
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        let running_action_impl = running_actions_manager
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(ExecuteRequest {
                        action_digest: Some(action_digest.into()),
                        digest_function: ProtoDigestFunction::Sha256.into(),
                        ..Default::default()
                    }),
                    salt: SALT,
                    queued_timestamp: Some(queued_timestamp.into()),
                },
            )
            .await?;

        run_action(running_action_impl.clone()).await?;

        let mut dir_stream = fs::read_dir(&root_action_directory).await?;
        assert!(
            dir_stream.as_mut().next_entry().await?.is_none(),
            "Expected empty directory at {root_action_directory}"
        );
        Ok(())
    }

    // We've experienced deadlocks when uploading, so make only a single permit available and
    // check it's able to handle uploading some directories with some files in.
    // Be default this test is ignored because it *must* be run single threaded... to run this
    // test execute:
    // cargo test -p nativelink-worker --test running_actions_manager_test -- --test-threads=1 --ignored
    #[nativelink_test]
    #[ignore]
    async fn upload_with_single_permit() -> Result<(), Box<dyn std::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";

        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let (_, slow_store, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_action_directory");
        fs::create_dir_all(&root_action_directory).await?;

        // Take all but one FD permit away.
        let _permits = futures::stream::iter(1..fs::OPEN_FILE_SEMAPHORE.available_permits())
            .then(|_| fs::OPEN_FILE_SEMAPHORE.acquire())
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(1, fs::OPEN_FILE_SEMAPHORE.available_permits());

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
            RunningActionsManagerArgs {
                root_action_directory,
                execution_configuration: ExecutionConfiguration::default(),
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                sleep_fn: |_duration| Box::pin(futures::future::pending()),
            },
        )?);
        let action_result = {
            const SALT: u64 = 55;
            #[cfg(target_family = "unix")]
            let arguments = vec![
                "sh".to_string(),
                "-c".to_string(),
                "printf '123 ' > ./test.txt; mkdir ./tst; printf '456 ' > ./tst/tst.txt; printf 'foo-stdout '; >&2 printf 'bar-stderr  '"
                    .to_string(),
            ];
            #[cfg(target_family = "windows")]
            let arguments = vec![
                "cmd".to_string(),
                "/C".to_string(),
                // Note: Windows adds two spaces after 'set /p=XXX'.
                "echo | set /p=123> ./test.txt & mkdir ./tst & echo | set /p=456> ./tst/tst.txt & echo | set /p=foo-stdout & echo | set /p=bar-stderr 1>&2 & exit 0"
                    .to_string(),
            ];
            let working_directory = "some_cwd";
            let command = Command {
                arguments,
                output_paths: vec!["test.txt".to_string(), "tst".to_string()],
                working_directory: working_directory.to_string(),
                ..Default::default()
            };
            let command_digest = serialize_and_upload_message(
                &command,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            let input_root_digest = serialize_and_upload_message(
                &Directory {
                    directories: vec![DirectoryNode {
                        name: working_directory.to_string(),
                        digest: Some(
                            serialize_and_upload_message(
                                &Directory::default(),
                                cas_store.as_ref(),
                                &mut DigestHasherFunc::Sha256.hasher(),
                            )
                            .await?
                            .into(),
                        ),
                    }],
                    ..Default::default()
                },
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;
            let action = Action {
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input_root_digest.into()),
                ..Default::default()
            };
            let action_digest = serialize_and_upload_message(
                &action,
                cas_store.as_ref(),
                &mut DigestHasherFunc::Sha256.hasher(),
            )
            .await?;

            let running_action_impl = running_actions_manager
                .create_and_add_action(
                    WORKER_ID.to_string(),
                    StartExecute {
                        execute_request: Some(ExecuteRequest {
                            action_digest: Some(action_digest.into()),
                            digest_function: ProtoDigestFunction::Sha256.into(),
                            ..Default::default()
                        }),
                        salt: SALT,
                        queued_timestamp: None,
                    },
                )
                .await?;

            run_action(running_action_impl.clone()).await?
        };
        let file_content = slow_store
            .as_ref()
            .get_part_unchunked(action_result.output_files[0].digest, 0, None)
            .await?;
        assert_eq!(from_utf8(&file_content)?, "123 ");
        let stdout_content = slow_store
            .as_ref()
            .get_part_unchunked(action_result.stdout_digest, 0, None)
            .await?;
        assert_eq!(from_utf8(&stdout_content)?, "foo-stdout ");
        let stderr_content = slow_store
            .as_ref()
            .get_part_unchunked(action_result.stderr_digest, 0, None)
            .await?;
        assert_eq!(from_utf8(&stderr_content)?, "bar-stderr  ");
        let mut clock_time = make_system_time(0);
        assert_eq!(
            action_result,
            ActionResult {
                output_files: vec![FileInfo {
                    name_or_path: NameOrPath::Path("test.txt".to_string()),
                    digest: DigestInfo::try_new(
                        "c69e10a5f54f4e28e33897fbd4f8701595443fa8c3004aeaa20dd4d9a463483b",
                        4
                    )?,
                    is_executable: false,
                }],
                stdout_digest: DigestInfo::try_new(
                    "15019a676f057d97d1ad3af86f3cc1e623cb33b18ff28422bbe3248d2471cc94",
                    11
                )?,
                stderr_digest: DigestInfo::try_new(
                    "2375ab8a01ca11e1ea7606dfb58756c153d49733cde1dbfb5a1e00f39afacf06",
                    12
                )?,
                exit_code: 0,
                output_folders: vec![DirectoryInfo {
                    path: "tst".to_string(),
                    tree_digest: DigestInfo::try_new(
                        "95711c1905d4898a70209dd6e98241dcafb479c00241a1ea4ed8415710d706f3",
                        166,
                    )?,
                },],
                output_file_symlinks: vec![],
                output_directory_symlinks: vec![],
                server_logs: HashMap::new(),
                execution_metadata: ExecutionMetadata {
                    worker: WORKER_ID.to_string(),
                    queued_timestamp: SystemTime::UNIX_EPOCH,
                    worker_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_start_timestamp: increment_clock(&mut clock_time),
                    input_fetch_completed_timestamp: increment_clock(&mut clock_time),
                    execution_start_timestamp: increment_clock(&mut clock_time),
                    execution_completed_timestamp: increment_clock(&mut clock_time),
                    output_upload_start_timestamp: increment_clock(&mut clock_time),
                    output_upload_completed_timestamp: increment_clock(&mut clock_time),
                    worker_completed_timestamp: increment_clock(&mut clock_time),
                },
                error: None,
                message: String::new(),
            }
        );
        Ok(())
    }

    #[nativelink_test]
    async fn running_actions_manager_respects_action_timeout(
    ) -> Result<(), Box<dyn std::error::Error>> {
        const WORKER_ID: &str = "foo_worker_id";
        const SALT: u64 = 66;

        let (_, _, cas_store, ac_store) = setup_stores().await?;
        let root_action_directory = make_temp_path("root_work_directory");
        fs::create_dir_all(&root_action_directory).await?;

        // Ignore the sleep and immediately timeout.
        static ACTION_TIMEOUT: i64 = 1;
        fn test_monotonic_clock() -> SystemTime {
            static CLOCK: AtomicU64 = AtomicU64::new(0);
            monotonic_clock(&CLOCK)
        }

        let running_actions_manager = Arc::new(RunningActionsManagerImpl::new_with_callbacks(
            RunningActionsManagerArgs {
                root_action_directory,
                execution_configuration: Default::default(),
                cas_store: Pin::into_inner(cas_store.clone()),
                ac_store: Some(Pin::into_inner(ac_store.clone())),
                historical_store: Pin::into_inner(cas_store.clone()),
                upload_action_result_config:
                    &nativelink_config::cas_server::UploadActionResultConfig {
                        upload_ac_results_strategy:
                            nativelink_config::cas_server::UploadCacheResultsStrategy::never,
                        ..Default::default()
                    },
                max_action_timeout: Duration::MAX,
                timeout_handled_externally: false,
            },
            Callbacks {
                now_fn: test_monotonic_clock,
                // If action_timeout is the passed duration then return immeidately,
                // which will cause the action to be killed and pass the test,
                // otherwise return pending and fail the test.
                sleep_fn: |duration| {
                    assert_eq!(duration.as_secs(), ACTION_TIMEOUT as u64);
                    Box::pin(futures::future::ready(()))
                },
            },
        )?);
        #[cfg(target_family = "unix")]
        let arguments = vec!["sh".to_string(), "-c".to_string(), "sleep 2".to_string()];
        #[cfg(target_family = "windows")]
        let arguments = vec![
            "cmd".to_string(),
            "/C".to_string(),
            "ping -n 99999 127.0.0.1".to_string(),
        ];
        let command = Command {
            arguments,
            working_directory: ".".to_string(),
            ..Default::default()
        };
        let command_digest = serialize_and_upload_message(
            &command,
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let action = Action {
            command_digest: Some(command_digest.into()),
            input_root_digest: Some(input_root_digest.into()),
            platform: Some(Platform {
                properties: vec![Property {
                    name: "property_name".into(),
                    value: "property_value".into(),
                }],
            }),
            timeout: Some(prost_types::Duration {
                seconds: ACTION_TIMEOUT,
                nanos: 0,
            }),
            ..Default::default()
        };
        let action_digest = serialize_and_upload_message(
            &action,
            cas_store.as_ref(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;

        let running_action_impl = running_actions_manager
            .clone()
            .create_and_add_action(
                WORKER_ID.to_string(),
                StartExecute {
                    execute_request: Some(ExecuteRequest {
                        action_digest: Some(action_digest.into()),
                        digest_function: ProtoDigestFunction::Sha256.into(),
                        ..Default::default()
                    }),
                    salt: SALT,
                    queued_timestamp: Some(make_system_time(1000).into()),
                },
            )
            .await?;

        let result = run_action(running_action_impl).await?;

        #[cfg(target_family = "unix")]
        assert_eq!(result.exit_code, 9, "Action process should be been killed");
        #[cfg(target_family = "windows")]
        assert_eq!(result.exit_code, 1, "Action process should be been killed");
        Ok(())
    }
}
