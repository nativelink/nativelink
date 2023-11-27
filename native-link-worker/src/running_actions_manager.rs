// Copyright 2023 The Native Link Authors. All rights reserved.
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

use std::borrow::Cow;
use std::collections::vec_deque::VecDeque;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fmt::Debug;
#[cfg(target_family = "unix")]
use std::fs::Permissions;
#[cfg(target_family = "unix")]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use error::{make_err, make_input_err, Code, Error, ResultExt};
use filetime::{set_file_mtime, FileTime};
use formatx::Template;
use futures::future::{try_join, try_join3, try_join_all, BoxFuture, Future, FutureExt, TryFutureExt};
use futures::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use native_link_config::cas_server::{EnvironmentSource, UploadActionResultConfig, UploadCacheResultsStrategy};
use native_link_store::ac_utils::{
    compute_buf_digest, compute_digest, get_and_decode_digest, serialize_and_upload_message, upload_buf_to_store,
    upload_file_to_store, ESTIMATED_DIGEST_SIZE,
};
use native_link_store::fast_slow_store::FastSlowStore;
use native_link_store::filesystem_store::{FileEntry, FilesystemStore};
use native_link_store::grpc_store::GrpcStore;
use native_link_util::action_messages::{
    to_execute_response, ActionInfo, ActionResult, DirectoryInfo, ExecutionMetadata, FileInfo, NameOrPath, SymlinkInfo,
};
use native_link_util::common::{fs, log, DigestInfo, JoinHandleDropGuard};
use native_link_util::digest_hasher::DigestHasherFunc;
use native_link_util::metrics_utils::{AsyncCounterWrapper, CollectorState, CounterWithTime, MetricsComponent};
use native_link_util::store_trait::Store;
use parking_lot::Mutex;
use prost::Message;
use proto::build::bazel::remote::execution::v2::{
    Action, ActionResult as ProtoActionResult, Command as ProtoCommand, Directory as ProtoDirectory, Directory,
    DirectoryNode, ExecuteResponse, FileNode, SymlinkNode, Tree as ProtoTree, UpdateActionResultRequest,
};
use proto::com::github::trace_machina::native_link::remote_execution::{HistoricalExecuteResponse, StartExecute};
use relative_path::RelativePath;
use scopeguard::{guard, ScopeGuard};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::process;
use tokio::sync::{oneshot, watch};
use tokio::task::spawn_blocking;
use tokio::time::timeout;
use tokio_stream::wrappers::ReadDirStream;
use tonic::Request;
use uuid::Uuid;

pub type ActionId = [u8; 32];

/// For simplicity we use a fixed exit code for cases when our program is terminated
/// due to a signal.
const EXIT_CODE_FOR_SIGNAL: i32 = 9;

/// Default strategy for uploading historical results.
/// Note: If this value changes the config documentation
/// should reflect it.
const DEFAULT_HISTORICAL_RESULTS_STRATEGY: UploadCacheResultsStrategy = UploadCacheResultsStrategy::FailuresOnly;

/// Valid string reasons for a failure.
/// Note: If these change, the documentation should be updated.
#[allow(non_camel_case_types)]
#[derive(Debug, Deserialize)]
enum SideChannelFailureReason {
    /// Task should be considered timedout.
    timeout,
}

/// This represents the json data that can be passed from the running process
/// to the parent via the SideChannelFile. See:
/// `config::EnvironmentSource::SideChannelFile` for more details.
/// Note: Any fields added here must be added to the documentation.
#[derive(Debug, Deserialize, Default)]
struct SideChannelInfo {
    /// If the task should be considered a failure and why.
    failure: Option<SideChannelFailureReason>,
}

/// Aggressively download the digests of files and make a local folder from it. This function
/// will spawn unbounded number of futures to try and get these downloaded. The store itself
/// should be rate limited if spawning too many requests at once is an issue.
/// We require the `FilesystemStore` to be the `fast` store of `FastSlowStore`. This is for
/// efficiency reasons. We will request the `FastSlowStore` to populate the entry then we will
/// assume the `FilesystemStore` has the file available immediately after and hardlink the file
/// to a new location.
// Sadly we cannot use `async fn` here because the rust compiler cannot determine the auto traits
// of the future. So we need to force this function to return a dynamic future instead.
// see: https://github.com/rust-lang/rust/issues/78649
pub fn download_to_directory<'a>(
    cas_store: Pin<&'a FastSlowStore>,
    filesystem_store: Pin<&'a FilesystemStore>,
    digest: &'a DigestInfo,
    current_directory: &'a str,
) -> BoxFuture<'a, Result<(), Error>> {
    async move {
        let directory = get_and_decode_digest::<ProtoDirectory>(cas_store, digest)
            .await
            .err_tip(|| "Converting digest to Directory")?;
        let mut futures = FuturesUnordered::new();

        for file in directory.files {
            let digest: DigestInfo = file
                .digest
                .err_tip(|| "Expected Digest to exist in Directory::file::digest")?
                .try_into()
                .err_tip(|| "In Directory::file::digest")?;
            let dest = format!("{}/{}", current_directory, file.name);
            let (mtime, mut unix_mode) = match file.node_properties {
                Some(properties) => (properties.mtime, properties.unix_mode),
                None => (None, None),
            };
            #[cfg_attr(target_family = "windows", allow(unused_assignments))]
            if file.is_executable {
                unix_mode = Some(unix_mode.unwrap_or(0o444) | 0o111);
            }
            futures.push(
                cas_store
                    .populate_fast_store(digest)
                    .and_then(move |_| async move {
                        let file_entry = filesystem_store
                            .get_file_entry_for_digest(&digest)
                            .await
                            .err_tip(|| "During hard link")?;
                        file_entry
                            .get_file_path_locked(|src| fs::hard_link(src, &dest))
                            .await
                            .map_err(|e| make_err!(Code::Internal, "Could not make hardlink, {e:?} : {dest}"))?;
                        #[cfg(target_family = "unix")]
                        if let Some(unix_mode) = unix_mode {
                            fs::set_permissions(&dest, Permissions::from_mode(unix_mode))
                                .await
                                .err_tip(|| format!("Could not set unix mode in download_to_directory {dest}"))?;
                        }
                        if let Some(mtime) = mtime {
                            spawn_blocking(move || {
                                set_file_mtime(&dest, FileTime::from_unix_time(mtime.seconds, mtime.nanos as u32))
                                    .err_tip(|| format!("Failed to set mtime in download_to_directory {dest}"))
                            })
                            .await
                            .err_tip(|| "Failed to launch spawn_blocking in download_to_directory")??;
                        }
                        Ok(())
                    })
                    .map_err(move |e| e.append(format!("for digest {digest:?}")))
                    .boxed(),
            );
        }

        for directory in directory.directories {
            let digest: DigestInfo = directory
                .digest
                .err_tip(|| "Expected Digest to exist in Directory::directories::digest")?
                .try_into()
                .err_tip(|| "In Directory::file::digest")?;
            let new_directory_path = format!("{}/{}", current_directory, directory.name);
            futures.push(
                async move {
                    fs::create_dir(&new_directory_path)
                        .await
                        .err_tip(|| format!("Could not create directory {new_directory_path}"))?;
                    download_to_directory(cas_store, filesystem_store, &digest, &new_directory_path)
                        .await
                        .err_tip(|| format!("in download_to_directory : {new_directory_path}"))?;
                    Ok(())
                }
                .boxed(),
            );
        }

        #[cfg(target_family = "unix")]
        for symlink_node in directory.symlinks {
            let dest = format!("{}/{}", current_directory, symlink_node.name);
            futures.push(
                async move {
                    fs::symlink(&symlink_node.target, &dest)
                        .await
                        .err_tip(|| format!("Could not create symlink {} -> {}", symlink_node.target, dest))?;
                    Ok(())
                }
                .boxed(),
            );
        }

        while futures.try_next().await?.is_some() {}
        Ok(())
    }
    .boxed()
}

#[cfg(target_family = "windows")]
async fn is_executable(_file_handle: &fs::FileSlot, _full_path: &impl AsRef<Path>) -> Result<bool, Error> {
    static EXECUTABLE_EXTENSIONS: &[&str] = &["exe", "bat", "com"];
    Ok(EXECUTABLE_EXTENSIONS
        .iter()
        .any(|ext| _full_path.as_ref().extension().map_or(false, |v| v == *ext)))
}

#[cfg(target_family = "unix")]
async fn is_executable(file_handle: &fs::FileSlot, full_path: &impl AsRef<Path>) -> Result<bool, Error> {
    let metadata = file_handle
        .as_ref()
        .metadata()
        .await
        .err_tip(|| format!("While reading metadata for {:?}", full_path.as_ref()))?;
    Ok((metadata.mode() & 0o001) != 0)
}

async fn upload_file(
    mut resumeable_file: fs::ResumeableFileSlot<'static>,
    cas_store: Pin<&dyn Store>,
    full_path: impl AsRef<Path> + Debug,
    hasher: DigestHasherFunc,
) -> Result<FileInfo, Error> {
    let (digest, is_executable, resumeable_file) = {
        let (digest, mut resumeable_file) = JoinHandleDropGuard::new(tokio::spawn(async move {
            let file_handle = resumeable_file
                .as_reader()
                .await
                .err_tip(|| "Could not get reader from file slot in RunningActionsManager::upload_file()")?;
            let digest = compute_digest(file_handle, &mut hasher.into()).await?.0;
            Ok::<_, Error>((digest, resumeable_file))
        }))
        .await
        .err_tip(|| "Failed to launch spawn")?
        .err_tip(|| format!("for {full_path:?}"))?;

        // Sadly we to reaquire a `reader` from `resumeable_file` because `tokio::spawn` requires an owned
        // version of the struct. Luckily acquireing a reader is cheap, but this code was dirtied up a bit
        // from this.
        let file_handle = resumeable_file
            .as_reader()
            .await
            .err_tip(|| "Could not get reader from file slot in RunningActionsManager::upload_file()")?;
        let is_executable = is_executable(file_handle.get_ref(), &full_path).await?;
        file_handle
            .get_mut()
            .rewind()
            .await
            .err_tip(|| "Could not rewind file")?;
        (digest, is_executable, resumeable_file)
    };
    upload_file_to_store(cas_store, digest, resumeable_file)
        .await
        .err_tip(|| format!("for {full_path:?}"))?;

    let name = full_path
        .as_ref()
        .file_name()
        .err_tip(|| format!("Expected file_name to exist on {full_path:?}"))?
        .to_str()
        .err_tip(|| make_err!(Code::Internal, "Could not convert {:?} to string", full_path))?
        .to_string();

    Ok(FileInfo {
        name_or_path: NameOrPath::Name(name),
        digest,
        is_executable,
    })
}

async fn upload_symlink(
    full_path: impl AsRef<Path> + Debug,
    full_work_directory_path: impl AsRef<Path>,
) -> Result<SymlinkInfo, Error> {
    let full_target_path = fs::read_link(full_path.as_ref())
        .await
        .err_tip(|| format!("Could not get read_link path of {full_path:?}"))?;

    // Detect if our symlink is inside our work directory, if it is find the
    // relative path otherwise use the absolute path.
    let target = if full_target_path.starts_with(full_work_directory_path.as_ref()) {
        let full_target_path = RelativePath::from_path(&full_target_path)
            .map_err(|v| make_err!(Code::Internal, "Could not convert {} to RelativePath", v))?;
        RelativePath::from_path(full_work_directory_path.as_ref())
            .map_err(|v| make_err!(Code::Internal, "Could not convert {} to RelativePath", v))?
            .relative(full_target_path)
            .normalize()
            .into_string()
    } else {
        full_target_path
            .to_str()
            .err_tip(|| make_err!(Code::Internal, "Could not convert '{:?}' to string", full_target_path))?
            .to_string()
    };

    let name = full_path
        .as_ref()
        .file_name()
        .err_tip(|| format!("Expected file_name to exist on {full_path:?}"))?
        .to_str()
        .err_tip(|| make_err!(Code::Internal, "Could not convert {:?} to string", full_path))?
        .to_string();

    Ok(SymlinkInfo {
        name_or_path: NameOrPath::Name(name),
        target,
    })
}

fn upload_directory<'a, P: AsRef<Path> + Debug + Send + Sync + Clone + 'a>(
    cas_store: Pin<&'a dyn Store>,
    full_dir_path: P,
    full_work_directory: &'a str,
    hasher: DigestHasherFunc,
) -> BoxFuture<'a, Result<(Directory, VecDeque<ProtoDirectory>), Error>> {
    Box::pin(async move {
        let file_futures = FuturesUnordered::new();
        let dir_futures = FuturesUnordered::new();
        let symlink_futures = FuturesUnordered::new();
        {
            let (_permit, dir_handle) = fs::read_dir(&full_dir_path)
                .await
                .err_tip(|| format!("Error reading dir for reading {full_dir_path:?}"))?
                .into_inner();
            let mut dir_stream = ReadDirStream::new(dir_handle);
            // Note: Try very hard to not leave file descriptors open. Try to keep them as short
            // lived as possible. This is why we iterate the directory and then build a bunch of
            // futures with all the work we are wanting to do then execute it. It allows us to
            // close the directory iterator file descriptor, then open the child files/folders.
            while let Some(entry) = dir_stream.next().await {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(e) => return Err(e).err_tip(|| "Error while iterating directory")?,
                };
                let file_type = entry
                    .file_type()
                    .await
                    .err_tip(|| format!("Error running file_type() on {entry:?}"))?;
                let full_path = full_dir_path.as_ref().join(entry.path());
                if file_type.is_dir() {
                    let full_dir_path = full_dir_path.clone();
                    dir_futures.push(
                        upload_directory(cas_store, full_path.clone(), full_work_directory, hasher)
                            .and_then(|(dir, all_dirs)| async move {
                                let directory_name = full_path
                                    .file_name()
                                    .err_tip(|| format!("Expected file_name to exist on {full_dir_path:?}"))?
                                    .to_str()
                                    .err_tip(|| {
                                        make_err!(Code::Internal, "Could not convert {:?} to string", full_dir_path)
                                    })?
                                    .to_string();

                                let digest = serialize_and_upload_message(&dir, cas_store, &mut hasher.into())
                                    .await
                                    .err_tip(|| format!("for {full_path:?}"))?;

                                Result::<(DirectoryNode, VecDeque<Directory>), Error>::Ok((
                                    DirectoryNode {
                                        name: directory_name,
                                        digest: Some(digest.into()),
                                    },
                                    all_dirs,
                                ))
                            })
                            .boxed(),
                    );
                } else if file_type.is_file() {
                    file_futures.push(async move {
                        let file_handle = fs::open_file(full_path.as_os_str().to_os_string(), u64::MAX)
                            .await
                            .err_tip(|| format!("Could not open file {full_path:?}"))?;
                        upload_file(file_handle, cas_store, &full_path, hasher)
                            .map_ok(|v| v.into())
                            .await
                    });
                } else if file_type.is_symlink() {
                    symlink_futures.push(upload_symlink(full_path, &full_work_directory).map_ok(Into::into));
                }
            }
        }

        let (mut file_nodes, dir_entries, mut symlinks) = try_join3(
            file_futures.try_collect::<Vec<FileNode>>(),
            dir_futures.try_collect::<Vec<(DirectoryNode, VecDeque<Directory>)>>(),
            symlink_futures.try_collect::<Vec<SymlinkNode>>(),
        )
        .await?;

        let mut directory_nodes = Vec::with_capacity(dir_entries.len());
        // For efficiency we use a deque because it allows cheap concat of Vecs.
        // We make the assumption here that when performance is important it is because
        // our directory is quite large. This allows us to cheaply merge large amounts of
        // directories into one VecDeque. Then after we are done we need to collapse it
        // down into a single Vec.
        let mut all_child_directories = VecDeque::with_capacity(dir_entries.len());
        for (directory_node, mut recursive_child_directories) in dir_entries {
            directory_nodes.push(directory_node);
            all_child_directories.append(&mut recursive_child_directories);
        }

        file_nodes.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        directory_nodes.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        symlinks.sort_unstable_by(|a, b| a.name.cmp(&b.name));

        let directory = Directory {
            files: file_nodes,
            directories: directory_nodes,
            symlinks,
            node_properties: None, // We don't support file properties.
        };
        all_child_directories.push_back(directory.clone());

        Ok((directory, all_child_directories))
    })
}

async fn process_side_channel_file(
    side_channel_file: Cow<'_, OsStr>,
    args: &[&OsStr],
    timeout: Duration,
) -> Result<Option<Error>, Error> {
    let mut json_contents = String::new();
    {
        // Note: Scoping `file_slot` allows the file_slot semaphore to be released faster.
        let mut file_slot = match fs::open_file(side_channel_file, u64::MAX).await {
            Ok(file_slot) => file_slot,
            Err(e) => {
                if e.code != Code::NotFound {
                    return Err(e).err_tip(|| "Error opening side channel file");
                }
                // Note: If file does not exist, it's ok. Users are not required to create this file.
                return Ok(None);
            }
        };
        let reader = file_slot
            .as_reader()
            .await
            .err_tip(|| "Error getting reader from side channel file (maybe permissions?)")?;
        reader
            .read_to_string(&mut json_contents)
            .await
            .err_tip(|| "Error reading side channel file")?;
    }

    let side_channel_info: SideChannelInfo = serde_json5::from_str(&json_contents).map_err(|e| {
        make_input_err!("Could not convert contents of side channel file (json) to SideChannelInfo : {e:?}")
    })?;
    Ok(side_channel_info.failure.map(|failure| match failure {
        SideChannelFailureReason::timeout => Error::new(
            Code::DeadlineExceeded,
            format!(
                "Command '{}' timed out after {} seconds",
                args.join(OsStr::new(" ")).to_string_lossy(),
                timeout.as_secs_f32()
            ),
        ),
    }))
}

#[async_trait]
pub trait RunningAction: Sync + Send + Sized + Unpin + 'static {
    /// Anything that needs to execute before the actions is actually executed should happen here.
    async fn prepare_action(self: Arc<Self>) -> Result<Arc<Self>, Error>;

    /// Actually perform the execution of the action.
    async fn execute(self: Arc<Self>) -> Result<Arc<Self>, Error>;

    /// Any uploading, processing or analyzing of the results should happen here.
    async fn upload_results(self: Arc<Self>) -> Result<Arc<Self>, Error>;

    /// Cleanup any residual files, handles or other junk resulting from running the action.
    async fn cleanup(self: Arc<Self>) -> Result<Arc<Self>, Error>;

    /// Returns the final result. As a general rule this action should be thought of as
    /// a consumption of `self`, meaning once a return happens here the lifetime of `Self`
    /// is over and any action performed on it after this call is undefined behavior.
    async fn get_finished_result(self: Arc<Self>) -> Result<ActionResult, Error>;

    /// Returns the work directory of the action.
    fn get_work_directory(&self) -> &String;
}

struct RunningActionImplExecutionResult {
    stdout: Bytes,
    stderr: Bytes,
    exit_code: i32,
}

struct RunningActionImplState {
    command_proto: Option<ProtoCommand>,
    // TODO(allada) Kill is not implemented yet, but is instrumented.
    // However, it is used if the worker disconnects to destroy current jobs.
    kill_channel_tx: Option<oneshot::Sender<()>>,
    kill_channel_rx: Option<oneshot::Receiver<()>>,
    execution_result: Option<RunningActionImplExecutionResult>,
    action_result: Option<ActionResult>,
    execution_metadata: ExecutionMetadata,
    // If there was an internal error, this will be set.
    // This should NOT be set if everything was fine, but the process had a
    // non-zero exit code. Instead this should be used for internal errors
    // that prevented the action from running, upload failures, timeouts, exc...
    // but we have (or could have) the action results (like stderr/stdout).
    error: Option<Error>,
}

pub struct RunningActionImpl {
    action_id: ActionId,
    work_directory: String,
    action_info: ActionInfo,
    timeout: Duration,
    running_actions_manager: Arc<RunningActionsManagerImpl>,
    state: Mutex<RunningActionImplState>,
    did_cleanup: AtomicBool,
}

impl RunningActionImpl {
    fn new(
        execution_metadata: ExecutionMetadata,
        action_id: ActionId,
        work_directory: String,
        action_info: ActionInfo,
        timeout: Duration,
        running_actions_manager: Arc<RunningActionsManagerImpl>,
    ) -> Self {
        let (kill_channel_tx, kill_channel_rx) = oneshot::channel();
        Self {
            action_id,
            work_directory,
            action_info,
            timeout,
            running_actions_manager,
            state: Mutex::new(RunningActionImplState {
                command_proto: None,
                kill_channel_rx: Some(kill_channel_rx),
                kill_channel_tx: Some(kill_channel_tx),
                execution_result: None,
                action_result: None,
                execution_metadata,
                error: None,
            }),
            did_cleanup: AtomicBool::new(false),
        }
    }

    fn metrics(&self) -> &Arc<Metrics> {
        &self.running_actions_manager.metrics
    }

    /// Prepares any actions needed to execution this action. This action will do the following:
    /// * Download any files needed to execute the action
    /// * Build a folder with all files needed to execute the action.
    /// This function will aggressively download and spawn potentially thousands of futures. It is
    /// up to the stores to rate limit if needed.
    async fn inner_prepare_action(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        {
            let mut state = self.state.lock();
            state.execution_metadata.input_fetch_start_timestamp = (self.running_actions_manager.callbacks.now_fn)();
        }
        let command = {
            // Download and build out our input files/folders. Also fetch and decode our Command.
            let cas_store_pin = Pin::new(self.running_actions_manager.cas_store.as_ref());
            let command_fut = self.metrics().get_proto_command_from_store.wrap(async {
                get_and_decode_digest::<ProtoCommand>(cas_store_pin, &self.action_info.command_digest)
                    .await
                    .err_tip(|| "Converting command_digest to Command")
            });
            let filesystem_store_pin = Pin::new(self.running_actions_manager.filesystem_store.as_ref());
            // Download the input files/folder and place them into the temp directory.
            let download_to_directory_fut = self.metrics().download_to_directory.wrap(download_to_directory(
                cas_store_pin,
                filesystem_store_pin,
                &self.action_info.input_root_digest,
                &self.work_directory,
            ));
            let (command, _) = try_join(command_fut, download_to_directory_fut).await?;
            command
        };
        {
            // Create all directories needed for our output paths. This is required by the bazel spec.
            let prepare_output_directories = |output_file| {
                let full_output_path = if command.working_directory.is_empty() {
                    format!("{}/{}", self.work_directory, output_file)
                } else {
                    format!("{}/{}/{}", self.work_directory, command.working_directory, output_file)
                };
                async move {
                    let full_parent_path = Path::new(&full_output_path)
                        .parent()
                        .err_tip(|| format!("Parent path for {full_output_path} has no parent"))?;
                    fs::create_dir_all(full_parent_path)
                        .await
                        .err_tip(|| format!("Error creating output directory {} (file)", full_parent_path.display()))?;
                    Result::<(), Error>::Ok(())
                }
            };
            self.metrics()
                .prepare_output_files
                .wrap(try_join_all(
                    command.output_files.iter().map(prepare_output_directories),
                ))
                .await?;
            self.metrics()
                .prepare_output_paths
                .wrap(try_join_all(
                    command.output_paths.iter().map(prepare_output_directories),
                ))
                .await?;
        }
        log::info!("\x1b[0;31mWorker Received Command\x1b[0m: {:?}", command);
        {
            let mut state = self.state.lock();
            state.command_proto = Some(command);
            state.execution_metadata.input_fetch_completed_timestamp =
                (self.running_actions_manager.callbacks.now_fn)();
        }
        Ok(self)
    }

    async fn inner_execute(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        let (command_proto, mut kill_channel_rx) = {
            let mut state = self.state.lock();
            state.execution_metadata.execution_start_timestamp = (self.running_actions_manager.callbacks.now_fn)();
            (
                state
                    .command_proto
                    .take()
                    .err_tip(|| "Expected state to have command_proto in execute()")?,
                state
                    .kill_channel_rx
                    .take()
                    .err_tip(|| "Expected state to have kill_channel_rx in execute()")?
                    // This is important as we may be killed at any point.
                    .fuse(),
            )
        };
        if command_proto.arguments.is_empty() {
            return Err(make_input_err!("No arguments provided in Command proto"));
        }
        let args: Vec<&OsStr> =
            if let Some(entrypoint_cmd) = &self.running_actions_manager.execution_configuration.entrypoint_cmd {
                std::iter::once(entrypoint_cmd.as_ref())
                    .chain(command_proto.arguments.iter().map(AsRef::as_ref))
                    .collect()
            } else {
                command_proto.arguments.iter().map(AsRef::as_ref).collect()
            };
        log::info!("\x1b[0;31mWorker Executing\x1b[0m: {:?}", &args);
        let mut command_builder = process::Command::new(args[0]);
        command_builder
            .args(&args[1..])
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .current_dir(format!("{}/{}", self.work_directory, command_proto.working_directory))
            .env_clear();

        let mut maybe_side_channel_file: Option<Cow<'_, OsStr>> = None;
        if let Some(additional_environment) = &self
            .running_actions_manager
            .execution_configuration
            .additional_environment
        {
            for (name, source) in additional_environment {
                let value = match source {
                    EnvironmentSource::Property(property) => self
                        .action_info
                        .platform_properties
                        .properties
                        .get(property)
                        .map_or_else(|| Cow::Borrowed(""), |v| v.as_str()),
                    EnvironmentSource::Value(value) => Cow::Borrowed(value.as_str()),
                    EnvironmentSource::TimeoutMillis => Cow::Owned(self.timeout.as_millis().to_string()),
                    EnvironmentSource::SideChannelFile => {
                        let file_cow = format!(
                            "{}/{}/{}",
                            self.work_directory,
                            command_proto.working_directory,
                            Uuid::new_v4().simple(),
                        );
                        maybe_side_channel_file = Some(Cow::Owned(file_cow.clone().into()));
                        Cow::Owned(file_cow)
                    }
                };
                command_builder.env(name, value.as_ref());
            }
        }

        #[cfg(target_family = "unix")]
        let envs = &command_proto.environment_variables;
        // If SystemRoot is not set on windows we set it to default. Failing to do
        // this causes all commands to fail.
        #[cfg(target_family = "windows")]
        let envs = {
            let mut envs = command_proto.environment_variables.clone();
            if !envs.iter().any(|v| v.name.to_uppercase() == "SYSTEMROOT") {
                envs.push(
                    proto::build::bazel::remote::execution::v2::command::EnvironmentVariable {
                        name: "SystemRoot".to_string(),
                        value: "C:\\Windows".to_string(),
                    },
                );
            }
            if !envs.iter().any(|v| v.name.to_uppercase() == "PATH") {
                envs.push(
                    proto::build::bazel::remote::execution::v2::command::EnvironmentVariable {
                        name: "PATH".to_string(),
                        value: "C:\\Windows\\System32".to_string(),
                    },
                );
            }
            envs
        };
        for environment_variable in envs {
            command_builder.env(&environment_variable.name, &environment_variable.value);
        }

        let mut child_process = command_builder
            .spawn()
            .err_tip(|| format!("Could not execute command {:?}", args))?;
        let mut stdout_reader = child_process
            .stdout
            .take()
            .err_tip(|| "Expected stdout to exist on command this should never happen")?;
        let mut stderr_reader = child_process
            .stderr
            .take()
            .err_tip(|| "Expected stderr to exist on command this should never happen")?;

        let mut child_process_guard = guard(child_process, |mut child_process| {
            log::error!(
                "Child process was not cleaned up before dropping the call to execute(), killing in background spawn."
            );
            tokio::spawn(async move { child_process.kill().await });
        });

        let all_stdout_fut = JoinHandleDropGuard::new(tokio::spawn(async move {
            let mut all_stdout = BytesMut::new();
            loop {
                let sz = stdout_reader
                    .read_buf(&mut all_stdout)
                    .await
                    .err_tip(|| "Error reading stdout stream")?;
                if sz == 0 {
                    break; // EOF.
                }
            }
            Result::<Bytes, Error>::Ok(all_stdout.freeze())
        }));
        let all_stderr_fut = JoinHandleDropGuard::new(tokio::spawn(async move {
            let mut all_stderr = BytesMut::new();
            loop {
                let sz = stderr_reader
                    .read_buf(&mut all_stderr)
                    .await
                    .err_tip(|| "Error reading stderr stream")?;
                if sz == 0 {
                    break; // EOF.
                }
            }
            Result::<Bytes, Error>::Ok(all_stderr.freeze())
        }));
        let mut killed_action = false;

        let timer = self.metrics().child_process.begin_timer();
        let mut sleep_fut = (self.running_actions_manager.callbacks.sleep_fn)(self.timeout).fuse();
        loop {
            tokio::select! {
                _ = &mut sleep_fut => {
                    self.running_actions_manager.metrics.task_timeouts.inc();
                    killed_action = true;
                    if let Err(e) = child_process_guard.start_kill() {
                        log::error!("Could not kill process in RunningActionsManager for timeout : {:?}", e);
                    }
                    {
                        let mut state = self.state.lock();
                        state.error = Error::merge_option(state.error.take(), Some(Error::new(
                            Code::DeadlineExceeded,
                            format!(
                                "Command '{}' timed out after {} seconds",
                                args.join(OsStr::new(" ")).to_string_lossy(),
                                self.action_info.timeout.as_secs_f32()
                            )
                        )));
                    }
                },
                maybe_exit_status = child_process_guard.wait() => {
                    // Defuse our guard so it does not try to cleanup and make nessless logs.
                    drop(ScopeGuard::<_, _>::into_inner(child_process_guard));
                    let exit_status = maybe_exit_status.err_tip(|| "Failed to collect exit code of process")?;
                    // TODO(allada) We should implement stderr/stdout streaming to client here.
                    // If we get killed before the stream is started, then these will lock up.
                    // TODO(allada) There is a significant bug here. If we kill the action and the action creates
                    // child processes, it can create zombies. See: https://github.com/tracemachina/native-link/issues/225
                    let (stdout, stderr) = if killed_action {
                        drop(timer);
                        (Bytes::new(), Bytes::new())
                    } else {
                        timer.measure();
                        let (maybe_all_stdout, maybe_all_stderr) = tokio::join!(all_stdout_fut, all_stderr_fut);
                        (
                            maybe_all_stdout.err_tip(|| "Internal error reading from stdout of worker task")??,
                            maybe_all_stderr.err_tip(|| "Internal error reading from stderr of worker task")??
                        )
                    };
                    let exit_code = if let Some(exit_code) = exit_status.code() {
                        if exit_code == 0 {
                            self.metrics().child_process_success_error_code.inc();
                        } else {
                            self.metrics().child_process_failure_error_code.inc();
                        }
                        exit_code
                    } else {
                        EXIT_CODE_FOR_SIGNAL
                    };

                    let maybe_error_override = if let Some(side_channel_file) = maybe_side_channel_file {
                        process_side_channel_file(side_channel_file.clone(), &args, self.timeout).await
                        .err_tip(|| format!("Error processing side channel file: {side_channel_file:?}"))?
                    } else {
                        None
                    };
                    {
                        let mut state = self.state.lock();
                        state.error = Error::merge_option(state.error.take(), maybe_error_override);

                        state.command_proto = Some(command_proto);
                        state.execution_result = Some(RunningActionImplExecutionResult{
                            stdout,
                            stderr,
                            exit_code,
                        });
                        state.execution_metadata.execution_completed_timestamp = (self.running_actions_manager.callbacks.now_fn)();
                    }
                    return Ok(self);
                },
                _ = &mut kill_channel_rx => {
                    killed_action = true;
                    if let Err(e) = child_process_guard.start_kill() {
                        log::error!(
                            "Could not kill process in RunningActionsManager for action {} : {:?}",
                            hex::encode(self.action_id),
                            e);
                    } else {
                        log::error!(
                            "Could not get child process id, maybe already dead? for action {}",
                            hex::encode(self.action_id)
                        );
                    }
                    {
                        let mut state = self.state.lock();
                        state.error = Error::merge_option(state.error.take(), Some(Error::new(
                            Code::Aborted,
                            format!(
                                "Command '{}' was killed by scheduler",
                                args.join(OsStr::new(" ")).to_string_lossy()
                            )
                        )));
                    }
                },
            }
        }
        // Unreachable.
    }

    async fn inner_upload_results(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        log::info!("\x1b[0;31mWorker Uploading Results\x1b[0m");
        let (mut command_proto, execution_result, mut execution_metadata) = {
            let mut state = self.state.lock();
            state.execution_metadata.output_upload_start_timestamp = (self.running_actions_manager.callbacks.now_fn)();
            (
                state
                    .command_proto
                    .take()
                    .err_tip(|| "Expected state to have command_proto in execute()")?,
                state
                    .execution_result
                    .take()
                    .err_tip(|| "Execution result does not exist at upload_results stage")?,
                state.execution_metadata.clone(),
            )
        };
        let cas_store = Pin::new(self.running_actions_manager.cas_store.as_ref());
        let hasher = self.action_info.digest_function;
        enum OutputType {
            None,
            File(FileInfo),
            Directory(DirectoryInfo),
            FileSymlink(SymlinkInfo),
            DirectorySymlink(SymlinkInfo),
        }

        let mut output_path_futures = FuturesUnordered::new();
        let mut output_paths = command_proto.output_paths;
        if output_paths.is_empty() {
            output_paths.reserve(command_proto.output_files.len() + command_proto.output_directories.len());
            output_paths.append(&mut command_proto.output_files);
            output_paths.append(&mut command_proto.output_directories);
        }
        for entry in output_paths {
            let full_path = OsString::from(if command_proto.working_directory.is_empty() {
                format!("{}/{}", self.work_directory, entry)
            } else {
                format!("{}/{}/{}", self.work_directory, command_proto.working_directory, entry)
            });
            let work_directory = &self.work_directory;
            output_path_futures.push(async move {
                let metadata = {
                    let mut resumeable_file = match fs::open_file(full_path.clone(), u64::MAX).await {
                        Ok(file) => file,
                        Err(e) => {
                            if e.code == Code::NotFound {
                                // In the event our output does not exist, according to the bazel remote
                                // execution spec, we simply ignore it continue.
                                return Result::<OutputType, Error>::Ok(OutputType::None);
                            }
                            return Err(e).err_tip(|| format!("Could not open file {full_path:?}"));
                        }
                    };
                    // We cannot rely on the file_handle's metadata, because it follows symlinks, so
                    // we need to instead use `symlink_metadata`.
                    let metadata_fut = fs::symlink_metadata(&full_path);
                    tokio::pin!(metadata_fut);

                    // Just in case we are starved for open file descriptors, we timeout the metadata
                    // call and close the file, then try again.
                    let metadata = match timeout(fs::idle_file_descriptor_timeout(), &mut metadata_fut).await {
                        Ok(result) => result,
                        Err(_) => {
                            resumeable_file
                                .close_file()
                                .await
                                .err_tip(|| "In inner_upload_results()")?;
                            (&mut metadata_fut).await
                        }
                    }
                    .err_tip(|| format!("While querying symlink metadata for {entry}"))?;
                    if metadata.is_file() {
                        return Ok(OutputType::File(
                            upload_file(resumeable_file, cas_store, &full_path, hasher)
                                .await
                                .map(|mut file_info| {
                                    file_info.name_or_path = NameOrPath::Path(entry);
                                    file_info
                                })
                                .err_tip(|| format!("Uploading file {full_path:?}"))?,
                        ));
                    }
                    metadata
                };
                if metadata.is_dir() {
                    Ok(OutputType::Directory(
                        upload_directory(cas_store, &full_path, work_directory, hasher)
                            .and_then(|(root_dir, children)| async move {
                                let tree = ProtoTree {
                                    root: Some(root_dir),
                                    children: children.into(),
                                };
                                let tree_digest = serialize_and_upload_message(&tree, cas_store, &mut hasher.into())
                                    .await
                                    .err_tip(|| format!("While processing {entry}"))?;
                                Ok(DirectoryInfo {
                                    path: entry,
                                    tree_digest,
                                })
                            })
                            .await
                            .err_tip(|| format!("Uploading directory {full_path:?}"))?,
                    ))
                } else if metadata.is_symlink() {
                    let output_symlink = upload_symlink(&full_path, work_directory)
                        .await
                        .map(|mut symlink_info| {
                            symlink_info.name_or_path = NameOrPath::Path(entry);
                            symlink_info
                        })
                        .err_tip(|| format!("Uploading symlink {full_path:?}"))?;
                    match fs::metadata(&full_path).await {
                        Ok(metadata) => {
                            if metadata.is_dir() {
                                return Ok(OutputType::DirectorySymlink(output_symlink));
                            }
                            // Note: If it's anything but directory we put it as a file symlink.
                            return Ok(OutputType::FileSymlink(output_symlink));
                        }
                        Err(e) => {
                            if e.code != Code::NotFound {
                                return Err(e)
                                    .err_tip(|| format!("While querying target symlink metadata for {full_path:?}"));
                            }
                            // If the file doesn't exist, we consider it a file. Even though the
                            // file doesn't exist we still need to populate an entry.
                            return Ok(OutputType::FileSymlink(output_symlink));
                        }
                    }
                } else {
                    Err(make_err!(
                        Code::Internal,
                        "{full_path:?} was not a file, folder or symlink. Must be one.",
                    ))
                }
            });
        }
        let mut output_files = vec![];
        let mut output_folders = vec![];
        let mut output_directory_symlinks = vec![];
        let mut output_file_symlinks = vec![];

        if execution_result.exit_code != 0 {
            log::info!(
                "Command returned exit code {} : {} {}",
                execution_result.exit_code,
                std::str::from_utf8(&execution_result.stdout).unwrap_or(""),
                std::str::from_utf8(&execution_result.stderr).unwrap_or("")
            );
        }

        let stdout_digest_fut = self.metrics().upload_stdout.wrap(async {
            let data = execution_result.stdout;
            let digest = compute_buf_digest(&data, &mut hasher.into())
                .await
                .err_tip(|| "Computing stdout digest")?;
            upload_buf_to_store(cas_store, digest, data)
                .await
                .err_tip(|| "Uploading stdout")?;
            Result::<DigestInfo, Error>::Ok(digest)
        });
        let stderr_digest_fut = self.metrics().upload_stderr.wrap(async {
            let data = execution_result.stderr;
            let digest = compute_buf_digest(&data, &mut hasher.into())
                .await
                .err_tip(|| "Computing stderr digest")?;
            upload_buf_to_store(cas_store, digest, data)
                .await
                .err_tip(|| "Uploading stderr")?;
            Result::<DigestInfo, Error>::Ok(digest)
        });

        let upload_result = futures::try_join!(stdout_digest_fut, stderr_digest_fut, async {
            while let Some(output_type) = output_path_futures.try_next().await? {
                match output_type {
                    OutputType::File(output_file) => output_files.push(output_file),
                    OutputType::Directory(output_folder) => output_folders.push(output_folder),
                    OutputType::FileSymlink(output_symlink) => output_file_symlinks.push(output_symlink),
                    OutputType::DirectorySymlink(output_symlink) => output_directory_symlinks.push(output_symlink),
                    OutputType::None => { /* Safe to ignore */ }
                }
            }
            Ok(())
        });
        drop(output_path_futures);
        let (stdout_digest, stderr_digest) = match upload_result {
            Ok((stdout_digest, stderr_digest, _)) => (stdout_digest, stderr_digest),
            Err(e) => return Err(e).err_tip(|| "Error while uploading results"),
        };

        execution_metadata.output_upload_completed_timestamp = (self.running_actions_manager.callbacks.now_fn)();
        output_files.sort_unstable_by(|a, b| a.name_or_path.cmp(&b.name_or_path));
        output_folders.sort_unstable_by(|a, b| a.path.cmp(&b.path));
        output_file_symlinks.sort_unstable_by(|a, b| a.name_or_path.cmp(&b.name_or_path));
        output_directory_symlinks.sort_unstable_by(|a, b| a.name_or_path.cmp(&b.name_or_path));
        {
            let mut state = self.state.lock();
            execution_metadata.worker_completed_timestamp = (self.running_actions_manager.callbacks.now_fn)();
            state.action_result = Some(ActionResult {
                output_files,
                output_folders,
                output_directory_symlinks,
                output_file_symlinks,
                exit_code: execution_result.exit_code,
                stdout_digest,
                stderr_digest,
                execution_metadata,
                server_logs: HashMap::default(), // TODO(allada) Not implemented.
                error: state.error.clone(),
                message: String::new(), // Will be filled in on cache_action_result if needed.
            });
        }
        Ok(self)
    }

    async fn inner_cleanup(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        log::info!("\x1b[0;31mWorker Cleanup\x1b[0m");
        // Note: We need to be careful to keep trying to cleanup even if one of the steps fails.
        let remove_dir_result = fs::remove_dir_all(&self.work_directory)
            .await
            .err_tip(|| format!("Could not remove working directory {}", self.work_directory));
        self.did_cleanup.store(true, Ordering::Relaxed);
        if let Err(e) = self.running_actions_manager.cleanup_action(&self.action_id) {
            log::error!("Error cleaning up action: {e:?}");
            return Result::<Arc<Self>, Error>::Err(e).merge(remove_dir_result.map(|_| self));
        }
        if let Err(e) = remove_dir_result {
            log::error!(
                "Error removing working for action {} directory: {e:?}",
                hex::encode(self.action_id)
            );
            return Err(e);
        }
        Ok(self)
    }

    async fn inner_get_finished_result(self: Arc<Self>) -> Result<ActionResult, Error> {
        let mut state = self.state.lock();
        state
            .action_result
            .take()
            .err_tip(|| "Expected action_result to exist in get_finished_result")
    }
}

impl Drop for RunningActionImpl {
    fn drop(&mut self) {
        assert!(
            self.did_cleanup.load(Ordering::Relaxed),
            "RunningActionImpl did not cleanup. This is a violation of how RunningActionImpl's requirements"
        );
    }
}

#[async_trait]
impl RunningAction for RunningActionImpl {
    async fn prepare_action(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        self.metrics()
            .clone()
            .prepare_action
            .wrap(Self::inner_prepare_action(self))
            .await
    }

    async fn execute(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        self.metrics().clone().execute.wrap(Self::inner_execute(self)).await
    }

    async fn upload_results(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        self.metrics()
            .clone()
            .upload_results
            .wrap(Self::inner_upload_results(self))
            .await
    }

    async fn cleanup(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        self.metrics().clone().cleanup.wrap(Self::inner_cleanup(self)).await
    }

    async fn get_finished_result(self: Arc<Self>) -> Result<ActionResult, Error> {
        self.metrics()
            .clone()
            .get_finished_result
            .wrap(Self::inner_get_finished_result(self))
            .await
    }

    fn get_work_directory(&self) -> &String {
        &self.work_directory
    }
}

#[async_trait]
pub trait RunningActionsManager: Sync + Send + Sized + Unpin + 'static {
    type RunningAction: RunningAction;

    async fn create_and_add_action(
        self: &Arc<Self>,
        worker_id: String,
        start_execute: StartExecute,
    ) -> Result<Arc<Self::RunningAction>, Error>;

    async fn cache_action_result(
        &self,
        action_digest: DigestInfo,
        action_result: &mut ActionResult,
        hasher: DigestHasherFunc,
    ) -> Result<(), Error>;

    async fn kill_all(&self);

    fn metrics(&self) -> &Arc<Metrics>;
}

/// A function to get the current system time, used to allow mocking for tests
type NowFn = fn() -> SystemTime;
type SleepFn = fn(Duration) -> BoxFuture<'static, ()>;

/// Functions that may be injected for testing purposes, during standard control
/// flows these are specified by the new function.
pub struct Callbacks {
    /// A function that gets the current time.
    pub now_fn: NowFn,
    /// A function that sleeps for a given Duration.
    pub sleep_fn: SleepFn,
}

/// The set of additional information for executing an action over and above
/// those given in the ActionInfo passed to the worker.  This allows
/// modification of the action for execution on this particular worker.  This
/// may be used to run the action with a particular set of additional
/// environment variables, or perhaps configure it to execute within a
/// container.
#[derive(Default)]
pub struct ExecutionConfiguration {
    /// If set, will be executed instead of the first argument passed in the
    /// ActionInfo with all of the arguments in the ActionInfo passed as
    /// arguments to this command.
    pub entrypoint_cmd: Option<String>,
    /// The only environment variables that will be specified when the command
    /// executes other than those in the ActionInfo.  On Windows, SystemRoot
    /// and PATH are also assigned (see inner_execute).
    pub additional_environment: Option<HashMap<String, EnvironmentSource>>,
}

struct UploadActionResults {
    upload_ac_results_strategy: UploadCacheResultsStrategy,
    upload_historical_results_strategy: UploadCacheResultsStrategy,
    ac_store: Option<Arc<dyn Store>>,
    historical_store: Arc<dyn Store>,
    success_message_template: Template,
    failure_message_template: Template,
}

impl UploadActionResults {
    fn new(
        config: &UploadActionResultConfig,
        ac_store: Option<Arc<dyn Store>>,
        historical_store: Arc<dyn Store>,
    ) -> Result<Self, Error> {
        let upload_historical_results_strategy = config
            .upload_historical_results_strategy
            .unwrap_or(DEFAULT_HISTORICAL_RESULTS_STRATEGY);
        if !matches!(config.upload_ac_results_strategy, UploadCacheResultsStrategy::Never) && ac_store.is_none() {
            return Err(make_input_err!(
                "upload_ac_results_strategy is set, but no ac_store is configured"
            ));
        }
        Ok(Self {
            upload_ac_results_strategy: config.upload_ac_results_strategy,
            upload_historical_results_strategy,
            ac_store,
            historical_store,
            success_message_template: Template::new(&config.success_message_template).map_err(|e| {
                make_input_err!(
                    "Could not convert success_message_template to rust template: {} : {e:?}",
                    config.success_message_template
                )
            })?,
            failure_message_template: Template::new(&config.failure_message_template).map_err(|e| {
                make_input_err!(
                    "Could not convert failure_message_template to rust template: {} : {e:?}",
                    config.success_message_template
                )
            })?,
        })
    }

    fn should_cache_result(
        strategy: UploadCacheResultsStrategy,
        action_result: &ActionResult,
        treat_infra_error_as_failure: bool,
    ) -> bool {
        let mut did_fail = action_result.exit_code != 0;
        if treat_infra_error_as_failure && action_result.error.is_some() {
            did_fail = true;
        }
        match strategy {
            UploadCacheResultsStrategy::SuccessOnly => !did_fail,
            UploadCacheResultsStrategy::Never => false,
            // Never cache internal errors or timeouts.
            UploadCacheResultsStrategy::Everything => treat_infra_error_as_failure || action_result.error.is_none(),
            UploadCacheResultsStrategy::FailuresOnly => did_fail,
        }
    }

    /// Formats the message field in ExecuteResponse from the success_message_template or
    /// failure_message_template config templates.
    fn format_execute_response_message(
        mut template_str: Template,
        action_digest_info: DigestInfo,
        maybe_historical_digest_info: Option<DigestInfo>,
        hasher: DigestHasherFunc,
    ) -> Result<String, Error> {
        template_str.replace("digest_function", hasher.proto_digest_func().as_str_name());
        template_str.replace("action_digest_hash", action_digest_info.hash_str());
        template_str.replace("action_digest_size", action_digest_info.size_bytes);
        if let Some(historical_digest_info) = maybe_historical_digest_info {
            template_str.replace("historical_results_hash", historical_digest_info.hash_str());
            template_str.replace("historical_results_size", historical_digest_info.size_bytes);
        } else {
            template_str.replace("historical_results_hash", "");
            template_str.replace("historical_results_size", "");
        }
        template_str
            .text()
            .map_err(|e| make_input_err!("Could not convert template to text: {e:?}"))
    }

    async fn upload_ac_results(
        &self,
        action_digest: DigestInfo,
        action_result: ProtoActionResult,
        hasher: DigestHasherFunc,
    ) -> Result<(), Error> {
        let Some(ac_store) = &self.ac_store else { return Ok(()) };
        // If we are a GrpcStore we shortcut here, as this is a special store.
        let any_store = ac_store.clone().as_any();
        let maybe_grpc_store = any_store.downcast_ref::<Arc<GrpcStore>>();
        if let Some(grpc_store) = maybe_grpc_store {
            let update_action_request = UpdateActionResultRequest {
                // This is populated by `update_action_result`.
                instance_name: String::new(),
                action_digest: Some(action_digest.into()),
                action_result: Some(action_result),
                results_cache_policy: None,
                digest_function: hasher.proto_digest_func().into(),
            };
            return grpc_store
                .update_action_result(Request::new(update_action_request))
                .await
                .map(|_| ())
                .err_tip(|| "Caching ActionResult");
        }

        let mut store_data = BytesMut::with_capacity(ESTIMATED_DIGEST_SIZE);
        action_result
            .encode(&mut store_data)
            .err_tip(|| "Encoding ActionResult for caching")?;

        Pin::new(ac_store.as_ref())
            .update_oneshot(action_digest, store_data.split().freeze())
            .await
            .err_tip(|| "Caching ActionResult")
    }

    async fn upload_historical_results_with_message(
        &self,
        action_digest: DigestInfo,
        execute_response: ExecuteResponse,
        message_template: Template,
        hasher: DigestHasherFunc,
    ) -> Result<String, Error> {
        let historical_digest_info = serialize_and_upload_message(
            &HistoricalExecuteResponse {
                action_digest: Some(action_digest.into()),
                execute_response: Some(execute_response.clone()),
            },
            Pin::new(self.historical_store.as_ref()),
            &mut hasher.into(),
        )
        .await
        .err_tip(|| format!("Caching HistoricalExecuteResponse for digest: {action_digest:?}"))?;

        Self::format_execute_response_message(message_template, action_digest, Some(historical_digest_info), hasher)
            .err_tip(|| "Could not format message in upload_historical_results_with_message")
    }

    async fn cache_action_result(
        &self,
        action_info: DigestInfo,
        action_result: &mut ActionResult,
        hasher: DigestHasherFunc,
    ) -> Result<(), Error> {
        let should_upload_historical_results =
            Self::should_cache_result(self.upload_historical_results_strategy, action_result, true);
        let should_upload_ac_results = Self::should_cache_result(self.upload_ac_results_strategy, action_result, false);
        // Shortcut so we don't need to convert to proto if not needed.
        if !should_upload_ac_results && !should_upload_historical_results {
            return Ok(());
        }

        let mut execute_response = to_execute_response(action_result.clone());

        // In theory exit code should always be != 0 if there's an error, but for safety we
        // catch both.
        let message_template = if action_result.exit_code == 0 && action_result.error.is_none() {
            self.success_message_template.clone()
        } else {
            self.failure_message_template.clone()
        };

        let upload_historical_results_with_message_result = if should_upload_historical_results {
            let maybe_message = self
                .upload_historical_results_with_message(action_info, execute_response.clone(), message_template, hasher)
                .await;
            match maybe_message {
                Ok(message) => {
                    action_result.message = message.clone();
                    execute_response.message = message;
                    Ok(())
                }
                Err(e) => Result::<(), Error>::Err(e),
            }
        } else {
            match Self::format_execute_response_message(message_template, action_info, None, hasher) {
                Ok(message) => {
                    action_result.message = message.clone();
                    execute_response.message = message;
                    Ok(())
                }
                Err(e) => Err(e).err_tip(|| "Could not format message in cache_action_result"),
            }
        };

        // Note: Done in this order because we assume most results will succed and most configs will
        // either always upload upload historical results or only upload on filure. In which case
        // we can avoid an extra clone of the protos by doing this last with the above assumption.
        let ac_upload_results = if should_upload_ac_results {
            self.upload_ac_results(
                action_info,
                execute_response
                    .result
                    .err_tip(|| "No result set in cache_action_result")?,
                hasher,
            )
            .await
        } else {
            Ok(())
        };
        upload_historical_results_with_message_result.merge(ac_upload_results)
    }
}

pub struct RunningActionsManagerArgs<'a> {
    pub root_work_directory: String,
    pub execution_configuration: ExecutionConfiguration,
    pub cas_store: Arc<FastSlowStore>,
    pub ac_store: Option<Arc<dyn Store>>,
    pub historical_store: Arc<dyn Store>,
    pub upload_action_result_config: &'a UploadActionResultConfig,
    pub max_action_timeout: Duration,
    pub timeout_handled_externally: bool,
}

/// Holds state info about what is being executed and the interface for interacting
/// with actions while they are running.
pub struct RunningActionsManagerImpl {
    root_work_directory: String,
    execution_configuration: ExecutionConfiguration,
    cas_store: Arc<FastSlowStore>,
    filesystem_store: Arc<FilesystemStore>,
    upload_action_results: UploadActionResults,
    max_action_timeout: Duration,
    timeout_handled_externally: bool,
    running_actions: Mutex<HashMap<ActionId, Weak<RunningActionImpl>>>,
    // Note: We don't use Notify because we need to support a .wait_for()-like function, which
    // Notify does not support.
    action_done_tx: watch::Sender<()>,
    callbacks: Callbacks,
    metrics: Arc<Metrics>,
}

impl RunningActionsManagerImpl {
    pub fn new_with_callbacks(args: RunningActionsManagerArgs<'_>, callbacks: Callbacks) -> Result<Self, Error> {
        // Sadly because of some limitations of how Any works we need to clone more times than optimal.
        let filesystem_store = args
            .cas_store
            .fast_store()
            .clone()
            .as_any()
            .downcast_ref::<Arc<FilesystemStore>>()
            .err_tip(|| "Expected FilesystemStore store for .fast_store() in RunningActionsManagerImpl")?
            .clone();
        let (action_done_tx, _) = watch::channel(());
        Ok(Self {
            root_work_directory: args.root_work_directory,
            execution_configuration: args.execution_configuration,
            cas_store: args.cas_store,
            filesystem_store,
            upload_action_results: UploadActionResults::new(
                args.upload_action_result_config,
                args.ac_store,
                args.historical_store,
            )
            .err_tip(|| "During RunningActionsManagerImpl construction")?,
            max_action_timeout: args.max_action_timeout,
            timeout_handled_externally: args.timeout_handled_externally,
            running_actions: Mutex::new(HashMap::new()),
            action_done_tx,
            callbacks,
            metrics: Arc::new(Metrics::default()),
        })
    }

    pub fn new(args: RunningActionsManagerArgs<'_>) -> Result<Self, Error> {
        Self::new_with_callbacks(
            args,
            Callbacks {
                now_fn: SystemTime::now,
                sleep_fn: |duration| Box::pin(tokio::time::sleep(duration)),
            },
        )
    }

    fn make_work_directory<'a>(&'a self, action_id: &'a ActionId) -> impl Future<Output = Result<String, Error>> + 'a {
        self.metrics.make_work_directory.wrap(async move {
            let work_directory = format!("{}/{}", self.root_work_directory, hex::encode(action_id));
            fs::create_dir(&work_directory)
                .await
                .err_tip(|| format!("Error creating work directory {work_directory}"))?;
            Ok(work_directory)
        })
    }

    fn create_action_info(
        &self,
        start_execute: StartExecute,
        queued_timestamp: SystemTime,
    ) -> impl Future<Output = Result<ActionInfo, Error>> + '_ {
        self.metrics.create_action_info.wrap(async move {
            let execute_request = start_execute
                .execute_request
                .err_tip(|| "Expected execute_request to exist in StartExecute")?;
            let action_digest: DigestInfo = execute_request
                .action_digest
                .clone()
                .err_tip(|| "Expected action_digest to exist on StartExecute")?
                .try_into()?;
            let load_start_timestamp = (self.callbacks.now_fn)();
            let action = get_and_decode_digest::<Action>(Pin::new(self.cas_store.as_ref()), &action_digest)
                .await
                .err_tip(|| "During start_action")?;
            let action_info = ActionInfo::try_from_action_and_execute_request_with_salt(
                execute_request,
                action,
                start_execute.salt,
                load_start_timestamp,
                queued_timestamp,
            )
            .err_tip(|| "Could not create ActionInfo in create_and_add_action()")?;
            Ok(action_info)
        })
    }

    fn cleanup_action(&self, action_id: &ActionId) -> Result<(), Error> {
        let mut running_actions = self.running_actions.lock();
        let result = running_actions
            .remove(action_id)
            .err_tip(|| format!("Expected action id '{action_id:?}' to exist in RunningActionsManagerImpl"));
        // No need to copy anything, we just are telling the receivers an event happened.
        self.action_done_tx.send_modify(|_| {});
        result.map(|_| ())
    }

    // Note: We do not capture metrics on this call, only `.kill_all()`.
    // Important: When the future returns the process may still be running.
    async fn kill_action(action: Arc<RunningActionImpl>) {
        let kill_channel_tx = {
            let mut action_state = action.state.lock();
            action_state.kill_channel_tx.take()
        };
        if let Some(kill_channel_tx) = kill_channel_tx {
            if kill_channel_tx.send(()).is_err() {
                log::error!("Error sending kill to running action {}", hex::encode(action.action_id));
            }
        }
    }
}

#[async_trait]
impl RunningActionsManager for RunningActionsManagerImpl {
    type RunningAction = RunningActionImpl;

    async fn create_and_add_action(
        self: &Arc<Self>,
        worker_id: String,
        start_execute: StartExecute,
    ) -> Result<Arc<RunningActionImpl>, Error> {
        self.metrics
            .create_and_add_action
            .wrap(async move {
                let queued_timestamp = start_execute
                    .queued_timestamp
                    .clone()
                    .and_then(|time| time.try_into().ok())
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                let action_info = self.create_action_info(start_execute, queued_timestamp).await?;
                log::info!("\x1b[0;31mWorker Received Action\x1b[0m: {:?}", action_info);
                let action_id = action_info.unique_qualifier.get_hash();
                let work_directory = self.make_work_directory(&action_id).await?;
                let execution_metadata = ExecutionMetadata {
                    worker: worker_id,
                    queued_timestamp: action_info.insert_timestamp,
                    worker_start_timestamp: action_info.load_timestamp,
                    worker_completed_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_start_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_completed_timestamp: SystemTime::UNIX_EPOCH,
                    execution_start_timestamp: SystemTime::UNIX_EPOCH,
                    execution_completed_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_start_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_completed_timestamp: SystemTime::UNIX_EPOCH,
                };
                let timeout = if action_info.timeout == Duration::ZERO || self.timeout_handled_externally {
                    self.max_action_timeout
                } else {
                    action_info.timeout
                };
                if timeout > self.max_action_timeout {
                    return Err(make_err!(
                        Code::InvalidArgument,
                        "Action timeout of {} seconds is greater than the maximum allowed timeout of {} seconds",
                        timeout.as_secs_f32(),
                        self.max_action_timeout.as_secs_f32()
                    ));
                }
                let running_action = Arc::new(RunningActionImpl::new(
                    execution_metadata,
                    action_id,
                    work_directory,
                    action_info,
                    timeout,
                    self.clone(),
                ));
                {
                    let mut running_actions = self.running_actions.lock();
                    running_actions.insert(action_id, Arc::downgrade(&running_action));
                }
                Ok(running_action)
            })
            .await
    }

    async fn cache_action_result(
        &self,
        action_info: DigestInfo,
        action_result: &mut ActionResult,
        hasher: DigestHasherFunc,
    ) -> Result<(), Error> {
        self.metrics
            .cache_action_result
            .wrap(
                self.upload_action_results
                    .cache_action_result(action_info, action_result, hasher),
            )
            .await
    }

    // Note: When the future returns the process should be fully killed and cleaned up.
    async fn kill_all(&self) {
        self.metrics
            .kill_all
            .wrap_no_capture_result(async move {
                let kill_actions: Vec<Arc<RunningActionImpl>> = {
                    let running_actions = self.running_actions.lock();
                    running_actions
                        .iter()
                        .filter_map(|(_action_id, action)| action.upgrade())
                        .collect()
                };
                for action in kill_actions {
                    Self::kill_action(action).await;
                }
            })
            .await;
        // Ignore error. If error happens it means there's no sender, which is not a problem.
        // Note: Sanity check this API will always check current value then future values:
        // https://play.rust-lang.org/?version=stable&edition=2021&gist=23103652cc1276a97e5f9938da87fdb2
        let _ = self
            .action_done_tx
            .subscribe()
            .wait_for(|_| self.running_actions.lock().is_empty())
            .await;
    }

    #[inline]
    fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}

#[derive(Default)]
pub struct Metrics {
    create_and_add_action: AsyncCounterWrapper,
    cache_action_result: AsyncCounterWrapper,
    kill_all: AsyncCounterWrapper,
    create_action_info: AsyncCounterWrapper,
    make_work_directory: AsyncCounterWrapper,
    prepare_action: AsyncCounterWrapper,
    execute: AsyncCounterWrapper,
    upload_results: AsyncCounterWrapper,
    cleanup: AsyncCounterWrapper,
    get_finished_result: AsyncCounterWrapper,
    get_proto_command_from_store: AsyncCounterWrapper,
    download_to_directory: AsyncCounterWrapper,
    prepare_output_files: AsyncCounterWrapper,
    prepare_output_paths: AsyncCounterWrapper,
    child_process: AsyncCounterWrapper,
    child_process_success_error_code: CounterWithTime,
    child_process_failure_error_code: CounterWithTime,
    upload_stdout: AsyncCounterWrapper,
    upload_stderr: AsyncCounterWrapper,
    task_timeouts: CounterWithTime,
}

impl MetricsComponent for Metrics {
    fn gather_metrics(&self, c: &mut CollectorState) {
        c.publish(
            "create_and_add_action",
            &self.create_and_add_action,
            "Stats about the create_and_add_action command.",
        );
        c.publish(
            "cache_action_result",
            &self.cache_action_result,
            "Stats about the cache_action_result command.",
        );
        c.publish("kill_all", &self.kill_all, "Stats about the kill_all command.");
        c.publish(
            "create_action_info",
            &self.create_action_info,
            "Stats about the create_action_info command.",
        );
        c.publish(
            "make_work_directory",
            &self.make_work_directory,
            "Stats about the make_work_directory command.",
        );
        c.publish(
            "prepare_action",
            &self.prepare_action,
            "Stats about the prepare_action command.",
        );
        c.publish("execute", &self.execute, "Stats about the execute command.");
        c.publish(
            "upload_results",
            &self.upload_results,
            "Stats about the upload_results command.",
        );
        c.publish("cleanup", &self.cleanup, "Stats about the cleanup command.");
        c.publish(
            "get_finished_result",
            &self.get_finished_result,
            "Stats about the get_finished_result command.",
        );
        c.publish(
            "get_proto_command_from_store",
            &self.get_proto_command_from_store,
            "Stats about the get_proto_command_from_store command.",
        );
        c.publish(
            "download_to_directory",
            &self.download_to_directory,
            "Stats about the download_to_directory command.",
        );
        c.publish(
            "prepare_output_files",
            &self.prepare_output_files,
            "Stats about the prepare_output_files command.",
        );
        c.publish(
            "prepare_output_paths",
            &self.prepare_output_paths,
            "Stats about the prepare_output_paths command.",
        );
        c.publish(
            "child_process",
            &self.child_process,
            "Stats about the child_process command.",
        );
        c.publish(
            "child_process_success_error_code",
            &self.child_process_success_error_code,
            "Stats about the child_process_success_error_code command.",
        );
        c.publish(
            "child_process_failure_error_code",
            &self.child_process_failure_error_code,
            "Stats about the child_process_failure_error_code command.",
        );
        c.publish(
            "upload_stdout",
            &self.upload_stdout,
            "Total time spent uploading stdout.",
        );
        c.publish(
            "upload_stderr",
            &self.upload_stderr,
            "Total time spent uploading stderr.",
        );
        c.publish(
            "task_timeouts_count",
            &self.task_timeouts,
            "Total number of task timeouts.",
        )
    }
}
