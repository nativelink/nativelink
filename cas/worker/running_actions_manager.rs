// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::{vec_deque::VecDeque, HashMap};
use std::fmt::Debug;
use std::fs::Permissions;
use std::io::Cursor;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc, Weak};
use std::time::SystemTime;

use bytes::{BufMut, Bytes, BytesMut};
use fast_async_mutex::mutex::Mutex;
use filetime::{set_file_mtime, FileTime};
use futures::future::{try_join, try_join3, try_join_all, BoxFuture, FutureExt, TryFutureExt};
use futures::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use hex;
use relative_path::RelativePath;
use tokio::io::AsyncSeekExt;
use tokio::process;
use tokio::sync::oneshot;
use tokio::task::spawn_blocking;
use tokio_stream::wrappers::ReadDirStream;
use tokio_util::io::ReaderStream;

use ac_utils::{compute_digest, get_and_decode_digest, serialize_and_upload_message, upload_to_store};
use action_messages::{ActionInfo, ActionResult, DirectoryInfo, ExecutionMetadata, FileInfo, NameOrPath, SymlinkInfo};
use async_trait::async_trait;
use common::{fs, log, DigestInfo, JoinHandleDropGuard};
use error::{make_err, make_input_err, Code, Error, ResultExt};
use fast_slow_store::FastSlowStore;
use filesystem_store::FilesystemStore;
use proto::build::bazel::remote::execution::v2::{
    Action, Command as ProtoCommand, Directory as ProtoDirectory, Directory, DirectoryNode, FileNode, SymlinkNode,
    Tree as ProtoTree,
};
use proto::com::github::allada::turbo_cache::remote_execution::StartExecute;
use store::Store;

pub type ActionId = [u8; 32];

/// For simplicity we use a fixed exit code for cases when our program is terminated
/// due to a signal.
const EXIT_CODE_FOR_SIGNAL: i32 = 9;

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
            let src = filesystem_store.get_file_for_digest(&digest);
            let dest = format!("{}/{}", current_directory, file.name);
            let mut mtime = None;
            let mut unix_mode = None;
            if let Some(properties) = file.node_properties {
                mtime = properties.mtime;
                unix_mode = properties.unix_mode;
            }
            let is_executable = file.is_executable;
            futures.push(
                cas_store
                    .populate_fast_store(digest.clone())
                    .and_then(move |_| async move {
                        fs::hard_link(src, &dest)
                            .await
                            .map_err(|e| make_err!(Code::Internal, "Could not make hardlink, {:?} : {}", e, dest))?;
                        if is_executable {
                            unix_mode = Some(unix_mode.unwrap_or(0o444) | 0o111);
                        }
                        if let Some(unix_mode) = unix_mode {
                            fs::set_permissions(&dest, Permissions::from_mode(unix_mode))
                                .await
                                .err_tip(|| format!("Could not set unix mode in download_to_directory {}", dest))?;
                        }
                        if let Some(mtime) = mtime {
                            spawn_blocking(move || {
                                set_file_mtime(&dest, FileTime::from_unix_time(mtime.seconds, mtime.nanos as u32))
                                    .err_tip(|| format!("Failed to set mtime in download_to_directory {}", dest))
                            })
                            .await
                            .err_tip(|| "Failed to launch spawn_blocking in download_to_directory")??;
                        }
                        Ok(())
                    })
                    .map_err(move |e| e.append(format!("for digest {:?}", digest)))
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
                        .err_tip(|| format!("Could not create directory {}", new_directory_path))?;
                    download_to_directory(cas_store, filesystem_store, &digest, &new_directory_path)
                        .await
                        .err_tip(|| format!("in download_to_directory : {}", new_directory_path))?;
                    Ok(())
                }
                .boxed(),
            );
        }

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

async fn upload_file<'a>(
    file_handle: fs::FileSlot<'static>,
    cas_store: Pin<&'a dyn Store>,
    full_path: impl AsRef<Path> + Debug,
) -> Result<FileInfo, Error> {
    let (digest, mut file_handle) = compute_digest(file_handle)
        .await
        .err_tip(|| format!("for {:?}", full_path))?;
    file_handle.rewind().await.err_tip(|| "Could not rewind file")?;
    upload_to_store(cas_store, digest.clone(), &mut file_handle)
        .await
        .err_tip(|| format!("for {:?}", full_path))?;

    let name = full_path
        .as_ref()
        .file_name()
        .err_tip(|| format!("Expected file_name to exist on {:?}", full_path))?
        .to_str()
        .err_tip(|| make_err!(Code::Internal, "Could not convert {:?} to string", full_path))?
        .to_string();
    let metadata = file_handle
        .as_ref()
        .metadata()
        .await
        .err_tip(|| format!("While reading metadata for {:?}", full_path))?;
    let is_executable = (metadata.mode() & 0o001) != 0;
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
        .err_tip(|| format!("Could not get read_link path of {:?}", full_path))?;

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
        .err_tip(|| format!("Expected file_name to exist on {:?}", full_path))?
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
) -> BoxFuture<'a, Result<(Directory, VecDeque<ProtoDirectory>), Error>> {
    Box::pin(async move {
        let file_futures = FuturesUnordered::new();
        let dir_futures = FuturesUnordered::new();
        let symlink_futures = FuturesUnordered::new();
        {
            let (_permit, dir_handle) = fs::read_dir(&full_dir_path)
                .await
                .err_tip(|| format!("Error reading dir for reading {:?}", full_dir_path))?
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
                    .err_tip(|| format!("Error running file_type() on {:?}", entry))?;
                let full_path = full_dir_path.as_ref().join(entry.path());
                if file_type.is_dir() {
                    let full_dir_path = full_dir_path.clone();
                    dir_futures.push(
                        upload_directory(cas_store, full_path.clone(), &full_work_directory)
                            .and_then(|(dir, all_dirs)| async move {
                                let directory_name = full_path
                                    .file_name()
                                    .err_tip(|| format!("Expected file_name to exist on {:?}", full_dir_path))?
                                    .to_str()
                                    .err_tip(|| {
                                        make_err!(Code::Internal, "Could not convert {:?} to string", full_dir_path)
                                    })?
                                    .to_string();

                                let digest = serialize_and_upload_message(&dir, cas_store)
                                    .await
                                    .err_tip(|| format!("for {:?}", full_path))?;

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
                        let file_handle = fs::open_file(&full_path)
                            .await
                            .err_tip(|| format!("Could not open file {:?}", full_path))?;
                        upload_file(file_handle, cas_store, full_path)
                            .map_ok(|v| v.into())
                            .await
                    });
                } else if file_type.is_symlink() {
                    symlink_futures.push(upload_symlink(full_path, &full_work_directory).map_ok(|v| v.into()));
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
}

struct RunningActionImplExecutionResult {
    stdout: Bytes,
    stderr: Bytes,
    exit_code: i32,
}

struct RunningActionImplState {
    command_proto: Option<ProtoCommand>,
    // TODO(allada) Kill is not implemented yet, but is instrumented.
    _kill_channel_tx: Option<oneshot::Sender<()>>,
    kill_channel_rx: Option<oneshot::Receiver<()>>,
    execution_result: Option<RunningActionImplExecutionResult>,
    action_result: Option<ActionResult>,
}

pub struct RunningActionImpl {
    worker_id: String,
    action_id: ActionId,
    work_directory: String,
    action_info: ActionInfo,
    running_actions_manager: Arc<RunningActionsManagerImpl>,
    state: Mutex<RunningActionImplState>,
    did_cleanup: AtomicBool,
}

impl RunningActionImpl {
    fn new(
        worker_id: String,
        action_id: ActionId,
        work_directory: String,
        action_info: ActionInfo,
        running_actions_manager: Arc<RunningActionsManagerImpl>,
    ) -> Self {
        let (kill_channel_tx, kill_channel_rx) = oneshot::channel();
        Self {
            worker_id,
            action_id,
            work_directory,
            action_info,
            running_actions_manager,
            state: Mutex::new(RunningActionImplState {
                command_proto: None,
                kill_channel_rx: Some(kill_channel_rx),
                _kill_channel_tx: Some(kill_channel_tx),
                execution_result: None,
                action_result: None,
            }),
            did_cleanup: AtomicBool::new(false),
        }
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
    /// Prepares any actions needed to execution this action. This action will do the following:
    /// * Download any files needed to execute the action
    /// * Build a folder with all files needed to execute the action.
    /// This function will aggressively download and spawn potentially thousands of futures. It is
    /// up to the stores to rate limit if needed.
    async fn prepare_action(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        let command = {
            // Download and build out our input files/folders. Also fetch and decode our Command.
            let cas_store_pin = Pin::new(self.running_actions_manager.cas_store.as_ref());
            let command_fut = async {
                Ok(
                    get_and_decode_digest::<ProtoCommand>(cas_store_pin, &self.action_info.command_digest)
                        .await
                        .err_tip(|| "Converting command_digest to Command")?,
                )
            };
            let filesystem_store_pin = Pin::new(self.running_actions_manager.filesystem_store.as_ref());
            // Download the input files/folder and place them into the temp directory.
            let download_to_directory_fut = download_to_directory(
                cas_store_pin,
                filesystem_store_pin,
                &self.action_info.input_root_digest,
                &self.work_directory,
            );
            let (command, _) = try_join(command_fut, download_to_directory_fut).await?;
            command
        };
        log::info!("\x1b[0;31mWorker Received Command\x1b[0m: {:?}", command);
        {
            // Create all directories needed for our output paths. This is required by the bazel spec.
            let prepare_output_directories = |output_file| {
                let full_output_path = format!("{}/{}", self.work_directory, output_file);
                async move {
                    let full_parent_path = Path::new(&full_output_path)
                        .parent()
                        .err_tip(|| format!("Parent path for {} has no parent", full_output_path))?;
                    fs::create_dir_all(full_parent_path)
                        .await
                        .err_tip(|| format!("Error creating output directory {} (file)", full_parent_path.display()))?;
                    Result::<(), Error>::Ok(())
                }
            };
            try_join_all(command.output_paths.iter().map(prepare_output_directories)).await?;
        }
        {
            let mut state = self.state.lock().await;
            state.command_proto = Some(command);
        }
        Ok(self)
    }

    async fn execute(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        let (command_proto, mut kill_channel_rx) = {
            let mut state = self.state.lock().await;
            (
                state
                    .command_proto
                    .take()
                    .err_tip(|| "Expected state to have command_proto in execute()")?,
                state
                    .kill_channel_rx
                    .take()
                    .err_tip(|| "Expected state to have kill_channel_rx in execute()")?,
            )
        };
        let args = &command_proto.arguments[..];
        if args.len() < 1 {
            return Err(make_input_err!("No arguments provided in Command proto"));
        }
        let mut command_builder = process::Command::new(&args[0]);
        command_builder
            .args(&args[1..])
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .current_dir(format!("{}/{}", self.work_directory, command_proto.working_directory))
            .env_clear();
        for environment_variable in &command_proto.environment_variables {
            command_builder.env(&environment_variable.name, &environment_variable.value);
        }

        let mut child_process = command_builder
            .spawn()
            .err_tip(|| format!("Could not execute command {:?}", command_proto.arguments))?;
        let mut stdout_stream = ReaderStream::new(
            child_process
                .stdout
                .take()
                .err_tip(|| "Expected stdout to exist on command this should never happen")?,
        );
        let mut stderr_stream = ReaderStream::new(
            child_process
                .stderr
                .take()
                .err_tip(|| "Expected stderr to exist on command this should never happen")?,
        );

        let all_stdout_fut = JoinHandleDropGuard::new(tokio::spawn(async move {
            let mut all_stdout = BytesMut::new();
            while let Some(chunk) = stdout_stream.next().await {
                all_stdout.put(chunk.err_tip(|| "Error reading stdout stream")?);
            }
            Result::<Bytes, Error>::Ok(all_stdout.freeze())
        }));
        let all_stderr_fut = JoinHandleDropGuard::new(tokio::spawn(async move {
            let mut all_stderr = BytesMut::new();
            while let Some(chunk) = stderr_stream.next().await {
                all_stderr.put(chunk.err_tip(|| "Error reading stderr stream")?);
            }
            Result::<Bytes, Error>::Ok(all_stderr.freeze())
        }));
        loop {
            tokio::select! {
                maybe_exit_status = child_process.wait() => {
                    let exit_status = maybe_exit_status.err_tip(|| "Failed to collect exit code of process")?;
                    // TODO(allada) We should implement stderr/stdout streaming to client here.
                    let stdout = all_stdout_fut.await.err_tip(|| "Internal error reading from stdout of worker task")??;
                    let stderr = all_stderr_fut.await.err_tip(|| "Internal error reading from stderr of worker task")??;
                    {
                        let mut state = self.state.lock().await;
                        state.command_proto = Some(command_proto);
                        state.execution_result = Some(RunningActionImplExecutionResult{
                            stdout,
                            stderr,
                            exit_code: exit_status.code().unwrap_or(EXIT_CODE_FOR_SIGNAL),
                        });
                    }
                    return Ok(self);
                },
                _ = &mut kill_channel_rx => {
                    if let Err(e) = child_process.start_kill() {
                        log::error!("Could kill process in RunningActionsManager : {:?}", e);
                    }
                },
            }
        }
        // Unreachable.
    }

    async fn upload_results(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        let (mut command_proto, execution_result) = {
            let mut state = self.state.lock().await;
            (
                state
                    .command_proto
                    .take()
                    .err_tip(|| "Expected state to have command_proto in execute()")?,
                state
                    .execution_result
                    .take()
                    .err_tip(|| "Execution result does not exist at upload_results stage")?,
            )
        };
        let cas_store = Pin::new(self.running_actions_manager.cas_store.as_ref());
        let (stdout_digest, stderr_digest) = {
            // Upload our stdout/stderr to our CAS store.
            try_join(
                async {
                    let cursor = Cursor::new(execution_result.stdout);
                    let (digest, mut cursor) = compute_digest(cursor).await?;
                    cursor.rewind().await.err_tip(|| "Could not rewind cursor")?;
                    upload_to_store(cas_store, digest.clone(), &mut cursor).await?;
                    Result::<DigestInfo, Error>::Ok(digest)
                },
                async {
                    let cursor = Cursor::new(execution_result.stderr);
                    let (digest, mut cursor) = compute_digest(cursor).await?;
                    cursor.rewind().await.err_tip(|| "Could not rewind cursor")?;
                    upload_to_store(cas_store, digest.clone(), &mut cursor).await?;
                    Result::<DigestInfo, Error>::Ok(digest)
                },
            )
            .await?
        };

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
        for entry in output_paths.into_iter() {
            let full_path = format!("{}/{}", self.work_directory, entry);
            let work_directory = &self.work_directory;
            output_path_futures.push(async move {
                let metadata = {
                    let file_handle = match fs::open_file(&full_path).await {
                        Ok(handle) => handle,
                        Err(e) => {
                            if e.code == Code::NotFound {
                                // In the event our output does not exist, according to the bazel remote
                                // execution spec, we simply ignore it continue.
                                return Result::<OutputType, Error>::Ok(OutputType::None);
                            }
                            return Err(e).err_tip(|| format!("Could not open file {}", full_path));
                        }
                    };
                    // We cannot rely on the file_handle's metadata, because it follows symlinks, so
                    // we need to instead use `symlink_metadata`.
                    let metadata = fs::symlink_metadata(&full_path)
                        .await
                        .err_tip(|| format!("While querying symlink metadata for {}", entry))?;
                    if metadata.is_file() {
                        return Ok(OutputType::File(
                            upload_file(file_handle, cas_store, full_path)
                                .await
                                .map(|mut file_info| {
                                    file_info.name_or_path = NameOrPath::Path(entry);
                                    file_info
                                })?,
                        ));
                    }
                    metadata
                };
                if metadata.is_dir() {
                    Ok(OutputType::Directory(
                        upload_directory(cas_store, full_path, work_directory)
                            .and_then(|(root_dir, children)| async move {
                                let tree = ProtoTree {
                                    root: Some(root_dir),
                                    children: children.into(),
                                };
                                let tree_digest = serialize_and_upload_message(&tree, cas_store)
                                    .await
                                    .err_tip(|| format!("While processing {}", entry))?;
                                Ok(DirectoryInfo {
                                    path: entry,
                                    tree_digest,
                                })
                            })
                            .await?,
                    ))
                } else if metadata.is_symlink() {
                    let output_symlink = upload_symlink(&full_path, work_directory)
                        .await
                        .map(|mut symlink_info| {
                            symlink_info.name_or_path = NameOrPath::Path(entry);
                            symlink_info
                        })?;
                    match fs::metadata(&full_path).await {
                        Ok(metadata) => {
                            if metadata.is_dir() {
                                return Ok(OutputType::DirectorySymlink(output_symlink));
                            } else {
                                // Note: If it's anything but directory we put it as a file symlink.
                                return Ok(OutputType::FileSymlink(output_symlink));
                            }
                        }
                        Err(e) => {
                            if e.code != Code::NotFound {
                                return Err(e)
                                    .err_tip(|| format!("While querying target symlink metadata for {}", full_path));
                            }
                            // If the file doesn't exist, we consider it a file. Even though the
                            // file doesn't exist we still need to populate an entry.
                            return Ok(OutputType::FileSymlink(output_symlink));
                        }
                    }
                } else {
                    Err(make_err!(
                        Code::Internal,
                        "{} was not a file, folder or symlink. Must be one.",
                        full_path
                    ))
                }
            });
        }
        let mut output_files = vec![];
        let mut output_folders = vec![];
        let mut output_directory_symlinks = vec![];
        let mut output_file_symlinks = vec![];
        while let Some(output_type) = output_path_futures.try_next().await? {
            match output_type {
                OutputType::File(output_file) => output_files.push(output_file),
                OutputType::Directory(output_folder) => output_folders.push(output_folder),
                OutputType::FileSymlink(output_symlink) => output_file_symlinks.push(output_symlink),
                OutputType::DirectorySymlink(output_symlink) => output_directory_symlinks.push(output_symlink),
                OutputType::None => { /* Safe to ignore */ }
            }
        }
        drop(output_path_futures);
        output_files.sort_unstable_by(|a, b| a.name_or_path.cmp(&b.name_or_path));
        output_folders.sort_unstable_by(|a, b| a.path.cmp(&b.path));
        output_file_symlinks.sort_unstable_by(|a, b| a.name_or_path.cmp(&b.name_or_path));
        output_directory_symlinks.sort_unstable_by(|a, b| a.name_or_path.cmp(&b.name_or_path));
        {
            let mut state = self.state.lock().await;
            state.action_result = Some(ActionResult {
                output_files,
                output_folders,
                output_directory_symlinks,
                output_file_symlinks,
                exit_code: execution_result.exit_code,
                stdout_digest: stdout_digest.into(),
                stderr_digest: stderr_digest.into(),
                // TODO(allada) We should implement the timing info here.
                execution_metadata: ExecutionMetadata {
                    worker: self.worker_id.to_string(),
                    queued_timestamp: SystemTime::UNIX_EPOCH,
                    worker_start_timestamp: SystemTime::UNIX_EPOCH,
                    worker_completed_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_start_timestamp: SystemTime::UNIX_EPOCH,
                    input_fetch_completed_timestamp: SystemTime::UNIX_EPOCH,
                    execution_start_timestamp: SystemTime::UNIX_EPOCH,
                    execution_completed_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_start_timestamp: SystemTime::UNIX_EPOCH,
                    output_upload_completed_timestamp: SystemTime::UNIX_EPOCH,
                },
                server_logs: Default::default(), // TODO(allada) Not implemented.
            });
        }
        Ok(self)
    }

    async fn cleanup(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        // Note: We need to be careful to keep trying to cleanup even if one of the steps fails.
        let remove_dir_result = fs::remove_dir_all(&self.work_directory)
            .await
            .err_tip(|| format!("Could not remove working directory {}", self.work_directory));
        self.did_cleanup.store(true, Ordering::Relaxed);
        if let Err(e) = self.running_actions_manager.cleanup_action(&self.action_id).await {
            return Result::<Arc<Self>, Error>::Err(e).merge(remove_dir_result.map(|_| self));
        }
        remove_dir_result.map(|_| self)
    }

    async fn get_finished_result(self: Arc<Self>) -> Result<ActionResult, Error> {
        let mut state = self.state.lock().await;
        state
            .action_result
            .take()
            .err_tip(|| "Expected action_result to exist in get_finished_result")
    }
}

#[async_trait]
pub trait RunningActionsManager: Sync + Send + Sized + Unpin + 'static {
    type RunningAction: RunningAction;

    async fn create_and_add_action(
        self: Arc<Self>,
        worker_id: String,
        start_execute: StartExecute,
    ) -> Result<Arc<Self::RunningAction>, Error>;

    async fn get_action(&self, action_id: &ActionId) -> Result<Arc<Self::RunningAction>, Error>;
}

/// Holds state info about what is being executed and the interface for interacting
/// with actions while they are running.
pub struct RunningActionsManagerImpl {
    root_work_directory: String,
    cas_store: Arc<FastSlowStore>,
    filesystem_store: Arc<FilesystemStore>,
    running_actions: Mutex<HashMap<ActionId, Weak<RunningActionImpl>>>,
}

impl RunningActionsManagerImpl {
    pub fn new(root_work_directory: String, cas_store: Arc<FastSlowStore>) -> Result<Self, Error> {
        // Sadly because of some limitations of how Any works we need to clone more times than optimal.
        let filesystem_store = cas_store
            .fast_store()
            .clone()
            .as_any()
            .downcast_ref::<Arc<FilesystemStore>>()
            .err_tip(|| "Expected fast slow store for cas_store in RunningActionsManagerImpl")?
            .clone();
        Ok(Self {
            root_work_directory,
            cas_store,
            filesystem_store,
            running_actions: Mutex::new(HashMap::new()),
        })
    }

    async fn make_work_directory(&self, action_id: &ActionId) -> Result<String, Error> {
        let work_directory = format!("{}/{}", self.root_work_directory, hex::encode(action_id));
        fs::create_dir(&work_directory)
            .await
            .err_tip(|| format!("Error creating work directory {}", work_directory))?;
        Ok(work_directory)
    }

    async fn create_action_info(&self, start_execute: StartExecute) -> Result<ActionInfo, Error> {
        let execute_request = start_execute
            .execute_request
            .err_tip(|| "Expected execute_request to exist in StartExecute")?;
        let action_digest: DigestInfo = execute_request
            .action_digest
            .clone()
            .err_tip(|| "Expected action_digest to exist on StartExecute")?
            .try_into()?;
        let action = get_and_decode_digest::<Action>(Pin::new(self.cas_store.as_ref()), &action_digest)
            .await
            .err_tip(|| "During start_action")?;
        Ok(
            ActionInfo::try_from_action_and_execute_request_with_salt(execute_request, action, start_execute.salt)
                .err_tip(|| "Could not create ActionInfo in create_and_add_action()")?,
        )
    }

    async fn cleanup_action(&self, action_id: &ActionId) -> Result<(), Error> {
        let mut running_actions = self.running_actions.lock().await;
        running_actions.remove(action_id).err_tip(|| {
            format!(
                "Expected action id '{:?}' to exist in RunningActionsManagerImpl",
                action_id
            )
        })?;
        Ok(())
    }
}

#[async_trait]
impl RunningActionsManager for RunningActionsManagerImpl {
    type RunningAction = RunningActionImpl;

    async fn create_and_add_action(
        self: Arc<Self>,
        worker_id: String,
        start_execute: StartExecute,
    ) -> Result<Arc<RunningActionImpl>, Error> {
        let action_info = self.create_action_info(start_execute).await?;
        log::info!("\x1b[0;31mWorker Received Action\x1b[0m: {:?}", action_info);
        let action_id = action_info.unique_qualifier.get_hash();
        let work_directory = self.make_work_directory(&action_id).await?;
        let running_action = Arc::new(RunningActionImpl::new(
            worker_id,
            action_id,
            work_directory,
            action_info,
            self.clone(),
        ));
        {
            let mut running_actions = self.running_actions.lock().await;
            running_actions.insert(action_id, Arc::downgrade(&running_action));
        }
        Ok(running_action)
    }

    async fn get_action(&self, action_id: &ActionId) -> Result<Arc<Self::RunningAction>, Error> {
        let running_actions = self.running_actions.lock().await;
        Ok(running_actions
            .get(action_id)
            .err_tip(|| format!("Action '{:?}' not found", action_id))?
            .upgrade()
            .err_tip(|| "Could not upgrade RunningAction Arc")?)
    }
}
