// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use fast_async_mutex::mutex::Mutex;
use filetime::{set_file_mtime, FileTime};
use futures::future::{BoxFuture, FutureExt, TryFutureExt};
use futures::stream::{FuturesUnordered, TryStreamExt};
use tokio::task::spawn_blocking;

use ac_utils::get_and_decode_digest;
use action_messages::ActionInfo;
use async_trait::async_trait;
use common::{fs, DigestInfo};
use error::{make_err, Code, Error, ResultExt};
use fast_slow_store::FastSlowStore;
use filesystem_store::FilesystemStore;
use proto::build::bazel::remote::execution::v2::{Action, Directory as ProtoDirectory};
use proto::com::github::allada::turbo_cache::remote_execution::{ExecuteFinishedResult, StartExecute};

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
            futures.push(
                cas_store
                    .populate_fast_store(digest.clone())
                    .and_then(move |_| async move {
                        fs::hard_link(src, &dest)
                            .await
                            .map_err(|e| make_err!(Code::Internal, "Could not make hardlink, {:?} : {}", e, dest))?;
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

        for symlink in directory.symlinks {
            let dest = format!("{}/{}", current_directory, symlink.name);
            futures.push(
                async move {
                    fs::symlink(&symlink.target, &dest)
                        .await
                        .err_tip(|| format!("Could not create symlink {} -> {}", symlink.target, dest))?;
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
    async fn get_finished_result(self: Arc<Self>) -> Result<ExecuteFinishedResult, Error>;
}

pub struct RunningActionImpl {
    _action_info: ActionInfo,
    _cas_store: Pin<Arc<FastSlowStore>>,
    _filesystem_store: Pin<Arc<FilesystemStore>>,
}

impl RunningActionImpl {
    fn new(action_info: ActionInfo, cas_store: Arc<FastSlowStore>, filesystem_store: Arc<FilesystemStore>) -> Self {
        Self {
            _action_info: action_info,
            _cas_store: Pin::new(cas_store),
            _filesystem_store: Pin::new(filesystem_store),
        }
    }
}

#[async_trait]
impl RunningAction for RunningActionImpl {
    async fn prepare_action(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        unimplemented!();
    }

    async fn execute(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        unimplemented!();
    }

    async fn upload_results(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        unimplemented!();
    }

    async fn cleanup(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        unimplemented!();
    }

    async fn get_finished_result(self: Arc<Self>) -> Result<ExecuteFinishedResult, Error> {
        unimplemented!();
    }
}

#[async_trait]
pub trait RunningActionsManager: Sync + Send + Sized + Unpin + 'static {
    type RunningAction: RunningAction;

    async fn create_and_add_action(
        self: Arc<Self>,
        start_execute: StartExecute,
    ) -> Result<Arc<Self::RunningAction>, Error>;
}

type ActionId = [u8; 32];

/// Holds state info about what is being executed and the interface for interacting
/// with actions while they are running.
pub struct RunningActionsManagerImpl {
    cas_store: Arc<FastSlowStore>,
    filesystem_store: Arc<FilesystemStore>,
    running_actions: Mutex<HashMap<ActionId, Weak<RunningActionImpl>>>,
}

impl RunningActionsManagerImpl {
    pub fn new(cas_store: Arc<FastSlowStore>) -> Result<Self, Error> {
        // Sadly because of some limitations of how Any works we need to clone more times than optimal.
        let filesystem_store = cas_store
            .fast_slow()
            .clone()
            .as_any()
            .downcast_ref::<Arc<FilesystemStore>>()
            .err_tip(|| "Expected fast slow store for cas_store in RunningActionsManagerImpl")?
            .clone();
        Ok(Self {
            cas_store,
            filesystem_store,
            running_actions: Mutex::new(HashMap::new()),
        })
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
}

#[async_trait]
impl RunningActionsManager for RunningActionsManagerImpl {
    type RunningAction = RunningActionImpl;

    async fn create_and_add_action(
        self: Arc<Self>,
        start_execute: StartExecute,
    ) -> Result<Arc<RunningActionImpl>, Error> {
        let action_info = self.create_action_info(start_execute).await?;
        let action_id = action_info.unique_qualifier.get_hash();
        let running_action = Arc::new(RunningActionImpl::new(
            action_info,
            self.cas_store.clone(),
            self.filesystem_store.clone(),
        ));
        {
            let mut running_actions = self.running_actions.lock().await;
            running_actions.insert(action_id, Arc::downgrade(&running_action));
        }
        Ok(running_action)
    }
}
