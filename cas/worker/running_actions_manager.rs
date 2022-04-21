// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use fast_async_mutex::mutex::Mutex;

use ac_utils::get_and_decode_digest;
use action_messages::ActionInfo;
use async_trait::async_trait;
use common::DigestInfo;
use error::{Error, ResultExt};
use proto::build::bazel::remote::execution::v2::Action;
use proto::com::github::allada::turbo_cache::remote_execution::{ExecuteFinishedResult, StartExecute};
use store::Store;

type JobId = [u8; 32];
struct RunningAction {}

impl RunningAction {
    fn new(_action_info: ActionInfo, _salt: u64) -> Self {
        Self {}
    }
}

#[async_trait]
pub trait RunningActionsManager: Sync + Send + Sized + Unpin + 'static {
    async fn start_action(self: Arc<Self>, start_execute: StartExecute) -> Result<ExecuteFinishedResult, Error>;
}

/// Holds state info about what is being executed and the interface for interacting
/// with actions while they are running.
pub struct RunningActionsManagerImpl {
    cas_store: Pin<Arc<dyn Store>>,
    running_actions: Mutex<HashMap<JobId, RunningAction>>,
}

impl RunningActionsManagerImpl {
    pub fn new(cas_store: Arc<dyn Store>) -> Self {
        Self {
            cas_store: Pin::new(cas_store),
            running_actions: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl RunningActionsManager for RunningActionsManagerImpl {
    async fn start_action(self: Arc<Self>, start_execute: StartExecute) -> Result<ExecuteFinishedResult, Error> {
        let execute_request = start_execute
            .execute_request
            .err_tip(|| "Expected execute_request to exist in LocalWorker")?;
        let action_digest: DigestInfo = execute_request
            .action_digest
            .clone()
            .err_tip(|| "Expected action_digest to exist on StartExecute")?
            .try_into()?;
        let action = get_and_decode_digest::<Action>(self.cas_store.as_ref(), &action_digest)
            .await
            .err_tip(|| "During start_action")?;
        let action_info =
            ActionInfo::try_from_action_and_execute_request_with_salt(execute_request, action, start_execute.salt)
                .err_tip(|| "Could not create ActionInfo in start_action()")?;
        {
            let mut running_actions = self.running_actions.lock().await;
            running_actions.insert(
                action_info.unique_qualifier.get_hash(),
                RunningAction::new(action_info, start_execute.salt),
            );
        }
        Ok(ExecuteFinishedResult::default())
    }
}
