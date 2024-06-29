// Copyright 2024 The NativeLink Authors. All rights reserved.
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

use std::cmp;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

use async_lock::Mutex;
use async_trait::async_trait;
use futures::stream;
use hashbrown::{HashMap, HashSet};
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_util::action_messages::{
    ActionInfo, ActionInfoHashKey, ActionStage, ActionState, OperationId, WorkerId,
};
use tokio::sync::watch::error::SendError;
use tokio::sync::{watch, Notify};
use tracing::{event, Level};

use crate::operation_state_manager::{
    ActionStateResult, ActionStateResultStream, ClientStateManager, MatchingEngineStateManager,
    OperationFilter, WorkerStateManager,
};
use crate::scheduler_state::awaited_action::AwaitedAction;
use crate::scheduler_state::client_action_state_result::ClientActionStateResult;
use crate::scheduler_state::completed_action::CompletedAction;
use crate::scheduler_state::matching_engine_action_state_result::MatchingEngineActionStateResult;
use crate::scheduler_state::metrics::Metrics;

#[repr(transparent)]
pub(crate) struct StateManager {
    pub inner: Mutex<StateManagerImpl>,
}

impl StateManager {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        queued_actions_set: HashSet<Arc<ActionInfo>>,
        queued_actions: BTreeMap<Arc<ActionInfo>, AwaitedAction>,
        active_actions: HashMap<Arc<ActionInfo>, AwaitedAction>,
        recently_completed_actions: HashSet<CompletedAction>,
        metrics: Arc<Metrics>,
        tasks_change_notify: Arc<Notify>,
    ) -> Self {
        Self {
            inner: Mutex::new(StateManagerImpl {
                queued_actions_set,
                queued_actions,
                active_actions,
                recently_completed_actions,
                metrics,
                tasks_change_notify,
            }),
        }
    }
}

/// StateManager is responsible for maintaining the state of the scheduler. Scheduler state
/// includes the actions that are queued, active, and recently completed. It also includes the
/// workers that are available to execute actions based on allocation strategy.
pub(crate) struct StateManagerImpl {
    // TODO(adams): Move `queued_actions_set` and `queued_actions` into a single struct that
    //  provides a unified interface for interacting with the two containers.

    // Important: `queued_actions_set` and `queued_actions` are two containers that provide
    // different search and sort capabilities. We are using the two different containers to
    // optimize different use cases. `HashSet` is used to look up actions in O(1) time. The
    // `BTreeMap` is used to sort actions in O(log n) time based on priority and timestamp.
    // These two fields must be kept in-sync, so if you modify one, you likely need to modify the
    // other.
    /// A `HashSet` of all actions that are queued. A hashset is used to find actions that are queued
    /// in O(1) time. This set allows us to find and join on new actions onto already existing
    /// (or queued) actions where insert timestamp of queued actions is not known. Using an
    /// additional `HashSet` will prevent us from having to iterate the `BTreeMap` to find actions.
    ///
    /// Important: `queued_actions_set` and `queued_actions` must be kept in sync.
    pub(crate) queued_actions_set: HashSet<Arc<ActionInfo>>,

    /// A BTreeMap of sorted actions that are primarily based on priority and insert timestamp.
    /// `ActionInfo` implements `Ord` that defines the `cmp` function for order. Using a BTreeMap
    /// gives us to sorted actions that are queued in O(log n) time.
    ///
    /// Important: `queued_actions_set` and `queued_actions` must be kept in sync.
    pub(crate) queued_actions: BTreeMap<Arc<ActionInfo>, AwaitedAction>,

    /// A map of all actions that are active. A hashmap is used to find actions that are active in
    /// O(1) time. The key is the `ActionInfo` struct. The value is the `AwaitedAction` struct.
    pub(crate) active_actions: HashMap<Arc<ActionInfo>, AwaitedAction>,

    /// These actions completed recently but had no listener, they might have
    /// completed while the caller was thinking about calling wait_execution, so
    /// keep their completion state around for a while to send back.
    /// TODO(#192) Revisit if this is the best way to handle recently completed actions.
    pub(crate) recently_completed_actions: HashSet<CompletedAction>,

    pub(crate) metrics: Arc<Metrics>,

    /// Notify task<->worker matching engine that work needs to be done.
    pub(crate) tasks_change_notify: Arc<Notify>,
}

/// Modifies the `stage` of `current_state` within `AwaitedAction`. Sends notification channel
/// the new state.
///
///
/// # Discussion
///
/// The use of `Arc::make_mut` is potentially dangerous because it clones the data and
/// invalidates all weak references to it. However, in this context, it is considered
/// safe because the data is going to be re-sent back out. The primary reason for using
/// `Arc` is to reduce the number of copies, not to enforce read-only access. This approach
/// ensures that all downstream components receive the same pointer. If an update occurs
/// while another thread is operating on the data, it is acceptable, since the other thread
/// will receive another update with the new version.
///
pub(crate) fn mutate_stage(
    awaited_action: &mut AwaitedAction,
    action_stage: ActionStage,
) -> Result<(), SendError<Arc<ActionState>>> {
    Arc::make_mut(&mut awaited_action.current_state).stage = action_stage;
    awaited_action
        .notify_channel
        .send(awaited_action.current_state.clone())
}

/// Updates the `last_error` field of the provided `AwaitedAction` and sends the current state
/// to the notify channel.
///
fn mutate_last_error(
    awaited_action: &mut AwaitedAction,
    last_error: Error,
) -> Result<(), SendError<Arc<ActionState>>> {
    awaited_action.last_error = Some(last_error);
    awaited_action
        .notify_channel
        .send(awaited_action.current_state.clone())
}

/// Sets the action stage for the given `AwaitedAction` based on the result of the provided
/// `action_stage`. If the `action_stage` is an error, it updates the `last_error` field
/// and logs a warning.
///
/// # Note
///
/// Intended utility function for matching engine.
///
/// # Errors
///
/// This function will return an error if updating the state of the `awaited_action` fails.
///
async fn worker_set_action_stage(
    awaited_action: &mut AwaitedAction,
    action_stage: Result<ActionStage, Error>,
    worker_id: WorkerId,
) -> Result<(), SendError<Arc<ActionState>>> {
    match action_stage {
        Ok(action_stage) => mutate_stage(awaited_action, action_stage),
        Err(e) => {
            event!(
                Level::WARN,
                ?worker_id,
                "Action stage setting error during do_try_match()"
            );
            mutate_last_error(awaited_action, e)
        }
    }
}

/// Modifies the `priority` of `action_info` within `ActionInfo`.
///
fn mutate_priority(action_info: &mut Arc<ActionInfo>, priority: i32) {
    Arc::make_mut(action_info).priority = priority;
}

impl StateManagerImpl {
    /// Marks the specified action as active, assigns it to the given worker, and updates the
    /// action stage. This function removes the action from the queue, updates the action's state
    /// or error, and inserts it into the set of active actions.
    ///
    /// # Note
    ///
    /// Intended utility function for matching engine.
    ///
    /// # Errors
    ///
    /// This function will return an error if it fails to update the action's state or if any other
    /// error occurs during the process.
    ///
    async fn worker_set_as_active(
        &mut self,
        action_info: Arc<ActionInfo>,
        worker_id: WorkerId,
        action_stage: Result<ActionStage, Error>,
    ) -> Result<(), Error> {
        if let Some((action_info, mut awaited_action)) =
            self.queued_actions.remove_entry(action_info.as_ref())
        {
            assert!(
                self.queued_actions_set.remove(&action_info),
                "queued_actions_set should always have same keys as queued_actions"
            );

            awaited_action.worker_id = Some(worker_id);

            let send_result =
                worker_set_action_stage(&mut awaited_action, action_stage, worker_id).await;

            if send_result.is_err() {
                event!(
                    Level::WARN,
                    ?action_info,
                    ?worker_id,
                    "Action has no more listeners during do_try_match()"
                );
            }

            awaited_action.attempts += 1;
            self.active_actions.insert(action_info, awaited_action);
            Ok(())
        } else {
            Err(make_err!(
                Code::Internal,
                "Action not found in queued_actions_set or queued_actions"
            ))
        }
    }

    fn update_action_with_internal_error(
        &mut self,
        worker_id: &WorkerId,
        action_info_hash_key: ActionInfoHashKey,
        err: Error,
    ) {
        self.metrics.update_action_with_internal_error.inc();
        let Some((action_info, mut running_action)) =
            self.active_actions.remove_entry(&action_info_hash_key)
        else {
            self.metrics
                .update_action_with_internal_error_no_action
                .inc();
            event!(
                Level::ERROR,
                ?action_info_hash_key,
                ?worker_id,
                "Could not find action info in active actions"
            );
            return;
        };

        let due_to_backpressure = err.code == Code::ResourceExhausted;
        // Don't count a backpressure failure as an attempt for an action.
        if due_to_backpressure {
            self.metrics
                .update_action_with_internal_error_backpressure
                .inc();
            running_action.attempts -= 1;
        }
        let Some(running_action_worker_id) = running_action.worker_id else {
            event!(
                Level::ERROR,
                ?action_info_hash_key,
                ?worker_id,
                "Got a result from a worker that should not be running the action, Removing worker. Expected action to be unassigned got worker",
            );
            return;
        };
        if running_action_worker_id == *worker_id {
            // Don't set the error on an action that's running somewhere else.
            event!(
                Level::WARN,
                ?action_info_hash_key,
                ?worker_id,
                ?running_action_worker_id,
                ?err,
                "Internal worker error",
            );
            running_action.last_error = Some(err.clone());
        } else {
            self.metrics
                .update_action_with_internal_error_from_wrong_worker
                .inc();
        }

        // Now put it back. retry_action() needs it to be there to send errors properly.
        self.active_actions
            .insert(action_info.clone(), running_action);

        self.tasks_change_notify.notify_one();
    }
}

#[async_trait]
impl ClientStateManager for StateManager {
    async fn add_action(
        &self,
        action_info: ActionInfo,
    ) -> Result<Arc<dyn ActionStateResult>, Error> {
        let mut inner = self.inner.lock().await;
        // Check to see if the action is running, if it is and cacheable, merge the actions.
        if let Some(running_action) = inner.active_actions.get_mut(&action_info) {
            let subscription = running_action.notify_channel.subscribe();
            inner.metrics.add_action_joined_running_action.inc();
            inner.tasks_change_notify.notify_one();
            return Ok(Arc::new(ClientActionStateResult::new(subscription)));
        }

        // Check to see if the action is queued, if it is and cacheable, merge the actions.
        if let Some(mut arc_action_info) = inner.queued_actions_set.take(&action_info) {
            let (original_action_info, queued_action) = inner
                .queued_actions
                .remove_entry(&arc_action_info)
                .err_tip(|| "Internal error queued_actions and queued_actions_set should match")?;
            inner.metrics.add_action_joined_queued_action.inc();

            let new_priority = cmp::max(original_action_info.priority, action_info.priority);
            drop(original_action_info); // This increases the chance Arc::make_mut won't copy.

            // In the event our task is higher priority than the one already scheduled, increase
            // the priority of the scheduled one.
            mutate_priority(&mut arc_action_info, new_priority);

            let result = Arc::new(ClientActionStateResult::new(
                queued_action.notify_channel.subscribe(),
            ));

            // Even if we fail to send our action to the client, we need to add this action back to the
            // queue because it was remove earlier.
            inner
                .queued_actions
                .insert(arc_action_info.clone(), queued_action);
            inner.queued_actions_set.insert(arc_action_info);
            inner.tasks_change_notify.notify_one();
            return Ok(result);
        }

        inner.metrics.add_action_new_action_created.inc();
        // Action needs to be added to queue or is not cacheable.
        let action_info = Arc::new(action_info);

        let operation_id = OperationId::new(action_info.unique_qualifier.clone());

        let current_state = Arc::new(ActionState {
            stage: ActionStage::Queued,
            id: operation_id,
        });

        let (tx, rx) = watch::channel(current_state.clone());

        inner.queued_actions_set.insert(action_info.clone());
        inner.queued_actions.insert(
            action_info.clone(),
            AwaitedAction {
                action_info,
                current_state,
                notify_channel: tx,
                attempts: 0,
                last_error: None,
                worker_id: None,
            },
        );
        inner.tasks_change_notify.notify_one();
        return Ok(Arc::new(ClientActionStateResult::new(rx)));
    }

    async fn filter_operations(
        &self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream, Error> {
        let inner = self.inner.lock().await;
        // TODO(adams): Build out a proper filter for other fields for state, at the moment
        //  this only supports the unique qualifier.
        let unique_qualifier = &filter
            .unique_qualifier
            .err_tip(|| "No unique qualifier provided")?;
        let maybe_awaited_action = inner
            .queued_actions_set
            .get(unique_qualifier)
            .and_then(|action_info| inner.queued_actions.get(action_info))
            .or_else(|| inner.active_actions.get(unique_qualifier));

        let Some(awaited_action) = maybe_awaited_action else {
            return Ok(Box::pin(stream::empty()));
        };

        let rx = awaited_action.notify_channel.subscribe();
        let action_result: [Arc<dyn ActionStateResult>; 1] =
            [Arc::new(ClientActionStateResult::new(rx))];
        Ok(Box::pin(stream::iter(action_result)))
    }
}

#[async_trait]
impl WorkerStateManager for StateManager {
    async fn update_operation(
        &self,
        operation_id: OperationId,
        worker_id: WorkerId,
        action_stage: Result<ActionStage, Error>,
    ) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;
        match action_stage {
            Ok(action_stage) => {
                let action_info_hash_key = operation_id.unique_qualifier;
                if !action_stage.has_action_result() {
                    inner.metrics.update_action_missing_action_result.inc();
                    event!(
                        Level::ERROR,
                        ?action_info_hash_key,
                        ?worker_id,
                        ?action_stage,
                        "Worker sent error while updating action. Removing worker"
                    );
                    return Err(make_err!(
                        Code::Internal,
                        "Worker '{worker_id}' set the action_stage of running action {action_info_hash_key:?} to {action_stage:?}. Removing worker.",
                    ));
                }

                let (action_info, mut running_action) = inner
                    .active_actions
                    .remove_entry(&action_info_hash_key)
                    .err_tip(|| {
                        format!("Could not find action info in active actions : {action_info_hash_key:?}")
                    })?;

                if running_action.worker_id != Some(worker_id) {
                    inner.metrics.update_action_from_wrong_worker.inc();
                    let err = match running_action.worker_id {
                        Some(running_action_worker_id) => make_err!(
                            Code::Internal,
                            "Got a result from a worker that should not be running the action, Removing worker. Expected worker {running_action_worker_id} got worker {worker_id}",
                        ),
                        None => make_err!(
                            Code::Internal,
                            "Got a result from a worker that should not be running the action, Removing worker. Expected action to be unassigned got worker {worker_id}",
                        ),
                    };
                    event!(
                        Level::ERROR,
                        ?action_info,
                        ?worker_id,
                        ?running_action.worker_id,
                        ?err,
                        "Got a result from a worker that should not be running the action, Removing worker"
                    );
                    // First put it back in our active_actions or we will drop the task.
                    inner.active_actions.insert(action_info, running_action);
                    return Err(err);
                }

                let send_result = mutate_stage(&mut running_action, action_stage);

                if !running_action.current_state.stage.is_finished() {
                    if send_result.is_err() {
                        inner.metrics.update_action_no_more_listeners.inc();
                        event!(
                            Level::WARN,
                            ?action_info,
                            ?worker_id,
                            "Action has no more listeners during update_action()"
                        );
                    }
                    // If the operation is not finished it means the worker is still working on it, so put it
                    // back or else we will lose track of the task.
                    inner.active_actions.insert(action_info, running_action);

                    inner.tasks_change_notify.notify_one();
                    return Ok(());
                }

                // Keep in case this is asked for soon.
                inner.recently_completed_actions.insert(CompletedAction {
                    completed_time: SystemTime::now(),
                    state: running_action.current_state,
                });

                inner.tasks_change_notify.notify_one();
                Ok(())
            }
            Err(e) => {
                inner.update_action_with_internal_error(
                    &worker_id,
                    operation_id.unique_qualifier,
                    e.clone(),
                );
                return Err(e);
            }
        }
    }
}

#[async_trait]
impl MatchingEngineStateManager for StateManager {
    async fn filter_operations(
        &self,
        _filter: OperationFilter, // TODO(adam): reference filter
    ) -> Result<ActionStateResultStream, Error> {
        let inner = self.inner.lock().await;
        // TODO(adams): use OperationFilter vs directly encoding it.
        let action_infos =
            inner
                .queued_actions
                .iter()
                .rev()
                .map(|(action_info, awaited_action)| {
                    let cloned_action_info = action_info.clone();
                    Arc::new(MatchingEngineActionStateResult::new(
                        cloned_action_info,
                        awaited_action.notify_channel.subscribe(),
                    )) as Arc<dyn ActionStateResult>
                });

        let action_infos: Vec<Arc<dyn ActionStateResult>> = action_infos.collect();
        Ok(Box::pin(stream::iter(action_infos)))
    }

    async fn update_operation(
        &self,
        operation_id: OperationId,
        worker_id: Option<WorkerId>,
        action_stage: Result<ActionStage, Error>,
    ) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;
        if let Some(action_info) = inner.queued_actions_set.get(&operation_id.unique_qualifier) {
            if let Some(worker_id) = worker_id {
                let action_info = action_info.clone();
                inner
                    .worker_set_as_active(action_info, worker_id, action_stage)
                    .await?;
            } else {
                event!(
                    Level::WARN,
                    ?operation_id,
                    ?worker_id,
                    "No worker found in do_try_match()"
                );
            }
        } else {
            event!(
                Level::WARN,
                ?operation_id,
                ?worker_id,
                "No action info found in do_try_match()"
            );
        }

        Ok(())
    }

    async fn remove_operation(&self, _operation_id: OperationId) -> Result<(), Error> {
        todo!()
    }
}
