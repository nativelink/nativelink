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

use std::borrow::Borrow;
use std::cmp;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::iter::Map;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use async_trait::async_trait;
use futures::Future;
use hashbrown::hash_map::Iter;
use hashbrown::{HashMap, HashSet};
use lru::LruCache;
use nativelink_config::schedulers::WorkerAllocationStrategy;
use nativelink_error::{error_if, make_err, make_input_err, Code, Error, ResultExt};
use nativelink_proto::google::longrunning::Operation;
use nativelink_util::action_messages::{
    ActionInfo, ActionInfoHashKey, ActionResult, ActionStage, ActionState, ExecutionMetadata,
};
use nativelink_util::metrics_utils::{
    AsyncCounterWrapper, Collector, CollectorState, CounterWithTime, FuncCounterWrapper,
    MetricsComponent, Registry,
};
use nativelink_util::platform_properties::PlatformPropertyValue;
use nativelink_util::spawn;
use nativelink_util::task::JoinHandleDropGuard;
use parking_lot::{Mutex, MutexGuard, RawMutex};
use tokio::sync::{watch, Notify};
use tokio::time::Duration;
use tracing::{event, Level};

use crate::action_scheduler::ActionScheduler;
use crate::operations::Operations;
use crate::platform_property_manager::PlatformPropertyManager;
use crate::worker::{Worker, WorkerId, WorkerTimestamp, WorkerUpdate};
use crate::worker_scheduler::WorkerScheduler;

/// Default timeout for workers in seconds.
/// If this changes, remember to change the documentation in the config.
const DEFAULT_WORKER_TIMEOUT_S: u64 = 5;

/// Default timeout for recently completed actions in seconds.
/// If this changes, remember to change the documentation in the config.
const DEFAULT_RETAIN_COMPLETED_FOR_S: u64 = 60;

/// Default times a job can retry before failing.
/// If this changes, remember to change the documentation in the config.
const DEFAULT_MAX_JOB_RETRIES: usize = 3;

/// An action that is being awaited on and last known state.
#[derive(Clone, Debug)]
struct AwaitedAction {
    action_info: Arc<ActionInfo>,
    current_state: Arc<ActionState>,
    notify_channel: watch::Sender<Arc<ActionState>>,

    /// Number of attempts the job has been tried.
    attempts: usize,
    /// Possible last error set by the worker. If empty and attempts is set, it may be due to
    /// something like a worker timeout.
    last_error: Option<Error>,

    /// Worker that is currently running this action, None if unassigned.
    worker_id: Option<WorkerId>,
}

struct Workers {
    workers: LruCache<WorkerId, Worker>,
    /// The allocation strategy for workers.
    allocation_strategy: WorkerAllocationStrategy,
}

impl Workers {
    fn new(allocation_strategy: WorkerAllocationStrategy) -> Self {
        Self {
            workers: LruCache::unbounded(),
            allocation_strategy,
        }
    }

    /// Refreshes the lifetime of the worker with the given timestamp.
    fn refresh_lifetime(
        &mut self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        let worker = self.workers.get_mut(worker_id).ok_or_else(|| {
            make_input_err!(
                "Worker not found in worker map in refresh_lifetime() {}",
                worker_id
            )
        })?;
        error_if!(
            worker.last_update_timestamp > timestamp,
            "Worker already had a timestamp of {}, but tried to update it with {}",
            worker.last_update_timestamp,
            timestamp
        );
        worker.last_update_timestamp = timestamp;
        Ok(())
    }

    /// Adds a worker to the pool.
    /// Note: This function will not do any task matching.
    fn add_worker(&mut self, worker: Worker) -> Result<(), Error> {
        let worker_id = worker.id;
        self.workers.put(worker_id, worker);

        // Worker is not cloneable, and we do not want to send the initial connection results until
        // we have added it to the map, or we might get some strange race conditions due to the way
        // the multi-threaded runtime works.
        let worker = self.workers.peek_mut(&worker_id).unwrap();
        let res = worker
            .send_initial_connection_result()
            .err_tip(|| "Failed to send initial connection result to worker");
        if let Err(err) = &res {
            event!(
                Level::ERROR,
                ?worker_id,
                ?err,
                "Worker connection appears to have been closed while adding to pool"
            );
        }
        res
    }

    /// Removes worker from pool.
    /// Note: The caller is responsible for any rescheduling of any tasks that might be
    /// running.
    fn remove_worker(&mut self, worker_id: &WorkerId) -> Option<Worker> {
        self.workers.pop(worker_id)
    }

    /// Attempts to find a worker that is capable of running this action.
    // TODO(blaise.bruer) This algorithm is not very efficient. Simple testing using a tree-like
    // structure showed worse performance on a 10_000 worker * 7 properties * 1000 queued tasks
    // simulation of worst cases in a single threaded environment.
    fn find_worker_for_action_mut<'a>(
        &'a mut self,
        awaited_action: &AwaitedAction,
    ) -> Option<&'a mut Worker> {
        assert!(matches!(
            awaited_action.current_state.stage,
            ActionStage::Queued
        ));
        let action_properties = &awaited_action.action_info.platform_properties;
        let mut workers_iter = self.workers.iter_mut();
        let workers_iter = match self.allocation_strategy {
            // Use rfind to get the least recently used that satisfies the properties.
            WorkerAllocationStrategy::least_recently_used => workers_iter.rfind(|(_, w)| {
                w.can_accept_work() && action_properties.is_satisfied_by(&w.platform_properties)
            }),
            // Use find to get the most recently used that satisfies the properties.
            WorkerAllocationStrategy::most_recently_used => workers_iter.find(|(_, w)| {
                w.can_accept_work() && action_properties.is_satisfied_by(&w.platform_properties)
            }),
        };
        let worker_id = workers_iter.map(|(_, w)| &w.id);
        // We need to "touch" the worker to ensure it gets re-ordered in the LRUCache, since it was selected.
        if let Some(&worker_id) = worker_id {
            self.workers.get_mut(&worker_id)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
struct CompletedAction {
    completed_time: SystemTime,
    state: Arc<ActionState>,
}

impl Hash for CompletedAction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        ActionInfoHashKey::hash(&self.state.unique_qualifier, state);
    }
}

impl PartialEq for CompletedAction {
    fn eq(&self, other: &Self) -> bool {
        ActionInfoHashKey::eq(&self.state.unique_qualifier, &other.state.unique_qualifier)
    }
}

impl Eq for CompletedAction {}

impl Borrow<ActionInfoHashKey> for CompletedAction {
    #[inline]
    fn borrow(&self) -> &ActionInfoHashKey {
        &self.state.unique_qualifier
    }
}

struct SimpleSchedulerImpl {
    // BTreeMap uses `cmp` to do it's comparisons, this is a problem because we want to sort our
    // actions based on priority and insert timestamp but also want to find and join new actions
    // onto already executing (or queued) actions. We don't know the insert timestamp of queued
    // actions, so we won't be able to find it in a BTreeMap without iterating the entire map. To
    // get around this issue, we use two containers, one that will search using `Eq` which will
    // only match on the `unique_qualifier` field, which ignores fields that would prevent
    // multiplexing, and another which uses `Ord` for sorting.
    //
    // Important: These two fields must be kept in-sync, so if you modify one, you likely need to
    // modify the other.
    queued_actions_set: HashSet<Arc<ActionInfo>>,
    queued_actions: BTreeMap<Arc<ActionInfo>, AwaitedAction>,
    workers: Workers,
    active_actions: HashMap<Arc<ActionInfo>, AwaitedAction>,
    // These actions completed recently but had no listener, they might have
    // completed while the caller was thinking about calling wait_execution, so
    // keep their completion state around for a while to send back.
    // TODO(#192) Revisit if this is the best way to handle recently completed actions.
    recently_completed_actions: HashSet<CompletedAction>,
    /// The duration that actions are kept in recently_completed_actions for.
    retain_completed_for: Duration,
    /// Timeout of how long to evict workers if no response in this given amount of time in seconds.
    worker_timeout_s: u64,
    /// Default times a job can retry before failing.
    max_job_retries: usize,
    /// Notify task<->worker matching engine that work needs to be done.
    tasks_or_workers_change_notify: Arc<Notify>,
    metrics: Arc<Metrics>,
}

impl SimpleSchedulerImpl {
    fn subscribe_to_channel(awaited_action: &AwaitedAction) -> watch::Receiver<Arc<ActionState>> {
        let rx = awaited_action.notify_channel.subscribe();
        // TODO: Fix this when fixed upstream tokio-rs/tokio#5871
        awaited_action
            .notify_channel
            .send(awaited_action.current_state.clone())
            .unwrap();
        rx
    }

    /// Attempts to find a worker to execute an action and begins executing it.
    /// If an action is already running that is cacheable it may merge this action
    /// with the results and state changes of the already running action.
    /// If the task cannot be executed immediately it will be queued for execution
    /// based on priority and other metrics.
    /// All further updates to the action will be provided through `listener`.
    fn add_action(
        &mut self,
        action_info: ActionInfo,
    ) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        // Check to see if the action is running, if it is and cacheable, merge the actions.
        if let Some(running_action) = self.active_actions.get_mut(&action_info) {
            self.metrics.add_action_joined_running_action.inc();
            return Ok(Self::subscribe_to_channel(running_action));
        }

        // Check to see if the action is queued, if it is and cacheable, merge the actions.
        if let Some(mut arc_action_info) = self.queued_actions_set.take(&action_info) {
            let (original_action_info, queued_action) = self
                .queued_actions
                .remove_entry(&arc_action_info)
                .err_tip(|| "Internal error queued_actions and queued_actions_set should match")?;
            self.metrics.add_action_joined_queued_action.inc();

            let new_priority = cmp::max(original_action_info.priority, action_info.priority);
            drop(original_action_info); // This increases the chance Arc::make_mut won't copy.

            // In the event our task is higher priority than the one already scheduled, increase
            // the priority of the scheduled one.
            Arc::make_mut(&mut arc_action_info).priority = new_priority;

            let rx = queued_action.notify_channel.subscribe();
            // TODO: Fix this when fixed upstream tokio-rs/tokio#5871
            let _ = queued_action
                .notify_channel
                .send(queued_action.current_state.clone());

            // Even if we fail to send our action to the client, we need to add this action back to the
            // queue because it was remove earlier.
            self.queued_actions
                .insert(arc_action_info.clone(), queued_action);
            self.queued_actions_set.insert(arc_action_info);
            return Ok(rx);
        }

        self.metrics.add_action_new_action_created.inc();
        // Action needs to be added to queue or is not cacheable.
        let action_info = Arc::new(action_info);

        let current_state = Arc::new(ActionState {
            unique_qualifier: action_info.unique_qualifier.clone(),
            stage: ActionStage::Queued,
        });

        let (tx, rx) = watch::channel(current_state.clone());
        self.queued_actions_set.insert(action_info.clone());
        self.queued_actions.insert(
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

        self.tasks_or_workers_change_notify.notify_one();
        Ok(rx)
    }

    fn clean_recently_completed_actions(&mut self) {
        let expiry_time = SystemTime::now()
            .checked_sub(self.retain_completed_for)
            .unwrap();
        self.recently_completed_actions
            .retain(|action| action.completed_time > expiry_time);
    }

    fn find_recently_completed_action(
        &self,
        unique_qualifier: &ActionInfoHashKey,
    ) -> Option<watch::Receiver<Arc<ActionState>>> {
        self.recently_completed_actions
            .get(unique_qualifier)
            .map(|action| watch::channel(action.state.clone()).1)
    }

    fn find_existing_action(
        &self,
        unique_qualifier: &ActionInfoHashKey,
    ) -> Option<watch::Receiver<Arc<ActionState>>> {
        self.queued_actions_set
            .get(unique_qualifier)
            .and_then(|action_info| self.queued_actions.get(action_info))
            .or_else(|| self.active_actions.get(unique_qualifier))
            .map(Self::subscribe_to_channel)
    }

    fn retry_action(&mut self, action_info: &Arc<ActionInfo>, worker_id: &WorkerId, err: Error) {
        match self.active_actions.remove(action_info) {
            Some(running_action) => {
                let mut awaited_action = running_action;
                let send_result = if awaited_action.attempts >= self.max_job_retries {
                    self.metrics.retry_action_max_attempts_reached.inc();
                    Arc::make_mut(&mut awaited_action.current_state).stage = ActionStage::Completed(ActionResult {
                        execution_metadata: ExecutionMetadata {
                            worker: format!("{worker_id}"),
                            ..ExecutionMetadata::default()
                        },
                        error: Some(err.merge(make_err!(
                            Code::Internal,
                            "Job cancelled because it attempted to execute too many times and failed"
                        ))),
                        ..ActionResult::default()
                    });
                    awaited_action
                        .notify_channel
                        .send(awaited_action.current_state.clone())
                    // Do not put the action back in the queue here, as this action attempted to run too many
                    // times.
                } else {
                    self.metrics.retry_action.inc();
                    Arc::make_mut(&mut awaited_action.current_state).stage = ActionStage::Queued;
                    let send_result = awaited_action
                        .notify_channel
                        .send(awaited_action.current_state.clone());
                    self.queued_actions_set.insert(action_info.clone());
                    self.queued_actions
                        .insert(action_info.clone(), awaited_action);
                    send_result
                };

                if send_result.is_err() {
                    self.metrics.retry_action_no_more_listeners.inc();
                    // Don't remove this task, instead we keep them around for a bit just in case
                    // the client disconnected and will reconnect and ask for same job to be executed
                    // again.
                    event!(
                        Level::WARN,
                        ?action_info,
                        ?worker_id,
                        "Action has no more listeners during evict_worker()"
                    );
                }
            }
            None => {
                self.metrics.retry_action_but_action_missing.inc();
                event!(
                    Level::ERROR,
                    ?action_info,
                    ?worker_id,
                    "Worker stated it was running an action, but it was not in the active_actions"
                );
            }
        }
    }

    /// Evicts the worker from the pool and puts items back into the queue if anything was being executed on it.
    fn immediate_evict_worker(&mut self, worker_id: &WorkerId, err: Error) {
        if let Some(mut worker) = self.workers.remove_worker(worker_id) {
            self.metrics.workers_evicted.inc();
            // We don't care if we fail to send message to worker, this is only a best attempt.
            let _ = worker.notify_update(WorkerUpdate::Disconnect);
            // We create a temporary Vec to avoid doubt about a possible code
            // path touching the worker.running_action_infos elsewhere.
            for action_info in worker.running_action_infos.drain() {
                self.metrics.workers_evicted_with_running_action.inc();
                self.retry_action(&action_info, worker_id, err.clone());
            }
        }
        // Note: Calling this many time is very cheap, it'll only trigger `do_try_match` once.
        self.tasks_or_workers_change_notify.notify_one();
    }

    /// Sets if the worker is draining or not.
    fn set_drain_worker(&mut self, worker_id: WorkerId, is_draining: bool) -> Result<(), Error> {
        let worker = self
            .workers
            .workers
            .get_mut(&worker_id)
            .err_tip(|| format!("Worker {worker_id} doesn't exist in the pool"))?;
        self.metrics.workers_drained.inc();
        worker.is_draining = is_draining;
        self.tasks_or_workers_change_notify.notify_one();
        Ok(())
    }

    // TODO(blaise.bruer) This is an O(n*m) (aka n^2) algorithm. In theory we can create a map
    // of capabilities of each worker and then try and match the actions to the worker using
    // the map lookup (ie. map reduce).
    fn do_try_match(&mut self) {
        // TODO(blaise.bruer) This is a bit difficult because of how rust's borrow checker gets in
        // the way. We need to conditionally remove items from the `queued_action`. Rust is working
        // to add `drain_filter`, which would in theory solve this problem, but because we need
        // to iterate the items in reverse it becomes more difficult (and it is currently an
        // unstable feature [see: https://github.com/rust-lang/rust/issues/70530]).
        let action_infos: Vec<Arc<ActionInfo>> =
            self.queued_actions.keys().rev().cloned().collect();
        for action_info in action_infos {
            let Some(awaited_action) = self.queued_actions.get(action_info.as_ref()) else {
                event!(
                    Level::ERROR,
                    ?action_info,
                    "queued_actions out of sync with itself"
                );
                continue;
            };
            let Some(worker) = self.workers.find_worker_for_action_mut(awaited_action) else {
                // No worker found, check the next action to see if there's a
                // matching one for that.
                continue;
            };
            let worker_id = worker.id;

            // Try to notify our worker of the new action to run, if it fails remove the worker from the
            // pool and try to find another worker.
            let notify_worker_result =
                worker.notify_update(WorkerUpdate::RunAction(action_info.clone()));
            if notify_worker_result.is_err() {
                // Remove worker, as it is no longer receiving messages and let it try to find another worker.
                event!(
                    Level::WARN,
                    ?worker_id,
                    ?action_info,
                    "Worker command failed, removing worker",
                );
                self.immediate_evict_worker(
                    &worker_id,
                    make_err!(
                        Code::Internal,
                        "Worker command failed, removing worker {}",
                        worker_id
                    ),
                );
                return;
            }

            // At this point everything looks good, so remove it from the queue and add it to active actions.
            let (action_info, mut awaited_action) = self
                .queued_actions
                .remove_entry(action_info.as_ref())
                .unwrap();
            assert!(
                self.queued_actions_set.remove(&action_info),
                "queued_actions_set should always have same keys as queued_actions"
            );
            Arc::make_mut(&mut awaited_action.current_state).stage = ActionStage::Executing;
            awaited_action.worker_id = Some(worker_id);
            let send_result = awaited_action
                .notify_channel
                .send(awaited_action.current_state.clone());
            if send_result.is_err() {
                // Don't remove this task, instead we keep them around for a bit just in case
                // the client disconnected and will reconnect and ask for same job to be executed
                // again.
                event!(
                    Level::WARN,
                    ?action_info,
                    ?worker_id,
                    "Action has no more listeners during do_try_match()"
                );
            }
            awaited_action.attempts += 1;
            self.active_actions.insert(action_info, awaited_action);
        }
    }

    fn update_action_with_internal_error(
        &mut self,
        worker_id: &WorkerId,
        action_info_hash_key: &ActionInfoHashKey,
        err: Error,
    ) {
        self.metrics.update_action_with_internal_error.inc();
        let Some((action_info, mut running_action)) =
            self.active_actions.remove_entry(action_info_hash_key)
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

        // Clear this action from the current worker.
        if let Some(worker) = self.workers.workers.get_mut(worker_id) {
            let was_paused = !worker.can_accept_work();
            // This unpauses, but since we're completing with an error, don't
            // unpause unless all actions have completed.
            worker.complete_action(&action_info);
            // Only pause if there's an action still waiting that will unpause.
            if (was_paused || due_to_backpressure) && worker.has_actions() {
                worker.is_paused = true;
            }
        }

        // Re-queue the action or fail on max attempts.
        self.retry_action(&action_info, worker_id, err);
        self.tasks_or_workers_change_notify.notify_one();
    }

    fn update_action(
        &mut self,
        worker_id: &WorkerId,
        action_info_hash_key: &ActionInfoHashKey,
        action_stage: ActionStage,
    ) -> Result<(), Error> {
        if !action_stage.has_action_result() {
            self.metrics.update_action_missing_action_result.inc();
            event!(
                Level::ERROR,
                ?action_info_hash_key,
                ?worker_id,
                ?action_stage,
                "Worker sent error while updating action. Removing worker"
            );
            let err = make_err!(
                Code::Internal,
                "Worker '{worker_id}' set the action_stage of running action {action_info_hash_key:?} to {action_stage:?}. Removing worker.",
            );
            self.immediate_evict_worker(worker_id, err.clone());
            return Err(err);
        }

        let (action_info, mut running_action) = self
            .active_actions
            .remove_entry(action_info_hash_key)
            .err_tip(|| {
                format!("Could not find action info in active actions : {action_info_hash_key:?}")
            })?;

        if running_action.worker_id != Some(*worker_id) {
            self.metrics.update_action_from_wrong_worker.inc();
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
            self.active_actions.insert(action_info, running_action);
            self.immediate_evict_worker(worker_id, err.clone());
            return Err(err);
        }

        Arc::make_mut(&mut running_action.current_state).stage = action_stage;

        let send_result = running_action
            .notify_channel
            .send(running_action.current_state.clone());

        if !running_action.current_state.stage.is_finished() {
            if send_result.is_err() {
                self.metrics.update_action_no_more_listeners.inc();
                event!(
                    Level::WARN,
                    ?action_info,
                    ?worker_id,
                    "Action has no more listeners during update_action()"
                );
            }
            // If the operation is not finished it means the worker is still working on it, so put it
            // back or else we will loose track of the task.
            self.active_actions.insert(action_info, running_action);
            return Ok(());
        }

        // Keep in case this is asked for soon.
        self.recently_completed_actions.insert(CompletedAction {
            completed_time: SystemTime::now(),
            state: running_action.current_state,
        });

        let worker = self.workers.workers.get_mut(worker_id).ok_or_else(|| {
            make_input_err!("WorkerId '{}' does not exist in workers map", worker_id)
        })?;
        worker.complete_action(&action_info);
        self.tasks_or_workers_change_notify.notify_one();

        Ok(())
    }
}

/// Engine used to manage the queued/running tasks and relationship with
/// the worker nodes. All state on how the workers and actions are interacting
/// should be held in this struct.
pub struct SimpleScheduler {
    inner: Arc<Mutex<SimpleSchedulerImpl>>,
    platform_property_manager: Arc<PlatformPropertyManager>,
    metrics: Arc<Metrics>,
    // Triggers `drop()`` call if scheduler is dropped.
    _task_worker_matching_future: JoinHandleDropGuard<()>,
}

impl SimpleScheduler {
    #[inline]
    #[must_use]
    pub fn new(scheduler_cfg: &nativelink_config::schedulers::SimpleScheduler) -> Self {
        Self::new_with_callback(scheduler_cfg, || {
            // The cost of running `do_try_match()` is very high, but constant
            // in relation to the number of changes that have happened. This means
            // that grabbing this lock to process `do_try_match()` should always
            // yield to any other tasks that might want the lock. The easiest and
            // most fair way to do this is to sleep for a small amount of time.
            // Using something like tokio::task::yield_now() does not yield as
            // aggresively as we'd like if new futures are scheduled within a future.
            tokio::time::sleep(Duration::from_millis(1))
        })
    }

    pub fn new_with_callback<
        Fut: Future<Output = ()> + Send,
        F: Fn() -> Fut + Send + Sync + 'static,
    >(
        scheduler_cfg: &nativelink_config::schedulers::SimpleScheduler,
        on_matching_engine_run: F,
    ) -> Self {
        let platform_property_manager = Arc::new(PlatformPropertyManager::new(
            scheduler_cfg
                .supported_platform_properties
                .clone()
                .unwrap_or_default(),
        ));

        let mut worker_timeout_s = scheduler_cfg.worker_timeout_s;
        if worker_timeout_s == 0 {
            worker_timeout_s = DEFAULT_WORKER_TIMEOUT_S;
        }

        let mut retain_completed_for_s = scheduler_cfg.retain_completed_for_s;
        if retain_completed_for_s == 0 {
            retain_completed_for_s = DEFAULT_RETAIN_COMPLETED_FOR_S;
        }

        let mut max_job_retries = scheduler_cfg.max_job_retries;
        if max_job_retries == 0 {
            max_job_retries = DEFAULT_MAX_JOB_RETRIES;
        }

        let tasks_or_workers_change_notify = Arc::new(Notify::new());

        let metrics = Arc::new(Metrics::default());
        let metrics_for_do_try_match = metrics.clone();
        let inner = Arc::new(Mutex::new(SimpleSchedulerImpl {
            queued_actions_set: HashSet::new(),
            queued_actions: BTreeMap::new(),
            workers: Workers::new(scheduler_cfg.allocation_strategy),
            active_actions: HashMap::new(),
            recently_completed_actions: HashSet::new(),
            retain_completed_for: Duration::new(retain_completed_for_s, 0),
            worker_timeout_s,
            max_job_retries,
            tasks_or_workers_change_notify: tasks_or_workers_change_notify.clone(),
            metrics: metrics.clone(),
        }));
        let weak_inner = Arc::downgrade(&inner);
        Self {
            inner,
            platform_property_manager,
            _task_worker_matching_future: spawn!(
                "simple_scheduler_task_worker_matching",
                async move {
                    // Break out of the loop only when the inner is dropped.
                    loop {
                        tasks_or_workers_change_notify.notified().await;
                        match weak_inner.upgrade() {
                            // Note: According to `parking_lot` documentation, the default
                            // `Mutex` implementation is eventual fairness, so we don't
                            // really need to worry about this thread taking the lock
                            // starving other threads too much.
                            Some(inner_mux) => {
                                let mut inner = inner_mux.lock();
                                let timer = metrics_for_do_try_match.do_try_match.begin_timer();
                                inner.do_try_match();
                                timer.measure();
                            }
                            // If the inner went away it means the scheduler is shutting
                            // down, so we need to resolve our future.
                            None => return,
                        };
                        on_matching_engine_run().await;
                    }
                    // Unreachable.
                }
            ),
            metrics,
        }
    }

    /// Checks to see if the worker exists in the worker pool. Should only be used in unit tests.
    #[must_use]
    pub fn contains_worker_for_test(&self, worker_id: &WorkerId) -> bool {
        let inner = self.get_inner_lock();
        inner.workers.workers.contains(worker_id)
    }

    /// Checks to see if the worker can accept work. Should only be used in unit tests.
    pub fn can_worker_accept_work_for_test(&self, worker_id: &WorkerId) -> Result<bool, Error> {
        let mut inner = self.get_inner_lock();
        let worker = inner.workers.workers.get_mut(worker_id).ok_or_else(|| {
            make_input_err!("WorkerId '{}' does not exist in workers map", worker_id)
        })?;
        Ok(worker.can_accept_work())
    }

    /// A unit test function used to send the keep alive message to the worker from the server.
    pub fn send_keep_alive_to_worker_for_test(&self, worker_id: &WorkerId) -> Result<(), Error> {
        let mut inner = self.get_inner_lock();
        let worker = inner.workers.workers.get_mut(worker_id).ok_or_else(|| {
            make_input_err!("WorkerId '{}' does not exist in workers map", worker_id)
        })?;
        worker.keep_alive()
    }

    fn get_inner_lock(&self) -> MutexGuard<'_, SimpleSchedulerImpl> {
        // We don't use one of the wrappers because we only want to capture the time spent,
        // nothing else beacuse this is a hot path.
        let start = Instant::now();
        let lock = self.inner.lock();
        self.metrics
            .lock_stall_time
            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.metrics
            .lock_stall_time_counter
            .fetch_add(1, Ordering::Relaxed);
        lock
    }
}

#[async_trait]
impl ActionScheduler for SimpleScheduler {
    async fn get_platform_property_manager(
        &self,
        _instance_name: &str,
    ) -> Result<Arc<PlatformPropertyManager>, Error> {
        Ok(self.platform_property_manager.clone())
    }

    async fn add_action(
        &self,
        action_info: ActionInfo,
    ) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        let mut inner = self.get_inner_lock();
        self.metrics
            .add_action
            .wrap(move || inner.add_action(action_info))
    }

    async fn find_existing_action(
        &self,
        unique_qualifier: &ActionInfoHashKey,
    ) -> Option<watch::Receiver<Arc<ActionState>>> {
        let inner = self.get_inner_lock();
        let result = inner
            .find_existing_action(unique_qualifier)
            .or_else(|| inner.find_recently_completed_action(unique_qualifier));
        if result.is_some() {
            self.metrics.existing_actions_found.inc();
        } else {
            self.metrics.existing_actions_not_found.inc();
        }
        result
    }

    async fn clean_recently_completed_actions(&self) {
        self.get_inner_lock().clean_recently_completed_actions();
        self.metrics.clean_recently_completed_actions.inc()
    }

    fn register_metrics(self: Arc<Self>, registry: &mut Registry) {
        registry.register_collector(Box::new(Collector::new(&self)));
    }
}

#[async_trait]
impl WorkerScheduler for SimpleScheduler {
    fn get_platform_property_manager(&self) -> &PlatformPropertyManager {
        self.platform_property_manager.as_ref()
    }

    async fn add_worker(&self, worker: Worker) -> Result<(), Error> {
        let worker_id = worker.id;
        let mut inner = self.get_inner_lock();
        self.metrics.add_worker.wrap(move || {
            let res = inner
                .workers
                .add_worker(worker)
                .err_tip(|| "Error while adding worker, removing from pool");
            if let Err(err) = &res {
                inner.immediate_evict_worker(&worker_id, err.clone());
            }
            inner.tasks_or_workers_change_notify.notify_one();
            res
        })
    }

    async fn update_action_with_internal_error(
        &self,
        worker_id: &WorkerId,
        action_info_hash_key: &ActionInfoHashKey,
        err: Error,
    ) {
        let mut inner = self.get_inner_lock();
        inner.update_action_with_internal_error(worker_id, action_info_hash_key, err);
    }

    async fn update_action(
        &self,
        worker_id: &WorkerId,
        action_info_hash_key: &ActionInfoHashKey,
        action_stage: ActionStage,
    ) -> Result<(), Error> {
        let mut inner = self.get_inner_lock();
        self.metrics
            .update_action
            .wrap(move || inner.update_action(worker_id, action_info_hash_key, action_stage))
    }

    async fn worker_keep_alive_received(
        &self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        let mut inner = self.get_inner_lock();
        inner
            .workers
            .refresh_lifetime(worker_id, timestamp)
            .err_tip(|| "Error refreshing lifetime in worker_keep_alive_received()")
    }

    async fn remove_worker(&self, worker_id: WorkerId) {
        let mut inner = self.get_inner_lock();
        inner.immediate_evict_worker(
            &worker_id,
            make_err!(Code::Internal, "Received request to remove worker"),
        );
    }

    async fn remove_timedout_workers(&self, now_timestamp: WorkerTimestamp) -> Result<(), Error> {
        let mut inner = self.get_inner_lock();
        self.metrics.remove_timedout_workers.wrap(move || {
            // Items should be sorted based on last_update_timestamp, so we don't need to iterate the entire
            // map most of the time.
            let worker_ids_to_remove: Vec<WorkerId> = inner
                .workers
                .workers
                .iter()
                .rev()
                .map_while(|(worker_id, worker)| {
                    if worker.last_update_timestamp <= now_timestamp - inner.worker_timeout_s {
                        Some(*worker_id)
                    } else {
                        None
                    }
                })
                .collect();
            for worker_id in &worker_ids_to_remove {
                event!(
                    Level::WARN,
                    ?worker_id,
                    "Worker timed out, removing from pool"
                );
                inner.immediate_evict_worker(
                    worker_id,
                    make_err!(
                        Code::Internal,
                        "Worker {worker_id} timed out, removing from pool"
                    ),
                );
            }

            Ok(())
        })
    }

    async fn set_drain_worker(&self, worker_id: WorkerId, is_draining: bool) -> Result<(), Error> {
        let mut inner = self.get_inner_lock();
        inner.set_drain_worker(worker_id, is_draining)
    }

    fn register_metrics(self: Arc<Self>, _registry: &mut Registry) {
        // We do not register anything here because we only want to register metrics
        // once and we rely on the `ActionScheduler::register_metrics()` to do that.
    }
}

#[async_trait]
impl Operations for SimpleScheduler {
    fn list_actions(&self) -> Vec<Operation> {
        let lock = self.inner.lock();
        let active_actions: Iter<Arc<ActionInfo>, AwaitedAction> = lock.active_actions.iter();
        let queued_actions: std::collections::btree_map::Iter<Arc<ActionInfo>, AwaitedAction> =
            lock.queued_actions.iter();
        let recently_completed_actions = lock.recently_completed_actions.iter();

        let active_operations: Vec<Operation> = active_actions
            .map(|(_, awaited_action)| {
                Operation::from(awaited_action.current_state.as_ref().clone())
            })
            .collect();
        let queued_operations: Vec<Operation> = queued_actions
            .map(|(_, awaited_action)| {
                Operation::from(awaited_action.current_state.as_ref().clone())
            })
            .collect();
        let completed_operations: Vec<Operation> = recently_completed_actions
            .map(|completed_action| Operation::from(completed_action.state.as_ref().clone()))
            .collect();

        let operations: Vec<Operation> = active_operations
            .into_iter()
            .chain(queued_operations)
            .chain(completed_operations)
            .collect();
        operations
    }

    fn get_action(&self, action_info_hash_key: &ActionInfoHashKey) -> Option<Operation> {
        let lock: parking_lot::lock_api::MutexGuard<RawMutex, SimpleSchedulerImpl> =
            self.inner.lock();
        let active_actions: HashMap<Arc<ActionInfo>, AwaitedAction> = lock.active_actions.clone();
        let queued_actions: BTreeMap<Arc<ActionInfo>, AwaitedAction> = lock.queued_actions.clone();
        let queued_actions_set: HashSet<Arc<ActionInfo>> = lock.queued_actions_set.clone();
        let recently_completed_actions = lock.recently_completed_actions.clone();
        let action: Option<&ActionState> = queued_actions_set
            .get(action_info_hash_key)
            .and_then(|action_info| {
                queued_actions
                    .get(action_info)
                    .map(|queued_action| queued_action.current_state.as_ref())
            })
            .or_else(|| {
                active_actions
                    .get(action_info_hash_key)
                    .map(|awaited_action| awaited_action.current_state.as_ref())
            })
            .or_else(|| {
                recently_completed_actions
                    .get(action_info_hash_key)
                    .map(|completed_action| completed_action.state.as_ref())
            });
        let action: Option<Operation> = action.map(|a| Operation::from(a.clone()));
        action
    }

    async fn delete_action(&self) {
        todo!()
    }

    async fn cancel_action(&self) {
        todo!()
    }

    async fn wait_action(&self) {
        todo!()
    }
}

impl MetricsComponent for SimpleScheduler {
    fn gather_metrics(&self, c: &mut CollectorState) {
        self.metrics.gather_metrics(c);
        {
            // We use the raw lock because we dont gather stats about gathering stats.
            let inner = self.inner.lock();
            c.publish(
                "queued_actions_total",
                &inner.queued_actions.len(),
                "The number actions in the queue.",
            );
            c.publish(
                "workers_total",
                &inner.workers.workers.len(),
                "The number workers active.",
            );
            c.publish(
                "active_actions_total",
                &inner.active_actions.len(),
                "The number of running actions.",
            );
            c.publish(
                "recently_completed_actions_total",
                &inner.recently_completed_actions.len(),
                "The number of recently completed actions in the buffer.",
            );
            c.publish(
                "retain_completed_for_seconds",
                &inner.retain_completed_for,
                "The duration completed actions are retained for.",
            );
            c.publish(
                "worker_timeout_seconds",
                &inner.worker_timeout_s,
                "The configured timeout if workers have not responded for a while.",
            );
            c.publish(
                "max_job_retries",
                &inner.max_job_retries,
                "The amount of times a job is allowed to retry from an internal error before it is dropped.",
            );
            let mut props = HashMap::<&String, u64>::new();
            for (_worker_id, worker) in inner.workers.workers.iter() {
                c.publish_with_labels(
                    "workers",
                    worker,
                    "",
                    vec![("worker_id".into(), worker.id.to_string().into())],
                );
                for (property, prop_value) in &worker.platform_properties.properties {
                    let current_value = props.get(&property).unwrap_or(&0);
                    if let PlatformPropertyValue::Minimum(worker_value) = prop_value {
                        props.insert(property, *current_value + *worker_value);
                    }
                }
            }
            for (property, prop_value) in props {
                c.publish(
                    &format!("{property}_available_properties"),
                    &prop_value,
                    format!("Total sum of available properties for {property}"),
                );
            }
            for (_, active_action) in inner.active_actions.iter() {
                let action_name = active_action
                    .action_info
                    .unique_qualifier
                    .action_name()
                    .into();
                let worker_id_str = match active_action.worker_id {
                    Some(id) => id.to_string(),
                    None => "Unassigned".to_string(),
                };
                c.publish_with_labels(
                    "active_actions",
                    active_action,
                    "",
                    vec![
                        ("worker_id".into(), worker_id_str.into()),
                        ("digest".into(), action_name),
                    ],
                );
            }
            // Note: We don't publish queued_actions because it can be very large.
            // Note: We don't publish recently completed actions because it can be very large.
        }
    }
}

impl MetricsComponent for CompletedAction {
    fn gather_metrics(&self, c: &mut CollectorState) {
        c.publish(
            "completed_timestamp",
            &self.completed_time,
            "The timestamp this action was completed",
        );
        c.publish(
            "current_state",
            self.state.as_ref(),
            "The current stage of the action.",
        );
    }
}

impl MetricsComponent for AwaitedAction {
    fn gather_metrics(&self, c: &mut CollectorState) {
        c.publish(
            "action_digest",
            &self.action_info.unique_qualifier.action_name(),
            "The digest of the action.",
        );
        c.publish(
            "current_state",
            self.current_state.as_ref(),
            "The current stage of the action.",
        );
        c.publish(
            "attempts",
            &self.attempts,
            "The number of attempts this action has tried.",
        );
        c.publish(
            "last_error",
            &format!("{:?}", self.last_error),
            "The last error this action caused from a retry (if any).",
        );
    }
}

#[derive(Default)]
struct Metrics {
    add_action: FuncCounterWrapper,
    existing_actions_found: CounterWithTime,
    existing_actions_not_found: CounterWithTime,
    clean_recently_completed_actions: CounterWithTime,
    remove_timedout_workers: FuncCounterWrapper,
    update_action: FuncCounterWrapper,
    update_action_missing_action_result: CounterWithTime,
    update_action_from_wrong_worker: CounterWithTime,
    update_action_no_more_listeners: CounterWithTime,
    update_action_with_internal_error: CounterWithTime,
    update_action_with_internal_error_no_action: CounterWithTime,
    update_action_with_internal_error_backpressure: CounterWithTime,
    update_action_with_internal_error_from_wrong_worker: CounterWithTime,
    workers_evicted: CounterWithTime,
    workers_evicted_with_running_action: CounterWithTime,
    workers_drained: CounterWithTime,
    retry_action: CounterWithTime,
    retry_action_max_attempts_reached: CounterWithTime,
    retry_action_no_more_listeners: CounterWithTime,
    retry_action_but_action_missing: CounterWithTime,
    add_action_joined_running_action: CounterWithTime,
    add_action_joined_queued_action: CounterWithTime,
    add_action_new_action_created: CounterWithTime,
    add_worker: FuncCounterWrapper,
    timedout_workers: CounterWithTime,
    lock_stall_time: AtomicU64,
    lock_stall_time_counter: AtomicU64,
    do_try_match: AsyncCounterWrapper,
}

impl Metrics {
    fn gather_metrics(&self, c: &mut CollectorState) {
        c.publish(
            "add_action",
            &self.add_action,
            "The number of times add_action was called.",
        );
        c.publish_with_labels(
            "find_existing_action",
            &self.existing_actions_found,
            "The number of times existing_actions_found had an action found.",
            vec![("result".into(), "found".into())],
        );
        c.publish_with_labels(
            "find_existing_action",
            &self.existing_actions_not_found,
            "The number of times existing_actions_found had an action not found.",
            vec![("result".into(), "not_found".into())],
        );
        c.publish(
            "clean_recently_completed_actions",
            &self.clean_recently_completed_actions,
            "The number of times clean_recently_completed_actions was triggered.",
        );
        c.publish(
            "remove_timedout_workers",
            &self.remove_timedout_workers,
            "The number of times remove_timedout_workers was triggered.",
        );
        {
            c.publish_with_labels(
                "update_action",
                &self.update_action,
                "Stats about errors when worker sends update_action() to scheduler.",
                vec![("result".into(), "missing_action_result".into())],
            );
            c.publish_with_labels(
                "update_action_errors",
                &self.update_action_missing_action_result,
                "Stats about errors when worker sends update_action() to scheduler. These errors are not complete, just the most common.",
                vec![("result".into(), "missing_action_result".into())],
            );
            c.publish_with_labels(
                "update_action_errors",
                &self.update_action_from_wrong_worker,
                "Stats about errors when worker sends update_action() to scheduler. These errors are not complete, just the most common.",
                vec![("result".into(), "from_wrong_worker".into())],
            );
            c.publish_with_labels(
                "update_action_errors",
                &self.update_action_no_more_listeners,
                "Stats about errors when worker sends update_action() to scheduler. These errors are not complete, just the most common.",
                vec![("result".into(), "no_more_listeners".into())],
            );
        }
        c.publish(
            "update_action_with_internal_error",
            &self.update_action_with_internal_error,
            "The number of times update_action_with_internal_error was triggered.",
        );
        {
            c.publish_with_labels(
                "update_action_with_internal_error_errors",
                &self.update_action_with_internal_error_no_action,
                "Stats about what errors caused update_action_with_internal_error() in scheduler.",
                vec![("result".into(), "no_action".into())],
            );
            c.publish_with_labels(
                "update_action_with_internal_error_errors",
                &self.update_action_with_internal_error_backpressure,
                "Stats about what errors caused update_action_with_internal_error() in scheduler.",
                vec![("result".into(), "backpressure".into())],
            );
            c.publish_with_labels(
                "update_action_with_internal_error_errors",
                &self.update_action_with_internal_error_from_wrong_worker,
                "Stats about what errors caused update_action_with_internal_error() in scheduler.",
                vec![("result".into(), "from_wrong_worker".into())],
            );
        }
        c.publish(
            "workers_evicted_total",
            &self.workers_evicted,
            "The number of workers evicted from scheduler.",
        );
        c.publish(
            "workers_evicted_with_running_action",
            &self.workers_evicted_with_running_action,
            "The number of jobs cancelled because worker was evicted from scheduler.",
        );
        c.publish(
            "workers_drained_total",
            &self.workers_drained,
            "The number of workers drained from scheduler.",
        );
        {
            c.publish_with_labels(
                "retry_action",
                &self.retry_action,
                "Stats about retry_action().",
                vec![("result".into(), "success".into())],
            );
            c.publish_with_labels(
                "retry_action",
                &self.retry_action_max_attempts_reached,
                "Stats about retry_action().",
                vec![("result".into(), "max_attempts_reached".into())],
            );
            c.publish_with_labels(
                "retry_action",
                &self.retry_action_no_more_listeners,
                "Stats about retry_action().",
                vec![("result".into(), "no_more_listeners".into())],
            );
            c.publish_with_labels(
                "retry_action",
                &self.retry_action_but_action_missing,
                "Stats about retry_action().",
                vec![("result".into(), "action_missing".into())],
            );
        }
        {
            c.publish_with_labels(
                "add_action",
                &self.add_action_joined_running_action,
                "Stats about add_action().",
                vec![("result".into(), "joined_running_action".into())],
            );
            c.publish_with_labels(
                "add_action",
                &self.add_action_joined_queued_action,
                "Stats about add_action().",
                vec![("result".into(), "joined_queued_action".into())],
            );
            c.publish_with_labels(
                "add_action",
                &self.add_action_new_action_created,
                "Stats about add_action().",
                vec![("result".into(), "new_action_created".into())],
            );
        }
        c.publish(
            "add_worker",
            &self.add_worker,
            "Stats about add_worker() being called on the scheduler.",
        );
        c.publish(
            "timedout_workers",
            &self.timedout_workers,
            "The number of workers that timed out.",
        );
        c.publish(
            "lock_stall_time_nanos_total",
            &self.lock_stall_time,
            "The total number of nanos spent waiting on the lock in the scheduler.",
        );
        c.publish(
            "lock_stall_time_total",
            &self.lock_stall_time_counter,
            "The number of times a lock request was made in the scheduler.",
        );
        c.publish(
            "lock_stall_time_avg_nanos",
            &(self.lock_stall_time.load(Ordering::Relaxed)
                / self.lock_stall_time_counter.load(Ordering::Relaxed)),
            "The average time the scheduler stalled waiting on the lock to release in nanos.",
        );
        c.publish(
            "matching_engine",
            &self.do_try_match,
            "The job<->worker matching engine stats. This is a very expensive operation, so it is not run every time (often called do_try_match).",
        );
    }
}
