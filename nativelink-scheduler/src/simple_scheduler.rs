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

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use async_lock::{Mutex, MutexGuard};
use async_trait::async_trait;
use futures::{Future, Stream, TryFutureExt};
use hashbrown::{HashMap, HashSet};
use nativelink_error::{make_err, make_input_err, Code, Error, ResultExt};
use nativelink_util::action_messages::{
    ActionInfo, ActionInfoHashKey, ActionResult, ActionStage, ActionState, ExecutionMetadata,
    OperationId, WorkerId,
};
use nativelink_util::metrics_utils::{
    AsyncCounterWrapper, Collector, CollectorState, CounterWithTime, FuncCounterWrapper,
    MetricsComponent, Registry,
};
use nativelink_util::platform_properties::PlatformPropertyValue;
use nativelink_util::spawn;
use nativelink_util::task::JoinHandleDropGuard;
use tokio::sync::{watch, Notify};
use tokio::time::Duration;
use tokio_stream::StreamExt;
use tracing::{event, Level};

use crate::action_scheduler::ActionScheduler;
use crate::operation_state_manager::{
    ActionStateResult, ClientStateManager, MatchingEngineStateManager, OperationFilter,
    OperationStageFlags, WorkerStateManager,
};
use crate::platform_property_manager::PlatformPropertyManager;
use crate::scheduler_state::awaited_action::AwaitedAction;
use crate::scheduler_state::metrics::Metrics as SchedulerMetrics;
use crate::scheduler_state::state_manager::{mutate_stage, StateManager, StateManagerImpl};
use crate::scheduler_state::workers::Workers;
use crate::worker::{Worker, WorkerTimestamp, WorkerUpdate};
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

struct SimpleSchedulerImpl {
    /// The manager responsible for holding the state of actions and workers.
    state_manager: StateManager,
    /// The duration that actions are kept in recently_completed_actions for.
    retain_completed_for: Duration,
    /// Timeout of how long to evict workers if no response in this given amount of time in seconds.
    worker_timeout_s: u64,
    /// Default times a job can retry before failing.
    max_job_retries: usize,
    metrics: Arc<Metrics>,
}

impl SimpleSchedulerImpl {
    /// Attempts to find a worker to execute an action and begins executing it.
    /// If an action is already running that is cacheable it may merge this action
    /// with the results and state changes of the already running action.
    /// If the task cannot be executed immediately it will be queued for execution
    /// based on priority and other metrics.
    /// All further updates to the action will be provided through `listener`.
    async fn add_action(
        &mut self,
        action_info: ActionInfo,
    ) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        let add_action_result = self.state_manager.add_action(action_info).await?;
        add_action_result.as_receiver().await.cloned()
    }

    async fn clean_recently_completed_actions(&mut self) {
        let expiry_time = SystemTime::now()
            .checked_sub(self.retain_completed_for)
            .unwrap();
        self.state_manager
            .inner
            .lock()
            .await
            .recently_completed_actions
            .retain(|action| action.completed_time > expiry_time);
    }

    async fn find_recently_completed_action(
        &self,
        unique_qualifier: &ActionInfoHashKey,
    ) -> Result<Option<watch::Receiver<Arc<ActionState>>>, Error> {
        Ok(self
            .state_manager
            .inner
            .lock()
            .await
            .recently_completed_actions
            .get(unique_qualifier)
            .map(|action| watch::channel(action.state.clone()).1))
    }

    async fn find_existing_action(
        &self,
        unique_qualifier: &ActionInfoHashKey,
    ) -> Result<Option<watch::Receiver<Arc<ActionState>>>, Error> {
        let filter_result = <StateManager as ClientStateManager>::filter_operations(
            &self.state_manager,
            OperationFilter {
                stages: OperationStageFlags::Any,
                operation_id: None,
                worker_id: None,
                action_digest: None,
                worker_update_before: None,
                completed_before: None,
                last_client_update_before: None,
                unique_qualifier: Some(unique_qualifier.clone()),
                order_by: None,
            },
        )
        .await;

        let mut stream = filter_result
            .err_tip(|| "In SimpleScheduler::find_existing_action getting filter result")?;
        if let Some(result) = stream.next().await {
            Ok(Some(
                result
                    .as_receiver()
                    .await
                    .err_tip(|| "In SimpleScheduler::find_existing_action getting receiver")?
                    .clone(),
            ))
        } else {
            Ok(None)
        }
    }

    fn retry_action(
        inner_state: &mut MutexGuard<'_, StateManagerImpl>,
        max_job_retries: usize,
        metrics: &Metrics,
        action_info: &Arc<ActionInfo>,
        worker_id: &WorkerId,
        err: Error,
    ) {
        match inner_state.active_actions.remove(action_info) {
            Some(running_action) => {
                let mut awaited_action = running_action;
                let send_result = if awaited_action.attempts >= max_job_retries {
                    metrics.retry_action_max_attempts_reached.inc();

                    mutate_stage(&mut awaited_action, ActionStage::Completed(ActionResult {
                        execution_metadata: ExecutionMetadata {
                            worker: format!("{worker_id}"),
                            ..ExecutionMetadata::default()
                        },
                        error: Some(err.merge(make_err!(
                            Code::Internal,
                            "Job cancelled because it attempted to execute too many times and failed"
                        ))),
                        ..ActionResult::default()
                    }))
                    // Do not put the action back in the queue here, as this action attempted to run too many
                    // times.
                } else {
                    metrics.retry_action.inc();
                    let send_result = mutate_stage(&mut awaited_action, ActionStage::Queued);
                    inner_state.queued_actions_set.insert(action_info.clone());
                    inner_state
                        .queued_actions
                        .insert(action_info.clone(), awaited_action);
                    send_result
                };

                if send_result.is_err() {
                    metrics.retry_action_no_more_listeners.inc();
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
                metrics.retry_action_but_action_missing.inc();
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
    fn immediate_evict_worker(
        inner_state: &mut MutexGuard<'_, StateManagerImpl>,
        max_job_retries: usize,
        metrics: &Metrics,
        worker_id: &WorkerId,
        err: Error,
    ) {
        if let Some(mut worker) = inner_state.workers.remove_worker(worker_id) {
            metrics.workers_evicted.inc();
            // We don't care if we fail to send message to worker, this is only a best attempt.
            let _ = worker.notify_update(WorkerUpdate::Disconnect);
            // We create a temporary Vec to avoid doubt about a possible code
            // path touching the worker.running_action_infos elsewhere.
            for action_info in worker.running_action_infos.drain() {
                metrics.workers_evicted_with_running_action.inc();
                SimpleSchedulerImpl::retry_action(
                    inner_state,
                    max_job_retries,
                    metrics,
                    &action_info,
                    worker_id,
                    err.clone(),
                );
            }
        }
        // Note: Calling this many time is very cheap, it'll only trigger `do_try_match` once.
        inner_state.tasks_change_notify.notify_one();
    }

    /// Sets if the worker is draining or not.
    async fn set_drain_worker(
        &mut self,
        worker_id: WorkerId,
        is_draining: bool,
    ) -> Result<(), Error> {
        let mut inner_state = self.state_manager.inner.lock().await;
        let worker = inner_state
            .workers
            .workers
            .get_mut(&worker_id)
            .err_tip(|| format!("Worker {worker_id} doesn't exist in the pool"))?;
        self.metrics.workers_drained.inc();
        worker.is_draining = is_draining;
        inner_state.tasks_change_notify.notify_one();
        Ok(())
    }

    async fn get_queued_operations(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Arc<dyn ActionStateResult + 'static>> + Send>>, Error>
    {
        <StateManager as MatchingEngineStateManager>::filter_operations(
            &self.state_manager,
            OperationFilter {
                stages: OperationStageFlags::Queued,
                operation_id: None,
                worker_id: None,
                action_digest: None,
                worker_update_before: None,
                completed_before: None,
                last_client_update_before: None,
                unique_qualifier: None,
                order_by: None,
            },
        )
        .await
    }

    // TODO(blaise.bruer) This is an O(n*m) (aka n^2) algorithm. In theory we can create a map
    // of capabilities of each worker and then try and match the actions to the worker using
    // the map lookup (ie. map reduce).
    async fn do_try_match(&mut self) {
        // TODO(blaise.bruer) This is a bit difficult because of how rust's borrow checker gets in
        // the way. We need to conditionally remove items from the `queued_action`. Rust is working
        // to add `drain_filter`, which would in theory solve this problem, but because we need
        // to iterate the items in reverse it becomes more difficult (and it is currently an
        // unstable feature [see: https://github.com/rust-lang/rust/issues/70530]).

        let action_state_results = self.get_queued_operations().await;

        match action_state_results {
            Ok(mut stream) => {
                while let Some(action_state_result) = stream.next().await {
                    let action_state_result = action_state_result.as_action_info().await;
                    let Ok(action_info) = action_state_result else {
                        let _ = action_state_result.inspect_err(|err| {
                            event!(
                                Level::ERROR,
                                ?err,
                                "Failed to get action_info from action_state_results stream"
                            );
                        });
                        continue;
                    };

                    let (operation_id, maybe_worker_id) = {
                        let inner_state = self.state_manager.inner.lock().await;
                        let Some(awaited_action): Option<&AwaitedAction> =
                            inner_state.queued_actions.get(action_info.as_ref())
                        else {
                            event!(
                                Level::ERROR,
                                ?action_info,
                                "queued_actions out of sync with itself"
                            );
                            continue;
                        };

                        (
                            awaited_action.current_state.id.clone(),
                            inner_state.workers.find_worker_for_action(awaited_action),
                        )
                    };

                    let ret = <StateManager as MatchingEngineStateManager>::update_operation(
                        &self.state_manager,
                        operation_id.clone(),
                        maybe_worker_id,
                        Ok(ActionStage::Executing),
                    )
                    .await;

                    if let Err(e) = ret {
                        event!(
                            Level::ERROR,
                            ?e,
                            "update operation failed for {}",
                            operation_id
                        );
                    }
                }
            }
            Err(e) => {
                event!(Level::ERROR, ?e, "stream error in do_try_match");
            }
        }
    }

    async fn update_action(
        &mut self,
        worker_id: &WorkerId,
        action_info_hash_key: ActionInfoHashKey,
        action_stage: Result<ActionStage, Error>,
    ) -> Result<(), Error> {
        let update_operation_result = <StateManager as WorkerStateManager>::update_operation(
            &self.state_manager,
            OperationId::new(action_info_hash_key.clone()),
            *worker_id,
            action_stage,
        )
        .await;
        if let Err(e) = &update_operation_result {
            event!(
                Level::ERROR,
                ?action_info_hash_key,
                ?worker_id,
                ?e,
                "Failed to update_operation on update_action"
            );
        }
        update_operation_result
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

        let tasks_change_notify = Arc::new(Notify::new());
        let state_manager = StateManager::new(
            HashSet::new(),
            BTreeMap::new(),
            Workers::new(scheduler_cfg.allocation_strategy),
            HashMap::new(),
            HashSet::new(),
            Arc::new(SchedulerMetrics::default()),
            max_job_retries,
            tasks_change_notify.clone(),
        );
        let metrics = Arc::new(Metrics::default());
        let metrics_for_do_try_match = metrics.clone();
        let inner = Arc::new(Mutex::new(SimpleSchedulerImpl {
            state_manager,
            retain_completed_for: Duration::new(retain_completed_for_s, 0),
            worker_timeout_s,
            max_job_retries,
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
                        tasks_change_notify.notified().await;
                        match weak_inner.upgrade() {
                            // Note: According to `parking_lot` documentation, the default
                            // `Mutex` implementation is eventual fairness, so we don't
                            // really need to worry about this thread taking the lock
                            // starving other threads too much.
                            Some(inner_mux) => {
                                let mut inner = inner_mux.lock().await;
                                let timer = metrics_for_do_try_match.do_try_match.begin_timer();
                                inner.do_try_match().await;
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
    pub async fn contains_worker_for_test(&self, worker_id: &WorkerId) -> bool {
        let inner_scheduler = self.get_inner_lock().await;
        let inner_state = inner_scheduler.state_manager.inner.lock().await;
        inner_state.workers.workers.contains(worker_id)
    }

    /// A unit test function used to send the keep alive message to the worker from the server.
    pub async fn send_keep_alive_to_worker_for_test(
        &self,
        worker_id: &WorkerId,
    ) -> Result<(), Error> {
        let inner_scheduler = self.get_inner_lock().await;
        let mut inner_state = inner_scheduler.state_manager.inner.lock().await;
        let worker = inner_state
            .workers
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| {
                make_input_err!("WorkerId '{}' does not exist in workers map", worker_id)
            })?;
        worker.keep_alive()
    }

    async fn get_inner_lock(&self) -> MutexGuard<'_, SimpleSchedulerImpl> {
        // We don't use one of the wrappers because we only want to capture the time spent,
        // nothing else beacuse this is a hot path.
        let start = Instant::now();
        let lock: MutexGuard<SimpleSchedulerImpl> = self.inner.lock().await;
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
        let mut inner = self.get_inner_lock().await;
        self.metrics
            .add_action
            .wrap(inner.add_action(action_info))
            .await
    }

    async fn find_existing_action(
        &self,
        unique_qualifier: &ActionInfoHashKey,
    ) -> Result<Option<watch::Receiver<Arc<ActionState>>>, Error> {
        let inner = self.get_inner_lock().await;
        let maybe_receiver = inner
            .find_existing_action(unique_qualifier)
            .and_then(|maybe_action| async {
                if let Some(action) = maybe_action {
                    Ok(Some(action))
                } else {
                    inner.find_recently_completed_action(unique_qualifier).await
                }
            })
            .await
            .err_tip(|| "Error while finding existing action")?;
        if maybe_receiver.is_some() {
            self.metrics.existing_actions_found.inc();
        } else {
            self.metrics.existing_actions_not_found.inc();
        }
        Ok(maybe_receiver)
    }

    async fn clean_recently_completed_actions(&self) {
        self.get_inner_lock()
            .await
            .clean_recently_completed_actions()
            .await;
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
        let inner_scheduler = self.get_inner_lock().await;
        let max_job_retries = inner_scheduler.max_job_retries;
        let mut inner_state = inner_scheduler.state_manager.inner.lock().await;
        self.metrics.add_worker.wrap(move || {
            let res = inner_state
                .workers
                .add_worker(worker)
                .err_tip(|| "Error while adding worker, removing from pool");
            if let Err(err) = &res {
                SimpleSchedulerImpl::immediate_evict_worker(
                    &mut inner_state,
                    max_job_retries,
                    &self.metrics,
                    &worker_id,
                    err.clone(),
                );
            }
            inner_state.tasks_change_notify.notify_one();
            res
        })
    }

    async fn update_action(
        &self,
        worker_id: &WorkerId,
        action_info_hash_key: ActionInfoHashKey,
        action_stage: Result<ActionStage, Error>,
    ) -> Result<(), Error> {
        let mut inner = self.get_inner_lock().await;
        self.metrics
            .update_action
            .wrap(inner.update_action(worker_id, action_info_hash_key, action_stage))
            .await
    }

    async fn worker_keep_alive_received(
        &self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        let inner_scheduler = self.get_inner_lock().await;
        let mut inner_state = inner_scheduler.state_manager.inner.lock().await;
        inner_state
            .workers
            .refresh_lifetime(worker_id, timestamp)
            .err_tip(|| "Error refreshing lifetime in worker_keep_alive_received()")
    }

    async fn remove_worker(&self, worker_id: WorkerId) {
        let inner_scheduler = self.get_inner_lock().await;
        let mut inner_state = inner_scheduler.state_manager.inner.lock().await;
        SimpleSchedulerImpl::immediate_evict_worker(
            &mut inner_state,
            inner_scheduler.max_job_retries,
            &inner_scheduler.metrics,
            &worker_id,
            make_err!(Code::Internal, "Received request to remove worker"),
        );
    }

    async fn remove_timedout_workers(&self, now_timestamp: WorkerTimestamp) -> Result<(), Error> {
        let inner_scheduler = self.get_inner_lock().await;
        let worker_timeout_s = inner_scheduler.worker_timeout_s;
        let max_job_retries = inner_scheduler.max_job_retries;
        let metrics = inner_scheduler.metrics.clone();
        let mut inner_state = inner_scheduler.state_manager.inner.lock().await;
        self.metrics.remove_timedout_workers.wrap(move || {
            // Items should be sorted based on last_update_timestamp, so we don't need to iterate the entire
            // map most of the time.
            let worker_ids_to_remove: Vec<WorkerId> = inner_state
                .workers
                .workers
                .iter()
                .rev()
                .map_while(|(worker_id, worker)| {
                    if worker.last_update_timestamp <= now_timestamp - worker_timeout_s {
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
                SimpleSchedulerImpl::immediate_evict_worker(
                    &mut inner_state,
                    max_job_retries,
                    &metrics,
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
        let mut inner = self.get_inner_lock().await;
        inner.set_drain_worker(worker_id, is_draining).await
    }

    fn register_metrics(self: Arc<Self>, _registry: &mut Registry) {
        // We do not register anything here because we only want to register metrics
        // once and we rely on the `ActionScheduler::register_metrics()` to do that.
    }
}

impl MetricsComponent for SimpleScheduler {
    fn gather_metrics(&self, c: &mut CollectorState) {
        self.metrics.gather_metrics(c);
        {
            // We use the raw lock because we dont gather stats about gathering stats.
            let inner_scheduler = self.inner.lock_blocking();
            let inner_state = inner_scheduler.state_manager.inner.lock_blocking();
            inner_state.metrics.gather_metrics(c);
            c.publish(
                "queued_actions_total",
                &inner_state.queued_actions.len(),
                "The number actions in the queue.",
            );
            c.publish(
                "workers_total",
                &inner_state.workers.workers.len(),
                "The number workers active.",
            );
            c.publish(
                "active_actions_total",
                &inner_state.active_actions.len(),
                "The number of running actions.",
            );
            c.publish(
                "recently_completed_actions_total",
                &inner_state.recently_completed_actions.len(),
                "The number of recently completed actions in the buffer.",
            );
            c.publish(
                "retain_completed_for_seconds",
                &inner_scheduler.retain_completed_for,
                "The duration completed actions are retained for.",
            );
            c.publish(
                "worker_timeout_seconds",
                &inner_scheduler.worker_timeout_s,
                "The configured timeout if workers have not responded for a while.",
            );
            c.publish(
                "max_job_retries",
                &inner_scheduler.max_job_retries,
                "The amount of times a job is allowed to retry from an internal error before it is dropped.",
            );
            let mut props = HashMap::<&String, u64>::new();
            for (_worker_id, worker) in inner_state.workers.workers.iter() {
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
            for (_, active_action) in inner_state.active_actions.iter() {
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

#[derive(Default)]
struct Metrics {
    add_action: AsyncCounterWrapper,
    existing_actions_found: CounterWithTime,
    existing_actions_not_found: CounterWithTime,
    clean_recently_completed_actions: CounterWithTime,
    remove_timedout_workers: FuncCounterWrapper,
    update_action: AsyncCounterWrapper,
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
