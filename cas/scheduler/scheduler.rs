// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::cmp;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use fast_async_mutex::mutex::Mutex;
use lru::LruCache;
use rand::{thread_rng, Rng};
use tokio::sync::watch;

use action_messages::{ActionInfo, ActionStage, ActionState};
use common::log;
use error::{error_if, make_input_err, Error, ResultExt};
use worker::{Worker, WorkerId, WorkerTimestamp, WorkerUpdate};

/// An action that is being awaited on and last known state.
struct AwaitedAction {
    action_info: Arc<ActionInfo>,
    current_state: Arc<ActionState>,
    notify_channel: watch::Sender<Arc<ActionState>>,
}

/// Holds the relationship of a worker that is executing a specific action.
struct RunningAction {
    _worker_id: WorkerId,
    action: AwaitedAction,
}

struct Workers {
    workers: LruCache<WorkerId, Worker>,
}

impl Workers {
    fn new() -> Self {
        Self {
            workers: LruCache::unbounded(),
        }
    }

    /// Refreshes the lifetime of the worker with the given timestamp.
    fn refresh_lifetime(&mut self, worker_id: &WorkerId, timestamp: WorkerTimestamp) -> Result<(), Error> {
        let worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| make_input_err!("Worker not found in worker map in refresh_lifetime() {}", worker_id))?;
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
        if let Err(e) = &res {
            log::error!(
                "Worker connection appears to have been closed while adding to pool : {:?}",
                e
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
    fn find_worker_for_action_mut<'a>(&'a mut self, awaited_action: &AwaitedAction) -> Option<&'a mut Worker> {
        assert!(matches!(awaited_action.current_state.stage, ActionStage::Queued));
        let action_properties = &awaited_action.action_info.platform_properties;
        return self.workers.iter_mut().find_map(|(_, w)| {
            if action_properties.is_satisfied_by(&w.platform_properties) {
                Some(w)
            } else {
                None
            }
        });
    }
}

/// Simple helper type to help with self-documentation.
type ShouldRunAgain = bool;

struct SchedulerImpl {
    // We cannot use the special hash function we use for ActionInfo with BTreeMap because
    // we need to be able to get an exact match when we look for `ActionInfo` structs that
    // have `digest` and `salt` matches and nothing else. This is because we want to sort
    // the `queued_actions` entries in order of `priority`, but if a new entry matches
    // we want to ignore the priority and join them together then use the higher priority
    // of the two, so we use a `HashSet` to find the original `ActionInfo` when trying to
    // merge actions together.
    queued_actions_set: HashSet<Arc<ActionInfo>>,
    queued_actions: BTreeMap<Arc<ActionInfo>, AwaitedAction>,
    workers: Workers,
    active_actions: HashMap<Arc<ActionInfo>, RunningAction>,
    /// Timeout of how long to evict workers if no response in this given amount of time in seconds.
    worker_timeout_s: u64,
}

impl SchedulerImpl {
    /// Attempts to find a worker to execute an action and begins executing it.
    /// If an action is already running that is cacheable it may merge this action
    /// with the results and state changes of the already running action.
    /// If the task cannot be executed immediately it will be queued for execution
    /// based on priority and other metrics.
    /// All further updates to the action will be provided through `listener`.
    fn add_action(&mut self, action_info: ActionInfo) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        // Check to see if the action is running, if it is and cacheable, merge the actions.
        if let Some(running_action) = self.active_actions.get_mut(&action_info) {
            let rx = running_action.action.notify_channel.subscribe();
            running_action
                .action
                .notify_channel
                .send(running_action.action.current_state.clone())
                .unwrap();
            return Ok(rx);
        }

        // Check to see if the action is queued, if it is and cacheable, merge the actions.
        if let Some(mut arc_action_info) = self.queued_actions_set.take(&action_info) {
            let (original_action_info, queued_action) = self
                .queued_actions
                .remove_entry(&arc_action_info)
                .err_tip(|| "Internal error queued_actions and queued_actions_set should match")?;

            // In the event our task is higher priority than the one already scheduled, increase
            // the priority of the scheduled one.
            Arc::make_mut(&mut arc_action_info).priority =
                cmp::max(original_action_info.priority, action_info.priority);

            let rx = queued_action.notify_channel.subscribe();
            queued_action
                .notify_channel
                .send(queued_action.current_state.clone())
                .unwrap();

            // Even if we fail to send our action to the client, we need to add this action back to the
            // queue because it was remove earlier.
            self.queued_actions.insert(arc_action_info.clone(), queued_action);
            self.queued_actions_set.insert(arc_action_info);
            return Ok(rx);
        }

        // Action needs to be added to queue or is not cacheable.
        let action_info = Arc::new(action_info);
        let action_digest = action_info.digest.clone();

        let current_state = Arc::new(ActionState {
            name: format!("{:X}", thread_rng().gen::<u128>()),
            stage: ActionStage::Queued,
            action_digest,
        });

        let (tx, rx) = watch::channel(current_state.clone());
        self.queued_actions_set.insert(action_info.clone());
        self.queued_actions.insert(
            action_info.clone(),
            AwaitedAction {
                action_info,
                current_state,
                notify_channel: tx,
            },
        );

        self.do_try_match();
        Ok(rx)
    }

    /// Evicts the worker from the pool and puts items back into the queue if anything was being executed on it.
    /// Note: This will not call .do_try_match().
    fn immediate_evict_worker(&mut self, worker_id: &WorkerId) {
        if let Some(mut worker) = self.workers.remove_worker(&worker_id) {
            // We don't care if we fail to send message to worker, this is only a best attempt.
            let _ = worker.notify_update(WorkerUpdate::Disconnect);
            if let Some(action_info) = worker.running_action_info {
                match self.active_actions.remove(&action_info) {
                    Some(running_action) => {
                        let mut awaited_action = running_action.action;
                        Arc::make_mut(&mut awaited_action.current_state).stage = ActionStage::Queued;
                        let send_result = awaited_action.notify_channel.send(awaited_action.current_state.clone());
                        self.queued_actions_set.insert(action_info.clone());
                        self.queued_actions.insert(action_info.clone(), awaited_action);
                        if send_result.is_err() {
                            // Don't remove this task, instead we keep them around for a bit just in case
                            // the client disconnected and will reconnect and ask for same job to be executed
                            // again.
                            log::warn!(
                                "Action {} has no more listeners during evict_worker()",
                                action_info.digest.str()
                            );
                        }
                    }
                    None => {
                        log::error!("Worker stated it was running an action, but it was not in the active_actions : Worker: {:?}, ActionInfo: {:?}", worker.id, action_info);
                    }
                }
            }
        }
    }

    /// Wrapper to keep running in the event we could not complete all scheduling in one iteration.
    fn do_try_match(&mut self) {
        // Run do_try_match until it doesn't need to run again.
        while self.inner_do_try_match() {}
    }

    // TODO(blaise.bruer) This is an O(n*m) (aka n^2) algorithm. In theory we can create a map
    // of capabilities of each worker and then try and match the actions to the worker using
    // the map lookup (ie. map reduce).
    fn inner_do_try_match(&mut self) -> ShouldRunAgain {
        let mut should_run_again = false;
        // TODO(blaise.bruer) This is a bit difficult because of how rust's borrow checker gets in
        // the way. We need to conditionally remove items from the `queued_action`. Rust is working
        // to add `drain_filter`, which would in theory solve this problem, but because we need
        // to iterate the items in reverse it becomes more difficult (and it is currently an
        // unstable feature [see: https://github.com/rust-lang/rust/issues/70530]).
        let action_infos: Vec<Arc<ActionInfo>> = self.queued_actions.keys().rev().cloned().collect();
        for action_info in action_infos {
            let mut worker_received_msg = false;
            while worker_received_msg == false {
                let awaited_action = self.queued_actions.get(action_info.as_ref()).unwrap();
                let worker = if let Some(worker) = self.workers.find_worker_for_action_mut(&awaited_action) {
                    worker
                } else {
                    // No worker found, break out of our loop.
                    break;
                };
                let worker_id = worker.id.clone();

                // Try to notify our worker of the new action to run, if it fails remove the worker from the
                // pool and try to find another worker.
                let notify_worker_result = worker.notify_update(WorkerUpdate::RunAction(action_info.clone()));
                if notify_worker_result.is_err() {
                    // Remove worker, as it is no longer receiving messages and let it try to find another worker.
                    self.immediate_evict_worker(&worker_id);
                    should_run_again = true;
                    continue;
                }

                // At this point everything looks good, so remove it from the queue and add it to active actions.
                let (action_info, mut awaited_action) = self.queued_actions.remove_entry(action_info.as_ref()).unwrap();
                Arc::make_mut(&mut awaited_action.current_state).stage = ActionStage::Executing;
                let send_result = awaited_action.notify_channel.send(awaited_action.current_state.clone());
                if send_result.is_err() {
                    // Don't remove this task, instead we keep them around for a bit just in case
                    // the client disconnected and will reconnect and ask for same job to be executed
                    // again.
                    log::warn!(
                        "Action {} has no more listeners",
                        awaited_action.action_info.digest.str()
                    );
                }
                self.active_actions.insert(
                    action_info.clone(),
                    RunningAction {
                        _worker_id: worker_id,
                        action: awaited_action,
                    },
                );
                worker_received_msg = true;
            }
        }
        should_run_again
    }
}

/// Engine used to manage the queued/running tasks and relationship with
/// the worker nodes. All state on how the workers and actions are interacting
/// should be held in this struct.
pub struct Scheduler {
    inner: Mutex<SchedulerImpl>,
}

impl Scheduler {
    pub fn new(worker_timeout_s: u64) -> Self {
        Self {
            inner: Mutex::new(SchedulerImpl {
                queued_actions_set: HashSet::new(),
                queued_actions: BTreeMap::new(),
                workers: Workers::new(),
                active_actions: HashMap::new(),
                worker_timeout_s,
            }),
        }
    }

    /// Adds a worker to the scheduler and begin using it to execute actions (when able).
    pub async fn add_worker(&self, worker: Worker) -> Result<(), Error> {
        let worker_id = worker.id.clone();
        let mut inner = self.inner.lock().await;
        let res = inner
            .workers
            .add_worker(worker)
            .err_tip(|| "Error while adding worker, removing from pool");
        if res.is_err() {
            inner.immediate_evict_worker(&worker_id);
            return res;
        }
        inner.do_try_match();
        Ok(())
    }

    /// Adds an action to the scheduler for remote execution.
    pub async fn add_action(&self, action_info: ActionInfo) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        let mut inner = self.inner.lock().await;
        inner.add_action(action_info)
    }

    /// Checks to see if the worker exists in the worker pool. Should only be used in unit tests.
    pub async fn contains_worker_for_test(&self, worker_id: &WorkerId) -> bool {
        let inner = self.inner.lock().await;
        inner.workers.workers.contains(worker_id)
    }

    /// Event for when the keep alive message was received from the worker.
    pub async fn worker_keep_alive_received(
        &self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;
        inner
            .workers
            .refresh_lifetime(worker_id, timestamp)
            .err_tip(|| "Error refreshing lifetime in worker_keep_alive_received()")
    }

    pub async fn remove_timedout_workers(&self, now_timestamp: WorkerTimestamp) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;
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
        for worker_id in worker_ids_to_remove {
            log::warn!("Worker {} timed out, removing from pool", worker_id);
            inner.immediate_evict_worker(&worker_id);
        }
        inner.do_try_match();
        Ok(())
    }

    /// A unit test function used to send the keep alive message to the worker from the server.
    pub async fn send_keep_alive_to_worker_for_test(&self, worker_id: &WorkerId) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;
        let worker = inner
            .workers
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| make_input_err!("WorkerId '{}' does not exist in workers map", worker_id))?;
        worker.keep_alive()
    }
}
