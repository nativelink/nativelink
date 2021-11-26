// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::cmp;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use fast_async_mutex::mutex::Mutex;
use rand::{thread_rng, Rng};
use tokio::sync::watch;

use action_messages::{ActionInfo, ActionStage, ActionState};
use common::log;
use error::{Error, ResultExt};
use worker::{Worker, WorkerId, WorkerUpdate};

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
    workers: HashMap<WorkerId, Worker>,
}

impl Workers {
    fn new() -> Self {
        Self {
            workers: HashMap::new(),
        }
    }

    /// Adds a worker to the pool.
    /// Note: This function will not do any task matching.
    fn add_worker(&mut self, worker: Worker) {
        self.workers.insert(worker.id.clone(), worker);
    }

    /// Removes worker from pool.
    /// Note: The caller is responsible for any rescheduling of any tasks that might be
    /// running.
    fn remove_worker(&mut self, worker_id: &WorkerId) {
        self.workers.remove(worker_id);
    }

    /// Attempts to find a worker that is capable of running this action.
    // TODO(blaise.bruer) This algorithm is not very efficient. Simple testing using a tree-like
    // structure showed worse performance on a 10_000 worker * 7 properties * 1000 queued tasks
    // simulation of worst cases in a single threaded environment.
    fn find_worker_for_action_mut<'a>(&'a mut self, awaited_action: &AwaitedAction) -> Option<&'a mut Worker> {
        assert!(matches!(awaited_action.current_state.stage, ActionStage::Queued));
        let action_properties = &awaited_action.action_info.platform_properties;
        return self
            .workers
            .values_mut()
            .find(|w| action_properties.is_satisfied_by(&w.platform_properties));
    }
}

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

    // TODO(blaise.bruer) This is an O(n*m) (aka n^2) algorithm. In theory we can create a map
    // of capabilities of each worker and then try and match the actions to the worker using
    // the map lookup (ie. map reduce).
    fn do_try_match(&mut self) {
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
                    self.workers.remove_worker(&worker_id);
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
    }
}

/// Engine used to manage the queued/running tasks and relationship with
/// the worker nodes. All state on how the workers and actions are interacting
/// should be held in this struct.
pub struct Scheduler {
    inner: Arc<Mutex<SchedulerImpl>>,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SchedulerImpl {
                queued_actions_set: HashSet::new(),
                queued_actions: BTreeMap::new(),
                workers: Workers::new(),
                active_actions: HashMap::new(),
            })),
        }
    }

    pub async fn add_worker(&self, worker: Worker) {
        let mut inner = self.inner.lock().await;
        inner.workers.add_worker(worker);
        inner.do_try_match();
    }

    /// Adds an action to the scheduler for remote execution.
    pub async fn add_action(&self, action_info: ActionInfo) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        let mut inner = self.inner.lock().await;
        inner.add_action(action_info)
    }
}
