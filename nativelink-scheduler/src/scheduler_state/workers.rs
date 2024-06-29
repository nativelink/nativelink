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

use std::sync::Arc;

use lru::LruCache;
use nativelink_config::schedulers::WorkerAllocationStrategy;
use nativelink_error::{error_if, make_input_err, Error, ResultExt};
use nativelink_util::action_messages::WorkerId;
use nativelink_util::platform_properties::PlatformProperties;
use tokio::sync::Notify;
use tracing::{event, Level};

use crate::worker::{Worker, WorkerTimestamp};

/// A collection of workers that are available to run tasks.
pub struct Workers {
    /// A `LruCache` of workers availabled based on `allocation_strategy`.
    pub(crate) workers: LruCache<WorkerId, Worker>,
    /// The allocation strategy for workers.
    pub(crate) allocation_strategy: WorkerAllocationStrategy,
    /// A channel to notify the matching engine that the worker pool has changed.
    pub(crate) worker_change_notify: Arc<Notify>,
}

impl Workers {
    pub(crate) fn new(
        allocation_strategy: WorkerAllocationStrategy,
        worker_change_notify: Arc<Notify>,
    ) -> Self {
        Self {
            workers: LruCache::unbounded(),
            allocation_strategy,
            worker_change_notify,
        }
    }

    /// Refreshes the lifetime of the worker with the given timestamp.
    pub(crate) fn refresh_lifetime(
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
    pub(crate) fn add_worker(&mut self, worker: Worker) -> Result<(), Error> {
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
        self.worker_change_notify.notify_one();
        res
    }

    /// Removes worker from pool.
    /// Note: The caller is responsible for any rescheduling of any tasks that might be
    /// running.
    pub(crate) fn remove_worker(&mut self, worker_id: &WorkerId) -> Option<Worker> {
        let result = self.workers.pop(worker_id);
        self.worker_change_notify.notify_one();
        result
    }

    // Attempts to find a worker that is capable of running this action.
    // TODO(blaise.bruer) This algorithm is not very efficient. Simple testing using a tree-like
    // structure showed worse performance on a 10_000 worker * 7 properties * 1000 queued tasks
    // simulation of worst cases in a single threaded environment.
    pub(crate) fn find_worker_for_action(
        &self,
        platform_properties: &PlatformProperties,
    ) -> Option<WorkerId> {
        let mut workers_iter = self.workers.iter();
        let workers_iter = match self.allocation_strategy {
            // Use rfind to get the least recently used that satisfies the properties.
            WorkerAllocationStrategy::least_recently_used => workers_iter.rfind(|(_, w)| {
                w.can_accept_work() && platform_properties.is_satisfied_by(&w.platform_properties)
            }),
            // Use find to get the most recently used that satisfies the properties.
            WorkerAllocationStrategy::most_recently_used => workers_iter.find(|(_, w)| {
                w.can_accept_work() && platform_properties.is_satisfied_by(&w.platform_properties)
            }),
        };
        workers_iter.map(|(_, w)| &w.id).copied()
    }
}
