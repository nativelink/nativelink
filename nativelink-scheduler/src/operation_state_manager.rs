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

use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bitflags::bitflags;
use futures::Stream;
use nativelink_error::Error;
use nativelink_util::action_messages::{
    ActionInfo, ActionInfoHashKey, ActionStage, ActionState, OperationId, WorkerId,
};
use nativelink_util::common::DigestInfo;
use tokio::sync::watch;

bitflags! {
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct OperationStageFlags: u32 {
        const CacheCheck = 1 << 1;
        const Queued     = 1 << 2;
        const Executing  = 1 << 3;
        const Completed  = 1 << 4;
        const Any        = u32::MAX;
    }
}

#[async_trait]
pub trait ActionStateResult: Send + Sync + 'static {
    // Provides the current state of the action.
    async fn as_state(&self) -> Result<Arc<ActionState>, Error>;
    // Subscribes to the state of the action, receiving updates as they are published.
    async fn as_receiver(&self) -> Result<&'_ watch::Receiver<Arc<ActionState>>, Error>;
    // Provide result as action info. This behavior will not be supported by all implementations.
    // TODO(adams): Expectation is this to experimental and removed in the future.
    async fn as_action_info(&self) -> Result<Arc<ActionInfo>, Error>;
}

/// The filters used to query operations from the state manager.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OperationFilter {
    // TODO(adams): create rust builder pattern?
    /// The stage(s) that the operation must be in.
    pub stages: OperationStageFlags,

    /// The operation id.
    pub operation_id: Option<OperationId>,

    /// The worker that the operation must be assigned to.
    pub worker_id: Option<WorkerId>,

    /// The digest of the action that the operation must have.
    pub action_digest: Option<DigestInfo>,

    /// The operation must have it's worker timestamp before this time.
    pub worker_update_before: Option<SystemTime>,

    /// The operation must have been completed before this time.
    pub completed_before: Option<SystemTime>,

    /// The operation must have it's last client update before this time.
    pub last_client_update_before: Option<SystemTime>,

    /// The unique key for filtering specific action results.
    pub unique_qualifier: Option<ActionInfoHashKey>,

    /// The order by in which results are returned by the filter operation.
    pub order_by: Option<OrderBy>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OperationFields {
    Priority,
    Timestamp,
}

/// The order in which results are returned by the filter operation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderBy {
    /// The fields to order by, each field is ordered in the order they are provided.
    pub fields: Vec<OperationFields>,
    /// The order of the fields, true for descending, false for ascending.
    pub desc: bool,
}

pub type ActionStateResultStream = Pin<Box<dyn Stream<Item = Arc<dyn ActionStateResult>> + Send>>;

#[async_trait]
pub trait ClientStateManager {
    /// Add a new action to the queue or joins an existing action.
    async fn add_action(
        &self,
        action_info: ActionInfo,
    ) -> Result<Arc<dyn ActionStateResult>, Error>;

    /// Returns a stream of operations that match the filter.
    async fn filter_operations(
        &self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream, Error>;
}

#[async_trait]
pub trait WorkerStateManager {
    /// Update that state of an operation.
    /// The worker must also send periodic updates even if the state
    /// did not change with a modified timestamp in order to prevent
    /// the operation from being considered stale and being rescheduled.
    async fn update_operation(
        &self,
        operation_id: OperationId,
        worker_id: WorkerId,
        action_stage: Result<ActionStage, Error>,
    ) -> Result<(), Error>;
}

#[async_trait]
pub trait MatchingEngineStateManager {
    /// Returns a stream of operations that match the filter.
    async fn filter_operations(
        &self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream, Error>;

    /// Update that state of an operation.
    async fn update_operation(
        &self,
        operation_id: OperationId,
        worker_id: Option<WorkerId>,
        action_stage: Result<ActionStage, Error>,
    ) -> Result<(), Error>;

    /// Remove an operation from the state manager.
    /// It is important to use this function to remove operations
    /// that are no longer needed to prevent memory leaks.
    async fn remove_operation(&self, operation_id: OperationId) -> Result<(), Error>;
}
