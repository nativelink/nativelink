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

use std::str::FromStr;
use std::sync::Arc;

use futures::StreamExt;
use nativelink_error::{make_input_err, Error, ResultExt};
use nativelink_store::redis_store::RedisStore;
use nativelink_util::action_messages::{
    ActionInfo, ActionInfoHashKey, ActionStage, OperationId, WorkerId,
};
use nativelink_util::store_trait::{StoreDriver, StoreLike};
use redis::aio::{ConnectionLike, ConnectionManager};
use redis::{AsyncCommands, Pipeline};
use tonic::async_trait;

use crate::operation_state_manager::{
    ActionStateResult, ActionStateResultStream, ClientStateManager, MatchingEngineStateManager,
    OperationFilter, WorkerStateManager,
};
use crate::scheduler_state::operation_state::{OperationState, OperationStateInfo};

#[inline]
fn build_action_key(unique_qualifier: &ActionInfoHashKey) -> String {
    format!("actions:{}", unique_qualifier.action_name())
}

#[inline]
fn build_operations_key(operation_id: &OperationId) -> String {
    format!("operations:{operation_id}")
}

pub struct RedisStateManager<
    T: ConnectionLike + Unpin + Clone + Send + Sync + 'static = ConnectionManager,
> {
    store: Arc<RedisStore<T>>,
}

impl<T: ConnectionLike + Unpin + Clone + Send + Sync + 'static> RedisStateManager<T> {
    pub fn _new(store: Arc<RedisStore<T>>) -> Self {
        Self { store }
    }

    pub async fn get_conn(&self) -> Result<T, Error> {
        self.store.get_conn().await
    }

    async fn list<'a, V>(
        &self,
        prefix: &str,
        handler: impl Fn(String, String) -> Result<V, Error>,
    ) -> Result<Vec<V>, Error>
    where
        V: Send + Sync,
    {
        let mut con = self
            .get_conn()
            .await
            .err_tip(|| "In RedisStateManager::list")?;
        let ids_iter = con
            .scan_match::<&str, String>(prefix)
            .await
            .err_tip(|| "In RedisStateManager::list")?;
        let keys = ids_iter.collect::<Vec<String>>().await;
        let raw_values: Vec<String> = con
            .get(&keys)
            .await
            .err_tip(|| "In RedisStateManager::list")?;
        keys.into_iter()
            .zip(raw_values.into_iter())
            .map(|(k, v)| handler(k, v))
            .collect()
    }

    async fn inner_add_action(
        &self,
        action_info: ActionInfo,
    ) -> Result<Arc<dyn ActionStateResult>, Error> {
        let operation_id = OperationId::new(action_info.unique_qualifier.clone());
        let mut con = self
            .get_conn()
            .await
            .err_tip(|| "In RedisStateManager::inner_add_action")?;
        let action_key = build_action_key(&operation_id.unique_qualifier);
        // TODO: List API call to find existing actions.
        let mut existing_operations: Vec<OperationId> = Vec::new();
        let operation = match existing_operations.pop() {
            Some(existing_operation) => {
                let operations_key = build_operations_key(&existing_operation);
                let operation: OperationStateInfo = con
                    .get(operations_key)
                    .await
                    .err_tip(|| "In RedisStateManager::inner_add_action")?;
                OperationStateInfo::from_existing(operation.clone(), operation_id.clone())
            }
            None => OperationStateInfo::new(action_info, operation_id.clone()),
        };

        let operation_key = build_operations_key(&operation_id);

        // The values being stored in redis are pretty small so we can do our uploads as oneshots.
        // We do not parallelize these uploads since we should always upload an operation followed by the action,
        let store = self.store.as_store_driver_pin();
        store
            .update_oneshot(operation_key.clone().into(), operation.as_json().into())
            .await
            .err_tip(|| "In RedisStateManager::inner_add_action")?;
        store
            .update_oneshot(action_key.into(), operation_id.to_string().into())
            .await
            .err_tip(|| "In RedisStateManager::inner_add_action")?;

        let store_subscription = self.store.clone().subscribe(operation_key.into()).await;
        let state = OperationState::new(Arc::new(operation), store_subscription);
        Ok(Arc::new(state))
    }

    async fn inner_filter_operations(
        &self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream, Error> {
        let handler = &|k: String, v: String| -> Result<(String, Arc<OperationStateInfo>), Error> {
            let operation = Arc::new(
                OperationStateInfo::from_str(&v)
                    .err_tip(|| "In RedisStateManager::inner_filter_operations")?,
            );
            Ok((k, operation))
        };
        let existing_operations: Vec<(String, Arc<OperationStateInfo>)> = self
            .list("operations:*", &handler)
            .await
            .err_tip(|| "In RedisStateManager::inner_filter_operations")?;
        let mut v: Vec<Arc<dyn ActionStateResult>> = Vec::new();
        for (key, operation) in existing_operations.into_iter() {
            if operation.matches_filter(&filter) {
                let store_subscription = self.store.clone().subscribe(key.into()).await;
                v.push(Arc::new(OperationState::new(operation, store_subscription)));
            }
        }
        Ok(Box::pin(futures::stream::iter(v)))
    }

    async fn inner_update_operation(
        &self,
        operation_id: OperationId,
        worker_id: Option<WorkerId>,
        action_stage: Result<ActionStage, Error>,
    ) -> Result<(), Error> {
        let store = self.store.as_store_driver_pin();
        let key = format!("operations:{operation_id}");
        let operation_bytes_res = &store.get_part_unchunked(key.clone().into(), 0, None).await;
        let Ok(operation_bytes) = operation_bytes_res else {
            return Err(make_input_err!("Received request to update operation {operation_id}, but operation does not exist."));
        };

        let mut operation = OperationStateInfo::from_slice(&operation_bytes[..])
            .err_tip(|| "In RedisStateManager::inner_update_operation")?;
        match action_stage {
            Ok(stage) => {
                operation.stage = stage
                    .try_into()
                    .err_tip(|| "In RedisStateManager::inner_update_operation")?;
            }
            Err(e) => operation.last_error = Some(e),
        }

        operation.worker_id = worker_id;
        store
            .update_oneshot(key.into(), operation.as_json().into())
            .await
    }

    // TODO: This should be done through store but API endpoint does not exist yet.
    async fn inner_remove_operation(&self, operation_id: OperationId) -> Result<(), Error> {
        let mut con = self
            .get_conn()
            .await
            .err_tip(|| "In RedisStateManager::inner_remove_operation")?;
        let mut pipe = Pipeline::new();
        Ok(pipe
            .del(format!("operations:{operation_id}"))
            .query_async(&mut con)
            .await?)
    }
}

#[async_trait]
impl ClientStateManager for RedisStateManager {
    async fn add_action(
        &mut self,
        action_info: ActionInfo,
    ) -> Result<Arc<dyn ActionStateResult>, Error> {
        self.inner_add_action(action_info).await
    }

    async fn filter_operations(
        &self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream, Error> {
        self.inner_filter_operations(filter).await
    }
}

#[async_trait]
impl WorkerStateManager for RedisStateManager {
    async fn update_operation(
        &mut self,
        operation_id: OperationId,
        worker_id: WorkerId,
        action_stage: Result<ActionStage, Error>,
    ) -> Result<(), Error> {
        self.inner_update_operation(operation_id, Some(worker_id), action_stage)
            .await
    }
}

#[async_trait]
impl MatchingEngineStateManager for RedisStateManager {
    async fn filter_operations(
        &self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream, Error> {
        self.inner_filter_operations(filter).await
    }

    async fn update_operation(
        &mut self,
        operation_id: OperationId,
        worker_id: Option<WorkerId>,
        action_stage: Result<ActionStage, Error>,
    ) -> Result<(), Error> {
        self.inner_update_operation(operation_id, worker_id, action_stage)
            .await
    }

    async fn remove_operation(&self, operation_id: OperationId) -> Result<(), Error> {
        self.inner_remove_operation(operation_id).await
    }
}
