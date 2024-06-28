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
use std::time::SystemTime;

use futures::{join, StreamExt};
use nativelink_error::{make_input_err, Error, ResultExt};
use nativelink_store::redis_store::RedisStore;
use nativelink_util::action_messages::{
    ActionInfo, ActionInfoHashKey, ActionStage, ActionState, OperationId, WorkerId,
};
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::spawn;
use nativelink_util::store_trait::{StoreDriver, StoreLike, StoreSubscription};
use nativelink_util::task::JoinHandleDropGuard;
use redis::aio::{ConnectionLike, ConnectionManager};
use redis::{AsyncCommands, Pipeline};
use redis_macros::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tonic::async_trait;
use tracing::{event, Level};

use crate::operation_state_manager::{
    ActionStateResult, ActionStateResultStream, ClientStateManager, MatchingEngineStateManager,
    OperationFilter, WorkerStateManager,
};
use crate::redis_action_stage::RedisOperationStage;

#[inline]
fn build_action_key(unique_qualifier: &ActionInfoHashKey) -> String {
    format!("actions:{}", unique_qualifier.action_name())
}

#[inline]
fn build_operations_key(operation_id: &OperationId) -> String {
    format!("operations:{operation_id}")
}

pub struct RedisOperationState {
    rx: watch::Receiver<Arc<ActionState>>,
    inner: Arc<RedisOperation>,
    _join_handle: JoinHandleDropGuard<()>,
}

impl RedisOperationState {
    fn new(
        inner: Arc<RedisOperation>,
        mut operation_subscription: Box<dyn StoreSubscription>,
    ) -> Self {
        let (tx, rx) = watch::channel(inner.as_state());

        let _join_handle = spawn!("redis_subscription_watcher", async move {
            loop {
                let Ok(item) = operation_subscription.changed().await else {
                    // This might occur if the store subscription is dropped
                    // or if there is an error fetching the data.
                    return;
                };
                let (mut data_tx, mut data_rx) = make_buf_channel_pair();
                let (get_res, data_res) = join!(
                    // We use async move because we want to transfer ownership of data_tx into the closure.
                    // That way if join! selects data_rx.consume(None) because get fails,
                    // data_tx goes out of scope and will be dropped.
                    async move { item.get(&mut data_tx).await },
                    data_rx.consume(None)
                );

                let res = get_res
                    .merge(data_res)
                    .and_then(|data| {
                        RedisOperation::from_slice(&data[..])
                            .err_tip(|| "Error while Publishing RedisSubscription")
                    })
                    .map(|redis_operation| {
                        tx.send_modify(move |cur_state| *cur_state = redis_operation.as_state())
                    });
                if let Err(e) = res {
                    // TODO: Refactor API to allow error to be propogated to client.
                    event!(
                        Level::ERROR,
                        ?e,
                        "Error During Redis Operation Subscription",
                    );
                    return;
                }
            }
        });
        Self {
            rx,
            _join_handle,
            inner,
        }
    }
}

#[async_trait]
impl ActionStateResult for RedisOperationState {
    async fn as_state(&self) -> Result<Arc<ActionState>, Error> {
        Ok(Arc::new(ActionState::from(self.inner.as_ref())))
    }

    async fn as_receiver(&self) -> Result<&'_ watch::Receiver<Arc<ActionState>>, Error> {
        Ok(&self.rx)
    }

    async fn as_action_info(&self) -> Result<Arc<ActionInfo>, Error> {
        Ok(Arc::new(self.inner.info.clone()))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToRedisArgs, FromRedisValue)]
pub struct RedisOperation {
    operation_id: OperationId,
    info: ActionInfo,
    worker_id: Option<WorkerId>,
    stage: RedisOperationStage,
    last_worker_update: Option<SystemTime>,
    last_client_update: Option<SystemTime>,
    last_error: Option<Error>,
    completed_at: Option<SystemTime>,
}

impl RedisOperation {
    pub fn as_json(&self) -> String {
        serde_json::json!(&self).to_string()
    }

    pub fn from_slice(s: &[u8]) -> Result<Self, Error> {
        serde_json::from_slice(s).map_err(|e| {
            make_input_err!("Create RedisOperation from slice failed with Error - {e:?}")
        })
    }

    pub fn new(info: ActionInfo, operation_id: OperationId) -> Self {
        Self {
            operation_id,
            info,
            worker_id: None,
            stage: RedisOperationStage::CacheCheck,
            last_worker_update: None,
            last_client_update: None,
            last_error: None,
            completed_at: None,
        }
    }

    pub fn from_existing(existing: RedisOperation, operation_id: OperationId) -> Self {
        Self {
            operation_id,
            info: existing.info,
            worker_id: existing.worker_id,
            stage: existing.stage,
            last_worker_update: existing.last_worker_update,
            last_client_update: existing.last_client_update,
            last_error: existing.last_error,
            completed_at: existing.completed_at,
        }
    }

    pub fn as_state(&self) -> Arc<ActionState> {
        let action_state = ActionState {
            stage: self.stage.clone().into(),
            id: self.operation_id.clone(),
        };
        Arc::new(action_state)
    }

    pub fn unique_qualifier(&self) -> &ActionInfoHashKey {
        &self.operation_id.unique_qualifier
    }

    pub fn matches_filter(&self, filter: &OperationFilter) -> bool {
        // If the filter value is None, we can match anything and return true.
        // If the filter value is Some and the value is None, it can't be a match so we return false.
        // If both values are Some, we compare to determine if there is a match.
        let matches_stage_filter = filter.stages.contains(self.stage.as_state_flag());
        if !matches_stage_filter {
            return false;
        }

        let matches_operation_filter = filter
            .operation_id
            .as_ref()
            .map_or(true, |id| &self.operation_id == id);
        if !matches_operation_filter {
            return false;
        }

        let matches_worker_filter = self.worker_id == filter.worker_id;
        if !matches_worker_filter {
            return false;
        };

        let matches_digest_filter = filter
            .action_digest
            .map_or(true, |digest| self.unique_qualifier().digest == digest);
        if !matches_digest_filter {
            return false;
        };

        let matches_completed_before = filter.completed_before.map_or(true, |before| {
            self.completed_at
                .map_or(false, |completed_at| completed_at < before)
        });
        if !matches_completed_before {
            return false;
        };

        let matches_last_update = filter.last_client_update_before.map_or(true, |before| {
            self.last_client_update
                .map_or(false, |last_update| last_update < before)
        });
        if !matches_last_update {
            return false;
        };

        true
    }
}

impl FromStr for RedisOperation {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(|e| {
            make_input_err!(
                "Decode string {s} to RedisOperation failed with error: {}",
                e.to_string()
            )
        })
    }
}

impl From<&RedisOperation> for ActionState {
    fn from(value: &RedisOperation) -> Self {
        ActionState {
            id: value.operation_id.clone(),
            stage: value.stage.clone().into(),
        }
    }
}

pub struct RedisStateManager<
    T: ConnectionLike + Unpin + Clone + Send + Sync + 'static = ConnectionManager,
> {
    store: Arc<RedisStore<T>>,
}

impl<T: ConnectionLike + Unpin + Clone + Send + Sync + 'static> RedisStateManager<T> {
    pub fn new(store: Arc<RedisStore<T>>) -> Self {
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
                let operation: RedisOperation = con
                    .get(operations_key)
                    .await
                    .err_tip(|| "In RedisStateManager::inner_add_action")?;
                RedisOperation::from_existing(operation.clone(), operation_id.clone())
            }
            None => RedisOperation::new(action_info, operation_id.clone()),
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
        let state = RedisOperationState::new(Arc::new(operation), store_subscription);
        Ok(Arc::new(state))
    }

    async fn inner_filter_operations(
        &self,
        filter: OperationFilter,
    ) -> Result<ActionStateResultStream, Error> {
        let handler = &|k: String, v: String| -> Result<(String, Arc<RedisOperation>), Error> {
            let operation = Arc::new(
                RedisOperation::from_str(&v)
                    .err_tip(|| "In RedisStateManager::inner_filter_operations")?,
            );
            Ok((k, operation))
        };
        let existing_operations: Vec<(String, Arc<RedisOperation>)> = self
            .list("operations:*", &handler)
            .await
            .err_tip(|| "In RedisStateManager::inner_filter_operations")?;
        let mut v: Vec<Arc<dyn ActionStateResult>> = Vec::new();
        for (key, operation) in existing_operations.into_iter() {
            if operation.matches_filter(&filter) {
                let store_subscription = self.store.clone().subscribe(key.into()).await;
                v.push(Arc::new(RedisOperationState::new(
                    operation,
                    store_subscription,
                )));
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

        let mut operation = RedisOperation::from_slice(&operation_bytes[..])
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
        &self,
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
        &self,
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
        &self,
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
