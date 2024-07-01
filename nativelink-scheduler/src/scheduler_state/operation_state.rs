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

use futures::join;
use nativelink_error::{make_input_err, Error, ResultExt};
use nativelink_util::action_messages::{
    ActionInfo, ActionInfoHashKey, ActionResult, ActionStage, ActionState, OperationId, WorkerId,
};
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::spawn;
use nativelink_util::store_trait::StoreSubscription;
use nativelink_util::task::JoinHandleDropGuard;
use redis_macros::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tonic::async_trait;
use tracing::{event, Level};

use crate::operation_state_manager::{ActionStateResult, OperationFilter, OperationStageFlags};

#[inline]
pub fn build_action_key(unique_qualifier: &ActionInfoHashKey) -> String {
    format!("actions:{}", unique_qualifier.action_name())
}

#[inline]
pub fn build_operation_key(operation_id: &OperationId) -> String {
    format!("operations:{operation_id}")
}

#[derive(Serialize, Deserialize, Clone, Debug, ToRedisArgs, FromRedisValue)]
pub struct OperationStateInfo {
    pub operation_id: OperationId,
    pub info: ActionInfo,
    pub worker_id: Option<WorkerId>,
    pub stage: OperationStage,
    pub last_worker_update: Option<SystemTime>,
    pub last_client_update: Option<SystemTime>,
    pub last_error: Option<Error>,
    pub completed_at: Option<SystemTime>,
}

pub struct OperationState {
    rx: watch::Receiver<Arc<ActionState>>,
    inner: Arc<OperationStateInfo>,
    _join_handle: JoinHandleDropGuard<()>,
}

impl OperationState {
    pub fn new(
        inner: Arc<OperationStateInfo>,
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
                        OperationStateInfo::from_slice(&data[..])
                            .err_tip(|| "Error while Publishing Subscription")
                    })
                    .map(|redis_operation| {
                        tx.send_modify(move |cur_state| *cur_state = redis_operation.as_state())
                    });
                if let Err(e) = res {
                    // TODO: Refactor API to allow error to be propogated to client.
                    event!(Level::ERROR, ?e, "Error During  Operation Subscription",);
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
impl ActionStateResult for OperationState {
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

impl OperationStateInfo {
    pub fn as_json(&self) -> String {
        serde_json::json!(&self).to_string()
    }

    pub fn from_slice(s: &[u8]) -> Result<Self, Error> {
        serde_json::from_slice(s)
            .map_err(|e| make_input_err!("Create Operation from slice failed with Error - {e:?}"))
    }

    pub fn new(info: ActionInfo, operation_id: OperationId) -> Self {
        Self {
            operation_id,
            info,
            worker_id: None,
            stage: OperationStage::CacheCheck,
            last_worker_update: None,
            last_client_update: None,
            last_error: None,
            completed_at: None,
        }
    }

    pub fn from_existing(existing: OperationStateInfo, operation_id: OperationId) -> Self {
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

impl FromStr for OperationStateInfo {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(|e| {
            make_input_err!(
                "Decode string {s} to Operation failed with error: {}",
                e.to_string()
            )
        })
    }
}

impl From<&OperationStateInfo> for ActionState {
    fn from(value: &OperationStateInfo) -> Self {
        ActionState {
            id: value.operation_id.clone(),
            stage: value.stage.clone().into(),
        }
    }
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum OperationStage {
    CacheCheck,
    Queued,
    Executing,
    Completed(ActionResult),
    CompletedFromCache(ActionResult),
}

impl OperationStage {
    pub fn as_state_flag(&self) -> OperationStageFlags {
        match self {
            Self::CacheCheck => OperationStageFlags::CacheCheck,
            Self::Executing => OperationStageFlags::Executing,
            Self::Queued => OperationStageFlags::Queued,
            Self::Completed(_) => OperationStageFlags::Completed,
            Self::CompletedFromCache(_) => OperationStageFlags::Completed,
        }
    }
}

impl TryFrom<ActionStage> for OperationStage {
    type Error = Error;
    fn try_from(stage: ActionStage) -> Result<OperationStage, Error> {
        match stage {
            ActionStage::CacheCheck => Ok(OperationStage::CacheCheck),
            ActionStage::Queued => Ok(OperationStage::Queued),
            ActionStage::Executing => Ok(OperationStage::Executing),
            ActionStage::Completed(result) => Ok(OperationStage::Completed(result)),
            ActionStage::CompletedFromCache(proto_result) => {
                let decoded = ActionResult::try_from(proto_result)
                    .err_tip(|| "In OperationStage::try_from::<ActionStage>")?;
                Ok(OperationStage::Completed(decoded))
            }
            ActionStage::Unknown => Err(make_input_err!("ActionStage conversion to OperationStage failed with Error - Unknown is not a valid OperationStage")),
        }
    }
}

impl From<OperationStage> for ActionStage {
    fn from(stage: OperationStage) -> ActionStage {
        match stage {
            OperationStage::CacheCheck => ActionStage::CacheCheck,
            OperationStage::Queued => ActionStage::Queued,
            OperationStage::Executing => ActionStage::Executing,
            OperationStage::Completed(result) => ActionStage::Completed(result),
            OperationStage::CompletedFromCache(result) => {
                ActionStage::CompletedFromCache(result.into())
            }
        }
    }
}

impl From<&OperationStage> for ActionStage {
    fn from(stage: &OperationStage) -> Self {
        stage.clone().into()
    }
}
