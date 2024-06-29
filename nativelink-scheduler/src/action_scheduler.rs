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

use std::sync::Arc;

use async_trait::async_trait;
use nativelink_error::Error;
use nativelink_util::action_messages::{ActionInfo, ActionInfoHashKey, ActionState};
use nativelink_util::metrics_utils::Registry;
use tokio::sync::watch;

use crate::platform_property_manager::PlatformPropertyManager;

/// ActionScheduler interface is responsible for interactions between the scheduler
/// and action related operations.
#[async_trait]
pub trait ActionScheduler: Sync + Send + Unpin {
    /// Returns the platform property manager.
    async fn get_platform_property_manager(
        &self,
        instance_name: &str,
    ) -> Result<Arc<PlatformPropertyManager>, Error>;

    /// Adds an action to the scheduler for remote execution.
    async fn add_action(
        &self,
        action_info: ActionInfo,
    ) -> Result<watch::Receiver<Arc<ActionState>>, Error>;

    /// Find an existing action by its name.
    async fn find_existing_action(
        &self,
        unique_qualifier: &ActionInfoHashKey,
    ) -> Result<Option<watch::Receiver<Arc<ActionState>>>, Error>;

    /// Cleans up the cache of recently completed actions.
    async fn clean_recently_completed_actions(&self);

    /// Register the metrics for the action scheduler.
    fn register_metrics(self: Arc<Self>, _registry: &mut Registry) {}
}
