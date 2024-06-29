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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use nativelink_config::schedulers::{PropertyModification, PropertyType};
use nativelink_error::{Error, ResultExt};
use nativelink_util::action_messages::{ActionInfo, ActionInfoHashKey, ActionState};
use nativelink_util::metrics_utils::Registry;
use parking_lot::Mutex;
use tokio::sync::watch;

use crate::action_scheduler::ActionScheduler;
use crate::platform_property_manager::PlatformPropertyManager;

pub struct PropertyModifierScheduler {
    modifications: Vec<nativelink_config::schedulers::PropertyModification>,
    scheduler: Arc<dyn ActionScheduler>,
    property_managers: Mutex<HashMap<String, Arc<PlatformPropertyManager>>>,
}

impl PropertyModifierScheduler {
    pub fn new(
        config: &nativelink_config::schedulers::PropertyModifierScheduler,
        scheduler: Arc<dyn ActionScheduler>,
    ) -> Self {
        Self {
            modifications: config.modifications.clone(),
            scheduler,
            property_managers: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl ActionScheduler for PropertyModifierScheduler {
    async fn get_platform_property_manager(
        &self,
        instance_name: &str,
    ) -> Result<Arc<PlatformPropertyManager>, Error> {
        {
            let property_managers = self.property_managers.lock();
            if let Some(property_manager) = property_managers.get(instance_name) {
                return Ok(property_manager.clone());
            }
        }
        let property_manager = self
            .scheduler
            .get_platform_property_manager(instance_name)
            .await?;
        let mut known_properties = property_manager.get_known_properties().clone();
        for modification in &self.modifications {
            match modification {
                PropertyModification::remove(name) => {
                    known_properties
                        .entry(name.into())
                        .or_insert(PropertyType::priority);
                }
                PropertyModification::add(_) => (),
            }
        }
        let property_manager = {
            let mut property_managers = self.property_managers.lock();
            match property_managers.entry(instance_name.into()) {
                Entry::Vacant(new_entry) => {
                    let property_manager = Arc::new(PlatformPropertyManager::new(known_properties));
                    new_entry.insert(property_manager.clone());
                    property_manager
                }
                // We lost the race, use the other manager.
                Entry::Occupied(old_entry) => old_entry.get().clone(),
            }
        };
        Ok(property_manager)
    }

    async fn add_action(
        &self,
        mut action_info: ActionInfo,
    ) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
        let platform_property_manager = self
            .get_platform_property_manager(&action_info.unique_qualifier.instance_name)
            .await
            .err_tip(|| "In PropertyModifierScheduler::add_action")?;
        for modification in &self.modifications {
            match modification {
                PropertyModification::add(addition) => {
                    action_info.platform_properties.properties.insert(
                        addition.name.clone(),
                        platform_property_manager
                            .make_prop_value(&addition.name, &addition.value)
                            .err_tip(|| "In PropertyModifierScheduler::add_action")?,
                    )
                }
                PropertyModification::remove(name) => {
                    action_info.platform_properties.properties.remove(name)
                }
            };
        }
        self.scheduler.add_action(action_info).await
    }

    async fn find_existing_action(
        &self,
        unique_qualifier: &ActionInfoHashKey,
    ) -> Result<Option<watch::Receiver<Arc<ActionState>>>, Error> {
        self.scheduler.find_existing_action(unique_qualifier).await
    }

    async fn clean_recently_completed_actions(&self) {
        self.scheduler.clean_recently_completed_actions().await
    }

    // Register metrics for the underlying ActionScheduler.
    fn register_metrics(self: Arc<Self>, registry: &mut Registry) {
        let scheduler_registry = registry.sub_registry_with_prefix("property_modifier");
        self.scheduler.clone().register_metrics(scheduler_registry);
    }
}
