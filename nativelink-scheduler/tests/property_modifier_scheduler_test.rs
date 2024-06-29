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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

mod utils {
    pub(crate) mod mock_scheduler;
    pub(crate) mod scheduler_utils;
}

use futures::join;
use nativelink_config::schedulers::{PlatformPropertyAddition, PropertyModification, PropertyType};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_scheduler::action_scheduler::ActionScheduler;
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_scheduler::property_modifier_scheduler::PropertyModifierScheduler;
use nativelink_util::action_messages::{ActionInfoHashKey, ActionStage, ActionState, OperationId};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::platform_properties::PlatformPropertyValue;
use pretty_assertions::assert_eq;
use tokio::sync::watch;
use utils::mock_scheduler::MockActionScheduler;
use utils::scheduler_utils::{make_base_action_info, INSTANCE_NAME};

struct TestContext {
    mock_scheduler: Arc<MockActionScheduler>,
    modifier_scheduler: PropertyModifierScheduler,
}

fn make_modifier_scheduler(modifications: Vec<PropertyModification>) -> TestContext {
    let mock_scheduler = Arc::new(MockActionScheduler::new());
    let config = nativelink_config::schedulers::PropertyModifierScheduler {
        modifications,
        scheduler: Box::new(nativelink_config::schedulers::SchedulerConfig::simple(
            nativelink_config::schedulers::SimpleScheduler::default(),
        )),
    };
    let modifier_scheduler = PropertyModifierScheduler::new(&config, mock_scheduler.clone());
    TestContext {
        mock_scheduler,
        modifier_scheduler,
    }
}

#[nativelink_test]
async fn add_action_adds_property() -> Result<(), Error> {
    let name = "name".to_string();
    let value = "value".to_string();
    let context =
        make_modifier_scheduler(vec![PropertyModification::add(PlatformPropertyAddition {
            name: name.clone(),
            value: value.clone(),
        })]);
    let action_info = make_base_action_info(UNIX_EPOCH);
    let (_forward_watch_channel_tx, forward_watch_channel_rx) =
        watch::channel(Arc::new(ActionState {
            id: OperationId::new(action_info.unique_qualifier.clone()),
            stage: ActionStage::Queued,
        }));
    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::from([(
        name.clone(),
        PropertyType::exact,
    )])));
    let (_, _, action_info) = join!(
        context.modifier_scheduler.add_action(action_info),
        context
            .mock_scheduler
            .expect_get_platform_property_manager(Ok(platform_property_manager)),
        context
            .mock_scheduler
            .expect_add_action(Ok(forward_watch_channel_rx)),
    );
    assert_eq!(
        HashMap::from([(name, PlatformPropertyValue::Exact(value))]),
        action_info.platform_properties.properties
    );
    Ok(())
}

#[nativelink_test]
async fn add_action_overwrites_property() -> Result<(), Error> {
    let name = "name".to_string();
    let original_value = "value".to_string();
    let replaced_value = "replaced".to_string();
    let context =
        make_modifier_scheduler(vec![PropertyModification::add(PlatformPropertyAddition {
            name: name.clone(),
            value: replaced_value.clone(),
        })]);
    let mut action_info = make_base_action_info(UNIX_EPOCH);
    action_info
        .platform_properties
        .properties
        .insert(name.clone(), PlatformPropertyValue::Unknown(original_value));
    let (_forward_watch_channel_tx, forward_watch_channel_rx) =
        watch::channel(Arc::new(ActionState {
            id: OperationId::new(action_info.unique_qualifier.clone()),
            stage: ActionStage::Queued,
        }));
    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::from([(
        name.clone(),
        PropertyType::exact,
    )])));
    let (_, _, action_info) = join!(
        context.modifier_scheduler.add_action(action_info),
        context
            .mock_scheduler
            .expect_get_platform_property_manager(Ok(platform_property_manager)),
        context
            .mock_scheduler
            .expect_add_action(Ok(forward_watch_channel_rx)),
    );
    assert_eq!(
        HashMap::from([(name, PlatformPropertyValue::Exact(replaced_value))]),
        action_info.platform_properties.properties
    );
    Ok(())
}

#[nativelink_test]
async fn add_action_property_added_after_remove() -> Result<(), Error> {
    let name = "name".to_string();
    let value = "value".to_string();
    let context = make_modifier_scheduler(vec![
        PropertyModification::remove(name.clone()),
        PropertyModification::add(PlatformPropertyAddition {
            name: name.clone(),
            value: value.clone(),
        }),
    ]);
    let action_info = make_base_action_info(UNIX_EPOCH);
    let (_forward_watch_channel_tx, forward_watch_channel_rx) =
        watch::channel(Arc::new(ActionState {
            id: OperationId::new(action_info.unique_qualifier.clone()),
            stage: ActionStage::Queued,
        }));
    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::from([(
        name.clone(),
        PropertyType::exact,
    )])));
    let (_, _, action_info) = join!(
        context.modifier_scheduler.add_action(action_info),
        context
            .mock_scheduler
            .expect_get_platform_property_manager(Ok(platform_property_manager)),
        context
            .mock_scheduler
            .expect_add_action(Ok(forward_watch_channel_rx)),
    );
    assert_eq!(
        HashMap::from([(name, PlatformPropertyValue::Exact(value))]),
        action_info.platform_properties.properties
    );
    Ok(())
}

#[nativelink_test]
async fn add_action_property_remove_after_add() -> Result<(), Error> {
    let name = "name".to_string();
    let value = "value".to_string();
    let context = make_modifier_scheduler(vec![
        PropertyModification::add(PlatformPropertyAddition {
            name: name.clone(),
            value: value.clone(),
        }),
        PropertyModification::remove(name.clone()),
    ]);
    let action_info = make_base_action_info(UNIX_EPOCH);
    let (_forward_watch_channel_tx, forward_watch_channel_rx) =
        watch::channel(Arc::new(ActionState {
            id: OperationId::new(action_info.unique_qualifier.clone()),
            stage: ActionStage::Queued,
        }));
    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::from([(
        name,
        PropertyType::exact,
    )])));
    let (_, _, action_info) = join!(
        context.modifier_scheduler.add_action(action_info),
        context
            .mock_scheduler
            .expect_get_platform_property_manager(Ok(platform_property_manager)),
        context
            .mock_scheduler
            .expect_add_action(Ok(forward_watch_channel_rx)),
    );
    assert_eq!(
        HashMap::from([]),
        action_info.platform_properties.properties
    );
    Ok(())
}

#[nativelink_test]
async fn add_action_property_remove() -> Result<(), Error> {
    let name = "name".to_string();
    let value = "value".to_string();
    let context = make_modifier_scheduler(vec![PropertyModification::remove(name.clone())]);
    let mut action_info = make_base_action_info(UNIX_EPOCH);
    action_info
        .platform_properties
        .properties
        .insert(name, PlatformPropertyValue::Unknown(value));
    let (_forward_watch_channel_tx, forward_watch_channel_rx) =
        watch::channel(Arc::new(ActionState {
            id: OperationId::new(action_info.unique_qualifier.clone()),
            stage: ActionStage::Queued,
        }));
    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::new()));
    let (_, _, action_info) = join!(
        context.modifier_scheduler.add_action(action_info),
        context
            .mock_scheduler
            .expect_get_platform_property_manager(Ok(platform_property_manager)),
        context
            .mock_scheduler
            .expect_add_action(Ok(forward_watch_channel_rx)),
    );
    assert_eq!(
        HashMap::from([]),
        action_info.platform_properties.properties
    );
    Ok(())
}

#[nativelink_test]
async fn find_existing_action_call_passed() -> Result<(), Error> {
    let context = make_modifier_scheduler(vec![]);
    let action_name = ActionInfoHashKey {
        instance_name: "instance".to_string(),
        digest_function: DigestHasherFunc::Sha256,
        digest: DigestInfo::new([8; 32], 1),
        salt: 1000,
    };
    let (actual_result, actual_action_name) = join!(
        context
            .modifier_scheduler
            .find_existing_action(&action_name),
        context.mock_scheduler.expect_find_existing_action(Ok(None)),
    );
    assert_eq!(true, actual_result.unwrap().is_none());
    assert_eq!(action_name, actual_action_name);
    Ok(())
}

#[nativelink_test]
async fn remove_adds_to_underlying_manager() -> Result<(), Error> {
    let name = "name".to_string();
    let context = make_modifier_scheduler(vec![PropertyModification::remove(name.clone())]);
    let scheduler_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::new()));
    let get_property_manager_fut = context
        .mock_scheduler
        .expect_get_platform_property_manager(Ok(scheduler_property_manager));
    let property_manager_fut = context
        .modifier_scheduler
        .get_platform_property_manager(INSTANCE_NAME);
    let (actual_instance_name, property_manager) =
        join!(get_property_manager_fut, property_manager_fut);
    assert_eq!(
        HashMap::<_, _>::from_iter([(name, PropertyType::priority)]),
        *property_manager?.get_known_properties()
    );
    assert_eq!(actual_instance_name, INSTANCE_NAME);
    Ok(())
}

#[nativelink_test]
async fn remove_retains_type_in_underlying_manager() -> Result<(), Error> {
    let name = "name".to_string();
    let context = make_modifier_scheduler(vec![PropertyModification::remove(name.clone())]);
    let scheduler_property_manager =
        Arc::new(PlatformPropertyManager::new(HashMap::<_, _>::from_iter([
            (name.clone(), PropertyType::exact),
        ])));
    let get_property_manager_fut = context
        .mock_scheduler
        .expect_get_platform_property_manager(Ok(scheduler_property_manager));
    let property_manager_fut = context
        .modifier_scheduler
        .get_platform_property_manager(INSTANCE_NAME);
    let (_, property_manager) = join!(get_property_manager_fut, property_manager_fut);
    assert_eq!(
        HashMap::<_, _>::from_iter([(name, PropertyType::exact)]),
        *property_manager?.get_known_properties()
    );
    Ok(())
}
