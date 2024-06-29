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
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::ActionResult as ProtoActionResult;
use nativelink_scheduler::action_scheduler::ActionScheduler;
use nativelink_scheduler::cache_lookup_scheduler::CacheLookupScheduler;
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::action_messages::{
    ActionInfoHashKey, ActionResult, ActionStage, ActionState, OperationId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::store_trait::{Store, StoreLike};
use pretty_assertions::assert_eq;
use prost::Message;
use tokio::sync::watch;
use tokio::{self};
use utils::mock_scheduler::MockActionScheduler;
use utils::scheduler_utils::{make_base_action_info, INSTANCE_NAME};

struct TestContext {
    mock_scheduler: Arc<MockActionScheduler>,
    ac_store: Store,
    cache_scheduler: CacheLookupScheduler,
}

fn make_cache_scheduler() -> Result<TestContext, Error> {
    let mock_scheduler = Arc::new(MockActionScheduler::new());
    let ac_store = Store::new(MemoryStore::new(
        &nativelink_config::stores::MemoryStore::default(),
    ));
    let cache_scheduler = CacheLookupScheduler::new(ac_store.clone(), mock_scheduler.clone())?;
    Ok(TestContext {
        mock_scheduler,
        ac_store,
        cache_scheduler,
    })
}

#[nativelink_test]
async fn platform_property_manager_call_passed() -> Result<(), Error> {
    let context = make_cache_scheduler()?;
    let platform_property_manager = Arc::new(PlatformPropertyManager::new(HashMap::new()));
    let instance_name = INSTANCE_NAME.to_string();
    let (actual_manager, actual_instance_name) = join!(
        context
            .cache_scheduler
            .get_platform_property_manager(&instance_name),
        context
            .mock_scheduler
            .expect_get_platform_property_manager(Ok(platform_property_manager.clone())),
    );
    assert_eq!(
        Arc::as_ptr(&platform_property_manager),
        Arc::as_ptr(&actual_manager?)
    );
    assert_eq!(instance_name, actual_instance_name);
    Ok(())
}

#[nativelink_test]
async fn add_action_handles_skip_cache() -> Result<(), Error> {
    let context = make_cache_scheduler()?;
    let action_info = make_base_action_info(UNIX_EPOCH);
    let action_result = ProtoActionResult::from(ActionResult::default());
    context
        .ac_store
        .update_oneshot(*action_info.digest(), action_result.encode_to_vec().into())
        .await?;
    let (_forward_watch_channel_tx, forward_watch_channel_rx) =
        watch::channel(Arc::new(ActionState {
            id: OperationId::new(action_info.unique_qualifier.clone()),
            stage: ActionStage::Queued,
        }));
    let mut skip_cache_action = action_info.clone();
    skip_cache_action.skip_cache_lookup = true;
    let _ = join!(
        context.cache_scheduler.add_action(skip_cache_action),
        context
            .mock_scheduler
            .expect_add_action(Ok(forward_watch_channel_rx))
    );
    Ok(())
}

#[nativelink_test]
async fn find_existing_action_call_passed() -> Result<(), Error> {
    let context = make_cache_scheduler()?;
    let action_name = ActionInfoHashKey {
        instance_name: "instance".to_string(),
        digest_function: DigestHasherFunc::Sha256,
        digest: DigestInfo::new([8; 32], 1),
        salt: 1000,
    };
    let (actual_result, actual_action_name) = join!(
        context.cache_scheduler.find_existing_action(&action_name),
        context.mock_scheduler.expect_find_existing_action(Ok(None)),
    );
    assert_eq!(true, actual_result.unwrap().is_none());
    assert_eq!(action_name, actual_action_name);
    Ok(())
}
