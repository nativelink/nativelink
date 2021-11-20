// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use ac_utils::get_and_decode_digest;
use common::DigestInfo;
use config::cas_server::ExecutionConfig;
use error::{Error, ResultExt};
use platform_property_manager::{PlatformPropertyManager, PlatformPropertyValue};
use proto::build::bazel::remote::execution::v2::{Action, Command};
use store::Store;

struct ActionInfo {
    _digest: DigestInfo,
    _command_digest: DigestInfo,
    _input_root_digest: DigestInfo,
    _timeout: Duration,
    _do_not_cache: bool,
    _platform_properties: HashMap<String, PlatformPropertyValue>,
}

pub struct Scheduler {
    _queued_actions: HashMap<DigestInfo, ActionInfo>,
    _platform_property_manager: PlatformPropertyManager,
    ac_store: Arc<dyn Store>,
    cas_store: Arc<dyn Store>,
}

// TODO(blaise.bruer) This section of code is a WIP, using remote execution is currently
// not fully developed and looking at the code at this point of time it does not work.
// Please do not enable the remote execution functionality in the config at this point.
impl Scheduler {
    pub fn new(
        _execution_config: &ExecutionConfig,
        platform_property_manager: PlatformPropertyManager,
        ac_store: Arc<dyn Store>,
        cas_store: Arc<dyn Store>,
    ) -> Self {
        Self {
            _queued_actions: HashMap::new(),
            _platform_property_manager: platform_property_manager,
            ac_store,
            cas_store,
        }
    }

    pub fn cas_pin<'a>(&'a self) -> Pin<&'a dyn Store> {
        Pin::new(self.cas_store.as_ref())
    }

    pub fn ac_pin<'a>(&'a self) -> Pin<&'a dyn Store> {
        Pin::new(self.ac_store.as_ref())
    }

    pub async fn queue_action(&self, action: &Action) -> Result<(), Error> {
        println!("{:?}", action);
        let command_digest = DigestInfo::try_from(
            action
                .command_digest
                .clone()
                .err_tip(|| "Expected command_digest to exist")?,
        )
        .err_tip(|| "Could not decode command digest")?;
        let command = get_and_decode_digest::<Command>(&self.cas_pin(), &command_digest).await?;

        println!("{:?}", command);

        Ok(())
    }
}
