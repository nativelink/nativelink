// Copyright 2020-2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::sync::Arc;

use tonic::{Request, Response, Status};

use config::cas_server::{CapabilitiesConfig, InstanceName};
use error::{Error, ResultExt};
use proto::build::bazel::remote::execution::v2::{
    capabilities_server::Capabilities, capabilities_server::CapabilitiesServer as Server,
    digest_function::Value as DigestFunction, priority_capabilities::PriorityRange,
    symlink_absolute_path_strategy::Value as SymlinkAbsolutePathStrategy, ActionCacheUpdateCapabilities,
    CacheCapabilities, ExecutionCapabilities, GetCapabilitiesRequest, PriorityCapabilities, ServerCapabilities,
};
use proto::build::bazel::semver::SemVer;
use scheduler::Scheduler;

const MAX_BATCH_TOTAL_SIZE: i64 = 64 * 1024;

#[derive(Debug, Default)]
pub struct CapabilitiesServer {
    supported_node_properties_for_instance: HashMap<InstanceName, Vec<String>>,
}

impl CapabilitiesServer {
    pub fn new(
        config: &HashMap<InstanceName, CapabilitiesConfig>,
        scheduler_map: &HashMap<String, Arc<Scheduler>>,
    ) -> Result<Self, Error> {
        let mut supported_node_properties_for_instance = HashMap::new();
        for (instance_name, cfg) in config {
            let mut properties = Vec::new();
            if let Some(remote_execution_cfg) = &cfg.remote_execution {
                let scheduler = scheduler_map
                    .get(&remote_execution_cfg.scheduler)
                    .err_tip(|| {
                        format!(
                            "Scheduler needs config for '{}' because it exists in capabilities",
                            remote_execution_cfg.scheduler
                        )
                    })?
                    .clone();

                for (platform_key, _) in scheduler.get_platform_property_manager().get_known_properties() {
                    properties.push(platform_key.clone());
                }
            }
            supported_node_properties_for_instance.insert(instance_name.clone(), properties);
        }
        Ok(CapabilitiesServer {
            supported_node_properties_for_instance,
        })
    }

    pub fn into_service(self) -> Server<CapabilitiesServer> {
        Server::new(self)
    }
}

#[tonic::async_trait]
impl Capabilities for CapabilitiesServer {
    async fn get_capabilities(
        &self,
        request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<ServerCapabilities>, Status> {
        let instance_name = request.into_inner().instance_name;
        let maybe_supported_node_properties = self.supported_node_properties_for_instance.get(&instance_name);
        let execution_capabilities = if let Some(props_for_instance) = maybe_supported_node_properties {
            Some(ExecutionCapabilities {
                digest_function: DigestFunction::Sha256.into(),
                exec_enabled: true, // TODO(blaise.bruer) Make this configurable.
                execution_priority_capabilities: Some(PriorityCapabilities {
                    priorities: vec![PriorityRange {
                        min_priority: 0,
                        max_priority: i32::MAX,
                    }],
                }),
                supported_node_properties: props_for_instance.clone(),
            })
        } else {
            None
        };

        let resp = ServerCapabilities {
            cache_capabilities: Some(CacheCapabilities {
                digest_function: vec![DigestFunction::Sha256.into()],
                action_cache_update_capabilities: Some(ActionCacheUpdateCapabilities { update_enabled: true }),
                cache_priority_capabilities: None,
                max_batch_total_size_bytes: MAX_BATCH_TOTAL_SIZE,
                symlink_absolute_path_strategy: SymlinkAbsolutePathStrategy::Disallowed.into(),
            }),
            execution_capabilities,
            deprecated_api_version: None,
            low_api_version: Some(SemVer {
                major: 2,
                minor: 0,
                patch: 0,
                prerelease: "".to_string(),
            }),
            high_api_version: Some(SemVer {
                major: 2,
                minor: 0,
                patch: 0,
                prerelease: "".to_string(),
            }),
        };
        Ok(Response::new(resp))
    }
}
