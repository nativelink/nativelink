// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;

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

const MAX_BATCH_TOTAL_SIZE: i64 = 64 * 1024;

#[derive(Debug, Default)]
pub struct CapabilitiesServer {
    supported_node_properties: HashMap<InstanceName, Vec<String>>,
}

impl CapabilitiesServer {
    pub fn new(config: &HashMap<InstanceName, CapabilitiesConfig>) -> Result<Self, Error> {
        let mut supported_node_properties = HashMap::new();
        for (instance_name, cfg) in config {
            let mut properties = Vec::new();
            if let Some(supported_node_properties_proto) = &cfg.supported_platform_properties {
                for (platform_key, _) in supported_node_properties_proto {
                    properties.push(platform_key.clone());
                }
            }
            supported_node_properties.insert(instance_name.clone(), properties);
        }
        Ok(CapabilitiesServer {
            supported_node_properties,
        })
    }

    pub fn into_service(self) -> Server<CapabilitiesServer> {
        Server::new(self)
    }

    fn get_capabilities(&self, instance_name: &str) -> Result<&Vec<String>, Error> {
        self.supported_node_properties
            .get(instance_name)
            .err_tip(|| format!("Instance name '{}' not configured", instance_name))
    }
}

#[tonic::async_trait]
impl Capabilities for CapabilitiesServer {
    async fn get_capabilities(
        &self,
        request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<ServerCapabilities>, Status> {
        let supported_node_properties = self
            .get_capabilities(&request.into_inner().instance_name)
            .map_err(|e| Into::<Status>::into(e))?;
        let resp = ServerCapabilities {
            cache_capabilities: Some(CacheCapabilities {
                digest_function: vec![DigestFunction::Sha256.into()],
                action_cache_update_capabilities: Some(ActionCacheUpdateCapabilities { update_enabled: true }),
                cache_priority_capabilities: None,
                max_batch_total_size_bytes: MAX_BATCH_TOTAL_SIZE,
                symlink_absolute_path_strategy: SymlinkAbsolutePathStrategy::Disallowed.into(),
            }),
            execution_capabilities: Some(ExecutionCapabilities {
                digest_function: DigestFunction::Sha256.into(),
                exec_enabled: true, // TODO(blaise.bruer) Make this configurable.
                execution_priority_capabilities: Some(PriorityCapabilities {
                    priorities: vec![PriorityRange {
                        min_priority: 0,
                        max_priority: i32::MAX,
                    }],
                }),
                supported_node_properties: supported_node_properties.clone(),
            }),
            deprecated_api_version: None,
            low_api_version: Some(SemVer {
                major: 2,
                minor: 0,
                patch: 0,
                prerelease: "".to_string(),
            }),
            high_api_version: Some(SemVer {
                major: 2,
                minor: 1,
                patch: 0,
                prerelease: "".to_string(),
            }),
        };
        Ok(Response::new(resp))
    }
}
