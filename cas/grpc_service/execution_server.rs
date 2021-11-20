// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use futures::Stream;
use tonic::{Request, Response, Status};

use ac_utils::get_and_decode_digest;
use common::{log, DigestInfo};
use config::cas_server::{CapabilitiesConfig, ExecutionConfig, InstanceName};
use error::{make_input_err, Error, ResultExt};
use platform_property_manager::PlatformPropertyManager;
use proto::build::bazel::remote::execution::v2::{
    execution_server::Execution, execution_server::ExecutionServer as Server, Action, ExecuteRequest,
    WaitExecutionRequest,
};
use proto::google::longrunning::Operation;
use scheduler::Scheduler;
use store::StoreManager;

pub struct ExecutionServer {
    schedulers: HashMap<InstanceName, Arc<Scheduler>>,
}

type ExecuteStream = Pin<Box<dyn Stream<Item = Result<Operation, Status>> + Send + Sync + 'static>>;

impl ExecutionServer {
    pub fn new(
        config: &HashMap<InstanceName, ExecutionConfig>,
        capabilities_config: &HashMap<InstanceName, CapabilitiesConfig>,
        store_manager: &StoreManager,
    ) -> Result<Self, Error> {
        let mut schedulers = HashMap::with_capacity(config.len());
        for (instance_name, exec_cfg) in config {
            let capabilities_cfg = capabilities_config
                .get(instance_name)
                .err_tip(|| "Capabilities needs config for '{}' because it exists in execution")?;
            let cas_store = store_manager
                .get_store(&exec_cfg.cas_store)
                .ok_or_else(|| make_input_err!("'cas_store': '{}' does not exist", exec_cfg.cas_store))?
                .clone();
            let ac_store = store_manager
                .get_store(&exec_cfg.ac_store)
                .ok_or_else(|| make_input_err!("'cas_store': '{}' does not exist", exec_cfg.ac_store))?
                .clone();
            let platform_property_manager = PlatformPropertyManager::new(
                capabilities_cfg
                    .supported_platform_properties
                    .clone()
                    .unwrap_or(HashMap::new()),
            );
            schedulers.insert(
                instance_name.to_string(),
                Arc::new(Scheduler::new(exec_cfg, platform_property_manager, ac_store, cas_store)),
            );
        }
        Ok(Self { schedulers })
    }

    pub fn into_service(self) -> Server<ExecutionServer> {
        Server::new(self)
    }

    async fn inner_execute(&self, request: Request<ExecuteRequest>) -> Result<Response<ExecuteStream>, Error> {
        let execute_req = request.into_inner();
        let instance_name = execute_req.instance_name;

        let scheduler = self
            .schedulers
            .get(&instance_name)
            .err_tip(|| "Instance name '{}' not configured")?;

        let digest = DigestInfo::try_from(
            execute_req
                .action_digest
                .err_tip(|| "Expected action_digest to exist")?,
        )
        .err_tip(|| "Failed to unwrap action cache")?;

        let action = get_and_decode_digest::<Action>(&scheduler.cas_pin(), &digest).await?;

        scheduler
            .queue_action(&action)
            .await
            .err_tip(|| "Failed to queue operation")?;

        Err(make_input_err!(""))
    }
}

#[tonic::async_trait]
impl Execution for ExecutionServer {
    type ExecuteStream = ExecuteStream;

    async fn execute(&self, grpc_request: Request<ExecuteRequest>) -> Result<Response<ExecuteStream>, Status> {
        // TODO(blaise.bruer) This is a work in progress, remote execution likely won't work yet.
        log::info!("\x1b[0;31mexecute Req\x1b[0m: {:?}", grpc_request.get_ref());
        let now = Instant::now();
        let resp = self
            .inner_execute(grpc_request)
            .await
            .err_tip(|| "Failed on execute() command")
            .map_err(|e| e.into());
        let d = now.elapsed().as_secs_f32();
        if let Err(err) = &resp {
            log::error!("\x1b[0;31mexecute Resp\x1b[0m: {} {:?}", d, err);
        } else {
            log::info!("\x1b[0;31mexecute Resp\x1b[0m: {}", d);
        }
        resp
    }

    type WaitExecutionStream = Pin<Box<dyn Stream<Item = Result<Operation, Status>> + Send + Sync + 'static>>;
    async fn wait_execution(
        &self,
        request: Request<WaitExecutionRequest>,
    ) -> Result<Response<Self::WaitExecutionStream>, Status> {
        use stdext::function_name;
        let output = format!("{} not yet implemented", function_name!());
        println!("{:?}", request);
        Err(Status::unimplemented(output))
    }
}
