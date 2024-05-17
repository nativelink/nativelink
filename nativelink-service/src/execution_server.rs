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
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::{Stream, StreamExt};
use nativelink_config::cas_server::{ExecutionConfig, InstanceName};
use nativelink_error::{make_input_err, Error, ResultExt};
use nativelink_proto::build::bazel::remote::execution::v2::execution_server::{
    Execution, ExecutionServer as Server,
};
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, Command, ExecuteRequest, WaitExecutionRequest,
};
use nativelink_proto::google::longrunning::{CancelOperationRequest, DeleteOperationRequest, GetOperationRequest, ListOperationsRequest, ListOperationsResponse, Operation, WaitOperationRequest};
use nativelink_scheduler::action_scheduler::ActionScheduler;
use nativelink_store::ac_utils::get_and_decode_digest;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::action_messages::{
    ActionInfo, ActionInfoHashKey, ActionState, DEFAULT_EXECUTION_PRIORITY,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::platform_properties::PlatformProperties;
use nativelink_util::store_trait::Store;
use rand::{thread_rng, Rng};
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tonic::{async_trait, Request, Response, Status};
use tonic::metadata::MetadataMap;
use tracing::{event, instrument, Level};
use nativelink_proto::google::longrunning::operations_server::OperationsServer;
use nativelink_proto::google::longrunning::operations_server::Operations;
use nativelink_scheduler::operations::{Operations as SchedulerOperations};

struct InstanceInfo {
    scheduler: Arc<dyn ActionScheduler>,
    cas_store: Arc<dyn Store>,
}

impl InstanceInfo {
    fn cas_pin(&self) -> Pin<&dyn Store> {
        Pin::new(self.cas_store.as_ref())
    }

    async fn build_action_info(
        &self,
        instance_name: String,
        action_digest: DigestInfo,
        action: &Action,
        priority: i32,
        skip_cache_lookup: bool,
        digest_function: DigestHasherFunc,
    ) -> Result<ActionInfo, Error> {
        let command_digest = DigestInfo::try_from(
            action
                .command_digest
                .clone()
                .err_tip(|| "Expected command_digest to exist")?,
        )
        .err_tip(|| "Could not decode command digest")?;

        let input_root_digest = DigestInfo::try_from(
            action
                .clone()
                .input_root_digest
                .err_tip(|| "Expected input_digest_root")?,
        )?;
        let timeout = action
            .timeout
            .clone()
            .map(|v| Duration::new(v.seconds as u64, v.nanos as u32))
            .unwrap_or(Duration::MAX);

        let mut platform_properties = HashMap::new();
        if let Some(platform) = &action.platform {
            for property in &platform.properties {
                let platform_property = self
                    .scheduler
                    .get_platform_property_manager(&instance_name)
                    .await
                    .err_tip(|| "Failed to get platform properties in build_action_info")?
                    .make_prop_value(&property.name, &property.value)
                    .err_tip(|| "Failed to convert platform property in build_action_info")?;
                platform_properties.insert(property.name.clone(), platform_property);
            }
        }

        // Goma puts the properties in the Command.
        if platform_properties.is_empty() {
            let command = get_and_decode_digest::<Command>(self.cas_pin(), &command_digest).await?;
            if let Some(platform) = &command.platform {
                for property in &platform.properties {
                    let platform_property = self
                        .scheduler
                        .get_platform_property_manager(&instance_name)
                        .await
                        .err_tip(|| "Failed to get platform properties in build_action_info")?
                        .make_prop_value(&property.name, &property.value)
                        .err_tip(|| {
                            "Failed to convert command platform property in build_action_info"
                        })?;
                    platform_properties.insert(property.name.clone(), platform_property);
                }
            }
        }

        Ok(ActionInfo {
            command_digest,
            input_root_digest,
            timeout,
            platform_properties: PlatformProperties::new(platform_properties),
            priority,
            load_timestamp: UNIX_EPOCH,
            insert_timestamp: SystemTime::now(),
            unique_qualifier: ActionInfoHashKey {
                instance_name,
                digest: action_digest,
                salt: if action.do_not_cache {
                    thread_rng().gen::<u64>()
                } else {
                    0
                },
            },
            skip_cache_lookup,
            digest_function,
        })
    }
}

pub struct ExecutionServer {
    instance_infos: HashMap<InstanceName, InstanceInfo>,
    scheduler_operations: HashMap<InstanceName, Arc<dyn SchedulerOperations>>
}

type ExecuteStream = Pin<Box<dyn Stream<Item = Result<Operation, Status>> + Send + Sync + 'static>>;

impl ExecutionServer {
    pub fn new(
        config: &HashMap<InstanceName, ExecutionConfig>,
        scheduler_map: &HashMap<String, Arc<dyn ActionScheduler>>,
        operations_schedulers: &HashMap<String, Arc<dyn SchedulerOperations>>,
        store_manager: &StoreManager,
    ) -> Result<Self, Error> {
        let mut instance_infos = HashMap::with_capacity(config.len());
        let mut scheduler_operations = HashMap::with_capacity(config.len());

        for (instance_name, exec_cfg) in config {
            let cas_store = store_manager
                .get_store(&exec_cfg.cas_store)
                .ok_or_else(|| {
                    make_input_err!("'cas_store': '{}' does not exist", exec_cfg.cas_store)
                })?;
            let scheduler = scheduler_map
                .get(&exec_cfg.scheduler)
                .err_tip(|| {
                    format!(
                        "Scheduler needs config for '{}' because it exists in execution",
                        exec_cfg.scheduler
                    )
                })?
                .clone();

            instance_infos.insert(
                instance_name.to_string(),
                InstanceInfo {
                    scheduler,
                    cas_store,
                },
            );

            let scheduler = operations_schedulers.get(&exec_cfg.scheduler)
                .err_tip(|| {
                    format!("Failed to get scheduler name '{}'", exec_cfg.scheduler)
                })?
                .clone();

            scheduler_operations.insert(
                instance_name.to_string(),
                scheduler
            );
        }
        Ok(Self { instance_infos, scheduler_operations })
    }

    pub fn into_service(self) -> Server<ExecutionServer> {
        Server::new(self)
    }

    pub fn into_operations_service(self) -> OperationsServer<ExecutionServer> { OperationsServer::new(self) }

    fn to_execute_stream(receiver: watch::Receiver<Arc<ActionState>>) -> Response<ExecuteStream> {
        let receiver_stream = Box::pin(WatchStream::new(receiver).map(|action_update| {
            event!(Level::INFO, ?action_update, "Execute Resp Stream",);
            Ok(Into::<Operation>::into(action_update.as_ref().clone()))
        }));
        tonic::Response::new(receiver_stream)
    }

    async fn inner_execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteStream>, Error> {
        let execute_req = request.into_inner();
        let instance_name = execute_req.instance_name;

        let instance_info = self
            .instance_infos
            .get(&instance_name)
            .err_tip(|| "Instance name '{}' not configured")?;

        let digest = DigestInfo::try_from(
            execute_req
                .action_digest
                .err_tip(|| "Expected action_digest to exist")?,
        )
        .err_tip(|| "Failed to unwrap action cache")?;

        let priority = execute_req
            .execution_policy
            .map_or(DEFAULT_EXECUTION_PRIORITY, |p| p.priority);

        let action = get_and_decode_digest::<Action>(instance_info.cas_pin(), &digest).await?;
        let action_info = instance_info
            .build_action_info(
                instance_name,
                digest,
                &action,
                priority,
                execute_req.skip_cache_lookup,
                execute_req
                    .digest_function
                    .try_into()
                    .err_tip(|| "Could not convert digest function in inner_execute()")?,
            )
            .await?;

        let rx = instance_info
            .scheduler
            .add_action(action_info)
            .await
            .err_tip(|| "Failed to schedule task")?;

        Ok(Self::to_execute_stream(rx))
    }

    async fn inner_wait_execution(
        &self,
        request: Request<WaitExecutionRequest>,
    ) -> Result<Response<ExecuteStream>, Status> {
        let unique_qualifier = ActionInfoHashKey::try_from(request.into_inner().name.as_str())
            .err_tip(|| "Decoding operation name into ActionInfoHashKey")?;
        let Some(instance_info) = self.instance_infos.get(&unique_qualifier.instance_name) else {
            return Err(Status::not_found(format!(
                "No scheduler with the instance name {}",
                unique_qualifier.instance_name
            )));
        };
        let Some(rx) = instance_info
            .scheduler
            .find_existing_action(&unique_qualifier)
            .await
        else {
            return Err(Status::not_found("Failed to find existing task"));
        };
        Ok(Self::to_execute_stream(rx))
    }
}

#[tonic::async_trait]
impl Execution for ExecutionServer {
    type ExecuteStream = ExecuteStream;
    type WaitExecutionStream = ExecuteStream;

    #[allow(clippy::blocks_in_conditions)]
    #[instrument(
        err,
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn execute(
        &self,
        grpc_request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteStream>, Status> {
        self.inner_execute(grpc_request)
            .await
            .err_tip(|| "Failed on execute() command")
            .map_err(|e| e.into())
    }

    #[allow(clippy::blocks_in_conditions)]
    #[instrument(
        err,
        level = Level::ERROR,
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn wait_execution(
        &self,
        grpc_request: Request<WaitExecutionRequest>,
    ) -> Result<Response<ExecuteStream>, Status> {
        let resp = self
            .inner_wait_execution(grpc_request)
            .await
            .err_tip(|| "Failed on wait_execution() command")
            .map_err(|e| e.into());

        if resp.is_ok() {
            event!(Level::DEBUG, return = "Ok(<stream>)");
        }
        resp
    }
}

#[async_trait]
impl Operations for ExecutionServer {
    async fn list_operations(&self, request: Request<ListOperationsRequest>) -> Result<Response<ListOperationsResponse>, Status> {
        // TODO(adams): if page size, filter, token is set, then return error.
        let (metadata, extensions, message) = request.into_parts();
        let name = message.name;

        let maybe_operations = self.scheduler_operations.get(&name);

        match maybe_operations {
            Some(oper) => {
                let mut operations_results: Vec<Operation> = oper.list_actions();
                let list_operations_response = ListOperationsResponse {
                    operations: operations_results,
                    next_page_token: "".to_string(),
                };
                return Ok(Response::new(list_operations_response))
            },
            None =>
                return Ok(Response::new(ListOperationsResponse::default()))
        }
    }

    async fn get_operation(&self, request: Request<GetOperationRequest>) -> Result<Response<Operation>, Status> {
        let (metadata, extensions, message) = request.into_parts();
        let name = message.name;
        let action_info_hash_key= ActionInfoHashKey::try_from(name.as_str())
            .err_tip(|| "Decoding operation name into ActionInfoHashKey")?;
        let maybe_operations = self.scheduler_operations.get(&action_info_hash_key.instance_name);

        let maybe_action_response = maybe_operations.map(|oper|
            oper.get_action(&action_info_hash_key).map(|action| Response::new(action))
        ).flatten();

        match maybe_action_response {
            Some(operation_response) => Ok(operation_response),
            None => Ok(Response::new(Operation::default()))
        }
    }

    async fn delete_operation(&self, request: Request<DeleteOperationRequest>) -> Result<Response<()>, Status> {
        unimplemented!()
    }

    async fn cancel_operation(&self, request: Request<CancelOperationRequest>) -> Result<Response<()>, Status> {
        unimplemented!()
    }

    async fn wait_operation(&self, request: Request<WaitOperationRequest>) -> Result<Response<Operation>, Status> {
        let (metadata, extensions, message) = request.into_parts();
        let name = message.name;
        let timeout: Duration =  match message.timeout {
            Some(timeout) => Duration::new(timeout.seconds as u64, timeout.nanos as u32),
            None => Duration::MAX
        };
        let wait_execution_request = Request::new(WaitExecutionRequest { name });
        let resp = self.inner_wait_execution(wait_execution_request).await
            .err_tip(|| "Failed on wait_execution() command")?;
            // .map_err(|e| e.info())?;

        let (_, mut e, _) = resp.into_parts();
        let mut execute_stream: ExecuteStream = e;
        // TODO(adams): need to set a timeout
        // execute_stream.timeout(timeout);

        // TODO(adams): handle failure cases better
        let mut ret: Result<Response<Operation>, Status> = Err(Status::unknown(""));
        while let Some(result) = execute_stream.next().await {
            // TODO(adams): handle error from reading stream
            ret = result.map(|operation| Response::new(operation));
        }

        return ret;
    }
}
