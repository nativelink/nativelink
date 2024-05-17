use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use tonic::{async_trait, Request, Response, Status};
use nativelink_config::cas_server::{InstanceName, OperationsConfig};
use nativelink_error::{make_err, make_input_err, Code, Error, ResultExt};
use nativelink_proto::google::longrunning::{CancelOperationRequest, DeleteOperationRequest, GetOperationRequest, ListOperationsRequest, ListOperationsResponse, Operation, WaitOperationRequest};
use nativelink_proto::google::longrunning::operations_server::{OperationsServer as Server};
use nativelink_proto::google::longrunning::operations_server::Operations;
use nativelink_scheduler::action_scheduler::ActionScheduler;
use nativelink_scheduler::operations::{Operations as SchedulerOperations};
use nativelink_util::action_messages::ActionInfoHashKey;

pub struct OperationsServer {
    scheduler_operations: HashMap<InstanceName, Arc<dyn SchedulerOperations>>
}
impl OperationsServer {
    pub fn new(
        config: &HashMap<InstanceName, OperationsConfig>,
        scheduler_map: &HashMap<String, Arc<dyn SchedulerOperations>>,
    ) -> Result<Self, Error> {
        let mut scheduler_operations = HashMap::with_capacity(config.len());

        for (instance_name, operations_config) in config {

            let scheduler = scheduler_map.get(&operations_config.scheduler)
                .err_tip(|| {
                    format!("Failed to get scheduler name '{}'", operations_config.scheduler)
                })?
                .clone();

            scheduler_operations.insert(
                instance_name.to_string(),
                scheduler
            );
        }

        Ok(OperationsServer{scheduler_operations})
    }

    pub fn into_service(self) -> Server<OperationsServer> { Server::new(self) }
}

impl Debug for OperationsServer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperationsServer").finish()
    }
}

#[async_trait]
impl Operations for OperationsServer {
    async fn list_operations(&self, request: Request<ListOperationsRequest>) -> Result<Response<ListOperationsResponse>, Status> {
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
        // Not fully implemented in scheduler
        // AwaitedAction has the channel to communicate with the worker and the workerid
        todo!()
    }

    async fn cancel_operation(&self, request: Request<CancelOperationRequest>) -> Result<Response<()>, Status> {
        // Not fully implemented in scheduler
        todo!()
    }

    async fn wait_operation(&self, request: Request<WaitOperationRequest>) -> Result<Response<Operation>, Status> {
        // execution server has this operation
        todo!()
    }
}
