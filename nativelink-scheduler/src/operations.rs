

use async_trait::async_trait;
use nativelink_proto::google::longrunning::Operation;

use nativelink_util::action_messages::ActionInfoHashKey;

#[async_trait]
pub trait Operations: Sync + Send + Unpin {
    fn list_actions(&self) -> Vec<Operation>;

    fn get_action(&self, action_info_hash_key: &ActionInfoHashKey) -> Option<Operation>;

    async fn delete_action(&self) {
        unimplemented!()
    }

    async fn cancel_action(&self) {
        unimplemented!()
    }

    async fn wait_action(&self) {
        // Can be attached via execution_server.wait_execution WaitExecutionStream
        unimplemented!()
    }
}
