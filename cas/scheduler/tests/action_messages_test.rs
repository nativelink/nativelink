// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::time::SystemTime;

use action_messages::{ActionResult, ActionStage, ActionState, ExecutionMetadata};
use common::DigestInfo;
use error::Error;
use proto::build::bazel::remote::execution::v2::ExecuteResponse;
use proto::google::longrunning::{operation, Operation};
use proto::google::rpc::Status;

#[cfg(test)]
mod action_messages_tests {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    #[tokio::test]
    async fn action_state_any_url_test() -> Result<(), Error> {
        let operation: Operation = ActionState {
            name: "test".to_string(),
            action_digest: DigestInfo::new([1u8; 32], 5),
            stage: ActionStage::Unknown,
        }
        .into();

        match operation.result {
            Some(operation::Result::Response(any)) => assert_eq!(
                any.type_url,
                "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteResponse"
            ),
            other => assert!(false, "Expected Some(Result(Any)), got: {:?}", other),
        }

        Ok(())
    }

    #[tokio::test]
    async fn execute_response_status_message_is_some_on_success_test() -> Result<(), Error> {
        let execute_response: ExecuteResponse = ActionStage::Completed(ActionResult {
            output_files: vec![],
            output_folders: vec![],
            output_file_symlinks: vec![],
            output_directory_symlinks: vec![],
            exit_code: 0,
            stdout_digest: DigestInfo::new([2u8; 32], 5),
            stderr_digest: DigestInfo::new([3u8; 32], 5),
            execution_metadata: ExecutionMetadata {
                worker: "foo_worker_id".to_string(),
                queued_timestamp: SystemTime::UNIX_EPOCH,
                worker_start_timestamp: SystemTime::UNIX_EPOCH,
                worker_completed_timestamp: SystemTime::UNIX_EPOCH,
                input_fetch_start_timestamp: SystemTime::UNIX_EPOCH,
                input_fetch_completed_timestamp: SystemTime::UNIX_EPOCH,
                execution_start_timestamp: SystemTime::UNIX_EPOCH,
                execution_completed_timestamp: SystemTime::UNIX_EPOCH,
                output_upload_start_timestamp: SystemTime::UNIX_EPOCH,
                output_upload_completed_timestamp: SystemTime::UNIX_EPOCH,
            },
            server_logs: Default::default(),
        })
        .into();

        // This was once discovered to be None, which is why this test exists.
        assert_eq!(execute_response.status, Some(Status::default()));

        Ok(())
    }
}
