// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use action_messages::{ActionStage, ActionState};
use common::DigestInfo;
use error::Error;
use proto::google::longrunning::{operation, Operation};

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
}
