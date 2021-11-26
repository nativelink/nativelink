// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use action_messages::{ActionInfo, ActionStage, ActionState};
use common::DigestInfo;
use error::Error;
use platform_property_manager::{PlatformProperties, PlatformPropertyValue};
use proto::build::bazel::remote::execution::v2::ExecuteRequest;
use proto::com::github::allada::rust_cas::remote_execution::{update_for_worker, StartExecute, UpdateForWorker};
use scheduler::Scheduler;
use tokio::sync::mpsc;
use worker::Worker;

const INSTANCE_NAME: &str = "foobar_instance_name";

fn make_base_action_info() -> ActionInfo {
    ActionInfo {
        instance_name: INSTANCE_NAME.to_string(),
        digest: DigestInfo::new([0u8; 32], 0),
        command_digest: DigestInfo::new([0u8; 32], 0),
        input_root_digest: DigestInfo::new([0u8; 32], 0),
        timeout: Duration::MAX,
        platform_properties: PlatformProperties {
            properties: HashMap::new(),
        },
        priority: 0,
        insert_timestamp: SystemTime::now(),
        salt: 0,
    }
}

#[cfg(test)]
mod buf_channel_tests {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    #[tokio::test]
    async fn basic_add_action_with_one_worker_test() -> Result<(), Error> {
        const WORKER_ID: &str = "foobar_worker_id";

        let scheduler = Scheduler::new();
        let action_digest = DigestInfo::new([99u8; 32], 512);

        let mut rx_from_worker = {
            let (tx, rx) = mpsc::unbounded_channel();
            scheduler
                .add_worker(Worker::new(WORKER_ID.to_string(), PlatformProperties::default(), tx))
                .await;
            rx
        };

        let mut client_rx = {
            let mut action_info = make_base_action_info();
            action_info.digest = action_digest.clone();
            scheduler.add_action(action_info).await?
        };

        {
            // Worker should have been sent an execute command.
            let expected_msg_for_worker = UpdateForWorker {
                update: Some(update_for_worker::Update::StartAction(StartExecute {
                    execute_request: Some(ExecuteRequest {
                        instance_name: INSTANCE_NAME.to_string(),
                        skip_cache_lookup: true,
                        action_digest: Some(action_digest.clone().into()),
                        ..Default::default()
                    }),
                })),
            };
            let msg_for_worker = rx_from_worker.recv().await.unwrap();
            assert_eq!(msg_for_worker, expected_msg_for_worker);
        }
        {
            // Client should get notification saying it's being executed.
            let action_state = client_rx.borrow_and_update();
            let expected_action_state = ActionState {
                // Name is a random string, so we ignore it and just make it the same.
                name: action_state.name.clone(),
                action_digest: action_digest.clone(),
                stage: ActionStage::Executing,
            };
            assert_eq!(action_state.as_ref(), &expected_action_state);
        }

        Ok(())
    }

    #[tokio::test]
    async fn worker_should_not_queue_if_properties_dont_match_test() -> Result<(), Error> {
        let scheduler = Scheduler::new();
        let action_digest = DigestInfo::new([99u8; 32], 512);
        let mut platform_properties = PlatformProperties::default();
        platform_properties
            .properties
            .insert("prop".to_string(), PlatformPropertyValue::Exact("1".to_string()));
        let mut worker_properties = platform_properties.clone();
        worker_properties
            .properties
            .insert("prop".to_string(), PlatformPropertyValue::Exact("2".to_string()));

        let mut rx_from_worker1 = {
            let (tx, rx) = mpsc::unbounded_channel();
            scheduler
                .add_worker(Worker::new("worker_id1".to_string(), platform_properties.clone(), tx))
                .await;
            rx
        };

        let mut client_rx = {
            let mut action_info = make_base_action_info();
            action_info.platform_properties = worker_properties.clone();
            action_info.digest = action_digest.clone();
            scheduler.add_action(action_info).await?
        };

        {
            // Client should get notification saying it's been queued.
            let action_state = client_rx.borrow_and_update();
            let expected_action_state = ActionState {
                // Name is a random string, so we ignore it and just make it the same.
                name: action_state.name.clone(),
                action_digest: action_digest.clone(),
                stage: ActionStage::Queued,
            };
            assert_eq!(action_state.as_ref(), &expected_action_state);
        }

        let mut rx_from_worker2 = {
            let (tx, rx) = mpsc::unbounded_channel();
            scheduler
                .add_worker(Worker::new("worker_id2".to_string(), worker_properties, tx))
                .await;
            rx
        };
        {
            // Worker should have been sent an execute command.
            let expected_msg_for_worker = UpdateForWorker {
                update: Some(update_for_worker::Update::StartAction(StartExecute {
                    execute_request: Some(ExecuteRequest {
                        instance_name: INSTANCE_NAME.to_string(),
                        skip_cache_lookup: true,
                        action_digest: Some(action_digest.clone().into()),
                        ..Default::default()
                    }),
                })),
            };
            let msg_for_worker = rx_from_worker2.recv().await.unwrap();
            assert_eq!(msg_for_worker, expected_msg_for_worker);
        }
        {
            // Client should get notification saying it's being executed.
            let action_state = client_rx.borrow_and_update();
            let expected_action_state = ActionState {
                // Name is a random string, so we ignore it and just make it the same.
                name: action_state.name.clone(),
                action_digest: action_digest.clone(),
                stage: ActionStage::Executing,
            };
            assert_eq!(action_state.as_ref(), &expected_action_state);
        }

        // Our first worker should have no updates over this test.
        assert_eq!(rx_from_worker1.try_recv(), Err(mpsc::error::TryRecvError::Empty));

        Ok(())
    }

    #[tokio::test]
    async fn cacheable_items_join_same_action_queued_test() -> Result<(), Error> {
        const WORKER_ID: &str = "foobar_worker_id";

        let scheduler = Scheduler::new();
        let action_digest = DigestInfo::new([99u8; 32], 512);

        let mut expected_action_state = ActionState {
            name: "".to_string(), // Will be filled later.
            action_digest: action_digest.clone(),
            stage: ActionStage::Queued,
        };

        let (mut client1_rx, mut client2_rx) = {
            // Send our actions to the scheduler.
            let mut action_info = make_base_action_info();
            action_info.digest = action_digest.clone();
            let client1_rx = scheduler.add_action(action_info).await?;

            let mut action_info = make_base_action_info();
            action_info.digest = action_digest.clone();
            let client2_rx = scheduler.add_action(action_info).await?;
            (client1_rx, client2_rx)
        };

        {
            // Clients should get notification saying it's been queued.
            let action_state1 = client1_rx.borrow_and_update();
            let action_state2 = client2_rx.borrow_and_update();
            // Name is random so we set force it to be the same.
            expected_action_state.name = action_state1.name.to_string();
            assert_eq!(action_state1.as_ref(), &expected_action_state);
            assert_eq!(action_state2.as_ref(), &expected_action_state);
        }

        let mut rx_from_worker = {
            let (tx, rx) = mpsc::unbounded_channel();
            scheduler
                .add_worker(Worker::new(WORKER_ID.to_string(), PlatformProperties::default(), tx))
                .await;
            rx
        };

        {
            // Worker should have been sent an execute command.
            let expected_msg_for_worker = UpdateForWorker {
                update: Some(update_for_worker::Update::StartAction(StartExecute {
                    execute_request: Some(ExecuteRequest {
                        instance_name: INSTANCE_NAME.to_string(),
                        skip_cache_lookup: true,
                        action_digest: Some(action_digest.clone().into()),
                        ..Default::default()
                    }),
                })),
            };
            let msg_for_worker = rx_from_worker.recv().await.unwrap();
            assert_eq!(msg_for_worker, expected_msg_for_worker);
        }

        // Action should now be executing.
        expected_action_state.stage = ActionStage::Executing;
        {
            // Both client1 and client2 should be receiving the same updates.
            // Most importantly the `name` (which is random) will be the same.
            assert_eq!(client1_rx.borrow_and_update().as_ref(), &expected_action_state);
            assert_eq!(client2_rx.borrow_and_update().as_ref(), &expected_action_state);
        }

        {
            // Now if another action is requested it should also join with executing action.
            let mut action_info = make_base_action_info();
            action_info.digest = action_digest.clone();
            let mut client3_rx = scheduler.add_action(action_info).await?;

            assert_eq!(client3_rx.borrow_and_update().as_ref(), &expected_action_state);
        }

        Ok(())
    }

    #[tokio::test]
    async fn worker_disconnects_does_not_schedule_for_execution_test() -> Result<(), Error> {
        let scheduler = Scheduler::new();
        let action_digest = DigestInfo::new([99u8; 32], 512);
        let platform_properties = PlatformProperties::default();

        let rx_from_worker = {
            let (tx, rx) = mpsc::unbounded_channel();
            scheduler
                .add_worker(Worker::new("worker_id".to_string(), platform_properties.clone(), tx))
                .await;
            rx
        };

        // Now act like the worker disconnected.
        drop(rx_from_worker);

        let mut client_rx = {
            let mut action_info = make_base_action_info();
            action_info.digest = action_digest.clone();
            scheduler.add_action(action_info).await?
        };
        {
            // Client should get notification saying it's being queued not executed.
            let action_state = client_rx.borrow_and_update();
            let expected_action_state = ActionState {
                // Name is a random string, so we ignore it and just make it the same.
                name: action_state.name.clone(),
                action_digest: action_digest.clone(),
                stage: ActionStage::Queued,
            };
            assert_eq!(action_state.as_ref(), &expected_action_state);
        }

        Ok(())
    }
}
