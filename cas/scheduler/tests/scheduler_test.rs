// Copyright 2021-2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::{mpsc, watch};

use action_messages::{ActionInfo, ActionStage, ActionState};
use common::DigestInfo;
use config::cas_server::SchedulerConfig;
use error::{Error, ResultExt};
use platform_property_manager::{PlatformProperties, PlatformPropertyValue};
use proto::build::bazel::remote::execution::v2::ExecuteRequest;
use proto::com::github::allada::turbo_cache::remote_execution::{
    update_for_worker, ConnectionResult, StartExecute, UpdateForWorker,
};
use scheduler::Scheduler;
use worker::{Worker, WorkerId};

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

async fn verify_initial_connection_message(worker_id: WorkerId, rx: &mut mpsc::UnboundedReceiver<UpdateForWorker>) {
    use pretty_assertions::assert_eq;
    // Worker should have been sent an execute command.
    let expected_msg_for_worker = UpdateForWorker {
        update: Some(update_for_worker::Update::ConnectionResult(ConnectionResult {
            worker_id: worker_id.to_string(),
        })),
    };
    let msg_for_worker = rx.recv().await.unwrap();
    assert_eq!(msg_for_worker, expected_msg_for_worker);
}

const NOW_TIME: u64 = 10000;

async fn setup_new_worker(
    scheduler: &Scheduler,
    worker_id: WorkerId,
    props: PlatformProperties,
) -> Result<mpsc::UnboundedReceiver<UpdateForWorker>, Error> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let worker = Worker::new(worker_id, props, tx, NOW_TIME);
    scheduler.add_worker(worker).await.err_tip(|| "Failed to add worker")?;
    verify_initial_connection_message(worker_id, &mut rx).await;
    Ok(rx)
}

async fn setup_action(
    scheduler: &Scheduler,
    action_digest: DigestInfo,
    platform_properties: PlatformProperties,
) -> Result<watch::Receiver<Arc<ActionState>>, Error> {
    let mut action_info = make_base_action_info();
    action_info.platform_properties = platform_properties;
    action_info.digest = action_digest;
    scheduler.add_action(action_info).await
}

#[cfg(test)]
mod scheduler_tests {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    const WORKER_TIMEOUT_S: u64 = 100;

    #[tokio::test]
    async fn basic_add_action_with_one_worker_test() -> Result<(), Error> {
        const WORKER_ID: WorkerId = WorkerId(0x123456789111);

        let scheduler = Scheduler::new(&SchedulerConfig::default());
        let action_digest = DigestInfo::new([99u8; 32], 512);

        let mut rx_from_worker = setup_new_worker(&scheduler, WORKER_ID, Default::default()).await?;
        let mut client_rx = setup_action(&scheduler, action_digest.clone(), Default::default()).await?;

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
    async fn remove_worker_reschedules_running_job_test() -> Result<(), Error> {
        const WORKER_ID1: WorkerId = WorkerId(0x111111);
        const WORKER_ID2: WorkerId = WorkerId(0x222222);
        let scheduler = Scheduler::new(&SchedulerConfig {
            worker_timeout_s: WORKER_TIMEOUT_S,
            ..Default::default()
        });
        let action_digest = DigestInfo::new([99u8; 32], 512);

        let mut rx_from_worker1 = setup_new_worker(&scheduler, WORKER_ID1, Default::default()).await?;
        let mut client_rx = setup_action(&scheduler, action_digest.clone(), Default::default()).await?;
        let mut rx_from_worker2 = setup_new_worker(&scheduler, WORKER_ID2, Default::default()).await?;

        let mut expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            name: "UNKNOWN_HERE".to_string(),
            action_digest: action_digest.clone(),
            stage: ActionStage::Executing,
        };

        let execution_request_for_worker = UpdateForWorker {
            update: Some(update_for_worker::Update::StartAction(StartExecute {
                execute_request: Some(ExecuteRequest {
                    instance_name: INSTANCE_NAME.to_string(),
                    skip_cache_lookup: true,
                    action_digest: Some(action_digest.clone().into()),
                    ..Default::default()
                }),
            })),
        };
        {
            // Worker1 should now see execution request.
            let msg_for_worker = rx_from_worker1.recv().await.unwrap();
            assert_eq!(msg_for_worker, execution_request_for_worker);
        }

        {
            // Client should get notification saying it's being executed.
            let action_state = client_rx.borrow_and_update();
            // We now know the name of the action so populate it.
            expected_action_state.name = action_state.name.clone();
            assert_eq!(action_state.as_ref(), &expected_action_state);
        }

        // Now remove worker.
        scheduler.remove_worker(WORKER_ID1).await;

        {
            // Worker1 should have received a disconnect message.
            let msg_for_worker = rx_from_worker1.recv().await.unwrap();
            assert_eq!(
                msg_for_worker,
                UpdateForWorker {
                    update: Some(update_for_worker::Update::Disconnect(()))
                }
            );
        }
        {
            // Client should get notification saying it's being executed.
            let action_state = client_rx.borrow_and_update();
            expected_action_state.stage = ActionStage::Executing;
            assert_eq!(action_state.as_ref(), &expected_action_state);
        }
        {
            // Worker2 should now see execution request.
            let msg_for_worker = rx_from_worker2.recv().await.unwrap();
            assert_eq!(msg_for_worker, execution_request_for_worker);
        }

        Ok(())
    }

    #[tokio::test]
    async fn worker_should_not_queue_if_properties_dont_match_test() -> Result<(), Error> {
        let scheduler = Scheduler::new(&SchedulerConfig::default());
        let action_digest = DigestInfo::new([99u8; 32], 512);
        let mut platform_properties = PlatformProperties::default();
        platform_properties
            .properties
            .insert("prop".to_string(), PlatformPropertyValue::Exact("1".to_string()));
        let mut worker_properties = platform_properties.clone();
        worker_properties
            .properties
            .insert("prop".to_string(), PlatformPropertyValue::Exact("2".to_string()));

        const WORKER_ID1: WorkerId = WorkerId(0x100001);
        const WORKER_ID2: WorkerId = WorkerId(0x100002);
        let mut rx_from_worker1 = setup_new_worker(&scheduler, WORKER_ID1, platform_properties.clone()).await?;
        let mut client_rx = setup_action(&scheduler, action_digest.clone(), worker_properties.clone()).await?;

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

        let mut rx_from_worker2 = setup_new_worker(&scheduler, WORKER_ID2, worker_properties).await?;
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
        const WORKER_ID: WorkerId = WorkerId(0x100009);

        let scheduler = Scheduler::new(&SchedulerConfig::default());
        let action_digest = DigestInfo::new([99u8; 32], 512);

        let mut expected_action_state = ActionState {
            name: "".to_string(), // Will be filled later.
            action_digest: action_digest.clone(),
            stage: ActionStage::Queued,
        };

        let mut client1_rx = setup_action(&scheduler, action_digest.clone(), Default::default()).await?;
        let mut client2_rx = setup_action(&scheduler, action_digest.clone(), Default::default()).await?;

        {
            // Clients should get notification saying it's been queued.
            let action_state1 = client1_rx.borrow_and_update();
            let action_state2 = client2_rx.borrow_and_update();
            // Name is random so we set force it to be the same.
            expected_action_state.name = action_state1.name.to_string();
            assert_eq!(action_state1.as_ref(), &expected_action_state);
            assert_eq!(action_state2.as_ref(), &expected_action_state);
        }

        let mut rx_from_worker = setup_new_worker(&scheduler, WORKER_ID, Default::default()).await?;

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
            let mut client3_rx = setup_action(&scheduler, action_digest.clone(), Default::default()).await?;
            assert_eq!(client3_rx.borrow_and_update().as_ref(), &expected_action_state);
        }

        Ok(())
    }

    #[tokio::test]
    async fn worker_disconnects_does_not_schedule_for_execution_test() -> Result<(), Error> {
        const WORKER_ID: WorkerId = WorkerId(0x100010);
        let scheduler = Scheduler::new(&SchedulerConfig::default());
        let action_digest = DigestInfo::new([99u8; 32], 512);

        let rx_from_worker = setup_new_worker(&scheduler, WORKER_ID, Default::default()).await?;

        // Now act like the worker disconnected.
        drop(rx_from_worker);

        let mut client_rx = setup_action(&scheduler, action_digest.clone(), Default::default()).await?;
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

    #[tokio::test]
    async fn worker_timesout_reschedules_running_job_test() -> Result<(), Error> {
        const WORKER_ID1: WorkerId = WorkerId(0x111111);
        const WORKER_ID2: WorkerId = WorkerId(0x222222);
        let scheduler = Scheduler::new(&SchedulerConfig {
            worker_timeout_s: WORKER_TIMEOUT_S,
            ..Default::default()
        });
        let action_digest = DigestInfo::new([99u8; 32], 512);

        // Note: This needs to stay in scope or a disconnect will trigger.
        let mut rx_from_worker1 = setup_new_worker(&scheduler, WORKER_ID1, Default::default()).await?;
        let mut client_rx = setup_action(&scheduler, action_digest.clone(), Default::default()).await?;

        // Note: This needs to stay in scope or a disconnect will trigger.
        let mut rx_from_worker2 = setup_new_worker(&scheduler, WORKER_ID2, Default::default()).await?;

        let mut expected_action_state = ActionState {
            // Name is a random string, so we ignore it and just make it the same.
            name: "UNKNOWN_HERE".to_string(),
            action_digest: action_digest.clone(),
            stage: ActionStage::Executing,
        };

        let execution_request_for_worker = UpdateForWorker {
            update: Some(update_for_worker::Update::StartAction(StartExecute {
                execute_request: Some(ExecuteRequest {
                    instance_name: INSTANCE_NAME.to_string(),
                    skip_cache_lookup: true,
                    action_digest: Some(action_digest.clone().into()),
                    ..Default::default()
                }),
            })),
        };

        {
            // Worker1 should now see execution request.
            let msg_for_worker = rx_from_worker1.recv().await.unwrap();
            assert_eq!(msg_for_worker, execution_request_for_worker);
        }

        {
            // Client should get notification saying it's being executed.
            let action_state = client_rx.borrow_and_update();
            // We now know the name of the action so populate it.
            expected_action_state.name = action_state.name.clone();
            assert_eq!(action_state.as_ref(), &expected_action_state);
        }

        // Keep worker 2 alive.
        scheduler
            .worker_keep_alive_received(&WORKER_ID2, NOW_TIME + WORKER_TIMEOUT_S)
            .await?;
        // This should remove worker 1 (the one executing our job).
        scheduler.remove_timedout_workers(NOW_TIME + WORKER_TIMEOUT_S).await?;

        {
            // Worker1 should have received a disconnect message.
            let msg_for_worker = rx_from_worker1.recv().await.unwrap();
            assert_eq!(
                msg_for_worker,
                UpdateForWorker {
                    update: Some(update_for_worker::Update::Disconnect(()))
                }
            );
        }
        {
            // Client should get notification saying it's being executed.
            let action_state = client_rx.borrow_and_update();
            expected_action_state.stage = ActionStage::Executing;
            assert_eq!(action_state.as_ref(), &expected_action_state);
        }
        {
            // Worker2 should now see execution request.
            let msg_for_worker = rx_from_worker2.recv().await.unwrap();
            assert_eq!(msg_for_worker, execution_request_for_worker);
        }

        Ok(())
    }
}
