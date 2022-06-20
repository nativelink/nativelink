// Copyright 2021-2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, watch};

use action_messages::{
    ActionInfo, ActionInfoHashKey, ActionResult, ActionStage, ActionState, DirectoryInfo, ExecutionMetadata, FileInfo,
    NameOrPath, SymlinkInfo,
};
use common::DigestInfo;
use config::cas_server::SchedulerConfig;
use error::{make_err, Code, Error, ResultExt};
use platform_property_manager::{PlatformProperties, PlatformPropertyValue};
use proto::build::bazel::remote::execution::v2::ExecuteRequest;
use proto::com::github::allada::turbo_cache::remote_execution::{
    update_for_worker, ConnectionResult, StartExecute, UpdateForWorker,
};
use scheduler::{Scheduler, INTERNAL_ERROR_EXIT_CODE};
use worker::{Worker, WorkerId};

const INSTANCE_NAME: &str = "foobar_instance_name";

fn make_base_action_info() -> ActionInfo {
    ActionInfo {
        instance_name: INSTANCE_NAME.to_string(),
        command_digest: DigestInfo::new([0u8; 32], 0),
        input_root_digest: DigestInfo::new([0u8; 32], 0),
        timeout: Duration::MAX,
        platform_properties: PlatformProperties {
            properties: HashMap::new(),
        },
        priority: 0,
        insert_timestamp: SystemTime::now(),
        unique_qualifier: ActionInfoHashKey {
            digest: DigestInfo::new([0u8; 32], 0),
            salt: 0,
        },
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

fn make_system_time(add_time: u64) -> SystemTime {
    UNIX_EPOCH
        .checked_add(Duration::from_secs(NOW_TIME + add_time))
        .unwrap()
}

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
    action_info.unique_qualifier.digest = action_digest;
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
                    salt: 0,
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
                salt: 0,
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
                    salt: 0,
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
                    salt: 0,
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
                salt: 0,
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

    #[tokio::test]
    async fn update_action_sends_completed_result_to_client_test() -> Result<(), Error> {
        const WORKER_ID: WorkerId = WorkerId(0x123456789111);

        let scheduler = Scheduler::new(&SchedulerConfig::default());
        let action_digest = DigestInfo::new([99u8; 32], 512);

        let mut rx_from_worker = setup_new_worker(&scheduler, WORKER_ID, Default::default()).await?;
        let mut client_rx = setup_action(&scheduler, action_digest.clone(), Default::default()).await?;

        {
            // Other tests check full data. We only care if we got StartAction.
            match rx_from_worker.recv().await.unwrap().update {
                Some(update_for_worker::Update::StartAction(_)) => { /* Success */ }
                v => assert!(false, "Expected StartAction, got : {:?}", v),
            }
            // Other tests check full data. We only care if client thinks we are Executing.
            assert_eq!(client_rx.borrow_and_update().stage, ActionStage::Executing);
        }

        let action_info_hash_key = ActionInfoHashKey {
            digest: action_digest.clone(),
            salt: 0,
        };
        let action_result = ActionResult {
            output_files: vec![FileInfo {
                name_or_path: NameOrPath::Name("hello".to_string()),
                digest: DigestInfo::new([5u8; 32], 18),
                is_executable: true,
            }],
            output_folders: vec![DirectoryInfo {
                path: "123".to_string(),
                tree_digest: DigestInfo::new([9u8; 32], 100),
            }],
            output_file_symlinks: vec![SymlinkInfo {
                name_or_path: NameOrPath::Name("foo".to_string()),
                target: "bar".to_string(),
            }],
            output_directory_symlinks: vec![SymlinkInfo {
                name_or_path: NameOrPath::Name("foo2".to_string()),
                target: "bar2".to_string(),
            }],
            exit_code: 0,
            stdout_digest: DigestInfo::new([6u8; 32], 19),
            stderr_digest: DigestInfo::new([7u8; 32], 20),
            execution_metadata: ExecutionMetadata {
                worker: WORKER_ID.to_string(),
                queued_timestamp: make_system_time(5),
                worker_start_timestamp: make_system_time(6),
                worker_completed_timestamp: make_system_time(7),
                input_fetch_start_timestamp: make_system_time(8),
                input_fetch_completed_timestamp: make_system_time(9),
                execution_start_timestamp: make_system_time(10),
                execution_completed_timestamp: make_system_time(11),
                output_upload_start_timestamp: make_system_time(12),
                output_upload_completed_timestamp: make_system_time(13),
            },
            server_logs: Default::default(),
        };
        scheduler
            .update_action(
                &WORKER_ID,
                &action_info_hash_key,
                ActionStage::Completed(action_result.clone()),
            )
            .await?;

        {
            // Client should get notification saying it has been completed.
            let action_state = client_rx.borrow_and_update();
            let expected_action_state = ActionState {
                // Name is a random string, so we ignore it and just make it the same.
                name: action_state.name.clone(),
                action_digest: action_digest.clone(),
                stage: ActionStage::Completed(action_result),
            };
            assert_eq!(action_state.as_ref(), &expected_action_state);
        }
        {
            // Update info for the action should now be closed (notification happens through Err).
            let result = client_rx.changed().await;
            assert!(result.is_err(), "Expected result to be an error : {:?}", result);
        }

        Ok(())
    }

    #[tokio::test]
    async fn update_action_with_wrong_worker_id_errors_test() -> Result<(), Error> {
        const GOOD_WORKER_ID: WorkerId = WorkerId(0x123456789111);
        const ROGUE_WORKER_ID: WorkerId = WorkerId(0x987654321);

        let scheduler = Scheduler::new(&SchedulerConfig::default());
        let action_digest = DigestInfo::new([99u8; 32], 512);

        let mut rx_from_worker = setup_new_worker(&scheduler, GOOD_WORKER_ID, Default::default()).await?;
        let mut client_rx = setup_action(&scheduler, action_digest.clone(), Default::default()).await?;

        {
            // Other tests check full data. We only care if we got StartAction.
            match rx_from_worker.recv().await.unwrap().update {
                Some(update_for_worker::Update::StartAction(_)) => { /* Success */ }
                v => assert!(false, "Expected StartAction, got : {:?}", v),
            }
            // Other tests check full data. We only care if client thinks we are Executing.
            assert_eq!(client_rx.borrow_and_update().stage, ActionStage::Executing);
        }

        let action_info_hash_key = ActionInfoHashKey {
            digest: action_digest.clone(),
            salt: 0,
        };
        let action_result = ActionResult {
            output_files: Default::default(),
            output_folders: Default::default(),
            output_file_symlinks: Default::default(),
            output_directory_symlinks: Default::default(),
            exit_code: 0,
            stdout_digest: DigestInfo::new([6u8; 32], 19),
            stderr_digest: DigestInfo::new([7u8; 32], 20),
            execution_metadata: ExecutionMetadata {
                worker: GOOD_WORKER_ID.to_string(),
                queued_timestamp: make_system_time(5),
                worker_start_timestamp: make_system_time(6),
                worker_completed_timestamp: make_system_time(7),
                input_fetch_start_timestamp: make_system_time(8),
                input_fetch_completed_timestamp: make_system_time(9),
                execution_start_timestamp: make_system_time(10),
                execution_completed_timestamp: make_system_time(11),
                output_upload_start_timestamp: make_system_time(12),
                output_upload_completed_timestamp: make_system_time(13),
            },
            server_logs: Default::default(),
        };
        let update_action_result = scheduler
            .update_action(
                &ROGUE_WORKER_ID,
                &action_info_hash_key,
                ActionStage::Completed(action_result.clone()),
            )
            .await;

        {
            // Our request should have sent an error back.
            assert!(
                update_action_result.is_err(),
                "Expected error, got: {:?}",
                &update_action_result
            );
            const EXPECTED_ERR: &str = "Got a result from a worker that should not be running the action";
            let err = update_action_result.unwrap_err();
            assert!(
                err.to_string().contains(EXPECTED_ERR),
                "Error should contain '{}', got: {:?}",
                EXPECTED_ERR,
                err
            );
        }
        {
            // Ensure client did not get notified.
            assert_eq!(
                client_rx.has_changed().unwrap(),
                false,
                "Client should not have been notified of event"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn does_not_crash_if_operation_joined_then_relaunched() -> Result<(), Error> {
        const WORKER_ID: WorkerId = WorkerId(0x10000f);

        let scheduler = Scheduler::new(&SchedulerConfig::default());
        let action_digest = DigestInfo::new([99u8; 32], 512);

        let mut expected_action_state = ActionState {
            name: "".to_string(), // Will be filled later.
            action_digest: action_digest.clone(),
            stage: ActionStage::Executing,
        };

        let mut client_rx = setup_action(&scheduler, action_digest.clone(), Default::default()).await?;
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
                    salt: 0,
                })),
            };
            let msg_for_worker = rx_from_worker.recv().await.unwrap();
            assert_eq!(msg_for_worker, expected_msg_for_worker);
        }

        {
            // Client should get notification saying it's being executed.
            let action_state = client_rx.borrow_and_update();
            // We now know the name of the action so populate it.
            expected_action_state.name = action_state.name.clone();
            assert_eq!(action_state.as_ref(), &expected_action_state);
        }

        let action_result = ActionResult {
            output_files: Default::default(),
            output_folders: Default::default(),
            output_directory_symlinks: Default::default(),
            output_file_symlinks: Default::default(),
            exit_code: Default::default(),
            stdout_digest: DigestInfo::new([1u8; 32], 512),
            stderr_digest: DigestInfo::new([2u8; 32], 512),
            execution_metadata: ExecutionMetadata {
                worker: "".to_string(),
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
        };

        scheduler
            .update_action(
                &WORKER_ID,
                &ActionInfoHashKey {
                    digest: action_digest.clone(),
                    salt: 0,
                },
                ActionStage::Completed(action_result.clone()),
            )
            .await?;

        {
            // Action should now be executing.
            expected_action_state.stage = ActionStage::Completed(action_result.clone());
            assert_eq!(client_rx.borrow_and_update().as_ref(), &expected_action_state);
        }

        // Now we need to ensure that if we schedule another execution of the same job it doesn't
        // fail.

        {
            let mut client_rx = setup_action(&scheduler, action_digest.clone(), Default::default()).await?;
            // We didn't disconnect our worker, so it will have scheduled it to the worker.
            expected_action_state.stage = ActionStage::Executing;
            let action_state = client_rx.borrow_and_update();
            // The name of the action changed (since it's a new action), so update it.
            expected_action_state.name = action_state.name.clone();
            assert_eq!(action_state.as_ref(), &expected_action_state);
        }

        Ok(())
    }

    /// This tests to ensure that platform property restrictions allow jobs to continue to run after
    /// a job finished on a specific worker (eg: restore platform properties).
    #[tokio::test]
    async fn run_two_jobs_on_same_worker_with_platform_properties_restrictions() -> Result<(), Error> {
        const WORKER_ID: WorkerId = WorkerId(0x123456789111);

        let scheduler = Scheduler::new(&SchedulerConfig::default());
        let action_digest1 = DigestInfo::new([11u8; 32], 512);
        let action_digest2 = DigestInfo::new([99u8; 32], 512);

        let mut properties = HashMap::new();
        properties.insert("prop1".to_string(), PlatformPropertyValue::Minimum(1));
        let platform_properties = PlatformProperties { properties };
        let mut rx_from_worker = setup_new_worker(&scheduler, WORKER_ID, platform_properties.clone()).await?;
        let mut client1_rx = setup_action(&scheduler, action_digest1.clone(), platform_properties.clone()).await?;
        let mut client2_rx = setup_action(&scheduler, action_digest2.clone(), platform_properties).await?;

        match rx_from_worker.recv().await.unwrap().update {
            Some(update_for_worker::Update::StartAction(_)) => { /* Success */ }
            v => assert!(false, "Expected StartAction, got : {:?}", v),
        }
        {
            // First client should be in an Executing state.
            assert_eq!(client1_rx.borrow_and_update().stage, ActionStage::Executing);
            // Second client should be in a queued state.
            assert_eq!(client2_rx.borrow_and_update().stage, ActionStage::Queued);
        }

        let action_result = ActionResult {
            output_files: Default::default(),
            output_folders: Default::default(),
            output_file_symlinks: Default::default(),
            output_directory_symlinks: Default::default(),
            exit_code: 0,
            stdout_digest: DigestInfo::new([6u8; 32], 19),
            stderr_digest: DigestInfo::new([7u8; 32], 20),
            execution_metadata: ExecutionMetadata {
                worker: WORKER_ID.to_string(),
                queued_timestamp: make_system_time(5),
                worker_start_timestamp: make_system_time(6),
                worker_completed_timestamp: make_system_time(7),
                input_fetch_start_timestamp: make_system_time(8),
                input_fetch_completed_timestamp: make_system_time(9),
                execution_start_timestamp: make_system_time(10),
                execution_completed_timestamp: make_system_time(11),
                output_upload_start_timestamp: make_system_time(12),
                output_upload_completed_timestamp: make_system_time(13),
            },
            server_logs: Default::default(),
        };

        // Tell scheduler our first task is completed.
        scheduler
            .update_action(
                &WORKER_ID,
                &ActionInfoHashKey {
                    digest: action_digest1.clone(),
                    salt: 0,
                },
                ActionStage::Completed(action_result.clone()),
            )
            .await?;

        // Ensure client did not get notified.
        assert!(
            client1_rx.changed().await.is_ok(),
            "Client should have been notified of event"
        );

        {
            // First action should now be completed.
            let action_state = client1_rx.borrow_and_update();
            let mut expected_action_state = ActionState {
                // Name is a random string, so we ignore it and just make it the same.
                name: action_state.name.clone(),
                action_digest: action_digest1.clone(),
                stage: ActionStage::Completed(action_result.clone()),
            };
            // We now know the name of the action so populate it.
            expected_action_state.name = action_state.name.clone();
            assert_eq!(action_state.as_ref(), &expected_action_state);
        }

        // At this stage it should have added back any platform_properties and the next
        // task should be executing on the same worker.

        {
            // Our second client should now executing.
            match rx_from_worker.recv().await.unwrap().update {
                Some(update_for_worker::Update::StartAction(_)) => { /* Success */ }
                v => assert!(false, "Expected StartAction, got : {:?}", v),
            }
            // Other tests check full data. We only care if client thinks we are Executing.
            assert_eq!(client2_rx.borrow_and_update().stage, ActionStage::Executing);
        }

        // Tell scheduler our second task is completed.
        scheduler
            .update_action(
                &WORKER_ID,
                &ActionInfoHashKey {
                    digest: action_digest2.clone(),
                    salt: 0,
                },
                ActionStage::Completed(action_result.clone()),
            )
            .await?;

        {
            // Our second client should be notified it completed.
            let action_state = client2_rx.borrow_and_update();
            let mut expected_action_state = ActionState {
                // Name is a random string, so we ignore it and just make it the same.
                name: action_state.name.clone(),
                action_digest: action_digest2.clone(),
                stage: ActionStage::Completed(action_result.clone()),
            };
            // We now know the name of the action so populate it.
            expected_action_state.name = action_state.name.clone();
            assert_eq!(action_state.as_ref(), &expected_action_state);
        }

        Ok(())
    }

    #[tokio::test]
    async fn worker_retries_on_internal_error_and_fails_test() -> Result<(), Error> {
        const WORKER_ID: WorkerId = WorkerId(0x123456789111);

        let scheduler = Scheduler::new(&SchedulerConfig {
            max_job_retries: 2,
            ..Default::default()
        });
        let action_digest = DigestInfo::new([99u8; 32], 512);

        let mut rx_from_worker = setup_new_worker(&scheduler, WORKER_ID, Default::default()).await?;
        let mut client_rx = setup_action(&scheduler, action_digest.clone(), Default::default()).await?;

        {
            // Other tests check full data. We only care if we got StartAction.
            match rx_from_worker.recv().await.unwrap().update {
                Some(update_for_worker::Update::StartAction(_)) => { /* Success */ }
                v => assert!(false, "Expected StartAction, got : {:?}", v),
            }
            // Other tests check full data. We only care if client thinks we are Executing.
            assert_eq!(client_rx.borrow_and_update().stage, ActionStage::Executing);
        }

        let action_info_hash_key = ActionInfoHashKey {
            digest: action_digest.clone(),
            salt: 0,
        };
        scheduler
            .update_worker_with_internal_error(
                &WORKER_ID,
                &action_info_hash_key,
                make_err!(Code::Internal, "Some error"),
            )
            .await;

        {
            // Client should get notification saying it has been queued again.
            let action_state = client_rx.borrow_and_update();
            let expected_action_state = ActionState {
                // Name is a random string, so we ignore it and just make it the same.
                name: action_state.name.clone(),
                action_digest: action_digest.clone(),
                stage: ActionStage::Queued,
            };
            assert_eq!(action_state.as_ref(), &expected_action_state);
        }

        // Now connect a new worker and it should pickup the action.
        let mut rx_from_worker = setup_new_worker(&scheduler, WORKER_ID, Default::default()).await?;
        {
            // Other tests check full data. We only care if we got StartAction.
            match rx_from_worker.recv().await.unwrap().update {
                Some(update_for_worker::Update::StartAction(_)) => { /* Success */ }
                v => assert!(false, "Expected StartAction, got : {:?}", v),
            }
            // Other tests check full data. We only care if client thinks we are Executing.
            assert_eq!(client_rx.borrow_and_update().stage, ActionStage::Executing);
        }

        // Send internal error from worker again.
        scheduler
            .update_worker_with_internal_error(
                &WORKER_ID,
                &action_info_hash_key,
                make_err!(Code::Internal, "Some error"),
            )
            .await;

        {
            // Client should get notification saying it has been queued again.
            let action_state = client_rx.borrow_and_update();
            let expected_action_state = ActionState {
                // Name is a random string, so we ignore it and just make it the same.
                name: action_state.name.clone(),
                action_digest: action_digest.clone(),
                stage: ActionStage::Error((
                    make_err!(Code::Internal, "Some error"),
                    ActionResult {
                        output_files: Default::default(),
                        output_folders: Default::default(),
                        output_file_symlinks: Default::default(),
                        output_directory_symlinks: Default::default(),
                        exit_code: INTERNAL_ERROR_EXIT_CODE,
                        stdout_digest: DigestInfo::empty_digest(),
                        stderr_digest: DigestInfo::empty_digest(),
                        execution_metadata: ExecutionMetadata {
                            worker: WORKER_ID.to_string(),
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
                    },
                )),
            };
            assert_eq!(action_state.as_ref(), &expected_action_state);
        }

        Ok(())
    }
}
