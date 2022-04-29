// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio_stream::StreamExt;
use tonic::Request;

use action_messages::{ActionInfo, ActionInfoHashKey, ActionStage};
use common::DigestInfo;
use config::cas_server::{SchedulerConfig, WorkerApiConfig};
use error::{Error, ResultExt};
use platform_property_manager::PlatformProperties;
use proto::build::bazel::remote::execution::v2::{
    ActionResult as ProtoActionResult, ExecuteResponse, ExecutedActionMetadata, LogFile, OutputDirectory, OutputFile,
    OutputSymlink,
};
use proto::com::github::allada::turbo_cache::remote_execution::{
    execute_result, update_for_worker, worker_api_server::WorkerApi, ExecuteFinishedResult, ExecuteResult,
    KeepAliveRequest, SupportedProperties,
};
use proto::google::rpc::Status as ProtoStatus;
use scheduler::Scheduler;
use worker::WorkerId;
use worker_api_server::{ConnectWorkerStream, NowFn, WorkerApiServer};

const BASE_NOW_S: u64 = 10;
const BASE_WORKER_TIMEOUT_S: u64 = 100;

struct TestContext {
    scheduler: Arc<Scheduler>,
    worker_api_server: WorkerApiServer,
    connection_worker_stream: ConnectWorkerStream,
    worker_id: WorkerId,
}

fn static_now_fn() -> Result<Duration, Error> {
    Ok(Duration::from_secs(BASE_NOW_S))
}

async fn setup_api_server(worker_timeout: u64, now_fn: NowFn) -> Result<TestContext, Error> {
    const SCHEDULER_NAME: &str = "DUMMY_SCHEDULE_NAME";

    let scheduler = Arc::new(Scheduler::new(&SchedulerConfig {
        worker_timeout_s: worker_timeout,
        ..Default::default()
    }));

    let mut schedulers = HashMap::new();
    schedulers.insert(SCHEDULER_NAME.to_string(), scheduler.clone());
    let worker_api_server = WorkerApiServer::new_with_now_fn(
        &WorkerApiConfig {
            scheduler: SCHEDULER_NAME.to_string(),
        },
        &schedulers,
        now_fn,
    )
    .err_tip(|| "Error creating WorkerApiServer")?;

    let supported_properties = SupportedProperties::default();
    let mut connection_worker_stream = worker_api_server
        .connect_worker(Request::new(supported_properties))
        .await?
        .into_inner();

    let maybe_first_message = connection_worker_stream.next().await;
    assert!(maybe_first_message.is_some(), "Expected first message from stream");
    let first_update = maybe_first_message
        .unwrap()
        .err_tip(|| "Expected success result")?
        .update
        .err_tip(|| "Expected update field to be populated")?;
    let worker_id = match first_update {
        update_for_worker::Update::ConnectionResult(connection_result) => connection_result.worker_id,
        other => unreachable!("Expected ConnectionResult, got {:?}", other),
    };

    const UUID_SIZE: usize = 36;
    assert_eq!(worker_id.len(), UUID_SIZE, "Worker ID should be 36 characters");

    Ok(TestContext {
        scheduler,
        worker_api_server,
        connection_worker_stream,
        worker_id: worker_id.try_into()?,
    })
}

#[cfg(test)]
pub mod connect_worker_tests {
    use super::*;

    #[tokio::test]
    pub async fn connect_worker_adds_worker_to_scheduler_test() -> Result<(), Box<dyn std::error::Error>> {
        let test_context = setup_api_server(BASE_WORKER_TIMEOUT_S, Box::new(static_now_fn)).await?;

        let worker_exists = test_context
            .scheduler
            .contains_worker_for_test(&test_context.worker_id)
            .await;
        assert!(worker_exists, "Expected worker to exist in worker map");

        Ok(())
    }
}

#[cfg(test)]
pub mod keep_alive_tests {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    #[tokio::test]
    pub async fn server_times_out_workers_test() -> Result<(), Box<dyn std::error::Error>> {
        let test_context = setup_api_server(BASE_WORKER_TIMEOUT_S, Box::new(static_now_fn)).await?;

        let mut now_timestamp = BASE_NOW_S;
        {
            // Now change time to 1 second before timeout and ensure the worker is still in the pool.
            now_timestamp += BASE_WORKER_TIMEOUT_S - 1;
            test_context.scheduler.remove_timedout_workers(now_timestamp).await?;
            let worker_exists = test_context
                .scheduler
                .contains_worker_for_test(&test_context.worker_id)
                .await;
            assert!(worker_exists, "Expected worker to exist in worker map");
        }
        {
            // Now add 1 second and our worker should have been evicted due to timeout.
            now_timestamp += 1;
            test_context.scheduler.remove_timedout_workers(now_timestamp).await?;
            let worker_exists = test_context
                .scheduler
                .contains_worker_for_test(&test_context.worker_id)
                .await;
            assert!(!worker_exists, "Expected worker to not exist in map");
        }

        Ok(())
    }

    #[tokio::test]
    pub async fn server_does_not_timeout_if_keep_alive_test() -> Result<(), Box<dyn std::error::Error>> {
        let now_timestamp = Arc::new(Mutex::new(BASE_NOW_S));
        let now_timestamp_clone = now_timestamp.clone();
        let add_and_return_timestamp = move |add_amount: u64| -> u64 {
            let mut locked_now_timestamp = now_timestamp.lock().unwrap();
            *locked_now_timestamp += add_amount;
            *locked_now_timestamp
        };

        let test_context = setup_api_server(
            BASE_WORKER_TIMEOUT_S,
            Box::new(move || Ok(Duration::from_secs(*now_timestamp_clone.lock().unwrap()))),
        )
        .await?;
        {
            // Now change time to 1 second before timeout and ensure the worker is still in the pool.
            let timestamp = add_and_return_timestamp(BASE_WORKER_TIMEOUT_S - 1);
            test_context.scheduler.remove_timedout_workers(timestamp).await?;
            let worker_exists = test_context
                .scheduler
                .contains_worker_for_test(&test_context.worker_id)
                .await;
            assert!(worker_exists, "Expected worker to exist in worker map");
        }
        {
            // Now send keep alive.
            test_context
                .worker_api_server
                .keep_alive(Request::new(KeepAliveRequest {
                    worker_id: test_context.worker_id.to_string(),
                }))
                .await
                .err_tip(|| "Error sending keep alive")?;
        }
        {
            // Now add 1 second and our worker should still exist in our map.
            let timestamp = add_and_return_timestamp(1);
            test_context.scheduler.remove_timedout_workers(timestamp).await?;
            let worker_exists = test_context
                .scheduler
                .contains_worker_for_test(&test_context.worker_id)
                .await;
            assert!(worker_exists, "Expected worker to exist in map");
        }

        Ok(())
    }

    #[tokio::test]
    pub async fn worker_receives_keep_alive_request_test() -> Result<(), Box<dyn std::error::Error>> {
        let mut test_context = setup_api_server(BASE_WORKER_TIMEOUT_S, Box::new(static_now_fn)).await?;

        // Send keep alive to client.
        test_context
            .scheduler
            .send_keep_alive_to_worker_for_test(&test_context.worker_id)
            .await
            .err_tip(|| "Could not send keep alive to worker")?;

        {
            // Read stream and ensure it was a keep alive message.
            let maybe_message = test_context.connection_worker_stream.next().await;
            assert!(maybe_message.is_some(), "Expected next message in stream to exist");
            let update_message = maybe_message
                .unwrap()
                .err_tip(|| "Expected success result")?
                .update
                .err_tip(|| "Expected update field to be populated")?;
            assert_eq!(
                update_message,
                update_for_worker::Update::KeepAlive(()),
                "Expected KeepAlive message"
            );
        }

        Ok(())
    }
}

#[cfg(test)]
pub mod going_away_tests {
    use super::*;

    #[tokio::test]
    pub async fn going_away_removes_worker_test() -> Result<(), Box<dyn std::error::Error>> {
        let test_context = setup_api_server(BASE_WORKER_TIMEOUT_S, Box::new(static_now_fn)).await?;

        let worker_exists = test_context
            .scheduler
            .contains_worker_for_test(&test_context.worker_id)
            .await;
        assert!(worker_exists, "Expected worker to exist in worker map");

        test_context.scheduler.remove_worker(test_context.worker_id).await;

        let worker_exists = test_context
            .scheduler
            .contains_worker_for_test(&test_context.worker_id)
            .await;
        assert!(!worker_exists, "Expected worker to be removed from worker map");

        Ok(())
    }
}

#[cfg(test)]
pub mod execution_response_tests {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    fn make_system_time(time: u64) -> SystemTime {
        UNIX_EPOCH.checked_add(Duration::from_secs(time)).unwrap()
    }

    #[tokio::test]
    pub async fn execution_response_success_test() -> Result<(), Box<dyn std::error::Error>> {
        let test_context = setup_api_server(BASE_WORKER_TIMEOUT_S, Box::new(static_now_fn)).await?;

        const SALT: u64 = 5;
        let action_digest = DigestInfo::new([7u8; 32], 123);

        let action_info = ActionInfo {
            instance_name: "instance_name".to_string(),
            command_digest: DigestInfo::new([0u8; 32], 0),
            input_root_digest: DigestInfo::new([0u8; 32], 0),
            timeout: Duration::MAX,
            platform_properties: PlatformProperties {
                properties: HashMap::new(),
            },
            priority: 0,
            insert_timestamp: make_system_time(0),
            unique_qualifier: ActionInfoHashKey {
                digest: action_digest.clone(),
                salt: SALT,
            },
        };
        let mut client_action_state_receiver = test_context.scheduler.add_action(action_info).await?;

        let mut server_logs = HashMap::new();
        server_logs.insert(
            "log_name".to_string(),
            LogFile {
                digest: Some(DigestInfo::new([9u8; 32], 124).clone().into()),
                human_readable: false, // We only support non-human readable.
            },
        );
        let result = ExecuteResult {
            response: Some(execute_result::Response::Result(ExecuteFinishedResult {
                worker_id: test_context.worker_id.to_string(),
                action_digest: Some(action_digest.clone().into()),
                salt: SALT,
                execute_response: Some(ExecuteResponse {
                    result: Some(ProtoActionResult {
                        output_files: vec![OutputFile {
                            path: "some path1".to_string(),
                            digest: Some(DigestInfo::new([8u8; 32], 124).into()),
                            is_executable: true,
                            contents: Default::default(), // We don't implement this.
                            node_properties: None,
                        }],
                        output_file_symlinks: vec![OutputSymlink {
                            path: "some path3".to_string(),
                            target: "some target3".to_string(),
                            node_properties: None,
                        }],
                        output_symlinks: vec![OutputSymlink {
                            path: "some path3".to_string(),
                            target: "some target3".to_string(),
                            node_properties: None,
                        }],
                        output_directories: vec![OutputDirectory {
                            path: "some path4".to_string(),
                            tree_digest: Some(DigestInfo::new([12u8; 32], 124).into()),
                        }],
                        output_directory_symlinks: Default::default(), // Bazel deprecated this.
                        exit_code: 5,
                        stdout_raw: Default::default(), // We don't implement this.
                        stdout_digest: Some(DigestInfo::new([10u8; 32], 124).into()),
                        stderr_raw: Default::default(), // We don't implement this.
                        stderr_digest: Some(DigestInfo::new([11u8; 32], 124).into()),
                        execution_metadata: Some(ExecutedActionMetadata {
                            worker: test_context.worker_id.to_string(),
                            queued_timestamp: Some(make_system_time(1).into()),
                            worker_start_timestamp: Some(make_system_time(2).into()),
                            worker_completed_timestamp: Some(make_system_time(3).into()),
                            input_fetch_start_timestamp: Some(make_system_time(4).into()),
                            input_fetch_completed_timestamp: Some(make_system_time(5).into()),
                            execution_start_timestamp: Some(make_system_time(6).into()),
                            execution_completed_timestamp: Some(make_system_time(7).into()),
                            output_upload_start_timestamp: Some(make_system_time(8).into()),
                            output_upload_completed_timestamp: Some(make_system_time(9).into()),
                            auxiliary_metadata: vec![],
                        }),
                    }),
                    cached_result: false,
                    status: Some(ProtoStatus {
                        code: 9,
                        message: "foo".to_string(),
                        details: Default::default(),
                    }),
                    server_logs: server_logs,
                    message: "TODO(blaise.bruer) We should put a reference something like bb_browser".to_string(),
                }),
            })),
        };
        {
            // Ensure our client thinks we are executing.
            client_action_state_receiver.changed().await?;
            let action_state = client_action_state_receiver.borrow();
            assert_eq!(action_state.as_ref().stage, ActionStage::Executing);
        }

        // Now send the result of our execution to the scheduler.
        test_context
            .worker_api_server
            .execution_response(Request::new(result.clone()))
            .await?;

        {
            // Check the result that the client would have received.
            client_action_state_receiver.changed().await?;
            let client_given_state = client_action_state_receiver.borrow();
            let execute_response = if let execute_result::Response::Result(v) = result.response.unwrap() {
                v.execute_response.unwrap()
            } else {
                panic!("Expected type to be Result");
            };

            assert_eq!(client_given_state.stage, execute_response.clone().try_into()?);

            // We just checked if conversion from ExecuteResponse into ActionStage was an exact mach.
            // Now check if we cast the ActionStage into an ExecuteResponse we get the exact same struct.
            assert_eq!(execute_response, client_given_state.stage.clone().into());
        }
        Ok(())
    }
}
