// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_stream::StreamExt;
use tonic::Request;

use config::cas_server::SchedulerConfig;
use error::{Error, ResultExt};
use platform_property_manager::PlatformPropertyManager;
use proto::com::github::allada::turbo_cache::remote_execution::{
    update_for_worker, worker_api_server::WorkerApi, KeepAliveRequest, SupportedProperties,
};
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
    let platform_properties = HashMap::new();
    let scheduler = Arc::new(Scheduler::new(&SchedulerConfig {
        worker_timeout_s: worker_timeout,
        ..Default::default()
    }));
    let worker_api_server = WorkerApiServer::new_with_now_fn(
        Arc::new(PlatformPropertyManager::new(platform_properties)),
        scheduler.clone(),
        now_fn,
    );

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
