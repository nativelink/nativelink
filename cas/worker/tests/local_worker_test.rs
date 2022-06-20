// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use prost::Message;
use rand::{thread_rng, Rng};
use tokio::io::AsyncWriteExt;
use tonic::Response;

use action_messages::{ActionInfo, ActionInfoHashKey, ActionResult, ActionStage, ExecutionMetadata};
use common::{encode_stream_proto, fs, DigestInfo};
use config::cas_server::{LocalWorkerConfig, WrokerProperty};
use error::{make_input_err, Error};
use fast_slow_store::FastSlowStore;
use filesystem_store::FilesystemStore;
use local_worker::new_local_worker;
use local_worker_test_utils::{setup_grpc_stream, setup_local_worker};
use memory_store::MemoryStore;
use mock_running_actions_manager::MockRunningAction;
use platform_property_manager::PlatformProperties;
use proto::build::bazel::remote::execution::v2::platform::Property;
use proto::com::github::allada::turbo_cache::remote_execution::{
    execute_result, update_for_worker::Update, ConnectionResult, ExecuteFinishedResult, ExecuteResult, StartExecute,
    SupportedProperties, UpdateForWorker,
};

/// Get temporary path from either `TEST_TMPDIR` or best effort temp directory if
/// not set.
fn make_temp_path(data: &str) -> String {
    format!(
        "{}/{}/{}",
        env::var("TEST_TMPDIR").unwrap_or(env::temp_dir().to_str().unwrap().to_string()),
        thread_rng().gen::<u64>(),
        data
    )
}

#[cfg(test)]
mod local_worker_tests {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    #[ctor::ctor]
    fn init() {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
            .format_timestamp_millis()
            .init();
    }

    #[tokio::test]
    async fn platform_properties_smoke_test() -> Result<(), Error> {
        let mut platform_properties = HashMap::new();
        platform_properties.insert(
            "foo".to_string(),
            WrokerProperty::values(vec!["bar1".to_string(), "bar2".to_string()]),
        );
        platform_properties.insert(
            "baz".to_string(),
            // Note: new lines will result in two entries for same key.
            WrokerProperty::query_cmd("echo -e 'hello\ngoodbye'".to_string()),
        );
        let mut test_context = setup_local_worker(platform_properties).await;
        let streaming_response = test_context.maybe_streaming_response.take().unwrap();

        // Now wait for our client to send `.connect_worker()` (which has our platform properties).
        let mut supported_properties = test_context.client.expect_connect_worker(Ok(streaming_response)).await;
        // It is undefined which order these will be returned in, so we sort it.
        supported_properties
            .properties
            .sort_by(|a, b| a.encode_to_vec().cmp(&b.encode_to_vec()));
        assert_eq!(
            supported_properties,
            SupportedProperties {
                properties: vec![
                    Property {
                        name: "baz".to_string(),
                        value: "hello".to_string(),
                    },
                    Property {
                        name: "baz".to_string(),
                        value: "goodbye".to_string(),
                    },
                    Property {
                        name: "foo".to_string(),
                        value: "bar1".to_string(),
                    },
                    Property {
                        name: "foo".to_string(),
                        value: "bar2".to_string(),
                    }
                ]
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn reconnect_on_server_disconnect_test() -> Result<(), Box<dyn std::error::Error>> {
        let mut test_context = setup_local_worker(HashMap::new()).await;
        let streaming_response = test_context.maybe_streaming_response.take().unwrap();

        {
            // Ensure our worker connects and properties were sent.
            let props = test_context.client.expect_connect_worker(Ok(streaming_response)).await;
            assert_eq!(props, SupportedProperties::default());
        }

        // Disconnect our grpc stream.
        test_context.maybe_tx_stream.take().unwrap().abort();

        {
            // Client should try to auto reconnect and check our properties again.
            let (_, streaming_response) = setup_grpc_stream();
            let props = test_context.client.expect_connect_worker(Ok(streaming_response)).await;
            assert_eq!(props, SupportedProperties::default());
        }

        Ok(())
    }

    #[tokio::test]
    async fn simple_worker_start_action_test() -> Result<(), Box<dyn std::error::Error>> {
        let mut test_context = setup_local_worker(HashMap::new()).await;
        let streaming_response = test_context.maybe_streaming_response.take().unwrap();

        {
            // Ensure our worker connects and properties were sent.
            let props = test_context.client.expect_connect_worker(Ok(streaming_response)).await;
            assert_eq!(props, SupportedProperties::default());
        }

        let expected_worker_id = "foobar".to_string();

        let mut tx_stream = test_context.maybe_tx_stream.take().unwrap();
        {
            // First initialize our worker by sending the response to the connection request.
            tx_stream
                .send_data(encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::ConnectionResult(ConnectionResult {
                        worker_id: expected_worker_id.clone(),
                    })),
                })?)
                .await
                .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
        }

        const SALT: u64 = 1000;
        let action_digest = DigestInfo::new([03u8; 32], 10);
        let action_info = ActionInfo {
            instance_name: "foo".to_string(),
            command_digest: DigestInfo::new([01u8; 32], 10),
            input_root_digest: DigestInfo::new([02u8; 32], 10),
            timeout: Duration::from_secs(1),
            platform_properties: PlatformProperties::default(),
            priority: 0,
            insert_timestamp: SystemTime::UNIX_EPOCH,
            unique_qualifier: ActionInfoHashKey {
                digest: action_digest.clone(),
                salt: SALT,
            },
        };

        {
            // Send execution request.
            tx_stream
                .send_data(encode_stream_proto(&UpdateForWorker {
                    update: Some(Update::StartAction(StartExecute {
                        execute_request: Some(action_info.into()),
                        salt: SALT,
                    })),
                })?)
                .await
                .map_err(|e| make_input_err!("Could not send : {:?}", e))?;
        }
        let action_result = ActionResult {
            output_files: vec![],
            output_folders: vec![],
            output_file_symlinks: vec![],
            output_directory_symlinks: vec![],
            exit_code: 5,
            stdout_digest: DigestInfo::new([21u8; 32], 10),
            stderr_digest: DigestInfo::new([22u8; 32], 10),
            execution_metadata: ExecutionMetadata {
                worker: expected_worker_id.clone(),
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
            server_logs: HashMap::new(),
        };
        let running_action = Arc::new(MockRunningAction::new());

        // Send and wait for response from create_and_add_action to RunningActionsManager.
        test_context
            .actions_manager
            .expect_create_and_add_action(Ok(running_action.clone()))
            .await;

        // Now the RunningAction needs to send a series of state updates. This shortcuts them
        // into a single call (shortcut for prepare, execute, upload, collect_results, cleanup).
        running_action
            .simple_expect_get_finished_result(Ok(action_result.clone()))
            .await?;

        // Now our client should be notified that our runner finished.
        let execution_response = test_context
            .client
            .expect_execution_response(Ok(Response::new(())))
            .await;

        // Now ensure the final results match our expectations.
        assert_eq!(
            execution_response,
            ExecuteResult {
                response: Some(execute_result::Response::Result(ExecuteFinishedResult {
                    worker_id: expected_worker_id,
                    action_digest: Some(action_digest.into()),
                    salt: SALT,
                    execute_response: Some(ActionStage::Completed(action_result).into()),
                }))
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn new_local_worker_creates_work_directory_test() -> Result<(), Box<dyn std::error::Error>> {
        let cas_store = Arc::new(FastSlowStore::new(
            &config::backends::FastSlowStore {
                // Note: These are not needed for this test, so we put dummy memory stores here.
                fast: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
                slow: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
            },
            Arc::new(
                FilesystemStore::new(&config::backends::FilesystemStore {
                    content_path: make_temp_path("content_path"),
                    temp_path: make_temp_path("temp_path"),
                    ..Default::default()
                })
                .await?,
            ),
            Arc::new(MemoryStore::new(&config::backends::MemoryStore::default())),
        ));
        let work_directory = make_temp_path("foo");
        new_local_worker(
            Arc::new(LocalWorkerConfig {
                work_directory: work_directory.clone(),
                ..Default::default()
            }),
            cas_store,
        )
        .await?;

        assert!(
            fs::metadata(work_directory).await.is_ok(),
            "Expected work_directory to be created"
        );

        Ok(())
    }

    #[tokio::test]
    async fn new_local_worker_removes_work_directory_before_start_test() -> Result<(), Box<dyn std::error::Error>> {
        let cas_store = Arc::new(FastSlowStore::new(
            &config::backends::FastSlowStore {
                // Note: These are not needed for this test, so we put dummy memory stores here.
                fast: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
                slow: config::backends::StoreConfig::memory(config::backends::MemoryStore::default()),
            },
            Arc::new(
                FilesystemStore::new(&config::backends::FilesystemStore {
                    content_path: make_temp_path("content_path"),
                    temp_path: make_temp_path("temp_path"),
                    ..Default::default()
                })
                .await?,
            ),
            Arc::new(MemoryStore::new(&config::backends::MemoryStore::default())),
        ));
        let work_directory = make_temp_path("foo");
        fs::create_dir_all(format!("{}/{}", work_directory, "another_dir")).await?;
        let mut file = fs::create_file(format!("{}/{}", work_directory, "foo.txt")).await?;
        file.write_all(b"Hello, world!").await?;
        new_local_worker(
            Arc::new(LocalWorkerConfig {
                work_directory: work_directory.clone(),
                ..Default::default()
            }),
            cas_store,
        )
        .await?;

        let work_directory_path_buf = PathBuf::from(work_directory);

        assert!(
            work_directory_path_buf.read_dir()?.next().is_none(),
            "Expected work_directory to have removed all files and to be empty"
        );

        Ok(())
    }
}
