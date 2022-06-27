// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::sync::Arc;

use hyper::body::Sender as HyperSender;
use tonic::{
    codec::Codec, // Needed for .decoder().
    codec::CompressionEncoding,
    codec::ProstCodec,
    transport::Body,
    Response,
    Streaming,
};

use common::JoinHandleDropGuard;
use config::cas_server::{EndpointConfig, LocalWorkerConfig, WrokerProperty};
use error::Error;
use local_worker::LocalWorker;
use mock_running_actions_manager::MockRunningActionsManager;
use mock_worker_api_client::MockWorkerApiClient;
use proto::com::github::allada::turbo_cache::remote_execution::UpdateForWorker;

pub fn setup_grpc_stream() -> (HyperSender, Response<Streaming<UpdateForWorker>>) {
    let (tx, body) = Body::channel();
    let mut codec = ProstCodec::<UpdateForWorker, UpdateForWorker>::default();
    // Note: This is an undocumented function.
    let stream = Streaming::new_request(codec.decoder(), body, Some(CompressionEncoding::Gzip));
    (tx, Response::new(stream))
}

pub async fn setup_local_worker(platform_properties: HashMap<String, WrokerProperty>) -> TestContext {
    let mock_worker_api_client = MockWorkerApiClient::new();
    let mock_worker_api_client_clone = mock_worker_api_client.clone();
    let actions_manager = Arc::new(MockRunningActionsManager::new());
    const ARBITRARY_LARGE_TIMEOUT: f32 = 10000.;
    let local_worker_config = LocalWorkerConfig {
        platform_properties,
        worker_api_endpoint: EndpointConfig {
            timeout: Some(ARBITRARY_LARGE_TIMEOUT),
            ..Default::default()
        },
        ..Default::default()
    };
    let worker = LocalWorker::new_with_connection_factory_and_actions_manager(
        Arc::new(local_worker_config),
        actions_manager.clone(),
        Box::new(move || {
            let mock_worker_api_client = mock_worker_api_client_clone.clone();
            Box::pin(async move { Ok(mock_worker_api_client) })
        }),
        Box::new(move |_| Box::pin(async move { /* No sleep */ })),
    );
    let drop_guard = JoinHandleDropGuard::new(tokio::spawn(async move { worker.run().await }));

    let (tx_stream, streaming_response) = setup_grpc_stream();
    TestContext {
        client: mock_worker_api_client,
        actions_manager,

        maybe_streaming_response: Some(streaming_response),
        maybe_tx_stream: Some(tx_stream),

        _drop_guard: drop_guard,
    }
}

pub struct TestContext {
    pub client: MockWorkerApiClient,
    pub actions_manager: Arc<MockRunningActionsManager>,

    pub maybe_streaming_response: Option<Response<Streaming<UpdateForWorker>>>,
    pub maybe_tx_stream: Option<HyperSender>,

    _drop_guard: JoinHandleDropGuard<Result<(), Error>>,
}
