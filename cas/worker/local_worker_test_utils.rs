// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use fast_async_mutex::mutex::Mutex;
use hyper::body::Sender as HyperSender;
use tokio::sync::mpsc;
use tonic::{
    codec::Codec, // Needed for .decoder().
    codec::ProstCodec,
    transport::Body,
    Response,
    Status,
    Streaming,
};

use common::JoinHandleDropGuard;
use config::cas_server::{EndpointConfig, LocalWorkerConfig, WrokerProperty};
use error::Error;
use local_worker::LocalWorker;
use proto::com::github::allada::turbo_cache::remote_execution::{
    ExecuteFinishedResult, ExecuteResult, GoingAwayRequest, KeepAliveRequest, StartExecute, SupportedProperties,
    UpdateForWorker,
};
use running_actions_manager::RunningActionsManager;
use worker_api_client_wrapper::WorkerApiClientTrait;

#[derive(Debug)]
enum WorkerClientApiCalls {
    ConnectWorker(SupportedProperties),
    ExecutionResponse(ExecuteResult),
}

#[derive(Debug)]
enum WorkerClientApiReturns {
    ConnectWorker(Result<Response<Streaming<UpdateForWorker>>, Status>),
    ExecutionResponse(Result<Response<()>, Status>),
}

#[derive(Clone)]
struct MockWorkerApiClient {
    tx_call: mpsc::UnboundedSender<WorkerClientApiCalls>,
    rx_resp: Arc<Mutex<mpsc::UnboundedReceiver<WorkerClientApiReturns>>>,
}

impl MockWorkerApiClient {
    fn new() -> (
        Self,
        mpsc::UnboundedReceiver<WorkerClientApiCalls>,
        mpsc::UnboundedSender<WorkerClientApiReturns>,
    ) {
        let (tx_call, rx_call) = mpsc::unbounded_channel();
        let (tx_resp, rx_resp) = mpsc::unbounded_channel();
        (
            Self {
                tx_call,
                rx_resp: Arc::new(Mutex::new(rx_resp)),
            },
            rx_call,
            tx_resp,
        )
    }
}

#[async_trait]
impl WorkerApiClientTrait for MockWorkerApiClient {
    async fn connect_worker(
        &mut self,
        request: SupportedProperties,
    ) -> Result<Response<Streaming<UpdateForWorker>>, Status> {
        self.tx_call
            .send(WorkerClientApiCalls::ConnectWorker(request))
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock.recv().await.expect("Could not receive msg in mpsc") {
            WorkerClientApiReturns::ConnectWorker(result) => result,
            resp => panic!("connect_worker expected ConnectWorker response, received {:?}", resp),
        }
    }

    async fn keep_alive(&mut self, _request: KeepAliveRequest) -> Result<Response<()>, Status> {
        unreachable!();
    }

    async fn going_away(&mut self, _request: GoingAwayRequest) -> Result<Response<()>, Status> {
        unreachable!();
    }

    async fn execution_response(&mut self, request: ExecuteResult) -> Result<Response<()>, Status> {
        self.tx_call
            .send(WorkerClientApiCalls::ExecutionResponse(request))
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock.recv().await.expect("Could not receive msg in mpsc") {
            WorkerClientApiReturns::ExecutionResponse(result) => result,
            resp => panic!(
                "execution_response expected ExecutionResponse response, received {:?}",
                resp
            ),
        }
    }
}

#[derive(Debug)]
enum RunnerApiCalls {
    StartAction(StartExecute),
}

#[derive(Debug)]
enum RunnerApiReturns {
    StartAction(Result<ExecuteFinishedResult, Error>),
}

struct MockRunningActionsManager {
    tx_call: mpsc::UnboundedSender<RunnerApiCalls>,
    rx_resp: Mutex<mpsc::UnboundedReceiver<RunnerApiReturns>>,
}

impl MockRunningActionsManager {
    fn new() -> (
        Self,
        mpsc::UnboundedReceiver<RunnerApiCalls>,
        mpsc::UnboundedSender<RunnerApiReturns>,
    ) {
        let (tx_call, rx_call) = mpsc::unbounded_channel();
        let (tx_resp, rx_resp) = mpsc::unbounded_channel();
        (
            Self {
                tx_call,
                rx_resp: Mutex::new(rx_resp),
            },
            rx_call,
            tx_resp,
        )
    }
}

#[async_trait]
impl RunningActionsManager for MockRunningActionsManager {
    async fn start_action(self: Arc<Self>, start_execute: StartExecute) -> Result<ExecuteFinishedResult, Error> {
        self.tx_call
            .send(RunnerApiCalls::StartAction(start_execute))
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock.recv().await.expect("Could not receive msg in mpsc") {
            RunnerApiReturns::StartAction(result) => result,
        }
    }
}

pub struct TestContext {
    _client: MockWorkerApiClient,
    pub maybe_streaming_response: Option<Response<Streaming<UpdateForWorker>>>,
    pub maybe_tx_stream: Option<HyperSender>,

    rx_call: mpsc::UnboundedReceiver<WorkerClientApiCalls>,
    tx_resp: mpsc::UnboundedSender<WorkerClientApiReturns>,

    am_rx_call: mpsc::UnboundedReceiver<RunnerApiCalls>,
    am_tx_resp: mpsc::UnboundedSender<RunnerApiReturns>,

    _drop_guard: JoinHandleDropGuard<Result<(), Error>>,
}

impl TestContext {
    pub async fn am_expect_start_action(&mut self, result: Result<ExecuteFinishedResult, Error>) -> StartExecute {
        let req = match self.am_rx_call.recv().await.expect("Could not receive msg in mpsc") {
            RunnerApiCalls::StartAction(req) => req,
        };
        self.am_tx_resp
            .send(RunnerApiReturns::StartAction(result))
            .expect("Could not send request to mpsc");
        req
    }

    pub async fn expect_connect_worker(
        &mut self,
        result: Result<Response<Streaming<UpdateForWorker>>, Status>,
    ) -> SupportedProperties {
        let req = match self.rx_call.recv().await.expect("Could not receive msg in mpsc") {
            WorkerClientApiCalls::ConnectWorker(req) => req,
            req => panic!("expect_connect_worker expected ConnectWorker, got : {:?}", req),
        };
        self.tx_resp
            .send(WorkerClientApiReturns::ConnectWorker(result))
            .expect("Could not send request to mpsc");
        req
    }

    pub async fn expect_execution_response(&mut self, result: Result<Response<()>, Status>) -> ExecuteResult {
        let req = match self.rx_call.recv().await.expect("Could not receive msg in mpsc") {
            WorkerClientApiCalls::ExecutionResponse(req) => req,
            req => panic!("expect_execution_response expected ExecutionResponse, got : {:?}", req),
        };
        self.tx_resp
            .send(WorkerClientApiReturns::ExecutionResponse(result))
            .expect("Could not send request to mpsc");
        req
    }
}

pub fn setup_grpc_stream() -> (HyperSender, Response<Streaming<UpdateForWorker>>) {
    let (tx, body) = Body::channel();
    let mut codec = ProstCodec::<UpdateForWorker, UpdateForWorker>::default();
    // Note: This is an undocumented function.
    let stream = Streaming::new_request(codec.decoder(), body);
    (tx, Response::new(stream))
}

pub async fn setup_local_worker(platform_properties: HashMap<String, WrokerProperty>) -> TestContext {
    let (mock_worker_api_client, rx_call, tx_resp) = MockWorkerApiClient::new();
    let mock_worker_api_client_clone = mock_worker_api_client.clone();
    let (actions_manager, am_rx_call, am_tx_resp) = MockRunningActionsManager::new();
    let local_worker_config = LocalWorkerConfig {
        platform_properties,
        worker_api_endpoint: EndpointConfig {
            timeout: Some(10000.),
            ..Default::default()
        },
        ..Default::default()
    };
    let worker = LocalWorker::new_with_connection_factory_and_actions_manager(
        Arc::new(local_worker_config),
        Arc::new(actions_manager),
        Box::new(move || {
            let mock_worker_api_client = mock_worker_api_client_clone.clone();
            Box::pin(async move { Ok(mock_worker_api_client) })
        }),
        Box::new(move |_| Box::pin(async move { /* No sleep */ })),
    );
    let drop_guard = JoinHandleDropGuard::new(tokio::spawn(async move { worker.run().await }));

    let (tx_stream, streaming_response) = setup_grpc_stream();
    TestContext {
        _client: mock_worker_api_client,
        maybe_streaming_response: Some(streaming_response),
        maybe_tx_stream: Some(tx_stream),
        rx_call,
        tx_resp,
        am_rx_call,
        am_tx_resp,
        _drop_guard: drop_guard,
    }
}
