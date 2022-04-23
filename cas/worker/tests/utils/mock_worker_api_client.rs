// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::sync::Arc;

use async_trait::async_trait;
use fast_async_mutex::mutex::Mutex;
use tokio::sync::mpsc;
use tonic::{Response, Status, Streaming};

use proto::com::github::allada::turbo_cache::remote_execution::{
    ExecuteResult, GoingAwayRequest, KeepAliveRequest, SupportedProperties, UpdateForWorker,
};
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
pub struct MockWorkerApiClient {
    rx_call: Arc<Mutex<mpsc::UnboundedReceiver<WorkerClientApiCalls>>>,
    tx_call: mpsc::UnboundedSender<WorkerClientApiCalls>,
    rx_resp: Arc<Mutex<mpsc::UnboundedReceiver<WorkerClientApiReturns>>>,
    tx_resp: mpsc::UnboundedSender<WorkerClientApiReturns>,
}

impl MockWorkerApiClient {
    pub fn new() -> Self {
        let (tx_call, rx_call) = mpsc::unbounded_channel();
        let (tx_resp, rx_resp) = mpsc::unbounded_channel();
        Self {
            rx_call: Arc::new(Mutex::new(rx_call)),
            tx_call,
            rx_resp: Arc::new(Mutex::new(rx_resp)),
            tx_resp,
        }
    }
}

impl MockWorkerApiClient {
    pub async fn expect_connect_worker(
        &mut self,
        result: Result<Response<Streaming<UpdateForWorker>>, Status>,
    ) -> SupportedProperties {
        let mut rx_call_lock = self.rx_call.lock().await;
        let req = match rx_call_lock.recv().await.expect("Could not receive msg in mpsc") {
            WorkerClientApiCalls::ConnectWorker(req) => req,
            req => panic!("expect_connect_worker expected ConnectWorker, got : {:?}", req),
        };
        self.tx_resp
            .send(WorkerClientApiReturns::ConnectWorker(result))
            .expect("Could not send request to mpsc");
        req
    }

    pub async fn expect_execution_response(&mut self, result: Result<Response<()>, Status>) -> ExecuteResult {
        let mut rx_call_lock = self.rx_call.lock().await;
        let req = match rx_call_lock.recv().await.expect("Could not receive msg in mpsc") {
            WorkerClientApiCalls::ExecutionResponse(req) => req,
            req => panic!("expect_execution_response expected ExecutionResponse, got : {:?}", req),
        };
        self.tx_resp
            .send(WorkerClientApiReturns::ExecutionResponse(result))
            .expect("Could not send request to mpsc");
        req
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
