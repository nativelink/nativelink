// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::sync::Arc;

use async_trait::async_trait;
use fast_async_mutex::mutex::Mutex;
use tokio::sync::mpsc;

use action_messages::ActionResult;
use error::{make_input_err, Error};
use proto::com::github::allada::turbo_cache::remote_execution::StartExecute;
use running_actions_manager::{ActionId, RunningAction, RunningActionsManager};

#[derive(Debug)]
enum RunningActionManagerCalls {
    CreateAndAddAction((String, StartExecute)),
}

enum RunningActionManagerReturns {
    CreateAndAddAction(Result<Arc<MockRunningAction>, Error>),
}

pub struct MockRunningActionsManager {
    rx_call: Mutex<mpsc::UnboundedReceiver<RunningActionManagerCalls>>,
    tx_call: mpsc::UnboundedSender<RunningActionManagerCalls>,

    rx_resp: Mutex<mpsc::UnboundedReceiver<RunningActionManagerReturns>>,
    tx_resp: mpsc::UnboundedSender<RunningActionManagerReturns>,
}

impl MockRunningActionsManager {
    pub fn new() -> Self {
        let (tx_call, rx_call) = mpsc::unbounded_channel();
        let (tx_resp, rx_resp) = mpsc::unbounded_channel();
        Self {
            rx_call: Mutex::new(rx_call),
            tx_call,
            rx_resp: Mutex::new(rx_resp),
            tx_resp,
        }
    }
}

impl MockRunningActionsManager {
    pub async fn expect_create_and_add_action(
        &self,
        result: Result<Arc<MockRunningAction>, Error>,
    ) -> (String, StartExecute) {
        let mut rx_call_lock = self.rx_call.lock().await;
        let req = match rx_call_lock.recv().await.expect("Could not receive msg in mpsc") {
            RunningActionManagerCalls::CreateAndAddAction(req) => req,
        };
        self.tx_resp
            .send(RunningActionManagerReturns::CreateAndAddAction(result))
            .map_err(|_| make_input_err!("Could not send request to mpsc"))
            .unwrap();
        req
    }
}

#[async_trait]
impl RunningActionsManager for MockRunningActionsManager {
    type RunningAction = MockRunningAction;

    async fn create_and_add_action(
        self: Arc<Self>,
        worker_id: String,
        start_execute: StartExecute,
    ) -> Result<Arc<Self::RunningAction>, Error> {
        self.tx_call
            .send(RunningActionManagerCalls::CreateAndAddAction((
                worker_id,
                start_execute,
            )))
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock.recv().await.expect("Could not receive msg in mpsc") {
            RunningActionManagerReturns::CreateAndAddAction(result) => result,
        }
    }

    async fn get_action(&self, _action_id: &ActionId) -> Result<Arc<Self::RunningAction>, Error> {
        unimplemented!("get_action not implemented");
    }
}

#[derive(Debug)]
enum RunningActionCalls {
    PrepareAction,
    Execute,
    UploadResults,
    Cleanup,
    GetFinishedResult,
}

#[derive(Debug)]
enum RunningActionReturns {
    PrepareAction(Result<Arc<MockRunningAction>, Error>),
    Execute(Result<Arc<MockRunningAction>, Error>),
    UploadResults(Result<Arc<MockRunningAction>, Error>),
    Cleanup(Result<Arc<MockRunningAction>, Error>),
    GetFinishedResult(Result<ActionResult, Error>),
}

#[derive(Debug)]
pub struct MockRunningAction {
    rx_call: Mutex<mpsc::UnboundedReceiver<RunningActionCalls>>,
    tx_call: mpsc::UnboundedSender<RunningActionCalls>,

    rx_resp: Mutex<mpsc::UnboundedReceiver<RunningActionReturns>>,
    tx_resp: mpsc::UnboundedSender<RunningActionReturns>,
}

impl MockRunningAction {
    pub fn new() -> Self {
        let (tx_call, rx_call) = mpsc::unbounded_channel();
        let (tx_resp, rx_resp) = mpsc::unbounded_channel();
        Self {
            rx_call: Mutex::new(rx_call),
            tx_call,
            rx_resp: Mutex::new(rx_resp),
            tx_resp,
        }
    }

    pub async fn simple_expect_get_finished_result(
        self: &Arc<Self>,
        result: Result<ActionResult, Error>,
    ) -> Result<(), Error> {
        self.expect_prepare_action(Ok(())).await?;
        self.expect_execute(Ok(())).await?;
        self.upload_results(Ok(())).await?;
        let result = self.get_finished_result(result).await;
        self.cleanup(Ok(())).await?;
        result
    }

    pub async fn expect_prepare_action(self: &Arc<Self>, result: Result<(), Error>) -> Result<(), Error> {
        let mut rx_call_lock = self.rx_call.lock().await;
        let req = match rx_call_lock.recv().await.expect("Could not receive msg in mpsc") {
            RunningActionCalls::PrepareAction => (),
            req => panic!("expect_prepare_action expected PrepareAction, got : {:?}", req),
        };
        let result = match result {
            Ok(()) => Ok(self.clone()),
            Err(e) => Err(e),
        };
        self.tx_resp
            .send(RunningActionReturns::PrepareAction(result))
            .expect("Could not send request to mpsc");
        Ok(req)
    }

    pub async fn expect_execute(self: &Arc<Self>, result: Result<(), Error>) -> Result<(), Error> {
        let mut rx_call_lock = self.rx_call.lock().await;
        let req = match rx_call_lock.recv().await.expect("Could not receive msg in mpsc") {
            RunningActionCalls::Execute => (),
            req => panic!("expect_execute expected Execute, got : {:?}", req),
        };
        let result = match result {
            Ok(()) => Ok(self.clone()),
            Err(e) => Err(e),
        };
        self.tx_resp
            .send(RunningActionReturns::Execute(result))
            .expect("Could not send request to mpsc");
        Ok(req)
    }

    pub async fn upload_results(self: &Arc<Self>, result: Result<(), Error>) -> Result<(), Error> {
        let mut rx_call_lock = self.rx_call.lock().await;
        let req = match rx_call_lock.recv().await.expect("Could not receive msg in mpsc") {
            RunningActionCalls::UploadResults => (),
            req => panic!("expect_upload_results expected UploadResults, got : {:?}", req),
        };
        let result = match result {
            Ok(()) => Ok(self.clone()),
            Err(e) => Err(e),
        };
        self.tx_resp
            .send(RunningActionReturns::UploadResults(result))
            .expect("Could not send request to mpsc");
        Ok(req)
    }

    pub async fn cleanup(self: &Arc<Self>, result: Result<(), Error>) -> Result<(), Error> {
        let mut rx_call_lock = self.rx_call.lock().await;
        let req = match rx_call_lock.recv().await.expect("Could not receive msg in mpsc") {
            RunningActionCalls::Cleanup => (),
            req => panic!("expect_cleanup expected Cleanup, got : {:?}", req),
        };
        let result = match result {
            Ok(()) => Ok(self.clone()),
            Err(e) => Err(e),
        };
        self.tx_resp
            .send(RunningActionReturns::Cleanup(result))
            .expect("Could not send request to mpsc");
        Ok(req)
    }

    pub async fn get_finished_result(self: &Arc<Self>, result: Result<ActionResult, Error>) -> Result<(), Error> {
        let mut rx_call_lock = self.rx_call.lock().await;
        let req = match rx_call_lock.recv().await.expect("Could not receive msg in mpsc") {
            RunningActionCalls::GetFinishedResult => (),
            req => panic!("expect_get_finished_result expected GetFinishedResult, got : {:?}", req),
        };
        self.tx_resp
            .send(RunningActionReturns::GetFinishedResult(result))
            .expect("Could not send request to mpsc");
        Ok(req)
    }
}

#[async_trait]
impl RunningAction for MockRunningAction {
    async fn prepare_action(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        self.tx_call
            .send(RunningActionCalls::PrepareAction)
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock.recv().await.expect("Could not receive msg in mpsc") {
            RunningActionReturns::PrepareAction(result) => result,
            resp => panic!(
                "execution_response expected PrepareAction response, received {:?}",
                resp
            ),
        }
    }

    async fn execute(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        self.tx_call
            .send(RunningActionCalls::Execute)
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock.recv().await.expect("Could not receive msg in mpsc") {
            RunningActionReturns::Execute(result) => result,
            resp => panic!("execution_response expected Execute response, received {:?}", resp),
        }
    }

    async fn upload_results(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        self.tx_call
            .send(RunningActionCalls::UploadResults)
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock.recv().await.expect("Could not receive msg in mpsc") {
            RunningActionReturns::UploadResults(result) => result,
            resp => panic!(
                "execution_response expected UploadResults response, received {:?}",
                resp
            ),
        }
    }

    async fn cleanup(self: Arc<Self>) -> Result<Arc<Self>, Error> {
        self.tx_call
            .send(RunningActionCalls::Cleanup)
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock.recv().await.expect("Could not receive msg in mpsc") {
            RunningActionReturns::Cleanup(result) => result,
            resp => panic!("execution_response expected Cleanup response, received {:?}", resp),
        }
    }

    async fn get_finished_result(self: Arc<Self>) -> Result<ActionResult, Error> {
        self.tx_call
            .send(RunningActionCalls::GetFinishedResult)
            .expect("Could not send request to mpsc");
        let mut rx_resp_lock = self.rx_resp.lock().await;
        match rx_resp_lock.recv().await.expect("Could not receive msg in mpsc") {
            RunningActionReturns::GetFinishedResult(result) => result,
            resp => panic!(
                "execution_response expected GetFinishedResult response, received {:?}",
                resp
            ),
        }
    }
}
