// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use async_trait::async_trait;
use proto::com::github::allada::turbo_cache::remote_execution::{
    worker_api_client::WorkerApiClient, ExecuteResult, GoingAwayRequest, KeepAliveRequest, SupportedProperties,
    UpdateForWorker,
};
use tonic::{codec::Streaming, transport::Channel, Response, Status};

/// This is used in order to allow unit tests to intercept these calls. This should always match
/// the API of WorkerApiClient defined in the worker_api.proto file.
#[async_trait]
pub trait WorkerApiClientTrait: Clone + Sync + Send + Sized + Unpin {
    async fn connect_worker(
        &mut self,
        request: SupportedProperties,
    ) -> Result<Response<Streaming<UpdateForWorker>>, Status>;

    async fn keep_alive(&mut self, request: KeepAliveRequest) -> Result<Response<()>, Status>;

    async fn going_away(&mut self, request: GoingAwayRequest) -> Result<Response<()>, Status>;

    async fn execution_response(&mut self, request: ExecuteResult) -> Result<Response<()>, Status>;
}

#[derive(Clone)]
pub struct WorkerApiClientWrapper {
    inner: WorkerApiClient<Channel>,
}

impl From<WorkerApiClient<Channel>> for WorkerApiClientWrapper {
    fn from(other: WorkerApiClient<Channel>) -> Self {
        Self { inner: other }
    }
}

#[async_trait]
impl WorkerApiClientTrait for WorkerApiClientWrapper {
    async fn connect_worker(
        &mut self,
        request: SupportedProperties,
    ) -> Result<Response<Streaming<UpdateForWorker>>, Status> {
        self.inner.connect_worker(request).await
    }

    async fn keep_alive(&mut self, request: KeepAliveRequest) -> Result<Response<()>, Status> {
        self.inner.keep_alive(request).await
    }

    async fn going_away(&mut self, request: GoingAwayRequest) -> Result<Response<()>, Status> {
        self.inner.going_away(request).await
    }

    async fn execution_response(&mut self, request: ExecuteResult) -> Result<Response<()>, Status> {
        self.inner.execution_response(request).await
    }
}
