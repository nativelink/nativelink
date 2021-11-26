// Copyright 2020 Nathan (Blaise) Bruer.  All rights reserved.
//// Communication from the worker to the scheduler.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateFromWorker {
    #[prost(oneof = "update_from_worker::Update", tags = "1, 2, 3, 4")]
    pub update: ::core::option::Option<update_from_worker::Update>,
}
/// Nested message and enum types in `UpdateFromWorker`.
pub mod update_from_worker {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Update {
        //// Message used to let the scheduler know that it is still alive as
        //// well as check to see if the scheduler is still alive. The scheduler
        //// may close the connection if the worker has not sent any messages
        //// after some amount of time (configured in the scheduler's
        //// configuration).
        #[prost(message, tag = "1")]
        KeepAlive(()),
        //// Registers this worker and informs the scheduler what properties
        //// this worker supports. This command MUST be sent before issuing
        //// any other commands. Failing to do so is undefined behavior.
        #[prost(message, tag = "2")]
        RegisterSupportedProperties(super::RegisterSupportedProperties),
        //// Informs the scheduler about the result of an execution request.
        #[prost(message, tag = "3")]
        ExecuteResult(super::ExecuteResult),
        //// Informs the scheduler that the service is going offline and
        //// should stop issuing any new actions on this worker.
        ////
        //// The worker may stay connected even after sending this command
        //// and may even send an `ExecuteResult` after sending this command.
        //// It is up to the scheduler implementation to decide how to handle
        //// this case.
        ////
        //// Any job that was running on this instance likely needs to be
        //// executed again, but up to the scheduler on how or when to handle
        //// this case.
        #[prost(message, tag = "4")]
        GoingAway(super::GoingAway),
    }
}
//// Represents the initial request sent to the scheduler informing the
//// scheduler about this worker's capabilities.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterSupportedProperties {
    //// The list of properties this worker can support. The exact
    //// implementation is driven by the configuration matrix between the
    //// worker and scheduler.
    ////
    //// The scheduler may reject this worker if any property keys that
    //// the scheduler is not configured to support, or may simply ignore
    //// the unsupported properties.
    ////
    //// The details on how to use this property can be found here:
    //// <https://github.com/allada/rust_cas/blob/c91f61edf182f2b64451fd48a5e63fa506a43aae/config/cas_server.rs>
    #[prost(message, repeated, tag = "1")]
    pub properties: ::prost::alloc::vec::Vec<
        super::super::super::super::super::build::bazel::remote::execution::v2::platform::Property,
    >,
}
//// Represents the result of an execution.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecuteResult {
    //// Result of the execution. See `build.bazel.remote.execution.v2.ExecuteResponse`
    //// for details.
    #[prost(message, optional, tag = "1")]
    pub execute_response: ::core::option::Option<
        super::super::super::super::super::build::bazel::remote::execution::v2::ExecuteResponse,
    >,
}
//// Informs the scheduler that the node is going offline.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GoingAway {}
//// Communication from the scheduler to the worker.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateForWorker {
    #[prost(oneof = "update_for_worker::Update", tags = "1, 2")]
    pub update: ::core::option::Option<update_for_worker::Update>,
}
/// Nested message and enum types in `UpdateForWorker`.
pub mod update_for_worker {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Update {
        //// Message used to let the worker know that it is still alive as well
        //// as check to see if the worker is still alive. The worker
        //// may close the connection if the scheduler has not sent any messages
        //// after some amount of time (configured in the scheduler's
        //// configuration).
        #[prost(message, tag = "1")]
        KeepAlive(()),
        //// Informs the worker about some work it should begin performing the
        //// requested action.
        #[prost(message, tag = "2")]
        StartAction(super::StartExecute),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StartExecute {
    #[prost(message, optional, tag = "1")]
    pub execute_request: ::core::option::Option<
        super::super::super::super::super::build::bazel::remote::execution::v2::ExecuteRequest,
    >,
}
#[doc = r" Generated client implementations."]
pub mod worker_api_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "/ This API describes how schedulers communicate with Worker nodes."]
    #[doc = "/"]
    #[doc = "/ When a worker node comes online it must be pre-configured with the"]
    #[doc = "/ endpoint of the scheduler it will register with. Once the worker"]
    #[doc = "/ connects to the scheduler it must send a `RegisterSupportedProperties`"]
    #[doc = "/ command to the scheduler. The scheduler will then use this information"]
    #[doc = "/ to determine which jobs the worker can process."]
    #[derive(Debug, Clone)]
    pub struct WorkerApiClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl WorkerApiClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> WorkerApiClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> WorkerApiClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            WorkerApiClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        #[doc = "/ The worker will run this connect command which will initiate the"]
        #[doc = "/ bi-directional stream. The worker must first run"]
        #[doc = "/ `RegisterSupportedProperties` before issuing any other commands."]
        pub async fn connect_worker(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::UpdateFromWorker>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::UpdateForWorker>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/com.github.allada.rust_cas.remote_execution.WorkerApi/ConnectWorker",
            );
            self.inner
                .streaming(request.into_streaming_request(), path, codec)
                .await
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod worker_api_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with WorkerApiServer."]
    #[async_trait]
    pub trait WorkerApi: Send + Sync + 'static {
        #[doc = "Server streaming response type for the ConnectWorker method."]
        type ConnectWorkerStream: futures_core::Stream<Item = Result<super::UpdateForWorker, tonic::Status>>
            + Send
            + 'static;
        #[doc = "/ The worker will run this connect command which will initiate the"]
        #[doc = "/ bi-directional stream. The worker must first run"]
        #[doc = "/ `RegisterSupportedProperties` before issuing any other commands."]
        async fn connect_worker(
            &self,
            request: tonic::Request<tonic::Streaming<super::UpdateFromWorker>>,
        ) -> Result<tonic::Response<Self::ConnectWorkerStream>, tonic::Status>;
    }
    #[doc = "/ This API describes how schedulers communicate with Worker nodes."]
    #[doc = "/"]
    #[doc = "/ When a worker node comes online it must be pre-configured with the"]
    #[doc = "/ endpoint of the scheduler it will register with. Once the worker"]
    #[doc = "/ connects to the scheduler it must send a `RegisterSupportedProperties`"]
    #[doc = "/ command to the scheduler. The scheduler will then use this information"]
    #[doc = "/ to determine which jobs the worker can process."]
    #[derive(Debug)]
    pub struct WorkerApiServer<T: WorkerApi> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: WorkerApi> WorkerApiServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for WorkerApiServer<T>
    where
        T: WorkerApi,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/com.github.allada.rust_cas.remote_execution.WorkerApi/ConnectWorker" => {
                    #[allow(non_camel_case_types)]
                    struct ConnectWorkerSvc<T: WorkerApi>(pub Arc<T>);
                    impl<T: WorkerApi> tonic::server::StreamingService<super::UpdateFromWorker>
                        for ConnectWorkerSvc<T>
                    {
                        type Response = super::UpdateForWorker;
                        type ResponseStream = T::ConnectWorkerStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::UpdateFromWorker>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).connect_worker(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ConnectWorkerSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: WorkerApi> Clone for WorkerApiServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: WorkerApi> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: WorkerApi> tonic::transport::NamedService for WorkerApiServer<T> {
        const NAME: &'static str = "com.github.allada.rust_cas.remote_execution.WorkerApi";
    }
}
