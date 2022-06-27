// Copyright 2020 Nathan (Blaise) Bruer.  All rights reserved.
/// This resource represents a long-running operation that is the result of a
/// network API call.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Operation {
    /// The server-assigned name, which is only unique within the same service that
    /// originally returns it. If you use the default HTTP mapping, the
    /// `name` should be a resource name ending with `operations/{unique_id}`.
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// Service-specific metadata associated with the operation.  It typically
    /// contains progress information and common metadata such as create time.
    /// Some services might not provide such metadata.  Any method that returns a
    /// long-running operation should document the metadata type, if any.
    #[prost(message, optional, tag="2")]
    pub metadata: ::core::option::Option<::prost_types::Any>,
    /// If the value is `false`, it means the operation is still in progress.
    /// If `true`, the operation is completed, and either `error` or `response` is
    /// available.
    #[prost(bool, tag="3")]
    pub done: bool,
    /// The operation result, which can be either an `error` or a valid `response`.
    /// If `done` == `false`, neither `error` nor `response` is set.
    /// If `done` == `true`, exactly one of `error` or `response` is set.
    #[prost(oneof="operation::Result", tags="4, 5")]
    pub result: ::core::option::Option<operation::Result>,
}
/// Nested message and enum types in `Operation`.
pub mod operation {
    /// The operation result, which can be either an `error` or a valid `response`.
    /// If `done` == `false`, neither `error` nor `response` is set.
    /// If `done` == `true`, exactly one of `error` or `response` is set.
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        /// The error result of the operation in case of failure or cancellation.
        #[prost(message, tag="4")]
        Error(super::super::rpc::Status),
        /// The normal response of the operation in case of success.  If the original
        /// method returns no data on success, such as `Delete`, the response is
        /// `google.protobuf.Empty`.  If the original method is standard
        /// `Get`/`Create`/`Update`, the response should be the resource.  For other
        /// methods, the response should have the type `XxxResponse`, where `Xxx`
        /// is the original method name.  For example, if the original method name
        /// is `TakeSnapshot()`, the inferred response type is
        /// `TakeSnapshotResponse`.
        #[prost(message, tag="5")]
        Response(::prost_types::Any),
    }
}
/// The request message for \[Operations.GetOperation][google.longrunning.Operations.GetOperation\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetOperationRequest {
    /// The name of the operation resource.
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
}
/// The request message for \[Operations.ListOperations][google.longrunning.Operations.ListOperations\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListOperationsRequest {
    /// The name of the operation's parent resource.
    #[prost(string, tag="4")]
    pub name: ::prost::alloc::string::String,
    /// The standard list filter.
    #[prost(string, tag="1")]
    pub filter: ::prost::alloc::string::String,
    /// The standard list page size.
    #[prost(int32, tag="2")]
    pub page_size: i32,
    /// The standard list page token.
    #[prost(string, tag="3")]
    pub page_token: ::prost::alloc::string::String,
}
/// The response message for \[Operations.ListOperations][google.longrunning.Operations.ListOperations\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListOperationsResponse {
    /// A list of operations that matches the specified filter in the request.
    #[prost(message, repeated, tag="1")]
    pub operations: ::prost::alloc::vec::Vec<Operation>,
    /// The standard List next-page token.
    #[prost(string, tag="2")]
    pub next_page_token: ::prost::alloc::string::String,
}
/// The request message for \[Operations.CancelOperation][google.longrunning.Operations.CancelOperation\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CancelOperationRequest {
    /// The name of the operation resource to be cancelled.
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
}
/// The request message for \[Operations.DeleteOperation][google.longrunning.Operations.DeleteOperation\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteOperationRequest {
    /// The name of the operation resource to be deleted.
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
}
/// The request message for \[Operations.WaitOperation][google.longrunning.Operations.WaitOperation\].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WaitOperationRequest {
    /// The name of the operation resource to wait on.
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    /// The maximum duration to wait before timing out. If left blank, the wait
    /// will be at most the time permitted by the underlying HTTP/RPC protocol.
    /// If RPC context deadline is also specified, the shorter one will be used.
    #[prost(message, optional, tag="2")]
    pub timeout: ::core::option::Option<::prost_types::Duration>,
}
/// A message representing the message types used by a long-running operation.
///
/// Example:
///
///   rpc LongRunningRecognize(LongRunningRecognizeRequest)
///       returns (google.longrunning.Operation) {
///     option (google.longrunning.operation_info) = {
///       response_type: "LongRunningRecognizeResponse"
///       metadata_type: "LongRunningRecognizeMetadata"
///     };
///   }
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OperationInfo {
    /// Required. The message name of the primary return type for this
    /// long-running operation.
    /// This type will be used to deserialize the LRO's response.
    ///
    /// If the response is in a different package from the rpc, a fully-qualified
    /// message name must be used (e.g. `google.protobuf.Struct`).
    ///
    /// Note: Altering this value constitutes a breaking change.
    #[prost(string, tag="1")]
    pub response_type: ::prost::alloc::string::String,
    /// Required. The message name of the metadata type for this long-running
    /// operation.
    ///
    /// If the response is in a different package from the rpc, a fully-qualified
    /// message name must be used (e.g. `google.protobuf.Struct`).
    ///
    /// Note: Altering this value constitutes a breaking change.
    #[prost(string, tag="2")]
    pub metadata_type: ::prost::alloc::string::String,
}
/// Generated client implementations.
pub mod operations_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Manages long-running operations with an API service.
    ///
    /// When an API method normally takes long time to complete, it can be designed
    /// to return [Operation][google.longrunning.Operation] to the client, and the client can use this
    /// interface to receive the real response asynchronously by polling the
    /// operation resource, or pass the operation resource to another API (such as
    /// Google Cloud Pub/Sub API) to receive the response.  Any API service that
    /// returns long-running operations should implement the `Operations` interface
    /// so developers can have a consistent client experience.
    #[derive(Debug, Clone)]
    pub struct OperationsClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl OperationsClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> OperationsClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> OperationsClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            OperationsClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        /// Lists operations that match the specified filter in the request. If the
        /// server doesn't support this method, it returns `UNIMPLEMENTED`.
        ///
        /// NOTE: the `name` binding allows API services to override the binding
        /// to use different resource name schemes, such as `users/*/operations`. To
        /// override the binding, API services can add a binding such as
        /// `"/v1/{name=users/*}/operations"` to their service configuration.
        /// For backwards compatibility, the default name includes the operations
        /// collection id, however overriding users must ensure the name binding
        /// is the parent resource, without the operations collection id.
        pub async fn list_operations(
            &mut self,
            request: impl tonic::IntoRequest<super::ListOperationsRequest>,
        ) -> Result<tonic::Response<super::ListOperationsResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/google.longrunning.Operations/ListOperations",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Gets the latest state of a long-running operation.  Clients can use this
        /// method to poll the operation result at intervals as recommended by the API
        /// service.
        pub async fn get_operation(
            &mut self,
            request: impl tonic::IntoRequest<super::GetOperationRequest>,
        ) -> Result<tonic::Response<super::Operation>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/google.longrunning.Operations/GetOperation",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Deletes a long-running operation. This method indicates that the client is
        /// no longer interested in the operation result. It does not cancel the
        /// operation. If the server doesn't support this method, it returns
        /// `google.rpc.Code.UNIMPLEMENTED`.
        pub async fn delete_operation(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteOperationRequest>,
        ) -> Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/google.longrunning.Operations/DeleteOperation",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Starts asynchronous cancellation on a long-running operation.  The server
        /// makes a best effort to cancel the operation, but success is not
        /// guaranteed.  If the server doesn't support this method, it returns
        /// `google.rpc.Code.UNIMPLEMENTED`.  Clients can use
        /// [Operations.GetOperation][google.longrunning.Operations.GetOperation] or
        /// other methods to check whether the cancellation succeeded or whether the
        /// operation completed despite cancellation. On successful cancellation,
        /// the operation is not deleted; instead, it becomes an operation with
        /// an [Operation.error][google.longrunning.Operation.error] value with a [google.rpc.Status.code][google.rpc.Status.code] of 1,
        /// corresponding to `Code.CANCELLED`.
        pub async fn cancel_operation(
            &mut self,
            request: impl tonic::IntoRequest<super::CancelOperationRequest>,
        ) -> Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/google.longrunning.Operations/CancelOperation",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        /// Waits for the specified long-running operation until it is done or reaches
        /// at most a specified timeout, returning the latest state.  If the operation
        /// is already done, the latest state is immediately returned.  If the timeout
        /// specified is greater than the default HTTP/RPC timeout, the HTTP/RPC
        /// timeout is used.  If the server does not support this method, it returns
        /// `google.rpc.Code.UNIMPLEMENTED`.
        /// Note that this method is on a best-effort basis.  It may return the latest
        /// state before the specified timeout (including immediately), meaning even an
        /// immediate response is no guarantee that the operation is done.
        pub async fn wait_operation(
            &mut self,
            request: impl tonic::IntoRequest<super::WaitOperationRequest>,
        ) -> Result<tonic::Response<super::Operation>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/google.longrunning.Operations/WaitOperation",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod operations_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with OperationsServer.
    #[async_trait]
    pub trait Operations: Send + Sync + 'static {
        /// Lists operations that match the specified filter in the request. If the
        /// server doesn't support this method, it returns `UNIMPLEMENTED`.
        ///
        /// NOTE: the `name` binding allows API services to override the binding
        /// to use different resource name schemes, such as `users/*/operations`. To
        /// override the binding, API services can add a binding such as
        /// `"/v1/{name=users/*}/operations"` to their service configuration.
        /// For backwards compatibility, the default name includes the operations
        /// collection id, however overriding users must ensure the name binding
        /// is the parent resource, without the operations collection id.
        async fn list_operations(
            &self,
            request: tonic::Request<super::ListOperationsRequest>,
        ) -> Result<tonic::Response<super::ListOperationsResponse>, tonic::Status>;
        /// Gets the latest state of a long-running operation.  Clients can use this
        /// method to poll the operation result at intervals as recommended by the API
        /// service.
        async fn get_operation(
            &self,
            request: tonic::Request<super::GetOperationRequest>,
        ) -> Result<tonic::Response<super::Operation>, tonic::Status>;
        /// Deletes a long-running operation. This method indicates that the client is
        /// no longer interested in the operation result. It does not cancel the
        /// operation. If the server doesn't support this method, it returns
        /// `google.rpc.Code.UNIMPLEMENTED`.
        async fn delete_operation(
            &self,
            request: tonic::Request<super::DeleteOperationRequest>,
        ) -> Result<tonic::Response<()>, tonic::Status>;
        /// Starts asynchronous cancellation on a long-running operation.  The server
        /// makes a best effort to cancel the operation, but success is not
        /// guaranteed.  If the server doesn't support this method, it returns
        /// `google.rpc.Code.UNIMPLEMENTED`.  Clients can use
        /// [Operations.GetOperation][google.longrunning.Operations.GetOperation] or
        /// other methods to check whether the cancellation succeeded or whether the
        /// operation completed despite cancellation. On successful cancellation,
        /// the operation is not deleted; instead, it becomes an operation with
        /// an [Operation.error][google.longrunning.Operation.error] value with a [google.rpc.Status.code][google.rpc.Status.code] of 1,
        /// corresponding to `Code.CANCELLED`.
        async fn cancel_operation(
            &self,
            request: tonic::Request<super::CancelOperationRequest>,
        ) -> Result<tonic::Response<()>, tonic::Status>;
        /// Waits for the specified long-running operation until it is done or reaches
        /// at most a specified timeout, returning the latest state.  If the operation
        /// is already done, the latest state is immediately returned.  If the timeout
        /// specified is greater than the default HTTP/RPC timeout, the HTTP/RPC
        /// timeout is used.  If the server does not support this method, it returns
        /// `google.rpc.Code.UNIMPLEMENTED`.
        /// Note that this method is on a best-effort basis.  It may return the latest
        /// state before the specified timeout (including immediately), meaning even an
        /// immediate response is no guarantee that the operation is done.
        async fn wait_operation(
            &self,
            request: tonic::Request<super::WaitOperationRequest>,
        ) -> Result<tonic::Response<super::Operation>, tonic::Status>;
    }
    /// Manages long-running operations with an API service.
    ///
    /// When an API method normally takes long time to complete, it can be designed
    /// to return [Operation][google.longrunning.Operation] to the client, and the client can use this
    /// interface to receive the real response asynchronously by polling the
    /// operation resource, or pass the operation resource to another API (such as
    /// Google Cloud Pub/Sub API) to receive the response.  Any API service that
    /// returns long-running operations should implement the `Operations` interface
    /// so developers can have a consistent client experience.
    #[derive(Debug)]
    pub struct OperationsServer<T: Operations> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Operations> OperationsServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.accept_compression_encodings.enable_gzip();
            self
        }
        /// Compress responses with `gzip`, if the client supports it.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.send_compression_encodings.enable_gzip();
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for OperationsServer<T>
    where
        T: Operations,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/google.longrunning.Operations/ListOperations" => {
                    #[allow(non_camel_case_types)]
                    struct ListOperationsSvc<T: Operations>(pub Arc<T>);
                    impl<
                        T: Operations,
                    > tonic::server::UnaryService<super::ListOperationsRequest>
                    for ListOperationsSvc<T> {
                        type Response = super::ListOperationsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListOperationsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).list_operations(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListOperationsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/google.longrunning.Operations/GetOperation" => {
                    #[allow(non_camel_case_types)]
                    struct GetOperationSvc<T: Operations>(pub Arc<T>);
                    impl<
                        T: Operations,
                    > tonic::server::UnaryService<super::GetOperationRequest>
                    for GetOperationSvc<T> {
                        type Response = super::Operation;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetOperationRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).get_operation(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetOperationSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/google.longrunning.Operations/DeleteOperation" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteOperationSvc<T: Operations>(pub Arc<T>);
                    impl<
                        T: Operations,
                    > tonic::server::UnaryService<super::DeleteOperationRequest>
                    for DeleteOperationSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteOperationRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).delete_operation(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteOperationSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/google.longrunning.Operations/CancelOperation" => {
                    #[allow(non_camel_case_types)]
                    struct CancelOperationSvc<T: Operations>(pub Arc<T>);
                    impl<
                        T: Operations,
                    > tonic::server::UnaryService<super::CancelOperationRequest>
                    for CancelOperationSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CancelOperationRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).cancel_operation(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CancelOperationSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/google.longrunning.Operations/WaitOperation" => {
                    #[allow(non_camel_case_types)]
                    struct WaitOperationSvc<T: Operations>(pub Arc<T>);
                    impl<
                        T: Operations,
                    > tonic::server::UnaryService<super::WaitOperationRequest>
                    for WaitOperationSvc<T> {
                        type Response = super::Operation;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::WaitOperationRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).wait_operation(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = WaitOperationSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: Operations> Clone for OperationsServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Operations> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Operations> tonic::transport::NamedService for OperationsServer<T> {
        const NAME: &'static str = "google.longrunning.Operations";
    }
}
