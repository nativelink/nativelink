// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::{future::BoxFuture, select, stream::FuturesUnordered, FutureExt, StreamExt, TryFutureExt};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{transport::Channel as TonicChannel, Streaming};

use action_messages::{ActionResult, ActionStage};
use common::log;
use config::cas_server::LocalWorkerConfig;
use error::{make_err, make_input_err, Code, Error, ResultExt};
use fast_slow_store::FastSlowStore;
use proto::com::github::allada::turbo_cache::remote_execution::{
    execute_result, update_for_worker::Update, worker_api_client::WorkerApiClient, ExecuteFinishedResult,
    ExecuteResult, KeepAliveRequest, UpdateForWorker,
};
use running_actions_manager::{RunningAction, RunningActionsManager, RunningActionsManagerImpl};
use store::Store;
use worker_api_client_wrapper::{WorkerApiClientTrait, WorkerApiClientWrapper};
use worker_utils::make_supported_properties;

/// If we loose connection to the worker api server we will wait this many seconds
/// before trying to connect.
const CONNECTION_RETRY_DELAY_S: f32 = 0.5;

/// Default endpoint timeout. If this value gets modified the documentation in cas_server.rs
/// must also be updated.
const DEFAULT_ENDPOINT_TIMEOUT_S: f32 = 5.;

struct LocalWorkerImpl<'a, T: WorkerApiClientTrait, U: RunningActionsManager> {
    config: &'a LocalWorkerConfig,
    // According to the tonic documentation it is a cheap operation to clone this.
    grpc_client: T,
    worker_id: String,
    running_actions_manager: Arc<U>,
}

impl<'a, T: WorkerApiClientTrait, U: RunningActionsManager> LocalWorkerImpl<'a, T, U> {
    fn new(config: &'a LocalWorkerConfig, grpc_client: T, worker_id: String, running_actions_manager: Arc<U>) -> Self {
        Self {
            config,
            grpc_client,
            worker_id,
            running_actions_manager,
        }
    }

    /// Starts a background spawn/thread that will send a message to the server every `timeout / 2`.
    async fn start_keep_alive(&self) -> Result<(), Error> {
        // According to tonic's documentation this call should be cheap and is the same stream.
        let mut grpc_client = self.grpc_client.clone();

        loop {
            let timeout = self
                .config
                .worker_api_endpoint
                .timeout
                .unwrap_or(DEFAULT_ENDPOINT_TIMEOUT_S);
            // We always send 2 keep alive requests per timeout. Http2 should manage most of our
            // timeout issues, this is a secondary check to ensure we can still send data.
            sleep(Duration::from_secs_f32(timeout / 2.)).await;
            if let Err(e) = grpc_client
                .keep_alive(KeepAliveRequest {
                    worker_id: self.worker_id.clone(),
                })
                .await
            {
                return Err(make_err!(
                    Code::Internal,
                    "Failed to send KeepAlive in LocalWorker : {:?}",
                    e
                ));
            }
        }
    }

    async fn run(&mut self, update_for_worker_stream: Streaming<UpdateForWorker>) -> Result<(), Error> {
        // This big block of logic is designed to help simplify upstream components. Upstream
        // components can write standard futures that return a `Result<(), Error>` and this block
        // will forward the error up to the client and disconnect from the scheduler.
        // It is a common use case that an item sent through update_for_worker_stream will always
        // have a response but the response will be triggered through a callback to the scheduler.
        // This can be quite tricky to manage, so what we have done here is given access to a
        // `futures` variable which because this is in a single thread as well as a channel that you
        // send a future into that makes it into the `futures` variable.
        // This means that if you want to perform an action based on the result of the future
        // you use the `.map()` method and the new action will always come to live in this spawn,
        // giving mutable access to stuff in this struct.
        // NOTE: If you ever return from this function it will disconnect from the scheduler.
        let mut futures = FuturesUnordered::new();
        futures.push(self.start_keep_alive().boxed());

        let (add_future_channel, add_future_rx) = mpsc::unbounded_channel();
        let mut add_future_rx = UnboundedReceiverStream::new(add_future_rx).fuse();

        let mut update_for_worker_stream = update_for_worker_stream.fuse();

        loop {
            select! {
                maybe_update = update_for_worker_stream.next() => {
                    match maybe_update
                        .err_tip(|| "UpdateForWorker stream closed early")?
                        .err_tip(|| "Got error in UpdateForWorker stream")?
                        .update
                        .err_tip(|| "Expected update to exist in UpdateForWorker")?
                    {
                        Update::ConnectionResult(_) => {
                            return Err(make_input_err!(
                                "Got ConnectionResult in LocalWorker::run which should never happen"
                            ));
                        }
                        Update::KeepAlive(()) => { /* Do nothing, we don't need to do anything to keep-alives. */ }
                        // TODO(allada) We should possibly do something with this notification.
                        Update::Disconnect(()) => { /* Do nothing */ }
                        Update::StartAction(start_execute) => {
                            let add_future_channel = add_future_channel.clone();
                            let mut grpc_client = self.grpc_client.clone();
                            let salt = start_execute.salt.clone();
                            let worker_id = self.worker_id.clone();
                            let action_digest = start_execute.execute_request.as_ref().map_or(None, |v| v.action_digest.clone());
                            let start_action_fut = self
                                .running_actions_manager
                                .clone()
                                .create_and_add_action(worker_id.clone(), start_execute)
                                .and_then(|action|
                                    action
                                        .clone()
                                        .prepare_action()
                                        .and_then(|action| action.execute())
                                        .and_then(|action| action.upload_results())
                                        .and_then(|action| action.get_finished_result())
                                        // Note: We need ensure we run cleanup even if one of the other steps fail.
                                        .then(|result| async move {
                                            if let Err(e) = action.cleanup().await {
                                                return Result::<ActionResult, Error>::Err(e).merge(result);
                                            }
                                            result
                                        })
                                );

                            let make_publish_future = move |res: Result<ActionResult, Error>| async move {
                                match res {
                                    Ok(action_result) => {
                                        grpc_client.execution_response(
                                            ExecuteResult{
                                                response: Some(execute_result::Response::Result(ExecuteFinishedResult{
                                                    worker_id,
                                                    action_digest,
                                                    salt,
                                                    execute_response: Some(ActionStage::Completed(action_result).into()),
                                                })),
                                            }
                                        )
                                        .await
                                        .err_tip(|| "Error while calling execution_response")?;
                                    },
                                    Err(e) => {
                                        grpc_client.execution_response(ExecuteResult{
                                            response: Some(execute_result::Response::InternalError(e.into())),
                                        }).await.err_tip(|| "Error calling execution_response with error")?;
                                    },
                                }
                                Ok(())
                            };

                            let mapped_fut = tokio::spawn(start_action_fut)
                                .map(move |res| {
                                    let res = res.err_tip(|| "Failed to launch spawn")?;
                                    add_future_channel
                                        .send(make_publish_future(res).boxed())
                                        .map_err(|_| make_err!(Code::Internal, "LocalWorker could not send future"))?;
                                    Ok(())
                                })
                                .boxed();
                            futures.push(mapped_fut);
                        }
                    };
                },
                res = add_future_rx.next() => {
                    let fut = res.err_tip(|| "New future stream receives should never be closed")?;
                    futures.push(fut);
                },
                res = futures.next() => res.err_tip(|| "Keep-alive should always pending. This is an internal error")??,
            };
        }
        // Unreachable.
    }
}

type ConnectionFactory<T> = Box<dyn Fn() -> BoxFuture<'static, Result<T, Error>> + Send + Sync>;

pub struct LocalWorker<T: WorkerApiClientTrait, U: RunningActionsManager> {
    config: Arc<LocalWorkerConfig>,
    running_actions_manager: Arc<U>,
    connection_factory: ConnectionFactory<T>,
    sleep_fn: Option<Box<dyn Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync>>,
}

/// Creates a new LocalWorker. The `cas_store` must be an instance of FastSlowStore and will be
/// checked at runtime.
pub fn new_local_worker(
    config: Arc<LocalWorkerConfig>,
    cas_store: Arc<dyn Store>,
) -> Result<LocalWorker<WorkerApiClientWrapper, RunningActionsManagerImpl>, Error> {
    let fast_slow_store = cas_store
        .as_any()
        .downcast_ref::<Arc<FastSlowStore>>()
        .err_tip(|| "Expected store for LocalWorker's store to be a FastSlowStore")?
        .clone();
    let running_actions_manager = Arc::new(RunningActionsManagerImpl::new(
        config.work_directory.clone(),
        fast_slow_store,
    )?)
    .clone();
    Ok(LocalWorker::new_with_connection_factory_and_actions_manager(
        config.clone(),
        running_actions_manager,
        Box::new(move || {
            let config = config.clone();
            Box::pin(async move {
                let timeout = config.worker_api_endpoint.timeout.unwrap_or(DEFAULT_ENDPOINT_TIMEOUT_S);
                let timeout_duration = Duration::from_secs_f32(timeout);
                let uri = (&config.worker_api_endpoint.uri)
                    .try_into()
                    .map_err(|e| make_input_err!("Invalid URI for worker endpoint : {:?}", e))?;
                let endpoint = TonicChannel::builder(uri)
                    .connect_timeout(timeout_duration)
                    .timeout(timeout_duration);
                let transport = endpoint
                    .connect()
                    .await
                    .map_err(|e| make_err!(Code::Internal, "Could not connect to endpoint : {:?}", e))?;
                Ok(WorkerApiClient::new(transport).into())
            })
        }),
        Box::new(move |d| Box::pin(sleep(d))),
    ))
}

impl<T: WorkerApiClientTrait, U: RunningActionsManager> LocalWorker<T, U> {
    pub fn new_with_connection_factory_and_actions_manager(
        config: Arc<LocalWorkerConfig>,
        running_actions_manager: Arc<U>,
        connection_factory: ConnectionFactory<T>,
        sleep_fn: Box<dyn Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync>,
    ) -> Self {
        Self {
            config,
            running_actions_manager,
            connection_factory,
            sleep_fn: Some(sleep_fn),
        }
    }

    async fn register_worker(&mut self, client: &mut T) -> Result<(String, Streaming<UpdateForWorker>), Error> {
        let supported_properties = make_supported_properties(&self.config.platform_properties).await?;
        let mut update_for_worker_stream = client
            .connect_worker(supported_properties)
            .await
            .err_tip(|| "Could not call connect_worker() in worker")?
            .into_inner();

        let first_msg_update = update_for_worker_stream
            .next()
            .await
            .err_tip(|| "Got EOF expected UpdateForWorker")?
            .err_tip(|| "Got error when receiving UpdateForWorker")?
            .update;

        let worker_id = match first_msg_update {
            Some(Update::ConnectionResult(connection_result)) => connection_result.worker_id,
            other => {
                return Err(make_input_err!(
                    "Expected first response from scheduler to be a ConnectResult got : {:?}",
                    other
                ))
            }
        };
        Ok((worker_id, update_for_worker_stream))
    }

    pub async fn run(mut self) -> Result<(), Error> {
        let sleep_fn = self
            .sleep_fn
            .take()
            .err_tip(|| "Could not unwrap sleep_fn in LocalWorker::run")?;
        let sleep_fn_pin = Pin::new(&sleep_fn);
        let error_handler = Box::pin(move |e: Error| async move {
            log::error!("{:?}", e);
            (&sleep_fn_pin)(Duration::from_secs_f32(CONNECTION_RETRY_DELAY_S)).await;
        });

        loop {
            // First connect to our endpoint.
            let mut client = match (self.connection_factory)().await {
                Ok(client) => client,
                Err(e) => {
                    (error_handler)(e).await;
                    continue; // Try to connect again.
                }
            };

            // Next register our worker with the scheduler.
            let (mut inner, update_for_worker_stream) = match self.register_worker(&mut client).await {
                Err(e) => {
                    (error_handler)(e).await;
                    continue; // Try to connect again.
                }
                Ok((worker_id, update_for_worker_stream)) => (
                    LocalWorkerImpl::new(&self.config, client, worker_id, self.running_actions_manager.clone()),
                    update_for_worker_stream,
                ),
            };

            // Now listen for connections and run all other services.
            if let Err(e) = inner.run(update_for_worker_stream).await {
                (error_handler)(e).await;
                continue; // Try to connect again.
            }
        }
        // Unreachable.
    }
}
