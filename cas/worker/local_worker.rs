// Copyright 2022 The Native Link Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::pin::Pin;
use std::process::Stdio;
use std::str;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use futures::{future::BoxFuture, select, stream::FuturesUnordered, Future, FutureExt, StreamExt, TryFutureExt};
use tokio::process;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{transport::Channel as TonicChannel, Streaming};

use action_messages::{ActionResult, ActionStage};
use common::{fs, log};
use config::cas_server::LocalWorkerConfig;
use error::{make_err, make_input_err, Code, Error, ResultExt};
use fast_slow_store::FastSlowStore;
use metrics_utils::{AsyncCounterWrapper, Collector, CollectorState, CounterWithTime, MetricsComponent, Registry};
use proto::com::github::trace_machina::native_link::remote_execution::{
    execute_result, update_for_worker::Update, worker_api_client::WorkerApiClient, ExecuteResult, KeepAliveRequest,
    UpdateForWorker,
};
use running_actions_manager::{
    ExecutionConfiguration, Metrics as RunningActionManagerMetrics, RunningAction, RunningActionsManager,
    RunningActionsManagerArgs, RunningActionsManagerImpl,
};
use store::Store;
use worker_api_client_wrapper::{WorkerApiClientTrait, WorkerApiClientWrapper};
use worker_utils::make_supported_properties;

/// Amount of time to wait if we have actions in transit before we try to
/// consider an error to have occurred.
const ACTIONS_IN_TRANSIT_TIMEOUT_S: f32 = 10.;

/// If we loose connection to the worker api server we will wait this many seconds
/// before trying to connect.
const CONNECTION_RETRY_DELAY_S: f32 = 0.5;

/// Default endpoint timeout. If this value gets modified the documentation in
/// `cas_server.rs` must also be updated.
const DEFAULT_ENDPOINT_TIMEOUT_S: f32 = 5.;

/// Default maximum amount of time a task is allowed to run for.
/// If this value gets modified the documentation in `cas_server.rs` must also be updated.
const DEFAULT_MAX_ACTION_TIMEOUT: Duration = Duration::from_secs(1200); // 20 mins.

struct LocalWorkerImpl<'a, T: WorkerApiClientTrait, U: RunningActionsManager> {
    config: &'a LocalWorkerConfig,
    // According to the tonic documentation it is a cheap operation to clone this.
    grpc_client: T,
    worker_id: String,
    running_actions_manager: Arc<U>,
    // Number of actions that have been received in `Update::StartAction`, but
    // not yet processed by running_actions_manager's spawn. This number should
    // always be zero if there are no actions running and no actions being waited
    // on by the scheduler.
    actions_in_transit: Arc<AtomicU64>,
    metrics: Arc<Metrics>,
}

async fn preconditions_met(precondition_script: Option<String>) -> Result<(), Error> {
    let Some(precondition_script) = &precondition_script else {
        // No script means we are always ok to proceed.
        return Ok(());
    };
    // TODO: Might want to pass some information about the command to the
    //       script, but at this point it's not even been downloaded yet,
    //       so that's not currently possible.  Perhaps we'll move this in
    //       future to pass useful information through?  Or perhaps we'll
    //       have a pre-condition and a pre-execute script instead, although
    //       arguably entrypoint_cmd already gives us that.
    let precondition_process = process::Command::new(precondition_script)
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .env_clear()
        .spawn()
        .err_tip(|| format!("Could not execute precondition command {:?}", precondition_script))?;
    let output = precondition_process.wait_with_output().await?;
    if output.status.code() == Some(0) {
        Ok(())
    } else {
        Err(make_err!(
            Code::ResourceExhausted,
            "Preconditions script returned status {} - {}",
            output.status,
            str::from_utf8(&output.stdout).unwrap_or("")
        ))
    }
}

impl<'a, T: WorkerApiClientTrait, U: RunningActionsManager> LocalWorkerImpl<'a, T, U> {
    fn new(
        config: &'a LocalWorkerConfig,
        grpc_client: T,
        worker_id: String,
        running_actions_manager: Arc<U>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            config,
            grpc_client,
            worker_id,
            running_actions_manager,
            // Number of actions that have been received in `Update::StartAction`, but
            // not yet processed by running_actions_manager's spawn. This number should
            // always be zero if there are no actions running and no actions being waited
            // on by the scheduler.
            actions_in_transit: Arc::new(AtomicU64::new(0)),
            metrics,
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
                        // TODO(allada) We should possibly do something with this notification.
                        Update::Disconnect(()) => {
                            self.metrics.disconnects_received.inc();
                        }
                        Update::KeepAlive(()) => {
                            self.metrics.keep_alives_received.inc();
                        }
                        Update::StartAction(start_execute) => {
                            self.metrics.start_actions_received.inc();
                            let add_future_channel = add_future_channel.clone();
                            let mut grpc_client = self.grpc_client.clone();
                            let maybe_instance_name = start_execute.execute_request.as_ref().map(|v| v.instance_name.clone());
                            let salt = start_execute.salt;
                            let worker_id = self.worker_id.clone();
                            let action_digest = start_execute.execute_request.as_ref().and_then(|v| v.action_digest.clone());
                            let running_actions_manager = self.running_actions_manager.clone();
                            let worker_id_clone = worker_id.clone();
                            let precondition_script_cfg = self.config.precondition_script.clone();
                            let actions_in_transit = self.actions_in_transit.clone();
                            let start_action_fut = self.metrics.clone().wrap(move |metrics| async move {
                                metrics.preconditions.wrap(preconditions_met(precondition_script_cfg))
                                .and_then(|_| running_actions_manager.create_and_add_action(worker_id_clone, start_execute))
                                .map(|r| {
                                    // Now that we either failed or registered our action, we can
                                    // consider the action to no longer be in transit.
                                    actions_in_transit.fetch_sub(1, Ordering::Release);
                                    r
                                })
                                .and_then(|action|
                                    action
                                        .clone()
                                        .prepare_action()
                                        .and_then(RunningAction::execute)
                                        .and_then(RunningAction::upload_results)
                                        .and_then(RunningAction::get_finished_result)
                                        // Note: We need ensure we run cleanup even if one of the other steps fail.
                                        .then(|result| async move {
                                            if let Err(e) = action.cleanup().await {
                                                return Result::<ActionResult, Error>::Err(e).merge(result);
                                            }
                                            result
                                        })
                                ).await
                            });

                            let running_actions_manager = self.running_actions_manager.clone();
                            let make_publish_future = move |res: Result<ActionResult, Error>| async move {
                                let instance_name = maybe_instance_name
                                    .err_tip(|| "`instance_name` could not be resolved; this is likely an internal error in local_worker.")?;
                                match res {
                                    Ok(mut action_result) => {
                                        // Save in the action cache before notifying the scheduler that we've completed.
                                        if let Some(digest_info) = action_digest.clone().and_then(|action_digest| action_digest.try_into().ok()) {
                                            if let Err(err) = running_actions_manager.cache_action_result(digest_info, &mut action_result).await {
                                                log::error!("\x1b[0;31mError saving action in store\x1b[0m: {} - {:?}", err, action_digest);
                                            }
                                        }
                                        let action_stage = ActionStage::Completed(action_result);
                                        grpc_client.execution_response(
                                            ExecuteResult{
                                                worker_id,
                                                instance_name,
                                                action_digest,
                                                salt,
                                                result: Some(execute_result::Result::ExecuteResponse(action_stage.into())),
                                            }
                                        )
                                        .await
                                        .err_tip(|| "Error while calling execution_response")?;
                                    },
                                    Err(e) => {
                                        grpc_client.execution_response(ExecuteResult{
                                            worker_id,
                                            instance_name,
                                            action_digest,
                                            salt,
                                            result: Some(execute_result::Result::InternalError(e.into())),
                                        }).await.err_tip(|| "Error calling execution_response with error")?;
                                    },
                                }
                                Ok(())
                            };

                            self.actions_in_transit.fetch_add(1, Ordering::Release);
                            futures.push(
                                tokio::spawn(start_action_fut).map(move |res| {
                                    let res = res.err_tip(|| "Failed to launch spawn")?;
                                    if let Err(err) = &res {
                                        log::info!("\x1b[0;31mError executing action\x1b[0m: {}", err);
                                    }
                                    add_future_channel
                                        .send(make_publish_future(res).boxed())
                                        .map_err(|_| make_err!(Code::Internal, "LocalWorker could not send future"))?;
                                    Ok(())
                                })
                                .boxed()
                            );
                        }
                    };
                },
                res = add_future_rx.next() => {
                    let fut = res.err_tip(|| "New future stream receives should never be closed")?;
                    futures.push(fut);
                },
                res = futures.next() => res.err_tip(|| "Keep-alive should always pending. Likely unable to send data to scheduler")??,
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
    metrics: Arc<Metrics>,
}

/// Creates a new `LocalWorker`. The `cas_store` must be an instance of
/// `FastSlowStore` and will be checked at runtime.
pub async fn new_local_worker(
    config: Arc<LocalWorkerConfig>,
    cas_store: Arc<dyn Store>,
    ac_store: Option<Arc<dyn Store>>,
    historical_store: Arc<dyn Store>,
) -> Result<LocalWorker<WorkerApiClientWrapper, RunningActionsManagerImpl>, Error> {
    let fast_slow_store = cas_store
        .as_any()
        .downcast_ref::<Arc<FastSlowStore>>()
        .err_tip(|| "Expected store for LocalWorker's store to be a FastSlowStore")?
        .clone();

    if let Ok(path) = fs::canonicalize(&config.work_directory).await {
        fs::remove_dir_all(path)
            .await
            .err_tip(|| "Could not remove work_directory in LocalWorker")?;
    }

    fs::create_dir_all(&config.work_directory)
        .await
        .err_tip(|| format!("Could not make work_directory : {}", config.work_directory))?;
    let entrypoint_cmd = if config.entrypoint_cmd.is_empty() {
        None
    } else {
        Some(config.entrypoint_cmd.clone())
    };
    let max_action_timeout = if config.max_action_timeout == 0 {
        DEFAULT_MAX_ACTION_TIMEOUT
    } else {
        Duration::from_secs(config.max_action_timeout as u64)
    };
    let running_actions_manager = Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
        root_work_directory: config.work_directory.clone(),
        execution_configuration: ExecutionConfiguration {
            entrypoint_cmd,
            additional_environment: config.additional_environment.clone(),
        },
        cas_store: fast_slow_store,
        ac_store,
        historical_store,
        upload_action_result_config: &config.upload_action_result,
        max_action_timeout,
        timeout_handled_externally: config.timeout_handled_externally,
    })?);
    Ok(LocalWorker::new_with_connection_factory_and_actions_manager(
        config.clone(),
        running_actions_manager,
        Box::new(move || {
            let config = config.clone();
            Box::pin(async move {
                let timeout = config.worker_api_endpoint.timeout.unwrap_or(DEFAULT_ENDPOINT_TIMEOUT_S);
                let timeout_duration = Duration::from_secs_f32(timeout);

                let uri = config
                    .worker_api_endpoint
                    .uri
                    .clone()
                    .try_into()
                    .map_err(|e| make_input_err!("Invalid URI for worker endpoint : {:?}", e))?;
                let endpoint = TonicChannel::builder(uri)
                    .connect_timeout(timeout_duration)
                    .timeout(timeout_duration);
                let transport = endpoint.connect().await.map_err(|e| {
                    make_err!(
                        Code::Internal,
                        "Could not connect to endpoint {}: {:?}",
                        config.worker_api_endpoint.uri,
                        e
                    )
                })?;
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
        let metrics = Arc::new(Metrics::new(Arc::downgrade(running_actions_manager.metrics())));
        Self {
            config,
            running_actions_manager,
            connection_factory,
            sleep_fn: Some(sleep_fn),
            metrics,
        }
    }

    pub fn name(&self) -> &String {
        &self.config.name
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
            (sleep_fn_pin)(Duration::from_secs_f32(CONNECTION_RETRY_DELAY_S)).await;
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
                    LocalWorkerImpl::new(
                        &self.config,
                        client,
                        worker_id,
                        self.running_actions_manager.clone(),
                        self.metrics.clone(),
                    ),
                    update_for_worker_stream,
                ),
            };
            log::warn!("Worker {} connected to scheduler", inner.worker_id);

            // Now listen for connections and run all other services.
            if let Err(e) = inner.run(update_for_worker_stream).await {
                'no_more_actions: {
                    // Ensure there are no actions in transit before we try to kill
                    // all our actions.
                    const ITERATIONS: usize = 1_000;
                    let sleep_duration = ACTIONS_IN_TRANSIT_TIMEOUT_S / ITERATIONS as f32;
                    for _ in 0..ITERATIONS {
                        if inner.actions_in_transit.load(Ordering::Acquire) == 0 {
                            break 'no_more_actions;
                        }
                        (sleep_fn_pin)(Duration::from_secs_f32(sleep_duration)).await;
                    }
                    let e = make_err!(
                        Code::Internal,
                        "Actions in transit did not reach zero before we disconnected from the scheduler."
                    );
                    log::error!("{e:?}");
                    return Err(e);
                }
                // Kill off any existing actions because if we re-connect, we'll
                // get some more and it might resource lock us.
                self.running_actions_manager.kill_all().await;

                (error_handler)(e).await;
                continue; // Try to connect again.
            }
        }
        // Unreachable.
    }

    pub fn register_metrics(&self, registry: &mut Registry) {
        registry.register_collector(Box::new(Collector::new(&self.metrics)));
    }
}

struct Metrics {
    start_actions_received: CounterWithTime,
    disconnects_received: CounterWithTime,
    keep_alives_received: CounterWithTime,
    preconditions: AsyncCounterWrapper,
    running_actions_manager_metrics: Weak<RunningActionManagerMetrics>,
}

impl Metrics {
    fn new(running_actions_manager_metrics: Weak<RunningActionManagerMetrics>) -> Self {
        Self {
            start_actions_received: CounterWithTime::default(),
            disconnects_received: CounterWithTime::default(),
            keep_alives_received: CounterWithTime::default(),
            preconditions: AsyncCounterWrapper::default(),
            running_actions_manager_metrics,
        }
    }
}

impl Metrics {
    async fn wrap<U, T: Future<Output = U>, F: FnOnce(Arc<Self>) -> T>(self: Arc<Self>, fut: F) -> U {
        fut(self).await
    }
}

impl MetricsComponent for Metrics {
    fn gather_metrics(&self, c: &mut CollectorState) {
        c.publish(
            "start_actions_received",
            &self.start_actions_received,
            concat!(
                "Total number of actions sent to this worker to process. This ",
                "does not mean it started them, it just means it received a request ",
                "to execute it."
            ),
        );
        c.publish(
            "disconnects_received",
            &self.disconnects_received,
            "Total number of disconnects received from the scheduler.",
        );
        c.publish(
            "keep_alives_received",
            &self.keep_alives_received,
            "Total number of keep-alives received from the scheduler.",
        );
        c.publish(
            "preconditions",
            &self.preconditions,
            "Stats about the calls to check if an action satisfies the config supplied script.", // Data is appended to this.
        );
        if let Some(running_actions_manager_metrics) = self.running_actions_manager_metrics.upgrade() {
            c.publish("", running_actions_manager_metrics.as_ref(), "");
        }
    }
}
