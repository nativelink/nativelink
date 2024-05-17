// Copyright 2023 The NativeLink Authors. All rights reserved.
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

use std::collections::HashMap;

use serde::Deserialize;

use crate::schedulers::SchedulerConfig;
use crate::serde_utils::{
    convert_numeric_with_shellexpand, convert_optional_numeric_with_shellexpand,
    convert_optional_string_with_shellexpand, convert_string_with_shellexpand,
    convert_vec_string_with_shellexpand,
};
use crate::stores::{ClientTlsConfig, ConfigDigestHashFunction, StoreConfig, StoreRefName};

/// Name of the scheduler. This type will be used when referencing a
/// scheduler in the `CasConfig::schedulers`'s map key.
pub type SchedulerRefName = String;

/// Used when the config references `instance_name` in the protocol.
pub type InstanceName = String;

#[allow(non_camel_case_types)]
#[derive(Deserialize, Debug, Default, Clone, Copy)]
pub enum CompressionAlgorithm {
    /// No compression.
    #[default]
    none,

    /// Zlib compression.
    gzip,
}

/// Note: Compressing data in the cloud rarely has a benefit, since most
/// cloud providers have very high bandwidth backplanes. However, for
/// clients not inside the data center, it might be a good idea to
/// compress data to and from the cloud. This will however come at a high
/// CPU and performance cost. If you are making remote execution share the
/// same CAS/AC servers as client's remote cache, you can create multiple
/// services with different compression settings that are served on
/// different ports. Then configure the non-cloud clients to use one port
/// and cloud-clients to use another.
#[derive(Deserialize, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct CompressionConfig {
    /// The compression algorithm that the server will use when sending
    /// responses to clients. Enabling this will likely save a lot of
    /// data transfer, but will consume a lot of CPU and add a lot of
    /// latency.
    /// see: <https://github.com/tracemachina/nativelink/issues/109>
    ///
    /// Default: CompressionAlgorithm::None
    pub send_compression_algorithm: Option<CompressionAlgorithm>,

    /// The compression algorithm that the server will accept from clients.
    /// The server will broadcast the supported compression algorithms to
    /// clients and the client will choose which compression algorithm to
    /// use. Enabling this will likely save a lot of data transfer, but
    /// will consume a lot of CPU and add a lot of latency.
    /// see: <https://github.com/tracemachina/nativelink/issues/109>
    ///
    /// Defaults: {no supported compression}
    pub accepted_compression_algorithms: Vec<CompressionAlgorithm>,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct AcStoreConfig {
    /// The store name referenced in the `stores` map in the main config.
    /// This store name referenced here may be reused multiple times.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub ac_store: StoreRefName,

    /// Whether the Action Cache store may be written to, this if set to false
    /// it is only possible to read from the Action Cache.
    #[serde(default)]
    pub read_only: bool,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct CasStoreConfig {
    /// The store name referenced in the `stores` map in the main config.
    /// This store name referenced here may be reused multiple times.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub cas_store: StoreRefName,
}

#[derive(Deserialize, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct CapabilitiesRemoteExecutionConfig {
    /// Scheduler used to configure the capabilities of remote execution.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub scheduler: SchedulerRefName,
}

#[derive(Deserialize, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct CapabilitiesConfig {
    /// Configuration for remote execution capabilities.
    /// If not set the capabilities service will inform the client that remote
    /// execution is not supported.
    pub remote_execution: Option<CapabilitiesRemoteExecutionConfig>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    /// The store name referenced in the `stores` map in the main config.
    /// This store name referenced here may be reused multiple times.
    /// This value must be a CAS store reference.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub cas_store: StoreRefName,

    /// The scheduler name referenced in the `schedulers` map in the main config.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub scheduler: SchedulerRefName,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ByteStreamConfig {
    /// Name of the store in the "stores" configuration.
    pub cas_stores: HashMap<InstanceName, StoreRefName>,

    /// Max number of bytes to send on each grpc stream chunk.
    /// According to <https://github.com/grpc/grpc.github.io/issues/371>
    /// 16KiB - 64KiB is optimal.
    ///
    /// Defaults: 64KiB
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_bytes_per_stream: usize,

    /// In the event a client disconnects while uploading a blob, we will hold
    /// the internal stream open for this many seconds before closing it.
    /// This allows clients that disconnect to reconnect and continue uploading
    /// the same blob.
    ///
    /// Defaults: 10 (seconds)
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub persist_stream_on_disconnect_timeout: usize,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct WorkerApiConfig {
    /// The scheduler name referenced in the `schedulers` map in the main config.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub scheduler: SchedulerRefName,
}

#[derive(Deserialize, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct PrometheusConfig {
    /// Path to register prometheus metrics. If path is "/metrics", and your
    /// domain is "example.com", you can reach the endpoint with:
    /// <http://example.com/metrics>.
    ///
    /// Default: "/metrics"
    #[serde(default)]
    pub path: String,
}

#[derive(Deserialize, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct AdminConfig {
    /// Path to register the admin API. If path is "/admin", and your
    /// domain is "example.com", you can reach the endpoint with:
    /// <http://example.com/admin>.
    ///
    /// Default: "/admin"
    #[serde(default)]
    pub path: String,
}

#[derive(Deserialize, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct HealthConfig {
    /// Path to register the health status check. If path is "/status", and your
    /// domain is "example.com", you can reach the endpoint with:
    /// <http://example.com/status>.
    ///
    /// Default: "/status"
    #[serde(default)]
    pub path: String,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct OperationsConfig {
    /// The scheduler name referenced in the `schedulers` map in the main config.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub scheduler: SchedulerRefName,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ServicesConfig {
    /// The Content Addressable Storage (CAS) backend config.
    /// The key is the instance_name used in the protocol and the
    /// value is the underlying CAS store config.
    pub cas: Option<HashMap<InstanceName, CasStoreConfig>>,

    /// The Action Cache (AC) backend config.
    /// The key is the instance_name used in the protocol and the
    /// value is the underlying AC store config.
    pub ac: Option<HashMap<InstanceName, AcStoreConfig>>,

    /// Capabilities service is required in order to use most of the
    /// bazel protocol. This service is used to provide the supported
    /// features and versions of this bazel GRPC service.
    pub capabilities: Option<HashMap<InstanceName, CapabilitiesConfig>>,

    /// The remote execution service configuration.
    /// NOTE: This service is under development and is currently just a
    /// place holder.
    pub execution: Option<HashMap<InstanceName, ExecutionConfig>>,

    /// This is the service used to stream data to and from the CAS.
    /// Bazel's protocol strongly encourages users to use this streaming
    /// interface to interact with the CAS when the data is large.
    pub bytestream: Option<ByteStreamConfig>,

    /// This is the service used for workers to connect and communicate
    /// through.
    /// NOTE: This service should be served on a different, non-public port.
    /// In other words, `worker_api` configuration should not have any other
    /// services that are served on the same port. Doing so is a security
    /// risk, as workers have a different permission set than a client
    /// that makes the remote execution/cache requests.
    pub worker_api: Option<WorkerApiConfig>,

    /// Experimental - Prometheus metrics configuration. Metrics are gathered
    /// as a singleton but may be served on multiple endpoints.
    pub experimental_prometheus: Option<PrometheusConfig>,

    /// This is the service for any administrative tasks.
    /// It provides a REST API endpoint for administrative purposes.
    pub admin: Option<AdminConfig>,

    /// This is the service for health status check.
    pub health: Option<HealthConfig>,

    /// This is the service for operations interface.
    pub operations: Option<HashMap<InstanceName, OperationsConfig>>,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Path to the certificate file.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub cert_file: String,

    /// Path to the private key file.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub key_file: String,

    /// Path to the certificate authority for mTLS, if client authentication is
    /// required for this endpoint.
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub client_ca_file: Option<String>,

    /// Path to the certificate revocation list for mTLS, if client
    /// authentication is required for this endpoint.
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub client_crl_file: Option<String>,
}

/// Advanced Http configurations. These are generally should not be set.
/// For documentation on what each of these do, see the hyper documentation:
/// See: <https://docs.rs/hyper/latest/hyper/server/conn/struct.Http.html>
///
/// Note: All of these default to hyper's default values unless otherwise
/// specified.
#[derive(Deserialize, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct HttpServerConfig {
    /// Interval to send keep-alive pings via HTTP2.
    /// Note: This is in seconds.
    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub http2_keep_alive_interval: Option<u32>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_max_pending_accept_reset_streams: Option<u32>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_initial_stream_window_size: Option<u32>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_initial_connection_window_size: Option<u32>,

    #[serde(default)]
    pub experimental_http2_adaptive_window: Option<bool>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_max_frame_size: Option<u32>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_max_concurrent_streams: Option<u32>,

    /// Note: This is in seconds.
    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_keep_alive_timeout: Option<u32>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_max_send_buf_size: Option<u32>,

    #[serde(default)]
    pub experimental_http2_enable_connect_protocol: Option<bool>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_max_header_list_size: Option<u32>,
}

#[allow(non_camel_case_types)]
#[derive(Deserialize, Debug)]
pub enum ListenerConfig {
    /// Listener for HTTP/HTTPS/HTTP2 sockets.
    http(HttpListener),
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct HttpListener {
    /// Address to listen on. Example: `127.0.0.1:8080` or `:8080` to listen
    /// to all IPs.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub socket_address: String,

    /// Data transport compression configuration to use for this service.
    #[serde(default)]
    pub compression: CompressionConfig,

    /// Advanced Http server configuration.
    #[serde(default)]
    pub advanced_http: HttpServerConfig,

    /// Tls Configuration for this server.
    /// If not set, the server will not use TLS.
    ///
    /// Default: None
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Name of the server. This is used to help identify the service
    /// for telemetry and logs.
    ///
    /// Default: {index of server in config}
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub name: String,

    /// Configuration
    pub listener: ListenerConfig,

    /// Services to attach to server.
    pub services: Option<ServicesConfig>,
}

#[allow(non_camel_case_types)]
#[derive(Deserialize, Debug)]
pub enum WorkerProperty {
    /// List of static values.
    /// Note: Generally there should only ever be 1 value, but if the platform
    /// property key is PropertyType::Priority it may have more than one value.
    #[serde(deserialize_with = "convert_vec_string_with_shellexpand")]
    values(Vec<String>),

    /// A dynamic configuration. The string will be executed as a command
    /// (not sell) and will be split by "\n" (new line character).
    query_cmd(String),
}

/// Generic config for an endpoint and associated configs.
#[derive(Deserialize, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct EndpointConfig {
    /// URI of the endpoint.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub uri: String,

    /// Timeout in seconds that a request should take.
    /// Default: 5 (seconds)
    pub timeout: Option<f32>,

    /// The TLS configuration to use to connect to the endpoint.
    pub tls_config: Option<ClientTlsConfig>,
}

#[allow(non_camel_case_types)]
#[derive(Copy, Clone, Deserialize, Debug, Default)]
pub enum UploadCacheResultsStrategy {
    /// Only upload action results with an exit code of 0.
    #[default]
    success_only,

    /// Don't upload any action results.
    never,

    /// Upload all action results that complete.
    everything,

    /// Only upload action results that fail.
    failures_only,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Deserialize, Debug)]
pub enum EnvironmentSource {
    /// The name of the platform property in the action to get the value from.
    property(String),

    /// The raw value to set.
    value(#[serde(deserialize_with = "convert_string_with_shellexpand")] String),

    /// The max amount of time in milliseconds the command is allowed to run
    /// (requested by the client).
    timeout_millis,

    /// A special file path will be provided that can be used to comminicate
    /// with the parent process about out-of-band information. This file
    /// will be read after the command has finished executing. Based on the
    /// contents of the file, the behavior of the result may be modified.
    ///
    /// The format of the file contents should be json with the following
    /// schema:
    /// {
    ///   // If set the command will be considered a failure.
    ///   // May be one of the following static strings:
    ///   // "timeout": Will Consider this task to be a timeout.
    ///   "failure": "timeout",
    /// }
    ///
    /// All fields are optional, file does not need to be created and may be
    /// empty.
    side_channel_file,

    /// A "root" directory for the action. This directory can be used to
    /// store temporary files that are not needed after the action has
    /// completed. This directory will be purged after the action has
    /// completed.
    ///
    /// For example:
    /// If an action writes temporary data to a path but nativelink should
    /// clean up this path after the job has executed, you may create any
    /// directory under the path provided in this variable. A common pattern
    /// would be to use `entrypoint` to set a shell script that reads this
    /// variable, `mkdir $ENV_VAR_NAME/tmp` and `export TMPDIR=$ENV_VAR_NAME/tmp`.
    /// Another example might be to bind-mount the `/tmp` path in a container to
    /// this path in `entrypoint`.
    action_directory,
}

#[derive(Deserialize, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct UploadActionResultConfig {
    /// Underlying AC store that the worker will use to publish execution results
    /// into. Objects placed in this store should be reachable from the
    /// scheduler/client-cas after they have finished updating.
    /// Default: {No uploading is done}
    pub ac_store: Option<StoreRefName>,

    /// In which situations should the results be published to the ac_store, if
    /// set to SuccessOnly then only results with an exit code of 0 will be
    /// uploaded, if set to Everything all completed results will be uploaded.
    ///
    /// Default: UploadCacheResultsStrategy::SuccessOnly
    #[serde(default)]
    pub upload_ac_results_strategy: UploadCacheResultsStrategy,

    /// Store to upload historical results to. This should be a CAS store if set.
    ///
    /// Default: {CAS store of parent}
    pub historical_results_store: Option<StoreRefName>,

    /// In which situations should the results be published to the historical CAS.
    /// The historical CAS is where failures are published. These messages conform
    /// to the CAS key-value lookup format and are always a `HistoricalExecuteResponse`
    /// serialized message.
    ///
    /// Default: UploadCacheResultsStrategy::FailuresOnly
    #[serde(default)]
    pub upload_historical_results_strategy: Option<UploadCacheResultsStrategy>,

    /// Template to use for the `ExecuteResponse.message` property. This message
    /// is attached to the response before it is sent to the client. The following
    /// special variables are supported:
    /// - {digest_function} - Digest function used to calculate the action digest.
    /// - {action_digest_hash} - Action digest hash.
    /// - {action_digest_size} - Action digest size.
    /// - {historical_results_hash} - HistoricalExecuteResponse digest hash.
    /// - {historical_results_size} - HistoricalExecuteResponse digest size.
    /// A common use case of this is to provide a link to the web page that contains more
    /// useful information for the user.
    ///
    /// An example that is fully compatible with `bb_browser` is:
    /// <https://example.com/my-instance-name-here/blobs/{digest_function}/action/{action_digest_hash}-{action_digest_size}/>
    ///
    /// Default: "" (no message)
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub success_message_template: String,

    /// Same as `success_message_template` but for failure case.
    ///
    /// An example that is fully compatible with `bb_browser` is:
    /// <https://example.com/my-instance-name-here/blobs/{digest_function}/historical_execute_response/{historical_results_hash}-{historical_results_size}/>
    ///
    /// Default: "" (no message)
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub failure_message_template: String,
}

#[derive(Deserialize, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct LocalWorkerConfig {
    /// Name of the worker. This is give a more friendly name to a worker for logging
    /// and metric publishing.
    /// Default: {Index position in the workers list}
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub name: String,

    /// Endpoint which the worker will connect to the scheduler's WorkerApiService.
    pub worker_api_endpoint: EndpointConfig,

    /// The maximum time an action is allowed to run. If a task requests for a timeout
    /// longer than this time limit, the task will be rejected. Value in seconds.
    ///
    /// Default: 1200 (seconds / 20 mins)
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_action_timeout: usize,

    /// If timeout is handled in `entrypoint` or another wrapper script.
    /// If set to true NativeLink will not honor the timeout the action requested
    /// and instead will always force kill the action after max_action_timeout
    /// has been reached. If this is set to false, the smaller value of the action's
    /// timeout and max_action_timeout will be used to which NativeLink will kill
    /// the action.
    ///
    /// The real timeout can be received via an environment variable set in:
    /// `EnvironmentSource::TimeoutMillis`.
    ///
    /// Example on where this is useful: `entrypoint` launches the action inside
    /// a docker container, but the docker container may need to be downloaded. Thus
    /// the timer should not start until the docker container has started executing
    /// the action. In this case, action will likely be wrapped in another program,
    /// like `timeout` and propagate timeouts via `EnvironmentSource::SideChannelFile`.
    ///
    /// Default: false (NativeLink fully handles timeouts)
    #[serde(default)]
    pub timeout_handled_externally: bool,

    /// The command to execute on every execution request. This will be parsed as
    /// a command + arguments (not shell).
    /// Example: "run.sh" and a job with command: "sleep 5" will result in a
    /// command like: "run.sh sleep 5".
    /// Default: {Use the command from the job request}.
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub entrypoint: String,

    /// An optional script to run before every action is processed on the worker.
    /// The value should be the full path to the script to execute and will pause
    /// all actions on the worker if it returns an exit code other than 0.
    /// If not set, then the worker will never pause and will continue to accept
    /// jobs according to the scheduler configuration.
    /// This is useful, for example, if the worker should not take any more
    /// actions until there is enough resource available on the machine to
    /// handle them.
    pub experimental_precondition_script: Option<String>,

    /// Underlying CAS store that the worker will use to download CAS artifacts.
    /// This store must be a `FastSlowStore`. The `fast` store must be a
    /// `FileSystemStore` because it will use hardlinks when building out the files
    /// instead of copying the files. The slow store must eventually resolve to the
    /// same store the scheduler/client uses to send job requests.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub cas_fast_slow_store: StoreRefName,

    /// Configuration for uploading action results.
    #[serde(default)]
    pub upload_action_result: UploadActionResultConfig,

    /// The directory work jobs will be executed from. This directory will be fully
    /// managed by the worker service and will be purged on startup.
    /// This directory and the directory referenced in local_filesystem_store_ref's
    /// stores::FilesystemStore::content_path must be on the same filesystem.
    /// Hardlinks will be used when placing files that are accessible to the jobs
    /// that are sourced from local_filesystem_store_ref's content_path.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub work_directory: String,

    /// Properties of this worker. This configuration will be sent to the scheduler
    /// and used to tell the scheduler to restrict what should be executed on this
    /// worker.
    pub platform_properties: HashMap<String, WorkerProperty>,

    /// An optional mapping of environment names to set for the execution
    /// as well as those specified in the action itself.  If set, will set each
    /// key as an environment variable before executing the job with the value
    /// of the environment variable being the value of the property of the
    /// action being executed of that name or the fixed value.
    pub additional_environment: Option<HashMap<String, EnvironmentSource>>,
}

#[allow(non_camel_case_types)]
#[derive(Deserialize, Debug)]
pub enum WorkerConfig {
    /// A worker type that executes jobs locally on this machine.
    local(LocalWorkerConfig),
}

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub struct GlobalConfig {
    /// Maximum number of open files that can be opened at one time.
    /// This value is not strictly enforced, it is a best effort. Some internal libraries
    /// open files or read metadata from a files which do not obay this limit, however
    /// the vast majority of cases will have this limit be honored.
    /// As a rule of thumb this value should be less than half the value of `ulimit -n`.
    /// Any network open file descriptors is not counted in this limit, but is counted
    /// in the kernel limit. It is a good idea to set a very large `ulimit -n`.
    /// Note: This value must be greater than 10.
    ///
    /// Default: 512
    #[serde(deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_open_files: usize,

    /// If a file descriptor is idle for this many milliseconds, it will be closed.
    /// In the event a client or store takes a long time to send or receive data
    /// the file descriptor will be closed, and since `max_open_files` blocks new
    /// open_file requests until a slot opens up, it will allow new requests to be
    /// processed. If a read or write is attempted on a closed file descriptor, the
    /// file will be reopened and the operation will continue.
    ///
    /// On services where worker(s) and scheduler(s) live in the same process, this
    /// also prevents deadlocks if a file->file copy is happening, but cannot open
    /// a new file descriptor because the limit has been reached.
    ///
    /// Default: 1000 (1 second)
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub idle_file_descriptor_timeout_millis: u64,

    /// This flag can be used to prevent metrics from being collected at runtime.
    /// Metrics are still able to be collected, but this flag prevents metrics that
    /// are collected at runtime (performance metrics) from being tallied. The
    /// overhead of collecting metrics is very low, so this flag should only be
    /// used if there is a very good reason to disable metrics.
    /// This flag can be forcably set using the `NATIVELINK_DISABLE_METRICS` variable.
    /// If the variable is set it will always disable metrics regardless of what
    /// this flag is set to.
    ///
    /// Default: <true (disabled) if no prometheus service enabled, false otherwise>
    #[serde(default)]
    pub disable_metrics: bool,

    /// Default hash function to use while uploading blobs to the CAS when not set
    /// by client.
    ///
    /// Default: ConfigDigestHashFunction::sha256
    pub default_digest_hash_function: Option<ConfigDigestHashFunction>,

    /// Default digest size to use for health check when running
    /// diagnostics checks. Health checks are expected to use this
    /// size for filling a buffer that is used for creation of
    /// digest.
    ///
    /// Default: 1024*1024 (1MiB)
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub default_digest_size_health_check: usize,
}

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct CasConfig {
    /// List of stores available to use in this config.
    /// The keys can be used in other configs when needing to reference a store.
    pub stores: HashMap<StoreRefName, StoreConfig>,

    /// Worker configurations used to execute jobs.
    pub workers: Option<Vec<WorkerConfig>>,

    /// List of schedulers available to use in this config.
    /// The keys can be used in other configs when needing to reference a
    /// scheduler.
    pub schedulers: Option<HashMap<SchedulerRefName, SchedulerConfig>>,

    /// Servers to setup for this process.
    pub servers: Vec<ServerConfig>,

    /// Any global configurations that apply to all modules live here.
    pub global: Option<GlobalConfig>,
}
