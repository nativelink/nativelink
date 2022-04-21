// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;

use serde::Deserialize;

use backends;

/// Name of the store. This type will be used when referencing a store
/// in the `CasConfig::stores`'s map key.
pub type StoreRefName = String;

/// Name of the scheduler. This type will be used when referencing a
/// scheduler in the `CasConfig::schedulers`'s map key.
pub type SchedulerRefName = String;

/// Used when the config references `instance_name` in the protocol.
pub type InstanceName = String;

#[derive(Deserialize, Debug)]
pub struct AcStoreConfig {
    /// The store name referenced in the `stores` map in the main config.
    /// This store name referenced here may be reused multiple times.
    pub ac_store: StoreRefName,
}

#[derive(Deserialize, Debug)]
pub struct CasStoreConfig {
    /// The store name referenced in the `stores` map in the main config.
    /// This store name referenced here may be reused multiple times.
    pub cas_store: StoreRefName,
}

#[derive(Deserialize, Debug, Default)]
pub struct SchedulerConfig {
    /// A list of supported platform properties mapped to how these properties
    /// are used when the scheduler looks for worker nodes capable of running
    /// the task.
    ///
    /// For example, a value of:
    /// ```
    /// { "cpu_count": "Minimum", "cpu_arch": "Exact" }
    /// ```
    /// With a job that contains:
    /// ```
    /// { "cpu_count": "8", "cpu_arch": "arm" }
    /// ```
    /// Will result in the scheduler filtering out any workers that do not have
    /// "cpu_arch" = "arm" and filter out any workers that have less than 8 cpu
    /// cores available.
    ///
    /// The property names here must match the property keys provided by the
    /// worker nodes when they join the pool. In other words, the workers will
    /// publish their capabilities to the scheduler when they join the worker
    /// pool. If the worker fails to notify the scheduler of it's (for example)
    /// "cpu_arch", the scheduler will never send any jobs to it, if all jobs
    /// have the "cpu_arch" label. There is no special treatment of any platform
    /// property labels other and entirely driven by worker configs and this
    /// config.
    pub supported_platform_properties: Option<HashMap<String, PropertyType>>,

    /// Remove workers from pool once the worker has not responded in this
    /// amount of time in seconds.
    /// Default: 5 (seconds)
    #[serde(default)]
    pub worker_timeout_s: u64,
}

#[derive(Deserialize, Debug, Default)]
pub struct CapabilitiesRemoteExecutionConfig {
    /// Scheduler used to configure the capabilities of remote execution.
    pub scheduler: SchedulerRefName,
}

#[derive(Deserialize, Debug, Default)]
pub struct CapabilitiesConfig {
    /// Configuration for remote execution capabilities.
    /// If not set the capabilities service will inform the client that remote
    /// execution is not supported.
    pub remote_execution: Option<CapabilitiesRemoteExecutionConfig>,
}

/// When the scheduler matches tasks to workers that are capable of running
/// the task, this value will be used to determine how the property is treated.
#[derive(Deserialize, Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum PropertyType {
    /// Requires the platform property to be a u64 and when the scheduler looks
    /// for appropriate worker nodes that are capable of executing the task,
    /// the task will not run on a node that has less than this value.
    Minimum,

    /// Requires the platform property to be a string and when the scheduler
    /// looks for appropriate worker nodes that are capable of executing the
    /// task, the task will not run on a node that does not have this property
    /// set to the value with exact string match.
    Exact,

    /// Does not restrict on this value and instead will be passed to the worker
    /// as an informational piece.
    /// TODO(allada) In the future this will be used by the scheduler and worker
    /// to cause the scheduler to prefer certain workers over others, but not
    /// restrict them based on these values.
    Priority,
}

#[derive(Deserialize, Debug)]
pub struct ExecutionConfig {
    /// The store name referenced in the `stores` map in the main config.
    /// This store name referenced here may be reused multiple times.
    /// This value must be a CAS store reference.
    pub cas_store: StoreRefName,

    /// The scheduler name referenced in the `schedulers` map in the main config.
    pub scheduler: SchedulerRefName,
}

#[derive(Deserialize, Debug)]
pub struct ByteStreamConfig {
    /// Name of the store in the "stores" configuration.
    pub cas_stores: HashMap<InstanceName, StoreRefName>,

    // Max number of bytes to send on each grpc stream chunk.
    pub max_bytes_per_stream: usize,
}

#[derive(Deserialize, Debug)]
pub struct WorkerApiConfig {
    /// The scheduler name referenced in the `schedulers` map in the main config.
    pub scheduler: SchedulerRefName,
}

#[derive(Deserialize, Debug)]
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
}

#[derive(Deserialize, Debug)]
pub struct ServerConfig {
    /// Address to listen on. Example: `127.0.0.1:8080` or `:8080` to listen
    /// to all IPs.
    pub listen_address: String,

    /// Services to attach to server.
    pub services: Option<ServicesConfig>,
}

#[allow(non_camel_case_types)]
#[derive(Deserialize, Debug)]
pub enum WrokerProperty {
    /// List of static values.
    /// Note: Generally there should only ever be 1 value, but if the platform
    /// property key is PropertyType::Priority it may have more than one value.
    values(Vec<String>),

    /// A dynamic configuration. The string will be executed as a command
    /// (not sell) and will be split by "\n" (new line character).
    query_cmd(String),
}

/// Generic config for an endpoint and associated configs.
#[derive(Deserialize, Debug, Default)]
pub struct EndpointConfig {
    /// URI of the endpoint.
    pub uri: String,

    /// Timeout in seconds that a request should take.
    /// Default: 5 (seconds)
    pub timeout: Option<f32>,
}

#[derive(Deserialize, Debug, Default)]
pub struct LocalWorkerConfig {
    /// Endpoint which the worker will connect to the scheduler's WorkerApiService.
    pub worker_api_endpoint: EndpointConfig,

    /// The command to execute on every execution request. This will be parsed as
    /// a command + arguments (not shell).
    /// '$@' has a special meaning in that all the arguments will expand into this
    /// location.
    /// Example: "run.sh $@" and a job with command: "sleep 5" will result in a
    /// command like: "run.sh sleep 5".
    pub entrypoint_cmd: String,

    /// Reference to a filesystem store (runtime enforced). This store will be used
    /// to store a local cache of files for optimization purposes.
    /// Must be a reference to a store implementing backends::FilesystemStore.
    pub local_filesystem_store_ref: StoreRefName,

    /// Underlying CAS store that the worker will use to download CAS artifacts.
    /// This store must have the same objects that the scheduler/client-cas uses.
    /// The scheduler will send job requests that will reference objects stored
    /// in this store. If the objects referenced in the job request don't exist
    /// in this store an error may be returned.
    pub cas_store: StoreRefName,

    /// Underlying AC store that the worker will use to publish execution results
    /// into. Objects placed in this store should be reachable from the
    /// scheduler/client-cas after they have finished updating.
    pub ac_store: StoreRefName,

    /// The directory work jobs will be executed from. This directory will be fully
    /// managed by the worker service and will be purged on startup.
    /// This directory and the directory referenced in local_filesystem_store_ref's
    /// backends::FilesystemStore::content_path must be on the same filesystem.
    /// Hardlinks will be used when placing files that are accessible to the jobs
    /// that are sourced from local_filesystem_store_ref's content_path.
    pub work_directory: String,

    /// Properties of this worker. This configuration will be sent to the scheduler
    /// and used to tell the scheduler to restrict what should be executed on this
    /// worker.
    pub platform_properties: HashMap<String, WrokerProperty>,
}

#[allow(non_camel_case_types)]
#[derive(Deserialize, Debug)]
pub enum WorkerConfig {
    /// A worker type that executes jobs locally on this machine.
    local(LocalWorkerConfig),
}

#[derive(Deserialize, Debug)]
pub struct CasConfig {
    /// List of stores available to use in this config.
    /// The keys can be used in other configs when needing to reference a store.
    pub stores: HashMap<StoreRefName, backends::StoreConfig>,

    /// Worker configurations used to execute jobs.
    pub workers: Option<Vec<WorkerConfig>>,

    /// List of schedulers available to use in this config.
    /// The keys can be used in other configs when needing to reference a
    /// scheduler.
    pub schedulers: Option<HashMap<SchedulerRefName, SchedulerConfig>>,

    /// Servers to setup for this process.
    pub servers: Vec<ServerConfig>,
}
