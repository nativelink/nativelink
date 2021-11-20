// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;

use serde::Deserialize;

use backends;

/// Name of the store. This type will be used when referencing a store
/// in the `CasConfig::stores`'s map key.
pub type StoreRefName = String;

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
pub struct CapabilitiesConfig {
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
}

/// When the scheduler matches tasks to workers that are capable of running
/// the task, this value will be used to determine how the property is
/// treated.
///
/// `Minimum` will require the platform property to be a u64 and when the
/// scheduler looks for appropriate worker nodes that are capable of
/// executing the task, the task will not run on a node that has less than
/// this value.
///
/// `Exact` will require the platform property to be a string and when the
/// scheduler looks for appropriate worker nodes that are capable of
/// executing the task, the task will not run on a node that does not have
/// this property set to the value with exact string match.
#[derive(Deserialize, Debug, Clone, Copy)]
pub enum PropertyType {
    Minimum,
    Exact,
}

#[derive(Deserialize, Debug)]
pub struct ExecutionConfig {
    /// The store name referenced in the `stores` map in the main config.
    /// This store name referenced here may be reused multiple times.
    /// This value must be an AC (Action Cache) store reference.
    pub ac_store: StoreRefName,

    /// The store name referenced in the `stores` map in the main config.
    /// This store name referenced here may be reused multiple times.
    /// This value must be a CAS store reference.
    pub cas_store: StoreRefName,
}

#[derive(Deserialize, Debug)]
pub struct ByteStreamConfig {
    /// Name of the store in the "stores" configuration.
    pub cas_stores: HashMap<InstanceName, StoreRefName>,

    // Max number of bytes to send on each grpc stream chunk.
    pub max_bytes_per_stream: usize,
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
}

#[derive(Deserialize, Debug)]
pub struct ServerConfig {
    /// Address to listen on. Example: `127.0.0.1:8080` or `:8080` to listen
    /// to all IPs.
    pub listen_address: String,

    /// Services to attach to server.
    pub services: Option<ServicesConfig>,
}

#[derive(Deserialize, Debug)]
pub struct CasConfig {
    /// List of stores available to use in this config.
    /// The keys can be used in other conigs when needing to reference a store.
    pub stores: HashMap<StoreRefName, backends::StoreConfig>,

    /// Servers to setup for this process.
    pub servers: Vec<ServerConfig>,
}
