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

#[derive(Deserialize, Debug)]
pub struct CapabilitiesConfig {}

#[derive(Deserialize, Debug)]
pub struct ExecutionConfig {
    // TODO(allada) Not implemented yet.
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
    /// bazel prtotocol. This service is used to provide the supported
    /// features and versions of this bazel GRPC service.
    pub capabilities: Option<CapabilitiesConfig>,

    /// The remote execution service configuration. This service is
    /// under development and is currently just a place holder.
    pub execution: Option<ExecutionConfig>,

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
