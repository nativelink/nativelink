// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::sync::Arc;

use config::{self, backends::StoreConfig};
use error::Error;
use memory_store::MemoryStore;
use s3_store::S3Store;
use verify_store::VerifyStore;

pub use traits::{StoreTrait as Store, StoreType};

pub struct StoreManager {
    stores: HashMap<String, Arc<dyn Store>>,
}

fn private_make_store(backend: &StoreConfig) -> Result<Arc<dyn Store>, Error> {
    match backend {
        StoreConfig::memory(config) => Ok(Arc::new(MemoryStore::new(&config))),
        StoreConfig::s3_store(config) => Ok(Arc::new(S3Store::new(&config)?)),
        StoreConfig::verify(config) => Ok(Arc::new(VerifyStore::new(
            &config,
            private_make_store(&config.backend)?,
        ))),
    }
}

impl StoreManager {
    pub fn new() -> StoreManager {
        StoreManager { stores: HashMap::new() }
    }

    pub fn make_store(&mut self, name: &str, backend: &StoreConfig) -> Result<Arc<dyn Store>, Error> {
        let store = private_make_store(backend)?;
        self.stores.insert(name.to_string(), store.clone());
        Ok(store)
    }

    pub fn get_store(&self, name: &str) -> Option<&Arc<dyn Store>> {
        self.stores.get(name)
    }
}
