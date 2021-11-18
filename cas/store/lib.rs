// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use compression_store::CompressionStore;
use config::{self, backends::StoreConfig};
use dedup_store::DedupStore;
use error::Error;
use fast_slow_store::FastSlowStore;
use filesystem_store::FilesystemStore;
use futures::Future;
use memory_store::MemoryStore;
use s3_store::S3Store;
use verify_store::VerifyStore;

pub use traits::{StoreTrait as Store, UploadSizeInfo};

pub struct StoreManager {
    stores: HashMap<String, Arc<dyn Store>>,
}

fn private_make_store<'a>(
    backend: &'a StoreConfig,
) -> Pin<Box<dyn Future<Output = Result<Arc<dyn Store>, Error>> + 'a>> {
    Box::pin(async move {
        let store: Arc<dyn Store> = match backend {
            StoreConfig::memory(config) => Arc::new(MemoryStore::new(&config)),
            StoreConfig::s3_store(config) => Arc::new(S3Store::new(&config)?),
            StoreConfig::verify(config) => {
                Arc::new(VerifyStore::new(&config, private_make_store(&config.backend).await?))
            }
            StoreConfig::compression(config) => Arc::new(CompressionStore::new(
                *config.clone(),
                private_make_store(&config.backend).await?,
            )?),
            StoreConfig::dedup(config) => Arc::new(DedupStore::new(
                &config,
                private_make_store(&config.index_store).await?,
                private_make_store(&config.content_store).await?,
            )),
            StoreConfig::fast_slow(config) => Arc::new(FastSlowStore::new(
                &config,
                private_make_store(&config.fast).await?,
                private_make_store(&config.slow).await?,
            )),
            StoreConfig::filesystem(config) => Arc::new(FilesystemStore::new(&config).await?),
        };
        Ok(store)
    })
}

impl StoreManager {
    pub fn new() -> StoreManager {
        StoreManager { stores: HashMap::new() }
    }

    pub async fn make_store(&mut self, name: &str, backend: &StoreConfig) -> Result<Arc<dyn Store>, Error> {
        let store = private_make_store(backend).await?;
        self.stores.insert(name.to_string(), store.clone());
        Ok(store)
    }

    pub fn get_store(&self, name: &str) -> Option<&Arc<dyn Store>> {
        self.stores.get(name)
    }
}
