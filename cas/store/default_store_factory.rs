// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::pin::Pin;
use std::sync::Arc;

use compression_store::CompressionStore;
use config::{self, backends::StoreConfig};
use dedup_store::DedupStore;
use error::Error;
use fast_slow_store::FastSlowStore;
use filesystem_store::FilesystemStore;
use futures::Future;
use grpc_store::GrpcStore;
use memory_store::MemoryStore;
use ref_store::RefStore;
use s3_store::S3Store;
use size_partitioning_store::SizePartitioningStore;
use store::{Store, StoreManager};
use verify_store::VerifyStore;

pub fn store_factory<'a>(
    backend: &'a StoreConfig,
    store_manager: &'a Arc<StoreManager>,
) -> Pin<Box<dyn Future<Output = Result<Arc<dyn Store>, Error>> + 'a>> {
    Box::pin(async move {
        let store: Arc<dyn Store> = match backend {
            StoreConfig::memory(config) => Arc::new(MemoryStore::new(&config)),
            StoreConfig::s3_store(config) => Arc::new(S3Store::new(&config)?),
            StoreConfig::verify(config) => Arc::new(VerifyStore::new(
                &config,
                store_factory(&config.backend, store_manager).await?,
            )),
            StoreConfig::compression(config) => Arc::new(CompressionStore::new(
                *config.clone(),
                store_factory(&config.backend, store_manager).await?,
            )?),
            StoreConfig::dedup(config) => Arc::new(DedupStore::new(
                &config,
                store_factory(&config.index_store, store_manager).await?,
                store_factory(&config.content_store, store_manager).await?,
            )),
            StoreConfig::fast_slow(config) => Arc::new(FastSlowStore::new(
                &config,
                store_factory(&config.fast, store_manager).await?,
                store_factory(&config.slow, store_manager).await?,
            )),
            StoreConfig::filesystem(config) => Arc::new(FilesystemStore::new(&config).await?),
            StoreConfig::ref_store(config) => Arc::new(RefStore::new(&config, store_manager.clone())),
            StoreConfig::size_partitioning(config) => Arc::new(SizePartitioningStore::new(
                &config,
                store_factory(&config.lower_store, store_manager).await?,
                store_factory(&config.upper_store, store_manager).await?,
            )),
            StoreConfig::grpc(config) => Arc::new(GrpcStore::new(&config).await?),
        };
        Ok(store)
    })
}
