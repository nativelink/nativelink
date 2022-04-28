// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;

use buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use common::DigestInfo;
use config;
use error::Error;
use traits::{StoreTrait, UploadSizeInfo};

pub struct SizePartitioningStore {
    size: i64,
    lower_store: Arc<dyn StoreTrait>,
    upper_store: Arc<dyn StoreTrait>,
}

impl SizePartitioningStore {
    pub fn new(
        config: &config::backends::SizePartitioningStore,
        lower_store: Arc<dyn StoreTrait>,
        upper_store: Arc<dyn StoreTrait>,
    ) -> Self {
        SizePartitioningStore {
            size: config.size as i64,
            lower_store,
            upper_store,
        }
    }
}

#[async_trait]
impl StoreTrait for SizePartitioningStore {
    async fn has(self: Pin<&Self>, digest: DigestInfo) -> Result<Option<usize>, Error> {
        if digest.size_bytes < self.size {
            return Pin::new(self.lower_store.as_ref()).has(digest).await;
        }
        Pin::new(self.upper_store.as_ref()).has(digest).await
    }

    async fn update(
        self: Pin<&Self>,
        digest: DigestInfo,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        if digest.size_bytes < self.size {
            return Pin::new(self.lower_store.as_ref())
                .update(digest, reader, size_info)
                .await;
        }
        Pin::new(self.upper_store.as_ref())
            .update(digest, reader, size_info)
            .await
    }

    async fn get_part(
        self: Pin<&Self>,
        digest: DigestInfo,
        writer: DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> Result<(), Error> {
        if digest.size_bytes < self.size {
            return Pin::new(self.lower_store.as_ref())
                .get_part(digest, writer, offset, length)
                .await;
        }
        Pin::new(self.upper_store.as_ref())
            .get_part(digest, writer, offset, length)
            .await
    }

    fn as_any(self: Arc<Self>) -> Box<dyn std::any::Any> {
        Box::new(self)
    }
}
