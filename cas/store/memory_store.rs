// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;

use buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use bytes::{Bytes, BytesMut};
use common::DigestInfo;
use config;
use error::{Code, Error, ResultExt};
use evicting_map::{EvictingMap, LenEntry};
use traits::{StoreTrait, UploadSizeInfo};

#[derive(Clone)]
pub struct BytesWrapper(Bytes);

impl Debug for BytesWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BytesWrapper { -- Binary data -- }")
    }
}

impl LenEntry for BytesWrapper {
    #[inline]
    fn len(&self) -> usize {
        Bytes::len(&self.0)
    }
}

pub struct MemoryStore {
    map: EvictingMap<BytesWrapper, SystemTime>,
}

impl MemoryStore {
    pub fn new(config: &config::backends::MemoryStore) -> Self {
        let empty_policy = config::backends::EvictionPolicy::default();
        let eviction_policy = config.eviction_policy.as_ref().unwrap_or(&empty_policy);
        MemoryStore {
            map: EvictingMap::new(eviction_policy, SystemTime::now()),
        }
    }

    pub async fn remove_entry(&self, digest: &DigestInfo) -> bool {
        self.map.remove(digest).await
    }
}

#[async_trait]
impl StoreTrait for MemoryStore {
    async fn has(self: Pin<&Self>, digest: DigestInfo) -> Result<Option<usize>, Error> {
        Ok(self.map.size_for_key(&digest).await.map(|v| v as usize))
    }

    async fn update(
        self: Pin<&Self>,
        digest: DigestInfo,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        let max_size = match size_info {
            UploadSizeInfo::ExactSize(sz) => sz,
            UploadSizeInfo::MaxSize(sz) => sz,
        };
        let buffer = reader
            .collect_all_with_size_hint(max_size)
            .await
            .err_tip(|| "Failed to collect all bytes from reader in memory_store::update")?;

        // Resize our buffer if our max_size was not accurate.
        // The buffer might have reserved much more than the amount of data transferred.
        // This will ensure we use less memory for the long term stored data.
        let buffer = if buffer.len() != max_size {
            let mut new_buffer = BytesMut::with_capacity(buffer.len());
            new_buffer.extend_from_slice(&buffer[..]);
            new_buffer.freeze()
        } else {
            buffer
        };
        self.map.insert(digest, BytesWrapper(buffer)).await;
        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        digest: DigestInfo,
        mut writer: DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> Result<(), Error> {
        let value = self
            .map
            .get(&digest)
            .await
            .err_tip_with_code(|_| (Code::NotFound, format!("Hash {} not found", digest.str())))?;

        let default_len = value.len() - offset;
        let length = length.unwrap_or(default_len).min(default_len);
        if length > 0 {
            writer
                .send(value.0.slice(offset..(offset + length)))
                .await
                .err_tip(|| "Failed to write data in memory store")?;
        }
        writer
            .send_eof()
            .await
            .err_tip(|| "Failed to write EOF in memory store get_part")?;
        Ok(())
    }

    fn as_any(self: Arc<Self>) -> Box<dyn std::any::Any> {
        Box::new(self)
    }
}
