// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::time::SystemTime;

use async_trait::async_trait;

use buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use bytes::{Bytes, BytesMut};
use common::DigestInfo;
use config;
use error::{Code, ResultExt};
use evicting_map::EvictingMap;
use traits::{ResultFuture, StoreTrait, UploadSizeInfo};

pub struct MemoryStore {
    map: EvictingMap<Bytes, SystemTime>,
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
    fn has<'a>(self: std::pin::Pin<&'a Self>, digest: DigestInfo) -> ResultFuture<'a, Option<usize>> {
        Box::pin(async move { Ok(self.map.size_for_key(&digest).await.map(|v| v as usize)) })
    }

    fn update<'a>(
        self: std::pin::Pin<&'a Self>,
        digest: DigestInfo,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> ResultFuture<'a, ()> {
        Box::pin(async move {
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
            self.map.insert(digest, buffer).await;
            Ok(())
        })
    }

    fn get_part<'a>(
        self: std::pin::Pin<&'a Self>,
        digest: DigestInfo,
        mut writer: DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> ResultFuture<'a, ()> {
        Box::pin(async move {
            let value = self
                .map
                .get(&digest)
                .await
                .err_tip_with_code(|_| (Code::NotFound, format!("Hash {} not found", digest.str())))?;

            let default_len = value.len() - offset;
            let length = length.unwrap_or(default_len).min(default_len);
            writer
                .send(value.slice(offset..(offset + length)))
                .await
                .err_tip(|| "Failed to write data in memory store")?;
            writer
                .send_eof()
                .await
                .err_tip(|| "Failed to write EOF in memory store get_part")?;
            Ok(())
        })
    }
}
