// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::marker::Send;
use std::time::SystemTime;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use common::DigestInfo;
use config;
use error::{Code, ResultExt};
use evicting_map::EvictingMap;
use traits::{ResultFuture, StoreTrait, UploadSizeInfo};

pub struct MemoryStore {
    map: EvictingMap<Vec<u8>, SystemTime>,
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
        mut reader: Box<dyn AsyncRead + Send + Sync + Unpin + 'static>,
        size_info: UploadSizeInfo,
    ) -> ResultFuture<'a, ()> {
        Box::pin(async move {
            let max_size = match size_info {
                UploadSizeInfo::ExactSize(sz) => sz,
                UploadSizeInfo::MaxSize(sz) => sz,
            };
            let mut buffer = Vec::with_capacity(max_size);
            reader.read_to_end(&mut buffer).await?;
            buffer.shrink_to_fit();
            self.map.insert(digest, buffer).await;
            Ok(())
        })
    }

    fn get_part<'a>(
        self: std::pin::Pin<&'a Self>,
        digest: DigestInfo,
        writer: &'a mut (dyn AsyncWrite + Send + Unpin + Sync),
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
                .write_all(&value[offset..(offset + length)])
                .await
                .err_tip(|| "Error writing all data to writer")?;
            writer.write(&[]).await.err_tip(|| "Error writing EOF to writer")?;
            writer.shutdown().await.err_tip(|| "Error shutting down writer")?;
            Ok(())
        })
    }
}
