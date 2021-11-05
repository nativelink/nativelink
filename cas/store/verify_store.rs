// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::convert::TryFrom;
use std::marker::Send;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use hex;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, WriteHalf};

use async_fixed_buffer::AsyncFixedBuf;
use common::DigestInfo;
use error::{error_if, Error, ResultExt};
use traits::{ResultFuture, StoreTrait, UploadSizeInfo};

pub struct VerifyStore {
    inner_store: Arc<dyn StoreTrait>,
    verify_size: bool,
    verify_hash: bool,
}

impl VerifyStore {
    pub fn new(config: &config::backends::VerifyStore, inner_store: Arc<dyn StoreTrait>) -> Self {
        VerifyStore {
            inner_store: inner_store,
            verify_size: config.verify_size,
            verify_hash: config.verify_hash,
        }
    }

    fn pin_inner<'a>(&'a self) -> std::pin::Pin<&'a dyn StoreTrait> {
        Pin::new(self.inner_store.as_ref())
    }
}

async fn inner_check_update<'a>(
    mut tx: WriteHalf<AsyncFixedBuf<Box<[u8]>>>,
    mut reader: Box<dyn AsyncRead + Send + Sync + Unpin + 'a>,
    size_info: &UploadSizeInfo,
    mut maybe_hasher: Option<([u8; 32], Sha256)>,
) -> Result<(), Error> {
    let mut buffer = vec![0u8; 1024 * 4];
    let mut sum_size: usize = 0;
    loop {
        let sz = reader
            .read(&mut buffer[..])
            .await
            .err_tip(|| "Stream read terminated early")?;
        sum_size += sz;
        let write_future = tx.write_all(&buffer[0..sz]);
        // This will allows us to hash while sending data to another thread.
        if let Some((_, hasher)) = maybe_hasher.as_mut() {
            hasher.update(&buffer[0..sz]);
        }
        write_future.await.err_tip(|| "Failed to write to underlying store")?;
        if sz != 0 {
            continue;
        }
        if let UploadSizeInfo::ExactSize(expected_size) = size_info {
            error_if!(
                sum_size != *expected_size,
                "Expected size {} but got size {} on insert",
                expected_size,
                sum_size
            );
        }
        if let Some((original_hash, hasher)) = maybe_hasher {
            let hash_result: [u8; 32] = hasher.finalize().into();
            error_if!(
                original_hash != hash_result,
                "Hashes do not match, got: {} but digest hash was {}",
                hex::encode(original_hash),
                hex::encode(hash_result),
            );
        }

        // Note: EOF is not sent from write_all() only sent in write().
        tx.write(&vec![]).await.err_tip(|| "Failed to write EOF byte")?;
        tx.shutdown()
            .await
            .err_tip(|| "Failed to shutdown underlying verify_size stream")?;
        return Ok(());
    }
}

#[async_trait]
impl StoreTrait for VerifyStore {
    fn has<'a>(self: std::pin::Pin<&'a Self>, digest: DigestInfo) -> ResultFuture<'a, bool> {
        Box::pin(async move { self.pin_inner().has(digest).await })
    }

    fn update<'a>(
        self: std::pin::Pin<&'a Self>,
        digest: DigestInfo,
        reader: Box<dyn AsyncRead + Send + Sync + Unpin + 'static>,
        size_info: UploadSizeInfo,
    ) -> ResultFuture<'a, ()> {
        Box::pin(async move {
            let digest_size =
                usize::try_from(digest.size_bytes).err_tip(|| "Digest size_bytes was not convertible to usize")?;
            if let UploadSizeInfo::ExactSize(ref expected_size) = size_info {
                error_if!(
                    self.verify_size && *expected_size != digest_size,
                    "Expected size to match. Got {} but digest says {} on update",
                    expected_size,
                    digest.size_bytes
                );
            }
            let mut raw_fixed_buffer = AsyncFixedBuf::new(vec![0u8; 1024 * 4].into_boxed_slice());
            let stream_closer_fut = raw_fixed_buffer.get_closer();
            let (rx, tx) = tokio::io::split(raw_fixed_buffer);

            let hash_copy = digest.packed_hash;
            let inner_store_clone = self.inner_store.clone();
            let spawn_future = tokio::spawn(async move {
                Pin::new(inner_store_clone.as_ref())
                    .update(digest, Box::new(rx), size_info)
                    .await
            });
            let mut hasher = None;
            if self.verify_hash {
                hasher = Some((hash_copy, Sha256::new()));
            }
            let result = inner_check_update(tx, reader, &size_info, hasher).await;
            stream_closer_fut.await;
            result.merge(
                spawn_future
                    .await
                    .err_tip(|| "Failed to join verify size spawn")
                    .and_then(|v| v),
            )
        })
    }

    fn get_part<'a>(
        self: std::pin::Pin<&'a Self>,
        digest: DigestInfo,
        writer: &'a mut (dyn AsyncWrite + Send + Unpin + Sync),
        offset: usize,
        length: Option<usize>,
    ) -> ResultFuture<'a, ()> {
        Box::pin(async move { self.pin_inner().get_part(digest, writer, offset, length).await })
    }
}
