// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::convert::TryFrom;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use hex;
use sha2::{Digest, Sha256};

use buf_channel::{make_buf_channel_pair, DropCloserReadHalf, DropCloserWriteHalf};
use common::DigestInfo;
use error::{error_if, Error, ResultExt};
use traits::{StoreTrait, UploadSizeInfo};

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

    fn pin_inner<'a>(&'a self) -> Pin<&'a dyn StoreTrait> {
        Pin::new(self.inner_store.as_ref())
    }
}

async fn inner_check_update(
    mut tx: DropCloserWriteHalf,
    mut rx: DropCloserReadHalf,
    size_info: UploadSizeInfo,
    mut maybe_hasher: Option<([u8; 32], Sha256)>,
) -> Result<(), Error> {
    let mut sum_size: u64 = 0;
    loop {
        let chunk = rx
            .recv()
            .await
            .err_tip(|| "Failed to reach chunk in check_update in verify store")?;
        sum_size += chunk.len() as u64;

        if chunk.len() == 0 {
            // Is EOF.
            if let UploadSizeInfo::ExactSize(expected_size) = size_info {
                error_if!(
                    sum_size != expected_size as u64,
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
            tx.send_eof().await.err_tip(|| "In verify_store::check_update")?;
            break;
        }

        // This will allows us to hash while sending data to another thread.
        let write_future = tx.send(chunk.clone());

        if let Some((_, hasher)) = maybe_hasher.as_mut() {
            hasher.update(chunk.as_ref());
        }

        write_future
            .await
            .err_tip(|| "Failed to write chunk to inner store in verify store")?;
    }
    Ok(())
}

#[async_trait]
impl StoreTrait for VerifyStore {
    async fn has(self: Pin<&Self>, digest: DigestInfo) -> Result<Option<usize>, Error> {
        self.pin_inner().has(digest).await
    }

    async fn update(
        self: Pin<&Self>,
        digest: DigestInfo,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        let digest_size =
            usize::try_from(digest.size_bytes).err_tip(|| "Digest size_bytes was not convertible to usize")?;
        if let UploadSizeInfo::ExactSize(expected_size) = size_info {
            error_if!(
                self.verify_size && expected_size != digest_size,
                "Expected size to match. Got {} but digest says {} on update",
                expected_size,
                digest.size_bytes
            );
        }

        let mut hasher = None;
        if self.verify_hash {
            hasher = Some((digest.packed_hash, Sha256::new()));
        }

        let (tx, rx) = make_buf_channel_pair();

        let update_fut = self.pin_inner().update(digest, rx, size_info);
        let check_fut = inner_check_update(tx, reader, size_info, hasher);

        let (update_res, check_res) = tokio::join!(update_fut, check_fut);

        update_res.merge(check_res)
    }

    async fn get_part(
        self: Pin<&Self>,
        digest: DigestInfo,
        writer: DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> Result<(), Error> {
        self.pin_inner().get_part(digest, writer, offset, length).await
    }

    fn as_any(self: Arc<Self>) -> Box<dyn std::any::Any> {
        Box::new(self)
    }
}
