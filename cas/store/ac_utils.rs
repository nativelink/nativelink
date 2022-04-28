// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::default::Default;
use std::io::Cursor;
use std::pin::Pin;

use bytes::BytesMut;
use futures::{future::try_join, Future, FutureExt, TryFutureExt};
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};

use buf_channel::make_buf_channel_pair;
use common::DigestInfo;
use error::{Code, Error, ResultExt};
use store::{Store, UploadSizeInfo};

// NOTE(blaise.bruer) From some local testing it looks like action cache items are rarely greater than
// 1.2k. Giving a bit more just in case to reduce allocs.
pub const ESTIMATED_DIGEST_SIZE: usize = 2048;

/// This is more of a safety check. We are going to collect this entire message
/// into memory. If we don't bound the max size of the object we enable users
/// to use up all the memory on this machine.
const MAX_ACTION_MSG_SIZE: usize = 10 << 20; // 10mb.

/// Attempts to fetch the digest contents from a store into the associated proto.
pub async fn get_and_decode_digest<T: Message + Default>(
    store: Pin<&dyn Store>,
    digest: &DigestInfo,
) -> Result<T, Error> {
    let mut store_data_resp = store
        .get_part_unchunked(
            digest.clone(),
            0,
            Some(MAX_ACTION_MSG_SIZE),
            Some(ESTIMATED_DIGEST_SIZE),
        )
        .await;
    if let Err(err) = &mut store_data_resp {
        if err.code == Code::NotFound {
            // Trim the error code. Not Found is quite common and we don't want to send a large
            // error (debug) message for something that is common. We resize to just the last
            // message as it will be the most relevant.
            err.messages.resize_with(1, || "".to_string());
        }
    }
    let store_data = store_data_resp?;

    T::decode(store_data).err_tip_with_code(|e| (Code::NotFound, format!("Stored value appears to be corrupt: {}", e)))
}

/// Takes a proto message and will serialize it and upload it to the provided store.
pub fn serialize_and_upload_message<'a, T: Message>(
    message: &'a T,
    cas_store: Pin<&'a dyn Store>,
) -> impl Future<Output = Result<DigestInfo, Error>> + 'a {
    async move {
        let mut buffer = BytesMut::new();
        let digest = {
            message
                .encode(&mut buffer)
                .err_tip(|| "Could not encode directory proto")?;
            let mut hasher = Sha256::new();
            hasher.update(&buffer);
            DigestInfo::new(hasher.finalize().into(), buffer.len() as i64)
        };
        upload_to_store(cas_store, digest.clone(), &mut Cursor::new(buffer)).await?;
        Ok(digest)
    }
}

/// Given a bytestream computes the digest for the data.
/// Note: This will happen in a new spawn since computing digests can be thread intensive.
pub fn compute_digest<R: AsyncRead + Unpin + Send + 'static>(
    mut reader: R,
) -> impl Future<Output = Result<(DigestInfo, R), Error>> {
    tokio::spawn(async move {
        const DEFAULT_READ_BUFF_SIZE: usize = 4096;
        let mut chunk = BytesMut::with_capacity(DEFAULT_READ_BUFF_SIZE);
        let mut hasher = Sha256::new();
        let mut digest_size = 0;
        loop {
            reader
                .read_buf(&mut chunk)
                .await
                .err_tip(|| "Could not read chunk during compute_digest")?;
            if chunk.is_empty() {
                break; // EOF.
            }
            digest_size += chunk.len();
            hasher.update(&chunk);
            chunk.clear();
        }

        Ok((DigestInfo::new(hasher.finalize().into(), digest_size as i64), reader))
    })
    .map(|r| r.err_tip(|| "Failed to launch spawn")?)
}

/// Uploads data to our store for given digest.
/// Sadly we cannot upload our data while computing our hash, this means that we often
/// will need to read the file two times, one to hash the file and the other to upload
/// it. In the future we could possibly upload to store while computing the hash and
/// then "finish" the upload by giving the digest, but not all stores will support this
/// for now we will just always read twice.
pub fn upload_to_store<'a, R: AsyncRead + Unpin>(
    cas_store: Pin<&'a dyn Store>,
    digest: DigestInfo,
    reader: &'a mut R,
) -> impl Future<Output = Result<(), Error>> + 'a {
    let (mut tx, rx) = make_buf_channel_pair();
    let upload_to_store_fut = cas_store
        .update(
            digest.clone(),
            rx,
            UploadSizeInfo::ExactSize(digest.size_bytes as usize),
        )
        .map(|r| r.err_tip(|| "Could not upload data to store in upload_to_store"));
    let read_data_fut = async move {
        loop {
            const DEFAULT_READ_BUFF_SIZE: usize = 4096;
            let mut chunk = BytesMut::with_capacity(DEFAULT_READ_BUFF_SIZE);
            reader
                .read_buf(&mut chunk)
                .await
                .err_tip(|| "Could not read chunk during upload_to_store")?;
            if chunk.len() == 0 {
                break; // EOF.
            }
            tx.send(chunk.freeze())
                .await
                .err_tip(|| "Could not send buffer data to store in upload_to_store")?;
        }
        tx.send_eof()
            .await
            .err_tip(|| "Could not send EOF to store in upload_to_store")?;
        Ok(())
    };
    try_join(read_data_fut, upload_to_store_fut).map_ok(|(_, _)| ())
}
