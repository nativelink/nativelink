// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::marker::Send;
use std::pin::Pin;

use async_trait::async_trait;
use futures::{join, try_join, Future};
use serde::{Deserialize, Serialize};

use buf_channel::{make_buf_channel_pair, DropCloserReadHalf, DropCloserWriteHalf};
use bytes::Bytes;
use common::DigestInfo;
use error::{Error, ResultExt};

pub type ResultFuture<'a, Res> = Pin<Box<dyn Future<Output = Result<Res, Error>> + 'a + Send>>;

#[derive(Debug, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub enum UploadSizeInfo {
    /// When the data transfer amount is known to be exact size, this enum should be used.
    /// The receiver store can use this to better optimize the way the data is sent or stored.
    ExactSize(usize),

    /// When the data transfer amount is not known to be exact, the caller should use this enum
    /// to provide the maximum size that could possibly be sent. This will bypass the exact size
    /// checks, but still provide useful information to the underlying store about the data being
    /// sent that it can then use to optimize the upload process.
    MaxSize(usize),
}

#[async_trait]
pub trait StoreTrait: Sync + Send + Unpin {
    fn has<'a>(self: Pin<&'a Self>, digest: DigestInfo) -> ResultFuture<'a, Option<usize>>;

    fn update<'a>(
        self: Pin<&'a Self>,
        digest: DigestInfo,
        reader: DropCloserReadHalf,
        upload_size: UploadSizeInfo,
    ) -> ResultFuture<'a, ()>;

    // Utility to send all the data to the store when you have all the bytes.
    async fn update_oneshot(self: Pin<&Self>, digest: DigestInfo, data: Bytes) -> Result<(), Error> {
        // TODO(blaise.bruer) This is extremely inefficient, since we have exactly
        // what we need here. Maybe we could instead make a version of the stream
        // that can take objects already fully in memory instead?
        let (mut tx, rx) = make_buf_channel_pair();

        let data_len = data.len();
        let send_fut = async move {
            // Only send if we are not EOF.
            if data.len() != 0 {
                tx.send(data)
                    .await
                    .err_tip(|| "Failed to write data in update_oneshot")?;
            }
            tx.send_eof()
                .await
                .err_tip(|| "Failed to write EOF in update_oneshot")?;
            Ok(())
        };
        try_join!(send_fut, self.update(digest, rx, UploadSizeInfo::ExactSize(data_len)))?;
        Ok(())
    }

    fn get_part<'a>(
        self: Pin<&'a Self>,
        digest: DigestInfo,
        writer: DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> ResultFuture<'a, ()>;

    fn get<'a>(self: Pin<&'a Self>, digest: DigestInfo, writer: DropCloserWriteHalf) -> ResultFuture<'a, ()> {
        self.get_part(digest, writer, 0, None)
    }

    // Utility that will return all the bytes at once instead of in a streaming manner.
    async fn get_part_unchunked(
        self: Pin<&Self>,
        digest: DigestInfo,
        offset: usize,
        length: Option<usize>,
        size_hint: Option<usize>,
    ) -> Result<Bytes, Error> {
        // TODO(blaise.bruer) This is extremely inefficient, since we have exactly
        // what we need here. Maybe we could instead make a version of the stream
        // that can take objects already fully in memory instead?
        let (tx, rx) = make_buf_channel_pair();

        let (data_res, get_part_res) = join!(
            rx.collect_all_with_size_hint(length.unwrap_or(size_hint.unwrap_or(0))),
            self.get_part(digest, tx, offset, length),
        );
        get_part_res
            .err_tip(|| "Failed to get_part in get_part_unchunked")
            .merge(data_res.err_tip(|| "Failed to read stream to completion in get_part_unchunked"))
    }
}
