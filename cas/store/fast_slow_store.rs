// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::marker::Send;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::future::{join, try_join3};
use futures::FutureExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use async_fixed_buffer::AsyncFixedBuf;
use common::DigestInfo;
use config;
use error::{make_err, Code, ResultExt};
use traits::{ResultFuture, StoreTrait, UploadSizeInfo};

// Note: If this value is updated, you must also update documentation in backends.rs.
const DEFAULT_BUFFER_SIZE: usize = 8 * 1024;

// TODO(blaise.bruer) This store needs to be evaluated for more efficient memory usage,
// there are many copies happening internally.

// TODO(blaise.bruer) We should consider copying the data in the background to allow the
// client to hang up while the data is buffered. An alternative is to possibly make a
// "BufferedStore" that could be placed on the "slow" store that would hang up early
// if data is in the buffer.
pub struct FastSlowStore {
    fast_store: Arc<dyn StoreTrait>,
    slow_store: Arc<dyn StoreTrait>,
    buffer_size: usize,
}

impl FastSlowStore {
    pub fn new(
        config: &config::backends::FastSlowStore,
        fast_store: Arc<dyn StoreTrait>,
        slow_store: Arc<dyn StoreTrait>,
    ) -> Self {
        let mut buffer_size = config.buffer_size as usize;
        if buffer_size == 0 {
            buffer_size = DEFAULT_BUFFER_SIZE;
        }
        Self {
            fast_store,
            slow_store,
            buffer_size,
        }
    }

    fn pin_fast_store<'a>(&'a self) -> std::pin::Pin<&'a dyn StoreTrait> {
        Pin::new(self.fast_store.as_ref())
    }

    fn pin_slow_store<'a>(&'a self) -> std::pin::Pin<&'a dyn StoreTrait> {
        Pin::new(self.slow_store.as_ref())
    }
}

#[async_trait]
impl StoreTrait for FastSlowStore {
    fn has<'a>(self: std::pin::Pin<&'a Self>, digest: DigestInfo) -> ResultFuture<'a, Option<usize>> {
        Box::pin(async move {
            // TODO(blaise.bruer) Investigate if we should maybe ignore errors here instead of
            // forwarding the up.
            if let Some(sz) = self.pin_fast_store().has(digest.clone()).await? {
                return Ok(Some(sz));
            }
            self.pin_slow_store().has(digest).await
        })
    }

    fn update<'a>(
        self: std::pin::Pin<&'a Self>,
        digest: DigestInfo,
        mut reader: Box<dyn AsyncRead + Send + Sync + Unpin + 'static>,
        size_info: UploadSizeInfo,
    ) -> ResultFuture<'a, ()> {
        Box::pin(async move {
            let (fast_rx, mut fast_tx) = AsyncFixedBuf::new(vec![0u8; self.buffer_size]).split_into_reader_writer();
            let (slow_rx, mut slow_tx) = AsyncFixedBuf::new(vec![0u8; self.buffer_size]).split_into_reader_writer();

            let data_stream_fut = async move {
                let mut buffer = BytesMut::with_capacity(self.buffer_size);
                loop {
                    reader
                        .read_buf(&mut buffer)
                        .await
                        .err_tip(|| "Failed to read buffer in fastslow store")?;
                    if buffer.is_empty() {
                        // EOF received.
                        fast_tx
                            .write(&[])
                            .await
                            .err_tip(|| "Failed to write eof to fast store in fast_slow store update")?;
                        slow_tx
                            .write(&[])
                            .await
                            .err_tip(|| "Failed to write eof to writer in fast_slow store update")?;
                        return Ok(());
                    }

                    let mut fast_tx_buf = buffer.split().freeze();
                    let mut slow_tx_buf = fast_tx_buf.clone();

                    let fast_send_fut = fast_tx.write_all_buf(&mut fast_tx_buf);
                    let slow_send_fut = slow_tx.write_all_buf(&mut slow_tx_buf);

                    let (fast_result, slow_result) = join(fast_send_fut, slow_send_fut).await;
                    fast_result.map_err(|e| {
                        make_err!(
                            Code::Internal,
                            "Failed to send message to fast_store in fast_slow_store {:?}",
                            e
                        )
                    })?;
                    slow_result.map_err(|e| {
                        make_err!(
                            Code::Internal,
                            "Failed to send message to slow_store in fast_slow store {:?}",
                            e
                        )
                    })?;
                }
            };

            let fast_store_fut = self
                .pin_slow_store()
                .update(digest.clone(), Box::new(fast_rx), size_info);
            let slow_store_fut = self.pin_fast_store().update(digest, Box::new(slow_rx), size_info);

            try_join3(data_stream_fut, fast_store_fut, slow_store_fut).await?;
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
            // TODO(blaise.bruer) Investigate if we should maybe ignore errors here instead of
            // forwarding the up.
            let fast_store = self.pin_fast_store();
            let slow_store = self.pin_slow_store();
            if fast_store.has(digest.clone()).await?.is_some() {
                return fast_store.get_part(digest.clone(), writer, offset, length).await;
            }
            // We can only copy the data to our fast store if we are copying everything.
            if offset != 0 || length.is_some() {
                return slow_store.get_part(digest, writer, offset, length).await;
            }

            let sz_result = slow_store
                .has(digest.clone())
                .await
                .err_tip(|| "Failed to run has() on slow store");
            let sz = match sz_result {
                Ok(Some(sz)) => sz,
                Ok(None) => {
                    return Err(make_err!(
                        Code::NotFound,
                        "Object not found in either fast or slow store"
                    ))
                }
                Err(err) => return Err(err),
            };

            let (fast_rx, mut fast_tx) = AsyncFixedBuf::new(vec![0u8; 4096]).split_into_reader_writer();
            let (mut slow_rx, mut slow_tx) = AsyncFixedBuf::new(vec![0u8; 4096]).split_into_reader_writer();
            let data_stream_fut = async move {
                let mut writer_pin = Pin::new(writer);
                let mut output_buf = BytesMut::new();
                loop {
                    slow_rx
                        .read_buf(&mut output_buf)
                        .await
                        .err_tip(|| "Failed to read data data buffer from slow store")?;
                    if output_buf.is_empty() {
                        // EOF received.
                        fast_tx
                            .write(&[])
                            .await
                            .err_tip(|| "Failed to write eof to fast store in fast_slow store")?;
                        writer_pin
                            .write(&[])
                            .await
                            .err_tip(|| "Failed to write eof to writer in fast_slow store")?;
                        return Ok(());
                    }
                    let (fast_tx_res, writer_res) = {
                        let mut fast_tx_buf = output_buf.split().freeze();
                        let mut writer_buf = fast_tx_buf.clone();
                        let fast_write_fut = fast_tx.write_all_buf(&mut fast_tx_buf).boxed();
                        let writer_fut = writer_pin.write_all_buf(&mut writer_buf).boxed();
                        join(fast_write_fut, writer_fut).await
                    };
                    if let Err(err) = fast_tx_res {
                        return Err(err).err_tip(|| "Failed to write to fast store in fast_slow store");
                    }
                    if let Err(err) = writer_res {
                        return Err(err).err_tip(|| "Failed to write result to writer in fast_slow store");
                    }
                }
            };

            let slow_store_fut = slow_store.get(digest.clone(), &mut slow_tx);
            let fast_store_fut = fast_store.update(digest, Box::new(fast_rx), UploadSizeInfo::ExactSize(sz));

            try_join3(data_stream_fut, slow_store_fut, fast_store_fut).await?;
            Ok(())
        })
    }
}
