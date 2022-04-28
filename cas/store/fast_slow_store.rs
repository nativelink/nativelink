// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{join, FutureExt};

use buf_channel::{make_buf_channel_pair, DropCloserReadHalf, DropCloserWriteHalf};
use common::DigestInfo;
use config;
use error::{make_err, Code, Error, ResultExt};
use traits::{StoreTrait, UploadSizeInfo};

// TODO(blaise.bruer) This store needs to be evaluated for more efficient memory usage,
// there are many copies happening internally.

// TODO(blaise.bruer) We should consider copying the data in the background to allow the
// client to hang up while the data is buffered. An alternative is to possibly make a
// "BufferedStore" that could be placed on the "slow" store that would hang up early
// if data is in the buffer.
pub struct FastSlowStore {
    fast_store: Arc<dyn StoreTrait>,
    slow_store: Arc<dyn StoreTrait>,
}

impl FastSlowStore {
    pub fn new(
        _config: &config::backends::FastSlowStore,
        fast_store: Arc<dyn StoreTrait>,
        slow_store: Arc<dyn StoreTrait>,
    ) -> Self {
        Self { fast_store, slow_store }
    }

    pub fn fast_store<'a>(&'a self) -> &'a Arc<dyn StoreTrait> {
        &self.fast_store
    }

    /// Ensure our fast store is populated. This should be kept as a low
    /// cost function. Since the data itself is shared and not copied it should be fairly
    /// low cost to just discard the data, but does cost a few mutex locks while
    /// streaming.
    pub async fn populate_fast_store(self: Pin<&Self>, digest: DigestInfo) -> Result<(), Error> {
        let maybe_size_info = self
            .pin_fast_store()
            .has(digest.clone())
            .await
            .err_tip(|| "While querying in populate_fast_store")?;
        if let Some(_) = maybe_size_info {
            return Ok(());
        }
        // TODO(blaise.bruer) This is extremely inefficient, since we are just trying
        // to send the stream to /dev/null. Maybe we could instead make a version of
        // the stream that can send to the drain more efficiently?
        let (tx, mut rx) = make_buf_channel_pair();
        let drain_fut = async move {
            while !rx.recv().await?.is_empty() {}
            Ok(())
        };
        let (drain_res, get_res) = join!(drain_fut, self.get(digest, tx));
        get_res.err_tip(|| "Failed to populate()").merge(drain_res)
    }

    fn pin_fast_store<'a>(&'a self) -> Pin<&'a dyn StoreTrait> {
        Pin::new(self.fast_store.as_ref())
    }

    fn pin_slow_store<'a>(&'a self) -> Pin<&'a dyn StoreTrait> {
        Pin::new(self.slow_store.as_ref())
    }
}

#[async_trait]
impl StoreTrait for FastSlowStore {
    async fn has(self: Pin<&Self>, digest: DigestInfo) -> Result<Option<usize>, Error> {
        // TODO(blaise.bruer) Investigate if we should maybe ignore errors here instead of
        // forwarding the up.
        if let Some(sz) = self.pin_fast_store().has(digest.clone()).await? {
            return Ok(Some(sz));
        }
        self.pin_slow_store().has(digest).await
    }

    async fn update(
        self: Pin<&Self>,
        digest: DigestInfo,
        mut reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        let (mut fast_tx, fast_rx) = make_buf_channel_pair();
        let (mut slow_tx, slow_rx) = make_buf_channel_pair();

        let data_stream_fut = async move {
            loop {
                let buffer = reader
                    .recv()
                    .await
                    .err_tip(|| "Failed to read buffer in fastslow store")?;
                if buffer.len() == 0 {
                    // EOF received.
                    fast_tx
                        .send_eof()
                        .await
                        .err_tip(|| "Failed to write eof to fast store in fast_slow store update")?;
                    slow_tx
                        .send_eof()
                        .await
                        .err_tip(|| "Failed to write eof to writer in fast_slow store update")?;
                    return Result::<(), Error>::Ok(());
                }

                let (fast_result, slow_result) = join!(fast_tx.send(buffer.clone()), slow_tx.send(buffer));
                fast_result
                    .map_err(|e| {
                        make_err!(
                            Code::Internal,
                            "Failed to send message to fast_store in fast_slow_store {:?}",
                            e
                        )
                    })
                    .merge(slow_result.map_err(|e| {
                        make_err!(
                            Code::Internal,
                            "Failed to send message to slow_store in fast_slow store {:?}",
                            e
                        )
                    }))?;
            }
        };

        let fast_store_fut = self.pin_slow_store().update(digest.clone(), fast_rx, size_info);
        let slow_store_fut = self.pin_fast_store().update(digest, slow_rx, size_info);

        let (data_stream_res, fast_res, slow_res) = join!(data_stream_fut, fast_store_fut, slow_store_fut);
        data_stream_res.merge(fast_res).merge(slow_res)?;
        Ok(())
    }

    async fn get_part(
        self: Pin<&Self>,
        digest: DigestInfo,
        mut writer: DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> Result<(), Error> {
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

        let (mut fast_tx, fast_rx) = make_buf_channel_pair();
        let (slow_tx, mut slow_rx) = make_buf_channel_pair();
        let data_stream_fut = async move {
            let mut writer_pin = Pin::new(&mut writer);
            loop {
                let output_buf = slow_rx
                    .recv()
                    .await
                    .err_tip(|| "Failed to read data data buffer from slow store")?;
                if output_buf.len() == 0 {
                    // Write out our EOF.
                    // It is possible for the client to disconnect the stream because they got
                    // all the data they wanted, which could lead to an error when writing this
                    // EOF. If that was to happen, we could end up terminating this early and
                    // the resulting upload to the fast store might fail.
                    let _ = fast_tx.send_eof().await?;
                    let _ = writer_pin.send_eof().await?;
                    return Ok(());
                }
                let (fast_tx_res, writer_res) = join!(
                    fast_tx.send(output_buf.clone()).boxed(),
                    writer_pin.send(output_buf).boxed(),
                );
                if let Err(err) = fast_tx_res {
                    return Err(err).err_tip(|| "Failed to write to fast store in fast_slow store");
                }
                if let Err(err) = writer_res {
                    return Err(err).err_tip(|| "Failed to write result to writer in fast_slow store");
                }
            }
        };

        let slow_store_fut = slow_store.get(digest.clone(), slow_tx);
        let fast_store_fut = fast_store.update(digest, fast_rx, UploadSizeInfo::ExactSize(sz));

        let (data_stream_res, slow_res, fast_res) = join!(data_stream_fut, slow_store_fut, fast_store_fut);
        data_stream_res.merge(fast_res).merge(slow_res)?;
        Ok(())
    }

    fn as_any(self: Arc<Self>) -> Box<dyn std::any::Any> {
        Box::new(self)
    }
}
