// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::pin::Pin;
use std::task::Poll;

use bytes::{BufMut, Bytes, BytesMut};
use futures::{task::Context, Future, Stream, StreamExt};
use tokio::sync::{mpsc, oneshot};
pub use tokio_util::io::StreamReader;

use error::{make_err, Code, Error, ResultExt};

/// Create a channel pair that can be used to transport buffer objects around to
/// different components. This wrapper is used because the streams give some
/// utility like managing EOF in a more friendly way, ensure if no EOF is received
/// it will send an error to the receiver channel before shutting down and count
/// the number of bytes sent.
pub fn make_buf_channel_pair() -> (DropCloserWriteHalf, DropCloserReadHalf) {
    // We allow up to 2 items in the buffer at any given time. There is no major
    // reason behind this magic number other than thinking it will be nice to give
    // a little time for another thread to wake up and consume data if another
    // thread is pumping large amounts of data into the channel.
    let (tx, rx) = mpsc::channel(2);
    let (close_tx, close_rx) = oneshot::channel();
    (
        DropCloserWriteHalf {
            tx: Some(tx),
            bytes_written: 0,
            close_rx,
        },
        DropCloserReadHalf {
            rx: rx,
            partial: None,
            close_tx: Some(close_tx),
            close_after_size: u64::MAX,
        },
    )
}

/// Writer half of the pair.
pub struct DropCloserWriteHalf {
    tx: Option<mpsc::Sender<Result<Bytes, Error>>>,
    bytes_written: u64,
    /// Receiver channel used to know the error (or success) value of the
    /// receiver end's drop status (ie: if the receiver dropped unexpectedly).
    close_rx: oneshot::Receiver<Result<(), Error>>,
}

impl DropCloserWriteHalf {
    /// Sends data over the channel to the receiver.
    pub async fn send(&mut self, buf: Bytes) -> Result<(), Error> {
        let tx = self
            .tx
            .as_ref()
            .ok_or_else(|| make_err!(Code::Internal, "Tried to send while stream is closed"))?;
        let buf_len = buf.len() as u64;
        assert!(buf_len != 0, "Cannot send EOF in send(). Instead use send_eof()");
        let result = tx
            .send(Ok(buf))
            .await
            .map_err(|_| make_err!(Code::Internal, "Failed to write to data, receiver disconnected"));
        if result.is_err() {
            // Close our channel to prevent drop() from spawning a task.
            self.tx = None;
        }
        self.bytes_written += buf_len;
        result
    }

    /// Sends an EOF (End of File) message to the receiver which will gracefully let the
    /// stream know it has no more data. This will close the stream.
    pub async fn send_eof(&mut self) -> Result<(), Error> {
        assert!(self.tx.is_some(), "Tried to send an EOF when pipe is broken");
        self.tx = None;

        // The final result will be provided in this oneshot channel.
        Pin::new(&mut self.close_rx)
            .await
            .map_err(|_| make_err!(Code::Internal, "Receiver went away before receiving EOF"))?
    }

    /// Forwards data from this writer to a reader. This is an efficient way to bind a writer
    /// and reader together to just forward the data on.
    pub async fn forward<S>(&mut self, mut reader: S, forward_eof: bool) -> Result<(), Error>
    where
        S: Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin,
    {
        loop {
            match reader.next().await {
                Some(maybe_chunk) => {
                    let chunk = maybe_chunk.err_tip(|| "Failed to forward message")?;
                    if chunk.len() == 0 {
                        // Don't send EOF here. We instead rely on None result to be EOF.
                        continue;
                    }
                    self.send(chunk).await?;
                }
                None => {
                    if forward_eof {
                        self.send_eof().await?;
                    }
                    break;
                }
            }
        }
        Ok(())
    }

    /// Returns the number of bytes written so far. This does not mean the receiver received
    /// all of the bytes written to the stream so far.
    pub fn get_bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Returns if the pipe was broken. This is good for determining if the reader broke the
    /// pipe or the writer broke the pipe, since this will only return true if the pipe was
    /// broken by the writer.
    pub fn is_pipe_broken(&self) -> bool {
        self.tx.is_none()
    }
}

impl Drop for DropCloserWriteHalf {
    /// This will notify the reader of an error if we did not send an EOF.
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            // If we do not notify the receiver of the premature close of the stream (ie: without EOF)
            // we could end up with the receiver thinking everything is good and saving this bad data.
            tokio::spawn(async move {
                let _ = tx
                    .send(Err(
                        make_err!(Code::Internal, "Writer was dropped before EOF was sent",),
                    ))
                    .await; // Nowhere to send failure to write here.
            });
        }
    }
}

/// Reader half of the pair.
pub struct DropCloserReadHalf {
    rx: mpsc::Receiver<Result<Bytes, Error>>,
    /// Represents a partial chunk of data. This is used when we only wanted
    /// to take a part of the chunk in the stream and leave the rest.
    partial: Option<Result<Bytes, Error>>,
    /// A channel used to notify the sender that we are closed (with error).
    close_tx: Option<oneshot::Sender<Result<(), Error>>>,
    /// Once this number of bytes is sent the stream will be considered closed.
    /// This is a work around for cases when we never receive an EOF because the
    /// reader's future is dropped because it got the exact amount of data and
    /// will never poll more. This prevents the `drop()` handle from sending an
    /// error to our writer that we dropped the stream before receiving an EOF
    /// if we know the exact amount of data we will receive in this stream.
    close_after_size: u64,
}

impl DropCloserReadHalf {
    /// Receive a chunk of data.
    pub async fn recv(&mut self) -> Result<Bytes, Error> {
        let maybe_chunk = match self.partial.take() {
            Some(result_bytes) => Some(result_bytes),
            None => self.rx.recv().await,
        };
        match maybe_chunk {
            Some(Ok(chunk)) => {
                let chunk_len = chunk.len() as u64;
                assert!(chunk_len != 0, "Chunk should never be EOF, expected None in this case");
                assert!(
                    self.close_after_size >= chunk_len,
                    "Received too much data. This only happens when `close_after_size` is set."
                );
                self.close_after_size -= chunk_len;
                if self.close_after_size == 0 {
                    assert!(self.close_tx.is_some(), "Expected stream to not be closed");
                    self.close_tx.take().unwrap().send(Ok(())).map_err(|_| {
                        make_err!(Code::Internal, "Failed to send closing ok message to write with size")
                    })?;
                }
                Ok(chunk)
            }

            Some(Err(e)) => Err(e),

            // None is a safe EOF received.
            None => {
                // Notify our sender that we received the EOF with success.
                if let Some(close_tx) = self.close_tx.take() {
                    close_tx
                        .send(Ok(()))
                        .map_err(|_| make_err!(Code::Internal, "Failed to send closing ok message to write"))?;
                }
                Ok(Bytes::new())
            }
        }
    }

    /// Sets the number of bytes before the stream will be considered closed.
    pub fn set_close_after_size(&mut self, size: u64) {
        self.close_after_size = size;
    }

    /// Utility function that will collect all the data of the stream into a Bytes struct.
    /// This method is optimized to reduce copies when possible.
    pub async fn collect_all_with_size_hint(mut self, size_hint: usize) -> Result<Bytes, Error> {
        let (first_chunk, second_chunk) = {
            // This is an optimization for when there's only one chunk and an EOF.
            // This prevents us from any copies and we just shuttle the bytes.
            let first_chunk = self
                .recv()
                .await
                .err_tip(|| "Failed to recv first chunk in collect_all_with_size_hint")?;

            if first_chunk.len() == 0 {
                return Ok(first_chunk);
            }

            let second_chunk = self
                .recv()
                .await
                .err_tip(|| "Failed to recv second chunk in collect_all_with_size_hint")?;

            if second_chunk.len() == 0 {
                return Ok(first_chunk);
            }
            (first_chunk, second_chunk)
        };

        let mut buf = BytesMut::with_capacity(size_hint);
        buf.put(first_chunk);
        buf.put(second_chunk);

        loop {
            let chunk = self
                .recv()
                .await
                .err_tip(|| "Failed to recv in collect_all_with_size_hint")?;
            if chunk.len() == 0 {
                break; // EOF.
            }
            buf.put(chunk);
        }
        Ok(buf.freeze())
    }

    /// Takes exactly `size` number of bytes from the stream and returns them.
    /// This means the stream will keep polling until either an EOF is received or
    /// `size` bytes are received and concat them all together then return them.
    /// This method is optimized to reduce copies when possible.
    pub async fn take(&mut self, size: usize) -> Result<Bytes, Error> {
        fn populate_partial_if_needed(
            current_size: usize,
            desired_size: usize,
            chunk: &mut Bytes,
            partial: &mut Option<Result<Bytes, Error>>,
        ) {
            if current_size + chunk.len() <= desired_size {
                return;
            }
            assert!(partial.is_none(), "Partial should have been consumed during the recv()");
            let local_partial = chunk.split_off(desired_size - current_size);
            *partial = if local_partial.len() == 0 {
                None
            } else {
                Some(Ok(local_partial))
            };
        }

        let (first_chunk, second_chunk) = {
            // This is an optimization for a relatively common case when the first chunk in the
            // stream satisfies all the requirements to fill this `take()`.
            // This will us from needing to copy the data into a new buffer and instead we can
            // just forward on the original Bytes object. If we need more than the first chunk
            // we will then go the slow path and actually copy our data.
            let mut first_chunk = self.recv().await.err_tip(|| "During first buf_channel::take()")?;
            populate_partial_if_needed(0, size, &mut first_chunk, &mut self.partial);
            if first_chunk.len() == 0 || first_chunk.len() >= size {
                assert!(
                    first_chunk.len() == 0 || first_chunk.len() == size,
                    "Length should be exactly size here"
                );
                return Ok(first_chunk);
            }

            let mut second_chunk = self.recv().await.err_tip(|| "During second buf_channel::take()")?;
            if second_chunk.len() == 0 {
                assert!(
                    first_chunk.len() <= size,
                    "Length should never be larger than size here"
                );
                return Ok(first_chunk);
            }
            populate_partial_if_needed(first_chunk.len(), size, &mut second_chunk, &mut self.partial);
            (first_chunk, second_chunk)
        };
        let mut output = BytesMut::with_capacity(size);
        output.put(first_chunk);
        output.put(second_chunk);

        loop {
            let mut chunk = self.recv().await.err_tip(|| "During buf_channel::take()")?;
            if chunk.len() == 0 {
                break; // EOF.
            }

            populate_partial_if_needed(output.len(), size, &mut chunk, &mut self.partial);

            output.put(chunk);

            if output.len() >= size {
                assert!(output.len() == size); // Length should never be larger than size here.
                break;
            }
        }
        Ok(output.freeze())
    }
}

impl Stream for DropCloserReadHalf {
    type Item = Result<Bytes, std::io::Error>;

    // TODO(blaise.bruer) This is not very efficient as we are creating a new future on every
    // poll() call. It might be better to use a waker.
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Box::pin(self.recv()).as_mut().poll(cx).map(|result| match result {
            Ok(bytes) => {
                if bytes.len() == 0 {
                    return None;
                }
                Some(Ok(bytes))
            }
            Err(e) => Some(Err(e.to_std_err())),
        })
    }
}
