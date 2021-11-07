// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

#![forbid(unsafe_code)]

use std::convert::AsRef;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use fast_async_mutex::mutex::Mutex;
use fixed_buffer::FixedBuf;
use futures::{ready, Future};
use pin_project_lite::pin_project;
use tokio::io::{self, AsyncRead, AsyncWrite, ReadBuf, ReadHalf, WriteHalf};

pin_project! {
    /// This library was significantly inspired by:
    /// https://docs.rs/fixed-buffer-tokio/0.1.1/fixed_buffer_tokio/
    ///
    /// This struct will buffer the data between an `AsyncRead` and `AsyncWrite`
    /// with a fixed buffer as an intermediary. In addition it gives the ability
    /// to call `get_closer()` which if awaited will close the writer stream.
    /// Finally, this struct can also deal with EOF in a more natural manner.
    pub struct AsyncFixedBuf<T> {
        inner: FixedBuf<T>,
        waker: Arc<Mutex<Option<Waker>>>,
        did_shutdown: Arc<AtomicBool>,
        received_eof: AtomicBool,
    }
}

fn wake(waker: &mut Option<Waker>) {
    waker.take().map(|w| {
        w.wake();
    });
}

fn park(waker: &mut Option<Waker>, cx: &Context<'_>) {
    wake(waker);
    *waker = Some(cx.waker().clone());
}

impl<T> AsyncFixedBuf<T> {
    /// Creates a new FixedBuf and wraps it in an AsyncFixedBuf.
    ///
    /// See
    /// [`FixedBuf::new`](https://docs.rs/fixed-buffer/latest/fixed_buffer/struct.FixedBuf.html#method.new)
    /// for details.
    pub fn new(mem: T) -> Self {
        AsyncFixedBuf {
            inner: FixedBuf::new(mem),
            waker: Arc::new(Mutex::new(None)),
            did_shutdown: Arc::new(AtomicBool::new(false)),
            received_eof: AtomicBool::new(false),
        }
    }

    /// Utility method that can be used to get a lambda that will close the
    /// stream. This is useful for the reader to close the stream.
    pub fn get_closer(&mut self) -> Pin<Box<dyn Future<Output = ()> + Sync + Send>> {
        let did_shutdown = self.did_shutdown.clone();
        let waker = self.waker.clone();
        Box::pin(async move {
            if did_shutdown.swap(true, Ordering::Relaxed) {
                return;
            }
            wake(waker.lock().await.deref_mut());
        })
    }
}

impl<T: AsMut<[u8]> + AsRef<[u8]> + Unpin> AsyncFixedBuf<T> {
    pub fn split_into_reader_writer(mut self) -> (DropCloserReadHalf<T>, DropCloserWriteHalf<T>) {
        let did_shutdown_r = self.did_shutdown.clone();
        let did_shutdown_w = self.did_shutdown.clone();
        let closer_r = self.get_closer();
        let closer_w = self.get_closer();
        let (rx, tx) = io::split(self);
        (
            DropCloserReadHalf {
                inner: rx,
                did_shutdown: did_shutdown_r,
                close_fut: Some(closer_r),
            },
            DropCloserWriteHalf {
                inner: tx,
                did_shutdown: did_shutdown_w,
                close_fut: Some(closer_w),
            },
        )
    }
}

impl<T: AsRef<[u8]> + Unpin> AsyncRead for AsyncFixedBuf<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let me = self.project();
        let mut waker_lock = me.waker.lock();
        let mut waker = ready!(Pin::new(&mut waker_lock).poll(cx));

        let num_read = me.inner.read_and_copy_bytes(buf.initialize_unfilled());
        buf.advance(num_read);
        if num_read <= 0 {
            if me.received_eof.load(Ordering::Relaxed) {
                wake(&mut waker);
                return Poll::Ready(Ok(()));
            } else if me.did_shutdown.load(Ordering::Relaxed) {
                wake(&mut waker);
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Sender disconnected",
                )));
            }
        }
        // Have not yet reached EOF and have no data to read,
        // so we need to try and wake our writer, replace the waker and wait.
        if num_read <= 0 {
            park(&mut waker, &cx);
            return Poll::Pending;
        }
        wake(&mut waker);
        Poll::Ready(Ok(()))
    }
}

impl<T: AsMut<[u8]> + AsRef<[u8]>> AsyncWrite for AsyncFixedBuf<T> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, std::io::Error>> {
        let me = self.project();
        let mut waker_lock = me.waker.lock();
        let mut waker = ready!(Pin::new(&mut waker_lock).poll(cx));

        if me.did_shutdown.load(Ordering::Relaxed) {
            wake(&mut waker);
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Receiver disconnected",
            )));
        }
        if buf.len() == 0 {
            // EOF happens when a zero byte message is sent.
            me.received_eof.store(true, Ordering::Relaxed);
            me.did_shutdown.store(true, Ordering::Relaxed);
            wake(&mut waker);
            return Poll::Ready(Ok(0));
        }
        match me.inner.writable() {
            Some(writable_slice) => {
                let write_amt = buf.len().min(writable_slice.len());
                if write_amt > 0 {
                    writable_slice[..write_amt].clone_from_slice(&buf[..write_amt]);
                    me.inner.wrote(write_amt);
                }
                wake(&mut waker);
                Poll::Ready(Ok(write_amt))
            }
            None => {
                // Sometimes it is more efficient to recover some more space for the writer to use. Since
                // this is not a ring buffer we need to re-arrange the data every once in a while.
                // TODO(blaise.bruer) A ringbuffer implementation would likely be quite a lot more efficient.
                if (me.inner.len() as f32) / (me.inner.capacity() as f32) < 0.25 {
                    me.inner.shift();
                    // After `shift()` is called there's a good chance that we can now write to this buffer.
                    // So we call our waker immediately then return pending. This is a special case and
                    // we don't need to let the reader know yet as the next call to write will do the proper
                    // thing.
                    // NOTE: There's an extremely small chance that the above logic could cause an infinite loop.
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }

                park(&mut waker, &cx);
                Poll::Pending
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        let mut waker_lock = self.waker.lock();
        let mut waker = ready!(Pin::new(&mut waker_lock).poll(cx));

        if self.inner.is_empty() {
            wake(&mut waker);
            return Poll::Ready(Ok(()));
        }
        park(&mut waker, &cx);
        Poll::Pending
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        let mut waker_lock = self.waker.lock();
        let mut waker = ready!(Pin::new(&mut waker_lock).poll(cx));

        self.did_shutdown.store(true, Ordering::Relaxed);
        wake(&mut waker);
        Poll::Ready(Ok(()))
    }
}

pin_project! {
    pub struct DropCloserReadHalf<T> {
        #[pin]
        inner: ReadHalf<AsyncFixedBuf<T>>,
        did_shutdown: Arc<AtomicBool>,
        close_fut: Option<Pin<Box<dyn Future<Output = ()> + Sync + Send>>>,
    }

    impl<T> PinnedDrop for DropCloserReadHalf<T> {
        fn drop(mut this: Pin<&mut Self>) {
            if !this.did_shutdown.load(Ordering::Relaxed) {
                let close_fut = this.close_fut.take().unwrap();
                tokio::spawn(close_fut);
            }
        }
    }
}

impl<T: AsRef<[u8]> + Unpin> AsyncRead for DropCloserReadHalf<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let me = self.project();
        me.inner.poll_read(cx, buf)
    }
}

pin_project! {
    pub struct DropCloserWriteHalf<T> {
        #[pin]
        inner: WriteHalf<AsyncFixedBuf<T>>,
        did_shutdown: Arc<AtomicBool>,
        close_fut: Option<Pin<Box<dyn Future<Output = ()> + Sync + Send>>>,
    }

    impl<T> PinnedDrop for DropCloserWriteHalf<T> {
        fn drop(mut this: Pin<&mut Self>) {
            if !this.did_shutdown.load(Ordering::Relaxed) {
                let close_fut = this.close_fut.take().unwrap();
                tokio::spawn(async move { close_fut.await });
            }
        }
    }
}

impl<T: AsMut<[u8]> + AsRef<[u8]>> AsyncWrite for DropCloserWriteHalf<T> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, std::io::Error>> {
        let me = self.project();
        me.inner.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        let me = self.project();
        me.inner.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        let me = self.project();
        me.inner.poll_shutdown(cx)
    }
}
