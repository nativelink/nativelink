// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::marker::Send;
use std::pin::Pin;

use async_trait::async_trait;
use futures::Future;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

use common::DigestInfo;
use error::Error;

pub enum StoreType {
    Memory,
}

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
    fn has<'a>(self: Pin<&'a Self>, digest: DigestInfo) -> ResultFuture<'a, bool>;

    fn update<'a>(
        self: Pin<&'a Self>,
        digest: DigestInfo,
        reader: Box<dyn AsyncRead + Send + Unpin + Sync + 'static>,
        upload_size: UploadSizeInfo,
    ) -> ResultFuture<'a, ()>;

    fn get_part<'a>(
        self: Pin<&'a Self>,
        digest: DigestInfo,
        writer: &'a mut (dyn AsyncWrite + Send + Unpin + Sync),
        offset: usize,
        length: Option<usize>,
    ) -> ResultFuture<'a, ()>;

    fn get<'a>(
        self: Pin<&'a Self>,
        digest: DigestInfo,
        writer: &'a mut (dyn AsyncWrite + Send + Unpin + Sync),
    ) -> ResultFuture<'a, ()> {
        self.get_part(digest, writer, 0, None)
    }
}
