// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::fs::Metadata;
use std::io::IoSlice;
use std::path::Path;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use error::{make_err, Code, Error, ResultExt};
use log;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf, SeekFrom};
use tokio::sync::{Semaphore, SemaphorePermit};

/// We wrap all tokio::fs items in our own wrapper so we can limit the number of outstanding
/// open files at any given time. This will greatly reduce the chance we'll hit open file limit
/// issues.

#[derive(Debug)]
pub struct FileSlot<'a> {
    // We hold the permit because once it is dropped it goes back into the queue.
    _permit: SemaphorePermit<'a>,
    inner: tokio::fs::File,
}

impl<'a> AsRef<tokio::fs::File> for FileSlot<'a> {
    fn as_ref(&self) -> &tokio::fs::File {
        &self.inner
    }
}

impl<'a> AsMut<tokio::fs::File> for FileSlot<'a> {
    fn as_mut(&mut self) -> &mut tokio::fs::File {
        &mut self.inner
    }
}

impl<'a> AsyncRead for FileSlot<'a> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), tokio::io::Error>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<'a> AsyncSeek for FileSlot<'a> {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> Result<(), tokio::io::Error> {
        Pin::new(&mut self.inner).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<u64, tokio::io::Error>> {
        Pin::new(&mut self.inner).poll_complete(cx)
    }
}

impl<'a> AsyncWrite for FileSlot<'a> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, tokio::io::Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), tokio::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), tokio::io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<Result<usize, tokio::io::Error>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

const DEFAULT_OPEN_FILE_PERMITS: usize = 10;
static OPEN_FILE_SEMAPHORE: Semaphore = Semaphore::const_new(DEFAULT_OPEN_FILE_PERMITS);

pub fn set_open_file_limit(limit: usize) {
    if limit < DEFAULT_OPEN_FILE_PERMITS {
        log::error!(
            "set_open_file_limit({}) must be greater than {}",
            limit,
            DEFAULT_OPEN_FILE_PERMITS
        );
        return;
    }
    OPEN_FILE_SEMAPHORE.add_permits(limit - DEFAULT_OPEN_FILE_PERMITS);
}

pub async fn open_file(path: impl AsRef<Path> + std::fmt::Debug) -> Result<FileSlot<'static>, Error> {
    let permit = OPEN_FILE_SEMAPHORE
        .acquire()
        .await
        .map_err(|e| make_err!(Code::Internal, "Open file semaphore closed {:?}", e))?;
    Ok(FileSlot {
        _permit: permit,
        inner: tokio::fs::File::open(&path)
            .await
            .err_tip(|| format!("Could not open {:?}", path))?,
    })
}

pub async fn create_file(path: impl AsRef<Path> + std::fmt::Debug) -> Result<FileSlot<'static>, Error> {
    let permit = OPEN_FILE_SEMAPHORE
        .acquire()
        .await
        .map_err(|e| make_err!(Code::Internal, "Open file semaphore closed {:?}", e))?;
    Ok(FileSlot {
        _permit: permit,
        inner: tokio::fs::File::create(&path)
            .await
            .err_tip(|| format!("Could not open {:?}", path))?,
    })
}

pub async fn hard_link(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<(), Error> {
    let _permit = OPEN_FILE_SEMAPHORE
        .acquire()
        .await
        .map_err(|e| make_err!(Code::Internal, "Open file semaphore closed {:?}", e))?;
    tokio::fs::hard_link(src, dst).await.map_err(|e| e.into())
}

pub async fn set_permissions(src: impl AsRef<Path>, perm: std::fs::Permissions) -> Result<(), Error> {
    let _permit = OPEN_FILE_SEMAPHORE
        .acquire()
        .await
        .map_err(|e| make_err!(Code::Internal, "Open file semaphore closed {:?}", e))?;
    tokio::fs::set_permissions(src, perm).await.map_err(|e| e.into())
}

pub async fn create_dir(path: impl AsRef<Path>) -> Result<(), Error> {
    let _permit = OPEN_FILE_SEMAPHORE
        .acquire()
        .await
        .map_err(|e| make_err!(Code::Internal, "Open file semaphore closed {:?}", e))?;
    tokio::fs::create_dir(path).await.map_err(|e| e.into())
}

pub async fn create_dir_all(path: impl AsRef<Path>) -> Result<(), Error> {
    let _permit = OPEN_FILE_SEMAPHORE
        .acquire()
        .await
        .map_err(|e| make_err!(Code::Internal, "Open file semaphore closed {:?}", e))?;
    tokio::fs::create_dir_all(path).await.map_err(|e| e.into())
}

pub async fn symlink(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<(), Error> {
    let _permit = OPEN_FILE_SEMAPHORE
        .acquire()
        .await
        .map_err(|e| make_err!(Code::Internal, "Open file semaphore closed {:?}", e))?;
    tokio::fs::symlink(src, dst).await.map_err(|e| e.into())
}

pub async fn read_link(path: impl AsRef<Path>) -> Result<std::path::PathBuf, Error> {
    let _permit = OPEN_FILE_SEMAPHORE
        .acquire()
        .await
        .map_err(|e| make_err!(Code::Internal, "Open file semaphore closed {:?}", e))?;
    tokio::fs::read_link(path).await.map_err(|e| e.into())
}

pub struct ReadDir<'a> {
    // We hold the permit because once it is dropped it goes back into the queue.
    permit: SemaphorePermit<'a>,
    inner: tokio::fs::ReadDir,
}

impl<'a> ReadDir<'a> {
    pub fn into_inner(self) -> (SemaphorePermit<'a>, tokio::fs::ReadDir) {
        (self.permit, self.inner)
    }
}

impl<'a> AsRef<tokio::fs::ReadDir> for ReadDir<'a> {
    fn as_ref(&self) -> &tokio::fs::ReadDir {
        &self.inner
    }
}

impl<'a> AsMut<tokio::fs::ReadDir> for ReadDir<'a> {
    fn as_mut(&mut self) -> &mut tokio::fs::ReadDir {
        &mut self.inner
    }
}

pub async fn read_dir(path: impl AsRef<Path>) -> Result<ReadDir<'static>, Error> {
    let permit = OPEN_FILE_SEMAPHORE
        .acquire()
        .await
        .map_err(|e| make_err!(Code::Internal, "Open file semaphore closed {:?}", e))?;
    Ok(ReadDir {
        permit,
        inner: tokio::fs::read_dir(path).await.map_err(|e| Into::<Error>::into(e))?,
    })
}

pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> Result<(), Error> {
    let _permit = OPEN_FILE_SEMAPHORE
        .acquire()
        .await
        .map_err(|e| make_err!(Code::Internal, "Open file semaphore closed {:?}", e))?;
    tokio::fs::rename(from, to).await.map_err(|e| e.into())
}

pub async fn remove_file(path: impl AsRef<Path>) -> Result<(), Error> {
    let _permit = OPEN_FILE_SEMAPHORE
        .acquire()
        .await
        .map_err(|e| make_err!(Code::Internal, "Open file semaphore closed {:?}", e))?;
    tokio::fs::remove_file(path).await.map_err(|e| e.into())
}

pub async fn metadata(path: impl AsRef<Path>) -> Result<Metadata, Error> {
    let _permit = OPEN_FILE_SEMAPHORE
        .acquire()
        .await
        .map_err(|e| make_err!(Code::Internal, "Open file semaphore closed {:?}", e))?;
    tokio::fs::metadata(path).await.map_err(|e| e.into())
}

pub async fn read(path: impl AsRef<Path>) -> Result<Vec<u8>, Error> {
    let _permit = OPEN_FILE_SEMAPHORE
        .acquire()
        .await
        .map_err(|e| make_err!(Code::Internal, "Open file semaphore closed {:?}", e))?;
    tokio::fs::read(path).await.map_err(|e| e.into())
}

pub async fn symlink_metadata(path: impl AsRef<Path>) -> Result<Metadata, Error> {
    let _permit = OPEN_FILE_SEMAPHORE
        .acquire()
        .await
        .map_err(|e| make_err!(Code::Internal, "Open file semaphore closed {:?}", e))?;
    tokio::fs::symlink_metadata(path).await.map_err(|e| e.into())
}
