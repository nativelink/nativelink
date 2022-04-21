// Copyright 2020-2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use hex::FromHex;
use lazy_init::LazyTransform;
pub use log;
use prost::Message;
use proto::build::bazel::remote::execution::v2::Digest;
use serde::{Deserialize, Serialize};
use tokio::task::{JoinError, JoinHandle};

use error::{make_input_err, Error, ResultExt};

pub struct DigestInfo {
    /// Possibly the size of the digest in bytes. This should only be trusted
    /// if `truest_size` is true.
    pub size_bytes: i64,

    /// Raw hash in packed form.
    pub packed_hash: [u8; 32],

    /// Cached string representation of the `packed_hash`.
    str_hash: LazyTransform<Option<String>, String>,
}

impl DigestInfo {
    pub fn new(packed_hash: [u8; 32], size_bytes: i64) -> Self {
        DigestInfo {
            size_bytes,
            packed_hash,
            str_hash: LazyTransform::new(None),
        }
    }

    pub fn try_new<T>(hash: &str, size_bytes: T) -> Result<Self, Error>
    where
        T: TryInto<i64> + std::fmt::Display + Copy,
    {
        let packed_hash = <[u8; 32]>::from_hex(hash).err_tip(|| format!("Invalid sha256 hash: {}", hash))?;
        let size_bytes = size_bytes
            .try_into()
            .or_else(|_| Err(make_input_err!("Could not convert {} into i64", size_bytes)))?;
        Ok(DigestInfo {
            size_bytes: size_bytes,
            packed_hash: packed_hash,
            str_hash: LazyTransform::new(None),
        })
    }

    pub fn str<'a>(&'a self) -> &'a str {
        &self
            .str_hash
            .get_or_create(|v| v.unwrap_or_else(|| hex::encode(self.packed_hash)))
    }
}

impl fmt::Debug for DigestInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DigestInfo")
            .field("size_bytes", &self.size_bytes)
            .field("hash", &self.str())
            .finish()
    }
}

impl PartialEq for DigestInfo {
    fn eq(&self, other: &Self) -> bool {
        self.size_bytes == other.size_bytes && self.packed_hash == other.packed_hash
    }
}

impl Eq for DigestInfo {}

impl Hash for DigestInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.size_bytes.hash(state);
        self.packed_hash.hash(state);
    }
}

impl Clone for DigestInfo {
    fn clone(&self) -> Self {
        DigestInfo {
            size_bytes: self.size_bytes,
            packed_hash: self.packed_hash,
            str_hash: LazyTransform::new(None),
        }
    }
}

impl TryFrom<Digest> for DigestInfo {
    type Error = Error;

    fn try_from(digest: Digest) -> Result<Self, Self::Error> {
        let packed_hash =
            <[u8; 32]>::from_hex(&digest.hash).err_tip(|| format!("Invalid sha256 hash: {}", digest.hash))?;
        Ok(DigestInfo {
            size_bytes: digest.size_bytes,
            packed_hash: packed_hash,
            str_hash: LazyTransform::new(Some(digest.hash)),
        })
    }
}

impl Into<Digest> for DigestInfo {
    fn into(self) -> Digest {
        let packed_hash = self.packed_hash;
        let hash = self
            .str_hash
            .into_inner()
            .unwrap_or_else(|v| v.unwrap_or_else(|| hex::encode(packed_hash)));
        Digest {
            hash: hash,
            size_bytes: self.size_bytes,
        }
    }
}

impl Into<Digest> for &DigestInfo {
    fn into(self) -> Digest {
        Digest {
            hash: self.str().to_string(),
            size_bytes: self.size_bytes,
        }
    }
}

impl Into<DigestInfo> for SerializableDigestInfo {
    fn into(self) -> DigestInfo {
        DigestInfo::new(self.hash, self.size_bytes as i64)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Clone)]
#[repr(C)]
pub struct SerializableDigestInfo {
    pub hash: [u8; 32],
    pub size_bytes: u64,
}

/// Simple wrapper that will abort a future that is running in another spawn in the
/// event that this handle gets dropped.
pub struct JoinHandleDropGuard<T> {
    inner: JoinHandle<T>,
}

impl<T> JoinHandleDropGuard<T> {
    pub fn new(inner: JoinHandle<T>) -> Self {
        Self { inner }
    }
}

impl<T> Future for JoinHandleDropGuard<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx)
    }
}

impl<T> Drop for JoinHandleDropGuard<T> {
    fn drop(&mut self) {
        self.inner.abort();
    }
}

// Simple utility trait that makes it easier to apply `.try_map` to Vec.
// This will convert one vector into another vector with a different type.
pub trait VecExt<T> {
    fn try_map<F, U>(self, f: F) -> Result<Vec<U>, Error>
    where
        Self: Sized,
        F: (std::ops::Fn(T) -> Result<U, Error>) + Sized;
}

impl<T> VecExt<T> for Vec<T> {
    fn try_map<F, U>(self, f: F) -> Result<Vec<U>, Error>
    where
        Self: Sized,
        F: (std::ops::Fn(T) -> Result<U, Error>) + Sized,
    {
        let mut output = Vec::with_capacity(self.len());
        for item in self {
            output.push((f)(item)?);
        }
        Ok(output)
    }
}

// Simple utility trait that makes it easier to apply `.try_map` to HashMap.
// This will convert one HashMap into another keeping the key the same, but
// different value type.
pub trait HashMapExt<K: std::cmp::Eq + std::hash::Hash, T> {
    fn try_map<F, U>(self, f: F) -> Result<HashMap<K, U>, Error>
    where
        Self: Sized,
        F: (std::ops::Fn(T) -> Result<U, Error>) + Sized;
}

impl<K: std::cmp::Eq + std::hash::Hash, T> HashMapExt<K, T> for HashMap<K, T> {
    fn try_map<F, U>(self, f: F) -> Result<HashMap<K, U>, Error>
    where
        Self: Sized,
        F: (std::ops::Fn(T) -> Result<U, Error>) + Sized,
    {
        let mut output = HashMap::with_capacity(self.len());
        for (k, v) in self {
            output.insert(k, (f)(v)?);
        }
        Ok(output)
    }
}

// Utility to encode our proto into GRPC stream format.
pub fn encode_stream_proto<T: Message>(proto: &T) -> Result<Bytes, Box<dyn std::error::Error>> {
    let mut buf = BytesMut::new();
    // See below comment on spec.
    use std::mem::size_of;
    const PREFIX_BYTES: usize = size_of::<u8>() + size_of::<u32>();
    for _ in 0..PREFIX_BYTES {
        // Advance our buffer first.
        // We will backfill it once we know the size of the message.
        buf.put_u8(0);
    }
    proto.encode(&mut buf)?;
    let len = buf.len() - PREFIX_BYTES;
    {
        let mut buf = &mut buf[0..PREFIX_BYTES];
        // See: https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#:~:text=Compressed-Flag
        // for more details on spec.
        // Compressed-Flag -> 0 / 1 # encoded as 1 byte unsigned integer.
        buf.put_u8(0);
        // Message-Length -> {length of Message} # encoded as 4 byte unsigned integer (big endian).
        buf.put_u32(len as u32);
        // Message -> *{binary octet}.
    }

    Ok(buf.freeze())
}
