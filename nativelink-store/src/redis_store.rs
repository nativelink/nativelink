// Copyright 2024 The NativeLink Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;
use std::cell::OnceCell;
use std::pin::Pin;
use std::sync::Arc;

use arc_cell::ArcCell;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt, Shared};
use futures::stream::{unfold, FuturesUnordered, TryStreamExt};
use nativelink_config::stores::Retry;
use nativelink_error::{error_if, make_err, Code, Error, ResultExt};
use nativelink_util::background_spawn;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthRegistryBuilder, HealthStatus, HealthStatusIndicator};
use nativelink_util::metrics_utils::{Collector, CollectorState, MetricsComponent, Registry};
use nativelink_util::retry::{Retrier, RetryResult};
use nativelink_util::store_trait::{Store, UploadSizeInfo};
use redis::aio::{ConnectionLike, ConnectionManager};
use redis::AsyncCommands;
use tokio::time::{sleep, Duration};

use crate::cas_utils::is_zero_digest;

// Default max buffer size for retrying upload requests.
// Note: If you change this, adjust the docs in the config.
const DEFAULT_MAX_RETRY_BUFFER_PER_REQUEST: usize = 1 * 1024 * 1024; // 1MB.

const READ_CHUNK_SIZE: isize = 64 * 1024; // 64KB.

fn digest_to_key(digest: &DigestInfo) -> String {
    format!("{}-{}", digest.hash_str(), digest.size_bytes)
}

/// Holds a connection result or a future that resolves to a connection.
/// This is a utility to allow us to start a connection but not block on it.
pub enum LazyConnection<T: ConnectionLike + Unpin + Clone + Send + Sync> {
    Connection(Result<T, Error>),
    Future(Shared<BoxFuture<'static, Result<T, Error>>>),
}

pub struct RedisStore<T: ConnectionLike + Unpin + Clone + Send + Sync = ConnectionManager> {
    lazy_conn: ArcCell<LazyConnection<T>>,
    retrier: Retrier,
    max_retry_buffer_per_request: usize,
    temp_name_generator_fn: fn() -> String,
}

impl RedisStore {
    pub fn new(
        config: &nativelink_config::stores::RedisStore,
    ) -> Result<RedisStore<ConnectionManager>, Error> {
        // Note: Currently only one connection is supported.
        error_if!(
            config.addresses.len() != 1,
            "Only one address is supported for Redis store"
        );

        let address = config.addresses[0].clone();
        let conn_fut = async move {
            redis::Client::open(address)
                .map_err(from_redis_err)?
                .get_connection_manager()
                .await
                .map_err(from_redis_err)
        }
        .boxed()
        .shared();

        let conn_fut_clone = conn_fut.clone();
        // Start connecting to redis, but don't block our construction on it.
        background_spawn!("redis_initial_connection", async move {
            if let Err(e) = conn_fut_clone.await {
                make_err!(Code::Unavailable, "Failed to connect to Redis: {:?}", e);
            }
        });

        let lazy_conn = LazyConnection::Future(conn_fut);

        Ok(RedisStore::new_with_conn_and_name_generator(
            lazy_conn,
            config.retry.clone(),
            config.max_retry_buffer_per_request.clone(),
            || uuid::Uuid::new_v4().to_string(),
        ))
    }
}

impl<T: ConnectionLike + Unpin + Clone + Send + Sync> RedisStore<T> {
    pub fn new_with_conn_and_name_generator(
        lazy_conn: LazyConnection<T>,
        retry: Retry,
        max_retry_buffer_per_request: Option<usize>,
        temp_name_generator_fn: fn() -> String,
    ) -> RedisStore<T> {
        RedisStore {
            lazy_conn: ArcCell::new(Arc::new(lazy_conn)),
            retrier: Retrier::new(
                Arc::new(|duration| Box::pin(sleep(duration))),
                Arc::new(|duration: Duration| duration),
                retry,
            ),
            max_retry_buffer_per_request: max_retry_buffer_per_request
                .unwrap_or(DEFAULT_MAX_RETRY_BUFFER_PER_REQUEST),
            temp_name_generator_fn,
        }
    }

    async fn get_conn(&self) -> Result<T, Error> {
        let result = match self.lazy_conn.get().as_ref() {
            LazyConnection::Connection(conn_result) => return conn_result.clone(),
            LazyConnection::Future(fut) => fut.clone().await,
        };
        self.lazy_conn
            .set(Arc::new(LazyConnection::Connection(result.clone())));
        result
    }

    async fn has(self: Pin<&Self>, digest: &DigestInfo) -> Result<Option<usize>, Error> {
        self.retrier
            .retry(unfold((), move |state| async move {
                let mut conn = match self.get_conn().await {
                    Ok(conn) => conn,
                    Err(e) => return Some((RetryResult::Retry(e), state)),
                };
                let result: Result<usize, Error> = conn
                    .strlen(digest_to_key(digest))
                    .await
                    .map_err(from_redis_err);

                match result {
                    Ok(length) => {
                        return Some((RetryResult::Ok(Some(length as usize)), state));
                    }
                    Err(e) => {
                        return Some((RetryResult::Retry(e), state));
                    }
                }
            }))
            .await
    }
}

#[async_trait]
impl<T: ConnectionLike + Unpin + Clone + Send + Sync + 'static> Store for RedisStore<T> {
    async fn has_with_results(
        self: Pin<&Self>,
        digests: &[DigestInfo],
        results: &mut [Option<usize>],
    ) -> Result<(), Error> {
        digests
            .iter()
            .zip(results.iter_mut())
            .map(|(digest, result)| async move {
                // We need to do a special pass to ensure our zero digest exist.
                if is_zero_digest(digest) {
                    *result = Some(0);
                    return Ok::<_, Error>(());
                }
                *result = self.has(digest).await?;
                Ok::<_, Error>(())
            })
            .collect::<FuturesUnordered<_>>()
            .try_collect()
            .await?;
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        digest: DigestInfo,
        mut reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<(), Error> {
        reader.set_max_recent_data_size(self.max_retry_buffer_per_request);
        self.retrier
            .retry(unfold(reader, move |mut reader| async move {
                let temp_key = OnceCell::new();
                let make_temp_name = || format!("temp-{}", (self.temp_name_generator_fn)());
                let mut conn = match self.get_conn().await {
                    Ok(conn) => conn,
                    Err(e) => return Some((RetryResult::Retry(e), reader)),
                };

                let mut pipe = redis::pipe();
                pipe.atomic();

                'outer: loop {
                    let mut force_recv = true;

                    while force_recv || !reader.is_empty() {
                        let chunk = match reader
                            .recv()
                            .await
                            .err_tip(|| "Failed to reach chunk in update in redis store")
                        {
                            Ok(chunk) => chunk,
                            Err(e) => return Some((RetryResult::Retry(e), reader)),
                        };

                        if chunk.is_empty() {
                            if is_zero_digest(&digest) {
                                return Some((RetryResult::Ok(()), reader));
                            }
                            if force_recv {
                                conn.append::<std::string::String, &[u8], ()>(
                                    digest_to_key(&digest),
                                    &chunk[..],
                                )
                                .await
                                .map_or_else(
                                    |e| {
                                        RetryResult::Retry(make_err!(
                                            Code::Aborted,
                                            "RedisError in update: {e:?}"
                                        ))
                                    },
                                    |_| RetryResult::Ok(()),
                                );
                            }
                            break 'outer;
                        }

                        pipe.cmd("APPEND")
                            .arg(temp_key.get_or_init(make_temp_name))
                            .arg(&chunk[..]);
                        force_recv = false;

                        // Give other tasks a chance to run to populate the reader's
                        // buffer if possible.
                        tokio::task::yield_now().await;
                    }

                    pipe.query_async::<_, ()>(&mut conn)
                        .await
                        .map_err(from_redis_err)
                        .err_tip(|| "In RedisStore::update::query_async")
                        .map_or_else(
                            |e| {
                                RetryResult::Retry(make_err!(
                                    Code::Aborted,
                                    "RedisError in update: {e:?}"
                                ))
                            },
                            |_| RetryResult::Ok(()),
                        );
                    pipe.clear();
                }

                pipe.cmd("RENAME")
                    .arg(temp_key.get_or_init(make_temp_name))
                    .arg(digest_to_key(&digest));
                match pipe
                    .query_async::<_, ()>(&mut conn)
                    .await
                    .map_err(from_redis_err)
                    .err_tip(|| "In RedisStore::update")
                {
                    Ok(_) => (),
                    Err(e) => return Some((RetryResult::Retry(e), reader)),
                };

                Some((RetryResult::Ok(()), reader))
            }))
            .await
    }

    async fn get_part_ref(
        self: Pin<&Self>,
        digest: DigestInfo,
        writer: &mut DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> Result<(), Error> {
        // To follow RBE spec we need to consider any digest's with
        // zero size to be existing.
        if is_zero_digest(&digest) {
            writer
                .send_eof()
                .err_tip(|| "Failed to send zero EOF in redis store get_part_ref")?;
            return Ok(());
        }

        self.retrier
            .retry(unfold(writer, move |writer| async move {
                let mut conn = match self.get_conn().await {
                    Ok(conn) => conn,
                    Err(e) => return Some((RetryResult::Retry(e), writer)),
                };
                if length == Some(0) {
                    let exists = match conn
                        .exists::<_, bool>(digest_to_key(&digest))
                        .await
                        .map_err(from_redis_err)
                        .err_tip(|| "In RedisStore::get_part_ref::zero_exists")
                    {
                        Ok(exists) => exists,
                        Err(e) => return Some((RetryResult::Retry(e), writer)),
                    };

                    if !exists {
                        return Err(make_err!(
                            Code::NotFound,
                            "Data not found in Redis store for digest: {}",
                            digest_to_key(&digest)
                        ))
                        .ok()?;
                    }
                    match writer
                        .send_eof()
                        .err_tip(|| "Failed to write EOF in redis store get_part_ref")
                    {
                        Ok(_) => (),
                        Err(e) => return Some((RetryResult::Retry(e), writer)),
                    };
                    return Some((RetryResult::Ok(()), writer));
                }

                let mut current_start = match isize::try_from(offset)
                    .err_tip(|| "Cannot convert offset to isize in RedisStore::get_part_ref()")
                {
                    Ok(current_start) => current_start,
                    Err(e) => return Some((RetryResult::Retry(e), writer)),
                };
                let max_length = match isize::try_from(length.unwrap_or(isize::MAX as usize))
                    .err_tip(|| "Cannot convert length to isize in RedisStore::get_part_ref()")
                {
                    Ok(max_length) => max_length,
                    Err(e) => return Some((RetryResult::Retry(e), writer)),
                };
                let end_position = current_start.saturating_add(max_length);

                loop {
                    // Note: Redis getrange is inclusive, so we need to subtract 1 from the end.
                    let current_end =
                        std::cmp::min(current_start.saturating_add(READ_CHUNK_SIZE), end_position)
                            - 1;
                    let chunk = match conn
                        .getrange::<_, Bytes>(digest_to_key(&digest), current_start, current_end)
                        .await
                        .map_err(from_redis_err)
                        .err_tip(|| "In RedisStore::get_part_ref::getrange")
                    {
                        Ok(chunk) => chunk,
                        Err(e) => return Some((RetryResult::Retry(e), writer)),
                    };

                    if chunk.is_empty() {
                        match writer
                            .send_eof()
                            .err_tip(|| "Failed to write EOF in redis store get_part")
                        {
                            Ok(_) => (),
                            Err(e) => return Some((RetryResult::Retry(e), writer)),
                        };

                        break;
                    }

                    // Note: Redis getrange is inclusive, so we need to add 1 to the end.
                    let was_partial_data = chunk.len() as isize != current_end - current_start + 1;
                    current_start += chunk.len() as isize;
                    match writer
                        .send(chunk)
                        .await
                        .err_tip(|| "Failed to write data in Redis store")
                    {
                        Ok(_) => (),
                        Err(e) => return Some((RetryResult::Retry(e), writer)),
                    };

                    // If we got partial data or the exact requested number of bytes, we are done.
                    if writer.get_bytes_written() as isize == max_length || was_partial_data {
                        match writer
                            .send_eof()
                            .err_tip(|| "Failed to write EOF in redis store get_part")
                        {
                            Ok(_) => (),
                            Err(e) => return Some((RetryResult::Retry(e), writer)),
                        };

                        break;
                    }

                    if writer.get_bytes_written() as isize > max_length {
                        return Err(make_err!(
                            Code::OutOfRange,
                            "Data received exceeds requested length for digest: {}",
                            digest_to_key(&digest)
                        ))
                        .ok()?;
                    }
                }

                Some((RetryResult::Ok(()), writer))
            }))
            .await
    }

    fn inner_store(&self, _digest: Option<DigestInfo>) -> &'_ dyn Store {
        self
    }

    fn inner_store_arc(self: Arc<Self>, _digest: Option<DigestInfo>) -> Arc<dyn Store> {
        self
    }

    fn as_any(&self) -> &(dyn std::any::Any + Sync + Send) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Sync + Send> {
        self
    }

    fn register_metrics(self: Arc<Self>, registry: &mut Registry) {
        registry.register_collector(Box::new(Collector::new(&self)));
    }

    fn register_health(self: Arc<Self>, registry: &mut HealthRegistryBuilder) {
        registry.register_indicator(self);
    }
}

impl<T: ConnectionLike + Unpin + Clone + Send + Sync + 'static> MetricsComponent for RedisStore<T> {
    fn gather_metrics(&self, _c: &mut CollectorState) {}
}

#[async_trait]
impl<T: ConnectionLike + ConnectionLike + Unpin + Clone + Send + Sync + 'static>
    HealthStatusIndicator for RedisStore<T>
{
    fn get_name(&self) -> &'static str {
        "RedisStore"
    }

    async fn check_health(&self, namespace: Cow<'static, str>) -> HealthStatus {
        Store::check_health(Pin::new(self), namespace).await
    }
}

fn from_redis_err(call_res: redis::RedisError) -> Error {
    make_err!(Code::Internal, "Redis Error: {call_res}")
}
