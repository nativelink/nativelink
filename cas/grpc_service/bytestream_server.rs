// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::convert::TryFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use async_fixed_buffer::AsyncFixedBuf;
use futures::{stream::unfold, Future, Stream};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tonic::{Request, Response, Status, Streaming};

use proto::google::bytestream::{
    byte_stream_server::ByteStream, byte_stream_server::ByteStreamServer as Server, QueryWriteStatusRequest,
    QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest, WriteResponse,
};

use common::{log, DigestInfo};
use config::cas_server::ByteStreamConfig;
use error::{error_if, make_err, make_input_err, Code, Error, ResultExt};
use store::{Store, StoreManager, UploadSizeInfo};

pub struct ByteStreamServer {
    stores: HashMap<String, Arc<dyn Store>>,
    // Buffer size for transferring data between grpc endpoint and store.
    write_buffer_stream_size: usize,
    // Buffer size for transferring data between store and grpc endpoint.
    read_buffer_stream_size: usize,
    // Max number of bytes to send on each grpc stream chunk.
    max_bytes_per_stream: usize,
}

struct ReaderState {
    max_bytes_per_stream: usize,
    maybe_stream_closer_fut: Option<Pin<Box<dyn Future<Output = ()> + Sync + Send>>>,
    rx: Box<dyn AsyncRead + Sync + Send + Unpin>,
    reading_future: tokio::task::JoinHandle<Result<(), Error>>,
}

impl ReaderState {
    /// Drops the state without running the stream_closer.
    /// This is useful for cases when the tx already sent the EOF.
    fn drop_without_closing_stream(mut self) {
        drop(self.maybe_stream_closer_fut.take());
    }
}

impl Drop for ReaderState {
    // Just in case this state gets unexpectedly dropped (which can happen if the
    // owned future is dropped) we notify the writer that it can shutdown.
    fn drop(&mut self) {
        // Close stream then wait for reader stream to finish.
        if let Some(stream_closer) = self.maybe_stream_closer_fut.take() {
            tokio::spawn(async move {
                // Ignoring errors because we don't have anywhere to publish them.
                stream_closer.await;
            });
        }
    }
}

type ReadStream = Pin<Box<dyn Stream<Item = Result<ReadResponse, Status>> + Send + Sync + 'static>>;

impl ByteStreamServer {
    pub fn new(config: &ByteStreamConfig, store_manager: &StoreManager) -> Result<Self, Error> {
        let mut stores = HashMap::with_capacity(config.cas_stores.len());
        for (instance_name, store_name) in &config.cas_stores {
            let store = store_manager
                .get_store(&store_name)
                .ok_or_else(|| make_input_err!("'cas_store': '{}' does not exist", store_name))?
                .clone();
            stores.insert(instance_name.to_string(), store);
        }
        Ok(ByteStreamServer {
            stores: stores,
            write_buffer_stream_size: config.write_buffer_stream_size,
            read_buffer_stream_size: config.read_buffer_stream_size,
            max_bytes_per_stream: config.max_bytes_per_stream,
        })
    }

    pub fn into_service(self) -> Server<ByteStreamServer> {
        Server::new(self)
    }

    async fn inner_read(&self, grpc_request: Request<ReadRequest>) -> Result<Response<ReadStream>, Error> {
        let read_request = grpc_request.into_inner();

        let read_limit =
            usize::try_from(read_request.read_limit).err_tip(|| "read_limit has is not convertible to usize")?;
        let resource_info = ResourceInfo::new(&read_request.resource_name)?;
        let digest = DigestInfo::try_new(&resource_info.hash, resource_info.expected_size)?;

        let mut raw_fixed_buffer = AsyncFixedBuf::new(vec![0u8; self.read_buffer_stream_size].into_boxed_slice());
        let maybe_stream_closer_fut = Some(raw_fixed_buffer.get_closer());
        let (rx, mut tx) = tokio::io::split(raw_fixed_buffer);
        let rx: Box<dyn tokio::io::AsyncRead + Sync + Send + Unpin> = if read_limit != 0 {
            Box::new(rx.take(u64::try_from(read_limit).err_tip(|| "read_limit has is not convertible to u64")?))
        } else {
            Box::new(rx)
        };

        let instance_name = resource_info.instance_name;
        let store_clone = self
            .stores
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{}'", instance_name))?
            .clone();

        let reading_future = tokio::spawn(async move {
            let store = Pin::new(store_clone.as_ref());
            let read_limit = if read_limit != 0 { Some(read_limit) } else { None };
            let p = store
                .get_part(digest, &mut tx, read_request.read_offset as usize, read_limit)
                .await
                .err_tip(|| "Error retrieving data from store");
            tx.shutdown()
                .await
                .err_tip(|| "Error shutting down tx stream in bytestrem server")?;
            p
        });

        // This allows us to call a destructor when the the object is dropped.
        let state = Some(ReaderState {
            maybe_stream_closer_fut,
            rx,
            max_bytes_per_stream: self.max_bytes_per_stream,
            reading_future,
        });

        Ok(Response::new(Box::pin(unfold(state, move |state| async {
            let mut state = state?; // If state is None, we have already sent error if needed (None is Done).
            let mut response = ReadResponse {
                data: Vec::with_capacity(state.max_bytes_per_stream),
            };
            unsafe { response.data.set_len(state.max_bytes_per_stream) };
            let read_result = state.rx.read(&mut response.data[..]).await;
            match read_result.err_tip(|| "Error reading data from underlying store") {
                Ok(sz) => {
                    if sz > state.max_bytes_per_stream {
                        let err = make_err!(Code::Internal, "Returned store size was larger than read size");
                        return Some((Err(err.into()), None));
                    }
                    unsafe { response.data.set_len(sz) };
                    // Receiving zero bytes is an EOF.
                    let new_state = if sz == 0 {
                        state.drop_without_closing_stream();
                        None
                    } else {
                        Some(state)
                    };
                    Some((Ok(response), new_state))
                }
                Err(mut e) => {
                    // We may need to propagate the error from reading the data through first.
                    // For example, the NotFound error will come through `reading_future`, and
                    // will not be present in `e`, but we need to ensure we pass NotFound error
                    // code or the client won't know why it failed.
                    if let Ok(Err(err)) = (&mut state.reading_future).await {
                        e = err.merge(e);
                    }
                    Some((Err(e.into()), None))
                }
            }
        }))))
    }

    async fn inner_write(&self, mut stream: WriteRequestStreamWrapper) -> Result<Response<WriteResponse>, Error> {
        let raw_buffer = vec![0u8; self.write_buffer_stream_size].into_boxed_slice();
        let (rx, mut tx) = tokio::io::split(AsyncFixedBuf::new(raw_buffer));

        let join_handle = {
            let instance_name = &stream.instance_name;
            let store_clone = self
                .stores
                .get(instance_name)
                .err_tip(|| format!("'instance_name' not configured for '{}'", instance_name))?
                .clone();
            let hash = stream.hash.clone();
            let expected_size = stream.expected_size;
            tokio::spawn(async move {
                let rx = Box::new(rx.take(expected_size as u64));
                let store = Pin::new(store_clone.as_ref());
                store
                    .update(
                        DigestInfo::try_new(&hash, expected_size)?,
                        rx,
                        UploadSizeInfo::ExactSize(expected_size),
                    )
                    .await
            })
        };

        while let Some(write_request) = stream.next().await.err_tip(|| "Stream closed early")? {
            tx.write_all(&write_request.data)
                .await
                .err_tip(|| "Error writing to store stream")?;
        }
        join_handle
            .await
            .err_tip(|| "Error joining promise")?
            .err_tip(|| "Error updating inner store")?;
        Ok(Response::new(WriteResponse {
            committed_size: stream.bytes_received as i64,
        }))
    }
}

struct ResourceInfo<'a> {
    instance_name: &'a str,
    // TODO(allada) Currently we do not support stream resuming, this is
    // the field we would need.
    _uuid: Option<&'a str>,
    hash: &'a str,
    expected_size: usize,
}

impl<'a> ResourceInfo<'a> {
    fn new(resource_name: &'a str) -> Result<ResourceInfo<'a>, Error> {
        let mut parts = resource_name.splitn(6, '/');
        const ERROR_MSG: &str = concat!(
            "Expected resource_name to be of pattern ",
            "'{instance_name}/uploads/{uuid}/blobs/{hash}/{size}' or ",
            "'{instance_name}/blobs/{hash}/{size}'",
        );
        let instance_name = &parts.next().err_tip(|| ERROR_MSG)?;
        let mut blobs_or_uploads: &str = parts.next().err_tip(|| ERROR_MSG)?;
        let mut uuid = None;
        if &blobs_or_uploads == &"uploads" {
            uuid = Some(parts.next().err_tip(|| ERROR_MSG)?);
            blobs_or_uploads = parts.next().err_tip(|| ERROR_MSG)?;
        }

        error_if!(
            &blobs_or_uploads != &"blobs",
            "Element 2 or 4 of resource_name should have been 'blobs'. Got: {}",
            blobs_or_uploads
        );
        let hash = &parts.next().err_tip(|| ERROR_MSG)?;
        let raw_digest_size = parts.next().err_tip(|| ERROR_MSG)?;
        let expected_size = raw_digest_size.parse::<usize>().err_tip(|| {
            format!(
                "Digest size_bytes was not convertible to usize. Got: {}",
                raw_digest_size
            )
        })?;
        Ok(ResourceInfo {
            instance_name: instance_name,
            _uuid: uuid,
            hash,
            expected_size,
        })
    }
}

#[derive(Debug)]
struct WriteRequestStreamWrapper {
    stream: Streaming<WriteRequest>,
    current_msg: WriteRequest,
    hash: String,
    instance_name: String,
    expected_size: usize,
    is_first: bool,
    bytes_received: usize,
}

impl WriteRequestStreamWrapper {
    async fn from(mut stream: Streaming<WriteRequest>) -> Result<WriteRequestStreamWrapper, Error> {
        let current_msg = stream
            .message()
            .await
            .err_tip(|| "Error receiving first message in stream")?
            .err_tip(|| "Expected WriteRequest struct in stream")?;

        let resource_info = ResourceInfo::new(&current_msg.resource_name)
            .err_tip(|| "Could not extract resource info from first message of stream")?;
        let instance_name = resource_info.instance_name.to_string();
        let hash = resource_info.hash.to_string();
        let expected_size = resource_info.expected_size;
        Ok(WriteRequestStreamWrapper {
            stream,
            current_msg,
            hash,
            instance_name,
            expected_size,
            is_first: true,
            bytes_received: 0,
        })
    }

    async fn next<'a>(&'a mut self) -> Result<Option<&'a WriteRequest>, Error> {
        if self.is_first {
            self.is_first = false;
            self.bytes_received += self.current_msg.data.len();
            return Ok(Some(&self.current_msg));
        }
        if self.current_msg.finish_write {
            error_if!(
                self.bytes_received != self.expected_size,
                "Did not send enough data. Expected {}, but so far received {}",
                self.expected_size,
                self.bytes_received
            );
            return Ok(None); // Previous message said it was the last msg.
        }
        error_if!(
            self.bytes_received > self.expected_size,
            "Sent too much data. Expected {}, but so far received {}",
            self.expected_size,
            self.bytes_received
        );
        self.current_msg = self
            .stream
            .message()
            .await
            .err_tip(|| format!("Stream error at byte {}", self.bytes_received))?
            .err_tip(|| "Expected WriteRequest struct in stream")?;
        self.bytes_received += self.current_msg.data.len();

        Ok(Some(&self.current_msg))
    }
}

#[tonic::async_trait]
impl ByteStream for ByteStreamServer {
    type ReadStream = ReadStream;
    async fn read(&self, grpc_request: Request<ReadRequest>) -> Result<Response<Self::ReadStream>, Status> {
        log::info!("\x1b[0;31mRead Req\x1b[0m: {:?}", grpc_request.get_ref());
        let now = Instant::now();
        let resp = self
            .inner_read(grpc_request)
            .await
            .err_tip(|| format!("Failed on read() command"))
            .map_err(|e| e.into());
        let d = now.elapsed().as_secs_f32();
        if let Err(err) = resp.as_ref() {
            log::error!("\x1b[0;31mRead Resp\x1b[0m: {} {:?}", d, err);
        } else {
            log::info!("\x1b[0;31mRead Resp\x1b[0m: {}", d);
        }
        resp
    }

    async fn write(&self, grpc_request: Request<Streaming<WriteRequest>>) -> Result<Response<WriteResponse>, Status> {
        let now = Instant::now();
        let stream = WriteRequestStreamWrapper::from(grpc_request.into_inner())
            .await
            .err_tip(|| "Could not unwrap first stream message")
            .map_err(|e| Into::<Status>::into(e))?;
        let hash = if log::log_enabled!(log::Level::Info) {
            Some(stream.hash.clone())
        } else {
            None
        };
        log::info!("\x1b[0;31mWrite Req\x1b[0m: {:?}", hash);
        let resp = self
            .inner_write(stream)
            .await
            .err_tip(|| format!("Failed on write() command"))
            .map_err(|e| e.into());
        let d = now.elapsed().as_secs_f32();
        if let Err(err) = resp.as_ref() {
            log::error!("\x1b[0;31mWrite Resp\x1b[0m: {} {:?} {:?}", d, hash, err);
        } else {
            log::info!("\x1b[0;31mWrite Resp\x1b[0m: {} {:?}", d, hash);
        }
        resp
    }

    async fn query_write_status(
        &self,
        _grpc_request: Request<QueryWriteStatusRequest>,
    ) -> Result<Response<QueryWriteStatusResponse>, Status> {
        log::error!("query_write_status {:?}", _grpc_request.get_ref());
        Err(Status::unimplemented(""))
    }
}
