// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use fast_async_mutex::mutex::Mutex;
use futures::{stream::unfold, Stream};
use proto::google::bytestream::{
    byte_stream_server::ByteStream, byte_stream_server::ByteStreamServer as Server, QueryWriteStatusRequest,
    QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest, WriteResponse,
};
use tonic::{Request, Response, Status, Streaming};

use buf_channel::{make_buf_channel_pair, DropCloserReadHalf};
use common::{log, DigestInfo};
use config::cas_server::ByteStreamConfig;
use error::{make_err, make_input_err, Code, Error, ResultExt};
use grpc_store::GrpcStore;
use resource_info::ResourceInfo;
use store::{Store, StoreManager, UploadSizeInfo};
use write_request_stream_wrapper::WriteRequestStreamWrapper;

struct ReaderState {
    max_bytes_per_stream: usize,
    rx: DropCloserReadHalf,
    reading_future: tokio::task::JoinHandle<Result<(), Error>>,
}

type ReadStream = Pin<Box<dyn Stream<Item = Result<ReadResponse, Status>> + Send + 'static>>;

pub struct ByteStreamServer {
    stores: HashMap<String, Arc<dyn Store>>,
    // Max number of bytes to send on each grpc stream chunk.
    max_bytes_per_stream: usize,
    active_uploads: Mutex<HashSet<String>>,
}

impl ByteStreamServer {
    pub fn new(config: &ByteStreamConfig, store_manager: &StoreManager) -> Result<Self, Error> {
        let mut stores = HashMap::with_capacity(config.cas_stores.len());
        for (instance_name, store_name) in &config.cas_stores {
            let store = store_manager
                .get_store(&store_name)
                .ok_or_else(|| make_input_err!("'cas_store': '{}' does not exist", store_name))?;
            stores.insert(instance_name.to_string(), store);
        }
        Ok(ByteStreamServer {
            stores: stores,
            max_bytes_per_stream: config.max_bytes_per_stream,
            active_uploads: Mutex::new(HashSet::new()),
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
        let instance_name = resource_info.instance_name;
        let store_clone = self
            .stores
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{}'", instance_name))?
            .clone();

        // If we are a GrpcStore we shortcut here, as this is a special store.
        let any_store = store_clone.clone().as_any();
        let maybe_grpc_store = any_store.downcast_ref::<Arc<GrpcStore>>();
        if let Some(grpc_store) = maybe_grpc_store {
            let stream = grpc_store.read(Request::new(read_request)).await?.into_inner();
            return Ok(Response::new(Box::pin(stream)));
        }

        let digest = DigestInfo::try_new(&resource_info.hash, resource_info.expected_size)?;

        let (tx, rx) = buf_channel::make_buf_channel_pair();

        let reading_future = tokio::spawn(async move {
            let read_limit = if read_limit != 0 { Some(read_limit) } else { None };
            Pin::new(store_clone.as_ref())
                .get_part(digest, tx, read_request.read_offset as usize, read_limit)
                .await
                .err_tip(|| "Error retrieving data from store")
        });

        // This allows us to call a destructor when the the object is dropped.
        let state = Some(ReaderState {
            rx,
            max_bytes_per_stream: self.max_bytes_per_stream,
            reading_future,
        });

        Ok(Response::new(Box::pin(unfold(state, move |state| async {
            let mut state = if let Some(state) = state {
                state
            } else {
                return None; // Our stream is done.
            };

            let read_result = state
                .rx
                .take(state.max_bytes_per_stream)
                .await
                .err_tip(|| "Error reading data from underlying store");
            match read_result {
                Ok(bytes) => {
                    if bytes.len() == 0 {
                        // EOF.
                        return Some((Ok(ReadResponse { ..Default::default() }), None));
                    }
                    if bytes.len() > state.max_bytes_per_stream {
                        let err = make_err!(Code::Internal, "Returned store size was larger than read size");
                        return Some((Err(err.into()), None));
                    }
                    let response = ReadResponse { data: bytes };
                    log::debug!("\x1b[0;31mBytestream Read Chunk Resp\x1b[0m: {:?}", response);
                    Some((Ok(response), Some(state)))
                }
                Err(mut e) => {
                    // We may need to propagate the error from reading the data through first.
                    // For example, the NotFound error will come through `reading_future`, and
                    // will not be present in `e`, but we need to ensure we pass NotFound error
                    // code or the client won't know why it failed.
                    if let Ok(Err(err)) = state.reading_future.await {
                        e = err.merge(e);
                    }
                    if e.code == Code::NotFound {
                        // Trim the error code. Not Found is quite common and we don't want to send a large
                        // error (debug) message for something that is common. We resize to just the last
                        // message as it will be the most relevant.
                        e.messages.resize_with(1, || "".to_string());
                    }
                    log::debug!("\x1b[0;31mBytestream Read Chunk Resp\x1b[0m: Error {:?}", e);
                    Some((Err(e.into()), None))
                }
            }
        }))))
    }

    async fn inner_write(
        &self,
        mut stream: WriteRequestStreamWrapper<Streaming<WriteRequest>, Status>,
    ) -> Result<Response<WriteResponse>, Error> {
        let (mut tx, rx) = make_buf_channel_pair();

        let join_handle = {
            let instance_name = &stream.instance_name;
            let store_clone = self
                .stores
                .get(instance_name)
                .err_tip(|| format!("'instance_name' not configured for '{}'", instance_name))?
                .clone();

            // If we are a GrpcStore we shortcut here, as this is a special store.
            let any_store = store_clone.clone().as_any();
            let maybe_grpc_store = any_store.downcast_ref::<Arc<GrpcStore>>();
            if let Some(grpc_store) = maybe_grpc_store {
                return grpc_store.write(stream).await;
            }

            let hash = stream.hash.clone();
            let expected_size = stream.expected_size;
            tokio::spawn(async move {
                Pin::new(store_clone.as_ref())
                    .update(
                        DigestInfo::try_new(&hash, expected_size)?,
                        rx,
                        UploadSizeInfo::ExactSize(expected_size),
                    )
                    .await
            })
        };

        while let Some(write_request) = stream.next().await.err_tip(|| "Stream closed early")? {
            if write_request.data.len() == 0 {
                continue; // We don't want to send EOF, let the None option send it.
            }
            tx.send(write_request.data)
                .await
                .err_tip(|| "Error writing to store stream")?;
        }
        tx.send_eof()
            .await
            .err_tip(|| "Failed to send EOF in bytestream server")?;
        join_handle
            .await
            .err_tip(|| "Error joining promise")?
            .err_tip(|| "Error updating inner store")?;
        Ok(Response::new(WriteResponse {
            committed_size: stream.bytes_received as i64,
        }))
    }

    async fn inner_query_write_status(
        &self,
        query_request: &QueryWriteStatusRequest,
    ) -> Result<Response<QueryWriteStatusResponse>, Error> {
        let mut resource_info = ResourceInfo::new(&query_request.resource_name)?;

        let store_clone = self
            .stores
            .get(resource_info.instance_name)
            .err_tip(|| {
                format!(
                    "'instance_name' not configured for '{}'",
                    (&resource_info).instance_name
                )
            })?
            .clone();

        // If we are a GrpcStore we shortcut here, as this is a special store.
        let any_store = store_clone.clone().as_any();
        let maybe_grpc_store = any_store.downcast_ref::<Arc<GrpcStore>>();
        if let Some(grpc_store) = maybe_grpc_store {
            return grpc_store.query_write_status(Request::new(query_request.clone())).await;
        }

        let uuid = resource_info
            .uuid
            .take()
            .ok_or_else(|| make_input_err!("UUID must be set if querying write status"))?;

        {
            let active_uploads = self.active_uploads.lock().await;
            if active_uploads.contains(uuid) {
                return Ok(Response::new(QueryWriteStatusResponse {
                    // TODO(blaise.bruer) We currently don't support resuming a stream, so we always
                    // start from zero.
                    committed_size: 0,
                    complete: false,
                }));
            }
        }

        let digest = DigestInfo::try_new(resource_info.hash, resource_info.expected_size)?;
        let result = tokio::spawn(async move { Pin::new(store_clone.as_ref()).has(digest).await })
            .await
            .err_tip(|| "Failed to join spawn")?;

        if result.err_tip(|| "Failed to call .has() on store")?.is_none() {
            return Err(make_err!(Code::NotFound, "{}", "not found"));
        }
        Ok(Response::new(QueryWriteStatusResponse {
            committed_size: 0,
            complete: true,
        }))
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
        let mut stream = WriteRequestStreamWrapper::from(grpc_request.into_inner())
            .await
            .err_tip(|| "Could not unwrap first stream message")
            .map_err(|e| Into::<Status>::into(e))?;
        let hash = if log::log_enabled!(log::Level::Info) {
            Some(stream.hash.clone())
        } else {
            None
        };

        let uuid = stream
            .uuid
            .take()
            .ok_or_else(|| Into::<Status>::into(make_input_err!("UUID must be set if writing data")))?;
        {
            // Check to see if request is already being uploaded and if it is error, otherwise insert entry.
            let mut active_uploads = self.active_uploads.lock().await;
            if active_uploads.contains(&uuid) {
                return Err(Into::<Status>::into(make_input_err!(
                    "Cannot upload same UUID simultaneously"
                )));
            }
            active_uploads.insert(uuid.clone());
        }

        log::info!("\x1b[0;31mWrite Req\x1b[0m: {:?}", hash);
        let resp = self
            .inner_write(stream)
            .await
            .err_tip(|| "Failed on write() command")
            .map_err(|e| e.into());

        {
            // Remove the active upload request.
            let mut active_uploads = self.active_uploads.lock().await;
            active_uploads.remove(&uuid);
        }
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
        grpc_request: Request<QueryWriteStatusRequest>,
    ) -> Result<Response<QueryWriteStatusResponse>, Status> {
        let now = Instant::now();
        let query_request = grpc_request.into_inner();

        let resp = self
            .inner_query_write_status(&query_request)
            .await
            .err_tip(|| "Failed on query_write_status() command")
            .map_err(|e| e.into());

        let d = now.elapsed().as_secs_f32();
        if resp.is_err() {
            log::error!("\x1b[0;31mQuery Req\x1b[0m: {:?}", query_request);
            log::error!("\x1b[0;31mQuery Resp\x1b[0m: {} {:?}", d, resp);
        } else {
            log::info!("\x1b[0;31mQuery Req\x1b[0m: {:?}", query_request);
            log::info!("\x1b[0;31mQuery Resp\x1b[0m: {} {:?}", d, resp);
        }
        resp
    }
}
