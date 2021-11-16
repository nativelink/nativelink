// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::convert::TryFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use futures::{stream::unfold, Stream};
use proto::google::bytestream::{
    byte_stream_server::ByteStream, byte_stream_server::ByteStreamServer as Server, QueryWriteStatusRequest,
    QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest, WriteResponse,
};
use tonic::{Request, Response, Status, Streaming};

use buf_channel::{make_buf_channel_pair, DropCloserReadHalf};
use common::{log, DigestInfo};
use config::cas_server::ByteStreamConfig;
use error::{error_if, make_err, make_input_err, Code, Error, ResultExt};
use store::{Store, StoreManager, UploadSizeInfo};

struct ReaderState {
    max_bytes_per_stream: usize,
    rx: DropCloserReadHalf,
    reading_future: tokio::task::JoinHandle<Result<(), Error>>,
}

type ReadStream = Pin<Box<dyn Stream<Item = Result<ReadResponse, Status>> + Send + Sync + 'static>>;

pub struct ByteStreamServer {
    stores: HashMap<String, Arc<dyn Store>>,
    // Max number of bytes to send on each grpc stream chunk.
    max_bytes_per_stream: usize,
}

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

        let (tx, rx) = buf_channel::make_buf_channel_pair();

        let instance_name = resource_info.instance_name;
        let store_clone = self
            .stores
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{}'", instance_name))?
            .clone();

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
                    Some((Err(e.into()), None))
                }
            }
        }))))
    }

    async fn inner_write(&self, mut stream: WriteRequestStreamWrapper) -> Result<Response<WriteResponse>, Error> {
        let (mut tx, rx) = make_buf_channel_pair();

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
                // let rx = Box::new(rx.take(expected_size as u64));
                // let store = Pin::new(store_clone.as_ref());
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
    first_msg: Option<WriteRequest>,
    hash: String,
    instance_name: String,
    expected_size: usize,
    write_finished: bool,
    bytes_received: usize,
}

impl WriteRequestStreamWrapper {
    async fn from(mut stream: Streaming<WriteRequest>) -> Result<WriteRequestStreamWrapper, Error> {
        let first_msg = stream
            .message()
            .await
            .err_tip(|| "Error receiving first message in stream")?
            .err_tip(|| "Expected WriteRequest struct in stream")?;

        let resource_info = ResourceInfo::new(&first_msg.resource_name)
            .err_tip(|| "Could not extract resource info from first message of stream")?;
        let instance_name = resource_info.instance_name.to_string();
        let hash = resource_info.hash.to_string();
        let expected_size = resource_info.expected_size;
        let write_finished = first_msg.finish_write;

        Ok(WriteRequestStreamWrapper {
            stream,
            first_msg: Some(first_msg),
            hash,
            instance_name,
            expected_size,
            write_finished,
            bytes_received: 0,
        })
    }

    async fn next(&mut self) -> Result<Option<WriteRequest>, Error> {
        if let Some(first_msg) = self.first_msg.take() {
            self.bytes_received += first_msg.data.len();
            return Ok(Some(first_msg));
        }
        if self.write_finished {
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
        let next_msg = self
            .stream
            .message()
            .await
            .err_tip(|| format!("Stream error at byte {}", self.bytes_received))?
            .err_tip(|| "Expected WriteRequest struct in stream")?;
        self.write_finished = next_msg.finish_write;
        self.bytes_received += next_msg.data.len();

        Ok(Some(next_msg))
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
