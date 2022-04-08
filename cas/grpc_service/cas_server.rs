// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::{stream::Stream, FutureExt, StreamExt};
use proto::build::bazel::remote::execution::v2::{
    batch_read_blobs_response, batch_update_blobs_response,
    content_addressable_storage_server::ContentAddressableStorage,
    content_addressable_storage_server::ContentAddressableStorageServer as Server, BatchReadBlobsRequest,
    BatchReadBlobsResponse, BatchUpdateBlobsRequest, BatchUpdateBlobsResponse, FindMissingBlobsRequest,
    FindMissingBlobsResponse, GetTreeRequest, GetTreeResponse,
};
use proto::google::rpc::Status as GrpcStatus;
use tonic::{Request, Response, Status};

use common::{log, DigestInfo};
use config::cas_server::{CasStoreConfig, InstanceName};
use error::{error_if, make_input_err, Code, Error, ResultExt};
use store::{Store, StoreManager};

pub struct CasServer {
    stores: HashMap<String, Arc<dyn Store>>,
}

impl CasServer {
    pub fn new(config: &HashMap<InstanceName, CasStoreConfig>, store_manager: &StoreManager) -> Result<Self, Error> {
        let mut stores = HashMap::with_capacity(config.len());
        for (instance_name, cas_cfg) in config {
            let store = store_manager
                .get_store(&cas_cfg.cas_store)
                .ok_or_else(|| make_input_err!("'cas_store': '{}' does not exist", cas_cfg.cas_store))?;
            stores.insert(instance_name.to_string(), store);
        }
        Ok(CasServer { stores: stores })
    }

    pub fn into_service(self) -> Server<CasServer> {
        Server::new(self)
    }

    async fn inner_find_missing_blobs(
        &self,
        grpc_request: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Error> {
        let mut futures = futures::stream::FuturesOrdered::new();
        let inner_request = grpc_request.into_inner();
        let instance_name = inner_request.instance_name;
        for digest in inner_request.blob_digests.into_iter() {
            let digest: DigestInfo = digest.try_into()?;
            let store_owned = self
                .stores
                .get(&instance_name)
                .err_tip(|| format!("'instance_name' not configured for '{}'", &instance_name))?
                .clone();
            futures.push(tokio::spawn(async move {
                let store = Pin::new(store_owned.as_ref());
                store.has(digest.clone()).await.map_or_else(
                    |e| {
                        log::error!(
                            "Error during .has() call in .find_missing_blobs() : {:?} - {}",
                            e,
                            digest.str()
                        );
                        Some(digest.clone())
                    },
                    |maybe_sz| if maybe_sz.is_some() { None } else { Some(digest.clone()) },
                )
            }));
        }
        let mut responses = Vec::with_capacity(futures.len());
        while let Some(result) = futures.next().await {
            let val = result.err_tip(|| "Internal error joining future")?;
            if let Some(digest) = val {
                responses.push(digest.into());
            }
        }
        Ok(Response::new(FindMissingBlobsResponse {
            missing_blob_digests: responses,
        }))
    }

    async fn inner_batch_update_blobs(
        &self,
        grpc_request: Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Error> {
        let mut futures = futures::stream::FuturesOrdered::new();
        let inner_request = grpc_request.into_inner();
        let instance_name = inner_request.instance_name;
        for request in inner_request.requests {
            let digest: DigestInfo = request.digest.err_tip(|| "Digest not found in request")?.try_into()?;
            let digest_copy = digest.clone();
            let store_owned = self
                .stores
                .get(&instance_name)
                .err_tip(|| format!("'instance_name' not configured for '{}'", &instance_name))?
                .clone();
            let request_data = request.data;
            futures.push(tokio::spawn(
                async move {
                    let size_bytes = usize::try_from(digest_copy.size_bytes)
                        .err_tip(|| "Digest size_bytes was not convertible to usize")?;
                    error_if!(
                        size_bytes != request_data.len(),
                        "Digest for upload had mismatching sizes, digest said {} data  said {}",
                        size_bytes,
                        request_data.len()
                    );
                    Pin::new(store_owned.as_ref())
                        .update_oneshot(digest_copy, request_data)
                        .await
                        .err_tip(|| "Error writing to store")
                }
                .map(|result| batch_update_blobs_response::Response {
                    digest: Some(digest.into()),
                    status: Some(result.map_or_else(|e| e.into(), |_| GrpcStatus::default())),
                }),
            ));
        }
        let mut responses = Vec::with_capacity(futures.len());
        while let Some(result) = futures.next().await {
            responses.push(result.err_tip(|| "Internal error joining future")?);
        }
        Ok(Response::new(BatchUpdateBlobsResponse { responses: responses }))
    }

    async fn inner_batch_read_blobs(
        &self,
        grpc_request: Request<BatchReadBlobsRequest>,
    ) -> Result<Response<BatchReadBlobsResponse>, Error> {
        let mut futures = futures::stream::FuturesOrdered::new();
        let inner_request = grpc_request.into_inner();
        let instance_name = inner_request.instance_name;
        for digest in inner_request.digests {
            let digest: DigestInfo = digest.try_into()?;
            let digest_copy = digest.clone();
            let store_owned = self
                .stores
                .get(&instance_name)
                .err_tip(|| format!("'instance_name' not configured for '{}'", &instance_name))?
                .clone();

            futures.push(tokio::spawn(
                async move {
                    let size_bytes = usize::try_from(digest_copy.size_bytes)
                        .err_tip(|| "Digest size_bytes was not convertible to usize")?;
                    // TODO(allada) There is a security risk here of someone taking all the memory on the instance.
                    let store_data = Pin::new(store_owned.as_ref())
                        .get_part_unchunked(digest_copy, 0, None, Some(size_bytes))
                        .await
                        .err_tip(|| "Error reading from store")?;
                    Ok(store_data)
                }
                .map(|result: Result<Bytes, Error>| {
                    let (status, data) = result.map_or_else(
                        |mut e| {
                            if e.code == Code::NotFound {
                                // Trim the error code. Not Found is quite common and we don't want to send a large
                                // error (debug) message for something that is common. We resize to just the last
                                // message as it will be the most relevant.
                                e.messages.resize_with(1, || "".to_string());
                            }
                            (e.into(), Bytes::new())
                        },
                        |v| (GrpcStatus::default(), v),
                    );
                    batch_read_blobs_response::Response {
                        status: Some(status),
                        digest: Some(digest.into()),
                        data,
                    }
                }),
            ));
        }

        let mut responses = Vec::with_capacity(futures.len());
        while let Some(result) = futures.next().await {
            responses.push(result.err_tip(|| "Internal error joining future")?);
        }
        Ok(Response::new(BatchReadBlobsResponse { responses: responses }))
    }
}

#[tonic::async_trait]
impl ContentAddressableStorage for CasServer {
    async fn find_missing_blobs(
        &self,
        grpc_request: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Status> {
        log::info!("\x1b[0;31mfind_missing_blobs Req\x1b[0m: {:?}", grpc_request.get_ref());
        let now = Instant::now();
        let resp = self
            .inner_find_missing_blobs(grpc_request)
            .await
            .err_tip(|| format!("Failed on find_missing_blobs() command"))
            .map_err(|e| e.into());
        let d = now.elapsed().as_secs_f32();
        if resp.is_err() {
            log::error!("\x1b[0;31mfind_missing_blobs Resp\x1b[0m: {} {:?}", d, resp);
        } else {
            log::info!("\x1b[0;31mfind_missing_blobs Resp\x1b[0m: {} {:?}", d, resp);
        }
        resp
    }

    async fn batch_update_blobs(
        &self,
        grpc_request: Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        log::info!("\x1b[0;31mbatch_update_blobs Req\x1b[0m: {:?}", grpc_request.get_ref());
        let now = Instant::now();
        let resp = self
            .inner_batch_update_blobs(grpc_request)
            .await
            .err_tip(|| format!("Failed on batch_update_blobs() command"))
            .map_err(|e| e.into());
        let d = now.elapsed().as_secs_f32();
        if resp.is_err() {
            log::error!("\x1b[0;31mbatch_update_blobs Resp\x1b[0m: {} {:?}", d, resp);
        } else {
            log::info!("\x1b[0;31mbatch_update_blobs Resp\x1b[0m: {} {:?}", d, resp);
        }
        resp
    }

    async fn batch_read_blobs(
        &self,
        grpc_request: Request<BatchReadBlobsRequest>,
    ) -> Result<Response<BatchReadBlobsResponse>, Status> {
        log::info!("\x1b[0;31mbatch_read_blobs Req\x1b[0m: {:?}", grpc_request.get_ref());
        let now = Instant::now();
        let resp = self
            .inner_batch_read_blobs(grpc_request)
            .await
            .err_tip(|| format!("Failed on batch_read_blobs() command"))
            .map_err(|e| e.into());
        let d = now.elapsed().as_secs_f32();
        if resp.is_err() {
            log::error!("\x1b[0;31mbatch_read_blobs Resp\x1b[0m: {} {:?}", d, resp);
        } else {
            log::info!("\x1b[0;31mbatch_read_blobs Resp\x1b[0m: {} {:?}", d, resp);
        }
        resp
    }

    type GetTreeStream = Pin<Box<dyn Stream<Item = Result<GetTreeResponse, Status>> + Send + Sync + 'static>>;
    async fn get_tree(&self, _request: Request<GetTreeRequest>) -> Result<Response<Self::GetTreeStream>, Status> {
        use stdext::function_name;
        let output = format!("{} not yet implemented", function_name!());
        log::error!("\x1b[0;31mget_tree\x1b[0m: {:?}", output);
        Err(Status::unimplemented(output))
    }
}
