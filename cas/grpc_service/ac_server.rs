// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::convert::TryInto;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use prost::Message;
use tonic::{Request, Response, Status};

use proto::build::bazel::remote::execution::v2::{
    action_cache_server::ActionCache, action_cache_server::ActionCacheServer as Server, ActionResult,
    GetActionResultRequest, UpdateActionResultRequest,
};

use common::{log, DigestInfo};
use config::cas_server::{AcStoreConfig, InstanceName};
use error::{make_input_err, Code, Error, ResultExt};
use store::{Store, StoreManager};

// NOTE(blaise.bruer) From some local testing it looks like action cache items are rarely greater than
// 1.2k. Giving a bit more just in case to reduce allocs.
const ESTIMATED_DIGEST_SIZE: usize = 2048;

pub struct AcServer {
    stores: HashMap<String, Arc<dyn Store>>,
}

impl AcServer {
    pub fn new(config: &HashMap<InstanceName, AcStoreConfig>, store_manager: &StoreManager) -> Result<Self, Error> {
        let mut stores = HashMap::with_capacity(config.len());
        for (instance_name, ac_cfg) in config {
            let store = store_manager
                .get_store(&ac_cfg.ac_store)
                .ok_or_else(|| make_input_err!("'ac_store': '{}' does not exist", ac_cfg.ac_store))?
                .clone();
            stores.insert(instance_name.to_string(), store);
        }
        Ok(AcServer { stores: stores.clone() })
    }

    pub fn into_service(self) -> Server<AcServer> {
        Server::new(self)
    }

    async fn inner_get_action_result(
        &self,
        grpc_request: Request<GetActionResultRequest>,
    ) -> Result<Response<ActionResult>, Error> {
        let get_action_request = grpc_request.into_inner();

        // TODO(blaise.bruer) We should write a test for these errors.
        let digest: DigestInfo = get_action_request
            .action_digest
            .err_tip(|| "Action digest was not set in message")?
            .try_into()?;

        let instance_name = get_action_request.instance_name;
        let store = Pin::new(
            self.stores
                .get(&instance_name)
                .err_tip(|| format!("'instance_name' not configured for '{}'", &instance_name))?
                .as_ref(),
        );
        let mut store_data_resp = store
            .get_part_unchunked(digest.clone(), 0, None, Some(ESTIMATED_DIGEST_SIZE))
            .await;
        if let Err(err) = &mut store_data_resp {
            if err.code == Code::NotFound {
                // Trim the error code. Not Found is quite common and we don't want to send a large
                // error (debug) message for something that is common. We resize to just the last
                // message as it will be the most relevant.
                err.messages.resize_with(1, || "".to_string());
            }
        }
        let store_data = store_data_resp?;

        let action_result = ActionResult::decode(store_data)
            .err_tip_with_code(|e| (Code::NotFound, format!("Stored value appears to be corrupt: {}", e)))?;

        Ok(Response::new(action_result))
    }

    async fn inner_update_action_result(
        &self,
        grpc_request: Request<UpdateActionResultRequest>,
    ) -> Result<Response<ActionResult>, Error> {
        let update_action_request = grpc_request.into_inner();

        // TODO(blaise.bruer) This needs to be fixed. It is using wrong macro.
        // We also should write a test for these errors.
        let digest: DigestInfo = update_action_request
            .action_digest
            .err_tip(|| "Action digest was not set in message")?
            .try_into()?;

        let action_result = update_action_request
            .action_result
            .err_tip(|| "Action result was not set in message")?;

        let mut store_data = BytesMut::with_capacity(ESTIMATED_DIGEST_SIZE);
        action_result
            .encode(&mut store_data)
            .err_tip(|| "Provided ActionResult could not be serialized")?;

        let instance_name = update_action_request.instance_name;
        let store = Pin::new(
            self.stores
                .get(&instance_name)
                .err_tip(|| format!("'instance_name' not configured for '{}'", &instance_name))?
                .as_ref(),
        );
        store
            .update_oneshot(digest, store_data.freeze())
            .await
            .err_tip(|| "Failed to update in action cache")?;
        Ok(Response::new(action_result))
    }
}

#[tonic::async_trait]
impl ActionCache for AcServer {
    async fn get_action_result(
        &self,
        grpc_request: Request<GetActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let now = Instant::now();
        log::info!("\x1b[0;31mget_action_result Req\x1b[0m: {:?}", grpc_request.get_ref());
        let hash = grpc_request
            .get_ref()
            .action_digest
            .as_ref()
            .map(|v| v.hash.to_string());
        let resp = self.inner_get_action_result(grpc_request).await;
        let d = now.elapsed().as_secs_f32();
        if resp.is_err() && resp.as_ref().err().unwrap().code != error::Code::NotFound {
            log::error!("\x1b[0;31mget_action_result Resp\x1b[0m: {} {:?} {:?}", d, hash, resp);
        } else {
            log::info!("\x1b[0;31mget_action_result Resp\x1b[0m: {} {:?} {:?}", d, hash, resp);
        }
        return resp.map_err(|e| e.into());
    }

    async fn update_action_result(
        &self,
        grpc_request: Request<UpdateActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        let now = Instant::now();
        log::info!(
            "\x1b[0;31mupdate_action_result Req\x1b[0m: {:?}",
            grpc_request.get_ref()
        );
        let resp = self.inner_update_action_result(grpc_request).await;
        let d = now.elapsed().as_secs_f32();
        if resp.is_err() {
            log::error!("\x1b[0;31mupdate_action_result Resp\x1b[0m: {} {:?}", d, resp);
        } else {
            log::info!("\x1b[0;31mupdate_action_result Resp\x1b[0m: {} {:?}", d, resp);
        }
        return resp.map_err(|e| e.into());
    }
}
