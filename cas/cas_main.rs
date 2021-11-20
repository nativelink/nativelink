// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;

use futures::future::{select_all, BoxFuture};
use json5;
use runfiles::Runfiles;
use tonic::transport::Server;

use ac_server::AcServer;
use bytestream_server::ByteStreamServer;
use capabilities_server::CapabilitiesServer;
use cas_server::CasServer;
use config::cas_server::CasConfig;
use error::ResultExt;
use execution_server::ExecutionServer;
use store::StoreManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("error"))
        .format_timestamp_millis()
        .init();

    let r = Runfiles::create().err_tip(|| "Failed to create runfiles lookup object")?;
    let contents = String::from_utf8(tokio::fs::read(r.rlocation("rust_cas/config/examples/basic_cas.json")).await?)?;

    let cfg: CasConfig = json5::from_str(&contents)?;

    let mut store_manager = StoreManager::new();
    for (name, store_cfg) in cfg.stores {
        store_manager
            .make_store(&name, &store_cfg)
            .await
            .err_tip(|| format!("Failed to create store '{}'", name))?;
    }

    let mut servers: Vec<BoxFuture<Result<(), tonic::transport::Error>>> = Vec::new();
    for server_cfg in cfg.servers {
        let mut server = Server::builder();
        let services = server_cfg.services.ok_or_else(|| "'services' must be configured")?;

        let capabilities_config = services.capabilities.unwrap_or(HashMap::new());

        let server = server
            .add_optional_service(
                services
                    .ac
                    .map_or(Ok(None), |cfg| {
                        AcServer::new(&cfg, &store_manager).and_then(|v| Ok(Some(v.into_service())))
                    })
                    .err_tip(|| "Could not create AC service")?,
            )
            .add_optional_service(
                services
                    .cas
                    .map_or(Ok(None), |cfg| {
                        CasServer::new(&cfg, &store_manager).and_then(|v| Ok(Some(v.into_service())))
                    })
                    .err_tip(|| "Could not create CAS service")?,
            )
            .add_optional_service(
                services
                    .execution
                    .map_or(Ok(None), |cfg| {
                        ExecutionServer::new(&cfg, &capabilities_config, &store_manager)
                            .and_then(|v| Ok(Some(v.into_service())))
                    })
                    .err_tip(|| "Could not create Execution service")?,
            )
            .add_optional_service(
                services
                    .bytestream
                    .map_or(Ok(None), |cfg| {
                        ByteStreamServer::new(&cfg, &store_manager).and_then(|v| Ok(Some(v.into_service())))
                    })
                    .err_tip(|| "Could not create ByteStream service")?,
            )
            .add_optional_service(
                CapabilitiesServer::new(&capabilities_config).and_then(|v| Ok(Some(v.into_service())))?,
            );

        let addr = server_cfg.listen_address.parse()?;
        servers.push(Box::pin(server.serve(addr)));
    }

    if let Err(e) = select_all(servers).await.0 {
        panic!("{:?}", e);
    }
    panic!("No servers should ever resolve their future");
}
