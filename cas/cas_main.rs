// Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::sync::Arc;

use clap::Parser;
use futures::future::{select_all, BoxFuture};
use json5;
use runfiles::Runfiles;
use tonic::transport::Server;

use ac_server::AcServer;
use bytestream_server::ByteStreamServer;
use capabilities_server::CapabilitiesServer;
use cas_server::CasServer;
use config::cas_server::CasConfig;
use default_store_factory::store_factory;
use error::ResultExt;
use execution_server::ExecutionServer;
use scheduler::Scheduler;
use store::StoreManager;

const DEFAULT_CONFIG_FILE: &str = "<built-in example in config/examples/basic_cas.json>";

/// Backend for bazel remote execution / cache API.
#[derive(Parser, Debug)]
#[clap(
    author = "Nathan (Blaise) Bruer <thegreatall@gmail.com>",
    version = "0.0.1",
    about,
    long_about = None
)]
struct Args {
    /// Config file to use
    #[clap(default_value = DEFAULT_CONFIG_FILE)]
    config_file: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    // Note: We cannot mutate args, so we create another variable for it here.
    let mut config_file = args.config_file;
    if config_file.eq(DEFAULT_CONFIG_FILE) {
        let r = Runfiles::create().err_tip(|| "Failed to create runfiles lookup object")?;
        config_file = r
            .rlocation("turbo_cache/config/examples/basic_cas.json")
            .into_os_string()
            .into_string()
            .unwrap();
    }

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
        .format_timestamp_millis()
        .init();

    let json_contents = String::from_utf8(tokio::fs::read(config_file).await?)?;
    let cfg: CasConfig = json5::from_str(&json_contents)?;

    let store_manager = Arc::new(StoreManager::new());
    for (name, store_cfg) in cfg.stores {
        store_manager.add_store(
            &name,
            store_factory(&store_cfg, &store_manager)
                .await
                .err_tip(|| format!("Failed to create store '{}'", name))?,
        );
    }

    let mut schedulers = HashMap::new();
    if let Some(schedulers_cfg) = cfg.schedulers {
        for (scheduler_name, scheduler_cfg) in schedulers_cfg {
            schedulers.insert(scheduler_name, Arc::new(Scheduler::new(&scheduler_cfg)));
        }
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
                        ExecutionServer::new(&cfg, &schedulers, &store_manager).and_then(|v| Ok(Some(v.into_service())))
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
                CapabilitiesServer::new(&capabilities_config, &schedulers).and_then(|v| Ok(Some(v.into_service())))?,
            );

        let addr = server_cfg.listen_address.parse()?;
        servers.push(Box::pin(server.serve(addr)));
    }

    if let Err(e) = select_all(servers).await.0 {
        panic!("{:?}", e);
    }
    panic!("No servers should ever resolve their future");
}
