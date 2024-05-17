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

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_lock::Mutex as AsyncMutex;
use axum::Router;
use clap::Parser;
use futures::future::{select_all, BoxFuture, OptionFuture, TryFutureExt};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use hyper::server::conn::Http;
use hyper::{Response, StatusCode};
use mimalloc::MiMalloc;
use nativelink_config::cas_server::{
    CasConfig, CompressionAlgorithm, GlobalConfig, ListenerConfig, ServerConfig, WorkerConfig,
};
use nativelink_config::stores::ConfigDigestHashFunction;
use nativelink_error::{make_err, Code, Error, ResultExt};
use nativelink_scheduler::action_scheduler::ActionScheduler;
use nativelink_scheduler::default_scheduler_factory::scheduler_factory;
use nativelink_scheduler::operations::Operations;
use nativelink_scheduler::worker::WorkerId;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_service::ac_server::AcServer;
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_service::capabilities_server::CapabilitiesServer;
use nativelink_service::cas_server::CasServer;
use nativelink_service::execution_server::ExecutionServer;
use nativelink_service::health_server::HealthServer;
use nativelink_service::operations_server::OperationsServer;
use nativelink_service::worker_api_server::WorkerApiServer;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::common::fs::{set_idle_file_descriptor_timeout, set_open_file_limit};
use nativelink_util::digest_hasher::{set_default_digest_hasher_func, DigestHasherFunc};
use nativelink_util::health_utils::HealthRegistryBuilder;
use nativelink_util::metrics_utils::{
    set_metrics_enabled_for_this_thread, Collector, CollectorState, Counter, MetricsComponent,
    Registry,
};
use nativelink_util::origin_context::OriginContext;
use nativelink_util::store_trait::{
    set_default_digest_size_health_check, DEFAULT_DIGEST_SIZE_HEALTH_CHECK_CFG,
};
use nativelink_util::task::TaskExecutor;
use nativelink_util::{background_spawn, init_tracing, spawn, spawn_blocking};
use nativelink_worker::local_worker::new_local_worker;
use parking_lot::Mutex;
use rustls_pemfile::{certs as extract_certs, crls as extract_crls};
use scopeguard::guard;
use tokio::net::TcpListener;
#[cfg(target_family = "unix")]
use tokio::signal::unix::{signal, SignalKind};
use tokio_rustls::rustls::pki_types::{CertificateDer, CertificateRevocationListDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig as TlsServerConfig};
use tokio_rustls::TlsAcceptor;
use tonic::codec::CompressionEncoding;
use tonic::transport::Server as TonicServer;
use tower::util::ServiceExt;
use tracing::{error_span, event, trace_span, Level};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Note: This must be kept in sync with the documentation in `PrometheusConfig::path`.
const DEFAULT_PROMETHEUS_METRICS_PATH: &str = "/metrics";

/// Note: This must be kept in sync with the documentation in `AdminConfig::path`.
const DEFAULT_ADMIN_API_PATH: &str = "/admin";

// Note: This must be kept in sync with the documentation in `HealthConfig::path`.
const DEFAULT_HEALTH_STATUS_CHECK_PATH: &str = "/status";

/// Name of environment variable to disable metrics.
const METRICS_DISABLE_ENV: &str = "NATIVELINK_DISABLE_METRICS";

/// Backend for bazel remote execution / cache API.
#[derive(Parser, Debug)]
#[clap(
    author = "Trace Machina, Inc. <nativelink@tracemachina.com>",
    version,
    about,
    long_about = None
)]
struct Args {
    /// Config file to use.
    #[clap(value_parser)]
    config_file: String,
}

async fn inner_main(
    cfg: CasConfig,
    server_start_timestamp: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut root_metrics_registry = <Registry>::with_prefix("nativelink");
    let health_registry_builder = Arc::new(AsyncMutex::new(HealthRegistryBuilder::new(
        "nativelink".into(),
    )));

    let store_manager = Arc::new(StoreManager::new());
    {
        let mut health_registry_lock = health_registry_builder.lock().await;
        let root_store_metrics = root_metrics_registry.sub_registry_with_prefix("stores");

        for (name, store_cfg) in cfg.stores {
            let health_component_name = format!("stores/{name}");
            let mut health_register_store =
                health_registry_lock.sub_builder(health_component_name.into());
            let store_metrics = root_store_metrics.sub_registry_with_prefix(&name);
            store_manager.add_store(
                &name,
                store_factory(
                    &store_cfg,
                    &store_manager,
                    Some(store_metrics),
                    Some(&mut health_register_store),
                )
                .await
                .err_tip(|| format!("Failed to create store '{name}'"))?,
            );
        }
    }

    let mut action_schedulers: HashMap<String, Arc<dyn ActionScheduler>> = HashMap::new();
    let mut worker_schedulers: HashMap<String, Arc<dyn WorkerScheduler>> = HashMap::new();
    let mut operations_schedulers: HashMap<String, Arc<dyn Operations>> = HashMap::new();
    if let Some(schedulers_cfg) = cfg.schedulers {
        let root_scheduler_metrics = root_metrics_registry.sub_registry_with_prefix("schedulers");
        for (name, scheduler_cfg) in schedulers_cfg {
            let scheduler_metrics = root_scheduler_metrics.sub_registry_with_prefix(&name);
            let (maybe_action_scheduler, maybe_worker_scheduler, maybe_scheduler_operations) =
                scheduler_factory(&scheduler_cfg, &store_manager, scheduler_metrics)
                    .err_tip(|| format!("Failed to create scheduler '{name}'"))?;
            if let Some(action_scheduler) = maybe_action_scheduler {
                action_schedulers.insert(name.clone(), action_scheduler);
            }
            if let Some(worker_scheduler) = maybe_worker_scheduler {
                worker_schedulers.insert(name.clone(), worker_scheduler);
            }
            if let Some(scheduler_operations) = maybe_scheduler_operations {
                operations_schedulers.insert(name.clone(), scheduler_operations);
            }
        }
    }

    fn into_encoding(from: &CompressionAlgorithm) -> Option<CompressionEncoding> {
        match from {
            CompressionAlgorithm::gzip => Some(CompressionEncoding::Gzip),
            CompressionAlgorithm::none => None,
        }
    }

    /// Simple wrapper to enable us to register the Hashmap so it can
    /// report metrics about what clients are connected.
    struct ConnectedClientsMetrics {
        inner: Mutex<HashSet<SocketAddr>>,
        counter: Counter,
        server_start_ts: u64,
    }
    impl MetricsComponent for ConnectedClientsMetrics {
        fn gather_metrics(&self, c: &mut CollectorState) {
            c.publish(
                "server_start_time",
                &self.server_start_ts,
                "Timestamp when the server started",
            );

            let connected_clients = self.inner.lock();
            for client in connected_clients.iter() {
                c.publish_with_labels(
                    "connected_clients",
                    &1,
                    "The endpoint of the connected clients",
                    vec![("endpoint".into(), format!("{client}").into())],
                );
            }

            c.publish(
                "total_client_connections",
                &self.counter,
                "Total client connections since server started",
            );
        }
    }

    // Registers all the ConnectedClientsMetrics to the registries
    // and zips them in. It is done this way to get around the need
    // for `root_metrics_registry` to become immutable in the loop.
    let servers_and_clients: Vec<(ServerConfig, _)> = cfg
        .servers
        .into_iter()
        .enumerate()
        .map(|(i, server_cfg)| {
            let name = if server_cfg.name.is_empty() {
                format!("{i}")
            } else {
                server_cfg.name.clone()
            };
            let connected_clients_mux = Arc::new(ConnectedClientsMetrics {
                inner: Mutex::new(HashSet::new()),
                counter: Counter::default(),
                server_start_ts: server_start_timestamp,
            });
            let server_metrics =
                root_metrics_registry.sub_registry_with_prefix(format!("server_{name}"));
            server_metrics.register_collector(Box::new(Collector::new(&connected_clients_mux)));

            (server_cfg, connected_clients_mux)
        })
        .collect();

    let mut root_futures: Vec<BoxFuture<Result<(), Error>>> = Vec::new();

    // Lock our registry as immutable and clonable.
    let root_metrics_registry = Arc::new(AsyncMutex::new(root_metrics_registry));
    for (server_cfg, connected_clients_mux) in servers_and_clients {
        let services = server_cfg.services.ok_or("'services' must be configured")?;

        // Currently we only support http as our socket type.
        let ListenerConfig::http(http_config) = server_cfg.listener;

        let services_execution = services.execution;

        let tonic_services = TonicServer::builder()
            .add_optional_service(
                services
                    .ac
                    .map_or(Ok(None), |cfg| {
                        AcServer::new(&cfg, &store_manager).map(|v| {
                            let mut service = v.into_service();
                            let send_algo = &http_config.compression.send_compression_algorithm;
                            if let Some(encoding) =
                                into_encoding(&send_algo.unwrap_or(CompressionAlgorithm::none))
                            {
                                service = service.send_compressed(encoding);
                            }
                            for encoding in http_config
                                .compression
                                .accepted_compression_algorithms
                                .iter()
                                // Filter None values.
                                .filter_map(into_encoding)
                            {
                                service = service.accept_compressed(encoding);
                            }
                            Some(service)
                        })
                    })
                    .err_tip(|| "Could not create AC service")?,
            )
            .add_optional_service(
                services
                    .cas
                    .map_or(Ok(None), |cfg| {
                        CasServer::new(&cfg, &store_manager).map(|v| {
                            let mut service = v.into_service();
                            let send_algo = &http_config.compression.send_compression_algorithm;
                            if let Some(encoding) =
                                into_encoding(&send_algo.unwrap_or(CompressionAlgorithm::none))
                            {
                                service = service.send_compressed(encoding);
                            }
                            for encoding in http_config
                                .compression
                                .accepted_compression_algorithms
                                .iter()
                                // Filter None values.
                                .filter_map(into_encoding)
                            {
                                service = service.accept_compressed(encoding);
                            }
                            Some(service)
                        })
                    })
                    .err_tip(|| "Could not create CAS service")?,
            )
            .add_optional_service(
                services_execution
                    .clone()
                    .map_or(Ok(None), |cfg| {
                        ExecutionServer::new(
                            &cfg,
                            &action_schedulers,
                            &operations_schedulers,
                            &store_manager,
                        )
                        .map(|v| {
                            let mut service = v.into_service();
                            let send_algo = &http_config.compression.send_compression_algorithm;
                            if let Some(encoding) =
                                into_encoding(&send_algo.unwrap_or(CompressionAlgorithm::none))
                            {
                                service = service.send_compressed(encoding);
                            }
                            for encoding in http_config
                                .compression
                                .accepted_compression_algorithms
                                .iter()
                                // Filter None values.
                                .filter_map(into_encoding)
                            {
                                service = service.accept_compressed(encoding);
                            }
                            Some(service)
                        })
                    })
                    .err_tip(|| "Could not create Execution service")?,
            )
            .add_optional_service(
                services
                    .bytestream
                    .map_or(Ok(None), |cfg| {
                        ByteStreamServer::new(&cfg, &store_manager).map(|v| {
                            let mut service = v.into_service();
                            let send_algo = &http_config.compression.send_compression_algorithm;
                            if let Some(encoding) =
                                into_encoding(&send_algo.unwrap_or(CompressionAlgorithm::none))
                            {
                                service = service.send_compressed(encoding);
                            }
                            for encoding in http_config
                                .compression
                                .accepted_compression_algorithms
                                .iter()
                                // Filter None values.
                                .filter_map(into_encoding)
                            {
                                service = service.accept_compressed(encoding);
                            }
                            Some(service)
                        })
                    })
                    .err_tip(|| "Could not create ByteStream service")?,
            )
            .add_optional_service(
                OptionFuture::from(
                    services
                        .capabilities
                        .as_ref()
                        // Borrow checker fighting here...
                        .map(|_| {
                            CapabilitiesServer::new(
                                services.capabilities.as_ref().unwrap(),
                                &action_schedulers,
                            )
                        }),
                )
                .await
                .map_or(Ok::<Option<CapabilitiesServer>, Error>(None), |server| {
                    Ok(Some(server?))
                })
                .err_tip(|| "Could not create Capabilities service")?
                .map(|v| {
                    let mut service = v.into_service();
                    let send_algo = &http_config.compression.send_compression_algorithm;
                    if let Some(encoding) =
                        into_encoding(&send_algo.unwrap_or(CompressionAlgorithm::none))
                    {
                        service = service.send_compressed(encoding);
                    }
                    for encoding in http_config
                        .compression
                        .accepted_compression_algorithms
                        .iter()
                        // Filter None values.
                        .filter_map(into_encoding)
                    {
                        service = service.accept_compressed(encoding);
                    }
                    service
                }),
            )
            .add_optional_service(
                services
                    .worker_api
                    .map_or(Ok(None), |cfg| {
                        WorkerApiServer::new(&cfg, &worker_schedulers).map(|v| {
                            let mut service = v.into_service();
                            let send_algo = &http_config.compression.send_compression_algorithm;
                            if let Some(encoding) =
                                into_encoding(&send_algo.unwrap_or(CompressionAlgorithm::none))
                            {
                                service = service.send_compressed(encoding);
                            }
                            for encoding in http_config
                                .compression
                                .accepted_compression_algorithms
                                .iter()
                                // Filter None values.
                                .filter_map(into_encoding)
                            {
                                service = service.accept_compressed(encoding);
                            }
                            Some(service)
                        })
                    })
                    .err_tip(|| "Could not create WorkerApi service")?,
            )
            .add_optional_service(
                services_execution
                    .clone()
                    .map_or(Ok(None), |cfg| {
                        ExecutionServer::new(
                            &cfg,
                            &action_schedulers,
                            &operations_schedulers,
                            &store_manager,
                        )
                        .map(|v| {
                            let mut service = v.into_operations_service();
                            let send_algo = &http_config.compression.send_compression_algorithm;
                            if let Some(encoding) =
                                into_encoding(&send_algo.unwrap_or(CompressionAlgorithm::none))
                            {
                                service = service.send_compressed(encoding);
                            }
                            for encoding in http_config
                                .compression
                                .accepted_compression_algorithms
                                .iter()
                                // Filter None values.
                                .filter_map(into_encoding)
                            {
                                service = service.accept_compressed(encoding);
                            }
                            Some(service)
                        })
                    })
                    .err_tip(|| "Could not create Operations service")?,
            );
        // .add_optional_service(services.operations.map_or(None, |cfg| {
        //     OperationsServer::new(&cfg, &operations_schedulers)
        //         .map(|v| {
        //             let mut service = v.into_service();
        //             Some(service)
        //         })
        //         .ok()?
        // }));

        let root_metrics_registry = root_metrics_registry.clone();
        let health_registry = health_registry_builder.lock().await.build();

        let mut svc = Router::new()
            // This is the default service that executes if no other endpoint matches.
            .fallback_service(tonic_services.into_service().map_err(|e| panic!("{e}")));

        if let Some(health_cfg) = services.health {
            let path = if health_cfg.path.is_empty() {
                DEFAULT_HEALTH_STATUS_CHECK_PATH
            } else {
                &health_cfg.path
            };
            svc = svc.route_service(path, HealthServer::new(health_registry));
        }

        if let Some(prometheus_cfg) = services.experimental_prometheus {
            fn error_to_response<E: std::error::Error>(e: E) -> Response<String> {
                let mut response = Response::new(format!("Error: {e:?}"));
                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                response
            }
            let path = if prometheus_cfg.path.is_empty() {
                DEFAULT_PROMETHEUS_METRICS_PATH
            } else {
                &prometheus_cfg.path
            };
            svc = svc.route_service(
                path,
                axum::routing::get(move |_request: hyper::Request<hyper::Body>| {
                    Arc::new(OriginContext::new()).wrap_async(
                        trace_span!("prometheus_ctx"),
                        async move {
                            // We spawn on a thread that can block to give more freedom to our metrics
                            // collection. This allows it to call functions like `tokio::block_in_place`
                            // if it needs to wait on a future.
                            spawn_blocking!("prometheus_metrics", move || {
                                let mut buf = String::new();
                                let root_metrics_registry_guard =
                                    futures::executor::block_on(root_metrics_registry.lock());
                                prometheus_client::encoding::text::encode(
                                    &mut buf,
                                    &root_metrics_registry_guard,
                                )
                                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                                .map(|_| {
                                    // This is a hack to get around this bug: https://github.com/prometheus/client_rust/issues/155
                                    buf = buf.replace("nativelink_nativelink_stores_", "");
                                    buf = buf.replace("nativelink_nativelink_workers_", "");
                                    let mut response = Response::new(buf);
                                    // Per spec we should probably use `application/openmetrics-text; version=1.0.0; charset=utf-8`
                                    // https://github.com/OpenObservability/OpenMetrics/blob/1386544931307dff279688f332890c31b6c5de36/specification/OpenMetrics.md#overall-structure
                                    // However, this makes debugging more difficult, so we use the old text/plain instead.
                                    response.headers_mut().insert(
                                        hyper::header::CONTENT_TYPE,
                                        hyper::header::HeaderValue::from_static(
                                            "text/plain; version=0.0.4; charset=utf-8",
                                        ),
                                    );
                                    response
                                })
                                .unwrap_or_else(error_to_response)
                            })
                            .await
                            .unwrap_or_else(error_to_response)
                        },
                    )
                }),
            )
        }

        if let Some(admin_config) = services.admin {
            let path = if admin_config.path.is_empty() {
                DEFAULT_ADMIN_API_PATH
            } else {
                &admin_config.path
            };
            let worker_schedulers = Arc::new(worker_schedulers.clone());
            svc = svc.nest_service(
                path,
                Router::new().route(
                    "/scheduler/:instance_name/set_drain_worker/:worker_id/:is_draining",
                    axum::routing::post(
                        move |params: axum::extract::Path<(String, String, String)>| async move {
                            let (instance_name, worker_id, is_draining) = params.0;
                            (async move {
                                let is_draining = match is_draining.as_str() {
                                    "0" => false,
                                    "1" => true,
                                    _ => {
                                        return Err(make_err!(
                                            Code::Internal,
                                            "{} is neither 0 nor 1",
                                            is_draining
                                        ))
                                    }
                                };
                                worker_schedulers
                                    .get(&instance_name)
                                    .err_tip(|| {
                                        format!(
                                            "Can not get an instance with the name of '{}'",
                                            &instance_name
                                        )
                                    })?
                                    .clone()
                                    .set_drain_worker(
                                        WorkerId::try_from(worker_id.clone())?,
                                        is_draining,
                                    )
                                    .await?;
                                Ok::<_, Error>(format!("Draining worker {worker_id}"))
                            })
                            .await
                            .map_err(|e| {
                                Err::<String, _>((
                                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                                    format!("Error: {e:?}"),
                                ))
                            })
                        },
                    ),
                ),
            )
        }

        // Configure our TLS acceptor if we have TLS configured.
        let maybe_tls_acceptor = http_config.tls.map_or(Ok(None), |tls_config| {
            fn read_cert(cert_file: &str) -> Result<Vec<CertificateDer<'static>>, Error> {
                let mut cert_reader = std::io::BufReader::new(
                    std::fs::File::open(cert_file)
                        .err_tip(|| format!("Could not open cert file {cert_file}"))?,
                );
                let certs = extract_certs(&mut cert_reader)
                    .map(|certificate| certificate.map(CertificateDer::from))
                    .collect::<Result<Vec<CertificateDer<'_>>, _>>()
                    .err_tip(|| format!("Could not extract certs from file {cert_file}"))?;
                Ok(certs)
            }
            let certs = read_cert(&tls_config.cert_file)?;
            let mut key_reader = std::io::BufReader::new(
                std::fs::File::open(&tls_config.key_file)
                    .err_tip(|| format!("Could not open key file {}", tls_config.key_file))?,
            );
            let key = match rustls_pemfile::read_one(&mut key_reader)
                .err_tip(|| format!("Could not extract key(s) from file {}", tls_config.key_file))?
            {
                Some(rustls_pemfile::Item::Pkcs8Key(key)) => key.into(),
                Some(rustls_pemfile::Item::Sec1Key(key)) => key.into(),
                Some(rustls_pemfile::Item::Pkcs1Key(key)) => key.into(),
                _ => {
                    return Err(make_err!(
                        Code::Internal,
                        "No keys found in file {}",
                        tls_config.key_file
                    ))
                }
            };
            if let Ok(Some(_)) = rustls_pemfile::read_one(&mut key_reader) {
                return Err(make_err!(
                    Code::InvalidArgument,
                    "Expected 1 key in file {}",
                    tls_config.key_file
                ));
            }
            let verifier = if let Some(client_ca_file) = &tls_config.client_ca_file {
                let mut client_auth_roots = RootCertStore::empty();
                for cert in read_cert(client_ca_file)?.into_iter() {
                    client_auth_roots.add(cert).map_err(|e| {
                        make_err!(Code::Internal, "Could not read client CA: {e:?}")
                    })?;
                }
                let crls = if let Some(client_crl_file) = &tls_config.client_crl_file {
                    let mut crl_reader = std::io::BufReader::new(
                        std::fs::File::open(client_crl_file)
                            .err_tip(|| format!("Could not open CRL file {client_crl_file}"))?,
                    );
                    extract_crls(&mut crl_reader)
                        .map(|crl| crl.map(CertificateRevocationListDer::from))
                        .collect::<Result<_, _>>()
                        .err_tip(|| format!("Could not extract CRLs from file {client_crl_file}"))?
                } else {
                    Vec::new()
                };
                WebPkiClientVerifier::builder(Arc::new(client_auth_roots))
                    .with_crls(crls)
                    .build()
                    .map_err(|e| {
                        make_err!(
                            Code::Internal,
                            "Could not create WebPkiClientVerifier: {e:?}"
                        )
                    })?
            } else {
                WebPkiClientVerifier::no_client_auth()
            };
            let mut config = TlsServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .map_err(|e| {
                    make_err!(Code::Internal, "Could not create TlsServerConfig : {e:?}")
                })?;

            config.alpn_protocols.push("h2".into());
            Ok(Some(TlsAcceptor::from(Arc::new(config))))
        })?;

        let socket_addr = http_config.socket_address.parse::<SocketAddr>()?;
        let tcp_listener = TcpListener::bind(&socket_addr).await?;
        let mut http = Http::new();
        let http_config = &http_config.advanced_http;
        if let Some(value) = http_config.http2_keep_alive_interval {
            http.http2_keep_alive_interval(Duration::from_secs(u64::from(value)));
        }

        if let Some(value) = http_config.experimental_http2_max_pending_accept_reset_streams {
            http.http2_max_pending_accept_reset_streams(usize::try_from(value).err_tip(|| {
                "Could not convert experimental_http2_max_pending_accept_reset_streams"
            })?);
        }
        if let Some(value) = http_config.experimental_http2_initial_stream_window_size {
            http.http2_initial_stream_window_size(value);
        }
        if let Some(value) = http_config.experimental_http2_initial_connection_window_size {
            http.http2_initial_connection_window_size(value);
        }
        if let Some(value) = http_config.experimental_http2_adaptive_window {
            http.http2_adaptive_window(value);
        }
        if let Some(value) = http_config.experimental_http2_max_frame_size {
            http.http2_max_frame_size(value);
        }
        if let Some(value) = http_config.experimental_http2_max_concurrent_streams {
            http.http2_max_concurrent_streams(value);
        }
        if let Some(value) = http_config.experimental_http2_keep_alive_timeout {
            http.http2_keep_alive_timeout(Duration::from_secs(u64::from(value)));
        }
        if let Some(value) = http_config.experimental_http2_max_send_buf_size {
            http.http2_max_send_buf_size(
                usize::try_from(value).err_tip(|| "Could not convert http2_max_send_buf_size")?,
            );
        }
        if let Some(true) = http_config.experimental_http2_enable_connect_protocol {
            http.http2_enable_connect_protocol();
        }
        if let Some(value) = http_config.experimental_http2_max_header_list_size {
            http.http2_max_header_list_size(value);
        }

        event!(Level::WARN, "Ready, listening on {socket_addr}",);
        root_futures.push(Box::pin(async move {
            loop {
                // Wait for client to connect.
                let (tcp_stream, remote_addr) = match tcp_listener.accept().await {
                    Ok(result) => result,
                    Err(err) => {
                        event!(Level::ERROR, ?err, "Failed to accept tcp connection");
                        continue;
                    }
                };
                event!(
                    target: "nativelink::services",
                    Level::INFO,
                    ?remote_addr,
                    ?socket_addr,
                    "Client connected"
                );
                connected_clients_mux.inner.lock().insert(remote_addr);
                connected_clients_mux.counter.inc();

                // This is the safest way to guarantee that if our future
                // is ever dropped we will cleanup our data.
                let scope_guard = guard(
                    Arc::downgrade(&connected_clients_mux),
                    move |weak_connected_clients_mux| {
                        event!(
                            target: "nativelink::services",
                            Level::INFO,
                            ?remote_addr,
                            ?socket_addr,
                            "Client disconnected"
                        );
                        if let Some(connected_clients_mux) = weak_connected_clients_mux.upgrade() {
                            connected_clients_mux.inner.lock().remove(&remote_addr);
                        }
                    },
                );
                let (http, svc, maybe_tls_acceptor) =
                    (http.clone(), svc.clone(), maybe_tls_acceptor.clone());
                Arc::new(OriginContext::new()).background_spawn(
                    error_span!(
                        target: "nativelink::services",
                        "http_connection",
                        ?remote_addr,
                        ?socket_addr
                    ),
                    async move {},
                );
                background_spawn!(
                    name: "http_connection",
                    fut: async move {
                        // Move it into our spawn, so if our spawn dies the cleanup happens.
                        let _guard = scope_guard;
                        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                        let http = http.with_executor(TaskExecutor::new(tx));
                        let mut http_svc_fut = if let Some(tls_acceptor) = maybe_tls_acceptor {
                            let tls_stream = match tls_acceptor.accept(tcp_stream).await {
                                Ok(result) => result,
                                Err(err) => {
                                    event!(Level::ERROR, ?err, "Failed to accept tls stream");
                                    return;
                                }
                            };
                            http.serve_connection(tls_stream, svc).left_future()
                        } else {
                            http.serve_connection(tcp_stream, svc).right_future()
                        };
                        let mut futures = FuturesUnordered::new();
                        futures.push(futures::future::pending().right_future());
                        loop {
                            tokio::select! {
                                maybe_new_future = rx.recv() => {
                                    maybe_new_future.map(|fut| futures.push(fut.left_future())).unwrap_or_else(|| {
                                        event!(
                                            target: "nativelink::services",
                                            Level::DEBUG,
                                            ?remote_addr,
                                            "Dropped new_future_receiver",
                                        )
                                    });
                                },
                                result = &mut http_svc_fut => {
                                    if let Err(err) = result.map_err(|err| {
                                        use std::error::Error;
                                        if let Some(inner_err) = err.source() {
                                            if let Some(io_err) = inner_err.downcast_ref::<std::io::Error>() {
                                                if io_err.kind() == std::io::ErrorKind::NotConnected {
                                                    return Ok(());
                                                }
                                            }
                                        }
                                        Err(err)
                                    }) {
                                        event!(
                                            target: "nativelink::services",
                                            Level::ERROR,
                                            ?err,
                                            "Failed running service"
                                        );
                                    }
                                    return; // Once the service is done, we don't have any more work to do.
                                },
                                _ = futures.next() => { /* This just pulls a pool of futures. */ },
                            };
                        }
                    },
                    target: "nativelink::services",
                    ?remote_addr,
                    ?socket_addr,
                );
            }
        }));
    }

    {
        // We start workers after our TcpListener is setup so if our worker connects to one
        // of these services it will be able to connect.
        let worker_cfgs = cfg.workers.unwrap_or_default();
        let mut root_metrics_registry_guard = root_metrics_registry.lock().await;
        let root_worker_metrics = root_metrics_registry_guard.sub_registry_with_prefix("workers");
        let mut worker_names = HashSet::with_capacity(worker_cfgs.len());
        for (i, worker_cfg) in worker_cfgs.into_iter().enumerate() {
            let spawn_fut = match worker_cfg {
                WorkerConfig::local(local_worker_cfg) => {
                    let fast_slow_store = store_manager
                        .get_store(&local_worker_cfg.cas_fast_slow_store)
                        .err_tip(|| {
                            format!(
                                "Failed to find store for cas_store_ref in worker config : {}",
                                local_worker_cfg.cas_fast_slow_store
                            )
                        })?;

                    let maybe_ac_store = if let Some(ac_store_ref) =
                        &local_worker_cfg.upload_action_result.ac_store
                    {
                        Some(store_manager.get_store(ac_store_ref).err_tip(|| {
                            format!("Failed to find store for ac_store in worker config : {ac_store_ref}")
                        })?)
                    } else {
                        None
                    };
                    // Note: Defaults to fast_slow_store if not specified. If this ever changes it must
                    // be updated in config documentation for the `historical_results_store` the field.
                    let historical_store = if let Some(cas_store_ref) = &local_worker_cfg
                        .upload_action_result
                        .historical_results_store
                    {
                        store_manager.get_store(cas_store_ref).err_tip(|| {
                                format!(
                                "Failed to find store for historical_results_store in worker config : {cas_store_ref}"
                            )
                            })?
                    } else {
                        fast_slow_store.clone()
                    };
                    let local_worker = new_local_worker(
                        Arc::new(local_worker_cfg),
                        fast_slow_store,
                        maybe_ac_store,
                        historical_store,
                    )
                    .await
                    .err_tip(|| "Could not make LocalWorker")?;
                    let name = if local_worker.name().is_empty() {
                        format!("worker_{i}")
                    } else {
                        local_worker.name().clone()
                    };
                    if worker_names.contains(&name) {
                        Err(Box::new(make_err!(
                            Code::InvalidArgument,
                            "Duplicate worker name '{}' found in config",
                            name
                        )))?;
                    }
                    let worker_metrics = root_worker_metrics.sub_registry_with_prefix(&name);
                    local_worker.register_metrics(worker_metrics);
                    worker_names.insert(name.clone());
                    let fut = Arc::new(OriginContext::new())
                        .wrap_async(trace_span!("worker_ctx"), local_worker.run());
                    spawn!("worker", fut, ?name)
                }
            };
            root_futures.push(Box::pin(spawn_fut.map_ok_or_else(|e| Err(e.into()), |v| v)));
        }
    }

    if let Err(e) = select_all(root_futures).await.0 {
        panic!("{e:?}");
    }
    unreachable!("None of the futures should resolve in main()");
}

async fn get_config() -> Result<CasConfig, Box<dyn std::error::Error>> {
    let args = Args::parse();
    let json_contents = String::from_utf8(
        std::fs::read(&args.config_file)
            .err_tip(|| format!("Could not open config file {}", args.config_file))?,
    )?;
    Ok(serde_json5::from_str(&json_contents)?)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing()?;

    let mut cfg = futures::executor::block_on(get_config())?;

    let (mut metrics_enabled, max_blocking_threads) = {
        // Note: If the default changes make sure you update the documentation in
        // `config/cas_server.rs`.
        const DEFAULT_MAX_OPEN_FILES: usize = 512;
        // Note: If the default changes make sure you update the documentation in
        // `config/cas_server.rs`.
        const DEFAULT_IDLE_FILE_DESCRIPTOR_TIMEOUT_MILLIS: u64 = 1000;
        let global_cfg = if let Some(global_cfg) = &mut cfg.global {
            if global_cfg.max_open_files == 0 {
                global_cfg.max_open_files = DEFAULT_MAX_OPEN_FILES;
            }
            if global_cfg.idle_file_descriptor_timeout_millis == 0 {
                global_cfg.idle_file_descriptor_timeout_millis =
                    DEFAULT_IDLE_FILE_DESCRIPTOR_TIMEOUT_MILLIS;
            }
            if global_cfg.default_digest_size_health_check == 0 {
                global_cfg.default_digest_size_health_check = DEFAULT_DIGEST_SIZE_HEALTH_CHECK_CFG;
            }

            *global_cfg
        } else {
            GlobalConfig {
                max_open_files: DEFAULT_MAX_OPEN_FILES,
                idle_file_descriptor_timeout_millis: DEFAULT_IDLE_FILE_DESCRIPTOR_TIMEOUT_MILLIS,
                disable_metrics: cfg.servers.iter().all(|v| {
                    let Some(service) = &v.services else {
                        return true;
                    };
                    service.experimental_prometheus.is_none()
                }),
                default_digest_hash_function: None,
                default_digest_size_health_check: DEFAULT_DIGEST_SIZE_HEALTH_CHECK_CFG,
            }
        };
        set_open_file_limit(global_cfg.max_open_files);
        set_idle_file_descriptor_timeout(Duration::from_millis(
            global_cfg.idle_file_descriptor_timeout_millis,
        ))?;
        set_default_digest_hasher_func(DigestHasherFunc::from(
            global_cfg
                .default_digest_hash_function
                .unwrap_or(ConfigDigestHashFunction::sha256),
        ))?;
        set_default_digest_size_health_check(global_cfg.default_digest_size_health_check)?;
        // TODO (#513): prevent deadlocks by assigning max blocking threads number of open files * ten
        (!global_cfg.disable_metrics, global_cfg.max_open_files * 10)
    };
    // Override metrics enabled if the environment variable is set.
    if std::env::var(METRICS_DISABLE_ENV).is_ok() {
        metrics_enabled = false;
    }
    let server_start_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    #[allow(clippy::disallowed_methods)]
    {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .max_blocking_threads(max_blocking_threads)
            .enable_all()
            .on_thread_start(move || set_metrics_enabled_for_this_thread(metrics_enabled))
            .build()?;
        runtime.spawn(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to listen to SIGINT");
            eprintln!("User terminated process via SIGINT");
            std::process::exit(130);
        });

        #[cfg(target_family = "unix")]
        runtime.spawn(async move {
            signal(SignalKind::terminate())
                .expect("Failed to listen to SIGTERM")
                .recv()
                .await;
            eprintln!("Process terminated via SIGTERM");
            std::process::exit(143);
        });

        runtime.block_on(
            Arc::new(OriginContext::new())
                .wrap_async(trace_span!("main"), inner_main(cfg, server_start_time)),
        )
    }
}
