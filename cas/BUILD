# Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

load("@rules_rust//rust:defs.bzl", "rust_binary")

rust_binary(
    name = "cas",
    srcs = ["cas_main.rs"],
    data = ["//config:examples/basic_cas.json"],
    deps = [
        "//cas/grpc_service:ac_server",
        "//cas/grpc_service:bytestream_server",
        "//cas/grpc_service:capabilities_server",
        "//cas/grpc_service:cas_server",
        "//cas/grpc_service:execution_server",
        "//cas/scheduler",
        "//cas/store",
        "//cas/store:default_store_factory",
        "//config",
        "//third_party:clap",
        "//third_party:env_logger",
        "//third_party:futures",
        "//third_party:json5",
        "//third_party:tokio",
        "//third_party:tonic",
        "//util:common",
        "//util:error",
        "@rules_rust//tools/runfiles",
    ],
)
