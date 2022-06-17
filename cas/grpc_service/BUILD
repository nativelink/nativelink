# Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")

rust_library(
    name = "cas_server",
    srcs = ["cas_server.rs"],
    deps = [
        "//cas/store",
        "//cas/store:grpc_store",
        "//config",
        "//proto",
        "//third_party:bytes",
        "//third_party:futures",
        "//third_party:stdext",
        "//third_party:tokio",
        "//third_party:tonic",
        "//util:common",
        "//util:error",
    ],
    visibility = ["//cas:__pkg__"]
)

rust_library(
    name = "ac_server",
    srcs = ["ac_server.rs"],
    deps = [
        "//cas/store",
        "//cas/store:ac_utils",
        "//cas/store:grpc_store",
        "//config",
        "//proto",
        "//third_party:bytes",
        "//third_party:prost",
        "//third_party:tonic",
        "//util:common",
        "//util:error",
    ],
    visibility = ["//cas:__pkg__"]
)

rust_library(
    name = "capabilities_server",
    srcs = ["capabilities_server.rs"],
    deps = [
        "//cas/scheduler",
        "//cas/store",
        "//config",
        "//proto",
        "//third_party:tonic",
        "//util:error",
    ],
    visibility = ["//cas:__pkg__"]
)

rust_library(
    name = "bytestream_server",
    srcs = ["bytestream_server.rs"],
    deps = [
        "//cas/store",
        "//cas/store:grpc_store",
        "//config",
        "//proto",
        "//third_party:fast_async_mutex",
        "//third_party:futures",
        "//third_party:tokio",
        "//third_party:tonic",
        "//util:buf_channel",
        "//util:common",
        "//util:error",
        "//util:resource_info",
        "//util:write_request_stream_wrapper",
    ],
    visibility = ["//cas:__pkg__"]
)

rust_library(
    name = "execution_server",
    srcs = ["execution_server.rs"],
    deps = [
        "//cas/scheduler:action_messages",
        "//cas/scheduler:platform_property_manager",
        "//cas/scheduler:scheduler",
        "//cas/store",
        "//cas/store:ac_utils",
        "//config",
        "//proto",
        "//third_party:futures",
        "//third_party:rand",
        "//third_party:stdext",
        "//third_party:tokio",
        "//third_party:tokio_stream",
        "//third_party:tonic",
        "//util:common",
        "//util:error",
    ],
    visibility = ["//cas:__pkg__"]
)

rust_library(
    name = "worker_api_server",
    srcs = ["worker_api_server.rs"],
    deps = [
        "//cas/scheduler",
        "//cas/scheduler:action_messages",
        "//cas/scheduler:platform_property_manager",
        "//cas/scheduler:worker",
        "//config",
        "//proto",
        "//third_party:futures",
        "//third_party:tokio",
        "//third_party:tonic",
        "//third_party:uuid",
        "//util:common",
        "//util:error",
    ],
    visibility = ["//cas:__pkg__"]
)

rust_test(
    name = "worker_api_server_test",
    srcs = ["tests/worker_api_server_test.rs"],
    deps = [
        "//cas/scheduler",
        "//cas/scheduler:action_messages",
        "//cas/scheduler:platform_property_manager",
        "//cas/scheduler:worker",
        "//config",
        "//proto",
        "//third_party:pretty_assertions",
        "//third_party:tokio",
        "//third_party:tokio_stream",
        "//third_party:tonic",
        "//util:common",
        "//util:error",
        ":worker_api_server",
    ],
)

rust_test(
    name = "cas_server_test",
    srcs = ["tests/cas_server_test.rs"],
    deps = [
        "//cas/store",
        "//cas/store:default_store_factory",
        "//config",
        "//proto",
        "//third_party:maplit",
        "//third_party:pretty_assertions",
        "//third_party:tokio",
        "//third_party:tonic",
        "//util:common",
        "//util:error",
        ":cas_server",
    ],
)

rust_test(
    name = "ac_server_test",
    srcs = ["tests/ac_server_test.rs"],
    deps = [
        "//cas/store",
        "//cas/store:default_store_factory",
        "//config",
        "//proto",
        "//third_party:bytes",
        "//third_party:maplit",
        "//third_party:pretty_assertions",
        "//third_party:prost",
        "//third_party:tokio",
        "//third_party:tonic",
        "//util:common",
        "//util:error",
        ":ac_server",
    ],
)

rust_test(
    name = "bytestream_server_test",
    srcs = ["tests/bytestream_server_test.rs"],
    deps = [
        "//cas/store",
        "//cas/store:default_store_factory",
        "//config",
        "//proto",
        "//third_party:futures",
        "//third_party:maplit",
        "//third_party:pretty_assertions",
        "//third_party:tokio",
        "//third_party:tokio_stream",
        "//third_party:tonic",
        "//util:common",
        "//util:error",
        ":bytestream_server",
    ],
)
