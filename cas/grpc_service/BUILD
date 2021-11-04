# Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")

rust_library(
    name = "cas_server",
    srcs = ["cas_server.rs"],
    deps = [
        "//cas/store",
        "//config",
        "//proto",
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
        "//config",
        "//proto",
        "//third_party:prost",
        "//third_party:tokio",
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
        "//config",
        "//proto",
        "//third_party:futures",
        "//third_party:tokio",
        "//third_party:tonic",
        "//util:async_fixed_buffer",
        "//util:common",
        "//util:error",
    ],
    visibility = ["//cas:__pkg__"]
)

rust_library(
    name = "execution_server",
    srcs = ["execution_server.rs"],
    deps = [
        "//cas/store",
        "//config",
        "//proto",
        "//third_party:futures",
        "//third_party:stdext",
        "//third_party:tonic",
        "//util:error",
    ],
    visibility = ["//cas:__pkg__"]
)

rust_test(
    name = "cas_server_test",
    srcs = ["tests/cas_server_test.rs"],
    deps = [
        "//cas/store",
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
        "//config",
        "//proto",
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
        "//config",
        "//proto",
        "//third_party:futures",
        "//third_party:maplit",
        "//third_party:pretty_assertions",
        "//third_party:prost",
        "//third_party:tokio",
        "//third_party:tokio_stream",
        "//third_party:tonic",
        "//util:common",
        "//util:error",
        ":bytestream_server",
    ],
)
