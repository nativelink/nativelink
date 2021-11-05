# Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")

rust_library(
    name = "error",
    srcs = ["error.rs"],
    deps = [
        "//third_party:hex",
        "//third_party:prost",
        "//third_party:tokio",
        "//third_party:tonic",
        "//proto",
    ],
    visibility = ["//visibility:public"],
)

rust_library(
    name = "common",
    srcs = ["common.rs"],
    deps = [
        "//proto",
        "//third_party:hex",
        "//third_party:lazy_init",
        "//third_party:log",
        "//third_party:tokio",
        ":error",
    ],
    visibility = ["//visibility:public"],
)

rust_library(
    name = "retry",
    srcs = ["retry.rs"],
    deps = [
        "//third_party:futures",
        "//third_party:tokio",
        ":error",
    ],
    visibility = ["//visibility:public"],
)

rust_library(
    name = "evicting_map",
    srcs = ["evicting_map.rs"],
    deps = [
        "//config",
        "//third_party:lru",
        "//util:common",
    ],
    visibility = ["//visibility:public"],
)

rust_library(
    name = "async_fixed_buffer",
    srcs = ["async_fixed_buffer.rs"],
    deps = [
        "//third_party:fast_async_mutex",
        "//third_party:fixed_buffer",
        "//third_party:futures",
        "//third_party:pin_project_lite",
        "//third_party:tokio",
    ],
    visibility = ["//visibility:public"],
)

rust_test(
    name = "async_fixed_buffer_test",
    srcs = ["tests/async_fixed_buffer_test.rs"],
    deps = [
        "//third_party:futures",
        "//third_party:pretty_assertions",
        "//third_party:tokio",
        ":async_fixed_buffer",
        ":error",
    ],
)

rust_test(
    name = "evicting_map_test",
    srcs = ["tests/evicting_map_test.rs"],
    deps = [
        "//config",
        "//third_party:hex",
        "//third_party:mock_instant",
        "//third_party:tokio",
        "//util:common",
        ":error",
        ":evicting_map",
    ],
)

rust_test(
    name = "retry_test",
    srcs = ["tests/retry_test.rs"],
    deps = [
        "//third_party:futures",
        "//third_party:pretty_assertions",
        "//third_party:tokio",
        ":error",
        ":retry",
    ],
)
