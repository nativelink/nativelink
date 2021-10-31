# Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

load("@rules_rust//rust:rust.bzl", "rust_library", "rust_test")

rust_library(
    name = "store",
    srcs = ["lib.rs"],
    deps = [
        "//config",
        "//util:error",
        ":memory_store",
        ":s3_store",
        ":traits",
        ":verify_store",
    ],
    visibility = ["//cas:__pkg__", "//cas:__subpackages__"]
)

rust_library(
    name = "traits",
    srcs = ["store_trait.rs"],
    deps = [
        "//third_party:futures",
        "//third_party:tokio",
        "//util:common",
        "//util:error",
    ],
    proc_macro_deps = ["//third_party:async_trait"],
    visibility = ["//cas:__pkg__"]
)

rust_library(
    name = "memory_store",
    srcs = ["memory_store.rs"],
    deps = [
        "//config",
        "//third_party:async_mutex",
        "//third_party:tokio",
        "//util:common",
        "//util:error",
        "//util:evicting_map",
        ":traits",
    ],
    proc_macro_deps = ["//third_party:async_trait"],
    visibility = ["//cas:__pkg__"]
)

rust_library(
    name = "verify_store",
    srcs = ["verify_store.rs"],
    deps = [
        "//config",
        "//third_party:async_mutex",
        "//third_party:hex",
        "//third_party:sha2",
        "//third_party:tokio",
        "//util:async_fixed_buffer",
        "//util:common",
        "//util:error",
        ":traits",
    ],
    proc_macro_deps = ["//third_party:async_trait"],
    visibility = ["//cas:__pkg__"]
)

rust_test(
    name = "memory_store_test",
    srcs = ["tests/memory_store_test.rs"],
    deps = [
        "//config",
        "//third_party:pretty_assertions",
        "//third_party:tokio",
        "//util:common",
        "//util:error",
        ":memory_store",
        ":traits",
    ],
)

rust_test(
    name = "verify_store_test",
    srcs = ["tests/verify_store_test.rs"],
    deps = [
        "//config",
        "//third_party:pretty_assertions",
        "//third_party:tokio",
        "//util:async_fixed_buffer",
        "//util:common",
        "//util:error",
        ":memory_store",
        ":traits",
        ":verify_store",
    ],
)

rust_library(
    name = "s3_store",
    srcs = ["s3_store.rs"],
    deps = [
        "//config",
        "//third_party:futures",
        "//third_party:http",
        "//third_party:rand",
        "//third_party:rusoto_core",
        "//third_party:rusoto_s3",
        "//third_party:tokio",
        "//third_party:tokio_util",
        "//third_party:fast_async_mutex",
        "//util:common",
        "//util:async_read_taker",
        "//util:error",
        "//util:retry",
        ":traits",
    ],
    proc_macro_deps = ["//third_party:async_trait"],
    visibility = ["//cas:__pkg__"]
)

rust_test(
    name = "s3_store_test",
    srcs = ["tests/s3_store_test.rs"],
    deps = [
        "//config",
        "//third_party:futures",
        "//third_party:http",
        "//third_party:pretty_assertions",
        "//third_party:rusoto_core",
        "//third_party:rusoto_mock",
        "//third_party:rusoto_s3",
        "//third_party:tokio",
        "//util:common",
        "//util:error",
        ":s3_store",
        ":traits",
    ],
)
