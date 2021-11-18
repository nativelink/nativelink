# Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")

rust_library(
    name = "store",
    srcs = ["lib.rs"],
    deps = [
        "//config",
        "//third_party:futures",
        "//util:error",
        ":compression_store",
        ":dedup_store",
        ":fast_slow_store",
        ":filesystem_store",
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
        "//third_party:serde",
        "//third_party:bytes",
        "//util:buf_channel",
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
        "//third_party:bytes",
        "//util:common",
        "//util:error",
        "//util:buf_channel",
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
        "//third_party:hex",
        "//third_party:sha2",
        "//third_party:tokio",
        "//util:common",
        "//util:buf_channel",
        "//util:error",
        ":traits",
    ],
    proc_macro_deps = ["//third_party:async_trait"],
    visibility = ["//cas:__pkg__"]
)

rust_library(
    name = "fast_slow_store",
    srcs = ["fast_slow_store.rs"],
    deps = [
        "//config",
        "//third_party:futures",
        "//util:buf_channel",
        "//util:common",
        "//util:error",
        ":traits",
    ],
    proc_macro_deps = ["//third_party:async_trait"],
    visibility = ["//cas:__pkg__"]
)

rust_library(
    name = "dedup_store",
    srcs = ["dedup_store.rs"],
    deps = [
        "//config",
        "//third_party:bincode",
        "//third_party:futures",
        "//third_party:serde",
        "//third_party:sha2",
        "//third_party:tokio",
        "//third_party:tokio_util",
        "//util:buf_channel",
        "//util:common",
        "//util:error",
        "//util:fastcdc",
        ":traits",
    ],
    proc_macro_deps = ["//third_party:async_trait"],
    visibility = ["//cas:__pkg__"]
)

rust_library(
    name = "compression_store",
    srcs = ["compression_store.rs"],
    deps = [
        "//config",
        "//third_party:bincode",
        "//third_party:futures",
        "//third_party:bytes",
        "//third_party:byteorder",
        "//third_party:lz4_flex",
        "//third_party:serde",
        "//third_party:tokio",
        "//util:common",
        "//util:buf_channel",
        "//util:error",
        ":traits",
    ],
    proc_macro_deps = ["//third_party:async_trait"],
    visibility = ["//cas:__pkg__"]
)

rust_library(
    name = "s3_store",
    srcs = ["s3_store.rs"],
    deps = [
        "//config",
        "//third_party:bytes",
        "//third_party:futures",
        "//third_party:http",
        "//third_party:lazy_static",
        "//third_party:rand",
        "//third_party:rusoto_core",
        "//third_party:rusoto_s3",
        "//third_party:rusoto_signature",
        "//third_party:tokio",
        "//third_party:tokio_util",
        "//util:buf_channel",
        "//util:common",
        "//util:error",
        "//util:retry",
        ":traits",
    ],
    proc_macro_deps = ["//third_party:async_trait"],
    visibility = ["//cas:__pkg__"]
)

rust_library(
    name = "filesystem_store",
    srcs = ["filesystem_store.rs"],
    deps = [
        "//config",
        "//third_party:bytes",
        "//third_party:filetime",
        "//third_party:futures",
        "//third_party:rand",
        "//third_party:tokio",
        "//third_party:tokio_stream",
        "//util:buf_channel",
        "//util:common",
        "//util:error",
        "//util:evicting_map",
        ":traits",
    ],
    proc_macro_deps = ["//third_party:async_trait"],
    visibility = ["//cas:__pkg__"]
)

rust_test(
    name = "fast_slow_store_test",
    srcs = ["tests/fast_slow_store_test.rs"],
    deps = [
        "//config",
        "//third_party:pretty_assertions",
        "//third_party:rand",
        "//third_party:tokio",
        "//util:common",
        "//util:error",
        ":fast_slow_store",
        ":memory_store",
        ":traits",
    ],
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
        "//third_party:futures",
        "//third_party:pretty_assertions",
        "//third_party:tokio",
        "//util:buf_channel",
        "//util:common",
        "//util:error",
        ":memory_store",
        ":traits",
        ":verify_store",
    ],
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
        "//third_party:tokio_util",
        "//util:async_fixed_buffer",
        "//util:common",
        "//util:error",
        ":s3_store",
        ":traits",
    ],
)

rust_test(
    name = "compression_store_test",
    srcs = ["tests/compression_store_test.rs"],
    deps = [
        "//config",
        "//third_party:bincode",
        "//third_party:futures",
        "//third_party:pretty_assertions",
        "//third_party:rand",
        "//third_party:tokio",
        "//util:buf_channel",
        "//util:common",
        "//util:error",
        ":compression_store",
        ":memory_store",
        ":traits",
    ],
)

rust_test(
    name = "dedup_store_test",
    srcs = ["tests/dedup_store_test.rs"],
    deps = [
        "//config",
        ":dedup_store",
        "//third_party:pretty_assertions",
        "//third_party:tokio",
        "//third_party:rand",
        "//util:common",
        "//util:error",
        ":memory_store",
        ":traits",
    ],
)


rust_test(
    name = "filesystem_store_test",
    srcs = ["tests/filesystem_store_test.rs"],
    deps = [
        "//config",
        "//third_party:filetime",
        "//third_party:pretty_assertions",
        "//third_party:rand",
        "//third_party:tokio",
        "//util:common",
        "//util:error",
        ":filesystem_store",
        ":traits",
    ],
)
