# Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

load("@rules_rust//rust:defs.bzl", "rust_library")

exports_files([
    "examples/basic_cas.json",
    "examples/filesystem_cas.json",
    "examples/s3_backend_with_local_fast_cas.json",
])

rust_library(
    name = "config",
    srcs = ["lib.rs"],
    deps = [
        "//third_party:serde",
        ":backends",
        ":cas_server",
    ],
    visibility = ["//visibility:public"],
)

rust_library(
    name = "cas_server",
    srcs = ["cas_server.rs"],
    deps = [
        "//third_party:serde",
        "//util:serde_utils",
        ":backends",
    ],
)

rust_library(
    name = "backends",
    srcs = ["backends.rs"],
    deps = [
        "//third_party:serde",
        "//util:serde_utils",
    ],
)
