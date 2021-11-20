# Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")

rust_library(
    name = "platform_property_manager",
    srcs = ["platform_property_manager.rs"],
    deps = [
        "//config",
        "//util:error",
    ],
    visibility = ["//cas:__pkg__", "//cas:__subpackages__"]
)

rust_library(
    name = "scheduler",
    srcs = ["scheduler.rs"],
    deps = [
        "//cas/grpc_service:ac_utils",
        "//config",
        "//proto",
        "//cas/store",
        "//util:common",
        "//util:error",
        ":platform_property_manager",
    ],
    visibility = ["//cas:__pkg__", "//cas:__subpackages__"]
)
