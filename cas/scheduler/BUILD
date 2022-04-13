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
        "//config",
        "//third_party:fast_async_mutex",
        "//third_party:lru",
        "//third_party:rand",
        "//third_party:tokio",
        "//util:common",
        "//util:error",
        ":action_messages",
        ":platform_property_manager",
        ":worker",
    ],
    visibility = ["//cas:__pkg__", "//cas:__subpackages__"]
)

rust_library(
    name = "worker",
    srcs = ["worker.rs"],
    deps = [
        "//proto",
        "//third_party:tokio",
        "//third_party:uuid",
        "//util:error",
        ":action_messages",
        ":platform_property_manager",
    ],
    visibility = ["//cas:__pkg__", "//cas:__subpackages__"]
)

rust_library(
    name = "action_messages",
    srcs = ["action_messages.rs"],
    deps = [
        "//proto",
        "//third_party:prost",
        "//third_party:prost_types",
        "//util:common",
        "//util:error",
        ":platform_property_manager",
    ],
    visibility = ["//cas:__pkg__", "//cas:__subpackages__"]
)

rust_test(
    name = "scheduler_test",
    srcs = ["tests/scheduler_test.rs"],
    deps = [
        "//config",
        "//proto",
        "//third_party:pretty_assertions",
        "//third_party:tokio",
        "//util:common",
        "//util:error",
        ":action_messages",
        ":platform_property_manager",
        ":scheduler",
        ":worker",
    ],
    visibility = ["//cas:__pkg__", "//cas:__subpackages__"]
)
