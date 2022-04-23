# Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")

rust_library(
    name = "local_worker",
    srcs = ["local_worker.rs"],
    deps = [
        "//cas/store",
        "//cas/store:fast_slow_store",
        "//config",
        "//proto",
        "//third_party:futures",
        "//third_party:tokio",
        "//third_party:tokio_stream",
        "//third_party:tonic",
        "//util:common",
        "//util:error",
        ":running_actions_manager",
        ":worker_api_client_wrapper",
        ":worker_utils",
    ],
    visibility = ["//cas:__pkg__"],
)

rust_library(
    name = "running_actions_manager",
    srcs = ["running_actions_manager.rs"],
    deps = [
        "//cas/scheduler:action_messages",
        "//cas/store:ac_utils",
        "//cas/store:fast_slow_store",
        "//cas/store:filesystem_store",
        "//proto",
        "//third_party:fast_async_mutex",
        "//third_party:futures",
        "//third_party:tokio",
        "//third_party:filetime",
        "//util:common",
        "//util:error",
    ],
    proc_macro_deps = ["//third_party:async_trait"],
)


rust_library(
    name = "worker_utils",
    srcs = ["worker_utils.rs"],
    deps = [
        "//config",
        "//proto",
        "//third_party:futures",
        "//third_party:shlex",
        "//third_party:tokio",
        "//third_party:tonic",
        "//util:common",
        "//util:error",
    ],
)

rust_library(
    name = "worker_api_client_wrapper",
    srcs = ["worker_api_client_wrapper.rs"],
    deps = [
        "//proto",
        "//third_party:tonic",
    ],
    proc_macro_deps = ["//third_party:async_trait"],
)

rust_library(
    name = "local_worker_test_utils",
    srcs = ["tests/utils/local_worker_test_utils.rs"],
    testonly = True,
    deps = [
        "//config",
        "//proto",
        "//third_party:hyper",
        "//third_party:tokio",
        "//third_party:tonic",
        "//util:common",
        "//util:error",
        ":local_worker",
        ":mock_running_actions_manager",
        ":mock_worker_api_client",
    ],
)


rust_library(
    name = "mock_worker_api_client",
    srcs = ["tests/utils/mock_worker_api_client.rs"],
    testonly = True,
    deps = [
        "//proto",
        "//third_party:fast_async_mutex",
        "//third_party:tokio",
        "//third_party:tonic",
        ":worker_api_client_wrapper",
    ],
    proc_macro_deps = ["//third_party:async_trait"],
)

rust_library(
    name = "mock_running_actions_manager",
    srcs = ["tests/utils/mock_running_actions_manager.rs"],
    testonly = True,
    deps = [
        "//proto",
        "//third_party:fast_async_mutex",
        "//third_party:tokio",
        "//util:error",
        ":running_actions_manager",
    ],
    proc_macro_deps = ["//third_party:async_trait"],
)

rust_test(
    name = "running_actions_manager_test",
    srcs = ["tests/running_actions_manager_test.rs"],
    deps = [
        "//cas/store",
        "//cas/store:fast_slow_store",
        "//cas/store:filesystem_store",
        "//cas/store:memory_store",
        "//config",
        "//proto",
        "//third_party:pretty_assertions",
        "//third_party:prost",
        "//third_party:rand",
        "//third_party:tokio",
        "//util:common",
        "//util:error",
        ":running_actions_manager",
    ],
)

rust_test(
    name = "local_worker_test",
    srcs = ["tests/local_worker_test.rs"],
    deps = [
        "//cas/scheduler:action_messages",
        "//cas/scheduler:platform_property_manager",
        "//config",
        "//proto",
        "//third_party:env_logger",
        "//third_party:pretty_assertions",
        "//third_party:prost",
        "//third_party:tokio",
        "//third_party:tonic",
        "//util:common",
        "//util:error",
        ":local_worker_test_utils",
        ":mock_running_actions_manager",
    ],
    proc_macro_deps = ["//third_party:ctor"],
)
