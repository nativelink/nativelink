# Copyright 2020-2021 Nathan (Blaise) Bruer.  All rights reserved.

workspace(name = "turbo-cache")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_rust",
    sha256 = "7fb9b4fe1a6fb4341bdf7c623e619460ecc0f52d5061cc56abc750111fba8a87",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_rust/releases/download/0.7.0/rules_rust-v0.7.0.tar.gz",
        "https://github.com/bazelbuild/rules_rust/releases/download/0.7.0/rules_rust-v0.7.0.tar.gz",
    ],
)

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
http_archive(
    name = "rules_python",
    url = "https://github.com/bazelbuild/rules_python/releases/download/0.5.0/rules_python-0.5.0.tar.gz",
    sha256 = "cd6730ed53a002c56ce4e2f396ba3b3be262fd7cb68339f0377a45e8227fe332",
)

load("@rules_rust//rust:repositories.bzl", "rules_rust_dependencies", "rust_register_toolchains")
rules_rust_dependencies()
rust_register_toolchains(version = "1.62.1", edition="2021")
# rust_repositories(version = "nightly", iso_date = "2021-11-01", edition="2021", dev_components = True)

load("//third_party:crates.bzl", "raze_fetch_remote_crates")
raze_fetch_remote_crates()

load("@rules_rust//proto:repositories.bzl", "rust_proto_repositories")
rust_proto_repositories()

# @com_google_protobuf is loaded from `rust_proto_repositories`.
load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")
protobuf_deps()
