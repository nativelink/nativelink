# Copyright 2020 Nathan (Blaise) Bruer.  All rights reserved.

load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_library")

# These build rules will do the following:
# 1. Fetch all .proto files under in this directory
# 2. Generate .pb.rs files for each one of these proto files
# 3. Generate a lib.rs file that should be used as the import path used
#    for all dependencies.
# 4. Test to ensure the checked-in genproto/*.pb.rs file matches the generated
#    code.

_proto_files = glob(["**/*.proto"])
_proto_base_files = depset(['.'.join(file[:-len(".proto")].split('/')[:-1]) for file in _proto_files]).to_list()
_proto_base_files_without_google = [name for name in _proto_base_files if name != "google.protobuf"]
_genproto_files = ["%s.pb.rs" % pkg for pkg in _proto_base_files_without_google]

_checked_in_proto_files = ["genproto/%s" % file for file in _genproto_files]

rust_binary(
    name = "gen_protos_tool",
    srcs = ["gen_protos_tool.rs"],
    deps = [
        "//third_party:clap",
        "//third_party:prost_build",
        "//third_party:rustfmt_nightly",
        "//third_party:tonic_build",
    ]
)

genrule(
    name = "gen_rs_protos",
    srcs = _proto_files,
    outs = _genproto_files,
    cmd = "\n".join([
        "set -e",
        # By default `rustfmt` will be named `cargo_bin_rustfmt`, but in
        # `tonic_build` it is hard codded to look for `rustfmt` program, so we
        # make a temp directory symlink a new file with the right name in that
        # directory then change our path to look there first before running
        # `tonic_build`. Also cleanup the tmp directory after it runs.
        "TMPPATH=$$(mktemp -d -t gen_proto.XXXX)",
        "trap \"rm -rf $$TMPPATH\" EXIT",
        "ln -s $$PWD/$(location //third_party:cargo_bin_rustfmt) $$TMPPATH/rustfmt",
        "export PATH=$$TMPPATH:$$PATH",
        "export PROTOC=bazel-out/host/bin/external/com_google_protobuf/protoc",
        "$(execpath :gen_protos_tool) --output_dir $(RULEDIR) " + " ".join([
            ("--input $(location :%s)" % file) for file in _proto_files
        ]),
    ] + ["mv $(RULEDIR)/{name}.rs $(RULEDIR)/{name}.pb.rs".format(name=name) for name in _proto_base_files_without_google]),
    tools = [
        ":gen_protos_tool",
        "@com_google_protobuf//:protoc",
        "//third_party:cargo_bin_rustfmt",
    ],
)

py_binary(
    name = "gen_lib_rs_tool",
    srcs = ["gen_lib_rs_tool.py"],
    python_version = "PY3",
    srcs_version = "PY3",
)

genrule(
    name = "gen_lib_rs",
    srcs = _genproto_files,
    outs = ["lib.rs"],
    cmd = "$(execpath :gen_lib_rs_tool) --rootdir $(RULEDIR) $(SRCS) > $@",
    tools = [":gen_lib_rs_tool"],
)

rust_library(
    name = "proto",
    srcs = [
        ":gen_lib_rs",
    ] + _genproto_files,
    deps = [
        "//third_party:prost",
        "//third_party:prost_types",
        "//third_party:tonic",
    ],
    visibility = ["//visibility:public"],
)

py_binary(
    name = "update_protos",
    srcs = ["update_protos.py"],
    args = ["--update"] + _proto_base_files_without_google,
    data = _genproto_files,
    python_version = "PY3",
    srcs_version = "PY3",
)

# Test to ensure the proto files are in sync with the checked in files.
py_test(
    name = "update_protos_test",
    srcs = ["update_protos.py"],
    args = ["--check"] + _proto_base_files_without_google,
    data = _genproto_files + glob(_checked_in_proto_files),
    main = "update_protos.py",
    python_version = "PY3",
    srcs_version = "PY3",
)
