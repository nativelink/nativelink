#!/bin/bash
# Copyright 2020 Nathan (Blaise) Bruer.  All rights reserved.

set -eu -o pipefail

cd "$(dirname $(realpath $0))"

RUST_FMT="$(find ./bazel-bin/external/rules_rust/tools/rustfmt/rustfmt.runfiles -ipath */bin/rustfmt | head -n1)"
if [ ! -e "$RUST_FMT" ] ; then
  bazel build @rules_rust//:rustfmt
fi
if [ "${1:-}" != "" ]; then
  FILES="$@"
else
  # If there's a permission denied it may return a non-zero exit code, so just ignore it.
  FILES="$(find . \
    -type f \
    -name '*.rs' \
    ! -name '*.pb.rs' \
    ! -ipath '*/.*' \
    ! -ipath '*/target/*' \
  2> >(grep -v 'Permission denied' >&2) || true)"
fi
export BUILD_WORKSPACE_DIRECTORY=$PWD
echo "$FILES" | parallel -I% --max-args 1 $RUST_FMT --emit files --edition 2021 %
