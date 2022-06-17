#!/bin/bash
# Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

set -euo pipefail

TEST_PATTERNS=()

while [[ $# -gt 0 ]]; do
  case $1 in
    --help)
      echo <<'EOT'
Runner for integration tests

Usage:
  run_integration_tests.sh [TEST_PATTERNS...]

TEST_PATTERNS: Name of test you wish to execute. Wildcard (*) supported.
               Default: '*'
EOT
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
      ;;
    *)
      TEST_PATTERNS+=("$1")
      shift # past argument
      ;;
  esac
done

if [[ $EUID -ne 0 ]]; then
  echo "This script must be run as root due to docker permission issues" 
  exit 1
fi

if [[ "${#TEST_PATTERNS[@]}" -eq 0 ]]; then
  TEST_PATTERNS=("*")
fi

SELF_DIR=$(realpath $(dirname $0))
cd "$SELF_DIR/deployment-examples/docker-compose"

export UNDER_TEST_RUNNER=1

# Ensure our cache locations are empty.
rm -rf ~/.cache/docker-bazel
rm -rf ~/.cache/turbo-cache
mkdir -p ~/.cache/docker-bazel
mkdir -p ~/.cache/turbo-cache

# Ensure our docker compose is not running.
docker-compose down

export TMPDIR=$HOME/.cache/turbo-cache/
mkdir -p "$TMPDIR"
export CACHE_DIR=$(mktemp -d --suffix="-turbo-cache-integration-test")
export BAZEL_CACHE_DIR="$CACHE_DIR/bazel"
trap "rm -rf $CACHE_DIR; docker-compose down" EXIT

echo "" # New line.

DID_FAIL=0

export TURBO_CACHE_DIR="$CACHE_DIR/turbo-cache"
mkdir -p "$TURBO_CACHE_DIR"

for pattern in "${TEST_PATTERNS[@]}"; do
  find "$SELF_DIR/integration_tests/" -name "$pattern" -type f -print0 | while IFS= read -r -d $'\0' fullpath; do
    # Cleanup.
    find "$TURBO_CACHE_DIR" -delete
    bazel --output_base="$BAZEL_CACHE_DIR" clean
    FILENAME=$(basename $fullpath)
    echo "Running test $FILENAME"
    docker-compose up -d
    set +e
    bash -euo pipefail "$fullpath"
    EXIT_CODE="$?"
    set -e
    if [[ $EXIT_CODE -eq 0 ]]; then
      echo "$FILENAME passed"
    else
      echo "$FILENAME failed with exit code $EXIT_CODE"
      exit $EXIT_CODE
    fi
    docker-compose down
    echo "" # New line.
  done
done

echo "All tests passed!"
