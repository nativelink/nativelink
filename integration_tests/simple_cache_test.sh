#!/bin/bash
# Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.
# This is a sanity check test to ensure we are caching test results.

if [[ $UNDER_TEST_RUNNER -ne 1 ]]; then
  echo "This script should be run under run_integration_tests.sh" 
  exit 1
fi
set -x

# First run our test under bazel. It should not be cached.
OUTPUT=$(bazel --output_base="$BAZEL_CACHE_DIR" test --config self_test //:dummy_test)
if [[ "$OUTPUT" =~ .*'(cached)'.* ]]; then
   echo "Expected first bazel run to not have test cached."
   echo "STDOUT:"
   echo "$OUTPUT"
   exit 1
fi

# Clean our local cache.
bazel --output_base="$BAZEL_CACHE_DIR" clean

# Now run it under bazel again. This time the remote cache should have it.
OUTPUT=$(bazel --output_base="$BAZEL_CACHE_DIR" test --config self_test //:dummy_test)
if [[ ! "$OUTPUT" =~ .*'(cached)'.* ]]; then
   echo "Expected second bazel run to have test cached."
   echo "STDOUT:"
   echo "$OUTPUT"
   exit 1
fi
