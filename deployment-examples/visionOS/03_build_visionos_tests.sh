#!/usr/bin/env bash

set -euo pipefail

function fetch_visionOS_example() {
    cd ${HOME}
    git clone https://github.com/bazelbuild/rules_apple.git
}

# You must use macOS as it is the only supported sysetem for visionOS
# https://chromium.googlesource.com/chromium/src/+/main/docs/mac/build_instructions.md
if [[ "$(uname)" != "Darwin" ]]; then
    echo "This system is not running macOS."
    exit 0
fi

cd ${HOME}/rules_apple
bazel test //examples/visionos/HelloWorld:HelloWorldBuildTest \
  --remote_instance_name=main \
  --remote_cache=${CACHE} \
  --remote_executor=${SCHEDULER} \
  --remote_default_exec_properties=cpu_count=1
