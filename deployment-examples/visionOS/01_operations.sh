#!/usr/bin/env bash

# Trigger cluster-internal pipelines to build or fetch necessary images.

set -xeuo pipefail

curl -v \
    -H 'content-Type: application/json' \
    -d '{"flakeOutput": "./src_root#image"}' \
    localhost:8082/eventlistener

curl -v \
    -H 'content-Type: application/json' \
    -d '{"flakeOutput": "./src_root#nativelink-worker-init"}' \
    localhost:8082/eventlistener

curl -v \
    -H 'content-Type: application/json' \
    -d '{"flakeOutput": "./src_root#nativelink-worker-siso-visionOS"}' \
    localhost:8082/eventlistener

until kubectl get pipelinerun \
        -l tekton.dev/pipeline=rebuild-nativelink | grep -q 'NAME'; do
    echo "Waiting for PipelineRuns to start..."
    sleep 0.1
done

printf "Waiting for PipelineRuns to finish...

You may cancel this script now and use 'tkn pr ls' and 'tkn pr logs -f' to
monitor the PipelineRun logs.

"

kubectl wait \
    --for=condition=Succeeded \
    --timeout=45m \
    pipelinerun \
        -l tekton.dev/pipeline=rebuild-nativelink
