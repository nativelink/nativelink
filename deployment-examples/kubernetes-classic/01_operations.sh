# This script configures a cluster with a few standard deployments.

# TODO(aaronmondal): Add Grafana, OpenTelemetry and the various other standard
#                    deployments one would expect in a cluster.

set -xeuo pipefail

SRC_ROOT=$(git rev-parse --show-toplevel)

kubectl apply -f ${SRC_ROOT}/deployment-examples/kubernetes-classic/gateway.yaml

# The image for the scheduler and CAS.
nix run .#image.copyTo \
    docker://localhost:5001/nativelink:local \
    -- \
    --dest-tls-verify=false

# Build a base image for C++ actions.
docker buildx build \
    --platform linux/amd64 \
    -t localhost:5001/rbe-classic \
    --push \
    ${SRC_ROOT}/deployment-examples/kubernetes-classic

# Wrap it with nativelink to turn it into a worker.
nix run .#nativelink-worker-rbe-classic.copyTo \
    docker://localhost:5001/nativelink-worker-rbe-classic:local \
    -- \
    --dest-tls-verify=false
