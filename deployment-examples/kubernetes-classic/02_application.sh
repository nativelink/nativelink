# Get the nix derivation hash from the toolchain container, change the
# `TOOLCHAIN_TAG` variable in the `worker.json.template` to that hash and apply
# the configuration.

KUSTOMIZE_DIR=$(git rev-parse --show-toplevel)/deployment-examples/kubernetes-classic

kubectl apply -k "$KUSTOMIZE_DIR"

kubectl rollout status deploy/nativelink-cas
kubectl rollout status deploy/nativelink-scheduler
kubectl rollout status deploy/nativelink-worker
