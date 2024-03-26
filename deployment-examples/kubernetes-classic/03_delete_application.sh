# Get the nix derivation hash from the toolchain container, change the
# `TOOLCHAIN_TAG` variable in the `worker.json.template` to that hash and delete
# the configuration.

KUSTOMIZE_DIR=$(git rev-parse --show-toplevel)/deployment-examples/kubernetes-classic

kubectl delete -k "$KUSTOMIZE_DIR"
