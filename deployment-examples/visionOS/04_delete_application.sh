#!/usr/bin/env bash

# Delete the Kustomization but leave the rest of the cluster intact.

kubectl delete -k \
    "$(git rev-parse --show-toplevel)/deployment-examples/visionOS"
