# This script sets up a local development cluster. It's roughly equivalent to
# a managed K8s setup.

# For ease of development and to save disk space we pipe a local container
# registry through to kind.
#
# See https://kind.sigs.k8s.io/docs/user/local-registry/.

set -xeuo pipefail

reg_name='kind-registry'
reg_port='5001'
if [ "$(docker inspect -f '{{.State.Running}}' "${reg_name}" 2>/dev/null || true)" != 'true' ]; then
  docker run \
    -d --restart=always -p "127.0.0.1:${reg_port}:5000" --network bridge --name "${reg_name}" \
    registry:2
fi

# Start a basic cluster. We use cilium's CNI and eBPF kube-proxy replacement.

cat <<EOF |  kind create cluster --config -
---
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
networking:
  disableDefaultCNI: true
  kubeProxyMode: none
containerdConfigPatches:
  - |-
    [plugins."io.containerd.grpc.v1.cri".registry]
      config_path = "/etc/containerd/certs.d"
EOF

# Enable the registry on the nodes.

REGISTRY_DIR="/etc/containerd/certs.d/localhost:${reg_port}"
for node in $(kind get nodes); do
  docker exec "${node}" mkdir -p "${REGISTRY_DIR}"
  cat <<EOF | docker exec -i "${node}" cp /dev/stdin "${REGISTRY_DIR}/hosts.toml"
[host."http://${reg_name}:5000"]
EOF
done

# Connect the registry to the cluster network.

if [ "$(docker inspect -f='{{json .NetworkSettings.Networks.kind}}' "${reg_name}")" = 'null' ]; then
  docker network connect "kind" "${reg_name}"
fi

# Advertise the registry location.

cat <<EOF | kubectl apply -f -
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "localhost:${reg_port}"
    help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
EOF

# Prepare Gateway API CRDs. These MUST be available before we start cilium.

kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.0.0/experimental-install.yaml

kubectl wait --for condition=Established crd/gatewayclasses.gateway.networking.k8s.io
kubectl wait --for condition=Established crd/gateways.gateway.networking.k8s.io
kubectl wait --for condition=Established crd/httproutes.gateway.networking.k8s.io
kubectl wait --for condition=Established crd/tlsroutes.gateway.networking.k8s.io
kubectl wait --for condition=Established crd/grpcroutes.gateway.networking.k8s.io
kubectl wait --for condition=Established crd/referencegrants.gateway.networking.k8s.io

# Start cilium.

helm repo add cilium https://helm.cilium.io
helm repo update cilium
helm upgrade \
    --install cilium cilium/cilium \
    --version 1.14.5 \
    --namespace kube-system \
    --set k8sServiceHost=kind-control-plane \
    --set k8sServicePort=6443 \
    --set kubeProxyReplacement=strict \
    --set gatewayAPI.enabled=true \
    --set l2announcements.enabled=true \
    --wait

# Kind's nodes are containers running on the local docker network. We reuse that
# network for LB-IPAM so that LoadBalancers are available via "real" local IPs.

KIND_NET_CIDR=$(docker network inspect kind -f '{{(index .IPAM.Config 0).Subnet}}')
CILIUM_IP_CIDR=$(echo ${KIND_NET_CIDR} | sed "s@0.0/16@255.0/28@")

cat <<EOF | kubectl apply -f -
---
apiVersion: cilium.io/v2alpha1
kind: CiliumL2AnnouncementPolicy
metadata:
  name: l2-announcements
spec:
  externalIPs: true
  loadBalancerIPs: true
---
apiVersion: cilium.io/v2alpha1
kind: CiliumLoadBalancerIPPool
metadata:
  name: default-pool
spec:
  cidrs:
    - cidr: ${CILIUM_IP_CIDR}
EOF

# At this point we have a similar setup to the one that we'd get with a cloud
# provider. Move on to `01_operations.sh` for the cluster setup.
