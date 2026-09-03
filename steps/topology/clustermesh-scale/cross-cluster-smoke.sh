#!/usr/bin/env bash

set -euo pipefail
set -x

cross_cluster_smoke_enabled="${CLUSTERMESH_CROSS_CLUSTER_SMOKE_ENABLED:-true}"
if [ "${cross_cluster_smoke_enabled,,}" != "true" ]; then
  echo "Cross-cluster data-path smoke disabled by CLUSTERMESH_CROSS_CLUSTER_SMOKE_ENABLED=$cross_cluster_smoke_enabled"
  exit 0
fi

# Deploy a global Service backed by an echo Pod in the first cluster, then curl
# it from the second cluster. This proves the data path, not only control-plane
# convergence.
clusters=$(cat "$HOME/.kube/clustermesh-clusters.json")
smoke_cluster_count=$(echo "$clusters" | jq 'length')
if [ "$smoke_cluster_count" -lt 2 ]; then
  echo "Cross-cluster data-path smoke: single cluster ($smoke_cluster_count) - no remote peer to reach; skipping."
  exit 0
fi
first_role=$(echo "$clusters" | jq -r '.[0].role')
second_role=$(echo "$clusters" | jq -r '.[1].role')

kc_first="$HOME/.kube/$first_role.config"
kc_second="$HOME/.kube/$second_role.config"
namespace="cm-smoke"
server_manifest="/tmp/cm-smoke-server.yaml"
client_manifest="/tmp/cm-smoke-client.yaml"

cleanup() {
  KUBECONFIG="$kc_first" kubectl delete ns "$namespace" \
    --ignore-not-found --wait=true --timeout=60s || true
  KUBECONFIG="$kc_second" kubectl delete ns "$namespace" \
    --ignore-not-found --wait=true --timeout=60s || true
  rm -f "$server_manifest" "$client_manifest"
}
trap cleanup EXIT

cat <<'EOF' > "$server_manifest"
apiVersion: v1
kind: Namespace
metadata:
  name: cm-smoke
  annotations:
    # AKS managed Cilium gates ClusterMesh synchronization at namespace scope.
    clustermesh.cilium.io/global: "true"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: echo
  namespace: cm-smoke
spec:
  replicas: 1
  selector:
    matchLabels: { app: echo }
  template:
    metadata:
      labels: { app: echo }
    spec:
      containers:
        - name: echo
          image: registry.k8s.io/e2e-test-images/agnhost:2.47
          args: ["netexec", "--http-port=8080"]
          ports: [{ containerPort: 8080 }]
---
apiVersion: v1
kind: Service
metadata:
  name: echo
  namespace: cm-smoke
  annotations:
    service.cilium.io/global: "true"
spec:
  selector: { app: echo }
  ports:
    - port: 80
      targetPort: 8080
EOF

cat <<'EOF' > "$client_manifest"
apiVersion: v1
kind: Namespace
metadata:
  name: cm-smoke
  annotations:
    clustermesh.cilium.io/global: "true"
---
# Cilium global services require the same Service name in each participating
# cluster. This Service has no local backend and resolves to the first cluster.
apiVersion: v1
kind: Service
metadata:
  name: echo
  namespace: cm-smoke
  annotations:
    service.cilium.io/global: "true"
spec:
  selector: { app: echo }
  ports:
    - port: 80
      targetPort: 8080
---
apiVersion: v1
kind: Pod
metadata:
  name: curl
  namespace: cm-smoke
  labels: { app: curl }
spec:
  restartPolicy: Never
  containers:
    - name: curl
      image: curlimages/curl:8.10.1
      command: ["sleep", "600"]
EOF

KUBECONFIG="$kc_first" kubectl apply -f "$server_manifest"
KUBECONFIG="$kc_second" kubectl apply -f "$client_manifest"

KUBECONFIG="$kc_first" kubectl -n "$namespace" \
  rollout status deploy/echo --timeout=3m
KUBECONFIG="$kc_second" kubectl -n "$namespace" \
  wait --for=condition=Ready pod/curl --timeout=3m

sleep 15

ok=0
for attempt in $(seq 1 24); do
  if KUBECONFIG="$kc_second" kubectl -n "$namespace" exec curl -- \
      curl -fsS -m 5 http://echo.cm-smoke.svc.cluster.local/hostname; then
    ok=1
    echo ""
    echo "Cross-cluster curl succeeded on attempt $attempt"
    break
  fi
  echo "  attempt $attempt/24 failed, retrying in 5s..."
  sleep 5
done

if [ "$ok" -ne 1 ]; then
  echo "##vso[task.logissue type=error;] Cross-cluster data-path smoke failed: $second_role could not reach service in $first_role"
  exit 1
fi
