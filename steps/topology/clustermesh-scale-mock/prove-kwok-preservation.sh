#!/usr/bin/env bash

set -euo pipefail

mode="${1:-phase1}"
artifact_dir="${KWOK_PRESERVATION_ARTIFACT_DIR:?KWOK_PRESERVATION_ARTIFACT_DIR is required}"
repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
inventory="$HOME/.kube/clustermesh-clusters.json"
cluster_file="$artifact_dir/clusters.json"
context_file="$artifact_dir/context.json"
provision_script="$repository_root/scenarios/perf-eval/clustermesh-scale/mock/provision-kwok-layer.sh"
reconcile_script="$repository_root/modules/python/clusterloader2/clustermesh-scale/mock_layer_reconcile.py"
acr_host="${MOCK_ACR_HOST:?MOCK_ACR_HOST is required}"
agent_tag="${MOCK_AGENT_TAG:-v26}"
expected_count="${MOCK_NODE_COUNT:-100}"
run_id="${RUN_ID:?RUN_ID is required}"
roles=(mesh-1 mesh-2)
deleted_agents=(kwok-node-0 kwok-node-1 kwok-node-2 kwok-node-3 kwok-node-4)
deleted_nodes=(kwok-node-10 kwok-node-11 kwok-node-12 kwok-node-13 kwok-node-14)

mkdir -p "$artifact_dir"

log() {
  printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" |
    tee -a "$artifact_dir/kwok-preservation.log"
}

kubeconfig_for_role() {
  jq -r --arg role "$1" '.[] | select(.role == $role) | .kubeconfig' "$cluster_file"
}

cluster_name_for_role() {
  jq -r --arg role "$1" '.[] | select(.role == $role) | .name' "$cluster_file"
}

capture_uid_maps() {
  local role="$1"
  local suffix="$2"
  local kubeconfig
  kubeconfig=$(kubeconfig_for_role "$role")
  kubectl --kubeconfig "$kubeconfig" get nodes -l type=kwok -o json |
    jq '[.items[] | {name:.metadata.name,uid:.metadata.uid}] | sort_by(.name)' \
      > "$artifact_dir/$role-nodes-$suffix.json"
  kubectl --kubeconfig "$kubeconfig" -n mock-clustermesh \
    get pods -l app=mock-cilium-agent -o json |
    jq '[.items[] | {name:.metadata.name,uid:.metadata.uid,phase:.status.phase}] | sort_by(.name)' \
      > "$artifact_dir/$role-agents-$suffix.json"
}

wait_mock_ready() {
  local role="$1"
  local kubeconfig deadline nodes running ready
  kubeconfig=$(kubeconfig_for_role "$role")
  deadline=$(( $(date +%s) + 900 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    nodes=$(kubectl --kubeconfig "$kubeconfig" get nodes -l type=kwok \
      --no-headers 2>/dev/null | wc -l)
    running=$(kubectl --kubeconfig "$kubeconfig" -n mock-clustermesh \
      get pods -l app=mock-cilium-agent \
      --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l)
    ready=$(kubectl --kubeconfig "$kubeconfig" -n mock-clustermesh \
      get statefulset kwok-node -o jsonpath='{.status.readyReplicas}' \
      2>/dev/null || echo 0)
    ready="${ready:-0}"
    log "$role mock readiness nodes=$nodes/$expected_count running=$running/$expected_count statefulset=$ready/$expected_count"
    if [ "$nodes" -eq "$expected_count" ] &&
      [ "$running" -eq "$expected_count" ] &&
      [ "$ready" -eq "$expected_count" ]; then
      return 0
    fi
    sleep 15
  done
  return 1
}

wait_real_mesh() {
  local role="$1"
  local kubeconfig deadline status
  kubeconfig=$(kubeconfig_for_role "$role")
  deadline=$(( $(date +%s) + 600 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    status=$(kubectl --kubeconfig "$kubeconfig" -n kube-system \
      exec daemonset/cilium -- cilium-dbg status 2>/dev/null || true)
    if grep -Eq 'ClusterMesh:[[:space:]]+1/1 remote clusters ready' <<< "$status"; then
      return 0
    fi
    sleep 15
  done
  return 1
}

build_cluster_file() {
  jq --arg kube_root "$HOME/.kube" '
    map(. + {kubeconfig: ($kube_root + "/" + .role + ".config")})
    | sort_by(.role)
  ' "$inventory" > "$cluster_file"
  if [ "$(jq 'length' "$cluster_file")" -ne 2 ]; then
    log "expected exactly two clusters in $inventory"
    exit 1
  fi
}

capture_aks_ids() {
  local output="$1"
  jq -r '.[].rg' "$cluster_file" | sort -u |
    while IFS= read -r resource_group; do
      az aks list --resource-group "$resource_group" \
        --query '[].{name:name,id:id}' -o json
    done |
    jq -s 'add | sort_by(.name)' > "$output"
}

run_resume_apply() {
  local state_root="$1"
  local pids=() index role kubeconfig
  rm -rf "$state_root"
  mkdir -p "$state_root"
  for role in "${roles[@]}"; do
    kubeconfig=$(kubeconfig_for_role "$role")
    (
      KUBECONFIG_FILE="$kubeconfig" \
      NODE_COUNT="$expected_count" \
      ACR_HOST="$acr_host" \
      AGENT_TAG="$agent_tag" \
      CONSUME_CLUSTERMESH=true \
      MOCK_RUN_ID="$run_id" \
      MOCK_STATE_DIR="$state_root/$role" \
      MOCK_SETUP_MAX_ATTEMPTS=5 \
      MOCK_APPLY_MAX_ATTEMPTS=6 \
        bash "$provision_script"
    ) > "$artifact_dir/$role-resume-apply.log" 2>&1 &
    pids+=("$!")
  done
  for index in 0 1; do
    if ! wait "${pids[$index]}"; then
      role="${roles[$index]}"
      log "$role idempotent resume apply failed"
      tail -100 "$artifact_dir/$role-resume-apply.log"
      exit 1
    fi
  done
}

compare_recovery_uids() {
  python3 - "$artifact_dir" <<'PY'
import json
import os
import sys

root = sys.argv[1]
roles = ("mesh-1", "mesh-2")
deleted_agents = {f"kwok-node-{index}" for index in range(5)}
deleted_nodes = {f"kwok-node-{index}" for index in range(10, 15)}


def load_map(path):
    with open(path, encoding="utf-8") as handle:
        return {row["name"]: row["uid"] for row in json.load(handle)}


for role in roles:
    before_nodes = load_map(os.path.join(root, f"{role}-nodes-phase1.json"))
    final_nodes = load_map(os.path.join(root, f"{role}-nodes-final.json"))
    before_agents = load_map(os.path.join(root, f"{role}-agents-phase1.json"))
    final_agents = load_map(os.path.join(root, f"{role}-agents-final.json"))
    if set(before_nodes) != set(final_nodes) or len(final_nodes) != 100:
        raise SystemExit(f"{role}: final KWOK Node set is not exact")
    if set(before_agents) != set(final_agents) or len(final_agents) != 100:
        raise SystemExit(f"{role}: final mock-agent set is not exact")
    for name in before_nodes:
        changed = before_nodes[name] != final_nodes[name]
        if changed != (name in deleted_nodes):
            raise SystemExit(f"{role}: unexpected KWOK UID result for {name}")
    for name in before_agents:
        changed = before_agents[name] != final_agents[name]
        if changed != (name in deleted_agents):
            raise SystemExit(f"{role}: unexpected mock-agent UID result for {name}")
PY
}

phase_two() {
  local role cluster kubeconfig state_root="$artifact_dir/resume-state"

  log "phase two started in a fresh Bash process"
  for role in "${roles[@]}"; do
    cluster=$(cluster_name_for_role "$role")
    kubeconfig=$(kubeconfig_for_role "$role")
    resource_group=$(jq -r --arg role "$role" \
      '.[] | select(.role == $role) | .rg' "$cluster_file")
    az aks get-credentials \
      --resource-group "$resource_group" \
      --name "$cluster" \
      --file "$kubeconfig" \
      --overwrite-existing --only-show-errors >/dev/null
  done

  capture_aks_ids "$artifact_dir/aks-ids-phase2.json"
  cmp -s "$artifact_dir/aks-ids-phase1.json" "$artifact_dir/aks-ids-phase2.json"
  for role in "${roles[@]}"; do
    capture_uid_maps "$role" "phase2-before"
    cmp -s "$artifact_dir/$role-nodes-phase1.json" \
      "$artifact_dir/$role-nodes-phase2-before.json"
    cmp -s "$artifact_dir/$role-agents-phase1.json" \
      "$artifact_dir/$role-agents-phase2-before.json"
  done
  log "all 200 KWOK Node and 200 mock-agent UIDs survived the process boundary"

  run_resume_apply "$state_root"
  for role in "${roles[@]}"; do
    wait_mock_ready "$role"
    capture_uid_maps "$role" "reapplied"
    cmp -s "$artifact_dir/$role-nodes-phase1.json" \
      "$artifact_dir/$role-nodes-reapplied.json"
    cmp -s "$artifact_dir/$role-agents-phase1.json" \
      "$artifact_dir/$role-agents-reapplied.json"
  done
  log "idempotent resume apply preserved every existing UID"

  for role in "${roles[@]}"; do
    kubeconfig=$(kubeconfig_for_role "$role")
    kubectl --kubeconfig "$kubeconfig" delete node \
      "${deleted_nodes[@]}" --wait=true --timeout=180s >/dev/null
    kubectl --kubeconfig "$kubeconfig" -n mock-clustermesh delete pod \
      "${deleted_agents[@]}" --wait=false --grace-period=0 --force >/dev/null
  done
  log "injected loss: five KWOK Nodes and five mock-agent Pods per cluster"

  python3 "$reconcile_script" \
    --clusters "$cluster_file" \
    --state-root "$state_root" \
    --expected-mock-count "$expected_count" \
    --run-id "$run_id" \
    --summary-file "$artifact_dir/mock-reconcile-summary.json" \
    --diagnostics-dir "$artifact_dir/mock-reconcile-diagnostics" \
    --max-concurrent 2 \
    --attempts 6 \
    --settle-seconds 15 \
    --request-timeout-seconds 45

  for role in "${roles[@]}"; do
    wait_mock_ready "$role"
    wait_real_mesh "$role"
    capture_uid_maps "$role" "final"
  done
  compare_recovery_uids

  jq -n \
    --arg run_id "$run_id" \
    --arg result passed \
    '{
      run_id:$run_id,
      result:$result,
      aks_ids_preserved:true,
      phase_boundary_kwok_uids_preserved:200,
      phase_boundary_agent_uids_preserved:200,
      idempotent_resume_preserved_all_uids:true,
      injected_missing_kwok_nodes:10,
      injected_deleted_agent_pods:10,
      exact_reconcile:true,
      real_clustermesh_healthy:true
    }' > "$artifact_dir/summary.json"
  log "KWOK preservation and exact recovery proof passed"
}

if [ "$mode" = "phase2" ]; then
  phase_two
  exit 0
fi
if [ "$mode" != "phase1" ]; then
  echo "Unsupported mode: $mode" >&2
  exit 2
fi

build_cluster_file
for role in "${roles[@]}"; do
  wait_mock_ready "$role"
  wait_real_mesh "$role"
  capture_uid_maps "$role" "phase1"
done
capture_aks_ids "$artifact_dir/aks-ids-phase1.json"
log "phase one captured exact live identities; launching a fresh process"

KWOK_PRESERVATION_ARTIFACT_DIR="$artifact_dir" \
  bash "$0" phase2
