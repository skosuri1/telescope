#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=managed-prometheus-common.sh
source "$script_dir/managed-prometheus-common.sh"

if ! managed_telemetry_enabled; then
  echo "AKS control-plane managed Prometheus is disabled; skipping audit."
  exit 0
fi

: "${AUDIT_SCRIPT:?AUDIT_SCRIPT is required}"
: "${PLATFORM_EXPORT_SCRIPT:?PLATFORM_EXPORT_SCRIPT is required}"

initialize_managed_telemetry
load_collection_window

collection_concurrency="${AKS_CONTROL_PLANE_METRICS_CONCURRENCY:-4}"
if ! [[ "$collection_concurrency" =~ ^[1-9][0-9]*$ ]] ||
   [ "$collection_concurrency" -gt 16 ]; then
  echo "AKS_CONTROL_PLANE_METRICS_CONCURRENCY must be an integer from 1 through 16." >&2
  exit 1
fi
audit_work_state=$(mktemp -d)
trap 'rm -rf "$audit_work_state"' EXIT

capacity_audit_ok=true
capacity_end=$(date -u +%Y-%m-%dT%H:%M:%SZ)
capture_workspace_capacity() {
  local workspace="$1" workspace_slot workspace_id capacity_window_start
  local workspace_dir capacity_raw capacity_summary capacity_status=0
  workspace_slot=$(echo "$workspace" | jq -r '.slot // .name')
  workspace_id=$(echo "$workspace" | jq -r '.id')
  capacity_window_start=$(echo "$workspace" | jq -r \
    '.capacity_guard.monitoring_window_start // empty')
  if [ -z "$capacity_window_start" ]; then
    capacity_window_start="$configured_at"
  fi
  workspace_dir="$OUTPUT_DIR/workspace-${workspace_slot}"
  mkdir -p "$workspace_dir"
  capacity_raw="$workspace_dir/amw-capacity.json"
  capacity_summary="$workspace_dir/amw-capacity-summary.json"
  if ! capture_amw_capacity \
      "$workspace_id" \
      "$capacity_window_start" \
      "$capacity_end" \
      "$capacity_raw" \
      "$capacity_summary"; then
    capacity_status=1
  else
    amw_capacity_runtime_ok "$capacity_summary" || capacity_status=$?
  fi
  if [ -s "$capacity_summary" ]; then
    write_amw_capacity_markdown \
      "$capacity_summary" \
      "$workspace_dir/amw-capacity-summary.md"
  fi
  if [ "$capacity_status" -ne 0 ]; then
    return "$capacity_status"
  fi
  return 0
}

capacity_batch=0
while IFS= read -r workspace; do
  workspace_slot=$(echo "$workspace" | jq -r '.slot // .name')
  workspace_key=$(printf '%s' "$workspace_slot" | sed -E 's/[^a-zA-Z0-9_.-]+/_/g')
  (
    if capture_workspace_capacity "$workspace" \
        > "$audit_work_state/capacity-${workspace_key}.log" 2>&1; then
      echo ok > "$audit_work_state/capacity-${workspace_key}.status"
    else
      echo fail > "$audit_work_state/capacity-${workspace_key}.status"
    fi
  ) &
  capacity_batch=$((capacity_batch + 1))
  if [ "$capacity_batch" -ge "$collection_concurrency" ]; then
    wait
    capacity_batch=0
  fi
done < <(echo "$workspaces_json" | jq -c '.[]')
wait

while IFS= read -r workspace; do
  workspace_slot=$(echo "$workspace" | jq -r '.slot // .name')
  workspace_key=$(printf '%s' "$workspace_slot" | sed -E 's/[^a-zA-Z0-9_.-]+/_/g')
  cat "$audit_work_state/capacity-${workspace_key}.log" 2>/dev/null || true
  if [ "$(cat "$audit_work_state/capacity-${workspace_key}.status" 2>/dev/null || echo fail)" != "ok" ]; then
    capacity_audit_ok=false
    echo "##vso[task.logissue type=error;] AMW capacity audit failed for workspace slot $workspace_slot."
  fi
done < <(echo "$workspaces_json" | jq -c '.[]')
echo "##vso[task.setvariable variable=AKS_AMW_CAPACITY_AUDITED]$capacity_audit_ok"

token=$(az account get-access-token \
  --resource https://prometheus.monitor.azure.com \
  --query accessToken -o tsv)
export PROMETHEUS_BEARER_TOKEN="$token"

# Bounds the ThreadPoolExecutor concurrency used for schema-v2 (one
# workspace per cluster) audits. Each cluster issues ~15 API calls
# (1 label-values + 1 /series per MANAGED_SERIES_METRICS entry), so at
# n100 scale serial execution can approach ~1500 calls and threaten the 3h
# finalization reserve. Higher worker counts trade wall-clock audit time
# against burstier concurrent load on the per-cluster query endpoints.
audit_workers="${AKS_MANAGED_PROMETHEUS_AUDIT_WORKERS:-4}"
if ! [[ "$audit_workers" =~ ^[1-9][0-9]*$ ]]; then
  echo "AKS_MANAGED_PROMETHEUS_AUDIT_WORKERS must be a positive integer." >&2
  exit 1
fi

set +e
python3 "$AUDIT_SCRIPT" managed \
  --endpoint "$endpoint" \
  --resource-scope "$resource_scope" \
  --manifest "$MANIFEST_PATH" \
  --start "$audit_start" \
  --end "$end_time" \
  --output-prefix "$OUTPUT_DIR/telemetry-audit-managed" \
  --workers "$audit_workers"
audit_rc=$?
set -e
unset PROMETHEUS_BEARER_TOKEN
if [ "$audit_rc" -ne 0 ]; then
  echo "##vso[task.logissue type=warning;] Managed Prometheus telemetry audit returned $audit_rc; inspect the published audit."
fi

platform_export_state="$audit_work_state/platform"
mkdir -p "$platform_export_state"
jq -c '.clusters[]' "$MANIFEST_PATH" > "$platform_export_state/clusters.jsonl"
platform_cluster_count=$(wc -l < "$platform_export_state/clusters.jsonl")
echo "Exporting live platform metrics for ${platform_cluster_count} cluster(s), concurrency=${collection_concurrency}"

export_platform_cluster() {
  local cluster="$1" role cluster_id cluster_alias
  role=$(echo "$cluster" | jq -r '.role')
  cluster_id=$(echo "$cluster" | jq -r '.id')
  cluster_alias=$(echo "$cluster" | jq -r '.prometheus_cluster_alias')
  python3 "$PLATFORM_EXPORT_SCRIPT" \
    --resource "$cluster_id" \
    --cluster-label "$cluster_alias" \
    --start "$configured_at" \
    --end "$end_time" \
    --output "$OUTPUT_DIR/aks-platform-${role}.openmetrics" \
    --manifest "$OUTPUT_DIR/aks-platform-${role}.json"
}

platform_batch=0
while IFS= read -r cluster; do
  [ -n "$cluster" ] || continue
  role=$(echo "$cluster" | jq -r '.role')
  (
    if export_platform_cluster "$cluster" \
        > "$platform_export_state/${role}.log" 2>&1; then
      echo ok > "$platform_export_state/${role}.status"
    else
      echo fail > "$platform_export_state/${role}.status"
    fi
  ) &
  platform_batch=$((platform_batch + 1))
  if [ "$platform_batch" -ge "$collection_concurrency" ]; then
    wait
    platform_batch=0
  fi
done < "$platform_export_state/clusters.jsonl"
wait

platform_export_ok=true
while IFS= read -r cluster; do
  [ -n "$cluster" ] || continue
  role=$(echo "$cluster" | jq -r '.role')
  status=$(cat "$platform_export_state/${role}.status" 2>/dev/null || echo fail)
  if [ "$status" != "ok" ]; then
    platform_export_ok=false
    echo "##vso[task.logissue type=error;] Platform metric export failed for ${role}; log tail:"
    tail -50 "$platform_export_state/${role}.log" 2>/dev/null || true
  fi
done < "$platform_export_state/clusters.jsonl"

echo "Managed telemetry audit and live-coupled platform metrics written to $OUTPUT_DIR"
if [ "$capacity_audit_ok" != "true" ] ||
   [ "$platform_export_ok" != "true" ]; then
  exit 1
fi
