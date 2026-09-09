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

audit_phase_timeout_seconds="${AKS_MANAGED_TELEMETRY_AUDIT_PHASE_TIMEOUT_SECONDS:-9000}"
audit_timeout_kill_after_seconds="${AKS_MANAGED_TELEMETRY_TIMEOUT_KILL_AFTER_SECONDS:-10}"
for value_name in \
  audit_phase_timeout_seconds \
  audit_timeout_kill_after_seconds; do
  value="${!value_name}"
  if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "$value_name must be a positive integer." >&2
    exit 1
  fi
done

managed_report_pair_valid() {
  jq -e \
    'type == "object" and
     (.complete | type == "boolean") and
     (.checks | type == "array")' \
    "$OUTPUT_DIR/telemetry-audit-managed.json" >/dev/null 2>&1 &&
    [ -s "$OUTPUT_DIR/telemetry-audit-managed.md" ]
}

write_managed_fallback() {
  local exit_code="$1" timed_out="$2" reason="$3"
  local fallback_json fallback_markdown
  fallback_json=$(mktemp \
    "$OUTPUT_DIR/.telemetry-audit-managed.json.XXXXXX")
  fallback_markdown=$(mktemp \
    "$OUTPUT_DIR/.telemetry-audit-managed.md.XXXXXX")
  jq -n \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --argjson exit_code "$exit_code" \
    --argjson timed_out "$timed_out" \
    --arg reason "$reason" \
    '{
      schema_version: 1,
      source: "azure-monitor-managed-prometheus",
      generated_at: $generated_at,
      complete: false,
      execution: {
        exit_code: $exit_code,
        timed_out: $timed_out,
        reason: $reason
      },
      checks: []
    }' > "$fallback_json"
  {
    echo "# AKS control-plane managed Prometheus telemetry audit"
    echo
    echo "**Complete:** no"
    echo
    echo "$reason (exit $exit_code)."
  } > "$fallback_markdown"
  mv -f "$fallback_json" "$OUTPUT_DIR/telemetry-audit-managed.json"
  mv -f "$fallback_markdown" "$OUTPUT_DIR/telemetry-audit-managed.md"
}

if [ "${AKS_MANAGED_TELEMETRY_AUDIT_PHASE_CHILD:-false}" != "true" ]; then
  : "${OUTPUT_DIR:?OUTPUT_DIR is required}"
  mkdir -p "$OUTPUT_DIR"
  rm -f \
    "$OUTPUT_DIR/telemetry-audit-managed.json" \
    "$OUTPUT_DIR/telemetry-audit-managed.md" \
    "$OUTPUT_DIR/telemetry-audit-managed-execution.json" \
    "$OUTPUT_DIR/telemetry-audit-phase-execution.json" \
    "$OUTPUT_DIR/aks-platform-export-summary.json" \
    "$OUTPUT_DIR"/aks-platform-*.json \
    "$OUTPUT_DIR"/aks-platform-*.openmetrics
  phase_timeout_marker="$OUTPUT_DIR/.telemetry-audit-phase-timeout"
  rm -f "$phase_timeout_marker"
  phase_started_at=$(date +%s)
  set +e
  timeout --signal=TERM \
    --kill-after="${audit_timeout_kill_after_seconds}s" \
    "${audit_phase_timeout_seconds}s" \
    bash -c '
      marker=$1
      shift
      process_group_has_members() {
        local stat line pid rest state ppid pgrp
        for stat in /proc/[0-9]*/stat; do
          IFS= read -r line < "$stat" || continue
          pid=${line%% *}
          rest=${line##*) }
          read -r state ppid pgrp _ <<< "$rest"
          if [ "$pgrp" = "$phase_pgid" ] &&
             [ "$pid" != "$BASHPID" ] &&
             [ "$pid" != "$timeout_pid" ] &&
             [ "$state" != "Z" ]; then
            return 0
          fi
        done
        return 1
      }
      IFS= read -r self_stat < /proc/self/stat
      self_rest=${self_stat##*) }
      read -r _ timeout_pid phase_pgid _ <<< "$self_rest"
      on_deadline() {
        printf "%s\n" deadline-reached > "$marker"
        while process_group_has_members; do
          sleep 0.1
        done
        wait "$child" 2>/dev/null || true
        exit 124
      }
      trap on_deadline TERM
      "$@" &
      child=$!
      wait "$child"
      exit $?
    ' _ "$phase_timeout_marker" \
      env AKS_MANAGED_TELEMETRY_AUDIT_PHASE_CHILD=true \
        bash "$0" "$@"
  phase_rc=$?
  set -e
  phase_finished_at=$(date +%s)
  phase_timed_out=false
  if [ -s "$phase_timeout_marker" ]; then
    phase_timed_out=true
  fi
  rm -f "$phase_timeout_marker"
  phase_execution_tmp=$(mktemp \
    "$OUTPUT_DIR/.telemetry-audit-phase-execution.json.XXXXXX")
  jq -n \
    --argjson exit_code "$phase_rc" \
    --argjson timed_out "$phase_timed_out" \
    --argjson timeout_seconds "$audit_phase_timeout_seconds" \
    --argjson elapsed_seconds "$((phase_finished_at - phase_started_at))" \
    '{
      exit_code: $exit_code,
      timed_out: $timed_out,
      timeout_seconds: $timeout_seconds,
      elapsed_seconds: $elapsed_seconds
    }' > "$phase_execution_tmp"
  mv -f \
    "$phase_execution_tmp" \
    "$OUTPUT_DIR/telemetry-audit-phase-execution.json"
  if [ "$phase_rc" -ne 0 ]; then
    if ! managed_report_pair_valid; then
      phase_failure_reason="The managed telemetry audit/export phase failed before a valid managed audit report pair was written"
      if [ "$phase_timed_out" = "true" ]; then
        phase_failure_reason="The managed telemetry audit/export phase reached its total deadline before a valid managed audit report pair was written"
      fi
      write_managed_fallback \
        "$phase_rc" \
        "$phase_timed_out" \
        "$phase_failure_reason"
    fi
    if ! jq -e 'type == "object"' \
        "$OUTPUT_DIR/aks-platform-export-summary.json" >/dev/null 2>&1; then
      platform_summary_tmp=$(mktemp \
        "$OUTPUT_DIR/.aks-platform-export-summary.json.XXXXXX")
      jq -n \
        --argjson phase_exit_code "$phase_rc" \
        --argjson phase_timed_out "$phase_timed_out" \
        --argjson total_timeout_seconds "$audit_phase_timeout_seconds" \
        '{
          complete: false,
          phase_exit_code: $phase_exit_code,
          phase_timed_out: $phase_timed_out,
          skipped: false,
          scenario_window_count: 0,
          expected_count: 0,
          success_count: 0,
          failed_count: 0,
          total_timeout_seconds: $total_timeout_seconds
        }' > "$platform_summary_tmp"
      mv -f \
        "$platform_summary_tmp" \
        "$OUTPUT_DIR/aks-platform-export-summary.json"
    fi
    if [ "$phase_timed_out" = "true" ]; then
      echo "##vso[task.logissue type=error;] Managed telemetry audit/export phase reached its ${audit_phase_timeout_seconds}s total deadline."
      exit 1
    fi
  fi
  exit "$phase_rc"
fi

initialize_managed_telemetry
load_collection_window

collection_concurrency="${AKS_CONTROL_PLANE_METRICS_CONCURRENCY:-4}"
if ! [[ "$collection_concurrency" =~ ^[1-9][0-9]*$ ]] ||
   [ "$collection_concurrency" -gt 16 ]; then
  echo "AKS_CONTROL_PLANE_METRICS_CONCURRENCY must be an integer from 1 through 16." >&2
  exit 1
fi
managed_audit_timeout_seconds="${AKS_MANAGED_PROMETHEUS_AUDIT_TIMEOUT_SECONDS:-5400}"
managed_request_timeout_seconds="${AKS_MANAGED_PROMETHEUS_REQUEST_TIMEOUT_SECONDS:-30}"
platform_export_total_timeout_seconds="${AKS_PLATFORM_EXPORT_TOTAL_TIMEOUT_SECONDS:-2700}"
platform_export_cluster_timeout_seconds="${AKS_PLATFORM_EXPORT_CLUSTER_TIMEOUT_SECONDS:-180}"
platform_az_command_timeout_seconds="${AKS_PLATFORM_AZ_COMMAND_TIMEOUT_SECONDS:-60}"
skip_platform_without_scenarios="${AKS_PLATFORM_EXPORT_SKIP_WITHOUT_SCENARIOS:-false}"
for value_name in \
  managed_audit_timeout_seconds \
  managed_request_timeout_seconds \
  platform_export_total_timeout_seconds \
  platform_export_cluster_timeout_seconds \
  platform_az_command_timeout_seconds; do
  value="${!value_name}"
  if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "$value_name must be a positive integer." >&2
    exit 1
  fi
done
if [ "${skip_platform_without_scenarios,,}" != "true" ] &&
   [ "${skip_platform_without_scenarios,,}" != "false" ]; then
  echo "AKS_PLATFORM_EXPORT_SKIP_WITHOUT_SCENARIOS must be true or false." >&2
  exit 1
fi
audit_phase_deadline=$(( $(date +%s) + audit_phase_timeout_seconds ))
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
  rm -f \
    "$capacity_raw" \
    "${capacity_raw}.tmp" \
    "$capacity_summary" \
    "${capacity_summary}.tmp" \
    "$workspace_dir/amw-capacity-summary.md"
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

workspace_slots_file="$audit_work_state/workspace-slots.json"
collected_capacity_file="$audit_work_state/collected-capacity-audits.json"
printf '%s' "$workspaces_json" |
  jq '
    [.[] | {
      slot: (.slot // .name),
      resource_id: .id
    }]
    | sort_by(.slot)
  ' > "$workspace_slots_file"
jq '(.capacity_audits // [])' \
  "$collection_manifest" > "$collected_capacity_file"
reuse_collected_capacity=false
if jq -e \
    --slurpfile expected_workspaces "$workspace_slots_file" \
    '
      type == "array" and
      ([.[] | {
        slot: .slot,
        resource_id: .summary.resource_id
      }] | sort_by(.slot)) == $expected_workspaces[0] and
      all(.[];
        (.status | type == "number") and
        (.summary | type == "object") and
        .summary.query_succeeded == true and
        .summary.capacity_samples_complete == true)
    ' "$collected_capacity_file" >/dev/null; then
  reuse_collected_capacity=true
fi

if [ "$reuse_collected_capacity" = "true" ]; then
  collected_capacity_count=$(jq 'length' "$collected_capacity_file")
  echo "Reusing $collected_capacity_count complete post-workload AMW capacity audit(s) from the collection manifest."
  while IFS= read -r capacity_audit; do
    workspace_slot=$(echo "$capacity_audit" | jq -r '.slot')
    workspace_dir="$OUTPUT_DIR/workspace-${workspace_slot}"
    capacity_summary="$workspace_dir/amw-capacity-summary.json"
    mkdir -p "$workspace_dir"
    echo "$capacity_audit" |
      jq '.summary' > "${capacity_summary}.tmp"
    mv -f "${capacity_summary}.tmp" "$capacity_summary"
    write_amw_capacity_markdown \
      "$capacity_summary" \
      "$workspace_dir/amw-capacity-summary.md"
    capacity_status=0
    amw_capacity_runtime_ok "$capacity_summary" || capacity_status=$?
    if [ "$capacity_status" -ne 0 ]; then
      capacity_audit_ok=false
      echo "##vso[task.logissue type=error;] Collected AMW capacity audit failed for workspace slot $workspace_slot (status=$capacity_status)."
    fi
  done < <(jq -c '.[]' "$collected_capacity_file")
else
  echo "Collected AMW capacity proof is incomplete; recapturing live workspace capacity."
  for workspace_dir in "$OUTPUT_DIR"/workspace-*; do
    [ -d "$workspace_dir" ] || continue
    rm -f \
      "$workspace_dir/amw-capacity.json" \
      "$workspace_dir/amw-capacity.json.tmp" \
      "$workspace_dir/amw-capacity-summary.json" \
      "$workspace_dir/amw-capacity-summary.json.tmp" \
      "$workspace_dir/amw-capacity-summary.md"
  done
  capacity_batch=0
  mapfile -t capacity_workspace_rows < <(
    echo "$workspaces_json" | jq -c '.[]'
  )
  for workspace in "${capacity_workspace_rows[@]}"; do
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
  done
  wait

  for workspace in "${capacity_workspace_rows[@]}"; do
    workspace_slot=$(echo "$workspace" | jq -r '.slot // .name')
    workspace_key=$(printf '%s' "$workspace_slot" | sed -E 's/[^a-zA-Z0-9_.-]+/_/g')
    cat "$audit_work_state/capacity-${workspace_key}.log" 2>/dev/null || true
    if [ "$(cat "$audit_work_state/capacity-${workspace_key}.status" 2>/dev/null || echo fail)" != "ok" ]; then
      capacity_audit_ok=false
      echo "##vso[task.logissue type=error;] AMW capacity audit failed for workspace slot $workspace_slot."
    fi
  done
fi
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

remaining=$((audit_phase_deadline - $(date +%s)))
if [ "$remaining" -lt 0 ]; then
  remaining=0
fi
effective_managed_audit_timeout="$managed_audit_timeout_seconds"
if [ "$effective_managed_audit_timeout" -gt "$remaining" ]; then
  effective_managed_audit_timeout="$remaining"
fi
managed_timeout_marker="$audit_work_state/managed-audit-timeout"
managed_timed_out=false
rm -f \
  "$OUTPUT_DIR/telemetry-audit-managed.json" \
  "$OUTPUT_DIR/telemetry-audit-managed.md"
set +e
if [ "$effective_managed_audit_timeout" -le 0 ]; then
  printf '%s\n' phase-budget-exhausted > "$managed_timeout_marker"
  audit_rc=124
else
  timeout --signal=TERM \
    --kill-after="${audit_timeout_kill_after_seconds}s" \
    "${effective_managed_audit_timeout}s" \
    bash -c '
      marker=$1
      shift
      on_deadline() {
        printf "%s\n" deadline-reached > "$marker"
      }
      trap on_deadline TERM
      "$@" &
      child=$!
      while true; do
        wait "$child"
        child_rc=$?
        if ! kill -0 "$child" 2>/dev/null; then
          exit "$child_rc"
        fi
      done
    ' _ "$managed_timeout_marker" \
    python3 "$AUDIT_SCRIPT" managed \
      --endpoint "$endpoint" \
      --resource-scope "$resource_scope" \
      --manifest "$MANIFEST_PATH" \
      --start "$audit_start" \
      --end "$end_time" \
      --output-prefix "$OUTPUT_DIR/telemetry-audit-managed" \
      --workers "$audit_workers" \
      --request-timeout-seconds "$managed_request_timeout_seconds"
  audit_rc=$?
fi
set -e
unset PROMETHEUS_BEARER_TOKEN
if [ -s "$managed_timeout_marker" ]; then
  managed_timed_out=true
fi

managed_report_valid=true
if ! managed_report_pair_valid; then
  managed_report_valid=false
fi
managed_fallback_written=false
if [ "$managed_report_valid" != "true" ]; then
  managed_fallback_written=true
  write_managed_fallback \
    "$audit_rc" \
    "$managed_timed_out" \
    "Audit execution ended before a valid report pair was written"
fi
jq -n \
  --argjson exit_code "$audit_rc" \
  --argjson timed_out "$managed_timed_out" \
  --argjson timeout_seconds "$effective_managed_audit_timeout" \
  --argjson request_timeout_seconds "$managed_request_timeout_seconds" \
  --argjson workers "$audit_workers" \
  --argjson report_valid "$managed_report_valid" \
  --argjson fallback_written "$managed_fallback_written" \
  '{
    exit_code: $exit_code,
    timed_out: $timed_out,
    timeout_seconds: $timeout_seconds,
    request_timeout_seconds: $request_timeout_seconds,
    workers: $workers,
    report_valid: $report_valid,
    fallback_written: $fallback_written
  }' > "$OUTPUT_DIR/telemetry-audit-managed-execution.json"
if [ "$audit_rc" -ne 0 ] || [ "$managed_report_valid" != "true" ]; then
  echo "##vso[task.logissue type=warning;] Managed Prometheus telemetry audit returned $audit_rc with report_valid=$managed_report_valid; inspect the published audit."
fi
managed_audit_incomplete=false
if [ "$audit_rc" -ne 0 ] ||
   [ "$managed_report_valid" != "true" ] ||
   ! jq -e '.complete == true' \
      "$OUTPUT_DIR/telemetry-audit-managed.json" >/dev/null 2>&1; then
  managed_audit_incomplete=true
fi

platform_export_state="$audit_work_state/platform"
mkdir -p "$platform_export_state"
jq -c '.clusters[]' "$MANIFEST_PATH" > "$platform_export_state/clusters.jsonl"
platform_cluster_count=$(wc -l < "$platform_export_state/clusters.jsonl")
scenario_window_count=$(jq '(.scenario_windows // []) | length' "$collection_manifest")
platform_metrics_required="${AKS_PLATFORM_METRICS_REQUIRED:-false}"
if [ "${platform_metrics_required,,}" != "true" ] &&
   [ "${platform_metrics_required,,}" != "false" ]; then
  echo "AKS_PLATFORM_METRICS_REQUIRED must be true or false." >&2
  exit 1
fi
platform_export_skipped=false
if [ "${skip_platform_without_scenarios,,}" = "true" ] &&
   [ "${platform_metrics_required,,}" != "true" ] &&
   [ "$scenario_window_count" -eq 0 ]; then
  platform_export_skipped=true
  echo "No scenario windows were recorded; skipping optional per-cluster platform export."
fi

export_platform_cluster() {
  local cluster="$1" timeout_seconds="$2" role cluster_id cluster_alias
  local exporter_timeout_seconds
  role=$(echo "$cluster" | jq -r '.role')
  cluster_id=$(echo "$cluster" | jq -r '.id')
  cluster_alias=$(echo "$cluster" | jq -r '.prometheus_cluster_alias')
  exporter_timeout_seconds=$((
    timeout_seconds - audit_timeout_kill_after_seconds - 5
  ))
  timeout --signal=TERM \
    --kill-after="${audit_timeout_kill_after_seconds}s" \
    "${timeout_seconds}s" \
    python3 "$PLATFORM_EXPORT_SCRIPT" \
      --resource "$cluster_id" \
      --cluster-label "$cluster_alias" \
      --start "$configured_at" \
      --end "$end_time" \
      --output "$OUTPUT_DIR/aks-platform-${role}.openmetrics" \
      --manifest "$OUTPUT_DIR/aks-platform-${role}.json" \
      --command-timeout-seconds "$platform_az_command_timeout_seconds" \
      --total-timeout-seconds "$exporter_timeout_seconds"
}

platform_export_deadline=$((
  $(date +%s) + platform_export_total_timeout_seconds
))
if [ "$platform_export_deadline" -gt "$audit_phase_deadline" ]; then
  platform_export_deadline="$audit_phase_deadline"
fi
if [ "$platform_export_skipped" != "true" ]; then
  echo "Exporting live platform metrics for ${platform_cluster_count} cluster(s), concurrency=${collection_concurrency}, total_timeout=${platform_export_total_timeout_seconds}s"
  platform_batch=0
  while IFS= read -r cluster; do
    [ -n "$cluster" ] || continue
    role=$(echo "$cluster" | jq -r '.role')
    remaining=$((platform_export_deadline - $(date +%s)))
    if [ "$remaining" -le 0 ]; then
      echo budget_exhausted > "$platform_export_state/${role}.status"
      continue
    fi
    cluster_timeout="$platform_export_cluster_timeout_seconds"
    if [ "$cluster_timeout" -gt "$remaining" ]; then
      cluster_timeout="$remaining"
    fi
    if [ "$cluster_timeout" -le $((audit_timeout_kill_after_seconds + 5)) ]; then
      echo budget_exhausted > "$platform_export_state/${role}.status"
      continue
    fi
    (
      if export_platform_cluster "$cluster" "$cluster_timeout" \
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
fi

platform_export_ok=true
platform_export_success_count=0
platform_export_failed_count=0
platform_export_failed_roles=()
while IFS= read -r cluster; do
  [ -n "$cluster" ] || continue
  role=$(echo "$cluster" | jq -r '.role')
  if [ "$platform_export_skipped" = "true" ]; then
    continue
  fi
  status=$(cat "$platform_export_state/${role}.status" 2>/dev/null || echo fail)
  if [ "$status" = "ok" ]; then
    platform_export_success_count=$((platform_export_success_count + 1))
  else
    platform_export_failed_count=$((platform_export_failed_count + 1))
    platform_export_failed_roles+=("$role")
    platform_export_ok=false
    platform_issue_type=warning
    if [ "${platform_metrics_required,,}" = "true" ]; then
      platform_issue_type=error
    fi
    echo "##vso[task.logissue type=${platform_issue_type};] Platform metric export failed for ${role} (status=$status); log tail:"
    tail -50 "$platform_export_state/${role}.log" 2>/dev/null || true
  fi
done < "$platform_export_state/clusters.jsonl"
platform_export_failed_roles_json=$(
  printf '%s\n' "${platform_export_failed_roles[@]}" |
    jq -Rsc 'split("\n") | map(select(length > 0))'
)
jq -n \
  --argjson skipped "$platform_export_skipped" \
  --argjson scenario_window_count "$scenario_window_count" \
  --argjson expected_count "$platform_cluster_count" \
  --argjson success_count "$platform_export_success_count" \
  --argjson failed_count "$platform_export_failed_count" \
  --argjson failed_roles "$platform_export_failed_roles_json" \
  --argjson total_timeout_seconds "$platform_export_total_timeout_seconds" \
  --argjson cluster_timeout_seconds "$platform_export_cluster_timeout_seconds" \
  --argjson command_timeout_seconds "$platform_az_command_timeout_seconds" \
  '{
    skipped: $skipped,
    scenario_window_count: $scenario_window_count,
    expected_count: $expected_count,
    success_count: $success_count,
    failed_count: $failed_count,
    failed_roles: $failed_roles,
    complete: ($skipped or $failed_count == 0),
    partial: (($skipped | not) and $success_count > 0 and $failed_count > 0),
    total_timeout_seconds: $total_timeout_seconds,
    cluster_timeout_seconds: $cluster_timeout_seconds,
    command_timeout_seconds: $command_timeout_seconds
  }' > "$OUTPUT_DIR/aks-platform-export-summary.json"

echo "Managed telemetry audit and live-coupled platform metrics written to $OUTPUT_DIR"
if [ "$capacity_audit_ok" != "true" ]; then
  exit 1
fi
if [ "${platform_metrics_required,,}" = "true" ] &&
   [ "$platform_export_ok" != "true" ]; then
  exit 1
fi
if [ "$managed_audit_incomplete" = "true" ] ||
   [ "$platform_export_ok" != "true" ]; then
  echo "##vso[task.complete result=SucceededWithIssues;]Managed telemetry preserved with optional gaps; inspect the published audit and platform export summary."
fi
