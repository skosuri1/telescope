#!/usr/bin/env bash
set -uo pipefail

label="${1:?reconcile label is required}"
output="${2:?summary output is required}"
clusters="${3:?cluster inventory is required}"
cluster_count="${4:?full cluster count is required}"
budget="${5:?outer budget is required}"

if [ "${CL2_MOCK_MODE:-false}" != "true" ]; then
  exit 0
fi

mkdir -p "$(dirname "$output")"
target_count=$(jq 'length' "$clusters")
if [ "$target_count" -eq 0 ]; then
  echo "mock-layer-reconcile (${label}): no clusters selected"
  exit 0
fi
if [ "$target_count" -eq "$cluster_count" ] && [ "$cluster_count" -lt 50 ]; then
  concurrency="$cluster_count"
elif [ "$target_count" -gt 12 ]; then
  concurrency=12
else
  concurrency="$target_count"
fi
if [ "$cluster_count" -ge 50 ]; then
  attempts="${CL2_MOCK_RECONCILE_ATTEMPTS:-15}"
  settle_seconds="${CL2_MOCK_RECONCILE_SETTLE_SECONDS:-45}"
else
  attempts="${CL2_MOCK_RECONCILE_ATTEMPTS:-5}"
  settle_seconds="${CL2_MOCK_RECONCILE_SETTLE_SECONDS:-15}"
fi

rc=0
timeout --signal=TERM --kill-after=30s "${budget}s" \
  python3 "${MOCK_RECONCILER_SCRIPT:?mock reconciler path is required}" \
    --clusters "$clusters" \
    --state-root "$HOME/.kube/mock-layer-state/${RUN_ID:?run ID is required}" \
    --run-id "$RUN_ID" \
    --expected-mock-count "${CL2_MOCK_NODE_COUNT:?mock node count is required}" \
    --max-concurrent "$concurrency" \
    --attempts "$attempts" \
    --settle-seconds "$settle_seconds" \
    --request-timeout-seconds 30 \
    --diagnostics-dir "${CL2_REPORT_DIR:?report directory is required}/${SCENARIO:?scenario is required}/mock-layer-diagnostics/${label}" \
    --summary-file "$output" || rc=$?

if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then
  timed_out_at=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  if [ -s "$output" ]; then
    jq \
      --arg timed_out_at "$timed_out_at" \
      --argjson budget "$budget" \
      --argjson rc "$rc" \
      '.success = false
       | .timed_out = true
       | .timed_out_at = $timed_out_at
       | .budget_seconds = $budget
       | .timeout_rc = $rc
       | .phase = (.phase // "unknown")' \
      "$output" > "${output}.tmp" &&
      mv "${output}.tmp" "$output"
  else
    jq -n \
      --arg reconcile_label "$label" \
      --arg scenario "$SCENARIO" \
      --arg timed_out_at "$timed_out_at" \
      --argjson budget "$budget" \
      --argjson rc "$rc" \
      '{
        schema_version: 1,
        success: false,
        timed_out: true,
        timed_out_at: $timed_out_at,
        phase: "unknown",
        label: $reconcile_label,
        scenario: $scenario,
        budget_seconds: $budget,
        timeout_rc: $rc,
        note: "mock reconcile produced no summary before timeout"
      }' > "$output"
  fi
fi

if [ "$rc" -ne 0 ]; then
  echo "##vso[task.logissue type=error;] mock-layer-reconcile (${label}) failed rc=${rc} for ${SCENARIO}; see ${output}"
fi
exit "$rc"
