#!/usr/bin/env bash
set -uo pipefail

clusters="${1:?cluster inventory is required}"
output="${2:?summary output is required}"
budget="${3:?outer budget is required}"

mkdir -p "$(dirname "$output")"
rc=0
timeout --signal=TERM --kill-after=60s "${budget}s" \
  python3 "${PRESERVED_WORKER_RECONCILER:?worker reconciler path is required}" \
    --clusters "$clusters" \
    --summary-file "$output" \
    --max-repair-clusters "${CLUSTERMESH_DEBUG_MAX_WORKER_REPAIR_CLUSTERS:-5}" \
    --max-concurrent-probes "${CLUSTERMESH_DEBUG_WORKER_PROBE_CONCURRENCY:-5}" \
    --max-concurrent-repairs "${CLUSTERMESH_DEBUG_WORKER_REPAIR_CONCURRENCY:-2}" \
    --probe-attempts "${CLUSTERMESH_DEBUG_WORKER_PROBE_ATTEMPTS:-5}" \
    --probe-retry-seconds "${CLUSTERMESH_DEBUG_WORKER_PROBE_RETRY_SECONDS:-60}" \
    --query-timeout-seconds "${CLUSTERMESH_DEBUG_WORKER_QUERY_TIMEOUT_SECONDS:-120}" \
    --mutation-timeout-seconds "${CLUSTERMESH_DEBUG_WORKER_MUTATION_TIMEOUT_SECONDS:-1200}" \
    --recovery-timeout-seconds "${CLUSTERMESH_DEBUG_WORKER_RECOVERY_TIMEOUT_SECONDS:-1800}" \
    --poll-seconds "${CLUSTERMESH_DEBUG_WORKER_POLL_SECONDS:-30}" || rc=$?

if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then
  timed_out_at=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  if [ -s "$output" ] && jq -e 'type == "object"' "$output" >/dev/null 2>&1; then
    jq \
      --arg timed_out_at "$timed_out_at" \
      --argjson budget "$budget" \
      --argjson rc "$rc" \
      '.healthy = false
       | .timed_out = true
       | .timed_out_at = $timed_out_at
       | .budget_seconds = $budget
       | .timeout_rc = $rc' \
      "$output" > "${output}.tmp" &&
      mv "${output}.tmp" "$output"
  else
    jq -n \
      --arg scenario "${SCENARIO:-unknown}" \
      --arg timed_out_at "$timed_out_at" \
      --argjson budget "$budget" \
      --argjson rc "$rc" \
      '{
        schema_version: 1,
        healthy: false,
        timed_out: true,
        timed_out_at: $timed_out_at,
        scenario: $scenario,
        budget_seconds: $budget,
        timeout_rc: $rc,
        note: "worker reconcile exceeded its outer deadline"
      }' > "$output"
  fi
fi

if [ "$rc" -ne 0 ]; then
  echo "##vso[task.logissue type=error;] preserved-worker-reconcile failed rc=${rc} for ${SCENARIO:-unknown}; see ${output}"
fi
exit "$rc"
