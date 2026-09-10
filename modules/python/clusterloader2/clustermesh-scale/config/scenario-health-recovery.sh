#!/usr/bin/env bash
set -uo pipefail

clusters="${1:?cluster inventory is required}"
report_dir="${2:?scenario report directory is required}"
cluster_count="${3:?cluster count is required}"

scenario="${SCENARIO:?scenario is required}"
total_timeout="${HEALTH_GATE_TIMEOUT_SECONDS:?health timeout is required}"
cycle_timeout="${HEALTH_GATE_CYCLE_TIMEOUT_SECONDS:?cycle timeout is required}"
cluster_timeout="${HEALTH_GATE_CLUSTER_TIMEOUT_SECONDS:?cluster timeout is required}"
quiet_window="${HEALTH_GATE_QUIET_WINDOW_SECONDS:?quiet window is required}"
poll_interval="${HEALTH_GATE_POLL_INTERVAL_SECONDS:-30}"
concurrency="${HEALTH_GATE_CONCURRENCY:?health concurrency is required}"
completion_margin="${HEALTH_GATE_COMPLETION_MARGIN_SECONDS:-5}"
initial_cycles="${CL2_HEALTH_GATE_INITIAL_CYCLES:-1}"
max_repair_roles="${CL2_HEALTH_GATE_MAX_REPAIR_ROLES:-25}"
worker_budget="${CL2_HEALTH_GATE_WORKER_REPAIR_BUDGET_SECONDS:-3600}"
mock_budget="${CL2_MOCK_RECONCILE_BUDGET_SECONDS:?mock reconcile budget is required}"

final_summary="$report_dir/scenario-health-gate.json"
observation_summary="$report_dir/scenario-health-gate-observation.json"
repair_inventory="$report_dir/scenario-health-repair-clusters.json"
recovery_summary="$report_dir/scenario-health-recovery.json"
deadline=$(( $(date +%s) + total_timeout ))
waves=$(( (cluster_count + concurrency - 1) / concurrency ))
full_cycle_min=$(( waves * cluster_timeout ))
final_reserve=$(( 2 * full_cycle_min + quiet_window + completion_margin ))

health_observation_rc=0
health_repair_role_count=0
health_repair_attempted=false
health_repair_valid=true
mock_repair_valid=true
cleanup_gate_rc=0
gate_complete=false

write_summary() {
  local success="$1"
  local partial="${recovery_summary}.partial"
  jq -n \
    --argjson success "$success" \
    --arg scenario "$scenario" \
    --argjson health_observation_rc "$health_observation_rc" \
    --argjson health_repair_role_count "$health_repair_role_count" \
    --argjson health_repair_attempted "$health_repair_attempted" \
    --argjson health_repair_valid "$health_repair_valid" \
    --argjson mock_repair_valid "$mock_repair_valid" \
    --argjson cleanup_gate_rc "$cleanup_gate_rc" \
    '{
      schema_version: 1,
      success: $success,
      scenario: $scenario,
      health_observation_rc: $health_observation_rc,
      health_repair_role_count: $health_repair_role_count,
      health_repair_attempted: $health_repair_attempted,
      health_repair_valid: $health_repair_valid,
      mock_repair_valid: $mock_repair_valid,
      cleanup_gate_rc: $cleanup_gate_rc
    }' > "$partial" &&
    mv "$partial" "$recovery_summary"
}

run_gate() {
  local summary="$1" timeout_seconds="$2" max_cycles="${3:-0}"
  local -a max_cycle_args=()
  if [ "$max_cycles" -gt 0 ]; then
    max_cycle_args=(--max-cycles "$max_cycles")
  fi
  HEALTH_GATE_TIMEOUT_SECONDS="$timeout_seconds" \
  HEALTH_GATE_MAX_CYCLES="$max_cycles" \
  HEALTH_GATE_SUMMARY_FILE="$summary" \
    bash "${HEALTH_GATE_SCRIPT:?health gate path is required}" \
      --clusters "$clusters" \
      --scenario "$scenario" \
      --expected-mock-count "${EXPECTED_MOCK_COUNT:?expected mock count is required}" \
      --expected-remote-count "${EXPECTED_REMOTE_COUNT:?expected remote count is required}" \
      --cycle-timeout-seconds "$cycle_timeout" \
      --cluster-timeout-seconds "$cluster_timeout" \
      --quiet-window-seconds "$quiet_window" \
      --poll-interval-seconds "$poll_interval" \
      --concurrency "$concurrency" \
      --completion-margin-seconds "$completion_margin" \
      "${max_cycle_args[@]}" \
      --summary-file "$summary"
}

mkdir -p "$report_dir"
if [ "${CL2_HEALTH_GATE_REPAIR_ENABLED:-false}" = "true" ]; then
  observation_timeout=$((initial_cycles * cycle_timeout + completion_margin))
  remaining=$((deadline - $(date +%s)))
  if [ "$observation_timeout" -gt "$remaining" ]; then
    observation_timeout="$remaining"
  fi
  run_gate "$observation_summary" "$observation_timeout" "$initial_cycles" ||
    health_observation_rc=$?

  if [ "$health_observation_rc" -eq 0 ]; then
    if cp "$observation_summary" "$final_summary"; then
      gate_complete=true
    else
      health_observation_rc=1
    fi
  elif [ "$health_observation_rc" -eq 3 ] &&
       jq -e \
         --argjson expected "$cluster_count" \
         --argjson cycles "$initial_cycles" \
         --slurpfile inventory "$clusters" '
           .termination_reason == "cycle-limit" and
           .completed_cycle_count == $cycles and
           (.clusters | type == "array" and length == $expected) and
           ([.clusters[].role] | unique | length) == $expected and
           ([.clusters[].role] | sort) == ($inventory[0] | map(.role) | sort)
         ' "$observation_summary" >/dev/null 2>&1; then
    if jq \
        --slurpfile observations "$observation_summary" '
          [.[]
           | .role as $role
           | select(any($observations[0].clusters[];
               .role == $role and (.healthy | not)))]
        ' "$clusters" > "${repair_inventory}.tmp" &&
       mv "${repair_inventory}.tmp" "$repair_inventory"; then
      health_repair_role_count=$(jq 'length' "$repair_inventory")
      if [ "$health_repair_role_count" -gt 0 ]; then
        health_repair_attempted=true
        echo "${scenario}: auditing real workers after ${health_repair_role_count} unhealthy role observation(s)"
        worker_output="$report_dir/preserved-worker-reconcile-health-repair.json"
        worker_required=0
        if [ "${CLUSTERMESH_PRESERVED_WORKER_RECOVERY_ENABLED:-false}" = "true" ]; then
          worker_required=$((worker_budget + 60))
        fi
        mock_required=0
        if [ "${CL2_MOCK_MODE:-false}" = "true" ] &&
           [ "$health_repair_role_count" -le "$max_repair_roles" ]; then
          mock_required=$((mock_budget + 30))
        fi
        remaining=$((deadline - $(date +%s)))
        if [ "$worker_required" -gt 0 ]; then
          if [ "$remaining" -ge $((final_reserve + worker_required + mock_required)) ]; then
            if ! bash "${PRESERVED_WORKER_RECONCILE_WRAPPER:?worker wrapper path is required}" \
                "$clusters" "$worker_output" "$worker_budget"; then
              health_repair_valid=false
            fi
          else
            health_repair_valid=false
            echo "##vso[task.logissue type=error;] ${scenario}: insufficient health budget for real-worker repair and final certification"
          fi
        fi
        if [ "$health_repair_valid" = "true" ] && [ "$mock_required" -gt 0 ]; then
          remaining=$((deadline - $(date +%s)))
          if [ "$remaining" -ge $((final_reserve + mock_required)) ]; then
            mock_output="$report_dir/mock-layer-reconcile-health-repair.json"
            if ! bash "${MOCK_RECONCILE_WRAPPER:?mock wrapper path is required}" \
                health-repair "$mock_output" "$repair_inventory" \
                "$cluster_count" "$mock_budget"; then
              health_repair_valid=false
              mock_repair_valid=false
            fi
          else
            health_repair_valid=false
            mock_repair_valid=false
            echo "##vso[task.logissue type=error;] ${scenario}: insufficient health budget for mock repair and final certification"
          fi
        elif [ "$health_repair_valid" != "true" ]; then
          echo "##vso[task.logissue type=warning;] ${scenario}: skipping mock repair after an unsafe real-worker repair result"
        elif [ "${CL2_MOCK_MODE:-false}" = "true" ] &&
             [ "$health_repair_role_count" -gt "$max_repair_roles" ]; then
          echo "##vso[task.logissue type=warning;] ${scenario}: ${health_repair_role_count} unhealthy roles exceed the targeted mock-repair limit ${max_repair_roles}; refusing broad mutation"
        fi
      fi
    else
      rm -f "${repair_inventory}.tmp"
      echo "##vso[task.logissue type=warning;] ${scenario}: unable to derive targeted repair inventory"
    fi
  else
    echo "##vso[task.logissue type=warning;] ${scenario}: initial health observation was not usable for targeted repair (rc=${health_observation_rc})"
  fi
fi

if [ "$gate_complete" != "true" ]; then
  remaining=$((deadline - $(date +%s)))
  if [ "$remaining" -gt 0 ]; then
    run_gate "$final_summary" "$remaining" || cleanup_gate_rc=$?
  else
    cleanup_gate_rc=1
    echo "##vso[task.logissue type=error;] ${scenario}: health budget expired before final certification"
  fi
fi

success=true
if [ "$cleanup_gate_rc" -ne 0 ] || [ "$health_repair_valid" != "true" ]; then
  success=false
fi
write_summary "$success" || exit 1
[ "$success" = "true" ]
