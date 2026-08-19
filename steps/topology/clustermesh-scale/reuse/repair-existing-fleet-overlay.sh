#!/usr/bin/env bash

set -euo pipefail

target_run_id="${CLUSTERMESH_DEBUG_TARGET_RUN_ID:?CLUSTERMESH_DEBUG_TARGET_RUN_ID is required}"
expected_count="${CLUSTERMESH_DEBUG_EXPECTED_CLUSTER_COUNT:-100}"
fleet_name="${CLUSTERMESH_DEBUG_FLEET_NAME:-clustermesh-flt}"
profile_name="${CLUSTERMESH_DEBUG_PROFILE_NAME:-clustermesh-cmp}"
label_key="${CMP_MEMBER_LABEL_KEY:-mesh}"
selected_value="${CMP_MEMBER_LABEL_VALUE:-true}"
repair_value="${CMP_MEMBER_REPAIR_LABEL_VALUE:-repairing}"
max_repair_members="${CLUSTERMESH_DEBUG_MAX_REPAIR_MEMBERS:-20}"
detach_settle_seconds="${CLUSTERMESH_DEBUG_REPAIR_DETACH_SETTLE_SECONDS:-30}"
convergence_wait_seconds="${CLUSTERMESH_DEBUG_REPAIR_WAIT_SECONDS:-7200}"
poll_seconds="${CLUSTERMESH_DEBUG_REPAIR_POLL_SECONDS:-30}"
apply_retry_seconds="${CLUSTERMESH_DEBUG_REPAIR_APPLY_RETRY_SECONDS:-300}"
apply_attempts="${CLUSTERMESH_DEBUG_REPAIR_APPLY_ATTEMPTS:-6}"
force_repair_roles_file="${CLUSTERMESH_DEBUG_FORCE_REPAIR_ROLES_FILE:-}"

require_positive_integer() {
  local name="$1"
  local value="$2"

  if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "$name must be a positive integer, got '$value'." >&2
    exit 1
  fi
}

for setting in \
  "CLUSTERMESH_DEBUG_EXPECTED_CLUSTER_COUNT:$expected_count" \
  "CLUSTERMESH_DEBUG_MAX_REPAIR_MEMBERS:$max_repair_members" \
  "CLUSTERMESH_DEBUG_REPAIR_DETACH_SETTLE_SECONDS:$detach_settle_seconds" \
  "CLUSTERMESH_DEBUG_REPAIR_WAIT_SECONDS:$convergence_wait_seconds" \
  "CLUSTERMESH_DEBUG_REPAIR_POLL_SECONDS:$poll_seconds" \
  "CLUSTERMESH_DEBUG_REPAIR_APPLY_RETRY_SECONDS:$apply_retry_seconds" \
  "CLUSTERMESH_DEBUG_REPAIR_APPLY_ATTEMPTS:$apply_attempts"; do
  require_positive_integer "${setting%%:*}" "${setting#*:}"
done

if ! az fleet show --resource-group "$target_run_id" --name "$fleet_name" \
    --only-show-errors >/dev/null 2>&1; then
  echo "Expected existing Fleet $fleet_name in $target_run_id." >&2
  exit 1
fi
if ! az fleet clustermeshprofile show \
    --resource-group "$target_run_id" --fleet-name "$fleet_name" \
    --name "$profile_name" --only-show-errors >/dev/null 2>&1; then
  echo "Expected existing ClusterMeshProfile $profile_name." >&2
  exit 1
fi

inventory_dir=$(mktemp -d "${TMPDIR:-/tmp}/clustermesh-repair-inventory.XXXXXX")
member_file="$inventory_dir/fleet-members.json"
profile_member_file="$inventory_dir/profile-members.json"
cluster_file="$inventory_dir/aks-clusters.json"
repair_roles_pending_restore=()
cleanup_inventory() {
  local original_rc=$?
  local final_rc="$original_rc"
  local role restore_failed=false apply_output apply_rc

  trap - EXIT
  set +e
  if [ "${#repair_roles_pending_restore[@]}" -gt 0 ] &&
    declare -F update_member_label >/dev/null; then
    echo "Repair interrupted; restoring Fleet selector labels on: ${repair_roles_pending_restore[*]}" >&2
    for role in "${repair_roles_pending_restore[@]}"; do
      if ! update_member_label "$role" "$selected_value"; then
        echo "Failed to restore Fleet selector label on $role during cleanup." >&2
        restore_failed=true
      fi
    done
    if [ "$restore_failed" = "false" ]; then
      apply_output=""
      apply_rc=0
      apply_output=$(timeout --foreground 900s az fleet clustermeshprofile apply \
        --resource-group "$target_run_id" --fleet-name "$fleet_name" \
        --name "$profile_name" --output none --only-show-errors 2>&1) || apply_rc=$?
      if [ "$apply_rc" -ne 0 ]; then
        echo "Cleanup restored labels but profile apply failed: $apply_output" >&2
        restore_failed=true
      fi
    fi
    if [ "$restore_failed" = "true" ]; then
      final_rc=1
    fi
  fi
  rm -rf "$inventory_dir"
  exit "$final_rc"
}
trap cleanup_inventory EXIT

az fleet member list \
  --resource-group "$target_run_id" --fleet-name "$fleet_name" \
  --output json --only-show-errors >"$member_file"
az fleet clustermeshprofile list-members \
  --resource-group "$target_run_id" --fleet-name "$fleet_name" \
  --name "$profile_name" --output json --only-show-errors >"$profile_member_file"
az resource list \
  --resource-group "$target_run_id" \
  --resource-type Microsoft.ContainerService/managedClusters \
  --query "[?tags.run_id=='${target_run_id}' && starts_with(tags.role, 'mesh-')].{name:tags.role,clusterResourceId:id}" \
  --output json >"$cluster_file"

if ! jq -e -n \
    --slurpfile members "$member_file" \
    --slurpfile applied "$profile_member_file" \
    --slurpfile clusters "$cluster_file" \
    --arg label_key "$label_key" \
    --arg selected_value "$selected_value" \
    --argjson expected_count "$expected_count" '
      ($clusters[0]
        | map({key:.name, value:(.clusterResourceId | ascii_downcase)})
        | from_entries) as $expected_map
      | ($members[0]
        | map({key:.name, value:(.clusterResourceId | ascii_downcase)})
        | from_entries) as $actual_map
      | ($applied[0] | map(.name) | sort) as $applied_names
      | ($expected_map | keys | sort) as $expected_names
      | ($members[0] | length) == $expected_count
        and ($applied[0] | length) == $expected_count
        and ($clusters[0] | length) == $expected_count
        and ($actual_map | length) == $expected_count
        and $actual_map == $expected_map
        and $applied_names == $expected_names
        and all($members[0][]; (.labels[$label_key] // "") == $selected_value)
    ' >/dev/null; then
  echo "Existing Fleet/profile inventory does not exactly match validated mesh-1..mesh-$expected_count AKS resources." >&2
  exit 1
fi

forced_roles=()
if [ -n "$force_repair_roles_file" ]; then
  if [ ! -f "$force_repair_roles_file" ]; then
    echo "Forced repair role file does not exist: $force_repair_roles_file" >&2
    exit 1
  fi
  while IFS= read -r role || [ -n "$role" ]; do
    role="${role%$'\r'}"
    [ -n "$role" ] || continue
    if ! [[ "$role" =~ ^mesh-[1-9][0-9]*$ ]]; then
      echo "Invalid forced repair role '$role' in $force_repair_roles_file." >&2
      exit 1
    fi
    if ! jq -e --arg role "$role" 'any(.[]; .name == $role)' \
        "$profile_member_file" >/dev/null ||
      ! jq -e --arg role "$role" 'any(.[]; .name == $role)' \
        "$member_file" >/dev/null; then
      echo "Forced repair role $role is not present in the validated Fleet/profile inventory." >&2
      exit 1
    fi
    forced_roles+=("$role")
  done < "$force_repair_roles_file"
fi

mapfile -t repair_roles < <(
  {
    jq -r '
      .[]
      | select((.meshProperties.status.state // "unknown") != "Connected")
      | .name
    ' "$profile_member_file"
    printf '%s\n' "${forced_roles[@]}"
  } |
    awk 'NF' |
    sort -t- -k2,2n -u
)

if [ "${#repair_roles[@]}" -eq 0 ]; then
  echo "Existing Fleet is already $expected_count/$expected_count Connected; no member repair needed."
  exit 0
fi
if [ "${#repair_roles[@]}" -gt "$max_repair_members" ]; then
  echo "Refusing surgical repair of ${#repair_roles[@]} members; maximum is $max_repair_members." >&2
  exit 1
fi

echo "Surgically rejoining ${#repair_roles[@]} control-plane or live-data-plane unhealthy member(s): ${repair_roles[*]}"

update_member_label() {
  local role="$1"
  local value="$2"
  local attempt

  for attempt in 1 2 3 4 5; do
    if az fleet member update \
        --resource-group "$target_run_id" --fleet-name "$fleet_name" \
        --name "$role" --labels "${label_key}=${value}" \
        --output none --only-show-errors; then
      return 0
    fi
    echo "Member $role label update to $value failed on attempt $attempt/5." >&2
    sleep 10
  done
  return 1
}

for role in "${repair_roles[@]}"; do
  repair_roles_pending_restore+=("$role")
  update_member_label "$role" "$repair_value"
done
sleep "$detach_settle_seconds"
for role in "${repair_roles[@]}"; do
  update_member_label "$role" "$selected_value"
done
repair_roles_pending_restore=()

apply_accepted=false
for attempt in $(seq 1 "$apply_attempts"); do
  apply_output=""
  apply_rc=0
  apply_output=$(timeout --foreground 900s az fleet clustermeshprofile apply \
    --resource-group "$target_run_id" --fleet-name "$fleet_name" \
    --name "$profile_name" --output none --only-show-errors 2>&1) || apply_rc=$?
  if [ "$apply_rc" -eq 0 ]; then
    apply_accepted=true
    echo "Repair profile apply accepted on attempt $attempt/$apply_attempts."
    break
  fi
  if [ "$apply_rc" -eq 124 ] ||
    echo "$apply_output" | grep -Eqi 'ResourceNotFinalState|OperationNotAllowed|AnotherOperationInProgress'; then
    echo "Repair profile apply deferred by an active operation on attempt $attempt/$apply_attempts; retrying in ${apply_retry_seconds}s."
    sleep "$apply_retry_seconds"
    continue
  fi
  echo "Repair profile apply failed: $apply_output" >&2
  exit 1
done
if [ "$apply_accepted" != "true" ]; then
  echo "Repair profile apply was never accepted after $apply_attempts attempts." >&2
  exit 1
fi

deadline=$(( $(date +%s) + convergence_wait_seconds ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  state_json=$(az fleet clustermeshprofile list-members \
    --resource-group "$target_run_id" --fleet-name "$fleet_name" \
    --name "$profile_name" --output json --only-show-errors)
  applied=$(jq 'length' <<<"$state_json")
  connected=$(jq '[.[] | select(.meshProperties.status.state=="Connected")] | length' <<<"$state_json")
  failed=$(jq '[.[] | select(.meshProperties.status.state=="Failed")] | length' <<<"$state_json")
  echo "Fleet surgical repair convergence: applied=$applied connected=$connected failed=$failed expected=$expected_count"
  if [ "$applied" -eq "$expected_count" ] &&
    [ "$connected" -eq "$expected_count" ] &&
    [ "$failed" -eq 0 ]; then
    az group update --name "$target_run_id" \
      --set tags.clustermesh_debug_overlay_state=connected \
            tags.clustermesh_debug_repair_build="${BUILD_BUILDID:-manual}" \
      --only-show-errors >/dev/null
    echo "Fleet surgical repair completed: $connected/$expected_count Connected."
    exit 0
  fi
  sleep "$poll_seconds"
done

FLEET_RG="$target_run_id" \
FLEET_NAME="$fleet_name" \
FLEET_PROFILE="$profile_name" \
FLEET_STATE_CAPTURE_DIR="${BUILD_ARTIFACTSTAGINGDIRECTORY:-$(pwd)}/clustermeshprofile-repair-failure" \
FLEET_STATE_CAPTURE_REASON="existing-fleet-repair-timeout" \
  bash "$(dirname "${BASH_SOURCE[0]}")/../capture-fleet-profile-state.sh" || true
echo "Fleet surgical repair did not reach $expected_count/$expected_count Connected within ${convergence_wait_seconds}s." >&2
exit 1
