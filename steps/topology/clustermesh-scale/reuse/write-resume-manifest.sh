#!/usr/bin/env bash

set -euo pipefail

target_run_id="${RUN_ID:?RUN_ID is required}"
region="${CLUSTERMESH_DEBUG_EXPECTED_REGION:-eastus2}"
expected_count="${CLUSTERMESH_DEBUG_EXPECTED_CLUSTER_COUNT:-100}"
expected_fleet_count="${CLUSTERMESH_DEBUG_EXPECTED_FLEET_COUNT:--1}"
expected_fleet_name="${CLUSTERMESH_DEBUG_EXPECTED_FLEET_NAME:-clustermesh-flt}"
expected_subscription_id="${CLUSTERMESH_DEBUG_EXPECTED_SUBSCRIPTION_ID:-${AZURE_SUBSCRIPTION_ID:-}}"
tfvars_path="${CLUSTERMESH_DEBUG_TFVARS_PATH:-}"
output_path="${CLUSTERMESH_DEBUG_MANIFEST_PATH:?CLUSTERMESH_DEBUG_MANIFEST_PATH is required}"
inventory_attempts="${CLUSTERMESH_DEBUG_MANIFEST_INVENTORY_ATTEMPTS:-3}"
inventory_retry_seconds="${CLUSTERMESH_DEBUG_MANIFEST_RETRY_SECONDS:-10}"
inventory_timeout_seconds="${CLUSTERMESH_DEBUG_MANIFEST_INVENTORY_TIMEOUT_SECONDS:-600}"

mkdir -p "$(dirname "$output_path")"
for value_name in expected_count inventory_attempts inventory_retry_seconds inventory_timeout_seconds; do
  value="${!value_name}"
  if ! [[ "$value" =~ ^[0-9]+$ ]]; then
    echo "$value_name must be a non-negative integer." >&2
    exit 1
  fi
done
if ! [[ "$expected_fleet_count" =~ ^-1$|^[0-9]+$ ]]; then
  echo "expected_fleet_count must be -1 or a non-negative integer." >&2
  exit 1
fi
if [ "$expected_fleet_count" -gt 0 ] && [ -z "$expected_fleet_name" ]; then
  echo "expected_fleet_name must be nonempty when a Fleet is required." >&2
  exit 1
fi
if [ "$inventory_attempts" -eq 0 ] || [ "$inventory_timeout_seconds" -eq 0 ]; then
  echo "inventory_attempts and inventory_timeout_seconds must be positive." >&2
  exit 1
fi

write_error_manifest() {
  local status="$1"
  local error="$2"
  local temporary="${output_path}.tmp.$$"
  jq -n \
    --arg run_id "$target_run_id" \
    --arg status "$status" \
    --arg fatal_error "$error" \
    --arg subscription_id "${subscription_id:-$expected_subscription_id}" \
    --arg region "$region" \
    --arg source_version "${BUILD_SOURCEVERSION:-unknown}" \
    --arg build_id "${BUILD_BUILDID:-unknown}" \
    '{
      run_id:$run_id,
      status:$status,
      fatal_error:$fatal_error,
      subscription_id:$subscription_id,
      region:$region,
      source_version:$source_version,
      build_id:$build_id
    }' > "$temporary"
  mv "$temporary" "$output_path"
}

run_json_inventory() {
  local output="$1"
  local description="$2"
  shift 2
  local minimum_count=""
  local expected_json_type=""
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --minimum-count)
        minimum_count="$2"
        shift 2
        ;;
      --type)
        expected_json_type="$2"
        shift 2
        ;;
      *)
        break
        ;;
    esac
  done
  local attempt temporary error_file detail actual_count
  temporary="${output}.tmp"
  error_file="${output}.err"
  for attempt in $(seq 1 "$inventory_attempts"); do
    rm -f "$temporary" "$error_file"
    if timeout --signal=TERM --kill-after=10s \
        "${inventory_timeout_seconds}s" \
        "$@" >"$temporary" 2>"$error_file"; then
      if jq -e . "$temporary" >/dev/null 2>&1; then
        if [ -n "$expected_json_type" ] &&
           ! jq -e --arg expected "$expected_json_type" \
             'type == $expected' "$temporary" >/dev/null 2>&1; then
          detail="valid JSON response had type $(jq -r type "$temporary"), expected $expected_json_type"
        elif [ -z "$minimum_count" ]; then
          mv "$temporary" "$output"
          rm -f "$error_file"
          return 0
        elif actual_count=$(jq -er \
            'if type == "array" then length else error("not an array") end' \
            "$temporary" 2>/dev/null) &&
           [ "$actual_count" -ge "$minimum_count" ]; then
          mv "$temporary" "$output"
          rm -f "$error_file"
          return 0
        else
          detail="valid but incomplete response: found ${actual_count:-non-array} item(s), expected at least $minimum_count"
        fi
      else
        detail="response was not valid JSON"
      fi
    else
      detail=$(tr '\n' ' ' < "$error_file" | cut -c1-1000)
    fi
    echo "$description attempt $attempt/$inventory_attempts failed: $detail" >&2
    if [ "$attempt" -lt "$inventory_attempts" ] &&
       [ "$inventory_retry_seconds" -gt 0 ]; then
      sleep "$inventory_retry_seconds"
    fi
  done
  rm -f "$temporary" "$error_file"
  inventory_error="$description failed after $inventory_attempts attempt(s): $detail"
  return 1
}

confirm_expected_fleet_absence() {
  local output="$1"
  local attempt rc temporary error_file detail
  temporary="${output}.show.tmp"
  error_file="${output}.show.err"
  for attempt in $(seq 1 "$inventory_attempts"); do
    rm -f "$temporary" "$error_file"
    rc=0
    timeout --signal=TERM --kill-after=10s \
      "${inventory_timeout_seconds}s" \
      az fleet show "${subscription_args[@]}" \
        --resource-group "$target_run_id" \
        --name "$expected_fleet_name" \
        --output json --only-show-errors \
        >"$temporary" 2>"$error_file" || rc=$?
    detail=$(cat "$error_file" "$temporary" | tr '\n' ' ' | cut -c1-1000)
    if [ "$rc" -eq 0 ]; then
      if jq -e \
          --arg expected_name "$expected_fleet_name" \
          'type == "object" and
           .name == $expected_name and
           (.id | type == "string" and length > 0)' \
          "$temporary" >/dev/null 2>&1; then
        jq -s '.' "$temporary" >"$output"
        rm -f "$temporary" "$error_file"
        return 0
      fi
      detail="Fleet point lookup returned invalid JSON or the wrong Fleet identity"
    elif grep -Eqi \
        'AuthorizationFailed|Forbidden|SubscriptionNotFound|InvalidSubscriptionId|InvalidAuthenticationToken' \
        <<<"$detail"; then
      rm -f "$temporary" "$error_file"
      inventory_error="Fleet absence verification failed with a non-retryable error: $detail"
      return 1
    elif grep -Eqi 'ResourceGroupNotFound' <<<"$detail"; then
      rm -f "$temporary" "$error_file"
      inventory_error="Fleet absence verification lost the parent resource group: $detail"
      return 1
    elif grep -Eqi \
        'ResourceNotFound|NotFound|could not be found|was not found|HTTP([/[:space:]]+[0-9.]+)?[[:space:]]+404|status([[:space:]]+code)?[=:[:space:]]+404' \
        <<<"$detail" &&
       grep -Eqi \
        "Microsoft\\.ContainerService/fleets|/fleets/${expected_fleet_name}|Fleet[^[:alnum:]]+${expected_fleet_name}|${expected_fleet_name}[^[:alnum:]]+.*(not found|could not be found|was not found)" \
        <<<"$detail"; then
      printf '[]\n' >"$output"
      rm -f "$temporary" "$error_file"
      return 0
    fi
    echo "Fleet absence verification attempt $attempt/$inventory_attempts failed: $detail" >&2
    if [ "$attempt" -lt "$inventory_attempts" ] &&
       [ "$inventory_retry_seconds" -gt 0 ]; then
      sleep "$inventory_retry_seconds"
    fi
  done
  rm -f "$temporary" "$error_file"
  inventory_error="Fleet absence verification failed after $inventory_attempts attempt(s): $detail"
  return 1
}

if [ "$expected_count" -eq 0 ]; then
  write_error_manifest "manifest_configuration_invalid" \
    "CLUSTERMESH_DEBUG_EXPECTED_CLUSTER_COUNT must be positive"
  exit 1
fi
account_file=$(mktemp)
if ! run_json_inventory "$account_file" "Azure account query" \
    --type object \
    az account show --output json --only-show-errors; then
  write_error_manifest "account_query_failed" "$inventory_error"
  rm -f "$account_file"
  exit 1
fi
if ! subscription_id=$(jq -er \
    '.id | select(type == "string" and length > 0)' "$account_file"); then
  write_error_manifest "account_query_failed" \
    "Azure account query returned an invalid subscription ID"
  rm -f "$account_file"
  exit 1
fi
rm -f "$account_file"
if [ -n "$expected_subscription_id" ] &&
   [ "${subscription_id,,}" != "${expected_subscription_id,,}" ]; then
  write_error_manifest "subscription_mismatch" \
    "expected subscription $expected_subscription_id, got $subscription_id"
  exit 1
fi
subscription_args=(--subscription "$subscription_id")

exists=""
exists_error=""
exists_error_file=$(mktemp)
for attempt in $(seq 1 "$inventory_attempts"); do
  rm -f "$exists_error_file"
  if exists=$(timeout --signal=TERM --kill-after=10s \
      "${inventory_timeout_seconds}s" \
      az group exists "${subscription_args[@]}" --name "$target_run_id" \
        --only-show-errors 2>"$exists_error_file") &&
     { [ "$exists" = "true" ] || [ "$exists" = "false" ]; }; then
    break
  fi
  exists_error=$(tr '\n' ' ' < "$exists_error_file" | cut -c1-1000)
  if [ -z "$exists_error" ]; then
    exists_error="invalid response: $(tr '\n' ' ' <<<"$exists" | cut -c1-500)"
  fi
  echo "Resource-group existence query attempt $attempt/$inventory_attempts failed: $exists_error" >&2
  exists=""
  if [ "$attempt" -lt "$inventory_attempts" ] &&
     [ "$inventory_retry_seconds" -gt 0 ]; then
    sleep "$inventory_retry_seconds"
  fi
done
rm -f "$exists_error_file"
if [ -z "$exists" ]; then
  write_error_manifest "resource_group_query_failed" \
    "resource-group existence query failed: $exists_error"
  exit 1
fi
if [ "$exists" != "true" ]; then
  manifest_tmp="${output_path}.tmp.$$"
  jq -n --arg run_id "$target_run_id" --arg status "resource_group_absent" \
    '{run_id:$run_id,status:$status}' > "$manifest_tmp"
  mv "$manifest_tmp" "$output_path"
  exit 0
fi

tfvars_sha=""
if [ -z "$tfvars_path" ] || [ ! -f "$tfvars_path" ]; then
  write_error_manifest "manifest_configuration_invalid" \
    "CLUSTERMESH_DEBUG_TFVARS_PATH is missing or not a file: $tfvars_path"
  exit 1
fi
tfvars_sha=$(sha256sum "$tfvars_path" | awk '{print $1}')
group_file=$(mktemp)
cluster_file=$(mktemp)
fleet_file=$(mktemp)
cleanup() {
  rm -f "$group_file" "$cluster_file" "$fleet_file" "${output_path}.tmp.$$"
}
trap cleanup EXIT
if ! run_json_inventory "$group_file" "Resource-group inventory" \
    --type object \
    az group show "${subscription_args[@]}" --name "$target_run_id" \
      --output json --only-show-errors; then
  write_error_manifest "resource_group_inventory_failed" "$inventory_error"
  exit 1
fi
if ! jq -e \
    'type == "object" and
     ((.tags == null) or (
       (.tags | type == "object") and
       ((.tags.deletion_due_time == null) or
        (.tags.deletion_due_time | type == "string"))
     ))' \
    "$group_file" >/dev/null; then
  write_error_manifest "resource_group_inventory_failed" \
    "Resource-group inventory returned invalid tags or deletion_due_time"
  exit 1
fi
deletion_due_time=$(jq -r '(.tags // {}).deletion_due_time // ""' "$group_file")
if [ -n "$deletion_due_time" ] &&
   ! date -u -d "$deletion_due_time" +%s >/dev/null 2>&1; then
  write_error_manifest "resource_group_inventory_failed" \
    "Resource-group inventory returned an invalid deletion_due_time timestamp"
  exit 1
fi

if ! run_json_inventory "$cluster_file" "AKS inventory" \
    --type array \
    --minimum-count "$expected_count" \
    az aks list "${subscription_args[@]}" --resource-group "$target_run_id" \
      --query "[].{id:id,name:name,runId:tags.run_id,role:tags.role,location:location,provisioningState:provisioningState,powerState:powerState.code,nodeResourceGroup:nodeResourceGroup}" \
      --output json --only-show-errors; then
  write_error_manifest "cluster_inventory_failed" "$inventory_error"
  exit 1
fi
fleet_inventory_args=(--type array)
if [ "$expected_fleet_count" -gt 0 ]; then
  fleet_inventory_args+=(--minimum-count "$expected_fleet_count")
fi
if ! run_json_inventory "$fleet_file" "Fleet inventory" \
    "${fleet_inventory_args[@]}" \
    az resource list "${subscription_args[@]}" \
      --resource-group "$target_run_id" \
      --resource-type Microsoft.ContainerService/fleets \
      --output json --only-show-errors; then
  write_error_manifest "fleet_inventory_failed" "$inventory_error"
  exit 1
fi
if [ "$expected_fleet_count" -eq 0 ] &&
   [ "$(jq 'length' "$fleet_file")" -eq 0 ] &&
   ! confirm_expected_fleet_absence "$fleet_file"; then
  write_error_manifest "fleet_inventory_failed" "$inventory_error"
  exit 1
fi
clusters=$(cat "$cluster_file")
fleet=$(cat "$fleet_file")

inventory_error=""
if ! jq -e \
    --argjson expected "$expected_count" \
    --arg region "$region" \
    --arg run_id "$target_run_id" \
    --arg subscription_id "$subscription_id" '
      length == $expected and
      ([.[].role
        | select(type == "string" and test("^mesh-[1-9][0-9]*$"))
        | sub("^mesh-"; "")
        | tonumber]
       | sort) == [range(1; $expected + 1)] and
      all(.[];
        (.role | sub("^mesh-"; "")) as $number
        | .runId == $run_id and
          .name == ("clustermesh-" + $number) and
          (.id | type == "string") and
          ((.id | ascii_downcase) ==
            ("/subscriptions/" + $subscription_id +
             "/resourcegroups/" + $run_id +
             "/providers/microsoft.containerservice/managedclusters/" +
             .name | ascii_downcase)) and
          ((.location // "" | ascii_downcase) == ($region | ascii_downcase)) and
          .provisioningState == "Succeeded" and
          .powerState == "Running" and
          (.nodeResourceGroup | type == "string" and length > 0)
      )
    ' "$cluster_file" >/dev/null; then
  inventory_error="AKS inventory is not exactly mesh-1..mesh-${expected_count} in ${region}"
fi
if [ -z "$inventory_error" ] &&
   [ "$expected_fleet_count" -ge 0 ] &&
   ! jq -e \
      --argjson expected "$expected_fleet_count" \
      --arg expected_name "$expected_fleet_name" \
      --arg subscription_id "$subscription_id" \
      --arg resource_group "$target_run_id" '
      length == $expected and
      all(.[];
        ((.provisioningState // .properties.provisioningState) == "Succeeded") and
        ($expected == 0 or (
          .name == $expected_name and
          (.id | type == "string") and
          ((.id | ascii_downcase) ==
            ("/subscriptions/" + $subscription_id +
             "/resourcegroups/" + $resource_group +
             "/providers/microsoft.containerservice/fleets/" +
             $expected_name | ascii_downcase))
        ))
      )
    ' \
      "$fleet_file" >/dev/null; then
  inventory_error="Fleet inventory is not exactly ${expected_fleet_count} Succeeded ${expected_fleet_name} resource(s) with IDs"
fi
if [ -z "$inventory_error" ] && [ -z "$deletion_due_time" ]; then
  inventory_error="Preserved resource group has no deletion_due_time lease tag"
fi

manifest_tmp="${output_path}.tmp.$$"
jq -n \
  --arg status "$([ -z "$inventory_error" ] && echo ready || echo inventory_invalid)" \
  --arg fatal_error "$inventory_error" \
  --arg run_id "$target_run_id" \
  --arg subscription_id "$subscription_id" \
  --arg region "$region" \
  --arg source_version "${BUILD_SOURCEVERSION:-unknown}" \
  --arg build_id "${BUILD_BUILDID:-unknown}" \
  --arg deletion_due_time "$deletion_due_time" \
  --arg tfvars_sha256 "$tfvars_sha" \
  --argjson clusters "$clusters" \
  --argjson fleet "$fleet" \
  '{
    status:$status,
    run_id:$run_id,
    subscription_id:$subscription_id,
    region:$region,
    source_version:$source_version,
    build_id:$build_id,
    deletion_due_time:$deletion_due_time,
    tfvars_sha256:$tfvars_sha256,
    cluster_count:($clusters|length),
    clusters:$clusters,
    fleet:$fleet
  } + (if ($fatal_error | length) > 0
       then {fatal_error:$fatal_error}
       else {}
       end)' > "$manifest_tmp"
mv "$manifest_tmp" "$output_path"

if [ -n "$inventory_error" ]; then
  echo "$inventory_error" >&2
  exit 1
fi
