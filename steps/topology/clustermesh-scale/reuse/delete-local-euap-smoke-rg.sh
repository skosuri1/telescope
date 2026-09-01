#!/usr/bin/env bash

set -euo pipefail

target_run_id="${CLUSTERMESH_DEBUG_TARGET_RUN_ID:?CLUSTERMESH_DEBUG_TARGET_RUN_ID is required}"
confirm="${CLUSTERMESH_DEBUG_CONFIRM_DELETE:?CLUSTERMESH_DEBUG_CONFIRM_DELETE is required}"
expected_subscription="${CLUSTERMESH_DEBUG_EXPECTED_SUBSCRIPTION_ID:?CLUSTERMESH_DEBUG_EXPECTED_SUBSCRIPTION_ID is required}"
expected_region="${CLUSTERMESH_DEBUG_EXPECTED_REGION:-eastus2euap}"
expected_count="${CLUSTERMESH_DEBUG_EXPECTED_CLUSTER_COUNT:-2}"
expected_scenario="perf-eval-clustermesh-scale-local-smoke"

if [ "$confirm" != "$target_run_id" ]; then
  echo "Delete confirmation mismatch: CLUSTERMESH_DEBUG_CONFIRM_DELETE must equal $target_run_id." >&2
  exit 1
fi
if ! [[ "$target_run_id" =~ ^local-euap-[a-z0-9-]+-[0-9a-f]{6}$ ]]; then
  echo "Invalid local EUAP smoke RG '$target_run_id'." >&2
  exit 1
fi
if ! [[ "$expected_count" =~ ^(0|2)$ ]]; then
  echo "CLUSTERMESH_DEBUG_EXPECTED_CLUSTER_COUNT must be 0 or 2." >&2
  exit 1
fi

actual_subscription=$(az account show --query id -o tsv)
if [[ "${actual_subscription,,}" != "${expected_subscription,,}" ]]; then
  echo "Expected subscription $expected_subscription, got $actual_subscription." >&2
  exit 1
fi

rg_json=$(az group show --name "$target_run_id" -o json --only-show-errors)
location=$(jq -r '.location // empty' <<< "$rg_json")
tagged_run_id=$(jq -r '.tags.run_id // empty' <<< "$rg_json")
scenario=$(jq -r '.tags.scenario // empty' <<< "$rg_json")
owner=$(jq -r '.tags.owner // empty' <<< "$rg_json")
creation_date=$(jq -r '.tags.creation_date // empty' <<< "$rg_json")
deletion_due_time=$(jq -r '.tags.deletion_due_time // empty' <<< "$rg_json")

if [[ "${location,,}" != "${expected_region,,}" ]]; then
  echo "Refusing to delete RG outside expected region $expected_region." >&2
  exit 1
fi
if [ "$tagged_run_id" != "$target_run_id" ]; then
  echo "RG run_id tag mismatch: expected $target_run_id, got ${tagged_run_id:-missing}." >&2
  exit 1
fi
if [ "$scenario" != "$expected_scenario" ]; then
  echo "RG scenario tag mismatch: expected $expected_scenario, got ${scenario:-missing}." >&2
  exit 1
fi
if [ "$owner" != "aks" ]; then
  echo "RG owner tag mismatch: expected aks, got ${owner:-missing}." >&2
  exit 1
fi
if [ -z "$creation_date" ] || [ -z "$deletion_due_time" ]; then
  echo "RG is missing local-smoke lifecycle tags." >&2
  exit 1
fi

clusters=$(az aks list \
  --resource-group "$target_run_id" \
  --query '[].{
    name:name,
    id:id,
    node_resource_group:nodeResourceGroup,
    role:tags.role,
    scenario:tags.scenario
  }' \
  -o json \
  --only-show-errors)
if [ "$(jq 'length' <<< "$clusters")" -ne "$expected_count" ]; then
  echo "Expected exactly $expected_count local-smoke AKS clusters." >&2
  exit 1
fi
if [ "$expected_count" -eq 2 ] && ! jq -e --arg scenario "$expected_scenario" '
      ([.[].name] | sort) == ["clustermesh-1", "clustermesh-2"] and
      ([.[].role] | sort) == ["mesh-1", "mesh-2"] and
      all(.[]; .scenario == $scenario) and
      all(.[]; (.id | type) == "string" and (.id | length) > 0) and
      all(.[]; (.node_resource_group | type) == "string" and
        (.node_resource_group | length) > 0)
    ' <<< "$clusters" >/dev/null; then
  echo "Local-smoke AKS inventory does not match the expected two-cluster topology." >&2
  exit 1
fi
if [ "$expected_count" -eq 2 ] && ! jq -e '
    ([.[].node_resource_group | ascii_downcase] | unique | length) == length
  ' <<< "$clusters" >/dev/null; then
  echo "Local-smoke AKS inventory has duplicate node resource groups." >&2
  exit 1
fi

echo "Validated local EUAP smoke RG $target_run_id; deleting parent RG."
az group delete \
  --name "$target_run_id" \
  --yes \
  --no-wait \
  --only-show-errors

deadline=$(( $(date +%s) + 3600 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  if [ "$(az group exists --name "$target_run_id" --only-show-errors)" = "false" ]; then
    break
  fi
  sleep 20
done
if [ "$(az group exists --name "$target_run_id" --only-show-errors)" != "false" ]; then
  echo "Parent RG $target_run_id still exists after 3600s." >&2
  exit 1
fi

while IFS= read -r row; do
  node_rg=$(jq -r '.node_resource_group' <<< "$row")
  cluster_id=$(jq -r '.id' <<< "$row")
  if [ "$(az group exists --name "$node_rg" --only-show-errors)" = "false" ]; then
    continue
  fi
  managed_by=$(az group show \
    --name "$node_rg" \
    --query managedBy \
    -o tsv \
    --only-show-errors)
  if [[ "${managed_by,,}" != "${cluster_id,,}" ]]; then
    echo "Refusing to delete unexpected node RG $node_rg: managedBy=$managed_by." >&2
    exit 1
  fi
  echo "Deleting residual managed RG $node_rg."
  az group delete \
    --name "$node_rg" \
    --yes \
    --no-wait \
    --only-show-errors
done < <(jq -c '.[]' <<< "$clusters")

deadline=$(( $(date +%s) + 1800 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  remaining=0
  while IFS= read -r node_rg; do
    if [ "$(az group exists --name "$node_rg" --only-show-errors)" = "true" ]; then
      remaining=$((remaining + 1))
    fi
  done < <(jq -r '.[].node_resource_group' <<< "$clusters")
  if [ "$remaining" -eq 0 ]; then
    echo "Deleted local EUAP smoke parent and managed RGs for $target_run_id."
    exit 0
  fi
  sleep 20
done

echo "One or more managed RGs still exist after cleanup." >&2
exit 1
