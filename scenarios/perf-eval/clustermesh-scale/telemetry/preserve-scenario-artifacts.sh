#!/usr/bin/env bash
set +x
set -uo pipefail

shopt -s nullglob

: "${SCENARIO_REPORT_DIR:?SCENARIO_REPORT_DIR is required}"
: "${SCENARIO_NAME:?SCENARIO_NAME is required}"
: "${RUN_ID:?RUN_ID is required}"
: "${BUILD_ID:?BUILD_ID is required}"
: "${SNAPSHOT_TIER:?SNAPSHOT_TIER is required}"
: "${BUILD_BRANCH:?BUILD_BRANCH is required}"
: "${STORAGE_ACCOUNT_NAME:?STORAGE_ACCOUNT_NAME is required}"
: "${CONTAINER_NAME:?CONTAINER_NAME is required}"
: "${TARGET_SUBSCRIPTION_ID:?TARGET_SUBSCRIPTION_ID is required}"
: "${WORKER_SUMMARY_FILE:?WORKER_SUMMARY_FILE is required}"
: "${RELABEL_SCRIPT:?RELABEL_SCRIPT is required}"

snapshot_enabled="${CL2_PROM_SNAPSHOT_ENABLED:-false}"
snapshot_target="${CL2_PROM_SNAPSHOT_TARGET:-artifact}"
# PRESERVE_LIFECYCLE_ONLY=true is the "final lifecycle" mode invoked once
# more, from execute.yml, AFTER the final scenario-policy.json/health-gate
# metadata for this scenario is on disk -- it re-uploads just the small
# durable-state files (final scenario-policy.json, scenario-evidence.json,
# scenario-health-gate.json, mock-layer-reconcile-{before,after}.json,
# timing/evidence JSON, and the earlier artifact-preservation-summary.json)
# so Blob storage has the FINAL suite_continue/recovery fields even if the
# job dies before the end-of-stage collect() step runs. It writes to a
# DISTINCT summary file so it never races with (or overwrites) the early
# per-scenario summary, and it skips worker/snapshot/relabel requirements
# entirely -- those were already proven by the early (non-lifecycle-only)
# invocation earlier in this same scenario.
lifecycle_only="${PRESERVE_LIFECYCLE_ONLY:-false}"
summary_file="$SCENARIO_REPORT_DIR/artifact-preservation-summary.json"
if [ "${lifecycle_only,,}" = "true" ]; then
  summary_file="$SCENARIO_REPORT_DIR/artifact-preservation-final-summary.json"
fi
summary_tmp="${summary_file}.partial"

expected_roles=()
uploaded_snapshot_roles=()
missing_snapshot_roles=()
errors=()
expected_successful_worker_count=0
uploaded_snapshot_count=0
uploaded_snapshot_bytes=0
uploaded_audit_count=0
uploaded_audit_bytes=0
uploaded_acns_count=0
uploaded_acns_bytes=0
uploaded_lifecycle_count=0
uploaded_lifecycle_bytes=0
worker_summary_valid=false
operation_failed=false
no_op_reason=""
# Authoritative failure-scope classification (fix: distinguish artifact
# failure scope). infrastructure_failure means the shared evidence
# infrastructure itself is unsafe (Azure auth, blob upload/verify,
# required tooling) -- callers MUST stop the suite. scenario_incomplete
# means only THIS scenario's own artifact set is incomplete (missing/
# invalid worker summary, missing/invalid role snapshots, relabel
# failure) -- callers should invalidate this scenario's measurement but
# MUST NOT by itself stop later scenarios.
infrastructure_failure=false
scenario_incomplete=false
azure_oidc_refresh_enabled=false
azure_oidc_configuration_valid=true
azure_login_refreshed_at=0
azure_auth_failed=false
azure_auth_failure_reported=false
azure_oidc_refresh_interval_seconds="${AZURE_OIDC_REFRESH_INTERVAL_SECONDS:-2700}"
artifact_upload_concurrency="${CL2_ARTIFACT_UPLOAD_CONCURRENCY:-8}"

mkdir -p "$SCENARIO_REPORT_DIR"
rm -f -- "$summary_tmp"
artifact_work_dir=$(mktemp -d "${TMPDIR:-/tmp}/scenario-artifacts-XXXXXX")
cleanup_temp_files() {
  rm -f -- "$summary_tmp"
  rm -rf -- "$artifact_work_dir"
}
trap cleanup_temp_files EXIT

array_json() {
  if [ "$#" -eq 0 ]; then
    printf '[]'
    return
  fi
  printf '%s\n' "$@" | jq -R . | jq -s .
}

contains_value() {
  local needle="$1"
  shift
  local value
  for value in "$@"; do
    if [ "$value" = "$needle" ]; then
      return 0
    fi
  done
  return 1
}

add_error() {
  local message="$1"
  # Classification defaults to "scenario" (scenario_incomplete) --
  # call sites that represent shared-infrastructure problems must pass
  # "infra" explicitly. See the infrastructure_failure/scenario_incomplete
  # comment above for the authoritative definition of each scope.
  local classification="${2:-scenario}"
  errors+=("$message")
  operation_failed=true
  case "$classification" in
    infra)
      infrastructure_failure=true
      ;;
    *)
      scenario_incomplete=true
      ;;
  esac
  echo "$message" >&2
}

load_azure_oidc_context() {
  local decoded

  if [ -z "${AZURE_OIDC_CONTEXT_B64:-}" ]; then
    return 0
  fi
  if ! decoded=$(
    printf '%s' "$AZURE_OIDC_CONTEXT_B64" | base64 --decode 2>/dev/null
  ); then
    add_error "Refreshable Azure authentication context is invalid" infra
    return 1
  fi
  if ! SYSTEM_OIDCREQUESTURI=$(jq -er '.request_uri' <<<"$decoded") ||
     ! SYSTEM_ACCESSTOKEN=$(jq -er '.request_token' <<<"$decoded") ||
     ! AZURESUBSCRIPTION_SERVICE_CONNECTION_ID=$(
       jq -er '.service_connection_id' <<<"$decoded"
     ) ||
     ! AZURESUBSCRIPTION_CLIENT_ID=$(jq -er '.client_id' <<<"$decoded") ||
     ! AZURESUBSCRIPTION_TENANT_ID=$(jq -er '.tenant_id' <<<"$decoded"); then
    add_error "Refreshable Azure authentication context is malformed" infra
    return 1
  fi
  CL2_REQUIRE_AZURE_OIDC_REFRESH=true
}

configure_azure_oidc_refresh() {
  local required="${CL2_REQUIRE_AZURE_OIDC_REFRESH:-false}"
  local present=0
  local -a names=(
    SYSTEM_OIDCREQUESTURI
    SYSTEM_ACCESSTOKEN
    AZURESUBSCRIPTION_SERVICE_CONNECTION_ID
    AZURESUBSCRIPTION_CLIENT_ID
    AZURESUBSCRIPTION_TENANT_ID
  )
  local name

  required="${required,,}"
  if [ "$required" != "true" ] && [ "$required" != "false" ]; then
    add_error \
      "CL2_REQUIRE_AZURE_OIDC_REFRESH must be true or false, got $required" \
      infra
    return 1
  fi
  if ! [[ "$azure_oidc_refresh_interval_seconds" =~ ^[0-9]+$ ]]; then
    add_error \
      "AZURE_OIDC_REFRESH_INTERVAL_SECONDS must be a non-negative integer" \
      infra
    return 1
  fi

  for name in "${names[@]}"; do
    if [ -n "${!name:-}" ]; then
      present=$((present + 1))
    fi
  done
  if [ "$required" = "false" ] && [ "$present" -eq 0 ]; then
    return 0
  fi

  for name in "${names[@]}"; do
    if [ -z "${!name:-}" ]; then
      add_error \
        "Refreshable Azure authentication is missing $name" \
        infra
      return 1
    fi
  done
  azure_oidc_refresh_enabled=true
}

refresh_azure_login() {
  local separator="?" request_url response oidc_token actual_subscription_id
  local attempt

  if [[ "$SYSTEM_OIDCREQUESTURI" == *"?"* ]]; then
    separator="&"
  fi
  request_url="${SYSTEM_OIDCREQUESTURI}${separator}api-version=7.1&serviceConnectionId=${AZURESUBSCRIPTION_SERVICE_CONNECTION_ID}"

  for attempt in 1 2 3; do
    response=""
    oidc_token=""
    if response=$(curl \
        --silent \
        --show-error \
        --fail \
        --request POST \
        --header "Content-Length: 0" \
        --header "Content-Type: application/json" \
        --header "Authorization: Bearer ${SYSTEM_ACCESSTOKEN}" \
        "$request_url") &&
       oidc_token=$(jq -er '
         .oidcToken
         | select(type == "string" and length > 0)
       ' <<<"$response") &&
       az login \
         --service-principal \
         --username "$AZURESUBSCRIPTION_CLIENT_ID" \
         --tenant "$AZURESUBSCRIPTION_TENANT_ID" \
         --allow-no-subscriptions \
         --federated-token "$oidc_token" \
         --output none &&
       az account set --subscription "$TARGET_SUBSCRIPTION_ID" &&
       actual_subscription_id=$(
         az account show --query id --output tsv 2>/dev/null
       ) &&
       [[ "${actual_subscription_id,,}" == "${TARGET_SUBSCRIPTION_ID,,}" ]]; then
      azure_login_refreshed_at=$(date +%s)
      echo "Refreshed Azure workload-identity session for artifact preservation."
      return 0
    fi
    if [ "$attempt" -lt 3 ]; then
      sleep $((attempt * 2))
    fi
  done
  return 1
}

ensure_azure_login() {
  local now

  if [ "$azure_oidc_refresh_enabled" != "true" ]; then
    return 0
  fi
  if [ "$azure_auth_failed" = "true" ]; then
    return 1
  fi

  now=$(date +%s)
  if [ "$azure_login_refreshed_at" -gt 0 ] &&
     [ $((now - azure_login_refreshed_at)) -lt \
       "$azure_oidc_refresh_interval_seconds" ]; then
    return 0
  fi
  if refresh_azure_login; then
    return 0
  fi
  azure_auth_failed=true
  return 1
}

load_worker_summary() {
  if [ ! -s "$WORKER_SUMMARY_FILE" ]; then
    return 1
  fi
  if ! jq -e '
      (.succeeded_count | type == "number") and
      (.succeeded_count >= 0) and
      (.succeeded_count == (.succeeded_count | floor)) and
      (.succeeded_roles | type == "array") and
      all(.succeeded_roles[];
        (type == "string") and
        (length > 0) and
        (contains("/") | not) and
        (. != ".") and
        (. != "..")) and
      (.succeeded_count == (.succeeded_roles | length)) and
      (.succeeded_count == ([.succeeded_roles[]] | unique | length))
    ' "$WORKER_SUMMARY_FILE" >/dev/null; then
    return 1
  fi
  mapfile -t expected_roles < <(
    jq -r '.succeeded_roles[]' "$WORKER_SUMMARY_FILE" | sort
  )
  expected_successful_worker_count="${#expected_roles[@]}"
  worker_summary_valid=true
}

write_summary() {
  local success="$1"
  local expected_roles_json uploaded_roles_json missing_roles_json errors_json
  local uploaded_total_bytes lifecycle_only_json
  expected_roles_json=$(array_json "${expected_roles[@]}")
  uploaded_roles_json=$(array_json "${uploaded_snapshot_roles[@]}")
  missing_roles_json=$(array_json "${missing_snapshot_roles[@]}")
  errors_json=$(array_json "${errors[@]}")
  uploaded_total_bytes=$((uploaded_snapshot_bytes + uploaded_audit_bytes + uploaded_acns_bytes + uploaded_lifecycle_bytes))
  if [ "${lifecycle_only,,}" = "true" ]; then
    lifecycle_only_json=true
  else
    lifecycle_only_json=false
  fi

  if ! jq -n \
      --arg scenario "$SCENARIO_NAME" \
      --arg report_dir "$SCENARIO_REPORT_DIR" \
      --arg run_id "$RUN_ID" \
      --arg build_id "$BUILD_ID" \
      --arg snapshot_tier "$SNAPSHOT_TIER" \
      --arg build_branch "$BUILD_BRANCH" \
      --arg storage_account "$STORAGE_ACCOUNT_NAME" \
      --arg container "$CONTAINER_NAME" \
      --arg snapshot_enabled "$snapshot_enabled" \
      --arg snapshot_target "$snapshot_target" \
      --arg no_op_reason "$no_op_reason" \
      --argjson worker_summary_valid "$worker_summary_valid" \
      --argjson expected_successful_worker_count \
        "$expected_successful_worker_count" \
      --argjson expected_successful_worker_roles "$expected_roles_json" \
      --argjson uploaded_snapshot_count "$uploaded_snapshot_count" \
      --argjson uploaded_snapshot_roles "$uploaded_roles_json" \
      --argjson missing_snapshot_roles "$missing_roles_json" \
      --argjson uploaded_snapshot_bytes "$uploaded_snapshot_bytes" \
      --argjson uploaded_audit_count "$uploaded_audit_count" \
      --argjson uploaded_audit_bytes "$uploaded_audit_bytes" \
      --argjson uploaded_acns_count "$uploaded_acns_count" \
      --argjson uploaded_acns_bytes "$uploaded_acns_bytes" \
      --argjson uploaded_lifecycle_count "$uploaded_lifecycle_count" \
      --argjson uploaded_lifecycle_bytes "$uploaded_lifecycle_bytes" \
      --argjson uploaded_total_bytes "$uploaded_total_bytes" \
      --argjson errors "$errors_json" \
      --argjson success "$success" \
      --argjson infrastructure_failure "$infrastructure_failure" \
      --argjson scenario_incomplete "$scenario_incomplete" \
      --argjson lifecycle_only "$lifecycle_only_json" \
      '{
        schema_version: 1,
        scenario: $scenario,
        scenario_report_dir: $report_dir,
        run_id: $run_id,
        build_id: $build_id,
        snapshot_tier: $snapshot_tier,
        build_branch: $build_branch,
        storage_account: $storage_account,
        container: $container,
        snapshot_enabled: ($snapshot_enabled | ascii_downcase == "true"),
        snapshot_target: $snapshot_target,
        lifecycle_only: $lifecycle_only,
        no_op_reason: (if $no_op_reason == "" then null else $no_op_reason end),
        worker_summary_valid: $worker_summary_valid,
        expected_successful_worker_count: $expected_successful_worker_count,
        expected_successful_worker_roles: $expected_successful_worker_roles,
        uploaded_snapshot_count: $uploaded_snapshot_count,
        uploaded_snapshot_roles: $uploaded_snapshot_roles,
        missing_snapshot_roles: $missing_snapshot_roles,
        uploaded_snapshot_bytes: $uploaded_snapshot_bytes,
        uploaded_audit_count: $uploaded_audit_count,
        uploaded_audit_bytes: $uploaded_audit_bytes,
        uploaded_acns_count: $uploaded_acns_count,
        uploaded_acns_bytes: $uploaded_acns_bytes,
        uploaded_lifecycle_count: $uploaded_lifecycle_count,
        uploaded_lifecycle_bytes: $uploaded_lifecycle_bytes,
        uploaded_total_bytes: $uploaded_total_bytes,
        errors: $errors,
        success: $success,
        infrastructure_failure: $infrastructure_failure,
        scenario_incomplete: $scenario_incomplete
      }' > "$summary_tmp"; then
    echo "Failed to write preservation summary $summary_tmp" >&2
    return 1
  fi
  mv -f -- "$summary_tmp" "$summary_file"
}

handle_termination() {
  local signal_name="$1"
  trap - TERM INT
  add_error \
    "Artifact preservation interrupted by $signal_name before completion" \
    infra
  write_summary false || true
  exit 124
}

trap 'handle_termination TERM' TERM
trap 'handle_termination INT' INT

record_uploaded_artifact() {
  local artifact_type="$1"
  local size="$2"
  local role="${3:-}"

  case "$artifact_type" in
    snapshot)
      uploaded_snapshot_count=$((uploaded_snapshot_count + 1))
      uploaded_snapshot_bytes=$((uploaded_snapshot_bytes + size))
      if ! contains_value "$role" "${uploaded_snapshot_roles[@]}"; then
        uploaded_snapshot_roles+=("$role")
      fi
      ;;
    audit)
      uploaded_audit_count=$((uploaded_audit_count + 1))
      uploaded_audit_bytes=$((uploaded_audit_bytes + size))
      ;;
    acns)
      uploaded_acns_count=$((uploaded_acns_count + 1))
      uploaded_acns_bytes=$((uploaded_acns_bytes + size))
      ;;
    lifecycle)
      uploaded_lifecycle_count=$((uploaded_lifecycle_count + 1))
      uploaded_lifecycle_bytes=$((uploaded_lifecycle_bytes + size))
      ;;
  esac
}

upload_and_verify() {
  local file="$1"
  local blob_name="$2"
  local artifact_type="$3"
  local role="${4:-}"
  local delete_after_upload="${5:-false}"
  local size remote_size

  size=$(stat -c%s "$file" 2>/dev/null || printf '0')
  echo "Uploading $file (${size} bytes) -> ${STORAGE_ACCOUNT_NAME}/${CONTAINER_NAME}/${blob_name}"
  if ! ensure_azure_login; then
    if [ "$azure_auth_failure_reported" != "true" ]; then
      add_error \
        "Unable to refresh Azure authentication for artifact upload" \
        infra
      azure_auth_failure_reported=true
    fi
    return 1
  fi
  if ! az storage blob upload \
      --account-name "$STORAGE_ACCOUNT_NAME" \
      --container-name "$CONTAINER_NAME" \
      --name "$blob_name" \
      --file "$file" \
      --auth-mode login \
      --overwrite \
      --output none; then
    add_error "Upload failed for $file" infra
    return 1
  fi

  if ! ensure_azure_login; then
    if [ "$azure_auth_failure_reported" != "true" ]; then
      add_error \
        "Unable to refresh Azure authentication for upload verification" \
        infra
      azure_auth_failure_reported=true
    fi
    return 1
  fi
  if ! remote_size=$(az storage blob show \
      --account-name "$STORAGE_ACCOUNT_NAME" \
      --container-name "$CONTAINER_NAME" \
      --name "$blob_name" \
      --auth-mode login \
      --query properties.contentLength \
      --output tsv); then
    add_error "Upload verification failed for $file" infra
    return 1
  fi
  remote_size=${remote_size//$'\r'/}
  if ! [[ "$remote_size" =~ ^[0-9]+$ ]] || [ "$remote_size" -ne "$size" ]; then
    add_error "Upload verification size mismatch for $file: local=$size remote=${remote_size:-unknown}" infra
    return 1
  fi

  record_uploaded_artifact "$artifact_type" "$size" "$role"

  if [ "$delete_after_upload" = "true" ]; then
    if ! rm -f -- "$file"; then
      echo "Warning: unable to remove verified snapshot $file" >&2
    fi
  fi
}

flush_parallel_upload_batch() {
  local artifact_type="$1"
  local delete_after_upload="$2"
  local index

  for index in "${!parallel_upload_pids[@]}"; do
    if wait "${parallel_upload_pids[$index]}"; then
      record_uploaded_artifact \
        "$artifact_type" \
        "${parallel_upload_sizes[$index]}" \
        "${parallel_upload_roles[$index]}"
      if [ "$delete_after_upload" = "true" ] &&
         ! rm -f -- "${parallel_upload_files[$index]}"; then
        echo "Warning: unable to remove verified snapshot ${parallel_upload_files[$index]}" >&2
      fi
    else
      add_error \
        "Upload failed or verification failed for ${parallel_upload_files[$index]}" \
        infra
    fi
  done

  parallel_upload_pids=()
  parallel_upload_files=()
  parallel_upload_roles=()
  parallel_upload_sizes=()
}

upload_entries_parallel() {
  local artifact_type="$1"
  local delete_after_upload="$2"
  local entries_file="$3"
  local file blob_name role size remote_size
  local -a parallel_upload_pids=()
  local -a parallel_upload_files=()
  local -a parallel_upload_roles=()
  local -a parallel_upload_sizes=()

  while IFS=$'\t' read -r file blob_name role; do
    [ -n "$file" ] || continue
    if [ "${#parallel_upload_pids[@]}" -eq 0 ] &&
       ! ensure_azure_login; then
      if [ "$azure_auth_failure_reported" != "true" ]; then
        add_error \
          "Unable to refresh Azure authentication for parallel artifact upload" \
          infra
        azure_auth_failure_reported=true
      fi
      return 1
    fi

    size=$(stat -c%s "$file" 2>/dev/null || printf '0')
    echo "Uploading $file (${size} bytes) -> ${STORAGE_ACCOUNT_NAME}/${CONTAINER_NAME}/${blob_name}"
    (
      if ! az storage blob upload \
          --account-name "$STORAGE_ACCOUNT_NAME" \
          --container-name "$CONTAINER_NAME" \
          --name "$blob_name" \
          --file "$file" \
          --auth-mode login \
          --overwrite \
          --output none; then
        echo "Upload failed for $file" >&2
        exit 1
      fi
      if ! remote_size=$(az storage blob show \
          --account-name "$STORAGE_ACCOUNT_NAME" \
          --container-name "$CONTAINER_NAME" \
          --name "$blob_name" \
          --auth-mode login \
          --query properties.contentLength \
          --output tsv); then
        echo "Upload verification failed for $file" >&2
        exit 1
      fi
      remote_size=${remote_size//$'\r'/}
      if ! [[ "$remote_size" =~ ^[0-9]+$ ]] ||
         [ "$remote_size" -ne "$size" ]; then
        echo "Upload verification size mismatch for $file: local=$size remote=${remote_size:-unknown}" >&2
        exit 1
      fi
    ) &
    parallel_upload_pids+=("$!")
    parallel_upload_files+=("$file")
    parallel_upload_roles+=("$role")
    parallel_upload_sizes+=("$size")

    if [ "${#parallel_upload_pids[@]}" -ge \
         "$artifact_upload_concurrency" ]; then
      flush_parallel_upload_batch "$artifact_type" "$delete_after_upload"
    fi
  done < "$entries_file"

  if [ "${#parallel_upload_pids[@]}" -gt 0 ]; then
    flush_parallel_upload_batch "$artifact_type" "$delete_after_upload"
  fi
}

load_worker_summary || true

if [ "${lifecycle_only,,}" != "true" ]; then
  if [ "${snapshot_enabled,,}" != "true" ]; then
    no_op_reason="snapshots-disabled"
    if ! write_summary true; then
      exit 1
    fi
    echo "Prometheus snapshots are disabled; scenario preservation is a no-op."
    exit 0
  fi
  if [ "${snapshot_target,,}" != "blob" ]; then
    no_op_reason="snapshot-target-${snapshot_target}"
    if ! write_summary true; then
      exit 1
    fi
    echo "Prometheus snapshot target is $snapshot_target; scenario preservation is a no-op."
    exit 0
  fi
fi

# Lifecycle-only mode (the final, small-durable-state pass) deliberately
# skips the worker-summary/relabel-script requirements below -- those were
# already proven (or reported) by the early, non-lifecycle-only pass
# earlier in this same scenario; re-checking them here would just
# duplicate scenario_incomplete reasons for the same underlying facts.
if [ "${lifecycle_only,,}" != "true" ]; then
  if [ "$worker_summary_valid" != "true" ]; then
    add_error "Worker summary is missing or invalid: $WORKER_SUMMARY_FILE"
  fi
  if [ ! -f "$RELABEL_SCRIPT" ]; then
    add_error "Relabel script does not exist: $RELABEL_SCRIPT"
  fi
fi
for command_name in az jq gzip tar stat find sort base64; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    add_error "Required command is unavailable: $command_name" infra
  fi
done
if ! load_azure_oidc_context ||
   ! configure_azure_oidc_refresh; then
  azure_oidc_configuration_valid=false
fi
if [ "$azure_oidc_refresh_enabled" = "true" ]; then
  for command_name in curl date; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
      add_error "Required command is unavailable: $command_name" infra
      azure_oidc_configuration_valid=false
    fi
  done
fi
if ! [[ "$artifact_upload_concurrency" =~ ^[1-9][0-9]*$ ]] ||
   [ "$artifact_upload_concurrency" -gt 16 ]; then
  add_error \
    "CL2_ARTIFACT_UPLOAD_CONCURRENCY must be an integer from 1 through 16" \
    infra
  azure_oidc_configuration_valid=false
fi

azure_ready=true
if [ "$azure_oidc_configuration_valid" != "true" ]; then
  azure_ready=false
elif ! ensure_azure_login; then
  add_error "Unable to establish refreshable Azure authentication" infra
  azure_ready=false
elif ! az account set --subscription "$TARGET_SUBSCRIPTION_ID"; then
  add_error "Unable to select Azure subscription $TARGET_SUBSCRIPTION_ID" infra
  azure_ready=false
else
  actual_subscription_id=$(
    az account show --query id --output tsv 2>/dev/null || true
  )
  if [[ "${actual_subscription_id,,}" != "${TARGET_SUBSCRIPTION_ID,,}" ]]; then
    add_error "Expected Azure subscription $TARGET_SUBSCRIPTION_ID, got ${actual_subscription_id:-unknown}" infra
    azure_ready=false
  fi
fi

# The lifecycle name set differs by mode:
#   - normal (early) pass: worker-summary.json is included (this is the
#     first and only time it becomes durable) but scenario-health-gate.json
#     and the earlier artifact-preservation-summary.json are NOT (neither
#     exists yet at this point in the scenario lifecycle).
#   - lifecycle-only (final) pass: worker-summary.json is skipped (already
#     durable from the early pass) but scenario-health-gate.json and the
#     early artifact-preservation-summary.json ARE included, since both
#     now exist and are part of the final durable-state snapshot. The
#     final pass's OWN summary (artifact-preservation-final-summary.json)
#     is deliberately excluded here -- it is written by write_summary
#     AFTER all uploads complete (see the "Avoid recursive self-upload
#     races" note near the top of this script), so it does not exist yet
#     when this find runs, and even if it did, self-upload before the
#     content is final would be a race.
lifecycle_find_names=(
  -name 'worker-summary.json'
  -o -name 'worker-status-*.json'
  -o -name 'scenario-policy.json'
  -o -name 'scenario-evidence.json'
  -o -name 'mock-layer-reconcile-*.json'
  -o -name 'cilium-policy-guard-*.json'
  -o -name 'scenario-cleanup-reconcile.json'
  -o -name 'NodeChurnTimings_*.json'
  -o -name 'ApiserverFailureTimings_*.json'
  -o -name 'IsolationChurnTimings_*.json'
)
if [ "${lifecycle_only,,}" = "true" ]; then
  lifecycle_find_names=(
    -name 'scenario-policy.json'
    -o -name 'scenario-evidence.json'
    -o -name 'scenario-health-gate.json'
    -o -name 'mock-layer-reconcile-*.json'
    -o -name 'cilium-policy-guard-*.json'
    -o -name 'scenario-cleanup-reconcile.json'
    -o -name 'NodeChurnTimings_*.json'
    -o -name 'ApiserverFailureTimings_*.json'
    -o -name 'IsolationChurnTimings_*.json'
    -o -name 'artifact-preservation-summary.json'
  )
fi

if [ "$azure_ready" = "true" ]; then
  while IFS= read -r -d '' lifecycle_file; do
    blob_name="${BUILD_BRANCH}/lifecycle/${SCENARIO_NAME}/${RUN_ID}/$(basename "$lifecycle_file")"
    upload_and_verify "$lifecycle_file" "$blob_name" lifecycle || true
  done < <(
    find "$SCENARIO_REPORT_DIR" \
      -type f \
      \( "${lifecycle_find_names[@]}" \) \
      -print0 | sort -z
  )

  # Audit/ACNS telemetry are independent of the lifecycle-only durable-state
  # set. Per-role scenario evidence is likewise already durable from the
  # early pass; re-uploading 100 role files plus verification serially can
  # exhaust the bounded final-lifecycle budget. Only the normal pass uploads
  # these role-qualified files.
  if [ "${lifecycle_only,,}" != "true" ]; then
    while IFS= read -r -d '' role_evidence_file; do
      role=$(basename "$(dirname "$role_evidence_file")")
      blob_name="${BUILD_BRANCH}/lifecycle/${SCENARIO_NAME}/${RUN_ID}/${role}-$(basename "$role_evidence_file")"
      upload_and_verify "$role_evidence_file" "$blob_name" lifecycle || true
    done < <(
      find "$SCENARIO_REPORT_DIR" \
        -mindepth 2 \
        -maxdepth 2 \
        -type f \
        \( -name 'EventThroughputEvidence.json' \
           -o -name 'PodChurnEvidence.json' \
           -o -name 'PolicyScaleEvidence.json' \) \
        -print0 | sort -z
    )

    audit_entries="$artifact_work_dir/audit-entries.tsv"
    : > "$audit_entries"
    while IFS= read -r -d '' audit; do
      role=$(basename "$(dirname "$(dirname "$audit")")")
      extension="${audit##*.}"
      blob_name="${BUILD_BRANCH}/telemetry-audit-self-hosted/${SCENARIO_NAME}/${RUN_ID}/telemetry-audit-self-hosted-${role}.${extension}"
      printf '%s\t%s\t%s\n' \
        "$audit" "$blob_name" "$role" >> "$audit_entries"
    done < <(
      find "$SCENARIO_REPORT_DIR" \
        -mindepth 3 \
        -maxdepth 3 \
        -type f \
        \( -name 'telemetry-audit-self-hosted.json' \
           -o -name 'telemetry-audit-self-hosted.md' \) \
        -print0 | sort -z
    )
    upload_entries_parallel audit false "$audit_entries" || true

    acns_entries="$artifact_work_dir/acns-entries.tsv"
    : > "$acns_entries"
    while IFS= read -r -d '' acns_file; do
      role=$(basename "$(dirname "$(dirname "$(dirname "$acns_file")")")")
      blob_name="${BUILD_BRANCH}/acns/${SCENARIO_NAME}/${RUN_ID}/${role}/$(basename "$acns_file")"
      printf '%s\t%s\t%s\n' \
        "$acns_file" "$blob_name" "$role" >> "$acns_entries"
    done < <(
      find "$SCENARIO_REPORT_DIR" \
        -mindepth 4 \
        -maxdepth 4 \
        -type f \
        -path '*/telemetry/acns/*' \
        -print0 | sort -z
    )
    upload_entries_parallel acns false "$acns_entries" || true
  fi
fi

snapshot_files=()
relabel_ready=false
if [ "${lifecycle_only,,}" != "true" ]; then
  while IFS= read -r -d '' snapshot; do
    snapshot_files+=("$snapshot")
    if [ ! -s "$snapshot" ] ||
       ! gzip -t "$snapshot" 2>/dev/null ||
       ! tar -tzf "$snapshot" >/dev/null 2>&1; then
      add_error "Invalid Prometheus snapshot tarball: $snapshot"
    fi
  done < <(
    find "$SCENARIO_REPORT_DIR" \
      -mindepth 2 \
      -maxdepth 2 \
      -type f \
      -name 'prom-snapshot-*.tar.gz' \
      -print0 | sort -z
  )

  if [ "$azure_ready" = "true" ] &&
     [ -f "$RELABEL_SCRIPT" ] &&
     [ "${#snapshot_files[@]}" -gt 0 ]; then
    if CL2_REPORT_DIR="$SCENARIO_REPORT_DIR" \
        RUN_ID="$RUN_ID" \
        BUILD_ID="$BUILD_ID" \
        SNAPSHOT_TIER="$SNAPSHOT_TIER" \
        bash "$RELABEL_SCRIPT"; then
      relabel_ready=true
    else
      add_error "Prometheus snapshot relabeling failed for $SCENARIO_NAME"
    fi
  elif [ "${#snapshot_files[@]}" -eq 0 ]; then
    relabel_ready=true
  fi

  if [ "$azure_ready" = "true" ] && [ "$relabel_ready" = "true" ]; then
    snapshot_entries="$artifact_work_dir/snapshot-entries.tsv"
    : > "$snapshot_entries"
    for snapshot in "${snapshot_files[@]}"; do
      role=$(basename "$(dirname "$snapshot")")
      if [ ! -s "$snapshot" ] ||
         ! gzip -t "$snapshot" 2>/dev/null ||
         ! tar -tzf "$snapshot" >/dev/null 2>&1; then
        add_error "Relabeled Prometheus snapshot is invalid: $snapshot"
        continue
      fi
      blob_name="${BUILD_BRANCH}/${SCENARIO_NAME}/${RUN_ID}/$(basename "$snapshot")"
      printf '%s\t%s\t%s\n' \
        "$snapshot" "$blob_name" "$role" >> "$snapshot_entries"
    done
    upload_entries_parallel snapshot true "$snapshot_entries" || true
  fi

  if [ "${#uploaded_snapshot_roles[@]}" -gt 0 ]; then
    mapfile -t uploaded_snapshot_roles < <(
      printf '%s\n' "${uploaded_snapshot_roles[@]}" | sort -u
    )
  fi
  for role in "${expected_roles[@]}"; do
    if ! contains_value "$role" "${uploaded_snapshot_roles[@]}"; then
      missing_snapshot_roles+=("$role")
    fi
  done
  if [ "${#missing_snapshot_roles[@]}" -gt 0 ]; then
    add_error "Missing verified snapshot uploads for successful worker role(s): ${missing_snapshot_roles[*]}"
  fi
fi

if [ "$operation_failed" = "true" ]; then
  write_summary false || true
  exit 1
fi

if ! write_summary true; then
  exit 1
fi
echo "Preserved $uploaded_snapshot_count snapshot(s), $uploaded_audit_count telemetry audit file(s), $uploaded_acns_count ACNS file(s), and $uploaded_lifecycle_count lifecycle file(s) for $SCENARIO_NAME."
