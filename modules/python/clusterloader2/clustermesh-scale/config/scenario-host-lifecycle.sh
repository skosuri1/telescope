#!/usr/bin/env bash

# Launch node-churner.sh for the named scenario; populates
# NODE_CHURNER_PID. Caller must:
#   - mkdir -p the per-cluster target report dir BEFORE calling so
#     the churner has a writable place for NodeChurnTimings_*.json
#   - call `wait $NODE_CHURNER_PID` after execute-parallel returns
#   - unset NODE_CHURNER_PID after wait
launch_node_churner() {
  local _scen="$1" _report_dir_base="$2"
  # Discover target cluster + kubeconfig from the augmented clusters
  # JSON written to $HOME/.kube/clustermesh-clusters.json. The shell
  # `$clusters` var in this script is the EARLY discovery output
  # WITHOUT the kubeconfig field; using it here gave node-churner an
  # empty TARGET_KUBECONFIG arg in build 67126.
  local _all _target_role _target_row
  _all=$(cat "$HOME/.kube/clustermesh-clusters.json" 2>/dev/null || echo "[]")
  _target_role="${CL2_NODE_CHURN_TARGET_CONTEXT}"
  # Map role → AKS name + RG. Our tfvars set aks_name == role-derived
  # name (e.g., role=mesh-1 → name=clustermesh-1), and `az aks
  # get-credentials` writes kubectl context = AKS name. So
  # CL2_NODE_CHURN_TARGET_CONTEXT is the AKS cluster name.
  _target_row=$(echo "$_all" | jq -c --arg n "$_target_role" '.[] | select(.name==$n)')
  if [ -z "$_target_row" ]; then
    # Fallback: maybe the user set NODE_CHURN_TARGET_CONTEXT to a role.
    _target_row=$(echo "$_all" | jq -c --arg r "$_target_role" '.[] | select(.role==$r)')
  fi
  if [ -z "$_target_row" ]; then
    echo "##vso[task.logissue type=warning;] node-churner: target cluster '${_target_role}' not found in discovered clusters; skipping scenario stimulus"
    NODE_CHURNER_PID=""
    return 0
  fi
  local _target_name _target_rg _target_role_field _target_kubeconfig
  _target_name=$(echo "$_target_row" | jq -r '.name')
  _target_rg=$(echo "$_target_row" | jq -r '.rg')
  _target_role_field=$(echo "$_target_row" | jq -r '.role')
  _target_kubeconfig=$(echo "$_target_row" | jq -r '.kubeconfig // ""')

  # Per-scenario expected duration (matches the CL2 sleep window).
  local _expected_dur
  case "$_scen" in
    node-churn-scale)    _expected_dur="$CL2_NODE_CHURN_SCALE_DURATION_SECONDS" ;;
    node-churn-replace)  _expected_dur="$CL2_NODE_CHURN_REPLACE_DURATION_SECONDS" ;;
    node-churn-combined) _expected_dur="$CL2_NODE_CHURN_COMBINED_DURATION_SECONDS" ;;
    *)                   _expected_dur=1500 ;;
  esac

  # Clear sentinels for THIS scenario so the prior scenario's
  # leftovers (if any) don't pre-trigger the barrier.
  rm -f "$SENTINEL_DIR"/ready-* 2>/dev/null || true

  # Target report dir for NodeChurnTimings_*.json. Pre-create so
  # node-churner.sh can write even before CL2 finishes for that
  # cluster (CL2 lazy-creates report dirs).
  local _target_report_dir="${_report_dir_base}/${_target_role_field}"
  mkdir -p "$_target_report_dir"

  local _churner_log="${_target_report_dir}/node-churner.log"
  echo "===== node-churner launch: scenario=${_scen} target=${_target_name} rg=${_target_rg} =====" | tee -a "$_churner_log"

  # Dedicated process group ensures a scenario timeout reaches the
  # churner and its Azure CLI children together. Redirect directly to
  # the log (no tee pipeline) so the EXIT finalizer keeps a live output
  # sink after SIGTERM while it restores the pool.
  node_churn_cleanup_grace=$(( \
    CL2_NODE_CHURN_FINALIZER_TIMEOUT_SECONDS +
    CL2_NODE_CHURN_RECOVERY_GRACE_SECONDS +
    60 ))
  node_churn_runtime=$(( \
    _expected_dur +
    CL2_NODE_CHURN_READY_TIMEOUT_SECONDS +
    CL2_NODE_CHURN_FINALIZER_TIMEOUT_SECONDS +
    CL2_NODE_CHURN_RECOVERY_GRACE_SECONDS +
    60 ))
  setsid timeout --signal=TERM \
    --kill-after="${node_churn_cleanup_grace}s" \
    "${node_churn_runtime}s" \
    bash "$NODE_CHURNER_SCRIPT" \
      "$_scen" \
      "$_target_name" \
      "$_target_rg" \
      "$CL2_NODE_CHURN_TARGET_NODEPOOL" \
      "$_target_report_dir" \
      "$SENTINEL_DIR" \
      "$cluster_count" \
      "$CL2_NODE_CHURN_CYCLES" \
      "$CL2_NODE_CHURN_DELTA" \
      "$CL2_NODE_CHURN_SETTLE_SECONDS" \
      "$CL2_NODE_REPLACE_BATCH_SIZE" \
      "$CL2_NODE_CHURN_READY_TIMEOUT_SECONDS" \
      "$_expected_dur" \
      "$_target_kubeconfig" >> "$_churner_log" 2>&1 &
  NODE_CHURNER_PID=$!
  NODE_CHURNER_LOG="$_churner_log"
  echo "node-churner: launched PID=$NODE_CHURNER_PID for scenario=${_scen}; log=${_churner_log}"
}

# Wait helper — caller invokes after execute-parallel returns.
wait_node_churner() {
  local _scen="$1"
  NODE_CHURNER_WAIT_RC=0
  if [ -z "${NODE_CHURNER_PID:-}" ]; then
    return 0
  fi
  echo "node-churner: waiting on PID=$NODE_CHURNER_PID for scenario=${_scen}"
  wait "$NODE_CHURNER_PID" || NODE_CHURNER_WAIT_RC=$?
  if [ -s "${NODE_CHURNER_LOG:-}" ]; then
    cat "$NODE_CHURNER_LOG"
  fi
  if [ "$NODE_CHURNER_WAIT_RC" -ne 0 ]; then
    echo "##vso[task.logissue type=warning;] node-churner: scenario=${_scen} exited rc=${NODE_CHURNER_WAIT_RC}; cleanup completion is unverifiable"
  fi
  NODE_CHURNER_PID=""
  NODE_CHURNER_LOG=""
  return 0
}

# Persist failure-time cluster, mesh, scenario, and churn diagnostics.
scenario_failure_diag() {
  local _scen="$1" _rc="${2:-0}"
  local _diag_dir="${CL2_REPORT_DIR}/_debug"
  mkdir -p "$_diag_dir"
  local _diag_log="${_diag_dir}/scenario-diag-${_scen}.log"
  # Use the augmented inventory because it contains kubeconfig paths.
  local _clusters_with_kc
  _clusters_with_kc=$(cat "$HOME/.kube/clustermesh-clusters.json" 2>/dev/null || echo "[]")
  {
    echo "================================================================"
    echo "=== scenario-failure-diag: scenario=${_scen} rc=${_rc}"
    echo "=== timestamp: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "================================================================"
    echo ""
    echo "-- clusters JSON (kubeconfig-augmented) --"
    echo "$_clusters_with_kc" | jq . 2>&1 || echo "$_clusters_with_kc"
    echo ""
    if [ -f "${SHARE_INFRA_META:-/nonexistent}" ]; then
      echo "-- share-infra meta --"
      jq . "$SHARE_INFRA_META" 2>&1 || cat "$SHARE_INFRA_META"
      echo ""
    fi
    echo "-- per-cluster state --"
    for _row in $(echo "$_clusters_with_kc" | jq -c '.[]'); do
      local _role _name _kc
      _role=$(echo "$_row" | jq -r '.role')
      _name=$(echo "$_row" | jq -r '.name')
      _kc=$(echo "$_row" | jq -r '.kubeconfig')
      echo "--- cluster ${_role} (${_name}, kubeconfig=${_kc}) ---"
      if [ ! -f "$_kc" ]; then
        echo "(kubeconfig file missing: ${_kc})"
        continue
      fi
      echo "-- nodes --"
      KUBECONFIG="$_kc" kubectl --context "$_name" get nodes -o wide 2>&1 | head -40 || echo "(kubectl get nodes failed)"
      echo "-- nodes providerID --"
      KUBECONFIG="$_kc" kubectl --context "$_name" get nodes \
        -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.providerID}{"\n"}{end}' 2>&1 | head -40 || true
      echo "-- kube-system pods (clustermesh/cilium) --"
      KUBECONFIG="$_kc" kubectl --context "$_name" -n kube-system get pods \
        -l 'k8s-app in (clustermesh-apiserver,cilium)' -o wide 2>&1 | head -20 || true
      echo "-- recent kube-system events --"
      KUBECONFIG="$_kc" kubectl --context "$_name" -n kube-system get events \
        --sort-by=.lastTimestamp 2>&1 | tail -20 || true
      # Capture the APF/KWOK signals behind prior readiness collapse.
      echo "-- APF rejections by priority (non-zero; kwok status-PATCHes live in workload-low) --"
      KUBECONFIG="$_kc" kubectl --context "$_name" get --raw /metrics 2>/dev/null \
        | awk '/^apiserver_flowcontrol_rejected_requests_total/ && $NF+0 != 0 { if (++n <= 20) print } END { if (n==0) exit 1 }' \
        || echo "(no non-zero APF rejections / metrics unavailable)"
      echo "-- kwok-controller pod + restart count --"
      KUBECONFIG="$_kc" kubectl --context "$_name" -n kube-system get pods \
        -l 'app=kwok-controller' -o wide 2>&1 | head -10 || true
      echo "-- kwok-controller client-side throttling (last 100 log lines) --"
      KUBECONFIG="$_kc" kubectl --context "$_name" -n kube-system logs \
        -l 'app=kwok-controller' --tail=100 2>/dev/null \
        | grep -iE 'throttl|client-side|Waited for|429' | tail -15 || echo "(no throttle log lines)"
      echo "-- fresh deployment readiness tally (fresh API read; bypasses CL2 informer -> real vs cosmetic) --"
      KUBECONFIG="$_kc" kubectl --context "$_name" get deploy -A --no-headers 2>/dev/null \
        | awk '{n=split($3,a,"/"); if(n==2){rd+=a[1]; ds+=a[2]; if(a[1]<a[2]) under++}} END{printf "  deployments=%d under-ready=%d readyReplicas=%d desiredReplicas=%d\n", NR, under+0, rd+0, ds+0}' \
        || echo "(deploy tally failed)"
      echo "-- kwok-controller APF objects present? (Step 1.5) --"
      KUBECONFIG="$_kc" kubectl --context "$_name" get \
        flowschema/kwok-controller prioritylevelconfiguration/kwok-controller 2>&1 | head -6 \
        || echo "(kwok-controller APF objects MISSING — provision-kwok-layer Step 1.5 did not land)"
      echo ""
    done
    echo "-- sentinel dir contents (${SENTINEL_DIR:-unset}) --"
    ls -la "${SENTINEL_DIR:-/nonexistent}" 2>&1 || echo "(sentinel dir missing)"
    echo ""
    if is_node_churn_scenario "$_scen"; then
      echo "-- node-churn timing files + logs --"
      # Search both share-infra and single-scenario report layouts.
      find "${CL2_REPORT_DIR}" \
        \( -name 'NodeChurnTimings_*.json' -o -name 'node-churner*.log' \) \
        2>/dev/null | while IFS= read -r _f; do
        echo "--- ${_f} ---"
        cat "$_f" 2>&1 || true
        echo ""
      done || true
    fi
    if is_upper_bound_scenario "$_scen"; then
      echo "-- upper-bound scenario state --"
      echo "-- CL2_SATURATION_* env (as passed into CL2) --"
      env | grep -E '^CL2_SATURATION_' 2>&1 || echo "(no CL2_SATURATION_* env vars)"
      echo ""
      echo "-- rendered overrides.yaml (CL2 sees this — verifies scale.py configure landed the saturation knobs) --"
      if [ -f "${CL2_CONFIG_DIR}/overrides.yaml" ]; then
        grep -E '^CL2_(SATURATION|NAMESPACES|DEPLOYMENTS|REPLICAS)' "${CL2_CONFIG_DIR}/overrides.yaml" 2>&1 || true
      else
        echo "(${CL2_CONFIG_DIR}/overrides.yaml does not exist)"
      fi
      echo ""
      # Per-cluster: which rung measurement files made it to disk?
      # If a rung is missing entirely, the classifier flags rung_completed=false;
      # this dump tells postmortem WHY (e.g. CL2 timed out mid-rung,
      # Prometheus pod was Pending, restart-burst hung).
      for _row in $(echo "$_clusters_with_kc" | jq -c '.[]'); do
        local _role _name _kc
        _role=$(echo "$_row" | jq -r '.role')
        _name=$(echo "$_row" | jq -r '.name')
        _kc=$(echo "$_row" | jq -r '.kubeconfig')
        # Single-scenario mode: report dir is <CL2_REPORT_DIR>/<role>.
        # Share-infra mode: <CL2_REPORT_DIR>/<scenario>/<role>. Try both.
        local _report_dir="${CL2_REPORT_DIR}/${_scen}/${_role}"
        if [ ! -d "$_report_dir" ]; then
          _report_dir="${CL2_REPORT_DIR}/${_role}"
        fi
        echo "--- cluster ${_role} (${_name}) report dir: ${_report_dir} ---"
        echo "-- per-rung measurement file counts --"
        for _rung in 0 1 2 3 4 5 6 7; do
          # CL2 emits filenames like "GenericPrometheusQuery <metricName> Rung<N>_<group>_<ts>.json"
          # with a SPACE between method and metric name (build 67211 verified).
          # Match both space and legacy underscore conventions via "GenericPrometheusQuery*".
          local _count
          _count=$(find "${_report_dir}" -maxdepth 1 -name "GenericPrometheusQuery*Rung${_rung}_*.json" 2>/dev/null | wc -l)
          if [ "$_count" -gt 0 ]; then
            echo "  Rung${_rung}: ${_count} measurement files"
          fi
        done
        echo "-- junit.xml (CL2 phase pass/fail per rung) --"
        if [ -f "${_report_dir}/junit.xml" ]; then
          head -200 "${_report_dir}/junit.xml" 2>&1 || true
        else
          echo "(no junit.xml — CL2 likely failed before gathering measurements)"
        fi
        echo "-- monitoring/prometheus pod status (saturation can OOM Prom) --"
        if [ -f "$_kc" ]; then
          KUBECONFIG="$_kc" kubectl --context "$_name" -n monitoring get pods \
            -o wide 2>&1 | head -20 || echo "(kubectl get pods -n monitoring failed)"
          echo "-- clustermesh-apiserver pod resource state (OOM/Restart signals) --"
          KUBECONFIG="$_kc" kubectl --context "$_name" -n kube-system describe pod \
            -l 'k8s-app=clustermesh-apiserver' 2>&1 \
            | grep -E 'OOMKilled|Last State|Restart Count|Ready:' \
            | head -30 || true
        else
          echo "(kubeconfig missing: ${_kc})"
        fi
        echo ""
      done
    fi
    echo "=== end scenario-failure-diag ==="
  } 2>&1 | tee -a "$_diag_log"
  echo "scenario-failure-diag: wrote ${_diag_log}"
}
