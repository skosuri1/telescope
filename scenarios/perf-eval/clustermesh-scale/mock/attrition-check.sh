#!/usr/bin/env bash
# attrition-check.sh — NON-FATAL liveness check for the mock-cilium-agent layer.
#
# Compares the number of *Running* mock-cilium-agents against the KWOK virtual
# nodes they are meant to serve (1 agent per node) and reports any gaps:
#   - virtual nodes with NO Running agent serving them (lost coverage), and
#   - agent pods that are not Running (Pending / CrashLoopBackOff / Failed / ...).
#
# By design this NEVER fails the caller: it always exits 0 and only prints
# OK / WARN lines, so it is safe to drop into a scale-test loop or cron without
# aborting the run on transient attrition. Current agents are owned by the
# kwok-node StatefulSet, so gaps should self-heal; this check reports whether
# controller convergence has actually happened.
#
# Usage:
#   KUBECONFIG_FILE=~/.kube/mockmesh3-1 ./attrition-check.sh
#   # several clusters in one pass:
#   KUBECONFIG_FILES="$HOME/.kube/mockmesh3-1 $HOME/.kube/mockmesh3-2" ./attrition-check.sh
#
# Optional:
#   AGENT_NS      agent namespace                   (default mock-clustermesh)
#   AGENT_LABEL   agent pod label selector          (default app=mock-cilium-agent)
#   NODE_LABEL    KWOK node label selector          (default type=kwok)
#   SERVES_LABEL  legacy per-agent serves-node label key (migration fallback)
#
# Deliberately NO `set -e`: this check must never abort whatever invoked it.
set -uo pipefail

AGENT_NS="${AGENT_NS:-mock-clustermesh}"
AGENT_LABEL="${AGENT_LABEL:-app=mock-cilium-agent}"
NODE_LABEL="${NODE_LABEL:-type=kwok}"
SERVES_LABEL="${SERVES_LABEL:-mock-clustermesh/serves-node}"

# Resolve the set of kubeconfigs to check.
if [[ -n "${KUBECONFIG_FILES:-}" ]]; then
  read -r -a KCS <<< "${KUBECONFIG_FILES}"
elif [[ -n "${KUBECONFIG_FILE:-}" ]]; then
  KCS=("${KUBECONFIG_FILE}")
else
  echo "WARN: set KUBECONFIG_FILE=<path> (or KUBECONFIG_FILES=\"<p1> <p2>\"). Nothing to check."
  exit 0   # non-fatal even on misconfiguration
fi

overall_gap=0

for KC in "${KCS[@]}"; do
  KC="${KC/#\~/$HOME}"                          # expand a leading ~
  K() { kubectl --kubeconfig="$KC" "$@"; }
  CTX="$(basename "$KC")"

  if ! K version --request-timeout=10s >/dev/null 2>&1; then
    echo "── ${CTX} ───────────────────────────────"
    echo "   WARN: cluster unreachable via ${KC} (skipping, not failing)."
    overall_gap=1
    continue
  fi

  # Expected = KWOK virtual nodes.
  mapfile -t NODES < <(K get nodes -l "${NODE_LABEL}" \
      -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | sed '/^$/d' | sort)
  expected="${#NODES[@]}"

  agents_json=$(K -n "${AGENT_NS}" get pods -l "${AGENT_LABEL}" -o json \
    2>/dev/null || echo '{"items":[]}')

  # Served = distinct logical Nodes that currently have a Running agent.
  # StatefulSet Pods use metadata.name (kwok-node-N); the old serves-node
  # label is accepted only as a migration-era diagnostic fallback.
  mapfile -t SERVED < <(
    jq -r --arg serves_label "$SERVES_LABEL" '
      .items[]
      | select(.status.phase == "Running")
      | (
          ((.metadata.labels // {})[$serves_label])
          // (
            if any(.metadata.ownerReferences[]?;
                .kind == "StatefulSet" and
                .name == "kwok-node" and
                (.controller // false) == true)
            then .metadata.name
            else empty
            end
          )
        )
    ' <<<"$agents_json" | sed '/^$/d' | sort -u
  )
  running="${#SERVED[@]}"

  # Agent pods that are NOT Running.
  mapfile -t NOTREADY < <(
    jq -r '.items[] | "\(.metadata.name)=\(.status.phase // "Unknown")"' \
      <<<"$agents_json" | grep -v '=Running$' | sed '/^$/d'
  )
  controller_ready=$(K -n "${AGENT_NS}" get statefulset kwok-node \
    -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)
  controller_ready="${controller_ready:-0}"

  echo "── ${CTX} ───────────────────────────────"
  echo "   KWOK nodes (expected agents) : ${expected}"
  echo "   agents Running (node served) : ${running}"
  echo "   StatefulSet Ready replicas   : ${controller_ready}/${expected}"

  if (( running == expected )) &&
     (( controller_ready == expected )) &&
     (( ${#NOTREADY[@]} == 0 )); then
    echo "   OK: every virtual node has a Running agent."
  else
    overall_gap=1
    declare -A have=()
    for s in "${SERVED[@]}"; do have["$s"]=1; done
    missing=()
    for n in "${NODES[@]}"; do [[ -z "${have[$n]:-}" ]] && missing+=("$n"); done
    if (( ${#missing[@]} > 0 )); then
      echo "   WARN: ${#missing[@]} node(s) with NO Running agent: ${missing[*]}"
      echo "         -> inspect StatefulSet/kwok-node convergence."
    fi
    if (( ${#NOTREADY[@]} > 0 )); then
      echo "   WARN: ${#NOTREADY[@]} agent pod(s) not Running: ${NOTREADY[*]}"
    fi
    unset have
  fi
done

if (( overall_gap == 0 )); then
  echo "attrition-check: all clusters healthy."
else
  echo "attrition-check: gaps detected (see WARN above) — NOT failing run (exit 0)."
fi
exit 0
