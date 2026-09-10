"""Focused tests for the inter-scenario ClusterMesh health gate."""

import json
import os
import stat
import subprocess
import textwrap
import time
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "clusterloader2"
    / "clustermesh-scale"
    / "config"
    / "scenario-health-gate.sh"
)
CILIUM_AGENT_HEALTH_PROBE = (
    SCRIPT_PATH.parents[1] / "cilium_agent_health.py"
)


def _write_fake_tools(tmp_path: Path) -> tuple[Path, Path, Path]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    clock_file = tmp_path / "clock"
    clock_file.write_text("0\n", encoding="utf-8")
    poll_file = tmp_path / "poll"
    poll_file.write_text("0\n", encoding="utf-8")

    fake_date = fake_bin / "date"
    fake_date.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -euo pipefail
            now=$(cat "$FAKE_CLOCK")
            if [[ " $* " == *" +%s "* ]]; then
              printf '%s\\n' "$now"
            else
              printf '2026-07-20T08:00:%02dZ\\n' "$now"
            fi
            """
        ),
        encoding="utf-8",
    )

    fake_sleep = fake_bin / "sleep"
    fake_sleep.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -euo pipefail
            now=$(cat "$FAKE_CLOCK")
            printf '%s\\n' "$((now + ${1:?seconds required}))" > "$FAKE_CLOCK"
            """
        ),
        encoding="utf-8",
    )

    fake_kubectl = fake_bin / "kubectl"
    fake_kubectl.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -euo pipefail
            args="$*"
            poll=$(cat "$FAKE_POLL")
            kubeconfig="${KUBECONFIG:-}"
            role="${kubeconfig##*/}"
            role="${role%.config}"
            if [[ "$args" == *"/fake/mesh-1.config"* ]]; then
              role="mesh-1"
            elif [[ "$args" == *"/fake/mesh-2.config"* ]]; then
              role="mesh-2"
            fi

            if [[ " $args " == *" get namespaces -o name "* ]]; then
              poll=$((poll + 1))
              printf '%s\\n' "$poll" > "$FAKE_POLL"
              if [ "$FAKE_MODE" = "transient-cleanup" ] && [ "$poll" -eq 1 ]; then
                echo "Unable to connect to the server: connection reset" >&2
                exit 1
              fi
              if [ "$FAKE_MODE" = "targeted-recheck" ] &&
                 [ "$role" = "mesh-2" ] &&
                 [ "$poll" -le "$FAKE_CLUSTER_COUNT" ]; then
                printf '%s\\n' 'namespace/clustermesh-old'
              fi
              exit 0
            elif [[ " $args " == *" get containernetworklogs.acn.azure.com -o name "* ]]; then
              echo 'No resources found' >&2
              if { [ "$FAKE_MODE" = "transient-cleanup" ] && [ "$poll" -eq 1 ]; } ||
                 [ "$FAKE_MODE" = "timeout" ]; then
                printf '%s\\n' 'containernetworklog.acn.azure.com/old-log'
              fi
            elif [[ " $args " == *" get containernetworkmetrics.acn.azure.com -o name "* ]]; then
              echo 'No resources found' >&2
            elif [[ " $args " == *" get ciliumendpoints.cilium.io -A "* ]]; then
              echo 'No resources found in all namespaces' >&2
              if [ "$FAKE_MODE" = "transient-cleanup" ] && [ "$poll" -eq 1 ]; then
                printf '%s\\n' 'clustermesh-old'
              else
                printf '%s\\n' 'kube-system' 'mock-clustermesh'
              fi
            elif [[ " $args " == *" api-resources --api-group=monitoring.coreos.com "* ]]; then
              printf '%s\\n' \
                'podmonitors.monitoring.coreos.com' \
                'prometheuses.monitoring.coreos.com'
            elif [[ " $args " == *" -n monitoring get all,configmaps,secrets,serviceaccounts,persistentvolumeclaims,roles.rbac.authorization.k8s.io,rolebindings.rbac.authorization.k8s.io"* ]]; then
              if [ "$FAKE_MODE" = "transient-cleanup" ] && [ "$poll" -eq 1 ]; then
                cat <<'JSON'
            {"items":[
              {"kind":"Deployment","metadata":{"name":"prometheus-operator"}},
              {"kind":"Deployment","metadata":{"name":"kube-state-metrics"}},
              {"kind":"ConfigMap","metadata":{"name":"ama-metrics-settings"}},
              {"kind":"PodMonitor","metadata":{"name":"ama-metrics"}},
              {"kind":"PodMonitor","metadata":{"name":"controlplane-apiserver"}},
              {"kind":"Prometheus","metadata":{"name":"managed-prometheus"}},
              {"kind":"PodMonitor","metadata":{"name":"hubble-metrics-old"}},
              {"kind":"Deployment","metadata":{"name":"apiserver-backend-exporter-old"}}
            ]}
            JSON
              else
                cat <<'JSON'
            {"items":[
              {"kind":"Deployment","metadata":{"name":"prometheus-operator"}},
              {"kind":"Deployment","metadata":{"name":"kube-state-metrics"}},
              {"kind":"ConfigMap","metadata":{"name":"ama-metrics-settings"}},
              {"kind":"PodMonitor","metadata":{"name":"ama-metrics"}},
              {"kind":"PodMonitor","metadata":{"name":"controlplane-apiserver"}},
              {"kind":"Prometheus","metadata":{"name":"managed-prometheus"}}
            ]}
            JSON
              fi
            elif [[ " $args " == *" get clusterroles.rbac.authorization.k8s.io,clusterrolebindings.rbac.authorization.k8s.io -o json "* ]]; then
              if [ "$FAKE_MODE" = "transient-cleanup" ] && [ "$poll" -eq 1 ]; then
                printf '%s\\n' \
                  '{"items":[{"kind":"ClusterRole","metadata":{"name":"prometheus-operator"}},{"kind":"ClusterRole","metadata":{"name":"apiserver-backend-exporter-old"}}]}'
              else
                printf '%s\\n' \
                  '{"items":[{"kind":"ClusterRole","metadata":{"name":"prometheus-operator"}}]}'
              fi
            elif [[ " $args " == *" -n kube-system get daemonset cilium -o json "* ]]; then
              printf '%s\\n' \
                '{"status":{"desiredNumberScheduled":1,"numberReady":1}}'
            elif [[ " $args " == *" -n kube-system get pods -l k8s-app=cilium -o json "* ]]; then
              printf '%s\\n' \
                '{"items":[{"metadata":{"name":"cilium-a"},"spec":{"nodeName":"node-a"},"status":{"phase":"Running","containerStatuses":[{"name":"cilium-agent","ready":true}]}}]}'
            elif [[ " $args " == *" -n mock-clustermesh get statefulset kwok-node -o json "* ]]; then
              printf '%s\\n' \
                '{"spec":{"replicas":2},"status":{"currentReplicas":2,"readyReplicas":2}}'
            elif [[ " $args " == *" get nodes -l type=kwok -o json "* ]]; then
              if [ -n "${FAKE_MOCK_NODES_JSON:-}" ] && [ -f "$FAKE_MOCK_NODES_JSON" ]; then
                cat "$FAKE_MOCK_NODES_JSON"
              else
                cat <<'JSON'
            {"items":[
              {"metadata":{"name":"kwok-node-1"},"spec":{"unschedulable":false},"status":{"conditions":[{"type":"Ready","status":"True"}]}},
              {"metadata":{"name":"kwok-node-2"},"spec":{"unschedulable":false},"status":{"conditions":[{"type":"Ready","status":"True"}]}}
            ]}
            JSON
              fi
            elif [[ " $args " == *" -n mock-clustermesh get pods -l app=mock-cilium-agent -o json "* ]]; then
              if [ -n "${FAKE_MOCK_AGENTS_JSON:-}" ] && [ -f "$FAKE_MOCK_AGENTS_JSON" ]; then
                cat "$FAKE_MOCK_AGENTS_JSON"
              else
                printf '%s\\n' \
                  '{"items":[{"metadata":{"name":"kwok-node-1","ownerReferences":[{"kind":"StatefulSet","name":"kwok-node","controller":true}]},"status":{"phase":"Running","containerStatuses":[{"ready":true}]}},{"metadata":{"name":"kwok-node-2","ownerReferences":[{"kind":"StatefulSet","name":"kwok-node","controller":true}]},"status":{"phase":"Running","containerStatuses":[{"ready":true}]}}]}'
              fi
            elif [[ " $args " == *" -n kube-system exec cilium-a -c cilium-agent -- cilium-dbg status -o json "* ]]; then
              if [ "$FAKE_CLUSTER_COUNT" -eq 2 ] && [ "$role" = "mesh-1" ]; then
                printf '%s\\n' '{"cluster-mesh":{"clusters":[{"name":"mesh-22","ready":true,"connected":true,"config":{"required":true,"retrieved":true}}]}}'
              elif [ "$FAKE_CLUSTER_COUNT" -eq 2 ] && [ "$role" = "mesh-2" ]; then
                printf '%s\\n' '{"cluster-mesh":{"clusters":[{"name":"mesh-11","ready":true,"connected":true,"config":{"required":true,"retrieved":true}}]}}'
              else
                printf '%s\\n' '{"cluster-mesh":{"clusters":[]}}'
              fi
            elif [[ " $args " == *" get ciliumidentities.cilium.io -o name "* ]]; then
              echo 'No resources found' >&2
              count=5
              if [ "$FAKE_MODE" = "instability" ] && [ "$poll" -ge 2 ]; then
                count=6
              fi
              for ((i=1; i<=count; i++)); do
                printf 'ciliumidentity.cilium.io/%s\\n' "$i"
              done
            elif [[ " $args " == *" get services -A -o json "* ]]; then
              if [ "$FAKE_MODE" = "deadline-at-cycle-end" ]; then
                printf '%s\\n' "$FAKE_DEADLINE_CLOCK" > "$FAKE_CLOCK"
              fi
              printf '%s\\n' \
                '{"items":[{"metadata":{"annotations":{"service.cilium.io/global":"true"}}}]}'
            else
              echo "Unexpected kubectl command: $args" >&2
              exit 1
            fi
            """
        ),
        encoding="utf-8",
    )

    for tool in (fake_date, fake_sleep, fake_kubectl):
        tool.chmod(tool.stat().st_mode | stat.S_IXUSR)
    return fake_bin, clock_file, poll_file


def _run_gate(
    tmp_path: Path,
    mode: str,
    *,
    quiet_window: int,
    timeout: int,
    cycle_timeout: int = 180,
    cluster_timeout: int = 1,
    cluster_count: int = 1,
    concurrency: int = 8,
    completion_margin: int = 1,
    max_cycles: int = 0,
    mock_nodes_json: str | None = None,
    mock_agents_json: str | None = None,
) -> tuple[subprocess.CompletedProcess[str], dict, int]:
    fake_bin, clock_file, poll_file = _write_fake_tools(tmp_path)
    inventory = tmp_path / "clusters.json"
    inventory.write_text(
        json.dumps(
            [
                {
                    "role": f"mesh-{index}",
                    "name": f"cluster-{index}",
                    "context": f"cluster-{index}",
                    "kubeconfig": f"/fake/mesh-{index}.config",
                }
                for index in range(1, cluster_count + 1)
            ]
        ),
        encoding="utf-8",
    )
    summary_file = tmp_path / "health-summary.json"
    cilium_identity_inventory = tmp_path / "cilium-identities.json"
    cilium_identity_inventory.write_text(
        json.dumps(
            [
                {
                    "role": f"mesh-{index}",
                    "cluster_name": f"mesh-{index}{index}",
                    "cluster_id": index,
                }
                for index in range(1, cluster_count + 1)
            ]
        ),
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment.update(
        {
            "FAKE_CLOCK": str(clock_file),
            "FAKE_POLL": str(poll_file),
            "FAKE_MODE": mode,
            "FAKE_CLUSTER_COUNT": str(cluster_count),
            "FAKE_DEADLINE_CLOCK": str(timeout),
            "PATH": f"{fake_bin}:{environment['PATH']}",
            "CILIUM_AGENT_HEALTH_PROBE": str(CILIUM_AGENT_HEALTH_PROBE),
            "CILIUM_IDENTITY_INVENTORY": str(cilium_identity_inventory),
        }
    )
    if mock_nodes_json is not None:
        mock_nodes_path = tmp_path / "mock-nodes.json"
        mock_nodes_path.write_text(mock_nodes_json, encoding="utf-8")
        environment["FAKE_MOCK_NODES_JSON"] = str(mock_nodes_path)
    if mock_agents_json is not None:
        mock_agents_path = tmp_path / "mock-agents.json"
        mock_agents_path.write_text(mock_agents_json, encoding="utf-8")
        environment["FAKE_MOCK_AGENTS_JSON"] = str(mock_agents_path)
    command = [
        "bash",
        str(SCRIPT_PATH),
        "--clusters",
        str(inventory),
        "--scenario",
        "pod-churn-combined",
        "--expected-mock-count",
        "2",
        "--expected-remote-count",
        str(cluster_count - 1),
        "--timeout-seconds",
        str(timeout),
        "--cycle-timeout-seconds",
        str(cycle_timeout),
        "--cluster-timeout-seconds",
        str(cluster_timeout),
        "--quiet-window-seconds",
        str(quiet_window),
        "--poll-interval-seconds",
        "1",
        "--concurrency",
        str(concurrency),
        "--completion-margin-seconds",
        str(completion_margin),
        "--summary-file",
        str(summary_file),
    ]
    if max_cycles > 0:
        command.extend(["--max-cycles", str(max_cycles)])
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        env=environment,
        timeout=10,
    )
    summary = (
        json.loads(summary_file.read_text(encoding="utf-8"))
        if summary_file.exists()
        else {}
    )
    poll_count = int(poll_file.read_text(encoding="utf-8"))
    return result, summary, poll_count


def test_health_gate_succeeds_after_transient_cleanup(tmp_path):
    result, summary, poll_count = _run_gate(
        tmp_path,
        "transient-cleanup",
        quiet_window=2,
        timeout=10,
    )

    assert result.returncode == 0
    assert poll_count == 3
    assert summary["success"] is True
    assert summary["infrastructure_healthy"] is True
    assert summary["scenario"] == "pod-churn-combined"
    assert summary["quiet_window_basis"] == "continuous-health"
    assert summary["cluster_timeout_seconds"] == 1
    assert summary["observation_wave_count"] == 1
    assert summary["minimum_fair_cycle_seconds"] == 1
    assert summary["completed_cycle_count"] >= 1
    assert summary["stable_seconds"] == 2
    assert summary["clusters"][0]["healthy"] is True
    assert summary["clusters"][0]["cleanup"]["scenario_namespace_count"] == 0
    assert summary["clusters"][0]["cleanup"]["container_network_log_count"] == 0
    assert summary["clusters"][0]["cleanup"]["container_network_metric_count"] == 0
    assert summary["clusters"][0]["cleanup"]["cilium_endpoint_total"] == 2
    assert (
        summary["clusters"][0]["cleanup"]["scenario_monitoring_resource_count"]
        == 0
    )
    assert summary["clusters"][0]["mock"]["ready_nodes"] == 2
    assert summary["clusters"][0]["mock"]["schedulable_nodes"] == 2
    assert summary["clusters"][0]["mock"]["controller"]["ready"] == 2
    assert summary["clusters"][0]["mock"]["ready_agents"] == 2
    assert summary["clusters"][0]["mock"]["controller_owned_agents"] == 2
    assert summary["clusters"][0]["clustermesh"]["cilium_agent_count"] == 1
    assert summary["clusters"][0]["clustermesh"]["healthy_agent_count"] == 1
    coverage = summary["clusters"][0]["mock"]["serves_node_coverage"]
    assert coverage["served_count"] == 2
    assert coverage["unique_count"] == 2
    assert coverage["duplicate_count"] == 0
    assert coverage["missing_nodes"] == []
    assert coverage["orphan_agents"] == []
    assert coverage["exact_match"] is True
    assert summary["clusters"][0]["fingerprint"]["cilium_identities"] == 5
    assert "get namespaces failed: Unable to connect" in result.stderr
    assert "PodMonitor/hubble-metrics-old" in result.stderr
    assert "ClusterRole/apiserver-backend-exporter-old" in result.stderr


def test_health_gate_keeps_quiet_window_when_diagnostic_counts_change(
    tmp_path,
):
    result, summary, poll_count = _run_gate(
        tmp_path,
        "instability",
        quiet_window=2,
        timeout=10,
    )

    assert result.returncode == 0
    assert poll_count == 2
    assert summary["success"] is True
    assert summary["quiet_window_basis"] == "continuous-health"
    assert summary["clusters"][0]["fingerprint"]["cilium_identities"] == 6
    assert "Health fingerprint changed" not in result.stderr
    assert "continuously healthy" in result.stdout


def test_health_gate_times_out_with_actionable_summary(tmp_path):
    result, summary, poll_count = _run_gate(
        tmp_path,
        "timeout",
        quiet_window=2,
        timeout=5,
    )

    assert result.returncode == 1
    assert poll_count == 1
    assert summary["success"] is False
    cluster = summary["clusters"][0]
    assert cluster["healthy"] is False
    assert cluster["cleanup"]["container_network_log_count"] == 1
    assert any(
        "ContainerNetworkLog resource(s) remain" in failure
        for failure in cluster["failures"]
    )
    assert summary["termination_reason"] == "insufficient-time-for-fair-cycle"
    assert (
        "below the" in result.stderr
        or "before cycle 1 could be certified" in result.stderr
    )
    assert "final full certification" in result.stderr


def test_health_gate_rejects_unfair_cycle_budget(tmp_path):
    result, summary, _ = _run_gate(
        tmp_path,
        "transient-cleanup",
        quiet_window=1,
        timeout=10,
        cycle_timeout=1,
        cluster_timeout=2,
    )

    assert result.returncode == 2
    assert not summary
    assert "too small" in result.stderr
    assert "require at least 2s" in result.stderr


def test_health_gate_removes_prior_cilium_agent_summary_before_probe():
    script = SCRIPT_PATH.read_text(encoding="utf-8")

    summary_assignment = (
        'cilium_agent_summary="$state_dir/cilium-agents-${role}.json"'
    )
    cleanup = 'rm -f "$cilium_agent_summary" "$cilium_agent_log"'
    probe = 'python3 "$cilium_agent_health_probe"'

    assert script.index(summary_assignment) < script.index(cleanup)
    assert script.index(cleanup) < script.index(probe)


def test_health_gate_checkpoints_large_observations_outside_argv():
    script = SCRIPT_PATH.read_text(encoding="utf-8")

    assert 'last_observations_file="$state_dir/last-observations.json"' in script
    assert '--slurpfile observation_documents "$last_observations_file"' in script
    assert '--argjson clusters "$last_observations"' not in script
    assert (
        'collect_observations "$cycle_clusters_file" > "$observations_partial"'
        in script
    )
    assert 'length == $expected' in script
    assert "if ! write_summary true; then" in script


def test_health_gate_reaps_any_completed_worker_and_retries_cilium():
    script = SCRIPT_PATH.read_text(encoding="utf-8")

    assert 'wait -n -p completed_pid "${pids[@]}"' in script
    assert 'if [ "$pid" != "$completed_pid" ]; then' in script
    assert "HEALTH_GATE_CILIUM_PROBE_ATTEMPTS:-2" in script
    assert "HEALTH_GATE_CILIUM_PROBE_RETRY_SECONDS:-2" in script
    assert '--attempts "$cilium_probe_attempts"' in script
    assert '--retry-seconds "$cilium_probe_retry_seconds"' in script


def test_health_gate_assigns_each_role_a_fair_deadline():
    script = SCRIPT_PATH.read_text(encoding="utf-8")

    assert (
        "minimum_fair_cycle_seconds=$(( observation_wave_count * "
        "cluster_timeout_seconds ))"
        in script
    )
    assert (
        "active_observation_deadline=$((observation_started_epoch + "
        "cluster_timeout_seconds))"
        in script
    )
    assert "remaining=$((active_observation_deadline - now))" in script
    assert 'if [ "$remaining" -lt "$required_remaining" ]; then' in script
    assert "Completed health observation cycle" in script


def test_health_gate_targets_only_unhealthy_roles_before_full_certification(
    tmp_path,
):
    result, summary, poll_count = _run_gate(
        tmp_path,
        "targeted-recheck",
        quiet_window=1,
        timeout=20,
        cluster_count=2,
        concurrency=1,
    )

    assert result.returncode == 0
    assert poll_count == 5
    assert summary["success"] is True
    assert summary["completed_cycle_count"] == 3
    assert summary["last_completed_cycle_scope"] == "full"
    assert summary["last_completed_cycle_cluster_count"] == 2
    assert summary["termination_reason"] == "healthy"
    assert all(cluster["healthy"] for cluster in summary["clusters"])
    assert "scope=targeted clusters=1" in result.stdout
    assert "full 2-cluster certification cycle" in result.stdout


def test_health_gate_cycle_limit_writes_complete_observation(tmp_path):
    result, summary, poll_count = _run_gate(
        tmp_path,
        "timeout",
        quiet_window=2,
        timeout=10,
        max_cycles=1,
    )

    assert result.returncode == 3
    assert poll_count == 1
    assert summary["success"] is False
    assert summary["termination_reason"] == "cycle-limit"
    assert summary["completed_cycle_count"] == 1
    assert summary["last_completed_cycle_scope"] == "full"
    assert summary["last_completed_cycle_cluster_count"] == 1
    assert summary["next_cycle_scope"] == "targeted"
    assert summary["next_cycle_cluster_count"] == 1


def test_health_gate_cannot_certify_after_deadline(tmp_path):
    result, summary, poll_count = _run_gate(
        tmp_path,
        "deadline-at-cycle-end",
        quiet_window=1,
        timeout=4,
    )

    assert result.returncode == 1
    assert poll_count == 1
    assert summary["success"] is False
    assert summary["termination_reason"] == "timeout"
    assert summary["completed_cycle_count"] == 1
    assert "before cycle 1 could be certified" in result.stderr


def _healthy_nodes_json() -> str:
    return json.dumps(
        {
            "items": [
                {
                    "metadata": {"name": "kwok-node-1"},
                    "spec": {"unschedulable": False},
                    "status": {"conditions": [{"type": "Ready", "status": "True"}]},
                },
                {
                    "metadata": {"name": "kwok-node-2"},
                    "spec": {"unschedulable": False},
                    "status": {"conditions": [{"type": "Ready", "status": "True"}]},
                },
            ]
        }
    )


def _healthy_agents_json() -> str:
    return json.dumps(
        {
            "items": [
                {
                    "metadata": {
                        "name": "kwok-node-1",
                        "labels": {
                            "mock-clustermesh/serves-node": "kwok-node-1"
                        },
                        "ownerReferences": [
                            {
                                "kind": "StatefulSet",
                                "name": "kwok-node",
                                "controller": True,
                            }
                        ],
                    },
                    "status": {
                        "phase": "Running",
                        "containerStatuses": [{"ready": True}],
                    },
                },
                {
                    "metadata": {
                        "name": "kwok-node-2",
                        "labels": {
                            "mock-clustermesh/serves-node": "kwok-node-2"
                        },
                        "ownerReferences": [
                            {
                                "kind": "StatefulSet",
                                "name": "kwok-node",
                                "controller": True,
                            }
                        ],
                    },
                    "status": {
                        "phase": "Running",
                        "containerStatuses": [{"ready": True}],
                    },
                },
            ]
        }
    )


def test_health_gate_detects_unschedulable_kwok_node(tmp_path):
    nodes = json.loads(_healthy_nodes_json())
    nodes["items"][1]["spec"]["unschedulable"] = True
    result, summary, _ = _run_gate(
        tmp_path,
        "transient-cleanup",
        quiet_window=1,
        timeout=5,
        mock_nodes_json=json.dumps(nodes),
        mock_agents_json=_healthy_agents_json(),
    )

    assert result.returncode == 1
    assert summary["success"] is False
    assert summary["infrastructure_healthy"] is False
    cluster = summary["clusters"][0]
    assert cluster["mock"]["nodes"] == 2
    assert cluster["mock"]["ready_nodes"] == 2
    assert cluster["mock"]["schedulable_nodes"] == 1
    assert any(
        "KWOK nodes expected/present/Ready/schedulable=2/2/2/1" in failure
        for failure in cluster["failures"]
    )


def test_health_gate_detects_unready_mock_agent_container(tmp_path):
    agents = json.loads(_healthy_agents_json())
    agents["items"][1]["status"]["containerStatuses"] = [{"ready": False}]
    result, summary, _ = _run_gate(
        tmp_path,
        "transient-cleanup",
        quiet_window=1,
        timeout=5,
        mock_nodes_json=_healthy_nodes_json(),
        mock_agents_json=json.dumps(agents),
    )

    assert result.returncode == 1
    assert summary["success"] is False
    cluster = summary["clusters"][0]
    assert cluster["mock"]["agents"] == 2
    assert cluster["mock"]["running_agents"] == 2
    assert cluster["mock"]["ready_agents"] == 1
    assert any(
        "mock Cilium agents expected/present/Running/Ready/controller-owned="
        "2/2/2/1/2" in failure
        for failure in cluster["failures"]
    )


def test_health_gate_detects_duplicate_and_missing_serves_node_coverage(tmp_path):
    agents = json.loads(_healthy_agents_json())
    # Both agents claim to serve the same node; kwok-node-2 ends up with no
    # serving agent at all.
    agents["items"][1]["metadata"]["labels"][
        "mock-clustermesh/serves-node"
    ] = "kwok-node-1"
    result, summary, _ = _run_gate(
        tmp_path,
        "transient-cleanup",
        quiet_window=1,
        timeout=5,
        mock_nodes_json=_healthy_nodes_json(),
        mock_agents_json=json.dumps(agents),
    )

    assert result.returncode == 1
    assert summary["success"] is False
    coverage = summary["clusters"][0]["mock"]["serves_node_coverage"]
    assert coverage["served_count"] == 2
    assert coverage["unique_count"] == 1
    assert coverage["duplicate_count"] == 1
    assert coverage["missing_nodes"] == ["kwok-node-2"]
    assert coverage["orphan_agents"] == []
    assert coverage["exact_match"] is False
    assert any(
        "mock-agent logical-node coverage mismatch" in failure
        for failure in summary["clusters"][0]["failures"]
    )


def test_health_gate_detects_orphan_serves_node_agent(tmp_path):
    agents = json.loads(_healthy_agents_json())
    # Second agent claims a node name that no longer exists.
    agents["items"][1]["metadata"]["labels"][
        "mock-clustermesh/serves-node"
    ] = "kwok-node-stale"
    result, summary, _ = _run_gate(
        tmp_path,
        "transient-cleanup",
        quiet_window=1,
        timeout=5,
        mock_nodes_json=_healthy_nodes_json(),
        mock_agents_json=json.dumps(agents),
    )

    assert result.returncode == 1
    assert summary["success"] is False
    coverage = summary["clusters"][0]["mock"]["serves_node_coverage"]
    assert coverage["unique_count"] == 2
    assert coverage["duplicate_count"] == 0
    assert coverage["missing_nodes"] == ["kwok-node-2"]
    assert coverage["orphan_agents"] == ["kwok-node-stale"]
    assert coverage["exact_match"] is False


def test_health_gate_hard_bounds_a_hung_kubectl(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_kubectl = fake_bin / "kubectl"
    fake_kubectl.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -euo pipefail
            printf '%s\\n' "$*" >> "$HUNG_KUBECTL_LOG"
            sleep 30
            """
        ),
        encoding="utf-8",
    )
    fake_kubectl.chmod(fake_kubectl.stat().st_mode | stat.S_IXUSR)
    inventory = tmp_path / "clusters.json"
    inventory.write_text(
        json.dumps(
            [
                {
                    "role": "mesh-1",
                    "name": "cluster-a",
                    "kubeconfig": "/fake/mesh-1.config",
                }
            ]
        ),
        encoding="utf-8",
    )
    summary_file = tmp_path / "health-summary.json"
    cilium_identity_inventory = tmp_path / "cilium-identities.json"
    cilium_identity_inventory.write_text(
        json.dumps(
            [{"role": "mesh-1", "cluster_name": "mesh-11", "cluster_id": 1}]
        ),
        encoding="utf-8",
    )
    kubectl_log = tmp_path / "kubectl.log"
    environment = os.environ.copy()
    environment["PATH"] = f"{fake_bin}:{environment['PATH']}"
    environment["HUNG_KUBECTL_LOG"] = str(kubectl_log)
    environment["CILIUM_AGENT_HEALTH_PROBE"] = str(
        CILIUM_AGENT_HEALTH_PROBE
    )
    environment["CILIUM_IDENTITY_INVENTORY"] = str(
        cilium_identity_inventory
    )

    started = time.monotonic()
    result = subprocess.run(
        [
            "bash",
            str(SCRIPT_PATH),
            "--clusters",
            str(inventory),
            "--scenario",
            "hung-kubectl",
            "--expected-mock-count",
            "0",
            "--expected-remote-count",
            "0",
            "--timeout-seconds",
            "4",
            "--cycle-timeout-seconds",
            "1",
            "--cluster-timeout-seconds",
            "1",
            "--quiet-window-seconds",
            "1",
            "--poll-interval-seconds",
            "1",
            "--completion-margin-seconds",
            "1",
            "--summary-file",
            str(summary_file),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
        timeout=8,
    )
    elapsed = time.monotonic() - started

    summary = json.loads(summary_file.read_text(encoding="utf-8"))
    kubectl_calls = (
        kubectl_log.read_text(encoding="utf-8").splitlines()
        if kubectl_log.exists()
        else []
    )
    assert result.returncode == 1
    assert 0.8 <= elapsed < 6
    assert len(kubectl_calls) <= 1
    if kubectl_calls:
        assert "--request-timeout=1s" in kubectl_calls[0]
    assert summary["success"] is False
    assert summary["cycle_timeout_seconds"] == 1
    assert any(
        "kubectl timed out after" in failure
        or "observation cycle deadline exhausted" in failure
        or "cluster observation deadline exhausted" in failure
        for failure in summary["clusters"][0]["failures"]
    )
    assert (
        "below the" in result.stderr
        or "before cycle 1 could be certified" in result.stderr
    )
