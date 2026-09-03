"""Tests for all-agent structured Cilium health validation."""

import importlib.util
import json
import sys
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "clusterloader2"
    / "clustermesh-scale"
    / "cilium_agent_health.py"
)
MODULE_SPEC = importlib.util.spec_from_file_location(
    "cilium_agent_health",
    MODULE_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise ImportError(f"Unable to load module from {MODULE_PATH}")
health = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = health
MODULE_SPEC.loader.exec_module(health)


def remote(name, ready=True):
    return {
        "name": name,
        "ready": ready,
        "connected": True,
        "config": {"required": True, "retrieved": True},
    }


def test_inspect_agents_requires_every_named_agent_ready():
    statuses = [
        health.overlay.CiliumAgentStatus(
            pod_name="cilium-a",
            node_name="node-a",
            remotes=[remote("mesh-22"), remote("mesh-33")],
        ),
        health.overlay.CiliumAgentStatus(
            pod_name="cilium-b",
            node_name="node-b",
            remotes=[remote("mesh-22"), remote("mesh-33")],
        ),
    ]

    summary = health.inspect_agents(statuses, 2, {"mesh-22", "mesh-33"})

    assert summary["healthy"] is True
    assert summary["cilium_agent_count"] == 2
    assert summary["healthy_agent_count"] == 2

    statuses[1].remotes[1]["ready"] = False
    summary = health.inspect_agents(statuses, 2, {"mesh-22", "mesh-33"})
    assert summary["healthy"] is False
    assert summary["healthy_agent_count"] == 1
    assert summary["agents"][1]["not_ready_remote_names"] == ["mesh-33"]


def test_probe_retries_all_agent_status_until_healthy():
    attempts = {"count": 0}

    def runner(args, _timeout):
        if "get" in args and "pods" in args:
            return json.dumps(
                {
                    "items": [
                        {
                            "metadata": {"name": "cilium-a"},
                            "spec": {"nodeName": "node-a"},
                            "status": {
                                "phase": "Running",
                                "containerStatuses": [
                                    {"name": "cilium-agent", "ready": True}
                                ],
                            },
                        }
                    ]
                }
            )
        if "exec" in args:
            attempts["count"] += 1
            return json.dumps(
                {
                    "cluster-mesh": {
                        "clusters": [
                            remote("mesh-22", ready=attempts["count"] > 1)
                        ]
                    }
                }
            )
        raise AssertionError(args)

    summary = health.probe(
        role="mesh-1",
        kubeconfig="/tmp/mesh-1.config",
        expected_remote_count=1,
        expected_remote_names={"mesh-22"},
        attempts=2,
        retry_seconds=0,
        command_timeout_seconds=30,
        runner=runner,
    )

    assert summary["healthy"] is True
    assert summary["attempts"] == 2


def test_inspect_agents_rejects_wrong_or_missing_remote_names():
    wrong = [
        health.overlay.CiliumAgentStatus(
            pod_name="cilium-a",
            node_name="node-a",
            remotes=[remote("stale-peer")],
        )
    ]
    missing = [
        health.overlay.CiliumAgentStatus(
            pod_name="cilium-a",
            node_name="node-a",
            remotes=[
                {
                    "ready": True,
                    "connected": True,
                    "config": {"required": True, "retrieved": True},
                }
            ],
        )
    ]

    assert health.inspect_agents(wrong, 1, {"mesh-22"})["healthy"] is False
    missing_summary = health.inspect_agents(missing, 1, {"mesh-22"})
    assert missing_summary["healthy"] is False
    assert missing_summary["agents"][0]["invalid_remote_name_count"] == 1
