"""Tests for preserved n=100 mock-layer baseline capture."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "clusterloader2"
    / "clustermesh-scale"
    / "preserved_mock_capture.py"
)
MODULE_SPEC = importlib.util.spec_from_file_location(
    "preserved_mock_capture",
    MODULE_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise ImportError(f"Unable to load module from {MODULE_PATH}")
capture = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = capture
MODULE_SPEC.loader.exec_module(capture)


def test_load_clusters_requires_exact_role_inventory(tmp_path):
    inventory = tmp_path / "clusters.json"
    inventory.write_text(
        json.dumps(
            [
                {"name": "clustermesh-1", "rg": "run", "role": "mesh-1"},
                {"name": "clustermesh-2", "rg": "run", "role": "mesh-2"},
            ]
        ),
        encoding="utf-8",
    )

    clusters = capture.load_clusters(str(inventory), 2)
    assert [cluster.role for cluster in clusters] == ["mesh-1", "mesh-2"]

    payload = json.loads(inventory.read_text(encoding="utf-8"))
    payload[1]["role"] = "mesh-3"
    payload[1]["name"] = "clustermesh-3"
    inventory.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(capture.CaptureError, match="not exactly"):
        capture.load_clusters(str(inventory), 2)


def test_identity_parsers_require_exact_healthy_objects():
    nodes = {
        "items": [
            {"metadata": {"name": f"kwok-node-{index}", "uid": f"n-{index}"}}
            for index in range(2)
        ]
    }
    pods = {
        "items": [
            {
                "metadata": {
                    "name": f"kwok-node-{index}",
                    "uid": f"p-{index}",
                },
                "status": {
                    "phase": "Running",
                    "containerStatuses": [{"ready": True}],
                },
            }
            for index in range(2)
        ]
    }

    assert capture.parse_node_identities(nodes, 2)["kwok-node-1"] == "n-1"
    assert capture.parse_agent_identities(pods, 2)["kwok-node-1"] == "p-1"

    pods["items"][1]["status"]["containerStatuses"][0]["ready"] = False
    with pytest.raises(capture.CaptureError, match="not healthy"):
        capture.parse_agent_identities(pods, 2)


def test_structured_cilium_status_requires_every_remote_ready():
    remotes = [
        {
            "name": "mesh-11",
            "ready": True,
            "connected": True,
            "config": {"required": True, "retrieved": True},
        }
    ]
    payload = {"cluster-mesh": {"clusters": remotes}}

    capture.validate_cilium_status(payload, 1, {"mesh-11"})
    remotes[0]["config"]["retrieved"] = False
    with pytest.raises(capture.CaptureError, match="unhealthy Cilium remotes"):
        capture.validate_cilium_status(payload, 1, {"mesh-11"})

    remotes[0]["config"]["retrieved"] = True
    with pytest.raises(capture.CaptureError, match="names are not exact"):
        capture.validate_cilium_status(payload, 1, {"mesh-12"})


def test_state_metadata_requires_matching_run_and_manifests(tmp_path):
    state = tmp_path / "mesh-1"
    support = state / "support"
    support.mkdir(parents=True)
    metadata = {
        "run_id": "run-id",
        "node_count": 2,
        "node_manifest": "nodes.yaml",
        "agent_manifest": "agents.yaml",
        "agent_controller_manifest": "agent-controller.yaml",
    }
    (state / "metadata.json").write_text(
        json.dumps(metadata), encoding="utf-8"
    )
    for name in ("nodes.yaml", "agents.yaml", "agent-controller.yaml"):
        (state / name).write_text("test\n", encoding="utf-8")
    for name in (
        "kwok-controller.yaml",
        "stage-fast.yaml",
        "kwok-apf.yaml",
        "rbac.yaml",
    ):
        (support / name).write_text("test\n", encoding="utf-8")

    assert capture.load_state_metadata(str(state), "run-id", 2) == metadata
    with pytest.raises(capture.CaptureError, match="run_id mismatch"):
        capture.load_state_metadata(str(state), "other-run", 2)


def test_probe_cluster_validates_every_cilium_agent(tmp_path, monkeypatch):
    state = tmp_path / "mesh-1"
    support = state / "support"
    support.mkdir(parents=True)
    metadata = {
        "run_id": "run-id",
        "node_count": 2,
        "cluster_name": "mesh-11",
        "cluster_id": "1",
        "node_manifest": "nodes.yaml",
        "agent_manifest": "agents.yaml",
        "agent_controller_manifest": "agent-controller.yaml",
    }
    (state / "metadata.json").write_text(
        json.dumps(metadata),
        encoding="utf-8",
    )
    for name in ("nodes.yaml", "agents.yaml", "agent-controller.yaml"):
        (state / name).write_text("test\n", encoding="utf-8")
    for name in (
        "kwok-controller.yaml",
        "stage-fast.yaml",
        "kwok-apf.yaml",
        "rbac.yaml",
    ):
        (support / name).write_text("test\n", encoding="utf-8")

    node_payload = {
        "items": [
            {"metadata": {"name": f"kwok-node-{index}", "uid": f"n-{index}"}}
            for index in range(2)
        ]
    }
    pod_payload = {
        "items": [
            {
                "metadata": {
                    "name": f"kwok-node-{index}",
                    "uid": f"p-{index}",
                },
                "status": {
                    "phase": "Running",
                    "containerStatuses": [{"ready": True}],
                },
            }
            for index in range(2)
        ]
    }
    statefulset = {"status": {"readyReplicas": 2}}

    def runner(args, _timeout):
        if "get" in args and "nodes" in args:
            return json.dumps(node_payload)
        if "get" in args and "pods" in args:
            return json.dumps(pod_payload)
        if "statefulset" in args:
            return json.dumps(statefulset)
        raise AssertionError(args)

    remotes = [
        {
            "name": "mesh-22",
            "ready": True,
            "connected": True,
            "config": {"required": True, "retrieved": True},
        }
    ]
    monkeypatch.setattr(
        capture.live_overlay,
        "read_status",
        lambda *_args, **_kwargs: [
            capture.live_overlay.CiliumAgentStatus(
                pod_name="cilium-a",
                node_name="node-a",
                remotes=remotes,
            ),
            capture.live_overlay.CiliumAgentStatus(
                pod_name="cilium-b",
                node_name="node-b",
                remotes=remotes,
            ),
        ],
    )

    result = capture.probe_cluster(
        capture.Cluster(
            name="clustermesh-1",
            resource_group="run-id",
            role="mesh-1",
            kubeconfig="/tmp/mesh-1.config",
        ),
        state_root=str(tmp_path),
        run_id="run-id",
        expected_cluster_count=2,
        expected_mock_count=2,
        command_timeout_seconds=30,
        runner=runner,
        expected_remote_names={"mesh-22"},
    )

    assert result["cilium_agents"] == [
        {"pod_name": "cilium-a", "node_name": "node-a"},
        {"pod_name": "cilium-b", "node_name": "node-b"},
    ]
