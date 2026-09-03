"""Tests for the verified mock-layer workload handoff."""

import copy
import importlib.util
import json
import sys
from pathlib import Path

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "clusterloader2"
    / "clustermesh-scale"
    / "preserved_mock_handoff.py"
)
MODULE_SPEC = importlib.util.spec_from_file_location(
    "preserved_mock_handoff",
    MODULE_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise ImportError(f"Unable to load module from {MODULE_PATH}")
handoff = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = handoff
MODULE_SPEC.loader.exec_module(handoff)


def _baseline_rows(cluster_count=2, mock_count=2):
    rows = {}
    for number in range(1, cluster_count + 1):
        role = f"mesh-{number}"
        rows[role] = {
            "role": role,
            "name": f"clustermesh-{number}",
            "resource_id": (
                "/subscriptions/sub/resourceGroups/run-id/providers/"
                f"Microsoft.ContainerService/managedClusters/clustermesh-{number}"
            ),
            "cluster_name": f"{role}-cilium",
            "cluster_id": str(number),
            "desired_state_sha256": {"nodes.yaml": f"sha-{number}"},
            "node_uids": {
                f"kwok-node-{index}": f"{role}-node-{index}"
                for index in range(mock_count)
            },
            "agent_uids": {
                f"kwok-node-{index}": f"{role}-agent-{index}"
                for index in range(mock_count)
            },
        }
    return rows


def _write_verification(
    root,
    baseline_by_role,
    *,
    baseline_build_id=78812,
    pool_count=5,
):
    cluster_count = len(baseline_by_role)
    mock_count = len(next(iter(baseline_by_role.values()))["node_uids"])
    fault_roles = ["mesh-1"]
    fault_plan = {
        "roles": fault_roles,
        "node_names": ["kwok-node-1"],
        "agent_names": ["kwok-node-0", "kwok-node-1"],
        "total_deleted_nodes": 1,
        "total_deleted_agent_pods": 2,
    }
    summary = {
        "healthy": True,
        "identity_verification_healthy": True,
        "cross_cluster_data_path_valid": True,
        "no_cl2_scenarios_run": True,
        "stage": "complete",
        "run_id": "run-id",
        "baseline_build_id": baseline_build_id,
        "cluster_count": cluster_count,
        "pool_count": pool_count,
        "fleet_connected_count": cluster_count,
        "total_kwok_nodes": cluster_count * mock_count,
        "total_mock_agents": cluster_count * mock_count,
    }
    proof = {
        "run_id": "run-id",
        "baseline_build_id": baseline_build_id,
        "fault_plan": fault_plan,
        "fault_results": [{"role": "mesh-1", "success": True}],
        "reconcile": {
            "success": True,
            "healthy_count": cluster_count,
            "total_clusters": cluster_count,
        },
        "platform_after": {
            "aks_count": cluster_count,
            "pool_count": pool_count,
            "fleet_connected_count": cluster_count,
        },
    }
    live_rows = copy.deepcopy(list(baseline_by_role.values()))
    live_rows[0]["node_uids"]["kwok-node-1"] += "-new"
    live_rows[0]["agent_uids"]["kwok-node-0"] += "-new"
    live_rows[0]["agent_uids"]["kwok-node-1"] += "-new"
    proof["post_recovery"] = handoff.verify.compare_post_recovery(
        baseline_by_role,
        live_rows,
        fault_plan,
    )
    live_post = {"clusters": live_rows}
    root.mkdir(parents=True)
    (root / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
    (root / "verification.json").write_text(json.dumps(proof), encoding="utf-8")
    (root / "live-post.json").write_text(json.dumps(live_post), encoding="utf-8")


def test_verification_artifact_chain_must_be_exact(tmp_path):
    baseline = _baseline_rows()
    verification_dir = tmp_path / "verification"
    _write_verification(verification_dir, baseline)

    result = handoff.validate_verification_artifact(
        str(verification_dir),
        baseline,
        run_id="run-id",
        baseline_build_id=78812,
        expected_cluster_count=2,
        expected_mock_count=2,
        expected_pool_count=5,
    )
    assert result["verified_kwok_nodes"] == 4
    assert result["verified_mock_agents"] == 4

    summary_path = verification_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["baseline_build_id"] = 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    with pytest.raises(handoff.HandoffError, match="baseline_build_id"):
        handoff.validate_verification_artifact(
            str(verification_dir),
            baseline,
            run_id="run-id",
            baseline_build_id=78812,
            expected_cluster_count=2,
            expected_mock_count=2,
            expected_pool_count=5,
        )


def test_verification_artifact_rejects_inconsistent_uid_changes(tmp_path):
    baseline = _baseline_rows()
    verification_dir = tmp_path / "verification"
    _write_verification(verification_dir, baseline)
    proof_path = verification_dir / "verification.json"
    proof = json.loads(proof_path.read_text(encoding="utf-8"))
    proof["post_recovery"]["changed_agent_uids"] = 3
    proof_path.write_text(json.dumps(proof), encoding="utf-8")

    with pytest.raises(handoff.HandoffError, match="changed-UID counts"):
        handoff.validate_verification_artifact(
            str(verification_dir),
            baseline,
            run_id="run-id",
            baseline_build_id=78812,
            expected_cluster_count=2,
            expected_mock_count=2,
            expected_pool_count=5,
        )


def test_verification_artifact_recomputes_live_post_uid_changes(tmp_path):
    baseline = _baseline_rows()
    verification_dir = tmp_path / "verification"
    _write_verification(verification_dir, baseline)
    live_path = verification_dir / "live-post.json"
    live = json.loads(live_path.read_text(encoding="utf-8"))
    live["clusters"][1]["agent_uids"]["kwok-node-0"] = "unexpected-new-uid"
    live_path.write_text(json.dumps(live), encoding="utf-8")

    with pytest.raises(
        handoff.HandoffError,
        match="live-post UID changes do not match",
    ):
        handoff.validate_verification_artifact(
            str(verification_dir),
            baseline,
            run_id="run-id",
            baseline_build_id=78812,
            expected_cluster_count=2,
            expected_mock_count=2,
            expected_pool_count=5,
        )
