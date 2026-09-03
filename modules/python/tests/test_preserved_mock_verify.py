"""Tests for preserved n=100 cross-run mock-layer verification."""

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
    / "preserved_mock_verify.py"
)
MODULE_SPEC = importlib.util.spec_from_file_location(
    "preserved_mock_verify",
    MODULE_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise ImportError(f"Unable to load module from {MODULE_PATH}")
verify = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = verify
MODULE_SPEC.loader.exec_module(verify)


def _write_state(root, role, run_id, count):
    role_dir = root / role
    support = role_dir / "support"
    support.mkdir(parents=True)
    metadata = {
        "schema_version": 3,
        "run_id": run_id,
        "cluster_name": f"{role}-cilium",
        "cluster_id": role.removeprefix("mesh-"),
        "node_count": count,
        "node_manifest": "nodes.yaml",
        "agent_manifest": "agents.yaml",
        "agent_controller_manifest": "agent-controller.yaml",
    }
    (role_dir / "metadata.json").write_text(
        json.dumps(metadata),
        encoding="utf-8",
    )
    for name in ("nodes.yaml", "agents.yaml", "agent-controller.yaml"):
        (role_dir / name).write_text(f"{role}-{name}\n", encoding="utf-8")
    for name in (
        "kwok-controller.yaml",
        "stage-fast.yaml",
        "kwok-apf.yaml",
        "rbac.yaml",
    ):
        (support / name).write_text(f"{role}-{name}\n", encoding="utf-8")
    return role_dir


def _write_baseline(tmp_path, cluster_count=2, mock_count=2):
    run_id = "run-id"
    baseline_dir = tmp_path / "baseline"
    state_root = baseline_dir / "desired-state"
    state_root.mkdir(parents=True)
    rows = []
    for number in range(1, cluster_count + 1):
        role = f"mesh-{number}"
        role_dir = _write_state(state_root, role, run_id, mock_count)
        rows.append(
            {
                "role": role,
                "name": f"clustermesh-{number}",
                "resource_group": run_id,
                "resource_id": (
                    "/subscriptions/sub/resourceGroups/run-id/providers/"
                    f"Microsoft.ContainerService/managedClusters/clustermesh-{number}"
                ),
                "cluster_name": f"{role}-cilium",
                "cluster_id": str(number),
                "node_uids": {
                    f"kwok-node-{index}": f"{role}-node-{index}"
                    for index in range(mock_count)
                },
                "agent_uids": {
                    f"kwok-node-{index}": f"{role}-agent-{index}"
                    for index in range(mock_count)
                },
                "desired_state_sha256": verify.capture.file_digests(str(role_dir)),
            }
        )
    summary = {
        "healthy": True,
        "run_id": run_id,
        "cluster_count": cluster_count,
        "mock_count_per_cluster": mock_count,
        "total_kwok_nodes": cluster_count * mock_count,
        "total_mock_agents": cluster_count * mock_count,
        "desired_state_copied": True,
    }
    baseline = {
        "run_id": run_id,
        "cluster_count": cluster_count,
        "mock_count_per_cluster": mock_count,
        "total_kwok_nodes": cluster_count * mock_count,
        "total_mock_agents": cluster_count * mock_count,
        "clusters": rows,
    }
    (baseline_dir / "summary.json").write_text(
        json.dumps(summary),
        encoding="utf-8",
    )
    (baseline_dir / "baseline.json").write_text(
        json.dumps(baseline),
        encoding="utf-8",
    )
    return baseline_dir, rows


def test_load_baseline_and_restore_exact_desired_state(tmp_path):
    baseline_dir, _ = _write_baseline(tmp_path)

    _, by_role = verify.load_baseline(str(baseline_dir), "run-id", 2, 2)
    destination = tmp_path / "mock-layer-state" / "run-id"
    destination.mkdir(parents=True)
    (destination / "stale").write_text("stale\n", encoding="utf-8")
    verify.restore_state(
        str(baseline_dir),
        str(destination),
        by_role,
        "run-id",
        2,
    )

    assert sorted(path.name for path in destination.iterdir()) == [
        "mesh-1",
        "mesh-2",
    ]
    verify.validate_state_tree(str(destination), by_role, "run-id", 2)


def test_restore_rejects_symlinked_state_parent(tmp_path):
    baseline_dir, _ = _write_baseline(tmp_path)
    _, by_role = verify.load_baseline(str(baseline_dir), "run-id", 2, 2)
    target = tmp_path / "outside"
    target.mkdir()
    parent = tmp_path / "mock-layer-state"
    parent.symlink_to(target, target_is_directory=True)

    with pytest.raises(verify.VerificationError, match="symlinked path"):
        verify.restore_state(
            str(baseline_dir),
            str(parent / "run-id"),
            by_role,
            "run-id",
            2,
        )
    assert not (target / "run-id").exists()


def test_load_baseline_rejects_desired_state_digest_drift(tmp_path):
    baseline_dir, _ = _write_baseline(tmp_path)
    (baseline_dir / "desired-state" / "mesh-1" / "nodes.yaml").write_text(
        "changed\n",
        encoding="utf-8",
    )

    with pytest.raises(verify.VerificationError, match="SHA-256 map changed"):
        verify.load_baseline(str(baseline_dir), "run-id", 2, 2)


def test_load_baseline_rejects_extra_desired_state_file(tmp_path):
    baseline_dir, _ = _write_baseline(tmp_path)
    extra = baseline_dir / "desired-state" / "mesh-1" / "extra.yaml"
    extra.write_text("extra\n", encoding="utf-8")
    baseline_path = baseline_dir / "baseline.json"
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    baseline["clusters"][0]["desired_state_sha256"] = verify.capture.file_digests(
        str(extra.parent)
    )
    baseline_path.write_text(json.dumps(baseline), encoding="utf-8")

    with pytest.raises(verify.VerificationError, match="files are not exact"):
        verify.load_baseline(str(baseline_dir), "run-id", 2, 2)


def test_pre_boundary_requires_every_uid_to_survive(tmp_path):
    baseline_dir, rows = _write_baseline(tmp_path)
    _, by_role = verify.load_baseline(str(baseline_dir), "run-id", 2, 2)
    live = copy.deepcopy(rows)

    result = verify.compare_pre_boundary(by_role, live)
    assert result["kwok_uids_preserved"] == 4
    assert result["agent_uids_preserved"] == 4

    live[1]["agent_uids"]["kwok-node-1"] = "changed"
    with pytest.raises(verify.VerificationError, match="before fault injection"):
        verify.compare_pre_boundary(by_role, live)


def test_fault_plan_is_bounded_and_post_changes_are_exact(tmp_path):
    baseline_dir, rows = _write_baseline(
        tmp_path,
        cluster_count=5,
        mock_count=20,
    )
    _, by_role = verify.load_baseline(str(baseline_dir), "run-id", 5, 20)
    plan = verify.build_fault_plan(
        ["mesh-1", "mesh-3", "mesh-5"],
        expected_cluster_count=5,
        expected_mock_count=20,
        fault_count=2,
        agent_start=0,
        node_start=10,
    )
    post = copy.deepcopy(rows)
    for row in post:
        if row["role"] not in plan["roles"]:
            continue
        for name in plan["node_names"]:
            row["node_uids"][name] += "-new"
        for name in plan["agent_names"]:
            row["agent_uids"][name] += "-new"

    result = verify.compare_post_recovery(by_role, post, plan)
    assert result["changed_node_uids"] == 6
    assert result["changed_agent_uids"] == 12
    assert result["unchanged_node_uids"] == 94
    assert result["unchanged_agent_uids"] == 88

    post[1]["node_uids"]["kwok-node-19"] += "-unexpected"
    with pytest.raises(verify.VerificationError, match="unexpected changed KWOK"):
        verify.compare_post_recovery(by_role, post, plan)


def test_fault_plan_rejects_broad_or_overlapping_mutation():
    with pytest.raises(verify.VerificationError, match="at most five"):
        verify.build_fault_plan(
            [f"mesh-{index}" for index in range(1, 7)],
            expected_cluster_count=10,
            expected_mock_count=100,
            fault_count=5,
            agent_start=0,
            node_start=10,
        )
    with pytest.raises(verify.VerificationError, match="must not overlap"):
        verify.build_fault_plan(
            ["mesh-1"],
            expected_cluster_count=1,
            expected_mock_count=100,
            fault_count=5,
            agent_start=0,
            node_start=4,
        )


def test_platform_state_requires_exact_aks_pools_and_fleet():
    clusters = [
        verify.capture.Cluster(
            name=f"clustermesh-{number}",
            resource_group="run-id",
            role=f"mesh-{number}",
            kubeconfig=f"/tmp/mesh-{number}.config",
        )
        for number in (1, 2)
    ]
    resource_ids = {
        role: (
            "/subscriptions/sub/resourceGroups/run-id/providers/"
            "Microsoft.ContainerService/managedClusters/"
            f"{cluster.name}"
        )
        for role, cluster in ((cluster.role, cluster) for cluster in clusters)
    }
    aks = [
        {
            "id": resource_ids[cluster.role],
            "name": cluster.name,
            "provisioningState": "Succeeded",
            "powerState": {"code": "Running"},
            "tags": {"role": cluster.role},
            "agentPoolProfiles": [
                {
                    "name": name,
                    "provisioningState": "Succeeded",
                    "powerState": {"code": "Running"},
                }
                for name in (
                    ("default", "prompool", "churnpool")
                    if cluster.role == "mesh-1"
                    else ("default", "prompool")
                )
            ],
        }
        for cluster in clusters
    ]
    members = [
        {
            "name": cluster.role,
            "clusterResourceId": resource_ids[cluster.role],
            "provisioningState": "Succeeded",
            "meshProperties": {"status": {"state": "Connected"}},
        }
        for cluster in clusters
    ]

    def runner(args, _timeout):
        if args[:3] == ["az", "aks", "list"]:
            return json.dumps(aks)
        if args[:4] == ["az", "fleet", "clustermeshprofile", "list-members"]:
            return json.dumps(members)
        raise AssertionError(args)

    result = verify.validate_platform_state(
        clusters,
        subscription_id="sub",
        run_id="run-id",
        expected_pool_count=5,
        fleet_name="fleet",
        profile_name="profile",
        runner=runner,
    )
    assert result["aks_count"] == 2
    assert result["pool_count"] == 5
    assert result["fleet_connected_count"] == 2

    members[1]["meshProperties"]["status"]["state"] = "Failed"
    with pytest.raises(verify.VerificationError, match="Succeeded/Connected"):
        verify.validate_platform_state(
            clusters,
            subscription_id="sub",
            run_id="run-id",
            expected_pool_count=5,
            fleet_name="fleet",
            profile_name="profile",
            runner=runner,
        )


def test_fault_injection_touches_only_planned_objects():
    cluster = verify.capture.Cluster(
        name="clustermesh-1",
        resource_group="run-id",
        role="mesh-1",
        kubeconfig="/tmp/mesh-1.config",
    )
    plan = verify.build_fault_plan(
        ["mesh-1"],
        expected_cluster_count=1,
        expected_mock_count=20,
        fault_count=2,
        agent_start=0,
        node_start=10,
    )
    calls = []

    def runner(args, timeout):
        calls.append((list(args), timeout))
        return ""

    results = verify.inject_faults(
        [cluster],
        plan,
        max_concurrent=1,
        command_timeout_seconds=45,
        runner=runner,
    )

    assert results == [{"role": "mesh-1", "success": True}]
    assert calls[0][0][4:] == [
        "delete",
        "node",
        "kwok-node-10",
        "kwok-node-11",
        "--wait=true",
        "--timeout=180s",
    ]
    assert calls[1][0][4:] == [
        "-n",
        "mock-clustermesh",
        "delete",
        "pod",
        "kwok-node-0",
        "kwok-node-1",
        "kwok-node-10",
        "kwok-node-11",
        "--wait=false",
        "--grace-period=0",
        "--force",
    ]
    assert set(plan["agent_names"]) == {
        "kwok-node-0",
        "kwok-node-1",
        "kwok-node-10",
        "kwok-node-11",
    }


def test_main_records_success_while_awaiting_data_path(tmp_path, monkeypatch):
    baseline_dir, rows = _write_baseline(
        tmp_path,
        cluster_count=1,
        mock_count=20,
    )
    inventory = tmp_path / "clusters.json"
    inventory.write_text(
        json.dumps(
            [{"name": "clustermesh-1", "rg": "run-id", "role": "mesh-1"}]
        ),
        encoding="utf-8",
    )
    artifact_dir = tmp_path / "verification"
    state_root = tmp_path / "mock-layer-state" / "run-id"
    post = copy.deepcopy(rows)
    for name in ("kwok-node-10", "kwok-node-11"):
        post[0]["node_uids"][name] += "-new"
    for name in (
        "kwok-node-0",
        "kwok-node-1",
        "kwok-node-10",
        "kwok-node-11",
    ):
        post[0]["agent_uids"][name] += "-new"
    snapshots = iter((rows, post))
    platform = {
        "aks_count": 1,
        "pool_count": 1,
        "fleet_member_count": 1,
        "fleet_connected_count": 1,
        "resource_ids": {"mesh-1": rows[0]["resource_id"]},
    }

    monkeypatch.setattr(
        verify,
        "validate_platform_state",
        lambda *_args, **_kwargs: copy.deepcopy(platform),
    )
    monkeypatch.setattr(
        verify,
        "capture_live",
        lambda *_args, **_kwargs: copy.deepcopy(next(snapshots)),
    )
    monkeypatch.setattr(
        verify,
        "inject_faults",
        lambda *_args, **_kwargs: [{"role": "mesh-1", "success": True}],
    )
    monkeypatch.setattr(
        verify,
        "run_reconciler",
        lambda *_args, **_kwargs: {
            "success": True,
            "healthy_count": 1,
            "total_clusters": 1,
        },
    )

    result = verify.main(
        [
            "--baseline-dir",
            str(baseline_dir),
            "--baseline-build-id",
            "78812",
            "--clusters",
            str(inventory),
            "--state-root",
            str(state_root),
            "--artifact-dir",
            str(artifact_dir),
            "--reconciler",
            str(tmp_path / "reconciler.py"),
            "--run-id",
            "run-id",
            "--expected-subscription-id",
            "sub",
            "--expected-cluster-count",
            "1",
            "--expected-mock-count",
            "20",
            "--expected-pool-count",
            "1",
            "--fault-role",
            "mesh-1",
            "--fault-count",
            "2",
            "--fault-agent-start",
            "0",
            "--fault-node-start",
            "10",
        ]
    )

    assert result == 0
    summary = json.loads(
        (artifact_dir / "summary.json").read_text(encoding="utf-8")
    )
    assert summary["healthy"] is False
    assert summary["identity_verification_healthy"] is True
    assert summary["cross_cluster_data_path_valid"] is False
    assert summary["stage"] == "awaiting_cross_cluster_data_path"
    assert summary["pre_boundary"]["kwok_uids_preserved"] == 20
    assert summary["post_recovery"]["changed_node_uids"] == 2
    assert summary["post_recovery"]["changed_agent_uids"] == 4
