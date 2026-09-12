"""Explicit modern mesh-96 layout proofs never replace historical verification."""

import copy
import importlib
import json
import sys
from pathlib import Path

import pytest


MODULE_DIR = Path(__file__).resolve().parents[1] / "clusterloader2" / "clustermesh-scale"
sys.path.insert(0, str(MODULE_DIR))
try:
    baseline = importlib.import_module("modern_pool_baseline")
    verify = importlib.import_module("preserved_mock_verify")
    handoff = importlib.import_module("preserved_mock_handoff")
finally:
    sys.path.pop(0)


def receipt():
    prefix = (
        f"/subscriptions/{baseline.SUBSCRIPTION}/resourceGroups/{baseline.RUN_ID}/providers/"
        "Microsoft.ContainerService/managedClusters/clustermesh-96/agentPools/"
    )
    return {
        "success": True, "repaired": True, "workloads_ready": False, "plan_sha256": baseline.PLAN_SHA,
        "modern_cni": {"completed": True, "source_retired": True, "pool_name": "cniv5",
                       "default_pool_count": 1, "destination_pool_count": 2, "default_role_worker_count": 3},
        "baseline_pool_layout": {
            "schema_version": 1, "role": "mesh-96", "expected_total_pool_count": 202,
            "pools": {name: {**row, "resource_id": prefix + name}
                      for name, row in baseline.EXPECTED_POOLS.items()},
        },
    }


def approved(payload):
    return baseline.validate_receipt(
        payload, run_id=baseline.RUN_ID, subscription_id=baseline.SUBSCRIPTION, expected_pool_count=202,
    )


@pytest.mark.parametrize("fault", [
    "incomplete", "not-retired", "wrong-plan", "other-role", "wrong-count", "boolean-count",
    "extra-pool", "wrong-sku", "wrong-mode", "foreign-id", "boolean-schema",
])
def test_only_exact_completed_modern_layout_is_accepted(fault):
    payload = receipt()
    if fault == "incomplete":
        payload["repaired"] = False
    elif fault == "not-retired":
        payload["modern_cni"]["source_retired"] = False
    elif fault == "wrong-plan":
        payload["plan_sha256"] = "b" * 64
    elif fault == "other-role":
        payload["baseline_pool_layout"]["role"] = "mesh-95"
    elif fault == "wrong-count":
        payload["baseline_pool_layout"]["expected_total_pool_count"] = 203
    elif fault == "boolean-count":
        payload["modern_cni"]["default_pool_count"] = True
    elif fault == "extra-pool":
        payload["baseline_pool_layout"]["pools"]["extra"] = {}
    elif fault == "boolean-schema":
        payload["baseline_pool_layout"]["schema_version"] = True
    else:
        pool = payload["baseline_pool_layout"]["pools"]["cniv5"]
        pool[{"wrong-sku": "vm_size", "wrong-mode": "mode", "foreign-id": "resource_id"}[fault]] = "unexpected"
    with pytest.raises(baseline.BaselineError):
        approved(payload)


def platform():
    clusters, aks, members = [], [], []
    for index in range(1, 101):
        role, name = f"mesh-{index}", f"clustermesh-{index}"
        cluster = verify.capture.Cluster(name=name, resource_group=baseline.RUN_ID, role=role, kubeconfig="unused")
        clusters.append(cluster)
        resource_id = (
            f"/subscriptions/{baseline.SUBSCRIPTION}/resourceGroups/{baseline.RUN_ID}/providers/"
            f"Microsoft.ContainerService/managedClusters/{name}"
        )
        names = list(baseline.EXPECTED_POOLS) if index == 96 else ["default", "prompool"]
        if index == 1:
            names.append("churnpool")
        pools = []
        for pool_name in names:
            row = {"name": pool_name, "provisioningState": "Succeeded", "powerState": {"code": "Running"}}
            if index == 96:
                expected = baseline.EXPECTED_POOLS[pool_name]
                row.update(count=expected["count"], mode=expected["mode"], vmSize=expected["vm_size"],
                           enableAutoScaling=False, nodeLabels={"prometheus": "true"} if pool_name == "promv5" else {})
            pools.append(row)
        aks.append({"id": resource_id, "name": name, "provisioningState": "Succeeded",
                    "powerState": {"code": "Running"}, "tags": {"role": role}, "agentPoolProfiles": pools})
        members.append({"name": role, "clusterResourceId": resource_id, "provisioningState": "Succeeded",
                        "meshProperties": {"status": {"state": "Connected"}}})

    def runner(command, _timeout):
        if command[:3] == ["az", "aks", "list"]:
            return json.dumps(aks)
        if command[:4] == ["az", "fleet", "clustermeshprofile", "list-members"]:
            return json.dumps(members)
        raise AssertionError(command)

    return clusters, aks, members, runner


def check_platform(clusters, runner, layout=None):
    return verify.validate_platform_state(
        clusters, subscription_id=baseline.SUBSCRIPTION, run_id=baseline.RUN_ID,
        expected_pool_count=202, fleet_name="clustermesh-flt", profile_name="clustermesh-cmp",
        runner=runner, modern_pool_layout=layout,
    )


def test_pool_count_alone_cannot_waive_exact_original_layout():
    clusters, _, _, runner = platform()
    with pytest.raises(verify.VerificationError, match="pool inventory is not exact"):
        check_platform(clusters, runner)
    payload = receipt()
    original = copy.deepcopy(payload)
    result = check_platform(clusters, runner, approved(payload))
    assert result["pool_count"] == 202 and result["original_pool_count"] == 201
    assert result["intentional_hardware_baseline_change"] and payload == original


@pytest.mark.parametrize("fault", ["other-cluster", "new-sku", "count", "placement", "fleet", "pool-state"])
def test_modern_delta_does_not_waive_live_health_or_unrelated_pool_identity(fault):
    clusters, aks, members, runner = platform()
    if fault == "other-cluster":
        aks[1]["agentPoolProfiles"][1]["name"] = "unapproved"
    elif fault == "fleet":
        members[95]["meshProperties"]["status"]["state"] = "Failed"
    else:
        row = aks[95]["agentPoolProfiles"][2]
        if fault == "new-sku":
            row["vmSize"] = "Standard_D16s_v5"
        elif fault == "count":
            row["count"] = 3
        elif fault == "placement":
            row["nodeLabels"]["prometheus"] = "true"
        else:
            row["provisioningState"] = "Failed"
    with pytest.raises(verify.VerificationError):
        check_platform(clusters, runner, approved(receipt()))


def test_handoff_preserves_historical_201_proof_and_validates_current_202(tmp_path, monkeypatch):
    proof = tmp_path / "modern.json"
    proof.write_text(json.dumps(receipt()), encoding="utf-8")
    clusters, _, _, runner = platform()
    rows = {cluster.role: {"cluster_name": cluster.role, "desired_state_sha256": {"nodes.yaml": "same"}}
            for cluster in clusters}
    seen = []
    monkeypatch.setattr(handoff.verify, "load_baseline", lambda *_args: ({}, rows))
    monkeypatch.setattr(handoff, "validate_verification_artifact",
                        lambda *_args, **kwargs: seen.append(kwargs["expected_pool_count"]) or {"healthy": True})
    monkeypatch.setattr(handoff.capture, "load_clusters", lambda *_args: clusters)
    monkeypatch.setattr(handoff.verify, "restore_state", lambda *_args: None)
    monkeypatch.setattr(handoff.verify, "run_reconciler",
                        lambda *_args, **_kwargs: {"success": True, "total_clusters": 100, "healthy_count": 100})
    monkeypatch.setattr(handoff.capture, "run_command", runner)
    monkeypatch.setattr(handoff.verify, "capture_live", lambda *_args, **_kwargs: [{}] * 100)
    arguments = [
        "--baseline-dir", "baseline", "--baseline-build-id", "79230",
        "--verification-dir", "verification", "--verification-build-id", "79261",
        "--clusters", "clusters.json", "--state-root", "state", "--artifact-dir", str(tmp_path / "output"),
        "--reconciler", "unused", "--run-id", baseline.RUN_ID, "--expected-subscription-id", baseline.SUBSCRIPTION,
        "--expected-cluster-count", "100", "--expected-mock-count", "100", "--expected-pool-count", "202",
        "--modern-baseline-proof", str(proof),
    ]
    assert handoff.main(arguments) == 0
    summary = json.loads((tmp_path / "output" / "summary.json").read_text(encoding="utf-8"))
    assert seen == [201] and summary["pool_count"] == 202
    assert summary["historical_verified_pool_count"] == 201 and summary["intentional_hardware_baseline_change"]
    assert not summary["healthy"] and not summary["workloads_started"]
