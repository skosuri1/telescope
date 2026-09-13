"""Fenced replacement lineage and current mesh-96 identity preservation at handoff."""

import copy
import hashlib
import importlib
import json
import sys
from pathlib import Path

import pytest


MODULE_DIR = Path(__file__).resolve().parents[1] / "clusterloader2/clustermesh-scale"
sys.path.insert(0, str(MODULE_DIR))
try:
    handoff = importlib.import_module("preserved_mock_handoff")
    baseline = importlib.import_module("modern_pool_baseline")
finally:
    sys.path.pop(0)


def evidence():
    """Build explicit synthetic source identities, not live recovery evidence."""

    originals = {f"kwok-node-{index}": f"retired-{index}" for index in range(56)}
    protected = {f"kwok-node-{index}": f"preserved-{index}" for index in range(56, 100)}
    current = {**protected, **{name: f"replacement-{index}" for index, name in enumerate(originals)}}
    nodes = {f"kwok-node-{index}": f"node-{index}" for index in range(100)}
    retirement = {
        "execute": True, "success": True, "native_fencing_proven": True, "source_retired": True,
        "replacements_ready": True, "placement_hold_removed": True, "current_mock_ready": 100,
        "kwok_ready": 100, "cleanup_errors": [], "plan_sha256": baseline.PLAN_SHA,
        "target": {
            "node_name": "aks-default-28928250-vmss000001",
            "node_uid": "c673a142-17ac-44c7-92cc-32efc0d34c61",
            "vm_id": "d81b78a9-fe40-468d-91ec-d66f0456bfa7",
        },
        "native": {"accepted": True, "ambiguous": False, "vm_absence_observed_at": "2026-09-13T06:08:25Z"},
        "current_mock_uids": current, "preserved_kwok_uids": nodes,
        "protected_mock_uids": protected, "original_target_mock_uids": originals,
        "controller_replacements": {
            name: {"old_uid": old, "new_uid": current[name], "ready": True, "fencing_proven": True,
                   "node_name": f"aks-cniv5-27550670-vmss00000{index % 2}"}
            for index, (name, old) in enumerate(originals.items())
        },
    }
    prefix = (
        f"/subscriptions/{baseline.SUBSCRIPTION}/resourceGroups/{baseline.RUN_ID}/providers/"
        "Microsoft.ContainerService/managedClusters/clustermesh-96/agentPools/"
    )
    receipt = {
        "success": True, "repaired": True, "workloads_ready": False, "plan_sha256": baseline.PLAN_SHA,
        "modern_cni": {"completed": True, "source_retired": True, "pool_name": "cniv5",
                       "default_pool_count": 1, "destination_pool_count": 2, "default_role_worker_count": 3},
        "baseline_pool_layout": {
            "schema_version": 1, "role": "mesh-96", "expected_total_pool_count": 202,
            "pools": {name: {**row, "resource_id": prefix + name} for name, row in baseline.EXPECTED_POOLS.items()},
        },
        "retirement_build_id": 80001, "current_mock_uids": copy.deepcopy(current),
        "preserved_kwok_uids": copy.deepcopy(nodes),
    }
    return retirement, receipt


def write_evidence(root, retirement, receipt):
    directory = root / "retirement-input"
    directory.mkdir(parents=True)
    source = directory / "retirement.json"
    source.write_text(json.dumps(retirement), encoding="utf-8")
    receipt["retirement_sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
    proof = root / "recovery.json"
    proof.write_text(json.dumps(receipt), encoding="utf-8")
    return proof


def test_retirement_source_authorizes_only_its_current_identities(tmp_path):
    retirement, receipt = evidence()
    path = write_evidence(tmp_path, retirement, receipt)
    before = copy.deepcopy((retirement, receipt))
    result = handoff.validate_post_retirement_artifact(str(path), receipt)
    assert result["retirement_build_id"] == 80001
    assert result["preserved_agent_count"] == 44 and result["fenced_replacement_count"] == 56
    assert result["agent_uids"] == retirement["current_mock_uids"]
    assert result["node_uids"] == retirement["preserved_kwok_uids"]
    assert (retirement, receipt) == before


@pytest.mark.parametrize("fault", [
    "wrong-build", "wrong-hash", "source-failed", "wrong-plan", "not-fenced", "not-retired", "hold-remains",
    "unready", "cleanup-error", "wrong-target", "not-accepted", "ambiguous", "not-absent",
    "missing-mock", "duplicate-uid", "missing-kwok", "missing-protected", "missing-replacement",
    "duplicate-original", "unfenced-replacement", "unready-replacement", "wrong-new-uid", "wrong-old-uid",
    "wrong-worker", "protected-changed", "monitoring-agent-changed", "monitoring-node-changed", "symlink",
])
def test_unproved_or_drifted_retirement_cannot_authorize_handoff(tmp_path, fault):
    retirement, receipt = evidence()
    if fault in ("source-failed", "not-fenced", "not-retired", "hold-remains"):
        retirement[{"source-failed": "success", "not-fenced": "native_fencing_proven",
                    "not-retired": "source_retired", "hold-remains": "placement_hold_removed"}[fault]] = False
    elif fault == "wrong-plan":
        retirement["plan_sha256"] = "wrong"
    elif fault == "unready":
        retirement["kwok_ready"] = 99
    elif fault == "cleanup-error":
        retirement["cleanup_errors"] = ["hold remains"]
    elif fault == "wrong-target":
        retirement["target"]["vm_id"] = "other"
    elif fault in ("not-accepted", "ambiguous", "not-absent"):
        retirement["native"][{"not-accepted": "accepted", "ambiguous": "ambiguous",
                              "not-absent": "vm_absence_observed_at"}[fault]] = fault == "ambiguous"
    elif fault in ("missing-mock", "missing-kwok", "missing-protected", "missing-replacement"):
        mapping = retirement[{"missing-mock": "current_mock_uids", "missing-kwok": "preserved_kwok_uids",
                              "missing-protected": "protected_mock_uids",
                              "missing-replacement": "controller_replacements"}[fault]]
        mapping.pop(next(iter(mapping)))
    elif fault == "duplicate-uid":
        retirement["current_mock_uids"]["kwok-node-0"] = retirement["current_mock_uids"]["kwok-node-1"]
    elif fault == "duplicate-original":
        retirement["original_target_mock_uids"]["kwok-node-0"] = retirement["original_target_mock_uids"]["kwok-node-1"]
    elif fault in ("unfenced-replacement", "unready-replacement", "wrong-new-uid", "wrong-old-uid", "wrong-worker"):
        key = {"unfenced-replacement": "fencing_proven", "unready-replacement": "ready",
               "wrong-new-uid": "new_uid", "wrong-old-uid": "old_uid", "wrong-worker": "node_name"}[fault]
        retirement["controller_replacements"]["kwok-node-0"][key] = False if key in ("fencing_proven", "ready") else "other"
    elif fault == "protected-changed":
        retirement["protected_mock_uids"]["kwok-node-99"] = "other"
    elif fault == "monitoring-agent-changed":
        receipt["current_mock_uids"]["kwok-node-0"] = "other"
    elif fault == "monitoring-node-changed":
        receipt["preserved_kwok_uids"]["kwok-node-0"] = "other"
    elif fault == "wrong-build":
        receipt["retirement_build_id"] = 79992
    proof = write_evidence(tmp_path, retirement, receipt)
    if fault == "wrong-hash":
        receipt["retirement_sha256"] = "0" * 64
    if fault == "symlink":
        source = tmp_path / "retirement-input/retirement.json"
        replacement = tmp_path / "source.json"
        source.rename(replacement)
        source.symlink_to(replacement)
    with pytest.raises((handoff.HandoffError, handoff.verify.VerificationError)):
        handoff.validate_post_retirement_artifact(str(proof), receipt)


@pytest.mark.parametrize("fault", ["none", "pre-agent", "pre-node", "post-agent", "post-node", "missing-post"])
def test_current_identity_checks_surround_reconcile_without_rewriting_history(tmp_path, monkeypatch, fault):
    retirement, receipt = evidence()
    proof = write_evidence(tmp_path / "source", retirement, receipt)
    expected = {"role": "mesh-96", "agent_uids": retirement["current_mock_uids"],
                "node_uids": retirement["preserved_kwok_uids"]}
    clusters = [handoff.capture.Cluster(name=f"clustermesh-{index}", resource_group=baseline.RUN_ID,
                                      role=f"mesh-{index}", kubeconfig="private-unused")
                for index in range(1, 101)]
    rows = {cluster.role: {"cluster_name": cluster.role, "desired_state_sha256": {"nodes.yaml": "same"}}
            for cluster in clusters}
    pre = copy.deepcopy(expected)
    post = [{"role": cluster.role} for cluster in clusters]
    post[95] = copy.deepcopy(expected)
    if fault.startswith("pre-"):
        pre["agent_uids" if fault == "pre-agent" else "node_uids"]["kwok-node-0"] = "unauthorized"
    if fault.startswith("post-"):
        post[95]["agent_uids" if fault == "post-agent" else "node_uids"]["kwok-node-0"] = "unauthorized"
    if fault == "missing-post":
        post.pop(95)
    events = []
    monkeypatch.setattr(handoff.verify, "load_baseline", lambda *_args: ({}, rows))
    monkeypatch.setattr(handoff, "validate_verification_artifact",
                        lambda *_args, **kwargs: events.append(("historical", kwargs["expected_pool_count"])) or {"healthy": True})
    monkeypatch.setattr(handoff.capture, "load_clusters", lambda *_args: clusters)
    monkeypatch.setattr(handoff.verify, "restore_state", lambda *_args: None)
    monkeypatch.setattr(handoff, "capture_post_retirement_identities",
                        lambda *_args: events.append(("pre",)) or pre)
    monkeypatch.setattr(handoff.verify, "run_reconciler",
                        lambda *_args, **_kwargs: events.append(("reconcile",))
                        or {"success": True, "total_clusters": 100, "healthy_count": 100})
    monkeypatch.setattr(handoff.verify, "validate_platform_state",
                        lambda *_args, **kwargs: {"pool_count": kwargs["expected_pool_count"], "resource_ids": {}})
    monkeypatch.setattr(handoff.verify, "capture_live", lambda *_args, **_kwargs: post)
    args = [
        "--baseline-dir", "baseline", "--baseline-build-id", "79230",
        "--verification-dir", "verification", "--verification-build-id", "79261",
        "--clusters", "clusters.json", "--state-root", "state", "--artifact-dir", str(tmp_path / "output"),
        "--reconciler", "unused", "--run-id", baseline.RUN_ID, "--expected-subscription-id", baseline.SUBSCRIPTION,
        "--expected-cluster-count", "100", "--expected-mock-count", "100", "--expected-pool-count", "202",
        "--modern-baseline-proof", str(proof),
    ]
    assert handoff.main(args) == (0 if fault == "none" else 1)
    assert events[:2] == [("historical", 201), ("pre",)]
    assert (("reconcile",) in events) is not fault.startswith("pre-")
    summary = json.loads((tmp_path / "output/summary.json").read_text(encoding="utf-8"))
    assert summary["historical_verified_pool_count"] == 201 and not summary["workloads_started"]
    if fault == "none":
        assert summary["post_retirement_identities_preserved"]
        assert summary["pool_count"] == 202 and summary["handoff_validation_healthy"] and not summary["healthy"]
        stored = json.loads((tmp_path / "output/handoff.json").read_text(encoding="utf-8"))
        assert stored["post_retirement_identity_chain"]["fenced_replacement_count"] == 56
    else:
        assert summary["healthy"] is False and "fatal_error" in summary
    assert json.loads(proof.read_text(encoding="utf-8")) == receipt


@pytest.mark.parametrize("fault", ["none", "deleting-node", "deleting-agent", "unready-node", "unready-agent", "bad-conditions"])
def test_prereconcile_identity_probe_is_readonly_and_requires_health(monkeypatch, fault):
    nodes = {"items": [{"metadata": {"name": f"kwok-node-{index}", "uid": f"node-{index}"},
                        "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
                       for index in range(100)]}
    agents = {"items": [{"metadata": {"name": f"kwok-node-{index}", "uid": f"agent-{index}"},
                         "status": {"phase": "Running", "containerStatuses": [{"ready": True}]}}
                        for index in range(100)]}
    if fault.startswith("deleting-"):
        (nodes if fault == "deleting-node" else agents)["items"][0]["metadata"]["deletionTimestamp"] = "now"
    if fault == "unready-node":
        nodes["items"][0]["status"]["conditions"][0]["status"] = "False"
    if fault == "unready-agent":
        agents["items"][0]["status"]["containerStatuses"][0]["ready"] = False
    if fault == "bad-conditions":
        nodes["items"][0]["status"]["conditions"] = None
    commands = []

    def runner(command, timeout):
        commands.append(command)
        assert command[:4] == ["kubectl", "--kubeconfig", "private-unused", "--request-timeout=120s"]
        assert timeout == 120 and "get" in command
        return json.dumps(nodes if "nodes" in command else agents)

    monkeypatch.setattr(handoff.capture, "run_command", runner)
    cluster = handoff.capture.Cluster(name="clustermesh-96", resource_group=baseline.RUN_ID,
                                     role="mesh-96", kubeconfig="private-unused")
    if fault == "none":
        result = handoff.capture_post_retirement_identities(cluster, 120)
        assert len(result["node_uids"]) == len(result["agent_uids"]) == 100
    else:
        with pytest.raises((handoff.HandoffError, handoff.capture.CaptureError)):
            handoff.capture_post_retirement_identities(cluster, 120)
    assert len(commands) == 2
