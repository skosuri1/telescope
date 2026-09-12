"""Tests for finishing a UID-pinned, already drained worker retirement."""

import importlib.util
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml


MODULE_DIR = (
    Path(__file__).resolve().parents[1] / "clusterloader2" / "clustermesh-scale"
)
SPEC = importlib.util.spec_from_file_location(
    "prepared_worker_retirement", MODULE_DIR / "prepared_worker_retirement.py"
)
retirement = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = retirement
sys.path.insert(0, str(MODULE_DIR))
try:
    SPEC.loader.exec_module(retirement)
finally:
    sys.path.pop(0)

VMSS = "aks-default-12345678-vmss"
SOURCE = f"{VMSS}000005"
SOURCE_UID = "88888888-8888-8888-8888-888888888888"


@pytest.fixture(name="args")
def retirement_args(tmp_path):
    return SimpleNamespace(
        expected_subscription="11111111-1111-1111-1111-111111111111",
        resource_group="12345-aabbccdd",
        confirm_resource_group="12345-aabbccdd",
        expected_region="eastus2euap",
        expected_tfvars_sha="a" * 64,
        role="mesh-38",
        node_name=SOURCE,
        node_uid=SOURCE_UID,
        timeout_seconds=300,
        execute=True,
        summary_file=str(tmp_path / "summary.json"),
    )


def scope_data(options):
    scope = (
        f"/subscriptions/{options.expected_subscription}"
        f"/resourceGroups/{options.resource_group}"
    )
    expiry = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    group = {
        "id": scope, "location": options.expected_region,
        "tags": {
            "clustermesh_debug_preserved": "true",
            "run_id": options.resource_group,
            "scenario": "perf-eval-clustermesh-scale",
            "clustermesh_debug_expected_clusters": "100",
            "clustermesh_debug_tfvars_sha256": options.expected_tfvars_sha,
            "deletion_due_time": expiry,
        },
    }
    clusters, members = [], []
    fleet = f"{scope}/providers/Microsoft.ContainerService/fleets/clustermesh-flt"
    for index in range(1, 101):
        name, role = f"clustermesh-{index}", f"mesh-{index}"
        resource_id = (
            f"{scope}/providers/Microsoft.ContainerService/managedClusters/{name}"
        )
        clusters.append({
            "id": resource_id, "name": name, "location": options.expected_region,
            "nodeResourceGroup": f"MC_{options.resource_group}_{name}_eastus2euap",
            "tags": {"role": role, "run_id": options.resource_group},
            "provisioningState": "Succeeded", "powerState": {"code": "Running"},
        })
        members.append({
            "id": f"{fleet}/members/{role}", "name": role,
            "clusterResourceId": resource_id, "provisioningState": "Succeeded",
            "labels": {"mesh": "true"},
            "meshProperties": {
                "ciliumProperties": {"name": f"assigned-{index}", "id": index},
                "clusterMeshProfileResourceId": f"{fleet}/clusterMeshProfiles/clustermesh-cmp",
                "status": {"state": "Connected"},
            },
        })
    return group, clusters, members


def workload_data(options, deleted=False):
    node_group = f"MC_{options.resource_group}_clustermesh-38_eastus2euap"
    nodes = [
        {
            "metadata": {
                "name": f"kwok-node-{index}", "uid": f"kwok-uid-{index}",
                "labels": {"type": "kwok"},
            },
            "status": {"conditions": [{"type": "Ready", "status": "True"}]},
        }
        for index in range(100)
    ]
    for index in ([1, 6, 8] if deleted else [1, 5, 6, 8]):
        name = f"{VMSS}{index:06}"
        nodes.append({
            "metadata": {
                "name": name, "uid": SOURCE_UID if index == 5 else f"real-{index}",
                "labels": {
                    "agentpool": "default",
                    "kubernetes.azure.com/cluster": node_group,
                },
                "annotations": {retirement.HOLD_KEY: "test-bounded-worker-retirement"},
            },
            "spec": {
                "unschedulable": index == 5,
                "providerID": (
                    f"azure:///subscriptions/{options.expected_subscription}/resourceGroups/"
                    f"{node_group}/providers/Microsoft.Compute/virtualMachineScaleSets/"
                    f"{VMSS}/virtualMachines/{index}"
                ),
                "taints": [{
                    "key": retirement.HOLD_KEY, "value": "cns-ip-programming",
                    "effect": "NoSchedule",
                }] if index == 5 else [],
            },
            "status": {"conditions": [{"type": "Ready", "status": "True"}]},
        })
    pods = [
        {
            "metadata": {
                "name": f"kwok-node-{index}", "uid": f"agent-uid-{index}",
                "namespace": "mock-clustermesh", "labels": {"app": "mock-cilium-agent"},
                "ownerReferences": [{
                    "kind": "StatefulSet", "name": "kwok-node",
                    "uid": "controller-uid", "controller": True,
                }],
            },
            "spec": {"nodeName": f"{VMSS}000001"},
            "status": {
                "phase": "Running", "containerStatuses": [{"ready": True}],
                "conditions": [{"type": "Ready", "status": "True"}],
            },
        }
        for index in range(100)
    ]
    if not deleted:
        pods.append({
            "metadata": {
                "name": "cilium-source", "namespace": "kube-system",
                "ownerReferences": [{
                    "kind": "DaemonSet", "name": "cilium",
                    "uid": "cilium-uid", "controller": True,
                }],
            },
            "spec": {"nodeName": SOURCE},
        })
    controller = {
        "metadata": {"uid": "controller-uid"},
        "spec": {"replicas": 100, "template": {"spec": {}}},
    }
    daemonsets = {"items": [{"metadata": {"name": "cilium", "uid": "cilium-uid"}}]}
    return {"items": nodes}, {"items": pods}, controller, daemonsets


def pool_state(deleted=False):
    ids = ["1", "6", "8"] if deleted else ["1", "5", "6", "8"]
    pool = retirement.workers.PoolState(
        role="mesh-38", cluster_name="clustermesh-38",
        resource_group="12345-aabbccdd", node_resource_group="node-rg",
        pool_name="default", desired_count=len(ids),
        pool_provisioning_state="Succeeded", pool_power_state="Running",
        vmss_name=VMSS, vmss_capacity=len(ids), vmss_provisioning_state="Succeeded",
        instance_ids=ids, failed_instance_ids=[], node_instance_ids=ids,
        ready_instance_ids=ids, unschedulable_nodes=[] if deleted else [SOURCE],
        stale_instance_ids=[],
    )
    return retirement.workers.ClusterState("mesh-38", "clustermesh-38", "12345-aabbccdd", [pool])


def test_exact_scope_uses_fleet_assigned_identities(args):
    selected, identities = retirement.validate_scope(args, *scope_data(args))
    assert selected["name"] == "clustermesh-38"
    assert len(identities) == 100
    assert identities[37]["cluster_name"] == "assigned-38"


@pytest.mark.parametrize("fault", [
    "fingerprint", "expired", "missing_cluster", "duplicate_role",
    "wrong_member_owner", "duplicate_cilium_id", "disconnected", "wrong_profile",
])
def test_unsafe_scope_is_rejected(args, fault):
    group, clusters, members = scope_data(args)
    if fault == "fingerprint":
        group["tags"]["clustermesh_debug_tfvars_sha256"] = "b" * 64
    elif fault == "expired":
        group["tags"]["deletion_due_time"] = "2000-01-01T00:00:00Z"
    elif fault == "missing_cluster":
        clusters.pop()
    elif fault == "duplicate_role":
        clusters[1]["tags"]["role"] = "mesh-1"
    elif fault == "wrong_member_owner":
        members[0]["clusterResourceId"] = clusters[1]["id"]
    elif fault == "duplicate_cilium_id":
        members[1]["meshProperties"]["ciliumProperties"]["id"] = 1
    elif fault == "disconnected":
        members[0]["meshProperties"]["status"]["state"] = "Failed"
    elif fault == "wrong_profile":
        members[0]["meshProperties"]["clusterMeshProfileResourceId"] += "-foreign"
    with pytest.raises(retirement.workers.ReconcileError):
        retirement.validate_scope(args, group, clusters, members)


def test_only_prepared_cordon_is_accepted():
    state = pool_state()
    assert retirement.validate_pool_state(state, SOURCE, True).desired_count == 4
    state.pools[0].unschedulable_nodes.append(f"{VMSS}000001")
    with pytest.raises(retirement.workers.ReconcileError, match="only cordoned"):
        retirement.validate_pool_state(state, SOURCE, True)


@pytest.mark.parametrize("field,value", [
    ("desired_count", 5), ("vmss_capacity", 5),
    ("pool_provisioning_state", "Updating"), ("stale_instance_ids", ["1"]),
])
def test_unrelated_worker_drift_is_rejected(field, value):
    state = pool_state()
    setattr(state.pools[0], field, value)
    with pytest.raises(retirement.workers.ReconcileError):
        retirement.validate_pool_state(state, SOURCE, True)


def test_ready_workloads_and_exact_drained_source_are_accepted(args):
    before = retirement.validate_workloads(args, *workload_data(args))
    after = retirement.validate_workloads(args, *workload_data(args, deleted=True))
    assert before["source"]["metadata"]["uid"] == SOURCE_UID
    assert after["source"] is None
    assert before["kwok_uids"] == after["kwok_uids"]
    assert before["agent_uids"] == after["agent_uids"]


@pytest.mark.parametrize("fault", [
    "source_uid", "source_deleting", "uncordoned", "kwok_unready",
    "kwok_duplicate_uid", "agent_unready", "agent_condition",
    "agent_owner", "agent_on_source", "foreign_daemonset", "source_pvc",
])
def test_unsafe_workload_or_drain_state_is_rejected(args, fault):
    nodes, pods, controller, daemonsets = workload_data(args)
    source = next(row for row in nodes["items"] if row["metadata"]["name"] == SOURCE)
    if fault == "source_uid":
        source["metadata"]["uid"] = "replacement-uid"
    elif fault == "source_deleting":
        source["metadata"]["deletionTimestamp"] = "2026-09-01T00:00:00Z"
    elif fault == "uncordoned":
        source["spec"]["unschedulable"] = False
    elif fault == "kwok_unready":
        nodes["items"][0]["status"]["conditions"][0]["status"] = "False"
    elif fault == "kwok_duplicate_uid":
        nodes["items"][1]["metadata"]["uid"] = nodes["items"][0]["metadata"]["uid"]
    elif fault == "agent_unready":
        pods["items"][0]["status"]["containerStatuses"][0]["ready"] = False
    elif fault == "agent_condition":
        pods["items"][0]["status"]["conditions"][0]["status"] = "False"
    elif fault == "agent_owner":
        pods["items"][0]["metadata"]["ownerReferences"][0]["uid"] = "foreign"
    elif fault == "agent_on_source":
        pods["items"][0]["spec"]["nodeName"] = SOURCE
    elif fault == "foreign_daemonset":
        pods["items"][-1]["metadata"]["ownerReferences"][0]["uid"] = "foreign"
    elif fault == "source_pvc":
        pods["items"][-1]["spec"]["volumes"] = [{"persistentVolumeClaim": {"claimName": "data"}}]
    with pytest.raises(retirement.workers.ReconcileError):
        retirement.validate_workloads(args, nodes, pods, controller, daemonsets)


@pytest.fixture(name="backend")
def retirement_backend(args, monkeypatch):
    group, clusters, members = scope_data(args)
    state = SimpleNamespace(
        deleted=False, denied=False, fail_final_peers=False, calls=[],
        orphan_pod_reads=0, terminating_node_reads=0, clock=0.0,
    )
    node_group = {
        "managedBy": clusters[37]["id"], "location": args.expected_region,
        "tags": {"deletion_due_time": group["tags"]["deletion_due_time"]},
    }

    def run(command, _timeout):
        state.calls.append(list(command))
        if command[:3] == ["az", "account", "show"]:
            return json.dumps({"id": args.expected_subscription})
        if command[0] == "az":
            assert command[-2:] == ["--subscription", args.expected_subscription]
        if command[:3] == ["az", "group", "show"]:
            name = command[command.index("--name") + 1]
            return json.dumps(group if name == args.resource_group else node_group)
        if command[:3] == ["az", "aks", "list"]:
            return json.dumps(clusters)
        if command[:4] == ["az", "fleet", "member", "list"]:
            return json.dumps(members)
        if command[:3] == ["az", "aks", "get-credentials"]:
            Path(command[command.index("--file") + 1]).touch()
            return ""
        if command[:3] == ["az", "vmss", "list-instances"]:
            ids = ["1", "6", "8"] if state.deleted else ["1", "5", "6", "8"]
            return json.dumps([
                {"instanceId": value, "provisioningState": "Succeeded"}
                for value in ids
            ])
        if command[:4] == ["az", "aks", "nodepool", "show"]:
            return json.dumps({
                "count": 3 if state.deleted else 4, "enableAutoScaling": False,
                "provisioningState": "Succeeded", "powerState": {"code": "Running"},
                "vmSize": "Standard_D8_v3",
            })
        if command[:4] == ["az", "aks", "nodepool", "delete-machines"]:
            if state.denied:
                raise retirement.workers.ReconcileError("AuthorizationFailed")
            state.deleted = True
            return ""
        if command[0] == "kubectl":
            kind = command[command.index("get") + 1]
            data = workload_data(args, deleted=state.deleted)
            if state.deleted and kind == "nodes" and state.terminating_node_reads:
                state.terminating_node_reads -= 1
                source = next(
                    row for row in workload_data(args)[0]["items"]
                    if row["metadata"]["name"] == SOURCE
                )
                source["metadata"]["deletionTimestamp"] = "2026-09-01T00:00:00Z"
                source["status"]["conditions"][0]["status"] = "False"
                data[0]["items"].append(source)
            if state.deleted and kind == "pods" and state.orphan_pod_reads:
                state.orphan_pod_reads -= 1
                data[1]["items"].append(workload_data(args)[1]["items"][-1])
            if kind == "nnc":
                return '{"items":[]}'
            return json.dumps(data[{"nodes": 0, "pods": 1, "statefulset": 2, "daemonsets": 3}[kind]])
        raise AssertionError(f"Unexpected command: {command}")

    def peer_proof(**_kwargs):
        data = workload_data(args, deleted=state.deleted)[0]["items"]
        healthy = not (state.deleted and state.fail_final_peers)
        return {
            "healthy": healthy,
            "agents": [
                {"node_name": row["metadata"]["name"], "healthy": healthy}
                for row in data if "providerID" in row.get("spec", {})
            ],
        }

    monkeypatch.setattr(retirement.workers, "probe_cluster", lambda *_: pool_state(state.deleted))
    monkeypatch.setattr(retirement.cilium, "probe", peer_proof)
    monkeypatch.setattr(retirement.time, "monotonic", lambda: state.clock)
    monkeypatch.setattr(
        retirement.time, "sleep", lambda seconds: setattr(state, "clock", state.clock + seconds)
    )
    state.run = run
    return state


def test_retirement_submits_once_and_preserves_workload_identity(args, backend):
    summary = {"success": False, "mutation_started": False, "request_accepted": False}
    retirement.execute_retirement(args, summary, backend.run)
    assert summary["success"] and summary["status"] == "retired"
    assert summary["request_accepted"]
    writes = [call for call in backend.calls if "delete-machines" in call]
    assert len(writes) == 1
    assert writes[0][writes[0].index("--machine-names") + 1] == SOURCE
    assert summary["after"]["pools"][0]["desired_count"] == 3


def test_observe_only_does_not_mutate(args, backend):
    args.execute = False
    summary = {"success": False, "mutation_started": False}
    retirement.execute_retirement(args, summary, backend.run)
    assert summary["status"] == "prepared"
    assert not any("delete-machines" in call for call in backend.calls)


def test_already_absent_worker_is_never_deleted_again(args, backend):
    backend.deleted = True
    summary = {"success": False, "mutation_started": False}
    retirement.execute_retirement(args, summary, backend.run)
    assert summary["status"] == "already-absent"
    assert not any("delete-machines" in call for call in backend.calls)


def test_post_retirement_waits_for_node_and_daemonset_garbage_collection(args, backend):
    backend.terminating_node_reads = 1
    backend.orphan_pod_reads = 3
    summary = {"success": False, "mutation_started": False, "request_accepted": False}
    retirement.execute_retirement(args, summary, backend.run)
    assert summary["success"] and summary["status"] == "retired"
    assert summary["pending_source_pod_references"] == []
    assert backend.clock >= 30
    assert len([call for call in backend.calls if "delete-machines" in call]) == 1


def test_idempotent_observation_waits_for_old_daemonset_pods_without_mutation(args, backend):
    backend.deleted = True
    backend.orphan_pod_reads = 3
    summary = {"success": False, "mutation_started": False}
    retirement.execute_retirement(args, summary, backend.run)
    assert summary["success"] and summary["status"] == "already-absent"
    assert backend.clock >= 30
    assert not any("delete-machines" in call for call in backend.calls)


def test_authorization_failure_is_not_retried(args, backend):
    backend.denied = True
    summary = {"success": False, "mutation_started": False, "request_accepted": False}
    with pytest.raises(retirement.workers.ReconcileError, match="AuthorizationFailed"):
        retirement.execute_retirement(args, summary, backend.run)
    assert not summary["success"] and not summary["request_accepted"]
    assert len([call for call in backend.calls if "delete-machines" in call]) == 1


def test_transient_read_timeout_is_reobserved_without_repeating_deletion(args, backend):
    reads = {"group": 0}

    def transient(command, timeout):
        if command[:3] == ["az", "group", "show"] and args.resource_group in command:
            reads["group"] += 1
            if reads["group"] == 1:
                raise retirement.workers.ReconcileError("command timed out after 45s: az group show")
        return backend.run(command, timeout)

    summary = {"success": False, "mutation_started": False, "request_accepted": False}
    retirement.execute_retirement(args, summary, transient)
    assert summary["success"] is True and reads["group"] == 2
    assert len([call for call in backend.calls if "delete-machines" in call]) == 1


def test_authorization_failure_on_read_is_never_retried(args, backend):
    reads = []

    def denied(command, timeout):
        if command[:3] == ["az", "group", "show"]:
            reads.append(command)
            raise retirement.workers.ReconcileError("AuthorizationFailed")
        return backend.run(command, timeout)

    with pytest.raises(retirement.workers.ReconcileError, match="AuthorizationFailed"):
        retirement.execute_retirement(args, {"success": False}, denied)
    assert len(reads) == 1 and not backend.deleted


@pytest.mark.parametrize("fault", ["converges", "exhausted", "identity", "foreign", "other-error", "malformed-error"])
def test_fleet_partial_observation_keeps_all_identity_and_connected_gates(args, backend, fault):
    observed = {"profile": 0}

    def fleet_reads(command, timeout):
        initial = command[:4] == ["az", "fleet", "member", "list"]
        profile = command[:4] == ["az", "fleet", "clustermeshprofile", "list-members"]
        if not initial and not profile:
            return backend.run(command, timeout)
        members = scope_data(args)[2]
        if profile:
            observed["profile"] += 1
        if initial or fault != "converges" or observed["profile"] == 1:
            members[21]["meshProperties"]["status"] = {
                "state": "Disconnected", "error": {"code": "PartialConnectivity"},
            }
        if fault == "foreign":
            members[21]["clusterResourceId"] = "foreign"
        if fault == "identity" and profile:
            members[21]["meshProperties"]["ciliumProperties"]["name"] = "changed-name"
        if fault == "other-error":
            members[21]["meshProperties"]["status"]["error"]["code"] = "ConnectivityTimeout"
        if fault == "malformed-error":
            members[21]["meshProperties"]["status"]["error"] = "unreadable"
        return json.dumps(members)

    summary = {"success": False, "mutation_started": False, "request_accepted": False}
    if fault == "converges":
        retirement.execute_retirement(args, summary, fleet_reads)
        assert summary["success"] is True and observed["profile"] == 2
        assert summary["initial_unhealthy_fleet_roles"] == ["mesh-22"]
        assert summary["initial_fleet_members"][21]["meshProperties"]["status"]["state"] != "Connected"
        assert summary["retirement_fleet_members"][21]["meshProperties"]["status"]["state"] == "Connected"
        assert len([call for call in backend.calls if "delete-machines" in call]) == 1
    else:
        with pytest.raises(retirement.workers.ReconcileError):
            retirement.execute_retirement(args, summary, fleet_reads)
        assert not backend.deleted and not summary["mutation_started"]
        assert summary["initial_fleet_members"]
        if fault in ("foreign", "other-error", "malformed-error"):
            assert observed["profile"] == 0
        if fault == "exhausted":
            assert observed["profile"] == 4
        if fault == "identity":
            assert observed["profile"] == 1


@pytest.mark.parametrize("fault", [
    "healthy-peers", "failed-read", "failed-cilium", "denied-credentials", "foreign", "deadline", "too-many",
])
def test_failed_fleet_diagnostics_are_private_bounded_and_never_permit_retirement(
    args, backend, monkeypatch, fault,
):
    calls, credentials = [], []
    _, clusters, members = scope_data(args)
    roles = range(6) if fault == "too-many" else [95]
    for index in roles:
        members[index]["meshProperties"]["status"] = {
            "state": "Failed", "error": {"code": "ConnectivityTimeout"},
        }
    if fault == "foreign":
        members[95]["clusterResourceId"] = "foreign"
    pod = {
        "metadata": {
            "name": "clustermesh-apiserver-test", "uid": "api-pod-uid",
            "namespace": "kube-system",
        },
        "spec": {"containers": [{"name": "apiserver"}]},
        "status": {"containerStatuses": [{"name": "apiserver", "restartCount": 1}]},
    }

    def diagnostic_read(command, timeout):
        calls.append(list(command))
        assert 0 < timeout <= 45
        if command[:4] == ["az", "fleet", "member", "list"]:
            return json.dumps(members)
        if command[:3] == ["az", "aks", "get-credentials"]:
            assert command[-2:] == ["--subscription", args.expected_subscription]
            assert command[command.index("--name") + 1] == clusters[95]["name"]
            path = Path(command[command.index("--file") + 1])
            credentials.append(path)
            if fault == "denied-credentials":
                raise retirement.workers.ReconcileError("AuthorizationFailed")
            path.write_text("private-test-credential", encoding="utf-8")
            if fault == "deadline":
                backend.clock += 301
            return ""
        if command[0] == "kubectl":
            if "--context" in command:
                assert command[command.index("--context") + 1] == clusters[95]["name"]
            path = Path(command[command.index("--kubeconfig") + 1])
            assert path.stat().st_mode & 0o777 == 0o600
            if "exec" in command:
                if fault == "failed-cilium":
                    raise retirement.workers.ReconcileError("Cilium status read failed")
                return json.dumps({"cluster-mesh": {"clusters": []}})
            if "logs" in command:
                return "bounded container log\n"
            if "--raw=/readyz" in command:
                return "ok"
            if "events" in command and fault == "failed-read":
                raise retirement.workers.ReconcileError("command timed out after 45s")
            if "pods" in command:
                return json.dumps({"items": [pod]})
            return json.dumps({"items": []})
        return backend.run(command, timeout)

    def peers(**kwargs):
        assert kwargs["role"] == "mesh-96"
        assert kwargs["expected_remote_count"] == 99
        assert kwargs["expected_remote_names"] == {f"assigned-{index}" for index in range(1, 101) if index != 96}
        try:
            kwargs["runner"]([
                "kubectl", "--kubeconfig", kwargs["kubeconfig"],
                "-n", "kube-system", "exec", "cilium-test", "-c", "cilium-agent",
                "--", "cilium-dbg", "status", "-o", "json",
            ], 45)
        except retirement.cilium.overlay.ProbeError as error:
            return {"healthy": False, "agents": [], "fatal_error": str(error)}
        return {"healthy": True, "agents": [{"node_name": "real-node", "healthy": True}]}

    monkeypatch.setattr(retirement.cilium, "probe", peers)
    summary = {"success": False, "mutation_started": False, "request_accepted": False}
    with pytest.raises(retirement.workers.ReconcileError):
        retirement.execute_retirement(args, summary, diagnostic_read)
    assert not summary["success"] and not summary["mutation_started"] and not summary["request_accepted"]
    assert not any("delete-machines" in call or "update" in call or "patch" in call for call in calls)
    assert all(not path.exists() for path in credentials)
    if fault == "foreign":
        assert "fleet_failure_diagnostics" not in summary and not credentials
        return
    diagnostics = summary["fleet_failure_diagnostics"]
    assert diagnostics["read_only"]
    if fault == "too-many":
        assert diagnostics["errors"] and not credentials and not diagnostics["roles"]
        return
    record = diagnostics["roles"]["mesh-96"]
    if fault == "denied-credentials":
        assert len(credentials) == 1
        assert record["errors"][0]["error"] == "AuthorizationFailed"
        assert not any(call[0] == "kubectl" for call in calls)
    elif fault == "deadline":
        assert not any(call[0] == "kubectl" for call in calls)
        assert any("deadline" in error["error"] for error in record["errors"])
    else:
        assert record["log_pod_uids"] == {pod["metadata"]["name"]: "api-pod-uid"}
        assert record["cilium"]["healthy"] is (fault != "failed-cilium")
        if fault == "failed-cilium":
            assert record["cilium"]["fatal_error"] == "Cilium status read failed"
        else:
            assert "cilium-test-status.json" in record["files"]
        assert "clustermesh-apiserver-test-apiserver-previous.log" in record["files"]
        if fault == "failed-read":
            assert any(error["capture"] == "events.json" for error in record["errors"])
    for path in Path(args.summary_file).parent.rglob("*"):
        if path.is_file():
            assert "private-test-credential" not in path.read_text(encoding="utf-8")


def test_failed_final_peer_proof_is_preserved_and_fatal(args, backend):
    backend.fail_final_peers = True
    summary = {"success": False, "mutation_started": False, "request_accepted": False}
    with pytest.raises(retirement.workers.ReconcileError, match="Strict Cilium"):
        retirement.execute_retirement(args, summary, backend.run)
    saved = json.loads(Path(args.summary_file).read_text(encoding="utf-8"))
    assert saved["request_accepted"]
    assert not saved["cilium_after"]["healthy"]
    assert not saved["success"]


def test_invalid_cli_confirmation_fails_before_execution(args):
    arguments = []
    for name in (
        "resource_group", "confirm_resource_group", "expected_subscription",
        "expected_region", "expected_tfvars_sha", "role", "node_name",
        "node_uid", "summary_file",
    ):
        value = "different" if name == "confirm_resource_group" else getattr(args, name)
        arguments.extend([f"--{name.replace('_', '-')}", value])
    with pytest.raises(SystemExit) as error:
        retirement.parse_args(arguments)
    assert error.value.code == 2


@pytest.mark.parametrize("role,node,uid,expected_returncode", [
    ("", "", "", 0),
    ("mesh-38", SOURCE, "", 1),
    ("", SOURCE, SOURCE_UID, 1),
])
def test_pipeline_step_is_disabled_or_rejects_partial_plan_before_azure(
    role, node, uid, expected_returncode
):
    repository = MODULE_DIR.parents[3]
    template = yaml.safe_load(
        (repository / "steps/topology/clustermesh-scale/reuse/retire-prepared-worker.yml")
        .read_text(encoding="utf-8")
    )
    environment = dict(
        os.environ, RETIREMENT_ROLE=role, RETIREMENT_NODE=node, RETIREMENT_UID=uid
    )
    result = subprocess.run(
        ["bash", "-c", template["steps"][0]["script"]],
        env=environment, capture_output=True, text=True, check=False, timeout=10,
    )
    assert result.returncode == expected_returncode
    assert "command not found" not in result.stderr


def test_pipeline_wires_retirement_before_arm_recovery():
    repository = MODULE_DIR.parents[3]
    job = yaml.safe_load(
        (repository / "jobs/clustermesh-debug-resume.yml").read_text(encoding="utf-8")
    )
    steps = job["jobs"][0]["steps"]
    retirement_index = next(
        index for index, step in enumerate(steps)
        if step.get("template", "").endswith("/retire-prepared-worker.yml")
    )
    arm_index = next(
        index for index, step in enumerate(steps)
        if step.get("template", "").endswith("/reconcile-preserved-arm.yml")
    )
    assert retirement_index < arm_index
    pipeline = yaml.safe_load(
        (repository / "pipelines/system/new-pipeline-test.yml").read_text(encoding="utf-8")
    )
    stage = next(
        item for item in pipeline["stages"]
        if item.get("stage") == "azure_eastus2euap_n100_debug_resume_37deca"
    )
    resume_job = next(
        item for item in stage["jobs"]
        if item.get("template") == "/jobs/clustermesh-debug-resume.yml"
    )
    parameters = resume_job["parameters"]
    assert parameters["prepared_retirement_role"] == (
        "${{ parameters.scaleDebugPreparedRetirementRole }}"
    )
    assert parameters["prepared_retirement_node"] == (
        "${{ parameters.scaleDebugPreparedRetirementNode }}"
    )
    assert parameters["prepared_retirement_uid"] == (
        "${{ parameters.scaleDebugPreparedRetirementUid }}"
    )
    assert job["jobs"][0]["condition"] == (
        "and(succeeded(), "
        "ne(variables['CLUSTERMESH_PREPARED_RETIREMENT_ONLY'], 'true'), "
        "ne(variables['CLUSTERMESH_PREPARED_RETIREMENT_OBSERVE_ONLY'], 'true'), "
        "ne(variables['CLUSTERMESH_UNREACHABLE_WORKER_RECOVERY_ONLY'], 'true'), "
        "ne(variables['CLUSTERMESH_ARM_REPAIR_ONLY'], 'true'), "
        "ne(variables['CLUSTERMESH_CNI_WORKER_MAINTENANCE_ONLY'], 'true'))"
    )
    retirement_jobs = stage["jobs"][0][
        "${{ if or(parameters.scaleDebugPreparedRetirementObserveOnly, and(parameters.scaleDebugPreparedRetirementOnly, not(parameters.scaleDebugUnreachableWorkerRecoveryOnly))) }}"
    ]
    assert retirement_jobs[0]["template"] == (
        "/jobs/clustermesh-prepared-worker-retirement.yml"
    )
    assert retirement_jobs[0]["parameters"]["observe_only"] == (
        "${{ parameters.scaleDebugPreparedRetirementObserveOnly }}"
    )
    assert "${{ if and(parameters.scaleDebugArmRepairOnly, not(parameters.scaleDebugPreparedRetirementObserveOnly), not(parameters.scaleDebugUnreachableWorkerRecoveryOnly)) }}" in stage["jobs"][1]
    assert "${{ if and(parameters.scaleDebugCniWorkerMaintenanceOnly, not(parameters.scaleDebugPreparedRetirementObserveOnly), not(parameters.scaleDebugUnreachableWorkerRecoveryOnly)) }}" in stage["jobs"][2]


@pytest.mark.parametrize("observe_only", ["true", "false", "invalid"])
def test_pipeline_observation_never_passes_execute(tmp_path, observe_only):
    repository = MODULE_DIR.parents[3]
    template = yaml.safe_load(
        (repository / "steps/topology/clustermesh-scale/reuse/retire-prepared-worker.yml")
        .read_text(encoding="utf-8")
    )
    script = template["steps"][0]["script"]
    script = script.replace("$(Build.ArtifactStagingDirectory)", str(tmp_path / "artifacts"))
    script = script.replace("$(Pipeline.Workspace)/s", str(repository))
    script = script.replace(
        "${{ parameters.tfvars_path }}",
        "scenarios/perf-eval/clustermesh-scale/terraform-inputs/azure-100-mock-shared.tfvars",
    )
    script = script.replace("${{ parameters.expected_subscription_id }}", "test-subscription")
    script = script.replace("${{ parameters.expected_region }}", "eastus2euap")
    captured = tmp_path / "arguments.txt"
    fake_python = tmp_path / "python3"
    fake_python.write_text(
        '#!/usr/bin/env bash\nprintf "%s\\n" "$@" > "$CAPTURED_ARGUMENTS"\n',
        encoding="utf-8",
    )
    fake_python.chmod(0o755)
    result = subprocess.run(
        ["bash", "-c", script],
        env={
            **os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "CAPTURED_ARGUMENTS": str(captured), "OBSERVE_ONLY": observe_only,
            "RETIREMENT_ROLE": "mesh-38", "RETIREMENT_NODE": SOURCE,
            "RETIREMENT_UID": SOURCE_UID, "EXPECTED_CLUSTER_COUNT": "100",
            "RUN_ID": "12345-aabbccdd", "CONFIRM_RESUME": "12345-aabbccdd",
        },
        capture_output=True, text=True, check=False, timeout=10,
    )
    if observe_only == "invalid":
        assert result.returncode == 1 and not captured.exists()
        assert "OBSERVE_ONLY must be true or false" in result.stderr
        return
    assert result.returncode == 0, result.stderr
    arguments = captured.read_text(encoding="utf-8").splitlines()
    assert ("--execute" in arguments) is (observe_only == "false")
    if observe_only == "true":
        assert arguments[-2:] == ["--timeout-seconds", "600"]
        assert "Read-only observation" in result.stdout


@pytest.mark.parametrize("fault", ["none", "workload", "not-exclusive", "arm", "cni", "unreachable"])
def test_observation_job_rejects_conflicting_modes_before_observation(fault):
    repository = MODULE_DIR.parents[3]
    job = yaml.safe_load(
        (repository / "jobs/clustermesh-prepared-worker-retirement.yml")
        .read_text(encoding="utf-8")
    )["jobs"][0]
    environment = {
        **os.environ, "EXPECTED_CLUSTER_COUNT": "100", "ARM_REPAIR_ONLY": "false",
        "CNI_MAINTENANCE_ONLY": "false", "RETIREMENT_ONLY": "true",
        "OBSERVE_ONLY": "true", "RUN_WORKLOAD": "false",
        "UNREACHABLE_RECOVERY_ONLY": "false",
        "RETIREMENT_ROLE": "mesh-38", "RETIREMENT_NODE": SOURCE,
        "RETIREMENT_UID": SOURCE_UID,
    }
    changes = {
        "workload": ("RUN_WORKLOAD", "true"),
        "not-exclusive": ("RETIREMENT_ONLY", "false"),
        "arm": ("ARM_REPAIR_ONLY", "true"),
        "cni": ("CNI_MAINTENANCE_ONLY", "true"),
        "unreachable": ("UNREACHABLE_RECOVERY_ONLY", "True"),
    }
    if fault in changes:
        key, value = changes[fault]
        environment[key] = value
    result = subprocess.run(
        ["bash", "-c", job["steps"][0]["script"]], env=environment,
        capture_output=True, text=True, check=False, timeout=10,
    )
    assert result.returncode == (0 if fault == "none" else 1), result.stderr
    assert [step.get("template") for step in job["steps"] if "template" in step] == [
        "/steps/setup-tests.yml",
        "/steps/topology/clustermesh-scale/reuse/retire-prepared-worker.yml",
    ]
    assert job["steps"][-1]["parameters"]["observe_only"] == "${{ parameters.observe_only }}"
    assert job["${{ if eq(parameters.observe_only, true) }}"]["timeoutInMinutes"] == 45
    assert job["variables"]["${{ if eq(parameters.observe_only, true) }}"]["SKIP_RESOURCE_MANAGEMENT"] == "true"


@pytest.mark.parametrize("job_name", [
    "clustermesh-prepared-worker-retirement.yml",
    "clustermesh-arm-repair.yml",
    "clustermesh-cni-worker-maintenance.yml",
])
def test_maintenance_bootstrap_resolves_vendored_fleet_wheel(job_name):
    repository = MODULE_DIR.parents[3]
    maintenance = yaml.safe_load(
        (repository / "jobs" / job_name)
        .read_text(encoding="utf-8")
    )
    setup = yaml.safe_load(
        (repository / "steps/setup-tests.yml").read_text(encoding="utf-8")
    )
    installer = next(
        step for step in setup["steps"]
        if step.get("displayName") == "Install Fleet preview CLI (clustermesh scenarios)"
    )
    wheel = re.search(r'^whl="([^"]+)"$', installer["script"], re.MULTILINE)
    assert wheel is not None
    scenario = maintenance["jobs"][0]["variables"]["SCENARIO_NAME"]
    resolved = wheel.group(1).replace("$(Pipeline.Workspace)/s", str(repository))
    resolved = resolved.replace("$(SCENARIO_NAME)", scenario)
    assert Path(resolved).is_file()
