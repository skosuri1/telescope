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
        "ne(variables['CLUSTERMESH_ARM_REPAIR_ONLY'], 'true'))"
    )
    retirement_jobs = stage["jobs"][0][
        "${{ if eq(parameters.scaleDebugPreparedRetirementOnly, true) }}"
    ]
    assert retirement_jobs[0]["template"] == (
        "/jobs/clustermesh-prepared-worker-retirement.yml"
    )


@pytest.mark.parametrize("job_name", [
    "clustermesh-prepared-worker-retirement.yml",
    "clustermesh-arm-repair.yml",
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
