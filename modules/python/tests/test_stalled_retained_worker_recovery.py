"""Offline normal-restart safety tests; no Azure or Kubernetes clients are invoked."""

# pylint: disable=protected-access,too-many-lines

from __future__ import annotations

import copy
import importlib.util
import json
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import jmespath
import pytest


DIRECTORY = Path(__file__).resolve().parents[1] / "clusterloader2" / "clustermesh-scale"
SPEC = importlib.util.spec_from_file_location("stalled_retained_worker_recovery",
                                             DIRECTORY / "stalled_retained_worker_recovery.py")
recovery = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = recovery
sys.path.insert(0, str(DIRECTORY))
try:
    SPEC.loader.exec_module(recovery)
finally:
    sys.path.pop(0)
base = recovery.base


def uid(value):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, value))


def now():
    return datetime.now(timezone.utc).isoformat()


def metadata(name, namespace="", row_uid=None):
    result = {"name": name, "uid": row_uid or uid(f"{namespace}/{name}"), "resourceVersion": "1", "labels": {}}
    if namespace:
        result["namespace"] = namespace
    return result


def ref(kind, name, row_uid):
    return {"kind": kind, "name": name, "uid": row_uid, "controller": True}


def status(ready=True):
    return {"phase": "Running" if ready else "Pending", "podIP": "10.1.0.5" if ready else "",
            "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
            "containerStatuses": [{"name": "container", "ready": ready, "state": {"running": {}} if ready else {
                "waiting": {"reason": "ContainerCreating"}}}]}


def pod(name, namespace, node_name, owner, *, ready=True, terminating=False):
    meta = metadata(name, namespace)
    meta["ownerReferences"] = [owner]
    if terminating:
        meta["deletionTimestamp"] = now()
    pod_status = status(ready)
    if terminating:
        pod_status["phase"] = "Running"
        pod_status["containerStatuses"][0]["state"] = {"running": {}}
    return {"metadata": meta, "spec": {"nodeName": node_name, "containers": [{"name": "container", "image": "pinned"}],
                                     "volumes": []}, "status": pod_status}


class Cloud:
    """Stateful raw provider documents, projected with the actual CLI query."""

    max_call_timeout = 45

    def __init__(self, args):
        self.args = args
        self.writes, self.commands, self.restart_calls = [], [], []
        self.journal = None
        self.hook = None
        self.restart_error = None
        self.journal_error = False
        self.recover_on_restart = True
        self.after_restart = None
        self.old = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        scope = f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.RESOURCE_GROUP}"
        self.cluster_id = f"{scope}/providers/Microsoft.ContainerService/managedClusters/{base.CLUSTER}"
        self.group = {"id": scope, "location": base.REGION, "tags": {
            "clustermesh_debug_preserved": "true", "run_id": base.RESOURCE_GROUP,
            "scenario": "perf-eval-clustermesh-scale", "clustermesh_debug_expected_clusters": "100",
            "clustermesh_debug_tfvars_sha256": args.expected_tfvars_sha,
            "deletion_due_time": (datetime.now(timezone.utc) + timedelta(days=2)).isoformat(),
        }}
        self.clusters, self.members, self.identities = [], [], []
        for index in range(1, 101):
            role, name = f"mesh-{index}", f"clustermesh-{index}"
            cluster_id = f"{scope}/providers/Microsoft.ContainerService/managedClusters/{name}"
            self.clusters.append({
                "id": cluster_id, "name": name, "location": base.REGION,
                "nodeResourceGroup": f"MC_{base.RESOURCE_GROUP}_{name}_{base.REGION}",
                "tags": {"role": role, "run_id": base.RESOURCE_GROUP, "deletion_due_time": "2026-09-09T00:00:00Z"},
                "provisioningState": "Succeeded", "powerState": {"code": "Running"},
            })
            fleet = f"{scope}/providers/Microsoft.ContainerService/fleets/clustermesh-flt"
            mesh_status = ({"state": "Failed", "error": {"code": "ConnectivityTimeout"}}
                           if index == 96 else {"state": "Connected"})
            self.members.append({
                "id": f"{fleet}/members/{role}", "name": role, "clusterResourceId": cluster_id,
                "provisioningState": "Succeeded", "labels": {"mesh": "true"},
                "meshProperties": {"ciliumProperties": {"name": f"assigned-{index}", "id": index},
                                   "clusterMeshProfileResourceId": f"{fleet}/clusterMeshProfiles/clustermesh-cmp",
                                   "status": mesh_status},
            })
            self.identities.append({"role": role, "cluster_name": f"assigned-{index}", "cluster_id": index})
        self.node_group = {
            "id": f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.NODE_GROUP.upper()}",
            "location": base.REGION, "managedBy": self.cluster_id.upper(),
            "tags": {"deletion_due_time": self.group["tags"]["deletion_due_time"]},
        }
        self.nodes = {}
        self.instances, self.views, self.nncs = [], {}, []
        for instance, name in enumerate((recovery.SOURCE, recovery.TARGET)):
            provider = (f"azure:///subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.NODE_GROUP}"
                        f"/providers/Microsoft.Compute/virtualMachineScaleSets/{base.DEFAULT_VMSS}/virtualMachines/{instance}")
            node = {"metadata": metadata(name, row_uid=base.REAL_UIDS[name]),
                    "spec": {"providerID": provider},
                    "status": {"nodeInfo": {"bootID": recovery.BOOTS[name]},
                               "conditions": [{"type": "Ready", "status": "True" if instance == 0 else "Unknown",
                                               "lastHeartbeatTime": now() if instance == 0 else self.old,
                                               "lastTransitionTime": self.old}]}}
            node["metadata"]["labels"] = {"agentpool": "default", "kubernetes.azure.com/agentpool": "default",
                                          "kubernetes.azure.com/cluster": base.NODE_GROUP}
            if instance == 1:
                node["spec"]["taints"] = [{"key": "node.kubernetes.io/unreachable", "effect": "NoExecute"}]
                node["status"]["conditions"].append({
                    "type": "VMEventScheduled", "status": "True",
                    "message": f"Freeze Started. memory-preserving Live Migration. EventId: {recovery.FREEZE_EVENT}",
                })
            self.nodes[name] = node
            self.instances.append({"id": provider.removeprefix("azure://"), "instanceId": str(instance),
                                   "osProfile": {"computerName": name}, "provisioningState": "Succeeded",
                                   "latestModelApplied": True, "vmId": recovery.VM_IDS[name]})
            self.views[str(instance)] = {
                "statuses": [{"code": "ProvisioningState/succeeded"}, {"code": "PowerState/running"}],
                "vmAgent": {"statuses": [{"code": "ProvisioningState/succeeded" if instance == 0 else "ProvisioningState/Unavailable",
                                         "displayStatus": "Ready" if instance == 0 else "Not Ready",
                                         "message": "Guest Agent is running" if instance == 0 else "VM Agent is unresponsive.",
                                         "time": now()}]},
                "extensions": [{"name": "vmssCSE", "statuses": [{"code": "ProvisioningState/succeeded"}] if instance == 0 else None}],
            }
            self.nncs.append({
                "metadata": {**metadata(name, "kube-system"), "ownerReferences": [ref("Node", name, base.REAL_UIDS[name])]},
                "status": {"assignedIPCount": 64, "networkContainers": [
                    {"id": uid(f"nc/{name}"), "version": 10, "ipAssignments": []}]},
            })
        for index in range(100):
            name = f"kwok-node-{index}"
            self.nodes[name] = {
                "metadata": {**metadata(name), "labels": {"type": "kwok"}},
                "spec": {"providerID": f"kwok://{name}", "podCIDR": f"100.96.{index}.0/24",
                         "taints": [{"key": "kwok.x-k8s.io/node", "effect": "NoSchedule", "value": "fake"},
                                    {"key": "node.kubernetes.io/unreachable", "effect": "NoExecute"}]},
                "status": {"conditions": [{"type": "Ready", "status": "Unknown"}]},
            }
        self.controllers = [{
            "kind": "StatefulSet", "metadata": metadata("kwok-node", "mock-clustermesh"),
            "spec": {"replicas": 100, "template": {"spec": {"containers": [{"name": "mock", "image": "pinned"}]}}},
        }]
        for name in ("cilium", "azure-cns"):
            self.controllers.append({
                "kind": "DaemonSet", "metadata": metadata(name, "kube-system"),
                "spec": {"template": {"metadata": {"annotations": {"kubernetes.azure.com/azure-cns-configmap-checksum": "old"}},
                                      "spec": {"containers": [{"name": name, "image": "pinned"}]}}},
            })
        self.controllers.extend([
            {"kind": "Deployment", "metadata": metadata("kwok-controller", "kube-system"),
             "spec": {"replicas": 1, "template": {"spec": {"containers": [{"image": "kwok"}]}}}},
            {"kind": "ReplicaSet", "metadata": {
                **metadata("kwok-controller-rs", "kube-system"),
                "ownerReferences": [ref("Deployment", "kwok-controller", uid("kube-system/kwok-controller"))]},
             "spec": {"replicas": 1, "template": {"spec": {"containers": [{"image": "kwok"}]}}}},
        ])
        self.pods = []
        for index in range(100):
            name = f"kwok-node-{index}"
            node = recovery.SOURCE if index < 44 else recovery.TARGET
            row = pod(name, "mock-clustermesh", node,
                      ref("StatefulSet", "kwok-node", uid("mock-clustermesh/kwok-node")),
                      ready=index < 38, terminating=index >= 44)
            row["metadata"]["labels"] = {"app": "mock-cilium-agent", "mock-clustermesh/agent-controller": "kwok-node"}
            self.pods.append(row)
        for name in (recovery.SOURCE, recovery.TARGET):
            for daemon in ("cilium", "azure-cns"):
                self.pods.append(pod(f"{daemon}-{name}", "kube-system", name,
                                     ref("DaemonSet", daemon, uid(f"kube-system/{daemon}")),
                                     ready=name == recovery.SOURCE))
        self.pods.append(pod("kwok-controller-old", "kube-system", recovery.TARGET,
                             ref("ReplicaSet", "kwok-controller-rs", uid("kube-system/kwok-controller-rs")),
                             ready=False, terminating=True))
        self.pdbs = [{"metadata": metadata("kwok-pdb", "kube-system"),
                      "spec": {"minAvailable": 1, "selector": {"matchLabels": {"app": "kwok"}}}}]
        self.pools, self.scales = [], []
        for name, vmss, count in (("default", base.DEFAULT_VMSS, 2), ("prompool", base.PROM_VMSS, 0)):
            self.pools.append({
                "name": name, "id": f"{self.cluster_id}/agentPools/{name}", "count": count,
                "mode": "System" if name == "default" else "User", "vmSize": "Standard_D8_v3",
                "enableAutoScaling": False, "provisioningState": "Succeeded", "powerState": {"code": "Running"},
                "nodeImageVersion": "AKSUbuntu-pinned", "vnetSubnetId": f"/subscriptions/{base.SUBSCRIPTION}/subnet-node",
                "podSubnetId": f"/subscriptions/{base.SUBSCRIPTION}/subnet-pod",
            })
            self.scales.append({
                "name": vmss, "id": (f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.NODE_GROUP}"
                                     f"/providers/Microsoft.Compute/virtualMachineScaleSets/{vmss}"),
                "location": base.REGION, "tags": {"aks-managed-poolName": name}, "orchestrationMode": "Uniform",
                "sku": {"name": "Standard_D8_v3", "capacity": count}, "provisioningState": "Succeeded",
            })
        self.operation = {"name": "original-cluster-operation", "status": "Succeeded", "operationType": "PutManagedCluster",
                          "startTime": self.old, "endTime": self.old}

    def snapshot(self):
        return {"nodes": {"items": list(self.nodes.values())}, "pods": {"items": self.pods},
                "controllers": {"items": self.controllers}, "pdbs": {"items": self.pdbs},
                "nnc": {"items": self.nncs}}

    def write_source(self):
        root = Path(self.args.source_state_directory)
        root.mkdir()
        prior = {
            "plan_sha256": recovery.PLAN_SHA, "authoritative_identities": self.identities,
            "replacement": {"delete": {"attempted": True, "accepted": True, "ambiguous": False},
                            "native_removal": {"old_node_pods_nnc_absent": True, "pool_count": 0, "vmss_capacity": 0},
                            "marker": {"vm_id": base.FAILED_PROM_VM_ID}},
            "original_model_pins": {"defaults": {name: {"vm_id": value} for name, value in recovery.VM_IDS.items()}},
            "controller_pins": base.frozen_controllers({"controllers": {"items": self.controllers}}),
        }
        payloads = {
            **{f"current-{key}.json": payload for key, payload in self.snapshot().items()},
            "default-instances.json": jmespath.search(base.VM_QUERY, self.instances),
            "preserved-group.json": self.group, "cluster.json": self.clusters[95],
            "quota-observation.json": {"observation_only": True, "mutation_started": False, "worker_state_collected": True,
                                       "source_native_build": 79894, "observation_complete": False},
            "prior-native-action.json": prior, "default-0-instance-view.json": self.views["0"],
            "default-1-instance-view.json": self.views["1"], "pool-configuration.json": self.pools,
            "vmsses.json": self.scales,
        }
        for name, value in payloads.items():
            (root / name).write_text(json.dumps(value), encoding="utf-8")
        (root / "default-1-resource-health-read.log").write_text(
            'ERROR: Unprocessable Entity({"code":"UnsupportedResourceType","message":"Resource type not supported."})',
            encoding="utf-8",
        )

    @staticmethod
    def value(command, option):
        return command[command.index(option) + 1]

    def receipt(self):
        return json.loads(Path(self.args.summary_file).read_text(encoding="utf-8"))

    def recover(self, *, recreate=True):
        target = self.nodes[recovery.TARGET]
        target["status"]["nodeInfo"]["bootID"] = uid("recovered-boot")
        target["status"]["conditions"][0].update(status="True", lastHeartbeatTime=now())
        target["spec"].pop("taints", None)
        self.views["1"]["vmAgent"]["statuses"] = [{
            "code": "ProvisioningState/succeeded", "displayStatus": "Ready", "message": "Guest Agent is running", "time": now(),
        }]
        self.views["1"]["extensions"][0]["statuses"] = [{"code": "ProvisioningState/succeeded"}]
        for row in self.pods:
            if row["spec"].get("nodeName") != recovery.TARGET:
                continue
            if row.get("status", {}).get("phase") in ("Succeeded", "Failed"):
                continue
            if row["metadata"].get("deletionTimestamp"):
                if not recreate:
                    continue
                row["metadata"]["uid"] = uid("replacement/" + row["metadata"]["uid"])
                row["metadata"].pop("deletionTimestamp")
            if row["metadata"]["name"] == "kwok-controller-old":
                row["metadata"]["name"] = "kwok-controller-new"
                row["spec"]["nodeName"] = recovery.SOURCE
                row["status"] = status(False)
            else:
                row["status"] = status(True)

    def run(self, command, timeout_seconds):
        assert 0 < timeout_seconds <= self.max_call_timeout
        self.commands.append(command)
        if self.hook:
            self.hook(command)
        if command[0] == "az":
            if command[1:3] != ["account", "show"]:
                assert self.value(command, "--subscription") == base.SUBSCRIPTION
            result = self.azure(command)
            if "--query" in command:
                result = jmespath.search(self.value(command, "--query"), result)
        else:
            assert self.value(command, "--context") == base.CLUSTER
            assert self.value(command, "--kubeconfig") == self.args.kubeconfig
            result = self.kube(command)
        return result if isinstance(result, str) else json.dumps(copy.deepcopy(result))

    def azure(self, command):
        if command[1:3] == ["account", "show"]:
            return {"id": base.SUBSCRIPTION}
        if command[1:3] == ["group", "show"]:
            return self.group if self.value(command, "--name") == base.RESOURCE_GROUP else self.node_group
        if command[1:3] == ["aks", "list"]:
            return self.clusters
        if command[1:4] == ["fleet", "member", "list"]:
            return self.members
        if command[1:4] == ["aks", "operation", "show-latest"]:
            return self.operation
        if command[1:4] == ["aks", "nodepool", "list"]:
            return self.pools
        if command[1:3] == ["vmss", "list"]:
            return self.scales
        if command[1:3] == ["vmss", "list-instances"]:
            return self.instances if self.value(command, "--name") == base.DEFAULT_VMSS else []
        if command[1:3] == ["vmss", "get-instance-view"]:
            assert self.value(command, "--name") == base.DEFAULT_VMSS
            return self.views[self.value(command, "--instance-id")]
        assert command == recovery.Recovery.restart_command(), f"Forbidden Azure action: {command}"
        self.writes.append(command)
        self.restart_calls.append(command)
        receipt = self.receipt()["restart"]
        assert receipt["attempted"] and receipt["accepted"] is None and receipt["ambiguous"]
        journal = json.loads(self.journal["data"]["receipt"])
        assert journal["command"] == command and journal["attempted"] and journal["accepted"] is None
        if self.restart_error:
            raise recovery.workers.ReconcileError(self.restart_error)
        if self.recover_on_restart:
            self.recover()
        if self.after_restart:
            self.after_restart()
        return ""

    def kube(self, command):
        if "create" in command:
            self.writes.append(command)
            assert self.value(command, "create") == "configmap" and recovery.JOURNAL in command
            assert self.journal is None
            self.journal = {"apiVersion": "v1", "kind": "ConfigMap",
                            "metadata": metadata(recovery.JOURNAL, "kube-system"), "data": dict(
                entry.removeprefix("--from-literal=").split("=", 1)
                for entry in command if entry.startswith("--from-literal="))}
            if self.journal_error:
                raise recovery.workers.ReconcileError("ambiguous journal create")
            return self.journal
        if "patch" in command:
            self.writes.append(command)
            assert self.value(command, "patch") == "configmap" and recovery.JOURNAL in command
            operations = json.loads(self.value(command, "-p"))
            assert operations[0] == {"op": "test", "path": "/metadata/uid", "value": self.journal["metadata"]["uid"]}
            assert operations[1]["value"] == self.journal["metadata"]["resourceVersion"]
            assert operations[2] == {"op": "test", "path": "/data", "value": self.journal["data"]}
            self.journal["data"] = operations[3]["value"]
            self.journal["metadata"]["resourceVersion"] = str(int(self.journal["metadata"]["resourceVersion"]) + 1)
            return self.journal
        assert "get" in command
        resource = self.value(command, "get")
        if resource == "--raw=/readyz":
            return "ok"
        if resource == "configmaps":
            return {"apiVersion": "v1", "kind": "ConfigMapList", "metadata": {},
                    "items": [self.journal] if self.journal else []}
        if resource == "configmap":
            assert recovery.JOURNAL in command
            return self.journal
        return {
            "nodes": {"items": list(self.nodes.values())}, "pods": {"items": self.pods},
            "deployments,replicasets,daemonsets,statefulsets": {"items": self.controllers},
            "pdb": {"items": self.pdbs}, "nodenetworkconfigs": {"items": self.nncs},
        }[resource]


@pytest.fixture(name="environment")
def setup_environment(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    args = SimpleNamespace(
        resource_group=base.RESOURCE_GROUP, confirm_resource_group=base.RESOURCE_GROUP,
        expected_subscription=base.SUBSCRIPTION, expected_region=base.REGION, expected_tfvars_sha="a" * 64,
        source_state_directory="observation", summary_file="recovery.json", kubeconfig="private-config",
        context=base.CLUSTER, timeout_seconds=1200, execute=False,
    )
    cloud = Cloud(args)
    cloud.write_source()
    return args, cloud


def run(environment, execute=False):
    args, cloud = environment
    args.execute = execute
    summary = {}
    recovery.execute_recovery(args, summary, runner=cloud.run)
    assert summary == cloud.receipt()
    return summary


def test_plan_has_zero_writes_and_does_not_claim_stale_71_healthy_or_full_kwok(environment):
    _, cloud = environment
    summary = run(environment)
    assert summary["plan_valid"] and not summary["mutation_started"] and not summary["host_recovered"]
    assert summary["mock_readiness"] == {"present": 100, "ready": 38, "pending": 6, "terminating": 56}
    assert summary["current_kwok_ready"] == 0 and len(summary["preserved_kwok_node_uids"]) == 100
    assert not cloud.writes and not summary["workloads_ready"] and not summary["full_suite_qualified"]


def test_exact_one_instance_restart_new_boot_and_recreated_56_without_workload_claim(environment):
    _, cloud = environment
    source_before = copy.deepcopy(cloud.nodes[recovery.SOURCE])
    summary = run(environment, True)
    assert cloud.restart_calls == [recovery.Recovery.restart_command()]
    assert "--instance-ids" in cloud.restart_calls[0] and cloud.value(cloud.restart_calls[0], "--instance-ids") == "1"
    assert summary["success"] and summary["host_recovered"] and summary["restart"]["host_proven"]
    assert summary["restart"]["accepted"] is True and not summary["restart"]["ambiguous"]
    assert cloud.nodes[recovery.SOURCE] == source_before
    assert summary["effective_identity"]["boot_id"] != summary["original_identity"]["boot_id"]
    assert len(summary["authorized_controller_replacements"]) == 56
    assert summary["mock_readiness"]["ready"] == 94 and summary["mock_readiness"]["pending"] == 6
    assert summary["current_kwok_ready"] == 0 and not summary["workloads_ready"]
    assert cloud.journal and not summary["full_suite_qualified"]
    assert all(command[0] == "az" or "configmap" in command for command in cloud.writes)


def test_already_truly_healthy_adopts_without_any_write(environment):
    _, cloud = environment
    cloud.recover()
    summary = run(environment, True)
    assert summary["host_recovered"] and summary["success"] and not cloud.writes and not cloud.restart_calls
    assert not summary["restart"]["attempted"] and len(summary["authorized_controller_replacements"]) == 56


@pytest.mark.parametrize("fault", [
    "scope", "lease", "node-group-lease", "fleet-foreign", "fleet-identity", "target-node", "target-vm", "target-boot",
    "source-node", "source-vm", "source-boot", "source-pod", "source-pending-uid",
    "kwok-uid", "kwok-spec", "pdb", "controller", "pvc", "ephemeral", "foreign-controller", "ready-target",
])
def test_scope_identity_ownership_pvc_pdb_and_healthy_source_fail_before_writes(environment, fault):
    _, cloud = environment
    target_pod = next(row for row in cloud.pods if row["metadata"]["name"] == "kwok-node-44")
    if fault == "scope":
        cloud.node_group["managedBy"] += "-other"
    elif fault == "lease":
        cloud.group["tags"]["deletion_due_time"] = now()
    elif fault == "node-group-lease":
        cloud.node_group["tags"]["deletion_due_time"] = now()
    elif fault == "fleet-foreign":
        cloud.members[0]["meshProperties"]["status"]["state"] = "Failed"
    elif fault == "fleet-identity":
        cloud.members[0]["meshProperties"]["ciliumProperties"]["name"] = "different-cluster"
    elif fault in ("target-node", "source-node"):
        cloud.nodes[recovery.TARGET if fault == "target-node" else recovery.SOURCE]["metadata"]["uid"] = uid("different")
    elif fault in ("target-vm", "source-vm"):
        cloud.instances[1 if fault == "target-vm" else 0]["vmId"] = uid("different")
    elif fault in ("target-boot", "source-boot"):
        cloud.nodes[recovery.TARGET if fault == "target-boot" else recovery.SOURCE]["status"]["nodeInfo"]["bootID"] = uid("different")
    elif fault in ("source-pod", "source-pending-uid"):
        name = "kwok-node-0" if fault == "source-pod" else "kwok-node-40"
        next(row for row in cloud.pods if row["metadata"]["name"] == name)["metadata"]["uid"] = uid("changed-pod")
    elif fault == "kwok-uid":
        cloud.nodes["kwok-node-1"]["metadata"]["uid"] = uid("changed-kwok")
    elif fault == "kwok-spec":
        cloud.nodes["kwok-node-1"]["spec"]["podCIDR"] = "1.2.3.0/24"
    elif fault == "pdb":
        cloud.pdbs[0]["spec"]["minAvailable"] = 0
    elif fault == "controller":
        cloud.controllers[1]["spec"]["template"]["spec"]["containers"][0]["image"] = "new-functional-image"
    elif fault in ("pvc", "ephemeral"):
        target_pod["spec"]["volumes"] = [{"name": "claim", "persistentVolumeClaim" if fault == "pvc" else "ephemeral": {}}]
    elif fault == "foreign-controller":
        target_pod["metadata"]["ownerReferences"][0]["uid"] = uid("foreign")
    else:
        target_pod["status"] = status(True)
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, True)
    assert not cloud.writes


def test_managed_checksum_annotation_only_change_is_not_functional_drift(environment):
    _, cloud = environment
    cloud.controllers[1]["spec"]["template"]["metadata"]["annotations"]["kubernetes.azure.com/azure-cns-configmap-checksum"] = "new"
    summary = run(environment)
    assert summary["plan_valid"] and not cloud.writes


@pytest.mark.parametrize("fault", ["remove", "add", "different-effect", "other-toleration", "different-uid"])
def test_only_exact_managed_security_readiness_toleration_can_vary(environment, fault):
    args, cloud = environment
    daemon = copy.deepcopy(cloud.controllers[1])
    daemon["metadata"] = metadata("azuresecuritylinuxagent", "kube-system", recovery.SECURITY_DAEMONSET_UID)
    original = [{"key": "kwok.x-k8s.io/node", "operator": "Equal", "value": "fake", "effect": "NoSchedule"}]
    daemon["spec"]["template"]["spec"]["tolerations"] = copy.deepcopy(original)
    if fault != "add":
        daemon["spec"]["template"]["spec"]["tolerations"].append(copy.deepcopy(recovery.READINESS_TOLERATION))
    cloud.controllers.append(daemon)
    (Path(args.source_state_directory) / "current-controllers.json").write_text(json.dumps({"items": cloud.controllers}))
    tolerations = daemon["spec"]["template"]["spec"]["tolerations"]
    if fault == "add":
        tolerations.append(copy.deepcopy(recovery.READINESS_TOLERATION))
    elif fault == "remove":
        tolerations.remove(recovery.READINESS_TOLERATION)
    elif fault == "different-effect":
        tolerations[-1]["effect"] = "NoExecute"
    elif fault == "other-toleration":
        tolerations.append({"key": "unapproved", "operator": "Exists"})
    else:
        daemon["metadata"]["uid"] = uid("different-security-controller")
    if fault in ("remove", "add"):
        assert run(environment)["plan_valid"]
    else:
        with pytest.raises(recovery.workers.ReconcileError):
            run(environment, True)
    assert not cloud.writes


def pre_submit_reservation(environment):
    args, cloud = environment
    prior = copy.deepcopy(run(environment))
    reserved = datetime.now(timezone.utc) - timedelta(minutes=1)
    prior.update(
        execute=True, mutation_started=True, status="failed-closed", success=False, host_recovered=False,
        error="ReconcileError: Captured functional controller specs/UIDs changed",
        started_at=(reserved - timedelta(seconds=30)).isoformat(),
        finished_at=(reserved + timedelta(seconds=30)).isoformat(),
        restart={
            "attempted": True, "accepted": None, "ambiguous": True, "automatic_retry_allowed": False,
            "command": recovery.Recovery.restart_command(), "previous_boot_id": recovery.BOOTS[recovery.TARGET],
            "requested_at": reserved.isoformat(),
        },
        journal={
            "name": recovery.JOURNAL, "namespace": "kube-system", "uid": recovery.PRE_SUBMIT_JOURNAL_UID,
            "create_attempted": True, "accepted": True, "ambiguous": False, "retained": True,
        },
    )
    path = Path("prior-reservation.json")
    path.write_text(json.dumps(prior), encoding="utf-8")
    args.summary_file, args.resume_checkpoint, args.resume_build_id = "continued.json", str(path), recovery.PRE_SUBMIT_BUILD
    cloud.journal = {
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": metadata(recovery.JOURNAL, "kube-system", recovery.PRE_SUBMIT_JOURNAL_UID),
        "data": {
            "owner": recovery.OWNER, "token": "a" * 32,
            "target_node_uid": base.REAL_UIDS[recovery.TARGET], "target_vm_id": recovery.VM_IDS[recovery.TARGET],
            "source_state_sha256": prior["source_state_sha256"],
            "receipt": json.dumps(prior["restart"], sort_keys=True, separators=(",", ":")),
        },
    }
    return prior


@pytest.mark.parametrize("execute", [False, True])
def test_proven_pre_submit_reservation_continues_same_journal_without_recreating_it(environment, execute):
    _, cloud = environment
    prior = pre_submit_reservation(environment)
    original_uid, original_token = cloud.journal["metadata"]["uid"], cloud.journal["data"]["token"]
    summary = run(environment, execute)
    assert summary["plan_valid"] and summary["continuation"]["source_build"] == 79945
    assert cloud.journal["metadata"]["uid"] == original_uid and cloud.journal["data"]["token"] == original_token
    assert not any("create" in command for command in cloud.writes)
    if execute:
        assert len(cloud.restart_calls) == 1 and summary["restart"]["submission_started"]
        assert summary["host_recovered"] and not summary["workloads_ready"]
        assert json.loads(cloud.journal["data"]["prior_unsubmitted_receipt"]) == prior["restart"]
    else:
        assert not cloud.writes and not cloud.restart_calls
        assert "prior_unsubmitted_receipt" not in cloud.journal["data"]


@pytest.mark.parametrize("fault", [
    "accepted", "submission-marker", "unknown-error", "source-hashes", "target-identity", "wrong-build",
    "journal-uid", "journal-receipt", "journal-source", "journal-token", "already-continued",
])
def test_unproved_or_changed_reservation_never_restarts(environment, fault):
    args, cloud = environment
    prior = pre_submit_reservation(environment)
    if fault == "accepted":
        prior["restart"]["accepted"] = True
    elif fault == "submission-marker":
        prior["restart"]["submission_started"] = True
    elif fault == "unknown-error":
        prior["error"] = "POST timed out"
    elif fault == "source-hashes":
        prior["source_hashes"] = {}
    elif fault == "target-identity":
        prior["original_identity"]["node_uid"] = uid("foreign-node")
    elif fault == "wrong-build":
        args.resume_build_id = 79946
    elif fault == "journal-uid":
        cloud.journal["metadata"]["uid"] = uid("replacement-journal")
    elif fault == "journal-receipt":
        cloud.journal["data"]["receipt"] = "{}"
    elif fault == "journal-source":
        cloud.journal["data"]["source_state_sha256"] = "f" * 64
    elif fault == "journal-token":
        cloud.journal["data"]["token"] = "invalid"
    else:
        cloud.journal["data"]["prior_build_id"] = "79945"
    Path(args.resume_checkpoint).write_text(json.dumps(prior), encoding="utf-8")
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, True)
    assert not cloud.writes and not cloud.restart_calls


@pytest.mark.parametrize("fault", ["journal-existing", "journal-ambiguous", "conflict", "timeout"])
def test_existing_or_ambiguous_operations_never_replay(environment, fault):
    _, cloud = environment
    if fault == "journal-existing":
        cloud.journal = {"metadata": metadata(recovery.JOURNAL, "kube-system"), "data": {"owner": recovery.OWNER}}
    elif fault == "journal-ambiguous":
        cloud.journal_error = True
    else:
        cloud.restart_error = "OperationNotAllowed: active migration conflict" if fault == "conflict" else "POST timeout"
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, True)
    assert len(cloud.restart_calls) == (0 if fault.startswith("journal") else 1)
    assert cloud.journal is not None and not cloud.receipt()["success"]
    if cloud.restart_calls:
        assert cloud.receipt()["restart"]["accepted"] is None and cloud.receipt()["restart"]["ambiguous"]


@pytest.mark.parametrize("fault", ["running-only", "same-boot", "extensions-missing", "guest-missing"])
def test_accepted_observation_waits_bounded_without_running_only_health(environment, monkeypatch, fault):
    _, cloud = environment
    clock = [recovery.time.monotonic()]
    monkeypatch.setattr(recovery.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(recovery.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + max(seconds, 1200)))
    if fault in ("running-only", "same-boot", "guest-missing"):
        cloud.recover_on_restart = False

    def after():
        if fault == "same-boot":
            cloud.recover(recreate=False)
            cloud.nodes[recovery.TARGET]["status"]["nodeInfo"]["bootID"] = recovery.BOOTS[recovery.TARGET]
        elif fault == "extensions-missing":
            cloud.views["1"]["extensions"][0]["statuses"] = None
        elif fault == "guest-missing":
            cloud.views["1"]["vmAgent"] = {}
            cloud.nodes[recovery.TARGET]["status"]["nodeInfo"]["bootID"] = uid("new-boot-before-guest")
            cloud.nodes[recovery.TARGET]["status"]["conditions"][0].update(status="True", lastHeartbeatTime=now())
    cloud.after_restart = after
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, True)
    assert len(cloud.restart_calls) == 1 and cloud.receipt()["restart"]["accepted"]
    assert not cloud.receipt()["host_recovered"]


def test_target_replacement_before_liveness_is_forbidden(environment):
    _, cloud = environment
    next(row for row in cloud.pods if row["metadata"]["name"] == "kwok-node-44")["metadata"]["uid"] = uid("premature-replacement")
    with pytest.raises(recovery.workers.ReconcileError, match="before fresh host"):
        run(environment, True)
    assert not cloud.writes


def test_input_hash_change_after_journalling_prevents_restart(environment):
    args, cloud = environment
    changed = []

    def hook(command):
        if "patch" in command and not changed:
            changed.append(True)
            path = Path(args.source_state_directory) / "quota-observation.json"
            path.write_text(path.read_text() + "\n")
    cloud.hook = hook
    with pytest.raises(recovery.workers.ReconcileError, match="hashes changed"):
        run(environment, True)
    assert not cloud.restart_calls


def test_source_uid_change_immediately_before_restart_is_caught(environment):
    _, cloud = environment

    def hook(command):
        if "patch" in command:
            cloud.nodes[recovery.SOURCE]["metadata"]["uid"] = uid("late-source-change")
    cloud.hook = hook
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, True)
    assert not cloud.restart_calls


def completed_certificate_hook(environment):
    args, cloud = environment
    hook = pod(recovery.COMPLETED_CERT_POD, "kube-system", recovery.TARGET,
               ref("Job", "gone-job", uid("gone")), ready=False)
    hook["metadata"]["uid"] = recovery.COMPLETED_CERT_UID
    hook["metadata"]["ownerReferences"] = []
    hook["metadata"]["labels"] = {"kubernetes.azure.com/managedby": "aks", "k8s-app": "hubble-generate-certs"}
    hook["spec"].update(restartPolicy="OnFailure", containers=[
        {"name": "certgen", "image": "mcr.microsoft.com/containernetworking/cilium/certgen:v0.3.2"},
    ])
    hook["status"] = {"phase": "Succeeded", "containerStatuses": [
        {"name": "certgen", "ready": False, "started": False, "state": {"terminated": {
            "exitCode": 0, "reason": "Completed",
            "finishedAt": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
        }}}]}
    cloud.pods.append(hook)
    path = Path(args.source_state_directory) / "current-pods.json"
    path.write_text(json.dumps({"items": cloud.pods}))
    return hook


def test_exact_captured_completed_hook_does_not_block_unresponsive_host_recovery(environment):
    _, cloud = environment
    hook = completed_certificate_hook(environment)
    original = copy.deepcopy(hook)
    summary = run(environment, True)
    assert summary["host_recovered"] and len(cloud.restart_calls) == 1
    assert hook == original
    assert summary["completed_certificate_hook"]["uid"] == recovery.COMPLETED_CERT_UID
    assert summary["completed_certificate_hook"]["running_workload"] is False
    assert not summary["workloads_ready"]


@pytest.mark.parametrize("fault", [
    "uid", "running", "waiting", "exit-code", "finished-at", "image", "always-restart",
    "labels", "extra-container", "ephemeral-container", "pvc",
])
def test_terminal_hook_classification_never_allows_changed_or_live_unowned_pods(environment, fault):
    _, cloud = environment
    hook = completed_certificate_hook(environment)
    if fault == "uid":
        hook["metadata"]["uid"] = uid("other-certificate-pod")
    elif fault == "running":
        hook["status"]["phase"] = "Running"
    elif fault == "waiting":
        hook["status"]["containerStatuses"][0]["state"] = {"waiting": {"reason": "ContainerCreating"}}
    elif fault == "exit-code":
        hook["status"]["containerStatuses"][0]["state"]["terminated"]["exitCode"] = 1
    elif fault == "finished-at":
        hook["status"]["containerStatuses"][0]["state"]["terminated"]["finishedAt"] = now()
    elif fault == "image":
        hook["spec"]["containers"][0]["image"] = "foreign-image"
    elif fault == "always-restart":
        hook["spec"]["restartPolicy"] = "Always"
    elif fault == "labels":
        hook["metadata"]["labels"]["kubernetes.azure.com/managedby"] = "foreign"
    elif fault == "extra-container":
        hook["spec"]["containers"].append({"name": "other", "image": "foreign-image"})
    elif fault == "ephemeral-container":
        hook["spec"]["ephemeralContainers"] = [{"name": "debug", "image": "foreign-image"}]
    else:
        hook["spec"]["volumes"] = [{"name": "claim", "persistentVolumeClaim": {"claimName": "foreign"}}]
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, True)
    assert not cloud.writes


@pytest.mark.parametrize("instance", ["0", "1"])
def test_stale_live_guest_status_never_authorizes_restart_or_health(environment, instance):
    _, cloud = environment
    cloud.views[instance]["vmAgent"]["statuses"][0]["time"] = cloud.old
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, True)
    assert not cloud.writes


@pytest.mark.parametrize("field", ["owner", "source_state_sha256", "receipt"])
def test_journal_replaced_during_final_preflight_prevents_azure_restart(environment, field):
    _, cloud = environment
    changed = []

    def hook(command):
        if "nodenetworkconfigs" in command and cloud.journal and not changed:
            receipt = json.loads(cloud.journal["data"]["receipt"])
            if receipt.get("attempted"):
                changed.append(True)
                cloud.journal["data"][field] = "changed"
    cloud.hook = hook
    with pytest.raises(recovery.workers.ReconcileError, match="journal"):
        run(environment, True)
    assert changed and not cloud.restart_calls


def test_legitimate_controller_recreation_can_precede_full_node_readiness_after_proven_reboot(environment, monkeypatch):
    _, cloud = environment
    observed = []

    def partial_boot():
        cloud.nodes[recovery.TARGET]["status"]["conditions"][0].update(status="False", lastHeartbeatTime=now())

    def finish_boot(_seconds):
        observed.append(True)
        summary = cloud.receipt()
        assert summary["restart"]["reboot_fenced"] and not summary["host_recovered"]
        assert len(summary["authorized_controller_replacements"]) == 56
        cloud.nodes[recovery.TARGET]["status"]["conditions"][0].update(status="True", lastHeartbeatTime=now())

    cloud.after_restart = partial_boot
    monkeypatch.setattr(recovery.time, "sleep", finish_boot)
    summary = run(environment, True)
    assert observed and summary["host_recovered"] and len(cloud.restart_calls) == 1


def test_python310_syntax_and_seven_digit_provider_timestamps():
    import ast  # pylint: disable=import-outside-toplevel
    ast.parse((DIRECTORY / "stalled_retained_worker_recovery.py").read_text(), feature_version=(3, 10))
    assert base.timestamp("2026-09-12T19:16:38.1234567Z", "VM agent").microsecond == 123456


@pytest.mark.parametrize("fault", ["target-vm", "target-node", "source-pod", "source-taint"])
def test_accepted_restart_cannot_requalify_changed_protected_or_target_identities(environment, fault):
    _, cloud = environment

    def changed():
        if fault == "target-vm":
            cloud.instances[1]["vmId"] = uid("foreign-vm")
        elif fault == "target-node":
            cloud.nodes[recovery.TARGET]["metadata"]["uid"] = uid("foreign-node")
        elif fault == "source-pod":
            next(row for row in cloud.pods if row["metadata"]["name"] == "kwok-node-0")["metadata"]["uid"] = uid("new-source-pod")
        else:
            cloud.nodes[recovery.SOURCE]["spec"]["taints"] = [{"key": "node.kubernetes.io/unreachable", "effect": "NoExecute"}]
    cloud.after_restart = changed
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, True)
    assert len(cloud.restart_calls) == 1 and cloud.receipt()["restart"]["accepted"] is True
    assert not cloud.receipt()["host_recovered"]


def test_safe_diagnostics_do_not_copy_inline_secret_env_values_or_last_applied_payloads():
    raw = {"metadata": {"annotations": {"kubectl.kubernetes.io/last-applied-configuration": "secret-json"}},
           "spec": {"containers": [{"env": [{"name": "ACCESS_TOKEN", "value": "secret"}, {"name": "NODE_NAME", "value": "node"}]}]}}
    safe = recovery.safe_diagnostics(raw)
    assert safe["metadata"]["annotations"] == {}
    assert safe["spec"]["containers"][0]["env"][0]["value"] == "<redacted>"
    assert safe["spec"]["containers"][0]["env"][1]["value"] == "node"
