"""Stateful, offline ARM/Kubernetes safety tests for the explicit modern repair."""

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


MODULE_DIR = Path(__file__).resolve().parents[1] / "clusterloader2" / "clustermesh-scale"
SPEC = importlib.util.spec_from_file_location("modern_cni_recovery", MODULE_DIR / "modern_cni_recovery.py")
modern = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = modern
sys.path.insert(0, str(MODULE_DIR))
try:
    SPEC.loader.exec_module(modern)
finally:
    sys.path.pop(0)
base = modern.base
PROM_VMSS = "aks-promv5-12345678-vmss"
PROM = f"{PROM_VMSS}000000"
NEW_VMSS = "aks-cniv5-87654321-vmss"
NEW = [f"{NEW_VMSS}00000{index}" for index in range(2)]
CORDON = {"key": "node.kubernetes.io/unschedulable", "effect": "NoSchedule"}


def identity(name):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, name))


def now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def meta(name, namespace="", row_uid=None):
    result = {"name": name, "uid": row_uid or identity(f"{namespace}/{name}"),
              "resourceVersion": "1", "generation": 1, "labels": {}, "annotations": {}}
    if namespace:
        result["namespace"] = namespace
    return result


def owner(kind, name, row_uid):
    return {"kind": kind, "name": name, "uid": row_uid, "controller": True}


def make_plan():
    agents = {f"kwok-node-{index}": identity(f"agent/{index}") for index in range(100)}
    return {
        "schema_version": 1, "role": base.ROLE, "node_name": base.PROM_NODE,
        "node_uid": base.REAL_UIDS[base.PROM_NODE], "provider_id": base.PROVIDER,
        "api_pod_name": "clustermesh-apiserver-75c9b44965-wm84r",
        "api_pod_uid": "d1548ec6-ad38-483b-b395-9999b87898d4",
        "api_replica_set_name": "clustermesh-apiserver-75c9b44965",
        "api_replica_set_uid": "67b9481a-bb66-46de-8c9a-d64d122e16a1",
        "api_deployment_uid": "6fc6e6e0-e212-4b25-86a6-971d04e00797",
        "mock_controller_uid": identity("mock-controller"),
        "mock_pod_uids": agents,
        "ready_mock_pod_uids": {name: value for name, value in agents.items() if int(name.rsplit("-", 1)[1]) < 71},
        "kwok_node_uids": {name: identity(name) for name in agents},
        "real_node_uids": copy.deepcopy(base.REAL_UIDS),
        "cni_source": {"node_name": modern.SOURCE, "node_uid": base.REAL_UIDS[modern.SOURCE],
                       "network_container_id": base.SOURCE_NC},
        "framework_pods": copy.deepcopy(list(base.APPROVED_FRAMEWORKS)),
    }


def pod_status(ready=True, address="10.0.0.1"):
    return {
        "phase": "Running" if ready else "Pending", "podIP": address if ready else "",
        "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
        "containerStatuses": [{"name": "container", "ready": ready, "started": ready, "restartCount": 0,
                               "state": {"running": {}} if ready else {"waiting": {"reason": "ContainerCreating"}}}],
    }


def make_pod(name, namespace, node, controller, *, ready=True, row_uid=None):
    metadata = meta(name, namespace, row_uid)
    metadata["ownerReferences"] = [controller]
    return {
        "apiVersion": "v1", "kind": "Pod", "metadata": metadata,
        "spec": {"nodeName": node, "containers": [{"name": "container", "image": "existing"}],
                 "volumes": [{"name": "scratch", "emptyDir": {}}]},
        "status": pod_status(ready),
    }


class FakeCloud:
    """Raw Azure response schemas; the same real JMESPath queries run in tests."""

    def __init__(self, plan, args):
        self.plan, self.args = plan, args
        self.commands, self.writes, self.deletes, self.evictions = [], [], [], []
        self.hook = None
        self.after_delete = None
        self.pool_error = False
        self.delete_error = False
        self.eviction_error = False
        self.retirement_error = False
        self.probe_create_error = False
        self.probe_delete_error = False
        self.no_growth = False
        self.no_version = False
        self.http_error = False
        self.replacement_ready = True
        self.foreign_destination = False
        self.peer_fault = ""
        self.metrics_stamp = None
        self.metrics_memory = None
        self.framework_moves = []
        self.journals = {}
        self.nodes, self.nncs, self.pods, self.controllers = {}, {}, [], []
        self.pools, self.scales, self.instances, self.views = {}, {}, {}, {}
        self.pool_operations = {}
        self.scale_views = {}
        self.operation = {"name": "original-completed-operation", "status": "Succeeded",
                          "operationType": "PutManagedCluster", "startTime": now(), "endTime": now()}
        self.scope()
        self.add_pool("default", base.DEFAULT_VMSS, 2, "Standard_D8_v3", "System", 110)
        self.add_pool("promv5", PROM_VMSS, 1, modern.SKU, "User", 250)
        for name, node_uid in plan["kwok_node_uids"].items():
            self.nodes[name] = {
                "kind": "Node", "metadata": meta(name, row_uid=node_uid),
                "spec": {"taints": [{"key": "kwok-provider", "value": "true", "effect": "NoSchedule"}]},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            }
            self.nodes[name]["metadata"]["labels"] = {"type": "kwok"}
        self.template = {
            "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [{"matchExpressions": [
                    {"key": "kubernetes.azure.com/cluster", "operator": "Exists"},
                    {"key": "prometheus", "operator": "DoesNotExist"},
                ]}],
            }}},
            "containers": [{"name": "mock-cilium-agent", "image": "existing-agent",
                            "resources": {"requests": {"cpu": "100m", "memory": "256Mi"},
                                          "limits": {"memory": "1Gi"}}}],
        }
        self.controllers.append({
            "kind": "StatefulSet", "metadata": meta("kwok-node", "mock-clustermesh", plan["mock_controller_uid"]),
            "spec": {"replicas": 100, "template": {"spec": self.template}},
        })
        for name in ("cilium", "azure-cns"):
            self.controllers.append({
                "kind": "DaemonSet", "metadata": meta(name, "kube-system"),
                "spec": {"selector": {"matchLabels": {"k8s-app": name}},
                         "template": {"spec": {"containers": [{"name": name, "image": "pinned"}]}}},
            })
        for node in (modern.SOURCE, modern.RETAINED, PROM):
            self.add_daemonsets(node)
        for name, pod_uid in plan["mock_pod_uids"].items():
            index = int(name.rsplit("-", 1)[1])
            node = modern.RETAINED if index < 56 else modern.SOURCE
            pod = make_pod(name, "mock-clustermesh", node,
                           owner("StatefulSet", "kwok-node", plan["mock_controller_uid"]),
                           ready=index < 71, row_uid=pod_uid)
            pod["metadata"]["labels"] = {"app": "mock-cilium-agent", "mock-clustermesh/agent-controller": "kwok-node"}
            pod["spec"].update(copy.deepcopy(self.template))
            if index < 71:
                pod["status"]["podIP"] = self.assign_ip(node)
            self.pods.append(pod)
        targets = [{
            "namespace": "kube-system", "deployment_name": "clustermesh-apiserver",
            "deployment_uid": plan["api_deployment_uid"],
            "replica_set_name": plan["api_replica_set_name"], "replica_set_uid": plan["api_replica_set_uid"],
            "pod_name": plan["api_pod_name"], "pod_uid": plan["api_pod_uid"],
        }, *plan["framework_pods"]]
        for target in targets:
            if not any(row["metadata"]["name"] == target["deployment_name"] and row["kind"] == "Deployment"
                       for row in self.controllers):
                self.add_deployment(target, 5 if target["deployment_name"] == "coredns" else 1)
            new_name = f"{target['pod_name']}-modern"
            pod = make_pod(new_name, target["namespace"], PROM,
                           owner("ReplicaSet", target["replica_set_name"], target["replica_set_uid"]))
            pod["status"]["podIP"] = self.assign_ip(PROM)
            self.pods.append(pod)
            self.framework_moves.append({**target, "ready_pod_uid": modern.uid(pod), "ready_node": PROM,
                                         "state": "moved", "delete_attempted": True})
        for index in range(3):
            pod = make_pod(f"coredns-fixed-{index}", "kube-system", modern.RETAINED,
                           owner("ReplicaSet", base.DNS_REPLICA_SET, base.DNS_REPLICA_SET_UID))
            pod["status"]["podIP"] = self.assign_ip(modern.RETAINED)
            self.pods.append(pod)
        self.add_deployment({"namespace": "kube-system", "deployment_name": "metrics-server",
                             "replica_set_name": "metrics-server-pinned",
                             "replica_set_uid": identity("metrics-server-rs")}, 2)
        for index, node in enumerate((modern.RETAINED, modern.SOURCE)):
            pod = make_pod(f"metrics-server-{index}", "kube-system", node,
                           owner("ReplicaSet", "metrics-server-pinned", identity("metrics-server-rs")),
                           ready=index == 0)
            pod["metadata"]["labels"] = {"app": "metrics-server"}
            if index == 0:
                pod["status"]["podIP"] = self.assign_ip(node)
            self.pods.append(pod)
        self.pdbs = [{
            "kind": "PodDisruptionBudget", "metadata": meta("metrics-server-pdb", "kube-system"),
            "spec": {"minAvailable": 1, "selector": {"matchLabels": {"app": "metrics-server"}}},
            "status": {"observedGeneration": 1, "disruptionsAllowed": 1, "currentHealthy": 1, "desiredHealthy": 1},
        }]
        self.skus = [{
            "name": modern.SKU, "resourceType": "virtualMachines", "family": modern.FAMILY,
            "locations": [base.REGION], "restrictions": [],
            "capabilities": [{"name": name, "value": value}
                             for name, value in (("vCPUs", "8"), ("MemoryGB", "32"), ("PremiumIO", "True"))],
        }]
        self.usage = [
            {"name": {"value": modern.FAMILY}, "currentValue": "100.0", "limit": "1000.0"},
            {"name": {"value": "cores"}, "currentValue": "5900.0", "limit": "10000.0"},
            {"name": {"value": "standardDv3Family"}, "currentValue": "5464.0", "limit": "5000.0"},
        ]
        self.original_healthy = {name: modern.uid(self.pod(name)) for name in plan["ready_mock_pod_uids"]}

    def scope(self):
        prefix = f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.RESOURCE_GROUP}"
        expiry = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        self.group = {
            "id": prefix, "location": base.REGION, "tags": {
                "clustermesh_debug_preserved": "true", "run_id": base.RESOURCE_GROUP,
                "scenario": "perf-eval-clustermesh-scale", "clustermesh_debug_expected_clusters": "100",
                "clustermesh_debug_tfvars_sha256": self.args.expected_tfvars_sha, "deletion_due_time": expiry,
            },
        }
        self.clusters, self.members = [], []
        self.identities = []
        fleet = f"{prefix}/providers/Microsoft.ContainerService/fleets/clustermesh-flt"
        for index in range(1, 101):
            role, name = f"mesh-{index}", f"clustermesh-{index}"
            cluster_id = f"{prefix}/providers/Microsoft.ContainerService/managedClusters/{name}"
            self.clusters.append({
                "id": cluster_id, "name": name, "location": base.REGION,
                "nodeResourceGroup": f"mc_{base.RESOURCE_GROUP}_{name}_{base.REGION}",
                "tags": {"role": role, "run_id": base.RESOURCE_GROUP},
                "provisioningState": "Succeeded", "powerState": {"code": "Running"},
            })
            self.members.append({
                "id": f"{fleet}/members/{role}", "name": role, "clusterResourceId": cluster_id,
                "provisioningState": "Succeeded", "labels": {"mesh": "true"},
                "meshProperties": {"ciliumProperties": {"name": f"assigned-{index}", "id": index},
                                   "clusterMeshProfileResourceId": f"{fleet}/clusterMeshProfiles/clustermesh-cmp",
                                   "status": {"state": "Connected"}},
            })
            self.identities.append({"role": role, "cluster_name": f"assigned-{index}", "cluster_id": index})
        self.cluster_id = self.clusters[95]["id"]
        self.node_group = {"id": f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.NODE_GROUP}",
                           "location": base.REGION, "managedBy": self.cluster_id,
                           "tags": {"deletion_due_time": expiry}}

    def add_pool(self, name, vmss, count, sku, mode, max_pods):
        image = "AKSUbuntu-2404containerd-202609.10.0" if name != "default" else "AKSUbuntu-2404containerd-202608.26.0"
        pool = {
            "id": f"{self.cluster_id}/agentPools/{name}", "name": name, "count": count,
            "vmSize": sku, "mode": mode, "maxPods": max_pods, "osSku": "Ubuntu", "osType": "Linux",
            "osDiskType": "Managed", "osDiskSizeGb": 256, "kubeletDiskType": "OS",
            "enableAutoScaling": False, "enableFips": False, "enableEncryptionAtHost": False,
            "enableNodePublicIp": False, "nodeTaints": None,
            "nodeLabels": {"prometheus": "true"} if name == "promv5" else None,
            "vnetSubnetId": modern.SUBNET_PREFIX + "node", "podSubnetId": modern.SUBNET_PREFIX + "pod",
            "orchestratorVersion": "1.35.1" if name == modern.POOL else "1.35",
            "currentOrchestratorVersion": "1.35.1", "nodeImageVersion": image,
            "provisioningState": "Succeeded", "powerState": {"code": "Running"},
        }
        self.pools[name] = pool
        self.pool_operations[name] = {
            "name": f"{name}-initial-pool-operation", "status": "Succeeded",
            "operationType": "PutAgentPool", "startTime": now(), "endTime": now(),
        }
        scale_id = (f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.NODE_GROUP}"
                    f"/providers/Microsoft.Compute/virtualMachineScaleSets/{vmss}")
        self.scales[vmss] = {
            "id": scale_id, "name": vmss, "location": base.REGION, "sku": {"capacity": count, "name": sku},
            "tags": {"aks-managed-poolName": name}, "orchestrationMode": "Uniform", "provisioningState": "Succeeded",
        }
        self.scale_views[vmss] = {
            "statuses": [{"code": "ProvisioningState/succeeded"}],
            "virtualMachine": {"statusesSummary": [{"code": "ProvisioningState/succeeded", "count": count}]},
        }
        self.instances[vmss] = []
        for index in range(count):
            node = f"{vmss}{index:06d}"
            node_uid = base.REAL_UIDS.get(node, identity(node))
            vm_id = (modern.SOURCE_VM_ID if node == modern.SOURCE else
                     modern.RETAINED_VM_ID if node == modern.RETAINED else identity(f"vm/{node}"))
            instance = {
                "id": f"{scale_id}/virtualMachines/{index}", "name": f"{vmss}_{index}", "instanceId": str(index),
                "osProfile": {"computerName": node}, "provisioningState": "Succeeded",
                "vmId": vm_id, "latestModelApplied": True,
            }
            self.instances[vmss].append(instance)
            self.views[(vmss, str(index))] = {
                "statuses": [{"code": "ProvisioningState/succeeded"}, {"code": "PowerState/running"}],
                "extensions": [{"name": "vmssCSE", "statuses": [{"code": "ProvisioningState/succeeded"}]}],
            }
            labels = {"kubernetes.azure.com/cluster": base.NODE_GROUP, "kubernetes.azure.com/agentpool": name,
                      "agentpool": name, "kubernetes.azure.com/node-image-version": image, "kubernetes.io/os": "linux"}
            if name == "promv5":
                labels["prometheus"] = "true"
            self.nodes[node] = {
                "kind": "Node", "metadata": meta(node, row_uid=node_uid),
                "spec": {"providerID": "azure://" + instance["id"], "taints": [], "unschedulable": False},
                "status": {"conditions": [{"type": "Ready", "status": "True"}],
                           "allocatable": {"cpu": "7820m", "memory": "28Gi", "pods": str(max_pods)},
                           "nodeInfo": {"kubeletVersion": "v1.35.1", "bootID": identity(f"boot/{node}")}},
            }
            self.nodes[node]["metadata"]["labels"] = labels
            count_ips = 16 if name == "cniv5" else 128
            self.nncs[node] = {
                "metadata": meta(node, "kube-system"), "spec": {"requestedIPCount": count_ips},
                "status": {"assignedIPCount": count_ips, "networkContainers": [
                    {"id": base.SOURCE_NC if node == modern.SOURCE else identity(f"nc/{node}"),
                     "version": 10, "ipAssignments": [{"ip": address} for address in self.ip_block(node, count_ips)]},
                ]},
            }
            self.nncs[node]["metadata"]["ownerReferences"] = [owner("Node", node, node_uid)]
            if name == "cniv5":
                self.add_daemonsets(node)

    @staticmethod
    def ip_block(node, count):
        prefix = [modern.SOURCE, modern.RETAINED, PROM, *NEW].index(node) + 1
        return [f"10.244.{prefix}.{index}" for index in range(1, count + 1)]

    def assign_ip(self, node):
        used = {pod["status"].get("podIP") for pod in self.pods if pod["spec"].get("nodeName") == node}
        row = self.nncs[node]
        container = row["status"]["networkContainers"][0]
        available = [item["ip"] for item in container["ipAssignments"] if item["ip"] not in used]
        if not available:
            previous = row["status"]["assignedIPCount"]
            block = self.ip_block(node, previous + 16)
            if not self.no_growth:
                row["status"]["assignedIPCount"] += 16
                row["spec"]["requestedIPCount"] += 16
                container["ipAssignments"] = [{"ip": ip} for ip in block]
            if not self.no_version:
                container["version"] += 1
            return block[-1]
        return available[0]

    def add_daemonsets(self, node):
        for name in ("cilium", "azure-cns"):
            pod = make_pod(f"{name}-{node}", "kube-system", node,
                           owner("DaemonSet", name, identity(f"kube-system/{name}")))
            pod["metadata"]["labels"] = {"k8s-app": name}
            pod["spec"]["hostNetwork"] = True
            pod["status"]["podIP"] = f"192.168.0.{list(self.nodes).index(node) + 1}"
            pod["status"]["containerStatuses"][0]["name"] = "cilium-agent" if name == "cilium" else name
            self.pods.append(pod)

    def add_deployment(self, target, count):
        namespace = target["namespace"]
        name = target["deployment_name"]
        deployment_uid = target.get("deployment_uid", identity(f"{namespace}/{name}"))
        self.controllers.extend([
            {"kind": "Deployment", "metadata": meta(name, namespace, deployment_uid),
             "spec": {"replicas": count, "template": {"spec": {"containers": [{"name": name, "image": "pinned"}]}}},
             "status": {"readyReplicas": count}},
            {"kind": "ReplicaSet", "metadata": {
                **meta(target["replica_set_name"], namespace, target["replica_set_uid"]),
                "ownerReferences": [owner("Deployment", name, deployment_uid)]},
             "spec": {"replicas": count, "template": {"spec": {"containers": [{"name": name, "image": "pinned"}]}}}},
        ])

    def pod(self, name):
        return next(row for row in self.pods if row["metadata"]["name"] == name)

    def receipt(self):
        return json.loads(Path(self.args.summary_file).read_text(encoding="utf-8"))

    def checkpoint(self):
        prom = self.nodes[PROM]
        return {
            "repaired": True, "success": True, "execute": True, "phase1_only": True, "workloads_ready": False,
            "plan_sha256": modern.digest(self.plan), "controller_pins": base.frozen_controllers({
                "controllers": {"items": self.controllers}}),
            "pdb_pins": base.frozen_pdbs({"pdbs": {"items": self.pdbs}}),
            "authoritative_identities": self.identities, "pod_moves": self.framework_moves,
            "modern_baseline_delta": {"pool_name": "promv5", "heterogeneous_skus": ["Standard_D8_v3", modern.SKU]},
            "modern_prom": {
                "pool_name": "promv5", "pool_resource_id": self.pools["promv5"]["id"],
                "vmss_name": PROM_VMSS, "instance_id": "0", "node_name": PROM, "node_uid": modern.uid(prom),
                "vm_id": self.instances[PROM_VMSS][0]["vmId"], "provider_id": prom["spec"]["providerID"],
                "network_container_id": self.nncs[PROM]["status"]["networkContainers"][0]["id"],
                "pool_configuration_sha256": modern.digest(modern.prepared.pool_configuration(self.pools["promv5"])),
                "legacy_empty_pool_retired": True,
            },
        }

    @staticmethod
    def value(command, flag):
        return command[command.index(flag) + 1]

    def run(self, command, timeout_seconds):
        assert 0 < timeout_seconds <= 45
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
            assert command[0] == "kubectl"
            assert self.value(command, "--kubeconfig") == self.args.kubeconfig
            assert self.value(command, "--context") == base.CLUSTER
            result = self.kubernetes(command)
        return result if isinstance(result, str) else json.dumps(copy.deepcopy(result))

    def azure(self, command):
        route = command[1:3]
        if route == ["account", "show"]:
            return {"id": base.SUBSCRIPTION}
        if route == ["group", "show"]:
            return self.group if self.value(command, "--name") == base.RESOURCE_GROUP else self.node_group
        if route == ["aks", "list"]:
            return self.clusters
        if route == ["aks", "show"]:
            return {**self.clusters[95], "currentKubernetesVersion": "1.35.1"}
        if command[1:4] == ["fleet", "member", "list"]:
            return self.members
        if command[1:4] == ["aks", "operation", "show-latest"]:
            if "--nodepool-name" in command:
                return self.pool_operations[self.value(command, "--nodepool-name")]
            return self.operation
        if command[1:4] == ["aks", "nodepool", "list"]:
            return list(self.pools.values())
        if route == ["vm", "list-skus"]:
            return self.skus
        if route == ["vm", "list-usage"]:
            return self.usage
        if route == ["vmss", "list"]:
            return list(self.scales.values())
        if route == ["vmss", "list-instances"]:
            return self.instances[self.value(command, "--name")]
        if route == ["vmss", "get-instance-view"]:
            name = self.value(command, "--name")
            if "--instance-id" in command:
                return self.views[(name, self.value(command, "--instance-id"))]
            return self.scale_views[name]
        if command[1:4] == ["aks", "nodepool", "add"]:
            self.writes.append(command)
            receipt = self.receipt()["actions"]["pool-create"]
            assert receipt["attempted"] and receipt["accepted"] is None and receipt["ambiguous"]
            assert self.value(command, "--name") == "cniv5" and self.value(command, "--mode") == "System"
            assert self.value(command, "--node-count") == "2" and self.value(command, "--node-vm-size") == modern.SKU
            assert "--no-wait" in command and not any("scale" == word for word in command)
            assert self.nodes[modern.SOURCE]["spec"]["unschedulable"]
            if self.pool_error:
                raise modern.workers.ReconcileError("ambiguous accepted pool add transport")
            self.add_pool("cniv5", NEW_VMSS, 2, modern.SKU, "System", 110)
            self.pool_operations["cniv5"] = {"name": "owned-pool-create", "status": "Succeeded",
                                             "operationType": "PutAgentPool", "startTime": now(), "endTime": now()}
            return ""
        if command[1:4] == ["aks", "nodepool", "delete-machines"]:
            self.writes.append(command)
            assert self.value(command, "--name") == "default"
            assert self.value(command, "--machine-names") == modern.SOURCE
            assert all(modern.mocks._pod_ready(self.pod(name)) for name in self.plan["mock_pod_uids"])
            assert not any(pod["spec"].get("nodeName") == modern.SOURCE
                           and pod["metadata"]["ownerReferences"][0]["kind"] != "DaemonSet" for pod in self.pods)
            record = self.receipt()["actions"]["source-retirement"]
            assert record["accepted"] is None and record["ambiguous"]
            if self.retirement_error:
                raise modern.workers.ReconcileError("ambiguous native retirement transport")
            self.pools["default"]["count"] = 1
            self.scales[base.DEFAULT_VMSS]["sku"]["capacity"] = 1
            self.scale_views[base.DEFAULT_VMSS]["virtualMachine"]["statusesSummary"][0]["count"] = 1
            self.instances[base.DEFAULT_VMSS] = [self.instances[base.DEFAULT_VMSS][1]]
            self.nodes.pop(modern.SOURCE)
            self.nncs.pop(modern.SOURCE)
            self.pods = [pod for pod in self.pods if pod["spec"].get("nodeName") != modern.SOURCE]
            self.pool_operations["default"] = {"name": "owned-source-retirement", "status": "Succeeded",
                                               "operationType": "DeleteMachines", "startTime": now(), "endTime": now()}
            return ""
        raise AssertionError(f"Forbidden Azure action: {command}")

    def kubernetes(self, command):
        namespace = self.value(command, "-n") if "-n" in command else None
        if "create" in command:
            assert command[command.index("create") + 1] == "configmap"
            self.writes.append(command)
            name = command[command.index("create") + 2]
            assert name not in self.journals
            data = dict(word.removeprefix("--from-literal=").split("=", 1)
                        for word in command if word.startswith("--from-literal="))
            self.journals[name] = {"metadata": meta(name, namespace), "data": data}
            return self.journals[name]
        if "patch" in command:
            self.writes.append(command)
            resource = command[command.index("patch") + 1]
            name = command[command.index("patch") + 2]
            target = self.nodes[name] if resource == "node" else self.journals[name]
            patch = json.loads(self.value(command, "-p"))
            self.apply_patch(target, patch)
            if resource == "node" and target["spec"].get("unschedulable") and CORDON not in target["spec"]["taints"]:
                target["spec"]["taints"].append(dict(CORDON))
                target["metadata"]["resourceVersion"] = str(int(target["metadata"]["resourceVersion"]) + 1)
            return target
        if "run" in command:
            self.writes.append(command)
            name = command[command.index("run") + 1]
            override = json.loads(next(word.split("=", 1)[1] for word in command if word.startswith("--overrides=")))
            key, token = next(word.split("=", 1)[1] for word in command if word.startswith("--labels=")).split("=", 1)
            node = override["spec"]["nodeName"]
            pod = {"kind": "Pod", "metadata": meta(name, namespace), "spec": override["spec"],
                   "status": pod_status(address=self.assign_ip(node))}
            pod["metadata"]["labels"] = {key: token}
            self.pods.append(pod)
            if self.probe_create_error:
                raise modern.workers.ReconcileError("ambiguous probe creation")
            return pod
        if "exec" in command:
            remotes = [{"name": f"assigned-{index}", "ready": True, "connected": True,
                        "config": {"required": True, "retrieved": True, "cluster-id": index}}
                       for index in range(1, 101) if index != 96]
            if self.peer_fault == "name":
                remotes[0]["name"] = "foreign"
            if self.peer_fault == "id":
                remotes[0]["config"]["cluster-id"] = 500
            if self.peer_fault == "connected":
                remotes[0]["connected"] = False
            return {"cluster-mesh": {"clusters": remotes}}
        assert "get" in command, f"Forbidden Kubernetes command: {command}"
        following = command[command.index("get") + 1:]
        if following[0] == "--raw=/readyz":
            return "ok"
        if following[0] == "--raw":
            path = following[1]
            if "/proxy/hostname" in path:
                return "wrong-host" if self.http_error else path.split("/pods/")[1].split(":")[0]
            if path.endswith("/nodes"):
                rows = []
                for name, node in self.nodes.items():
                    if not node["spec"].get("providerID"):
                        continue
                    agents = sum(pod["spec"].get("nodeName") == name
                                 and pod["metadata"].get("labels", {}).get("app") == "mock-cilium-agent"
                                 and modern.mocks._pod_ready(pod) for pod in self.pods)
                    rows.append({"metadata": {"name": name}, "timestamp": self.metrics_stamp or now(),
                                 "usage": {"cpu": "400m", "memory": self.metrics_memory or str(
                                     2 * 1024**3 + agents * 300 * 1024**2)}})
                return {"items": rows}
            assert path.endswith("/pods")
            return {"items": [
                {"metadata": {"name": pod["metadata"]["name"], "namespace": "mock-clustermesh"},
                 "timestamp": self.metrics_stamp or now(),
                 "containers": [{"name": "mock-cilium-agent", "usage": {"cpu": "30m", "memory": "300Mi"}}]}
                for pod in self.pods if pod["metadata"].get("labels", {}).get("app") == "mock-cilium-agent"
                and modern.mocks._pod_ready(pod)
            ]}
        resource = following[0]
        if resource == "nodes":
            return {"items": list(self.nodes.values())}
        if resource == "node":
            return self.nodes[following[1]]
        if resource == "pods":
            pods = [pod for pod in self.pods if namespace is None or pod["metadata"].get("namespace") == namespace]
            if "-l" in command:
                key, value = self.value(command, "-l").split("=", 1)
                pods = [pod for pod in pods if pod["metadata"].get("labels", {}).get(key) == value]
            return {"items": pods}
        if resource == "events":
            return {"items": [
                {"involvedObject": {"kind": "Pod", "name": name, "uid": self.plan["mock_pod_uids"][name],
                                    "namespace": "mock-clustermesh"},
                 "reason": "FailedCreatePodSandBox", "lastTimestamp": now(), "source": {"host": modern.SOURCE},
                 "message": "cilium-cni AllocateIPConfig failed: not enough IPs available of type ipv4"}
                for name in self.plan["mock_pod_uids"] if name not in self.plan["ready_mock_pod_uids"]
            ]}
        if resource in ("nnc", "nodenetworkconfigs"):
            return {"items": list(self.nncs.values())}
        if resource == "deployments,replicasets,daemonsets,statefulsets":
            return {"items": self.controllers}
        if resource == "pdb":
            return {"items": self.pdbs}
        if resource == "configmaps":
            return {"items": list(self.journals.values())}
        if resource == "configmap":
            if following[1] == "cilium-config":
                return {"data": {"cluster-name": "assigned-96", "cluster-id": "96"}}
            return self.journals[following[1]]
        raise AssertionError(f"Unexpected read: {command}")

    @staticmethod
    def apply_patch(target, operations):
        for operation in operations:
            parts = [part.replace("~1", "/").replace("~0", "~") for part in operation["path"].split("/")[1:]]
            parent = target
            for part in parts[:-1]:
                parent = parent[int(part)] if isinstance(parent, list) else parent[part]
            key = int(parts[-1]) if isinstance(parent, list) else parts[-1]
            if operation["op"] == "test":
                assert parent[key] == operation["value"]
            elif operation["op"] == "add":
                parent[key] = copy.deepcopy(operation["value"])
            else:
                assert operation["op"] == "remove"
                del parent[key]
        target["metadata"]["resourceVersion"] = str(int(target["metadata"]["resourceVersion"]) + 1)

    def delete(self, _cluster, *, namespace, name, uid, timeout_seconds, attempts, retry_seconds):
        assert namespace == "mock-clustermesh" and attempts == 1 and retry_seconds == 0 and 0 < timeout_seconds <= 45
        pod = self.pod(name)
        assert modern.uid(pod) == uid
        self.deletes.append((name, uid))
        if name.startswith("cni-maint-probe"):
            if self.probe_delete_error:
                raise modern.mocks.RecoveryError("ambiguous probe delete")
            self.pods.remove(pod)
            return
        assert pod["spec"]["nodeName"] == modern.SOURCE
        receipt = self.receipt()
        if not receipt.get("pending_phase_complete"):
            assert name not in self.plan["ready_mock_pod_uids"]
            assert all(modern.uid(self.pod(n)) == value and base.pod_ready(self.pod(n))
                       for n, value in self.original_healthy.items())
        else:
            assert receipt["pending_phase_ready"] == 100
            assert sum(base.pod_ready(self.pod(n)) for n in self.plan["mock_pod_uids"]) == 100
        old = copy.deepcopy(pod)
        self.pods.remove(pod)
        counts = {node: sum(row["spec"].get("nodeName") == node for row in self.pods) for node in NEW}
        destination = modern.RETAINED if self.foreign_destination else min(NEW, key=counts.get)
        pod["metadata"]["uid"] = identity(f"replacement/{uid}")
        pod["spec"]["nodeName"] = destination
        pod["status"] = pod_status(self.replacement_ready, self.assign_ip(destination))
        self.pods.append(pod)
        if self.after_delete:
            self.after_delete(old, pod)
        if self.delete_error:
            raise modern.mocks.RecoveryError("ambiguous mock delete")

    def evict(self, _cluster, *, namespace, name, pod_uid, timeout_seconds):
        assert 0 < timeout_seconds <= 45 and namespace == "kube-system"
        pod = self.pod(name)
        assert modern.uid(pod) == pod_uid and pod["spec"]["nodeName"] == modern.SOURCE
        self.evictions.append(pod_uid)
        if self.eviction_error:
            raise modern.workers.ReconcileError("PDB eviction HTTP 429")
        self.pods.remove(pod)
        pod["metadata"]["uid"] = identity(f"evicted/{pod_uid}")
        pod["metadata"]["name"] += "-replacement"
        pod["spec"]["nodeName"] = NEW[0]
        pod["status"] = pod_status(address=self.assign_ip(NEW[0]))
        self.pods.append(pod)


@pytest.fixture(name="environment")
def modern_environment(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    plan = make_plan()
    monkeypatch.setattr(modern, "ORIGINAL_PLAN_SHA256", modern.digest(plan))
    args = SimpleNamespace(
        plan_file="original-plan.json", modern_prom_checkpoint="monitoring-receipt.json",
        summary_file="cni-receipt.json", kubeconfig="private-config", context=base.CLUSTER,
        resource_group=base.RESOURCE_GROUP, confirm_resource_group=base.RESOURCE_GROUP,
        expected_subscription=base.SUBSCRIPTION, expected_region=base.REGION, expected_tfvars_sha="a" * 64,
        execute=False, timeout_seconds=5400, request_timeout_seconds=45, poll_seconds=1, per_pod_ready_seconds=1,
    )
    fake = FakeCloud(plan, args)
    checkpoint = fake.checkpoint()
    Path(args.plan_file).write_text(json.dumps(plan), encoding="utf-8")
    Path(args.modern_prom_checkpoint).write_text(json.dumps(checkpoint), encoding="utf-8")
    return args, plan, fake, checkpoint


def run(environment, *, execute=False):
    args, _, fake, _ = environment
    args.execute = execute
    summary = {}
    modern.execute_recovery(args, summary, runner=fake.run, delete_pod=fake.delete, evict_pod=fake.evict)
    assert summary == fake.receipt()
    return summary


def test_read_only_full_scope_plan_has_no_writes_and_all_29_together(environment):
    _, plan, fake, _ = environment
    summary = run(environment)
    assert not fake.writes and not fake.deletes and not fake.evictions
    assert summary["plan_valid"] and not summary["success"] and not summary["repaired"]
    assert not summary["modern_cni"]["completed"]
    assert "baseline_pool_layout" not in summary
    assert not summary["workloads_ready"] and summary["pending_plan_count"] == 29
    assert len(summary["original_pending_plan"]) == 29
    assert len(summary["protected_other_worker_uids"]) == 56
    assert len(summary["original_healthy_source_uids"]) == 15
    assert summary["original_identity"]["mock_pod_uids"] == plan["mock_pod_uids"]
    assert summary["desired_pool"]["orchestratorVersion"] == "1.35.1"
    assert summary["quota_proof"]["quotas"][modern.FAMILY]["used"] == 100


def test_complete_one_add_pending_first_99_barriers_exact_retirement_and_202_delta(environment):
    args, plan, fake, checkpoint = environment
    original_plan = Path(args.plan_file).read_bytes()
    retained_node = copy.deepcopy(fake.nodes[modern.RETAINED])
    original_vm = copy.deepcopy(fake.instances[base.DEFAULT_VMSS][1])
    summary = run(environment, execute=True)
    assert summary["repaired"] and summary["success"] and summary["workloads_ready"] is False
    assert summary["modern_cni"] == {
        "completed": True, "pool_name": "cniv5", "source_retired": True,
        "default_pool_count": 1, "destination_pool_count": 2, "default_role_worker_count": 3,
    }
    assert summary["baseline_pool_layout"] == {
        "schema_version": 1, "role": "mesh-96", "expected_total_pool_count": 202,
        "pools": {
            "default": {"count": 1, "mode": "System", "vm_size": "Standard_D8_v3",
                        "resource_id": fake.pools["default"]["id"]},
            "promv5": {"count": 1, "mode": "User", "vm_size": modern.SKU,
                       "resource_id": fake.pools["promv5"]["id"]},
            "cniv5": {"count": 2, "mode": "System", "vm_size": modern.SKU,
                      "resource_id": fake.pools["cniv5"]["id"]},
        },
    }
    assert summary["plan_sha256"] == modern.digest(plan)
    assert summary["modern_prom_checkpoint_sha256"] == modern.digest(checkpoint)
    assert Path(args.plan_file).read_bytes() == original_plan
    assert fake.instances[base.DEFAULT_VMSS] == [original_vm]
    actual_retained = copy.deepcopy(fake.nodes[modern.RETAINED])
    actual_retained["metadata"]["resourceVersion"] = retained_node["metadata"]["resourceVersion"]
    assert actual_retained == retained_node
    assert len(summary["pod_moves"]) == 44
    assert [row["phase"] for row in summary["pod_moves"]] == ["pending"] * 29 + ["healthy"] * 15
    assert all(row["ready_before"] == row["ready_after"] == 100 for row in summary["pod_moves"][29:])
    assert len({row["uid"] for row in summary["pod_moves"]}) == 44
    assert all(row["completed"] and row["ready_node"] in NEW for row in summary["pod_moves"])
    assert all(summary["effective_identity"]["mock_pod_uids"][name] == value
               for name, value in summary["protected_other_worker_uids"].items())
    assert summary["effective_identity"]["kwok_node_uids"] == plan["kwok_node_uids"]
    assert summary["actions"]["source-retirement"]["native_disappearance_proven"]
    assert summary["actions"]["pool-create"]["operation_name"] == "owned-pool-create"
    assert summary["last_arm_proof"]["operation"]["name"] == summary["initial_operation"]["name"]
    assert summary["last_arm_proof"]["operation"]["operationType"] == "PutManagedCluster"
    azure_writes = [command for command in fake.writes if command[0] == "az"]
    assert [command[1:4] for command in azure_writes] == [
        ["aks", "nodepool", "add"], ["aks", "nodepool", "delete-machines"],
    ]
    assert not summary["probe_cleanup_pending"] and not summary["temporary_exclusions"]
    assert fake.evictions and modern.SOURCE not in fake.nodes
    assert summary["cilium_proof"]["cilium_agent_count"] == 4
    assert summary["final_fleet_connected"] == summary["final_mock_ready"] == summary["final_kwok_ready"] == 100
    delta = summary["modern_baseline_delta"]
    assert delta["global_pool_count_before"] == 201 and delta["global_pool_count_after"] == 202
    assert delta["pools"]["default"]["count"] == 1 and delta["pools"]["cniv5"]["count"] == 2
    assert delta["pools"]["default"]["node_image_version"] != delta["pools"]["cniv5"]["node_image_version"]
    assert len(fake.journals) == 1 and summary["journal"]["create_accepted"]
    journal = json.loads(next(iter(fake.journals.values()))["data"]["receipt"])
    assert journal["status"] == "final-proofs-complete"
    assert journal["baseline_pool_layout_sha256"] == modern.digest(summary["baseline_pool_layout"])
    for proof in summary["fresh_ip_growth"].values():
        assert proof["qualified"] and proof["after"]["version"] > proof["baseline"]["version"]
        assert proof["after"]["assigned_ip_count"] > proof["baseline"]["assigned_ip_count"]
        assert len(proof["ready_probe_ips"]) == proof["probe_count"]


@pytest.mark.parametrize("fault", [
    "fleet", "scope", "lease", "source-uid", "retained-uid", "source-vm", "retained-vm", "source-nc",
    "prom-vm", "prom-node", "prom-config", "pvc", "controller", "pdb", "kwok", "healthy-pod",
    "family-quota", "regional-quota", "sku", "extensions-none", "aggregate-plural", "pool-count",
    "fips", "image", "foreign-hold", "unexpected-cniv5", "journal", "peer-id", "peer-name",
])
def test_preflight_faults_make_no_resource_writes(environment, fault):
    _, _, fake, _ = environment
    if fault == "fleet":
        fake.members[0]["meshProperties"]["status"]["state"] = "Failed"
    elif fault == "scope":
        fake.group["tags"]["run_id"] = "wrong"
    elif fault == "lease":
        fake.group["tags"]["deletion_due_time"] = now()
    elif fault in ("source-uid", "retained-uid"):
        fake.nodes[modern.SOURCE if fault == "source-uid" else modern.RETAINED]["metadata"]["uid"] = identity(fault)
    elif fault in ("source-vm", "retained-vm"):
        fake.instances[base.DEFAULT_VMSS][0 if fault == "source-vm" else 1]["vmId"] = identity(fault)
    elif fault == "source-nc":
        fake.nncs[modern.SOURCE]["status"]["networkContainers"][0]["id"] = identity(fault)
    elif fault == "prom-vm":
        fake.instances[PROM_VMSS][0]["vmId"] = identity(fault)
    elif fault == "prom-node":
        fake.nodes[PROM]["metadata"]["uid"] = identity(fault)
    elif fault == "prom-config":
        fake.pools["promv5"]["maxPods"] = 110
    elif fault == "pvc":
        fake.pod("kwok-node-71")["spec"]["volumes"] = [{"name": "claim", "persistentVolumeClaim": {"claimName": "x"}}]
    elif fault == "controller":
        fake.controllers[0]["spec"]["replicas"] = 99
    elif fault == "pdb":
        fake.pdbs[0]["spec"]["minAvailable"] = 0
    elif fault == "kwok":
        fake.nodes["kwok-node-0"]["status"]["conditions"][0]["status"] = "False"
    elif fault == "healthy-pod":
        fake.pod("kwok-node-0")["metadata"]["uid"] = identity(fault)
    elif fault in ("family-quota", "regional-quota"):
        fake.usage[0 if fault == "family-quota" else 1]["limit"] = "1.0"
    elif fault == "sku":
        fake.skus[0]["restrictions"] = [{"type": "Location", "reasonCode": "NotAvailableForSubscription"}]
    elif fault == "extensions-none":
        fake.views[(base.DEFAULT_VMSS, "1")]["extensions"] = None
    elif fault == "aggregate-plural":
        raw = fake.scale_views[base.DEFAULT_VMSS]
        raw["virtualMachines"] = raw.pop("virtualMachine")
    elif fault == "pool-count":
        fake.pools["default"]["count"] = 3
    elif fault == "fips":
        fake.pools["default"]["enableFips"] = True
    elif fault == "image":
        fake.nodes[modern.RETAINED]["metadata"]["labels"]["kubernetes.azure.com/node-image-version"] = ""
        # The original default image and its workers must agree, not merely hash a new empty value.
    elif fault == "foreign-hold":
        fake.nodes[modern.SOURCE]["metadata"]["annotations"][modern.JOURNAL_KEY] = "someone-else"
    elif fault == "unexpected-cniv5":
        fake.add_pool("cniv5", NEW_VMSS, 2, modern.SKU, "System", 110)
    elif fault == "journal":
        name = f"modern-cni-{modern.digest(fake.plan)[:16]}"
        fake.journals[name] = {"metadata": meta(name, "kube-system"), "data": {}}
    else:
        fake.peer_fault = "id" if fault == "peer-id" else "name"
    with pytest.raises(modern.workers.ReconcileError):
        run(environment, execute=True)
    assert not fake.writes and not fake.deletes and not fake.evictions


@pytest.mark.parametrize("field,value", [
    ("repaired", False), ("phase1_only", False), ("workloads_ready", True), ("execute", False),
    ("plan_sha256", "f" * 64), ("controller_pins", {}), ("pdb_pins", {}),
    ("pod_moves", []), ("modern_baseline_delta", {}),
])
def test_monitoring_receipt_is_not_a_health_waiver(environment, field, value):
    args, _, fake, checkpoint = environment
    checkpoint[field] = value
    Path(args.modern_prom_checkpoint).write_text(json.dumps(checkpoint), encoding="utf-8")
    with pytest.raises(modern.workers.ReconcileError):
        run(environment, execute=True)
    assert not fake.writes


def test_original_plan_hash_is_not_rewritten_for_modern_identities(environment):
    args, plan, fake, _ = environment
    plan["mock_pod_uids"]["kwok-node-71"] = identity("unapproved-mock")
    Path(args.plan_file).write_text(json.dumps(plan), encoding="utf-8")
    with pytest.raises(modern.workers.ReconcileError, match="original approved plan SHA256"):
        run(environment, execute=True)
    assert not fake.commands


def test_more_than_25_current_healthy_source_agents_rejects_without_splitting(environment):
    _, _, fake, _ = environment
    for index in range(71, 82):
        pod = fake.pod(f"kwok-node-{index}")
        pod["status"] = pod_status(address=fake.assign_ip(modern.SOURCE))
    with pytest.raises(modern.workers.ReconcileError, match="cap 25"):
        run(environment, execute=True)
    assert not fake.writes


@pytest.mark.parametrize("fault", ["old-time", "wrong-uid", "other-source", "no-cns"])
def test_current_uid_bound_source_cns_evidence_required(environment, fault, monkeypatch):
    _, _, fake, _ = environment
    original = fake.kubernetes

    def kubernetes(command):
        result = original(command)
        if "events" in command:
            row = result["items"][0]
            if fault == "old-time":
                row["lastTimestamp"] = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
            elif fault == "wrong-uid":
                row["involvedObject"]["uid"] = identity("other-event-uid")
            elif fault == "other-source":
                row["source"]["host"] = modern.RETAINED
            else:
                row["message"] = "image pull failure"
        return result

    monkeypatch.setattr(fake, "kubernetes", kubernetes)
    with pytest.raises(modern.workers.ReconcileError):
        run(environment, execute=True)
    assert not fake.writes


@pytest.mark.parametrize("fault", ["no_growth", "no_version", "http_error", "probe_create_error", "probe_delete_error"])
def test_ready_or_assigned_ips_do_not_waive_real_growth_and_cleanup(environment, fault, monkeypatch):
    _, _, fake, _ = environment
    setattr(fake, fault, True)
    clock = [modern.time.monotonic()]
    monkeypatch.setattr(modern.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(modern.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + max(seconds, 610)))
    with pytest.raises(modern.workers.ReconcileError if fault != "probe_delete_error" else modern.mocks.RecoveryError):
        run(environment, execute=True)
    assert not any(name.startswith("kwok-node") for name, _ in fake.deletes)
    assert not any(command[1:4] == ["aks", "nodepool", "delete-machines"] for command in fake.writes)
    receipt = fake.receipt()
    assert receipt["probe_cleanup_pending"] and receipt["journal_retained"] and not receipt["rollback_attempted"]
    assert sum(command[1:4] == ["aks", "nodepool", "add"] for command in fake.writes) == 1


@pytest.mark.parametrize("fault", ["pool_error", "delete_error", "eviction_error", "retirement_error"])
def test_ambiguous_actions_are_single_attempt_and_have_no_rollback(environment, fault):
    _, _, fake, _ = environment
    setattr(fake, fault, True)
    with pytest.raises((modern.workers.ReconcileError, modern.mocks.RecoveryError)):
        run(environment, execute=True)
    receipt = fake.receipt()
    assert not receipt["success"] and not receipt["repaired"] and not receipt["workloads_ready"]
    assert len(fake.journals) == 1 and receipt["journal_retained"]
    assert any(row["ambiguous"] and row["accepted"] is None for row in receipt["actions"].values())
    assert len(fake.deletes) == len(set(fake.deletes))
    assert sum(command[1:4] == ["aks", "nodepool", "add"] for command in fake.writes) == 1
    if fault in ("pool_error", "delete_error", "eviction_error"):
        assert modern.SOURCE in fake.nodes
    if fault == "delete_error":
        assert len([name for name, _ in fake.deletes if name.startswith("kwok-node")]) == 1
    if fault == "retirement_error":
        assert sum(command[1:4] == ["aks", "nodepool", "delete-machines"] for command in fake.writes) == 1


@pytest.mark.parametrize("fault", ["retained-ready", "source-ready", "kwok", "new-uid", "new-nc", "controller", "pdb"])
def test_each_mock_exchange_preserves_all_other_health_and_identity(environment, fault):
    _, _, fake, _ = environment

    def changed(_old, _new):
        if fault in ("retained-ready", "source-ready"):
            fake.pod("kwok-node-0" if fault == "retained-ready" else "kwok-node-56")["status"] = pod_status(False)
        elif fault == "kwok":
            fake.nodes["kwok-node-10"]["metadata"]["uid"] = identity("kwok-replacement")
        elif fault == "new-uid":
            fake.nodes[NEW[0]]["metadata"]["uid"] = identity("replacement-node")
        elif fault == "new-nc":
            fake.nncs[NEW[0]]["status"]["networkContainers"][0]["id"] = identity("replacement-nc")
        elif fault == "controller":
            fake.controllers[0]["spec"]["replicas"] = 101
        else:
            fake.pdbs[0]["spec"]["minAvailable"] = 0

    fake.after_delete = changed
    with pytest.raises(modern.workers.ReconcileError):
        run(environment, execute=True)
    assert len([name for name, _ in fake.deletes if name.startswith("kwok-node")]) == 1
    assert modern.SOURCE in fake.nodes and not fake.evictions


def test_unbound_new_pending_is_never_deleted_again(environment):
    _, _, fake, _ = environment
    fake.replacement_ready = False
    with pytest.raises(modern.workers.ReconcileError, match="did not converge"):
        run(environment, execute=True)
    assert len([name for name, _ in fake.deletes if name.startswith("kwok-node")]) == 1
    assert not fake.evictions


def test_current_ready_original_pending_is_protected_until_all_pending_are_ready(environment):
    _, _, fake, _ = environment
    pod = fake.pod("kwok-node-71")
    pod["status"] = pod_status(address=fake.assign_ip(modern.SOURCE))
    summary = run(environment, execute=True)
    assert summary["naturally_ready_pending_uids"]["kwok-node-71"] == fake.plan["mock_pod_uids"]["kwok-node-71"]
    assert [row["phase"] for row in summary["pod_moves"]].count("pending") == 28
    assert [row["phase"] for row in summary["pod_moves"]].count("healthy") == 16
    assert len(summary["original_pending_plan"]) == 29


@pytest.mark.parametrize("fault", ["framework", "fleet", "peers"])
def test_final_postproof_never_claims_success_on_regression(environment, fault):
    _, _, fake, _ = environment

    def changed(command):
        if modern.SOURCE in fake.nodes:
            return
        if command[0] == "az" and command[1:3] == ["account", "show"]:
            if fault == "fleet":
                fake.members[0]["meshProperties"]["status"]["state"] = "Connecting"
            elif fault == "peers":
                fake.peer_fault = "connected"
            else:
                fake.pod(fake.framework_moves[0]["pod_name"] + "-modern")["status"] = pod_status(False)

    fake.hook = changed
    with pytest.raises(modern.workers.ReconcileError):
        run(environment, execute=True)
    assert modern.SOURCE not in fake.nodes
    assert not fake.receipt()["success"] and not fake.receipt()["workloads_ready"]
    assert not fake.receipt()["modern_cni"]["completed"]
    assert "baseline_pool_layout" not in fake.receipt()


def test_final_journal_failure_cannot_publish_a_completed_baseline_layout(environment, monkeypatch):
    _, _, fake, _ = environment
    original = fake.kubernetes
    attempted = []

    def kubernetes(command):
        result = original(command)
        if "patch" in command and "configmap" in command:
            operations = json.loads(fake.value(command, "-p"))
            data = next(row["value"] for row in operations if row["path"] == "/data")
            if json.loads(data["receipt"])["status"] == "final-proofs-complete":
                before = fake.receipt()
                assert not before["success"] and not before["repaired"]
                assert not before["modern_cni"]["completed"]
                attempted.append(True)
                raise modern.workers.ReconcileError("ambiguous final journal publication")
        return result

    monkeypatch.setattr(fake, "kubernetes", kubernetes)
    with pytest.raises(modern.workers.ReconcileError, match="final journal"):
        run(environment, execute=True)
    summary = fake.receipt()
    assert attempted == [True] and modern.SOURCE not in fake.nodes
    assert not summary["success"] and not summary["repaired"] and not summary["modern_cni"]["completed"]
    assert summary["modern_cni"]["source_retired"] is True
    assert "baseline_pool_layout" not in summary
    assert summary["uncommitted_baseline_pool_layout"]["expected_total_pool_count"] == 202


def test_scope_cli_timeout_and_paths_are_explicit(environment):
    args, _, fake, _ = environment
    parsed = modern.parse_args([
        "--plan-file", args.plan_file, "--modern-prom-checkpoint", args.modern_prom_checkpoint,
        "--resource-group", base.RESOURCE_GROUP, "--confirm-resource-group", base.RESOURCE_GROUP,
        "--expected-subscription", base.SUBSCRIPTION, "--expected-region", base.REGION,
        "--expected-tfvars-sha", "a" * 64, "--summary-file", "fresh.json", "--kubeconfig", "private-config",
    ])
    assert not parsed.execute and parsed.timeout_seconds == 5400 and parsed.per_pod_ready_seconds == 180
    args.summary_file = args.modern_prom_checkpoint
    with pytest.raises(modern.workers.ReconcileError, match="distinct"):
        run(environment, execute=True)
    assert not fake.commands


def test_quota_decimal_and_python310_timestamp_normalization():
    assert modern._number("1000.0", "quota") == 1000
    for value in ("NaN", "Infinity", "-1", "1.5", None, True):
        with pytest.raises(modern.workers.ReconcileError):
            modern._number(value, "quota")
    stamp = base.timestamp("2026-09-12T08:42:20.6101413Z", "Azure operation")
    assert stamp.microsecond == 610141


@pytest.mark.parametrize("fault", ["pvc", "ephemeral", "unmanaged", "foreign-owner", "overlapping-workload"])
def test_unsupported_source_work_or_overlap_rejected_before_pool_creation(environment, fault):
    _, _, fake, _ = environment
    pod = fake.pod("metrics-server-1")
    if fault == "pvc":
        pod["spec"]["volumes"] = [{"name": "disk", "persistentVolumeClaim": {"claimName": "foreign"}}]
    elif fault == "ephemeral":
        pod["spec"]["volumes"] = [{"name": "disk", "ephemeral": {"volumeClaimTemplate": {}}}]
    elif fault == "unmanaged":
        pod["metadata"]["ownerReferences"] = []
    elif fault == "foreign-owner":
        pod["metadata"]["ownerReferences"][0]["uid"] = identity("foreign-owner")
    else:
        pod["spec"]["nodeName"] = "kwok-node-1"
    with pytest.raises(modern.workers.ReconcileError):
        run(environment, execute=True)
    assert not fake.writes


@pytest.mark.parametrize("fault", [
    "fips", "image", "vm-id", "node-uid", "nc", "provider", "subnet", "max-pods", "extensions",
])
def test_actual_created_pool_and_worker_faults_prevent_any_mock_delete(environment, fault, monkeypatch):
    _, _, fake, _ = environment
    changed = [False]
    clock = [modern.time.monotonic()]
    monkeypatch.setattr(modern.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(modern.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + max(seconds, 1300)))

    def change(_command):
        if "cniv5" not in fake.pools or changed[0]:
            return
        changed[0] = True
        if fault == "fips":
            fake.pools["cniv5"]["enableFips"] = True
        elif fault == "image":
            fake.nodes[NEW[0]]["metadata"]["labels"]["kubernetes.azure.com/node-image-version"] = "invented-image"
        elif fault == "vm-id":
            fake.instances[NEW_VMSS][0]["vmId"] = modern.RETAINED_VM_ID
        elif fault == "node-uid":
            fake.nodes[NEW[0]]["metadata"]["uid"] = base.REAL_UIDS[modern.RETAINED]
        elif fault == "nc":
            fake.nncs[NEW[0]]["status"]["networkContainers"][0]["id"] = base.SOURCE_NC
        elif fault == "provider":
            fake.nodes[NEW[0]]["spec"]["providerID"] = fake.nodes[modern.RETAINED]["spec"]["providerID"]
        elif fault == "subnet":
            fake.pools["cniv5"]["vnetSubnetId"] += "-wrong"
        elif fault == "max-pods":
            fake.pools["cniv5"]["maxPods"] = 250
        else:
            fake.views[(NEW_VMSS, "0")]["extensions"][0]["statuses"] = None

    fake.hook = change
    with pytest.raises(modern.workers.ReconcileError):
        run(environment, execute=True)
    assert changed[0] and not fake.deletes
    assert sum(command[1:4] == ["aks", "nodepool", "add"] for command in fake.writes) == 1


def test_only_owned_accepted_creating_operation_can_converge(environment, monkeypatch):
    _, _, fake, _ = environment
    polls = [0]
    original = fake.azure

    def azure(command):
        result = original(command)
        if command[1:4] == ["aks", "nodepool", "add"]:
            fake.pool_operations["cniv5"]["status"] = "InProgress"
            fake.pool_operations["cniv5"].pop("endTime")
            fake.pools["cniv5"]["provisioningState"] = "Creating"
            fake.scales[NEW_VMSS]["provisioningState"] = "Creating"
            fake.views[(NEW_VMSS, "0")]["extensions"][0]["statuses"] = None
        if command[1:4] == ["aks", "operation", "show-latest"] and "--nodepool-name" in command and fake.value(
            command, "--nodepool-name"
        ) == "cniv5":
            polls[0] += 1
            if polls[0] == 2:
                fake.pool_operations["cniv5"].update(status="Succeeded", endTime=now())
                fake.pools["cniv5"]["provisioningState"] = "Succeeded"
                fake.scales[NEW_VMSS]["provisioningState"] = "Succeeded"
                fake.views[(NEW_VMSS, "0")]["extensions"][0]["statuses"] = [{"code": "ProvisioningState/succeeded"}]
        return result

    monkeypatch.setattr(fake, "azure", azure)
    summary = run(environment, execute=True)
    assert polls[0] > 2 and summary["repaired"]
    assert summary["actions"]["pool-create"]["operation_name"] == "owned-pool-create"


def test_unrelated_azure_operation_after_acceptance_stops_without_retry(environment):
    _, _, fake, _ = environment

    def change(command):
        if (command[1:4] == ["aks", "operation", "show-latest"] and "--nodepool-name" in command
                and fake.value(command, "--nodepool-name") == "cniv5"):
            fake.pool_operations["cniv5"]["operationType"] = "PutManagedCluster"
            fake.pool_operations["cniv5"]["status"] = "InProgress"

    fake.hook = change
    with pytest.raises(modern.workers.ReconcileError, match="causally bound"):
        run(environment, execute=True)
    assert not fake.deletes
    assert sum(command[1:4] == ["aks", "nodepool", "add"] for command in fake.writes) == 1


@pytest.mark.parametrize("fault", ["memory", "stale", "pod-slots", "cpu", "rss"])
def test_actual_capacity_is_checked_before_first_mock_delete(environment, fault, monkeypatch):
    _, _, fake, _ = environment
    original = fake.kubernetes

    def kubernetes(command):
        result = original(command)
        if "--raw" in command and "/apis/metrics.k8s.io" in fake.value(command, "--raw"):
            if fault == "memory" and fake.value(command, "--raw").endswith("/nodes"):
                for row in result["items"]:
                    row["usage"]["memory"] = "26Gi"
            elif fault == "stale":
                for row in result["items"]:
                    row["timestamp"] = (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat()
            elif fault == "cpu" and fake.value(command, "--raw").endswith("/nodes"):
                for row in result["items"]:
                    row["usage"]["cpu"] = "7000m"
            elif fault == "rss" and fake.value(command, "--raw").endswith("/pods"):
                for row in result["items"]:
                    row["containers"][0]["usage"]["memory"] = "2Gi"
        if fault == "pod-slots" and "nodes" in command:
            for row in result["items"]:
                if row["metadata"]["name"] in NEW:
                    row["status"]["allocatable"]["pods"] = "10"
        return result

    monkeypatch.setattr(fake, "kubernetes", kubernetes)
    with pytest.raises(modern.workers.ReconcileError):
        run(environment, execute=True)
    assert not any(name.startswith("kwok-node") for name, _ in fake.deletes)


def test_pdb_identity_selector_status_and_server_authoritative_pending_eviction():
    pod = make_pod("metrics", "kube-system", modern.SOURCE, owner("ReplicaSet", "metrics", identity("owner")))
    pod["metadata"]["labels"] = {"app": "metrics"}
    pdb = {"metadata": meta("budget", "kube-system"),
           "spec": {"selector": {"matchExpressions": [{"key": "app", "operator": "In", "values": ["metrics"]}]}},
           "status": {"observedGeneration": 1, "disruptionsAllowed": 0, "currentHealthy": 1, "desiredHealthy": 1}}
    snapshot = {"pdbs": {"items": [pdb]}}
    with pytest.raises(modern.workers.ReconcileError, match="permit"):
        modern._pdb_allows(snapshot, pod)
    pod["status"] = pod_status(False)
    modern._pdb_allows(snapshot, pod, eviction=True)
    with pytest.raises(modern.workers.ReconcileError, match="permit"):
        modern._pdb_allows(snapshot, pod)
    pdb["status"]["observedGeneration"] = 0
    with pytest.raises(modern.workers.ReconcileError, match="observed"):
        modern._pdb_allows(snapshot, pod, eviction=True)


def test_preserved_native_default_configuration_pin_cannot_be_rebased(environment):
    args, _, fake, checkpoint = environment
    checkpoint["original_model_pins"] = {"pools": {"default": "b" * 64}}
    Path(args.modern_prom_checkpoint).write_text(json.dumps(checkpoint), encoding="utf-8")
    with pytest.raises(modern.workers.ReconcileError, match="native receipt"):
        run(environment, execute=True)
    assert not fake.writes


@pytest.mark.parametrize("reject", [False, True])
def test_real_eviction_primitive_has_one_uid_preconditioned_policy_request(monkeypatch, reject):
    calls, closed = [], []
    api_client = SimpleNamespace(close=lambda: closed.append(True))

    def eviction(**kwargs):
        calls.append(kwargs)
        if reject:
            raise modern.workers.ReconcileError("server denied the PDB eviction")

    monkeypatch.setattr(modern.config, "new_client_from_config", lambda **_kwargs: api_client)
    monkeypatch.setattr(modern.client, "CoreV1Api",
                        lambda _api: SimpleNamespace(create_namespaced_pod_eviction=eviction))
    cluster = SimpleNamespace(kubeconfig="private-not-opened", context=base.CLUSTER)
    if reject:
        with pytest.raises(modern.workers.ReconcileError):
            modern.evict_pod_with_uid_precondition(
                cluster, namespace="kube-system", name="metrics-server", pod_uid="pinned-uid", timeout_seconds=45,
            )
    else:
        modern.evict_pod_with_uid_precondition(
            cluster, namespace="kube-system", name="metrics-server", pod_uid="pinned-uid", timeout_seconds=45,
        )
    assert len(calls) == 1 and closed == [True]
    assert calls[0]["body"].api_version == "policy/v1"
    assert calls[0]["body"].delete_options.preconditions.uid == "pinned-uid"
    assert calls[0]["_request_timeout"] == (45, 45)


def prepare_owned_creation(environment):
    args, plan, fake, checkpoint = environment
    args.execute = True
    modern.validate_args(args)
    summary = {"actions": {}, "pod_moves": [], "status": "validating",
               "success": False, "repaired": False, "workloads_ready": False}
    operation = modern.ModernRecovery(args, plan, checkpoint, summary, fake.run, fake.delete, fake.evict)
    operation.preflight()
    operation.acquire()
    operation.hold_source()
    return operation


def install_initializing_pool(fake, monkeypatch, aggregate_form="empty"):
    saved = {}
    original = fake.azure

    def azure(command):
        result = original(command)
        if command[1:4] == ["aks", "nodepool", "add"]:
            saved.update(
                instances=copy.deepcopy(fake.instances[NEW_VMSS]),
                nodes={name: copy.deepcopy(fake.nodes[name]) for name in NEW},
                nncs={name: copy.deepcopy(fake.nncs[name]) for name in NEW},
                pods=copy.deepcopy([pod for pod in fake.pods if pod["spec"].get("nodeName") in NEW]),
            )
            fake.pool_operations["cniv5"]["status"] = "InProgress"
            fake.pool_operations["cniv5"].pop("endTime")
            fake.pools["cniv5"]["provisioningState"] = "Creating"
            fake.scales[NEW_VMSS]["provisioningState"] = "Creating"
            fake.instances[NEW_VMSS] = []
            for name in NEW:
                fake.nodes.pop(name)
                fake.nncs.pop(name)
            fake.pods = [pod for pod in fake.pods if pod["spec"].get("nodeName") not in NEW]
            if aggregate_form == "empty":
                fake.scale_views[NEW_VMSS]["virtualMachine"]["statusesSummary"] = []
            else:
                fake.scale_views[NEW_VMSS].pop("virtualMachine")
        return result

    monkeypatch.setattr(fake, "azure", azure)
    return saved


def register_initializing_node(fake, saved, name, *, empty_containers=False):
    index = NEW.index(name)
    fake.instances[NEW_VMSS].append(copy.deepcopy(saved["instances"][index]))
    fake.nodes[name] = copy.deepcopy(saved["nodes"][name])
    fake.nncs[name] = copy.deepcopy(saved["nncs"][name])
    fake.pods.extend(copy.deepcopy([pod for pod in saved["pods"] if pod["spec"].get("nodeName") == name]))
    if empty_containers:
        fake.nncs[name]["status"] = {"networkContainers": []}
    else:
        fake.nncs[name].pop("status")


def test_owned_cordon_taint_propagation_is_only_normalized_for_comparison(environment):
    _, _, fake, _ = environment
    operation = prepare_owned_creation(environment)
    source = fake.nodes[modern.SOURCE]
    assert CORDON in source["spec"]["taints"]
    writes_before = len(fake.writes)
    operation.guard(operation.snapshot())
    assert CORDON in source["spec"]["taints"]
    source["spec"]["taints"].remove(CORDON)
    operation.guard(operation.snapshot())
    source["spec"]["taints"].append({**CORDON, "value": "", "timeAdded": now()})
    operation.guard(operation.snapshot())
    assert len(fake.writes) == writes_before
    assert source["spec"]["unschedulable"] is True
    assert source["metadata"]["annotations"][modern.maintenance.HOLD_ANNOTATION] == operation.token


@pytest.mark.parametrize("name", [modern.SOURCE, modern.RETAINED, PROM])
def test_cordon_taint_on_unowned_node_is_never_normalized(environment, name):
    _, _, fake, _ = environment
    fake.nodes[name]["spec"]["taints"].append(dict(CORDON))
    with pytest.raises(modern.workers.ReconcileError):
        run(environment, execute=True)
    assert not fake.writes


@pytest.mark.parametrize("fault", ["retained", "other-taint", "wrong-effect", "wrong-hold"])
def test_cordon_normalization_does_not_hide_unrelated_owned_source_drift(environment, fault):
    _, _, fake, _ = environment
    operation = prepare_owned_creation(environment)
    if fault == "retained":
        fake.nodes[modern.RETAINED]["spec"]["taints"].append(dict(CORDON))
    elif fault == "other-taint":
        fake.nodes[modern.SOURCE]["spec"]["taints"].append({"key": "foreign", "effect": "NoSchedule"})
    elif fault == "wrong-effect":
        fake.nodes[modern.SOURCE]["spec"]["taints"].remove(CORDON)
        fake.nodes[modern.SOURCE]["spec"]["taints"].append({**CORDON, "effect": "NoExecute"})
    else:
        fake.nodes[modern.SOURCE]["metadata"]["annotations"][modern.maintenance.HOLD_ANNOTATION] = "other-owner"
    with pytest.raises(modern.workers.ReconcileError):
        operation.create_pool()
    assert not any(command[0] == "az" for command in fake.writes)


@pytest.mark.parametrize("aggregate_form", ["empty", "missing"])
@pytest.mark.parametrize("empty_containers", [False, True])
def test_owned_initialization_waits_for_actual_vms_and_populated_nncs(
    environment, monkeypatch, aggregate_form, empty_containers,
):
    _, _, fake, _ = environment
    operation = prepare_owned_creation(environment)
    saved = install_initializing_pool(fake, monkeypatch, aggregate_form)
    waits = []

    def progress(_seconds):
        waits.append(len(waits) + 1)
        assert not fake.deletes and not fake.evictions and not operation.fresh
        assert CORDON in fake.nodes[modern.SOURCE]["spec"]["taints"]
        assert all(modern.uid(fake.pod(name)) == original_uid and base.pod_ready(fake.pod(name))
                   for name, original_uid in fake.original_healthy.items())
        assert {name: fake.nodes[name]["metadata"]["uid"] for name in fake.plan["kwok_node_uids"]} == fake.plan["kwok_node_uids"]
        assert len(fake.instances[NEW_VMSS]) == len(waits) - 1
        if len(waits) == 1:
            assert set(operation.summary["last_arm_proof"]["vm_ids"]) == {modern.SOURCE, modern.RETAINED, PROM}
            register_initializing_node(fake, saved, NEW[0], empty_containers=empty_containers)
        elif len(waits) == 2:
            register_initializing_node(fake, saved, NEW[1], empty_containers=empty_containers)
            fake.pool_operations["cniv5"].update(status="Succeeded", endTime=now())
            fake.pools["cniv5"]["provisioningState"] = "Succeeded"
            fake.scales[NEW_VMSS]["provisioningState"] = "Succeeded"
            fake.scale_views[NEW_VMSS]["virtualMachine"] = {
                "statusesSummary": [{"code": "ProvisioningState/succeeded", "count": 2}],
            }
        elif len(waits) == 3:
            # ARM can finish before the node-network controller publishes status.
            assert operation.models("creating")[0] is True
            for name in NEW:
                fake.nncs[name] = copy.deepcopy(saved["nncs"][name])
        else:
            raise AssertionError("Unexpected extra initialization polling")

    monkeypatch.setattr(modern.time, "sleep", progress)
    operation.create_pool()
    assert waits == [1, 2, 3] and set(operation.fresh) == set(NEW)
    assert not fake.deletes and not fake.evictions
    assert sum(command[1:4] == ["aks", "nodepool", "add"] for command in fake.writes) == 1
    assert all(row["network_container_id"] for row in operation.fresh.values())
    assert not operation.summary["success"]


@pytest.mark.parametrize("fault", [
    "extra-vm", "wrong-instance-id", "wrong-scope", "wrong-owner", "failed-scale-view",
    "failed-aggregate", "failed-vm", "failed-extension", "protected-aggregate",
    "protected-uid", "unbound-operation", "already-terminal",
])
def test_owned_initialization_never_waives_bad_arm_or_protected_identity(environment, monkeypatch, fault):
    _, _, fake, _ = environment
    operation = prepare_owned_creation(environment)
    saved = install_initializing_pool(fake, monkeypatch)
    applied = []

    def corrupt(command):
        if not saved or applied or command[1:3] != ["account", "show"]:
            return
        applied.append(True)
        if fault == "extra-vm":
            fake.instances[NEW_VMSS] = copy.deepcopy(saved["instances"]) + [copy.deepcopy(saved["instances"][0])]
        elif fault == "wrong-instance-id":
            fake.instances[NEW_VMSS] = [copy.deepcopy(saved["instances"][0])]
            fake.instances[NEW_VMSS][0]["instanceId"] = "2"
        elif fault == "wrong-scope":
            fake.scales[NEW_VMSS]["id"] = fake.scales[NEW_VMSS]["id"].replace(base.NODE_GROUP, "unowned-group")
        elif fault == "wrong-owner":
            fake.scales[NEW_VMSS]["tags"]["aks-managed-poolName"] = "foreign"
        elif fault == "failed-scale-view":
            fake.scale_views[NEW_VMSS]["statuses"] = [{"code": "ProvisioningState/failed"}]
        elif fault == "failed-aggregate":
            fake.scale_views[NEW_VMSS]["virtualMachine"]["statusesSummary"] = [
                {"code": "ProvisioningState/failed", "count": 1},
            ]
        elif fault in ("failed-vm", "failed-extension"):
            fake.instances[NEW_VMSS] = [copy.deepcopy(saved["instances"][0])]
            if fault == "failed-vm":
                fake.instances[NEW_VMSS][0]["provisioningState"] = "Failed"
            else:
                fake.views[(NEW_VMSS, "0")]["extensions"][0]["statuses"] = [{"code": "ProvisioningState/failed"}]
        elif fault == "protected-aggregate":
            fake.scale_views[base.DEFAULT_VMSS]["virtualMachine"]["statusesSummary"] = []
        elif fault == "protected-uid":
            fake.nodes[modern.RETAINED]["metadata"]["uid"] = identity("changed-retained")
        elif fault == "unbound-operation":
            fake.pool_operations["cniv5"]["startTime"] = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        else:
            fake.pool_operations["cniv5"].update(status="Succeeded", endTime=now())

    fake.hook = corrupt
    with pytest.raises(modern.workers.ReconcileError):
        operation.create_pool()
    assert applied == [True] and not fake.deletes and not fake.evictions
    assert not operation.fresh
    assert sum(command[1:4] == ["aks", "nodepool", "add"] for command in fake.writes) == 1


@pytest.mark.parametrize("fault", [
    "owner-uid", "owner-name", "resource-uid", "namespace", "provider", "node-uid", "unlisted-vm", "source-status",
])
def test_registering_nnc_requires_exact_node_vm_and_resource_ownership(environment, monkeypatch, fault):
    _, _, fake, _ = environment
    operation = prepare_owned_creation(environment)
    original = fake.azure

    def azure(command):
        result = original(command)
        if command[1:4] == ["aks", "nodepool", "add"]:
            fake.nncs[NEW[0]].pop("status")
            if fault == "owner-uid":
                fake.nncs[NEW[0]]["metadata"]["ownerReferences"][0]["uid"] = identity("foreign")
            elif fault == "owner-name":
                fake.nncs[NEW[0]]["metadata"]["ownerReferences"][0]["name"] = modern.SOURCE
            elif fault == "resource-uid":
                fake.nncs[NEW[0]]["metadata"]["uid"] = ""
            elif fault == "namespace":
                fake.nncs[NEW[0]]["metadata"]["namespace"] = "foreign"
            elif fault == "provider":
                fake.nodes[NEW[0]]["spec"]["providerID"] = fake.nodes[modern.RETAINED]["spec"]["providerID"]
            elif fault == "node-uid":
                fake.nodes[NEW[0]]["metadata"]["uid"] = base.REAL_UIDS[modern.RETAINED]
            elif fault == "unlisted-vm":
                fake.instances[NEW_VMSS] = [fake.instances[NEW_VMSS][1]]
            else:
                fake.nncs[modern.SOURCE].pop("status")
        return result

    monkeypatch.setattr(fake, "azure", azure)
    with pytest.raises(modern.workers.ReconcileError):
        operation.create_pool()
    assert not fake.deletes and not fake.evictions and not operation.fresh


def test_qualified_nnc_and_vmss_cannot_return_to_initialization(environment):
    _, _, fake, _ = environment
    operation = prepare_owned_creation(environment)
    operation.create_pool()
    fake.nncs[NEW[0]].pop("status")
    with pytest.raises(modern.workers.ReconcileError, match="networkContainers"):
        operation.guard(operation.snapshot(), "creating")
    fake.nncs[NEW[0]]["status"] = {
        "assignedIPCount": 16, "networkContainers": [{"id": identity(f"nc/{NEW[0]}"), "version": 10, "ipAssignments": []}],
    }
    fake.pools["cniv5"]["provisioningState"] = "Creating"
    fake.scales[NEW_VMSS]["provisioningState"] = "Creating"
    fake.pool_operations["cniv5"].update(status="InProgress", endTime=None)
    fake.scale_views[NEW_VMSS]["virtualMachine"]["statusesSummary"] = []
    with pytest.raises(modern.workers.ReconcileError, match="outside owned initialization"):
        operation.models("creating")
    assert not fake.deletes


def test_owned_initialization_keeps_the_existing_deadline_and_never_retries_add(environment, monkeypatch):
    _, _, fake, _ = environment
    clock = [modern.time.monotonic()]
    monkeypatch.setattr(modern.time, "monotonic", lambda: clock[0])
    operation = prepare_owned_creation(environment)
    install_initializing_pool(fake, monkeypatch)
    monkeypatch.setattr(modern.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + max(seconds, 1300)))
    with pytest.raises(modern.workers.ReconcileError, match="did not converge"):
        operation.create_pool()
    assert not operation.fresh and not fake.deletes and not fake.evictions
    assert sum(command[1:4] == ["aks", "nodepool", "add"] for command in fake.writes) == 1
