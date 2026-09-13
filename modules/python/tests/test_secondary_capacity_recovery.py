"""Offline stateful tests for the build-80022 secondary capacity protocol."""

# pylint: disable=protected-access,too-many-lines,attribute-defined-outside-init

from __future__ import annotations

import copy
import importlib.util
import json
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone

import pytest


MODULE_DIR = Path(__file__).resolve().parents[1] / "clusterloader2" / "clustermesh-scale"
SPEC = importlib.util.spec_from_file_location(
    "secondary_capacity_recovery", MODULE_DIR / "secondary_capacity_recovery.py",
)
recovery = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = recovery
sys.path.insert(0, str(MODULE_DIR))
try:
    SPEC.loader.exec_module(recovery)
finally:
    sys.path.pop(0)


def uid(name):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, name))


def metadata(name, namespace="", row_uid=None):
    result = {
        "name": name, "uid": row_uid or uid(f"{namespace}/{name}"),
        "resourceVersion": "1", "labels": {}, "annotations": {},
    }
    if namespace:
        result["namespace"] = namespace
    return result


def owner(kind, name, row_uid):
    return {"kind": kind, "name": name, "uid": row_uid, "controller": True}


def ready_status(address):
    return {
        "phase": "Running", "podIP": address,
        "conditions": [{"type": "Ready", "status": "True"}],
        "containerStatuses": [{
            "name": "container", "ready": True, "started": True,
            "restartCount": 0, "state": {"running": {}},
        }],
    }


def node(name, row_uid, pool_name, provider, *, ready=True):
    return {
        "kind": "Node",
        "metadata": {
            **metadata(name, row_uid=row_uid),
            "labels": {
                "agentpool": pool_name, "kubernetes.azure.com/agentpool": pool_name,
                "kubernetes.io/os": "linux",
                "kubernetes.azure.com/cluster": "node-rg",
                "kubernetes.azure.com/node-image-version": "AKSUbuntu-2404containerd-202609.03.1",
                **({"prometheus": "true"} if pool_name == "promv5" else {}),
            },
        },
        "spec": {
            "providerID": provider, "taints": [] if ready else [{
                "key": "node.kubernetes.io/unreachable", "effect": "NoSchedule",
            }], "unschedulable": False,
        },
        "status": {
            "conditions": [{
                "type": "Ready", "status": "True" if ready else "Unknown",
                "lastHeartbeatTime": "2026-09-13T06:00:00Z",
            }],
            "allocatable": {"cpu": "7820m", "memory": "28Gi", "pods": "250"},
            "nodeInfo": {
                "bootID": uid(f"boot/{name}"), "kubeletVersion": "v1.35.7",
                "operatingSystem": "linux", "osImage": "Ubuntu 24.04.3 LTS",
            },
        },
    }


def nnc(name, node_uid, prefix, *, count=128, version=4):
    addresses = [f"10.{prefix // 200 + 1}.{prefix % 200}.{index}" for index in range(1, count + 1)]
    return {
        "kind": "NodeNetworkConfig",
        "metadata": {
            **metadata(name, "kube-system"),
            "ownerReferences": [owner("Node", name, node_uid)],
        },
        "spec": {"requestedIPCount": count},
        "status": {
            "assignedIPCount": count,
            "networkContainers": [{
                "id": uid(f"nc/{name}"), "version": version,
                "ipAssignments": [{"ip": address} for address in addresses],
            }],
        },
    }


def daemon_pod(daemon, node_name, address):
    daemon_uid = uid(f"daemon/{daemon}")
    return {
        "kind": "Pod",
        "metadata": {
            **metadata(f"{daemon}-{node_name}", "kube-system"),
            "labels": {"k8s-app": daemon},
            "ownerReferences": [owner("DaemonSet", daemon, daemon_uid)],
        },
        "spec": {
            "nodeName": node_name, "hostNetwork": True,
            "containers": [{"name": daemon, "image": "pinned"}],
        },
        "status": ready_status(address),
    }


def pool(name, count, mode, vm_size, role):
    return {
        "id": (
            f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{recovery.RESOURCE_GROUP}/"
            f"providers/Microsoft.ContainerService/managedClusters/clustermesh-{role[5:]}/agentPools/{name}"
        ),
        "name": name, "count": count, "mode": mode, "vmSize": vm_size,
        "maxPods": 110 if name == "default" else 250,
        "osType": "Linux", "osSku": "Ubuntu", "osDiskType": "Managed",
        "osDiskSizeGb": 256, "kubeletDiskType": "OS",
        "enableAutoScaling": False, "enableFips": False,
        "enableEncryptionAtHost": False, "enableNodePublicIp": False,
        "nodeLabels": {}, "nodeTaints": None, "availabilityZones": None,
        "kubeletConfig": None, "linuxOsConfig": None,
        "vnetSubnetId": (
            f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{recovery.RESOURCE_GROUP}/"
            f"providers/Microsoft.Network/virtualNetworks/vnet/subnets/{role}-node"
        ),
        "podSubnetId": (
            f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{recovery.RESOURCE_GROUP}/"
            f"providers/Microsoft.Network/virtualNetworks/vnet/subnets/{role}-pod"
        ),
        "orchestratorVersion": "1.35", "currentOrchestratorVersion": recovery.PATCH,
        "provisioningState": "Succeeded", "powerState": {"code": "Running"},
        "nodeImageVersion": "AKSUbuntu-2404containerd-202609.03.1",
        "upgradeSettings": {
            "maxSurge": "10%", "maxUnavailable": "0",
            "drainTimeoutInMinutes": None, "nodeSoakDurationInMinutes": None,
        },
    }


def vmss(role, pool_name, vmss_name, count, state):
    return {
        "id": (
            f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/"
            f"MC_{recovery.RESOURCE_GROUP}_clustermesh-{role[5:]}_{recovery.REGION}/"
            f"providers/Microsoft.Compute/virtualMachineScaleSets/{vmss_name}"
        ),
        "name": vmss_name, "location": recovery.REGION,
        "orchestrationMode": "Uniform", "provisioningState": state,
        "sku": {"name": "Standard_D8_v3", "capacity": count, "tier": "Standard"},
        "tags": {"aks-managed-poolName": pool_name},
    }


def instance(vmss_row, instance_id, name, state):
    return {
        "id": f"{vmss_row['id']}/virtualMachines/{instance_id}",
        "instanceId": str(instance_id), "computerName": name,
        "vmId": uid(f"vm/{name}"), "provisioningState": state,
        "latestModelApplied": True,
    }


def failed_view(role):
    return {
        "statuses": [{
            "code": recovery.TERMINAL_CODES[role], "displayStatus": "Provisioning failed",
            "level": "Error", "message": "terminal known failure",
            "time": "2026-09-13T06:44:56+00:00",
        }, {"code": "PowerState/running", "level": "Info"}],
        "vmAgent": {"statuses": [{
            "code": "ProvisioningState/Unavailable", "displayStatus": "Not Ready",
            "level": "Warning", "message": "guest unavailable",
            "time": datetime.now(timezone.utc).isoformat(),
        }]},
        "extensions": [{"name": "vmssCSE", "statuses": None}],
    }


def succeeded_view():
    return {
        "statuses": [
            {"code": "ProvisioningState/succeeded", "level": "Info"},
            {"code": "PowerState/running", "level": "Info"},
        ],
        "vmAgent": {"statuses": [{
            "code": "ProvisioningState/succeeded", "displayStatus": "Ready",
            "time": datetime.now(timezone.utc).isoformat(),
        }]},
        "extensions": [{
            "name": "vmssCSE",
            "statuses": [{"code": "ProvisioningState/succeeded"}],
        }],
    }


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def build_role(root, role, cluster):
    settings = recovery.ROLE_SETTINGS[role]
    role_number = int(role.split("-")[1])
    source_pool = settings["source_pool"]
    default_count = 3 if role in ("mesh-79", "mesh-89") else 2
    default_vmss_name = f"aks-default-{role_number:08d}-vmss"
    prom_vmss_name = f"aks-prompool-{role_number + 100:08d}-vmss"
    if source_pool == "default":
        default_vmss_name = settings["failed_node"][:-6]
    else:
        prom_vmss_name = settings["failed_node"][:-6]
    pools = [
        pool("default", default_count, "System", "Standard_D8_v3", role),
        pool("prompool", 1, "User", "Standard_D8_v3", role),
    ]
    vmsses = [
        vmss(role, "default", default_vmss_name, default_count,
             "Failed" if source_pool == "default" else "Succeeded"),
        vmss(role, "prompool", prom_vmss_name, 1,
             "Failed" if source_pool == "prompool" else "Succeeded"),
    ]
    by_pool = {row["tags"]["aks-managed-poolName"]: row for row in vmsses}
    failed_instance = int(settings["failed_node"][-6:])
    if role == "mesh-51":
        default_ids = [0, 3]
    elif role == "mesh-66":
        default_ids = [0, 1]
    elif role == "mesh-79":
        default_ids = [1, 2, 3]
    else:
        default_ids = [1, 5, 7]
    instances = {
        "default": [
            instance(
                by_pool["default"], index, f"{default_vmss_name}{index:06d}",
                "Failed" if source_pool == "default" and index == failed_instance else "Succeeded",
            ) for index in default_ids
        ],
        "prompool": [
            instance(
                by_pool["prompool"], 0, f"{prom_vmss_name}000000",
                "Failed" if source_pool == "prompool" else "Succeeded",
            )
        ],
    }
    all_instances = [*instances["default"], *instances["prompool"]]
    nodes, networks, pods = [], [], []
    prefixes = {}
    for offset, row in enumerate(all_instances, 1):
        name = row["computerName"]
        is_failed = name == settings["failed_node"]
        pool_name = "default" if name.startswith(default_vmss_name) else "prompool"
        row_node = node(
            name, settings["failed_uid"] if is_failed else uid(f"node/{role}/{name}"),
            pool_name, "azure://" + row["id"], ready=not is_failed,
        )
        nodes.append(row_node)
        network = nnc(name, row_node["metadata"]["uid"], role_number * 10 + offset)
        networks.append(network)
        prefixes[name] = [
            entry["ip"] for entry in network["status"]["networkContainers"][0]["ipAssignments"]
        ]
        if not is_failed:
            pods.extend([
                daemon_pod("cilium", name, f"192.168.{role_number}.{offset}"),
                daemon_pod("azure-cns", name, f"192.169.{role_number}.{offset}"),
            ])
    for index in range(100):
        name = f"kwok-node-{index}"
        nodes.append({
            "kind": "Node",
            "metadata": {
                **metadata(name, row_uid=uid(f"kwok/{role}/{index}")),
                "labels": {"type": "kwok"},
            },
            "spec": {"taints": [{"key": "kwok-provider", "effect": "NoSchedule"}]},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]},
        })
    placements = []
    mock_counts = list(settings["mock_placements"])
    if role == "mesh-51":
        targets = [settings["failed_node"], all_instances[1]["computerName"]]
    elif role == "mesh-66":
        targets = [all_instances[0]["computerName"], settings["failed_node"]]
    elif role == "mesh-79":
        targets = [all_instances[1]["computerName"], settings["failed_node"], all_instances[2]["computerName"]]
    else:
        targets = [all_instances[0]["computerName"], all_instances[2]["computerName"],
                   all_instances[1]["computerName"]]
    for target, count in zip(targets, mock_counts):
        placements.extend([target] * count)
    used = {name: 0 for name in prefixes}
    for index, target in enumerate(placements):
        address = prefixes[target][used[target]]
        used[target] += 1
        deleting = target == settings["failed_node"] and settings["terminating_mocks"]
        pod = {
            "kind": "Pod",
            "metadata": {
                **metadata(f"kwok-node-{index}", "mock-clustermesh",
                           uid(f"mock/{role}/{index}")),
                "labels": {"app": "mock-cilium-agent"},
                "ownerReferences": [owner(
                    "StatefulSet", "kwok-node", uid(f"mock-controller/{role}"),
                )],
                **({"deletionTimestamp": "2026-09-13T06:00:00Z"} if deleting else {}),
            },
            "spec": {
                "nodeName": target,
                "containers": [{"name": "mock", "image": "pinned"}],
            },
            "status": ready_status(address),
        }
        pods.append(pod)
    directory = root / role
    write_json(directory / "summary.json", {
        "role": role, "read_only": True, "cluster_id": cluster["id"],
        "reads": [], "unready_cilium": [{"node": settings["failed_node"]}],
    })
    write_json(directory / "nodes.json", {"kind": "NodeList", "items": nodes})
    write_json(directory / "pods.json", {"kind": "PodList", "items": pods})
    write_json(directory / "nnc.json", {"kind": "NodeNetworkConfigList", "items": networks})
    write_json(directory / "pdbs.json", {"kind": "PodDisruptionBudgetList", "items": [{
        "kind": "PodDisruptionBudget", "metadata": metadata("pdb", "monitoring"),
        "spec": {"minAvailable": 1},
    }]})
    write_json(directory / "pools.json", pools)
    write_json(directory / "vmsses.json", vmsses)
    write_json(directory / "default-operation.json", {
        "name": uid(f"operation/{role}"), "status": "Succeeded",
        "operationType": "PutAgentPool", "startTime": "2026-09-12T00:00:00+00:00",
        "endTime": "2026-09-12T00:01:00+00:00", "errorCode": None,
    })
    write_json(directory / "node-resource-group.json", {
        "id": f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{cluster['nodeResourceGroup']}",
        "name": cluster["nodeResourceGroup"], "location": recovery.REGION,
        "managedBy": cluster["id"], "properties": {"provisioningState": "Succeeded"},
    })
    write_json(directory / "cilium-daemonset.json", {"kind": "DaemonSet"})
    write_json(directory / "events.json", {"kind": "EventList", "items": []})
    write_json(directory / "credential-read.json", {"read_only": True})
    for pool_name, rows in instances.items():
        write_json(directory / f"{by_pool[pool_name]['name']}-instances.json", rows)
    write_json(directory / f"{settings['failed_node']}-instance-view.json", failed_view(role))
    return {
        "pools": pools, "vmsses": vmsses, "instances": instances,
        "nodes": nodes, "pods": pods, "nnc": networks,
        "pdbs": json.loads((directory / "pdbs.json").read_text()),
        "cluster": cluster,
    }


def build_source(tmp_path):
    root = tmp_path / "source"
    clusters = []
    for index in range(1, 101):
        role = f"mesh-{index}"
        clusters.append({
            "id": (
                f"/subscriptions/{recovery.SUBSCRIPTION}/resourcegroups/{recovery.RESOURCE_GROUP}/"
                f"providers/Microsoft.ContainerService/managedClusters/clustermesh-{index}"
            ),
            "name": f"clustermesh-{index}", "location": recovery.REGION,
            "nodeResourceGroup": (
                f"MC_{recovery.RESOURCE_GROUP}_clustermesh-{index}_{recovery.REGION}"
            ),
            "provisioningState": "Succeeded", "tags": {"role": role},
        })
    write_json(root / "account.json", {"id": recovery.SUBSCRIPTION})
    write_json(root / "resource-group.json", {
        "id": f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{recovery.RESOURCE_GROUP}",
        "name": recovery.RESOURCE_GROUP, "location": recovery.REGION,
        "properties": {"provisioningState": "Succeeded"},
        "tags": {"clustermesh_debug_tfvars_sha256": recovery.TFVARS_SHA},
    })
    write_json(root / "clusters.json", clusters)
    write_json(root / "summary.json", {
        "read_only": True, "resource_mutations": 0, "health_claimed": False,
        "source_build_id": recovery.DIAGNOSED_BUILD,
        "roles": ["mesh-2", "mesh-51", "mesh-66", "mesh-79", "mesh-89", "mesh-94"],
        "results": [],
    })
    states = {}
    for role in recovery.ROLES:
        cluster = next(row for row in clusters if row["tags"]["role"] == role)
        states[role] = build_role(root, role, cluster)
    return root, states


def make_args(tmp_path, source, *, execute=False, name="summary.json"):
    kube = tmp_path / f"kube-{name}"
    kube.mkdir()
    for role in recovery.ROLES:
        (kube / f"{role}.config").write_text("private", encoding="utf-8")
    return SimpleNamespace(
        source_directory=str(source), source_build_id=recovery.DIAGNOSTIC_BUILD,
        resource_group=recovery.RESOURCE_GROUP,
        confirm_resource_group=recovery.RESOURCE_GROUP,
        expected_subscription=recovery.SUBSCRIPTION,
        expected_region=recovery.REGION, expected_tfvars_sha=recovery.TFVARS_SHA,
        kubeconfig_directory=str(kube), summary_file=str(tmp_path / name),
        timeout_seconds=600, request_timeout_seconds=60, execute=execute,
    )


class StatefulCloud:
    """Serve raw source state, one pending observation, then genuine readiness."""

    def __init__(self, states):
        self.states = copy.deepcopy(states)
        self.configmaps = {
            role: [{
                "kind": "ConfigMap", "apiVersion": "v1",
                "metadata": {
                    **metadata("historical-capacity-journal", "kube-system",
                               uid(f"journal/{role}")),
                    "resourceVersion": "7",
                },
                "data": {"owner": "historical", "token": uid(f"token/{role}")},
            }] for role in recovery.ROLES
        }
        self.adds = []
        self.patches = []
        self.reads = []
        self.phase = {role: "source" for role in recovery.ROLES}
        self.pending_reads = {role: 0 for role in recovery.ROLES}
        self.add_error_role = None
        self.cas_no_advance = False
        self.mutate_existing_on_create = False

    @staticmethod
    def value(command, flag):
        return command[command.index(flag) + 1]

    def role(self, command):
        if command[0] == "kubectl":
            context = self.value(command, "--context")
            return f"mesh-{context.split('-')[-1]}"
        cluster = None
        for flag in ("--cluster-name", "--name"):
            if flag in command:
                candidate = self.value(command, flag)
                if candidate.startswith("clustermesh-"):
                    cluster = candidate
                    break
        if cluster:
            return f"mesh-{cluster.split('-')[-1]}"
        if "--resource-group" in command:
            group = self.value(command, "--resource-group")
            for role, state in self.states.items():
                if group == state["cluster"]["nodeResourceGroup"]:
                    return role
        if command[1:3] == ["group", "show"] and "--name" in command:
            group = self.value(command, "--name")
            for role, state in self.states.items():
                if group == state["cluster"]["nodeResourceGroup"]:
                    return role
        return None

    def transition(self, role):
        if self.phase[role] != "pending":
            return
        self.pending_reads[role] += 1
        if self.pending_reads[role] < 2:
            return
        state = self.states[role]
        desired = recovery.ROLE_SETTINGS[role]
        pool_row = next(row for row in state["pools"] if row["name"] == desired["pool"])
        pool_row["provisioningState"] = "Succeeded"
        vmss_row = next(
            row for row in state["vmsses"]
            if row["tags"]["aks-managed-poolName"] == desired["pool"]
        )
        vmss_row["provisioningState"] = "Succeeded"
        for offset, row in enumerate(state["instances"][desired["pool"]]):
            row["provisioningState"] = "Succeeded"
            row["latestModelApplied"] = True
            node_row = node(
                row["computerName"], uid(f"new-node/{role}/{row['computerName']}"),
                desired["pool"], "azure://" + row["id"], ready=True,
            )
            state["nodes"].append(node_row)
            network = nnc(
                row["computerName"], node_row["metadata"]["uid"],
                150 + 3 * int(role.split("-")[1]) + offset, count=16, version=0,
            )
            state["nnc"].append(network)
            state["pods"].extend([
                daemon_pod("cilium", row["computerName"], "192.170.1.1"),
                daemon_pod("azure-cns", row["computerName"], "192.170.1.2"),
            ])
        self.phase[role] = "ready"

    def add_pool(self, role):
        state = self.states[role]
        settings = recovery.ROLE_SETTINGS[role]
        desired_name = settings["pool"]
        pool_row = pool(desired_name, settings["count"], settings["mode"], recovery.VM_SIZE, role)
        pool_row.update(
            maxPods=settings["max_pods"], nodeLabels={"prometheus": "true"} if role == "mesh-89" else {},
            provisioningState="Creating",
        )
        state["pools"].append(pool_row)
        vmss_name = f"aks-{desired_name}-{int(role.split('-')[1]):08d}-vmss"
        vmss_row = vmss(role, desired_name, vmss_name, settings["count"], "Creating")
        vmss_row["sku"]["name"] = recovery.VM_SIZE
        state["vmsses"].append(vmss_row)
        state["instances"][desired_name] = [
            instance(vmss_row, index, f"{vmss_name}{index:06d}", "Creating")
            for index in range(settings["count"])
        ]
        state["new_operation"] = {
            "name": uid(f"new-operation/{role}"), "status": "InProgress",
            "operationType": "PutAgentPool",
            "startTime": recovery.utc_now(), "endTime": None,
            "errorCode": None,
        }
        self.phase[role] = "pending"

    def apply_patch(self, row, operations):
        expected_rv = row["metadata"]["resourceVersion"]
        for operation in operations:
            if operation["op"] == "test":
                if operation["path"] == "/metadata/uid":
                    assert row["metadata"]["uid"] == operation["value"]
                elif operation["path"] == "/metadata/resourceVersion":
                    assert expected_rv == operation["value"]
                elif operation["path"] == "/data/token":
                    assert row["data"]["token"] == operation["value"]
                elif operation["path"] == "/data":
                    assert row["data"] == operation["value"]
            elif operation["path"] == "/data":
                row["data"] = copy.deepcopy(operation["value"])
        if not self.cas_no_advance:
            row["metadata"]["resourceVersion"] = str(int(expected_rv) + 1)

    def kubectl(self, command, role):
        state = self.states[role]
        if "create" in command and "configmap" in command:
            name = command[command.index("configmap") + 1]
            assert not any(row["metadata"]["name"] == name for row in self.configmaps[role])
            if self.mutate_existing_on_create:
                self.configmaps[role][0]["data"]["record"] = "changed-after-plan"
            data = dict(
                word.removeprefix("--from-literal=").split("=", 1)
                for word in command if word.startswith("--from-literal=")
            )
            row = {
                "kind": "ConfigMap", "apiVersion": "v1",
                "metadata": {
                    **metadata(name, "kube-system", uid(f"new-journal/{role}")),
                    "resourceVersion": "1",
                },
                "data": data,
            }
            self.configmaps[role].append(row)
            return json.dumps(row)
        if "patch" in command and "configmap" in command:
            name = command[command.index("configmap") + 1]
            row = next(item for item in self.configmaps[role] if item["metadata"]["name"] == name)
            self.apply_patch(row, json.loads(self.value(command, "-p")))
            self.patches.append((role, copy.deepcopy(command)))
            return json.dumps(row)
        if "get" in command and "configmap" in command and "configmaps" not in command:
            name = command[command.index("configmap") + 1]
            row = next(item for item in self.configmaps[role] if item["metadata"]["name"] == name)
            return json.dumps(row)
        if "get" in command and "configmaps" in command:
            return json.dumps({"kind": "ConfigMapList", "apiVersion": "v1",
                               "items": self.configmaps[role]})
        if "--raw=/readyz" in command:
            return "ok"
        if "nodes" in command:
            return json.dumps({"kind": "NodeList", "items": state["nodes"]})
        if "pods" in command:
            return json.dumps({"kind": "PodList", "items": state["pods"]})
        if "nodenetworkconfigs" in command:
            return json.dumps({"kind": "NodeNetworkConfigList", "items": state["nnc"]})
        if "pdb" in command:
            return json.dumps(state["pdbs"])
        raise AssertionError(command)

    def azure(self, command, role):
        if command[1:3] == ["account", "show"]:
            return json.dumps({"id": recovery.SUBSCRIPTION})
        if command[1:3] == ["vm", "list-usage"]:
            return json.dumps([
                {"name": recovery.QUOTA_FAMILY, "currentValue": 100, "limit": 992},
                {"name": "cores", "currentValue": 1000, "limit": 4223},
            ])
        if command[1:3] == ["vm", "list-skus"]:
            return json.dumps([{
                "name": recovery.VM_SIZE, "family": recovery.QUOTA_FAMILY,
                "resourceType": "virtualMachines", "locations": [recovery.REGION],
                "restrictions": [], "capabilities": [
                    {"name": "vCPUs", "value": "8"},
                    {"name": "MemoryGB", "value": "32"},
                    {"name": "PremiumIO", "value": "True"},
                    {"name": "EphemeralOSDiskSupported", "value": "False"},
                    {"name": "OSVhdSizeMB", "value": str(256 * 1024)},
                ],
            }])
        if command[1:3] == ["group", "show"]:
            name = self.value(command, "--name")
            if name == recovery.RESOURCE_GROUP:
                return json.dumps({
                    "id": f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{recovery.RESOURCE_GROUP}",
                    "name": recovery.RESOURCE_GROUP, "location": recovery.REGION,
                    "tags": {"clustermesh_debug_tfvars_sha256": recovery.TFVARS_SHA,
                             "run_id": recovery.RESOURCE_GROUP, "clustermesh_debug_preserved": "true",
                             "scenario": "perf-eval-clustermesh-scale", "clustermesh_debug_expected_clusters": "100",
                             "deletion_due_time": (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()},
                })
            state = self.states[role]
            return json.dumps({
                "id": f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{name}",
                "name": name, "location": recovery.REGION,
                "managedBy": state["cluster"]["id"],
                "tags": {"deletion_due_time": (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()},
            })
        state = self.states[role]
        if command[1:3] == ["aks", "show"]:
            return json.dumps({
                **state["cluster"], "currentKubernetesVersion": recovery.PATCH,
                "kubernetesVersion": "1.35", "powerState": {"code": "Running"},
            })
        if command[1:4] == ["aks", "nodepool", "list"]:
            self.transition(role)
            if self.phase[role] == "ready":
                state["new_operation"]["status"] = "Succeeded"
                state["new_operation"]["endTime"] = recovery.utc_now()
            return json.dumps(state["pools"])
        if command[1:3] == ["vmss", "list"] and command[2] != "list-instances":
            return json.dumps(state["vmsses"])
        if command[1:3] == ["vmss", "show"]:
            vmss_name = self.value(command, "--name")
            vmss_row = next(row for row in state["vmsses"] if row["name"] == vmss_name)
            return json.dumps({
                "id": vmss_row["id"],
                "osDisk": {
                    "osType": "Linux", "diskSizeGb": 256, "diskSizeGB": None,
                    "managedDisk": {"storageAccountType": "Premium_LRS"},
                    "diffDiskOption": None,
                },
                "imageReference": {
                    "id": "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/aks-images/providers/Microsoft.Compute/galleries/AKSUbuntu/images/2404containerd/versions/202609.03.1",
                },
            })
        if command[1:3] == ["vmss", "list-instances"]:
            vmss_name = self.value(command, "--name")
            pool_name = next(
                row["tags"]["aks-managed-poolName"] for row in state["vmsses"]
                if row["name"] == vmss_name
            )
            return json.dumps(state["instances"][pool_name])
        if command[1:3] == ["vmss", "get-instance-view"]:
            vmss_name = self.value(command, "--name")
            instance_id = self.value(command, "--instance-id")
            pool_name = next(
                row["tags"]["aks-managed-poolName"] for row in state["vmsses"]
                if row["name"] == vmss_name
            )
            row = next(item for item in state["instances"][pool_name]
                       if item["instanceId"] == instance_id)
            settings = recovery.ROLE_SETTINGS[role]
            if row["computerName"] == settings["failed_node"]:
                return json.dumps(failed_view(role))
            if row["provisioningState"] == "Creating":
                return json.dumps({
                    "statuses": [{"code": "ProvisioningState/creating"}],
                    "vmAgent": {"statuses": []}, "extensions": [],
                })
            return json.dumps(succeeded_view())
        if command[1:4] == ["aks", "operation", "show-latest"]:
            pool_name = self.value(command, "--nodepool-name")
            if pool_name == recovery.ROLE_SETTINGS[role]["pool"] and "new_operation" in state:
                return json.dumps(state["new_operation"])
            if pool_name == recovery.ROLE_SETTINGS[role]["pool"]:
                return json.dumps(None)
            return json.dumps({
                "name": uid(f"old-operation/{role}/{pool_name}"), "status": "Succeeded",
                "operationType": "PutAgentPool", "startTime": "2026-09-12T00:00:00+00:00",
                "endTime": "2026-09-12T00:01:00+00:00", "errorCode": None,
            })
        if command[1:4] == ["aks", "nodepool", "add"]:
            self.adds.append((role, copy.deepcopy(command)))
            self.add_pool(role)
            if self.add_error_role == role:
                raise recovery.workers.ReconcileError("provider response lost after delivery")
            return ""
        raise AssertionError(command)

    def __call__(self, command, timeout):
        self.reads.append((copy.deepcopy(command), timeout))
        role = self.role(command)
        if command[0] == "kubectl":
            return self.kubectl(command, role)
        return self.azure(command, role)


@pytest.fixture(name="environment")
def fixture_environment(tmp_path, monkeypatch):
    source, states = build_source(tmp_path)
    cloud = StatefulCloud(states)
    monkeypatch.setattr(recovery.time, "sleep", lambda _seconds: None)
    return tmp_path, source, cloud


def read_receipt(args):
    return json.loads(Path(args.summary_file).read_text(encoding="utf-8"))


def test_plan_is_raw_source_bound_and_zero_mutation(environment):
    tmp_path, source, cloud = environment
    args = make_args(tmp_path, source, name="plan.json")
    summary = {}
    recovery.execute_recovery(args, summary, cloud)
    receipt = read_receipt(args)
    assert receipt["success"] and receipt["plan_valid"] and receipt["status"] == "plan-valid"
    assert not receipt["mutation_started"]
    assert not receipt["capacity_qualified"] and not receipt["workloads_ready"]
    assert receipt["source_build_id"] == 80022 and receipt["diagnosed_build_id"] == 80017
    assert set(receipt["per_role"]) == set(recovery.ROLES)
    assert all(row["diagnostics_captured_before_health_guards"]
               for row in receipt["per_role"].values())
    assert not cloud.adds and not cloud.patches
    assert all(command[0] != "kubectl" or "create" not in command
               for command, _ in cloud.reads)
    sku_reads = [timeout for command, timeout in cloud.reads
                 if command[:3] == ["az", "vm", "list-skus"]]
    assert sku_reads == [180]
    for role in recovery.ROLES:
        command = receipt["per_role"][role]["command"]
        assert command[:4] == ["az", "aks", "nodepool", "add"]
        assert "--kubelet-disk-type" not in command
        assert command[command.index("--kubernetes-version") + 1] == "1.35.7"


def test_execute_adds_once_per_role_through_pending_to_ready(environment):
    tmp_path, source, cloud = environment
    args = make_args(tmp_path, source, execute=True, name="execute.json")
    summary = {}
    recovery.execute_recovery(args, summary, cloud)
    receipt = read_receipt(args)
    assert receipt["success"] and receipt["capacity_created"]
    assert receipt["initial_network_ready"] and not receipt["capacity_qualified"]
    assert not receipt["workloads_ready"] and not receipt["completed_global_baseline"]
    assert [role for role, _ in cloud.adds] == list(recovery.ROLES)
    assert len(cloud.adds) == 4
    assert all(timeout == 180 for command, timeout in cloud.reads
               if command[:4] == ["az", "aks", "nodepool", "add"])
    for role, row in receipt["per_role"].items():
        assert row["capacity_created"] and row["initial_network_ready"]
        assert not row["capacity_qualified"] and not row["workloads_ready"]
        assert row["action"]["submission_started"] is True
        assert row["action"]["accepted"] is True and row["action"]["ambiguous"] is False
        assert len(row["new_identities"]) == recovery.ROLE_SETTINGS[role]["count"]
        assert all(network["assigned_ip_count"] == 16 and network["version"] == 0
                   for network in row["new_networks"].values())
        assert any("HTTP probes" in item for item in row["qualification_required"])
    assert cloud.pending_reads == {role: 2 for role in recovery.ROLES}


def test_delivered_write_failure_is_ambiguous_and_stops_partial_batch(environment):
    tmp_path, source, cloud = environment
    cloud.add_error_role = "mesh-51"
    args = make_args(tmp_path, source, execute=True, name="ambiguous.json")
    summary = {}
    with pytest.raises(recovery.workers.ReconcileError, match="response lost"):
        recovery.execute_recovery(args, summary, cloud)
    receipt = read_receipt(args)
    action = receipt["per_role"]["mesh-51"]["action"]
    assert [role for role, _ in cloud.adds] == ["mesh-51"]
    assert action["attempted"] and action["submission_started"]
    assert action["accepted"] is None and action["ambiguous"] is True
    assert receipt["per_role"]["mesh-51"]["status"] == "add-ambiguous"
    assert receipt["per_role"]["mesh-66"]["action"]["attempted"] is False
    assert not receipt["success"] and receipt["status"] == "failed-closed"
    assert receipt["automatic_resume_or_adoption"] is False


def test_changed_data_cas_requires_resource_version_advance(environment):
    tmp_path, source, cloud = environment
    cloud.cas_no_advance = True
    args = make_args(tmp_path, source, execute=True, name="cas-failure.json")
    summary = {}
    with pytest.raises(recovery.workers.ReconcileError, match="changed-data journal CAS"):
        recovery.execute_recovery(args, summary, cloud)
    receipt = read_receipt(args)
    assert not receipt["success"] and not cloud.adds
    assert receipt["per_role"]["mesh-51"]["journal"]["attempted"] is True


def test_unchanged_journal_data_skips_patch_without_rv_advance(environment):
    tmp_path, source, cloud = environment
    args = make_args(tmp_path, source, execute=True, name="noop.json")
    summary = {
        "mutation_started": False,
        "per_role": {
            role: {
                "status": "validating", "capacity_created": False,
                "initial_network_ready": False, "action": recovery.empty_action(),
                "journal": {
                    "name": f"{recovery.JOURNAL_PREFIX}-{role}",
                    "namespace": "kube-system", "retained": True,
                    "attempted": False, "accepted": None, "ambiguous": False,
                },
            } for role in recovery.ROLES
        },
    }
    bundle = recovery.load_source(args)
    deadline = recovery.time.monotonic() + 600
    worker = recovery.RoleRecovery(
        args, bundle, bundle["roles"]["mesh-51"], summary, cloud, deadline,
    )
    worker.acquire()
    patch_count = len(cloud.patches)
    resource_version = worker.journal_rv
    worker.persist()
    assert len(cloud.patches) == patch_count
    assert worker.journal_rv == resource_version
    assert summary["per_role"]["mesh-51"]["journal"]["noop_update_skipped"] is True


@pytest.mark.parametrize("fault", [
    "source-hash", "failed-node-uid", "failed-vm", "mock-uid", "kwok-spec",
    "resident-ip", "existing-journal", "quota", "sku",
])
def test_safety_or_data_drift_fails_before_provider_add(environment, fault):
    tmp_path, source, cloud = environment
    if fault == "source-hash":
        original = cloud.azure
        changed = False

        def mutate_source(command, role):
            nonlocal changed
            result = original(command, role)
            if not changed and command[1:3] == ["vm", "list-usage"]:
                payload = json.loads((source / "mesh-51" / "events.json").read_text())
                payload["items"].append({"changed": True})
                write_json(source / "mesh-51" / "events.json", payload)
                changed = True
            return result

        cloud.azure = mutate_source
    elif fault == "failed-node-uid":
        node_row = next(row for row in cloud.states["mesh-51"]["nodes"]
                        if row["metadata"]["name"] == recovery.ROLE_SETTINGS["mesh-51"]["failed_node"])
        node_row["metadata"]["uid"] = uid("different-failed-node")
    elif fault == "failed-vm":
        cloud.states["mesh-51"]["instances"]["default"][0]["vmId"] = uid("different-failed-vm")
    elif fault == "mock-uid":
        mock = next(row for row in cloud.states["mesh-51"]["pods"]
                    if row["metadata"].get("namespace") == "mock-clustermesh")
        mock["metadata"]["uid"] = uid("different-mock")
    elif fault == "kwok-spec":
        kwok = next(row for row in cloud.states["mesh-51"]["nodes"]
                    if row["metadata"].get("labels", {}).get("type") == "kwok")
        kwok["spec"]["unschedulable"] = True
    elif fault == "resident-ip":
        network = cloud.states["mesh-51"]["nnc"][0]
        network["status"]["networkContainers"][0]["ipAssignments"].pop()
        network["status"]["assignedIPCount"] -= 1
    elif fault == "existing-journal":
        cloud.mutate_existing_on_create = True
    elif fault == "quota":
        original = cloud.azure

        def low_quota(command, role):
            if command[1:3] == ["vm", "list-usage"]:
                return json.dumps([
                    {"name": recovery.QUOTA_FAMILY, "currentValue": 950, "limit": 992},
                    {"name": "cores", "currentValue": 1000, "limit": 4223},
                ])
            return original(command, role)

        cloud.azure = low_quota
    else:
        original = cloud.azure

        def restricted(command, role):
            if command[1:3] == ["vm", "list-skus"]:
                row = json.loads(original(command, role))[0]
                row["restrictions"] = [{"reasonCode": "NotAvailableForSubscription"}]
                return json.dumps([row])
            return original(command, role)

        cloud.azure = restricted
    args = make_args(tmp_path, source, execute=True, name=f"{fault}.json")
    with pytest.raises(recovery.workers.ReconcileError):
        recovery.execute_recovery(args, {}, cloud)
    assert not cloud.adds


def test_cli_contract_and_exact_kubeconfig_set(environment):
    tmp_path, source, _ = environment
    args = make_args(tmp_path, source, name="cli.json")
    extra = Path(args.kubeconfig_directory) / "mesh-94.config"
    extra.write_text("private", encoding="utf-8")
    with pytest.raises(recovery.workers.ReconcileError, match="exactly the four"):
        recovery.validate_args(args)
    parsed = recovery.parse_args([
        "--source-directory", str(source), "--source-build-id", "80022",
        "--resource-group", recovery.RESOURCE_GROUP,
        "--confirm-resource-group", recovery.RESOURCE_GROUP,
        "--expected-subscription", recovery.SUBSCRIPTION,
        "--expected-region", recovery.REGION,
        "--expected-tfvars-sha", recovery.TFVARS_SHA,
        "--kubeconfig-directory", str(Path(args.kubeconfig_directory)),
        "--summary-file", str(tmp_path / "parsed.json"),
        "--timeout-seconds", "7200",
    ])
    assert parsed.source_build_id == 80022 and parsed.timeout_seconds == 7200
    assert not parsed.execute


@pytest.mark.parametrize("fault", ["healthy-boot", "healthy-node-ready", "healthy-mock-ready", "pdb-spec", "lease"])
def test_protected_health_and_scope_are_required_before_capacity_add(environment, fault):
    tmp_path, source, cloud = environment
    role = "mesh-51"
    healthy = next(row for row in cloud.states[role]["nodes"]
                   if row["metadata"]["name"] != recovery.ROLE_SETTINGS[role]["failed_node"]
                   and row["metadata"].get("labels", {}).get("type") != "kwok")
    if fault == "healthy-boot":
        healthy["status"]["nodeInfo"]["bootID"] = uid("unexpected-boot")
    elif fault == "healthy-node-ready":
        healthy["status"]["conditions"][0]["status"] = "Unknown"
    elif fault == "healthy-mock-ready":
        pod = next(row for row in cloud.states[role]["pods"]
                   if row["metadata"]["namespace"] == "mock-clustermesh"
                   and not row["metadata"].get("deletionTimestamp"))
        pod["status"]["containerStatuses"][0]["ready"] = False
    elif fault == "pdb-spec":
        cloud.states[role]["pdbs"]["items"][0]["spec"]["minAvailable"] = 0
    else:
        original = cloud.azure

        def short_lease(command, selected):
            result = original(command, selected)
            if command[1:3] == ["group", "show"]:
                row = json.loads(result)
                row["tags"]["deletion_due_time"] = recovery.utc_now()
                return json.dumps(row)
            return result

        cloud.azure = short_lease
    with pytest.raises(recovery.workers.ReconcileError):
        recovery.execute_recovery(make_args(tmp_path, source, execute=True, name=f"health-{fault}.json"), {}, cloud)
    assert not cloud.adds


def test_pdb_status_changes_do_not_change_the_immutable_disruption_policy(environment):
    tmp_path, source, cloud = environment
    row = cloud.states["mesh-51"]["pdbs"]["items"][0]
    row["metadata"]["resourceVersion"] = "999"
    row["status"] = {"currentHealthy": 2, "disruptionsAllowed": 1}
    result = {}
    recovery.execute_recovery(make_args(tmp_path, source, name="pdb-status.json"), result, cloud)
    assert result["plan_valid"] and not cloud.adds


def test_source_projection_does_not_pin_unobserved_full_vmss_fields():
    source = {"id": "/subscriptions/s/resourceGroups/G/providers/Microsoft.Compute/virtualMachineScaleSets/x",
              "name": "x", "sku": {"capacity": 2}, "tags": {}, "provisioningState": "Failed"}
    live = {**source, "id": source["id"].lower(), "location": "eastus2euap", "orchestrationMode": "Uniform",
            "virtualMachineProfile": {"not_part_of_source_projection": True}}
    assert recovery.vmss_contract(source) == recovery.vmss_contract(live)
    live["sku"] = {"capacity": 3}
    assert recovery.vmss_contract(source) != recovery.vmss_contract(live)


def test_read_queries_match_the_diagnostic_projection(environment):
    tmp_path, source, cloud = environment
    recovery.execute_recovery(make_args(tmp_path, source, name="queries.json"), {}, cloud)
    for command, _ in cloud.reads:
        if command[:3] == ["az", "vmss", "list"]:
            assert command[command.index("--query") + 1] == recovery.VMSS_QUERY
        if command[:3] == ["az", "vmss", "list-instances"]:
            assert command[command.index("--query") + 1] == recovery.VM_QUERY


def test_actual_azure_decimal_string_quota_counters_are_normalized(environment):
    tmp_path, source, cloud = environment
    original = cloud.azure

    def string_counters(command, role):
        result = original(command, role)
        if command[:3] == ["az", "vm", "list-usage"]:
            rows = json.loads(result)
            for row in rows:
                row["currentValue"] = str(row["currentValue"])
                row["limit"] = str(row["limit"])
            return json.dumps(rows)
        return result

    cloud.azure = string_counters
    args = make_args(tmp_path, source, name="quota-strings.json")
    summary = {}
    recovery.execute_recovery(args, summary, cloud)
    assert summary["plan_valid"] and summary["capacity"]["counters"][recovery.QUOTA_FAMILY]["remaining"] == 892
    assert summary["capacity_diagnostics"]["usage"][0]["currentValue"] == "100"
    assert not cloud.adds


@pytest.mark.parametrize("invalid", [None, True, -1, 1.5, "1.5", "-1", "unreadable"])
def test_invalid_quota_counters_fail_with_raw_read_evidence(environment, invalid):
    tmp_path, source, cloud = environment
    original = cloud.azure

    def bad_counter(command, role):
        result = original(command, role)
        if command[:3] == ["az", "vm", "list-usage"]:
            rows = json.loads(result)
            rows[0]["currentValue"] = invalid
            return json.dumps(rows)
        return result

    cloud.azure = bad_counter
    args = make_args(tmp_path, source, name="bad-quota.json")
    with pytest.raises(recovery.workers.ReconcileError):
        recovery.execute_recovery(args, {}, cloud)
    saved = read_receipt(args)
    assert saved["capacity_diagnostics"]["usage"][0]["currentValue"] == invalid
    assert saved["mutation_started"] is False and not cloud.adds


def test_new_nullable_upgrade_field_does_not_change_pool_configuration(environment):
    tmp_path, source, cloud = environment
    for state in cloud.states.values():
        for row in state["pools"]:
            row["upgradeSettings"]["maxBlockedNodes"] = None
    summary = {}
    recovery.execute_recovery(make_args(tmp_path, source, name="nullable-upgrade.json"), summary, cloud)
    assert summary["plan_valid"] and not cloud.adds


@pytest.mark.parametrize("value", [0, 1, "0", "10%"])
def test_explicit_blocked_node_upgrade_setting_is_not_ignored(environment, value):
    tmp_path, source, cloud = environment
    cloud.states["mesh-51"]["pools"][1]["upgradeSettings"]["maxBlockedNodes"] = value
    with pytest.raises(recovery.workers.ReconcileError, match="pool configuration changed"):
        recovery.execute_recovery(make_args(tmp_path, source, name="changed-upgrade.json"), {}, cloud)
    assert not cloud.adds
