"""Focused offline tests for the build-80001 monitoring capacity repair."""

# pylint: disable=protected-access,too-many-lines,attribute-defined-outside-init,unsubscriptable-object

from __future__ import annotations

import copy
import importlib.util
import json
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml


MODULE_DIR = Path(__file__).resolve().parents[1] / "clusterloader2" / "clustermesh-scale"
SPEC = importlib.util.spec_from_file_location(
    "post_retirement_prom_recovery", MODULE_DIR / "post_retirement_prom_recovery.py",
)
prom = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = prom
sys.path.insert(0, str(MODULE_DIR))
try:
    SPEC.loader.exec_module(prom)
finally:
    sys.path.pop(0)


def identity(name):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, name))


def now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def meta(name, namespace="", row_uid=None):
    result = {
        "name": name, "uid": row_uid or identity(f"{namespace}/{name}"),
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


def make_node(name, row_uid, pool, provider, boot):
    return {
        "kind": "Node",
        "metadata": {
            **meta(name, row_uid=row_uid),
            "labels": {
                "agentpool": pool, "kubernetes.azure.com/agentpool": pool,
                "kubernetes.io/os": "linux",
                "kubernetes.azure.com/cluster": prom.base.NODE_GROUP,
                "kubernetes.azure.com/node-image-version": "AKSUbuntu-2404containerd-202609.10.0",
                **({"prometheus": "true"} if pool == prom.NEW_POOL else {}),
            },
        },
        "spec": {"providerID": provider, "taints": [], "unschedulable": False},
        "status": {
            "conditions": [{"type": "Ready", "status": "True"}],
            "allocatable": {"cpu": "7820m", "memory": "28Gi", "pods": "250"},
            "nodeInfo": {
                "bootID": boot, "kubeletVersion": "v1.35.7",
                "operatingSystem": "linux", "osImage": "Ubuntu 24.04.3 LTS",
            },
        },
    }


def make_nnc(name, node_uid, nc_id, addresses, version=4):
    row = {
        "kind": "NodeNetworkConfig",
        "metadata": {
            **meta(name, "kube-system"),
            "ownerReferences": [owner("Node", name, node_uid)],
        },
        "spec": {"requestedIPCount": len(addresses)},
        "status": {
            "assignedIPCount": len(addresses),
            "networkContainers": [{
                "id": nc_id, "version": version,
                "ipAssignments": [{"ip": address} for address in addresses],
            }],
        },
    }
    return row


def make_daemon_pod(name, node, address):
    daemon_uid = identity(f"daemon/{name}")
    return {
        "kind": "Pod",
        "metadata": {
            **meta(f"{name}-{node}", "kube-system"),
            "labels": {"k8s-app": name},
            "ownerReferences": [owner("DaemonSet", name, daemon_uid)],
        },
        "spec": {
            "nodeName": node, "hostNetwork": True,
            "containers": [{"name": name, "image": "pinned"}],
        },
        "status": ready_status(address),
    }


def operator_pod(*, node="", address="", pvc=False, row_uid=prom.OPERATOR_UID):
    volumes = [{"name": "data", "persistentVolumeClaim": {"claimName": "forbidden"}}] if pvc else []
    pod = {
        "kind": "Pod",
        "metadata": {
            **meta(prom.OPERATOR_NAME, "monitoring", row_uid),
            "ownerReferences": [owner(
                "ReplicaSet", "prometheus-operator-7c59d5d8c4", prom.OPERATOR_RS_UID,
            )],
        },
        "spec": {
            "nodeSelector": {"kubernetes.io/os": "linux", "prometheus": "true"},
            "containers": [{"name": "prometheus-operator", "image": "pinned"}],
            "volumes": volumes,
        },
        "status": {
            "phase": "Pending", "podIP": "",
            "conditions": [{"type": "Ready", "status": "False"}],
            "containerStatuses": [],
        },
    }
    if node:
        pod["spec"]["nodeName"] = node
        pod["status"] = ready_status(address)
    return pod


def make_state(*, with_new=False, new_version=0):
    existing = {
        prom.SOURCE_NODE: {
            "uid": prom.SOURCE_NODE_UID, "pool": "default", "vmss": prom.base.DEFAULT_VMSS,
            "instance": "0", "vm": prom.SOURCE_VM_ID, "boot": prom.SOURCE_BOOT_ID,
            "nc": prom.base.SOURCE_NC, "prefix": 1,
        },
        "aks-cniv5-27550670-vmss000000": {
            "uid": identity("cniv5-node-0"), "pool": "cniv5", "vmss": "aks-cniv5-27550670-vmss",
            "instance": "0", "vm": identity("cniv5-vm-0"), "boot": identity("cniv5-boot-0"),
            "nc": identity("cniv5-nc-0"), "prefix": 2,
        },
        "aks-cniv5-27550670-vmss000001": {
            "uid": identity("cniv5-node-1"), "pool": "cniv5", "vmss": "aks-cniv5-27550670-vmss",
            "instance": "1", "vm": identity("cniv5-vm-1"), "boot": identity("cniv5-boot-1"),
            "nc": identity("cniv5-nc-1"), "prefix": 3,
        },
    }
    new_name = "aks-promv5-12345678-vmss000000"
    if with_new:
        existing[new_name] = {
            "uid": identity("promv5-node"), "pool": prom.NEW_POOL,
            "vmss": "aks-promv5-12345678-vmss", "instance": "0",
            "vm": identity("promv5-vm"), "boot": identity("promv5-boot"),
            "nc": identity("promv5-nc"), "prefix": 4,
        }
    nodes, nncs, pods = [], [], []
    address_sets = {}
    for name, row in existing.items():
        resource = (
            f"/subscriptions/{prom.base.SUBSCRIPTION}/resourceGroups/{prom.base.NODE_GROUP}/"
            f"providers/Microsoft.Compute/virtualMachineScaleSets/{row['vmss']}/virtualMachines/{row['instance']}"
        )
        nodes.append(make_node(name, row["uid"], row["pool"], f"azure://{resource}", row["boot"]))
        addresses = [f"10.244.{row['prefix']}.{index}" for index in range(1, 81)]
        if row["pool"] == prom.NEW_POOL:
            addresses = addresses[:16]
        address_sets[name] = addresses
        nncs.append(make_nnc(
            name, row["uid"], row["nc"], addresses,
            new_version if row["pool"] == prom.NEW_POOL else 4,
        ))
        for index, daemon in enumerate(("cilium", "azure-cns")):
            pods.append(make_daemon_pod(daemon, name, f"192.168.{row['prefix']}.{index + 1}"))
    kwok_uids, mock_uids = {}, {}
    for index in range(100):
        name = f"kwok-node-{index}"
        kwok_uid = identity(f"kwok/{index}")
        kwok_uids[name] = kwok_uid
        nodes.append({
            "kind": "Node",
            "metadata": {**meta(name, row_uid=kwok_uid), "labels": {"type": "kwok"}},
            "spec": {"taints": [{"key": "kwok-provider", "effect": "NoSchedule"}]},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]},
        })
        target = list(existing)[0 if index < 44 else 1 + (index - 44) % 2]
        address_index = index if index < 44 else (index - 44) // 2
        pod_uid = identity(f"mock/{index}")
        mock_uids[name] = pod_uid
        pod = {
            "kind": "Pod",
            "metadata": {
                **meta(name, "mock-clustermesh", pod_uid),
                "labels": {"app": "mock-cilium-agent"},
                "ownerReferences": [owner("StatefulSet", "kwok-node", identity("mock-controller"))],
            },
            "spec": {
                "nodeName": target,
                "containers": [{
                    "name": "mock-cilium-agent", "image": "pinned",
                    "resources": {
                        "requests": {"cpu": "100m", "memory": "256Mi"},
                        "limits": {"memory": "1Gi"},
                    },
                }],
            },
            "status": ready_status(address_sets[target][address_index]),
        }
        pods.append(pod)
    grafana = {
        "kind": "Pod",
        "metadata": {
            **meta("grafana-6df78447fb-xdcsl", "monitoring", prom.GRAFANA_UID),
            "ownerReferences": [owner("ReplicaSet", "grafana-6df78447fb", identity("grafana-rs"))],
        },
        "spec": {
            "nodeName": prom.SOURCE_NODE,
            "containers": [{"name": "grafana", "image": "pinned"}],
        },
        "status": ready_status(address_sets[prom.SOURCE_NODE][70]),
    }
    pods.append(grafana)
    operator = operator_pod(
        node=new_name if with_new else "",
        address=address_sets[new_name][0] if with_new else "",
    )
    pods.append(operator)
    operator_template = copy.deepcopy(operator["spec"])
    operator_template.pop("nodeName", None)
    controllers = [
        {
            "kind": "StatefulSet",
            "metadata": meta("kwok-node", "mock-clustermesh", identity("mock-controller")),
            "spec": {"replicas": 100, "template": {"spec": {"containers": [{"name": "mock", "image": "pinned"}]}}},
        },
        {
            "kind": "Deployment",
            "metadata": meta("prometheus-operator", "monitoring", identity("operator-deployment")),
            "spec": {"replicas": 1, "template": {"spec": {"containers": [{"name": "operator", "image": "pinned"}]}}},
        },
        {
            "kind": "ReplicaSet",
            "metadata": {
                **meta("prometheus-operator-7c59d5d8c4", "monitoring", prom.OPERATOR_RS_UID),
                "ownerReferences": [owner(
                    "Deployment", "prometheus-operator", identity("operator-deployment"),
                )],
            },
            "spec": {"replicas": 1, "template": {"spec": operator_template}},
        },
    ]
    for name in ("cilium", "azure-cns"):
        controllers.append({
            "kind": "DaemonSet",
            "metadata": meta(name, "kube-system", identity(f"daemon/{name}")),
            "spec": {"selector": {"matchLabels": {"k8s-app": name}},
                     "template": {"spec": {"containers": [{"name": name, "image": "pinned"}]}}},
        })
    pdbs = [{
        "kind": "PodDisruptionBudget",
        "metadata": meta("operator-pdb", "monitoring"),
        "spec": {"minAvailable": 1, "selector": {"matchLabels": {"app": "operator"}}},
    }]
    snapshot = {
        "nodes": {"kind": "NodeList", "apiVersion": "v1", "items": nodes},
        "pods": {"kind": "PodList", "apiVersion": "v1", "items": pods},
        "nnc": {"kind": "NodeNetworkConfigList", "apiVersion": "v1", "items": nncs},
        "controllers": {"kind": "List", "apiVersion": "v1", "items": controllers},
        "pdbs": {"kind": "PodDisruptionBudgetList", "apiVersion": "v1", "items": pdbs},
    }
    return snapshot, existing, kwok_uids, mock_uids, new_name


def make_bundle(snapshot, existing, kwok_uids, mock_uids):
    networks = prom.maintenance._nnc_map(snapshot["nnc"])
    nodes = prom.maintenance._real_node_map(snapshot["nodes"])
    real_pins = {}
    instances = {}
    for name, row in existing.items():
        if row["pool"] == prom.NEW_POOL:
            continue
        node = nodes[name]
        real_pins[name] = {
            "node_name": name, "node_uid": row["uid"], "boot_id": row["boot"],
            "provider_id": node["spec"]["providerID"].lower(), "pool_name": row["pool"],
            "nnc_uid": networks[name]["uid"], "network_container_id": row["nc"],
            "nnc_version": networks[name]["version"],
            "nnc_ip_addresses": copy.deepcopy(networks[name]["ip_addresses"]),
            "logical_node": prom.stalled.logical_node(node),
        }
        resource = node["spec"]["providerID"].removeprefix("azure://")
        instances[name] = {
            "vm_id": row["vm"], "resource_id": resource.lower(),
            "instance_id": row["instance"],
        }
    agents = prom.maintenance._agent_map(snapshot["pods"])
    operator = prom._operator(snapshot["pods"])
    grafana = next(row for row in snapshot["pods"]["items"] if prom.uid(row) == prom.GRAFANA_UID)
    return {
        "hashes": {"retirement.json": "a" * 64},
        "receipt": {
            "current_mock_uids": mock_uids, "preserved_kwok_uids": kwok_uids,
        },
        "controllers": prom.stalled.controllers_pin(snapshot["controllers"]),
        "pdbs": prom.base.frozen_pdbs(snapshot),
        "mock_specs": {
            name: prom.retirement.semantic_pod_spec(pod["spec"]) for name, pod in agents.items()
        },
        "real_pins": real_pins, "instances": instances,
        "operator_spec": prom.retirement.semantic_pod_spec(operator["spec"]),
        "grafana": {
            "name": grafana["metadata"]["name"], "uid": prom.GRAFANA_UID,
            "node_name": prom.SOURCE_NODE,
            "semantic_spec": prom.retirement.semantic_pod_spec(grafana["spec"]),
        },
        "pool_pins": {}, "vmss_pins": {},
    }


class GuardHarness(prom.PromRecovery):
    def save(self):
        return None

    def unchanged_inputs(self):
        return None


def guard_harness(bundle):
    value = object.__new__(GuardHarness)
    value.bundle = bundle
    value.summary = {}
    value.daemonsets = set()
    value.new_identity = None
    value.new_network = None
    value.candidate_node = None
    value.existing_nnc = copy.deepcopy(bundle["real_pins"])
    return value


def test_commands_are_exact_user_pool_delta():
    command = prom.pool_add_command()
    assert command[:4] == ["az", "aks", "nodepool", "add"]
    assert command[command.index("--mode") + 1] == "User"
    assert command[command.index("--node-vm-size") + 1] == "Standard_D8s_v5"
    assert command[command.index("--labels") + 1] == "prometheus=true"
    assert command[command.index("--node-osdisk-type") + 1] == "Managed"
    assert command[command.index("--node-osdisk-size") + 1] == "256"
    assert "--kubelet-disk-type" not in command
    assert prom.pool_delete_command()[prom.pool_delete_command().index("--name") + 1] == "prompool"


def test_genuine_retirement_contract_is_strict():
    current = {f"kwok-node-{index}": identity(f"current/{index}") for index in range(100)}
    kwok = {f"kwok-node-{index}": identity(f"kwok/{index}") for index in range(100)}
    protected = dict(list(current.items())[:44])
    original = {name: identity(f"old/{name}") for name in list(current)[44:]}
    replacements = {
        name: {
            "old_uid": old_uid, "new_uid": current[name], "ready": True,
            "fencing_proven": True,
        } for name, old_uid in original.items()
    }
    receipt = {
        "schema_version": 1, "execute": True, "plan_valid": True,
        "mutation_started": True, "success": True, "native_fencing_proven": True,
        "source_retired": True, "replacements_ready": True, "placement_hold_removed": True,
        "current_mock_ready": 100, "kwok_ready": 100, "workloads_ready": False,
        "bootstrap_complete": False, "cleanup_errors": [], "plan_sha256": prom.baseline.PLAN_SHA,
        "target": {
            "node_name": prom.RETIRED_NODE, "node_uid": prom.RETIRED_NODE_UID,
            "vm_id": prom.RETIRED_VM_ID,
        },
        "native": {
            "attempted": True, "submission_started": True, "accepted": True,
            "ambiguous": False, "automatic_retry_allowed": False,
            "operation_name": "25c65dfe-cdcf-428c-9685-53ec1b3982ec",
            "vm_absence_observed_at": now(),
        },
        "journal": {
            "name": prom.retirement.JOURNAL, "uid": prom.RETIREMENT_JOURNAL_UID,
            "retained": True, "accepted": True, "ambiguous": False,
        },
        "hold": {
            "node_name": prom.SOURCE_NODE, "node_uid": prom.SOURCE_NODE_UID,
            "applied": False, "cleanup_started": True, "remove": {"accepted": True},
        },
        "current_mock_uids": current, "preserved_kwok_uids": kwok,
        "protected_mock_uids": protected, "original_target_mock_uids": original,
        "controller_replacements": replacements,
    }
    prom._validate_retirement_receipt(receipt)
    broken = copy.deepcopy(receipt)
    broken["native"]["operation_name"] = identity("wrong-operation")
    with pytest.raises(prom.workers.ReconcileError, match="fencing"):
        prom._validate_retirement_receipt(broken)


def test_guard_accepts_version_zero_concrete_allocation_and_same_operator_uid():
    initial, initial_rows, kwok, mocks, _ = make_state()
    bundle = make_bundle(initial, initial_rows, kwok, mocks)
    harness = guard_harness(bundle)
    assert harness.guard(initial) is False
    value_snapshot, _, _, _, new_name = make_state(with_new=True, new_version=0)
    harness.new_identity = {
        "node_name": new_name,
        "provider_id": next(
            node["spec"]["providerID"] for node in value_snapshot["nodes"]["items"]
            if node["metadata"]["name"] == new_name
        ),
    }
    assert harness.guard(value_snapshot, require_new=True) is True
    assert harness.new_identity["operator_uid"] == prom.OPERATOR_UID
    assert harness.new_identity["nnc_uid"]
    assert harness.summary["current_mock_ready"] == 100
    returned = copy.deepcopy(value_snapshot)
    nnc = next(row for row in returned["nnc"]["items"] if row["metadata"]["name"] == new_name)
    container = nnc["status"]["networkContainers"][0]
    container["version"] = 1
    container["ipAssignments"][-1] = {"ip": "10.244.4.99"}
    assert harness.guard(returned, require_new=True) is True
    changed_without_version = copy.deepcopy(returned)
    nnc = next(row for row in changed_without_version["nnc"]["items"]
               if row["metadata"]["name"] == new_name)
    nnc["status"]["networkContainers"][0]["ipAssignments"][-1] = {"ip": "10.244.4.98"}
    with pytest.raises(prom.workers.ReconcileError, match="version advance"):
        harness.guard(changed_without_version, require_new=True)


@pytest.mark.parametrize("drift", ["source-boot", "pdb", "operator-pvc", "foreign-node"])
def test_guard_fails_closed_on_protected_state_drift(drift):
    initial, rows, kwok, mocks, _ = make_state()
    bundle = make_bundle(initial, rows, kwok, mocks)
    harness = guard_harness(bundle)
    changed = copy.deepcopy(initial)
    if drift == "source-boot":
        next(row for row in changed["nodes"]["items"]
             if row["metadata"]["name"] == prom.SOURCE_NODE)["status"]["nodeInfo"]["bootID"] = identity("reboot")
    elif drift == "pdb":
        changed["pdbs"]["items"][0]["spec"]["minAvailable"] = 0
    elif drift == "operator-pvc":
        index = next(index for index, row in enumerate(changed["pods"]["items"])
                     if row["metadata"]["name"] == prom.OPERATOR_NAME)
        changed["pods"]["items"][index] = operator_pod(pvc=True)
    else:
        changed["nodes"]["items"].append(make_node(
            "aks-foreign-vmss000000", identity("foreign-node"), "foreign",
            "azure:///subscriptions/37deca37-c375-4a14-b90a-043849bd2bf1/resourceGroups/"
            f"{prom.base.NODE_GROUP}/providers/Microsoft.Compute/virtualMachineScaleSets/"
            "aks-foreign-vmss/virtualMachines/0", identity("foreign-boot"),
        ))
    with pytest.raises((prom.workers.ReconcileError, prom.mocks.RecoveryError)):
        harness.guard(changed)


class ModelHarness(prom.PromRecovery):
    def save(self):
        return None

    def az_json(self, *command, **_kwargs):
        route = command[:2]
        if route == ("vmss", "show"):
            return copy.deepcopy(self.model)
        if route == ("vmss", "list-instances"):
            return copy.deepcopy(self.instances)
        if route == ("vmss", "get-instance-view"):
            if "--instance-id" not in command:
                return copy.deepcopy(self.scale_view)
            return copy.deepcopy(self.view)
        raise AssertionError(command)


def make_model_harness(initializing):
    value = object.__new__(ModelHarness)
    value.authority_pin = {
        "clusters": {
            prom.base.ROLE: (
                f"/subscriptions/{prom.base.SUBSCRIPTION}/resourceGroups/{prom.base.RESOURCE_GROUP}/"
                f"providers/Microsoft.ContainerService/managedClusters/{prom.base.CLUSTER}"
            )
        }
    }
    value.phase = "creating"
    value.summary = {}
    value.new_vmss = ""
    value.new_identity = None
    value.bundle = {
        "instances": {prom.SOURCE_NODE: {"vm_id": prom.SOURCE_VM_ID}},
        "real_pins": {prom.SOURCE_NODE: {}},
    }
    state = "Creating" if initializing else "Succeeded"
    image = None if initializing else "AKSUbuntu-2404containerd-202609.10.0"
    pool = {
        **copy.deepcopy(prom.modern.POOL_SETTINGS),
        "id": f"{value.authority_pin['clusters'][prom.base.ROLE]}/agentPools/{prom.NEW_POOL}",
        "name": prom.NEW_POOL, "count": 1, "mode": "User", "vmSize": prom.modern.VM_SIZE,
        "maxPods": 250, "osType": "Linux", "osSku": "Ubuntu", "osDiskType": "Managed",
        "osDiskSizeGB": 256, "kubeletDiskType": "OS", "nodeLabels": {"prometheus": "true"},
        "nodeTaints": None, "enableAutoScaling": False, "enableFips": False,
        "enableEncryptionAtHost": False, "enableNodePublicIp": False,
        "upgradeSettings": {"maxSurge": "10%", "maxUnavailable": "0"},
        "orchestratorVersion": prom.PATCH,
        "currentOrchestratorVersion": None if initializing else prom.PATCH,
        "nodeImageVersion": image, "provisioningState": state, "powerState": {"code": "Running"},
    }
    scale_name = "aks-promv5-12345678-vmss"
    scale_id = (
        f"/subscriptions/{prom.base.SUBSCRIPTION}/resourceGroups/{prom.base.NODE_GROUP}/"
        f"providers/Microsoft.Compute/virtualMachineScaleSets/{scale_name}"
    )
    scale = {
        "id": scale_id, "name": scale_name, "location": prom.base.REGION,
        "tags": {"aks-managed-poolName": prom.NEW_POOL}, "orchestrationMode": "Uniform",
        "sku": {"name": prom.modern.VM_SIZE, "capacity": 1}, "provisioningState": state,
    }
    value.model = {
        "id": scale_id,
        "osDisk": {
            "osType": "Linux", "diskSizeGB": 256, "diffDiskOption": None,
            "managedDisk": {"storageAccountType": "StandardSSD_LRS"},
        },
        "imageReference": {
            "publisher": "microsoft-aks", "offer": "aks-ubuntu", "sku": "2404",
            "version": "202609.10.0",
        },
    }
    value.instances = [{
        "id": f"{scale_id}/virtualMachines/0", "instanceId": "0",
        "computerName": None if initializing else f"{scale_name}000000",
        "vmId": None if initializing else identity("new-vm"),
        "latestModelApplied": None if initializing else True,
        "provisioningState": state,
    }]
    value.view = (
        {
            "statuses": [
                {"code": "ProvisioningState/creating"},
                {"code": "ProvisioningState/osProvisioningComplete"},
            ],
            "vmAgent": {"statuses": None}, "extensions": [{"name": "vmssCSE", "statuses": None}],
        } if initializing else {
            "statuses": [
                {"code": "ProvisioningState/succeeded"}, {"code": "PowerState/running"},
            ],
            "vmAgent": {"statuses": [{
                "code": "ProvisioningState/succeeded", "displayStatus": "Ready",
                "message": "Ready", "time": now(),
            }]},
            "extensions": [{"name": "vmssCSE", "statuses": [{"code": "ProvisioningState/succeeded"}]}],
        }
    )
    value.scale_view = (
        {
            "statuses": [{"code": "ProvisioningState/creating"}],
            "virtualMachines": [{"code": "ProvisioningState/creating", "count": 1}],
        } if initializing else {
            "statuses": [{"code": "ProvisioningState/succeeded"}],
            "virtualMachines": [{"code": "ProvisioningState/succeeded", "count": 1}],
        }
    )
    operation = {
        "name": "owned-create", "status": "InProgress" if initializing else "Succeeded",
        "operationType": "CreateAgentPool", "startTime": now(),
        **({} if initializing else {"endTime": now()}),
    }
    return value, pool, scale, operation


def test_new_vm_initialization_is_pending_not_success_and_disk_aliases_work():
    harness, pool, scale, operation = make_model_harness(True)
    assert harness._validate_new_model([pool], [scale], operation) is False
    assert harness.new_identity is None
    harness, pool, scale, operation = make_model_harness(False)
    assert harness._validate_new_model([pool], [scale], operation) is True
    identity_row = harness.new_identity
    assert isinstance(identity_row, dict)
    assert identity_row["vm_id"] == identity("new-vm")
    assert harness.summary["new_pool_receipt"]["os_disk_size_gib"] == 256


class SubmitHarness(prom.PromRecovery):
    def persist_journal(self):
        self.persisted += 1

    def unchanged_inputs(self):
        return None

    def authority(self):
        return None

    def journals(self, *, allow_own=False):
        assert allow_own

    def models(self):
        return True, [], []

    def snapshot(self):
        return {}

    def guard(self, _snapshot, *, require_new=False):
        return require_new

    def capacity(self):
        self.capacity_reads += 1

    def _validate_old_zero(self, _pools, _vmsses, **_kwargs):
        return None

    def raw_write(self, command):
        self.writes.append(command)
        if self.fail:
            raise prom.workers.ReconcileError("provider response lost")
        return ""


def submit_harness():
    value = object.__new__(SubmitHarness)
    value.args = SimpleNamespace(execute=True)
    value.summary = {
        "pool_add": prom.empty_action(), "old_pool_delete": prom.empty_action(),
    }
    value.journal_uid = identity("journal")
    value.initial_operations = {prom.NEW_POOL: "old-promv5-op", prom.OLD_POOL: "old-prompool-op"}
    value.persisted = 0
    value.capacity_reads = 0
    value.writes = []
    value.fail = False
    return value


def test_exactly_one_add_and_one_empty_delete_are_journaled():
    harness = submit_harness()
    harness.submit("pool_add", prom.pool_add_command())
    harness.submit("old_pool_delete", prom.pool_delete_command())
    assert harness.writes == [prom.pool_add_command(), prom.pool_delete_command()]
    assert harness.capacity_reads == 1
    assert prom._action_complete(harness.summary["pool_add"])
    assert prom._action_complete(harness.summary["old_pool_delete"])
    assert harness.persisted == 8


def test_ambiguous_submission_is_never_replayed():
    harness = submit_harness()
    harness.fail = True
    with pytest.raises(prom.workers.ReconcileError, match="response lost"):
        harness.submit("pool_add", prom.pool_add_command())
    action = harness.summary["pool_add"]
    assert action["submission_started"] is True
    assert action["accepted"] is None and action["ambiguous"] is True
    with pytest.raises(prom.workers.ReconcileError, match="duplicate"):
        harness.submit("pool_add", prom.pool_add_command())
    assert harness.writes == [prom.pool_add_command()]


def test_unknown_write_operation_and_short_lease_are_noops():
    harness = object.__new__(prom.PromRecovery)
    harness.args = SimpleNamespace(execute=True)
    harness.summary = {}
    harness.save = lambda: None
    with pytest.raises(prom.workers.ReconcileError, match="whitelist"):
        harness.raw_write(["az", "aks", "nodepool", "scale"])
    group = {
        "tags": {
            "deletion_due_time": (
                datetime.now(timezone.utc) + timedelta(seconds=100)
            ).isoformat()
        }
    }
    with pytest.raises(prom.workers.ReconcileError, match="lease"):
        prom.prepared.require_lease(group, 600)


def test_unknown_child_operation_is_not_adopted():
    harness = object.__new__(ModelHarness)
    harness.summary = {}
    harness.initial_operations = {}
    harness.save = lambda: None
    harness.az_json = lambda *_args, **_kwargs: {
        "name": "foreign", "status": "InProgress", "operationType": "ScaleAgentPool",
        "startTime": now(),
    }
    action = {
        **prom.empty_action(), "attempted": True, "submission_started": True,
        "accepted": True, "ambiguous": False, "requested_at": now(),
    }
    with pytest.raises(prom.workers.ReconcileError, match="causally bound"):
        harness.operation(prom.NEW_POOL, action)


def test_truthful_final_baseline_contains_only_actual_three_pools():
    cluster = (
        f"/subscriptions/{prom.base.SUBSCRIPTION}/resourceGroups/{prom.base.RESOURCE_GROUP}/"
        f"providers/Microsoft.ContainerService/managedClusters/{prom.base.CLUSTER}/agentPools"
    )
    layout = {
        "schema_version": 1, "role": prom.base.ROLE,
        "expected_total_pool_count": 202,
        "pools": {
            "default": {
                "count": 1, "mode": "System", "vm_size": "Standard_D8_v3",
                "resource_id": f"{cluster}/default",
            },
            "cniv5": {
                "count": 2, "mode": "System", "vm_size": "Standard_D8s_v5",
                "resource_id": f"{cluster}/cniv5",
            },
            "promv5": {
                "count": 1, "mode": "User", "vm_size": "Standard_D8s_v5",
                "resource_id": f"{cluster}/promv5",
            },
        },
    }
    assert prom.baseline.validate_layout(
        layout, run_id=prom.base.RESOURCE_GROUP, subscription_id=prom.base.SUBSCRIPTION,
        expected_pool_count=202,
    ) == layout


def make_pool(name, count, mode, sku, max_pods):
    cluster = (
        f"/subscriptions/{prom.base.SUBSCRIPTION}/resourceGroups/{prom.base.RESOURCE_GROUP}/"
        f"providers/Microsoft.ContainerService/managedClusters/{prom.base.CLUSTER}"
    )
    row = {
        **copy.deepcopy(prom.modern.POOL_SETTINGS),
        "id": f"{cluster}/agentPools/{name}", "name": name, "count": count,
        "mode": mode, "vmSize": sku, "maxPods": max_pods,
        "nodeLabels": {"prometheus": "true"} if name in (prom.OLD_POOL, prom.NEW_POOL) else None,
        "orchestratorVersion": prom.PATCH if name == prom.NEW_POOL else "1.35",
        "currentOrchestratorVersion": prom.PATCH,
        "nodeImageVersion": (
            "AKSUbuntu-2404containerd-202609.10.0"
            if name == prom.NEW_POOL else "AKSUbuntu-2404containerd-202608.26.0"
        ),
        "provisioningState": "Succeeded", "powerState": {"code": "Running"},
        "upgradeSettings": {"maxSurge": "10%", "maxUnavailable": "0"},
    }
    return row


def make_scale(name, pool, count, sku):
    return {
        "id": (
            f"/subscriptions/{prom.base.SUBSCRIPTION}/resourceGroups/{prom.base.NODE_GROUP}/"
            f"providers/Microsoft.Compute/virtualMachineScaleSets/{name}"
        ),
        "name": name, "location": prom.base.REGION,
        "sku": {"name": sku, "capacity": count, "tier": "Standard"},
        "tags": {"aks-managed-poolName": pool}, "orchestrationMode": "Uniform",
        "provisioningState": "Succeeded",
    }


def synthetic_raw_fixture():
    snapshot, rows, kwok_uids, mock_uids, new_name = make_state()
    pools = [
        make_pool("default", 1, "System", "Standard_D8_v3", 110),
        make_pool("cniv5", 2, "System", "Standard_D8s_v5", 250),
        make_pool(prom.OLD_POOL, 0, "User", "Standard_D8_v3", 250),
    ]
    scales = [
        make_scale(prom.base.DEFAULT_VMSS, "default", 1, "Standard_D8_v3"),
        make_scale("aks-cniv5-27550670-vmss", "cniv5", 2, "Standard_D8s_v5"),
        make_scale(prom.OLD_VMSS, prom.OLD_POOL, 0, "Standard_D8_v3"),
    ]
    instances = {}
    by_node = {
        node["metadata"]["name"]: node for node in snapshot["nodes"]["items"]
        if node["metadata"].get("labels", {}).get("type") != "kwok"
    }
    for name, row in rows.items():
        resource = by_node[name]["spec"]["providerID"].removeprefix("azure://")
        instance = {
            "id": resource, "name": f"{row['vmss']}_{row['instance']}",
            "instanceId": row["instance"], "computerName": name,
            "vmId": row["vm"], "latestModelApplied": True,
            "provisioningState": "Succeeded",
        }
        instances.setdefault(row["vmss"], []).append(instance)
    instances[prom.OLD_VMSS] = []
    protected = dict(list(mock_uids.items())[:44])
    original = {name: identity(f"retired/{name}") for name in list(mock_uids)[44:]}
    replacements = {
        name: {
            "old_uid": old_uid, "new_uid": mock_uids[name],
            "node_name": list(rows)[1 + index % 2],
            "ready": True, "fencing_proven": True,
        }
        for index, (name, old_uid) in enumerate(original.items())
    }
    receipt = {
        "schema_version": 1, "execute": True, "plan_valid": True,
        "mutation_started": True, "success": True, "native_fencing_proven": True,
        "source_retired": True, "replacements_ready": True, "placement_hold_removed": True,
        "current_mock_ready": 100, "kwok_ready": 100, "workloads_ready": False,
        "bootstrap_complete": False, "cleanup_errors": [], "plan_sha256": prom.baseline.PLAN_SHA,
        "target": {
            "node_name": prom.RETIRED_NODE, "node_uid": prom.RETIRED_NODE_UID,
            "vm_id": prom.RETIRED_VM_ID,
        },
        "native": {
            "attempted": True, "submission_started": True, "accepted": True,
            "ambiguous": False, "automatic_retry_allowed": False,
            "operation_name": "25c65dfe-cdcf-428c-9685-53ec1b3982ec",
            "vm_absence_observed_at": now(),
        },
        "journal": {
            "name": prom.retirement.JOURNAL, "uid": prom.RETIREMENT_JOURNAL_UID,
            "retained": True, "accepted": True, "ambiguous": False,
        },
        "hold": {
            "node_name": prom.SOURCE_NODE, "node_uid": prom.SOURCE_NODE_UID,
            "applied": False, "cleanup_started": True, "remove": {"accepted": True},
        },
        "current_mock_uids": mock_uids, "preserved_kwok_uids": kwok_uids,
        "protected_mock_uids": protected, "original_target_mock_uids": original,
        "controller_replacements": replacements,
        "current_kubernetes_diagnostics": snapshot,
        "native_observation": {
            "pools": pools, "vmsses": scales,
            "default_instances": instances[prom.base.DEFAULT_VMSS],
            "new_instances": instances["aks-cniv5-27550670-vmss"],
        },
    }
    return {
        "receipt": receipt, "snapshot": snapshot, "rows": rows,
        "pools": pools, "scales": scales, "instances": instances,
        "new_name": new_name,
    }


def write_raw_artifact(root, fixture):
    raw_resources = {
        "current-controllers.json": "controllers", "current-pods.json": "pods",
        "current-nodes.json": "nodes", "current-pdbs.json": "pdbs", "current-nnc.json": "nnc",
    }
    for name in sorted(prom.RETIREMENT_REQUIRED_FILES - {"retirement.json"}):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        if name.startswith("worker-state/") and path.name in raw_resources:
            payload = fixture["snapshot"][raw_resources[path.name]]
        elif name.endswith(".json"):
            payload = {"schema_version": 1, "synthetic_raw_shape": True}
        else:
            path.write_text("synthetic-complete-input\n", encoding="utf-8")
            continue
        path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    fixture["receipt"]["worker_state_input_hashes"] = prom.qualification.hash_tree(root / "worker-state")
    fixture["receipt"]["qualification_input_hashes"] = prom.qualification.hash_tree(root / "qualification-input")
    (root / "retirement.json").write_text(json.dumps(fixture["receipt"], sort_keys=True), encoding="utf-8")


class FullCloud:
    """Stateful raw-schema runner for the complete plan and execute protocols."""

    def __init__(self, fixture, args):
        self.fixture, self.args = fixture, args
        self.pools = copy.deepcopy(fixture["pools"])
        self.scales = copy.deepcopy(fixture["scales"])
        self.instances = copy.deepcopy(fixture["instances"])
        self.snapshot = copy.deepcopy(fixture["snapshot"])
        self.writes = []
        self.add_count = 0
        self.delete_count = 0
        self.cluster_id = (
            f"/subscriptions/{prom.base.SUBSCRIPTION}/resourceGroups/{prom.base.RESOURCE_GROUP}/"
            f"providers/Microsoft.ContainerService/managedClusters/{prom.base.CLUSTER}"
        )
        self.group, self.node_group, self.clusters, self.members = self.scope()
        self.operations = {
            name: self.operation(f"{name}-initial", "PutAgentPool")
            for name in ("default", "cniv5", prom.OLD_POOL)
        }
        self.top_operation = self.operation("old-top-level", "PutManagedCluster")
        self.configmaps = {
            name: {
                "apiVersion": "v1", "kind": "ConfigMap",
                "metadata": {
                    **meta(name, "kube-system",
                           prom.RETIREMENT_JOURNAL_UID if name == prom.retirement.JOURNAL else identity(name)),
                    "resourceVersion": "1",
                },
                "data": {"state": "retained"},
            }
            for name in prom.HISTORICAL_JOURNALS
        }

    @staticmethod
    def operation(name, operation_type):
        stamp = now()
        return {
            "name": name, "status": "Succeeded", "operationType": operation_type,
            "startTime": stamp, "endTime": stamp, "errorCode": None,
        }

    def scope(self):
        scope = f"/subscriptions/{prom.base.SUBSCRIPTION}/resourceGroups/{prom.base.RESOURCE_GROUP}"
        expiry = (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()
        group = {
            "id": scope, "location": prom.base.REGION,
            "tags": {
                "clustermesh_debug_preserved": "true", "run_id": prom.base.RESOURCE_GROUP,
                "scenario": "perf-eval-clustermesh-scale",
                "clustermesh_debug_expected_clusters": "100",
                "clustermesh_debug_tfvars_sha256": self.args.expected_tfvars_sha,
                "deletion_due_time": expiry,
            },
        }
        clusters, members = [], []
        fleet = f"{scope}/providers/Microsoft.ContainerService/fleets/clustermesh-flt"
        for index in range(1, 101):
            role, name = f"mesh-{index}", f"clustermesh-{index}"
            cluster_id = f"{scope}/providers/Microsoft.ContainerService/managedClusters/{name}"
            clusters.append({
                "id": cluster_id, "name": name, "location": prom.base.REGION,
                "nodeResourceGroup": (
                    prom.base.NODE_GROUP if role == prom.base.ROLE
                    else f"mc_{prom.base.RESOURCE_GROUP}_{name}_{prom.base.REGION}"
                ),
                "tags": {"role": role, "run_id": prom.base.RESOURCE_GROUP},
                "provisioningState": "Succeeded", "powerState": {"code": "Running"},
            })
            members.append({
                "id": f"{fleet}/members/{role}", "name": role,
                "clusterResourceId": cluster_id, "provisioningState": "Succeeded",
                "labels": {"mesh": "true"},
                "meshProperties": {
                    "ciliumProperties": {"name": f"assigned-{index}", "id": index},
                    "clusterMeshProfileResourceId": (
                        f"{fleet}/clusterMeshProfiles/clustermesh-cmp"
                    ),
                    "status": {"state": "Connected"},
                },
            })
        selected = next(row for row in clusters if row["tags"]["role"] == prom.base.ROLE)
        node_group = {
            "id": f"/subscriptions/{prom.base.SUBSCRIPTION}/resourceGroups/{prom.base.NODE_GROUP}",
            "location": prom.base.REGION, "managedBy": selected["id"],
            "tags": {"deletion_due_time": expiry},
        }
        return group, node_group, clusters, members

    @staticmethod
    def value(command, flag):
        return command[command.index(flag) + 1]

    @staticmethod
    def ready_view():
        return {
            "statuses": [
                {"code": "ProvisioningState/succeeded"},
                {"code": "PowerState/running"},
            ],
            "vmAgent": {"statuses": [{
                "code": "ProvisioningState/succeeded", "displayStatus": "Ready",
                "message": "Ready", "time": now(),
            }]},
            "extensions": [{
                "name": "vmssCSE", "statuses": [{"code": "ProvisioningState/succeeded"}],
            }],
        }

    def add_promv5(self):
        self.add_count += 1
        assert self.add_count == 1
        pool = make_pool(prom.NEW_POOL, 1, "User", prom.modern.VM_SIZE, 250)
        scale_name = "aks-promv5-12345678-vmss"
        scale = make_scale(scale_name, prom.NEW_POOL, 1, prom.modern.VM_SIZE)
        resource = f"{scale['id']}/virtualMachines/0"
        self.pools.append(pool)
        self.scales.append(scale)
        self.instances[scale_name] = [{
            "id": resource, "name": f"{scale_name}_0", "instanceId": "0",
            "computerName": f"{scale_name}000000", "vmId": identity("promv5-vm"),
            "latestModelApplied": True, "provisioningState": "Succeeded",
        }]
        self.operations[prom.NEW_POOL] = self.operation("owned-promv5-add", "CreateAgentPool")
        self.snapshot = make_state(with_new=True, new_version=0)[0]

    def delete_old(self):
        self.delete_count += 1
        assert self.delete_count == 1
        self.pools = [row for row in self.pools if row["name"] != prom.OLD_POOL]
        self.scales = [
            row for row in self.scales if prom.workers.vmss_pool_name(row) != prom.OLD_POOL
        ]
        self.instances.pop(prom.OLD_VMSS)
        self.operations[prom.OLD_POOL] = self.operation("owned-old-prom-delete", "DeleteAgentPool")

    def azure(self, command):
        route = command[1:3]
        if route == ["account", "show"]:
            return {"id": prom.base.SUBSCRIPTION}
        if route == ["group", "show"]:
            return self.node_group if self.value(command, "--name") == prom.base.NODE_GROUP else self.group
        if route == ["aks", "list"]:
            return self.clusters
        if route == ["fleet", "member"]:
            return self.members
        if route == ["aks", "show"]:
            return {
                "id": self.cluster_id, "name": prom.base.CLUSTER,
                "kubernetesVersion": "1.35", "currentKubernetesVersion": prom.PATCH,
            }
        if route == ["aks", "nodepool"] and command[3] == "list":
            return self.pools
        if route == ["aks", "operation"]:
            pool = self.value(command, "--nodepool-name") if "--nodepool-name" in command else ""
            return self.operations[pool] if pool else self.top_operation
        if route == ["vmss", "list"] and command[2] == "list":
            return self.scales
        if route == ["vmss", "list-instances"]:
            return self.instances[self.value(command, "--name")]
        if route == ["vmss", "get-instance-view"]:
            name = self.value(command, "--name")
            if "--instance-id" in command:
                return self.ready_view()
            count = next(row["sku"]["capacity"] for row in self.scales if row["name"] == name)
            return {
                "statuses": [{"code": "ProvisioningState/succeeded"}],
                "virtualMachines": (
                    [] if count == 0 else [{"code": "ProvisioningState/succeeded", "count": count}]
                ),
            }
        if route == ["vmss", "show"]:
            name = self.value(command, "--name")
            scale = next(row for row in self.scales if row["name"] == name)
            return {
                "id": scale["id"],
                "osDisk": {
                    "osType": "Linux", "diskSizeGb": 256, "diffDiskOption": None,
                    "managedDisk": {"storageAccountType": "StandardSSD_LRS"},
                },
                "imageReference": {
                    "id": (
                        "/subscriptions/00000000-0000-0000-0000-000000000000/"
                        "resourceGroups/aks-images/providers/Microsoft.Compute/galleries/"
                        "AKSUbuntu/images/2404containerd/versions/202609.10.0"
                    ),
                },
            }
        if route == ["vm", "list-usage"]:
            return [
                {"name": prom.modern.QUOTA_FAMILY, "currentValue": 100, "limit": 1000},
                {"name": "cores", "currentValue": 1000, "limit": 10000},
            ]
        if route == ["vm", "list-skus"]:
            assert self.value(command, "--size") == prom.modern.VM_SIZE
            return [{
                "name": prom.modern.VM_SIZE, "family": prom.modern.QUOTA_FAMILY,
                "resourceType": "virtualMachines", "locations": [prom.base.REGION],
                "restrictions": [],
                "capabilities": [
                    {"name": "vCPUs", "value": "8"}, {"name": "MemoryGB", "value": "32"},
                    {"name": "PremiumIO", "value": "True"},
                ],
            }]
        if command[1:4] == ["aks", "nodepool", "add"]:
            assert command[:-2] == prom.pool_add_command()
            self.writes.append(command)
            self.add_promv5()
            return ""
        if command[1:4] == ["aks", "nodepool", "delete"]:
            assert command[:-2] == prom.pool_delete_command()
            self.writes.append(command)
            self.delete_old()
            return ""
        raise AssertionError(command)

    @staticmethod
    def kubectl_args(command):
        result, index = [], 1
        while index < len(command):
            if command[index] in ("--kubeconfig", "--context"):
                index += 2
            elif command[index].startswith("--request-timeout="):
                index += 1
            else:
                result.append(command[index])
                index += 1
        return result

    def kubectl(self, command):
        args = self.kubectl_args(command)
        if args == ["get", "--raw=/readyz"]:
            return "ok"
        if len(args) == 2 and args[0] == "get" and args[1].startswith(
                "--raw=/apis/metrics.k8s.io/v1beta1/nodes/"):
            return {
                "metadata": {"name": self.fixture["new_name"]},
                "timestamp": now(), "usage": {"memory": "1Gi", "cpu": "500m"},
            }
        if "create" in args and "configmap" in args:
            name = args[args.index("configmap") + 1]
            data = {}
            for item in args:
                if item.startswith("--from-literal="):
                    key, value = item.removeprefix("--from-literal=").split("=", 1)
                    data[key] = value
            row = {
                "apiVersion": "v1", "kind": "ConfigMap",
                "metadata": {**meta(name, "kube-system"), "resourceVersion": "1"},
                "data": data,
            }
            self.configmaps[name] = row
            self.writes.append(command)
            return row
        if "patch" in args and "configmap" in args:
            name = args[args.index("configmap") + 1]
            patch = json.loads(self.value(args, "-p"))
            row = self.configmaps[name]
            assert patch[0]["value"] == row["metadata"]["uid"]
            assert patch[1]["value"] == row["metadata"]["resourceVersion"]
            assert patch[2]["value"] == row["data"]
            row["data"] = patch[3]["value"]
            row["metadata"]["resourceVersion"] = str(int(row["metadata"]["resourceVersion"]) + 1)
            self.writes.append(command)
            return row
        if "configmaps" in args:
            if "metadata.name=" in " ".join(args):
                name = self.value(args, "--field-selector").split("=", 1)[1]
                return {"apiVersion": "v1", "kind": "ConfigMapList",
                        "items": [self.configmaps[name]] if name in self.configmaps else []}
            if "-o" in args and "json" in args and args[-3:-1] != ["configmap", prom.JOURNAL]:
                return {
                    "apiVersion": "v1", "kind": "ConfigMapList",
                    "items": list(self.configmaps.values()),
                }
        if "configmap" in args and prom.JOURNAL in args:
            return self.configmaps[prom.JOURNAL]
        resource = args[args.index("get") + 1] if "get" in args else ""
        return {
            "nodes": self.snapshot["nodes"],
            "pods": self.snapshot["pods"],
            "nodenetworkconfigs": self.snapshot["nnc"],
            "deployments,replicasets,daemonsets,statefulsets": self.snapshot["controllers"],
            "pdb": self.snapshot["pdbs"],
        }[resource]

    def __call__(self, command, _timeout):
        value = self.azure(command) if command[0] == "az" else self.kubectl(command)
        return value if isinstance(value, str) else json.dumps(value)


def recovery_args(root, summary, execute):
    return SimpleNamespace(
        resource_group=prom.base.RESOURCE_GROUP,
        confirm_resource_group=prom.base.RESOURCE_GROUP,
        expected_subscription=prom.base.SUBSCRIPTION,
        expected_region=prom.base.REGION,
        expected_tfvars_sha="a" * 64,
        retirement_directory=str(root),
        retirement_build_id=prom.RETIREMENT_BUILD,
        kubeconfig=str(root.parent / "private-mesh96.config"),
        context=prom.base.CLUSTER,
        timeout_seconds=3600,
        request_timeout_seconds=45,
        summary_file=str(summary),
        execute=execute,
    )


def test_full_plan_and_execution_from_raw_build80001_shape(tmp_path):
    root = tmp_path / "retirement-input"
    fixture = synthetic_raw_fixture()
    write_raw_artifact(root, fixture)
    (tmp_path / "private-mesh96.config").write_text("private synthetic config\n", encoding="utf-8")
    plan_args = recovery_args(root, tmp_path / "plan-summary.json", False)
    plan_cloud = FullCloud(fixture, plan_args)
    plan = {}
    prom.execute_recovery(plan_args, plan, runner=plan_cloud)
    assert plan["execute"] is False and plan["plan_valid"] is True
    assert plan["mutation_started"] is False and plan["success"] is False
    assert not plan_cloud.writes
    assert "current_kubernetes_diagnostics" in plan and "arm_diagnostics" in plan
    execute_args = recovery_args(root, tmp_path / "execute-summary.json", True)
    cloud = FullCloud(fixture, execute_args)
    historical = copy.deepcopy(cloud.configmaps)
    summary = {}
    prom.execute_recovery(execute_args, summary, runner=cloud)
    assert summary["execute"] is True and summary["success"] is True
    assert summary["repaired"] is True and summary["workloads_ready"] is False
    assert summary["modern_cni"]["completed"] is True
    assert summary["modern_cni"]["source_retired"] is True
    assert summary["baseline_pool_layout"]["expected_total_pool_count"] == 202
    assert summary["current_mock_uids"] == fixture["receipt"]["current_mock_uids"]
    assert summary["preserved_kwok_uids"] == fixture["receipt"]["preserved_kwok_uids"]
    assert summary["retirement_build_id"] == 80001 and summary["retirement_sha256"]
    assert summary["controller_replacements"] == fixture["receipt"]["controller_replacements"]
    assert summary["new_prom_identity"]["operator_uid"] == prom.OPERATOR_UID
    assert cloud.add_count == cloud.delete_count == 1
    assert len([row for row in cloud.writes if row[0] == "az"]) == 2
    assert set(cloud.configmaps) == set(historical) | {prom.JOURNAL}
    assert all(cloud.configmaps[name] == row for name, row in historical.items())
    forbidden = {"delete-machines", "scale", "reimage", "restart"}
    assert not any(row in forbidden for command in cloud.writes for row in command)
    assert summary["prometheus_capacity_reserve"]["reserved_memory_bytes"] == 16 * 1024**3


@pytest.mark.parametrize("failure", ["pdb", "vmss"])
def test_raw_plan_failure_persists_pre_failure_diagnostics(tmp_path, failure):
    root = tmp_path / "retirement-input"
    fixture = synthetic_raw_fixture()
    write_raw_artifact(root, fixture)
    (tmp_path / "private-mesh96.config").write_text("private synthetic config\n", encoding="utf-8")
    summary_path = tmp_path / "failed-summary.json"
    args = recovery_args(root, summary_path, False)
    cloud = FullCloud(fixture, args)
    if failure == "pdb":
        cloud.snapshot["pdbs"]["items"][0]["spec"]["minAvailable"] = 0
    else:
        cloud.scales[0]["provisioningState"] = "Updating"
    summary = {}
    with pytest.raises(prom.workers.ReconcileError, match="PDB|VMSS"):
        prom.execute_recovery(args, summary, runner=cloud)
    persisted = json.loads(summary_path.read_text(encoding="utf-8"))
    assert persisted["status"] == "failed-closed" and persisted["mutation_started"] is False
    assert persisted["arm_diagnostics"]["pools"]
    assert persisted["current_kubernetes_diagnostics"]["pdbs"]["items"]
    assert persisted["scope_diagnostics"]["members"] and not cloud.writes


@pytest.mark.parametrize("pending", ["not-ready", "missing-info", "missing-nnc", "empty-nnc", "daemonset", "operator"])
def test_owned_new_worker_startup_waits_without_weakening_final_readiness(pending):
    initial, existing, kwok, agents, _ = make_state()
    harness = guard_harness(make_bundle(initial, existing, kwok, agents))
    assert harness.guard(initial) is False
    ready, _, _, _, name = make_state(with_new=True)
    node = next(row for row in ready["nodes"]["items"] if row["metadata"]["name"] == name)
    harness.new_identity = {"node_name": name, "provider_id": node["spec"]["providerID"]}
    starting = copy.deepcopy(ready)
    new_node = next(row for row in starting["nodes"]["items"] if row["metadata"]["name"] == name)
    if pending == "not-ready":
        new_node["status"]["conditions"][0]["status"] = "False"
        new_node["spec"]["taints"] = [{"key": "node.cilium.io/agent-not-ready", "effect": "NoSchedule"}]
    elif pending == "missing-info":
        new_node["status"]["nodeInfo"] = {}
    elif pending == "missing-nnc":
        starting["nnc"]["items"] = [row for row in starting["nnc"]["items"] if row["metadata"]["name"] != name]
    elif pending == "empty-nnc":
        next(row for row in starting["nnc"]["items"] if row["metadata"]["name"] == name)["status"] = {}
    elif pending == "daemonset":
        pod = next(row for row in starting["pods"]["items"]
                   if row["spec"].get("nodeName") == name and row["metadata"]["labels"].get("k8s-app") == "cilium")
        pod["status"]["conditions"][0]["status"] = "False"
        pod["status"]["containerStatuses"][0]["ready"] = False
    else:
        pod = next(row for row in starting["pods"]["items"] if prom.uid(row) == prom.OPERATOR_UID)
        pod["status"] = {"phase": "Pending", "containerStatuses": []}
    assert harness.guard(starting, require_new=True) is False
    assert harness.summary["new_prom_wait_reason"]
    assert harness.summary["current_mock_ready"] == harness.summary["kwok_ready"] == 100
    assert harness.guard(ready, require_new=True) is True
    with pytest.raises(prom.workers.ReconcileError, match="regressed"):
        harness.guard(starting, require_new=True)


def test_uninitialized_guest_agent_is_not_misreported_as_completed_creation():
    harness, pool, scale, operation = make_model_harness(False)
    harness.view["vmAgent"]["statuses"] = None
    assert harness._validate_new_model([pool], [scale], operation) is False


@pytest.mark.parametrize("regression", ["provider", "operator"])
def test_old_empty_pool_is_not_deleted_after_monitoring_readiness_regression(regression):
    harness = submit_harness()
    if regression == "provider":
        harness.models = lambda: (False, [], [])
    else:
        harness.guard = lambda *_args, **_kwargs: False
    with pytest.raises(prom.workers.ReconcileError, match="readiness"):
        harness.submit("old_pool_delete", prom.pool_delete_command())
    assert not harness.writes
    assert harness.summary["old_pool_delete"]["submission_started"] is False


def test_provider_submit_has_one_bounded_budget_separate_from_kubernetes_reads(tmp_path):
    args = recovery_args(tmp_path, tmp_path / "unused.json", True)
    calls = []
    recovery = GuardHarness(args, {"real_pins": {}}, {},
                            lambda command, timeout: calls.append((command, timeout)) or "")
    recovery.raw_write(prom.pool_add_command())
    assert len(calls) == 1 and 179 <= calls[0][1] <= 180
    assert calls[0][0][-2:] == ["--subscription", prom.base.SUBSCRIPTION]


def test_memory_reserve_covers_the_configured_n100_prometheus_limit():
    pipeline = yaml.safe_load((MODULE_DIR.parents[3] / "pipelines/system/new-pipeline-test.yml").read_text(encoding="utf-8"))
    stage = next(row for row in pipeline["stages"] if row["stage"] == "azure_eastus2euap_n100_debug_resume_37deca")
    assert prom.PROM_MEMORY_RESERVE >= int(stage["variables"]["CL2_PROMETHEUS_MEMORY_LIMIT_GI"]) * 1024**3


@pytest.mark.parametrize("operation,timeout", [("list-skus", 180), ("list-usage", 45)])
@pytest.mark.parametrize("failure", ["none", "transient", "authorization"])
def test_capacity_read_budget_is_scoped_and_only_transient_reads_retry(monkeypatch, operation, timeout, failure):
    recovery = object.__new__(prom.PromRecovery)
    calls = []
    sleeps = []

    def read(*command, timeout_seconds=45):
        calls.append((command, timeout_seconds))
        if failure == "authorization":
            raise prom.workers.ReconcileError("AuthorizationFailed")
        if failure == "transient" and len(calls) == 1:
            raise prom.workers.ReconcileError(f"command timed out after {timeout_seconds}s: az vm {operation}")
        return ["synthetic-read-result"]

    recovery.az_json = read
    recovery.remaining_seconds = lambda seconds: seconds
    monkeypatch.setattr(prom.time, "sleep", sleeps.append)
    if failure == "authorization":
        with pytest.raises(prom.workers.ReconcileError, match="AuthorizationFailed"):
            recovery.az_json_retry("vm", operation)
    else:
        assert recovery.az_json_retry("vm", operation) == ["synthetic-read-result"]
    assert all(value == timeout for _, value in calls)
    assert len(calls) == (2 if failure == "transient" else 1)
    assert sleeps == ([2] if failure == "transient" else [])
    assert 3 * prom.SKU_READ_SECONDS < 15 * 60


@pytest.mark.parametrize("error,complete", [("ResourceNotFound (404)", True), ("Forbidden (403)", False)])
def test_deleted_pool_child_endpoint_absence_requires_positive_pool_and_vmss_absence(tmp_path, error, complete):
    class DeletedEndpointCloud(FullCloud):
        def azure(self, command):
            if self.delete_count and command[1:4] == ["aks", "operation", "show-latest"]:
                if "--nodepool-name" in command and self.value(command, "--nodepool-name") == prom.OLD_POOL:
                    raise prom.workers.ReconcileError(error)
            return super().azure(command)

    root = tmp_path / "retirement-input"
    fixture = synthetic_raw_fixture()
    write_raw_artifact(root, fixture)
    args = recovery_args(root, tmp_path / "summary.json", True)
    cloud = DeletedEndpointCloud(fixture, args)
    summary = {}
    if complete:
        prom.execute_recovery(args, summary, runner=cloud)
        assert summary["success"]
        assert summary["old_pool_delete"]["child_operation_unavailable_after_absence"]
        assert summary["old_pool_delete"]["pool_and_vmss_absent_at"]
    else:
        with pytest.raises(prom.workers.ReconcileError, match="Forbidden"):
            prom.execute_recovery(args, summary, runner=cloud)
        assert summary["success"] is False
    assert cloud.add_count == cloud.delete_count == 1


def test_raw_controller_contracts_are_hash_bound_not_compared_with_masked_values(tmp_path):
    root = tmp_path / "retirement-input"
    fixture = synthetic_raw_fixture()
    daemon = next(row for row in fixture["snapshot"]["controllers"]["items"] if row["kind"] == "DaemonSet")
    daemon["spec"]["template"]["spec"]["containers"][0]["env"] = [
        {"name": "AZURE_CREDENTIAL_FILE", "value": "/run/synthetic-azure-config.json"},
    ]
    fixture["receipt"]["current_kubernetes_diagnostics"] = prom.stalled.safe_diagnostics(fixture["snapshot"])
    write_raw_artifact(root, fixture)
    args = recovery_args(root, tmp_path / "unused.json", False)
    bundle = prom.load_retirement(args)
    assert bundle["controllers"] == prom.stalled.controllers_pin(fixture["snapshot"]["controllers"])
    assert bundle["controllers"] != prom.stalled.controllers_pin(
        fixture["receipt"]["current_kubernetes_diagnostics"]["controllers"],
    )
    harness = guard_harness(bundle)
    assert harness.guard(fixture["snapshot"]) is False
    changed = copy.deepcopy(fixture["snapshot"])
    next(row for row in changed["controllers"]["items"] if row["kind"] == "DaemonSet")[
        "spec"]["template"]["spec"]["containers"][0]["env"][0]["value"] = "/run/changed.json"
    with pytest.raises(prom.workers.ReconcileError, match="Controller"):
        harness.guard(changed)
    (root / "worker-state/current-controllers.json").write_text(
        json.dumps(changed["controllers"]), encoding="utf-8",
    )
    with pytest.raises(prom.workers.ReconcileError, match="input hashes changed"):
        prom.load_retirement(args)
