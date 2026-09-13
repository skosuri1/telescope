"""Stateful offline tests for three-role native failed-worker retirement."""

# pylint: disable=protected-access,too-many-lines,attribute-defined-outside-init,redefined-outer-name

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


MODULE_DIR = Path(__file__).resolve().parents[1] / "clusterloader2" / "clustermesh-scale"
SPEC = importlib.util.spec_from_file_location(
    "secondary_failed_worker_retirement",
    MODULE_DIR / "secondary_failed_worker_retirement.py",
)
retirement = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = retirement
sys.path.insert(0, str(MODULE_DIR))
try:
    SPEC.loader.exec_module(retirement)
finally:
    sys.path.pop(0)


def uid(name):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, name))


def meta(name, namespace="", row_uid=None):
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


def node(name, row_uid, pool, provider, *, ready=True):
    return {
        "kind": "Node",
        "metadata": {
            **meta(name, row_uid=row_uid),
            "labels": {
                "agentpool": pool, "kubernetes.azure.com/agentpool": pool,
                "kubernetes.azure.com/cluster": "node-rg",
                "kubernetes.io/os": "linux",
            },
        },
        "spec": {
            "providerID": provider, "unschedulable": False,
            "taints": [] if ready else [{
                "key": "node.kubernetes.io/unreachable", "effect": "NoSchedule",
            }],
        },
        "status": {
            "conditions": [{
                "type": "Ready", "status": "True" if ready else "Unknown",
                "lastHeartbeatTime": "2026-09-13T00:00:00Z",
            }],
            "allocatable": {"cpu": "8", "memory": "28Gi", "pods": "110"},
            "nodeInfo": {
                "bootID": uid(f"boot/{name}"), "kubeletVersion": "v1.35.7",
                "operatingSystem": "linux", "osImage": "Ubuntu 24.04",
            },
        },
    }


def mock_pod(role, index, node_name, controller_uid, *, deleting):
    pod = {
        "kind": "Pod",
        "metadata": {
            **meta(f"kwok-node-{index}", "mock-clustermesh", uid(f"mock/{role}/{index}")),
            "labels": {"app": "mock-cilium-agent"},
            "ownerReferences": [owner("StatefulSet", "kwok-node", controller_uid)],
            **({"deletionTimestamp": "2026-09-13T00:00:00Z"} if deleting else {}),
        },
        "spec": {
            "nodeName": node_name,
            "containers": [{
                "name": "mock", "image": "pinned",
                "env": [{"name": "NODE_NAME", "value": f"kwok-node-{index}"}],
                "resources": {"requests": {"cpu": "100m", "memory": "256Mi"}},
            }],
            "volumes": [],
        },
        "status": (
            {"phase": "Running", "conditions": [{"type": "Ready", "status": "False"}]}
            if deleting else ready_status(f"10.10.{int(role[5:])}.{index + 1}")
        ),
    }
    return pod


def network(name, node_uid, prefix, count):
    addresses = [f"10.{prefix}.0.{index}" for index in range(1, count + 1)]
    return {
        "kind": "NodeNetworkConfig",
        "metadata": {
            **meta(name, "kube-system", uid(f"nnc/{name}")),
            "ownerReferences": [owner("Node", name, node_uid)],
        },
        "spec": {"requestedIPCount": count},
        "status": {
            "assignedIPCount": count,
            "networkContainers": [{
                "id": uid(f"nc/{name}"), "version": 2,
                "ipAssignments": [{"ip": address} for address in addresses],
            }],
        },
    }


def system_pod(role, daemon, daemon_uid, node_name, *, ready=True):
    pod = {
        "kind": "Pod",
        "metadata": {
            **meta(f"{daemon}-{node_name}", "kube-system"),
            "labels": {"k8s-app": daemon},
            "ownerReferences": [owner("DaemonSet", daemon, daemon_uid)],
        },
        "spec": {
            "nodeName": node_name, "hostNetwork": True,
            "containers": [{"name": daemon, "image": "pinned"}],
        },
        "status": (
            ready_status(f"192.168.{int(role[5:])}.{1 if daemon == 'cilium' else 2}")
            if ready else {"phase": "Running", "conditions": [{"type": "Ready", "status": "False"}]}
        ),
    }
    return pod


def pool(name, count, role):
    return {
        "id": f"/clusters/{role}/agentPools/{name}",
        "name": name, "count": count,
        "mode": "System" if name in ("default", "cniv5") else "User",
        "vmSize": "Standard_D8s_v5" if name == "cniv5" else "Standard_D8_v3",
        "maxPods": 110 if name != "prompool" else 250,
        "osType": "Linux", "osSku": "Ubuntu", "osDiskType": "Managed",
        "osDiskSizeGb": 256, "kubeletDiskType": "OS",
        "enableAutoScaling": False, "enableFips": False,
        "enableEncryptionAtHost": False, "enableNodePublicIp": False,
        "nodeLabels": {}, "nodeTaints": None,
        "vnetSubnetId": f"/subnets/{role}-node",
        "podSubnetId": f"/subnets/{role}-pod",
        "currentOrchestratorVersion": "1.35.7",
        "provisioningState": "Succeeded", "powerState": {"code": "Running"},
    }


def vmss(role, pool_name, name, count, state):
    return {
        "id": f"/subscriptions/{retirement.qualification.capacity.SUBSCRIPTION}/resourceGroups/"
        f"node-{role}/providers/Microsoft.Compute/virtualMachineScaleSets/{name}",
        "name": name, "location": retirement.qualification.capacity.REGION,
        "provisioningState": state,
        "sku": {
            "name": "Standard_D8s_v5" if pool_name == "cniv5" else "Standard_D8_v3",
            "capacity": count,
        },
        "tags": {"aks-managed-poolName": pool_name},
    }


def instance(vmss_row, instance_id, name, vm_id, state):
    return {
        "id": f"{vmss_row['id']}/virtualMachines/{instance_id}",
        "instanceId": str(instance_id), "computerName": name, "vmId": vm_id,
        "provisioningState": state, "latestModelApplied": True,
    }


def ready_view():
    now = datetime.now(timezone.utc).isoformat()
    return {
        "statuses": [
            {"code": "ProvisioningState/succeeded"},
            {"code": "PowerState/running"},
        ],
        "vmAgent": {"statuses": [{
            "code": "ProvisioningState/succeeded", "displayStatus": "Ready", "time": now,
        }]},
        "extensions": [{
            "name": "vmssCSE",
            "statuses": [{"code": "ProvisioningState/succeeded"}],
        }],
    }


def failed_view(role):
    return {
        "statuses": [{
            "code": retirement.qualification.capacity.TERMINAL_CODES[role],
            "level": "Error", "message": "terminal source failure",
            "time": "2026-09-13T00:00:00+00:00",
        }, {"code": "PowerState/running"}],
        "vmAgent": {"statuses": [{
            "code": "ProvisioningState/Unavailable", "displayStatus": "Not Ready",
            "time": datetime.now(timezone.utc).isoformat(),
        }]},
        "extensions": [{"name": "vmssCSE", "statuses": None}],
    }


def build_role(role):
    settings = retirement.ROLE_SETTINGS[role]
    default_vmss = settings["target"][:-6]
    hold_instances = sorted(settings["retained_instances"])
    hold_nodes = [f"{default_vmss}{int(value):06d}" for value in hold_instances]
    prom_node = f"aks-prompool-{role[5:]}-vmss000000"
    prom_vmss = prom_node[:-6]
    new_vmss = f"aks-cniv5-{role[5:]}-vmss"
    new_nodes = [f"{new_vmss}{index:06d}" for index in range(2)]
    controller_uid = uid(f"statefulset/{role}")
    nodes = []
    target_provider = (
        f"azure:///subscriptions/{retirement.qualification.capacity.SUBSCRIPTION}/"
        f"resourceGroups/node-{role}/providers/Microsoft.Compute/"
        f"virtualMachineScaleSets/{default_vmss}/virtualMachines/{settings['target_instance']}"
    )
    target_node = node(
        settings["target"], settings["target_uid"], "default", target_provider, ready=False,
    )
    nodes.append(target_node)
    retained_vm_ids = {}
    for instance_id, name in zip(hold_instances, hold_nodes):
        provider = (
            f"azure:///subscriptions/{retirement.qualification.capacity.SUBSCRIPTION}/"
            f"resourceGroups/node-{role}/providers/Microsoft.Compute/"
            f"virtualMachineScaleSets/{default_vmss}/virtualMachines/{instance_id}"
        )
        nodes.append(node(name, uid(f"node/{name}"), "default", provider))
        retained_vm_ids[instance_id] = uid(f"vm/{name}")
    prom_provider = (
        f"azure:///subscriptions/{retirement.qualification.capacity.SUBSCRIPTION}/"
        f"resourceGroups/node-{role}/providers/Microsoft.Compute/"
        f"virtualMachineScaleSets/{prom_vmss}/virtualMachines/0"
    )
    nodes.append(node(prom_node, uid(f"node/{prom_node}"), "prompool", prom_provider))
    new_identities = {}
    for index, name in enumerate(new_nodes):
        provider = (
            f"azure:///subscriptions/{retirement.qualification.capacity.SUBSCRIPTION}/"
            f"resourceGroups/node-{role}/providers/Microsoft.Compute/"
            f"virtualMachineScaleSets/{new_vmss}/virtualMachines/{index}"
        )
        row = node(name, uid(f"node/{name}"), "cniv5", provider)
        nodes.append(row)
        new_identities[name] = {
            "node_name": name, "node_uid": row["metadata"]["uid"],
            "boot_id": row["status"]["nodeInfo"]["bootID"],
            "vm_id": uid(f"vm/{name}"), "provider_id": provider,
            "instance_id": str(index),
        }
    kwok = []
    leases = []
    now = datetime.now(timezone.utc).isoformat()
    for index in range(100):
        name = f"kwok-node-{index}"
        row = {
            "kind": "Node",
            "metadata": {
                **meta(name, row_uid=uid(f"kwok/{role}/{index}")),
                "labels": {"type": "kwok"},
            },
            "spec": {"taints": [{"key": "kwok", "effect": "NoSchedule"}]},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]},
        }
        kwok.append(row)
        leases.append({
            "kind": "Lease",
            "metadata": {
                **meta(name, "kube-node-lease", uid(f"lease/{role}/{name}")),
                "ownerReferences": [owner("Node", name, row["metadata"]["uid"])],
            },
            "spec": {
                "holderIdentity": f"kwok-controller-{role}",
                "leaseDurationSeconds": 40, "renewTime": now, "leaseTransitions": 1,
            },
        })
    pods = []
    healthy_count = 100 - settings["replacement_count"]
    raw_agents = {}
    for index in range(100):
        deleting = index >= healthy_count
        destination = settings["target"] if deleting else hold_nodes[index % len(hold_nodes)]
        pod = mock_pod(role, index, destination, controller_uid, deleting=deleting)
        pods.append(pod)
        raw_agents[pod["metadata"]["name"]] = copy.deepcopy(pod)
    daemonsets = []
    for daemon in ("cilium", "azure-cns"):
        daemon_uid = uid(f"daemon/{role}/{daemon}")
        daemonsets.append(["kube-system", daemon, daemon_uid])
        for node_name in [*hold_nodes, prom_node, *new_nodes]:
            pods.append(system_pod(role, daemon, daemon_uid, node_name))
        pods.append(system_pod(role, daemon, daemon_uid, settings["target"], ready=False))
    monitoring = {
        "kind": "Pod",
        "metadata": {
            **meta(f"grafana-{role}", "monitoring", uid(f"grafana/{role}")),
            "ownerReferences": [owner("ReplicaSet", f"grafana-{role}-rs", uid(f"grafana-rs/{role}"))],
        },
        "spec": {
            "nodeName": prom_node, "containers": [{"name": "grafana", "image": "pinned"}],
            "volumes": [],
        },
        "status": ready_status(f"10.20.{int(role[5:])}.1"),
    }
    pods.append(monitoring)
    target_monitoring = {
        "kind": "Pod",
        "metadata": {
            **meta(f"old-monitor-{role}", "monitoring", uid(f"old-monitor/{role}")),
            "deletionTimestamp": "2026-09-13T00:00:00Z",
            "ownerReferences": [owner("ReplicaSet", f"old-monitor-{role}-rs", uid(f"old-rs/{role}"))],
        },
        "spec": {
            "nodeName": settings["target"],
            "containers": [{"name": "monitor", "image": "pinned"}], "volumes": [],
        },
        "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "False"}]},
    }
    pods.append(target_monitoring)
    controllers = [{
        "kind": "StatefulSet",
        "metadata": {
            **meta("kwok-node", "mock-clustermesh", controller_uid), "generation": 3,
        },
        "spec": {
            "replicas": 100,
            "template": {"spec": {
                "schedulerName": "default-scheduler",
                "containers": [{
                    "name": "mock", "image": "pinned",
                    "resources": {"requests": {"cpu": "100m", "memory": "256Mi"}},
                }],
            }},
        },
    }]
    for daemon, daemon_uid in [(row[1], row[2]) for row in daemonsets]:
        controllers.append({
            "kind": "DaemonSet", "metadata": meta(daemon, "kube-system", daemon_uid),
            "spec": {"template": {"spec": {
                "containers": [{"name": daemon, "image": "pinned"}],
            }}},
        })
    pdbs = {"kind": "PodDisruptionBudgetList", "items": [{
        "kind": "PodDisruptionBudget",
        "metadata": {
            **meta("mock-pdb", "mock-clustermesh", uid(f"pdb/{role}")), "generation": 2,
        },
        "spec": {"minAvailable": 1, "unhealthyPodEvictionPolicy": "AlwaysAllow"},
        "status": {
            "observedGeneration": 2, "disruptionsAllowed": 1,
            "currentHealthy": 99, "desiredHealthy": 1,
        },
    }]}
    networks = []
    prefix = int(role[5:])
    for index, row in enumerate(nodes):
        count = 64 if row["metadata"]["name"] in new_nodes else 128
        networks.append(network(row["metadata"]["name"], row["metadata"]["uid"], prefix + index, count))
    addresses = {
        row["metadata"]["name"]: [
            entry["ip"] for entry in row["status"]["networkContainers"][0]["ipAssignments"]
        ] for row in networks
    }
    used_addresses = {name: 0 for name in addresses}
    for pod in pods:
        node_name = pod["spec"].get("nodeName")
        if (
            node_name in addresses and not pod["spec"].get("hostNetwork")
            and not pod["metadata"].get("deletionTimestamp")
        ):
            pod["status"] = ready_status(addresses[node_name][used_addresses[node_name]])
            used_addresses[node_name] += 1
    snapshot = {
        "nodes": {"kind": "NodeList", "items": [*nodes, *kwok]},
        "pods": {"kind": "PodList", "items": pods},
        "nnc": {"kind": "NodeNetworkConfigList", "items": networks},
        "controllers": {"kind": "List", "items": controllers},
        "pdbs": pdbs,
        "kwok_leases": {"kind": "LeaseList", "items": leases},
    }
    target_pods = {
        pod["metadata"]["uid"]: {
            "name": pod["metadata"]["name"], "namespace": pod["metadata"]["namespace"],
            "uid": pod["metadata"]["uid"],
            "owner": next(item for item in pod["metadata"]["ownerReferences"]
                          if item.get("controller") is True),
            "deleting": bool(pod["metadata"].get("deletionTimestamp")),
            "pvc_or_ephemeral_claim": False, "ready": False,
            "spec_sha256": retirement.digest(pod["spec"]),
        }
        for pod in pods if pod["spec"].get("nodeName") == settings["target"]
    }
    healthy_uids = {
        name: pod["metadata"]["uid"] for name, pod in raw_agents.items()
        if not pod["metadata"].get("deletionTimestamp")
    }
    target_uids = {
        name: pod["metadata"]["uid"] for name, pod in raw_agents.items()
        if pod["metadata"].get("deletionTimestamp")
    }
    default_scale = vmss(
        role, "default", default_vmss, settings["initial_count"], "Failed",
    )
    cni_scale = vmss(role, "cniv5", new_vmss, 2, "Succeeded")
    prom_scale = vmss(role, "prompool", prom_vmss, 1, "Succeeded")
    default_instances = [
        instance(default_scale, settings["target_instance"], settings["target"],
                 settings["target_vm"], "Failed"),
        *[
            instance(default_scale, value, name, retained_vm_ids[value], "Succeeded")
            for value, name in zip(hold_instances, hold_nodes)
        ],
    ]
    cni_instances = [
        instance(cni_scale, index, name, new_identities[name]["vm_id"], "Succeeded")
        for index, name in enumerate(new_nodes)
    ]
    prom_instances = [
        instance(prom_scale, 0, prom_node, uid(f"vm/{prom_node}"), "Succeeded")
    ]
    q_journal_data = {
        "owner": retirement.qualification.OWNER,
        "token": uid(f"q-token/{role}").replace("-", "")[:32],
        "role": role, "capacity_source_build_id": "80039",
        "capacity_tree_sha256": "c" * 64, "source_tree_sha256": "d" * 64,
        "record": "qualified",
    }
    q_journal = {
        "kind": "ConfigMap", "apiVersion": "v1",
        "metadata": {
            **meta(
                f"secondary-capacity-qualification-{role}", "kube-system",
                settings["qualification_journal_uid"],
            ),
            "resourceVersion": settings["qualification_journal_rv"],
        },
        "data": q_journal_data,
    }
    capacity_journal = {
        "kind": "ConfigMap", "apiVersion": "v1",
        "metadata": {
            **meta(f"secondary-capacity-recovery-{role}", "kube-system",
                   uid(f"capacity-journal/{role}")),
            "resourceVersion": "50",
        },
        "data": {"owner": "capacity", "token": uid(f"capacity-token/{role}")},
    }
    legacy_journal = {
        "kind": "ConfigMap", "apiVersion": "v1",
        "metadata": {
            **meta(f"legacy-worker-journal-{role}", "kube-system",
                   uid(f"legacy-journal/{role}")),
            "resourceVersion": "9",
        },
        "data": {"owner": "legacy", "token": uid(f"legacy-token/{role}")},
    }
    snapshot["configmaps"] = {
        "kind": "ConfigMapList", "apiVersion": "v1",
        "items": [capacity_journal, q_journal, legacy_journal],
    }
    source = {
        "cluster": {
            "name": f"clustermesh-{role[5:]}",
            "id": f"/subscriptions/{retirement.qualification.capacity.SUBSCRIPTION}/"
            f"resourceGroups/{retirement.qualification.capacity.RESOURCE_GROUP}/clusters/{role}",
        },
        "node_group": f"node-{role}",
        "node_contracts": {
            row["metadata"]["name"]: retirement.qualification.capacity.node_contract(row)
            for row in nodes if row["metadata"]["name"] not in new_nodes
        },
        "kwok_contracts": {
            row["metadata"]["name"]: retirement.qualification.capacity.node_contract(row)
            for row in kwok
        },
        "mock_contracts": {
            name: retirement.qualification.capacity.pod_contract(pod)
            for name, pod in raw_agents.items()
        },
        "failed": {
            "node_name": settings["target"], "node_uid": settings["target_uid"],
            "vm_id": settings["target_vm"],
            "terminal_failure": retirement.qualification.capacity.terminal_failure(
                failed_view(role), retirement.qualification.capacity.TERMINAL_CODES[role],
            ),
        },
        "daemonsets": daemonsets,
    }
    q_receipt = {
        "final_evidence": {
            "controller_pins": retirement.qualification.controller_pins(snapshot["controllers"]),
            "pdbs": retirement.qualification.pdb_evidence(snapshot["pdbs"]),
            "kwok_node_leases": {
                row["metadata"]["name"]: {
                    "uid": row["metadata"]["uid"],
                    "owner": row["metadata"]["ownerReferences"][0],
                    "holder_identity": row["spec"]["holderIdentity"],
                    "lease_duration_seconds": row["spec"]["leaseDurationSeconds"],
                } for row in leases
            },
            "protected_healthy_mock_uids": healthy_uids,
            "protected_healthy_source_nodes": {
                name: {
                    "uid": next(row for row in nodes if row["metadata"]["name"] == name)["metadata"]["uid"],
                    "boot_id": next(row for row in nodes if row["metadata"]["name"] == name)[
                        "status"]["nodeInfo"]["bootID"],
                } for name in hold_nodes
            },
            "target_host": {
                "terminating_mock_uids": target_uids,
                "target_pods": target_pods,
                "failed_target": source["failed"],
            },
            "future_native_fencing_hold": {
                "target_nodes": hold_nodes, "key": retirement.qualification.NATIVE_HOLD_KEY,
            },
            "mock_statefulset": {"uid": controller_uid},
            "system_daemonsets": {
                name: {"all_expected_ready": True} for name in new_nodes
            },
        },
        "placement_headroom": {
            "healthy_rss_high_water_bytes": 128 * 1024**2,
            "destinations": {name: {"safe_slots": 100} for name in new_nodes},
        },
        "journal": {
            "name": f"secondary-capacity-qualification-{role}",
            "uid": settings["qualification_journal_uid"],
            "resource_version": settings["qualification_journal_rv"],
            "data_sha256": retirement.digest(q_journal_data),
        },
    }
    capacity_role = {
        "desired_configuration": {"count": 2},
        "new_identities": new_identities,
    }
    monitoring_pins = {
        monitoring["metadata"]["uid"]: retirement.qualification.pod_contract(monitoring),
        target_monitoring["metadata"]["uid"]: retirement.qualification.pod_contract(target_monitoring),
    }
    role_bundle = {
        "source": source, "capacity": capacity_role, "receipt": q_receipt,
        "final_objects": copy.deepcopy(snapshot), "raw_agents": raw_agents,
        "raw_pods": {pod["metadata"]["uid"]: copy.deepcopy(pod) for pod in pods},
        "monitoring_pins": monitoring_pins,
        "qualification_journal_data": q_journal_data,
        "journal_pins": retirement.journal_rows(snapshot["configmaps"]),
    }
    state = {
        "snapshot": snapshot,
        "pools": [pool("default", settings["initial_count"], role),
                  pool("cniv5", 2, role), pool("prompool", 1, role)],
        "vmsses": [default_scale, cni_scale, prom_scale],
        "instances": {
            default_vmss: default_instances,
            new_vmss: cni_instances,
            prom_vmss: prom_instances,
        },
        "views": {
            **{f"{default_vmss}/{row['instanceId']}":
               failed_view(role) if row["instanceId"] == settings["target_instance"] else ready_view()
               for row in default_instances},
            **{f"{new_vmss}/{row['instanceId']}": ready_view() for row in cni_instances},
            f"{prom_vmss}/0": ready_view(),
        },
        "child_operation": {
            "name": uid(f"old-child-operation/{role}"), "status": "Succeeded",
            "operationType": "PutAgentPool", "startTime": "2026-09-13T00:00:00+00:00",
            "endTime": "2026-09-13T00:01:00+00:00", "errorCode": None,
        },
        "top_operation": {
            "name": uid(f"old-top-operation/{role}"), "status": "Succeeded",
            "operationType": "PutAgentPool", "startTime": "2026-09-13T00:00:00+00:00",
            "endTime": "2026-09-13T00:01:00+00:00", "errorCode": None,
        },
        "default_vmss": default_vmss, "new_nodes": new_nodes,
        "hold_nodes": hold_nodes, "target_uids": target_uids,
        "controller_uid": controller_uid,
    }
    state["vmss_models"] = {
        scale["name"]: {"id": scale["id"], "osDisk": {"osType": "Linux", "diskSizeGb": 256},
                        "imageReference": {"id": "/images/qualified"}}
        for scale in state["vmsses"]
    }
    q_receipt["ip_growth"] = {
        name: {"http_proven": True, "probe_count": settings["replacement_count"] // 2 + 1,
               "after": retirement.qualification.capacity.network_record(
                   next(row for row in snapshot["nnc"]["items"] if row["metadata"]["name"] == name)
               )}
        for name in new_nodes
    }
    role_bundle["observation"] = {
        "pools": copy.deepcopy(state["pools"]), "vmsses": copy.deepcopy(state["vmsses"]),
        "vmss_models": copy.deepcopy(state["vmss_models"]),
        "instances": copy.deepcopy(state["instances"]),
        "operations": {
            "default": copy.deepcopy(state["child_operation"]),
            **{pool_name: {"name": uid(f"operation/{role}/{pool_name}"), "status": "Succeeded"}
               for pool_name in ("cniv5", "prompool")},
        },
    }
    return role_bundle, state


def build_environment(tmp_path):
    input_dir = tmp_path / "qualification"
    input_dir.mkdir()
    (input_dir / "qualification.json").write_text('{"pinned":true}', encoding="utf-8")
    roles, states = {}, {}
    for role in retirement.ROLES:
        roles[role], states[role] = build_role(role)
    hashes = retirement.hash_tree(input_dir)
    bundle = {
        "root": input_dir, "hashes": hashes, "tree_sha256": retirement.digest(hashes),
        "receipt": {
            "source_tree_sha256": "d" * 64, "capacity_input_sha256": "c" * 64,
        },
        "roles": roles,
    }
    return bundle, states


class StatefulCloud:
    def __init__(self, states):
        self.states = states
        self.writes = []
        self.native_roles = []
        self.progress_reads = {role: 0 for role in retirement.ROLES}
        self.native_error_role = None
        self.no_hold_rv_advance = False
        self.early_replacement_role = None
        self.wrong_vm_role = None

    @staticmethod
    def value(command, flag):
        return command[command.index(flag) + 1]

    def role(self, command):
        if command[0] == "kubectl":
            context = self.value(command, "--context")
            return f"mesh-{context.split('-')[-1]}"
        for flag in ("--cluster-name", "--name"):
            if flag in command:
                value = self.value(command, flag)
                if value.startswith("clustermesh-"):
                    return f"mesh-{value.split('-')[-1]}"
        if "--resource-group" in command:
            group = self.value(command, "--resource-group")
            for role in retirement.ROLES:
                if group == f"node-{role}":
                    return role
        return None

    def refresh(self, role):
        now = datetime.now(timezone.utc).isoformat()
        state = self.states[role]
        for lease in state["snapshot"]["kwok_leases"]["items"]:
            lease["spec"]["renewTime"] = now
        for view in state["views"].values():
            guest = (view.get("vmAgent") or {}).get("statuses") or []
            if guest:
                guest[0]["time"] = now

    def complete_native(self, role):
        state = self.states[role]
        settings = retirement.ROLE_SETTINGS[role]
        state["child_operation"] = {
            "name": uid(f"native-operation/{role}"), "status": "Succeeded",
            "id": f"/subscriptions/{retirement.qualification.capacity.SUBSCRIPTION}/resourceGroups/"
                  f"{retirement.qualification.capacity.RESOURCE_GROUP}/clusters/{role}/agentPools/default/"
                  f"operations/{uid(f'native-operation/{role}')}",
            "operationType": "DeleteMachines",
            "startTime": state["native_started"], "endTime": datetime.now(timezone.utc).isoformat(),
            "errorCode": None,
        }
        default_pool = next(row for row in state["pools"] if row["name"] == "default")
        default_pool["count"] = settings["final_count"]
        default_pool["provisioningState"] = "Succeeded"
        default_scale = next(row for row in state["vmsses"]
                             if row["tags"]["aks-managed-poolName"] == "default")
        default_scale["sku"]["capacity"] = settings["final_count"]
        default_scale["provisioningState"] = "Succeeded"
        rows = state["instances"][state["default_vmss"]]
        state["instances"][state["default_vmss"]] = [
            row for row in rows if row["instanceId"] != settings["target_instance"]
        ]
        if self.wrong_vm_role == role:
            state["instances"][state["default_vmss"]].append(
                instance(default_scale, "9", f"{state['default_vmss']}000009",
                         uid(f"foreign/{role}"), "Succeeded")
            )
            state["views"][f"{state['default_vmss']}/9"] = ready_view()
        snapshot = state["snapshot"]
        snapshot["nodes"]["items"] = [
            row for row in snapshot["nodes"]["items"]
            if row["metadata"]["name"] != settings["target"]
        ]
        snapshot["nnc"]["items"] = [
            row for row in snapshot["nnc"]["items"]
            if row["metadata"]["name"] != settings["target"]
        ]
        target_names = set(state["target_uids"])
        preserved = [
            pod for pod in snapshot["pods"]["items"]
            if pod["spec"].get("nodeName") != settings["target"]
            and not (pod["metadata"].get("namespace") == "mock-clustermesh"
                     and pod["metadata"]["name"] in target_names)
        ]
        agents = {
            pod["metadata"]["name"]: pod for pod in snapshot["pods"]["items"]
            if pod["metadata"].get("namespace") == "mock-clustermesh"
        }
        replacements = []
        addresses = {
            name: [
                entry["ip"] for row in snapshot["nnc"]["items"]
                if row["metadata"]["name"] == name
                for entry in row["status"]["networkContainers"][0]["ipAssignments"]
            ] for name in state["new_nodes"]
        }
        used = {name: 5 for name in state["new_nodes"]}
        for index, name in enumerate(sorted(target_names)):
            original = agents[name]
            destination = state["new_nodes"][index % 2]
            pod = copy.deepcopy(original)
            pod["metadata"]["uid"] = uid(f"replacement/{role}/{name}")
            pod["metadata"]["resourceVersion"] = "2"
            pod["metadata"].pop("deletionTimestamp", None)
            pod["spec"]["nodeName"] = destination
            pod["status"] = ready_status(addresses[destination][used[destination]])
            used[destination] += 1
            replacements.append(pod)
        snapshot["pods"]["items"] = [*preserved, *replacements]

    def advance(self, role):
        state = self.states[role]
        if "native_started" not in state:
            return
        self.progress_reads[role] += 1
        if self.progress_reads[role] == 1:
            state["child_operation"] = {
                "name": uid(f"native-operation/{role}"), "status": "InProgress",
                "id": f"/subscriptions/{retirement.qualification.capacity.SUBSCRIPTION}/resourceGroups/"
                      f"{retirement.qualification.capacity.RESOURCE_GROUP}/clusters/{role}/agentPools/default/"
                      f"operations/{uid(f'native-operation/{role}')}",
                "operationType": "DeleteMachines", "startTime": state["native_started"],
                "endTime": None, "errorCode": None,
            }
            if self.early_replacement_role == role:
                name = next(iter(state["target_uids"]))
                pod = next(row for row in state["snapshot"]["pods"]["items"]
                           if row["metadata"].get("name") == name
                           and row["metadata"].get("namespace") == "mock-clustermesh")
                pod["metadata"]["uid"] = uid(f"early/{role}/{name}")
            return
        self.complete_native(role)

    def apply_patch(self, row, operations):
        old_rv = row["metadata"]["resourceVersion"]
        for operation in operations:
            path = operation["path"]
            if operation["op"] == "test":
                if path == "/metadata/uid":
                    assert row["metadata"]["uid"] == operation["value"]
                elif path == "/metadata/resourceVersion":
                    assert row["metadata"]["resourceVersion"] == operation["value"]
                elif path == "/data/token":
                    assert row["data"]["token"] == operation["value"]
                elif path == "/data":
                    assert row["data"] == operation["value"]
                elif path == "/spec/taints":
                    assert row["spec"]["taints"] == operation["value"]
            elif path == "/data":
                row["data"] = copy.deepcopy(operation["value"])
            elif path == "/spec/taints":
                row["spec"]["taints"] = copy.deepcopy(operation["value"])
        if not self.no_hold_rv_advance or "data" in row:
            row["metadata"]["resourceVersion"] = str(int(old_rv) + 1)

    def kubernetes(self, command, role):
        state = self.states[role]
        snapshot = state["snapshot"]
        if "--raw=/readyz" in command:
            return "ok"
        if "create" in command and "configmap" in command:
            name = command[command.index("configmap") + 1]
            data = dict(word.removeprefix("--from-literal=").split("=", 1)
                        for word in command if word.startswith("--from-literal="))
            row = {
                "kind": "ConfigMap", "apiVersion": "v1",
                "metadata": {
                    **meta(name, "kube-system", uid(f"retirement-journal/{role}")),
                    "resourceVersion": "1",
                },
                "data": data,
            }
            snapshot["configmaps"]["items"].append(row)
            self.writes.append(("journal-create", role, name))
            return json.dumps(row)
        if "patch" in command and "configmap" in command:
            name = command[command.index("configmap") + 1]
            row = next(item for item in snapshot["configmaps"]["items"]
                       if item["metadata"]["name"] == name)
            self.apply_patch(row, json.loads(self.value(command, "-p")))
            self.writes.append(("journal-patch", role, name))
            return json.dumps(row)
        if "patch" in command and "node" in command:
            name = command[command.index("node") + 1]
            row = next(item for item in snapshot["nodes"]["items"]
                       if item["metadata"]["name"] == name)
            self.apply_patch(row, json.loads(self.value(command, "-p")))
            self.writes.append(("node-patch", role, name))
            return json.dumps(row)
        if "configmap" in command and "configmaps" not in command:
            name = command[command.index("configmap") + 1]
            row = next(item for item in snapshot["configmaps"]["items"]
                       if item["metadata"]["name"] == name)
            return json.dumps(row)
        if "configmaps" in command:
            return json.dumps(snapshot["configmaps"])
        if "node" in command and "nodes" not in command:
            name = command[command.index("node") + 1]
            row = next(item for item in snapshot["nodes"]["items"]
                       if item["metadata"]["name"] == name)
            return json.dumps(row)
        if "nodes" in command and "--raw" not in command:
            return json.dumps(snapshot["nodes"])
        if "pods" in command and "--raw" not in command:
            return json.dumps(snapshot["pods"])
        if "nodenetworkconfigs" in command:
            return json.dumps(snapshot["nnc"])
        if "deployments,replicasets,daemonsets,statefulsets" in command:
            return json.dumps(snapshot["controllers"])
        if "pdb" in command:
            return json.dumps(snapshot["pdbs"])
        if "leases" in command:
            return json.dumps(snapshot["kwok_leases"])
        now = datetime.now(timezone.utc).isoformat()
        if "/apis/metrics.k8s.io/v1beta1/nodes" in command:
            return json.dumps({"items": [{
                "metadata": {"name": row["metadata"]["name"]},
                "timestamp": now, "usage": {"memory": "1Gi", "cpu": "500m"},
            } for row in snapshot["nodes"]["items"]
            if row["metadata"].get("labels", {}).get("type") != "kwok"]})
        if any("/apis/metrics.k8s.io" in item and "/pods" in item for item in command):
            return json.dumps({"items": [{
                "metadata": {
                    "name": pod["metadata"]["name"], "namespace": "mock-clustermesh",
                },
                "timestamp": now,
                "containers": [{"name": "mock", "usage": {"memory": "128Mi"}}],
            } for pod in snapshot["pods"]["items"]
            if pod["metadata"].get("namespace") == "mock-clustermesh"]})
        raise AssertionError(command)

    def azure(self, command, role):
        if command[1:3] == ["account", "show"]:
            return json.dumps({"id": retirement.qualification.capacity.SUBSCRIPTION})
        if command[1:3] == ["group", "show"]:
            name = self.value(command, "--name")
            if name == retirement.qualification.capacity.RESOURCE_GROUP:
                return json.dumps({
                    "name": name, "location": retirement.qualification.capacity.REGION,
                    "tags": {
                        "run_id": name, "clustermesh_debug_preserved": "true",
                        "scenario": "perf-eval-clustermesh-scale", "clustermesh_debug_expected_clusters": "100",
                        "clustermesh_debug_tfvars_sha256": retirement.qualification.capacity.TFVARS_SHA,
                        "deletion_due_time": (datetime.now(timezone.utc) + timedelta(days=2)).isoformat(),
                    },
                })
            selected = name.removeprefix("node-")
            return json.dumps({
                "name": name, "location": retirement.qualification.capacity.REGION,
                "managedBy": f"/subscriptions/{retirement.qualification.capacity.SUBSCRIPTION}/"
                             f"resourceGroups/{retirement.qualification.capacity.RESOURCE_GROUP}/clusters/{selected}",
                "tags": {"deletion_due_time": (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()},
            })
        state = self.states[role]
        self.refresh(role)
        if command[1:3] == ["aks", "show"]:
            source = f"/subscriptions/{retirement.qualification.capacity.SUBSCRIPTION}/" \
                     f"resourceGroups/{retirement.qualification.capacity.RESOURCE_GROUP}/clusters/{role}"
            return json.dumps({
                "id": source, "name": f"clustermesh-{role[5:]}",
                "provisioningState": "Succeeded",
                "nodeResourceGroup": f"node-{role}",
                "currentKubernetesVersion": "1.35.7", "kubernetesVersion": "1.35",
            })
        if command[1:4] == ["aks", "nodepool", "list"]:
            self.advance(role)
            return json.dumps(state["pools"])
        if command[1:3] == ["vmss", "list"] and command[2] != "list-instances":
            return json.dumps(state["vmsses"])
        if command[1:3] == ["vmss", "list-instances"]:
            return json.dumps(state["instances"][self.value(command, "--name")])
        if command[1:3] == ["vmss", "show"]:
            return json.dumps(state["vmss_models"][self.value(command, "--name")])
        if command[1:3] == ["vmss", "get-instance-view"]:
            key = f"{self.value(command, '--name')}/{self.value(command, '--instance-id')}"
            return json.dumps(state["views"][key])
        if command[1:4] == ["aks", "operation", "show-latest"]:
            if "--nodepool-name" in command:
                pool_name = self.value(command, "--nodepool-name")
                if pool_name == "default":
                    return json.dumps(state["child_operation"])
                return json.dumps({
                    "name": uid(f"operation/{role}/{pool_name}"), "status": "Succeeded",
                    "operationType": "PutAgentPool",
                    "startTime": "2026-09-13T00:00:00+00:00",
                    "endTime": "2026-09-13T00:01:00+00:00", "errorCode": None,
                })
            return json.dumps(state["top_operation"])
        if command[1:4] == ["aks", "nodepool", "delete-machines"]:
            self.native_roles.append(role)
            self.writes.append(("native", role, retirement.ROLE_SETTINGS[role]["target"]))
            state["native_started"] = datetime.now(timezone.utc).isoformat()
            if self.native_error_role == role:
                raise retirement.workers.ReconcileError("native response lost after delivery")
            return ""
        raise AssertionError(command)

    def __call__(self, command, _timeout):
        role = self.role(command)
        if command[0] == "kubectl":
            return self.kubernetes(command, role)
        return self.azure(command, role)


def args(tmp_path, bundle, *, execute=False, name="summary.json"):
    kube = tmp_path / f"kube-{name}"
    kube.mkdir()
    for role in retirement.ROLES:
        (kube / f"{role}.config").write_text("private", encoding="utf-8")
    return SimpleNamespace(
        qualification_directory=str(bundle["root"]),
        qualification_build_id=80046,
        resource_group=retirement.qualification.capacity.RESOURCE_GROUP,
        confirm_resource_group=retirement.qualification.capacity.RESOURCE_GROUP,
        expected_subscription=retirement.qualification.capacity.SUBSCRIPTION,
        expected_region=retirement.qualification.capacity.REGION,
        expected_tfvars_sha=retirement.qualification.capacity.TFVARS_SHA,
        kubeconfig_directory=str(kube), summary_file=str(tmp_path / name),
        timeout_seconds=1200, request_timeout_seconds=60, execute=execute,
    )


@pytest.fixture(name="environment")
def fixture_environment(tmp_path, monkeypatch):
    bundle, states = build_environment(tmp_path)
    cloud = StatefulCloud(states)
    monkeypatch.setattr(retirement, "load_inputs", lambda _args: bundle)
    monkeypatch.setattr(retirement.time, "sleep", lambda _seconds: None)
    return tmp_path, bundle, cloud


def receipt(value):
    return json.loads(Path(value.summary_file).read_text(encoding="utf-8"))


def test_plan_preflights_all_three_roles_without_writes(environment):
    tmp_path, bundle, cloud = environment
    value = args(tmp_path, bundle, name="plan.json")
    retirement.execute_recovery(value, {}, cloud)
    result = receipt(value)
    assert result["execute"] is False and result["plan_valid"] is True
    assert result["mutation_started"] is False
    assert result["success"] and result["status"] == "plan-valid"
    assert not result["system_roles_recovered"]
    assert result["mesh89_explicitly_unqualified"] is True
    assert result["unresolved_roles"] == ["mesh-2", "mesh-89", "mesh-94"]
    assert all(row["status"] == "plan-valid" for row in result["per_role"].values())
    assert not cloud.writes


def test_execute_holds_deletes_replaces_and_cleans_all_three_roles(environment, capsys):
    tmp_path, bundle, cloud = environment
    value = args(tmp_path, bundle, execute=True, name="execute.json")
    retirement.execute_recovery(value, {}, cloud)
    result = receipt(value)
    assert result["execute"] is True and result["success"] is True
    assert result["system_roles_recovered"] is True
    assert result["workloads_ready"] is False
    assert result["completed_global_baseline"] is False
    assert result["cleanup_errors"] == []
    assert cloud.native_roles == list(retirement.ROLES)
    for role, row in result["per_role"].items():
        assert row["source_retired"] and row["native_fencing_proven"]
        assert row["replacements_ready"] and row["placement_holds_removed"]
        assert len(row["replacements"]) == retirement.ROLE_SETTINGS[role]["replacement_count"]
        assert row["native"]["accepted"] is True and row["native"]["ambiguous"] is False
        assert all(not hold["applied"] for hold in row["holds"].values())
        assert row["final_headroom"]["actual_metrics"] is True
    assert not any(write[0] not in {
        "journal-create", "journal-patch", "node-patch", "native",
    } for write in cloud.writes)
    assert not any("mesh-89" in write for write in cloud.writes)
    output = capsys.readouterr().out
    for role in retirement.ROLES:
        assert f"role={role} action=global-preflight-complete" in output
        assert f"role={role} action=native-delete-submitting" in output
        assert f"role={role} action=post-action-observation" in output
        assert f"role={role} action=role-recovery-complete" in output


def test_ambiguous_native_stops_batch_and_preserves_owned_holds(environment):
    tmp_path, bundle, cloud = environment
    cloud.native_error_role = "mesh-51"
    value = args(tmp_path, bundle, execute=True, name="ambiguous.json")
    with pytest.raises(retirement.workers.ReconcileError, match="response lost"):
        retirement.execute_recovery(value, {}, cloud)
    result = receipt(value)
    assert cloud.native_roles == ["mesh-51"]
    assert result["per_role"]["mesh-51"]["native"]["ambiguous"] is True
    assert any(hold["applied"] for hold in result["per_role"]["mesh-51"]["holds"].values())
    assert result["per_role"]["mesh-66"]["native"]["attempted"] is False
    assert not result["system_roles_recovered"]


def test_new_mock_uid_before_positive_vm_absence_fails_closed(environment):
    tmp_path, bundle, cloud = environment
    cloud.early_replacement_role = "mesh-51"
    value = args(tmp_path, bundle, execute=True, name="early.json")
    with pytest.raises(retirement.workers.ReconcileError, match="before positive VM absence"):
        retirement.execute_recovery(value, {}, cloud)
    assert cloud.native_roles == ["mesh-51"]
    assert not receipt(value)["system_roles_recovered"]


def test_wrong_vm_inventory_cannot_certify_fencing(environment):
    tmp_path, bundle, cloud = environment
    cloud.wrong_vm_role = "mesh-51"
    value = args(tmp_path, bundle, execute=True, name="wrong-vm.json")
    with pytest.raises(retirement.workers.ReconcileError, match="target VM remains|protected default VM"):
        retirement.execute_recovery(value, {}, cloud)
    assert not receipt(value)["per_role"]["mesh-51"]["native_fencing_proven"]


def test_hold_patch_requires_resource_version_advance(environment):
    tmp_path, bundle, cloud = environment
    cloud.no_hold_rv_advance = True
    value = args(tmp_path, bundle, execute=True, name="hold-cas.json")
    with pytest.raises(retirement.workers.ReconcileError, match="hold add response is ambiguous"):
        retirement.execute_recovery(value, {}, cloud)
    assert not cloud.native_roles


@pytest.mark.parametrize("fault", ["healthy-uid", "pdb", "journal", "metrics"])
def test_preflight_safety_drift_blocks_all_native_writes(environment, fault):
    tmp_path, bundle, cloud = environment
    state = cloud.states["mesh-51"]
    if fault == "healthy-uid":
        healthy_name = state["hold_nodes"][0]
        pod = next(row for row in state["snapshot"]["pods"]["items"]
                   if row["spec"].get("nodeName") == healthy_name
                   and row["metadata"].get("namespace") == "mock-clustermesh")
        pod["metadata"]["uid"] = uid("changed-healthy")
    elif fault == "pdb":
        state["snapshot"]["pdbs"]["items"][0]["status"]["disruptionsAllowed"] = 0
    elif fault == "journal":
        journal = next(row for row in state["snapshot"]["configmaps"]["items"]
                       if row["metadata"]["name"].startswith("secondary-capacity-qualification"))
        journal["data"]["record"] = "changed"
    else:
        original = cloud.kubernetes

        def missing_metrics(command, role):
            if "/apis/metrics.k8s.io/v1beta1/nodes" in command:
                return json.dumps({"items": []})
            return original(command, role)

        cloud.kubernetes = missing_metrics
    value = args(tmp_path, bundle, execute=True, name=f"{fault}.json")
    with pytest.raises((retirement.workers.ReconcileError, retirement.mocks.RecoveryError)):
        retirement.execute_recovery(value, {}, cloud)
    assert not cloud.native_roles
    assert not any(write[0] == "node-patch" for write in cloud.writes)


def test_actual_partial_80046_loader_accepts_only_three_completed_roles():
    path = (
        "/home/skosuri/.copilot/session-state/478bd706-9d9d-436f-a721-2f32d3afcb77/"
        "files/mesh96-scoped-recovery/secondary-qualification-ddacefe17875-from-80039/artifact"
    )
    if not Path(path).is_dir():
        pytest.skip("session artifact is not available")
    loaded = retirement.load_inputs(SimpleNamespace(qualification_directory=path))
    assert set(loaded["roles"]) == set(retirement.ROLES)
    assert loaded["receipt"]["per_role"]["mesh-89"]["capacity_qualified"] is False
    assert sum(len(row["receipt"]["probe_receipts"]) for row in loaded["roles"].values()) == 149
    dirty = copy.deepcopy(loaded["receipt"])
    dirty["per_role"]["mesh-51"]["probe_cleanup_pending"] = ["leftover"]
    with pytest.raises(retirement.workers.ReconcileError, match="exact completed"):
        retirement._validate_completed_role(
            dirty, "mesh-51", loaded["roles"]["mesh-51"]["final_objects"],
        )


def test_cli_contract_requires_exact_three_private_kubeconfigs(environment):
    tmp_path, bundle, _ = environment
    value = args(tmp_path, bundle, name="cli.json")
    (Path(value.kubeconfig_directory) / "mesh-89.config").write_text("private", encoding="utf-8")
    with pytest.raises(retirement.workers.ReconcileError, match="exactly mesh-51/66/79"):
        retirement.validate_args(value)
    parsed = retirement.parse_args([
        "--qualification-directory", str(bundle["root"]),
        "--qualification-build-id", "80046",
        "--resource-group", retirement.qualification.capacity.RESOURCE_GROUP,
        "--confirm-resource-group", retirement.qualification.capacity.RESOURCE_GROUP,
        "--expected-subscription", retirement.qualification.capacity.SUBSCRIPTION,
        "--expected-region", retirement.qualification.capacity.REGION,
        "--expected-tfvars-sha", retirement.qualification.capacity.TFVARS_SHA,
        "--kubeconfig-directory", value.kubeconfig_directory,
        "--summary-file", str(tmp_path / "parsed.json"),
        "--timeout-seconds", "7200",
    ])
    assert parsed.qualification_build_id == 80046 and parsed.timeout_seconds == 7200
    assert not parsed.execute


@pytest.mark.parametrize("fault", ["pool-model", "vm-identity", "node-taint", "future-capacity", "pod-condition"])
def test_fresh_source_and_replacement_capacity_fail_before_any_mutation(environment, fault):
    tmp_path, bundle, cloud = environment
    state = cloud.states["mesh-51"]
    if fault == "pool-model":
        state["pools"][0]["maxPods"] = 250
    elif fault == "vm-identity":
        state["instances"][state["default_vmss"]][-1]["vmId"] = uid("unapproved-survivor")
    elif fault == "node-taint":
        row = next(node for node in state["snapshot"]["nodes"]["items"]
                   if node["metadata"]["name"] in state["new_nodes"])
        row["spec"]["taints"] = [{"key": "unapproved", "effect": "NoSchedule"}]
    elif fault == "future-capacity":
        for row in state["snapshot"]["nodes"]["items"]:
            if row["metadata"]["name"] in state["new_nodes"]:
                row["status"]["allocatable"]["cpu"] = "2"
    else:
        pod = next(pod for pod in state["snapshot"]["pods"]["items"]
                   if pod["spec"].get("nodeName") in state["hold_nodes"]
                   and pod["metadata"]["namespace"] == "mock-clustermesh")
        pod["status"]["conditions"] = [{"type": "Ready", "status": "False"}]
    with pytest.raises((retirement.workers.ReconcileError, retirement.mocks.RecoveryError)):
        retirement.execute_recovery(args(tmp_path, bundle, execute=True, name=f"drift-{fault}.json"), {}, cloud)
    assert not cloud.writes


def test_absent_taints_and_stale_failed_container_readiness_are_handled(environment):
    tmp_path, bundle, cloud = environment
    for role, state in cloud.states.items():
        for row in state["snapshot"]["nodes"]["items"]:
            if row["metadata"]["name"] in state["hold_nodes"]:
                row["spec"].pop("taints", None)
        for pod in state["snapshot"]["pods"]["items"]:
            if pod["spec"].get("nodeName") == retirement.ROLE_SETTINGS[role]["target"]:
                pod["status"]["containerStatuses"] = [{"name": "stale", "ready": True}]
    result = {}
    retirement.execute_recovery(args(tmp_path, bundle, execute=True, name="optional-and-stale.json"), result, cloud)
    assert result["system_roles_recovered"] and cloud.native_roles == list(retirement.ROLES)


def test_natural_replacement_gaps_pending_scheduling_and_nnc_gc_are_observed(environment):
    tmp_path, bundle, cloud = environment
    original = cloud.advance
    saved = {}

    def staged(role):
        state = cloud.states[role]
        if role in saved and saved[role].get("missing") is not None:
            state["snapshot"]["pods"]["items"].append(saved[role].pop("missing"))
        old_network = next((copy.deepcopy(row) for row in state["snapshot"]["nnc"]["items"]
                            if row["metadata"]["name"] == retirement.ROLE_SETTINGS[role]["target"]), None)
        original(role)
        step = cloud.progress_reads[role]
        if step == 2:
            name = next(iter(state["target_uids"]))
            pod = next(pod for pod in state["snapshot"]["pods"]["items"]
                       if pod["metadata"]["name"] == name and pod["metadata"]["namespace"] == "mock-clustermesh")
            state["snapshot"]["pods"]["items"].remove(pod)
            saved[role] = {"missing": pod}
            old_network["metadata"]["deletionTimestamp"] = datetime.now(timezone.utc).isoformat()
            old_network["status"]["networkContainers"] = []
            state["snapshot"]["nnc"]["items"].append(old_network)
        elif step == 3:
            pod = next(pod for pod in state["snapshot"]["pods"]["items"]
                       if pod["metadata"]["name"] in state["target_uids"]
                       and pod["metadata"]["namespace"] == "mock-clustermesh")
            pod["spec"].pop("nodeName", None)
            pod["status"] = {"phase": "Pending"}

    cloud.advance = staged
    result = {}
    retirement.execute_recovery(args(tmp_path, bundle, execute=True, name="natural-transitions.json"), result, cloud)
    assert result["system_roles_recovered"]
    assert all(count >= 4 for count in cloud.progress_reads.values())
