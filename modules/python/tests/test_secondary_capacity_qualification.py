"""Stateful offline tests for secondary DSv5 capacity qualification."""

# pylint: disable=protected-access,too-many-lines,attribute-defined-outside-init,redefined-outer-name

from __future__ import annotations

import copy
import importlib.util
import json
import shlex
import shutil
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml


MODULE_DIR = Path(__file__).resolve().parents[1] / "clusterloader2" / "clustermesh-scale"
SPEC = importlib.util.spec_from_file_location(
    "secondary_capacity_qualification",
    MODULE_DIR / "secondary_capacity_qualification.py",
)
qualification = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = qualification
sys.path.insert(0, str(MODULE_DIR))
try:
    SPEC.loader.exec_module(qualification)
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


def ready_status(address):
    return {
        "phase": "Running", "podIP": address,
        "conditions": [{"type": "Ready", "status": "True"}],
        "containerStatuses": [{
            "name": "container", "ready": True, "started": True,
            "restartCount": 0, "state": {"running": {}},
        }],
    }


def make_node(name, row_uid, pool, provider, *, prom=False, ready=True):
    return {
        "kind": "Node",
        "metadata": {
            **meta(name, row_uid=row_uid),
            "labels": {
                "agentpool": pool, "kubernetes.azure.com/agentpool": pool,
                "kubernetes.azure.com/cluster": "node-rg",
                "kubernetes.io/os": "linux",
                **({"prometheus": "true"} if prom else {}),
            },
        },
        "spec": {
            "providerID": provider,
            "taints": [] if ready else [{
                "key": "node.kubernetes.io/unreachable", "effect": "NoSchedule",
            }],
            "unschedulable": False,
        },
        "status": {
            "conditions": [{
                "type": "Ready", "status": "True" if ready else "Unknown",
                "lastHeartbeatTime": "2026-09-13T00:00:00Z",
            }],
            "allocatable": {"cpu": "8", "memory": "28Gi", "pods": "110" if not prom else "250"},
            "nodeInfo": {
                "bootID": uid(f"boot/{name}"), "kubeletVersion": "v1.35.7",
                "operatingSystem": "linux", "osImage": "Ubuntu 24.04",
            },
        },
    }


def make_mock(role, index, node_name, *, deleting):
    pod = {
        "kind": "Pod",
        "metadata": {
            **meta(f"kwok-node-{index}", "mock-clustermesh", uid(f"mock/{role}/{index}")),
            "labels": {"app": "mock-cilium-agent"},
            "ownerReferences": [{
                "kind": "StatefulSet", "name": "kwok-node",
                "uid": uid(f"mock-statefulset/{role}"), "controller": True,
            }],
            **({"deletionTimestamp": "2026-09-13T00:00:00Z"} if deleting else {}),
        },
        "spec": {
            "nodeName": node_name,
            "containers": [{
                "name": "mock", "image": "pinned",
                "resources": {
                    "requests": {"cpu": "100m", "memory": "256Mi"},
                    "limits": {"memory": "1Gi"},
                },
            }],
        },
        "status": ready_status(f"10.10.{int(role[5:])}.{index + 1}") if not deleting else {
            "phase": "Running", "conditions": [{"type": "Ready", "status": "False"}],
        },
    }
    return pod


def make_monitoring(role, name, node_name, *, operator=False):
    spec = {
        "nodeName": node_name, "hostNetwork": False,
        "containers": [{
            "name": name, "image": "pinned",
            "resources": {"requests": {
                "cpu": "100m" if operator else "50m",
                "memory": "128Mi" if operator else "64Mi",
            }},
        }],
        "volumes": [],
    }
    if operator:
        spec["nodeSelector"] = {"kubernetes.io/os": "linux", "prometheus": "true"}
    return {
        "kind": "Pod",
        "metadata": {
            **meta(name, "monitoring", uid(f"monitoring/{role}/{name}")),
            "labels": {"app": name},
            "ownerReferences": [{
                "kind": "ReplicaSet", "name": f"{name}-rs",
                "uid": uid(f"monitoring-rs/{role}/{name}"), "controller": True,
            }],
        },
        "spec": spec,
        "status": ready_status(f"10.20.{int(role[5:])}.1"),
    }


def make_nnc(network):
    return {
        "kind": "NodeNetworkConfig",
        "metadata": {
            **meta(network["name"], "kube-system", network["uid"]),
            "ownerReferences": [{
                "kind": "Node", "name": network["name"],
                "uid": network["node_uid"], "controller": True,
            }],
        },
        "spec": {"requestedIPCount": network["assigned_ip_count"]},
        "status": {
            "assignedIPCount": network["assigned_ip_count"],
            "networkContainers": [{
                "id": network["network_container_id"],
                "version": network["version"],
                "ipAssignments": [{"ip": address} for address in network["ip_addresses"]],
            }],
        },
    }


def system_pod(role, daemon, daemon_uid, node_name):
    return {
        "kind": "Pod",
        "metadata": {
            **meta(f"{daemon}-{node_name}", "kube-system"),
            "labels": {"k8s-app": daemon},
            "ownerReferences": [{
                "kind": "DaemonSet", "name": daemon,
                "uid": daemon_uid, "controller": True,
            }],
        },
        "spec": {
            "nodeName": node_name, "hostNetwork": True,
            "containers": [{"name": daemon, "image": "pinned"}],
        },
        "status": ready_status(f"192.168.{int(role[5:])}.{1 if daemon == 'cilium' else 2}"),
    }


def make_inputs(tmp_path):
    capacity_dir = tmp_path / "capacity"
    capacity_dir.mkdir()
    (capacity_dir / "immutable.json").write_text('{"immutable":true}', encoding="utf-8")
    source_roles = {}
    receipt_roles = {}
    monitoring = {}
    states = {}
    for role in qualification.ROLES:
        count = 1 if role == "mesh-89" else 2
        pool_name = "promv5" if role == "mesh-89" else "cniv5"
        desired = {
            "name": pool_name, "count": count, "mode": "User" if role == "mesh-89" else "System",
            "vmSize": "Standard_D8s_v5", "maxPods": 250 if role == "mesh-89" else 110,
            "kubernetes_patch": "1.35.7",
        }
        healthy_count = 100 if role == "mesh-89" else qualification.EXPECTED_HEALTHY_MOCKS[role]
        failed_name = f"failed-{role}"
        healthy_name = f"healthy-{role}"
        mocks = {}
        pods = []
        for index in range(100):
            deleting = index >= healthy_count
            pod = make_mock(role, index, failed_name if deleting else healthy_name, deleting=deleting)
            pods.append(pod)
            mocks[pod["metadata"]["name"]] = qualification.pod_contract(pod)
        grafana = make_monitoring(role, f"grafana-{role}", healthy_name)
        pods.append(grafana)
        monitoring[role] = {
            grafana["metadata"]["uid"]: qualification.pod_contract(grafana),
        }
        if role == "mesh-89":
            operator = make_monitoring(
                role, "prometheus-operator-test", failed_name, operator=True,
            )
            operator["metadata"]["deletionTimestamp"] = "2026-09-13T00:00:00Z"
            operator["status"] = {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "False"}],
            }
            pods.append(operator)
            monitoring[role][operator["metadata"]["uid"]] = qualification.pod_contract(operator)
        identities = {}
        networks = {}
        new_nodes = []
        for index in range(count):
            name = f"aks-{pool_name}-{role[5:]}-vmss{index:06d}"
            provider = (
                f"azure:///subscriptions/{qualification.capacity.SUBSCRIPTION}/resourceGroups/"
                f"mc_{qualification.capacity.RESOURCE_GROUP}_{role}/providers/Microsoft.Compute/"
                f"virtualMachineScaleSets/aks-{pool_name}-{role[5:]}-vmss/virtualMachines/{index}"
            )
            node_uid = uid(f"node/{role}/{index}")
            identity = {
                "instance_id": str(index), "node_name": name, "node_uid": node_uid,
                "vm_id": uid(f"vm/{role}/{index}"), "provider_id": provider,
                "boot_id": uid(f"boot/{name}"),
            }
            addresses = [f"10.{int(role[5:])}.{index}.{item}" for item in range(1, 17)]
            network = {
                "name": name, "uid": uid(f"nnc/{role}/{index}"), "node_uid": node_uid,
                "network_container_id": uid(f"nc/{role}/{index}"), "version": 0,
                "assigned_ip_count": 16, "ip_addresses": addresses,
            }
            identities[name] = identity
            networks[name] = network
            new_nodes.append(make_node(
                name, node_uid, pool_name, provider, prom=role == "mesh-89",
            ))
            for daemon in ("cilium", "azure-cns"):
                pods.append(system_pod(
                    role, daemon, uid(f"daemon/{role}/{daemon}"), name,
                ))
            for filler in range(15):
                pods.append({
                    "kind": "Pod",
                    "metadata": meta(f"filler-{role}-{index}-{filler}", "kube-system"),
                    "spec": {
                        "nodeName": name, "hostNetwork": False,
                        "containers": [{"name": "filler", "image": "pinned"}],
                    },
                    "status": ready_status(addresses[filler]),
                })
        if role == "mesh-89":
            restored = make_monitoring(
                role, "prometheus-operator-restored", new_nodes[0]["metadata"]["name"], operator=True,
            )
            restored["metadata"]["ownerReferences"] = copy.deepcopy(operator["metadata"]["ownerReferences"])
            restored["status"] = ready_status(networks[new_nodes[0]["metadata"]["name"]]["ip_addresses"][-1])
            pods.append(restored)
        old_nodes = [
            make_node(
                healthy_name, uid(f"healthy-node/{role}"), "default",
                f"azure:///subscriptions/{qualification.capacity.SUBSCRIPTION}/resourceGroups/"
                f"node-rg/providers/Microsoft.Compute/virtualMachineScaleSets/default/virtualMachines/0",
            ),
            make_node(
                failed_name, uid(f"failed-node/{role}"), "default",
                f"azure:///subscriptions/{qualification.capacity.SUBSCRIPTION}/resourceGroups/"
                f"node-rg/providers/Microsoft.Compute/virtualMachineScaleSets/default/virtualMachines/1",
                ready=False,
            ),
        ]
        kwok = [{
            "kind": "Node",
            "metadata": {
                **meta(f"kwok-node-{index}", row_uid=uid(f"kwok/{role}/{index}")),
                "labels": {"type": "kwok"},
            },
            "spec": {"taints": [{"key": "kwok", "effect": "NoSchedule"}]},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]},
        } for index in range(100)]
        capacity_journal_data = {
            "owner": "secondary-capacity-recovery", "token": uid(f"capacity-token/{role}"),
            "record": "accepted",
        }
        capacity_journal = {
            "kind": "ConfigMap", "apiVersion": "v1",
            "metadata": {
                **meta(
                    f"secondary-capacity-recovery-{role}", "kube-system",
                    uid(f"capacity-journal/{role}"),
                ),
                "resourceVersion": "70",
            },
            "data": capacity_journal_data,
        }
        source = {
            "role": role, "cluster": {"name": f"clustermesh-{role[5:]}"},
            "desired": desired, "pin_sha256": "a" * 64,
            "mock_contracts": mocks,
            "failed": {
                "node_name": failed_name, "node_uid": uid(f"failed-node/{role}"),
                "vm_id": uid(f"failed-vm/{role}"),
                "terminal_failure": {
                    "code": "ProvisioningState/failed/OSProvisioningClientError",
                    "time": "2026-09-13T00:00:00Z",
                },
            },
            "kwok_contracts": {
                node["metadata"]["name"]: qualification.node_contract(node)
                for node in kwok
            },
            "daemonsets": [
                ["kube-system", daemon, uid(f"daemon/{role}/{daemon}")]
                for daemon in ("cilium", "azure-cns")
            ],
        }
        statefulset = {
            "kind": "StatefulSet",
            "metadata": {
                **meta("kwok-node", "mock-clustermesh", uid(f"mock-statefulset/{role}")),
                "generation": 4,
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
        }
        controllers = [statefulset]
        for daemon in ("cilium", "azure-cns"):
            controllers.append({
                "kind": "DaemonSet",
                "metadata": meta(
                    daemon, "kube-system", uid(f"daemon/{role}/{daemon}")
                ),
                "spec": {"template": {"spec": {
                    "containers": [{"name": daemon, "image": "pinned"}],
                }}},
            })
        pdbs = {
            "kind": "PodDisruptionBudgetList",
            "items": [{
                "kind": "PodDisruptionBudget",
                "metadata": {
                    **meta("mock-pdb", "mock-clustermesh", uid(f"pdb/{role}")),
                    "generation": 2,
                },
                "spec": {
                    "minAvailable": 1,
                    "unhealthyPodEvictionPolicy": "AlwaysAllow",
                },
                "status": {
                    "observedGeneration": 2, "disruptionsAllowed": 1,
                    "currentHealthy": 99, "desiredHealthy": 1,
                },
            }],
        }
        leases = []
        renew_time = datetime.now(timezone.utc).isoformat()
        for node_row in kwok:
            name = node_row["metadata"]["name"]
            leases.append({
                "kind": "Lease",
                "metadata": {
                    **meta(name, "kube-node-lease", uid(f"lease/{role}/{name}")),
                    "ownerReferences": [{
                        "kind": "Node", "name": name,
                        "uid": node_row["metadata"]["uid"], "controller": True,
                    }],
                },
                "spec": {
                    "holderIdentity": f"kwok-controller-{role}",
                    "leaseDurationSeconds": 40,
                    "renewTime": renew_time, "leaseTransitions": 1,
                },
            })
        action = {
            "attempted": True, "submission_started": True, "accepted": True,
            "ambiguous": False, "automatic_retry_allowed": False,
            "submission_started_at": "2026-09-13T12:00:00+00:00",
            "accepted_at": "2026-09-13T12:01:00+00:00",
            "returned_at": "2026-09-13T12:01:00+00:00",
            "command": ["add", role],
        }
        receipt_roles[role] = {
            "role": role, "cluster": source["cluster"]["name"], "plan_valid": True,
            "capacity_created": True, "initial_network_ready": True,
            "capacity_qualified": False, "workloads_ready": False,
            "source_pin_sha256": source["pin_sha256"],
            "desired_configuration": desired,
            "action": action,
            "journal": {
                "name": f"secondary-capacity-recovery-{role}", "namespace": "kube-system",
                "retained": True, "attempted": True, "accepted": True,
                "ambiguous": False, "uid": capacity_journal["metadata"]["uid"],
                "resource_version": "70", "data_sha256": qualification.digest(capacity_journal_data),
            },
            "new_identities": identities, "new_networks": copy.deepcopy(networks),
            "diagnostics": {"kubernetes": {"pods": {"items": copy.deepcopy(pods)}}},
        }
        source_roles[role] = source
        states[role] = {
            "nodes": [*old_nodes, *new_nodes, *kwok],
            "pods": pods, "networks": networks,
            "configmaps": [capacity_journal],
            "new_identities": identities,
            "controllers": {"kind": "List", "items": controllers},
            "pdbs": pdbs,
            "leases": {"kind": "LeaseList", "items": leases},
        }
    hashes = qualification.hash_tree(capacity_dir)
    inputs = {
        "root": capacity_dir, "hashes": hashes, "tree_sha256": qualification.digest(hashes),
        "source": {
            "hashes": {"summary.json": "b" * 64},
            "tree_sha256": "ceed8d3237168c48702f2e85257848d21b6f653a36cca5dc9467942a28c0c48a",
            "roles": source_roles,
        },
        "receipt": {"per_role": receipt_roles},
        "plan": {}, "monitoring_pins": monitoring,
        "accepted_checkpoint_hashes": {
            "recovery.json": qualification.ACCEPTED_RECOVERY_SHA,
            "plan.json": qualification.ACCEPTED_PLAN_SHA,
        },
    }
    return inputs, states


class JournalGuard:
    def journal_inventory(self, payload):
        result = {}
        for row in payload["items"]:
            metadata = row["metadata"]
            data = row.get("data") or {}
            if "owner" not in data:
                continue
            result[metadata["name"]] = {
                "uid": metadata["uid"], "resourceVersion": metadata["resourceVersion"],
                "data": copy.deepcopy(data),
                "deletionTimestamp": metadata.get("deletionTimestamp"),
                "ownerReferences": metadata.get("ownerReferences") or [],
            }
        return result


class FakeObserver:
    clouds = None

    def __init__(self, _args, _inputs, role, _runner):
        self.role = role
        self.guard = JournalGuard()

    def observe(self):
        cloud = self.clouds
        state = cloud.states[self.role]
        snapshot = {
            "nodes": {"kind": "NodeList", "items": copy.deepcopy(state["nodes"])},
            "pods": {"kind": "PodList", "items": copy.deepcopy(state["pods"])},
            "nnc": {
                "kind": "NodeNetworkConfigList",
                "items": [make_nnc(row) for row in copy.deepcopy(state["networks"]).values()],
            },
            "pdbs": copy.deepcopy(state["pdbs"]),
            "configmaps": {
                "kind": "ConfigMapList", "apiVersion": "v1",
                "items": copy.deepcopy(state["configmaps"]),
            },
        }
        return (
            {"kubernetes": snapshot},
            copy.deepcopy(state["new_identities"]),
            copy.deepcopy(state["networks"]),
        )


class StatefulCloud:
    def __init__(self, states):
        self.states = states
        self.writes = []
        self.http_reads = []
        self.fail_create_after_delivery = False
        self.fail_delete_after_delivery = False
        self.disable_growth = False
        self.no_advance = False

    @staticmethod
    def value(command, flag):
        return command[command.index(flag) + 1]

    def role(self, command):
        context = self.value(command, "--context")
        return f"mesh-{context.split('-')[-1]}"

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
            elif path == "/data":
                row["data"] = copy.deepcopy(operation["value"])
        if not self.no_advance:
            row["metadata"]["resourceVersion"] = str(int(old_rv) + 1)

    def metrics(self, role, *, pods=False):
        now = datetime.now(timezone.utc).isoformat()
        state = self.states[role]
        if pods:
            rows = []
            for pod in state["pods"]:
                metadata = pod.get("metadata") or {}
                if metadata.get("namespace") != "mock-clustermesh":
                    continue
                rows.append({
                    "metadata": {"name": metadata["name"], "namespace": "mock-clustermesh"},
                    "timestamp": now,
                    "containers": [{"name": "mock", "usage": {"memory": "128Mi"}}],
                })
            return {"kind": "PodMetricsList", "items": rows}
        return {
            "kind": "NodeMetricsList",
            "items": [{
                "metadata": {"name": node["metadata"]["name"]},
                "timestamp": now, "usage": {"memory": "1Gi", "cpu": "500m"},
            } for node in state["nodes"]
            if (node.get("metadata") or {}).get("labels", {}).get("type") != "kwok"],
        }

    def maybe_grow(self, role, node_name):
        state = self.states[role]
        probes = [
            pod for pod in state["pods"]
            if (pod.get("metadata") or {}).get("labels", {}).get(
                qualification.maintenance.PROBE_LABEL_KEY
            )
            and (pod.get("spec") or {}).get("nodeName") == node_name
        ]
        if self.disable_growth:
            return
        network = state["networks"][node_name]
        occupied = {
            pod.get("status", {}).get("podIP") for pod in state["pods"]
            if pod.get("spec", {}).get("nodeName") == node_name
            and not pod.get("spec", {}).get("hostNetwork") and pod.get("status", {}).get("podIP")
            and qualification.maintenance.PROBE_LABEL_KEY not in pod["metadata"].get("labels", {})
        }
        count = 16 * ((len(occupied) + len(probes) + 15) // 16)
        if count > network["assigned_ip_count"]:
            node_index = sorted(state["new_identities"]).index(node_name)
            network["version"] += 1
            network["ip_addresses"] = [
                *network["ip_addresses"][:16],
                *[f"172.{int(role[5:])}.{node_index}.{index}"
                  for index in range(17, count + 1)],
            ]
            network["assigned_ip_count"] = count
        for index, pod in enumerate(probes):
            available = [address for address in network["ip_addresses"] if address not in occupied]
            pod["status"] = ready_status(available[index])

    def __call__(self, command, _timeout):
        role = self.role(command)
        state = self.states[role]
        if "create" in command and "configmap" in command:
            name = command[command.index("configmap") + 1]
            data = dict(
                word.removeprefix("--from-literal=").split("=", 1)
                for word in command if word.startswith("--from-literal=")
            )
            row = {
                "kind": "ConfigMap", "apiVersion": "v1",
                "metadata": {
                    **meta(name, "kube-system", uid(f"qualification-journal/{role}")),
                    "resourceVersion": "1",
                },
                "data": data,
            }
            state["configmaps"].append(row)
            self.writes.append(("journal-create", role, name))
            return json.dumps(row)
        if "patch" in command and "configmap" in command:
            name = command[command.index("configmap") + 1]
            row = next(item for item in state["configmaps"] if item["metadata"]["name"] == name)
            self.apply_patch(row, json.loads(self.value(command, "-p")))
            self.writes.append(("journal-patch", role, name))
            return json.dumps(row)
        if "run" in command:
            name = command[command.index("run") + 1]
            overrides = json.loads(next(
                word.removeprefix("--overrides=") for word in command
                if word.startswith("--overrides=")
            ))
            token = next(
                word.split("=", 2)[-1] for word in command
                if word.startswith(f"--labels={qualification.maintenance.PROBE_LABEL_KEY}=")
            )
            pod = {
                "kind": "Pod",
                "metadata": {
                    **meta(name, "mock-clustermesh", uid(f"probe/{role}/{name}")),
                    "labels": {qualification.maintenance.PROBE_LABEL_KEY: token},
                },
                "spec": overrides["spec"],
                "status": {"phase": "Pending", "conditions": []},
            }
            state["pods"].append(pod)
            self.writes.append(("probe-create", role, name))
            self.maybe_grow(role, pod["spec"]["nodeName"])
            if self.fail_create_after_delivery:
                self.fail_create_after_delivery = False
                raise qualification.workers.ReconcileError("probe create response lost")
            return json.dumps(pod)
        if "configmaps" in command:
            if "--field-selector" in command:
                name = self.value(command, "--field-selector").split("=", 1)[1]
                rows = [row for row in state["configmaps"] if row["metadata"]["name"] == name]
            else:
                rows = state["configmaps"]
            return json.dumps({"kind": "ConfigMapList", "apiVersion": "v1", "items": rows})
        if "configmap" in command:
            name = command[command.index("configmap") + 1]
            row = next(item for item in state["configmaps"] if item["metadata"]["name"] == name)
            return json.dumps(row)
        if "deployments,replicasets,daemonsets,statefulsets" in command:
            return json.dumps(state["controllers"])
        if "leases" in command and "kube-node-lease" in command:
            return json.dumps(state["leases"])
        if "pods" in command and "--raw" not in command:
            return json.dumps({"kind": "PodList", "items": state["pods"]})
        if "/apis/metrics.k8s.io/v1beta1/nodes" in command:
            return json.dumps(self.metrics(role))
        if any("/apis/metrics.k8s.io" in word and "/pods" in word for word in command):
            return json.dumps(self.metrics(role, pods=True))
        raw = next((word for word in command if "/proxy/hostname" in word), "")
        if raw:
            name = raw.split("/pods/", 1)[1].split(":", 1)[0]
            self.http_reads.append((role, name))
            return name
        raise AssertionError(command)

    def delete_pod(self, cluster, *, namespace, name, uid, **_kwargs):
        role = cluster.role
        state = self.states[role]
        pod = next(row for row in state["pods"]
                   if row["metadata"]["namespace"] == namespace
                   and row["metadata"]["name"] == name)
        assert pod["metadata"]["uid"] == uid
        state["pods"].remove(pod)
        self.writes.append(("probe-delete", role, name))
        if not any(
            (row.get("metadata") or {}).get("labels", {}).get(
                qualification.maintenance.PROBE_LABEL_KEY
            ) and (row.get("spec") or {}).get("nodeName") == pod["spec"]["nodeName"]
            for row in state["pods"]
        ):
            network = state["networks"][pod["spec"]["nodeName"]]
            network["version"] = max(network["version"] + 1, 2)
            network["ip_addresses"] = network["ip_addresses"][:16]
            network["assigned_ip_count"] = 16
        if self.fail_delete_after_delivery:
            self.fail_delete_after_delivery = False
            raise qualification.mocks.RecoveryError("probe delete response lost")


def make_args(tmp_path, inputs, *, execute=False, name="summary.json"):
    kube = tmp_path / f"kube-{name}"
    kube.mkdir()
    for role in qualification.ROLES:
        (kube / f"{role}.config").write_text("private", encoding="utf-8")
    return SimpleNamespace(
        capacity_directory=str(inputs["root"]), source_build_id=90001,
        resource_group=qualification.capacity.RESOURCE_GROUP,
        confirm_resource_group=qualification.capacity.RESOURCE_GROUP,
        expected_subscription=qualification.capacity.SUBSCRIPTION,
        expected_region=qualification.capacity.REGION,
        expected_tfvars_sha=qualification.capacity.TFVARS_SHA,
        kubeconfig_directory=str(kube), summary_file=str(tmp_path / name),
        timeout_seconds=1200, request_timeout_seconds=60, execute=execute,
    )


@pytest.fixture(name="environment")
def fixture_environment(tmp_path, monkeypatch):
    inputs, states = make_inputs(tmp_path)
    cloud = StatefulCloud(states)
    FakeObserver.clouds = cloud
    monkeypatch.setattr(qualification, "CapacityObserver", FakeObserver)
    monkeypatch.setattr(qualification, "load_inputs", lambda _args: inputs)
    monkeypatch.setattr(qualification.time, "sleep", lambda _seconds: None)
    return tmp_path, inputs, cloud


def receipt(args):
    return json.loads(Path(args.summary_file).read_text(encoding="utf-8"))


def test_plan_is_zero_write_and_plans_all_seven_workers(environment):
    tmp_path, inputs, cloud = environment
    args = make_args(tmp_path, inputs, name="plan.json")
    qualification.execute_qualification(args, {}, cloud, cloud.delete_pod)
    result = receipt(args)
    assert result["success"] and result["plan_valid"] and result["status"] == "plan-valid"
    assert not result["mutation_started"] and not result["capacity_qualified"]
    assert not result["workloads_ready"] and not result["completed_global_baseline"]
    assert not cloud.writes
    assert sum(len(row["ip_growth"]) for row in result["per_role"].values()) == 7
    for role, row in result["per_role"].items():
        if role in qualification.SYSTEM_ROLES:
            assert all(row["ip_growth"][name]["probe_count"] >= count
                       for name, count in row["placement_headroom"]["placement_counts"].items())
        else:
            assert all(proof["probe_count"] == 2 for proof in row["ip_growth"].values())
    assert result["per_role"]["mesh-51"]["placement_headroom"]["replacement_count"] == 61
    assert result["per_role"]["mesh-66"]["placement_headroom"]["replacement_count"] == 48
    assert result["per_role"]["mesh-79"]["placement_headroom"]["replacement_count"] == 40
    assert result["per_role"]["mesh-89"]["placement_headroom"][
        "prometheus_memory_reserve_bytes"] == 16 * 1024**3


def test_execute_proves_growth_http_headroom_and_uid_cleanup(environment):
    tmp_path, inputs, cloud = environment
    args = make_args(tmp_path, inputs, execute=True, name="execute.json")
    qualification.execute_qualification(args, {}, cloud, cloud.delete_pod)
    result = receipt(args)
    assert result["success"] and result["capacity_qualified"]
    assert result["actual_ip_growth_proven"] and result["actual_headroom_proven"]
    assert not result["workloads_ready"] and not result["completed_global_baseline"]
    count = sum(proof["probe_count"] for row in result["per_role"].values()
                for proof in row["ip_growth"].values())
    assert count >= 151
    assert len(cloud.http_reads) == count
    assert len([row for row in cloud.writes if row[0] == "probe-create"]) == count
    assert len([row for row in cloud.writes if row[0] == "probe-delete"]) == count
    assert not any(row[0] not in {
        "journal-create", "journal-patch", "probe-create", "probe-delete",
    } for row in cloud.writes)
    for row in result["per_role"].values():
        assert row["capacity_qualified"] and not row["probe_cleanup_pending"]
        assert all(proof["http_proven"] and proof["after"]["assigned_ip_count"] > 16
                   and proof["after"]["version"] >= 1
                   for proof in row["ip_growth"].values())
        assert all(record["delete_accepted"] is True
                   for record in row["probe_receipts"].values())
        assert row["journal"]["accepted"] is True
        evidence = row["final_evidence"]
        assert len(evidence["kwok_node_leases"]) == 100
        assert evidence["mock_statefulset"]["future_hold_tolerated"] is False
        assert evidence["pdbs"]["mock-clustermesh/mock-pdb"]["disruptions_allowed"] == 1
        assert evidence["target_host"]["controller_pods_force_deleted"] is False
        assert evidence["future_native_fencing_hold"]["applied_in_qualification"] is False
        assert evidence["system_daemonsets"]
        reference = evidence["final_objects_artifact"]
        path = Path(args.summary_file).parent / reference["path"]
        assert qualification.digest(path.read_bytes()) == reference["sha256"]
        assert set(qualification.read_json(path)) == {
            "nodes", "pods", "nnc", "controllers", "pdbs", "kwok_leases",
        }
        assert "diagnostics" not in row and "final_objects" not in evidence
    bundle = qualification.load_native_fencing_bundle(
        args.summary_file, expected_capacity_source_build_id=90001,
    )
    assert bundle["schema_version"] == qualification.NATIVE_BUNDLE_SCHEMA
    assert bundle["retirement_authorized_by_execution"] is False
    assert set(bundle["roles"]) == set(qualification.ROLES)
    assert bundle["roles"]["mesh-51"]["future_native_fencing_hold"]["required"] is True
    assert bundle["roles"]["mesh-89"]["future_native_fencing_hold"]["required"] is False
    assert len(bundle["roles"]["mesh-79"]["terminating_target_mock_uids"]) == 40
    wrapper = yaml.safe_load((
        Path(__file__).resolve().parents[3]
        / "steps/topology/clustermesh-scale/reuse/qualify-secondary-capacity.yml"
    ).read_text(encoding="utf-8"))["steps"][0]["script"]
    start = wrapper.index("require_evidence_directory() {")
    end = wrapper.index('\nif [ "$PHASE" = "plan" ]; then', start)
    command = (
        f"set -euo pipefail\ndirectory={shlex.quote(str(Path(args.summary_file).parent))}\n"
        + wrapper[start:end] + "\nrequire_evidence_directory execute true"
    )
    checked = subprocess.run(["bash", "-c", command], capture_output=True, text=True, timeout=30, check=False)
    assert checked.returncode == 0, checked.stderr
    pin = result["per_role"]["mesh-51"]["preflight_objects_artifact"]
    result["unrelated_sha_string"] = pin["sha256"]
    pin["sha256"] = "0" * 64
    Path(args.summary_file).write_text(json.dumps(result), encoding="utf-8")
    checked = subprocess.run(["bash", "-c", command], capture_output=True, text=True, timeout=30, check=False)
    assert checked.returncode != 0


def test_no_nnc_growth_never_qualifies_and_cleans_owned_probes(environment, monkeypatch):
    tmp_path, inputs, cloud = environment
    cloud.disable_growth = True
    monkeypatch.setattr(qualification, "PROBE_WAIT_SECONDS", 0)
    args = make_args(tmp_path, inputs, execute=True, name="no-growth.json")
    with pytest.raises(qualification.workers.ReconcileError, match="NNC version/IP growth"):
        qualification.execute_qualification(args, {}, cloud, cloud.delete_pod)
    result = receipt(args)
    assert not result["capacity_qualified"] and result["status"] == "failed-closed"
    assert not any(
        (pod.get("metadata") or {}).get("labels", {}).get(
            qualification.maintenance.PROBE_LABEL_KEY
        )
        for state in cloud.states.values() for pod in state["pods"]
    )


def test_ambiguous_probe_create_is_uid_resolved_only_for_cleanup(environment):
    tmp_path, inputs, cloud = environment
    cloud.fail_create_after_delivery = True
    args = make_args(tmp_path, inputs, execute=True, name="create-ambiguous.json")
    with pytest.raises(qualification.workers.ReconcileError, match="response lost"):
        qualification.execute_qualification(args, {}, cloud, cloud.delete_pod)
    result = receipt(args)
    role = result["per_role"]["mesh-51"]
    record = next(iter(role["probe_receipts"].values()))
    assert record["create_accepted"] is None and record["create_ambiguous"] is True
    assert record["uid_resolved_for_cleanup"]
    assert record["delete_accepted"] is True
    assert not role["probe_cleanup_pending"]
    assert not result["capacity_qualified"]


def test_ambiguous_delete_is_not_replayed_and_absence_is_proven(environment):
    tmp_path, inputs, cloud = environment
    cloud.fail_delete_after_delivery = True
    args = make_args(tmp_path, inputs, execute=True, name="delete-ambiguous.json")
    with pytest.raises(qualification.mocks.RecoveryError, match="response lost"):
        qualification.execute_qualification(args, {}, cloud, cloud.delete_pod)
    result = receipt(args)
    role = result["per_role"]["mesh-51"]
    name, record = next((name, row) for name, row in role["probe_receipts"].items()
                        if row.get("delete_attempted") and row.get("delete_accepted") is None)
    assert record["delete_accepted"] is None and record["delete_ambiguous"] is True
    assert record["absence_observed"] is True
    assert len([row for row in cloud.writes
                if row[0] == "probe-delete" and row[2] == name]) == 1
    assert not result["capacity_qualified"]


def test_changed_data_cas_requires_resource_version_advance(environment):
    tmp_path, inputs, cloud = environment
    cloud.no_advance = True
    args = make_args(tmp_path, inputs, execute=True, name="cas.json")
    with pytest.raises(qualification.workers.ReconcileError, match="changed-data"):
        qualification.execute_qualification(args, {}, cloud, cloud.delete_pod)
    assert not any(row[0] == "probe-create" for row in cloud.writes)


@pytest.mark.parametrize("fault", ["pdb-budget", "lease-owner", "target-pvc", "hold-toleration"])
def test_downstream_native_safety_evidence_fails_before_probes(environment, fault):
    tmp_path, inputs, cloud = environment
    state = cloud.states["mesh-51"]
    if fault == "pdb-budget":
        state["pdbs"]["items"][0]["status"]["disruptionsAllowed"] = 0
    elif fault == "lease-owner":
        state["leases"]["items"][0]["metadata"]["ownerReferences"][0]["uid"] = uid("wrong-owner")
    elif fault == "target-pvc":
        target = inputs["source"]["roles"]["mesh-51"]["failed"]["node_name"]
        pod = next(row for row in state["pods"]
                   if (row.get("spec") or {}).get("nodeName") == target)
        pod["spec"]["volumes"] = [{
            "name": "data", "persistentVolumeClaim": {"claimName": "forbidden"},
        }]
    else:
        statefulset = next(row for row in state["controllers"]["items"]
                           if row["kind"] == "StatefulSet")
        statefulset["spec"]["template"]["spec"]["tolerations"] = [{
            "key": qualification.NATIVE_HOLD_KEY,
            "operator": "Equal", "value": "<downstream-owned-token>",
            "effect": "NoSchedule",
        }]
    args = make_args(tmp_path, inputs, execute=True, name=f"{fault}.json")
    with pytest.raises(qualification.workers.ReconcileError):
        qualification.execute_qualification(args, {}, cloud, cloud.delete_pod)
    assert not any(row[0] == "probe-create" for row in cloud.writes)


def test_noop_journal_persist_skips_patch(environment):
    tmp_path, inputs, cloud = environment
    args = make_args(tmp_path, inputs, execute=True, name="noop.json")
    summary = {
        "capacity_source_build_id": 90001, "mutation_started": False,
        "per_role": {
            role: {
                "status": "validating", "capacity_qualified": False,
                "ip_growth": {}, "probe_receipts": {}, "probe_cleanup_pending": [],
                "journal": {
                    "name": f"{qualification.JOURNAL_PREFIX}-{role}",
                    "namespace": "kube-system", "retained": True,
                    "attempted": False, "accepted": None, "ambiguous": False,
                },
            } for role in qualification.ROLES
        },
    }
    operation = qualification.RoleQualification(
        args, inputs, "mesh-51", summary, cloud, cloud.delete_pod,
    )
    operation.observe()
    operation.acquire()
    patches = len([row for row in cloud.writes if row[0] == "journal-patch"])
    resource_version = operation.journal_rv
    operation.persist()
    assert len([row for row in cloud.writes if row[0] == "journal-patch"]) == patches
    assert operation.journal_rv == resource_version
    assert summary["per_role"]["mesh-51"]["journal"]["noop_update_skipped"] is True


def test_large_receipt_reader_accepts_over_32mib_and_rejects_over_128mib(tmp_path):
    allowed = tmp_path / "allowed.json"
    allowed.write_text(json.dumps({"padding": "x" * (33 * 1024 * 1024)}), encoding="utf-8")
    assert len(qualification.read_json(allowed)["padding"]) == 33 * 1024 * 1024
    with pytest.raises(qualification.workers.ReconcileError, match="bounded limit"):
        qualification.read_json(allowed, maximum_bytes=1024)


def test_current_80029_failure_is_not_accepted_as_final_capacity(tmp_path, monkeypatch):
    artifact = tmp_path / "failed-capacity"
    (artifact / "source-input").mkdir(parents=True)
    (artifact / "source-input/summary.json").write_text("{}", encoding="utf-8")
    source = {
        "hashes": qualification.hash_tree(artifact / "source-input"),
        "tree_sha256": "ceed8d3237168c48702f2e85257848d21b6f653a36cca5dc9467942a28c0c48a",
        "roles": {},
    }
    plan = {
        "schema_version": 1, "execute": False, "mutation_started": False,
        "plan_valid": True, "success": True, "status": "plan-valid",
        "capacity_qualified": False, "workloads_ready": False,
        "completed_global_baseline": False,
        "source_tree_hashes": source["hashes"], "source_tree_sha256": source["tree_sha256"],
    }
    failed = {**plan, "execute": True, "mutation_started": True,
              "success": False, "status": "failed-closed"}
    (artifact / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
    (artifact / "recovery.json").write_text(json.dumps(failed), encoding="utf-8")
    real_digest = qualification.digest
    monkeypatch.setattr(qualification, "digest",
                        lambda value: source["tree_sha256"] if value == source["hashes"] else real_digest(value))
    monkeypatch.setattr(qualification.capacity, "load_source", lambda _args: source)
    with pytest.raises(qualification.workers.ReconcileError, match="final successful"):
        qualification.load_inputs(SimpleNamespace(capacity_directory=str(artifact)))


def test_capacity_receipt_contract_requires_every_role_action_and_identity(tmp_path, monkeypatch):
    root = tmp_path / "final"
    (root / "source-input").mkdir(parents=True)
    (root / "source-input" / "summary.json").write_text("{}", encoding="utf-8")
    source = {
        "hashes": {},
        "tree_sha256": "ceed8d3237168c48702f2e85257848d21b6f653a36cca5dc9467942a28c0c48a",
        "roles": {},
    }
    receipt_roles = {}
    for role in qualification.ROLES:
        desired = {"name": "promv5" if role == "mesh-89" else "cniv5",
                   "count": 1 if role == "mesh-89" else 2}
        role_source = {
            "role": role, "cluster": {"name": f"clustermesh-{role[5:]}"},
            "desired": desired, "pin_sha256": "a" * 64,
        }
        source["roles"][role] = role_source
        identities = {}
        networks = {}
        for index in range(desired["count"]):
            name = f"new-{role}-{index}"
            identities[name] = {
                "node_name": name, "node_uid": uid(f"node/{name}"),
                "vm_id": uid(f"vm/{name}"), "boot_id": uid(f"boot/{name}"),
                "instance_id": str(index),
                "provider_id": (
                    f"azure:///subscriptions/{qualification.capacity.SUBSCRIPTION}/"
                    f"resourceGroups/node/providers/Microsoft.Compute/"
                    f"virtualMachineScaleSets/new/virtualMachines/{index}"
                ),
            }
            networks[name] = {
                "name": name, "uid": uid(f"nnc/{name}"),
                "node_uid": identities[name]["node_uid"],
                "network_container_id": uid(f"nc/{name}"), "version": 0,
                "assigned_ip_count": 16,
                "ip_addresses": [f"10.{index}.0.{item}" for item in range(1, 17)],
            }
        receipt_roles[role] = {
            "role": role, "cluster": role_source["cluster"]["name"],
            "plan_valid": True, "capacity_created": True,
            "initial_network_ready": True, "capacity_qualified": False,
            "workloads_ready": False, "source_pin_sha256": "a" * 64,
            "desired_configuration": desired,
            "action": {
                "attempted": True, "submission_started": True, "accepted": True,
                "ambiguous": False, "automatic_retry_allowed": False,
                "submission_started_at": "2026-09-13T12:00:00+00:00",
                "accepted_at": "2026-09-13T12:01:00+00:00",
                "returned_at": "2026-09-13T12:01:00+00:00",
                "command": ["add", role],
            },
            "journal": {
                "name": f"secondary-capacity-recovery-{role}",
                "namespace": "kube-system", "retained": True,
                "attempted": True, "accepted": True, "ambiguous": False,
                "uid": uid(f"journal/{role}"), "resource_version": "2",
                "data_sha256": "b" * 64,
            },
            "new_identities": identities, "new_networks": networks,
        }
        (root / "source-input" / role).mkdir()
        (root / "source-input" / role / "pods.json").write_text(json.dumps({
            "items": [make_monitoring(role, f"grafana-{role}", "healthy")]
        }), encoding="utf-8")
    source["hashes"] = qualification.hash_tree(root / "source-input")
    plan = {
        "schema_version": 1, "execute": False, "mutation_started": False,
        "plan_valid": True, "success": True, "status": "plan-valid",
        "capacity_qualified": False, "workloads_ready": False,
        "completed_global_baseline": False,
        "source_tree_hashes": source["hashes"], "source_tree_sha256": source["tree_sha256"],
    }
    final = {
        "schema_version": 1, "execute": True, "mutation_started": True,
        "plan_valid": True, "success": True,
        "capacity_created": True, "initial_network_ready": True,
        "capacity_qualified": False, "workloads_ready": False,
        "completed_global_baseline": False,
        "status": "secondary-capacity-initial-network-ready",
        "source_build_id": qualification.capacity.DIAGNOSTIC_BUILD,
        "diagnosed_build_id": qualification.capacity.DIAGNOSED_BUILD,
        "automatic_resume_or_adoption": False, "roles": list(qualification.ROLES),
        "per_role": receipt_roles,
        "source_tree_hashes": source["hashes"], "source_tree_sha256": source["tree_sha256"],
    }
    accepted_root = root / "accepted-input"
    accepted_root.mkdir()
    shutil.copytree(root / "source-input", accepted_root / "source-input")
    accepted_roles = copy.deepcopy(receipt_roles)
    accepted_roles["mesh-51"].update(
        status="add-accepted", capacity_created=False, initial_network_ready=False,
        new_identities={}, new_networks={},
    )
    for role in qualification.ROLES[1:]:
        accepted_roles[role]["action"] = {
            "attempted": False, "submission_started": False, "accepted": None,
            "ambiguous": False, "automatic_retry_allowed": False,
        }
        accepted_roles[role]["journal"] = {
            "name": f"secondary-capacity-recovery-{role}", "namespace": "kube-system",
            "retained": True, "attempted": False, "accepted": None, "ambiguous": False,
        }
        accepted_roles[role].update(
            capacity_created=False, initial_network_ready=False,
            new_identities={}, new_networks={},
        )
    accepted = {
        "schema_version": 1, "execute": True, "mutation_started": True,
        "plan_valid": True, "success": False, "status": "failed-closed",
        "capacity_qualified": False, "workloads_ready": False,
        "completed_global_baseline": False, "per_role": accepted_roles,
        "source_tree_hashes": source["hashes"], "source_tree_sha256": source["tree_sha256"],
    }
    (accepted_root / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
    (accepted_root / "recovery.json").write_text(json.dumps(accepted), encoding="utf-8")
    accepted_hashes = qualification.hash_tree(accepted_root)
    receipt_roles["mesh-51"]["journal"].update(
        attached_existing=True, created_in_build=80029, read_only_continuation=True,
    )
    receipt_roles["mesh-51"]["action"]["operation_name"] = qualification.ACCEPTED_OPERATION
    final["continuation"] = {
        "checkpoint_hashes": accepted_hashes,
        "accepted_role_observed_read_only": "mesh-51",
        "resume_build_id": 80029,
    }
    (root / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
    (root / "recovery.json").write_text(json.dumps(final), encoding="utf-8")
    monkeypatch.setattr(qualification.capacity, "load_source", lambda _args: source)
    monkeypatch.setattr(
        qualification.capacity, "pool_add_command",
        lambda role_source: ["add", role_source["role"]],
    )
    monkeypatch.setattr(qualification, "digest", lambda _value: source["tree_sha256"])
    monkeypatch.setattr(
        qualification, "ACCEPTED_RECOVERY_SHA",
        accepted_hashes["recovery.json"],
    )
    monkeypatch.setattr(
        qualification, "ACCEPTED_PLAN_SHA",
        accepted_hashes["plan.json"],
    )
    monkeypatch.setattr(
        qualification, "ACCEPTED_JOURNAL_UID",
        accepted_roles["mesh-51"]["journal"]["uid"],
    )
    monkeypatch.setattr(
        qualification, "ACCEPTED_JOURNAL_RV",
        accepted_roles["mesh-51"]["journal"]["resource_version"],
    )
    loaded = qualification.load_inputs(SimpleNamespace(capacity_directory=str(root)))
    assert set(loaded["receipt"]["per_role"]) == set(qualification.ROLES)
    final["continuation"]["accepted_role_observed_read_only"] = "mesh-66"
    (root / "recovery.json").write_text(json.dumps(final), encoding="utf-8")
    with pytest.raises(qualification.workers.ReconcileError, match="continuation chain"):
        qualification.load_inputs(SimpleNamespace(capacity_directory=str(root)))
    final["continuation"]["accepted_role_observed_read_only"] = "mesh-51"
    final["per_role"]["mesh-79"]["new_identities"] = {}
    (root / "recovery.json").write_text(json.dumps(final), encoding="utf-8")
    with pytest.raises(qualification.workers.ReconcileError):
        qualification.load_inputs(SimpleNamespace(capacity_directory=str(root)))


def test_cli_contract_rejects_extra_kubeconfig(environment):
    tmp_path, inputs, _ = environment
    args = make_args(tmp_path, inputs, name="cli.json")
    (Path(args.kubeconfig_directory) / "mesh-94.config").write_text("private", encoding="utf-8")
    with pytest.raises(qualification.workers.ReconcileError, match="exactly the four"):
        qualification.validate_args(args)
    parsed = qualification.parse_args([
        "--capacity-directory", str(inputs["root"]),
        "--source-build-id", "90001",
        "--resource-group", qualification.capacity.RESOURCE_GROUP,
        "--confirm-resource-group", qualification.capacity.RESOURCE_GROUP,
        "--expected-subscription", qualification.capacity.SUBSCRIPTION,
        "--expected-region", qualification.capacity.REGION,
        "--expected-tfvars-sha", qualification.capacity.TFVARS_SHA,
        "--kubeconfig-directory", args.kubeconfig_directory,
        "--summary-file", str(tmp_path / "parsed.json"),
        "--timeout-seconds", "7200",
    ])
    assert parsed.source_build_id == 90001 and parsed.timeout_seconds == 7200
    assert not parsed.execute


def test_real_capacity_observer_retains_all_completed_journals(tmp_path, monkeypatch):
    fixture_spec = importlib.util.spec_from_file_location(
        "capacity_observer_fixtures", Path(__file__).with_name("test_secondary_capacity_recovery.py"),
    )
    fixtures = importlib.util.module_from_spec(fixture_spec)
    fixture_spec.loader.exec_module(fixtures)
    monkeypatch.setattr(fixtures, "recovery", qualification.capacity)
    monkeypatch.setattr(qualification.time, "sleep", lambda _seconds: None)
    source, states = fixtures.build_source(tmp_path)
    cloud = fixtures.StatefulCloud(states)
    args = fixtures.make_args(tmp_path, source, execute=True, name="capacity.json")
    completed = {}
    qualification.capacity.execute_recovery(args, completed, cloud)
    inputs = {"source": qualification.capacity.load_source(args), "receipt": completed}
    original = copy.deepcopy(cloud.configmaps)
    cloud.adds.clear()
    cloud.patches.clear()
    for role in qualification.ROLES:
        observer = qualification.CapacityObserver(args, inputs, role, cloud)
        _, identities, networks = observer.observe()
        assert identities == completed["per_role"][role]["new_identities"]
        assert networks == completed["per_role"][role]["new_networks"]
        with pytest.raises(qualification.workers.ReconcileError, match="forbids"):
            observer.guard.write(qualification.capacity.pool_add_command(inputs["source"]["roles"][role]), 60)
    assert cloud.configmaps == original and not cloud.adds and not cloud.patches


@pytest.mark.parametrize("fault", ["stale-ready-condition", "late-pdb", "prom-cpu", "prom-requested-memory"])
def test_live_readiness_and_whole_cohort_guards_are_not_bypassed(environment, monkeypatch, fault):
    tmp_path, inputs, cloud = environment
    if fault == "stale-ready-condition":
        original = cloud.maybe_grow

        def stale_ready(role, node_name):
            original(role, node_name)
            for pod in cloud.states[role]["pods"]:
                if qualification.maintenance.PROBE_LABEL_KEY in pod["metadata"].get("labels", {}):
                    pod["status"]["conditions"] = [{"type": "Ready", "status": "False"}]

        cloud.maybe_grow = stale_ready
        monkeypatch.setattr(qualification, "PROBE_WAIT_SECONDS", 0)
    elif fault == "late-pdb":
        cloud.states["mesh-89"]["pdbs"]["items"][0]["status"]["disruptionsAllowed"] = 0
    elif fault == "prom-cpu":
        original = cloud.metrics

        def busy(role, *, pods=False):
            result = original(role, pods=pods)
            if role == "mesh-89" and not pods:
                for row in result["items"]:
                    row["usage"]["cpu"] = "7500m"
            return result

        cloud.metrics = busy
    else:
        state = cloud.states["mesh-89"]
        name = next(iter(state["new_identities"]))
        pod = next(row for row in state["pods"] if row["spec"].get("nodeName") == name)
        pod["spec"]["containers"][0]["resources"] = {"requests": {"memory": "20Gi"}}
    args = make_args(tmp_path, inputs, execute=True, name=f"guard-{fault}.json")
    with pytest.raises(qualification.workers.ReconcileError):
        qualification.execute_qualification(args, {}, cloud, cloud.delete_pod)
    result = receipt(args)
    assert not result["capacity_qualified"] and not result["workloads_ready"]
    if fault == "stale-ready-condition":
        assert not cloud.http_reads
        assert not any(row["probe_cleanup_pending"] for row in result["per_role"].values())
    else:
        assert not cloud.writes


@pytest.mark.parametrize("fault", ["none", "different-node", "different-image", "different-owner"])
def test_same_pending_operator_uid_can_only_gain_its_approved_assignment(tmp_path, fault):
    old = make_monitoring("mesh-89", "prometheus-operator-pending", "", operator=True)
    old["spec"].pop("nodeName")
    old["status"] = {"phase": "Pending"}
    current = copy.deepcopy(old)
    current["spec"]["nodeName"] = "new-promv5"
    current["status"] = ready_status("10.89.4.8")
    if fault == "different-node":
        current["spec"]["nodeName"] = "wrong-node"
    elif fault == "different-image":
        current["spec"]["containers"][0]["image"] = "changed"
    elif fault == "different-owner":
        current["metadata"]["ownerReferences"][0]["uid"] = uid("other-controller")
    root = tmp_path / "source"
    (root / "mesh-89").mkdir(parents=True)
    (root / "mesh-89/pods.json").write_text(json.dumps({"items": [old]}), encoding="utf-8")
    recorded = {
        "new_identities": {"new-promv5": {}},
        "diagnostics": {"kubernetes": {"pods": {"items": [current]}}},
    }
    if fault == "none":
        assert qualification._monitoring_pins(root, "mesh-89", recorded) == {
            old["metadata"]["uid"]: qualification.pod_contract(current),
        }
    else:
        with pytest.raises(qualification.workers.ReconcileError):
            qualification._monitoring_pins(root, "mesh-89", recorded)
