"""Offline safety tests for the narrowly approved mesh-96 host/API recovery."""

# pylint: disable=too-many-lines,protected-access

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
import re
import sys
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import jmespath


MODULE_DIR = Path(__file__).resolve().parents[1] / "clusterloader2" / "clustermesh-scale"
SPEC = importlib.util.spec_from_file_location(
    "unreachable_prom_worker_recovery", MODULE_DIR / "unreachable_prom_worker_recovery.py",
)
recovery = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = recovery
sys.path.insert(0, str(MODULE_DIR))
try:
    SPEC.loader.exec_module(recovery)
finally:
    sys.path.pop(0)


def uid(name):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, name))


def now():
    return datetime.now(timezone.utc).isoformat()


def use_python310_datetime(monkeypatch):
    parsed_arguments = []

    class Python310Datetime(datetime):
        @classmethod
        def fromisoformat(cls, date_string):
            parsed_arguments.append(date_string)
            fraction = re.search(r"\d{2}:\d{2}:\d{2}\.(\d+)", date_string)
            if fraction and len(fraction[1]) not in (3, 6):
                raise ValueError("Python 3.10 requires three or six fractional digits")
            return super().fromisoformat(date_string)

    monkeypatch.setattr(recovery, "datetime", Python310Datetime)
    return parsed_arguments


@pytest.mark.parametrize("fraction", ["1", "12", "123", "1234", "12345", "123456", "6101413", "123456789"])
def test_azure_timestamp_precision_is_compatible_with_pipeline_python310(monkeypatch, fraction):
    arguments = use_python310_datetime(monkeypatch)
    parsed = recovery.timestamp(f"2026-09-12T08:42:20.{fraction}Z", "Azure terminal failure")
    normalized = fraction[:6].ljust(6, "0")
    assert arguments == [f"2026-09-12T08:42:20.{normalized}+00:00"]
    assert parsed.microsecond == int(normalized) and parsed.utcoffset() == timedelta(0)


def test_scale_instance_view_query_uses_the_actual_arm_singular_property():
    status = {"code": "ProvisioningState/failed", "count": 1}
    raw = {
        "statuses": [{"code": recovery.OS_FAILURE_CODE}],
        "virtualMachine": {"statusesSummary": [status]},
        "extensions": [{"name": "irrelevant-to-this-projection"}],
    }
    assert jmespath.search(recovery.SCALE_VIEW_QUERY, raw) == {
        "statuses": raw["statuses"], "virtualMachines": [status],
    }


def metadata(name, namespace="", row_uid=None):
    result = {"name": name, "uid": row_uid or uid(f"{namespace}/{name}"),
              "resourceVersion": "1", "labels": {}, "annotations": {}}
    if namespace:
        result["namespace"] = namespace
    return result


def reference(kind, name, row_uid):
    return {"kind": kind, "name": name, "uid": row_uid, "controller": True}


def ready_status(ready=True):
    return {
        "phase": "Running" if ready else "Pending",
        "podIP": "10.96.0.10" if ready else "",
        "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
        "containerStatuses": [{
            "name": "container", "ready": ready, "restartCount": 0, "started": ready,
            "state": {"running": {"startedAt": now()}} if ready else {"waiting": {"reason": "ContainerCreating"}},
        }],
    }


def make_pod(name, namespace, row_uid, node_name, owner, *, ready=False):
    meta = metadata(name, namespace, row_uid)
    meta["ownerReferences"] = [owner]
    return {
        "apiVersion": "v1", "kind": "Pod", "metadata": meta,
        "spec": {"nodeName": node_name, "containers": [{"name": "container", "image": "test-image"}],
                 "volumes": [{"name": "data", "emptyDir": {}}]},
        "status": ready_status(ready),
    }


def make_plan():
    mocks = {f"kwok-node-{index}": uid(f"mock-{index}") for index in range(100)}
    return {
        "schema_version": 1, "role": recovery.ROLE,
        "node_name": recovery.PROM_NODE, "node_uid": recovery.REAL_UIDS[recovery.PROM_NODE],
        "provider_id": recovery.PROVIDER,
        "api_pod_name": "clustermesh-apiserver-75c9b44965-wm84r",
        "api_pod_uid": "d1548ec6-ad38-483b-b395-9999b87898d4",
        "api_replica_set_name": "clustermesh-apiserver-75c9b44965",
        "api_replica_set_uid": "67b9481a-bb66-46de-8c9a-d64d122e16a1",
        "api_deployment_uid": "6fc6e6e0-e212-4b25-86a6-971d04e00797",
        "mock_controller_uid": uid("mock-controller"),
        "mock_pod_uids": mocks,
        "ready_mock_pod_uids": {f"kwok-node-{index}": mocks[f"kwok-node-{index}"] for index in range(71)},
        "kwok_node_uids": {f"kwok-node-{index}": uid(f"kwok-node-{index}") for index in range(100)},
        "real_node_uids": dict(recovery.REAL_UIDS),
        "cni_source": {
            "node_name": recovery.SOURCE_NODE, "node_uid": recovery.REAL_UIDS[recovery.SOURCE_NODE],
            "network_container_id": recovery.SOURCE_NC,
        },
        "framework_pods": [],
    }


def make_node(name, row_uid, *, pool=None, instance="0"):
    node = {
        "kind": "Node", "metadata": metadata(name, row_uid=row_uid),
        "spec": {"taints": [], "unschedulable": False},
        "status": {
            "conditions": [{"type": "Ready", "status": "True", "reason": "KubeletReady",
                            "lastTransitionTime": now(), "lastHeartbeatTime": now()}],
            "nodeInfo": {"bootID": uid(f"boot/{name}")},
            "allocatable": {"cpu": "7820m", "memory": "27Gi", "pods": "250"},
        },
    }
    labels = node["metadata"]["labels"]
    labels["kubernetes.io/os"] = "linux"
    if pool:
        vmss = recovery.PROM_VMSS if pool == "prompool" else recovery.DEFAULT_VMSS
        node["spec"]["providerID"] = (
            f"azure:///subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{recovery.NODE_GROUP}/"
            f"providers/Microsoft.Compute/virtualMachineScaleSets/{vmss}/virtualMachines/{instance}"
        )
        labels.update({"agentpool": pool, "kubernetes.azure.com/agentpool": pool,
                       "kubernetes.azure.com/cluster": recovery.NODE_GROUP})
    else:
        labels["type"] = "kwok"
        node["spec"]["taints"] = [{"key": "kwok-provider", "value": "true", "effect": "NoSchedule"}]
    return node


class FakeCloud:
    """A stateful runner; any unexpected or destructive command is an assertion."""

    def __init__(self, plan, args):
        self.plan = plan
        self.args = args
        self.commands = []
        self.writes = []
        self.deleted = []
        self.hook = None
        self.restart_error = None
        self.restart_callback = None
        self.delete_callback = None
        self.probe_ready = True
        self.probe_create_error = False
        self.probe_delete_error = False
        self.replacement_ready = True
        self.fleet_stuck = False
        self.peer_fault = None
        self.cleanup_error = False
        self.memory = "2Gi"
        self.cpu = "400m"
        self.metrics_missing = False
        self.metrics_timestamp = None
        self.nodes = {
            name: make_node(name, row_uid, pool="prompool" if name == recovery.PROM_NODE else "default",
                            instance="1" if name.endswith("000001") else "0")
            for name, row_uid in recovery.REAL_UIDS.items()
        }
        self.nodes.update({name: make_node(name, row_uid) for name, row_uid in plan["kwok_node_uids"].items()})
        host = self.nodes[recovery.PROM_NODE]
        old = (datetime.now(timezone.utc) - timedelta(hours=8)).isoformat()
        host["status"]["conditions"] = [{
            "type": "Ready", "status": "Unknown", "reason": "NodeStatusUnknown",
            "lastTransitionTime": old, "lastHeartbeatTime": old,
        }]
        host["spec"]["taints"] = [{"key": "node.kubernetes.io/unreachable", "effect": "NoSchedule"}]
        self.controllers = []
        self.pods = []
        self.events = []
        self.pdbs = [{"kind": "PodDisruptionBudget", "metadata": metadata("keep-pdb", "kube-system"),
                      "spec": {"minAvailable": 1, "selector": {"matchLabels": {"app": "keep"}}}}]
        mock_controller = {
            "kind": "StatefulSet", "metadata": metadata("kwok-node", "mock-clustermesh", plan["mock_controller_uid"]),
            "spec": {"replicas": 100, "template": {"metadata": {"labels": {"app": "mock-cilium-agent"}}, "spec": {
                "containers": [{"name": "mock-cilium-agent", "image": "unchanged-mock-image"}],
            }}},
        }
        self.controllers.append(mock_controller)
        for index in range(100):
            name = f"kwok-node-{index}"
            ready = index < 71
            node_name = recovery.SOURCE_NODE if index < 15 or not ready else f"{recovery.DEFAULT_VMSS}000001"
            pod = make_pod(
                name, "mock-clustermesh", plan["mock_pod_uids"][name], node_name,
                reference("StatefulSet", "kwok-node", plan["mock_controller_uid"]), ready=ready,
            )
            pod["metadata"]["labels"] = {
                "app": "mock-cilium-agent", "mock-clustermesh/agent-controller": "kwok-node",
            }
            self.pods.append(pod)
        for ds_name in ("cilium", "azure-cns"):
            ds_uid = uid(ds_name)
            self.controllers.append({
                "kind": "DaemonSet", "metadata": metadata(ds_name, "kube-system", ds_uid),
                "spec": {"selector": {"matchLabels": {"k8s-app": ds_name}},
                         "template": {"spec": {"containers": [{"name": ds_name, "image": "unchanged-ds-image"}]}}},
            })
            for name in recovery.REAL_UIDS:
                pod = make_pod(f"{ds_name}-{name}", "kube-system", uid(f"{ds_name}-{name}"), name,
                               reference("DaemonSet", ds_name, ds_uid), ready=name != recovery.PROM_NODE)
                pod["spec"]["hostNetwork"] = True
                pod["metadata"]["labels"] = {"k8s-app": ds_name}
                pod["status"]["containerStatuses"][0]["name"] = "cilium-agent" if ds_name == "cilium" else ds_name
                self.pods.append(pod)
        self.api_target = {
            "namespace": "kube-system", "pod_name": plan["api_pod_name"], "pod_uid": plan["api_pod_uid"],
            "replica_set_name": plan["api_replica_set_name"], "replica_set_uid": plan["api_replica_set_uid"],
            "deployment_name": "clustermesh-apiserver", "deployment_uid": plan["api_deployment_uid"],
        }
        self.add_target(self.api_target)
        self.add_dns()
        old_api = make_pod(
            "clustermesh-apiserver-75c9b44965-zddjf", "kube-system",
            "98165011-2ef1-4d28-9b9a-ebb16578d4f2", recovery.PROM_NODE,
            reference("ReplicaSet", plan["api_replica_set_name"], plan["api_replica_set_uid"]),
        )
        old_api["metadata"]["deletionTimestamp"] = old
        old_api["status"] = ready_status(True)
        old_api["status"]["conditions"] = [{"type": "Ready", "status": "False"}]
        self.pods.append(old_api)
        self.nncs = [{
            "metadata": {**metadata(name), "ownerReferences": [reference("Node", name, row_uid)]},
            "spec": {"requestedIPCount": 64},
            "status": {"assignedIPCount": 64, "networkContainers": [{
                "id": recovery.SOURCE_NC if name == recovery.SOURCE_NODE else uid(f"nc/{name}"),
                "version": 10, "ipAssignments": [],
            }]},
        } for name, row_uid in recovery.REAL_UIDS.items()]
        self.make_scope()
        self.scale_view = {
            "statuses": [{"code": "ProvisioningState/failed", "message": "must-not-be-published"}],
            "virtualMachines": [{"code": "ProvisioningState/failed", "count": 1}],
        }

    def make_scope(self):
        scope = f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{recovery.RESOURCE_GROUP}"
        expiry = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        self.group = {
            "id": scope, "location": recovery.REGION,
            "tags": {
                "clustermesh_debug_preserved": "true", "run_id": recovery.RESOURCE_GROUP,
                "scenario": "perf-eval-clustermesh-scale", "clustermesh_debug_expected_clusters": "100",
                "clustermesh_debug_tfvars_sha256": self.args.expected_tfvars_sha, "deletion_due_time": expiry,
            },
        }
        self.clusters, self.members = [], []
        fleet_id = f"{scope}/providers/Microsoft.ContainerService/fleets/clustermesh-flt"
        for index in range(1, 101):
            role, name = f"mesh-{index}", f"clustermesh-{index}"
            cluster_id = f"{scope}/providers/Microsoft.ContainerService/managedClusters/{name}"
            self.clusters.append({
                "id": cluster_id, "name": name, "location": recovery.REGION,
                "nodeResourceGroup": f"mc_{recovery.RESOURCE_GROUP}_{name}_{recovery.REGION}",
                "tags": {"role": role, "run_id": recovery.RESOURCE_GROUP},
                "provisioningState": "Succeeded", "powerState": {"code": "Running"},
            })
            self.members.append({
                "id": f"{fleet_id}/members/{role}", "name": role, "clusterResourceId": cluster_id,
                "provisioningState": "Succeeded", "labels": {"mesh": "true"},
                "meshProperties": {
                    "clusterMeshProfileResourceId": f"{fleet_id}/clusterMeshProfiles/clustermesh-cmp",
                    "ciliumProperties": {"id": index, "name": f"assigned-{index}"},
                    "status": {"state": "Connected"} if index != 96 else {
                        "state": "Failed", "error": {"code": "ConnectivityTimeout"},
                    },
                },
            })
        cluster_id = self.clusters[95]["id"]
        self.node_group = {
            "id": f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{recovery.NODE_GROUP}",
            "location": recovery.REGION, "managedBy": cluster_id,
            "tags": {"deletion_due_time": expiry},
        }
        self.pools, self.vmsses, self.instances, self.views = [], [], {}, {}
        for pool_name, count, vmss_name in (
            ("default", 2, recovery.DEFAULT_VMSS), ("prompool", 1, recovery.PROM_VMSS),
        ):
            self.pools.append({
                "id": f"{cluster_id}/agentPools/{pool_name}", "name": pool_name, "count": count,
                "enableAutoScaling": False, "provisioningState": "Succeeded", "powerState": {"code": "Running"},
                "vmSize": "Standard_D8ds_v5", "maxPods": 250 if pool_name == "prompool" else 110,
            })
            vmss_id = (
                f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{recovery.NODE_GROUP}/"
                f"providers/Microsoft.Compute/virtualMachineScaleSets/{vmss_name}"
            )
            self.vmsses.append({
                "id": vmss_id, "name": vmss_name, "location": recovery.REGION,
                "tags": {"aks-managed-poolName": pool_name}, "orchestrationMode": "Uniform",
                "provisioningState": "Succeeded", "sku": {"capacity": count, "name": "Standard_D8ds_v5"},
            })
            self.instances[vmss_name] = [{
                "id": f"{vmss_id}/virtualMachines/{index}", "name": f"{vmss_name}_{index}",
                "computerName": f"{vmss_name}{index:06d}", "instanceId": str(index),
                "latestModelApplied": True, "provisioningState": "Succeeded", "vmId": uid(f"vm/{vmss_name}/{index}"),
            } for index in range(count)]
            for index in range(count):
                self.views[(vmss_name, str(index))] = {
                    "statuses": [{"code": "PowerState/running"}, {"code": "ProvisioningState/succeeded"}],
                    "extensions": [{"name": "vmssCSE", "statuses": [{"code": "ProvisioningState/succeeded"}]}],
                }
        self.operation = {
            "name": uid("operation"), "status": "Succeeded", "operationType": "PutExtensionAddon",
            "startTime": now(), "endTime": now(), "errorCode": None,
        }

    def add_target(self, target):
        namespace, name = target["namespace"], target["deployment_name"]
        deployment_uid = target.get("deployment_uid") or uid(f"deployment/{namespace}/{name}")
        template = {"containers": [{"name": "container", "image": f"unchanged-{name}"}],
                    "volumes": [{"name": "data", "emptyDir": {}}], "nodeSelector": {"kubernetes.io/os": "linux"}}
        if name == "coredns":
            template["containers"][0]["resources"] = {
                "requests": {"cpu": "100m", "memory": "70Mi"}, "limits": {"cpu": "3", "memory": "500Mi"},
            }
        elif name == "kube-state-metrics":
            template["containers"][0]["resources"] = {
                "requests": {"cpu": "200m", "memory": "2Gi"}, "limits": {"cpu": "200m", "memory": "2Gi"},
            }
        elif name == "grafana":
            template["containers"][0]["resources"] = {"requests": {"cpu": "250m", "memory": "250Mi"}}
        deployment = {
            "kind": "Deployment", "metadata": metadata(name, namespace, deployment_uid),
            "spec": {"replicas": 1, "selector": {"matchLabels": {"app": name}},
                     "template": {"metadata": {"labels": {"app": name}}, "spec": copy.deepcopy(template)}},
            "status": {"observedGeneration": 1},
        }
        deployment["metadata"]["generation"] = 1
        rs = {
            "kind": "ReplicaSet", "metadata": metadata(target["replica_set_name"], namespace, target["replica_set_uid"]),
            "spec": {"replicas": 1, "selector": {"matchLabels": {"app": name}},
                     "template": {"metadata": {"labels": {"app": name}}, "spec": copy.deepcopy(template)}},
        }
        rs["metadata"]["ownerReferences"] = [reference("Deployment", name, deployment_uid)]
        self.controllers.extend([deployment, rs])
        pod = make_pod(target["pod_name"], namespace, target["pod_uid"], recovery.SOURCE_NODE,
                       reference("ReplicaSet", target["replica_set_name"], target["replica_set_uid"]))
        pod["metadata"]["labels"] = {"app": name}
        pod["spec"].update(copy.deepcopy(template))
        self.pods.append(pod)
        self.events.append({
            "involvedObject": {"kind": "Pod", "namespace": namespace, "name": target["pod_name"], "uid": target["pod_uid"]},
            "reason": "FailedCreatePodSandBox", "lastTimestamp": now(),
            "message": f"AllocateIPConfig failed: not enough IPs available of type ipv4 for {recovery.SOURCE_NC}",
        })

    def add_dns(self):
        selected = copy.deepcopy(recovery.APPROVED_FRAMEWORKS[:2])
        self.add_target(selected[0])
        self.get_controller("coredns")["spec"]["replicas"] = recovery.DNS_REPLICAS
        self.get_controller(recovery.DNS_REPLICA_SET, "ReplicaSet")["spec"]["replicas"] = recovery.DNS_REPLICAS
        first = self.get_pod(selected[0]["pod_name"])
        first["spec"]["nodeName"] = f"{recovery.DEFAULT_VMSS}000001"
        first["status"] = ready_status(True)
        for index in range(1, recovery.DNS_REPLICAS):
            pod = copy.deepcopy(first)
            name = selected[1]["pod_name"] if index == 1 else f"{recovery.DNS_REPLICA_SET}-healthy-{index}"
            pod["metadata"]["name"] = name
            pod["metadata"]["uid"] = selected[1]["pod_uid"] if index == 1 else uid(name)
            self.pods.append(pod)
        event = copy.deepcopy(self.events[-1])
        event["involvedObject"].update(name=selected[1]["pod_name"], uid=selected[1]["pod_uid"])
        self.events.append(event)

    def get_pod(self, name):
        return next(row for row in self.pods if row["metadata"]["name"] == name)

    def get_controller(self, name, kind="Deployment"):
        return next(row for row in self.controllers if row["metadata"]["name"] == name and row["kind"] == kind)

    def recover_host(self):
        host = self.nodes[recovery.PROM_NODE]
        host["status"]["conditions"] = [{"type": "Ready", "status": "True", "lastHeartbeatTime": now()}]
        host["status"]["nodeInfo"]["bootID"] = uid("new-prom-boot")
        host["spec"]["taints"] = []
        for pod in self.pods:
            if pod["spec"].get("nodeName") == recovery.PROM_NODE and not pod["metadata"].get("deletionTimestamp"):
                container_name = pod["status"]["containerStatuses"][0]["name"]
                pod["status"] = ready_status(True)
                pod["status"]["containerStatuses"][0]["name"] = container_name

    @staticmethod
    def value(command, key):
        return command[command.index(key) + 1]

    def run(self, command, _timeout):
        command = list(command)
        self.commands.append(command)
        if self.hook:
            self.hook(command)
        if command[0] == "az":
            result = self.azure(command)
        else:
            result = self.kubernetes(command)
        return result if isinstance(result, str) else json.dumps(result)

    def azure(self, command):
        route = command[1:3]
        if route == ["account", "show"]:
            return {"id": recovery.SUBSCRIPTION}
        assert self.value(command, "--subscription") == recovery.SUBSCRIPTION
        if route == ["group", "show"]:
            return self.group if self.value(command, "--name") == recovery.RESOURCE_GROUP else self.node_group
        if route == ["aks", "list"]:
            return self.clusters
        if command[1:4] == ["fleet", "member", "list"]:
            return self.members
        if command[1:4] == ["aks", "operation", "show-latest"]:
            return self.operation
        if command[1:4] == ["aks", "nodepool", "list"]:
            return self.pools
        if route == ["vmss", "list"]:
            assert "--query" in command
            return self.vmsses
        if route == ["vmss", "list-instances"]:
            assert self.value(command, "--query") == recovery.VM_QUERY
            return self.instances[self.value(command, "--name")]
        if route == ["vmss", "get-instance-view"]:
            if "--instance-id" not in command:
                assert self.value(command, "--query") == recovery.SCALE_VIEW_QUERY
                return jmespath.search(self.value(command, "--query"), {
                    "statuses": self.scale_view.get("statuses"),
                    "virtualMachine": {"statusesSummary": self.scale_view.get("virtualMachines")},
                })
            assert self.value(command, "--query") == recovery.VIEW_QUERY
            return self.views[(self.value(command, "--name"), self.value(command, "--instance-id"))]
        if route == ["aks", "get-credentials"]:
            path = Path(self.value(command, "--file"))
            assert path.parent.stat().st_mode & 0o777 == 0o700
            assert path.stat().st_mode & 0o777 == 0o600
            assert os.environ["TMPDIR"] == str(path.parent.resolve())
            assert tempfile.tempdir == str(path.parent.resolve())
            path.write_text("fake offline kubeconfig", encoding="utf-8")
            return ""
        if route in (["vmss", "restart"], ["vmss", "reimage"]):
            self.writes.append(command)
            assert self.value(command, "--resource-group") == recovery.NODE_GROUP
            assert self.value(command, "--name") == recovery.PROM_VMSS
            assert self.value(command, "--instance-ids") == "0"
            assert "--no-wait" in command
            restart_receipt = json.loads(Path(self.args.summary_file).read_text(encoding="utf-8"))["restart"]
            assert restart_receipt["attempted"] and restart_receipt["accepted"] is None and restart_receipt["ambiguous"]
            assert recovery.MARKER_KEY in self.nodes[recovery.PROM_NODE]["metadata"]["annotations"]
            if self.restart_error:
                raise recovery.workers.ReconcileError(self.restart_error)
            if route == ["vmss", "reimage"]:
                assert getattr(self.args, "reimage_failed_os", False)
                self.vmsses[1]["provisioningState"] = "Succeeded"
                self.instances[recovery.PROM_VMSS][0]["provisioningState"] = "Succeeded"
                self.views[(recovery.PROM_VMSS, "0")] = {
                    "statuses": [{"code": "ProvisioningState/succeeded"}, {"code": "PowerState/running"}],
                    "extensions": [{"name": "vmssCSE", "statuses": [{"code": "ProvisioningState/succeeded"}]}],
                }
            self.recover_host()
            if self.restart_callback:
                self.restart_callback()
            return ""
        raise AssertionError(f"Unexpected Azure command: {command}")

    def kubernetes(self, command):
        assert self.value(command, "--context") == recovery.CLUSTER
        assert self.value(command, "--kubeconfig") == self.args.kubeconfig
        namespace = self.value(command, "-n") if "-n" in command else None
        if "patch" in command:
            self.writes.append(command)
            name = command[command.index("patch") + 2]
            operations = json.loads(self.value(command, "-p"))
            if self.cleanup_error and any(row["op"] == "remove" and row["path"].startswith("/spec/taints/")
                                          for row in operations):
                raise recovery.workers.ReconcileError("cleanup patch timeout")
            self.apply_patch(self.nodes[name], operations)
            return self.nodes[name]
        if "run" in command:
            self.writes.append(command)
            name = command[command.index("run") + 1]
            overrides = json.loads(next(part.removeprefix("--overrides=") for part in command if part.startswith("--overrides=")))
            label = next(part.removeprefix("--labels=") for part in command if part.startswith("--labels="))
            key, token = label.split("=", 1)
            pod = {
                "metadata": metadata(name, namespace, uid(name)),
                "spec": overrides["spec"], "status": ready_status(self.probe_ready),
            }
            pod["metadata"]["labels"] = {key: token}
            self.pods.append(pod)
            if self.probe_create_error:
                raise recovery.workers.ReconcileError("probe create transport timeout")
            return pod
        if "exec" in command:
            assert command[command.index("--") + 1:] == ["cilium-dbg", "status", "-o", "json"]
            remotes = [{"name": f"assigned-{index}", "ready": True, "connected": True}
                       for index in range(1, 101) if index != 96]
            if self.peer_fault == "wrong-name":
                remotes[0]["name"] = "not-an-authoritative-peer"
            elif self.peer_fault == "disconnected":
                remotes[0]["connected"] = False
            return {"cluster-mesh": {"clusters": remotes}}
        assert "get" in command, f"Unexpected Kubernetes mutation: {command}"
        following = command[command.index("get") + 1:]
        if following[0] == "--raw=/readyz":
            return "ok"
        if following[0] == "--raw":
            assert following[1] == "/apis/metrics.k8s.io/v1beta1/nodes"
            return {"items": [] if self.metrics_missing else [{
                "metadata": {"name": recovery.PROM_NODE}, "timestamp": self.metrics_timestamp or now(),
                "usage": {"cpu": self.cpu, "memory": self.memory},
            }]}
        resource = following[0]
        if resource == "nodes":
            return {"items": list(self.nodes.values())}
        if resource == "node":
            return self.nodes[following[1]]
        if resource == "pods":
            pods = [row for row in self.pods if namespace is None or row["metadata"].get("namespace") == namespace]
            if "-l" in command:
                label = self.value(command, "-l")
                key, value = label.split("=", 1)
                pods = [row for row in pods if row["metadata"].get("labels", {}).get(key) == value]
            return {"items": pods}
        if resource == "events":
            return {"items": self.events}
        if resource == "nodenetworkconfigs":
            return {"items": self.nncs}
        if resource == "deployments,replicasets,daemonsets,statefulsets":
            return {"items": self.controllers}
        if resource == "pdb":
            return {"items": self.pdbs}
        if resource == "configmap":
            assert following[1] == "cilium-config"
            return {"data": {"cluster-name": "assigned-96", "cluster-id": "96"}}
        raise AssertionError(f"Unexpected Kubernetes command: {command}")

    @staticmethod
    def apply_patch(node, operations):
        for operation in operations:
            segments = [segment.replace("~1", "/").replace("~0", "~") for segment in operation["path"].split("/")[1:]]
            parent = node
            for segment in segments[:-1]:
                parent = parent[int(segment)] if isinstance(parent, list) else parent[segment]
            key = int(segments[-1]) if isinstance(parent, list) else segments[-1]
            if operation["op"] == "test":
                assert parent[key] == operation["value"]
            elif operation["op"] == "add":
                parent[key] = copy.deepcopy(operation["value"])
            else:
                assert operation["op"] == "remove"
                del parent[key]
        node["metadata"]["resourceVersion"] = str(int(node["metadata"]["resourceVersion"]) + 1)

    def delete(self, _cluster, *, namespace, name, uid: str, timeout_seconds, attempts, retry_seconds):  # pylint: disable=redefined-outer-name
        assert attempts == 1 and retry_seconds == 0 and 0 < timeout_seconds <= 45
        matches = [row for row in self.pods if row["metadata"]["name"] == name and row["metadata"]["namespace"] == namespace]
        assert len(matches) == 1 and matches[0]["metadata"]["uid"] == uid
        self.deleted.append((namespace, name, uid))
        pod = matches[0]
        if self.probe_delete_error and name.startswith("prom-recovery-ip-"):
            raise recovery.mocks.RecoveryError("probe delete transport timeout")
        self.pods.remove(pod)
        if name.startswith("prom-recovery-ip-"):
            return
        assert name == self.plan["api_pod_name"] or any(row["pod_name"] == name for row in self.plan["framework_pods"])
        replacement = copy.deepcopy(pod)
        replacement["metadata"]["name"] = f"{name}-replacement"
        replacement["metadata"]["uid"] = str(uuid.uuid5(uuid.NAMESPACE_OID, f"replacement/{uid}"))
        replacement["spec"]["nodeName"] = recovery.PROM_NODE
        replacement["status"] = ready_status(self.replacement_ready)
        self.pods.append(replacement)
        if name == self.plan["api_pod_name"] and not self.fleet_stuck and self.replacement_ready:
            self.members[95]["meshProperties"]["status"] = {"state": "Connected"}
        if self.delete_callback:
            self.delete_callback(pod, replacement)


@pytest.fixture(name="environment")
def recovery_environment(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    plan = make_plan()
    args = SimpleNamespace(
        resource_group=recovery.RESOURCE_GROUP, confirm_resource_group=recovery.RESOURCE_GROUP,
        expected_subscription=recovery.SUBSCRIPTION, expected_region=recovery.REGION,
        expected_tfvars_sha="a" * 64, plan_file="plan.json", summary_file="summary.json",
        timeout_seconds=1800, execute=False,
    )
    Path(args.plan_file).write_text(json.dumps(plan), encoding="utf-8")
    fake = FakeCloud(plan, args)
    return args, plan, fake


def run(environment, *, execute=None):
    args, plan, fake = environment
    if execute is not None:
        args.execute = execute
    Path(args.plan_file).write_text(json.dumps(plan), encoding="utf-8")
    summary = {}
    recovery.execute_recovery(args, summary, runner=fake.run, delete_pod=fake.delete)
    assert summary == json.loads(Path(args.summary_file).read_text(encoding="utf-8"))
    assert summary["success"] is True
    assert not list(Path(".").glob(".unreachable-prom-private-*"))
    return summary


def receipt():
    summary = json.loads(Path("summary.json").read_text(encoding="utf-8"))
    assert all(isinstance(summary[key], bool) for key in ("execute", "mutation_started", "plan_valid", "success"))
    if summary["status"] == "failed":
        assert summary["success"] is False
    return summary


def abort_wait(monkeypatch):
    def expired(_self, _deadline, description):
        raise recovery.workers.ReconcileError(f"{description}: test bounded deadline")
    monkeypatch.setattr(recovery.Recovery, "wait", expired)


def add_framework(environment, name="grafana", *, pinned_deployment=True):
    _, plan, fake = environment
    target = copy.deepcopy(next(row for row in recovery.APPROVED_FRAMEWORKS if row["deployment_name"] == name))
    if pinned_deployment:
        target.setdefault("deployment_uid", uid(f"deployment/{target['namespace']}/{name}"))
    else:
        target.pop("deployment_uid", None)
    plan["framework_pods"].append(target)
    fake.add_target(target)
    return target


def select_dns(environment):
    _, plan, fake = environment
    targets = copy.deepcopy(recovery.APPROVED_FRAMEWORKS[:2])
    plan["framework_pods"].extend(targets)
    for target in targets:
        pod = fake.get_pod(target["pod_name"])
        pod["spec"]["nodeName"] = recovery.SOURCE_NODE
        pod["status"] = ready_status(False)
        old = copy.deepcopy(pod)
        old["metadata"].update(name=f"{target['pod_name']}-old", uid=uid(f"old/{target['pod_name']}"),
                               deletionTimestamp=now())
        old["spec"]["nodeName"] = recovery.PROM_NODE
        old["status"] = ready_status(True)
        old["status"]["conditions"][0]["status"] = "False"
        fake.pods.append(old)
    return targets


def set_marker(fake, plan, *, foreign=False):
    host = fake.nodes[recovery.PROM_NODE]
    marker = {
        "schema_version": 1, "owner": "foreign" if foreign else recovery.OWNER,
        "node_uid": recovery.REAL_UIDS[recovery.PROM_NODE], "provider_id": recovery.PROVIDER,
        "previous_boot_id": host["status"]["nodeInfo"]["bootID"], "plan_sha256": recovery.digest(plan),
        "token": str(uuid.uuid4()), "action": "single-instance-restart",
    }
    host["metadata"]["annotations"][recovery.MARKER_KEY] = json.dumps(marker, sort_keys=True, separators=(",", ":"))
    return marker


def test_default_plan_performs_zero_resource_writes(environment):
    summary = run(environment)
    _, _, fake = environment
    assert summary["plan_valid"] and summary["status"] == "plan_valid"
    assert not summary["repaired"] and not summary["mutation_started"]
    assert not summary["restart"]["attempted"] and not summary["pod_moves"]
    assert not fake.writes and not fake.deleted
    assert summary["fleet_connected"] is False
    assert summary["initial_mock_ready"] == 71
    assert summary["planned_actions"]["restart_required"]
    assert summary["planned_actions"]["instance_ids"] == ["0"]
    assert summary["planned_actions"]["pods"][0]["decision"] == "delete-pinned"
    assert all(not command[1:3] == ["vmss", "restart"] for command in fake.commands)
    assert len([command for command in fake.commands if command[1:3] == ["aks", "get-credentials"]]) == 1


@pytest.mark.parametrize("change", [
    lambda plan: plan.update(schema_version=True),
    lambda plan: plan.update(role="mesh-95"),
    lambda plan: plan.update(node_name=recovery.SOURCE_NODE),
    lambda plan: plan.update(provider_id=recovery.PROVIDER.replace("/0", "/1")),
    lambda plan: plan.update(node_uid=uid("wrong-node")),
    lambda plan: plan.update(api_pod_uid=uid("unpinned-api")),
    lambda plan: plan.update(extra_authority=True),
    lambda plan: plan["mock_pod_uids"].pop("kwok-node-99"),
    lambda plan: plan["kwok_node_uids"].update({"kwok-node-0": plan["kwok_node_uids"]["kwok-node-1"]}),
    lambda plan: plan["ready_mock_pod_uids"].pop("kwok-node-1"),
    lambda plan: plan["ready_mock_pod_uids"].update({"kwok-node-0": uid("wrong-healthy")}),
    lambda plan: plan["cni_source"].update(network_container_id=uid("wrong-nc")),
])
def test_plan_schema_is_strict_and_failure_finalization_never_writes(environment, change):
    _, plan, fake = environment
    change(plan)
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment)
    assert receipt()["status"] == "failed" and not receipt()["plan_valid"]
    assert not fake.writes and not fake.deleted and not fake.commands


def test_duplicate_json_keys_rejected(environment):
    args, _, _ = environment
    Path(args.plan_file).write_text('{"schema_version":1,"schema_version":1}', encoding="utf-8")
    with pytest.raises(recovery.workers.ReconcileError, match="Duplicate JSON"):
        recovery.load_plan(args.plan_file)


@pytest.mark.parametrize("field,value", [
    ("resource_group", "other-group"), ("confirm_resource_group", "other-group"),
    ("expected_subscription", "11111111-1111-1111-1111-111111111111"),
    ("expected_region", "eastus"), ("expected_tfvars_sha", "abc"),
    ("timeout_seconds", 3601), ("timeout_seconds", 0),
])
def test_cli_scope_and_deadline_guards(environment, field, value):
    args, _, fake = environment
    setattr(args, field, value)
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment)
    assert not fake.commands


@pytest.mark.parametrize("fault", [
    "lease", "node-lease", "tfvars", "managed-by", "missing-aks", "duplicate-role",
    "duplicate-fleet-identity", "other-fleet-fault", "wrong-selected-fault", "fleet-resource",
])
def test_full_preserved_scope_must_validate_before_credentials_or_writes(environment, fault):
    _, _, fake = environment
    if fault == "lease":
        fake.group["tags"]["deletion_due_time"] = now()
    elif fault == "node-lease":
        fake.node_group["tags"]["deletion_due_time"] = now()
    elif fault == "tfvars":
        fake.group["tags"]["clustermesh_debug_tfvars_sha256"] = "b" * 64
    elif fault == "managed-by":
        fake.node_group["managedBy"] = fake.clusters[94]["id"]
    elif fault == "missing-aks":
        fake.clusters.pop()
    elif fault == "duplicate-role":
        fake.clusters[0]["tags"]["role"] = "mesh-2"
    elif fault == "duplicate-fleet-identity":
        fake.members[0]["meshProperties"]["ciliumProperties"]["id"] = 96
    elif fault == "other-fleet-fault":
        fake.members[0]["meshProperties"]["status"] = {"state": "Failed", "error": {"code": "ConnectivityTimeout"}}
    elif fault == "wrong-selected-fault":
        fake.members[95]["meshProperties"]["status"]["error"]["code"] = "PartialConnectivity"
    else:
        fake.members[0]["clusterResourceId"] = fake.clusters[1]["id"]
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    assert not fake.writes and not fake.deleted
    assert not any(command[1:3] == ["aks", "get-credentials"] for command in fake.commands)


@pytest.mark.parametrize("fault", [
    "pool-count", "autoscale", "vmss-capacity", "extra-instance", "computer-name",
    "failed-vm", "stopped-vm", "busy-operation", "failed-operation", "busy-extension",
])
def test_arm_model_and_operation_faults_fail_closed(environment, fault):
    _, _, fake = environment
    if fault == "pool-count":
        fake.pools[1]["count"] = 2
    elif fault == "autoscale":
        fake.pools[1]["enableAutoScaling"] = True
    elif fault == "vmss-capacity":
        fake.vmsses[1]["sku"]["capacity"] = 2
    elif fault == "extra-instance":
        fake.instances[recovery.PROM_VMSS].append(copy.deepcopy(fake.instances[recovery.PROM_VMSS][0]))
    elif fault == "computer-name":
        fake.instances[recovery.PROM_VMSS][0]["computerName"] = recovery.SOURCE_NODE
    elif fault == "failed-vm":
        fake.instances[recovery.PROM_VMSS][0]["provisioningState"] = "Failed"
    elif fault == "stopped-vm":
        fake.views[(recovery.PROM_VMSS, "0")]["statuses"][0]["code"] = "PowerState/stopped"
    elif fault in ("busy-operation", "failed-operation"):
        fake.operation["status"] = "Running" if fault == "busy-operation" else "Failed"
    else:
        fake.views[(recovery.PROM_VMSS, "0")]["extensions"][0]["statuses"][0]["code"] = "ProvisioningState/updating"
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    assert not fake.writes and not fake.deleted
    assert "arm_metadata" in receipt()
    if fault == "stopped-vm":
        assert "PowerState/stopped" in receipt()["arm_metadata"]["instances"][recovery.PROM_NODE]["status_codes"]


def test_failed_prom_parent_captures_vm_state_without_waiving_gate(environment):
    _, _, fake = environment
    fake.vmsses[1]["provisioningState"] = "Failed"
    fake.instances[recovery.PROM_VMSS][0].update(
        provisioningState="Failed", customData="must-not-be-published",
    )
    fake.views[(recovery.PROM_VMSS, "0")]["statuses"] = [
        {"code": "ProvisioningState/failed/OSProvisioningTimedOut",
         "displayStatus": "Provisioning failed", "message": "must-not-be-published"},
        {"code": "PowerState/running"},
    ]
    with pytest.raises(recovery.workers.ReconcileError, match="parent pool/VMSS"):
        run(environment, execute=True)
    observed = receipt()["arm_metadata"]["failed_prom_instance_diagnostics"]
    assert observed["read_only"]
    assert observed["instances"][0]["provisioningState"] == "Failed"
    assert observed["scale_set_statuses"][0]["code"] == "ProvisioningState/failed"
    assert observed["vm_status_counts"] == [{"code": "ProvisioningState/failed", "count": 1}]
    assert observed["statuses"][0]["code"] == "ProvisioningState/failed/OSProvisioningTimedOut"
    assert "must-not-be-published" not in json.dumps(receipt())
    assert not fake.writes and not fake.deleted and not receipt()["mutation_started"]
    assert not any(command[1:3] == ["aks", "get-credentials"] for command in fake.commands)


def test_failed_prom_parent_does_not_read_a_foreign_instance(environment):
    _, _, fake = environment
    fake.vmsses[1].update(provisioningState="Failed", id="/foreign/vmss")
    with pytest.raises(recovery.workers.ReconcileError, match="ownership"):
        run(environment, execute=True)
    assert not fake.writes and not fake.deleted
    assert not any(
        command[1:3] == ["vmss", "list-instances"] and recovery.PROM_VMSS in command
        for command in fake.commands
    )


def test_failed_prom_parent_records_missing_original_vm(environment):
    _, _, fake = environment
    fake.vmsses[1]["provisioningState"] = "Failed"
    fake.instances[recovery.PROM_VMSS] = []
    with pytest.raises(recovery.workers.ReconcileError, match="parent pool/VMSS"):
        run(environment, execute=True)
    observed = receipt()["arm_metadata"]["failed_prom_instance_diagnostics"]
    assert observed["instances"] == [] and "absent" in observed["error"]
    assert not fake.writes and not fake.deleted


def diagnosed_os_failure(environment):
    args, _, fake = environment
    args.reimage_failed_os = True
    old = (datetime.now(timezone.utc) - timedelta(hours=8)).isoformat()
    fake.vmsses[1]["provisioningState"] = "Failed"
    fake.instances[recovery.PROM_VMSS][0].update(
        provisioningState="Failed", vmId=recovery.FAILED_PROM_VM_ID,
    )
    fake.scale_view["statuses"] = [{"code": recovery.OS_FAILURE_CODE, "time": old}]
    fake.views[(recovery.PROM_VMSS, "0")] = {
        "statuses": [{"code": recovery.OS_FAILURE_CODE, "time": old}, {"code": "PowerState/running"}],
        "extensions": [{"name": "vmssCSE", "statuses": []}],
    }
    return fake


@pytest.mark.parametrize("extension_statuses", [None, []])
def test_exact_os_failure_plan_is_read_only_and_names_reimage(environment, extension_statuses):
    fake = diagnosed_os_failure(environment)
    fake.views[(recovery.PROM_VMSS, "0")]["extensions"][0]["statuses"] = extension_statuses
    summary = run(environment, execute=False)
    assert summary["plan_valid"] and not summary["mutation_started"]
    assert summary["planned_actions"]["host_action"] == "reimage"
    assert summary["os_reimage_eligible"]
    assert not fake.writes and not fake.deleted


@pytest.mark.parametrize("extension_statuses", [None, []])
def test_exact_os_failure_reimages_one_vm_then_requires_full_postproof(environment, extension_statuses):
    fake = diagnosed_os_failure(environment)
    fake.views[(recovery.PROM_VMSS, "0")]["extensions"][0]["statuses"] = extension_statuses
    summary = run(environment, execute=True)
    operations = [command for command in fake.writes if command[0] == "az"]
    assert len(operations) == 1 and operations[0][1:3] == ["vmss", "reimage"]
    assert fake.value(operations[0], "--instance-ids") == "0"
    assert summary["restart"]["action"] == "reimage" and summary["restart"]["accepted"]
    assert summary["repaired"] and summary["restart"]["marker_removed"]


@pytest.mark.parametrize("fault", ["no-opt-in", "vm-id", "error-code", "power-stopped", "default-failed"])
def test_reimage_does_not_waive_unrelated_failed_state_guards(environment, fault):
    args, _, _ = environment
    fake = diagnosed_os_failure(environment)
    if fault == "no-opt-in":
        args.reimage_failed_os = False
    elif fault == "vm-id":
        fake.instances[recovery.PROM_VMSS][0]["vmId"] = uid("different-vm")
    elif fault == "error-code":
        fake.views[(recovery.PROM_VMSS, "0")]["statuses"][0]["code"] = "ProvisioningState/failed/OtherError"
    elif fault == "power-stopped":
        fake.views[(recovery.PROM_VMSS, "0")]["statuses"][1]["code"] = "PowerState/stopped"
    else:
        fake.vmsses[0]["provisioningState"] = "Failed"
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    assert not fake.writes and not fake.deleted


def test_ambiguous_os_reimage_retains_durable_marker_without_retry(environment):
    fake = diagnosed_os_failure(environment)
    fake.restart_error = "command timed out after 45s"
    with pytest.raises(recovery.workers.ReconcileError, match="timed out"):
        run(environment, execute=True)
    operations = [command for command in fake.writes if command[1:3] == ["vmss", "reimage"]]
    assert len(operations) == 1
    assert receipt()["restart"]["ambiguous"] and receipt()["restart"]["accepted"] is None
    assert recovery.MARKER_KEY in fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"]
    assert not fake.deleted


def test_reimage_never_runs_against_a_healthy_model_without_the_os_fault(environment):
    args, _, fake = environment
    args.reimage_failed_os = True
    with pytest.raises(recovery.workers.ReconcileError, match="exact diagnosed"):
        run(environment, execute=True)
    assert not fake.writes and not fake.deleted


def test_recent_os_failure_is_not_reimaged(environment):
    fake = diagnosed_os_failure(environment)
    fake.views[(recovery.PROM_VMSS, "0")]["statuses"][0]["time"] = now()
    with pytest.raises(recovery.workers.ReconcileError, match="stably terminal"):
        run(environment, execute=True)
    assert not fake.writes and not fake.deleted


def test_failed_reimage_is_not_repeated_or_accepted_as_healthy(environment):
    fake = diagnosed_os_failure(environment)

    def fail_again():
        fake.nodes[recovery.PROM_NODE]["status"]["conditions"][0]["status"] = "Unknown"
        fake.vmsses[1]["provisioningState"] = "Failed"
        fake.instances[recovery.PROM_VMSS][0]["provisioningState"] = "Failed"
        fake.views[(recovery.PROM_VMSS, "0")]["statuses"] = [
            {"code": recovery.OS_FAILURE_CODE, "time": now()}, {"code": "PowerState/running"},
        ]

    fake.restart_callback = fail_again
    with pytest.raises(recovery.workers.ReconcileError, match="new provisioning failure"):
        run(environment, execute=True)
    assert len([row for row in fake.writes if row[1:3] == ["vmss", "reimage"]]) == 1
    assert not receipt()["success"] and not receipt()["repaired"]
    assert recovery.MARKER_KEY in fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"]
    assert not fake.deleted


def test_null_extension_state_is_never_accepted_by_normal_restart(environment):
    _, _, fake = environment
    fake.views[(recovery.PROM_VMSS, "0")]["extensions"][0]["statuses"] = None
    with pytest.raises(recovery.workers.ReconcileError, match="extension operations"):
        run(environment, execute=True)
    assert not fake.writes and not fake.deleted


def test_reimage_postproof_requires_actual_succeeded_extension_status(environment, monkeypatch):
    fake = diagnosed_os_failure(environment)
    abort_wait(monkeypatch)
    fake.restart_callback = lambda: fake.views[(recovery.PROM_VMSS, "0")]["extensions"][0].update(statuses=None)
    with pytest.raises(recovery.workers.ReconcileError, match="convergence"):
        run(environment, execute=True)
    assert len([row for row in fake.writes if row[1:3] == ["vmss", "reimage"]]) == 1
    assert not receipt()["success"] and not receipt()["repaired"]
    assert not fake.deleted


def accepted_observation(environment, *, healthy):
    args, plan, fake = environment
    args.reimage_failed_os = True
    args.execute = False
    previous_boot = recovery.node_boot(fake.nodes[recovery.PROM_NODE])
    marker_time = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    requested_time = (datetime.now(timezone.utc) - timedelta(minutes=55)).isoformat()
    marker = {
        "schema_version": 1, "owner": recovery.OWNER, "action": "single-instance-reimage",
        "node_uid": recovery.REAL_UIDS[recovery.PROM_NODE], "provider_id": recovery.PROVIDER,
        "plan_sha256": recovery.digest(plan), "previous_boot_id": previous_boot,
        "token": uid("accepted-marker"), "recorded_at": marker_time,
    }
    checkpoint = {
        "execute": True, "mutation_started": True, "plan_sha256": recovery.digest(plan),
        "restart": {
            "action": "reimage", "attempted": True, "accepted": True, "ambiguous": False,
            "marker": marker, "requested_at": requested_time, "previous_boot_id": previous_boot,
        },
        "arm_metadata": {"instances": {recovery.PROM_NODE: {
            "vm_id": recovery.FAILED_PROM_VM_ID, "instance_id": "0",
        }}},
    }
    fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"][recovery.MARKER_KEY] = (
        json.dumps(marker, sort_keys=True, separators=(",", ":"))
    )
    fake.instances[recovery.PROM_VMSS][0]["vmId"] = recovery.FAILED_PROM_VM_ID
    args.observe_accepted_action = str(Path(args.plan_file).parent / "accepted.json")
    Path(args.observe_accepted_action).write_text(json.dumps(checkpoint), encoding="utf-8")
    if healthy:
        fake.recover_host()
    else:
        fake.vmsses[1]["provisioningState"] = "Updating"
        fake.instances[recovery.PROM_VMSS][0]["provisioningState"] = "Updating"
        fake.views[(recovery.PROM_VMSS, "0")]["statuses"] = [
            {"code": "ProvisioningState/updating"}, {"code": "PowerState/running"},
        ]
    return checkpoint


def test_accepted_action_observer_certifies_host_without_any_write(environment):
    accepted_observation(environment, healthy=True)
    _, _, fake = environment
    summary = run(environment, execute=False)
    assert summary["observation_only"] and summary["observed_host_ready"]
    assert not summary["repaired"] and not summary["mutation_started"]
    assert not summary["restart"]["attempted"]
    assert not fake.writes and not fake.deleted
    assert recovery.MARKER_KEY in fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"]


def test_accepted_action_observer_waits_without_resubmitting(environment, monkeypatch):
    accepted_observation(environment, healthy=False)
    _, _, fake = environment
    abort_wait(monkeypatch)
    with pytest.raises(recovery.workers.ReconcileError, match="accepted-reimage observation"):
        run(environment, execute=False)
    assert not fake.writes and not fake.deleted
    assert not receipt()["mutation_started"] and not receipt()["restart"]["attempted"]
    assert receipt()["observed_action"]["models_stable"] is False


@pytest.mark.parametrize("fault", ["unaccepted", "different-plan", "different-vm", "different-marker"])
def test_accepted_action_observer_rejects_unproven_checkpoint(environment, fault):
    checkpoint = accepted_observation(environment, healthy=True)
    args, _, fake = environment
    if fault == "unaccepted":
        checkpoint["restart"]["accepted"] = False
    elif fault == "different-plan":
        checkpoint["plan_sha256"] = "x" * 64
    elif fault == "different-vm":
        checkpoint["arm_metadata"]["instances"][recovery.PROM_NODE]["vm_id"] = uid("foreign")
    else:
        fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"][recovery.MARKER_KEY] = "{}"
    Path(args.observe_accepted_action).write_text(json.dumps(checkpoint), encoding="utf-8")
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=False)
    assert not fake.writes and not fake.deleted


def test_accepted_action_observer_forbids_execute(environment):
    accepted_observation(environment, healthy=True)
    _, _, fake = environment
    with pytest.raises(recovery.workers.ReconcileError, match="cannot be combined"):
        run(environment, execute=True)
    assert not fake.commands and not fake.writes


@pytest.mark.parametrize("other_mode", ["reimage_failed_os", "observe_accepted_action"])
def test_failed_host_replacement_cannot_combine_with_a_different_action(environment, other_mode):
    args, _, fake = environment
    args.replace_failed_host = "accepted.json"
    setattr(args, other_mode, True if other_mode == "reimage_failed_os" else "observed.json")
    with pytest.raises(recovery.workers.ReconcileError, match="cannot be combined"):
        run(environment)
    assert not fake.commands and not fake.writes


@pytest.mark.parametrize("mode", ["observe_accepted_action", "replace_failed_host"])
@pytest.mark.parametrize("path_name", ["plan_file", "summary_file"])
def test_accepted_checkpoint_cannot_be_overwritten_by_plan_or_summary(environment, mode, path_name):
    args, _, fake = environment
    path = Path(getattr(args, path_name))
    if not path.exists():
        path.write_text('{"accepted": "retain-the-existing-receipt"}', encoding="utf-8")
    original = path.read_bytes()
    setattr(args, mode, str(path))
    with pytest.raises(recovery.workers.ReconcileError, match="must differ"):
        run(environment)
    assert path.read_bytes() == original
    assert not fake.commands and not fake.writes


@pytest.mark.parametrize("change", [
    lambda checkpoint: checkpoint.update(arm_metadata=[]),
    lambda checkpoint: checkpoint["arm_metadata"].update(instances=[]),
    lambda checkpoint: checkpoint["arm_metadata"]["instances"].update({recovery.PROM_NODE: []}),
    lambda checkpoint: checkpoint["restart"].update(marker=[]),
    lambda checkpoint: checkpoint["restart"]["marker"].update(schema_version=True),
])
def test_shared_accepted_receipt_validator_rejects_malformed_shapes(environment, change):
    checkpoint = accepted_observation(environment, healthy=False)
    _, plan, fake = environment
    change(checkpoint)
    with pytest.raises(recovery.workers.ReconcileError):
        recovery.validate_accepted_reimage(checkpoint, recovery.digest(plan))
    assert not fake.writes and not fake.deleted


@pytest.mark.parametrize("fault", [
    "node-uid", "node-provider", "default-unready", "kwok-uid", "kwok-unready",
    "mock-uid", "healthy-on-source", "mock-on-host", "pvc-host", "unknown-controller",
    "host-pod-ready", "brief-unreachable", "source-nc",
])
def test_restart_workload_and_identity_guards(environment, fault):
    _, plan, fake = environment
    host = fake.nodes[recovery.PROM_NODE]
    if fault == "node-uid":
        host["metadata"]["uid"] = uid("replaced-node")
    elif fault == "node-provider":
        host["spec"]["providerID"] = host["spec"]["providerID"].replace("/0", "/1")
    elif fault == "default-unready":
        fake.nodes[recovery.SOURCE_NODE]["status"]["conditions"][0]["status"] = "Unknown"
    elif fault == "kwok-uid":
        fake.nodes["kwok-node-0"]["metadata"]["uid"] = uid("new-kwok")
    elif fault == "kwok-unready":
        fake.nodes["kwok-node-0"]["status"]["conditions"][0]["status"] = "False"
    elif fault == "mock-uid":
        fake.get_pod("kwok-node-99")["metadata"]["uid"] = uid("new-mock")
    elif fault == "healthy-on-source":
        fake.get_pod("kwok-node-0")["status"]["conditions"][0]["status"] = "False"
    elif fault == "mock-on-host":
        fake.get_pod("kwok-node-99")["spec"]["nodeName"] = recovery.PROM_NODE
    elif fault == "pvc-host":
        fake.get_pod(f"azure-cns-{recovery.PROM_NODE}")["spec"]["volumes"] = [
            {"name": "data", "persistentVolumeClaim": {"claimName": "important"}},
        ]
    elif fault == "unknown-controller":
        fake.get_pod(f"azure-cns-{recovery.PROM_NODE}")["metadata"]["ownerReferences"][0]["uid"] = uid("foreign")
    elif fault == "host-pod-ready":
        fake.get_pod(f"azure-cns-{recovery.PROM_NODE}")["status"]["conditions"][0]["status"] = "True"
    elif fault == "brief-unreachable":
        host["status"]["conditions"][0]["lastTransitionTime"] = now()
    else:
        next(row for row in fake.nncs if row["metadata"]["name"] == plan["cni_source"]["node_name"])[
            "status"
        ]["networkContainers"][0]["id"] = uid("new-nc")
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    assert not fake.writes and not fake.deleted
    assert receipt()["status"] == "failed"


@pytest.mark.parametrize("fault", [
    "pvc", "ephemeral-pvc", "pod-ip", "container-id", "started", "restarts", "last-state",
    "init-started", "wrong-event-uid", "wrong-event-nc", "stale-event", "wrong-pod-owner",
    "wrong-deployment-uid", "replicas", "wrong-source",
])
def test_only_exact_never_started_controller_owned_cns_pods_are_eligible(environment, fault):
    _, plan, fake = environment
    pod = fake.get_pod(plan["api_pod_name"])
    container = pod["status"]["containerStatuses"][0]
    if fault == "pvc":
        pod["spec"]["volumes"] = [{"name": "data", "persistentVolumeClaim": {"claimName": "claim"}}]
    elif fault == "ephemeral-pvc":
        pod["spec"]["volumes"] = [{"name": "data", "ephemeral": {"volumeClaimTemplate": {}}}]
    elif fault == "pod-ip":
        pod["status"]["podIP"] = "10.0.0.99"
    elif fault == "container-id":
        container["containerID"] = "containerd://old"
    elif fault == "started":
        container["started"] = True
    elif fault == "restarts":
        container["restartCount"] = 1
    elif fault == "last-state":
        container["lastState"] = {"terminated": {"exitCode": 1}}
    elif fault == "init-started":
        pod["status"]["initContainerStatuses"] = [{"started": True}]
    elif fault == "wrong-event-uid":
        fake.events[0]["involvedObject"]["uid"] = uid("wrong")
    elif fault == "wrong-event-nc":
        fake.events[0]["message"] = "AllocateIPConfig failed: not enough IPs available of type ipv4 for another-nc"
    elif fault == "stale-event":
        fake.events[0]["lastTimestamp"] = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    elif fault == "wrong-pod-owner":
        pod["metadata"]["ownerReferences"][0]["uid"] = uid("wrong-rs")
    elif fault == "wrong-deployment-uid":
        fake.get_controller("clustermesh-apiserver")["metadata"]["uid"] = uid("wrong-deployment")
    elif fault == "replicas":
        fake.get_controller("clustermesh-apiserver")["spec"]["replicas"] = 2
    else:
        pod["spec"]["nodeName"] = f"{recovery.DEFAULT_VMSS}000001"
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    assert not fake.writes and not fake.deleted


def test_execute_restarts_only_instance_zero_then_exact_api_uid_and_keeps_all_mocks(environment):
    _, plan, fake = environment
    initial_mock_pods = {row["metadata"]["name"]: copy.deepcopy(row) for row in fake.pods
                         if row["metadata"].get("namespace") == "mock-clustermesh"}
    initial_controllers = copy.deepcopy(fake.controllers)
    initial_pdbs = copy.deepcopy(fake.pdbs)
    summary = run(environment, execute=True)
    assert summary["repaired"] and summary["status"] == "repaired"
    assert summary["restart"]["attempted"] and summary["restart"]["accepted"] and not summary["restart"]["ambiguous"]
    assert summary["restart"]["marker_removed"] and summary["restart"]["host_proven"]
    restarts = [command for command in fake.writes if command[0:3] == ["az", "vmss", "restart"]]
    assert len(restarts) == 1
    assert fake.value(restarts[0], "--instance-ids") == "0"
    assert summary["pod_moves"][0]["state"] == "moved"
    assert fake.deleted[-1] == ("kube-system", plan["api_pod_name"], plan["api_pod_uid"])
    assert fake.deleted[0][1].startswith("prom-recovery-ip-")
    assert len(fake.deleted) == 2
    assert all("zddjf" not in row[1] for row in fake.deleted)
    assert fake.controllers == initial_controllers and fake.pdbs == initial_pdbs
    assert {row["metadata"]["name"]: row for row in fake.pods
            if row["metadata"].get("namespace") == "mock-clustermesh"} == initial_mock_pods
    assert summary["final_mock_ready"] == 71 and summary["final_mock_pending"] == 29
    assert summary["final_kwok_ready"] == 100 and summary["workloads_ready"] is False
    assert summary["cilium_proof"]["healthy"] and summary["cilium_proof"]["cilium_agent_count"] == 3
    assert summary["fleet_connected"] is True
    assert not summary["cleanup_errors"] and not summary["temporary_exclusions"] and not summary["probe_cleanup_pending"]
    assert recovery.MARKER_KEY not in fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"]
    for command in fake.writes:
        if command[0] == "kubectl" and "patch" in command:
            operations = json.loads(fake.value(command, "-p"))
            assert operations[0]["op"] == "test" and operations[0]["path"] == "/metadata/uid"
            assert all(row["path"] != "/spec/unschedulable" for row in operations)
            assert all(row.get("effect") != "NoExecute" for op in operations
                       for row in (op.get("value") if isinstance(op.get("value"), list) else []) if isinstance(row, dict))


def test_optional_framework_uid_is_derived_then_recovered_after_api(environment):
    target = add_framework(environment, pinned_deployment=False)
    summary = run(environment, execute=True)
    assert len(summary["pod_moves"]) == 2
    assert [row["deployment_name"] for row in summary["pod_moves"]] == ["clustermesh-apiserver", "grafana"]
    assert all(row["state"] == "moved" and row["ready_node"] == recovery.PROM_NODE for row in summary["pod_moves"])
    assert summary["effective_targets"][1]["deployment_uid"] == uid("deployment/monitoring/grafana")
    assert summary["pod_moves"][1]["pod_uid"] == target["pod_uid"]
    assert len(summary["ip_proofs"]) == 2
    assert summary["last_capacity_proof"]["prior_move_reserve_bytes"] == recovery.API_MEMORY_RESERVE


def test_framework_list_is_explicit_and_bounded(environment):
    _, plan, _ = environment
    target = add_framework(environment)
    plan["framework_pods"] = [target] * 5
    with pytest.raises(recovery.workers.ReconcileError, match="At most five"):
        run(environment)
    plan["framework_pods"] = [target, target]
    with pytest.raises(recovery.workers.ReconcileError, match="Duplicate"):
        run(environment)
    plan["framework_pods"] = [{**target, "deployment_name": "unknown-application"}]
    with pytest.raises(recovery.workers.ReconcileError, match="not explicitly supported"):
        run(environment)


def test_ambiguous_restart_is_one_request_with_durable_marker_and_no_pod_deletes(environment):
    _, _, fake = environment
    fake.restart_error = "command timed out after accepting the restart"
    with pytest.raises(recovery.workers.ReconcileError, match="timed out"):
        run(environment, execute=True)
    summary = receipt()
    assert summary["restart"]["attempted"] and summary["restart"]["accepted"] is None
    assert summary["restart"]["ambiguous"] and not summary["repaired"]
    assert recovery.MARKER_KEY in fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"]
    assert len([row for row in fake.writes if row[:3] == ["az", "vmss", "restart"]]) == 1
    assert not fake.deleted
    fake.restart_error = None
    before = len(fake.writes)
    with pytest.raises(recovery.workers.ReconcileError, match="prior restart marker"):
        run(environment, execute=True)
    assert len(fake.writes) == before and not fake.deleted


@pytest.mark.parametrize("execute", [False, True])
def test_preexisting_marker_and_unready_host_forbid_another_attempt(environment, execute):
    _, plan, fake = environment
    set_marker(fake, plan)
    with pytest.raises(recovery.workers.ReconcileError, match="prior restart marker"):
        run(environment, execute=execute)
    assert not fake.writes and not fake.deleted
    assert receipt()["restart"]["prior_marker"]


def test_already_recovered_host_skips_restart_but_still_proves_api_repair(environment):
    _, _, fake = environment
    fake.recover_host()
    summary = run(environment, execute=True)
    assert summary["repaired"] and not summary["restart"]["attempted"]
    assert summary["restart"]["skipped_reason"] == "host-already-ready"
    assert not any(row[:3] == ["az", "vmss", "restart"] for row in fake.writes)


def test_prior_owned_restart_can_continue_only_after_changed_boot(environment):
    _, plan, fake = environment
    marker = set_marker(fake, plan)
    fake.recover_host()
    summary = run(environment, execute=True)
    assert summary["repaired"] and not summary["restart"]["attempted"]
    assert summary["restart"]["previous_boot_id"] == marker["previous_boot_id"]
    assert summary["restart"]["marker_removed"]


@pytest.mark.parametrize("fault", ["foreign-marker", "same-boot"])
def test_recovered_host_does_not_adopt_foreign_or_unproven_marker(environment, fault):
    _, plan, fake = environment
    marker = set_marker(fake, plan, foreign=fault == "foreign-marker")
    fake.recover_host()
    if fault == "same-boot":
        fake.nodes[recovery.PROM_NODE]["status"]["nodeInfo"]["bootID"] = marker["previous_boot_id"]
    with pytest.raises(recovery.workers.ReconcileError, match="owned-marker"):
        run(environment, execute=True)
    assert not fake.writes and not fake.deleted
    assert recovery.MARKER_KEY in fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"]


@pytest.mark.parametrize("fault", ["node-uid", "same-boot", "not-ready", "cns-unready", "default-uid"])
def test_restart_convergence_requires_same_nodes_new_boot_and_ready_system_agents(environment, monkeypatch, fault):
    _, _, fake = environment
    old_boot = fake.nodes[recovery.PROM_NODE]["status"]["nodeInfo"]["bootID"]
    abort_wait(monkeypatch)

    def after_restart():
        if fault == "node-uid":
            fake.nodes[recovery.PROM_NODE]["metadata"]["uid"] = uid("new-node")
        elif fault == "same-boot":
            fake.nodes[recovery.PROM_NODE]["status"]["nodeInfo"]["bootID"] = old_boot
        elif fault == "not-ready":
            fake.nodes[recovery.PROM_NODE]["status"]["conditions"][0]["status"] = "Unknown"
        elif fault == "cns-unready":
            fake.get_pod(f"azure-cns-{recovery.PROM_NODE}")["status"]["conditions"][0]["status"] = "False"
        else:
            fake.nodes[recovery.SOURCE_NODE]["metadata"]["uid"] = uid("new-default")
    fake.restart_callback = after_restart
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    assert receipt()["restart"]["accepted"] and not receipt()["repaired"]
    assert len([row for row in fake.writes if row[:3] == ["az", "vmss", "restart"]]) == 1
    assert not fake.deleted


@pytest.mark.parametrize("fault", ["missing-metrics", "stale-metrics", "memory", "cpu", "pod-slots",
                                 "exclusion-toleration", "kwok-eligible"])
def test_capacity_is_real_measured_and_scheduling_must_be_unambiguous(environment, fault):
    _, plan, fake = environment
    fake.recover_host()
    if fault == "missing-metrics":
        fake.metrics_missing = True
    elif fault == "stale-metrics":
        fake.metrics_timestamp = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    elif fault == "memory":
        fake.memory = "20Gi"
    elif fault == "cpu":
        fake.cpu = "7750m"
    elif fault == "pod-slots":
        fake.nodes[recovery.PROM_NODE]["status"]["allocatable"]["pods"] = "5"
    elif fault == "exclusion-toleration":
        fake.get_controller(plan["api_replica_set_name"], "ReplicaSet")["spec"]["template"]["spec"][
            "tolerations"
        ] = [{"operator": "Exists"}]
    else:
        fake.nodes["kwok-node-0"]["spec"]["taints"] = []
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    assert not fake.writes and not fake.deleted


def test_api_headroom_reserve_and_actual_http_ip_probe(environment):
    summary = run(environment, execute=True)
    _, _, fake = environment
    capacity = summary["last_capacity_proof"]
    assert capacity["next_memory_reserve_bytes"] == 8 * 1024**3
    assert capacity["memory_reserve_is_not_a_container_limit"]
    command = next(row for row in fake.writes if row[0] == "kubectl" and "run" in row)
    overrides = json.loads(next(part.split("=", 1)[1] for part in command if part.startswith("--overrides=")))
    spec = overrides["spec"]
    assert spec["nodeName"] == recovery.PROM_NODE and spec["hostNetwork"] is False
    assert spec["automountServiceAccountToken"] is False
    container = spec["containers"][0]
    assert container["image"].endswith("/agnhost:2.47")
    assert container["args"] == ["netexec", "--http-port=8080"]
    assert container["readinessProbe"]["httpGet"]["port"] == 8080
    assert "limits" not in fake.get_controller("clustermesh-apiserver")["spec"]["template"]["spec"]["containers"][0]


def test_unready_ip_probe_never_authorizes_pinned_pod_deletion(environment, monkeypatch):
    _, plan, fake = environment
    fake.probe_ready = False
    abort_wait(monkeypatch)
    with pytest.raises(recovery.workers.ReconcileError, match="Actual Ready IP probe"):
        run(environment, execute=True)
    assert all(row[2] != plan["api_pod_uid"] for row in fake.deleted)
    assert len(fake.deleted) == 1 and fake.deleted[0][1].startswith("prom-recovery-ip-")
    assert not receipt()["temporary_exclusions"] and not receipt()["probe_cleanup_pending"]
    assert recovery.MARKER_KEY in fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"]


def test_ambiguous_probe_create_is_owned_cleaned_without_retry_or_target_delete(environment):
    _, plan, fake = environment
    fake.probe_create_error = True
    with pytest.raises(recovery.workers.ReconcileError, match="probe create"):
        run(environment, execute=True)
    assert len([row for row in fake.writes if row[0] == "kubectl" and "run" in row]) == 1
    assert len(fake.deleted) == 1 and fake.deleted[0][2] != plan["api_pod_uid"]
    assert receipt()["probe_cleanup_pending"] is None and not receipt()["cleanup_errors"]


def test_probe_uid_delete_failure_is_not_retried_or_used_as_capacity(environment):
    _, plan, fake = environment
    fake.probe_delete_error = True
    with pytest.raises(recovery.mocks.RecoveryError, match="probe delete"):
        run(environment, execute=True)
    assert len(fake.deleted) == 1 and fake.deleted[0][2] != plan["api_pod_uid"]
    assert receipt()["cleanup_errors"] and not receipt()["repaired"]


def test_initial_naturally_ready_pod_is_never_deleted(environment):
    _, plan, fake = environment
    fake.get_pod(plan["api_pod_name"])["status"] = ready_status(True)
    fake.members[95]["meshProperties"]["status"] = {"state": "Connected"}
    summary = run(environment, execute=True)
    assert summary["pod_moves"][0]["state"] == "already-ready" and not fake.deleted
    assert not summary.get("ip_proofs")


def test_absent_original_uid_unique_owned_ready_replacement_is_adopted_read_only(environment):
    _, plan, fake = environment
    pod = fake.get_pod(plan["api_pod_name"])
    pod["metadata"].update({"name": "clustermesh-apiserver-unique-new", "uid": uid("natural-replacement")})
    pod["status"] = ready_status(True)
    fake.members[95]["meshProperties"]["status"] = {"state": "Connected"}
    fake.recover_host()
    summary = run(environment, execute=True)
    assert summary["repaired"] and summary["pod_moves"][0]["state"] == "adopted-ready"
    assert not fake.writes and not fake.deleted


def test_absent_original_uid_never_authorizes_new_pending_uid_delete(environment):
    _, plan, fake = environment
    pod = fake.get_pod(plan["api_pod_name"])
    pod["metadata"].update({"name": "clustermesh-apiserver-new-pending", "uid": uid("new-unbound-pending")})
    with pytest.raises(recovery.workers.ReconcileError, match="unpinned subsequent Pending"):
        run(environment, execute=True)
    assert not fake.writes and not fake.deleted


def test_pending_replacement_is_never_deleted_again(environment, monkeypatch):
    _, plan, fake = environment
    fake.replacement_ready = False
    abort_wait(monkeypatch)
    with pytest.raises(recovery.workers.ReconcileError, match="replacement readiness"):
        run(environment, execute=True)
    assert len([row for row in fake.deleted if row[2] == plan["api_pod_uid"]]) == 1
    assert len(fake.deleted) == 2
    assert not receipt()["repaired"]
    before = len(fake.deleted)
    with pytest.raises(recovery.workers.ReconcileError, match="unpinned subsequent Pending"):
        run(environment, execute=True)
    assert len(fake.deleted) == before


@pytest.mark.parametrize("fault", ["controller-spec", "pdb", "healthy-mock", "fleet-identity", "source-uid"])
def test_execute_rereads_authority_and_workload_guards_before_first_mutation(environment, fault):
    _, plan, fake = environment
    reads = 0

    def hook(command):
        nonlocal reads
        if command[:3] == ["az", "account", "show"]:
            reads += 1
            if reads == 2:
                if fault == "controller-spec":
                    fake.get_controller("clustermesh-apiserver")["spec"]["template"]["spec"]["containers"][0]["image"] = "changed"
                elif fault == "pdb":
                    fake.pdbs[0]["spec"]["minAvailable"] = 0
                elif fault == "healthy-mock":
                    fake.get_pod("kwok-node-0")["status"]["conditions"][0]["status"] = "False"
                elif fault == "fleet-identity":
                    fake.members[95]["meshProperties"]["ciliumProperties"]["name"] = "changed-identity"
                else:
                    fake.nodes[plan["cni_source"]["node_name"]]["metadata"]["uid"] = uid("new-source")
    fake.hook = hook
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    assert not fake.writes and not fake.deleted


@pytest.mark.parametrize("fault", ["wrong-peer-name", "disconnected-peer", "stale-fleet-failed", "post-move-healthy-loss"])
def test_strict_postproof_cannot_be_replaced_by_node_ready_or_assigned_ip_count(environment, monkeypatch, fault):
    _, _, fake = environment
    abort_wait(monkeypatch)
    if fault == "wrong-peer-name":
        fake.peer_fault = "wrong-name"
    elif fault == "disconnected-peer":
        fake.peer_fault = "disconnected"
    elif fault == "stale-fleet-failed":
        fake.fleet_stuck = True
    else:
        fake.delete_callback = lambda _old, _new: fake.get_pod("kwok-node-0")["status"]["conditions"][0].update(status="False")
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    summary = receipt()
    assert not summary["repaired"] and summary["status"] == "failed"
    assert recovery.MARKER_KEY in fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"]
    assert not summary["temporary_exclusions"] and summary["probe_cleanup_pending"] is None


def test_cleanup_preserves_foreign_holds_and_only_removes_owned_noschedule_taints(environment):
    _, _, fake = environment
    taint = {"key": "foreign/maintenance", "value": "do-not-remove", "effect": "NoSchedule"}
    fake.nodes[recovery.SOURCE_NODE]["spec"]["taints"] = [taint]
    summary = run(environment, execute=True)
    assert summary["repaired"]
    assert fake.nodes[recovery.SOURCE_NODE]["spec"]["taints"] == [taint]


def test_foreign_recovery_exclusion_is_not_removed(environment):
    _, _, fake = environment
    fake.recover_host()
    foreign = {"key": recovery.EXCLUSION_KEY, "value": "foreign-token", "effect": "NoSchedule"}
    fake.nodes[recovery.SOURCE_NODE]["spec"]["taints"] = [foreign]
    with pytest.raises(recovery.workers.ReconcileError, match="Foreign placement"):
        run(environment, execute=True)
    assert fake.nodes[recovery.SOURCE_NODE]["spec"]["taints"] == [foreign]
    assert not fake.deleted and not fake.writes


def test_uncertain_cleanup_prevents_success_and_has_no_repeated_mutations(environment):
    _, _, fake = environment
    fake.cleanup_error = True
    with pytest.raises(recovery.workers.ReconcileError, match="cleanup failed"):
        run(environment, execute=True)
    summary = receipt()
    assert not summary["repaired"] and summary["cleanup_errors"]
    cleanup_commands = [
        row for row in fake.writes if row[0] == "kubectl" and "patch" in row
        and any(op["op"] == "remove" and op["path"].startswith("/spec/taints/")
                for op in json.loads(fake.value(row, "-p")))
    ]
    assert len(cleanup_commands) == 2
    assert recovery.MARKER_KEY in fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"]


@pytest.mark.parametrize("message,expected_attempts", [
    ("AuthorizationFailed: forbidden 403", 1),
    ("authentication timeout AADSTS", 1),
    ("ServiceUnavailable", 3),
])
def test_only_transient_reads_retry_never_auth_or_mutations(environment, monkeypatch, message, expected_attempts):
    _, _, fake = environment
    monkeypatch.setattr(recovery.time, "sleep", lambda _seconds: None)
    calls = 0

    def fail_group(command):
        nonlocal calls
        if command[:3] == ["az", "group", "show"]:
            calls += 1
            raise recovery.workers.ReconcileError(message)
    fake.hook = fail_group
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    assert calls == expected_attempts and not fake.writes and not fake.deleted


def test_arm_summary_allowlists_vm_fields_and_never_publishes_private_credentials(environment):
    summary = run(environment)
    content = json.dumps(summary)
    assert "fake offline kubeconfig" not in content
    assert "osProfile" not in content and "customData" not in content and "protectedSettings" not in content
    assert "kubeconfig" not in content
    _, _, fake = environment
    for command in fake.commands:
        if command[:3] in (["az", "vmss", "list"], ["az", "vmss", "list-instances"], ["az", "vmss", "get-instance-view"]):
            assert "--query" in command


def test_cli_default_is_read_only_and_supports_only_the_agreed_arguments():
    arguments = [
        "--resource-group", recovery.RESOURCE_GROUP, "--confirm-resource-group", recovery.RESOURCE_GROUP,
        "--expected-subscription", recovery.SUBSCRIPTION, "--expected-region", recovery.REGION,
        "--expected-tfvars-sha", "a" * 64, "--plan-file", "plan.json", "--summary-file", "summary.json",
    ]
    parsed = recovery.parse_args(arguments)
    assert parsed.timeout_seconds == 1800 and parsed.execute is False
    assert recovery.parse_args([*arguments, "--execute"]).execute is True
    with pytest.raises(SystemExit):
        recovery.parse_args([*arguments, "--allow-unsafe"])


def test_plan_already_ready_foreign_marker_is_not_success_shaped(environment):
    _, plan, fake = environment
    set_marker(fake, plan, foreign=True)
    fake.recover_host()
    with pytest.raises(recovery.workers.ReconcileError, match="owned-marker"):
        run(environment)
    assert not receipt()["plan_valid"] and not receipt()["repaired"]
    assert not fake.writes and not fake.deleted
    assert set(receipt()["restart"]["prior_marker"]) == {"sha256"}


def test_marker_patch_ambiguous_acceptance_is_durable_before_any_restart(environment, monkeypatch):
    _, _, fake = environment
    original = fake.kubernetes

    def response_lost(command):
        result = original(command)
        if "patch" in command and any(
            row["op"] == "add" and row["path"] == "/metadata/annotations"
            for row in json.loads(fake.value(command, "-p"))
        ):
            raise recovery.workers.ReconcileError("marker response timeout after server accepted the patch")
        return result
    monkeypatch.setattr(fake, "kubernetes", response_lost)
    with pytest.raises(recovery.workers.ReconcileError, match="marker response"):
        run(environment, execute=True)
    assert recovery.MARKER_KEY in fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"]
    assert not receipt()["restart"]["attempted"] and not fake.deleted
    assert not any(row[:3] == ["az", "vmss", "restart"] for row in fake.writes)


def test_ambiguous_pinned_delete_never_retries_and_a_later_ready_pod_can_be_adopted(environment):
    _, plan, fake = environment

    def response_lost(_old, _new):
        raise recovery.mocks.RecoveryError("Pod delete response timeout after acceptance")
    fake.delete_callback = response_lost
    with pytest.raises(recovery.mocks.RecoveryError, match="delete response"):
        run(environment, execute=True)
    summary = receipt()
    assert summary["pod_moves"][0]["delete_attempted"] and summary["pod_moves"][0]["delete_ambiguous"]
    assert summary["pod_moves"][0]["delete_accepted"] is None
    assert not summary["repaired"] and not summary["temporary_exclusions"]
    assert len([row for row in fake.deleted if row[2] == plan["api_pod_uid"]]) == 1
    fake.delete_callback = None
    before = len(fake.deleted), len(fake.writes)
    summary = run(environment, execute=True)
    assert summary["repaired"] and summary["pod_moves"][0]["state"] == "adopted-ready"
    assert len(fake.deleted) == before[0]
    assert len(fake.writes) == before[1] + 1  # Only the successful owned marker is cleared.


def test_vm_identity_must_survive_restart_even_if_node_uid_and_provider_remain(environment):
    _, _, fake = environment
    fake.restart_callback = lambda: fake.instances[recovery.PROM_VMSS][0].update(vmId=uid("different-vm"))
    with pytest.raises(recovery.workers.ReconcileError, match="VM model/count"):
        run(environment, execute=True)
    assert receipt()["restart"]["accepted"] and not fake.deleted


def test_actual_metrics_cannot_predate_the_restart_even_when_recent(environment):
    _, _, fake = environment
    fake.metrics_timestamp = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
    with pytest.raises(recovery.workers.ReconcileError, match="predate"):
        run(environment, execute=True)
    assert not fake.deleted


def test_framework_init_container_limits_are_reserved_as_peak_memory(environment):
    target = add_framework(environment, name="kube-state-metrics")
    _, _, fake = environment
    for name, kind in ((target["deployment_name"], "Deployment"), (target["replica_set_name"], "ReplicaSet")):
        fake.get_controller(name, kind)["spec"]["template"]["spec"]["initContainers"] = [{
            "name": "framework-init", "image": "same-init", "resources": {"limits": {"memory": "3Gi"}},
        }]
    summary = run(environment, execute=True)
    assert summary["last_capacity_proof"]["next_memory_reserve_bytes"] == 3 * 1024**3
    assert summary["last_capacity_proof"]["prior_cpu_reserve_millicores"] == 500


def test_admission_changed_probe_spec_cannot_prove_ip_but_owned_uid_is_cleaned(environment, monkeypatch):
    _, plan, fake = environment
    original = fake.kubernetes

    def change_probe(command):
        result = original(command)
        if "run" in command:
            result["spec"]["containers"][0].pop("readinessProbe")
        return result
    monkeypatch.setattr(fake, "kubernetes", change_probe)
    with pytest.raises(recovery.workers.ReconcileError, match="HTTP readiness contract"):
        run(environment, execute=True)
    assert len(fake.deleted) == 1 and fake.deleted[0][2] != plan["api_pod_uid"]
    assert not receipt()["cleanup_errors"] and receipt()["probe_cleanup_pending"] is None


def test_cleanup_never_patches_a_same_name_replacement_node_uid(environment):
    _, _, fake = environment
    fake.delete_callback = lambda _old, _new: fake.nodes[recovery.SOURCE_NODE]["metadata"].update(uid=uid("replacement-node"))
    with pytest.raises(recovery.workers.ReconcileError, match="original Node UID"):
        run(environment, execute=True)
    summary = receipt()
    assert summary["cleanup_errors"] and not summary["repaired"]
    cleanup = [
        row for row in fake.writes if "patch" in row and recovery.SOURCE_NODE in row
        and any(op["op"] == "remove" for op in json.loads(fake.value(row, "-p")))
    ]
    assert not cleanup


def test_authentication_failure_during_peer_postproof_is_not_polled_or_retried(environment, monkeypatch):
    _, _, fake = environment
    abort_wait(monkeypatch)
    attempts = 0

    def deny_exec(command):
        nonlocal attempts
        if "exec" in command:
            attempts += 1
            raise recovery.workers.ReconcileError("Forbidden 403: peer status requires authorization")
    fake.hook = deny_exec
    with pytest.raises(recovery.workers.ReconcileError, match="authorization failure must not be retried"):
        run(environment, execute=True)
    assert attempts == 1 and not receipt()["repaired"]


def test_missing_default_cilium_agent_cannot_pass_three_agent_postproof(environment, monkeypatch):
    _, _, fake = environment
    fake.pods.remove(fake.get_pod(f"cilium-{recovery.SOURCE_NODE}"))
    abort_wait(monkeypatch)
    with pytest.raises(recovery.workers.ReconcileError, match="Strict three-agent"):
        run(environment, execute=True)
    assert receipt()["cilium_proof"]["cilium_agent_count"] == 2
    assert not receipt()["repaired"]


def test_new_other_fleet_fault_after_repair_is_not_treated_as_stale_selected_fault(environment):
    _, _, fake = environment
    fake.delete_callback = lambda _old, _new: fake.members[0]["meshProperties"].update(
        status={"state": "Failed", "error": {"code": "ConnectivityTimeout"}},
    )
    with pytest.raises(recovery.workers.ReconcileError, match="Only the original"):
        run(environment, execute=True)
    assert not receipt()["repaired"] and not receipt()["temporary_exclusions"]


def test_private_cli_and_certificate_files_are_removed_and_environment_is_restored(environment, monkeypatch):
    _, _, fake = environment
    original_environment = {name: os.environ.get(name) for name in ("TMPDIR", "TEMP", "TMP")}
    original_tempdir = tempfile.tempdir
    original = fake.azure

    def with_certificate_file(command):
        result = original(command)
        if command[1:3] == ["aks", "get-credentials"]:
            Path(os.environ["TMPDIR"], "fake-client-ca.crt").write_text("offline test certificate", encoding="utf-8")
        return result
    monkeypatch.setattr(fake, "azure", with_certificate_file)
    assert run(environment)["plan_valid"]
    assert {name: os.environ.get(name) for name in original_environment} == original_environment
    assert tempfile.tempdir == original_tempdir
    assert not list(Path(".").glob(".unreachable-prom-private-*"))


def test_unexpected_cleanup_error_still_removes_credentials_and_persists_failure(environment, monkeypatch):
    def broken_cleanup(_self):
        raise RuntimeError("unexpected cleanup error")
    monkeypatch.setattr(recovery.Recovery, "cleanup", broken_cleanup)
    with pytest.raises(RuntimeError, match="unexpected cleanup"):
        run(environment)
    summary = receipt()
    assert summary["status"] == "failed" and not summary["repaired"]
    assert not list(Path(".").glob(".unreachable-prom-private-*"))


@pytest.mark.parametrize("deployment_name", ["metrics-server", "konnectivity-agent"])
def test_other_pending_frameworks_are_left_to_the_existing_cni_worker_phase(environment, deployment_name):
    _, plan, fake = environment
    target = {
        "namespace": "kube-system", "deployment_name": deployment_name, "deployment_uid": uid("deferred-deployment"),
        "replica_set_name": f"{deployment_name}-abc", "replica_set_uid": uid("deferred-rs"),
        "pod_name": f"{deployment_name}-abc-pending", "pod_uid": uid("deferred-pending"),
    }
    plan["framework_pods"].append(target)
    fake.add_target(target)
    with pytest.raises(recovery.workers.ReconcileError, match="not explicitly supported"):
        run(environment, execute=True)
    assert not fake.writes and not fake.deleted


@pytest.mark.parametrize("successful", [True, False])
def test_main_cli_persists_explicit_plan_success_or_failure_without_live_tools(environment, monkeypatch, successful):
    args, _, fake = environment
    execute = recovery.execute_recovery

    def offline(options, summary):
        fake.args = options
        return execute(options, summary, runner=fake.run, delete_pod=fake.delete)
    monkeypatch.setattr(recovery, "execute_recovery", offline)
    if not successful:
        fake.group["tags"]["deletion_due_time"] = now()
    argv = [
        "--resource-group", args.resource_group, "--confirm-resource-group", args.confirm_resource_group,
        "--expected-subscription", args.expected_subscription, "--expected-region", args.expected_region,
        "--expected-tfvars-sha", args.expected_tfvars_sha, "--plan-file", args.plan_file,
        "--summary-file", args.summary_file,
    ]
    assert recovery.main(argv) == (0 if successful else 1)
    assert receipt()["status"] == ("plan_valid" if successful else "failed")
    assert receipt()["plan_valid"] is successful and receipt()["repaired"] is False
    assert not fake.writes and not fake.deleted


def test_nontransient_peer_schema_failure_is_not_retried(environment, monkeypatch):
    _, _, fake = environment
    original = fake.kubernetes
    attempts = 0

    def malformed_status(command):
        nonlocal attempts
        if "exec" in command:
            attempts += 1
            return "not-json"
        return original(command)
    monkeypatch.setattr(fake, "kubernetes", malformed_status)
    abort_wait(monkeypatch)
    with pytest.raises(recovery.workers.ReconcileError, match="Nontransient Cilium"):
        run(environment, execute=True)
    assert attempts == 1 and not receipt()["repaired"]


def five_pod_selection(environment):
    dns = select_dns(environment)
    metrics = add_framework(environment, "kube-state-metrics", pinned_deployment=False)
    grafana = add_framework(environment, pinned_deployment=False)
    return [*dns, environment[2].api_target, metrics, grafana]


def test_five_pod_plan_is_dns_first_without_any_resource_mutations(environment):
    expected = five_pod_selection(environment)
    _, plan, fake = environment
    plan["framework_pods"].reverse()
    summary = run(environment)
    assert summary["plan_valid"] and not summary["repaired"]
    assert [row["pod_uid"] for row in summary["planned_actions"]["pods"]] == [row["pod_uid"] for row in expected]
    assert summary["dns_proof"]["replicas"] == 5 and not summary["dns_proof"]["all_ready"]
    assert summary["dns_proof"]["ready_replicas"] == 3
    assert not fake.writes and not fake.deleted and not summary["mutation_started"]
    assert recovery.MARKER_KEY not in fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"]


def test_dns_pair_then_api_then_kube_state_metrics_and_grafana_all_with_exact_uid_ip_exchange(environment):
    expected = five_pod_selection(environment)
    _, plan, fake = environment
    plan["framework_pods"].reverse()
    fake.nodes[recovery.PROM_NODE]["status"]["allocatable"]["memory"] = "27592916Ki"
    healthy_dns = {
        row["metadata"]["uid"]: copy.deepcopy(row) for row in fake.pods
        if row["metadata"]["name"].startswith(f"{recovery.DNS_REPLICA_SET}-healthy-")
    }
    original_controllers = copy.deepcopy(fake.controllers)
    original_pdbs = copy.deepcopy(fake.pdbs)
    original_mocks = {row["metadata"]["name"]: copy.deepcopy(row) for row in fake.pods
                      if row["metadata"].get("namespace") == "mock-clustermesh"}
    summary = run(environment, execute=True)
    assert summary["repaired"] and summary["dns_proof"]["all_ready"]
    assert summary["dns_proof"]["ready_replicas"] == 5
    assert [row["pod_uid"] for row in summary["pod_moves"]] == [row["pod_uid"] for row in expected]
    assert [row[2] for row in fake.deleted if not row[1].startswith("prom-recovery-ip-")] == [
        row["pod_uid"] for row in expected
    ]
    assert len(fake.deleted) == 10 and len(summary["ip_proofs"]) == 5
    for index in range(0, 10, 2):
        assert fake.deleted[index][1].startswith("prom-recovery-ip-")
        assert fake.deleted[index + 1][2] == expected[index // 2]["pod_uid"]
    assert all(row["state"] == "moved" and row["ready_node"] == recovery.PROM_NODE for row in summary["pod_moves"])
    for pod_uid, original in healthy_dns.items():
        assert next(row for row in fake.pods if row["metadata"]["uid"] == pod_uid) == original
    assert fake.controllers == original_controllers and fake.pdbs == original_pdbs
    assert original_mocks == {row["metadata"]["name"]: row for row in fake.pods
                              if row["metadata"].get("namespace") == "mock-clustermesh"}
    assert summary["last_capacity_proof"]["next_memory_reserve_bytes"] == 8 * 1024**3
    commitments = summary["memory_commitments"]
    assert sum(row["reserved_memory_bytes"] for row in commitments.values()) == (
        16 * 1024**3 + 2 * 1024**3 + 1000 * 1024**2
    )
    for name in ("clustermesh-apiserver", "grafana"):
        template = fake.get_controller(name)["spec"]["template"]["spec"]
        assert all(not row.get("resources", {}).get("limits") for row in template["containers"])
    assert summary["final_mock_ready"] == 71 and summary["final_mock_pending"] == 29
    assert not summary["cleanup_errors"] and not summary["workloads_ready"]


def test_missing_dns_selection_cannot_bootstrap_api_while_coredns_is_pending(environment):
    select_dns(environment)
    _, plan, fake = environment
    plan["framework_pods"] = []
    with pytest.raises(recovery.workers.ReconcileError, match="Both pinned CoreDNS recoveries"):
        run(environment, execute=True)
    assert not fake.writes and not fake.deleted


def test_dns_selection_must_pin_both_approved_original_uids(environment):
    select_dns(environment)
    _, plan, fake = environment
    plan["framework_pods"].pop()
    with pytest.raises(recovery.workers.ReconcileError, match="selected together"):
        run(environment)
    plan["framework_pods"].append(copy.deepcopy(recovery.APPROVED_FRAMEWORKS[1]))
    plan["framework_pods"][0]["pod_uid"] = uid("arbitrary-coredns")
    with pytest.raises(recovery.workers.ReconcileError, match="not explicitly supported"):
        run(environment)
    assert not fake.commands and not fake.writes


@pytest.mark.parametrize("fault", ["pvc", "started", "ip", "missing-cns-event", "wrong-event-nc", "unselected-unready"])
def test_two_pending_dns_siblings_require_individual_safe_live_proofs(environment, fault):
    targets = select_dns(environment)
    _, _, fake = environment
    second = fake.get_pod(targets[1]["pod_name"])
    if fault == "pvc":
        second["spec"]["volumes"] = [{"name": "data", "persistentVolumeClaim": {"claimName": "keep"}}]
    elif fault == "started":
        second["status"]["containerStatuses"][0]["started"] = True
    elif fault == "ip":
        second["status"]["podIP"] = "10.0.0.5"
    elif fault == "missing-cns-event":
        fake.events = [row for row in fake.events if row["involvedObject"]["uid"] != targets[1]["pod_uid"]]
    elif fault == "wrong-event-nc":
        for row in fake.events:
            if row["involvedObject"]["uid"] == targets[1]["pod_uid"]:
                row["message"] = row["message"].replace(recovery.SOURCE_NC, uid("wrong-nc"))
    else:
        fake.get_pod(f"{recovery.DNS_REPLICA_SET}-healthy-2")["status"]["conditions"][0]["status"] = "False"
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    assert not fake.writes and not fake.deleted


def test_dns_replica_count_is_five_not_two_and_cannot_be_changed(environment):
    select_dns(environment)
    _, _, fake = environment
    fake.get_controller("coredns")["spec"]["replicas"] = 2
    fake.get_controller(recovery.DNS_REPLICA_SET, "ReplicaSet")["spec"]["replicas"] = 2
    with pytest.raises(recovery.workers.ReconcileError, match="five-replica"):
        run(environment, execute=True)
    assert not fake.writes


def test_dns_replacement_must_be_ready_before_any_api_probe_or_delete(environment, monkeypatch):
    expected = five_pod_selection(environment)
    _, _, fake = environment
    fake.replacement_ready = False
    abort_wait(monkeypatch)
    with pytest.raises(recovery.workers.ReconcileError, match="replacement readiness"):
        run(environment, execute=True)
    assert [row[2] for row in fake.deleted if not row[1].startswith("prom-recovery-ip-")] == [expected[0]["pod_uid"]]
    assert not any(row[2] == fake.plan["api_pod_uid"] for row in fake.deleted)
    assert len([row for row in fake.writes if row[0] == "kubectl" and "run" in row]) == 1
    assert not receipt()["temporary_exclusions"]


def test_healthy_dns_sibling_uid_cannot_drift_during_either_dns_move(environment):
    expected = five_pod_selection(environment)
    _, _, fake = environment

    def replace_healthy_dns(old, _new):
        if old["metadata"]["uid"] == expected[0]["pod_uid"]:
            fake.get_pod(f"{recovery.DNS_REPLICA_SET}-healthy-2")["metadata"]["uid"] = uid("changed-dns-sibling")
    fake.delete_callback = replace_healthy_dns
    with pytest.raises(recovery.workers.ReconcileError, match="CoreDNS sibling changed"):
        run(environment, execute=True)
    assert [row[2] for row in fake.deleted if not row[1].startswith("prom-recovery-ip-")] == [expected[0]["pod_uid"]]
    assert not receipt()["repaired"]


def test_dns_ready_regression_blocks_api_bootstrap_after_both_dns_deletions(environment):
    expected = five_pod_selection(environment)
    _, _, fake = environment

    def regress_first_dns(old, _new):
        if old["metadata"]["uid"] == expected[1]["pod_uid"]:
            fake.get_pod(f"{expected[0]['pod_name']}-replacement")["status"]["conditions"][0]["status"] = "False"
    fake.delete_callback = regress_first_dns
    with pytest.raises(recovery.workers.ReconcileError, match="CoreDNS sibling changed"):
        run(environment, execute=True)
    assert [row[2] for row in fake.deleted if not row[1].startswith("prom-recovery-ip-")] == [
        expected[0]["pod_uid"], expected[1]["pod_uid"],
    ]
    assert not receipt()["repaired"] and not receipt()["temporary_exclusions"]


def test_partial_dns_delete_receipt_can_resume_with_read_only_ready_uid_set_adoption(environment):
    expected = five_pod_selection(environment)
    _, _, fake = environment

    def lose_first_response(old, _new):
        if old["metadata"]["uid"] == expected[0]["pod_uid"]:
            raise recovery.mocks.RecoveryError("lost first DNS delete response")
    fake.delete_callback = lose_first_response
    with pytest.raises(recovery.mocks.RecoveryError, match="lost first DNS"):
        run(environment, execute=True)
    assert not receipt()["repaired"]
    fake.delete_callback = None
    summary = run(environment, execute=True)
    assert summary["repaired"] and not summary["restart"]["attempted"]
    assert summary["pod_moves"][0]["state"] == "adopted-ready-group"
    assert summary["read_only_replica_set_adoptions"][0]["individual_ancestry_claimed"] is False
    assert len([row for row in fake.deleted if row[2] == expected[0]["pod_uid"]]) == 1
    assert {row[2] for row in fake.deleted if not row[1].startswith("prom-recovery-ip-")} == {
        row["pod_uid"] for row in expected
    }


def test_complete_dns_ready_uid_set_is_adopted_without_new_deletions(environment):
    expected = five_pod_selection(environment)
    _, _, fake = environment
    summary = run(environment, execute=True)
    assert summary["repaired"]
    old_deletes, old_writes = len(fake.deleted), len(fake.writes)
    summary = run(environment, execute=True)
    assert summary["repaired"] and not summary["restart"]["attempted"]
    assert all(row["state"] == "adopted-ready-group" for row in summary["pod_moves"][:2])
    assert len(fake.deleted) == old_deletes and len(fake.writes) == old_writes
    assert len(summary["pod_moves"]) == len(expected)


def test_grafana_requires_its_own_eight_gib_reserve_and_real_ip_proof(environment):
    target = add_framework(environment)
    _, _, fake = environment
    summary = run(environment, execute=True)
    assert summary["last_capacity_proof"]["next_memory_reserve_bytes"] == 8 * 1024**3
    assert len(summary["ip_proofs"]) == 2
    assert ("monitoring", target["pod_name"], target["pod_uid"]) in fake.deleted
    assert sum(row["reserved_memory_bytes"] for row in summary["memory_commitments"].values()) == 16 * 1024**3


def test_memory_high_water_retains_commitments_after_observed_usage_rises_and_falls(environment):
    add_framework(environment)
    _, plan, fake = environment
    reads_after_api = 0

    def usage_changes(command):
        nonlocal reads_after_api
        if "--raw" in command and "/apis/metrics.k8s.io/v1beta1/nodes" in command:
            if any(row[2] == plan["api_pod_uid"] for row in fake.deleted):
                reads_after_api += 1
                fake.memory = "14Gi" if reads_after_api == 1 else "4Gi"
    fake.hook = usage_changes
    summary = run(environment, execute=True)
    proof = summary["last_capacity_proof"]
    assert proof["baseline_high_water_bytes"] == 6 * 1024**3
    assert proof["prior_move_reserve_bytes"] == 8 * 1024**3
    assert proof["effective_reserved_memory_bytes"] == 10 * 1024**3
    assert summary["memory_state"]["committed_high_water_bytes"] == 22 * 1024**3
    assert summary["memory_state"]["observed_memory_bytes"] == 4 * 1024**3


def test_grafana_is_not_deleted_if_its_full_reserve_would_exceed_actual_headroom(environment):
    target = add_framework(environment)
    _, plan, fake = environment

    def memory_pressure_after_api(command):
        if "--raw" in command and any(row[2] == plan["api_pod_uid"] for row in fake.deleted):
            fake.memory = "15Gi"
    fake.hook = memory_pressure_after_api
    with pytest.raises(recovery.workers.ReconcileError, match="API/Grafana reserves are 8Gi each"):
        run(environment, execute=True)
    assert not any(row[2] == target["pod_uid"] for row in fake.deleted)
    assert receipt()["memory_state"]["next_memory_reserve_bytes"] == 8 * 1024**3
    assert not receipt()["repaired"]


def test_postproof_rejects_actual_memory_growth_beyond_committed_headroom(environment):
    target = add_framework(environment)
    _, _, fake = environment

    def grafana_uses_more_than_its_reserve(old, _new):
        if old["metadata"]["uid"] == target["pod_uid"]:
            fake.memory = "24Gi"
    fake.delete_callback = grafana_uses_more_than_its_reserve
    with pytest.raises(recovery.workers.ReconcileError, match="Actual memory headroom"):
        run(environment, execute=True)
    assert not receipt()["repaired"] and not receipt()["temporary_exclusions"]
    assert receipt()["memory_state"]["committed_high_water_bytes"] == 24 * 1024**3


@pytest.mark.parametrize("writer_newline", [b"", b"\n"])
def test_plan_serialized_limit_accepts_exactly_32768_bytes_with_optional_writer_newline(environment, writer_newline):
    args, plan, _ = environment
    five_pod_selection(environment)
    encoded = json.dumps(plan, separators=(",", ":")).encode("utf-8")
    assert len(encoded) < recovery.MAX_PLAN_BYTES
    serialized = encoded + b" " * (recovery.MAX_PLAN_BYTES - len(encoded)) + writer_newline
    Path(args.plan_file).write_bytes(serialized)
    assert recovery.load_plan(args.plan_file) == plan
    assert Path(args.plan_file).read_bytes() == serialized


@pytest.mark.parametrize("writer_newline", [b"", b"\n"])
def test_oversized_plan_fails_before_any_commands_and_has_standard_failure_fields(environment, writer_newline):
    args, plan, fake = environment
    encoded = json.dumps(plan, separators=(",", ":")).encode("utf-8")
    serialized = encoded + b" " * (recovery.MAX_PLAN_BYTES + 1 - len(encoded)) + writer_newline
    Path(args.plan_file).write_bytes(serialized)
    with pytest.raises(recovery.workers.ReconcileError, match="32768 bytes"):
        recovery.execute_recovery(args, {}, runner=fake.run, delete_pod=fake.delete)
    summary = receipt()
    assert summary["execute"] is False and summary["mutation_started"] is False
    assert summary["plan_valid"] is False and summary["success"] is False
    assert not fake.commands and not fake.writes


def test_size_limit_cannot_be_bypassed_with_padding_or_direct_validation(environment):
    args, plan, fake = environment
    Path(args.plan_file).write_bytes(b"\n" * (recovery.MAX_PLAN_BYTES + 100))
    with pytest.raises(recovery.workers.ReconcileError, match="32768 bytes"):
        recovery.execute_recovery(args, {}, runner=fake.run, delete_pod=fake.delete)
    oversized = copy.deepcopy(plan)
    oversized["provider_id"] += "/" * recovery.MAX_PLAN_BYTES
    with pytest.raises(recovery.workers.ReconcileError, match="exceeds 32768 bytes"):
        recovery.validate_plan(oversized)
    assert not fake.commands


def test_parent_plan_then_execute_summary_contract_keeps_input_sha_unchanged(environment):
    five_pod_selection(environment)
    args, plan, fake = environment
    serialized = json.dumps(plan, separators=(",", ":")).encode("utf-8") + b"\n"
    assert len(serialized) - 1 <= recovery.MAX_PLAN_BYTES
    Path(args.plan_file).write_bytes(serialized)
    expected_sha = hashlib.sha256(serialized).hexdigest()
    summary = {}
    recovery.execute_recovery(args, summary, runner=fake.run, delete_pod=fake.delete)
    assert summary["execute"] is False and summary["mutation_started"] is False
    assert summary["plan_valid"] is True or summary["success"] is True
    assert summary["plan_valid"] is True and summary["success"] is True and summary["repaired"] is False
    assert hashlib.sha256(Path(args.plan_file).read_bytes()).hexdigest() == expected_sha
    assert not fake.writes and not fake.deleted
    args.execute = True
    args.summary_file = "recovery.json"
    recovery_summary = {}
    recovery.execute_recovery(args, recovery_summary, runner=fake.run, delete_pod=fake.delete)
    assert recovery_summary["execute"] is True and recovery_summary["mutation_started"] is True
    assert recovery_summary["repaired"] is True and recovery_summary["success"] is True
    assert hashlib.sha256(Path(args.plan_file).read_bytes()).hexdigest() == expected_sha


@pytest.mark.parametrize("deployment_name", ["grafana", "kube-state-metrics"])
def test_framework_replica_count_is_preserved_not_assumed_one_or_limited_by_recovery_count(environment, deployment_name):
    target = add_framework(environment, name=deployment_name, pinned_deployment=False)
    _, _, fake = environment
    fake.get_controller(deployment_name)["spec"]["replicas"] = 6
    fake.get_controller(target["replica_set_name"], "ReplicaSet")["spec"]["replicas"] = 6
    siblings = []
    for index in range(5):
        sibling = make_pod(
            f"{target['replica_set_name']}-healthy-{index}", target["namespace"], uid(f"{deployment_name}-sibling-{index}"),
            f"{recovery.DEFAULT_VMSS}000001",
            reference("ReplicaSet", target["replica_set_name"], target["replica_set_uid"]), ready=True,
        )
        fake.pods.append(sibling)
        siblings.append(copy.deepcopy(sibling))
    summary = run(environment, execute=True)
    assert summary["success"] and summary["repaired"] and len(summary["pod_moves"]) == 2
    assert fake.get_controller(deployment_name)["spec"]["replicas"] == 6
    assert fake.get_controller(target["replica_set_name"], "ReplicaSet")["spec"]["replicas"] == 6
    assert all(fake.get_pod(row["metadata"]["name"]) == row for row in siblings)
    assert all(row[2] not in {pod["metadata"]["uid"] for pod in siblings} for row in fake.deleted)
