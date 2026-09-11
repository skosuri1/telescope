"""Tests for bounded single-source real-worker CNI maintenance."""

# pylint: disable=protected-access,too-many-lines

from __future__ import annotations

import copy
import importlib.util
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest


MODULE_DIR = (
    Path(__file__).resolve().parents[1] / "clusterloader2" / "clustermesh-scale"
)
SPEC = importlib.util.spec_from_file_location(
    "cni_worker_maintenance",
    MODULE_DIR / "cni_worker_maintenance.py",
)
maintenance = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = maintenance
sys.path.insert(0, str(MODULE_DIR))
try:
    SPEC.loader.exec_module(maintenance)
finally:
    sys.path.pop(0)


SUBSCRIPTION = "11111111-1111-1111-1111-111111111111"
RESOURCE_GROUP = "78751-f36f3d5a"
ROLE = "mesh-89"
CLUSTER_NAME = "clustermesh-89"
NODE_RESOURCE_GROUP = f"MC_{RESOURCE_GROUP}_{CLUSTER_NAME}_eastus2euap"
VMSS = "aks-default-13520174-vmss"
OLD_A = f"{VMSS}000000"
OLD_B = f"{VMSS}000001"
SOURCE = f"{VMSS}000002"
FRESH_A = f"{VMSS}000003"
FRESH_B = f"{VMSS}000004"
SOURCE_UID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
SOURCE_NC_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
SOURCE_PROVIDER = (
    f"azure:///subscriptions/{SUBSCRIPTION}/resourceGroups/{NODE_RESOURCE_GROUP}/"
    f"providers/Microsoft.Compute/virtualMachineScaleSets/{VMSS}/virtualMachines/2"
)
NODE_IMAGE = "AKSUbuntu-2404containerd-202609.03.1"
NOW = datetime.now(timezone.utc)


@pytest.fixture(name="args")
def maintenance_args(tmp_path):
    return SimpleNamespace(
        expected_subscription=SUBSCRIPTION,
        resource_group=RESOURCE_GROUP,
        confirm_resource_group=RESOURCE_GROUP,
        expected_region="eastus2euap",
        expected_tfvars_sha="a" * 64,
        role=ROLE,
        node_name=SOURCE,
        node_uid=SOURCE_UID,
        source_provider_id=SOURCE_PROVIDER,
        source_network_container_id=SOURCE_NC_ID,
        kubeconfig="/fake/mesh-89.config",
        context=CLUSTER_NAME,
        summary_file=str(tmp_path / "summary.json"),
        timeout_seconds=900,
        request_timeout_seconds=45,
        poll_seconds=1,
        per_pod_ready_seconds=30,
        probe_image=maintenance.DEFAULT_PROBE_IMAGE,
        probe_pod_count=2,
        memory_threshold_percent=85,
        execute=True,
    )


def scope_data(options):
    scope = (
        f"/subscriptions/{options.expected_subscription}"
        f"/resourceGroups/{options.resource_group}"
    )
    expiry = (NOW + timedelta(days=1)).isoformat()
    group = {
        "id": scope,
        "location": options.expected_region,
        "tags": {
            "clustermesh_debug_preserved": "true",
            "run_id": options.resource_group,
            "scenario": "perf-eval-clustermesh-scale",
            "clustermesh_debug_expected_clusters": "100",
            "clustermesh_debug_tfvars_sha256": options.expected_tfvars_sha,
            "deletion_due_time": expiry,
        },
    }
    clusters = []
    members = []
    fleet_scope = (
        f"{scope}/providers/Microsoft.ContainerService/fleets/clustermesh-flt"
    )
    for index in range(1, 101):
        role = f"mesh-{index}"
        name = f"clustermesh-{index}"
        resource_id = (
            f"{scope}/providers/Microsoft.ContainerService/managedClusters/{name}"
        )
        clusters.append(
            {
                "id": resource_id,
                "name": name,
                "location": options.expected_region,
                "nodeResourceGroup": (
                    f"MC_{options.resource_group}_{name}_eastus2euap"
                ),
                "tags": {"role": role, "run_id": options.resource_group},
                "provisioningState": "Succeeded",
                "powerState": {"code": "Running"},
            }
        )
        members.append(
            {
                "id": f"{fleet_scope}/members/{role}",
                "name": role,
                "clusterResourceId": resource_id,
                "provisioningState": "Succeeded",
                "labels": {"mesh": "true"},
                "meshProperties": {
                    "ciliumProperties": {"name": f"assigned-{index}", "id": index},
                    "clusterMeshProfileResourceId": (
                        f"{fleet_scope}/clusterMeshProfiles/clustermesh-cmp"
                    ),
                    "status": {"state": "Connected"},
                },
            }
        )
    return group, clusters, members


def node(name, uid, instance_id, *, ready=True, unschedulable=False):
    return {
        "metadata": {
            "name": name,
            "uid": uid,
            "resourceVersion": f"rv-{uid}",
            "labels": {
                "agentpool": "default",
                "kubernetes.azure.com/agentpool": "default",
                "kubernetes.azure.com/cluster": NODE_RESOURCE_GROUP,
                "kubernetes.azure.com/node-image-version": NODE_IMAGE,
            },
            "annotations": {},
        },
        "spec": {
            "providerID": (
                f"azure:///subscriptions/{SUBSCRIPTION}/resourceGroups/"
                f"{NODE_RESOURCE_GROUP}/providers/Microsoft.Compute/"
                f"virtualMachineScaleSets/{VMSS}/virtualMachines/{instance_id}"
            ),
            "unschedulable": unschedulable,
            "taints": [],
        },
        "status": {
            "allocatable": {"cpu": "7820m", "memory": "30Gi", "pods": "110"},
            "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
        },
    }


def kwok(index, *, ready=True):
    return {
        "metadata": {
            "name": f"kwok-node-{index}",
            "uid": f"kwok-uid-{index}",
            "labels": {"type": "kwok"},
        },
        "spec": {},
        "status": {
            "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
        },
    }


def mock_agent(name, uid, node_name, *, ready=True, deleting=False):
    status = {
        "phase": "Running" if ready else "Pending",
        "containerStatuses": (
            [{"ready": True}]
            if ready
            else [{"ready": False, "state": {"waiting": {"reason": "ContainerCreating"}}}]
        ),
        "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
    }
    metadata = {
        "name": name,
        "uid": uid,
        "namespace": "mock-clustermesh",
        "labels": {
            "app": "mock-cilium-agent",
            "mock-clustermesh/agent-controller": "kwok-node",
        },
        "ownerReferences": [
            {
                "kind": "StatefulSet",
                "name": "kwok-node",
                "uid": "controller-uid",
                "controller": True,
            }
        ],
    }
    if deleting:
        metadata["deletionTimestamp"] = NOW.isoformat()
    return {
        "metadata": metadata,
        "spec": {
            "nodeName": node_name,
            "containers": [
                {
                    "name": "mock-cilium-agent",
                    "resources": {
                        "requests": {"cpu": "100m", "memory": "256Mi"}
                    },
                }
            ],
        },
        "status": status,
    }


def event_for(name, uid):
    return {
        "involvedObject": {"kind": "Pod", "name": name, "uid": uid},
        "reason": "FailedCreatePodSandBox",
        "message": (
            "cilium-cni failed: AllocateIPConfig failed: not enough IPs "
            "available of type ipv4"
        ),
    }


def daemonset_pod(name, node_name, owner):
    return {
        "metadata": {
            "name": f"{name}-{node_name[-1]}",
            "namespace": "kube-system",
            "ownerReferences": [
                {
                    "kind": "DaemonSet",
                    "name": name,
                    "uid": owner,
                    "controller": True,
                }
            ],
        },
        "spec": {"nodeName": node_name, "hostNetwork": True},
        "status": {
            "phase": "Running",
            "containerStatuses": [{"ready": True}],
            "conditions": [{"type": "Ready", "status": "True"}],
        },
    }


def cilium_operator_pod(node_name):
    return {
        "metadata": {
            "name": "cilium-operator-abc",
            "namespace": "kube-system",
            "ownerReferences": [
                {
                    "kind": "ReplicaSet",
                    "name": "cilium-operator-rs",
                    "uid": "rs-cilium-operator",
                    "controller": True,
                }
            ],
        },
        "spec": {
            "nodeName": node_name,
            "hostNetwork": True,
            "volumes": [],
            "containers": [{"name": "cilium-operator"}],
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [{"ready": True}],
            "conditions": [{"type": "Ready", "status": "True"}],
        },
    }


def kube_state_metrics_pod(node_name):
    return {
        "metadata": {
            "name": "kube-state-metrics-0",
            "namespace": "kube-state-metrics-perf-test",
            "ownerReferences": [
                {
                    "kind": "ReplicaSet",
                    "name": "kube-state-metrics-rs",
                    "uid": "rs-ksm",
                    "controller": True,
                }
            ],
        },
        "spec": {
            "nodeName": node_name,
            "hostNetwork": False,
            "volumes": [{"emptyDir": {}}],
            "containers": [{"name": "kube-state-metrics"}],
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [{"ready": True}],
            "conditions": [{"type": "Ready", "status": "True"}],
        },
    }


def controller_payload():
    return {
        "metadata": {"uid": "controller-uid"},
        "spec": {
            "replicas": 100,
            "template": {"spec": {"tolerations": [], "nodeSelector": {}, "affinity": {}}},
        },
    }


def deployments_payload():
    return {
        "items": [
            {
                "metadata": {
                    "namespace": "kube-system",
                    "name": "cilium-operator",
                    "uid": "dep-cilium-operator",
                }
            },
            {
                "metadata": {
                    "namespace": "kube-state-metrics-perf-test",
                    "name": "kube-state-metrics",
                    "uid": "dep-ksm",
                }
            },
        ]
    }


def replicasets_payload():
    return {
        "items": [
            {
                "metadata": {
                    "namespace": "kube-system",
                    "name": "cilium-operator-rs",
                    "uid": "rs-cilium-operator",
                    "ownerReferences": [
                        {
                            "kind": "Deployment",
                            "name": "cilium-operator",
                            "uid": "dep-cilium-operator",
                            "controller": True,
                        }
                    ],
                }
            },
            {
                "metadata": {
                    "namespace": "kube-state-metrics-perf-test",
                    "name": "kube-state-metrics-rs",
                    "uid": "rs-ksm",
                    "ownerReferences": [
                        {
                            "kind": "Deployment",
                            "name": "kube-state-metrics",
                            "uid": "dep-ksm",
                            "controller": True,
                        }
                    ],
                }
            },
        ]
    }


def statefulsets_payload():
    return {"items": []}


def daemonsets_payload():
    return {
        "items": [
            {"metadata": {"namespace": "kube-system", "name": "cilium", "uid": "ds-cilium"}},
            {"metadata": {"namespace": "kube-system", "name": "ama", "uid": "ds-ama"}},
        ]
    }


def nnc_payload(node_name, node_uid, nc_id, assigned, ips):
    return {
        "metadata": {
            "name": node_name,
            "uid": f"nnc-{node_name}",
            "ownerReferences": [
                {
                    "kind": "Node",
                    "name": node_name,
                    "uid": node_uid,
                    "controller": True,
                }
            ],
        },
        "spec": {"requestedIPCount": 16},
        "status": {
            "assignedIPCount": assigned,
            "networkContainers": [
                {
                    "id": nc_id,
                    "version": 0,
                    "ipAssignments": [
                        {"ip": ip, "name": f"{node_name}-{index}"}
                        for index, ip in enumerate(ips)
                    ],
                }
            ],
        },
    }


def pool_payload(count=3, *, provisioning="Succeeded", autoscaling=False):
    scope = (
        f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{RESOURCE_GROUP}/providers/"
        f"Microsoft.ContainerService/managedClusters/{CLUSTER_NAME}/agentPools/default"
    )
    return {
        "id": scope,
        "name": "default",
        "count": count,
        "enableAutoScaling": autoscaling,
        "provisioningState": provisioning,
        "powerState": {"code": "Running"},
        "nodeImageVersion": NODE_IMAGE,
        "vmSize": "Standard_D8_v3",
    }


def prom_pool_state():
    return maintenance.workers.PoolState(
        role=ROLE,
        cluster_name=CLUSTER_NAME,
        resource_group=RESOURCE_GROUP,
        node_resource_group=NODE_RESOURCE_GROUP,
        pool_name="prompool",
        desired_count=1,
        pool_provisioning_state="Succeeded",
        pool_power_state="Running",
        vmss_name="aks-prompool-vmss",
        vmss_capacity=1,
        vmss_provisioning_state="Succeeded",
        instance_ids=["0"],
        failed_instance_ids=[],
        node_instance_ids=["0"],
        ready_instance_ids=["0"],
        unschedulable_nodes=[],
        stale_instance_ids=[],
    )


def default_pool_state(count, *, source_unschedulable=False, retired=False):
    names = [OLD_A]
    if count >= 3 and not retired:
        names.append(OLD_B)
    if not retired:
        names.append(SOURCE)
    if count == 4:
        names.extend([FRESH_A] if SOURCE not in names and len(names) == 3 else [])
    instance_ids = []
    for name in names:
        instance_ids.append(name[-1])
    if count == 4 and retired:
        instance_ids = ["0", "1", "3"]
    return maintenance.workers.PoolState(
        role=ROLE,
        cluster_name=CLUSTER_NAME,
        resource_group=RESOURCE_GROUP,
        node_resource_group=NODE_RESOURCE_GROUP,
        pool_name="default",
        desired_count=count,
        pool_provisioning_state="Succeeded",
        pool_power_state="Running",
        vmss_name=VMSS,
        vmss_capacity=count,
        vmss_provisioning_state="Succeeded",
        instance_ids=sorted(instance_ids),
        failed_instance_ids=[],
        node_instance_ids=sorted(instance_ids),
        ready_instance_ids=sorted(instance_ids),
        unschedulable_nodes=[SOURCE] if source_unschedulable and not retired else [],
        stale_instance_ids=[],
    )


class Backend:
    """Stateful fake workflow backend."""

    def __init__(self, args, *, initial_pool_count):
        self.args = args
        self.initial_pool_count = initial_pool_count
        self.calls = []
        self.drain_timeout = 0
        self.retirement_timeout = 0
        self.retirement_validated = False
        self.clock = 0
        self.cluster_state_calls = 0
        self.pool_show_calls = 0
        self.scale_calls = 0
        self.retirement_calls = 0
        self.deleted_agents = []
        self.deleted_probes = []
        self.deleted_probe_errors = []
        self.cilium_mode = "healthy"
        self.final_cilium_unhealthy = False
        self.node_image_mismatch = False
        self.pool_provisioning = "Succeeded"
        self.scale_progress_reads = 1
        self.saw_scaling_state = False
        self.autoscaling = False
        self.scale_error = None
        self.change_pool_config_before_write = False
        self.vmss_updating_before_write = False
        self.patch_failure_node = None
        self.ambiguous_probe_uid = False
        self.probe_delete_error = False
        self.probe_growth = {FRESH_A: True, FRESH_B: True}
        self.pending_gap_name = None
        self.uid_change_name = None
        self.high_memory_before_healthy = False
        self.high_memory_before_pending = False
        self.high_memory_target_node = FRESH_A
        self.fresh_base_memory = None
        self.fresh_base_memory_during_healthy = None
        self.missing_metrics_before_healthy = False
        self.stale_metrics_before_healthy = False
        self.final_uid_drift = False
        self.uid_changed = False
        self.nnc_owner_invalid = False
        self.kwok_wait_reads = 0
        self.kwok_uid_change_on_wait = False
        self.healthy_agent_memory_bytes = 512 * 1024 * 1024
        self.fresh_uid_drift_before_moves = False
        self.fresh_nc_drift_before_moves = False
        self.require_unsupported_affinity = False
        self.drain_force_unknown = False
        self.fail_after_retirement = False
        self.defer_probe_cleanup = False
        self.current_phase = "initial"
        self.pending_done = False
        self.source_non_ds_present = True
        self.retired = False
        self.exclusion_token = ""
        self.fresh_nodes_added = []
        self.old_nodes = [OLD_A] if initial_pool_count == 2 else [OLD_A, OLD_B]
        self.nodes = {
            OLD_A: node(OLD_A, "00000000-0000-0000-0000-000000000000", 0),
            SOURCE: node(SOURCE, SOURCE_UID, 2),
        }
        if initial_pool_count == 3:
            self.nodes[OLD_B] = node(OLD_B, "11111111-1111-1111-1111-111111111111", 1)
        self.nnc = {
            OLD_A: {
                "uid": self.nodes[OLD_A]["metadata"]["uid"],
                "id": "cccccccc-cccc-cccc-cccc-cccccccccccc",
                "assigned": 48,
                "ips": [f"10.89.4.{index}" for index in range(1, 49)],
            },
            SOURCE: {
                "uid": SOURCE_UID,
                "id": SOURCE_NC_ID,
                "assigned": 16,
                "ips": [f"10.89.6.{index}" for index in range(1, 17)],
            },
        }
        if initial_pool_count == 3:
            self.nnc[OLD_B] = {
                "uid": self.nodes[OLD_B]["metadata"]["uid"],
                "id": "dddddddd-dddd-dddd-dddd-dddddddddddd",
                "assigned": 48,
                "ips": [f"10.89.5.{index}" for index in range(1, 49)],
            }
        self.agent_nodes = {}
        self.agent_uids = {}
        self.agent_ready = {}
        self.terminating = {}
        for index in range(100):
            name = f"kwok-node-{index}"
            if index < 2:
                host = SOURCE
                ready = False
            elif index < 18:
                host = SOURCE
                ready = True
            elif initial_pool_count == 2:
                host = OLD_A
                ready = True
            elif index < 59:
                host = OLD_A
                ready = True
            else:
                host = OLD_B
                ready = True
            self.agent_nodes[name] = host
            self.agent_uids[name] = f"pod-{index:03}"
            self.agent_ready[name] = ready
        self.probes = {}

    def summary_status(self):
        path = Path(self.args.summary_file)
        if not path.exists():
            return ""
        return json.loads(path.read_text(encoding="utf-8")).get("status", "")

    def monotonic(self):
        return self.clock

    def sleep(self, seconds):
        self.clock += max(1, int(seconds))

    def expected_new_nodes(self):
        return [FRESH_A] if self.initial_pool_count == 3 else [FRESH_A, FRESH_B]

    def build_pool(self):
        count = len(self.current_default_nodes())
        self.pool_show_calls += 1
        provisioning = self.pool_provisioning
        payload = pool_payload(count, provisioning=provisioning, autoscaling=self.autoscaling)
        if (
            self.change_pool_config_before_write
            and self.scale_calls == 0
            and self.pool_show_calls >= 2
        ):
            payload["vmSize"] = "Standard_D16_v3"
        if self.scale_calls > 0 and self.scale_progress_reads > 0:
            payload["provisioningState"] = "Scaling"
            self.scale_progress_reads -= 1
            self.saw_scaling_state = True
        return payload

    def current_default_nodes(self):
        if self.retired:
            return sorted([name for name in self.nodes if name != SOURCE])
        return sorted(self.nodes)

    def build_cluster_state(self):
        self.cluster_state_calls += 1
        count = len(self.current_default_nodes())
        state = maintenance.workers.ClusterState(
            ROLE,
            CLUSTER_NAME,
            RESOURCE_GROUP,
            [
                maintenance.workers.PoolState(
                    role=ROLE,
                    cluster_name=CLUSTER_NAME,
                    resource_group=RESOURCE_GROUP,
                    node_resource_group=NODE_RESOURCE_GROUP,
                    pool_name="default",
                    desired_count=count,
                    pool_provisioning_state="Succeeded",
                    pool_power_state="Running",
                    vmss_name=VMSS,
                    vmss_capacity=count,
                    vmss_provisioning_state="Succeeded",
                    instance_ids=sorted(name[-1] for name in self.current_default_nodes()),
                    failed_instance_ids=[],
                    node_instance_ids=sorted(name[-1] for name in self.current_default_nodes()),
                    ready_instance_ids=sorted(name[-1] for name in self.current_default_nodes()),
                    unschedulable_nodes=[SOURCE] if self.nodes.get(SOURCE, {}).get("spec", {}).get("unschedulable") else [],
                    stale_instance_ids=[],
                ),
                prom_pool_state(),
            ],
        )
        if self.vmss_updating_before_write and self.cluster_state_calls >= 2 and self.scale_calls == 0:
            state.pools[0].vmss_provisioning_state = "Updating"
        return state

    def build_nodes(self):
        rows = []
        for index in range(100):
            ready = True
            uid = f"kwok-uid-{index}"
            if index == 0 and self.kwok_wait_reads > 0:
                ready = False
                if self.kwok_uid_change_on_wait:
                    uid = "kwok-regenerated"
            rows.append(
                {
                    "metadata": {
                        "name": f"kwok-node-{index}",
                        "uid": uid,
                        "labels": {"type": "kwok"},
                    },
                    "spec": {},
                    "status": {
                        "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
                    },
                }
            )
        if self.kwok_wait_reads > 0 and self.summary_status() == "waiting-kwok-ready":
            self.kwok_wait_reads -= 1
        for name in sorted(self.nodes):
            row = copy.deepcopy(self.nodes[name])
            if self.node_image_mismatch and name == OLD_A:
                row["metadata"]["labels"]["kubernetes.azure.com/node-image-version"] = "wrong-image"
            if self.fresh_uid_drift_before_moves and name == FRESH_A and self.summary_status() in ("moving-pending", "moving-healthy"):
                row["metadata"]["uid"] = "fresh-uid-drift"
            rows.append(row)
        rows.append(
            {
                "metadata": {
                    "name": "aks-prompool-vmss000000",
                    "uid": "99999999-9999-9999-9999-999999999999",
                    "labels": {
                        "agentpool": "prompool",
                        "kubernetes.azure.com/agentpool": "prompool",
                        "kubernetes.azure.com/cluster": NODE_RESOURCE_GROUP,
                        "kubernetes.azure.com/node-image-version": NODE_IMAGE,
                    },
                    "annotations": {},
                },
                "spec": {
                    "providerID": (
                        f"azure:///subscriptions/{SUBSCRIPTION}/resourceGroups/{NODE_RESOURCE_GROUP}/"
                        "providers/Microsoft.Compute/virtualMachineScaleSets/aks-prompool-vmss/virtualMachines/0"
                    ),
                    "unschedulable": False,
                    "taints": [],
                },
                "status": {
                    "allocatable": {"cpu": "7820m", "memory": "30Gi", "pods": "110"},
                    "conditions": [{"type": "Ready", "status": "True"}],
                },
            }
        )
        return {"items": rows}

    def build_mock_pods(self, *, label_selector=None):
        items = []
        for name in sorted(self.agent_nodes):
            if self.pending_gap_name == name and self.current_phase == "pending-gap":
                continue
            deleting = self.terminating.get(name, False)
            items.append(
                mock_agent(
                    name,
                    self.agent_uids[name],
                    self.agent_nodes[name],
                    ready=self.agent_ready[name],
                    deleting=deleting,
                )
            )
        for pod in self.probes.values():
            items.append(copy.deepcopy(pod))
        if label_selector and label_selector.startswith(f"{maintenance.PROBE_LABEL_KEY}="):
            token = label_selector.split("=", 1)[1]
            return {
                "items": [
                    pod for pod in items
                    if (pod.get("metadata", {}).get("labels", {}) or {}).get(maintenance.PROBE_LABEL_KEY) == token
                ]
            }
        if label_selector and label_selector.startswith(f"{maintenance.PROBE_LABEL_KEY} in"):
            return {"items": [pod for pod in items if maintenance.PROBE_LABEL_KEY in (pod.get("metadata", {}).get("labels", {}) or {})]}
        return {"items": items}

    def build_all_pods(self):
        items = self.build_mock_pods()["items"]
        for node_name in sorted(self.nodes):
            items.append(daemonset_pod("cilium", node_name, "ds-cilium"))
            items.append(daemonset_pod("ama", node_name, "ds-ama"))
        if self.source_non_ds_present and SOURCE in self.nodes:
            items.append(cilium_operator_pod(SOURCE))
            items.append(kube_state_metrics_pod(SOURCE))
        return {"items": items}

    def build_events(self):
        return {
            "items": [
                event_for(name, self.agent_uids[name])
                for name in sorted(self.agent_nodes)
                if self.agent_nodes[name] == SOURCE and not self.agent_ready[name]
            ]
        }

    def build_nnc(self):
        items = []
        for name in sorted(self.nnc):
            info = self.nnc[name]
            node_uid = info["uid"]
            if self.nnc_owner_invalid and name == SOURCE:
                node_uid = "foreign-uid"
            nc_id = info["id"]
            if self.fresh_nc_drift_before_moves and name == FRESH_A and self.summary_status() in ("moving-pending", "moving-healthy"):
                nc_id = "nc-drift"
            items.append(
                nnc_payload(
                    name,
                    node_uid,
                    nc_id,
                    info["assigned"],
                    info["ips"],
                )
            )
        return {"items": items}

    def metrics_timestamp(self):
        if self.stale_metrics_before_healthy and self.summary_status() == "moving-healthy":
            return (NOW - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
        return NOW.strftime("%Y-%m-%dT%H:%M:%SZ")

    def build_metrics(self):
        items = []
        for node_name in sorted(self.nodes):
            if self.missing_metrics_before_healthy and self.summary_status() == "moving-healthy" and node_name in self.expected_new_nodes():
                continue
            memory = "6Gi"
            if self.fresh_base_memory is not None and node_name in self.expected_new_nodes():
                memory = self.fresh_base_memory
            if self.fresh_base_memory_during_healthy is not None and self.summary_status() == "moving-healthy" and node_name in self.expected_new_nodes():
                memory = self.fresh_base_memory_during_healthy
            if self.high_memory_before_pending and node_name == self.high_memory_target_node:
                memory = "29Gi"
            if self.high_memory_before_healthy and self.summary_status() == "moving-healthy" and node_name in self.expected_new_nodes():
                memory = "29Gi"
            elif node_name == SOURCE:
                memory = "10Gi"
            elif node_name in self.old_nodes:
                memory = "24Gi"
            items.append(
                {
                    "metadata": {"name": node_name},
                    "timestamp": self.metrics_timestamp(),
                    "usage": {"memory": memory},
                }
            )
        return {"items": items}

    def build_pod_metrics(self):
        items = []
        for name in sorted(self.agent_nodes):
            if not self.agent_ready[name]:
                continue
            items.append(
                {
                    "metadata": {"namespace": "mock-clustermesh", "name": name},
                    "timestamp": self.metrics_timestamp(),
                    "containers": [
                        {
                            "name": "mock-cilium-agent",
                            "usage": {
                                "memory": str(self.healthy_agent_memory_bytes),
                            },
                        }
                    ],
                }
            )
        return {"items": items}

    def current_controller_payload(self):
        payload = controller_payload()
        if self.require_unsupported_affinity:
            payload["spec"]["template"]["spec"]["affinity"] = {
                "podAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": [
                        {"topologyKey": "kubernetes.io/hostname"}
                    ]
                }
            }
        return payload

    def cilium_probe(self, **_kwargs):
        healthy = self.cilium_mode == "healthy"
        if self.retired and self.final_cilium_unhealthy:
            healthy = False
        return {
            "healthy": healthy,
            "agents": [
                {"node_name": node_name, "healthy": healthy}
                for node_name in sorted([*self.nodes, "aks-prompool-vmss000000"])
            ],
        }

    def add_fresh_nodes(self):
        for name in self.expected_new_nodes():
            if name in self.nodes:
                continue
            uid = f"{name[-1]*8}-{name[-1]*4}-{name[-1]*4}-{name[-1]*4}-{name[-1]*12}"
            instance_id = int(name[-1])
            self.nodes[name] = node(name, uid, instance_id)
            self.nnc[name] = {
                "uid": uid,
                "id": f"{name[-1]*8}-{name[-1]*4}-{name[-1]*4}-{name[-1]*4}-{name[-1]*12}",
                "assigned": 16,
                "ips": [f"10.89.{7 if name == FRESH_A else 8}.{index}" for index in range(1, 17)],
            }
            self.fresh_nodes_added.append(name)

    def delete_pod(self, _cluster, *, namespace, name, uid, **_kwargs):
        if name.startswith("cni-maint-probe"):
            if self.probe_delete_error:
                raise maintenance.mocks.RecoveryError(f"{name}: probe cleanup failed")
            pod = self.probes.get(name)
            if pod and pod["metadata"]["uid"] == uid:
                self.deleted_probes.append((name, uid))
                del self.probes[name]
            return
        assert namespace == "mock-clustermesh"
        assert self.agent_uids[name] == uid
        self.deleted_agents.append((name, uid))
        self.terminating[name] = False
        if self.uid_change_name == name:
            self.agent_uids[name] = f"{uid}-unexpected"
            return
        destination_cycle = self.expected_new_nodes()
        destination = destination_cycle[len(self.deleted_agents) % len(destination_cycle) - 1]
        self.agent_uids[name] = f"{uid}-replacement"
        self.agent_nodes[name] = destination
        self.agent_ready[name] = True
        if self.current_phase == "pending":
            self.pending_done = len(self.deleted_agents) >= 2

    def fake_retirement(self, passed_args, summary, _runner):
        self.retirement_calls += 1
        assert passed_args.summary_file.endswith(".retirement.json")
        assert (
            maintenance.RETIREMENT_MINIMUM_SECONDS
            <= passed_args.timeout_seconds
            <= maintenance.RETIREMENT_PHASE_BUDGET_SECONDS
        )
        self.retirement_timeout = passed_args.timeout_seconds
        retirement_state = self.build_cluster_state()
        maintenance.retirement.validate_pool_state(retirement_state, SOURCE, True)
        maintenance.retirement.validate_workloads(
            passed_args,
            self.build_nodes(),
            self.build_all_pods(),
            controller_payload(),
            daemonsets_payload(),
        )
        self.retirement_validated = True
        summary["success"] = True
        summary["status"] = "retired"
        self.retired = True
        self.source_non_ds_present = False
        if SOURCE in self.nodes:
            del self.nodes[SOURCE]
        if SOURCE in self.nnc:
            del self.nnc[SOURCE]
        if self.fail_after_retirement:
            # Leave a changed UID on a preserved agent for final qualification.
            self.agent_uids["kwok-node-90"] = "changed-after-retirement"

    def run(self, command, _timeout):
        command = list(command)
        self.calls.append(command)
        if command and command[0] == "kubectl":
            assert "--kubeconfig" in command
            assert "--context" in command
            assert command[command.index("--kubeconfig") + 1] == self.args.kubeconfig
            assert command[command.index("--context") + 1] == self.args.context
        if command[:3] == ["az", "account", "show"]:
            return json.dumps({"id": SUBSCRIPTION})
        if command and command[0] == "az" and command[1:3] != ["account", "show"]:
            assert "--subscription" in command
            assert command[command.index("--subscription") + 1] == SUBSCRIPTION
        if command[:3] == ["az", "group", "show"]:
            name = command[command.index("--name") + 1]
            if name == RESOURCE_GROUP:
                return json.dumps(scope_data(self.args)[0])
            return json.dumps(
                {
                    "managedBy": scope_data(self.args)[1][88]["id"],
                    "location": self.args.expected_region,
                    "tags": {"deletion_due_time": scope_data(self.args)[0]["tags"]["deletion_due_time"]},
                }
            )
        if command[:3] == ["az", "aks", "list"]:
            return json.dumps(scope_data(self.args)[1])
        if command[:4] == ["az", "fleet", "member", "list"]:
            return json.dumps(scope_data(self.args)[2])
        if command[:4] == ["az", "aks", "nodepool", "show"]:
            return json.dumps(self.build_pool())
        if command[:4] == ["az", "aks", "nodepool", "scale"]:
            self.scale_calls += 1
            if self.scale_error:
                raise maintenance.workers.ReconcileError(self.scale_error)
            self.add_fresh_nodes()
            return ""
        if command[0] != "kubectl":
            raise AssertionError(f"Unexpected command: {command}")
        if "--raw" in command:
            path = command[command.index("--raw") + 1]
            if path.endswith("/nodes"):
                return json.dumps(self.build_metrics())
            return json.dumps(self.build_pod_metrics())
        if "get" in command and "nodes" in command and "-o" in command:
            return json.dumps(self.build_nodes())
        if "get" in command and "events" in command:
            return json.dumps(self.build_events())
        if "get" in command and "statefulset" in command and "kwok-node" in command:
            return json.dumps(self.current_controller_payload())
        if "get" in command and "statefulsets" in command:
            return json.dumps(statefulsets_payload())
        if "get" in command and "replicasets" in command:
            return json.dumps(replicasets_payload())
        if "get" in command and "deployments" in command:
            return json.dumps(deployments_payload())
        if "get" in command and "daemonsets" in command:
            return json.dumps(daemonsets_payload())
        if "get" in command and "nnc" in command:
            return json.dumps(self.build_nnc())
        if "get" in command and "node" in command:
            name = command[command.index("node") + 1]
            return json.dumps(copy.deepcopy(self.nodes[name]))
        if "get" in command and "pods" in command:
            label_selector = None
            if "-l" in command:
                label_selector = command[command.index("-l") + 1]
            if "-A" in command:
                return json.dumps(self.build_all_pods())
            if (
                label_selector == "app=mock-cilium-agent"
                and self.uid_change_name
                and not self.uid_changed
                and self.summary_status() == "moving-pending"
            ):
                self.agent_uids[self.uid_change_name] = (
                    self.agent_uids[self.uid_change_name] + "-changed"
                )
                self.uid_changed = True
            return json.dumps(self.build_mock_pods(label_selector=label_selector))
        if "run" in command:
            name = command[command.index("run") + 1]
            assert "--command" not in command
            assert "--" not in command
            assert command[command.index("-o") + 1] == "json"
            overrides = json.loads(next(
                entry.split("=", 1)[1]
                for entry in command if entry.startswith("--overrides=")
            ))
            label_token = next(
                entry.split("=", 2)[2]
                for entry in command
                if entry.startswith(f"--labels={maintenance.PROBE_LABEL_KEY}=")
            )
            expected_node = next(
                row["node_name"]
                for row in json.loads(Path(self.args.summary_file).read_text(encoding="utf-8")).get("probe_cleanup_pending", [])
                if row["name"] == name
            )
            node_name = overrides["spec"]["nodeName"]
            assert node_name == expected_node
            assert overrides["spec"]["hostNetwork"] is False
            container = overrides["spec"]["containers"][0]
            assert container["name"] == name
            assert container["args"] == ["netexec", "--http-port=8080"]
            assert container["resources"]["requests"] == {
                "cpu": maintenance.PROBE_CPU_REQUEST,
                "memory": maintenance.PROBE_MEMORY_REQUEST,
            }
            uid = f"{name}-uid"
            ip_octet = 17 + len(self.probes)
            pod_ip = f"10.89.{7 if node_name == FRESH_A else 8}.{ip_octet}"
            self.probes[name] = {
                "metadata": {
                    "name": name,
                    "uid": uid,
                    "namespace": "mock-clustermesh",
                    "labels": {maintenance.PROBE_LABEL_KEY: label_token},
                },
                "spec": {"nodeName": node_name},
                "status": {
                    "phase": "Running",
                    "podIP": pod_ip,
                    "containerStatuses": [{"ready": True}],
                    "conditions": [{"type": "Ready", "status": "True"}],
                },
            }
            if self.probe_growth.get(node_name, False):
                self.nnc[node_name]["assigned"] = 32
                self.nnc[node_name]["ips"].append(pod_ip)
            result = copy.deepcopy(self.probes[name])
            if self.ambiguous_probe_uid:
                result["metadata"].pop("uid")
            return json.dumps(result)
        if "patch" in command and "node" in command:
            name = command[command.index("node") + 1]
            if self.patch_failure_node == name:
                raise maintenance.workers.ReconcileError(f"{name}: taint patch failed")
            patch = json.loads(command[command.index("-p") + 1])
            target = self.nodes[name]
            for operation in patch:
                path = operation["path"]
                if path == "/spec/unschedulable":
                    target["spec"]["unschedulable"] = operation["value"]
                elif path == "/spec/taints":
                    target["spec"]["taints"] = operation["value"]
                    for taint in operation["value"]:
                        if taint.get("key") == maintenance.EXCLUSION_KEY:
                            self.exclusion_token = taint["value"]
                elif path == "/metadata/annotations":
                    target["metadata"]["annotations"] = operation["value"]
                elif path.startswith("/spec/taints/") and operation["op"] == "remove":
                    del target["spec"]["taints"][int(path.split("/")[-1])]
            return ""
        if "drain" in command:
            assert "--delete-emptydir-data" in command
            assert "--force" not in command
            assert _timeout > self.args.request_timeout_seconds
            self.drain_timeout = _timeout
            self.source_non_ds_present = False
            return ""
        raise AssertionError(f"Unexpected kubectl command: {command}")


def install_backend(monkeypatch, backend):
    monkeypatch.setattr(maintenance.time, "monotonic", backend.monotonic)
    monkeypatch.setattr(maintenance.time, "sleep", backend.sleep)
    monkeypatch.setattr(maintenance.workers, "run_command", backend.run)
    monkeypatch.setattr(maintenance.workers, "probe_cluster", lambda *_args: backend.build_cluster_state())
    monkeypatch.setattr(maintenance.cilium, "probe", backend.cilium_probe)
    monkeypatch.setattr(maintenance.mocks, "delete_pod_with_uid_precondition", backend.delete_pod)
    monkeypatch.setattr(maintenance.retirement, "execute_retirement", backend.fake_retirement)


def make_backend(args, monkeypatch, *, initial_pool_count):
    backend = Backend(args, initial_pool_count=initial_pool_count)
    install_backend(monkeypatch, backend)
    return backend


def test_parse_args_validates_role_uuid_sha_and_bounds():
    with pytest.raises(SystemExit):
        maintenance.parse_args(
            [
                "--resource-group", RESOURCE_GROUP,
                "--confirm-resource-group", RESOURCE_GROUP,
                "--expected-subscription", SUBSCRIPTION,
                "--expected-region", "eastus2euap",
                "--expected-tfvars-sha", "BAD",
                "--role", "bad",
                "--node-name", SOURCE,
                "--node-uid", "bad",
                "--source-provider-id", "bad",
                "--source-network-container-id", "bad",
                "--kubeconfig", "/fake",
                "--summary-file", "summary.json",
            ]
        )


@pytest.mark.parametrize("initial_pool_count,expected_fresh", [(2, [FRESH_A, FRESH_B]), (3, [FRESH_A])])
def test_execute_successful_workflow_supports_initial_two_or_three(
    args, monkeypatch, initial_pool_count, expected_fresh
):
    backend = make_backend(args, monkeypatch, initial_pool_count=initial_pool_count)
    summary = {"success": False, "mutation_started": False}
    maintenance.execute_maintenance(args, summary, backend.run)
    saved = json.loads(Path(args.summary_file).read_text(encoding="utf-8"))
    assert saved["success"] is True
    assert saved["initial_pool_count"] == initial_pool_count
    assert saved["fresh_nodes"] == expected_fresh
    assert backend.scale_calls == 1
    assert backend.saw_scaling_state is True
    assert backend.retirement_calls == 1
    assert backend.retirement_validated is True
    assert backend.retirement_timeout >= maintenance.RETIREMENT_MINIMUM_SECONDS
    assert backend.drain_timeout > args.request_timeout_seconds
    assert SOURCE not in backend.nodes
    assert SOURCE not in backend.nnc
    assert saved["retirement"]["success"] is True
    assert saved["temporary_exclusions"] == []
    assert saved["probe_cleanup_pending"] == []
    assert any(
        call[:4] == ["az", "aks", "nodepool", "scale"]
        for call in backend.calls
    )


def test_mixed_node_images_abort_before_scale(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    backend.node_image_mismatch = True
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(maintenance.workers.ReconcileError, match="image-stable"):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert backend.scale_calls == 0


def test_changed_pool_configuration_since_initial_proof_aborts_before_scale(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    backend.change_pool_config_before_write = True
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(
        maintenance.workers.ReconcileError,
        match="changed since the initial proof",
    ):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert backend.scale_calls == 0


def test_inflight_pool_abort_before_scale(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    backend.pool_provisioning = "Updating"
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(maintenance.workers.ReconcileError, match="quiescent"):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert backend.scale_calls == 0


def test_vmss_updating_before_scale_aborts_before_write(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    backend.vmss_updating_before_write = True
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(
        maintenance.workers.ReconcileError,
        match="VMSS or Kubernetes worker state drifted",
    ):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert backend.scale_calls == 0


def test_nnc_owner_schema_is_required(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    backend.nnc_owner_invalid = True
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(maintenance.workers.ReconcileError, match="NodeNetworkConfig"):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert backend.scale_calls == 0


def test_second_fresh_node_ip_growth_failure_happens_before_healthy_deletes(
    args, monkeypatch
):
    backend = make_backend(args, monkeypatch, initial_pool_count=2)
    backend.probe_growth[FRESH_B] = False
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(maintenance.workers.ReconcileError, match="IP-batch growth"):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert len(backend.deleted_agents) == 0


def test_unfit_eligible_fresh_destination_fails_before_any_delete(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=2)
    backend.high_memory_before_pending = True
    backend.high_memory_target_node = FRESH_B
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(
        maintenance.workers.ReconcileError,
        match="each eligible fresh destination must have safe projected headroom",
    ):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert len(backend.deleted_agents) == 0


def test_ambiguous_probe_creation_is_reconciled_and_cleaned(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    backend.ambiguous_probe_uid = True
    summary = {"success": False, "mutation_started": False}
    maintenance.execute_maintenance(args, summary, backend.run)
    saved = json.loads(Path(args.summary_file).read_text(encoding="utf-8"))
    assert saved["success"] is True
    assert saved["probe_cleanup_pending"] == []
    assert backend.deleted_probes


def test_probe_cleanup_aggregates_mock_recovery_errors(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    backend.probe_delete_error = True
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(maintenance.workers.ReconcileError, match="probe cleanup failed"):
        maintenance.execute_maintenance(args, summary, backend.run)
    saved = json.loads(Path(args.summary_file).read_text(encoding="utf-8"))
    assert saved["success"] is False
    assert saved["probe_cleanup_pending"]


def test_probe_cleanup_read_failure_keeps_ownership_ledger(args):
    class ForbiddenOperator:
        def kubectl_json(self, *_args, **_kwargs):
            raise maintenance.workers.ReconcileError("Forbidden probe inventory")

    summary = {"probe_cleanup_pending": [{
        "name": "owned-probe", "uid": "original-uid",
        "node_name": FRESH_A, "token": "owned-token",
    }]}
    errors = maintenance._cleanup_probe_pods(ForbiddenOperator(), args, None, summary)
    assert errors == ["Forbidden probe inventory"]
    assert summary["probe_cleanup_pending"][0]["uid"] == "original-uid"


def test_probe_uid_resolution_never_adopts_same_name_replacement():
    summary = {"probe_cleanup_pending": [{
        "name": "owned-probe", "uid": "original-uid",
        "node_name": FRESH_A, "token": "owned-token",
    }]}
    errors = maintenance._resolve_probe_uids(summary, {"items": [{
        "metadata": {
            "name": "owned-probe", "uid": "replacement-uid",
            "labels": {maintenance.PROBE_LABEL_KEY: "owned-token"},
        },
        "spec": {"nodeName": FRESH_A},
    }]})
    assert errors
    assert summary["probe_cleanup_pending"][0]["uid"] == "original-uid"
    assert summary["probe_cleanup_pending"][0]["unexpected_replacement_uid"] == "replacement-uid"


def test_healthy_source_without_cni_evidence_cannot_authorize_replacement(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=2)
    backend.agent_ready.update({name: True for name in backend.agent_ready})
    with pytest.raises(maintenance.workers.ReconcileError, match="UID-proven CNI-broken source"):
        maintenance.execute_maintenance(args, {"success": False}, backend.run)
    assert backend.scale_calls == 0
    assert not backend.deleted_agents


def test_second_taint_patch_failure_is_cleaned_from_first_node(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    backend.patch_failure_node = OLD_B
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(maintenance.workers.ReconcileError, match="taint patch failed"):
        maintenance.execute_maintenance(args, summary, backend.run)
    saved = json.loads(Path(args.summary_file).read_text(encoding="utf-8"))
    assert saved["success"] is False
    assert not any(
        isinstance(taint, dict) and taint.get("key") == maintenance.EXCLUSION_KEY
        for taint in backend.nodes[OLD_A]["spec"]["taints"]
    )
    assert saved["temporary_exclusions"]


def test_pending_phase_allows_one_inflight_gap(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    original_delete = backend.delete_pod

    def delete_with_gap(*delete_args, **delete_kwargs):
        backend.current_phase = "pending-gap"
        backend.pending_gap_name = "kwok-node-0"
        original_delete(*delete_args, **delete_kwargs)
        backend.pending_gap_name = None
        backend.current_phase = "pending"

    monkeypatch.setattr(maintenance.mocks, "delete_pod_with_uid_precondition", delete_with_gap)
    summary = {"success": False, "mutation_started": False}
    maintenance.execute_maintenance(args, summary, backend.run)
    saved = json.loads(Path(args.summary_file).read_text(encoding="utf-8"))
    assert saved["success"] is True


def test_kwok_ready_wait_allows_delayed_recovery_without_uid_regeneration(
    args, monkeypatch
):
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    backend.kwok_wait_reads = 2
    summary = {"success": False, "mutation_started": False}
    maintenance.execute_maintenance(args, summary, backend.run)
    assert backend.clock >= 2
    saved = json.loads(Path(args.summary_file).read_text(encoding="utf-8"))
    assert saved["success"] is True


def test_kwok_ready_wait_rejects_identity_regeneration(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    backend.kwok_wait_reads = 1
    backend.kwok_uid_change_on_wait = True
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(
        maintenance.workers.ReconcileError,
        match="identity changed",
    ):
        maintenance.execute_maintenance(args, summary, backend.run)


def test_uid_change_abort_happens_before_later_unproved_delete(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    backend.uid_change_name = "kwok-node-0"
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(maintenance.workers.ReconcileError, match="UID changed"):
        maintenance.execute_maintenance(args, summary, backend.run)


@pytest.mark.parametrize("mode,expected", [("missing", "metrics"), ("stale", "stale"), ("high", "headroom")])
def test_memory_checks_stop_before_first_healthy_delete(args, monkeypatch, mode, expected):
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    if mode == "missing":
        backend.missing_metrics_before_healthy = True
    elif mode == "stale":
        backend.stale_metrics_before_healthy = True
    else:
        backend.high_memory_before_healthy = True
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(maintenance.workers.ReconcileError, match=expected):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert len(backend.deleted_agents) == 2


def test_memory_reservation_blocks_next_healthy_delete_before_metrics_catch_up(
    args, monkeypatch
):
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    backend.healthy_agent_memory_bytes = 3 * 1024 * 1024 * 1024
    backend.fresh_base_memory_during_healthy = "21Gi"
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(
        maintenance.workers.ReconcileError,
        match="safe projected headroom before the next delete",
    ):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert len(backend.deleted_agents) == 3


def test_actual_pod_template_rules_are_enforced_for_destinations(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    backend.require_unsupported_affinity = True
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(
        maintenance.workers.ReconcileError,
        match="unsupported",
    ):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert len(backend.deleted_agents) == 0


@pytest.mark.parametrize(
    "field,expected",
    [("uid", "UID changed"), ("nc", "network container changed")],
)
def test_fresh_identity_must_not_change_after_ip_proof(
    args, monkeypatch, field, expected
):
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    if field == "uid":
        backend.fresh_uid_drift_before_moves = True
    else:
        backend.fresh_nc_drift_before_moves = True
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(maintenance.workers.ReconcileError, match=expected):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert len(backend.deleted_agents) == 0


def test_final_failure_after_retirement_keeps_overall_success_false(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    backend.fail_after_retirement = True
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(maintenance.workers.ReconcileError, match="preserved mock-agent UID changed"):
        maintenance.execute_maintenance(args, summary, backend.run)
    saved = json.loads(Path(args.summary_file).read_text(encoding="utf-8"))
    assert saved["success"] is False
    assert saved["retirement"]["success"] is True


def test_small_budget_blocks_healthy_mutation_before_retirement(args, monkeypatch):
    args.timeout_seconds = 320
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(
        maintenance.workers.ReconcileError,
        match="retirement and final qualification",
    ):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert backend.retirement_calls == 0
    assert len(backend.deleted_agents) == 2


def test_main_dry_run_reports_planned_success(args, monkeypatch, capsys):
    args.execute = False
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    original_execute = maintenance.execute_maintenance
    monkeypatch.setattr(
        maintenance,
        "execute_maintenance",
        lambda parsed_args, summary: original_execute(parsed_args, summary, backend.run),
    )
    result = maintenance.main(
        [
            "--resource-group", RESOURCE_GROUP,
            "--confirm-resource-group", RESOURCE_GROUP,
            "--expected-subscription", SUBSCRIPTION,
            "--expected-region", "eastus2euap",
            "--expected-tfvars-sha", "a" * 64,
            "--role", ROLE,
            "--node-name", SOURCE,
            "--node-uid", SOURCE_UID,
            "--source-provider-id", SOURCE_PROVIDER,
            "--source-network-container-id", SOURCE_NC_ID,
            "--kubeconfig", args.kubeconfig,
            "--context", args.context,
            "--summary-file", args.summary_file,
        ]
    )
    assert result == 0
    assert "plan validated" in capsys.readouterr().out
    saved = json.loads(Path(args.summary_file).read_text(encoding="utf-8"))
    assert saved["status"] == "planned"
    assert saved["success"] is True


def test_main_catches_mock_recovery_errors(args, monkeypatch):
    make_backend(args, monkeypatch, initial_pool_count=3)
    monkeypatch.setattr(
        maintenance,
        "execute_maintenance",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            maintenance.mocks.RecoveryError("cleanup exploded")
        ),
    )
    result = maintenance.main(
        [
            "--resource-group", RESOURCE_GROUP,
            "--confirm-resource-group", RESOURCE_GROUP,
            "--expected-subscription", SUBSCRIPTION,
            "--expected-region", "eastus2euap",
            "--expected-tfvars-sha", "a" * 64,
            "--role", ROLE,
            "--node-name", SOURCE,
            "--node-uid", SOURCE_UID,
            "--source-provider-id", SOURCE_PROVIDER,
            "--source-network-container-id", SOURCE_NC_ID,
            "--kubeconfig", args.kubeconfig,
            "--context", args.context,
            "--summary-file", args.summary_file,
        ]
    )
    assert result == 1
    saved = json.loads(Path(args.summary_file).read_text(encoding="utf-8"))
    assert saved["success"] is False
    assert saved["error"] == "cleanup exploded"
