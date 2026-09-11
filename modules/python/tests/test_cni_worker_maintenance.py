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
            "uid": f"{name}-{node_name}-pod-uid",
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
                elif path.startswith("/metadata/annotations/"):
                    key = path[len("/metadata/annotations/"):].replace("~1", "/").replace("~0", "~")
                    if operation["op"] == "test":
                        assert target["metadata"]["annotations"][key] == operation["value"]
                    elif operation["op"] == "remove":
                        del target["metadata"]["annotations"][key]
                    else:
                        target["metadata"]["annotations"][key] = operation["value"]
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


def prepare_resume(args, monkeypatch, *, initial_pool_count=2, legacy=False):
    """Capture a genuine fake pre-operation manifest and accepted-surge failure."""
    backend = make_backend(args, monkeypatch, initial_pool_count=initial_pool_count)
    original = backend.build_nodes()
    manifest = {
        "schema_version": 1,
        "source_build_id": 79797,
        "resource_group": RESOURCE_GROUP,
        "role": ROLE,
        "source_worker": SOURCE,
        "source_worker_uid": SOURCE_UID,
        "original_real_node_uids": {
            name: row["metadata"]["uid"]
            for name, row in maintenance._real_node_map(original).items()
        },
        "original_kwok_node_uids": {
            name: row["metadata"]["uid"]
            for name, row in maintenance._kwok_map(original).items()
        },
        "agent_uids": dict(backend.agent_uids),
        "controller_uid": "controller-uid",
    }
    prior = {}
    with monkeypatch.context() as patch:
        def fail_after_accepted_surge(*_args):
            raise maintenance.workers.ReconcileError("Fresh default workers are not all Ready and schedulable")
        patch.setattr(maintenance, "_wait_for_fresh_nodes", fail_after_accepted_surge)
        with pytest.raises(maintenance.workers.ReconcileError, match="Fresh default"):
            maintenance.execute_maintenance(args, prior, backend.run)
    assert prior["status"] == "waiting-for-surge"
    assert prior["surge_request_accepted"] is True
    assert backend.scale_calls == 1
    assert not backend.deleted_agents
    prior["error"] = "Fresh default workers are not all Ready and schedulable"
    if legacy:
        for key in (
            "original_real_node_uids", "original_real_node_identities",
            "original_kwok_node_uids", "agent_uids", "controller_uid",
            "expected_subscription", "expected_region", "expected_tfvars_sha",
        ):
            prior.pop(key)
    manifest["fresh_node_uids"] = {
        name: backend.nodes[name]["metadata"]["uid"]
        for name in backend.expected_new_nodes()
    }
    manifest["fresh_network_container_ids"] = {
        name: backend.nnc[name]["id"] for name in backend.expected_new_nodes()
    }
    args.resume_build_id = 79797
    args.resume_summary = str(Path(args.summary_file).with_name("prior-maintenance.json"))
    args.resume_manifest = str(Path(args.summary_file).with_name("resume-manifest.json"))
    Path(args.resume_summary).write_text(json.dumps(prior), encoding="utf-8")
    Path(args.resume_manifest).write_text(json.dumps(manifest), encoding="utf-8")
    backend.calls.clear()
    backend.scale_calls = 0
    backend.scale_progress_reads = 0
    return backend, prior, manifest


def assert_no_mutations(backend):
    assert backend.scale_calls == backend.retirement_calls == 0
    assert not backend.deleted_agents and not backend.probes
    assert not any(
        token in command for command in backend.calls
        for token in ("scale", "update", "patch", "drain", "run", "delete", "apply")
    )


def prepare_empty_host_recovery(args, monkeypatch):
    backend, prior, manifest = prepare_resume(args, monkeypatch)
    prior["status"] = "proving-fresh-ip-growth"
    prior["fresh_nodes"] = list(manifest["fresh_node_uids"])
    prior["fresh_ip_growth"] = {
        name: {
            "node_uid": manifest["fresh_node_uids"][name],
            "network_container_id": manifest["fresh_network_container_ids"][name],
            "initial_assigned": 16,
            **({"after_assigned": 32, "ready_probe_ips": ["10.89.7.17", "10.89.7.18"]}
               if name == FRESH_A else {}),
        }
        for name in manifest["fresh_node_uids"]
    }
    Path(args.resume_summary).write_text(json.dumps(prior), encoding="utf-8")
    args.recover_empty_fresh_node = FRESH_B
    args.recover_empty_fresh_uid = manifest["fresh_node_uids"][FRESH_B]
    backend.nodes[FRESH_B]["status"]["nodeInfo"] = {"bootID": "boot-before"}
    redeploys = []

    def runner(command, timeout):
        if command[:2] == ["az", "rest"]:
            assert command[command.index("--method") + 1] == "post"
            assert command[command.index("--url") + 1] == (
                f"https://management.azure.com/subscriptions/{SUBSCRIPTION}"
                f"/resourceGroups/{NODE_RESOURCE_GROUP}/providers/Microsoft.Compute"
                f"/virtualMachineScaleSets/{VMSS}/virtualMachines/4/redeploy?api-version=2026-04-01"
            )
            assert command[command.index("--subscription") + 1] == SUBSCRIPTION
            redeploys.append(list(command))
            backend.nodes[FRESH_B]["status"]["nodeInfo"]["bootID"] = "boot-after"
            return ""
        if command[:3] == ["az", "vmss", "get-instance-view"]:
            assert command[command.index("--instance-id") + 1] == "4"
            return json.dumps({"statuses": [
                {"code": "ProvisioningState/succeeded"}, {"code": "PowerState/running"},
            ]})
        return backend.run(command, timeout)

    return backend, prior, manifest, runner, redeploys


@pytest.mark.parametrize("execute", [False, True])
def test_empty_host_recovery_is_explicit_and_never_scales(args, monkeypatch, execute):
    backend, _, _, runner, redeploys = prepare_empty_host_recovery(args, monkeypatch)
    args.execute = execute
    summary = {}
    maintenance.execute_maintenance(args, summary, runner)
    assert summary["success"] is True
    assert backend.scale_calls == 0
    assert len(redeploys) == int(execute)
    if execute:
        assert summary["empty_host_recovery"]["success"] is True
        assert summary["empty_host_recovery"]["cordon_retained"] is False
        assert summary["empty_host_recovery"]["current_boot_id"] == "boot-after"
        assert backend.retirement_calls == 1
    else:
        assert_no_mutations(backend)


@pytest.mark.parametrize("drift", [
    "original-worker", "qualified-worker", "target-uid", "missing-persisted-uids",
    "non-daemonset", "pvc", "dirty-probes", "workload-moved",
])
def test_empty_host_recovery_refuses_unqualified_scope(args, monkeypatch, drift):
    backend, prior, manifest, runner, redeploys = prepare_empty_host_recovery(args, monkeypatch)
    if drift == "original-worker":
        args.recover_empty_fresh_node = SOURCE
        args.recover_empty_fresh_uid = SOURCE_UID
    elif drift == "qualified-worker":
        args.recover_empty_fresh_node = FRESH_A
        args.recover_empty_fresh_uid = manifest["fresh_node_uids"][FRESH_A]
    elif drift == "target-uid":
        args.recover_empty_fresh_uid = SOURCE_UID
    elif drift == "missing-persisted-uids":
        prior.pop("agent_uids")
    elif drift == "dirty-probes":
        prior["probe_cleanup_pending"] = [{"uid": "leftover"}]
    elif drift == "workload-moved":
        prior["pending_moves"] = [{"name": "kwok-node-0"}]
    else:
        original = backend.build_all_pods

        def unsafe_pods():
            payload = original()
            if drift == "non-daemonset":
                payload["items"].append(cilium_operator_pod(FRESH_B))
            else:
                for pod in payload["items"]:
                    if pod.get("spec", {}).get("nodeName") == FRESH_B:
                        pod["spec"]["volumes"] = [{"persistentVolumeClaim": {"claimName": "unsafe"}}]
            return payload

        monkeypatch.setattr(backend, "build_all_pods", unsafe_pods)
    Path(args.resume_summary).write_text(json.dumps(prior), encoding="utf-8")
    with pytest.raises(maintenance.workers.ReconcileError):
        maintenance.execute_maintenance(args, {}, runner)
    assert not redeploys
    assert_no_mutations(backend)


def test_empty_host_recovery_never_retries_rejected_request(args, monkeypatch):
    backend, _, _, runner, _ = prepare_empty_host_recovery(args, monkeypatch)
    attempts = []

    def denied(command, timeout):
        if command[:2] == ["az", "rest"]:
            attempts.append(command)
            raise maintenance.workers.ReconcileError("AuthorizationFailed")
        return runner(command, timeout)

    summary = {}
    with pytest.raises(maintenance.workers.ReconcileError, match="AuthorizationFailed"):
        maintenance.execute_maintenance(args, summary, denied)
    assert len(attempts) == 1 and not backend.deleted_agents
    assert backend.scale_calls == 0
    assert summary["empty_host_recovery"]["cordon_retained"] is True
    assert summary["success"] is False


def test_empty_host_recovery_requires_actual_new_boot(args, monkeypatch):
    backend, _, _, runner, redeploys = prepare_empty_host_recovery(args, monkeypatch)
    monkeypatch.setattr(maintenance, "EMPTY_HOST_RECOVERY_SECONDS", 2)

    def unchanged_boot(command, timeout):
        result = runner(command, timeout)
        if command[:2] == ["az", "rest"]:
            backend.nodes[FRESH_B]["status"]["nodeInfo"]["bootID"] = "boot-before"
        return result

    summary = {}
    with pytest.raises(maintenance.workers.ReconcileError, match="bounded observation"):
        maintenance.execute_maintenance(args, summary, unchanged_boot)
    assert len(redeploys) == 1 and not backend.deleted_agents
    assert summary["success"] is False


def test_empty_host_recovery_refuses_busy_instance_before_cordon(args, monkeypatch):
    backend, _, _, runner, redeploys = prepare_empty_host_recovery(args, monkeypatch)

    def busy(command, timeout):
        if command[:3] == ["az", "vmss", "get-instance-view"]:
            return json.dumps({"statuses": [
                {"code": "ProvisioningState/updating"}, {"code": "PowerState/running"},
            ]})
        return runner(command, timeout)

    with pytest.raises(maintenance.workers.ReconcileError, match="quiescent"):
        maintenance.execute_maintenance(args, {}, busy)
    assert not redeploys
    assert_no_mutations(backend)


def test_empty_host_recovery_requires_real_ip_growth_after_redeploy(args, monkeypatch):
    backend, _, _, runner, redeploys = prepare_empty_host_recovery(args, monkeypatch)
    backend.probe_growth[FRESH_B] = False
    monkeypatch.setattr(maintenance, "IP_GROWTH_WAIT_SECONDS", 3)
    summary = {}
    with pytest.raises(maintenance.workers.ReconcileError):
        maintenance.execute_maintenance(args, summary, runner)
    assert len(redeploys) == 1 and backend.scale_calls == 0
    assert not backend.deleted_agents
    assert summary["empty_host_recovery"]["redeploy_completed"] is True
    assert summary["empty_host_recovery"]["success"] is False
    assert summary["empty_host_recovery"]["cordon_retained"] is True
    assert backend.nodes[FRESH_B]["spec"]["unschedulable"] is True
    assert summary["success"] is False and not summary["probe_cleanup_pending"]


@pytest.mark.parametrize("legacy", [False, True])
def test_resume_plan_is_read_only_with_original_hold(args, monkeypatch, legacy):
    backend, _, _ = prepare_resume(args, monkeypatch, legacy=legacy)
    source_before = copy.deepcopy(backend.nodes[SOURCE])
    args.execute = False
    summary = {}
    maintenance.execute_maintenance(args, summary, backend.run)
    assert summary["status"] == "planned" and summary["success"] is True
    assert summary["mutation_started"] is False
    assert summary["resume_provenance"]["source_build_id"] == 79797
    assert len(summary["original_kwok_node_uids"]) == len(summary["agent_uids"]) == 100
    assert len(summary["cilium_before"]["covered_node_names"]) == 5
    assert source_before == backend.nodes[SOURCE]
    assert_no_mutations(backend)


@pytest.mark.parametrize("execute", [False, True])
def test_resume_accepts_timestamped_controller_cordon(args, monkeypatch, execute):
    backend, _, _ = prepare_resume(args, monkeypatch, legacy=True)
    backend.nodes[SOURCE]["spec"]["taints"].append({
        "key": "node.kubernetes.io/unschedulable",
        "effect": "NoSchedule",
        "timeAdded": "2026-09-11T13:27:33Z",
    })
    source_before = copy.deepcopy(backend.nodes[SOURCE])
    args.execute = execute
    summary = {}
    maintenance.execute_maintenance(args, summary, backend.run)
    assert summary["success"] is True
    assert backend.scale_calls == 0
    if not execute:
        assert backend.nodes[SOURCE] == source_before
        assert_no_mutations(backend)


def test_normal_startup_accepts_timestamped_controller_cordon(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=2)
    original_nodes = backend.build_nodes

    def timestamped_nodes():
        payload = original_nodes()
        for row in payload["items"]:
            if row["metadata"]["name"] == SOURCE and row["spec"].get("unschedulable"):
                row["spec"]["taints"].append({
                    "key": "node.kubernetes.io/unschedulable",
                    "effect": "NoSchedule",
                    "timeAdded": "2026-09-11T13:27:33Z",
                })
        return payload

    monkeypatch.setattr(backend, "build_nodes", timestamped_nodes)
    summary = {}
    maintenance.execute_maintenance(args, summary, backend.run)
    assert summary["success"] is True and backend.scale_calls == 1


@pytest.mark.parametrize("initial_pool_count,legacy", [(2, True), (2, False), (3, False)])
def test_resume_executes_existing_surge_without_scale_or_hold(
    args, monkeypatch, initial_pool_count, legacy
):
    backend, _, manifest = prepare_resume(
        args, monkeypatch, initial_pool_count=initial_pool_count, legacy=legacy,
    )
    summary = {}
    maintenance.execute_maintenance(args, summary, backend.run)
    assert summary["success"] is True and summary["status"] == "completed"
    assert summary["initial_pool_count"] == initial_pool_count
    assert backend.scale_calls == 0 and backend.retirement_calls == 1
    assert not any("scale" in command or "update" in command for command in backend.calls)
    source_patches = [command for command in backend.calls if "patch" in command and SOURCE in command]
    assert source_patches == []
    assert SOURCE not in backend.nodes and SOURCE not in backend.nnc
    assert len(backend.nodes) == 3 and all(backend.agent_ready.values())
    assert summary["probe_cleanup_pending"] == summary["temporary_exclusions"] == summary["cleanup_errors"] == []
    assert len(summary["cilium_after_surge"]["covered_node_names"]) == 5
    assert len(summary["cilium_final"]["covered_node_names"]) == 4
    for name, uid in manifest["fresh_node_uids"].items():
        growth = summary["fresh_ip_growth"][name]
        assert growth["node_uid"] == uid
        assert growth["network_container_id"] == manifest["fresh_network_container_ids"][name]
        assert growth["initial_assigned"] == 16
        assert backend.nnc[name]["assigned"] == 32
    assert summary["pending_moved_count"] == 2 and summary["healthy_moved_count"] == 16


@pytest.mark.parametrize("field", [
    "resource_group", "role", "source_worker", "source_worker_uid",
    "schema_version", "source_build_id", "original_real_node_uids",
    "original_kwok_node_uids", "agent_uids", "controller_uid",
    "fresh_node_uids", "fresh_network_container_ids",
])
def test_resume_refuses_missing_manifest_fields_before_mutation(args, monkeypatch, field):
    backend, _, manifest = prepare_resume(args, monkeypatch, legacy=True)
    manifest.pop(field)
    Path(args.resume_manifest).write_text(json.dumps(manifest), encoding="utf-8")
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(maintenance.workers.ReconcileError):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert_no_mutations(backend)
    assert not summary["success"] and not summary["mutation_started"]


@pytest.mark.parametrize("field,value", [
    ("scope", "foreign"), ("resource_group", "foreign"), ("role", "mesh-88"),
    ("source_worker", OLD_A), ("source_worker_uid", "foreign"),
    ("source_provider_id", SOURCE_PROVIDER + "9"), ("source_network_container_id", "foreign"),
    ("source_build_id", 1), ("initial_pool_count", 4), ("initial_worker_state", {}),
    ("initial_pool_configuration", {"vmSize": "Standard_D32_v3"}),
    ("execute", False), ("success", True), ("mutation_started", False),
    ("source_quarantined", False), ("surge_request_accepted", False),
    ("status", "moving-pending"), ("pending_moves", [{"name": "kwok-node-0"}]),
    ("healthy_moves", [{"name": "kwok-node-3"}]), ("pending_move_intents", ["intent"]),
    ("pending_moved_count", 1), ("healthy_moved_count", 1),
    ("probe_cleanup_pending", [{"name": "probe"}]), ("probe_intents", ["intent"]),
    ("fresh_ip_growth", {"node": {"initial_assigned": 16}}),
    ("retirement", {"request_accepted": True}), ("source_pre_drain", ["pod"]),
    ("temporary_exclusions", [{"name": OLD_A}]), ("cleanup_errors", ["unclean"]),
    ("initial_pending_source_agents", ["kwok-node-1"]),
    ("initial_healthy_source_agents", []),
    ("pod_template", {"containers": []}),
])
def test_resume_rejects_prior_scope_phase_or_mutation(args, monkeypatch, field, value):
    backend, prior, _ = prepare_resume(args, monkeypatch, legacy=True)
    prior[field] = value
    Path(args.resume_summary).write_text(json.dumps(prior), encoding="utf-8")
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(maintenance.workers.ReconcileError):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert_no_mutations(backend)
    assert summary["success"] is False and summary["mutation_started"] is False


@pytest.mark.parametrize("field,key", [
    ("original_real_node_uids", OLD_A), ("original_real_node_uids", "aks-prompool-vmss000000"),
    ("fresh_node_uids", FRESH_A), ("fresh_network_container_ids", FRESH_B),
    ("agent_uids", "kwok-node-90"), ("original_kwok_node_uids", "kwok-node-90"),
])
def test_resume_rejects_manifest_identity_drift(args, monkeypatch, field, key):
    backend, _, manifest = prepare_resume(args, monkeypatch, legacy=True)
    manifest[field][key] = "changed-identity"
    Path(args.resume_manifest).write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(maintenance.workers.ReconcileError):
        maintenance.execute_maintenance(args, {}, backend.run)
    assert_no_mutations(backend)


@pytest.mark.parametrize("drift", [
    "build", "controller", "agent", "kwok", "source-hold", "source-taint",
    "foreign-taint", "fresh-nc", "fresh-owner", "source-nc", "vmss", "original-provider",
    "extra-node", "missing-node", "probe", "events", "config",
])
def test_resume_rejects_fresh_live_drift(args, monkeypatch, drift):
    backend, _, manifest = prepare_resume(args, monkeypatch, legacy=True)
    if drift == "build":
        args.resume_build_id += 1
    elif drift == "controller":
        manifest["controller_uid"] = "changed-controller"
        Path(args.resume_manifest).write_text(json.dumps(manifest), encoding="utf-8")
    elif drift == "agent":
        backend.agent_uids["kwok-node-90"] = "changed-agent"
    elif drift == "kwok":
        backend.kwok_wait_reads = 1
        backend.kwok_uid_change_on_wait = True
    elif drift == "source-hold":
        backend.nodes[SOURCE]["metadata"]["annotations"][maintenance.HOLD_ANNOTATION] += " foreign"
    elif drift in ("source-taint", "foreign-taint"):
        backend.nodes[SOURCE if drift == "source-taint" else OLD_A]["spec"]["taints"].append(
            {"key": maintenance.EXCLUSION_KEY, "value": "foreign", "effect": "NoSchedule"}
        )
    elif drift == "fresh-nc":
        backend.nnc[FRESH_A]["id"] = "changed"
    elif drift == "fresh-owner":
        backend.nnc[FRESH_A]["uid"] = "changed"
    elif drift == "source-nc":
        backend.nnc[SOURCE]["id"] = "changed"
    elif drift == "vmss":
        backend.vmss_updating_before_write = True
    elif drift == "original-provider":
        backend.nodes[OLD_A]["spec"]["providerID"] += "9"
    elif drift == "extra-node":
        backend.nodes[OLD_B] = node(OLD_B, "unexpected-uid", 1)
    elif drift == "missing-node":
        del backend.nodes[FRESH_A]
    elif drift == "probe":
        original_pods = backend.build_all_pods
        monkeypatch.setattr(backend, "build_all_pods", lambda: {
            "items": original_pods()["items"] + [{
                "metadata": {"name": "cni-maint-probe-leftover", "labels": {maintenance.PROBE_LABEL_KEY: "old"}}
            }],
        })
    elif drift == "events":
        monkeypatch.setattr(backend, "build_events", lambda: {"items": []})
    elif drift == "config":
        backend.change_pool_config_before_write = True
    with pytest.raises(maintenance.workers.ReconcileError):
        maintenance.execute_maintenance(args, {}, backend.run)
    assert_no_mutations(backend)


@pytest.mark.parametrize("options", [
    {"resume_build_id": 79797}, {"resume_summary": "prior.json"},
    {"resume_manifest": "manifest.json"},
    {"resume_build_id": 79797, "resume_summary": "prior.json"},
    {"resume_build_id": -1, "resume_summary": "prior.json", "resume_manifest": "manifest.json"},
])
def test_resume_options_require_all_three_positive_build(args, options):
    for key, value in options.items():
        setattr(args, key, value)
    with pytest.raises(maintenance.workers.ReconcileError, match="all three"):
        maintenance.execute_maintenance(args, {}, lambda *_: pytest.fail("No reads allowed"))


def test_resume_refuses_overwriting_original_summary(args, monkeypatch):
    backend, _, _ = prepare_resume(args, monkeypatch)
    args.summary_file = args.resume_summary
    before = Path(args.resume_summary).read_bytes()
    with pytest.raises(maintenance.workers.ReconcileError, match="distinct files"):
        maintenance.execute_maintenance(args, {}, backend.run)
    assert Path(args.resume_summary).read_bytes() == before
    assert_no_mutations(backend)


@pytest.mark.parametrize("failure", ["retirement", "cleanup"])
def test_resumed_retirement_or_cleanup_failure_is_not_success(args, monkeypatch, failure):
    backend, _, _ = prepare_resume(args, monkeypatch)
    if failure == "retirement":
        def failed_retirement(_args, summary, _runner):
            summary["request_accepted"] = True
            summary["error"] = "Retirement did not converge"
            raise maintenance.workers.ReconcileError(summary["error"])
        monkeypatch.setattr(maintenance.retirement, "execute_retirement", failed_retirement)
    else:
        backend.probe_delete_error = True
    summary = {}
    with pytest.raises(maintenance.workers.ReconcileError):
        maintenance.execute_maintenance(args, summary, backend.run)
    saved = json.loads(Path(args.summary_file).read_text(encoding="utf-8"))
    assert saved["success"] is False and backend.scale_calls == 0
    assert SOURCE in backend.nodes and saved["source_quarantined"] is True
    if failure == "retirement":
        assert saved["retirement"]["request_accepted"] is True
    else:
        assert saved["probe_cleanup_pending"] and saved["cleanup_errors"]


def delayed_fresh_nodes(backend, monkeypatch, *, ready_after=3, drift=None):
    original_nodes = backend.build_nodes
    reads = {"count": 0}

    def observations():
        payload = original_nodes()
        if backend.scale_calls and not backend.retired:
            reads["count"] += 1
            if reads["count"] <= ready_after:
                for row in payload["items"]:
                    if row["metadata"]["name"] == FRESH_A:
                        row["status"]["conditions"][0]["status"] = "False"
            if drift and reads["count"] == 2:
                for row in payload["items"]:
                    if row["metadata"]["name"] == (FRESH_A if drift == "fresh-uid" else OLD_A):
                        row["metadata"]["uid"] = "drifted-uid"
        return payload

    monkeypatch.setattr(backend, "build_nodes", observations)
    return reads


def test_accepted_surge_waits_read_only_for_new_worker_ready(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=2)
    reads = delayed_fresh_nodes(backend, monkeypatch)
    summary = {}
    maintenance.execute_maintenance(args, summary, backend.run)
    assert summary["success"] is True and backend.scale_calls == 1
    assert reads["count"] > 3 and backend.clock >= 3


@pytest.mark.parametrize("drift", ["fresh-uid", "original-uid"])
def test_startup_does_not_retry_structural_uid_drift(args, monkeypatch, drift):
    backend = make_backend(args, monkeypatch, initial_pool_count=2)
    delayed_fresh_nodes(backend, monkeypatch, ready_after=20, drift=drift)
    summary = {}
    with pytest.raises(maintenance.workers.ReconcileError, match="identity.*drifted"):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert backend.scale_calls == 1 and backend.clock < 10
    assert not backend.deleted_agents and not backend.probes
    assert summary["source_quarantined"] is True and summary["success"] is False


def test_startup_readiness_deadline_is_bounded_with_retirement_reserve(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=2)
    delayed_fresh_nodes(backend, monkeypatch, ready_after=10000)
    summary = {}
    with pytest.raises(maintenance.workers.ReconcileError, match="deadline expired"):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert backend.scale_calls == 1 and backend.retirement_calls == 0
    assert backend.clock <= maintenance.SURGE_READY_WAIT_SECONDS + 1
    assert not backend.deleted_agents and not backend.probes
    assert summary["success"] is False and summary["status"] == "waiting-for-surge"


def test_surge_waits_for_registration_with_only_new_stale_instances(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=2)
    original_nodes, original_state = backend.build_nodes, backend.build_cluster_state
    observations = {"missing": 3}

    def missing_registration():
        payload = original_nodes()
        if backend.scale_calls and observations["missing"]:
            observations["missing"] -= 1
            payload["items"] = [row for row in payload["items"] if row["metadata"]["name"] != FRESH_A]
        return payload

    def pending_state():
        state = original_state()
        if backend.scale_calls and observations["missing"]:
            state.pools[0].node_instance_ids.remove("3")
            state.pools[0].ready_instance_ids.remove("3")
            state.pools[0].stale_instance_ids = ["3"]
        return state

    monkeypatch.setattr(backend, "build_nodes", missing_registration)
    monkeypatch.setattr(backend, "build_cluster_state", pending_state)
    summary = {}
    maintenance.execute_maintenance(args, summary, backend.run)
    assert summary["success"] is True and backend.scale_calls == 1
    assert observations["missing"] == 0 and backend.clock >= 3


@pytest.mark.parametrize("drift", ["count", "config", "taint", "source-not-ready", "fresh-image", "provider"])
def test_startup_structural_drift_is_fatal_without_retry(args, monkeypatch, drift):
    backend = make_backend(args, monkeypatch, initial_pool_count=2)
    original_pool, original_nodes = backend.build_pool, backend.build_nodes

    def changed_pool():
        payload = original_pool()
        if backend.scale_calls and backend.saw_scaling_state and payload["provisioningState"] == "Succeeded":
            if drift == "count":
                payload["count"] = 5
            if drift == "config":
                payload["vmSize"] = "changed-size"
        return payload

    def changed_nodes():
        payload = original_nodes()
        if backend.scale_calls:
            mapping = maintenance._real_node_map(payload)
            if drift == "taint":
                mapping[FRESH_A]["spec"]["taints"] = [{"key": "unexpected", "effect": "NoSchedule"}]
            elif drift == "source-not-ready":
                mapping[SOURCE]["status"]["conditions"][0]["status"] = "False"
            elif drift == "fresh-image":
                mapping[FRESH_A]["metadata"]["labels"]["kubernetes.azure.com/node-image-version"] = "changed-image"
            elif drift == "provider":
                mapping[OLD_A]["spec"]["providerID"] += "9"
        return payload

    monkeypatch.setattr(backend, "build_pool", changed_pool)
    monkeypatch.setattr(backend, "build_nodes", changed_nodes)
    summary = {}
    with pytest.raises(maintenance.workers.ReconcileError):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert backend.scale_calls == 1 and backend.clock <= 1
    assert not backend.deleted_agents and not backend.probes


def test_normal_four_worker_state_never_implies_resume(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=2)
    backend.add_fresh_nodes()
    with pytest.raises(maintenance.workers.ReconcileError, match="exactly 2 or 3"):
        maintenance.execute_maintenance(args, {}, backend.run)
    assert_no_mutations(backend)


def test_resume_plan_requires_every_real_cilium_agent(args, monkeypatch):
    backend, _, _ = prepare_resume(args, monkeypatch)
    args.execute = False
    original_proof = backend.cilium_probe

    def missing_agent(**kwargs):
        proof = original_proof(**kwargs)
        proof["agents"] = [agent for agent in proof["agents"] if agent["node_name"] != FRESH_B]
        return proof

    monkeypatch.setattr(maintenance.cilium, "probe", missing_agent)
    summary = {}
    with pytest.raises(maintenance.workers.ReconcileError, match="99-peer"):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert summary["success"] is False and summary["mutation_started"] is False
    assert_no_mutations(backend)


@pytest.mark.parametrize("drift", ["lease", "fleet", "node-group", "unknown-source-pod"])
def test_resume_requires_fresh_scope_and_unchanged_drain_allowlist(args, monkeypatch, drift):
    backend, _, _ = prepare_resume(args, monkeypatch)
    original_run = backend.run

    def unsafe_observation(command, timeout):
        result = original_run(command, timeout)
        if drift == "lease" and command[:3] == ["az", "group", "show"]:
            payload = json.loads(result)
            payload["tags"]["deletion_due_time"] = (NOW - timedelta(hours=1)).isoformat()
            return json.dumps(payload)
        if drift == "fleet" and command[:4] == ["az", "fleet", "member", "list"]:
            return json.dumps(json.loads(result)[:-1])
        if drift == "node-group" and command[:3] == ["az", "group", "show"] and NODE_RESOURCE_GROUP in command:
            payload = json.loads(result)
            payload["managedBy"] = "foreign-cluster"
            return json.dumps(payload)
        if drift == "unknown-source-pod" and command[0] == "kubectl" and "pods" in command and "-A" in command:
            payload = json.loads(result)
            unknown = cilium_operator_pod(SOURCE)
            unknown["metadata"]["namespace"] = "mock-clustermesh"
            unknown["metadata"]["name"] = "unknown-owner-pod"
            payload["items"].append(unknown)
            return json.dumps(payload)
        return result

    args.execute = False
    with pytest.raises(maintenance.workers.ReconcileError):
        maintenance.execute_maintenance(args, {}, unsafe_observation)
    assert_no_mutations(backend)


@pytest.mark.parametrize("payload", ['{"schema_version":1,"schema_version":1}', "{invalid", "[]"])
def test_resume_refuses_malformed_or_ambiguous_artifacts(args, monkeypatch, payload):
    backend, _, _ = prepare_resume(args, monkeypatch)
    Path(args.resume_manifest).write_text(payload, encoding="utf-8")
    with pytest.raises(maintenance.workers.ReconcileError):
        maintenance.execute_maintenance(args, {}, backend.run)
    assert_no_mutations(backend)


def test_resume_preserves_persisted_identity_manifest_crosscheck(args, monkeypatch):
    backend, prior, _ = prepare_resume(args, monkeypatch)
    prior["agent_uids"]["kwok-node-90"] = "different-pre-operation-pod"
    Path(args.resume_summary).write_text(json.dumps(prior), encoding="utf-8")
    with pytest.raises(maintenance.workers.ReconcileError, match="persisted agent_uids"):
        maintenance.execute_maintenance(args, {}, backend.run)
    assert_no_mutations(backend)


def test_resume_rejects_false_retirement_result(args, monkeypatch):
    backend, _, _ = prepare_resume(args, monkeypatch)
    monkeypatch.setattr(maintenance.retirement, "execute_retirement", lambda *_: None)
    summary = {}
    with pytest.raises(maintenance.workers.ReconcileError, match="retirement did not succeed"):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert summary["success"] is False and summary["retirement"]["success"] is False
    assert backend.scale_calls == 0


def test_resume_cli_defaults_and_exact_flags(args):
    argv = []
    for key in (
        "resource_group", "confirm_resource_group", "expected_subscription",
        "expected_region", "expected_tfvars_sha", "role", "node_name", "node_uid",
        "source_provider_id", "source_network_container_id", "kubeconfig", "summary_file",
    ):
        argv.extend(["--" + key.replace("_", "-"), str(getattr(args, key))])
    parsed = maintenance.parse_args(argv)
    assert parsed.resume_build_id == 0
    assert parsed.resume_summary == parsed.resume_manifest == ""
    assert parsed.execute is False
    parsed = maintenance.parse_args(argv + [
        "--resume-build-id", "79797", "--resume-summary", "prior.json",
        "--resume-manifest", "manifest.json",
    ])
    assert parsed.resume_build_id == 79797 and parsed.execute is False
    for extra in (
        ["--resume-build-id", "79797"],
        ["--resume-build-id", "0", "--resume-summary", "prior.json", "--resume-manifest", "manifest.json"],
        ["--resume-build-id", "79797", "--resume-summary", args.summary_file, "--resume-manifest", "manifest.json"],
    ):
        with pytest.raises(SystemExit):
            maintenance.parse_args(argv + extra)


def test_fresh_daemonsets_ready_before_post_surge_peer_proof(args, monkeypatch):
    backend = make_backend(args, monkeypatch, initial_pool_count=2)
    original_pods = backend.build_all_pods
    original_proof = backend.cilium_probe
    observations = {"pending": 3}

    def system_pods():
        payload = original_pods()
        if backend.scale_calls and observations["pending"]:
            observations["pending"] -= 1
            for pod in payload["items"]:
                if pod.get("spec", {}).get("nodeName") == FRESH_A and pod["metadata"].get("namespace") == "kube-system":
                    pod["status"]["conditions"][0]["status"] = "False"
                    pod["status"]["containerStatuses"][0]["ready"] = False
        return payload

    def peer_proof(**kwargs):
        assert kwargs["expected_remote_count"] == 99
        assert len(kwargs["expected_remote_names"]) == 99
        if backend.scale_calls:
            assert observations["pending"] == 0
        return original_proof(**kwargs)

    monkeypatch.setattr(backend, "build_all_pods", system_pods)
    monkeypatch.setattr(maintenance.cilium, "probe", peer_proof)
    summary = {}
    maintenance.execute_maintenance(args, summary, backend.run)
    assert summary["success"] is True and backend.scale_calls == 1


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


def test_small_budget_blocks_pod_mutation_preserving_retirement(args, monkeypatch):
    args.timeout_seconds = 320
    backend = make_backend(args, monkeypatch, initial_pool_count=3)
    summary = {"success": False, "mutation_started": False}
    with pytest.raises(
        maintenance.workers.ReconcileError,
        match="retirement and final qualification",
    ):
        maintenance.execute_maintenance(args, summary, backend.run)
    assert backend.retirement_calls == 0
    assert not backend.deleted_agents


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
