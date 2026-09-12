"""Offline stateful tests for the one approved failed-host native replacement."""

# pylint: disable=protected-access,too-many-lines

from __future__ import annotations

import copy
import importlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from . import test_unreachable_prom_worker_recovery as base


recovery = base.recovery
sys.path.insert(0, str(base.MODULE_DIR))
try:
    replacement = importlib.import_module("failed_prom_worker_replacement")
finally:
    sys.path.pop(0)

NEW_INSTANCE = "35"
NEW_NODE = f"{recovery.PROM_VMSS}00000z"
NEW_VM_ID = base.uid("genuine-replacement-vm")
NEW_NODE_UID = base.uid("genuine-replacement-node")
NEW_NC = base.uid("genuine-replacement-nc")
IMAGE = "AKSUbuntu-2204gen2containerd-2026.08.17"


class ReplacementCloud(base.FakeCloud):
    """Only the two scoped native mutations are supported; no real client runs."""

    def __init__(self, plan, args):
        super().__init__(plan, args)
        self.on_native_delete = None
        self.on_scale = None
        self.native_delete_error = None
        self.scale_error = None
        self.native_actions = []
        self.old_system_pods = []
        self.new_name = NEW_NODE

    def azure(self, command):
        native = command[1:4]
        if native not in (["aks", "nodepool", "delete-machines"], ["aks", "nodepool", "scale"]):
            assert command[1:3] not in (["vmss", "restart"], ["vmss", "reimage"])
            return super().azure(command)
        assert self.value(command, "--subscription") == recovery.SUBSCRIPTION
        assert self.value(command, "--resource-group") == recovery.RESOURCE_GROUP
        assert self.value(command, "--cluster-name") == recovery.CLUSTER
        assert self.value(command, "--name") == "prompool" and "--no-wait" in command
        self.writes.append(command)
        receipt = base.receipt()["replacement"]
        if native[-1] == "delete-machines":
            assert self.value(command, "--machine-names") == recovery.PROM_NODE
            assert not self.native_actions
            self.native_actions.append("delete")
            request = receipt["delete"]
            assert request["attempted"] and request["accepted"] is None and request["ambiguous"]
            assert request["requested_at"]
            host = self.nodes[recovery.PROM_NODE]
            assert host["spec"]["unschedulable"]
            assert recovery.MARKER_KEY in host["metadata"]["annotations"]
            assert replacement.REPLACEMENT_KEY in host["metadata"]["annotations"]
            self.old_system_pods = copy.deepcopy([
                row for row in self.pods if row["spec"].get("nodeName") == recovery.PROM_NODE
                and row["metadata"]["ownerReferences"][0]["kind"] == "DaemonSet"
            ])
            if self.native_delete_error:
                raise recovery.workers.ReconcileError(self.native_delete_error)
            if self.on_native_delete:
                self.on_native_delete()
            else:
                self.finish_removal()
        else:
            assert self.value(command, "--node-count") == "1"
            assert self.native_actions == ["delete"]
            assert self.pools[1]["count"] == 0 and self.vmsses[1]["sku"]["capacity"] == 0
            assert not self.instances[recovery.PROM_VMSS] and recovery.PROM_NODE not in self.nodes
            assert not any(row["spec"].get("nodeName") == recovery.PROM_NODE for row in self.pods)
            assert not any(row["metadata"]["name"] == recovery.PROM_NODE for row in self.nncs)
            assert receipt["native_removal"]["old_node_pods_nnc_absent"]
            self.native_actions.append("scale")
            request = receipt["restore"]
            assert request["attempted"] and request["accepted"] is None and request["ambiguous"]
            assert request["requested_at"]
            if self.scale_error:
                raise recovery.workers.ReconcileError(self.scale_error)
            if self.on_scale:
                self.on_scale()
            else:
                self.finish_restoration()
        return ""

    def finish_removal(self):
        self.pools[1].update(count=0, provisioningState="Succeeded")
        self.vmsses[1]["sku"]["capacity"] = 0
        self.vmsses[1]["provisioningState"] = "Succeeded"
        self.instances[recovery.PROM_VMSS] = []
        self.scale_view = {"statuses": [{"code": "ProvisioningState/succeeded"}], "virtualMachines": []}
        self.nodes.pop(recovery.PROM_NODE, None)
        self.pods = [row for row in self.pods if row["spec"].get("nodeName") != recovery.PROM_NODE]
        self.nncs = [row for row in self.nncs if row["metadata"]["name"] != recovery.PROM_NODE]

    def finish_restoration(self):
        self.pools[1].update(count=1, provisioningState="Succeeded")
        self.vmsses[1]["sku"]["capacity"] = 1
        self.vmsses[1]["provisioningState"] = "Succeeded"
        vmss_id = self.vmsses[1]["id"]
        self.instances[recovery.PROM_VMSS] = [{
            "id": f"{vmss_id}/virtualMachines/{NEW_INSTANCE}", "name": f"{recovery.PROM_VMSS}_{NEW_INSTANCE}",
            "computerName": self.new_name, "instanceId": NEW_INSTANCE,
            "latestModelApplied": True, "provisioningState": "Succeeded", "vmId": NEW_VM_ID,
        }]
        self.views[(recovery.PROM_VMSS, NEW_INSTANCE)] = {
            "statuses": [{"code": "ProvisioningState/succeeded"}, {"code": "PowerState/running"}],
            "extensions": [{"name": "vmssCSE", "statuses": [{"code": "ProvisioningState/succeeded"}]}],
        }
        self.scale_view = {
            "statuses": [{"code": "ProvisioningState/succeeded"}],
            "virtualMachines": [{"code": "ProvisioningState/succeeded", "count": 1}],
        }
        node = base.make_node(self.new_name, NEW_NODE_UID, pool="prompool", instance=NEW_INSTANCE)
        node["metadata"]["labels"]["kubernetes.azure.com/node-image-version"] = IMAGE
        self.nodes[self.new_name] = node
        self.nncs.append({
            "metadata": {
                **base.metadata(self.new_name, "kube-system", base.uid("new-nnc-object")),
                "ownerReferences": [base.reference("Node", self.new_name, NEW_NODE_UID)],
            },
            "spec": {"requestedIPCount": 64},
            "status": {"assignedIPCount": 64, "networkContainers": [{
                "id": NEW_NC, "version": 1,
                "ipAssignments": [{"ip": f"10.96.1.{index}"} for index in range(1, 65)],
            }]},
        })
        created = set()
        for original in self.old_system_pods:
            owner = original["metadata"]["ownerReferences"][0]
            if owner["uid"] in created:
                continue
            created.add(owner["uid"])
            pod = copy.deepcopy(original)
            name = f"{owner['name']}-new-prom"
            pod["metadata"].update(name=name, uid=base.uid(name), resourceVersion="1")
            pod["metadata"].pop("deletionTimestamp", None)
            pod["spec"]["nodeName"] = self.new_name
            container_name = pod["status"]["containerStatuses"][0]["name"]
            pod["status"] = base.ready_status(True)
            pod["status"]["containerStatuses"][0]["name"] = container_name
            self.pods.append(pod)

    def kubernetes(self, command):
        result = super().kubernetes(command)
        if "/apis/metrics.k8s.io/v1beta1/nodes" in command and self.new_name in self.nodes:
            for row in result["items"]:
                row["metadata"]["name"] = self.new_name
        return result

    def delete(self, cluster, **kwargs):
        name = kwargs["name"]
        if not name.startswith("prom-recovery-ip-"):
            assert self.native_actions == ["delete", "scale"]
            assert base.receipt()["replacement_derived_identity"]["node_name"] == self.new_name
            assert base.receipt()["replacement"]["replacement_completed"]
        callback = self.delete_callback
        self.delete_callback = None
        try:
            super().delete(cluster, **kwargs)
        finally:
            self.delete_callback = callback
        if not name.startswith("prom-recovery-ip-"):
            pod = self.get_pod(f"{name}-replacement")
            pod["spec"]["nodeName"] = self.new_name
            if callback:
                callback(None, pod)


def save_checkpoint(args, checkpoint):
    Path(args.replace_failed_host).write_text(json.dumps(checkpoint), encoding="utf-8")


@pytest.fixture(name="environment")
def replacement_environment(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    plan = base.make_plan()
    args = SimpleNamespace(
        resource_group=recovery.RESOURCE_GROUP, confirm_resource_group=recovery.RESOURCE_GROUP,
        expected_subscription=recovery.SUBSCRIPTION, expected_region=recovery.REGION,
        expected_tfvars_sha="a" * 64, plan_file="plan.json", summary_file="summary.json",
        timeout_seconds=3600, execute=False,
    )
    fake = ReplacementCloud(plan, args)
    env = args, plan, fake
    base.select_dns(env)
    base.add_framework(env, "kube-state-metrics")
    base.add_framework(env, "grafana")
    for pool in fake.pools:
        pool.update(mode="User" if pool["name"] == "prompool" else "System", nodeImageVersion=IMAGE)
    for name in recovery.REAL_UIDS:
        fake.nodes[name]["metadata"]["labels"]["kubernetes.azure.com/node-image-version"] = IMAGE
    original_nnc = next(row for row in fake.nncs if row["metadata"]["name"] == recovery.PROM_NODE)
    original_nnc["status"]["networkContainers"][0]["id"] = replacement.FAILED_NETWORK_CONTAINER
    host_pods = [row for row in fake.pods if row["spec"].get("nodeName") == recovery.PROM_NODE]
    while len(host_pods) < replacement.ORIGINAL_HOST_PODS:
        pod = copy.deepcopy(host_pods[0])
        name = f"old-terminating-system-{len(host_pods)}"
        pod["metadata"].update(name=name, uid=base.uid(name), deletionTimestamp=base.now())
        fake.pods.append(pod)
        host_pods.append(pod)
    checkpoint = base.accepted_observation(env, healthy=False)
    args.replace_failed_host = args.observe_accepted_action
    args.observe_accepted_action = None
    args.reimage_failed_os = False
    checkpoint["arm_metadata"]["pools"] = {
        row["name"]: {"configuration_sha256": recovery.digest(recovery.prepared.pool_configuration(row))}
        for row in fake.pools
    }
    for vm in fake.instances[recovery.DEFAULT_VMSS]:
        checkpoint["arm_metadata"]["instances"][vm["computerName"]] = {
            "id": vm["id"], "instance_id": vm["instanceId"], "vm_id": vm["vmId"],
        }
    failed_at = (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat()
    fake.vmsses[1]["provisioningState"] = "Failed"
    fake.instances[recovery.PROM_VMSS][0]["provisioningState"] = "Failed"
    fake.views[(recovery.PROM_VMSS, "0")] = {
        "statuses": [{"code": replacement.FAILURE_CODE, "time": failed_at}, {"code": "PowerState/running"}],
        "extensions": [{"name": "vmssCSE", "statuses": None}],
    }
    fake.scale_view = {
        "statuses": [{"code": replacement.FAILURE_CODE, "time": failed_at}], "virtualMachines": [],
    }
    save_checkpoint(args, checkpoint)
    Path(args.plan_file).write_text(json.dumps(plan), encoding="utf-8")
    return env


def run(environment, *, execute=False):
    return base.run(environment, execute=execute)


def failed_receipt():
    summary = base.receipt()
    assert summary["status"] == "failed" and not summary["success"] and not summary["repaired"]
    assert not summary["restart"]["attempted"]
    assert summary["restart"]["accepted"] is False and summary["restart"]["ambiguous"] is False
    return summary


def assert_no_writes(fake):
    assert not fake.writes and not fake.deleted and not fake.native_actions
    assert not base.receipt()["mutation_started"]


def test_default_plan_is_zero_write_and_keeps_original_identity(environment):
    _, plan, fake = environment
    original_plan = copy.deepcopy(plan)
    original_marker = fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"][recovery.MARKER_KEY]
    summary = run(environment)
    assert summary["plan_valid"] and summary["status"] == "plan_valid"
    assert summary["planned_actions"]["pool_counts"] == [1, 0, 1]
    assert summary["planned_actions"]["machine_names"] == [recovery.PROM_NODE]
    assert [row["deployment_name"] for row in summary["planned_actions"]["pods"]] == [
        "coredns", "coredns", "clustermesh-apiserver", "kube-state-metrics", "grafana",
    ]
    assert all(row["decision"] == "delete-pinned" for row in summary["planned_actions"]["pods"])
    assert summary["original_identity"]["network_container_id"] == replacement.FAILED_NETWORK_CONTAINER
    assert not summary["replacement"]["delete"]["attempted"] and not summary["replacement"]["restore"]["attempted"]
    assert not summary["repaired"] and not summary["workloads_ready"] and summary["phase1_only"]
    assert not summary["restart"]["attempted"]
    assert plan == original_plan
    assert fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"][recovery.MARKER_KEY] == original_marker
    assert_no_writes(fake)


@pytest.mark.parametrize("fault", [
    "prom-system", "default-user", "prom-zero", "prom-two", "default-one", "autoscale", "pool-busy",
    "pool-image", "vm-id", "vm-provider", "vm-not-latest", "vm-power", "vm-generic-failure",
    "vm-old-code", "vm-before-action", "vm-recent", "vm-future", "vm-missing-time",
    "scale-code", "scale-recent", "scale-before-action", "guest-failed", "marker-missing",
    "marker-token", "prior-replacement-marker", "old-boot", "old-node-uid", "old-network",
    "old-network-owner", "old-pod-addition", "old-pod-ready", "old-pod-ready-unknown",
    "old-pod-pvc", "old-pod-ephemeral", "old-pod-unowned", "default-vm-id", "default-vm-failed",
    "default-vm-extension", "default-node-uid", "kwok-uid", "kwok-not-ready", "mock-uid",
    "mock-regression", "mock-on-host", "controller-uid", "dns-count", "dns-sibling",
    "framework-pvc", "unpinned-framework", "unknown-node", "missing-vmss", "wrong-fleet",
    "lease", "aks-busy",
])
def test_initial_qualification_faults_have_zero_writes(environment, fault):
    _, plan, fake = environment
    host = fake.nodes[recovery.PROM_NODE]
    vm = fake.instances[recovery.PROM_VMSS][0]
    view = fake.views[(recovery.PROM_VMSS, "0")]
    old_pod = next(row for row in fake.pods if row["spec"].get("nodeName") == recovery.PROM_NODE)
    nnc = next(row for row in fake.nncs if row["metadata"]["name"] == recovery.PROM_NODE)
    if fault == "prom-system":
        fake.pools[1]["mode"] = "System"
    elif fault == "default-user":
        fake.pools[0]["mode"] = "User"
    elif fault in ("prom-zero", "prom-two", "default-one"):
        fake.pools[0 if fault == "default-one" else 1]["count"] = {
            "prom-zero": 0, "prom-two": 2, "default-one": 1,
        }[fault]
    elif fault == "autoscale":
        fake.pools[1]["enableAutoScaling"] = True
    elif fault == "pool-busy":
        fake.pools[1]["provisioningState"] = "Scaling"
    elif fault == "pool-image":
        fake.pools[1]["nodeImageVersion"] = "other-image"
    elif fault == "vm-id":
        vm["vmId"] = base.uid("wrong-original-vm")
    elif fault == "vm-provider":
        vm["id"] = vm["id"].replace("/virtualMachines/0", "/virtualMachines/1")
    elif fault == "vm-not-latest":
        vm["latestModelApplied"] = False
    elif fault == "vm-power":
        view["statuses"][1]["code"] = "PowerState/stopped"
    elif fault == "vm-generic-failure":
        view["statuses"][0]["code"] = "ProvisioningState/failed"
    elif fault == "vm-old-code":
        view["statuses"][0]["code"] = recovery.OS_FAILURE_CODE
    elif fault in ("vm-before-action", "vm-recent", "vm-future", "vm-missing-time"):
        view["statuses"][0]["time"] = {
            "vm-before-action": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
            "vm-recent": base.now(), "vm-future": (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat(),
            "vm-missing-time": None,
        }[fault]
    elif fault == "scale-code":
        fake.scale_view["statuses"][0]["code"] = "ProvisioningState/failed/OtherError"
    elif fault == "scale-recent":
        fake.scale_view["statuses"][0]["time"] = base.now()
    elif fault == "scale-before-action":
        fake.scale_view["statuses"][0]["time"] = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    elif fault == "guest-failed":
        view["extensions"][0]["statuses"] = [{"code": "ProvisioningState/failed"}]
    elif fault == "marker-missing":
        del host["metadata"]["annotations"][recovery.MARKER_KEY]
    elif fault == "marker-token":
        host["metadata"]["annotations"][recovery.MARKER_KEY] = "{}"
    elif fault == "prior-replacement-marker":
        host["metadata"]["annotations"][replacement.REPLACEMENT_KEY] = "{}"
    elif fault == "old-boot":
        host["status"]["nodeInfo"]["bootID"] = base.uid("host-rebooted")
    elif fault == "old-node-uid":
        host["metadata"]["uid"] = base.uid("not-original")
    elif fault == "old-network":
        nnc["status"]["networkContainers"][0]["id"] = base.uid("different-old-nc")
    elif fault == "old-network-owner":
        nnc["metadata"]["ownerReferences"][0]["name"] = recovery.SOURCE_NODE
    elif fault == "old-pod-addition":
        extra = copy.deepcopy(old_pod)
        extra["metadata"].update(name="additional-host-pod", uid=base.uid("additional-host-pod"))
        fake.pods.append(extra)
    elif fault == "old-pod-ready":
        old_pod["status"]["conditions"][0]["status"] = "True"
    elif fault == "old-pod-ready-unknown":
        old_pod["status"]["conditions"][0]["status"] = "Unknown"
    elif fault in ("old-pod-pvc", "old-pod-ephemeral"):
        old_pod["spec"]["volumes"] = [{
            "name": "unsafe", "persistentVolumeClaim" if fault == "old-pod-pvc" else "ephemeral": {},
        }]
    elif fault == "old-pod-unowned":
        old_pod["metadata"]["ownerReferences"] = []
    elif fault == "default-vm-id":
        fake.instances[recovery.DEFAULT_VMSS][0]["vmId"] = base.uid("different-default")
    elif fault == "default-vm-failed":
        fake.instances[recovery.DEFAULT_VMSS][0]["provisioningState"] = "Failed"
    elif fault == "default-vm-extension":
        fake.views[(recovery.DEFAULT_VMSS, "0")]["extensions"][0]["statuses"] = None
    elif fault == "default-node-uid":
        fake.nodes[recovery.SOURCE_NODE]["metadata"]["uid"] = base.uid("changed-default-node")
    elif fault == "kwok-uid":
        fake.nodes["kwok-node-99"]["metadata"]["uid"] = base.uid("changed-kwok")
    elif fault == "kwok-not-ready":
        fake.nodes["kwok-node-99"]["status"]["conditions"][0]["status"] = "False"
    elif fault == "mock-uid":
        fake.get_pod("kwok-node-99")["metadata"]["uid"] = base.uid("changed-mock")
    elif fault == "mock-regression":
        fake.get_pod("kwok-node-0")["status"] = base.ready_status(False)
    elif fault == "mock-on-host":
        fake.get_pod("kwok-node-99")["spec"]["nodeName"] = recovery.PROM_NODE
    elif fault == "controller-uid":
        fake.get_controller("kwok-node", "StatefulSet")["metadata"]["uid"] = base.uid("changed-controller")
    elif fault == "dns-count":
        fake.get_controller("coredns")["spec"]["replicas"] = 4
    elif fault == "dns-sibling":
        fake.get_pod(f"{recovery.DNS_REPLICA_SET}-healthy-2")["status"] = base.ready_status(False)
    elif fault == "framework-pvc":
        fake.get_pod(plan["api_pod_name"])["spec"]["volumes"] = [{"persistentVolumeClaim": {}}]
    elif fault == "unpinned-framework":
        fake.get_pod(plan["api_pod_name"])["metadata"]["uid"] = base.uid("unpinned-pending")
    elif fault == "unknown-node":
        fake.nodes["foreign"] = base.make_node("foreign", base.uid("foreign"))
    elif fault == "missing-vmss":
        fake.vmsses.pop()
    elif fault == "wrong-fleet":
        fake.members[0]["meshProperties"]["status"] = {"state": "Failed", "error": {"code": "ConnectivityTimeout"}}
    elif fault == "lease":
        fake.group["tags"]["deletion_due_time"] = base.now()
    else:
        fake.operation["status"] = "InProgress"
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    failed_receipt()
    assert_no_writes(fake)


@pytest.mark.parametrize("statuses", [None, []])
def test_uninitialized_guest_extensions_are_failed_candidates_not_healthy(environment, statuses):
    _, _, fake = environment
    fake.views[(recovery.PROM_VMSS, "0")]["extensions"][0]["statuses"] = statuses
    summary = run(environment)
    assert not summary["replacement"]["replacement_completed"] and not summary["repaired"]
    assert_no_writes(fake)


def test_single_native_delete_zero_scale_and_inherited_framework_path(environment, monkeypatch):
    args, plan, fake = environment
    pinned_plan = copy.deepcopy(plan)
    original_nodes = copy.deepcopy({name: node for name, node in fake.nodes.items() if name != recovery.PROM_NODE})
    original_mock_uids = {row["metadata"]["name"]: row["metadata"]["uid"] for row in fake.pods
                          if row["metadata"].get("namespace") == "mock-clustermesh"}

    def forbidden(*_args, **_kwargs):
        raise AssertionError("The replacement path called an original-host model/restart/marker clear")

    monkeypatch.setattr(recovery.Recovery, "models", forbidden)
    monkeypatch.setattr(recovery.Recovery, "restart_host", forbidden)
    monkeypatch.setattr(recovery.Recovery, "clear_marker", forbidden)
    summary = run(environment, execute=True)
    assert summary["success"] and summary["repaired"] and summary["phase1_only"] and not summary["workloads_ready"]
    assert fake.native_actions == ["delete", "scale"]
    assert len([row for row in fake.writes if row[0] == "az"]) == 2
    marker_patch = json.loads(fake.value(fake.writes[0], "-p"))
    assert [row["path"] for row in marker_patch if row["op"] == "test"][:3] == [
        "/metadata/uid", "/metadata/resourceVersion",
        f"/metadata/annotations/{recovery.MARKER_KEY.replace('/', '~1')}",
    ]
    assert not any(row["path"].startswith("/spec/taints") for row in marker_patch)
    assert plan == pinned_plan and json.loads(Path(args.plan_file).read_text(encoding="utf-8")) == pinned_plan
    derived = summary["replacement_derived_identity"]
    assert derived["node_name"] == NEW_NODE and derived["instance_id"] == NEW_INSTANCE
    assert derived["vm_id"] == NEW_VM_ID and derived["node_uid"] == NEW_NODE_UID
    assert derived["network_container_id"] == NEW_NC
    assert summary["replacement_derived_manifest"]["real_node_uids"] == {
        **{name: uid for name, uid in recovery.REAL_UIDS.items() if name != recovery.PROM_NODE},
        NEW_NODE: NEW_NODE_UID,
    }
    assert summary["original_identity"]["node_name"] == recovery.PROM_NODE
    assert summary["replacement"]["native_removal"]["original_marker_removed_by"] == "native-node-removal"
    assert not summary["replacement"]["native_removal"]["manual_marker_clearance"]
    assert not summary["restart"]["attempted"] and summary["restart"]["accepted"] is False
    assert summary["restart"]["skipped_reason"] == "replaced-not-restarted"
    assert summary["restart"]["host_proven"] and not summary["restart"].get("marker_removed")
    assert all(row["ready_node"] == NEW_NODE for row in summary["pod_moves"])
    assert len(summary["ip_proofs"]) == 5 and all(row["node_uid"] == NEW_NODE_UID for row in summary["ip_proofs"])
    assert summary["memory_commitments"][f"kube-system/{plan['api_pod_name']}-replacement"]["reserved_memory_bytes"] \
        == 8 * 1024**3
    assert summary["memory_commitments"]["monitoring/grafana-6df78447fb-xdcsl-replacement"]["reserved_memory_bytes"] \
        == 8 * 1024**3
    assert summary["cilium_proof"]["cilium_agent_count"] == 3
    assert set(summary["cilium_proof"]["covered_node_names"]) == set(summary["replacement_derived_manifest"]["real_node_uids"])
    assert summary["fleet_connected"] and summary["final_mock_ready"] == 71 and summary["final_kwok_ready"] == 100
    assert {row["metadata"]["name"]: row["metadata"]["uid"] for row in fake.pods
            if row["metadata"].get("namespace") == "mock-clustermesh"} == original_mock_uids
    for name, node in original_nodes.items():
        assert fake.nodes[name]["spec"] == node["spec"]
        assert fake.nodes[name]["status"]["nodeInfo"] == node["status"]["nodeInfo"]
    moved = [name for _, name, _ in fake.deleted if not name.startswith("prom-recovery-ip-")]
    assert moved == [row["pod_name"] for row in summary["effective_targets"]]
    assert not set(summary["original_identity"]["host_pod_uids"]) & {uid for _, _, uid in fake.deleted}


@pytest.mark.parametrize("remaining", ["node", "pod", "nnc", "vm", "capacity", "pool-count"])
def test_no_restore_before_authoritative_native_removal(environment, monkeypatch, remaining):
    _, _, fake = environment
    old_node = copy.deepcopy(fake.nodes[recovery.PROM_NODE])
    old_pod = copy.deepcopy(next(row for row in fake.pods if row["spec"].get("nodeName") == recovery.PROM_NODE))
    old_nnc = copy.deepcopy(next(row for row in fake.nncs if row["metadata"]["name"] == recovery.PROM_NODE))
    old_vm = copy.deepcopy(fake.instances[recovery.PROM_VMSS][0])

    def incomplete():
        marked = copy.deepcopy(fake.nodes[recovery.PROM_NODE])
        fake.finish_removal()
        if remaining == "node":
            fake.nodes[recovery.PROM_NODE] = marked
        elif remaining == "pod":
            fake.pods.append(old_pod)
        elif remaining == "nnc":
            fake.nncs.append(old_nnc)
        elif remaining == "vm":
            fake.instances[recovery.PROM_VMSS] = [old_vm]
        elif remaining == "capacity":
            fake.vmsses[1]["sku"]["capacity"] = 1
        else:
            fake.pools[1]["count"] = 1
        assert old_node["metadata"]["uid"] == recovery.REAL_UIDS[recovery.PROM_NODE]

    fake.on_native_delete = incomplete
    base.abort_wait(monkeypatch)
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    summary = failed_receipt()
    assert fake.native_actions == ["delete"] and not fake.deleted
    assert not summary["replacement"]["restore"]["attempted"]


@pytest.mark.parametrize("action", ["delete", "scale"])
def test_ambiguous_native_request_is_never_retried_or_followed_by_mutation(environment, action):
    _, _, fake = environment
    if action == "delete":
        fake.native_delete_error = "native delete transport timeout"
    else:
        fake.scale_error = "native scale transport timeout"
    with pytest.raises(recovery.workers.ReconcileError, match="transport timeout"):
        run(environment, execute=True)
    summary = failed_receipt()
    key = "delete" if action == "delete" else "restore"
    receipt = summary["replacement"][key]
    assert receipt["attempted"] and receipt["accepted"] is None and receipt["ambiguous"]
    assert receipt["requested_at"] and receipt["returned_at"]
    expected = ["delete"] if action == "delete" else ["delete", "scale"]
    assert fake.native_actions == expected and not fake.deleted
    assert summary["replacement"]["accepted_reimage_lineage"]["marker"]["action"] == "single-instance-reimage"
    if action == "delete":
        assert replacement.REPLACEMENT_KEY in fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"]
        assert recovery.MARKER_KEY in fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"]
    else:
        assert summary["replacement"]["native_removal"]["old_node_pods_nnc_absent"]
    writes = copy.deepcopy(fake.writes)
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    assert fake.writes == writes and fake.native_actions == expected


@pytest.mark.parametrize("fault", [
    "old-vm-id", "old-instance-id", "old-node-name", "old-node-uid", "default-node-uid", "old-boot",
    "provider", "node-image", "old-network", "network-owner", "network-object", "network-empty",
    "network-zero", "network-duplicate-ip", "network-terminating", "unschedulable", "blocking-taint", "marker", "vm-failed",
    "vmss-failed", "guest-uninitialized", "guest-failed", "no-node", "two-nodes",
])
def test_only_a_distinct_initialized_healthy_replacement_can_be_certified(environment, monkeypatch, fault):
    _, _, fake = environment
    old_boot = recovery.node_boot(fake.nodes[recovery.PROM_NODE])
    old_nnc_uid = next(row["metadata"]["uid"] for row in fake.nncs if row["metadata"]["name"] == recovery.PROM_NODE)

    def bad_restoration():
        fake.finish_restoration()
        node = fake.nodes[NEW_NODE]
        vm = fake.instances[recovery.PROM_VMSS][0]
        nnc = next(row for row in fake.nncs if row["metadata"]["name"] == NEW_NODE)
        if fault == "old-vm-id":
            vm["vmId"] = recovery.FAILED_PROM_VM_ID
        elif fault == "old-instance-id":
            vm.update(instanceId="0", id=vm["id"].rsplit("/", 1)[0] + "/0")
        elif fault == "old-node-name":
            vm["computerName"] = recovery.PROM_NODE
        elif fault == "old-node-uid":
            node["metadata"]["uid"] = recovery.REAL_UIDS[recovery.PROM_NODE]
        elif fault == "default-node-uid":
            node["metadata"]["uid"] = recovery.REAL_UIDS[recovery.SOURCE_NODE]
        elif fault == "old-boot":
            node["status"]["nodeInfo"]["bootID"] = old_boot
        elif fault == "provider":
            node["spec"]["providerID"] = recovery.PROVIDER
        elif fault == "node-image":
            node["metadata"]["labels"]["kubernetes.azure.com/node-image-version"] = "other-image"
        elif fault == "old-network":
            nnc["status"]["networkContainers"][0]["id"] = replacement.FAILED_NETWORK_CONTAINER
        elif fault == "network-owner":
            nnc["metadata"]["ownerReferences"][0]["name"] = recovery.SOURCE_NODE
        elif fault == "network-object":
            nnc["metadata"]["uid"] = old_nnc_uid
        elif fault == "network-empty":
            nnc["status"]["networkContainers"] = []
        elif fault == "network-zero":
            nnc["status"]["assignedIPCount"] = 0
        elif fault == "network-duplicate-ip":
            assignments = nnc["status"]["networkContainers"][0]["ipAssignments"]
            assignments[1]["ip"] = assignments[0]["ip"]
        elif fault == "network-terminating":
            nnc["metadata"]["deletionTimestamp"] = base.now()
        elif fault == "unschedulable":
            node["spec"]["unschedulable"] = True
        elif fault == "blocking-taint":
            node["spec"]["taints"] = [{"key": "foreign", "effect": "NoSchedule"}]
        elif fault == "marker":
            node["metadata"]["annotations"][replacement.REPLACEMENT_KEY] = "{}"
        elif fault == "vm-failed":
            vm["provisioningState"] = "Failed"
        elif fault == "vmss-failed":
            fake.vmsses[1]["provisioningState"] = "Failed"
        elif fault in ("guest-uninitialized", "guest-failed"):
            fake.views[(recovery.PROM_VMSS, NEW_INSTANCE)]["extensions"][0]["statuses"] = (
                None if fault == "guest-uninitialized" else [{"code": "ProvisioningState/failed"}]
            )
        elif fault == "no-node":
            del fake.nodes[NEW_NODE]
        else:
            extra = base.make_node(f"{NEW_NODE}-extra", base.uid("extra"), pool="prompool", instance="36")
            fake.nodes[extra["metadata"]["name"]] = extra

    fake.on_scale = bad_restoration
    base.abort_wait(monkeypatch)
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    summary = failed_receipt()
    assert fake.native_actions == ["delete", "scale"] and not fake.deleted
    assert not summary["replacement"]["replacement_completed"] and not summary["workloads_ready"]


@pytest.mark.parametrize("phase", ["delete", "restore"])
@pytest.mark.parametrize("fault", [
    "default-vm", "default-boot", "default-pool", "kwok", "kwok-taint", "mock-uid",
    "mock-ready", "controller", "pdb", "dns-sibling", "default-network", "fleet",
])
def test_all_protected_scopes_remain_pinned_during_host_operations(environment, monkeypatch, phase, fault):
    _, _, fake = environment

    def drift():
        (fake.finish_removal if phase == "delete" else fake.finish_restoration)()
        if fault == "default-vm":
            fake.instances[recovery.DEFAULT_VMSS][0]["vmId"] = base.uid("changed-default-vm")
        elif fault == "default-boot":
            fake.nodes[recovery.SOURCE_NODE]["status"]["nodeInfo"]["bootID"] = base.uid("changed-boot")
        elif fault == "default-pool":
            fake.pools[0]["vmSize"] = "Standard_D16_v3"
        elif fault == "kwok":
            fake.nodes["kwok-node-99"]["metadata"]["uid"] = base.uid("changed-kwok")
        elif fault == "kwok-taint":
            fake.nodes["kwok-node-99"]["spec"]["taints"] = []
        elif fault == "mock-uid":
            fake.get_pod("kwok-node-99")["metadata"]["uid"] = base.uid("changed-mock")
        elif fault == "mock-ready":
            fake.get_pod("kwok-node-0")["status"] = base.ready_status(False)
        elif fault == "controller":
            fake.get_controller("kwok-node", "StatefulSet")["spec"]["template"]["spec"]["containers"][0]["image"] = "changed"
        elif fault == "pdb":
            fake.pdbs[0]["spec"]["minAvailable"] = 0
        elif fault == "dns-sibling":
            fake.get_pod(f"{recovery.DNS_REPLICA_SET}-healthy-2")["status"] = base.ready_status(False)
        elif fault == "default-network":
            nnc = next(row for row in fake.nncs if row["metadata"]["name"] == f"{recovery.DEFAULT_VMSS}000001")
            nnc["status"]["networkContainers"][0]["id"] = base.uid("changed-default-nc")
        else:
            fake.members[0]["meshProperties"]["status"] = {"state": "Failed", "error": {"code": "ConnectivityTimeout"}}

    if phase == "delete":
        fake.on_native_delete = drift
    else:
        fake.on_scale = drift
    base.abort_wait(monkeypatch)
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    failed_receipt()
    assert fake.native_actions == (["delete"] if phase == "delete" else ["delete", "scale"])
    assert not fake.deleted


def test_owned_deletion_and_restoration_transitions_are_bounded_reads(environment, monkeypatch):
    _, _, fake = environment
    waits = []

    def deleting():
        fake.pools[1]["provisioningState"] = "DeletingMachines"
        fake.vmsses[1]["provisioningState"] = "Updating"
        fake.instances[recovery.PROM_VMSS][0]["provisioningState"] = "Deleting"
        fake.views[(recovery.PROM_VMSS, "0")]["statuses"] = [
            {"code": "ProvisioningState/deleting"}, {"code": "PowerState/stopping"},
        ]
        fake.scale_view["statuses"] = [{"code": "ProvisioningState/updating"}]

    def creating():
        fake.pools[1].update(count=1, provisioningState="Scaling")
        fake.vmsses[1]["provisioningState"] = "Updating"
        fake.scale_view["statuses"] = [{"code": "ProvisioningState/updating"}]

    def advance(operator, deadline, description):
        waits.append(description)
        assert deadline <= operator.work_deadline
        if operator.stage == "deleting":
            assert fake.native_actions == ["delete"]
            fake.finish_removal()
        else:
            assert operator.stage == "restoring" and fake.native_actions == ["delete", "scale"]
            fake.finish_restoration()

    fake.on_native_delete = deleting
    fake.on_scale = creating
    monkeypatch.setattr(recovery.Recovery, "wait", advance)
    summary = run(environment, execute=True)
    assert summary["repaired"] and len(waits) == 2 and fake.native_actions == ["delete", "scale"]


def test_marker_and_scope_are_rechecked_after_cordon_before_delete(environment):
    _, _, fake = environment
    original = fake.kubernetes

    def alter_marker(command):
        result = original(command)
        if "patch" in command and replacement.REPLACEMENT_KEY.replace("/", "~1") in fake.value(command, "-p"):
            fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"][recovery.MARKER_KEY] = "{}"
        return result

    fake.kubernetes = alter_marker
    with pytest.raises(recovery.workers.ReconcileError, match="marker changed"):
        run(environment, execute=True)
    summary = failed_receipt()
    assert summary["replacement"]["marker_write"]["accepted"]
    assert not summary["replacement"]["delete"]["attempted"] and not fake.native_actions
    assert len(fake.writes) == 1 and not fake.deleted


@pytest.mark.parametrize("fault", ["memory", "fleet", "peers", "probe", "replacement-vm", "replacement-nc"])
def test_post_host_failure_keeps_genuine_receipt_without_false_success(environment, monkeypatch, fault):
    _, _, fake = environment
    if fault == "memory":
        fake.memory = "23Gi"
    elif fault == "fleet":
        fake.fleet_stuck = True
    elif fault == "peers":
        fake.peer_fault = "wrong-name"
    elif fault == "probe":
        fake.probe_ready = False
    else:
        def change_vm(_old, _new):
            if fault == "replacement-vm":
                fake.instances[recovery.PROM_VMSS][0]["vmId"] = base.uid("second-replacement-vm")
            else:
                nnc = next(row for row in fake.nncs if row["metadata"]["name"] == NEW_NODE)
                nnc["status"]["assignedIPCount"] = 0
        fake.delete_callback = change_vm
    base.abort_wait(monkeypatch)
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    summary = failed_receipt()
    assert fake.native_actions == ["delete", "scale"]
    assert summary["replacement"]["replacement_completed"]
    assert summary["replacement_derived_identity"]["node_uid"] == NEW_NODE_UID
    assert summary["replacement_derived_identity"]["vm_id"] == NEW_VM_ID
    assert not summary["workloads_ready"] and summary["phase1_only"]
    assert not summary.get("temporary_exclusions")
    assert summary.get("probe_cleanup_pending") is None


@pytest.mark.parametrize("fault", ["ambiguous", "plan", "owner", "timestamp", "vm", "pools", "defaults"])
def test_accepted_action_receipt_is_not_optional_authority(environment, fault):
    args, _, fake = environment
    checkpoint = json.loads(Path(args.replace_failed_host).read_text(encoding="utf-8"))
    if fault == "ambiguous":
        checkpoint["restart"]["ambiguous"] = True
    elif fault == "plan":
        checkpoint["plan_sha256"] = "b" * 64
    elif fault == "owner":
        checkpoint["restart"]["marker"]["owner"] = "foreign"
    elif fault == "timestamp":
        checkpoint["restart"]["requested_at"] = "not-a-timestamp"
    elif fault == "vm":
        checkpoint["arm_metadata"]["instances"][recovery.PROM_NODE]["vm_id"] = base.uid("wrong-vm")
    elif fault == "pools":
        checkpoint["arm_metadata"]["pools"] = []
    else:
        del checkpoint["arm_metadata"]["instances"][recovery.SOURCE_NODE]
    save_checkpoint(args, checkpoint)
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    failed_receipt()
    assert_no_writes(fake)
    assert not fake.commands


def test_cordon_marker_ambiguity_retains_both_lineages_without_host_action(environment):
    _, _, fake = environment
    original = fake.kubernetes

    def ambiguous(command):
        result = original(command)
        if "patch" in command:
            raise recovery.workers.ReconcileError("marker patch transport timeout")
        return result

    fake.kubernetes = ambiguous
    with pytest.raises(recovery.workers.ReconcileError, match="marker patch transport timeout"):
        run(environment, execute=True)
    summary = failed_receipt()
    marker = summary["replacement"]["marker_write"]
    assert marker["attempted"] and marker["ambiguous"] and marker["accepted"] is None
    assert not fake.native_actions and len(fake.writes) == 1 and not fake.deleted
    annotations = fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"]
    assert recovery.MARKER_KEY in annotations and replacement.REPLACEMENT_KEY in annotations


@pytest.mark.parametrize("fault", ["new-failure-time", "new-failure-code", "unrelated-operation", "timeout"])
def test_deletion_does_not_treat_new_failures_or_timeout_as_completion(environment, monkeypatch, fault):
    _, _, fake = environment

    def stalled():
        if fault == "new-failure-time":
            fake.views[(recovery.PROM_VMSS, "0")]["statuses"][0]["time"] = (
                datetime.now(timezone.utc) - timedelta(minutes=10)
            ).isoformat()
        elif fault == "new-failure-code":
            fake.scale_view["statuses"][0]["code"] = "ProvisioningState/failed/NewError"
        elif fault == "unrelated-operation":
            fake.operation.update(status="InProgress", endTime=None, startTime=base.now(),
                                  operationType="PutManagedCluster")

    fake.on_native_delete = stalled
    base.abort_wait(monkeypatch)
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    summary = failed_receipt()
    assert fake.native_actions == ["delete"] and not fake.deleted
    assert not summary["replacement"]["restore"]["attempted"]
    assert recovery.MARKER_KEY in fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"]
    assert replacement.REPLACEMENT_KEY in fake.nodes[recovery.PROM_NODE]["metadata"]["annotations"]


def test_stale_exact_failure_during_owned_vmss_update_is_observed_not_retried(environment, monkeypatch):
    _, _, fake = environment

    def begin_deletion():
        fake.pools[1]["provisioningState"] = "DeletingMachines"
        fake.vmsses[1]["provisioningState"] = "Updating"

    def finish(operator, _deadline, _description):
        assert operator.stage == "deleting" and fake.native_actions == ["delete"]
        fake.finish_removal()

    fake.on_native_delete = begin_deletion
    monkeypatch.setattr(recovery.Recovery, "wait", finish)
    assert run(environment, execute=True)["repaired"]
    assert fake.native_actions == ["delete", "scale"]


def test_uninitialized_candidate_never_rewrites_plan_or_host_before_real_proof(environment, monkeypatch):
    _, plan, fake = environment
    ready = {}

    def starting():
        fake.finish_restoration()
        node = fake.nodes[NEW_NODE]
        nnc = next(row for row in fake.nncs if row["metadata"]["name"] == NEW_NODE)
        ready["conditions"] = copy.deepcopy(node["status"]["conditions"])
        ready["network"] = copy.deepcopy(nnc["status"])
        node["status"]["conditions"][0]["status"] = "False"
        nnc["status"]["networkContainers"] = []

    def initialize(operator, _deadline, _description):
        assert operator.stage == "restoring" and operator.derived is None
        assert operator.host_node == recovery.PROM_NODE and operator.real_uids == recovery.REAL_UIDS
        assert operator.plan["node_name"] == plan["node_name"] == recovery.PROM_NODE
        assert "replacement_derived_identity" not in base.receipt()
        assert fake.native_actions == ["delete", "scale"] and not fake.deleted
        fake.nodes[NEW_NODE]["status"]["conditions"] = ready["conditions"]
        nnc = next(row for row in fake.nncs if row["metadata"]["name"] == NEW_NODE)
        nnc["status"] = ready["network"]

    fake.on_scale = starting
    monkeypatch.setattr(recovery.Recovery, "wait", initialize)
    summary = run(environment, execute=True)
    assert summary["repaired"] and summary["replacement_derived_identity"]["node_name"] == NEW_NODE


@pytest.mark.parametrize("when", ["before-plan", "before-delete"])
def test_native_removal_accepts_only_natural_loss_of_existing_failed_pods(environment, when):
    _, _, fake = environment
    pod = next(row for row in fake.pods if row["metadata"]["name"].startswith("old-terminating-system-"))
    if when == "before-plan":
        fake.pods.remove(pod)
        summary = run(environment)
        assert len(summary["original_identity"]["host_pod_uids"]) == 17
        assert_no_writes(fake)
        return
    authority_reads = 0

    def collect_failed_pod(command):
        nonlocal authority_reads
        if command[:3] == ["az", "account", "show"]:
            authority_reads += 1
            if authority_reads == 2:
                fake.pods.remove(pod)

    fake.hook = collect_failed_pod
    summary = run(environment, execute=True)
    assert summary["repaired"] and fake.native_actions == ["delete", "scale"]
    assert not any(row[2] == pod["metadata"]["uid"] for row in fake.deleted)


@pytest.mark.parametrize("initializing", [
    "null-extensions", "empty-extensions", "partial-extensions", "null-statuses", "unapplied-model",
])
def test_owned_creating_vm_waits_for_actual_model_and_guest_initialization(environment, monkeypatch, initializing):
    _, _, fake = environment
    final = {}
    if initializing == "partial-extensions":
        fake.views[(recovery.PROM_VMSS, "0")]["extensions"].append({"name": "AKSLinuxExtension", "statuses": None})

    def starting():
        fake.finish_restoration()
        view = fake.views[(recovery.PROM_VMSS, NEW_INSTANCE)]
        if initializing == "partial-extensions":
            view["extensions"].append({
                "name": "AKSLinuxExtension", "statuses": [{"code": "ProvisioningState/succeeded"}],
            })
        final["view"] = copy.deepcopy(view)
        final["scale_view"] = copy.deepcopy(fake.scale_view)
        fake.pools[1]["provisioningState"] = "Scaling"
        fake.vmsses[1]["provisioningState"] = "Updating"
        vm = fake.instances[recovery.PROM_VMSS][0]
        vm["provisioningState"] = "Creating"
        view["statuses"] = [{"code": "ProvisioningState/creating"}, {"code": "PowerState/starting"}]
        fake.scale_view["statuses"] = [{"code": "ProvisioningState/updating"}]
        fake.scale_view["virtualMachines"] = [{"code": "ProvisioningState/creating", "count": 1}]
        if initializing == "null-extensions":
            view["extensions"] = None
        elif initializing == "empty-extensions":
            view["extensions"] = []
        elif initializing == "partial-extensions":
            view["extensions"].pop()
        elif initializing == "null-statuses":
            view["statuses"] = None
        else:
            vm["latestModelApplied"] = None

    def initialize(operator, _deadline, _description):
        assert operator.stage == "restoring" and operator.derived is None
        assert not base.receipt()["replacement"]["replacement_completed"] and not fake.deleted
        assert fake.native_actions == ["delete", "scale"]
        fake.pools[1]["provisioningState"] = "Succeeded"
        fake.vmsses[1]["provisioningState"] = "Succeeded"
        fake.instances[recovery.PROM_VMSS][0].update(provisioningState="Succeeded", latestModelApplied=True)
        fake.views[(recovery.PROM_VMSS, NEW_INSTANCE)] = final["view"]
        fake.scale_view = final["scale_view"]

    fake.on_scale = starting
    monkeypatch.setattr(recovery.Recovery, "wait", initialize)
    assert run(environment, execute=True)["repaired"]


def test_derived_host_regression_cannot_complete_replacement(environment, monkeypatch):
    _, _, fake = environment
    waits = 0

    def starting():
        fake.finish_restoration()
        fake.get_pod("azure-cns-new-prom")["status"]["conditions"][0]["status"] = "False"

    def regress(operator, _deadline, _description):
        nonlocal waits
        waits += 1
        assert operator.derived is not None and not operator.record["replacement_completed"]
        if waits == 1:
            fake.nodes[NEW_NODE]["status"]["conditions"][0]["status"] = "False"
            fake.get_pod("azure-cns-new-prom")["status"]["conditions"][0]["status"] = "True"
            return
        raise recovery.workers.ReconcileError("Replacement Node regressed while waiting for system readiness")

    fake.on_scale = starting
    monkeypatch.setattr(recovery.Recovery, "wait", regress)
    with pytest.raises(recovery.workers.ReconcileError, match="Node regressed"):
        run(environment, execute=True)
    assert waits == 2 and not failed_receipt()["replacement"]["replacement_completed"]
    assert fake.native_actions == ["delete", "scale"] and not fake.deleted


@pytest.mark.parametrize("execute", [False, True])
def test_real_azure_failure_precision_on_pipeline_python310(environment, monkeypatch, execute):
    _, _, fake = environment
    base.use_python310_datetime(monkeypatch)
    observed = datetime.now(timezone.utc) - timedelta(minutes=20)
    vm_time = observed.strftime("%Y-%m-%dT%H:%M:%S.%f") + "3+00:00"
    vmss_time = observed.strftime("%Y-%m-%dT%H:%M:%S.%f") + "6+00:00"
    fake.views[(recovery.PROM_VMSS, "0")]["statuses"][0]["time"] = vm_time
    fake.scale_view["statuses"][0]["time"] = vmss_time
    summary = run(environment, execute=execute)
    assert summary["terminal_failure_observations"]["VM"]["time"] == vm_time
    assert summary["terminal_failure_observations"]["VMSS"]["time"] == vmss_time
    assert summary["repaired"] is execute
    assert fake.native_actions == (["delete", "scale"] if execute else [])


def test_unreported_summary_with_a_live_original_vm_is_not_an_empty_inventory(environment):
    _, _, fake = environment
    fake.scale_view["virtualMachines"] = None
    with pytest.raises(recovery.workers.ReconcileError, match="status summary"):
        run(environment, execute=True)
    summary = failed_receipt()
    assert summary["arm_metadata"]["reported_vm_status_counts_type"] == "NoneType"
    assert summary["arm_metadata"]["vm_status_counts"] is None
    assert_no_writes(fake)


def test_no_vm_summary_after_exact_native_removal_uses_authoritative_zero(environment):
    _, _, fake = environment

    def remove():
        fake.finish_removal()
        fake.scale_view["virtualMachines"] = None

    fake.on_native_delete = remove
    summary = run(environment, execute=True)
    assert summary["repaired"] and summary["replacement"]["native_removal"]["pool_count"] == 0
    assert fake.native_actions == ["delete", "scale"]


def test_no_vm_summary_while_owned_restoration_has_no_instances_is_not_ready(environment, monkeypatch):
    _, _, fake = environment

    def creating():
        fake.pools[1].update(count=1, provisioningState="Scaling")
        fake.vmsses[1].update(provisioningState="Updating")
        fake.vmsses[1]["sku"]["capacity"] = 1
        fake.scale_view = {"statuses": [{"code": "ProvisioningState/updating"}], "virtualMachines": None}

    def finish(operator, _deadline, _description):
        assert operator.stage == "restoring" and operator.derived is None and not fake.deleted
        assert not base.receipt()["replacement"]["replacement_completed"]
        fake.finish_restoration()

    fake.on_scale = creating
    monkeypatch.setattr(recovery.Recovery, "wait", finish)
    assert run(environment, execute=True)["repaired"]
    assert fake.native_actions == ["delete", "scale"]
