"""Focused offline tests for the capacity-only phase after the failed VM1 restart."""

# pylint: disable=protected-access,too-many-lines

from __future__ import annotations

import copy
import importlib.util
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import jmespath
import pytest

from .test_stalled_retained_worker_recovery import Cloud, metadata, now, pod, ref, uid


MODULE_DIR = Path(__file__).resolve().parents[1] / "clusterloader2" / "clustermesh-scale"
SPEC = importlib.util.spec_from_file_location("capacity_first_worker_recovery", MODULE_DIR / "capacity_first_worker_recovery.py")
capacity = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = capacity
sys.path.insert(0, str(MODULE_DIR))
try:
    SPEC.loader.exec_module(capacity)
finally:
    sys.path.pop(0)
base = capacity.base
stalled = capacity.stalled
VMSS = "aks-cniv5-23456789-vmss"
NAMES = [f"{VMSS}000007", f"{VMSS}00000b"]
IMAGE = "AKSUbuntu-2404containerd-202609.10.0"


class CapacityCloud(Cloud):
    """Reuse the real-schema failed-worker fixture; forbid its restart route."""

    def __init__(self, args):
        super().__init__(args)
        self.node_group["tags"] = {"deletion_due_time": self.group["tags"]["deletion_due_time"]}
        self.scales[0]["provisioningState"] = "Failed"
        for node in (stalled.SOURCE, stalled.TARGET):
            self.nodes[node]["status"]["nodeInfo"]["kubeletVersion"] = "v1.35.7"
        for pool in self.pools:
            pool["orchestratorVersion"] = "1.35"
            pool["vnetSubnetId"] = capacity.prom.POOL_SETTINGS["vnetSubnetId"]
            pool["podSubnetId"] = capacity.prom.POOL_SETTINGS["podSubnetId"]
        self.journals = {}
        self.new_instances = []
        self.new_views = {}
        self.new_scale = None
        self.new_model = None
        self.new_aggregate = None
        self.add_calls = []
        self.add_error = False
        self.journal_create_error = False
        self.partial = False
        self.restart_receipt = None
        self.failure_time = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        self.aggregate = {
            "statuses": [{"code": capacity.FAILURE, "time": self.failure_time}],
            "virtualMachine": {"statusesSummary": [{"code": "ProvisioningState/succeeded", "count": 2}]},
        }
        self.skus = [{
            "name": capacity.prom.VM_SIZE, "resourceType": "virtualMachines", "family": capacity.prom.QUOTA_FAMILY,
            "locations": [base.REGION], "restrictions": [],
            "capabilities": [{"name": name, "value": value} for name, value in (
                ("vCPUs", "8"), ("MemoryGB", "32"), ("CpuArchitectureType", "x64"),
                ("PremiumIO", "True"), ("OSVhdSizeMB", "1047552"),
            )],
        }]
        self.usage = [
            {"name": {"value": capacity.prom.QUOTA_FAMILY}, "currentValue": "100.0", "limit": "1000.0"},
            {"name": {"value": "cores"}, "currentValue": "8682.0", "limit": "11897.0"},
            {"name": {"value": "standardDv3Family"}, "currentValue": "5464.0", "limit": "5000.0"},
        ]

    def write_source(self):
        super().write_source()
        root = Path(self.args.source_state_directory)
        started = datetime.now(timezone.utc) - timedelta(minutes=20)
        accepted = started + timedelta(seconds=1)
        failed = capacity.base.timestamp(self.failure_time, "fixture failure") + timedelta(seconds=1)
        detail = {
            "status": "Failed", "error": {"code": "ResourceOperationFailure", "details": [
                {"code": "VMExtensionProvisioningError", "target": "1",
                 "message": f"{base.DEFAULT_VMSS}-AKSLinuxBilling AKSLinuxExtension vmssCSE "
                            "has not reported status for VM agent or extensions"}]},
        }
        activity = [{
            "correlationId": capacity.CORRELATION, "operation": capacity.RESTART_OPERATION,
            "resourceId": self.scales[0]["id"], "status": status, "eventTimestamp": stamp.isoformat(),
            "properties": {"statusMessage": json.dumps(detail)} if status == "Failed" else {},
        } for status, stamp in (("Started", started), ("Accepted", accepted), ("Failed", failed))]
        (root / "default-vmss-activity-log.json").write_text(json.dumps(activity), encoding="utf-8")
        (root / "default-vmss-instance-view.json").write_text(
            json.dumps(jmespath.search(base.SCALE_VIEW_QUERY, self.aggregate)), encoding="utf-8")
        self.restart_receipt = {
            "execute": True, "plan_sha256": stalled.PLAN_SHA,
            "original_identity": {"node_name": stalled.TARGET, "node_uid": base.REAL_UIDS[stalled.TARGET],
                                  "vm_id": stalled.VM_IDS[stalled.TARGET], "boot_id": stalled.BOOTS[stalled.TARGET]},
            "restart": {"attempted": True, "submission_started": True, "accepted": True, "ambiguous": False,
                        "command": stalled.Recovery.restart_command(), "requested_at": started.isoformat(),
                        "accepted_at": accepted.isoformat()},
            "journal": {"name": stalled.JOURNAL, "uid": capacity.RESTART_JOURNAL_UID},
            "preserved_kwok_node_uids": {name: self.nodes[name]["metadata"]["uid"]
                                         for name in capacity.maintenance.EXPECTED_AGENT_NAMES},
        }
        Path(self.args.restart_checkpoint).write_text(json.dumps(self.restart_receipt), encoding="utf-8")
        self.journals[stalled.JOURNAL] = {
            "metadata": metadata(stalled.JOURNAL, "kube-system", capacity.RESTART_JOURNAL_UID),
            "data": {"old": "do-not-touch"},
        }

    def add_new_pool(self):
        pool = {**capacity.pool_settings("1.35.7"), "id": f"{self.cluster_id}/agentPools/cniv5",
                "nodeImageVersion": IMAGE, "provisioningState": "Creating" if self.partial else "Succeeded",
                "powerState": {"code": "Running"}}
        self.pools.append(pool)
        scale_id = (f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.NODE_GROUP}"
                    f"/providers/Microsoft.Compute/virtualMachineScaleSets/{VMSS}")
        self.new_scale = {"id": scale_id, "name": VMSS, "location": base.REGION, "tags": {"aks-managed-poolName": "cniv5"},
                          "sku": {"name": capacity.prom.VM_SIZE, "capacity": 2}, "orchestrationMode": "Uniform",
                          "provisioningState": pool["provisioningState"]}
        self.scales.append(self.new_scale)
        self.new_aggregate = {
            "statuses": [{"code": "ProvisioningState/succeeded"}],
            "virtualMachine": {"statusesSummary": [] if self.partial else [{"code": "ProvisioningState/succeeded", "count": 2}]},
        }
        self.new_model = {"id": scale_id, "virtualMachineProfile": {"storageProfile": {
            "osDisk": {"osType": "Linux", "diskSizeGb": 256, "managedDisk": {"storageAccountType": "Premium_LRS"}},
            "imageReference": {"id": "/galleries/AKSUbuntu/images/2404containerd/versions/202609.10.0"},
        }}}
        if not self.partial:
            self.register_new_nodes()

    def register_new_nodes(self, *, network_ready=True):
        for instance, name in zip(("7", "11"), NAMES):
            self.new_instances.append({
                "instanceId": instance, "computerName": name, "osProfile": {"computerName": name},
                "id": f"{self.new_scale['id']}/virtualMachines/{instance}", "vmId": uid("vm/" + name),
                "latestModelApplied": True, "provisioningState": "Succeeded",
            })
            self.new_views[instance] = {
                "statuses": [{"code": "ProvisioningState/succeeded"}, {"code": "PowerState/running"}],
                "extensions": [{"name": "vmssCSE", "statuses": [{"code": "ProvisioningState/succeeded"}]}],
            }
            node = {
                "metadata": metadata(name), "spec": {"providerID": "azure://" + self.new_instances[-1]["id"]},
                "status": {"nodeInfo": {"bootID": uid("boot/" + name), "kubeletVersion": "v1.35.7"},
                           "conditions": [{"type": "Ready", "status": "True"}]},
            }
            node["metadata"]["labels"] = {
                "kubernetes.azure.com/cluster": base.NODE_GROUP, "agentpool": "cniv5",
                "kubernetes.azure.com/agentpool": "cniv5", "kubernetes.azure.com/node-image-version": IMAGE,
            }
            self.nodes[name] = node
            network = {
                "metadata": {**metadata(name, "kube-system"), "ownerReferences": [ref("Node", name, uid("/" + name))]},
            }
            network["metadata"]["ownerReferences"][0]["uid"] = node["metadata"]["uid"]
            if network_ready:
                network["status"] = self.network_status(name)
            self.nncs.append(network)
            for daemon in ("cilium", "azure-cns"):
                self.pods.append(pod(f"{daemon}-{name}", "kube-system", name,
                                     ref("DaemonSet", daemon, uid(f"kube-system/{daemon}"))))
        self.new_scale["provisioningState"] = "Succeeded"
        self.pools[-1]["provisioningState"] = "Succeeded"
        self.new_aggregate["virtualMachine"]["statusesSummary"] = [{"code": "ProvisioningState/succeeded", "count": 2}]

    @staticmethod
    def network_status(name):
        return {"assignedIPCount": 16, "networkContainers": [
            {"id": uid("nc/" + name), "version": 2,
             "ipAssignments": [{"ip": f"10.100.{NAMES.index(name)}.{index}"} for index in range(1, 17)]}]}

    def azure(self, command):
        if command[1:3] == ["vmss", "restart"]:
            raise AssertionError("No restart is authorized in capacity phase")
        if command[1:3] == ["aks", "show"]:
            return {"id": self.cluster_id, "kubernetesVersion": "1.35", "currentKubernetesVersion": "1.35.7"}
        if command[1:3] == ["vm", "list-usage"]:
            return self.usage
        if command[1:3] == ["vm", "list-skus"]:
            return self.skus
        if command[1:3] == ["vmss", "show"]:
            assert self.value(command, "--name") == VMSS
            return self.new_model
        if command[1:3] == ["vmss", "list-instances"] and self.value(command, "--name") == VMSS:
            return self.new_instances
        if command[1:3] == ["vmss", "get-instance-view"]:
            if self.value(command, "--name") == VMSS:
                return self.new_views[self.value(command, "--instance-id")] if "--instance-id" in command else self.new_aggregate
            if "--instance-id" not in command:
                assert self.value(command, "--name") == base.DEFAULT_VMSS
                return self.aggregate
        if command[1:4] == ["aks", "nodepool", "add"]:
            assert [word for word in command if word not in ("--subscription", base.SUBSCRIPTION)] == capacity.add_command("1.35.7")
            self.writes.append(command)
            self.add_calls.append(command)
            record = self.receipt()["create"]
            assert record["attempted"] and record["accepted"] is None and record["ambiguous"]
            assert json.loads(self.journals[capacity.JOURNAL]["data"]["create"])["accepted"] is None
            self.add_new_pool()
            if self.add_error:
                raise capacity.workers.ReconcileError("ambiguous pool add response")
            return ""
        return super().azure(command)

    def kube(self, command):
        if "create" in command:
            self.writes.append(command)
            assert self.value(command, "create") == "configmap" and capacity.JOURNAL in command
            assert capacity.JOURNAL not in self.journals
            self.journals[capacity.JOURNAL] = {
                "metadata": metadata(capacity.JOURNAL, "kube-system"),
                "data": dict(entry.removeprefix("--from-literal=").split("=", 1)
                             for entry in command if entry.startswith("--from-literal=")),
            }
            if self.journal_create_error:
                raise capacity.workers.ReconcileError("ambiguous journal create")
            return self.journals[capacity.JOURNAL]
        if "patch" in command:
            self.writes.append(command)
            assert self.value(command, "patch") == "configmap" and capacity.JOURNAL in command
            operations = json.loads(self.value(command, "-p"))
            journal = self.journals[capacity.JOURNAL]
            assert operations[0]["value"] == journal["metadata"]["uid"]
            assert operations[1]["value"] == journal["metadata"]["resourceVersion"]
            assert operations[2] == {"op": "test", "path": "/data", "value": journal["data"]}
            journal["data"] = operations[3]["value"]
            journal["metadata"]["resourceVersion"] = str(int(journal["metadata"]["resourceVersion"]) + 1)
            return journal
        if "get" in command and self.value(command, "get") == "configmaps":
            return {"items": list(self.journals.values())}
        if "get" in command and self.value(command, "get") == "configmap":
            return self.journals[self.value(command, "configmap")]
        return super().kube(command)


@pytest.fixture(name="environment")
def setup_environment(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    args = SimpleNamespace(
        resource_group=base.RESOURCE_GROUP, confirm_resource_group=base.RESOURCE_GROUP,
        expected_subscription=base.SUBSCRIPTION, expected_region=base.REGION, expected_tfvars_sha="a" * 64,
        source_state_directory="source", restart_checkpoint="accepted-restart.json", kubeconfig="private-config",
        context=base.CLUSTER, summary_file="capacity.json", timeout_seconds=1800, execute=False,
    )
    cloud = CapacityCloud(args)
    cloud.write_source()
    return args, cloud


def run(environment, *, execute=False):
    args, cloud = environment
    args.execute = execute
    summary = {}
    capacity.execute_recovery(args, summary, runner=cloud.run)
    assert summary == cloud.receipt()
    return summary


def test_plan_is_read_only_and_does_not_gate_on_broken_kwok_or_metrics(environment):
    _, cloud = environment
    summary = run(environment)
    assert not cloud.writes and summary["plan_valid"] and not summary["mutation_started"]
    assert summary["current_mock_ready"] == 38 and summary["current_kwok_ready"] == 0
    assert not summary["capacity_qualified"] and not summary["pool_created"] and not summary["workloads_ready"]
    assert summary["quota_proof"]["required_cores"] == 24
    assert not any("/apis/metrics" in " ".join(command) for command in cloud.commands)


def test_one_exact_add_preserves_every_old_identity_and_only_claims_registration(environment):
    _, cloud = environment
    old_nodes = copy.deepcopy(cloud.nodes)
    old_pods = copy.deepcopy(cloud.pods)
    old_journal = copy.deepcopy(cloud.journals[stalled.JOURNAL])
    summary = run(environment, execute=True)
    assert len(cloud.add_calls) == 1 and not cloud.restart_calls
    assert "--kubelet-disk-type" not in cloud.add_calls[0]
    assert summary["create"]["submission_started"] is True
    assert all(cloud.nodes[name] == node for name, node in old_nodes.items())
    assert cloud.pods[:len(old_pods)] == old_pods
    assert cloud.journals[stalled.JOURNAL] == old_journal
    assert summary["pool_created"] and summary["registered_nodes_ready"] and summary["success"]
    assert not summary["capacity_qualified"] and not summary["bootstrap_complete"] and not summary["workloads_ready"]
    assert "baseline_pool_layout" not in summary and "modern_cni" not in summary
    assert {row["instance_id"] for row in summary["new_identities"].values()} == {"7", "11"}
    assert len(summary["preserved_kwok_node_uids"]) == len(summary["current_mock_uids"]) == 100
    assert all(command[1:4] == ["aks", "nodepool", "add"] for command in cloud.writes if command[0] == "az")


def test_partial_empty_vmss_and_missing_nnc_are_pending_not_ready(environment, monkeypatch):
    _, cloud = environment
    cloud.partial = True
    waits = []

    def advance(_seconds):
        waits.append(True)
        assert not cloud.receipt()["registered_nodes_ready"] and not cloud.receipt()["capacity_qualified"]
        if len(waits) == 1:
            assert cloud.new_instances == []
            cloud.register_new_nodes(network_ready=False)
        elif len(waits) == 2:
            for row in cloud.nncs:
                if row["metadata"]["name"] in NAMES:
                    row["status"] = cloud.network_status(row["metadata"]["name"])
        else:
            raise AssertionError("Unexpected extra registration wait")
    monkeypatch.setattr(capacity.time, "sleep", advance)
    summary = run(environment, execute=True)
    assert len(waits) == 2 and summary["registered_nodes_ready"] and len(cloud.add_calls) == 1


@pytest.mark.parametrize("initial_capacity", [0, 1, 2])
def test_owned_pool_power_and_capacity_initialize_before_registration(environment, monkeypatch, initial_capacity):
    _, cloud = environment
    cloud.partial = True
    original = cloud.add_new_pool
    observed = []

    def partial():
        original()
        cloud.new_scale["sku"]["capacity"] = initial_capacity
        cloud.pools[-1]["powerState"] = None

    def complete(_seconds):
        observed.append(True)
        assert not cloud.receipt()["registered_nodes_ready"]
        cloud.new_scale["sku"]["capacity"] = 2
        cloud.pools[-1]["powerState"] = {"code": "Running"}
        cloud.register_new_nodes()

    cloud.add_new_pool = partial
    monkeypatch.setattr(capacity.time, "sleep", complete)
    assert run(environment, execute=True)["registered_nodes_ready"]
    assert observed and len(cloud.add_calls) == 1


def test_missing_stable_aggregate_status_is_not_registration_success(environment, monkeypatch):
    _, cloud = environment
    original = cloud.add_new_pool
    observed = []

    def incomplete():
        original()
        cloud.new_aggregate["statuses"] = None

    def complete(_seconds):
        observed.append(True)
        assert not cloud.receipt()["registered_nodes_ready"]
        cloud.new_aggregate["statuses"] = [{"code": "ProvisioningState/succeeded"}]

    cloud.add_new_pool = incomplete
    monkeypatch.setattr(capacity.time, "sleep", complete)
    assert run(environment, execute=True)["registered_nodes_ready"]
    assert observed and len(cloud.add_calls) == 1


@pytest.mark.parametrize("field", ["owner", "source_state_sha256", "desired_pool_sha256", "create"])
def test_changed_live_capacity_journal_prevents_provider_add(environment, field):
    _, cloud = environment
    changed = []

    def mutate(command):
        journal = cloud.journals.get(capacity.JOURNAL)
        if "nodenetworkconfigs" in command and journal and not changed:
            if json.loads(journal["data"]["create"]).get("attempted"):
                changed.append(True)
                journal["data"][field] = "changed"

    cloud.hook = mutate
    with pytest.raises(capacity.workers.ReconcileError, match="journal"):
        run(environment, execute=True)
    assert changed and not cloud.add_calls


@pytest.mark.parametrize("instance", ["0", "1"])
def test_stale_guest_status_never_qualifies_original_workers(environment, instance):
    _, cloud = environment
    cloud.views[instance]["vmAgent"]["statuses"][0]["time"] = cloud.old
    with pytest.raises(capacity.workers.ReconcileError):
        run(environment, execute=True)
    assert not cloud.writes


@pytest.mark.parametrize("fault", [
    "correlation", "terminal-target", "aggregate-code", "aggregate-time", "not-submitted", "ambiguous-restart",
])
def test_source_terminal_lineage_is_mandatory(environment, fault):
    args, cloud = environment
    root = Path(args.source_state_directory)
    if fault.startswith("aggregate"):
        path = root / "default-vmss-instance-view.json"
        data = json.loads(path.read_text(encoding="utf-8"))
        data["statuses"][0]["code" if fault == "aggregate-code" else "time"] = (
            "ProvisioningState/succeeded" if fault == "aggregate-code" else "2026-09-01T00:00:00Z")
    elif fault in ("not-submitted", "ambiguous-restart"):
        path = Path(args.restart_checkpoint)
        data = copy.deepcopy(cloud.restart_receipt)
        data["restart"]["submission_started" if fault == "not-submitted" else "ambiguous"] = fault != "not-submitted"
    else:
        path = root / "default-vmss-activity-log.json"
        data = json.loads(path.read_text(encoding="utf-8"))
        if fault == "correlation":
            data[0]["correlationId"] = uid("wrong-correlation")
        else:
            detail = json.loads(data[2]["properties"]["statusMessage"])
            detail["error"]["details"][0]["target"] = "0"
            data[2]["properties"]["statusMessage"] = json.dumps(detail)
    path.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(capacity.workers.ReconcileError):
        run(environment, execute=True)
    assert not cloud.commands and not cloud.writes


@pytest.mark.parametrize("fault", ["scope", "node-lease", "vm0", "boot0", "pod0", "kwok-uid", "kwok-spec", "mock-terminating",
                                  "family-quota", "region-quota", "sku", "default-failure", "extra-pool", "journal"])
def test_preflight_protects_original_scope_workloads_and_capacity(environment, fault):
    _, cloud = environment
    if fault == "scope":
        cloud.node_group["managedBy"] += "-wrong"
    elif fault == "node-lease":
        cloud.node_group["tags"]["deletion_due_time"] = now()
    elif fault == "vm0":
        cloud.instances[0]["vmId"] = uid("wrong-vm")
    elif fault == "boot0":
        cloud.nodes[stalled.SOURCE]["status"]["nodeInfo"]["bootID"] = uid("wrong-boot")
    elif fault == "pod0":
        cloud.pods[0]["metadata"]["uid"] = uid("wrong-pod")
    elif fault == "kwok-uid":
        cloud.nodes["kwok-node-2"]["metadata"]["uid"] = uid("wrong-kwok")
    elif fault == "kwok-spec":
        cloud.nodes["kwok-node-2"]["spec"]["podCIDR"] = "1.2.3.0/24"
    elif fault == "mock-terminating":
        cloud.pods[44]["metadata"].pop("deletionTimestamp")
    elif fault in ("family-quota", "region-quota"):
        cloud.usage[0 if fault == "family-quota" else 1]["limit"] = "1.0"
    elif fault == "sku":
        cloud.skus[0]["restrictions"] = [{"type": "Location"}]
    elif fault == "default-failure":
        cloud.aggregate["statuses"][0]["code"] = "ProvisioningState/failed/UnrelatedFailure"
    elif fault == "extra-pool":
        cloud.add_new_pool()
    else:
        cloud.journals["modern-cni-old-attempt"] = {"metadata": metadata("modern-cni-old-attempt", "kube-system"), "data": {}}
    with pytest.raises(capacity.workers.ReconcileError):
        run(environment, execute=True)
    assert not cloud.writes


@pytest.mark.parametrize("fault", ["journal", "add", "late-source-change"])
def test_ambiguous_or_invalid_submission_has_no_replay_or_cleanup(environment, fault):
    _, cloud = environment
    if fault == "journal":
        cloud.journal_create_error = True
    elif fault == "add":
        cloud.add_error = True
    else:
        def mutate(command):
            if "patch" in command:
                cloud.nodes[stalled.SOURCE]["metadata"]["uid"] = uid("changed-after-intent")
        cloud.hook = mutate
    with pytest.raises(capacity.workers.ReconcileError):
        run(environment, execute=True)
    assert len(cloud.add_calls) == (1 if fault == "add" else 0)
    assert capacity.JOURNAL in cloud.journals
    assert not cloud.receipt()["success"]
    assert not any("delete" in command or "restart" in command or "taint" in command for command in cloud.writes)


@pytest.mark.parametrize("fault", ["extra-vm", "vm-failed", "scope", "owner", "node-uid", "nc-owner", "old-pod", "boot0"])
def test_owned_registration_never_adopts_failure_foreign_identity_or_old_drift(environment, fault):
    _, cloud = environment
    changed = []

    def mutate(_command):
        if not cloud.new_scale or changed:
            return
        changed.append(True)
        if fault == "extra-vm":
            cloud.new_instances.append(copy.deepcopy(cloud.new_instances[0]))
        elif fault == "vm-failed":
            cloud.new_instances[0]["provisioningState"] = "Failed"
        elif fault == "scope":
            cloud.new_scale["id"] += "-wrong"
        elif fault == "owner":
            cloud.new_scale["tags"]["aks-managed-poolName"] = "foreign"
        elif fault == "node-uid":
            cloud.nodes[NAMES[0]]["metadata"]["uid"] = base.REAL_UIDS[stalled.SOURCE]
        elif fault == "nc-owner":
            cloud.nncs[-1]["metadata"]["ownerReferences"][0]["uid"] = uid("unowned")
        elif fault == "old-pod":
            cloud.pods[0]["metadata"]["uid"] = uid("old-pod-changed")
        else:
            cloud.nodes[stalled.SOURCE]["status"]["nodeInfo"]["bootID"] = uid("old-boot-changed")
    cloud.hook = mutate
    with pytest.raises(capacity.workers.ReconcileError):
        run(environment, execute=True)
    assert len(cloud.add_calls) == 1 and not cloud.receipt()["registered_nodes_ready"]


def test_partial_registration_timeout_never_retries_provider_write(environment, monkeypatch):
    _, cloud = environment
    clock = [capacity.time.monotonic()]
    monkeypatch.setattr(capacity.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(capacity.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + max(seconds, 1800)))
    cloud.partial = True
    with pytest.raises(capacity.workers.ReconcileError):
        run(environment, execute=True)
    assert len(cloud.add_calls) == 1 and cloud.receipt()["create"]["accepted"] is True
    assert not cloud.receipt()["capacity_qualified"] and not cloud.receipt()["workloads_ready"]


def test_source_hashes_are_rechecked_after_journal_before_add(environment):
    args, cloud = environment
    def mutate(command):
        if "patch" in command:
            path = Path(args.source_state_directory) / "quota-observation.json"
            path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    cloud.hook = mutate
    with pytest.raises(capacity.workers.ReconcileError, match="hashes changed"):
        run(environment, execute=True)
    assert not cloud.add_calls


def test_cli_and_python310_syntax():
    import ast  # pylint: disable=import-outside-toplevel
    ast.parse((MODULE_DIR / "capacity_first_worker_recovery.py").read_text(encoding="utf-8"), feature_version=(3, 10))
    args = capacity.parse_args([
        "--resource-group", base.RESOURCE_GROUP, "--confirm-resource-group", base.RESOURCE_GROUP,
        "--expected-subscription", base.SUBSCRIPTION, "--expected-region", base.REGION,
        "--expected-tfvars-sha", "a" * 64, "--source-state-directory", "source",
        "--restart-checkpoint", "restart.json", "--kubeconfig", "private", "--summary-file", "out.json",
    ])
    assert not args.execute and args.context == base.CLUSTER and args.timeout_seconds == 1800


@pytest.mark.parametrize("missing", ["computerName", "vmId"])
def test_owned_partial_vm_guest_metadata_waits_without_guessing_identity(environment, monkeypatch, missing):
    _, cloud = environment
    cloud.partial = True
    original = cloud.add_new_pool

    def partial():
        original()
        cloud.new_instances = [{
            "instanceId": "7", "id": f"{cloud.new_scale['id']}/virtualMachines/7",
            "osProfile": {"computerName": None if missing == "computerName" else NAMES[0]},
            "vmId": None if missing == "vmId" else uid("vm/" + NAMES[0]),
            "provisioningState": "Creating", "latestModelApplied": False,
        }]
        cloud.new_views["7"] = {"statuses": [{"code": "ProvisioningState/creating"}, {"code": "PowerState/starting"}],
                                "extensions": [{"name": "vmssCSE", "statuses": None}]}

    def advance(_seconds):
        assert not cloud.receipt()["registered_nodes_ready"] and not cloud.receipt()["new_identities"]
        cloud.new_instances = []
        cloud.register_new_nodes()

    monkeypatch.setattr(cloud, "add_new_pool", partial)
    monkeypatch.setattr(capacity.time, "sleep", advance)
    summary = run(environment, execute=True)
    assert summary["registered_nodes_ready"] and len(cloud.add_calls) == 1
