"""Synthetic native-retirement tests: no real Azure/Kubernetes mutations."""

# pylint: disable=protected-access,too-many-lines

from __future__ import annotations

import copy
import importlib.util
import json
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from .test_capacity_first_qualification import QualificationCloud, NAMES, qualification as q
from .test_stalled_retained_worker_recovery import metadata, now, status, uid


MODULE_DIR = Path(__file__).resolve().parents[1] / "clusterloader2" / "clustermesh-scale"
SPEC = importlib.util.spec_from_file_location("qualified_failed_worker_retirement", MODULE_DIR / "qualified_failed_worker_retirement.py")
retirement = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = retirement
sys.path.insert(0, str(MODULE_DIR))
try:
    SPEC.loader.exec_module(retirement)
finally:
    sys.path.pop(0)
base, stalled = retirement.base, retirement.stalled


class RetirementCloud(QualificationCloud):
    def __init__(self, args):
        super().__init__(args)
        self.retirement_mode = False
        self.native_calls = []
        self.native_error = False
        self.before_fence = False
        self.never_complete = False
        self.fail_inventory = False
        self.after_native = None
        self.hold_cleanup_error = False
        self.retirement_journal_error = False
        self.default_operation = {"name": "original-default-operation", "operationType": "PutAgentPool",
                                  "status": "Succeeded", "startTime": self.old, "endTime": self.old}

    def allocate(self, node):
        network = next(row for row in self.nncs if row["metadata"]["name"] == node)
        container = network["status"]["networkContainers"][0]
        allocated = {row["ip"] for row in container["ipAssignments"]}
        occupied = {row["status"].get("podIP") for row in self.pods if row["spec"].get("nodeName") == node}
        free = sorted(allocated - occupied)
        if free:
            return free[0]
        added = [f"10.100.{NAMES.index(node)}.{index}" for index in range(1, 255)
                 if f"10.100.{NAMES.index(node)}.{index}" not in allocated][:16]
        container["ipAssignments"].extend({"ip": value} for value in added)
        container["version"] += 1
        network["status"]["assignedIPCount"] += len(added)
        return added[0]

    def retire(self):
        targets = [row for row in self.pods if row["spec"].get("nodeName") == stalled.TARGET
                   and row["metadata"].get("namespace") == "mock-clustermesh"]
        self.pods = [row for row in self.pods if row["spec"].get("nodeName") != stalled.TARGET]
        if not self.before_fence:
            self.instances = [row for row in self.instances if str(row["instanceId"]) == "0"]
            self.nodes.pop(stalled.TARGET)
            self.nncs = [row for row in self.nncs if row["metadata"]["name"] != stalled.TARGET]
            self.pools[0]["count"] = 1
            self.scales[0]["sku"]["capacity"] = 1
            self.scales[0]["provisioningState"] = "Succeeded"
        else:
            self.scales[0]["provisioningState"] = "Updating"
        for index, row in enumerate(targets):
            row["metadata"]["uid"] = uid("retired-replacement/" + row["metadata"]["uid"])
            row["metadata"].pop("deletionTimestamp", None)
            row["spec"]["nodeName"] = NAMES[index % 2]
            row["status"] = status(True)
            row["status"]["podIP"] = self.allocate(row["spec"]["nodeName"])
            self.pods.append(row)

    def azure(self, command):
        if command[1:4] == ["aks", "operation", "show-latest"] and "--nodepool-name" in command:
            if self.value(command, "--nodepool-name") == "default":
                return self.default_operation
        if self.retirement_mode and self.fail_inventory and self.native_calls and command[1:3] == ["vmss", "list-instances"]:
            if self.value(command, "--name") == base.DEFAULT_VMSS:
                raise retirement.workers.ReconcileError("Forbidden authoritative VM enumeration")
        if command[1:4] == ["aks", "nodepool", "delete-machines"]:
            assert self.retirement_mode
            assert self.value(command, "--machine-names") == stalled.TARGET
            assert self.value(command, "--name") == "default"
            assert not any(word in command for word in ("--force", "--ignore-pod-disruption-budget", "--skip-drain"))
            assert len([row for row in self.nodes[stalled.SOURCE]["spec"].get("taints", [])
                        if row.get("key") == retirement.HOLD_KEY and row.get("effect") == "NoSchedule"]) == 1
            receipt = self.receipt()["native"]
            assert receipt["submission_started"] is True and receipt["accepted"] is None and receipt["ambiguous"]
            data = self.journals[retirement.JOURNAL]["data"]
            assert json.loads(data["native"])["submission_started"] is True
            self.native_calls.append(command)
            self.writes.append(command)
            if self.native_error:
                raise retirement.workers.ReconcileError("ambiguous native DeleteMachines delivery")
            self.default_operation = {"name": "owned-native-default1-operation", "operationType": "DeleteMachines",
                                      "status": "InProgress" if self.before_fence or self.never_complete else "Succeeded",
                                      "startTime": now(), "endTime": None if self.before_fence or self.never_complete else now()}
            if not self.never_complete:
                self.retire()
            if self.after_native:
                self.after_native()
            return ""
        return super().azure(command)

    def kube(self, command):
        if self.retirement_mode and ("create" in command or "patch" in command):
            self.writes.append(command)
            if "create" in command:
                assert retirement.JOURNAL in command and self.value(command, "create") == "configmap"
                assert retirement.JOURNAL not in self.journals
                self.journals[retirement.JOURNAL] = {
                    "metadata": metadata(retirement.JOURNAL, "kube-system"),
                    "data": dict(item.removeprefix("--from-literal=").split("=", 1)
                                 for item in command if item.startswith("--from-literal=")),
                }
                if self.retirement_journal_error:
                    raise retirement.workers.ReconcileError("ambiguous retirement journal creation")
                return self.journals[retirement.JOURNAL]
            patch = json.loads(self.value(command, "-p"))
            if self.value(command, "patch") == "node":
                assert self.value(command, "node") == stalled.SOURCE
                node = self.nodes[stalled.SOURCE]
                assert patch[0]["value"] == node["metadata"]["uid"]
                assert patch[1]["value"] == node["metadata"]["resourceVersion"]
                if self.hold_cleanup_error and any(row["op"] == "remove" for row in patch):
                    raise retirement.workers.ReconcileError("ambiguous hold cleanup")
                for row in patch[2:]:
                    if row["path"] == "/spec/taints":
                        node["spec"]["taints"] = copy.deepcopy(row["value"])
                    elif row["op"] == "test":
                        assert node["spec"]["taints"][int(row["path"].rsplit("/", 1)[1])] == row["value"]
                    elif row["op"] == "remove":
                        node["spec"]["taints"].pop(int(row["path"].rsplit("/", 1)[1]))
                node["metadata"]["resourceVersion"] = str(int(node["metadata"]["resourceVersion"]) + 1)
                return node
            assert retirement.JOURNAL in command and self.value(command, "patch") == "configmap"
            journal = self.journals[retirement.JOURNAL]
            assert patch[0]["value"] == journal["metadata"]["uid"] and patch[1]["value"] == journal["metadata"]["resourceVersion"]
            assert patch[2]["op"] == "test" and patch[2]["value"] == journal["data"]
            journal["data"] = patch[3]["value"]
            journal["metadata"]["resourceVersion"] = str(int(journal["metadata"]["resourceVersion"]) + 1)
            return journal
        if self.retirement_mode and "get" in command and self.value(command, "get") == "configmaps":
            name = self.value(command, "--field-selector").split("=", 1)[1]
            return {"items": [self.journals[name]] if name in self.journals else []}
        if self.retirement_mode and "get" in command and self.value(command, "get") == "node":
            return self.nodes[self.value(command, "node")]
        if self.retirement_mode and "get" in command and self.value(command, "get") == "configmap":
            return self.journals[self.value(command, "configmap")]
        return super().kube(command)


@pytest.fixture(scope="module", name="qualified_seed")
def build_qualified_seed(tmp_path_factory):
    root = tmp_path_factory.getbasetemp() / "retirement-qualified-seed"
    root.mkdir()
    args = SimpleNamespace(
        resource_group=base.RESOURCE_GROUP, confirm_resource_group=base.RESOURCE_GROUP,
        expected_subscription=base.SUBSCRIPTION, expected_region=base.REGION, expected_tfvars_sha="a" * 64,
        observation_directory=str(root / "observation"), observation_build_id=79975,
        capacity_directory=str(root / "capacity-input"), capacity_build_id=79971,
        kubeconfig="not-opened-private", context=base.CLUSTER, timeout_seconds=2400,
        summary_file=str(root / "probe-output.json"), execute=True,
        completed_qualification_checkpoint=None, completed_qualification_build_id=0,
    )
    cloud = RetirementCloud(args)
    cloud.pdbs = [{
        "metadata": {**metadata(name, "kube-system"), "generation": 1},
        "spec": {"minAvailable": 1, "unhealthyPodEvictionPolicy": "AlwaysAllow", "selector": {"matchLabels": {"app": name}}},
        "status": {"observedGeneration": 1, "disruptionsAllowed": budget},
    } for name, budget in (("ama-metrics-pdb", 1), ("coredns-pdb", 4), ("konnectivity-agent", 2), ("metrics-server-pdb", 1))]
    cloud.initialize_artifacts()
    # Match the real diagnostic redaction boundary without weakening live pins.
    controller = next(row for row in cloud.controllers if row["kind"] == "DaemonSet" and row["metadata"]["name"] == "cilium")
    controller["spec"]["template"]["spec"]["containers"][0]["env"] = [{"name": "CSI_SECRET", "value": "synthetic-secret"}]
    for pod in cloud.pods:
        if pod["metadata"].get("ownerReferences", [{}])[0].get("kind") == "DaemonSet" and pod["metadata"]["ownerReferences"][0]["name"] == "cilium":
            pod["spec"]["containers"][0]["env"] = [{"name": "CSI_SECRET", "value": "synthetic-secret"}]
    source_controllers = Path(cloud.args.source_state_directory) / "current-controllers.json"
    source_controllers.write_text(json.dumps({"items": cloud.controllers}), encoding="utf-8")
    source_hashes = stalled.file_hashes(cloud.args.source_state_directory)
    creation_path = Path(args.capacity_directory) / "recovery.json"
    creation = json.loads(creation_path.read_text(encoding="utf-8"))
    creation["source_hashes"] = source_hashes
    creation["source_state_sha256"] = q.digest(source_hashes)
    creation_path.write_text(json.dumps(creation), encoding="utf-8")
    cloud.journals[q.capacity.JOURNAL]["data"]["source_state_sha256"] = q.digest(source_hashes)
    (Path(args.observation_directory) / "cniv5-capacity-journal.json").write_text(
        json.dumps(cloud.journals[q.capacity.JOURNAL]), encoding="utf-8")
    for node_name in q.maintenance.EXPECTED_AGENT_NAMES:
        cloud.nodes[node_name]["status"]["conditions"][0]["status"] = "True"
    source_addresses = next(row for row in cloud.nncs if row["metadata"]["name"] == stalled.SOURCE)["status"]["networkContainers"][0]["ipAssignments"]
    source_index = 0
    for row in cloud.pods:
        if row["metadata"].get("ownerReferences", [{}])[0].get("kind") == "DaemonSet":
            row["spec"]["hostNetwork"] = True
        if row["spec"].get("nodeName") == stalled.SOURCE and not row["spec"].get("hostNetwork") and base.pod_ready(row):
            row["status"]["podIP"] = source_addresses[source_index]["ip"]
            source_index += 1
    for index, name in enumerate(NAMES):
        cloud.pods.append({
            "metadata": metadata(f"resident-{index}", "kube-system"),
            "spec": {"nodeName": name, "containers": [{"name": "resident", "image": "pinned",
                                                       "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}}}]},
            "status": {**status(True), "podIP": f"10.100.{index}.1"},
        })
    for key, value in cloud.snapshot().items():
        (Path(args.observation_directory) / f"current-{key}.json").write_text(json.dumps(value), encoding="utf-8")
    cloud.qual_journal_uid = q.COMPLETED_JOURNAL_UID
    original_delete = cloud.delete_probe

    def release(cluster, **kwargs):
        original_delete(cluster, **kwargs)
        if any(q.maintenance.PROBE_LABEL_KEY in row["metadata"].get("labels", {}) for row in cloud.pods):
            return
        for row in cloud.nncs:
            name = row["metadata"]["name"]
            if name not in NAMES:
                continue
            container = row["status"]["networkContainers"][0]
            occupied = {pod["status"]["podIP"] for pod in cloud.pods
                        if pod["spec"].get("nodeName") == name and not pod["spec"].get("hostNetwork")}
            spare = [entry["ip"] for entry in reversed(container["ipAssignments"]) if entry["ip"] not in occupied]
            container["ipAssignments"] = [{"ip": value} for value in sorted(occupied) + spare[:16 - len(occupied)]]
            container["version"] += 1
            row["status"]["assignedIPCount"] = 16
    cloud.delete_probe = release
    receipt = {}
    q.execute_qualification(args, receipt, runner=cloud.run, delete_pod=cloud.delete_probe)
    assert receipt["capacity_qualified"] and len(cloud.deleted) == 32
    prior = copy.deepcopy(receipt)
    prior.update(success=False, capacity_qualified=False, actual_ip_growth_proven=False,
                 actual_memory_headroom_proven=False, error="The pinned allocated IP baseline regressed", status="failed-closed")
    prior_path = root / "prior-qualification.json"
    prior_path.write_text(json.dumps(prior), encoding="utf-8")
    cloud.journals[q.JOURNAL]["data"]["state"] = "proving-real-ip-growth"
    args.completed_qualification_checkpoint = str(prior_path)
    args.completed_qualification_build_id = 79979
    args.summary_file = cloud.args.summary_file = str(root / "qualification.json")
    args.execute = False
    completed = {}
    q.execute_qualification(args, completed, runner=cloud.run)
    assert completed["capacity_qualified"] and completed["current_kwok_ready"] == 100
    (root / "probe-output.json").unlink()
    cloud.retirement_mode = True
    cloud.commands.clear()
    cloud.writes.clear()
    cloud.deleted.clear()
    return root, cloud


@pytest.fixture(name="environment")
def setup_environment(tmp_path, qualified_seed, monkeypatch):
    root, original = qualified_seed
    monkeypatch.chdir(tmp_path)
    shutil.copytree(root, tmp_path / "qualification-input")
    cloud = copy.deepcopy(original)
    args = SimpleNamespace(
        resource_group=base.RESOURCE_GROUP, confirm_resource_group=base.RESOURCE_GROUP,
        expected_subscription=base.SUBSCRIPTION, expected_region=base.REGION, expected_tfvars_sha="a" * 64,
        qualification_directory="qualification-input", qualification_build_id=79986,
        kubeconfig="not-opened-private", context=base.CLUSTER, timeout_seconds=3600,
        summary_file="retirement.json", execute=False,
    )
    cloud.args.summary_file = args.summary_file
    cloud.args.kubeconfig = args.kubeconfig
    cloud.public_args.summary_file = args.summary_file
    return args, cloud


def run(environment, execute=False):
    args, cloud = environment
    args.execute = execute
    summary = {}
    retirement.execute_retirement(args, summary, runner=cloud.run)
    assert summary == cloud.receipt()
    return summary


def test_plan_is_read_only_and_preserves_completed_qualification(environment):
    _, cloud = environment
    result = run(environment)
    assert result["plan_valid"] and not result["mutation_started"] and not cloud.writes
    assert result["kwok_ready"] == 100 and not result["native_fencing_proven"]
    assert result["placement_hold_removed"] is False and result["cleanup_errors"] == []
    assert result["fresh_pre_retirement_headroom"]["remaining_count"] == 56
    assert not result["workloads_ready"] and not result["bootstrap_complete"]


def test_exact_single_native_target1_fencing_then_56_ready_and_owned_hold_cleanup(environment):
    _, cloud = environment
    old_nodes = copy.deepcopy(cloud.nodes)
    old_journals = copy.deepcopy(cloud.journals)
    source_uids = {pod["metadata"]["name"]: pod["metadata"]["uid"] for pod in cloud.pods
                   if pod["spec"].get("nodeName") == stalled.SOURCE}
    result = run(environment, True)
    assert result["success"] and result["native_fencing_proven"] and result["source_retired"] and result["replacements_ready"]
    assert len(cloud.native_calls) == 1 and result["native"]["submission_started"]
    assert len(result["controller_replacements"]) == 56 and result["current_mock_ready"] == result["kwok_ready"] == 100
    assert all(record["node_name"] in NAMES and record["ready"] for record in result["controller_replacements"].values())
    assert all(cloud.journals[name] == value for name, value in old_journals.items())
    assert all(pod["metadata"]["uid"] == source_uids[pod["metadata"]["name"]] for pod in cloud.pods
               if pod["metadata"]["name"] in source_uids)
    assert cloud.nodes[stalled.SOURCE]["status"]["nodeInfo"] == old_nodes[stalled.SOURCE]["status"]["nodeInfo"]
    assert not cloud.nodes[stalled.SOURCE]["spec"].get("taints") and result["hold"]["applied"] is False
    assert result["hold"]["remove"]["accepted"] is True
    assert {key: result[key] for key in (
        "execute", "success", "native_fencing_proven", "source_retired", "replacements_ready",
        "placement_hold_removed", "cleanup_errors", "workloads_ready", "bootstrap_complete",
    )} == {
        "execute": True, "success": True, "native_fencing_proven": True, "source_retired": True,
        "replacements_ready": True, "placement_hold_removed": True, "cleanup_errors": [],
        "workloads_ready": False, "bootstrap_complete": False,
    }
    assert all(command[1:4] == ["aks", "nodepool", "delete-machines"] for command in cloud.writes if command[0] == "az")
    assert not cloud.deleted and not result["workloads_ready"] and "baseline_pool_layout" not in result


@pytest.mark.parametrize("fault", ["node0", "vm0", "target-vm", "target-boot", "target-ready", "mock44", "target56",
                                  "pvc", "pdb", "controller", "kwok", "journal"])
def test_live_preflight_guards_prevent_all_writes(environment, fault):
    _, cloud = environment
    if fault == "node0":
        cloud.nodes[stalled.SOURCE]["metadata"]["uid"] = uid("foreign")
    elif fault == "vm0":
        cloud.instances[0]["vmId"] = uid("foreign")
    elif fault == "target-vm":
        cloud.instances[1]["vmId"] = uid("foreign")
    elif fault == "target-boot":
        cloud.nodes[stalled.TARGET]["status"]["nodeInfo"]["bootID"] = uid("foreign")
    elif fault == "target-ready":
        cloud.nodes[stalled.TARGET]["status"]["conditions"][0].update(status="True", lastHeartbeatTime=now())
    elif fault in ("mock44", "target56", "pvc"):
        name = "kwok-node-0" if fault == "mock44" else "kwok-node-44"
        pod = next(row for row in cloud.pods if row["metadata"]["name"] == name)
        if fault == "pvc":
            pod["spec"]["volumes"].append({"name": "bad", "persistentVolumeClaim": {"claimName": "foreign"}})
        else:
            pod["metadata"]["uid"] = uid("foreign")
    elif fault == "pdb":
        cloud.pdbs[0]["status"]["disruptionsAllowed"] = 0
    elif fault == "controller":
        cloud.controllers[0]["spec"]["replicas"] = 99
    elif fault == "kwok":
        cloud.nodes["kwok-node-0"]["status"]["conditions"][0]["status"] = "Unknown"
    else:
        cloud.journals[retirement.JOURNAL] = {"metadata": metadata(retirement.JOURNAL, "kube-system"), "data": {}}
    with pytest.raises(retirement.EXPECTED_ERRORS):
        run(environment, True)
    assert not cloud.writes and not cloud.native_calls


@pytest.mark.parametrize("fault", ["journal", "native", "inventory", "before-fencing", "timeout", "hold-cleanup"])
def test_ambiguous_or_incomplete_retirement_never_replays_or_unsafely_undoes(environment, monkeypatch, fault):
    _, cloud = environment
    if fault == "journal":
        cloud.retirement_journal_error = True
    elif fault == "native":
        cloud.native_error = True
    elif fault == "inventory":
        cloud.fail_inventory = True
    elif fault == "before-fencing":
        cloud.before_fence = True
    elif fault == "timeout":
        clock = [retirement.time.monotonic()]
        monkeypatch.setattr(retirement.time, "monotonic", lambda: clock[0])
        monkeypatch.setattr(retirement.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + 3600))
        cloud.never_complete = True
    else:
        cloud.hold_cleanup_error = True
    with pytest.raises(retirement.EXPECTED_ERRORS):
        run(environment, True)
    result = cloud.receipt()
    assert not result["success"] and len(cloud.native_calls) == (0 if fault == "journal" else 1)
    assert result["placement_hold_removed"] is False
    if fault == "hold-cleanup":
        assert result["cleanup_errors"]
    if fault != "journal":
        assert any(row.get("key") == retirement.HOLD_KEY for row in cloud.nodes[stalled.SOURCE]["spec"]["taints"])
    if fault in ("native", "inventory", "before-fencing"):
        assert not result["native_fencing_proven"]
    if fault == "before-fencing":
        assert result["uncertified_replacements_before_fencing"]
    assert not cloud.deleted and not any("restart" in command or "evict" in command for command in cloud.writes)


@pytest.mark.parametrize("fault", ["node0", "replacement-owner", "replacement-image", "replacement-on-source", "new-worker"])
def test_post_fencing_requalification_does_not_waive_identity_or_placement(environment, fault):
    _, cloud = environment
    def changed():
        if fault == "node0":
            cloud.nodes[stalled.SOURCE]["status"]["nodeInfo"]["bootID"] = uid("unexpected-reboot")
        elif fault == "new-worker":
            cloud.nodes[NAMES[0]]["metadata"]["uid"] = uid("unexpected-node")
        else:
            pod = next(row for row in cloud.pods if row["metadata"]["name"] == "kwok-node-44")
            if fault == "replacement-owner":
                pod["metadata"]["ownerReferences"][0]["uid"] = uid("foreign-controller")
            elif fault == "replacement-image":
                pod["spec"]["containers"][0]["image"] = "foreign-image"
            else:
                pod["spec"]["nodeName"] = stalled.SOURCE
    cloud.after_native = changed
    with pytest.raises(retirement.EXPECTED_ERRORS):
        run(environment, True)
    assert len(cloud.native_calls) == 1 and not cloud.receipt()["success"]
    assert cloud.receipt()["native_fencing_proven"]
    assert any(row["key"] == retirement.HOLD_KEY for row in cloud.nodes[stalled.SOURCE]["spec"]["taints"])


def test_qualification_proof_cannot_be_forged_from_registration_only(environment):
    args, cloud = environment
    path = Path(args.qualification_directory) / "qualification.json"
    value = json.loads(path.read_text(encoding="utf-8"))
    value["actual_ip_growth_proven"] = False
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(retirement.EXPECTED_ERRORS):
        run(environment, True)
    assert not cloud.commands


def test_changed_qualification_after_owned_hold_prevents_native_submission(environment):
    args, cloud = environment
    changed = []
    def mutate(command):
        if ("patch" in command and command[command.index("patch") + 1:command.index("patch") + 3]
                == ["node", stalled.SOURCE] and not changed):
            path = Path(args.qualification_directory) / "qualification.json"
            path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
            changed.append(True)
    cloud.hook = mutate
    with pytest.raises(retirement.EXPECTED_ERRORS, match="inputs changed"):
        run(environment, True)
    assert changed and not cloud.native_calls
    assert any(row["key"] == retirement.HOLD_KEY for row in cloud.nodes[stalled.SOURCE]["spec"]["taints"])


def test_final_journal_uncertainty_never_emits_wrapper_success_fields(environment):
    _, cloud = environment
    def fail_final_cas(command):
        if ("patch" in command and retirement.JOURNAL in command
                and cloud.receipt()["status"] == "native-retirement-complete-workloads-not-qualified"):
            assert cloud.receipt()["placement_hold_removed"] is False
            raise retirement.workers.ReconcileError("uncertain final retirement journal CAS")
    cloud.hook = fail_final_cas
    with pytest.raises(retirement.EXPECTED_ERRORS, match="final retirement journal"):
        run(environment, True)
    result = cloud.receipt()
    assert result["native_fencing_proven"] and result["source_retired"] and result["replacements_ready"]
    assert not result["success"] and not result["placement_hold_removed"] and result["cleanup_errors"]
    assert len(cloud.native_calls) == 1


def test_python310_cli_and_no_provider_target_guessing():
    import ast  # pylint: disable=import-outside-toplevel
    ast.parse((MODULE_DIR / "qualified_failed_worker_retirement.py").read_text(encoding="utf-8"), feature_version=(3, 10))
    args = retirement.parse_args([
        "--resource-group", base.RESOURCE_GROUP, "--confirm-resource-group", base.RESOURCE_GROUP,
        "--expected-subscription", base.SUBSCRIPTION, "--expected-region", base.REGION, "--expected-tfvars-sha", "a" * 64,
        "--qualification-directory", "completed", "--qualification-build-id", "79986",
        "--kubeconfig", "private", "--summary-file", "out.json", "--timeout-seconds", "3600",
    ])
    assert not args.execute and args.timeout_seconds == 3600
    command = retirement.delete_command()
    assert command[command.index("--machine-names") + 1] == stalled.TARGET
    assert stalled.SOURCE not in command


def test_owned_deleting_machines_pool_state_is_observed_not_rejected(environment, monkeypatch):
    _, cloud = environment
    cloud.never_complete = True
    cloud.after_native = lambda: cloud.pools[0].update(provisioningState="DeletingMachines")
    waits = []

    def complete(_seconds):
        waits.append(True)
        assert not cloud.receipt()["native_fencing_proven"]
        cloud.retire()
        cloud.pools[0]["provisioningState"] = "Succeeded"
        cloud.default_operation.update(status="Succeeded", endTime=now())

    monkeypatch.setattr(retirement.time, "sleep", complete)
    result = run(environment, True)
    assert waits and result["success"] and len(cloud.native_calls) == 1


def test_known_target_nnc_deletion_is_pending_after_positive_vm_fencing(environment, monkeypatch):
    _, cloud = environment
    old_network = copy.deepcopy(next(row for row in cloud.nncs if row["metadata"]["name"] == stalled.TARGET))

    def delay_gc():
        old_network["metadata"]["deletionTimestamp"] = now()
        old_network["status"].update(assignedIPCount=0, networkContainers=[])
        cloud.nncs.append(old_network)

    def finish_gc(_seconds):
        assert cloud.receipt()["native_fencing_proven"]
        assert cloud.receipt()["native"]["pending_original_nnc_removal"]
        cloud.nncs.remove(old_network)

    cloud.after_native = delay_gc
    monkeypatch.setattr(retirement.time, "sleep", finish_gc)
    result = run(environment, True)
    assert result["success"] and len(cloud.native_calls) == 1


def test_journal_foreign_owner_cannot_be_adopted(environment):
    _, cloud = environment

    def change(command):
        journal = cloud.journals.get(retirement.JOURNAL)
        if journal and "get" in command and retirement.JOURNAL in command:
            journal["metadata"]["ownerReferences"] = [{"kind": "Node", "name": stalled.TARGET, "uid": base.REAL_UIDS[stalled.TARGET]}]

    cloud.hook = change
    with pytest.raises(retirement.EXPECTED_ERRORS, match="journal UID/data/lifecycle"):
        run(environment, True)
    assert not cloud.native_calls


def test_hold_is_rechecked_at_the_native_request_boundary(environment):
    _, cloud = environment

    def change(command):
        if ("get" in command and cloud.value(command, "get") == "node"
                and cloud.receipt()["native"]["submission_started"] and not cloud.native_calls):
            cloud.nodes[stalled.SOURCE]["spec"]["taints"] = []

    cloud.hook = change
    with pytest.raises(retirement.EXPECTED_ERRORS, match="native submission boundary"):
        run(environment, True)
    assert not cloud.native_calls


def test_original_target_spec_cannot_change_while_native_fencing_is_pending(environment):
    _, cloud = environment
    cloud.never_complete = True

    def change():
        next(row for row in cloud.pods if row["metadata"]["name"] == "kwok-node-44")["spec"]["containers"][0]["image"] = "changed"

    cloud.after_native = change
    with pytest.raises(retirement.EXPECTED_ERRORS, match="original target mock UID/spec"):
        run(environment, True)
    assert len(cloud.native_calls) == 1 and not cloud.receipt()["native_fencing_proven"]


@pytest.mark.parametrize("kind", ["pool", "vmss"])
def test_captured_configuration_drift_is_rejected_before_any_worker_action(environment, kind):
    _, cloud = environment
    if kind == "pool":
        cloud.pools[0]["upgradeSettings"] = {"maxSurge": "99%"}
    else:
        cloud.scales[0]["tags"]["unexpected-controller-change"] = "changed"
    with pytest.raises(retirement.EXPECTED_ERRORS, match="configuration changed before retirement"):
        run(environment, True)
    assert not cloud.writes and not cloud.native_calls
