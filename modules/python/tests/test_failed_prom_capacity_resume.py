"""Offline continuation tests using the real native flow and a stateful cloud."""

# pylint: disable=protected-access,too-many-lines

from __future__ import annotations

import copy
import importlib
import json
import sys
from pathlib import Path

import pytest

from . import test_failed_prom_worker_replacement as native_tests


base = native_tests.base
recovery = base.recovery
sys.path.insert(0, str(base.MODULE_DIR))
try:
    resume = importlib.import_module("failed_prom_capacity_resume")
finally:
    sys.path.pop(0)

QUOTA_ERROR = (
    f"command failed (exit=1): {' '.join(resume.SCALE_COMMAND)} --subscription {recovery.SUBSCRIPTION}: "
    "ERROR: (ErrCode_InsufficientVCPUQuota) Insufficient vcpu quota requested 8, remaining 0 "
    "for family standardDv3Family for region eastus2euap.\nCode: ErrCode_InsufficientVCPUQuota\n"
    "Message: Insufficient vcpu quota requested 8, remaining 0 for family standardDv3Family for region eastus2euap."
)


class CapacityCloud(native_tests.ReplacementCloud):
    """Reuse the native state machine; continuation permits only one new scale."""

    def __init__(self, plan, args):
        super().__init__(plan, args)
        self.continuing = False
        self.configmaps = []
        self.new_scales = []
        self.usage = [
            {"name": {"value": resume.QUOTA_FAMILY, "localizedValue": "irrelevant"},
             "currentValue": 800, "limit": 808, "unit": "Count"},
            {"name": {"value": "cores", "localizedValue": "Total Regional vCPUs"},
             "currentValue": 1000, "limit": 1008, "unit": "Count"},
        ]
        self.create_error = False
        self.patch_error = None
        self.create_conflict = False
        self.resume_scale_error = None

    def azure(self, command):
        if command[1:3] == ["vm", "list-usage"]:
            assert self.continuing
            assert self.value(command, "--subscription") == recovery.SUBSCRIPTION
            assert self.value(command, "--location") == recovery.REGION
            assert self.value(command, "--query") == resume.USAGE_QUERY
            return base.jmespath.search(resume.USAGE_QUERY, self.usage)
        if not self.continuing or command[1:4] != ["aks", "nodepool", "scale"]:
            if self.continuing:
                assert command[1:4] != ["aks", "nodepool", "delete-machines"]
            return super().azure(command)
        assert command == [*resume.SCALE_COMMAND, "--subscription", recovery.SUBSCRIPTION]
        assert not self.new_scales
        assert len(self.configmaps) == 1
        guard = self.configmaps[0]
        journal = json.loads(guard["data"]["record"])
        receipt = base.receipt()
        assert receipt["replacement"]["restore"]["attempted"]
        assert receipt["replacement"]["restore"]["accepted"] is None
        assert receipt["replacement"]["restore"]["ambiguous"]
        assert journal["state"] == "attempted" and journal["restore"]["attempted"]
        assert journal["restore"]["accepted"] is None and journal["restore"]["ambiguous"]
        assert receipt["capacity_resume"]["attempt_guard"]["uid"] == guard["metadata"]["uid"]
        assert receipt["quota_ready"] and receipt["capacity_resume"]["fresh_state"]["outcome"] == "quiescent-zero"
        assert self.pools[1]["count"] == self.vmsses[1]["sku"]["capacity"] == 0
        assert not self.instances[recovery.PROM_VMSS] and recovery.PROM_NODE not in self.nodes
        self.writes.append(command)
        self.new_scales.append(command)
        if self.resume_scale_error:
            raise recovery.workers.ReconcileError(self.resume_scale_error)
        (self.on_scale or self.finish_restoration)()
        return ""

    def kubernetes(self, command):
        if "create" in command and "configmap" in command:
            assert self.value(command, "-n") == "kube-system"
            assert self.value(command, "--kubeconfig") == self.args.kubeconfig
            assert self.value(command, "--context") == recovery.CLUSTER
            assert command[command.index("configmap") + 1] == resume.GUARD_NAME
            self.writes.append(command)
            receipt = base.receipt()["capacity_resume"]["attempt_guard"]["create"]
            assert receipt["attempted"] and receipt["accepted"] is None and receipt["ambiguous"]
            if self.create_conflict:
                self.configmaps = [{"metadata": base.metadata(resume.GUARD_NAME, "kube-system")}]
            if self.configmaps:
                raise recovery.workers.ReconcileError("ConfigMap AlreadyExists")
            data = dict(word.removeprefix("--from-literal=").split("=", 1)
                        for word in command if word.startswith("--from-literal="))
            row = {"apiVersion": "v1", "kind": "ConfigMap",
                   "metadata": base.metadata(resume.GUARD_NAME, "kube-system"), "data": data}
            self.configmaps.append(row)
            if self.create_error:
                raise recovery.workers.ReconcileError("exclusive create response lost")
            return row
        if "patch" in command and "configmap" in command:
            self.writes.append(command)
            assert self.value(command, "-n") == "kube-system"
            operations = json.loads(self.value(command, "-p"))
            tests = {row["path"] for row in operations if row["op"] == "test"}
            assert tests >= {"/metadata/uid", "/metadata/resourceVersion", "/data/token", "/data"}
            assert len(self.configmaps) == 1
            self.apply_patch(self.configmaps[0], operations)
            state = json.loads(self.configmaps[0]["data"]["record"])["state"]
            if self.patch_error == state:
                raise recovery.workers.ReconcileError("owned journal patch response lost")
            return self.configmaps[0]
        if "get" in command and "configmaps" in command:
            assert self.value(command, "-n") == "kube-system"
            assert self.value(command, "--field-selector") == f"metadata.name={resume.GUARD_NAME}"
            return {"apiVersion": "v1", "kind": "ConfigMapList", "metadata": {}, "items": self.configmaps}
        return super().kubernetes(command)


def add_system_daemonset(fake, name, *, windows=False):
    daemonset = copy.deepcopy(fake.get_controller("azure-cns", "DaemonSet"))
    daemonset["metadata"] = base.metadata(name, "kube-system")
    daemonset["spec"]["selector"] = {"matchLabels": {"k8s-app": name}}
    daemonset["spec"]["template"]["spec"]["nodeSelector"] = {"kubernetes.io/os": "windows" if windows else "linux"}
    fake.controllers.append(daemonset)
    if not windows:
        pod = next(row for row in fake.pods if row["metadata"]["name"].startswith("old-terminating-system-"))
        pod["metadata"].update(name=f"{name}-old-prom", uid=base.uid(f"{name}-old-prom"))
        pod["metadata"].pop("deletionTimestamp", None)
        pod["metadata"]["labels"] = {"k8s-app": name}
        pod["metadata"]["ownerReferences"] = [base.reference("DaemonSet", name, recovery.object_uid(daemonset))]


@pytest.fixture(name="environment")
def capacity_environment(tmp_path, monkeypatch):
    monkeypatch.setattr(native_tests, "ReplacementCloud", CapacityCloud)
    args, plan, fake = native_tests.replacement_environment.__wrapped__(tmp_path, monkeypatch)
    add_system_daemonset(fake, "cloud-node-manager")
    add_system_daemonset(fake, "ama-metrics-node", windows=True)
    for pool in fake.pools:
        pool["vmSize"] = "Standard_D8_v3"
    for vmss in fake.vmsses:
        vmss["sku"]["name"] = "Standard_D8_v3"
    accepted = json.loads(Path(args.replace_failed_host).read_text(encoding="utf-8"))
    accepted["arm_metadata"]["pools"] = {
        row["name"]: {"configuration_sha256": recovery.digest(recovery.prepared.pool_configuration(row))}
        for row in fake.pools
    }
    native_tests.save_checkpoint(args, accepted)
    fake.scale_error = QUOTA_ERROR
    environment = args, plan, fake
    with pytest.raises(recovery.workers.ReconcileError, match="ErrCode_InsufficientVCPUQuota"):
        native_tests.run(environment, execute=True)
    original = base.receipt()
    assert original["replacement"]["delete"]["accepted"] is True
    assert original["replacement"]["restore"]["accepted"] is None
    assert original["replacement"]["native_removal"]["old_node_pods_nnc_absent"] is True
    args.resume_replacement = "native.json"
    Path(args.resume_replacement).write_bytes(Path(args.summary_file).read_bytes())
    args.quota_wait_seconds = 0
    fake.continuing = True
    fake.scale_error = None
    fake.commands.clear()
    fake.writes.clear()
    fake.deleted.clear()
    return environment


def run(environment, *, execute=False):
    return base.run(environment, execute=execute)


def source(environment):
    return json.loads(Path(environment[0].resume_replacement).read_text(encoding="utf-8"))


def write_source(environment, receipt):
    Path(environment[0].resume_replacement).write_text(json.dumps(receipt), encoding="utf-8")


def no_writes(fake):
    assert not fake.writes and not fake.deleted and not fake.new_scales
    assert not base.receipt()["mutation_started"]


@pytest.mark.parametrize("ready", [False, True])
def test_plan_is_zero_write_even_when_quota_is_pending(environment, ready):
    args, plan, fake = environment
    original = copy.deepcopy(plan)
    native = source(environment)
    if not ready:
        fake.usage[0]["limit"] = 800
    summary = run(environment)
    assert summary["success"] and summary["plan_valid"] and summary["status"] == "plan_valid"
    assert summary["quota_ready"] is ready
    assert summary["capacity_resume"]["previous_restore_disambiguation"]["outcome"] == "rejected"
    assert summary["replacement"]["previous_restore"] == native["replacement"]["restore"]
    assert summary["replacement"]["restore"]["attempted"] is False
    assert summary["replacement"]["delete"] == native["replacement"]["delete"]
    assert summary["original_identity"] == native["original_identity"]
    assert summary["controller_pins"] == native["controller_pins"]
    assert summary["pdb_pins"] == native["pdb_pins"]
    assert summary["original_model_pins"] == native["original_model_pins"]
    assert summary["capacity_resume"]["default_boot_evidence_origin"] == "this-continuation-zero-snapshot"
    assert summary["planned_actions"]["pool_counts"] == [0, 1]
    assert not summary["planned_actions"]["delete_required"]
    assert not summary["repaired"] and not summary["workloads_ready"]
    assert len(summary["authoritative_identities"]) == 100
    assert plan == original and json.loads(Path(args.plan_file).read_text(encoding="utf-8")) == original
    no_writes(fake)


@pytest.mark.parametrize("path,value", [
    (("plan_sha256",), "f" * 64),
    (("execute",), False),
    (("phase1_only",), False),
    (("workloads_ready",), True),
    (("pod_moves",), [{"delete_attempted": True}]),
    (("temporary_exclusions",), [{"name": recovery.SOURCE_NODE}]),
    (("probe_cleanup_pending",), {}),
    (("replacement_derived_identity",), {}),
    (("replacement_derived_manifest",), {}),
    (("replacement", "replacement_completed"), True),
    (("replacement", "delete", "accepted"), None),
    (("replacement", "delete", "attempted"), False),
    (("replacement", "delete", "ambiguous"), True),
    (("replacement", "delete", "requested_at"), "not-a-date"),
    (("replacement", "restore", "attempted"), False),
    (("replacement", "restore", "accepted"), False),
    (("replacement", "restore", "accepted"), True),
    (("replacement", "restore", "ambiguous"), False),
    (("replacement", "restore", "requested_at"), "2000-01-01T00:00:00Z"),
    (("replacement", "native_removal", "pool_count"), False),
    (("replacement", "native_removal", "vmss_capacity"), 1),
    (("replacement", "native_removal", "manual_marker_clearance"), True),
    (("replacement", "native_removal", "original_marker_removed_by"), "manual"),
    (("replacement", "native_removal", "old_node_pods_nnc_absent"), False),
    (("replacement", "native_removal", "verified_at"), "2000-01-01T00:00:00Z"),
    (("replacement", "native_removal", "verified_at"), "2999-01-01T00:00:00Z"),
    (("replacement", "removal_observation", "old_resources_absent"), False),
    (("replacement", "accepted_reimage_lineage", "vm_id"), base.uid("wrong-lineage")),
    (("replacement", "marker", "accepted_reimage_token"), base.uid("wrong-marker")),
    (("replacement", "marker", "token"), ""),
    (("replacement", "marker", "accepted_reimage_marker_sha256"), "d" * 64),
    (("replacement", "marker_write", "accepted"), False),
    (("original_identity", "vm_id"), base.uid("foreign-vm")),
    (("original_identity", "node_uid"), base.uid("foreign-node")),
    (("original_identity", "nnc_uid"), "not-a-uid"),
    (("original_identity", "host_pod_uids"), {}),
    (("original_identity", "network_container_id"), base.uid("foreign-nc")),
    (("original_identity", "provider_id"), "azure://other-vm"),
    (("controller_pins",), []),
    (("controller_pins", "DaemonSet/kube-system/cilium", "spec_sha256"), "bad"),
    (("pdb_pins",), {}),
    (("original_model_pins", "defaults"), {}),
    (("original_model_pins", "pools", "prompool"), "bad"),
    (("effective_targets",), []),
    (("planned_actions", "pods"), []),
    (("planned_actions", "machine_names"), [recovery.SOURCE_NODE]),
    (("planned_actions", "pool"), "default"),
    (("finished_at",), "2000-01-01T00:00:00Z"),
])
def test_malformed_or_later_receipts_stop_before_reads(environment, path, value):
    receipt = source(environment)
    parent = receipt
    for part in path[:-1]:
        parent = parent[part]
    parent[path[-1]] = value
    write_source(environment, receipt)
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    fake = environment[2]
    no_writes(fake)
    assert not fake.commands


@pytest.mark.parametrize("old,new", [
    ("ErrCode_InsufficientVCPUQuota", "Failed"),
    ("nodepool scale", "nodepool delete-machines"),
    ("--name prompool", "--name default"),
    ("--node-count 1", "--node-count 2"),
    (recovery.RESOURCE_GROUP, "another-rg"),
    (recovery.SUBSCRIPTION, base.uid("other-subscription")),
    ("requested 8", "requested 16"),
    ("remaining 0", "remaining 8"),
    ("standardDv3Family", "standardDSv5Family"),
    ("eastus2euap.", "westus2."),
])
def test_only_the_exact_prior_scale_quota_error_is_eligible(environment, old, new):
    receipt = source(environment)
    receipt["error"] = receipt["error"].replace(old, new)
    write_source(environment, receipt)
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    no_writes(environment[2])


def test_original_reimage_receipt_is_still_required(environment):
    args, _, fake = environment
    prior = json.loads(Path(args.replace_failed_host).read_text(encoding="utf-8"))
    prior["restart"]["accepted"] = None
    native_tests.save_checkpoint(args, prior)
    with pytest.raises(recovery.workers.ReconcileError, match="exact owned reimage"):
        run(environment, execute=True)
    no_writes(fake)
    assert not fake.commands


def test_duplicate_receipt_json_keys_are_not_silent_overrides(environment):
    args, _, fake = environment
    path = Path(args.resume_replacement)
    path.write_text('{"execute": false,' + path.read_text(encoding="utf-8")[1:], encoding="utf-8")
    with pytest.raises(recovery.workers.ReconcileError, match="duplicate"):
        run(environment, execute=True)
    no_writes(fake)


@pytest.mark.parametrize("fault", [
    "pool-one", "pool-failed", "pool-scaling", "pool-power", "vmss-one", "vmss-creating",
    "vmss-missing", "vmss-status-unknown", "vmss-status-nonzero", "actual-new-vm",
    "new-node", "old-node", "old-nnc", "old-pod", "operation-busy", "operation-failed", "operation-unknown",
    "default-vm", "default-vm-failed", "default-node", "default-pool", "kwok", "mock", "ready-mock",
    "controller", "pdb", "dns-sibling", "unpinned-pending", "fleet", "exclusion", "source-network",
])
def test_nonempty_busy_unknown_or_drifted_zero_never_restores(environment, fault):
    _, plan, fake = environment
    if fault == "pool-one":
        fake.pools[1]["count"] = 1
    elif fault in ("pool-failed", "pool-scaling"):
        fake.pools[1]["provisioningState"] = "Failed" if fault.endswith("failed") else "Scaling"
    elif fault == "pool-power":
        fake.pools[1]["powerState"]["code"] = "Stopped"
    elif fault == "vmss-one":
        fake.vmsses[1]["sku"]["capacity"] = 1
    elif fault == "vmss-creating":
        fake.vmsses[1]["provisioningState"] = "Creating"
    elif fault == "vmss-missing":
        fake.vmsses.pop()
    elif fault == "vmss-status-unknown":
        fake.scale_view["statuses"] = None
    elif fault == "vmss-status-nonzero":
        fake.scale_view["virtualMachines"] = [{"code": "ProvisioningState/succeeded", "count": 1}]
    elif fault == "actual-new-vm":
        fake.finish_restoration()
    elif fault in ("new-node", "old-node"):
        name = native_tests.NEW_NODE if fault == "new-node" else recovery.PROM_NODE
        row_uid = native_tests.NEW_NODE_UID if fault == "new-node" else recovery.REAL_UIDS[name]
        fake.nodes[name] = base.make_node(name, row_uid, pool="prompool")
    elif fault == "old-nnc":
        row = copy.deepcopy(fake.nncs[0])
        row["status"]["networkContainers"][0]["id"] = native_tests.replacement.FAILED_NETWORK_CONTAINER
        fake.nncs.append(row)
    elif fault == "old-pod":
        fake.pods.append(copy.deepcopy(fake.old_system_pods[0]))
    elif fault.startswith("operation-"):
        fake.operation.update(status={"operation-busy": "InProgress", "operation-failed": "Failed",
                                      "operation-unknown": "Unknown"}[fault], endTime=None)
    elif fault == "default-vm":
        fake.instances[recovery.DEFAULT_VMSS][0]["vmId"] = base.uid("other-default-vm")
    elif fault == "default-vm-failed":
        fake.instances[recovery.DEFAULT_VMSS][0]["provisioningState"] = "Failed"
    elif fault == "default-node":
        fake.nodes[recovery.SOURCE_NODE]["metadata"]["uid"] = base.uid("other-default-node")
    elif fault == "default-pool":
        fake.pools[0]["mode"] = "User"
    elif fault == "kwok":
        fake.nodes["kwok-node-99"]["metadata"]["uid"] = base.uid("other-kwok")
    elif fault == "mock":
        fake.get_pod("kwok-node-99")["metadata"]["uid"] = base.uid("other-mock")
    elif fault == "ready-mock":
        fake.get_pod("kwok-node-0")["status"] = base.ready_status(False)
    elif fault == "controller":
        fake.get_controller("grafana")["spec"]["replicas"] = 2
    elif fault == "pdb":
        fake.pdbs[0]["spec"]["minAvailable"] = 0
    elif fault == "dns-sibling":
        fake.get_pod(f"{recovery.DNS_REPLICA_SET}-healthy-2")["status"] = base.ready_status(False)
    elif fault == "unpinned-pending":
        fake.get_pod(plan["api_pod_name"])["metadata"]["uid"] = base.uid("unpinned-api")
    elif fault == "fleet":
        fake.members[0]["meshProperties"]["ciliumProperties"]["name"] = "changed-identity"
    elif fault == "exclusion":
        fake.nodes[recovery.SOURCE_NODE]["spec"]["taints"] = [
            {"key": recovery.EXCLUSION_KEY, "value": "foreign", "effect": "NoSchedule"},
        ]
    else:
        fake.nncs[0]["status"]["networkContainers"][0]["id"] = base.uid("changed-source-nc")
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    summary = base.receipt()
    assert summary["capacity_resume"]["fresh_state"]["outcome"] == "not-proven"
    assert summary["capacity_resume"]["previous_restore_disambiguation"] is None
    if fault in ("controller", "pdb"):
        drift = summary["capacity_resume"]["pin_drift"]
        kind = "controllers" if fault == "controller" else "pdbs"
        assert len(drift[kind]) == 1
        changed = next(iter(drift[kind].values()))
        assert changed["expected"]["uid"] == changed["observed"]["uid"]
        assert changed["expected"]["spec_sha256"] != changed["observed"]["spec_sha256"]
        if fault == "controller":
            configuration = next(iter(drift["current_controller_configuration"].values()))
            template = fake.get_controller("grafana")["spec"]["template"]
            assert configuration["replicas"] == 2
            assert configuration["template_metadata"] == template.get("metadata", {})
            assert configuration["pod_spec_sha256"] == recovery.digest(template["spec"])
            assert all(set(row) == {"name", "image", "resources"} for row in configuration["containers"])
        else:
            assert next(iter(drift["current_pdb_specs"].values()))["minAvailable"] == 0
    no_writes(fake)


@pytest.mark.parametrize("change", ["added", "removed", "uid"])
def test_controller_pin_drift_preserves_inventory_evidence(environment, change):
    _, _, fake = environment
    row = fake.get_controller("grafana")
    key = f"{row['kind']}/{row['metadata']['namespace']}/{row['metadata']['name']}"
    if change == "removed":
        fake.controllers.remove(row)
    elif change == "added":
        row = copy.deepcopy(row)
        row["metadata"]["name"] = "new-unapproved-controller"
        row["metadata"]["uid"] = base.uid("new-unapproved-controller")
        fake.controllers.append(row)
        key = f"{row['kind']}/{row['metadata']['namespace']}/{row['metadata']['name']}"
    else:
        row["metadata"]["uid"] = base.uid("replaced-controller")
    with pytest.raises(recovery.workers.ReconcileError, match="controller/PDB pins changed"):
        run(environment, execute=True)
    drift = base.receipt()["capacity_resume"]["pin_drift"]
    assert list(drift["controllers"]) == [key]
    assert not drift["pdbs"]
    assert (drift["controllers"][key]["observed"] is None) is (change == "removed")
    assert (drift["controllers"][key]["expected"] is None) is (change == "added")
    no_writes(fake)


@pytest.mark.parametrize("counter", [0, 1])
def test_insufficient_family_or_total_quota_deadline_has_zero_writes(environment, counter):
    _, _, fake = environment
    fake.usage[counter]["limit"] = fake.usage[counter]["currentValue"] + 7
    with pytest.raises(recovery.workers.ReconcileError, match="quota"):
        run(environment, execute=True)
    assert not base.receipt()["quota_ready"]
    assert base.receipt()["replacement"]["previous_restore"]["ambiguous"] is True
    no_writes(fake)


@pytest.mark.parametrize("fault", ["missing-family", "missing-total", "duplicate", "bool", "negative", "string", "null"])
def test_quota_api_shape_is_strict(environment, fault):
    _, _, fake = environment
    if fault == "missing-family":
        fake.usage.pop(0)
    elif fault == "missing-total":
        fake.usage.pop()
    elif fault == "duplicate":
        fake.usage.append(copy.deepcopy(fake.usage[0]))
    elif fault == "bool":
        fake.usage[0]["currentValue"] = False
    elif fault == "negative":
        fake.usage[0]["currentValue"] = -1
    elif fault == "string":
        fake.usage[0]["limit"] = "808.0"
    else:
        fake.usage[0]["currentValue"] = None
    with pytest.raises(recovery.workers.ReconcileError, match="quota"):
        run(environment, execute=True)
    no_writes(fake)


@pytest.mark.parametrize("value,expected", [(0, 0), (5464, 5464), ("0", 0), ("5464", 5464), ("008", 8)])
def test_quota_counters_accept_only_integers_and_unsigned_decimal_strings(value, expected):
    assert resume.quota_counter(value) == expected


@pytest.mark.parametrize("value", [
    True, False, 8.0, 0.0, -1, None, "", " 8", "8 ", "8\n", "+8", "-8", "8.0", "8e2", "８", "٨", [], {},
])
def test_quota_counters_reject_coercible_but_invalid_values(value):
    with pytest.raises(recovery.workers.ReconcileError, match="quota counter"):
        resume.quota_counter(value)


@pytest.mark.parametrize("execute", [False, True])
def test_observed_decimal_string_family_deficit_never_opens_write_gate(environment, execute):
    _, _, fake = environment
    fake.usage[0].update(limit="5000", currentValue="5464")
    fake.usage[1].update(limit="11897", currentValue="8690")
    original_usage = copy.deepcopy(fake.usage)
    if execute:
        with pytest.raises(recovery.workers.ReconcileError, match="quota"):
            run(environment, execute=True)
        summary = base.receipt()
    else:
        summary = run(environment)
        assert summary["plan_valid"] and summary["success"]
    counters = summary["capacity_resume"]["quota"]["counters"]
    assert counters[resume.QUOTA_FAMILY] == {
        "name": resume.QUOTA_FAMILY, "limit": 5000, "currentValue": 5464, "remaining": -464,
    }
    assert counters["cores"]["remaining"] == 3207
    assert not summary["quota_ready"] and not summary["capacity_resume"]["quota_ready"]
    assert not summary["replacement"]["restore"]["attempted"] and not fake.configmaps
    assert fake.usage == original_usage
    no_writes(fake)


@pytest.mark.parametrize("execute", [False, True])
def test_decimal_string_quota_can_authorize_only_the_normal_gated_path(environment, execute):
    _, _, fake = environment
    for row in fake.usage:
        row["limit"] = str(row["limit"])
        row["currentValue"] = str(row["currentValue"])
    summary = run(environment, execute=execute)
    assert summary["quota_ready"]
    assert all(row["remaining"] == 8 for row in summary["capacity_resume"]["quota"]["counters"].values())
    if execute:
        assert summary["repaired"] and len(fake.new_scales) == 1
    else:
        no_writes(fake)


def test_quota_projection_discards_localized_names_and_unrelated_metadata():
    raw = [{"name": {"value": "cores", "localizedValue": "localized"}, "currentValue": 21, "limit": 29,
            "unit": "Count", "private": "not-retained"}]
    assert base.jmespath.search(resume.USAGE_QUERY, raw) == [{"name": "cores", "currentValue": 21, "limit": 29}]


def test_success_has_one_restore_all_system_daemonsets_and_strict_framework_proof(environment):
    args, plan, fake = environment
    files = [Path(args.plan_file), Path(args.replace_failed_host), Path(args.resume_replacement)]
    original_bytes = [path.read_bytes() for path in files]
    native = source(environment)
    protected_specs = {name: copy.deepcopy(fake.nodes[name]["spec"])
                       for name in (recovery.SOURCE_NODE, f"{recovery.DEFAULT_VMSS}000001")}
    summary = run(environment, execute=True)
    assert summary["repaired"] and summary["success"] and summary["phase1_only"] and not summary["workloads_ready"]
    assert len(fake.new_scales) == 1 and [row for row in fake.writes if row[0] == "az"] == fake.new_scales
    assert summary["replacement"]["previous_restore"] == native["replacement"]["restore"]
    assert summary["replacement"]["restore"]["accepted"] is True
    assert summary["replacement"]["delete"] == native["replacement"]["delete"]
    assert summary["replacement"]["replacement_completed"]
    assert summary["replacement_derived_identity"]["node_uid"] == native_tests.NEW_NODE_UID
    assert summary["replacement_derived_identity"]["vm_id"] == native_tests.NEW_VM_ID
    assert summary["replacement_derived_identity"]["network_container_id"] == native_tests.NEW_NC
    systems = summary["capacity_resume"]["system_daemonsets"]
    assert set(systems) == {"cilium", "azure-cns", "cloud-node-manager"}
    assert all(row["ready"] for row in systems.values())
    assert [row["deployment_name"] for row in summary["pod_moves"]] == [
        "coredns", "coredns", "clustermesh-apiserver", "kube-state-metrics", "grafana",
    ]
    reserves = summary["memory_commitments"]
    for name in ("clustermesh-apiserver", "grafana"):
        assert any(name in key and row["reserved_memory_bytes"] == 8 * 1024**3 for key, row in reserves.items())
    assert len([row for row in fake.writes if "run" in row]) == 5
    assert summary["cilium_proof"]["healthy"] and summary["cilium_proof"]["cilium_agent_count"] == 3
    assert summary["fleet_connected"] and summary["final_mock_ready"] == 71 and summary["final_kwok_ready"] == 100
    assert not summary["temporary_exclusions"] and not summary["cleanup_errors"]
    assert len(fake.configmaps) == 1
    journal = json.loads(fake.configmaps[0]["data"]["record"])
    assert journal["state"] == "completed" and journal["restore"]["accepted"] is True
    assert summary["capacity_resume"]["attempt_guard"]["retained_owned_non_workload_record"]
    assert [path.read_bytes() for path in files] == original_bytes
    assert all(fake.nodes[name]["spec"] == spec for name, spec in protected_specs.items())
    assert all(fake.nodes[name]["metadata"]["uid"] == row_uid for name, row_uid in plan["kwok_node_uids"].items())
    assert all(fake.get_pod(name)["metadata"]["uid"] == row_uid for name, row_uid in plan["mock_pod_uids"].items())
    assert all(recovery.pod_ready(fake.get_pod(name)) for name in plan["ready_mock_pod_uids"])
    assert not any("patch" in row and recovery.PROM_NODE in row for row in fake.writes)


def test_wait_for_quota_is_read_only_then_rechecks_all_gates(environment, monkeypatch):
    args, _, fake = environment
    args.quota_wait_seconds = 900
    fake.usage[0]["limit"] = 800
    waits = []

    def release(operator, deadline, description):
        assert deadline <= operator.work_deadline
        waits.append(description)
        no_writes(fake)
        fake.usage[0]["limit"] = 808

    monkeypatch.setattr(recovery.Recovery, "wait", release)
    assert run(environment, execute=True)["repaired"]
    assert len(waits) == 1 and len(fake.new_scales) == 1


@pytest.mark.parametrize("drift", ["default-boot", "controller", "pdb", "healthy-mock", "source-file"])
def test_quota_wait_does_not_waive_scope_or_immutable_sources(environment, monkeypatch, drift):
    args, _, fake = environment
    args.quota_wait_seconds = 900
    fake.usage[0]["limit"] = 800

    def release(_operator, _deadline, _description):
        fake.usage[0]["limit"] = 808
        if drift == "default-boot":
            fake.nodes[recovery.SOURCE_NODE]["status"]["nodeInfo"]["bootID"] = base.uid("new-default-boot")
        elif drift == "controller":
            fake.get_controller("grafana")["metadata"]["uid"] = base.uid("new-controller")
        elif drift == "pdb":
            fake.pdbs[0]["metadata"]["uid"] = base.uid("new-pdb")
        elif drift == "healthy-mock":
            fake.get_pod("kwok-node-0")["status"] = base.ready_status(False)
        else:
            Path(args.resume_replacement).write_text("{}", encoding="utf-8")

    monkeypatch.setattr(recovery.Recovery, "wait", release)
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    no_writes(fake)


@pytest.mark.parametrize("kind", ["foreign", "reserved", "attempted", "completed"])
def test_every_existing_guard_blocks_another_attempt(environment, kind):
    _, _, fake = environment
    fake.configmaps = [{"metadata": base.metadata(resume.GUARD_NAME, "kube-system"),
                       "data": {"owner": recovery.OWNER if kind != "foreign" else "foreign", "state": kind}}]
    with pytest.raises(recovery.workers.ReconcileError, match="existing capacity attempt"):
        run(environment, execute=True)
    no_writes(fake)


@pytest.mark.parametrize("fault", ["create-ambiguous", "conflict", "patch-ambiguous", "scale-ambiguous"])
def test_ambiguous_owned_attempts_are_retained_and_block_replay(environment, fault):
    _, _, fake = environment
    fake.create_error = fault == "create-ambiguous"
    fake.create_conflict = fault == "conflict"
    fake.patch_error = "attempted" if fault == "patch-ambiguous" else None
    fake.resume_scale_error = "native scale response lost" if fault == "scale-ambiguous" else None
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    summary = base.receipt()
    assert not summary["repaired"] and not fake.deleted and len(fake.configmaps) == 1
    assert len(fake.new_scales) == (1 if fault == "scale-ambiguous" else 0)
    if fault == "scale-ambiguous":
        assert summary["replacement"]["restore"]["accepted"] is None
        assert summary["replacement"]["restore"]["ambiguous"] is True
    before = len(fake.writes)
    with pytest.raises(recovery.workers.ReconcileError, match="existing capacity attempt"):
        run(environment, execute=True)
    assert len(fake.writes) == before


@pytest.mark.parametrize("fault", ["uid", "token", "record", "disappeared"])
def test_owned_guard_identity_is_rechecked_before_native_scale(environment, fault):
    _, _, fake = environment
    changed = []

    def change(command):
        if "get" in command and "configmaps" in command and fake.configmaps and not changed:
            changed.append(True)
            if fault == "uid":
                fake.configmaps[0]["metadata"]["uid"] = base.uid("other-guard")
            elif fault == "token":
                fake.configmaps[0]["data"]["token"] = base.uid("other-token")
            elif fault == "record":
                fake.configmaps[0]["data"]["record"] = "{}"
            else:
                fake.configmaps.clear()

    fake.hook = change
    with pytest.raises(recovery.workers.ReconcileError, match="guard"):
        run(environment, execute=True)
    assert changed and not fake.new_scales and not fake.deleted


@pytest.mark.parametrize("phase", [2, 3, 4])
def test_quota_is_freshly_rechecked_before_each_write_boundary(environment, phase):
    _, _, fake = environment
    reads = []

    def exhaust(command):
        if command[1:3] == ["vm", "list-usage"]:
            reads.append(True)
            if len(reads) == phase:
                fake.usage[1]["limit"] = fake.usage[1]["currentValue"]

    fake.hook = exhaust
    with pytest.raises(recovery.workers.ReconcileError, match="Quota headroom disappeared"):
        run(environment, execute=True)
    assert not fake.new_scales and not fake.deleted
    if phase == 2:
        no_writes(fake)
    else:
        assert len(fake.configmaps) == 1


@pytest.mark.parametrize("fault", [
    "missing-system", "unready-system", "wrong-owner", "duplicate-system", "default-boot",
    "old-node", "old-vm-id", "old-nc", "uninitialized-nc", "not-ready", "unschedulable",
    "image", "guest-failed", "missing-extensions", "not-latest",
])
def test_new_host_needs_distinct_identity_and_every_applicable_system_daemonset(environment, monkeypatch, fault):
    _, _, fake = environment

    def restore_badly():
        fake.finish_restoration()
        name = native_tests.NEW_NODE
        node = fake.nodes[name]
        vm = fake.instances[recovery.PROM_VMSS][0]
        network = next(row for row in fake.nncs if row["metadata"]["name"] == name)
        system = fake.get_pod("cloud-node-manager-new-prom")
        view = fake.views[(recovery.PROM_VMSS, native_tests.NEW_INSTANCE)]
        if fault == "missing-system":
            fake.pods.remove(system)
        elif fault == "unready-system":
            system["status"] = base.ready_status(False)
        elif fault == "wrong-owner":
            system["metadata"]["ownerReferences"][0]["uid"] = base.uid("wrong-controller")
        elif fault == "duplicate-system":
            other = copy.deepcopy(system)
            other["metadata"].update(name="duplicate-system", uid=base.uid("duplicate-system"))
            fake.pods.append(other)
        elif fault == "default-boot":
            fake.nodes[recovery.SOURCE_NODE]["status"]["nodeInfo"]["bootID"] = base.uid("default-reboot")
        elif fault == "old-node":
            node["metadata"]["uid"] = recovery.REAL_UIDS[recovery.PROM_NODE]
        elif fault == "old-vm-id":
            vm["vmId"] = recovery.FAILED_PROM_VM_ID
        elif fault == "old-nc":
            network["status"]["networkContainers"][0]["id"] = native_tests.replacement.FAILED_NETWORK_CONTAINER
        elif fault == "uninitialized-nc":
            network["status"]["assignedIPCount"] = 0
        elif fault == "not-ready":
            node["status"]["conditions"][0]["status"] = "False"
        elif fault == "unschedulable":
            node["spec"]["unschedulable"] = True
        elif fault == "image":
            node["metadata"]["labels"]["kubernetes.azure.com/node-image-version"] = "other-image"
        elif fault == "guest-failed":
            view["extensions"][0]["statuses"] = [{"code": "ProvisioningState/failed"}]
        elif fault == "missing-extensions":
            view["extensions"] = None
        else:
            vm["latestModelApplied"] = False

    fake.on_scale = restore_badly
    base.abort_wait(monkeypatch)
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    assert len(fake.new_scales) == 1 and not fake.deleted and len(fake.configmaps) == 1
    assert not base.receipt()["repaired"]
    assert json.loads(fake.configmaps[0]["data"]["record"])["state"] == "attempted"


def test_creating_instance_views_are_observed_without_learning_empty_extensions(environment, monkeypatch):
    _, _, fake = environment

    def creating():
        fake.finish_restoration()
        fake.instances[recovery.PROM_VMSS][0].update(provisioningState="Creating", latestModelApplied=None)
        fake.views[(recovery.PROM_VMSS, native_tests.NEW_INSTANCE)] = {"statuses": None, "extensions": None}
        fake.operation.update(status="InProgress", operationType="ScaleAgentPool", startTime=base.now(), endTime=None)
        fake.pools[1]["provisioningState"] = "Scaling"
        fake.vmsses[1]["provisioningState"] = "Updating"
        fake.scale_view["statuses"] = [{"code": "ProvisioningState/updating"}]
        fake.scale_view["virtualMachines"] = [{"code": "ProvisioningState/creating", "count": 1}]

    def finish(operator, _deadline, _description):
        assert operator.stage == "restoring" and operator.derived is None and operator.extension_names is None
        assert len(fake.new_scales) == 1 and not fake.deleted
        fake.operation.update(status="Succeeded", endTime=base.now())
        # Keep already created identities and Pods rather than creating duplicates.
        fake.pools[1]["provisioningState"] = fake.vmsses[1]["provisioningState"] = "Succeeded"
        fake.instances[recovery.PROM_VMSS][0].update(provisioningState="Succeeded", latestModelApplied=True)
        fake.views[(recovery.PROM_VMSS, native_tests.NEW_INSTANCE)] = {
            "statuses": [{"code": "ProvisioningState/succeeded"}, {"code": "PowerState/running"}],
            "extensions": [{"name": "vmssCSE", "statuses": [{"code": "ProvisioningState/succeeded"}]}],
        }
        fake.scale_view = {"statuses": [{"code": "ProvisioningState/succeeded"}],
                           "virtualMachines": [{"code": "ProvisioningState/succeeded", "count": 1}]}

    fake.on_scale = creating
    monkeypatch.setattr(recovery.Recovery, "wait", finish)
    assert run(environment, execute=True)["repaired"]
    assert len(fake.new_scales) == 1


@pytest.mark.parametrize("fault", ["memory", "cpu", "probe", "cilium-peers", "fleet", "completion-journal"])
def test_capacity_ip_or_final_proof_failure_never_claims_repaired(environment, monkeypatch, fault):
    _, _, fake = environment
    if fault == "memory":
        fake.memory = "23Gi"
    elif fault == "cpu":
        fake.cpu = "8"
    elif fault == "probe":
        fake.probe_ready = False
    elif fault == "cilium-peers":
        fake.peer_fault = "disconnected"
    elif fault == "fleet":
        fake.fleet_stuck = True
    else:
        fake.patch_error = "completed"
    base.abort_wait(monkeypatch)
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    summary = base.receipt()
    assert len(fake.new_scales) == 1 and len(fake.configmaps) == 1
    assert not summary["success"] and not summary["repaired"] and not summary["workloads_ready"]
    assert not summary["temporary_exclusions"]
    assert not summary.get("probe_cleanup_pending")


def test_already_healthy_owned_target_is_adopted_but_never_deleted(environment):
    _, plan, fake = environment
    original = fake.get_pod(plan["api_pod_name"])
    original["metadata"].update(name="already-healthy-owned-api", uid=base.uid("already-healthy-owned-api"))
    original["spec"]["nodeName"] = recovery.SOURCE_NODE
    original["status"] = base.ready_status(True)
    fake.members[95]["meshProperties"]["status"] = {"state": "Connected"}
    summary = run(environment, execute=True)
    assert summary["repaired"]
    api = next(row for row in summary["pod_moves"] if row["deployment_name"] == "clustermesh-apiserver")
    assert not api["delete_attempted"] and api["ready_pod_uid"] == recovery.object_uid(original)
    assert not any(name in (plan["api_pod_name"], "already-healthy-owned-api") for _, name, _ in fake.deleted)


def test_empty_azure_singular_vm_summary_and_seven_digit_timestamps(environment, monkeypatch):
    _, _, fake = environment
    fake.scale_view["virtualMachines"] = None
    arguments = base.use_python310_datetime(monkeypatch)
    fake.operation["startTime"] = fake.operation["endTime"] = "2026-09-04T00:10:34.1234567Z"
    summary = run(environment)
    assert summary["plan_valid"]
    assert summary["arm_metadata"]["vm_summary_absent_for_empty_inventory"]
    assert "2026-09-04T00:10:34.123456+00:00" in arguments
    no_writes(fake)
