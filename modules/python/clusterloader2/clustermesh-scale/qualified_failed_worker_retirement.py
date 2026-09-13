#!/usr/bin/env python3
"""One native default1 retirement after genuine completed cniv5 qualification."""

# pylint: disable=protected-access,too-many-lines,too-many-boolean-expressions

from __future__ import annotations

import argparse
import copy
import json
import signal
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import capacity_first_qualification as qualification
import capacity_first_worker_recovery as capacity
import cni_worker_maintenance as maintenance
import mock_cni_recovery as mocks
import stalled_retained_worker_recovery as stalled


base = qualification.base
workers = qualification.workers
require = qualification.require
uid = base.object_uid
digest = base.digest
TARGET = stalled.TARGET
SOURCE = stalled.SOURCE
JOURNAL = "mesh96-qualified-default1-retirement"
HOLD_KEY = "mock-clustermesh/qualified-default1-retirement"
QUALIFICATION_BUILD = 79986
RESERVE_SECONDS = 180
EXPECTED_ERRORS = qualification.EXPECTED_ERRORS


def delete_command():
    return ["az", "aks", "nodepool", "delete-machines", "--resource-group", base.RESOURCE_GROUP,
            "--cluster-name", base.CLUSTER, "--name", "default", "--machine-names", TARGET,
            "--no-wait", "--only-show-errors"]


def load_inputs(args):
    hashes = qualification.hash_tree(args.qualification_directory)
    require({"qualification.json", "prior-qualification.json"} <= set(hashes), "Whole completed qualification artifact required")
    root = Path(args.qualification_directory)
    internal = SimpleNamespace(**vars(args))
    internal.observation_directory = str(root / "observation")
    internal.capacity_directory = str(root / "capacity-input")
    internal.observation_build_id, internal.capacity_build_id = 79975, 79971
    internal.completed_qualification_checkpoint = str(root / "prior-qualification.json")
    inputs = qualification.load_inputs(internal)
    prior, prior_hash = qualification.load_completed_proof(internal, inputs)
    receipt = stalled.read_json(root / "qualification.json")
    require(receipt.get("success") is True and receipt.get("capacity_qualified") is True
            and receipt.get("actual_ip_growth_proven") is True and receipt.get("actual_memory_headroom_proven") is True
            and receipt.get("completion_only") is True and receipt.get("execute") is False
            and receipt.get("mutation_started") is False and receipt.get("workloads_ready") is False
            and receipt.get("bootstrap_complete") is False and receipt.get("probe_cleanup_pending") == []
            and receipt.get("cleanup_errors") == []
            and receipt.get("completed_probe_build") == 79979
            and receipt.get("completed_probe_checkpoint_sha256") == prior_hash
            and receipt.get("input_hashes") == {"observation": inputs["observation_hashes"], "capacity": inputs["capacity_hashes"]}
            and receipt.get("identities") == inputs["identities"]
            and receipt.get("journal", {}).get("uid") == qualification.COMPLETED_JOURNAL_UID
            and receipt["journal"].get("name") == qualification.JOURNAL
            and receipt.get("plan_sha256") == stalled.PLAN_SHA,
            "Only the genuine read-only 79986 completion of clean 79979 probes may authorize retirement")
    require(receipt.get("ip_growth") == prior.get("ip_growth")
            and receipt.get("current_kwok_ready") == 100 and receipt.get("current_mock_ready") == 44
            and receipt.get("protected_healthy_mock_uids") == inputs["healthy_agents"]
            and receipt.get("remaining_replacement_uids") == inputs["remaining_agents"]
            and receipt.get("current_mock_uids") == receipt.get("original_mock_uids"),
            "Completed qualification identity/readiness evidence changed")
    memory = receipt.get("memory_projection") or {}
    require(memory.get("remaining_count") == 56 and set(memory.get("placements") or {}) == set(inputs["remaining_agents"])
            and set(memory.get("placements", {}).values()) <= set(inputs["identities"])
            and memory.get("threshold_percent") == 85
            and base.integer(memory.get("healthy_agent_rss_high_water_bytes"))
            and memory["healthy_agent_rss_high_water_bytes"] > 0,
            "The completed qualification does not cover all 56 replacements")
    current = receipt.get("read_only_capacity_guard", {}).get("kubernetes_diagnostics")
    require(isinstance(current, dict), "Completed qualification has no current captured Kubernetes state")
    maintenance._require_all_kwok_ready(current["nodes"], receipt["preserved_kwok_uids"])
    require(stalled.controllers_pin(current["controllers"]) == stalled.controllers_pin(
        stalled.safe_diagnostics(inputs["data"]["current-controllers.json"]))
            and base.frozen_pdbs(current) == base.frozen_pdbs({"pdbs": inputs["data"]["current-pdbs.json"]}),
            "Completed qualification controller/PDB contracts changed")
    require(qualification.hash_tree(args.qualification_directory) == hashes, "Qualification bundle changed while loading")
    return {"qual_args": internal, "inputs": inputs, "prior": prior, "prior_hash": prior_hash,
            "receipt": receipt, "hashes": hashes, "current": current}


def semantic_pod_spec(spec):
    """Normalize only scheduler placement and API-generated service-account mounts."""
    result = copy.deepcopy(spec)
    result.pop("nodeName", None)
    automatic = {
        row["name"] for row in result.get("volumes") or []
        if str(row.get("name", "")).startswith("kube-api-access-")
        and isinstance(row.get("projected"), dict)
        and any("serviceAccountToken" in source for source in row["projected"].get("sources", []))
    }
    for row in result.get("volumes") or []:
        if row.get("name") in automatic:
            row["name"] = "<service-account-token>"
            for source in row["projected"].get("sources", []):
                if "serviceAccountToken" in source:
                    source["serviceAccountToken"].pop("expirationSeconds", None)
    for container in [*result.get("containers", []), *result.get("initContainers", [])]:
        for mount in container.get("volumeMounts") or []:
            if mount.get("name") in automatic:
                require(mount.get("mountPath") == "/var/run/secrets/kubernetes.io/serviceaccount"
                        and mount.get("readOnly") is True, "Unexpected service-account mount semantics")
                mount["name"] = "<service-account-token>"
    return result


class QualificationChecks(qualification.Qualification):
    """Read-only reuse of proven scope, capacity, identity and memory checks."""

    def __init__(self, args, bundle, outer, runner):
        self.outer = outer
        scratch = copy.deepcopy(bundle["receipt"])
        super().__init__(args, bundle["inputs"], scratch, runner, self.forbidden_delete,
                         completed=bundle["prior"], completed_hash=bundle["prior_hash"])
        self.rss_high = max(self.rss_high, bundle["receipt"]["memory_projection"]["healthy_agent_rss_high_water_bytes"])
        self.node_high = copy.deepcopy(bundle["receipt"]["memory_projection"]["node_high_water"])

    @staticmethod
    def forbidden_delete(*_args, **_kwargs):
        raise workers.ReconcileError("Retirement may not delete probe or production Pods")

    def save(self):
        self.outer.summary["qualification_recheck"] = {
            key: self.summary[key] for key in ("memory_projection", "read_only_capacity_guard", "all_nnc_allocations")
            if key in self.summary
        }
        self.outer.save()

    def raw_write(self, _command, *, cleanup=False):
        raise workers.ReconcileError("Retirement must not mutate existing qualification/capacity journals or probes")

    def execute(self):
        raise workers.ReconcileError("Existing qualification must not be executed again")


class Retirement(maintenance.ClusterOperator):
    """One exact native deletion; uncertain retirement retains the placement hold."""

    def __init__(self, args, bundle, summary, runner):
        deadline = time.monotonic() + args.timeout_seconds
        super().__init__(args, base.CLUSTER, runner, deadline - RESERVE_SECONDS, deadline)
        self.bundle, self.summary = bundle, summary
        self.token, self.journal_uid = uuid.uuid4().hex, ""
        self.summary["hold"]["token"] = self.token
        self.journal_data_pin = None
        self.hold_applied = False
        self.native_submitted = False
        self.fenced = False
        self.operation_name = None
        self.previous_operation = None
        self.checks = QualificationChecks(bundle["qual_args"], bundle, self, self.read_command)
        self.checks.work_deadline = self.checks.reader.work_deadline = self.work_deadline
        self.checks.cleanup_deadline = self.checks.reader.cleanup_deadline = self.cleanup_deadline
        self.node_pins = {row["metadata"]["name"]: copy.deepcopy(row) for row in bundle["current"]["nodes"]["items"]}
        self.healthy_uids = bundle["inputs"]["healthy_agents"]
        self.target_uids = bundle["inputs"]["remaining_agents"]
        self.original_agents = maintenance._agent_map(bundle["inputs"]["observation"]["current-pods.json"])
        self.replacement_uids = {}
        self.networks = qualification.allocation_map(bundle["current"]["nnc"])
        self.latest_networks = copy.deepcopy(self.networks)
        self.lease_pins = {row["name"]: row for row in bundle["receipt"]["kwok_diagnostics"]["node_leases"]["items"]}
        require(set(self.lease_pins) == maintenance.EXPECTED_AGENT_NAMES, "Completed qualification lacks all 100 Node leases")
        target_pods = {uid(pod): pod for pod in bundle["current"]["pods"]["items"] if pod["spec"].get("nodeName") == TARGET}
        raw = {uid(pod): pod for pod in bundle["inputs"]["observation"]["current-pods.json"]["items"]}
        self.target_pods = {pod_uid: raw.get(pod_uid, pod) for pod_uid, pod in target_pods.items()}
        self.target_owners = {
            (pod["metadata"]["namespace"], ref.get("kind"), ref.get("name"), ref.get("uid"))
            for pod in self.target_pods.values() for ref in pod["metadata"].get("ownerReferences", [])
            if ref.get("controller") is True
        }
        self.target_checker = SimpleNamespace(initial_pods=self.target_pods, nodes=self.node_pins,
                                             allowed_owners=self.target_owners, summary=self.summary)
        self.source_protected = {
            uid(pod): stalled.pod_pin(raw.get(uid(pod), pod)) for pod in bundle["current"]["pods"]["items"]
            if pod["spec"].get("nodeName") == SOURCE and base.pod_ready(pod)
        }
        self.old_pool_configs = {
            name: capacity.prepared.pool_configuration(pool)
            for name, pool in ((row["name"], row) for row in
                               bundle["receipt"]["read_only_capacity_guard"]["arm_diagnostics"]["pools"])
        }
        self.old_scale_models = {
            row["name"]: capacity.canonical({key: value for key, value in row.items() if key != "provisioningState"})
            for row in bundle["receipt"]["read_only_capacity_guard"]["arm_diagnostics"]["vmsses"]
        }
        for row in self.old_scale_models.values():
            row["sku"].pop("capacity", None)

    def save(self):
        mocks.write_json_atomic(self.args.summary_file, self.summary)

    def unchanged_inputs(self):
        require(qualification.hash_tree(self.args.qualification_directory) == self.bundle["hashes"],
                "Immutable completed-qualification inputs changed")

    def read_command(self, command, timeout_seconds):
        command = list(command)
        allowed = command[0] == "az" and (
            command[1:3] in (["account", "show"], ["group", "show"], ["aks", "list"], ["aks", "show"],
                            ["vmss", "list"], ["vmss", "show"], ["vmss", "list-instances"],
                            ["vmss", "get-instance-view"], ["vm", "list-usage"], ["vm", "list-skus"])
            or command[1:4] in (["fleet", "member", "list"], ["aks", "nodepool", "list"], ["aks", "operation", "show-latest"])
        )
        if command[0] == "kubectl":
            allowed = "get" in command and not any(item in command for item in ("delete", "patch", "create", "run", "exec", "drain"))
        require(allowed, "Retirement read path rejected a mutation")
        return super().run(command, min(timeout_seconds, 120), cleanup=self.cleanup_mode)

    def run(self, command, timeout_seconds=45, *, cleanup=False):
        require(not cleanup or self.cleanup_mode, "Unexpected cleanup read")
        return self.read_command(command, timeout_seconds)

    def kube(self, *command):
        return workers.parse_json(self.run(["kubectl", "--request-timeout=45s", *command]), "retirement Kubernetes read")

    def old_journals(self):
        self.checks.historical_journal()
        row = self.kube("-n", "kube-system", "get", "configmap", qualification.JOURNAL, "-o", "json")
        prior = self.bundle["prior"]
        token = next(iter(prior["probe_receipts"].values()))["token"]
        expected = {
            "owner": qualification.OWNER, "token": token, "capacity_build": "79971", "observation_build": "79975",
            "input_sha256": digest(prior["input_hashes"]), "probe_receipts": json.dumps(prior["probe_receipts"], sort_keys=True),
            "state": "proving-real-ip-growth",
        }
        require(uid(row) == qualification.COMPLETED_JOURNAL_UID and row.get("data") == expected
                and not row["metadata"].get("deletionTimestamp") and not row["metadata"].get("ownerReferences"),
                "Original qualification journal changed; never acquire or modify it")

    def kwok_ready(self, snapshot):
        expected = self.bundle["receipt"]["preserved_kwok_uids"]
        maintenance._require_all_kwok_ready(snapshot["nodes"], expected)
        rows = self.kube("-n", "kube-node-lease", "get", "leases", "-o", "json")["items"]
        leases = {row["metadata"]["name"]: row for row in rows if row["metadata"]["name"] in expected}
        require(set(leases) == set(expected) and len(leases) == sum(row["metadata"]["name"] in expected for row in rows),
                "All 100 KWOK Node leases must be present exactly once")
        current = datetime.now(timezone.utc)
        for name, row in leases.items():
            spec = row.get("spec") or {}
            previous = self.lease_pins[name]
            require(uid(row) == previous["uid"] and spec.get("holderIdentity") == previous["spec"]["holderIdentity"]
                    and base.integer(spec.get("leaseDurationSeconds")) and spec["leaseDurationSeconds"] > 0
                    and 0 <= (current - base.timestamp(spec.get("renewTime"), "KWOK lease renewal")).total_seconds()
                    <= spec["leaseDurationSeconds"] + 30
                    and any(ref.get("kind") == "Node" and ref.get("name") == name and ref.get("uid") == expected[name]
                            for ref in row["metadata"].get("ownerReferences") or []),
                    "A KWOK Node lease owner/identity/freshness changed")
        self.summary["kwok_ready"] = 100
        self.summary["kwok_lease_owner"] = next(iter(leases.values()))["spec"]["holderIdentity"]

    def pdb_safe(self, snapshot):
        require(base.frozen_pdbs(snapshot) == base.frozen_pdbs(self.bundle["current"]), "Original PDB UID/spec changed")
        for pdb in snapshot["pdbs"]["items"]:
            status = pdb.get("status") or {}
            require(pdb.get("spec", {}).get("unhealthyPodEvictionPolicy") == "AlwaysAllow"
                    and base.integer(status.get("disruptionsAllowed")) and status["disruptionsAllowed"] > 0
                    and base.integer(status.get("observedGeneration"))
                    and status["observedGeneration"] >= pdb["metadata"].get("generation", 1),
                    "Fresh original PDB eviction policy/budget is not safe; no waiver is authorized")

    def target_safe(self, snapshot):
        nodes = maintenance._real_node_map(snapshot["nodes"])
        target = nodes.get(TARGET)
        require(target is not None and uid(target) == base.REAL_UIDS[TARGET]
                and base.node_boot(target) == stalled.BOOTS[TARGET]
                and not workers.node_is_ready(target), "Failed target identity/recovery changed")
        condition = stalled.ready_condition(target)
        require(condition["status"] in ("Unknown", "False")
                and (datetime.now(timezone.utc) - base.timestamp(condition["lastHeartbeatTime"], "failed heartbeat")).total_seconds() >= 300,
                "The target is not the prolonged unresponsive worker")
        for pod in snapshot["pods"]["items"]:
            if pod["spec"].get("nodeName") != TARGET:
                continue
            require(uid(pod) in self.target_pods and not maintenance._readiness_condition_true(pod),
                    "A new or currently Ready workload appeared on the failed target")
            stalled.Recovery.owned_pod(self.target_checker, snapshot, pod)
        agents = maintenance._agent_map(snapshot["pods"])
        require(all(name in agents and uid(agents[name]) == pod_uid
                    and agents[name]["spec"].get("nodeName") == TARGET
                    and agents[name]["metadata"].get("deletionTimestamp")
                    and mocks._pod_owned_by_controller_uid(agents[name], self.checks.reader.mock_uid)
                    and base.pvc_free(agents[name]["spec"])
                    for name, pod_uid in self.target_uids.items()),
                "All 56 target Pods must remain their original terminating, controller-owned, PVC-free identities")
        self.summary["target_process_fencing_assumed"] = False

    def preflight_state(self):
        self.unchanged_inputs()
        snapshot, networks = self.checks.observe()
        current_pools = self.checks.reader.summary["arm_diagnostics"]["pools"]
        require({row["name"]: capacity.canonical(capacity.prepared.pool_configuration(row)) for row in current_pools}
                == capacity.canonical(self.old_pool_configs),
                "Pool configuration changed before retirement; no worker action is authorized")
        current_scales = {}
        for row in self.checks.reader.summary["arm_diagnostics"]["vmsses"]:
            model = capacity.canonical({key: value for key, value in row.items() if key != "provisioningState"})
            model["sku"].pop("capacity")
            current_scales[row["name"]] = model
        require(current_scales == self.old_scale_models,
                "VMSS configuration changed before retirement; no worker action is authorized")
        self.kwok_ready(snapshot)
        self.pdb_safe(snapshot)
        self.target_safe(snapshot)
        template = base.controller(snapshot, "StatefulSet", mocks.DEFAULT_NAMESPACE, "kwok-node")["spec"]["template"]["spec"]
        require(not template.get("nodeName") and template.get("schedulerName", "default-scheduler") == "default-scheduler"
                and not mocks._tolerates(self.hold_taint(), template.get("tolerations") or []),
                "The unchanged mock template could bypass the owned source0 placement hold")
        self.old_journals()
        self.checks.metrics(snapshot, networks)
        self.summary["fresh_pre_retirement_headroom"] = copy.deepcopy(self.checks.summary["memory_projection"])
        return snapshot

    def default_operation(self):
        operation = self.checks.reader.az_json(
            "aks", "operation", "show-latest", "--resource-group", base.RESOURCE_GROUP,
            "--name", base.CLUSTER, "--nodepool-name", "default", "--query", base.OPERATION_QUERY)
        require(isinstance(operation, dict) and operation.get("name") and not operation.get("errorCode"),
                "Default-pool operation is missing or failed")
        start = base.timestamp(operation.get("startTime"), "default operation start")
        require(start <= datetime.now(timezone.utc), "Default operation is future-dated")
        if not self.native_submitted:
            require(operation.get("status") == "Succeeded" and operation.get("endTime"),
                    "Default-pool customer operation is not quiescent")
        elif operation["name"] == self.previous_operation:
            require(operation.get("status") == "Succeeded", "The prior default operation became busy")
            return False
        else:
            require(operation.get("operationType") in ("DeleteMachines", "DeleteAgentPoolMachines", "AgentPoolDeleteMachines")
                    and operation.get("status") in ("Succeeded", "InProgress", "Running")
                    and start >= base.timestamp(self.summary["native"]["requested_at"], "native request")
                    and (self.operation_name is None or operation["name"] == self.operation_name),
                    "A different provider operation overlapped the accepted exact retirement")
            self.operation_name = operation["name"]
            self.summary["native"]["operation_name"] = self.operation_name
        if operation["status"] == "Succeeded":
            require(start <= base.timestamp(operation.get("endTime"), "default operation end") <= datetime.now(timezone.utc),
                    "Default operation completion is invalid")
        self.summary["default_operation"] = operation
        return operation["status"] == "Succeeded"

    def models_after_submission(self):
        self.checks.reader.authority()
        operation_complete = self.default_operation()
        reader = self.checks.reader
        customer = reader.az_json("aks", "operation", "show-latest", "--resource-group", base.RESOURCE_GROUP,
                                  "--name", base.CLUSTER, "--query", base.OPERATION_QUERY)
        require(customer.get("status") == "Succeeded" and customer.get("name") == reader.old_arm["operation"]
                and not customer.get("errorCode"), "An unrelated customer operation overlapped retirement")
        pools = reader.az_json("aks", "nodepool", "list", "--resource-group", base.RESOURCE_GROUP, "--cluster-name", base.CLUSTER)
        scales = reader.az_json("vmss", "list", "--resource-group", base.NODE_GROUP, "--query", base.VMSS_QUERY)
        pool_map = {row.get("name"): row for row in pools}
        scale_map = {workers.vmss_pool_name(row): row for row in scales}
        require(len(pool_map) == len(pools) == 3 and set(pool_map) == {"default", "prompool", "cniv5"}
                and len(scale_map) == len(scales) == 3 and set(scale_map) == set(pool_map),
                "Pool/VMSS inventory changed outside exact retirement")
        for name, pool in pool_map.items():
            require(capacity.canonical(capacity.prepared.pool_configuration(pool))
                    == capacity.canonical(self.old_pool_configs[name]), "Pool configuration changed outside count reduction")
            expected = (1, 2) if name == "default" else (2,) if name == "cniv5" else (0,)
            require(base.integer(pool.get("count")) and pool["count"] in expected
                    and pool.get("powerState", {}).get("code") == "Running"
                    and pool.get("provisioningState") in (
                        {"Succeeded", "Updating", "Scaling", "DeletingMachines"} if name == "default" else {"Succeeded"}),
                    "Pool count/power/state is not this accepted retirement")
            scale = scale_map[name]
            model = capacity.canonical({key: value for key, value in scale.items() if key != "provisioningState"})
            count = model["sku"].pop("capacity")
            require(model == self.old_scale_models[scale["name"]] and base.integer(count) and count in expected
                    and scale.get("provisioningState") in (
                        {"Failed", "Updating", "Succeeded"} if name == "default" else {"Succeeded"}),
                    "VMSS model/count/state changed outside exact retirement")
        instances = reader.az_json("vmss", "list-instances", "--resource-group", base.NODE_GROUP,
                                    "--name", base.DEFAULT_VMSS, "--query", base.VM_QUERY)
        require(isinstance(instances, list) and len(instances) in (1, 2), "Authoritative default VM inventory is unavailable")
        by_id = {str(row.get("instanceId")): row for row in instances}
        require(len(by_id) == len(instances) and "0" in by_id and set(by_id) <= {"0", "1"},
                "Unexpected default VM appeared or protected instance0 disappeared")
        for instance, row in by_id.items():
            name = SOURCE if instance == "0" else TARGET
            require(row.get("computerName") == name and row.get("vmId") == stalled.VM_IDS[name]
                    and capacity.prepared.resource_equal("azure://" + str(row.get("id")), self.node_pins[name]["spec"]["providerID"]),
                    "A default VM identity changed")
        require(by_id["0"].get("provisioningState") == "Succeeded" and by_id["0"].get("latestModelApplied") is True,
                "Protected default0 VM model/regression")
        view = reader.az_json("vmss", "get-instance-view", "--resource-group", base.NODE_GROUP,
                              "--name", base.DEFAULT_VMSS, "--instance-id", "0", "--query", stalled.VIEW_QUERY)
        require({row.get("code") for row in view.get("statuses") or []}
                == {"ProvisioningState/succeeded", "PowerState/running"}
                and stalled.guest_state(view, max_age_seconds=300) == "ready" and stalled.extensions_ready(view),
                "Protected default0 lost guest health")
        empty = reader.az_json("vmss", "list-instances", "--resource-group", base.NODE_GROUP,
                               "--name", base.PROM_VMSS, "--query", base.VM_QUERY)
        require(empty == [], "Natively removed original Prom VM reappeared")
        if "1" not in by_id:
            self.fenced = True
            self.summary["native_fencing_proven"] = True
            self.summary["native"].setdefault("vm_absence_observed_at", workers.utc_now())
        else:
            require(not self.fenced, "Failed instance1 reappeared after fencing")
        evidence = {"views": {}, "default_instances": instances, "pools": pools, "vmsses": scales}
        reader.summary["arm_diagnostics"] = evidence
        new = reader.new_models(pool_map["cniv5"], scale_map["cniv5"], evidence)
        require(new["healthy"] is True, "Qualified cniv5 VM/disk/image state regressed")
        self.summary["native_observation"] = evidence
        return new, (operation_complete and pool_map["default"]["count"] == 1
                     and scale_map["default"]["sku"]["capacity"] == 1
                     and pool_map["default"]["provisioningState"] == scale_map["default"]["provisioningState"] == "Succeeded")

    def current_guard(self, snapshot, new):
        self.summary["current_kubernetes_diagnostics"] = stalled.safe_diagnostics(snapshot)
        self.save()
        require(stalled.controllers_pin(snapshot["controllers"]) == self.checks.reader.controllers
                and base.frozen_pdbs(snapshot) == self.checks.reader.pdbs, "Functional controller/PDB contracts changed")
        nodes = {row["metadata"]["name"]: row for row in snapshot["nodes"]["items"]}
        expected = set(self.node_pins) - {TARGET}
        require(expected <= set(nodes) <= expected | {TARGET} and len(nodes) == len(snapshot["nodes"]["items"]),
                "Unexpected/missing protected Nodes")
        for name in expected:
            original = copy.deepcopy(self.node_pins[name])
            if name == SOURCE and self.hold_applied:
                original.setdefault("spec", {}).setdefault("taints", []).append(self.hold_taint())
            require(stalled.logical_node(nodes[name]) == stalled.logical_node(original)
                    and not nodes[name]["metadata"].get("deletionTimestamp"), "Protected Node UID/spec or exact owned hold changed")
            if name == SOURCE or name in self.checks.identities:
                require(base.node_boot(nodes[name]) == base.node_boot(original) and stalled.fresh_node_ready(nodes[name]),
                        "A surviving worker rebooted or lost fresh readiness")
        if TARGET in nodes:
            require(uid(nodes[TARGET]) == base.REAL_UIDS[TARGET]
                    and base.node_boot(nodes[TARGET]) == stalled.BOOTS[TARGET]
                    and capacity.prepared.resource_equal(nodes[TARGET]["spec"].get("providerID"), self.node_pins[TARGET]["spec"]["providerID"]),
                    "The target Node identity changed during native removal")
        self.kwok_ready(snapshot)
        by_uid = {uid(pod): pod for pod in snapshot["pods"]["items"]}
        require(len(by_uid) == len(snapshot["pods"]["items"]), "Pod UIDs are duplicated")
        for pod_uid, pin in self.source_protected.items():
            require(pod_uid in by_uid and stalled.pod_pin(by_uid[pod_uid]) == pin and base.pod_ready(by_uid[pod_uid]),
                    "A protected healthy default0 Pod changed or regressed")
        require(not any(maintenance.PROBE_LABEL_KEY in pod["metadata"].get("labels", {}) for pod in snapshot["pods"]["items"]),
                "Unexpected probe/workload overlap")
        agents = maintenance._agent_map(snapshot["pods"])
        require(set(self.healthy_uids) <= set(agents) <= set(self.original_agents), "Logical mock names changed")
        for name, expected_uid in self.healthy_uids.items():
            require(uid(agents[name]) == expected_uid and base.pod_ready(agents[name])
                    and stalled.pod_pin(agents[name]) == stalled.pod_pin(self.original_agents[name]),
                    "One of 44 retained mock identities/specs/readiness changed")
        replacements = {}
        for name, original_uid in self.target_uids.items():
            pod = agents.get(name)
            if pod is None or uid(pod) == original_uid:
                if pod is not None:
                    require(pod["metadata"].get("deletionTimestamp") and pod["spec"].get("nodeName") == TARGET,
                            "An original terminating target mock changed scheduling/deletion state")
                    require(stalled.pod_pin(pod) == stalled.pod_pin(self.original_agents[name]),
                            "An original target mock UID/spec/owner changed during native retirement")
                continue
            if not self.fenced:
                self.summary.setdefault("uncertified_replacements_before_fencing", {})[name] = uid(pod)
                self.save()
                raise workers.ReconcileError("A new target Pod UID appeared before positive native VM fencing")
            owner = base.controller_owner(pod, "StatefulSet")
            require(owner["name"] == "kwok-node" and owner["uid"] == self.checks.reader.mock_uid
                    and uid(pod) not in {uid(row) for row in self.original_agents.values()}
                    and (name not in self.replacement_uids or self.replacement_uids[name] == uid(pod))
                    and semantic_pod_spec(pod["spec"]) == semantic_pod_spec(self.original_agents[name]["spec"])
                    and base.pvc_free(pod["spec"]) and pod["spec"].get("nodeName") in (None, "", *self.checks.identities),
                    "Replacement identity/StatefulSet/spec/image/env/PVC/placement differs")
            self.replacement_uids[name] = uid(pod)
            replacements[name] = {"old_uid": original_uid, "new_uid": uid(pod), "node_name": pod["spec"].get("nodeName"),
                                  "ready": base.pod_ready(pod), "fencing_proven": True}
        self.summary["controller_replacements"] = replacements
        raw_networks = snapshot["nnc"]["items"]
        network_map = {row["metadata"]["name"]: row for row in raw_networks}
        expected_network_names = {SOURCE, *self.checks.identities}
        require(len(network_map) == len(raw_networks) and expected_network_names <= set(network_map)
                <= expected_network_names | {TARGET}, "NNC lifecycle or inventory changed unexpectedly")
        networks, ips = {}, set()
        for name, raw in network_map.items():
            if name == TARGET and self.fenced:
                owner = base.controller_owner(raw, "Node")
                require(uid(raw) == self.networks[TARGET]["uid"] and raw["metadata"].get("namespace") == "kube-system"
                        and owner["name"] == TARGET and owner["uid"] == base.REAL_UIDS[TARGET],
                        "The natively retiring NNC changed ownership")
                containers = (raw.get("status") or {}).get("networkContainers")
                if raw["metadata"].get("deletionTimestamp") or not containers:
                    require(not containers or len(containers) == 1
                            and containers[0].get("id") == self.networks[TARGET]["network_container_id"],
                            "The retiring NNC changed network-container identity")
                    self.summary["native"]["pending_original_nnc_removal"] = True
                    continue
            network = qualification.concrete_network(raw)
            previous = self.latest_networks[name]
            require(all(network[key] == self.networks[name][key] for key in ("uid", "node_uid", "network_container_id"))
                    and network["version"] >= previous["version"]
                    and (set(network["ip_addresses"]) == set(previous["ip_addresses"]) or network["version"] > previous["version"]),
                    "NNC owner/version or unversioned allocation changed")
            require(not ips & set(network["ip_addresses"]), "Real-worker NNC IP allocations conflict")
            ips.update(network["ip_addresses"])
            networks[name] = network
            if name in self.checks.identities or name == SOURCE:
                resident_ips = {pod.get("status", {}).get("podIP") for pod in snapshot["pods"]["items"]
                                if pod["spec"].get("nodeName") == name and not pod["spec"].get("hostNetwork")
                                and pod.get("status", {}).get("podIP") and not pod["metadata"].get("deletionTimestamp")}
                require(resident_ips <= set(network["ip_addresses"]), "A versioned allocation lost a surviving resident IP")
        self.latest_networks.update(copy.deepcopy(networks))
        for name, identity in self.checks.identities.items():
            require(new["vms"][name]["vm_id"] == identity["vm_id"]
                    and uid(nodes[name]) == identity["node_uid"] and base.node_boot(nodes[name]) == identity["boot_id"]
                    and self.checks.reader.daemonsets <= maintenance._healthy_system_daemonsets_on_node(snapshot["pods"], name),
                    "Qualified worker identity/system readiness changed")
            view = self.checks.reader.summary["arm_diagnostics"]["views"][f"{new['vmss']['name']}/{identity['instance_id']}"]
            require(stalled.guest_state(view, max_age_seconds=300) == "ready" and stalled.extensions_ready(view),
                    "Qualified worker guest health regressed")
        no_old_refs = TARGET not in nodes and TARGET not in network_map and not any(
            pod["spec"].get("nodeName") == TARGET or uid(pod) in self.target_pods for pod in snapshot["pods"]["items"])
        ready = len(replacements) == 56 and all(row["ready"] and row["node_name"] in self.checks.identities
                                                for row in replacements.values())
        self.summary.update(replacements_ready=ready, current_mock_uids={name: uid(pod) for name, pod in agents.items()},
                            current_mock_ready=sum(base.pod_ready(pod) for pod in agents.values()))
        self.save()
        return networks, no_old_refs, ready

    def final_headroom(self, snapshot, networks):
        node_metrics = self.kube("get", "--raw", "/apis/metrics.k8s.io/v1beta1/nodes")["items"]
        pod_metrics = self.kube("get", "--raw", "/apis/metrics.k8s.io/v1beta1/namespaces/mock-clustermesh/pods")["items"]
        def normalize(row):
            result = copy.deepcopy(row)
            observed = base.timestamp(row.get("timestamp"), "final metrics")
            require(0 <= (datetime.now(timezone.utc) - observed).total_seconds() <= 180, "Final metrics are stale")
            result["timestamp"] = observed.isoformat(timespec="microseconds")
            return result
        metrics = {row["metadata"]["name"]: normalize(row) for row in node_metrics if row["metadata"]["name"] in self.checks.identities}
        samples = maintenance._pod_memory_usage_bytes({"items": [
            normalize(row) for row in pod_metrics if row["metadata"]["name"] in self.healthy_uids]}, mocks.DEFAULT_NAMESPACE)
        require(set(metrics) == set(self.checks.identities) and set(self.healthy_uids) <= set(samples),
                "Fresh final worker/44-agent metrics are missing")
        rss = max(self.checks.rss_high, max(row["memory_bytes"] for row in samples.values()))
        agents = maintenance._agent_map(snapshot["pods"])
        counts = Counter(agents[name]["spec"]["nodeName"] for name in self.target_uids)
        nodes = maintenance._real_node_map(snapshot["nodes"])
        for name in self.checks.identities:
            baseline = self.bundle["receipt"]["memory_projection"]["node_high_water"][name]
            used = int(mocks._quantity(metrics[name]["usage"]["memory"], "actual memory"))
            projected = baseline["memory"] + counts[name] * rss + 512 * 1024**2
            require(maintenance._headroom_ok(
                nodes[name], metrics[name], threshold_percent=85,
                effective_reserved_memory_bytes=max(projected - used, 0), next_memory_bytes=0),
                "Final actual/projected worker memory exceeds 85%")
            actual_cpu = int(mocks._quantity(metrics[name]["usage"]["cpu"], "actual CPU") * 1000)
            allocatable = nodes[name]["status"]["allocatable"]
            active = [pod for pod in snapshot["pods"]["items"] if pod["spec"].get("nodeName") == name
                      and pod["status"].get("phase") not in ("Succeeded", "Failed")]
            requested = sum(mocks._resource_requests(pod)[0] for pod in active)
            require(max(actual_cpu, requested) + 250 < int(mocks._quantity(allocatable["cpu"], "CPU") * 1000) * .85
                    and len(active) + 5 <= int(allocatable["pods"]), "Final CPU/Pod-slot headroom is unsafe")
            for agent in (agents[row] for row in self.target_uids if agents[row]["spec"]["nodeName"] == name):
                require(agent["status"].get("podIP") in networks[name]["ip_addresses"], "A replacement IP is not in its actual NC")
        self.summary["final_headroom"] = {"replacement_counts": dict(counts), "healthy_rss_high_water_bytes": rss,
                                         "threshold_percent": 85, "checked_at": workers.utc_now()}

    def hold_taint(self):
        return {"key": HOLD_KEY, "value": self.token, "effect": "NoSchedule"}

    def journal_payload(self):
        return {"owner": JOURNAL, "token": self.token, "qualification_build": str(QUALIFICATION_BUILD),
                "qualification_bundle_sha256": digest(self.bundle["hashes"]),
                "target_node_uid": base.REAL_UIDS[TARGET], "target_vm_id": stalled.VM_IDS[TARGET],
                "hold": json.dumps(self.summary["hold"], sort_keys=True),
                "native": json.dumps(self.summary["native"], sort_keys=True)}

    def raw_write(self, command):
        require(self.args.execute, "Plan mode cannot mutate")
        allowed = command == delete_command() or command[:6] in (
            ["kubectl", "-n", "kube-system", "create", "configmap", JOURNAL],
            ["kubectl", "-n", "kube-system", "patch", "configmap", JOURNAL],
        ) or command[:4] == ["kubectl", "patch", "node", SOURCE]
        require(allowed, "Write escaped the journal/owned NoSchedule/exact default1 native-delete whitelist")
        if command == delete_command():
            require(self.journal_uid and self.hold_applied and not self.native_submitted
                    and self.summary["native"]["accepted"] is None, "Duplicate/unowned native retirement is forbidden")
            self.check_journal()
            self.unchanged_inputs()
            source = self.kube("get", "node", SOURCE, "-o", "json")
            expected = copy.deepcopy(self.node_pins[SOURCE])
            expected.setdefault("spec", {}).setdefault("taints", []).append(self.hold_taint())
            require(stalled.logical_node(source) == stalled.logical_node(expected)
                    and base.node_boot(source) == stalled.BOOTS[SOURCE] and stalled.fresh_node_ready(source),
                    "Healthy source or its placement hold changed at the native submission boundary")
            self.native_submitted = True
        self.summary["mutation_started"] = True
        self.save()
        return super().run(command, 45, cleanup=self.cleanup_mode)

    def acquire(self):
        rows = self.kube("-n", "kube-system", "get", "configmaps", "--field-selector", f"metadata.name={JOURNAL}", "-o", "json")["items"]
        require(not rows, "Existing retirement journal blocks replay/adoption")
        self.summary["journal"].update(attempted=True, accepted=None, ambiguous=True)
        self.save()
        data = self.journal_payload()
        row = workers.parse_json(self.raw_write(["kubectl", "-n", "kube-system", "create", "configmap", JOURNAL,
                                                 *[f"--from-literal={key}={value}" for key, value in data.items()], "-o", "json"]),
                                 "retirement journal")
        require(uid(row) and row.get("data") == data, "Retirement journal creation is ambiguous")
        self.journal_uid, self.journal_data_pin = uid(row), data
        self.check_journal()
        self.summary["journal"].update(uid=self.journal_uid, accepted=True, ambiguous=False)
        self.save()

    def check_journal(self):
        row = self.kube("-n", "kube-system", "get", "configmap", JOURNAL, "-o", "json")
        metadata = row.get("metadata") or {}
        require(uid(row) == self.journal_uid and row.get("data") == self.journal_data_pin
                and metadata.get("name") == JOURNAL and metadata.get("namespace") == "kube-system"
                and metadata.get("resourceVersion") and not metadata.get("deletionTimestamp")
                and not metadata.get("ownerReferences"), "Retirement journal UID/data/lifecycle changed")
        return row

    def persist_journal(self):
        self.unchanged_inputs()
        row = self.check_journal()
        data = self.journal_payload()
        self.raw_write(["kubectl", "-n", "kube-system", "patch", "configmap", JOURNAL, "--type=json", "-p", json.dumps([
            {"op": "test", "path": "/metadata/uid", "value": self.journal_uid},
            {"op": "test", "path": "/metadata/resourceVersion", "value": row["metadata"]["resourceVersion"]},
            {"op": "test", "path": "/data", "value": self.journal_data_pin},
            {"op": "add", "path": "/data", "value": data},
        ])])
        self.journal_data_pin = copy.deepcopy(data)
        self.check_journal()
        self.save()

    def change_hold(self, *, remove=False):
        node = self.kube("get", "node", SOURCE, "-o", "json")
        require(uid(node) == base.REAL_UIDS[SOURCE], "Healthy source UID changed before placement-hold CAS")
        taints = maintenance._taints(node)
        patch = [{"op": "test", "path": "/metadata/uid", "value": uid(node)},
                 {"op": "test", "path": "/metadata/resourceVersion", "value": node["metadata"]["resourceVersion"]}]
        if remove:
            require(taints.count(self.hold_taint()) == 1, "Only the exact owned placement hold may be removed")
            index = taints.index(self.hold_taint())
            patch.extend([{"op": "test", "path": f"/spec/taints/{index}", "value": self.hold_taint()},
                          {"op": "remove", "path": f"/spec/taints/{index}"}])
        else:
            require(not any(row.get("key") == HOLD_KEY for row in taints), "Another placement hold exists")
            patch.append({"op": "add", "path": "/spec/taints", "value": taints + [self.hold_taint()]})
        key = "remove" if remove else "add"
        self.summary["hold"][key] = {"attempted": True, "accepted": None, "ambiguous": True}
        self.persist_journal()
        self.raw_write(["kubectl", "patch", "node", SOURCE, "--type=json", "-p", json.dumps(patch)])
        self.hold_applied = not remove
        original = copy.deepcopy(self.node_pins[SOURCE])
        if self.hold_applied:
            original.setdefault("spec", {}).setdefault("taints", []).append(self.hold_taint())
        self.checks.reader.old_nodes[SOURCE] = original
        self.summary["hold"][key].update(accepted=True, ambiguous=False)
        self.summary["hold"]["applied"] = self.hold_applied
        self.persist_journal()

    def execute(self):
        self.preflight_state()
        self.default_operation()
        require(not self.kube("-n", "kube-system", "get", "configmaps", "--field-selector", f"metadata.name={JOURNAL}", "-o", "json")["items"],
                "Existing retirement journal prohibits replay")
        self.summary.update(plan_valid=True, status="planned-read-only")
        self.save()
        if not self.args.execute:
            return
        self.acquire()
        self.preflight_state()
        self.change_hold()
        self.preflight_state()
        self.default_operation()
        self.previous_operation = self.summary["default_operation"]["name"]
        native = self.summary["native"]
        native.update(attempted=True, submission_started=False, accepted=None, ambiguous=True,
                      requested_at=workers.utc_now(), command=delete_command())
        self.persist_journal()
        self.preflight_state()
        self.default_operation()
        self.check_journal()
        self.unchanged_inputs()
        native["submission_started"] = True
        self.persist_journal()
        self.raw_write(delete_command())
        native.update(accepted=True, ambiguous=False, accepted_at=workers.utc_now())
        self.persist_journal()
        self.summary["status"] = "observing-exact-native-default1-retirement"
        while True:
            self.unchanged_inputs()
            new, complete = self.models_after_submission()
            snapshot = self.checks.reader.snapshot()
            networks, no_old_refs, ready = self.current_guard(snapshot, new)
            self.old_journals()
            if self.fenced and complete and no_old_refs and ready:
                self.pdb_safe(snapshot)
                self.final_headroom(snapshot, networks)
                self.summary["source_retired"] = True
                self.save()
                break
            require(time.monotonic() < self.work_deadline, "Accepted native retirement/replacement readiness exceeded its bound")
            time.sleep(min(10, self.remaining_seconds(10)))
        self.cleanup_mode = True
        self.checks.reader.work_deadline = self.cleanup_deadline
        self.summary["hold"]["cleanup_started"] = True
        self.change_hold(remove=True)
        new, complete = self.models_after_submission()
        snapshot = self.checks.reader.snapshot()
        networks, no_old_refs, ready = self.current_guard(snapshot, new)
        require(complete and no_old_refs and ready and not self.hold_applied, "Final retirement/hold cleanup did not remain safe")
        self.final_headroom(snapshot, networks)
        self.summary["status"] = "native-retirement-complete-workloads-not-qualified"
        self.persist_journal()
        self.summary.update(success=True, placement_hold_removed=True)
        self.save()


def validate_args(args):
    require(args.resource_group == args.confirm_resource_group == base.RESOURCE_GROUP
            and args.expected_subscription.lower() == base.SUBSCRIPTION and args.expected_region.lower() == base.REGION
            and capacity.quantities.valid_sha(args.expected_tfvars_sha), "Retirement scope/tfvars mismatch")
    require(args.qualification_build_id == QUALIFICATION_BUILD
            and base.integer(args.timeout_seconds) and 600 <= args.timeout_seconds <= 3600,
            "Only completed qualification 79986 and a bounded 600..3600-second retirement are supported")
    require(args.kubeconfig and args.context == base.CLUSTER, "Private mesh96 credentials/context required")
    root, output, config = (Path(value).resolve() for value in (
        args.qualification_directory, args.summary_file, args.kubeconfig))
    require(len({root, output, config}) == 3 and root not in output.parents and not output.exists(),
            "Output must be new and cannot overwrite immutable inputs/private credentials")
    args.role = base.ROLE


def execute_retirement(args, summary, runner=workers.run_command):
    validate_args(args)
    summary.update(schema_version=1, execute=args.execute, plan_valid=False, mutation_started=False, success=False,
                   native_fencing_proven=False, source_retired=False, replacements_ready=False,
                   placement_hold_removed=False, cleanup_errors=[],
                   workloads_ready=False, bootstrap_complete=False, status="validating",
                   started_at=workers.utc_now(), qualification_build_id=QUALIFICATION_BUILD,
                   journal={"name": JOURNAL, "retained": True},
                   hold={"node_name": SOURCE, "node_uid": base.REAL_UIDS[SOURCE], "applied": False},
                   native={"attempted": False, "submission_started": False, "accepted": None,
                           "ambiguous": False, "automatic_retry_allowed": False})
    try:
        bundle = load_inputs(args)
        summary.update(qualification_input_hashes=bundle["hashes"], qualification_sha256=bundle["hashes"]["qualification.json"],
                       protected_mock_uids=bundle["inputs"]["healthy_agents"],
                       original_target_mock_uids=bundle["inputs"]["remaining_agents"],
                       preserved_kwok_uids=bundle["receipt"]["preserved_kwok_uids"],
                       target={"node_name": TARGET, "node_uid": base.REAL_UIDS[TARGET], "vm_id": stalled.VM_IDS[TARGET]},
                       plan_sha256=stalled.PLAN_SHA)
        Retirement(args, bundle, summary, runner).execute()
    except EXPECTED_ERRORS as error:
        summary.update(success=False, status="failed-closed", error=str(error), rollback_attempted=False,
                       placement_hold_removed=False,
                       hold_disposition="retained if applied or ambiguous; no automatic undo")
        if summary["hold"].get("cleanup_started"):
            summary["cleanup_errors"].append(str(error))
        raise
    finally:
        summary["finished_at"] = workers.utc_now()
        summary["workloads_ready"] = summary["bootstrap_complete"] = False
        mocks.write_json_atomic(args.summary_file, summary)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("resource-group", "confirm-resource-group", "expected-subscription", "expected-region",
                 "expected-tfvars-sha", "qualification-directory", "kubeconfig", "summary-file"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--qualification-build-id", type=int, required=True)
    parser.add_argument("--context", default=base.CLUSTER)
    parser.add_argument("--timeout-seconds", type=int, default=3600)
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    def interrupted(signum, _frame):
        raise workers.ReconcileError(f"Interrupted ({signum}); retain journal/hold, never retry native deletion")
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, interrupted)
    args, summary = parse_args(argv), {}
    try:
        execute_retirement(args, summary)
    except EXPECTED_ERRORS as error:
        print(f"Qualified failed-worker retirement failed closed: {error}", file=sys.stderr)
        return 1
    print(f"{summary['status']}; workloads_ready=false")
    return 0


if __name__ == "__main__":
    sys.exit(main())
