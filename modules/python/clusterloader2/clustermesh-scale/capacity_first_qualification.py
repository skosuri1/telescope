#!/usr/bin/env python3
"""Qualify the existing 79971 cniv5 workers; never create capacity or move workloads."""

# pylint: disable=protected-access,too-many-lines,too-many-boolean-expressions

from __future__ import annotations

import argparse
import copy
import hashlib
import ipaddress
import json
import re
import signal
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import yaml

import capacity_first_worker_recovery as capacity
import cni_worker_maintenance as maintenance
import mock_cni_recovery as mocks
import stalled_retained_worker_recovery as stalled


base = capacity.base
workers = capacity.workers
require = capacity.require
digest = capacity.digest
uid = base.object_uid
JOURNAL = "mesh96-cniv5-qualification-79971"
OWNER = "capacity-first-qualification"
OBSERVATION_BUILD = 79975
CAPACITY_BUILD = 79971
COMPLETED_PROBE_BUILD = 79979
COMPLETED_JOURNAL_UID = "a94ad391-c28f-4211-bc5c-88ed6d03615c"
RESERVE_SECONDS = 300
OBSERVATION_FILES = (
    "cniv5-observation.json", "cniv5-operation.json", "cniv5-capacity-journal.json",
    "cniv5-instances.json", "cniv5-vmss-model.json", "current-nodes.json", "current-pods.json",
    "current-controllers.json", "current-pdbs.json", "current-nnc.json",
)
EXPECTED_ERRORS = (workers.ReconcileError, mocks.RecoveryError, OSError, ValueError, TypeError, KeyError)
SENSITIVE_DIAGNOSTIC_KEY = re.compile(
    r"password|token|secret|credential|authorization|certificate|private.?key|client.?key|kubeconfig", re.I,
)


def redact_diagnostic(value):
    if isinstance(value, list):
        return [redact_diagnostic(row) for row in value]
    if isinstance(value, dict):
        if value.get("kind") == "Secret":
            return {"kind": "Secret", "content": "<redacted>"}
        return {
            key: "<redacted>" if SENSITIVE_DIAGNOSTIC_KEY.search(str(key)) or (
                key == "value" and SENSITIVE_DIAGNOSTIC_KEY.search(str(value.get("name", "")))
            ) else redact_diagnostic(item)
            for key, item in value.items()
            if key not in ("managedFields", "kubectl.kubernetes.io/last-applied-configuration")
        }
    if not isinstance(value, str):
        return value
    text = re.sub(r"-----BEGIN [^-]*PRIVATE KEY-----.*?(?:-----END [^-]*PRIVATE KEY-----|$)",
                  "<redacted-private-key>", value, flags=re.S)
    text = re.sub(r"(?i)(bearer\s+)\S+", r"\1<redacted>", text)
    text = re.sub(
        r"""(?i)(["']?(?:password|token|secret|credential|authorization|access_token|client_secret)[\w.-]*["']?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,;]+)""",
        r"\1<redacted>", text,
    )
    text = re.sub(r"(https?://)[^/\s:@]+:[^/\s@]+@", r"\1<redacted>@", text)
    return re.sub(r"\beyJ[\w-]+\.[\w-]+\.[\w-]+\b", "<redacted-jwt>", text)


def hash_tree(directory):
    root = Path(directory).resolve()
    require(root.is_dir() and not Path(directory).is_symlink(), "Immutable input directory is missing or symlinked")
    result = {}
    for path in sorted(root.rglob("*")):
        require(not path.is_symlink(), "Immutable input tree contains a symlink")
        if path.is_file():
            result[str(path.relative_to(root))] = capacity.checkpoint_hash(path)
        else:
            require(path.is_dir(), "Immutable input tree contains a nonregular entry")
    require(result, "Immutable input directory is empty")
    return result


def concrete_network(row):
    owner = base.controller_owner(row, "Node")
    metadata = row.get("metadata") or {}
    require(metadata.get("namespace") == "kube-system" and capacity.quantities.valid_uuid(uid(row))
            and not metadata.get("deletionTimestamp") and owner["name"] == metadata.get("name"),
            "NNC resource/owner identity is malformed")
    status = row.get("status")
    containers = status.get("networkContainers") if isinstance(status, dict) else None
    require(isinstance(containers, list) and len(containers) == 1 and isinstance(containers[0], dict),
            "NNC allocation is missing or ambiguous")
    container = containers[0]
    require("version" in container and base.integer(container["version"]) and container["version"] >= 0
            and base.integer(status.get("assignedIPCount")) and status["assignedIPCount"] > 0,
            "NNC must have an explicit nonnegative version and concrete allocation")
    result = maintenance._nnc_map({"items": [row]})[metadata["name"]]
    addresses = result["ip_addresses"]
    require(len(set(addresses)) == len(addresses) == result["assigned_ip_count"]
            and all(ipaddress.ip_address(address).version == 4 for address in addresses),
            "NNC assigned IP count must match distinct concrete IPv4 addresses")
    return result


def allocation_map(payload):
    result = {}
    seen = {}
    for raw in mocks._items(payload, "all NNC allocations"):
        row = concrete_network(raw)
        require(row["name"] not in result, "NNC names are duplicated")
        result[row["name"]] = row
        for address in row["ip_addresses"]:
            require(address not in seen, f"NNC allocations conflict: {address} on {seen.get(address)} and {row['name']}")
            seen[address] = row["name"]
    require(len(result) == 4, "Exactly four real-worker NNC allocations must remain conflict-free")
    return result


def load_inputs(args):
    observation_hashes = hash_tree(args.observation_directory)
    capacity_hashes = hash_tree(args.capacity_directory)
    require(set(OBSERVATION_FILES) <= set(observation_hashes), "79975 observation artifacts are incomplete")
    root = Path(args.capacity_directory)
    require({"recovery.json", "accepted-restart.json", "prior-capacity.json"} <= set(capacity_hashes),
            "The entire 79971 capacity artifact is required")
    internal = SimpleNamespace(**vars(args))
    internal.source_state_directory = str(root / "source-state")
    internal.restart_checkpoint = str(root / "accepted-restart.json")
    data, source_hashes, restart_hash, failure = capacity.load_inputs(internal)
    receipt = stalled.read_json(root / "recovery.json")
    prior = stalled.read_json(root / "prior-capacity.json")
    obs = {name: stalled.read_json(Path(args.observation_directory) / name) for name in OBSERVATION_FILES}
    observed = obs["cniv5-observation.json"]
    require(observed.get("observation_only") is True and observed.get("mutation_started") is False
            and observed.get("creation_receipt_reference_build") == CAPACITY_BUILD,
            "The observation is not the read-only accepted-capacity observation")
    create = receipt.get("create") or {}
    desired = receipt.get("desired_pool") or {}
    patch = desired.get("orchestratorVersion")
    require(receipt.get("schema_version") == 1 and receipt.get("phase") == "capacity-first-registration-only"
            and receipt.get("pool_created") is True
            and receipt.get("execute") is True and receipt.get("plan_sha256") == stalled.PLAN_SHA
            and create.get("submission_started") is True and create.get("attempted") is True
            and create.get("accepted") is True and create.get("ambiguous") is False
            and create.get("automatic_retry_allowed") is False
            and isinstance(patch, str) and re.fullmatch(r"\d+\.\d+\.\d+", patch)
            and create.get("command") == capacity.add_command(patch)
            and desired == capacity.pool_settings(patch),
            "Only the genuine single accepted cniv5 System(2) request may be qualified")
    require(receipt.get("source_hashes") == source_hashes and receipt.get("source_state_sha256") == digest(source_hashes)
            and receipt.get("restart_checkpoint_sha256") == restart_hash
            and receipt.get("continuation", {}).get("source_build") == 79959
            and receipt["continuation"].get("checkpoint_sha256") == capacity_hashes["prior-capacity.json"],
            "Capacity input/restart/prior-reservation lineage changed")
    old_create = prior.get("create") or {}
    require(old_create.get("submission_started") is False and old_create.get("accepted") is None,
            "The prior capacity reservation was not the approved UNSENT reservation")
    journal = obs["cniv5-capacity-journal.json"]
    journal_data = journal.get("data") or {}
    require(uid(journal) == receipt.get("journal", {}).get("uid") == capacity.RESERVED_JOURNAL_UID
            and journal["metadata"].get("name") == capacity.JOURNAL
            and journal["metadata"].get("namespace") == "kube-system"
            and not journal["metadata"].get("deletionTimestamp") and not journal["metadata"].get("ownerReferences")
            and journal_data.get("owner") == capacity.OWNER
            and journal_data.get("failure_correlation") == capacity.CORRELATION
            and json.loads(journal_data.get("create", "{}")) == create
            and journal_data.get("source_state_sha256") == digest(source_hashes)
            and journal_data.get("restart_checkpoint_sha256") == restart_hash
            and journal_data.get("desired_pool_sha256") == digest(desired)
            and journal_data.get("prior_checkpoint_sha256") == capacity_hashes["prior-capacity.json"]
            and journal_data.get("prior_build_id") == "79959"
            and json.loads(journal_data.get("prior_unsubmitted_create", "{}")) == old_create
            and re.fullmatch(r"[0-9a-f]{32}", str(journal_data.get("token", ""))),
            "Existing capacity journal UID/data/history is not exact")
    operation = obs["cniv5-operation.json"]
    require(operation.get("status") == "Succeeded" and operation.get("operationType") == "PutAgentPool"
            and operation.get("name") and not operation.get("errorCode")
            and base.timestamp(create.get("requested_at"), "accepted creation request")
            <= base.timestamp(operation.get("startTime"), "creation operation start")
            <= base.timestamp(create.get("accepted_at"), "creation acknowledgement")
            <= base.timestamp(operation.get("endTime"), "creation completion"),
            "The completed pool operation does not bind to the historical accepted request")
    model = obs["cniv5-vmss-model.json"]
    require(model.get("tags", {}).get("aks-managed-createOperationID") == operation["name"]
            and model.get("tags", {}).get("aks-managed-poolName") == capacity.POOL
            and model.get("provisioningState") == "Succeeded" and model.get("sku", {}).get("capacity") == 2
            and model.get("sku", {}).get("name") == capacity.prom.VM_SIZE,
            "Observed VMSS is not the completed accepted two-worker pool")
    model_id = (f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.NODE_GROUP}"
                f"/providers/Microsoft.Compute/virtualMachineScaleSets/{model.get('name')}")
    require(capacity.prepared.resource_equal(model.get("id"), model_id), "Observed VMSS scope is incorrect")
    nodes = {row["metadata"]["name"]: row for row in obs["current-nodes.json"]["items"]}
    original_nodes = {row["metadata"]["name"]: row for row in data["current-nodes.json"]["items"]}
    require(set(original_nodes) <= set(nodes) and len(nodes) == len(original_nodes) + 2,
            "Observation has extra/missing original or cniv5 Nodes")
    agents = maintenance._agent_map(obs["current-pods.json"])
    original_agents = maintenance._agent_map(data["current-pods.json"])
    require(set(agents) == set(original_agents) and all(stalled.pod_pin(agents[name]) == stalled.pod_pin(pod)
            for name, pod in original_agents.items()), "Original 100 mock UID/spec/ownership map changed")
    require(all(stalled.logical_node(nodes[name]) == stalled.logical_node(node) for name, node in original_nodes.items()),
            "Original default/KWOK logical Node identities changed")
    require(stalled.controllers_pin(obs["current-controllers.json"]) == stalled.controllers_pin(data["current-controllers.json"])
            and base.frozen_pdbs({"pdbs": obs["current-pdbs.json"]}) == base.frozen_pdbs({"pdbs": data["current-pdbs.json"]}),
            "Original functional controller/PDB pins changed")
    healthy = {name: uid(pod) for name, pod in agents.items()
               if pod["spec"].get("nodeName") == stalled.SOURCE and base.pod_ready(pod)}
    remaining = {name: uid(pod) for name, pod in agents.items() if pod["spec"].get("nodeName") == stalled.TARGET}
    require(len(healthy) == 44 and len(remaining) == 56
            and all(agents[name]["metadata"].get("deletionTimestamp") for name in remaining),
            "Qualification requires the observed 44 healthy / 56 already-Terminating logical agents")
    networks = {row["metadata"]["name"]: row for row in obs["current-nnc.json"]["items"]}
    allocation_map(obs["current-nnc.json"])
    instances = obs["cniv5-instances.json"]
    require(isinstance(instances, list) and len(instances) == 2, "Exactly two existing cniv5 VMs are required")
    created_instances = (receipt.get("arm_diagnostics") or {}).get("new_instances")
    require(isinstance(created_instances, list) and len(created_instances) == 2,
            "The accepted creation must record both original new VM identities")
    created = {str(row.get("instanceId")): row for row in created_instances}
    require(len(created) == 2, "The accepted new VM instance map is ambiguous")
    identities = {}
    for vm in instances:
        name = vm.get("computerName")
        node = nodes.get(name)
        require(name not in original_nodes and node is not None and name not in identities
                and vm.get("latestModelApplied") is True and vm.get("provisioningState") == "Succeeded"
                and capacity.quantities.valid_uuid(vm.get("vmId"))
                and capacity.prepared.resource_equal(vm.get("id"), f"{model_id}/virtualMachines/{vm.get('instanceId')}")
                and capacity.prepared.resource_equal(node["spec"].get("providerID"), "azure://" + vm["id"])
                and mocks._node_pool_name(node) == capacity.POOL, "Observed VM/Node ownership is not exact")
        original_vm = created.get(str(vm["instanceId"]))
        require(original_vm is not None and all(vm.get(key) == original_vm.get(key) for key in (
            "instanceId", "vmId", "computerName",
        )) and capacity.prepared.resource_equal(vm["id"], original_vm.get("id")),
                "The observed new VM differs from the single accepted creation")
        network = concrete_network(networks[name])
        require(network["node_uid"] == uid(node), "Observed NNC owner differs from its actual new Node")
        identities[name] = {"node_uid": uid(node), "boot_id": base.node_boot(node), "vm_id": vm["vmId"],
                            "provider_id": ("azure://" + vm["id"]).lower(), "instance_id": str(vm["instanceId"]),
                            "nnc_uid": network["uid"], "network_container_id": network["network_container_id"],
                            "initial_network": network}
    require(len({row["node_uid"] for row in identities.values()}) == 2
            and len({row["vm_id"] for row in identities.values()}) == 2
            and len({row["network_container_id"] for row in identities.values()}) == 2,
            "Existing cniv5 identities are not distinct")
    # A protection baseline, not a modified live inventory: all live reads retain
    # all four workers. Readiness improvements are explicitly authorized by 79975.
    protection = copy.deepcopy(data)
    protection["current-nodes.json"] = {"items": [nodes[name] for name in original_nodes]}
    protection["current-nnc.json"] = {"items": [row for row in obs["current-nnc.json"]["items"]
                                               if row["metadata"]["name"] in original_nodes]}
    for key in ("current-pods.json", "current-controllers.json", "current-pdbs.json"):
        protection[key] = copy.deepcopy(obs[key])
    require(hash_tree(args.observation_directory) == observation_hashes and hash_tree(args.capacity_directory) == capacity_hashes,
            "Qualification inputs changed while loading")
    return {"args": internal, "data": protection, "source_hashes": source_hashes, "restart_hash": restart_hash,
            "failure": failure, "receipt": receipt, "observation": obs, "identities": identities,
            "healthy_agents": healthy, "remaining_agents": remaining, "observation_hashes": observation_hashes,
            "capacity_hashes": capacity_hashes}


def load_completed_proof(args, inputs):
    path = getattr(args, "completed_qualification_checkpoint", None)
    if not path:
        return None, ""
    checksum = capacity.checkpoint_hash(path)
    prior = stalled.read_json(path)
    require(prior.get("execute") is True and prior.get("plan_valid") is True
            and prior.get("mutation_started") is True and prior.get("capacity_qualified") is False
            and prior.get("error") == "The pinned allocated IP baseline regressed"
            and prior.get("journal", {}).get("uid") == COMPLETED_JOURNAL_UID
            and prior["journal"].get("accepted") is True and prior["journal"].get("ambiguous") is False
            and prior.get("input_hashes") == {
                "observation": inputs["observation_hashes"], "capacity": inputs["capacity_hashes"]}
            and prior.get("identities") == inputs["identities"]
            and prior.get("probe_cleanup_pending") == [] and prior.get("cleanup_errors") == [],
            "Only the exact cleaned 79979 probe proof may be completed read-only")
    proofs, receipts = prior.get("ip_growth") or {}, prior.get("probe_receipts") or {}
    require(set(proofs) == set(inputs["identities"]) and len(receipts) == 32,
            "Both original worker growth proofs and all 32 probe receipts are required")
    tokens = {row.get("token") for row in receipts.values()}
    require(len(tokens) == 1 and re.fullmatch(r"[0-9a-f]{32}", str(next(iter(tokens)))),
            "Completed probe ownership token is ambiguous")
    recorded = prior.get("read_only_capacity_guard", {}).get("kubernetes_diagnostics") or {}
    final_networks = allocation_map(recorded["nnc"])
    pods = recorded["pods"]["items"]
    require(not any(pod["metadata"].get("name") in receipts for pod in pods
                    if pod["metadata"].get("namespace") == mocks.DEFAULT_NAMESPACE),
            "The completed source still contains a probe name")
    proved_uids = set()
    for name, proof in proofs.items():
        before, after = proof["before"], proof["after"]
        expected, final = inputs["identities"][name], final_networks[name]
        require(proof.get("http_proven") is True and proof.get("probe_count") == len(proof.get("probe_uids") or [])
                == len(proof.get("ready_ips") or [])
                and before["version"] == 0 and after["version"] == 1 and final["version"] == 2
                and before["assigned_ip_count"] == final["assigned_ip_count"] == 16
                and after["assigned_ip_count"] == 32
                and len(set(before["ip_addresses"])) == 16
                and len(set(after["ip_addresses"])) == 32
                and len(set(proof["ready_ips"])) == proof["probe_count"]
                and set(before["ip_addresses"]) < set(after["ip_addresses"])
                and set(proof["ready_ips"]) <= set(after["ip_addresses"])
                and bool(set(proof["ready_ips"]) - set(before["ip_addresses"])),
                "The recorded real growth/HTTP/cleanup sequence is incomplete")
        for row in (before, after, final):
            require(row["node_uid"] == expected["node_uid"] and row["uid"] == expected["nnc_uid"]
                    and row["network_container_id"] == expected["network_container_id"],
                    "A completed network proof changed physical ownership")
        matching = [row for row in receipts.values() if row.get("node_name") == name]
        require({row.get("uid") for row in matching} == set(proof["probe_uids"])
                and len(matching) == proof["probe_count"], "Completed HTTP and cleanup UIDs differ")
        for row in matching:
            require(row.get("node_uid") == expected["node_uid"]
                    and row.get("create_submission_started") is True and row.get("create_accepted") is True
                    and row.get("create_ambiguous") is False and row.get("delete_attempted") is True
                    and row.get("delete_accepted") is True and row.get("delete_ambiguous") is False,
                    "An ambiguous or unclean probe cannot establish completion")
        proved_uids.update(proof["probe_uids"])
    require(len(proved_uids) == 32 and not proved_uids & {uid(pod) for pod in pods},
            "Completed probe UIDs are duplicated or remain present")
    require(capacity.checkpoint_hash(path) == checksum, "Completed proof changed while loading")
    return prior, checksum


class ReadOnlyCapacityGuard(capacity.CapacityFirst):
    """Use the existing guards with historical creation evidence, never its executor."""

    def __init__(self, inputs, outer, runner):
        summary = {"create": copy.deepcopy(inputs["receipt"]["create"])}
        self.outer = outer
        super().__init__(inputs["args"], inputs["data"], inputs["source_hashes"], inputs["restart_hash"],
                         inputs["failure"], summary, runner)
        self.submitted = True  # Historical 79971 submission, not a write in this phase.

    def save(self):
        self.outer.summary["read_only_capacity_guard"] = {
            key: stalled.safe_diagnostics(self.summary[key]) for key in (
                "arm_diagnostics", "kubernetes_diagnostics", "quota_proof", "fleet_connected_count",
            ) if key in self.summary
        }
        self.outer.save()

    def az_json(self, *command, timeout_seconds=45):
        command = list(command)
        if command[:2] == ["vmss", "show"] and "--query" in command:
            index = command.index("--query") + 1
            command[index] = command[index].replace("diskSizeGb:diskSizeGb", "diskSizeGb:diskSizeGb || diskSizeGB")
        return super().az_json(*command, timeout_seconds=timeout_seconds)

    def write(self, _command):
        raise workers.ReconcileError("Historical capacity guard cannot write")

    def acquire(self):
        raise workers.ReconcileError("Historical capacity journal cannot be acquired")

    def execute(self):
        raise workers.ReconcileError("Historical capacity creation cannot be executed")


class Qualification(maintenance.ClusterOperator):
    """Only journal/probe writes; all production and provider mutation paths are absent."""

    def __init__(self, args, inputs, summary, runner, delete_pod, *, completed=None, completed_hash=""):
        deadline = time.monotonic() + args.timeout_seconds
        super().__init__(args, base.CLUSTER, runner, deadline - RESERVE_SECONDS, deadline)
        self.inputs, self.summary, self.delete_pod = inputs, summary, delete_pod
        self.identities = inputs["identities"]
        self.token, self.journal_uid = uuid.uuid4().hex, ""
        self.cluster = mocks.Cluster(base.ROLE, args.kubeconfig, args.context, base.CLUSTER, base.RESOURCE_GROUP)
        self.reader = ReadOnlyCapacityGuard(inputs, self, self.read_command)
        self.reader.work_deadline, self.reader.cleanup_deadline = self.work_deadline, self.cleanup_deadline
        self.rss_high = 0
        self.node_high = {}
        self.growth_done = False
        self.persisted_journal_data = None
        self.probes_cleaned = False
        self.completed, self.completed_hash = completed, completed_hash
        if completed:
            self.summary["ip_growth"] = copy.deepcopy(completed["ip_growth"])
            self.growth_done = self.probes_cleaned = True
            self.rss_high = completed["memory_projection"]["healthy_agent_rss_high_water_bytes"]
            self.node_high = copy.deepcopy(completed["memory_projection"]["node_high_water"])

    def save(self):
        mocks.write_json_atomic(self.args.summary_file, self.summary)

    def unchanged_inputs(self):
        require(hash_tree(self.args.observation_directory) == self.inputs["observation_hashes"]
                and hash_tree(self.args.capacity_directory) == self.inputs["capacity_hashes"],
                "Immutable qualification input hashes changed")
        if self.completed:
            require(capacity.checkpoint_hash(self.args.completed_qualification_checkpoint) == self.completed_hash,
                    "Completed qualification proof changed")

    def read_command(self, command, timeout_seconds):
        command = list(command)
        if command[0] == "az":
            allowed = command[1:3] in (
                ["account", "show"], ["group", "show"], ["aks", "list"], ["aks", "show"],
                ["vmss", "list"], ["vmss", "show"], ["vmss", "list-instances"], ["vmss", "get-instance-view"],
                ["vm", "list-usage"], ["vm", "list-skus"],
            ) or command[1:4] in (["aks", "operation", "show-latest"], ["aks", "nodepool", "list"],
                                  ["fleet", "member", "list"])
        else:
            allowed = command[0] == "kubectl" and ("get" in command or "logs" in command) and not any(
                item in command for item in ("create", "patch", "delete", "run", "exec", "apply", "taint", "drain"))
        require(allowed, "Qualification read path rejected a mutation or unsupported command")
        return super().run(command, min(timeout_seconds, 120), cleanup=self.cleanup_mode)

    def run(self, command, timeout_seconds=45, *, cleanup=False):
        require(not cleanup or self.cleanup_mode, "Unexpected cleanup read")
        return self.read_command(command, timeout_seconds)

    def kube(self, *command):
        return workers.parse_json(self.run(["kubectl", "--request-timeout=45s", *command]), "qualification read")

    def historical_journal(self):
        current = self.kube("-n", "kube-system", "get", "configmap", capacity.JOURNAL, "-o", "json")
        original = self.inputs["observation"]["cniv5-capacity-journal.json"]
        require(uid(current) == uid(original) and current.get("data") == original.get("data"),
                "Existing capacity journal UID/data/history changed")
        require(current["metadata"].get("name") == capacity.JOURNAL
                and current["metadata"].get("namespace") == "kube-system"
                and not current["metadata"].get("deletionTimestamp")
                and not current["metadata"].get("ownerReferences"),
                "Existing capacity journal lifecycle/ownership changed")

    def guard(self, snapshot, new):
        self.reader.guard(snapshot, new)  # False at version 0 is not accepted as networking proof.
        require(new is not None and new["healthy"] is True, "Existing pool/VMs/disk/image are not fully healthy")
        nodes = maintenance._real_node_map(snapshot["nodes"])
        networks = {row["metadata"]["name"]: row for row in snapshot["nnc"]["items"]}
        allocations = allocation_map(snapshot["nnc"])
        require(set(allocations) == set(nodes), "NNC allocation set differs from the four real workers")
        self.summary["all_nnc_allocations"] = {
            name: {"nnc_uid": row["uid"], "network_container_id": row["network_container_id"],
                   "assigned_ip_count": row["assigned_ip_count"], "version": row["version"]}
            for name, row in allocations.items()
        }
        actual = {}
        for name, expected in self.identities.items():
            require(name in nodes and name in networks and name in new["vms"], "An existing qualified worker disappeared")
            node = nodes[name]
            row = allocations[name]
            actual[name] = row
            require(uid(node) == expected["node_uid"] and base.node_boot(node) == expected["boot_id"]
                    and new["vms"][name]["vm_id"] == expected["vm_id"]
                    and capacity.canonical(node["spec"].get("providerID")) == expected["provider_id"]
                    and row["uid"] == expected["nnc_uid"] and row["node_uid"] == expected["node_uid"]
                    and row["network_container_id"] == expected["network_container_id"],
                    "Pinned VM/Node/boot/NNC/network-container identity changed")
            require(mocks._node_ready_and_schedulable(node) and stalled.fresh_node_ready(node)
                    and not maintenance._taints(node)
                    and self.reader.daemonsets <= maintenance._healthy_system_daemonsets_on_node(snapshot["pods"], name),
                    "A new worker or applicable Cilium/CNS/system DaemonSet lost readiness")
            view = self.reader.summary["arm_diagnostics"]["views"][f"{new['vmss']['name']}/{expected['instance_id']}"]
            require(stalled.guest_state(view, max_age_seconds=300) == "ready" and stalled.extensions_ready(view),
                    "New VM guest/extension readiness is missing or stale")
            baseline = self.summary.get("ip_growth", {}).get(name, {}).get("before")
            if baseline:
                require(row["version"] >= baseline["version"], "The allocated IP version regressed")
                if not self.probes_cleaned:
                    require(set(baseline["ip_addresses"]) <= set(row["ip_addresses"]),
                            "The pinned allocated IP baseline regressed")
                else:
                    after = self.summary["ip_growth"][name]["after"]
                    require(row["version"] >= after["version"]
                            and (set(row["ip_addresses"]) == set(after["ip_addresses"])
                                 or row["version"] > after["version"]),
                            "Post-cleanup allocation changed without a newer version")
                    residents = {pod.get("status", {}).get("podIP") for pod in snapshot["pods"]["items"]
                                 if pod["spec"].get("nodeName") == name and not pod["spec"].get("hostNetwork")
                                 and pod.get("status", {}).get("podIP")}
                    require(residents <= set(row["ip_addresses"]), "Post-cleanup allocation lost a resident Pod IP")
                    self.summary.setdefault("post_cleanup_allocation", {})[name] = {
                        "version": row["version"], "assigned_ip_count": row["assigned_ip_count"],
                        "proven_growth_capacity": after["assigned_ip_count"],
                        "resident_ip_count": len(residents),
                        "unused_ips_released": sorted(set(after["ip_addresses"]) - set(row["ip_addresses"])),
                    }
        agents = maintenance._agent_map(snapshot["pods"])
        for name, expected_uid in self.inputs["healthy_agents"].items():
            require(name in agents and uid(agents[name]) == expected_uid and base.pod_ready(agents[name]),
                    "One of the 44 protected healthy source mocks changed/regressed")
        self.summary["current_kwok_ready"] = self.reader.summary["current_kwok_ready"]
        self.summary["current_mock_ready"] = self.reader.summary["current_mock_ready"]
        self.summary["current_mock_uids"] = self.reader.summary["current_mock_uids"]
        self.save()
        return actual

    def observe(self):
        self.unchanged_inputs()
        self.reader.authority()
        snapshot = self.reader.snapshot()
        new = self.reader.models(snapshot)
        networks = self.guard(snapshot, new)
        self.historical_journal()
        operation = self.reader.az_json("aks", "operation", "show-latest", "--resource-group", base.RESOURCE_GROUP,
                                        "--name", base.CLUSTER, "--nodepool-name", capacity.POOL, "--query", base.OPERATION_QUERY)
        original = self.inputs["observation"]["cniv5-operation.json"]
        require(operation.get("name") == original["name"] and operation.get("status") == "Succeeded"
                and operation.get("operationType") == "PutAgentPool" and not operation.get("errorCode"),
                "The accepted cniv5 customer operation changed")
        return snapshot, networks

    def kwok_diagnostics(self, snapshot):
        deployment = base.controller(snapshot, "Deployment", "kube-system", "kwok-controller")
        replicasets = {
            uid(row) for row in snapshot["controllers"]["items"] if row["kind"] == "ReplicaSet"
            and row["metadata"].get("namespace") == "kube-system"
            and base.controller_owner(row, "Deployment")["uid"] == uid(deployment)
        }
        pods = [row for row in snapshot["pods"]["items"] if row["metadata"].get("namespace") == "kube-system"
                and not row["metadata"].get("deletionTimestamp") and any(
                    owner.get("controller") is True and owner.get("kind") == "ReplicaSet" and owner.get("uid") in replicasets
                    for owner in row["metadata"].get("ownerReferences", []))]
        require(len(pods) == 1, "Current KWOK controller Pod ownership is ambiguous")
        pod = pods[0]
        require(base.pod_ready(pod), "The selected current KWOK controller is not Ready")
        logs = self.run(["kubectl", "-n", "kube-system", "logs", pod["metadata"]["name"], "--all-containers=true",
                         "--tail=200", "--timestamps=true", "--limit-bytes=32768", "--request-timeout=45s"])
        leases = self.kube("-n", "kube-system", "get", "leases", "-o", "json")
        terms = ("leader", "lease", "forbidden", "unauthorized", "timeout", "error", "node", "update")
        self.summary["kwok_diagnostics"] = {
            "pod": stalled.pod_pin(pod), "ready": base.pod_ready(pod), "kwok_ready": self.summary["current_kwok_ready"],
            "log_bytes": len(logs.encode()), "log_sha256": hashlib.sha256(logs.encode()).hexdigest(),
            "log_category_counts": {term: logs.lower().count(term) for term in terms},
            "raw_logs_published": False,
            "redacted_log_excerpt": redact_diagnostic(logs[:32768]).splitlines()[-200:],
            "deployment": {"uid": uid(deployment), "conditions": deployment.get("status", {}).get("conditions", []),
                           "template": redact_diagnostic(deployment["spec"].get("template", {}))},
            "bootstrap_health_established": False,
            "leases": [{"name": row["metadata"]["name"], "uid": uid(row),
                        "spec": {key: row.get("spec", {}).get(key) for key in (
                            "holderIdentity", "leaseDurationSeconds", "acquireTime", "renewTime", "leaseTransitions")}}
                       for row in leases["items"] if "kwok" in row["metadata"]["name"].lower()
                       or "kwok" in str(row.get("spec", {}).get("holderIdentity", "")).lower()],
        }
        diagnostics = self.summary["kwok_diagnostics"]
        node_leases = self.optional_kwok_read("-n", "kube-node-lease", "get", "leases", "-o", "json")
        expected_nodes = maintenance.EXPECTED_AGENT_NAMES
        if node_leases["read_status"] == "read":
            rows = [row for row in mocks._items(node_leases.pop("payload"), "KWOK Node leases")
                    if row.get("metadata", {}).get("name") in expected_nodes]
            names = [row["metadata"]["name"] for row in rows]
            require(len(names) == len(set(names))
                    and all(row["metadata"].get("namespace") == "kube-node-lease" for row in rows),
                    "KWOK Node lease names or namespaces are ambiguous")
            node_leases["items"] = [{
                "name": row["metadata"]["name"], "uid": uid(row),
                "owner_references": row["metadata"].get("ownerReferences") or [],
                "spec": {key: row.get("spec", {}).get(key) for key in (
                    "holderIdentity", "leaseDurationSeconds", "acquireTime", "renewTime", "leaseTransitions")},
            } for row in rows]
            node_leases["missing_names"] = sorted(expected_nodes - set(names))
        node_leases["expected_node_count"] = 100
        diagnostics["node_leases"] = node_leases
        diagnostics["kwok_node_heartbeats"] = [{
            "name": node["metadata"]["name"], "uid": uid(node),
            "kwok_annotation": node["metadata"].get("annotations", {}).get("kwok.x-k8s.io/node"),
            "phase": node.get("status", {}).get("phase"),
            "ready_conditions": [{key: row.get(key) for key in (
                "status", "reason", "lastHeartbeatTime", "lastTransitionTime")}
                for row in node.get("status", {}).get("conditions", []) if row.get("type") == "Ready"],
        } for node in snapshot["nodes"]["items"] if node["metadata"]["name"] in expected_nodes]
        diagnostics["unknown_readiness_explained"] = False
        config = self.optional_kwok_read("-n", "kube-system", "get", "configmap", "kwok", "-o", "json")
        if config["read_status"] == "read":
            row = config.pop("payload")
            require(row.get("metadata", {}).get("name") == "kwok"
                    and row["metadata"].get("namespace") == "kube-system", "KWOK ConfigMap scope differs")
            text = (row.get("data") or {}).get("kwok.yaml")
            config.update(uid=uid(row), name="kwok", namespace="kube-system",
                          data_keys=sorted((row.get("data") or {}).keys()), binary_data_published=False)
            if isinstance(text, str) and len(text.encode()) <= 262144:
                try:
                    documents = list(yaml.safe_load_all(text))
                except yaml.YAMLError:
                    config["configuration_status"] = "unparseable"
                else:
                    config["configuration_status"] = "parsed"
                    config["kwok_yaml_sha256"] = hashlib.sha256(text.encode()).hexdigest()
                    config["documents"] = [
                        redact_diagnostic(document) if isinstance(document, dict)
                        and document.get("kind") in ("KwokConfiguration", "Stage")
                        else {"content": "<redacted-unrecognized-config-document>"}
                        for document in documents
                    ]
            else:
                config["configuration_status"] = "missing-or-over-size-bound"
        diagnostics["configmap"] = config
        stages = self.optional_kwok_read("get", "stages.kwok.x-k8s.io", "-o", "json")
        if stages["read_status"] == "read":
            rows = mocks._items(stages.pop("payload"), "KWOK Stages")
            require(len(rows) <= 100 and len(json.dumps(rows).encode()) <= 524288,
                    "KWOK Stage diagnostic inventory exceeds its bound")
            stages["items"] = [redact_diagnostic({
                "apiVersion": row.get("apiVersion"), "kind": row.get("kind"), "metadata": row.get("metadata"),
                "spec": row.get("spec"),
            }) for row in rows]
        diagnostics["stages"] = stages
        self.save()

    def optional_kwok_read(self, *command):
        try:
            payload = self.kube(*command)
        except workers.ReconcileError as error:
            text = str(error)
            if base.AUTH_ERROR.search(text):
                status = "denied"
            elif re.search(r"not.?found|doesn.t have a resource type|could not find", text, re.I):
                status = "unavailable"
            else:
                raise
            return {"read_status": status, "error_sha256": hashlib.sha256(text.encode()).hexdigest(),
                    "bootstrap_health_established": False}
        return {"read_status": "read", "payload": payload, "bootstrap_health_established": False}

    def metrics(self, snapshot, networks, *, include_probes=False):
        node_rows = self.kube("get", "--raw", "/apis/metrics.k8s.io/v1beta1/nodes")["items"]
        pod_rows = self.kube("get", "--raw", "/apis/metrics.k8s.io/v1beta1/namespaces/mock-clustermesh/pods")["items"]
        def normalize(row):
            item = copy.deepcopy(row)
            observed = base.timestamp(item.get("timestamp"), "metrics timestamp")
            require(0 <= (datetime.now(timezone.utc) - observed).total_seconds() <= 180, "Metrics are stale")
            item["timestamp"] = observed.isoformat(timespec="microseconds")
            return item
        nodes = maintenance._real_node_map(snapshot["nodes"])
        metrics = {row["metadata"]["name"]: normalize(row) for row in node_rows
                   if row["metadata"]["name"] in self.identities}
        samples = maintenance._pod_memory_usage_bytes({
            "items": [normalize(row) for row in pod_rows if row["metadata"]["name"] in self.inputs["healthy_agents"]],
        }, mocks.DEFAULT_NAMESPACE)
        require(set(metrics) == set(self.identities) and set(self.inputs["healthy_agents"]) <= set(samples),
                "Fresh actual metrics for both destinations and all 44 healthy mocks are required")
        self.rss_high = max(self.rss_high, max(samples[name]["memory_bytes"] for name in self.inputs["healthy_agents"]))
        agents = maintenance._agent_map(snapshot["pods"])
        remaining = [agents[name] for name in sorted(self.inputs["remaining_agents"])]
        require(len(remaining) == 56, "All 56 remaining replacements must be projected together")
        template = base.controller(snapshot, "StatefulSet", mocks.DEFAULT_NAMESPACE, "kwok-node")["spec"]["template"]["spec"]
        request_cpu, request_memory = mocks._resource_requests({"spec": template})
        require(request_cpu > 0 and request_memory > 0 and all(
            mocks._resource_requests(pod) == (request_cpu, request_memory) for pod in remaining),
            "All remaining mock requests must match their unchanged controller")
        memory = max(self.rss_high, request_memory)
        schedule = mocks.assess_recovery_capacity(
            nodes_payload=snapshot["nodes"], pods_payload=snapshot["pods"], affected=remaining,
            saturated_nodes=sorted(set(nodes) - set(self.identities)), pod_template=template,
            config_settings=mocks.CapacityRepairConfig(),
        )
        require(schedule["sufficient"] and set(schedule["alternate_nodes"]) == set(self.identities),
                "The entire remaining mock set cannot fit the genuine destination scheduling inventory")
        capacities = {}
        for name, metric in metrics.items():
            row = schedule["nodes"][name]
            used_memory = int(mocks._quantity(metric["usage"].get("memory"), "actual memory"))
            used_cpu = int(mocks._quantity(metric["usage"].get("cpu"), "actual CPU") * 1000)
            require(used_memory > 0 and used_cpu >= 0, "Actual usage metrics are invalid")
            high = self.node_high.setdefault(name, {"memory": used_memory, "cpu": used_cpu})
            high["memory"], high["cpu"] = max(high["memory"], used_memory), max(high["cpu"], used_cpu)
            extra_probes = self.summary["ip_growth"].get(name, {}).get("probe_count", 0) if include_probes else 0
            free_memory = min(row["available_memory_bytes"],
                              int(row["allocatable_memory_bytes"] * .85)
                              - max(high["memory"], row["requested_memory_bytes"]) - 512 * 1024**2
                              - extra_probes * 16 * 1024**2)
            free_cpu = min(row["available_cpu_millicores"],
                           int(row["allocatable_cpu_millicores"] * .85)
                           - max(high["cpu"], row["requested_cpu_millicores"]) - 250 - extra_probes * 5)
            free_slots = row["available_pod_slots"] - extra_probes
            seats = max(0, min(free_memory // memory, free_cpu // request_cpu, free_slots))
            if self.growth_done:
                occupied = {pod.get("status", {}).get("podIP") for pod in snapshot["pods"]["items"]
                            if pod["spec"].get("nodeName") == name and not pod["spec"].get("hostNetwork")}
                occupied.discard(None)
                proven = self.summary["ip_growth"][name]["after"]["assigned_ip_count"]
                seats = min(seats, proven - len(occupied))
            capacities[name] = {"safe_slots": int(seats), "free_memory": free_memory, "free_cpu": free_cpu,
                                "free_pod_slots": free_slots, "metric_timestamp": metric["timestamp"],
                                "current_allocated_ips": networks[name]["assigned_ip_count"],
                                "proven_demand_allocation": self.summary["ip_growth"][name].get("after", {}).get("assigned_ip_count")}
        slots = {name: row["safe_slots"] for name, row in capacities.items()}
        placements = {}
        for pod in remaining:
            candidate = max(slots, key=lambda name: (slots[name], capacities[name]["free_memory"], name))
            require(slots[candidate] > 0, "Actual high-water/85% headroom cannot fit ALL 56 replacements")
            placements[pod["metadata"]["name"]] = candidate
            slots[candidate] -= 1
        counts = Counter(placements.values())
        for name, count in counts.items():
            used = int(mocks._quantity(metrics[name]["usage"]["memory"], "actual memory"))
            require(maintenance._headroom_ok(
                nodes[name], metrics[name], threshold_percent=85,
                effective_reserved_memory_bytes=max(self.node_high[name]["memory"] - used, 0) + 512 * 1024**2,
                next_memory_bytes=count * memory,
            ), "Projected actual-memory headroom is unsafe")
        self.summary["memory_projection"] = {
            "remaining_count": 56, "protected_sample_count": 44, "memory_per_agent_bytes": memory,
            "healthy_agent_rss_high_water_bytes": self.rss_high, "request_memory_bytes": request_memory,
            "threshold_percent": 85, "destinations": capacities, "placements": placements,
            "placement_counts": dict(counts), "node_high_water": copy.deepcopy(self.node_high),
            "not_a_scheduler_binding_or_resource_reservation": True,
        }
        self.save()

    def journal_data(self):
        return {
            "owner": OWNER, "token": self.token, "capacity_build": str(CAPACITY_BUILD),
            "observation_build": str(OBSERVATION_BUILD), "input_sha256": digest({
                "observation": self.inputs["observation_hashes"], "capacity": self.inputs["capacity_hashes"]}),
            "probe_receipts": json.dumps(self.summary["probe_receipts"], sort_keys=True),
            "state": self.summary["status"],
        }

    def raw_write(self, command, *, cleanup=False):
        require(self.args.execute and command[0] == "kubectl", "Qualification cannot write Azure or execute in plan mode")
        allowed = command[:6] in (
            ["kubectl", "-n", "kube-system", "create", "configmap", JOURNAL],
            ["kubectl", "-n", "kube-system", "patch", "configmap", JOURNAL],
        )
        if not allowed:
            require(command[:4] == ["kubectl", "-n", mocks.DEFAULT_NAMESPACE, "run"] and self.journal_uid,
                    "Only qualification journal/probe creation is permitted")
            name = command[command.index("run") + 1]
            require(any(row["name"] == name and row["token"] == self.token
                        for row in self.summary["probe_cleanup_pending"]), "Probe create has no owned durable intent")
            self.owned_journal()
            self.summary["probe_receipts"][name]["create_submission_started"] = True
        self.summary["mutation_started"] = True
        self.save()
        return super().run(command, 45, cleanup=cleanup)

    def owned_journal(self):
        current = self.kube("-n", "kube-system", "get", "configmap", JOURNAL, "-o", "json")
        metadata = current.get("metadata") or {}
        require(uid(current) == self.journal_uid and metadata.get("name") == JOURNAL
                and metadata.get("namespace") == "kube-system" and metadata.get("resourceVersion")
                and not metadata.get("deletionTimestamp") and not metadata.get("ownerReferences")
                and current.get("data") == self.persisted_journal_data,
                "Qualification journal UID/data/input/ownership changed")
        return current

    def persist_journal(self, *, cleanup=False):
        self.unchanged_inputs()
        current = self.owned_journal()
        desired = self.journal_data()
        self.raw_write(["kubectl", "-n", "kube-system", "patch", "configmap", JOURNAL, "--type=json", "-p", json.dumps([
            {"op": "test", "path": "/metadata/uid", "value": self.journal_uid},
            {"op": "test", "path": "/metadata/resourceVersion", "value": current["metadata"]["resourceVersion"]},
            {"op": "test", "path": "/data", "value": self.persisted_journal_data},
            {"op": "add", "path": "/data", "value": desired},
        ])], cleanup=cleanup)
        self.persisted_journal_data = desired
        self.owned_journal()

    def acquire(self):
        existing = self.kube("-n", "kube-system", "get", "configmaps", "--field-selector", f"metadata.name={JOURNAL}", "-o", "json")
        require(existing["items"] == [], "Existing qualification journal prohibits replay/adoption")
        self.summary["journal"].update(attempted=True, accepted=None, ambiguous=True)
        self.save()
        data = self.journal_data()
        result = workers.parse_json(self.raw_write([
            "kubectl", "-n", "kube-system", "create", "configmap", JOURNAL,
            *[f"--from-literal={key}={value}" for key, value in data.items()], "-o", "json",
        ]), "qualification journal")
        require(uid(result) and result.get("data") == data, "Qualification journal creation is ambiguous")
        self.journal_uid = uid(result)
        self.persisted_journal_data = data
        self.owned_journal()
        self.summary["journal"].update(uid=self.journal_uid, accepted=True, ambiguous=False)
        self.save()

    def kubectl(self, command, *, timeout_seconds=45, cleanup=False):
        if "run" not in command:
            return self.run(["kubectl", *command], timeout_seconds, cleanup=cleanup)
        name = command[command.index("run") + 1]
        require(name not in self.summary["probe_receipts"], "A probe creation may never be retried")
        intent = next(row for row in self.summary["probe_cleanup_pending"] if row["name"] == name)
        record = {"create_attempted": True, "create_submission_started": False,
                  "create_accepted": None, "create_ambiguous": True, "delete_attempted": False,
                  "node_name": intent["node_name"], "node_uid": self.identities[intent["node_name"]]["node_uid"],
                  "token": self.token, "requested_at": workers.utc_now()}
        self.summary["probe_receipts"][name] = record
        self.save()
        self.persist_journal()
        result = self.raw_write(["kubectl", *command])
        created = workers.parse_json(result, "probe creation response")
        self.owned_probe(created, intent)
        require(capacity.quantities.valid_uuid(uid(created)), "Probe creation response has no valid UID")
        record.update(create_accepted=True, create_ambiguous=False, uid=uid(created), accepted_at=workers.utc_now())
        self.save()
        self.persist_journal()
        return result

    def owned_probe(self, pod, intent):
        require(pod["metadata"].get("namespace") == mocks.DEFAULT_NAMESPACE
                and pod["metadata"].get("name") == intent["name"]
                and pod["metadata"].get("labels", {}).get(maintenance.PROBE_LABEL_KEY) == self.token
                and pod["spec"].get("nodeName") == intent["node_name"]
                and (not intent["uid"] or uid(pod) == intent["uid"])
                and base.pvc_free(pod["spec"]) and not pod["spec"].get("hostNetwork")
                and pod["spec"].get("restartPolicy") == "Never"
                and not pod["metadata"].get("ownerReferences")
                and not pod["spec"].get("initContainers") and not pod["spec"].get("ephemeralContainers")
                and len(pod["spec"].get("containers") or []) == 1
                and pod["spec"]["containers"][0].get("image") == maintenance.DEFAULT_PROBE_IMAGE
                and pod["spec"]["containers"][0].get("args") == maintenance.PROBE_COMMAND
                and mocks._resource_requests(pod) == (5, 16 * 1024**2),
                "Probe ownership/UID/spec changed; it cannot be adopted or deleted")

    def prove_growth(self):
        for name, proof in self.summary["ip_growth"].items():
            self.observe()  # One full guard per node batch, not per small probe.
            for index in range(proof["probe_count"]):
                maintenance._create_probe_pod(self, self.args, name, self.token, index, self.summary)
        deadline = min(self.work_deadline, time.monotonic() + 600)
        while True:
            snapshot, networks = self.observe()
            by_name = {pod["metadata"]["name"]: pod for pod in snapshot["pods"]["items"]
                       if pod["metadata"].get("namespace") == mocks.DEFAULT_NAMESPACE}
            all_ips = []
            complete = True
            for name, proof in self.summary["ip_growth"].items():
                intents = [row for row in self.summary["probe_cleanup_pending"] if row["node_name"] == name]
                pods = [by_name.get(row["name"]) for row in intents]
                require(len(intents) == proof["probe_count"], "Probe demand count changed")
                for pod, intent in zip(pods, intents):
                    if pod is not None:
                        self.owned_probe(pod, intent)
                        require(intent["uid"], "Accepted probe did not return a pinned UID")
                if not all(pod is not None and base.pod_ready(pod) for pod in pods):
                    complete = False
                    continue
                ips = [pod["status"]["podIP"] for pod in pods]
                require(len(set(ips)) == len(ips), "Probe IPs are not unique")
                all_ips.extend(ips)
                row, before = networks[name], proof["before"]
                grown = (row["version"] > before["version"] and row["assigned_ip_count"] > before["assigned_ip_count"]
                         and set(before["ip_addresses"]) < set(row["ip_addresses"])
                         and set(ips) <= set(row["ip_addresses"]) and bool(set(ips) - set(before["ip_addresses"])))
                if not grown:
                    complete = False
                    continue
                for pod in pods:
                    output = self.run(["kubectl", "get", "--raw",
                                       f"/api/v1/namespaces/{mocks.DEFAULT_NAMESPACE}/pods/{pod['metadata']['name']}:8080/proxy/hostname"])
                    require(output.strip() == pod["metadata"]["name"], "Owned Ready/IP HTTP probe did not answer")
                proof.update(after=row, ready_ips=ips, probe_uids=[uid(pod) for pod in pods], http_proven=True)
            require(len(set(all_ips)) == len(all_ips), "Destinations returned duplicate probe IPs")
            self.save()
            if complete:
                self.growth_done = True
                return
            require(time.monotonic() < deadline, "Actual IP/version growth did not converge")
            time.sleep(min(10, self.remaining_seconds(10)))

    def cleanup(self):
        if not self.summary["probe_cleanup_pending"]:
            return
        previous_mode, previous_deadline = self.cleanup_mode, self.cleanup_deadline
        reader_deadline = self.reader.work_deadline
        self.cleanup_mode = True
        self.cleanup_deadline = min(self.cleanup_deadline, time.monotonic() + RESERVE_SECONDS)
        self.reader.work_deadline = self.cleanup_deadline
        try:
            self.cleanup_probes()
        finally:
            self.cleanup_mode, self.cleanup_deadline = previous_mode, previous_deadline
            self.reader.work_deadline = reader_deadline

    def cleanup_probes(self):
        pods = self.kube("-n", mocks.DEFAULT_NAMESPACE, "get", "pods", "-o", "json")
        by_name = {pod["metadata"]["name"]: pod for pod in pods["items"]}
        for intent in list(self.summary["probe_cleanup_pending"]):
            record = self.summary["probe_receipts"][intent["name"]]
            pod = by_name.get(intent["name"])
            if pod is None and (record.get("delete_accepted") or intent["uid"]
                                or record.get("create_submission_started") is False):
                self.summary["probe_cleanup_pending"].remove(intent)
                continue
            require(pod is not None, "Ambiguous probe creation cannot be certified absent without a known UID")
            self.owned_probe(pod, intent)
            require(capacity.quantities.valid_uuid(uid(pod)), "Owned probe UID is invalid")
            if not intent["uid"]:
                intent["uid"] = uid(pod)
                record["uid_resolved_after_ambiguous_create"] = uid(pod)
            if record["delete_attempted"]:
                continue  # Observe an accepted or delivery-ambiguous delete; never submit it again.
            record.update(delete_attempted=True, delete_accepted=None, delete_ambiguous=True, uid=intent["uid"])
            self.save()
            self.persist_journal(cleanup=True)
            self.delete_pod(self.cluster, namespace=mocks.DEFAULT_NAMESPACE, name=intent["name"], uid=intent["uid"],
                            timeout_seconds=self.remaining_seconds(45, cleanup=True), attempts=1, retry_seconds=0)
            record.update(delete_accepted=True, delete_ambiguous=False)
            self.save()
        while self.summary["probe_cleanup_pending"]:
            pods = self.kube("-n", mocks.DEFAULT_NAMESPACE, "get", "pods", "-o", "json")
            by_name = {pod["metadata"]["name"]: pod for pod in pods["items"]}
            remaining = []
            for intent in self.summary["probe_cleanup_pending"]:
                pod = by_name.get(intent["name"])
                if pod is not None:
                    self.owned_probe(pod, intent)
                    remaining.append(intent)
            self.summary["probe_cleanup_pending"] = remaining
            self.save()
            if remaining:
                time.sleep(self.remaining_seconds(5, cleanup=True))
        self.persist_journal(cleanup=True)
        self.probes_cleaned = True

    def complete_read_only(self):
        require(not self.args.execute and self.completed is not None, "Completion must be read-only")
        snapshot, networks = self.observe()
        prior = self.completed
        journal = self.kube("-n", "kube-system", "get", "configmap", JOURNAL, "-o", "json")
        token = next(iter(prior["probe_receipts"].values()))["token"]
        expected_data = {
            "owner": OWNER, "token": token, "capacity_build": str(CAPACITY_BUILD),
            "observation_build": str(OBSERVATION_BUILD), "input_sha256": digest(prior["input_hashes"]),
            "probe_receipts": json.dumps(prior["probe_receipts"], sort_keys=True),
            "state": "proving-real-ip-growth",
        }
        require(uid(journal) == COMPLETED_JOURNAL_UID and journal.get("data") == expected_data
                and journal["metadata"].get("name") == JOURNAL
                and journal["metadata"].get("namespace") == "kube-system"
                and not journal["metadata"].get("deletionTimestamp") and not journal["metadata"].get("ownerReferences"),
                "The original completed-probe journal changed")
        require(not any(maintenance.PROBE_LABEL_KEY in pod["metadata"].get("labels", {})
                        or pod["metadata"]["name"].startswith("cni-maint-probe-") for pod in snapshot["pods"]["items"]),
                "Live probe residue prevents read-only completion")
        captured = allocation_map(prior["read_only_capacity_guard"]["kubernetes_diagnostics"]["nnc"])
        require(all(networks[name]["version"] >= captured[name]["version"] for name in self.identities),
                "The current allocation predates the recorded clean probe outcome")
        self.metrics(snapshot, networks)
        self.kwok_diagnostics(snapshot)
        self.historical_journal()
        final = self.kube("-n", "kube-system", "get", "configmap", JOURNAL, "-o", "json")
        require(uid(final) == uid(journal) and final.get("data") == expected_data,
                "Completed-probe journal changed during read-only finalization")
        require(not final["metadata"].get("deletionTimestamp") and not final["metadata"].get("ownerReferences"),
                "Completed-probe journal lifecycle changed during finalization")
        self.summary["journal"].update(uid=uid(journal), observed_existing=True, attempted=False)
        self.summary.update(plan_valid=True, success=True, capacity_qualified=True,
                            actual_ip_growth_proven=True, actual_memory_headroom_proven=True,
                            status="capacity-qualified-read-only-from-cleaned-probes")
        self.save()

    def execute(self):
        if self.completed:
            self.complete_read_only()
            return
        snapshot, networks = self.observe()
        require(not self.kube("-n", "kube-system", "get", "configmaps", "--field-selector",
                              f"metadata.name={JOURNAL}", "-o", "json")["items"], "Existing qualification journal blocks replay")
        require(not any(maintenance.PROBE_LABEL_KEY in pod["metadata"].get("labels", {})
                        or pod["metadata"]["name"].startswith("cni-maint-probe-") for pod in snapshot["pods"]["items"]),
                "Existing probe Pods prohibit automatic replay/adoption")
        self.reader.quota()
        self.kwok_diagnostics(snapshot)
        for name, expected in self.identities.items():
            before = networks[name]
            require(before["version"] >= expected["initial_network"]["version"],
                    "The recorded new-worker allocation version regressed")
            occupied = {pod.get("status", {}).get("podIP") for pod in snapshot["pods"]["items"]
                        if pod["spec"].get("nodeName") == name and not pod["spec"].get("hostNetwork")}
            count = max(2, len(set(before["ip_addresses"]) - occupied) + 1)
            require(count <= 64, "Probe demand exceeds the bounded per-node limit")
            self.summary["ip_growth"][name] = {"before": before, "probe_count": count, "http_proven": False}
        self.metrics(snapshot, networks, include_probes=True)
        self.summary.update(plan_valid=True, status="planned-read-only")
        self.save()
        if not self.args.execute:
            return
        self.observe()
        self.acquire()
        self.summary["status"] = "proving-real-ip-growth"
        self.prove_growth()
        self.cleanup()
        snapshot, networks = self.observe()
        self.metrics(snapshot, networks)
        require(not self.summary["probe_cleanup_pending"] and not self.summary["cleanup_errors"],
                "Uncertain cleanup prevents qualification")
        self.summary["status"] = "qualification-proofs-complete"
        self.persist_journal(cleanup=True)
        self.summary.update(success=True, capacity_qualified=True, actual_ip_growth_proven=True,
                            actual_memory_headroom_proven=True, status="capacity-qualified-not-bootstrap-or-workload-qualified")
        self.save()


def validate_args(args):
    require(args.resource_group == args.confirm_resource_group == base.RESOURCE_GROUP
            and args.expected_subscription.lower() == base.SUBSCRIPTION and args.expected_region.lower() == base.REGION
            and capacity.quantities.valid_sha(args.expected_tfvars_sha), "Qualification scope/tfvars fingerprint changed")
    require(args.observation_build_id == OBSERVATION_BUILD and args.capacity_build_id == CAPACITY_BUILD,
            "Only observations 79975 and accepted capacity 79971 are supported")
    require(base.integer(args.timeout_seconds) and 600 <= args.timeout_seconds <= 2400, "Timeout must be 600..2400 seconds")
    require(args.kubeconfig and args.context == base.CLUSTER, "Private explicit mesh96 context is required")
    completed_path = getattr(args, "completed_qualification_checkpoint", None)
    completed_build = getattr(args, "completed_qualification_build_id", 0)
    require((not completed_path and completed_build == 0)
            or (completed_path and completed_build == COMPLETED_PROBE_BUILD and not args.execute),
            "Cleaned-probe completion requires exact build 79979 and cannot execute mutations")
    observation, capacity_dir, output, config = (Path(value).resolve() for value in (
        args.observation_directory, args.capacity_directory, args.summary_file, args.kubeconfig))
    require(len({observation, capacity_dir, output, config}) == 4 and not output.exists()
            and observation not in output.parents and capacity_dir not in output.parents,
            "Summary must be new and outside immutable inputs/private credentials")
    if completed_path:
        require(Path(completed_path).resolve() not in (observation, capacity_dir, output, config),
                "Completed proof must be separate from outputs and other inputs")
    args.role = base.ROLE
    args.probe_image = maintenance.DEFAULT_PROBE_IMAGE
    args.request_timeout_seconds = 45


def execute_qualification(args, summary, runner=workers.run_command, delete_pod=None):
    validate_args(args)
    summary.update(schema_version=1, execute=args.execute, mutation_started=False, plan_valid=False,
                   success=False, capacity_qualified=False, actual_ip_growth_proven=False,
                   actual_memory_headroom_proven=False, workloads_ready=False, bootstrap_complete=False,
                   status="validating", started_at=workers.utc_now(), ip_growth={}, probe_receipts={},
                   probe_cleanup_pending=[], cleanup_errors=[], journal={"name": JOURNAL, "retained": True})
    operation = None
    try:
        inputs = load_inputs(args)
        completed, completed_hash = load_completed_proof(args, inputs)
        summary.update(input_hashes={"observation": inputs["observation_hashes"], "capacity": inputs["capacity_hashes"]},
                       historical_capacity_create=inputs["receipt"]["create"],
                       historical_capacity_journal_uid=uid(inputs["observation"]["cniv5-capacity-journal.json"]),
                       identities=inputs["identities"], protected_healthy_mock_uids=inputs["healthy_agents"],
                       remaining_replacement_uids=inputs["remaining_agents"],
                       original_mock_uids=inputs["receipt"]["original_mock_pod_uids"],
                       preserved_kwok_uids=inputs["receipt"]["preserved_kwok_node_uids"],
                       plan_sha256=stalled.PLAN_SHA)
        if completed:
            summary.update(completion_only=True, completed_probe_build=COMPLETED_PROBE_BUILD,
                           completed_probe_checkpoint_sha256=completed_hash)
        operation = Qualification(args, inputs, summary, runner, delete_pod or mocks.delete_pod_with_uid_precondition,
                                  completed=completed, completed_hash=completed_hash)
        operation.execute()
    except EXPECTED_ERRORS as error:
        summary.update(success=False, capacity_qualified=False, actual_ip_growth_proven=False,
                       actual_memory_headroom_proven=False, status="failed-closed", error=str(error))
        if operation is not None and summary["probe_cleanup_pending"]:
            try:
                operation.cleanup()
            except EXPECTED_ERRORS as cleanup_error:
                summary["cleanup_errors"].append(str(cleanup_error))
        raise
    finally:
        summary["finished_at"] = workers.utc_now()
        mocks.write_json_atomic(args.summary_file, summary)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("resource-group", "confirm-resource-group", "expected-subscription", "expected-region",
                 "expected-tfvars-sha", "observation-directory", "capacity-directory", "kubeconfig", "summary-file"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--observation-build-id", type=int, required=True)
    parser.add_argument("--capacity-build-id", type=int, required=True)
    parser.add_argument("--context", default=base.CLUSTER)
    parser.add_argument("--timeout-seconds", type=int, default=2400)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--completed-qualification-checkpoint")
    parser.add_argument("--completed-qualification-build-id", type=int, default=0)
    return parser.parse_args(argv)


def main(argv=None):
    def interrupted(signum, _frame):
        raise workers.ReconcileError(f"Interrupted ({signum}); retain all qualification receipts")
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, interrupted)
    args, summary = parse_args(argv), {}
    try:
        execute_qualification(args, summary)
    except EXPECTED_ERRORS as error:
        print(f"Capacity qualification failed closed: {error}", file=sys.stderr)
        return 1
    print(f"{summary['status']}; bootstrap_complete=false; workloads_ready=false")
    return 0


if __name__ == "__main__":
    sys.exit(main())
