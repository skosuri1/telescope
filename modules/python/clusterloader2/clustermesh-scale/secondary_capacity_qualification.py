#!/usr/bin/env python3
"""Qualify the seven source-bound secondary DSv5 workers without production mutation."""

# pylint: disable=protected-access,too-many-lines,too-many-branches,too-many-statements

from __future__ import annotations

import argparse
import copy
import hashlib
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

import cni_worker_maintenance as maintenance
import mock_cni_recovery as mocks
import preserved_worker_reconcile as workers
import secondary_capacity_recovery as capacity
import stalled_retained_worker_recovery as stalled


ROLES = ("mesh-51", "mesh-66", "mesh-79", "mesh-89")
SYSTEM_ROLES = ("mesh-51", "mesh-66", "mesh-79")
EXPECTED_HEALTHY_MOCKS = {"mesh-51": 39, "mesh-66": 52, "mesh-79": 60}
EXPECTED_REPLACEMENTS = {"mesh-51": 61, "mesh-66": 48, "mesh-79": 40}
OWNER = "secondary-capacity-qualification"
JOURNAL_PREFIX = "secondary-capacity-qualification"
ACCEPTED_CAPACITY_BUILD = 80029
ACCEPTED_RECOVERY_SHA = "10fa2206fcc9373882d7f19106545d3f49d9140e66daaa76c3588a39944f640f"
ACCEPTED_PLAN_SHA = "c4206c837ab810d40d0c9d4d523d493e92a75d18c12b6eea2a27e81ab7b81cf7"
ACCEPTED_ROLE = "mesh-51"
ACCEPTED_JOURNAL_UID = "2d395a10-8d9f-4e47-9a39-503014ec6e49"
ACCEPTED_JOURNAL_RV = "14582445"
ACCEPTED_OPERATION = "bd372efc-2f92-4959-b4eb-747eb5dc0816"
NATIVE_BUNDLE_SCHEMA = 1
NATIVE_HOLD_KEY = "mock-clustermesh/secondary-capacity-fencing"
MAX_INPUT_FILE_BYTES = 128 * 1024 * 1024
MAX_PROBES_PER_NODE = 64
PROBE_WAIT_SECONDS = 900
FINAL_RESERVE_SECONDS = 300
POLL_SECONDS = 10
PROM_MEMORY_RESERVE = 16 * 1024**3
MEMORY_SAFETY_RESERVE = 512 * 1024**2
HEADROOM_PERCENT = 85
EXPECTED_ERRORS = (
    workers.ReconcileError, mocks.RecoveryError, OSError, ValueError, TypeError,
    KeyError, json.JSONDecodeError,
)


def require(condition, message):
    if not condition:
        raise workers.ReconcileError(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value):
    encoded = value if isinstance(value, bytes) else canonical(value).encode()
    return hashlib.sha256(encoded).hexdigest()


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def read_json(path, *, maximum_bytes=MAX_INPUT_FILE_BYTES):
    """Read a bounded receipt, including the genuine ~39MiB capacity receipts."""

    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"{path}: duplicate JSON key {key}")
            result[key] = value
        return result

    require(0 < Path(path).stat().st_size <= maximum_bytes,
            f"{path}: receipt size is outside the bounded limit")
    content = Path(path).read_bytes()
    require(0 < len(content) <= maximum_bytes, f"{path}: receipt size is outside the bounded limit")
    return json.loads(content, object_pairs_hook=unique)


def hash_tree(directory):
    root = Path(directory).resolve()
    require(root.is_dir() and not Path(directory).is_symlink(),
            "Capacity input directory is missing or symlinked")
    result = {}
    for path in sorted(root.rglob("*")):
        require(not path.is_symlink(), "Capacity input tree contains a symlink")
        if path.is_file():
            require(path.stat().st_size <= MAX_INPUT_FILE_BYTES,
                    "Capacity input file exceeds the 128MiB bound")
            result[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
        else:
            require(path.is_dir(), "Capacity input tree contains a nonregular entry")
    require(result, "Capacity input tree is empty")
    return result


def write_summary(path, summary):
    """Keep per-probe receipts compact; large observations have separate hash-bound files."""
    output = {**summary, "per_role": {}}
    for role, row in summary.get("per_role", {}).items():
        compact = dict(row)
        compact.pop("diagnostics", None)
        if row.get("final_evidence"):
            compact["final_evidence"] = dict(row["final_evidence"])
            compact["final_evidence"].pop("final_objects", None)
        output["per_role"][role] = compact
    mocks.write_json_atomic(path, output)


def object_uid(row):
    value = str((row.get("metadata") or {}).get("uid") or "")
    require(maintenance.UUID_RE.fullmatch(value), "Kubernetes UID is missing or malformed")
    return value


def resource_equal(left, right):
    return (
        isinstance(left, str) and isinstance(right, str)
        and left.rstrip("/").lower() == right.rstrip("/").lower()
    )


def pod_contract(pod):
    metadata = pod.get("metadata") or {}
    return {
        "uid": object_uid(pod), "namespace": metadata.get("namespace"),
        "name": metadata.get("name"), "spec_sha256": digest(pod.get("spec") or {}),
        "owners": copy.deepcopy(metadata.get("ownerReferences") or []),
        "deleting": bool(metadata.get("deletionTimestamp")),
    }


def node_contract(node):
    metadata = node.get("metadata") or {}
    return {
        "uid": object_uid(node),
        "labels": copy.deepcopy(metadata.get("labels") or {}),
        "spec": copy.deepcopy(node.get("spec") or {}),
        "boot_id": (node.get("status") or {}).get("nodeInfo", {}).get("bootID"),
        "ready": workers.node_is_ready(node),
    }


def pvc_free(spec):
    return not any(
        "persistentVolumeClaim" in volume or "ephemeral" in volume
        for volume in spec.get("volumes") or []
    )


def controller_pins(payload):
    result = {}
    for row in mocks._items(payload, "controller inventory"):
        metadata = row.get("metadata") or {}
        key = f"{row.get('kind')}/{metadata.get('namespace')}/{metadata.get('name')}"
        require(key not in result and object_uid(row),
                "Controller names or UIDs are ambiguous")
        result[key] = {
            "uid": object_uid(row),
            "generation": metadata.get("generation"),
            "spec_sha256": digest(row.get("spec") or {}),
        }
    return result


def pdb_evidence(payload):
    result = {}
    for row in mocks._items(payload, "PDB inventory"):
        metadata = row.get("metadata") or {}
        status = row.get("status") or {}
        name = f"{metadata.get('namespace')}/{metadata.get('name')}"
        generation = metadata.get("generation", 1)
        require(
            name not in result and object_uid(row)
            and (row.get("spec") or {}).get("unhealthyPodEvictionPolicy") == "AlwaysAllow"
            and isinstance(status.get("disruptionsAllowed"), int)
            and not isinstance(status["disruptionsAllowed"], bool)
            and status["disruptionsAllowed"] > 0
            and isinstance(status.get("observedGeneration"), int)
            and not isinstance(status["observedGeneration"], bool)
            and status["observedGeneration"] >= generation,
            f"{name}: current PDB generation/budget does not safely permit later native fencing",
        )
        result[name] = {
            "uid": object_uid(row), "generation": generation,
            "spec_sha256": digest(row.get("spec") or {}),
            "observed_generation": status["observedGeneration"],
            "disruptions_allowed": status["disruptionsAllowed"],
            "current_healthy": status.get("currentHealthy"),
            "desired_healthy": status.get("desiredHealthy"),
        }
    require(result, "At least one current PDB contract is required")
    return result


def timestamp(value, description):
    require(isinstance(value, str) and value, f"{description}: timestamp is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise workers.ReconcileError(f"{description}: timestamp is malformed") from error
    require(parsed.tzinfo is not None, f"{description}: timestamp lacks a timezone")
    return parsed.astimezone(timezone.utc)


def _valid_action(action):
    return (
        isinstance(action, dict)
        and action.get("attempted") is True
        and action.get("submission_started") is True
        and action.get("accepted") is True
        and action.get("ambiguous") is False
        and action.get("automatic_retry_allowed") is False
        and action.get("submission_started_at")
        and action.get("accepted_at")
    )


def _validate_identity(role, name, identity, network):
    require(
        isinstance(identity, dict) and identity.get("node_name") == name
        and maintenance.UUID_RE.fullmatch(str(identity.get("node_uid") or ""))
        and maintenance.UUID_RE.fullmatch(str(identity.get("vm_id") or ""))
        and maintenance.UUID_RE.fullmatch(str(identity.get("boot_id") or ""))
        and isinstance(identity.get("instance_id"), str)
        and str(identity.get("provider_id") or "").lower().startswith(
            f"azure:///subscriptions/{capacity.SUBSCRIPTION}/"
        )
        and isinstance(network, dict)
        and network.get("name") == name
        and network.get("node_uid") == identity["node_uid"]
        and maintenance.UUID_RE.fullmatch(str(network.get("uid") or ""))
        and maintenance.UUID_RE.fullmatch(str(network.get("network_container_id") or ""))
        and isinstance(network.get("version"), int) and not isinstance(network["version"], bool)
        and network["version"] >= 0
        and network.get("assigned_ip_count") == 16
        and isinstance(network.get("ip_addresses"), list)
        and len(set(network["ip_addresses"])) == 16,
        f"{role}/{name}: final capacity identity or initial 16-IP allocation is malformed",
    )


def _validate_capacity_role(role, row, source):
    desired = source["desired"]
    require(
        isinstance(row, dict) and row.get("role") == role
        and row.get("cluster") == source["cluster"]["name"]
        and row.get("plan_valid") is True
        and row.get("capacity_created") is True
        and row.get("initial_network_ready") is True
        and row.get("capacity_qualified") is False
        and row.get("workloads_ready") is False
        and row.get("source_pin_sha256") == source["pin_sha256"]
        and row.get("desired_configuration") == desired,
        f"{role}: final capacity receipt is not the exact successful unqualified role result",
    )
    action = row.get("action")
    journal = row.get("journal")
    require(
        _valid_action(action)
        and action.get("command") == capacity.pool_add_command(source)
        and timestamp(action["submission_started_at"], f"{role} submission")
        <= timestamp(action["accepted_at"], f"{role} acceptance")
        <= timestamp(action.get("returned_at"), f"{role} return")
        and isinstance(journal, dict)
        and journal.get("name") == f"{capacity.JOURNAL_PREFIX}-{role}"
        and journal.get("namespace") == "kube-system"
        and journal.get("retained") is True
        and journal.get("attempted") is True
        and journal.get("accepted") is True
        and journal.get("ambiguous") is False
        and maintenance.UUID_RE.fullmatch(str(journal.get("uid") or ""))
        and isinstance(journal.get("resource_version"), str)
        and journal["resource_version"]
        and maintenance.SHA256_RE.fullmatch(str(journal.get("data_sha256") or "")),
        f"{role}: final capacity action/journal receipt is not accepted and exact",
    )
    identities = row.get("new_identities")
    networks = row.get("new_networks")
    require(
        isinstance(identities, dict) and isinstance(networks, dict)
        and set(identities) == set(networks)
        and len(identities) == desired["count"],
        f"{role}: final capacity receipt has an incomplete new-worker identity set",
    )
    for name, identity in identities.items():
        _validate_identity(role, name, identity, networks[name])
    require(
        len({item["node_uid"] for item in identities.values()}) == desired["count"]
        and len({item["vm_id"] for item in identities.values()}) == desired["count"]
        and len({item["boot_id"] for item in identities.values()}) == desired["count"]
        and len({item["network_container_id"] for item in networks.values()}) == desired["count"],
        f"{role}: final capacity identities are not distinct",
    )


def _validate_accepted_checkpoint(root, final_receipt, source):
    accepted_root = root / "accepted-input"
    require(
        accepted_root.is_dir() and not accepted_root.is_symlink(),
        "Final capacity artifact lacks the immutable whole build-80029 checkpoint",
    )
    accepted_hashes = hash_tree(accepted_root)
    require(
        accepted_hashes.get("recovery.json") == ACCEPTED_RECOVERY_SHA
        and accepted_hashes.get("plan.json") == ACCEPTED_PLAN_SHA
        and any(name.startswith("source-input/") for name in accepted_hashes),
        "Accepted build-80029 checkpoint SHA/source-input lineage is not exact",
    )
    accepted = read_json(accepted_root / "recovery.json")
    accepted_plan = read_json(accepted_root / "plan.json")
    require(
        accepted_plan.get("schema_version") == 1
        and accepted_plan.get("execute") is False
        and accepted_plan.get("mutation_started") is False
        and accepted_plan.get("plan_valid") is True
        and accepted_plan.get("success") is True
        and accepted_plan.get("capacity_qualified") is False
        and accepted_plan.get("workloads_ready") is False
        and accepted_plan.get("completed_global_baseline") is False
        and accepted_plan.get("source_tree_hashes") == source["hashes"]
        and accepted_plan.get("source_tree_sha256") == source["tree_sha256"],
        "Accepted build-80029 plan is not the exact zero-mutation source-bound plan",
    )
    require(
        accepted.get("schema_version") == 1
        and accepted.get("execute") is True
        and accepted.get("mutation_started") is True
        and accepted.get("plan_valid") is True
        and accepted.get("success") is False
        and accepted.get("status") == "failed-closed"
        and accepted.get("capacity_qualified") is False
        and accepted.get("workloads_ready") is False
        and accepted.get("completed_global_baseline") is False
        and accepted.get("source_tree_hashes") == source["hashes"]
        and accepted.get("source_tree_sha256") == source["tree_sha256"]
        and set(accepted.get("per_role") or {}) == set(ROLES),
        "Accepted build-80029 recovery is not the exact failed partial-capacity checkpoint",
    )
    accepted_51 = accepted["per_role"][ACCEPTED_ROLE]
    accepted_journal = accepted_51.get("journal") or {}
    accepted_action = accepted_51.get("action") or {}
    require(
        accepted_51.get("status") == "add-accepted"
        and accepted_51.get("capacity_created") is False
        and accepted_51.get("initial_network_ready") is False
        and _valid_action(accepted_action)
        and accepted_journal.get("uid") == ACCEPTED_JOURNAL_UID
        and accepted_journal.get("resource_version") == ACCEPTED_JOURNAL_RV
        and accepted_journal.get("accepted") is True
        and accepted_journal.get("ambiguous") is False,
        "Accepted build-80029 does not contain the exact sole mesh-51 accepted add/journal",
    )
    for role in ROLES[1:]:
        row = accepted["per_role"][role]
        require(
            (row.get("action") or {}).get("attempted") is False
            and (row.get("action") or {}).get("submission_started") is False
            and (row.get("journal") or {}).get("attempted") is False,
            f"Accepted build-80029 unexpectedly attempted {role}",
        )
    continuation = final_receipt.get("continuation")
    require(
        isinstance(continuation, dict)
        and continuation.get("checkpoint_hashes") == accepted_hashes
        and continuation.get("accepted_role_observed_read_only") == ACCEPTED_ROLE
        and continuation.get("resume_build_id", continuation.get("source_build_id"))
        == ACCEPTED_CAPACITY_BUILD,
        "Final capacity receipt lacks the exact read-only build-80029 continuation chain",
    )
    final_51 = final_receipt["per_role"][ACCEPTED_ROLE]
    final_journal = final_51.get("journal") or {}
    final_action = final_51.get("action") or {}
    require(
        all(final_journal.get(key) == accepted_journal.get(key)
            for key in ("uid", "resource_version", "data_sha256"))
        and final_journal.get("attached_existing") is True
        and final_journal.get("created_in_build") == ACCEPTED_CAPACITY_BUILD
        and final_journal.get("read_only_continuation") is True
        and all(final_action.get(key) == accepted_action.get(key) for key in (
            "attempted", "submission_started", "accepted", "ambiguous",
            "automatic_retry_allowed", "command", "requested_at",
            "submission_started_at", "accepted_at", "returned_at",
        ))
        and final_action.get("operation_name") == ACCEPTED_OPERATION,
        "Final mesh-51 proof rewrote/replayed the accepted journal/action instead of observing it read-only",
    )
    accepted_source_hashes = {
        name.removeprefix("source-input/"): value
        for name, value in accepted_hashes.items()
        if name.startswith("source-input/")
    }
    require(
        accepted_source_hashes == source["hashes"],
        "Accepted build-80029 source-input differs from the final embedded build-80022 source",
    )
    return accepted_hashes


def _monitoring_pins(source_root, role, capacity_role):
    pods = capacity.read_json(Path(source_root) / role / "pods.json")
    result = {}
    for pod in mocks._items(pods, f"{role} source Pod inventory"):
        metadata = pod.get("metadata") or {}
        if metadata.get("namespace") != "monitoring":
            continue
        pinned = copy.deepcopy(pod)
        if (role == "mesh-89" and str(metadata.get("name", "")).startswith("prometheus-operator-")
                and not pod.get("spec", {}).get("nodeName")
                and pod.get("status", {}).get("phase") == "Pending"
                and not metadata.get("deletionTimestamp")):
            matches = [
                item for item in capacity_role["diagnostics"]["kubernetes"]["pods"]["items"]
                if object_uid(item) == object_uid(pod)
            ]
            require(len(matches) == 1 and stalled.base.pod_ready(matches[0]),
                    "mesh-89: the original Pending operator did not naturally become Ready")
            current = matches[0]
            name = current.get("spec", {}).get("nodeName")
            require(name in capacity_role["new_identities"]
                    and pod.get("spec", {}).get("nodeSelector", {}).get("prometheus") == "true",
                    "mesh-89: the original operator was not assigned to the approved promv5 worker")
            pinned["spec"]["nodeName"] = name
            require(pod_contract(stalled.safe_diagnostics(pinned)) == pod_contract(current),
                    "mesh-89: the original operator changed by more than natural scheduler assignment")
        pin = pod_contract(pinned)
        require(pin["uid"] not in result, f"{role}: source monitoring Pod UIDs are duplicated")
        result[pin["uid"]] = pin
    require(result, f"{role}: source monitoring Pod inventory is empty")
    return result


def load_inputs(args):
    root = Path(args.capacity_directory).resolve()
    hashes = hash_tree(root)
    require(
        {"recovery.json", "plan.json", "source-input/summary.json"} <= set(hashes),
        "Final capacity artifact lacks recovery, plan, or complete source-input",
    )
    receipt = read_json(root / "recovery.json")
    plan = read_json(root / "plan.json")
    source_args = SimpleNamespace(source_directory=str(root / "source-input"))
    source = capacity.load_source(source_args)
    require(
        source["tree_sha256"] == digest(source["hashes"]),
        "Source-input hash calculation is inconsistent",
    )
    require(
        source["tree_sha256"] == "ceed8d3237168c48702f2e85257848d21b6f653a36cca5dc9467942a28c0c48a"
        and receipt.get("source_tree_hashes") == source["hashes"]
        and receipt.get("source_tree_sha256") == source["tree_sha256"]
        and plan.get("source_tree_hashes") == source["hashes"]
        and plan.get("source_tree_sha256") == source["tree_sha256"],
        "Capacity artifact is not bound to the exact build-80022 source tree",
    )
    require(
        plan.get("schema_version") == 1 and plan.get("execute") is False
        and plan.get("mutation_started") is False and plan.get("plan_valid") is True
        and plan.get("success") is True and plan.get("status") == "plan-valid"
        and plan.get("capacity_qualified") is False
        and plan.get("workloads_ready") is False
        and plan.get("completed_global_baseline") is False,
        "Capacity plan is not the genuine zero-mutation plan",
    )
    require(
        receipt.get("schema_version") == 1 and receipt.get("execute") is True
        and receipt.get("mutation_started") is True and receipt.get("plan_valid") is True
        and receipt.get("success") is True
        and receipt.get("capacity_created") is True
        and receipt.get("initial_network_ready") is True
        and receipt.get("capacity_qualified") is False
        and receipt.get("workloads_ready") is False
        and receipt.get("completed_global_baseline") is False
        and str(receipt.get("status") or "").startswith("secondary-capacity")
        and receipt.get("source_build_id") == capacity.DIAGNOSTIC_BUILD
        and receipt.get("diagnosed_build_id") == capacity.DIAGNOSED_BUILD
        and receipt.get("automatic_resume_or_adoption") is False
        and receipt.get("roles") == list(ROLES)
        and isinstance(receipt.get("per_role"), dict)
        and set(receipt["per_role"]) == set(ROLES),
        "Only a genuine final successful capacity receipt may authorize qualification",
    )
    for role in ROLES:
        _validate_capacity_role(role, receipt["per_role"][role], source["roles"][role])
    accepted_hashes = _validate_accepted_checkpoint(root, receipt, source)
    monitoring = {
        role: _monitoring_pins(root / "source-input", role, receipt["per_role"][role]) for role in ROLES
    }
    require(hash_tree(root) == hashes, "Final capacity artifact changed while loading")
    return {
        "root": root, "hashes": hashes, "tree_sha256": digest(hashes),
        "receipt": receipt, "plan": plan, "source": source,
        "monitoring_pins": monitoring, "accepted_checkpoint_hashes": accepted_hashes,
    }


class CapacityObserver:
    """Reuse the capacity helper's public role guard, but expose no write path."""

    def __init__(self, args, inputs, role, runner):
        self.args = args
        self.inputs = inputs
        self.role = role
        self.capacity_summary = copy.deepcopy(inputs["receipt"])
        deadline = time.monotonic() + args.timeout_seconds
        def read_only(command, timeout_seconds):
            allowed = command[0] == "az" and (
                command[1:3] in (
                    ["account", "show"], ["group", "show"], ["aks", "show"],
                    ["vmss", "list"], ["vmss", "show"],
                    ["vmss", "list-instances"], ["vmss", "get-instance-view"],
                ) or command[1:4] in (
                    ["aks", "nodepool", "list"], ["aks", "operation", "show-latest"],
                )
            )
            if command[0] == "kubectl":
                allowed = "get" in command and not any(word in command for word in (
                    "create", "patch", "delete", "run", "exec", "apply", "taint", "drain",
                ))
            require(allowed, "Capacity observation rejected a resource mutation")
            return runner(command, timeout_seconds)

        self.guard = capacity.RoleRecovery(
            args, inputs["source"], inputs["source"]["roles"][role],
            self.capacity_summary, read_only, deadline,
        )
        self.guard.save = lambda: None
        self.qualification_journal_name = f"{JOURNAL_PREFIX}-{role}"
        receipt = inputs["receipt"]["per_role"][role]
        recorded = receipt["diagnostics"]
        journals = self.guard.journal_inventory(recorded["kubernetes"]["configmaps"])
        journal = journals.pop(receipt["journal"]["name"])
        require(journal["uid"] == receipt["journal"]["uid"]
                and journal["resourceVersion"] == receipt["journal"]["resource_version"]
                and digest(journal["data"]) == receipt["journal"]["data_sha256"],
                f"{role}: final capacity snapshot differs from its retained journal receipt")
        self.guard.journal_uid = journal["uid"]
        self.guard.journal_rv = journal["resourceVersion"]
        self.guard.journal_pin = copy.deepcopy(journal["data"])
        self.guard.token = journal["data"]["token"]
        self.guard.existing_journals = journals
        vmsses = [row for row in recorded["vmsses"]
                  if workers.vmss_pool_name(row) == inputs["source"]["roles"][role]["desired"]["name"]]
        require(len(vmsses) == 1, f"{role}: final capacity VMSS identity is ambiguous")
        self.guard.new_vmss = vmsses[0]["name"]
        self.guard.new_identities = copy.deepcopy(receipt["new_identities"])
        self.guard.new_networks = copy.deepcopy(receipt["new_networks"])
        self.guard.read_only_accepted = True

    def observe(self):
        observed = self.guard.capture()
        configmaps = observed["kubernetes"]["configmaps"]
        original_configmaps = copy.deepcopy(configmaps)
        configmaps["items"] = [
            row for row in configmaps.get("items") or []
            if (row.get("metadata") or {}).get("name") != self.qualification_journal_name
        ]
        try:
            guarded = self.guard.guard_source(observed, allow_new=True)
            ready, reason = self.guard.new_pool_state(observed, guarded)
            require(ready, f"{self.role}: accepted secondary capacity is not fully ready: {reason}")
        finally:
            observed["kubernetes"]["configmaps"] = original_configmaps
        return observed, copy.deepcopy(self.guard.new_identities), copy.deepcopy(self.guard.new_networks)


class RoleQualification(maintenance.ClusterOperator):
    """One role's exclusive journal, probes, growth proof, and cleanup."""

    def __init__(self, args, inputs, role, summary, runner, delete_pod):
        source = inputs["source"]["roles"][role]
        role_args = copy.copy(args)
        role_args.source_directory = str(inputs["root"] / "source-input")
        role_args.kubeconfig = str(Path(args.kubeconfig_directory) / f"{role}.config")
        role_args.context = source["cluster"]["name"]
        deadline = time.monotonic() + args.timeout_seconds
        super().__init__(
            role_args, source["cluster"]["name"], runner,
            deadline - FINAL_RESERVE_SECONDS, deadline,
        )
        self.args = role_args
        self.inputs = inputs
        self.source = source
        self.role = role
        self.summary = summary
        self.delete_pod = delete_pod
        self.observer = CapacityObserver(role_args, inputs, role, runner)
        self.token = uuid.uuid4().hex
        self.journal_name = f"{JOURNAL_PREFIX}-{role}"
        self.journal_uid = ""
        self.journal_rv = ""
        self.journal_pin = None
        self.initial_journals = None
        self.probes_cleaned = False
        self.preflight_contracts = None
        self.healthy_rss_high_water = 0
        self.cluster = mocks.Cluster(
            role, role_args.kubeconfig, role_args.context,
            source["cluster"]["name"], capacity.RESOURCE_GROUP,
        )

    @property
    def role_summary(self):
        return self.summary["per_role"][self.role]

    def save(self):
        write_summary(self.args.summary_file, self.summary)

    def export_evidence(self, name, payload):
        summary_path = Path(self.args.summary_file)
        path = summary_path.parent / f"{summary_path.stem}-evidence" / f"{self.role}-{name}.json"
        mocks.write_json_atomic(path, payload)
        return {
            "path": str(path.relative_to(summary_path.parent)),
            "sha256": digest(path.read_bytes()),
        }

    def unchanged_inputs(self):
        require(
            hash_tree(self.args.capacity_directory) == self.inputs["hashes"],
            "Immutable final capacity artifact changed",
        )

    def read(self, command, timeout_seconds=60):
        command = list(command)
        require(
            command[0] == "kubectl"
            and ("get" in command or "logs" in command)
            and not any(word in command for word in (
                "create", "patch", "delete", "run", "exec", "apply", "taint", "drain",
            )),
            "Qualification read path rejected a mutation",
        )
        return super().run(command, timeout_seconds, cleanup=self.cleanup_mode)

    def kube(self, *command):
        return workers.parse_json(
            self.read([
                "kubectl", f"--request-timeout={self.args.request_timeout_seconds}s", *command,
            ], self.args.request_timeout_seconds),
            f"{self.role} qualification read",
        )

    def journals(self, payload, *, allow_own=False):
        selected = self.observer.guard.journal_inventory(payload)
        own = selected.pop(self.journal_name, None)
        if allow_own:
            require(own is not None, f"{self.role}: owned qualification journal disappeared")
        else:
            require(own is None, f"{self.role}: existing qualification journal blocks replay/adoption")
        expected = self.inputs["receipt"]["per_role"][self.role]["journal"]
        historical = selected.get(expected["name"])
        require(
            historical is not None
            and historical["uid"] == expected["uid"]
            and historical["resourceVersion"] == expected["resource_version"]
            and digest(historical["data"]) == expected["data_sha256"]
            and not historical["deletionTimestamp"]
            and not historical["ownerReferences"],
            f"{self.role}: accepted capacity journal UID/RV/data changed",
        )
        if self.initial_journals is None:
            self.initial_journals = copy.deepcopy(selected)
        require(selected == self.initial_journals,
                f"{self.role}: an existing capacity/legacy journal changed")
        if allow_own:
            self.owned_journal()

    def _monitoring_guard(self, pods):
        live = {}
        by_uid = {}
        for pod in mocks._items(pods, f"{self.role} Pod inventory"):
            metadata = pod.get("metadata") or {}
            if metadata.get("namespace") == "monitoring":
                live[object_uid(pod)] = pod_contract(pod)
                by_uid[object_uid(pod)] = pod
        for pod_uid, pin in self.inputs["monitoring_pins"][self.role].items():
            require(live.get(pod_uid) == pin,
                    f"{self.role}: source monitoring Pod UID/spec/placement changed")
        recorded = self.inputs["receipt"]["per_role"][self.role]["diagnostics"]["kubernetes"]["pods"]
        for pod in recorded["items"]:
            if (pod.get("metadata") or {}).get("namespace") != "monitoring" or not stalled.base.pod_ready(pod):
                continue
            current = by_uid.get(object_uid(pod))
            require(current is not None and stalled.base.pod_ready(current)
                    and pod_contract(stalled.safe_diagnostics(current)) == pod_contract(pod),
                    f"{self.role}: monitoring restored during capacity creation changed")

    def observe(self):
        self.unchanged_inputs()
        observed, identities, networks = self.observer.observe()
        snapshot = observed["kubernetes"]
        snapshot["controllers"] = self.kube(
            "get", "deployments,replicasets,daemonsets,statefulsets", "-A", "-o", "json",
        )
        snapshot["kwok_leases"] = self.kube(
            "get", "leases", "-n", "kube-node-lease", "-o", "json",
        )
        self.journals(snapshot["configmaps"], allow_own=bool(self.journal_uid))
        self._monitoring_guard(snapshot["pods"])
        for pod in snapshot["pods"]["items"]:
            metadata = pod.get("metadata") or {}
            name = metadata.get("name", "")
            if (maintenance.PROBE_LABEL_KEY not in (metadata.get("labels") or {})
                    and not name.startswith("secondary-cap-probe-")):
                continue
            record = self.role_summary["probe_receipts"].get(name)
            require(record is not None and not self.probes_cleaned,
                    f"{self.role}: an unexpected or already cleaned probe appeared")
            self.owned_probe(pod, {
                "name": name, "node_name": record["node_name"], "uid": record.get("uid", ""),
            })
        kwok = maintenance._kwok_map(snapshot["nodes"])
        require(
            len(kwok) == 100 and all(workers.node_is_ready(node) for node in kwok.values()),
            f"{self.role}: all 100 pinned KWOK Nodes must remain Ready",
        )
        receipt = self.inputs["receipt"]["per_role"][self.role]
        require(set(identities) == set(receipt["new_identities"])
                and set(networks) == set(receipt["new_networks"]),
                f"{self.role}: live secondary identity set differs from the accepted capacity receipt")
        for name, expected in receipt["new_identities"].items():
            actual = identities[name]
            require(all(actual.get(key) == expected.get(key) for key in (
                "instance_id", "node_name", "node_uid", "vm_id", "provider_id", "boot_id",
            )), f"{self.role}/{name}: accepted VM/Node/boot identity changed")
            current = networks[name]
            initial = receipt["new_networks"][name]
            require(
                all(current.get(key) == initial.get(key) for key in (
                    "uid", "node_uid", "network_container_id",
                ))
                and current["version"] >= initial["version"]
                and (current["version"] > initial["version"]
                     or current["ip_addresses"] == initial["ip_addresses"]),
                f"{self.role}/{name}: accepted initial NNC identity/version regressed",
            )
        self.role_summary["diagnostics"] = stalled.safe_diagnostics(observed)
        self.role_summary["diagnostics_artifact"] = self.export_evidence(
            "observation", self.role_summary["diagnostics"],
        )
        self.save()
        return snapshot, identities, networks

    def journal_data(self):
        return {
            "owner": OWNER, "token": self.token, "role": self.role,
            "capacity_source_build_id": str(self.summary["capacity_source_build_id"]),
            "capacity_tree_sha256": self.inputs["tree_sha256"],
            "source_tree_sha256": self.inputs["source"]["tree_sha256"],
            "record": canonical({
                "status": self.role_summary["status"],
                "probe_receipts": self.role_summary["probe_receipts"],
                "ip_growth": self.role_summary["ip_growth"],
                "capacity_qualified": self.role_summary["capacity_qualified"],
            }),
        }

    def owned_journal(self):
        row = self.kube("get", "configmap", self.journal_name, "-n", "kube-system", "-o", "json")
        metadata = row.get("metadata") or {}
        require(
            object_uid(row) == self.journal_uid
            and metadata.get("resourceVersion") == self.journal_rv
            and not metadata.get("deletionTimestamp")
            and not metadata.get("ownerReferences")
            and row.get("data") == self.journal_pin,
            f"{self.role}: qualification journal UID/RV/data/lifecycle changed",
        )
        return row

    def raw_write(self, command, *, cleanup=False):
        require(self.args.execute and command[0] == "kubectl",
                "Qualification cannot write Azure or mutate in plan mode")
        allowed_journal = command[:5] in (
            ["kubectl", "create", "configmap", self.journal_name, "-n"],
            ["kubectl", "patch", "configmap", self.journal_name, "-n"],
        )
        allowed_probe = (
            command[:3] == ["kubectl", "-n", mocks.DEFAULT_NAMESPACE]
            and "run" in command and self.journal_uid
        )
        require(allowed_journal or allowed_probe,
                "Only qualification journal and owned probe creation writes are allowed")
        self.summary["mutation_started"] = True
        self.save()
        return super().run(
            command, self.args.request_timeout_seconds,
            cleanup=cleanup,
        )

    def acquire(self):
        record = self.role_summary["journal"]
        require(not record["attempted"] and not self.journal_uid,
                f"{self.role}: qualification journal acquisition cannot repeat")
        record.update(attempted=True, accepted=None, ambiguous=True, requested_at=utc_now())
        self.save()
        data = self.journal_data()
        output = self.raw_write([
            "kubectl", "create", "configmap", self.journal_name, "-n", "kube-system",
            *(f"--from-literal={key}={value}" for key, value in data.items()), "-o", "json",
        ])
        row = workers.parse_json(output, f"{self.role} qualification journal creation")
        self.journal_uid = object_uid(row)
        self.journal_rv = str((row.get("metadata") or {}).get("resourceVersion") or "")
        self.journal_pin = data
        require(self.journal_rv and row.get("data") == data,
                f"{self.role}: qualification journal creation is ambiguous")
        self.owned_journal()
        record.update(
            uid=self.journal_uid, resource_version=self.journal_rv,
            data_sha256=digest(data), accepted=True, ambiguous=False, accepted_at=utc_now(),
        )
        self.persist()

    def persist(self, *, cleanup=False):
        self.unchanged_inputs()
        self.owned_journal()
        desired = self.journal_data()
        if desired == self.journal_pin:
            self.role_summary["journal"].update(
                resource_version=self.journal_rv, data_sha256=digest(desired),
                noop_update_skipped=True,
            )
            self.save()
            return
        output = self.raw_write([
            "kubectl", "patch", "configmap", self.journal_name, "-n", "kube-system",
            "--type=json", "-p", json.dumps([
                {"op": "test", "path": "/metadata/uid", "value": self.journal_uid},
                {"op": "test", "path": "/metadata/resourceVersion", "value": self.journal_rv},
                {"op": "test", "path": "/data/token", "value": self.token},
                {"op": "test", "path": "/data", "value": self.journal_pin},
                {"op": "add", "path": "/data", "value": desired},
            ]), "-o", "json",
        ], cleanup=cleanup)
        row = workers.parse_json(output, f"{self.role} qualification journal CAS")
        metadata = row.get("metadata") or {}
        new_rv = str(metadata.get("resourceVersion") or "")
        require(
            object_uid(row) == self.journal_uid and row.get("data") == desired
            and new_rv and new_rv != self.journal_rv,
            f"{self.role}: changed-data qualification journal CAS is ambiguous",
        )
        self.journal_pin = copy.deepcopy(desired)
        self.journal_rv = new_rv
        self.role_summary["journal"].update(
            resource_version=new_rv, data_sha256=digest(desired),
            noop_update_skipped=False,
        )
        self.owned_journal()
        self.save()

    def _probe_name(self, node_name, index):
        component = re.sub(r"[^a-z0-9]+", "-", node_name.lower()).strip("-")[-20:]
        return f"secondary-cap-probe-{component}-{self.token[:8]}-{index:02d}"

    def owned_probe(self, pod, intent):
        metadata = pod.get("metadata") or {}
        spec = pod.get("spec") or {}
        containers = spec.get("containers") or []
        require(
            metadata.get("namespace") == mocks.DEFAULT_NAMESPACE
            and metadata.get("name") == intent["name"]
            and (metadata.get("labels") or {}).get(maintenance.PROBE_LABEL_KEY) == self.token
            and spec.get("nodeName") == intent["node_name"]
            and (not intent.get("uid") or object_uid(pod) == intent["uid"])
            and not spec.get("hostNetwork") and spec.get("restartPolicy") == "Never"
            and not metadata.get("ownerReferences")
            and not spec.get("initContainers") and not spec.get("ephemeralContainers")
            and len(containers) == 1
            and containers[0].get("image") == maintenance.DEFAULT_PROBE_IMAGE
            and containers[0].get("args") == maintenance.PROBE_COMMAND
            and mocks._resource_requests(pod) == (5, 16 * 1024**2),
            f"{self.role}/{intent['name']}: probe UID/token/node/spec ownership changed",
        )

    def create_probe(self, node_name, index):
        name = self._probe_name(node_name, index)
        require(name not in self.role_summary["probe_receipts"],
                f"{self.role}/{name}: probe creation cannot repeat")
        intent = {
            "name": name, "node_name": node_name, "token": self.token, "uid": "",
        }
        record = {
            "node_name": node_name, "node_uid": self.inputs["receipt"]["per_role"][self.role][
                "new_identities"][node_name]["node_uid"],
            "token": self.token, "create_attempted": True,
            "create_submission_started": False, "create_accepted": None,
            "create_ambiguous": True, "delete_attempted": False,
            "requested_at": utc_now(),
        }
        self.role_summary["probe_cleanup_pending"].append(intent)
        self.role_summary["probe_receipts"][name] = record
        self.persist()
        overrides = {
            "apiVersion": "v1",
            "spec": {
                "nodeName": node_name, "hostNetwork": False, "restartPolicy": "Never",
                "containers": [{
                    "name": name, "image": maintenance.DEFAULT_PROBE_IMAGE,
                    "args": list(maintenance.PROBE_COMMAND),
                    "resources": {"requests": {
                        "cpu": maintenance.PROBE_CPU_REQUEST,
                        "memory": maintenance.PROBE_MEMORY_REQUEST,
                    }},
                }],
            },
        }
        record["create_submission_started"] = True
        record["submission_started_at"] = utc_now()
        self.persist()
        output = self.raw_write([
            "kubectl", "-n", mocks.DEFAULT_NAMESPACE, "run", name,
            f"--image={maintenance.DEFAULT_PROBE_IMAGE}", "--restart=Never",
            f"--labels={maintenance.PROBE_LABEL_KEY}={self.token}",
            "-o", "json", f"--overrides={json.dumps(overrides)}",
        ])
        pod = workers.parse_json(output, f"{self.role} probe creation")
        self.owned_probe(pod, intent)
        intent["uid"] = object_uid(pod)
        record.update(
            uid=intent["uid"], create_accepted=True, create_ambiguous=False,
            accepted_at=utc_now(),
        )
        self.persist()

    def _current_probes(self):
        pods = self.kube("get", "pods", "-n", mocks.DEFAULT_NAMESPACE, "-o", "json")
        return {
            pod["metadata"]["name"]: pod
            for pod in mocks._items(pods, f"{self.role} probe Pod inventory")
        }

    def prove_growth(self):
        for node_name, proof in self.role_summary["ip_growth"].items():
            for index in range(proof["probe_count"]):
                self.create_probe(node_name, index)
        deadline = min(self.work_deadline, time.monotonic() + PROBE_WAIT_SECONDS)
        while True:
            snapshot, _, networks = self.observe()
            pods = {
                pod["metadata"]["name"]: pod
                for pod in mocks._items(snapshot["pods"], f"{self.role} Pod inventory")
            }
            complete = True
            all_ips = []
            for node_name, proof in self.role_summary["ip_growth"].items():
                intents = [
                    row for row in self.role_summary["probe_cleanup_pending"]
                    if row["node_name"] == node_name
                ]
                selected = [pods.get(intent["name"]) for intent in intents]
                require(len(intents) == proof["probe_count"],
                        f"{self.role}/{node_name}: probe demand count changed")
                for pod, intent in zip(selected, intents):
                    if pod is not None:
                        self.owned_probe(pod, intent)
                if not all(pod is not None and stalled.base.pod_ready(pod) for pod in selected):
                    complete = False
                    continue
                addresses = [(pod.get("status") or {}).get("podIP") for pod in selected]
                require(all(addresses) and len(set(addresses)) == len(addresses),
                        f"{self.role}/{node_name}: Ready probe IPs are absent or duplicated")
                all_ips.extend(addresses)
                before = proof["before"]
                initial = proof["initial"]
                after = networks[node_name]
                grown = (
                    after["version"] > before["version"]
                    and after["assigned_ip_count"] > before["assigned_ip_count"]
                    and set(before["ip_addresses"]) < set(after["ip_addresses"])
                    and set(initial["ip_addresses"]) < set(after["ip_addresses"])
                    and set(addresses) <= set(after["ip_addresses"])
                    and bool(set(addresses) - set(initial["ip_addresses"]))
                )
                if not grown:
                    complete = False
                    continue
                for pod in selected:
                    response = self.read([
                        "kubectl", "get", "--raw",
                        f"/api/v1/namespaces/{mocks.DEFAULT_NAMESPACE}/pods/"
                        f"{pod['metadata']['name']}:8080/proxy/hostname",
                    ], self.args.request_timeout_seconds)
                    require(response.strip() == pod["metadata"]["name"],
                            f"{self.role}: owned Ready/IP HTTP probe did not answer")
                proof.update(
                    after=copy.deepcopy(after), ready_ips=addresses,
                    probe_uids=[object_uid(pod) for pod in selected],
                    http_proven=True,
                )
            require(len(set(all_ips)) == len(all_ips),
                    f"{self.role}: probe IPs overlap across new workers")
            self.save()
            if complete:
                return
            require(time.monotonic() < deadline,
                    f"{self.role}: actual NNC version/IP growth exceeded the bounded wait")
            time.sleep(min(POLL_SECONDS, self.remaining_seconds(POLL_SECONDS)))

    def cleanup(self):
        if not self.role_summary["probe_cleanup_pending"]:
            return
        previous_mode = self.cleanup_mode
        self.cleanup_mode = True
        absence_rounds = {}
        try:
            while self.role_summary["probe_cleanup_pending"]:
                pods = self._current_probes()
                remaining = []
                for intent in self.role_summary["probe_cleanup_pending"]:
                    record = self.role_summary["probe_receipts"][intent["name"]]
                    pod = pods.get(intent["name"])
                    if pod is None:
                        absence_rounds[intent["name"]] = absence_rounds.get(intent["name"], 0) + 1
                        if (
                            record.get("delete_accepted") is True
                            or record.get("create_submission_started") is False
                            or absence_rounds[intent["name"]] >= 3
                        ):
                            record["absence_observed"] = True
                            record["absence_observation_count"] = absence_rounds[intent["name"]]
                            continue
                        remaining.append(intent)
                        continue
                    absence_rounds[intent["name"]] = 0
                    self.owned_probe(pod, intent)
                    observed_uid = object_uid(pod)
                    if intent.get("uid"):
                        require(intent["uid"] == observed_uid,
                                f"{self.role}/{intent['name']}: probe UID replacement cannot be adopted")
                    else:
                        intent["uid"] = observed_uid
                        record["uid_resolved_for_cleanup"] = observed_uid
                    if not record.get("delete_attempted"):
                        record.update(
                            delete_attempted=True, delete_accepted=None,
                            delete_ambiguous=True, delete_requested_at=utc_now(),
                            uid=intent["uid"],
                        )
                        self.persist(cleanup=True)
                        self.delete_pod(
                            self.cluster, namespace=mocks.DEFAULT_NAMESPACE,
                            name=intent["name"], uid=intent["uid"],
                            timeout_seconds=self.remaining_seconds(60, cleanup=True),
                            attempts=1, retry_seconds=0,
                        )
                        record.update(
                            delete_accepted=True, delete_ambiguous=False,
                            delete_returned_at=utc_now(),
                        )
                        self.persist(cleanup=True)
                    remaining.append(intent)
                self.role_summary["probe_cleanup_pending"] = remaining
                self.save()
                if remaining:
                    time.sleep(min(5, self.remaining_seconds(5, cleanup=True)))
            self.persist(cleanup=True)
            self.probes_cleaned = True
        finally:
            self.cleanup_mode = previous_mode

    def _metrics(self, snapshot):
        node_metrics = self.kube("get", "--raw", "/apis/metrics.k8s.io/v1beta1/nodes")
        pod_metrics = self.kube(
            "get", "--raw",
            f"/apis/metrics.k8s.io/v1beta1/namespaces/{mocks.DEFAULT_NAMESPACE}/pods",
        )
        metrics = {
            row["metadata"]["name"]: row
            for row in mocks._items(node_metrics, f"{self.role} Node metrics")
        }
        pod_usage = maintenance._pod_memory_usage_bytes(
            pod_metrics, mocks.DEFAULT_NAMESPACE,
        )
        nodes = maintenance._real_node_map(snapshot["nodes"])
        agents = maintenance._agent_map(snapshot["pods"])
        self.role_summary["metrics_evidence"] = {
            "captured_at": utc_now(),
            "node_metrics": stalled.safe_diagnostics(node_metrics),
            "pod_metrics": stalled.safe_diagnostics(pod_metrics),
            "node_metrics_sha256": digest(node_metrics),
            "pod_metrics_sha256": digest(pod_metrics),
            "synthetic": False,
        }
        self.save()
        return nodes, agents, metrics, pod_usage

    def system_headroom(self, snapshot, networks, *, include_probes):
        del networks
        nodes, agents, metrics, pod_usage = self._metrics(snapshot)
        receipt = self.inputs["receipt"]["per_role"][self.role]
        destinations = set(receipt["new_identities"])
        source_contracts = self.source["mock_contracts"]
        healthy_names = {
            name for name, pin in source_contracts.items() if not pin["deleting"]
        }
        replacement_names = {
            name for name, pin in source_contracts.items() if pin["deleting"]
        }
        require(
            len(healthy_names) == EXPECTED_HEALTHY_MOCKS[self.role]
            and len(replacement_names) == EXPECTED_REPLACEMENTS[self.role]
            and set(agents) == set(source_contracts)
            and all(object_uid(agents[name]) == source_contracts[name]["uid"]
                    and stalled.base.pod_ready(agents[name]) for name in healthy_names)
            and set(healthy_names) <= set(pod_usage),
            f"{self.role}: protected healthy/terminating mock inventory or metrics changed",
        )
        affected = [agents[name] for name in sorted(replacement_names)]
        request_pairs = {mocks._resource_requests(pod) for pod in affected}
        require(len(request_pairs) == 1, f"{self.role}: replacement resource requests differ")
        request_cpu, request_memory = next(iter(request_pairs))
        require(request_cpu > 0 and request_memory > 0,
                f"{self.role}: replacement CPU/memory requests must be explicit")
        self.healthy_rss_high_water = max(
            self.healthy_rss_high_water,
            max(pod_usage[name]["memory_bytes"] for name in healthy_names),
        )
        memory_per_pod = max(request_memory, self.healthy_rss_high_water)
        controller_rows = [
            row for row in mocks._items(snapshot["controllers"], f"{self.role} controllers")
            if row.get("kind") == "StatefulSet"
            and (row.get("metadata") or {}).get("namespace") == mocks.DEFAULT_NAMESPACE
            and (row.get("metadata") or {}).get("name") == "kwok-node"
        ]
        require(len(controller_rows) == 1,
                f"{self.role}: exact current mock StatefulSet is required for scheduler projection")
        template = copy.deepcopy(
            ((controller_rows[0].get("spec") or {}).get("template") or {}).get("spec") or {}
        )
        require(
            not template.get("nodeName")
            and template.get("schedulerName", "default-scheduler") == "default-scheduler",
            f"{self.role}: mock controller template bypasses the normal scheduler",
        )
        assessment = mocks.assess_recovery_capacity(
            nodes_payload=snapshot["nodes"], pods_payload=snapshot["pods"],
            affected=affected,
            saturated_nodes=sorted(set(nodes) - destinations),
            pod_template=template, config_settings=mocks.CapacityRepairConfig(),
        )
        require(
            assessment["sufficient"] and set(assessment["alternate_nodes"]) == destinations,
            f"{self.role}: all terminating mocks do not fit only the two new cniv5 workers",
        )
        capacities = {}
        for name in destinations:
            require(name in metrics, f"{self.role}/{name}: fresh Node metrics are missing")
            row = assessment["nodes"][name]
            metric = metrics[name]
            observed = timestamp(metric.get("timestamp"), f"{self.role}/{name} Node metrics")
            require(0 <= (datetime.now(timezone.utc) - observed).total_seconds() <= 180,
                    f"{self.role}/{name}: Node metrics are stale")
            used_memory = int(mocks._quantity((metric.get("usage") or {}).get("memory"), "memory"))
            used_cpu = int(mocks._quantity((metric.get("usage") or {}).get("cpu"), "cpu") * 1000)
            probes = self.role_summary["ip_growth"][name]["probe_count"] if include_probes else 0
            free_memory = min(
                row["available_memory_bytes"],
                int(row["allocatable_memory_bytes"] * HEADROOM_PERCENT / 100)
                - max(used_memory, row["requested_memory_bytes"])
                - MEMORY_SAFETY_RESERVE - probes * 16 * 1024**2,
            )
            free_cpu = min(
                row["available_cpu_millicores"],
                int(row["allocatable_cpu_millicores"] * HEADROOM_PERCENT / 100)
                - max(used_cpu, row["requested_cpu_millicores"])
                - 250 - probes * 5,
            )
            free_slots = row["available_pod_slots"] - probes
            seats = max(0, min(
                free_memory // memory_per_pod,
                free_cpu // request_cpu,
                free_slots,
            ))
            capacities[name] = {
                "safe_slots": int(seats), "free_memory_bytes": free_memory,
                "free_cpu_millicores": free_cpu, "free_pod_slots": free_slots,
                "metric_timestamp": metric["timestamp"],
            }
        slots = {name: row["safe_slots"] for name, row in capacities.items()}
        placements = {}
        for pod in affected:
            selected = max(slots, key=lambda name: (slots[name], name))
            require(slots[selected] > 0,
                    f"{self.role}: actual 85% high-water headroom cannot fit every replacement")
            placements[pod["metadata"]["name"]] = selected
            slots[selected] -= 1
        counts = Counter(placements.values())
        if not include_probes:
            for name, count in counts.items():
                proof = self.role_summary["ip_growth"][name]
                require(
                    proof.get("http_proven") is True
                    and proof["after"]["assigned_ip_count"]
                    - len({
                        pod.get("status", {}).get("podIP")
                        for pod in snapshot["pods"]["items"]
                        if pod.get("spec", {}).get("nodeName") == name
                        and not pod.get("spec", {}).get("hostNetwork")
                        and pod.get("status", {}).get("podIP")
                    }) >= count,
                    f"{self.role}/{name}: proven IP growth cannot seat projected replacements",
                )
        result = {
            "replacement_count": len(affected),
            "healthy_sample_count": len(healthy_names),
            "healthy_sample_uids": {name: object_uid(agents[name]) for name in healthy_names},
            "memory_per_replacement_bytes": memory_per_pod,
            "healthy_rss_high_water_bytes": self.healthy_rss_high_water,
            "request_cpu_millicores": request_cpu,
            "request_memory_bytes": request_memory,
            "threshold_percent": HEADROOM_PERCENT,
            "scheduler_template_sha256": digest(template),
            "scheduler_constraints": stalled.safe_diagnostics({
                key: template.get(key) for key in (
                    "nodeSelector", "affinity", "tolerations", "topologySpreadConstraints",
                    "schedulerName", "runtimeClassName", "priorityClassName",
                )
            }),
            "destinations": capacities, "placements": placements,
            "placement_counts": dict(counts),
            "actual_metrics": True,
            "not_a_scheduler_binding_or_resource_reservation": True,
        }
        self.role_summary["placement_headroom"] = result
        self.save()
        return result

    def prom_headroom(self, snapshot, networks, *, include_probes):
        del networks
        nodes, _, metrics, _ = self._metrics(snapshot)
        receipt = self.inputs["receipt"]["per_role"][self.role]
        require(len(receipt["new_identities"]) == 1, "mesh-89 must have one promv5 worker")
        name = next(iter(receipt["new_identities"]))
        node = nodes[name]
        metric = metrics.get(name)
        require(metric is not None, "mesh-89 promv5 Node metrics are missing")
        active = [
            pod for pod in snapshot["pods"]["items"]
            if pod.get("spec", {}).get("nodeName") == name
            and (pod.get("status") or {}).get("phase") not in ("Succeeded", "Failed")
        ]
        requested_cpu = sum(mocks._resource_requests(pod)[0] for pod in active)
        requested_memory = sum(mocks._resource_requests(pod)[1] for pod in active)
        allocatable = node.get("status", {}).get("allocatable") or {}
        alloc_cpu = int(mocks._quantity(allocatable.get("cpu"), "promv5 allocatable CPU") * 1000)
        alloc_pods = int(mocks._quantity(allocatable.get("pods"), "promv5 allocatable Pods"))
        probes = self.role_summary["ip_growth"][name]["probe_count"] if include_probes else 0
        recorded = self.inputs["receipt"]["per_role"][self.role]["diagnostics"]["kubernetes"]["pods"]
        recorded_operators = [
            pod for pod in recorded["items"]
            if (pod.get("metadata") or {}).get("namespace") == "monitoring"
            and "prometheus-operator" in str((pod.get("metadata") or {}).get("name", ""))
            and pod.get("spec", {}).get("nodeName") == name and stalled.base.pod_ready(pod)
        ]
        require(len(recorded_operators) == 1,
                "mesh-89 capacity receipt lacks the exact naturally restored operator")
        operator_candidates = [
            pod for pod in snapshot["pods"]["items"] if object_uid(pod) == object_uid(recorded_operators[0])
        ]
        require(len(operator_candidates) == 1 and stalled.base.pod_ready(operator_candidates[0]),
                "mesh-89 restored prometheus-operator UID/readiness changed")
        operator = operator_candidates[0]
        selector = (operator.get("spec") or {}).get("nodeSelector") or {}
        operator_cpu, operator_memory = mocks._resource_requests(operator)
        require(
            selector.get("prometheus") == "true"
            and resource_equal(selector.get("kubernetes.io/os", "linux"), "linux")
            and not (operator.get("spec") or {}).get("hostNetwork")
            and pvc_free(operator.get("spec") or {}),
            "mesh-89 operator must remain Linux/prometheus-selected, PVC-free, and non-host-network",
        )
        require(operator_cpu > 0 and operator_memory > 0,
                "mesh-89 operator CPU/memory requests must be explicit")
        used_memory = int(mocks._quantity((metric.get("usage") or {}).get("memory"), "promv5 memory"))
        used_cpu = int(mocks._quantity((metric.get("usage") or {}).get("cpu"), "promv5 CPU") * 1000)
        require(
            maintenance._headroom_ok(
                node, metric, threshold_percent=HEADROOM_PERCENT,
                effective_reserved_memory_bytes=(
                    PROM_MEMORY_RESERVE + MEMORY_SAFETY_RESERVE
                    + probes * 16 * 1024**2
                    + max(requested_memory - used_memory, 0)
                ),
                next_memory_bytes=0,
            )
            and max(requested_cpu, used_cpu) + 250 + probes * 5
            < int(alloc_cpu * HEADROOM_PERCENT / 100)
            and len(active) + 5 + probes <= alloc_pods,
            "mesh-89 promv5 lacks actual 16GiB Prometheus/operator CPU-memory-slot headroom",
        )
        result = {
            "node_name": name, "node_uid": receipt["new_identities"][name]["node_uid"],
            "threshold_percent": HEADROOM_PERCENT,
            "prometheus_memory_reserve_bytes": PROM_MEMORY_RESERVE,
            "safety_memory_bytes": MEMORY_SAFETY_RESERVE,
            "operator_uid": object_uid(operator),
            "operator_cpu_millicores": operator_cpu,
            "operator_memory_bytes": operator_memory,
            "operator_already_running_on_promv5": True,
            "active_pod_count": len(active), "allocatable_pods": alloc_pods,
            "actual_metrics": True,
            "not_a_scheduler_binding_or_resource_reservation": True,
        }
        self.role_summary["placement_headroom"] = result
        self.save()
        return result

    def headroom(self, snapshot, networks, *, include_probes):
        if self.role in SYSTEM_ROLES:
            return self.system_headroom(snapshot, networks, include_probes=include_probes)
        return self.prom_headroom(snapshot, networks, include_probes=include_probes)

    def _mock_statefulset(self, controllers):
        rows = [
            row for row in mocks._items(controllers, f"{self.role} controller inventory")
            if row.get("kind") == "StatefulSet"
            and (row.get("metadata") or {}).get("namespace") == mocks.DEFAULT_NAMESPACE
            and (row.get("metadata") or {}).get("name") == "kwok-node"
        ]
        require(len(rows) == 1, f"{self.role}: mock StatefulSet identity is ambiguous")
        row = rows[0]
        spec = row.get("spec") or {}
        template = (spec.get("template") or {}).get("spec") or {}
        future_hold = {
            "key": NATIVE_HOLD_KEY, "value": "<downstream-owned-token>",
            "effect": "NoSchedule",
        }
        require(
            spec.get("replicas") == 100
            and not template.get("nodeName")
            and template.get("schedulerName", "default-scheduler") == "default-scheduler"
            and not mocks._tolerates(future_hold, template.get("tolerations") or []),
            f"{self.role}: mock StatefulSet cannot safely use the later owned NoSchedule hold",
        )
        require(all(
            any(owner.get("kind") == "StatefulSet" and owner.get("controller") is True
                and owner.get("name") == "kwok-node" and owner.get("uid") == object_uid(row)
                for owner in pin["owners"])
            for pin in self.source["mock_contracts"].values()
        ), f"{self.role}: mock StatefulSet UID differs from the preserved Pod owners")
        return {
            "uid": object_uid(row),
            "generation": (row.get("metadata") or {}).get("generation"),
            "spec_sha256": digest(spec),
            "template_sha256": digest(template),
            "scheduler_name": template.get("schedulerName", "default-scheduler"),
            "future_hold_tolerated": False,
            "template": stalled.safe_diagnostics(template),
        }

    def _kwok_lease_evidence(self, snapshot):
        expected = self.source["kwok_contracts"]
        rows = mocks._items(snapshot["kwok_leases"], f"{self.role} KWOK lease inventory")
        selected = {
            row["metadata"]["name"]: row
            for row in rows if (row.get("metadata") or {}).get("name") in expected
        }
        require(
            set(selected) == set(expected)
            and len(selected) == sum(
                (row.get("metadata") or {}).get("name") in expected for row in rows
            ),
            f"{self.role}: all 100 KWOK Node leases must be present exactly once",
        )
        now = datetime.now(timezone.utc)
        result = {}
        for name, row in selected.items():
            metadata = row.get("metadata") or {}
            spec = row.get("spec") or {}
            owners = [
                owner for owner in metadata.get("ownerReferences") or []
                if owner.get("kind") == "Node" and owner.get("name") == name
                and owner.get("uid") == expected[name]["uid"]
            ]
            duration = spec.get("leaseDurationSeconds")
            renewed = timestamp(spec.get("renewTime"), f"{self.role}/{name} lease renewal")
            require(
                metadata.get("namespace") == "kube-node-lease"
                and len(owners) == len(metadata.get("ownerReferences") or []) == 1
                and (owners[0].get("controller") is None
                     or isinstance(owners[0]["controller"], bool))
                and not metadata.get("deletionTimestamp")
                and isinstance(spec.get("holderIdentity"), str)
                and bool(spec["holderIdentity"])
                and isinstance(duration, int) and not isinstance(duration, bool)
                and duration > 0
                and 0 <= (now - renewed).total_seconds() <= duration + 30,
                f"{self.role}/{name}: KWOK lease UID/owner/holder/freshness is unsafe",
            )
            result[name] = {
                "uid": object_uid(row),
                "owner": copy.deepcopy(owners[0]),
                "holder_identity": spec["holderIdentity"],
                "lease_duration_seconds": duration,
                "renew_time": renewed.isoformat(timespec="microseconds"),
                "lease_transitions": spec.get("leaseTransitions"),
            }
        return result

    def _target_host_evidence(self, snapshot):
        failed = self.source["failed"]
        nodes = maintenance._real_node_map(snapshot["nodes"])
        target = nodes.get(failed["node_name"])
        require(
            target is not None and object_uid(target) == failed["node_uid"]
            and not workers.node_is_ready(target),
            f"{self.role}: failed target Node identity/readiness changed",
        )
        target_pods = [
            pod for pod in snapshot["pods"]["items"]
            if (pod.get("spec") or {}).get("nodeName") == failed["node_name"]
        ]
        evidence = {}
        for pod in target_pods:
            metadata = pod.get("metadata") or {}
            owners = [
                owner for owner in metadata.get("ownerReferences") or []
                if owner.get("controller") is True
            ]
            require(
                len(owners) == 1 and pvc_free(pod.get("spec") or {})
                and not maintenance._readiness_condition_true(pod),
                f"{self.role}/{metadata.get('namespace')}/{metadata.get('name')}: "
                "target-host Pod ownership/PVC/readiness is unsafe",
            )
            evidence[object_uid(pod)] = {
                "name": metadata.get("name"), "namespace": metadata.get("namespace"),
                "uid": object_uid(pod), "owner": copy.deepcopy(owners[0]),
                "deleting": bool(metadata.get("deletionTimestamp")),
                "pvc_or_ephemeral_claim": False, "ready": False,
                "spec_sha256": digest(pod.get("spec") or {}),
            }
        require(evidence, f"{self.role}: failed target-host Pod inventory is unexpectedly empty")
        agents = maintenance._agent_map(snapshot["pods"])
        target_mock_uids = {
            name: pin["uid"] for name, pin in self.source["mock_contracts"].items()
            if pin["deleting"]
        }
        require(
            all(
                name in agents and object_uid(agents[name]) == pod_uid
                and (agents[name].get("spec") or {}).get("nodeName") == failed["node_name"]
                and (agents[name].get("metadata") or {}).get("deletionTimestamp")
                and pvc_free(agents[name].get("spec") or {})
                for name, pod_uid in target_mock_uids.items()
            ),
            f"{self.role}: terminating target mocks changed identity/ownership/PVC state",
        )
        return {
            "failed_target": copy.deepcopy(failed),
            "node_contract": node_contract(target),
            "target_pods": evidence,
            "terminating_mock_uids": target_mock_uids,
            "terminal_failure_live_validated": True,
            "controller_pods_force_deleted": False,
        }

    def _system_daemonset_evidence(self, snapshot):
        expected = set(map(tuple, self.source["daemonsets"]))
        require(
            {name for _, name, _ in expected} >= {"cilium", "azure-cns"},
            f"{self.role}: source applicable system DaemonSets are incomplete",
        )
        result = {}
        for name in self.inputs["receipt"]["per_role"][self.role]["new_identities"]:
            present = maintenance._healthy_system_daemonsets_on_node(snapshot["pods"], name)
            require(expected <= present,
                    f"{self.role}/{name}: actual system DaemonSet readiness regressed")
            result[name] = {
                "expected": sorted(expected), "ready": sorted(present),
                "all_expected_ready": True,
            }
        return result

    def capture_final_evidence(self, snapshot, networks):
        controllers = snapshot["controllers"]
        controller_contracts = controller_pins(controllers)
        statefulset = self._mock_statefulset(controllers)
        pdbs = pdb_evidence(snapshot["pdbs"])
        leases = self._kwok_lease_evidence(snapshot)
        target = self._target_host_evidence(snapshot)
        daemonsets = self._system_daemonset_evidence(snapshot)
        agents = maintenance._agent_map(snapshot["pods"])
        protected = {
            name: pin["uid"] for name, pin in self.source["mock_contracts"].items()
            if not pin["deleting"]
        }
        require(
            all(name in agents and object_uid(agents[name]) == pod_uid
                and stalled.base.pod_ready(agents[name]) for name, pod_uid in protected.items()),
            f"{self.role}: protected healthy mock UIDs/readiness changed",
        )
        nodes = maintenance._real_node_map(snapshot["nodes"])
        healthy_source_nodes = sorted({
            (agents[name].get("spec") or {}).get("nodeName") for name in protected
        })
        require(
            all(
                node_name in nodes and workers.node_is_ready(nodes[node_name])
                and not any(taint.get("key") == NATIVE_HOLD_KEY
                            for taint in (nodes[node_name].get("spec") or {}).get("taints") or [])
                for node_name in healthy_source_nodes
            ),
            f"{self.role}: protected healthy source workers changed or already have a fencing hold",
        )
        stable_contracts = {
            "controllers": controller_contracts,
            "mock_statefulset": {
                key: statefulset[key] for key in (
                    "uid", "generation", "spec_sha256", "template_sha256",
                    "scheduler_name", "future_hold_tolerated",
                )
            },
            "pdbs": {
                name: {
                    key: row[key] for key in ("uid", "generation", "spec_sha256")
                } for name, row in pdbs.items()
            },
            "kwok_leases": {
                name: {
                    key: row[key] for key in (
                        "uid", "owner", "holder_identity", "lease_duration_seconds",
                    )
                } for name, row in leases.items()
            },
            "target_pods": copy.deepcopy(target["target_pods"]),
            "protected_healthy_source_nodes": {
                name: {
                    "uid": object_uid(nodes[name]),
                    "boot_id": (nodes[name].get("status") or {}).get(
                        "nodeInfo", {}
                    ).get("bootID"),
                } for name in healthy_source_nodes
            },
        }
        if self.preflight_contracts is None:
            self.preflight_contracts = stable_contracts
        else:
            require(
                stable_contracts == self.preflight_contracts,
                f"{self.role}: controller/PDB/lease/target-Pod/source-Node contract changed "
                "during qualification",
            )
        evidence = {
            "captured_at": utc_now(),
            "controller_pins": controller_contracts,
            "mock_statefulset": statefulset,
            "pdbs": pdbs,
            "kwok_node_leases": leases,
            "target_host": target,
            "system_daemonsets": daemonsets,
            "protected_healthy_mock_uids": protected,
            "protected_healthy_source_nodes": {
                name: {
                    "uid": object_uid(nodes[name]),
                    "boot_id": (nodes[name].get("status") or {}).get(
                        "nodeInfo", {}
                    ).get("bootID"),
                    "node_contract": node_contract(nodes[name]),
                } for name in healthy_source_nodes
            },
            "future_native_fencing_hold": {
                "required": self.role in SYSTEM_ROLES,
                "applied_in_qualification": False,
                "key": NATIVE_HOLD_KEY,
                "effect": "NoSchedule",
                "value_must_be_downstream_owned_token": True,
                "target_nodes": healthy_source_nodes if self.role in SYSTEM_ROLES else [],
                "mock_template_can_bypass_hold": False,
            },
            "final_object_hashes": {
                "nodes": digest(snapshot["nodes"]), "pods": digest(snapshot["pods"]),
                "nnc": digest(snapshot["nnc"]), "controllers": digest(controllers),
                "pdbs": digest(snapshot["pdbs"]),
                "kwok_leases": digest(snapshot["kwok_leases"]),
            },
            "final_objects": stalled.safe_diagnostics({
                "nodes": snapshot["nodes"], "pods": snapshot["pods"],
                "nnc": snapshot["nnc"], "controllers": controllers,
                "pdbs": snapshot["pdbs"], "kwok_leases": snapshot["kwok_leases"],
            }),
            "final_networks": copy.deepcopy(networks),
            "capacity_guard_arm_diagnostics": copy.deepcopy({
                key: value for key, value in self.role_summary["diagnostics"].items()
                if key != "kubernetes"
            }),
            "capacity_guard_arm_diagnostics_sha256": digest({
                key: value for key, value in self.role_summary["diagnostics"].items()
                if key != "kubernetes"
            }),
            "production_pods_deleted": False,
            "nodes_or_pools_mutated": False,
        }
        evidence["final_objects_artifact"] = self.export_evidence(
            "final-objects" if self.probes_cleaned else "preflight-objects",
            evidence["final_objects"],
        )
        if not self.probes_cleaned:
            self.role_summary["preflight_objects_artifact"] = copy.deepcopy(
                evidence["final_objects_artifact"],
            )
        self.role_summary["final_evidence"] = evidence
        self.save()
        return evidence

    def plan(self):
        snapshot, _, networks = self.observe()
        receipt = self.inputs["receipt"]["per_role"][self.role]
        require(not self.kube(
            "get", "configmaps", "-n", "kube-system",
            "--field-selector", f"metadata.name={self.journal_name}", "-o", "json",
        )["items"], f"{self.role}: existing qualification journal blocks replay")
        require(not any(
            maintenance.PROBE_LABEL_KEY in ((pod.get("metadata") or {}).get("labels") or {})
            or str((pod.get("metadata") or {}).get("name", "")).startswith("secondary-cap-probe-")
            for pod in snapshot["pods"]["items"]
        ), f"{self.role}: existing probe Pod blocks replay/adoption")
        for name, initial in receipt["new_networks"].items():
            before = networks[name]
            occupied = {
                pod.get("status", {}).get("podIP")
                for pod in snapshot["pods"]["items"]
                if pod.get("spec", {}).get("nodeName") == name
                and not pod.get("spec", {}).get("hostNetwork")
                and pod.get("status", {}).get("podIP")
            }
            count = max(2, len(set(before["ip_addresses"]) - occupied) + 1)
            require(count <= MAX_PROBES_PER_NODE,
                    f"{self.role}/{name}: bounded probe demand exceeds {MAX_PROBES_PER_NODE}")
            self.role_summary["ip_growth"][name] = {
                "initial": copy.deepcopy(initial), "before": copy.deepcopy(before),
                "probe_count": count, "http_proven": False,
            }
        for _ in range(MAX_PROBES_PER_NODE):
            projection = self.headroom(snapshot, networks, include_probes=True)
            if self.role not in SYSTEM_ROLES:
                break
            changed = False
            for name, needed in projection["placement_counts"].items():
                proof = self.role_summary["ip_growth"][name]
                if proof["probe_count"] < needed:
                    require(needed <= MAX_PROBES_PER_NODE,
                            f"{self.role}/{name}: replacement-sized IP demand exceeds its bound")
                    proof["probe_count"] = needed
                    changed = True
            if not changed:
                break
        else:
            raise workers.ReconcileError(f"{self.role}: replacement-sized probe projection did not converge")
        self.capture_final_evidence(snapshot, networks)
        self.role_summary["final_evidence"]["phase"] = "pre-mutation-plan"
        self.role_summary.update(plan_valid=True, status="plan-valid")
        self.save()

    def execute(self, *, planned=False):
        if not planned:
            self.plan()
        if not self.args.execute:
            return
        snapshot, _, networks = self.observe()
        self.headroom(snapshot, networks, include_probes=True)
        self.capture_final_evidence(snapshot, networks)
        self.acquire()
        self.role_summary["status"] = "proving-real-network-growth"
        self.persist()
        self.prove_growth()
        self.cleanup()
        snapshot, _, networks = self.observe()
        self.headroom(snapshot, networks, include_probes=False)
        require(
            not self.role_summary["probe_cleanup_pending"]
            and all(proof.get("http_proven") is True
                    for proof in self.role_summary["ip_growth"].values()),
            f"{self.role}: qualification proof or UID-bound cleanup is incomplete",
        )
        final_evidence = self.capture_final_evidence(snapshot, networks)
        final_evidence["phase"] = "probe-cleaned-final"
        self.save()
        self.role_summary.update(
            status="capacity-qualified-workloads-not-started",
            capacity_qualified=True, actual_ip_growth_proven=True,
            actual_headroom_proven=True,
        )
        self.persist()


def build_native_fencing_bundle(summary, inputs):
    """Return the strict, read-only evidence contract consumed by native fencing."""

    roles = {}
    for role in ROLES:
        result = summary["per_role"][role]
        source = inputs["source"]["roles"][role]
        capacity_receipt = inputs["receipt"]["per_role"][role]
        require(
            result.get("capacity_qualified") is True
            and result.get("actual_ip_growth_proven") is True
            and result.get("actual_headroom_proven") is True
            and not result.get("probe_cleanup_pending")
            and isinstance(result.get("final_evidence"), dict),
            f"{role}: complete cleaned qualification evidence is required for native handoff",
        )
        roles[role] = {
            "role": role,
            "cluster": source["cluster"]["name"],
            "failed_target": copy.deepcopy(result["final_evidence"]["target_host"]["failed_target"]),
            "replacement_workers": copy.deepcopy(capacity_receipt["new_identities"]),
            "network_qualification": copy.deepcopy(result["ip_growth"]),
            "placement_headroom": copy.deepcopy(result["placement_headroom"]),
            "protected_healthy_mock_uids": copy.deepcopy(
                result["final_evidence"]["protected_healthy_mock_uids"]
            ),
            "terminating_target_mock_uids": copy.deepcopy(
                result["final_evidence"]["target_host"]["terminating_mock_uids"]
            ),
            "protected_healthy_source_nodes": copy.deepcopy(
                result["final_evidence"]["protected_healthy_source_nodes"]
            ),
            "controllers": copy.deepcopy(result["final_evidence"]["controller_pins"]),
            "mock_statefulset": copy.deepcopy(result["final_evidence"]["mock_statefulset"]),
            "pdbs": copy.deepcopy(result["final_evidence"]["pdbs"]),
            "kwok_node_leases": copy.deepcopy(
                result["final_evidence"]["kwok_node_leases"]
            ),
            "target_host_pods": copy.deepcopy(
                result["final_evidence"]["target_host"]["target_pods"]
            ),
            "system_daemonsets": copy.deepcopy(
                result["final_evidence"]["system_daemonsets"]
            ),
            "final_object_hashes": copy.deepcopy(
                result["final_evidence"]["final_object_hashes"]
            ),
            "final_objects_artifact": copy.deepcopy(
                result["final_evidence"]["final_objects_artifact"]
            ),
            "capacity_observation_artifact": copy.deepcopy(result["diagnostics_artifact"]),
            "capacity_guard_arm_diagnostics_sha256": result["final_evidence"][
                "capacity_guard_arm_diagnostics_sha256"
            ],
            "final_networks": copy.deepcopy(result["final_evidence"]["final_networks"]),
            "capacity_journal": copy.deepcopy(capacity_receipt["journal"]),
            "qualification_journal": copy.deepcopy(result["journal"]),
            "future_native_fencing_hold": copy.deepcopy(
                result["final_evidence"]["future_native_fencing_hold"]
            ),
            "production_pods_deleted": False,
            "nodes_or_pools_mutated": False,
        }
    return {
        "schema_version": NATIVE_BUNDLE_SCHEMA,
        "qualification_complete": True,
        "capacity_source_build_id": summary["capacity_source_build_id"],
        "accepted_capacity_build_id": ACCEPTED_CAPACITY_BUILD,
        "accepted_role_observed_read_only": ACCEPTED_ROLE,
        "capacity_input_sha256": inputs["tree_sha256"],
        "source_tree_sha256": inputs["source"]["tree_sha256"],
        "roles": roles,
        "workloads_ready": False,
        "completed_global_baseline": False,
        "retirement_authorized_by_execution": False,
    }


def load_native_fencing_bundle(path, *, expected_capacity_source_build_id):
    """Load and validate a completed qualification receipt for a later native helper."""

    receipt = read_json(path)
    bundle = receipt.get("native_fencing_bundle")
    require(
        receipt.get("schema_version") == 1
        and receipt.get("execute") is True
        and receipt.get("success") is True
        and receipt.get("capacity_qualified") is True
        and receipt.get("actual_ip_growth_proven") is True
        and receipt.get("actual_headroom_proven") is True
        and receipt.get("workloads_ready") is False
        and receipt.get("completed_global_baseline") is False
        and receipt.get("cleanup_errors") == []
        and receipt.get("capacity_source_build_id") == expected_capacity_source_build_id
        and isinstance(bundle, dict)
        and receipt.get("native_fencing_bundle_sha256") == digest(bundle)
        and bundle.get("schema_version") == NATIVE_BUNDLE_SCHEMA
        and bundle.get("qualification_complete") is True
        and bundle.get("capacity_source_build_id") == expected_capacity_source_build_id
        and bundle.get("workloads_ready") is False
        and bundle.get("completed_global_baseline") is False
        and bundle.get("retirement_authorized_by_execution") is False
        and set(bundle.get("roles") or {}) == set(ROLES),
        "Qualification receipt is not the exact completed native-fencing evidence bundle",
    )
    for role, row in bundle["roles"].items():
        expected_workers = 1 if role == "mesh-89" else 2
        expected_targets = 0 if role == "mesh-89" else EXPECTED_REPLACEMENTS[role]
        hold = row.get("future_native_fencing_hold") or {}
        require(
            row.get("role") == role
            and isinstance(row.get("failed_target"), dict)
            and len(row.get("replacement_workers") or {}) == expected_workers
            and isinstance(row.get("network_qualification"), dict)
            and isinstance(row.get("placement_headroom"), dict)
            and len(row.get("terminating_target_mock_uids") or {}) == expected_targets
            and len(row.get("kwok_node_leases") or {}) == 100
            and isinstance(row.get("controllers"), dict)
            and isinstance(row.get("pdbs"), dict)
            and isinstance(row.get("target_host_pods"), dict)
            and row.get("production_pods_deleted") is False
            and row.get("nodes_or_pools_mutated") is False
            and hold.get("required") is (role in SYSTEM_ROLES)
            and hold.get("applied_in_qualification") is False
            and hold.get("key") == NATIVE_HOLD_KEY
            and hold.get("effect") == "NoSchedule",
            f"{role}: native-fencing evidence contract is incomplete",
        )
        for key, suffix in (("final_objects_artifact", "final-objects"),
                            ("capacity_observation_artifact", "observation")):
            reference = row.get(key) or {}
            relative = f"{Path(path).stem}-evidence/{role}-{suffix}.json"
            target = Path(path).parent / relative
            require(reference.get("path") == relative and target.is_file()
                    and not target.is_symlink() and not target.parent.is_symlink()
                    and target.stat().st_size <= MAX_INPUT_FILE_BYTES
                    and digest(target.read_bytes()) == reference.get("sha256"),
                    f"{role}: native-fencing evidence file is missing or changed")
    return copy.deepcopy(bundle)


def validate_args(args):
    require(
        args.resource_group == args.confirm_resource_group == capacity.RESOURCE_GROUP
        and args.expected_subscription.lower() == capacity.SUBSCRIPTION
        and args.expected_region.lower() == capacity.REGION
        and args.expected_tfvars_sha.lower() == capacity.TFVARS_SHA,
        "Qualification scope/subscription/region/tfvars confirmation mismatch",
    )
    require(
        isinstance(args.source_build_id, int) and not isinstance(args.source_build_id, bool)
        and args.source_build_id > 0
        and isinstance(args.timeout_seconds, int) and 1200 <= args.timeout_seconds <= 7200
        and isinstance(args.request_timeout_seconds, int)
        and 10 <= args.request_timeout_seconds <= 120,
        "Positive final capacity source build and bounded timeout values are required",
    )
    source = Path(args.capacity_directory).resolve()
    kube = Path(args.kubeconfig_directory).resolve()
    output = Path(args.summary_file).resolve()
    require(
        source.is_dir() and kube.is_dir()
        and not Path(args.capacity_directory).is_symlink()
        and not Path(args.kubeconfig_directory).is_symlink()
        and len({source, kube, output}) == 3
        and source not in output.parents and kube not in output.parents
        and output not in source.parents and output not in kube.parents
        and not output.exists(),
        "Capacity input, private kubeconfigs, and new output must be separate",
    )
    require(not (output.parent / f"{output.stem}-evidence").exists(),
            "Qualification evidence directory must be new")
    files = {path.name for path in kube.iterdir() if path.is_file()}
    require(
        files == {f"{role}.config" for role in ROLES}
        and all(not path.is_symlink() for path in kube.iterdir()),
        "Kubeconfig directory must contain exactly the four private role.config files",
    )
    args.probe_image = maintenance.DEFAULT_PROBE_IMAGE


def execute_qualification(args, summary, runner=workers.run_command, delete_pod=None):
    validate_args(args)
    summary.update(
        schema_version=1, execute=args.execute, mutation_started=False,
        plan_valid=False, success=False, status="validating-final-capacity",
        capacity_source_build_id=args.source_build_id,
        capacity_qualified=False, actual_ip_growth_proven=False,
        actual_headroom_proven=False, workloads_ready=False,
        completed_global_baseline=False, bootstrap_complete=False,
        automatic_resume_or_adoption=False, started_at=utc_now(), finished_at=None,
        per_role={}, cleanup_errors=[], native_fencing_bundle=None,
        native_fencing_bundle_sha256=None,
    )
    operations = []
    try:
        inputs = load_inputs(args)
        summary.update(
            capacity_input_hashes=inputs["hashes"],
            capacity_input_sha256=inputs["tree_sha256"],
            source_tree_hashes=inputs["source"]["hashes"],
            source_tree_sha256=inputs["source"]["tree_sha256"],
            source_diagnostic_build_id=capacity.DIAGNOSTIC_BUILD,
            accepted_capacity_build_id=ACCEPTED_CAPACITY_BUILD,
            accepted_checkpoint_hashes=inputs["accepted_checkpoint_hashes"],
            accepted_role_observed_read_only=ACCEPTED_ROLE,
            roles=list(ROLES),
        )
        for role in ROLES:
            summary["per_role"][role] = {
                "role": role, "cluster": inputs["source"]["roles"][role]["cluster"]["name"],
                "status": "validating", "plan_valid": False,
                "capacity_qualified": False, "actual_ip_growth_proven": False,
                "actual_headroom_proven": False, "workloads_ready": False,
                "completed_global_baseline": False,
                "ip_growth": {}, "placement_headroom": {},
                "probe_receipts": {}, "probe_cleanup_pending": [],
                "journal": {
                    "name": f"{JOURNAL_PREFIX}-{role}", "namespace": "kube-system",
                    "retained": True, "attempted": False,
                    "accepted": None, "ambiguous": False,
                },
            }
        operations = [
            RoleQualification(
                args, inputs, role, summary, runner,
                delete_pod or mocks.delete_pod_with_uid_precondition,
            )
            for role in ROLES
        ]
        for operation in operations:
            operation.plan()
        summary.update(plan_valid=True, status="plan-valid", success=not args.execute)
        write_summary(args.summary_file, summary)
        if args.execute:
            for operation in operations:
                operation.execute(planned=True)
            for operation in operations:
                snapshot, _, networks = operation.observe()
                operation.headroom(snapshot, networks, include_probes=False)
                operation.capture_final_evidence(snapshot, networks)["phase"] = "probe-cleaned-final"
            require(all(row["capacity_qualified"] for row in summary["per_role"].values()),
                    "All four role qualification receipts are required")
            bundle = build_native_fencing_bundle(summary, inputs)
            summary.update(
                success=True, status="secondary-capacity-qualified-workloads-not-started",
                capacity_qualified=True, actual_ip_growth_proven=True,
                actual_headroom_proven=True,
                native_fencing_bundle=bundle,
                native_fencing_bundle_sha256=digest(bundle),
            )
    except EXPECTED_ERRORS as error:
        summary.update(
            success=False, status="failed-closed", error=str(error),
            capacity_qualified=False, actual_ip_growth_proven=False,
            actual_headroom_proven=False, workloads_ready=False,
            completed_global_baseline=False, automatic_resume_or_adoption=False,
        )
        for operation in operations:
            if operation.role_summary["probe_cleanup_pending"]:
                try:
                    operation.cleanup()
                except EXPECTED_ERRORS as cleanup_error:
                    summary["cleanup_errors"].append({
                        "role": operation.role, "error": str(cleanup_error),
                    })
        raise
    finally:
        summary["finished_at"] = utc_now()
        summary["workloads_ready"] = False
        summary["completed_global_baseline"] = False
        summary["bootstrap_complete"] = False
        write_summary(args.summary_file, summary)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "capacity-directory", "resource-group", "confirm-resource-group",
        "expected-subscription", "expected-region", "expected-tfvars-sha",
        "kubeconfig-directory", "summary-file",
    ):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--source-build-id", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=3600)
    parser.add_argument("--request-timeout-seconds", type=int, default=60)
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    def interrupted(signum, _frame):
        raise workers.ReconcileError(
            f"Interrupted ({signum}); retain all journals and clean only UID-owned probes"
        )

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, interrupted)
    args, summary = parse_args(argv), {}
    try:
        execute_qualification(args, summary)
    except EXPECTED_ERRORS as error:
        print(f"Secondary capacity qualification failed closed: {error}", file=sys.stderr)
        return 1
    print(
        f"{summary['status']}; completed_global_baseline=false; workloads_ready=false",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
