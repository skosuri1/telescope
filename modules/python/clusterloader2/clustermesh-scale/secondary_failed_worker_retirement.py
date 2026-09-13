#!/usr/bin/env python3
"""Natively retire only the three qualified failed default workers."""

# pylint: disable=protected-access,too-many-lines,too-many-branches,too-many-statements,too-many-boolean-expressions

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
import qualified_failed_worker_retirement as prior
import secondary_capacity_qualification as qualification
import stalled_retained_worker_recovery as stalled


ROLES = ("mesh-51", "mesh-66", "mesh-79")
QUALIFICATION_BUILD = 80046
QUALIFICATION_SHA = "2751dcc9d6c5757ec258eb51629bb608091f66e80f3824f62489a85ba35c2e48"
CAPACITY_SHA = "8f91863e24e33a3c13b777a2cae4bf9d48371bfe449760bc2e45313654769e89"
OWNER = "secondary-failed-worker-retirement"
JOURNAL_PREFIX = "secondary-failed-worker-retirement"
MAX_INPUT_BYTES = 128 * 1024 * 1024
FINAL_RESERVE_SECONDS = 300
POLL_SECONDS = 10
PROVIDER_SECONDS = 180
READ_SECONDS = 60
ROLE_SETTINGS = {
    "mesh-51": {
        "target": "aks-default-37313277-vmss000000",
        "target_uid": "b46adff9-4d4f-45e5-87c5-b1241df1c372",
        "target_vm": "384089fe-4d4f-4b55-932f-e788add54285",
        "target_instance": "0", "retained_instances": {"3"},
        "initial_count": 2, "final_count": 1, "replacement_count": 61,
        "qualification_journal_uid": "4d27d414-d227-429c-a9f2-a8ab22c1d9f0",
        "qualification_journal_rv": "14729760",
    },
    "mesh-66": {
        "target": "aks-default-42633075-vmss000001",
        "target_uid": "5cca193f-ea99-4c6c-8f32-63b0aefa3e6b",
        "target_vm": "e9306089-e236-4157-be7f-64a50609abc8",
        "target_instance": "1", "retained_instances": {"0"},
        "initial_count": 2, "final_count": 1, "replacement_count": 48,
        "qualification_journal_uid": "93b12c75-9850-492c-90b4-056a9f771143",
        "qualification_journal_rv": "14750042",
    },
    "mesh-79": {
        "target": "aks-default-23134330-vmss000001",
        "target_uid": "dfc1de6c-ea9d-4e3c-a2b8-da8b68d19a2f",
        "target_vm": "66fb03ec-6cb4-483d-bf9f-b411afd799f1",
        "target_instance": "1", "retained_instances": {"2", "3"},
        "initial_count": 3, "final_count": 2, "replacement_count": 40,
        "qualification_journal_uid": "67792a94-bf4c-44c3-8e51-7f7818a1ca43",
        "qualification_journal_rv": "14851802",
    },
}
PROGRESS_STATES = {
    "InProgress", "Running", "Accepted", "Creating", "Updating", "DeletingMachines",
}
DELETE_TYPES = {"DeleteMachines", "DeleteAgentPoolMachines", "AgentPoolDeleteMachines"}
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


def timestamp(value, description):
    require(isinstance(value, str) and value, f"{description}: timestamp is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise workers.ReconcileError(f"{description}: timestamp is malformed") from error
    require(parsed.tzinfo is not None, f"{description}: timestamp lacks timezone")
    return parsed.astimezone(timezone.utc)


def read_json(path, *, maximum_bytes=MAX_INPUT_BYTES):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"{path}: duplicate JSON key {key}")
            result[key] = value
        return result

    content = Path(path).read_bytes()
    require(0 < len(content) <= maximum_bytes, f"{path}: JSON size is outside the bound")
    return json.loads(content, object_pairs_hook=unique)


def hash_tree(directory):
    root = Path(directory).resolve()
    require(root.is_dir() and not Path(directory).is_symlink(),
            "Qualification directory is missing or symlinked")
    result = {}
    for path in sorted(root.rglob("*")):
        require(not path.is_symlink(), "Qualification input contains a symlink")
        if path.is_file():
            require(path.stat().st_size <= MAX_INPUT_BYTES,
                    "Qualification input file exceeds 128MiB")
            result[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
        else:
            require(path.is_dir(), "Qualification input contains a nonregular entry")
    require(result, "Qualification input tree is empty")
    return result


def object_uid(row):
    value = str((row.get("metadata") or {}).get("uid") or "")
    require(maintenance.UUID_RE.fullmatch(value), "Kubernetes UID is missing or malformed")
    return value


def resource_equal(left, right):
    return (
        isinstance(left, str) and isinstance(right, str)
        and left.rstrip("/").lower() == right.rstrip("/").lower()
    )


def pvc_free(spec):
    return not any(
        "persistentVolumeClaim" in volume or "ephemeral" in volume
        for volume in spec.get("volumes") or []
    )


def pool_model(pool):
    result = qualification.capacity.pool_contract(pool)
    result.pop("count", None)
    return result


def vmss_model(vmss):
    result = qualification.capacity.vmss_contract(vmss)
    sku = result.get("sku") or {}
    sku.pop("capacity", None)
    return result


def action_receipt():
    return {
        "attempted": False, "submission_started": False, "accepted": None,
        "ambiguous": False, "automatic_retry_allowed": False,
    }


def hold_receipt():
    return {
        "attempted": False, "accepted": None, "ambiguous": False,
        "applied": False,
    }


def load_reference(root, reference, expected):
    require(
        isinstance(reference, dict) and set(reference) == {"path", "sha256"}
        and reference["path"] == expected
        and maintenance.SHA256_RE.fullmatch(str(reference["sha256"] or "")),
        f"Evidence reference {expected} is malformed",
    )
    path = root / reference["path"]
    require(path.is_file() and not path.is_symlink(), f"Evidence file {expected} is missing")
    require(hashlib.sha256(path.read_bytes()).hexdigest() == reference["sha256"],
            f"Evidence file {expected} changed")
    return read_json(path)


def reconstruct_qualification_journal(receipt, role):
    row = receipt["per_role"][role]
    tokens = {
        item.get("token") for item in (row.get("probe_receipts") or {}).values()
    }
    require(
        len(tokens) == 1 and re.fullmatch(r"[0-9a-f]{32}", str(next(iter(tokens)))),
        f"{role}: qualification probe token is missing or ambiguous",
    )
    return {
        "owner": qualification.OWNER,
        "token": next(iter(tokens)),
        "role": role,
        "capacity_source_build_id": str(receipt["capacity_source_build_id"]),
        "capacity_tree_sha256": receipt["capacity_input_sha256"],
        "source_tree_sha256": receipt["source_tree_sha256"],
        "record": qualification.canonical({
            "status": row["status"],
            "probe_receipts": row["probe_receipts"],
            "ip_growth": row["ip_growth"],
            "capacity_qualified": row["capacity_qualified"],
        }),
    }


def journal_rows(payload):
    selected = {}
    for row in mocks._items(payload, "ConfigMap inventory"):
        metadata = row.get("metadata") or {}
        data = row.get("data") or {}
        name = metadata.get("name")
        if not (
            "journal" in str(name).lower()
            or ("owner" in data and ("token" in data or "record" in data))
        ):
            continue
        require(name and name not in selected, "Journal names are ambiguous")
        selected[name] = {
            "uid": object_uid(row),
            "resourceVersion": metadata.get("resourceVersion"),
            "data": copy.deepcopy(data),
            "deletionTimestamp": metadata.get("deletionTimestamp"),
            "ownerReferences": copy.deepcopy(metadata.get("ownerReferences") or []),
        }
    return selected


def _validate_completed_role(receipt, role, final_objects):
    row = receipt["per_role"][role]
    settings = ROLE_SETTINGS[role]
    journal = row.get("journal") or {}
    require(
        row.get("status") == "capacity-qualified-workloads-not-started"
        and row.get("plan_valid") is True
        and row.get("capacity_qualified") is True
        and row.get("actual_ip_growth_proven") is True
        and row.get("actual_headroom_proven") is True
        and row.get("workloads_ready") is False
        and row.get("completed_global_baseline") is False
        and row.get("probe_cleanup_pending") == []
        and journal.get("uid") == settings["qualification_journal_uid"]
        and journal.get("resource_version") == settings["qualification_journal_rv"]
        and journal.get("accepted") is True and journal.get("ambiguous") is False,
        f"{role}: qualification is not the exact completed system-role result",
    )
    proofs = row.get("ip_growth")
    probes = row.get("probe_receipts")
    require(
        isinstance(proofs, dict) and len(proofs) == 2
        and all(proof.get("http_proven") is True for proof in proofs.values())
        and sum(proof.get("probe_count", 0) for proof in proofs.values())
        == settings["replacement_count"]
        and isinstance(probes, dict) and len(probes) == settings["replacement_count"]
        and all(
            item.get("create_accepted") is True
            and item.get("create_ambiguous") is False
            and item.get("delete_attempted") is True
            and item.get("delete_accepted") is True
            and item.get("delete_ambiguous") is False
            and item.get("absence_observed") is True
            and maintenance.UUID_RE.fullmatch(str(item.get("uid") or ""))
            for item in probes.values()
        ),
        f"{role}: HTTP growth or UID-bound cleanup proof is incomplete",
    )
    placement = row.get("placement_headroom") or {}
    final = row.get("final_evidence") or {}
    require(
        placement.get("actual_metrics") is True
        and placement.get("replacement_count") == settings["replacement_count"]
        and len(placement.get("placements") or {}) == settings["replacement_count"]
        and set((placement.get("placements") or {}).values()) == set(proofs)
        and final.get("phase") == "probe-cleaned-final"
        and final.get("production_pods_deleted") is False
        and final.get("nodes_or_pools_mutated") is False
        and (final.get("target_host") or {}).get("failed_target", {}).get("node_uid")
        == settings["target_uid"]
        and (final.get("target_host") or {}).get("controller_pods_force_deleted") is False
        and set(final_objects) == {
            "nodes", "pods", "nnc", "controllers", "pdbs", "kwok_leases",
        },
        f"{role}: placement/final fencing handoff evidence is incomplete",
    )
    journal_data = reconstruct_qualification_journal(receipt, role)
    require(
        digest(journal_data) == journal.get("data_sha256"),
        f"{role}: reconstructed qualification journal data does not match its receipt",
    )
    return journal_data


def load_inputs(args):
    root = Path(args.qualification_directory).resolve()
    hashes = hash_tree(root)
    require(
        hashes.get("qualification.json") == QUALIFICATION_SHA
        and hashes.get("capacity-input/recovery.json") == CAPACITY_SHA,
        "Only the exact build-80046 qualification and completed build-80039 capacity are supported",
    )
    receipt = read_json(root / "qualification.json")
    require(
        receipt.get("schema_version") == 1
        and receipt.get("execute") is True
        and receipt.get("success") is False
        and receipt.get("status") == "failed-closed"
        and receipt.get("error") == "mesh-89: actual NNC version/IP growth exceeded the bounded wait"
        and receipt.get("capacity_source_build_id") == 80039
        and receipt.get("capacity_qualified") is False
        and receipt.get("actual_ip_growth_proven") is False
        and receipt.get("actual_headroom_proven") is False
        and receipt.get("workloads_ready") is False
        and receipt.get("completed_global_baseline") is False
        and receipt.get("cleanup_errors") == []
        and set(receipt.get("per_role") or {}) == {"mesh-51", "mesh-66", "mesh-79", "mesh-89"},
        "Qualification source is not the exact partial build-80046 result",
    )
    failed_89 = receipt["per_role"]["mesh-89"]
    require(
        failed_89.get("capacity_qualified") is False
        and failed_89.get("actual_ip_growth_proven") is False
        and failed_89.get("probe_cleanup_pending") == [],
        "mesh-89 must remain explicitly unqualified and outside this retirement",
    )
    capacity_args = SimpleNamespace(capacity_directory=str(root / "capacity-input"))
    capacity_inputs = qualification.load_inputs(capacity_args)
    require(
        receipt.get("capacity_input_hashes") == capacity_inputs["hashes"]
        and receipt.get("capacity_input_sha256") == capacity_inputs["tree_sha256"]
        and receipt.get("source_tree_hashes") == capacity_inputs["source"]["hashes"]
        and receipt.get("source_tree_sha256") == capacity_inputs["source"]["tree_sha256"],
        "Partial qualification is not bound to the complete successful capacity/source input",
    )
    roles = {}
    for role in ROLES:
        row = receipt["per_role"][role]
        final_objects = load_reference(
            root, row["final_evidence"]["final_objects_artifact"],
            f"qualification-evidence/{role}-final-objects.json",
        )
        observation = load_reference(
            root, row["diagnostics_artifact"],
            f"qualification-evidence/{role}-observation.json",
        )
        preflight = load_reference(
            root, row["preflight_objects_artifact"],
            f"qualification-evidence/{role}-preflight-objects.json",
        )
        journal_data = _validate_completed_role(receipt, role, final_objects)
        source = capacity_inputs["source"]["roles"][role]
        raw_pods = qualification.capacity.read_json(
            root / "capacity-input" / "source-input" / role / "pods.json"
        )
        raw_agents = maintenance._agent_map(raw_pods)
        require(
            set(raw_agents) == set(source["mock_contracts"]) and len(raw_agents) == 100,
            f"{role}: raw build-80022 mock source is incomplete",
        )
        journal_pins = journal_rows(observation["kubernetes"]["configmaps"])
        qualification_name = row["journal"]["name"]
        qualification_pin = journal_pins.get(qualification_name)
        require(
            qualification_pin is not None
            and qualification_pin["uid"] == ROLE_SETTINGS[role]["qualification_journal_uid"],
            f"{role}: qualification observation lacks its exact original journal UID",
        )
        qualification_pin.update(
            resourceVersion=ROLE_SETTINGS[role]["qualification_journal_rv"],
            data=copy.deepcopy(journal_data),
        )
        roles[role] = {
            "receipt": row, "source": source,
            "capacity": capacity_inputs["receipt"]["per_role"][role],
            "final_objects": final_objects, "observation": observation,
            "preflight_objects": preflight, "qualification_journal_data": journal_data,
            "raw_agents": raw_agents,
            "raw_pods": {object_uid(pod): pod for pod in raw_pods["items"]},
            "monitoring_pins": capacity_inputs["monitoring_pins"][role],
            "journal_pins": journal_pins,
        }
    require(hash_tree(root) == hashes, "Qualification input tree changed while loading")
    return {
        "root": root, "hashes": hashes, "tree_sha256": digest(hashes),
        "receipt": receipt, "capacity_inputs": capacity_inputs, "roles": roles,
    }


def native_command(cluster, target):
    return [
        "az", "aks", "nodepool", "delete-machines",
        "--resource-group", qualification.capacity.RESOURCE_GROUP,
        "--cluster-name", cluster, "--name", "default",
        "--machine-names", target, "--no-wait", "--only-show-errors",
    ]


class RoleRecovery(maintenance.ClusterOperator):
    """One role's hold, single native delete, natural replacements, and cleanup."""

    def __init__(self, args, bundle, role, summary, runner):
        role_args = copy.copy(args)
        role_args.kubeconfig = str(Path(args.kubeconfig_directory) / f"{role}.config")
        role_args.context = bundle["roles"][role]["source"]["cluster"]["name"]
        role_args.source_directory = str(bundle["root"] / "capacity-input" / "source-input")
        deadline = time.monotonic() + args.timeout_seconds
        super().__init__(
            role_args, role_args.context, runner,
            deadline - FINAL_RESERVE_SECONDS, deadline,
        )
        self.args = role_args
        self.bundle = bundle
        self.role_bundle = bundle["roles"][role]
        self.role = role
        self.settings = ROLE_SETTINGS[role]
        self.summary = summary
        self.token = uuid.uuid4().hex
        self.journal_name = f"{JOURNAL_PREFIX}-{role}"
        self.journal_uid = ""
        self.journal_rv = ""
        self.journal_pin = None
        self.baseline = None
        self.hold_applied = set()
        self.native_submitted = False
        self.fenced = False
        self.operation_name = None
        self.replacement_uids = {}
        self.latest_networks = {}
        self.observation_count = 0
        self.native_request_sent = False
        self.healthy_rss_high_water = self.role_bundle["receipt"]["placement_headroom"][
            "healthy_rss_high_water_bytes"
        ]
        self.baseline_real_nodes = maintenance._real_node_map(
            self.role_bundle["final_objects"]["nodes"]
        )
        hold_nodes = set(
            self.role_bundle["receipt"]["final_evidence"][
                "future_native_fencing_hold"
            ]["target_nodes"]
        )
        self.protected_hold_pods = {
            object_uid(pod): qualification.pod_contract(stalled.safe_diagnostics(pod))
            for pod in self.role_bundle["final_objects"]["pods"]["items"]
            if (pod.get("spec") or {}).get("nodeName") in hold_nodes
            and stalled.base.pod_ready(pod)
        }
        require(self.protected_hold_pods,
                f"{role}: qualified healthy hold targets have no protected Ready Pods")

    @property
    def role_summary(self):
        return self.summary["per_role"][self.role]

    def save(self):
        mocks.write_json_atomic(self.args.summary_file, self.summary)

    def progress(self, action, **details):
        fields = " ".join(
            f"{key}={value}" for key, value in sorted(details.items())
        )
        print(
            f"{utc_now()}: role={self.role} action={action}"
            f"{(' ' + fields) if fields else ''}",
            flush=True,
        )

    def unchanged_inputs(self):
        require(
            hash_tree(self.args.qualification_directory) == self.bundle["hashes"],
            "Immutable build-80046 qualification tree changed",
        )

    def read(self, command, timeout_seconds=READ_SECONDS):
        allowed = command[0] == "az" and (
            command[1:3] in (
                ["account", "show"], ["group", "show"], ["aks", "show"],
                ["vmss", "list"], ["vmss", "show"], ["vmss", "list-instances"],
                ["vmss", "get-instance-view"],
            ) or command[1:4] in (
                ["aks", "nodepool", "list"], ["aks", "operation", "show-latest"],
            )
        )
        if command[0] == "kubectl":
            allowed = "get" in command and not any(word in command for word in (
                "create", "patch", "delete", "exec", "run", "drain", "taint", "apply",
            ))
        require(allowed, "Retirement read path rejected a mutation")
        for attempt in range(1, 4):
            try:
                return super().run(command, timeout_seconds, cleanup=self.cleanup_mode)
            except workers.ReconcileError as error:
                if attempt == 3 or qualification.capacity.TRANSIENT_READ_RE.search(str(error)) is None:
                    raise
                time.sleep(min(2, self.remaining_seconds(2)))
        raise workers.ReconcileError("Transient retirement read retry exhausted")

    def az(self, *command, timeout_seconds=READ_SECONDS):
        return workers.parse_json(
            self.read(
                ["az", *command, "--output", "json", "--only-show-errors"],
                timeout_seconds,
            ),
            f"{self.role} Azure read",
        )

    def kube(self, *command):
        return workers.parse_json(
            self.read([
                "kubectl", f"--request-timeout={self.args.request_timeout_seconds}s",
                *command,
            ], self.args.request_timeout_seconds),
            f"{self.role} Kubernetes read",
        )

    def capture(self):
        source = self.role_bundle["source"]
        readyz = self.read([
            "kubectl", f"--request-timeout={self.args.request_timeout_seconds}s", "get", "--raw=/readyz",
        ], self.args.request_timeout_seconds).strip()
        account = self.az("account", "show", "--query", "{id:id}")
        group = self.az("group", "show", "--name", qualification.capacity.RESOURCE_GROUP)
        cluster = self.az(
            "aks", "show", "--resource-group", qualification.capacity.RESOURCE_GROUP,
            "--name", source["cluster"]["name"],
        )
        node_group = self.az("group", "show", "--name", source["node_group"])
        pools = self.az(
            "aks", "nodepool", "list", "--resource-group",
            qualification.capacity.RESOURCE_GROUP, "--cluster-name", source["cluster"]["name"],
        )
        vmsses = self.az("vmss", "list", "--resource-group", source["node_group"],
                        "--query", qualification.capacity.VMSS_QUERY)
        vmss_models = {}
        instances = {}
        views = {}
        for vmss in vmsses:
            name = vmss["name"]
            vmss_models[name] = self.az(
                "vmss", "show", "--resource-group", source["node_group"], "--name", name,
                "--query", qualification.capacity.VMSS_MODEL_QUERY,
            )
            rows = self.az(
                "vmss", "list-instances", "--resource-group", source["node_group"],
                "--name", name, "--query", qualification.capacity.VM_QUERY,
            )
            instances[name] = rows
            for row in rows:
                instance = str(row["instanceId"])
                if (self.native_submitted and workers.vmss_pool_name(vmss) == "default"
                        and instance == self.settings["target_instance"]):
                    continue
                views[f"{name}/{instance}"] = self.az(
                    "vmss", "get-instance-view", "--resource-group", source["node_group"],
                    "--name", name, "--instance-id", instance,
                    "--query", qualification.capacity.VIEW_QUERY,
                )
        operations = {
            pool["name"]: self.az(
                "aks", "operation", "show-latest", "--resource-group",
                qualification.capacity.RESOURCE_GROUP, "--name", source["cluster"]["name"],
                "--nodepool-name", pool["name"],
            ) for pool in pools
        }
        customer_operation = self.az(
            "aks", "operation", "show-latest", "--resource-group",
            qualification.capacity.RESOURCE_GROUP, "--name", source["cluster"]["name"],
        )
        snapshot = {
            "nodes": self.kube("get", "nodes", "-o", "json"),
            "pods": self.kube("get", "pods", "-A", "-o", "json"),
            "nnc": self.kube(
                "get", "nodenetworkconfigs", "-n", "kube-system", "-o", "json",
            ),
            "controllers": self.kube(
                "get", "deployments,replicasets,daemonsets,statefulsets", "-A", "-o", "json",
            ),
            "pdbs": self.kube("get", "pdb", "-A", "-o", "json"),
            "kwok_leases": self.kube(
                "get", "leases", "-n", "kube-node-lease", "-o", "json",
            ),
            "configmaps": self.kube("get", "configmaps", "-n", "kube-system", "-o", "json"),
        }
        observed = {
            "account": account, "group": group, "cluster": cluster,
            "node_group": node_group, "pools": pools, "vmsses": vmsses,
            "instances": instances, "views": views, "operations": operations,
            "customer_operation": customer_operation, "snapshot": snapshot,
            "vmss_models": vmss_models,
            "readyz": readyz,
        }
        self.observation_count += 1
        self.role_summary["latest_observation_artifact"] = self.export_evidence(
            f"observation-{self.observation_count:06d}", observed,
        )
        self.save()
        return observed

    def export_evidence(self, phase, payload):
        output = Path(self.args.summary_file).resolve()
        directory = output.parent / f"{output.stem}-evidence"
        directory.mkdir(mode=0o700, exist_ok=True)
        path = directory / f"{self.role}-{phase}.json"
        require(not path.exists(), f"{self.role}: evidence output cannot be overwritten")
        mocks.write_json_atomic(str(path), stalled.safe_diagnostics(payload))
        return {
            "path": f"{directory.name}/{path.name}",
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }

    def journal_inventory(self, payload):
        selected = journal_rows(payload)
        own_rows = [
            row for row in mocks._items(payload, f"{self.role} ConfigMap inventory")
            if (row.get("metadata") or {}).get("name") == self.journal_name
        ]
        if own_rows:
            require(len(own_rows) == 1, f"{self.role}: retirement journal is duplicated")
            row = own_rows[0]
            metadata = row.get("metadata") or {}
            selected[self.journal_name] = {
                "uid": object_uid(row),
                "resourceVersion": metadata.get("resourceVersion"),
                "data": copy.deepcopy(row.get("data") or {}),
                "deletionTimestamp": metadata.get("deletionTimestamp"),
                "ownerReferences": copy.deepcopy(metadata.get("ownerReferences") or []),
            }
        return selected

    def validate_journals(self, payload, *, allow_own=False):
        current = self.journal_inventory(payload)
        own = current.pop(self.journal_name, None)
        if allow_own:
            require(own is not None, f"{self.role}: owned retirement journal disappeared")
        else:
            require(own is None, f"{self.role}: existing retirement journal blocks replay/adoption")
        qrow = self.role_bundle["receipt"]["journal"]
        qualified = current.get(qrow["name"])
        require(
            qualified is not None
            and qualified["uid"] == self.settings["qualification_journal_uid"]
            and qualified["resourceVersion"] == self.settings["qualification_journal_rv"]
            and qualified["data"] == self.role_bundle["qualification_journal_data"]
            and digest(qualified["data"]) == qrow["data_sha256"],
            f"{self.role}: completed qualification journal UID/RV/token/data changed",
        )
        require(all(
            row["uid"] and row["resourceVersion"]
            and not row["deletionTimestamp"] and not row["ownerReferences"]
            for row in current.values()
        ), f"{self.role}: an existing capacity/qualification journal is malformed")
        require(current == self.role_bundle["journal_pins"],
                f"{self.role}: a source-bound capacity/qualification/legacy journal changed")
        if allow_own:
            self.owned_journal()

    def hold_taint(self):
        return {
            "key": qualification.NATIVE_HOLD_KEY,
            "value": self.token, "effect": "NoSchedule",
        }

    def _pdb_guard(self, snapshot):
        current = qualification.pdb_evidence(snapshot["pdbs"])
        expected = self.role_bundle["receipt"]["final_evidence"]["pdbs"]
        require(
            {
                name: {key: row[key] for key in ("uid", "generation", "spec_sha256")}
                for name, row in current.items()
            } == {
                name: {key: row[key] for key in ("uid", "generation", "spec_sha256")}
                for name, row in expected.items()
            },
            f"{self.role}: PDB UID/spec/generation changed",
        )
        return current

    def _lease_guard(self, snapshot):
        expected_nodes = self.role_bundle["source"]["kwok_contracts"]
        recorded = self.role_bundle["receipt"]["final_evidence"]["kwok_node_leases"]
        rows = mocks._items(snapshot["kwok_leases"], f"{self.role} lease inventory")
        selected = {
            row["metadata"]["name"]: row for row in rows
            if (row.get("metadata") or {}).get("name") in expected_nodes
        }
        require(set(selected) == set(expected_nodes) == set(recorded)
                and len(selected) == sum(row.get("metadata", {}).get("name") in expected_nodes for row in rows),
                f"{self.role}: all 100 KWOK leases are required")
        now = datetime.now(timezone.utc)
        result = {}
        for name, row in selected.items():
            metadata = row.get("metadata") or {}
            spec = row.get("spec") or {}
            owners = [
                owner for owner in metadata.get("ownerReferences") or []
                if owner.get("kind") == "Node" and owner.get("name") == name
                and owner.get("uid") == expected_nodes[name]["uid"]
            ]
            renewed = timestamp(spec.get("renewTime"), f"{self.role}/{name} lease")
            duration = spec.get("leaseDurationSeconds")
            require(
                object_uid(row) == recorded[name]["uid"]
                and metadata.get("namespace") == "kube-node-lease"
                and not metadata.get("deletionTimestamp")
                and len(owners) == len(metadata.get("ownerReferences") or []) == 1
                and owners[0] == recorded[name]["owner"]
                and spec.get("holderIdentity") == recorded[name]["holder_identity"]
                and duration == recorded[name]["lease_duration_seconds"]
                and isinstance(duration, int)
                and not isinstance(duration, bool) and duration > 0
                and 0 <= (now - renewed).total_seconds() <= duration + 30,
                f"{self.role}/{name}: KWOK lease UID/owner/holder/freshness changed",
            )
            result[name] = {
                "uid": object_uid(row), "holder": spec["holderIdentity"],
                "renew_time": renewed.isoformat(timespec="microseconds"),
            }
        return result

    def _metrics(self):
        node_metrics = self.kube("get", "--raw", "/apis/metrics.k8s.io/v1beta1/nodes")
        pod_metrics = self.kube(
            "get", "--raw",
            f"/apis/metrics.k8s.io/v1beta1/namespaces/{mocks.DEFAULT_NAMESPACE}/pods",
        )
        metrics = {
            row["metadata"]["name"]: row
            for row in mocks._items(node_metrics, f"{self.role} Node metrics")
        }
        usage = maintenance._pod_memory_usage_bytes(pod_metrics, mocks.DEFAULT_NAMESPACE)
        return node_metrics, pod_metrics, metrics, usage

    def _provider_guard(self, observed, *, post=False):
        source = self.role_bundle["source"]
        require(
            observed.get("readyz") == "ok"
            and str(observed["account"].get("id", "")).lower()
            == qualification.capacity.SUBSCRIPTION
            and observed["group"].get("name") == qualification.capacity.RESOURCE_GROUP
            and str(observed["group"].get("location", "")).lower()
            == qualification.capacity.REGION
            and resource_equal(observed["cluster"].get("id"), source["cluster"]["id"])
            and observed["cluster"].get("provisioningState") == "Succeeded"
            and observed["cluster"].get("currentKubernetesVersion") == qualification.capacity.PATCH,
            f"{self.role}: Azure scope/cluster/patch changed",
        )
        tags = observed["group"].get("tags") or {}
        require(tags.get("run_id") == qualification.capacity.RESOURCE_GROUP
                and tags.get("clustermesh_debug_preserved") == "true"
                and tags.get("scenario") == "perf-eval-clustermesh-scale"
                and tags.get("clustermesh_debug_expected_clusters") == "100"
                and tags.get("clustermesh_debug_tfvars_sha256") == qualification.capacity.TFVARS_SHA
                and resource_equal(observed["node_group"].get("managedBy"), source["cluster"]["id"])
                and str(observed["node_group"].get("location", "")).lower() == qualification.capacity.REGION
                and str(observed["cluster"].get("nodeResourceGroup", "")).lower() == source["node_group"].lower(),
                f"{self.role}: preserved scope/ownership/tfvars changed")
        qualification.capacity.prepared.require_lease(observed["group"], self.args.timeout_seconds)
        qualification.capacity.prepared.require_lease(observed["node_group"], self.args.timeout_seconds)
        pools = {row["name"]: row for row in observed["pools"]}
        vmsses = {workers.vmss_pool_name(row): row for row in observed["vmsses"]}
        require(
            len(pools) == len(observed["pools"]) == len(vmsses) == len(observed["vmsses"]) == 3
            and set(pools) == set(vmsses) == {"default", "cniv5", "prompool"},
            f"{self.role}: pool/VMSS inventory changed",
        )
        if self.baseline is None:
            original = self.role_bundle["observation"]
            old_scales = {workers.vmss_pool_name(row): row for row in original["vmsses"]}
            self.baseline = {
                "pool_models": {row["name"]: pool_model(row) for row in original["pools"]},
                "vmss_models": {name: vmss_model(row) for name, row in old_scales.items()},
                "vm_ids": {
                    name: {
                        str(item["instanceId"]): item["vmId"]
                        for item in original["instances"][row["name"]]
                    } for name, row in old_scales.items()
                },
                "customer_operation": observed["customer_operation"]["name"],
                "default_operation": original["operations"]["default"]["name"],
            }
        require(
            all(pool_model(pools[name]) == self.baseline["pool_models"][name] for name in pools)
            and all(vmss_model(vmsses[name]) == self.baseline["vmss_models"][name] for name in vmsses),
            f"{self.role}: pool/VMSS model changed outside the default count delta",
        )
        original = self.role_bundle["observation"]
        require(
            stalled.arm_canonical(observed["vmss_models"]) == stalled.arm_canonical(original["vmss_models"]),
            f"{self.role}: VMSS image/disk model changed from qualified evidence",
        )
        for pool_name, scale in vmsses.items():
            original_rows = {
                str(row["instanceId"]): row for row in original["instances"][scale["name"]]
            }
            for row in observed["instances"][scale["name"]]:
                before = original_rows.get(str(row.get("instanceId")))
                require(before is not None and row.get("vmId") == before.get("vmId")
                        and row.get("computerName") == before.get("computerName")
                        and resource_equal(row.get("id"), before.get("id")),
                        f"{self.role}: protected {pool_name} VM identity changed")
        for name in ("cniv5", "prompool"):
            operation = observed["operations"][name]
            require(operation.get("name") == original["operations"][name]["name"]
                    and operation.get("status") == "Succeeded"
                    and not operation.get("error") and not operation.get("errorCode"),
                    f"{self.role}: unrelated protected pool operation overlapped retirement")
            require(
                pools[name].get("count") == self.role_bundle["capacity"]["desired_configuration"]["count"]
                if name == "cniv5" else pools[name].get("count") == 1,
                f"{self.role}: protected secondary/monitoring pool count changed",
            )
            require(
                pools[name].get("provisioningState") == "Succeeded"
                and vmsses[name].get("provisioningState") == "Succeeded",
                f"{self.role}: protected {name} provider state changed",
            )
            scale_name = vmsses[name]["name"]
            rows = observed["instances"][scale_name]
            by_id = {str(row["instanceId"]): row for row in rows}
            require(
                len(by_id) == len(rows)
                and {instance: row["vmId"] for instance, row in by_id.items()}
                == self.baseline["vm_ids"][name],
                f"{self.role}: protected {name} VM identity inventory changed",
            )
            for instance, row in by_id.items():
                view = observed["views"][f"{scale_name}/{instance}"]
                require(
                    row.get("provisioningState") == "Succeeded"
                    and row.get("latestModelApplied") is True
                    and stalled.guest_state(view, max_age_seconds=300) == "ready"
                    and stalled.extensions_ready(view),
                    f"{self.role}: protected {name} VM guest/extensions changed",
                )
        allowed_counts = (
            {self.settings["initial_count"], self.settings["final_count"]}
            if post else {self.settings["initial_count"]}
        )
        require(
            pools["default"].get("count") in allowed_counts
            and (vmsses["default"].get("sku") or {}).get("capacity") in allowed_counts
            and pools["default"].get("provisioningState")
            in ({"Succeeded"} if not post else {"Succeeded", "Updating", "Scaling", "DeletingMachines"})
            and vmsses["default"].get("provisioningState")
            in ({"Failed"} if not post else {"Failed", "Updating", "Succeeded"}),
            f"{self.role}: default pool count/state is not the authorized transition",
        )
        default_name = vmsses["default"]["name"]
        rows = observed["instances"][default_name]
        by_id = {str(row["instanceId"]): row for row in rows}
        require(len(by_id) == len(rows), f"{self.role}: default instance IDs are duplicated")
        target_present = self.settings["target_instance"] in by_id
        require(not (self.fenced and target_present), f"{self.role}: fenced VM reappeared")
        if not post or target_present:
            require(
                set(by_id) == self.settings["retained_instances"] | {self.settings["target_instance"]}
                and by_id[self.settings["target_instance"]]["vmId"] == self.settings["target_vm"]
                and by_id[self.settings["target_instance"]].get("provisioningState")
                in ({"Failed", "Deleting", "Updating"} if post else {"Failed"}),
                f"{self.role}: exact failed default VM is absent or changed",
            )
            if not post:
                view = observed["views"][f"{default_name}/{self.settings['target_instance']}"]
                failure = qualification.capacity.terminal_failure(
                    view, qualification.capacity.TERMINAL_CODES[self.role],
                )
                require(failure == source["failed"]["terminal_failure"],
                        f"{self.role}: terminal OS failure identity/time/message changed")
        else:
            require(
                set(by_id) == self.settings["retained_instances"],
                f"{self.role}: target VM remains or a protected default VM disappeared",
            )
        for instance in self.settings["retained_instances"]:
            row = by_id[instance]
            require(
                row["vmId"] == self.baseline["vm_ids"]["default"][instance]
                and row.get("provisioningState") == "Succeeded"
                and row.get("latestModelApplied") is True
                and stalled.guest_state(
                    observed["views"][f"{default_name}/{instance}"], max_age_seconds=300,
                ) == "ready"
                and stalled.extensions_ready(observed["views"][f"{default_name}/{instance}"]),
                f"{self.role}: protected default VM identity/guest/extensions changed",
            )
        customer = observed["customer_operation"]
        require(
            customer.get("name") == self.baseline["customer_operation"]
            and customer.get("status") == "Succeeded"
            and not customer.get("errorCode"),
            f"{self.role}: unrelated top-level AKS operation overlapped retirement",
        )
        require(not customer.get("error"), f"{self.role}: top-level AKS operation failed")
        return pools, vmsses, by_id

    def _kubernetes_guard(self, observed, *, post=False, final=False):
        snapshot = observed["snapshot"]
        pod_rows = snapshot["pods"]["items"]
        require(len({object_uid(pod) for pod in pod_rows}) == len(pod_rows)
                and len({(pod["metadata"]["namespace"], pod["metadata"]["name"]) for pod in pod_rows})
                == len(pod_rows), f"{self.role}: Pod identity inventory contains duplicates")
        source = self.role_bundle["source"]
        receipt = self.role_bundle["receipt"]
        settings = self.settings
        require(
            qualification.controller_pins(snapshot["controllers"])
            == receipt["final_evidence"]["controller_pins"],
            f"{self.role}: controller UID/spec contracts changed",
        )
        statefulsets = [
            row for row in snapshot["controllers"]["items"]
            if row.get("kind") == "StatefulSet"
            and (row.get("metadata") or {}).get("namespace") == mocks.DEFAULT_NAMESPACE
            and (row.get("metadata") or {}).get("name") == "kwok-node"
        ]
        require(len(statefulsets) == 1, f"{self.role}: mock StatefulSet is ambiguous")
        template = (
            (statefulsets[0].get("spec") or {}).get("template") or {}
        ).get("spec") or {}
        require(
            object_uid(statefulsets[0])
            == receipt["final_evidence"]["mock_statefulset"]["uid"]
            and not template.get("nodeName")
            and template.get("schedulerName", "default-scheduler") == "default-scheduler"
            and not mocks._tolerates(self.hold_taint(), template.get("tolerations") or []),
            f"{self.role}: current mock StatefulSet can bypass the owned healthy-worker hold",
        )
        self._pdb_guard(snapshot)
        leases = self._lease_guard(snapshot)
        nodes = maintenance._real_node_map(snapshot["nodes"])
        kwok = maintenance._kwok_map(snapshot["nodes"])
        require(
            len(kwok) == 100
            and {name: object_uid(row) for name, row in kwok.items()}
            == {name: row["uid"] for name, row in source["kwok_contracts"].items()}
            and all(workers.node_is_ready(row) for row in kwok.values())
            and {name: qualification.capacity.node_contract(row) for name, row in kwok.items()}
            == source["kwok_contracts"],
            f"{self.role}: KWOK UID/readiness inventory changed",
        )
        agents = maintenance._agent_map(snapshot["pods"])
        raw_agents = self.role_bundle["raw_agents"]
        healthy = receipt["final_evidence"]["protected_healthy_mock_uids"]
        targets = receipt["final_evidence"]["target_host"]["terminating_mock_uids"]
        target_pins = receipt["final_evidence"]["target_host"]["target_pods"]
        current_target_pods = {
            object_uid(pod): pod for pod in snapshot["pods"]["items"]
            if (pod.get("spec") or {}).get("nodeName") == settings["target"]
        }
        unexpected_target_uids = set(current_target_pods) - set(target_pins)
        if unexpected_target_uids and not self.fenced and any(
            (current_target_pods[pod_uid].get("metadata") or {}).get("namespace")
            == mocks.DEFAULT_NAMESPACE
            and (current_target_pods[pod_uid].get("metadata") or {}).get("name") in targets
            for pod_uid in unexpected_target_uids
        ):
            raise workers.ReconcileError(
                f"{self.role}: new mock UID appeared before positive VM absence"
            )
        require(
            (set(current_target_pods) == set(target_pins) if not post
             else set(current_target_pods) <= set(target_pins)),
            f"{self.role}: failed target-host Pod identity set changed unexpectedly",
        )
        for pod_uid, pod in current_target_pods.items():
            metadata = pod.get("metadata") or {}
            owners = [
                owner for owner in metadata.get("ownerReferences") or []
                if owner.get("controller") is True
            ]
            pin = target_pins[pod_uid]
            require(
                len(owners) == 1 and owners[0] == pin["owner"]
                and digest(pod.get("spec") or {}) == pin["spec_sha256"]
                and pvc_free(pod.get("spec") or {})
                and not maintenance._readiness_condition_true(pod),
                f"{self.role}/{metadata.get('namespace')}/{metadata.get('name')}: "
                "target-host owner/spec/PVC/PodReady state changed",
            )
        require(set(healthy) | set(targets) == set(raw_agents)
                and set(healthy) <= set(agents) <= set(raw_agents)
                and (post or set(agents) == set(raw_agents)),
                f"{self.role}: logical mock names changed")
        for name, pod_uid in healthy.items():
            require(
                object_uid(agents[name]) == pod_uid
                and stalled.base.pod_ready(agents[name])
                and qualification.pod_contract(agents[name]) == qualification.pod_contract(raw_agents[name])
                and prior.semantic_pod_spec(agents[name]["spec"])
                == prior.semantic_pod_spec(raw_agents[name]["spec"]),
                f"{self.role}/{name}: protected healthy mock UID/spec/readiness changed",
            )
        if not post:
            for name, pod_uid in targets.items():
                pod = agents[name]
                require(
                    object_uid(pod) == pod_uid
                    and (pod.get("metadata") or {}).get("deletionTimestamp")
                    and (pod.get("spec") or {}).get("nodeName") == settings["target"]
                    and not maintenance._readiness_condition_true(pod) and pvc_free(pod["spec"]),
                    f"{self.role}/{name}: terminating target mock changed",
                )
        replacements = {}
        for name, old_uid in targets.items():
            pod = agents.get(name)
            if pod is None or object_uid(pod) == old_uid:
                if pod is not None:
                    require(
                        (pod.get("metadata") or {}).get("deletionTimestamp")
                        and (pod.get("spec") or {}).get("nodeName") == settings["target"]
                        and prior.semantic_pod_spec(pod["spec"])
                        == prior.semantic_pod_spec(raw_agents[name]["spec"]),
                        f"{self.role}/{name}: original target mock changed while terminating",
                    )
                continue
            require(self.fenced, f"{self.role}: new mock UID appeared before positive VM absence")
            owners = [
                owner for owner in (pod.get("metadata") or {}).get("ownerReferences") or []
                if owner.get("kind") == "StatefulSet" and owner.get("controller") is True
            ]
            require(
                len(owners) == 1
                and owners[0]["uid"] == receipt["final_evidence"]["mock_statefulset"]["uid"]
                and object_uid(pod) not in {object_uid(item) for item in raw_agents.values()}
                and prior.semantic_pod_spec(pod["spec"])
                == prior.semantic_pod_spec(raw_agents[name]["spec"])
                and pvc_free(pod["spec"])
                and (pod.get("spec") or {}).get("nodeName")
                in (None, "", *self.role_bundle["capacity"]["new_identities"]),
                f"{self.role}/{name}: natural replacement identity/owner/spec/PVC/placement changed",
            )
            prior_uid = self.replacement_uids.get(name)
            require(prior_uid in (None, object_uid(pod)),
                    f"{self.role}/{name}: replacement UID changed")
            self.replacement_uids[name] = object_uid(pod)
            replacements[name] = {
                "old_uid": old_uid, "new_uid": object_uid(pod),
                "node_name": pod["spec"].get("nodeName"),
                "ready": stalled.base.pod_ready(pod)
                and pod["spec"].get("nodeName") in self.role_bundle["capacity"]["new_identities"],
                "fencing_proven": True,
            }
        for pin_uid, pin in self.role_bundle["monitoring_pins"].items():
            matches = [
                pod for pod in snapshot["pods"]["items"]
                if (pod.get("metadata") or {}).get("uid") == pin_uid
            ]
            if post and pin_uid in target_pins and not matches:
                continue
            require(
                len(matches) == 1 and qualification.pod_contract(matches[0]) == pin,
                f"{self.role}: monitoring Pod UID/spec/placement changed",
            )
        by_uid = {
            object_uid(pod): pod for pod in snapshot["pods"]["items"]
            if (pod.get("metadata") or {}).get("uid")
        }
        for pod_uid, pin in self.protected_hold_pods.items():
            require(
                pod_uid in by_uid
                and qualification.pod_contract(stalled.safe_diagnostics(by_uid[pod_uid])) == pin
                and stalled.base.pod_ready(by_uid[pod_uid]),
                f"{self.role}: a healthy Pod on an owned hold target changed or was evicted",
            )
        expected_nodes = set(source["node_contracts"])
        replacement_nodes = set(self.role_bundle["capacity"]["new_identities"])
        allowed_nodes = expected_nodes | replacement_nodes
        if post:
            allowed_nodes.remove(settings["target"])
        require(
            allowed_nodes <= set(nodes)
            <= allowed_nodes | ({settings["target"]} if post and not final else set()),
            f"{self.role}: real Node inventory changed outside target removal",
        )
        if settings["target"] in nodes:
            require(
                object_uid(nodes[settings["target"]]) == settings["target_uid"]
                and not workers.node_is_ready(nodes[settings["target"]])
                and qualification.capacity.node_contract(nodes[settings["target"]])
                == source["node_contracts"][settings["target"]],
                f"{self.role}: failed target Node identity/readiness changed",
            )
        for name in expected_nodes - {settings["target"]}:
            node = nodes[name]
            expected = source["node_contracts"][name]
            taints = copy.deepcopy((node.get("spec") or {}).get("taints") or [])
            owned = [row for row in taints if row.get("key") == qualification.NATIVE_HOLD_KEY]
            if name in self.hold_applied:
                require(owned == [self.hold_taint()],
                        f"{self.role}/{name}: owned hold changed")
                taints.remove(self.hold_taint())
            else:
                require(not owned, f"{self.role}/{name}: unexpected fencing hold exists")
            normalized = copy.deepcopy(node)
            normalized.setdefault("spec", {})["taints"] = taints
            contract = qualification.capacity.node_contract(normalized)
            require(
                contract == expected
                and workers.node_is_ready(node)
                and (node.get("status") or {}).get("nodeInfo", {}).get("bootID")
                == (self.baseline_real_nodes[name].get("status") or {}).get(
                    "nodeInfo", {}
                ).get("bootID"),
                f"{self.role}/{name}: protected source Node UID/spec/readiness changed",
            )
            core_daemonsets = set(map(tuple, source["daemonsets"]))
            require(
                core_daemonsets
                <= maintenance._healthy_system_daemonsets_on_node(snapshot["pods"], name),
                f"{self.role}/{name}: protected Cilium/CNS readiness changed",
            )
        for name, identity in self.role_bundle["capacity"]["new_identities"].items():
            node = nodes[name]
            expected_daemonsets = set(map(tuple, source["daemonsets"]))
            require(
                object_uid(node) == identity["node_uid"]
                and (node.get("status") or {}).get("nodeInfo", {}).get("bootID")
                == identity["boot_id"]
                and workers.node_is_ready(node)
                and qualification.capacity.node_contract(node)
                == qualification.capacity.node_contract(self.baseline_real_nodes[name])
                and receipt["final_evidence"]["system_daemonsets"][name]["all_expected_ready"] is True,
                f"{self.role}/{name}: qualified cniv5 Node/boot/readiness changed",
            )
            require(
                expected_daemonsets
                <= maintenance._healthy_system_daemonsets_on_node(snapshot["pods"], name),
                f"{self.role}/{name}: qualified Cilium/CNS/system DaemonSet readiness changed",
            )
        raw_networks = mocks._items(snapshot["nnc"], f"{self.role} NNC inventory")
        network_map = {row["metadata"]["name"]: row for row in raw_networks}
        require(len(network_map) == len(raw_networks), f"{self.role}: NNC names are duplicated")
        baseline_networks = {
            row["metadata"]["name"]: qualification.capacity.network_record(row)
            for row in self.role_bundle["final_objects"]["nnc"]["items"]
        }
        required_networks = set(baseline_networks) - ({settings["target"]} if post else set())
        require(required_networks <= set(network_map) <= set(baseline_networks),
                f"{self.role}: protected NNC inventory changed")
        current_networks = {}
        all_ips = set()
        for name, row in network_map.items():
            if name == settings["target"] and post:
                baseline = baseline_networks[name]
                if self.native_submitted:
                    metadata = row.get("metadata") or {}
                    owners = metadata.get("ownerReferences") or []
                    containers = (row.get("status") or {}).get("networkContainers") or []
                    require(object_uid(row) == baseline["uid"] and metadata.get("namespace") == "kube-system"
                            and any(owner.get("kind") == "Node" and owner.get("uid") == settings["target_uid"]
                                    and owner.get("name") == name for owner in owners)
                            and (not containers or len(containers) == 1
                                 and containers[0].get("id") == baseline["network_container_id"]),
                            f"{self.role}: natively retiring NNC ownership changed")
                    if final:
                        raise workers.ReconcileError(f"{self.role}: target NNC still exists")
                    continue
                current = qualification.capacity.network_record(row)
                require(
                    all(current[key] == baseline[key] for key in (
                        "uid", "node_uid", "network_container_id",
                    )),
                    f"{self.role}: retiring target NNC identity changed",
                )
                if final:
                    raise workers.ReconcileError(f"{self.role}: target NNC still exists")
                continue
            network = qualification.capacity.network_record(row)
            baseline = baseline_networks[name]
            previous = self.latest_networks.get(name, baseline)
            require(
                all(network[key] == baseline[key] for key in (
                    "uid", "node_uid", "network_container_id",
                ))
                and network["version"] >= previous["version"]
                and (network["version"] > previous["version"]
                     or network["ip_addresses"] == previous["ip_addresses"])
                and not all_ips.intersection(network["ip_addresses"]),
                f"{self.role}/{name}: NNC identity/version/allocation changed unsafely",
            )
            resident = {
                pod.get("status", {}).get("podIP")
                for pod in snapshot["pods"]["items"]
                if (pod.get("spec") or {}).get("nodeName") == name
                and not (pod.get("spec") or {}).get("hostNetwork")
                and (pod.get("status") or {}).get("podIP")
                and not (pod.get("metadata") or {}).get("deletionTimestamp")
            }
            require(resident <= set(network["ip_addresses"]),
                    f"{self.role}/{name}: NNC lost a resident Pod IP")
            all_ips.update(network["ip_addresses"])
            current_networks[name] = network
        self.latest_networks.update(copy.deepcopy(current_networks))
        target_refs_absent = (
            settings["target"] not in nodes
            and settings["target"] not in network_map
            and not any(
                (pod.get("spec") or {}).get("nodeName") == settings["target"]
                for pod in snapshot["pods"]["items"]
            )
        )
        ready = (
            len(replacements) == settings["replacement_count"]
            and all(row["ready"] for row in replacements.values())
        )
        return {
            "snapshot": snapshot, "nodes": nodes, "agents": agents,
            "networks": current_networks, "replacements": replacements,
            "target_refs_absent": target_refs_absent, "replacements_ready": ready,
            "leases": leases,
        }

    def _headroom_guard(self, state, *, project_replacements=False):
        snapshot = state["snapshot"]
        node_metrics, pod_metrics, metrics, usage = self._metrics()
        receipt = self.role_bundle["receipt"]
        healthy = receipt["final_evidence"]["protected_healthy_mock_uids"]
        require(set(healthy) <= set(usage), f"{self.role}: healthy mock RSS samples are missing")
        rss = max(
            self.healthy_rss_high_water,
            receipt["placement_headroom"]["healthy_rss_high_water_bytes"],
            max(usage[name]["memory_bytes"] for name in healthy),
        )
        self.healthy_rss_high_water = rss
        if project_replacements:
            projection = SimpleNamespace(
                role=self.role, source=self.role_bundle["source"],
                inputs={"receipt": {"per_role": {self.role: self.role_bundle["capacity"]}}},
                role_summary={"ip_growth": copy.deepcopy(receipt["ip_growth"])},
                healthy_rss_high_water=rss,
                _metrics=lambda _snapshot: (state["nodes"], state["agents"], metrics, usage),
                save=lambda: None,
            )
            result = qualification.RoleQualification.system_headroom(
                projection, snapshot, state["networks"], include_probes=False,
            )
            self.healthy_rss_high_water = projection.healthy_rss_high_water
            self.role_summary["pre_retirement_headroom"] = result
            return result
        counts = Counter(
            state["agents"][name]["spec"]["nodeName"]
            for name in receipt["final_evidence"]["target_host"]["terminating_mock_uids"]
        )
        for name in self.role_bundle["capacity"]["new_identities"]:
            require(name in metrics, f"{self.role}/{name}: final Node metrics are missing")
            node = state["nodes"][name]
            metric = metrics[name]
            active = [
                pod for pod in snapshot["pods"]["items"]
                if (pod.get("spec") or {}).get("nodeName") == name
                and (pod.get("status") or {}).get("phase") not in ("Succeeded", "Failed")
            ]
            requested_cpu = sum(mocks._resource_requests(pod)[0] for pod in active)
            requested_memory = sum(mocks._resource_requests(pod)[1] for pod in active)
            used_cpu = int(mocks._quantity(metric["usage"]["cpu"], "CPU") * 1000)
            used_memory = int(mocks._quantity(metric["usage"]["memory"], "memory"))
            allocatable = node["status"]["allocatable"]
            projected_replacement_memory = counts[name] * rss
            requested_replacement_memory = sum(
                mocks._resource_requests(state["agents"][logical])[1]
                for logical, replacement in state["replacements"].items()
                if replacement["node_name"] == name
            )
            require(
                counts[name]
                <= receipt["placement_headroom"]["destinations"][name]["safe_slots"]
                and
                maintenance._headroom_ok(
                    node, metric, threshold_percent=85,
                    effective_reserved_memory_bytes=(
                        max(requested_memory - used_memory, 0)
                        + max(projected_replacement_memory - requested_replacement_memory, 0)
                        + 512 * 1024**2
                    ),
                    next_memory_bytes=0,
                )
                and max(requested_cpu, used_cpu) + 250
                < int(mocks._quantity(allocatable["cpu"], "CPU") * 1000) * 85 // 100
                and len(active) + 5 <= int(allocatable["pods"]),
                f"{self.role}/{name}: final actual CPU/memory/Pod-slot headroom is unsafe",
            )
            for agent_name, replacement in state["replacements"].items():
                if replacement["node_name"] == name:
                    require(
                        state["agents"][agent_name]["status"]["podIP"]
                        in state["networks"][name]["ip_addresses"],
                        f"{self.role}/{agent_name}: replacement Pod IP is outside its NNC",
                    )
        return {
            "captured_at": utc_now(), "replacement_counts": dict(counts),
            "healthy_rss_high_water_bytes": rss, "threshold_percent": 85,
            "node_metrics_sha256": digest(node_metrics),
            "pod_metrics_sha256": digest(pod_metrics),
            "actual_metrics": True,
        }

    def preflight(self, *, allow_holds=False):
        self.unchanged_inputs()
        observed = self.capture()
        self.validate_journals(
            observed["snapshot"]["configmaps"], allow_own=bool(self.journal_uid),
        )
        self._provider_guard(observed, post=False)
        state = self._kubernetes_guard(observed, post=False)
        if not allow_holds:
            require(not self.hold_applied, f"{self.role}: no hold is allowed in plan preflight")
        child = observed["operations"]["default"]
        require(
            child.get("status") == "Succeeded"
            and child.get("name") == self.baseline["default_operation"]
            and child.get("endTime") and not child.get("errorCode") and not child.get("error"),
            f"{self.role}: default child operation is not quiescent",
        )
        self._headroom_guard(state, project_replacements=True)
        return observed, state

    def journal_data(self):
        return {
            "owner": OWNER, "token": self.token, "role": self.role,
            "qualification_build_id": str(QUALIFICATION_BUILD),
            "qualification_tree_sha256": self.bundle["tree_sha256"],
            "qualification_journal_sha256": digest(
                self.role_bundle["qualification_journal_data"]
            ),
            "target_node_uid": self.settings["target_uid"],
            "target_vm_id": self.settings["target_vm"],
            "record": canonical({
                "status": self.role_summary["status"],
                "holds": self.role_summary["holds"],
                "native": self.role_summary["native"],
                "replacement_count": len(self.role_summary.get("replacements", {})),
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
            f"{self.role}: retirement journal UID/RV/token/data changed",
        )
        return row

    def raw_write(self, command):
        require(self.args.execute, "Plan mode cannot mutate")
        native = native_command(
            self.role_bundle["source"]["cluster"]["name"], self.settings["target"],
        )
        allowed = (
            command == native
            or command[:4] == ["kubectl", "create", "configmap", self.journal_name]
            or command[:4] == ["kubectl", "patch", "configmap", self.journal_name]
            or (
                command[:3] == ["kubectl", "patch", "node"]
                and command[3] in set(
                    self.role_bundle["receipt"]["final_evidence"][
                        "future_native_fencing_hold"
                    ]["target_nodes"]
                )
            )
        )
        require(allowed, f"{self.role}: write escaped journal/owned-hold/exact-native whitelist")
        if command[0] == "kubectl":
            require("-n" in command and command[command.index("-n") + 1] == "kube-system"
                    or command[:3] == ["kubectl", "patch", "node"],
                    f"{self.role}: journal write escaped kube-system")
        if command == native:
            require(not self.native_request_sent and self.journal_uid
                    and self.hold_applied == set(self.role_summary["holds"])
                    and self.role_summary["native"]["submission_started"] is True,
                    f"{self.role}: duplicate or unprepared native request")
            self.owned_journal()
            for name in self.hold_applied:
                node = self.kube("get", "node", name, "-o", "json")
                pin = self.role_bundle["receipt"]["final_evidence"]["protected_healthy_source_nodes"][name]
                require(object_uid(node) == pin["uid"]
                        and node.get("status", {}).get("nodeInfo", {}).get("bootID") == pin["boot_id"]
                        and workers.node_is_ready(node)
                        and (node.get("spec", {}).get("taints") or []).count(self.hold_taint()) == 1,
                        f"{self.role}/{name}: placement hold changed at native submission")
            self.native_request_sent = True
        if command[0] == "az" or command[:3] == ["kubectl", "patch", "node"]:
            self.unchanged_inputs()
        self.summary["mutation_started"] = True
        self.save()
        timeout_seconds = PROVIDER_SECONDS if command[0] == "az" else self.args.request_timeout_seconds
        return super().run(command, timeout_seconds, cleanup=self.cleanup_mode)

    def acquire(self):
        record = self.role_summary["journal"]
        require(not record["attempted"], f"{self.role}: retirement journal cannot be reacquired")
        self.unchanged_inputs()
        record.update(attempted=True, accepted=None, ambiguous=True, requested_at=utc_now())
        self.save()
        data = self.journal_data()
        output = self.raw_write([
            "kubectl", "create", "configmap", self.journal_name,
            "-n", "kube-system",
            *(f"--from-literal={key}={value}" for key, value in data.items()),
            "-o", "json",
        ])
        row = workers.parse_json(output, f"{self.role} retirement journal creation")
        self.journal_uid = object_uid(row)
        self.journal_rv = str((row.get("metadata") or {}).get("resourceVersion") or "")
        self.journal_pin = data
        require(self.journal_rv and row.get("data") == data,
                f"{self.role}: retirement journal creation is ambiguous")
        self.owned_journal()
        record.update(
            uid=self.journal_uid, resource_version=self.journal_rv,
            data_sha256=digest(data), accepted=True, ambiguous=False, accepted_at=utc_now(),
        )
        self.persist()

    def persist(self):
        self.owned_journal()
        desired = self.journal_data()
        if desired == self.journal_pin:
            self.role_summary["journal"].update(
                resource_version=self.journal_rv,
                data_sha256=digest(desired), noop_update_skipped=True,
            )
            self.save()
            return
        output = self.raw_write([
            "kubectl", "patch", "configmap", self.journal_name,
            "-n", "kube-system", "--type=json", "-p", json.dumps([
                {"op": "test", "path": "/metadata/uid", "value": self.journal_uid},
                {"op": "test", "path": "/metadata/resourceVersion", "value": self.journal_rv},
                {"op": "test", "path": "/data/token", "value": self.token},
                {"op": "test", "path": "/data", "value": self.journal_pin},
                {"op": "add", "path": "/data", "value": desired},
            ]), "-o", "json",
        ])
        row = workers.parse_json(output, f"{self.role} retirement journal CAS")
        new_rv = str((row.get("metadata") or {}).get("resourceVersion") or "")
        require(
            object_uid(row) == self.journal_uid and row.get("data") == desired
            and new_rv and new_rv != self.journal_rv,
            f"{self.role}: changed-data retirement journal CAS is ambiguous",
        )
        self.journal_pin = copy.deepcopy(desired)
        self.journal_rv = new_rv
        self.role_summary["journal"].update(
            resource_version=new_rv, data_sha256=digest(desired),
            noop_update_skipped=False,
        )
        self.owned_journal()
        self.save()

    def change_hold(self, node_name, *, remove=False):
        node = self.kube("get", "node", node_name, "-o", "json")
        expected = self.role_bundle["receipt"]["final_evidence"][
            "protected_healthy_source_nodes"
        ][node_name]
        require(
            object_uid(node) == expected["uid"]
            and (node.get("status") or {}).get("nodeInfo", {}).get("bootID")
            == expected["boot_id"]
            and workers.node_is_ready(node),
            f"{self.role}/{node_name}: healthy hold target identity/readiness changed",
        )
        taints = copy.deepcopy((node.get("spec") or {}).get("taints") or [])
        hold = self.hold_taint()
        key = "remove" if remove else "add"
        record = self.role_summary["holds"][node_name][key]
        require(not record["attempted"], f"{self.role}/{node_name}: hold {key} cannot repeat")
        patch = [
            {"op": "test", "path": "/metadata/uid", "value": object_uid(node)},
            {"op": "test", "path": "/metadata/resourceVersion",
             "value": node["metadata"]["resourceVersion"]},
        ]
        if "taints" in node.get("spec", {}):
            patch.append({"op": "test", "path": "/spec/taints", "value": copy.deepcopy(taints)})
        if remove:
            require(taints.count(hold) == 1, f"{self.role}/{node_name}: owned hold is not exact")
            taints.remove(hold)
        else:
            require(not any(row.get("key") == qualification.NATIVE_HOLD_KEY for row in taints),
                    f"{self.role}/{node_name}: another retirement hold exists")
            taints.append(hold)
        record.update(attempted=True, accepted=None, ambiguous=True, requested_at=utc_now())
        self.persist()
        self.progress(f"hold-{key}-submitting", node=node_name)
        output = self.raw_write([
            "kubectl", "patch", "node", node_name, "--type=json",
            "-p", json.dumps([*patch, {"op": "add", "path": "/spec/taints", "value": taints}]),
            "-o", "json",
        ])
        updated = workers.parse_json(output, f"{self.role}/{node_name} hold {key}")
        require(
            object_uid(updated) == expected["uid"]
            and (updated.get("spec") or {}).get("taints") == taints
            and (updated.get("metadata") or {}).get("resourceVersion")
            != node["metadata"]["resourceVersion"],
            f"{self.role}/{node_name}: hold {key} response is ambiguous",
        )
        if remove:
            self.hold_applied.remove(node_name)
        else:
            self.hold_applied.add(node_name)
        record.update(accepted=True, ambiguous=False, accepted_at=utc_now())
        self.role_summary["holds"][node_name]["applied"] = not remove
        self.persist()
        self.progress(f"hold-{key}-accepted", node=node_name)

    def submit_native(self, preflight):
        action = self.role_summary["native"]
        require(not action["attempted"] and self.journal_uid
                and self.hold_applied == set(self.role_summary["holds"]),
                f"{self.role}: native delete requires owned journal and all holds")
        command = native_command(
            self.role_bundle["source"]["cluster"]["name"], self.settings["target"],
        )
        action.update(
            attempted=True, submission_started=False, accepted=None, ambiguous=True,
            requested_at=utc_now(), command=command,
            previous_operation_name=preflight["operations"]["default"]["name"],
        )
        self.persist()
        self.preflight(allow_holds=True)
        action["submission_started"] = True
        action["submission_started_at"] = utc_now()
        self.persist()
        self.native_submitted = True
        self.progress("native-delete-submitting", target=self.settings["target"])
        try:
            self.raw_write(command)
        except EXPECTED_ERRORS:
            action["returned_at"] = utc_now()
            self.role_summary["status"] = "native-delete-ambiguous"
            self.save()
            try:
                self.persist()
            except EXPECTED_ERRORS as error:
                self.role_summary["ambiguous_journal_error"] = str(error)
                self.save()
            raise
        action.update(accepted=True, accepted_at=utc_now(), returned_at=utc_now())
        self.role_summary["status"] = "native-delete-accepted"
        self.persist()
        action["ambiguous"] = False
        try:
            self.persist()
        except EXPECTED_ERRORS:
            action["ambiguous"] = True
            self.save()
            raise
        self.progress("native-delete-accepted", target=self.settings["target"])

    def operation(self, observed):
        operation = observed["operations"]["default"]
        action = self.role_summary["native"]
        require(operation.get("name") and not operation.get("errorCode") and not operation.get("error"),
                f"{self.role}: default child operation is absent or failed")
        if operation["name"] == action["previous_operation_name"]:
            require(operation.get("status") == "Succeeded",
                    f"{self.role}: prior default operation became busy")
            return False
        started = timestamp(operation.get("startTime"), f"{self.role} native operation start")
        status = operation.get("status")
        progress = re.fullmatch(r"DeletingMachines: ([01])/1 (?:nodes|machines) completed", str(status))
        require(
            operation.get("operationType") in DELETE_TYPES
            and (status in PROGRESS_STATES | {"Succeeded"} or progress is not None)
            and timestamp(action["submission_started_at"], f"{self.role} submission")
            <= started <= datetime.now(timezone.utc)
            and resource_equal(operation.get("id"),
                f"{self.role_bundle['source']['cluster']['id']}/agentPools/default/operations/{operation['name']}")
            and (self.operation_name is None or operation["name"] == self.operation_name),
            f"{self.role}: unrelated or unsupported provider operation followed native deletion",
        )
        self.operation_name = operation["name"]
        action["operation_name"] = self.operation_name
        if operation["status"] == "Succeeded":
            ended = timestamp(operation.get("endTime"), f"{self.role} native operation end")
            require(started <= ended <= datetime.now(timezone.utc),
                    f"{self.role}: native operation completion time is invalid")
            return True
        require(not operation.get("endTime"),
                f"{self.role}: progressing native operation has a terminal end time")
        return False

    def wait_recovery(self):
        while True:
            observed = self.capture()
            self.validate_journals(observed["snapshot"]["configmaps"], allow_own=True)
            operation_complete = self.operation(observed)
            pools, vmsses, instances = self._provider_guard(observed, post=True)
            if (
                self.settings["target_instance"] not in instances
                and pools["default"]["count"] == self.settings["final_count"]
            ):
                self.fenced = True
                self.role_summary["native"].setdefault("vm_absence_observed_at", utc_now())
                self.role_summary["native_fencing_proven"] = True
            state = self._kubernetes_guard(observed, post=True)
            self.role_summary["replacements"] = state["replacements"]
            self.role_summary["status"] = "waiting-natural-controller-replacements"
            self.save()
            self.progress(
                "post-action-observation",
                operation=observed["operations"]["default"].get("status"),
                fenced=str(self.fenced).lower(),
                replacements=len(state["replacements"]),
                target_refs_absent=str(state["target_refs_absent"]).lower(),
            )
            if (
                self.fenced and operation_complete
                and pools["default"]["count"] == self.settings["final_count"]
                and vmsses["default"]["sku"]["capacity"] == self.settings["final_count"]
                and pools["default"]["provisioningState"] == "Succeeded"
                and vmsses["default"]["provisioningState"] == "Succeeded"
                and state["target_refs_absent"] and state["replacements_ready"]
            ):
                headroom = self._headroom_guard(state)
                self.role_summary["final_headroom"] = headroom
                return observed, state
            require(time.monotonic() < self.work_deadline,
                    f"{self.role}: native removal/replacements exceeded bounded wait")
            time.sleep(min(POLL_SECONDS, self.remaining_seconds(POLL_SECONDS)))

    def execute(self):
        self.progress("execution-start")
        self.preflight()
        self.acquire()
        self.progress("journal-acquired", journal_uid=self.journal_uid)
        for node_name in self.role_summary["holds"]:
            self.change_hold(node_name)
        self.progress("all-placement-holds-applied", count=len(self.hold_applied))
        guarded, _ = self.preflight(allow_holds=True)
        self.progress("held-boundary-preflight-complete")
        self.submit_native(guarded)
        self.wait_recovery()
        self.progress(
            "native-fencing-and-replacements-complete",
            replacements=len(self.role_summary["replacements"]),
        )
        self.cleanup_mode = True
        for node_name in self.role_summary["holds"]:
            self.change_hold(node_name, remove=True)
        self.progress("all-placement-holds-removed")
        observed = self.capture()
        self.validate_journals(observed["snapshot"]["configmaps"], allow_own=True)
        require(self.operation(observed), f"{self.role}: native operation did not remain completed")
        pools, vmsses, _ = self._provider_guard(observed, post=True)
        require(pools["default"]["count"] == vmsses["default"]["sku"]["capacity"]
                == self.settings["final_count"], f"{self.role}: final default capacity changed")
        state = self._kubernetes_guard(observed, post=True, final=True)
        require(
            state["target_refs_absent"] and state["replacements_ready"]
            and not self.hold_applied,
            f"{self.role}: final target/hold/replacement state is incomplete",
        )
        self.unchanged_inputs()
        self.role_summary["final_headroom"] = self._headroom_guard(state)
        self.role_summary.update(
            status="system-role-recovered-workloads-not-started",
            success=True, source_retired=True, native_fencing_proven=True,
            replacements_ready=True, placement_holds_removed=True,
        )
        evidence = {
            "observed": observed, "state": {
                "replacements": state["replacements"],
                "target_refs_absent": state["target_refs_absent"],
                "replacements_ready": state["replacements_ready"],
            },
            "final_headroom": self.role_summary["final_headroom"],
        }
        self.role_summary["final_evidence_artifact"] = self.export_evidence("final", evidence)
        self.persist()
        self.progress("role-recovery-complete")


def validate_args(args):
    require(
        args.qualification_build_id == QUALIFICATION_BUILD
        and args.resource_group == args.confirm_resource_group
        == qualification.capacity.RESOURCE_GROUP
        and args.expected_subscription.lower() == qualification.capacity.SUBSCRIPTION
        and args.expected_region.lower() == qualification.capacity.REGION
        and args.expected_tfvars_sha.lower() == qualification.capacity.TFVARS_SHA,
        "Retirement qualification/scope/subscription/region/tfvars confirmation mismatch",
    )
    require(
        isinstance(args.timeout_seconds, int) and 1200 <= args.timeout_seconds <= 7200
        and isinstance(args.request_timeout_seconds, int)
        and 10 <= args.request_timeout_seconds <= 120,
        "Retirement timeout values are outside bounded limits",
    )
    source = Path(args.qualification_directory).resolve()
    kube = Path(args.kubeconfig_directory).resolve()
    output = Path(args.summary_file).resolve()
    require(
        source.is_dir() and kube.is_dir()
        and not Path(args.qualification_directory).is_symlink()
        and not Path(args.kubeconfig_directory).is_symlink()
        and len({source, kube, output}) == 3
        and source not in output.parents and kube not in output.parents
        and output not in source.parents and output not in kube.parents
        and not output.exists(),
        "Qualification input, private kubeconfigs, and new output must be separate",
    )
    files = {path.name for path in kube.iterdir() if path.is_file()}
    require(
        files == {f"{role}.config" for role in ROLES}
        and all(not path.is_symlink() for path in kube.iterdir()),
        "Kubeconfig directory must contain exactly mesh-51/66/79 private configs",
    )


def execute_recovery(args, summary, runner=workers.run_command):
    validate_args(args)
    summary.update(
        schema_version=1, execute=args.execute, mutation_started=False,
        plan_valid=False, success=False, status="validating-partial-qualification",
        qualification_build_id=QUALIFICATION_BUILD,
        system_roles_recovered=False, unresolved_roles=["mesh-2", "mesh-89", "mesh-94"],
        workloads_ready=False, completed_global_baseline=False,
        started_at=utc_now(), finished_at=None, per_role={}, cleanup_errors=[],
    )
    try:
        bundle = load_inputs(args)
        summary.update(
            qualification_input_hashes=bundle["hashes"],
            qualification_input_sha256=bundle["tree_sha256"],
            source_tree_sha256=bundle["receipt"]["source_tree_sha256"],
            capacity_input_sha256=bundle["receipt"]["capacity_input_sha256"],
            mesh89_explicitly_unqualified=True,
        )
        recoveries = []
        for role in ROLES:
            hold_nodes = bundle["roles"][role]["receipt"]["final_evidence"][
                "future_native_fencing_hold"
            ]["target_nodes"]
            summary["per_role"][role] = {
                "role": role,
                "cluster": bundle["roles"][role]["source"]["cluster"]["name"],
                "status": "validating", "success": False,
                "source_retired": False, "native_fencing_proven": False,
                "replacements_ready": False, "placement_holds_removed": False,
                "workloads_ready": False, "completed_global_baseline": False,
                "journal": {
                    "name": f"{JOURNAL_PREFIX}-{role}", "namespace": "kube-system",
                    "retained": True, "attempted": False,
                    "accepted": None, "ambiguous": False,
                },
                "holds": {
                    name: {"applied": False, "add": hold_receipt(), "remove": hold_receipt()}
                    for name in hold_nodes
                },
                "native": action_receipt(), "replacements": {},
            }
            recoveries.append(RoleRecovery(args, bundle, role, summary, runner))
        for recovery in recoveries:
            recovery.progress("global-preflight-start")
            observed, state = recovery.preflight()
            summary["per_role"][recovery.role]["preflight_evidence_artifact"] = (
                recovery.export_evidence("preflight", {
                    "observed": observed,
                    "state": {
                        "target_refs_absent": state["target_refs_absent"],
                        "replacements_ready": state["replacements_ready"],
                    },
                })
            )
            summary["per_role"][recovery.role]["status"] = "plan-valid"
            recovery.save()
            recovery.progress("global-preflight-complete")
        summary.update(plan_valid=True, status="plan-valid", success=not args.execute)
        if not args.execute:
            print(f"{utc_now()}: all three system-role plans are valid; no mutations submitted", flush=True)
            return
        for index, recovery in enumerate(recoveries, start=1):
            recovery.execute()
            print(
                f"##vso[task.setprogress value={index * 30};]"
                f"{recovery.role}: {len(recovery.role_summary['replacements'])} mock replacements Ready; "
                "final cohort observation still required",
                flush=True,
            )
        for recovery in recoveries:
            observed = recovery.capture()
            recovery.validate_journals(observed["snapshot"]["configmaps"], allow_own=True)
            require(recovery.operation(observed), f"{recovery.role}: final native operation changed")
            pools, vmsses, _ = recovery._provider_guard(observed, post=True)
            state = recovery._kubernetes_guard(observed, post=True, final=True)
            require(pools["default"]["count"] == vmsses["default"]["sku"]["capacity"]
                    == recovery.settings["final_count"]
                    and state["target_refs_absent"] and state["replacements_ready"]
                    and not recovery.hold_applied, f"{recovery.role}: cohort completion regressed")
            recovery.role_summary["final_headroom"] = recovery._headroom_guard(state)
            recovery.role_summary["final_evidence_artifact"] = recovery.export_evidence(
                "cohort-final", {"observed": observed, "final_headroom": recovery.role_summary["final_headroom"],
                                 "replacements": state["replacements"]},
            )
            recovery.save()
            recovery.progress("cohort-final-readiness-confirmed")
        require(all(row["success"] for row in summary["per_role"].values()),
                "All three qualified system roles must complete")
        summary.update(
            success=True, status="qualified-system-roles-recovered",
            system_roles_recovered=True,
            workloads_ready=False, completed_global_baseline=False,
        )
        print("##vso[task.setprogress value=100;]Three qualified System roles recovered; benchmarks not started", flush=True)
        print(
            f"{utc_now()}: all three qualified system roles recovered; "
            "mesh-2/mesh-89/mesh-94 remain unresolved",
            flush=True,
        )
    except EXPECTED_ERRORS as error:
        summary.update(
            success=False, status="failed-closed", error=str(error),
            system_roles_recovered=False,
            workloads_ready=False, completed_global_baseline=False,
        )
        for role, row in summary.get("per_role", {}).items():
            if any(hold["remove"].get("attempted") and hold["remove"].get("accepted") is not True
                   for hold in row.get("holds", {}).values()):
                summary["cleanup_errors"].append({"role": role, "error": str(error)})
        summary["hold_disposition"] = "Retain any applied or ambiguous owned holds; no automatic replay or undo."
        raise
    finally:
        summary["finished_at"] = utc_now()
        summary["workloads_ready"] = False
        summary["completed_global_baseline"] = False
        mocks.write_json_atomic(args.summary_file, summary)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "qualification-directory", "resource-group", "confirm-resource-group",
        "expected-subscription", "expected-region", "expected-tfvars-sha",
        "kubeconfig-directory", "summary-file",
    ):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--qualification-build-id", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=7200)
    parser.add_argument("--request-timeout-seconds", type=int, default=60)
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    def interrupted(signum, _frame):
        raise workers.ReconcileError(
            f"Interrupted ({signum}); retain journals/holds and never replay native deletion"
        )

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, interrupted)
    args, summary = parse_args(argv), {}
    try:
        execute_recovery(args, summary)
    except EXPECTED_ERRORS as error:
        print(f"Secondary failed-worker retirement failed closed: {error}", file=sys.stderr)
        return 1
    print(
        f"{summary['status']}; system_roles_recovered="
        f"{str(summary['system_roles_recovered']).lower()}; workloads_ready=false",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
