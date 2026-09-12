"""Continue the quota-rejected mesh-96 scale only from a freshly proven zero.

The old deletion and ambiguous scale receipts remain immutable lineage. A retained
exclusive ConfigMap owns the single new attempt; it is never a retry/adoption key.
"""

# pylint: disable=protected-access,too-many-boolean-expressions

from __future__ import annotations

import copy
import hashlib
import json
import re
import shlex
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import failed_prom_worker_replacement as replacement
import unreachable_prom_worker_recovery as recovery


GUARD_NAME = "mesh96-prom-capacity-restore"
QUOTA_FAMILY = "standardDv3Family"
QUOTA_CORES = 8
USAGE_QUERY = "[].{name:name.value,currentValue:currentValue,limit:limit}"
SCALE_COMMAND = [
    "az", "aks", "nodepool", "scale", "--resource-group", recovery.RESOURCE_GROUP,
    "--cluster-name", recovery.CLUSTER, "--name", "prompool", "--node-count", "1",
    "--no-wait", "--only-show-errors", "--output", "none",
]
require = recovery.require


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def valid_uuid(value):
    return isinstance(value, str) and recovery.maintenance.UUID_RE.fullmatch(value) is not None


def valid_sha(value):
    return isinstance(value, str) and recovery.maintenance.SHA256_RE.fullmatch(value) is not None


def quota_counter(value):
    require(recovery.integer(value) or (
        isinstance(value, str) and re.fullmatch(r"[0-9]+", value) is not None
    ), "Regional quota counter must be an integer or unsigned decimal string")
    result = int(value)
    require(result >= 0, "Regional quota counter must not be negative")
    return result


def read_receipt(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "Receipt contains duplicate JSON keys")
            result[key] = value
        return result

    content = Path(path).read_bytes()
    require(0 < len(content) <= 1024 * 1024, "Recovery receipt size is invalid")
    return json.loads(content, object_pairs_hook=unique), hashlib.sha256(content).hexdigest()


def validate_pins(pins, *, controllers=False):
    require(isinstance(pins, dict) and bool(pins), "Original controller/PDB pins are missing")
    for key, row in pins.items():
        require(isinstance(key, str), "Original controller/PDB pin key is malformed")
        parts = key.split("/")
        require(len(parts) == (3 if controllers else 2)
                and all(recovery.NAME_RE.fullmatch(part) for part in parts[1 if controllers else 0:])
                and (not controllers or parts[0] in ("Deployment", "ReplicaSet", "DaemonSet", "StatefulSet"))
                and isinstance(row, dict) and set(row) == {"uid", "spec_sha256"}
                and valid_uuid(row["uid"]) and valid_sha(row["spec_sha256"]),
                "Original controller/PDB pin shape is invalid")


def action_times(action, description, *, ambiguous=False):
    require(isinstance(action, dict) and set(action) == {
        "attempted", "accepted", "ambiguous", "requested_at", "returned_at",
    } and action["attempted"] is True and action["ambiguous"] is ambiguous
        and action["accepted"] is (None if ambiguous else True),
        f"{description}: native attempt receipt is not exact")
    start = recovery.timestamp(action["requested_at"], description)
    end = recovery.timestamp(action["returned_at"], description)
    require(start <= end <= datetime.now(timezone.utc), f"{description}: invalid attempt ordering")
    return start, end


def validate_quota_failure(error):
    require(isinstance(error, str), "Prior scale lacks its explicit quota failure")
    command, separator, message = error.partition(": ERROR: ")
    match = re.fullmatch(r"command failed \(exit=[1-9]\d*\): (.+)", command)
    require(separator and match is not None and shlex.split(match[1]) == [
        *SCALE_COMMAND, "--subscription", recovery.SUBSCRIPTION,
    ], "Quota failure is not from this exact one-node native scale command")
    require(message.startswith(
        "(ErrCode_InsufficientVCPUQuota) Insufficient vcpu quota requested 8, remaining 0 "
        f"for family {QUOTA_FAMILY} for region {recovery.REGION}."
    ) and "\nCode: ErrCode_InsufficientVCPUQuota\n" in message,
        "Prior scale error does not explicitly prove this regional family quota rejection")


class CapacityResumeRecovery(replacement.ReplacementRecovery):
    """One quota-gated restoration, then inherited identity/framework postproof."""

    def __init__(self, args, plan, summary, runner, delete_pod):
        super().__init__(args, plan, summary, runner, delete_pod)
        self.native = None
        self.source_hashes = {}
        self.cluster_open = False
        self.guard_uid = None
        self.guard_data = None
        self.continuation = {
            "quota_ready": False, "previous_restore_disambiguation": None,
            "attempt_guard": {
                "name": GUARD_NAME, "namespace": "kube-system",
                "create": {"attempted": False, "accepted": False, "ambiguous": False},
                "retained_owned_non_workload_record": False,
            },
        }
        summary["capacity_resume"] = self.continuation
        summary["quota_ready"] = False

    def load_receipts(self):
        self.accepted, original_sha = read_receipt(self.args.replace_failed_host)
        native, native_sha = read_receipt(self.args.resume_replacement)
        action, marker = recovery.validate_accepted_reimage(self.accepted, self.summary["plan_sha256"])
        metadata = self.accepted["arm_metadata"]
        require(isinstance(metadata.get("pools"), dict) and set(metadata["pools"]) == {"default", "prompool"}
                and all(isinstance(row, dict) and valid_sha(row.get("configuration_sha256"))
                        for row in metadata["pools"].values())
                and set(metadata["instances"]) == set(recovery.REAL_UIDS)
                and all(isinstance(row, dict) for row in metadata["instances"].values()),
                "Accepted reimage lacks the original pool/default VM pins")
        require(isinstance(native, dict) and native.get("execute") is True
                and native.get("mutation_started") is True and native.get("plan_valid") is True
                and native.get("plan_sha256") == self.summary["plan_sha256"]
                and native.get("status") == "failed" and native.get("success") is False
                and native.get("repaired") is False and native.get("phase1_only") is True
                and native.get("workloads_ready") is False,
                "Native receipt is not this plan's failed phase-1 execution")
        require(all(native.get(key) == [] for key in ("pod_moves", "temporary_exclusions", "cleanup_errors"))
                and native.get("restart") == {"attempted": False, "accepted": False, "ambiguous": False}
                and not any(key in native for key in (
                    "replacement_derived_identity", "replacement_derived_manifest", "capacity_resume",
                    "probe", "probe_cleanup_pending", "memory_commitments", "last_capacity_proof",
                    "cilium_proof", "read_only_replica_set_adoptions",
                )), "Native receipt has later-phase activity; continuation must not replay it")
        record = native.get("replacement")
        require(isinstance(record, dict) and set(record) == {
            "accepted_reimage_lineage", "automatic_retry_allowed", "delete", "marker", "marker_write",
            "native_removal", "removal_observation", "replacement_completed", "restore",
        } and record["replacement_completed"] is False and record["automatic_retry_allowed"] is False,
            "Native receipt is incomplete, already continued, or already replaced")
        lineage = {"action": "reimage", "requested_at": action["requested_at"],
                   "marker": marker, "vm_id": recovery.FAILED_PROM_VM_ID}
        require(record["accepted_reimage_lineage"] == lineage, "Native accepted-reimage lineage differs")
        identity = native.get("original_identity")
        expected = {
            "node_name": recovery.PROM_NODE, "node_uid": recovery.REAL_UIDS[recovery.PROM_NODE],
            "provider_id": recovery.PROVIDER, "instance_id": "0", "vm_id": recovery.FAILED_PROM_VM_ID,
            "boot_id": action["previous_boot_id"], "network_container_id": replacement.FAILED_NETWORK_CONTAINER,
        }
        require(isinstance(identity, dict) and set(identity) == {*expected, "nnc_uid", "host_pod_uids"}
                and all(identity[key] == value for key, value in expected.items())
                and valid_uuid(identity["nnc_uid"]), "Native receipt has a different original host identity")
        pods = identity["host_pod_uids"]
        require(isinstance(pods, dict) and 0 < len(pods) <= replacement.ORIGINAL_HOST_PODS
                and all(valid_uuid(key) and isinstance(value, list) and len(value) == 2
                        and all(isinstance(part, str) and recovery.NAME_RE.fullmatch(part) for part in value)
                        for key, value in pods.items())
                and len({tuple(value) for value in pods.values()}) == len(pods)
                and not set(pods) & (set(self.plan["mock_pod_uids"].values())
                                    | {target["pod_uid"] for target in self.targets}),
                "Original host Pod UID inventory is missing or malformed")
        original_marker = canonical(marker)
        native_marker = record["marker"]
        require(isinstance(native_marker, dict) and valid_uuid(native_marker.get("token")),
                "Native deletion marker token is invalid")
        require(native_marker == {
            "schema_version": 1, "owner": recovery.OWNER, "action": "native-failed-host-replacement",
            "token": native_marker["token"], "plan_sha256": self.summary["plan_sha256"],
            "node_uid": recovery.REAL_UIDS[recovery.PROM_NODE], "provider_id": recovery.PROVIDER,
            "vm_id": recovery.FAILED_PROM_VM_ID, "recorded_at": native_marker.get("recorded_at"),
            "accepted_reimage_token": marker["token"], "accepted_reimage_requested_at": action["requested_at"],
            "accepted_reimage_marker_sha256": recovery.digest(original_marker),
        } and recovery.integer(native_marker["schema_version"]),
            "Native deletion marker is not bound to the exact original reimage")
        marked, mark_end = action_times(record["marker_write"], "native marker")
        deleted, delete_end = action_times(record["delete"], "native deletion")
        restored, restore_end = action_times(record["restore"], "previous restore", ambiguous=True)
        removal = record["native_removal"]
        require(isinstance(removal, dict) and removal == {
            "verified_at": removal.get("verified_at"), "pool_count": 0, "vmss_capacity": 0,
            "old_node_pods_nnc_absent": True, "original_marker_removed_by": "native-node-removal",
            "manual_marker_clearance": False,
        } and recovery.integer(removal["pool_count"]) and recovery.integer(removal["vmss_capacity"])
            and removal["old_node_pods_nnc_absent"] is True and removal["manual_marker_clearance"] is False,
            "Native removal lacks exact zero/garbage-collection/no-manual-clearance proof")
        observation = record["removal_observation"]
        require(isinstance(observation, dict) and observation == {
            "observed_at": observation.get("observed_at"), "pool_count": 0,
            "arm_empty": True, "old_resources_absent": True,
        } and recovery.integer(observation["pool_count"])
            and observation["arm_empty"] is True and observation["old_resources_absent"] is True,
            "Prior native removal observation is not complete")
        require(recovery.timestamp(action["requested_at"], "reimage")
                < recovery.timestamp(native.get("started_at"), "native start")
                <= recovery.timestamp(native_marker["recorded_at"], "deletion marker")
                <= marked <= mark_end <= deleted <= delete_end
                <= recovery.timestamp(observation["observed_at"], "removal observation")
                <= recovery.timestamp(removal["verified_at"], "native zero proof")
                <= restored <= restore_end
                <= recovery.timestamp(native.get("finished_at"), "native completion")
                <= datetime.now(timezone.utc), "Native deletion/zero/previous restore ordering is invalid")
        validate_quota_failure(native.get("error"))
        validate_pins(native.get("controller_pins"), controllers=True)
        validate_pins(native.get("pdb_pins"))
        pins = native["controller_pins"]
        require(all(f"DaemonSet/kube-system/{name}" in pins for name in ("cilium", "azure-cns")),
                "Native controller pins lack mandatory Cilium/CNS")
        expected_targets = copy.deepcopy(self.targets)
        for target in expected_targets:
            namespace = target["namespace"]
            deployment = pins.get(f"Deployment/{namespace}/{target['deployment_name']}")
            replica_set = pins.get(f"ReplicaSet/{namespace}/{target['replica_set_name']}")
            require(deployment is not None and replica_set is not None
                    and deployment["uid"] == target.get("deployment_uid", deployment["uid"])
                    and replica_set["uid"] == target["replica_set_uid"],
                    "Native pins do not own the five original framework targets")
            target["deployment_uid"] = deployment["uid"]
        require(len(expected_targets) == 5 and native.get("effective_targets") == expected_targets,
                "Native receipt changed the original five effective targets")
        model = native.get("original_model_pins")
        require(isinstance(model, dict) and set(model) == {"pools", "vmsses", "defaults"}
                and isinstance(model["pools"], dict) and set(model["pools"]) == {"default", "prompool"}
                and isinstance(model["vmsses"], dict)
                and set(model["vmsses"]) == {recovery.DEFAULT_VMSS, recovery.PROM_VMSS}
                and all(valid_sha(value) for value in (*model["pools"].values(), *model["vmsses"].values()))
                and all(model["pools"][name] == metadata["pools"][name]["configuration_sha256"]
                        for name in model["pools"])
                and isinstance(model["defaults"], dict)
                and set(model["defaults"]) == set(recovery.REAL_UIDS) - {recovery.PROM_NODE},
                "Original pool/VMSS/default model pins are malformed")
        for name, row in model["defaults"].items():
            prior = metadata["instances"][name]
            instance = "0" if name == recovery.SOURCE_NODE else "1"
            resource_id = (
                f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{recovery.NODE_GROUP}/"
                f"providers/Microsoft.Compute/virtualMachineScaleSets/{recovery.DEFAULT_VMSS}/"
                f"virtualMachines/{instance}"
            )
            require(isinstance(row, dict) and valid_uuid(row.get("vm_id"))
                    and row["vm_id"] == prior.get("vm_id") and row.get("node_name") == name
                    and row.get("instance_id") == prior.get("instance_id") == instance
                    and recovery.prepared.resource_equal(row.get("resource_id"), prior.get("id", ""))
                    and recovery.prepared.resource_equal(row.get("resource_id"), resource_id)
                    and recovery.prepared.resource_equal(row.get("provider_id"), f"azure://{resource_id}")
                    and row.get("latest_model_applied") is True and row.get("instance_view_initializing") is False
                    and row.get("provisioning_state") == "Succeeded"
                    and row.get("status_codes") == ["PowerState/running", "ProvisioningState/succeeded"]
                    and isinstance(row.get("extensions"), dict) and row["extensions"]
                    and all(isinstance(key, str) and key and value == ["ProvisioningState/succeeded"]
                            for key, value in row["extensions"].items()),
                    "Original default VM identity/health pin is invalid")
        require(len({row["vm_id"] for row in model["defaults"].values()} | {recovery.FAILED_PROM_VM_ID}) == 3,
                "Original VM IDs are not distinct")
        planned = native.get("planned_actions")
        require(isinstance(planned, dict) and planned.get("host_action") == "native-failed-host-replacement"
                and planned.get("machine_names") == [recovery.PROM_NODE] and planned.get("pool") == "prompool"
                and planned.get("pool_counts") == [1, 0, 1] and planned.get("restart_required") is False
                and planned.get("restore_requires_verified_native_removal") is True
                and planned.get("pods") == [{**target, "decision": "delete-pinned"} for target in expected_targets],
                "Native receipt names a different deletion or restoration")
        self.native = native
        self.source_hashes = {
            "accepted_reimage_sha256": original_sha, "native_receipt_sha256": native_sha,
            "plan_file_sha256": hashlib.sha256(Path(self.args.plan_file).read_bytes()).hexdigest(),
        }
        self.continuation.update(source_hashes=dict(self.source_hashes), previous_restore_error=native["error"])
        self.record.update({key: copy.deepcopy(record[key]) for key in (
            "delete", "native_removal", "accepted_reimage_lineage", "marker", "marker_write",
        )})
        self.record["previous_restore"] = copy.deepcopy(record["restore"])
        self.original_marker = original_marker
        self.original_pods, self.original_nnc_uid = copy.deepcopy(pods), identity["nnc_uid"]
        self.model_pin = copy.deepcopy(model)
        self.stage = "empty"
        self.summary.update(original_identity=copy.deepcopy(identity),
                            controller_pins=copy.deepcopy(native["controller_pins"]),
                            pdb_pins=copy.deepcopy(native["pdb_pins"]))
        self.save()

    def check_sources(self):
        for path, key in (
            (self.args.replace_failed_host, "accepted_reimage_sha256"),
            (self.args.resume_replacement, "native_receipt_sha256"),
            (self.args.plan_file, "plan_file_sha256"),
        ):
            require(hashlib.sha256(Path(path).read_bytes()).hexdigest() == self.source_hashes[key],
                    "An immutable original plan or source receipt changed during continuation")

    def fresh_zero(self):
        observation = {"observed_at": recovery.workers.utc_now(), "outcome": "not-proven"}
        self.continuation["fresh_state"] = observation
        self.save()
        self.check_sources()
        self.authority()
        require(self.authority_pin["identities"] == self.native.get("authoritative_identities"),
                "The original full Fleet identity map changed")
        self.models()
        if not self.cluster_open:
            self.open_cluster()
            self.cluster_open = True
        snapshot = self.snapshot()
        require(recovery.frozen_controllers(snapshot) == self.native["controller_pins"]
                and recovery.frozen_pdbs(snapshot) == self.native["pdb_pins"],
                "Original native-run controller/PDB pins changed")
        _, agents = self.guard(snapshot, host_optional=True)
        require(self.stage == "empty" and self.live["empty"] and self.old_resources_absent(snapshot),
                "Previous scale is not disambiguated: requires quiescent zero and native old-resource GC")
        require(self.live["pool"].get("vmSize") == "Standard_D8_v3",
                "Quota gate is only valid for the unchanged eight-core Dv3 pool")
        require(not any(taint.get("key") == recovery.EXCLUSION_KEY
                        for node in snapshot["nodes"]["items"]
                        for taint in recovery.maintenance._taints(node)),
                "A prior/foreign placement exclusion forbids capacity continuation")
        decisions = self.targets_ready_to_plan(snapshot)
        if self.initial is None:
            self.initial = snapshot
            self.continuation["current_default_boot_ids"] = {
                row["metadata"]["name"]: recovery.node_boot(row) for row in snapshot["nodes"]["items"]
                if row["metadata"]["name"] in self.model_pin["defaults"]
            }
            self.continuation["default_boot_evidence_origin"] = "this-continuation-zero-snapshot"
            self.summary["initial_mock_ready"] = sum(recovery.pod_ready(pod) for pod in agents.values())
            self.summary["initial_system_ready"] = False
        observation.update(outcome="quiescent-zero", old_node_pods_nnc_absent=True,
                           pool_count=0, vmss_capacity=0, vm_count=0,
                           operation=copy.deepcopy(self.summary["arm_metadata"]["operation"]))
        self.continuation["previous_restore_disambiguation"] = {
            "outcome": "rejected", "basis": "explicit-quota-error-and-fresh-quiescent-zero",
            "verified_at": recovery.workers.utc_now(),
            "previous_restore": copy.deepcopy(self.record["previous_restore"]),
            "source_native_receipt_sha256": self.source_hashes["native_receipt_sha256"],
        }
        self.save()
        return decisions

    def read_quota(self):
        self.summary["quota_ready"] = self.continuation["quota_ready"] = False
        self.save()
        rows = self.az_json("vm", "list-usage", "--location", recovery.REGION, "--query", USAGE_QUERY)
        require(isinstance(rows, list) and all(isinstance(row, dict) and isinstance(row.get("name"), str)
                                             for row in rows),
                "Regional quota response is malformed")
        rows = [
            {"name": row["name"], "currentValue": quota_counter(row.get("currentValue")),
             "limit": quota_counter(row.get("limit"))}
            for row in rows
        ]
        selected = {}
        for name in (QUOTA_FAMILY, "cores"):
            matches = [row for row in rows if row["name"] == name]
            require(len(matches) == 1, f"Regional quota lacks exactly one {name} counter")
            row = matches[0]
            selected[name] = {**row, "remaining": row["limit"] - row["currentValue"]}
        ready = all(row["remaining"] >= QUOTA_CORES for row in selected.values())
        self.continuation["quota"] = {
            "observed_at": recovery.workers.utc_now(), "region": recovery.REGION,
            "required_cores": QUOTA_CORES, "counters": selected, "ready": ready,
        }
        self.summary["quota_ready"] = self.continuation["quota_ready"] = ready
        self.save()
        return ready

    def guard_objects(self):
        payload = self.kube("get", "configmaps", "-n", "kube-system",
                            "--field-selector", f"metadata.name={GUARD_NAME}", "-o", "json")
        rows = recovery.mocks._items(payload, "exclusive capacity attempt guard")
        require(payload.get("apiVersion") == "v1" and payload.get("kind") in ("List", "ConfigMapList")
                and not (payload.get("metadata") or {}).get("continue") and len(rows) <= 1
                and all(row["metadata"].get("name") == GUARD_NAME
                        and row["metadata"].get("namespace") == "kube-system" for row in rows),
                "Capacity attempt guard absence/identity is ambiguous")
        return rows

    def guard_absent(self):
        require(not self.guard_objects(), "An existing capacity attempt guard forbids another attempt")

    def owned_guard(self, row=None):
        if row is None:
            rows = self.guard_objects()
            require(len(rows) == 1, "Owned capacity guard disappeared")
            row = rows[0]
        meta = row.get("metadata") or {}
        require(row.get("kind") == "ConfigMap" and row.get("apiVersion") == "v1"
                and meta.get("name") == GUARD_NAME and meta.get("namespace") == "kube-system"
                and valid_uuid(self.guard_uid) and meta.get("uid") == self.guard_uid
                and isinstance(meta.get("resourceVersion"), str) and meta["resourceVersion"]
                and not meta.get("deletionTimestamp") and not meta.get("ownerReferences")
                and row.get("data") == self.guard_data,
                "Owned capacity guard UID/token/content changed")
        return row

    def create_guard(self):
        self.guard_absent()
        token = str(uuid.uuid4())
        self.guard_data = {
            "owner": recovery.OWNER, "token": token,
            "record": canonical({
                "schema_version": 1, "action": "quota-gated-prom-capacity-restoration",
                "plan_sha256": self.summary["plan_sha256"], **self.source_hashes,
                "native_delete": self.record["delete"], "native_removal": self.record["native_removal"],
                "previous_restore_disambiguation": self.continuation["previous_restore_disambiguation"],
                "state": "reserved", "restore": copy.deepcopy(self.record["restore"]),
            }),
        }
        audit = self.continuation["attempt_guard"]
        audit.update(token=token, retained_owned_non_workload_record=True)
        receipt = audit["create"]
        require(not receipt["attempted"], "Exclusive guard creation cannot be repeated")
        receipt.update(attempted=True, accepted=None, ambiguous=True, requested_at=recovery.workers.utc_now())
        self.save()
        try:
            output = self.write([
                "kubectl", "create", "configmap", GUARD_NAME, "-n", "kube-system",
                *(f"--from-literal={key}={value}" for key, value in self.guard_data.items()), "-o", "json",
            ])
            row = recovery.workers.parse_json(output, "exclusive capacity guard creation")
            self.guard_uid = recovery.object_uid(row)
            audit["uid"] = self.guard_uid
            self.save()
            self.owned_guard(row)
            self.owned_guard()
            receipt.update(accepted=True, ambiguous=False)
        finally:
            receipt["returned_at"] = recovery.workers.utc_now()
            self.save()

    def patch_guard(self, state):
        current = self.owned_guard()
        record = json.loads(self.guard_data["record"])
        require((state == "attempted" and record["state"] == "reserved")
                or (state == "completed" and record["state"] == "attempted"
                    and self.record["restore"]["accepted"] is True),
                "Capacity guard transition would repeat an attempt")
        record["state"] = state
        record["restore"] = (
            {"attempted": True, "accepted": None, "ambiguous": True}
            if state == "attempted" else copy.deepcopy(self.record["restore"])
        )
        record["recorded_at"] = recovery.workers.utc_now()
        desired = {**self.guard_data, "record": canonical(record)}
        receipt = {"attempted": True, "accepted": None, "ambiguous": True, "state": state}
        self.continuation["attempt_guard"][f"{state}_write"] = receipt
        self.save()
        try:
            self.write(["kubectl", "patch", "configmap", GUARD_NAME, "-n", "kube-system",
                        "--type=json", "-p", json.dumps([
                            {"op": "test", "path": "/metadata/uid", "value": self.guard_uid},
                            {"op": "test", "path": "/metadata/resourceVersion",
                             "value": current["metadata"]["resourceVersion"]},
                            {"op": "test", "path": "/data/token", "value": self.guard_data["token"]},
                            {"op": "test", "path": "/data", "value": self.guard_data},
                            {"op": "add", "path": "/data", "value": desired},
                        ])])
            self.guard_data = desired
            self.owned_guard()
            receipt.update(accepted=True, ambiguous=False)
        finally:
            self.save()

    def instance(self, vm, vmss_name, *, original=False, default=False):
        learn = not default and self.extension_names is None
        result = super().instance(vm, vmss_name, original=original, default=default)
        if learn and vm.get("provisioningState") in ("Creating", "Updating"):
            # The old receipt has no prom guest-extension inventory. Do not pin an
            # incomplete Creating list; Succeeded still requires every real status.
            self.extension_names = None
        return result

    def system_ready(self, snapshot):
        require(recovery.frozen_controllers(snapshot) == self.native["controller_pins"],
                "Pinned system controller UID/spec changed")
        hosts = [row for row in snapshot["nodes"]["items"] if row["metadata"]["name"] == self.host_node]
        if not hosts:
            return False
        host = hosts[0]
        expected = {}
        for name in sorted(recovery.HOST_DAEMONSETS):
            key = f"DaemonSet/kube-system/{name}"
            if key not in self.native["controller_pins"]:
                continue
            pin = self.native["controller_pins"][key]
            daemonset = recovery.controller(snapshot, "DaemonSet", "kube-system", name, pin["uid"])
            template = (daemonset["spec"].get("template") or {}).get("spec")
            require(isinstance(template, dict) and bool(template.get("containers")),
                    "Pinned system DaemonSet template is malformed")
            if recovery.mocks._node_matches_pod_template(host, template):
                expected[name] = pin["uid"]
        require(set(expected) >= {"cilium", "azure-cns"}, "Cilium/CNS must both apply to the genuine new host")
        actual = {name: [] for name in expected}
        for pod in snapshot["pods"]["items"]:
            if pod["spec"].get("nodeName") != self.host_node:
                continue
            owners = pod["metadata"].get("ownerReferences") or []
            if not any(row.get("controller") is True and row.get("kind") == "DaemonSet" for row in owners):
                continue
            owner = recovery.controller_owner(pod, "DaemonSet")
            require(pod["metadata"].get("namespace") == "kube-system"
                    and expected.get(owner["name"]) == owner["uid"] and valid_uuid(recovery.object_uid(pod)),
                    "New-host system Pod has foreign or inapplicable pinned DaemonSet ownership")
            if not pod["metadata"].get("deletionTimestamp"):
                actual[owner["name"]].append(pod)
        self.continuation["system_daemonsets"] = {
            name: {"controller_uid": row_uid, "ready": len(actual[name]) == 1 and recovery.pod_ready(actual[name][0])}
            for name, row_uid in expected.items()
        }
        self.save()
        return all(row["ready"] for row in self.continuation["system_daemonsets"].values())

    def restore(self):
        self.fresh_zero()
        require(self.read_quota(), "Quota headroom disappeared before exclusive guard creation")
        self.create_guard()
        self.fresh_zero()
        require(self.read_quota(), "Quota headroom disappeared before journaling the attempt")
        self.patch_guard("attempted")
        self.fresh_zero()
        require(self.read_quota(), "Quota headroom disappeared before the sole native restore")
        self.owned_guard()
        self.summary["status"] = "restoring-prom-capacity"
        self.submit("restore", list(SCALE_COMMAND))
        self.stage = "restoring"
        deadline = min(self.work_deadline, time.monotonic() + replacement.RESTORE_SECONDS)
        while True:
            self.check_sources()
            self.owned_guard()
            self.authority()
            stable = self.models()
            snapshot = self.snapshot()
            if self.discover(snapshot, stable):
                self.record.update(replacement_completed=True, completed_at=recovery.workers.utc_now())
                self.summary["restart"].update(
                    skipped_reason="replaced-not-restarted", host_proven=True,
                    current_boot_id=self.derived["boot_id"], requested_at=self.record["restore"]["requested_at"],
                )
                self.save()
                return
            self.wait(deadline, "Capacity restoration Node, initialized NC, and all applicable system DaemonSets")

    def execute(self):
        self.load_receipts()
        decisions = self.fresh_zero()
        self.guard_absent()
        self.read_quota()
        self.summary.update(
            plan_valid=True, effective_targets=self.targets,
            planned_actions={
                "host_action": "quota-gated-native-capacity-continuation", "restart_required": False,
                "pool": "prompool", "pool_counts": [0, 1], "delete_required": False, "pods": decisions,
                "durable_guard": GUARD_NAME, "source_native_receipt_sha256": self.source_hashes["native_receipt_sha256"],
            },
            capacity_and_actual_ip_proof_required_at_execution=True,
        )
        self.save()
        if not self.args.execute:
            self.summary["status"] = "plan_valid"
            return
        deadline = min(self.work_deadline, time.monotonic() + self.args.quota_wait_seconds)
        while not self.summary["quota_ready"]:
            self.summary["status"] = "waiting-for-capacity-quota"
            self.save()
            self.wait(deadline, "Regional Dv3 and total-core quota")
            self.fresh_zero()
            self.guard_absent()
            self.read_quota()
        self.restore()
        for target in self.targets:
            self.check_sources()
            self.owned_guard()
            self.move_pod(target)
        self.cleanup()
        require(not self.summary["cleanup_errors"], "Cleanup failed; capacity continuation cannot be certified")
        self.postproof()
        self.check_sources()
        self.patch_guard("completed")
        self.summary.update(repaired=True, status="repaired")
