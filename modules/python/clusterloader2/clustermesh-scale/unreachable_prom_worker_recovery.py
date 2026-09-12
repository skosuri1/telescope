#!/usr/bin/env python3
"""Recover only the approved mesh-96 system host and explicitly pinned Pods.

The default mode proves a plan without changing Azure or Kubernetes resources.
Failed-host replacement requires a separate accepted-action checkpoint.
This is not mock-agent recovery or a workload-ready gate.
"""

# pylint: disable=too-many-lines,protected-access,too-many-boolean-expressions

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import shutil
import signal
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

import cni_worker_maintenance as maintenance
import mock_cni_recovery as mocks
import prepared_worker_retirement as prepared
import preserved_aks_arm_reconcile as arm
import preserved_worker_reconcile as workers


SUBSCRIPTION = "37deca37-c375-4a14-b90a-043849bd2bf1"
RESOURCE_GROUP = "78751-f36f3d5a"
REGION = "eastus2euap"
ROLE = "mesh-96"
CLUSTER = "clustermesh-96"
NODE_GROUP = f"mc_{RESOURCE_GROUP}_{CLUSTER}_{REGION}"
PROM_VMSS = "aks-prompool-38822163-vmss"
DEFAULT_VMSS = "aks-default-28928250-vmss"
PROM_NODE = f"{PROM_VMSS}000000"
SOURCE_NODE = f"{DEFAULT_VMSS}000000"
REAL_UIDS = {
    PROM_NODE: "9d8a9811-0e9a-4ff5-9a94-db89c42d223b",
    SOURCE_NODE: "6e4ad0c4-1cde-451a-966b-aefc04ca59d4",
    f"{DEFAULT_VMSS}000001": "c673a142-17ac-44c7-92cc-32efc0d34c61",
}
SOURCE_NC = "ffd9a896-50c2-4070-97e8-040d4c06361f"
FAILED_PROM_VM_ID = "d731b838-501d-438d-a087-fd4545f1d607"
OS_FAILURE_CODE = "ProvisioningState/failed/OSProvisioningClientError"
PROVIDER = (
    f"azure:///subscriptions/{SUBSCRIPTION}/resourceGroups/{NODE_GROUP}/"
    f"providers/Microsoft.Compute/virtualMachineScaleSets/{PROM_VMSS}/virtualMachines/0"
)
MARKER_KEY = "mock-clustermesh/unreachable-prom-restart"
EXCLUSION_KEY = "mock-clustermesh/unreachable-prom-exclusion"
PROBE_KEY = "mock-clustermesh/unreachable-prom-probe"
OWNER = "unreachable-prom-worker-recovery"
API_MEMORY_RESERVE = 8 * 1024**3
FRAMEWORK_MEMORY_RESERVE = 1024**3
MAX_PLAN_BYTES = 32768
DNS_DEPLOYMENT_UID = "7f531432-3379-4ec5-a5b0-6557965c2cdd"
DNS_REPLICA_SET = "coredns-5d474ff6db"
DNS_REPLICA_SET_UID = "357dff70-eb1c-4b5c-960f-b895b5f88f58"
DNS_REPLICAS = 5
APPROVED_FRAMEWORKS = (
    {
        "namespace": "kube-system", "pod_name": "coredns-5d474ff6db-4czw6",
        "pod_uid": "fe94a626-f606-4226-8be6-d996b5ad6147",
        "replica_set_name": DNS_REPLICA_SET, "replica_set_uid": DNS_REPLICA_SET_UID,
        "deployment_name": "coredns", "deployment_uid": DNS_DEPLOYMENT_UID,
    },
    {
        "namespace": "kube-system", "pod_name": "coredns-5d474ff6db-fzvm6",
        "pod_uid": "f33d4987-292a-4851-9f3c-6c710d55ef8e",
        "replica_set_name": DNS_REPLICA_SET, "replica_set_uid": DNS_REPLICA_SET_UID,
        "deployment_name": "coredns", "deployment_uid": DNS_DEPLOYMENT_UID,
    },
    {
        "namespace": "kube-state-metrics-perf-test", "pod_name": "kube-state-metrics-675ff6485d-nnk55",
        "pod_uid": "02ba0706-7ae6-4271-b2c1-9d4f711b887b",
        "replica_set_name": "kube-state-metrics-675ff6485d",
        "replica_set_uid": "feb97848-3690-44c5-89b8-611fa9d6899b", "deployment_name": "kube-state-metrics",
    },
    {
        "namespace": "monitoring", "pod_name": "grafana-6df78447fb-xdcsl",
        "pod_uid": "622c2703-b855-4411-a8e9-705b129ab766",
        "replica_set_name": "grafana-6df78447fb", "replica_set_uid": "677535b6-2f43-4af0-ba4f-30c45f7c87da",
        "deployment_name": "grafana",
    },
)
CLEANUP_SECONDS = 120
RESTART_SECONDS = 900
POD_READY_SECONDS = 180
POSTPROOF_SECONDS = 300
POLL_SECONDS = 10
AUTH_ERROR = re.compile(r"unauthorized|forbidden|authorizationfailed|authentication|AADSTS|\b(?:401|403)\b", re.I)
NAME_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?$")
EXPECTED_ERRORS = (workers.ReconcileError, mocks.RecoveryError, arm.ReconcileError, OSError)
HOST_DEPLOYMENTS = maintenance.KNOWN_ALLOWED_DEPLOYMENTS | {
    ("kube-system", "clustermesh-apiserver"),
    ("monitoring", "grafana"),
    ("monitoring", "prometheus-operator"),
}
HOST_DAEMONSETS = {
    "acns-security-agent", "ama-metrics-node", "azure-cns",
    "azuresecuritylinuxagent", "cilium", "cloud-node-manager",
    "csi-azuredisk-node", "csi-azurefile-node",
}
CLUSTER_QUERY = (
    "[].{id:id,name:name,location:location,tags:tags,"
    "nodeResourceGroup:nodeResourceGroup,provisioningState:provisioningState,powerState:powerState}"
)
VMSS_QUERY = (
    "[].{id:id,name:name,location:location,sku:sku,"
    "provisioningState:provisioningState,tags:tags,orchestrationMode:orchestrationMode}"
)
VM_QUERY = (
    "[].{id:id,name:name,instanceId:instanceId,provisioningState:provisioningState,"
    "computerName:osProfile.computerName,latestModelApplied:latestModelApplied,vmId:vmId}"
)
VIEW_QUERY = "{statuses:statuses,extensions:extensions[].{name:name,statuses:statuses}}"
SCALE_VIEW_QUERY = "{statuses:statuses,virtualMachines:virtualMachine.statusesSummary}"
OPERATION_QUERY = (
    "{name:name,status:status,operationType:operationType,startTime:startTime,"
    "endTime:endTime,errorCode:error.code}"
)


require = prepared.require


def integer(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def timestamp(value, description: str) -> datetime:
    try:
        text = str(value).replace("Z", "+00:00")
        # Azure's 100ns fractions need normalization for the pipeline's Python 3.10.
        text = re.sub(
            r"(\d{2}:\d{2}:\d{2}\.)(\d+)(?=[+-]\d{2}:\d{2}$)",
            lambda match: match[1] + match[2][:6].ljust(6, "0"),
            text,
        )
        parsed = datetime.fromisoformat(text)
        require(parsed.tzinfo is not None, f"{description}: timezone is missing")
        return parsed
    except (ValueError, TypeError) as error:
        raise workers.ReconcileError(f"{description}: invalid timestamp") from error


def object_uid(row: dict) -> str:
    return str((row.get("metadata") or {}).get("uid") or "")


def pod_ready(pod: dict) -> bool:
    return (
        mocks._pod_ready(pod)
        and maintenance._readiness_condition_true(pod)
        and not (pod.get("metadata") or {}).get("deletionTimestamp")
        and bool((pod.get("status") or {}).get("podIP"))
    )


def pvc_free(spec: dict) -> bool:
    volumes = spec.get("volumes") or []
    require(isinstance(volumes, list) and all(isinstance(row, dict) for row in volumes),
            "Pod volumes are malformed")
    return not any("persistentVolumeClaim" in row or "ephemeral" in row for row in volumes)


def controller_owner(row: dict, kind: str) -> dict:
    owners = (row.get("metadata") or {}).get("ownerReferences") or []
    matches = [owner for owner in owners if isinstance(owner, dict) and owner.get("controller") is True]
    require(
        len(matches) == 1 and matches[0].get("kind") == kind
        and matches[0].get("name") and matches[0].get("uid"),
        f"{(row.get('metadata') or {}).get('name')}: controller ownership is not exact",
    )
    return matches[0]


def uid_map(value, names, description: str) -> dict:
    require(isinstance(value, dict) and set(value) == set(names), f"{description}: names are not exact")
    require(
        all(isinstance(uid, str) and maintenance.UUID_RE.fullmatch(uid) for uid in value.values())
        and len(set(value.values())) == len(value),
        f"{description}: UIDs are not exact",
    )
    return value


def validate_plan(payload) -> dict:
    """Reject unbounded selections and plans for any host outside this approval."""

    required = {
        "schema_version", "role", "node_name", "node_uid", "provider_id",
        "api_pod_name", "api_pod_uid", "api_replica_set_name", "api_replica_set_uid",
        "api_deployment_uid", "mock_controller_uid", "mock_pod_uids",
        "ready_mock_pod_uids", "kwok_node_uids", "real_node_uids", "cni_source",
    }
    require(isinstance(payload, dict) and required <= set(payload)
            and set(payload) <= required | {"framework_pods"}, "Plan fields are not exact")
    require(integer(payload["schema_version"]) and payload["schema_version"] == 1,
            "Only plan schema_version 1 is supported")
    require(
        payload["role"] == ROLE and payload["node_name"] == PROM_NODE
        and payload["node_uid"] == REAL_UIDS[PROM_NODE]
        and prepared.resource_equal(payload["provider_id"], PROVIDER),
        "Plan does not pin the approved mesh-96 prompool host",
    )
    require(payload["real_node_uids"] == REAL_UIDS, "Original three real Node UIDs must be exact")
    require(payload["cni_source"] == {
        "node_name": SOURCE_NODE, "node_uid": REAL_UIDS[SOURCE_NODE], "network_container_id": SOURCE_NC,
    }, "CNI source identity must remain pinned; this helper never repairs that worker")
    names = maintenance.EXPECTED_AGENT_NAMES
    uid_map(payload["mock_pod_uids"], names, "Mock Pods")
    uid_map(payload["kwok_node_uids"], names, "KWOK Nodes")
    ready = payload["ready_mock_pod_uids"]
    require(isinstance(ready, dict) and len(ready) == 71 and set(ready) <= names,
            "The original 71 healthy mock identities are required")
    require(all(payload["mock_pod_uids"][name] == uid for name, uid in ready.items()),
            "Healthy mock UIDs disagree with the original inventory")
    for key in ("api_pod_uid", "api_replica_set_uid", "api_deployment_uid", "mock_controller_uid"):
        require(isinstance(payload[key], str) and maintenance.UUID_RE.fullmatch(payload[key]),
                f"{key} must be a UUID")
    require(
        payload["api_pod_name"] == "clustermesh-apiserver-75c9b44965-wm84r"
        and payload["api_pod_uid"] == "d1548ec6-ad38-483b-b395-9999b87898d4"
        and payload["api_replica_set_name"] == "clustermesh-apiserver-75c9b44965"
        and payload["api_replica_set_uid"] == "67b9481a-bb66-46de-8c9a-d64d122e16a1"
        and payload["api_deployment_uid"] == "6fc6e6e0-e212-4b25-86a6-971d04e00797",
        "The original ClusterMesh API Pod and controllers must remain pinned",
    )
    frameworks = payload.get("framework_pods", [])
    require(isinstance(frameworks, list) and len(frameworks) <= 4,
            "At most five Pod recoveries, including the API, are authorized")
    seen = {("kube-system", payload["api_pod_name"])}
    seen_uids = {payload["api_pod_uid"]}
    fields = {"namespace", "pod_name", "pod_uid", "replica_set_name", "replica_set_uid", "deployment_name"}
    for row in frameworks:
        require(isinstance(row, dict) and fields <= set(row)
                and set(row) <= fields | {"deployment_uid"}, "Framework Pod fields are not exact")
        require(all(isinstance(row[key], str) and NAME_RE.fullmatch(row[key])
                    for key in ("namespace", "pod_name", "replica_set_name", "deployment_name")),
                "Framework names are invalid")
        approved = next((entry for entry in APPROVED_FRAMEWORKS
                         if (entry["namespace"], entry["pod_name"]) == (row["namespace"], row["pod_name"])), None)
        require(approved is not None and all(row[key] == approved[key] for key in fields),
                "Framework Deployment is not explicitly supported")
        require("deployment_uid" not in approved or row.get("deployment_uid", approved["deployment_uid"])
                == approved["deployment_uid"], "Pinned CoreDNS Deployment UID changed")
        for key in ("pod_uid", "replica_set_uid", "deployment_uid"):
            if key in row:
                require(isinstance(row[key], str) and maintenance.UUID_RE.fullmatch(row[key]),
                        f"Framework {key} must be a UUID")
        identity = row["namespace"], row["pod_name"]
        require(identity not in seen and row["pod_uid"] not in seen_uids, "Duplicate Pod recovery")
        seen.add(identity)
        seen_uids.add(row["pod_uid"])
    dns = [row for row in frameworks if row["deployment_name"] == "coredns"]
    require(len(dns) in (0, 2), "Both explicitly pinned CoreDNS Pods must be selected together")
    require(len(json.dumps(payload, separators=(",", ":")).encode("utf-8")) <= MAX_PLAN_BYTES,
            "Serialized recovery plan exceeds 32768 bytes")
    return copy.deepcopy(payload)


def load_plan(path: str) -> dict:
    def unique_keys(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"Duplicate JSON field: {key}")
            result[key] = value
        return result

    with open(path, "rb") as handle:
        serialized = handle.read(MAX_PLAN_BYTES + 2)
    # The pipeline writes the bounded JSON value with one printf-added newline.
    if serialized.endswith(b"\n"):
        serialized = serialized[:-1]
    require(0 < len(serialized) <= MAX_PLAN_BYTES, "Serialized recovery plan must be 1..32768 bytes")
    return validate_plan(json.loads(serialized.decode("utf-8"), object_pairs_hook=unique_keys))


def validate_paths(args) -> None:
    require(Path(args.plan_file).resolve() != Path(args.summary_file).resolve(),
            "Plan and summary must be different files")
    for name in ("observe_accepted_action", "replace_failed_host", "resume_replacement"):
        checkpoint = getattr(args, name, None)
        require(not checkpoint or Path(checkpoint).resolve() not in {
            Path(args.plan_file).resolve(), Path(args.summary_file).resolve(),
        }, "Accepted-action checkpoint must differ from the plan and summary")


def validate_args(args) -> None:
    require(
        args.resource_group == args.confirm_resource_group == RESOURCE_GROUP
        and args.expected_subscription.lower() == SUBSCRIPTION
        and args.expected_region.lower() == REGION,
        "Only the explicitly approved preserved subscription/RG/region is supported",
    )
    require(maintenance.SHA256_RE.fullmatch(args.expected_tfvars_sha) is not None,
            "The preserved tfvars SHA256 must be exact")
    require(integer(args.timeout_seconds) and 0 < args.timeout_seconds <= 3600,
            "timeout-seconds must be between 1 and 3600")
    validate_paths(args)
    require(not getattr(args, "observe_accepted_action", None) or not args.execute,
            "Accepted-action observation cannot be combined with --execute")
    require(not getattr(args, "replace_failed_host", None) or not (
        getattr(args, "observe_accepted_action", None) or getattr(args, "reimage_failed_os", False)
    ), "Failed-host replacement cannot be combined with observation or another reimage")
    require(not getattr(args, "resume_replacement", None) or bool(getattr(args, "replace_failed_host", None)),
            "Capacity continuation requires the original accepted reimage lineage")
    quota_wait = getattr(args, "quota_wait_seconds", 900)
    require(integer(quota_wait) and 0 <= quota_wait <= 900, "Quota observation is bounded to 0..900 seconds")


def validate_accepted_reimage(prior, plan_sha256):
    """Bind subsequent recovery to the original accepted action, not a retry."""

    require(isinstance(prior, dict), "Accepted-action checkpoint must be an object")
    action = prior.get("restart") or {}
    require(isinstance(action, dict), "Accepted-action restart receipt is malformed")
    marker = action.get("marker") or {}
    require(isinstance(marker, dict), "Accepted-action marker is malformed")
    require(
        prior.get("execute") is True and prior.get("mutation_started") is True
        and prior.get("plan_sha256") == plan_sha256
        and action.get("action") == "reimage" and action.get("attempted") is True
        and action.get("accepted") is True and action.get("ambiguous") is False
        and marker.get("owner") == OWNER
        and integer(marker.get("schema_version")) and marker["schema_version"] == 1
        and marker.get("action") == "single-instance-reimage"
        and marker.get("node_uid") == REAL_UIDS[PROM_NODE]
        and marker.get("provider_id") == PROVIDER
        and marker.get("plan_sha256") == plan_sha256
        and isinstance(marker.get("token"), str) and maintenance.UUID_RE.fullmatch(marker["token"])
        and action.get("previous_boot_id") == marker.get("previous_boot_id")
        and bool(marker.get("previous_boot_id")),
        "Accepted-action checkpoint does not prove this exact owned reimage",
    )
    requested = timestamp(action.get("requested_at"), "accepted reimage request")
    require(timestamp(marker.get("recorded_at"), "owned reimage marker") <= requested
            <= datetime.now(timezone.utc), "Accepted-action timestamps are invalid")
    metadata = prior.get("arm_metadata")
    require(isinstance(metadata, dict) and isinstance(metadata.get("instances"), dict),
            "Accepted-action VM inventory is malformed")
    original_instances = metadata["instances"]
    original_vm = original_instances.get(PROM_NODE)
    require(
        isinstance(original_vm, dict) and original_vm.get("vm_id") == FAILED_PROM_VM_ID
        and original_vm.get("instance_id") == "0",
        "Accepted-action checkpoint has a different VM identity",
    )
    return action, marker


def frozen_controllers(snapshot: dict) -> dict:
    result = {}
    for row in mocks._items(snapshot["controllers"], "controllers"):
        meta = row.get("metadata") or {}
        key = f"{row.get('kind')}/{meta.get('namespace')}/{meta.get('name')}"
        require(key not in result and object_uid(row) and isinstance(row.get("spec"), dict),
                "Controller identity/spec inventory is ambiguous")
        result[key] = {"uid": object_uid(row), "spec_sha256": digest(row["spec"])}
    return result


def frozen_pdbs(snapshot: dict) -> dict:
    result = {}
    for row in mocks._items(snapshot["pdbs"], "PodDisruptionBudgets"):
        meta = row.get("metadata") or {}
        key = f"{meta.get('namespace')}/{meta.get('name')}"
        require(key not in result and object_uid(row), "PDB identity is ambiguous")
        result[key] = {"uid": object_uid(row), "spec_sha256": digest(row.get("spec"))}
    return result


def controller(snapshot: dict, kind: str, namespace: str, name: str, uid: str = "") -> dict:
    rows = [
        row for row in mocks._items(snapshot["controllers"], "controllers")
        if row.get("kind") == kind and (row.get("metadata") or {}).get("namespace") == namespace
        and (row.get("metadata") or {}).get("name") == name
    ]
    require(len(rows) == 1 and (not uid or object_uid(rows[0]) == uid)
            and not rows[0]["metadata"].get("deletionTimestamp"),
            f"{namespace}/{name}: live {kind} ownership changed")
    return rows[0]


def node_boot(node: dict) -> str:
    boot = ((node.get("status") or {}).get("nodeInfo") or {}).get("bootID")
    require(isinstance(boot, str) and bool(boot), "The pinned Node bootID is missing")
    return boot


def target_key(target):
    return target["namespace"], target["pod_name"]


def memory_reserve(target, template):
    """Use real template bounds, reserving 8Gi for each unbounded API/Grafana."""

    _, requested = mocks._resource_requests({"spec": template})
    limits = copy.deepcopy(template)
    containers = [*limits.get("containers", []), *limits.get("initContainers", [])]
    fully_bounded = bool(containers)
    for row in containers:
        resources = row.setdefault("resources", {})
        resources["requests"] = resources.get("limits") or {}
        fully_bounded = fully_bounded and int(
            mocks._quantity(resources["requests"].get("memory"), "container memory limit")
        ) > 0
    if "resources" in limits:
        limits["resources"]["requests"] = limits["resources"].get("limits") or {}
    _, bounded = mocks._resource_requests({"spec": limits})
    minimum = 0 if fully_bounded else FRAMEWORK_MEMORY_RESERVE
    if (target["namespace"], target["deployment_name"]) in {
        ("kube-system", "clustermesh-apiserver"), ("monitoring", "grafana"),
    }:
        minimum = API_MEMORY_RESERVE
    return max(requested, bounded, minimum)


def prove_unreachable(node: dict) -> None:
    conditions = [row for row in (node.get("status") or {}).get("conditions", [])
                  if isinstance(row, dict) and row.get("type") == "Ready"]
    require(len(conditions) == 1 and conditions[0].get("status") in ("Unknown", "False"),
            "Restart requires an explicitly NotReady/Unknown Node")
    condition = conditions[0]
    now = datetime.now(timezone.utc)
    require(
        condition.get("reason") in ("NodeStatusUnknown", "KubeletNotReady")
        and (now - timestamp(condition.get("lastTransitionTime"), "Ready transition")).total_seconds() >= 300
        and (now - timestamp(condition.get("lastHeartbeatTime"), "kubelet heartbeat")).total_seconds() >= 300,
        "The host is not persistently unreachable for at least five minutes",
    )


def never_started(pod: dict) -> None:
    status = pod.get("status") or {}
    require(
        status.get("phase") == "Pending" and not status.get("podIP") and not status.get("podIPs")
        and not (pod.get("metadata") or {}).get("deletionTimestamp")
        and not maintenance._readiness_condition_true(pod),
        "Only a nonterminating Pending Pod without any Pod IP can be deleted",
    )
    for key in ("containerStatuses", "initContainerStatuses", "ephemeralContainerStatuses"):
        rows = status.get(key) or []
        require(isinstance(rows, list), "Pod container statuses are malformed")
        for row in rows:
            require(
                isinstance(row, dict) and not row.get("containerID") and not row.get("started")
                and not row.get("ready") and row.get("restartCount", 0) == 0
                and not (row.get("state") or {}).get("running")
                and not (row.get("state") or {}).get("terminated")
                and not (row.get("lastState") or {}),
                "The pinned failed Pod has evidence of a container start",
            )


class Recovery(maintenance.ClusterOperator):
    """One subscription/context, one restart request, and explicit Pod UID deletes."""

    def __init__(self, args, plan, summary, runner, delete_pod):
        deadline = time.monotonic() + args.timeout_seconds
        super().__init__(args, CLUSTER, runner,
                         deadline - min(CLEANUP_SECONDS, args.timeout_seconds // 4) if args.execute else deadline,
                         deadline)
        self.plan = plan
        self.summary = summary
        self.delete_pod = delete_pod
        self.initial = None
        self.model_pin = None
        self.authority_pin = None
        self.identities = []
        self.bindings = {}
        self.groups = {}
        self.ready_targets = {}
        self.dns_pins = {}
        self.reservations = set()
        self.memory_baseline = 0
        self.memory_observed = False
        self.reserved_memory = 0
        self.reserved_cpu = 0
        self.marker = ""
        self.probe = None
        self.exclusions = []
        self.cleanup_done = False
        self.host_action = "reimage" if getattr(args, "reimage_failed_os", False) else "restart"
        self.host_node = PROM_NODE
        self.host_provider_id = PROVIDER
        self.real_uids = dict(REAL_UIDS)
        self.system_origin_node = PROM_NODE
        self.targets = [{
            "namespace": "kube-system", "pod_name": plan["api_pod_name"], "pod_uid": plan["api_pod_uid"],
            "replica_set_name": plan["api_replica_set_name"], "replica_set_uid": plan["api_replica_set_uid"],
            "deployment_name": "clustermesh-apiserver", "deployment_uid": plan["api_deployment_uid"],
        }, *copy.deepcopy(plan.get("framework_pods", []))]
        priority = {"coredns": 0, "clustermesh-apiserver": 1, "kube-state-metrics": 2, "grafana": 3}
        self.targets.sort(key=lambda target: (priority[target["deployment_name"]], target["pod_name"]))

    def save(self):
        mocks.write_json_atomic(self.args.summary_file, self.summary)

    def run(self, command, timeout_seconds=45, *, cleanup=False):
        """Retry only transient reads; authorization failures are never retried."""

        command = list(command)
        if command[0] == "az":
            allowed = command[1:3] in (
                ["account", "show"], ["group", "show"], ["aks", "list"],
                ["vmss", "list"], ["vmss", "list-instances"], ["vmss", "get-instance-view"],
                ["vm", "list-usage"],
            ) or command[1:4] in (
                ["fleet", "member", "list"], ["aks", "nodepool", "list"],
                ["aks", "operation", "show-latest"],
            )
        else:
            require(command[0] == "kubectl", "Unsupported read executable")
            allowed = "get" in command and not any(word in command for word in ("patch", "delete", "run"))
            if "exec" in command:
                allowed = "--" in command and command[command.index("--") + 1:] == [
                    "cilium-dbg", "status", "-o", "json",
                ]
        require(allowed, "A mutation was passed to the read-only command path")

        invoke = super().run

        def once(arguments, timeout):
            try:
                return invoke(
                    arguments, timeout, cleanup=cleanup or self.cleanup_mode,
                )
            except workers.ReconcileError as error:
                if AUTH_ERROR.search(str(error)):
                    raise
                raise arm.ReconcileError(str(error)) from error

        try:
            return arm.run_read_with_retries(
                command, once, timeout_seconds=timeout_seconds, attempts=3, retry_seconds=2,
            )
        except arm.ReconcileError as error:
            raise workers.ReconcileError(str(error)) from error

    def write(self, command, *, cleanup=False):
        require(self.args.execute, "Read-only plans must never mutate resources")
        self.summary["mutation_started"] = True
        self.save()
        return super().run(command, 45, cleanup=cleanup or self.cleanup_mode)

    def kube(self, *command):
        output = self.run(["kubectl", "--request-timeout=45s", *command])
        return workers.parse_json(output, "recovery Kubernetes read")

    def open_cluster(self):
        super().run([
            "az", "aks", "get-credentials", "--resource-group", RESOURCE_GROUP, "--name", CLUSTER,
            "--file", self.args.kubeconfig, "--context", CLUSTER, "--only-show-errors",
        ], 45)
        require(Path(self.args.kubeconfig).is_file() and Path(self.args.kubeconfig).stat().st_size > 0,
                "Private selected-cluster kubeconfig was not produced")
        Path(self.args.kubeconfig).chmod(0o600)

    def authority(self, *, strict=False):
        account = self.az_json("account", "show", "--query", "{id:id}")
        require(str(account.get("id", "")).lower() == SUBSCRIPTION, "Current Azure subscription changed")
        group = self.az_json("group", "show", "--name", RESOURCE_GROUP)
        clusters = self.az_json("aks", "list", "--resource-group", RESOURCE_GROUP, "--query", CLUSTER_QUERY)
        members = self.az_json(
            "fleet", "member", "list", "--resource-group", RESOURCE_GROUP, "--fleet-name", "clustermesh-flt",
        )
        connected = True
        try:
            selected, identities = prepared.validate_scope(self.args, group, clusters, members)
        except prepared.FleetNotConnected as error:
            require(not strict and len(error.members) == 1 and error.members[0]["name"] == ROLE
                    and error.members[0]["meshProperties"]["status"]["state"] == "Failed"
                    and (error.members[0]["meshProperties"]["status"].get("error") or {}).get("code")
                    == "ConnectivityTimeout", "Only the original mesh-96 Failed/ConnectivityTimeout is permitted")
            identities = error.identities
            selected = next(row for row in clusters if row["tags"]["role"] == ROLE)
            connected = False
        require(all(row.get("provisioningState") == "Succeeded"
                    and (row.get("powerState") or {}).get("code") == "Running" for row in clusters),
                "All preserved AKS resources must remain Running/Succeeded")
        require(selected["name"] == CLUSTER and selected["nodeResourceGroup"].lower() == NODE_GROUP,
                "Selected cluster or node resource group differs from the approved host")
        node_group = self.az_json("group", "show", "--name", NODE_GROUP)
        require(
            prepared.resource_equal(node_group.get("id"),
                                    f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{NODE_GROUP}")
            and prepared.resource_equal(node_group.get("managedBy"), selected["id"])
            and str(node_group.get("location", "")).lower() == REGION,
            "Node resource-group identity, managedBy, or region changed",
        )
        prepared.require_lease(node_group, self.args.timeout_seconds)
        pin = {
            "clusters": {row["tags"]["role"]: row["id"].lower() for row in clusters},
            "identities": sorted(identities, key=lambda row: row["role"]),
        }
        require(self.authority_pin is None or pin == self.authority_pin, "Preserved/Fleet identity map drifted")
        self.authority_pin = pin
        self.identities = identities
        self.summary["authoritative_identities"] = pin["identities"]
        self.summary["fleet_connected"] = connected
        self.summary["lease_checked_at"] = workers.utc_now()
        self.save()
        return selected, connected

    def models(self, *, restarting=False, allow_failed_os=False):
        operation = self.az_json(
            "aks", "operation", "show-latest", "--resource-group", RESOURCE_GROUP,
            "--name", CLUSTER, "--query", OPERATION_QUERY,
        )
        evidence = {"operation": operation, "pools": {}, "vmsses": {}, "instances": {}}
        self.summary["arm_metadata"] = evidence
        self.save()
        require(isinstance(operation, dict) and operation.get("status") == "Succeeded"
                and operation.get("name") and operation.get("endTime") and not operation.get("errorCode"),
                "Latest AKS provider operation is failed, busy, absent, or ambiguous")
        operation_end = timestamp(operation["endTime"], "AKS operation completion")
        require(timestamp(operation.get("startTime"), "AKS operation start") <= operation_end
                and (operation_end - datetime.now(timezone.utc)).total_seconds() <= 30,
                "Latest AKS operation timestamps are ambiguous")
        pools = self.az_json("aks", "nodepool", "list", "--resource-group", RESOURCE_GROUP,
                             "--cluster-name", CLUSTER)
        vmsses = self.az_json("vmss", "list", "--resource-group", NODE_GROUP, "--query", VMSS_QUERY)
        require(isinstance(pools, list) and len(pools) == 2
                and {row.get("name") for row in pools} == {"default", "prompool"},
                "Only the unchanged default(2) and prompool(1) models are supported")
        require(isinstance(vmsses, list) and len(vmsses) == 2
                and {row.get("name") for row in vmsses} == {DEFAULT_VMSS, PROM_VMSS},
                "VMSS inventory is not exactly the original two scale sets")
        stable = True
        self.summary["os_reimage_eligible"] = False
        for pool in pools:
            pool_name = pool["name"]
            count = 1 if pool_name == "prompool" else 2
            vmss_name = PROM_VMSS if pool_name == "prompool" else DEFAULT_VMSS
            vmss = next(row for row in vmsses if row["name"] == vmss_name)
            vmss_id = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{NODE_GROUP}/providers/Microsoft.Compute/virtualMachineScaleSets/{vmss_name}"
            evidence["pools"][pool_name] = {
                "count": pool.get("count"), "provisioning_state": pool.get("provisioningState"),
                "power_state": (pool.get("powerState") or {}).get("code"),
                "autoscaling": pool.get("enableAutoScaling"),
                "configuration_sha256": digest(prepared.pool_configuration(pool)),
            }
            evidence["vmsses"][vmss_name] = {
                "id": vmss.get("id"), "capacity": (vmss.get("sku") or {}).get("capacity"),
                "provisioning_state": vmss.get("provisioningState"), "sku": vmss.get("sku"),
            }
            self.save()
            reimaging = restarting and self.host_action == "reimage" and pool_name == "prompool"
            known_os_failure = False
            if pool_name == "prompool" and vmss.get("provisioningState") == "Failed":
                require(
                    prepared.resource_equal(vmss.get("id"), vmss_id)
                    and str(vmss.get("location", "")).lower() == REGION
                    and workers.vmss_pool_name(vmss) == pool_name,
                    "Failed prompool VMSS ownership is not exact",
                )
                self.capture_failed_prom_instance(evidence, vmss_id)
                diagnostic = evidence["failed_prom_instance_diagnostics"]
                failed_instances = diagnostic["instances"]
                status_codes = {row.get("code") for row in diagnostic.get("statuses", [])}
                known_os_failure = (
                    self.host_action == "reimage"
                    and (allow_failed_os or reimaging)
                    and len(failed_instances) == 1
                    and str(failed_instances[0].get("instanceId")) == "0"
                    and failed_instances[0].get("vmId") == FAILED_PROM_VM_ID
                    and failed_instances[0].get("provisioningState") == "Failed"
                    and failed_instances[0].get("latestModelApplied") is True
                    and OS_FAILURE_CODE in status_codes and "PowerState/running" in status_codes
                    and any(row.get("code") == OS_FAILURE_CODE
                            for row in diagnostic.get("scale_set_statuses", []))
                )
                if known_os_failure and not reimaging:
                    failures = [row for row in diagnostic["statuses"] if row.get("code") == OS_FAILURE_CODE]
                    require(all(
                        (datetime.now(timezone.utc) - timestamp(row.get("time"), "OS provisioning failure"))
                        .total_seconds() >= 300 for row in failures
                    ), "The diagnosed OS provisioning failure is not stably terminal")
                if known_os_failure and reimaging:
                    requested = timestamp(self.summary["restart"]["requested_at"], "OS reimage request")
                    failures = [row for row in diagnostic["statuses"] if row.get("code") == OS_FAILURE_CODE]
                    require(all(timestamp(row.get("time"), "OS provisioning failure") < requested for row in failures),
                            "The single OS reimage returned a new provisioning failure")
                self.summary["os_reimage_eligible"] = known_os_failure
                self.save()
            vmss_states = {"Succeeded"}
            if known_os_failure:
                vmss_states.add("Failed")
            if reimaging:
                vmss_states.update(("Updating", "Creating"))
            require(
                integer(pool.get("count")) and pool["count"] == count
                and pool.get("enableAutoScaling") is False
                and pool.get("provisioningState") == "Succeeded"
                and (pool.get("powerState") or {}).get("code") == "Running"
                and prepared.resource_equal(pool.get("id"), f"{self.authority_pin['clusters'][ROLE]}/agentPools/{pool_name}")
                and prepared.resource_equal(vmss.get("id"), vmss_id)
                and str(vmss.get("location", "")).lower() == REGION
                and vmss.get("provisioningState") in vmss_states
                and vmss.get("orchestrationMode") == "Uniform"
                and workers.vmss_pool_name(vmss) == pool_name
                and integer((vmss.get("sku") or {}).get("capacity"))
                and (vmss.get("sku") or {}).get("capacity") == count,
                f"{pool_name}: parent pool/VMSS must be fixed-count, Running/Succeeded and exactly owned",
            )
            instances = self.az_json("vmss", "list-instances", "--resource-group", NODE_GROUP,
                                     "--name", vmss_name, "--query", VM_QUERY)
            evidence.setdefault("instance_inventory", {})[vmss_name] = [
                {key: row.get(key) for key in ("id", "instanceId", "computerName", "vmId", "provisioningState")}
                for row in instances if isinstance(row, dict)
            ] if isinstance(instances, list) else None
            self.save()
            require(isinstance(instances, list) and len(instances) == count
                    and {str(row.get("instanceId")) for row in instances} == {str(i) for i in range(count)},
                    f"{pool_name}: VM instance inventory changed")
            for instance in instances:
                instance_id = str(instance["instanceId"])
                name = f"{vmss_name}{int(instance_id):06d}"
                require(
                    prepared.resource_equal(instance.get("id"), f"{vmss_id}/virtualMachines/{instance_id}")
                    and instance.get("computerName") == name and name in REAL_UIDS
                    and instance.get("latestModelApplied") is True
                    and isinstance(instance.get("vmId"), str) and maintenance.UUID_RE.fullmatch(instance["vmId"]),
                    f"{pool_name}: VM/provider/computer identity or applied model changed",
                )
                view = self.az_json(
                    "vmss", "get-instance-view", "--resource-group", NODE_GROUP,
                    "--name", vmss_name, "--instance-id", instance_id, "--query", VIEW_QUERY,
                )
                statuses = view.get("statuses")
                evidence["instances"][name] = {
                    "id": instance["id"].lower(), "instance_id": instance_id,
                    "vm_id": instance["vmId"],
                    "provisioning_state": instance.get("provisioningState"),
                    "status_codes": [row.get("code") for row in statuses if isinstance(row, dict)]
                    if isinstance(statuses, list) else None,
                }
                self.save()
                require(isinstance(statuses, list) and all(isinstance(row, dict) for row in statuses),
                        f"{name}: VM instanceView is unreadable")
                codes = [row.get("code") for row in statuses]
                power = [code for code in codes if isinstance(code, str) and code.startswith("PowerState/")]
                provisioning = [code for code in codes if isinstance(code, str) and code.startswith("ProvisioningState/")]
                allowed_transition = restarting and name == PROM_NODE
                failed_os_vm = known_os_failure and name == PROM_NODE
                instance_states = {"Succeeded"}
                power_states = {"PowerState/running"}
                provisioning_states = {"ProvisioningState/succeeded"}
                if allowed_transition:
                    instance_states.add("Updating")
                    power_states.add("PowerState/starting")
                    provisioning_states.add("ProvisioningState/updating")
                if reimaging:
                    instance_states.add("Creating")
                    power_states.update(("PowerState/stopping", "PowerState/stopped"))
                    provisioning_states.add("ProvisioningState/creating")
                if failed_os_vm:
                    instance_states.add("Failed")
                    provisioning_states.add(OS_FAILURE_CODE)
                require(
                    len(power) == len(provisioning) == 1
                    and instance.get("provisioningState") in instance_states
                    and power[0] in power_states and provisioning[0] in provisioning_states
                    and (not failed_os_vm or instance["vmId"] == FAILED_PROM_VM_ID),
                    f"{name}: VM is stopped, failed, busy, or not safely Running/Succeeded",
                )
                extensions = view.get("extensions") or []
                require(isinstance(extensions, list), f"{name}: extension state is malformed")
                pending_extensions = failed_os_vm or (reimaging and name == PROM_NODE)
                extension_states = {"ProvisioningState/succeeded"}
                if pending_extensions:
                    extension_states.update(("ProvisioningState/creating", "ProvisioningState/updating",
                                             "ProvisioningState/transitioning"))
                require(all(
                    isinstance(row, dict) and (
                        (row.get("statuses") is None and pending_extensions)
                        or (
                            isinstance(row.get("statuses"), list)
                            and (row["statuses"] or pending_extensions)
                            and all(status.get("code") in extension_states for status in row["statuses"])
                        )
                    )
                    for row in extensions
                ), f"{name}: VM extension operations are not safely Succeeded")
                is_stable = (
                    vmss.get("provisioningState") == "Succeeded"
                    and instance["provisioningState"] == "Succeeded"
                    and {"PowerState/running", "ProvisioningState/succeeded"} <= set(codes)
                    and all(isinstance(row.get("statuses"), list) and row["statuses"]
                            and all(status.get("code") == "ProvisioningState/succeeded"
                                    for status in row["statuses"]) for row in extensions)
                )
                stable = stable and is_stable
        pin = {"pools": evidence["pools"],
               "vmsses": {name: {key: value for key, value in row.items() if key != "provisioning_state"}
                          for name, row in evidence["vmsses"].items()},
               "instances": {name: (row["id"], row["vm_id"]) for name, row in evidence["instances"].items()}}
        require(self.model_pin is None or pin == self.model_pin, "Pool configuration or VM model/count changed")
        self.model_pin = pin
        self.summary["arm_metadata"] = evidence
        self.save()
        return stable

    def capture_failed_prom_instance(self, evidence, vmss_id):
        """Keep the failed-parent gate, but record its exact VM's nonsecret state."""

        diagnostic = {"read_only": True, "instances": []}
        evidence["failed_prom_instance_diagnostics"] = diagnostic
        self.save()
        scale_view = self.az_json(
            "vmss", "get-instance-view", "--resource-group", NODE_GROUP,
            "--name", PROM_VMSS, "--query", SCALE_VIEW_QUERY,
        )
        fields = ("code", "level", "displayStatus", "time")
        diagnostic["scale_set_statuses"] = [
            {key: row.get(key) for key in fields}
            for row in scale_view.get("statuses") or [] if isinstance(row, dict)
        ]
        diagnostic["vm_status_counts"] = [
            {key: row.get(key) for key in ("code", "count")}
            for row in scale_view.get("virtualMachines") or [] if isinstance(row, dict)
        ]
        diagnostic["reported_vm_status_counts_type"] = type(scale_view.get("virtualMachines")).__name__
        self.save()
        instances = self.az_json(
            "vmss", "list-instances", "--resource-group", NODE_GROUP,
            "--name", PROM_VMSS, "--query", VM_QUERY,
        )
        require(isinstance(instances, list) and all(isinstance(row, dict) for row in instances),
                "Failed prompool instance inventory is unreadable")
        diagnostic["instances"] = [
            {key: row.get(key) for key in (
                "id", "instanceId", "computerName", "vmId", "provisioningState", "latestModelApplied",
            )}
            for row in instances
        ]
        self.save()
        selected = [row for row in instances if str(row.get("instanceId")) == "0"]
        if len(selected) != 1:
            diagnostic["error"] = "The originally pinned instance 0 is absent or ambiguous"
            self.save()
            return
        require(
            prepared.resource_equal(selected[0].get("id"), f"{vmss_id}/virtualMachines/0"),
            "Failed prompool instance resource identity is not exact",
        )
        view = self.az_json(
            "vmss", "get-instance-view", "--resource-group", NODE_GROUP,
            "--name", PROM_VMSS, "--instance-id", "0", "--query", VIEW_QUERY,
        )
        statuses = view.get("statuses")
        require(isinstance(statuses, list) and all(isinstance(row, dict) for row in statuses),
                "Failed prompool instance status is unreadable")
        diagnostic["statuses"] = [
            {key: row.get(key) for key in fields} for row in statuses
        ]
        extensions = view.get("extensions") or []
        require(isinstance(extensions, list) and all(isinstance(row, dict) for row in extensions),
                "Failed prompool extension statuses are unreadable")
        diagnostic["extensions"] = [
            {"name": row.get("name"), "reported_status_type": type(row.get("statuses")).__name__, "statuses": [
                {key: status.get(key) for key in fields}
                for status in row.get("statuses") or [] if isinstance(status, dict)
            ]}
            for row in extensions
        ]
        self.save()

    def snapshot(self):
        require(self.run(["kubectl", "--request-timeout=45s", "get", "--raw=/readyz"]).strip() == "ok",
                "The selected Kubernetes API is not ready")
        return {
            "nodes": self.kube("get", "nodes", "-o", "json"),
            "pods": self.kube("get", "pods", "-A", "-o", "json"),
            "events": self.kube("get", "events", "-A", "-o", "json"),
            "nnc": self.kube("get", "nodenetworkconfigs", "-n", "kube-system", "-o", "json"),
            "controllers": self.kube("get", "deployments,replicasets,daemonsets,statefulsets", "-A", "-o", "json"),
            "pdbs": self.kube("get", "pdb", "-A", "-o", "json"),
            "cilium_config": self.kube("-n", "kube-system", "get", "configmap", "cilium-config", "-o", "json"),
        }

    def guard(self, snapshot, *, host_unready=False, host_optional=False):
        require(not host_optional or bool(getattr(self.args, "replace_failed_host", None)),
                "Only explicit failed-host replacement may observe native host removal")
        nodes = mocks._items(snapshot["nodes"], "Nodes")
        mapping = {row["metadata"]["name"]: row for row in nodes}
        expected = {**self.plan["kwok_node_uids"], **self.real_uids}
        if host_optional and self.host_node not in mapping:
            expected.pop(self.host_node)
        require(len(nodes) == len(mapping) == len(expected) and set(mapping) == set(expected),
                "The complete pinned Node inventory must remain exact")
        for name, uid in expected.items():
            node = mapping[name]
            require(object_uid(node) == uid and (
                not node["metadata"].get("deletionTimestamp") or (host_optional and name == self.host_node)
            ),
                    f"{name}: original Node UID/deletion state changed")
            if name in self.real_uids:
                maintenance._validate_real_node_scope(node, subscription=SUBSCRIPTION, node_resource_group=NODE_GROUP)
                if name == self.host_node:
                    exact_provider = prepared.resource_equal(
                        node["spec"].get("providerID"), self.host_provider_id,
                    )
                else:
                    instance = "1" if name == f"{DEFAULT_VMSS}000001" else "0"
                    exact_provider = workers.provider_identity(node) == (DEFAULT_VMSS, instance)
                require(exact_provider
                        and mocks._node_pool_name(node) == ("prompool" if name == self.host_node else "default"),
                        f"{name}: real provider or pool identity changed")
            else:
                require((node["metadata"].get("labels") or {}).get("type") == "kwok",
                        f"{name}: KWOK Node label changed")
            if name != self.host_node:
                require(workers.node_is_ready(node), f"{name}: original healthy Node is no longer Ready")
                if self.initial is not None:
                    original = next(row for row in self.initial["nodes"]["items"] if row["metadata"]["name"] == name)
                    require(node["spec"].get("unschedulable", False) == original["spec"].get("unschedulable", False),
                            f"{name}: original scheduling state changed")
                    if name in self.real_uids:
                        require(node_boot(node) == node_boot(original), f"{name}: an original default worker rebooted")
        mock_controller = controller(snapshot, "StatefulSet", mocks.DEFAULT_NAMESPACE, "kwok-node",
                                     self.plan["mock_controller_uid"])
        require(mock_controller["spec"].get("replicas") == 100, "Mock controller replicas changed")
        agents = maintenance._require_exact_agents(snapshot["pods"], self.plan["mock_controller_uid"])
        require({name: object_uid(pod) for name, pod in agents.items()} == self.plan["mock_pod_uids"],
                "An original mock Pod UID changed")
        require(all((pod.get("spec") or {}).get("nodeName") in set(self.real_uids) - {self.host_node}
                    for pod in agents.values()), "A mock agent is on the target or an unexpected worker")
        require(all(pod_ready(agents[name]) for name in self.plan["ready_mock_pod_uids"]),
                "An originally healthy mock agent regressed (including healthy agents on the CNI source)")
        if self.initial is not None:
            require(frozen_controllers(snapshot) == frozen_controllers(self.initial),
                    "Live controller UIDs/specifications changed")
            require(frozen_pdbs(snapshot) == frozen_pdbs(self.initial), "PDB identities/specifications changed")
        pod_by_uid = {object_uid(pod): pod for pod in mocks._items(snapshot["pods"], "protected framework Pods")}
        protected = [*self.ready_targets.values(), *self.dns_pins.values()]
        for group in self.groups.values():
            protected.extend(group["fixed"].values())
        for pinned in protected:
            pod = pod_by_uid.get(pinned["uid"])
            require(
                pod is not None and pod_ready(pod) and pod["metadata"]["name"] == pinned["name"]
                and pod["metadata"].get("namespace") == pinned["namespace"]
                and pod["spec"].get("nodeName") == pinned["node_name"]
                and pinned["node_name"] in mapping and workers.node_is_ready(mapping[pinned["node_name"]])
                and controller_owner(pod, "ReplicaSet")["uid"] == pinned["owner_uid"],
                "An already healthy/recovered framework Pod or CoreDNS sibling changed or regressed",
            )
        local = next(row for row in self.identities if row["role"] == ROLE)
        data = snapshot["cilium_config"].get("data") or {}
        require(data.get("cluster-name") == local["cluster_name"] and str(data.get("cluster-id")) == str(local["cluster_id"]),
                "Local Cilium identity disagrees with the authoritative Fleet map")
        nncs = maintenance._nnc_map(snapshot["nnc"])
        require(SOURCE_NODE in nncs and nncs[SOURCE_NODE]["node_uid"] == REAL_UIDS[SOURCE_NODE]
                and nncs[SOURCE_NODE]["network_container_id"] == SOURCE_NC,
                "Pinned CNI source NodeNetworkConfig identity changed")
        host = mapping.get(self.host_node)
        require(host is not None or (host_optional and not host_unready),
                "The pinned host is missing outside owned replacement observation")
        if host is not None:
            require(prepared.resource_equal(host["spec"].get("providerID"), self.host_provider_id),
                    "Target provider ID changed")
            node_boot(host)
        if self.summary["restart"].get("host_proven"):
            require(host is not None and workers.node_is_ready(host)
                    and node_boot(host) == self.summary["restart"]["current_boot_id"],
                    "The IP-qualified prom host lost readiness or rebooted again")
        for pod in mocks._items(snapshot["pods"], "Pods"):
            if (pod.get("spec") or {}).get("nodeName") != self.host_node:
                continue
            meta = pod["metadata"]
            if self.probe and meta.get("name") == self.probe["name"]:
                self.prove_probe_owner(pod)
                continue
            require(pvc_free(pod.get("spec") or {}), "A target-host Pod has a PVC/ephemeral claim")
            owners = meta.get("ownerReferences") or []
            kind = next((row.get("kind") for row in owners if row.get("controller") is True), "")
            owner = controller_owner(pod, kind)
            namespace = meta.get("namespace")
            if kind == "DaemonSet":
                require(namespace == "kube-system" and owner["name"] in HOST_DAEMONSETS,
                        "An unsupported DaemonSet occupies the target host")
                controller(snapshot, kind, namespace, owner["name"], owner["uid"])
            else:
                require(kind == "ReplicaSet", "Target host has a non-system or unmanaged Pod")
                replica_set = controller(snapshot, kind, namespace, owner["name"], owner["uid"])
                deployment = controller_owner(replica_set, "Deployment")
                require((namespace, deployment["name"]) in HOST_DEPLOYMENTS,
                        "Target host has an unsupported framework Deployment")
                controller(snapshot, "Deployment", namespace, deployment["name"], deployment["uid"])
            if host_unready:
                require(not maintenance._readiness_condition_true(pod),
                        "A Pod on the unreachable host is still Pod Ready; restart is not authorized")
        if host_unready:
            prove_unreachable(host)
        return mapping, agents

    def system_ready(self, snapshot):
        pods = mocks._items(snapshot["pods"], "Pods")
        host_pods = [row for row in pods if (row.get("spec") or {}).get("nodeName") == self.host_node]
        expected = {
            (row["metadata"]["namespace"], owner["name"], owner["uid"])
            for row in mocks._items(self.initial["pods"], "initial host Pods")
            if (row.get("spec") or {}).get("nodeName") == self.system_origin_node
            for owner in row["metadata"].get("ownerReferences", [])
            if owner.get("controller") is True and owner.get("kind") == "DaemonSet"
        }
        require({name for namespace, name, _ in expected if namespace == "kube-system"} >= {"cilium", "azure-cns"},
                "Initial host lacks pinned Cilium/CNS DaemonSet ownership")
        actual = {
            (row["metadata"]["namespace"], owner["name"], owner["uid"])
            for row in host_pods if pod_ready(row)
            for owner in row["metadata"].get("ownerReferences", [])
            if owner.get("controller") is True and owner.get("kind") == "DaemonSet"
        }
        return expected <= actual

    def owned_pods(self, snapshot, target):
        result = []
        for pod in mocks._items(snapshot["pods"], "Pods"):
            if pod["metadata"].get("namespace") != target["namespace"]:
                continue
            owners = pod["metadata"].get("ownerReferences") or []
            if any(row.get("controller") is True and row.get("kind") == "ReplicaSet"
                   and row.get("name") == target["replica_set_name"]
                   and row.get("uid") == target["replica_set_uid"] for row in owners):
                controller_owner(pod, "ReplicaSet")
                result.append(pod)
        return result

    @staticmethod
    def ready_pin(pod, target):
        return {
            "uid": object_uid(pod), "name": pod["metadata"]["name"],
            "namespace": target["namespace"], "node_name": pod["spec"]["nodeName"],
            "owner_uid": target["replica_set_uid"],
        }

    def bind_group(self, snapshot, target, count):
        key = target["namespace"], target["replica_set_uid"]
        if key in self.groups:
            return self.groups[key]
        selected = [row for row in self.targets
                    if (row["namespace"], row["replica_set_uid"]) == key]
        active = [row for row in self.owned_pods(snapshot, target) if not row["metadata"].get("deletionTimestamp")]
        require(len(active) == count and len({object_uid(row) for row in active}) == count,
                "Live ReplicaSet Pod count is ambiguous")
        present = {object_uid(row): row for row in active}
        assignments = {target_key(row): row["pod_uid"] for row in selected if row["pod_uid"] in present}
        unknown = sorted(
            (row for row in active if object_uid(row) not in set(assignments.values())),
            key=lambda row: object_uid(row),
        )
        require(all(pod_ready(row) for row in unknown),
                "An unselected or unpinned subsequent Pending framework sibling is not Ready")
        missing = [row for row in selected if target_key(row) not in assignments]
        require(len(unknown) >= len(missing), "Selected ReplicaSet slots have no Ready replacements")
        # ReplicaSet replicas have no individual ancestry. Adopt only an entirely
        # Ready unpinned UID set, protecting every member; never delete from it.
        for row, pod in zip(missing, unknown):
            assignments[target_key(row)] = object_uid(pod)
        fixed = {object_uid(pod): self.ready_pin(pod, target) for pod in unknown[len(missing):]}
        group = {
            "selected": selected, "assignments": assignments, "fixed": fixed,
            "known_uids": {object_uid(row) for row in self.owned_pods(snapshot, target)},
            "group_adopted": bool(missing) and count > 1,
        }
        self.groups[key] = group
        if missing and count > 1:
            self.summary.setdefault("read_only_replica_set_adoptions", []).append({
                "namespace": target["namespace"], "replica_set_uid": target["replica_set_uid"],
                "ready_unpinned_uids": [object_uid(row) for row in unknown],
                "missing_original_uids": [row["pod_uid"] for row in missing],
                "individual_ancestry_claimed": False,
            })
        return group

    def dns_ready(self, snapshot):
        deployment = controller(snapshot, "Deployment", "kube-system", "coredns", DNS_DEPLOYMENT_UID)
        replica_set = controller(snapshot, "ReplicaSet", "kube-system", DNS_REPLICA_SET, DNS_REPLICA_SET_UID)
        owner = controller_owner(replica_set, "Deployment")
        require(owner["name"] == "coredns" and owner["uid"] == DNS_DEPLOYMENT_UID
                and deployment["spec"].get("replicas") == replica_set["spec"].get("replicas") == DNS_REPLICAS,
                "The original five-replica CoreDNS controller identity/configuration changed")
        target = {"namespace": "kube-system", "replica_set_name": DNS_REPLICA_SET,
                  "replica_set_uid": DNS_REPLICA_SET_UID}
        active = [row for row in self.owned_pods(snapshot, target) if not row["metadata"].get("deletionTimestamp")]
        nodes = maintenance._real_node_map(snapshot["nodes"])
        healthy = [row for row in active if pod_ready(row) and row["spec"].get("nodeName") in nodes
                   and workers.node_is_ready(nodes[row["spec"]["nodeName"]])]
        ready = len(active) == len(healthy) == DNS_REPLICAS
        self.summary["dns_proof"] = {
            "deployment_uid": DNS_DEPLOYMENT_UID, "replica_set_uid": DNS_REPLICA_SET_UID,
            "replicas": DNS_REPLICAS, "ready_replicas": len(healthy), "all_ready": ready,
            "ready_pod_uids": {row["metadata"]["name"]: object_uid(row) for row in healthy},
        }
        if ready and not self.dns_pins:
            self.dns_pins = {object_uid(row): self.ready_pin(row, target) for row in active}
        return ready

    def bind_target(self, snapshot, target):
        key = target["namespace"], target["pod_name"]
        replica_set = controller(snapshot, "ReplicaSet", target["namespace"],
                                 target["replica_set_name"], target["replica_set_uid"])
        owner = controller_owner(replica_set, "Deployment")
        require(owner["name"] == target["deployment_name"]
                and owner["uid"] == target.get("deployment_uid", owner["uid"]),
                "Pinned ReplicaSet's Deployment owner changed")
        deployment = controller(snapshot, "Deployment", target["namespace"], owner["name"], owner["uid"])
        target["deployment_uid"] = owner["uid"]
        count = deployment["spec"].get("replicas")
        require(integer(count) and count >= 1 and replica_set["spec"].get("replicas") == count
                and not deployment["spec"].get("paused")
                and (deployment.get("status") or {}).get("observedGeneration", 0)
                >= deployment["metadata"].get("generation", 1),
                "Deployment/ReplicaSet replicas or observed generation are unsafe")
        require(target["deployment_name"] != "clustermesh-apiserver" or count == 1,
                "ClusterMesh API Deployment must remain at exactly one replica")
        require(target["deployment_name"] != "coredns"
                or (count == DNS_REPLICAS and owner["uid"] == DNS_DEPLOYMENT_UID),
                "The five-replica CoreDNS Deployment UID/count changed")
        for row in mocks._items(snapshot["controllers"], "controllers"):
            if row.get("kind") != "ReplicaSet" or row["metadata"].get("namespace") != target["namespace"]:
                continue
            if any(ref.get("controller") is True and ref.get("uid") == owner["uid"]
                   for ref in row["metadata"].get("ownerReferences", [])):
                require(object_uid(row) == target["replica_set_uid"] or row["spec"].get("replicas") == 0,
                        "Deployment has an active competing ReplicaSet/rollout")
        template = replica_set["spec"].get("template", {}).get("spec")
        require(isinstance(template, dict) and pvc_free(template) and not template.get("hostNetwork")
                and not template.get("nodeName") and template.get("schedulerName", "default-scheduler") == "default-scheduler",
                "Pinned controller has a PVC or unsupported scheduling/network configuration")
        require(deployment["spec"].get("template", {}).get("spec") == template,
                "Deployment and pinned ReplicaSet Pod specifications disagree")
        if key not in self.bindings:
            group = self.bind_group(snapshot, target, count)
            self.bindings[key] = {
                "template": copy.deepcopy(template), "group": group,
                "known_uids": group["known_uids"],
                "replicas": count,
            }
        binding = self.bindings[key]
        require(template == binding["template"] and count == binding["replicas"],
                "Target Pod controller spec/replicas changed")
        return binding

    def target_state(self, snapshot, target, *, after_delete=False):
        binding = self.bind_target(snapshot, target)
        group = binding["group"]
        owned = self.owned_pods(snapshot, target)
        active = [row for row in owned if not row["metadata"].get("deletionTimestamp")]
        by_uid = {object_uid(row): row for row in active}
        siblings = {row_uid: by_uid[row_uid] for row_uid in group["fixed"] if row_uid in by_uid}
        require(set(siblings) == set(group["fixed"]) and all(pod_ready(row) for row in siblings.values()),
                "An original framework sibling changed or regressed")
        excluded = set(group["fixed"])
        for peer in group["selected"]:
            if target_key(peer) == target_key(target):
                continue
            row_uid = group["assignments"][target_key(peer)]
            pod = by_uid.get(row_uid)
            require(pod is not None, "An explicitly selected sibling disappeared or changed UID")
            if pod_ready(pod):
                self.ready_targets[target_key(peer)] = self.ready_pin(pod, peer)
            else:
                require(target_key(peer) not in self.ready_targets and row_uid == peer["pod_uid"],
                        "An already recovered sibling regressed")
                self.pending_evidence(snapshot, peer, pod)
            excluded.add(row_uid)
        candidates = [row for row in active if object_uid(row) not in excluded]
        require(len(candidates) <= 1, "Target ReplicaSet replacement ownership is not unique")
        if not candidates:
            require(after_delete, "The pinned failed Pod disappeared without a unique Ready replacement")
            return "waiting", None
        pod = candidates[0]
        require(pvc_free(pod.get("spec") or {}), "Pinned Pod has a PVC/ephemeral claim")
        same = object_uid(pod) == target["pod_uid"]
        if same:
            require(pod["metadata"]["name"] == target["pod_name"], "Pinned Pod UID/name changed")
        if pod_ready(pod):
            node_name = pod.get("spec", {}).get("nodeName")
            node = maintenance._real_node_map(snapshot["nodes"]).get(node_name)
            require(node is not None and workers.node_is_ready(node), "Ready replacement is not on a Ready real node")
            if after_delete:
                require(not same and object_uid(pod) not in binding["known_uids"] and node_name == self.host_node,
                        "New Pod must have a new owned UID on the IP-qualified prom host")
            previous = self.ready_targets.get(target_key(target))
            require(previous is None or object_uid(pod) == previous["uid"], "A recovered Pod UID changed again")
            group["assignments"][target_key(target)] = object_uid(pod)
            self.ready_targets[target_key(target)] = self.ready_pin(pod, target)
            state = "already-ready" if same else "adopted-ready"
            if not same and group["group_adopted"]:
                state = "adopted-ready-group"
            return state, pod
        if after_delete:
            return "waiting", pod
        require(same, "Refusing to delete an unpinned subsequent Pending Pod")
        require(target_key(target) not in self.ready_targets, "An already recovered framework Pod regressed")
        self.pending_evidence(snapshot, target, pod)
        return "delete-pinned", pod

    @staticmethod
    def pending_evidence(snapshot, target, pod):
        require(object_uid(pod) == target["pod_uid"] and pod["metadata"]["name"] == target["pod_name"]
                and pod["metadata"].get("namespace") == target["namespace"] and pvc_free(pod.get("spec") or {}),
                "Pending sibling UID/name/namespace/PVC proof changed")
        require(pod.get("spec", {}).get("nodeName") == SOURCE_NODE,
                "Pinned failed Pod is not on the explicit bad CNI source")
        never_started(pod)
        now = datetime.now(timezone.utc)
        events = []
        for event in mocks._items(snapshot["events"], "events"):
            reference = event.get("involvedObject") or event.get("regarding") or {}
            if (reference.get("kind"), reference.get("namespace"), reference.get("name"), reference.get("uid")) != (
                "Pod", target["namespace"], target["pod_name"], target["pod_uid"],
            ):
                continue
            if not mocks._event_proves_cni_exhaustion(event) or SOURCE_NC not in str(event.get("message", "")):
                continue
            last = (event.get("series") or {}).get("lastObservedTime") or event.get("lastTimestamp") or event.get("eventTime")
            age = (now - timestamp(last, "CNS exhaustion event")).total_seconds()
            if 0 <= age <= 600:
                events.append(event)
        require(events, "Current UID-bound CNS exhaustion evidence is missing")

    def wait(self, deadline, description):
        require(time.monotonic() < min(deadline, self.work_deadline), f"{description} exceeded its bounded deadline")
        time.sleep(min(POLL_SECONDS, max(0.01, min(deadline, self.work_deadline) - time.monotonic())))

    def prior_marker(self, host):
        existing = maintenance._annotations(host).get(MARKER_KEY)
        if existing is None:
            return False
        record = self.summary["restart"]
        record["prior_marker"] = {"sha256": digest(existing)}
        self.save()
        require(workers.node_is_ready(host), "A prior restart marker forbids another request while the host is unready")
        try:
            marker = json.loads(existing)
        except (ValueError, TypeError) as error:
            raise workers.ReconcileError("Foreign/malformed restart marker must not be cleared") from error
        require(
            isinstance(marker, dict) and marker.get("owner") == OWNER and marker.get("schema_version") == 1
            and marker.get("action") == f"single-instance-{self.host_action}"
            and marker.get("node_uid") == REAL_UIDS[PROM_NODE] and marker.get("provider_id") == PROVIDER
            and marker.get("plan_sha256") == self.summary["plan_sha256"]
            and isinstance(marker.get("token"), str) and maintenance.UUID_RE.fullmatch(marker["token"])
            and marker.get("previous_boot_id") and node_boot(host) != marker["previous_boot_id"],
            "Recovered host lacks exact owned-marker and changed-boot proof",
        )
        self.marker = existing
        record["prior_marker"].update({
            key: marker[key] for key in ("owner", "token", "node_uid", "previous_boot_id", "plan_sha256")
        })
        record["previous_boot_id"] = marker["previous_boot_id"]
        record["skipped_reason"] = "prior-owned-restart-already-recovered"
        self.save()
        return True

    def restart_host(self, snapshot):
        nodes, _ = self.guard(snapshot)
        host = nodes[PROM_NODE]
        record = self.summary["restart"]
        record["action"] = self.host_action
        resumed = self.prior_marker(host)
        if not resumed and workers.node_is_ready(host):
            record["skipped_reason"] = "host-already-ready"
        elif not resumed:
            self.authority()
            self.models(allow_failed_os=self.host_action == "reimage")
            require(self.host_action != "reimage" or self.summary["os_reimage_eligible"],
                    "OS reimage requires the exact pinned OSProvisioningClientError VM")
            snapshot = self.snapshot()
            nodes, _ = self.guard(snapshot, host_unready=True)
            host = nodes[PROM_NODE]
            require(MARKER_KEY not in maintenance._annotations(host), "Another restart marker appeared")
            for target in self.targets:
                self.target_state(snapshot, target)
            previous = node_boot(host)
            marker = {
                "schema_version": 1, "owner": OWNER, "token": str(uuid.uuid4()),
                "node_uid": REAL_UIDS[PROM_NODE], "provider_id": PROVIDER,
                "previous_boot_id": previous, "plan_sha256": self.summary["plan_sha256"],
                "recorded_at": workers.utc_now(), "action": f"single-instance-{self.host_action}",
            }
            self.marker = json.dumps(marker, sort_keys=True, separators=(",", ":"))
            record.update({"previous_boot_id": previous, "marker": marker, "marker_write_attempted": True})
            self.save()
            annotations = dict(maintenance._annotations(host))
            annotations[MARKER_KEY] = self.marker
            self.write(["kubectl", "patch", "node", PROM_NODE, "--type=json", "-p", json.dumps([
                {"op": "test", "path": "/metadata/uid", "value": REAL_UIDS[PROM_NODE]},
                {"op": "test", "path": "/metadata/resourceVersion", "value": host["metadata"]["resourceVersion"]},
                {"op": "add", "path": "/metadata/annotations", "value": annotations},
            ])])
            self.authority()
            self.models(allow_failed_os=self.host_action == "reimage")
            require(self.host_action != "reimage" or self.summary["os_reimage_eligible"],
                    "The pinned OS provisioning failure changed before reimage")
            latest = self.snapshot()
            latest_nodes, _ = self.guard(latest, host_unready=True)
            require(maintenance._annotations(latest_nodes[PROM_NODE]).get(MARKER_KEY) == self.marker,
                    "Durable restart marker was not confirmed")
            require(node_boot(latest_nodes[PROM_NODE]) == previous, "Host boot changed before the restart request")
            record.update({"attempted": True, "accepted": None, "ambiguous": True, "requested_at": workers.utc_now()})
            self.save()
            self.write([
                "az", "vmss", self.host_action, "--resource-group", NODE_GROUP, "--name", PROM_VMSS,
                "--instance-ids", "0", "--no-wait", "--only-show-errors", "--output", "none",
            ])
            record.update({"accepted": True, "ambiguous": False})
            self.save()
        deadline = min(self.work_deadline, time.monotonic() + RESTART_SECONDS)
        while True:
            snapshot = self.snapshot()
            nodes, _ = self.guard(snapshot)
            host = nodes[PROM_NODE]
            stable = self.models(restarting=record["attempted"])
            boot = node_boot(host)
            record["current_boot_id"] = boot
            self.save()
            changed = not self.marker or boot != record["previous_boot_id"]
            if workers.node_is_ready(host) and changed and stable and self.system_ready(snapshot):
                require(not host.get("spec", {}).get("unschedulable")
                        and not any(row.get("effect") in ("NoSchedule", "NoExecute")
                                    for row in maintenance._taints(host)),
                        "Recovered prom host still has scheduling holds")
                record["host_proven"] = True
                self.save()
                return
            self.wait(deadline, "Single-restart host/boot/DaemonSet convergence")

    def node_metric(self):
        metrics = self.kube("get", "--raw", "/apis/metrics.k8s.io/v1beta1/nodes")
        matches = [row for row in mocks._items(metrics, "actual node metrics")
                   if (row.get("metadata") or {}).get("name") == self.host_node]
        require(len(matches) == 1, "Actual prom-host memory/CPU metrics are missing")
        return matches[0]

    def prove_memory(self, node, metric, reserve, *, pod_uid="", probe=False):
        observed = int(mocks._quantity((metric.get("usage") or {}).get("memory"), "actual node memory"))
        require(observed > 0, "Actual node memory usage is missing/invalid")
        if not self.memory_observed:
            self.memory_baseline = observed
            self.memory_observed = True
        else:
            self.memory_baseline = max(self.memory_baseline, observed - self.reserved_memory)
        effective_reserved = max(self.memory_baseline + self.reserved_memory - observed, 0)
        self.summary["memory_state"] = {
            "baseline_high_water_bytes": self.memory_baseline, "reserved_memory_bytes": self.reserved_memory,
            "committed_high_water_bytes": self.memory_baseline + self.reserved_memory,
            "observed_memory_bytes": observed, "effective_reserved_memory_bytes": effective_reserved,
            "next_pod_uid": pod_uid, "next_memory_reserve_bytes": reserve, "metric_timestamp": metric.get("timestamp"),
        }
        self.save()
        require(maintenance._headroom_ok(
            node, metric, threshold_percent=85, effective_reserved_memory_bytes=effective_reserved,
            next_memory_bytes=reserve + (16 * 1024**2 if probe else 0),
        ), "Actual memory headroom cannot safely reserve the next Pod (API/Grafana reserves are 8Gi each, not limits)")
        if self.summary["restart"].get("requested_at"):
            require(timestamp(metric.get("timestamp"), "node metrics")
                    >= timestamp(self.summary["restart"]["requested_at"], "restart request"),
                    "Node metrics predate the host restart")
        return effective_reserved

    def qualify_capacity(self, snapshot, target):
        nodes, _ = self.guard(snapshot)
        node = nodes[self.host_node]
        require(workers.node_is_ready(node) and self.system_ready(snapshot), "Prom host/system readiness regressed")
        binding = self.bind_target(snapshot, target)
        template = binding["template"]
        require(mocks._node_ready_and_schedulable(node) and mocks._node_matches_pod_template(node, template),
                "Prom host does not satisfy the unchanged Pod scheduling requirements")
        exclusion = {"key": EXCLUSION_KEY, "value": "test-token", "effect": "NoSchedule"}
        require(not mocks._tolerates(exclusion, template.get("tolerations") or []),
                "Pod tolerations would bypass the temporary placement exclusion")
        for name in self.plan["kwok_node_uids"]:
            require(not (mocks._node_ready_and_schedulable(nodes[name])
                         and mocks._node_matches_pod_template(nodes[name], template)),
                    "An unchanged Pod template could schedule onto a KWOK Node")
        metric = self.node_metric()
        requested_cpu, _ = mocks._resource_requests({"spec": template})
        reserve = memory_reserve(target, template)
        effective_reserved = self.prove_memory(node, metric, reserve, pod_uid=target["pod_uid"], probe=True)
        alloc = node["status"].get("allocatable") or {}
        active = [row for row in mocks._items(snapshot["pods"], "capacity Pods")
                  if (row.get("spec") or {}).get("nodeName") == self.host_node
                  and (row.get("status") or {}).get("phase") not in ("Succeeded", "Failed")]
        cpu_requests = sum(mocks._resource_requests(row)[0] for row in active)
        actual_cpu = int(mocks._quantity((metric.get("usage") or {}).get("cpu"), "actual node CPU") * 1000)
        require((metric.get("usage") or {}).get("cpu") is not None and actual_cpu >= 0,
                "Actual node CPU metrics are missing/invalid")
        free_cpu = (
            int(mocks._quantity(alloc.get("cpu"), "allocatable CPU") * 1000)
            - max(cpu_requests, actual_cpu) - self.reserved_cpu
        )
        free_slots = int(mocks._quantity(alloc.get("pods"), "allocatable Pods")) - len(active)
        require(free_cpu >= max(requested_cpu, 500) + 250 and free_slots >= 7,
                "Actual CPU/request headroom or reserved Pod slots are insufficient")
        self.summary["last_capacity_proof"] = {
            "node_uid": self.real_uids[self.host_node], "metric_timestamp": metric["timestamp"],
            "actual_memory": metric["usage"]["memory"], "actual_cpu": metric["usage"]["cpu"],
            "next_memory_reserve_bytes": reserve, "prior_move_reserve_bytes": self.reserved_memory,
            "baseline_high_water_bytes": self.memory_baseline,
            "effective_reserved_memory_bytes": effective_reserved,
            "prior_cpu_reserve_millicores": self.reserved_cpu,
            "free_cpu_millicores": free_cpu, "free_pod_slots": free_slots,
            "memory_reserve_is_not_a_container_limit": True,
        }
        self.save()
        return reserve

    def commit_memory(self, target, pod):
        key = target_key(target)
        if pod["spec"]["nodeName"] != self.host_node or object_uid(pod) in self.reservations:
            return
        template = self.bindings[key]["template"]
        reserve = memory_reserve(target, template)
        self.reserved_memory += reserve
        self.reserved_cpu += max(mocks._resource_requests({"spec": template})[0], 500)
        self.reservations.add(object_uid(pod))
        self.summary.setdefault("memory_commitments", {})[f"{target['namespace']}/{pod['metadata']['name']}"] = {
            "pod_uid": object_uid(pod), "node_uid": self.real_uids[self.host_node],
            "reserved_memory_bytes": reserve, "reservation_is_not_a_container_limit": True,
        }
        if "memory_state" in self.summary:
            self.summary["memory_state"].update({
                "reserved_memory_bytes": self.reserved_memory,
                "committed_high_water_bytes": self.memory_baseline + self.reserved_memory,
                "effective_reserved_memory_bytes": max(
                    self.memory_baseline + self.reserved_memory - self.summary["memory_state"]["observed_memory_bytes"], 0,
                ),
            })
        self.save()

    def add_exclusions(self):
        if self.exclusions:
            return
        for name, uid in self.real_uids.items():
            if name == self.host_node:
                continue
            node = self.kube("get", "node", name, "-o", "json")
            require(object_uid(node) == uid and workers.node_is_ready(node), "Exclusion Node identity/readiness changed")
            taints = maintenance._taints(node)
            require(not any(row.get("key") == EXCLUSION_KEY for row in taints), "Foreign placement exclusion exists")
            entry = {"name": name, "uid": uid, "token": uuid.uuid4().hex, "write_attempted": True}
            self.exclusions.append(entry)
            self.summary["temporary_exclusions"] = self.exclusions
            self.save()
            self.write(["kubectl", "patch", "node", name, "--type=json", "-p", json.dumps([
                {"op": "test", "path": "/metadata/uid", "value": uid},
                {"op": "test", "path": "/metadata/resourceVersion", "value": node["metadata"]["resourceVersion"]},
                {"op": "add", "path": "/spec/taints", "value": list(taints) + [{
                    "key": EXCLUSION_KEY, "value": entry["token"], "effect": "NoSchedule",
                }]},
            ])])

    def require_exclusions(self, snapshot):
        nodes = maintenance._real_node_map(snapshot["nodes"])
        require(len(self.exclusions) == 2, "Both real alternate nodes must be temporarily excluded")
        for entry in self.exclusions:
            require(object_uid(nodes[entry["name"]]) == entry["uid"] and {
                "key": EXCLUSION_KEY, "value": entry["token"], "effect": "NoSchedule",
            } in maintenance._taints(nodes[entry["name"]]), "Owned placement exclusion disappeared")

    def prove_probe_owner(self, pod, *, validate_spec=True):
        containers = (pod.get("spec") or {}).get("containers") or []
        require(
            self.probe is not None and pod["metadata"].get("namespace") == "kube-system"
            and pod["metadata"]["name"] == self.probe["name"]
            and (pod["metadata"].get("labels") or {}).get(PROBE_KEY) == self.probe["token"]
            and pod.get("spec", {}).get("nodeName") == self.host_node
            and (not self.probe.get("uid") or self.probe["uid"] == object_uid(pod)),
            "Probe UID/token/node ownership changed",
        )
        require(object_uid(pod), "Owned probe UID is missing")
        self.probe["uid"] = object_uid(pod)
        if validate_spec:
            require(
                not pod["spec"].get("hostNetwork") and len(containers) == 1
                and containers[0].get("image") == maintenance.DEFAULT_PROBE_IMAGE
                and containers[0].get("args") == ["netexec", "--http-port=8080"]
                and (containers[0].get("readinessProbe") or {}).get("httpGet", {}).get("port") == 8080
                and (containers[0].get("readinessProbe") or {}).get("httpGet", {}).get("path") == "/"
                and pod["spec"].get("automountServiceAccountToken") is False and pvc_free(pod["spec"]),
                "The owned actual-IP probe image, command, or HTTP readiness contract changed",
            )

    def delete_exact(self, namespace, name, uid, receipt):
        require(self.args.execute and not receipt.get("delete_attempted"), "A Pod UID deletion cannot be repeated")
        receipt.update({"delete_attempted": True, "delete_accepted": None, "delete_ambiguous": True})
        self.summary["mutation_started"] = True
        self.save()
        self.delete_pod(
            mocks.Cluster(ROLE, self.args.kubeconfig, CLUSTER, CLUSTER, RESOURCE_GROUP),
            namespace=namespace, name=name, uid=uid,
            timeout_seconds=self.remaining_seconds(45, cleanup=self.cleanup_mode), attempts=1, retry_seconds=0,
        )
        receipt.update({"delete_accepted": True, "delete_ambiguous": False})
        self.save()

    def probe_ip(self, snapshot, target):
        self.qualify_capacity(snapshot, target)
        token = uuid.uuid4().hex
        name = f"prom-recovery-ip-{token[:20]}"
        self.probe = {"name": name, "token": token, "uid": "", "create_attempted": True}
        self.summary["probe_cleanup_pending"] = self.probe
        self.save()
        overrides = {
            "apiVersion": "v1",
            "spec": {
                "nodeName": self.host_node, "hostNetwork": False, "restartPolicy": "Never",
                "automountServiceAccountToken": False, "enableServiceLinks": False,
                "containers": [{
                    "name": name, "image": maintenance.DEFAULT_PROBE_IMAGE,
                    "args": ["netexec", "--http-port=8080"],
                    "resources": {"requests": {"cpu": "5m", "memory": "16Mi"}},
                    "readinessProbe": {"httpGet": {"path": "/", "port": 8080}, "periodSeconds": 2},
                }],
            },
        }
        output = self.write([
            "kubectl", "-n", "kube-system", "run", name, f"--image={maintenance.DEFAULT_PROBE_IMAGE}",
            "--restart=Never", f"--labels={PROBE_KEY}={token}", "-o", "json",
            f"--overrides={json.dumps(overrides)}",
        ])
        self.prove_probe_owner(workers.parse_json(output, "owned IP probe"))
        self.save()
        deadline = min(self.work_deadline, time.monotonic() + POD_READY_SECONDS)
        while True:
            current = self.snapshot()
            self.guard(current)
            self.require_exclusions(current)
            candidates = [row for row in mocks._items(current["pods"], "IP probe Pods")
                          if row["metadata"].get("namespace") == "kube-system" and row["metadata"]["name"] == name]
            require(len(candidates) == 1, "The owned IP probe disappeared or became ambiguous")
            self.prove_probe_owner(candidates[0])
            if pod_ready(candidates[0]):
                self.summary.setdefault("ip_proofs", []).append({
                    "pod_name": name, "pod_uid": self.probe["uid"], "node_uid": self.real_uids[self.host_node],
                    "pod_ip": candidates[0]["status"]["podIP"], "ready": True,
                })
                self.save()
                break
            self.wait(deadline, "Actual Ready IP probe")
        self.cleanup_probe()
        require(self.probe is None, "The actual-IP probe must be UID-deleted before exchanging its slot")

    def cleanup_probe(self):
        if self.probe is None:
            return
        rows = mocks._items(self.kube("-n", "kube-system", "get", "pods", "-o", "json"), "probe cleanup")
        matches = [row for row in rows if row["metadata"]["name"] == self.probe["name"]]
        require(len(matches) <= 1, "Probe cleanup inventory is ambiguous")
        if matches:
            self.prove_probe_owner(matches[0], validate_spec=False)
            self.delete_exact("kube-system", self.probe["name"], self.probe["uid"], self.probe)
            deadline = min(self.cleanup_deadline if self.cleanup_mode else self.work_deadline, time.monotonic() + 60)
            while True:
                rows = mocks._items(self.kube("-n", "kube-system", "get", "pods", "-o", "json"), "probe deletion")
                matches = [row for row in rows if row["metadata"]["name"] == self.probe["name"]]
                if not matches:
                    break
                self.prove_probe_owner(matches[0], validate_spec=False)
                require(time.monotonic() < deadline, "Owned probe deletion did not converge")
                time.sleep(min(POLL_SECONDS, max(0.01, deadline - time.monotonic())))
        self.probe = None
        self.summary["probe_cleanup_pending"] = None
        self.save()

    def move_pod(self, target):
        snapshot = self.snapshot()
        self.guard(snapshot)
        if target["deployment_name"] != "coredns":
            require(self.dns_ready(snapshot), "CoreDNS must be fully Ready before API/framework bootstrap")
        state, pod = self.target_state(snapshot, target)
        receipt = {**target, "state": state, "delete_attempted": False}
        self.summary["pod_moves"].append(receipt)
        self.save()
        if state != "delete-pinned":
            receipt["ready_pod_uid"] = object_uid(pod)
            receipt["ready_node"] = pod["spec"]["nodeName"]
            group = self.bindings[target_key(target)]["group"]
            residents = self.owned_pods(snapshot, target) if group["group_adopted"] else [pod]
            for resident in residents:
                if pod_ready(resident):
                    self.commit_memory(target, resident)
            self.save()
            return
        self.authority()
        self.models()
        snapshot = self.snapshot()
        self.guard(snapshot)
        self.target_state(snapshot, target)
        self.qualify_capacity(snapshot, target)
        self.add_exclusions()
        self.probe_ip(self.snapshot(), target)
        self.authority()
        self.models()
        snapshot = self.snapshot()
        self.guard(snapshot)
        self.require_exclusions(snapshot)
        self.qualify_capacity(snapshot, target)
        state, pod = self.target_state(snapshot, target)
        if state == "delete-pinned":
            self.delete_exact(target["namespace"], target["pod_name"], target["pod_uid"], receipt)
            deadline = min(self.work_deadline, time.monotonic() + POD_READY_SECONDS)
            while True:
                current = self.snapshot()
                self.guard(current)
                self.require_exclusions(current)
                require(self.system_ready(current), "Recovered Cilium/CNS/system DaemonSets regressed during Pod exchange")
                state, pod = self.target_state(current, target, after_delete=True)
                if state != "waiting":
                    break
                self.wait(deadline, "Pinned Pod replacement readiness")
            receipt["state"] = "moved"
        else:
            receipt["state"] = state
        receipt["ready_pod_uid"] = object_uid(pod)
        receipt["ready_node"] = pod["spec"]["nodeName"]
        self.commit_memory(target, pod)
        self.save()

    def cleanup_exclusions(self):
        remaining = []
        for entry in reversed(self.exclusions):
            try:
                node = self.kube("get", "node", entry["name"], "-o", "json")
                require(object_uid(node) == entry["uid"], "Exclusion cleanup must not touch a replacement Node UID")
                taint = {"key": EXCLUSION_KEY, "value": entry["token"], "effect": "NoSchedule"}
                taints = maintenance._taints(node)
                if taint in taints:
                    require(not entry.get("cleanup_attempted"), "Exclusion cleanup mutation cannot be repeated")
                    index = taints.index(taint)
                    entry["cleanup_attempted"] = True
                    self.save()
                    self.write(["kubectl", "patch", "node", entry["name"], "--type=json", "-p", json.dumps([
                        {"op": "test", "path": "/metadata/uid", "value": entry["uid"]},
                        {"op": "test", "path": f"/spec/taints/{index}", "value": taint},
                        {"op": "remove", "path": f"/spec/taints/{index}"},
                    ])], cleanup=True)
                current = self.kube("get", "node", entry["name"], "-o", "json")
                require(object_uid(current) == entry["uid"] and not any(
                    row.get("key") == EXCLUSION_KEY for row in maintenance._taints(current)
                ), "Placement exclusion cleanup is uncertain or foreign")
            except EXPECTED_ERRORS as error:
                self.summary["cleanup_errors"].append(str(error))
                remaining.append(entry)
        self.exclusions = list(reversed(remaining))
        self.summary["temporary_exclusions"] = self.exclusions
        self.save()

    def cleanup(self):
        if not self.args.execute:
            require(self.probe is None and not self.exclusions, "Read-only execution acquired mutation cleanup state")
            return
        if self.cleanup_done:
            return
        self.cleanup_done = True
        self.cleanup_mode = True
        try:
            try:
                self.cleanup_probe()
            except EXPECTED_ERRORS as error:
                self.summary["cleanup_errors"].append(str(error))
            self.cleanup_exclusions()
        finally:
            self.cleanup_mode = False
            self.save()

    def postproof(self):
        deadline = min(self.work_deadline, time.monotonic() + POSTPROOF_SECONDS)
        while True:
            snapshot = self.snapshot()
            nodes, agents = self.guard(snapshot)
            require(workers.node_is_ready(nodes[self.host_node]) and self.system_ready(snapshot),
                    "Recovered host/system readiness regressed")
            require(self.dns_ready(snapshot), "CoreDNS regressed before strict postproof")
            require(not self.exclusions and self.probe is None and not self.summary["cleanup_errors"],
                    "Owned cleanup must be certain before successful postproof")
            for node in nodes.values():
                require(not any(row.get("key") == EXCLUSION_KEY for row in maintenance._taints(node)),
                        "A placement exclusion remains")
            for target, receipt in zip(self.targets, self.summary["pod_moves"]):
                _, pod = self.target_state(snapshot, target)
                require(pod_ready(pod) and object_uid(pod) == receipt["ready_pod_uid"],
                        "A recovered framework/API Pod changed or regressed")
            self.models()
            self.prove_memory(nodes[self.host_node], self.node_metric(), 0)
            _, connected = self.authority()
            proof = maintenance._read_cilium_proof(self, self.args, self.identities)
            self.summary["cilium_proof"] = proof
            fatal_error = str(proof.get("fatal_error") or "")
            require(not AUTH_ERROR.search(fatal_error),
                    "Cilium authorization failure must not be retried during postproof")
            require(not fatal_error or arm.TRANSIENT_READ_RE.search(fatal_error)
                    or "Cilium agent Pod is not Running/Ready" in fatal_error,
                    "Nontransient Cilium read/schema failure must not be retried during postproof")
            covered = proof.get("covered_node_names") or []
            expected_cilium = controller(snapshot, "DaemonSet", "kube-system", "cilium")
            live_agents = [
                pod for pod in mocks._items(snapshot["pods"], "Cilium agents")
                if pod["metadata"].get("namespace") == "kube-system"
                and (pod["metadata"].get("labels") or {}).get("k8s-app") == "cilium"
            ]
            for pod in live_agents:
                owner = controller_owner(pod, "DaemonSet")
                require(owner["name"] == "cilium" and owner["uid"] == object_uid(expected_cilium),
                        "Cilium peer proof includes a foreign agent owner")
            good = (
                proof.get("healthy") is True and proof.get("cilium_agent_count") == 3
                and len(proof.get("agents") or []) == 3 and set(covered) == set(self.real_uids)
                and len(live_agents) == 3
                and {pod["metadata"]["name"] for pod in live_agents}
                == {row["pod_name"] for row in proof.get("agents") or []}
            )
            self.save()
            if connected and good:
                self.authority(strict=True)
                self.summary["final_mock_ready"] = sum(pod_ready(pod) for pod in agents.values())
                self.summary["final_mock_pending"] = sum(
                    (pod.get("status") or {}).get("phase") == "Pending" for pod in agents.values()
                )
                self.summary["final_kwok_ready"] = 100
                self.summary["phase1_only"] = True
                self.summary["workloads_ready"] = False
                self.save()
                return
            self.wait(deadline, "Strict three-agent 99-peer and 100 literally Connected Fleet postproof")

    def clear_marker(self):
        if not self.marker:
            return
        node = self.kube("get", "node", PROM_NODE, "-o", "json")
        require(object_uid(node) == REAL_UIDS[PROM_NODE] and workers.node_is_ready(node)
                and node_boot(node) != self.summary["restart"]["previous_boot_id"]
                and node_boot(node) == self.summary["restart"]["current_boot_id"]
                and maintenance._annotations(node).get(MARKER_KEY) == self.marker,
                "Only the exact successful owned restart marker can be cleared")
        path = f"/metadata/annotations/{MARKER_KEY.replace('/', '~1')}"
        self.write(["kubectl", "patch", "node", PROM_NODE, "--type=json", "-p", json.dumps([
            {"op": "test", "path": "/metadata/uid", "value": REAL_UIDS[PROM_NODE]},
            {"op": "test", "path": path, "value": self.marker},
            {"op": "remove", "path": path},
        ])])
        node = self.kube("get", "node", PROM_NODE, "-o", "json")
        require(object_uid(node) == REAL_UIDS[PROM_NODE] and MARKER_KEY not in maintenance._annotations(node),
                "Successful restart marker cleanup is uncertain")
        self.summary["restart"]["marker_removed"] = True
        self.save()

    def execute(self):
        self.authority()
        if getattr(self.args, "observe_accepted_action", None):
            self.observe_accepted_action()
            return
        self.models(allow_failed_os=self.host_action == "reimage")
        self.open_cluster()
        self.initial = self.snapshot()
        nodes, agents = self.guard(self.initial)
        self.prior_marker(nodes[PROM_NODE])
        if not workers.node_is_ready(nodes[PROM_NODE]):
            self.guard(self.initial, host_unready=True)
            require(self.host_action != "reimage" or self.summary["os_reimage_eligible"],
                    "OS reimage is restricted to the exact diagnosed OS provisioning failure")
        decisions = []
        for target in self.targets:
            state, _ = self.target_state(self.initial, target)
            decisions.append({**target, "decision": state})
        dns_is_ready = self.dns_ready(self.initial)
        require(dns_is_ready or sum(row["deployment_name"] == "coredns" for row in self.targets) == 2,
                "Both pinned CoreDNS recoveries are required before API bootstrap when DNS is not fully Ready")
        self.summary.update({
            "plan_valid": True, "effective_targets": self.targets,
            "planned_actions": {
                "restart_required": not workers.node_is_ready(nodes[PROM_NODE]),
                "host_action": self.host_action,
                "vmss_name": PROM_VMSS, "instance_ids": ["0"], "pods": decisions,
            },
            "controller_pins": frozen_controllers(self.initial), "pdb_pins": frozen_pdbs(self.initial),
            "initial_mock_ready": sum(pod_ready(pod) for pod in agents.values()),
            "capacity_and_actual_ip_proof_required_at_execution": True,
        })
        self.save()
        if not self.args.execute:
            self.summary["status"] = "plan_valid"
            return
        self.restart_host(self.snapshot())
        for target in self.targets:
            self.move_pod(target)
        self.cleanup()
        require(not self.summary["cleanup_errors"], "Resource cleanup failed; recovery cannot be certified")
        self.postproof()
        try:
            self.clear_marker()
        except EXPECTED_ERRORS as error:
            self.summary["cleanup_errors"].append(f"Restart marker cleanup failed: {error}")
            raise
        self.summary.update({"repaired": True, "status": "repaired"})

    def observe_accepted_action(self):
        """Observe a proven accepted reimage without submitting any resource write."""

        require(not self.args.execute and self.host_action == "reimage",
                "Accepted reimage observation is strictly read-only")
        with open(self.args.observe_accepted_action, encoding="utf-8") as handle:
            prior = json.load(handle)
        action, marker = validate_accepted_reimage(prior, self.summary["plan_sha256"])
        self.summary["observation_only"] = True
        self.summary["observed_action"] = {
            "action": "reimage", "accepted_at": action["requested_at"],
            "marker_token": marker["token"], "node_uid": REAL_UIDS[PROM_NODE],
            "vm_id": FAILED_PROM_VM_ID, "previous_boot_id": marker["previous_boot_id"],
        }
        self.summary["restart"]["requested_at"] = action["requested_at"]
        self.save()
        self.open_cluster()
        self.initial = self.snapshot()
        expected_marker = json.dumps(marker, sort_keys=True, separators=(",", ":"))
        while True:
            stable = self.models(restarting=True)
            current_vm = self.summary["arm_metadata"]["instances"][PROM_NODE]
            require(current_vm["vm_id"] == FAILED_PROM_VM_ID,
                    "The accepted reimage target VM identity changed")
            snapshot = self.snapshot()
            nodes, _ = self.guard(snapshot)
            host = nodes[PROM_NODE]
            require(maintenance._annotations(host).get(MARKER_KEY) == expected_marker,
                    "The accepted-action marker changed or disappeared")
            boot = node_boot(host)
            ready = workers.node_is_ready(host)
            self.summary["observed_action"].update({
                "current_boot_id": boot, "node_ready": ready,
                "models_stable": stable, "observed_at": workers.utc_now(),
            })
            self.save()
            if stable and ready and boot != marker["previous_boot_id"] and self.system_ready(snapshot):
                self.summary.update({
                    "status": "plan_valid", "plan_valid": True,
                    "observed_host_ready": True, "repaired": False,
                })
                return
            self.wait(self.work_deadline, "Read-only accepted-reimage observation")


def execute_recovery(args, summary: dict, runner=workers.run_command, delete_pod=None):
    """Persist truthful failure receipts and always clean only this run's resources."""

    validate_paths(args)
    summary.update({
        "status": "reading", "execute": bool(args.execute), "plan_valid": False, "repaired": False,
        "success": False,
        "mutation_started": False, "restart": {"attempted": False, "accepted": False, "ambiguous": False},
        "pod_moves": [], "cleanup_errors": [], "started_at": workers.utc_now(),
        "phase1_only": True, "workloads_ready": False,
    })
    operator = None
    private = None
    previous_tempdir = tempfile.tempdir
    previous_environment = {name: os.environ.get(name) for name in ("TMPDIR", "TEMP", "TMP")}
    try:
        validate_args(args)
        plan = load_plan(args.plan_file)
        summary["plan_sha256"] = digest(plan)
        args.role, args.context, args.request_timeout_seconds = ROLE, CLUSTER, 45
        candidate = Path(f".unreachable-prom-private-{uuid.uuid4().hex}")
        candidate.mkdir(mode=0o700)
        private = candidate
        # Azure CLI and the Kubernetes client's certificate files must also stay private.
        for name in previous_environment:
            os.environ[name] = str(private.resolve())
        tempfile.tempdir = str(private.resolve())
        config_path = private / "cluster.config"
        config_path.touch(mode=0o600, exist_ok=False)
        args.kubeconfig = str(config_path)
        operator_type = Recovery
        if getattr(args, "replace_failed_host", None):
            # The subclass is loaded only after this base module is fully initialized.
            from failed_prom_worker_replacement import ReplacementRecovery  # pylint: disable=import-outside-toplevel,cyclic-import
            operator_type = ReplacementRecovery
            if getattr(args, "resume_replacement", None):
                from failed_prom_capacity_resume import CapacityResumeRecovery  # pylint: disable=import-outside-toplevel,cyclic-import
                operator_type = CapacityResumeRecovery
        operator = operator_type(args, plan, summary, runner, delete_pod or mocks.delete_pod_with_uid_precondition)
        operator.execute()
    finally:
        error = sys.exc_info()[1]
        if error is not None:
            summary["error"] = str(error)
            summary["status"], summary["repaired"] = "failed", False
        try:
            if operator is not None:
                operator.cleanup()
        except EXPECTED_ERRORS as cleanup_error:
            summary["cleanup_errors"].append(f"Resource cleanup finalization failed: {cleanup_error}")
        finally:
            finalization_error = sys.exc_info()[1]
            if finalization_error is not None:
                summary["status"], summary["repaired"] = "failed", False
                summary.setdefault("error", str(finalization_error))
            if private is not None:
                try:
                    shutil.rmtree(private)
                except OSError as cleanup_error:
                    summary["cleanup_errors"].append(f"Private credential-file cleanup failed: {cleanup_error}")
            tempfile.tempdir = previous_tempdir
            for name, value in previous_environment.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
            if summary["cleanup_errors"]:
                summary["status"], summary["repaired"] = "failed", False
            summary["success"] = not summary["cleanup_errors"] and (
                (
                    summary["status"] == "plan_valid" and summary["execute"] is False
                    and summary["mutation_started"] is False and summary["plan_valid"] is True
                )
                or (summary["status"] == "repaired" and summary["execute"] is True and summary["repaired"] is True)
            )
            summary["finished_at"] = workers.utc_now()
            mocks.write_json_atomic(args.summary_file, summary)
    require(not summary["cleanup_errors"], "Cleanup was uncertain; inspect the persisted recovery summary")


def parse_args(argv: Optional[Sequence[str]] = None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resource-group", required=True)
    parser.add_argument("--confirm-resource-group", required=True)
    parser.add_argument("--expected-subscription", required=True)
    parser.add_argument("--expected-region", required=True)
    parser.add_argument("--expected-tfvars-sha", required=True)
    parser.add_argument("--plan-file", required=True)
    parser.add_argument("--summary-file", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--reimage-failed-os", action="store_true")
    parser.add_argument("--observe-accepted-action")
    parser.add_argument("--replace-failed-host")
    parser.add_argument("--resume-replacement")
    parser.add_argument("--quota-wait-seconds", type=int, default=900)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    summary = {}

    def interrupted(signum, _frame):
        raise workers.ReconcileError(f"Recovery interrupted by signal {signum}")

    previous = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        execute_recovery(args, summary)
        print(f"{ROLE}: {summary['status']}; phase 1 is not authorization to run workloads.", flush=True)
        return 0
    except (*EXPECTED_ERRORS, ValueError, TypeError, KeyError) as error:
        print(f"{ROLE}: recovery failed closed: {error}", file=sys.stderr, flush=True)
        return 1
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    sys.modules["unreachable_prom_worker_recovery"] = sys.modules[__name__]
    raise SystemExit(main())
