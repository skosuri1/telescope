#!/usr/bin/env python3
"""One approved normal restart of mesh-96 default VM1, never cluster recovery."""

# pylint: disable=protected-access,too-many-lines,too-many-boolean-expressions

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
from datetime import datetime, timezone
from pathlib import Path

import cni_worker_maintenance as maintenance
import mock_cni_recovery as mocks
import prepared_worker_retirement as prepared
import preserved_worker_reconcile as workers
import unreachable_prom_worker_recovery as base


TARGET = f"{base.DEFAULT_VMSS}000001"
SOURCE = base.SOURCE_NODE
VM_IDS = {
    SOURCE: "c9f1454f-144e-4281-916b-7612f89a31e6",
    TARGET: "d81b78a9-fe40-468d-91ec-d66f0456bfa7",
}
BOOTS = {
    SOURCE: "fbecbcc6-d934-4c29-adb5-1331497b3706",
    TARGET: "e84bfbf6-e392-48eb-b634-e52ad9ef945e",
}
PLAN_SHA = "1a4385e2db5a0a5b38d750a6a82fcb9b3d4c683e801bf12bd8111c75355e380a"
FREEZE_EVENT = "FCC75884-DB5F-44AA-83BF-4FE8FD23B6B0"
JOURNAL = "retained-worker-restart-mesh96-instance1"
OWNER = "stalled-retained-worker-recovery"
NAMESPACE = "kube-system"
COMPLETED_CERT_POD = "hubble-generate-certs-9a7a0d0e-zq9m9"
COMPLETED_CERT_UID = "a47d492a-bdc7-43bf-b0e5-ec330ccff287"
EXPECTED_ERRORS = base.EXPECTED_ERRORS + (ValueError, TypeError, KeyError, IndexError)
PRE_SUBMIT_BUILD = 79945
PRE_SUBMIT_COMMIT = "dc520ca5f67cca6871de1c2b12043f8cd2d681cf"
PRE_SUBMIT_JOURNAL_UID = "55117c2c-db51-41b2-b8ec-12fc9d357501"
SECURITY_DAEMONSET_UID = "c7fcc5e0-f23c-4399-85fd-c4a3926e22b4"
READINESS_TOLERATION = {"effect": "NoSchedule", "key": "node.cilium.io/agent-not-ready", "operator": "Exists"}
REQUIRED_FILES = (
    "current-nodes.json", "current-pods.json", "current-controllers.json", "current-pdbs.json",
    "current-nnc.json", "default-instances.json", "preserved-group.json", "cluster.json",
    "quota-observation.json", "prior-native-action.json", "default-0-instance-view.json",
    "default-1-instance-view.json", "pool-configuration.json", "vmsses.json",
)
VIEW_QUERY = (
    "{statuses:statuses[].{code:code,time:time},"
    "vmAgent:{statuses:vmAgent.statuses[].{code:code,displayStatus:displayStatus,message:message,time:time}},"
    "extensions:extensions[].{name:name,statuses:statuses[].{code:code}}}"
)
HEALTH_TAINTS = {"node.kubernetes.io/unreachable", "node.kubernetes.io/not-ready"}
CHECKSUM_KEYS = {
    "kubernetes.azure.com/cilium-envoy-configmap-checksum", "kubernetes.azure.com/fqdn-policy-configmap-checksum",
    "kubernetes.azure.com/azure-cns-configmap-checksum", "cilium.io/cilium-configmap-checksum",
    "checksum/ce-info", "checksum/tenant-config", "acn.azure.com/retina-configmap-checksum",
    "acn.azure.com/retina-config-win-checksum",
}
require = prepared.require
digest = base.digest
uid = base.object_uid


def file_hashes(directory):
    root = Path(directory).resolve()
    require(root.is_dir(), "The exact observation artifact directory is required")
    files = sorted(root.iterdir())
    require(files and all(path.is_file() and not path.is_symlink() for path in files),
            "Observation inputs must be regular, non-symlink files")
    require(set(REQUIRED_FILES) <= {path.name for path in files}, "Observation artifact is incomplete")
    hashes = {}
    for path in files:
        require(path.stat().st_size <= 32 * 1024 * 1024, "Observation file exceeds its bound")
        hashes[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return hashes


def read_json(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "Duplicate JSON field in observation input")
            result[key] = value
        return result
    return json.loads(Path(path).read_text(encoding="utf-8"), object_pairs_hook=unique)


def logical_node(node):
    spec = copy.deepcopy(node.get("spec") or {})
    if str(spec.get("providerID", "")).lower().startswith("azure:///"):
        spec["providerID"] = spec["providerID"].lower()
    spec["taints"] = [
        row for row in spec.get("taints") or []
        if not (node["metadata"]["name"] != SOURCE
                and row.get("key") in HEALTH_TAINTS and row.get("effect") in ("NoSchedule", "NoExecute")
                and not row.get("value"))
        and not (node["metadata"]["name"] == TARGET and row.get("key") == "node.cilium.io/agent-not-ready"
                 and row.get("effect") == "NoSchedule" and not row.get("value"))
    ]
    return {"uid": uid(node), "spec": spec, "labels": node["metadata"].get("labels") or {}}


def controllers_pin(payload):
    result = {}
    for row in mocks._items(payload, "controller inventory"):
        metadata = row["metadata"]
        key = f"{row['kind']}/{metadata.get('namespace')}/{metadata['name']}"
        spec = copy.deepcopy(row["spec"])
        if row["kind"] == "DaemonSet" and metadata.get("namespace") == NAMESPACE:
            annotations = spec.get("template", {}).get("metadata", {}).get("annotations", {})
            for name in CHECKSUM_KEYS:
                annotations.pop(name, None)
            if metadata.get("name") == "azuresecuritylinuxagent" and uid(row) == SECURITY_DAEMONSET_UID:
                pod_spec = spec.get("template", {}).get("spec", {})
                tolerations = pod_spec.get("tolerations") or []
                require(isinstance(tolerations, list) and tolerations.count(READINESS_TOLERATION) <= 1,
                        "Managed security-agent readiness toleration is ambiguous")
                if READINESS_TOLERATION in tolerations:
                    pod_spec["tolerations"] = [item for item in tolerations if item != READINESS_TOLERATION]
        require(key not in result and uid(row), "Controller inventory is ambiguous")
        result[key] = {"uid": uid(row), "functional_spec_sha256": digest(spec)}
    return result


def ready_condition(node):
    rows = [row for row in node.get("status", {}).get("conditions", []) if row.get("type") == "Ready"]
    require(len(rows) == 1, "Node Ready condition is ambiguous")
    return rows[0]


def fresh_node_heartbeat(node):
    condition = ready_condition(node)
    age = (datetime.now(timezone.utc) - base.timestamp(condition.get("lastHeartbeatTime"), "Node heartbeat")).total_seconds()
    return 0 <= age <= 360


def fresh_node_ready(node):
    return workers.node_is_ready(node) and fresh_node_heartbeat(node)


def guest_state(view, *, initializing=False, max_age_seconds=None):
    rows = (view.get("vmAgent") or {}).get("statuses")
    if initializing and (rows is None or rows == []):
        return "unready"
    require(isinstance(rows, list) and len(rows) == 1, "VM Guest Agent observation is missing or ambiguous")
    row = rows[0]
    observed = base.timestamp(row.get("time"), "VM Guest Agent observation")
    require((observed - datetime.now(timezone.utc)).total_seconds() <= 30, "VM Agent observation is future-dated")
    if max_age_seconds is not None and not 0 <= (datetime.now(timezone.utc) - observed).total_seconds() <= max_age_seconds:
        return "unready"
    if row.get("code") == "ProvisioningState/succeeded" and row.get("displayStatus") == "Ready":
        return "ready"
    if row.get("code") == "ProvisioningState/Unavailable" and "unresponsive" in str(row.get("message", "")).lower():
        return "unresponsive"
    return "unready"


def extensions_ready(view):
    rows = view.get("extensions")
    return isinstance(rows, list) and bool(rows) and all(
        row.get("name") and isinstance(row.get("statuses"), list) and row["statuses"]
        and all(status.get("code") == "ProvisioningState/succeeded" for status in row["statuses"])
        for row in rows
    )


def pod_pin(pod):
    return {"uid": uid(pod), "name": pod["metadata"]["name"], "namespace": pod["metadata"]["namespace"],
            "node_name": pod["spec"].get("nodeName"), "spec_sha256": digest(pod["spec"]),
            "owners": pod["metadata"].get("ownerReferences") or []}


def arm_canonical(value):
    if isinstance(value, dict):
        return {key: arm_canonical(item) for key, item in value.items()}
    if isinstance(value, list):
        return [arm_canonical(item) for item in value]
    if isinstance(value, str) and value.lower().startswith(("/subscriptions/", "azure:///subscriptions/")):
        return value.lower()
    return value


def safe_diagnostics(value):
    """Never serialize kubeconfigs, secrets, extension stdout, or inline secret env values."""
    if isinstance(value, list):
        return [safe_diagnostics(row) for row in value]
    if not isinstance(value, dict):
        return value
    result = {}
    for key, item in value.items():
        if key in ("managedFields", "kubectl.kubernetes.io/last-applied-configuration"):
            continue
        if key == "value" and re.search(r"secret|password|token|credential|private.?key",
                                        str(value.get("name", "")), re.I):
            result[key] = "<redacted>"
        else:
            result[key] = safe_diagnostics(item)
    return result


def load_source(args):
    hashes = file_hashes(args.source_state_directory)
    data = {name: read_json(Path(args.source_state_directory) / name) for name in REQUIRED_FILES}
    observation = data["quota-observation.json"]
    require(observation.get("observation_only") is True and observation.get("mutation_started") is False
            and observation.get("worker_state_collected") is True and observation.get("source_native_build") == 79894,
            "Input must be the read-only native-79894 worker observation, not an action receipt")
    if observation.get("observation_complete") is not True:
        path = Path(args.source_state_directory) / "default-1-resource-health-read.log"
        require(path.name in hashes, "Incomplete observation lacks its exact resource-health failure")
        error = path.read_text(encoding="utf-8")
        require("UnsupportedResourceType" in error and "Unprocessable Entity" in error
                and not base.AUTH_ERROR.search(error), "Only the known nonauthorization ResourceHealth 422 is permitted")
    prior = data["prior-native-action.json"]
    replacement = prior.get("replacement") or {}
    removal = replacement.get("native_removal") or {}
    delete = replacement.get("delete") or {}
    require(prior.get("plan_sha256") == PLAN_SHA and delete.get("attempted") is True
            and delete.get("accepted") is True and delete.get("ambiguous") is False
            and removal.get("old_node_pods_nnc_absent") is True and removal.get("pool_count") == 0
            and removal.get("vmss_capacity") == 0
            and (replacement.get("marker") or {}).get("vm_id") == base.FAILED_PROM_VM_ID,
            "Original native empty-prompool lineage is not proven")
    cluster = data["cluster.json"]
    cluster_id = (f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.RESOURCE_GROUP}"
                  f"/providers/Microsoft.ContainerService/managedClusters/{base.CLUSTER}")
    require(prepared.resource_equal(cluster.get("id"), cluster_id)
            and str(cluster.get("nodeResourceGroup", "")).lower() == base.NODE_GROUP
            and str(cluster.get("location", "")).lower() == base.REGION,
            "Observation cluster/Node RG scope changed")
    group = data["preserved-group.json"]
    require(prepared.resource_equal(group.get("id"),
                                   f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.RESOURCE_GROUP}"),
            "Observation preserved RG scope changed")
    nodes = {row["metadata"]["name"]: row for row in data["current-nodes.json"]["items"]}
    require(set(nodes) == maintenance.EXPECTED_AGENT_NAMES | {SOURCE, TARGET}, "Observation Node inventory is not exact")
    for name in (SOURCE, TARGET):
        node = nodes[name]
        require(uid(node) == base.REAL_UIDS[name] and base.node_boot(node) == BOOTS[name],
                "Observation default Node UID or last boot differs from the approved event")
        maintenance._validate_real_node_scope(node, subscription=base.SUBSCRIPTION, node_resource_group=base.NODE_GROUP)
        require(workers.provider_identity(node) == (base.DEFAULT_VMSS, "0" if name == SOURCE else "1"),
                "Observation provider points to a different VM")
    freeze = [row for row in nodes[TARGET]["status"]["conditions"] if row.get("type") == "VMEventScheduled"]
    require(len(freeze) == 1 and freeze[0].get("status") == "True"
            and FREEZE_EVENT in str(freeze[0].get("message")) and "Freeze Started" in freeze[0]["message"],
            "The captured approved live-migration freeze evidence is missing")
    require(guest_state(data["default-1-instance-view.json"]) == "unresponsive",
            "The captured target Guest Agent is not the approved unresponsive host")
    instances = data["default-instances.json"]
    require(isinstance(instances, list) and len(instances) == 2, "Observation default VM inventory changed")
    for row in instances:
        name = row.get("computerName")
        require(name in VM_IDS and row.get("vmId") == VM_IDS[name]
                and str(row.get("instanceId")) == ("0" if name == SOURCE else "1")
                and prepared.resource_equal("azure://" + str(row.get("id")), nodes[name]["spec"]["providerID"]),
                "Observation VM identity changed")
        native = prior.get("original_model_pins", {}).get("defaults", {}).get(name) or {}
        require(native.get("vm_id") == VM_IDS[name], "Original native default VM pin changed")
    controller = base.controller({"controllers": data["current-controllers.json"]},
                                 "StatefulSet", mocks.DEFAULT_NAMESPACE, "kwok-node")
    native_controller = prior["controller_pins"].get("StatefulSet/mock-clustermesh/kwok-node") or {}
    require(uid(controller) == native_controller.get("uid")
            and digest(controller["spec"]) == native_controller.get("spec_sha256")
            and controller["spec"].get("replicas") == 100, "Original mock StatefulSet identity/spec changed")
    agents = maintenance._agent_map(data["current-pods.json"])
    require(set(agents) == maintenance.EXPECTED_AGENT_NAMES and len({uid(pod) for pod in agents.values()}) == 100,
            "Observation must contain exactly 100 logical mock-agent UIDs")
    target_names = {name for name, pod in agents.items() if pod["spec"].get("nodeName") == TARGET}
    source_names = set(agents) - target_names
    require(len(target_names) == 56 and len(source_names) == 44
            and all(agents[name]["metadata"].get("deletionTimestamp") for name in target_names)
            and all(agents[name]["spec"].get("nodeName") == SOURCE
                    and not agents[name]["metadata"].get("deletionTimestamp") for name in source_names)
            and sum(base.pod_ready(agents[name]) for name in source_names) == 38,
            "Observation is not the renewed 38-Ready/6-Pending/56-Terminating identity plan")
    require(file_hashes(args.source_state_directory) == hashes, "Observation files changed while loading")
    return data, hashes


def load_pre_submit_checkpoint(args, data, hashes):
    path = getattr(args, "resume_checkpoint", None)
    if not path:
        return None, ""
    checkpoint_path = Path(path)
    require(checkpoint_path.is_file() and not checkpoint_path.is_symlink()
            and checkpoint_path.stat().st_size <= 32 * 1024 * 1024, "Prior checkpoint must be a bounded regular file")
    checksum = hashlib.sha256(checkpoint_path.read_bytes()).hexdigest()
    prior = read_json(checkpoint_path)
    restart, journal = prior.get("restart") or {}, prior.get("journal") or {}
    require(getattr(args, "resume_build_id", 0) == PRE_SUBMIT_BUILD
            and prior.get("execute") is True and prior.get("mutation_started") is True
            and prior.get("plan_valid") is True and prior.get("success") is False
            and prior.get("host_recovered") is False and prior.get("workloads_ready") is False
            and prior.get("status") == "failed-closed"
            and prior.get("error") == "ReconcileError: Captured functional controller specs/UIDs changed"
            and prior.get("plan_sha256") == PLAN_SHA and prior.get("source_hashes") == hashes
            and prior.get("source_state_sha256") == digest(hashes)
            and prior.get("original_identity") == {
                "node_name": TARGET, "node_uid": base.REAL_UIDS[TARGET],
                "vm_id": VM_IDS[TARGET], "boot_id": BOOTS[TARGET],
            },
            "Only build 79945's proven pre-POST controller guard failure may continue")
    require(restart.get("attempted") is True and restart.get("accepted") is None
            and restart.get("ambiguous") is True and "accepted_at" not in restart
            and "submission_started" not in restart and not restart.get("host_proven")
            and not restart.get("reboot_fenced") and restart.get("command") == Recovery.restart_command()
            and restart.get("previous_boot_id") == BOOTS[TARGET]
            and journal.get("uid") == PRE_SUBMIT_JOURNAL_UID and journal.get("name") == JOURNAL
            and journal.get("namespace") == NAMESPACE and journal.get("create_attempted") is True
            and journal.get("accepted") is True and journal.get("ambiguous") is False,
            "An accepted, submitted, or differently owned restart must never be replayed")
    require(base.timestamp(prior.get("started_at"), "prior execution")
            <= base.timestamp(restart.get("requested_at"), "prior reservation")
            <= base.timestamp(prior.get("finished_at"), "prior guard failure")
            <= datetime.now(timezone.utc), "Prior reservation timestamps are invalid")
    snapshot = prior.get("current_kubernetes_diagnostics") or {}
    # The saved diagnostics redact environment values; the live guard still compares raw configuration.
    require(controllers_pin(snapshot["controllers"]) == controllers_pin(safe_diagnostics(data["current-controllers.json"]))
            and base.frozen_pdbs(snapshot) == base.frozen_pdbs({"pdbs": data["current-pdbs.json"]}),
            "Prior drift is not confined to the specifically understood managed-controller variation")
    require(hashlib.sha256(checkpoint_path.read_bytes()).hexdigest() == checksum,
            "Prior checkpoint changed while validating its no-submission proof")
    return prior, checksum


class Recovery(maintenance.ClusterOperator):
    """No write path other than the exclusive journal and one explicit VM1 restart."""

    def __init__(self, args, data, hashes, summary, runner, *, resume=None, resume_hash=""):
        deadline = time.monotonic() + args.timeout_seconds
        super().__init__(args, base.CLUSTER, runner, deadline - 60, deadline)
        self.data, self.hashes, self.summary = data, hashes, summary
        self.token, self.journal_uid = uuid.uuid4().hex, ""
        self.nodes = {row["metadata"]["name"]: row for row in data["current-nodes.json"]["items"]}
        self.agents = maintenance._agent_map(data["current-pods.json"])
        self.target_names = {name for name, pod in self.agents.items() if pod["spec"]["nodeName"] == TARGET}
        self.kwok_uids = {name: uid(self.nodes[name]) for name in maintenance.EXPECTED_AGENT_NAMES}
        self.mock_uid = uid(base.controller({"controllers": data["current-controllers.json"]},
                                           "StatefulSet", mocks.DEFAULT_NAMESPACE, "kwok-node"))
        self.controller_pins = controllers_pin(data["current-controllers.json"])
        self.pdb_pins = base.frozen_pdbs({"pdbs": data["current-pdbs.json"]})
        self.nnc_pins = maintenance._nnc_map(data["current-nnc.json"])
        self.protected = {
            uid(pod): pod_pin(pod) for pod in data["current-pods.json"]["items"]
            if pod["spec"].get("nodeName") == SOURCE and base.pod_ready(pod)
        }
        self.source_pins = {name: pod_pin(pod) for name, pod in self.agents.items() if name not in self.target_names}
        self.initial_pods = {uid(pod): pod for pod in data["current-pods.json"]["items"]}
        self.allowed_owners = set()
        self.target_daemonsets = set()
        for pod in data["current-pods.json"]["items"]:
            refs = [ref for ref in pod["metadata"].get("ownerReferences", []) if ref.get("controller") is True]
            if len(refs) == 1:
                self.allowed_owners.add((pod["metadata"]["namespace"], refs[0]["kind"], refs[0]["name"], refs[0]["uid"]))
                if pod["spec"].get("nodeName") == TARGET and refs[0]["kind"] == "DaemonSet":
                    self.target_daemonsets.add((pod["metadata"]["namespace"], refs[0]["name"], refs[0]["uid"]))
        self.arm_pin = None
        self.allow_replacements = False
        self.replacement_pins = {}
        self.pre_restart_boot = ""
        self.restart_sent = False
        self.persisted_journal_data = None
        self.resume, self.resume_hash = resume, resume_hash

    def save(self):
        mocks.write_json_atomic(self.args.summary_file, self.summary)

    def unchanged_inputs(self):
        require(file_hashes(self.args.source_state_directory) == self.hashes, "Immutable observation input hashes changed")
        if self.resume:
            require(hashlib.sha256(Path(self.args.resume_checkpoint).read_bytes()).hexdigest() == self.resume_hash,
                    "Immutable prior reservation checkpoint changed")

    def run(self, command, timeout_seconds=45, *, cleanup=False):
        command = list(command)
        allowed = command[0] == "az" and (
            command[1:3] in (["account", "show"], ["group", "show"], ["aks", "list"],
                            ["aks", "show"], ["vmss", "list"], ["vmss", "list-instances"], ["vmss", "get-instance-view"])
            or command[1:4] in (["fleet", "member", "list"], ["aks", "nodepool", "list"],
                               ["aks", "operation", "show-latest"])
        )
        if command[0] == "kubectl":
            allowed = "get" in command and not any(word in command for word in ("delete", "patch", "create", "exec", "run"))
        require(allowed, "Unsupported command on the read-only path")
        return super().run(command, min(timeout_seconds, 45), cleanup=cleanup)

    def kube(self, *command):
        return workers.parse_json(self.run(["kubectl", "--request-timeout=45s", *command]), "retained-worker read")

    def snapshot(self):
        require(self.run(["kubectl", "--request-timeout=45s", "get", "--raw=/readyz"]).strip() == "ok",
                "Kubernetes API is not ready")
        return {
            "nodes": self.kube("get", "nodes", "-o", "json"),
            "pods": self.kube("get", "pods", "-A", "-o", "json"),
            "controllers": self.kube("get", "deployments,replicasets,daemonsets,statefulsets", "-A", "-o", "json"),
            "pdbs": self.kube("get", "pdb", "-A", "-o", "json"),
            "nnc": self.kube("get", "nodenetworkconfigs", "-n", NAMESPACE, "-o", "json"),
        }

    def authority(self):
        account = self.az_json("account", "show", "--query", "{id:id}")
        require(str(account.get("id", "")).lower() == base.SUBSCRIPTION, "Current subscription changed")
        group = self.az_json("group", "show", "--name", base.RESOURCE_GROUP)
        clusters = self.az_json("aks", "list", "--resource-group", base.RESOURCE_GROUP, "--query", base.CLUSTER_QUERY)
        members = self.az_json("fleet", "member", "list", "--resource-group", base.RESOURCE_GROUP,
                               "--fleet-name", "clustermesh-flt")
        try:
            selected, identities = prepared.validate_scope(self.args, group, clusters, members)
        except prepared.FleetNotConnected as error:
            require(len(error.members) == 1 and error.members[0]["name"] == base.ROLE,
                    "Only known mesh-96 Fleet degradation is permitted")
            status = error.members[0]["meshProperties"]["status"]
            require(status.get("state") in ("Failed", "PartialConnectivity")
                    and ((status.get("error") or {}).get("code") in ("ConnectivityTimeout", "PartialConnectivity")
                         or status.get("state") == "PartialConnectivity" and not status.get("error")),
                    "Fleet failure is not the known mesh-96 connectivity degradation")
            selected = next(row for row in clusters if row["tags"]["role"] == base.ROLE)
            identities = error.identities
        require(all(row.get("provisioningState") == "Succeeded"
                    and (row.get("powerState") or {}).get("code") == "Running" for row in clusters),
                "All preserved AKS customer resources must be quiescent")
        require(selected["name"] == base.CLUSTER and selected["nodeResourceGroup"].lower() == base.NODE_GROUP,
                "Selected AKS/Node RG changed")
        require(sorted(identities, key=lambda row: row["role"]) == sorted(
            self.data["prior-native-action.json"]["authoritative_identities"], key=lambda row: row["role"]),
            "Original 100-cluster Fleet identities changed")
        node_group = self.az_json("group", "show", "--name", base.NODE_GROUP)
        require(prepared.resource_equal(node_group.get("id"),
                                       f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.NODE_GROUP}")
                and prepared.resource_equal(node_group.get("managedBy"), selected["id"])
                and str(node_group.get("location", "")).lower() == base.REGION,
                "Node RG ownership, subscription, or region changed")
        prepared.require_lease(node_group, self.args.timeout_seconds)
        self.summary["authoritative_identities"] = identities
        self.summary["fleet_connected_count"] = sum(
            row["meshProperties"]["status"].get("state") == "Connected" for row in members)
        self.summary["lease_checked_at"] = workers.utc_now()

    def models(self, *, observing=False):
        diagnostics = {"observed_at": workers.utc_now(), "views": {}}
        self.summary["current_model_diagnostics"] = diagnostics
        operation = self.az_json("aks", "operation", "show-latest", "--resource-group", base.RESOURCE_GROUP,
                                 "--name", base.CLUSTER, "--query", base.OPERATION_QUERY)
        diagnostics["operation"] = operation
        self.save()
        require(operation.get("status") == "Succeeded" and operation.get("name") and not operation.get("errorCode"),
                "Latest customer AKS operation is not quiescent")
        require(base.timestamp(operation.get("startTime"), "AKS operation start")
                <= base.timestamp(operation.get("endTime"), "AKS operation end")
                <= datetime.now(timezone.utc), "AKS operation timestamps are invalid")
        pools = self.az_json("aks", "nodepool", "list", "--resource-group", base.RESOURCE_GROUP,
                             "--cluster-name", base.CLUSTER)
        diagnostics["pools"] = pools
        self.save()
        require(isinstance(pools, list) and len(pools) == 2
                and {row.get("name") for row in pools} == {"default", "prompool"},
                "Only original default(2)/empty prompool(0), without modern capacity, is authorized")
        initial_pools = {row["name"]: row for row in self.data["pool-configuration.json"]}
        pool_pins = {}
        for pool in pools:
            name = pool["name"]
            require(pool.get("count") == (2 if name == "default" else 0)
                    and pool.get("enableAutoScaling") is False and pool.get("vmSize") == "Standard_D8_v3"
                    and pool.get("mode") == ("System" if name == "default" else "User")
                    and pool.get("provisioningState") == "Succeeded"
                    and pool.get("powerState", {}).get("code") == "Running", "Original pool model/count changed")
            for key, value in prepared.pool_configuration(initial_pools[name]).items():
                actual = pool.get(key)
                require(prepared.resource_equal(actual, value) if key in ("id", "vnetSubnetId", "podSubnetId")
                        else actual == value, f"{name}: original pool configuration changed: {key}")
            pool_pins[name] = prepared.pool_configuration(pool)
        scales = self.az_json("vmss", "list", "--resource-group", base.NODE_GROUP, "--query", base.VMSS_QUERY)
        diagnostics["vmsses"] = scales
        self.save()
        require(isinstance(scales, list) and len(scales) == 2
                and {row.get("name") for row in scales} == {base.DEFAULT_VMSS, base.PROM_VMSS},
                "VMSS inventory includes missing/extra capacity")
        for scale in scales:
            name = scale["name"]
            pool_name = "default" if name == base.DEFAULT_VMSS else "prompool"
            expected_id = (f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.NODE_GROUP}"
                           f"/providers/Microsoft.Compute/virtualMachineScaleSets/{name}")
            require(prepared.resource_equal(scale.get("id"), expected_id)
                    and str(scale.get("location", "")).lower() == base.REGION
                    and workers.vmss_pool_name(scale) == pool_name and scale.get("orchestrationMode") == "Uniform"
                    and scale.get("sku", {}).get("capacity") == (2 if pool_name == "default" else 0)
                    and scale.get("sku", {}).get("name") == "Standard_D8_v3"
                    and scale.get("provisioningState") == "Succeeded", "VMSS ownership/model/capacity changed")
        instances = self.az_json("vmss", "list-instances", "--resource-group", base.NODE_GROUP,
                                 "--name", base.DEFAULT_VMSS, "--query", base.VM_QUERY)
        empty = self.az_json("vmss", "list-instances", "--resource-group", base.NODE_GROUP,
                             "--name", base.PROM_VMSS, "--query", base.VM_QUERY)
        diagnostics.update(instances=instances, prom_instances=empty)
        self.save()
        require(empty == [] and isinstance(instances, list) and len(instances) == 2
                and {str(row.get("instanceId")) for row in instances} == {"0", "1"},
                "Default VM enumeration or natively removed prom VM changed")
        views = {}
        for row in instances:
            name = SOURCE if str(row["instanceId"]) == "0" else TARGET
            expected_provider = self.nodes[name]["spec"]["providerID"]
            require(row.get("computerName") == name and row.get("vmId") == VM_IDS[name]
                    and prepared.resource_equal("azure://" + str(row.get("id")), expected_provider)
                    and row.get("latestModelApplied") is True
                    and row.get("provisioningState") == "Succeeded", "Exact default VM identity/model changed")
            view = self.az_json("vmss", "get-instance-view", "--resource-group", base.NODE_GROUP,
                                "--name", base.DEFAULT_VMSS, "--instance-id", str(row["instanceId"]),
                                "--query", VIEW_QUERY)
            views[name] = view
            diagnostics["views"][name] = view
            self.save()
            codes = {entry.get("code") for entry in view.get("statuses") or []}
            require("ProvisioningState/succeeded" in codes and not any("failed" in str(code).lower() for code in codes),
                    "VM provisioning failed or is ambiguous")
            require(codes == {"ProvisioningState/succeeded", "PowerState/running"}
                    or observing and name == TARGET and codes <= {
                        "ProvisioningState/succeeded", "PowerState/running", "PowerState/stopping",
                        "PowerState/stopped", "PowerState/starting"},
                    "An unrelated VM power operation occurred")
            if name == SOURCE:
                require(guest_state(view, max_age_seconds=300) == "ready" and extensions_ready(view),
                        "Protected default0 VM lost fresh guest health")
        pin = arm_canonical({"pools": pool_pins, "scales": sorted(scales, key=lambda row: row["name"]), "operation": operation["name"],
               "instances": {row["computerName"]: (row["id"].lower(), row["vmId"]) for row in instances}}
        )
        require(self.arm_pin is None or pin == self.arm_pin, "A customer ARM model changed during host observation")
        self.arm_pin = pin
        return views

    def owned_pod(self, snapshot, pod):
        require(base.pvc_free(pod["spec"]), "A target Pod has a PVC/ephemeral claim")
        original = self.initial_pods.get(uid(pod))
        require(original is None or pod_pin(pod) == pod_pin(original),
                "An original target Pod identity/spec changed in place")
        if uid(pod) == COMPLETED_CERT_UID:
            metadata = pod["metadata"]
            status = pod.get("status") or {}
            containers = pod["spec"].get("containers") or []
            states = status.get("containerStatuses") or []
            require(original is not None and metadata.get("name") == COMPLETED_CERT_POD
                    and metadata.get("namespace") == NAMESPACE and not metadata.get("ownerReferences")
                    and metadata.get("labels") == original["metadata"].get("labels")
                    and metadata.get("labels", {}).get("kubernetes.azure.com/managedby") == "aks"
                    and metadata.get("labels", {}).get("k8s-app") == "hubble-generate-certs"
                    and pod["spec"].get("restartPolicy") in ("OnFailure", "Never")
                    and not pod["spec"].get("initContainers") and not pod["spec"].get("ephemeralContainers")
                    and not status.get("initContainerStatuses") and not status.get("ephemeralContainerStatuses")
                    and status.get("phase") == original.get("status", {}).get("phase") == "Succeeded"
                    and len(containers) == len(states) == 1 and containers[0].get("name") == states[0].get("name") == "certgen"
                    and containers[0].get("image") == "mcr.microsoft.com/containernetworking/cilium/certgen:v0.3.2"
                    and states[0].get("ready") is False and states[0].get("started") is not True
                    and set(states[0].get("state") or {}) == {"terminated"},
                    "The captured completed certificate hook changed or became live")
            terminal = states[0]["state"]["terminated"]
            previous = original["status"]["containerStatuses"][0]["state"]["terminated"]
            require(terminal == previous and base.integer(terminal.get("exitCode")) and terminal["exitCode"] == 0
                    and terminal.get("reason") == "Completed"
                    and base.timestamp(terminal.get("finishedAt"), "certificate completion")
                    < base.timestamp(ready_condition(self.nodes[TARGET]).get("lastTransitionTime"), "captured Node failure"),
                    "Certificate hook completion predates neither the captured failure nor the approved restart")
            self.summary["completed_certificate_hook"] = {
                "namespace": NAMESPACE, "name": COMPLETED_CERT_POD, "uid": COMPLETED_CERT_UID,
                "finished_at": terminal["finishedAt"], "running_workload": False,
                "disposition": "unchanged successfully completed AKS hook; no Pod mutation",
            }
            return
        refs = [row for row in pod["metadata"].get("ownerReferences", []) if row.get("controller") is True]
        require(len(refs) == 1, f"{pod['metadata']['name']}: target Pod is not exactly controller-owned")
        ref = refs[0]
        namespace = pod["metadata"]["namespace"]
        require((namespace, ref["kind"], ref["name"], ref["uid"]) in self.allowed_owners,
                "A foreign/new controller workload occupies the target")
        controller = base.controller(snapshot, ref["kind"], namespace, ref["name"], ref["uid"])
        if ref["kind"] == "ReplicaSet":
            deployment = base.controller_owner(controller, "Deployment")
            base.controller(snapshot, "Deployment", namespace, deployment["name"], deployment["uid"])
        else:
            require(ref["kind"] in ("DaemonSet", "StatefulSet"), "Unsupported target controller kind")

    def host_live(self, snapshot, view):
        target = next(row for row in snapshot["nodes"]["items"] if row["metadata"]["name"] == TARGET)
        accepted = self.summary["restart"].get("accepted") is True
        codes = {row.get("code") for row in view.get("statuses") or []}
        return (
            fresh_node_ready(target) and guest_state(view, initializing=accepted, max_age_seconds=300) == "ready"
            and codes == {"ProvisioningState/succeeded", "PowerState/running"}
            and (not accepted or base.node_boot(target) != self.pre_restart_boot)
        )

    def guard(self, snapshot, views, *, before_restart=False):
        self.summary["current_kubernetes_diagnostics"] = safe_diagnostics(snapshot)
        self.save()
        require(controllers_pin(snapshot["controllers"]) == self.controller_pins,
                "Captured functional controller specs/UIDs changed")
        require(base.frozen_pdbs(snapshot) == self.pdb_pins, "PDB UID/spec changed; no relaxation is allowed")
        rows = snapshot["nodes"]["items"]
        nodes = {row["metadata"]["name"]: row for row in rows}
        require(len(rows) == len(nodes) and set(nodes) == set(self.nodes), "Node/KWOK identity inventory changed")
        for name, original in self.nodes.items():
            node = nodes[name]
            require(not node["metadata"].get("deletionTimestamp") and logical_node(node) == logical_node(original),
                    f"{name}: Node UID/logical spec/labels changed")
        require(base.node_boot(nodes[SOURCE]) == BOOTS[SOURCE] and fresh_node_ready(nodes[SOURCE]),
                "Protected default0 rebooted or lost fresh Node readiness")
        require(base.node_boot(nodes[TARGET]), "Target boot identity is absent")
        nncs = maintenance._nnc_map(snapshot["nnc"])
        require(set(nncs) == {SOURCE, TARGET}, "NNC ownership inventory changed")
        for name in (SOURCE, TARGET):
            require(all(nncs[name][key] == self.nnc_pins[name][key]
                        for key in ("uid", "node_uid", "network_container_id")),
                    "Original NNC/Node/network container identity changed")
        live = self.host_live(snapshot, views[TARGET])
        reboot_fenced = (
            self.summary["restart"].get("accepted") is True
            and base.node_boot(nodes[TARGET]) != self.pre_restart_boot
            and fresh_node_heartbeat(nodes[TARGET])
            and base.timestamp(ready_condition(nodes[TARGET]).get("lastHeartbeatTime"), "new boot heartbeat")
            >= base.timestamp(self.summary["restart"]["requested_at"], "approved restart request")
        )
        if reboot_fenced:
            self.summary["restart"]["reboot_fenced"] = True
        if live or reboot_fenced:
            self.allow_replacements = True
        elif before_restart:
            require(base.node_boot(nodes[TARGET]) == BOOTS[TARGET], "An unresponsive target rebooted outside this action")
        pods = snapshot["pods"]["items"]
        by_uid = {uid(pod): pod for pod in pods}
        require(len(by_uid) == len(pods), "Pod UIDs are duplicated")
        for pod in pods:
            if pod["spec"].get("nodeName") == TARGET:
                self.owned_pod(snapshot, pod)
                if before_restart and not live:
                    require(not maintenance._readiness_condition_true(pod), "A currently Ready target Pod forbids restart")
            if pod["spec"].get("nodeName") == SOURCE and base.pod_ready(pod):
                self.protected.setdefault(uid(pod), pod_pin(pod))
        for pod_uid, pin in self.protected.items():
            require(pod_uid in by_uid and pod_pin(by_uid[pod_uid]) == pin and base.pod_ready(by_uid[pod_uid]),
                    "A protected healthy default0 Pod UID/spec/readiness changed")
        agents = maintenance._agent_map(snapshot["pods"])
        require(set(agents) <= set(self.agents) and set(agents) >= set(self.source_pins),
                "Only originally terminating target mocks may be absent")
        changes = {}
        for name, pod in agents.items():
            require(mocks._pod_owned_by_controller_uid(pod, self.mock_uid)
                    and base.pvc_free(pod["spec"]) and pod["spec"].get("nodeName") in ("", None, SOURCE, TARGET),
                    "A mock replacement has foreign ownership, PVC, or placement")
            if name in self.source_pins:
                require(pod_pin(pod) == self.source_pins[name] and not pod["metadata"].get("deletionTimestamp"),
                        "An original nonterminating default0 mock UID/spec changed")
            elif uid(pod) != uid(self.agents[name]):
                require(self.allow_replacements, "Target mock UID changed before fresh host recovery or reboot fencing")
                require(uid(pod) not in {uid(row) for row in self.agents.values()}, "A replacement reused an original UID")
                require(name not in self.replacement_pins or self.replacement_pins[name] == uid(pod),
                        "A controller replacement was replaced again")
                self.replacement_pins[name] = uid(pod)
                changes[name] = {"old_uid": uid(self.agents[name]), "new_uid": uid(pod),
                                 "controller_uid": self.mock_uid, "node_name": pod["spec"].get("nodeName"),
                                 "ready": base.pod_ready(pod)}
        self.summary.update(
            preserved_kwok_node_uids=self.kwok_uids,
            current_kwok_ready=sum(workers.node_is_ready(nodes[name]) for name in self.kwok_uids),
            current_mock_uids={name: uid(pod) for name, pod in agents.items()},
            mock_readiness={"present": len(agents), "ready": sum(base.pod_ready(pod) for pod in agents.values()),
                            "terminating": sum(bool(pod["metadata"].get("deletionTimestamp")) for pod in agents.values()),
                            "pending": sum(pod["status"].get("phase") == "Pending" for pod in agents.values())},
            authorized_controller_replacements=changes,
            effective_identity={"node_uid": uid(nodes[TARGET]), "vm_id": VM_IDS[TARGET],
                                "boot_id": base.node_boot(nodes[TARGET]), "provider_id": nodes[TARGET]["spec"]["providerID"]},
            protected_source_pod_uids=sorted(self.protected),
        )
        self.save()
        system = maintenance._healthy_system_daemonsets_on_node(snapshot["pods"], TARGET)
        require({name for _, name, _ in self.target_daemonsets} >= {"cilium", "azure-cns"},
                "Captured target system ownership lacks Cilium/CNS")
        return live and extensions_ready(views[TARGET]) and self.target_daemonsets <= system

    def observe(self, *, before_restart=False):
        self.unchanged_inputs()
        self.authority()
        views = self.models(observing=self.summary["restart"].get("accepted") is True)
        snapshot = self.snapshot()
        healthy = self.guard(snapshot, views, before_restart=before_restart)
        return healthy, snapshot, views

    def stalled(self, snapshot, views):
        node = next(row for row in snapshot["nodes"]["items"] if row["metadata"]["name"] == TARGET)
        condition = ready_condition(node)
        now = datetime.now(timezone.utc)
        require(condition.get("status") in ("False", "Unknown")
                and (now - base.timestamp(condition.get("lastHeartbeatTime"), "lost heartbeat")).total_seconds() >= 300
                and (now - base.timestamp(condition.get("lastTransitionTime"), "unready transition")).total_seconds() >= 300
                and guest_state(views[TARGET], max_age_seconds=300) == "unresponsive"
                and base.node_boot(node) == BOOTS[TARGET],
                "Restart requires the exact still-unresponsive host and prolonged heartbeat loss")

    def journal_list(self):
        payload = self.kube("-n", NAMESPACE, "get", "configmaps",
                            "--field-selector", f"metadata.name={JOURNAL}", "-o", "json")
        rows = mocks._items(payload, "exclusive restart journal inventory")
        require(payload.get("apiVersion") == "v1" and payload.get("kind") in ("List", "ConfigMapList")
                and not (payload.get("metadata") or {}).get("continue") and len(rows) <= 1
                and all(row["metadata"].get("name") == JOURNAL
                        and row["metadata"].get("namespace") == NAMESPACE for row in rows),
                "Exclusive restart journal absence or identity is ambiguous")
        return payload

    def owned_journal(self):
        current = self.kube("-n", NAMESPACE, "get", "configmap", JOURNAL, "-o", "json")
        metadata = current.get("metadata") or {}
        require(current.get("apiVersion") == "v1" and current.get("kind") == "ConfigMap"
                and metadata.get("name") == JOURNAL and metadata.get("namespace") == NAMESPACE
                and uid(current) == self.journal_uid and not metadata.get("deletionTimestamp")
                and not metadata.get("ownerReferences") and metadata.get("resourceVersion")
                and current.get("data") == self.persisted_journal_data,
                "Exclusive restart journal UID/token/source/receipt changed")
        return current

    def journal_data(self):
        data = {
            "owner": OWNER, "token": self.token, "target_node_uid": base.REAL_UIDS[TARGET],
            "target_vm_id": VM_IDS[TARGET], "source_state_sha256": digest(self.hashes),
            "receipt": json.dumps(self.summary["restart"], sort_keys=True, separators=(",", ":")),
        }
        if self.resume:
            data.update(
                prior_unsubmitted_receipt=json.dumps(self.resume["restart"], sort_keys=True, separators=(",", ":")),
                prior_checkpoint_sha256=self.resume_hash, prior_build_id=str(PRE_SUBMIT_BUILD),
            )
        return data

    def attach_pre_submit_reservation(self):
        rows = self.journal_list()["items"]
        require(len(rows) == 1, "The original pre-submit reservation must still exist")
        current = rows[0]
        data = current.get("data") or {}
        expected = {
            "owner": OWNER, "target_node_uid": base.REAL_UIDS[TARGET], "target_vm_id": VM_IDS[TARGET],
            "source_state_sha256": digest(self.hashes),
            "receipt": json.dumps(self.resume["restart"], sort_keys=True, separators=(",", ":")),
        }
        require(uid(current) == PRE_SUBMIT_JOURNAL_UID and set(data) == set(expected) | {"token"}
                and all(data.get(key) == value for key, value in expected.items())
                and isinstance(data.get("token"), str) and re.fullmatch(r"[0-9a-f]{32}", data["token"]),
                "Original journal changed or has already been continued; no restart may be replayed")
        self.token, self.journal_uid = data["token"], uid(current)
        self.persisted_journal_data = copy.deepcopy(data)
        self.owned_journal()
        self.summary["journal"].update(
            uid=self.journal_uid, accepted=True, ambiguous=False, create_attempted=False,
            continued_existing_reservation=True,
        )
        self.save()

    def write(self, command):
        require(self.args.execute, "Read-only planning must never write")
        permitted = command[:6] == ["kubectl", "-n", NAMESPACE, "create", "configmap", JOURNAL]
        permitted = permitted or command[:6] == ["kubectl", "-n", NAMESPACE, "patch", "configmap", JOURNAL]
        permitted = permitted or command == self.restart_command()
        require(permitted, "Write escaped the exclusive-journal/single-instance restart whitelist")
        if command == self.restart_command():
            require(self.journal_uid and self.summary["restart"].get("attempted") is True
                    and self.summary["restart"].get("accepted") is None and not self.restart_sent,
                    "The exact restart requires a journalled first and only submission")
            self.owned_journal()
            self.restart_sent = True
            self.summary["restart"]["submission_started"] = True
        self.summary["mutation_started"] = True
        self.save()
        return super().run(command, 45)

    def acquire(self):
        require(self.journal_list().get("items") == [], "Existing journal prohibits restart replay/adoption")
        self.summary["journal"].update(create_attempted=True, accepted=None, ambiguous=True)
        self.save()
        data = self.journal_data()
        output = self.write(["kubectl", "-n", NAMESPACE, "create", "configmap", JOURNAL,
                             *[f"--from-literal={key}={value}" for key, value in data.items()], "-o", "json"])
        created = workers.parse_json(output, "exclusive restart journal")
        require(uid(created) and created.get("data") == data, "Journal creation outcome is ambiguous")
        self.journal_uid = uid(created)
        self.persisted_journal_data = data
        self.owned_journal()
        self.summary["journal"].update(uid=self.journal_uid, accepted=True, ambiguous=False)
        self.save()

    def persist_journal(self):
        self.unchanged_inputs()
        current = self.owned_journal()
        desired = self.journal_data()
        self.write(["kubectl", "-n", NAMESPACE, "patch", "configmap", JOURNAL, "--type=json", "-p", json.dumps([
            {"op": "test", "path": "/metadata/uid", "value": self.journal_uid},
            {"op": "test", "path": "/metadata/resourceVersion", "value": current["metadata"]["resourceVersion"]},
            {"op": "test", "path": "/data", "value": self.persisted_journal_data},
            {"op": "add", "path": "/data", "value": desired},
        ])])
        self.persisted_journal_data = desired
        self.owned_journal()

    @staticmethod
    def restart_command():
        return ["az", "vmss", "restart", "--subscription", base.SUBSCRIPTION, "--resource-group", base.NODE_GROUP,
                "--name", base.DEFAULT_VMSS, "--instance-ids", "1", "--no-wait", "--only-show-errors", "--output", "none"]

    def execute(self):
        healthy, snapshot, views = self.observe(before_restart=True)
        if self.resume:
            self.attach_pre_submit_reservation()
        else:
            require(self.journal_list().get("items") == [], "Existing journal prohibits blind accepted-action adoption")
        if healthy:
            self.summary.update(plan_valid=True, success=True, host_recovered=True, status="already-healthy-no-restart")
            self.save()
            return
        self.stalled(snapshot, views)
        self.summary.update(plan_valid=True, status="planned-read-only")
        self.save()
        if not self.args.execute:
            return
        if self.resume:
            self.persist_journal()
        else:
            self.acquire()
        healthy, snapshot, views = self.observe(before_restart=True)
        if healthy:
            self.summary.update(success=True, host_recovered=True, status="recovered-before-restart-no-post")
            self.save()
            return
        self.stalled(snapshot, views)
        self.pre_restart_boot = BOOTS[TARGET]
        receipt = self.summary["restart"]
        require(not receipt["attempted"], "A second restart POST is forbidden")
        receipt.update(attempted=True, accepted=None, ambiguous=True, requested_at=workers.utc_now(),
                       previous_boot_id=self.pre_restart_boot, command=self.restart_command())
        self.save()
        self.persist_journal()
        healthy, snapshot, views = self.observe(before_restart=True)
        if healthy:
            receipt.update(attempted=False, accepted=False, ambiguous=False, not_submitted=True)
            self.persist_journal()
            self.summary.update(success=True, host_recovered=True, status="recovered-before-restart-no-post")
            self.save()
            return
        self.stalled(snapshot, views)
        self.unchanged_inputs()
        # No second write is possible if the client cannot prove acceptance.
        self.write(self.restart_command())
        receipt.update(accepted=True, ambiguous=False, accepted_at=workers.utc_now())
        self.save()
        self.persist_journal()
        self.summary["status"] = "observing-single-accepted-restart"
        while True:
            healthy, _, _ = self.observe()
            if healthy:
                receipt["host_proven"] = True
                self.persist_journal()
                self.summary.update(success=True, host_recovered=True, status="host-recovered-workloads-not-qualified")
                self.save()
                return
            require(time.monotonic() < self.work_deadline, "Accepted restart did not recover the host within its bound")
            time.sleep(min(10, self.remaining_seconds(10)))


def validate_args(args):
    require(args.resource_group == args.confirm_resource_group == base.RESOURCE_GROUP
            and args.expected_subscription.lower() == base.SUBSCRIPTION and args.expected_region.lower() == base.REGION,
            "Only the approved subscription/RG/region is supported")
    require(maintenance.SHA256_RE.fullmatch(args.expected_tfvars_sha) is not None, "tfvars SHA256 is invalid")
    require(base.integer(args.timeout_seconds) and 300 <= args.timeout_seconds <= 1800, "Timeout must be 300..1800 seconds")
    require(args.context == base.CLUSTER and args.kubeconfig, "Private explicit mesh-96 credentials/context are required")
    require((not getattr(args, "resume_checkpoint", None) and getattr(args, "resume_build_id", 0) == 0)
            or (getattr(args, "resume_checkpoint", None) and getattr(args, "resume_build_id", 0) == PRE_SUBMIT_BUILD),
            "Continuation requires only the exact known unsubmitted reservation from build 79945")
    root, output, credentials = (Path(value).resolve() for value in
                                 (args.source_state_directory, args.summary_file, args.kubeconfig))
    require(output != root and root not in output.parents and output != credentials
            and not output.exists(), "Summary must be new and outside immutable inputs/private credentials")
    args.role = base.ROLE


def execute_recovery(args, summary, runner=workers.run_command):
    validate_args(args)
    summary.update(schema_version=1, execute=args.execute, mutation_started=False, plan_valid=False,
                   success=False, host_recovered=False, workloads_ready=False, full_suite_qualified=False,
                   status="validating", started_at=workers.utc_now(),
                   restart={"attempted": False, "accepted": None, "ambiguous": False,
                            "submission_started": False, "automatic_retry_allowed": False},
                   journal={"name": JOURNAL, "namespace": NAMESPACE, "retained": True})
    try:
        data, hashes = load_source(args)
        resume, resume_hash = load_pre_submit_checkpoint(args, data, hashes)
        summary.update(source_hashes=hashes, source_state_sha256=digest(hashes), plan_sha256=PLAN_SHA,
                       original_identity={"node_name": TARGET, "node_uid": base.REAL_UIDS[TARGET],
                                          "vm_id": VM_IDS[TARGET], "boot_id": BOOTS[TARGET]},
                       migration_completed=None,
                       accepted_risk="Normal restart can discard paused memory and interrupt unfinished live migration")
        if resume:
            summary["continuation"] = {
                "source_build": PRE_SUBMIT_BUILD, "source_commit": PRE_SUBMIT_COMMIT,
                "checkpoint_sha256": resume_hash, "prior_unsubmitted_restart": copy.deepcopy(resume["restart"]),
                "proof": "exact pre-POST guard failure in pinned source; accepted flag is set before any post-POST observation",
            }
        Recovery(args, data, hashes, summary, runner, resume=resume, resume_hash=resume_hash).execute()
    except EXPECTED_ERRORS as error:
        summary.update(success=False, host_recovered=False, status="failed-closed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        summary["finished_at"] = workers.utc_now()
        summary["workloads_ready"] = False
        summary["full_suite_qualified"] = False
        mocks.write_json_atomic(args.summary_file, summary)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("resource-group", "confirm-resource-group", "expected-subscription", "expected-region",
                 "expected-tfvars-sha", "source-state-directory", "summary-file", "kubeconfig"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--context", default=base.CLUSTER)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--resume-checkpoint")
    parser.add_argument("--resume-build-id", type=int, default=0)
    return parser.parse_args(argv)


def main(argv=None):
    def interrupted(signum, _frame):
        raise workers.ReconcileError(f"Interrupted ({signum}); no retry or fallback is authorized")
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, interrupted)
    args, summary = parse_args(argv), {}
    try:
        execute_recovery(args, summary)
    except EXPECTED_ERRORS as error:
        print(f"Retained-worker recovery failed closed: {error}", file=sys.stderr)
        return 1
    print(f"{summary['status']}; workloads_ready=false; receipt={args.summary_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
