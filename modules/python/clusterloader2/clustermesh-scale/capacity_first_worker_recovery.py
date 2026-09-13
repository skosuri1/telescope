#!/usr/bin/env python3
"""Create only cniv5 System(2) after the proved VM1 restart failure.

No bootstrap, migration, fencing, Prom creation, or IP/memory qualification is
performed. Registration readiness is not permission to move workloads.
"""

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
import failed_prom_capacity_resume as quantities
import failed_prom_worker_replacement as replacement
import mock_cni_recovery as mocks
import modern_prom_recovery as prom
import prepared_worker_retirement as prepared
import preserved_worker_reconcile as workers
import stalled_retained_worker_recovery as stalled
import unreachable_prom_worker_recovery as base


POOL = "cniv5"
JOURNAL = "mesh96-capacity-first-cniv5"
OWNER = "capacity-first-worker-recovery"
CORRELATION = "42bdc21a-2d4a-46dd-b3da-ab4637a6ead3"
FAILURE = "ProvisioningState/failed/VMExtensionProvisioningError"
RESTART_OPERATION = "Microsoft.Compute/virtualMachineScaleSets/restart/action"
RESTART_JOURNAL_UID = "55117c2c-db51-41b2-b8ec-12fc9d357501"
CREATING = {"Creating", "Updating"}
EXTRA_FILES = ("default-vmss-activity-log.json", "default-vmss-instance-view.json")
RESERVED_BUILD = 79959
RESERVED_JOURNAL_UID = "1e3b51d5-83d4-406d-bb38-4ead04e1c425"
ROLLED_SECURITY_POD_UID = "b03b9e77-57f1-4bf9-96b4-e43413892bc4"
SECURITY_OWNER = ("kube-system", "azuresecuritylinuxagent", stalled.SECURITY_DAEMONSET_UID)
require = prepared.require
digest = base.digest
uid = base.object_uid
canonical = stalled.arm_canonical


def counter(value):
    if isinstance(value, str) and re.fullmatch(r"[0-9]+\.0+", value):
        value = value.split(".", 1)[0]
    return quantities.quota_counter(value)


def checkpoint_hash(path):
    value = Path(path)
    require(value.is_file() and not value.is_symlink() and value.stat().st_size <= 32 * 1024 * 1024,
            "Recovery checkpoint must be a bounded regular file")
    return hashlib.sha256(value.read_bytes()).hexdigest()


def terminal_proof(data, checkpoint):
    record = checkpoint.get("restart") or {}
    original = checkpoint.get("original_identity") or {}
    journal = checkpoint.get("journal") or {}
    require(checkpoint.get("execute") is True and checkpoint.get("plan_sha256") == stalled.PLAN_SHA
            and record.get("submission_started") is True and record.get("attempted") is True
            and record.get("accepted") is True and record.get("ambiguous") is False
            and record.get("command") == stalled.Recovery.restart_command()
            and original.get("node_name") == stalled.TARGET
            and original.get("node_uid") == base.REAL_UIDS[stalled.TARGET]
            and original.get("vm_id") == stalled.VM_IDS[stalled.TARGET]
            and original.get("boot_id") == stalled.BOOTS[stalled.TARGET]
            and journal.get("uid") == RESTART_JOURNAL_UID and journal.get("name") == stalled.JOURNAL,
            "Only the actual single accepted VM1 restart checkpoint is a valid lineage")
    requested = base.timestamp(record.get("requested_at"), "restart request")
    acknowledged = base.timestamp(record.get("accepted_at"), "restart acknowledgement")
    rows = data["default-vmss-activity-log.json"]
    require(isinstance(rows, list), "Activity Log observation is malformed")
    correlated = [row for row in rows if row.get("correlationId") == CORRELATION]
    expected_id = (f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.NODE_GROUP}"
                   f"/providers/Microsoft.Compute/virtualMachineScaleSets/{base.DEFAULT_VMSS}")
    events = {}
    for row in correlated:
        state = row.get("status")
        require(state in ("Started", "Accepted", "Failed") and state not in events
                and row.get("operation") == RESTART_OPERATION
                and prepared.resource_equal(row.get("resourceId"), expected_id),
                "Restart failure correlation, operation, resource, or event status is ambiguous")
        events[state] = row
    require(set(events) == {"Started", "Accepted", "Failed"}, "Complete terminal restart correlation is required")
    times = {state: base.timestamp(row.get("eventTimestamp"), f"restart {state}") for state, row in events.items()}
    require(requested <= times["Started"] <= times["Accepted"] <= times["Failed"] <= datetime.now(timezone.utc)
            and abs((acknowledged - times["Accepted"]).total_seconds()) <= 30,
            "Terminal provider timestamps do not bind to the accepted restart")
    message = (events["Failed"].get("properties") or {}).get("statusMessage")
    result = json.loads(message) if isinstance(message, str) else message
    error = (result or {}).get("error") or {}
    details = error.get("details")
    require((result or {}).get("status") == "Failed" and error.get("code") == "ResourceOperationFailure"
            and isinstance(details, list) and len(details) == 1
            and details[0].get("code") == "VMExtensionProvisioningError" and details[0].get("target") == "1",
            "Terminal failure does not specifically target VM1 extension provisioning")
    text = str(details[0].get("message") or "")
    require("has not reported status for VM agent or extensions" in text and all(
        name in text for name in ("vmssCSE", "AKSLinuxExtension", f"{base.DEFAULT_VMSS}-AKSLinuxBilling")
    ), "The three unresponsive VM1 extension failures are not proven")
    statuses = replacement.status_rows(data["default-vmss-instance-view.json"], "captured default VMSS")
    require(len(statuses) == 1 and statuses[0]["code"] == FAILURE, "Captured aggregate failure differs")
    failure_time = base.timestamp(statuses[0].get("time"), "aggregate failure")
    require(0 <= (times["Failed"] - failure_time).total_seconds() <= 60
            and failure_time > acknowledged, "Aggregate failure is not correlated to the terminal restart")
    return {"correlation_id": CORRELATION, "target_instance_id": "1", "target_vm_id": stalled.VM_IDS[stalled.TARGET],
            "failure_code": FAILURE, "aggregate_time": failure_time.isoformat(),
            "started_at": times["Started"].isoformat(), "accepted_at": times["Accepted"].isoformat(),
            "failed_at": times["Failed"].isoformat(), "fencing_proven": False, "restart_retry_allowed": False}


def load_inputs(args):
    hashes = stalled.file_hashes(args.source_state_directory)
    require(set(EXTRA_FILES) <= set(hashes), "Authoritative aggregate and Activity Log evidence are required")
    data = {name: stalled.read_json(Path(args.source_state_directory) / name)
            for name in (*stalled.REQUIRED_FILES, *EXTRA_FILES)}
    receipt_hash = checkpoint_hash(args.restart_checkpoint)
    checkpoint = stalled.read_json(args.restart_checkpoint)
    proof = terminal_proof(data, checkpoint)
    observation = data["quota-observation.json"]
    require(observation.get("observation_only") is True and observation.get("mutation_started") is False
            and observation.get("worker_state_collected") is True and observation.get("source_native_build") == 79894,
            "A read-only, native-lineage-bound worker observation is required")
    native = data["prior-native-action.json"]
    removal = native.get("replacement", {}).get("native_removal") or {}
    require(native.get("plan_sha256") == stalled.PLAN_SHA
            and native.get("replacement", {}).get("delete", {}).get("accepted") is True
            and removal.get("old_node_pods_nnc_absent") is True
            and removal.get("pool_count") == removal.get("vmss_capacity") == 0,
            "Original natively deleted prom VM lineage changed")
    cluster_id = (f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.RESOURCE_GROUP}"
                  f"/providers/Microsoft.ContainerService/managedClusters/{base.CLUSTER}")
    require(prepared.resource_equal(data["cluster.json"].get("id"), cluster_id)
            and str(data["cluster.json"].get("nodeResourceGroup", "")).lower() == base.NODE_GROUP,
            "Observation AKS/Node RG identity changed")
    nodes = {row["metadata"]["name"]: row for row in data["current-nodes.json"]["items"]}
    require(set(nodes) == maintenance.EXPECTED_AGENT_NAMES | {stalled.SOURCE, stalled.TARGET},
            "Observation must contain precisely the original two default Nodes and 100 KWOK Nodes")
    maintenance._require_exact_kwok_nodes(data["current-nodes.json"])
    for name in (stalled.SOURCE, stalled.TARGET):
        require(uid(nodes[name]) == base.REAL_UIDS[name] and base.node_boot(nodes[name]) == stalled.BOOTS[name],
                "Original default Node/boot identity changed")
        maintenance._validate_real_node_scope(nodes[name], subscription=base.SUBSCRIPTION,
                                               node_resource_group=base.NODE_GROUP)
        require(workers.provider_identity(nodes[name]) == (base.DEFAULT_VMSS, "0" if name == stalled.SOURCE else "1"),
                "Observation default Node provider identity changed")
    agents = maintenance._agent_map(data["current-pods.json"])
    require(set(agents) == maintenance.EXPECTED_AGENT_NAMES and len({uid(pod) for pod in agents.values()}) == 100,
            "All 100 current mock Pod identities are required")
    source = [pod for pod in agents.values() if pod["spec"].get("nodeName") == stalled.SOURCE]
    failed = [pod for pod in agents.values() if pod["spec"].get("nodeName") == stalled.TARGET]
    require(len(source) == 44 and len(failed) == 56 and sum(base.pod_ready(pod) for pod in source) == 38
            and all(not pod["metadata"].get("deletionTimestamp") for pod in source)
            and all(pod["metadata"].get("deletionTimestamp") for pod in failed),
            "Observation is not the current 38-healthy/6-Pending/56-Terminating identity plan")
    require({name: uid(nodes[name]) for name in maintenance.EXPECTED_AGENT_NAMES}
            == checkpoint.get("preserved_kwok_node_uids"), "Original KWOK UIDs disagree with the restart receipt")
    require(stalled.file_hashes(args.source_state_directory) == hashes
            and checkpoint_hash(args.restart_checkpoint) == receipt_hash, "Inputs changed while loading")
    return data, hashes, receipt_hash, proof


def pool_settings(patch):
    settings = copy.deepcopy(prom.POOL_SETTINGS)
    settings.update(name=POOL, count=2, mode="System", maxPods=110, nodeLabels=None, orchestratorVersion=patch)
    return settings


def add_command(patch):
    return [
        "az", "aks", "nodepool", "add", "--resource-group", base.RESOURCE_GROUP,
        "--cluster-name", base.CLUSTER, "--name", POOL, "--node-count", "2",
        "--node-vm-size", prom.VM_SIZE, "--mode", "System", "--os-type", "Linux", "--os-sku", "Ubuntu",
        "--node-osdisk-type", "Managed", "--node-osdisk-size", "256",
        "--max-pods", "110", "--vnet-subnet-id", prom.POOL_SETTINGS["vnetSubnetId"],
        "--pod-subnet-id", prom.POOL_SETTINGS["podSubnetId"], "--kubernetes-version", patch,
        "--no-wait", "--only-show-errors", "--output", "none",
    ]


def load_capacity_reservation(args, data, hashes, receipt_hash):
    path = getattr(args, "resume_capacity_checkpoint", None)
    if not path:
        return None, ""
    checksum = checkpoint_hash(path)
    prior = stalled.read_json(path)
    create, journal = prior.get("create") or {}, prior.get("journal") or {}
    require(getattr(args, "resume_build_id", 0) == RESERVED_BUILD
            and prior.get("schema_version") == 1 and prior.get("status") == "failed-closed"
            and prior.get("phase") == "capacity-first-registration-only" and prior.get("execute") is True
            and prior.get("plan_sha256") == stalled.PLAN_SHA
            and prior.get("mutation_started") is True and prior.get("plan_valid") is True
            and prior.get("success") is False and prior.get("pool_created") is False
            and prior.get("registered_nodes_ready") is False and prior.get("workloads_ready") is False
            and prior.get("capacity_qualified") is False and prior.get("bootstrap_complete") is False
            and prior.get("actual_ip_growth_proven") is False and prior.get("actual_memory_headroom_proven") is False
            and "continuation" not in prior
            and prior.get("error") == "ReconcileError: A protected healthy default0 Pod UID/spec/readiness changed"
            and prior.get("source_hashes") == hashes and prior.get("source_state_sha256") == digest(hashes)
            and prior.get("restart_checkpoint_sha256") == receipt_hash,
            "Only build 79959's exact unsubmitted capacity reservation may continue")
    version = (prior.get("desired_pool") or {}).get("orchestratorVersion")
    require(isinstance(version, str) and re.fullmatch(r"[1-9]\d*\.\d+\.\d+", version)
            and prior["desired_pool"] == pool_settings(version)
            and create.get("attempted") is True and create.get("submission_started") is False
            and create.get("accepted") is None and create.get("ambiguous") is True
            and create.get("automatic_retry_allowed") is False and not create.get("registration_proven")
            and "accepted_at" not in create and create.get("command") == add_command(version)
            and journal.get("uid") == RESERVED_JOURNAL_UID and journal.get("name") == JOURNAL
            and journal.get("namespace") == "kube-system" and journal.get("attempted") is True
            and journal.get("accepted") is True and journal.get("ambiguous") is False,
            "Submitted, accepted, ambiguous-delivery or differently owned pool adds cannot be replayed")
    snapshot = prior.get("kubernetes_diagnostics") or {}
    source_pods = {uid(pod): pod for pod in data["current-pods.json"]["items"]}
    current = {uid(pod): pod for pod in snapshot["pods"]["items"]}
    old = source_pods.get(ROLLED_SECURITY_POD_UID)
    observed = current.get(ROLLED_SECURITY_POD_UID)
    ready_at_stop = copy.deepcopy(observed or {})
    ready_at_stop.get("metadata", {}).pop("deletionTimestamp", None)
    require(old is not None and observed is not None and base.pod_ready(old)
            and observed["metadata"].get("deletionTimestamp")
            and base.pod_ready(ready_at_stop) and base.pvc_free(observed["spec"])
            and stalled.pod_pin(observed) == stalled.pod_pin(stalled.safe_diagnostics(old))
            and observed["metadata"].get("namespace") == SECURITY_OWNER[0]
            and base.controller_owner(observed, "DaemonSet")["name"] == SECURITY_OWNER[1]
            and base.controller_owner(observed, "DaemonSet")["uid"] == SECURITY_OWNER[2]
            and observed["spec"].get("nodeName") == stalled.SOURCE,
            "Prior stop is not the captured controller-owned security Pod termination")
    require(stalled.controllers_pin(snapshot["controllers"]) == stalled.controllers_pin(
                stalled.safe_diagnostics(data["current-controllers.json"]))
            and base.frozen_pdbs(snapshot) == base.frozen_pdbs({"pdbs": data["current-pdbs.json"]}),
            "Prior controller or PDB configuration changed")
    for pod_uid, original in source_pods.items():
        if original["spec"].get("nodeName") == stalled.SOURCE and base.pod_ready(original) and pod_uid != ROLLED_SECURITY_POD_UID:
            require(pod_uid in current and base.pod_ready(current[pod_uid])
                    and stalled.pod_pin(current[pod_uid]) == stalled.pod_pin(stalled.safe_diagnostics(original)),
                    "The prior failure included an unrelated protected workload change")
    require(base.timestamp(prior.get("started_at"), "capacity execution start")
            <= base.timestamp(create.get("requested_at"), "capacity reservation")
            <= base.timestamp(prior.get("finished_at"), "pre-submit failure") <= datetime.now(timezone.utc),
            "Prior capacity reservation timestamps are invalid")
    require(checkpoint_hash(path) == checksum, "Capacity reservation checkpoint changed while validating")
    return prior, checksum


class CapacityFirst(maintenance.ClusterOperator):
    """One journal, one cniv5 add, no existing-node or workload writes."""

    def __init__(self, args, data, hashes, receipt_hash, proof, summary, runner, *, resume=None, resume_hash=""):
        deadline = time.monotonic() + args.timeout_seconds
        super().__init__(args, base.CLUSTER, runner, deadline - 60, deadline)
        self.data, self.hashes, self.receipt_hash, self.proof, self.summary = data, hashes, receipt_hash, proof, summary
        self.token, self.journal_uid, self.submitted = uuid.uuid4().hex, "", False
        self.old_nodes = {row["metadata"]["name"]: row for row in data["current-nodes.json"]["items"]}
        self.agents = maintenance._agent_map(data["current-pods.json"])
        self.mock_uid = uid(base.controller({"controllers": data["current-controllers.json"]},
                                           "StatefulSet", mocks.DEFAULT_NAMESPACE, "kwok-node"))
        self.controllers = stalled.controllers_pin(data["current-controllers.json"])
        self.pdbs = base.frozen_pdbs({"pdbs": data["current-pdbs.json"]})
        self.old_nncs = maintenance._nnc_map(data["current-nnc.json"])
        self.protected = {uid(pod): stalled.pod_pin(pod) for pod in data["current-pods.json"]["items"]
                          if pod["spec"].get("nodeName") == stalled.SOURCE and base.pod_ready(pod)}
        self.daemonsets = maintenance._derive_applicable_daemonsets(data["current-pods.json"], [stalled.SOURCE])
        require({name for _, name, _ in self.daemonsets} >= {"cilium", "azure-cns"},
                "The protected reference worker lacks Cilium/CNS DaemonSet ownership")
        self.old_arm = None
        self.patch = ""
        self.new_scale = None
        self.new_vms, self.new_nodes, self.new_nncs = {}, {}, {}
        self.new_boots, self.new_containers = {}, {}
        self.persisted_journal_data = None
        self.resume, self.resume_hash = resume, resume_hash
        self.security_deletion = next(
            pod["metadata"]["deletionTimestamp"] for pod in resume["kubernetes_diagnostics"]["pods"]["items"]
            if uid(pod) == ROLLED_SECURITY_POD_UID
        ) if resume else ""

    def save(self):
        mocks.write_json_atomic(self.args.summary_file, self.summary)

    def unchanged_inputs(self):
        require(stalled.file_hashes(self.args.source_state_directory) == self.hashes
                and checkpoint_hash(self.args.restart_checkpoint) == self.receipt_hash,
                "Immutable observation/restart checkpoint hashes changed")
        if self.resume:
            require(checkpoint_hash(self.args.resume_capacity_checkpoint) == self.resume_hash,
                    "Immutable prior capacity reservation changed")

    def run(self, command, timeout_seconds=45, *, cleanup=False):
        command = list(command)
        allowed = command[0] == "az" and (
            command[1:3] in (["account", "show"], ["group", "show"], ["aks", "list"], ["aks", "show"],
                            ["vmss", "list"], ["vmss", "show"], ["vmss", "list-instances"],
                            ["vmss", "get-instance-view"], ["vm", "list-usage"], ["vm", "list-skus"])
            or command[1:4] in (["aks", "operation", "show-latest"], ["aks", "nodepool", "list"],
                               ["fleet", "member", "list"])
        )
        if command[0] == "kubectl":
            allowed = "get" in command and not any(word in command for word in ("patch", "create", "delete", "exec", "run"))
        require(allowed, "Unsupported command on the strictly read-only path")
        if command[:3] == ["az", "vm", "list-skus"]:
            invoke = super().run

            def once(arguments, timeout):
                try:
                    return invoke(arguments, timeout, cleanup=cleanup)
                except workers.ReconcileError as error:
                    if base.AUTH_ERROR.search(str(error)):
                        raise
                    raise base.arm.ReconcileError(str(error)) from error

            try:
                return base.arm.run_read_with_retries(
                    command, once, timeout_seconds=120, attempts=2, retry_seconds=2,
                )
            except base.arm.ReconcileError as error:
                raise workers.ReconcileError(str(error)) from error
        return super().run(command, min(timeout_seconds, 45), cleanup=cleanup)

    def kube(self, *command):
        return workers.parse_json(self.run(["kubectl", "--request-timeout=45s", *command]), "capacity-first read")

    def snapshot(self):
        require(self.run(["kubectl", "--request-timeout=45s", "get", "--raw=/readyz"]).strip() == "ok",
                "Kubernetes API is not ready")
        return {
            "nodes": self.kube("get", "nodes", "-o", "json"),
            "pods": self.kube("get", "pods", "-A", "-o", "json"),
            "controllers": self.kube("get", "deployments,replicasets,daemonsets,statefulsets", "-A", "-o", "json"),
            "pdbs": self.kube("get", "pdb", "-A", "-o", "json"),
            "nnc": self.kube("get", "nodenetworkconfigs", "-n", "kube-system", "-o", "json"),
        }

    def authority(self):
        require(str(self.az_json("account", "show", "--query", "{id:id}").get("id", "")).lower() == base.SUBSCRIPTION,
                "Current Azure subscription changed")
        group = self.az_json("group", "show", "--name", base.RESOURCE_GROUP)
        clusters = self.az_json("aks", "list", "--resource-group", base.RESOURCE_GROUP, "--query", base.CLUSTER_QUERY)
        members = self.az_json("fleet", "member", "list", "--resource-group", base.RESOURCE_GROUP,
                               "--fleet-name", "clustermesh-flt")
        try:
            selected, identities = prepared.validate_scope(self.args, group, clusters, members)
        except prepared.FleetNotConnected as error:
            require(len(error.members) == 1 and error.members[0]["name"] == base.ROLE,
                    "Only the known mesh96 connectivity failure may be observed")
            status = error.members[0]["meshProperties"]["status"]
            require(status.get("state") in ("Failed", "PartialConnectivity")
                    and (status.get("error") or {}).get("code") in ("ConnectivityTimeout", "PartialConnectivity"),
                    "Fleet failure differs from the explicitly known mesh96 degradation")
            identities = error.identities
            selected = next(row for row in clusters if row["tags"]["role"] == base.ROLE)
        require(all(row.get("provisioningState") == "Succeeded"
                    and row.get("powerState", {}).get("code") == "Running" for row in clusters),
                "Customer AKS resources must remain quiescent")
        require(selected["name"] == base.CLUSTER and selected["nodeResourceGroup"].lower() == base.NODE_GROUP
                and canonical(selected["id"]) == canonical(self.data["cluster.json"]["id"]),
                "AKS/Node RG ownership changed")
        require(sorted(identities, key=lambda row: row["role"]) == sorted(
            self.data["prior-native-action.json"]["authoritative_identities"], key=lambda row: row["role"]),
            "The original 100-cluster Fleet identity map changed")
        group = self.az_json("group", "show", "--name", base.NODE_GROUP)
        require(prepared.resource_equal(group.get("id"),
                                       f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.NODE_GROUP}")
                and prepared.resource_equal(group.get("managedBy"), selected["id"])
                and str(group.get("location", "")).lower() == base.REGION, "Node RG scope/managedBy changed")
        prepared.require_lease(group, self.args.timeout_seconds)
        self.summary["authoritative_identities"] = identities
        self.summary["fleet_connected_count"] = sum(row["meshProperties"]["status"]["state"] == "Connected" for row in members)

    def quota(self):
        rows = self.az_json("vm", "list-usage", "--location", base.REGION, "--query", quantities.USAGE_QUERY)
        counters = {}
        require(isinstance(rows, list), "Quota response is malformed")
        for name in (prom.QUOTA_FAMILY, "cores"):
            matches = [row for row in rows if str(row.get("name", "")).lower() == name.lower()]
            require(len(matches) == 1, "Exactly one DSv5 and regional quota counter is required")
            used, limit = (counter(matches[0].get(key)) for key in ("currentValue", "limit"))
            require(limit - used >= 24, "Current DSv5/regional headroom must cover 16 now plus 8 future Prom cores")
            counters[name] = {"used": used, "limit": limit, "remaining": limit - used}
        rows = self.az_json("vm", "list-skus", "--location", base.REGION, "--resource-type", "virtualMachines",
                            "--size", prom.VM_SIZE, "--all", "--query", prom.SKU_QUERY)
        require(isinstance(rows, list) and len(rows) == 1, "Exactly one actual DSv5 SKU is required")
        sku = rows[0]
        caps = {row["name"]: row.get("value") for row in sku.get("capabilities") or []}
        require(sku.get("name") == prom.VM_SIZE and sku.get("family") == prom.QUOTA_FAMILY
                and sku.get("resourceType") == "virtualMachines" and sku.get("restrictions") == []
                and base.REGION in [str(value).lower() for value in sku.get("locations") or []]
                and counter(caps.get("vCPUs")) == 8
                and counter(caps.get("MemoryGB")) == 32
                and caps.get("CpuArchitectureType") == "x64" and caps.get("PremiumIO") == "True"
                and counter(caps.get("OSVhdSizeMB")) >= 256 * 1024,
                "SKU restrictions, CPU/memory, architecture, or managed OS disk support changed")
        self.summary["quota_proof"] = {"observed_at": workers.utc_now(), "required_cores": 24,
                                       "counters": counters, "azure_reservation_claimed": False}
        self.save()

    def models(self, snapshot):
        evidence = {"observed_at": workers.utc_now(), "views": {}, "instances": {}}
        self.summary["arm_diagnostics"] = evidence
        operation = self.az_json("aks", "operation", "show-latest", "--resource-group", base.RESOURCE_GROUP,
                                 "--name", base.CLUSTER, "--query", base.OPERATION_QUERY)
        require(operation.get("status") == "Succeeded" and operation.get("name") and not operation.get("errorCode"),
                "Customer managed-cluster operation must remain quiescent")
        require(base.timestamp(operation.get("startTime"), "customer operation start")
                <= base.timestamp(operation.get("endTime"), "customer operation end") <= datetime.now(timezone.utc),
                "Customer operation timestamps are invalid")
        patch = self.az_json("aks", "show", "--resource-group", base.RESOURCE_GROUP, "--name", base.CLUSTER,
                             "--query", prom.PATCH_QUERY)
        require(prepared.resource_equal(patch.get("id"), self.data["cluster.json"]["id"]), "Patch AKS scope changed")
        version = patch.get("currentKubernetesVersion")
        source = next(row for row in snapshot["nodes"]["items"] if row["metadata"]["name"] == stalled.SOURCE)
        require(isinstance(version, str) and re.fullmatch(r"[1-9]\d*\.\d+\.\d+", version)
                and patch.get("kubernetesVersion") in (version, version.rsplit(".", 1)[0])
                and source["status"]["nodeInfo"].get("kubeletVersion") == f"v{version}"
                and stalled.fresh_node_ready(source), "AKS and healthy default0 must prove the same fresh full patch")
        require(not self.patch or self.patch == version, "Pinned Kubernetes patch changed")
        self.patch = version
        pools = self.az_json("aks", "nodepool", "list", "--resource-group", base.RESOURCE_GROUP,
                             "--cluster-name", base.CLUSTER)
        scales = self.az_json("vmss", "list", "--resource-group", base.NODE_GROUP, "--query", base.VMSS_QUERY)
        evidence.update(operation=operation, pools=pools, vmsses=scales)
        self.save()
        require(isinstance(pools, list) and isinstance(scales, list), "Pool/VMSS inventory is malformed")
        pool_map = {row.get("name"): row for row in pools}
        scale_map = {workers.vmss_pool_name(row): row for row in scales}
        permitted = {"default", "prompool"} | ({POOL} if self.submitted else set())
        require(len(pool_map) == len(pools) and len(scale_map) == len(scales)
                and {"default", "prompool"} <= set(pool_map) <= permitted
                and {"default", "prompool"} <= set(scale_map) <= permitted,
                "Existing cniv5, unknown pool/VMSS, or missing original capacity")
        old_pools = {row["name"]: row for row in self.data["pool-configuration.json"]}
        old_pin = {"operation": operation["name"], "pools": {}, "vmsses": {}}
        for name, vmss, count in (("default", base.DEFAULT_VMSS, 2), ("prompool", base.PROM_VMSS, 0)):
            pool, scale = pool_map[name], scale_map[name]
            require(base.integer(pool.get("count")) and pool["count"] == count
                    and pool.get("enableAutoScaling") is False and pool.get("provisioningState") == "Succeeded"
                    and pool.get("powerState", {}).get("code") == "Running"
                    and pool.get("vmSize") == "Standard_D8_v3"
                    and pool.get("mode") == ("System" if count else "User"), "Original pool state/count changed")
            for key, value in prepared.pool_configuration(old_pools[name]).items():
                require(canonical(pool.get(key)) == canonical(value), f"Original {name} setting changed: {key}")
            self.validate_scale(scale, vmss, name, count, "Standard_D8_v3",
                                {"Failed"} if name == "default" else {"Succeeded"})
            instances = self.az_json("vmss", "list-instances", "--resource-group", base.NODE_GROUP,
                                     "--name", vmss, "--query", base.VM_QUERY)
            evidence["instances"][vmss] = instances
            require(isinstance(instances, list) and len(instances) == count, "Original VM count changed")
            if count:
                require({str(row.get("instanceId")) for row in instances} == {"0", "1"}, "Default VM instances changed")
                for row in instances:
                    node_name = stalled.SOURCE if str(row["instanceId"]) == "0" else stalled.TARGET
                    require(row.get("computerName") == node_name and row.get("vmId") == stalled.VM_IDS[node_name]
                            and prepared.resource_equal("azure://" + str(row.get("id")),
                                                        self.old_nodes[node_name]["spec"]["providerID"]),
                            "Protected/default VM identity changed")
                    self.validate_original_instance_state(row, node_name)
                    view = self.az_json("vmss", "get-instance-view", "--resource-group", base.NODE_GROUP,
                                        "--name", vmss, "--instance-id", str(row["instanceId"]), "--query", stalled.VIEW_QUERY)
                    evidence["views"][node_name] = view
                    self.validate_original_instance_view(view, node_name)
                aggregate = self.az_json("vmss", "get-instance-view", "--resource-group", base.NODE_GROUP,
                                         "--name", vmss, "--query", base.SCALE_VIEW_QUERY)
                self.validate_default_aggregate(aggregate)
                evidence["default_aggregate"] = aggregate
            old_pin["pools"][name] = prepared.pool_configuration(pool)
            old_pin["vmsses"][name] = scale
        old_pin = canonical(old_pin)
        require(self.old_arm is None or self.old_arm == old_pin, "Original customer pool/VMSS model changed")
        self.old_arm = old_pin
        if POOL not in pool_map or POOL not in scale_map:
            return None
        return self.new_models(pool_map[POOL], scale_map[POOL], evidence)

    def validate_original_instance_state(self, row, node_name):
        require(row.get("provisioningState") == "Succeeded" and row.get("latestModelApplied") is True,
                f"{node_name}: protected/default VM provisioning or applied model changed")

    def validate_original_instance_view(self, view, node_name):
        codes = {entry.get("code") for entry in view.get("statuses") or []}
        require(codes == {"ProvisioningState/succeeded", "PowerState/running"}, "An old VM changed power/state")
        if node_name == stalled.SOURCE:
            require(stalled.guest_state(view, max_age_seconds=300) == "ready" and stalled.extensions_ready(view),
                    "Healthy default0 guest/system health changed")
        else:
            require(stalled.guest_state(view, max_age_seconds=300) == "unresponsive",
                    "Failed VM1 is no longer the freshly observed unresponsive host")

    def validate_default_aggregate(self, aggregate):
        rows = replacement.status_rows(aggregate, "live default aggregate")
        require(len(rows) == 1 and rows[0]["code"] == FAILURE
                and base.timestamp(rows[0].get("time"), "live default failure").isoformat() == self.proof["aggregate_time"],
                "Only the exact correlated terminal default-VMSS failure may remain degraded")

    @staticmethod
    def validate_scale(scale, vmss, pool, count, size, states):
        resource_id = (f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.NODE_GROUP}"
                       f"/providers/Microsoft.Compute/virtualMachineScaleSets/{vmss}")
        require(scale.get("name") == vmss and prepared.resource_equal(scale.get("id"), resource_id)
                and str(scale.get("location", "")).lower() == base.REGION
                and workers.vmss_pool_name(scale) == pool and scale.get("orchestrationMode") == "Uniform"
                and base.integer(scale.get("sku", {}).get("capacity")) and scale["sku"]["capacity"] == count
                and scale["sku"].get("name") == size and scale.get("provisioningState") in states,
                "VMSS resource, owner, region, SKU, capacity, or state is outside the bounded contract")

    def new_models(self, pool, scale, evidence):
        require(self.submitted and self.summary["create"]["accepted"] is True, "New capacity has no accepted owned request")
        desired = pool_settings(self.patch)
        for key, value in desired.items():
            actual = pool.get(key)
            if key in ("nodeLabels", "nodeTaints"):
                actual = actual or None
            require(canonical(actual) == canonical(value)
                    and (not isinstance(value, bool) or actual is value), f"cniv5 setting changed: {key}")
        pool_id = f"{self.data['cluster.json']['id']}/agentPools/{POOL}"
        initializing = pool.get("provisioningState") in CREATING or scale.get("provisioningState") in CREATING
        power = pool.get("powerState")
        require(prepared.resource_equal(pool.get("id"), pool_id)
                and pool.get("provisioningState") in CREATING | {"Succeeded"}
                and ((power or {}).get("code") == "Running" or initializing and not power),
                "New pool ownership/state changed")
        self.summary["pool_created"] = True
        count = (scale.get("sku") or {}).get("capacity")
        require(base.integer(count) and count in ({0, 1, 2} if initializing else {2}),
                "New VMSS capacity is outside its bounded owned initialization")
        self.validate_scale(scale, scale.get("name"), POOL, count, prom.VM_SIZE, CREATING | {"Succeeded"})
        stable_model = copy.deepcopy(scale)
        stable_model.pop("provisioningState", None)
        stable_model["sku"].pop("capacity", None)
        pin = canonical(stable_model)
        require(self.new_scale is None or self.new_scale == pin, "Owned new VMSS identity/model changed")
        self.new_scale = pin
        image = pool.get("nodeImageVersion")
        require(initializing and image is None or isinstance(image, str) and image.startswith("AKSUbuntu-"),
                "The actual new node image is missing or not Ubuntu")
        instances = self.az_json("vmss", "list-instances", "--resource-group", base.NODE_GROUP,
                                 "--name", scale["name"], "--query", base.VM_QUERY)
        require(isinstance(instances, list) and len(instances) <= 2
                and len({str(row.get("instanceId")) for row in instances}) == len(instances),
                "The owned new VM inventory is duplicated or exceeds two")
        aggregate = self.az_json("vmss", "get-instance-view", "--resource-group", base.NODE_GROUP,
                                 "--name", scale["name"], "--query", base.SCALE_VIEW_QUERY)
        evidence.update(new_pool=pool, new_instances=instances, new_aggregate=aggregate)
        aggregate_rows = aggregate.get("virtualMachines")
        if aggregate_rows is None or aggregate_rows == []:
            require(initializing, "Missing VMSS aggregate is only permitted during owned initialization")
            aggregate_rows = []
        require(isinstance(aggregate_rows, list) and all(
            isinstance(row, dict) and base.integer(row.get("count")) and 0 <= row["count"] <= 2
            and row.get("code") in {"ProvisioningState/succeeded", "ProvisioningState/creating", "ProvisioningState/updating"}
            for row in aggregate_rows) and sum(row["count"] for row in aggregate_rows) <= 2,
            "New aggregate failure/unknown state or extra capacity")
        scale_statuses = aggregate.get("statuses")
        require(scale_statuses is None or isinstance(scale_statuses, list), "New VMSS status is malformed")
        require(all(isinstance(row, dict) and row.get("code") in {
            "ProvisioningState/succeeded", "ProvisioningState/creating", "ProvisioningState/updating",
        } for row in scale_statuses or []), "New VMSS reports a failure")
        healthy = (not initializing and (power or {}).get("code") == "Running"
                   and count == 2 and len(instances) == 2 and len(aggregate_rows) == 1
                   and aggregate_rows[0]["code"] == "ProvisioningState/succeeded" and aggregate_rows[0]["count"] == 2
                   and isinstance(scale_statuses, list) and len(scale_statuses) == 1
                   and scale_statuses[0].get("code") == "ProvisioningState/succeeded")
        current = {}
        for vm in instances:
            instance = str(vm.get("instanceId", ""))
            require(instance.isascii() and instance.isdecimal() and str(int(instance)) == instance
                    and prepared.resource_equal(vm.get("id"), f"{scale['id']}/virtualMachines/{instance}")
                    and vm.get("provisioningState") in CREATING | {"Succeeded"}, "New VM scope/instance/state is invalid")
            name, vm_id = vm.get("computerName"), vm.get("vmId")
            complete = isinstance(name, str) and bool(name) and quantities.valid_uuid(vm_id)
            require((complete or initializing)
                    and (name is None or isinstance(name, str) and base.NAME_RE.fullmatch(name))
                    and (vm_id is None or quantities.valid_uuid(vm_id))
                    and (complete or instance not in self.new_vms),
                    "New VM guest identity is invalid or disappeared")
            if complete:
                identity = {"node_name": name, "vm_id": vm_id, "instance_id": instance,
                            "provider_id": "azure://" + vm["id"].lower()}
                require(name not in self.old_nodes and vm_id not in {*stalled.VM_IDS.values(), base.FAILED_PROM_VM_ID}
                        and (instance not in self.new_vms or self.new_vms[instance] == identity),
                        "New VM reused or changed an original/observed identity")
                self.new_vms[instance] = identity
                current[name] = identity
            view = self.az_json("vmss", "get-instance-view", "--resource-group", base.NODE_GROUP,
                                "--name", scale["name"], "--instance-id", instance, "--query", stalled.VIEW_QUERY)
            evidence["views"][f"{scale['name']}/{instance}"] = view
            statuses = view.get("statuses")
            require(statuses is None and initializing or isinstance(statuses, list), "New VM status is malformed")
            codes = {row.get("code") for row in statuses or []}
            require(codes <= {"ProvisioningState/succeeded", "ProvisioningState/creating", "ProvisioningState/updating",
                             "PowerState/running", "PowerState/starting"}, "New VM reports failure or unknown power state")
            extensions = replacement.extension_states(view, pending=initializing, allow_missing=initializing)
            healthy = healthy and complete and vm.get("latestModelApplied") is True and vm["provisioningState"] == "Succeeded"
            healthy = healthy and codes == {"ProvisioningState/succeeded", "PowerState/running"}
            healthy = healthy and bool(extensions) and all(value == ["ProvisioningState/succeeded"] for value in extensions.values())
        require(set(self.new_vms) <= {str(row["instanceId"]) for row in instances},
                "An observed new VM disappeared")
        require(len({row["vm_id"] for row in current.values()}) == len(current), "New VM IDs are duplicated")
        model = self.az_json("vmss", "show", "--resource-group", base.NODE_GROUP, "--name", scale["name"],
                             "--query", prom.VMSS_MODEL_QUERY)
        evidence["new_vmss_model"] = model
        require(prepared.resource_equal(model.get("id"), scale["id"]), "New VMSS disk/image evidence is out of scope")
        disk = model.get("osDisk") or {}
        disk_ok = (disk.get("osType") == "Linux" and disk.get("diskSizeGb") == 256
                   and isinstance(disk.get("managedDisk"), dict) and bool(disk["managedDisk"].get("storageAccountType"))
                   and not disk.get("diffDiskOption") and isinstance(image, str)
                   and prom.image_matches_pool(model.get("imageReference") or {}, image))
        require(disk_ok or initializing, "Actual new VMSS is not the pinned managed OS disk/image")
        return {"healthy": healthy and disk_ok, "vms": current, "pool": pool, "vmss": scale}

    def guard(self, snapshot, new):
        self.summary["kubernetes_diagnostics"] = stalled.safe_diagnostics(snapshot)
        self.save()
        require(stalled.controllers_pin(snapshot["controllers"]) == self.controllers
                and base.frozen_pdbs(snapshot) == self.pdbs, "Captured controller/PDB contracts changed")
        rows = snapshot["nodes"]["items"]
        nodes = {row["metadata"]["name"]: row for row in rows}
        require(len(nodes) == len(rows) and set(self.old_nodes) <= set(nodes), "Original Node/KWOK inventory changed")
        for name, original in self.old_nodes.items():
            node = nodes[name]
            require(stalled.logical_node(node) == stalled.logical_node(original)
                    and not node["metadata"].get("deletionTimestamp"), f"{name}: protected Node UID/logical spec changed")
            if name in stalled.BOOTS:
                require(base.node_boot(node) == stalled.BOOTS[name], "An old worker rebooted")
        require(stalled.fresh_node_ready(nodes[stalled.SOURCE]), "Healthy default0 lost fresh readiness")
        by_uid = {uid(pod): pod for pod in snapshot["pods"]["items"]}
        require(len(by_uid) == len(snapshot["pods"]["items"]), "Pod UIDs are duplicated")
        for pod in snapshot["pods"]["items"]:
            if pod["spec"].get("nodeName") == stalled.SOURCE and base.pod_ready(pod):
                self.protected.setdefault(uid(pod), stalled.pod_pin(pod))
        for pod_uid, pin in self.protected.items():
            if pod_uid == ROLLED_SECURITY_POD_UID and self.resume:
                old = by_uid.get(pod_uid)
                require(old is None or old["metadata"].get("deletionTimestamp"),
                        "Known terminating security Pod lost its recorded deletion state")
                if old is not None and old["metadata"].get("deletionTimestamp"):
                    require(stalled.pod_pin(old) == pin and base.pvc_free(old["spec"])
                            and old["metadata"]["deletionTimestamp"] == self.security_deletion,
                            "Known terminating security Pod identity/spec/PVC changed")
                    # Container shutdown is expected here; this is not a healthy-Pod exemption.
                    self.summary["managed_security_rollout"] = {
                        "old_uid": pod_uid, "controller_uid": SECURITY_OWNER[2],
                        "state": "already-terminating", "deletion_timestamp": self.security_deletion,
                        "healthy": False, "caused_by_this_recovery": False,
                    }
                    continue
                if old is None:
                    replacements = [
                        pod for pod in snapshot["pods"]["items"]
                        if pod["metadata"].get("namespace") == SECURITY_OWNER[0]
                        and pod["spec"].get("nodeName") == stalled.SOURCE
                        and any(ref.get("controller") is True and ref.get("kind") == "DaemonSet"
                                and ref.get("name") == SECURITY_OWNER[1] and ref.get("uid") == SECURITY_OWNER[2]
                                for ref in pod["metadata"].get("ownerReferences") or [])
                    ]
                    require(len(replacements) == 1 and base.pod_ready(replacements[0])
                            and base.pvc_free(replacements[0]["spec"]) and quantities.valid_uuid(uid(replacements[0])),
                            "Known security rollout has no single healthy, controller-owned replacement")
                    base.controller_owner(replacements[0], "DaemonSet")
                    self.summary["managed_security_rollout"] = {
                        "old_uid": pod_uid, "new_uid": uid(replacements[0]), "controller_uid": SECURITY_OWNER[2],
                        "state": "healthy-controller-replacement", "healthy": True, "caused_by_this_recovery": False,
                    }
                    continue
            require(pod_uid in by_uid and base.pod_ready(by_uid[pod_uid]) and stalled.pod_pin(by_uid[pod_uid]) == pin,
                    "A protected healthy default0 Pod UID/spec/readiness changed")
        agents = maintenance._agent_map(snapshot["pods"])
        require(set(agents) == set(self.agents), "A current mock Pod disappeared or appeared")
        for name, original in self.agents.items():
            require(stalled.pod_pin(agents[name]) == stalled.pod_pin(original)
                    and mocks._pod_owned_by_controller_uid(agents[name], self.mock_uid)
                    and bool(agents[name]["metadata"].get("deletionTimestamp")) == bool(original["metadata"].get("deletionTimestamp")),
                    "Current mock UID/spec/termination changed; this phase authorizes no Pod migration")
        nnc_rows = snapshot["nnc"]["items"]
        nncs = {row["metadata"]["name"]: row for row in nnc_rows}
        require(len(nncs) == len(nnc_rows) and set(nncs) <= set(nodes), "NNC names are duplicated or foreign")
        old_network = maintenance._nnc_map({"items": [nncs[name] for name in self.old_nncs if name in nncs]})
        require(set(old_network) == set(self.old_nncs), "Original NNC disappeared")
        for name, original in self.old_nncs.items():
            require(all(old_network[name][key] == original[key] for key in ("uid", "node_uid", "network_container_id")),
                    "An original NNC identity changed")
        added = set(nodes) - set(self.old_nodes)
        require(len(added) <= 2 and (not added or self.submitted and new is not None), "Unowned extra real Nodes appeared")
        registered = bool(new and new["healthy"]) and len(added) == 2
        derived = {}
        for name in added:
            node = nodes[name]
            vm = new["vms"].get(name)
            maintenance._validate_real_node_scope(node, subscription=base.SUBSCRIPTION, node_resource_group=base.NODE_GROUP)
            require(vm is not None and prepared.resource_equal(node["spec"].get("providerID"), vm["provider_id"])
                    and mocks._node_pool_name(node) == POOL and node["metadata"]["labels"].get("agentpool") == POOL
                    and quantities.valid_uuid(uid(node)) and uid(node) not in {uid(row) for row in self.old_nodes.values()}
                    and not node["metadata"].get("deletionTimestamp") and "prometheus" not in node["metadata"]["labels"],
                    "New Node has no genuine distinct cniv5 VM/provider identity")
            boot = node.get("status", {}).get("nodeInfo", {}).get("bootID")
            require(boot is None or quantities.valid_uuid(boot) and boot not in stalled.BOOTS.values(), "New boot ID is invalid")
            require(name not in self.new_boots or self.new_boots[name] == boot, "An observed new boot changed/disappeared")
            if boot:
                self.new_boots[name] = boot
            pin = {"node_uid": uid(node), "provider_id": vm["provider_id"]}
            require(name not in self.new_nodes or self.new_nodes[name] == pin, "Observed new Node identity changed")
            self.new_nodes[name] = pin
            row = nncs.get(name)
            network = None
            if row is not None:
                owner = base.controller_owner(row, "Node")
                require(row["metadata"].get("namespace") == "kube-system" and quantities.valid_uuid(uid(row))
                        and uid(row) not in {item["uid"] for item in self.old_nncs.values()}
                        and owner["name"] == name and owner["uid"] == uid(node)
                        and not row["metadata"].get("deletionTimestamp"), "New NNC resource/owner identity changed")
                require(name not in self.new_nncs or self.new_nncs[name] == uid(row), "Observed NNC UID changed")
                self.new_nncs[name] = uid(row)
                status = row.get("status")
                require(status is None or isinstance(status, dict), "New NNC status is malformed")
                containers = (status or {}).get("networkContainers")
                if containers is not None and containers != []:
                    network = maintenance._nnc_map({"items": [row]})[name]
                    require(network["network_container_id"] not in {
                        item["network_container_id"] for item in self.old_nncs.values()}, "New NC reused an original identity")
                    require(name not in self.new_containers or self.new_containers[name] == network["network_container_id"],
                            "Observed new network container changed")
                    self.new_containers[name] = network["network_container_id"]
                else:
                    require(name not in self.new_containers, "An initialized new network container disappeared")
            ready = (bool(boot) and mocks._node_ready_and_schedulable(node) and not maintenance._taints(node)
                     and self.daemonsets <= maintenance._healthy_system_daemonsets_on_node(snapshot["pods"], name)
                     and network is not None and replacement.initialized_network(network)
                     and node["metadata"]["labels"].get("kubernetes.azure.com/node-image-version") == new["pool"]["nodeImageVersion"]
                     and node["status"].get("nodeInfo", {}).get("kubeletVersion") == f"v{self.patch}")
            registered = registered and ready
            derived[name] = {**vm, **pin, "boot_id": boot, "nnc_uid": uid(row) if row else None,
                             "network_container_id": network["network_container_id"] if network else None,
                             "node_image_version": node["metadata"]["labels"].get("kubernetes.azure.com/node-image-version"),
                             "registered_ready": bool(ready)}
        require(set(self.new_nodes) <= added, "An observed new Node disappeared")
        require(len({row["node_uid"] for row in derived.values()}) == len(derived), "New Node UIDs are duplicated")
        ids = [row["network_container_id"] for row in derived.values() if row["network_container_id"]]
        require(len(set(ids)) == len(ids), "New network container IDs are duplicated")
        self.summary.update(new_identities=derived,
                            current_mock_uids={name: uid(pod) for name, pod in agents.items()},
                            current_mock_ready=sum(base.pod_ready(pod) for pod in agents.values()),
                            current_kwok_ready=sum(workers.node_is_ready(nodes[name]) for name in maintenance.EXPECTED_AGENT_NAMES))
        self.save()
        return registered

    def observe(self):
        self.unchanged_inputs()
        self.authority()
        snapshot = self.snapshot()
        new = self.models(snapshot)
        return self.guard(snapshot, new)

    def journals(self):
        rows = self.kube("-n", "kube-system", "get", "configmaps", "-o", "json")["items"]
        conflicts = [row for row in rows if row["metadata"]["name"] in (
            JOURNAL, "mesh96-modern-prom-recovery", "mesh96-prom-capacity-restore",
        ) or row["metadata"]["name"].startswith("modern-cni-")]
        return rows, conflicts

    def journal_data(self):
        data = {"owner": OWNER, "token": self.token, "source_state_sha256": digest(self.hashes),
                "restart_checkpoint_sha256": self.receipt_hash, "failure_correlation": CORRELATION,
                "desired_pool_sha256": digest(pool_settings(self.patch)),
                "create": json.dumps(self.summary["create"], sort_keys=True)}
        if self.resume:
            data.update(prior_unsubmitted_create=json.dumps(self.resume["create"], sort_keys=True),
                        prior_checkpoint_sha256=self.resume_hash, prior_build_id=str(RESERVED_BUILD))
        return data

    def attach_capacity_reservation(self):
        conflicts = self.journals()[1]
        require(len(conflicts) == 1 and conflicts[0]["metadata"].get("name") == JOURNAL,
                "The exact original capacity reservation must exist without competing attempts")
        row = conflicts[0]
        data = row.get("data") or {}
        expected = {
            "owner": OWNER, "source_state_sha256": digest(self.hashes),
            "restart_checkpoint_sha256": self.receipt_hash, "failure_correlation": CORRELATION,
            "desired_pool_sha256": digest(pool_settings(self.patch)),
            "create": json.dumps(self.resume["create"], sort_keys=True),
        }
        require(uid(row) == RESERVED_JOURNAL_UID and set(data) == set(expected) | {"token"}
                and all(data.get(key) == value for key, value in expected.items())
                and isinstance(data.get("token"), str) and re.fullmatch(r"[0-9a-f]{32}", data["token"]),
                "Original capacity reservation changed or was already continued")
        self.token, self.journal_uid = data["token"], uid(row)
        self.persisted_journal_data = copy.deepcopy(data)
        self.owned_journal()
        self.summary["journal"].update(
            uid=self.journal_uid, accepted=True, ambiguous=False, attempted=False, continued_existing_reservation=True,
        )
        self.save()

    def write(self, command):
        require(self.args.execute, "Read-only planning must never write")
        is_add = command == add_command(self.patch)
        require(is_add or command[:6] in (
            ["kubectl", "-n", "kube-system", "create", "configmap", JOURNAL],
            ["kubectl", "-n", "kube-system", "patch", "configmap", JOURNAL],
        ), "Write is not the exact new-journal or sole cniv5 add")
        if is_add:
            require(self.journal_uid and not self.submitted and self.summary["create"]["attempted"]
                    and self.summary["create"]["accepted"] is None, "Duplicate or unjournalled pool add")
            self.owned_journal()
            self.submitted = True
            self.summary["create"]["submission_started"] = True
        self.summary["mutation_started"] = True
        self.save()
        return super().run(command, 45)

    def acquire(self):
        require(not self.journals()[1], "An existing capacity attempt prohibits creation/replay")
        self.summary["journal"].update(attempted=True, accepted=None, ambiguous=True)
        self.save()
        data = self.journal_data()
        result = workers.parse_json(self.write([
            "kubectl", "-n", "kube-system", "create", "configmap", JOURNAL,
            *[f"--from-literal={key}={value}" for key, value in data.items()], "-o", "json",
        ]), "exclusive capacity journal")
        require(uid(result) and result.get("data") == data, "Journal creation outcome is ambiguous")
        self.journal_uid = uid(result)
        self.persisted_journal_data = data
        self.owned_journal()
        self.summary["journal"].update(uid=self.journal_uid, accepted=True, ambiguous=False)
        self.save()

    def owned_journal(self):
        row = self.kube("-n", "kube-system", "get", "configmap", JOURNAL, "-o", "json")
        metadata = row.get("metadata") or {}
        require(uid(row) == self.journal_uid and metadata.get("name") == JOURNAL
                and metadata.get("namespace") == "kube-system" and metadata.get("resourceVersion")
                and not metadata.get("deletionTimestamp") and not metadata.get("ownerReferences")
                and row.get("data") == self.persisted_journal_data,
                "Exclusive capacity journal UID/token/source/configuration/receipt changed")
        return row

    def persist_journal(self):
        self.unchanged_inputs()
        row = self.owned_journal()
        desired = self.journal_data()
        self.write(["kubectl", "-n", "kube-system", "patch", "configmap", JOURNAL, "--type=json", "-p", json.dumps([
            {"op": "test", "path": "/metadata/uid", "value": self.journal_uid},
            {"op": "test", "path": "/metadata/resourceVersion", "value": row["metadata"]["resourceVersion"]},
            {"op": "test", "path": "/data", "value": self.persisted_journal_data},
            {"op": "add", "path": "/data", "value": desired},
        ])])
        self.persisted_journal_data = desired
        self.owned_journal()

    def execute(self):
        self.observe()
        if self.resume:
            self.attach_capacity_reservation()
        else:
            require(not self.journals()[1], "Existing capacity journal blocks plan/adoption")
        self.quota()
        self.summary.update(plan_valid=True, desired_pool=pool_settings(self.patch), status="planned-read-only")
        self.save()
        if not self.args.execute:
            return
        if self.resume:
            self.persist_journal()
        else:
            self.acquire()
        self.observe()
        self.quota()
        self.summary["create"].update(attempted=True, accepted=None, ambiguous=True, requested_at=workers.utc_now(),
                                      command=add_command(self.patch))
        self.save()
        self.persist_journal()
        self.observe()
        self.quota()
        self.owned_journal()
        self.unchanged_inputs()
        self.write(add_command(self.patch))
        self.summary["create"].update(accepted=True, ambiguous=False, accepted_at=workers.utc_now())
        self.save()
        self.persist_journal()
        self.summary["status"] = "observing-owned-pool-registration"
        while True:
            if self.observe():
                self.summary["registered_nodes_ready"] = True
                self.summary["create"]["registration_proven"] = True
                self.persist_journal()
                self.summary.update(success=True, status="pool-and-nodes-registered-further-qualification-required")
                self.save()
                return
            require(time.monotonic() < self.work_deadline, "Owned registration did not complete within its bound")
            time.sleep(min(10, self.remaining_seconds(10)))


def validate_args(args):
    require(args.resource_group == args.confirm_resource_group == base.RESOURCE_GROUP
            and args.expected_subscription.lower() == base.SUBSCRIPTION and args.expected_region.lower() == base.REGION,
            "Only the explicitly approved preserved scope is supported")
    require(quantities.valid_sha(args.expected_tfvars_sha), "Preserved tfvars SHA256 is invalid")
    require(base.integer(args.timeout_seconds) and 300 <= args.timeout_seconds <= 3600, "Timeout must be 300..3600 seconds")
    require(args.kubeconfig and args.context == base.CLUSTER, "Private selected-cluster credentials/context are required")
    require((not getattr(args, "resume_capacity_checkpoint", None) and getattr(args, "resume_build_id", 0) == 0)
            or (getattr(args, "resume_capacity_checkpoint", None)
                and getattr(args, "resume_build_id", 0) == RESERVED_BUILD),
            "Capacity continuation requires the exact unsubmitted reservation from build 79959")
    root, checkpoint, config, output = (Path(value).resolve() for value in (
        args.source_state_directory, args.restart_checkpoint, args.kubeconfig, args.summary_file))
    paths = [root, checkpoint, config, output]
    if getattr(args, "resume_capacity_checkpoint", None):
        paths.append(Path(args.resume_capacity_checkpoint).resolve())
    require(len(set(paths)) == len(paths) and root not in output.parents and not output.exists(),
            "Summary must be new and separate from immutable inputs/private credentials")
    args.role = base.ROLE


def execute_recovery(args, summary, runner=workers.run_command):
    validate_args(args)
    summary.update(schema_version=1, phase="capacity-first-registration-only", execute=args.execute,
                   plan_valid=False, mutation_started=False, success=False, pool_created=False, registered_nodes_ready=False,
                   bootstrap_complete=False, capacity_qualified=False, workloads_ready=False,
                   actual_ip_growth_proven=False, actual_memory_headroom_proven=False,
                   required_before_pod_movement=["both-node actual IP-growth/HTTP proof with UID cleanup",
                                                 "fresh CPU/memory/Pod-slot headroom", "explicit bootstrap/migration plan"],
                   status="validating", started_at=workers.utc_now(),
                   create={"attempted": False, "accepted": None, "ambiguous": False,
                           "submission_started": False, "automatic_retry_allowed": False},
                   journal={"name": JOURNAL, "namespace": "kube-system", "retained": True})
    try:
        data, hashes, receipt_hash, proof = load_inputs(args)
        resume, resume_hash = load_capacity_reservation(args, data, hashes, receipt_hash)
        summary.update(source_hashes=hashes, source_state_sha256=digest(hashes), restart_checkpoint_sha256=receipt_hash,
                       accepted_restart_terminal_proof=proof, plan_sha256=stalled.PLAN_SHA,
                       preserved_kwok_node_uids={row["metadata"]["name"]: uid(row)
                                                for row in data["current-nodes.json"]["items"]
                                                if row["metadata"]["name"] in maintenance.EXPECTED_AGENT_NAMES},
                       original_mock_pod_uids={name: uid(pod) for name, pod in maintenance._agent_map(data["current-pods.json"]).items()},
                       original_default_node_uids={name: base.REAL_UIDS[name] for name in stalled.BOOTS},
                       original_default_vm_ids=stalled.VM_IDS, original_default_boot_ids=stalled.BOOTS)
        if resume:
            summary["continuation"] = {
                "source_build": RESERVED_BUILD, "checkpoint_sha256": resume_hash,
                "prior_unsubmitted_create": copy.deepcopy(resume["create"]),
                "proof": "explicit submission_started=false with exact pre-POST guard failure and original journal",
            }
        CapacityFirst(args, data, hashes, receipt_hash, proof, summary, runner,
                      resume=resume, resume_hash=resume_hash).execute()
    except stalled.EXPECTED_ERRORS as error:
        summary.update(success=False, registered_nodes_ready=False, status="failed-closed",
                       error=f"{type(error).__name__}: {error}", rollback_attempted=False)
        raise
    finally:
        summary["finished_at"] = workers.utc_now()
        mocks.write_json_atomic(args.summary_file, summary)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ("resource-group", "confirm-resource-group", "expected-subscription", "expected-region",
                  "expected-tfvars-sha", "source-state-directory", "restart-checkpoint", "kubeconfig", "summary-file"):
        parser.add_argument(f"--{field}", required=True)
    parser.add_argument("--context", default=base.CLUSTER)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--resume-capacity-checkpoint")
    parser.add_argument("--resume-build-id", type=int, default=0)
    return parser.parse_args(argv)


def main(argv=None):
    def interrupted(signum, _frame):
        raise workers.ReconcileError(f"Interrupted ({signum}); no subsequent mutation is authorized")
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, interrupted)
    args, summary = parse_args(argv), {}
    try:
        execute_recovery(args, summary)
    except stalled.EXPECTED_ERRORS as error:
        print(f"Capacity-first phase failed closed: {error}", file=sys.stderr)
        return 1
    print(f"{summary['status']}; capacity_qualified=false; workloads_ready=false")
    return 0


if __name__ == "__main__":
    sys.exit(main())
