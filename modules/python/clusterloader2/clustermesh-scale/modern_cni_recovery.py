#!/usr/bin/env python3
"""Explicit mesh-96 Dsv5 CNI recovery; never a normal workload-readiness gate.

The original plan is immutable. A successful monitoring recovery receipt is a
prerequisite, not permission to weaken any live identity or health check. The
exclusive Kubernetes journal is deliberately retained on success and failure.
There is no automatic adoption, rollback, provider retry, or second Pod delete.
Read-only success is represented by plan_valid, not repair completion flags.
"""

# pylint: disable=protected-access,too-many-lines,too-many-boolean-expressions

from __future__ import annotations

import argparse
import copy
import ipaddress
import json
import re
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional, Sequence

from kubernetes import client, config

import cni_worker_maintenance as maintenance
import mock_cni_recovery as mocks
import prepared_worker_retirement as prepared
import preserved_worker_reconcile as workers
import unreachable_prom_worker_recovery as base


POOL = "cniv5"
PROM_POOL = "promv5"
SKU = "Standard_D8s_v5"
FAMILY = "standardDSv5Family"
ORIGINAL_PLAN_SHA256 = "1a4385e2db5a0a5b38d750a6a82fcb9b3d4c683e801bf12bd8111c75355e380a"
SOURCE = base.SOURCE_NODE
RETAINED = f"{base.DEFAULT_VMSS}000001"
SOURCE_VM_ID = "c9f1454f-144e-4281-916b-7612f89a31e6"
RETAINED_VM_ID = "d81b78a9-fe40-468d-91ec-d66f0456bfa7"
SUBNET_PREFIX = (
    f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.RESOURCE_GROUP}"
    "/providers/Microsoft.Network/virtualNetworks/clustermesh-shared-vnet/subnets/clustermesh-96-"
)
JOURNAL_KEY = "mock-clustermesh/modern-cni-recovery"
EXCLUSION_KEY = "mock-clustermesh/modern-cni-exclusion"
FINAL_RESERVE = 300
RETIREMENT_RESERVE = 600
MAX_CHECKPOINT_BYTES = 4 * 1024 * 1024
BUSY = {"Creating", "Scaling", "Updating", "Deleting"}
CREATING_STATES = {"Creating", "Scaling", "Updating"}
require = prepared.require
digest = base.digest
uid = base.object_uid


def load_checkpoint(path: str) -> dict:
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "Checkpoint contains duplicate JSON fields")
            result[key] = value
        return result

    with open(path, "rb") as handle:
        data = handle.read(MAX_CHECKPOINT_BYTES + 1)
    require(0 < len(data) <= MAX_CHECKPOINT_BYTES, "Monitoring checkpoint size is invalid")
    result = json.loads(data, object_pairs_hook=unique)
    require(isinstance(result, dict), "Monitoring checkpoint must be an object")
    return result


def validate_checkpoint(plan: dict, checkpoint: dict) -> dict:
    require(digest(plan) == ORIGINAL_PLAN_SHA256, "The original approved plan SHA256 changed")
    require(
        checkpoint.get("repaired") is True and checkpoint.get("phase1_only") is True
        and checkpoint.get("workloads_ready") is False and checkpoint.get("success") is True
        and checkpoint.get("execute") is True and checkpoint.get("plan_sha256") == digest(plan)
        and not checkpoint.get("error") and not checkpoint.get("cleanup_errors")
        and not checkpoint.get("temporary_exclusions") and not checkpoint.get("probe_cleanup_pending"),
        "A genuinely completed, clean, executed modern monitoring receipt is required",
    )
    prom = checkpoint.get("modern_prom")
    required = {
        "pool_name", "pool_resource_id", "vmss_name", "instance_id", "node_name", "node_uid",
        "vm_id", "provider_id", "network_container_id", "pool_configuration_sha256",
        "legacy_empty_pool_retired",
    }
    require(isinstance(prom, dict) and required <= set(prom)
            and prom["pool_name"] == PROM_POOL and prom["legacy_empty_pool_retired"] is True,
            "The completed modern monitoring identity/empty-pool retirement is missing")
    cluster_id = (
        f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.RESOURCE_GROUP}"
        f"/providers/Microsoft.ContainerService/managedClusters/{base.CLUSTER}"
    )
    require(prepared.resource_equal(prom["pool_resource_id"], f"{cluster_id}/agentPools/{PROM_POOL}")
            and re.fullmatch(r"aks-promv5-[0-9]+-vmss", str(prom["vmss_name"])) is not None
            and str(prom["instance_id"]).isdigit(), "Monitoring pool/provider scope is invalid")
    provider = (
        f"azure:///subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.NODE_GROUP}"
        f"/providers/Microsoft.Compute/virtualMachineScaleSets/{prom['vmss_name']}"
        f"/virtualMachines/{prom['instance_id']}"
    )
    require(prepared.resource_equal(prom["provider_id"], provider)
            and prom["node_name"] == f"{prom['vmss_name']}{int(prom['instance_id']):06d}",
            "Monitoring Node/provider binding is not exact")
    for field in ("node_uid", "vm_id", "network_container_id"):
        require(isinstance(prom[field], str) and maintenance.UUID_RE.fullmatch(prom[field]) is not None,
                f"Monitoring {field} is invalid")
    require(prom["node_uid"] not in plan["real_node_uids"].values()
            and maintenance.SHA256_RE.fullmatch(prom["pool_configuration_sha256"]) is not None,
            "Monitoring identity is not a new, configuration-pinned worker")
    require(isinstance(checkpoint.get("controller_pins"), dict) and checkpoint["controller_pins"]
            and isinstance(checkpoint.get("pdb_pins"), dict) and checkpoint["pdb_pins"]
            and isinstance(checkpoint.get("modern_baseline_delta"), dict)
            and checkpoint["modern_baseline_delta"],
            "Original controller/PDB pins and explicit modern baseline delta are required")
    require(isinstance(checkpoint.get("authoritative_identities"), list)
            and len(checkpoint["authoritative_identities"]) == 100,
            "Monitoring receipt lacks the complete authoritative Fleet identities")
    targets = [{
        "namespace": "kube-system", "pod_name": plan["api_pod_name"], "pod_uid": plan["api_pod_uid"],
        "replica_set_uid": plan["api_replica_set_uid"],
    }, *plan.get("framework_pods", [])]
    moves = checkpoint.get("pod_moves")
    require(len(targets) == 5 and isinstance(moves, list) and len(moves) == 5,
            "All original five framework targets must be recovered together")
    seen = set()
    for target in targets:
        matches = [row for row in moves if isinstance(row, dict)
                   and row.get("namespace") == target["namespace"]
                   and row.get("pod_name") == target["pod_name"]]
        require(len(matches) == 1, "Framework recovery receipt is incomplete or duplicated")
        row = matches[0]
        require(row.get("pod_uid") == target["pod_uid"]
                and row.get("replica_set_uid") == target["replica_set_uid"]
                and isinstance(row.get("ready_pod_uid"), str)
                and maintenance.UUID_RE.fullmatch(row["ready_pod_uid"]) is not None
                and row["ready_pod_uid"] not in seen and row.get("ready_node") == prom["node_name"],
                "Framework replacement UID/owner/destination is not pinned by monitoring recovery")
        seen.add(row["ready_pod_uid"])
    return copy.deepcopy(prom)


def _number(value, description: str) -> int:
    try:
        require(not isinstance(value, bool), f"{description}: booleans are not quantities")
        result = Decimal(str(value))
        require(result.is_finite() and result >= 0 and result == result.to_integral_value(),
                f"{description}: expected a nonnegative integer quantity")
        return int(result)
    except (InvalidOperation, ValueError, TypeError) as error:
        raise workers.ReconcileError(f"{description}: invalid quantity") from error


def _metric(row: dict) -> dict:
    result = copy.deepcopy(row)
    observed = base.timestamp(row.get("timestamp"), "metrics timestamp")
    require(0 <= (datetime.now(timezone.utc) - observed).total_seconds() <= 180, "Metrics are stale")
    result["timestamp"] = observed.isoformat(timespec="microseconds")
    return result


def _node_pin(node: dict, nnc: dict) -> dict:
    spec = copy.deepcopy(node["spec"])
    spec.setdefault("taints", [])
    spec.setdefault("unschedulable", False)
    return {
        "node_uid": uid(node), "provider_id": node["spec"]["providerID"].lower(),
        "boot_id": base.node_boot(node), "labels": copy.deepcopy(node["metadata"].get("labels") or {}),
        "spec": spec,
        "annotations": copy.deepcopy(node["metadata"].get("annotations") or {}),
        "nnc_uid": nnc["uid"], "network_container_id": nnc["network_container_id"],
    }


def _pod_pin(pod: dict) -> dict:
    return {"uid": uid(pod), "name": pod["metadata"]["name"], "namespace": pod["metadata"]["namespace"],
            "node_name": pod["spec"]["nodeName"], "owner": copy.deepcopy(pod["metadata"]["ownerReferences"])}


def _source_evidence(snapshot: dict, pod: dict) -> dict:
    base.never_started(pod)
    require(mocks._pending_container_creating(pod) and pod["spec"].get("nodeName") == SOURCE,
            "Only an originally pinned, source-bound CNI Pending agent may be recovered")
    events = []
    for event in mocks._items(snapshot["events"], "CNI events"):
        obj = event.get("involvedObject") or {}
        if (obj.get("uid") != uid(pod) or obj.get("name") != pod["metadata"]["name"]
                or obj.get("namespace") != mocks.DEFAULT_NAMESPACE
                or not mocks._event_proves_cni_exhaustion(event)):
            continue
        stamp = ((event.get("series") or {}).get("lastObservedTime")
                 or event.get("lastTimestamp") or event.get("eventTime"))
        observed = base.timestamp(stamp, "current source CNI event")
        if 0 <= (datetime.now(timezone.utc) - observed).total_seconds() <= 600:
            host = (event.get("source") or {}).get("host")
            require(not host or host == SOURCE, "CNI exhaustion event reports a different source")
            events.append(observed.isoformat())
    require(events, "An original Pending UID lacks CURRENT source-NC CNI exhaustion evidence")
    return {"pod_uid": uid(pod), "source_node_uid": base.REAL_UIDS[SOURCE],
            "network_container_id": base.SOURCE_NC, "last_observed_at": max(events)}


def _pdb_allows(snapshot: dict, pod: dict, *, eviction=False) -> None:
    labels = pod["metadata"].get("labels") or {}
    for pdb in mocks._items(snapshot["pdbs"], "PDBs"):
        if pdb["metadata"].get("namespace") != pod["metadata"].get("namespace"):
            continue
        selector = (pdb.get("spec") or {}).get("selector")
        require(isinstance(selector, dict), "PDB selector is unreadable")
        matches = all(labels.get(key) == value for key, value in (selector.get("matchLabels") or {}).items())
        matches = matches and all(
            mocks._requirement_matches(labels, row) for row in selector.get("matchExpressions", [])
        )
        if matches:
            status = pdb.get("status") or {}
            require(_number(status.get("observedGeneration"), "PDB observed generation")
                    >= _number(pdb["metadata"].get("generation"), "PDB generation"),
                    "A matching PDB status has not observed its current specification")
            allowed = _number(status.get("disruptionsAllowed"), "PDB allowed disruptions") > 0
            if eviction and not maintenance._readiness_condition_true(pod):
                # The Eviction API remains authoritative; this mirrors its
                # unhealthy-Pod policy, without changing the PDB or force deleting.
                allowed = allowed or pod.get("status", {}).get("phase") == "Pending"
                allowed = allowed or pdb.get("spec", {}).get("unhealthyPodEvictionPolicy") == "AlwaysAllow"
                if not allowed:
                    allowed = (_number(status.get("currentHealthy"), "PDB current healthy")
                               >= _number(status.get("desiredHealthy"), "PDB desired healthy"))
            require(allowed, "A matching PDB does not currently permit this disruption")


def evict_pod_with_uid_precondition(cluster, *, namespace, name, pod_uid, timeout_seconds):
    """Use policy/v1 eviction, never drain --force or --disable-eviction."""

    api_client = config.new_client_from_config(config_file=cluster.kubeconfig, context=cluster.context)
    try:
        api = client.CoreV1Api(api_client)
        api.create_namespaced_pod_eviction(
            name=name, namespace=namespace,
            body=client.V1Eviction(
                api_version="policy/v1", kind="Eviction",
                metadata=client.V1ObjectMeta(name=name, namespace=namespace),
                delete_options=client.V1DeleteOptions(preconditions=client.V1Preconditions(uid=pod_uid)),
            ),
            _request_timeout=(timeout_seconds, timeout_seconds),
        )
    finally:
        api_client.close()


class ModernRecovery(maintenance.ClusterOperator):
    """Compose generic guards around a real, distinctly named modern System pool."""

    def __init__(self, args, plan, checkpoint, summary, runner, delete_pod, evict_pod):
        deadline = time.monotonic() + args.timeout_seconds
        super().__init__(args, base.CLUSTER, runner, deadline - FINAL_RESERVE, deadline)
        self.plan, self.checkpoint, self.summary = plan, checkpoint, summary
        self.prom = validate_checkpoint(plan, checkpoint)
        self.delete_pod, self.evict_pod = delete_pod, evict_pod
        self.cluster = mocks.Cluster(base.ROLE, args.kubeconfig, args.context, base.CLUSTER, base.RESOURCE_GROUP)
        self.authority_pin: Optional[dict] = None
        self.identities = []
        self.node_pins = {}
        self.nnc_config_pins = {}
        self.agent_nodes = {}
        self.pool_pins = {}
        self.vmss_pins = {}
        self.vm_pins = {}
        self._observed_instances = {}
        self.fresh = {}
        self.initial = None
        self.protected = dict(plan["ready_mock_pod_uids"])
        self.effective = dict(plan["mock_pod_uids"])
        self.framework = {}
        self.original_source = set()
        self.pending = sorted(set(plan["mock_pod_uids"]) - set(plan["ready_mock_pod_uids"]))
        self.healthy_phase = False
        self.retired = False
        self.inflight = None
        self.drain_inflight = None
        self.hold = False
        self.exclusions = {}
        self.memory = {}
        self.memory_high_water = 0
        self.journal_name = f"modern-cni-{digest(plan)[:16]}"
        self.journal_uid = ""
        self.token = uuid.uuid4().hex
        self.desired = {}
        self.template = {}
        self.patch_version = ""
        self.daemonsets = set()

    def save(self):
        mocks.write_json_atomic(self.args.summary_file, self.summary)

    def run(self, command, timeout_seconds=45, *, cleanup=False):
        command = list(command)
        if command[0] == "az":
            read = command[1:3] in (
                ["account", "show"], ["group", "show"], ["aks", "list"], ["aks", "show"],
                ["vmss", "list"], ["vmss", "list-instances"], ["vmss", "get-instance-view"],
                ["vm", "list-usage"], ["vm", "list-skus"],
            ) or command[1:4] in (
                ["fleet", "member", "list"], ["aks", "nodepool", "list"], ["aks", "operation", "show-latest"],
            )
        else:
            read = command[0] == "kubectl" and "get" in command and not any(
                word in command for word in ("patch", "delete", "run", "create", "apply", "drain")
            )
            if "exec" in command:
                read = "--" in command and command[command.index("--") + 1:] == [
                    "cilium-dbg", "status", "-o", "json",
                ]
        require(read, "Mutation or unsupported command on the read-only path")
        return super().run(command, min(timeout_seconds, self.args.request_timeout_seconds), cleanup=cleanup)

    def kube(self, *command):
        return workers.parse_json(
            self.run(["kubectl", f"--request-timeout={self.args.request_timeout_seconds}s", *command]),
            "modern CNI Kubernetes read",
        )

    def kubectl(self, command, *, timeout_seconds=45, cleanup=False):
        # The shared probe creator records its intent before reaching this path.
        if "run" in command:
            name = command[command.index("run") + 1]
            intents = [row for row in maintenance._probe_intents(self.summary) if row["name"] == name]
            require(len(intents) == 1 and not intents[0]["created"], "Probe create is not an owned first attempt")
            return self.action(
                f"probe-create/{name}", {"probe": copy.deepcopy(intents[0])},
                lambda: self.raw_write(["kubectl", *command]),
            )
        return super().kubectl(command, timeout_seconds=timeout_seconds, cleanup=cleanup)

    def raw_write(self, command):
        require(self.args.execute, "Read-only planning cannot mutate")
        return super().run(command, self.args.request_timeout_seconds, cleanup=self.cleanup_mode)

    def _journal_data(self):
        return {
            "owner": JOURNAL_KEY, "token": self.token, "plan_sha256": digest(self.plan),
            "modern_prom_checkpoint_sha256": digest(self.checkpoint),
            "source_node_uid": base.REAL_UIDS[SOURCE], "desired_pool_sha256": digest(self.desired),
            "receipt": json.dumps({
                "actions": self.summary["actions"], "pod_moves": self.summary["pod_moves"],
                "source_hold": self.summary.get("source_hold"),
                "temporary_exclusions": self.summary.get("temporary_exclusions", []),
                "probe_cleanup_pending": self.summary.get("probe_cleanup_pending", []),
                "status": self.summary["status"],
                "baseline_pool_layout_sha256": (
                    digest(self.summary["baseline_pool_layout"])
                    if "baseline_pool_layout" in self.summary else ""
                ),
            }, sort_keys=True, separators=(",", ":")),
        }

    def persist_journal(self):
        current = self.kube("-n", "kube-system", "get", "configmap", self.journal_name, "-o", "json")
        require(uid(current) == self.journal_uid
                and (current.get("data") or {}).get("token") == self.token,
                "Exclusive operation journal ownership changed")
        self.raw_write([
            "kubectl", "-n", "kube-system", "patch", "configmap", self.journal_name, "--type=json", "-p",
            json.dumps([
                {"op": "test", "path": "/metadata/uid", "value": self.journal_uid},
                {"op": "test", "path": "/metadata/resourceVersion",
                 "value": current["metadata"]["resourceVersion"]},
                {"op": "add", "path": "/data", "value": self._journal_data()},
            ]),
        ])

    def action(self, key, detail, operation):
        require(self.args.execute and self.journal_uid, "Mutation requires the exclusive owned operation journal")
        actions = self.summary["actions"]
        require(key not in actions and all(row.get("accepted") is True and not row.get("ambiguous")
                                          for row in actions.values()),
                "A duplicate or ambiguous operation blocks every subsequent mutation")
        record = {**copy.deepcopy(detail), "attempted": True, "accepted": None, "ambiguous": True,
                  "requested_at": workers.utc_now()}
        if key in ("pool-create", "source-retirement"):
            pool_name = POOL if key == "pool-create" else "default"
            prior = self.summary["last_arm_proof"]["pool_operations"].get(pool_name) or {}
            record["previous_operation_name"] = prior.get("name")
        actions[key] = record
        self.summary["mutation_started"] = True
        self.save()
        self.persist_journal()
        result = operation()
        record.update(accepted=True, ambiguous=False, accepted_at=workers.utc_now())
        self.save()
        self.persist_journal()
        return result

    def authority(self):
        selected, connected = base.Recovery.authority(self, strict=True)
        require(connected and sorted(self.identities, key=lambda row: row["role"])
                == sorted(self.checkpoint["authoritative_identities"], key=lambda row: row["role"]),
                "The original authoritative 100-cluster/Fleet map changed")
        return selected

    def snapshot(self):
        return base.Recovery.snapshot(self)

    def _operation(self, phase, pool_name=""):
        # Cluster show-latest can remain PutManagedCluster from September 4
        # throughout child pool actions. Query the genuine agentPool scope too.
        operation = self.az_json(
            "aks", "operation", "show-latest", "--resource-group", base.RESOURCE_GROUP,
            "--name", base.CLUSTER, *(["--nodepool-name", pool_name] if pool_name else []),
            "--query", base.OPERATION_QUERY,
        )
        require(isinstance(operation, dict) and operation.get("name") and not operation.get("errorCode"),
                "Latest AKS operation is absent, failed, or ambiguous")
        start = base.timestamp(operation.get("startTime"), "AKS operation start")
        require((start - datetime.now(timezone.utc)).total_seconds() <= 30, "AKS operation starts in the future")
        terminal = operation.get("status") == "Succeeded"
        provider_phase = phase == "creating" and pool_name == POOL or phase == "retiring" and pool_name == "default"
        key = "pool-create" if phase == "creating" else "source-retirement"
        record = self.summary["actions"].get(key) or {}
        if terminal:
            end = base.timestamp(operation.get("endTime"), "AKS operation end")
            require(start <= end and (end - datetime.now(timezone.utc)).total_seconds() <= 30,
                    "AKS operation timestamps are invalid")
        else:
            require(provider_phase and operation.get("status") in ("InProgress", "Running", *BUSY),
                    "Latest AKS operation is failed, busy, or unrelated")
        if provider_phase:
            require(record.get("accepted") is True, "Cannot observe a provider request without its accepted receipt")
            if operation["name"] == record.get("previous_operation_name"):
                require(terminal, "The prior provider operation became busy again")
                return operation

            allowed_types = (
                {"PutAgentPool", "CreateAgentPool", "CreateOrUpdateAgentPool", "AgentPoolCreate", "AgentPoolCreateOrUpdate"}
                if phase == "creating" else {"DeleteMachines", "DeleteAgentPoolMachines", "AgentPoolDeleteMachines"}
            )
            require(operation.get("operationType") in allowed_types
                    and start >= base.timestamp(record["requested_at"], "owned request")
                    and operation["name"] != self.summary["initial_operation"]["name"],
                    "Only the causally bound accepted pool request may be observed in progress")
            prior = record.setdefault("operation_name", operation["name"])
            require(prior == operation["name"], "A different provider operation superseded our request")
        else:
            expected = (self.summary.get("last_pool_operations", {}).get(pool_name)
                        if pool_name else self.summary.get("cluster_operation_name"))
            require(not expected or operation["name"] == expected,
                    "An unrelated provider operation overlapped this exclusive recovery")
        return operation

    def _owns_unqualified_creation(self, phase, operation):
        record = self.summary["actions"].get("pool-create") or {}
        return (
            phase == "creating" and self.args.execute and bool(self.journal_uid)
            and not self.fresh and POOL not in self.pool_pins
            and record.get("accepted") is True and record.get("ambiguous") is False
            and isinstance(operation, dict) and bool(record.get("operation_name"))
            and operation.get("name") == record["operation_name"]
            and operation.get("status") in {"Succeeded", "InProgress", "Running", *CREATING_STATES}
        )

    def models(self, phase="steady"):
        operation = self._operation(phase)
        cluster = self.az_json("aks", "show", "--resource-group", base.RESOURCE_GROUP, "--name", base.CLUSTER)
        authority = self.authority_pin or {}
        require(prepared.resource_equal(cluster.get("id"), authority.get("clusters", {}).get(base.ROLE, "")),
                "Live AKS identity changed")
        patch = cluster.get("currentKubernetesVersion") or cluster.get("kubernetesVersion")
        require(isinstance(patch, str) and re.fullmatch(r"1\.35\.\d+", patch) is not None,
                "Read the exact current AKS Kubernetes patch; a minor version is not sufficient")
        require(not self.patch_version or patch == self.patch_version, "The control-plane patch changed")
        pools = self.az_json("aks", "nodepool", "list", "--resource-group", base.RESOURCE_GROUP,
                             "--cluster-name", base.CLUSTER)
        vmsses = self.az_json("vmss", "list", "--resource-group", base.NODE_GROUP, "--query", base.VMSS_QUERY)
        require(isinstance(pools, list) and isinstance(vmsses, list), "ARM pool/VMSS inventory is malformed")
        by_pool = {row.get("name"): row for row in pools}
        expected = {"default", PROM_POOL} | ({POOL} if "pool-create" in self.summary["actions"] else set())
        missing_new = phase == "creating" and POOL not in by_pool
        require(len(by_pool) == len(pools) and set(by_pool) == expected - ({POOL} if missing_new else set()),
                "Unexpected pool, duplicate cniv5, or unrelated pool disappearance")
        vmss_by_pool = {workers.vmss_pool_name(row): row for row in vmsses}
        require(len(vmss_by_pool) == len(vmsses) and set(vmss_by_pool) <= expected
                and {"default", PROM_POOL} <= set(vmss_by_pool),
                "Only genuine owned default/promv5/cniv5 VMSS resources are permitted")
        complete = not missing_new and set(vmss_by_pool) == expected
        all_instances = {}
        pool_operations = {}
        for name, pool in by_pool.items():
            expected_count = 2 if name in ("default", POOL) else 1
            if name == "default" and (phase == "retiring" or self.retired):
                expected_count = 1
            count = pool.get("count")
            transitional = phase == "creating" and name == POOL or phase == "retiring" and name == "default"
            require(base.integer(count) and (
                count in (1, 2) if phase == "retiring" and name == "default" else count == expected_count
            ) and pool.get("enableAutoScaling") is False
                and pool.get("provisioningState") in ({"Succeeded"} | (BUSY if transitional else set()))
                and (pool.get("powerState") or {}).get("code") == "Running"
                and prepared.resource_equal(pool.get("id"), f"{cluster['id']}/agentPools/{name}"),
                f"{name}: unsafe count, scaling, power, or pool identity")
            try:
                pool_operation = self._operation(phase, name)
            except workers.ReconcileError as error:
                require(phase == "creating" and name == POOL and pool.get("provisioningState") in BUSY
                        and re.search(r"ResourceNotFound|OperationNotFound|\b404\b", str(error)) is not None,
                        str(error))
                pool_operation = None
            pool_operations[name] = pool_operation
            pool_complete = pool_operation is not None and pool_operation.get("status") == "Succeeded"
            owned = phase == "creating" and name == POOL or phase == "retiring" and name == "default"
            if owned:
                key = "pool-create" if name == POOL else "source-retirement"
                pool_complete = pool_complete and (
                    self.summary["actions"][key].get("operation_name") == pool_operation["name"]
                )
            else:
                self.summary.setdefault("last_pool_operations", {}).setdefault(name, pool_operation["name"])
            complete = complete and pool_complete
            configuration = prepared.pool_configuration(pool)
            if name in self.pool_pins:
                require(configuration == self.pool_pins[name], f"{name}: pool configuration/image changed")
            if name == PROM_POOL:
                require(digest(configuration) == self.prom["pool_configuration_sha256"]
                        and pool.get("mode") == "User" and pool.get("vmSize") == SKU
                        and (pool.get("nodeLabels") or {}).get("prometheus") == "true"
                        and pool.get("maxPods") == 250, "Modern monitoring pool contract drifted")
            elif name == POOL:
                self.validate_new_pool(pool)
            else:
                require(pool.get("mode") == "System" and pool.get("vmSize") == "Standard_D8_v3"
                        and pool.get("osSku") == "Ubuntu" and pool.get("osType") == "Linux"
                        and pool.get("osDiskType") == "Managed" and pool.get("osDiskSizeGb") == 256
                        and pool.get("kubeletDiskType") == "OS" and pool.get("maxPods") == 110
                        and pool.get("enableFips") is False and pool.get("enableNodePublicIp") is False
                        and pool.get("enableEncryptionAtHost") is False
                        and not pool.get("nodeLabels") and not pool.get("nodeTaints")
                        and prepared.resource_equal(pool.get("vnetSubnetId"), SUBNET_PREFIX + "node")
                        and prepared.resource_equal(pool.get("podSubnetId"), SUBNET_PREFIX + "pod"),
                        "The original default pool contract is not intact")
                original_sha = (self.checkpoint.get("original_model_pins") or {}).get("pools", {}).get("default")
                require(not original_sha or digest(configuration) == original_sha,
                        "Original native receipt's default pool configuration changed")
            require(pool.get("orchestratorVersion") in ("1.35", patch)
                    and pool.get("currentOrchestratorVersion", patch) == patch,
                    f"{name}: pool Kubernetes version differs from the current control plane")
            scale = vmss_by_pool.get(name)
            if scale is None:
                require(phase == "creating" and name == POOL, "A protected VMSS disappeared")
                complete = False
                continue
            scale_id = (
                f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.NODE_GROUP}"
                f"/providers/Microsoft.Compute/virtualMachineScaleSets/{scale.get('name')}"
            )
            require(prepared.resource_equal(scale.get("id"), scale_id)
                    and str(scale.get("location", "")).lower() == base.REGION
                    and scale.get("orchestrationMode") == "Uniform"
                    and scale.get("provisioningState") in ({"Succeeded"} | (BUSY if transitional else set()))
                    and scale.get("sku", {}).get("name") == pool["vmSize"],
                    f"{name}: VMSS ownership, SKU, or model state changed")
            capacity = scale.get("sku", {}).get("capacity")
            require(base.integer(capacity) and (
                capacity in (1, 2) if phase == "retiring" and name == "default" else capacity == expected_count
            ), f"{name}: VMSS exceeds its explicitly approved capacity")
            if name == "default":
                require(scale["name"] == base.DEFAULT_VMSS, "The original default VMSS changed")
            if name == PROM_POOL:
                require(scale["name"] == self.prom["vmss_name"], "Monitoring VMSS identity changed")
            contract = {key: value for key, value in scale.items() if key != "provisioningState"}
            contract = copy.deepcopy(contract)
            contract["sku"].pop("capacity", None)
            require(name not in self.vmss_pins or contract == self.vmss_pins[name],
                    f"{name}: the VMSS model changed")
            instances = self.az_json("vmss", "list-instances", "--resource-group", base.NODE_GROUP,
                                     "--name", scale["name"], "--query", base.VM_QUERY)
            require(isinstance(instances, list) and len(instances) <= 2
                    and len({row.get("instanceId") for row in instances}) == len(instances),
                    "VM instance inventory is duplicated or exceeds the cap")
            allowed_ids = {"0", "1"} if name in ("default", POOL) else {str(self.prom["instance_id"])}
            actual_ids = {str(row.get("instanceId")) for row in instances}
            if name == "default" and self.retired:
                allowed_ids = {"1"}
            require(actual_ids <= allowed_ids
                    and (transitional or actual_ids == allowed_ids)
                    and (name != "default" or "1" in actual_ids),
                    f"{name}: original retained/modern VM instance identity changed")
            expected_ids = {"1"} if name == "default" and phase == "retiring" else allowed_ids
            complete = complete and actual_ids == expected_ids
            scale_view = self.az_json(
                "vmss", "get-instance-view", "--resource-group", base.NODE_GROUP,
                "--name", scale["name"], "--query", base.SCALE_VIEW_QUERY,
            )
            aggregate = scale_view.get("virtualMachines")
            owned_new = name == POOL and self._owns_unqualified_creation(phase, pool_operation)
            initializing = (
                owned_new and pool_operation["status"] != "Succeeded"
                and pool["provisioningState"] in CREATING_STATES | {"Succeeded"}
                and scale["provisioningState"] in CREATING_STATES | {"Succeeded"}
                and (pool["provisioningState"] in CREATING_STATES
                     or scale["provisioningState"] in CREATING_STATES)
            )
            if aggregate is None or aggregate == []:
                require(initializing, "VMSS virtualMachine.statusesSummary is missing outside owned initialization")
                aggregate = []
            else:
                require(isinstance(aggregate, list)
                        and all(isinstance(row, dict) and base.integer(row.get("count")) and row["count"] >= 0
                                for row in aggregate)
                        and sum(row["count"] for row in aggregate) <= 2,
                        "VMSS aggregate counts are malformed or exceed the approved capacity")
            require(not any("failed" in str(row.get("code")).lower() for row in aggregate),
                    "A VMSS aggregate reports an explicit failed operation")
            require(not any("failed" in str(row.get("code")).lower() for row in scale_view.get("statuses") or []),
                    "A VMSS instance view reports an explicit failed operation")
            aggregate_ok = (
                len(aggregate) == 1 and aggregate[0].get("code") == "ProvisioningState/succeeded"
                and aggregate[0].get("count") == expected_count
                and all(row.get("code") == "ProvisioningState/succeeded"
                        for row in scale_view.get("statuses") or [])
            )
            require(aggregate_ok or transitional, f"{name}: VMSS aggregate is not Succeeded")
            complete = complete and aggregate_ok and count == expected_count and capacity == expected_count
            complete = complete and pool["provisioningState"] == scale["provisioningState"] == "Succeeded"
            for instance in instances:
                instance_id = str(instance["instanceId"])
                node_name = instance.get("computerName")
                expected_name = f"{scale['name']}{int(instance_id):06d}"
                require(node_name == expected_name
                        and prepared.resource_equal(instance.get("id"), f"{scale_id}/virtualMachines/{instance_id}")
                        and isinstance(instance.get("vmId"), str)
                        and maintenance.UUID_RE.fullmatch(instance["vmId"]) is not None,
                        "A VM does not have an exact genuine computerName/resource ID/VM ID")
                if node_name in (SOURCE, RETAINED):
                    expected_vm_id = SOURCE_VM_ID if node_name == SOURCE else RETAINED_VM_ID
                    require(instance["vmId"] == expected_vm_id, "An original default VM was replaced")
                if name == PROM_POOL:
                    require(instance["vmId"] == self.prom["vm_id"], "Monitoring VM ID changed")
                require(node_name not in self.vm_pins or instance["vmId"] == self.vm_pins[node_name]["vmId"],
                        "A pinned VM ID changed after qualification")
                view = self.az_json(
                    "vmss", "get-instance-view", "--resource-group", base.NODE_GROUP,
                    "--name", scale["name"], "--instance-id", instance_id, "--query", base.VIEW_QUERY,
                )
                statuses = [row.get("code") for row in view.get("statuses") or []]
                extensions = view.get("extensions")
                healthy = (
                    instance.get("provisioningState") == "Succeeded" and instance.get("latestModelApplied") is True
                    and set(statuses) == {"ProvisioningState/succeeded", "PowerState/running"}
                    and isinstance(extensions, list) and bool(extensions)
                    and all(row.get("name") and isinstance(row.get("statuses"), list) and row["statuses"]
                            and all(state.get("code") == "ProvisioningState/succeeded"
                                    for state in row["statuses"]) for row in extensions)
                )
                may_converge = owned_new or phase == "retiring" and node_name == SOURCE
                require(healthy or may_converge, f"{node_name}: VM/guest extensions are not fully healthy")
                extension_codes = [
                    state.get("code") for extension in extensions or [] if isinstance(extension, dict)
                    for state in extension.get("statuses") or [] if isinstance(state, dict)
                ]
                require("failed" not in str(instance.get("provisioningState")).lower()
                        and not any("failed" in str(code).lower() for code in [*statuses, *extension_codes]),
                        "An accepted operation produced a failed VM")
                complete = complete and healthy
                all_instances[node_name] = instance
            if name not in self.vmss_pins:
                self.vmss_pins[name] = contract
        require(len({row["vmId"] for row in all_instances.values()}) == len(all_instances),
                "Distinct workers report duplicate VM IDs")
        self._observed_instances = copy.deepcopy(all_instances)
        if not self.pool_pins:
            self.pool_pins = {name: prepared.pool_configuration(pool) for name, pool in by_pool.items()}
            self.vm_pins = copy.deepcopy(all_instances)
            self.patch_version = patch
            self.summary["initial_operation"] = operation
            self.summary["cluster_operation_name"] = operation["name"]
        self.summary["last_arm_proof"] = {
            "checked_at": workers.utc_now(), "operation": operation,
            "pool_operations": pool_operations,
            "counts": {name: pool["count"] for name, pool in by_pool.items()},
            "vm_ids": {name: row["vmId"] for name, row in all_instances.items()},
        }
        self.save()
        return complete and operation["status"] == "Succeeded", by_pool, all_instances

    def _nncs(self, snapshot, phase="steady"):
        nodes = maintenance._real_node_map(snapshot["nodes"])
        registering = set(nodes) - {SOURCE, RETAINED, self.prom["node_name"]} - set(self.fresh)
        if not registering:
            return maintenance._nnc_map(snapshot["nnc"])
        operation = self.summary.get("last_arm_proof", {}).get("pool_operations", {}).get(POOL)
        require(self._owns_unqualified_creation(phase, operation) and len(registering) <= 2,
                "Only causally bound unqualified cniv5 registration may initialize NNC status")
        protected_uids = {pin["node_uid"] for pin in self.node_pins.values()} | set(self.plan["kwok_node_uids"].values())
        require(len({uid(node) for node in nodes.values()}) == len(nodes), "Registered real Node UIDs are duplicated")
        for name in registering:
            node = nodes[name]
            maintenance._validate_real_node_scope(node, subscription=base.SUBSCRIPTION,
                                                   node_resource_group=base.NODE_GROUP)
            instance = self._observed_instances.get(name)
            identity = workers.provider_identity(node)
            require(mocks._node_pool_name(node) == POOL
                    and maintenance.UUID_RE.fullmatch(uid(node)) is not None
                    and uid(node) not in protected_uids and uid(node) not in self.plan["real_node_uids"].values()
                    and not node["metadata"].get("deletionTimestamp")
                    and instance is not None and instance["computerName"] == name
                    and instance["vmId"] != base.FAILED_PROM_VM_ID
                    and identity is not None and identity[0] == self.vmss_pins[POOL]["name"].lower()
                    and identity[1] in {"0", "1"}
                    and prepared.resource_equal(node["spec"].get("providerID"), "azure://" + instance["id"]),
                    "An initializing NNC does not belong to an authoritatively enumerated cniv5 Node/VM")
        rows = mocks._items(snapshot["nnc"], "NodeNetworkConfig inventory")
        require(len({row.get("metadata", {}).get("name") for row in rows}) == len(rows),
                "NodeNetworkConfig names are duplicated")
        populated = []
        for row in rows:
            metadata = row.get("metadata") or {}
            name = metadata.get("name")
            if name in registering:
                owner = base.controller_owner(row, "Node")
                require(metadata.get("namespace") == "kube-system"
                        and maintenance.UUID_RE.fullmatch(uid(row)) is not None
                        and not metadata.get("deletionTimestamp")
                        and owner["name"] == name and owner["uid"] == uid(nodes[name]),
                        "Initializing NNC resource UID, namespace, or Node owner is not exact")
                status = row.get("status")
                require(status is None or isinstance(status, dict), "Initializing NNC status is malformed")
                containers = (status or {}).get("networkContainers")
                if containers is None or containers == []:
                    continue
            populated.append(row)
        return maintenance._nnc_map({"items": populated})

    def desired_pool(self):
        default = self.pool_pins["default"]
        return {
            "name": POOL, "count": 2, "vmSize": SKU, "mode": "System", "osType": "Linux",
            "osSku": "Ubuntu", "osDiskType": "Managed", "osDiskSizeGb": 256, "kubeletDiskType": "OS",
            "maxPods": 110, "enableAutoScaling": False, "enableFips": False,
            "enableEncryptionAtHost": False, "enableNodePublicIp": False, "nodeLabels": {}, "nodeTaints": [],
            "vnetSubnetId": default["vnetSubnetId"], "podSubnetId": default["podSubnetId"],
            "orchestratorVersion": self.patch_version,
        }

    def validate_new_pool(self, pool):
        for key, value in self.desired.items():
            if key == "count":
                continue
            actual = pool.get(key)
            if key in ("nodeLabels", "nodeTaints"):
                actual = actual or type(value)()
            if key in ("vnetSubnetId", "podSubnetId"):
                require(prepared.resource_equal(actual, value), f"cniv5 {key} changed")
            else:
                require(actual == value, f"cniv5 {key} differs from the explicit supported-family contract")
        require(isinstance(pool.get("nodeImageVersion"), str) and pool["nodeImageVersion"],
                "The actual modern node image must be recorded, not invented")

    def quota(self):
        skus = self.az_json("vm", "list-skus", "--location", base.REGION, "--size", SKU,
                            "--resource-type", "virtualMachines", "--all")
        matches = [row for row in skus if row.get("name") == SKU] if isinstance(skus, list) else []
        require(len(matches) == 1, "The supported SKU must resolve exactly once")
        sku = matches[0]
        capabilities = {row["name"]: row["value"] for row in sku.get("capabilities") or []}
        require(sku.get("family", "").lower() == FAMILY.lower()
                and sku.get("resourceType") == "virtualMachines"
                and base.REGION in [str(row).lower() for row in sku.get("locations") or []]
                and sku.get("restrictions") == [] and capabilities.get("PremiumIO") == "True"
                and _number(capabilities.get("vCPUs"), "SKU vCPUs") == 8
                and _number(capabilities.get("MemoryGB"), "SKU memory") == 32,
                "Standard_D8s_v5 is restricted or does not provide the approved 8 CPU/32 GiB managed-disk SKU")
        usages = self.az_json("vm", "list-usage", "--location", base.REGION)
        require(isinstance(usages, list), "Quota inventory is malformed")
        result = {}
        for name in (FAMILY, "cores"):
            rows = [row for row in usages if str((row.get("name") or {}).get("value", "")).lower() == name.lower()]
            require(len(rows) == 1, f"Current {name} quota is missing or duplicated")
            used, limit = (_number(rows[0].get(key), f"{name} {key}") for key in ("currentValue", "limit"))
            require(limit - used >= 16, f"{name}: fewer than 16 current vCPUs available; no quota increase/retry")
            result[name] = {"used": used, "limit": limit, "required": 16}
        self.summary["quota_proof"] = {"observed_at": workers.utc_now(), "sku": SKU, "quotas": result}
        self.save()

    def _framework_guard(self, snapshot):
        pods = mocks._items(snapshot["pods"], "framework Pods")
        by_uid = {uid(pod): pod for pod in pods}
        require(len(by_uid) == len(pods), "Pod UID inventory is duplicated")
        if not self.framework:
            for move in self.checkpoint["pod_moves"]:
                pod = by_uid.get(move["ready_pod_uid"])
                require(pod is not None and pod["metadata"].get("namespace") == move["namespace"]
                        and pod["spec"].get("nodeName") == move["ready_node"]
                        and base.controller_owner(pod, "ReplicaSet")["uid"] == move["replica_set_uid"],
                        "Published monitoring Pod UID is not present on its genuine modern worker")
                self.framework[uid(pod)] = _pod_pin(pod)
        for expected_uid, pin in self.framework.items():
            pod = by_uid.get(expected_uid)
            require(pod is not None and _pod_pin(pod) == pin and base.pod_ready(pod),
                    "A protected monitoring/framework Pod changed or lost Ready/IP")
        required_groups = {
            ("kube-system", "coredns"), ("kube-system", "clustermesh-apiserver"),
            ("monitoring", "grafana"), ("kube-state-metrics-perf-test", "kube-state-metrics"),
        }
        for namespace, name in required_groups:
            deployment = base.controller(snapshot, "Deployment", namespace, name)
            replicas = deployment["spec"].get("replicas")
            require(base.integer(replicas) and replicas > 0, "Required framework replicas are invalid")
            if name == "coredns":
                require(replicas == 5, "The original full five-replica DNS service must remain Ready")
            sets = [
                row for row in snapshot["controllers"]["items"] if row["kind"] == "ReplicaSet"
                and row["metadata"].get("namespace") == namespace
                and base.controller_owner(row, "Deployment")["uid"] == uid(deployment)
            ]
            owned = [pod for pod in pods if pod["metadata"].get("namespace") == namespace
                     and any(owner.get("controller") is True and owner.get("kind") == "ReplicaSet"
                             and owner.get("uid") in {uid(row) for row in sets}
                             for owner in pod["metadata"].get("ownerReferences") or [])]
            draining = self.drain_inflight
            if (draining and draining["namespace"] == namespace
                    and draining["owner"]["uid"] in {uid(row) for row in sets}):
                survivors = [pod for pod in owned if uid(pod) != draining["uid"]]
                require(replicas - 1 <= len(survivors) <= replicas
                        and sum(base.pod_ready(pod) for pod in survivors) >= replicas - 1,
                        "Only the UID/PDB-authorized in-flight framework may have a readiness gap")
            else:
                require(len(owned) == replicas and all(base.pod_ready(pod) for pod in owned),
                        f"{namespace}/{name}: full required framework readiness regressed")

    def guard(self, snapshot, phase="steady"):
        require(base.frozen_controllers(snapshot) == self.checkpoint["controller_pins"],
                "Original controller UIDs/specs changed")
        require(base.frozen_pdbs(snapshot) == self.checkpoint["pdb_pins"], "Original PDB UIDs/specs changed")
        maintenance._require_all_kwok_ready(snapshot["nodes"], self.plan["kwok_node_uids"])
        nodes = maintenance._real_node_map(snapshot["nodes"])
        require(not any(pod.get("spec", {}).get("nodeName") in self.plan["kwok_node_uids"]
                        for pod in snapshot["pods"]["items"]),
                "An overlapping workload occupies the preserved KWOK nodes")
        expected = {SOURCE, RETAINED, self.prom["node_name"]} | set(self.fresh)
        if self.retired or phase == "retiring" and SOURCE not in nodes:
            expected.discard(SOURCE)
        extras = set(nodes) - expected
        require(not extras or phase == "creating" and all(mocks._node_pool_name(nodes[name]) == POOL for name in extras),
                "An unexpected real Node appeared")
        require(expected <= set(nodes) and len(snapshot["nodes"]["items"]) == 100 + len(nodes),
                "A pinned real Node or the exact 100 KWOK inventory changed")
        nncs = self._nncs(snapshot, phase)
        raw_nncs = {row["metadata"]["name"]: row for row in snapshot["nnc"]["items"]}
        require(set(nncs) <= set(nodes) | ({SOURCE} if phase == "retiring" else set()),
                "A foreign or stale NodeNetworkConfig appeared")
        for name, node in nodes.items():
            maintenance._validate_real_node_scope(node, subscription=base.SUBSCRIPTION,
                                                   node_resource_group=base.NODE_GROUP)
            if name in extras:
                continue
            retiring = phase == "retiring" and name == SOURCE
            require(not node["metadata"].get("deletionTimestamp") or retiring, "A pinned Node entered deletion")
            require(workers.node_is_ready(node) or retiring, f"{name}: a protected worker is not Ready")
            require(name in nncs or retiring, f"{name}: pinned NodeNetworkConfig disappeared")
            if name not in nncs:
                continue
            nnc = nncs[name]
            require(nnc["node_uid"] == uid(node) and nnc["uid"], "NodeNetworkConfig owner/UID is not exact")
            raw_nnc = raw_nncs[name]
            nc_config = {
                "spec": {key: value for key, value in raw_nnc.get("spec", {}).items() if key != "requestedIPCount"},
                "labels": raw_nnc["metadata"].get("labels") or {},
                "annotations": raw_nnc["metadata"].get("annotations") or {},
            }
            require(name not in self.nnc_config_pins or self.nnc_config_pins[name] == nc_config,
                    "A pinned NNC configuration changed")
            self.nnc_config_pins[name] = copy.deepcopy(nc_config)
            wanted_pool = POOL if name in self.fresh else PROM_POOL if name == self.prom["node_name"] else "default"
            require(mocks._node_pool_name(node) == wanted_pool, "A worker's genuine pool label changed")
            image = self.pool_pins.get(wanted_pool, {}).get("nodeImageVersion")
            require(image and node["metadata"].get("labels", {}).get("kubernetes.azure.com/node-image-version") == image,
                    "A real worker image does not match its own genuine pool's pinned image")
            if name in (SOURCE, RETAINED):
                require(uid(node) == base.REAL_UIDS[name] and workers.provider_identity(node)
                        == (base.DEFAULT_VMSS, "0" if name == SOURCE else "1"),
                        "Original default Node UID/provider changed")
            if name == SOURCE:
                require(nnc["network_container_id"] == base.SOURCE_NC, "Source network container changed")
            if name == self.prom["node_name"]:
                require(uid(node) == self.prom["node_uid"]
                        and prepared.resource_equal(node["spec"].get("providerID"), self.prom["provider_id"])
                        and nnc["network_container_id"] == self.prom["network_container_id"],
                        "Modern monitoring identity/NC changed")
            normalized = copy.deepcopy(node)
            if name == SOURCE and self.hold:
                require(node["spec"].get("unschedulable") is True
                        and maintenance._annotations(node).get(maintenance.HOLD_ANNOTATION) == self.token,
                        "Owned source hold disappeared or changed")
                hold = {"key": maintenance.HOLD_ANNOTATION, "value": self.token, "effect": "NoSchedule"}
                require(maintenance._taints(node).count(hold) == 1, "Owned source hold taint changed")
                normalized["spec"]["taints"].remove(hold)
                cordon = [
                    taint for taint in normalized["spec"]["taints"]
                    if isinstance(taint, dict)
                    and {key: value for key, value in taint.items()
                         if key != "timeAdded" and not (key == "value" and value == "")}
                    == {"key": "node.kubernetes.io/unschedulable", "effect": "NoSchedule"}
                ]
                require(len(cordon) <= 1, "Owned source has duplicate standard cordon taints")
                for taint in cordon:
                    normalized["spec"]["taints"].remove(taint)
                normalized["spec"]["unschedulable"] = False
                normalized["metadata"]["annotations"].pop(maintenance.HOLD_ANNOTATION)
            if name in self.exclusions:
                exclusion = {"key": EXCLUSION_KEY, "value": self.token, "effect": "NoSchedule"}
                require(maintenance._taints(node).count(exclusion) == 1, "Owned placement exclusion changed")
                normalized["spec"]["taints"].remove(exclusion)
            pin = _node_pin(normalized, nnc)
            if name in self.node_pins:
                require(pin == self.node_pins[name], f"{name}: Node UID/boot/provider/metadata/NNC pin changed")
            else:
                require(not normalized["spec"].get("unschedulable") and not maintenance._taints(normalized)
                        and not any(key.startswith("mock-clustermesh/") for key in maintenance._annotations(normalized)),
                        "Foreign holds/taints or scheduling changes prohibit adoption")
                self.node_pins[name] = pin
        local = next(row for row in self.identities if row["role"] == base.ROLE)
        data = snapshot["cilium_config"].get("data") or {}
        require(data.get("cluster-name") == local["cluster_name"] and data.get("cluster-id") == str(local["cluster_id"]),
                "Local Cilium identity drifted from the preserved Fleet map")
        self._framework_guard(snapshot)
        protected = dict(self.protected)
        if self.inflight:
            protected.pop(self.inflight["name"], None)
        agents = maintenance._observe_agents_with_gap(
            snapshot["pods"], self.plan["mock_controller_uid"], protected,
            inflight_name=self.inflight["name"] if self.inflight else None,
            inflight_uid=self.inflight["uid"] if self.inflight else None,
        )
        require(len({uid(pod) for pod in agents.values()}) == len(agents), "Mock Pod UIDs are duplicated")
        ready_ips = [pod["status"]["podIP"] for pod in agents.values() if base.pod_ready(pod)]
        require(len(set(ready_ips)) == len(ready_ips), "Ready mock Pods report duplicate IP addresses")
        for name, pod in agents.items():
            if self.inflight and name == self.inflight["name"]:
                continue
            require(uid(pod) == self.effective[name], "An unpinned mock Pod UID changed")
            require(name not in self.agent_nodes or pod["spec"].get("nodeName") == self.agent_nodes[name],
                    "A protected mock moved outside its explicit UID-bound exchange")
            require(base.pvc_free(pod["spec"]) and not pod["spec"].get("hostNetwork"),
                    "A mock Pod acquired a PVC/ephemeral claim or host network")
            require(mocks._resource_requests(pod) == (100, 256 * 1024**2)
                    and len(pod["spec"].get("containers") or []) == 1
                    and int(mocks._quantity(pod["spec"]["containers"][0].get("resources", {}).get(
                        "limits", {}).get("memory"), "actual mock memory limit")) == 1024**3,
                    "An actual mock Pod differs from the pinned CPU/memory request/limit contract")
            if not base.pod_ready(pod):
                require(not self.healthy_phase and name in self.pending,
                        "Every other mock must stay Ready; only the original 29 Pending are authorized")
                _source_evidence(snapshot, pod)
            else:
                self.protected[name] = uid(pod)
        if self.healthy_phase:
            require(sum(base.pod_ready(pod) for pod in agents.values()) >= (99 if self.inflight else 100),
                    "The 99-Ready exchange/100-Ready inter-move barrier failed")
        if self.initial is None:
            require(len(agents) == 100, "Initial mock inventory must contain all 100 original UIDs")
            source_agents = {name for name, pod in agents.items() if pod["spec"].get("nodeName") == SOURCE}
            retained_agents = {name for name, pod in agents.items() if pod["spec"].get("nodeName") == RETAINED}
            require(len(source_agents) == 44 and len(retained_agents) == 56
                    and source_agents | retained_agents == set(agents) and set(self.pending) <= source_agents
                    and len(source_agents & set(self.plan["ready_mock_pod_uids"])) == 15,
                    "Only the original 44-source/56-retained mock placement is approved")
            self.original_source = source_agents
            self.agent_nodes = {name: pod["spec"]["nodeName"] for name, pod in agents.items()}
            self.summary["protected_other_worker_uids"] = {name: uid(agents[name]) for name in sorted(retained_agents)}
            self.summary["original_healthy_source_uids"] = {
                name: uid(agents[name]) for name in sorted(source_agents & set(self.plan["ready_mock_pod_uids"]))
            }
            controller = base.controller(snapshot, "StatefulSet", mocks.DEFAULT_NAMESPACE, "kwok-node",
                                         self.plan["mock_controller_uid"])
            self.template = copy.deepcopy(controller["spec"]["template"]["spec"])
            terms = (((self.template.get("affinity") or {}).get("nodeAffinity") or {}).get(
                "requiredDuringSchedulingIgnoredDuringExecution") or {}).get("nodeSelectorTerms")
            expressions = terms[0].get("matchExpressions") if isinstance(terms, list) and len(terms) == 1 else None
            require(isinstance(expressions, list) and len(expressions) == 2
                and not terms[0].get("matchFields")
                and all(isinstance(row, dict) and not row.get("values") for row in expressions)
                and {(row.get("key"), row.get("operator")) for row in expressions} == {
                    ("kubernetes.azure.com/cluster", "Exists"), ("prometheus", "DoesNotExist"),
                } and not self.template.get("nodeName")
                and not any("agentpool" in key for key in self.template.get("nodeSelector", {})),
                "The real mock template is not the approved cluster-Exists/prometheus-DoesNotExist affinity")
            require(controller["spec"].get("replicas") == 100 and base.pvc_free(self.template)
                    and mocks._resource_requests({"spec": self.template}) == (100, 256 * 1024**2)
                    and len(self.template.get("containers") or []) == 1
                    and int(mocks._quantity(self.template["containers"][0].get("resources", {}).get(
                        "limits", {}).get("memory"), "mock memory limit")) == 1024**3,
                    "Original mock replicas/resources/PVC contract changed")
            self.daemonsets = maintenance._derive_applicable_daemonsets(snapshot["pods"], [RETAINED])
            require({name for _, name, _ in self.daemonsets} >= {"cilium", "azure-cns"},
                    "Pinned reference worker lacks applicable Cilium/CNS DaemonSets")
            self.initial = copy.deepcopy(snapshot)
        if not self.healthy_phase:
            require(sum(base.pod_ready(pod) and pod["spec"].get("nodeName") == SOURCE
                        for pod in agents.values()) <= maintenance.MAX_HEALTHY_SOURCE_AGENTS,
                    "Healthy SOURCE agents exceed the safety cap 25; do not split the original 29-Pending plan")
        if self.fresh and phase not in ("creating",):
            for name in self.fresh:
                require(self.daemonsets <= maintenance._healthy_system_daemonsets_on_node(snapshot["pods"], name),
                        "An applicable pinned destination DaemonSet lost Ready")
        for name in (RETAINED, self.prom["node_name"]):
            require(self.daemonsets <= maintenance._healthy_system_daemonsets_on_node(snapshot["pods"], name),
                    "A protected real-worker system DaemonSet lost Ready")
        return nodes, agents

    def peers(self, snapshot):
        statuses = maintenance.cilium.overlay.read_status(
            maintenance.cilium.overlay.Cluster(base.CLUSTER, base.RESOURCE_GROUP, base.ROLE, self.args.kubeconfig),
            self.run, self.args.request_timeout_seconds,
        )
        expected = {row["cluster_name"]: row["cluster_id"] for row in self.identities if row["role"] != base.ROLE}
        proof = maintenance.cilium.inspect_agents(statuses, 99, set(expected))
        nodes = set(maintenance._real_node_map(snapshot["nodes"]))
        pods = [row for row in snapshot["pods"]["items"]
                if row["metadata"].get("namespace") == "kube-system"
                and row["metadata"].get("labels", {}).get("k8s-app") == "cilium"]
        cilium_uid = uid(base.controller(snapshot, "DaemonSet", "kube-system", "cilium"))
        require(proof["healthy"] and proof["cilium_agent_count"] == len(nodes)
                and len(statuses) == len(nodes) and {row.node_name for row in statuses} == nodes
                and len(pods) == len(nodes)
                and {row.pod_name for row in statuses} == {row["metadata"]["name"] for row in pods}
                and all(base.controller_owner(row, "DaemonSet")["uid"] == cilium_uid for row in pods),
                "Strict all-real-Cilium-agent/99-peer coverage failed")
        for status in statuses:
            for remote in status.remotes:
                remote_id = (remote.get("config") or {}).get("cluster-id")
                require(base.integer(remote_id) and remote_id == expected[remote["name"]],
                        "A Cilium peer cluster ID differs from its authoritative Fleet identity")
        self.summary["cilium_proof"] = proof
        self.save()

    def check(self, phase="steady", *, authority=True):
        if authority:
            self.authority()
        complete, pools, instances = self.models(phase)
        snapshot = self.snapshot()
        self.guard(snapshot, phase)
        return complete, pools, instances, snapshot

    def preflight(self):
        self.authority()
        complete, _, _ = self.models()
        require(complete, "Initial AKS/provider/pools are not quiescent")
        snapshot = self.snapshot()
        nodes, _ = self.guard(snapshot)
        self.source_pods(snapshot, framework_only=True)
        require(nodes[RETAINED]["status"].get("nodeInfo", {}).get("kubeletVersion") == f"v{self.patch_version}"
                and nodes[SOURCE]["status"].get("nodeInfo", {}).get("kubeletVersion") == f"v{self.patch_version}",
                "Current healthy default kubelet patch must match AKS exactly")
        journals = self.kube("-n", "kube-system", "get", "configmaps", "-o", "json")
        require(not any(row["metadata"].get("name") == self.journal_name
                        or JOURNAL_KEY in (row["metadata"].get("labels") or {})
                        for row in mocks._items(journals, "operation journals")),
                "Existing cniv5 journal blocks duplicate execution; receipt adoption is not implemented")
        require(not any(maintenance.PROBE_LABEL_KEY in row["metadata"].get("labels", {})
                        or JOURNAL_KEY in row["metadata"].get("labels", {})
                        for row in snapshot["pods"]["items"]), "An earlier owned probe/workload remains")
        self.desired = self.desired_pool()
        self.quota()
        self.peers(snapshot)
        self.summary.update(
            plan_valid=True, status="planned", desired_pool=self.desired,
            original_identity={"mock_pod_uids": self.plan["mock_pod_uids"],
                               "kwok_node_uids": self.plan["kwok_node_uids"],
                               "real_node_uids": self.plan["real_node_uids"]},
            initial_real_workers=copy.deepcopy(self.node_pins),
            original_default_configuration=self.pool_pins["default"],
            original_pending_plan=[{"name": name, "uid": self.plan["mock_pod_uids"][name]} for name in self.pending],
            pending_plan_count=29, healthy_source_cap=25, temporary_default_role_cap=4,
            final_default_role_count=3, modern_prom=copy.deepcopy(self.prom),
            controller_pins=copy.deepcopy(self.checkpoint["controller_pins"]),
            pdb_pins=copy.deepcopy(self.checkpoint["pdb_pins"]),
            monitoring_pod_moves=copy.deepcopy(self.checkpoint["pod_moves"]),
        )
        self.save()

    def acquire(self):
        self.summary["journal"] = {
            "name": self.journal_name, "namespace": "kube-system", "token": self.token,
            "create_attempted": True, "create_accepted": None, "ambiguous": True,
        }
        self.summary["mutation_started"] = True
        self.save()
        data = self._journal_data()
        output = self.raw_write([
            "kubectl", "-n", "kube-system", "create", "configmap", self.journal_name,
            *[f"--from-literal={key}={value}" for key, value in data.items()], "-o", "json",
        ])
        created = workers.parse_json(output, "exclusive operation journal")
        require(uid(created) and created.get("data") == data, "Journal creation was not authoritatively confirmed")
        self.journal_uid = uid(created)
        self.summary["journal"].update(uid=self.journal_uid, create_accepted=True, ambiguous=False)
        self.save()

    def patch_node(self, name, key, changes):
        node = self.kube("get", "node", name, "-o", "json")
        require(uid(node) == self.node_pins[name]["node_uid"], "Node changed immediately before an owned patch")
        operations = [
            {"op": "test", "path": "/metadata/uid", "value": uid(node)},
            {"op": "test", "path": "/metadata/resourceVersion", "value": node["metadata"]["resourceVersion"]},
            *changes(node),
        ]
        self.action(key, {"node_name": name, "node_uid": uid(node)}, lambda: self.raw_write([
            "kubectl", "patch", "node", name, "--type=json", "-p", json.dumps(operations),
        ]))

    def hold_source(self):
        self.check()
        self.summary["source_hold"] = {"node_name": SOURCE, "node_uid": base.REAL_UIDS[SOURCE], "token": self.token}
        self.patch_node(SOURCE, "source-hold", lambda node: [
            {"op": "add", "path": "/spec/unschedulable", "value": True},
            {"op": "add", "path": "/spec/taints", "value": maintenance._taints(node) + [
                {"key": maintenance.HOLD_ANNOTATION, "value": self.token, "effect": "NoSchedule"}]},
            {"op": "add", "path": "/metadata/annotations",
             "value": {**maintenance._annotations(node), maintenance.HOLD_ANNOTATION: self.token}},
        ])
        self.hold = True

    def wait(self, deadline, description):
        require(time.monotonic() < min(deadline, self.work_deadline), f"{description} did not converge within its bound")
        time.sleep(min(self.args.poll_seconds, max(0.01, deadline - time.monotonic())))

    def create_pool(self):
        self.check()
        self.quota()
        self.guard(self.snapshot())
        self.summary["status"] = "creating-modern-system-pool"
        self.action("pool-create", {"desired": copy.deepcopy(self.desired)}, lambda: self.raw_write([
            "az", "aks", "nodepool", "add", "--resource-group", base.RESOURCE_GROUP,
            "--cluster-name", base.CLUSTER, "--name", POOL, "--mode", "System",
            "--node-count", "2", "--node-vm-size", SKU, "--os-type", "Linux", "--os-sku", "Ubuntu",
            "--node-osdisk-type", "Managed", "--node-osdisk-size", "256", "--kubelet-disk-type", "OS",
            "--max-pods", "110", "--vnet-subnet-id", self.desired["vnetSubnetId"],
            "--pod-subnet-id", self.desired["podSubnetId"], "--kubernetes-version", self.patch_version,
            "--no-wait", "--only-show-errors",
        ]))
        deadline = min(self.work_deadline - RETIREMENT_RESERVE, time.monotonic() + 1200)
        while True:
            complete, pools, instances, snapshot = self.check("creating")
            nodes = maintenance._real_node_map(snapshot["nodes"])
            fresh = {name: node for name, node in nodes.items() if mocks._node_pool_name(node) == POOL}
            require(len(fresh) <= 2, "Modern pool exceeded the temporary four default-role worker cap")
            nncs = self._nncs(snapshot, "creating")
            if complete and len(fresh) == 2 and all(
                mocks._node_ready_and_schedulable(node) and name in nncs
                and self.daemonsets <= maintenance._healthy_system_daemonsets_on_node(snapshot["pods"], name)
                for name, node in fresh.items()
            ):
                for name, node in fresh.items():
                    identity = workers.provider_identity(node)
                    require(name in instances and identity is not None and identity[0] != base.DEFAULT_VMSS
                            and identity[0] != self.prom["vmss_name"]
                            and identity[0] == self.vmss_pins[POOL]["name"].lower()
                            and prepared.resource_equal(node["spec"].get("providerID"),
                                                        "azure://" + instances[name]["id"])
                            and name == instances[name]["computerName"]
                            and uid(node) not in {pin["node_uid"] for pin in self.node_pins.values()}
                            and uid(node) not in self.plan["real_node_uids"].values()
                            and instances[name]["vmId"] != base.FAILED_PROM_VM_ID
                            and nncs[name]["network_container_id"] not in {
                                pin["network_container_id"] for pin in self.node_pins.values()}
                            and node["metadata"]["labels"].get("kubernetes.azure.com/node-image-version")
                            == pools[POOL]["nodeImageVersion"]
                            and node["status"].get("nodeInfo", {}).get("kubeletVersion") == f"v{self.patch_version}"
                            and mocks._node_matches_pod_template(node, self.template),
                            "New cniv5 worker does not have a genuine new VM/Node/NC/image/eligible-template identity")
                    self.fresh[name] = {
                        "node_uid": uid(node), "vm_id": instances[name]["vmId"], "provider_id": node["spec"]["providerID"],
                        "network_container_id": nncs[name]["network_container_id"], "nnc_uid": nncs[name]["uid"],
                        "node_image_version": pools[POOL]["nodeImageVersion"],
                    }
                require(len({row["node_uid"] for row in self.fresh.values()}) == 2
                        and len({row["network_container_id"] for row in self.fresh.values()}) == 2,
                        "The two modern worker identities/NCs are not distinct")
                self.pool_pins[POOL] = prepared.pool_configuration(pools[POOL])
                self.vm_pins.update(copy.deepcopy(instances))
                self.guard(snapshot)
                self.summary["modern_cni_workers"] = copy.deepcopy(self.fresh)
                self.summary["actual_modern_pool_configuration"] = self.pool_pins[POOL]
                self.summary["last_pool_operations"][POOL] = self.summary["actions"]["pool-create"]["operation_name"]
                self.save()
                return
            self.wait(deadline, "Two genuine modern workers and all applicable pinned DaemonSets")

    def placement(self, *, remove=False):
        for name in ([*self.exclusions] if remove else [RETAINED, self.prom["node_name"]]):
            self.check()
            if remove:
                def changes(node):
                    taint = {"key": EXCLUSION_KEY, "value": self.token, "effect": "NoSchedule"}
                    require(maintenance._taints(node).count(taint) == 1, "Cannot remove a foreign/missing exclusion")
                    index = maintenance._taints(node).index(taint)
                    return [{"op": "test", "path": f"/spec/taints/{index}", "value": taint},
                            {"op": "remove", "path": f"/spec/taints/{index}"}]
                self.patch_node(name, f"exclusion-remove/{name}", changes)
                self.exclusions.pop(name)
            else:
                require(not any(mocks._tolerates(
                    {"key": EXCLUSION_KEY, "value": self.token, "effect": "NoSchedule"}, [row],
                ) for row in self.template.get("tolerations") or []),
                        "Mock template tolerates the temporary owned placement exclusion")
                self.patch_node(name, f"exclusion-add/{name}", lambda node: [
                    {"op": "add", "path": "/spec/taints", "value": maintenance._taints(node) + [
                        {"key": EXCLUSION_KEY, "value": self.token, "effect": "NoSchedule"}]},
                ])
                self.exclusions[name] = self.node_pins[name]["node_uid"]
            self.summary["temporary_exclusions"] = [
                {"node_name": node, "node_uid": node_uid, "token": self.token}
                for node, node_uid in self.exclusions.items()
            ]
            self.save()
            self.persist_journal()

    def capacity(self, snapshot, remaining):
        nodes, agents = self.guard(snapshot)
        actual_destinations = {
            name for name, node in nodes.items()
            if mocks._eligible_capacity_node(node, [], self.template)
        }
        require(actual_destinations == set(self.fresh), "Only IP-qualified cniv5 nodes may be eligible destinations")
        node_metrics = self.kube("get", "--raw", "/apis/metrics.k8s.io/v1beta1/nodes")
        pod_metrics = self.kube("get", "--raw", f"/apis/metrics.k8s.io/v1beta1/namespaces/{mocks.DEFAULT_NAMESPACE}/pods")
        metrics = {row["metadata"]["name"]: _metric(row) for row in mocks._items(node_metrics, "node metrics")}
        normalized = {"items": [_metric(row) for row in mocks._items(pod_metrics, "pod metrics")]}
        samples = maintenance._pod_memory_usage_bytes(normalized, mocks.DEFAULT_NAMESPACE)
        healthy_names = {name for name, pod in agents.items() if base.pod_ready(pod)}
        require(healthy_names <= set(samples), "Actual fresh metrics for every healthy mock agent are required")
        high = max(samples[name]["memory_bytes"] for name in healthy_names)
        self.memory_high_water = max(self.memory_high_water, high, 256 * 1024**2)
        affected = []
        estimates = {}
        for name in remaining:
            pod = copy.deepcopy(agents[name])
            amount = max(self.memory_high_water, samples.get(name, {}).get("memory_bytes", 0))
            estimates[name] = amount
            pod["spec"]["containers"][0].setdefault("resources", {}).setdefault("requests", {})["memory"] = str(amount)
            affected.append(pod)
        proof = mocks.assess_recovery_capacity(
            nodes_payload=snapshot["nodes"], pods_payload=snapshot["pods"], affected=affected,
            saturated_nodes=sorted(set(nodes) - set(self.fresh)), pod_template=self.template,
            config_settings=mocks.CapacityRepairConfig(),
        )
        require(proof["sufficient"] is True and set(proof["alternate_nodes"]) == set(self.fresh),
                "All remaining agents, not a split of 29, must fit with actual-memory and scheduling reserves")
        projected = {name: 0 for name in self.fresh}
        for name, destination in proof["planned_placements"].items():
            projected[destination] += estimates[name]
        for name in self.fresh:
            require(name in metrics, "Destination CPU/memory metrics are missing")
            metric = metrics[name]
            used = int(mocks._quantity(metric["usage"].get("memory"), "actual destination memory"))
            current_cpu = int(mocks._quantity(metric["usage"].get("cpu"), "actual destination CPU") * 1000)
            require(current_cpu > 0, "Destination CPU metrics are invalid")
            memory = self.memory.setdefault(name, {"baseline": used, "reserved": 0})
            memory["baseline"] = max(memory["baseline"], used - memory["reserved"])
            extra = max(0, memory["baseline"] + memory["reserved"] - used)
            next_amount = max(estimates.values(), default=0)
            require(maintenance._headroom_ok(
                nodes[name], metric, threshold_percent=85, effective_reserved_memory_bytes=extra,
                next_memory_bytes=max(projected[name], next_amount),
            ), "Actual-memory high-water projection exceeds the fixed 85% threshold")
            placed_count = sum(destination == name for destination in proof["planned_placements"].values())
            ip_proof = self.summary.get("fresh_ip_growth", {}).get(name)
            if ip_proof and ip_proof.get("qualified"):
                current_nc = maintenance._nnc_map(snapshot["nnc"])[name]
                current_ips = set(current_nc["ip_addresses"])
                occupied_ips = {
                    pod.get("status", {}).get("podIP") for pod in snapshot["pods"]["items"]
                    if pod.get("spec", {}).get("nodeName") == name and not pod["spec"].get("hostNetwork")
                }
                require(current_nc["version"] >= ip_proof["after"]["version"]
                        and set(ip_proof["after"]["ip_addresses"]) <= current_ips
                        and len(current_ips - occupied_ips) >= max(placed_count, 1 if remaining else 0),
                        "The proved fresh IP allocation regressed or cannot fit all remaining projected agents")
            alloc_cpu = proof["nodes"][name]["allocatable_cpu_millicores"]
            require(current_cpu + max(100 if remaining else 0, placed_count * 100) + 250 < alloc_cpu * 0.85,
                    "Actual CPU headroom is unsafe on an eligible destination")
        self.summary["latest_capacity_proof"] = proof
        self.summary["destination_memory"] = copy.deepcopy(self.memory)
        self.save()
        return estimates

    def prove_ip_growth(self):
        snapshot = self.snapshot()
        self.guard(snapshot)
        before = maintenance._nnc_map(snapshot["nnc"])
        token = self.token
        self.summary["fresh_ip_growth"] = {}
        self.summary["probe_cleanup_pending"] = []
        for node_name in sorted(self.fresh):
            row = before[node_name]
            original_ips = set(row["ip_addresses"])
            require(row["assigned_ip_count"] == len(original_ips) and original_ips and row["version"] > 0,
                    "Initial allocated IP inventory/version must be concrete and unique")
            occupied = {
                pod.get("status", {}).get("podIP") for pod in snapshot["pods"]["items"]
                if pod.get("spec", {}).get("nodeName") == node_name and not pod["spec"].get("hostNetwork")
                and pod.get("status", {}).get("podIP") in original_ips
            }
            count = max(2, len(original_ips - occupied) + 1)
            node = next(row for row in snapshot["nodes"]["items"] if row["metadata"]["name"] == node_name)
            residents = sum(pod.get("spec", {}).get("nodeName") == node_name for pod in snapshot["pods"]["items"])
            require(count + residents + 5 <= int(node["status"]["allocatable"]["pods"]),
                    "Insufficient real Pod/IP slots to force a new allocated block safely")
            self.summary["fresh_ip_growth"][node_name] = {
                "baseline": copy.deepcopy(row), "probe_count": count, "qualified": False,
            }
            for index in range(count):
                self.guard(self.snapshot())
                maintenance._create_probe_pod(self, self.args, node_name, token, index, self.summary)
        deadline = min(self.work_deadline - RETIREMENT_RESERVE, time.monotonic() + 600)
        while True:
            snapshot = self.snapshot()
            self.guard(snapshot)
            nncs = maintenance._nnc_map(snapshot["nnc"])
            probes = {"items": [row for row in snapshot["pods"]["items"]
                                 if row["metadata"].get("labels", {}).get(maintenance.PROBE_LABEL_KEY) == token]}
            errors = maintenance._resolve_probe_uids(self.summary, probes)
            require(not errors, "; ".join(errors))
            all_ips = []
            complete = True
            for node_name, proof in self.summary["fresh_ip_growth"].items():
                row = nncs[node_name]
                initial = proof["baseline"]
                require(row["node_uid"] == initial["node_uid"] and row["uid"] == initial["uid"]
                        and row["network_container_id"] == initial["network_container_id"],
                        "A fresh Node/NNC/NC changed during actual IP qualification")
                owned = [pod for pod in probes["items"] if pod["spec"].get("nodeName") == node_name]
                intended = {item["uid"] for item in maintenance._probe_intents(self.summary)
                            if item["node_name"] == node_name and item["uid"]}
                ready = (len(owned) == proof["probe_count"] and len(intended) == len(owned)
                         and {uid(pod) for pod in owned} == intended and all(base.pod_ready(pod) for pod in owned))
                if not ready:
                    complete = False
                    continue
                ips = [pod["status"]["podIP"] for pod in owned]
                require(len(set(ips)) == len(ips) and all(ipaddress.ip_address(ip).version == 4 for ip in ips),
                        "Probes do not have distinct real IPv4 Pod addresses")
                all_ips.extend(ips)
                grown = (
                    row["assigned_ip_count"] > initial["assigned_ip_count"] and row["version"] > initial["version"]
                    and len(set(row["ip_addresses"])) == row["assigned_ip_count"]
                    and set(initial["ip_addresses"]) < set(row["ip_addresses"])
                    and set(ips) <= set(row["ip_addresses"]) and bool(set(ips) - set(initial["ip_addresses"]))
                )
                if not grown:
                    complete = False
                    continue
                for pod in owned:
                    require(not pod["spec"].get("hostNetwork")
                            and pod["spec"]["containers"][0]["image"] == maintenance.DEFAULT_PROBE_IMAGE,
                            "A probe spec changed")
                    hostname = self.run([
                        "kubectl", "get", "--raw",
                        f"/api/v1/namespaces/{mocks.DEFAULT_NAMESPACE}/pods/{pod['metadata']['name']}:8080/proxy/hostname",
                    ]).strip()
                    require(hostname == pod["metadata"]["name"], "An actual Ready/IP HTTP probe did not answer")
                proof.update(qualified=True, after=copy.deepcopy(row), ready_probe_ips=sorted(ips),
                             ready_probe_uids=sorted(intended))
            require(len(set(all_ips)) == len(all_ips), "The two destinations reused a probe IP")
            self.save()
            if complete:
                break
            self.wait(deadline, "Both allocated IP blocks, versions, unique Ready/IP Pods, and HTTP probes")
        for intent in list(maintenance._probe_intents(self.summary)):
            self.guard(self.snapshot())
            self.action(f"probe-delete/{intent['uid']}", {"probe": copy.deepcopy(intent)}, lambda intent=intent:
                        self.delete_pod(self.cluster, namespace=mocks.DEFAULT_NAMESPACE, name=intent["name"],
                                        uid=intent["uid"], timeout_seconds=self.remaining_seconds(
                                            self.args.request_timeout_seconds), attempts=1, retry_seconds=0))
            deadline = min(self.work_deadline, time.monotonic() + self.args.per_pod_ready_seconds)
            while True:
                snapshot = self.snapshot()
                self.guard(snapshot)
                matches = [row for row in snapshot["pods"]["items"] if row["metadata"]["name"] == intent["name"]
                           and row["metadata"].get("namespace") == mocks.DEFAULT_NAMESPACE]
                require(not matches or len(matches) == 1 and uid(matches[0]) == intent["uid"],
                        "Owned probe name was replaced during cleanup")
                if not matches:
                    break
                self.wait(deadline, "Exact owned probe cleanup")
            self.summary["probe_cleanup_pending"].remove(intent)
            self.save()
            self.persist_journal()

    def move(self, name, phase):
        require(set(self.summary.get("fresh_ip_growth") or {}) == set(self.fresh)
                and all(row.get("qualified") is True for row in self.summary["fresh_ip_growth"].values())
                and not self.summary.get("probe_cleanup_pending"),
                "Both real IP-growth proofs and exact probe cleanup are required before any mock delete")
        require(self.remaining_seconds(7200) > self.args.per_pod_ready_seconds + RETIREMENT_RESERVE,
                "Insufficient reserved retirement/finalization time for another Pod exchange")
        _, _, _, snapshot = self.check()
        _, agents = self.guard(snapshot)
        pod = agents[name]
        require(name in self.original_source and uid(pod) == self.plan["mock_pod_uids"][name]
                and pod["spec"].get("nodeName") == SOURCE, "Only original UID-pinned SOURCE mocks may be deleted")
        if phase == "pending" and base.pod_ready(pod):
            self.summary.setdefault("naturally_ready_pending_uids", {})[name] = uid(pod)
            self.save()
            return
        require(phase != "healthy" or self.healthy_phase and base.pod_ready(pod), "Healthy-source barrier is not armed")
        if phase == "pending":
            _source_evidence(snapshot, pod)
        _pdb_allows(snapshot, pod)
        remaining = sorted(agent_name for agent_name, current in agents.items()
                           if current["spec"].get("nodeName") == SOURCE)
        estimates = self.capacity(snapshot, remaining)
        # Metrics/ARM reads must not turn a formerly Pending UID into an unchecked delete.
        snapshot = self.snapshot()
        _, agents = self.guard(snapshot)
        pod = agents[name]
        if phase == "pending" and base.pod_ready(pod):
            self.summary.setdefault("naturally_ready_pending_uids", {})[name] = uid(pod)
            self.save()
            return
        if phase == "pending":
            _source_evidence(snapshot, pod)
        _pdb_allows(snapshot, pod)
        record = {"name": name, "uid": uid(pod), "source": SOURCE, "phase": phase,
                  "estimated_memory_bytes": estimates[name], "ready_before": sum(
                      base.pod_ready(row) for row in agents.values())}
        self.summary["pod_moves"].append(record)
        self.inflight = record
        self.action(f"mock-delete/{uid(pod)}", copy.deepcopy(record), lambda:
                    self.delete_pod(self.cluster, namespace=mocks.DEFAULT_NAMESPACE, name=name, uid=record["uid"],
                                    timeout_seconds=self.remaining_seconds(self.args.request_timeout_seconds),
                                    attempts=1, retry_seconds=0))
        deadline = min(self.work_deadline - RETIREMENT_RESERVE, time.monotonic() + self.args.per_pod_ready_seconds)
        replacement_uid = ""
        while True:
            snapshot = self.snapshot()
            _, current = self.guard(snapshot)
            replacement = current.get(name)
            if replacement is not None and uid(replacement) != record["uid"]:
                require(not replacement_uid or replacement_uid == uid(replacement),
                        "The replacement changed UID; never delete or adopt a second replacement")
                require(uid(replacement) not in self.plan["mock_pod_uids"].values(),
                        "A replacement reused an original Pod UID")
                replacement_uid = uid(replacement)
                destination = replacement["spec"].get("nodeName")
                require(destination in (None, "", *self.fresh), "A replacement was scheduled outside IP-proven cniv5")
                if base.pod_ready(replacement):
                    require(destination in self.fresh
                            and replacement["status"]["podIP"] in maintenance._nnc_map(snapshot["nnc"])[destination]["ip_addresses"],
                            "Replacement Ready/IP is not allocated by its pinned destination NC")
                    record.update(ready_pod_uid=replacement_uid, ready_node=destination,
                                  ready_pod_ip=replacement["status"]["podIP"], completed=True,
                                  ready_after=sum(base.pod_ready(row) for row in current.values()))
                    self.memory[destination]["reserved"] += estimates[name]
                    self.effective[name] = replacement_uid
                    self.agent_nodes[name] = destination
                    self.protected[name] = replacement_uid
                    self.inflight = None
                    self.guard(snapshot)
                    self.save()
                    self.persist_journal()
                    return
            self.wait(deadline, "The single UID-bound mock replacement Ready/IP barrier")

    def recover_agents(self):
        _, _, _, snapshot = self.check()
        self.peers(snapshot)
        self.summary["status"] = "recovering-original-29-pending-as-one-plan"
        for name in self.pending:
            self.move(name, "pending")
        self.healthy_phase = True
        _, _, _, snapshot = self.check()
        _, agents = self.guard(snapshot)
        self.peers(snapshot)
        self.summary["pending_phase_complete"] = True
        self.summary["pending_phase_ready"] = sum(base.pod_ready(pod) for pod in agents.values())
        remaining = sorted(name for name, pod in agents.items() if pod["spec"].get("nodeName") == SOURCE)
        require(set(remaining) <= self.original_source and len(remaining) <= 25,
                "Healthy SOURCE migration exceeds its explicit identity/cap boundary")
        self.summary["healthy_source_exchange_plan"] = [{"name": name, "uid": uid(agents[name])} for name in remaining]
        self.summary["status"] = "serial-healthy-source-exchanges"
        self.save()
        for name in remaining:
            self.move(name, "healthy")
        self.guard(self.snapshot())

    def source_pods(self, snapshot, *, framework_only=False):
        rows = [pod for pod in snapshot["pods"]["items"] if pod["spec"].get("nodeName") == SOURCE
                and not (framework_only and pod["metadata"].get("labels", {}).get("app") == "mock-cilium-agent")]
        require(all(base.pvc_free(pod["spec"]) for pod in rows), "A source Pod has a PVC or ephemeral claim")
        kinds = ("DaemonSet", "ReplicaSet", "Deployment", "StatefulSet")
        inventories = [{"items": [row for row in snapshot["controllers"]["items"] if row["kind"] == kind]}
                       for kind in kinds]
        proof = maintenance._validate_pre_drain_source_pods(rows, *inventories)
        self.summary["source_pre_drain"] = proof
        return rows

    def drain_source(self):
        self.summary["status"] = "uid-and-pdb-guarded-source-drain"
        _, _, _, snapshot = self.check()
        self.source_pods(snapshot)
        while True:
            self.guard(snapshot)
            remaining = [pod for pod in self.source_pods(snapshot)
                         if base.controller_owner(pod, next(owner["kind"] for owner in pod["metadata"]["ownerReferences"]
                                                           if owner.get("controller") is True))["kind"] != "DaemonSet"]
            if not remaining:
                return
            require(self.remaining_seconds(7200) > RETIREMENT_RESERVE, "Source drain exhausted the retirement reserve")
            pod = remaining[0]
            meta = pod["metadata"]
            owner = next(row for row in meta["ownerReferences"] if row.get("controller") is True)
            controller = base.controller(snapshot, owner["kind"], meta["namespace"], owner["name"], owner["uid"])
            count = controller["spec"].get("replicas")
            require(base.integer(count) and count > 0, "Drain controller replicas are not exact")
            _pdb_allows(snapshot, pod, eviction=True)
            fixed = {uid(row): _pod_pin(row) for row in snapshot["pods"]["items"]
                     if row["metadata"].get("namespace") == meta["namespace"] and row["spec"].get("nodeName") != SOURCE
                     and any(ref.get("controller") is True and ref.get("uid") == owner["uid"]
                             for ref in row["metadata"].get("ownerReferences") or []) and base.pod_ready(row)}
            self.framework.update(fixed)
            record = {"namespace": meta["namespace"], "name": meta["name"], "uid": uid(pod), "owner": owner}
            self.summary.setdefault("drain_evictions", []).append(record)
            require(uid(pod) not in {row["ready_pod_uid"] for row in self.checkpoint["pod_moves"]},
                    "A protected monitoring target can never be drained")
            self.framework.pop(uid(pod), None)
            self.drain_inflight = record
            self.action(f"source-eviction/{uid(pod)}", record, lambda:
                        self.evict_pod(self.cluster, namespace=meta["namespace"], name=meta["name"], pod_uid=uid(pod),
                                       timeout_seconds=self.remaining_seconds(self.args.request_timeout_seconds)))
            deadline = min(self.work_deadline - 180, time.monotonic() + self.args.per_pod_ready_seconds)
            while True:
                snapshot = self.snapshot()
                self.guard(snapshot)
                owned = [row for row in snapshot["pods"]["items"] if row["metadata"].get("namespace") == meta["namespace"]
                         and any(ref.get("controller") is True and ref.get("uid") == owner["uid"]
                                 for ref in row["metadata"].get("ownerReferences") or [])]
                if (not any(uid(row) == record["uid"] for row in snapshot["pods"]["items"])
                        and len(owned) == count and all(base.pod_ready(row) for row in owned)):
                    require(all(row["spec"].get("nodeName") != SOURCE for row in owned),
                            "Drained framework rescheduled to the held source")
                    self.framework.update({uid(row): _pod_pin(row) for row in owned})
                    record["ready_replacement_uids"] = sorted(uid(row) for row in owned)
                    self.drain_inflight = None
                    self.save()
                    break
                self.wait(deadline, "UID/PDB-guarded source eviction and full controller readiness")
            _, _, _, snapshot = self.check()

    def retire_source(self):
        _, _, _, snapshot = self.check()
        require(all(base.controller_owner(pod, "DaemonSet") for pod in self.source_pods(snapshot)),
                "Source must contain only pinned PVC-free DaemonSets before native DeleteMachines")
        self.guard(snapshot)
        self.peers(snapshot)
        require(self.remaining_seconds(7200) > RETIREMENT_RESERVE,
                "Insufficient reserved time to submit and observe native source retirement")
        self.summary["status"] = "retiring-exact-original-source"
        self.action("source-retirement", {
            "node_name": SOURCE, "node_uid": base.REAL_UIDS[SOURCE], "vm_id": SOURCE_VM_ID,
            "vmss": base.DEFAULT_VMSS, "instance_id": "0", "default_before": 2, "default_after": 1,
        }, lambda: self.raw_write([
            "az", "aks", "nodepool", "delete-machines", "--resource-group", base.RESOURCE_GROUP,
            "--cluster-name", base.CLUSTER, "--name", "default", "--machine-names", SOURCE,
            "--no-wait", "--only-show-errors",
        ]))
        deadline = min(self.work_deadline, time.monotonic() + 600)
        while True:
            complete, pools, instances, snapshot = self.check("retiring")
            absent = (
                SOURCE not in instances and SOURCE not in maintenance._real_node_map(snapshot["nodes"])
                and not any(pod["spec"].get("nodeName") == SOURCE for pod in snapshot["pods"]["items"])
                and not any(row["node_uid"] == base.REAL_UIDS[SOURCE]
                            or row["network_container_id"] == base.SOURCE_NC
                            for row in maintenance._nnc_map(snapshot["nnc"]).values())
            )
            if complete and pools["default"]["count"] == 1 and absent:
                self.retired = True
                self.summary["actions"]["source-retirement"]["native_disappearance_proven"] = True
                self.summary["source_hold"]["removed_by_native_node_deletion"] = True
                self.summary["modern_cni"]["source_retired"] = True
                self.summary["last_pool_operations"]["default"] = self.summary["actions"]["source-retirement"]["operation_name"]
                self.save()
                return
            self.wait(deadline, "Exact source VM/Node/NNC/Pod-reference disappearance and default count 1")

    def finalize(self):
        self.cleanup_mode = True
        self.work_deadline = self.cleanup_deadline
        complete, pools, _, snapshot = self.check()
        require(complete and self.retired, "Final ARM models are not quiescent")
        nodes, agents = self.guard(snapshot)
        require(len(nodes) == 4 and len(self.fresh) == 2
                and {name: pool["count"] for name, pool in pools.items()} == {"default": 1, PROM_POOL: 1, POOL: 2}
                and not self.exclusions and not self.summary.get("probe_cleanup_pending")
                and all(not any(taint.get("key") in (EXCLUSION_KEY, maintenance.HOLD_ANNOTATION)
                                for taint in maintenance._taints(node)) for node in nodes.values()),
                "Final default(1)+cniv5-System(2)+promv5-User(1)/owned cleanup proof failed")
        self.peers(snapshot)
        for row in snapshot["controllers"]["items"]:
            key = (row["metadata"].get("namespace"), row["metadata"].get("name"))
            if row["kind"] == "Deployment" and key in base.HOST_DEPLOYMENTS:
                require(row.get("status", {}).get("readyReplicas", 0) >= row["spec"].get("replicas", 1),
                        f"{key}: a required final framework Deployment is not fully Ready")
        delta = {
            "modern_prom": copy.deepcopy(self.checkpoint["modern_baseline_delta"]),
            "role": base.ROLE, "cluster_name": base.CLUSTER,
            "global_pool_count_before": 201, "global_pool_count_after": 202,
            "cluster_pool_objects_before": 2, "cluster_pool_objects_after": 3,
            "default_role_workers_before": 2, "default_role_workers_temporary": 4, "default_role_workers_after": 3,
            "default_pool_count_before": 2, "default_pool_count_after": 1,
            "pools": {name: {"count": pool["count"], "mode": pool["mode"], "vm_size": pool["vmSize"],
                             "node_image_version": pool["nodeImageVersion"],
                             "configuration_sha256": digest(prepared.pool_configuration(pool))}
                      for name, pool in pools.items()},
            "heterogeneous_skus": ["Standard_D8_v3", SKU],
            "original_manifest_unchanged": True, "requires_explicit_full_baseline_handoff": True,
            "global_pool_count_basis": "explicit approved baseline; full n100 baseline verification is still required",
        }
        self.summary.update(
            status="final-proofs-complete",
            phase1_only=False, cni_recovery_only=True, workloads_ready=False,
            final_mock_ready=100, final_kwok_ready=100, final_fleet_connected=100,
            effective_identity={
                "mock_pod_uids": {name: uid(pod) for name, pod in agents.items()},
                "kwok_node_uids": copy.deepcopy(self.plan["kwok_node_uids"]),
                "real_workers": {name: copy.deepcopy(self.node_pins[name]) for name in nodes},
            },
            modern_baseline_delta=delta,
            modern_cni={
                "completed": False, "pool_name": POOL, "source_retired": self.retired,
                "default_pool_count": pools["default"]["count"],
                "destination_pool_count": pools[POOL]["count"],
                "default_role_worker_count": pools["default"]["count"] + pools[POOL]["count"],
            },
            baseline_pool_layout={
                "schema_version": 1, "role": base.ROLE, "expected_total_pool_count": 202,
                "pools": {
                    name: {
                        "count": pools[name]["count"], "mode": pools[name]["mode"],
                        "vm_size": pools[name]["vmSize"], "resource_id": pools[name]["id"],
                    } for name in ("default", PROM_POOL, POOL)
                },
            },
        )
        self.save()
        self.persist_journal()
        self.summary.update(repaired=True, success=True, status="repaired-awaiting-explicit-baseline-handoff")
        self.summary["modern_cni"]["completed"] = True
        self.save()

    def execute(self):
        self.preflight()
        if not self.args.execute:
            self.summary.update(status="planned-read-only")
            self.save()
            return
        self.check()
        self.quota()
        self.acquire()
        self.hold_source()
        self.create_pool()
        self.placement()
        self.capacity(self.snapshot(), sorted(self.original_source))
        self.prove_ip_growth()
        self.recover_agents()
        self.placement(remove=True)
        self.drain_source()
        self.retire_source()
        self.finalize()


def validate_args(args):
    require(args.resource_group == args.confirm_resource_group == base.RESOURCE_GROUP
            and args.expected_subscription.lower() == base.SUBSCRIPTION
            and args.expected_region.lower() == base.REGION, "Only the fixed approved subscription/RG/region is supported")
    require(maintenance.SHA256_RE.fullmatch(args.expected_tfvars_sha) is not None, "Expected tfvars SHA256 is invalid")
    require(base.integer(args.timeout_seconds) and 900 <= args.timeout_seconds <= 5400,
            "Total timeout must be 900..5400 seconds, including reserved finalization")
    require(base.integer(args.request_timeout_seconds) and 1 <= args.request_timeout_seconds <= 45
            and base.integer(args.poll_seconds) and 1 <= args.poll_seconds <= 30
            and base.integer(args.per_pod_ready_seconds) and 1 <= args.per_pod_ready_seconds <= 180,
            "Request, poll, or per-Pod timeout exceeds its safety bound")
    require(args.kubeconfig and args.context == base.CLUSTER, "An explicitly acquired private mesh-96 kubeconfig/context is required")
    paths = [Path(getattr(args, field)).resolve() for field in
             ("plan_file", "modern_prom_checkpoint", "summary_file", "kubeconfig")]
    require(len(set(paths)) == 4, "Plan/checkpoint/summary/kubeconfig must be distinct; inputs cannot be overwritten")
    require(not Path(args.summary_file).exists(), "Existing summary blocks duplicate execution; use a new receipt path")
    args.role = base.ROLE
    args.probe_image = maintenance.DEFAULT_PROBE_IMAGE


def execute_recovery(args, summary, runner=workers.run_command, delete_pod=None, evict_pod=None):
    validate_args(args)
    plan = base.load_plan(args.plan_file)
    checkpoint = load_checkpoint(args.modern_prom_checkpoint)
    summary.update(
        schema_version=1, recovery_kind="modern-cni", execute=args.execute, plan_valid=False,
        plan_sha256=digest(plan), modern_prom_checkpoint_sha256=digest(checkpoint),
        started_at=workers.utc_now(), status="validating", repaired=False, success=False,
        workloads_ready=False, cni_recovery_only=True, mutation_started=False,
        modern_cni={"completed": False, "pool_name": POOL, "source_retired": False},
        actions={}, pod_moves=[], cleanup_errors=[], temporary_exclusions=[],
    )
    operation = None
    try:
        operation = ModernRecovery(
            args, plan, checkpoint, summary, runner, delete_pod or mocks.delete_pod_with_uid_precondition,
            evict_pod or evict_pod_with_uid_precondition,
        )
        operation.execute()
    except BaseException as error:
        summary.update(success=False, repaired=False, workloads_ready=False, status="failed-closed",
                       error=f"{type(error).__name__}: {error}", journal_retained=True, rollback_attempted=False)
        summary["modern_cni"]["completed"] = False
        if "baseline_pool_layout" in summary:
            summary["uncommitted_baseline_pool_layout"] = summary.pop("baseline_pool_layout")
        raise
    finally:
        summary["finished_at"] = workers.utc_now()
        mocks.write_json_atomic(args.summary_file, summary)


def parse_args(argv: Optional[Sequence[str]] = None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("plan-file", "modern-prom-checkpoint", "resource-group", "confirm-resource-group",
                 "expected-subscription", "expected-region", "expected-tfvars-sha", "summary-file", "kubeconfig"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--context", default=base.CLUSTER)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=5400)
    parser.add_argument("--request-timeout-seconds", type=int, default=45)
    parser.add_argument("--poll-seconds", type=int, default=10)
    parser.add_argument("--per-pod-ready-seconds", type=int, default=180)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None):
    args = parse_args(argv)
    summary = {}

    def interrupted(signum, _frame):
        raise maintenance.MaintenanceInterrupted(f"Interrupted by signal {signum}; no further mutation is permitted")

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, interrupted)
    try:
        execute_recovery(args, summary)
    except Exception as error:
        print(f"Modern CNI recovery failed closed: {error}", file=sys.stderr)
        return 1
    print(f"{summary['status']}; workloads_ready=false; receipt={args.summary_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
