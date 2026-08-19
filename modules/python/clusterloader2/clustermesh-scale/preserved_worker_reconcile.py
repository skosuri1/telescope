#!/usr/bin/env python3
"""Repair missing real AKS workers before preserved ClusterMesh validation.

The preserved-resource gate checks the AKS control-plane state, but a cluster
can still have no registered worker Nodes while its ARM resource reports
Succeeded/Running. This reconciler compares every AKS node pool with its
backing VMSS and Kubernetes Nodes, then repairs only confirmed unhealthy VMSS
instances. It preserves each pool's existing desired count and refuses broad,
ambiguous, or in-progress changes.
"""
# pylint: disable=too-many-lines

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple


class ReconcileError(Exception):
    """Expected fail-closed reconciliation error."""


@dataclass(frozen=True)
class Cluster:
    """One preserved AKS cluster."""

    name: str
    resource_group: str
    role: str
    kubeconfig: str


@dataclass
class PoolState:
    """Observed ARM, VMSS, and Kubernetes state for one node pool."""

    role: str
    cluster_name: str
    resource_group: str
    node_resource_group: str
    pool_name: str
    desired_count: int
    pool_provisioning_state: str
    pool_power_state: str
    vmss_name: str
    vmss_capacity: int
    vmss_provisioning_state: str
    instance_ids: List[str]
    node_instance_ids: List[str]
    ready_instance_ids: List[str]
    unschedulable_nodes: List[str]
    stale_instance_ids: List[str]
    unsafe_reasons: List[str] = field(default_factory=list)

    @property
    def healthy(self) -> bool:
        return (
            not self.unsafe_reasons
            and self.pool_provisioning_state == "Succeeded"
            and self.pool_power_state in ("", "Running")
            and self.vmss_provisioning_state == "Succeeded"
            and self.vmss_capacity == self.desired_count
            and len(self.instance_ids) == self.desired_count
            and len(self.node_instance_ids) == self.desired_count
            and len(self.ready_instance_ids) == self.desired_count
            and not self.stale_instance_ids
            and not self.unschedulable_nodes
        )


@dataclass
class ClusterState:
    """Observed state for all real pools in one cluster."""

    role: str
    cluster_name: str
    resource_group: str
    pools: List[PoolState]

    @property
    def healthy(self) -> bool:
        return bool(self.pools) and all(pool.healthy for pool in self.pools)


@dataclass
class RepairResult:
    """Structured result for one repaired cluster."""

    role: str
    status: str
    actions: List[str] = field(default_factory=list)
    error: Optional[str] = None
    before: Optional[dict] = None
    after: Optional[dict] = None


Runner = Callable[[Sequence[str], int], str]
PROVIDER_ID_RE = re.compile(
    r"/virtualMachineScaleSets/(?P<vmss>[^/]+)/virtualMachines/(?P<instance>[^/]+)",
    re.IGNORECASE,
)
TRANSIENT_ARM_ERROR_RE = re.compile(
    r"AnotherOperationInProgress|OperationNotAllowed|ResourceNotFinalState|"
    r"TooManyRequests|\b429\b|temporar|timeout|timed out",
    re.IGNORECASE,
)


def utc_now() -> str:
    """Return an RFC3339 UTC timestamp."""

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_command(args: Sequence[str], timeout_seconds: int) -> str:
    """Run a bounded command and return stdout."""

    try:
        completed = subprocess.run(
            list(args),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise ReconcileError(
            f"command timed out after {timeout_seconds}s: {' '.join(args)}"
        ) from exc
    except OSError as exc:
        raise ReconcileError(f"unable to execute {args[0]}: {exc}") from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise ReconcileError(
            f"command failed (exit={completed.returncode}): {' '.join(args)}: "
            f"{detail[:1000]}"
        )
    return completed.stdout


def role_sort_key(role: str) -> Tuple[int, str]:
    """Sort mesh-N roles numerically."""

    if role.startswith("mesh-") and role[5:].isdigit():
        return int(role[5:]), role
    return sys.maxsize, role


def resolve_kubeconfig(row: dict) -> str:
    """Resolve the standard per-role kubeconfig path."""

    explicit = row.get("kubeconfig")
    if explicit:
        return os.path.expanduser(str(explicit))
    return os.path.join(
        os.path.expanduser("~"), ".kube", f"{row['role']}.config"
    )


def load_clusters(path: str) -> List[Cluster]:
    """Load and validate the cluster inventory."""

    try:
        with open(path, "r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ReconcileError(f"unable to load cluster inventory {path}: {exc}") from exc
    if not isinstance(raw, list) or not raw:
        raise ReconcileError("cluster inventory must be a non-empty JSON array")

    clusters: List[Cluster] = []
    seen_roles: Set[str] = set()
    for row in raw:
        if not isinstance(row, dict):
            raise ReconcileError("each cluster inventory row must be a JSON object")
        role = row.get("role")
        name = row.get("name")
        resource_group = row.get("rg") or row.get("resource_group")
        if not isinstance(role, str) or not role:
            raise ReconcileError("cluster inventory row is missing role")
        if not role.startswith("mesh-") or not role[5:].isdigit():
            raise ReconcileError(f"invalid cluster role in inventory: {role}")
        if role in seen_roles:
            raise ReconcileError(f"duplicate cluster role in inventory: {role}")
        if not isinstance(name, str) or not name:
            raise ReconcileError(f"{role}: cluster inventory row is missing name")
        if not isinstance(resource_group, str) or not resource_group:
            raise ReconcileError(
                f"{role}: cluster inventory row is missing resource group"
            )
        seen_roles.add(role)
        clusters.append(
            Cluster(
                name=name,
                resource_group=resource_group,
                role=role,
                kubeconfig=resolve_kubeconfig(row),
            )
        )
    return sorted(clusters, key=lambda cluster: role_sort_key(cluster.role))


def parse_json(output: str, description: str) -> object:
    """Parse JSON command output with an actionable error."""

    try:
        return json.loads(output)
    except json.JSONDecodeError as exc:
        raise ReconcileError(f"invalid {description} JSON: {exc}") from exc


def node_is_ready(node: dict) -> bool:
    """Return whether a Kubernetes Node has Ready=True."""

    conditions = node.get("status", {}).get("conditions", [])
    return any(
        isinstance(condition, dict)
        and condition.get("type") == "Ready"
        and condition.get("status") == "True"
        for condition in conditions
    )


def provider_identity(node: dict) -> Optional[Tuple[str, str]]:
    """Extract (VMSS, instance ID) from a real AKS Node providerID."""

    provider_id = node.get("spec", {}).get("providerID")
    if not isinstance(provider_id, str):
        return None
    match = PROVIDER_ID_RE.search(provider_id)
    if match is None:
        return None
    return match.group("vmss").lower(), match.group("instance")


def vmss_pool_name(vmss: dict) -> Optional[str]:
    """Read the AKS-managed node-pool tag from a VMSS."""

    tags = vmss.get("tags")
    if not isinstance(tags, dict):
        return None
    value = tags.get("aks-managed-poolName")
    return value if isinstance(value, str) and value else None


def build_cluster_state(
    cluster: Cluster,
    node_resource_group: str,
    nodepools: List[dict],
    vmsses: List[dict],
    instances_by_vmss: Dict[str, List[dict]],
    nodes: List[dict],
) -> ClusterState:
    """Build a pure, validated pool-health model from command output."""

    vmss_by_pool: Dict[str, dict] = {}
    for vmss in vmsses:
        if not isinstance(vmss, dict):
            raise ReconcileError(f"{cluster.role}: malformed VMSS list entry")
        pool_name = vmss_pool_name(vmss)
        if pool_name is None:
            continue
        if pool_name in vmss_by_pool:
            raise ReconcileError(
                f"{cluster.role}: multiple VMSS resources map to pool {pool_name}"
            )
        vmss_by_pool[pool_name] = vmss

    nodes_by_vmss: Dict[str, List[Tuple[dict, str]]] = {}
    for node in nodes:
        if not isinstance(node, dict):
            raise ReconcileError(f"{cluster.role}: malformed Kubernetes Node entry")
        identity = provider_identity(node)
        if identity is None:
            continue
        vmss_name, instance_id = identity
        nodes_by_vmss.setdefault(vmss_name, []).append((node, instance_id))

    pools: List[PoolState] = []
    for pool in nodepools:
        if not isinstance(pool, dict):
            raise ReconcileError(f"{cluster.role}: malformed AKS node-pool entry")
        pool_name = pool.get("name")
        desired_count = pool.get("count")
        if not isinstance(pool_name, str) or not pool_name:
            raise ReconcileError(f"{cluster.role}: node pool is missing name")
        if not isinstance(desired_count, int) or desired_count <= 0:
            raise ReconcileError(
                f"{cluster.role}/{pool_name}: desired count must be positive"
            )
        vmss = vmss_by_pool.get(pool_name)
        if vmss is None:
            raise ReconcileError(
                f"{cluster.role}/{pool_name}: backing VMSS was not found"
            )
        vmss_name = vmss.get("name")
        capacity = vmss.get("sku", {}).get("capacity")
        if not isinstance(vmss_name, str) or not vmss_name:
            raise ReconcileError(
                f"{cluster.role}/{pool_name}: backing VMSS has no name"
            )
        if not isinstance(capacity, int) or capacity < 0:
            raise ReconcileError(
                f"{cluster.role}/{pool_name}: invalid VMSS capacity {capacity!r}"
            )
        instances = instances_by_vmss.get(vmss_name.lower())
        if instances is None:
            raise ReconcileError(
                f"{cluster.role}/{pool_name}: VMSS instances were not collected"
            )
        instance_ids = sorted(
            str(instance.get("instanceId"))
            for instance in instances
            if isinstance(instance, dict) and instance.get("instanceId") is not None
        )
        if len(instance_ids) != len(instances):
            raise ReconcileError(
                f"{cluster.role}/{pool_name}: VMSS instance is missing instanceId"
            )

        current_instance_ids = set(instance_ids)
        matched_nodes = [
            (node, instance_id)
            for node, instance_id in nodes_by_vmss.get(vmss_name.lower(), [])
            if instance_id in current_instance_ids
        ]
        node_instance_ids = sorted(instance_id for _, instance_id in matched_nodes)
        ready_instance_ids = sorted(
            instance_id
            for node, instance_id in matched_nodes
            if node_is_ready(node)
        )
        unschedulable_nodes = sorted(
            str(node.get("metadata", {}).get("name"))
            for node, _ in matched_nodes
            if node.get("spec", {}).get("unschedulable") is True
        )
        stale_instance_ids = sorted(set(instance_ids) - set(ready_instance_ids))

        pool_provisioning_state = str(
            pool.get("provisioningState") or "Unknown"
        )
        power_state_value = pool.get("powerState")
        if isinstance(power_state_value, dict):
            pool_power_state = str(power_state_value.get("code") or "")
        else:
            pool_power_state = str(power_state_value or "")
        vmss_provisioning_state = str(
            vmss.get("provisioningState") or "Unknown"
        )

        unsafe_reasons: List[str] = []
        if pool_provisioning_state not in ("Succeeded",):
            unsafe_reasons.append(
                f"node pool provisioningState={pool_provisioning_state}"
            )
        if pool_power_state not in ("", "Running", "Stopped"):
            unsafe_reasons.append(f"node pool powerState={pool_power_state}")
        if vmss_provisioning_state != "Succeeded":
            unsafe_reasons.append(
                f"VMSS provisioningState={vmss_provisioning_state}"
            )
        if capacity > desired_count:
            unsafe_reasons.append(
                f"VMSS capacity {capacity} exceeds desired count {desired_count}"
            )
        if len(instance_ids) > desired_count:
            unsafe_reasons.append(
                f"VMSS instances {len(instance_ids)} exceed desired count "
                f"{desired_count}"
            )
        if len(node_instance_ids) > desired_count:
            unsafe_reasons.append(
                f"Kubernetes Nodes {len(node_instance_ids)} exceed desired count "
                f"{desired_count}"
            )

        pools.append(
            PoolState(
                role=cluster.role,
                cluster_name=cluster.name,
                resource_group=cluster.resource_group,
                node_resource_group=node_resource_group,
                pool_name=pool_name,
                desired_count=desired_count,
                pool_provisioning_state=pool_provisioning_state,
                pool_power_state=pool_power_state,
                vmss_name=vmss_name,
                vmss_capacity=capacity,
                vmss_provisioning_state=vmss_provisioning_state,
                instance_ids=instance_ids,
                node_instance_ids=node_instance_ids,
                ready_instance_ids=ready_instance_ids,
                unschedulable_nodes=unschedulable_nodes,
                stale_instance_ids=stale_instance_ids,
                unsafe_reasons=unsafe_reasons,
            )
        )
    return ClusterState(
        role=cluster.role,
        cluster_name=cluster.name,
        resource_group=cluster.resource_group,
        pools=sorted(pools, key=lambda pool: pool.pool_name),
    )


def probe_cluster(
    cluster: Cluster,
    runner: Runner,
    query_timeout_seconds: int,
) -> ClusterState:
    """Collect ARM, VMSS, and Kubernetes state for one cluster."""

    node_resource_group = runner(
        [
            "az",
            "aks",
            "show",
            "--resource-group",
            cluster.resource_group,
            "--name",
            cluster.name,
            "--query",
            "nodeResourceGroup",
            "--output",
            "tsv",
            "--only-show-errors",
        ],
        query_timeout_seconds,
    ).strip()
    if not node_resource_group:
        raise ReconcileError(f"{cluster.role}: AKS nodeResourceGroup is empty")

    nodepools_raw = parse_json(
        runner(
            [
                "az",
                "aks",
                "nodepool",
                "list",
                "--resource-group",
                cluster.resource_group,
                "--cluster-name",
                cluster.name,
                "--output",
                "json",
                "--only-show-errors",
            ],
            query_timeout_seconds,
        ),
        f"{cluster.role} AKS node-pool",
    )
    vmsses_raw = parse_json(
        runner(
            [
                "az",
                "vmss",
                "list",
                "--resource-group",
                node_resource_group,
                "--output",
                "json",
                "--only-show-errors",
            ],
            query_timeout_seconds,
        ),
        f"{cluster.role} VMSS",
    )
    nodes_raw = parse_json(
        runner(
            [
                "kubectl",
                "--kubeconfig",
                cluster.kubeconfig,
                f"--request-timeout={query_timeout_seconds}s",
                "get",
                "nodes",
                "-o",
                "json",
            ],
            query_timeout_seconds,
        ),
        f"{cluster.role} Kubernetes Node",
    )
    if not isinstance(nodepools_raw, list):
        raise ReconcileError(f"{cluster.role}: AKS node-pool output is not an array")
    if not isinstance(vmsses_raw, list):
        raise ReconcileError(f"{cluster.role}: VMSS output is not an array")
    if not isinstance(nodes_raw, dict) or not isinstance(nodes_raw.get("items"), list):
        raise ReconcileError(
            f"{cluster.role}: Kubernetes Node output has no items array"
        )

    instances_by_vmss: Dict[str, List[dict]] = {}
    for vmss in vmsses_raw:
        if not isinstance(vmss, dict):
            raise ReconcileError(f"{cluster.role}: malformed VMSS list entry")
        if vmss_pool_name(vmss) is None:
            continue
        vmss_name = vmss.get("name")
        if not isinstance(vmss_name, str) or not vmss_name:
            raise ReconcileError(f"{cluster.role}: managed VMSS has no name")
        instances_raw = parse_json(
            runner(
                [
                    "az",
                    "vmss",
                    "list-instances",
                    "--resource-group",
                    node_resource_group,
                    "--name",
                    vmss_name,
                    "--output",
                    "json",
                    "--only-show-errors",
                ],
                query_timeout_seconds,
            ),
            f"{cluster.role}/{vmss_name} VMSS instance",
        )
        if not isinstance(instances_raw, list):
            raise ReconcileError(
                f"{cluster.role}/{vmss_name}: VMSS instances are not an array"
            )
        instances_by_vmss[vmss_name.lower()] = instances_raw

    return build_cluster_state(
        cluster,
        node_resource_group,
        nodepools_raw,
        vmsses_raw,
        instances_by_vmss,
        nodes_raw["items"],
    )


def state_to_dict(state: ClusterState) -> dict:
    """Convert a cluster state to JSON-safe primitives."""

    return {
        "role": state.role,
        "cluster_name": state.cluster_name,
        "resource_group": state.resource_group,
        "healthy": state.healthy,
        "pools": [
            {
                **asdict(pool),
                "healthy": pool.healthy,
            }
            for pool in state.pools
        ],
    }


def scale_nodepool(
    pool: PoolState,
    target_count: int,
    runner: Runner,
    mutation_timeout_seconds: int,
) -> None:
    """Submit one bounded AKS node-pool scale request."""

    run_mutation_with_retries(
        [
            "az",
            "aks",
            "nodepool",
            "scale",
            "--resource-group",
            pool.resource_group,
            "--cluster-name",
            pool.cluster_name,
            "--name",
            pool.pool_name,
            "--node-count",
            str(target_count),
            "--no-wait",
            "--output",
            "none",
            "--only-show-errors",
        ],
        runner,
        mutation_timeout_seconds=mutation_timeout_seconds,
    )


def run_mutation_with_retries(
    args: Sequence[str],
    runner: Runner,
    *,
    mutation_timeout_seconds: int,
    attempts: int = 5,
    retry_seconds: int = 30,
) -> str:
    """Retry only known transient Azure mutation conflicts."""

    last_error: Optional[ReconcileError] = None
    for attempt in range(1, attempts + 1):
        try:
            return runner(args, mutation_timeout_seconds)
        except ReconcileError as exc:
            last_error = exc
            if attempt >= attempts or TRANSIENT_ARM_ERROR_RE.search(str(exc)) is None:
                raise
            print(
                f"Transient Azure mutation failure on attempt "
                f"{attempt}/{attempts}: {exc}",
                file=sys.stderr,
            )
            time.sleep(retry_seconds)
    if last_error is None:
        raise ReconcileError("mutation retry called without attempts")
    raise last_error


def wait_arm_pool_count(
    pool: PoolState,
    target_count: int,
    runner: Runner,
    query_timeout_seconds: int,
    deadline: float,
    poll_seconds: int,
) -> None:
    """Wait until both AKS and VMSS report the requested stable count."""

    last_error: Optional[str] = None
    while time.monotonic() < deadline:
        try:
            pool_payload = parse_json(
                runner(
                    [
                        "az",
                        "aks",
                        "nodepool",
                        "show",
                        "--resource-group",
                        pool.resource_group,
                        "--cluster-name",
                        pool.cluster_name,
                        "--name",
                        pool.pool_name,
                        "--output",
                        "json",
                        "--only-show-errors",
                    ],
                    query_timeout_seconds,
                ),
                f"{pool.role}/{pool.pool_name} node-pool",
            )
            vmss_payload = parse_json(
                runner(
                    [
                        "az",
                        "vmss",
                        "show",
                        "--resource-group",
                        pool.node_resource_group,
                        "--name",
                        pool.vmss_name,
                        "--output",
                        "json",
                        "--only-show-errors",
                    ],
                    query_timeout_seconds,
                ),
                f"{pool.role}/{pool.pool_name} VMSS",
            )
        except ReconcileError as exc:
            last_error = str(exc)
            print(
                f"{pool.role}/{pool.pool_name}: ARM wait query failed; "
                f"retrying: {exc}",
                file=sys.stderr,
            )
            time.sleep(poll_seconds)
            continue
        if not isinstance(pool_payload, dict) or not isinstance(vmss_payload, dict):
            raise ReconcileError(
                f"{pool.role}/{pool.pool_name}: malformed ARM wait response"
            )
        if (
            pool_payload.get("count") == target_count
            and pool_payload.get("provisioningState") == "Succeeded"
            and vmss_payload.get("sku", {}).get("capacity") == target_count
            and vmss_payload.get("provisioningState") == "Succeeded"
        ):
            return
        time.sleep(poll_seconds)
    raise ReconcileError(
        f"{pool.role}/{pool.pool_name}: timed out waiting for stable count "
        f"{target_count}"
        + (f"; last query error: {last_error}" if last_error else "")
    )


def read_arm_pool_counts(
    pool: PoolState,
    runner: Runner,
    query_timeout_seconds: int,
    attempts: int = 5,
    retry_seconds: int = 15,
) -> Tuple[int, int]:
    """Read the current AKS desired count and backing VMSS capacity."""

    last_error: Optional[ReconcileError] = None
    for attempt in range(1, attempts + 1):
        try:
            current_pool = parse_json(
                runner(
                    [
                        "az",
                        "aks",
                        "nodepool",
                        "show",
                        "--resource-group",
                        pool.resource_group,
                        "--cluster-name",
                        pool.cluster_name,
                        "--name",
                        pool.pool_name,
                        "--output",
                        "json",
                        "--only-show-errors",
                    ],
                    query_timeout_seconds,
                ),
                f"{pool.role}/{pool.pool_name} node-pool",
            )
            current_vmss = parse_json(
                runner(
                    [
                        "az",
                        "vmss",
                        "show",
                        "--resource-group",
                        pool.node_resource_group,
                        "--name",
                        pool.vmss_name,
                        "--output",
                        "json",
                        "--only-show-errors",
                    ],
                    query_timeout_seconds,
                ),
                f"{pool.role}/{pool.pool_name} VMSS",
            )
            if not isinstance(current_pool, dict) or not isinstance(
                current_vmss, dict
            ):
                raise ReconcileError(
                    f"{pool.role}/{pool.pool_name}: malformed ARM count response"
                )
            current_count = current_pool.get("count")
            current_capacity = current_vmss.get("sku", {}).get("capacity")
            if not isinstance(current_count, int) or not isinstance(
                current_capacity, int
            ):
                raise ReconcileError(
                    f"{pool.role}/{pool.pool_name}: invalid ARM count/capacity"
                )
            return current_count, current_capacity
        except ReconcileError as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(retry_seconds)
    if last_error is None:
        raise ReconcileError("ARM count query retry called without attempts")
    raise last_error


def restore_pool_desired_count(
    pool: PoolState,
    runner: Runner,
    *,
    query_timeout_seconds: int,
    mutation_timeout_seconds: int,
    recovery_timeout_seconds: int,
    poll_seconds: int,
    force_nudge: bool,
) -> List[str]:
    """Force the pool back to its captured desired count."""

    actions: List[str] = []
    deadline = time.monotonic() + recovery_timeout_seconds
    current_count, current_capacity = read_arm_pool_counts(
        pool, runner, query_timeout_seconds
    )
    nudge_attempted = False
    if current_count == pool.desired_count and (
        force_nudge or current_capacity < pool.desired_count
    ):
        nudge_count = pool.desired_count + 1
        scale_nodepool(pool, nudge_count, runner, mutation_timeout_seconds)
        nudge_attempted = True
        actions.append(f"nudged desired count to {nudge_count}")
        try:
            wait_arm_pool_count(
                pool,
                nudge_count,
                runner,
                query_timeout_seconds,
                deadline,
                poll_seconds,
            )
        except ReconcileError as exc:
            print(
                f"{pool.role}/{pool.pool_name}: count nudge did not fully "
                f"converge before restore: {exc}",
                file=sys.stderr,
            )
        current_count = nudge_count

    if current_count != pool.desired_count:
        scale_nodepool(
            pool, pool.desired_count, runner, mutation_timeout_seconds
        )
        actions.append(f"restored desired count to {pool.desired_count}")
    if nudge_attempted:
        deadline = max(
            deadline, time.monotonic() + recovery_timeout_seconds
        )
    wait_arm_pool_count(
        pool,
        pool.desired_count,
        runner,
        query_timeout_seconds,
        deadline,
        poll_seconds,
    )
    return actions


def repair_pool(
    pool: PoolState,
    cluster: Cluster,
    runner: Runner,
    *,
    query_timeout_seconds: int,
    mutation_timeout_seconds: int,
    recovery_timeout_seconds: int,
    poll_seconds: int,
) -> List[str]:
    """Repair one confirmed unhealthy pool and preserve its desired count."""

    if pool.unsafe_reasons:
        raise ReconcileError(
            f"{pool.role}/{pool.pool_name}: unsafe to repair: "
            + "; ".join(pool.unsafe_reasons)
        )
    actions: List[str] = []
    mutated = False
    stale_instances_deleted = False

    try:
        for node_name in pool.unschedulable_nodes:
            runner(
                [
                    "kubectl",
                    "--kubeconfig",
                    cluster.kubeconfig,
                    f"--request-timeout={query_timeout_seconds}s",
                    "uncordon",
                    node_name,
                ],
                query_timeout_seconds,
            )
            actions.append(f"uncordoned {node_name}")

        if pool.pool_power_state == "Stopped":
            mutated = True
            run_mutation_with_retries(
                [
                    "az",
                    "aks",
                    "nodepool",
                    "start",
                    "--resource-group",
                    pool.resource_group,
                    "--cluster-name",
                    pool.cluster_name,
                    "--name",
                    pool.pool_name,
                    "--no-wait",
                    "--output",
                    "none",
                    "--only-show-errors",
                ],
                runner,
                mutation_timeout_seconds=mutation_timeout_seconds,
            )
            actions.append(f"started node pool {pool.pool_name}")

        if pool.stale_instance_ids:
            mutated = True
            stale_instances_deleted = True
            run_mutation_with_retries(
                [
                    "az",
                    "vmss",
                    "delete-instances",
                    "--resource-group",
                    pool.node_resource_group,
                    "--name",
                    pool.vmss_name,
                    "--instance-ids",
                    *pool.stale_instance_ids,
                    "--output",
                    "none",
                    "--only-show-errors",
                ],
                runner,
                mutation_timeout_seconds=mutation_timeout_seconds,
            )
            actions.append(
                f"deleted unhealthy VMSS instances "
                f"{','.join(pool.stale_instance_ids)}"
            )
        mutated = True
        mutated = True
        actions.extend(
            restore_pool_desired_count(
                pool,
                runner,
                query_timeout_seconds=query_timeout_seconds,
                mutation_timeout_seconds=mutation_timeout_seconds,
                recovery_timeout_seconds=recovery_timeout_seconds,
                poll_seconds=poll_seconds,
                force_nudge=stale_instances_deleted,
            )
        )
        return actions
    except ReconcileError as exc:
        if not mutated:
            raise
        try:
            rollback_actions = restore_pool_desired_count(
                pool,
                runner,
                query_timeout_seconds=query_timeout_seconds,
                mutation_timeout_seconds=mutation_timeout_seconds,
                recovery_timeout_seconds=recovery_timeout_seconds,
                poll_seconds=poll_seconds,
                force_nudge=stale_instances_deleted,
            )
            if rollback_actions:
                print(
                    f"{pool.role}/{pool.pool_name}: rollback actions: "
                    + "; ".join(rollback_actions),
                    file=sys.stderr,
                )
        except ReconcileError as rollback_error:
            raise ReconcileError(
                f"{exc}; rollback to desired count {pool.desired_count} "
                f"also failed: {rollback_error}"
            ) from exc
        raise


def repair_cluster(
    cluster: Cluster,
    before: ClusterState,
    runner: Runner,
    *,
    query_timeout_seconds: int,
    mutation_timeout_seconds: int,
    recovery_timeout_seconds: int,
    poll_seconds: int,
) -> RepairResult:
    """Repair every unhealthy pool in one cluster and verify Kubernetes health."""

    result = RepairResult(
        role=cluster.role,
        status="failed",
        before=state_to_dict(before),
    )
    try:
        for pool in before.pools:
            if pool.healthy:
                continue
            result.actions.extend(
                repair_pool(
                    pool,
                    cluster,
                    runner,
                    query_timeout_seconds=query_timeout_seconds,
                    mutation_timeout_seconds=mutation_timeout_seconds,
                    recovery_timeout_seconds=recovery_timeout_seconds,
                    poll_seconds=poll_seconds,
                )
            )

        deadline = time.monotonic() + recovery_timeout_seconds
        last_probe_error: Optional[str] = None
        while time.monotonic() < deadline:
            try:
                after = probe_cluster(cluster, runner, query_timeout_seconds)
            except ReconcileError as exc:
                last_probe_error = str(exc)
                time.sleep(poll_seconds)
                continue
            result.after = state_to_dict(after)
            if after.healthy:
                result.status = "repaired"
                return result
            time.sleep(poll_seconds)
        raise ReconcileError(
            f"{cluster.role}: real worker pools did not become healthy within "
            f"{recovery_timeout_seconds}s"
            + (
                f"; last probe error: {last_probe_error}"
                if last_probe_error
                else ""
            )
        )
    except ReconcileError as exc:
        result.error = str(exc)
        return result


def write_json_atomic(path: str, payload: dict) -> None:
    """Write JSON atomically."""

    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    temporary = f"{path}.tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(temporary, path)


def probe_with_confirmation(
    cluster: Cluster,
    runner: Runner,
    *,
    query_timeout_seconds: int,
    attempts: int,
    retry_seconds: int,
) -> ClusterState:
    """Confirm worker drift across repeated complete snapshots."""

    latest: Optional[ClusterState] = None
    last_error: Optional[ReconcileError] = None
    for attempt in range(1, attempts + 1):
        try:
            latest = probe_cluster(cluster, runner, query_timeout_seconds)
            last_error = None
        except ReconcileError as exc:
            last_error = exc
            if attempt == attempts:
                raise
            time.sleep(retry_seconds)
            continue
        if latest.healthy or attempt == attempts:
            return latest
        time.sleep(retry_seconds)
    if last_error is not None:
        raise last_error
    raise ReconcileError(f"{cluster.role}: worker probe did not run")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clusters", required=True)
    parser.add_argument("--summary-file", required=True)
    parser.add_argument("--max-repair-clusters", type=int, default=5)
    parser.add_argument("--max-concurrent-probes", type=int, default=5)
    parser.add_argument("--max-concurrent-repairs", type=int, default=2)
    parser.add_argument("--probe-attempts", type=int, default=5)
    parser.add_argument("--probe-retry-seconds", type=int, default=60)
    parser.add_argument("--query-timeout-seconds", type=int, default=120)
    parser.add_argument("--mutation-timeout-seconds", type=int, default=1200)
    parser.add_argument("--recovery-timeout-seconds", type=int, default=1800)
    parser.add_argument("--poll-seconds", type=int, default=30)
    args = parser.parse_args(argv)
    positive_names = (
        "max_repair_clusters",
        "max_concurrent_probes",
        "max_concurrent_repairs",
        "probe_attempts",
        "query_timeout_seconds",
        "mutation_timeout_seconds",
        "recovery_timeout_seconds",
        "poll_seconds",
    )
    for name in positive_names:
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if args.probe_retry_seconds < 0:
        parser.error("--probe-retry-seconds must be non-negative")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Probe, bound, repair, and verify preserved real worker pools."""

    args = parse_args(argv)
    started_at = utc_now()
    try:
        clusters = load_clusters(args.clusters)

        def probe_one(cluster: Cluster) -> ClusterState:
            return probe_with_confirmation(
                cluster,
                run_command,
                query_timeout_seconds=args.query_timeout_seconds,
                attempts=args.probe_attempts,
                retry_seconds=args.probe_retry_seconds,
            )

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=args.max_concurrent_probes
        ) as executor:
            states = list(executor.map(probe_one, clusters))
        unhealthy = [state for state in states if not state.healthy]
        if len(unhealthy) > args.max_repair_clusters:
            raise ReconcileError(
                f"refusing worker repair on {len(unhealthy)} clusters; maximum "
                f"is {args.max_repair_clusters}: "
                + " ".join(
                    sorted(
                        (state.role for state in unhealthy),
                        key=role_sort_key,
                    )
                )
            )
        unsafe_pools = [
            pool
            for state in unhealthy
            for pool in state.pools
            if pool.unsafe_reasons
        ]
        if unsafe_pools:
            raise ReconcileError(
                "refusing worker mutation because one or more pools are "
                "ambiguous or have an active operation: "
                + "; ".join(
                    f"{pool.role}/{pool.pool_name}="
                    + ",".join(pool.unsafe_reasons)
                    for pool in unsafe_pools
                )
            )

        cluster_by_role = {cluster.role: cluster for cluster in clusters}
        if unhealthy:
            def repair_one(state: ClusterState) -> RepairResult:
                return repair_cluster(
                    cluster_by_role[state.role],
                    state,
                    run_command,
                    query_timeout_seconds=args.query_timeout_seconds,
                    mutation_timeout_seconds=args.mutation_timeout_seconds,
                    recovery_timeout_seconds=args.recovery_timeout_seconds,
                    poll_seconds=args.poll_seconds,
                )

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=args.max_concurrent_repairs
            ) as executor:
                results = list(executor.map(repair_one, unhealthy))
        else:
            results = []

        failures = [result for result in results if result.status != "repaired"]
        summary = {
            "schema_version": 1,
            "started_at": started_at,
            "finished_at": utc_now(),
            "healthy": not failures,
            "cluster_count": len(clusters),
            "repair_cluster_count": len(unhealthy),
            "initial_states": [state_to_dict(state) for state in states],
            "repairs": [asdict(result) for result in results],
        }
        write_json_atomic(args.summary_file, summary)
        if failures:
            for failure in failures:
                print(
                    f"{failure.role}: worker repair failed: {failure.error}",
                    file=sys.stderr,
                )
            return 1
        if unhealthy:
            print(
                "Preserved worker recovery completed: "
                + " ".join(
                    sorted(
                        (state.role for state in unhealthy),
                        key=role_sort_key,
                    )
                )
            )
        else:
            print(f"Preserved real workers healthy on {len(clusters)} clusters.")
        return 0
    except ReconcileError as exc:
        write_json_atomic(
            args.summary_file,
            {
                "schema_version": 1,
                "started_at": started_at,
                "finished_at": utc_now(),
                "healthy": False,
                "fatal_error": str(exc),
            },
        )
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
