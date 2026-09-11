#!/usr/bin/env python3
"""Clear stale AKS ARM failure states after healthy Fleet mesh formation."""

# pylint: disable=too-many-lines

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Sequence, Tuple


class ReconcileError(Exception):
    """Expected fail-closed reconciliation error."""

    def __init__(self, message: str, *, evidence: Optional[dict] = None):
        super().__init__(message)
        self.evidence = evidence or {}


class InventoryBusyError(ReconcileError):
    """A recognized in-flight operation that may only be observed."""


@dataclass(frozen=True)
class Cluster:
    """Validated AKS cluster inventory entry."""

    name: str
    role: str
    resource_group: str
    resource_id: str
    node_resource_group: str
    state: str
    power_state: str
    failed_pools: Tuple[str, ...] = ()


@dataclass
class ReconcileResult:
    """Result of reconciling one stale AKS ARM state."""

    name: str
    role: str
    status: str = "failed"
    attempts: int = 0
    observed_states: List[str] = field(default_factory=list)
    error: Optional[str] = None


Runner = Callable[[Sequence[str], int], str]
TRANSIENT_UPDATE_RE = re.compile(
    r"AnotherOperationInProgress|OperationNotAllowed|ResourceNotFinalState|"
    r"EtagMismatch|TooManyRequests|\b429\b|temporar|timeout|timed out",
    re.IGNORECASE,
)
TRANSIENT_READ_RE = re.compile(
    r"command timed out|TooManyRequests|\b429\b|ServiceUnavailable|"
    r"InternalServerError|temporar",
    re.IGNORECASE,
)
ROLE_RE = re.compile(r"^mesh-(?P<number>[1-9][0-9]*)$")
BUSY_POOL_STATES = {"Updating", "Scaling", "Upgrading", "DeletingMachines"}
MAX_FAILED_POOL_REPAIRS = 5
POOL_CONFIG_FIELDS = (
    "count", "vmSize", "mode", "osType", "osSKU", "osDiskType", "osDiskSizeGb",
    "maxPods", "vnetSubnetId", "podSubnetId", "availabilityZones",
    "enableAutoScaling", "minCount", "maxCount", "nodeLabels", "nodeTaints",
    "orchestratorVersion", "kubeletConfig", "linuxOSConfig",
)


def utc_now() -> str:
    """Return the current UTC time in RFC3339 format."""

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
            f"{detail[:2000]}"
        )
    return completed.stdout


def parse_json(output: str, description: str) -> object:
    """Parse JSON command output with an actionable error."""

    try:
        return json.loads(output)
    except json.JSONDecodeError as exc:
        raise ReconcileError(f"invalid {description} JSON: {exc}") from exc


def run_read_with_retries(
    args: Sequence[str],
    runner: Runner,
    *,
    timeout_seconds: int,
    attempts: int,
    retry_seconds: int,
) -> str:
    """Retry only transient failures for a read-only command."""

    last_error: Optional[ReconcileError] = None
    for attempt in range(1, attempts + 1):
        try:
            return runner(args, timeout_seconds)
        except ReconcileError as exc:
            last_error = exc
            if (
                attempt >= attempts
                or TRANSIENT_READ_RE.search(str(exc)) is None
            ):
                raise
            print(
                f"Transient inventory read failure on attempt "
                f"{attempt}/{attempts}: {exc}",
                file=sys.stderr,
            )
            time.sleep(retry_seconds)
    if last_error is None:
        raise ReconcileError("inventory retry called without attempts")
    raise last_error


def validate_resource_group(
    payload: object,
    target_run_id: str,
    region: str,
    expected_count: int,
    expected_tfvars_sha: str,
) -> None:
    """Validate the preserved parent RG identity."""

    if not isinstance(payload, dict):
        raise ReconcileError("resource-group response is not an object")
    tags = payload.get("tags")
    if not isinstance(tags, dict):
        raise ReconcileError("preserved resource group has no tags")
    if str(payload.get("location") or "").lower() != region.lower():
        raise ReconcileError(
            f"preserved RG region mismatch: expected {region}, "
            f"got {payload.get('location') or 'missing'}"
        )
    expected_tags = {
        "run_id": target_run_id,
        "scenario": "perf-eval-clustermesh-scale",
        "clustermesh_debug_preserved": "true",
        "clustermesh_debug_expected_clusters": str(expected_count),
        "clustermesh_debug_tfvars_sha256": expected_tfvars_sha,
    }
    for key, expected in expected_tags.items():
        if tags.get(key) != expected:
            raise ReconcileError(
                f"preserved RG tag {key} mismatch: expected {expected}, "
                f"got {tags.get(key) or 'missing'}"
            )


def _pool_is_quiescent(pool: object, *, allow_failed: bool) -> bool:
    if not isinstance(pool, dict):
        return False
    power = pool.get("powerState")
    power_state = power.get("code") if isinstance(power, dict) else power
    allowed_states = ("Succeeded", "Failed") if allow_failed else ("Succeeded",)
    return (
        pool.get("provisioningState") in allowed_states
        and power_state in (None, "", "Running")
    )


def validate_cluster_inventory(
    payload: object,
    *,
    expected_count: int,
    region: str,
    max_repair_clusters: int,
    allow_failed_pool_repair: bool = False,
) -> Tuple[List[Cluster], List[Cluster]]:
    """Validate exact cluster identity and return all/failed clusters."""

    if not isinstance(payload, list) or len(payload) != expected_count:
        actual = len(payload) if isinstance(payload, list) else "non-array"
        raise ReconcileError(
            f"expected exactly {expected_count} AKS clusters, got {actual}"
        )

    clusters: List[Cluster] = []
    role_numbers: List[int] = []
    seen_names = set()
    for row in payload:
        if not isinstance(row, dict):
            raise ReconcileError("malformed AKS inventory entry")
        name = row.get("name")
        role = (row.get("tags") or {}).get("role")
        match = ROLE_RE.fullmatch(str(role or ""))
        if not isinstance(name, str) or not name:
            raise ReconcileError("AKS inventory entry has no name")
        if name in seen_names:
            raise ReconcileError(f"duplicate AKS cluster name: {name}")
        if match is None:
            raise ReconcileError(f"{name}: invalid or missing mesh role {role!r}")
        number = int(match.group("number"))
        if name != f"clustermesh-{number}":
            raise ReconcileError(
                f"{role}: expected cluster name clustermesh-{number}, got {name}"
            )
        if str(row.get("location") or "").lower() != region.lower():
            raise ReconcileError(f"{role}: cluster is outside {region}")
        network_profile = row.get("networkProfile")
        if not isinstance(network_profile, dict) or (
            network_profile.get("networkDataplane") != "cilium"
            or network_profile.get("networkPolicy") != "cilium"
        ):
            raise ReconcileError(
                f"{role}: cluster does not use Cilium dataplane and policy"
            )
        state = str(row.get("provisioningState") or "Unknown")
        power = row.get("powerState")
        power_state = str(
            power.get("code") if isinstance(power, dict) else power or ""
        )
        if power_state not in ("", "Running"):
            raise ReconcileError(f"{role}: unsafe powerState={power_state}")
        if state == "Updating":
            raise InventoryBusyError(f"{role}: unsafe provisioningState={state}")
        if state not in ("Succeeded", "Failed"):
            raise ReconcileError(f"{role}: unsafe provisioningState={state}")
        resource_id = row.get("id")
        resource_group = row.get("resourceGroup")
        node_resource_group = row.get("nodeResourceGroup")
        if not isinstance(resource_group, str) or not resource_group:
            raise ReconcileError(f"{role}: AKS resource group is missing")
        if not isinstance(resource_id, str) or not resource_id:
            raise ReconcileError(f"{role}: AKS resource ID is missing")
        if not isinstance(node_resource_group, str) or not node_resource_group:
            raise ReconcileError(f"{role}: node resource group is missing")
        pools = row.get("agentPoolProfiles")
        if not isinstance(pools, list) or not pools:
            raise ReconcileError(
                f"{role}: node pool inventory is missing or malformed"
            )
        unsafe_pools = [
            pool for pool in pools
            if not _pool_is_quiescent(
                pool, allow_failed=state == "Failed" or allow_failed_pool_repair
            )
        ]
        if unsafe_pools:
            details = []
            busy_only = True
            for pool in unsafe_pools:
                if not isinstance(pool, dict):
                    details.append({"pool": "malformed"})
                    busy_only = False
                    continue
                pool_power = pool.get("powerState")
                pool_power = (
                    pool_power.get("code")
                    if isinstance(pool_power, dict) else pool_power
                )
                pool_state = pool.get("provisioningState")
                details.append({
                    "pool": pool.get("name"),
                    "provisioning_state": pool_state,
                    "power_state": pool_power,
                })
                busy_only = busy_only and (
                    pool_state in BUSY_POOL_STATES
                    and pool_power in (None, "", "Running")
                )
            error_type = InventoryBusyError if busy_only else ReconcileError
            raise error_type(
                f"{role}: one or more node pools are not safely quiescent: "
                + json.dumps(details, sort_keys=True)
            )
        failed_pools = tuple(
            str(pool.get("name") or "")
            for pool in pools
            if allow_failed_pool_repair and pool.get("provisioningState") == "Failed"
        )
        if any(name not in ("default", "prompool", "churnpool") for name in failed_pools):
            raise ReconcileError(f"{role}: refusing repair of an unknown failed pool")
        seen_names.add(name)
        role_numbers.append(number)
        clusters.append(
            Cluster(
                name=name,
                role=str(role),
                resource_group=resource_group,
                resource_id=resource_id,
                node_resource_group=node_resource_group,
                state=state,
                power_state=power_state,
                failed_pools=failed_pools,
            )
        )

    if sorted(role_numbers) != list(range(1, expected_count + 1)):
        raise ReconcileError(
            f"AKS role inventory is not exactly mesh-1..mesh-{expected_count}"
        )
    failed = [cluster for cluster in clusters if cluster.state == "Failed"]
    if len(failed) > max_repair_clusters:
        raise ReconcileError(
            f"refusing AKS ARM repair on {len(failed)} clusters; maximum is "
            f"{max_repair_clusters}"
        )
    if sum(len(cluster.failed_pools) for cluster in clusters) > MAX_FAILED_POOL_REPAIRS:
        raise ReconcileError(
            f"refusing failed-pool repair above maximum {MAX_FAILED_POOL_REPAIRS}"
        )
    return clusters, failed


def validate_fleet_members(payload: object, clusters: List[Cluster]) -> None:
    """Require exact, fully Connected Fleet membership."""

    if not isinstance(payload, list):
        raise ReconcileError("Fleet member response is not an array")
    expected_roles = {cluster.role for cluster in clusters}
    actual_roles = {
        str(row.get("name"))
        for row in payload
        if isinstance(row, dict) and row.get("name") is not None
    }
    if actual_roles != expected_roles:
        raise ReconcileError("Fleet member names do not match AKS mesh roles")
    unhealthy = [
        str(row.get("name"))
        for row in payload
        if not isinstance(row, dict)
        or row.get("meshProperties", {}).get("status", {}).get("state")
        != "Connected"
    ]
    if unhealthy:
        raise ReconcileError(
            "refusing AKS ARM repair while Fleet members are not Connected: "
            + " ".join(sorted(unhealthy))
        )


def validate_latest_operation(cluster: Cluster, payload: object) -> dict:
    """Require the latest AKS operation to be the known Fleet addon failure."""

    if not isinstance(payload, dict):
        raise ReconcileError(
            f"{cluster.role}: latest AKS operation response is not an object"
        )
    status = str(payload.get("status") or "")
    operation_type = str(payload.get("operationType") or "")
    error = payload.get("error")
    error_code = error.get("code") if isinstance(error, dict) else None
    if (
        status != "Failed"
        or operation_type != "PutExtensionAddon"
        or error_code != "OverlaymgrReconcileError"
    ):
        raise ReconcileError(
            f"{cluster.role}: latest AKS operation is not the expected failed "
            "PutExtensionAddon/OverlaymgrReconcileError"
        )
    return {
        "operation_id": str(payload.get("name") or ""),
        "operation_type": operation_type,
        "status": status,
        "error_code": str(error_code),
        "start_time": str(payload.get("startTime") or ""),
        "end_time": str(payload.get("endTime") or ""),
    }


def validate_cluster_data_plane(
    cluster: Cluster,
    kubeconfig: str,
    expected_remote_count: int,
    runner: Runner,
    query_timeout_seconds: int,
    identity_inventory: Optional[str] = None,
) -> Optional[dict]:
    """Require a reachable control plane and healthy live ClusterMesh."""

    runner(
        [
            "az",
            "aks",
            "get-credentials",
            "--resource-group",
            cluster.resource_group,
            "--name",
            cluster.name,
            "--file",
            kubeconfig,
            "--overwrite-existing",
            "--only-show-errors",
        ],
        query_timeout_seconds,
    )
    ready = runner(
        [
            "kubectl",
            "--kubeconfig",
            kubeconfig,
            f"--request-timeout={query_timeout_seconds}s",
            "get",
            "--raw=/readyz",
        ],
        query_timeout_seconds,
    ).strip()
    if ready != "ok":
        raise ReconcileError(f"{cluster.role}: Kubernetes readyz returned {ready!r}")

    deployment = parse_json(
        runner(
            [
                "kubectl",
                "--kubeconfig",
                kubeconfig,
                f"--request-timeout={query_timeout_seconds}s",
                "-n",
                "kube-system",
                "get",
                "deployment",
                "clustermesh-apiserver",
                "-o",
                "json",
            ],
            query_timeout_seconds,
        ),
        f"{cluster.role} clustermesh-apiserver",
    )
    if not isinstance(deployment, dict) or not any(
        condition.get("type") == "Available"
        and condition.get("status") == "True"
        for condition in deployment.get("status", {}).get("conditions", [])
        if isinstance(condition, dict)
    ):
        raise ReconcileError(
            f"{cluster.role}: clustermesh-apiserver is not Available"
        )

    if identity_inventory is not None:
        summary_path = f"{kubeconfig}.cilium-health.json"
        if os.path.exists(summary_path):
            os.remove(summary_path)
        command_error = None
        try:
            runner(
                [
                    sys.executable,
                    os.path.join(os.path.dirname(__file__), "cilium_agent_health.py"),
                    "--role", cluster.role,
                    "--kubeconfig", kubeconfig,
                    "--expected-remote-count", str(expected_remote_count),
                    "--identity-inventory", identity_inventory,
                    "--attempts", "3",
                    "--retry-seconds", "5",
                    "--command-timeout-seconds", "20",
                    "--summary-file", summary_path,
                ],
                max(query_timeout_seconds, 300),
            )
        except ReconcileError as error:
            command_error = str(error)
        try:
            with open(summary_path, encoding="utf-8") as handle:
                summary = json.load(handle)
        except (OSError, json.JSONDecodeError) as error:
            raise ReconcileError(
                f"{cluster.role}: Cilium proof is unavailable: {error}",
                evidence={"command_error": command_error},
            ) from error
        if not isinstance(summary, dict):
            raise ReconcileError(
                f"{cluster.role}: Cilium proof is not an object",
                evidence={"command_error": command_error},
            )
        if command_error is not None or summary.get("healthy") is not True:
            details = [
                {
                    "pod": agent.get("pod_name"),
                    "node": agent.get("node_name"),
                    "ready": agent.get("ready_remote_count"),
                    "total": agent.get("remote_count"),
                    "not_ready": agent.get("not_ready_remote_names", [])[:10],
                    "missing": agent.get("missing_remote_names", [])[:10],
                    "unexpected": agent.get("unexpected_remote_names", [])[:10],
                    "duplicates": agent.get("duplicate_remote_names", [])[:10],
                }
                for agent in summary.get("agents", []) if isinstance(agent, dict)
            ]
            raise ReconcileError(
                f"{cluster.role}: all-agent Cilium identity/peer proof failed: "
                + json.dumps({"agents": details, "fatal_error": summary.get("fatal_error")}),
                evidence={"cilium_health": summary, "command_error": command_error},
            )
        return summary

    status = parse_json(
        runner(
            [
                "kubectl",
                "--kubeconfig",
                kubeconfig,
                f"--request-timeout={query_timeout_seconds}s",
                "-n",
                "kube-system",
                "exec",
                "daemonset/cilium",
                "--",
                "cilium-dbg",
                "status",
                "-o",
                "json",
            ],
            query_timeout_seconds,
        ),
        f"{cluster.role} Cilium status",
    )
    mesh = status.get("cluster-mesh") if isinstance(status, dict) else None
    remotes = mesh.get("clusters") if isinstance(mesh, dict) else None
    if not isinstance(remotes, list) or len(remotes) != expected_remote_count:
        raise ReconcileError(
            f"{cluster.role}: expected {expected_remote_count} Cilium remotes, "
            f"got {len(remotes) if isinstance(remotes, list) else 'invalid'}"
        )
    unhealthy = []
    for remote in remotes:
        if not isinstance(remote, dict):
            raise ReconcileError(
                f"{cluster.role}: malformed Cilium remote status entry"
            )
        config = remote.get("config")
        if (
            remote.get("ready") is not True
            or remote.get("connected") is not True
            or not isinstance(config, dict)
            or config.get("required") is not True
            or config.get("retrieved") is not True
        ):
            unhealthy.append(str(remote.get("name") or "unknown"))
    if unhealthy:
        raise ReconcileError(
            f"{cluster.role}: unhealthy Cilium remotes: "
            + " ".join(sorted(unhealthy))
        )
    return None


def write_pool_repair_identities(path: str, members: object, clusters: List[Cluster]) -> None:
    """Use authoritative Fleet assignments, never inferred role-number identities."""

    identities = []
    for member in members:
        identity = member.get("meshProperties", {}).get("ciliumProperties", {})
        name = identity.get("name")
        cluster_id = identity.get("id")
        if (
            not isinstance(name, str) or not name
            or not isinstance(cluster_id, int) or isinstance(cluster_id, bool)
            or cluster_id <= 0
        ):
            raise ReconcileError(f"{member.get('name')}: missing Fleet Cilium identity")
        identities.append({
            "role": member["name"], "cluster_name": name, "cluster_id": cluster_id,
        })
    if (
        len(identities) != len(clusters)
        or {item["role"] for item in identities} != {cluster.role for cluster in clusters}
        or len({item["cluster_name"] for item in identities}) != len(clusters)
        or len({item["cluster_id"] for item in identities}) != len(clusters)
    ):
        raise ReconcileError("Failed-pool repair requires an exact, unique Fleet identity inventory")
    write_json_atomic(path, identities)


def pool_configuration(pool: dict) -> dict:
    """Configuration fields a no-option pool update must preserve."""

    return {name: pool.get(name) for name in POOL_CONFIG_FIELDS}


def read_pool(cluster: Cluster, pool_name: str, runner: Runner, timeout: int) -> dict:
    payload = parse_json(
        runner(
            [
                "az", "aks", "nodepool", "show",
                "--resource-group", cluster.resource_group,
                "--cluster-name", cluster.name, "--name", pool_name,
                "--output", "json", "--only-show-errors",
            ],
            timeout,
        ),
        f"{cluster.role}/{pool_name}",
    )
    if not isinstance(payload, dict):
        raise ReconcileError(f"{cluster.role}/{pool_name}: pool response is not an object")
    expected_id = f"{cluster.resource_id}/agentPools/{pool_name}".lower()
    if str(payload.get("id") or "").lower() != expected_id or payload.get("name") != pool_name:
        raise ReconcileError(f"{cluster.role}/{pool_name}: pool resource identity mismatch")
    count = payload.get("count")
    if (
        not isinstance(count, int) or isinstance(count, bool) or count <= 0
        or payload.get("enableAutoScaling") is True
    ):
        raise ReconcileError(f"{cluster.role}/{pool_name}: pool count/autoscaling is not fixed")
    power = payload.get("powerState")
    if not isinstance(power, dict) or power.get("code") != "Running":
        raise ReconcileError(f"{cluster.role}/{pool_name}: pool is not powered Running")
    return payload


def validate_pool_workers(
    cluster: Cluster, pool: dict, kubeconfig: str, runner: Runner, timeout: int,
) -> List[str]:
    """Require exact Ready workers and a stable backing VMSS before any pool PUT."""

    payload = parse_json(
        runner(
            ["kubectl", "--kubeconfig", kubeconfig, f"--request-timeout={timeout}s",
             "get", "nodes", "-o", "json"],
            timeout,
        ),
        f"{cluster.role} worker inventory",
    )
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise ReconcileError(f"{cluster.role}: invalid worker inventory")
    nodes = [
        node for node in payload["items"]
        if (node.get("metadata", {}).get("labels", {}).get("kubernetes.azure.com/agentpool")
            or node.get("metadata", {}).get("labels", {}).get("agentpool")) == pool["name"]
        and node.get("metadata", {}).get("labels", {}).get("type") != "kwok"
    ]
    if len(nodes) != pool["count"]:
        raise ReconcileError(f"{cluster.role}/{pool['name']}: real worker count differs from desired")
    vmss_names = set()
    for node in nodes:
        metadata = node.get("metadata", {})
        spec = node.get("spec", {})
        if (
            metadata.get("deletionTimestamp")
            or spec.get("unschedulable")
            or not any(
                condition.get("type") == "Ready" and condition.get("status") == "True"
                for condition in node.get("status", {}).get("conditions", [])
            )
        ):
            raise ReconcileError(f"{cluster.role}/{pool['name']}: worker is not safely Ready")
        match = re.fullmatch(
            r"azure:///subscriptions/([^/]+)/resourceGroups/([^/]+)/providers/"
            r"Microsoft.Compute/virtualMachineScaleSets/([^/]+)/virtualMachines/([^/]+)",
            str(spec.get("providerID") or ""), re.IGNORECASE,
        )
        if (
            match is None
            or match.group(2).lower() != cluster.node_resource_group.lower()
            or not cluster.resource_id.lower().startswith(f"/subscriptions/{match.group(1).lower()}/")
        ):
            raise ReconcileError(f"{cluster.role}/{pool['name']}: worker VMSS identity mismatch")
        vmss_names.add(match.group(3))
    if len(vmss_names) != 1:
        raise ReconcileError(f"{cluster.role}/{pool['name']}: ambiguous backing VMSS")
    vmss = parse_json(
        runner(
            ["az", "vmss", "show", "--resource-group", cluster.node_resource_group,
             "--name", next(iter(vmss_names)), "--output", "json", "--only-show-errors"],
            timeout,
        ),
        f"{cluster.role}/{pool['name']} backing VMSS",
    )
    if (
        not isinstance(vmss, dict)
        or vmss.get("provisioningState") != "Succeeded"
        or vmss.get("sku", {}).get("capacity") != pool["count"]
    ):
        raise ReconcileError(f"{cluster.role}/{pool['name']}: backing VMSS is not quiescent at desired capacity")
    return [node["metadata"]["name"] for node in nodes]


def require_pool_cilium_coverage(cluster: Cluster, pool_name: str, workers: List[str], health: dict) -> None:
    covered = {
        agent.get("node_name")
        for agent in health.get("agents", [])
        if isinstance(agent, dict) and agent.get("healthy") is True
    }
    if not set(workers).issubset(covered):
        raise ReconcileError(f"{cluster.role}/{pool_name}: Cilium proof does not cover every pool worker")


def reconcile_failed_pool(
    cluster: Cluster, pool_name: str, kubeconfig: str, identity_inventory: str,
    expected_remote_count: int, runner: Runner, args: argparse.Namespace, evidence: dict,
) -> None:
    """Reassert only an unchanged, terminal-Failed pool after live health proof."""

    evidence.update({"role": cluster.role, "pool": pool_name, "status": "validating"})
    pool = read_pool(cluster, pool_name, runner, args.query_timeout_seconds)
    evidence["configuration_before"] = pool_configuration(pool)
    if pool.get("provisioningState") == "Succeeded":
        evidence["status"] = "already-succeeded"
        return
    if pool.get("provisioningState") != "Failed":
        raise ReconcileError(f"{cluster.role}/{pool_name}: pool is no longer terminal Failed")
    evidence["health_before"] = validate_cluster_data_plane(
        cluster, kubeconfig, expected_remote_count, runner, args.query_timeout_seconds,
        identity_inventory=identity_inventory,
    )
    evidence["workers_before"] = validate_pool_workers(
        cluster, pool, kubeconfig, runner, args.query_timeout_seconds,
    )
    require_pool_cilium_coverage(
        cluster, pool_name, evidence["workers_before"], evidence["health_before"],
    )
    latest = read_pool(cluster, pool_name, runner, args.query_timeout_seconds)
    if pool_configuration(latest) != pool_configuration(pool):
        raise ReconcileError(f"{cluster.role}/{pool_name}: configuration changed during health proof")
    if latest.get("provisioningState") == "Succeeded":
        evidence["status"] = "already-succeeded"
        return
    if latest.get("provisioningState") != "Failed":
        raise ReconcileError(f"{cluster.role}/{pool_name}: another operation started during health proof")
    evidence["status"] = "reconciling"
    print(f"{cluster.role}/{pool_name}: healthy workers/mesh; reasserting unchanged pool configuration", flush=True)
    runner(
        ["az", "aks", "nodepool", "update",
         "--resource-group", cluster.resource_group, "--cluster-name", cluster.name,
         "--name", pool_name, "--no-wait", "--output", "none", "--only-show-errors"],
        min(args.mutation_timeout_seconds, 180),
    )
    submitted_at = time.monotonic()
    deadline = submitted_at + args.recovery_timeout_seconds
    saw_processing = False
    evidence["observed_states"] = []
    while time.monotonic() < deadline:
        remaining = math.ceil(deadline - time.monotonic())
        current = read_pool(cluster, pool_name, runner, min(args.query_timeout_seconds, remaining))
        state = current.get("provisioningState")
        evidence["observed_states"].append(state)
        evidence["configuration_after"] = pool_configuration(current)
        if pool_configuration(current) != pool_configuration(pool):
            raise ReconcileError(f"{cluster.role}/{pool_name}: no-option update changed pool configuration")
        if state == "Succeeded":
            if time.monotonic() >= deadline:
                raise ReconcileError(f"{cluster.role}/{pool_name}: completion was observed after deadline")
            evidence["workers_after"] = validate_pool_workers(
                cluster, current, kubeconfig, runner, args.query_timeout_seconds,
            )
            evidence["health_after"] = validate_cluster_data_plane(
                cluster, kubeconfig, expected_remote_count, runner, args.query_timeout_seconds,
                identity_inventory=identity_inventory,
            )
            require_pool_cilium_coverage(
                cluster, pool_name, evidence["workers_after"], evidence["health_after"],
            )
            evidence["status"] = "repaired"
            return
        if state in BUSY_POOL_STATES:
            saw_processing = True
        elif (
            state == "Failed" and not saw_processing
            and time.monotonic() - submitted_at < 60
        ):
            # An accepted asynchronous PUT can briefly retain its old Failed state.
            pass
        else:
            raise ReconcileError(f"{cluster.role}/{pool_name}: update ended in {state}")
        time.sleep(min(args.poll_seconds, max(0, deadline - time.monotonic())))
    raise ReconcileError(f"{cluster.role}/{pool_name}: pool reconciliation deadline exhausted")


def reconcile_cluster(
    cluster: Cluster,
    runner: Runner,
    *,
    query_timeout_seconds: int,
    mutation_timeout_seconds: int,
    recovery_timeout_seconds: int,
    poll_seconds: int,
    submit_attempts: int,
) -> ReconcileResult:
    """Submit one no-op AKS update and wait for stable Succeeded."""

    result = ReconcileResult(name=cluster.name, role=cluster.role)
    args = [
        "az",
        "aks",
        "update",
        "--resource-group",
        cluster.resource_group,
        "--name",
        cluster.name,
        "--yes",
        "--no-wait",
        "--output",
        "none",
        "--only-show-errors",
    ]
    try:
        for attempt in range(1, submit_attempts + 1):
            result.attempts = attempt
            try:
                runner(args, mutation_timeout_seconds)
                break
            except ReconcileError as exc:
                if (
                    attempt >= submit_attempts
                    or TRANSIENT_UPDATE_RE.search(str(exc)) is None
                ):
                    raise
                time.sleep(poll_seconds)

        deadline = time.monotonic() + recovery_timeout_seconds
        while time.monotonic() < deadline:
            current = parse_json(
                runner(
                    [
                        "az",
                        "aks",
                        "show",
                        "--resource-group",
                        cluster.resource_group,
                        "--name",
                        cluster.name,
                        "--query",
                        "{state:provisioningState,power:powerState.code}",
                        "--output",
                        "json",
                        "--only-show-errors",
                    ],
                    query_timeout_seconds,
                ),
                f"{cluster.role} AKS state",
            )
            if not isinstance(current, dict):
                raise ReconcileError(
                    f"{cluster.role}: malformed AKS state response"
                )
            state = str(current.get("state") or "Unknown")
            power = str(current.get("power") or "")
            if not result.observed_states or result.observed_states[-1] != state:
                result.observed_states.append(state)
            if state == "Succeeded" and power in ("", "Running"):
                result.status = "repaired"
                return result
            time.sleep(poll_seconds)
        raise ReconcileError(
            f"{cluster.role}: AKS state did not reach Succeeded within "
            f"{recovery_timeout_seconds}s"
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


def read_quiescent_inventory(
    args: argparse.Namespace,
    summary: Dict[str, object],
    phase: str,
    runner: Runner,
) -> Tuple[List[Cluster], List[Cluster]]:
    """Observe known in-flight operations without mutating or resetting budgets."""

    deadline = time.monotonic() + args.quiescence_timeout_seconds
    observations = []
    summary[f"{phase}_quiescence_observations"] = observations
    read_failures = 0
    command = [
        "az", "aks", "list", "--resource-group", args.resource_group,
        "--output", "json", "--only-show-errors",
    ]
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ReconcileError(
                f"{phase} inventory did not become safely quiescent within "
                f"{args.quiescence_timeout_seconds}s"
            )
        try:
            payload = parse_json(
                runner(command, min(args.inventory_timeout_seconds, math.ceil(remaining))),
                f"{phase} AKS inventory",
            )
        except ReconcileError as error:
            read_failures += 1
            if (
                read_failures >= args.inventory_attempts
                or TRANSIENT_READ_RE.search(str(error)) is None
            ):
                raise
            print(f"{phase} inventory read retry: {error}", file=sys.stderr, flush=True)
            pause = args.inventory_retry_seconds
        else:
            read_failures = 0
            summary[f"{phase}_pool_states"] = [
                {
                    "name": row.get("name"),
                    "role": (row.get("tags") or {}).get("role"),
                    "provisioning_state": row.get("provisioningState"),
                    "power_state": row.get("powerState"),
                    "pools": row.get("agentPoolProfiles"),
                }
                for row in payload if isinstance(row, dict)
            ] if isinstance(payload, list) else []
            write_json_atomic(args.summary_file, summary)
            try:
                result = validate_cluster_inventory(
                    payload, expected_count=args.expected_count,
                    region=args.expected_region,
                    max_repair_clusters=args.max_repair_clusters,
                    allow_failed_pool_repair=(
                        phase == "initial" and args.failed_pool_repair_enabled
                    ),
                )
            except InventoryBusyError as error:
                observations.append({"observed_at": utc_now(), "reason": str(error)})
                write_json_atomic(args.summary_file, summary)
                print(f"Waiting for {phase} inventory quiescence: {error}", flush=True)
                pause = args.poll_seconds
            else:
                if time.monotonic() >= deadline:
                    raise ReconcileError(
                        f"{phase} inventory observation completed after its deadline"
                    )
                return result
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(pause, remaining))


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resource-group", required=True)
    parser.add_argument("--expected-subscription", required=True)
    parser.add_argument("--expected-region", required=True)
    parser.add_argument("--expected-count", type=int, required=True)
    parser.add_argument("--expected-tfvars-sha", required=True)
    parser.add_argument("--summary-file", required=True)
    parser.add_argument("--fleet-name", default="clustermesh-flt")
    parser.add_argument("--profile-name", default="clustermesh-cmp")
    parser.add_argument("--max-repair-clusters", type=int, default=10)
    parser.add_argument("--max-concurrent", type=int, default=2)
    parser.add_argument("--query-timeout-seconds", type=int, default=180)
    parser.add_argument("--inventory-timeout-seconds", type=int, default=600)
    parser.add_argument("--inventory-attempts", type=int, default=3)
    parser.add_argument("--inventory-retry-seconds", type=int, default=15)
    parser.add_argument("--quiescence-timeout-seconds", type=int, default=900)
    parser.add_argument("--failed-pool-repair-enabled", action="store_true")
    parser.add_argument("--mutation-timeout-seconds", type=int, default=1800)
    parser.add_argument("--recovery-timeout-seconds", type=int, default=1800)
    parser.add_argument("--poll-seconds", type=int, default=30)
    parser.add_argument("--submit-attempts", type=int, default=10)
    args = parser.parse_args(argv)
    for name in (
        "expected_count",
        "max_repair_clusters",
        "max_concurrent",
        "query_timeout_seconds",
        "inventory_timeout_seconds",
        "inventory_attempts",
        "inventory_retry_seconds",
        "quiescence_timeout_seconds",
        "mutation_timeout_seconds",
        "recovery_timeout_seconds",
        "poll_seconds",
        "submit_attempts",
    ):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Validate, reconcile known stale states, and verify the full fleet."""

    args = parse_args(argv)
    started_at = utc_now()
    summary: Dict[str, object] = {
        "schema_version": 1,
        "started_at": started_at,
        "resource_group": args.resource_group,
    }
    try:
        actual_subscription = run_command(
            ["az", "account", "show", "--query", "id", "-o", "tsv"],
            args.query_timeout_seconds,
        ).strip()
        if actual_subscription.lower() != args.expected_subscription.lower():
            raise ReconcileError(
                f"expected subscription {args.expected_subscription}, got "
                f"{actual_subscription}"
            )

        resource_group = parse_json(
            run_command(
                [
                    "az",
                    "group",
                    "show",
                    "--name",
                    args.resource_group,
                    "--output",
                    "json",
                    "--only-show-errors",
                ],
                args.query_timeout_seconds,
            ),
            "resource-group",
        )
        validate_resource_group(
            resource_group,
            args.resource_group,
            args.expected_region,
            args.expected_count,
            args.expected_tfvars_sha,
        )

        clusters, failed = read_quiescent_inventory(
            args, summary, "initial", run_command,
        )
        summary["initial_failed_roles"] = [cluster.role for cluster in failed]
        summary["initial_failed_pools"] = [
            {"role": cluster.role, "pool": pool}
            for cluster in clusters for pool in cluster.failed_pools
        ]

        fleet_members = parse_json(
            run_read_with_retries(
                [
                    "az",
                    "fleet",
                    "clustermeshprofile",
                    "list-members",
                    "--resource-group",
                    args.resource_group,
                    "--fleet-name",
                    args.fleet_name,
                    "--name",
                    args.profile_name,
                    "--output",
                    "json",
                    "--only-show-errors",
                ],
                run_command,
                timeout_seconds=args.inventory_timeout_seconds,
                attempts=args.inventory_attempts,
                retry_seconds=args.inventory_retry_seconds,
            ),
            "Fleet member",
        )
        validate_fleet_members(fleet_members, clusters)

        failure_evidence: Dict[str, dict] = {}
        with tempfile.TemporaryDirectory(prefix="aks-arm-reconcile-") as temp_dir:
            for cluster in failed:
                exists = run_command(
                    [
                        "az",
                        "group",
                        "exists",
                        "--name",
                        cluster.node_resource_group,
                        "--only-show-errors",
                    ],
                    args.query_timeout_seconds,
                ).strip()
                if exists.lower() != "true":
                    raise ReconcileError(
                        f"{cluster.role}: node resource group is missing"
                    )
                latest_operation = parse_json(
                    run_command(
                        [
                            "az",
                            "aks",
                            "operation",
                            "show-latest",
                            "--resource-group",
                            cluster.resource_group,
                            "--name",
                            cluster.name,
                            "--output",
                            "json",
                            "--only-show-errors",
                        ],
                        args.query_timeout_seconds,
                    ),
                    f"{cluster.role} latest AKS operation",
                )
                failure_evidence[cluster.role] = validate_latest_operation(
                    cluster, latest_operation
                )
                validate_cluster_data_plane(
                    cluster,
                    os.path.join(temp_dir, f"{cluster.role}.config"),
                    args.expected_count - 1,
                    run_command,
                    args.query_timeout_seconds,
                )

        summary["failure_evidence"] = failure_evidence
        if failed:
            def reconcile_one(cluster: Cluster) -> ReconcileResult:
                return reconcile_cluster(
                    cluster,
                    run_command,
                    query_timeout_seconds=args.query_timeout_seconds,
                    mutation_timeout_seconds=args.mutation_timeout_seconds,
                    recovery_timeout_seconds=args.recovery_timeout_seconds,
                    poll_seconds=args.poll_seconds,
                    submit_attempts=args.submit_attempts,
                )

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=args.max_concurrent
            ) as executor:
                results = list(executor.map(reconcile_one, failed))
        else:
            results = []
        summary["repairs"] = [asdict(result) for result in results]
        failures = [result for result in results if result.status != "repaired"]
        if failures:
            raise ReconcileError(
                "one or more AKS ARM state reconciles failed: "
                + "; ".join(
                    f"{result.role}={result.error}" for result in failures
                )
            )

        summary["pool_repairs"] = []
        if any(cluster.failed_pools for cluster in clusters):
            identity_members = parse_json(
                run_read_with_retries(
                    ["az", "fleet", "member", "list",
                     "--resource-group", args.resource_group,
                     "--fleet-name", args.fleet_name,
                     "--output", "json", "--only-show-errors"],
                    run_command, timeout_seconds=args.inventory_timeout_seconds,
                    attempts=args.inventory_attempts,
                    retry_seconds=args.inventory_retry_seconds,
                ),
                "Fleet identity assignments",
            )
            validate_fleet_members(identity_members, clusters)
            with tempfile.TemporaryDirectory(prefix="aks-pool-reconcile-") as temp_dir:
                identities = os.path.join(temp_dir, "cilium-identities.json")
                write_pool_repair_identities(identities, identity_members, clusters)
                pool_failures = []
                for cluster in clusters:
                    for pool_name in cluster.failed_pools:
                        evidence = {}
                        summary["pool_repairs"].append(evidence)
                        try:
                            reconcile_failed_pool(
                                cluster, pool_name,
                                os.path.join(temp_dir, f"{cluster.role}.config"),
                                identities, args.expected_count - 1,
                                run_command, args, evidence,
                            )
                        except ReconcileError as error:
                            mutation_started = evidence.get("status") == "reconciling"
                            evidence.update({
                                "status": "failed", "error": str(error),
                                "failure_evidence": error.evidence,
                                "mutation_started": mutation_started,
                            })
                            pool_failures.append(f"{cluster.role}/{pool_name}")
                            print(
                                f"Pool repair blocked: {error}",
                                file=sys.stderr, flush=True,
                            )
                            if mutation_started:
                                raise
                        finally:
                            write_json_atomic(args.summary_file, summary)
                if pool_failures:
                    raise ReconcileError(
                        "Guarded pool repair did not complete: " + ", ".join(pool_failures)
                    )

        final_clusters, final_failed = read_quiescent_inventory(
            args, summary, "final", run_command,
        )
        if final_failed:
            raise ReconcileError(
                "AKS ARM states remain Failed after reconciliation: "
                + " ".join(cluster.role for cluster in final_failed)
            )
        final_fleet = parse_json(
            run_read_with_retries(
                [
                    "az",
                    "fleet",
                    "clustermeshprofile",
                    "list-members",
                    "--resource-group",
                    args.resource_group,
                    "--fleet-name",
                    args.fleet_name,
                    "--name",
                    args.profile_name,
                    "--output",
                    "json",
                    "--only-show-errors",
                ],
                run_command,
                timeout_seconds=args.inventory_timeout_seconds,
                attempts=args.inventory_attempts,
                retry_seconds=args.inventory_retry_seconds,
            ),
            "final Fleet member",
        )
        validate_fleet_members(final_fleet, final_clusters)
        summary.update(
            {
                "healthy": True,
                "finished_at": utc_now(),
                "cluster_count": len(final_clusters),
                "repaired_cluster_count": len(failed),
            }
        )
        write_json_atomic(args.summary_file, summary)
        print(
            f"Preserved AKS ARM reconciliation complete: "
            f"{len(failed)} repaired, {len(final_clusters)} healthy."
        )
        return 0
    except ReconcileError as exc:
        summary.update(
            {
                "healthy": False,
                "finished_at": utc_now(),
                "fatal_error": str(exc),
            }
        )
        write_json_atomic(args.summary_file, summary)
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
