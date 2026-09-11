#!/usr/bin/env python3
"""Reconcile preserved AKS ARM failures only after authoritative live health proof."""

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
from datetime import datetime, timedelta, timezone
from pathlib import Path
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
LIVE_OVERLAY_CLUSTER_COUNT = 100
MAX_LIVE_OVERLAY_REPAIR_ROLES = 20
LIVE_OVERLAY_CLEANUP_SECONDS = 300
POOL_CONFIG_FIELDS = (
    "count", "vmSize", "mode", "osType", "osSKU", "osSku", "osDiskType", "osDiskSizeGb",
    "maxPods", "vnetSubnetId", "podSubnetId", "availabilityZones",
    "enableAutoScaling", "minCount", "maxCount", "nodeLabels", "nodeTaints",
    "orchestratorVersion", "kubeletConfig", "linuxOSConfig",
    "upgradeSettings",
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
    expected_subscription: Optional[str] = None,
    expected_resource_group: Optional[str] = None,
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
        tags = row.get("tags")
        if not isinstance(tags, dict):
            raise ReconcileError("AKS inventory entry has no valid ownership tags")
        role = tags.get("role")
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
        if expected_resource_group is not None:
            expected_id = (
                f"/subscriptions/{expected_subscription}/resourceGroups/"
                f"{expected_resource_group}/providers/"
                f"Microsoft.ContainerService/managedClusters/{name}"
            )
            if (
                resource_group.lower() != expected_resource_group.lower()
                or resource_id.lower() != expected_id.lower()
                or tags.get("run_id") != expected_resource_group
            ):
                raise ReconcileError(f"{role}: preserved AKS ownership identity mismatch")
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


class FleetHealthError(ReconcileError):
    """An exact readable Fleet inventory has members that are not Connected."""


def validate_fleet_members(payload: object, clusters: List[Cluster]) -> None:
    """Require exact, fully Connected Fleet membership."""

    if (
        not isinstance(payload, list)
        or len(payload) != len(clusters)
        or any(not isinstance(row, dict) for row in payload)
    ):
        raise ReconcileError("Fleet member response is not an exact inventory")
    expected_roles = {cluster.role for cluster in clusters}
    actual_roles = {
        str(row.get("name"))
        for row in payload
        if isinstance(row, dict) and row.get("name") is not None
    }
    if actual_roles != expected_roles:
        raise ReconcileError("Fleet member names do not match AKS mesh roles")
    unhealthy = []
    for row in payload:
        mesh = row.get("meshProperties")
        status = mesh.get("status") if isinstance(mesh, dict) else None
        if not isinstance(status, dict) or not isinstance(status.get("state"), str):
            raise ReconcileError(
                f"{row['name']}: Fleet member health is unreadable",
                evidence={"fleet_member": row},
            )
        if status["state"] != "Connected":
            unhealthy.append(row)
    if unhealthy:
        raise FleetHealthError(
            "refusing AKS ARM repair while Fleet members are not Connected: "
            + " ".join(sorted(row["name"] for row in unhealthy)),
            evidence={"unhealthy_fleet_members": unhealthy},
        )


def read_connected_fleet_members(
    args: argparse.Namespace, clusters: List[Cluster], summary: dict, runner: Runner,
    *, phase: str = "initial",
) -> object:
    """Observe transient PartialConnectivity without ever accepting it as healthy."""

    deadline = time.monotonic() + args.quiescence_timeout_seconds
    command = [
        "az", "fleet", "clustermeshprofile", "list-members",
        "--resource-group", args.resource_group, "--fleet-name", args.fleet_name,
        "--name", args.profile_name, "--output", "json", "--only-show-errors",
    ]

    def bounded_read(arguments, timeout):
        remaining = math.ceil(deadline - time.monotonic())
        if remaining <= 0:
            raise ReconcileError("Fleet health observation deadline expired")
        return runner(arguments, min(timeout, remaining))

    observations = summary.setdefault(f"{phase}_fleet_health_observations", [])
    for attempt in range(1, args.inventory_attempts + 1):
        members = parse_json(
            run_read_with_retries(
                command, bounded_read,
                timeout_seconds=args.inventory_timeout_seconds,
                attempts=args.inventory_attempts,
                retry_seconds=args.inventory_retry_seconds,
            ),
            "Fleet member",
        )
        summary[f"{phase}_fleet_members"] = members
        write_json_atomic(args.summary_file, summary)
        try:
            validate_fleet_members(members, clusters)
            return members
        except FleetHealthError as error:
            unhealthy = error.evidence["unhealthy_fleet_members"]
            observations.append({
                "attempt": attempt, "observed_at": utc_now(),
                "unhealthy_members": unhealthy,
            })
            write_json_atomic(args.summary_file, summary)
            partial_connectivity = all(
                row.get("provisioningState") == "Succeeded"
                and isinstance(row.get("labels"), dict)
                and row["labels"].get("mesh") == "true"
                and isinstance(row["meshProperties"]["status"].get("error"), dict)
                and row["meshProperties"]["status"]["error"].get("code") == "PartialConnectivity"
                for row in unhealthy
            )
            remaining = deadline - time.monotonic()
            if not partial_connectivity or attempt == args.inventory_attempts or remaining <= 0:
                raise
            print(
                "Fleet PartialConnectivity observation; waiting read-only for Connected: "
                + ",".join(row["name"] for row in unhealthy),
                flush=True,
            )
            time.sleep(min(args.inventory_retry_seconds, remaining))
    raise ReconcileError("Fleet health observation did not run")


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


def read_terminal_cluster_operation(cluster: Cluster, runner: Runner, timeout: int) -> dict:
    """Fail closed on an unreadable or still-active latest provider operation."""

    operation = parse_json(
        runner(
            ["az", "aks", "operation", "show-latest", "--resource-group", cluster.resource_group,
             "--name", cluster.name, "--output", "json", "--only-show-errors"],
            timeout,
        ),
        f"{cluster.role} latest AKS operation",
    )
    if not isinstance(operation, dict) or operation.get("status") not in ("Succeeded", "Failed"):
        raise ReconcileError(
            f"{cluster.role}: latest AKS provider operation is not safely terminal",
            evidence={"latest_operation": operation},
        )
    return operation


def read_failed_cluster_operation(cluster: Cluster, runner: Runner, timeout: int) -> dict:
    """Recheck the original stale-addon gate, including after an overlay recovery."""

    exists = runner(
        ["az", "group", "exists", "--name", cluster.node_resource_group, "--only-show-errors"], timeout,
    ).strip()
    if exists.lower() != "true":
        raise ReconcileError(f"{cluster.role}: node resource group is missing")
    return validate_latest_operation(cluster, read_terminal_cluster_operation(cluster, runner, timeout))


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


def write_pool_repair_identities(path: str, members: object, clusters: List[Cluster]) -> List[dict]:
    """Use authoritative Fleet assignments, never inferred role-number identities."""

    if not isinstance(members, list) or any(not isinstance(member, dict) for member in members):
        raise ReconcileError("Fleet Cilium identity inventory is not an array of members")
    identities = []
    for member in members:
        mesh = member.get("meshProperties")
        identity = mesh.get("ciliumProperties") if isinstance(mesh, dict) else None
        if not isinstance(identity, dict):
            raise ReconcileError(f"{member.get('name')}: missing Fleet Cilium identity")
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
    identities.sort(key=lambda item: item["role"])
    write_json_atomic(path, identities)
    return identities


def pool_configuration(pool: dict) -> dict:
    """Configuration fields a no-option pool update must preserve."""

    return {name: pool.get(name) for name in POOL_CONFIG_FIELDS}


def pool_configuration_matches(before: dict, current: dict) -> bool:
    """Allow only the existing upgrade surge while busy; require exact final count."""

    expected = pool_configuration(before)
    observed = pool_configuration(current)
    if expected == observed:
        return True
    if current.get("provisioningState") not in ("Updating", "Upgrading"):
        return False
    expected_count = expected.pop("count")
    observed_count = observed.pop("count")
    if expected != observed or not isinstance(observed_count, int) or isinstance(observed_count, bool):
        return False
    settings = before.get("upgradeSettings") or {}
    surge = settings.get("maxSurge")
    if surge is None:
        maximum_extra = 1
    elif isinstance(surge, int) and not isinstance(surge, bool) and surge >= 0:
        maximum_extra = surge
    elif isinstance(surge, str) and re.fullmatch(r"\d+%?", surge):
        if surge.endswith("%"):
            percentage = int(surge[:-1])
            if percentage > 100:
                return False
            maximum_extra = math.ceil(expected_count * percentage / 100)
        else:
            maximum_extra = int(surge)
    else:
        return False
    return expected_count <= observed_count <= expected_count + maximum_extra


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
    print(
        f"{cluster.role}/{pool_name}: healthy workers/mesh; submitting unchanged "
        "pool update (may resume an upgrade and drain workers)",
        flush=True,
    )
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
        if not pool_configuration_matches(pool, current):
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
                    "role": row["tags"].get("role") if isinstance(row.get("tags"), dict) else None,
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
                        phase in (
                            "initial", "live_overlay_before_repair",
                            "live_overlay_after_repair", "live_overlay_after_probe",
                        ) and args.failed_pool_repair_enabled
                    ),
                    expected_subscription=(
                        args.expected_subscription if args.live_overlay_repair_enabled else None
                    ),
                    expected_resource_group=(
                        args.resource_group if args.live_overlay_repair_enabled else None
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


def read_live_overlay_authority(
    clusters: List[Cluster], args: argparse.Namespace, runner: Runner,
    evidence: dict, identities_path: str, lease_required_until: datetime,
    repair_roles: Sequence[str] = (),
) -> List[dict]:
    """Read the preserved ownership, leases and idle Fleet authority without writes."""

    if args.expected_count != LIVE_OVERLAY_CLUSTER_COUNT or len(clusters) != args.expected_count:
        raise ReconcileError("Early live-overlay recovery requires the full exact 100-cluster inventory")
    if not re.fullmatch(r"[0-9]+-[0-9a-f]{8}", args.resource_group):
        raise ReconcileError("Invalid preserved RUN_ID; expected <build-id>-<8 hex>")
    subscription = runner(
        ["az", "account", "show", "--query", "id", "-o", "tsv"], args.query_timeout_seconds,
    ).strip()
    if subscription.lower() != args.expected_subscription.lower():
        raise ReconcileError("Subscription changed before live-overlay recovery")
    prefix = f"/subscriptions/{args.expected_subscription}/resourceGroups/{args.resource_group}"
    resource_group = parse_json(
        runner(
            ["az", "group", "show", "--name", args.resource_group, "--output", "json", "--only-show-errors"],
            args.query_timeout_seconds,
        ),
        "preserved resource group",
    )
    evidence["resource_group"] = resource_group
    validate_resource_group(
        resource_group, args.resource_group, args.expected_region,
        args.expected_count, args.expected_tfvars_sha,
    )
    if str(resource_group.get("id") or "").lower() != prefix.lower():
        raise ReconcileError("Preserved resource-group resource identity mismatch")

    def lease_expiry(value: object, description: str) -> datetime:
        try:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except (TypeError, ValueError) as error:
            raise ReconcileError(f"{description}: invalid deletion_due_time") from error

    parent_expiry = lease_expiry(resource_group["tags"].get("deletion_due_time"), args.resource_group)
    if parent_expiry <= lease_required_until:
        raise ReconcileError("Preserved parent lease does not cover the bounded live-overlay recovery")
    expected_groups = {cluster.node_resource_group.lower(): cluster for cluster in clusters}
    if len(expected_groups) != args.expected_count:
        raise ReconcileError("Preserved inventory has duplicate managed resource groups")
    groups = parse_json(
        runner(
            ["az", "group", "list", "--query",
             "[].{name:name,location:location,managedBy:managedBy,deletion_due_time:tags.deletion_due_time}",
             "--output", "json", "--only-show-errors"],
            args.inventory_timeout_seconds,
        ),
        "managed resource-group inventory",
    )
    if not isinstance(groups, list) or any(not isinstance(group, dict) for group in groups):
        raise ReconcileError("Managed resource-group inventory is not readable")
    selected_groups = [
        group for group in groups if str(group.get("name") or "").lower() in expected_groups
    ]
    evidence["node_resource_groups"] = selected_groups
    if (
        len(selected_groups) != args.expected_count
        or len({str(group["name"]).lower() for group in selected_groups}) != args.expected_count
    ):
        raise ReconcileError("Managed resource-group inventory is not exact; refusing Fleet mutation")
    for group in selected_groups:
        cluster = expected_groups[group["name"].lower()]
        expected_id = f"{prefix}/providers/Microsoft.ContainerService/managedClusters/{cluster.name}"
        if (
            cluster.resource_group.lower() != args.resource_group.lower()
            or cluster.resource_id.lower() != expected_id.lower()
            or str(group.get("managedBy") or "").lower() != cluster.resource_id.lower()
            or str(group.get("location") or "").lower() != args.expected_region.lower()
        ):
            raise ReconcileError(f"{cluster.role}: managed resource-group ownership identity mismatch")
        if lease_expiry(group.get("deletion_due_time"), group["name"]) < parent_expiry:
            raise ReconcileError(f"{cluster.role}: managed resource-group lease is shorter than the parent lease")

    evidence["latest_operations"] = {}
    for cluster in clusters:
        if cluster.failed_pools or cluster.state == "Failed" or cluster.role in repair_roles:
            operation = read_terminal_cluster_operation(cluster, runner, args.query_timeout_seconds)
            evidence["latest_operations"][cluster.role] = operation
            if cluster.state == "Failed":
                validate_latest_operation(cluster, operation)

    fleet_id = f"{prefix}/providers/Microsoft.ContainerService/fleets/{args.fleet_name}"
    for kind, command, expected_id in (
        ("fleet", ["az", "fleet", "show", "--name", args.fleet_name], fleet_id),
        ("profile", ["az", "fleet", "clustermeshprofile", "show", "--fleet-name", args.fleet_name,
                     "--name", args.profile_name], f"{fleet_id}/clusterMeshProfiles/{args.profile_name}"),
    ):
        payload = parse_json(
            runner(
                command + ["--resource-group", args.resource_group, "--output", "json", "--only-show-errors"],
                args.query_timeout_seconds,
            ),
            kind,
        )
        evidence[kind] = payload
        properties = payload.get("properties") if isinstance(payload, dict) else None
        state = (
            payload.get("provisioningState") or (properties or {}).get("provisioningState")
        ) if isinstance(payload, dict) and (properties is None or isinstance(properties, dict)) else None
        if (
            not isinstance(payload, dict)
            or str(payload.get("id") or "").lower() != expected_id.lower()
            or state != "Succeeded"
        ):
            raise ReconcileError(f"Existing Fleet {kind} is not the expected idle Succeeded resource")

    for kind, command in (
        ("members", ["az", "fleet", "member", "list"]),
        ("applied_members", ["az", "fleet", "clustermeshprofile", "list-members", "--name", args.profile_name]),
    ):
        members = parse_json(
            runner(
                command + ["--resource-group", args.resource_group, "--fleet-name", args.fleet_name,
                           "--output", "json", "--only-show-errors"],
                args.inventory_timeout_seconds,
            ),
            f"Fleet {kind}",
        )
        evidence[kind] = members
        validate_fleet_members(members, clusters)
    by_role = {cluster.role: cluster for cluster in clusters}
    for member in evidence["members"]:
        if (
            str(member.get("clusterResourceId") or "").lower() != by_role[member["name"]].resource_id.lower()
            or member.get("provisioningState") != "Succeeded"
            or not isinstance(member.get("labels"), dict)
            or member["labels"].get("mesh") != "true"
        ):
            raise ReconcileError(f"{member['name']}: Fleet ownership, selector or operation state is unsafe")
    return write_pool_repair_identities(identities_path, evidence["members"], clusters)


def run_live_overlay_command(
    command: Sequence[str], timeout_seconds: int, log_path: str,
    environment: Optional[Dict[str, str]] = None,
) -> int:
    """Keep complete child output, including failures, outside credential tempdirs."""

    with open(log_path, "w", encoding="utf-8") as log:
        try:
            # TERM permits the existing Fleet script's selector-restoration trap;
            # a timed-out submission is still a failed, possibly active operation.
            completed = subprocess.run(
                ["timeout", "--signal=TERM", f"--kill-after={LIVE_OVERLAY_CLEANUP_SECONDS}s",
                 f"{timeout_seconds}s", *command],
                check=False, stdout=log, stderr=subprocess.STDOUT, text=True, env=environment,
            )
        except OSError as error:
            log.write(f"Unable to execute live-overlay command: {error}\n")
            raise ReconcileError(f"Unable to execute live-overlay command; see {log_path}: {error}") from error
    return completed.returncode


def validate_live_overlay_proof(
    proof: object, returncode: int, roles_path: str, identities: List[dict], max_roles: int,
) -> None:
    """Reject partial/read-failed probes even if they emitted a bounded repair plan."""

    if not isinstance(proof, dict) or returncode not in (0, 2):
        raise ReconcileError(f"Live-overlay probe failed unsafely (exit={returncode})")
    observed = proof.get("identities")
    if (
        proof.get("cluster_count") != LIVE_OVERLAY_CLUSTER_COUNT
        or not isinstance(observed, list) or len(observed) != len(identities)
        or any(
            not isinstance(item, dict)
            or not isinstance(item.get("cluster_id"), int) or isinstance(item["cluster_id"], bool)
            for item in observed
        )
        or sorted(observed, key=lambda item: str(item.get("role"))) != sorted(identities, key=lambda item: item["role"])
    ):
        raise ReconcileError("Live-overlay probe does not match the exact authoritative Fleet identities")
    drift = proof.get("drift")
    roles = proof.get("repair_roles")
    if (
        not isinstance(drift, list)
        or any(not isinstance(item, dict) or item.get("command_error") for item in drift)
    ):
        raise ReconcileError("Live-overlay agent probe is unreadable; refusing Fleet mutation")
    try:
        with open(roles_path, encoding="utf-8") as handle:
            written_roles = handle.read().splitlines()
    except OSError as error:
        raise ReconcileError(f"Live-overlay repair roles are unavailable: {error}") from error
    if not isinstance(roles, list) or roles != written_roles:
        raise ReconcileError("Live-overlay repair roles do not match the current proof")
    if returncode == 0:
        if proof.get("healthy") is not True or drift or roles:
            raise ReconcileError("Live-overlay success proof is inconsistent")
        return
    selection = proof.get("repair_selection")
    bounded_selection = (
        isinstance(selection, dict)
        and selection.get("cover_within_limit") is True
        and selection.get("repair_roles") == roles
        and 0 < len(roles) <= max_roles
    )
    owned_roles = len(set(roles)) == len(roles) and set(roles).issubset({item["role"] for item in identities})
    if (
        proof.get("healthy") is not False or not drift or not bounded_selection or not owned_roles
    ):
        raise ReconcileError("Live-overlay drift has no safe bounded repair plan")


def recover_live_overlay(
    clusters: List[Cluster], args: argparse.Namespace, summary: dict, runner: Runner,
) -> Tuple[List[Cluster], List[Cluster]]:
    """Run the established full-probe/rejoin/postproof sequence once, before pool PUTs."""

    artifact_parent = os.path.dirname(os.path.abspath(args.summary_file))
    os.makedirs(artifact_parent, exist_ok=True)
    artifact_dir = tempfile.mkdtemp(prefix="live-overlay-", dir=artifact_parent)
    evidence = {"status": "validating", "directory": artifact_dir}
    summary["live_overlay_recovery"] = evidence
    deadline = time.monotonic() + args.live_overlay_timeout_seconds
    lease_required_until = datetime.now(timezone.utc) + timedelta(
        seconds=args.live_overlay_timeout_seconds + LIVE_OVERLAY_CLEANUP_SECONDS,
    )
    evidence["lease_required_until"] = lease_required_until.isoformat()

    def remaining() -> int:
        seconds = math.ceil(deadline - time.monotonic())
        if seconds <= 0:
            raise ReconcileError("Early live-overlay recovery deadline exhausted")
        return seconds

    def bounded_read(command: Sequence[str], timeout: int) -> str:
        return runner(command, min(timeout, remaining()))

    def authority(
        phase: str, current_clusters: List[Cluster], repair_roles: Sequence[str] = (),
    ) -> List[dict]:
        evidence[phase] = {}
        write_json_atomic(args.summary_file, summary)
        return read_live_overlay_authority(
            current_clusters, args, bounded_read, evidence[phase],
            os.path.join(artifact_dir, f"{phase}-identities.json"), lease_required_until, repair_roles,
        )

    def probe(phase: str, attempts: int, identities: List[dict]) -> int:
        record = {
            "summary_file": os.path.join(artifact_dir, f"{phase}.json"),
            "roles_file": os.path.join(artifact_dir, f"{phase}-roles.txt"),
            "log_file": os.path.join(artifact_dir, f"{phase}.log"),
        }
        evidence[phase] = record
        write_json_atomic(args.summary_file, summary)
        record["exit_code"] = run_live_overlay_command(
            [sys.executable, os.path.join(os.path.dirname(__file__), "preserved_live_overlay.py"),
             "--clusters", os.path.join(artifact_dir, "clusters.json"),
             "--summary-file", record["summary_file"], "--repair-roles-file", record["roles_file"],
             "--attempts", str(attempts), "--retry-seconds", "30",
             "--command-timeout-seconds", "30", "--max-concurrent", "10",
             "--max-repair-roles", str(args.live_overlay_max_repair_roles)],
            remaining(), record["log_file"],
        )
        try:
            with open(record["summary_file"], encoding="utf-8") as handle:
                record["proof"] = json.load(handle)
        except (OSError, json.JSONDecodeError) as error:
            raise ReconcileError(f"Live-overlay {phase} proof is unavailable: {error}") from error
        validate_live_overlay_proof(
            record["proof"], record["exit_code"], record["roles_file"],
            identities, args.live_overlay_max_repair_roles,
        )
        return record["exit_code"]

    try:
        print("Early live-overlay recovery: validating full preserved n100 authority.", flush=True)
        identities = authority("authority_before", clusters)
        with tempfile.TemporaryDirectory(prefix="aks-overlay-credentials-") as credential_dir:
            inventory = [
                {"name": cluster.name, "rg": cluster.resource_group, "role": cluster.role,
                 "resource_id": cluster.resource_id,
                 "kubeconfig": os.path.join(credential_dir, f"{cluster.role}.config")}
                for cluster in clusters
            ]
            write_json_atomic(os.path.join(artifact_dir, "clusters.json"), inventory)
            evidence["status"] = "reading-credentials"
            write_json_atomic(args.summary_file, summary)
            print("Early live-overlay recovery: reading 100 private kubeconfigs.", flush=True)

            def credentials(row: dict) -> None:
                bounded_read(
                    ["az", "aks", "get-credentials", "--resource-group", row["rg"],
                     "--name", row["name"], "--file", row["kubeconfig"],
                     "--subscription", args.expected_subscription,
                     "--overwrite-existing", "--only-show-errors"],
                    args.query_timeout_seconds,
                )
                if not os.path.isfile(row["kubeconfig"]) or os.path.getsize(row["kubeconfig"]) == 0:
                    raise ReconcileError(f"{row['role']}: credentials are unavailable")

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                list(executor.map(credentials, inventory))
            evidence["status"] = "probing"
            print("Early live-overlay recovery: probing every cluster and Cilium agent.", flush=True)
            if probe("initial", 5, identities) == 0:
                current, failed = read_quiescent_inventory(args, summary, "live_overlay_after_probe", bounded_read)
                if authority("authority_after_probe", current) != identities:
                    raise ReconcileError("Fleet Cilium identities changed during the healthy live-overlay proof")
                evidence["status"] = "healthy"
                print("Early live-overlay recovery: full peer proof healthy; no Fleet mutation.", flush=True)
                return current, failed

            current, _ = read_quiescent_inventory(args, summary, "live_overlay_before_repair", bounded_read)
            repair_roles = evidence["initial"]["proof"]["repair_roles"]
            if authority("authority_before_repair", current, repair_roles) != identities:
                raise ReconcileError("Fleet Cilium identities changed during the live-overlay proof")
            repair = {"attempts": 1, "log_file": os.path.join(artifact_dir, "fleet-repair.log")}
            evidence["fleet_repair"] = repair
            evidence["status"] = "repairing"
            write_json_atomic(args.summary_file, summary)
            print(
                "Early live-overlay recovery: one bounded Fleet rejoin for "
                + ",".join(repair_roles),
                flush=True,
            )
            environment = os.environ.copy()
            environment.update({
                "CLUSTERMESH_DEBUG_TARGET_RUN_ID": args.resource_group,
                "CLUSTERMESH_DEBUG_EXPECTED_CLUSTER_COUNT": str(LIVE_OVERLAY_CLUSTER_COUNT),
                "CLUSTERMESH_DEBUG_FLEET_NAME": args.fleet_name,
                "CLUSTERMESH_DEBUG_PROFILE_NAME": args.profile_name,
                "CLUSTERMESH_DEBUG_MAX_REPAIR_MEMBERS": str(args.live_overlay_max_repair_roles),
                "CLUSTERMESH_DEBUG_FORCE_REPAIR_ROLES_FILE": evidence["initial"]["roles_file"],
                "CMP_MEMBER_LABEL_KEY": "mesh", "CMP_MEMBER_LABEL_VALUE": "true",
                "CMP_MEMBER_REPAIR_LABEL_VALUE": "repairing",
                "BUILD_ARTIFACTSTAGINGDIRECTORY": artifact_dir,
            })
            errors = []
            try:
                repair["exit_code"] = run_live_overlay_command(
                    ["bash", str(Path(__file__).resolve().parents[4]
                                 / "steps/topology/clustermesh-scale/reuse/repair-existing-fleet-overlay.sh")],
                    remaining(), repair["log_file"], environment,
                )
                if repair["exit_code"] != 0:
                    raise ReconcileError(f"Bounded Fleet repair failed (exit={repair['exit_code']})")
            except ReconcileError as error:
                repair["error"] = str(error)
                errors.append(str(error))
            evidence["status"] = "verifying"
            write_json_atomic(args.summary_file, summary)
            print("Early live-overlay recovery: requiring strict full-fleet postproof.", flush=True)
            try:
                if probe("after_repair", 40, identities) != 0:
                    raise ReconcileError("Live overlay did not converge after the single bounded Fleet repair")
                current, failed = read_quiescent_inventory(args, summary, "live_overlay_after_repair", bounded_read)
                if authority("authority_after_repair", current, repair_roles) != identities:
                    raise ReconcileError("Fleet Cilium identities changed after the bounded Fleet repair")
            except ReconcileError as error:
                evidence["post_repair_error"] = str(error)
                evidence["post_repair_failure_evidence"] = error.evidence
                errors.append(str(error))
            if errors:
                raise ReconcileError("; ".join(errors))
            evidence["status"] = "repaired"
            print("Early live-overlay recovery: repaired; original pool gates remain required.", flush=True)
            return current, failed
    except ReconcileError as error:
        evidence.update({"status": "failed", "error": str(error), "failure_evidence": error.evidence})
        raise
    finally:
        write_json_atomic(args.summary_file, summary)


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
    parser.add_argument("--live-overlay-repair-enabled", action="store_true")
    parser.add_argument("--live-overlay-max-repair-roles", type=int, default=20)
    parser.add_argument("--live-overlay-timeout-seconds", type=int, default=18000)
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
        "live_overlay_max_repair_roles",
        "live_overlay_timeout_seconds",
        "mutation_timeout_seconds",
        "recovery_timeout_seconds",
        "poll_seconds",
        "submit_attempts",
    ):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if args.live_overlay_max_repair_roles > MAX_LIVE_OVERLAY_REPAIR_ROLES:
        parser.error("--live-overlay-max-repair-roles cannot exceed 20")
    if args.live_overlay_repair_enabled and (
        not args.failed_pool_repair_enabled or args.expected_count != LIVE_OVERLAY_CLUSTER_COUNT
    ):
        parser.error("--live-overlay-repair-enabled requires --failed-pool-repair-enabled and --expected-count 100")
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

        read_connected_fleet_members(args, clusters, summary, run_command)

        failure_evidence: Dict[str, dict] = {}
        summary["failure_evidence"] = failure_evidence
        with tempfile.TemporaryDirectory(prefix="aks-arm-reconcile-") as temp_dir:
            for cluster in failed:
                failure_evidence[cluster.role] = read_failed_cluster_operation(
                    cluster, run_command, args.query_timeout_seconds,
                )
            if args.live_overlay_repair_enabled and any(cluster.failed_pools for cluster in clusters):
                clusters, failed = recover_live_overlay(clusters, args, summary, run_command)
                summary["post_overlay_failure_evidence"] = {}
                for cluster in failed:
                    summary["post_overlay_failure_evidence"][cluster.role] = read_failed_cluster_operation(
                        cluster, run_command, args.query_timeout_seconds,
                    )
            for cluster in failed:
                validate_cluster_data_plane(
                    cluster,
                    os.path.join(temp_dir, f"{cluster.role}.config"),
                    args.expected_count - 1,
                    run_command,
                    args.query_timeout_seconds,
                )

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
        read_connected_fleet_members(
            args, final_clusters, summary, run_command, phase="final",
        )
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
                "fatal_error_evidence": exc.evidence,
            }
        )
        write_json_atomic(args.summary_file, summary)
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
