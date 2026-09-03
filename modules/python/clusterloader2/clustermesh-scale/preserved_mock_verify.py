#!/usr/bin/env python3
"""Verify a preserved n=100 mock layer against a prior capture artifact."""

# pylint: disable=too-many-lines

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Sequence, Tuple


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

import preserved_mock_capture as capture  # pylint: disable=wrong-import-position


class VerificationError(Exception):
    """Expected fail-closed verification error."""


Runner = Callable[[Sequence[str], int], str]


def utc_now() -> str:
    """Return current UTC time in RFC3339 format."""

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_json_atomic(path: str, payload: dict) -> None:
    """Write JSON atomically."""

    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    temporary = f"{path}.tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(temporary, path)


def load_json(path: str, description: str) -> object:
    """Load one JSON file with an actionable error."""

    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise VerificationError(f"unable to read {description} {path}: {exc}") from exc


def normalize_resource_id(resource_id: str) -> str:
    """Normalize case-insensitive Azure resource IDs for comparison."""

    return resource_id.rstrip("/").casefold()


def validate_uid_map(
    payload: object,
    expected_count: int,
    description: str,
) -> Dict[str, str]:
    """Validate one exact name-to-UID map."""

    if not isinstance(payload, dict):
        raise VerificationError(f"{description} is not an object")
    expected_names = {f"kwok-node-{index}" for index in range(expected_count)}
    if set(payload) != expected_names:
        missing = sorted(expected_names - set(payload))[:10]
        extra = sorted(set(payload) - expected_names)[:10]
        raise VerificationError(
            f"{description} is not exact: missing={missing} extra={extra}"
        )
    identities = {}
    for name, uid in payload.items():
        if not isinstance(uid, str) or not uid:
            raise VerificationError(f"{description} has an invalid UID for {name}")
        identities[str(name)] = uid
    if len(set(identities.values())) != expected_count:
        raise VerificationError(f"{description} contains duplicate UIDs")
    return identities


def _reject_symlinks(root: str) -> None:
    """Reject symlinks in an artifact-owned desired-state tree."""

    for directory, dirnames, filenames in os.walk(root):
        for name in [*dirnames, *filenames]:
            path = os.path.join(directory, name)
            if os.path.islink(path):
                raise VerificationError(f"desired-state artifact contains symlink: {path}")


def validate_state_tree(
    state_root: str,
    baseline_by_role: Dict[str, dict],
    run_id: str,
    expected_mock_count: int,
) -> None:
    """Validate exact desired-state roles and file digests."""

    if not os.path.isdir(state_root) or os.path.islink(state_root):
        raise VerificationError(f"desired-state root is not a directory: {state_root}")
    _reject_symlinks(state_root)
    try:
        entries = list(os.scandir(state_root))
    except OSError as exc:
        raise VerificationError(
            f"unable to enumerate desired-state root {state_root}: {exc}"
        ) from exc
    role_dirs = {
        entry.name
        for entry in entries
        if entry.is_dir(follow_symlinks=False)
    }
    non_directories = sorted(
        entry.name for entry in entries if not entry.is_dir(follow_symlinks=False)
    )
    if role_dirs != set(baseline_by_role) or non_directories:
        raise VerificationError(
            "desired-state role inventory is not exact: "
            f"roles={len(role_dirs)} files={non_directories[:10]}"
        )

    for role, baseline_row in baseline_by_role.items():
        role_dir = os.path.join(state_root, role)
        capture.load_state_metadata(role_dir, run_id, expected_mock_count)
        try:
            digests = capture.file_digests(role_dir)
        except OSError as exc:
            raise VerificationError(
                f"{role}: unable to hash desired-state files: {exc}"
            ) from exc
        if digests != baseline_row["desired_state_sha256"]:
            raise VerificationError(f"{role}: desired-state SHA-256 map changed")


def load_baseline(
    baseline_dir: str,
    run_id: str,
    expected_cluster_count: int,
    expected_mock_count: int,
) -> Tuple[dict, Dict[str, dict]]:
    """Load and validate a complete prior capture artifact."""

    summary = load_json(
        os.path.join(baseline_dir, "summary.json"),
        "baseline summary",
    )
    baseline = load_json(
        os.path.join(baseline_dir, "baseline.json"),
        "baseline",
    )
    if not isinstance(summary, dict) or not isinstance(baseline, dict):
        raise VerificationError("baseline summary and baseline must be objects")

    expected_summary = {
        "healthy": True,
        "run_id": run_id,
        "cluster_count": expected_cluster_count,
        "mock_count_per_cluster": expected_mock_count,
        "total_kwok_nodes": expected_cluster_count * expected_mock_count,
        "total_mock_agents": expected_cluster_count * expected_mock_count,
        "desired_state_copied": True,
    }
    for key, expected in expected_summary.items():
        if summary.get(key) != expected:
            raise VerificationError(
                f"baseline summary {key}={summary.get(key)!r}, expected {expected!r}"
            )
    for key, expected in expected_summary.items():
        if key in ("healthy", "desired_state_copied"):
            continue
        if baseline.get(key) != expected:
            raise VerificationError(
                f"baseline {key}={baseline.get(key)!r}, expected {expected!r}"
            )

    rows = baseline.get("clusters")
    if not isinstance(rows, list) or len(rows) != expected_cluster_count:
        raise VerificationError("baseline cluster inventory count mismatch")
    expected_roles = {f"mesh-{index}" for index in range(1, expected_cluster_count + 1)}
    baseline_by_role = {}
    cluster_ids = set()
    cluster_names = set()
    for row in rows:
        if not isinstance(row, dict):
            raise VerificationError("baseline cluster entry is not an object")
        role = row.get("role")
        if role not in expected_roles or role in baseline_by_role:
            raise VerificationError(f"baseline has invalid or duplicate role: {role!r}")
        number = capture.role_number(str(role))
        if row.get("name") != f"clustermesh-{number}":
            raise VerificationError(f"{role}: baseline AKS name mismatch")
        if row.get("resource_group") != run_id:
            raise VerificationError(f"{role}: baseline resource group mismatch")
        resource_id = row.get("resource_id")
        if not isinstance(resource_id, str) or not resource_id:
            raise VerificationError(f"{role}: baseline AKS resource ID is missing")
        cluster_name = row.get("cluster_name")
        cluster_id = row.get("cluster_id")
        if not isinstance(cluster_name, str) or not cluster_name:
            raise VerificationError(f"{role}: baseline Cilium cluster name is missing")
        if not isinstance(cluster_id, (str, int)) or not str(cluster_id):
            raise VerificationError(f"{role}: baseline Cilium cluster ID is missing")
        cluster_ids.add(str(cluster_id))
        cluster_names.add(cluster_name)
        row["node_uids"] = validate_uid_map(
            row.get("node_uids"),
            expected_mock_count,
            f"{role} baseline KWOK identities",
        )
        row["agent_uids"] = validate_uid_map(
            row.get("agent_uids"),
            expected_mock_count,
            f"{role} baseline agent identities",
        )
        if not isinstance(row.get("desired_state_sha256"), dict):
            raise VerificationError(f"{role}: baseline desired-state digests are missing")
        baseline_by_role[str(role)] = row
    if set(baseline_by_role) != expected_roles:
        raise VerificationError("baseline roles are not exactly mesh-1..mesh-N")
    if len(cluster_ids) != expected_cluster_count:
        raise VerificationError("baseline Cilium cluster IDs are not unique")
    if len(cluster_names) != expected_cluster_count:
        raise VerificationError("baseline Cilium cluster names are not unique")

    state_root = os.path.join(baseline_dir, "desired-state")
    validate_state_tree(
        state_root,
        baseline_by_role,
        run_id,
        expected_mock_count,
    )
    return baseline, baseline_by_role


def restore_state(
    baseline_dir: str,
    state_root: str,
    baseline_by_role: Dict[str, dict],
    run_id: str,
    expected_mock_count: int,
) -> None:
    """Restore exact artifact state into the reconciler's run-scoped root."""

    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", run_id):
        raise VerificationError(f"unsafe run_id for state restore: {run_id!r}")
    destination = os.path.abspath(os.path.expanduser(state_root))
    if (
        os.path.basename(destination) != run_id
        or os.path.basename(os.path.dirname(destination)) != "mock-layer-state"
    ):
        raise VerificationError(
            f"state root must end with mock-layer-state/{run_id}: {destination}"
        )
    parent = os.path.dirname(destination)
    current = parent
    while True:
        if os.path.lexists(current) and os.path.islink(current):
            raise VerificationError(
                f"refusing state root beneath symlinked path: {current}"
            )
        next_parent = os.path.dirname(current)
        if next_parent == current:
            break
        current = next_parent
    source = os.path.join(os.path.abspath(baseline_dir), "desired-state")
    try:
        if os.path.lexists(destination):
            if os.path.islink(destination):
                raise VerificationError(f"refusing symlink state root: {destination}")
            shutil.rmtree(destination)
        os.makedirs(parent, exist_ok=True)
        if os.path.dirname(os.path.realpath(destination)) != os.path.realpath(parent):
            raise VerificationError(
                f"resolved state root escaped expected parent: {destination}"
            )
        shutil.copytree(source, destination)
    except OSError as exc:
        raise VerificationError(
            f"unable to restore desired state into {destination}: {exc}"
        ) from exc
    validate_state_tree(
        destination,
        baseline_by_role,
        run_id,
        expected_mock_count,
    )


def validate_platform_state(
    clusters: List[capture.Cluster],
    *,
    subscription_id: str,
    run_id: str,
    expected_pool_count: int,
    fleet_name: str,
    profile_name: str,
    runner: Runner,
) -> dict:
    """Require exact healthy AKS, pool, and Fleet membership state."""

    aks = capture.parse_json(
        runner(
            [
                "az",
                "aks",
                "list",
                "--subscription",
                subscription_id,
                "--resource-group",
                run_id,
                "--output",
                "json",
                "--only-show-errors",
            ],
            600,
        ),
        "AKS resource inventory",
    )
    resource_ids = capture.validate_aks_inventory(aks, clusters)
    if not isinstance(aks, list):
        raise VerificationError("AKS inventory is not an array")
    pools = []
    pool_keys = set()
    for row in aks:
        role = (row.get("tags") or {}).get("role") if isinstance(row, dict) else None
        if row.get("powerState", {}).get("code") != "Running":
            raise VerificationError(f"{role or 'unknown'}: AKS power state is not Running")
        profiles = row.get("agentPoolProfiles")
        if not isinstance(profiles, list) or not profiles:
            raise VerificationError(f"{role or 'unknown'}: AKS pool inventory is missing")
        for pool in profiles:
            if (
                not isinstance(pool, dict)
                or pool.get("provisioningState") != "Succeeded"
                or pool.get("powerState", {}).get("code") != "Running"
            ):
                raise VerificationError(
                    f"{role or 'unknown'}: AKS pool is not Succeeded/Running"
                )
            pool_name = pool.get("name")
            if not isinstance(pool_name, str) or not pool_name:
                raise VerificationError(f"{role or 'unknown'}: AKS pool name is missing")
            pool_key = (str(role), pool_name)
            if pool_key in pool_keys:
                raise VerificationError(f"{role}: duplicate AKS pool {pool_name}")
            pool_keys.add(pool_key)
            pools.append({"role": role, "name": pool_name})
    if len(pools) != expected_pool_count:
        raise VerificationError(
            f"expected {expected_pool_count} AKS pools, got {len(pools)}"
        )
    expected_pool_keys = {
        (cluster.role, pool_name)
        for cluster in clusters
        for pool_name in ("default", "prompool")
    }
    expected_pool_keys.add(("mesh-1", "churnpool"))
    if pool_keys != expected_pool_keys:
        missing = sorted(expected_pool_keys - pool_keys)[:10]
        extra = sorted(pool_keys - expected_pool_keys)[:10]
        raise VerificationError(
            f"AKS pool inventory is not exact: missing={missing} extra={extra}"
        )

    members = capture.parse_json(
        runner(
            [
                "az",
                "fleet",
                "clustermeshprofile",
                "list-members",
                "--subscription",
                subscription_id,
                "--resource-group",
                run_id,
                "--fleet-name",
                fleet_name,
                "--name",
                profile_name,
                "--output",
                "json",
                "--only-show-errors",
            ],
            300,
        ),
        "Fleet ClusterMesh member inventory",
    )
    if not isinstance(members, list) or len(members) != len(clusters):
        raise VerificationError("Fleet ClusterMesh member count mismatch")
    expected_roles = {cluster.role for cluster in clusters}
    member_by_role = {}
    for member in members:
        if not isinstance(member, dict):
            raise VerificationError("malformed Fleet member entry")
        role = member.get("name")
        if role not in expected_roles or role in member_by_role:
            raise VerificationError(f"invalid or duplicate Fleet member: {role!r}")
        state = (member.get("meshProperties") or {}).get("status", {}).get("state")
        if member.get("provisioningState") != "Succeeded" or state != "Connected":
            raise VerificationError(
                f"{role}: Fleet member is not Succeeded/Connected"
            )
        cluster_resource_id = member.get("clusterResourceId")
        if (
            not isinstance(cluster_resource_id, str)
            or normalize_resource_id(cluster_resource_id)
            != normalize_resource_id(resource_ids[str(role)])
        ):
            raise VerificationError(f"{role}: Fleet member AKS ID mismatch")
        member_by_role[str(role)] = member
    if set(member_by_role) != expected_roles:
        raise VerificationError("Fleet roles are not exactly mesh-1..mesh-N")

    return {
        "aks_count": len(aks),
        "pool_count": len(pools),
        "fleet_member_count": len(members),
        "fleet_connected_count": len(members),
        "resource_ids": resource_ids,
    }


def capture_live(
    clusters: List[capture.Cluster],
    *,
    state_root: str,
    run_id: str,
    expected_cluster_count: int,
    expected_mock_count: int,
    max_concurrent: int,
    command_timeout_seconds: int,
    resource_ids: Dict[str, str],
    expected_cilium_names: Dict[str, str],
    runner: Runner,
) -> List[dict]:
    """Capture exact live identities and Cilium health for every cluster."""

    def capture_one(cluster: capture.Cluster) -> dict:
        result = capture.probe_cluster(
            cluster,
            state_root=state_root,
            run_id=run_id,
            expected_cluster_count=expected_cluster_count,
            expected_mock_count=expected_mock_count,
            command_timeout_seconds=command_timeout_seconds,
            runner=runner,
            expected_remote_names={
                name
                for role, name in expected_cilium_names.items()
                if role != cluster.role
            },
        )
        result["resource_id"] = resource_ids[cluster.role]
        print(
            f"{cluster.role}: verified {expected_mock_count} KWOK and agent identities",
            flush=True,
        )
        return result

    results = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_concurrent
    ) as executor:
        futures = {executor.submit(capture_one, cluster): cluster.role for cluster in clusters}
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    return sorted(results, key=lambda row: capture.role_number(row["role"]))


def _rows_by_role(rows: Sequence[dict], description: str) -> Dict[str, dict]:
    indexed = {}
    for row in rows:
        role = row.get("role") if isinstance(row, dict) else None
        if not isinstance(role, str) or role in indexed:
            raise VerificationError(f"{description} has invalid or duplicate role")
        indexed[role] = row
    return indexed


def _changed_names(
    before: Dict[str, str],
    after: Dict[str, str],
    description: str,
) -> set:
    if set(before) != set(after):
        raise VerificationError(f"{description} object-name set changed")
    return {name for name in before if before[name] != after[name]}


def compare_pre_boundary(
    baseline_by_role: Dict[str, dict],
    live_rows: Sequence[dict],
) -> dict:
    """Require every cross-run identity and desired-state digest to survive."""

    live_by_role = _rows_by_role(live_rows, "pre-boundary snapshot")
    if set(live_by_role) != set(baseline_by_role):
        raise VerificationError("pre-boundary cluster roles changed")
    for role, baseline in baseline_by_role.items():
        live = live_by_role[role]
        if normalize_resource_id(live["resource_id"]) != normalize_resource_id(
            baseline["resource_id"]
        ):
            raise VerificationError(f"{role}: AKS resource ID changed")
        for field in ("cluster_name", "cluster_id", "desired_state_sha256"):
            if live.get(field) != baseline.get(field):
                raise VerificationError(f"{role}: {field} changed")
        changed_nodes = _changed_names(
            baseline["node_uids"],
            live["node_uids"],
            f"{role} KWOK identities",
        )
        changed_agents = _changed_names(
            baseline["agent_uids"],
            live["agent_uids"],
            f"{role} agent identities",
        )
        if changed_nodes or changed_agents:
            raise VerificationError(
                f"{role}: cross-run UIDs changed before fault injection: "
                f"nodes={sorted(changed_nodes)[:10]} "
                f"agents={sorted(changed_agents)[:10]}"
            )
    return {
        "aks_ids_preserved": len(baseline_by_role),
        "kwok_uids_preserved": sum(
            len(row["node_uids"]) for row in baseline_by_role.values()
        ),
        "agent_uids_preserved": sum(
            len(row["agent_uids"]) for row in baseline_by_role.values()
        ),
        "desired_state_digest_sets_preserved": len(baseline_by_role),
    }


def build_fault_plan(
    fault_roles: Sequence[str],
    *,
    expected_cluster_count: int,
    expected_mock_count: int,
    fault_count: int,
    agent_start: int,
    node_start: int,
) -> dict:
    """Build a bounded deterministic fault plan."""

    roles = sorted(set(fault_roles), key=capture.role_number)
    if not roles or len(roles) != len(fault_roles):
        raise VerificationError("fault roles must be non-empty and unique")
    if len(roles) > 5:
        raise VerificationError("fault injection is bounded to at most five clusters")
    expected_roles = {f"mesh-{index}" for index in range(1, expected_cluster_count + 1)}
    if any(role not in expected_roles for role in roles):
        raise VerificationError(f"fault role is outside the cluster inventory: {roles}")
    if fault_count < 1 or fault_count > 5:
        raise VerificationError("fault count must be between one and five")
    if min(agent_start, node_start) < 0:
        raise VerificationError("fault object starts must be non-negative")
    agent_only_names = [
        f"kwok-node-{index}" for index in range(agent_start, agent_start + fault_count)
    ]
    node_names = [
        f"kwok-node-{index}" for index in range(node_start, node_start + fault_count)
    ]
    if agent_start + fault_count > expected_mock_count:
        raise VerificationError("agent fault range exceeds expected mock count")
    if node_start + fault_count > expected_mock_count:
        raise VerificationError("node fault range exceeds expected mock count")
    if set(agent_only_names) & set(node_names):
        raise VerificationError("agent-only and node fault ranges must not overlap")
    agent_names = [*agent_only_names, *node_names]
    return {
        "roles": roles,
        "fault_count_per_kind_per_role": fault_count,
        "node_names": node_names,
        "agent_only_names": agent_only_names,
        "agent_names": agent_names,
        "total_deleted_nodes": len(roles) * len(node_names),
        "total_deleted_agent_pods": len(roles) * len(agent_names),
    }


def inject_faults(
    clusters: List[capture.Cluster],
    fault_plan: dict,
    *,
    max_concurrent: int,
    command_timeout_seconds: int,
    runner: Runner,
) -> List[dict]:
    """Delete only the bounded Node and agent names in the fault plan."""

    clusters_by_role = {cluster.role: cluster for cluster in clusters}

    def inject_one(role: str) -> dict:
        cluster = clusters_by_role[role]
        base = [
            "kubectl",
            "--kubeconfig",
            cluster.kubeconfig,
            f"--request-timeout={command_timeout_seconds}s",
        ]
        try:
            runner(
                base
                + [
                    "delete",
                    "node",
                    *fault_plan["node_names"],
                    "--wait=true",
                    "--timeout=180s",
                ],
                max(command_timeout_seconds, 240),
            )
            runner(
                base
                + [
                    "-n",
                    "mock-clustermesh",
                    "delete",
                    "pod",
                    *fault_plan["agent_names"],
                    "--wait=false",
                    "--grace-period=0",
                    "--force",
                ],
                max(command_timeout_seconds, 120),
            )
            print(f"{role}: bounded mock-layer loss injected", flush=True)
            return {"role": role, "success": True}
        except capture.CaptureError as exc:
            return {"role": role, "success": False, "error": str(exc)}

    results = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(max_concurrent, len(fault_plan["roles"]))
    ) as executor:
        futures = {
            executor.submit(inject_one, role): role for role in fault_plan["roles"]
        }
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    return sorted(results, key=lambda row: capture.role_number(row["role"]))


def run_reconciler(
    reconciler: str,
    *,
    clusters_path: str,
    state_root: str,
    run_id: str,
    expected_mock_count: int,
    artifact_dir: str,
    max_concurrent: int,
    timeout_seconds: int,
) -> dict:
    """Run the exact bounded reconciler and validate its summary."""

    summary_path = os.path.join(artifact_dir, "mock-reconcile-summary.json")
    diagnostics_dir = os.path.join(artifact_dir, "mock-reconcile-diagnostics")
    command = [
        sys.executable,
        reconciler,
        "--clusters",
        clusters_path,
        "--state-root",
        state_root,
        "--expected-mock-count",
        str(expected_mock_count),
        "--run-id",
        run_id,
        "--summary-file",
        summary_path,
        "--diagnostics-dir",
        diagnostics_dir,
        "--max-concurrent",
        str(max_concurrent),
        "--attempts",
        "6",
        "--settle-seconds",
        "15",
        "--request-timeout-seconds",
        "45",
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise VerificationError(
            f"mock-layer reconciliation timed out after {timeout_seconds}s"
        ) from exc
    except OSError as exc:
        raise VerificationError(f"unable to start mock-layer reconciler: {exc}") from exc
    summary = load_json(summary_path, "mock-layer reconcile summary")
    if not isinstance(summary, dict):
        raise VerificationError("mock-layer reconcile summary is not an object")
    if completed.returncode != 0 or summary.get("success") is not True:
        raise VerificationError(
            "mock-layer reconciliation failed: "
            f"exit={completed.returncode} roles={summary.get('failed_roles')}"
        )
    if (
        summary.get("total_clusters") != summary.get("healthy_count")
        or summary.get("failed_count") != 0
        or summary.get("pending_roles")
    ):
        raise VerificationError("mock-layer reconciliation summary is incomplete")
    return summary


def compare_post_recovery(
    baseline_by_role: Dict[str, dict],
    live_rows: Sequence[dict],
    fault_plan: dict,
) -> dict:
    """Require only the intentionally affected identities to change."""

    live_by_role = _rows_by_role(live_rows, "post-recovery snapshot")
    if set(live_by_role) != set(baseline_by_role):
        raise VerificationError("post-recovery cluster roles changed")
    fault_roles = set(fault_plan["roles"])
    expected_node_changes = set(fault_plan["node_names"])
    expected_agent_changes = set(fault_plan["agent_names"])
    role_results = []
    changed_node_total = 0
    changed_agent_total = 0
    for role, baseline in baseline_by_role.items():
        live = live_by_role[role]
        if normalize_resource_id(live["resource_id"]) != normalize_resource_id(
            baseline["resource_id"]
        ):
            raise VerificationError(f"{role}: AKS resource ID changed after recovery")
        for field in ("cluster_name", "cluster_id", "desired_state_sha256"):
            if live.get(field) != baseline.get(field):
                raise VerificationError(f"{role}: {field} changed after recovery")
        changed_nodes = _changed_names(
            baseline["node_uids"],
            live["node_uids"],
            f"{role} post-recovery KWOK identities",
        )
        changed_agents = _changed_names(
            baseline["agent_uids"],
            live["agent_uids"],
            f"{role} post-recovery agent identities",
        )
        wanted_nodes = expected_node_changes if role in fault_roles else set()
        wanted_agents = expected_agent_changes if role in fault_roles else set()
        if changed_nodes != wanted_nodes:
            raise VerificationError(
                f"{role}: unexpected changed KWOK UIDs: "
                f"actual={sorted(changed_nodes)} expected={sorted(wanted_nodes)}"
            )
        if changed_agents != wanted_agents:
            raise VerificationError(
                f"{role}: unexpected changed agent UIDs: "
                f"actual={sorted(changed_agents)} expected={sorted(wanted_agents)}"
            )
        changed_node_total += len(changed_nodes)
        changed_agent_total += len(changed_agents)
        if role in fault_roles:
            role_results.append(
                {
                    "role": role,
                    "changed_node_uids": sorted(changed_nodes),
                    "changed_agent_uids": sorted(changed_agents),
                }
            )
    total_nodes = sum(len(row["node_uids"]) for row in baseline_by_role.values())
    total_agents = sum(len(row["agent_uids"]) for row in baseline_by_role.values())
    return {
        "changed_node_uids": changed_node_total,
        "changed_agent_uids": changed_agent_total,
        "unchanged_node_uids": total_nodes - changed_node_total,
        "unchanged_agent_uids": total_agents - changed_agent_total,
        "fault_roles": role_results,
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", required=True)
    parser.add_argument("--baseline-build-id", type=int, required=True)
    parser.add_argument("--clusters", required=True)
    parser.add_argument("--state-root", required=True)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--reconciler", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expected-subscription-id", required=True)
    parser.add_argument("--expected-cluster-count", type=int, required=True)
    parser.add_argument("--expected-mock-count", type=int, required=True)
    parser.add_argument("--expected-pool-count", type=int, required=True)
    parser.add_argument("--fleet-name", default="clustermesh-flt")
    parser.add_argument("--profile-name", default="clustermesh-cmp")
    parser.add_argument("--fault-role", action="append", dest="fault_roles", required=True)
    parser.add_argument("--fault-count", type=int, default=5)
    parser.add_argument("--fault-agent-start", type=int, default=0)
    parser.add_argument("--fault-node-start", type=int, default=10)
    parser.add_argument("--max-concurrent", type=int, default=8)
    parser.add_argument("--reconcile-concurrent", type=int, default=8)
    parser.add_argument("--command-timeout-seconds", type=int, default=120)
    parser.add_argument("--reconcile-timeout-seconds", type=int, default=3600)
    args = parser.parse_args(argv)
    for name in (
        "baseline_build_id",
        "expected_cluster_count",
        "expected_mock_count",
        "expected_pool_count",
        "max_concurrent",
        "reconcile_concurrent",
        "command_timeout_seconds",
        "reconcile_timeout_seconds",
    ):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the cross-run preservation and exact recovery proof."""

    args = parse_args(argv)
    os.makedirs(args.artifact_dir, exist_ok=True)
    summary_path = os.path.join(args.artifact_dir, "summary.json")
    summary = {
        "schema_version": 1,
        "started_at": utc_now(),
        "finished_at": None,
        "healthy": False,
        "identity_verification_healthy": False,
        "cross_cluster_data_path_valid": False,
        "no_cl2_scenarios_run": True,
        "stage": "initializing",
        "run_id": args.run_id,
        "baseline_build_id": args.baseline_build_id,
    }
    write_json_atomic(summary_path, summary)
    stage = "loading_baseline"
    try:
        _, baseline_by_role = load_baseline(
            args.baseline_dir,
            args.run_id,
            args.expected_cluster_count,
            args.expected_mock_count,
        )
        clusters = capture.load_clusters(
            args.clusters,
            args.expected_cluster_count,
        )
        if any(cluster.resource_group != args.run_id for cluster in clusters):
            raise VerificationError("cluster inventory resource group does not match run_id")
        expected_cilium_names = {
            role: str(row["cluster_name"]) for role, row in baseline_by_role.items()
        }

        stage = "restoring_desired_state"
        restore_state(
            args.baseline_dir,
            args.state_root,
            baseline_by_role,
            args.run_id,
            args.expected_mock_count,
        )

        stage = "capturing_pre_boundary"
        platform_before = validate_platform_state(
            clusters,
            subscription_id=args.expected_subscription_id,
            run_id=args.run_id,
            expected_pool_count=args.expected_pool_count,
            fleet_name=args.fleet_name,
            profile_name=args.profile_name,
            runner=capture.run_command,
        )
        live_before = capture_live(
            clusters,
            state_root=args.state_root,
            run_id=args.run_id,
            expected_cluster_count=args.expected_cluster_count,
            expected_mock_count=args.expected_mock_count,
            max_concurrent=args.max_concurrent,
            command_timeout_seconds=args.command_timeout_seconds,
            resource_ids=platform_before["resource_ids"],
            expected_cilium_names=expected_cilium_names,
            runner=capture.run_command,
        )
        pre_boundary = compare_pre_boundary(baseline_by_role, live_before)
        write_json_atomic(
            os.path.join(args.artifact_dir, "live-pre.json"),
            {
                "captured_at": utc_now(),
                "platform": platform_before,
                "clusters": live_before,
            },
        )
        print(
            "Cross-run boundary preserved all "
            f"{pre_boundary['kwok_uids_preserved']} KWOK and "
            f"{pre_boundary['agent_uids_preserved']} agent UIDs.",
            flush=True,
        )

        stage = "injecting_bounded_loss"
        fault_plan = build_fault_plan(
            args.fault_roles,
            expected_cluster_count=args.expected_cluster_count,
            expected_mock_count=args.expected_mock_count,
            fault_count=args.fault_count,
            agent_start=args.fault_agent_start,
            node_start=args.fault_node_start,
        )
        write_json_atomic(
            os.path.join(args.artifact_dir, "fault-plan.json"),
            fault_plan,
        )
        fault_results = inject_faults(
            clusters,
            fault_plan,
            max_concurrent=args.max_concurrent,
            command_timeout_seconds=args.command_timeout_seconds,
            runner=capture.run_command,
        )
        write_json_atomic(
            os.path.join(args.artifact_dir, "fault-results.json"),
            {"injected_at": utc_now(), "results": fault_results},
        )

        stage = "reconciling_mock_layer"
        reconcile_summary = run_reconciler(
            args.reconciler,
            clusters_path=args.clusters,
            state_root=args.state_root,
            run_id=args.run_id,
            expected_mock_count=args.expected_mock_count,
            artifact_dir=args.artifact_dir,
            max_concurrent=args.reconcile_concurrent,
            timeout_seconds=args.reconcile_timeout_seconds,
        )
        failed_fault_roles = [
            result["role"] for result in fault_results if not result["success"]
        ]
        if failed_fault_roles:
            raise VerificationError(
                "fault injection failed on roles after bounded reconciliation: "
                + " ".join(failed_fault_roles)
            )

        stage = "capturing_post_recovery"
        platform_after = validate_platform_state(
            clusters,
            subscription_id=args.expected_subscription_id,
            run_id=args.run_id,
            expected_pool_count=args.expected_pool_count,
            fleet_name=args.fleet_name,
            profile_name=args.profile_name,
            runner=capture.run_command,
        )
        live_after = capture_live(
            clusters,
            state_root=args.state_root,
            run_id=args.run_id,
            expected_cluster_count=args.expected_cluster_count,
            expected_mock_count=args.expected_mock_count,
            max_concurrent=args.max_concurrent,
            command_timeout_seconds=args.command_timeout_seconds,
            resource_ids=platform_after["resource_ids"],
            expected_cilium_names=expected_cilium_names,
            runner=capture.run_command,
        )
        post_recovery = compare_post_recovery(
            baseline_by_role,
            live_after,
            fault_plan,
        )
        write_json_atomic(
            os.path.join(args.artifact_dir, "live-post.json"),
            {
                "captured_at": utc_now(),
                "platform": platform_after,
                "clusters": live_after,
            },
        )
        verification = {
            "schema_version": 1,
            "verified_at": utc_now(),
            "run_id": args.run_id,
            "baseline_build_id": args.baseline_build_id,
            "pre_boundary": pre_boundary,
            "fault_plan": fault_plan,
            "fault_results": fault_results,
            "reconcile": {
                "success": reconcile_summary["success"],
                "healthy_count": reconcile_summary["healthy_count"],
                "total_clusters": reconcile_summary["total_clusters"],
            },
            "post_recovery": post_recovery,
            "platform_before": platform_before,
            "platform_after": platform_after,
        }
        write_json_atomic(
            os.path.join(args.artifact_dir, "verification.json"),
            verification,
        )
        summary.update(
            {
                "stage": "awaiting_cross_cluster_data_path",
                "identity_verification_healthy": True,
                "cluster_count": args.expected_cluster_count,
                "pool_count": args.expected_pool_count,
                "fleet_connected_count": args.expected_cluster_count,
                "total_kwok_nodes": args.expected_cluster_count
                * args.expected_mock_count,
                "total_mock_agents": args.expected_cluster_count
                * args.expected_mock_count,
                "pre_boundary": pre_boundary,
                "fault_plan": fault_plan,
                "post_recovery": post_recovery,
            }
        )
        write_json_atomic(summary_path, summary)
        print(
            "Preserved mock verification passed identity and exact recovery gates; "
            "awaiting cross-cluster data-path validation.",
            flush=True,
        )
        return 0
    except (VerificationError, capture.CaptureError) as exc:
        summary.update(
            {
                "finished_at": utc_now(),
                "healthy": False,
                "identity_verification_healthy": False,
                "stage": stage,
                "fatal_error": str(exc),
            }
        )
        write_json_atomic(summary_path, summary)
        print(str(exc), file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
