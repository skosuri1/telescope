#!/usr/bin/env python3
"""Detect live ClusterMesh drift in a preserved Fleet overlay.

Fleet's member status can remain ``Connected`` after the Cilium data plane has
lost a peer or its remote cluster configuration. This probe reads each
cluster's actual Cilium identity and ``cilium-dbg status -o json`` output,
compares the live peer set with the complete preserved inventory, and emits the
smallest Fleet member set that should be rejoined.

Exit codes:
  0: every live peer is present and ready
  1: the inventory or live output is unsafe/invalid
  2: bounded member repair is required; roles are written to --repair-roles-file
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import subprocess
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple


class ProbeError(Exception):
    """Expected probe failure with an actionable message."""


@dataclass(frozen=True)
class Cluster:
    """One preserved AKS cluster."""

    name: str
    resource_group: str
    role: str
    kubeconfig: str


@dataclass(frozen=True)
class ClusterIdentity:
    """Cilium identity advertised by one local cluster."""

    role: str
    cluster_name: str
    cluster_id: int


@dataclass
class ClusterDrift:
    """Live drift observed from one local cluster."""

    role: str
    cluster_name: str
    missing_remote_names: List[str] = field(default_factory=list)
    not_ready_remote_names: List[str] = field(default_factory=list)
    unexpected_remote_names: List[str] = field(default_factory=list)
    duplicate_remote_names: List[str] = field(default_factory=list)
    command_error: Optional[str] = None

    @property
    def healthy(self) -> bool:
        return not (
            self.missing_remote_names
            or self.not_ready_remote_names
            or self.unexpected_remote_names
            or self.duplicate_remote_names
            or self.command_error
        )


Runner = Callable[[Sequence[str], int], str]


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
        raise ProbeError(
            f"command timed out after {timeout_seconds}s: {' '.join(args)}"
        ) from exc
    except OSError as exc:
        raise ProbeError(f"unable to execute {args[0]}: {exc}") from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise ProbeError(
            f"command failed (exit={completed.returncode}): {' '.join(args)}: "
            f"{detail[:1000]}"
        )
    return completed.stdout


def role_sort_key(role: str) -> Tuple[int, str]:
    """Sort mesh-N roles numerically and leave malformed values deterministic."""

    prefix = "mesh-"
    if role.startswith(prefix) and role[len(prefix) :].isdigit():
        return int(role[len(prefix) :]), role
    return sys.maxsize, role


def resolve_kubeconfig(cluster: dict) -> str:
    """Resolve the standard per-role kubeconfig path."""

    explicit = cluster.get("kubeconfig")
    if explicit:
        return os.path.expanduser(str(explicit))
    return os.path.join(
        os.path.expanduser("~"), ".kube", f"{cluster['role']}.config"
    )


def load_clusters(path: str) -> List[Cluster]:
    """Load and validate the preserved cluster inventory."""

    try:
        with open(path, "r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ProbeError(f"unable to load cluster inventory {path}: {exc}") from exc
    if not isinstance(raw, list) or not raw:
        raise ProbeError("cluster inventory must be a non-empty JSON array")

    clusters: List[Cluster] = []
    seen_roles: Set[str] = set()
    for row in raw:
        if not isinstance(row, dict):
            raise ProbeError("each cluster inventory row must be a JSON object")
        role = row.get("role")
        name = row.get("name")
        resource_group = row.get("rg") or row.get("resource_group")
        if not isinstance(role, str) or not role:
            raise ProbeError("cluster inventory row is missing role")
        if not isinstance(name, str) or not name:
            raise ProbeError(f"{role}: cluster inventory row is missing name")
        if not isinstance(resource_group, str) or not resource_group:
            raise ProbeError(
                f"{role}: cluster inventory row is missing resource group"
            )
        if role in seen_roles:
            raise ProbeError(f"duplicate cluster role in inventory: {role}")
        if not role.startswith("mesh-") or not role[5:].isdigit():
            raise ProbeError(f"invalid cluster role in inventory: {role}")
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


def retry(
    operation: Callable[[], object],
    *,
    attempts: int,
    retry_seconds: int,
) -> object:
    """Run one bounded operation with a small retry budget."""

    last_error: Optional[ProbeError] = None
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except ProbeError as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(retry_seconds)
    if last_error is None:
        raise ProbeError("retry called without attempts")
    raise last_error


def read_identity(
    cluster: Cluster,
    runner: Runner,
    command_timeout_seconds: int,
) -> ClusterIdentity:
    """Read the local Cilium cluster name and ID."""

    output = runner(
        [
            "kubectl",
            "--kubeconfig",
            cluster.kubeconfig,
            f"--request-timeout={command_timeout_seconds}s",
            "-n",
            "kube-system",
            "get",
            "configmap",
            "cilium-config",
            "-o",
            "json",
        ],
        command_timeout_seconds,
    )
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as exc:
        raise ProbeError(f"{cluster.role}: invalid cilium-config JSON: {exc}") from exc
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ProbeError(f"{cluster.role}: cilium-config has no data object")
    cluster_name = data.get("cluster-name")
    raw_cluster_id = data.get("cluster-id")
    if not isinstance(cluster_name, str) or not cluster_name:
        raise ProbeError(f"{cluster.role}: cilium-config cluster-name is missing")
    try:
        cluster_id = int(raw_cluster_id)
    except (TypeError, ValueError) as exc:
        raise ProbeError(
            f"{cluster.role}: invalid cilium-config cluster-id {raw_cluster_id!r}"
        ) from exc
    if cluster_id <= 0:
        raise ProbeError(
            f"{cluster.role}: invalid cilium-config cluster-id {cluster_id}"
        )
    return ClusterIdentity(
        role=cluster.role,
        cluster_name=cluster_name,
        cluster_id=cluster_id,
    )


def read_status(
    cluster: Cluster,
    runner: Runner,
    command_timeout_seconds: int,
) -> List[dict]:
    """Read the live Cilium remote-cluster status list."""

    output = runner(
        [
            "kubectl",
            "--kubeconfig",
            cluster.kubeconfig,
            f"--request-timeout={command_timeout_seconds}s",
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
        command_timeout_seconds,
    )
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as exc:
        raise ProbeError(f"{cluster.role}: invalid cilium status JSON: {exc}") from exc
    mesh = payload.get("cluster-mesh")
    if not isinstance(mesh, dict):
        raise ProbeError(f"{cluster.role}: cilium status has no cluster-mesh object")
    remotes = mesh.get("clusters")
    if not isinstance(remotes, list):
        raise ProbeError(f"{cluster.role}: cluster-mesh.clusters is not an array")
    if any(not isinstance(remote, dict) for remote in remotes):
        raise ProbeError(f"{cluster.role}: malformed remote cluster status entry")
    return remotes


def remote_is_ready(remote: dict) -> bool:
    """Return whether one remote has a usable live Cilium data-plane state."""

    if remote.get("ready") is not True:
        return False
    if "connected" in remote and remote.get("connected") is not True:
        return False
    config = remote.get("config")
    if isinstance(config, dict):
        if config.get("required") is True and config.get("retrieved") is not True:
            return False
    return True


def analyze_status(
    identity: ClusterIdentity,
    remotes: List[dict],
    identity_by_name: Dict[str, ClusterIdentity],
) -> ClusterDrift:
    """Compare one local Cilium view with the complete expected peer set."""

    expected_names = set(identity_by_name) - {identity.cluster_name}
    actual_names: List[str] = []
    not_ready_names: Set[str] = set()
    for remote in remotes:
        name = remote.get("name")
        if not isinstance(name, str) or not name:
            return ClusterDrift(
                role=identity.role,
                cluster_name=identity.cluster_name,
                command_error="remote cluster entry is missing name",
            )
        actual_names.append(name)
        if not remote_is_ready(remote):
            not_ready_names.add(name)

    duplicate_names = sorted(
        {name for name in actual_names if actual_names.count(name) > 1}
    )
    actual_name_set = set(actual_names)
    return ClusterDrift(
        role=identity.role,
        cluster_name=identity.cluster_name,
        missing_remote_names=sorted(expected_names - actual_name_set),
        not_ready_remote_names=sorted(not_ready_names),
        unexpected_remote_names=sorted(actual_name_set - expected_names),
        duplicate_remote_names=duplicate_names,
    )


def repair_selection_for_drift(
    drifts: Iterable[ClusterDrift],
    identity_by_name: Dict[str, ClusterIdentity],
    max_repair_roles: int = 20,
) -> dict:
    """Build a minimum bounded Fleet repair set for every unhealthy edge."""

    forced_local_roles: Set[str] = set()
    directed_edges: Set[Tuple[str, str]] = set()
    for drift in drifts:
        if drift.command_error or drift.unexpected_remote_names:
            forced_local_roles.add(drift.role)
        if drift.duplicate_remote_names:
            forced_local_roles.add(drift.role)
        for remote_name in (
            drift.missing_remote_names + drift.not_ready_remote_names
        ):
            remote_identity = identity_by_name.get(remote_name)
            if remote_identity is None:
                # The local member has a stale or malformed projection that
                # cannot be mapped safely to a target member.
                forced_local_roles.add(drift.role)
            elif remote_identity.role == drift.role:
                forced_local_roles.add(drift.role)
            else:
                directed_edges.add((drift.role, remote_identity.role))

    if max_repair_roles <= 0:
        raise ProbeError("max_repair_roles must be positive")
    remote_incidence: Counter[str] = Counter(
        remote_role for _, remote_role in directed_edges
    )
    cover_edges = {
        tuple(sorted(edge, key=role_sort_key)) for edge in directed_edges
    }
    uncovered_edges = {
        edge
        for edge in cover_edges
        if edge[0] not in forced_local_roles
        and edge[1] not in forced_local_roles
    }
    budget = max_repair_roles - len(forced_local_roles)
    cover = None
    minimum_additional_roles = None
    if budget >= 0:
        lower_bound = _matching_lower_bound(uncovered_edges)
        for limit in range(lower_bound, budget + 1):
            cover = _find_vertex_cover(
                uncovered_edges,
                limit,
                remote_incidence,
            )
            if cover is not None:
                minimum_additional_roles = len(cover)
                break

    cover_within_limit = cover is not None
    if cover is None:
        # All endpoints always cover every edge and make the unsafe breadth
        # explicit in the summary. The caller must not mutate when bounded=false.
        cover = {role for edge in uncovered_edges for role in edge}
    selected_roles = forced_local_roles | cover

    return {
        "directed_edge_count": len(directed_edges),
        "cover_edge_count": len(cover_edges),
        "forced_local_roles": sorted(forced_local_roles, key=role_sort_key),
        "max_repair_roles": max_repair_roles,
        "minimum_additional_roles": minimum_additional_roles,
        "cover_within_limit": cover_within_limit,
        "repair_roles": sorted(selected_roles, key=role_sort_key),
    }


def _matching_lower_bound(edges: Set[Tuple[str, str]]) -> int:
    """Return a deterministic maximal-matching lower bound."""

    matched: Set[str] = set()
    count = 0
    for left, right in sorted(
        edges,
        key=lambda edge: (role_sort_key(edge[0]), role_sort_key(edge[1])),
    ):
        if left in matched or right in matched:
            continue
        matched.update((left, right))
        count += 1
    return count


def _find_vertex_cover(
    edges: Set[Tuple[str, str]],
    limit: int,
    remote_incidence: Counter[str],
) -> Optional[Set[str]]:
    """Find one deterministic vertex cover using at most ``limit`` roles."""

    def search(
        remaining_edges: Set[Tuple[str, str]],
        remaining_limit: int,
    ) -> Optional[Set[str]]:
        selected: Set[str] = set()
        while remaining_edges:
            if remaining_limit <= 0:
                return None
            degree: Counter[str] = Counter()
            for left, right in remaining_edges:
                degree[left] += 1
                degree[right] += 1
            forced = [
                role for role, count in degree.items() if count > remaining_limit
            ]
            if not forced:
                break
            chosen = sorted(
                forced,
                key=lambda role: (
                    -degree[role],
                    -remote_incidence[role],
                    role_sort_key(role),
                ),
            )[0]
            selected.add(chosen)
            remaining_limit -= 1
            remaining_edges = {
                edge for edge in remaining_edges if chosen not in edge
            }

        if not remaining_edges:
            return selected
        if len(remaining_edges) > remaining_limit * remaining_limit:
            return None
        if _matching_lower_bound(remaining_edges) > remaining_limit:
            return None

        degree = Counter()
        for left, right in remaining_edges:
            degree[left] += 1
            degree[right] += 1
        branch_edge = sorted(
            remaining_edges,
            key=lambda edge: (
                -max(degree[edge[0]], degree[edge[1]]),
                -min(degree[edge[0]], degree[edge[1]]),
                role_sort_key(edge[0]),
                role_sort_key(edge[1]),
            ),
        )[0]
        branch_roles = sorted(
            branch_edge,
            key=lambda role: (
                -degree[role],
                -remote_incidence[role],
                role_sort_key(role),
            ),
        )
        for role in branch_roles:
            result = search(
                {edge for edge in remaining_edges if role not in edge},
                remaining_limit - 1,
            )
            if result is not None:
                return selected | {role} | result
        return None

    return search(set(edges), limit)


def repair_roles_for_drift(
    drifts: Iterable[ClusterDrift],
    identity_by_name: Dict[str, ClusterIdentity],
    max_repair_roles: int = 20,
) -> List[str]:
    """Map live peer drift to a compact safe Fleet member repair set."""

    return repair_selection_for_drift(
        drifts,
        identity_by_name,
        max_repair_roles,
    )["repair_roles"]


def write_json_atomic(path: str, payload: dict) -> None:
    """Write JSON without exposing a partial summary to artifact collection."""

    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    temporary = f"{path}.tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(temporary, path)


def write_roles_atomic(path: str, roles: Iterable[str]) -> None:
    """Write one repair role per line atomically."""

    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    temporary = f"{path}.tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        for role in roles:
            handle.write(f"{role}\n")
    os.replace(temporary, path)


def probe(
    clusters: List[Cluster],
    *,
    runner: Runner = run_command,
    attempts: int,
    retry_seconds: int,
    command_timeout_seconds: int,
    max_concurrent: int,
) -> Tuple[List[ClusterIdentity], List[ClusterDrift], int]:
    """Probe all identities once, then retry live status until healthy/bounded."""

    def get_identity(cluster: Cluster) -> ClusterIdentity:
        return retry(
            lambda: read_identity(cluster, runner, command_timeout_seconds),
            attempts=3,
            retry_seconds=retry_seconds,
        )  # type: ignore[return-value]

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_concurrent
    ) as executor:
        identities = list(executor.map(get_identity, clusters))

    identity_by_name: Dict[str, ClusterIdentity] = {}
    identity_by_id: Dict[int, ClusterIdentity] = {}
    for identity in identities:
        if identity.cluster_name in identity_by_name:
            other = identity_by_name[identity.cluster_name]
            raise ProbeError(
                "duplicate live Cilium cluster-name "
                f"{identity.cluster_name!r}: {other.role}, {identity.role}"
            )
        if identity.cluster_id in identity_by_id:
            other = identity_by_id[identity.cluster_id]
            raise ProbeError(
                f"duplicate live Cilium cluster-id {identity.cluster_id}: "
                f"{other.role}, {identity.role}"
            )
        identity_by_name[identity.cluster_name] = identity
        identity_by_id[identity.cluster_id] = identity

    identity_by_role = {identity.role: identity for identity in identities}
    last_drifts: List[ClusterDrift] = []
    for round_number in range(1, attempts + 1):
        def get_status(cluster: Cluster) -> Tuple[str, Optional[List[dict]], Optional[str]]:
            try:
                return (
                    cluster.role,
                    read_status(cluster, runner, command_timeout_seconds),
                    None,
                )
            except ProbeError as exc:
                return cluster.role, None, str(exc)

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_concurrent
        ) as executor:
            results = list(executor.map(get_status, clusters))

        drifts: List[ClusterDrift] = []
        for role, remotes, error in results:
            identity = identity_by_role[role]
            if error is not None:
                drifts.append(
                    ClusterDrift(
                        role=role,
                        cluster_name=identity.cluster_name,
                        command_error=error,
                    )
                )
            else:
                assert remotes is not None
                drift = analyze_status(identity, remotes, identity_by_name)
                if not drift.healthy:
                    drifts.append(drift)
        last_drifts = sorted(drifts, key=lambda drift: role_sort_key(drift.role))
        if not last_drifts:
            return identities, [], round_number
        if round_number < attempts:
            time.sleep(retry_seconds)
    return identities, last_drifts, attempts


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clusters", required=True)
    parser.add_argument("--summary-file", required=True)
    parser.add_argument("--repair-roles-file", required=True)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--retry-seconds", type=int, default=15)
    parser.add_argument("--command-timeout-seconds", type=int, default=30)
    parser.add_argument("--max-concurrent", type=int, default=10)
    parser.add_argument("--max-repair-roles", type=int, default=20)
    args = parser.parse_args(argv)
    if args.attempts <= 0:
        parser.error("--attempts must be positive")
    if args.retry_seconds < 0:
        parser.error("--retry-seconds must be non-negative")
    if args.command_timeout_seconds <= 0:
        parser.error("--command-timeout-seconds must be positive")
    if args.max_concurrent <= 0:
        parser.error("--max-concurrent must be positive")
    if args.max_repair_roles <= 0:
        parser.error("--max-repair-roles must be positive")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the live overlay probe."""

    args = parse_args(argv)
    started_at = utc_now()
    try:
        clusters = load_clusters(args.clusters)
        identities, drifts, rounds = probe(
            clusters,
            attempts=args.attempts,
            retry_seconds=args.retry_seconds,
            command_timeout_seconds=args.command_timeout_seconds,
            max_concurrent=args.max_concurrent,
        )
        identity_by_name = {
            identity.cluster_name: identity for identity in identities
        }
        repair_selection = repair_selection_for_drift(
            drifts,
            identity_by_name,
            args.max_repair_roles,
        )
        repair_roles = repair_selection["repair_roles"]
        healthy = not drifts
        summary = {
            "schema_version": 1,
            "started_at": started_at,
            "finished_at": utc_now(),
            "healthy": healthy,
            "cluster_count": len(clusters),
            "rounds": rounds,
            "identities": [asdict(identity) for identity in identities],
            "drift": [asdict(drift) for drift in drifts],
            "repair_selection": repair_selection,
            "repair_roles": repair_roles,
        }
        write_json_atomic(args.summary_file, summary)
        write_roles_atomic(args.repair_roles_file, repair_roles)
        if healthy:
            print(
                f"Live ClusterMesh overlay healthy on {len(clusters)} clusters "
                f"after {rounds} round(s)."
            )
            return 0
        if not repair_roles:
            print(
                "Live ClusterMesh drift was detected but no safe repair role "
                "could be derived.",
                file=sys.stderr,
            )
            return 1
        if not repair_selection["cover_within_limit"]:
            print(
                "Live ClusterMesh drift has no repair cover within "
                f"{args.max_repair_roles} member(s); refusing Fleet mutation.",
                file=sys.stderr,
            )
            return 1
        print(
            "Live ClusterMesh drift requires member repair: "
            + " ".join(repair_roles),
            file=sys.stderr,
        )
        return 2
    except ProbeError as exc:
        summary = {
            "schema_version": 1,
            "started_at": started_at,
            "finished_at": utc_now(),
            "healthy": False,
            "fatal_error": str(exc),
            "repair_roles": [],
        }
        write_json_atomic(args.summary_file, summary)
        write_roles_atomic(args.repair_roles_file, [])
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
