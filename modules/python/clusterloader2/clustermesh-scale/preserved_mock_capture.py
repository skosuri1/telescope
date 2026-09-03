#!/usr/bin/env python3
"""Capture an exact cross-run baseline for a preserved ClusterMesh mock layer."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Sequence


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

import preserved_live_overlay as live_overlay  # pylint: disable=wrong-import-position


class CaptureError(Exception):
    """Expected fail-closed capture error."""


@dataclass(frozen=True)
class Cluster:
    """Cluster inventory needed for one capture worker."""

    name: str
    resource_group: str
    role: str
    kubeconfig: str


Runner = Callable[[Sequence[str], int], str]


def utc_now() -> str:
    """Return current UTC time in RFC3339 format."""

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_command(args: Sequence[str], timeout_seconds: int) -> str:
    """Run one bounded command and return stdout."""

    try:
        completed = subprocess.run(
            list(args),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise CaptureError(
            f"command timed out after {timeout_seconds}s: {' '.join(args)}"
        ) from exc
    except OSError as exc:
        raise CaptureError(f"unable to execute {args[0]}: {exc}") from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise CaptureError(
            f"command failed (exit={completed.returncode}): {' '.join(args)}: "
            f"{detail[:2000]}"
        )
    return completed.stdout


def parse_json(output: str, description: str) -> object:
    """Parse JSON output with an actionable error."""

    try:
        return json.loads(output)
    except json.JSONDecodeError as exc:
        raise CaptureError(f"invalid {description} JSON: {exc}") from exc


def role_number(role: str) -> int:
    """Return a numeric mesh role suffix."""

    prefix = "mesh-"
    if not role.startswith(prefix) or not role[len(prefix) :].isdigit():
        raise CaptureError(f"invalid mesh role: {role!r}")
    number = int(role[len(prefix) :])
    if number <= 0:
        raise CaptureError(f"invalid mesh role: {role!r}")
    return number


def load_clusters(path: str, expected_count: int) -> List[Cluster]:
    """Load and validate an exact mesh-1..mesh-N inventory."""

    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise CaptureError(f"unable to read cluster inventory {path}: {exc}") from exc
    if not isinstance(payload, list) or len(payload) != expected_count:
        actual = len(payload) if isinstance(payload, list) else "non-array"
        raise CaptureError(
            f"expected {expected_count} cluster inventory entries, got {actual}"
        )

    clusters = []
    seen_roles = set()
    for row in payload:
        if not isinstance(row, dict):
            raise CaptureError("malformed cluster inventory entry")
        role = row.get("role")
        name = row.get("name")
        resource_group = row.get("rg")
        if not all(
            isinstance(value, str) and value
            for value in (role, name, resource_group)
        ):
            raise CaptureError("cluster inventory entry has missing fields")
        number = role_number(role)
        if name != f"clustermesh-{number}":
            raise CaptureError(
                f"{role}: expected cluster name clustermesh-{number}, got {name}"
            )
        if role in seen_roles:
            raise CaptureError(f"duplicate cluster role: {role}")
        kubeconfig = row.get("kubeconfig") or os.path.join(
            os.path.expanduser("~"), ".kube", f"{role}.config"
        )
        if not isinstance(kubeconfig, str) or not kubeconfig:
            raise CaptureError(f"{role}: invalid kubeconfig path")
        clusters.append(
            Cluster(
                name=name,
                resource_group=resource_group,
                role=role,
                kubeconfig=kubeconfig,
            )
        )
        seen_roles.add(role)
    if sorted(role_number(cluster.role) for cluster in clusters) != list(
        range(1, expected_count + 1)
    ):
        raise CaptureError(
            f"cluster roles are not exactly mesh-1..mesh-{expected_count}"
        )
    return sorted(clusters, key=lambda cluster: role_number(cluster.role))


def load_state_metadata(
    state_dir: str, run_id: str, expected_mock_count: int
) -> dict:
    """Validate persisted mock desired-state metadata and required files."""

    metadata_path = os.path.join(state_dir, "metadata.json")
    try:
        with open(metadata_path, "r", encoding="utf-8") as handle:
            metadata = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise CaptureError(f"invalid state metadata {metadata_path}: {exc}") from exc
    if not isinstance(metadata, dict):
        raise CaptureError(f"state metadata is not an object: {metadata_path}")
    if metadata.get("run_id") != run_id:
        raise CaptureError(
            f"state run_id mismatch in {metadata_path}: "
            f"{metadata.get('run_id')!r}"
        )
    if metadata.get("node_count") != expected_mock_count:
        raise CaptureError(
            f"state node_count mismatch in {metadata_path}: "
            f"{metadata.get('node_count')!r}"
        )
    required = [
        metadata.get("node_manifest", "nodes.yaml"),
        metadata.get("agent_manifest", "agents.yaml"),
        metadata.get("agent_controller_manifest", "agent-controller.yaml"),
        "support/kwok-controller.yaml",
        "support/stage-fast.yaml",
        "support/kwok-apf.yaml",
        "support/rbac.yaml",
    ]
    for relative_path in required:
        if not isinstance(relative_path, str) or not os.path.isfile(
            os.path.join(state_dir, relative_path)
        ):
            raise CaptureError(
                f"missing persisted desired-state file in {state_dir}: "
                f"{relative_path!r}"
            )
    return metadata


def file_digests(root: str) -> Dict[str, str]:
    """Hash every regular desired-state file under a role directory."""

    digests = {}
    for directory, _, filenames in os.walk(root):
        for filename in sorted(filenames):
            path = os.path.join(directory, filename)
            if not os.path.isfile(path):
                continue
            relative = os.path.relpath(path, root)
            digest = hashlib.sha256()
            with open(path, "rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    digest.update(chunk)
            digests[relative] = digest.hexdigest()
    if not digests:
        raise CaptureError(f"no desired-state files found under {root}")
    return digests


def parse_node_identities(payload: object, expected_count: int) -> Dict[str, str]:
    """Return exact KWOK Node name-to-UID identities."""

    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise CaptureError("malformed KWOK Node response")
    identities = {}
    for item in payload["items"]:
        metadata = item.get("metadata") if isinstance(item, dict) else None
        name = metadata.get("name") if isinstance(metadata, dict) else None
        uid = metadata.get("uid") if isinstance(metadata, dict) else None
        if not isinstance(name, str) or not isinstance(uid, str) or not uid:
            raise CaptureError("KWOK Node has no name or UID")
        if name in identities:
            raise CaptureError(f"duplicate KWOK Node: {name}")
        identities[name] = uid
    expected = {f"kwok-node-{index}" for index in range(expected_count)}
    if set(identities) != expected:
        missing = sorted(expected - set(identities))[:10]
        extra = sorted(set(identities) - expected)[:10]
        raise CaptureError(
            f"KWOK Node inventory is not exact: missing={missing} extra={extra}"
        )
    return identities


def parse_agent_identities(payload: object, expected_count: int) -> Dict[str, str]:
    """Return exact healthy mock-agent Pod name-to-UID identities."""

    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise CaptureError("malformed mock-agent Pod response")
    identities = {}
    for item in payload["items"]:
        metadata = item.get("metadata") if isinstance(item, dict) else None
        status = item.get("status") if isinstance(item, dict) else None
        name = metadata.get("name") if isinstance(metadata, dict) else None
        uid = metadata.get("uid") if isinstance(metadata, dict) else None
        containers = (
            status.get("containerStatuses") if isinstance(status, dict) else None
        )
        ready = (
            isinstance(containers, list)
            and bool(containers)
            and all(
                isinstance(container, dict) and container.get("ready") is True
                for container in containers
            )
        )
        if not isinstance(name, str) or not isinstance(uid, str) or not uid:
            raise CaptureError("mock-agent Pod has no name or UID")
        if (
            not isinstance(status, dict)
            or status.get("phase") != "Running"
            or not ready
        ):
            raise CaptureError(f"mock-agent Pod is not healthy: {name or 'unknown'}")
        if name in identities:
            raise CaptureError(f"duplicate mock-agent Pod: {name}")
        identities[name] = uid
    expected = {f"kwok-node-{index}" for index in range(expected_count)}
    if set(identities) != expected:
        missing = sorted(expected - set(identities))[:10]
        extra = sorted(set(identities) - expected)[:10]
        raise CaptureError(
            f"mock-agent inventory is not exact: missing={missing} extra={extra}"
        )
    return identities


def validate_cilium_status(
    payload: object,
    expected_remote_count: int,
    expected_remote_names: Optional[Sequence[str]] = None,
) -> None:
    """Require exact structured live ClusterMesh readiness."""

    mesh = payload.get("cluster-mesh") if isinstance(payload, dict) else None
    remotes = mesh.get("clusters") if isinstance(mesh, dict) else None
    if not isinstance(remotes, list) or len(remotes) != expected_remote_count:
        raise CaptureError(
            f"expected {expected_remote_count} Cilium remotes, got "
            f"{len(remotes) if isinstance(remotes, list) else 'invalid'}"
        )
    unhealthy = []
    names = []
    for remote in remotes:
        if not isinstance(remote, dict):
            raise CaptureError("malformed Cilium remote status entry")
        name = remote.get("name")
        if not isinstance(name, str) or not name:
            raise CaptureError("Cilium remote status entry has no name")
        names.append(name)
        config = remote.get("config")
        if (
            remote.get("ready") is not True
            or remote.get("connected") is not True
            or not isinstance(config, dict)
            or config.get("required") is not True
            or config.get("retrieved") is not True
        ):
            unhealthy.append(name)
    if len(set(names)) != len(names):
        raise CaptureError("Cilium remote status contains duplicate names")
    if expected_remote_names is not None:
        expected_names = set(expected_remote_names)
        if len(expected_names) != expected_remote_count or set(names) != expected_names:
            missing = sorted(expected_names - set(names))[:10]
            extra = sorted(set(names) - expected_names)[:10]
            raise CaptureError(
                f"Cilium remote names are not exact: missing={missing} extra={extra}"
            )
    if unhealthy:
        raise CaptureError(
            "unhealthy Cilium remotes: " + " ".join(sorted(unhealthy))
        )


def probe_cluster(
    cluster: Cluster,
    *,
    state_root: str,
    run_id: str,
    expected_cluster_count: int,
    expected_mock_count: int,
    command_timeout_seconds: int,
    runner: Runner,
    expected_remote_names: Optional[Sequence[str]] = None,
) -> dict:
    """Capture one cluster after validating exact live and persisted state."""

    state_dir = os.path.join(state_root, cluster.role)
    metadata = load_state_metadata(state_dir, run_id, expected_mock_count)
    base = [
        "kubectl",
        "--kubeconfig",
        cluster.kubeconfig,
        f"--request-timeout={command_timeout_seconds}s",
    ]
    nodes = parse_node_identities(
        parse_json(
            runner(
                base + ["get", "nodes", "-l", "type=kwok", "-o", "json"],
                command_timeout_seconds,
            ),
            f"{cluster.role} KWOK Nodes",
        ),
        expected_mock_count,
    )
    agents = parse_agent_identities(
        parse_json(
            runner(
                base
                + [
                    "-n",
                    "mock-clustermesh",
                    "get",
                    "pods",
                    "-l",
                    "app=mock-cilium-agent",
                    "-o",
                    "json",
                ],
                command_timeout_seconds,
            ),
            f"{cluster.role} mock-agent Pods",
        ),
        expected_mock_count,
    )
    statefulset = parse_json(
        runner(
            base
            + [
                "-n",
                "mock-clustermesh",
                "get",
                "statefulset",
                "kwok-node",
                "-o",
                "json",
            ],
            command_timeout_seconds,
        ),
        f"{cluster.role} mock-agent StatefulSet",
    )
    ready_replicas = (
        statefulset.get("status", {}).get("readyReplicas")
        if isinstance(statefulset, dict)
        else None
    )
    if ready_replicas != expected_mock_count:
        raise CaptureError(
            f"{cluster.role}: StatefulSet readyReplicas={ready_replicas}, "
            f"expected {expected_mock_count}"
        )
    try:
        cilium_agents = live_overlay.read_status(
            live_overlay.Cluster(
                name=cluster.name,
                resource_group=cluster.resource_group,
                role=cluster.role,
                kubeconfig=cluster.kubeconfig,
            ),
            runner,
            command_timeout_seconds,
        )
    except live_overlay.ProbeError as exc:
        raise CaptureError(str(exc)) from exc
    for agent in cilium_agents:
        validate_cilium_status(
            {"cluster-mesh": {"clusters": agent.remotes}},
            expected_cluster_count - 1,
            expected_remote_names,
        )
    return {
        "name": cluster.name,
        "resource_group": cluster.resource_group,
        "role": cluster.role,
        "cluster_name": metadata.get("cluster_name"),
        "cluster_id": metadata.get("cluster_id"),
        "cilium_agents": [
            {"pod_name": agent.pod_name, "node_name": agent.node_name}
            for agent in cilium_agents
        ],
        "node_uids": nodes,
        "agent_uids": agents,
        "desired_state_sha256": file_digests(state_dir),
    }


def validate_aks_inventory(payload: object, clusters: List[Cluster]) -> Dict[str, str]:
    """Require exact healthy AKS IDs for all cluster roles."""

    if not isinstance(payload, list) or len(payload) != len(clusters):
        raise CaptureError("AKS resource inventory count mismatch")
    expected = {cluster.role: cluster.name for cluster in clusters}
    resource_ids = {}
    for row in payload:
        if not isinstance(row, dict):
            raise CaptureError("malformed AKS resource inventory entry")
        role = (row.get("tags") or {}).get("role")
        name = row.get("name")
        resource_id = row.get("id")
        if (
            role not in expected
            or name != expected[role]
            or not isinstance(resource_id, str)
            or not resource_id
            or row.get("provisioningState") != "Succeeded"
        ):
            raise CaptureError(f"invalid AKS resource inventory entry: {name}")
        resource_ids[str(role)] = resource_id
    if set(resource_ids) != set(expected):
        raise CaptureError("AKS resource roles are not exact")
    return resource_ids


def write_json_atomic(path: str, payload: dict) -> None:
    """Write a JSON file atomically."""

    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    temporary = f"{path}.tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(temporary, path)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clusters", required=True)
    parser.add_argument("--state-root", required=True)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expected-cluster-count", type=int, required=True)
    parser.add_argument("--expected-mock-count", type=int, required=True)
    parser.add_argument("--max-concurrent", type=int, default=8)
    parser.add_argument("--command-timeout-seconds", type=int, default=120)
    args = parser.parse_args(argv)
    for name in (
        "expected_cluster_count",
        "expected_mock_count",
        "max_concurrent",
        "command_timeout_seconds",
    ):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Capture exact identities and desired state for the preserved run."""

    args = parse_args(argv)
    summary_path = os.path.join(args.artifact_dir, "summary.json")
    started_at = utc_now()
    try:
        clusters = load_clusters(args.clusters, args.expected_cluster_count)
        resource_groups = {cluster.resource_group for cluster in clusters}
        if len(resource_groups) != 1 or args.run_id not in resource_groups:
            raise CaptureError(
                f"cluster resource group does not exactly match run_id {args.run_id}"
            )
        aks = parse_json(
            run_command(
                [
                    "az",
                    "aks",
                    "list",
                    "--resource-group",
                    args.run_id,
                    "--output",
                    "json",
                    "--only-show-errors",
                ],
                600,
            ),
            "AKS resource inventory",
        )
        resource_ids = validate_aks_inventory(aks, clusters)

        def capture_one(cluster: Cluster) -> dict:
            result = probe_cluster(
                cluster,
                state_root=args.state_root,
                run_id=args.run_id,
                expected_cluster_count=args.expected_cluster_count,
                expected_mock_count=args.expected_mock_count,
                command_timeout_seconds=args.command_timeout_seconds,
                runner=run_command,
            )
            result["resource_id"] = resource_ids[cluster.role]
            print(
                f"{cluster.role}: captured {args.expected_mock_count} KWOK "
                "and agent identities"
            )
            return result

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=args.max_concurrent
        ) as executor:
            captured = list(executor.map(capture_one, clusters))
        captured.sort(key=lambda row: role_number(row["role"]))

        state_destination = os.path.join(args.artifact_dir, "desired-state")
        if os.path.exists(state_destination):
            raise CaptureError(
                f"artifact desired-state path already exists: {state_destination}"
            )
        shutil.copytree(args.state_root, state_destination)
        baseline = {
            "schema_version": 1,
            "captured_at": utc_now(),
            "run_id": args.run_id,
            "cluster_count": args.expected_cluster_count,
            "mock_count_per_cluster": args.expected_mock_count,
            "total_kwok_nodes": (
                args.expected_cluster_count * args.expected_mock_count
            ),
            "total_mock_agents": (
                args.expected_cluster_count * args.expected_mock_count
            ),
            "clusters": captured,
        }
        write_json_atomic(
            os.path.join(args.artifact_dir, "baseline.json"), baseline
        )
        write_json_atomic(
            summary_path,
            {
                "schema_version": 1,
                "started_at": started_at,
                "finished_at": utc_now(),
                "healthy": True,
                "run_id": args.run_id,
                "cluster_count": args.expected_cluster_count,
                "mock_count_per_cluster": args.expected_mock_count,
                "total_kwok_nodes": baseline["total_kwok_nodes"],
                "total_mock_agents": baseline["total_mock_agents"],
                "desired_state_copied": True,
            },
        )
        print(
            "Preserved mock baseline captured: "
            f"{baseline['total_kwok_nodes']} KWOK Nodes and "
            f"{baseline['total_mock_agents']} mock agents."
        )
        return 0
    except CaptureError as exc:
        write_json_atomic(
            summary_path,
            {
                "schema_version": 1,
                "started_at": started_at,
                "finished_at": utc_now(),
                "healthy": False,
                "run_id": args.run_id,
                "fatal_error": str(exc),
            },
        )
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
