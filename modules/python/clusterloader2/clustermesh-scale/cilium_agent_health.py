#!/usr/bin/env python3
"""Require every Cilium agent to have an exact healthy ClusterMesh view."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Optional, Sequence, Set


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

import preserved_live_overlay as overlay  # pylint: disable=wrong-import-position


def inspect_agents(
    statuses: Sequence[overlay.CiliumAgentStatus],
    expected_remote_count: int,
    expected_remote_names: Set[str],
) -> dict:
    """Return structured health for every Cilium agent."""

    agents = []
    healthy_count = 0
    for status in statuses:
        names = []
        not_ready = []
        invalid_name_count = 0
        for remote in status.remotes:
            name = remote.get("name") if isinstance(remote, dict) else None
            if not isinstance(name, str) or not name:
                name = "<missing>"
                invalid_name_count += 1
            names.append(name)
            if not isinstance(remote, dict) or not overlay.remote_is_ready(remote):
                not_ready.append(name)
        actual_names = set(names)
        duplicate_names = sorted(
            {name for name in names if names.count(name) > 1}
        )
        missing_names = sorted(expected_remote_names - actual_names)
        unexpected_names = sorted(actual_names - expected_remote_names)
        healthy = (
            len(status.remotes) == expected_remote_count
            and len(set(names)) == expected_remote_count
            and invalid_name_count == 0
            and not not_ready
            and not duplicate_names
            and not missing_names
            and not unexpected_names
        )
        if healthy:
            healthy_count += 1
        agents.append(
            {
                "pod_name": status.pod_name,
                "node_name": status.node_name,
                "healthy": healthy,
                "remote_count": len(status.remotes),
                "ready_remote_count": len(status.remotes) - len(not_ready),
                "not_ready_remote_names": sorted(set(not_ready)),
                "duplicate_remote_names": duplicate_names,
                "missing_remote_names": missing_names,
                "unexpected_remote_names": unexpected_names,
                "invalid_remote_name_count": invalid_name_count,
            }
        )
    return {
        "healthy": bool(agents) and healthy_count == len(agents),
        "cilium_agent_count": len(agents),
        "healthy_agent_count": healthy_count,
        "expected_remote_count": expected_remote_count,
        "expected_remote_names": sorted(expected_remote_names),
        "agents": agents,
    }


def load_expected_remote_names(
    path: str,
    role: str,
    expected_remote_count: int,
) -> Set[str]:
    """Load exact expected names and exclude the local role."""

    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise overlay.ProbeError(
            f"unable to read Cilium identity inventory {path}: {exc}"
        ) from exc
    if not isinstance(payload, list) or not payload:
        raise overlay.ProbeError("Cilium identity inventory must be a non-empty array")
    by_role = {}
    names = set()
    ids = set()
    for row in payload:
        if not isinstance(row, dict):
            raise overlay.ProbeError("malformed Cilium identity inventory row")
        row_role = row.get("role")
        cluster_name = row.get("cluster_name")
        cluster_id = row.get("cluster_id")
        fields_valid = (
            isinstance(row_role, str)
            and bool(row_role)
            and isinstance(cluster_name, str)
            and bool(cluster_name)
            and isinstance(cluster_id, int)
            and cluster_id > 0
        )
        unique = fields_valid and (
            row_role not in by_role
            and cluster_name not in names
            and cluster_id not in ids
        )
        if (
            not fields_valid
            or not unique
        ):
            raise overlay.ProbeError("Cilium identity inventory is not exact")
        by_role[row_role] = cluster_name
        names.add(cluster_name)
        ids.add(cluster_id)
    if role not in by_role:
        raise overlay.ProbeError(f"Cilium identity inventory has no role {role}")
    expected = names - {by_role[role]}
    if len(expected) != expected_remote_count:
        raise overlay.ProbeError(
            f"{role}: identity inventory provides {len(expected)} remotes, "
            f"expected {expected_remote_count}"
        )
    return expected


def probe(
    *,
    role: str,
    kubeconfig: str,
    expected_remote_count: int,
    expected_remote_names: Set[str],
    attempts: int,
    retry_seconds: int,
    command_timeout_seconds: int,
    runner: overlay.Runner = overlay.run_command,
) -> dict:
    """Probe all agents with a bounded retry budget."""

    cluster = overlay.Cluster(
        name=role,
        resource_group="",
        role=role,
        kubeconfig=kubeconfig,
    )
    last_summary = {
        "healthy": False,
        "role": role,
        "attempts": 0,
        "expected_remote_count": expected_remote_count,
        "cilium_agent_count": 0,
        "healthy_agent_count": 0,
        "agents": [],
    }
    for attempt in range(1, attempts + 1):
        try:
            statuses = overlay.read_status(
                cluster,
                runner,
                command_timeout_seconds,
            )
            last_summary = inspect_agents(
                statuses,
                expected_remote_count,
                expected_remote_names,
            )
            last_summary.update({"role": role, "attempts": attempt})
            if last_summary["healthy"]:
                return last_summary
            unhealthy = [
                (
                    f"{agent['pod_name']}="
                    f"{agent['ready_remote_count']}/{expected_remote_count}"
                )
                for agent in last_summary["agents"]
                if not agent["healthy"]
            ]
            print(
                f"{role}: Cilium agents not converged on attempt "
                f"{attempt}/{attempts}: {' '.join(unhealthy)}",
                flush=True,
            )
        except overlay.ProbeError as exc:
            last_summary = {
                "healthy": False,
                "role": role,
                "attempts": attempt,
                "expected_remote_count": expected_remote_count,
                "cilium_agent_count": 0,
                "healthy_agent_count": 0,
                "agents": [],
                "fatal_error": str(exc),
            }
            print(
                f"{role}: Cilium all-agent probe failed on attempt "
                f"{attempt}/{attempts}: {exc}",
                flush=True,
            )
        if attempt < attempts and retry_seconds > 0:
            time.sleep(retry_seconds)
    return last_summary


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role", required=True)
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument("--expected-remote-count", type=int, required=True)
    parser.add_argument("--identity-inventory", required=True)
    parser.add_argument("--attempts", type=int, default=1)
    parser.add_argument("--retry-seconds", type=int, default=15)
    parser.add_argument("--command-timeout-seconds", type=int, default=45)
    parser.add_argument("--summary-file", required=True)
    args = parser.parse_args(argv)
    if args.expected_remote_count < 0:
        parser.error("--expected-remote-count must be non-negative")
    if args.attempts <= 0:
        parser.error("--attempts must be positive")
    if args.retry_seconds < 0:
        parser.error("--retry-seconds must be non-negative")
    if args.command_timeout_seconds <= 0:
        parser.error("--command-timeout-seconds must be positive")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the all-agent health probe."""

    args = parse_args(argv)
    try:
        expected_remote_names = load_expected_remote_names(
            args.identity_inventory,
            args.role,
            args.expected_remote_count,
        )
        summary = probe(
            role=args.role,
            kubeconfig=args.kubeconfig,
            expected_remote_count=args.expected_remote_count,
            expected_remote_names=expected_remote_names,
            attempts=args.attempts,
            retry_seconds=args.retry_seconds,
            command_timeout_seconds=args.command_timeout_seconds,
        )
    except overlay.ProbeError as exc:
        summary = {
            "healthy": False,
            "role": args.role,
            "attempts": 0,
            "expected_remote_count": args.expected_remote_count,
            "expected_remote_names": [],
            "cilium_agent_count": 0,
            "healthy_agent_count": 0,
            "agents": [],
            "fatal_error": str(exc),
        }
    summary["generated_at"] = overlay.utc_now()
    overlay.write_json_atomic(args.summary_file, summary)
    if summary["healthy"]:
        print(
            f"{args.role}: all {summary['cilium_agent_count']} Cilium agent(s) "
            f"have {args.expected_remote_count}/{args.expected_remote_count} "
            "ready remotes.",
            flush=True,
        )
        return 0
    print(
        f"{args.role}: only {summary['healthy_agent_count']}/"
        f"{summary['cilium_agent_count']} Cilium agents are healthy.",
        file=sys.stderr,
        flush=True,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
