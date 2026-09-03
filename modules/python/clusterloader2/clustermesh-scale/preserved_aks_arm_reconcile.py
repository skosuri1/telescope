#!/usr/bin/env python3
"""Clear stale AKS ARM failure states after healthy Fleet mesh formation."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
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


def _pool_is_stable(pool: object) -> bool:
    if not isinstance(pool, dict):
        return False
    power = pool.get("powerState")
    power_state = power.get("code") if isinstance(power, dict) else power
    return (
        pool.get("provisioningState") == "Succeeded"
        and power_state in (None, "", "Running")
    )


def validate_cluster_inventory(
    payload: object,
    *,
    expected_count: int,
    region: str,
    max_repair_clusters: int,
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
        if state not in ("Succeeded", "Failed"):
            raise ReconcileError(f"{role}: unsafe provisioningState={state}")
        power = row.get("powerState")
        power_state = str(
            power.get("code") if isinstance(power, dict) else power or ""
        )
        if power_state not in ("", "Running"):
            raise ReconcileError(f"{role}: unsafe powerState={power_state}")
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
        if not isinstance(pools, list) or not pools or not all(
            _pool_is_stable(pool) for pool in pools
        ):
            raise ReconcileError(f"{role}: one or more node pools are not stable")
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
) -> None:
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

    status = runner(
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
        ],
        query_timeout_seconds,
    )
    expected = (
        f"ClusterMesh: {expected_remote_count}/{expected_remote_count} "
        "remote clusters ready"
    )
    if expected not in status:
        raise ReconcileError(
            f"{cluster.role}: live Cilium status does not contain {expected!r}"
        )


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

        initial_payload = parse_json(
            run_read_with_retries(
                [
                    "az",
                    "aks",
                    "list",
                    "--resource-group",
                    args.resource_group,
                    "--output",
                    "json",
                    "--only-show-errors",
                ],
                run_command,
                timeout_seconds=args.inventory_timeout_seconds,
                attempts=args.inventory_attempts,
                retry_seconds=args.inventory_retry_seconds,
            ),
            "AKS inventory",
        )
        clusters, failed = validate_cluster_inventory(
            initial_payload,
            expected_count=args.expected_count,
            region=args.expected_region,
            max_repair_clusters=args.max_repair_clusters,
        )
        summary["initial_failed_roles"] = [cluster.role for cluster in failed]

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

        final_payload = parse_json(
            run_read_with_retries(
                [
                    "az",
                    "aks",
                    "list",
                    "--resource-group",
                    args.resource_group,
                    "--output",
                    "json",
                    "--only-show-errors",
                ],
                run_command,
                timeout_seconds=args.inventory_timeout_seconds,
                attempts=args.inventory_attempts,
                retry_seconds=args.inventory_retry_seconds,
            ),
            "final AKS inventory",
        )
        final_clusters, final_failed = validate_cluster_inventory(
            final_payload,
            expected_count=args.expected_count,
            region=args.expected_region,
            max_repair_clusters=args.max_repair_clusters,
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
