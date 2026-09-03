#!/usr/bin/env python3
"""Restore and validate a verified preserved mock layer before workloads."""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, Optional, Sequence


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

import preserved_mock_capture as capture  # pylint: disable=wrong-import-position
import preserved_mock_verify as verify  # pylint: disable=wrong-import-position


class HandoffError(Exception):
    """Expected fail-closed workload handoff error."""


def _load_object(path: str, description: str) -> dict:
    payload = verify.load_json(path, description)
    if not isinstance(payload, dict):
        raise HandoffError(f"{description} is not an object")
    return payload


def _index_rows(rows: object, expected_cluster_count: int) -> Dict[str, dict]:
    if not isinstance(rows, list) or len(rows) != expected_cluster_count:
        raise HandoffError("verification live-post cluster count mismatch")
    indexed = {}
    for row in rows:
        role = row.get("role") if isinstance(row, dict) else None
        if not isinstance(role, str) or role in indexed:
            raise HandoffError("verification live-post has invalid or duplicate role")
        indexed[role] = row
    expected_roles = {
        f"mesh-{index}" for index in range(1, expected_cluster_count + 1)
    }
    if set(indexed) != expected_roles:
        raise HandoffError("verification live-post roles are not exact")
    return indexed


def validate_verification_artifact(
    verification_dir: str,
    baseline_by_role: Dict[str, dict],
    *,
    run_id: str,
    baseline_build_id: int,
    expected_cluster_count: int,
    expected_mock_count: int,
    expected_pool_count: int,
) -> dict:
    """Validate that a successful proof artifact matches the baseline."""

    summary = _load_object(
        os.path.join(verification_dir, "summary.json"),
        "verification summary",
    )
    proof = _load_object(
        os.path.join(verification_dir, "verification.json"),
        "verification proof",
    )
    live_post = _load_object(
        os.path.join(verification_dir, "live-post.json"),
        "verification live-post snapshot",
    )
    expected_summary = {
        "healthy": True,
        "identity_verification_healthy": True,
        "cross_cluster_data_path_valid": True,
        "no_cl2_scenarios_run": True,
        "stage": "complete",
        "run_id": run_id,
        "baseline_build_id": baseline_build_id,
        "cluster_count": expected_cluster_count,
        "pool_count": expected_pool_count,
        "fleet_connected_count": expected_cluster_count,
        "total_kwok_nodes": expected_cluster_count * expected_mock_count,
        "total_mock_agents": expected_cluster_count * expected_mock_count,
    }
    for key, expected in expected_summary.items():
        if summary.get(key) != expected:
            raise HandoffError(
                f"verification summary {key}={summary.get(key)!r}, "
                f"expected {expected!r}"
            )
    if proof.get("run_id") != run_id or proof.get(
        "baseline_build_id"
    ) != baseline_build_id:
        raise HandoffError("verification proof identity does not match workload")
    reconcile = proof.get("reconcile")
    if not isinstance(reconcile, dict) or (
        reconcile.get("success") is not True
        or reconcile.get("healthy_count") != expected_cluster_count
        or reconcile.get("total_clusters") != expected_cluster_count
    ):
        raise HandoffError("verification reconcile result is not exact")
    fault_plan = proof.get("fault_plan")
    fault_results = proof.get("fault_results")
    post_recovery = proof.get("post_recovery")
    if not isinstance(fault_plan, dict) or not isinstance(post_recovery, dict):
        raise HandoffError("verification fault or recovery evidence is missing")
    if (
        post_recovery.get("changed_node_uids")
        != fault_plan.get("total_deleted_nodes")
        or post_recovery.get("changed_agent_uids")
        != fault_plan.get("total_deleted_agent_pods")
    ):
        raise HandoffError("verification changed-UID counts do not match fault plan")
    if (
        not isinstance(fault_results, list)
        or len(fault_results) != len(fault_plan.get("roles") or [])
        or not all(
            isinstance(result, dict) and result.get("success") is True
            for result in fault_results
        )
    ):
        raise HandoffError("verification fault injection was not fully successful")

    platform_after = proof.get("platform_after")
    if not isinstance(platform_after, dict) or (
        platform_after.get("aks_count") != expected_cluster_count
        or platform_after.get("pool_count") != expected_pool_count
        or platform_after.get("fleet_connected_count") != expected_cluster_count
    ):
        raise HandoffError("verification post-platform state is not exact")

    live_by_role = _index_rows(
        live_post.get("clusters"),
        expected_cluster_count,
    )
    for role, baseline in baseline_by_role.items():
        live = live_by_role[role]
        if (
            verify.normalize_resource_id(str(live.get("resource_id") or ""))
            != verify.normalize_resource_id(baseline["resource_id"])
        ):
            raise HandoffError(f"{role}: verification AKS resource ID changed")
        for field in ("cluster_name", "cluster_id", "desired_state_sha256"):
            if live.get(field) != baseline.get(field):
                raise HandoffError(f"{role}: verification {field} mismatch")
        verify.validate_uid_map(
            live.get("node_uids"),
            expected_mock_count,
            f"{role} verification KWOK identities",
        )
        verify.validate_uid_map(
            live.get("agent_uids"),
            expected_mock_count,
            f"{role} verification agent identities",
        )
    try:
        computed_post_recovery = verify.compare_post_recovery(
            baseline_by_role,
            list(live_by_role.values()),
            fault_plan,
        )
    except verify.VerificationError as exc:
        raise HandoffError(
            f"verification live-post UID changes do not match proof evidence: {exc}"
        ) from exc
    if computed_post_recovery != post_recovery:
        raise HandoffError(
            "verification live-post UID changes do not match proof evidence"
        )
    return {
        "healthy": True,
        "verified_cluster_count": expected_cluster_count,
        "verified_kwok_nodes": expected_cluster_count * expected_mock_count,
        "verified_mock_agents": expected_cluster_count * expected_mock_count,
        "fault_roles": list(fault_plan["roles"]),
        "changed_node_uids": post_recovery["changed_node_uids"],
        "changed_agent_uids": post_recovery["changed_agent_uids"],
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", required=True)
    parser.add_argument("--baseline-build-id", type=int, required=True)
    parser.add_argument("--verification-dir", required=True)
    parser.add_argument("--verification-build-id", type=int, required=True)
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
    parser.add_argument("--max-concurrent", type=int, default=8)
    parser.add_argument("--reconcile-concurrent", type=int, default=12)
    parser.add_argument("--command-timeout-seconds", type=int, default=120)
    parser.add_argument("--reconcile-timeout-seconds", type=int, default=3600)
    args = parser.parse_args(argv)
    for name in (
        "baseline_build_id",
        "verification_build_id",
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
    """Restore the verified state and require an exact live pre-suite layer."""

    args = parse_args(argv)
    os.makedirs(args.artifact_dir, exist_ok=True)
    summary_path = os.path.join(args.artifact_dir, "summary.json")
    summary = {
        "schema_version": 1,
        "started_at": verify.utc_now(),
        "finished_at": None,
        "healthy": False,
        "handoff_validation_healthy": False,
        "cross_cluster_data_path_valid": False,
        "stage": "loading_artifact_chain",
        "run_id": args.run_id,
        "baseline_build_id": args.baseline_build_id,
        "verification_build_id": args.verification_build_id,
        "workloads_started": False,
        "mock_redeployed": False,
    }
    verify.write_json_atomic(summary_path, summary)
    stage = "loading_artifact_chain"
    try:
        _, baseline_by_role = verify.load_baseline(
            args.baseline_dir,
            args.run_id,
            args.expected_cluster_count,
            args.expected_mock_count,
        )
        proof_chain = validate_verification_artifact(
            args.verification_dir,
            baseline_by_role,
            run_id=args.run_id,
            baseline_build_id=args.baseline_build_id,
            expected_cluster_count=args.expected_cluster_count,
            expected_mock_count=args.expected_mock_count,
            expected_pool_count=args.expected_pool_count,
        )
        clusters = capture.load_clusters(
            args.clusters,
            args.expected_cluster_count,
        )
        if any(cluster.resource_group != args.run_id for cluster in clusters):
            raise HandoffError("cluster inventory resource group does not match run_id")

        stage = "restoring_desired_state"
        verify.restore_state(
            args.baseline_dir,
            args.state_root,
            baseline_by_role,
            args.run_id,
            args.expected_mock_count,
        )

        stage = "reconciling_pre_suite_layer"
        reconcile = verify.run_reconciler(
            args.reconciler,
            clusters_path=args.clusters,
            state_root=args.state_root,
            run_id=args.run_id,
            expected_mock_count=args.expected_mock_count,
            artifact_dir=args.artifact_dir,
            max_concurrent=args.reconcile_concurrent,
            timeout_seconds=args.reconcile_timeout_seconds,
            attempts=10,
            settle_seconds=30,
            request_timeout_seconds=30,
        )
        if reconcile.get("total_clusters") != args.expected_cluster_count:
            raise HandoffError("pre-suite reconcile cluster count mismatch")

        stage = "validating_live_pre_suite_layer"
        platform = verify.validate_platform_state(
            clusters,
            subscription_id=args.expected_subscription_id,
            run_id=args.run_id,
            expected_pool_count=args.expected_pool_count,
            fleet_name=args.fleet_name,
            profile_name=args.profile_name,
            runner=capture.run_command,
        )
        expected_cilium_names = {
            role: str(row["cluster_name"]) for role, row in baseline_by_role.items()
        }
        live = verify.capture_live(
            clusters,
            state_root=args.state_root,
            run_id=args.run_id,
            expected_cluster_count=args.expected_cluster_count,
            expected_mock_count=args.expected_mock_count,
            max_concurrent=args.max_concurrent,
            command_timeout_seconds=args.command_timeout_seconds,
            resource_ids=platform["resource_ids"],
            expected_cilium_names=expected_cilium_names,
            runner=capture.run_command,
        )
        verify.write_json_atomic(
            os.path.join(args.artifact_dir, "live.json"),
            {
                "captured_at": verify.utc_now(),
                "platform": platform,
                "clusters": live,
            },
        )
        handoff = {
            "schema_version": 1,
            "validated_at": verify.utc_now(),
            "run_id": args.run_id,
            "baseline_build_id": args.baseline_build_id,
            "verification_build_id": args.verification_build_id,
            "artifact_chain": proof_chain,
            "desired_state_roles_restored": len(baseline_by_role),
            "desired_state_files_restored": sum(
                len(row["desired_state_sha256"])
                for row in baseline_by_role.values()
            ),
            "reconcile": {
                "success": reconcile["success"],
                "healthy_count": reconcile["healthy_count"],
                "total_clusters": reconcile["total_clusters"],
            },
            "platform": platform,
            "live_cluster_count": len(live),
            "live_kwok_nodes": args.expected_cluster_count
            * args.expected_mock_count,
            "live_mock_agents": args.expected_cluster_count
            * args.expected_mock_count,
        }
        verify.write_json_atomic(
            os.path.join(args.artifact_dir, "handoff.json"),
            handoff,
        )
        summary.update(
            {
                "healthy": False,
                "handoff_validation_healthy": True,
                "cross_cluster_data_path_valid": False,
                "stage": "awaiting_cross_cluster_data_path",
                "artifact_chain_verified": True,
                "desired_state_roles_restored": len(baseline_by_role),
                "desired_state_files_restored": sum(
                    len(row["desired_state_sha256"])
                    for row in baseline_by_role.values()
                ),
                "reconcile_healthy_count": reconcile["healthy_count"],
                "cluster_count": args.expected_cluster_count,
                "pool_count": args.expected_pool_count,
                "fleet_connected_count": args.expected_cluster_count,
                "live_kwok_nodes": args.expected_cluster_count
                * args.expected_mock_count,
                "live_mock_agents": args.expected_cluster_count
                * args.expected_mock_count,
            }
        )
        verify.write_json_atomic(summary_path, summary)
        print(
            "Verified workload handoff complete: exact desired state restored, "
            f"{reconcile['healthy_count']}/{reconcile['total_clusters']} clusters "
            "reconciled, and the live mock layer is ready; awaiting "
            "cross-cluster data-path validation.",
            flush=True,
        )
        return 0
    except (HandoffError, verify.VerificationError, capture.CaptureError) as exc:
        summary.update(
            {
                "finished_at": verify.utc_now(),
                "healthy": False,
                "stage": stage,
                "fatal_error": str(exc),
            }
        )
        verify.write_json_atomic(summary_path, summary)
        print(str(exc), file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
