#!/usr/bin/env python3
"""Restore and validate a verified preserved mock layer before workloads."""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from typing import Dict, Optional, Sequence


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

import preserved_mock_capture as capture  # pylint: disable=wrong-import-position
import preserved_mock_verify as verify  # pylint: disable=wrong-import-position
import modern_pool_baseline as modern_baseline  # pylint: disable=wrong-import-position


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


def validate_post_retirement_artifact(proof_path: str, receipt: dict) -> dict:
    """Bind current mesh-96 identities to the completed native retirement."""

    retirement_dir = os.path.join(os.path.dirname(proof_path), "retirement-input")
    retirement_path = os.path.join(retirement_dir, "retirement.json")
    if os.path.islink(retirement_dir) or os.path.islink(retirement_path):
        raise HandoffError("post-retirement source evidence must not be symlinked")
    retirement = _load_object(retirement_path, "completed native retirement")
    try:
        with open(retirement_path, "rb") as handle:
            retirement_sha = hashlib.sha256(handle.read()).hexdigest()
    except OSError as exc:
        raise HandoffError(f"unable to hash completed native retirement: {exc}") from exc
    if (
        receipt.get("retirement_build_id") != 80001
        or isinstance(receipt.get("retirement_build_id"), bool)
        or receipt.get("retirement_sha256") != retirement_sha
        or retirement.get("plan_sha256") != modern_baseline.PLAN_SHA
    ):
        raise HandoffError("monitoring handoff lacks the exact successful 80001 retirement")
    if (
        not all(retirement.get(key) is True for key in (
            "execute", "success", "native_fencing_proven", "source_retired",
            "replacements_ready", "placement_hold_removed",
        ))
        or retirement.get("current_mock_ready") != 100
        or retirement.get("kwok_ready") != 100
        or retirement.get("cleanup_errors") != []
    ):
        raise HandoffError("monitoring handoff lacks the exact successful 80001 retirement")
    if retirement.get("target") != {
        "node_name": "aks-default-28928250-vmss000001",
        "node_uid": "c673a142-17ac-44c7-92cc-32efc0d34c61",
        "vm_id": "d81b78a9-fe40-468d-91ec-d66f0456bfa7",
    }:
        raise HandoffError("monitoring handoff names a different retired worker")
    native = retirement.get("native")
    if (
        not isinstance(native, dict) or native.get("accepted") is not True
        or native.get("ambiguous") is not False
        or not native.get("vm_absence_observed_at")
    ):
        raise HandoffError("monitoring handoff lacks positive native worker fencing")
    current = verify.validate_uid_map(retirement.get("current_mock_uids"), 100, "retired mesh-96 mock identities")
    nodes = verify.validate_uid_map(retirement.get("preserved_kwok_uids"), 100, "retired mesh-96 KWOK identities")
    protected = retirement.get("protected_mock_uids")
    original = retirement.get("original_target_mock_uids")
    replacements = retirement.get("controller_replacements")
    partition_error = "retirement does not account for exactly 44 preserved and 56 replacement agents"
    if not all(isinstance(mapping, dict) for mapping in (protected, original, replacements)):
        raise HandoffError(partition_error)
    if (
        len(protected) != 44 or len(original) != 56 or set(replacements) != set(original)
        or any(not isinstance(uid, str) or not uid for uid in original.values())
    ):
        raise HandoffError(partition_error)
    if (
        len(set(original.values())) != 56 or set(protected) & set(original)
        or set(protected) | set(original) != set(current)
        or any(current[name] != uid for name, uid in protected.items())
    ):
        raise HandoffError(partition_error)
    for name, replacement in replacements.items():
        if not isinstance(replacement, dict):
            raise HandoffError(f"{name}: replacement identity is not backed by native fencing")
        identity_matches = (
            replacement.get("old_uid") == original[name]
            and replacement.get("new_uid") == current[name]
            and original[name] not in current.values()
        )
        readiness_proven = (
            replacement.get("fencing_proven") is True and replacement.get("ready") is True
            and replacement.get("node_name") in (
                "aks-cniv5-27550670-vmss000000", "aks-cniv5-27550670-vmss000001",
            )
        )
        if not identity_matches or not readiness_proven:
            raise HandoffError(f"{name}: replacement identity is not backed by native fencing")
    if (
        verify.validate_uid_map(receipt.get("current_mock_uids"), 100, "monitoring mock identities") != current
        or verify.validate_uid_map(receipt.get("preserved_kwok_uids"), 100, "monitoring KWOK identities") != nodes
    ):
        raise HandoffError("monitoring recovery changed the completed retirement identities")
    return {
        "role": "mesh-96", "retirement_build_id": 80001, "retirement_sha256": retirement_sha,
        "preserved_agent_count": 44, "fenced_replacement_count": 56,
        "agent_uids": current, "node_uids": nodes,
    }


def validate_post_retirement_live(live: dict, expected: dict) -> None:
    """Require current identities, without substituting old terminated Pod UIDs."""

    if (
        live.get("role") != "mesh-96"
        or verify.validate_uid_map(live.get("agent_uids"), 100, "live mesh-96 mock identities") != expected["agent_uids"]
        or verify.validate_uid_map(live.get("node_uids"), 100, "live mesh-96 KWOK identities") != expected["node_uids"]
    ):
        raise HandoffError("mesh-96 identities changed since completed monitoring recovery")


def capture_post_retirement_identities(cluster: capture.Cluster, timeout: int) -> dict:
    """Observe the repaired role before reconciliation can make any changes."""

    base = ["kubectl", "--kubeconfig", cluster.kubeconfig, f"--request-timeout={timeout}s"]
    nodes = capture.parse_json(capture.run_command(
        base + ["get", "nodes", "-l", "type=kwok", "-o", "json"], timeout,
    ), "repaired mesh-96 KWOK Nodes")
    agents = capture.parse_json(capture.run_command(
        base + ["-n", "mock-clustermesh", "get", "pods", "-l", "app=mock-cilium-agent", "-o", "json"], timeout,
    ), "repaired mesh-96 mock agents")
    row = {
        "role": cluster.role,
        "node_uids": capture.parse_node_identities(nodes, 100),
        "agent_uids": capture.parse_agent_identities(agents, 100),
    }
    if any(item.get("metadata", {}).get("deletionTimestamp") for item in nodes["items"] + agents["items"]):
        raise HandoffError("repaired mesh-96 contains a deleting KWOK Node or mock agent")
    for item in nodes["items"]:
        status = item.get("status")
        conditions = status.get("conditions") if isinstance(status, dict) else None
        if not isinstance(conditions, list) or not any(
            isinstance(condition, dict) and condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in conditions
        ):
            raise HandoffError("repaired mesh-96 KWOK Nodes are not all Ready")
    return row


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
    parser.add_argument("--modern-baseline-proof")
    parser.add_argument("--fleet-name", default="clustermesh-flt")
    parser.add_argument("--profile-name", default="clustermesh-cmp")
    parser.add_argument("--max-concurrent", type=int, default=8)
    parser.add_argument("--reconcile-concurrent", type=int, default=12)
    parser.add_argument("--command-timeout-seconds", type=int, default=120)
    parser.add_argument("--reconcile-timeout-seconds", type=int, default=3600)
    parser.add_argument("--reconcile-attempts", type=int, default=15)
    parser.add_argument("--reconcile-settle-seconds", type=float, default=45)
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
        "reconcile_attempts",
        "reconcile_settle_seconds",
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
        modern_layout = None
        retirement_identities = None
        historical_pool_count = args.expected_pool_count
        if getattr(args, "modern_baseline_proof", None):
            modern_receipt = _load_object(args.modern_baseline_proof, "completed modern pool baseline receipt")
            modern_layout = modern_baseline.validate_receipt(
                modern_receipt,
                run_id=args.run_id, subscription_id=args.expected_subscription_id,
                expected_pool_count=args.expected_pool_count,
            )
            if "retirement_build_id" in modern_receipt or os.path.exists(
                os.path.join(os.path.dirname(args.modern_baseline_proof), "retirement-input")
            ):
                if args.expected_cluster_count != 100 or args.expected_mock_count != 100:
                    raise HandoffError("post-retirement handoff requires the exact n100 mock inventory")
                retirement_identities = validate_post_retirement_artifact(args.modern_baseline_proof, modern_receipt)
                summary["post_retirement_identity_chain"] = retirement_identities
            historical_pool_count = modern_baseline.ORIGINAL_POOL_COUNT
            summary["modern_pool_layout"] = modern_layout
            summary["historical_verified_pool_count"] = historical_pool_count
            summary["intentional_hardware_baseline_change"] = True
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
            expected_pool_count=historical_pool_count,
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

        if retirement_identities is not None:
            stage = "validating_repaired_mesh96_before_reconcile"
            repaired_cluster = next((cluster for cluster in clusters if cluster.role == "mesh-96"), None)
            if repaired_cluster is None:
                raise HandoffError("post-retirement handoff is missing mesh-96")
            repaired_live = capture_post_retirement_identities(repaired_cluster, args.command_timeout_seconds)
            verify.write_json_atomic(
                os.path.join(args.artifact_dir, "monitoring-identities-pre-reconcile.json"), repaired_live,
            )
            validate_post_retirement_live(repaired_live, retirement_identities)

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
            attempts=args.reconcile_attempts,
            settle_seconds=args.reconcile_settle_seconds,
            request_timeout_seconds=30,
        )
        if reconcile.get("total_clusters") != args.expected_cluster_count:
            raise HandoffError("pre-suite reconcile cluster count mismatch")

        stage = "validating_live_pre_suite_layer"
        platform_args = {
            "subscription_id": args.expected_subscription_id, "run_id": args.run_id,
            "expected_pool_count": args.expected_pool_count, "fleet_name": args.fleet_name,
            "profile_name": args.profile_name, "runner": capture.run_command,
        }
        if modern_layout is not None:
            platform_args["modern_pool_layout"] = modern_layout
        platform = verify.validate_platform_state(clusters, **platform_args)
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
        if retirement_identities is not None:
            repaired_live = next((row for row in live if row.get("role") == "mesh-96"), None)
            if repaired_live is None:
                raise HandoffError("post-reconcile capture is missing mesh-96")
            validate_post_retirement_live(repaired_live, retirement_identities)
            summary["post_retirement_identities_preserved"] = True
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
        if retirement_identities is not None:
            handoff["post_retirement_identity_chain"] = retirement_identities
            handoff["post_retirement_identities_preserved"] = True
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
    except (HandoffError, verify.VerificationError, capture.CaptureError, modern_baseline.BaselineError) as exc:
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
