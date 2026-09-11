#!/usr/bin/env python3
"""Finish one explicitly prepared n100 default-worker retirement from four to three.

This entry point does not cordon, drain, reschedule Pods, or select a victim.
The caller must pin an already quarantined, drained worker by name and UID.
"""

# pylint: disable=too-many-arguments,too-many-locals,protected-access

from __future__ import annotations

import argparse
import re
import sys
import tempfile
import time
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence

import cilium_agent_health as cilium
import mock_cni_recovery as mocks
import preserved_worker_reconcile as workers


EXPECTED_CLUSTERS = 100
CURRENT_POOL_COUNT = 4
TARGET_POOL_COUNT = 3
HOLD_KEY = "mock-clustermesh/repair-hold"
VOLATILE_POOL_FIELDS = {
    "count", "eTag", "etag", "powerState", "provisioningState", "status",
    "systemData", "virtualMachineNodesStatus",
}


def require(condition: bool, message: str) -> None:
    """Reject an unsafe or ambiguous observation."""

    if not condition:
        raise workers.ReconcileError(message)


def resource_equal(left: object, right: str) -> bool:
    """Compare complete ARM resource IDs without inventing missing values."""

    return isinstance(left, str) and left.rstrip("/").lower() == right.lower()


def require_lease(group: dict, timeout_seconds: int) -> None:
    """Keep the entire bounded operation inside the preserved lease."""

    try:
        expiry = datetime.fromisoformat(
            str((group.get("tags") or {}).get("deletion_due_time", "")).replace(
                "Z", "+00:00"
            )
        )
        remaining = (expiry - datetime.now(timezone.utc)).total_seconds()
    except (TypeError, ValueError) as error:
        raise workers.ReconcileError("Preserved lease is missing or invalid") from error
    require(remaining > timeout_seconds + 300, "Preserved lease is too short")


def validate_scope(args, group: dict, clusters: list, members: list) -> tuple:
    """Validate preserved ownership and authoritative Fleet identity mapping."""

    scope = (
        f"/subscriptions/{args.expected_subscription}"
        f"/resourceGroups/{args.resource_group}"
    )
    tags = group.get("tags") or {}
    expected_tags = {
        "clustermesh_debug_preserved": "true",
        "run_id": args.resource_group,
        "scenario": "perf-eval-clustermesh-scale",
        "clustermesh_debug_expected_clusters": str(EXPECTED_CLUSTERS),
        "clustermesh_debug_tfvars_sha256": args.expected_tfvars_sha,
    }
    require(
        resource_equal(group.get("id"), scope)
        and str(group.get("location", "")).lower() == args.expected_region.lower()
        and all(tags.get(key) == value for key, value in expected_tags.items()),
        "Preserved resource-group ownership, region, or tfvars fingerprint changed",
    )
    require_lease(group, args.timeout_seconds)
    require(
        isinstance(clusters, list) and len(clusters) == EXPECTED_CLUSTERS,
        "Expected exactly 100 AKS resources in the preserved resource group",
    )
    by_role = {}
    for cluster in clusters:
        require(isinstance(cluster, dict), "Malformed AKS inventory entry")
        role = (cluster.get("tags") or {}).get("role")
        name = cluster.get("name")
        require(
            isinstance(role, str) and role not in by_role
            and isinstance(name, str) and bool(name)
            and resource_equal(
                cluster.get("id"),
                f"{scope}/providers/Microsoft.ContainerService/managedClusters/{name}",
            )
            and (cluster.get("tags") or {}).get("run_id") == args.resource_group
            and str(cluster.get("location", "")).lower() == args.expected_region.lower(),
            "AKS role, resource identity, or preserved ownership is ambiguous",
        )
        by_role[role] = cluster
    expected_roles = {f"mesh-{index}" for index in range(1, EXPECTED_CLUSTERS + 1)}
    require(set(by_role) == expected_roles, "Preserved AKS role inventory is incomplete")
    require(
        isinstance(members, list) and len(members) == EXPECTED_CLUSTERS,
        "Expected exactly 100 existing Fleet members",
    )
    identities = []
    roles, names, cluster_ids = set(), set(), set()
    fleet_scope = f"{scope}/providers/Microsoft.ContainerService/fleets/clustermesh-flt"
    for member in members:
        require(isinstance(member, dict), "Malformed Fleet member")
        role = member.get("name")
        mesh = member.get("meshProperties") or {}
        identity = mesh.get("ciliumProperties") or {}
        name, cluster_id = identity.get("name"), identity.get("id")
        require(
            role in expected_roles and role not in roles
            and resource_equal(member.get("id"), f"{fleet_scope}/members/{role}")
            and resource_equal(member.get("clusterResourceId"), by_role[role]["id"])
            and resource_equal(
                mesh.get("clusterMeshProfileResourceId"),
                f"{fleet_scope}/clusterMeshProfiles/clustermesh-cmp",
            )
            and member.get("provisioningState") == "Succeeded"
            and (member.get("labels") or {}).get("mesh") == "true"
            and (mesh.get("status") or {}).get("state") == "Connected"
            and isinstance(name, str) and bool(name) and name not in names
            and isinstance(cluster_id, int) and not isinstance(cluster_id, bool)
            and cluster_id > 0
            and cluster_id not in cluster_ids,
            "Fleet ownership, live membership, or Cilium identity is not exact",
        )
        roles.add(role)
        names.add(name)
        cluster_ids.add(cluster_id)
        identities.append(
            {"role": role, "cluster_name": name, "cluster_id": cluster_id}
        )
    selected = by_role[args.role]
    require(
        selected.get("provisioningState") == "Succeeded"
        and (selected.get("powerState") or {}).get("code") == "Running"
        and bool(selected.get("nodeResourceGroup")),
        "Selected AKS cluster is not safely quiescent",
    )
    return selected, identities


def pool_configuration(pool: dict) -> dict:
    """Require all configuration except the intentional count reduction to stay fixed."""

    return {key: value for key, value in pool.items() if key not in VOLATILE_POOL_FIELDS}


def validate_pool_state(state: workers.ClusterState, node_name: str, before: bool):
    """Accept only the single prepared cordon, not unrelated worker drift."""

    selected = None
    for pool in state.pools:
        if pool.pool_name == "default":
            selected = pool
            expected = CURRENT_POOL_COUNT if before else TARGET_POOL_COUNT
            require(pool.desired_count == expected, f"Default pool count is not {expected}")
            if before:
                require(
                    pool.unschedulable_nodes == [node_name],
                    "The selected worker must be the only cordoned pool worker",
                )
                require(
                    replace(pool, unschedulable_nodes=[]).healthy,
                    "Prepared default pool has unrelated ARM, VMSS, or Node drift",
                )
            else:
                require(pool.healthy, "Final default pool is not fully healthy")
        else:
            require(pool.healthy, f"Unrelated pool {pool.pool_name} is unhealthy")
    require(selected is not None, "Default pool is missing")
    return selected


def validate_workloads(
    args, nodes: dict, pods: dict, controller: dict, daemonsets: dict,
    *, observe_retirement: bool = False,
):
    """Require unchanged 100-agent/100-KWOK readiness and a genuinely drained source."""

    node_rows = mocks._items(nodes, "Node inventory")
    pod_rows = mocks._items(pods, "Pod inventory")
    fake_nodes = {
        node["metadata"]["name"]: node["metadata"]["uid"]
        for node in node_rows
        if (node.get("metadata", {}).get("labels") or {}).get("type") == "kwok"
    }
    require(
        len(fake_nodes) == 100
        and len(set(fake_nodes.values())) == 100
        and all(
            workers.node_is_ready(node) and not node["metadata"].get("deletionTimestamp")
            for node in node_rows if node["metadata"]["name"] in fake_nodes
        ),
        "The exact 100 KWOK Nodes are not Ready",
    )
    controller_uid, _ = mocks._controller_details(controller)
    require(controller.get("spec", {}).get("replicas") == 100, "Mock replica count changed")
    agents = {
        pod["metadata"]["name"]: pod
        for pod in pod_rows
        if pod["metadata"].get("namespace") == mocks.DEFAULT_NAMESPACE
        and (pod["metadata"].get("labels") or {}).get("app") == "mock-cilium-agent"
    }
    require(
        set(agents) == {f"kwok-node-{index}" for index in range(100)}
        and len({pod["metadata"].get("uid") for pod in agents.values()}) == 100
        and all(
            mocks._pod_owned_by_controller_uid(pod, controller_uid)
            and mocks._pod_ready(pod)
            and any(
                condition.get("type") == "Ready" and condition.get("status") == "True"
                for condition in (pod.get("status") or {}).get("conditions", [])
            )
            and not pod["metadata"].get("deletionTimestamp")
            and pod["spec"].get("nodeName") != args.node_name
            for pod in agents.values()
        ),
        "All 100 owned mock agents must already be Ready away from the source",
    )
    source = next(
        (node for node in node_rows if node["metadata"]["name"] == args.node_name), None
    )
    if source is not None:
        require(
            source["metadata"].get("uid") == args.node_uid
            and (
                observe_retirement
                or (
                    not source["metadata"].get("deletionTimestamp")
                    and workers.node_is_ready(source)
                )
            )
            and source.get("spec", {}).get("unschedulable") is True
            and mocks._node_pool_name(source) == "default"
            and "bounded-worker-retirement" in str(
                (source["metadata"].get("annotations") or {}).get(HOLD_KEY, "")
            )
            and any(
                taint.get("key") == HOLD_KEY
                and taint.get("value") == "cns-ip-programming"
                and taint.get("effect") == "NoSchedule"
                for taint in source["spec"].get("taints", [])
            ),
            "Source UID, prepared cordon, or explicit repair ownership does not match",
        )
    owners = {
        (row["metadata"]["name"], row["metadata"]["uid"])
        for row in mocks._items(daemonsets, "DaemonSet inventory")
    }
    for pod in pod_rows:
        if pod["spec"].get("nodeName") != args.node_name:
            continue
        require(
            pod["metadata"].get("namespace") == "kube-system"
            and not any("persistentVolumeClaim" in volume for volume in pod["spec"].get("volumes", []))
            and any(
                owner.get("controller") is True and owner.get("kind") == "DaemonSet"
                and (owner.get("name"), owner.get("uid")) in owners
                for owner in pod["metadata"].get("ownerReferences", [])
            ),
            "The source still contains a non-DaemonSet, foreign, or PVC-backed Pod",
        )
    return {
        "source": source,
        "source_pod_references": sorted(
            f"{pod['metadata']['namespace']}/{pod['metadata']['name']}"
            for pod in pod_rows if pod["spec"].get("nodeName") == args.node_name
        ),
        "kwok_uids": fake_nodes,
        "agent_uids": {name: pod["metadata"]["uid"] for name, pod in agents.items()},
        "real_node_names": sorted(
            node["metadata"]["name"] for node in node_rows
            if workers.provider_identity(node) is not None
        ),
    }


def execute_retirement(args, summary: dict, runner=workers.run_command) -> None:
    """Read authority, prove the prepared state, submit once, and certify completion."""

    deadline = time.monotonic() + args.timeout_seconds

    def run(command, timeout=45):
        remaining = int(deadline - time.monotonic())
        require(remaining > 0, "Prepared-worker retirement deadline expired")
        command = list(command)
        if command[0] == "az" and command[1:3] != ["account", "show"]:
            command += ["--subscription", args.expected_subscription]
        return runner(command, min(timeout, remaining))

    def read(command):
        return workers.parse_json(run(command), "prepared retirement")

    def az(*command):
        return read(["az", *command, "--output", "json", "--only-show-errors"])

    def save():
        mocks.write_json_atomic(args.summary_file, summary)

    account = az("account", "show")
    require(
        str(account.get("id", "")).lower() == args.expected_subscription.lower(),
        "Current Azure subscription does not match the UI-selected subscription",
    )
    group = az("group", "show", "--name", args.resource_group)
    clusters = az("aks", "list", "--resource-group", args.resource_group)
    members = az(
        "fleet", "member", "list", "--resource-group", args.resource_group,
        "--fleet-name", "clustermesh-flt",
    )
    selected, identities = validate_scope(args, group, clusters, members)
    node_group = az("group", "show", "--name", selected["nodeResourceGroup"])
    require(
        resource_equal(node_group.get("managedBy"), selected["id"])
        and str(node_group.get("location", "")).lower() == args.expected_region.lower(),
        "Selected node resource group is not owned by the preserved AKS cluster",
    )
    require_lease(node_group, args.timeout_seconds)
    summary["cluster_id"] = selected["id"]
    summary["authoritative_identity_count"] = len(identities)
    save()
    with tempfile.TemporaryDirectory(prefix="prepared-worker-retirement-") as temporary:
        kubeconfig = str(Path(temporary) / "cluster.config")
        run([
            "az", "aks", "get-credentials", "--resource-group", args.resource_group,
            "--name", selected["name"], "--file", kubeconfig, "--only-show-errors",
        ])
        Path(kubeconfig).chmod(0o600)
        cluster = workers.Cluster(
            selected["name"], args.resource_group, args.role, kubeconfig
        )
        prefix = [
            "kubectl", "--kubeconfig", kubeconfig, "--context", selected["name"],
            "--request-timeout=45s",
        ]

        def workloads(*, observe_retirement=False):
            nodes = read(prefix + ["get", "nodes", "-o", "json"])
            provider_scope = (
                f"/subscriptions/{args.expected_subscription}/resourceGroups/"
                f"{selected['nodeResourceGroup']}/providers/Microsoft.Compute/"
                "virtualMachineScaleSets/"
            ).lower()
            for node in mocks._items(nodes, "Node inventory"):
                if (node["metadata"].get("labels") or {}).get("type") == "kwok":
                    continue
                provider_id = str(node.get("spec", {}).get("providerID", ""))
                require(
                    provider_id.removeprefix("azure://").lower().startswith(provider_scope)
                    and str(
                        (node["metadata"].get("labels") or {}).get(
                            "kubernetes.azure.com/cluster", ""
                        )
                    ).lower() == selected["nodeResourceGroup"].lower(),
                    "Real Kubernetes worker does not belong to the selected AKS node group",
                )
                if node["metadata"]["name"] != args.node_name:
                    require(
                        not any(
                            str(taint.get("key", "")).startswith("mock-clustermesh/")
                            for taint in node.get("spec", {}).get("taints", [])
                        ),
                        "Another worker has unresolved maintenance scheduling exclusions",
                    )
            return validate_workloads(
                args, nodes,
                read(prefix + ["get", "pods", "-A", "-o", "json"]),
                read(prefix + [
                    "-n", mocks.DEFAULT_NAMESPACE, "get", "statefulset",
                    "kwok-node", "-o", "json",
                ]),
                read(prefix + ["-n", "kube-system", "get", "daemonsets", "-o", "json"]),
                observe_retirement=observe_retirement,
            )

        def source_removed(snapshot):
            if snapshot["source"] is not None or snapshot["source_pod_references"]:
                return False
            containers = read(prefix + ["get", "nnc", "-A", "-o", "json"])
            return all(
                row["metadata"]["name"] != args.node_name
                for row in mocks._items(containers, "network-container inventory")
            )

        def cilium_runner(command, timeout):
            try:
                return run(command, timeout)
            except workers.ReconcileError as error:
                raise cilium.overlay.ProbeError(str(error)) from error

        def prove_peers(snapshot, label):
            names = {row["cluster_name"] for row in identities if row["role"] != args.role}
            proof = cilium.probe(
                role=args.role, kubeconfig=kubeconfig, expected_remote_count=99,
                expected_remote_names=names, attempts=1, retry_seconds=0,
                command_timeout_seconds=45, runner=cilium_runner,
            )
            summary[label] = proof
            save()
            covered = {
                row["node_name"] for row in proof["agents"] if row["healthy"]
            }
            require(
                proof["healthy"] and set(snapshot["real_node_names"]) == covered,
                "Strict Cilium identity/peer proof does not cover every real worker",
            )

        def prove_instances(pool):
            instances = az(
                "vmss", "list-instances", "--resource-group",
                selected["nodeResourceGroup"], "--name", pool.vmss_name,
            )
            require(
                isinstance(instances, list)
                and len(instances) == pool.desired_count
                and all(
                    isinstance(row, dict)
                    and row.get("provisioningState") == "Succeeded"
                    for row in instances
                )
                and {str(row.get("instanceId")) for row in instances}
                == set(pool.instance_ids),
                "Default-pool instances are not an exact quiescent set",
            )

        pool_args = [
            "aks", "nodepool", "show", "--resource-group", args.resource_group,
            "--cluster-name", selected["name"], "--name", "default",
        ]
        before = workloads()
        state = workers.probe_cluster(cluster, run, 45)
        exists = before["source"] is not None
        pool_state = validate_pool_state(state, args.node_name, exists)
        prove_instances(pool_state)
        pool_before = az(*pool_args)
        require(
            pool_before.get("enableAutoScaling") is False
            and pool_before.get("count") == (
                CURRENT_POOL_COUNT if exists else TARGET_POOL_COUNT
            )
            and pool_before.get("provisioningState") == "Succeeded"
            and (pool_before.get("powerState") or {}).get("code") == "Running",
            "Default pool is not fixed-count and safely quiescent",
        )
        if exists:
            identity = workers.provider_identity(before["source"])
            require(
                identity is not None and identity[0] == pool_state.vmss_name.lower()
                and identity[1] in pool_state.instance_ids,
                "Source worker is not an exact live instance of the selected pool",
            )
            summary["source_instance_id"] = identity[1]
        else:
            baseline = before
            while not source_removed(before):
                require(
                    time.monotonic() < deadline,
                    "Previously retired worker still has garbage-collection references",
                )
                time.sleep(min(15, max(0, deadline - time.monotonic())))
                before = workloads(observe_retirement=True)
                require(
                    before["kwok_uids"] == baseline["kwok_uids"]
                    and before["agent_uids"] == baseline["agent_uids"],
                    "Workload identities changed while observing retirement cleanup",
                )
        prove_peers(before, "cilium_before")
        summary["before"] = workers.state_to_dict(state)
        summary["pool_configuration_before"] = pool_configuration(pool_before)
        summary["workload_identity_before"] = {
            key: before[key] for key in ("kwok_uids", "agent_uids")
        }
        summary["status"] = "prepared" if exists else "already-absent"
        save()
        if not exists or not args.execute:
            summary["success"] = True
            return
        immediate = workloads()
        require(
            immediate["source"] is not None
            and immediate["kwok_uids"] == before["kwok_uids"]
            and immediate["agent_uids"] == before["agent_uids"],
            "Prepared worker or protected workload identities changed before submission",
        )
        current_pool = az(*pool_args)
        require(
            current_pool.get("count") == CURRENT_POOL_COUNT
            and current_pool.get("provisioningState") == "Succeeded"
            and (current_pool.get("powerState") or {}).get("code") == "Running"
            and pool_configuration(current_pool) == pool_configuration(pool_before),
            "Pool state changed before retirement submission",
        )
        summary["mutation_started"] = True
        summary["status"] = "retiring"
        save()
        run([
            "az", "aks", "nodepool", "delete-machines",
            "--resource-group", args.resource_group, "--cluster-name", selected["name"],
            "--name", "default", "--machine-names", args.node_name,
            "--no-wait", "--only-show-errors",
        ])
        summary["request_accepted"] = True
        save()
        while time.monotonic() < deadline:
            current_pool = az(*pool_args)
            summary["last_pool"] = current_pool
            save()
            require(
                current_pool.get("count") in (CURRENT_POOL_COUNT, TARGET_POOL_COUNT)
                and current_pool.get("provisioningState") in ("DeletingMachines", "Succeeded")
                and pool_configuration(current_pool) == pool_configuration(pool_before),
                "Retirement changed unrelated pool configuration or entered an unsafe state",
            )
            if (
                current_pool.get("count") == TARGET_POOL_COUNT
                and current_pool.get("provisioningState") == "Succeeded"
            ):
                after = workloads(observe_retirement=True)
                summary["pending_source_pod_references"] = after["source_pod_references"]
                save()
                if source_removed(after):
                    break
            time.sleep(min(15, max(0, deadline - time.monotonic())))
        else:
            raise workers.ReconcileError("Prepared-worker retirement did not converge")
        final_state = workers.probe_cluster(cluster, run, 45)
        final_pool = validate_pool_state(final_state, args.node_name, False)
        prove_instances(final_pool)
        require(
            summary["source_instance_id"] not in final_pool.instance_ids,
            "The retired source instance is still present in the VMSS",
        )
        final_configuration = az(*pool_args)
        require(
            final_configuration.get("count") == TARGET_POOL_COUNT
            and final_configuration.get("provisioningState") == "Succeeded"
            and (final_configuration.get("powerState") or {}).get("code") == "Running"
            and pool_configuration(final_configuration) == pool_configuration(pool_before),
            "Final pool configuration changed during retirement cleanup",
        )
        summary["last_pool"] = final_configuration
        require(
            after["kwok_uids"] == before["kwok_uids"]
            and after["agent_uids"] == before["agent_uids"],
            "Protected workload identities changed during retirement",
        )
        prove_peers(after, "cilium_after")
        summary.update(
            success=True, status="retired", after=workers.state_to_dict(final_state)
        )


def parse_args(argv: Optional[Sequence[str]] = None):
    """Require explicit preserved scope and a UID-pinned single-worker plan."""

    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "resource-group", "confirm-resource-group", "expected-subscription",
        "expected-region", "expected-tfvars-sha", "role", "node-name",
        "node-uid", "summary-file",
    ):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if args.resource_group != args.confirm_resource_group:
        parser.error("--confirm-resource-group must equal --resource-group")
    if not re.fullmatch(r"[0-9]+-[0-9a-f]{8}", args.resource_group):
        parser.error("preserved resource group must be <build-id>-<8 hex>")
    if args.role not in {f"mesh-{index}" for index in range(1, 101)}:
        parser.error("--role must select one preserved n100 role")
    if not re.fullmatch(r"aks-default-[a-z0-9]+-vmss[a-z0-9]{6}", args.node_name):
        parser.error("--node-name must identify one default-pool VMSS worker")
    if not re.fullmatch(r"[0-9a-f]{64}", args.expected_tfvars_sha):
        parser.error("--expected-tfvars-sha must be a SHA256 digest")
    try:
        uuid.UUID(args.node_uid)
        uuid.UUID(args.expected_subscription)
    except ValueError:
        parser.error("worker UID and subscription ID must be UUIDs")
    if not 300 <= args.timeout_seconds <= 3600:
        parser.error("--timeout-seconds must be between 300 and 3600")
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Preserve diagnostics on both successful and failed retirement attempts."""

    args = parse_args(argv)
    summary = {
        "schema_version": 1, "started_at": workers.utc_now(), "success": False,
        "role": args.role, "node_name": args.node_name, "node_uid": args.node_uid,
        "execute": args.execute, "mutation_started": False, "request_accepted": False,
    }
    mocks.write_json_atomic(args.summary_file, summary)
    try:
        execute_retirement(args, summary)
    except (workers.ReconcileError, mocks.RecoveryError, OSError) as error:
        summary["fatal_error"] = str(error)
        print(f"Prepared worker retirement failed: {error}", file=sys.stderr)
        return 1
    finally:
        summary["finished_at"] = workers.utc_now()
        mocks.write_json_atomic(args.summary_file, summary)
    print(f"{args.role}: prepared worker {summary['status']}.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
