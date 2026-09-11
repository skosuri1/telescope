#!/usr/bin/env python3
"""Safely replace one explicitly selected real worker with broken Azure CNI."""

# pylint: disable=too-many-lines,too-many-locals,too-many-branches,too-many-statements,protected-access,too-many-boolean-expressions

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import sys
import time
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple

import cilium_agent_health as cilium
import mock_cni_recovery as mocks
import prepared_worker_retirement as retirement
import preserved_worker_reconcile as workers


STEADY_POOL_COUNT = 3
SURGE_POOL_COUNT = 4
MIN_INITIAL_POOL_COUNT = 2
MAX_HEALTHY_SOURCE_AGENTS = 25
DEFAULT_NAMESPACE = mocks.DEFAULT_NAMESPACE
DEFAULT_POOL_NAME = "default"
EXCLUSION_KEY = "mock-clustermesh/cni-worker-maintenance"
HOLD_ANNOTATION = retirement.HOLD_KEY
HOLD_REASON = "cns-ip-programming"
EXPECTED_REMOTE_COUNT = 99
EXPECTED_AGENT_NAMES = {f"kwok-node-{index}" for index in range(100)}
PROBE_LABEL_KEY = "mock-clustermesh/cni-maintenance-probe"
PROBE_CPU_REQUEST = "5m"
PROBE_MEMORY_REQUEST = "16Mi"
DEFAULT_PROBE_IMAGE = "registry.k8s.io/e2e-test-images/agnhost:2.47"
PROBE_COMMAND = ["netexec", "--http-port=8080"]
FINAL_QUALIFICATION_RESERVE_SECONDS = 120
RETIREMENT_PHASE_BUDGET_SECONDS = 300
RETIREMENT_MINIMUM_SECONDS = 180
DRAIN_PHASE_BUDGET_SECONDS = 900
KWOK_READY_WAIT_SECONDS = 300
SURGE_READY_WAIT_SECONDS = 300
IP_GROWTH_WAIT_SECONDS = 300
EMPTY_HOST_RECOVERY_SECONDS = 900
EMPTY_HOST_RECOVERY_KEY = "mock-clustermesh/empty-host-recovery"
UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ROLE_RE = re.compile(r"^mesh-[1-9]\d*$")
PROVIDER_ID_RE = re.compile(
    r"^azure:///subscriptions/(?P<subscription>[^/]+)/resourceGroups/"
    r"(?P<resource_group>[^/]+)/providers/Microsoft\.Compute/"
    r"virtualMachineScaleSets/(?P<vmss>[^/]+)/virtualMachines/"
    r"(?P<instance>[^/]+)$",
    re.IGNORECASE,
)
EXPECTED_ERRORS = (workers.ReconcileError, mocks.RecoveryError, OSError)
KNOWN_ALLOWED_DEPLOYMENTS = {
    ("kube-system", "cilium-operator"),
    ("kube-system", "coredns"),
    ("kube-system", "coredns-autoscaler"),
    ("kube-system", "metrics-server"),
    ("kube-system", "ama-metrics"),
    ("kube-system", "ama-metrics-ksm"),
    ("kube-system", "ama-logs"),
    ("kube-system", "ama-logs-rs"),
    ("kube-system", "konnectivity-agent"),
    ("kube-system", "konnectivity-agent-autoscaler"),
    ("kube-system", "kwok-controller"),
    ("kube-system", "hubble-relay"),
    ("kube-state-metrics-perf-test", "kube-state-metrics"),
}
KNOWN_ALLOWED_STATEFULSETS = {
    ("kube-state-metrics-perf-test", "kube-state-metrics"),
}
Runner = Callable[[Sequence[str], int], str]


class MaintenanceInterrupted(workers.ReconcileError):
    """The bounded maintenance workflow was interrupted."""


class FreshWorkersNotReady(workers.ReconcileError):
    """Only expected fresh-worker registration or Ready convergence is pending."""


def require(condition: bool, message: str) -> None:
    """Reject an unsafe or ambiguous observation."""

    if not condition:
        raise workers.ReconcileError(message)


def utc_now() -> str:
    """Return an RFC3339 UTC timestamp."""

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalized_resource_id(value: str) -> str:
    return value.rstrip("/").lower()


def _resource_equal(left: object, right: str) -> bool:
    return isinstance(left, str) and _normalized_resource_id(left) == right.lower()


def _normalize_provider_id(provider_id: str) -> str:
    return provider_id.rstrip("/").lower()


def _safe_name_component(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.lower())
    return cleaned.strip("-")[:24] or "node"


def _parse_uuid(value: str, field: str) -> str:
    require(UUID_RE.fullmatch(value) is not None, f"{field} must be a UUID")
    return value.lower()


def _items(payload: dict, description: str) -> List[dict]:
    return mocks._items(payload, description)


def _taints(node: dict) -> List[dict]:
    taints = (node.get("spec") or {}).get("taints") or []
    require(isinstance(taints, list), "Node taints are malformed")
    return taints


def _annotations(node: dict) -> dict:
    annotations = (node.get("metadata") or {}).get("annotations") or {}
    require(isinstance(annotations, dict), "Node annotations are malformed")
    return annotations


def _readiness_condition_true(pod: dict) -> bool:
    return any(
        isinstance(condition, dict)
        and condition.get("type") == "Ready"
        and condition.get("status") == "True"
        for condition in (pod.get("status") or {}).get("conditions", [])
    )


def _agent_map(payload: dict) -> Dict[str, dict]:
    return {
        str((pod.get("metadata") or {}).get("name") or ""): pod
        for pod in _items(payload, "mock-agent inventory")
        if (pod.get("metadata") or {}).get("namespace") == DEFAULT_NAMESPACE
        and (pod.get("metadata") or {}).get("labels", {}).get("app")
        == "mock-cilium-agent"
        and (pod.get("metadata") or {}).get("name")
    }


def _kwok_map(payload: dict) -> Dict[str, dict]:
    return {
        str((node.get("metadata") or {}).get("name") or ""): node
        for node in _items(payload, "Node inventory")
        if (node.get("metadata") or {}).get("labels", {}).get("type") == "kwok"
        and (node.get("metadata") or {}).get("name")
    }


def _real_node_map(payload: dict) -> Dict[str, dict]:
    return {
        str((node.get("metadata") or {}).get("name") or ""): node
        for node in _items(payload, "Node inventory")
        if workers.provider_identity(node) is not None
        and (node.get("metadata") or {}).get("name")
    }


def _node_owner_uid(row: dict) -> str:
    owners = (row.get("metadata") or {}).get("ownerReferences") or []
    matches = [
        str(owner.get("uid") or "")
        for owner in owners
        if isinstance(owner, dict)
        and owner.get("kind") == "Node"
        and owner.get("controller") is True
        and owner.get("name")
    ]
    require(len(matches) == 1 and bool(matches[0]), "NodeNetworkConfig owner is not exact")
    return matches[0]


def _nnc_map(payload: dict) -> Dict[str, dict]:
    mapping = {}
    for row in _items(payload, "NodeNetworkConfig inventory"):
        metadata = row.get("metadata") or {}
        status = row.get("status") or {}
        containers = status.get("networkContainers") or []
        name = str(metadata.get("name") or "")
        require(name, "NodeNetworkConfig name is missing")
        require(isinstance(containers, list) and len(containers) == 1, "NodeNetworkConfig networkContainers are malformed")
        container = containers[0]
        assignments = container.get("ipAssignments") or []
        require(isinstance(assignments, list), "NodeNetworkConfig ipAssignments are malformed")
        mapping[name] = {
            "name": name,
            "uid": str(metadata.get("uid") or ""),
            "node_uid": _node_owner_uid(row),
            "network_container_id": str(container.get("id") or ""),
            "assigned_ip_count": int(status.get("assignedIPCount") or 0),
            "requested_ip_count": int((row.get("spec") or {}).get("requestedIPCount") or 0),
            "version": int(container.get("version") or 0),
            "ip_addresses": sorted(
                str(entry.get("ip") or "")
                for entry in assignments
                if isinstance(entry, dict) and entry.get("ip")
            ),
        }
        require(mapping[name]["network_container_id"], "NodeNetworkConfig network container ID is missing")
    return mapping


def _require_exact_agents(pods_payload: dict, controller_uid: str) -> Dict[str, dict]:
    agents = _agent_map(pods_payload)
    require(set(agents) == EXPECTED_AGENT_NAMES, "The exact 100 mock-agent names are required")
    uids = [str((pod.get("metadata") or {}).get("uid") or "") for pod in agents.values()]
    require(all(uids) and len(set(uids)) == 100, "Mock-agent UIDs are not exact")
    require(
        all(
            mocks._pod_owned_by_controller_uid(pod, controller_uid)
            and not (pod.get("metadata") or {}).get("deletionTimestamp")
            for pod in agents.values()
        ),
        "Mock-agent ownership or deletion state is unsafe",
    )
    return agents


def _require_exact_kwok_nodes(nodes_payload: dict) -> Dict[str, dict]:
    kwok = _kwok_map(nodes_payload)
    require(set(kwok) == EXPECTED_AGENT_NAMES, "The exact 100 KWOK Node identities are required")
    uids = [str((node.get("metadata") or {}).get("uid") or "") for node in kwok.values()]
    require(all(uids) and len(set(uids)) == 100, "KWOK Node UIDs are not exact")
    return kwok


def _require_all_kwok_ready(nodes_payload: dict, expected_uids: Dict[str, str]) -> None:
    kwok = _require_exact_kwok_nodes(nodes_payload)
    for name, expected_uid in expected_uids.items():
        node = kwok.get(name)
        require(node is not None, f"{name}: KWOK Node is missing")
        require(
            str((node.get("metadata") or {}).get("uid") or "") == expected_uid
            and workers.node_is_ready(node)
            and not (node.get("metadata") or {}).get("deletionTimestamp"),
            f"{name}: KWOK Node readiness or identity changed",
        )


def _pool_configuration(pool: dict) -> dict:
    return retirement.pool_configuration(pool)


def _validate_real_node_scope(node: dict, *, subscription: str, node_resource_group: str) -> None:
    metadata = node.get("metadata") or {}
    labels = metadata.get("labels") or {}
    provider_id = str((node.get("spec") or {}).get("providerID") or "")
    match = PROVIDER_ID_RE.fullmatch(provider_id)
    require(match is not None, f"{metadata.get('name')}: providerID is not an AKS VMSS resource")
    require(
        match.group("subscription").lower() == subscription.lower()
        and match.group("resource_group").lower() == node_resource_group.lower()
        and str(labels.get("kubernetes.azure.com/cluster") or "").lower()
        == node_resource_group.lower(),
        f"{metadata.get('name')}: providerID or node-resource-group label escaped the selected AKS scope",
    )


def _real_pool_nodes(
    nodes_payload: dict,
    *,
    pool_name: str,
    subscription: str,
    node_resource_group: str,
) -> Dict[str, dict]:
    real_nodes = _real_node_map(nodes_payload)
    selected = {}
    for name, node in real_nodes.items():
        _validate_real_node_scope(
            node,
            subscription=subscription,
            node_resource_group=node_resource_group,
        )
        if mocks._node_pool_name(node) == pool_name:
            selected[name] = node
    return selected


def _pool_image_stable(pool: dict, nodes_payload: dict, *, pool_name: str, expected_count: int) -> bool:
    expected_image = str(pool.get("nodeImageVersion") or "")
    if not expected_image:
        return False
    selected = [
        node
        for node in _real_node_map(nodes_payload).values()
        if mocks._node_pool_name(node) == pool_name
    ]
    if len(selected) != expected_count:
        return False
    return all(
        str(((node.get("metadata") or {}).get("labels") or {}).get("kubernetes.azure.com/node-image-version") or "")
        == expected_image
        for node in selected
    )


def _pool_from_state(state: workers.ClusterState, pool_name: str) -> workers.PoolState:
    match = next((pool for pool in state.pools if pool.pool_name == pool_name), None)
    require(match is not None, f"{pool_name}: node pool is missing")
    return match


def _validate_cluster_state(
    args,
    selected_cluster: dict,
    cluster_state: workers.ClusterState,
    pool_payload: dict,
    nodes_payload: dict,
    pods_payload: dict,
    events_payload: dict,
    controller_payload: dict,
    nnc_payload: dict,
) -> dict:
    default_pool = _pool_from_state(cluster_state, DEFAULT_POOL_NAME)
    require(cluster_state.pools, "Cluster has no real node pools")
    require(
        default_pool.desired_count in (MIN_INITIAL_POOL_COUNT, STEADY_POOL_COUNT),
        "Initial default pool count must be exactly 2 or 3",
    )
    for pool in cluster_state.pools:
        if pool.pool_name == DEFAULT_POOL_NAME:
            require(pool.healthy, "Default pool has unrelated ARM, VMSS, or Kubernetes drift")
        else:
            require(pool.healthy, f"Unrelated pool {pool.pool_name} is unhealthy")
    require(
        pool_payload.get("enableAutoScaling") is False
        and pool_payload.get("count") == default_pool.desired_count
        and pool_payload.get("provisioningState") == "Succeeded"
        and (pool_payload.get("powerState") or {}).get("code") == "Running"
        and _pool_image_stable(
            pool_payload,
            nodes_payload,
            pool_name=DEFAULT_POOL_NAME,
            expected_count=default_pool.desired_count,
        ),
        "Default pool is not fixed-count, quiescent, and image-stable",
    )
    initial = _validate_source_workload(
        args, selected_cluster, default_pool, nodes_payload, pods_payload,
        events_payload, controller_payload, nnc_payload, source_held=False,
    )
    initial.update({
        "initial_pool_count": default_pool.desired_count,
        "expected_fresh_nodes": SURGE_POOL_COUNT - default_pool.desired_count,
        "initial_pool_configuration": _pool_configuration(pool_payload),
    })
    return initial


def _validate_source_workload(
    args,
    selected_cluster: dict,
    default_pool: workers.PoolState,
    nodes_payload: dict,
    pods_payload: dict,
    events_payload: dict,
    controller_payload: dict,
    nnc_payload: dict,
    *,
    source_held: bool,
) -> dict:
    controller_uid, pod_template = mocks._controller_details(controller_payload)
    require(controller_payload.get("spec", {}).get("replicas") == 100, "Unexpected mock-agent StatefulSet replica count")
    kwok_nodes = _require_exact_kwok_nodes(nodes_payload)
    agents = _require_exact_agents(pods_payload, controller_uid)
    real_nodes = _real_pool_nodes(
        nodes_payload,
        pool_name=DEFAULT_POOL_NAME,
        subscription=args.expected_subscription,
        node_resource_group=str(selected_cluster["nodeResourceGroup"]),
    )
    require(
        len(real_nodes) == default_pool.desired_count,
        "Default pool Kubernetes worker count does not match the quiescent VMSS state",
    )
    source = real_nodes.get(args.node_name)
    require(source is not None, "The explicit source worker is not present in the default pool")
    require(
        str((source.get("metadata") or {}).get("uid") or "").lower()
        == args.node_uid.lower(),
        "The explicit source worker UID no longer matches",
    )
    provider_id = str((source.get("spec") or {}).get("providerID") or "")
    require(
        _normalize_provider_id(provider_id) == _normalize_provider_id(args.source_provider_id),
        "The explicit source worker providerID no longer matches",
    )
    source_identity = workers.provider_identity(source)
    require(
        source_identity is not None and source_identity[0] == default_pool.vmss_name.lower(),
        "The explicit source worker is not part of the quiescent default VMSS",
    )
    require(
        workers.node_is_ready(source)
        and not (source.get("metadata") or {}).get("deletionTimestamp")
        and bool((source.get("spec") or {}).get("unschedulable")) == source_held,
        "The explicit source worker is not Ready with the expected scheduling state",
    )
    source_nnc = _nnc_map(nnc_payload).get(args.node_name)
    require(source_nnc is not None, "The explicit source NodeNetworkConfig is missing")
    require(
        source_nnc["node_uid"].lower() == args.node_uid.lower()
        and source_nnc["network_container_id"].lower()
        == args.source_network_container_id.lower(),
        "The explicit source NodeNetworkConfig or network container changed",
    )
    affected = mocks.discover_cni_blocked_agents(pods_payload, events_payload)
    affected_names = {
        str((pod.get("metadata") or {}).get("name") or "")
        for pod in affected
    }
    unhealthy = {
        name for name, pod in agents.items()
        if not mocks._pod_ready(pod)
    }
    require(
        unhealthy == affected_names,
        "Every unhealthy mock agent must have exact UID-matched Azure CNI exhaustion evidence",
    )
    affected_sources = {
        str((pod.get("spec") or {}).get("nodeName") or "")
        for pod in affected
        if (pod.get("spec") or {}).get("nodeName")
    }
    require(
        affected_sources == {args.node_name},
        "Exactly one UID-proven CNI-broken source is required; refusing absent or multiple sources",
    )
    healthy_source = [
        name
        for name, pod in agents.items()
        if str((pod.get("spec") or {}).get("nodeName") or "") == args.node_name
        and mocks._pod_ready(pod)
    ]
    require(
        len(healthy_source) <= MAX_HEALTHY_SOURCE_AGENTS,
        f"Healthy source mock-agent count exceeds the safety cap {MAX_HEALTHY_SOURCE_AGENTS}",
    )
    return {
        "controller_uid": controller_uid,
        "pod_template": pod_template,
        "agent_uids": {
            name: str((pod.get("metadata") or {}).get("uid") or "")
            for name, pod in agents.items()
        },
        "kwok_uids": {
            name: str((node.get("metadata") or {}).get("uid") or "")
            for name, node in kwok_nodes.items()
        },
        "all_real_node_uids": {
            name: str((node.get("metadata") or {}).get("uid") or "")
            for name, node in _real_node_map(nodes_payload).items()
        },
        "initial_real_node_uids": {
            name: str((node.get("metadata") or {}).get("uid") or "")
            for name, node in real_nodes.items()
        },
        "initial_ready_agent_uids": {
            name: str((pod.get("metadata") or {}).get("uid") or "")
            for name, pod in agents.items()
            if mocks._pod_ready(pod)
        },
        "pending_source_agents": [
            {
                "name": name,
                "uid": str((agents[name].get("metadata") or {}).get("uid") or ""),
                "node_name": args.node_name,
                "memory_bytes": mocks._resource_requests(agents[name])[1],
            }
            for name in sorted(affected_names)
        ],
        "healthy_source_agents": [
            {
                "name": name,
                "uid": str((agents[name].get("metadata") or {}).get("uid") or ""),
                "node_name": args.node_name,
                "memory_bytes": mocks._resource_requests(agents[name])[1],
            }
            for name in sorted(healthy_source)
        ],
        "source_vmss_name": default_pool.vmss_name.lower(),
    }


def _validate_pre_scale_state(
    args,
    selected_cluster: dict,
    cluster_state: workers.ClusterState,
    pool_payload: dict,
    nodes_payload: dict,
    initial_pool_count: int,
    initial_pool_configuration: dict,
    initial_all_real_node_uids: Dict[str, str],
    *,
    additional_quarantined_node: str = "",
) -> None:
    default_pool = _pool_from_state(cluster_state, DEFAULT_POOL_NAME)
    require(default_pool.desired_count == initial_pool_count, "Default pool count changed before surge submission")
    require(
        replace(default_pool, unschedulable_nodes=[]).healthy,
        "Default VMSS or Kubernetes worker state drifted before surge submission",
    )
    require(
        default_pool.unschedulable_nodes == sorted(
            [args.node_name] + ([additional_quarantined_node] if additional_quarantined_node else [])
        ),
        "Only the explicit source worker may be unschedulable before the surge",
    )
    require(
        not default_pool.stale_instance_ids
        and not default_pool.failed_instance_ids
        and default_pool.vmss_capacity == initial_pool_count,
        "Default pool drifted before the surge",
    )
    for pool in cluster_state.pools:
        if pool.pool_name == DEFAULT_POOL_NAME:
            continue
        require(pool.healthy, f"Unrelated pool {pool.pool_name} drifted before the surge")
    require(
        pool_payload.get("enableAutoScaling") is False
        and pool_payload.get("count") == initial_pool_count
        and pool_payload.get("provisioningState") == "Succeeded"
        and (pool_payload.get("powerState") or {}).get("code") == "Running"
        and _pool_configuration(pool_payload) == initial_pool_configuration
        and _pool_image_stable(
            pool_payload,
            nodes_payload,
            pool_name=DEFAULT_POOL_NAME,
            expected_count=initial_pool_count,
        ),
        "Default pool configuration changed since the initial proof",
    )
    all_real_nodes = _real_node_map(nodes_payload)
    require(
        set(all_real_nodes) == set(initial_all_real_node_uids),
        "The real worker inventory changed before surge submission",
    )
    for name, uid in initial_all_real_node_uids.items():
        require(
            str((all_real_nodes[name].get("metadata") or {}).get("uid") or "") == uid,
            f"{name}: a real worker UID changed before surge submission",
        )
    real_nodes = _real_pool_nodes(
        nodes_payload,
        pool_name=DEFAULT_POOL_NAME,
        subscription=args.expected_subscription,
        node_resource_group=str(selected_cluster["nodeResourceGroup"]),
    )
    require(
        args.node_name in real_nodes,
        "The explicit source worker disappeared before surge submission",
    )


def _resume_options(args) -> bool:
    values = (
        getattr(args, "resume_build_id", 0),
        getattr(args, "resume_summary", ""),
        getattr(args, "resume_manifest", ""),
    )
    require(
        not any(values) or (
            all(values) and isinstance(values[0], int)
            and not isinstance(values[0], bool) and values[0] > 0
        ),
        "--resume-build-id, --resume-summary and --resume-manifest require all three, with a positive build ID",
    )
    recovery = (
        getattr(args, "recover_empty_fresh_node", ""),
        getattr(args, "recover_empty_fresh_uid", ""),
    )
    require(
        not any(recovery) or (all(recovery) and bool(values[0])),
        "Empty fresh-host recovery requires both target name/UID and an explicit resume checkpoint",
    )
    require(
        not getattr(args, "replace_empty_fresh", False) or all(recovery),
        "Empty-worker replacement requires the explicit fresh recovery target and checkpoint",
    )
    return bool(values[0])


def _require_resume_paths(args) -> None:
    inputs = [os.path.realpath(args.resume_summary), os.path.realpath(args.resume_manifest)]
    require(
        len(set(inputs)) == 2
        and os.path.realpath(args.summary_file) not in inputs
        and os.path.realpath(f"{args.summary_file}.retirement.json") not in inputs,
        "Resume inputs and output summaries must be distinct files",
    )


def _read_resume_json(path: str) -> Tuple[dict, str]:
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"Duplicate resume artifact key: {key}")
            result[key] = value
        return result

    with open(path, "rb") as stream:
        data = stream.read()
    try:
        value = json.loads(data, object_pairs_hook=unique_object)
    except (ValueError, UnicodeError) as error:
        raise workers.ReconcileError(f"Malformed resume artifact: {path}") from error
    require(isinstance(value, dict), "Resume artifact must contain an object")
    return value, hashlib.sha256(data).hexdigest()


def _identity_map(value: object, description: str, names: Optional[Set[str]] = None) -> dict:
    require(
        isinstance(value, dict) and bool(value)
        and all(isinstance(name, str) and name and isinstance(uid, str) and uid for name, uid in value.items()),
        f"{description}: explicit nonempty identity map is required",
    )
    require(len(set(value.values())) == len(value), f"{description}: duplicate identities")
    if names is not None:
        require(set(value) == names, f"{description}: identity names are not exact")
    return value


def _load_resume(args) -> Optional[dict]:
    if not _resume_options(args):
        return None
    _require_resume_paths(args)
    prior, summary_sha = _read_resume_json(args.resume_summary)
    manifest, manifest_sha = _read_resume_json(args.resume_manifest)
    recover_host = bool(getattr(args, "recover_empty_fresh_node", ""))
    require(
        isinstance(manifest.get("schema_version"), int)
        and not isinstance(manifest["schema_version"], bool) and manifest["schema_version"] == 1
        and isinstance(manifest.get("source_build_id"), int)
        and not isinstance(manifest["source_build_id"], bool)
        and manifest["source_build_id"] == args.resume_build_id,
        "Resume manifest schema or source build ID does not match",
    )
    for key, expected in (
        ("resource_group", args.resource_group), ("role", args.role),
        ("source_worker", args.node_name), ("source_worker_uid", args.node_uid),
    ):
        require(prior.get(key) == expected and manifest.get(key) == expected, f"Resume {key} does not match the explicit scope")
    require(
        prior.get("scope") == "single-source-real-worker-cni-maintenance"
        and prior.get("source_provider_id") == args.source_provider_id
        and prior.get("source_network_container_id") == args.source_network_container_id,
        "Resume prior scope, source providerID or network container does not match",
    )
    for key, expected in (
        ("source_build_id", args.resume_build_id),
        ("expected_subscription", args.expected_subscription),
        ("expected_region", args.expected_region),
        ("expected_tfvars_sha", args.expected_tfvars_sha),
    ):
        require(key not in prior or prior[key] == expected, f"Resume prior {key} does not match")
    require(
        prior.get("execute") is True and prior.get("success") is False
        and prior.get("mutation_started") is True
        and prior.get("surge_request_accepted") is True
        and prior.get("source_quarantined") is True
        and prior.get("status") == (
            "proving-fresh-ip-growth" if recover_host else "waiting-for-surge"
        )
        and isinstance(prior.get("initial_pool_count"), int)
        and not isinstance(prior["initial_pool_count"], bool)
        and prior["initial_pool_count"] in (MIN_INITIAL_POOL_COUNT, STEADY_POOL_COUNT),
        "Resume requires a failed executed, quarantined, accepted pre-probe surge from count 2 or 3",
    )
    forbidden_prefixes = ("pending_", "healthy_", "probe_", "fresh_ip_", "retirement", "source_pre_drain", "destination_memory")
    for key, value in prior.items():
        if recover_host and key == "fresh_ip_growth":
            continue
        if key.startswith(forbidden_prefixes) or key in ("temporary_exclusions", "cleanup_errors", "cilium_final"):
            require(not value, f"Resume refuses prior mutation or unclean evidence: {key}")
    original = _identity_map(manifest.get("original_real_node_uids"), "Original real workers")
    fresh = _identity_map(manifest.get("fresh_node_uids"), "Fresh real workers")
    require(
        original.get(args.node_name) == args.node_uid
        and not set(original) & set(fresh)
        and not set(original.values()) & set(fresh.values())
        and len(fresh) == SURGE_POOL_COUNT - prior["initial_pool_count"],
        "Resume original and fresh worker identities overlap or counts differ",
    )
    _identity_map(manifest.get("fresh_network_container_ids"), "Fresh network containers", set(fresh))
    _identity_map(manifest.get("original_kwok_node_uids"), "Original KWOK Nodes", EXPECTED_AGENT_NAMES)
    _identity_map(manifest.get("agent_uids"), "Original mock agents", EXPECTED_AGENT_NAMES)
    require(isinstance(manifest.get("controller_uid"), str) and manifest["controller_uid"], "Original controller UID is required")
    for key in ("original_real_node_uids", "original_kwok_node_uids", "agent_uids", "controller_uid"):
        require(not recover_host or key in prior, f"Empty-host recovery requires persisted {key}")
        require(key not in prior or prior[key] == manifest[key], f"Resume manifest differs from persisted {key}")
    if recover_host:
        target = args.recover_empty_fresh_node
        growth = prior.get("fresh_ip_growth")
        require(
            target in fresh and fresh[target] == args.recover_empty_fresh_uid
            and isinstance(growth, dict) and set(growth) == set(fresh)
            and sorted(prior.get("fresh_nodes") or []) == sorted(fresh),
            "Host recovery target must be one explicitly pinned original fresh worker",
        )
        for name, row in growth.items():
            require(
                isinstance(row, dict) and row.get("node_uid") == fresh[name]
                and row.get("network_container_id") == manifest["fresh_network_container_ids"][name]
                and isinstance(row.get("initial_assigned"), int) and row["initial_assigned"] > 0,
                f"{name}: original IP-qualification evidence is not exact",
            )
            if name == target:
                require(
                    "after_assigned" not in row and not row.get("ready_probe_ips"),
                    "Refusing host recovery of an already IP-qualified fresh worker",
                )
            else:
                require(
                    isinstance(row.get("after_assigned"), int)
                    and row["after_assigned"] > row["initial_assigned"]
                    and row.get("ready_probe_ips"),
                    "Only one unqualified fresh worker may undergo host recovery",
                )
        if getattr(args, "replace_empty_fresh", False):
            previous = prior.get("empty_host_recovery") or {}
            require(
                previous.get("node_name") == target and previous.get("node_uid") == fresh[target]
                and previous.get("request_accepted") is True
                and previous.get("redeploy_completed") is True
                and previous.get("success") is False and previous.get("cordon_retained") is True,
                "Replacement requires the exact failed, quarantined post-redeploy target",
            )
    require(
        isinstance(prior.get("initial_pool_configuration"), dict)
        and prior["initial_pool_configuration"]
        and isinstance(prior.get("pod_template"), dict) and prior["pod_template"]
        and isinstance(prior.get("initial_worker_state"), dict) and prior["initial_worker_state"],
        "Resume requires the original worker state, pool configuration and Pod template",
    )
    _require_cilium_proof(prior.get("cilium_before") or {}, sorted(original))
    return {
        "prior": prior, "manifest": manifest,
        "provenance": {
            "source_build_id": args.resume_build_id,
            "summary_sha256": summary_sha, "manifest_sha256": manifest_sha,
            "prior_status": prior["status"], "prior_error": prior.get("error", ""),
            "prior_source_quarantined": True, "prior_surge_request_accepted": True,
        },
    }


def _require_failed_host_quarantine(args, nodes, resume) -> None:
    name = args.recover_empty_fresh_node
    target = nodes.get(name)
    previous = resume["prior"]["empty_host_recovery"]
    source_build = (resume["prior"].get("resume_provenance") or {}).get("source_build_id")
    expected = f"unqualified-empty-host-recovery build={source_build} uid={args.recover_empty_fresh_uid}"
    require(
        target is not None and target["metadata"]["uid"] == args.recover_empty_fresh_uid
        and previous["node_uid"] == args.recover_empty_fresh_uid
        and target["spec"].get("unschedulable") is True
        and _annotations(target).get(EMPTY_HOST_RECOVERY_KEY) == expected
        and all(
            isinstance(taint, dict)
            and {key: value for key, value in taint.items() if key != "timeAdded" and value != ""}
            == {"key": "node.kubernetes.io/unschedulable", "effect": "NoSchedule"}
            for taint in _taints(target)
        ),
        "Failed empty-host quarantine does not match the original recovery provenance",
    )


def _empty_host_target(operator, args, selected, resume) -> Tuple[dict, str, str, Set[str]]:
    target_name = args.recover_empty_fresh_node
    nodes = _real_node_map(operator.kubectl_json(["get", "nodes", "-o", "json"]))
    target = nodes.get(target_name)
    if getattr(args, "replace_empty_fresh", False):
        _require_failed_host_quarantine(args, nodes, resume)
        require(workers.node_is_ready(target), "The quarantined replacement target is not Ready")
    else:
        require(
            target is not None and target["metadata"]["uid"] == args.recover_empty_fresh_uid
            and workers.node_is_ready(target) and not target["spec"].get("unschedulable")
            and not _taints(target) and EMPTY_HOST_RECOVERY_KEY not in _annotations(target),
            "The exact empty fresh worker must be Ready, schedulable and unowned by another recovery",
        )
    _validate_real_node_scope(
        target, subscription=args.expected_subscription,
        node_resource_group=selected["nodeResourceGroup"],
    )
    identity = workers.provider_identity(target)
    require(identity is not None and mocks._node_pool_name(target) == DEFAULT_POOL_NAME,
            "The empty-host target is not an exact default-pool VMSS instance")
    view = operator.az_json(
        "vmss", "get-instance-view", "--resource-group", selected["nodeResourceGroup"],
        "--name", identity[0], "--instance-id", identity[1],
    )
    require(
        {"ProvisioningState/succeeded", "PowerState/running"}
        <= {row.get("code") for row in (view.get("statuses") or [])},
        "The exact empty-host instance is not quiescent and running",
    )
    pods = operator.kubectl_json(["get", "pods", "-A", "-o", "json"])
    target_pods = [
        pod for pod in _items(pods, "empty-host Pod inventory")
        if (pod.get("spec") or {}).get("nodeName") == target_name
    ]
    daemonsets = operator.kubectl_json(["-n", "kube-system", "get", "daemonsets", "-o", "json"])
    owners = {
        (row["metadata"]["name"], row["metadata"]["uid"])
        for row in _items(daemonsets, "empty-host DaemonSet inventory")
    }
    require(
        all(
            (pod.get("metadata") or {}).get("namespace") == "kube-system"
            and isinstance((pod.get("metadata") or {}).get("uid"), str)
            and bool(pod["metadata"]["uid"])
            and not any("persistentVolumeClaim" in volume for volume in (pod.get("spec") or {}).get("volumes", []))
            and any(
                owner.get("kind") == "DaemonSet" and owner.get("controller") is True
                and (owner.get("name"), owner.get("uid")) in owners
                for owner in (pod.get("metadata") or {}).get("ownerReferences", [])
            )
            for pod in target_pods
        ),
        "Fresh-host recovery requires zero mock, non-DaemonSet or PVC-backed target Pods",
    )
    require(
        target["metadata"]["uid"] == resume["manifest"]["fresh_node_uids"][target_name],
        "The empty-host target no longer matches its original fresh-worker identity",
    )
    return target, identity[0], identity[1], {pod["metadata"]["uid"] for pod in target_pods}


def _recover_empty_fresh_host(operator, args, selected, initial, resume, summary) -> None:
    target, vmss_name, instance_id, target_pod_uids = _empty_host_target(operator, args, selected, resume)
    target_name = args.recover_empty_fresh_node
    boot_id = ((target.get("status") or {}).get("nodeInfo") or {}).get("bootID")
    require(isinstance(boot_id, str) and boot_id, "The original target boot ID is required")
    recovery_budget = _phase_budget(
        operator, maximum_seconds=EMPTY_HOST_RECOVERY_SECONDS,
        reserve_after_seconds=RETIREMENT_MINIMUM_SECONDS + FINAL_QUALIFICATION_RESERVE_SECONDS,
        minimum_seconds=args.request_timeout_seconds, description="one empty-host redeployment",
    )
    record = summary["empty_host_recovery"] = {
        "node_name": target_name, "node_uid": args.recover_empty_fresh_uid,
        "vmss_name": vmss_name, "instance_id": instance_id,
        "previous_boot_id": boot_id, "request_attempted": False,
        "request_accepted": False, "cordon_retained": True, "success": False,
        "redeploy_completed": False,
    }
    annotation = f"empty-fresh-host-recovery build={args.resume_build_id} uid={args.recover_empty_fresh_uid}"
    _save(args.summary_file, summary)
    annotations = dict(_annotations(target))
    annotations[EMPTY_HOST_RECOVERY_KEY] = annotation
    operator.kubectl([
        "patch", "node", target_name, "--type=json", "-p", json.dumps([
            {"op": "test", "path": "/metadata/uid", "value": args.recover_empty_fresh_uid},
            {"op": "test", "path": "/metadata/resourceVersion", "value": target["metadata"]["resourceVersion"]},
            {"op": "add", "path": "/metadata/annotations", "value": annotations},
            {"op": "add", "path": "/spec/unschedulable", "value": True},
        ]),
    ])
    after_cordon_pods = operator.kubectl_json(["get", "pods", "-A", "-o", "json"])
    require(
        {
            pod["metadata"]["uid"] for pod in _items(after_cordon_pods, "cordoned empty-host inventory")
            if (pod.get("spec") or {}).get("nodeName") == target_name
        } <= target_pod_uids,
        "The empty-host Pod inventory changed before redeployment",
    )
    record["request_attempted"] = True
    redeploy_url = (
        f"https://management.azure.com/subscriptions/{args.expected_subscription}"
        f"/resourceGroups/{selected['nodeResourceGroup']}/providers/Microsoft.Compute"
        f"/virtualMachineScaleSets/{vmss_name}/virtualMachines/{instance_id}"
        "/redeploy?api-version=2026-04-01"
    )
    record["redeploy_url"] = redeploy_url
    _save(args.summary_file, summary)
    operator.run([
        "az", "rest", "--method", "post", "--url", redeploy_url,
        "--output", "none", "--only-show-errors",
    ], args.request_timeout_seconds)
    record["request_accepted"] = True
    summary["status"] = "recovering-empty-fresh-host"
    _save(args.summary_file, summary)
    deadline = min(operator.work_deadline, time.monotonic() + recovery_budget)
    expected_nodes = {
        **resume["manifest"]["original_real_node_uids"], **resume["manifest"]["fresh_node_uids"],
    }
    while time.monotonic() < deadline:
        nodes = _real_node_map(operator.kubectl_json(["get", "nodes", "-o", "json"]))
        require(
            {name: node["metadata"]["uid"] for name, node in nodes.items()} == expected_nodes,
            "A real worker identity changed during the bounded host redeployment",
        )
        agents = _require_exact_agents(
            operator.kubectl_json(["-n", DEFAULT_NAMESPACE, "get", "pods", "-l", mocks.AGENT_SELECTOR, "-o", "json"]),
            initial["controller_uid"],
        )
        require(
            {name: pod["metadata"]["uid"] for name, pod in agents.items()} == initial["agent_uids"]
            and all(mocks._pod_ready(agents[name]) and _readiness_condition_true(agents[name])
                    for name in initial["initial_ready_agent_uids"]),
            "An original mock agent changed during the empty-host recovery",
        )
        view = operator.az_json(
            "vmss", "get-instance-view", "--resource-group", selected["nodeResourceGroup"],
            "--name", vmss_name, "--instance-id", instance_id,
        )
        codes = {row.get("code") for row in (view.get("statuses") or [])}
        target = nodes[target_name]
        current_boot = ((target.get("status") or {}).get("nodeInfo") or {}).get("bootID")
        record["last_instance_codes"] = sorted(code for code in codes if isinstance(code, str))
        record["current_boot_id"] = current_boot
        _save(args.summary_file, summary)
        if (
            {"ProvisioningState/succeeded", "PowerState/running"} <= codes
            and workers.node_is_ready(target) and current_boot and current_boot != boot_id
        ):
            break
        require(
            not any(str(code).lower().startswith("provisioningstate/failed") for code in codes),
            "The targeted host redeployment failed; refusing another request",
        )
        time.sleep(min(args.poll_seconds, operator.remaining_seconds(args.poll_seconds)))
    else:
        raise workers.ReconcileError("One empty-host redeployment exceeded its bounded observation window")
    target = operator.kubectl_json(["get", "node", target_name, "-o", "json"])
    operator.kubectl([
        "patch", "node", target_name, "--type=json", "-p", json.dumps([
            {"op": "test", "path": "/metadata/uid", "value": args.recover_empty_fresh_uid},
            {"op": "test", "path": f"/metadata/annotations/{EMPTY_HOST_RECOVERY_KEY.replace('/', '~1')}", "value": annotation},
            {"op": "add", "path": "/spec/unschedulable", "value": False},
            {"op": "remove", "path": f"/metadata/annotations/{EMPTY_HOST_RECOVERY_KEY.replace('/', '~1')}"},
        ]),
    ])
    restore_deadline = min(operator.work_deadline, time.monotonic() + 90)
    while time.monotonic() < restore_deadline:
        target = operator.kubectl_json(["get", "node", target_name, "-o", "json"])
        require(target["metadata"]["uid"] == args.recover_empty_fresh_uid, "Recovered host UID changed")
        if workers.node_is_ready(target) and not target["spec"].get("unschedulable") and not _taints(target):
            record["cordon_retained"] = False
            record["redeploy_completed"] = True
            _save(args.summary_file, summary)
            return
        time.sleep(min(args.poll_seconds, operator.remaining_seconds(args.poll_seconds)))
    raise workers.ReconcileError("Recovered host did not restore normal scheduling")


def _observe_empty_replacement(operator, args, initial, retained, old_name, old_uid):
    nodes = operator.kubectl_json(["get", "nodes", "-o", "json"])
    real = _real_node_map(nodes)
    for name, uid in retained.items():
        require(
            name in real and real[name]["metadata"]["uid"] == uid
            and workers.node_is_ready(real[name])
            and not real[name]["metadata"].get("deletionTimestamp")
            and bool(real[name]["spec"].get("unschedulable")) == (name == args.node_name),
            f"{name}: retained worker identity or health changed during empty replacement",
        )
    _require_resume_hold(args, {name: real[name] for name in retained})
    if old_name in real:
        require(real[old_name]["metadata"]["uid"] == old_uid, "Failed target UID changed before removal")
    require(len(set(real) - set(retained)) <= 1, "More than one replacement worker appeared")
    require(
        {name: node["metadata"]["uid"] for name, node in _require_exact_kwok_nodes(nodes).items()}
        == initial["kwok_uids"],
        "Original KWOK identities changed during empty-worker replacement",
    )
    pods = operator.kubectl_json(["get", "pods", "-A", "-o", "json"])
    agents = _require_exact_agents(pods, initial["controller_uid"])
    require(
        {name: pod["metadata"]["uid"] for name, pod in agents.items()} == initial["agent_uids"]
        and all(mocks._pod_ready(agents[name]) and _readiness_condition_true(agents[name])
                for name in initial["initial_ready_agent_uids"]),
        "Original mock agents changed during empty-worker replacement",
    )
    return nodes, real, pods


def _replace_empty_fresh_host(operator, args, selected, initial, resume, summary) -> None:
    target, vmss_name, instance_id, _ = _empty_host_target(operator, args, selected, resume)
    old_name, old_uid = target["metadata"]["name"], target["metadata"]["uid"]
    original_manifest = resume["manifest"]
    retained = {
        name: uid for name, uid in {
            **original_manifest["original_real_node_uids"], **original_manifest["fresh_node_uids"],
        }.items() if name != old_name
    }
    budget = _phase_budget(
        operator, maximum_seconds=EMPTY_HOST_RECOVERY_SECONDS,
        reserve_after_seconds=RETIREMENT_MINIMUM_SECONDS + FINAL_QUALIFICATION_RESERVE_SECONDS,
        minimum_seconds=args.request_timeout_seconds, description="one empty-worker replacement",
    )
    deadline = time.monotonic() + budget
    record = summary["empty_worker_replacement"] = {
        "old_node_name": old_name, "old_node_uid": old_uid, "old_instance_id": instance_id,
        "old_network_container_id": original_manifest["fresh_network_container_ids"][old_name],
        "delete_attempted": True, "delete_accepted": False,
        "restore_attempted": False, "restore_accepted": False,
        "replacement_completed": False, "success": False,
    }
    summary["status"] = "removing-unqualified-empty-worker"
    _save(args.summary_file, summary)
    operator.run([
        "az", "aks", "nodepool", "delete-machines", "--resource-group", args.resource_group,
        "--cluster-name", selected["name"], "--name", DEFAULT_POOL_NAME,
        "--machine-names", old_name, "--no-wait", "--only-show-errors",
    ], args.request_timeout_seconds)
    record["delete_accepted"] = True
    _save(args.summary_file, summary)
    while time.monotonic() < deadline:
        pool = _current_pool_payload(operator, args, selected["name"])
        require(
            pool.get("count") in (3, 4)
            and pool.get("provisioningState") in ("DeletingMachines", "Succeeded")
            and (pool.get("powerState") or {}).get("code") == "Running"
            and _pool_configuration(pool) == initial["initial_pool_configuration"],
            "Pool state or configuration drifted during exact empty-worker removal",
        )
        _, real, pods = _observe_empty_replacement(operator, args, initial, retained, old_name, old_uid)
        require(set(real) <= set(retained) | {old_name}, "An unexpected worker appeared before restoration")
        nnc = _nnc_map(operator.kubectl_json(["get", "nnc", "-A", "-o", "json"]))
        removed = old_name not in real and old_name not in nnc and not any(
            (pod.get("spec") or {}).get("nodeName") == old_name for pod in _items(pods, "target Pod references")
        )
        if pool.get("count") == 3 and pool.get("provisioningState") == "Succeeded" and removed:
            state = workers.probe_cluster(
                workers.Cluster(selected["name"], args.resource_group, args.role, args.kubeconfig),
                lambda command, timeout: operator.run(command, timeout), args.request_timeout_seconds,
            )
            _validate_pre_scale_state(
                args, selected, state, pool, operator.kubectl_json(["get", "nodes", "-o", "json"]),
                3, initial["initial_pool_configuration"], retained,
            )
            require(instance_id not in _pool_from_state(state, DEFAULT_POOL_NAME).instance_ids,
                    "The exact failed VMSS instance remains after machine deletion")
            record["intermediate_pool_count"] = 3
            break
        time.sleep(min(args.poll_seconds, operator.remaining_seconds(args.poll_seconds)))
    else:
        raise workers.ReconcileError("The exact failed empty worker did not finish removal")
    require(time.monotonic() < deadline, "No bounded time remains for replacement capacity")
    record["restore_attempted"] = True
    summary["status"] = "restoring-owned-replacement-capacity"
    _save(args.summary_file, summary)
    operator.run([
        "az", "aks", "nodepool", "scale", "--resource-group", args.resource_group,
        "--cluster-name", selected["name"], "--name", DEFAULT_POOL_NAME,
        "--node-count", "4", "--no-wait", "--output", "none", "--only-show-errors",
    ], args.request_timeout_seconds)
    record["restore_accepted"] = True
    _save(args.summary_file, summary)
    observed_new = None
    while time.monotonic() < deadline:
        pool = _current_pool_payload(operator, args, selected["name"])
        require(
            pool.get("count") in (3, 4)
            and pool.get("provisioningState") in ("Scaling", "Updating", "Succeeded")
            and (pool.get("powerState") or {}).get("code") == "Running"
            and _pool_configuration(pool) == initial["initial_pool_configuration"],
            "Pool state or configuration drifted during bounded capacity restoration",
        )
        nodes, real, _ = _observe_empty_replacement(operator, args, initial, retained, old_name, old_uid)
        require(old_name not in real, "The retired target reappeared instead of a new worker")
        candidates = [node for name, node in real.items() if name not in retained]
        if candidates:
            new = candidates[0]
            identity = _node_identity(new)
            require(observed_new is None or observed_new == identity, "The observed replacement identity changed")
            observed_new = identity
            _validate_real_node_scope(new, subscription=args.expected_subscription, node_resource_group=selected["nodeResourceGroup"])
            provider = workers.provider_identity(new)
            require(
                provider is not None and provider[0] == vmss_name and provider[1] != instance_id
                and mocks._node_pool_name(new) == DEFAULT_POOL_NAME
                and identity["uid"] not in set(retained.values()) | {old_uid}
                and identity["image"] == pool.get("nodeImageVersion"),
                "The new worker is not a distinct instance of the original default pool",
            )
            nnc = _nnc_map(operator.kubectl_json(["get", "nnc", "-A", "-o", "json"]))
            new_name = new["metadata"]["name"]
            network = nnc.get(new_name)
            if (
                pool.get("count") == 4 and pool.get("provisioningState") == "Succeeded"
                and workers.node_is_ready(new) and not new["spec"].get("unschedulable")
                and not _taints(new) and network is not None
                and network["node_uid"] == identity["uid"]
                and network["network_container_id"] != record["old_network_container_id"]
            ):
                state = workers.probe_cluster(
                    workers.Cluster(selected["name"], args.resource_group, args.role, args.kubeconfig),
                    lambda command, timeout: operator.run(command, timeout), args.request_timeout_seconds,
                )
                _validate_pre_scale_state(
                    args, selected, state, pool, nodes, 4, initial["initial_pool_configuration"],
                    {**retained, new_name: identity["uid"]},
                )
                manifest = dict(original_manifest)
                manifest["fresh_node_uids"] = {
                    name: uid for name, uid in original_manifest["fresh_node_uids"].items() if name != old_name
                }
                manifest["fresh_network_container_ids"] = {
                    name: nc for name, nc in original_manifest["fresh_network_container_ids"].items() if name != old_name
                }
                manifest["fresh_node_uids"][new_name] = identity["uid"]
                manifest["fresh_network_container_ids"][new_name] = network["network_container_id"]
                resume["manifest"] = manifest
                resume["replacement_completed"] = True
                summary["replacement_derived_manifest"] = manifest
                summary["resume_fresh_network_container_ids"] = manifest["fresh_network_container_ids"]
                record.update({
                    "node_name": new_name, "node_uid": identity["uid"], "new_instance_id": provider[1],
                    "network_container_id": network["network_container_id"],
                    "replacement_completed": True, "cordon_retained": False, "pool_count": 4,
                })
                _save(args.summary_file, summary)
                return
        time.sleep(min(args.poll_seconds, operator.remaining_seconds(args.poll_seconds)))
    raise workers.ReconcileError("Replacement capacity did not return to exactly four healthy workers")


def _require_resume_hold(args, nodes: dict) -> None:
    hold = {"key": HOLD_ANNOTATION, "value": HOLD_REASON, "effect": "NoSchedule"}
    cordon = {"key": "node.kubernetes.io/unschedulable", "effect": "NoSchedule"}
    for name, node in nodes.items():
        annotations = _annotations(node)
        taints = _taints(node)
        if name == args.node_name:
            require(
                annotations.get(HOLD_ANNOTATION)
                == f"bounded-worker-retirement role={args.role} node={args.node_name} uid={args.node_uid}"
                and taints.count(hold) == 1
                and all(
                    taint == hold or (
                        isinstance(taint, dict)
                        and {key: value for key, value in taint.items() if key != "timeAdded"}
                        in (cordon, {**cordon, "value": ""})
                    )
                    for taint in taints
                ),
                "Resume source hold annotation or taint is not the exact original quarantine",
            )
        else:
            require(not taints and HOLD_ANNOTATION not in annotations, f"{name}: unexpected hold or taint during resume")
        require(
            not any(key.startswith("mock-clustermesh/") and key != HOLD_ANNOTATION for key in annotations),
            f"{name}: unclean maintenance annotation during resume",
        )


def _validate_resume_state(
    args, selected, cluster_state, pool_payload, nodes_payload, pods_payload,
    events_payload, controller_payload, nnc_payload, resume,
) -> dict:
    prior, manifest = resume["prior"], resume["manifest"]
    original = manifest["original_real_node_uids"]
    fresh = manifest["fresh_node_uids"]
    union = {**original, **fresh}
    replacement_target = (
        args.recover_empty_fresh_node
        if getattr(args, "replace_empty_fresh", False) and not resume.get("replacement_completed") else ""
    )
    real = _real_node_map(nodes_payload)
    if replacement_target:
        _require_failed_host_quarantine(args, real, resume)
    _validate_pre_scale_state(
        args, selected, cluster_state, pool_payload, nodes_payload,
        SURGE_POOL_COUNT, prior["initial_pool_configuration"], union,
        additional_quarantined_node=replacement_target,
    )
    require(
        len(_items(nodes_payload, "Node inventory")) == len(real) + 100,
        "Resume Node inventory contains unexpected identities",
    )
    _require_resume_hold(args, {name: node for name, node in real.items() if name != replacement_target})
    for name, node in real.items():
        require(
            workers.node_is_ready(node)
            and not (node.get("metadata") or {}).get("deletionTimestamp")
            and bool((node.get("spec") or {}).get("unschedulable"))
            == (name in {args.node_name, replacement_target}),
            f"{name}: resume real worker health or scheduling state changed",
        )
    default_pool = _pool_from_state(cluster_state, DEFAULT_POOL_NAME)
    original_state = prior.get("initial_worker_state") or {}
    require(
        original_state.get("role") == args.role
        and original_state.get("resource_group") == args.resource_group
        and original_state.get("cluster_name") == selected["name"]
        and original_state.get("healthy") is True,
        "Resume original worker state scope or health is invalid",
    )
    old_pools = original_state.get("pools") or []
    require(
        isinstance(old_pools, list) and all(isinstance(pool, dict) for pool in old_pools)
        and len(old_pools) == len(cluster_state.pools)
        and len({pool.get("pool_name") for pool in old_pools}) == len(old_pools)
        and {pool.get("pool_name") for pool in old_pools} == {pool.pool_name for pool in cluster_state.pools},
        "Resume real pool inventory differs from the original",
    )
    old_by_pool = {pool["pool_name"]: pool for pool in old_pools}
    for pool in cluster_state.pools:
        old = old_by_pool[pool.pool_name]
        original_nodes = [real[name] for name in original if mocks._node_pool_name(real[name]) == pool.pool_name]
        current_nodes = [row for row in real.values() if mocks._node_pool_name(row) == pool.pool_name]
        original_ids = sorted(workers.provider_identity(row)[1] for row in original_nodes)
        current_ids = sorted(workers.provider_identity(row)[1] for row in current_nodes)
        expected_old_count = prior["initial_pool_count"] if pool.pool_name == DEFAULT_POOL_NAME else pool.desired_count
        require(
            all(old.get(key) == getattr(pool, key) for key in ("role", "cluster_name", "resource_group", "node_resource_group", "vmss_name"))
            and old.get("healthy") is True
            and old.get("pool_provisioning_state") == old.get("vmss_provisioning_state") == "Succeeded"
            and old.get("pool_power_state") == "Running"
            and old.get("desired_count") == old.get("vmss_capacity") == expected_old_count
            and len(original_ids) == len(set(original_ids)) == expected_old_count
            and all(old.get(key) == original_ids for key in ("instance_ids", "node_instance_ids", "ready_instance_ids"))
            and not any(old.get(key) for key in ("failed_instance_ids", "stale_instance_ids", "unsafe_reasons", "unschedulable_nodes"))
            and not pool.failed_instance_ids
            and len(current_ids) == len(set(current_ids)) == pool.desired_count
            and pool.instance_ids == pool.node_instance_ids == pool.ready_instance_ids == current_ids
            and all(workers.provider_identity(row)[0] == pool.vmss_name.lower() for row in current_nodes),
            f"{pool.pool_name}: resume original/current VMSS instance identities or configuration changed",
        )
    require(all(mocks._node_pool_name(real[name]) == DEFAULT_POOL_NAME for name in fresh), "Resume fresh workers must belong to the original default pool")
    if "original_real_node_identities" in prior:
        require(
            prior["original_real_node_identities"] == {name: _node_identity(real[name]) for name in original},
            "Resume original real worker provider, pool, or image identity changed",
        )
    initial = _validate_source_workload(
        args, selected, default_pool, nodes_payload, pods_payload, events_payload,
        controller_payload, nnc_payload, source_held=True,
    )
    require(
        initial["controller_uid"] == manifest["controller_uid"]
        and initial["agent_uids"] == manifest["agent_uids"]
        and initial["kwok_uids"] == manifest["original_kwok_node_uids"]
        and initial["pod_template"] == prior["pod_template"],
        "Resume original agent, KWOK, controller UID or template changed",
    )
    for kind in ("pending", "healthy"):
        names = [row["name"] for row in initial[f"{kind}_source_agents"]]
        old_names = prior.get(f"initial_{kind}_source_agents")
        require(
            isinstance(old_names, list) and all(isinstance(name, str) for name in old_names)
            and len(old_names) == len(set(old_names))
            and sorted(old_names) == sorted(names),
            f"Resume original {kind} source agent set changed",
        )
    agents = _agent_map(pods_payload)
    require(
        all((pod.get("spec") or {}).get("nodeName") in original for pod in agents.values()),
        "Resume agents have already moved to non-original workers",
    )
    nncs = _nnc_map(nnc_payload)
    require(
        len({row["network_container_id"] for row in nncs.values()}) == len(nncs),
        "Resume network container identities are not unique",
    )
    for name, uid in fresh.items():
        require(
            name in nncs and nncs[name]["node_uid"] == uid
            and nncs[name]["network_container_id"] == manifest["fresh_network_container_ids"][name],
            f"{name}: resume fresh network container identity changed",
        )
    initial.update({
        "initial_pool_count": prior["initial_pool_count"],
        "expected_fresh_nodes": SURGE_POOL_COUNT - prior["initial_pool_count"],
        "initial_pool_configuration": prior["initial_pool_configuration"],
        "all_real_node_uids": original,
        "initial_real_node_uids": {
            name: uid for name, uid in original.items()
            if mocks._node_pool_name(real[name]) == DEFAULT_POOL_NAME
        },
    })
    return initial


class ClusterOperator:
    """Pinned Azure and kubectl command helper for one exact cluster."""

    def __init__(
        self,
        args,
        cluster_name: str,
        runner: Runner,
        work_deadline: float,
        cleanup_deadline: float,
    ):
        self.args = args
        self.cluster_name = cluster_name
        self.runner = runner
        self.work_deadline = work_deadline
        self.cleanup_deadline = cleanup_deadline
        self.cleanup_mode = False

    def remaining_seconds(
        self,
        configured: int,
        *,
        cleanup: Optional[bool] = None,
        grace_seconds: int = 0,
    ) -> int:
        use_cleanup = self.cleanup_mode if cleanup is None else cleanup
        deadline = self.cleanup_deadline if use_cleanup else self.work_deadline
        remaining = int(deadline - time.monotonic()) - grace_seconds
        if remaining < 1:
            raise workers.ReconcileError(
                "CNI worker maintenance cleanup deadline expired"
                if use_cleanup else
                "CNI worker maintenance deadline expired"
            )
        return min(configured, remaining)

    def _bind_kubectl(self, command: List[str]) -> List[str]:
        require(command and command[0] == "kubectl", "kubectl binding requires a kubectl command")
        if "--kubeconfig" in command:
            kubeconfig = command[command.index("--kubeconfig") + 1]
            require(
                os.path.abspath(kubeconfig) == os.path.abspath(self.args.kubeconfig),
                "kubectl attempted to escape the selected kubeconfig",
            )
        else:
            command[1:1] = ["--kubeconfig", self.args.kubeconfig]
        if "--context" in command:
            context = command[command.index("--context") + 1]
            require(
                context == (self.args.context or self.cluster_name),
                "kubectl attempted to escape the selected context",
            )
        else:
            command[1:1] = ["--context", self.args.context or self.cluster_name]
        return command

    def run(
        self,
        command: Sequence[str],
        timeout_seconds: int,
        *,
        cleanup: bool = False,
    ) -> str:
        command = list(command)
        if command and command[0] == "az" and command[1:3] != ["account", "show"]:
            if "--subscription" in command:
                require(
                    command[command.index("--subscription") + 1].lower()
                    == self.args.expected_subscription.lower(),
                    "Azure command attempted to escape the selected subscription",
                )
            else:
                command.extend(["--subscription", self.args.expected_subscription])
        if command and command[0] == "kubectl":
            command = self._bind_kubectl(command)
        return self.runner(command, self.remaining_seconds(timeout_seconds, cleanup=cleanup))

    def az_json(self, *command: str, timeout_seconds: int = 45) -> object:
        output = self.run(
            ["az", *command, "--output", "json", "--only-show-errors"],
            timeout_seconds,
        )
        return workers.parse_json(output, f"Azure {' '.join(command[:3])}")

    def kubectl(self, command: Sequence[str], *, timeout_seconds: int = 45, cleanup: bool = False) -> str:
        return self.run(["kubectl", *command], timeout_seconds, cleanup=cleanup)

    def kubectl_json(
        self,
        command: Sequence[str],
        *,
        timeout_seconds: int = 45,
        cleanup: bool = False,
    ) -> dict:
        return workers.parse_json(
            self.kubectl(command, timeout_seconds=timeout_seconds, cleanup=cleanup),
            f"kubectl {' '.join(command[:3])}",
        )


def _work_seconds_remaining(operator: ClusterOperator) -> int:
    remaining = int(operator.work_deadline - time.monotonic())
    require(remaining > 0, "CNI worker maintenance deadline expired")
    return remaining


def _phase_budget(
    operator: ClusterOperator,
    *,
    maximum_seconds: int,
    reserve_after_seconds: int,
    minimum_seconds: int,
    description: str,
) -> int:
    remaining = _work_seconds_remaining(operator) - reserve_after_seconds
    require(
        remaining >= minimum_seconds,
        f"Not enough bounded time remains for {description}",
    )
    return min(maximum_seconds, remaining)


def _require_retirement_reserve(operator: ClusterOperator) -> None:
    _phase_budget(
        operator,
        maximum_seconds=RETIREMENT_PHASE_BUDGET_SECONDS,
        reserve_after_seconds=FINAL_QUALIFICATION_RESERVE_SECONDS,
        minimum_seconds=RETIREMENT_MINIMUM_SECONDS,
        description="retirement and final qualification",
    )


def _save(summary_file: str, summary: dict) -> None:
    mocks.write_json_atomic(summary_file, summary)


def _read_cilium_proof(
    operator: ClusterOperator,
    args,
    identities: Sequence[dict],
) -> dict:
    expected_names = {
        str(row["cluster_name"])
        for row in identities
        if isinstance(row, dict) and row.get("role") != args.role
    }

    def runner(command, timeout):
        try:
            return operator.run(command, timeout)
        except workers.ReconcileError as error:
            raise cilium.overlay.ProbeError(str(error)) from error

    proof = cilium.probe(
        role=args.role,
        kubeconfig=args.kubeconfig,
        expected_remote_count=EXPECTED_REMOTE_COUNT,
        expected_remote_names=expected_names,
        attempts=1,
        retry_seconds=0,
        command_timeout_seconds=45,
        runner=runner,
    )
    agents = proof.get("agents") or []
    proof["covered_node_names"] = sorted(
        {
            str(agent.get("node_name") or "")
            for agent in agents
            if isinstance(agent, dict) and agent.get("healthy") is True
        }
    )
    return proof


def _require_cilium_proof(proof: dict, required_nodes: Sequence[str]) -> None:
    covered = set(proof.get("covered_node_names") or [])
    require(
        proof.get("healthy") is True and set(required_nodes) <= covered,
        "Strict 99-peer Cilium proof must cover every required real worker",
    )


def _current_pool_payload(operator: ClusterOperator, args, cluster_name: str) -> dict:
    payload = operator.az_json(
        "aks",
        "nodepool",
        "show",
        "--resource-group",
        args.resource_group,
        "--cluster-name",
        cluster_name,
        "--name",
        DEFAULT_POOL_NAME,
        timeout_seconds=args.request_timeout_seconds,
    )
    require(isinstance(payload, dict), "AKS node-pool response is not an object")
    return payload


def _wait_for_surge(
    operator: ClusterOperator,
    args,
    cluster_name: str,
    baseline_config: dict,
    initial_pool_count: int,
) -> dict:
    last = {}
    while time.monotonic() < operator.work_deadline:
        current = _current_pool_payload(operator, args, cluster_name)
        last = current
        require(
            _pool_configuration(current) == baseline_config,
            "Default pool configuration drifted during the surge",
        )
        count = current.get("count")
        provisioning = current.get("provisioningState")
        power = (current.get("powerState") or {}).get("code")
        require(current.get("enableAutoScaling") is False, "Default pool autoscaler changed during the surge")
        require(power == "Running", "Default pool power state changed during the surge")
        if count == SURGE_POOL_COUNT and provisioning == "Succeeded":
            return current
        require(
            isinstance(count, int)
            and initial_pool_count <= count <= SURGE_POOL_COUNT,
            f"Default pool count drifted outside the bounded surge: {count!r}",
        )
        require(
            provisioning in ("Scaling", "Updating", "Succeeded"),
            f"Default pool entered unsupported provisioningState={provisioning!r}",
        )
        time.sleep(min(args.poll_seconds, operator.remaining_seconds(args.poll_seconds)))
    raise workers.ReconcileError(
        f"Default pool did not converge to temporary count {SURGE_POOL_COUNT}; "
        f"last state={last.get('provisioningState')!r} count={last.get('count')!r}"
    )


def _fresh_nodes_after_scale(
    args,
    selected_cluster: dict,
    nodes_payload: dict,
    initial_all_real_node_uids: Dict[str, str],
    initial_real_node_uids: Dict[str, str],
    expected_fresh_nodes: int,
) -> List[dict]:
    all_real_nodes = _real_node_map(nodes_payload)
    require(
        len(_items(nodes_payload, "Node inventory"))
        == len(all_real_nodes) + len(_kwok_map(nodes_payload)),
        "Unexpected or ambiguous Node inventory during the surge",
    )
    for name, uid in initial_all_real_node_uids.items():
        node = all_real_nodes.get(name)
        require(node is not None, f"{name}: an existing real worker disappeared during the surge")
        require(
            str((node.get("metadata") or {}).get("uid") or "") == uid,
            f"{name}: an existing real worker was replaced during the surge",
        )
    current = _real_pool_nodes(
        nodes_payload,
        pool_name=DEFAULT_POOL_NAME,
        subscription=args.expected_subscription,
        node_resource_group=str(selected_cluster["nodeResourceGroup"]),
    )
    require(
        set(all_real_nodes) - set(current)
        == set(initial_all_real_node_uids) - set(initial_real_node_uids),
        "Unexpected non-default real worker during the surge",
    )
    require(
        len(initial_real_node_uids) <= len(current) <= SURGE_POOL_COUNT,
        "The default pool drifted outside the exact bounded worker count",
    )
    for name, uid in initial_real_node_uids.items():
        node = current.get(name)
        require(node is not None, f"{name}: an existing default worker disappeared during the surge")
        require(
            str((node.get("metadata") or {}).get("uid") or "") == uid,
            f"{name}: an existing default worker was replaced instead of preserved",
        )
    fresh = [
        node for name, node in sorted(current.items())
        if name not in initial_real_node_uids
    ]
    require(
        len(fresh) <= expected_fresh_nodes,
        f"Expected at most {expected_fresh_nodes} fresh default worker(s)",
    )
    pending = len(fresh) != expected_fresh_nodes
    startup_taints = [
        {"key": "node.kubernetes.io/not-ready", "effect": effect}
        for effect in ("NoSchedule", "NoExecute")
    ]
    expected_vmss = workers.provider_identity(current[args.node_name])[0]
    for node in fresh:
        require(
            (node.get("metadata") or {}).get("uid")
            and not (node.get("metadata") or {}).get("deletionTimestamp")
            and not (node.get("spec") or {}).get("unschedulable")
            and workers.provider_identity(node)[0] == expected_vmss,
            "Fresh default worker identity, VMSS, deletion, or scheduling state is unsafe",
        )
        taints = [
            {key: value for key, value in taint.items() if key != "timeAdded" and value != ""}
            for taint in _taints(node) if isinstance(taint, dict)
        ]
        require(
            len(taints) == len(_taints(node))
            and all(taint in startup_taints for taint in taints),
            "Unexpected fresh worker taint during the surge",
        )
        pending = pending or not workers.node_is_ready(node) or bool(taints)
    if pending:
        raise FreshWorkersNotReady("Fresh default workers have not all registered Ready and schedulable")
    return fresh


def _node_identity(node: dict) -> dict:
    return {
        "uid": (node.get("metadata") or {}).get("uid"),
        "provider_id": _normalize_provider_id(str((node.get("spec") or {}).get("providerID") or "")),
        "pool": mocks._node_pool_name(node),
        "image": ((node.get("metadata") or {}).get("labels") or {}).get("kubernetes.azure.com/node-image-version"),
    }


def _startup_operator(operator: ClusterOperator) -> ClusterOperator:
    budget = _phase_budget(
        operator,
        maximum_seconds=SURGE_READY_WAIT_SECONDS,
        reserve_after_seconds=RETIREMENT_MINIMUM_SECONDS + FINAL_QUALIFICATION_RESERVE_SECONDS,
        minimum_seconds=1,
        description="fresh worker startup, retirement and final qualification",
    )
    return ClusterOperator(
        operator.args, operator.cluster_name, operator.runner,
        time.monotonic() + budget, operator.cleanup_deadline,
    )


def _wait_for_fresh_nodes(
    operator: ClusterOperator,
    args,
    selected: dict,
    initial: dict,
    original_nodes: dict,
) -> Tuple[dict, List[dict]]:
    original = _real_node_map(original_nodes)
    seen = {}
    while time.monotonic() < operator.work_deadline:
        pool = _current_pool_payload(operator, args, operator.cluster_name)
        require(
            pool.get("count") == SURGE_POOL_COUNT
            and pool.get("provisioningState") == "Succeeded"
            and (pool.get("powerState") or {}).get("code") == "Running"
            and pool.get("enableAutoScaling") is False
            and _pool_configuration(pool) == initial["initial_pool_configuration"],
            "Default pool drifted after accepted surge",
        )
        state = workers.probe_cluster(
            workers.Cluster(operator.cluster_name, args.resource_group, args.role, args.kubeconfig),
            lambda command, timeout: operator.run(command, timeout),
            args.request_timeout_seconds,
        )
        for observed_pool in state.pools:
            if observed_pool.pool_name != DEFAULT_POOL_NAME:
                require(observed_pool.healthy and not observed_pool.failed_instance_ids, "Unrelated real pool drifted during startup")
                continue
            original_instances = {
                workers.provider_identity(original[name])[1]
                for name in initial["initial_real_node_uids"]
            }
            require(
                observed_pool.desired_count == observed_pool.vmss_capacity == SURGE_POOL_COUNT
                and observed_pool.pool_provisioning_state == observed_pool.vmss_provisioning_state == "Succeeded"
                and observed_pool.pool_power_state == "Running"
                and len(observed_pool.instance_ids) == len(set(observed_pool.instance_ids)) == SURGE_POOL_COUNT
                and not observed_pool.failed_instance_ids
                and set(observed_pool.stale_instance_ids) <= set(observed_pool.instance_ids) - original_instances
                and original_instances <= set(observed_pool.ready_instance_ids)
                and set(observed_pool.ready_instance_ids) <= set(observed_pool.node_instance_ids) <= set(observed_pool.instance_ids)
                and not observed_pool.unsafe_reasons
                and observed_pool.unschedulable_nodes == [args.node_name]
                and original_instances <= set(observed_pool.instance_ids),
                "Default VMSS identity, health or count drifted during startup",
            )
        _pool_from_state(state, DEFAULT_POOL_NAME)
        nodes = operator.kubectl_json(
            ["get", "nodes", "-o", "json"], timeout_seconds=args.request_timeout_seconds,
        )
        current = _real_node_map(nodes)
        _require_resume_hold(args, {name: current[name] for name in original if name in current})
        for name, old in original.items():
            observed = current.get(name) or {}
            require(
                _node_identity(observed) == _node_identity(old)
                and workers.node_is_ready(observed)
                and not (observed.get("metadata") or {}).get("deletionTimestamp")
                and bool((observed.get("spec") or {}).get("unschedulable")) == (name == args.node_name),
                f"{name}: original real worker identity or health drifted during startup",
            )
        for name, identity in seen.items():
            require(
                name in current and _node_identity(current[name]) == identity,
                f"{name}: fresh worker identity drifted during startup",
            )
        for name, observed in current.items():
            if name not in original:
                require(
                    _node_identity(observed)["image"] == pool.get("nodeImageVersion"),
                    f"{name}: fresh worker image drifted during startup",
                )
                seen[name] = _node_identity(observed)
        try:
            fresh = _fresh_nodes_after_scale(
                args, selected, nodes, initial["all_real_node_uids"],
                initial["initial_real_node_uids"], initial["expected_fresh_nodes"],
            )
        except FreshWorkersNotReady:
            time.sleep(min(args.poll_seconds, operator.remaining_seconds(args.poll_seconds)))
            continue
        return nodes, fresh
    raise workers.ReconcileError("Fresh default worker readiness deadline expired")


def _requalify_surge(operator, args, selected, initial, pinned_nodes, resume) -> None:
    nodes = operator.kubectl_json(
        ["get", "nodes", "-o", "json"], timeout_seconds=args.request_timeout_seconds,
    )
    expected = {name: _node_identity(row) for name, row in _real_node_map(pinned_nodes).items()}
    require(
        {name: _node_identity(row) for name, row in _real_node_map(nodes).items()} == expected,
        "Pinned post-surge real worker identities changed before probing",
    )
    pool = _current_pool_payload(operator, args, operator.cluster_name)
    state = workers.probe_cluster(
        workers.Cluster(operator.cluster_name, args.resource_group, args.role, args.kubeconfig),
        lambda command, timeout: operator.run(command, timeout),
        args.request_timeout_seconds,
    )
    if resume:
        _validate_resume_state(
            args, selected, state, pool, nodes,
            operator.kubectl_json(["-n", DEFAULT_NAMESPACE, "get", "pods", "-l", mocks.AGENT_SELECTOR, "-o", "json"]),
            operator.kubectl_json(["-n", DEFAULT_NAMESPACE, "get", "events", "-o", "json"]),
            operator.kubectl_json(["-n", DEFAULT_NAMESPACE, "get", "statefulset", "kwok-node", "-o", "json"]),
            operator.kubectl_json(["get", "nnc", "-A", "-o", "json"]),
            resume,
        )
    else:
        _validate_pre_scale_state(
            args, selected, state, pool, nodes, SURGE_POOL_COUNT,
            initial["initial_pool_configuration"], {name: row["uid"] for name, row in expected.items()},
        )
        _require_resume_hold(args, _real_node_map(nodes))


def _daemonset_tuple(pod: dict) -> Tuple[str, str, str]:
    owner = next(
        (
            row for row in (pod.get("metadata") or {}).get("ownerReferences", [])
            if isinstance(row, dict) and row.get("kind") == "DaemonSet"
            and row.get("controller") is True
        ),
        None,
    )
    require(owner is not None, "DaemonSet pod is missing its controller owner")
    metadata = pod.get("metadata") or {}
    return (
        str(metadata.get("namespace") or ""),
        str(owner.get("name") or ""),
        str(owner.get("uid") or ""),
    )


def _healthy_system_daemonsets_on_node(pods_payload: dict, node_name: str) -> Set[Tuple[str, str, str]]:
    result = set()
    for pod in _items(pods_payload, "Pod inventory"):
        metadata = pod.get("metadata") or {}
        spec = pod.get("spec") or {}
        if (
            str(metadata.get("namespace") or "") == "kube-system"
            and str(spec.get("nodeName") or "") == node_name
            and mocks._pod_ready(pod)
            and _readiness_condition_true(pod)
            and any(
                isinstance(owner, dict)
                and owner.get("kind") == "DaemonSet"
                and owner.get("controller") is True
                for owner in metadata.get("ownerReferences", [])
            )
        ):
            result.add(_daemonset_tuple(pod))
    return result


def _derive_applicable_daemonsets(
    pods_payload: dict,
    reference_nodes: Sequence[str],
) -> Set[Tuple[str, str, str]]:
    sets = [
        _healthy_system_daemonsets_on_node(pods_payload, node_name)
        for node_name in reference_nodes
    ]
    require(all(sets), "Existing healthy default workers do not all expose kube-system DaemonSet coverage")
    first = sets[0]
    require(
        all(current == first for current in sets[1:]),
        "Healthy reference workers disagree about applicable kube-system DaemonSets",
    )
    return first


def _wait_for_fresh_daemonsets(
    operator: ClusterOperator,
    args,
    daemonsets: Set[Tuple[str, str, str]],
    fresh_nodes: Sequence[str],
) -> None:
    while time.monotonic() < operator.work_deadline:
        pods_payload = operator.kubectl_json(
            ["get", "pods", "-A", "-o", "json"],
            timeout_seconds=args.request_timeout_seconds,
        )
        missing = {}
        for node_name in fresh_nodes:
            present = _healthy_system_daemonsets_on_node(pods_payload, node_name)
            absent = sorted(daemonsets - present)
            if absent:
                missing[node_name] = absent
        if not missing:
            return
        time.sleep(min(args.poll_seconds, operator.remaining_seconds(args.poll_seconds)))
    raise workers.ReconcileError("Fresh workers did not converge all applicable kube-system DaemonSets")


def _probe_intents(summary: dict) -> List[dict]:
    return summary.setdefault("probe_cleanup_pending", [])


def _add_probe_intent(summary: dict, name: str, node_name: str, token: str) -> dict:
    intent = {
        "name": name,
        "node_name": node_name,
        "token": token,
        "uid": "",
        "created": False,
    }
    _probe_intents(summary).append(intent)
    return intent


def _resolve_probe_uids(summary: dict, probes_payload: dict) -> List[str]:
    errors = []
    by_name = {
        str((pod.get("metadata") or {}).get("name") or ""): pod
        for pod in _items(probes_payload, "probe Pod inventory")
    }
    for intent in _probe_intents(summary):
        pod = by_name.get(intent["name"])
        if (
            pod is not None
            and str((pod.get("spec") or {}).get("nodeName") or "") == intent["node_name"]
            and str(((pod.get("metadata") or {}).get("labels") or {}).get(PROBE_LABEL_KEY) or "")
            == intent["token"]
        ):
            observed_uid = str((pod.get("metadata") or {}).get("uid") or "")
            if not observed_uid:
                errors.append(f"{intent['name']}: owned probe UID is missing")
                continue
            if intent.get("uid") and intent["uid"] != observed_uid:
                intent["unexpected_replacement_uid"] = observed_uid
                errors.append(
                    f"{intent['name']}: owned probe UID changed; refusing replacement adoption"
                )
                continue
            intent["uid"] = observed_uid
            intent["created"] = True
    return errors


def _create_probe_pod(
    operator: ClusterOperator,
    args,
    node_name: str,
    token: str,
    index: int,
    summary: dict,
) -> None:
    node_component = _safe_name_component(node_name[-12:])
    name = f"cni-maint-probe-{node_component}-{token[:8]}-{index:02d}"
    intent = _add_probe_intent(summary, name, node_name, token)
    _save(args.summary_file, summary)
    overrides = {
        "apiVersion": "v1",
        "spec": {
            "nodeName": node_name,
            "hostNetwork": False,
            "restartPolicy": "Never",
            "containers": [{
                "name": name,
                "image": args.probe_image,
                "args": list(PROBE_COMMAND),
                "resources": {"requests": {
                    "cpu": PROBE_CPU_REQUEST,
                    "memory": PROBE_MEMORY_REQUEST,
                }},
            }],
        },
    }
    output = operator.kubectl_json(
        [
            "-n",
            DEFAULT_NAMESPACE,
            "run",
            name,
            f"--image={args.probe_image}",
            "--restart=Never",
            f"--labels={PROBE_LABEL_KEY}={token}",
            "-o",
            "json",
            f"--overrides={json.dumps(overrides)}",
        ],
        timeout_seconds=args.request_timeout_seconds,
    )
    uid = str((output.get("metadata") or {}).get("uid") or "")
    if uid:
        intent["uid"] = uid
    intent["created"] = True
    _save(args.summary_file, summary)


def _cleanup_probe_pods(
    operator: ClusterOperator,
    args,
    cluster: mocks.Cluster,
    summary: dict,
) -> List[str]:
    errors = []
    if not _probe_intents(summary):
        return errors
    try:
        probes_payload = operator.kubectl_json(
            [
                "-n",
                DEFAULT_NAMESPACE,
                "get",
                "pods",
                "-l",
                f"{PROBE_LABEL_KEY} in ({','.join(sorted({entry['token'] for entry in _probe_intents(summary)}))})",
                "-o",
                "json",
            ],
            timeout_seconds=args.request_timeout_seconds,
            cleanup=True,
        )
        errors.extend(_resolve_probe_uids(summary, probes_payload))
    except EXPECTED_ERRORS as error:
        return [str(error)]
    remaining = []
    for intent in reversed(_probe_intents(summary)):
        if intent.get("unexpected_replacement_uid"):
            remaining.append(intent)
            continue
        uid = str(intent.get("uid") or "")
        if not uid:
            remaining.append(intent)
            errors.append(f"{intent['name']}: probe UID was never confirmed")
            continue
        try:
            mocks.delete_pod_with_uid_precondition(
                cluster,
                namespace=DEFAULT_NAMESPACE,
                name=intent["name"],
                uid=uid,
                timeout_seconds=operator.remaining_seconds(
                    args.request_timeout_seconds,
                    cleanup=True,
                ),
                attempts=1,
                retry_seconds=0,
            )
        except EXPECTED_ERRORS as error:
            remaining.append(intent)
            errors.append(str(error))
    summary["probe_cleanup_pending"] = list(reversed(remaining))
    return errors


def _prove_fresh_ip_growth(
    operator: ClusterOperator,
    args,
    cluster: mocks.Cluster,
    fresh_nodes: Sequence[dict],
    summary: dict,
) -> None:
    require(fresh_nodes, "The bounded surge produced no fresh workers")
    token = uuid.uuid4().hex
    before_nnc = _nnc_map(
        operator.kubectl_json(
            ["get", "nnc", "-A", "-o", "json"],
            timeout_seconds=args.request_timeout_seconds,
        )
    )
    for name, nc_id in summary.get("resume_fresh_network_container_ids", {}).items():
        require(
            name in before_nnc and before_nnc[name]["network_container_id"] == nc_id,
            f"{name}: resume fresh network container changed before probing",
        )
    summary["fresh_ip_growth"] = {}
    summary["probe_cleanup_pending"] = []
    _save(args.summary_file, summary)
    for node in fresh_nodes:
        node_name = str((node.get("metadata") or {}).get("name") or "")
        node_uid = str((node.get("metadata") or {}).get("uid") or "")
        row = before_nnc.get(node_name)
        require(row is not None, f"{node_name}: fresh NodeNetworkConfig is missing")
        require(
            row["node_uid"] == node_uid,
            f"{node_name}: fresh NodeNetworkConfig owner no longer matches the fresh worker",
        )
        summary["fresh_ip_growth"][node_name] = {
            "node_uid": node_uid,
            "network_container_id": row["network_container_id"],
            "initial_assigned": row["assigned_ip_count"],
            "initial_ip_addresses": row["ip_addresses"],
        }
        for index in range(args.probe_pod_count):
            _create_probe_pod(operator, args, node_name, token, index, summary)
    _save(args.summary_file, summary)
    try:
        pending = {str((node.get("metadata") or {}).get("name") or "") for node in fresh_nodes}
        while time.monotonic() < operator.work_deadline and pending:
            current_nnc = _nnc_map(
                operator.kubectl_json(
                    ["get", "nnc", "-A", "-o", "json"],
                    timeout_seconds=args.request_timeout_seconds,
                )
            )
            probes = operator.kubectl_json(
                [
                    "-n",
                    DEFAULT_NAMESPACE,
                    "get",
                    "pods",
                    "-l",
                    f"{PROBE_LABEL_KEY}={token}",
                    "-o",
                    "json",
                ],
                timeout_seconds=args.request_timeout_seconds,
            )
            resolution_errors = _resolve_probe_uids(summary, probes)
            require(not resolution_errors, "; ".join(resolution_errors))
            by_node: Dict[str, List[dict]] = {}
            for pod in _items(probes, "probe Pod inventory"):
                node_name = str((pod.get("spec") or {}).get("nodeName") or "")
                if node_name:
                    by_node.setdefault(node_name, []).append(pod)
            satisfied = []
            for node_name in sorted(pending):
                planned = [
                    row for row in _probe_intents(summary)
                    if row["node_name"] == node_name
                ]
                require(len(planned) == args.probe_pod_count, f"{node_name}: probe intent count drifted")
                row = current_nnc.get(node_name)
                require(row is not None, f"{node_name}: fresh NodeNetworkConfig disappeared")
                initial = summary["fresh_ip_growth"][node_name]
                require(
                    row["node_uid"] == initial["node_uid"]
                    and row["network_container_id"] == initial["network_container_id"],
                    f"{node_name}: fresh NodeNetworkConfig owner or ID changed during probing",
                )
                current_pods = by_node.get(node_name, [])
                if len(current_pods) != args.probe_pod_count:
                    continue
                if not all(
                    str((pod.get("metadata") or {}).get("uid") or "") in {
                        row["uid"] for row in planned if row["uid"]
                    }
                    and mocks._pod_ready(pod)
                    and _readiness_condition_true(pod)
                    and str((pod.get("status") or {}).get("podIP") or "")
                    for pod in current_pods
                ):
                    continue
                current_ips = set(row["ip_addresses"])
                probe_ips = {
                    str((pod.get("status") or {}).get("podIP") or "")
                    for pod in current_pods
                }
                if (
                    row["assigned_ip_count"] > initial["initial_assigned"]
                    and probe_ips <= current_ips
                    and any(ip not in set(initial["initial_ip_addresses"]) for ip in probe_ips)
                ):
                    summary["fresh_ip_growth"][node_name].update(
                        {
                            "after_assigned": row["assigned_ip_count"],
                            "ready_probe_ips": sorted(probe_ips),
                        }
                    )
                    satisfied.append(node_name)
            for node_name in satisfied:
                pending.remove(node_name)
            _save(args.summary_file, summary)
            if pending:
                time.sleep(min(args.poll_seconds, operator.remaining_seconds(args.poll_seconds)))
        require(
            not pending,
            "Fresh workers did not all prove Azure CNI IP-batch growth before mock-agent evacuation",
        )
    finally:
        cleanup_errors = summary.setdefault("cleanup_errors", [])
        cleanup_errors.extend(_cleanup_probe_pods(operator, args, cluster, summary))
        _save(args.summary_file, summary)
    if summary.get("probe_cleanup_pending") or summary.get("cleanup_errors"):
        detail = "; ".join(summary.get("cleanup_errors") or [])
        if summary.get("probe_cleanup_pending"):
            pending = ",".join(
                sorted(entry["name"] for entry in summary["probe_cleanup_pending"])
            )
            detail = f"{detail}; pending={pending}".strip("; ")
        raise workers.ReconcileError(f"probe cleanup failed: {detail}")


def _add_source_hold(operator: ClusterOperator, args, summary: dict) -> None:
    node = operator.kubectl_json(
        ["get", "node", args.node_name, "-o", "json"],
        timeout_seconds=args.request_timeout_seconds,
    )
    taints = list(_taints(node))
    hold_taint = {"key": HOLD_ANNOTATION, "value": HOLD_REASON, "effect": "NoSchedule"}
    if hold_taint not in taints:
        taints.append(hold_taint)
    annotations = dict(_annotations(node))
    annotations[HOLD_ANNOTATION] = (
        f"bounded-worker-retirement role={args.role} node={args.node_name} uid={args.node_uid}"
    )
    operator.kubectl(
        [
            "patch",
            "node",
            args.node_name,
            "--type=json",
            "-p",
            json.dumps(
                [
                    {"op": "test", "path": "/metadata/uid", "value": args.node_uid},
                    {
                        "op": "test",
                        "path": "/metadata/resourceVersion",
                        "value": str((node.get("metadata") or {}).get("resourceVersion") or ""),
                    },
                    {"op": "add", "path": "/spec/unschedulable", "value": True},
                    {"op": "add", "path": "/spec/taints", "value": taints},
                    {"op": "add", "path": "/metadata/annotations", "value": annotations},
                ]
            ),
        ],
        timeout_seconds=args.request_timeout_seconds,
    )
    summary["source_quarantined"] = True
    _save(args.summary_file, summary)


def _temporary_exclusions(
    operator: ClusterOperator,
    args,
    summary: dict,
    node_names: Sequence[str],
) -> str:
    token = uuid.uuid4().hex
    entries = summary.setdefault("temporary_exclusions", [])
    for node_name in node_names:
        node = operator.kubectl_json(
            ["get", "node", node_name, "-o", "json"],
            timeout_seconds=args.request_timeout_seconds,
        )
        node_uid = str((node.get("metadata") or {}).get("uid") or "")
        entry = {
            "name": node_name,
            "uid": node_uid,
            "token": token,
            "status": "intent-recorded",
        }
        entries.append(entry)
        _save(args.summary_file, summary)
        taints = _taints(node)
        require(
            not any(
                isinstance(taint, dict)
                and str(taint.get("key") or "") == EXCLUSION_KEY
                for taint in taints
            ),
            f"{node_name}: another maintenance exclusion is already present",
        )
        entry["status"] = "patch-submitted"
        _save(args.summary_file, summary)
        operator.kubectl(
            [
                "patch",
                "node",
                node_name,
                "--type=json",
                "-p",
                json.dumps(
                    [
                        {"op": "test", "path": "/metadata/uid", "value": node_uid},
                        {
                            "op": "test",
                            "path": "/metadata/resourceVersion",
                            "value": str((node.get("metadata") or {}).get("resourceVersion") or ""),
                        },
                        {
                            "op": "add",
                            "path": "/spec/taints",
                            "value": list(taints) + [
                                {"key": EXCLUSION_KEY, "value": token, "effect": "NoSchedule"}
                            ],
                        },
                    ]
                ),
            ],
            timeout_seconds=args.request_timeout_seconds,
        )
        entry["status"] = "applied"
        _save(args.summary_file, summary)
    return token


def _remove_temporary_exclusions(
    operator: ClusterOperator,
    args,
    summary: dict,
    *,
    cleanup_errors: List[str],
) -> None:
    remaining = []
    for entry in reversed(summary.get("temporary_exclusions", [])):
        try:
            node = operator.kubectl_json(
                ["get", "node", entry["name"], "-o", "json"],
                timeout_seconds=args.request_timeout_seconds,
                cleanup=True,
            )
            removed = False
            for index, taint in enumerate(_taints(node)):
                if taint == {"key": EXCLUSION_KEY, "value": entry["token"], "effect": "NoSchedule"}:
                    operator.kubectl(
                        [
                            "patch",
                            "node",
                            entry["name"],
                            "--type=json",
                            "-p",
                            json.dumps(
                                [
                                    {
                                        "op": "test",
                                        "path": "/metadata/uid",
                                        "value": str((node.get("metadata") or {}).get("uid") or ""),
                                    },
                                    {"op": "test", "path": f"/spec/taints/{index}", "value": taint},
                                    {"op": "remove", "path": f"/spec/taints/{index}"},
                                ]
                            ),
                        ],
                        timeout_seconds=args.request_timeout_seconds,
                        cleanup=True,
                    )
                    removed = True
                    break
            if not removed:
                cleanup_errors.append(
                    f"{entry['name']}: maintenance exclusion could not be confirmed for cleanup"
                )
                remaining.append(entry)
        except EXPECTED_ERRORS as error:
            cleanup_errors.append(str(error))
            remaining.append(entry)
    summary["temporary_exclusions"] = list(reversed(remaining))


def _verify_only_source_hold_remains(
    operator: ClusterOperator,
    args,
    selected_cluster: dict,
) -> None:
    nodes_payload = operator.kubectl_json(
        ["get", "nodes", "-o", "json"],
        timeout_seconds=args.request_timeout_seconds,
    )
    real_nodes = _real_pool_nodes(
        nodes_payload,
        pool_name=DEFAULT_POOL_NAME,
        subscription=args.expected_subscription,
        node_resource_group=str(selected_cluster["nodeResourceGroup"]),
    )
    source = real_nodes.get(args.node_name)
    require(source is not None, "The explicit source worker disappeared before retirement")
    for name, node in real_nodes.items():
        taints = [
            str(taint.get("key") or "")
            for taint in _taints(node)
            if isinstance(taint, dict)
            and str(taint.get("key") or "").startswith("mock-clustermesh/")
        ]
        if name == args.node_name:
            require(
                taints == [HOLD_ANNOTATION],
                "Only the explicit source hold may remain before retirement",
            )
        else:
            require(
                not taints,
                f"{name}: a temporary maintenance taint still exists before retirement",
            )


def _current_destinations_only(
    operator: ClusterOperator,
    args,
    selected_cluster: dict,
    fresh_nodes: Sequence[str],
) -> dict:
    nodes_payload = operator.kubectl_json(
        ["get", "nodes", "-o", "json"],
        timeout_seconds=args.request_timeout_seconds,
    )
    pods_payload = operator.kubectl_json(
        ["get", "pods", "-A", "-o", "json"],
        timeout_seconds=args.request_timeout_seconds,
    )
    metrics_payload = operator.kubectl_json(
        ["get", "--raw", "/apis/metrics.k8s.io/v1beta1/nodes"],
        timeout_seconds=args.request_timeout_seconds,
    )
    pod_metrics_payload = operator.kubectl_json(
        ["get", "--raw", f"/apis/metrics.k8s.io/v1beta1/namespaces/{DEFAULT_NAMESPACE}/pods"],
        timeout_seconds=args.request_timeout_seconds,
    )
    destinations = _real_pool_nodes(
        nodes_payload,
        pool_name=DEFAULT_POOL_NAME,
        subscription=args.expected_subscription,
        node_resource_group=str(selected_cluster["nodeResourceGroup"]),
    )
    require(
        set(fresh_nodes) <= set(destinations),
        "An IP-proven fresh destination worker disappeared",
    )
    metrics = {
        str((row.get("metadata") or {}).get("name") or ""): row
        for row in _items(metrics_payload, "node metrics")
    }
    return {
        "nodes": nodes_payload,
        "pods": pods_payload,
        "metrics": metrics,
        "pod_metrics": pod_metrics_payload,
        "destinations": {
            name: destinations[name] for name in fresh_nodes
        },
    }


def _pod_memory_usage_bytes(pod_metrics: dict, namespace: str) -> Dict[str, dict]:
    usage = {}
    for row in _items(pod_metrics, "pod metrics"):
        metadata = row.get("metadata") or {}
        if str(metadata.get("namespace") or "") != namespace:
            continue
        containers = row.get("containers") or []
        require(isinstance(containers, list) and containers, "Pod metrics containers are malformed")
        total = 0
        for container in containers:
            require(isinstance(container, dict), "Pod metrics container entry is malformed")
            memory = ((container.get("usage") or {}).get("memory"))
            value = int(mocks._quantity(memory, "pod memory usage"))
            require(value > 0, "Pod metrics memory usage must be positive")
            total += value
        timestamp = str(row.get("timestamp") or "")
        require(timestamp, "Pod metrics timestamp is missing")
        observed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        age_seconds = (datetime.now(timezone.utc) - observed).total_seconds()
        require(0 <= age_seconds <= 180, "Pod metrics are stale")
        usage[str(metadata.get("name") or "")] = {
            "memory_bytes": total,
            "timestamp": timestamp,
        }
    return usage


def _fresh_identity_snapshot(
    snapshot: dict,
    fresh_nodes: Sequence[str],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    current_nodes = _real_node_map(snapshot["nodes"])
    current_nnc = _nnc_map(
        snapshot["nnc"]
        if "nnc" in snapshot
        else {"items": []}
    )
    node_uids = {}
    nc_ids = {}
    for name in fresh_nodes:
        node = current_nodes.get(name)
        require(node is not None, f"{name}: fresh destination worker disappeared")
        row = current_nnc.get(name)
        require(row is not None, f"{name}: fresh destination NodeNetworkConfig disappeared")
        node_uids[name] = str((node.get("metadata") or {}).get("uid") or "")
        nc_ids[name] = row["network_container_id"]
    return node_uids, nc_ids


def _headroom_ok(
    node: dict,
    metric: dict,
    *,
    threshold_percent: int,
    effective_reserved_memory_bytes: int,
    next_memory_bytes: int,
) -> bool:
    timestamp = str((metric.get("timestamp") or "")).replace("Z", "+00:00")
    require(timestamp, f"{(node.get('metadata') or {}).get('name')}: node metrics timestamp is missing")
    observed = datetime.fromisoformat(timestamp)
    age_seconds = (datetime.now(timezone.utc) - observed).total_seconds()
    require(0 <= age_seconds <= 180, f"{(node.get('metadata') or {}).get('name')}: node metrics are stale")
    allocatable = int(
        mocks._quantity(
            ((node.get("status") or {}).get("allocatable") or {}).get("memory"),
            "allocatable memory",
        )
    )
    used = int(mocks._quantity((metric.get("usage") or {}).get("memory"), "memory usage"))
    require(allocatable > 0, f"{(node.get('metadata') or {}).get('name')}: allocatable memory is invalid")
    require(used > 0, f"{(node.get('metadata') or {}).get('name')}: node memory usage is invalid")
    limit = int(allocatable * threshold_percent / 100)
    return used + effective_reserved_memory_bytes + next_memory_bytes < limit


def _missing_metric(name: str) -> dict:
    raise workers.ReconcileError(f"{name}: node metrics are missing")


def _planned_source_agents(
    agents: Dict[str, dict],
    source_name: str,
) -> List[dict]:
    return [
        {
            "name": name,
            "uid": str((pod.get("metadata") or {}).get("uid") or ""),
            "node_name": str((pod.get("spec") or {}).get("nodeName") or ""),
            "memory_bytes": mocks._resource_requests(pod)[1],
        }
        for name, pod in sorted(agents.items())
        if str((pod.get("spec") or {}).get("nodeName") or "") == source_name
    ]


def _observe_agents_with_gap(
    payload: dict,
    controller_uid: str,
    protected_uids: Dict[str, str],
    *,
    inflight_name: Optional[str],
    inflight_uid: Optional[str],
) -> Dict[str, dict]:
    agents = _agent_map(payload)
    missing = EXPECTED_AGENT_NAMES - set(agents)
    extra = set(agents) - EXPECTED_AGENT_NAMES
    require(not extra, "Mock-agent inventory contains unexpected names")
    if inflight_name is None:
        require(not missing, "The exact 100 mock-agent names are required")
    else:
        require(missing in (set(), {inflight_name}), "Only the in-flight mock agent may be absent")
    for name, pod in agents.items():
        uid = str((pod.get("metadata") or {}).get("uid") or "")
        require(uid, f"{name}: mock-agent UID is missing")
        require(
            mocks._pod_owned_by_controller_uid(pod, controller_uid),
            f"{name}: mock-agent ownership changed",
        )
        if inflight_name is not None and name == inflight_name:
            if uid == inflight_uid and (pod.get("metadata") or {}).get("deletionTimestamp"):
                continue
        require(
            not (pod.get("metadata") or {}).get("deletionTimestamp"),
            f"{name}: non in-flight mock-agent is terminating",
        )
    for name, expected_uid in protected_uids.items():
        pod = agents.get(name)
        require(pod is not None, f"{name}: a protected Ready mock agent disappeared")
        require(
            str((pod.get("metadata") or {}).get("uid") or "") == expected_uid
            and mocks._pod_ready(pod)
            and _readiness_condition_true(pod),
            "A protected Ready mock agent changed or lost readiness",
        )
    return agents


def _wait_for_all_mock_ready(
    operator: ClusterOperator,
    args,
    controller_uid: str,
    protected_uids: Dict[str, str],
) -> Dict[str, dict]:
    while time.monotonic() < operator.work_deadline:
        payload = operator.kubectl_json(
            ["-n", DEFAULT_NAMESPACE, "get", "pods", "-l", mocks.AGENT_SELECTOR, "-o", "json"],
            timeout_seconds=args.request_timeout_seconds,
        )
        agents = _observe_agents_with_gap(
            payload,
            controller_uid,
            protected_uids,
            inflight_name=None,
            inflight_uid=None,
        )
        if len(agents) == 100 and all(
            mocks._pod_ready(pod) and _readiness_condition_true(pod)
            for pod in agents.values()
        ):
            return agents
        time.sleep(min(args.poll_seconds, operator.remaining_seconds(args.poll_seconds)))
    raise workers.ReconcileError("The exact 100 mock agents never returned to Ready")


def _wait_for_kwok_ready(
    operator: ClusterOperator,
    args,
    expected_uids: Dict[str, str],
    *,
    reserve_after_seconds: int,
) -> None:
    wait_budget = _phase_budget(
        operator,
        maximum_seconds=KWOK_READY_WAIT_SECONDS,
        reserve_after_seconds=reserve_after_seconds,
        minimum_seconds=args.poll_seconds,
        description="KWOK readiness recovery",
    )
    deadline = min(operator.work_deadline, time.monotonic() + wait_budget)
    while time.monotonic() < deadline:
        nodes_payload = operator.kubectl_json(
            ["get", "nodes", "-o", "json"],
            timeout_seconds=args.request_timeout_seconds,
        )
        kwok_nodes = _require_exact_kwok_nodes(nodes_payload)
        all_ready = True
        for name, uid in expected_uids.items():
            node = kwok_nodes.get(name)
            require(node is not None, f"{name}: KWOK Node is missing")
            require(
                str((node.get("metadata") or {}).get("uid") or "") == uid,
                f"{name}: KWOK Node identity changed",
            )
            require(
                not (node.get("metadata") or {}).get("deletionTimestamp"),
                f"{name}: KWOK Node entered deletion during readiness recovery",
            )
            if not workers.node_is_ready(node):
                all_ready = False
        if all_ready:
            return
        time.sleep(min(args.poll_seconds, operator.remaining_seconds(args.poll_seconds)))
    raise workers.ReconcileError("The exact 100 KWOK Nodes never returned to Ready")


def _validate_move_preconditions(
    operator: ClusterOperator,
    args,
    selected_cluster: dict,
    fresh_nodes: Sequence[str],
    remaining_source_agents: Sequence[dict],
    memory_state: Dict[str, dict],
    pod_template: dict,
    expected_fresh_node_uids: Dict[str, str],
    expected_fresh_nc_ids: Dict[str, str],
) -> Tuple[dict, dict]:
    snapshot = _current_destinations_only(operator, args, selected_cluster, fresh_nodes)
    destination_names = list(fresh_nodes)
    snapshot["nnc"] = operator.kubectl_json(
        ["get", "nnc", "-A", "-o", "json"],
        timeout_seconds=args.request_timeout_seconds,
    )
    current_fresh_uids, current_fresh_nc_ids = _fresh_identity_snapshot(snapshot, fresh_nodes)
    require(
        current_fresh_uids == expected_fresh_node_uids,
        "A fresh destination worker UID changed after IP-growth proof",
    )
    require(
        current_fresh_nc_ids == expected_fresh_nc_ids,
        "A fresh destination network container changed after IP-growth proof",
    )
    affected = []
    for row in remaining_source_agents:
        pod = next(
            (
                pod for pod in _items(snapshot["pods"], "Pod inventory")
                if (pod.get("metadata") or {}).get("name") == row["name"]
                and (pod.get("metadata") or {}).get("namespace") == DEFAULT_NAMESPACE
            ),
            None,
        )
        if pod is None:
            continue
        affected.append(pod)
    try:
        capacity = mocks.assess_recovery_capacity(
            nodes_payload=snapshot["nodes"],
            pods_payload=snapshot["pods"],
            affected=affected,
            saturated_nodes=[
                str((node.get("metadata") or {}).get("name") or "")
                for node in _real_node_map(snapshot["nodes"]).values()
                if str((node.get("metadata") or {}).get("name") or "") not in destination_names
            ],
            pod_template=pod_template,
            config_settings=mocks.CapacityRepairConfig(),
        )
    except mocks.RecoveryError as error:
        raise workers.ReconcileError(str(error)) from error
    require(capacity["sufficient"] is True, "Fresh workers do not have enough projected scheduling capacity")
    require(set(capacity["alternate_nodes"]) == set(destination_names), "Only IP-proven fresh workers may act as destinations")
    metrics = snapshot["metrics"]
    pod_metrics = _pod_memory_usage_bytes(snapshot["pod_metrics"], DEFAULT_NAMESPACE)
    healthy_samples = [
        details["memory_bytes"]
        for name, details in pod_metrics.items()
        if name in EXPECTED_AGENT_NAMES
    ]
    require(healthy_samples, "Healthy mock-agent pod metrics are missing")
    conservative_sample = max(healthy_samples)
    unsafe_nodes = []
    for row in remaining_source_agents:
        pod_metric = pod_metrics.get(row["name"])
        if pod_metric is not None:
            pod_timestamp = datetime.fromisoformat(
                str(pod_metric["timestamp"]).replace("Z", "+00:00")
            )
            age_seconds = (datetime.now(timezone.utc) - pod_timestamp).total_seconds()
            require(0 <= age_seconds <= 180, f"{row['name']}: pod metrics are stale")
        next_memory = max(
            int(row["memory_bytes"]),
            conservative_sample,
            int((pod_metric or {}).get("memory_bytes") or 0),
        )
        for name in destination_names:
            metric = metrics[name] if name in metrics else _missing_metric(name)
            if name not in memory_state:
                memory_state[name] = {
                    "baseline_used_bytes": int(
                        mocks._quantity((metric.get("usage") or {}).get("memory"), "memory usage")
                    ),
                    "reserved_memory_bytes": 0,
                }
            observed_used = int(
                mocks._quantity((metric.get("usage") or {}).get("memory"), "memory usage")
            )
            memory_state[name]["baseline_used_bytes"] = max(
                memory_state[name]["baseline_used_bytes"],
                observed_used - memory_state[name]["reserved_memory_bytes"],
            )
            effective_reserved = max(
                memory_state[name]["baseline_used_bytes"]
                + memory_state[name]["reserved_memory_bytes"]
                - observed_used,
                0,
            )
            if not _headroom_ok(
                snapshot["destinations"][name],
                metric,
                threshold_percent=args.memory_threshold_percent,
                effective_reserved_memory_bytes=effective_reserved,
                next_memory_bytes=next_memory,
            ):
                unsafe_nodes.append(name)
        if unsafe_nodes:
            break
    require(
        not unsafe_nodes,
        "each eligible fresh destination must have safe projected headroom "
        f"before the next delete: {sorted(set(unsafe_nodes))}",
    )
    for row in remaining_source_agents:
        next_memory = max(
            int(row["memory_bytes"]),
            conservative_sample,
            int((pod_metrics.get(row["name"]) or {}).get("memory_bytes") or 0),
        )
        if not all(
            _headroom_ok(
                snapshot["destinations"][name],
                metrics[name] if name in metrics else _missing_metric(name),
                threshold_percent=args.memory_threshold_percent,
                effective_reserved_memory_bytes=max(
                    memory_state[name]["baseline_used_bytes"]
                    + memory_state[name]["reserved_memory_bytes"]
                    - int(mocks._quantity((metrics[name].get("usage") or {}).get("memory"), "memory usage")),
                    0,
                ),
                next_memory_bytes=next_memory,
            )
            for name in destination_names
        ):
            raise workers.ReconcileError(
                f"{row['name']}: each eligible fresh destination must have safe projected headroom before the next delete"
            )
        row["estimated_memory_bytes"] = next_memory
    return snapshot, capacity


def _move_agents_one_at_a_time(
    operator: ClusterOperator,
    args,
    selected_cluster: dict,
    cluster: mocks.Cluster,
    summary: dict,
    controller_uid: str,
    protected_uids: Dict[str, str],
    planned_agents: Sequence[dict],
    fresh_nodes: Sequence[str],
    *,
    phase: str,
) -> List[dict]:
    moves = []
    memory_state: Dict[str, dict] = summary.setdefault("destination_memory_state", {})
    expected_fresh_node_uids = {
        name: str(summary["fresh_ip_growth"][name]["node_uid"])
        for name in fresh_nodes
    }
    expected_fresh_nc_ids = {
        name: str(summary["fresh_ip_growth"][name]["network_container_id"])
        for name in fresh_nodes
    }
    for index, planned in enumerate(planned_agents, start=1):
        if phase == "healthy":
            _require_retirement_reserve(operator)
        remaining = planned_agents[index - 1:]
        _snapshot, capacity = _validate_move_preconditions(
            operator,
            args,
            selected_cluster,
            fresh_nodes,
            remaining,
            memory_state,
            summary["pod_template"],
            expected_fresh_node_uids,
            expected_fresh_nc_ids,
        )
        summary[f"{phase}_capacity_before_{planned['name']}"] = capacity
        _save(args.summary_file, summary)
        payload = operator.kubectl_json(
            ["-n", DEFAULT_NAMESPACE, "get", "pods", "-l", mocks.AGENT_SELECTOR, "-o", "json"],
            timeout_seconds=args.request_timeout_seconds,
        )
        agents = _observe_agents_with_gap(
            payload,
            controller_uid,
            protected_uids,
            inflight_name=None,
            inflight_uid=None,
        )
        live = agents.get(planned["name"])
        require(live is not None, f"{planned['name']}: planned source Pod disappeared before deletion")
        live_uid = str((live.get("metadata") or {}).get("uid") or "")
        live_node = str((live.get("spec") or {}).get("nodeName") or "")
        require(
            live_uid == planned["uid"],
            f"{planned['name']}: planned source Pod UID changed before deletion",
        )
        require(
            live_node == planned["node_name"],
            f"{planned['name']}: planned source Pod moved off the explicit source",
        )
        if phase == "pending" and mocks._pod_ready(live):
            protected_uids[planned["name"]] = live_uid
            continue
        if phase == "healthy":
            require(
                mocks._pod_ready(live) and _readiness_condition_true(live),
                f"{planned['name']}: healthy source Pod is no longer fully Ready",
            )
            protected_uids.pop(planned["name"], None)
        per_pod_deadline = min(
            operator.work_deadline,
            time.monotonic() + args.per_pod_ready_seconds,
        )
        mocks.delete_pod_with_uid_precondition(
            cluster,
            namespace=DEFAULT_NAMESPACE,
            name=planned["name"],
            uid=live_uid,
            timeout_seconds=args.request_timeout_seconds,
            attempts=1,
            retry_seconds=0,
        )
        while time.monotonic() < per_pod_deadline:
            payload = operator.kubectl_json(
                ["-n", DEFAULT_NAMESPACE, "get", "pods", "-l", mocks.AGENT_SELECTOR, "-o", "json"],
                timeout_seconds=args.request_timeout_seconds,
            )
            agents = _observe_agents_with_gap(
                payload,
                controller_uid,
                protected_uids,
                inflight_name=planned["name"],
                inflight_uid=live_uid,
            )
            replacement = agents.get(planned["name"])
            if replacement is None:
                time.sleep(min(args.poll_seconds, operator.remaining_seconds(args.poll_seconds)))
                continue
            new_uid = str((replacement.get("metadata") or {}).get("uid") or "")
            new_node = str((replacement.get("spec") or {}).get("nodeName") or "")
            if (
                new_uid not in ("", live_uid)
                and new_node in fresh_nodes
                and mocks._pod_ready(replacement)
                and _readiness_condition_true(replacement)
            ):
                protected_uids[planned["name"]] = new_uid
                memory_state.setdefault(
                    new_node,
                    {"baseline_used_bytes": 0, "reserved_memory_bytes": 0},
                )
                memory_state[new_node]["reserved_memory_bytes"] += int(
                    planned.get("estimated_memory_bytes") or planned["memory_bytes"]
                )
                move = {
                    "name": planned["name"],
                    "old_uid": live_uid,
                    "new_uid": new_uid,
                    "node": new_node,
                }
                moves.append(move)
                summary.setdefault(f"{phase}_moves", []).append(move)
                _save(args.summary_file, summary)
                break
            time.sleep(min(args.poll_seconds, operator.remaining_seconds(args.poll_seconds)))
        else:
            raise workers.ReconcileError(
                f"{planned['name']}: replacement did not become Ready within the bounded per-Pod limit"
            )
    return moves


def _replicaset_owner_map(replicasets_payload: dict, deployments_payload: dict) -> Dict[Tuple[str, str, str], Tuple[str, str]]:
    deployments = {
        (
            str((row.get("metadata") or {}).get("namespace") or ""),
            str((row.get("metadata") or {}).get("name") or ""),
            str((row.get("metadata") or {}).get("uid") or ""),
        )
        for row in _items(deployments_payload, "Deployment inventory")
    }
    mapping = {}
    for row in _items(replicasets_payload, "ReplicaSet inventory"):
        metadata = row.get("metadata") or {}
        identity = (
            str(metadata.get("namespace") or ""),
            str(metadata.get("name") or ""),
            str(metadata.get("uid") or ""),
        )
        owner = next(
            (
                ref for ref in metadata.get("ownerReferences", [])
                if isinstance(ref, dict) and ref.get("controller") is True
            ),
            None,
        )
        if (
            owner is not None
            and owner.get("kind") == "Deployment"
            and (
                identity[0],
                str(owner.get("name") or ""),
                str(owner.get("uid") or ""),
            ) in deployments
        ):
            mapping[identity] = (identity[0], str(owner.get("name") or ""))
    return mapping


def _statefulset_owner_set(statefulsets_payload: dict) -> Set[Tuple[str, str, str]]:
    return {
        (
            str((row.get("metadata") or {}).get("namespace") or ""),
            str((row.get("metadata") or {}).get("name") or ""),
            str((row.get("metadata") or {}).get("uid") or ""),
        )
        for row in _items(statefulsets_payload, "StatefulSet inventory")
    }


def _validate_pre_drain_source_pods(
    source_pods: Sequence[dict],
    daemonsets_payload: dict,
    replicasets_payload: dict,
    deployments_payload: dict,
    statefulsets_payload: dict,
) -> dict:
    daemonsets = {
        (
            str((row.get("metadata") or {}).get("namespace") or ""),
            str((row.get("metadata") or {}).get("name") or ""),
            str((row.get("metadata") or {}).get("uid") or ""),
        )
        for row in _items(daemonsets_payload, "DaemonSet inventory")
    }
    replicasets = _replicaset_owner_map(replicasets_payload, deployments_payload)
    statefulsets = _statefulset_owner_set(statefulsets_payload)
    daemonset_pods = []
    drain_safe = []
    for pod in source_pods:
        metadata = pod.get("metadata") or {}
        spec = pod.get("spec") or {}
        owners = metadata.get("ownerReferences") or []
        namespace = str(metadata.get("namespace") or "")
        name = str(metadata.get("name") or "")
        require(namespace and name and isinstance(owners, list), "Source Pod inventory is malformed")
        require(
            not any("persistentVolumeClaim" in volume for volume in spec.get("volumes", [])),
            f"{namespace}/{name}: PVC-backed source Pod is unsupported",
        )
        controller = next(
            (
                owner for owner in owners
                if isinstance(owner, dict) and owner.get("controller") is True
            ),
            None,
        )
        require(controller is not None, f"{namespace}/{name}: source Pod is unmanaged")
        identity = (
            namespace,
            str(controller.get("name") or ""),
            str(controller.get("uid") or ""),
        )
        kind = str(controller.get("kind") or "")
        if kind == "DaemonSet":
            require(identity in daemonsets, f"{namespace}/{name}: DaemonSet owner is not exact")
            daemonset_pods.append(f"{namespace}/{name}")
            continue
        if kind == "ReplicaSet":
            deployment = replicasets.get(identity)
            require(deployment is not None, f"{namespace}/{name}: ReplicaSet has no exact Deployment owner")
            require(
                deployment in KNOWN_ALLOWED_DEPLOYMENTS,
                f"{namespace}/{name}: Deployment {deployment[1]} is not an allowed system/framework controller",
            )
            drain_safe.append(
                {
                    "pod": f"{namespace}/{name}",
                    "controller_kind": "Deployment",
                    "controller_namespace": deployment[0],
                    "controller_name": deployment[1],
                    "host_network": bool(spec.get("hostNetwork", False)),
                }
            )
            continue
        if kind == "StatefulSet":
            require(identity in statefulsets, f"{namespace}/{name}: StatefulSet owner is not exact")
            controller_name = (namespace, identity[1])
            require(
                controller_name in KNOWN_ALLOWED_STATEFULSETS or controller_name in KNOWN_ALLOWED_DEPLOYMENTS,
                f"{namespace}/{name}: StatefulSet {identity[1]} is not an allowed controller",
            )
            drain_safe.append(
                {
                    "pod": f"{namespace}/{name}",
                    "controller_kind": kind,
                    "controller_namespace": namespace,
                    "controller_name": identity[1],
                    "host_network": bool(spec.get("hostNetwork", False)),
                }
            )
            continue
        raise workers.ReconcileError(f"{namespace}/{name}: controller {kind} is unsupported")
    return {
        "remaining_daemonsets": sorted(daemonset_pods),
        "drain_safe_controllers": sorted(drain_safe, key=lambda row: row["pod"]),
    }


def _validate_final_workload_identity(
    nodes_payload: dict,
    pods_payload: dict,
    controller_uid: str,
    expected_kwok_uids: Dict[str, str],
    preserved_agent_uids: Dict[str, str],
) -> None:
    _require_all_kwok_ready(nodes_payload, expected_kwok_uids)
    agents = _require_exact_agents(pods_payload, controller_uid)
    require(
        all(
            mocks._pod_ready(pod) and _readiness_condition_true(pod)
            for pod in agents.values()
        ),
        "All 100 mock agents must be Ready during final qualification",
    )
    for name, uid in preserved_agent_uids.items():
        pod = agents.get(name)
        require(
            pod is not None
            and str((pod.get("metadata") or {}).get("uid") or "") == uid,
            f"{name}: preserved mock-agent UID changed unexpectedly",
        )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resource-group", required=True)
    parser.add_argument("--confirm-resource-group", required=True)
    parser.add_argument("--expected-subscription", required=True)
    parser.add_argument("--expected-region", required=True)
    parser.add_argument("--expected-tfvars-sha", required=True)
    parser.add_argument("--role", required=True)
    parser.add_argument("--node-name", required=True)
    parser.add_argument("--node-uid", required=True)
    parser.add_argument("--source-provider-id", required=True)
    parser.add_argument("--source-network-container-id", required=True)
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument("--context", default="")
    parser.add_argument("--summary-file", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=2700)
    parser.add_argument("--request-timeout-seconds", type=int, default=45)
    parser.add_argument("--poll-seconds", type=int, default=15)
    parser.add_argument("--per-pod-ready-seconds", type=int, default=240)
    parser.add_argument("--probe-image", default=DEFAULT_PROBE_IMAGE)
    parser.add_argument("--probe-pod-count", type=int, default=20)
    parser.add_argument("--memory-threshold-percent", type=int, default=85)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--resume-build-id", type=int, default=0)
    parser.add_argument("--resume-summary", default="")
    parser.add_argument("--resume-manifest", default="")
    parser.add_argument("--recover-empty-fresh-node", default="")
    parser.add_argument("--recover-empty-fresh-uid", default="")
    parser.add_argument("--replace-empty-fresh", action="store_true")
    args = parser.parse_args(argv)
    try:
        if _resume_options(args):
            _require_resume_paths(args)
    except workers.ReconcileError as error:
        parser.error(str(error))
    if args.confirm_resource_group != args.resource_group:
        parser.error("--confirm-resource-group must exactly match --resource-group")
    if ROLE_RE.fullmatch(args.role) is None:
        parser.error("--role must match mesh-N")
    if SHA256_RE.fullmatch(args.expected_tfvars_sha or "") is None:
        parser.error("--expected-tfvars-sha must be a lowercase SHA-256 hex digest")
    if PROVIDER_ID_RE.fullmatch(args.source_provider_id or "") is None:
        parser.error("--source-provider-id must be a full AKS VMSS providerID")
    _parse_uuid(args.node_uid, "--node-uid")
    _parse_uuid(args.source_network_container_id, "--source-network-container-id")
    if args.recover_empty_fresh_uid:
        _parse_uuid(args.recover_empty_fresh_uid, "--recover-empty-fresh-uid")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")
    if args.request_timeout_seconds <= 0:
        parser.error("--request-timeout-seconds must be positive")
    if args.poll_seconds <= 0:
        parser.error("--poll-seconds must be positive")
    if args.per_pod_ready_seconds <= 0:
        parser.error("--per-pod-ready-seconds must be positive")
    if args.probe_pod_count <= 0 or args.probe_pod_count > 20:
        parser.error("--probe-pod-count must be between 1 and 20")
    if args.memory_threshold_percent < 80 or args.memory_threshold_percent > 85:
        parser.error("--memory-threshold-percent must be between 80 and 85")
    return args


def execute_maintenance(
    args,
    summary: dict,
    runner: Runner = workers.run_command,
) -> None:
    """Run one bounded single-source CNI worker replacement."""

    resume = _load_resume(args)
    total_deadline = time.monotonic() + args.timeout_seconds
    cleanup_reserve = min(120, max(30, args.timeout_seconds // 5))
    work_deadline = total_deadline - cleanup_reserve
    require(work_deadline > time.monotonic(), "The requested timeout leaves no cleanup reserve")
    summary.update({
        "started_at": utc_now(),
        "status": "starting",
        "cleanup_reserve_seconds": cleanup_reserve,
        "scope": "single-source-real-worker-cni-maintenance",
        "resource_group": args.resource_group,
        "role": args.role,
        "source_worker": args.node_name,
        "source_worker_uid": args.node_uid,
        "source_provider_id": args.source_provider_id,
        "source_network_container_id": args.source_network_container_id,
        "expected_subscription": args.expected_subscription,
        "expected_region": args.expected_region,
        "expected_tfvars_sha": args.expected_tfvars_sha,
        "execute": args.execute,
        "mutation_started": False,
        "success": False,
    })
    if resume:
        summary["resume_provenance"] = resume["provenance"]
    _save(args.summary_file, summary)
    cleanup_errors: List[str] = summary.setdefault("cleanup_errors", [])
    interrupted = {"raised": None}
    operator: Optional[ClusterOperator] = None
    cluster: Optional[mocks.Cluster] = None
    previous_int = signal.getsignal(signal.SIGINT)
    previous_term = signal.getsignal(signal.SIGTERM)

    def interrupt(signum, _frame):
        interrupted["raised"] = signum
        raise MaintenanceInterrupted(f"Single-worker maintenance interrupted by signal {signum}")

    signal.signal(signal.SIGINT, interrupt)
    signal.signal(signal.SIGTERM, interrupt)
    try:
        operator = ClusterOperator(
            args,
            args.context or args.role,
            runner,
            work_deadline,
            total_deadline,
        )
        account = operator.az_json("account", "show", timeout_seconds=args.request_timeout_seconds)
        require(
            str((account or {}).get("id") or "").lower() == args.expected_subscription.lower(),
            "Current Azure subscription does not match the explicit preserved subscription",
        )
        group = operator.az_json(
            "group", "show", "--name", args.resource_group,
            timeout_seconds=args.request_timeout_seconds,
        )
        clusters = operator.az_json(
            "aks", "list", "--resource-group", args.resource_group,
            timeout_seconds=args.request_timeout_seconds,
        )
        members = operator.az_json(
            "fleet", "member", "list",
            "--resource-group", args.resource_group,
            "--fleet-name", "clustermesh-flt",
            timeout_seconds=args.request_timeout_seconds,
        )
        selected, identities = retirement.validate_scope(args, group, clusters, members)
        node_group = operator.az_json(
            "group", "show", "--name", selected["nodeResourceGroup"],
            timeout_seconds=args.request_timeout_seconds,
        )
        require(
            _resource_equal(node_group.get("managedBy"), selected["id"])
            and str(node_group.get("location") or "").lower() == args.expected_region.lower(),
            "Selected AKS node resource group is not owned by the explicit preserved cluster",
        )
        retirement.require_lease(node_group, args.timeout_seconds)
        cluster_name = str(selected["name"])
        operator.cluster_name = cluster_name
        cluster = mocks.Cluster(
            role=args.role,
            kubeconfig=args.kubeconfig,
            context=args.context or cluster_name,
            name=cluster_name,
            resource_group=args.resource_group,
        )
        pool_payload = _current_pool_payload(operator, args, cluster_name)
        cluster_state = workers.probe_cluster(
            workers.Cluster(cluster_name, args.resource_group, args.role, args.kubeconfig),
            lambda command, timeout: operator.run(command, timeout),
            args.request_timeout_seconds,
        )
        summary["initial_worker_state"] = (
            resume["prior"]["initial_worker_state"] if resume else workers.state_to_dict(cluster_state)
        )
        summary["initial_pool_configuration"] = (
            resume["prior"]["initial_pool_configuration"] if resume else _pool_configuration(pool_payload)
        )
        _save(args.summary_file, summary)
        nodes_payload = operator.kubectl_json(
            ["get", "nodes", "-o", "json"],
            timeout_seconds=args.request_timeout_seconds,
        )
        pods_payload = operator.kubectl_json(
            ["-n", DEFAULT_NAMESPACE, "get", "pods", "-l", mocks.AGENT_SELECTOR, "-o", "json"],
            timeout_seconds=args.request_timeout_seconds,
        )
        events_payload = operator.kubectl_json(
            ["-n", DEFAULT_NAMESPACE, "get", "events", "-o", "json"],
            timeout_seconds=args.request_timeout_seconds,
        )
        controller_payload = operator.kubectl_json(
            ["-n", DEFAULT_NAMESPACE, "get", "statefulset", "kwok-node", "-o", "json"],
            timeout_seconds=args.request_timeout_seconds,
        )
        nnc_payload = operator.kubectl_json(
            ["get", "nnc", "-A", "-o", "json"],
            timeout_seconds=args.request_timeout_seconds,
        )
        validation_args = (
            args, selected, cluster_state, pool_payload, nodes_payload,
            pods_payload, events_payload, controller_payload, nnc_payload,
        )
        initial = (
            _validate_resume_state(*validation_args, resume)
            if resume else _validate_cluster_state(*validation_args)
        )
        summary["initial_pool_count"] = initial["initial_pool_count"]
        summary["initial_pending_source_agents"] = [
            row["name"] for row in initial["pending_source_agents"]
        ]
        summary["initial_healthy_source_agents"] = [
            row["name"] for row in initial["healthy_source_agents"]
        ]
        summary["pod_template"] = initial["pod_template"]
        summary["original_real_node_uids"] = initial["all_real_node_uids"]
        summary["original_real_node_identities"] = {
            name: _node_identity(_real_node_map(nodes_payload)[name])
            for name in initial["all_real_node_uids"]
        }
        summary["original_kwok_node_uids"] = initial["kwok_uids"]
        summary["agent_uids"] = initial["agent_uids"]
        summary["controller_uid"] = initial["controller_uid"]
        if resume:
            summary["source_quarantined"] = True
            summary["surge_request_accepted"] = True
            summary["resume_worker_state"] = workers.state_to_dict(cluster_state)
            summary["resume_fresh_network_container_ids"] = resume["manifest"]["fresh_network_container_ids"]
            system_pods = operator.kubectl_json(
                ["get", "pods", "-A", "-o", "json"], timeout_seconds=args.request_timeout_seconds,
            )
            require(
                not any(
                    PROBE_LABEL_KEY in ((pod.get("metadata") or {}).get("labels") or {})
                    or str((pod.get("metadata") or {}).get("name") or "").startswith("cni-maint-probe")
                    for pod in _items(system_pods, "Pod inventory")
                ),
                "Resume refuses existing maintenance probe Pods",
            )
            summary["resume_source_pre_drain"] = _validate_pre_drain_source_pods(
                [
                    pod for pod in _items(system_pods, "Pod inventory")
                    if (pod.get("spec") or {}).get("nodeName") == args.node_name
                    and not (
                        (pod.get("metadata") or {}).get("namespace") == DEFAULT_NAMESPACE
                        and (pod.get("metadata") or {}).get("name") in EXPECTED_AGENT_NAMES
                    )
                ],
                operator.kubectl_json(["-n", "kube-system", "get", "daemonsets", "-o", "json"]),
                operator.kubectl_json(["get", "replicasets", "-A", "-o", "json"]),
                operator.kubectl_json(["get", "deployments", "-A", "-o", "json"]),
                operator.kubectl_json(["get", "statefulsets", "-A", "-o", "json"]),
            )
            if getattr(args, "recover_empty_fresh_node", ""):
                _empty_host_target(operator, args, selected, resume)
            startup = _startup_operator(operator)
            applicable = _derive_applicable_daemonsets(
                system_pods, sorted(name for name in initial["initial_real_node_uids"] if name != args.node_name),
            )
            _wait_for_fresh_daemonsets(startup, args, applicable, sorted(resume["manifest"]["fresh_node_uids"]))
        summary["cilium_before"] = _read_cilium_proof(startup if resume else operator, args, identities)
        _save(args.summary_file, summary)
        _require_cilium_proof(summary["cilium_before"], sorted(_real_node_map(nodes_payload)))
        if not args.execute:
            summary["status"] = "planned"
            summary["success"] = True
            _save(args.summary_file, summary)
            return

        if resume and getattr(args, "recover_empty_fresh_node", ""):
            summary["mutation_started"] = True
            if getattr(args, "replace_empty_fresh", False):
                _replace_empty_fresh_host(operator, args, selected, initial, resume, summary)
                pool_payload = _current_pool_payload(operator, args, cluster_name)
                nodes_payload = operator.kubectl_json(["get", "nodes", "-o", "json"])
            else:
                _recover_empty_fresh_host(operator, args, selected, initial, resume, summary)
            startup = _startup_operator(operator)

        baseline_config = initial["initial_pool_configuration"]
        if resume:
            surge_pool, surge_nodes = pool_payload, nodes_payload
            fresh_nodes = [_real_node_map(nodes_payload)[name] for name in sorted(resume["manifest"]["fresh_node_uids"])]
        else:
            summary["mutation_started"] = True
            _add_source_hold(operator, args, summary)
            current_pool = _current_pool_payload(operator, args, cluster_name)
            current_state = workers.probe_cluster(
                workers.Cluster(cluster_name, args.resource_group, args.role, args.kubeconfig),
                lambda command, timeout: operator.run(command, timeout),
                args.request_timeout_seconds,
            )
            current_nodes = operator.kubectl_json(
                ["get", "nodes", "-o", "json"], timeout_seconds=args.request_timeout_seconds,
            )
            require(
                {name: _node_identity(row) for name, row in _real_node_map(current_nodes).items()}
                == summary["original_real_node_identities"],
                "Original real worker provider, pool or image identity changed before surge",
            )
            _validate_pre_scale_state(
                args, selected, current_state, current_pool, current_nodes,
                initial["initial_pool_count"], baseline_config, initial["all_real_node_uids"],
            )
            operator.run(
                [
                    "az", "aks", "nodepool", "scale",
                    "--resource-group", args.resource_group, "--cluster-name", cluster_name,
                    "--name", DEFAULT_POOL_NAME, "--node-count", str(SURGE_POOL_COUNT),
                    "--no-wait", "--output", "none", "--only-show-errors",
                ],
                args.request_timeout_seconds,
            )
            summary["surge_request_accepted"] = True
            summary["status"] = "waiting-for-surge"
            _save(args.summary_file, summary)
            surge_pool = _wait_for_surge(
                operator, args, cluster_name, baseline_config, initial["initial_pool_count"],
            )
            startup = _startup_operator(operator)
            surge_nodes, fresh_nodes = _wait_for_fresh_nodes(
                startup, args, selected, initial, current_nodes,
            )
        fresh_node_names = [
            str((node.get("metadata") or {}).get("name") or "")
            for node in fresh_nodes
        ]
        summary["fresh_nodes"] = fresh_node_names
        summary["pool_configuration_after_surge"] = _pool_configuration(surge_pool)
        system_pods = operator.kubectl_json(
            ["get", "pods", "-A", "-o", "json"],
            timeout_seconds=args.request_timeout_seconds,
        )
        existing_non_source = sorted(
            name for name in initial["initial_real_node_uids"]
            if name != args.node_name
        )
        applicable_daemonsets = _derive_applicable_daemonsets(system_pods, existing_non_source)
        _wait_for_fresh_daemonsets(startup, args, applicable_daemonsets, fresh_node_names)
        summary["cilium_after_surge"] = _read_cilium_proof(startup, args, identities)
        _save(args.summary_file, summary)
        _require_cilium_proof(summary["cilium_after_surge"], sorted(_real_node_map(surge_nodes)))
        _requalify_surge(operator, args, selected, initial, surge_nodes, resume)
        summary["status"] = "proving-fresh-ip-growth"
        summary["mutation_started"] = True
        _save(args.summary_file, summary)
        ip_budget = _phase_budget(
            operator, maximum_seconds=IP_GROWTH_WAIT_SECONDS,
            reserve_after_seconds=RETIREMENT_MINIMUM_SECONDS + FINAL_QUALIFICATION_RESERVE_SECONDS,
            minimum_seconds=args.request_timeout_seconds, description="bounded IP growth and retirement",
        )
        ip_operator = ClusterOperator(
            args, cluster_name, runner, time.monotonic() + ip_budget, operator.cleanup_deadline,
        )
        _prove_fresh_ip_growth(ip_operator, args, cluster, fresh_nodes, summary)
        if summary.get("empty_host_recovery"):
            summary["empty_host_recovery"]["success"] = True
            summary["empty_host_recovery"]["ip_growth_verified"] = True
            _save(args.summary_file, summary)
        if summary.get("empty_worker_replacement"):
            summary["empty_worker_replacement"]["success"] = True
            summary["empty_worker_replacement"]["ip_growth_verified"] = True
            _save(args.summary_file, summary)
        require(
            not mocks._tolerates(
                {"key": EXCLUSION_KEY, "value": "bounded", "effect": "NoSchedule"},
                initial["pod_template"].get("tolerations", []),
            ),
            "Mock agents tolerate the maintenance exclusion taint",
        )
        old_nodes_to_exclude = sorted(
            name for name in initial["initial_real_node_uids"]
            if name != args.node_name
        )
        if old_nodes_to_exclude:
            _temporary_exclusions(operator, args, summary, old_nodes_to_exclude)
        summary["status"] = "moving-pending"
        _save(args.summary_file, summary)
        protected_uids = dict(initial["initial_ready_agent_uids"])
        pending_moves = _move_agents_one_at_a_time(
            operator,
            args,
            selected,
            cluster,
            summary,
            initial["controller_uid"],
            protected_uids,
            initial["pending_source_agents"],
            fresh_node_names,
            phase="pending",
        )
        summary["pending_moved_count"] = len(pending_moves)
        _save(args.summary_file, summary)
        current_agents = _wait_for_all_mock_ready(
            operator,
            args,
            initial["controller_uid"],
            protected_uids,
        )
        healthy_source_agents = _planned_source_agents(current_agents, args.node_name)
        require(
            len(healthy_source_agents) <= MAX_HEALTHY_SOURCE_AGENTS,
            f"Healthy source mock-agent count exceeds the safety cap {MAX_HEALTHY_SOURCE_AGENTS}",
        )
        full_ready_uids = {
            name: str((pod.get("metadata") or {}).get("uid") or "")
            for name, pod in current_agents.items()
        }
        _require_retirement_reserve(operator)
        summary["status"] = "moving-healthy"
        _save(args.summary_file, summary)
        healthy_moves = _move_agents_one_at_a_time(
            operator,
            args,
            selected,
            cluster,
            summary,
            initial["controller_uid"],
            full_ready_uids,
            healthy_source_agents,
            fresh_node_names,
            phase="healthy",
        )
        summary["healthy_moved_count"] = len(healthy_moves)
        current_agents = _wait_for_all_mock_ready(
            operator,
            args,
            initial["controller_uid"],
            full_ready_uids,
        )
        require(
            not any(
                str((pod.get("spec") or {}).get("nodeName") or "") == args.node_name
                for pod in current_agents.values()
            ),
            "The explicit source still owns mock agents after bounded evacuation",
        )
        summary["status"] = "waiting-kwok-ready"
        _save(args.summary_file, summary)
        _remove_temporary_exclusions(
            operator,
            args,
            summary,
            cleanup_errors=cleanup_errors,
        )
        require(
            not summary.get("temporary_exclusions") and not cleanup_errors,
            "Temporary maintenance exclusions must be removed before drain and retirement",
        )
        _verify_only_source_hold_remains(operator, args, selected)
        _wait_for_kwok_ready(
            operator,
            args,
            initial["kwok_uids"],
            reserve_after_seconds=(
                args.request_timeout_seconds
                + RETIREMENT_MINIMUM_SECONDS
                + FINAL_QUALIFICATION_RESERVE_SECONDS
            ),
        )
        summary["status"] = "draining-source"
        _save(args.summary_file, summary)
        pre_drain_pods = operator.kubectl_json(
            ["get", "pods", "-A", "-o", "json"],
            timeout_seconds=args.request_timeout_seconds,
        )
        source_pods = [
            pod for pod in _items(pre_drain_pods, "Pod inventory")
            if str((pod.get("spec") or {}).get("nodeName") or "") == args.node_name
        ]
        summary["source_pre_drain"] = _validate_pre_drain_source_pods(
            source_pods,
            operator.kubectl_json(
                ["-n", "kube-system", "get", "daemonsets", "-o", "json"],
                timeout_seconds=args.request_timeout_seconds,
            ),
            operator.kubectl_json(
                ["get", "replicasets", "-A", "-o", "json"],
                timeout_seconds=args.request_timeout_seconds,
            ),
            operator.kubectl_json(
                ["get", "deployments", "-A", "-o", "json"],
                timeout_seconds=args.request_timeout_seconds,
            ),
            operator.kubectl_json(
                ["get", "statefulsets", "-A", "-o", "json"],
                timeout_seconds=args.request_timeout_seconds,
            ),
        )
        _save(args.summary_file, summary)
        operator.kubectl(
            [
                "drain",
                args.node_name,
                "--ignore-daemonsets",
                "--delete-emptydir-data",
                "--timeout=15m",
            ],
            timeout_seconds=_phase_budget(
                operator,
                maximum_seconds=DRAIN_PHASE_BUDGET_SECONDS,
                reserve_after_seconds=(
                    RETIREMENT_MINIMUM_SECONDS
                    + FINAL_QUALIFICATION_RESERVE_SECONDS
                ),
                minimum_seconds=args.request_timeout_seconds,
                description="source drain",
            ),
        )
        retirement_summary_file = f"{args.summary_file}.retirement.json"
        retirement_summary = {
            "success": False,
            "mutation_started": False,
            "request_accepted": False,
        }
        retirement_args = argparse.Namespace(**vars(args))
        retirement_args.summary_file = retirement_summary_file
        retirement_args.timeout_seconds = _phase_budget(
            operator,
            maximum_seconds=RETIREMENT_PHASE_BUDGET_SECONDS,
            reserve_after_seconds=FINAL_QUALIFICATION_RESERVE_SECONDS,
            minimum_seconds=RETIREMENT_MINIMUM_SECONDS,
            description="prepared retirement",
        )
        summary["retirement"] = retirement_summary
        summary["status"] = "retiring-source"
        _save(args.summary_file, summary)
        try:
            retirement.execute_retirement(retirement_args, retirement_summary, runner)
        finally:
            _save(args.summary_file, summary)
        require(retirement_summary.get("success") is True, "Prepared source retirement did not succeed")
        summary["status"] = "final-qualification"
        _save(args.summary_file, summary)
        final_nodes = operator.kubectl_json(
            ["get", "nodes", "-o", "json"],
            timeout_seconds=args.request_timeout_seconds,
        )
        final_pods = operator.kubectl_json(
            ["-n", DEFAULT_NAMESPACE, "get", "pods", "-l", mocks.AGENT_SELECTOR, "-o", "json"],
            timeout_seconds=args.request_timeout_seconds,
        )
        final_state = workers.probe_cluster(
            workers.Cluster(cluster_name, args.resource_group, args.role, args.kubeconfig),
            lambda command, timeout: operator.run(command, timeout),
            args.request_timeout_seconds,
        )
        final_default = _pool_from_state(final_state, DEFAULT_POOL_NAME)
        require(
            final_default.desired_count == STEADY_POOL_COUNT and final_default.healthy,
            "Final default pool is not exactly three healthy workers",
        )
        final_nnc = operator.kubectl_json(
            ["get", "nnc", "-A", "-o", "json"],
            timeout_seconds=args.request_timeout_seconds,
        )
        require(args.node_name not in _real_node_map(final_nodes), "The explicit source worker still exists after retirement")
        require(args.node_name not in _nnc_map(final_nnc), "The explicit source NodeNetworkConfig still exists after retirement")
        preserved_agent_uids = dict(initial["initial_ready_agent_uids"])
        for move in summary.get("pending_moves", []) + summary.get("healthy_moves", []):
            preserved_agent_uids.pop(move["name"], None)
        _validate_final_workload_identity(
            final_nodes,
            final_pods,
            initial["controller_uid"],
            initial["kwok_uids"],
            preserved_agent_uids,
        )
        summary["cilium_final"] = _read_cilium_proof(operator, args, identities)
        _save(args.summary_file, summary)
        _require_cilium_proof(summary["cilium_final"], sorted(_real_node_map(final_nodes)))
        summary["status"] = "completed"
    finally:
        if operator is not None:
            operator.cleanup_mode = True
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        host_recovery = summary.get("empty_host_recovery") or summary.get("empty_worker_replacement") or {}
        if operator is not None and (host_recovery.get("redeploy_completed") or host_recovery.get("replacement_completed")) \
                and not host_recovery.get("success") and not host_recovery.get("cordon_retained"):
            try:
                target = operator.kubectl_json(
                    ["get", "node", host_recovery["node_name"], "-o", "json"], cleanup=True,
                )
                require(target["metadata"]["uid"] == host_recovery["node_uid"], "Unqualified recovered host UID changed")
                annotation = f"unqualified-empty-host-recovery build={args.resume_build_id} uid={host_recovery['node_uid']}"
                annotations = dict(_annotations(target))
                require(EMPTY_HOST_RECOVERY_KEY not in annotations, "Another operation owns the recovered host annotation")
                annotations[EMPTY_HOST_RECOVERY_KEY] = annotation
                operator.kubectl([
                    "patch", "node", host_recovery["node_name"], "--type=json", "-p", json.dumps([
                        {"op": "test", "path": "/metadata/uid", "value": host_recovery["node_uid"]},
                        {"op": "test", "path": "/metadata/resourceVersion", "value": target["metadata"]["resourceVersion"]},
                        {"op": "add", "path": "/metadata/annotations", "value": annotations},
                        {"op": "add", "path": "/spec/unschedulable", "value": True},
                    ]),
                ], cleanup=True)
                host_recovery["cordon_retained"] = True
            except EXPECTED_ERRORS as error:
                cleanup_errors.append(f"Failed to quarantine unqualified recovered host: {error}")
        if operator is not None and cluster is not None:
            cleanup_errors.extend(_cleanup_probe_pods(operator, args, cluster, summary))
            _remove_temporary_exclusions(
                operator,
                args,
                summary,
                cleanup_errors=cleanup_errors,
            )
        summary["finished_at"] = utc_now()
        if interrupted["raised"] is not None:
            summary["interrupted"] = True
        summary["success"] = False
        if (
            summary.get("status") in ("planned", "completed")
            and not summary.get("probe_cleanup_pending")
            and not summary.get("temporary_exclusions")
            and not cleanup_errors
        ):
            summary["success"] = True
        _save(args.summary_file, summary)
        signal.signal(signal.SIGINT, previous_int)
        signal.signal(signal.SIGTERM, previous_term)
    if cleanup_errors:
        raise workers.ReconcileError("; ".join(cleanup_errors))


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the single-source real-worker CNI maintenance helper."""

    args = parse_args(argv)
    summary = {
        "scope": "single-source-real-worker-cni-maintenance",
        "resource_group": args.resource_group,
        "role": args.role,
        "source_worker": args.node_name,
        "source_worker_uid": args.node_uid,
        "source_provider_id": args.source_provider_id,
        "source_network_container_id": args.source_network_container_id,
        "execute": args.execute,
        "success": False,
        "mutation_started": False,
    }
    try:
        execute_maintenance(args, summary)
    except (workers.ReconcileError, mocks.RecoveryError, OSError) as error:
        summary["error"] = str(error)
        summary["success"] = False
        _save(args.summary_file, summary)
        print(str(error), file=sys.stderr, flush=True)
        return 1
    status = summary.get("status")
    if status == "planned":
        print(
            f"{args.role}: bounded single-worker CNI maintenance plan validated for {args.node_name}.",
            flush=True,
        )
    else:
        print(
            f"{args.role}: bounded single-worker CNI maintenance completed for {args.node_name}.",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
