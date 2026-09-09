#!/usr/bin/env python3
"""Reschedule mock agents blocked by proven Azure CNI IP exhaustion."""

# pylint: disable=too-many-lines

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
from kubernetes.config.config_exception import ConfigException
from kubernetes.utils.quantity import parse_quantity
from urllib3.exceptions import HTTPError


AGENT_SELECTOR = "app=mock-cilium-agent"
AGENT_CONTROLLER_LABEL = "mock-clustermesh/agent-controller"
AGENT_CONTROLLER_NAME = "kwok-node"
RECOVERY_LABEL = "mock-clustermesh/cni-recovery"
DEFAULT_NAMESPACE = "mock-clustermesh"
MAX_CAPACITY_REPAIR_POOL_COUNT = 3
CNI_ERROR_MARKERS = (
    "allocateipconfig failed",
    "not enough ips available",
)


class RecoveryError(Exception):
    """A bounded, expected CNI recovery failure."""

    def __init__(self, message: str, *, evidence: Optional[dict] = None):
        super().__init__(message)
        self.evidence = evidence or {}


class RecoveryInterrupted(RecoveryError):
    """Recovery was interrupted and must still restore node schedulability."""


@dataclass(frozen=True)
class Cluster:
    """One exact cluster selected from the handoff inventory."""

    role: str
    kubeconfig: str
    context: str
    name: str = ""
    resource_group: str = ""


@dataclass(frozen=True)
class CapacityRepairConfig:
    """Bounded node-pool repair settings for one explicit recovery role."""

    enabled: bool = False
    subscription_id: str = ""
    pool_name: str = "default"
    max_pool_count: int = 3
    timeout_seconds: int = 1800
    poll_seconds: int = 15
    cpu_reserve_millicores: int = 250
    memory_reserve_mib: int = 512
    pod_reserve: int = 5
    cilium_health_script: str = ""
    cilium_identity_inventory: str = ""
    expected_remote_count: int = 99


def utc_now() -> str:
    """Return an RFC3339 UTC timestamp."""

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_json_atomic(path: str, payload: dict) -> None:
    """Write one JSON object atomically."""

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    temporary = f"{path}.tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(temporary, path)


def run_command(
    command: Sequence[str],
    timeout_seconds: float,
) -> subprocess.CompletedProcess:
    """Run one bounded command; split out for tests."""

    return subprocess.run(
        list(command),
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )


def _kubectl_prefix(cluster: Cluster, request_timeout_seconds: int) -> List[str]:
    command = [
        "kubectl",
        "--kubeconfig",
        cluster.kubeconfig,
        f"--request-timeout={request_timeout_seconds}s",
    ]
    if cluster.context:
        command.extend(["--context", cluster.context])
    return command


def kubectl(
    cluster: Cluster,
    arguments: Sequence[str],
    *,
    timeout_seconds: int,
    attempts: int,
    retry_seconds: int,
) -> str:
    """Run one idempotent kubectl operation with bounded retries."""

    command = _kubectl_prefix(cluster, timeout_seconds) + list(arguments)
    detail = ""
    for attempt in range(1, attempts + 1):
        try:
            result = run_command(command, timeout_seconds + 5)
        except subprocess.TimeoutExpired as error:
            detail = f"timed out after {error.timeout}s"
        except OSError as error:
            detail = str(error)
        else:
            if result.returncode == 0:
                return result.stdout
            detail = (result.stderr or result.stdout or "").strip()[:2000]
        if attempt < attempts and retry_seconds > 0:
            time.sleep(retry_seconds)
    raise RecoveryError(
        f"{cluster.role}: kubectl {' '.join(arguments)} failed after "
        f"{attempts} attempt(s): {detail or 'unknown error'}"
    )


def kubectl_json(
    cluster: Cluster,
    arguments: Sequence[str],
    *,
    timeout_seconds: int,
    attempts: int,
    retry_seconds: int,
) -> dict:
    """Run kubectl and require a JSON object response."""

    output = kubectl(
        cluster,
        arguments,
        timeout_seconds=timeout_seconds,
        attempts=attempts,
        retry_seconds=retry_seconds,
    )
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise RecoveryError(
            f"{cluster.role}: kubectl returned invalid JSON"
        ) from error
    if not isinstance(payload, dict):
        raise RecoveryError(
            f"{cluster.role}: kubectl JSON response is not an object"
        )
    return payload


def load_clusters(path: str, roles: Sequence[str]) -> List[Cluster]:
    """Load exactly the requested roles from the standard cluster inventory."""

    try:
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        raise RecoveryError(f"unable to load cluster inventory: {error}") from error
    if not isinstance(payload, list):
        raise RecoveryError("cluster inventory must be a JSON array")
    by_role = {}
    for row in payload:
        if not isinstance(row, dict):
            raise RecoveryError("cluster inventory contains a non-object row")
        role = row.get("role")
        if not isinstance(role, str) or not role:
            raise RecoveryError("cluster inventory row is missing role")
        kubeconfig = row.get("kubeconfig") or os.path.join(
            os.path.expanduser("~"),
            ".kube",
            f"{role}.config",
        )
        if not isinstance(kubeconfig, str) or not kubeconfig:
            raise RecoveryError(f"{role}: invalid kubeconfig path")
        if role in by_role:
            raise RecoveryError(f"cluster inventory contains duplicate role {role}")
        by_role[role] = Cluster(
            role=role,
            kubeconfig=kubeconfig,
            context=str(row.get("context") or row.get("name") or ""),
            name=str(row.get("name") or ""),
            resource_group=str(
                row.get("rg") or row.get("resource_group") or ""
            ),
        )
    missing = [role for role in roles if role not in by_role]
    if missing:
        raise RecoveryError(f"requested recovery roles are missing: {missing}")
    return [by_role[role] for role in roles]


def _items(payload: dict, description: str) -> List[dict]:
    items = payload.get("items")
    if not isinstance(items, list) or not all(
        isinstance(item, dict) for item in items
    ):
        raise RecoveryError(f"{description} did not return a usable items list")
    return items


def _pod_ready(pod: dict) -> bool:
    status = pod.get("status") or {}
    statuses = status.get("containerStatuses") or []
    return (
        status.get("phase") == "Running"
        and isinstance(statuses, list)
        and bool(statuses)
        and all(
            isinstance(container, dict) and container.get("ready") is True
            for container in statuses
        )
    )


def _pending_container_creating(pod: dict) -> bool:
    status = pod.get("status") or {}
    statuses = status.get("containerStatuses") or []
    return (
        status.get("phase") == "Pending"
        and isinstance(statuses, list)
        and any(
            isinstance(container, dict)
            and ((container.get("state") or {}).get("waiting") or {}).get(
                "reason"
            )
            == "ContainerCreating"
            for container in statuses
        )
        and bool((pod.get("spec") or {}).get("nodeName"))
    )


def _node_ready_and_schedulable(node: dict) -> bool:
    conditions = (node.get("status") or {}).get("conditions") or []
    ready = any(
        isinstance(condition, dict)
        and condition.get("type") == "Ready"
        and condition.get("status") == "True"
        for condition in conditions
    )
    return ready and not bool((node.get("spec") or {}).get("unschedulable"))


def _event_proves_cni_exhaustion(event: dict) -> bool:
    message = str(event.get("message") or "").lower()
    return (
        event.get("reason") == "FailedCreatePodSandBox"
        and all(marker in message for marker in CNI_ERROR_MARKERS)
    )


def _owned_by_expected_statefulset(pod: dict) -> bool:
    metadata = pod.get("metadata") or {}
    labels = metadata.get("labels") or {}
    owners = metadata.get("ownerReferences") or []
    return (
        labels.get(AGENT_CONTROLLER_LABEL) == AGENT_CONTROLLER_NAME
        and isinstance(owners, list)
        and any(
            isinstance(owner, dict)
            and owner.get("kind") == "StatefulSet"
            and owner.get("name") == AGENT_CONTROLLER_NAME
            and owner.get("controller") is True
            for owner in owners
        )
    )


def discover_cni_blocked_agents(
    pods_payload: dict,
    events_payload: dict,
) -> List[dict]:
    """Return Pending agents whose own events prove Azure CNI IP exhaustion."""

    pending = {
        (
            str((pod.get("metadata") or {}).get("name")),
            str((pod.get("metadata") or {}).get("uid")),
        ): pod
        for pod in _items(pods_payload, "mock-agent inventory")
        if _pending_container_creating(pod)
        and _owned_by_expected_statefulset(pod)
        and (pod.get("metadata") or {}).get("name")
        and (pod.get("metadata") or {}).get("uid")
    }
    proven_pods = {
        (
            str((event.get("involvedObject") or {}).get("name")),
            str((event.get("involvedObject") or {}).get("uid")),
        )
        for event in _items(events_payload, "mock-agent event inventory")
        if _event_proves_cni_exhaustion(event)
        and (event.get("involvedObject") or {}).get("kind") == "Pod"
        and (event.get("involvedObject") or {}).get("name")
        and (event.get("involvedObject") or {}).get("uid")
    }
    return [
        pending[identity]
        for identity in sorted(set(pending) & proven_pods)
    ]


def _pod_map(payload: dict) -> Dict[str, dict]:
    return {
        str((pod.get("metadata") or {}).get("name")): pod
        for pod in _items(payload, "mock-agent inventory")
        if (pod.get("metadata") or {}).get("name")
    }


def _controller_details(payload: dict) -> tuple[str, dict]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        raise RecoveryError("mock-agent StatefulSet metadata is missing")
    uid = metadata.get("uid")
    if not isinstance(uid, str) or not uid:
        raise RecoveryError("mock-agent StatefulSet UID is missing")
    spec = payload.get("spec")
    template = spec.get("template") if isinstance(spec, dict) else None
    pod_spec = template.get("spec") if isinstance(template, dict) else None
    if not isinstance(pod_spec, dict):
        raise RecoveryError("mock-agent StatefulSet Pod template is missing")
    return uid, pod_spec


def _pod_owned_by_controller_uid(pod: dict, controller_uid: str) -> bool:
    owners = (pod.get("metadata") or {}).get("ownerReferences") or []
    return isinstance(owners, list) and any(
        isinstance(owner, dict)
        and owner.get("kind") == "StatefulSet"
        and owner.get("name") == AGENT_CONTROLLER_NAME
        and owner.get("uid") == controller_uid
        and owner.get("controller") is True
        for owner in owners
    )


def _requirement_matches(
    values: Dict[str, str],
    requirement: dict,
) -> bool:
    key = requirement.get("key")
    operator = requirement.get("operator")
    expected = requirement.get("values") or []
    if not isinstance(key, str) or not isinstance(operator, str):
        raise RecoveryError("StatefulSet node affinity is malformed")
    if not isinstance(expected, list):
        raise RecoveryError("StatefulSet node affinity values are malformed")
    actual = values.get(key)
    if operator == "In":
        return actual is not None and actual in expected
    if operator == "NotIn":
        return actual is None or actual not in expected
    if operator == "Exists":
        return key in values
    if operator == "DoesNotExist":
        return key not in values
    if operator in ("Gt", "Lt"):
        if actual is None or len(expected) != 1:
            return False
        try:
            actual_number = int(actual)
            expected_number = int(expected[0])
        except (TypeError, ValueError):
            return False
        return (
            actual_number > expected_number
            if operator == "Gt"
            else actual_number < expected_number
        )
    raise RecoveryError(
        f"unsupported StatefulSet node affinity operator {operator!r}"
    )


def _tolerates(taint: dict, tolerations: Sequence[dict]) -> bool:
    key = taint.get("key")
    value = str(taint.get("value") or "")
    effect = taint.get("effect")
    for toleration in tolerations:
        if not isinstance(toleration, dict):
            raise RecoveryError("StatefulSet toleration is malformed")
        tolerated_effect = toleration.get("effect")
        if tolerated_effect and tolerated_effect != effect:
            continue
        operator = toleration.get("operator") or "Equal"
        tolerated_key = toleration.get("key")
        if operator == "Exists":
            if not tolerated_key or tolerated_key == key:
                return True
        elif operator == "Equal":
            if (
                tolerated_key == key
                and str(toleration.get("value") or "") == value
            ):
                return True
        else:
            raise RecoveryError(
                f"unsupported StatefulSet toleration operator {operator!r}"
            )
    return False


def _node_matches_pod_template(node: dict, pod_spec: dict) -> bool:
    metadata = node.get("metadata") or {}
    labels = metadata.get("labels") or {}
    node_name = str(metadata.get("name") or "")
    if not isinstance(labels, dict):
        raise RecoveryError("node labels are malformed")

    node_selector = pod_spec.get("nodeSelector") or {}
    if not isinstance(node_selector, dict):
        raise RecoveryError("StatefulSet nodeSelector is malformed")
    if any(labels.get(key) != value for key, value in node_selector.items()):
        return False
    if pod_spec.get("nodeName") and pod_spec.get("nodeName") != node_name:
        return False

    affinity = pod_spec.get("affinity") or {}
    if not isinstance(affinity, dict):
        raise RecoveryError("StatefulSet affinity is malformed")
    for affinity_type in ("podAffinity", "podAntiAffinity"):
        pod_affinity = affinity.get(affinity_type) or {}
        if not isinstance(pod_affinity, dict):
            raise RecoveryError(f"StatefulSet {affinity_type} is malformed")
        if pod_affinity.get("requiredDuringSchedulingIgnoredDuringExecution"):
            raise RecoveryError(
                f"StatefulSet required {affinity_type} is unsupported "
                "for targeted CNI recovery"
            )

    node_affinity = affinity.get("nodeAffinity") or {}
    if not isinstance(node_affinity, dict):
        raise RecoveryError("StatefulSet nodeAffinity is malformed")
    required = (
        node_affinity.get("requiredDuringSchedulingIgnoredDuringExecution")
        or {}
    )
    if not isinstance(required, dict):
        raise RecoveryError("StatefulSet required nodeAffinity is malformed")
    terms = required.get("nodeSelectorTerms") or []
    if not isinstance(terms, list):
        raise RecoveryError("StatefulSet nodeSelectorTerms are malformed")
    if terms:
        fields = {"metadata.name": node_name}
        term_matches = False
        for term in terms:
            if not isinstance(term, dict):
                raise RecoveryError("StatefulSet nodeSelectorTerm is malformed")
            expressions = term.get("matchExpressions") or []
            match_fields = term.get("matchFields") or []
            if not isinstance(expressions, list) or not isinstance(
                match_fields,
                list,
            ):
                raise RecoveryError(
                    "StatefulSet node affinity requirements are malformed"
                )
            if all(
                isinstance(requirement, dict)
                and _requirement_matches(labels, requirement)
                for requirement in expressions
            ) and all(
                isinstance(requirement, dict)
                and _requirement_matches(fields, requirement)
                for requirement in match_fields
            ):
                term_matches = True
                break
        if not term_matches:
            return False

    topology_constraints = pod_spec.get("topologySpreadConstraints") or []
    if not isinstance(topology_constraints, list):
        raise RecoveryError(
            "StatefulSet topologySpreadConstraints are malformed"
        )
    if any(
        isinstance(constraint, dict)
        and constraint.get("whenUnsatisfiable") == "DoNotSchedule"
        for constraint in topology_constraints
    ):
        raise RecoveryError(
            "StatefulSet hard topology spread is unsupported "
            "for targeted CNI recovery"
        )

    tolerations = pod_spec.get("tolerations") or []
    if not isinstance(tolerations, list):
        raise RecoveryError("StatefulSet tolerations are malformed")
    taints = (node.get("spec") or {}).get("taints") or []
    if not isinstance(taints, list):
        raise RecoveryError("node taints are malformed")
    return all(
        not isinstance(taint, dict)
        or taint.get("effect") not in ("NoSchedule", "NoExecute")
        or _tolerates(taint, tolerations)
        for taint in taints
    )


def _quantity(value: object, description: str) -> Decimal:
    if value in (None, ""):
        return Decimal(0)
    try:
        return parse_quantity(str(value))
    except (InvalidOperation, ValueError) as error:
        raise RecoveryError(
            f"invalid Kubernetes quantity for {description}: {value!r}"
        ) from error


def _resource_requests(pod: dict) -> Tuple[int, int]:
    """Return the conservative scheduler CPU/memory request for one Pod."""

    spec = pod.get("spec") or {}
    containers = spec.get("containers") or []
    init_containers = spec.get("initContainers") or []
    if not isinstance(containers, list) or not isinstance(
        init_containers,
        list,
    ):
        raise RecoveryError("Pod container inventory is malformed")

    def container_request(container: dict) -> Tuple[Decimal, Decimal]:
        resources = container.get("resources") or {}
        requests = resources.get("requests") or {}
        if not isinstance(requests, dict):
            raise RecoveryError("Pod resource requests are malformed")
        return (
            _quantity(requests.get("cpu"), "container CPU request"),
            _quantity(requests.get("memory"), "container memory request"),
        )

    regular = [
        container_request(container)
        for container in containers
        if isinstance(container, dict)
    ]
    init = [
        container_request(container)
        for container in init_containers
        if isinstance(container, dict)
    ]
    if len(regular) != len(containers) or len(init) != len(init_containers):
        raise RecoveryError("Pod container entry is malformed")
    regular_cpu = sum((request[0] for request in regular), Decimal(0))
    regular_memory = sum((request[1] for request in regular), Decimal(0))
    restartable_cpu = Decimal(0)
    restartable_memory = Decimal(0)
    init_cpu = Decimal(0)
    init_memory = Decimal(0)
    for container, request in zip(init_containers, init):
        if container.get("restartPolicy") == "Always":
            restartable_cpu += request[0]
            restartable_memory += request[1]
            candidate_cpu = restartable_cpu
            candidate_memory = restartable_memory
        else:
            candidate_cpu = restartable_cpu + request[0]
            candidate_memory = restartable_memory + request[1]
        init_cpu = max(init_cpu, candidate_cpu)
        init_memory = max(init_memory, candidate_memory)
    overhead = spec.get("overhead") or {}
    if not isinstance(overhead, dict):
        raise RecoveryError("Pod overhead is malformed")
    cpu = max(regular_cpu + restartable_cpu, init_cpu) + _quantity(
        overhead.get("cpu"),
        "Pod CPU overhead",
    )
    memory = max(
        regular_memory + restartable_memory,
        init_memory,
    ) + _quantity(
        overhead.get("memory"),
        "Pod memory overhead",
    )
    pod_resources = spec.get("resources") or {}
    if not isinstance(pod_resources, dict):
        raise RecoveryError("Pod-level resources are malformed")
    pod_requests = pod_resources.get("requests") or {}
    if not isinstance(pod_requests, dict):
        raise RecoveryError("Pod-level resource requests are malformed")
    cpu = max(
        cpu,
        _quantity(pod_requests.get("cpu"), "Pod-level CPU request"),
    )
    memory = max(
        memory,
        _quantity(pod_requests.get("memory"), "Pod-level memory request"),
    )
    return int(cpu * 1000), int(memory)


def _node_pool_name(node: dict) -> str:
    labels = (node.get("metadata") or {}).get("labels") or {}
    return str(
        labels.get("kubernetes.azure.com/agentpool")
        or labels.get("agentpool")
        or ""
    )


def _eligible_capacity_node(
    node: dict,
    saturated_nodes: Sequence[str],
    pod_template: dict,
) -> bool:
    metadata = node.get("metadata") or {}
    labels = metadata.get("labels") or {}
    name = str(metadata.get("name") or "")
    return bool(
        name
        and name not in saturated_nodes
        and labels.get("type") != "kwok"
        and "kubernetes.azure.com/cluster" in labels
        and "prometheus" not in labels
        and _node_ready_and_schedulable(node)
        and _node_matches_pod_template(node, pod_template)
    )


def assess_recovery_capacity(
    *,
    nodes_payload: dict,
    pods_payload: dict,
    affected: Sequence[dict],
    saturated_nodes: Sequence[str],
    pod_template: dict,
    config_settings: CapacityRepairConfig,
) -> dict:
    """Prove all affected Pods fit eligible alternate nodes before mutation."""

    candidates = {}
    for node in _items(nodes_payload, "node inventory"):
        metadata = node.get("metadata") or {}
        name = str(metadata.get("name") or "")
        if not _eligible_capacity_node(
            node,
            saturated_nodes,
            pod_template,
        ):
            continue
        allocatable = (node.get("status") or {}).get("allocatable") or {}
        if not isinstance(allocatable, dict):
            raise RecoveryError(f"{name}: node allocatable is malformed")
        candidates[name] = {
            "pool": _node_pool_name(node),
            "allocatable_cpu_millicores": int(
                _quantity(allocatable.get("cpu"), f"{name} allocatable CPU")
                * 1000
            ),
            "allocatable_memory_bytes": int(
                _quantity(
                    allocatable.get("memory"),
                    f"{name} allocatable memory",
                )
            ),
            "allocatable_pods": int(
                _quantity(allocatable.get("pods"), f"{name} allocatable Pods")
            ),
            "requested_cpu_millicores": 0,
            "requested_memory_bytes": 0,
            "active_pods": 0,
        }

    for pod in _items(pods_payload, "all-Pod inventory"):
        phase = str((pod.get("status") or {}).get("phase") or "")
        node_name = str((pod.get("spec") or {}).get("nodeName") or "")
        if phase in ("Succeeded", "Failed") or node_name not in candidates:
            continue
        cpu_millicores, memory_bytes = _resource_requests(pod)
        candidates[node_name]["requested_cpu_millicores"] += cpu_millicores
        candidates[node_name]["requested_memory_bytes"] += memory_bytes
        candidates[node_name]["active_pods"] += 1

    remaining = {}
    for name, capacity in candidates.items():
        capacity["available_cpu_millicores"] = max(
            0,
            capacity["allocatable_cpu_millicores"]
            - capacity["requested_cpu_millicores"]
            - config_settings.cpu_reserve_millicores,
        )
        capacity["available_memory_bytes"] = max(
            0,
            capacity["allocatable_memory_bytes"]
            - capacity["requested_memory_bytes"]
            - config_settings.memory_reserve_mib * 1024 * 1024,
        )
        capacity["available_pod_slots"] = max(
            0,
            capacity["allocatable_pods"]
            - capacity["active_pods"]
            - config_settings.pod_reserve,
        )
        remaining[name] = {
            "cpu": capacity["available_cpu_millicores"],
            "memory": capacity["available_memory_bytes"],
            "pods": capacity["available_pod_slots"],
        }

    requirements = []
    for pod in affected:
        name = str((pod.get("metadata") or {}).get("name") or "")
        cpu_millicores, memory_bytes = _resource_requests(pod)
        requirements.append(
            {
                "name": name,
                "cpu_millicores": cpu_millicores,
                "memory_bytes": memory_bytes,
            }
        )
    requirements.sort(
        key=lambda request: (
            request["cpu_millicores"],
            request["memory_bytes"],
            request["name"],
        ),
        reverse=True,
    )

    placements = {}
    unplaced = []
    for request in requirements:
        eligible = [
            name
            for name, available in remaining.items()
            if available["cpu"] >= request["cpu_millicores"]
            and available["memory"] >= request["memory_bytes"]
            and available["pods"] >= 1
        ]
        if not eligible:
            unplaced.append(request["name"])
            continue
        selected = max(
            eligible,
            key=lambda name: (
                remaining[name]["cpu"],
                remaining[name]["memory"],
                remaining[name]["pods"],
                name,
            ),
        )
        placements[request["name"]] = selected
        remaining[selected]["cpu"] -= request["cpu_millicores"]
        remaining[selected]["memory"] -= request["memory_bytes"]
        remaining[selected]["pods"] -= 1

    return {
        "sufficient": not unplaced,
        "alternate_nodes": sorted(candidates),
        "nodes": candidates,
        "required": {
            "pod_count": len(requirements),
            "cpu_millicores": sum(
                request["cpu_millicores"] for request in requirements
            ),
            "memory_bytes": sum(
                request["memory_bytes"] for request in requirements
            ),
        },
        "planned_placements": placements,
        "unplaced_agents": sorted(unplaced),
        "reserves": {
            "cpu_millicores_per_node": (
                config_settings.cpu_reserve_millicores
            ),
            "memory_mib_per_node": config_settings.memory_reserve_mib,
            "pod_slots_per_node": config_settings.pod_reserve,
        },
    }


def _checked_command(
    command: Sequence[str],
    *,
    timeout_seconds: int,
    description: str,
) -> str:
    try:
        result = run_command(command, timeout_seconds)
    except subprocess.TimeoutExpired as error:
        raise RecoveryError(
            f"{description} timed out after {error.timeout}s"
        ) from error
    except OSError as error:
        raise RecoveryError(f"{description} failed: {error}") from error
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()[:2000]
        raise RecoveryError(
            f"{description} failed with exit {result.returncode}: "
            f"{detail or 'unknown error'}"
        )
    return result.stdout


def _read_nodepool(
    cluster: Cluster,
    config_settings: CapacityRepairConfig,
    request_timeout_seconds: int,
) -> dict:
    if not cluster.name or not cluster.resource_group:
        raise RecoveryError(
            f"{cluster.role}: capacity repair requires cluster name and "
            "resource group"
        )
    output = _checked_command(
        [
            "az",
            "aks",
            "nodepool",
            "show",
            "--subscription",
            config_settings.subscription_id,
            "--resource-group",
            cluster.resource_group,
            "--cluster-name",
            cluster.name,
            "--name",
            config_settings.pool_name,
            "--output",
            "json",
            "--only-show-errors",
        ],
        timeout_seconds=request_timeout_seconds,
        description=f"{cluster.role} node-pool query",
    )
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise RecoveryError(
            f"{cluster.role}: node-pool query returned invalid JSON"
        ) from error
    if not isinstance(payload, dict) or not isinstance(
        payload.get("count"),
        int,
    ):
        raise RecoveryError(
            f"{cluster.role}: node-pool query returned an invalid count"
        )
    return payload


def _wait_nodepool_stable(
    cluster: Cluster,
    config_settings: CapacityRepairConfig,
    *,
    minimum_count: int,
    deadline: float,
    request_timeout_seconds: int,
) -> dict:
    last_payload = {}
    while time.monotonic() < deadline:
        query_timeout = _remaining_timeout(
            deadline,
            request_timeout_seconds,
        )
        last_payload = _read_nodepool(
            cluster,
            config_settings,
            query_timeout,
        )
        if (
            last_payload.get("count", 0) >= minimum_count
            and last_payload.get("provisioningState") == "Succeeded"
            and (last_payload.get("powerState") or {}).get("code") == "Running"
        ):
            return last_payload
        sleep_seconds = min(
            config_settings.poll_seconds,
            max(0, int(deadline - time.monotonic())),
        )
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    raise RecoveryError(
        f"{cluster.role}: node pool {config_settings.pool_name} did not "
        f"stabilize at count>={minimum_count} within "
        f"{config_settings.timeout_seconds}s; last state="
        f"{last_payload.get('provisioningState')!r} "
        f"count={last_payload.get('count')!r}"
    )


def _scale_nodepool_once(
    cluster: Cluster,
    config_settings: CapacityRepairConfig,
    *,
    target_count: int,
    request_timeout_seconds: int,
    command_attempts: int,
    command_retry_seconds: int,
    deadline: float,
) -> None:
    if target_count > MAX_CAPACITY_REPAIR_POOL_COUNT:
        raise RecoveryError(
            f"{cluster.role}: refusing node-pool target {target_count}; "
            f"hard maximum is {MAX_CAPACITY_REPAIR_POOL_COUNT}"
        )
    command = [
        "az",
        "aks",
        "nodepool",
        "scale",
        "--subscription",
        config_settings.subscription_id,
        "--resource-group",
        cluster.resource_group,
        "--cluster-name",
        cluster.name,
        "--name",
        config_settings.pool_name,
        "--node-count",
        str(target_count),
        "--no-wait",
        "--output",
        "none",
        "--only-show-errors",
    ]
    detail = ""
    for attempt in range(1, command_attempts + 1):
        mutation_timeout = _remaining_timeout(
            deadline,
            request_timeout_seconds + 5,
        )
        try:
            result = run_command(command, mutation_timeout)
        except subprocess.TimeoutExpired as error:
            detail = f"timed out after {error.timeout}s"
        except OSError as error:
            detail = str(error)
        else:
            if result.returncode == 0:
                return
            detail = (result.stderr or result.stdout or "").strip()[:2000]
        current = _read_nodepool(
            cluster,
            config_settings,
            _remaining_timeout(deadline, request_timeout_seconds),
        )
        if current["count"] >= target_count:
            return
        if attempt < command_attempts and command_retry_seconds > 0:
            sleep_seconds = min(
                command_retry_seconds,
                max(0, int(deadline - time.monotonic())),
            )
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
    raise RecoveryError(
        f"{cluster.role}: node-pool scale to {target_count} failed after "
        f"{command_attempts} attempt(s): {detail or 'unknown error'}"
    )


def _run_cilium_health_after_repair(
    cluster: Cluster,
    config_settings: CapacityRepairConfig,
    *,
    deadline: float,
    required_node_names: Sequence[str],
) -> dict:
    if not os.path.isfile(config_settings.cilium_health_script):
        raise RecoveryError(
            f"{cluster.role}: Cilium health script is missing: "
            f"{config_settings.cilium_health_script}"
        )
    if not os.path.isfile(config_settings.cilium_identity_inventory):
        raise RecoveryError(
            f"{cluster.role}: Cilium identity inventory is missing: "
            f"{config_settings.cilium_identity_inventory}"
        )
    descriptor, summary_path = tempfile.mkstemp(
        prefix=f"{cluster.role}-capacity-cilium-",
        suffix=".json",
    )
    os.close(descriptor)
    required_nodes = set(required_node_names)
    last_detail = "no probe completed"
    try:
        while time.monotonic() < deadline:
            remaining_seconds = _remaining_timeout(
                deadline,
                config_settings.timeout_seconds,
            )
            try:
                result = run_command(
                    [
                        sys.executable,
                        config_settings.cilium_health_script,
                        "--role",
                        cluster.role,
                        "--kubeconfig",
                        cluster.kubeconfig,
                        "--expected-remote-count",
                        str(config_settings.expected_remote_count),
                        "--identity-inventory",
                        config_settings.cilium_identity_inventory,
                        "--attempts",
                        "1",
                        "--retry-seconds",
                        "0",
                        "--command-timeout-seconds",
                        "45",
                        "--summary-file",
                        summary_path,
                    ],
                    remaining_seconds,
                )
            except subprocess.TimeoutExpired as error:
                last_detail = f"probe timed out after {error.timeout}s"
            except OSError as error:
                last_detail = f"probe failed: {error}"
            else:
                try:
                    with open(summary_path, encoding="utf-8") as handle:
                        summary = json.load(handle)
                except (OSError, json.JSONDecodeError) as error:
                    last_detail = (
                        f"unable to read Cilium evidence: {error}"
                    )
                else:
                    covered_nodes = {
                        str(agent.get("node_name") or "")
                        for agent in summary.get("agents") or []
                        if isinstance(agent, dict) and agent.get("healthy") is True
                    }
                    missing_nodes = sorted(required_nodes - covered_nodes)
                    if (
                        result.returncode == 0
                        and summary.get("healthy") is True
                        and not missing_nodes
                    ):
                        summary["required_node_names"] = sorted(
                            required_nodes
                        )
                        summary["covered_node_names"] = sorted(covered_nodes)
                        return summary
                    detail = (result.stderr or result.stdout or "").strip()[
                        :2000
                    ]
                    coverage_detail = (
                        "missing Cilium coverage on "
                        + ",".join(missing_nodes)
                        if missing_nodes
                        else ""
                    )
                    last_detail = (
                        detail
                        or str(summary.get("fatal_error") or "")
                        or coverage_detail
                        or "unhealthy"
                    )
            sleep_seconds = min(
                config_settings.poll_seconds,
                max(0, int(deadline - time.monotonic())),
            )
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
        raise RecoveryError(
            f"{cluster.role}: post-repair Cilium health did not converge: "
            f"{last_detail}"
        )
    finally:
        try:
            os.unlink(summary_path)
        except FileNotFoundError:
            pass


def _error_with_capacity_evidence(
    error: BaseException,
    evidence: dict,
) -> RecoveryError:
    merged = (
        dict(error.evidence)
        if isinstance(error, RecoveryError)
        else {}
    )
    merged.setdefault("capacity", evidence)
    return RecoveryError(str(error), evidence=merged)


def _with_capacity_evidence(operation, evidence: dict):
    try:
        return operation()
    except (RecoveryError, OSError) as error:
        raise _error_with_capacity_evidence(error, evidence) from error


def _sleep_with_capacity_evidence(seconds: int, evidence: dict) -> None:
    if seconds <= 0:
        return
    try:
        time.sleep(seconds)
    except RecoveryError as error:
        raise _error_with_capacity_evidence(error, evidence) from error


def _remaining_timeout(
    deadline: float,
    configured_timeout_seconds: int,
    *,
    process_grace_seconds: int = 0,
) -> int:
    remaining = int(deadline - time.monotonic())
    available = remaining - process_grace_seconds
    if available < 1:
        raise RecoveryError("capacity-repair deadline exhausted")
    return min(configured_timeout_seconds, available)


def _daemonset_convergence(payload: dict) -> dict:
    unhealthy = []
    total = 0
    for daemonset in _items(payload, "DaemonSet inventory"):
        total += 1
        metadata = daemonset.get("metadata") or {}
        status = daemonset.get("status") or {}
        name = (
            f"{metadata.get('namespace') or 'default'}/"
            f"{metadata.get('name') or 'unknown'}"
        )
        generation = metadata.get("generation")
        observed = status.get("observedGeneration")
        desired = status.get("desiredNumberScheduled")
        current = status.get("currentNumberScheduled")
        ready = status.get("numberReady")
        updated = status.get("updatedNumberScheduled")
        unavailable = status.get("numberUnavailable") or 0
        healthy = (
            isinstance(generation, int)
            and isinstance(observed, int)
            and observed >= generation
            and isinstance(desired, int)
            and current == desired
            and ready == desired
            and updated == desired
            and unavailable == 0
        )
        if not healthy:
            unhealthy.append(
                {
                    "name": name,
                    "generation": generation,
                    "observed_generation": observed,
                    "desired": desired,
                    "current": current,
                    "ready": ready,
                    "updated": updated,
                    "unavailable": unavailable,
                }
            )
    return {
        "converged": not unhealthy,
        "total": total,
        "unhealthy": unhealthy,
    }


def _read_live_recovery_capacity(
    cluster: Cluster,
    *,
    affected: Sequence[dict],
    saturated_nodes: Sequence[str],
    pod_template: dict,
    config_settings: CapacityRepairConfig,
    request_timeout_seconds: int,
    command_attempts: int,
    command_retry_seconds: int,
    deadline: Optional[float] = None,
    require_daemonsets: bool = False,
) -> dict:
    attempts = command_attempts if deadline is None else 1
    daemonset_summary = None
    if require_daemonsets:
        daemonset_timeout = request_timeout_seconds
        if deadline is not None:
            daemonset_timeout = _remaining_timeout(
                deadline,
                request_timeout_seconds,
                process_grace_seconds=5,
            )
        daemonsets_payload = kubectl_json(
            cluster,
            ["get", "daemonsets", "--all-namespaces", "-o", "json"],
            timeout_seconds=daemonset_timeout,
            attempts=attempts,
            retry_seconds=command_retry_seconds,
        )
        daemonset_summary = _daemonset_convergence(daemonsets_payload)

    node_timeout = request_timeout_seconds
    if deadline is not None:
        node_timeout = _remaining_timeout(
            deadline,
            request_timeout_seconds,
            process_grace_seconds=5,
        )
    nodes_payload = kubectl_json(
        cluster,
        ["get", "nodes", "-o", "json"],
        timeout_seconds=node_timeout,
        attempts=attempts,
        retry_seconds=command_retry_seconds,
    )
    pod_timeout = request_timeout_seconds
    if deadline is not None:
        pod_timeout = _remaining_timeout(
            deadline,
            request_timeout_seconds,
            process_grace_seconds=5,
        )
    pods_payload = kubectl_json(
        cluster,
        ["get", "pods", "--all-namespaces", "-o", "json"],
        timeout_seconds=pod_timeout,
        attempts=attempts,
        retry_seconds=command_retry_seconds,
    )
    assessment = assess_recovery_capacity(
        nodes_payload=nodes_payload,
        pods_payload=pods_payload,
        affected=affected,
        saturated_nodes=saturated_nodes,
        pod_template=pod_template,
        config_settings=config_settings,
    )
    if daemonset_summary is not None:
        assessment["daemonsets"] = daemonset_summary
    return assessment


def _repair_nodepool_for_capacity(
    cluster: Cluster,
    *,
    initial_nodes_payload: dict,
    saturated_nodes: Sequence[str],
    affected: Sequence[dict],
    pod_template: dict,
    config_settings: CapacityRepairConfig,
    evidence: dict,
    deadline: float,
    request_timeout_seconds: int,
    command_attempts: int,
    command_retry_seconds: int,
) -> None:
    if not config_settings.subscription_id:
        raise RecoveryError(
            f"{cluster.role}: capacity repair requires subscription ID",
            evidence={"capacity": evidence},
        )
    if (
        config_settings.max_pool_count
        > MAX_CAPACITY_REPAIR_POOL_COUNT
    ):
        raise RecoveryError(
            f"{cluster.role}: capacity repair max pool count "
            f"{config_settings.max_pool_count} exceeds hard maximum "
            f"{MAX_CAPACITY_REPAIR_POOL_COUNT}",
            evidence={"capacity": evidence},
        )

    saturated_pools = {
        _node_pool_name(node)
        for node in _items(initial_nodes_payload, "node inventory")
        if str((node.get("metadata") or {}).get("name") or "")
        in saturated_nodes
    }
    if saturated_pools != {config_settings.pool_name}:
        raise RecoveryError(
            f"{cluster.role}: saturated nodes belong to pools "
            f"{sorted(saturated_pools)}, expected "
            f"{config_settings.pool_name!r}",
            evidence={"capacity": evidence},
        )

    pool_before = _with_capacity_evidence(
        lambda: _read_nodepool(
            cluster,
            config_settings,
            _remaining_timeout(deadline, request_timeout_seconds),
        ),
        evidence,
    )
    if pool_before.get("enableAutoScaling") is True:
        raise RecoveryError(
            f"{cluster.role}: refusing explicit capacity repair while "
            f"cluster autoscaler is enabled on {config_settings.pool_name}",
            evidence={"capacity": evidence},
        )
    evidence["pool_before"] = {
        "count": pool_before["count"],
        "provisioning_state": pool_before.get("provisioningState"),
        "power_state": (pool_before.get("powerState") or {}).get("code"),
    }
    if (
        pool_before.get("provisioningState") != "Succeeded"
        or (pool_before.get("powerState") or {}).get("code") != "Running"
    ):
        evidence["repair_action"] = "waited-for-existing-repair"
        pool_before = _with_capacity_evidence(
            lambda: _wait_nodepool_stable(
                cluster,
                config_settings,
                minimum_count=pool_before["count"],
                deadline=deadline,
                request_timeout_seconds=request_timeout_seconds,
            ),
            evidence,
        )
        existing_capacity = _with_capacity_evidence(
            lambda: _read_live_recovery_capacity(
                cluster,
                affected=affected,
                saturated_nodes=saturated_nodes,
                pod_template=pod_template,
                config_settings=config_settings,
                request_timeout_seconds=request_timeout_seconds,
                command_attempts=1,
                command_retry_seconds=0,
                deadline=deadline,
                require_daemonsets=True,
            ),
            evidence,
        )
        evidence["after_existing_repair"] = existing_capacity
        if existing_capacity["sufficient"]:
            evidence["pool_after"] = {
                "count": pool_before["count"],
                "provisioning_state": pool_before.get("provisioningState"),
                "power_state": (
                    pool_before.get("powerState") or {}
                ).get("code"),
            }
            return

    target_count = pool_before["count"]
    if target_count < config_settings.max_pool_count:
        target_count += 1
        evidence["repair_action"] = (
            "waited-then-scaled"
            if evidence["repair_action"] == "waited-for-existing-repair"
            else "scaled-up"
        )
        _with_capacity_evidence(
            lambda: _scale_nodepool_once(
                cluster,
                config_settings,
                target_count=target_count,
                request_timeout_seconds=request_timeout_seconds,
                command_attempts=command_attempts,
                command_retry_seconds=command_retry_seconds,
                deadline=deadline,
            ),
            evidence,
        )
    elif evidence["repair_action"] == "none":
        evidence["repair_action"] = "reused-existing-capacity"

    pool_after = _with_capacity_evidence(
        lambda: _wait_nodepool_stable(
            cluster,
            config_settings,
            minimum_count=target_count,
            deadline=deadline,
            request_timeout_seconds=request_timeout_seconds,
        ),
        evidence,
    )
    evidence["pool_after"] = {
        "count": pool_after["count"],
        "provisioning_state": pool_after.get("provisioningState"),
        "power_state": (pool_after.get("powerState") or {}).get("code"),
    }


def ensure_recovery_capacity(
    cluster: Cluster,
    *,
    affected: Sequence[dict],
    saturated_nodes: Sequence[str],
    pod_template: dict,
    initial_nodes_payload: dict,
    config_settings: CapacityRepairConfig,
    request_timeout_seconds: int,
    command_attempts: int,
    command_retry_seconds: int,
    deadline: Optional[float] = None,
    initial_evidence: Optional[dict] = None,
) -> dict:
    """Ensure enough alternate capacity, repairing one bounded pool if needed."""

    evidence = initial_evidence if initial_evidence is not None else {}
    evidence.update(
        {
            "repair_enabled": config_settings.enabled,
            "repair_action": "none",
            "pool": config_settings.pool_name,
            "stage": "initial-capacity-read",
        }
    )
    if config_settings.enabled and deadline is None:
        deadline = time.monotonic() + config_settings.timeout_seconds
    initial_timeout = request_timeout_seconds
    initial_attempts = command_attempts
    if deadline is not None:
        initial_timeout = _with_capacity_evidence(
            lambda: _remaining_timeout(
                deadline,
                request_timeout_seconds,
                process_grace_seconds=5,
            ),
            evidence,
        )
        initial_attempts = 1
    all_pods = _with_capacity_evidence(
        lambda: kubectl_json(
            cluster,
            ["get", "pods", "--all-namespaces", "-o", "json"],
            timeout_seconds=initial_timeout,
            attempts=initial_attempts,
            retry_seconds=command_retry_seconds,
        ),
        evidence,
    )
    before = _with_capacity_evidence(
        lambda: assess_recovery_capacity(
            nodes_payload=initial_nodes_payload,
            pods_payload=all_pods,
            affected=affected,
            saturated_nodes=saturated_nodes,
            pod_template=pod_template,
            config_settings=config_settings,
        ),
        evidence,
    )
    evidence.update(
        {
            "sufficient_before_repair": before["sufficient"],
            "before": before,
            "stage": "capacity-assessed",
        }
    )
    if before["sufficient"] and not config_settings.enabled:
        return {
            "alternate_nodes": before["alternate_nodes"],
            "evidence": evidence,
        }
    if not before["sufficient"] and not config_settings.enabled:
        raise RecoveryError(
            f"{cluster.role}: insufficient alternate recovery capacity; "
            f"unplaced={before['unplaced_agents']} "
            f"required_cpu_m={before['required']['cpu_millicores']}",
            evidence={"capacity": evidence},
        )
    if deadline is None:
        raise RecoveryError("capacity-repair deadline is missing")
    if not before["sufficient"]:
        _repair_nodepool_for_capacity(
            cluster,
            initial_nodes_payload=initial_nodes_payload,
            saturated_nodes=saturated_nodes,
            affected=affected,
            pod_template=pod_template,
            config_settings=config_settings,
            evidence=evidence,
            deadline=deadline,
            request_timeout_seconds=request_timeout_seconds,
            command_attempts=command_attempts,
            command_retry_seconds=command_retry_seconds,
        )

    after = None
    while time.monotonic() < deadline:
        after = _with_capacity_evidence(
            lambda: _read_live_recovery_capacity(
                cluster,
                affected=affected,
                saturated_nodes=saturated_nodes,
                pod_template=pod_template,
                config_settings=config_settings,
                request_timeout_seconds=request_timeout_seconds,
                command_attempts=1,
                command_retry_seconds=0,
                deadline=deadline,
                require_daemonsets=True,
            ),
            evidence,
        )
        if (
            after["sufficient"]
            and after["daemonsets"]["converged"]
        ):
            break
        sleep_seconds = min(
            config_settings.poll_seconds,
            max(0, int(deadline - time.monotonic())),
        )
        if sleep_seconds > 0:
            _sleep_with_capacity_evidence(sleep_seconds, evidence)
    if after is None:
        raise RecoveryError(
            f"{cluster.role}: capacity-repair deadline expired before the "
            "first convergence snapshot",
            evidence={"capacity": evidence},
        )
    if not after["sufficient"]:
        evidence["after"] = after
        raise RecoveryError(
            f"{cluster.role}: recovery capacity remained insufficient; "
            f"unplaced={after['unplaced_agents']}",
            evidence={"capacity": evidence},
        )
    if not after["daemonsets"]["converged"]:
        evidence["after"] = after
        raise RecoveryError(
            f"{cluster.role}: DaemonSets did not converge before the "
            "capacity-repair deadline",
            evidence={"capacity": evidence},
        )
    evidence["after"] = after
    cilium_rounds = []
    after_cilium = after
    while time.monotonic() < deadline:
        cilium_summary = _with_capacity_evidence(
            lambda: _run_cilium_health_after_repair(
                cluster,
                config_settings,
                deadline=deadline,
                required_node_names=after_cilium["alternate_nodes"],
            ),
            evidence,
        )
        cilium_rounds.append(cilium_summary)
        after_cilium = _with_capacity_evidence(
            lambda: _read_live_recovery_capacity(
                cluster,
                affected=affected,
                saturated_nodes=saturated_nodes,
                pod_template=pod_template,
                config_settings=config_settings,
                request_timeout_seconds=request_timeout_seconds,
                command_attempts=command_attempts,
                command_retry_seconds=command_retry_seconds,
                deadline=deadline,
                require_daemonsets=True,
            ),
            evidence,
        )
        evidence["after_cilium"] = after_cilium
        if not after_cilium["sufficient"]:
            raise RecoveryError(
                f"{cluster.role}: post-Cilium recovery capacity is "
                f"insufficient; unplaced={after_cilium['unplaced_agents']}",
                evidence={"capacity": evidence},
            )
        if not after_cilium["daemonsets"]["converged"]:
            after = after_cilium
            sleep_seconds = min(
                config_settings.poll_seconds,
                max(0, int(deadline - time.monotonic())),
            )
            if sleep_seconds > 0:
                _sleep_with_capacity_evidence(sleep_seconds, evidence)
            continue
        covered_nodes = set(cilium_summary["covered_node_names"])
        if set(after_cilium["alternate_nodes"]) <= covered_nodes:
            break
    else:
        raise RecoveryError(
            f"{cluster.role}: eligible recovery nodes did not stabilize "
            "before the capacity-repair deadline",
            evidence={"capacity": evidence},
        )
    evidence["post_repair_cilium"] = cilium_rounds[-1]
    evidence["post_repair_cilium_rounds"] = len(cilium_rounds)
    return {
        "alternate_nodes": after_cilium["alternate_nodes"],
        "evidence": evidence,
    }


def delete_pod_with_uid_precondition(
    cluster: Cluster,
    *,
    namespace: str,
    name: str,
    uid: str,
    timeout_seconds: int,
    attempts: int,
    retry_seconds: int,
) -> None:
    """Delete only the exact Pod UID, never a same-name replacement."""

    try:
        api_client = config.new_client_from_config(
            config_file=cluster.kubeconfig,
            context=cluster.context or None,
        )
    except (ConfigException, OSError) as error:
        raise RecoveryError(
            f"{cluster.role}: unable to load Kubernetes client: {error}"
        ) from error
    api = client.CoreV1Api(api_client)
    options = client.V1DeleteOptions(
        preconditions=client.V1Preconditions(uid=uid)
    )
    detail = ""
    try:
        for attempt in range(1, attempts + 1):
            try:
                api.delete_namespaced_pod(
                    name=name,
                    namespace=namespace,
                    body=options,
                    _request_timeout=(timeout_seconds, timeout_seconds),
                )
                return
            except ApiException as error:
                if error.status == 404:
                    return
                detail = (
                    f"status={error.status} "
                    f"{str(error.reason or error.body or error)[:1000]}"
                )
                if error.status in (400, 401, 403, 422):
                    break
            except (HTTPError, TimeoutError, OSError) as error:
                detail = str(error)[:1000]

            try:
                current = api.read_namespaced_pod(
                    name=name,
                    namespace=namespace,
                    _request_timeout=(timeout_seconds, timeout_seconds),
                )
            except ApiException as error:
                if error.status == 404:
                    return
                detail = (
                    f"{detail}; verification status={error.status} "
                    f"{str(error.reason or error.body or error)[:1000]}"
                )
            except (HTTPError, TimeoutError, OSError) as error:
                detail = f"{detail}; verification failed: {str(error)[:1000]}"
            else:
                current_uid = str(
                    getattr(getattr(current, "metadata", None), "uid", "") or ""
                )
                if current_uid and current_uid != uid:
                    return

            if attempt < attempts and retry_seconds > 0:
                time.sleep(retry_seconds)
    finally:
        api_client.close()
    raise RecoveryError(
        f"{cluster.role}: UID-preconditioned delete failed for {name} "
        f"after {attempts} attempt(s): {detail or 'unknown error'}"
    )


def _wait_recovered_agents_ready(
    cluster: Cluster,
    *,
    namespace: str,
    affected_names: Sequence[str],
    expected_uids: Dict[str, str],
    controller_uid: str,
    alternate_nodes: Sequence[str],
    timeout_seconds: int,
    poll_seconds: int,
    request_timeout_seconds: int,
    capacity_evidence: dict,
) -> Dict[str, dict]:
    deadline = time.monotonic() + timeout_seconds
    ready_pods = {}
    try:
        while time.monotonic() < deadline:
            current = _pod_map(
                kubectl_json(
                    cluster,
                    [
                        "-n",
                        namespace,
                        "get",
                        "pods",
                        "-l",
                        AGENT_SELECTOR,
                        "-o",
                        "json",
                    ],
                    timeout_seconds=request_timeout_seconds,
                    attempts=1,
                    retry_seconds=0,
                )
            )
            ready_pods = {
                name: current[name]
                for name in affected_names
                if name in current
                and str(
                    (current[name].get("metadata") or {}).get("uid") or ""
                )
                == expected_uids[name]
                and _pod_owned_by_controller_uid(
                    current[name],
                    controller_uid,
                )
                and (current[name].get("spec") or {}).get("nodeName")
                in alternate_nodes
                and _pod_ready(current[name])
            }
            if len(ready_pods) == len(affected_names):
                return ready_pods
            time.sleep(poll_seconds)
    except (RecoveryError, OSError) as error:
        raise _error_with_capacity_evidence(
            error,
            capacity_evidence,
        ) from error
    raise RecoveryError(
        f"{cluster.role}: rescheduled agents did not become Running/Ready "
        f"within {timeout_seconds}s",
        evidence={"capacity": capacity_evidence},
    )


def recover_cluster(
    cluster: Cluster,
    *,
    namespace: str,
    max_affected_pods: int,
    recovery_timeout_seconds: int,
    poll_seconds: int,
    request_timeout_seconds: int,
    command_attempts: int,
    command_retry_seconds: int,
    capacity_repair: Optional[CapacityRepairConfig] = None,
) -> dict:
    """Reschedule one cluster's CNI-blocked mock agents and restore nodes."""

    capacity_repair = capacity_repair or CapacityRepairConfig()
    pods_payload = kubectl_json(
        cluster,
        ["-n", namespace, "get", "pods", "-l", AGENT_SELECTOR, "-o", "json"],
        timeout_seconds=request_timeout_seconds,
        attempts=command_attempts,
        retry_seconds=command_retry_seconds,
    )
    events_payload = kubectl_json(
        cluster,
        ["-n", namespace, "get", "events", "-o", "json"],
        timeout_seconds=request_timeout_seconds,
        attempts=command_attempts,
        retry_seconds=command_retry_seconds,
    )
    affected = discover_cni_blocked_agents(pods_payload, events_payload)
    if not affected:
        return {
            "role": cluster.role,
            "status": "not-needed",
            "affected_agents": [],
            "cordoned_nodes": [],
            "rescheduled_agents": [],
        }
    if len(affected) > max_affected_pods:
        raise RecoveryError(
            f"{cluster.role}: {len(affected)} CNI-blocked agents exceed "
            f"the safety limit {max_affected_pods}"
        )
    controller_uid, controller_pod_spec = _controller_details(
        kubectl_json(
            cluster,
            [
                "-n",
                namespace,
                "get",
                "statefulset",
                AGENT_CONTROLLER_NAME,
                "-o",
                "json",
            ],
            timeout_seconds=request_timeout_seconds,
            attempts=command_attempts,
            retry_seconds=command_retry_seconds,
        )
    )
    if not all(
        _pod_owned_by_controller_uid(pod, controller_uid)
        for pod in affected
    ):
        raise RecoveryError(
            f"{cluster.role}: affected agents are not owned by the current "
            f"{AGENT_CONTROLLER_NAME} StatefulSet"
        )

    affected_names = sorted(
        str((pod.get("metadata") or {}).get("name")) for pod in affected
    )
    original_uids = {
        str((pod.get("metadata") or {}).get("name")): str(
            (pod.get("metadata") or {}).get("uid") or ""
        )
        for pod in affected
    }
    original_nodes = {
        str((pod.get("metadata") or {}).get("name")): str(
            (pod.get("spec") or {}).get("nodeName") or ""
        )
        for pod in affected
    }
    saturated_nodes = sorted(
        {
            str((pod.get("spec") or {}).get("nodeName"))
            for pod in affected
            if (pod.get("spec") or {}).get("nodeName")
        }
    )
    capacity_evidence_seed = {
        "repair_enabled": capacity_repair.enabled,
        "repair_action": "none",
        "pool": capacity_repair.pool_name,
        "stage": "initial-node-read",
    }
    capacity_deadline = None
    node_query_timeout = request_timeout_seconds
    node_query_attempts = command_attempts
    if capacity_repair.enabled:
        capacity_deadline = (
            time.monotonic() + capacity_repair.timeout_seconds
        )
        node_query_timeout = _with_capacity_evidence(
            lambda: _remaining_timeout(
                capacity_deadline,
                request_timeout_seconds,
                process_grace_seconds=5,
            ),
            capacity_evidence_seed,
        )
        node_query_attempts = 1
    nodes_payload = _with_capacity_evidence(
        lambda: kubectl_json(
            cluster,
            ["get", "nodes", "-o", "json"],
            timeout_seconds=node_query_timeout,
            attempts=node_query_attempts,
            retry_seconds=command_retry_seconds,
        ),
        capacity_evidence_seed,
    )
    real_nodes = _with_capacity_evidence(
        lambda: [
            node
            for node in _items(nodes_payload, "node inventory")
            if (
                ((node.get("metadata") or {}).get("labels") or {}).get(
                    "type"
                )
                != "kwok"
                and "kubernetes.azure.com/cluster"
                in ((node.get("metadata") or {}).get("labels") or {})
                and "prometheus"
                not in ((node.get("metadata") or {}).get("labels") or {})
            )
        ],
        capacity_evidence_seed,
    )
    by_name = {
        str((node.get("metadata") or {}).get("name")): node
        for node in real_nodes
    }
    if any(
        node_name not in by_name
        or not _node_ready_and_schedulable(by_name[node_name])
        or not _node_matches_pod_template(
            by_name[node_name],
            controller_pod_spec,
        )
        for node_name in saturated_nodes
    ):
        raise RecoveryError(
            f"{cluster.role}: saturated host nodes are not all Ready and "
            "schedulable",
            evidence={"capacity": capacity_evidence_seed},
        )
    capacity = ensure_recovery_capacity(
        cluster,
        affected=affected,
        saturated_nodes=saturated_nodes,
        pod_template=controller_pod_spec,
        initial_nodes_payload=nodes_payload,
        config_settings=capacity_repair,
        request_timeout_seconds=request_timeout_seconds,
        command_attempts=command_attempts,
        command_retry_seconds=command_retry_seconds,
        deadline=capacity_deadline,
        initial_evidence=capacity_evidence_seed,
    )
    alternate_nodes = capacity["alternate_nodes"]
    capacity_evidence = capacity["evidence"]

    cordoned_nodes = []
    cleanup_errors = []
    operation_error = None
    recovery_token = uuid.uuid4().hex
    try:
        for node_name in saturated_nodes:
            cordoned_nodes.append(node_name)
            kubectl(
                cluster,
                ["cordon", node_name],
                timeout_seconds=request_timeout_seconds,
                attempts=command_attempts,
                retry_seconds=command_retry_seconds,
            )

        for name in affected_names:
            patch = json.dumps(
                [
                    {
                        "op": "test",
                        "path": "/metadata/uid",
                        "value": original_uids[name],
                    },
                    {
                        "op": "add",
                        "path": (
                            "/metadata/labels/"
                            + RECOVERY_LABEL.replace("~", "~0").replace(
                                "/", "~1"
                            )
                        ),
                        "value": recovery_token,
                    },
                ]
            )
            kubectl(
                cluster,
                [
                    "-n",
                    namespace,
                    "patch",
                    "pod",
                    name,
                    "--type=json",
                    "-p",
                    patch,
                ],
                timeout_seconds=request_timeout_seconds,
                attempts=command_attempts,
                retry_seconds=command_retry_seconds,
            )

        for name in affected_names:
            delete_pod_with_uid_precondition(
                cluster,
                namespace=namespace,
                name=name,
                uid=original_uids[name],
                timeout_seconds=request_timeout_seconds,
                attempts=command_attempts,
                retry_seconds=command_retry_seconds,
            )

        deadline = time.monotonic() + recovery_timeout_seconds
        rescheduled = {}
        while time.monotonic() < deadline:
            current = _pod_map(
                kubectl_json(
                    cluster,
                    [
                        "-n",
                        namespace,
                        "get",
                        "pods",
                        "-l",
                        AGENT_SELECTOR,
                        "-o",
                        "json",
                    ],
                    timeout_seconds=request_timeout_seconds,
                    attempts=1,
                    retry_seconds=0,
                )
            )
            rescheduled = {
                name: current[name]
                for name in affected_names
                if name in current
                and not (current[name].get("metadata") or {}).get(
                    "deletionTimestamp"
                )
                and str(
                    (current[name].get("metadata") or {}).get("uid") or ""
                )
                not in ("", original_uids[name])
                and _pod_owned_by_controller_uid(
                    current[name],
                    controller_uid,
                )
                and (current[name].get("spec") or {}).get("nodeName")
                in alternate_nodes
            }
            if len(rescheduled) == len(affected_names):
                break
            time.sleep(poll_seconds)
        if len(rescheduled) != len(affected_names):
            raise RecoveryError(
                f"{cluster.role}: CNI-blocked agents did not reschedule away "
                f"from {saturated_nodes} within {recovery_timeout_seconds}s"
            )
    except (RecoveryError, OSError) as error:
        operation_error = _error_with_capacity_evidence(
            error,
            capacity_evidence,
        )
    finally:
        for node_name in reversed(cordoned_nodes):
            try:
                kubectl(
                    cluster,
                    ["uncordon", node_name],
                    timeout_seconds=request_timeout_seconds,
                    attempts=command_attempts,
                    retry_seconds=command_retry_seconds,
                )
            except RecoveryError as error:
                cleanup_errors.append(str(error))
        try:
            kubectl(
                cluster,
                [
                    "-n",
                    namespace,
                    "label",
                    "pods",
                    "-l",
                    f"{RECOVERY_LABEL}={recovery_token}",
                    f"{RECOVERY_LABEL}-",
                    "--overwrite",
                ],
                timeout_seconds=request_timeout_seconds,
                attempts=command_attempts,
                retry_seconds=command_retry_seconds,
            )
        except RecoveryError as error:
            cleanup_errors.append(str(error))
    if cleanup_errors:
        detail = "; ".join(cleanup_errors)
        if operation_error is not None:
            raise RecoveryError(
                f"{operation_error}; cleanup also failed: {detail}",
                evidence=operation_error.evidence,
            ) from operation_error
        raise RecoveryError(
            f"{cluster.role}: recovery cleanup failed: {detail}",
            evidence={"capacity": capacity_evidence},
        )
    if operation_error is not None:
        raise operation_error

    rescheduled_uids = {
        name: str(
            (rescheduled[name].get("metadata") or {}).get("uid") or ""
        )
        for name in affected_names
    }
    ready_pods = _wait_recovered_agents_ready(
        cluster,
        namespace=namespace,
        affected_names=affected_names,
        expected_uids=rescheduled_uids,
        controller_uid=controller_uid,
        alternate_nodes=alternate_nodes,
        timeout_seconds=recovery_timeout_seconds,
        poll_seconds=poll_seconds,
        request_timeout_seconds=request_timeout_seconds,
        capacity_evidence=capacity_evidence,
    )

    return {
        "role": cluster.role,
        "status": "recovered",
        "affected_agents": affected_names,
        "cordoned_nodes": saturated_nodes,
        "alternate_nodes": alternate_nodes,
        "capacity": capacity["evidence"],
        "rescheduled_agents": [
            {
                "name": name,
                "old_uid": original_uids[name],
                "old_node": original_nodes[name],
                "new_uid": str(
                    (ready_pods[name].get("metadata") or {}).get("uid") or ""
                ),
                "node": str((ready_pods[name].get("spec") or {}).get("nodeName")),
            }
            for name in affected_names
        ],
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clusters", required=True)
    parser.add_argument("--roles", default="")
    parser.add_argument("--summary-file", required=True)
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--max-affected-clusters", type=int, default=5)
    parser.add_argument("--max-affected-pods", type=int, default=25)
    parser.add_argument("--recovery-timeout-seconds", type=int, default=300)
    parser.add_argument("--poll-seconds", type=int, default=5)
    parser.add_argument("--request-timeout-seconds", type=int, default=30)
    parser.add_argument("--command-attempts", type=int, default=3)
    parser.add_argument("--command-retry-seconds", type=int, default=5)
    parser.add_argument("--capacity-repair-enabled", action="store_true")
    parser.add_argument("--subscription-id", default="")
    parser.add_argument("--capacity-repair-pool", default="default")
    parser.add_argument("--capacity-repair-max-pool-count", type=int, default=3)
    parser.add_argument("--capacity-repair-timeout-seconds", type=int, default=1800)
    parser.add_argument("--capacity-repair-poll-seconds", type=int, default=15)
    parser.add_argument("--capacity-cpu-reserve-millicores", type=int, default=250)
    parser.add_argument("--capacity-memory-reserve-mib", type=int, default=512)
    parser.add_argument("--capacity-pod-reserve", type=int, default=5)
    parser.add_argument("--cilium-health-script", default="")
    parser.add_argument("--cilium-identity-inventory", default="")
    parser.add_argument("--expected-cilium-remote-count", type=int, default=99)
    args = parser.parse_args(argv)
    for name in (
        "max_affected_clusters",
        "max_affected_pods",
        "recovery_timeout_seconds",
        "request_timeout_seconds",
        "command_attempts",
        "capacity_repair_max_pool_count",
        "capacity_repair_timeout_seconds",
    ):
        if getattr(args, name) < 1:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    for name in (
        "poll_seconds",
        "command_retry_seconds",
        "capacity_repair_poll_seconds",
        "capacity_cpu_reserve_millicores",
        "capacity_memory_reserve_mib",
        "capacity_pod_reserve",
        "expected_cilium_remote_count",
    ):
        if getattr(args, name) < 0:
            parser.error(f"--{name.replace('_', '-')} must be non-negative")
    if args.capacity_repair_enabled:
        if (
            args.capacity_repair_max_pool_count
            > MAX_CAPACITY_REPAIR_POOL_COUNT
        ):
            parser.error(
                "--capacity-repair-max-pool-count cannot exceed "
                f"{MAX_CAPACITY_REPAIR_POOL_COUNT}"
            )
        for name in (
            "subscription_id",
            "capacity_repair_pool",
            "cilium_health_script",
            "cilium_identity_inventory",
        ):
            if not getattr(args, name):
                parser.error(
                    f"--{name.replace('_', '-')} is required when "
                    "--capacity-repair-enabled is set"
                )
    return args


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Recover explicitly selected roles and write authoritative evidence."""

    args = parse_args(argv)
    roles = sorted(
        {
            role.strip()
            for role in args.roles.split(",")
            if role.strip()
        }
    )
    summary = {
        "schema_version": 1,
        "started_at": utc_now(),
        "finished_at": None,
        "success": False,
        "requested_roles": roles,
        "results": [],
    }
    write_json_atomic(args.summary_file, summary)
    if not roles:
        summary.update(
            {
                "finished_at": utc_now(),
                "success": True,
                "status": "disabled-no-roles",
            }
        )
        write_json_atomic(args.summary_file, summary)
        return 0
    if len(roles) > args.max_affected_clusters:
        summary.update(
            {
                "finished_at": utc_now(),
                "fatal_error": (
                    f"requested role count {len(roles)} exceeds safety limit "
                    f"{args.max_affected_clusters}"
                ),
            }
        )
        write_json_atomic(args.summary_file, summary)
        return 1

    previous_handlers = {}
    capacity_repair = CapacityRepairConfig(
        enabled=args.capacity_repair_enabled,
        subscription_id=args.subscription_id,
        pool_name=args.capacity_repair_pool,
        max_pool_count=args.capacity_repair_max_pool_count,
        timeout_seconds=args.capacity_repair_timeout_seconds,
        poll_seconds=args.capacity_repair_poll_seconds,
        cpu_reserve_millicores=args.capacity_cpu_reserve_millicores,
        memory_reserve_mib=args.capacity_memory_reserve_mib,
        pod_reserve=args.capacity_pod_reserve,
        cilium_health_script=args.cilium_health_script,
        cilium_identity_inventory=args.cilium_identity_inventory,
        expected_remote_count=args.expected_cilium_remote_count,
    )

    def _interrupt(signum, _frame):
        raise RecoveryInterrupted(f"received signal {signum}")

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[signum] = signal.signal(signum, _interrupt)
    try:
        clusters = load_clusters(args.clusters, roles)
        for cluster in clusters:
            try:
                result = recover_cluster(
                    cluster,
                    namespace=args.namespace,
                    max_affected_pods=args.max_affected_pods,
                    recovery_timeout_seconds=args.recovery_timeout_seconds,
                    poll_seconds=args.poll_seconds,
                    request_timeout_seconds=args.request_timeout_seconds,
                    command_attempts=args.command_attempts,
                    command_retry_seconds=args.command_retry_seconds,
                    capacity_repair=capacity_repair,
                )
            except RecoveryError as error:
                failure = {
                    "role": cluster.role,
                    "status": "failed",
                    "fatal_error": str(error),
                }
                failure.update(error.evidence)
                summary["results"].append(failure)
                raise
            summary["results"].append(result)
        summary.update(
            {
                "finished_at": utc_now(),
                "success": True,
                "status": "complete",
            }
        )
        write_json_atomic(args.summary_file, summary)
        return 0
    except (RecoveryError, OSError) as error:
        summary.update(
            {
                "finished_at": utc_now(),
                "success": False,
                "status": "failed",
                "fatal_error": str(error),
            }
        )
        write_json_atomic(args.summary_file, summary)
        print(f"mock CNI recovery failed: {error}", file=sys.stderr)
        return 1
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    sys.exit(main())
