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
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
from kubernetes.config.config_exception import ConfigException
from urllib3.exceptions import HTTPError


AGENT_SELECTOR = "app=mock-cilium-agent"
AGENT_CONTROLLER_LABEL = "mock-clustermesh/agent-controller"
AGENT_CONTROLLER_NAME = "kwok-node"
RECOVERY_LABEL = "mock-clustermesh/cni-recovery"
DEFAULT_NAMESPACE = "mock-clustermesh"
CNI_ERROR_MARKERS = (
    "allocateipconfig failed",
    "not enough ips available",
)


class RecoveryError(Exception):
    """A bounded, expected CNI recovery failure."""


class RecoveryInterrupted(RecoveryError):
    """Recovery was interrupted and must still restore node schedulability."""


@dataclass(frozen=True)
class Cluster:
    """One exact cluster selected from the handoff inventory."""

    role: str
    kubeconfig: str
    context: str


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
        kubeconfig = row.get("kubeconfig")
        if not isinstance(role, str) or not isinstance(kubeconfig, str):
            raise RecoveryError("cluster inventory row is missing role or kubeconfig")
        if role in by_role:
            raise RecoveryError(f"cluster inventory contains duplicate role {role}")
        by_role[role] = Cluster(
            role=role,
            kubeconfig=kubeconfig,
            context=str(row.get("context") or row.get("name") or ""),
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
) -> dict:
    """Reschedule one cluster's CNI-blocked mock agents and restore nodes."""

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
    nodes_payload = kubectl_json(
        cluster,
        ["get", "nodes", "-o", "json"],
        timeout_seconds=request_timeout_seconds,
        attempts=command_attempts,
        retry_seconds=command_retry_seconds,
    )
    real_nodes = [
        node
        for node in _items(nodes_payload, "node inventory")
        if (
            ((node.get("metadata") or {}).get("labels") or {}).get("type")
            != "kwok"
            and "kubernetes.azure.com/cluster"
            in ((node.get("metadata") or {}).get("labels") or {})
            and "prometheus"
            not in ((node.get("metadata") or {}).get("labels") or {})
        )
    ]
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
            f"{cluster.role}: saturated host nodes are not all Ready and schedulable"
        )
    alternate_nodes = sorted(
        name
        for name, node in by_name.items()
        if name not in saturated_nodes
        and _node_ready_and_schedulable(node)
        and _node_matches_pod_template(node, controller_pod_spec)
    )
    if not alternate_nodes:
        raise RecoveryError(
            f"{cluster.role}: no alternate Ready schedulable real node exists"
        )

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
        operation_error = error
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
                f"{operation_error}; cleanup also failed: {detail}"
            ) from operation_error
        raise RecoveryError(
            f"{cluster.role}: recovery cleanup failed: {detail}"
        )
    if operation_error is not None:
        raise operation_error

    rescheduled_uids = {
        name: str(
            (rescheduled[name].get("metadata") or {}).get("uid") or ""
        )
        for name in affected_names
    }
    deadline = time.monotonic() + recovery_timeout_seconds
    ready_pods = {}
    while time.monotonic() < deadline:
        current = _pod_map(
            kubectl_json(
                cluster,
                ["-n", namespace, "get", "pods", "-l", AGENT_SELECTOR, "-o", "json"],
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
            == rescheduled_uids[name]
            and _pod_owned_by_controller_uid(
                current[name],
                controller_uid,
            )
            and (current[name].get("spec") or {}).get("nodeName")
            in alternate_nodes
            and _pod_ready(current[name])
        }
        if len(ready_pods) == len(affected_names):
            break
        time.sleep(poll_seconds)
    if len(ready_pods) != len(affected_names):
        raise RecoveryError(
            f"{cluster.role}: rescheduled agents did not become Running/Ready "
            f"within {recovery_timeout_seconds}s"
        )

    return {
        "role": cluster.role,
        "status": "recovered",
        "affected_agents": affected_names,
        "cordoned_nodes": saturated_nodes,
        "alternate_nodes": alternate_nodes,
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
    args = parser.parse_args(argv)
    for name in (
        "max_affected_clusters",
        "max_affected_pods",
        "recovery_timeout_seconds",
        "request_timeout_seconds",
        "command_attempts",
    ):
        if getattr(args, name) < 1:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    for name in ("poll_seconds", "command_retry_seconds"):
        if getattr(args, name) < 0:
            parser.error(f"--{name.replace('_', '-')} must be non-negative")
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

    def _interrupt(signum, _frame):
        raise RecoveryInterrupted(f"received signal {signum}")

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[signum] = signal.signal(signum, _interrupt)
    try:
        clusters = load_clusters(args.clusters, roles)
        for cluster in clusters:
            summary["results"].append(
                recover_cluster(
                    cluster,
                    namespace=args.namespace,
                    max_affected_pods=args.max_affected_pods,
                    recovery_timeout_seconds=args.recovery_timeout_seconds,
                    poll_seconds=args.poll_seconds,
                    request_timeout_seconds=args.request_timeout_seconds,
                    command_attempts=args.command_attempts,
                    command_retry_seconds=args.command_retry_seconds,
                )
            )
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
