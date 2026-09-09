"""Tests for targeted mock-agent Azure CNI recovery."""

# pylint: disable=protected-access,too-many-lines

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from kubernetes.client.exceptions import ApiException


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "clusterloader2"
    / "clustermesh-scale"
    / "mock_cni_recovery.py"
)
MODULE_SPEC = importlib.util.spec_from_file_location(
    "mock_cni_recovery",
    MODULE_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise ImportError(f"Unable to load from {MODULE_PATH}")
recovery = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = recovery
MODULE_SPEC.loader.exec_module(recovery)


def _pending_pod(node="node-a"):
    return {
        "metadata": {
            "name": "kwok-node-19",
            "uid": "old-uid",
            "labels": {
                "app": "mock-cilium-agent",
                "mock-clustermesh/agent-controller": "kwok-node",
            },
            "ownerReferences": [
                {
                    "kind": "StatefulSet",
                    "name": "kwok-node",
                    "uid": "controller-uid",
                    "controller": True,
                }
            ],
        },
        "spec": {
            "nodeName": node,
            "containers": [
                {
                    "name": "mock-cilium-agent",
                    "resources": {
                        "requests": {"cpu": "100m", "memory": "256Mi"}
                    },
                }
            ],
        },
        "status": {
            "phase": "Pending",
            "containerStatuses": [
                {
                    "ready": False,
                    "state": {"waiting": {"reason": "ContainerCreating"}},
                }
            ],
        },
    }


def _ready_pod(node="node-b", uid="new-uid"):
    return {
        "metadata": {
            "name": "kwok-node-19",
            "uid": uid,
            "labels": {
                "app": "mock-cilium-agent",
                "mock-clustermesh/agent-controller": "kwok-node",
            },
            "ownerReferences": [
                {
                    "kind": "StatefulSet",
                    "name": "kwok-node",
                    "uid": "controller-uid",
                    "controller": True,
                }
            ],
        },
        "spec": {
            "nodeName": node,
            "containers": [
                {
                    "name": "mock-cilium-agent",
                    "resources": {
                        "requests": {"cpu": "100m", "memory": "256Mi"}
                    },
                }
            ],
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [{"ready": True}],
        },
    }


def _event(message=None):
    return {
        "involvedObject": {
            "kind": "Pod",
            "name": "kwok-node-19",
            "uid": "old-uid",
        },
        "reason": "FailedCreatePodSandBox",
        "message": message
        or (
            "cilium-cni failed: AllocateIPConfig failed: not enough IPs "
            "available of type ipv4"
        ),
    }


def _nodes(*, node_b_cpu="7820m", include_node_c=False):
    items = [
        {
            "metadata": {
                "name": "node-a",
                "labels": {
                    "kubernetes.azure.com/cluster": "cluster",
                    "kubernetes.azure.com/agentpool": "default",
                },
            },
            "spec": {"unschedulable": False},
            "status": {
                "allocatable": {
                    "cpu": "7820m",
                    "memory": "30Gi",
                    "pods": "110",
                },
                "conditions": [{"type": "Ready", "status": "True"}],
            },
        },
        {
            "metadata": {
                "name": "node-b",
                "labels": {
                    "kubernetes.azure.com/cluster": "cluster",
                    "kubernetes.azure.com/agentpool": "default",
                },
            },
            "spec": {"unschedulable": False},
            "status": {
                "allocatable": {
                    "cpu": node_b_cpu,
                    "memory": "30Gi",
                    "pods": "110",
                },
                "conditions": [{"type": "Ready", "status": "True"}],
            },
        },
        {
            "metadata": {
                "name": "kwok-node-19",
                "labels": {"type": "kwok"},
            },
            "spec": {},
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}]
            },
        },
    ]
    if include_node_c:
        items.append(
            {
                "metadata": {
                    "name": "node-c",
                    "labels": {
                        "kubernetes.azure.com/cluster": "cluster",
                        "kubernetes.azure.com/agentpool": "default",
                    },
                },
                "spec": {"unschedulable": False},
                "status": {
                    "allocatable": {
                        "cpu": "7820m",
                        "memory": "30Gi",
                        "pods": "110",
                    },
                    "conditions": [{"type": "Ready", "status": "True"}],
                },
            }
        )
    return {"items": items}


def _statefulset():
    return {
        "metadata": {"uid": "controller-uid"},
        "spec": {
            "template": {
                "spec": {
                    "affinity": {
                        "nodeAffinity": {
                            "requiredDuringSchedulingIgnoredDuringExecution": {
                                "nodeSelectorTerms": [
                                    {
                                        "matchExpressions": [
                                            {
                                                "key": (
                                                    "kubernetes.azure.com/"
                                                    "cluster"
                                                ),
                                                "operator": "Exists",
                                            },
                                            {
                                                "key": "prometheus",
                                                "operator": "DoesNotExist",
                                            },
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        },
    }


def _daemonsets(*, converged=True):
    desired = 3
    ready = desired if converged else desired - 1
    return {
        "items": [
            {
                "metadata": {
                    "name": "cilium",
                    "namespace": "kube-system",
                    "generation": 2,
                },
                "status": {
                    "observedGeneration": 2,
                    "desiredNumberScheduled": desired,
                    "currentNumberScheduled": desired,
                    "numberReady": ready,
                    "updatedNumberScheduled": desired,
                    "numberUnavailable": desired - ready,
                },
            }
        ]
    }


class FakeKubectl:
    """Stateful kubectl fake for one selected cluster."""

    def __init__(self, *, reschedules=True):
        self.reschedules = reschedules
        self.deleted = False
        self.commands = []

    def __call__(self, command, _timeout_seconds):
        command = list(command)
        self.commands.append(command)
        if "get" in command and "events" in command:
            payload = {"items": [_event()]}
        elif "get" in command and "nodes" in command:
            payload = _nodes()
        elif "get" in command and "daemonsets" in command:
            payload = _daemonsets()
        elif "get" in command and "pods" in command:
            pod = (
                _ready_pod()
                if self.deleted and self.reschedules
                else _pending_pod()
            )
            payload = {"items": [pod]}
        elif "get" in command and "statefulset" in command:
            payload = _statefulset()
        elif "cordon" in command:
            return subprocess.CompletedProcess(command, 0, "", "")
        elif "uncordon" in command:
            return subprocess.CompletedProcess(command, 0, "", "")
        elif "patch" in command and "pod" in command:
            return subprocess.CompletedProcess(command, 0, "", "")
        elif "label" in command and "pods" in command:
            return subprocess.CompletedProcess(command, 0, "", "")
        else:
            raise AssertionError(f"unexpected command: {command}")
        return subprocess.CompletedProcess(
            command,
            0,
            json.dumps(payload),
            "",
        )

    def delete_pod(
        self,
        _cluster,
        *,
        namespace,
        name,
        uid,
        timeout_seconds,
        attempts,
        retry_seconds,
    ):
        del namespace, timeout_seconds, attempts, retry_seconds
        self.commands.append(["uid-delete", name, uid])
        self.deleted = True


def _install_fake(monkeypatch, fake):
    monkeypatch.setattr(recovery, "run_command", fake)
    monkeypatch.setattr(
        recovery,
        "delete_pod_with_uid_precondition",
        fake.delete_pod,
    )


def _cluster():
    return recovery.Cluster(
        role="mesh-100",
        kubeconfig="/tmp/mesh-100.config",
        context="clustermesh-100",
        name="clustermesh-100",
        resource_group="78751-f36f3d5a",
    )


def test_cni_recovery_reschedules_and_restores_saturated_node(monkeypatch):
    fake = FakeKubectl()
    _install_fake(monkeypatch, fake)

    result = recovery.recover_cluster(
        _cluster(),
        namespace="mock-clustermesh",
        max_affected_pods=25,
        recovery_timeout_seconds=5,
        poll_seconds=0,
        request_timeout_seconds=5,
        command_attempts=1,
        command_retry_seconds=0,
    )

    assert result["status"] == "recovered"
    assert result["affected_agents"] == ["kwok-node-19"]
    assert result["cordoned_nodes"] == ["node-a"]
    assert result["capacity"]["sufficient_before_repair"] is True
    assert result["capacity"]["repair_action"] == "none"
    assert result["rescheduled_agents"] == [
        {
            "name": "kwok-node-19",
            "old_uid": "old-uid",
            "old_node": "node-a",
            "new_uid": "new-uid",
            "node": "node-b",
        }
    ]
    actions = [
        next(
            token
            for token in ("cordon", "uid-delete", "uncordon")
            if token in command
        )
        for command in fake.commands
        if any(
            token in command
            for token in ("cordon", "uid-delete", "uncordon")
        )
    ]
    assert actions == ["cordon", "uid-delete", "uncordon"]


def test_enabled_capacity_fast_path_still_proves_daemonsets_and_cilium(
    monkeypatch,
):
    fake = FakeKubectl()
    _install_fake(monkeypatch, fake)
    cilium_required_nodes = []

    def healthy_cilium(
        _cluster_value,
        _settings,
        *,
        deadline,
        required_node_names,
    ):
        del deadline
        cilium_required_nodes.append(list(required_node_names))
        return {
            "healthy": True,
            "covered_node_names": list(required_node_names),
        }

    monkeypatch.setattr(
        recovery,
        "_run_cilium_health_after_repair",
        healthy_cilium,
    )

    result = recovery.recover_cluster(
        _cluster(),
        namespace="mock-clustermesh",
        max_affected_pods=25,
        recovery_timeout_seconds=5,
        poll_seconds=0,
        request_timeout_seconds=5,
        command_attempts=1,
        command_retry_seconds=0,
        capacity_repair=_capacity_repair_config(),
    )

    assert result["status"] == "recovered"
    assert result["capacity"]["repair_action"] == "none"
    assert result["capacity"]["after"]["daemonsets"]["converged"] is True
    assert cilium_required_nodes == [["node-b"]]
    assert any(
        "daemonsets" in command
        for command in fake.commands
    )
    assert not any(
        command[:4] == ["az", "aks", "nodepool", "scale"]
        for command in fake.commands
    )


def test_cni_recovery_uncordons_when_reschedule_times_out(monkeypatch):
    fake = FakeKubectl(reschedules=False)
    _install_fake(monkeypatch, fake)
    ticks = iter(range(20))
    monkeypatch.setattr(recovery.time, "monotonic", lambda: next(ticks))

    with pytest.raises(
        recovery.RecoveryError,
        match="did not reschedule away",
    ):
        recovery.recover_cluster(
            _cluster(),
            namespace="mock-clustermesh",
            max_affected_pods=25,
            recovery_timeout_seconds=2,
            poll_seconds=0,
            request_timeout_seconds=5,
            command_attempts=1,
            command_retry_seconds=0,
        )

    actions = [
        token
        for command in fake.commands
        for token in ("cordon", "uncordon")
        if token in command
    ]
    assert actions == ["cordon", "uncordon"]


def test_cni_recovery_requires_matching_ip_exhaustion_event():
    affected = recovery.discover_cni_blocked_agents(
        {"items": [_pending_pod()]},
        {"items": [_event("some other sandbox failure")]},
    )

    assert affected == []


def test_cni_recovery_requires_event_for_exact_current_pod_uid():
    event = _event()
    event["involvedObject"]["uid"] = "stale-uid"

    affected = recovery.discover_cni_blocked_agents(
        {"items": [_pending_pod()]},
        {"items": [event]},
    )

    assert affected == []


def test_cni_recovery_requires_current_statefulset_ownership():
    pod = _pending_pod()
    pod["metadata"]["ownerReferences"] = []

    affected = recovery.discover_cni_blocked_agents(
        {"items": [pod]},
        {"items": [_event()]},
    )

    assert affected == []


def test_cni_recovery_no_matching_event_performs_no_mutation(monkeypatch):
    commands = []

    def fake(command, _timeout_seconds):
        command = list(command)
        commands.append(command)
        if "get" in command and "pods" in command:
            payload = {"items": [_pending_pod()]}
        elif "get" in command and "events" in command:
            payload = {"items": [_event("some other sandbox failure")]}
        else:
            raise AssertionError(f"unexpected mutation: {command}")
        return subprocess.CompletedProcess(command, 0, json.dumps(payload), "")

    monkeypatch.setattr(recovery, "run_command", fake)

    result = recovery.recover_cluster(
        _cluster(),
        namespace="mock-clustermesh",
        max_affected_pods=25,
        recovery_timeout_seconds=5,
        poll_seconds=0,
        request_timeout_seconds=5,
        command_attempts=1,
        command_retry_seconds=0,
    )

    assert result["status"] == "not-needed"
    assert not any(
        token in command
        for command in commands
        for token in ("cordon", "uid-delete", "uncordon")
    )


def test_cordon_failure_still_attempts_uncordon(monkeypatch):
    fake = FakeKubectl()

    def ambiguous_cordon(command, timeout_seconds):
        if "cordon" in command:
            fake.commands.append(list(command))
            return subprocess.CompletedProcess(command, 1, "", "timeout")
        return fake(command, timeout_seconds)

    monkeypatch.setattr(recovery, "run_command", ambiguous_cordon)
    monkeypatch.setattr(
        recovery,
        "delete_pod_with_uid_precondition",
        fake.delete_pod,
    )

    with pytest.raises(
        recovery.RecoveryError,
        match="cordon node-a failed",
    ) as raised:
        recovery.recover_cluster(
            _cluster(),
            namespace="mock-clustermesh",
            max_affected_pods=25,
            recovery_timeout_seconds=5,
            poll_seconds=0,
            request_timeout_seconds=5,
            command_attempts=1,
            command_retry_seconds=0,
        )

    actions = [
        token
        for command in fake.commands
        for token in ("cordon", "uncordon")
        if token in command
    ]
    assert actions == ["cordon", "uncordon"]
    assert raised.value.evidence["capacity"][
        "sufficient_before_repair"
    ] is True


def test_uid_precondition_failure_does_not_delete_replacement(monkeypatch):
    fake = FakeKubectl()

    def replacement_race(command, timeout_seconds):
        if "patch" in command and "pod" in command:
            fake.commands.append(list(command))
            return subprocess.CompletedProcess(
                command,
                1,
                "",
                "the UID test operation failed",
            )
        return fake(command, timeout_seconds)

    monkeypatch.setattr(recovery, "run_command", replacement_race)
    monkeypatch.setattr(
        recovery,
        "delete_pod_with_uid_precondition",
        fake.delete_pod,
    )

    with pytest.raises(recovery.RecoveryError, match="patch pod"):
        recovery.recover_cluster(
            _cluster(),
            namespace="mock-clustermesh",
            max_affected_pods=25,
            recovery_timeout_seconds=5,
            poll_seconds=0,
            request_timeout_seconds=5,
            command_attempts=1,
            command_retry_seconds=0,
        )

    assert not any("uid-delete" in command for command in fake.commands)
    assert any("uncordon" in command for command in fake.commands)


def test_cni_recovery_fails_closed_before_mutation_above_pod_limit(
    monkeypatch,
):
    second_pod = _pending_pod()
    second_pod["metadata"]["name"] = "kwok-node-20"
    second_pod["metadata"]["uid"] = "old-uid-20"
    second_event = _event()
    second_event["involvedObject"] = {
        "kind": "Pod",
        "name": "kwok-node-20",
        "uid": "old-uid-20",
    }
    commands = []

    def fake(command, _timeout_seconds):
        command = list(command)
        commands.append(command)
        if "get" in command and "pods" in command:
            payload = {"items": [_pending_pod(), second_pod]}
        elif "get" in command and "events" in command:
            payload = {"items": [_event(), second_event]}
        else:
            raise AssertionError(f"unexpected mutation: {command}")
        return subprocess.CompletedProcess(command, 0, json.dumps(payload), "")

    monkeypatch.setattr(recovery, "run_command", fake)

    with pytest.raises(recovery.RecoveryError, match="exceed the safety limit"):
        recovery.recover_cluster(
            _cluster(),
            namespace="mock-clustermesh",
            max_affected_pods=1,
            recovery_timeout_seconds=5,
            poll_seconds=0,
            request_timeout_seconds=5,
            command_attempts=1,
            command_retry_seconds=0,
        )

    assert not any(
        token in command
        for command in commands
        for token in ("cordon", "uid-delete", "uncordon")
    )


def test_empty_role_selection_is_a_noop(tmp_path):
    summary_path = tmp_path / "summary.json"

    result = recovery.main(
        [
            "--clusters",
            str(tmp_path / "missing-clusters.json"),
            "--roles",
            "",
            "--summary-file",
            str(summary_path),
        ]
    )

    assert result == 0
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["success"] is True
    assert summary["status"] == "disabled-no-roles"
    assert summary["requested_roles"] == []


def test_cluster_inventory_defaults_to_standard_role_kubeconfig(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("HOME", str(tmp_path))
    inventory = tmp_path / "clusters.json"
    inventory.write_text(
        json.dumps(
            [
                {
                    "role": "mesh-100",
                    "name": "clustermesh-100",
                    "rg": "78751-f36f3d5a",
                }
            ]
        ),
        encoding="utf-8",
    )

    clusters = recovery.load_clusters(str(inventory), ["mesh-100"])

    assert clusters == [
        recovery.Cluster(
            role="mesh-100",
            kubeconfig=str(tmp_path / ".kube" / "mesh-100.config"),
            context="clustermesh-100",
            name="clustermesh-100",
            resource_group="78751-f36f3d5a",
        )
    ]


def test_delete_uses_server_side_uid_precondition(monkeypatch):
    class FakeApiClient:
        closed = False

        def close(self):
            self.closed = True

    class FakeApi:
        body = None

        def delete_namespaced_pod(self, **kwargs):
            self.body = kwargs["body"]

    api_client = FakeApiClient()
    api = FakeApi()
    monkeypatch.setattr(
        recovery.config,
        "new_client_from_config",
        lambda **_kwargs: api_client,
    )
    monkeypatch.setattr(
        recovery.client,
        "CoreV1Api",
        lambda _client: api,
    )

    recovery.delete_pod_with_uid_precondition(
        _cluster(),
        namespace="mock-clustermesh",
        name="kwok-node-19",
        uid="old-uid",
        timeout_seconds=5,
        attempts=1,
        retry_seconds=0,
    )

    assert api.body.preconditions.uid == "old-uid"
    assert api_client.closed is True


def test_delete_treats_same_name_new_uid_as_old_pod_gone(monkeypatch):
    class FakeApiClient:
        def close(self):
            return None

    class FakeApi:
        delete_calls = 0

        def delete_namespaced_pod(self, **_kwargs):
            self.delete_calls += 1
            raise ApiException(status=409, reason="UID precondition failed")

        def read_namespaced_pod(self, **_kwargs):
            return SimpleNamespace(
                metadata=SimpleNamespace(uid="replacement-uid")
            )

    api = FakeApi()
    monkeypatch.setattr(
        recovery.config,
        "new_client_from_config",
        lambda **_kwargs: FakeApiClient(),
    )
    monkeypatch.setattr(
        recovery.client,
        "CoreV1Api",
        lambda _client: api,
    )

    recovery.delete_pod_with_uid_precondition(
        _cluster(),
        namespace="mock-clustermesh",
        name="kwok-node-19",
        uid="old-uid",
        timeout_seconds=5,
        attempts=3,
        retry_seconds=0,
    )

    assert api.delete_calls == 1


def test_final_readiness_rejects_later_same_name_replacement(monkeypatch):
    class ChurningKubectl(FakeKubectl):
        post_delete_reads = 0

        def __call__(self, command, timeout_seconds):
            if (
                self.deleted
                and "get" in command
                and "pods" in command
            ):
                self.commands.append(list(command))
                self.post_delete_reads += 1
                pod = (
                    _ready_pod()
                    if self.post_delete_reads == 1
                    else _ready_pod(node="node-a", uid="later-uid")
                )
                return subprocess.CompletedProcess(
                    command,
                    0,
                    json.dumps({"items": [pod]}),
                    "",
                )
            return super().__call__(command, timeout_seconds)

    fake = ChurningKubectl()
    _install_fake(monkeypatch, fake)
    ticks = iter(range(20))
    monkeypatch.setattr(recovery.time, "monotonic", lambda: next(ticks))

    with pytest.raises(
        recovery.RecoveryError,
        match="did not become Running/Ready",
    ):
        recovery.recover_cluster(
            _cluster(),
            namespace="mock-clustermesh",
            max_affected_pods=25,
            recovery_timeout_seconds=2,
            poll_seconds=0,
            request_timeout_seconds=5,
            command_attempts=1,
            command_retry_seconds=0,
        )


def test_recovery_rejects_alternate_with_untolerated_taint(monkeypatch):
    class TaintedAlternateKubectl(FakeKubectl):
        def __call__(self, command, timeout_seconds):
            if "get" in command and "nodes" in command:
                self.commands.append(list(command))
                nodes = _nodes()
                nodes["items"][1]["spec"]["taints"] = [
                    {
                        "key": "dedicated",
                        "value": "other",
                        "effect": "NoSchedule",
                    }
                ]
                return subprocess.CompletedProcess(
                    command,
                    0,
                    json.dumps(nodes),
                    "",
                )
            return super().__call__(command, timeout_seconds)

    fake = TaintedAlternateKubectl()
    _install_fake(monkeypatch, fake)

    with pytest.raises(
        recovery.RecoveryError,
        match="insufficient alternate recovery capacity",
    ):
        recovery.recover_cluster(
            _cluster(),
            namespace="mock-clustermesh",
            max_affected_pods=25,
            recovery_timeout_seconds=5,
            poll_seconds=0,
            request_timeout_seconds=5,
            command_attempts=1,
            command_retry_seconds=0,
        )

    assert not any("cordon" in command for command in fake.commands)


def test_insufficient_resource_capacity_fails_before_cordon(monkeypatch):
    class CpuConstrainedKubectl(FakeKubectl):
        def __call__(self, command, timeout_seconds):
            if "get" in command and "nodes" in command:
                self.commands.append(list(command))
                return subprocess.CompletedProcess(
                    command,
                    0,
                    json.dumps(_nodes(node_b_cpu="300m")),
                    "",
                )
            return super().__call__(command, timeout_seconds)

    fake = CpuConstrainedKubectl()
    _install_fake(monkeypatch, fake)

    with pytest.raises(
        recovery.RecoveryError,
        match="insufficient alternate recovery capacity",
    ) as raised:
        recovery.recover_cluster(
            _cluster(),
            namespace="mock-clustermesh",
            max_affected_pods=25,
            recovery_timeout_seconds=5,
            poll_seconds=0,
            request_timeout_seconds=5,
            command_attempts=1,
            command_retry_seconds=0,
        )

    assert raised.value.evidence["capacity"]["before"]["unplaced_agents"] == [
        "kwok-node-19"
    ]
    assert not any("cordon" in command for command in fake.commands)


def test_initial_capacity_node_read_failure_preserves_evidence(monkeypatch):
    class NodeReadFailureKubectl(FakeKubectl):
        def __call__(self, command, timeout_seconds):
            if "get" in command and "nodes" in command:
                self.commands.append(list(command))
                return subprocess.CompletedProcess(
                    command,
                    1,
                    "",
                    "node API unavailable",
                )
            return super().__call__(command, timeout_seconds)

    fake = NodeReadFailureKubectl()
    _install_fake(monkeypatch, fake)

    with pytest.raises(
        recovery.RecoveryError,
        match="node API unavailable",
    ) as raised:
        recovery.recover_cluster(
            _cluster(),
            namespace="mock-clustermesh",
            max_affected_pods=25,
            recovery_timeout_seconds=5,
            poll_seconds=0,
            request_timeout_seconds=5,
            command_attempts=1,
            command_retry_seconds=0,
            capacity_repair=_capacity_repair_config(),
        )

    assert raised.value.evidence["capacity"]["stage"] == "initial-node-read"


def test_initial_capacity_pod_read_failure_preserves_evidence(monkeypatch):
    class PodReadFailureKubectl(FakeKubectl):
        def __call__(self, command, timeout_seconds):
            if (
                "get" in command
                and "pods" in command
                and "--all-namespaces" in command
            ):
                self.commands.append(list(command))
                return subprocess.CompletedProcess(
                    command,
                    1,
                    "",
                    "Pod API unavailable",
                )
            return super().__call__(command, timeout_seconds)

    fake = PodReadFailureKubectl()
    _install_fake(monkeypatch, fake)

    with pytest.raises(
        recovery.RecoveryError,
        match="Pod API unavailable",
    ) as raised:
        recovery.recover_cluster(
            _cluster(),
            namespace="mock-clustermesh",
            max_affected_pods=25,
            recovery_timeout_seconds=5,
            poll_seconds=0,
            request_timeout_seconds=5,
            command_attempts=1,
            command_retry_seconds=0,
            capacity_repair=_capacity_repair_config(),
        )

    assert (
        raised.value.evidence["capacity"]["stage"]
        == "initial-capacity-read"
    )


class CapacityRepairKubectl(FakeKubectl):
    """Fake an insufficient pool that gains one healthy node."""

    def __init__(self, *, existing_repair=False, existing_count=3):
        super().__init__()
        self.existing_repair = existing_repair
        self.existing_count = existing_count
        self.scaled = False
        self.show_calls = 0

    def __call__(self, command, timeout_seconds):
        command = list(command)
        if command and command[0] == "az":
            self.commands.append(command)
            if command[1:4] == ["aks", "nodepool", "show"]:
                self.show_calls += 1
                in_progress = self.existing_repair and self.show_calls == 1
                if self.existing_repair:
                    count = self.existing_count
                elif self.scaled:
                    count = 3
                else:
                    count = 2
                payload = {
                    "count": count,
                    "provisioningState": (
                        "Scaling" if in_progress else "Succeeded"
                    ),
                    "powerState": {"code": "Running"},
                }
                return subprocess.CompletedProcess(
                    command,
                    0,
                    json.dumps(payload),
                    "",
                )
            if command[1:4] == ["aks", "nodepool", "scale"]:
                self.scaled = True
                return subprocess.CompletedProcess(command, 0, "", "")
            raise AssertionError(f"unexpected Azure command: {command}")
        if "get" in command and "daemonsets" in command:
            self.commands.append(command)
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps(_daemonsets()),
                "",
            )
        if "get" in command and "nodes" in command:
            self.commands.append(command)
            existing_repair_done = (
                self.existing_repair and self.show_calls >= 2
            )
            repaired = self.scaled or (
                existing_repair_done and self.existing_count >= 3
            )
            node_b_cpu = (
                "7820m"
                if existing_repair_done and self.existing_count == 2
                else "300m"
            )
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps(
                    _nodes(
                        node_b_cpu=node_b_cpu,
                        include_node_c=repaired,
                    )
                ),
                "",
            )
        if "get" in command and "pods" in command and self.deleted:
            self.commands.append(command)
            replacement_node = (
                "node-c"
                if self.scaled
                or (self.existing_repair and self.existing_count >= 3)
                else "node-b"
            )
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps(
                    {"items": [_ready_pod(node=replacement_node)]}
                ),
                "",
            )
        return super().__call__(command, timeout_seconds)


def _capacity_repair_config():
    return recovery.CapacityRepairConfig(
        enabled=True,
        subscription_id="subscription",
        pool_name="default",
        max_pool_count=3,
        timeout_seconds=30,
        poll_seconds=0,
        cpu_reserve_millicores=250,
        memory_reserve_mib=512,
        pod_reserve=5,
        cilium_health_script="/tmp/cilium_agent_health.py",
        cilium_identity_inventory="/tmp/cilium-identities.json",
        expected_remote_count=99,
    )


def test_capacity_repair_scales_once_then_continues_recovery(monkeypatch):
    fake = CapacityRepairKubectl()
    _install_fake(monkeypatch, fake)
    monkeypatch.setattr(
        recovery,
        "_run_cilium_health_after_repair",
        lambda *_args, **_kwargs: {
            "healthy": True,
            "cilium_agent_count": 4,
            "covered_node_names": ["node-b", "node-c"],
        },
    )

    result = recovery.recover_cluster(
        _cluster(),
        namespace="mock-clustermesh",
        max_affected_pods=25,
        recovery_timeout_seconds=5,
        poll_seconds=0,
        request_timeout_seconds=5,
        command_attempts=1,
        command_retry_seconds=0,
        capacity_repair=_capacity_repair_config(),
    )

    scale_commands = [
        command
        for command in fake.commands
        if command[:4] == ["az", "aks", "nodepool", "scale"]
    ]
    assert len(scale_commands) == 1
    assert scale_commands[0][
        scale_commands[0].index("--node-count") + 1
    ] == "3"
    assert result["status"] == "recovered"
    assert result["capacity"]["repair_action"] == "scaled-up"
    assert result["capacity"]["sufficient_before_repair"] is False
    assert result["capacity"]["after"]["sufficient"] is True
    assert result["rescheduled_agents"][0]["node"] == "node-c"


def test_capacity_repair_waits_for_existing_scale_without_duplicate(
    monkeypatch,
):
    fake = CapacityRepairKubectl(existing_repair=True)
    _install_fake(monkeypatch, fake)
    monkeypatch.setattr(
        recovery,
        "_run_cilium_health_after_repair",
        lambda *_args, **_kwargs: {
            "healthy": True,
            "cilium_agent_count": 4,
            "covered_node_names": ["node-b", "node-c"],
        },
    )

    result = recovery.recover_cluster(
        _cluster(),
        namespace="mock-clustermesh",
        max_affected_pods=25,
        recovery_timeout_seconds=5,
        poll_seconds=0,
        request_timeout_seconds=5,
        command_attempts=1,
        command_retry_seconds=0,
        capacity_repair=_capacity_repair_config(),
    )

    assert not any(
        command[:4] == ["az", "aks", "nodepool", "scale"]
        for command in fake.commands
    )
    assert (
        result["capacity"]["repair_action"]
        == "waited-for-existing-repair"
    )
    assert result["status"] == "recovered"


def test_existing_repair_is_reassessed_before_another_scale(monkeypatch):
    fake = CapacityRepairKubectl(
        existing_repair=True,
        existing_count=2,
    )
    _install_fake(monkeypatch, fake)
    monkeypatch.setattr(
        recovery,
        "_run_cilium_health_after_repair",
        lambda *_args, **_kwargs: {
            "healthy": True,
            "cilium_agent_count": 3,
            "covered_node_names": ["node-b"],
        },
    )

    result = recovery.recover_cluster(
        _cluster(),
        namespace="mock-clustermesh",
        max_affected_pods=25,
        recovery_timeout_seconds=5,
        poll_seconds=0,
        request_timeout_seconds=5,
        command_attempts=1,
        command_retry_seconds=0,
        capacity_repair=_capacity_repair_config(),
    )

    assert not any(
        command[:4] == ["az", "aks", "nodepool", "scale"]
        for command in fake.commands
    )
    assert (
        result["capacity"]["repair_action"]
        == "waited-for-existing-repair"
    )
    assert result["capacity"]["after_existing_repair"]["sufficient"] is True
    assert result["status"] == "recovered"


def test_existing_sufficient_capacity_waits_for_daemonsets_without_scale(
    monkeypatch,
):
    fake = CapacityRepairKubectl(
        existing_repair=True,
        existing_count=2,
    )
    monkeypatch.setattr(recovery, "run_command", fake)
    monkeypatch.setattr(
        recovery,
        "_read_live_recovery_capacity",
        lambda *_args, **_kwargs: {
            "sufficient": True,
            "daemonsets": {"converged": False},
        },
    )
    evidence = {"repair_action": "none"}

    recovery._repair_nodepool_for_capacity(
        _cluster(),
        initial_nodes_payload=_nodes(),
        saturated_nodes=["node-a"],
        affected=[_pending_pod()],
        pod_template=_statefulset()["spec"]["template"]["spec"],
        config_settings=_capacity_repair_config(),
        evidence=evidence,
        deadline=recovery.time.monotonic() + 30,
        request_timeout_seconds=5,
        command_attempts=1,
        command_retry_seconds=0,
    )

    assert not any(
        command[:4] == ["az", "aks", "nodepool", "scale"]
        for command in fake.commands
    )
    assert evidence["repair_action"] == "waited-for-existing-repair"
    assert evidence["after_existing_repair"]["sufficient"] is True


def test_post_scale_cilium_failure_prevents_pod_mutation(monkeypatch):
    fake = CapacityRepairKubectl()
    _install_fake(monkeypatch, fake)

    def fail_cilium(*_args, **_kwargs):
        raise recovery.RecoveryError("new Cilium agent is not connected")

    monkeypatch.setattr(
        recovery,
        "_run_cilium_health_after_repair",
        fail_cilium,
    )

    with pytest.raises(
        recovery.RecoveryError,
        match="new Cilium agent is not connected",
    ) as raised:
        recovery.recover_cluster(
            _cluster(),
            namespace="mock-clustermesh",
            max_affected_pods=25,
            recovery_timeout_seconds=5,
            poll_seconds=0,
            request_timeout_seconds=5,
            command_attempts=1,
            command_retry_seconds=0,
            capacity_repair=_capacity_repair_config(),
        )

    assert raised.value.evidence["capacity"]["repair_action"] == "scaled-up"
    assert not any("cordon" in command for command in fake.commands)
    assert not any("uid-delete" in command for command in fake.commands)


def test_post_repair_cilium_requires_every_destination_node(
    tmp_path,
    monkeypatch,
):
    health_script = tmp_path / "cilium_agent_health.py"
    identity_inventory = tmp_path / "identities.json"
    health_script.write_text("# test\n", encoding="utf-8")
    identity_inventory.write_text("[]\n", encoding="utf-8")
    settings = recovery.CapacityRepairConfig(
        enabled=True,
        cilium_health_script=str(health_script),
        cilium_identity_inventory=str(identity_inventory),
        poll_seconds=0,
    )
    calls = {"count": 0}

    def fake_run(command, _timeout_seconds):
        calls["count"] += 1
        summary_path = command[command.index("--summary-file") + 1]
        agents = [{"node_name": "node-b", "healthy": True}]
        if calls["count"] > 1:
            agents.append({"node_name": "node-c", "healthy": True})
        Path(summary_path).write_text(
            json.dumps({"healthy": True, "agents": agents}),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(recovery, "run_command", fake_run)

    summary = recovery._run_cilium_health_after_repair(
        _cluster(),
        settings,
        deadline=recovery.time.monotonic() + 10,
        required_node_names=["node-b", "node-c"],
    )

    assert calls["count"] == 2
    assert summary["required_node_names"] == ["node-b", "node-c"]
    assert summary["covered_node_names"] == ["node-b", "node-c"]


def test_post_repair_cilium_timeout_becomes_recovery_error(
    tmp_path,
    monkeypatch,
):
    health_script = tmp_path / "cilium_agent_health.py"
    identity_inventory = tmp_path / "identities.json"
    health_script.write_text("# test\n", encoding="utf-8")
    identity_inventory.write_text("[]\n", encoding="utf-8")
    settings = recovery.CapacityRepairConfig(
        enabled=True,
        cilium_health_script=str(health_script),
        cilium_identity_inventory=str(identity_inventory),
        poll_seconds=0,
    )
    ticks = iter((0, 0, 0, 3))
    monkeypatch.setattr(recovery.time, "monotonic", lambda: next(ticks))

    def timeout(command, timeout_seconds):
        raise subprocess.TimeoutExpired(command, timeout_seconds)

    monkeypatch.setattr(recovery, "run_command", timeout)

    with pytest.raises(
        recovery.RecoveryError,
        match="probe timed out",
    ):
        recovery._run_cilium_health_after_repair(
            _cluster(),
            settings,
            deadline=2,
            required_node_names=["node-c"],
        )


def test_arm_repair_failure_preserves_capacity_evidence(monkeypatch):
    class CpuConstrainedKubectl(FakeKubectl):
        def __call__(self, command, timeout_seconds):
            if "get" in command and "nodes" in command:
                self.commands.append(list(command))
                return subprocess.CompletedProcess(
                    command,
                    0,
                    json.dumps(_nodes(node_b_cpu="300m")),
                    "",
                )
            return super().__call__(command, timeout_seconds)

    fake = CpuConstrainedKubectl()
    _install_fake(monkeypatch, fake)
    monkeypatch.setattr(
        recovery,
        "_read_nodepool",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            recovery.RecoveryError("ARM unavailable")
        ),
    )

    with pytest.raises(
        recovery.RecoveryError,
        match="ARM unavailable",
    ) as raised:
        recovery.recover_cluster(
            _cluster(),
            namespace="mock-clustermesh",
            max_affected_pods=25,
            recovery_timeout_seconds=5,
            poll_seconds=0,
            request_timeout_seconds=5,
            command_attempts=1,
            command_retry_seconds=0,
            capacity_repair=_capacity_repair_config(),
        )

    capacity = raised.value.evidence["capacity"]
    assert capacity["before"]["sufficient"] is False
    assert capacity["before"]["unplaced_agents"] == ["kwok-node-19"]


def test_daemonset_convergence_requires_every_desired_pod_ready():
    assert recovery._daemonset_convergence(_daemonsets())["converged"] is True

    summary = recovery._daemonset_convergence(
        _daemonsets(converged=False)
    )

    assert summary["converged"] is False
    assert summary["unhealthy"][0]["name"] == "kube-system/cilium"
    assert summary["unhealthy"][0]["ready"] == 2


def test_capacity_snapshot_reads_daemonsets_before_nodes_and_pods(
    monkeypatch,
):
    fake = FakeKubectl()
    monkeypatch.setattr(recovery, "run_command", fake)

    summary = recovery._read_live_recovery_capacity(
        _cluster(),
        affected=[_pending_pod()],
        saturated_nodes=["node-a"],
        pod_template=_statefulset()["spec"]["template"]["spec"],
        config_settings=_capacity_repair_config(),
        request_timeout_seconds=5,
        command_attempts=1,
        command_retry_seconds=0,
        deadline=recovery.time.monotonic() + 20,
        require_daemonsets=True,
    )

    reads = [
        next(
            resource
            for resource in ("daemonsets", "nodes", "pods")
            if resource in command
        )
        for command in fake.commands
        if "get" in command
        and any(
            resource in command
            for resource in ("daemonsets", "nodes", "pods")
        )
    ]
    assert reads == ["daemonsets", "nodes", "pods"]
    assert summary["daemonsets"]["converged"] is True


def test_capacity_repair_hard_pool_ceiling_blocks_scale(monkeypatch):
    commands = []
    monkeypatch.setattr(
        recovery,
        "run_command",
        lambda command, _timeout: commands.append(command),
    )

    with pytest.raises(recovery.RecoveryError, match="hard maximum is 3"):
        recovery._scale_nodepool_once(
            _cluster(),
            _capacity_repair_config(),
            target_count=4,
            request_timeout_seconds=5,
            command_attempts=1,
            command_retry_seconds=0,
            deadline=recovery.time.monotonic() + 5,
        )

    assert not commands


def test_capacity_repair_cli_rejects_pool_ceiling_above_three(tmp_path):
    with pytest.raises(SystemExit):
        recovery.parse_args(
            [
                "--clusters",
                str(tmp_path / "clusters.json"),
                "--summary-file",
                str(tmp_path / "summary.json"),
                "--capacity-repair-enabled",
                "--subscription-id",
                "subscription",
                "--capacity-repair-pool",
                "default",
                "--capacity-repair-max-pool-count",
                "4",
                "--cilium-health-script",
                str(tmp_path / "health.py"),
                "--cilium-identity-inventory",
                str(tmp_path / "identities.json"),
            ]
        )


def test_final_readiness_interruption_preserves_capacity_evidence(
    monkeypatch,
):
    fake = FakeKubectl(reschedules=False)
    monkeypatch.setattr(recovery, "run_command", fake)
    monkeypatch.setattr(
        recovery.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(
            recovery.RecoveryInterrupted("received signal 15")
        ),
    )
    evidence = {"repair_action": "scaled-up"}

    with pytest.raises(
        recovery.RecoveryError,
        match="received signal 15",
    ) as raised:
        recovery._wait_recovered_agents_ready(
            _cluster(),
            namespace="mock-clustermesh",
            affected_names=["kwok-node-19"],
            expected_uids={"kwok-node-19": "new-uid"},
            controller_uid="controller-uid",
            alternate_nodes=["node-b"],
            timeout_seconds=5,
            poll_seconds=1,
            request_timeout_seconds=5,
            capacity_evidence=evidence,
        )

    assert raised.value.evidence["capacity"] == evidence


def test_capacity_poll_sleep_interruption_preserves_evidence(monkeypatch):
    evidence = {"before": {"sufficient": False}}
    monkeypatch.setattr(
        recovery.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(
            recovery.RecoveryInterrupted("received signal 15")
        ),
    )

    with pytest.raises(
        recovery.RecoveryError,
        match="received signal 15",
    ) as raised:
        recovery._sleep_with_capacity_evidence(1, evidence)

    assert raised.value.evidence["capacity"] == evidence


def test_deadline_before_first_convergence_snapshot_is_structured(
    monkeypatch,
):
    fake = FakeKubectl()
    monkeypatch.setattr(recovery, "run_command", fake)
    ticks = iter((0, 11))
    monkeypatch.setattr(recovery.time, "monotonic", lambda: next(ticks))

    with pytest.raises(
        recovery.RecoveryError,
        match="before the first convergence snapshot",
    ) as raised:
        recovery.ensure_recovery_capacity(
            _cluster(),
            affected=[_pending_pod()],
            saturated_nodes=["node-a"],
            pod_template=_statefulset()["spec"]["template"]["spec"],
            initial_nodes_payload=_nodes(),
            config_settings=_capacity_repair_config(),
            request_timeout_seconds=5,
            command_attempts=1,
            command_retry_seconds=0,
            deadline=10,
        )

    assert raised.value.evidence["capacity"][
        "sufficient_before_repair"
    ] is True
