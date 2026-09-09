"""Tests for targeted mock-agent Azure CNI recovery."""

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
        "spec": {"nodeName": node},
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
        "spec": {"nodeName": node},
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


def _nodes():
    return {
        "items": [
            {
                "metadata": {
                    "name": "node-a",
                    "labels": {"kubernetes.azure.com/cluster": "cluster"},
                },
                "spec": {"unschedulable": False},
                "status": {
                    "conditions": [{"type": "Ready", "status": "True"}]
                },
            },
            {
                "metadata": {
                    "name": "node-b",
                    "labels": {"kubernetes.azure.com/cluster": "cluster"},
                },
                "spec": {"unschedulable": False},
                "status": {
                    "conditions": [{"type": "Ready", "status": "True"}]
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
    }


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

    with pytest.raises(recovery.RecoveryError, match="cordon node-a failed"):
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

    with pytest.raises(recovery.RecoveryError, match="no alternate Ready"):
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
