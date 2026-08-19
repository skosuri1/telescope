"""Tests for preserved real-worker reconciliation."""

import importlib.util
import sys
from pathlib import Path

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "clusterloader2"
    / "clustermesh-scale"
    / "preserved_worker_reconcile.py"
)
MODULE_SPEC = importlib.util.spec_from_file_location(
    "preserved_worker_reconcile",
    MODULE_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise ImportError(f"Unable to load module from {MODULE_PATH}")
workers = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = workers
MODULE_SPEC.loader.exec_module(workers)


CLUSTER = workers.Cluster(
    name="clustermesh-38",
    resource_group="76575-f36f3d5a",
    role="mesh-38",
    kubeconfig="/tmp/mesh-38.config",
)


def node(vmss_name, instance_id, *, ready=True, unschedulable=False):
    return {
        "metadata": {"name": f"aks-{vmss_name}-{instance_id}"},
        "spec": {
            "providerID": (
                "/subscriptions/s/resourceGroups/rg/providers/"
                "Microsoft.Compute/virtualMachineScaleSets/"
                f"{vmss_name}/virtualMachines/{instance_id}"
            ),
            "unschedulable": unschedulable,
        },
        "status": {
            "conditions": [
                {
                    "type": "Ready",
                    "status": "True" if ready else "False",
                }
            ]
        },
    }


def pool(name, count):
    return {
        "name": name,
        "count": count,
        "provisioningState": "Succeeded",
        "powerState": {"code": "Running"},
    }


def vmss(name, pool_name, capacity):
    return {
        "name": name,
        "sku": {"capacity": capacity},
        "provisioningState": "Succeeded",
        "tags": {"aks-managed-poolName": pool_name},
    }


def instances(*ids):
    return [{"instanceId": value} for value in ids]


def test_healthy_pool_requires_matching_ready_kubernetes_nodes():
    state = workers.build_cluster_state(
        CLUSTER,
        "MC_rg_cluster_region",
        [pool("default", 2)],
        [vmss("aks-default", "default", 2)],
        {"aks-default": instances("0", "1")},
        [node("aks-default", "0"), node("aks-default", "1")],
    )

    assert state.healthy
    assert state.pools[0].stale_instance_ids == []


def test_missing_kubernetes_workers_marks_all_vmss_instances_stale():
    state = workers.build_cluster_state(
        CLUSTER,
        "MC_rg_cluster_region",
        [pool("default", 2), pool("prompool", 1)],
        [
            vmss("aks-default", "default", 2),
            vmss("aks-prom", "prompool", 1),
        ],
        {
            "aks-default": instances("0", "1"),
            "aks-prom": instances("0"),
        },
        [],
    )

    assert not state.healthy
    assert state.pools[0].stale_instance_ids == ["0", "1"]
    assert state.pools[1].stale_instance_ids == ["0"]


def test_not_ready_instance_is_replaced_but_ready_unschedulable_is_uncordoned():
    state = workers.build_cluster_state(
        CLUSTER,
        "MC_rg_cluster_region",
        [pool("default", 2)],
        [vmss("aks-default", "default", 2)],
        {"aks-default": instances("0", "1")},
        [
            node("aks-default", "0", ready=False),
            node("aks-default", "1", unschedulable=True),
        ],
    )
    observed = state.pools[0]

    assert observed.stale_instance_ids == ["0"]
    assert observed.unschedulable_nodes == ["aks-aks-default-1"]
    assert not observed.healthy


def test_capacity_above_desired_is_unsafe_for_automatic_repair():
    state = workers.build_cluster_state(
        CLUSTER,
        "MC_rg_cluster_region",
        [pool("default", 2)],
        [vmss("aks-default", "default", 3)],
        {"aks-default": instances("0", "1", "2")},
        [
            node("aks-default", "0"),
            node("aks-default", "1"),
            node("aks-default", "2"),
        ],
    )

    assert "VMSS capacity 3 exceeds desired count 2" in (
        state.pools[0].unsafe_reasons
    )


def test_stale_kubernetes_node_for_old_instance_is_ignored():
    state = workers.build_cluster_state(
        CLUSTER,
        "MC_rg_cluster_region",
        [pool("default", 2)],
        [vmss("aks-default", "default", 2)],
        {"aks-default": instances("2", "3")},
        [
            node("aks-default", "0"),
            node("aks-default", "2"),
            node("aks-default", "3"),
        ],
    )

    assert state.healthy
    assert state.pools[0].node_instance_ids == ["2", "3"]


def test_kwok_nodes_are_ignored_by_provider_identity():
    kwok = {
        "metadata": {"name": "kwok-node-0", "labels": {"type": "kwok"}},
        "spec": {"providerID": "kwok://kwok-node-0"},
        "status": {"conditions": [{"type": "Ready", "status": "False"}]},
    }
    state = workers.build_cluster_state(
        CLUSTER,
        "MC_rg_cluster_region",
        [pool("default", 1)],
        [vmss("aks-default", "default", 1)],
        {"aks-default": instances("0")},
        [kwok],
    )

    assert state.pools[0].node_instance_ids == []
    assert state.pools[0].stale_instance_ids == ["0"]


def test_repair_pool_always_nudges_after_deleting_stale_instances():
    observed = workers.build_cluster_state(
        CLUSTER,
        "MC_rg_cluster_region",
        [pool("default", 2)],
        [vmss("aks-default", "default", 2)],
        {"aks-default": instances("0", "1")},
        [node("aks-default", "0", ready=False), node("aks-default", "1")],
    ).pools[0]
    current_count = 2
    scale_targets = []
    deleted_instances = []

    def runner(args, _timeout):
        nonlocal current_count
        if args[1:3] == ["vmss", "delete-instances"]:
            deleted_instances.extend(
                args[args.index("--instance-ids") + 1 : args.index("--output")]
            )
            return ""
        if args[1:4] == ["aks", "nodepool", "scale"]:
            current_count = int(args[args.index("--node-count") + 1])
            scale_targets.append(current_count)
            return ""
        if args[1:4] == ["aks", "nodepool", "show"]:
            return (
                '{"count":%d,"provisioningState":"Succeeded"}'
                % current_count
            )
        if args[1:3] == ["vmss", "show"]:
            return (
                '{"sku":{"capacity":%d},"provisioningState":"Succeeded"}'
                % current_count
            )
        raise AssertionError(f"unexpected command: {args}")

    actions = workers.repair_pool(
        observed,
        CLUSTER,
        runner,
        query_timeout_seconds=5,
        mutation_timeout_seconds=5,
        recovery_timeout_seconds=30,
        poll_seconds=1,
    )

    assert deleted_instances == ["0"]
    assert scale_targets == [3, 2]
    assert "nudged desired count to 3" in actions
    assert "restored desired count to 2" in actions


def test_repair_pool_rolls_back_count_after_post_nudge_failure(monkeypatch):
    observed = workers.build_cluster_state(
        CLUSTER,
        "MC_rg_cluster_region",
        [pool("default", 2)],
        [vmss("aks-default", "default", 2)],
        {"aks-default": instances("0", "1")},
        [node("aks-default", "0", ready=False), node("aks-default", "1")],
    ).pools[0]
    current_count = 2
    scale_targets = []

    def runner(args, _timeout):
        nonlocal current_count
        if args[1:3] == ["vmss", "delete-instances"]:
            return ""
        if args[1:4] == ["aks", "nodepool", "scale"]:
            current_count = int(args[args.index("--node-count") + 1])
            scale_targets.append(current_count)
            return ""
        if args[1:4] == ["aks", "nodepool", "show"]:
            return (
                '{"count":%d,"provisioningState":"Succeeded"}'
                % current_count
            )
        if args[1:3] == ["vmss", "show"]:
            return (
                '{"sku":{"capacity":%d},"provisioningState":"Succeeded"}'
                % current_count
            )
        raise AssertionError(f"unexpected command: {args}")

    wait_calls = []

    def flaky_wait(_pool, target_count, *_args):
        wait_calls.append(target_count)
        if wait_calls == [3, 2]:
            raise workers.ReconcileError("post-restore observation failed")

    monkeypatch.setattr(workers, "wait_arm_pool_count", flaky_wait)
    with pytest.raises(workers.ReconcileError, match="post-restore"):
        workers.repair_pool(
            observed,
            CLUSTER,
            runner,
            query_timeout_seconds=5,
            mutation_timeout_seconds=5,
            recovery_timeout_seconds=30,
            poll_seconds=1,
        )

    assert scale_targets == [3, 2, 3, 2]
    assert current_count == 2
