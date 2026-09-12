"""Validate the explicit mesh-96 modern-pool delta without altering old proofs."""

from __future__ import annotations

import copy


SUBSCRIPTION = "37deca37-c375-4a14-b90a-043849bd2bf1"
RUN_ID = "78751-f36f3d5a"
ROLE = "mesh-96"
ORIGINAL_POOL_COUNT = 201
MODERN_POOL_COUNT = 202
PLAN_SHA = "1a4385e2db5a0a5b38d750a6a82fcb9b3d4c683e801bf12bd8111c75355e380a"
EXPECTED_POOLS = {
    "default": {"count": 1, "mode": "System", "vm_size": "Standard_D8_v3"},
    "promv5": {"count": 1, "mode": "User", "vm_size": "Standard_D8s_v5"},
    "cniv5": {"count": 2, "mode": "System", "vm_size": "Standard_D8s_v5"},
}


class BaselineError(Exception):
    """An unproved hardware-layout change must not authorize workload handoff."""


def require(condition, message):
    if not condition:
        raise BaselineError(message)


def validate_receipt(receipt, *, run_id, subscription_id, expected_pool_count):
    require(run_id == RUN_ID and isinstance(subscription_id, str) and subscription_id.lower() == SUBSCRIPTION
            and expected_pool_count == MODERN_POOL_COUNT,
            "Modern pool delta requires the exact preserved scope and 202 pools")
    require(isinstance(receipt, dict) and receipt.get("success") is True
            and receipt.get("repaired") is True and receipt.get("workloads_ready") is False
            and receipt.get("plan_sha256") == PLAN_SHA,
            "Modern pool delta lacks a completed, original-plan-bound recovery receipt")
    cni = receipt.get("modern_cni")
    require(isinstance(cni, dict) and cni.get("completed") is True
            and cni.get("source_retired") is True and cni.get("pool_name") == "cniv5"
            and cni.get("default_pool_count") == 1 and cni.get("destination_pool_count") == 2
            and cni.get("default_role_worker_count") == 3
            and all(isinstance(cni.get(key), int) and not isinstance(cni[key], bool)
                    for key in ("default_pool_count", "destination_pool_count", "default_role_worker_count")),
            "Modern CNI receipt does not prove the bounded final worker layout")
    return validate_layout(receipt.get("baseline_pool_layout"), run_id=run_id,
                           subscription_id=subscription_id, expected_pool_count=expected_pool_count)


def validate_layout(layout, *, run_id, subscription_id, expected_pool_count):
    require(run_id == RUN_ID and isinstance(subscription_id, str) and subscription_id.lower() == SUBSCRIPTION
            and expected_pool_count == MODERN_POOL_COUNT,
            "Modern layout is outside the approved preserved scope")
    require(isinstance(layout, dict) and layout.get("schema_version") == 1
            and not isinstance(layout["schema_version"], bool)
            and layout.get("role") == ROLE and layout.get("expected_total_pool_count") == MODERN_POOL_COUNT
            and isinstance(layout.get("pools"), dict) and set(layout["pools"]) == set(EXPECTED_POOLS),
            "Modern pool names or baseline count are not exact")
    for name, expected in EXPECTED_POOLS.items():
        pool = layout["pools"][name]
        resource_id = (
            f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{RUN_ID}/providers/"
            f"Microsoft.ContainerService/managedClusters/clustermesh-96/agentPools/{name}"
        )
        require(isinstance(pool, dict) and all(pool.get(key) == value for key, value in expected.items())
                and isinstance(pool.get("count"), int) and not isinstance(pool["count"], bool)
                and isinstance(pool.get("resource_id"), str) and pool["resource_id"].lower() == resource_id.lower(),
                f"Modern pool {name} has a different identity, count, mode, or SKU")
    return copy.deepcopy({
        "schema_version": 1, "role": ROLE, "expected_total_pool_count": MODERN_POOL_COUNT,
        "pools": {name: {key: layout["pools"][name][key] for key in ("count", "mode", "vm_size", "resource_id")}
                  for name in EXPECTED_POOLS},
    })


def expected_keys(clusters, layout):
    keys = {(cluster.role, name) for cluster in clusters for name in ("default", "prompool")}
    keys.add(("mesh-1", "churnpool"))
    if layout is not None:
        keys.remove((ROLE, "prompool"))
        keys.update((ROLE, name) for name in EXPECTED_POOLS)
    return keys


def validate_live_pool(role, pool, layout):
    if layout is None or role != ROLE:
        return
    name = pool.get("name")
    require(name in layout["pools"], f"{ROLE}: unexpected modern baseline pool")
    expected = layout["pools"][name]
    require(pool.get("count") == expected["count"]
            and isinstance(pool.get("count"), int) and not isinstance(pool["count"], bool)
            and pool.get("mode") == expected["mode"] and pool.get("vmSize") == expected["vm_size"]
            and pool.get("enableAutoScaling") is False,
            f"{ROLE}/{name}: live modern count, mode, SKU, or autoscaling drifted")
    labels = pool.get("nodeLabels") or {}
    require(isinstance(labels, dict), f"{ROLE}/{name}: malformed node labels")
    require(labels.get("prometheus") == "true" if name == "promv5" else "prometheus" not in labels,
            f"{ROLE}/{name}: monitoring/mock placement labels changed")
