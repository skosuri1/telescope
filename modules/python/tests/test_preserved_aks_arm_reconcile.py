"""Tests for bounded preserved AKS ARM state reconciliation."""

import importlib.util
import copy
import json
import sys
from pathlib import Path

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "clusterloader2"
    / "clustermesh-scale"
    / "preserved_aks_arm_reconcile.py"
)
MODULE_SPEC = importlib.util.spec_from_file_location(
    "preserved_aks_arm_reconcile",
    MODULE_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise ImportError(f"Unable to load module from {MODULE_PATH}")
arm = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = arm
MODULE_SPEC.loader.exec_module(arm)


def cluster_row(number, state="Succeeded", pool_state="Succeeded"):
    return {
        "id": (
            "/subscriptions/s/resourceGroups/12345-deadbeef/providers/"
            f"Microsoft.ContainerService/managedClusters/clustermesh-{number}"
        ),
        "name": f"clustermesh-{number}",
        "resourceGroup": "12345-deadbeef",
        "location": "eastus2euap",
        "networkProfile": {
            "networkDataplane": "cilium",
            "networkPolicy": "cilium",
        },
        "nodeResourceGroup": (
            f"MC_12345-deadbeef_clustermesh-{number}_eastus2euap"
        ),
        "powerState": {"code": "Running"},
        "provisioningState": state,
        "tags": {"role": f"mesh-{number}"},
        "agentPoolProfiles": [
            {
                "name": "default",
                "powerState": {"code": "Running"},
                "provisioningState": pool_state,
            }
        ],
    }


def test_inventory_reads_retry_only_transient_failures(monkeypatch):
    calls = []

    def transient_runner(_args, _timeout):
        calls.append("call")
        if len(calls) == 1:
            raise arm.ReconcileError("command timed out after 5s")
        return "[]"

    monkeypatch.setattr(arm.time, "sleep", lambda _seconds: None)
    assert arm.run_read_with_retries(
        ["az", "aks", "list"],
        transient_runner,
        timeout_seconds=5,
        attempts=2,
        retry_seconds=1,
    ) == "[]"
    assert len(calls) == 2

    def fatal_runner(_args, _timeout):
        raise arm.ReconcileError("AuthorizationFailed")

    with pytest.raises(arm.ReconcileError, match="AuthorizationFailed"):
        arm.run_read_with_retries(
            ["az", "aks", "list"],
            fatal_runner,
            timeout_seconds=5,
            attempts=3,
            retry_seconds=1,
        )


def test_resource_group_requires_exact_preserved_desired_state():
    payload = {
        "location": "eastus2euap",
        "tags": {
            "run_id": "12345-deadbeef",
            "scenario": "perf-eval-clustermesh-scale",
            "clustermesh_debug_preserved": "true",
            "clustermesh_debug_expected_clusters": "2",
            "clustermesh_debug_tfvars_sha256": "expected-sha",
        },
    }

    arm.validate_resource_group(
        payload,
        "12345-deadbeef",
        "eastus2euap",
        2,
        "expected-sha",
    )
    payload["tags"]["clustermesh_debug_tfvars_sha256"] = "other-sha"
    with pytest.raises(arm.ReconcileError, match="tfvars_sha256 mismatch"):
        arm.validate_resource_group(
            payload,
            "12345-deadbeef",
            "eastus2euap",
            2,
            "expected-sha",
        )


def test_inventory_accepts_only_bounded_failed_clusters():
    clusters, failed = arm.validate_cluster_inventory(
        [cluster_row(1), cluster_row(2, "Failed")],
        expected_count=2,
        region="eastus2euap",
        max_repair_clusters=1,
    )

    assert [cluster.role for cluster in clusters] == ["mesh-1", "mesh-2"]
    assert [cluster.role for cluster in failed] == ["mesh-2"]


def test_failed_cluster_allows_quiescent_failed_pool_state():
    _, failed = arm.validate_cluster_inventory(
        [cluster_row(1), cluster_row(2, "Failed", "Failed")],
        expected_count=2,
        region="eastus2euap",
        max_repair_clusters=1,
    )
    assert [cluster.role for cluster in failed] == ["mesh-2"]

    with pytest.raises(arm.ReconcileError, match="not safely quiescent"):
        arm.validate_cluster_inventory(
            [cluster_row(1), cluster_row(2, "Failed", "Updating")],
            expected_count=2,
            region="eastus2euap",
            max_repair_clusters=1,
        )

    with pytest.raises(arm.ReconcileError, match="not safely quiescent"):
        arm.validate_cluster_inventory(
            [cluster_row(1, "Succeeded", "Failed"), cluster_row(2)],
            expected_count=2,
            region="eastus2euap",
            max_repair_clusters=1,
        )


def test_failed_pool_repair_is_opt_in_and_capped():
    rows = [cluster_row(1), cluster_row(2, pool_state="Failed")]
    clusters, failed = arm.validate_cluster_inventory(
        rows, expected_count=2, region="eastus2euap", max_repair_clusters=1,
        allow_failed_pool_repair=True,
    )
    assert failed == []
    assert clusters[1].failed_pools == ("default",)
    with pytest.raises(arm.ReconcileError, match="maximum 5"):
        arm.validate_cluster_inventory(
            [cluster_row(number, pool_state="Failed") for number in range(1, 7)],
            expected_count=6, region="eastus2euap", max_repair_clusters=10,
            allow_failed_pool_repair=True,
        )
    rows[1]["agentPoolProfiles"][0]["name"] = "unowned"
    with pytest.raises(arm.ReconcileError, match="unknown failed pool"):
        arm.validate_cluster_inventory(
            rows, expected_count=2, region="eastus2euap", max_repair_clusters=1,
            allow_failed_pool_repair=True,
        )


def pool_cluster():
    return arm.Cluster(
        name="clustermesh-2", role="mesh-2", resource_group="12345-deadbeef",
        resource_id="/subscriptions/s/resourceGroups/12345-deadbeef/providers/Microsoft.ContainerService/managedClusters/clustermesh-2",
        node_resource_group="MC_12345-deadbeef_clustermesh-2_eastus2euap",
        state="Succeeded", power_state="Running", failed_pools=("prompool",),
    )


def pool_payload(state="Failed"):
    cluster = pool_cluster()
    return {
        "id": f"{cluster.resource_id}/agentPools/prompool",
        "name": "prompool", "provisioningState": state,
        "powerState": {"code": "Running"}, "count": 1,
        "enableAutoScaling": False, "vmSize": "Standard_D8_v3",
        "vnetSubnetId": "/node-subnet", "podSubnetId": "/pod-subnet",
    }


def pool_node(ready="True"):
    cluster = pool_cluster()
    return {
        "metadata": {
            "name": "node-a",
            "labels": {"kubernetes.azure.com/agentpool": "prompool"},
        },
        "spec": {
            "providerID": f"azure:///subscriptions/s/resourceGroups/{cluster.node_resource_group}/providers/Microsoft.Compute/virtualMachineScaleSets/pool-vmss/virtualMachines/0",
        },
        "status": {"conditions": [{"type": "Ready", "status": ready}]},
    }


def test_failed_pool_requires_exact_ready_workers_and_stable_vmss():
    calls = []

    def runner(command, _timeout):
        calls.append(command)
        if command[0] == "kubectl":
            return arm.json.dumps({"items": [pool_node()]})
        return arm.json.dumps({"provisioningState": "Succeeded", "sku": {"capacity": 1}})

    assert arm.validate_pool_workers(pool_cluster(), pool_payload(), "/fake", runner, 5) == ["node-a"]
    assert all(command[0] == "kubectl" or command[:3] == ["az", "vmss", "show"] for command in calls)


@pytest.mark.parametrize("fault", ["not-ready", "missing", "cordoned", "wrong-identity", "vmss-failed", "wrong-capacity"])
def test_failed_pool_unhealthy_workers_prevent_mutation(fault):
    node = pool_node()
    nodes = [node]
    vmss = {"provisioningState": "Succeeded", "sku": {"capacity": 1}}
    if fault == "not-ready":
        node["status"]["conditions"][0]["status"] = "False"
    elif fault == "missing":
        nodes = []
    elif fault == "cordoned":
        node["spec"]["unschedulable"] = True
    elif fault == "wrong-identity":
        node["spec"]["providerID"] = node["spec"]["providerID"].replace("/subscriptions/s/", "/subscriptions/other/")
    elif fault == "vmss-failed":
        vmss["provisioningState"] = "Failed"
    else:
        vmss["sku"]["capacity"] = 2

    def runner(command, _timeout):
        assert "update" not in command
        return arm.json.dumps({"items": nodes} if command[0] == "kubectl" else vmss)

    with pytest.raises(arm.ReconcileError):
        arm.validate_pool_workers(pool_cluster(), pool_payload(), "/fake", runner, 5)


def test_pool_reconcile_preserves_configuration_and_health_order(tmp_path, monkeypatch):
    fake_clock(monkeypatch)
    order = []
    pool_reads = iter(["Failed", "Failed", "Updating", "Succeeded"])

    def health(*_args, **kwargs):
        assert kwargs["identity_inventory"] == "/identities.json"
        order.append("health")
        return {"healthy": True, "agents": [{"node_name": "node-a", "healthy": True}]}

    def workers(*_args):
        order.append("workers")
        return ["node-a"]

    def runner(command, _timeout):
        if command[:4] == ["az", "aks", "nodepool", "show"]:
            order.append("read")
            return arm.json.dumps(pool_payload(next(pool_reads)))
        assert command[:4] == ["az", "aks", "nodepool", "update"]
        assert all(option not in command for option in ("--node-count", "--node-vm-size", "--kubernetes-version", "--node-image-only"))
        order.append("update")
        return ""

    monkeypatch.setattr(arm, "validate_cluster_data_plane", health)
    monkeypatch.setattr(arm, "validate_pool_workers", workers)
    evidence = {}
    arm.reconcile_failed_pool(
        pool_cluster(), "prompool", "/fake", "/identities.json", 1,
        runner, quiescence_args(tmp_path), evidence,
    )
    assert evidence["status"] == "repaired"
    assert evidence["configuration_before"] == evidence["configuration_after"]
    assert order == ["read", "health", "workers", "read", "update", "read", "read", "workers", "health"]


def test_pool_reconcile_aborts_before_update_if_live_health_fails(tmp_path, monkeypatch):
    calls = []

    def runner(command, _timeout):
        calls.append(command)
        assert "update" not in command
        return arm.json.dumps(pool_payload())

    def unhealthy(*_args, **_kwargs):
        raise arm.ReconcileError("Cilium unhealthy")

    monkeypatch.setattr(arm, "validate_cluster_data_plane", unhealthy)
    with pytest.raises(arm.ReconcileError, match="Cilium unhealthy"):
        arm.reconcile_failed_pool(
            pool_cluster(), "prompool", "/fake", "/identities.json", 1,
            runner, quiescence_args(tmp_path), {},
        )
    assert len(calls) == 1


@pytest.mark.parametrize("fault", ["configuration-change", "failed-update"])
def test_pool_reconcile_never_accepts_config_drift_or_failed_update(
    tmp_path, monkeypatch, fault,
):
    fake_clock(monkeypatch)
    monkeypatch.setattr(
        arm, "validate_cluster_data_plane",
        lambda *_a, **_k: {"healthy": True, "agents": [{"node_name": "node-a", "healthy": True}]},
    )
    monkeypatch.setattr(arm, "validate_pool_workers", lambda *_a: ["node-a"])
    reads = 0

    def runner(command, _timeout):
        nonlocal reads
        if "update" in command:
            return ""
        reads += 1
        payload = copy.deepcopy(pool_payload())
        if reads == 3:
            payload["provisioningState"] = "Updating"
        elif reads >= 4:
            payload["provisioningState"] = "Succeeded" if fault == "configuration-change" else "Failed"
            if fault == "configuration-change":
                payload["count"] = 2
        return arm.json.dumps(payload)

    with pytest.raises(arm.ReconcileError, match="changed pool configuration|update ended in Failed"):
        arm.reconcile_failed_pool(
            pool_cluster(), "prompool", "/fake", "/identities.json", 1,
            runner, quiescence_args(tmp_path), {},
        )


def test_upgrade_surge_is_temporary_and_bounded_by_existing_settings():
    before = pool_payload()
    before["count"] = 2
    before["upgradeSettings"] = {"maxSurge": "10%"}
    current = copy.deepcopy(before)
    current.update(provisioningState="Updating", count=3)
    assert arm.pool_configuration_matches(before, current) is True
    current["count"] = 4
    assert arm.pool_configuration_matches(before, current) is False
    current["count"] = 1
    assert arm.pool_configuration_matches(before, current) is False
    current["count"] = 3
    current["provisioningState"] = "Succeeded"
    assert arm.pool_configuration_matches(before, current) is False
    current["count"] = 2
    assert arm.pool_configuration_matches(before, current) is True
    current.update(provisioningState="Scaling", count=3)
    assert arm.pool_configuration_matches(before, current) is False


def test_upgrade_surge_never_allows_other_configuration_drift():
    before = pool_payload()
    before["count"] = 2
    before["upgradeSettings"] = {"maxSurge": "10%"}
    current = copy.deepcopy(before)
    current.update(provisioningState="Updating", count=3, vmSize="Standard_D16_v3")
    assert arm.pool_configuration_matches(before, current) is False
    current["vmSize"] = before["vmSize"]
    current["upgradeSettings"]["maxSurge"] = "50%"
    assert arm.pool_configuration_matches(before, current) is False


@pytest.mark.parametrize("field", ["osSKU", "osSku"])
def test_upgrade_surge_rejects_os_sku_drift_for_both_payload_spellings(field):
    before = pool_payload()
    before["count"] = 2
    before[field] = "Ubuntu"
    current = copy.deepcopy(before)
    current.update(provisioningState="Updating", count=3)
    current[field] = "AzureLinux"
    assert arm.pool_configuration_matches(before, current) is False


def test_final_inventory_never_accepts_remaining_failed_pool(tmp_path, monkeypatch):
    fake_clock(monkeypatch)
    args = quiescence_args(tmp_path)
    args.failed_pool_repair_enabled = True
    with pytest.raises(arm.ReconcileError, match="not safely quiescent"):
        arm.read_quiescent_inventory(
            args, {}, "final",
            lambda *_args: arm.json.dumps([cluster_row(1), cluster_row(2, pool_state="Failed")]),
        )


def test_failed_pool_requires_cilium_coverage_of_its_own_worker():
    with pytest.raises(arm.ReconcileError, match="every pool worker"):
        arm.require_pool_cilium_coverage(
            pool_cluster(), "prompool", ["node-a"],
            {"healthy": True, "agents": [{"node_name": "another-node", "healthy": True}]},
        )


@pytest.mark.parametrize("fault", ["wrong-id", "wrong-name", "stopped", "autoscaling", "zero-count"])
def test_pool_read_refuses_unsafe_identity_power_or_count(fault):
    payload = pool_payload()
    if fault == "wrong-id":
        payload["id"] = payload["id"].replace("clustermesh-2", "clustermesh-3")
    elif fault == "wrong-name":
        payload["name"] = "default"
    elif fault == "stopped":
        payload["powerState"]["code"] = "Stopped"
    elif fault == "autoscaling":
        payload["enableAutoScaling"] = True
    else:
        payload["count"] = 0
    with pytest.raises(arm.ReconcileError):
        arm.read_pool(
            pool_cluster(), "prompool",
            lambda *_args: arm.json.dumps(payload), 5,
        )


def test_pool_identity_inventory_uses_exact_fleet_assignments(tmp_path):
    clusters, _ = arm.validate_cluster_inventory(
        [cluster_row(1), cluster_row(2)],
        expected_count=2, region="eastus2euap", max_repair_clusters=1,
    )
    members = [
        {"name": "mesh-1", "meshProperties": {"ciliumProperties": {"name": "mesh-17", "id": 7}}},
        {"name": "mesh-2", "meshProperties": {"ciliumProperties": {"name": "mesh-29", "id": 9}}},
    ]
    path = tmp_path / "identities.json"
    arm.write_pool_repair_identities(str(path), members, clusters)
    assert arm.json.loads(path.read_text(encoding="utf-8")) == [
        {"role": "mesh-1", "cluster_name": "mesh-17", "cluster_id": 7},
        {"role": "mesh-2", "cluster_name": "mesh-29", "cluster_id": 9},
    ]
    with pytest.raises(arm.ReconcileError, match="exact, unique"):
        arm.write_pool_repair_identities(str(path), members + [members[0]], clusters)


@pytest.mark.parametrize("command_fails,reported_healthy", [(True, False), (True, True), (False, False)])
def test_failed_cilium_probe_preserves_current_detailed_evidence(
    tmp_path, command_fails, reported_healthy,
):
    health = {
        "healthy": reported_healthy,
        "agents": [{
            "pod_name": "cilium-a", "node_name": "node-a",
            "healthy": reported_healthy, "ready_remote_count": 0, "remote_count": 1,
            "not_ready_remote_names": ["mesh-11"], "missing_remote_names": [],
            "unexpected_remote_names": [], "duplicate_remote_names": [],
        }],
    }

    def runner(command, _timeout):
        if command[:3] == ["az", "aks", "get-credentials"]:
            return ""
        if "--raw=/readyz" in command:
            return "ok"
        if "deployment" in command:
            return '{"status":{"conditions":[{"type":"Available","status":"True"}]}}'
        summary_path = Path(command[command.index("--summary-file") + 1])
        summary_path.write_text(arm.json.dumps(health), encoding="utf-8")
        if command_fails:
            raise arm.ReconcileError("probe exited 1")
        return ""

    with pytest.raises(arm.ReconcileError, match="mesh-11") as failure:
        arm.validate_cluster_data_plane(
            pool_cluster(), str(tmp_path / "cluster.config"), 1, runner, 5,
            identity_inventory="/identities.json",
        )
    assert failure.value.evidence["cilium_health"] == health
    assert failure.value.evidence["command_error"] == (
        "probe exited 1" if command_fails else None
    )


def test_failed_cilium_probe_cannot_reuse_old_success_summary(tmp_path):
    kubeconfig = str(tmp_path / "cluster.config")
    Path(f"{kubeconfig}.cilium-health.json").write_text('{"healthy":true}', encoding="utf-8")

    def runner(command, _timeout):
        if command[:3] == ["az", "aks", "get-credentials"]:
            return ""
        if "--raw=/readyz" in command:
            return "ok"
        if "deployment" in command:
            return '{"status":{"conditions":[{"type":"Available","status":"True"}]}}'
        raise arm.ReconcileError("probe failed without a summary")

    with pytest.raises(arm.ReconcileError, match="proof is unavailable") as failure:
        arm.validate_cluster_data_plane(
            pool_cluster(), kubeconfig, 1, runner, 5,
            identity_inventory="/identities.json",
        )
    assert failure.value.evidence["command_error"] == "probe failed without a summary"


@pytest.mark.parametrize("mutation_started", [False, True])
def test_independent_pool_repair_continues_only_after_read_only_failure(
    tmp_path, monkeypatch, mutation_started,
):
    args = quiescence_args(tmp_path)
    args.failed_pool_repair_enabled = True
    clusters, failed = arm.validate_cluster_inventory(
        [cluster_row(1, pool_state="Failed"), cluster_row(2, pool_state="Failed")],
        expected_count=2, region="eastus2euap", max_repair_clusters=1,
        allow_failed_pool_repair=True,
    )
    members = [
        {"name": f"mesh-{number}", "meshProperties": {
            "status": {"state": "Connected"},
            "ciliumProperties": {"id": number, "name": f"mesh-{number}{number}"},
        }}
        for number in (1, 2)
    ]
    monkeypatch.setattr(arm, "parse_args", lambda _argv: args)
    monkeypatch.setattr(arm, "validate_resource_group", lambda *_args: None)

    def inventory(_args, _summary, phase, _runner):
        assert phase == "initial", "final certification cannot run after a failed pool guard"
        return clusters, failed

    def runner(command, _timeout):
        if command[:3] == ["az", "account", "show"]:
            return "s"
        if command[:3] == ["az", "group", "show"]:
            return "{}"
        return arm.json.dumps(members)

    calls = []

    def repair(cluster, pool, _config, _identities, _count, _runner, _args, evidence):
        calls.append(cluster.role)
        evidence.update({
            "role": cluster.role, "pool": pool,
            "status": "reconciling" if mutation_started else "validating",
        })
        if cluster.role == "mesh-1":
            raise arm.ReconcileError("Cilium not healthy", evidence={"cilium_health": {"healthy": False}})
        evidence["status"] = "repaired"

    monkeypatch.setattr(arm, "read_quiescent_inventory", inventory)
    monkeypatch.setattr(arm, "run_command", runner)
    monkeypatch.setattr(arm, "reconcile_failed_pool", repair)
    assert arm.main([]) == 1
    assert calls == (["mesh-1"] if mutation_started else ["mesh-1", "mesh-2"])
    summary = arm.json.loads(Path(args.summary_file).read_text(encoding="utf-8"))
    assert summary["healthy"] is False
    assert summary["pool_repairs"][0]["failure_evidence"]["cilium_health"]["healthy"] is False
    assert summary["pool_repairs"][0]["mutation_started"] is mutation_started
    if not mutation_started:
        assert summary["pool_repairs"][1]["status"] == "repaired"


def test_inventory_rejects_active_operations_and_excess_failures():
    with pytest.raises(arm.ReconcileError, match="unsafe provisioningState"):
        arm.validate_cluster_inventory(
            [cluster_row(1), cluster_row(2, "Updating")],
            expected_count=2,
            region="eastus2euap",
            max_repair_clusters=1,
        )

    with pytest.raises(arm.ReconcileError, match="maximum is 1"):
        arm.validate_cluster_inventory(
            [cluster_row(1, "Failed"), cluster_row(2, "Failed")],
            expected_count=2,
            region="eastus2euap",
            max_repair_clusters=1,
        )


def quiescence_args(tmp_path):
    return arm.parse_args([
        "--resource-group", "12345-deadbeef",
        "--expected-subscription", "s",
        "--expected-region", "eastus2euap",
        "--expected-count", "2",
        "--expected-tfvars-sha", "expected-sha",
        "--summary-file", str(tmp_path / "summary.json"),
        "--quiescence-timeout-seconds", "10",
        "--poll-seconds", "4",
        "--inventory-retry-seconds", "1",
    ])


def fake_clock(monkeypatch):
    now = [0.0]
    monkeypatch.setattr(arm.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(
        arm.time, "sleep",
        lambda seconds: now.__setitem__(0, now[0] + seconds),
    )
    return now


@pytest.mark.parametrize("pool_state", sorted(arm.BUSY_POOL_STATES))
def test_pool_quiescence_waits_read_only_and_preserves_evidence(
    tmp_path, monkeypatch, pool_state,
):
    fake_clock(monkeypatch)
    calls = []

    def runner(command, timeout):
        calls.append((command, timeout))
        state = pool_state if len(calls) == 1 else "Succeeded"
        return arm.json.dumps([cluster_row(1), cluster_row(2, pool_state=state)])

    summary = {}
    clusters, failed = arm.read_quiescent_inventory(
        quiescence_args(tmp_path), summary, "initial", runner,
    )

    assert len(clusters) == 2 and failed == []
    assert len(calls) == 2
    assert all(command[:3] == ["az", "aks", "list"] for command, _ in calls)
    assert len(summary["initial_quiescence_observations"]) == 1
    reason = summary["initial_quiescence_observations"][0]["reason"]
    assert "mesh-2" in reason and "default" in reason and pool_state in reason
    saved = arm.json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert saved["initial_pool_states"][1]["pools"][0]["provisioningState"] == "Succeeded"


@pytest.mark.parametrize("pool_state", ["Failed", "Canceled", "Deleting", "Unknown"])
def test_terminal_or_unknown_pool_states_never_become_waitable(
    tmp_path, monkeypatch, pool_state,
):
    now = fake_clock(monkeypatch)
    calls = []

    def runner(command, _timeout):
        calls.append(command)
        return arm.json.dumps([cluster_row(1), cluster_row(2, pool_state=pool_state)])

    summary = {}
    with pytest.raises(arm.ReconcileError, match=pool_state) as failure:
        arm.read_quiescent_inventory(quiescence_args(tmp_path), summary, "initial", runner)
    assert not isinstance(failure.value, arm.InventoryBusyError)
    assert len(calls) == 1 and now[0] == 0
    assert summary["initial_quiescence_observations"] == []


def test_stopped_pool_does_not_wait_or_start_itself(tmp_path, monkeypatch):
    now = fake_clock(monkeypatch)
    payload = [cluster_row(1), cluster_row(2, pool_state="Updating")]
    payload[1]["agentPoolProfiles"][0]["powerState"] = {"code": "Stopped"}

    with pytest.raises(arm.ReconcileError, match="Stopped") as failure:
        arm.read_quiescent_inventory(
            quiescence_args(tmp_path), {}, "initial",
            lambda _command, _timeout: arm.json.dumps(payload),
        )
    assert not isinstance(failure.value, arm.InventoryBusyError)
    assert now[0] == 0


def test_quiescence_deadline_is_shared_by_reads_and_sleeps(tmp_path, monkeypatch):
    now = fake_clock(monkeypatch)
    limits = []

    def runner(_command, timeout):
        limits.append(timeout)
        return arm.json.dumps([cluster_row(1), cluster_row(2, pool_state="Updating")])

    with pytest.raises(arm.ReconcileError, match="within 10s"):
        arm.read_quiescent_inventory(quiescence_args(tmp_path), {}, "initial", runner)
    assert limits == [10, 6, 2]
    assert now[0] == 10


def test_quiescence_does_not_accept_success_after_deadline(tmp_path, monkeypatch):
    now = fake_clock(monkeypatch)

    def runner(_command, _timeout):
        now[0] = 11
        return arm.json.dumps([cluster_row(1), cluster_row(2)])

    with pytest.raises(arm.ReconcileError, match="after its deadline"):
        arm.read_quiescent_inventory(quiescence_args(tmp_path), {}, "initial", runner)


def test_quiescence_authorization_failure_is_immediate(tmp_path, monkeypatch):
    now = fake_clock(monkeypatch)
    calls = []

    def runner(command, _timeout):
        calls.append(command)
        raise arm.ReconcileError("AuthorizationFailed")

    with pytest.raises(arm.ReconcileError, match="AuthorizationFailed"):
        arm.read_quiescent_inventory(quiescence_args(tmp_path), {}, "initial", runner)
    assert len(calls) == 1 and now[0] == 0


def test_cluster_update_is_observed_before_quiescence(tmp_path, monkeypatch):
    fake_clock(monkeypatch)
    calls = []

    def runner(command, _timeout):
        calls.append(command)
        state = "Updating" if len(calls) == 1 else "Succeeded"
        return arm.json.dumps([cluster_row(1), cluster_row(2, state)])

    clusters, failed = arm.read_quiescent_inventory(
        quiescence_args(tmp_path), {}, "initial", runner,
    )
    assert len(clusters) == 2 and failed == [] and len(calls) == 2


def test_job_publishes_diagnostics_without_masking_reconcile_failure():
    job = (
        MODULE_PATH.parents[4] / "jobs/clustermesh-debug-resume.yml"
    ).read_text(encoding="utf-8")
    shared = "steps/topology/clustermesh-scale/reuse/reconcile-preserved-arm.yml"
    assert f"- template: /{shared}" in job
    assert "overlay_mode: ${{ parameters.overlay_mode }}" in job
    template = (
        MODULE_PATH.parents[4] / shared
    ).read_text(encoding="utf-8")
    start = template.index('      summary_dir="$(Build.ArtifactStagingDirectory)/n100-aks-arm-reconcile"')
    end = template.index('  - task: PublishPipelineArtifact@1', start)
    script = template[start:end]

    assert 'reconcile_rc=0' in script
    assert "CLUSTERMESH_DEBUG_FAILED_POOL_REPAIR_ENABLED" in script
    assert "pool_repair_args=(--failed-pool-repair-enabled)" in script
    assert "CLUSTERMESH_DEBUG_EARLY_LIVE_OVERLAY_REPAIR_ENABLED:-true" in script
    assert 'CLUSTERMESH_LIVE_DATA_PLANE_REPAIR_ENABLED:-false' in script
    assert 'CLUSTERMESH_FLEET_ENABLED:-true' in script
    assert '[ "$OVERLAY_MODE" = "resume-existing" ]' in script
    assert '[ "${{ parameters.expected_cluster_count }}" -eq 100 ]' in script
    assert "--live-overlay-repair-enabled" in script
    assert "--live-overlay-max-repair-roles" in script
    assert "CLUSTERMESH_DEBUG_EARLY_LIVE_OVERLAY_TIMEOUT_SECONDS:-18000" in script
    assert '--summary-file "$summary_dir/aks-arm-reconcile.json" || reconcile_rc=$?' in script
    assert 'if [ -s "$summary_dir/aks-arm-reconcile.json" ]; then' in script
    assert script.index("task.uploadfile") < script.index('exit "$reconcile_rc"')
    assert "AKS_ARM_RECONCILE_DIAGNOSTICS_READY]true" in script
    assert (
        "condition: and(succeededOrFailed(), "
        "eq(variables['AKS_ARM_RECONCILE_DIAGNOSTICS_READY'], 'true'))"
    ) in template
    assert 'artifact: "n100-aks-arm-reconcile-$(Build.BuildId)-$(System.JobAttempt)"' in template


@pytest.mark.parametrize("outcome", ["recovers", "never-recovers", "invalid-identity", "other-error"])
@pytest.mark.parametrize("phase", ["initial", "final"])
def test_fleet_health_is_observed_without_weakening_gates(tmp_path, monkeypatch, outcome, phase):
    args = arm.parse_args([
        "--resource-group", "12345-deadbeef",
        "--expected-subscription", "s",
        "--expected-region", "eastus2euap",
        "--expected-count", "2",
        "--expected-tfvars-sha", "expected",
        "--summary-file", str(tmp_path / "summary.json"),
    ])
    clusters, _ = arm.validate_cluster_inventory(
        [cluster_row(1), cluster_row(2)],
        expected_count=2, region="eastus2euap", max_repair_clusters=1,
    )
    calls = []
    sleeps = []
    monkeypatch.setattr(arm.time, "sleep", sleeps.append)

    def runner(command, _timeout):
        assert command[:4] == ["az", "fleet", "clustermeshprofile", "list-members"]
        calls.append(command)
        members = [{
            "name": f"mesh-{index}", "provisioningState": "Succeeded",
            "labels": {"mesh": "true"},
            "meshProperties": {"status": {"state": "Connected"}},
        } for index in (1, 2)]
        if outcome == "invalid-identity":
            members[1]["name"] = "mesh-3"
        elif outcome != "recovers" or len(calls) == 1:
            members[1]["meshProperties"]["status"] = {
                "state": "Disconnected",
                "error": {
                    "code": "OtherFailure" if outcome == "other-error" else "PartialConnectivity"
                },
            }
        return json.dumps(members)

    summary = {}
    if outcome == "recovers":
        members = arm.read_connected_fleet_members(args, clusters, summary, runner, phase=phase)
        arm.validate_fleet_members(members, clusters)
        assert len(calls) == 2 and len(sleeps) == 1
    else:
        with pytest.raises(arm.ReconcileError):
            arm.read_connected_fleet_members(args, clusters, summary, runner, phase=phase)
        assert len(calls) == (args.inventory_attempts if outcome == "never-recovers" else 1)
    saved = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert len(saved[f"{phase}_fleet_members"]) == 2
    if outcome != "invalid-identity":
        assert saved[f"{phase}_fleet_health_observations"][0]["unhealthy_members"][0]["name"] == "mesh-2"


def test_fleet_members_must_be_exactly_connected():
    clusters, _ = arm.validate_cluster_inventory(
        [cluster_row(1), cluster_row(2, "Failed")],
        expected_count=2,
        region="eastus2euap",
        max_repair_clusters=1,
    )
    members = [
        {
            "name": "mesh-1",
            "meshProperties": {"status": {"state": "Connected"}},
        },
        {
            "name": "mesh-2",
            "meshProperties": {"status": {"state": "Connected"}},
        },
    ]

    arm.validate_fleet_members(members, clusters)
    with pytest.raises(arm.ReconcileError, match="exact inventory"):
        arm.validate_fleet_members(members + [members[0]], clusters)
    members[1]["meshProperties"]["status"]["state"] = "Failed"
    with pytest.raises(arm.ReconcileError, match="not Connected"):
        arm.validate_fleet_members(members, clusters)


def test_latest_operation_requires_known_fleet_addon_error():
    cluster = arm.Cluster(
        name="clustermesh-2",
        role="mesh-2",
        resource_group="rg",
        resource_id="/subscriptions/s/resourceGroups/rg/clustermesh-2",
        node_resource_group="MC_rg_cluster_region",
        state="Failed",
        power_state="Running",
    )
    known = {
        "name": "operation-id",
        "operationType": "PutExtensionAddon",
        "status": "Failed",
        "error": {"code": "OverlaymgrReconcileError"},
        "startTime": "2026-09-02T22:09:39Z",
        "endTime": "2026-09-02T22:21:51Z",
    }

    evidence = arm.validate_latest_operation(cluster, known)
    assert evidence["operation_id"] == "operation-id"

    known["error"]["code"] = "OtherFailure"
    with pytest.raises(
        arm.ReconcileError,
        match="expected failed PutExtensionAddon/OverlaymgrReconcileError",
    ):
        arm.validate_latest_operation(cluster, known)


def test_data_plane_validation_uses_structured_cilium_status():
    cluster = arm.Cluster(
        name="clustermesh-2",
        role="mesh-2",
        resource_group="rg",
        resource_id="/subscriptions/s/resourceGroups/rg/clustermesh-2",
        node_resource_group="MC_rg_cluster_region",
        state="Failed",
        power_state="Running",
    )
    remotes = [
        {
            "name": "mesh-11",
            "ready": True,
            "connected": True,
            "config": {"required": True, "retrieved": True},
        }
    ]

    def runner(args, _timeout):
        if args[1:3] == ["aks", "get-credentials"]:
            return ""
        if args[0] == "kubectl" and "--raw=/readyz" in args:
            return "ok\n"
        if args[0] == "kubectl" and "deployment" in args:
            return (
                '{"status":{"conditions":['
                '{"type":"Available","status":"True"}]}}'
            )
        if args[0] == "kubectl" and "cilium-dbg" in args:
            return arm.json.dumps({"cluster-mesh": {"clusters": remotes}})
        raise AssertionError(f"unexpected command: {args}")

    arm.validate_cluster_data_plane(
        cluster,
        "/tmp/mesh-2.config",
        1,
        runner,
        query_timeout_seconds=5,
    )
    remotes[0]["config"]["retrieved"] = False
    with pytest.raises(arm.ReconcileError, match="unhealthy Cilium remotes"):
        arm.validate_cluster_data_plane(
            cluster,
            "/tmp/mesh-2.config",
            1,
            runner,
            query_timeout_seconds=5,
        )


def test_reconcile_cluster_waits_for_succeeded(monkeypatch):
    cluster = arm.Cluster(
        name="clustermesh-2",
        role="mesh-2",
        resource_group="12345-deadbeef",
        resource_id=(
            "/subscriptions/s/resourceGroups/12345-deadbeef/providers/"
            "Microsoft.ContainerService/managedClusters/clustermesh-2"
        ),
        node_resource_group="MC_rg_cluster_region",
        state="Failed",
        power_state="Running",
    )
    states = iter(
        [
            '{"state":"Failed","power":"Running"}',
            '{"state":"Updating","power":"Running"}',
            '{"state":"Succeeded","power":"Running"}',
        ]
    )
    commands = []

    def runner(args, _timeout):
        commands.append(list(args))
        if args[1:3] == ["aks", "update"]:
            return ""
        if args[1:3] == ["aks", "show"]:
            return next(states)
        raise AssertionError(f"unexpected command: {args}")

    monkeypatch.setattr(arm.time, "sleep", lambda _seconds: None)
    result = arm.reconcile_cluster(
        cluster,
        runner,
        query_timeout_seconds=5,
        mutation_timeout_seconds=5,
        recovery_timeout_seconds=30,
        poll_seconds=1,
        submit_attempts=2,
    )

    assert result.status == "repaired"
    assert result.observed_states == ["Failed", "Updating", "Succeeded"]
    assert sum(command[1:3] == ["aks", "update"] for command in commands) == 1
