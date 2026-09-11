"""Tests for bounded preserved AKS ARM state reconciliation."""

import importlib.util
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
    template = (
        MODULE_PATH.parents[4] / "jobs/clustermesh-debug-resume.yml"
    ).read_text(encoding="utf-8")
    start = template.index('      summary_dir="$(Build.ArtifactStagingDirectory)/n100-aks-arm-reconcile"')
    end = template.index('  - task: PublishPipelineArtifact@1', start)
    script = template[start:end]

    assert 'reconcile_rc=0' in script
    assert '--summary-file "$summary_dir/aks-arm-reconcile.json" || reconcile_rc=$?' in script
    assert 'if [ -s "$summary_dir/aks-arm-reconcile.json" ]; then' in script
    assert script.index("task.uploadfile") < script.index('exit "$reconcile_rc"')
    assert "AKS_ARM_RECONCILE_DIAGNOSTICS_READY]true" in script
    assert (
        "condition: and(succeededOrFailed(), "
        "eq(variables['AKS_ARM_RECONCILE_DIAGNOSTICS_READY'], 'true'))"
    ) in template
    assert 'artifact: "n100-aks-arm-reconcile-$(Build.BuildId)-$(System.JobAttempt)"' in template


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
