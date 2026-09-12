"""Contract tests for explicit ARM/Fleet-only maintenance routing."""

import os
import subprocess
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]


def load(relative_path):
    return yaml.safe_load((ROOT / relative_path).read_text(encoding="utf-8"))


@pytest.mark.parametrize("count,mode,retirement_only,enabled,expected", [
    ("100", "resume-existing", "false", "true", 0),
    ("2", "resume-existing", "false", "true", 1),
    ("100", "resume", "false", "true", 1),
    ("100", "resume-existing", "true", "true", 1),
    ("100", "resume-existing", "false", "false", 1),
])
def test_arm_only_requires_exact_existing_n100_scope(
    count, mode, retirement_only, enabled, expected
):
    job = load("jobs/clustermesh-arm-repair.yml")["jobs"][0]
    result = subprocess.run(
        ["bash", "-c", job["steps"][0]["script"]],
        env=dict(
            os.environ, EXPECTED_CLUSTER_COUNT=count, OVERLAY_MODE=mode,
            RETIREMENT_ONLY=retirement_only, CNI_MAINTENANCE_ONLY="false",
            CLUSTERMESH_PRESERVED_AKS_ARM_RECONCILE_ENABLED=enabled,
        ),
        capture_output=True, text=True, check=False, timeout=10,
    )
    assert result.returncode == expected
    assert "command not found" not in result.stderr


def test_retirement_only_rejects_concurrent_arm_only_mode():
    job = load("jobs/clustermesh-prepared-worker-retirement.yml")["jobs"][0]
    result = subprocess.run(
        ["bash", "-c", job["steps"][0]["script"]],
        env=dict(
            os.environ, EXPECTED_CLUSTER_COUNT="100", ARM_REPAIR_ONLY="true",
            RETIREMENT_ROLE="mesh-38", RETIREMENT_NODE="worker", RETIREMENT_UID="uid",
        ),
        capture_output=True, text=True, check=False, timeout=10,
    )
    assert result.returncode == 1


def test_arm_only_reuses_normal_reconciliation_without_workload_validation():
    repair = load("jobs/clustermesh-arm-repair.yml")["jobs"][0]
    resume = load("jobs/clustermesh-debug-resume.yml")["jobs"][0]
    shared = "/steps/topology/clustermesh-scale/reuse/reconcile-preserved-arm.yml"
    assert [step["template"] for step in repair["steps"] if "template" in step] == [
        "/steps/setup-tests.yml", shared,
    ]
    assert any(step.get("template") == shared for step in resume["steps"])
    assert repair["variables"]["AKS_CONTROL_PLANE_METRICS_ENABLED"] == "false"
    assert "ne(variables['CLUSTERMESH_ARM_REPAIR_ONLY'], 'true')" in resume["condition"]
    body = load(shared.lstrip("/"))["steps"]
    assert body[0]["displayName"] == "Reconcile stale preserved AKS ARM states"
    script = body[0]["script"]
    assert "--live-overlay-repair-enabled" in script
    assert '"$OVERLAY_MODE" = "resume-existing"' in script
    assert '"${{ parameters.expected_cluster_count }}" -eq 100' in script
    assert "reconcile_rc=$?" in script
    assert 'exit "$reconcile_rc"' in script
    assert body[1]["task"] == "PublishPipelineArtifact@1"
    assert "succeededOrFailed()" in body[1]["condition"]


def test_only_selected_resume_stage_exposes_arm_only_job():
    pipeline = load("pipelines/system/new-pipeline-test.yml")
    parameter = next(
        item for item in pipeline["parameters"] if item["name"] == "scaleDebugArmRepairOnly"
    )
    assert parameter["type"] == "boolean" and parameter["default"] is False
    stage = next(
        item for item in pipeline["stages"]
        if item.get("stage") == "azure_eastus2euap_n100_debug_resume_37deca"
    )
    assert stage["variables"]["CLUSTERMESH_ARM_REPAIR_ONLY"] == (
        "${{ parameters.scaleDebugArmRepairOnly }}"
    )
    condition = (
        "${{ if and(eq(parameters.scaleDebugDv3QuotaRequestLimit, 0), eq(parameters.scaleDebugQuotaRequestReceiptBuildId, 0), parameters.scaleDebugArmRepairOnly, "
        "not(parameters.scaleDebugPreparedRetirementObserveOnly), "
        "not(parameters.scaleDebugUnreachableWorkerRecoveryOnly), "
        "eq(parameters.scaleDebugUnreachableWorkerReplaceFailedHostBuildId, 0), "
        "eq(parameters.scaleDebugUnreachableWorkerResumeReplacementBuildId, 0), "
        "not(parameters.scaleDebugUnreachableWorkerQuotaObserveOnly)) }}"
    )
    entry = next(item for item in stage["jobs"] if condition in item)
    maintenance = entry[condition][0]
    assert maintenance["template"] == "/jobs/clustermesh-arm-repair.yml"
    assert maintenance["parameters"]["target_run_id"] == "${{ parameters.debugTargetRunId }}"
    assert maintenance["parameters"]["expected_subscription_id"] == (
        "${{ parameters.lifecycleSubscriptionId }}"
    )
    assert maintenance["parameters"]["overlay_mode"] == "${{ parameters.debugMode }}"
