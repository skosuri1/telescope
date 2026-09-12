"""Exclusive routing and one-request shell checks for the bounded quota job."""

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]
JOB = ROOT / "jobs/clustermesh-quota-request.yml"
ENVIRONMENT = {
    "RUN_ID": "78751-f36f3d5a", "CONFIRM_RESUME": "78751-f36f3d5a",
    "SUBSCRIPTION": "37deca37-c375-4a14-b90a-043849bd2bf1", "REGION": "eastus2euap",
    "CLUSTER_COUNT": "100", "OVERLAY_MODE": "resume-existing", "RUN_WORKLOAD": "False",
    "QUOTA_LIMIT": "5500", "NATIVE_BUILD_ID": "79894",
    "RECOVERY_ONLY": "False", "QUOTA_OBSERVE_ONLY": "False", "ARM_ONLY": "False",
    "RETIREMENT_ONLY": "False", "RETIREMENT_OBSERVE_ONLY": "False", "CNI_ONLY": "False",
    "RECEIPT_BUILD_ID": "0",
}


def job():
    return yaml.safe_load(JOB.read_text(encoding="utf-8"))["jobs"][0]


def operation():
    return next(row for row in job()["steps"] if row.get("displayName") ==
                "Plan then submit at most one normal quota increase request")


@pytest.mark.parametrize("changes", [
    {}, {"SUBSCRIPTION": "other"}, {"RUN_ID": "79825-24946a3a"},
    {"CONFIRM_RESUME": ""}, {"REGION": "eastus"}, {"CLUSTER_COUNT": "2"},
    {"OVERLAY_MODE": "resume"}, {"RUN_WORKLOAD": "True"}, {"QUOTA_LIMIT": "6000"},
    {"NATIVE_BUILD_ID": "0"}, {"RECOVERY_ONLY": "True"}, {"QUOTA_OBSERVE_ONLY": "True"},
    {"ARM_ONLY": "True"}, {"RETIREMENT_ONLY": "True"}, {"RETIREMENT_OBSERVE_ONLY": "True"},
    {"CNI_ONLY": "True"},
])
def test_request_requires_exact_scope_limit_and_no_other_modes(changes):
    values = {**ENVIRONMENT, **changes}
    scope = {
        "target_run_id": values["RUN_ID"], "confirm_resume": values["CONFIRM_RESUME"],
        "expected_subscription_id": values["SUBSCRIPTION"], "expected_region": values["REGION"],
        "expected_cluster_count": int(values["CLUSTER_COUNT"]), "overlay_mode": values["OVERLAY_MODE"],
        "run_workload": values["RUN_WORKLOAD"].lower() == "true", "quota_limit": int(values["QUOTA_LIMIT"]),
        "native_build_id": int(values["NATIVE_BUILD_ID"]),
        "receipt_build_id": int(values["RECEIPT_BUILD_ID"]),
        "exclusive_modes": all(values[key].lower() == "false" for key in (
            "RECOVERY_ONLY", "QUOTA_OBSERVE_ONLY", "ARM_ONLY", "RETIREMENT_ONLY",
            "RETIREMENT_OBSERVE_ONLY", "CNI_ONLY",
        )),
    }
    result = subprocess.run(
        ["bash", "-c", job()["steps"][0]["script"]],
        env={**os.environ, "QUOTA_SCOPE_JSON": json.dumps(scope)},
        text=True, capture_output=True, check=False, timeout=10,
    )
    assert (result.returncode == 0) is (not changes), result.stderr
    assert "unbound variable" not in result.stderr


def test_quota_mode_is_exclusive_and_does_not_allocate_capacity():
    pipeline = yaml.safe_load((ROOT / "pipelines/system/new-pipeline-test.yml").read_text(encoding="utf-8"))
    parameters = {row["name"]: row for row in pipeline["parameters"]}
    assert parameters["scaleDebugDv3QuotaRequestLimit"]["default"] == 0
    assert parameters["scaleDebugDv3QuotaRequestLimit"]["values"] == [0, 5500]
    stage = next(row for row in pipeline["stages"] if row.get("stage") == "azure_eastus2euap_n100_debug_resume_37deca")
    key = "${{ if or(eq(parameters.scaleDebugDv3QuotaRequestLimit, 5500), ne(parameters.scaleDebugQuotaRequestReceiptBuildId, 0)) }}"
    invocation = next(row[key][0] for row in stage["jobs"] if key in row)
    assert invocation["template"] == "/jobs/clustermesh-quota-request.yml"
    assert invocation["parameters"]["native_build_id"] == "${{ parameters.scaleDebugUnreachableWorkerResumeReplacementBuildId }}"
    assert invocation["parameters"]["confirm_resume"] == "${{ parameters.debugConfirmResume }}"
    assert job()["steps"][0]["env"] == {"QUOTA_SCOPE_JSON": "${{ convertToJson(parameters) }}"}
    for parameter in ("scaleDebugUnreachableWorkerRecoveryOnly", "scaleDebugUnreachableWorkerQuotaObserveOnly",
                      "scaleDebugArmRepairOnly", "scaleDebugPreparedRetirementOnly",
                      "scaleDebugPreparedRetirementObserveOnly", "scaleDebugCniWorkerMaintenanceOnly"):
        assert f"not(parameters.{parameter})" in invocation["parameters"]["exclusive_modes"]
    for row in stage["jobs"][:4]:
        assert "eq(parameters.scaleDebugDv3QuotaRequestLimit, 0)" in next(iter(row))
        assert "eq(parameters.scaleDebugQuotaRequestReceiptBuildId, 0)" in next(iter(row))
    normal = yaml.safe_load((ROOT / "jobs/clustermesh-debug-resume.yml").read_text(encoding="utf-8"))["jobs"][0]
    assert "eq(variables['CLUSTERMESH_DV3_QUOTA_REQUEST_LIMIT'], '0')" in normal["condition"]
    steps = job()["steps"]
    assert steps[1]["parameters"]["credential_type"] == "service_connection"
    assert steps[2]["task"] == "DownloadPipelineArtifact@2"
    assert steps[2]["inputs"]["definition"] == "$(System.DefinitionId)"
    assert not any("clusterloader2" in row.get("displayName", "").lower() for row in steps)
    assert operation()["retryCountOnTaskFailure"] == 0
    assert steps[-1]["task"] == "PublishPipelineArtifact@1"


@pytest.mark.parametrize("fault,calls_expected,return_code", [
    ("none", 2, 0), ("plan", 1, 7), ("unsafe-plan", 1, 1),
    ("changed-source", 1, 1), ("execute", 2, 8),
    ("inspect", 1, 0), ("missing-journal", 0, 1),
])
def test_script_plans_then_requests_once_from_unchanged_receipt(tmp_path, fault, calls_expected, return_code):
    script = operation()["script"]
    script = script.replace("$(Build.ArtifactStagingDirectory)", str(tmp_path / "artifacts"))
    script = script.replace("$(Pipeline.Workspace)", str(tmp_path / "workspace"))
    native = tmp_path / "workspace" / "native-quota-source-79894"
    native.mkdir(parents=True)
    (native / "recovery.json").write_text('{"checkpoint":"immutable"}', encoding="utf-8")
    calls_file = tmp_path / "calls.jsonl"
    fake = tmp_path / "python3"
    fake.write_text(
        f"#!{sys.executable}\n" + textwrap.dedent(
            """
            import json
            import os
            import sys
            from pathlib import Path
            args = sys.argv[1:]
            with Path(os.environ["CALLS"]).open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(args) + "\\n")
            execute = "--execute" in args
            fault = os.environ["FAULT"]
            Path(args[args.index("--summary-file") + 1]).write_text(json.dumps({
                "execute": execute, "mutation_started": fault == "unsafe-plan", "plan_valid": True,
            }), encoding="utf-8")
            if fault == "changed-source":
                Path(args[args.index("--native-checkpoint") + 1]).write_text("{}", encoding="utf-8")
            if fault == "plan" and not execute:
                sys.exit(7)
            if fault == "execute" and execute:
                sys.exit(8)
            """
        ),
        encoding="utf-8",
    )
    fake.chmod(0o755)
    environment = {**os.environ, **ENVIRONMENT, "PATH": f"{tmp_path}:{os.environ['PATH']}",
                   "CALLS": str(calls_file), "FAULT": fault}
    if fault in ("inspect", "missing-journal"):
        environment["RECEIPT_BUILD_ID"] = "79908"
        if fault == "inspect":
            prior = tmp_path / "workspace" / "prior-quota-request-79908"
            prior.mkdir(parents=True)
            (prior / "request_mesh96_quota.attempt.json").write_text('{"request":"keep"}', encoding="utf-8")
    result = subprocess.run(
        ["bash", "-c", script],
        env=environment,
        text=True, capture_output=True, check=False, timeout=10,
    )
    assert result.returncode == return_code, result.stderr
    calls = [json.loads(line) for line in calls_file.read_text(encoding="utf-8").splitlines()] if calls_file.exists() else []
    assert len(calls) == calls_expected
    if calls:
        assert "--execute" not in calls[0] and calls[0][1].endswith("/request_mesh96_quota.py")
        assert calls[0][calls[0].index("--expected-subscription") + 1] == ENVIRONMENT["SUBSCRIPTION"]
    if fault == "inspect":
        assert "--inspect-rejection" in calls[0] and len(calls) == 1
        copied = tmp_path / "artifacts" / "n100-quota-request" / "request_mesh96_quota.attempt.json"
        assert json.loads(copied.read_text(encoding="utf-8")) == {"request": "keep"}
    if len(calls) == 2:
        assert "--execute" in calls[1]
        assert calls[0][:calls[0].index("--summary-file")] == calls[1][:calls[1].index("--summary-file")]
