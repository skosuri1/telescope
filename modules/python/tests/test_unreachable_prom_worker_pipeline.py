"""Exercise the scoped host recovery pipeline without Azure or Kubernetes."""

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import yaml


REPOSITORY = Path(__file__).resolve().parents[3]
JOB = "jobs/clustermesh-unreachable-worker-recovery.yml"
STEP = "steps/topology/clustermesh-scale/reuse/recover-unreachable-worker.yml"
PLAN = {
    "schema_version": 1,
    "role": "mesh-96",
    "node_name": "aks-prompool-test-vmss000000",
    "node_uid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
}
ENVIRONMENT = {
    "EXPECTED_CLUSTER_COUNT": "100",
    "OVERLAY_MODE": "resume-existing",
    "RUN_WORKLOAD": "False",
    "RECOVERY_ONLY": "True",
    "ARM_REPAIR_ONLY": "False",
    "RETIREMENT_ONLY": "False",
    "OBSERVE_ONLY": "False",
    "CNI_MAINTENANCE_ONLY": "False",
    "RECOVERY_PLAN_JSON": json.dumps(PLAN),
    "RUN_ID": "78751-f36f3d5a",
    "CONFIRM_RESUME": "78751-f36f3d5a",
    "OBSERVE_BUILD_ID": "0",
    "REPLACE_FAILED_HOST_BUILD_ID": "0",
    "RESUME_REPLACEMENT_BUILD_ID": "0",
    "QUOTA_OBSERVE_ONLY": "False",
    "MODERN_PROM_RECOVERY": "False",
    "REIMAGE_FAILED_OS": "False",
}


def template(path):
    """Read the actual checked-in template."""

    return yaml.safe_load((REPOSITORY / path).read_text(encoding="utf-8"))


@pytest.mark.parametrize("changes", [
    {},
    {"EXPECTED_CLUSTER_COUNT": "2"},
    {"OVERLAY_MODE": "resume"},
    {"RUN_WORKLOAD": "True"},
    {"RECOVERY_ONLY": "False"},
    {"ARM_REPAIR_ONLY": "True"},
    {"RETIREMENT_ONLY": "True"},
    {"OBSERVE_ONLY": "True"},
    {"CNI_MAINTENANCE_ONLY": "True"},
    {"RECOVERY_PLAN_JSON": ""},
    {"RECOVERY_PLAN_JSON": "not-json"},
    {"RECOVERY_PLAN_JSON": "[]"},
    {"RECOVERY_PLAN_JSON": json.dumps({**PLAN, "schema_version": 2})},
    {"RECOVERY_PLAN_JSON": "x" * 32769},
    {"OBSERVE_BUILD_ID": "-1"},
    {"OBSERVE_BUILD_ID": "79880"},
    {"REPLACE_FAILED_HOST_BUILD_ID": "-1"},
    {"REPLACE_FAILED_HOST_BUILD_ID": "079880"},
    {"REPLACE_FAILED_HOST_BUILD_ID": "79880", "REIMAGE_FAILED_OS": "True"},
    {"REPLACE_FAILED_HOST_BUILD_ID": "79880", "OBSERVE_BUILD_ID": "79880", "REIMAGE_FAILED_OS": "True"},
    {"REPLACE_FAILED_HOST_BUILD_ID": "79880", "RECOVERY_ONLY": "False"},
    {"REPLACE_FAILED_HOST_BUILD_ID": "79880", "RUN_WORKLOAD": "True"},
    {"RESUME_REPLACEMENT_BUILD_ID": "-1"},
    {"QUOTA_OBSERVE_ONLY": "True"},
    {"RESUME_REPLACEMENT_BUILD_ID": "79894", "QUOTA_OBSERVE_ONLY": "True"},
    {"MODERN_PROM_RECOVERY": "True"},
    {"MODERN_PROM_RECOVERY": "True", "RESUME_REPLACEMENT_BUILD_ID": "79894",
     "REPLACE_FAILED_HOST_BUILD_ID": "79880", "QUOTA_OBSERVE_ONLY": "True"},
])
def test_recovery_job_requires_complete_exclusive_mode(changes):
    result = subprocess.run(
        ["bash", "-c", template(JOB)["jobs"][0]["steps"][0]["script"]],
        env={**os.environ, **ENVIRONMENT, **changes},
        text=True, capture_output=True, check=False, timeout=10,
    )
    assert (result.returncode == 0) is (not changes), result.stderr
    assert "unbound variable" not in result.stderr


@pytest.mark.parametrize("changes", [
    {"REPLACE_FAILED_HOST_BUILD_ID": "79880"},
    {"REPLACE_FAILED_HOST_BUILD_ID": "79880", "RECOVERY_PLAN_JSON": ""},
    {"OBSERVE_BUILD_ID": "79880", "REIMAGE_FAILED_OS": "True"},
    {"RESUME_REPLACEMENT_BUILD_ID": "79894", "REPLACE_FAILED_HOST_BUILD_ID": "79880",
     "QUOTA_OBSERVE_ONLY": "True", "RECOVERY_PLAN_JSON": ""},
    {"RESUME_REPLACEMENT_BUILD_ID": "79894", "REPLACE_FAILED_HOST_BUILD_ID": "79880",
     "RECOVERY_PLAN_JSON": ""},
    {"RESUME_REPLACEMENT_BUILD_ID": "79894", "REPLACE_FAILED_HOST_BUILD_ID": "79880",
     "MODERN_PROM_RECOVERY": "True", "RECOVERY_PLAN_JSON": ""},
])
def test_recovery_job_accepts_distinct_checkpoint_modes(changes):
    result = subprocess.run(
        ["bash", "-c", template(JOB)["jobs"][0]["steps"][0]["script"]],
        env={**os.environ, **ENVIRONMENT, **changes},
        text=True, capture_output=True, check=False, timeout=10,
    )
    assert result.returncode == 0, result.stderr


def test_recovery_mode_excludes_other_mutation_paths():
    pipeline = template("pipelines/system/new-pipeline-test.yml")
    parameters = {row["name"]: row for row in pipeline["parameters"]}
    assert parameters["scaleDebugUnreachableWorkerRecoveryOnly"]["default"] is False
    assert parameters["scaleDebugUnreachableWorkerPlanJson"]["default"] == ""
    assert parameters["scaleDebugUnreachableWorkerReplaceFailedHostBuildId"]["default"] == 0
    stage = next(
        row for row in pipeline["stages"]
        if row.get("stage") == "azure_eastus2euap_n100_debug_resume_37deca"
    )
    key = (
        "${{ if and(eq(parameters.scaleDebugQualifiedWorkerRetirementBuildId, 0), eq(parameters.scaleDebugCapacityQualificationBuildId, 0), eq(parameters.scaleDebugCapacityFirstRecoveryBuildId, 0), eq(parameters.scaleDebugRetainedWorkerRestartBuildId, 0), eq(parameters.scaleDebugModernCniPromBuildId, 0), eq(parameters.scaleDebugDv3QuotaRequestLimit, 0), "
        "eq(parameters.scaleDebugQuotaRequestReceiptBuildId, 0), "
        "or(parameters.scaleDebugModernPromRecovery, ne(parameters.scaleDebugUnreachableWorkerReplaceFailedHostBuildId, 0), "
        "ne(parameters.scaleDebugUnreachableWorkerResumeReplacementBuildId, 0), "
        "parameters.scaleDebugUnreachableWorkerQuotaObserveOnly, "
        "and(parameters.scaleDebugUnreachableWorkerRecoveryOnly, "
        "not(parameters.scaleDebugPreparedRetirementObserveOnly)))) }}"
    )
    invocation = next(row[key][0] for row in stage["jobs"] if key in row)
    assert invocation["template"] == f"/{JOB}"
    assert invocation["parameters"]["plan_json"] == "${{ parameters.scaleDebugUnreachableWorkerPlanJson }}"
    assert invocation["parameters"]["run_workload"] == "${{ parameters.scaleDebugRunWorkload }}"
    assert invocation["parameters"]["reimage_failed_os"] == "${{ parameters.scaleDebugUnreachableWorkerReimageFailedOs }}"
    assert invocation["parameters"]["replace_failed_host_build_id"] == (
        "${{ parameters.scaleDebugUnreachableWorkerReplaceFailedHostBuildId }}"
    )
    for key in stage["jobs"][1:3]:
        assert "not(parameters.scaleDebugUnreachableWorkerRecoveryOnly)" in next(iter(key))
    for key in stage["jobs"][:3]:
        assert "not(parameters.scaleDebugModernPromRecovery)" in next(iter(key))
        assert "eq(parameters.scaleDebugUnreachableWorkerReplaceFailedHostBuildId, 0)" in next(iter(key))
        assert "eq(parameters.scaleDebugUnreachableWorkerResumeReplacementBuildId, 0)" in next(iter(key))
        assert "not(parameters.scaleDebugUnreachableWorkerQuotaObserveOnly)" in next(iter(key))
    normal = template("jobs/clustermesh-debug-resume.yml")["jobs"][0]
    assert "ne(variables['CLUSTERMESH_UNREACHABLE_WORKER_RECOVERY_ONLY'], 'true')" in normal["condition"]
    assert "ne(variables['CLUSTERMESH_MODERN_PROM_RECOVERY'], 'true')" in normal["condition"]
    assert "eq(variables['CLUSTERMESH_UNREACHABLE_WORKER_REPLACE_FAILED_HOST_BUILD_ID'], '0')" in normal["condition"]
    assert "eq(variables['CLUSTERMESH_UNREACHABLE_WORKER_RESUME_REPLACEMENT_BUILD_ID'], '0')" in normal["condition"]
    assert "ne(variables['CLUSTERMESH_UNREACHABLE_WORKER_QUOTA_OBSERVE_ONLY'], 'true')" in normal["condition"]
    assert stage["variables"]["CLUSTERMESH_UNREACHABLE_WORKER_REPLACE_FAILED_HOST_BUILD_ID"] == (
        "${{ parameters.scaleDebugUnreachableWorkerReplaceFailedHostBuildId }}"
    )
    job = template(JOB)["jobs"][0]
    assert job["variables"]["SCENARIO_NAME"] == "clustermesh-scale"
    assert [row.get("template") for row in job["steps"] if "template" in row] == [
        "/steps/setup-tests.yml",
    ]
    normal_key = "${{ if not(parameters.quota_observe_only) }}"
    observe_key = "${{ if parameters.quota_observe_only }}"
    assert next(row[normal_key][0] for row in job["steps"] if normal_key in row)["template"] == f"/{STEP}"
    observation = next(row[observe_key][0] for row in job["steps"] if observe_key in row)
    assert observation["template"] == "/steps/topology/clustermesh-scale/reuse/observe-native-prom-quota.yml"
    assert observation["parameters"]["native_build_id"] == "${{ parameters.resume_replacement_build_id }}"
    assert job["steps"][1]["parameters"]["credential_type"] == "service_connection"
    assert job["steps"][1]["parameters"]["ssh_key_enabled"] is False
    assert template(STEP)["steps"][0]["retryCountOnTaskFailure"] == 0
    assert "always()" in template(STEP)["steps"][1]["condition"]
    replacement_key = "${{ if gt(parameters.replace_failed_host_build_id, 0) }}"
    download = next(row[replacement_key][0] for row in job["steps"] if replacement_key in row)
    assert download["task"] == "DownloadPipelineArtifact@2"
    assert download["inputs"]["pipelineId"] == "${{ parameters.replace_failed_host_build_id }}"
    assert download["inputs"]["definition"] == "$(System.DefinitionId)"


@pytest.mark.parametrize("failure,expected_calls,expected_code", [
    ("none", 2, 0),
    ("plan", 1, 7),
    ("unsafe-plan", 1, 1),
    ("mutate-plan", 1, 1),
    ("execute", 2, 8),
    ("confirm", 0, 1),
    ("malformed", 0, 1),
    ("observe", 1, 0),
    ("missing-checkpoint", 0, 1),
    ("replace", 2, 0),
    ("missing-replacement-checkpoint", 0, 1),
    ("mutate-checkpoint", 1, 1),
    ("artifact-plan", 2, 0),
    ("missing-artifact-plan", 0, 1),
    ("resume-native", 2, 0),
    ("missing-native-checkpoint", 0, 1),
    ("mutate-native-checkpoint", 1, 1),
    ("modern-prom", 2, 0),
])
@pytest.mark.parametrize("reimage_failed_os", ["False", "True"])
def test_recovery_step_plans_before_exact_execution(
    tmp_path, failure, expected_calls, expected_code, reimage_failed_os,
):
    script = template(STEP)["steps"][0]["script"]
    script = script.replace("$(Build.ArtifactStagingDirectory)", str(tmp_path / "artifacts"))
    script = script.replace("$(Pipeline.Workspace)/s", str(REPOSITORY))
    script = script.replace("$(Pipeline.Workspace)", str(tmp_path / "workspace"))
    script = script.replace(
        "${{ parameters.tfvars_path }}",
        "scenarios/perf-eval/clustermesh-scale/terraform-inputs/azure-100-mock-shared.tfvars",
    )
    script = script.replace("${{ parameters.expected_subscription_id }}", "test-subscription")
    script = script.replace("${{ parameters.expected_region }}", "eastus2euap")
    calls_file = tmp_path / "calls.jsonl"
    fake_python = tmp_path / "python3"
    fake_python.write_text(
        f"#!{sys.executable}\n" + textwrap.dedent(
            """
            import json
            import os
            import sys
            from pathlib import Path

            args = sys.argv[1:]
            execute = "--execute" in args
            failure = os.environ["FAKE_FAILURE"]
            with open(os.environ["CALLS_FILE"], "a", encoding="utf-8") as handle:
                handle.write(json.dumps(args) + "\\n")
            summary = Path(args[args.index("--summary-file") + 1])
            summary.write_text(json.dumps({
                "execute": execute,
                "mutation_started": failure == "unsafe-plan",
                "plan_valid": not execute,
                "success": True,
            }), encoding="utf-8")
            if failure == "mutate-plan" and not execute:
                plan = Path(args[args.index("--plan-file") + 1])
                plan.write_text("{}", encoding="utf-8")
            if failure == "mutate-checkpoint" and not execute:
                checkpoint = Path(args[args.index("--replace-failed-host") + 1])
                checkpoint.write_text("{}", encoding="utf-8")
            if failure == "mutate-native-checkpoint" and not execute:
                checkpoint = Path(args[args.index("--resume-replacement") + 1])
                checkpoint.write_text("{}", encoding="utf-8")
            if failure == "plan" and not execute:
                sys.exit(7)
            if failure == "execute" and execute:
                sys.exit(8)
            """
        ),
        encoding="utf-8",
    )
    fake_python.chmod(0o755)
    environment = {
        **os.environ, **ENVIRONMENT, "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "CALLS_FILE": str(calls_file), "FAKE_FAILURE": failure,
        "REIMAGE_FAILED_OS": reimage_failed_os,
    }
    if failure == "confirm":
        environment["CONFIRM_RESUME"] = "different"
    if failure == "malformed":
        environment["RECOVERY_PLAN_JSON"] = "[]"
    if failure in ("observe", "missing-checkpoint"):
        environment["OBSERVE_BUILD_ID"] = "79880"
        if reimage_failed_os == "False":
            expected_calls, expected_code = 0, 1
        if failure == "observe":
            checkpoint = tmp_path / "workspace" / "accepted-host-action-79880" / "recovery.json"
            checkpoint.parent.mkdir(parents=True)
            checkpoint.write_text('{"accepted": true}', encoding="utf-8")
    replacing = failure in (
        "replace", "missing-replacement-checkpoint", "mutate-checkpoint",
        "artifact-plan", "missing-artifact-plan",
        "resume-native", "missing-native-checkpoint", "mutate-native-checkpoint",
        "modern-prom",
    )
    if replacing:
        environment["REPLACE_FAILED_HOST_BUILD_ID"] = "79880"
        if reimage_failed_os == "True":
            expected_calls, expected_code = 0, 1
        if failure != "missing-replacement-checkpoint":
            checkpoint = tmp_path / "workspace" / "accepted-failed-host-79880" / "recovery.json"
            checkpoint.parent.mkdir(parents=True)
            checkpoint.write_text('{"accepted": true}', encoding="utf-8")
            if failure == "artifact-plan":
                (checkpoint.parent / "input-plan.json").write_text(json.dumps(PLAN), encoding="utf-8")
        if failure in ("artifact-plan", "missing-artifact-plan"):
            environment["RECOVERY_PLAN_JSON"] = ""
        if failure in ("resume-native", "missing-native-checkpoint", "mutate-native-checkpoint", "modern-prom"):
            environment["RESUME_REPLACEMENT_BUILD_ID"] = "79894"
            if failure != "missing-native-checkpoint":
                native = tmp_path / "workspace" / "native-host-action-79894" / "recovery.json"
                native.parent.mkdir(parents=True)
                native.write_text('{"native_checkpoint": true}', encoding="utf-8")
        if failure == "modern-prom":
            environment["MODERN_PROM_RECOVERY"] = "True"
    result = subprocess.run(
        ["bash", "-c", script], env=environment, capture_output=True,
        text=True, check=False, timeout=10,
    )
    assert result.returncode == expected_code, result.stderr
    calls = [
        json.loads(line) for line in calls_file.read_text(encoding="utf-8").splitlines()
    ] if calls_file.exists() else []
    assert len(calls) == expected_calls
    if calls:
        assert "--execute" not in calls[0]
        assert ("--reimage-failed-os" in calls[0]) is (reimage_failed_os == "True")
        assert calls[0][0].endswith("/unreachable_prom_worker_recovery.py")
        assert calls[0][calls[0].index("--resource-group") + 1] == ENVIRONMENT["RUN_ID"]
        assert calls[0][calls[0].index("--expected-subscription") + 1] == "test-subscription"
        assert calls[0][calls[0].index("--timeout-seconds") + 1] == ("3600" if replacing else "1800")
        plan_path = Path(calls[0][calls[0].index("--plan-file") + 1])
        assert plan_path.stat().st_mode & 0o777 == 0o600
        if failure != "mutate-plan":
            assert json.loads(plan_path.read_text(encoding="utf-8")) == PLAN
        if failure == "observe":
            assert "--observe-accepted-action" in calls[0]
            assert len(calls) == 1 and "--execute" not in calls[0]
        if replacing:
            assert "--replace-failed-host" in calls[0]
            assert "--observe-accepted-action" not in calls[0]
            assert "--reimage-failed-os" not in calls[0]
        if failure in ("resume-native", "mutate-native-checkpoint", "modern-prom"):
            assert "--resume-replacement" in calls[0]
        if failure == "modern-prom":
            assert "--modern-prom-recovery" in calls[0]
    if len(calls) == 2:
        assert calls[1][-1] == "--execute"
        assert calls[0][:calls[0].index("--summary-file")] == calls[1][:calls[1].index("--summary-file")]
