"""Exercise exclusive qualification, distinct phases, input freezes and private credentials."""

import json
import os
import re
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]
JOB = ROOT / "jobs/clustermesh-capacity-qualification.yml"
STEP = ROOT / "steps/topology/clustermesh-scale/reuse/qualify-existing-capacity.yml"
TFVARS = "scenarios/perf-eval/clustermesh-scale/terraform-inputs/azure-100-mock-shared.tfvars"
SCOPE = {
    "target_run_id": "78751-f36f3d5a", "confirm_resume": "78751-f36f3d5a",
    "expected_subscription_id": "37deca37-c375-4a14-b90a-043849bd2bf1",
    "expected_region": "eastus2euap", "expected_cluster_count": 100, "tfvars_path": TFVARS,
    "overlay_mode": "resume-existing", "run_workload": False, "observation_build_id": 79975,
    "accepted_capacity_build_id": 79971, "exclusive_modes": True,
}


def job():
    return yaml.safe_load(JOB.read_text(encoding="utf-8"))["jobs"][0]


@pytest.mark.parametrize("changes", [
    {}, {"observation_build_id": 79979},
    {"target_run_id": "other"}, {"confirm_resume": "other"}, {"expected_subscription_id": "other"},
    {"expected_region": "westus"}, {"expected_cluster_count": 2}, {"expected_cluster_count": "100"},
    {"tfvars_path": "other"}, {"overlay_mode": "resume"}, {"run_workload": True},
    {"observation_build_id": 79971}, {"observation_build_id": "79975"},
    {"accepted_capacity_build_id": 79959}, {"accepted_capacity_build_id": "79971"}, {"exclusive_modes": False},
])
def test_scope_fails_before_setup_for_any_different_or_untyped_authority(changes):
    result = subprocess.run(
        ["bash", "-c", job()["steps"][0]["script"]],
        env={**os.environ, "QUALIFICATION_SCOPE_JSON": json.dumps({**SCOPE, **changes})},
        capture_output=True, text=True, check=False, timeout=5,
    )
    assert (result.returncode == 0) is (not changes or changes == {"observation_build_id": 79979})


def test_qualification_mode_excludes_every_other_mutation_and_normal_job():
    pipeline = yaml.safe_load((ROOT / "pipelines/system/new-pipeline-test.yml").read_text(encoding="utf-8"))
    mode = "scaleDebugCapacityQualificationBuildId"
    parameter = next(row for row in pipeline["parameters"] if row["name"] == mode)
    assert parameter["default"] == 0 and parameter["type"] == "number"
    stage = next(row for row in pipeline["stages"] if row["stage"] == "azure_eastus2euap_n100_debug_resume_37deca")
    key = "${{ if and(eq(parameters.scaleDebugQualifiedWorkerRetirementBuildId, 0), ne(parameters.scaleDebugCapacityQualificationBuildId, 0)) }}"
    invocation = next(row[key][0] for row in stage["jobs"] if key in row)
    assert invocation["template"] == "/jobs/clustermesh-capacity-qualification.yml"
    assert invocation["parameters"]["observation_build_id"] == "${{ parameters.scaleDebugCapacityQualificationBuildId }}"
    assert invocation["parameters"]["accepted_capacity_build_id"] == 79971
    for row in stage["jobs"]:
        condition = next(iter(row))
        if condition.startswith("${{") and condition not in (
            key, "${{ if ne(parameters.scaleDebugQualifiedWorkerRetirementBuildId, 0) }}",
        ):
            assert "eq(parameters.scaleDebugCapacityQualificationBuildId, 0)" in condition
    for name in ("CapacityFirstRecovery", "RetainedWorkerRestart", "ModernBaseline", "ModernCniProm"):
        assert f"eq(parameters.scaleDebug{name}BuildId, 0)" in invocation["parameters"]["exclusive_modes"]
    for name in ("ModernPromRecovery", "CniWorkerMaintenanceOnly", "UnreachableWorkerRecoveryOnly",
                 "UnreachableWorkerQuotaObserveOnly", "ArmRepairOnly", "PreparedRetirementOnly"):
        assert f"not(parameters.scaleDebug{name})" in invocation["parameters"]["exclusive_modes"]
    normal_call = next(row for row in stage["jobs"] if row.get("template") == "/jobs/clustermesh-debug-resume.yml")
    assert normal_call["parameters"]["capacity_qualification_build_id"] == "${{ parameters.scaleDebugCapacityQualificationBuildId }}"
    normal = yaml.safe_load((ROOT / "jobs/clustermesh-debug-resume.yml").read_text(encoding="utf-8"))
    assert next(row for row in normal["parameters"] if row["name"] == "capacity_qualification_build_id")["default"] == 0
    assert "eq(${{ parameters.capacity_qualification_build_id }}, 0)" in normal["jobs"][0]["condition"]
    assert all(mode not in json.dumps(other) for other in pipeline["stages"] if other is not stage)


def test_plan_and_probe_tasks_are_distinct_and_plan_is_published_first():
    definition = job()
    assert definition["timeoutInMinutes"] == 75 and definition["cancelTimeoutInMinutes"] == 30
    steps = definition["steps"]
    phases = [(index, step["parameters"]["phase"]) for index, step in enumerate(steps)
              if step.get("template") == "/steps/topology/clustermesh-scale/reuse/qualify-existing-capacity.yml"]
    assert [phase for _, phase in phases] == ["plan", "execute"]
    publication = next(index for index, step in enumerate(steps)
                       if step.get("displayName") == "Publish qualification plan before any probes")
    assert phases[0][0] < publication < phases[1][0]
    downloads = [step["inputs"] for step in steps if step.get("task") == "DownloadPipelineArtifact@2"]
    assert downloads[0]["${{ if eq(parameters.observation_build_id, 79979) }}"]["artifactName"] == "n100-capacity-qualification-79979-1"
    assert downloads[0]["${{ else }}"]["artifactName"] == "n100-unreachable-worker-recovery-${{ parameters.observation_build_id }}-1"
    assert downloads[1]["artifactName"] == "n100-capacity-first-${{ parameters.accepted_capacity_build_id }}-1"
    step = yaml.safe_load(STEP.read_text(encoding="utf-8"))["steps"][0]
    assert step["retryCountOnTaskFailure"] == 0
    assert step["${{ if eq(parameters.phase, 'plan') }}"]["timeoutInMinutes"] == 15
    assert step["${{ else }}"]["timeoutInMinutes"] == 45
    assert "always()" in steps[-1]["condition"] and "DIAGNOSTICS_READY" in steps[-1]["condition"]


@pytest.mark.parametrize("source_build", [79975, 79979])
@pytest.mark.parametrize("fault,expected_calls", [
    ("none", 2), ("initial-symlink", 0), ("output-alias", 0), ("existing-output", 0),
    ("credentials-plan", 0), ("plan-error", 1), ("unsafe-plan", 1),
    ("observation-change", 1), ("capacity-change", 1), ("nested-change", 1), ("tfvars-change", 1),
    ("plan-symlink", 1), ("between-change", 1), ("missing-freeze", 1), ("between-symlink", 1),
    ("credentials-execute", 1), ("execute-error", 2), ("false-qualified", 2), ("cleanup-left", 2),
])
def test_phase_inputs_are_frozen_and_credentials_never_enter_artifacts(tmp_path, source_build, fault, expected_calls):
    script = yaml.safe_load(STEP.read_text(encoding="utf-8"))["steps"][0]["script"]
    observation, creation, checkout, private, binaries = (
        tmp_path / name for name in ("observation", "creation", "checkout", "private", "bin")
    )
    for path in (observation, creation, checkout, private, binaries):
        path.mkdir()
    observation_input = observation / "observation" if source_build == 79979 else observation
    observation_input.mkdir(exist_ok=True)
    (observation_input / "current-nodes.json").write_text('{"source":79975}', encoding="utf-8")
    if source_build == 79979:
        (observation / "qualification.json").write_text('{"source":79979}', encoding="utf-8")
    (creation / "recovery.json").write_text('{"source":79971}', encoding="utf-8")
    (creation / "source-state").mkdir()
    (creation / "source-state" / "current-nodes.json").write_text('{"source":79955}', encoding="utf-8")
    if fault == "initial-symlink":
        (creation / "source-state" / "unexpected.json").symlink_to(creation / "recovery.json")
    tfvars = checkout / TFVARS
    tfvars.parent.mkdir(parents=True)
    tfvars.write_text("pinned-tfvars", encoding="utf-8")
    helper = checkout / "modules/python/clusterloader2/clustermesh-scale/capacity_first_qualification.py"
    helper.parent.mkdir(parents=True)
    helper.write_text(textwrap.dedent(
        """
        import json
        import os
        import sys
        from pathlib import Path
        args = sys.argv[1:]
        def value(name):
            return args[args.index(name) + 1]
        with Path(os.environ["CALLS"]).open("a") as handle:
            handle.write(json.dumps(args) + "\\n")
        complete = os.environ["SOURCE_BUILD_ID"] == "79979"
        execute = "--execute" in args
        finish = os.environ["PHASE"] == "execute"
        assert execute == (finish and not complete)
        if complete:
            assert value("--completed-qualification-build-id") == "79979"
            assert json.loads(Path(value("--completed-qualification-checkpoint")).read_text())["source"] == 79979
        else:
            assert "--completed-qualification-checkpoint" not in args
        assert value("--observation-build-id") == "79975" and value("--capacity-build-id") == "79971"
        assert value("--context") == "clustermesh-96" and value("--timeout-seconds") == "2400"
        config = Path(value("--kubeconfig"))
        assert config.read_text() == "private-qualification-credentials" and config.stat().st_mode & 0o777 == 0o600
        observation, creation = Path(value("--observation-directory")), Path(value("--capacity-directory"))
        output = Path(value("--summary-file"))
        assert output not in (observation, creation) and observation not in output.parents and creation not in output.parents
        assert json.loads((observation / "current-nodes.json").read_text())["source"] == 79975
        assert json.loads((creation / "source-state/current-nodes.json").read_text())["source"] == 79955
        fault = os.environ["FAULT"]
        if not finish:
            paths = {
                "observation-change": observation / "current-nodes.json",
                "capacity-change": creation / "recovery.json",
                "nested-change": creation / "source-state/current-nodes.json",
                "tfvars-change": Path(os.environ["REPOSITORY_DIRECTORY"]) / os.environ["TFVARS_PATH"],
            }
            if fault in paths:
                paths[fault].write_text("changed")
            if fault == "plan-symlink":
                (observation / "new-symlink.json").symlink_to(creation / "recovery.json")
        output.write_text(json.dumps({
            "execute": execute, "mutation_started": execute,
            "plan_valid": fault != "unsafe-plan", "capacity_qualified": finish,
            "actual_ip_growth_proven": finish and fault != "false-qualified",
            "actual_memory_headroom_proven": finish, "workloads_ready": False, "bootstrap_complete": False,
            "completion_only": complete,
            "probe_cleanup_pending": ["not-clean"] if fault == "cleanup-left" else [],
        }))
        sys.exit(1 if fault == ("execute-error" if finish else "plan-error") else 0)
        """
    ), encoding="utf-8")
    az = binaries / "az"
    az.write_text(f"#!{sys.executable}\n" + textwrap.dedent(
        """
        import os
        import sys
        from pathlib import Path
        args = sys.argv[1:]
        assert args[:2] == ["aks", "get-credentials"]
        assert args[args.index("--name") + 1] == "clustermesh-96"
        assert args[args.index("--subscription") + 1] == "37deca37-c375-4a14-b90a-043849bd2bf1"
        Path(args[args.index("--file") + 1]).write_text("private-qualification-credentials")
        sys.exit(1 if os.environ["FAULT"] == "credentials-" + os.environ["PHASE"] else 0)
        """
    ), encoding="utf-8")
    az.chmod(0o755)
    artifacts = observation if fault == "output-alias" else tmp_path / "artifacts"
    if fault == "existing-output":
        (artifacts / "n100-capacity-qualification").mkdir(parents=True)
    calls_file = tmp_path / "calls.jsonl"
    environment = {
        **os.environ, "PATH": f"{binaries}:{os.environ['PATH']}", "PHASE": "plan",
        "RUN_ID": SCOPE["target_run_id"], "CONFIRM_RESUME": SCOPE["confirm_resume"],
        "SUBSCRIPTION": SCOPE["expected_subscription_id"], "REGION": SCOPE["expected_region"],
        "TFVARS_PATH": TFVARS, "OBSERVATION_BUILD_ID": "79975", "CAPACITY_BUILD_ID": "79971",
        "SOURCE_BUILD_ID": str(source_build),
        "OBSERVATION_DIRECTORY": str(observation), "CAPACITY_DIRECTORY": str(creation),
        "REPOSITORY_DIRECTORY": str(checkout), "AGENT_TEMP_DIRECTORY": str(private),
        "ARTIFACT_DIRECTORY": str(artifacts), "INPUTS_SHA": "", "TFVARS_SHA": "",
        "FAULT": fault, "CALLS": str(calls_file),
    }
    result = subprocess.run(["bash", "-c", script], env=environment, capture_output=True,
                            text=True, check=False, timeout=15)
    assert not list(private.iterdir())
    if result.returncode == 0:
        variables = dict(re.findall(
            r"variable=(CAPACITY_QUALIFICATION_(?:INPUTS|TFVARS)_SHA);isReadOnly=true]([0-9a-f]{64})",
            result.stdout,
        ))
        assert len(variables) == 2
        directory = artifacts / "n100-capacity-qualification"
        if fault == "between-change":
            (directory / "capacity-input/source-state/current-nodes.json").write_text("between", encoding="utf-8")
        elif fault == "between-symlink":
            (directory / "observation/new-link.json").symlink_to(directory / "capacity-input/recovery.json")
        environment.update(
            PHASE="execute",
            INPUTS_SHA="" if fault == "missing-freeze" else variables["CAPACITY_QUALIFICATION_INPUTS_SHA"],
            TFVARS_SHA=variables["CAPACITY_QUALIFICATION_TFVARS_SHA"],
        )
        result = subprocess.run(["bash", "-c", script], env=environment, capture_output=True,
                                text=True, check=False, timeout=15)
    assert (result.returncode == 0) is (fault == "none"), result.stderr
    calls = [json.loads(line) for line in calls_file.read_text(encoding="utf-8").splitlines()] if calls_file.exists() else []
    assert len(calls) == expected_calls and not list(private.iterdir()), result.stderr
    if calls:
        assert "--execute" not in calls[0]
    if len(calls) == 2:
        assert ("--execute" in calls[1]) is (source_build == 79975)
    assert not any(b"private-qualification-credentials" in path.read_bytes()
                   for path in artifacts.rglob("*") if path.is_file() and not path.is_symlink())
