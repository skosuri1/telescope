"""Exclusive retirement routing, successful input evidence, and private phase execution."""

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
JOB = ROOT / "jobs/clustermesh-qualified-worker-retirement.yml"
STEP = ROOT / "steps/topology/clustermesh-scale/reuse/retire-qualified-worker.yml"
TFVARS = "scenarios/perf-eval/clustermesh-scale/terraform-inputs/azure-100-mock-shared.tfvars"
SCOPE = {
    "target_run_id": "78751-f36f3d5a", "confirm_resume": "78751-f36f3d5a",
    "expected_subscription_id": "37deca37-c375-4a14-b90a-043849bd2bf1",
    "expected_region": "eastus2euap", "expected_cluster_count": 100, "tfvars_path": TFVARS,
    "overlay_mode": "resume-existing", "run_workload": False, "qualification_build_id": 79986,
    "worker_state_build_id": 79993,
    "exclusive_modes": True,
}


def job():
    return yaml.safe_load(JOB.read_text(encoding="utf-8"))["jobs"][0]


@pytest.mark.parametrize("changes", [
    {}, {"target_run_id": "other"}, {"confirm_resume": "other"}, {"expected_subscription_id": "other"},
    {"expected_region": "westus"}, {"expected_cluster_count": 2}, {"expected_cluster_count": "100"},
    {"tfvars_path": "other"}, {"overlay_mode": "resume"}, {"run_workload": True},
    {"qualification_build_id": 79979}, {"qualification_build_id": "79986"}, {"exclusive_modes": False},
    {"worker_state_build_id": 79992}, {"worker_state_build_id": "79993"},
])
def test_only_exact_successful_qualification_scope_is_admitted(changes):
    result = subprocess.run(
        ["bash", "-c", job()["steps"][0]["script"]],
        env={**os.environ, "RETIREMENT_SCOPE_JSON": json.dumps({**SCOPE, **changes})},
        capture_output=True, text=True, check=False, timeout=5,
    )
    assert (result.returncode == 0) is (not changes)


def test_retirement_mode_excludes_every_other_job_and_keeps_other_stages_unchanged():
    pipeline = yaml.safe_load((ROOT / "pipelines/system/new-pipeline-test.yml").read_text(encoding="utf-8"))
    mode = "scaleDebugQualifiedWorkerRetirementBuildId"
    parameter = next(row for row in pipeline["parameters"] if row["name"] == mode)
    assert parameter["type"] == "number" and parameter["default"] == 0
    stage = next(row for row in pipeline["stages"] if row["stage"] == "azure_eastus2euap_n100_debug_resume_37deca")
    key = "${{ if and(eq(parameters.scaleDebugPostRetirementPromBuildId, 0), ne(parameters.scaleDebugQualifiedWorkerRetirementBuildId, 0)) }}"
    invocation = next(row[key][0] for row in stage["jobs"] if key in row)
    assert invocation["template"] == "/jobs/clustermesh-qualified-worker-retirement.yml"
    assert invocation["parameters"]["qualification_build_id"] == "${{ parameters.scaleDebugQualifiedWorkerRetirementBuildId }}"
    assert invocation["parameters"]["worker_state_build_id"] == 79993
    for row in stage["jobs"]:
        condition = next(iter(row))
        if condition.startswith("${{") and condition not in (
            key, "${{ if ne(parameters.scaleDebugPostRetirementPromBuildId, 0) }}",
        ):
            assert f"eq(parameters.{mode}, 0)" in condition
    for other in ("CapacityQualification", "CapacityFirstRecovery", "RetainedWorkerRestart",
                  "ModernCniProm", "ModernBaseline"):
        assert f"eq(parameters.scaleDebug{other}BuildId, 0)" in invocation["parameters"]["exclusive_modes"]
    assert all(mode not in json.dumps(other) for other in pipeline["stages"] if other is not stage)
    normal = yaml.safe_load((ROOT / "jobs/clustermesh-debug-resume.yml").read_text(encoding="utf-8"))
    assert next(row for row in normal["parameters"] if row["name"] == "qualified_retirement_build_id")["default"] == 0
    assert "eq(${{ parameters.qualified_retirement_build_id }}, 0)" in normal["jobs"][0]["condition"]


def test_retirement_publishes_plan_before_mutation_and_has_no_task_retries():
    definition = job()
    assert definition["timeoutInMinutes"] == 90 and definition["cancelTimeoutInMinutes"] == 30
    steps = definition["steps"]
    phase_steps = [(index, row["parameters"]["phase"]) for index, row in enumerate(steps)
                   if row.get("template") == "/steps/topology/clustermesh-scale/reuse/retire-qualified-worker.yml"]
    assert [phase for _, phase in phase_steps] == ["plan", "execute"]
    publication = next(index for index, row in enumerate(steps) if row.get("displayName") == "Publish retirement plan before any worker changes")
    assert phase_steps[0][0] < publication < phase_steps[1][0]
    download = next(row["inputs"] for row in steps if row.get("task") == "DownloadPipelineArtifact@2")
    assert download["artifactName"] == "n100-capacity-qualification-${{ parameters.qualification_build_id }}-1"
    assert any(row.get("inputs", {}).get("artifactName")
               == "n100-unreachable-worker-recovery-${{ parameters.worker_state_build_id }}-1" for row in steps)
    operation = yaml.safe_load(STEP.read_text(encoding="utf-8"))["steps"][0]
    assert operation["retryCountOnTaskFailure"] == 0
    assert operation["${{ if eq(parameters.phase, 'plan') }}"]["timeoutInMinutes"] == 15
    assert operation["${{ else }}"]["timeoutInMinutes"] == 65
    assert "always()" in steps[-1]["condition"]


@pytest.mark.parametrize("fault,count", [
    ("none", 2), ("unqualified", 0), ("unclean", 0), ("initial-symlink", 0), ("existing-output", 0),
    ("output-alias", 0), ("credentials-plan", 0), ("plan-error", 1), ("unsafe-plan", 1),
    ("input-change", 1), ("worker-state-change", 1), ("tfvars-change", 1), ("between-change", 1), ("missing-freeze", 1),
    ("credentials-execute", 1), ("execute-error", 2), ("not-fenced", 2), ("hold-remains", 2),
])
def test_retirement_freezes_entire_qualification_and_never_publishes_credentials(tmp_path, fault, count):
    script = yaml.safe_load(STEP.read_text(encoding="utf-8"))["steps"][0]["script"]
    source, checkout, private, binaries = (tmp_path / name for name in ("input", "checkout", "private", "bin"))
    for path in (source, checkout, private, binaries):
        path.mkdir()
    worker_state = tmp_path / "worker-state"
    worker_state.mkdir()
    (worker_state / "failure.json").write_text('{"source":79993}', encoding="utf-8")
    qualification = {
        "source_build": 79986, "success": True, "capacity_qualified": fault != "unqualified",
        "actual_ip_growth_proven": True, "actual_memory_headroom_proven": True,
        "completion_only": True, "execute": False, "mutation_started": False,
        "probe_cleanup_pending": ["unsafe"] if fault == "unclean" else [], "cleanup_errors": [],
    }
    (source / "qualification.json").write_text(json.dumps(qualification), encoding="utf-8")
    (source / "nested").mkdir()
    (source / "nested/evidence.json").write_text('{"fixed":true}', encoding="utf-8")
    if fault == "initial-symlink":
        (source / "link.json").symlink_to(source / "qualification.json")
    tfvars = checkout / TFVARS
    tfvars.parent.mkdir(parents=True)
    tfvars.write_text("original-tfvars", encoding="utf-8")
    helper = checkout / "modules/python/clusterloader2/clustermesh-scale/qualified_failed_worker_retirement.py"
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
        execute = "--execute" in args
        assert execute == (os.environ["PHASE"] == "execute")
        assert value("--qualification-build-id") == "79986" and value("--timeout-seconds") == "3600"
        assert value("--worker-state-build-id") == "79993"
        assert json.loads((Path(value("--worker-state-directory")) / "failure.json").read_text())["source"] == 79993
        assert value("--context") == "clustermesh-96"
        source = Path(value("--qualification-directory"))
        assert json.loads((source / "qualification.json").read_text())["source_build"] == 79986
        config = Path(value("--kubeconfig"))
        assert config.read_text() == "private-retirement-config" and config.stat().st_mode & 0o777 == 0o600
        output = Path(value("--summary-file"))
        assert source not in output.parents and output != config
        fault = os.environ["FAULT"]
        if not execute and fault == "input-change":
            (source / "nested/evidence.json").write_text("changed")
        if not execute and fault == "worker-state-change":
            (Path(value("--worker-state-directory")) / "failure.json").write_text("changed")
        if not execute and fault == "tfvars-change":
            (Path(os.environ["REPOSITORY_DIRECTORY"]) / os.environ["TFVARS_PATH"]).write_text("changed")
        output.write_text(json.dumps({
            "execute": execute, "mutation_started": execute, "plan_valid": fault != "unsafe-plan",
            "success": execute, "native_fencing_proven": execute and fault != "not-fenced",
            "source_retired": execute, "replacements_ready": execute,
            "placement_hold_removed": execute and fault != "hold-remains",
            "workloads_ready": False, "bootstrap_complete": False, "cleanup_errors": [],
        }))
        sys.exit(1 if fault == ("execute-error" if execute else "plan-error") else 0)
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
        Path(args[args.index("--file") + 1]).write_text("private-retirement-config")
        sys.exit(1 if os.environ["FAULT"] == "credentials-" + os.environ["PHASE"] else 0)
        """
    ), encoding="utf-8")
    az.chmod(0o755)
    artifacts = source if fault == "output-alias" else tmp_path / "artifacts"
    if fault == "existing-output":
        (artifacts / "n100-qualified-worker-retirement").mkdir(parents=True)
    calls_file = tmp_path / "calls.jsonl"
    environment = {
        **os.environ, "PATH": f"{binaries}:{os.environ['PATH']}", "PHASE": "plan",
        "RUN_ID": SCOPE["target_run_id"], "CONFIRM_RESUME": SCOPE["confirm_resume"],
        "SUBSCRIPTION": SCOPE["expected_subscription_id"], "REGION": SCOPE["expected_region"],
        "QUALIFICATION_BUILD_ID": "79986", "QUALIFICATION_DIRECTORY": str(source),
        "WORKER_STATE_BUILD_ID": "79993", "WORKER_STATE_DIRECTORY": str(worker_state),
        "TFVARS_PATH": TFVARS, "REPOSITORY_DIRECTORY": str(checkout), "AGENT_TEMP_DIRECTORY": str(private),
        "ARTIFACT_DIRECTORY": str(artifacts), "INPUTS_SHA": "", "TFVARS_SHA": "",
        "FAULT": fault, "CALLS": str(calls_file),
    }
    result = subprocess.run(["bash", "-c", script], env=environment, capture_output=True,
                            text=True, check=False, timeout=15)
    assert not list(private.iterdir())
    if result.returncode == 0:
        hashes = dict(re.findall(
            r"variable=(QUALIFIED_RETIREMENT_(?:INPUTS|TFVARS)_SHA);isReadOnly=true]([0-9a-f]{64})",
            result.stdout,
        ))
        assert len(hashes) == 2
        if fault == "between-change":
            (artifacts / "n100-qualified-worker-retirement/qualification-input/nested/evidence.json").write_text(
                "changed-between-phases", encoding="utf-8")
        environment.update(PHASE="execute", INPUTS_SHA="" if fault == "missing-freeze" else hashes["QUALIFIED_RETIREMENT_INPUTS_SHA"],
                           TFVARS_SHA=hashes["QUALIFIED_RETIREMENT_TFVARS_SHA"])
        result = subprocess.run(["bash", "-c", script], env=environment, capture_output=True,
                                text=True, check=False, timeout=15)
    assert (result.returncode == 0) is (fault == "none"), result.stderr
    calls = [json.loads(line) for line in calls_file.read_text(encoding="utf-8").splitlines()] if calls_file.exists() else []
    assert len(calls) == count and not list(private.iterdir()), result.stderr
    if calls:
        assert "--execute" not in calls[0]
    if len(calls) == 2:
        assert "--execute" in calls[1]
    assert not any(b"private-retirement-config" in path.read_bytes()
                   for path in artifacts.rglob("*") if path.is_file() and not path.is_symlink())
