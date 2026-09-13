"""Exercise exclusive capacity-only routing and immutable evidence handling."""

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]
JOB = ROOT / "jobs/clustermesh-capacity-first.yml"
TFVARS = "scenarios/perf-eval/clustermesh-scale/terraform-inputs/azure-100-mock-shared.tfvars"
SCOPE = {
    "target_run_id": "78751-f36f3d5a", "confirm_resume": "78751-f36f3d5a",
    "expected_subscription_id": "37deca37-c375-4a14-b90a-043849bd2bf1",
    "expected_region": "eastus2euap", "expected_cluster_count": 100,
    "tfvars_path": TFVARS, "overlay_mode": "resume-existing", "run_workload": False,
    "source_state_build_id": 79955, "accepted_restart_build_id": 79950, "exclusive_modes": True,
}
JOURNAL_UID = "1e3b51d5-83d4-406d-bb38-4ead04e1c425"


def job():
    return yaml.safe_load(JOB.read_text(encoding="utf-8"))["jobs"][0]


@pytest.mark.parametrize("changes", [
    {}, {"source_state_build_id": 79959}, {"target_run_id": "other"}, {"confirm_resume": "other"}, {"expected_subscription_id": "other"},
    {"expected_region": "westus"}, {"expected_cluster_count": 2}, {"expected_cluster_count": "100"},
    {"tfvars_path": "other.tfvars"}, {"overlay_mode": "resume"}, {"run_workload": True},
    {"source_state_build_id": 0}, {"source_state_build_id": -1}, {"source_state_build_id": "79955"},
    {"source_state_build_id": 79955.5}, {"accepted_restart_build_id": 79945}, {"exclusive_modes": False},
])
def test_capacity_scope_is_fixed_typed_and_exclusive(changes):
    result = subprocess.run(
        ["bash", "-c", job()["steps"][0]["script"]],
        env={**os.environ, "CAPACITY_SCOPE_JSON": json.dumps({**SCOPE, **changes})},
        text=True, capture_output=True, check=False, timeout=5,
    )
    assert (result.returncode == 0) is (not changes or changes == {"source_state_build_id": 79959})


def test_capacity_mode_disables_every_other_mutation_and_workload_job():
    pipeline = yaml.safe_load((ROOT / "pipelines/system/new-pipeline-test.yml").read_text(encoding="utf-8"))
    parameter = next(row for row in pipeline["parameters"] if row["name"] == "scaleDebugCapacityFirstRecoveryBuildId")
    assert parameter["type"] == "number" and parameter["default"] == 0
    stage = next(row for row in pipeline["stages"] if row["stage"] == "azure_eastus2euap_n100_debug_resume_37deca")
    key = "${{ if and(eq(parameters.scaleDebugPostRetirementPromBuildId, 0), eq(parameters.scaleDebugQualifiedWorkerRetirementBuildId, 0), eq(parameters.scaleDebugCapacityQualificationBuildId, 0), ne(parameters.scaleDebugCapacityFirstRecoveryBuildId, 0)) }}"
    invocation = next(row[key][0] for row in stage["jobs"] if key in row)
    assert invocation["template"] == "/jobs/clustermesh-capacity-first.yml"
    assert invocation["parameters"]["accepted_restart_build_id"] == 79950
    assert invocation["parameters"]["source_state_build_id"] == "${{ parameters.scaleDebugCapacityFirstRecoveryBuildId }}"
    for row in stage["jobs"]:
        condition = next(iter(row))
        if condition.startswith("${{") and condition not in (
            key, "${{ if and(eq(parameters.scaleDebugPostRetirementPromBuildId, 0), eq(parameters.scaleDebugQualifiedWorkerRetirementBuildId, 0), ne(parameters.scaleDebugCapacityQualificationBuildId, 0)) }}",
            "${{ if and(eq(parameters.scaleDebugPostRetirementPromBuildId, 0), ne(parameters.scaleDebugQualifiedWorkerRetirementBuildId, 0)) }}",
            "${{ if ne(parameters.scaleDebugPostRetirementPromBuildId, 0) }}",
        ):
            assert "eq(parameters.scaleDebugCapacityFirstRecoveryBuildId, 0)" in condition
    normal = yaml.safe_load((ROOT / "jobs/clustermesh-debug-resume.yml").read_text(encoding="utf-8"))
    assert next(row for row in normal["parameters"] if row["name"] == "capacity_first_build_id")["default"] == 0
    assert "eq(${{ parameters.capacity_first_build_id }}, 0)" in normal["jobs"][0]["condition"]
    assert "eq(parameters.scaleDebugRetainedWorkerRestartBuildId, 0)" in invocation["parameters"]["exclusive_modes"]
    for other in pipeline["stages"]:
        if other is not stage:
            assert "scaleDebugCapacityFirstRecoveryBuildId" not in json.dumps(other)


@pytest.mark.parametrize("source_build,fault,count,code", [
    (build, fault, count, code)
    for build in (79955, 79959)
    for fault, count, code in (
        ("none", 2, 0), ("unsafe-plan", 1, 1), ("source", 1, 1), ("restart", 1, 1),
        ("tfvars", 1, 1), ("execute", 2, 1), ("symlink", 0, 1), ("credentials", 0, 1),
        ("plan-error", 1, 1), ("output-alias", 0, 1), ("existing-output", 0, 1),
        ("new-source-symlink", 1, 1),
    )
] + [
    (79959, "prior-capacity", 1, 1), (79959, "prior-symlink", 0, 1),
    (79959, "missing-prior", 0, 1), (79959, "missing-nested", 0, 1),
    (79959, "submitted", 0, 1), (79959, "accepted", 0, 1), (79959, "wrong-journal", 0, 1),
])
def test_capacity_plan_freezes_sources_and_keeps_credentials_private(tmp_path, source_build, fault, count, code):
    operation = next(row for row in job()["steps"] if row.get("retryCountOnTaskFailure") == 0)
    source, restart, checkout, private, binaries = (
        tmp_path / name for name in ("source", "restart", "checkout", "private", "bin")
    )
    for path in (source, restart, checkout, private, binaries):
        path.mkdir()
    evidence = source / "source-state" if source_build == 79959 else source
    evidence.mkdir(exist_ok=True)
    (evidence / "current-nodes.json").write_text('{"items":[],"original_build":79955}', encoding="utf-8")
    if source_build == 79959:
        prior = {"create": {"submission_started": fault == "submitted", "accepted": True if fault == "accepted" else None},
                 "journal": {"uid": "wrong" if fault == "wrong-journal" else JOURNAL_UID}}
        (source / "recovery.json").write_text(json.dumps(prior), encoding="utf-8")
        (source / "plan.json").write_text('{"outer_artifact_not_observation":true}', encoding="utf-8")
        if fault == "missing-prior":
            (source / "recovery.json").unlink()
        elif fault == "prior-symlink":
            (source / "real-prior.json").write_text(json.dumps(prior), encoding="utf-8")
            (source / "recovery.json").unlink()
            (source / "recovery.json").symlink_to(source / "real-prior.json")
        elif fault == "missing-nested":
            (evidence / "current-nodes.json").rename(source / "current-nodes.json")
            evidence.rmdir()
    (restart / "recovery.json").write_text('{"accepted":true}', encoding="utf-8")
    if fault == "symlink":
        (evidence / "unsafe.json").symlink_to(tmp_path / "outside.json")
    tfvars = checkout / TFVARS
    tfvars.parent.mkdir(parents=True)
    tfvars.write_text("original-input", encoding="utf-8")
    helper = checkout / "modules/python/clusterloader2/clustermesh-scale/capacity_first_worker_recovery.py"
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
        with Path(os.environ["CALLS"]).open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(args) + "\\n")
        config = Path(value("--kubeconfig"))
        assert config.read_text() == "private-capacity-credentials"
        assert config.stat().st_mode & 0o777 == 0o600
        assert value("--context") == "clustermesh-96" and value("--timeout-seconds") == "2400"
        source = Path(value("--source-state-directory"))
        assert source.name == "source-state"
        assert set(path.name for path in source.iterdir()) == {"current-nodes.json"}
        assert json.loads((source / "current-nodes.json").read_text())["original_build"] == 79955
        assert value("--restart-checkpoint").endswith("/accepted-restart.json")
        if os.environ["SOURCE_STATE_BUILD_ID"] == "79959":
            assert value("--resume-build-id") == "79959"
            prior = Path(value("--resume-capacity-checkpoint"))
            assert prior.name == "prior-capacity.json" and prior.parent == source.parent
            assert json.loads(prior.read_text())["journal"]["uid"] == "1e3b51d5-83d4-406d-bb38-4ead04e1c425"
        else:
            assert "--resume-capacity-checkpoint" not in args and "--resume-build-id" not in args
        assert Path(value("--summary-file")) not in (
            Path(value("--restart-checkpoint")),
            Path(value("--resume-capacity-checkpoint")) if "--resume-capacity-checkpoint" in args else source,
        )
        execute, fault = "--execute" in args, os.environ["FAULT"]
        if not execute and fault == "plan-error":
            sys.exit(1)
        if not execute and fault == "source":
            (Path(value("--source-state-directory")) / "current-nodes.json").write_text("changed")
        if not execute and fault == "new-source-symlink":
            (source / "new-link.json").symlink_to(source / "current-nodes.json")
        if not execute and fault == "restart":
            Path(value("--restart-checkpoint")).write_text("changed")
        if not execute and fault == "prior-capacity":
            Path(value("--resume-capacity-checkpoint")).write_text("changed")
        if not execute and fault == "tfvars":
            (Path(os.environ["REPOSITORY_DIRECTORY"]) / os.environ["TFVARS_PATH"]).write_text("changed")
        Path(value("--summary-file")).write_text(json.dumps({
            "execute": execute, "mutation_started": fault == "unsafe-plan", "plan_valid": True,
            "workloads_ready": False,
        }))
        if execute and fault == "execute":
            sys.exit(1)
        """
    ), encoding="utf-8")
    fake = binaries / "az"
    fake.write_text(f"#!{sys.executable}\n" + textwrap.dedent(
        """
        import os
        import sys
        from pathlib import Path
        args = sys.argv[1:]
        assert args[:2] == ["aks", "get-credentials"]
        assert args[args.index("--name") + 1] == "clustermesh-96"
        assert args[args.index("--subscription") + 1] == "37deca37-c375-4a14-b90a-043849bd2bf1"
        Path(args[args.index("--file") + 1]).write_text("private-capacity-credentials")
        if os.environ["FAULT"] == "credentials":
            sys.exit(1)
        """
    ), encoding="utf-8")
    fake.chmod(0o755)
    artifacts, calls_file = tmp_path / "artifacts", tmp_path / "calls.jsonl"
    if fault == "output-alias":
        artifacts = source
    if fault == "existing-output":
        (artifacts / "n100-capacity-first").mkdir(parents=True)
    env = {
        **os.environ, "PATH": f"{binaries}:{os.environ['PATH']}",
        "RUN_ID": SCOPE["target_run_id"], "CONFIRM_RESUME": SCOPE["confirm_resume"],
        "SUBSCRIPTION": SCOPE["expected_subscription_id"], "REGION": SCOPE["expected_region"],
        "TFVARS_PATH": TFVARS, "SOURCE_DIRECTORY": str(source), "RESTART_DIRECTORY": str(restart),
        "SOURCE_STATE_BUILD_ID": str(source_build),
        "REPOSITORY_DIRECTORY": str(checkout), "AGENT_TEMP_DIRECTORY": str(private),
        "ARTIFACT_DIRECTORY": str(artifacts), "CALLS": str(calls_file), "FAULT": fault,
    }
    result = subprocess.run(
        ["bash", "-c", operation["script"]], env=env, capture_output=True, text=True, check=False, timeout=10,
    )
    assert result.returncode == code, result.stderr
    calls = [json.loads(row) for row in calls_file.read_text(encoding="utf-8").splitlines()] if calls_file.exists() else []
    assert len(calls) == count and not list(private.iterdir())
    assert not any(b"private-capacity-credentials" in p.read_bytes()
                   for p in artifacts.rglob("*") if p.is_file() and not p.is_symlink())
    if calls:
        assert "--execute" not in calls[0]
    if len(calls) == 2:
        assert calls[1][-1] == "--execute"
        assert calls[0][:calls[0].index("--summary-file")] == calls[1][:calls[1].index("--summary-file")]


def test_capacity_job_binds_both_evidence_artifacts_and_phase_specific_output():
    definition = job()
    assert definition["timeoutInMinutes"] == 75 and definition["cancelTimeoutInMinutes"] == 30
    downloads = [row for row in definition["steps"] if row.get("task") == "DownloadPipelineArtifact@2"]
    assert len(downloads) == 2
    download = downloads[0]["inputs"]
    assert download["${{ if eq(parameters.source_state_build_id, 79959) }}"]["artifactName"] == "n100-capacity-first-79959-1"
    assert download["${{ else }}"]["artifactName"] == "n100-unreachable-worker-recovery-${{ parameters.source_state_build_id }}-1"
    assert download["pipelineId"] == "${{ parameters.source_state_build_id }}"
    assert downloads[1]["inputs"]["artifactName"].startswith("n100-retained-worker-restart-")
    assert definition["steps"][-1]["inputs"]["artifact"] == "n100-capacity-first-$(Build.BuildId)-$(System.JobAttempt)"
