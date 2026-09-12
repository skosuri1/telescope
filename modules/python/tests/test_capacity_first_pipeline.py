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


def job():
    return yaml.safe_load(JOB.read_text(encoding="utf-8"))["jobs"][0]


@pytest.mark.parametrize("changes", [
    {}, {"target_run_id": "other"}, {"confirm_resume": "other"}, {"expected_subscription_id": "other"},
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
    assert (result.returncode == 0) is (not changes)


def test_capacity_mode_disables_every_other_mutation_and_workload_job():
    pipeline = yaml.safe_load((ROOT / "pipelines/system/new-pipeline-test.yml").read_text(encoding="utf-8"))
    parameter = next(row for row in pipeline["parameters"] if row["name"] == "scaleDebugCapacityFirstRecoveryBuildId")
    assert parameter["type"] == "number" and parameter["default"] == 0
    stage = next(row for row in pipeline["stages"] if row["stage"] == "azure_eastus2euap_n100_debug_resume_37deca")
    key = "${{ if ne(parameters.scaleDebugCapacityFirstRecoveryBuildId, 0) }}"
    invocation = next(row[key][0] for row in stage["jobs"] if key in row)
    assert invocation["template"] == "/jobs/clustermesh-capacity-first.yml"
    assert invocation["parameters"]["accepted_restart_build_id"] == 79950
    assert invocation["parameters"]["source_state_build_id"] == "${{ parameters.scaleDebugCapacityFirstRecoveryBuildId }}"
    for row in stage["jobs"]:
        condition = next(iter(row))
        if condition.startswith("${{") and condition != key:
            assert "eq(parameters.scaleDebugCapacityFirstRecoveryBuildId, 0)" in condition
    normal = yaml.safe_load((ROOT / "jobs/clustermesh-debug-resume.yml").read_text(encoding="utf-8"))
    assert next(row for row in normal["parameters"] if row["name"] == "capacity_first_build_id")["default"] == 0
    assert "eq(${{ parameters.capacity_first_build_id }}, 0)" in normal["jobs"][0]["condition"]
    assert "eq(parameters.scaleDebugRetainedWorkerRestartBuildId, 0)" in invocation["parameters"]["exclusive_modes"]
    for other in pipeline["stages"]:
        if other is not stage:
            assert "scaleDebugCapacityFirstRecoveryBuildId" not in json.dumps(other)


@pytest.mark.parametrize("fault,count,code", [
    ("none", 2, 0), ("unsafe-plan", 1, 1), ("source", 1, 1), ("restart", 1, 1),
    ("tfvars", 1, 1), ("execute", 2, 1), ("symlink", 0, 1),
])
def test_capacity_plan_freezes_both_sources_and_keeps_credentials_private(tmp_path, fault, count, code):
    operation = next(row for row in job()["steps"] if row.get("retryCountOnTaskFailure") == 0)
    source, restart, checkout, private, binaries = (
        tmp_path / name for name in ("source", "restart", "checkout", "private", "bin")
    )
    for path in (source, restart, checkout, private, binaries):
        path.mkdir()
    (source / "current-nodes.json").write_text('{"items":[]}', encoding="utf-8")
    (restart / "recovery.json").write_text('{"accepted":true}', encoding="utf-8")
    if fault == "symlink":
        (source / "unsafe.json").symlink_to(tmp_path / "outside.json")
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
        execute, fault = "--execute" in args, os.environ["FAULT"]
        if not execute and fault == "source":
            (Path(value("--source-state-directory")) / "current-nodes.json").write_text("changed")
        if not execute and fault == "restart":
            Path(value("--restart-checkpoint")).write_text("changed")
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
        import sys
        from pathlib import Path
        args = sys.argv[1:]
        assert args[:2] == ["aks", "get-credentials"]
        assert args[args.index("--name") + 1] == "clustermesh-96"
        assert args[args.index("--subscription") + 1] == "37deca37-c375-4a14-b90a-043849bd2bf1"
        Path(args[args.index("--file") + 1]).write_text("private-capacity-credentials")
        """
    ), encoding="utf-8")
    fake.chmod(0o755)
    artifacts, calls_file = tmp_path / "artifacts", tmp_path / "calls.jsonl"
    env = {
        **os.environ, "PATH": f"{binaries}:{os.environ['PATH']}",
        "RUN_ID": SCOPE["target_run_id"], "CONFIRM_RESUME": SCOPE["confirm_resume"],
        "SUBSCRIPTION": SCOPE["expected_subscription_id"], "REGION": SCOPE["expected_region"],
        "TFVARS_PATH": TFVARS, "SOURCE_DIRECTORY": str(source), "RESTART_DIRECTORY": str(restart),
        "REPOSITORY_DIRECTORY": str(checkout), "AGENT_TEMP_DIRECTORY": str(private),
        "ARTIFACT_DIRECTORY": str(artifacts), "CALLS": str(calls_file), "FAULT": fault,
    }
    result = subprocess.run(
        ["bash", "-c", operation["script"]], env=env, capture_output=True, text=True, check=False, timeout=10,
    )
    assert result.returncode == code, result.stderr
    calls = [json.loads(row) for row in calls_file.read_text(encoding="utf-8").splitlines()] if calls_file.exists() else []
    assert len(calls) == count and not list(private.iterdir())
    assert not any(b"private-capacity-credentials" in p.read_bytes() for p in artifacts.rglob("*") if p.is_file())
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
    assert downloads[0]["inputs"]["artifactName"].startswith("n100-unreachable-worker-recovery-")
    assert downloads[1]["inputs"]["artifactName"].startswith("n100-retained-worker-restart-")
    assert definition["steps"][-1]["inputs"]["artifact"] == "n100-capacity-first-$(Build.BuildId)-$(System.JobAttempt)"
