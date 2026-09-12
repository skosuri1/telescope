"""Exercise the one-worker restart routing without Azure or Kubernetes."""

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]
JOB = ROOT / "jobs/clustermesh-retained-worker-restart.yml"
TFVARS = "scenarios/perf-eval/clustermesh-scale/terraform-inputs/azure-100-mock-shared.tfvars"
SCOPE = {
    "target_run_id": "78751-f36f3d5a", "confirm_resume": "78751-f36f3d5a",
    "expected_subscription_id": "37deca37-c375-4a14-b90a-043849bd2bf1",
    "expected_region": "eastus2euap", "expected_cluster_count": 100,
    "tfvars_path": TFVARS, "overlay_mode": "resume-existing", "run_workload": False,
    "source_state_build_id": 79941, "exclusive_modes": True,
}


def job():
    return yaml.safe_load(JOB.read_text(encoding="utf-8"))["jobs"][0]


@pytest.mark.parametrize("changes", [
    {}, {"target_run_id": "another-run"}, {"confirm_resume": "another-run"},
    {"expected_subscription_id": "another-subscription"}, {"expected_region": "westus"},
    {"expected_cluster_count": 2}, {"expected_cluster_count": "100"}, {"tfvars_path": "other.tfvars"},
    {"overlay_mode": "resume"}, {"run_workload": True}, {"run_workload": "false"},
    {"source_state_build_id": 0}, {"source_state_build_id": -1},
    {"source_state_build_id": 79941.5}, {"source_state_build_id": "79941"}, {"exclusive_modes": False},
])
def test_restart_scope_is_typed_and_exclusive(changes):
    result = subprocess.run(
        ["bash", "-c", job()["steps"][0]["script"]],
        env={**os.environ, "RESTART_SCOPE_JSON": json.dumps({**SCOPE, **changes})},
        capture_output=True, text=True, check=False, timeout=5,
    )
    assert (result.returncode == 0) is (not changes)


def test_restart_mode_cannot_fall_through_to_other_jobs():
    pipeline = yaml.safe_load((ROOT / "pipelines/system/new-pipeline-test.yml").read_text(encoding="utf-8"))
    parameter = next(row for row in pipeline["parameters"] if row["name"] == "scaleDebugRetainedWorkerRestartBuildId")
    assert parameter["type"] == "number" and parameter["default"] == 0
    stage = next(row for row in pipeline["stages"] if row["stage"] == "azure_eastus2euap_n100_debug_resume_37deca")
    key = "${{ if ne(parameters.scaleDebugRetainedWorkerRestartBuildId, 0) }}"
    invocation = next(row[key][0] for row in stage["jobs"] if key in row)
    assert invocation["template"] == "/jobs/clustermesh-retained-worker-restart.yml"
    assert invocation["parameters"]["source_state_build_id"] == "${{ parameters.scaleDebugRetainedWorkerRestartBuildId }}"
    for row in stage["jobs"]:
        condition = next(iter(row))
        if condition.startswith("${{") and condition != key:
            assert "eq(parameters.scaleDebugRetainedWorkerRestartBuildId, 0)" in condition
    normal = yaml.safe_load((ROOT / "jobs/clustermesh-debug-resume.yml").read_text(encoding="utf-8"))
    assert next(row for row in normal["parameters"] if row["name"] == "retained_worker_restart_build_id")["default"] == 0
    assert "eq(${{ parameters.retained_worker_restart_build_id }}, 0)" in normal["jobs"][0]["condition"]
    assert "not(parameters.scaleDebugUnreachableWorkerRecoveryOnly)" in invocation["parameters"]["exclusive_modes"]
    assert "not(parameters.scaleDebugCniWorkerMaintenanceOnly)" in invocation["parameters"]["exclusive_modes"]
    assert "eq(parameters.scaleDebugDv3QuotaRequestLimit, 0)" in invocation["parameters"]["exclusive_modes"]
    for other in pipeline["stages"]:
        if other is not stage:
            assert "scaleDebugRetainedWorkerRestartBuildId" not in json.dumps(other)


@pytest.mark.parametrize("fault,expected_calls,expected_exit", [
    ("none", 2, 0), ("invalid-plan", 1, 1), ("mutate-source", 1, 1),
    ("extra-source", 1, 1), ("mutate-tfvars", 1, 1), ("execution-error", 2, 1),
    ("source-symlink", 0, 1), ("resume", 2, 0), ("mutate-checkpoint", 1, 1),
])
def test_plan_execute_hashes_and_private_credentials(tmp_path, fault, expected_calls, expected_exit):
    definition = job()
    operation = next(row for row in definition["steps"] if row.get("retryCountOnTaskFailure") == 0)
    source, checkout, private, binaries = (tmp_path / name for name in ("source", "checkout", "private", "bin"))
    for path in (source, checkout, private, binaries):
        path.mkdir()
    (source / "current-nodes.json").write_text('{"items": []}', encoding="utf-8")
    resuming = fault in ("resume", "mutate-checkpoint")
    if resuming:
        nested = source / "source-state"
        nested.mkdir()
        (source / "current-nodes.json").rename(nested / "current-nodes.json")
        (source / "recovery.json").write_text('{"known_reservation": true}', encoding="utf-8")
    if fault == "source-symlink":
        (source / "untrusted.json").symlink_to(tmp_path / "outside.json")
    tfvars = checkout / TFVARS
    tfvars.parent.mkdir(parents=True)
    tfvars.write_text("original-topology", encoding="utf-8")
    helper = checkout / "modules/python/clusterloader2/clustermesh-scale/stalled_retained_worker_recovery.py"
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
        assert config.read_text() == "credentials-stay-private"
        assert config.stat().st_mode & 0o777 == 0o600
        assert value("--context") == "clustermesh-96"
        assert value("--timeout-seconds") == "1800"
        execute = "--execute" in args
        fault = os.environ["FAULT"]
        source = Path(value("--source-state-directory"))
        if not execute and fault == "mutate-source":
            (source / "current-nodes.json").write_text('{"changed": true}')
        if not execute and fault == "extra-source":
            (source / "extra.json").write_text("{}")
        if not execute and fault == "mutate-tfvars":
            (Path(os.environ["REPOSITORY_DIRECTORY"]) / os.environ["TFVARS_PATH"]).write_text("changed")
        if fault in ("resume", "mutate-checkpoint"):
            assert value("--resume-build-id") == "79945"
            if not execute and fault == "mutate-checkpoint":
                Path(value("--resume-checkpoint")).write_text("changed")
        Path(value("--summary-file")).write_text(json.dumps({
            "execute": execute, "mutation_started": execute, "plan_valid": fault != "invalid-plan",
            "workloads_ready": False,
        }))
        if execute and fault == "execution-error":
            sys.exit(1)
        """
    ), encoding="utf-8")
    az = binaries / "az"
    az.write_text(f"#!{sys.executable}\n" + textwrap.dedent(
        """
        import sys
        from pathlib import Path

        args = sys.argv[1:]
        assert args[:2] == ["aks", "get-credentials"]
        assert args[args.index("--subscription") + 1] == "37deca37-c375-4a14-b90a-043849bd2bf1"
        assert args[args.index("--resource-group") + 1] == "78751-f36f3d5a"
        assert args[args.index("--name") + 1] == "clustermesh-96"
        Path(args[args.index("--file") + 1]).write_text("credentials-stay-private")
        """
    ), encoding="utf-8")
    az.chmod(0o755)
    calls_file = tmp_path / "calls.jsonl"
    artifacts = tmp_path / "artifacts"
    environment = {
        **os.environ, "PATH": f"{binaries}:{os.environ['PATH']}",
        "RUN_ID": SCOPE["target_run_id"], "CONFIRM_RESUME": SCOPE["confirm_resume"],
        "SUBSCRIPTION": SCOPE["expected_subscription_id"], "REGION": "eastus2euap",
        "TFVARS_PATH": TFVARS, "SOURCE_DIRECTORY": str(source), "ARTIFACT_DIRECTORY": str(artifacts),
        "SOURCE_STATE_BUILD_ID": "79945" if resuming else "79941",
        "REPOSITORY_DIRECTORY": str(checkout), "AGENT_TEMP_DIRECTORY": str(private),
        "CALLS": str(calls_file), "FAULT": fault,
    }
    result = subprocess.run(
        ["bash", "-c", operation["script"]], env=environment, capture_output=True,
        text=True, check=False, timeout=10,
    )
    assert result.returncode == expected_exit, result.stderr
    calls = [json.loads(row) for row in calls_file.read_text(encoding="utf-8").splitlines()] if calls_file.exists() else []
    assert len(calls) == expected_calls
    assert not list(private.iterdir())
    assert not any(b"credentials-stay-private" in path.read_bytes() for path in artifacts.rglob("*") if path.is_file())
    if calls:
        assert "--execute" not in calls[0]
    if len(calls) == 2:
        assert calls[1][-1] == "--execute"
        assert calls[0][:calls[0].index("--summary-file")] == calls[1][:calls[1].index("--summary-file")]


def test_restart_job_has_one_exact_source_and_bounded_publication():
    definition = job()
    assert definition["timeoutInMinutes"] == 60 and definition["cancelTimeoutInMinutes"] == 30
    downloads = [row for row in definition["steps"] if row.get("task") == "DownloadPipelineArtifact@2"]
    assert len(downloads) == 1
    assert downloads[0]["inputs"]["${{ if ne(parameters.source_state_build_id, 79945) }}"]["artifactName"] == (
        "n100-unreachable-worker-recovery-${{ parameters.source_state_build_id }}-1"
    )
    assert downloads[0]["inputs"]["${{ if eq(parameters.source_state_build_id, 79945) }}"]["artifactName"] == (
        "n100-retained-worker-restart-${{ parameters.source_state_build_id }}-1"
    )
    artifact = definition["steps"][-1]
    assert artifact["task"] == "PublishPipelineArtifact@1" and "always()" in artifact["condition"]
    assert artifact["inputs"]["artifact"] == "n100-retained-worker-restart-$(Build.BuildId)-$(System.JobAttempt)"
