"""Exercise the modern CNI job's immutable inputs and private credentials."""

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]
JOB = ROOT / "jobs/clustermesh-modern-cni.yml"
SCOPE = {
    "target_run_id": "78751-f36f3d5a", "confirm_resume": "78751-f36f3d5a",
    "expected_subscription_id": "37deca37-c375-4a14-b90a-043849bd2bf1",
    "expected_region": "eastus2euap", "expected_cluster_count": 100,
    "overlay_mode": "resume-existing", "run_workload": False,
    "original_plan_build_id": 79880, "modern_prom_build_id": 99999, "exclusive_modes": True,
}


def job():
    return yaml.safe_load(JOB.read_text(encoding="utf-8"))["jobs"][0]


@pytest.mark.parametrize("change", [
    {}, {"target_run_id": "other"}, {"confirm_resume": ""}, {"expected_subscription_id": "other"},
    {"expected_region": "eastus"}, {"expected_cluster_count": 2}, {"run_workload": True},
    {"overlay_mode": "resume"}, {"original_plan_build_id": 0}, {"modern_prom_build_id": 0},
    {"exclusive_modes": False},
])
def test_modern_cni_requires_exact_scope_and_complete_checkpoint(change):
    result = subprocess.run(
        ["bash", "-c", job()["steps"][0]["script"]],
        env={**os.environ, "RECOVERY_SCOPE_JSON": json.dumps({**SCOPE, **change})},
        capture_output=True, text=True, check=False, timeout=10,
    )
    assert (result.returncode == 0) is (not change), result.stderr


@pytest.mark.parametrize("fault,expected_count,expected_code", [
    ("none", 2, 0), ("plan", 1, 7), ("unsafe-plan", 1, 1),
    ("changed-plan", 1, 1), ("changed-monitoring", 1, 1), ("execute", 2, 8),
])
def test_modern_cni_plan_execute_and_private_input_contract(tmp_path, fault, expected_count, expected_code):
    source = job()["steps"][4]["script"]
    source = source.replace("$(Build.ArtifactStagingDirectory)", str(tmp_path / "artifacts"))
    source = source.replace("$(Agent.TempDirectory)", str(tmp_path / "private"))
    source = source.replace("$(Pipeline.Workspace)/s", str(ROOT))
    source = source.replace("$(Pipeline.Workspace)", str(tmp_path / "workspace"))
    (tmp_path / "private").mkdir()
    for name, file in (("modern-cni-original", "input-plan.json"), ("modern-cni-monitoring", "recovery.json")):
        directory = tmp_path / "workspace" / name
        directory.mkdir(parents=True)
        (directory / file).write_text('{"immutable":"source"}', encoding="utf-8")
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
            with Path(os.environ["CALLS"]).open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(args) + "\\n")
            config = Path(args[args.index("--kubeconfig") + 1])
            assert config.is_file() and config.stat().st_mode & 0o777 == 0o600
            assert config.parent.stat().st_mode & 0o777 == 0o700
            execute = "--execute" in args
            fault = os.environ["FAULT"]
            Path(args[args.index("--summary-file") + 1]).write_text(json.dumps({
                "execute": execute, "mutation_started": fault == "unsafe-plan", "plan_valid": True,
            }), encoding="utf-8")
            if not execute and fault in ("changed-plan", "changed-monitoring"):
                flag = "--plan-file" if fault == "changed-plan" else "--modern-prom-checkpoint"
                Path(args[args.index(flag) + 1]).write_text("{}", encoding="utf-8")
            if not execute and fault == "plan":
                sys.exit(7)
            if execute and fault == "execute":
                sys.exit(8)
            """
        ), encoding="utf-8",
    )
    fake_python.chmod(0o755)
    fake_az = tmp_path / "az"
    fake_az.write_text(
        f"#!{sys.executable}\n" + textwrap.dedent(
            """
            import sys
            from pathlib import Path
            args = sys.argv[1:]
            assert args[:2] == ["aks", "get-credentials"]
            assert args[args.index("--resource-group") + 1] == "78751-f36f3d5a"
            assert args[args.index("--name") + 1] == "clustermesh-96"
            Path(args[args.index("--file") + 1]).write_text("fake private config", encoding="utf-8")
            """
        ), encoding="utf-8",
    )
    fake_az.chmod(0o755)
    result = subprocess.run(
        ["bash", "-c", source],
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}", "CALLS": str(calls_file), "FAULT": fault,
             "RUN_ID": SCOPE["target_run_id"], "CONFIRM_RESUME": SCOPE["confirm_resume"],
             "SUBSCRIPTION": SCOPE["expected_subscription_id"], "REGION": SCOPE["expected_region"],
             "TFVARS_PATH": "scenarios/perf-eval/clustermesh-scale/terraform-inputs/azure-100-mock-shared.tfvars"},
        capture_output=True, text=True, check=False, timeout=10,
    )
    assert result.returncode == expected_code, result.stderr
    calls = [json.loads(row) for row in calls_file.read_text(encoding="utf-8").splitlines()]
    assert len(calls) == expected_count and "--execute" not in calls[0]
    assert not list((tmp_path / "private").iterdir())
    if len(calls) == 2:
        assert calls[0][:calls[0].index("--summary-file")] == calls[1][:calls[1].index("--summary-file")]
        assert "--execute" in calls[1]


def test_modern_cni_receipt_disables_legacy_and_normal_jobs():
    pipeline = yaml.safe_load((ROOT / "pipelines/system/new-pipeline-test.yml").read_text(encoding="utf-8"))
    stage = next(row for row in pipeline["stages"] if row.get("stage") == "azure_eastus2euap_n100_debug_resume_37deca")
    key = "${{ if and(eq(parameters.scaleDebugCapacityFirstRecoveryBuildId, 0), eq(parameters.scaleDebugRetainedWorkerRestartBuildId, 0), ne(parameters.scaleDebugModernCniPromBuildId, 0), eq(parameters.scaleDebugDv3QuotaRequestLimit, 0), eq(parameters.scaleDebugQuotaRequestReceiptBuildId, 0)) }}"
    entry = next(row[key][0] for row in stage["jobs"] if key in row)
    assert entry["template"] == "/jobs/clustermesh-modern-cni.yml"
    for row in stage["jobs"][:4]:
        assert "eq(parameters.scaleDebugModernCniPromBuildId, 0)" in next(iter(row))
    normal = yaml.safe_load((ROOT / "jobs/clustermesh-debug-resume.yml").read_text(encoding="utf-8"))["jobs"][0]
    assert "eq(variables['CLUSTERMESH_MODERN_CNI_PROM_BUILD_ID'], '0')" in normal["condition"]
    assert job()["steps"][4]["retryCountOnTaskFailure"] == 0
    assert job()["steps"][-1]["inputs"]["artifact"].startswith("n100-modern-cni-")
