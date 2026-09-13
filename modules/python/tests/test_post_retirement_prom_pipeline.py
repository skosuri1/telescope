"""Exclusive monitoring recovery, immutable retirement evidence, and private credentials."""

import json
import hashlib
import os
import re
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]
JOB = ROOT / "jobs/clustermesh-post-retirement-prom.yml"
STEP = ROOT / "steps/topology/clustermesh-scale/reuse/restore-post-retirement-prom.yml"
TFVARS = "scenarios/perf-eval/clustermesh-scale/terraform-inputs/azure-100-mock-shared.tfvars"
SCOPE = {
    "target_run_id": "78751-f36f3d5a", "confirm_resume": "78751-f36f3d5a",
    "expected_subscription_id": "37deca37-c375-4a14-b90a-043849bd2bf1",
    "expected_region": "eastus2euap", "expected_cluster_count": 100, "tfvars_path": TFVARS,
    "overlay_mode": "resume-existing", "run_workload": False, "retirement_build_id": 80001,
    "exclusive_modes": True,
}


def job():
    return yaml.safe_load(JOB.read_text(encoding="utf-8"))["jobs"][0]


@pytest.mark.parametrize("changes", [
    {}, {"target_run_id": "other"}, {"confirm_resume": "other"}, {"expected_subscription_id": "other"},
    {"expected_region": "westus"}, {"expected_cluster_count": 2}, {"expected_cluster_count": "100"},
    {"tfvars_path": "other"}, {"overlay_mode": "resume"}, {"run_workload": True},
    {"retirement_build_id": 79992}, {"retirement_build_id": "80001"}, {"exclusive_modes": False},
    {"run_workload": "false"}, {"exclusive_modes": "true"},
])
def test_monitoring_requires_exact_typed_completed_retirement_scope(changes):
    result = subprocess.run(
        ["bash", "-c", job()["steps"][0]["script"]],
        env={**os.environ, "PROM_SCOPE_JSON": json.dumps({**SCOPE, **changes})},
        capture_output=True, text=True, check=False, timeout=5,
    )
    assert (result.returncode == 0) is (not changes)


def test_monitoring_route_is_exclusive_and_only_in_approved_stage():
    pipeline = yaml.safe_load((ROOT / "pipelines/system/new-pipeline-test.yml").read_text(encoding="utf-8"))
    mode = "scaleDebugPostRetirementPromBuildId"
    parameter = next(row for row in pipeline["parameters"] if row["name"] == mode)
    assert parameter["type"] == "number" and parameter["default"] == 0
    stage = next(row for row in pipeline["stages"] if row["stage"] == "azure_eastus2euap_n100_debug_resume_37deca")
    route = "${{ if ne(parameters.scaleDebugPostRetirementPromBuildId, 0) }}"
    invocation = next(row[route][0] for row in stage["jobs"] if route in row)
    assert invocation["template"] == "/jobs/clustermesh-post-retirement-prom.yml"
    assert invocation["parameters"]["retirement_build_id"] == "${{ parameters.scaleDebugPostRetirementPromBuildId }}"
    assert invocation["parameters"]["run_workload"] == "${{ parameters.scaleDebugRunWorkload }}"
    for row in stage["jobs"]:
        condition = next(iter(row))
        if condition.startswith("${{") and condition != route:
            assert f"eq(parameters.{mode}, 0)" in condition
    exclusions = invocation["parameters"]["exclusive_modes"]
    for name in ("QualifiedWorkerRetirement", "CapacityQualification", "CapacityFirstRecovery",
                 "RetainedWorkerRestart", "ModernCniProm", "ModernBaseline", "CniWorkerResume",
                 "UnreachableWorkerObserve", "UnreachableWorkerReplaceFailedHost",
                 "UnreachableWorkerResumeReplacement", "QuotaRequestReceipt"):
        assert f"eq(parameters.scaleDebug{name}BuildId, 0)" in exclusions
    for name in ("ModernPromRecovery", "UnreachableWorkerRecoveryOnly", "UnreachableWorkerQuotaObserveOnly",
                 "UnreachableWorkerReimageFailedOs", "ArmRepairOnly", "PreparedRetirementOnly",
                 "PreparedRetirementObserveOnly", "CniWorkerMaintenanceOnly", "CniWorkerReplaceEmptyFresh"):
        assert f"not(parameters.scaleDebug{name})" in exclusions
    assert "eq(parameters.scaleDebugDv3QuotaRequestLimit, 0)" in exclusions
    assert all(mode not in json.dumps(other) for other in pipeline["stages"] if other is not stage)
    normal = yaml.safe_load((ROOT / "jobs/clustermesh-debug-resume.yml").read_text(encoding="utf-8"))
    assert next(row for row in normal["parameters"] if row["name"] == "post_retirement_prom_build_id")["default"] == 0
    assert "eq(${{ parameters.post_retirement_prom_build_id }}, 0)" in normal["jobs"][0]["condition"]
    normal_invocation = next(row for row in stage["jobs"] if row.get("template") == "/jobs/clustermesh-debug-resume.yml")
    assert normal_invocation["parameters"]["post_retirement_prom_build_id"] == "${{ parameters.scaleDebugPostRetirementPromBuildId }}"


def test_monitoring_publishes_readonly_plan_before_bounded_nonretrying_execution():
    definition = job()
    assert definition["timeoutInMinutes"] == 90 and definition["cancelTimeoutInMinutes"] == 30
    steps = definition["steps"]
    phases = [(index, row["parameters"]["phase"]) for index, row in enumerate(steps)
              if row.get("template") == "/steps/topology/clustermesh-scale/reuse/restore-post-retirement-prom.yml"]
    assert [phase for _, phase in phases] == ["plan", "execute"]
    publication = next(index for index, row in enumerate(steps)
                       if row.get("displayName") == "Publish monitoring restoration plan before any pool changes")
    assert phases[0][0] < publication < phases[1][0]
    download = next(row["inputs"] for row in steps if row.get("task") == "DownloadPipelineArtifact@2")
    assert download["artifactName"] == "n100-qualified-worker-retirement-${{ parameters.retirement_build_id }}-1"
    assert download["pipelineId"] == "${{ parameters.retirement_build_id }}"
    assert download["buildType"] == "specific" and download["specificBuildWithTriggering"] is False
    operation = yaml.safe_load(STEP.read_text(encoding="utf-8"))["steps"][0]
    assert operation["retryCountOnTaskFailure"] == 0
    assert operation["${{ if eq(parameters.phase, 'plan') }}"]["timeoutInMinutes"] == 15
    assert operation["${{ else }}"]["timeoutInMinutes"] == 65
    assert "always()" in steps[-1]["condition"] and "POST_RETIREMENT_PROM_DIAGNOSTICS_READY" in steps[-1]["condition"]
    assert steps[-1]["inputs"]["artifact"] == "n100-post-retirement-prom-$(Build.BuildId)-$(System.JobAttempt)"


def test_workload_artifact_selection_is_explicit_and_preserves_legacy_default():
    pipeline = yaml.safe_load((ROOT / "pipelines/system/new-pipeline-test.yml").read_text(encoding="utf-8"))
    parameter = next(row for row in pipeline["parameters"] if row["name"] == "scaleDebugModernBaselineArtifact")
    assert parameter["default"] == "n100-modern-cni"
    assert parameter["values"] == ["n100-modern-cni", "n100-post-retirement-prom"]
    stage = next(row for row in pipeline["stages"] if row["stage"] == "azure_eastus2euap_n100_debug_resume_37deca")
    invocation = next(row for row in stage["jobs"] if row.get("template") == "/jobs/clustermesh-debug-resume.yml")
    assert invocation["parameters"]["modern_baseline_artifact"] == "${{ parameters.scaleDebugModernBaselineArtifact }}"
    normal = yaml.safe_load((ROOT / "jobs/clustermesh-debug-resume.yml").read_text(encoding="utf-8"))
    parameter = next(row for row in normal["parameters"] if row["name"] == "modern_baseline_artifact")
    assert parameter["default"] == "n100-modern-cni"
    condition = "${{ if or(gt(parameters.modern_baseline_build_id, 0), ne(parameters.modern_baseline_artifact, 'n100-modern-cni')) }}"
    steps = next(row[condition] for row in normal["jobs"][0]["steps"] if condition in row)
    download = next(row["inputs"] for row in steps if row.get("task") == "DownloadPipelineArtifact@2")
    assert download["artifactName"] == "${{ parameters.modern_baseline_artifact }}-${{ parameters.modern_baseline_build_id }}-1"
    assert download["pipelineId"] == "${{ parameters.modern_baseline_build_id }}"
    assert "202" in steps[0]["script"] and "SOURCE_BUILD_ID" in steps[0]["script"]


@pytest.mark.parametrize("changes", [
    {}, {"RUN_WORKLOAD": "false"}, {"PRESERVATION_MODE": "verify"}, {"EXPECTED_POOLS": "201"},
    {"SOURCE_BUILD_ID": "0"}, {"SOURCE_BUILD_ID": "-1"}, {"SOURCE_BUILD_ID": "unresolved"},
])
def test_workload_modern_source_requires_positive_build_and_workload_mode(changes):
    definition = yaml.safe_load((ROOT / "jobs/clustermesh-debug-resume.yml").read_text(encoding="utf-8"))
    condition = "${{ if or(gt(parameters.modern_baseline_build_id, 0), ne(parameters.modern_baseline_artifact, 'n100-modern-cni')) }}"
    steps = next(row[condition] for row in definition["jobs"][0]["steps"] if condition in row)
    result = subprocess.run(
        ["bash", "-c", steps[0]["script"]],
        env={**os.environ, "RUN_WORKLOAD": "true", "PRESERVATION_MODE": "none",
             "EXPECTED_POOLS": "202", "SOURCE_BUILD_ID": "80002", **changes},
        capture_output=True, text=True, check=False, timeout=5,
    )
    assert (result.returncode == 0) is (not changes)


@pytest.mark.parametrize("fault", ["none", "legacy", "missing-source", "source-symlink", "nested-symlink", "existing-output"])
def test_workload_wrapper_preserves_the_complete_retirement_lineage(tmp_path, fault):
    definition = yaml.safe_load(
        (ROOT / "steps/topology/clustermesh-scale-mock/verified-workload-handoff.yml").read_text(encoding="utf-8"),
    )
    script = next(row["script"] for row in definition["steps"]
                  if row.get("displayName") == "Restore verified n100 KWOK state before workloads")
    script = script.replace("$(Build.ArtifactStagingDirectory)", str(tmp_path / "artifacts"))
    script = script.replace("$(Pipeline.Workspace)", str(tmp_path / "workspace"))
    for name, value in {
        "baseline_build_id": 79230, "verification_build_id": 79261,
        "expected_subscription_id": SCOPE["expected_subscription_id"], "expected_cluster_count": 100,
        "expected_mock_count": 100, "expected_pool_count": 202,
    }.items():
        script = script.replace("${{ parameters." + name + " }}", str(value))
    source = tmp_path / "source"
    source.mkdir()
    retirement = source / "retirement-input"
    if fault != "missing-source":
        (retirement / "qualification-input").mkdir(parents=True)
        (retirement / "retirement.json").write_text('{"retired":true}', encoding="utf-8")
        (retirement / "qualification-input/probes.json").write_text('{"passed":32}', encoding="utf-8")
    receipt = {} if fault == "legacy" else {"retirement_build_id": 80001}
    if fault not in ("legacy", "missing-source"):
        receipt["retirement_sha256"] = hashlib.sha256((retirement / "retirement.json").read_bytes()).hexdigest()
    proof = source / "recovery.json"
    proof.write_text(json.dumps(receipt), encoding="utf-8")
    if fault == "source-symlink":
        actual = source / "actual-retirement"
        retirement.rename(actual)
        retirement.symlink_to(actual, target_is_directory=True)
    if fault == "nested-symlink":
        (retirement / "link").symlink_to(retirement / "retirement.json")
    if fault == "legacy":
        (retirement / "qualification-input/probes.json").unlink()
        (retirement / "qualification-input").rmdir()
        (retirement / "retirement.json").unlink()
        retirement.rmdir()
    artifacts = tmp_path / "artifacts/n100-workload-handoff"
    artifacts.mkdir(parents=True)
    if fault == "existing-output":
        (artifacts / "retirement-input").mkdir()
    helper = tmp_path / "handoff.py"
    helper.write_text(textwrap.dedent(
        """
        import hashlib
        import json
        import os
        import sys
        from pathlib import Path
        args = sys.argv[1:]
        proof = Path(args[args.index("--modern-baseline-proof") + 1])
        assert proof.name == "modern-baseline-source.json"
        receipt = json.loads(proof.read_text())
        retirement = proof.parent / "retirement-input"
        if os.environ["FAULT"] == "legacy":
            assert not retirement.exists()
        else:
            assert hashlib.sha256((retirement / "retirement.json").read_bytes()).hexdigest() == receipt["retirement_sha256"]
            assert json.loads((retirement / "qualification-input/probes.json").read_text()) == {"passed": 32}
        Path(os.environ["CALLS"]).write_text(json.dumps(args))
        """
    ), encoding="utf-8")
    calls = tmp_path / "calls.json"
    result = subprocess.run(
        ["bash", "-c", script],
        env={**os.environ, "HOME": str(tmp_path), "MODERN_BASELINE_PROOF": str(proof),
             "PRESERVED_MOCK_HANDOFF": str(helper), "MOCK_LAYER_RECONCILER": "unused",
             "RUN_ID": SCOPE["target_run_id"], "FAULT": fault, "CALLS": str(calls)},
        capture_output=True, text=True, check=False, timeout=10,
    )
    assert (result.returncode == 0) is (fault in ("none", "legacy")), result.stderr
    assert calls.exists() is (fault in ("none", "legacy"))
    assert json.loads(proof.read_text(encoding="utf-8")) == receipt


@pytest.mark.parametrize("fault,count", [
    ("none", 2), ("not-fenced", 0), ("not-retired", 0), ("not-ready", 0), ("hold-remains", 0),
    ("kwok-unready", 0), ("unclean", 0), ("initial-symlink", 0), ("existing-output", 0),
    ("output-alias", 0), ("credentials-plan", 0), ("plan-error", 1), ("unsafe-plan", 1),
    ("mutating-plan", 1), ("input-change", 1), ("tfvars-change", 1), ("between-change", 1),
    ("between-symlink", 1), ("missing-freeze", 1), ("credentials-execute", 1),
    ("execute-error", 2), ("not-repaired", 2), ("workload-claim", 2), ("wrong-layout", 2),
    ("incomplete-cni", 2), ("cni-source-not-retired", 2),
])
def test_monitoring_freezes_full_retirement_tree_and_cleans_private_credentials(tmp_path, fault, count):
    script = yaml.safe_load(STEP.read_text(encoding="utf-8"))["steps"][0]["script"]
    source, checkout, private, binaries = (tmp_path / name for name in ("input", "checkout", "private", "bin"))
    for path in (source, checkout, private, binaries):
        path.mkdir()
    retirement = {
        "source_build": 80001, "success": True, "native_fencing_proven": fault != "not-fenced",
        "source_retired": fault != "not-retired", "replacements_ready": fault != "not-ready",
        "placement_hold_removed": fault != "hold-remains", "current_mock_ready": 100,
        "kwok_ready": 0 if fault == "kwok-unready" else 100,
        "cleanup_errors": ["unsafe"] if fault == "unclean" else [],
    }
    (source / "retirement.json").write_text(json.dumps(retirement), encoding="utf-8")
    (source / "qualification-input/nested").mkdir(parents=True)
    (source / "qualification-input/nested/evidence.json").write_text('{"fixed":true}', encoding="utf-8")
    if fault == "initial-symlink":
        (source / "link.json").symlink_to(source / "retirement.json")
    tfvars = checkout / TFVARS
    tfvars.parent.mkdir(parents=True)
    tfvars.write_text("original-tfvars", encoding="utf-8")
    helper = checkout / "modules/python/clusterloader2/clustermesh-scale/post_retirement_prom_recovery.py"
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
        assert value("--retirement-build-id") == "80001" and value("--timeout-seconds") == "3600"
        assert value("--resource-group") == value("--confirm-resource-group") == "78751-f36f3d5a"
        assert value("--expected-subscription") == "37deca37-c375-4a14-b90a-043849bd2bf1"
        assert value("--expected-region") == "eastus2euap" and value("--context") == "clustermesh-96"
        assert len(value("--expected-tfvars-sha")) == 64
        source = Path(value("--retirement-directory"))
        assert json.loads((source / "retirement.json").read_text())["source_build"] == 80001
        config = Path(value("--kubeconfig"))
        assert config.read_text() == "private-monitoring-config" and config.stat().st_mode & 0o777 == 0o600
        output = Path(value("--summary-file"))
        assert source not in output.parents and output != config
        fault = os.environ["FAULT"]
        if not execute and fault == "input-change":
            (source / "qualification-input/nested/evidence.json").write_text("changed")
        if not execute and fault == "tfvars-change":
            (Path(os.environ["REPOSITORY_DIRECTORY"]) / os.environ["TFVARS_PATH"]).write_text("changed")
        output.write_text(json.dumps({
            "execute": execute, "mutation_started": execute or fault == "mutating-plan",
            "plan_valid": fault != "unsafe-plan", "success": execute,
            "repaired": execute and fault != "not-repaired", "workloads_ready": fault == "workload-claim",
            "modern_cni": {"completed": fault != "incomplete-cni", "source_retired": fault != "cni-source-not-retired"},
            "baseline_pool_layout": {"expected_total_pool_count": 201 if fault == "wrong-layout" else 202},
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
        Path(args[args.index("--file") + 1]).write_text("private-monitoring-config")
        sys.exit(1 if os.environ["FAULT"] == "credentials-" + os.environ["PHASE"] else 0)
        """
    ), encoding="utf-8")
    az.chmod(0o755)
    artifacts = source if fault == "output-alias" else tmp_path / "artifacts"
    if fault == "existing-output":
        (artifacts / "n100-post-retirement-prom").mkdir(parents=True)
    calls_file = tmp_path / "calls.jsonl"
    environment = {
        **os.environ, "PATH": f"{binaries}:{os.environ['PATH']}", "PHASE": "plan",
        "RUN_ID": SCOPE["target_run_id"], "CONFIRM_RESUME": SCOPE["confirm_resume"],
        "SUBSCRIPTION": SCOPE["expected_subscription_id"], "REGION": SCOPE["expected_region"],
        "RETIREMENT_BUILD_ID": "80001", "RETIREMENT_DIRECTORY": str(source),
        "TFVARS_PATH": TFVARS, "REPOSITORY_DIRECTORY": str(checkout), "AGENT_TEMP_DIRECTORY": str(private),
        "ARTIFACT_DIRECTORY": str(artifacts), "INPUTS_SHA": "", "TFVARS_SHA": "",
        "FAULT": fault, "CALLS": str(calls_file),
    }
    result = subprocess.run(["bash", "-c", script], env=environment, capture_output=True,
                            text=True, check=False, timeout=15)
    assert not list(private.iterdir())
    if result.returncode == 0:
        hashes = dict(re.findall(
            r"variable=(POST_RETIREMENT_PROM_(?:INPUTS|TFVARS)_SHA);isReadOnly=true]([0-9a-f]{64})",
            result.stdout,
        ))
        assert len(hashes) == 2
        evidence = artifacts / "n100-post-retirement-prom/retirement-input/qualification-input/nested/evidence.json"
        if fault == "between-change":
            evidence.write_text("changed-between-phases", encoding="utf-8")
        if fault == "between-symlink":
            evidence.unlink()
            evidence.symlink_to(source / "qualification-input/nested/evidence.json")
        environment.update(
            PHASE="execute", INPUTS_SHA="" if fault == "missing-freeze" else hashes["POST_RETIREMENT_PROM_INPUTS_SHA"],
            TFVARS_SHA=hashes["POST_RETIREMENT_PROM_TFVARS_SHA"],
        )
        result = subprocess.run(["bash", "-c", script], env=environment, capture_output=True,
                                text=True, check=False, timeout=15)
    assert (result.returncode == 0) is (fault == "none"), result.stderr
    calls = [json.loads(line) for line in calls_file.read_text(encoding="utf-8").splitlines()] if calls_file.exists() else []
    assert len(calls) == count and not list(private.iterdir()), result.stderr
    if calls:
        assert "--execute" not in calls[0]
    if len(calls) == 2:
        assert "--execute" in calls[1]
    assert not any(b"private-monitoring-config" in path.read_bytes()
                   for path in artifacts.rglob("*") if path.is_file() and not path.is_symlink())
