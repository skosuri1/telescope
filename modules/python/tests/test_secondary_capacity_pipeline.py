"""Exclusive four-role capacity routing and private immutable phase execution."""

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
JOB = ROOT / "jobs/clustermesh-secondary-capacity.yml"
STEP = ROOT / "steps/topology/clustermesh-scale/reuse/secondary-capacity.yml"
TFVARS = "scenarios/perf-eval/clustermesh-scale/terraform-inputs/azure-100-mock-shared.tfvars"
SCOPE = {
    "target_run_id": "78751-f36f3d5a", "confirm_resume": "78751-f36f3d5a",
    "expected_subscription_id": "37deca37-c375-4a14-b90a-043849bd2bf1",
    "expected_region": "eastus2euap", "expected_cluster_count": 100,
    "tfvars_path": TFVARS, "overlay_mode": "resume-existing", "run_workload": False,
    "source_build_id": 80022, "exclusive_modes": True,
}


@pytest.mark.parametrize("change", [
    {}, {"target_run_id": "other"}, {"confirm_resume": "other"}, {"expected_subscription_id": "other"},
    {"expected_region": "westus"}, {"expected_cluster_count": "100"}, {"expected_cluster_count": 2},
    {"tfvars_path": "other"}, {"overlay_mode": "resume"}, {"run_workload": True},
    {"source_build_id": 80017}, {"source_build_id": "80022"}, {"exclusive_modes": False},
])
def test_only_exact_diagnostic_source_and_capacity_scope_is_admitted(change):
    job = yaml.safe_load(JOB.read_text(encoding="utf-8"))["jobs"][0]
    result = subprocess.run(["bash", "-c", job["steps"][0]["script"]],
                            env={**os.environ, "SCOPE_JSON": json.dumps({**SCOPE, **change})},
                            capture_output=True, text=True, timeout=5, check=False)
    assert (result.returncode == 0) is (not change)


def test_capacity_mode_is_exclusive_and_plan_publication_precedes_actions():
    pipeline = yaml.safe_load((ROOT / "pipelines/system/new-pipeline-test.yml").read_text(encoding="utf-8"))
    stage = next(row for row in pipeline["stages"] if row["stage"] == "azure_eastus2euap_n100_debug_resume_37deca")
    outer = "${{ if ne(parameters.scaleDebugPostRetirementPromBuildId, 0) }}"
    branches = next(row[outer] for row in stage["jobs"] if outer in row)
    branch = "${{ elseif eq(parameters.scaleDebugPostRetirementPromBuildId, 80022) }}"
    invocation = next(row[branch][0] for row in branches if branch in row)
    assert invocation["template"] == "/jobs/clustermesh-secondary-capacity.yml"
    assert "eq(parameters.scaleDebugModernBaselineBuildId, 0)" in invocation["parameters"]["exclusive_modes"]
    job = yaml.safe_load(JOB.read_text(encoding="utf-8"))["jobs"][0]
    assert job["timeoutInMinutes"] == 160 and job["cancelTimeoutInMinutes"] == 30
    accepted = next(row for row in job["steps"]
                    if row.get("inputs", {}).get("pipelineId") == 80029)
    assert accepted["inputs"]["artifactName"] == "n100-secondary-capacity-80029-1"
    assert accepted["inputs"]["allowFailedBuilds"] is True
    assert "itemPattern" not in accepted["inputs"]
    phases = [(index, row["parameters"]["phase"]) for index, row in enumerate(job["steps"])
              if row.get("template") == "/steps/topology/clustermesh-scale/reuse/secondary-capacity.yml"]
    publish = next(index for index, row in enumerate(job["steps"])
                   if row.get("displayName") == "Publish the live capacity-only plan before any pool changes")
    assert [phase for _, phase in phases] == ["plan", "execute"] and phases[0][0] < publish < phases[1][0]
    operation = yaml.safe_load(STEP.read_text(encoding="utf-8"))["steps"][0]
    assert operation["retryCountOnTaskFailure"] == 0
    assert operation["${{ if eq(parameters.phase, 'plan') }}"]["timeoutInMinutes"] == 25
    assert operation["${{ else }}"]["timeoutInMinutes"] == 125


@pytest.mark.parametrize("fault,count", [
    ("none", 2), ("bad-source", 0), ("symlink", 0), ("existing-output", 0),
    ("credential-failure", 0), ("unsafe-plan", 1), ("mutating-plan", 1),
    ("source-change", 1), ("tfvars-change", 1), ("between-change", 1), ("missing-hash", 1),
    ("execute-error", 2), ("no-capacity", 2), ("false-qualification", 2), ("workload-claim", 2),
    ("accepted-symlink", 0), ("accepted-source-change", 1), ("accepted-between-change", 1),
])
def test_capacity_wrapper_freezes_source_and_removes_all_four_private_configs(tmp_path, fault, count):
    script = yaml.safe_load(STEP.read_text(encoding="utf-8"))["steps"][0]["script"]
    source, accepted, checkout, private, binaries = (
        tmp_path / name for name in ("source", "accepted", "checkout", "private", "bin")
    )
    for directory in (source, accepted, checkout, private, binaries):
        directory.mkdir()
    (accepted / "recovery.json").write_text('{"accepted":true}', encoding="utf-8")
    if fault == "accepted-symlink":
        (accepted / "linked").symlink_to(accepted / "recovery.json")
    (source / "summary.json").write_text(json.dumps({
        "read_only": fault != "bad-source", "resource_mutations": 0, "source_build_id": 80017,
    }), encoding="utf-8")
    (source / "mesh-51").mkdir()
    (source / "mesh-51/nodes.json").write_text('{"preserved":true}', encoding="utf-8")
    if fault == "symlink":
        (source / "bad-link").symlink_to(source / "summary.json")
    tfvars = checkout / TFVARS
    tfvars.parent.mkdir(parents=True)
    tfvars.write_text("original", encoding="utf-8")
    helper = checkout / "modules/python/clusterloader2/clustermesh-scale/secondary_capacity_recovery.py"
    helper.parent.mkdir(parents=True)
    helper.write_text(textwrap.dedent(
        """
        import json,os,sys
        from pathlib import Path
        args=sys.argv[1:]
        def value(name): return args[args.index(name)+1]
        execute='--execute' in args
        assert value('--source-build-id')=='80022' and value('--timeout-seconds')=='7200'
        assert value('--resume-build-id')=='80029'
        accepted=Path(value('--resume-directory'))
        assert accepted.name=='accepted-input' and (accepted/'recovery.json').is_file()
        assert value('--resource-group')==value('--confirm-resource-group')=='78751-f36f3d5a'
        configs=Path(value('--kubeconfig-directory'))
        assert {p.name for p in configs.iterdir()}=={f'mesh-{n}.config' for n in (51,66,79,89)}
        assert all(p.read_text()=='private-test-credential' and p.stat().st_mode & 0o777==0o600 for p in configs.iterdir())
        source=Path(value('--source-directory'))
        with Path(os.environ['CALLS']).open('a') as handle: handle.write(json.dumps(args)+'\\n')
        fault=os.environ['FAULT']
        if not execute and fault=='source-change': (source/'mesh-51/nodes.json').write_text('changed')
        if not execute and fault=='accepted-source-change': (accepted/'recovery.json').write_text('changed')
        if not execute and fault=='tfvars-change': (Path(os.environ['REPOSITORY_DIRECTORY'])/os.environ['TFVARS_PATH']).write_text('changed')
        Path(value('--summary-file')).write_text(json.dumps({
            'execute':execute,'mutation_started':execute or fault=='mutating-plan',
            'plan_valid':fault!='unsafe-plan','success':execute,'workloads_ready':fault=='workload-claim',
            'capacity_created':fault!='no-capacity','initial_network_ready':True,
            'capacity_qualified':fault=='false-qualification','completed_global_baseline':False,
        }))
        sys.exit(1 if execute and fault=='execute-error' else 0)
        """
    ), encoding="utf-8")
    az = binaries / "az"
    az.write_text(f"#!{sys.executable}\n" + textwrap.dedent(
        """
        import os,sys
        from pathlib import Path
        args=sys.argv[1:]
        assert args[:2]==['aks','get-credentials']
        name=args[args.index('--name')+1]
        assert name in {f'clustermesh-{n}' for n in (51,66,79,89)}
        assert args[args.index('--context')+1]==name
        assert args[args.index('--subscription')+1]=='37deca37-c375-4a14-b90a-043849bd2bf1'
        Path(args[args.index('--file')+1]).write_text('private-test-credential')
        sys.exit(1 if os.environ['FAULT']=='credential-failure' and name=='clustermesh-66' else 0)
        """
    ), encoding="utf-8")
    az.chmod(0o755)
    artifacts = tmp_path / "artifacts"
    if fault == "existing-output":
        (artifacts / "n100-secondary-capacity").mkdir(parents=True)
    calls = tmp_path / "calls.jsonl"
    env = {
        **os.environ, "PATH": f"{binaries}:{os.environ['PATH']}", "PHASE": "plan",
        "SOURCE_DIRECTORY": str(source), "SOURCE_BUILD_ID": "80022", "RUN_ID": SCOPE["target_run_id"],
        "ACCEPTED_DIRECTORY": str(accepted),
        "CONFIRM_RESUME": SCOPE["confirm_resume"], "SUBSCRIPTION": SCOPE["expected_subscription_id"],
        "REGION": SCOPE["expected_region"], "TFVARS_PATH": TFVARS,
        "REPOSITORY_DIRECTORY": str(checkout), "AGENT_TEMP_DIRECTORY": str(private),
        "ARTIFACT_DIRECTORY": str(artifacts), "INPUTS_SHA": "", "TFVARS_SHA": "",
        "FAULT": fault, "CALLS": str(calls),
    }
    result = subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True, timeout=15, check=False)
    assert not list(private.iterdir())
    if result.returncode == 0:
        hashes = dict(re.findall(r"variable=(SECONDARY_CAPACITY_(?:INPUTS|TFVARS)_SHA);isReadOnly=true]([0-9a-f]{64})",
                                 result.stdout))
        assert len(hashes) == 2
        if fault == "between-change":
            (artifacts / "n100-secondary-capacity/source-input/mesh-51/nodes.json").write_text("changed", encoding="utf-8")
        if fault == "accepted-between-change":
            (artifacts / "n100-secondary-capacity/accepted-input/recovery.json").write_text("changed", encoding="utf-8")
        env.update(PHASE="execute", INPUTS_SHA="" if fault == "missing-hash" else hashes["SECONDARY_CAPACITY_INPUTS_SHA"],
                   TFVARS_SHA=hashes["SECONDARY_CAPACITY_TFVARS_SHA"])
        result = subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True, timeout=15, check=False)
    assert (result.returncode == 0) is (fault == "none"), result.stderr
    invocations = calls.read_text(encoding="utf-8").splitlines() if calls.exists() else []
    assert len(invocations) == count and not list(private.iterdir())
    assert not any(b"private-test-credential" in path.read_bytes() for path in artifacts.rglob("*")
                   if path.is_file() and not path.is_symlink())
