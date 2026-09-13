"""Pipeline boundary for three independently qualified System-role retirements."""

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
JOB = ROOT / "jobs/clustermesh-secondary-worker-retirement.yml"
STEP = ROOT / "steps/topology/clustermesh-scale/reuse/retire-secondary-workers.yml"
TFVARS = "scenarios/perf-eval/clustermesh-scale/terraform-inputs/azure-100-mock-shared.tfvars"
SCOPE = {
    "target_run_id": "78751-f36f3d5a", "confirm_resume": "78751-f36f3d5a",
    "expected_subscription_id": "37deca37-c375-4a14-b90a-043849bd2bf1",
    "expected_region": "eastus2euap", "expected_cluster_count": 100,
    "tfvars_path": TFVARS, "overlay_mode": "resume-existing",
    "run_workload": False, "qualification_build_id": 80046, "exclusive_modes": True,
}


@pytest.mark.parametrize("change", [
    {}, {"target_run_id": "other"}, {"confirm_resume": "other"},
    {"expected_subscription_id": "other"}, {"expected_region": "other"},
    {"expected_cluster_count": 2}, {"expected_cluster_count": "100"},
    {"tfvars_path": "other"}, {"overlay_mode": "reset"}, {"run_workload": True},
    {"qualification_build_id": 79986}, {"qualification_build_id": "80046"},
    {"exclusive_modes": False},
])
def test_only_exact_partial_qualification_scope_is_admitted(change):
    job = yaml.safe_load(JOB.read_text())["jobs"][0]
    result = subprocess.run(["bash", "-c", job["steps"][0]["script"]],
                            env={**os.environ, "SCOPE_JSON": json.dumps({**SCOPE, **change})},
                            capture_output=True, text=True, timeout=5, check=False)
    assert (result.returncode == 0) is (not change)


def test_route_excludes_legacy_retirement_and_downloads_whole_source():
    pipeline = yaml.safe_load((ROOT / "pipelines/system/new-pipeline-test.yml").read_text())
    assert len(pipeline["stages"]) == 43
    stage = next(row for row in pipeline["stages"] if row["stage"] == "azure_eastus2euap_n100_debug_resume_37deca")
    route = "${{ if and(eq(parameters.scaleDebugPostRetirementPromBuildId, 0), eq(parameters.scaleDebugQualifiedWorkerRetirementBuildId, 80046)) }}"
    selected = next(row[route][0] for row in stage["jobs"] if route in row)
    assert selected["template"] == "/jobs/clustermesh-secondary-worker-retirement.yml"
    legacy = next(next(iter(row)) for row in stage["jobs"]
                  if "/jobs/clustermesh-qualified-worker-retirement.yml" in str(row))
    assert "ne(parameters.scaleDebugQualifiedWorkerRetirementBuildId, 80046)" in legacy
    job = yaml.safe_load(JOB.read_text())["jobs"][0]
    download = next(row["inputs"] for row in job["steps"] if row.get("task") == "DownloadPipelineArtifact@2")
    assert download["allowFailedBuilds"] is True and "itemPattern" not in download
    assert job["timeoutInMinutes"] == 160 and job["cancelTimeoutInMinutes"] == 30
    phases = [row["parameters"]["phase"] for row in job["steps"] if row.get("template") == "/" + str(STEP.relative_to(ROOT))]
    assert phases == ["plan", "execute"]
    assert "always()" in job["steps"][-1]["condition"]


@pytest.mark.parametrize("fault,calls_expected", [
    ("none", 2), ("unqualified-role", 0), ("prom-claimed", 0), ("source-symlink", 0),
    ("credentials", 0), ("source-change", 1), ("tfvars-change", 1),
    ("plan-change", 1), ("execute-failed", 2), ("global-claim", 2),
])
def test_partial_source_freeze_and_three_private_credentials(tmp_path, fault, calls_expected):
    source, checkout, private, binaries = (tmp_path / name for name in ("source", "checkout", "private", "bin"))
    for directory in (source, checkout, private, binaries):
        directory.mkdir()
    per_role = {role: {"capacity_qualified": True, "actual_ip_growth_proven": True,
                      "actual_headroom_proven": True, "probe_cleanup_pending": []}
                for role in ("mesh-51", "mesh-66", "mesh-79")}
    per_role["mesh-89"] = {"capacity_qualified": fault == "prom-claimed"}
    if fault == "unqualified-role":
        per_role["mesh-66"]["capacity_qualified"] = False
    (source / "qualification.json").write_text(json.dumps({
        "execute": True, "success": False, "capacity_source_build_id": 80039,
        "error": "mesh-89: actual NNC version/IP growth exceeded the bounded wait",
        "workloads_ready": False, "cleanup_errors": [], "per_role": per_role,
    }))
    (source / "raw.json").write_text('{"preserved":true}')
    if fault == "source-symlink":
        (source / "bad").symlink_to(source / "raw.json")
    tfvars = checkout / TFVARS
    tfvars.parent.mkdir(parents=True)
    tfvars.write_text("original")
    helper = checkout / "modules/python/clusterloader2/clustermesh-scale/secondary_failed_worker_retirement.py"
    helper.parent.mkdir(parents=True)
    helper.write_text(textwrap.dedent(
        """
        import json,os,sys
        from pathlib import Path
        args=sys.argv[1:]
        def value(flag): return args[args.index(flag)+1]
        assert value('--qualification-build-id')=='80046'
        assert value('--timeout-seconds')=='7200'
        configs=Path(value('--kubeconfig-directory'))
        assert {p.name for p in configs.iterdir()}=={f'mesh-{n}.config' for n in (51,66,79)}
        assert all(p.read_text()=='private-retirement-credential' and p.stat().st_mode & 0o777==0o600 for p in configs.iterdir())
        execute='--execute' in args
        with Path(os.environ['CALLS']).open('a') as handle: handle.write(json.dumps(args)+'\\n')
        roles={role:{
            'success':True,'native_fencing_proven':True,'source_retired':True,
            'replacements_ready':True,'placement_holds_removed':True,
            'native':{'attempted':True,'submission_started':True,'accepted':True,
                      'ambiguous':False,'vm_absence_observed_at':'recorded'},
            'final_headroom':{'actual_metrics':True},
            'holds':{'old':{'applied':False,'remove':{'accepted':True,'ambiguous':False}}},
            'replacements':{str(i):{} for i in range(count)},
        } for role,count in (('mesh-51',61),('mesh-66',48),('mesh-79',40))}
        Path(value('--summary-file')).write_text(json.dumps({
            'execute':execute,'mutation_started':execute,'plan_valid':True,
            'success':execute,'system_roles_recovered':True,'workloads_ready':False,
            'completed_global_baseline':os.environ['FAULT']=='global-claim','cleanup_errors':[],
            'qualification_build_id':80046,'mesh89_explicitly_unqualified':True,'per_role':roles,
        }))
        sys.exit(1 if execute and os.environ['FAULT']=='execute-failed' else 0)
        """
    ))
    az = binaries / "az"
    az.write_text(f"#!{sys.executable}\n" + textwrap.dedent(
        """
        import os,sys
        from pathlib import Path
        args=sys.argv[1:]
        assert args[:2]==['aks','get-credentials']
        assert args[args.index('--name')+1] in {f'clustermesh-{n}' for n in (51,66,79)}
        Path(args[args.index('--file')+1]).write_text('private-retirement-credential')
        sys.exit(1 if os.environ['FAULT']=='credentials' else 0)
        """
    ))
    az.chmod(0o755)
    artifacts, calls = tmp_path / "artifacts", tmp_path / "calls.jsonl"
    environment = {
        **os.environ, "PATH": f"{binaries}:{os.environ['PATH']}", "PHASE": "plan",
        "QUALIFICATION_DIRECTORY": str(source), "REPOSITORY_DIRECTORY": str(checkout),
        "ARTIFACT_DIRECTORY": str(artifacts), "AGENT_TEMP_DIRECTORY": str(private),
        "TFVARS_PATH": TFVARS, "RUN_ID": SCOPE["target_run_id"],
        "CONFIRM_RESUME": SCOPE["confirm_resume"], "SUBSCRIPTION": SCOPE["expected_subscription_id"],
        "REGION": SCOPE["expected_region"], "FAULT": fault, "CALLS": str(calls),
    }
    script = yaml.safe_load(STEP.read_text())["steps"][0]["script"]
    result = subprocess.run(["bash", "-c", script], env=environment,
                            capture_output=True, text=True, timeout=20, check=False)
    assert not list(private.iterdir())
    if result.returncode == 0:
        hashes = dict(re.findall(r"variable=(SECONDARY_RETIREMENT_(?:INPUTS|TFVARS|PLAN)_SHA);isReadOnly=true]([0-9a-f]{64})",
                                 result.stdout))
        folder = artifacts / "n100-secondary-worker-retirement"
        if fault == "source-change":
            (folder / "qualification-input/raw.json").write_text("changed")
        elif fault == "tfvars-change":
            tfvars.write_text("changed")
        elif fault == "plan-change":
            (folder / "plan.json").write_text("changed")
        environment.update(PHASE="execute", INPUTS_SHA=hashes["SECONDARY_RETIREMENT_INPUTS_SHA"],
                           TFVARS_SHA=hashes["SECONDARY_RETIREMENT_TFVARS_SHA"],
                           PLAN_SHA=hashes["SECONDARY_RETIREMENT_PLAN_SHA"])
        result = subprocess.run(["bash", "-c", script], env=environment,
                                capture_output=True, text=True, timeout=20, check=False)
    assert (result.returncode == 0) is (fault == "none"), result.stderr
    assert len(calls.read_text().splitlines() if calls.exists() else []) == calls_expected
    assert not list(private.iterdir())
    assert not any(b"private-retirement-credential" in path.read_bytes()
                   for path in artifacts.rglob("*") if path.is_file())
