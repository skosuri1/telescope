"""Validate the isolated secondary-capacity qualification job and phase wrapper."""

from __future__ import annotations

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
JOB = ROOT / "jobs/clustermesh-secondary-qualification.yml"
STEP = ROOT / "steps/topology/clustermesh-scale/reuse/qualify-secondary-capacity.yml"
TFVARS = "scenarios/perf-eval/clustermesh-scale/terraform-inputs/azure-100-mock-shared.tfvars"
SCOPE = {
    "target_run_id": "78751-f36f3d5a",
    "confirm_resume": "78751-f36f3d5a",
    "expected_subscription_id": "37deca37-c375-4a14-b90a-043849bd2bf1",
    "expected_region": "eastus2euap",
    "expected_cluster_count": 100,
    "tfvars_path": TFVARS,
    "overlay_mode": "resume-existing",
    "run_workload": False,
    "source_build_id": 80039,
    "exclusive_modes": True,
}


def job():
    return yaml.safe_load(JOB.read_text(encoding="utf-8"))["jobs"][0]


def step():
    return yaml.safe_load(STEP.read_text(encoding="utf-8"))["steps"][0]


@pytest.mark.parametrize("changes", [
    {},
    {"source_build_id": 80022},
    {"source_build_id": 80029},
    {"source_build_id": "80039"},
    {"target_run_id": "other"},
    {"confirm_resume": "other"},
    {"expected_subscription_id": "other"},
    {"expected_region": "westus"},
    {"expected_cluster_count": 2},
    {"expected_cluster_count": "100"},
    {"tfvars_path": "other"},
    {"overlay_mode": "resume"},
    {"run_workload": True},
    {"exclusive_modes": False},
])
def test_typed_scope_accepts_only_successful_build_80039(changes):
    guard = job()["steps"][0]["script"]
    result = subprocess.run(
        ["bash", "-c", guard],
        env={
            **os.environ,
            "SECONDARY_QUALIFICATION_SCOPE_JSON": json.dumps({**SCOPE, **changes}),
        },
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    assert (result.returncode == 0) is (not changes)


def test_job_downloads_whole_build_and_splits_plan_execute_publication():
    definition = job()
    assert definition["job"] == "secondary_capacity_qualification"
    assert definition["timeoutInMinutes"] == 160
    assert definition["cancelTimeoutInMinutes"] == 30
    assert definition["variables"]["CLUSTERMESH_JOB_TIMEOUT_MINUTES"] == "160"
    steps = definition["steps"]
    download = next(row for row in steps if row.get("task") == "DownloadPipelineArtifact@2")
    assert download["inputs"] == {
        "buildType": "specific",
        "project": "$(System.TeamProjectId)",
        "definition": 23,
        "specificBuildWithTriggering": False,
        "buildVersionToDownload": "specific",
        "pipelineId": "${{ parameters.source_build_id }}",
        "artifactName": "n100-secondary-capacity-${{ parameters.source_build_id }}-1",
        "targetPath": "$(Pipeline.Workspace)/secondary-qualification-input",
    }
    phases = [
        (index, row["parameters"]["phase"])
        for index, row in enumerate(steps)
        if row.get("template")
        == "/steps/topology/clustermesh-scale/reuse/qualify-secondary-capacity.yml"
    ]
    assert [phase for _, phase in phases] == ["plan", "execute"]
    plan_publish = next(
        (index, row) for index, row in enumerate(steps)
        if row.get("displayName") == "Publish secondary qualification plan before probes"
    )
    assert phases[0][0] < plan_publish[0] < phases[1][0]
    assert plan_publish[1]["inputs"]["artifact"] == (
        "n100-secondary-qualification-plan-$(Build.BuildId)-$(System.JobAttempt)"
    )
    assert plan_publish[1]["inputs"]["targetPath"].endswith("/plan-publication")
    final = steps[-1]
    assert final["task"] == "PublishPipelineArtifact@1"
    assert "always()" in final["condition"]
    assert "SECONDARY_QUALIFICATION_DIAGNOSTICS_READY" in final["condition"]
    assert final["inputs"]["artifact"] == (
        "n100-secondary-qualification-$(Build.BuildId)-$(System.JobAttempt)"
    )


def test_step_contract_has_exact_budgets_cli_and_final_gate():
    definition = step()
    script = definition["script"]
    assert definition["retryCountOnTaskFailure"] == 0
    assert definition["${{ if eq(parameters.phase, 'plan') }}"]["timeoutInMinutes"] == 25
    assert definition["${{ else }}"]["timeoutInMinutes"] == 125
    for argument in (
        "--capacity-directory", "--source-build-id", "--resource-group",
        "--confirm-resource-group", "--expected-subscription", "--expected-region",
        "--expected-tfvars-sha", "--kubeconfig-directory", "--summary-file",
        "--timeout-seconds 7200",
    ):
        assert argument in script
    assert "secondary_capacity_qualification.py" in script
    assert "accepted-input/source-input" in script
    assert "plan-publication/plan-evidence" in script
    assert "require_evidence_directory qualification true" in script
    assert '"$bytes" -le 0' in script and "128 * 1024 * 1024" in script
    assert "SECONDARY_QUALIFICATION_PLAN_SHA" in script
    assert ".actual_ip_growth_proven == true" in script
    assert ".actual_headroom_proven == true" in script
    assert ".probe_cleanup_pending | type == \"array\" and length == 0" in script
    assert ".final_evidence.phase == \"probe-cleaned-final\"" in script
    assert ".retirement_authorized_by_execution == false" in script
    assert ".completed_global_baseline == false" in script
    assert ".workloads_ready == false" in script
    assert "patch node" not in script
    assert "delete-machines" not in script
    assert "nodepool add" not in script


def test_qualification_route_keeps_all_other_maintenance_modes_exclusive():
    pipeline = yaml.safe_load((ROOT / "pipelines/system/new-pipeline-test.yml").read_text(encoding="utf-8"))
    assert len({row["stage"] for row in pipeline["stages"]}) == len(pipeline["stages"]) == 43
    stage = next(row for row in pipeline["stages"]
                 if row["stage"] == "azure_eastus2euap_n100_debug_resume_37deca")
    outer = "${{ if ne(parameters.scaleDebugPostRetirementPromBuildId, 0) }}"
    branches = next(row[outer] for row in stage["jobs"] if outer in row)
    route = "${{ elseif eq(parameters.scaleDebugPostRetirementPromBuildId, 80039) }}"
    invocation = next(row[route][0] for row in branches if route in row)
    assert invocation["template"] == "/jobs/clustermesh-secondary-qualification.yml"
    assert invocation["parameters"]["source_build_id"] == "${{ parameters.scaleDebugPostRetirementPromBuildId }}"
    gates = invocation["parameters"]["exclusive_modes"]
    assert "eq(parameters.scaleDebugModernBaselineBuildId, 0)" in gates
    assert "eq(parameters.scaleDebugQualifiedWorkerRetirementBuildId, 0)" in gates
    assert "not(parameters.scaleDebugCniWorkerMaintenanceOnly)" in gates


def write_large_enough(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x" * 2048, encoding="utf-8")


def prepare_workspace(tmp_path):
    downloaded = tmp_path / "downloaded"
    checkout = tmp_path / "checkout"
    private = tmp_path / "private"
    artifacts = tmp_path / "artifacts"
    binaries = tmp_path / "bin"
    for directory in (downloaded, checkout, private, artifacts, binaries):
        directory.mkdir()
    for relative in (
        "recovery.json", "plan.json",
        "accepted-input/recovery.json", "accepted-input/plan.json",
    ):
        write_large_enough(downloaded / relative)
    (downloaded / "source-input").mkdir()
    (downloaded / "source-input" / "raw.json").write_text('{"source":80022}', encoding="utf-8")
    (downloaded / "accepted-input" / "source-input").mkdir()
    (downloaded / "accepted-input" / "source-input" / "raw.json").write_text(
        '{"source":80022}', encoding="utf-8",
    )
    tfvars = checkout / TFVARS
    tfvars.parent.mkdir(parents=True)
    tfvars.write_text("pinned-tfvars", encoding="utf-8")
    return downloaded, checkout, private, artifacts, binaries


def install_fakes(checkout, binaries):
    helper = (
        checkout
        / "modules/python/clusterloader2/clustermesh-scale/secondary_capacity_qualification.py"
    )
    helper.parent.mkdir(parents=True)
    helper.write_text(textwrap.dedent(
        """
        import hashlib
        import json
        import os
        import sys
        from pathlib import Path

        args = sys.argv[1:]
        def value(flag):
            return args[args.index(flag) + 1]

        with Path(os.environ["CALLS"]).open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(args) + "\\n")

        assert value("--source-build-id") == "80039"
        assert value("--resource-group") == value("--confirm-resource-group") == "78751-f36f3d5a"
        assert value("--expected-subscription") == "37deca37-c375-4a14-b90a-043849bd2bf1"
        assert value("--expected-region") == "eastus2euap"
        assert value("--timeout-seconds") == "7200"
        capacity = Path(value("--capacity-directory"))
        assert (capacity / "source-input/raw.json").read_text() == '{"source":80022}'
        assert (capacity / "accepted-input/source-input/raw.json").read_text() == '{"source":80022}'
        configs = Path(value("--kubeconfig-directory"))
        assert sorted(path.name for path in configs.iterdir()) == [
            "mesh-51.config", "mesh-66.config", "mesh-79.config", "mesh-89.config"
        ]
        assert all(path.read_text() == "private-secondary-credentials"
                   and path.stat().st_mode & 0o777 == 0o600 for path in configs.iterdir())

        execute = "--execute" in args
        fault = os.environ["FAULT"]
        output = Path(value("--summary-file"))
        evidence = output.parent / f"{output.stem}-evidence"
        evidence.mkdir()
        evidence_refs = {}
        for role in ("mesh-51", "mesh-66", "mesh-79", "mesh-89"):
            kinds = ["observation", "preflight-objects"]
            if execute:
                kinds.append("final-objects")
            evidence_refs[role] = {}
            for kind in kinds:
                path = evidence / f"{role}-{kind}.json"
                path.write_text(json.dumps({"role": role, "kind": kind, "execute": execute}))
                relative = f"{output.stem}-evidence/{path.name}"
                evidence_refs[role][kind] = {
                    "path": relative,
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                }
        if not execute:
            payload = {
                "schema_version": 1, "execute": False, "mutation_started": False,
                "capacity_source_build_id": 80039,
                "plan_valid": fault != "unsafe-plan", "success": fault != "unsafe-plan",
                "capacity_qualified": False, "workloads_ready": False,
                "completed_global_baseline": False,
                "evidence_files": evidence_refs,
            }
            output.write_text(json.dumps(payload))
            sys.exit(1 if fault == "plan-error" else 0)

        roles = {}
        for role in ("mesh-51", "mesh-66", "mesh-79", "mesh-89"):
            growth = {
                "new-node": {
                    "before": {"version": 0, "assigned_ip_count": 16},
                    "after": {"version": 1, "assigned_ip_count": 32},
                    "http_proven": fault != "false-growth",
                }
            }
            roles[role] = {
                "capacity_qualified": fault != "unqualified",
                "actual_ip_growth_proven": fault not in ("unqualified", "false-growth"),
                "actual_headroom_proven": fault != "unqualified",
                "workloads_ready": False, "completed_global_baseline": False,
                "probe_cleanup_pending": ["probe"] if fault == "cleanup-left" else [],
                "journal": {"accepted": True, "ambiguous": False},
                "placement_headroom": {"actual_metrics": True},
                "ip_growth": growth,
                "final_evidence": {
                    "phase": "probe-cleaned-final",
                    "production_pods_deleted": False,
                    "nodes_or_pools_mutated": False,
                    "target_host": {"controller_pods_force_deleted": False},
                    "future_native_fencing_hold": {"applied_in_qualification": False},
                },
            }
        success = fault not in ("unqualified", "cleanup-left", "false-growth")
        payload = {
            "schema_version": 1, "execute": True, "mutation_started": True,
            "capacity_source_build_id": 80039,
            "accepted_capacity_build_id": 80029, "source_diagnostic_build_id": 80022,
            "plan_valid": True, "success": success,
            "capacity_qualified": fault != "unqualified",
            "actual_ip_growth_proven": fault != "false-growth",
            "actual_headroom_proven": True,
            "workloads_ready": False, "completed_global_baseline": False,
            "bootstrap_complete": False,
            "cleanup_errors": ["cleanup"] if fault == "cleanup-error" else [],
            "per_role": roles,
            "native_fencing_bundle_sha256": "a" * 64,
            "native_fencing_bundle": {
                "schema_version": 1, "qualification_complete": True,
                "capacity_source_build_id": 80039,
                "retirement_authorized_by_execution": False,
                "workloads_ready": False, "completed_global_baseline": False,
                "roles": {role: {} for role in roles},
            },
            "evidence_files": evidence_refs,
        }
        output.write_text(json.dumps(payload))
        if fault == "missing-final-evidence":
            (evidence / "mesh-89-final-objects.json").unlink()
        sys.exit(1 if fault == "execute-error" else 0)
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
        name = args[args.index("--name") + 1]
        assert name in {"clustermesh-51", "clustermesh-66", "clustermesh-79", "clustermesh-89"}
        assert args[args.index("--subscription") + 1] == "37deca37-c375-4a14-b90a-043849bd2bf1"
        assert args[args.index("--resource-group") + 1] == "78751-f36f3d5a"
        destination = Path(args[args.index("--file") + 1])
        destination.write_text("private-secondary-credentials")
        if os.environ["FAULT"] == "credentials-" + os.environ["PHASE"] and name == "clustermesh-66":
            sys.exit(1)
        """
    ), encoding="utf-8")
    az.chmod(0o755)


@pytest.mark.parametrize("fault,expected_calls", [
    ("none", 2),
    ("initial-symlink", 0),
    ("existing-output", 0),
    ("credentials-plan", 0),
    ("plan-error", 1),
    ("unsafe-plan", 1),
    ("source-change", 1),
    ("accepted-change", 1),
    ("tfvars-change", 1),
    ("plan-byte-change", 1),
    ("plan-evidence-change", 1),
    ("credentials-execute", 1),
    ("execute-error", 2),
    ("unqualified", 2),
    ("cleanup-left", 2),
    ("cleanup-error", 2),
    ("false-growth", 2),
    ("missing-final-evidence", 2),
])
def test_split_phase_freezes_whole_input_and_cleans_private_credentials(
    tmp_path, fault, expected_calls,
):
    downloaded, checkout, private, artifacts, binaries = prepare_workspace(tmp_path)
    install_fakes(checkout, binaries)
    script = step()["script"]
    if fault == "initial-symlink":
        (downloaded / "source-input/link.json").symlink_to(downloaded / "recovery.json")
    elif fault == "existing-output":
        (artifacts / "n100-secondary-qualification").mkdir()
    calls = tmp_path / "calls.jsonl"
    environment = {
        **os.environ,
        "PATH": f"{binaries}:{os.environ['PATH']}",
        "PHASE": "plan",
        "RUN_ID": SCOPE["target_run_id"],
        "CONFIRM_RESUME": SCOPE["confirm_resume"],
        "SUBSCRIPTION": SCOPE["expected_subscription_id"],
        "REGION": SCOPE["expected_region"],
        "TFVARS_PATH": TFVARS,
        "SOURCE_BUILD_ID": "80039",
        "CAPACITY_DIRECTORY": str(downloaded),
        "ARTIFACT_DIRECTORY": str(artifacts),
        "REPOSITORY_DIRECTORY": str(checkout),
        "AGENT_TEMP_DIRECTORY": str(private),
        "INPUTS_SHA": "",
        "TFVARS_SHA": "",
        "PLAN_SHA": "",
        "FAULT": fault,
        "CALLS": str(calls),
    }
    result = subprocess.run(
        ["bash", "-c", script], env=environment,
        capture_output=True, text=True, check=False, timeout=30,
    )
    assert not list(private.iterdir())
    if result.returncode == 0:
        variables = dict(re.findall(
            r"variable=(SECONDARY_QUALIFICATION_(?:INPUTS|TFVARS|PLAN)_SHA)"
            r";isReadOnly=true]([0-9a-f]{64})",
            result.stdout,
        ))
        assert set(variables) == {
            "SECONDARY_QUALIFICATION_INPUTS_SHA",
            "SECONDARY_QUALIFICATION_TFVARS_SHA",
            "SECONDARY_QUALIFICATION_PLAN_SHA",
        }
        frozen = artifacts / "n100-secondary-qualification"
        assert (frozen / "plan-publication/plan.json").is_file()
        assert (frozen / "plan-publication/plan-evidence/mesh-51-observation.json").is_file()
        if fault == "source-change":
            (frozen / "capacity-input/source-input/raw.json").write_text("changed", encoding="utf-8")
        elif fault == "accepted-change":
            (frozen / "capacity-input/accepted-input/source-input/raw.json").write_text(
                "changed", encoding="utf-8",
            )
        elif fault == "tfvars-change":
            (checkout / TFVARS).write_text("changed", encoding="utf-8")
        elif fault == "plan-byte-change":
            (frozen / "plan.json").write_text("changed", encoding="utf-8")
        elif fault == "plan-evidence-change":
            (frozen / "plan-evidence/mesh-51-observation.json").write_text(
                "changed", encoding="utf-8",
            )
        environment.update(
            PHASE="execute",
            INPUTS_SHA=variables["SECONDARY_QUALIFICATION_INPUTS_SHA"],
            TFVARS_SHA=variables["SECONDARY_QUALIFICATION_TFVARS_SHA"],
            PLAN_SHA=variables["SECONDARY_QUALIFICATION_PLAN_SHA"],
        )
        result = subprocess.run(
            ["bash", "-c", script], env=environment,
            capture_output=True, text=True, check=False, timeout=30,
        )
    assert (result.returncode == 0) is (fault == "none"), result.stderr
    recorded = [
        json.loads(line)
        for line in calls.read_text(encoding="utf-8").splitlines()
    ] if calls.exists() else []
    assert len(recorded) == expected_calls
    if recorded:
        assert "--execute" not in recorded[0]
    if len(recorded) == 2:
        assert "--execute" in recorded[1]
    assert not list(private.iterdir())
    assert not any(
        b"private-secondary-credentials" in path.read_bytes()
        for path in artifacts.rglob("*")
        if path.is_file() and not path.is_symlink()
    )


def test_yaml_files_parse_without_central_pipeline_dependency():
    assert yaml.safe_load(JOB.read_text(encoding="utf-8"))["jobs"]
    assert yaml.safe_load(STEP.read_text(encoding="utf-8"))["steps"]
    combined = JOB.read_text(encoding="utf-8") + STEP.read_text(encoding="utf-8")
    assert "pipelines/system/new-pipeline-test.yml" not in combined
