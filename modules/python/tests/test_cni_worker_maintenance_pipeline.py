"""Exercise the exact single-worker maintenance pipeline scripts without Azure."""

import json
import os
import subprocess
import textwrap
from pathlib import Path

import pytest
import yaml


REPOSITORY = Path(__file__).resolve().parents[3]
JOB = "jobs/clustermesh-cni-worker-maintenance.yml"
STEP = "steps/topology/clustermesh-scale/reuse/maintain-cni-worker.yml"
SOURCE_ENV = {
    "EXPECTED_CLUSTER_COUNT": "100",
    "OVERLAY_MODE": "resume-existing",
    "RUN_WORKLOAD": "False",
    "ARM_REPAIR_ONLY": "False",
    "RETIREMENT_ONLY": "False",
    "SOURCE_ROLE": "mesh-60",
    "SOURCE_NODE": "aks-default-test-vmss000000",
    "SOURCE_UID": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "SOURCE_PROVIDER_ID": (
        "azure:///subscriptions/test/resourceGroups/MC_preserved/"
        "providers/Microsoft.Compute/virtualMachineScaleSets/"
        "aks-default-test-vmss/virtualMachines/0"
    ),
    "SOURCE_NETWORK_CONTAINER_ID": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    "RESUME_BUILD_ID": "0",
    "RESUME_MANIFEST_JSON": "",
    "RECOVER_EMPTY_FRESH_NODE": "",
    "RECOVER_EMPTY_FRESH_UID": "",
    "REPLACE_EMPTY_FRESH": "False",
}
RESUME_MANIFEST = {
    "schema_version": 1,
    "source_build_id": 42,
    "resource_group": "preserved-run",
    "role": SOURCE_ENV["SOURCE_ROLE"],
    "source_worker": SOURCE_ENV["SOURCE_NODE"],
    "source_worker_uid": SOURCE_ENV["SOURCE_UID"],
}


def template(path):
    """Load the actual pipeline template used by the maintenance job."""

    return yaml.safe_load((REPOSITORY / path).read_text(encoding="utf-8"))


@pytest.mark.parametrize("overrides,expected", [
    ({}, 0),
    ({"EXPECTED_CLUSTER_COUNT": "2"}, 1),
    ({"OVERLAY_MODE": "resume"}, 1),
    ({"RUN_WORKLOAD": "True"}, 1),
    ({"ARM_REPAIR_ONLY": "True"}, 1),
    ({"RETIREMENT_ONLY": "True"}, 1),
    ({"SOURCE_ROLE": ""}, 1),
    ({"SOURCE_NODE": ""}, 1),
    ({"SOURCE_UID": ""}, 1),
    ({"SOURCE_PROVIDER_ID": ""}, 1),
    ({"SOURCE_NETWORK_CONTAINER_ID": ""}, 1),
    ({"RESUME_BUILD_ID": "-1"}, 1),
    ({"RESUME_BUILD_ID": "01"}, 1),
    ({"RESUME_BUILD_ID": "42"}, 1),
    ({"RESUME_MANIFEST_JSON": json.dumps(RESUME_MANIFEST)}, 1),
    ({"RESUME_BUILD_ID": "43", "RESUME_MANIFEST_JSON": json.dumps(RESUME_MANIFEST)}, 1),
    ({"RESUME_BUILD_ID": "42", "RESUME_MANIFEST_JSON": json.dumps(RESUME_MANIFEST)}, 0),
    ({"RECOVER_EMPTY_FRESH_NODE": "fresh-node"}, 1),
    ({"RECOVER_EMPTY_FRESH_UID": "fresh-uid"}, 1),
    ({"RECOVER_EMPTY_FRESH_NODE": "fresh-node", "RECOVER_EMPTY_FRESH_UID": "fresh-uid"}, 1),
    ({"REPLACE_EMPTY_FRESH": "True"}, 1),
])
def test_job_guard_rejects_incomplete_or_conflicting_modes(overrides, expected):
    script = template(JOB)["jobs"][0]["steps"][0]["script"]
    result = subprocess.run(
        ["bash", "-c", script],
        env={**os.environ, **SOURCE_ENV, **overrides},
        capture_output=True, text=True, check=False, timeout=10,
    )
    assert result.returncode == expected, result.stderr
    assert "command not found" not in result.stderr


@pytest.mark.parametrize("other_job", [
    "jobs/clustermesh-arm-repair.yml",
    "jobs/clustermesh-prepared-worker-retirement.yml",
])
def test_other_maintenance_jobs_refuse_cni_mode(other_job):
    script = template(other_job)["jobs"][0]["steps"][0]["script"]
    result = subprocess.run(
        ["bash", "-c", script],
        env={
            **os.environ, **SOURCE_ENV,
            "CNI_MAINTENANCE_ONLY": "True",
            "CLUSTERMESH_PRESERVED_AKS_ARM_RECONCILE_ENABLED": "true",
            "RETIREMENT_ROLE": SOURCE_ENV["SOURCE_ROLE"],
            "RETIREMENT_NODE": SOURCE_ENV["SOURCE_NODE"],
            "RETIREMENT_UID": SOURCE_ENV["SOURCE_UID"],
        },
        capture_output=True, text=True, check=False, timeout=10,
    )
    assert result.returncode == 1
    assert "unbound variable" not in result.stderr


def test_pipeline_binds_complete_plan_and_disables_normal_resume():
    pipeline = template("pipelines/system/new-pipeline-test.yml")
    parameters = {item["name"]: item for item in pipeline["parameters"]}
    assert parameters["scaleDebugCniWorkerMaintenanceOnly"]["default"] is False
    assert parameters["scaleDebugCniWorkerResumeBuildId"]["default"] == 0
    assert parameters["scaleDebugCniWorkerResumeManifestJson"]["default"] == ""
    source_parameters = {
        "source_role": "scaleDebugCniWorkerRole",
        "source_node": "scaleDebugCniWorkerNode",
        "source_uid": "scaleDebugCniWorkerUid",
        "source_provider_id": "scaleDebugCniWorkerProviderId",
        "source_network_container_id": "scaleDebugCniWorkerNetworkContainerId",
    }
    assert all(parameters[name]["default"] == "" for name in source_parameters.values())
    stage = next(
        item for item in pipeline["stages"]
        if item.get("stage") == "azure_eastus2euap_n100_debug_resume_37deca"
    )
    condition = (
        "${{ if and(parameters.scaleDebugCniWorkerMaintenanceOnly, "
        "not(parameters.scaleDebugPreparedRetirementObserveOnly), "
        "not(parameters.scaleDebugUnreachableWorkerRecoveryOnly)) }}"
    )
    invocation = next(item[condition][0] for item in stage["jobs"] if condition in item)
    assert invocation["template"] == f"/{JOB}"
    for name, parameter in source_parameters.items():
        assert invocation["parameters"][name] == "${{ parameters." + parameter + " }}"
    assert invocation["parameters"]["overlay_mode"] == "${{ parameters.debugMode }}"
    assert invocation["parameters"]["run_workload"] == "${{ parameters.scaleDebugRunWorkload }}"
    assert invocation["parameters"]["resume_build_id"] == "${{ parameters.scaleDebugCniWorkerResumeBuildId }}"
    assert invocation["parameters"]["resume_manifest_json"] == "${{ parameters.scaleDebugCniWorkerResumeManifestJson }}"
    assert invocation["parameters"]["recover_empty_fresh_node"] == "${{ parameters.scaleDebugCniWorkerRecoverEmptyFreshNode }}"
    assert invocation["parameters"]["recover_empty_fresh_uid"] == "${{ parameters.scaleDebugCniWorkerRecoverEmptyFreshUid }}"
    assert invocation["parameters"]["replace_empty_fresh"] == "${{ parameters.scaleDebugCniWorkerReplaceEmptyFresh }}"
    assert stage["variables"]["CLUSTERMESH_CNI_WORKER_MAINTENANCE_ONLY"] == (
        "${{ parameters.scaleDebugCniWorkerMaintenanceOnly }}"
    )
    normal = template("jobs/clustermesh-debug-resume.yml")["jobs"][0]
    assert "ne(variables['CLUSTERMESH_CNI_WORKER_MAINTENANCE_ONLY'], 'true')" in normal["condition"]
    job = template(JOB)["jobs"][0]
    setup, operation = [step for step in job["steps"] if "template" in step]
    assert setup["template"] == "/steps/setup-tests.yml"
    assert setup["parameters"]["credential_type"] == "service_connection"
    assert operation["template"] == f"/{STEP}"
    assert operation["parameters"]["resume_build_id"] == "${{ parameters.resume_build_id }}"
    assert operation["parameters"]["resume_manifest_json"] == "${{ parameters.resume_manifest_json }}"
    assert job["variables"]["SCENARIO_NAME"] == "clustermesh-scale"
    download_condition = "${{ if gt(parameters.resume_build_id, 0) }}"
    download = next(step[download_condition][0] for step in job["steps"] if download_condition in step)
    assert download["task"] == "DownloadPipelineArtifact@2"
    assert download["inputs"]["project"] == "$(System.TeamProjectId)"
    assert download["inputs"]["definition"] == "$(System.DefinitionId)"
    assert download["inputs"]["pipelineId"] == "${{ parameters.resume_build_id }}"
    assert download["inputs"]["artifactName"] == "n100-cni-worker-maintenance-${{ parameters.resume_build_id }}-1"
    publication = template(STEP)["steps"][1]
    assert "always()" in publication["condition"]
    assert publication["inputs"]["targetPath"].endswith("/n100-cni-worker-maintenance")


@pytest.mark.parametrize("failure,expected,helper_calls", [
    ("none", 0, 2),
    ("plan", 7, 1),
    ("execute", 8, 2),
    ("credentials", 9, 0),
    ("empty-inventory", 5, 0),
    ("ambiguous-inventory", 5, 0),
])
@pytest.mark.parametrize("continuation", ["none", "resume", "host", "replace"])
def test_real_step_plans_then_executes_with_private_credentials(
    tmp_path, failure, expected, helper_calls, continuation,
):
    resuming = continuation != "none"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    private = tmp_path / "private"
    private.mkdir()
    source = tmp_path / "source"
    source.mkdir()
    artifacts = tmp_path / "artifacts"
    trace = tmp_path / "helper-calls.jsonl"
    checkpoint = tmp_path / "checkpoint"
    checkpoint.mkdir()
    if resuming:
        (checkpoint / "maintenance.json").write_text('{"checkpoint":"original"}', encoding="utf-8")
    (source / "test.tfvars").write_text("cluster_count = 100\n", encoding="utf-8")
    fake_az = bin_dir / "az"
    fake_az.write_text(textwrap.dedent("""\
        #!/usr/bin/env python3
        import json
        import os
        import sys
        from pathlib import Path

        args = sys.argv[1:]
        assert args[args.index("--subscription") + 1] == "test-subscription"
        assert args[args.index("--resource-group") + 1] == "preserved-run"
        failure = os.environ["FAKE_FAILURE"]
        if args[:2] == ["aks", "list"]:
            cluster = {"name": "clustermesh-60", "tags": {"role": "mesh-60"}}
            rows = [] if failure == "empty-inventory" else [cluster]
            if failure == "ambiguous-inventory":
                rows.append(cluster)
            print(json.dumps(rows))
        elif args[:2] == ["aks", "get-credentials"]:
            assert args[args.index("--name") + 1] == "clustermesh-60"
            path = Path(args[args.index("--file") + 1])
            path.write_text("fake-private-credentials", encoding="utf-8")
            if failure == "credentials":
                sys.exit(9)
        else:
            raise AssertionError(f"Unexpected Azure operation: {args}")
        """), encoding="utf-8")
    fake_az.chmod(0o755)
    helper = source / "modules/python/clusterloader2/clustermesh-scale/cni_worker_maintenance.py"
    helper.parent.mkdir(parents=True)
    helper.write_text(textwrap.dedent("""\
        import json
        import os
        import stat
        import sys
        from pathlib import Path

        args = sys.argv[1:]
        for flag, environment in {
            "--role": "SOURCE_ROLE",
            "--node-name": "SOURCE_NODE",
            "--node-uid": "SOURCE_UID",
            "--source-provider-id": "SOURCE_PROVIDER_ID",
            "--source-network-container-id": "SOURCE_NETWORK_CONTAINER_ID",
            "--expected-subscription": "EXPECTED_SUBSCRIPTION_ID",
            "--expected-region": "EXPECTED_REGION",
            "--resource-group": "RUN_ID",
            "--confirm-resource-group": "CONFIRM_RESUME",
        }.items():
            assert args[args.index(flag) + 1] == os.environ[environment]
        assert args[args.index("--context") + 1] == "clustermesh-60"
        path = Path(args[args.index("--kubeconfig") + 1])
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert path.read_text(encoding="utf-8") == "fake-private-credentials"
        if os.environ["RESUME_BUILD_ID"] != "0":
            assert args[args.index("--resume-build-id") + 1] == os.environ["RESUME_BUILD_ID"]
            original = Path(args[args.index("--resume-summary") + 1])
            manifest = Path(args[args.index("--resume-manifest") + 1])
            assert json.loads(original.read_text(encoding="utf-8")) == {"checkpoint": "original"}
            assert json.loads(manifest.read_text(encoding="utf-8")) == json.loads(os.environ["RESUME_MANIFEST_JSON"])
        else:
            assert not any(flag in args for flag in ("--resume-build-id", "--resume-summary", "--resume-manifest"))
        if os.environ["RECOVER_EMPTY_FRESH_NODE"]:
            assert args[args.index("--recover-empty-fresh-node") + 1] == os.environ["RECOVER_EMPTY_FRESH_NODE"]
            assert args[args.index("--recover-empty-fresh-uid") + 1] == os.environ["RECOVER_EMPTY_FRESH_UID"]
        else:
            assert "--recover-empty-fresh-node" not in args
            assert "--recover-empty-fresh-uid" not in args
        if os.environ["REPLACE_EMPTY_FRESH"].lower() == "true":
            assert "--replace-empty-fresh" in args
            assert args[args.index("--timeout-seconds") + 1] == "3600"
        else:
            assert "--replace-empty-fresh" not in args
        with open(os.environ["FAKE_TRACE"], "a", encoding="utf-8") as handle:
            handle.write(json.dumps(args) + "\\n")
        summary = Path(args[args.index("--summary-file") + 1])
        summary.write_text("{}", encoding="utf-8")
        executing = "--execute" in args
        if os.environ["FAKE_FAILURE"] == "plan" and not executing:
            sys.exit(7)
        if os.environ["FAKE_FAILURE"] == "execute" and executing:
            sys.exit(8)
        """), encoding="utf-8")
    environment = {
        **os.environ, **SOURCE_ENV,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "RUN_ID": "preserved-run",
        "CONFIRM_RESUME": "preserved-run",
        "EXPECTED_SUBSCRIPTION_ID": "test-subscription",
        "EXPECTED_REGION": "eastus2euap",
        "SOURCE_ROOT": str(source),
        "TFVARS_PATH": "test.tfvars",
        "ARTIFACT_STAGING_DIRECTORY": str(artifacts),
        "PRIVATE_TEMP_ROOT": str(private),
        "FAKE_TRACE": str(trace),
        "FAKE_FAILURE": failure,
        "RESUME_BUILD_ID": "42" if resuming else "0",
        "RESUME_MANIFEST_JSON": json.dumps(RESUME_MANIFEST) if resuming else "",
        "RESUME_INPUT_DIRECTORY": str(checkpoint),
        "RECOVER_EMPTY_FRESH_NODE": "fresh-worker" if continuation in ("host", "replace") else "",
        "RECOVER_EMPTY_FRESH_UID": "fresh-uid" if continuation in ("host", "replace") else "",
        "REPLACE_EMPTY_FRESH": "True" if continuation == "replace" else "False",
    }
    result = subprocess.run(
        ["bash", "-c", template(STEP)["steps"][0]["script"]],
        env=environment, capture_output=True, text=True, check=False, timeout=20,
    )
    assert result.returncode == expected, result.stderr
    calls = [
        json.loads(line) for line in trace.read_text(encoding="utf-8").splitlines()
    ] if trace.exists() else []
    assert len(calls) == helper_calls
    if calls:
        assert "--execute" not in calls[0]
        assert calls[0][calls[0].index("--summary-file") + 1].endswith("/plan.json")
    if len(calls) == 2:
        assert "--execute" in calls[1]
        assert calls[1][calls[1].index("--summary-file") + 1].endswith("/maintenance.json")
        assert calls[0][:calls[0].index("--summary-file")] == calls[1][:calls[1].index("--summary-file")]
    assert not list(private.iterdir())
    assert all(
        "fake-private-credentials" not in path.read_text(encoding="utf-8")
        for path in artifacts.rglob("*") if path.is_file()
    )
    if resuming:
        assert json.loads((checkpoint / "maintenance.json").read_text(encoding="utf-8")) == {
            "checkpoint": "original",
        }


@pytest.mark.parametrize("build_id,manifest", [
    ("0", json.dumps(RESUME_MANIFEST)),
    ("42", ""),
    ("-1", json.dumps(RESUME_MANIFEST)),
    ("01", json.dumps(RESUME_MANIFEST)),
    ("42", "{invalid"),
    ("43", json.dumps(RESUME_MANIFEST)),
    ("42", json.dumps({**RESUME_MANIFEST, "source_worker_uid": "different"})),
    ("42", json.dumps(RESUME_MANIFEST)),
    ("42", "{" * 32769),
])
def test_step_rejects_partial_or_unbound_continuation_before_azure(tmp_path, build_id, manifest):
    fake_az = tmp_path / "az"
    fake_az.write_text(
        "#!/bin/sh\nprintf '%s\\n' 'Azure must not be reached' >&2\nexit 97\n",
        encoding="utf-8",
    )
    fake_az.chmod(0o755)
    result = subprocess.run(
        ["bash", "-c", template(STEP)["steps"][0]["script"]],
        env={
            **os.environ, **SOURCE_ENV,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "RUN_ID": "preserved-run",
            "CONFIRM_RESUME": "preserved-run",
            "RESUME_BUILD_ID": build_id,
            "RESUME_MANIFEST_JSON": manifest,
            "RESUME_INPUT_DIRECTORY": str(tmp_path / "missing-checkpoint"),
            "ARTIFACT_STAGING_DIRECTORY": str(tmp_path / "artifacts"),
        },
        capture_output=True, text=True, check=False, timeout=10,
    )
    assert result.returncode == 1, result.stderr
    assert "Azure must not be reached" not in result.stderr
    assert "unbound variable" not in result.stderr
