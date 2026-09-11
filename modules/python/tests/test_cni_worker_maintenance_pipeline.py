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
    condition = "${{ if eq(parameters.scaleDebugCniWorkerMaintenanceOnly, true) }}"
    invocation = next(item[condition][0] for item in stage["jobs"] if condition in item)
    assert invocation["template"] == f"/{JOB}"
    for name, parameter in source_parameters.items():
        assert invocation["parameters"][name] == "${{ parameters." + parameter + " }}"
    assert invocation["parameters"]["overlay_mode"] == "${{ parameters.debugMode }}"
    assert invocation["parameters"]["run_workload"] == "${{ parameters.scaleDebugRunWorkload }}"
    assert stage["variables"]["CLUSTERMESH_CNI_WORKER_MAINTENANCE_ONLY"] == (
        "${{ parameters.scaleDebugCniWorkerMaintenanceOnly }}"
    )
    normal = template("jobs/clustermesh-debug-resume.yml")["jobs"][0]
    assert "ne(variables['CLUSTERMESH_CNI_WORKER_MAINTENANCE_ONLY'], 'true')" in normal["condition"]
    job = template(JOB)["jobs"][0]
    setup, operation = job["steps"][1:]
    assert setup["template"] == "/steps/setup-tests.yml"
    assert setup["parameters"]["credential_type"] == "service_connection"
    assert operation["template"] == f"/{STEP}"
    assert job["variables"]["SCENARIO_NAME"] == "clustermesh-scale"
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
def test_real_step_plans_then_executes_with_private_credentials(
    tmp_path, failure, expected, helper_calls,
):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    private = tmp_path / "private"
    private.mkdir()
    source = tmp_path / "source"
    source.mkdir()
    artifacts = tmp_path / "artifacts"
    trace = tmp_path / "helper-calls.jsonl"
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
