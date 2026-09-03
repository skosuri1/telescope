"""Static and guard checks for the reusable n=100 debug lifecycle."""

# pylint: disable=too-many-lines

import json
import os
import subprocess
from pathlib import Path

import yaml


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
PIPELINE_PATH = REPOSITORY_ROOT / "pipelines/system/new-pipeline-test.yml"
COMPETITIVE_JOB_PATH = REPOSITORY_ROOT / "jobs/competitive-test.yml"
RESUME_JOB_PATH = REPOSITORY_ROOT / "jobs/clustermesh-debug-resume.yml"
REUSE_DIR = (
    REPOSITORY_ROOT
    / "steps"
    / "topology"
    / "clustermesh-scale"
    / "reuse"
)
VALIDATE_SCRIPT = REUSE_DIR / "validate-existing-n100.sh"
RESET_SCRIPT = REUSE_DIR / "reset-fleet-overlay.sh"
CREATE_SCRIPT = REUSE_DIR / "create-staged-fleet-overlay.sh"
REPAIR_SCRIPT = REUSE_DIR / "repair-existing-fleet-overlay.sh"
DELETE_SCRIPT = REUSE_DIR / "delete-preserved-rg.sh"
MANIFEST_SCRIPT = REUSE_DIR / "write-resume-manifest.sh"
BASE_VALIDATE_PATH = (
    REPOSITORY_ROOT
    / "steps"
    / "topology"
    / "clustermesh-scale"
    / "validate-resources.yml"
)
CROSS_CLUSTER_SMOKE_PATH = (
    REPOSITORY_ROOT
    / "steps"
    / "topology"
    / "clustermesh-scale"
    / "cross-cluster-smoke.sh"
)
MOCK_VERIFY_SCRIPT = (
    REPOSITORY_ROOT
    / "modules"
    / "python"
    / "clusterloader2"
    / "clustermesh-scale"
    / "preserved_mock_verify.py"
)
MOCK_HANDOFF_SCRIPT = (
    REPOSITORY_ROOT
    / "modules"
    / "python"
    / "clusterloader2"
    / "clustermesh-scale"
    / "preserved_mock_handoff.py"
)
MOCK_HANDOFF_TEMPLATE_PATH = (
    REPOSITORY_ROOT
    / "steps"
    / "topology"
    / "clustermesh-scale-mock"
    / "verified-workload-handoff.yml"
)
MOCK_EXECUTE_TEMPLATE_PATH = (
    REPOSITORY_ROOT
    / "steps"
    / "topology"
    / "clustermesh-scale-mock"
    / "execute-clusterloader2.yml"
)
EXECUTE_TESTS_PATH = REPOSITORY_ROOT / "steps" / "execute-tests.yml"
LIVE_OVERLAY_SCRIPT = (
    REPOSITORY_ROOT
    / "modules"
    / "python"
    / "clusterloader2"
    / "clustermesh-scale"
    / "preserved_live_overlay.py"
)
WORKER_RECONCILE_SCRIPT = (
    REPOSITORY_ROOT
    / "modules"
    / "python"
    / "clusterloader2"
    / "clustermesh-scale"
    / "preserved_worker_reconcile.py"
)
AKS_ARM_RECONCILE_SCRIPT = (
    REPOSITORY_ROOT
    / "modules"
    / "python"
    / "clusterloader2"
    / "clustermesh-scale"
    / "preserved_aks_arm_reconcile.py"
)
SET_RUN_ID_PATH = REPOSITORY_ROOT / "steps" / "set-run-id.yml"
N100_TFVARS_PATH = (
    REPOSITORY_ROOT
    / "scenarios"
    / "perf-eval"
    / "clustermesh-scale"
    / "terraform-inputs"
    / "azure-100-mock-shared.tfvars"
)
AZURE_TERRAFORM_MAIN = (
    REPOSITORY_ROOT / "modules" / "terraform" / "azure" / "main.tf"
)


def _write_validation_fixture(tmp_path, *, include_second_node_group):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    az_log = tmp_path / "az.log"
    fixture_path = tmp_path / "fixture.json"
    parent_rg = "12345-deadbeef"
    cluster_ids = {
        role: (
            f"/subscriptions/test-subscription/resourceGroups/{parent_rg}/"
            "providers/Microsoft.ContainerService/managedClusters/"
            f"clustermesh-{index}"
        )
        for index, role in enumerate(("mesh-1", "mesh-2"), start=1)
    }
    clusters = [
        {
            "name": f"clustermesh-{index}",
            "rg": parent_rg,
            "role": role,
            "location": "eastus2euap",
        }
        for index, role in enumerate(("mesh-1", "mesh-2"), start=1)
    ]
    aks = [
        {
            "id": cluster_ids[role],
            "name": f"clustermesh-{index}",
            "location": "eastus2euap",
            "nodeResourceGroup": (
                f"MC_{parent_rg}_clustermesh-{index}_eastus2euap"
            ),
            "powerState": {"code": "Running"},
            "provisioningState": "Succeeded",
            "tags": {"role": role},
        }
        for index, role in enumerate(("mesh-1", "mesh-2"), start=1)
    ]
    groups = [
        {
            "name": f"MC_{parent_rg}_clustermesh-1_eastus2euap",
            "location": "eastus2euap",
            "managedBy": cluster_ids["mesh-1"],
            "deletion_due_time": "2000-01-01T00:00:00Z",
        }
    ]
    if include_second_node_group:
        groups.append(
            {
                "name": f"MC_{parent_rg}_clustermesh-2_eastus2euap",
                "location": "eastus2euap",
                "managedBy": cluster_ids["mesh-2"],
                "deletion_due_time": "2099-01-01T00:00:00Z",
            }
        )
    fixture_path.write_text(
        json.dumps(
            {
                "parent": {
                    "location": "eastus2euap",
                    "tags": {
                        "clustermesh_debug_preserved": "true",
                        "run_id": parent_rg,
                        "scenario": "perf-eval-clustermesh-scale",
                        "clustermesh_debug_expected_clusters": "2",
                        "clustermesh_debug_tfvars_sha256": "test-sha",
                        "deletion_due_time": "2000-01-01T00:00:00Z",
                    },
                },
                "clusters": clusters,
                "aks": aks,
                "groups": groups,
                "fleet_count": 0,
            }
        ),
        encoding="utf-8",
    )
    fake_az = fake_bin / "az"
    fake_az.write_text(
        "#!/usr/bin/env python3\n"
        "import json\n"
        "import os\n"
        "import sys\n"
        "args = sys.argv[1:]\n"
        "with open(os.environ['AZ_LOG'], 'a', encoding='utf-8') as log:\n"
        "    log.write(' '.join(args) + '\\n')\n"
        "with open(os.environ['AZ_FIXTURE'], encoding='utf-8') as handle:\n"
        "    fixture = json.load(handle)\n"
        "if args[:2] == ['account', 'show']:\n"
        "    print('test-subscription')\n"
        "elif args[:2] == ['group', 'show']:\n"
        "    print(json.dumps(fixture['parent']))\n"
        "elif args[:2] == ['resource', 'list']:\n"
        "    resource_type = args[args.index('--resource-type') + 1]\n"
        "    if resource_type == 'Microsoft.ContainerService/managedClusters':\n"
        "        print(json.dumps(fixture['clusters']))\n"
        "    elif resource_type == 'Microsoft.ContainerService/fleets':\n"
        "        print(fixture['fleet_count'])\n"
        "    else:\n"
        "        raise SystemExit(f'unexpected resource type: {resource_type}')\n"
        "elif args[:2] == ['aks', 'list']:\n"
        "    print(json.dumps(fixture['aks']))\n"
        "elif args[:2] == ['group', 'list']:\n"
        "    print(json.dumps(fixture['groups']))\n"
        "elif args[:2] == ['group', 'update']:\n"
        "    print('{}')\n"
        "else:\n"
        "    raise SystemExit(f'unexpected az command: {args}')\n",
        encoding="utf-8",
    )
    fake_az.chmod(0o755)
    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{fake_bin}:{env['PATH']}",
            "AZ_FIXTURE": str(fixture_path),
            "AZ_LOG": str(az_log),
            "BUILD_BUILDID": "999",
            "CLUSTERMESH_DEBUG_TARGET_RUN_ID": parent_rg,
            "CLUSTERMESH_DEBUG_EXPECTED_SUBSCRIPTION_ID": "test-subscription",
            "CLUSTERMESH_DEBUG_EXPECTED_REGION": "eastus2euap",
            "CLUSTERMESH_DEBUG_EXPECTED_CLUSTER_COUNT": "2",
            "CLUSTERMESH_DEBUG_EXPECTED_FLEET_COUNT": "1",
            "CLUSTERMESH_DEBUG_EXPECTED_TFVARS_SHA256": "test-sha",
            "CLUSTERMESH_DEBUG_EXTEND_LEASE_HOURS": "24",
            "CLUSTERMESH_DEBUG_REQUIRE_OVERLAY_RESET": "false",
            "CLUSTERMESH_DEBUG_MANIFEST_PATH": str(tmp_path / "manifest.json"),
        }
    )
    return env, az_log, tmp_path / "manifest.json"


def _write_resume_manifest_fixture(
    tmp_path,
    *,
    fail_aks_inventory=False,
    extra_aks=False,
    no_fleet=False,
):
    fake_bin = tmp_path / "manifest-bin"
    fake_bin.mkdir()
    output_path = tmp_path / "resume-manifest.json"
    fake_az = fake_bin / "az"
    fake_az.write_text(
        "#!/usr/bin/env python3\n"
        "import json\n"
        "import sys\n"
        "args = sys.argv[1:]\n"
        "if args[:2] == ['account', 'show']:\n"
        "    print(json.dumps({'id': 'test-subscription'}))\n"
        "elif args[:2] == ['group', 'exists']:\n"
        "    print('true')\n"
        "elif args[:2] == ['group', 'show']:\n"
        "    print(json.dumps({'tags': {'deletion_due_time': '2099-01-01T00:00:00Z'}}))\n"
        "elif args[:2] == ['aks', 'list']:\n"
        "    if 'FAIL_AKS' in __import__('os').environ:\n"
        "        print('transient AKS inventory failure', file=sys.stderr)\n"
        "        raise SystemExit(1)\n"
        "    clusters = [\n"
        "        {'id': '/subscriptions/test/resourceGroups/run/providers/Microsoft.ContainerService/managedClusters/clustermesh-1', 'name': 'clustermesh-1', 'runId': 'run', 'role': 'mesh-1', 'location': 'eastus2euap', 'provisioningState': 'Succeeded', 'powerState': 'Running', 'nodeResourceGroup': 'MC_run_1'},\n"
        "        {'id': '/subscriptions/test/resourceGroups/run/providers/Microsoft.ContainerService/managedClusters/clustermesh-2', 'name': 'clustermesh-2', 'runId': 'run', 'role': 'mesh-2', 'location': 'eastus2euap', 'provisioningState': 'Succeeded', 'powerState': 'Running', 'nodeResourceGroup': 'MC_run_2'},\n"
        "    ]\n"
        "    if 'EXTRA_AKS' in __import__('os').environ:\n"
        "        clusters.append({'id': '/subscriptions/test/resourceGroups/run/providers/Microsoft.ContainerService/managedClusters/untracked', 'name': 'untracked', 'runId': None, 'role': None, 'location': 'eastus2euap', 'provisioningState': 'Succeeded', 'powerState': 'Running', 'nodeResourceGroup': 'MC_run_extra'})\n"
        "    print(json.dumps(clusters))\n"
        "elif args[:2] == ['resource', 'list']:\n"
        "    if 'NO_FLEET' in __import__('os').environ:\n"
        "        print('[]')\n"
        "    else:\n"
        "        print(json.dumps([{'name': 'clustermesh-flt', 'provisioningState': 'Succeeded'}]))\n"
        "else:\n"
        "    raise SystemExit(f'unexpected az command: {args}')\n",
        encoding="utf-8",
    )
    fake_az.chmod(0o755)
    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{fake_bin}:{env['PATH']}",
            "RUN_ID": "run",
            "BUILD_BUILDID": "999",
            "BUILD_SOURCEVERSION": "test-source",
            "CLUSTERMESH_DEBUG_EXPECTED_SUBSCRIPTION_ID": "test-subscription",
            "CLUSTERMESH_DEBUG_EXPECTED_REGION": "eastus2euap",
            "CLUSTERMESH_DEBUG_EXPECTED_CLUSTER_COUNT": "2",
            "CLUSTERMESH_DEBUG_EXPECTED_FLEET_COUNT": "1",
            "CLUSTERMESH_DEBUG_MANIFEST_INVENTORY_ATTEMPTS": "2",
            "CLUSTERMESH_DEBUG_MANIFEST_RETRY_SECONDS": "0",
            "CLUSTERMESH_DEBUG_MANIFEST_INVENTORY_TIMEOUT_SECONDS": "10",
            "CLUSTERMESH_DEBUG_MANIFEST_PATH": str(output_path),
            "CLUSTERMESH_DEBUG_TFVARS_PATH": str(tmp_path / "test.tfvars"),
        }
    )
    (tmp_path / "test.tfvars").write_text("test\n", encoding="utf-8")
    if fail_aks_inventory:
        env["FAIL_AKS"] = "1"
    if extra_aks:
        env["EXTRA_AKS"] = "1"
    if no_fleet:
        env["NO_FLEET"] = "1"
        env["CLUSTERMESH_DEBUG_EXPECTED_FLEET_COUNT"] = "0"
    return env, output_path


def _stage_block(name: str, next_name: str) -> str:
    pipeline = PIPELINE_PATH.read_text(encoding="utf-8")
    start = pipeline.index(f"- stage: {name}")
    end = pipeline.index(f"- stage: {next_name}", start)
    return pipeline[start:end]


def test_debug_stages_are_explicitly_mode_gated():
    active = _stage_block(
        "azure_eastus2_n100_mock_aksstandalone2",
        "azure_eastus2euap_n100_debug_preserve_37deca",
    )
    fresh = _stage_block(
        "azure_eastus2euap_n100_debug_preserve_37deca",
        "azure_eastus2euap_n100_debug_reset_fleet_37deca",
    )
    reset = _stage_block(
        "azure_eastus2euap_n100_debug_reset_fleet_37deca",
        "azure_eastus2euap_n100_debug_resume_37deca",
    )
    resume = _stage_block(
        "azure_eastus2euap_n100_debug_resume_37deca",
        "azure_eastus2euap_n100_debug_cleanup_37deca",
    )
    cleanup = _stage_block(
        "azure_eastus2euap_n100_debug_cleanup_37deca",
        "azure_centraluseuap_n100_mock",
    )

    assert "eq(variables['CLUSTERMESH_DEBUG_MODE'], '')" in active
    pipeline = PIPELINE_PATH.read_text(encoding="utf-8")
    assert "CLUSTERMESH_DEBUG_MODE: ${{ parameters.debugMode }}" in pipeline
    assert "- name: debugMode" in pipeline
    assert "- name: debugTargetRunId" in pipeline
    assert "- name: debugConfirmReset" in pipeline
    assert "- name: debugConfirmResume" in pipeline
    assert "- name: debugConfirmDelete" in pipeline
    assert "- name: scaleDebugClusterCount" in pipeline
    assert "- name: scaleDebugRegion" in pipeline
    assert "- name: scaleDebugVmFamilyQuotaName" in pipeline
    assert "- name: scaleDebugTfvarsPath" in pipeline
    assert "- name: scaleDebugTopology" in pipeline
    assert "- name: scaleDebugRequiredFamilyVcpus" in pipeline
    assert "- name: scaleDebugRunWorkload" in pipeline
    assert "- name: scaleDebugKwokPreservationMode" in pipeline
    assert "- name: scaleDebugKwokBaselineBuildId" in pipeline
    assert "default: 78812" in pipeline
    assert "- name: scaleDebugKwokVerificationBuildId" in pipeline
    assert "default: 78851" in pipeline
    assert "- verify" in pipeline

    assert "CLUSTERMESH_DEBUG_MODE'], 'fresh-preserve'" in fresh
    assert 'SKIP_RESOURCE_DELETION: "true"' in fresh
    assert 'CLUSTERMESH_DEBUG_PRESERVE: "true"' in fresh
    assert "NETWORK_DATAPLANE: cilium" in fresh
    assert "NETWORK_POLICY: cilium" in fresh
    assert "emit_resume_manifest: true" in fresh
    assert "resume_manifest_expected_subscription_id" in fresh
    assert "resume_manifest_expected_cluster_count" in fresh
    assert "resume_manifest_expected_fleet_count" in fresh
    assert 'debug_preserve: "true"' in fresh
    assert "eq(variables['Build.Reason'], 'Manual')" in fresh
    assert "eq(variables['CLUSTERMESH_REUSE_SMOKE_MODE'], '')" in fresh
    assert "parameters.scaleDebugRegion" in fresh
    assert "parameters.lifecycleSubscriptionId" in fresh
    assert "parameters.scaleDebugVmFamilyQuotaName" in fresh
    assert "parameters.scaleDebugTfvarsPath" in fresh
    assert "parameters.scaleDebugClusterCount" in fresh
    assert "parameters.scaleDebugRequiredFamilyVcpus" in fresh
    assert "parameters.scaleDebugTopology" in fresh
    assert "parameters.scaleDebugRunWorkload" in fresh
    assert "suite_total_budget_seconds: 7200" in fresh
    assert "timeout_in_minutes: 600" in fresh
    assert 'cl2_prom_snapshot_storage_account: "cmshscaleprom"' in fresh
    assert 'AKS_AMW_CLUSTERS_PER_WORKSPACE: "1"' in fresh
    assert 'AKS_AMW_FORCE_SHARD_NAMING: "true"' in fresh
    assert 'AKS_AMW_PREFLIGHT_MAX_UTILIZATION_PERCENT: "90"' in fresh
    assert 'AKS_AMW_REGIONAL_WORKSPACE_LIMIT: "100"' in fresh
    assert 'AKS_AMW_MAX_ACTIVE_TIME_SERIES: "1000000"' in fresh
    assert 'AKS_AMW_MAX_EVENTS_PER_MINUTE: "1000000"' in fresh

    assert "CLUSTERMESH_DEBUG_MODE'], 'reset-fleet'" in reset
    assert "CLUSTERMESH_DEBUG_CONFIRM_RESET" in reset
    assert "reset-fleet-overlay.sh" in reset
    assert "eq(variables['Build.Reason'], 'Manual')" in reset
    assert "eq(variables['CLUSTERMESH_REUSE_SMOKE_MODE'], '')" in reset
    assert "region: ${{ parameters.scaleDebugRegion }}" in reset
    assert "parameters.lifecycleSubscriptionId" in reset
    assert "parameters.scaleDebugTfvarsPath" in reset
    assert "parameters.scaleDebugClusterCount" in reset
    assert 'CLUSTERMESH_DEBUG_EXTEND_LEASE_HOURS: "168"' in reset

    assert "CLUSTERMESH_DEBUG_MODE'], 'resume'" in resume
    assert "CLUSTERMESH_DEBUG_MODE'], 'resume-existing'" in resume
    assert "and(succeededOrFailed()," in resume
    assert "and(always()," not in resume
    assert "clustermesh-debug-resume.yml" in resume
    assert "CLUSTERMESH_QUOTA_PREFLIGHT_ENABLED: \"false\"" in resume
    assert "eq(variables['Build.Reason'], 'Manual')" in resume
    assert "eq(variables['CLUSTERMESH_REUSE_SMOKE_MODE'], '')" in resume
    assert "region: ${{ parameters.scaleDebugRegion }}" in resume
    assert "parameters.lifecycleSubscriptionId" in resume
    assert "parameters.scaleDebugTfvarsPath" in resume
    assert "parameters.scaleDebugClusterCount" in resume
    assert "parameters.scaleDebugTopology" in resume
    assert "parameters.scaleDebugRunWorkload" in resume
    assert "parameters.scaleDebugKwokPreservationMode" in resume
    assert "parameters.scaleDebugKwokBaselineBuildId" in resume
    assert "parameters.scaleDebugKwokVerificationBuildId" in resume
    assert 'cl2_prom_snapshot_storage_account: "cmshscaleprom"' in resume
    assert 'AKS_AMW_CLUSTERS_PER_WORKSPACE: "1"' in resume
    assert 'AKS_AMW_FORCE_SHARD_NAMING: "true"' in resume
    assert 'AKS_AMW_PREFLIGHT_MAX_UTILIZATION_PERCENT: "90"' in resume
    assert 'AKS_AMW_REGIONAL_WORKSPACE_LIMIT: "100"' in resume
    assert 'AKS_MANAGED_PROMETHEUS_REBALANCE_EXISTING: "true"' in resume
    assert 'AKS_AMW_REBALANCE_SETTLE_SECONDS: "600"' in resume
    assert 'AKS_AMW_MAX_ACTIVE_TIME_SERIES: "1000000"' in resume
    assert 'AKS_AMW_MAX_EVENTS_PER_MINUTE: "1000000"' in resume
    assert 'CLUSTERMESH_PRESERVED_WORKER_RECOVERY_ENABLED: "true"' in resume
    assert 'CLUSTERMESH_DEBUG_MAX_WORKER_REPAIR_CLUSTERS: "5"' in resume
    assert 'CLUSTERMESH_LIVE_DATA_PLANE_REPAIR_ENABLED: "true"' in resume
    assert 'CLUSTERMESH_NODE_READINESS_SELECTOR: "type!=kwok"' in resume
    assert 'CLUSTERMESH_PRESERVED_AKS_ARM_RECONCILE_ENABLED: "true"' in resume
    assert 'CLUSTERMESH_DEBUG_MAX_AKS_ARM_REPAIR_CLUSTERS: "10"' in resume
    assert "parameters.debugMode" in resume
    assert "overlay_mode: resume-existing" in resume
    assert "overlay_mode: resume" in resume

    assert "CLUSTERMESH_DEBUG_MODE'], 'cleanup'" in cleanup
    assert "CLUSTERMESH_DEBUG_CONFIRM_DELETE" in cleanup
    assert "CLUSTERMESH_DEBUG_EXPECTED_SUBSCRIPTION_ID" in cleanup
    assert "delete-preserved-rg.sh" in cleanup
    assert "eq(variables['Build.Reason'], 'Manual')" in cleanup
    assert "eq(variables['CLUSTERMESH_REUSE_SMOKE_MODE'], '')" in cleanup
    assert (
        "CLUSTERMESH_DEBUG_EXPECTED_REGION: "
        "${{ parameters.scaleDebugRegion }}"
        in cleanup
    )
    assert "parameters.lifecycleSubscriptionId" in cleanup
    assert "parameters.scaleDebugTfvarsPath" in cleanup
    assert "parameters.scaleDebugClusterCount" in cleanup

    invalid_start = pipeline.index("- stage: n100_debug_mode_invalid")
    invalid_end = pipeline.index(
        "- stage: azure_centraluseuap_n100_mock", invalid_start
    )
    invalid = pipeline[invalid_start:invalid_end]
    assert "Unsupported CLUSTERMESH_DEBUG_MODE" in invalid


def test_resume_job_skips_terraform_and_preserves_resources():
    resume = RESUME_JOB_PATH.read_text(encoding="utf-8")
    set_run_id = SET_RUN_ID_PATH.read_text(encoding="utf-8")

    assert "/steps/provision-resources.yml" not in resume
    assert "/steps/cleanup-resources.yml" not in resume
    assert "validate-existing-scale.sh" in resume
    assert "preserved_aks_arm_reconcile.py" in resume
    assert (
        resume.index('displayName: "Reconcile stale preserved AKS ARM states"')
        < resume.index(
            'displayName: "Validate preserved scale clusters and prepare Fleet overlay"'
        )
    )
    assert "create-staged-fleet-overlay.sh" in resume
    assert "repair-existing-fleet-overlay.sh" in resume
    assert "/steps/validate-resources.yml" in resume
    assert "/steps/execute-tests.yml" in resume
    assert "/steps/publish-results.yml" in resume
    assert "CLUSTERMESH_DEBUG_CONFIRM_RESUME" in resume
    assert "- name: overlay_mode" in resume
    assert "- name: target_run_id" in resume
    assert "run_id: ${{ parameters.target_run_id }}" in resume
    assert "RUN_ID: ${{ parameters.target_run_id }}" in resume
    assert "run_id: $(CLUSTERMESH_DEBUG_TARGET_RUN_ID)" not in resume
    assert "CLUSTERMESH_DEBUG_MAX_REPAIR_MEMBERS" in resume
    assert "requested_run_id='${{ parameters.run_id }}'" in set_run_id
    assert 'elif [ -n "${RUN_ID:-}" ]' in set_run_id
    assert "RUN_ID: ${{ parameters.run_id }}" not in set_run_id
    assert "- name: expected_cluster_count" in resume
    assert "- name: run_workload" in resume
    assert "- name: publish_results" in resume
    assert (
        "${{ if and(parameters.run_workload, "
        "eq(parameters.mock_preservation_mode, 'none')) }}:"
        in resume
    )
    assert "mock_preservation_mode" in resume
    assert "mock_preservation_baseline_build_id" in resume
    assert "mock_preservation_verification_build_id" in resume
    assert "preserved_mock_capture.py" in resume
    assert "Capture preserved n100 KWOK baseline" in resume
    assert "Publish preserved n100 KWOK baseline" in resume
    assert "DownloadPipelineArtifact@2" in resume
    assert "Download preserved n100 KWOK baseline" in resume
    assert "Download preserved n100 KWOK verification proof" in resume
    assert "buildVersionToDownload: specific" in resume
    assert resume.count("/steps/validate-resources.yml") == 1
    assert resume.index("Download preserved n100 KWOK baseline") < resume.index(
        "Reconcile stale preserved AKS ARM states"
    )
    assert (
        "artifactName: n100-kwok-preservation-"
        "${{ parameters.mock_preservation_baseline_build_id }}"
        in resume
    )
    assert "preserved_mock_verify.py" in resume
    assert "Verify preserved n100 KWOK identities and exact recovery" in resume
    assert "Revalidate cross-cluster data path after KWOK recovery" in resume
    assert "Publish preserved n100 KWOK verification" in resume
    assert "cross_cluster_data_path_failed" in resume
    assert 'payload["fatal_error"]' in resume
    assert 'CLUSTERMESH_CROSS_CLUSTER_SMOKE_ENABLED: "true"' in resume
    assert "Finalize incomplete preserved n100 KWOK verification evidence" in resume
    assert "pipeline_failed_before_verification_completion" in resume
    assert "mock_handoff_enabled:" in resume
    assert "mock_handoff_run_id:" in resume
    assert "mock_handoff_verification_build_id:" in resume
    assert resume.count("--fault-role") == 5
    assert "--fault-role mesh-1" in resume
    assert "--fault-role mesh-100" in resume
    assert "cross-cluster-smoke.sh" in resume
    assert 'CLUSTERMESH_DEBUG_EXTEND_LEASE_HOURS: "168"' in resume
    assert (
        "${{ if and(parameters.run_workload, parameters.publish_results, "
        "eq(parameters.mock_preservation_mode, 'none')) }}:"
        in resume
    )


def test_fresh_preserve_n100_lease_is_seven_days():
    tfvars = N100_TFVARS_PATH.read_text(encoding="utf-8")

    assert 'deletion_delay = "168h"' in tfvars


def test_global_cilium_policy_is_injected_into_aks_cli_configs():
    terraform = AZURE_TERRAFORM_MAIN.read_text(encoding="utf-8")

    assert "local.aks_network_dataplane != null" in terraform
    assert '"network-dataplane"' in terraform
    assert "local.aks_network_policy != null" in terraform
    assert '"network-policy"' in terraform
    assert terraform.count(
        "[for parameter in aks.optional_parameters : parameter.name]"
    ) >= 2


def test_fleet_reset_and_resume_do_not_mutate_aks_lifecycle():
    reset = RESET_SCRIPT.read_text(encoding="utf-8")
    create = CREATE_SCRIPT.read_text(encoding="utf-8")
    repair = REPAIR_SCRIPT.read_text(encoding="utf-8")

    for forbidden in ("az aks delete", "az aks create", "az group delete"):
        assert forbidden not in reset
    assert "az fleet clustermeshprofile delete" in reset
    assert "az fleet member delete" in reset
    assert "az fleet delete" in reset
    assert "does not exactly match" in reset
    assert "CLUSTERMESH_DEBUG_EXPECTED_CLUSTER_COUNT" in reset
    assert "--argjson expected_count" in reset
    assert '--slurpfile members "$member_file"' in reset
    assert '--slurpfile clusters "$cluster_file"' in reset
    assert "--argjson members" not in reset
    assert "--argjson clusters" not in reset
    assert "clustermesh-reset-inventory" in reset
    assert "issuing bounded apply nudge" in reset
    assert "deferring apply nudge" in reset
    assert "Removing residual ClusterMesh Kubernetes resources" in reset
    assert "cilium-ca" in reset
    assert "cilium-root-ca.crt" in reset
    assert "cilium-kvstoremesh" in reset
    assert "cilium-clustermesh" in reset
    assert "clustermesh-apiserver-server-cert" in reset
    assert "Cluster-side overlay reset complete" in reset

    assert "az aks create" not in create
    assert "az group create" not in create
    assert "az fleet create" in create
    assert "--labels \"${label_key}=${initial_value}\"" in create
    assert "Expected empty staged profile" in create

    for forbidden in ("az aks delete", "az aks create", "az group delete", "az fleet delete"):
        assert forbidden not in repair
    assert "CLUSTERMESH_DEBUG_MAX_REPAIR_MEMBERS" in repair
    assert "Surgically rejoining" in repair
    assert "repair profile apply" in repair.lower()
    assert "ResourceNotFinalState" in repair
    assert "Fleet surgical repair completed" in repair
    assert "CLUSTERMESH_DEBUG_FORCE_REPAIR_ROLES_FILE" in repair
    assert "live-data-plane unhealthy member(s)" in repair
    assert '--slurpfile members "$member_file"' in repair
    assert '--slurpfile applied "$profile_member_file"' in repair
    assert '--slurpfile clusters "$cluster_file"' in repair


def test_preserved_rg_validation_is_fail_closed():
    validation = VALIDATE_SCRIPT.read_text(encoding="utf-8")
    manifest = MANIFEST_SCRIPT.read_text(encoding="utf-8")

    for expected in (
        "clustermesh_debug_preserved",
        "perf-eval-clustermesh-scale",
        "mesh-1..mesh-$expected_count",
        "provisioningState",
        "powerState",
        "CLUSTERMESH_DEBUG_REQUIRE_OVERLAY_RESET",
        "clustermesh_debug_tfvars_sha256",
        "clustermesh_debug_expected_clusters",
        "exactly $expected_count total AKS clusters",
        "outside $expected_region",
        "Preserving later existing lease",
        "requested_deletion_due_time",
        "existing_deletion_due_time",
        "nodeResourceGroup",
        "managedBy",
        "node_resource_group_count",
        "cannot be repaired in place",
    ):
        assert expected in validation

    assert "az group update" not in manifest
    assert "cluster_inventory_failed" in manifest
    assert "inventory_invalid" in manifest
    assert "AKS inventory is not exactly mesh-1..mesh-${expected_count}" in manifest
    assert "CLUSTERMESH_DEBUG_EXPECTED_FLEET_COUNT" in manifest


def test_resume_manifest_requires_exact_live_inventory(tmp_path):
    env, output_path = _write_resume_manifest_fixture(tmp_path)

    result = subprocess.run(
        ["bash", str(MANIFEST_SCRIPT)],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    manifest = json.loads(output_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "ready"
    assert manifest["subscription_id"] == "test-subscription"
    assert manifest["cluster_count"] == 2
    assert [row["role"] for row in manifest["clusters"]] == [
        "mesh-1",
        "mesh-2",
    ]
    assert len(manifest["fleet"]) == 1


def test_resume_manifest_does_not_silently_publish_empty_inventory(tmp_path):
    env, output_path = _write_resume_manifest_fixture(
        tmp_path,
        fail_aks_inventory=True,
    )

    result = subprocess.run(
        ["bash", str(MANIFEST_SCRIPT)],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    manifest = json.loads(output_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "cluster_inventory_failed"
    assert "AKS inventory failed after 2 attempt" in manifest["fatal_error"]
    assert "clusters" not in manifest


def test_resume_manifest_rejects_extra_untagged_aks_cluster(tmp_path):
    env, output_path = _write_resume_manifest_fixture(
        tmp_path,
        extra_aks=True,
    )

    result = subprocess.run(
        ["bash", str(MANIFEST_SCRIPT)],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    manifest = json.loads(output_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "inventory_invalid"
    assert manifest["cluster_count"] == 3
    assert "not exactly mesh-1..mesh-2" in manifest["fatal_error"]


def test_resume_manifest_allows_explicit_post_reset_fleet_absence(tmp_path):
    env, output_path = _write_resume_manifest_fixture(
        tmp_path,
        no_fleet=True,
    )

    result = subprocess.run(
        ["bash", str(MANIFEST_SCRIPT)],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    manifest = json.loads(output_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "ready"
    assert manifest["fleet"] == []


def test_preserved_validation_rejects_missing_node_resource_group(tmp_path):
    env, az_log, _ = _write_validation_fixture(
        tmp_path,
        include_second_node_group=False,
    )

    result = subprocess.run(
        ["bash", str(VALIDATE_SCRIPT)],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "node resource groups are missing" in result.stderr
    assert "MC_12345-deadbeef_clustermesh-2_eastus2euap" in result.stderr
    assert "cannot be repaired in place" in result.stderr
    assert "group update" not in az_log.read_text(encoding="utf-8")


def test_preserved_validation_extends_child_leases_before_parent(tmp_path):
    env, az_log, manifest_path = _write_validation_fixture(
        tmp_path,
        include_second_node_group=True,
    )

    result = subprocess.run(
        ["bash", str(VALIDATE_SCRIPT)],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    updates = [
        line
        for line in az_log.read_text(encoding="utf-8").splitlines()
        if line.startswith("group update ")
    ]
    assert len(updates) == 2
    assert "--name MC_12345-deadbeef_clustermesh-1_eastus2euap" in updates[0]
    assert "--name 12345-deadbeef" in updates[1]
    assert not any("clustermesh-2" in update for update in updates)
    assert "Preserving later existing node RG lease" in result.stdout

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["node_resource_group_count"] == 2
    assert manifest["node_resource_groups_extended"] == 1
    assert len(manifest["node_resource_groups"]) == 2


def test_preserved_resume_recovery_runs_before_authoritative_validation():
    validation = BASE_VALIDATE_PATH.read_text(encoding="utf-8")
    cross_cluster_smoke = CROSS_CLUSTER_SMOKE_PATH.read_text(encoding="utf-8")

    enumerate_pos = validation.index('displayName: "Enumerate clustermesh clusters"')
    worker_pos = validation.index(
        'displayName: "Recover preserved real AKS workers"'
    )
    apiserver_pos = validation.index(
        'displayName: "Wait for clustermesh-apiserver Deployments + LBs (parallel)"'
    )
    live_pos = validation.index(
        'displayName: "Repair stale live ClusterMesh peers"'
    )
    validate_pos = validation.index(
        'displayName: "Validate Cilium + ClusterMesh on every cluster"'
    )

    assert enumerate_pos < worker_pos < apiserver_pos < live_pos < validate_pos
    assert "preserved_worker_reconcile.py" in validation
    assert "preserved_live_overlay.py" in validation
    assert 'node_selector_args=(-l "$node_readiness_selector")' in validation
    assert (
        'get nodes "${node_selector_args[@]}" -o json'
        in validation
    )
    assert "ensure_kubeconfig" in validation
    assert "cross-cluster-smoke.sh" in validation
    assert 'then .' in validation
    assert 'mv "$cilium_identity_inventory_tmp" "$cilium_identity_inventory"' in validation
    assert "service.cilium.io/global" in cross_cluster_smoke
    assert "Cross-cluster curl succeeded" in cross_cluster_smoke
    assert "wait_namespace_absent" in cross_cluster_smoke
    assert "trap - EXIT" in cross_cluster_smoke
    assert "smoke succeeded but cleanup failed" in cross_cluster_smoke


def test_n100_kwok_verifier_is_bounded_and_does_not_run_workloads():
    pipeline = PIPELINE_PATH.read_text(encoding="utf-8")
    resume = RESUME_JOB_PATH.read_text(encoding="utf-8")
    verifier = MOCK_VERIFY_SCRIPT.read_text(encoding="utf-8")

    assert "values:\n  - none\n  - capture\n  - verify" in resume
    assert "scaleDebugKwokBaselineBuildId" in pipeline
    assert "mock_preservation_baseline_build_id" in resume
    assert "fault injection is bounded to at most five clusters" in verifier
    assert "fault count must be between one and five" in verifier
    assert "expected_pool_count" in verifier
    assert "Fleet member is not Succeeded/Connected" in verifier
    assert (
        "${{ if and(parameters.run_workload, "
        "eq(parameters.mock_preservation_mode, 'none')) }}:"
        in resume
    )
    assert "condition: always()" in resume[
        resume.index('displayName: "Publish preserved n100 KWOK verification"') :
    ]


def test_n100_workload_handoff_requires_verified_artifacts_before_execute():
    resume = RESUME_JOB_PATH.read_text(encoding="utf-8")
    handoff = MOCK_HANDOFF_SCRIPT.read_text(encoding="utf-8")
    handoff_template = MOCK_HANDOFF_TEMPLATE_PATH.read_text(encoding="utf-8")
    mock_execute = MOCK_EXECUTE_TEMPLATE_PATH.read_text(encoding="utf-8")
    execute_tests = EXECUTE_TESTS_PATH.read_text(encoding="utf-8")

    assert (
        "or(eq(parameters.mock_preservation_mode, 'verify'), "
        "and(parameters.run_workload, "
        "eq(parameters.mock_preservation_mode, 'none'), "
        "eq(parameters.topology, 'clustermesh-scale-mock')))"
        in resume
    )
    assert "n100-kwok-preservation-" in resume
    assert "n100-kwok-verification-" in resume
    assert "validate_verification_artifact" in handoff
    assert "restore_state" in handoff
    assert "run_reconciler" in handoff
    assert "validate_platform_state" in handoff
    assert "capture_live" in handoff
    assert "attempts=10" in handoff
    assert "settle_seconds=30" in handoff
    assert "workloads_started" in handoff
    assert "mock_redeployed" in handoff
    assert "desired_state_files_restored" in handoff
    assert "Restore verified n100 KWOK state before workloads" in handoff_template
    assert "Validate post-telemetry n100 workload data path" in handoff_template
    assert "Finalize incomplete n100 workload handoff evidence" in handoff_template
    assert "Publish n100 workload handoff" in handoff_template
    assert "pipeline_failed_before_workload_handoff_completion" in handoff_template
    assert 'CLUSTERMESH_CROSS_CLUSTER_SMOKE_ENABLED: "true"' in handoff_template
    assert "- name: mock_handoff_enabled" in mock_execute
    assert "${{ if parameters.mock_handoff_enabled }}:" in mock_execute
    assert "${{ if not(parameters.mock_handoff_enabled) }}:" in mock_execute
    handoff_pos = mock_execute.index("verified-workload-handoff.yml")
    deploy_pos = mock_execute.index("deploy-mock-layer.yml")
    engine_pos = mock_execute.index(
        "/steps/engine/clusterloader2/clustermesh-scale/execute.yml"
    )
    assert handoff_pos < engine_pos
    assert deploy_pos < engine_pos
    assert "mock_handoff_enabled" in execute_tests
    assert "eq(parameters.topology, 'clustermesh-scale-mock')" in execute_tests


def test_preserved_resume_repair_is_bounded_and_non_destructive():
    arm = AKS_ARM_RECONCILE_SCRIPT.read_text(encoding="utf-8")
    worker = WORKER_RECONCILE_SCRIPT.read_text(encoding="utf-8")
    live = LIVE_OVERLAY_SCRIPT.read_text(encoding="utf-8")

    for forbidden in ("az aks delete", "az group delete", "az fleet delete"):
        assert forbidden not in arm
        assert forbidden not in worker
        assert forbidden not in live
    assert "max-repair-clusters" in worker
    assert "OverlaymgrReconcileError" in arm
    assert '"aks",\n        "update"' in arm
    assert "inventory-timeout-seconds" in arm
    assert "TRANSIENT_READ_RE" in arm
    assert "delete-instances" in worker
    assert "stale_instance_ids" in worker
    assert "repair_roles_for_drift" in live
    assert "cluster-mesh" in live


def test_destructive_scripts_require_exact_confirmation(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    az_log = tmp_path / "az.log"
    fake_az = fake_bin / "az"
    fake_az.write_text(
        "#!/usr/bin/env bash\n"
        'echo "$*" >> "$AZ_LOG"\n'
        "exit 99\n",
        encoding="utf-8",
    )
    fake_az.chmod(0o755)
    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{fake_bin}:{env['PATH']}",
            "AZ_LOG": str(az_log),
            "CLUSTERMESH_DEBUG_TARGET_RUN_ID": "12345-deadbeef",
            "CLUSTERMESH_DEBUG_EXPECTED_SUBSCRIPTION_ID": "test-subscription",
        }
    )

    delete_env = env | {"CLUSTERMESH_DEBUG_CONFIRM_DELETE": "wrong"}
    delete_result = subprocess.run(
        ["bash", str(DELETE_SCRIPT)],
        env=delete_env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert delete_result.returncode != 0
    assert "confirmation mismatch" in delete_result.stderr.lower()

    reset_env = env | {"CLUSTERMESH_DEBUG_CONFIRM_RESET": "wrong"}
    reset_result = subprocess.run(
        ["bash", str(RESET_SCRIPT)],
        env=reset_env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert reset_result.returncode != 0
    assert "confirmation mismatch" in reset_result.stderr.lower()
    assert not az_log.exists()


def test_connected_member_can_be_forced_through_surgical_repair(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    az_log = tmp_path / "az.log"
    fake_az = fake_bin / "az"
    fake_az.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'echo "$*" >> "$AZ_LOG"\n'
        'case "$*" in\n'
        '  "fleet show "*) echo "{}" ;;\n'
        '  "fleet clustermeshprofile show "*) echo "{}" ;;\n'
        '  "fleet member list "*) cat <<\'JSON\'\n'
        '[{"name":"mesh-1","clusterResourceId":"/subscriptions/s/mesh-1",'
        '"labels":{"mesh":"true"}},'
        '{"name":"mesh-2","clusterResourceId":"/subscriptions/s/mesh-2",'
        '"labels":{"mesh":"true"}}]\n'
        "JSON\n"
        "    ;;\n"
        '  "fleet clustermeshprofile list-members "*) cat <<\'JSON\'\n'
        '[{"name":"mesh-1","meshProperties":{"status":{"state":"Connected"}}},'
        '{"name":"mesh-2","meshProperties":{"status":{"state":"Connected"}}}]\n'
        "JSON\n"
        "    ;;\n"
        '  "resource list "*) cat <<\'JSON\'\n'
        '[{"name":"mesh-1","clusterResourceId":"/subscriptions/s/mesh-1"},'
        '{"name":"mesh-2","clusterResourceId":"/subscriptions/s/mesh-2"}]\n'
        "JSON\n"
        "    ;;\n"
        '  "fleet member update "*|"fleet clustermeshprofile apply "*|'
        '"group update "*) ;;\n'
        '  *) echo "unexpected az command: $*" >&2; exit 98 ;;\n'
        "esac\n",
        encoding="utf-8",
    )
    fake_az.chmod(0o755)
    fake_sleep = fake_bin / "sleep"
    fake_sleep.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_sleep.chmod(0o755)
    forced_roles = tmp_path / "roles.txt"
    forced_roles.write_text("mesh-2\n", encoding="utf-8")

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{fake_bin}:{env['PATH']}",
            "AZ_LOG": str(az_log),
            "CLUSTERMESH_DEBUG_TARGET_RUN_ID": "12345-deadbeef",
            "CLUSTERMESH_DEBUG_EXPECTED_CLUSTER_COUNT": "2",
            "CLUSTERMESH_DEBUG_MAX_REPAIR_MEMBERS": "2",
            "CLUSTERMESH_DEBUG_FORCE_REPAIR_ROLES_FILE": str(forced_roles),
            "CLUSTERMESH_DEBUG_REPAIR_DETACH_SETTLE_SECONDS": "1",
            "CLUSTERMESH_DEBUG_REPAIR_WAIT_SECONDS": "2",
            "CLUSTERMESH_DEBUG_REPAIR_POLL_SECONDS": "1",
            "CLUSTERMESH_DEBUG_REPAIR_APPLY_RETRY_SECONDS": "1",
            "CLUSTERMESH_DEBUG_REPAIR_APPLY_ATTEMPTS": "1",
            "BUILD_ARTIFACTSTAGINGDIRECTORY": str(tmp_path / "artifacts"),
        }
    )
    result = subprocess.run(
        ["bash", str(REPAIR_SCRIPT)],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    log = az_log.read_text(encoding="utf-8")
    assert log.count("fleet member update") == 2
    assert "fleet member update" in log
    assert "--name mesh-2" in log
    assert "--name mesh-1 --labels" not in log


def test_surgical_repair_restores_labels_when_detach_fails(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    az_log = tmp_path / "az.log"
    fake_az = fake_bin / "az"
    fake_az.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        'echo "$*" >> "$AZ_LOG"\n'
        'case "$*" in\n'
        '  "fleet show "*) echo "{}" ;;\n'
        '  "fleet clustermeshprofile show "*) echo "{}" ;;\n'
        '  "fleet member list "*) cat <<\'JSON\'\n'
        '[{"name":"mesh-1","clusterResourceId":"/subscriptions/s/mesh-1",'
        '"labels":{"mesh":"true"}},'
        '{"name":"mesh-2","clusterResourceId":"/subscriptions/s/mesh-2",'
        '"labels":{"mesh":"true"}}]\n'
        "JSON\n"
        "    ;;\n"
        '  "fleet clustermeshprofile list-members "*) cat <<\'JSON\'\n'
        '[{"name":"mesh-1","meshProperties":{"status":{"state":"Connected"}}},'
        '{"name":"mesh-2","meshProperties":{"status":{"state":"Connected"}}}]\n'
        "JSON\n"
        "    ;;\n"
        '  "resource list "*) cat <<\'JSON\'\n'
        '[{"name":"mesh-1","clusterResourceId":"/subscriptions/s/mesh-1"},'
        '{"name":"mesh-2","clusterResourceId":"/subscriptions/s/mesh-2"}]\n'
        "JSON\n"
        "    ;;\n"
        '  "fleet member update "*"--name mesh-2"*"--labels mesh=repairing"*) '
        "exit 42 ;;\n"
        '  "fleet member update "*|"fleet clustermeshprofile apply "*) ;;\n'
        '  *) echo "unexpected az command: $*" >&2; exit 98 ;;\n'
        "esac\n",
        encoding="utf-8",
    )
    fake_az.chmod(0o755)
    fake_sleep = fake_bin / "sleep"
    fake_sleep.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_sleep.chmod(0o755)
    forced_roles = tmp_path / "roles.txt"
    forced_roles.write_text("mesh-1\nmesh-2\n", encoding="utf-8")

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{fake_bin}:{env['PATH']}",
            "AZ_LOG": str(az_log),
            "CLUSTERMESH_DEBUG_TARGET_RUN_ID": "12345-deadbeef",
            "CLUSTERMESH_DEBUG_EXPECTED_CLUSTER_COUNT": "2",
            "CLUSTERMESH_DEBUG_MAX_REPAIR_MEMBERS": "2",
            "CLUSTERMESH_DEBUG_FORCE_REPAIR_ROLES_FILE": str(forced_roles),
            "CLUSTERMESH_DEBUG_REPAIR_DETACH_SETTLE_SECONDS": "1",
            "CLUSTERMESH_DEBUG_REPAIR_WAIT_SECONDS": "2",
            "CLUSTERMESH_DEBUG_REPAIR_POLL_SECONDS": "1",
            "CLUSTERMESH_DEBUG_REPAIR_APPLY_RETRY_SECONDS": "1",
            "CLUSTERMESH_DEBUG_REPAIR_APPLY_ATTEMPTS": "1",
        }
    )
    result = subprocess.run(
        ["bash", str(REPAIR_SCRIPT)],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    log = az_log.read_text(encoding="utf-8")
    assert "--name mesh-1 --labels mesh=true" in log
    assert "--name mesh-2 --labels mesh=true" in log
    assert "Repair interrupted; restoring Fleet selector labels" in result.stderr


def test_pipeline_and_debug_templates_parse_as_yaml():
    yaml.safe_load(PIPELINE_PATH.read_text(encoding="utf-8"))
    yaml.safe_load(COMPETITIVE_JOB_PATH.read_text(encoding="utf-8"))
    yaml.safe_load(RESUME_JOB_PATH.read_text(encoding="utf-8"))
    yaml.safe_load(
        (REUSE_DIR / "write-resume-manifest.yml").read_text(encoding="utf-8")
    )


def test_resume_manifest_template_preserves_legacy_defaults():
    template = (
        REUSE_DIR / "write-resume-manifest.yml"
    ).read_text(encoding="utf-8")

    assert "- name: expected_cluster_count\n  type: number\n  default: 100" in template
    assert "- name: expected_fleet_count\n  type: number\n  default: -1" in template
