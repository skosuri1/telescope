"""Read-only diagnostics must preserve failing observations without changing resources."""

import importlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]
MODULE_DIR = ROOT / "modules/python/clusterloader2/clustermesh-scale"
sys.path.insert(0, str(MODULE_DIR))
try:
    diagnostics = importlib.import_module("preserved_readiness_diagnostics")
finally:
    sys.path.pop(0)


def source():
    return {
        "resource_group": diagnostics.RUN_ID, "healthy": False,
        "initial_failed_pools": [{"role": "mesh-2"}, {"role": "mesh-94"}],
        "live_overlay_recovery": {"initial": {"proof": {
            "cluster_count": 100,
            "drift": [{"role": role, "command_error": "Cilium agent Pod is not Running/Ready"}
                      for role in ("mesh-51", "mesh-66", "mesh-79", "mesh-89")],
        }}},
    }


def test_exact_failed_workload_selects_only_six_observed_roles():
    payload = source()
    assert diagnostics.selected_roles(payload) == ["mesh-2", "mesh-51", "mesh-66", "mesh-79", "mesh-89", "mesh-94"]
    payload["initial_failed_pools"].append({"role": "mesh-96"})
    with pytest.raises(diagnostics.workers.ReconcileError):
        diagnostics.selected_roles(payload)


@pytest.mark.parametrize("command", [
    ["az", "aks", "update"], ["az", "aks", "nodepool", "add"], ["az", "vmss", "restart"],
    ["kubectl", "delete", "pod"], ["kubectl", "patch", "node"],
    ["kubectl", "exec", "pod", "--", "sh", "-c", "anything"],
])
def test_write_commands_never_reach_the_runner(tmp_path, command):
    reader = diagnostics.Reader(tmp_path, lambda *_args: pytest.fail("A mutating command was invoked"))
    with pytest.raises(diagnostics.workers.ReconcileError):
        reader.capture("forbidden", command, kubeconfig="private")


def test_read_failures_are_explicit_and_private_config_contents_are_not_captured(tmp_path):
    def unavailable(_command, _timeout):
        raise diagnostics.workers.ReconcileError("kubelet connection refused")

    reader = diagnostics.Reader(tmp_path, unavailable)
    assert reader.capture("cilium", ["kubectl", "exec", "cilium-one", "--", "cilium-dbg", "status", "-o", "json"],
                          kubeconfig="/private/cluster.config") is None
    saved = json.loads((tmp_path / "cilium-error.json").read_text(encoding="utf-8"))
    assert saved["success"] is False and "connection refused" in saved["error"]


def test_raw_object_diagnostics_redact_inline_credentials(tmp_path):
    reader = diagnostics.Reader(tmp_path, lambda *_args: json.dumps({
        "items": [{"spec": {"containers": [{"env": [{"name": "ACCESS_TOKEN", "value": "never-publish"}]}]}}],
    }))
    reader.capture("pods", ["kubectl", "get", "pods", "-o", "json"], kubeconfig="private")
    assert "never-publish" not in (tmp_path / "pods.json").read_text(encoding="utf-8")


def test_role_capture_preserves_unready_pod_and_unavailable_host_evidence(tmp_path):
    cluster_id = f"/subscriptions/{diagnostics.SUBSCRIPTION}/resourceGroups/{diagnostics.RUN_ID}/providers/Microsoft.ContainerService/managedClusters/clustermesh-51"
    node_group = "mc-preserved-mesh51"
    node = "aks-default-vmss000000"
    commands = []

    def runner(command, _timeout):
        commands.append(command)
        if command[0] == "az":
            if command[1:3] == ["aks", "get-credentials"]:
                Path(command[command.index("--file") + 1]).write_text("private-value", encoding="utf-8")
                return ""
            if command[1:3] == ["group", "show"]:
                return json.dumps({"managedBy": cluster_id, "location": diagnostics.REGION})
            if command[1:3] == ["vmss", "get-instance-view"]:
                return json.dumps({"vmAgent": {"statuses": [{"code": "ProvisioningState/Unavailable"}]}})
            return "[]"
        if "exec" in command:
            raise diagnostics.workers.ReconcileError("container is not running")
        if "nodes" in command:
            return json.dumps({"items": [{
                "metadata": {"name": node, "uid": "node-uid"}, "status": {"conditions": [{"type": "Ready", "status": "Unknown"}]},
                "spec": {"providerID": f"azure:///subscriptions/{diagnostics.SUBSCRIPTION}/resourceGroups/{node_group}/providers/Microsoft.Compute/virtualMachineScaleSets/aks-default-vmss/virtualMachines/0"},
            }]})
        if "pods" in command:
            return json.dumps({"items": [{
                "metadata": {"name": "cilium-one", "uid": "pod-uid", "namespace": "kube-system", "labels": {"k8s-app": "cilium"}},
                "spec": {"nodeName": node}, "status": {"phase": "Running", "containerStatuses": [{"name": "cilium-agent", "ready": False}]},
            }]})
        return json.dumps({"items": []})

    report = diagnostics.collect_role(
        {"id": cluster_id, "name": "clustermesh-51", "tags": {"role": "mesh-51"}, "nodeResourceGroup": node_group},
        tmp_path, runner,
    )
    assert report["read_only"] and report["unready_cilium"][0]["node"] == node
    assert any(row["name"].endswith("instance-view") and row["success"] for row in report["reads"])
    assert any(row["name"] == "cilium-one-status" and not row["success"] for row in report["reads"])
    assert not any("private-value" in path.read_text(encoding="utf-8") for path in tmp_path.rglob("*.json"))
    config = next(command[command.index("--file") + 1] for command in commands if "--file" in command)
    assert not Path(config).exists()


@pytest.mark.parametrize("change", [{}, {"source_build_id": 80016}, {"run_workload": True}, {"exclusive_modes": False}])
def test_diagnostic_job_requires_the_exact_readonly_failed_build(change):
    job = yaml.safe_load((ROOT / "jobs/clustermesh-readiness-diagnostics.yml").read_text(encoding="utf-8"))["jobs"][0]
    scope = {
        "target_run_id": diagnostics.RUN_ID, "confirm_resume": diagnostics.RUN_ID,
        "expected_subscription_id": diagnostics.SUBSCRIPTION, "expected_region": diagnostics.REGION,
        "expected_cluster_count": 100,
        "tfvars_path": "scenarios/perf-eval/clustermesh-scale/terraform-inputs/azure-100-mock-shared.tfvars",
        "overlay_mode": "resume-existing", "run_workload": False, "source_build_id": 80017, "exclusive_modes": True,
    }
    result = subprocess.run(["bash", "-c", job["steps"][0]["script"]],
                            env={**os.environ, "SCOPE_JSON": json.dumps({**scope, **change})},
                            capture_output=True, text=True, timeout=5, check=False)
    assert (result.returncode == 0) is (not change)
    assert job["steps"][-1]["condition"] == "always()"
    assert job["steps"][-2]["retryCountOnTaskFailure"] == 0


def test_diagnostic_route_is_a_readonly_alternative_not_an_additional_maintenance_job():
    pipeline = yaml.safe_load((ROOT / "pipelines/system/new-pipeline-test.yml").read_text(encoding="utf-8"))
    stage = next(row for row in pipeline["stages"] if row["stage"] == "azure_eastus2euap_n100_debug_resume_37deca")
    outer = "${{ if ne(parameters.scaleDebugPostRetirementPromBuildId, 0) }}"
    branches = next(row[outer] for row in stage["jobs"] if outer in row)
    diagnostic = branches[0]["${{ if eq(parameters.scaleDebugPostRetirementPromBuildId, 80017) }}"][0]
    assert diagnostic["template"] == "/jobs/clustermesh-readiness-diagnostics.yml"
    assert diagnostic["parameters"]["source_build_id"] == "${{ parameters.scaleDebugPostRetirementPromBuildId }}"
    assert branches[1]["${{ else }}"][0]["template"] == "/jobs/clustermesh-post-retirement-prom.yml"
