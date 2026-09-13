"""Scoped, read-only CNS comparison and private credential lifecycle."""

import copy
import importlib.util
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]
MODULE = ROOT / "modules/python/clusterloader2/clustermesh-scale/secondary_cns_diagnostics.py"
SPEC = importlib.util.spec_from_file_location("secondary_cns_diagnostics", MODULE)
diagnostics = importlib.util.module_from_spec(SPEC)
sys.path.insert(0, str(MODULE.parent))
try:
    SPEC.loader.exec_module(diagnostics)
finally:
    sys.path.pop(0)
JOB = ROOT / "jobs/clustermesh-secondary-cns-diagnostics.yml"


def identifier(name):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, name))


class Cloud:
    def __init__(self, *, changed=False, exec_error=False):
        self.changed, self.exec_error = changed, exec_error
        self.commands, self.configs, self.execs = [], [], []
        self.clusters = [{
            "id": f"/subscriptions/{diagnostics.capacity.SUBSCRIPTION}/resourceGroups/"
                  f"{diagnostics.capacity.RESOURCE_GROUP}/providers/Microsoft.ContainerService/"
                  f"managedClusters/clustermesh-{index}",
            "name": f"clustermesh-{index}", "nodeResourceGroup": f"mc-clustermesh-{index}",
            "location": diagnostics.capacity.REGION, "tags": {"role": f"mesh-{index}"},
        } for index in range(1, 101)]
        self.nodes, self.pods = {}, {}
        for role, targets in diagnostics.TARGETS.items():
            self.nodes[role], self.pods[role] = [], []
            for name, node_uid in targets.items():
                self.nodes[role].append({
                    "metadata": {"name": name, "uid": node_uid},
                    "spec": {"providerID": f"azure:///subscriptions/{diagnostics.capacity.SUBSCRIPTION}"
                             f"/resourceGroups/mc-clustermesh-{role[5:]}/providers/Microsoft.Compute/"
                             f"virtualMachineScaleSets/{name[:-6]}/virtualMachines/{int(name[-6:])}"},
                    "status": {"conditions": [{"type": "Ready", "status": "True"}]},
                })
                self.pods[role].append({
                    "metadata": {
                        "name": f"azure-cns-{role}-{name[-6:]}", "namespace": "kube-system",
                        "uid": identifier(role + name), "labels": {"k8s-app": "azure-cns"},
                        "ownerReferences": [{"kind": "DaemonSet", "name": "azure-cns",
                                             "controller": True, "uid": identifier(role)}],
                    },
                    "spec": {"nodeName": name, "containers": [{
                        "name": "cns-container",
                        "image": "mcr.microsoft.com/containernetworking/v2/azure-cns:v1.8.12",
                    }], "volumes": [{"configMap": {"name": "azure-cns"}}]},
                    "status": {
                        "phase": "Running", "podIP": "10.0.0.1",
                        "conditions": [{"type": "Ready", "status": "True"}],
                        "containerStatuses": [{"name": "cns-container", "ready": True,
                                               "containerID": "containerd://original", "restartCount": 0}],
                    },
                })

    def __call__(self, command, _timeout):
        self.commands.append(command)
        if command[:3] == ["az", "account", "show"]:
            return json.dumps({"id": diagnostics.capacity.SUBSCRIPTION})
        if command[:3] == ["az", "group", "show"]:
            return json.dumps({"location": diagnostics.capacity.REGION, "tags": {
                "run_id": diagnostics.capacity.RESOURCE_GROUP,
                "scenario": "perf-eval-clustermesh-scale", "clustermesh_debug_expected_clusters": "100",
                "clustermesh_debug_preserved": "true",
                "clustermesh_debug_tfvars_sha256": diagnostics.capacity.TFVARS_SHA,
            }})
        if command[:3] == ["az", "aks", "list"]:
            return json.dumps(self.clusters)
        if command[:3] == ["az", "aks", "get-credentials"]:
            path = Path(command[command.index("--file") + 1])
            path.write_text("private-credential-sentinel", encoding="utf-8")
            self.configs.append(path)
            return "credentials read"
        if command[0] == "az":
            return json.dumps({})
        config = command[command.index("--kubeconfig") + 1]
        role = "mesh-2" if "mesh-2-" in config else "mesh-89"
        if "exec" in command:
            assert tuple(command[command.index("--") + 1:]) == diagnostics.CNS_COMMAND
            self.execs.append(command)
            if self.exec_error:
                raise diagnostics.workers.ReconcileError("CNS diagnostic endpoint unavailable")
            return "10.0.0.2 PendingProgramming NCVersion:1"
        if "logs" in command:
            return "GET authenticationToken/super-secret/api-version/1 Authorization: Bearer bearer-secret"
        if "nodes" in command:
            return json.dumps({"items": self.nodes[role]})
        if "pods" in command:
            return json.dumps({"items": self.pods[role]})
        if "pod" in command:
            name = command[command.index("pod") + 1]
            pod = copy.deepcopy(next(row for row in self.pods[role] if row["metadata"]["name"] == name))
            if self.changed:
                pod["metadata"]["uid"] = identifier("changed")
            return json.dumps(pod)
        if any("proxy/metrics" in arg for arg in command):
            return "cns_ip_count 32"
        return json.dumps({"items": []})


def source_fixture(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    receipt = {
        "execute": True, "success": False, "capacity_source_build_id": 80039,
        "error": "mesh-89: actual NNC version/IP growth exceeded the bounded wait",
        "cleanup_errors": [], "workloads_ready": False,
        "per_role": {role: {
            "probe_cleanup_pending": [], "capacity_qualified": role != "mesh-89",
            "probe_receipts": {str(index): {"create_accepted": True, "delete_accepted": True,
                                           "absence_observed": True} for index in range(count)},
        } for role, count in (("mesh-51", 61), ("mesh-66", 48), ("mesh-79", 40), ("mesh-89", 15))},
    }
    path = source / "qualification.json"
    path.write_text(json.dumps(receipt), encoding="utf-8")
    monkeypatch.setattr(diagnostics, "SOURCE_SHA", diagnostics.capacity.digest(path.read_bytes()))
    return source


@pytest.mark.parametrize("fault", ["none", "exec-error", "identity-change"])
def test_read_only_diagnostic_collects_three_targets_and_cleans_private_files(tmp_path, monkeypatch, fault):
    source = source_fixture(tmp_path, monkeypatch)
    output = tmp_path / "output"
    cloud = Cloud(changed=fault == "identity-change", exec_error=fault == "exec-error")
    args = ["--source-directory", str(source), "--source-build-id", "80046",
            "--expected-tfvars-sha", diagnostics.capacity.TFVARS_SHA,
            "--output-directory", str(output)]
    if fault == "identity-change":
        with pytest.raises(diagnostics.workers.ReconcileError, match="changed identity"):
            diagnostics.main(args, cloud)
    else:
        assert diagnostics.main(args, cloud) == 0
        summary = json.loads((output / "summary.json").read_text())
        assert summary["read_only"] and summary["resource_mutations"] == 0 and not summary["health_claimed"]
        assert len(cloud.execs) == 3
        assert sum("proxy/metrics" in " ".join(command) for command in cloud.commands) == 3
    assert cloud.configs and all(not path.exists() for path in cloud.configs)
    content = "\n".join(path.read_text() for path in output.rglob("*.json"))
    assert "private-credential-sentinel" not in content
    assert "super-secret" not in content and "bearer-secret" not in content


@pytest.mark.parametrize("command", [
    ["az", "aks", "nodepool", "add"], ["az", "aks", "update"],
    ["kubectl", "delete", "pod", "x"], ["kubectl", "get", "secrets"],
    ["kubectl", "exec", "x", "-c", "cns-container", "--", "/bin/sh", "-c", "anything"],
    ["kubectl", "exec", "x", "-c", "cns-container", "--", "/usr/local/bin/azure-cns"],
])
def test_mutations_secrets_and_arbitrary_exec_are_rejected(tmp_path, command):
    calls = []
    reader = diagnostics.Reader(tmp_path, lambda args, _timeout: calls.append(args))
    with pytest.raises(diagnostics.workers.ReconcileError):
        reader.capture("forbidden", command, config=tmp_path / "private")
    assert not calls


def test_cleanup_receipt_change_blocks_every_live_read(tmp_path, monkeypatch):
    source = source_fixture(tmp_path, monkeypatch)
    with (source / "qualification.json").open("a") as handle:
        handle.write(" ")
    with pytest.raises(diagnostics.workers.ReconcileError, match="exact build"):
        diagnostics.validate_source(source)


@pytest.mark.parametrize("source_build", [80046, 80039, 80044, "80046"])
def test_job_rejects_other_source_and_string_build_ids(source_build):
    definition = yaml.safe_load(JOB.read_text())["jobs"][0]
    scope = {
        "target_run_id": diagnostics.capacity.RESOURCE_GROUP, "confirm_resume": diagnostics.capacity.RESOURCE_GROUP,
        "expected_subscription_id": diagnostics.capacity.SUBSCRIPTION, "expected_region": diagnostics.capacity.REGION,
        "expected_cluster_count": 100,
        "tfvars_path": "scenarios/perf-eval/clustermesh-scale/terraform-inputs/azure-100-mock-shared.tfvars",
        "overlay_mode": "resume-existing", "run_workload": False,
        "source_build_id": source_build, "exclusive_modes": True,
    }
    result = subprocess.run(["bash", "-c", definition["steps"][0]["script"]],
                            env={**os.environ, "SCOPE_JSON": json.dumps(scope)},
                            capture_output=True, text=True, check=False, timeout=5)
    assert (result.returncode == 0) is (source_build == 80046)


def test_diagnostic_route_and_whole_failed_artifact_are_exclusive():
    pipeline = yaml.safe_load((ROOT / "pipelines/system/new-pipeline-test.yml").read_text())
    assert len(pipeline["stages"]) == 43
    stage = next(row for row in pipeline["stages"] if row["stage"] == "azure_eastus2euap_n100_debug_resume_37deca")
    outer = "${{ if ne(parameters.scaleDebugPostRetirementPromBuildId, 0) }}"
    branches = next(row[outer] for row in stage["jobs"] if outer in row)
    selector = "${{ elseif eq(parameters.scaleDebugPostRetirementPromBuildId, 80046) }}"
    selected = next(row[selector][0] for row in branches if selector in row)
    assert selected["template"] == "/jobs/clustermesh-secondary-cns-diagnostics.yml"
    assert "eq(parameters.scaleDebugModernBaselineBuildId, 0)" in selected["parameters"]["exclusive_modes"]
    job = yaml.safe_load(JOB.read_text())["jobs"][0]
    download = next(row["inputs"] for row in job["steps"] if row.get("task") == "DownloadPipelineArtifact@2")
    assert download["allowFailedBuilds"] is True and "itemPattern" not in download
    assert job["steps"][-1]["condition"] == "always()"
