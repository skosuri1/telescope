"""Run the real quota-observation shell with read-only Azure fakes."""

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]
STEP = ROOT / "steps/topology/clustermesh-scale/reuse/observe-native-prom-quota.yml"
SUBSCRIPTION = "37deca37-c375-4a14-b90a-043849bd2bf1"


@pytest.mark.parametrize("fault", [
    "none", "scope", "checkpoint", "absent", "forbidden", "timeout", "foreign-cluster", "bad-counter",
    "managed-absent", "managed-forbidden", "activity-forbidden", "kube-forbidden", "cluster-scope",
])
@pytest.mark.parametrize("family,total", [(32, 100), (8, 100), (0, 100), (100, 0), (-16, 100)])
@pytest.mark.parametrize("counter_type", ["number", "string"])
def test_quota_observer_never_mutates_or_claims_workload_readiness(tmp_path, fault, family, total, counter_type):
    script = yaml.safe_load(STEP.read_text(encoding="utf-8"))["steps"][0]["script"]
    native = tmp_path / "native"
    native.mkdir()
    checkpoint = {
        "execute": True, "mutation_started": True,
        "original_identity": {
            "node_uid": "9d8a9811-0e9a-4ff5-9a94-db89c42d223b",
            "vm_id": "d731b838-501d-438d-a087-fd4545f1d607",
        },
        "replacement": {
            "delete": {"accepted": True, "ambiguous": False},
            "native_removal": {"pool_count": 0, "vmss_capacity": 0, "old_node_pods_nnc_absent": True},
            "restore": {"attempted": True},
        },
        "error": "ErrCode_InsufficientVCPUQuota",
    }
    if fault == "checkpoint":
        checkpoint["replacement"]["delete"]["accepted"] = False
    (native / "recovery.json").write_text(json.dumps(checkpoint), encoding="utf-8")
    fake = tmp_path / "az"
    fake.write_text(
        f"#!{sys.executable}\n" + textwrap.dedent(
            """
            import json
            import os
            import sys
            from pathlib import Path
            import jmespath

            args = sys.argv[1:]
            def arg(name):
                return args[args.index(name) + 1]
            with Path(os.environ["CALLS"]).open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(args) + "\\n")
            sub = os.environ["EXPECTED_SUBSCRIPTION_ID"]
            assert args[:2] == ["account", "show"] or arg("--subscription") == sub
            assert not set(args) & {"delete", "scale", "restart", "reimage", "create", "update", "set"}
            prefix = f"/subscriptions/{sub}/resourceGroups/"
            fault = os.environ["FAULT"]
            if args[:2] == ["account", "show"]:
                value = {"id": sub}
            elif args[:2] == ["vm", "list-usage"]:
                assert arg("--location") == "eastus2euap"
                value = [
                    {"name": {"value": name}, "currentValue": 100, "limit": 100 + remaining, "unit": "Count"}
                    for name, remaining in [
                        ("standardDv3Family", int(os.environ["FAMILY"])),
                        ("cores", int(os.environ["TOTAL"])),
                        ("standardDSv5Family", 900),
                    ]
                ]
                if os.environ["COUNTER_TYPE"] == "string":
                    for row in value:
                        row["currentValue"], row["limit"] = str(row["currentValue"]), str(row["limit"])
                if fault == "bad-counter":
                    value[0]["currentValue"] = "100.5"
            elif args[:2] == ["group", "show"]:
                name = arg("--name")
                if name.startswith("mc_79825-24946a3a_") and fault in ("managed-absent", "managed-forbidden"):
                    code = "ResourceGroupNotFound" if fault == "managed-absent" else "AuthorizationFailed"
                    print(f"ERROR: ({code}) explicit fake managed-group read", file=sys.stderr)
                    sys.exit(1)
                if name == "79825-24946a3a" and fault in ("absent", "forbidden", "timeout"):
                    code = {"absent": "ResourceGroupNotFound", "forbidden": "AuthorizationFailed",
                            "timeout": "GatewayTimeout"}[fault]
                    print(f"ERROR: ({code}) explicit fake read error", file=sys.stderr)
                    sys.exit(1)
                assert name in {"78751-f36f3d5a", "79825-24946a3a"} or name.startswith("mc_79825-24946a3a_")
                value = {"id": prefix + name, "name": name, "location": "eastus2euap",
                         "tags": {"run_id": name}, "properties": {"provisioningState": "Succeeded"}}
            elif args[:3] == ["aks", "operation", "show-latest"]:
                value = {"name": "operation", "status": "Succeeded", "operationType": "DeleteMachines"}
            elif args[:3] == ["aks", "nodepool", "list"]:
                value = [
                    {"name": "default", "count": 2, "mode": "System", "provisioningState": "Succeeded"},
                    {"name": "prompool", "count": 0, "mode": "User", "provisioningState": "Succeeded"},
                ]
            elif args[:2] == ["aks", "show"]:
                assert arg("--name") == "clustermesh-96" and arg("--resource-group") == "78751-f36f3d5a"
                value = {"id": prefix + "78751-f36f3d5a/providers/Microsoft.ContainerService/managedClusters/clustermesh-96",
                         "name": "clustermesh-96", "provisioningState": "Succeeded",
                         "location": "eastus2euap", "tags": {"role": "mesh-96"},
                         "nodeResourceGroup": "MC_78751-f36f3d5a_clustermesh-96_eastus2euap"}
                if fault == "cluster-scope":
                    value["nodeResourceGroup"] = "foreign-group"
            elif args[:2] == ["aks", "get-credentials"]:
                assert arg("--name") == "clustermesh-96" and arg("--resource-group") == "78751-f36f3d5a"
                assert arg("--context") == "clustermesh-96"
                target = Path(arg("--file"))
                assert target.parent.parent == Path(os.environ["AGENT_TEMP_DIRECTORY"])
                target.write_text("private-test-credentials", encoding="utf-8")
                sys.exit(0)
            elif args[:2] == ["vmss", "get-instance-view"]:
                assert arg("--name") == "aks-default-28928250-vmss"
                if "--instance-id" in args:
                    assert arg("--instance-id") in ("0", "1")
                    value = {"statuses": [{"code": "PowerState/running"}], "extensions": [],
                             "maintenanceRedeployStatus": {"isCustomerInitiatedMaintenanceAllowed": False}}
                else:
                    value = {"statuses": [{"code": "ProvisioningState/failed"}],
                             "virtualMachine": {"statusesSummary": [{"code": "ProvisioningState/failed", "count": 1}]}}
            elif args[:3] == ["monitor", "activity-log", "list"]:
                assert arg("--resource-id") == (
                    f"/subscriptions/{sub}/resourceGroups/"
                    "mc_78751-f36f3d5a_clustermesh-96_eastus2euap/providers/Microsoft.Compute/"
                    "virtualMachineScaleSets/aks-default-28928250-vmss"
                )
                assert arg("--offset") == "2h" and arg("--max-events") == "100"
                if fault == "activity-forbidden":
                    print("ERROR: (AuthorizationFailed) Activity Log is not readable", file=sys.stderr)
                    sys.exit(1)
                value = [{"operationName": {"value": "Microsoft.Compute/virtualMachineScaleSets/restart/action"},
                          "status": {"value": "Failed"}, "properties": {"statusMessage": "captured provider error"}}]
            elif args[:2] == ["aks", "list"]:
                assert arg("--resource-group") == "79825-24946a3a"
                names = ["foreign"] if fault == "foreign-cluster" else ["clustermesh-1", "clustermesh-2"]
                value = [{"name": name, "nodeResourceGroup": f"mc_79825-24946a3a_{name}_eastus2euap"}
                         for name in names]
            elif args[:2] == ["vmss", "list"]:
                group = arg("--resource-group")
                assert group.startswith(("mc_78751-f36f3d5a_clustermesh-96_", "mc_79825-24946a3a_"))
                value = [{"name": "allowed-vmss", "sku": {"name": "Standard_D8_v3", "capacity": 0},
                          "provisioningState": "Succeeded"}]
            elif args[:2] == ["vm", "list-skus"]:
                assert arg("--size") == "Standard_D8s_v5" and arg("--location") == "eastus2euap"
                value = [{"name": "Standard_D8s_v5", "family": "standardDSv5Family",
                          "locations": ["eastus2euap"], "restrictions": [],
                          "capabilities": [{"name": "vCPUs", "value": "8"}, {"name": "MemoryGB", "value": "32"}]}]
            else:
                assert args[:2] == ["vmss", "list-instances"]
                assert arg("--name") in ("aks-prompool-38822163-vmss", "aks-default-28928250-vmss")
                value = []
            if "--query" in args:
                value = jmespath.search(arg("--query"), value)
            print(json.dumps(value))
            """
        ),
        encoding="utf-8",
    )
    fake.chmod(0o755)
    kube = tmp_path / "kubectl"
    kube.write_text(
        f"#!{sys.executable}\n" + textwrap.dedent(
            """
            import json
            import os
            import sys
            from pathlib import Path

            args = sys.argv[1:]
            assert args[:1] == ["--kubeconfig"]
            config = Path(args[1])
            assert config.read_text() == "private-test-credentials"
            assert config.stat().st_mode & 0o777 == 0o600
            assert args[2:6] == ["--context", "clustermesh-96", "--request-timeout=20s", "get"]
            assert args[6] in ("nodes", "pods", "events", "deployments,replicasets,daemonsets,statefulsets",
                               "pdb", "nodenetworkconfigs")
            assert args[-2:] == ["-o", "json"]
            assert not set(args) & {"apply", "patch", "delete", "cordon", "taint", "exec"}
            if os.environ["FAULT"] == "kube-forbidden":
                print("Forbidden: node state is not readable", file=sys.stderr)
                sys.exit(1)
            print(json.dumps({"kind": "List", "items": [
                {"metadata": {"name": "original-worker"}, "status": {"conditions": [
                    {"type": "Ready", "status": "Unknown"},
                    {"type": "VMEventScheduled", "status": "True", "reason": "Freeze"}
                ]}}
            ]}))
            """
        ), encoding="utf-8",
    )
    kube.chmod(0o755)
    private_root = tmp_path / "private"
    private_root.mkdir()
    calls_file = tmp_path / "calls.jsonl"
    environment = {
        **os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "RUN_ID": "78751-f36f3d5a", "CONFIRM_RESUME": "78751-f36f3d5a",
        "EXPECTED_SUBSCRIPTION_ID": SUBSCRIPTION, "EXPECTED_REGION": "eastus2euap",
        "EXPECTED_CLUSTER_COUNT": "100", "NATIVE_BUILD_ID": "79894",
        "NATIVE_INPUT_DIRECTORY": str(native), "ARTIFACT_STAGING_DIRECTORY": str(tmp_path / "artifacts"),
        "AGENT_TEMP_DIRECTORY": str(private_root),
        "CALLS": str(calls_file), "FAULT": fault, "FAMILY": str(family), "TOTAL": str(total),
        "COUNTER_TYPE": counter_type,
    }
    if fault == "scope":
        environment["CONFIRM_RESUME"] = "different"
    result = subprocess.run(
        ["bash", "-c", script], env=environment, capture_output=True, text=True, check=False, timeout=20,
    )
    success = fault in ("none", "absent", "managed-absent")
    assert (result.returncode == 0) is success, result.stderr
    directory = tmp_path / "artifacts" / "n100-unreachable-worker-recovery"
    if success:
        summary = json.loads((directory / "quota-observation.json").read_text(encoding="utf-8"))
        assert summary["observation_only"] and not summary["mutation_started"] and not summary["workloads_ready"]
        assert summary["worker_state_collected"] and not summary["resource_health_complete"]
        assert summary["resource_health_supported"] is False
        current = json.loads((directory / "current-nodes.json").read_text(encoding="utf-8"))
        assert current["items"][0]["status"]["conditions"][0]["status"] == "Unknown"
        health = json.loads((directory / "default-1-resource-health.json").read_text(encoding="utf-8"))
        assert health["supported"] is False and health["queried"] is False and health["evidence_build"] == 79941
        activity = json.loads((directory / "default-vmss-activity-log.json").read_text(encoding="utf-8"))
        assert activity[0]["status"] == "Failed" and activity[0]["properties"]["statusMessage"] == "captured provider error"
        aggregate = json.loads((directory / "default-vmss-instance-view.json").read_text(encoding="utf-8"))
        assert aggregate["virtualMachines"] == [{"code": "ProvisioningState/failed", "count": 1}]
        assert summary["headroom_for_restore"] is (min(family, total) >= 8)
        assert summary["headroom_for_restore_and_cni"] is (min(family, total) >= 24)
        assert summary["prom_instances"] == []
        modern = json.loads((directory / "supported-family-quota.json").read_text(encoding="utf-8"))
        assert modern[0]["remaining"] == 900
        sku = json.loads((directory / "supported-vm-sku.json").read_text(encoding="utf-8"))
        assert sku[0]["name"] == "Standard_D8s_v5" and sku[0]["restrictions"] == []
        if fault == "absent":
            assert json.loads((directory / "accidental-group.json").read_text(encoding="utf-8"))["proof"] \
                == "ResourceGroupNotFound"
        if fault == "managed-absent":
            for name in ("clustermesh-1", "clustermesh-2"):
                assert json.loads((directory / f"accidental-{name}-node-group.json").read_text(encoding="utf-8"))["absent"]
                assert not (directory / f"accidental-{name}-vmsses.json").exists()
    else:
        assert not (directory / "quota-observation.json").exists()
        if fault == "activity-forbidden":
            assert (directory / "current-nodes.json").is_file()
    assert not list(private_root.iterdir())
    if fault == "cluster-scope":
        calls = [json.loads(row) for row in calls_file.read_text(encoding="utf-8").splitlines()]
        assert not any(row[:2] == ["aks", "get-credentials"] for row in calls)
    if fault in ("scope", "checkpoint"):
        assert not calls_file.exists()


def test_quota_observer_has_no_helper_execution_or_retries():
    template = yaml.safe_load(STEP.read_text(encoding="utf-8"))
    operation, artifact = template["steps"]
    assert operation["retryCountOnTaskFailure"] == 0
    assert "--execute" not in operation["script"] and "python3" not in operation["script"]
    assert "az rest" not in operation["script"]
    assert artifact["task"] == "PublishPipelineArtifact@1" and "always()" in artifact["condition"]
