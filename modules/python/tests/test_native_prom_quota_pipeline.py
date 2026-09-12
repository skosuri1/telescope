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
                    ]
                ]
                if os.environ["COUNTER_TYPE"] == "string":
                    for row in value:
                        row["currentValue"], row["limit"] = str(row["currentValue"]), str(row["limit"])
                if fault == "bad-counter":
                    value[0]["currentValue"] = "100.5"
            elif args[:2] == ["group", "show"]:
                name = arg("--name")
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
                         "name": "clustermesh-96", "provisioningState": "Succeeded"}
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
            else:
                assert args[:2] == ["vmss", "list-instances"]
                assert arg("--name") == "aks-prompool-38822163-vmss"
                value = []
            if "--query" in args:
                value = jmespath.search(arg("--query"), value)
            print(json.dumps(value))
            """
        ),
        encoding="utf-8",
    )
    fake.chmod(0o755)
    calls_file = tmp_path / "calls.jsonl"
    environment = {
        **os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "RUN_ID": "78751-f36f3d5a", "CONFIRM_RESUME": "78751-f36f3d5a",
        "EXPECTED_SUBSCRIPTION_ID": SUBSCRIPTION, "EXPECTED_REGION": "eastus2euap",
        "EXPECTED_CLUSTER_COUNT": "100", "NATIVE_BUILD_ID": "79894",
        "NATIVE_INPUT_DIRECTORY": str(native), "ARTIFACT_STAGING_DIRECTORY": str(tmp_path / "artifacts"),
        "CALLS": str(calls_file), "FAULT": fault, "FAMILY": str(family), "TOTAL": str(total),
        "COUNTER_TYPE": counter_type,
    }
    if fault == "scope":
        environment["CONFIRM_RESUME"] = "different"
    result = subprocess.run(
        ["bash", "-c", script], env=environment, capture_output=True, text=True, check=False, timeout=20,
    )
    success = fault in ("none", "absent")
    assert (result.returncode == 0) is success, result.stderr
    directory = tmp_path / "artifacts" / "n100-unreachable-worker-recovery"
    if success:
        summary = json.loads((directory / "quota-observation.json").read_text(encoding="utf-8"))
        assert summary["observation_only"] and not summary["mutation_started"] and not summary["workloads_ready"]
        assert summary["headroom_for_restore"] is (min(family, total) >= 8)
        assert summary["headroom_for_restore_and_cni"] is (min(family, total) >= 24)
        assert summary["prom_instances"] == []
        if fault == "absent":
            assert json.loads((directory / "accidental-group.json").read_text(encoding="utf-8"))["proof"] \
                == "ResourceGroupNotFound"
    else:
        assert not (directory / "quota-observation.json").exists()
    if fault in ("scope", "checkpoint"):
        assert not calls_file.exists()


def test_quota_observer_has_no_helper_execution_or_retries():
    template = yaml.safe_load(STEP.read_text(encoding="utf-8"))
    operation, artifact = template["steps"]
    assert operation["retryCountOnTaskFailure"] == 0
    assert "--execute" not in operation["script"] and "python3" not in operation["script"]
    assert artifact["task"] == "PublishPipelineArtifact@1" and "always()" in artifact["condition"]
