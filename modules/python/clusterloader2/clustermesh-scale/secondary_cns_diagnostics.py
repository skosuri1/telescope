#!/usr/bin/env python3
"""Read-only CNS comparison after the exact partially qualified build 80046."""

import argparse
import copy
import json
import re
import signal
import tempfile
from pathlib import Path

import mock_cni_recovery as mocks
import preserved_readiness_diagnostics as readiness
import secondary_capacity_qualification as qualification
import secondary_capacity_recovery as capacity
import stalled_retained_worker_recovery as stalled
import preserved_worker_reconcile as workers


SOURCE_BUILD = 80046
SOURCE_SHA = "2751dcc9d6c5757ec258eb51629bb608091f66e80f3824f62489a85ba35c2e48"
TARGETS = {
    "mesh-2": {
        "aks-default-27279174-vmss000002": "2681d531-19a7-4ab9-aabe-77f96b0c7365",
        "aks-default-27279174-vmss000003": "83d7f25b-b9f6-47f7-9fdd-df7ceb7614f2",
    },
    "mesh-89": {
        "aks-promv5-93638730-vmss000000": "3899c747-1c10-4e47-ba43-878d07cf7f58",
    },
}
CNS_COMMAND = (
    "/usr/local/bin/azure-cns", "--debugcmd", "get", "--debugarg", "all", "--log-target", "stdout",
)
SENSITIVE_KEY = re.compile(r"secret|password|authorization|token|privatekey|certificate|accesskey|sharedkey", re.I)
AUTH_PATH = re.compile(r"(authenticationToken/).*?(/api-version)", re.I)
BEARER = re.compile(r"\b(Bearer|Basic)\s+[A-Za-z0-9._~+/-]+=*", re.I)
AUTH_FIELD = re.compile(
    r'((?:authorization|authToken|accessToken|clientSecret|password|access_token|sig)["\s]*[:=]\s*)'
    r'(?:"(?:\\.|[^"])*"|[^\s,}&]+)', re.I,
)
require = capacity.require


def redact(value):
    if isinstance(value, dict):
        return {key: "<redacted>" if SENSITIVE_KEY.search(key) else redact(item)
                for key, item in value.items()}
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, str):
        return AUTH_FIELD.sub(
            r"\1<redacted>", BEARER.sub(r"\1 <redacted>", AUTH_PATH.sub(r"\1<redacted>\2", value)),
        )
    return value


def validate_source(root):
    hashes = qualification.hash_tree(root)
    require(hashes.get("qualification.json") == SOURCE_SHA,
            "Only the exact build 80046 qualification receipt is accepted")
    receipt = qualification.read_json(root / "qualification.json")
    require(receipt.get("execute") is True and receipt.get("success") is False
            and receipt.get("capacity_source_build_id") == 80039
            and receipt.get("error") == "mesh-89: actual NNC version/IP growth exceeded the bounded wait"
            and receipt.get("cleanup_errors") == []
            and receipt.get("workloads_ready") is False,
            "Source is not the diagnosed partial qualification")
    for role, count in (("mesh-51", 61), ("mesh-66", 48), ("mesh-79", 40), ("mesh-89", 15)):
        row = receipt["per_role"][role]
        require(not row["probe_cleanup_pending"] and len(row["probe_receipts"]) == count
                and all(probe.get("create_accepted") is True and probe.get("delete_accepted") is True
                        and probe.get("absence_observed") is True for probe in row["probe_receipts"].values())
                and row["capacity_qualified"] is (role != "mesh-89"),
                f"{role}: source qualification/cleanup accounting changed")
    return hashes, receipt


class Reader:
    """Exact read/debug-client whitelist; no CNS daemon startup or host writes."""

    def __init__(self, directory, runner):
        self.directory, self.runner = directory, runner
        self.reads = []

    def capture(self, name, command, *, config=None, text=False, timeout=60):
        command = list(command)
        if command[0] == "az":
            require(tuple(command[:3]) in (
                ("az", "account", "show"), ("az", "group", "show"), ("az", "aks", "list"),
                ("az", "aks", "get-credentials"), ("az", "vmss", "get-instance-view"),
            ) or tuple(command[:4]) in (
                ("az", "aks", "nodepool", "list"), ("az", "aks", "operation", "show-latest"),
            ), "CNS diagnosis rejected an Azure mutation")
            command += ["--only-show-errors"]
            if command[1:3] != ["account", "show"]:
                command += ["--subscription", capacity.SUBSCRIPTION]
        else:
            require(command[0] == "kubectl" and config, "Private Kubernetes context is required")
            allowed = command[1] == "logs"
            if command[1] == "get":
                allowed = command[2] in {
                    "nodes", "pods", "pod", "nodenetworkconfigs", "events", "pdb",
                    "deployments,replicasets,daemonsets", "configmaps", "configmap",
                } or bool(re.fullmatch(
                    r"--raw=(/apis/metrics.k8s.io/v1beta1/nodes/[a-z0-9.-]+|"
                    r"/api/v1/namespaces/kube-system/pods/[a-z0-9.-]+:10092/proxy/metrics)",
                    command[2],
                ))
            if command[1] == "exec":
                allowed = ("--" in command and tuple(command[command.index("--") + 1:]) == CNS_COMMAND
                           and "-c" in command and command[command.index("-c") + 1] == "cns-container")
            require(allowed, "CNS diagnosis rejected a Kubernetes mutation or arbitrary exec")
            command = ["kubectl", "--kubeconfig", str(config), "--request-timeout=30s", *command[1:]]
        record = {"name": name, "command": command, "success": False}
        self.reads.append(record)
        try:
            output = self.runner(command, timeout)
            value = {"output": output} if text else json.loads(output)
            mocks.write_json_atomic(self.directory / f"{name}.json", redact(stalled.safe_diagnostics(value)))
            record["success"] = True
            return value
        except (workers.ReconcileError, json.JSONDecodeError) as error:
            record["error"] = redact(str(error))
            mocks.write_json_atomic(self.directory / f"{name}-error.json", record)
            print(json.dumps({"read_error": name, "error": record["error"]}), flush=True)
            return None


def pod_identity(pod):
    metadata = pod.get("metadata") or {}
    return {
        "uid": metadata.get("uid"), "node": pod.get("spec", {}).get("nodeName"),
        "containers": [(row.get("name"), row.get("containerID"), row.get("restartCount"))
                       for row in pod.get("status", {}).get("containerStatuses") or []],
    }


def collect_role(cluster, root, runner):
    role = cluster["tags"]["role"]
    directory = root / role
    directory.mkdir()
    reader = Reader(directory, runner)
    report = {"role": role, "read_only": True, "reads": reader.reads, "targets": {}}
    try:
        with tempfile.TemporaryDirectory(prefix=f"cns-readonly-{role}-") as private:
            config = Path(private) / "cluster.config"
            reader.capture("credentials", [
                "az", "aks", "get-credentials", "--resource-group", capacity.RESOURCE_GROUP,
                "--name", cluster["name"], "--file", str(config), "--context", cluster["name"],
            ], text=True, timeout=120)
            require(config.is_file(), f"{role}: private credentials are unavailable")
            config.chmod(0o600)
            nodes = reader.capture("nodes", ["kubectl", "get", "nodes", "-o", "json"], config=config)
            pods = reader.capture("pods", ["kubectl", "get", "pods", "-A", "-o", "json"], config=config)
            require(nodes is not None and pods is not None, f"{role}: Node/Pod identity reads failed")
            for name, resource, scope in (
                ("nnc", "nodenetworkconfigs", ["-n", "kube-system"]),
                ("events", "events", ["-A"]), ("controllers", "deployments,replicasets,daemonsets", ["-A"]),
                ("pdbs", "pdb", ["-A"]), ("journals", "configmaps", ["-n", "kube-system"]),
            ):
                reader.capture(name, ["kubectl", "get", resource, *scope, "-o", "json"], config=config)
            for suffix, prefix in (("pools", ["aks", "nodepool", "list"]),
                                   ("default-operation", ["aks", "operation", "show-latest"])):
                args = ["--cluster-name", cluster["name"]] if suffix == "pools" else [
                    "--name", cluster["name"], "--nodepool-name", "default",
                ]
                reader.capture(suffix, ["az", *prefix, "--resource-group", capacity.RESOURCE_GROUP, *args, "-o", "json"])
            for node_name, expected_uid in TARGETS[role].items():
                matches = [node for node in nodes["items"] if node["metadata"]["name"] == node_name]
                require(len(matches) == 1 and matches[0]["metadata"]["uid"] == expected_uid,
                        f"{role}/{node_name}: diagnostic Node identity changed")
                node = matches[0]
                provider = str(node.get("spec", {}).get("providerID") or "").lower()
                require(provider.startswith(
                    f"azure:///subscriptions/{capacity.SUBSCRIPTION}/resourcegroups/"
                    f"{cluster['nodeResourceGroup'].lower()}/providers/microsoft.compute/virtualmachinescalesets/"
                ), f"{role}/{node_name}: provider escaped the preserved cluster")
                candidates = [pod for pod in pods["items"]
                              if pod.get("spec", {}).get("nodeName") == node_name
                              and pod["metadata"].get("namespace") == "kube-system"
                              and pod["metadata"].get("labels", {}).get("k8s-app") == "azure-cns"]
                require(len(candidates) == 1, f"{role}/{node_name}: CNS Pod is ambiguous")
                cns = candidates[0]
                containers = [row for row in cns["spec"]["containers"] if row["name"] == "cns-container"]
                require(len(containers) == 1, f"{role}/{node_name}: CNS container is ambiguous")
                owners = cns["metadata"].get("ownerReferences") or []
                require(len(owners) == 1 and owners[0].get("kind") == "DaemonSet"
                        and owners[0].get("name") == "azure-cns" and owners[0].get("controller") is True,
                        f"{role}/{node_name}: CNS ownership is not exact")
                name = cns["metadata"]["name"]
                report["targets"][node_name] = {"node_uid": expected_uid, "cns": pod_identity(cns)}
                print(json.dumps({"collecting": role, "node": node_name, "cns": name}), flush=True)
                reader.capture(f"{node_name}-cns-logs", [
                    "kubectl", "logs", "-n", "kube-system", name, "-c", "cns-container",
                    "--since=3h", "--tail=2500", "--limit-bytes=4194304", "--timestamps",
                ], config=config, text=True)
                if (stalled.base.pod_ready(cns)
                        and containers[0]["image"] == "mcr.microsoft.com/containernetworking/v2/azure-cns:v1.8.12"):
                    reader.capture(f"{node_name}-ipam", [
                        "kubectl", "exec", "-n", "kube-system", name, "-c", "cns-container",
                        "--", *CNS_COMMAND,
                    ], config=config, text=True)
                else:
                    report["targets"][node_name]["debug_skipped"] = "CNS is unready or not the reviewed v1.8.12 image"
                for volume in cns["spec"].get("volumes") or []:
                    config_name = (volume.get("configMap") or {}).get("name")
                    if config_name:
                        reader.capture(f"{node_name}-config-{config_name}", [
                            "kubectl", "get", "configmap", config_name, "-n", "kube-system", "-o", "json",
                        ], config=config)
                reader.capture(f"{node_name}-cns-metrics", [
                    "kubectl", "get",
                    f"--raw=/api/v1/namespaces/kube-system/pods/{name}:10092/proxy/metrics",
                ], config=config, text=True)
                reader.capture(f"{node_name}-node-metrics", [
                    "kubectl", "get", f"--raw=/apis/metrics.k8s.io/v1beta1/nodes/{node_name}",
                ], config=config)
                identity = workers.provider_identity(node)
                require(identity is not None, f"{role}/{node_name}: VMSS identity is malformed")
                vmss, instance = identity
                reader.capture(f"{node_name}-instance-view", [
                    "az", "vmss", "get-instance-view", "--resource-group", cluster["nodeResourceGroup"],
                    "--name", vmss, "--instance-id", instance, "--query", readiness.VIEW_QUERY, "-o", "json",
                ])
                current = reader.capture(f"{node_name}-cns-after", [
                    "kubectl", "get", "pod", name, "-n", "kube-system", "-o", "json",
                ], config=config)
                require(current is not None and pod_identity(current) == pod_identity(cns),
                        f"{role}/{node_name}: CNS changed identity or restarted during observation")
            reader.capture("nnc-after", ["kubectl", "get", "nodenetworkconfigs", "-n", "kube-system", "-o", "json"], config=config)
            reader.capture("pods-after", ["kubectl", "get", "pods", "-A", "-o", "json"], config=config)
    except workers.ReconcileError as error:
        report["error"] = redact(str(error))
        raise
    finally:
        mocks.write_json_atomic(directory / "summary.json", redact(report))
    return report


def main(argv=None, runner=workers.run_command):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-directory", required=True)
    parser.add_argument("--source-build-id", required=True, type=int)
    parser.add_argument("--expected-tfvars-sha", required=True)
    parser.add_argument("--output-directory", required=True)
    args = parser.parse_args(argv)
    require(args.source_build_id == SOURCE_BUILD and args.expected_tfvars_sha == capacity.TFVARS_SHA,
            "CNS diagnosis source/tfvars scope mismatch")
    source = Path(args.source_directory).resolve()
    output = Path(args.output_directory).resolve()
    require(source.is_dir() and not Path(args.source_directory).is_symlink()
            and not output.exists() and source != output and source not in output.parents
            and output not in source.parents, "CNS diagnostic output must be new and disjoint")
    hashes, receipt = validate_source(source)
    output.mkdir(parents=True)
    reader = Reader(output, runner)
    summary = {"source_build_id": SOURCE_BUILD, "source_receipt_sha256": SOURCE_SHA,
               "source_input_hashes": hashes, "read_only": True, "resource_mutations": 0,
               "health_claimed": False, "reads": reader.reads, "results": [],
               "preserved_qualified_roles": ["mesh-51", "mesh-66", "mesh-79"],
               "source_cleanup_errors": copy.deepcopy(receipt["cleanup_errors"])}
    try:
        account = reader.capture("account", ["az", "account", "show", "--query", "{id:id}", "-o", "json"])
        require(account and account.get("id", "").lower() == capacity.SUBSCRIPTION, "Unexpected subscription")
        group = reader.capture("resource-group", ["az", "group", "show", "--name", capacity.RESOURCE_GROUP, "-o", "json"])
        require(group and group.get("location", "").lower() == capacity.REGION
                and group.get("tags", {}).get("clustermesh_debug_tfvars_sha256") == capacity.TFVARS_SHA
                and group["tags"].get("clustermesh_debug_preserved") == "true"
                and group["tags"].get("scenario") == "perf-eval-clustermesh-scale"
                and group["tags"].get("clustermesh_debug_expected_clusters") == "100"
                and group["tags"].get("run_id") == capacity.RESOURCE_GROUP, "Preserved group ownership changed")
        clusters = reader.capture("clusters", [
            "az", "aks", "list", "--resource-group", capacity.RESOURCE_GROUP,
            "--query", "[].{id:id,name:name,nodeResourceGroup:nodeResourceGroup,location:location,tags:tags}", "-o", "json",
        ])
        require(isinstance(clusters, list) and len(clusters) == 100
                and {row.get("tags", {}).get("role") for row in clusters}
                == {f"mesh-{index}" for index in range(1, 101)}, "Preserved cluster inventory changed")
        for role in TARGETS:
            matches = [row for row in clusters if row.get("tags", {}).get("role") == role]
            require(len(matches) == 1, f"{role}: cluster is ambiguous")
            cluster = matches[0]
            name = "clustermesh-" + role.split("-")[1]
            require(cluster["name"] == name and cluster.get("location", "").lower() == capacity.REGION
                    and capacity.resource_equal(cluster.get("id"),
                        f"/subscriptions/{capacity.SUBSCRIPTION}/resourceGroups/{capacity.RESOURCE_GROUP}"
                        f"/providers/Microsoft.ContainerService/managedClusters/{name}"),
                    f"{role}: cluster identity escaped scope")
            summary["results"].append(collect_role(cluster, output, runner))
        require(qualification.hash_tree(source) == hashes, "Qualification source changed during diagnosis")
        summary["diagnostics_collected"] = True
        print(json.dumps({"read_only": True, "roles": list(TARGETS), "health_claimed": False}), flush=True)
        return 0
    except workers.ReconcileError as error:
        summary["error"] = redact(str(error))
        raise
    finally:
        mocks.write_json_atomic(output / "summary.json", redact(summary))


if __name__ == "__main__":
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt("Read-only CNS diagnosis interrupted")

    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
