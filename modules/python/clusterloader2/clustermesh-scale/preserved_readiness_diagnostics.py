#!/usr/bin/env python3
"""Read-only host/Cilium diagnostics for the exact failed workload preflight."""

import argparse
import json
import re
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import preserved_worker_reconcile as workers
import stalled_retained_worker_recovery as stalled
import mock_cni_recovery as mocks


SUBSCRIPTION = "37deca37-c375-4a14-b90a-043849bd2bf1"
RUN_ID = "78751-f36f3d5a"
REGION = "eastus2euap"
SOURCE_BUILD = 80017
ROLES = {"mesh-2", "mesh-51", "mesh-66", "mesh-79", "mesh-89", "mesh-94"}
PREFIXES = (
    ("az", "account", "show"), ("az", "group", "show"), ("az", "aks", "list"),
    ("az", "aks", "get-credentials"), ("az", "aks", "nodepool", "list"),
    ("az", "aks", "operation", "show-latest"), ("az", "vmss", "list"),
    ("az", "vmss", "list-instances"), ("az", "vmss", "get-instance-view"),
)
VM_QUERY = "[].{id:id,instanceId:instanceId,vmId:vmId,computerName:osProfile.computerName,provisioningState:provisioningState,latestModelApplied:latestModelApplied}"
VIEW_QUERY = (
    "{statuses:statuses,vmAgent:vmAgent,maintenanceRedeployStatus:maintenanceRedeployStatus,"
    "extensions:extensions[].{name:name,statuses:statuses[].{code:code,displayStatus:displayStatus,time:time}}}"
)


def require(condition, message):
    if not condition:
        raise workers.ReconcileError(message)


def selected_roles(source):
    overlay = source.get("live_overlay_recovery") or {}
    proof = (overlay.get("initial") or {}).get("proof") or {}
    drift = proof.get("drift") or []
    roles = {row.get("role") for row in drift}
    require(source.get("resource_group") == RUN_ID and source.get("healthy") is False
            and overlay.get("fleet_repair") is None
            and proof.get("cluster_count") == 100
            and roles == {"mesh-51", "mesh-66", "mesh-79", "mesh-89"}
            and all("Cilium agent Pod is not Running/Ready" in str(row.get("command_error")) for row in drift),
            "Only the exact unready-Cilium preflight from build 80017 is supported")
    roles.update(row.get("role") for row in source.get("initial_failed_pools") or [])
    require(roles == ROLES, "Diagnostic role set differs from the failed workload evidence")
    return sorted(roles, key=lambda role: int(role.split("-")[1]))


class Reader:
    def __init__(self, directory, runner):
        self.directory = directory
        self.runner = runner
        self.reads = []

    def capture(self, name, command, *, kubeconfig=None, timeout=60, json_output=True):
        if command[0] == "az":
            require(any(tuple(command[:len(prefix)]) == prefix for prefix in PREFIXES),
                    "Diagnostic command is not an allowed Azure read")
            command = [*command, "--only-show-errors"]
            if command[1:3] != ["account", "show"]:
                command += ["--subscription", SUBSCRIPTION]
        else:
            require(command[0] == "kubectl" and kubeconfig, "Private Kubernetes context is required")
            require(command[1] == "get" or (
                command[1] == "exec" and command[-5:] == ["--", "cilium-dbg", "status", "-o", "json"]
            ), "Diagnostic command is not an allowed Kubernetes read")
            command = ["kubectl", "--kubeconfig", str(kubeconfig), "--request-timeout=30s", *command[1:]]
        record = {"name": name, "success": False, "command": command}
        self.reads.append(record)
        try:
            output = self.runner(command, timeout)
            payload = json.loads(output) if json_output else {"output": output}
            safe = stalled.safe_diagnostics(payload)
            mocks.write_json_atomic(str(self.directory / f"{name}.json"), safe)
            record["success"] = True
            return payload
        except (workers.ReconcileError, json.JSONDecodeError) as error:
            record["error"] = str(error)
            mocks.write_json_atomic(str(self.directory / f"{name}-error.json"), record)
            return None


def collect_role(cluster, directory, runner):
    role = cluster["tags"]["role"]
    directory = directory / role
    directory.mkdir()
    reader = Reader(directory, runner)
    report = {"role": role, "cluster_id": cluster["id"], "read_only": True, "reads": reader.reads}
    try:
        with tempfile.TemporaryDirectory(prefix=f"readiness-{role}-") as temporary:
            config = Path(temporary) / "cluster.config"
            credentials = reader.capture("credential-read", [
                "az", "aks", "get-credentials", "--resource-group", RUN_ID, "--name", cluster["name"],
                "--file", str(config), "--context", cluster["name"],
            ], timeout=90, json_output=False)
            if credentials is None or not config.is_file():
                report["error"] = "Private credentials could not be read"
                return report
            nodes = reader.capture("nodes", ["kubectl", "get", "nodes", "-o", "json"], kubeconfig=config)
            pods = reader.capture("pods", ["kubectl", "get", "pods", "-A", "-o", "json"], kubeconfig=config)
            for name, resource, scope in (
                ("cilium-daemonset", "daemonset", ["cilium", "-n", "kube-system"]),
                ("events", "events", ["-A"]),
                ("nnc", "nodenetworkconfigs", ["-n", "kube-system"]),
                ("pdbs", "pdb", ["-A"]),
            ):
                reader.capture(name, ["kubectl", "get", resource, *scope, "-o", "json"], kubeconfig=config)
            reader.capture("pools", ["az", "aks", "nodepool", "list",
                                     "--resource-group", RUN_ID, "--cluster-name", cluster["name"], "-o", "json"])
            reader.capture("default-operation", [
                "az", "aks", "operation", "show-latest", "--resource-group", RUN_ID,
                "--name", cluster["name"], "--nodepool-name", "default", "-o", "json",
            ])
            node_group = cluster["nodeResourceGroup"]
            ownership = reader.capture("node-resource-group", [
                "az", "group", "show", "--name", node_group, "-o", "json",
            ])
            require(ownership and str(ownership.get("managedBy", "")).lower() == cluster["id"].lower()
                    and str(ownership.get("location", "")).lower() == REGION,
                    "Diagnostic node resource group is not owned by the selected AKS cluster")
            reader.capture("vmsses", ["az", "vmss", "list", "--resource-group", node_group,
                                     "--query", "[].{id:id,name:name,provisioningState:provisioningState,sku:sku,tags:tags}", "-o", "json"])
            report["unready_cilium"] = []
            targets = set()
            for pod in (pods or {}).get("items") or []:
                metadata, spec = pod.get("metadata") or {}, pod.get("spec") or {}
                if metadata.get("namespace") != "kube-system" or metadata.get("labels", {}).get("k8s-app") != "cilium":
                    continue
                name = metadata.get("name", "")
                require(re.fullmatch(r"[a-z0-9][a-z0-9.-]*", name), "Malformed Cilium Pod name")
                ready = stalled.base.pod_ready(pod) and not metadata.get("deletionTimestamp") and any(
                    row.get("type") == "Ready" and row.get("status") == "True"
                    for row in pod.get("status", {}).get("conditions") or []
                )
                if not ready:
                    targets.add(spec.get("nodeName"))
                    report["unready_cilium"].append({
                        "pod": name, "uid": metadata.get("uid"), "node": spec.get("nodeName"), "status": pod.get("status"),
                    })
                    reader.capture(f"{name}-status", [
                        "kubectl", "exec", "-n", "kube-system", name, "-c", "cilium-agent",
                        "--", "cilium-dbg", "status", "-o", "json",
                    ], kubeconfig=config, timeout=45)
            vmsses = set()
            for node in (nodes or {}).get("items") or []:
                identity = workers.provider_identity(node)
                if identity is None:
                    continue
                vmss, instance = identity
                name = node["metadata"]["name"]
                require(str(node.get("spec", {}).get("providerID", "")).lower().startswith(
                    f"azure:///subscriptions/{SUBSCRIPTION}/resourcegroups/{node_group.lower()}/providers/microsoft.compute/"
                ), "Diagnostic Node provider identity escaped the selected node resource group")
                vmsses.add(vmss)
                if name in targets or not workers.node_is_ready(node) or role in ("mesh-2", "mesh-94"):
                    reader.capture(f"{name}-instance-view", [
                        "az", "vmss", "get-instance-view", "--resource-group", node_group,
                        "--name", vmss, "--instance-id", instance, "--query", VIEW_QUERY, "-o", "json",
                    ])
                    reader.capture(f"{name}-metrics", [
                        "kubectl", "get", f"--raw=/apis/metrics.k8s.io/v1beta1/nodes/{name}",
                    ], kubeconfig=config)
            for vmss in sorted(vmsses):
                reader.capture(f"{vmss}-instances", [
                    "az", "vmss", "list-instances", "--resource-group", node_group,
                    "--name", vmss, "--query", VM_QUERY, "-o", "json",
                ])
            return report
    finally:
        mocks.write_json_atomic(str(directory / "summary.json"), stalled.safe_diagnostics(report))


def main(argv=None, runner=workers.run_command):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--source-build-id", type=int, required=True)
    parser.add_argument("--output-directory", required=True)
    parser.add_argument("--expected-tfvars-sha", required=True)
    args = parser.parse_args(argv)
    require(args.source_build_id == SOURCE_BUILD, "Unexpected failed-workload source build")
    roles = selected_roles(json.loads(Path(args.source).read_text(encoding="utf-8")))
    directory = Path(args.output_directory)
    require(not directory.exists(), "Diagnostic output must be new")
    directory.mkdir(parents=True)
    reader = Reader(directory, runner)
    account = reader.capture("account", ["az", "account", "show", "--query", "{id:id}", "-o", "json"])
    require(account and str(account.get("id", "")).lower() == SUBSCRIPTION, "Unexpected Azure subscription")
    group = reader.capture("resource-group", ["az", "group", "show", "--name", RUN_ID, "-o", "json"])
    require(group and str(group.get("location", "")).lower() == REGION
            and group.get("tags", {}).get("run_id") == RUN_ID
            and group["tags"].get("clustermesh_debug_preserved") == "true"
            and group["tags"].get("scenario") == "perf-eval-clustermesh-scale"
            and group["tags"].get("clustermesh_debug_expected_clusters") == "100"
            and group["tags"].get("clustermesh_debug_tfvars_sha256") == args.expected_tfvars_sha,
            "Preserved resource-group identity or input hash changed")
    clusters = reader.capture("clusters", ["az", "aks", "list", "--resource-group", RUN_ID,
                                         "--query", "[].{id:id,name:name,nodeResourceGroup:nodeResourceGroup,location:location,tags:tags,provisioningState:provisioningState}", "-o", "json"])
    require(isinstance(clusters, list) and len(clusters) == 100
            and {row.get("tags", {}).get("role") for row in clusters} == {f"mesh-{index}" for index in range(1, 101)},
            "Full preserved cluster inventory is not exact")
    selected = [row for row in clusters if row["tags"]["role"] in roles]
    for cluster in selected:
        name = "clustermesh-" + cluster["tags"]["role"].split("-")[1]
        expected_id = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{RUN_ID}/providers/Microsoft.ContainerService/managedClusters/{name}"
        require(cluster["name"] == name and cluster["id"].lower() == expected_id.lower()
                and cluster["location"].lower() == REGION and cluster["tags"].get("run_id") == RUN_ID
                and cluster.get("nodeResourceGroup"), "Diagnostic target is outside the preserved scope")
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(lambda cluster: collect_role(cluster, directory, runner), selected))
    summary = {"source_build_id": SOURCE_BUILD, "read_only": True, "resource_mutations": 0,
               "health_claimed": False, "roles": roles, "results": results}
    mocks.write_json_atomic(str(directory / "summary.json"), stalled.safe_diagnostics(summary))
    print(json.dumps({"diagnostics_collected": roles, "read_only": True, "health_claimed": False}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
