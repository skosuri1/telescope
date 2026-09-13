#!/usr/bin/env python3
"""Create only the four source-bound secondary DSv5 capacity pools."""

# pylint: disable=protected-access,too-many-lines,too-many-branches,too-many-statements

from __future__ import annotations

import argparse
import copy
import hashlib
import ipaddress
import json
import re
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import cni_worker_maintenance as maintenance
import mock_cni_recovery as mocks
import preserved_aks_arm_reconcile as arm
import preserved_worker_reconcile as workers
import prepared_worker_retirement as prepared
import modern_prom_recovery as modern
import stalled_retained_worker_recovery as stalled


RESOURCE_GROUP = "78751-f36f3d5a"
SUBSCRIPTION = "37deca37-c375-4a14-b90a-043849bd2bf1"
REGION = "eastus2euap"
TFVARS_SHA = "e99903cc5181367e6e2e08d0cb7d806ddd0ceb81aeacae505c55a5c52bf31160"
DIAGNOSTIC_BUILD = 80022
DIAGNOSED_BUILD = 80017
PATCH = "1.35.7"
VM_SIZE = "Standard_D8s_v5"
QUOTA_FAMILY = "standardDSv5Family"
TOTAL_CORES = 56
ROLES = ("mesh-51", "mesh-66", "mesh-79", "mesh-89")
OWNER = "secondary-capacity-recovery"
JOURNAL_PREFIX = "secondary-capacity-recovery"
PROVIDER_SUBMIT_SECONDS = 180
SKU_READ_SECONDS = 180
READ_SECONDS = 60
POLL_SECONDS = 10
FINAL_RESERVE_SECONDS = 90
VMSS_QUERY = "[].{id:id,name:name,location:location,orchestrationMode:orchestrationMode,provisioningState:provisioningState,sku:sku,tags:tags}"
VM_QUERY = "[].{id:id,instanceId:instanceId,vmId:vmId,computerName:osProfile.computerName,provisioningState:provisioningState,latestModelApplied:latestModelApplied}"
VIEW_QUERY = "{statuses:statuses,vmAgent:vmAgent,maintenanceRedeployStatus:maintenanceRedeployStatus,extensions:extensions[].{name:name,statuses:statuses[].{code:code,displayStatus:displayStatus,time:time}}}"
VMSS_MODEL_QUERY = (
    "{id:id,osDisk:virtualMachineProfile.storageProfile.osDisk."
    "{osType:osType,diskSizeGb:diskSizeGb,diskSizeGB:diskSizeGB,"
    "managedDisk:managedDisk.{storageAccountType:storageAccountType},"
    "diffDiskOption:diffDiskSettings.option},"
    "imageReference:virtualMachineProfile.storageProfile.imageReference}"
)
TRANSIENT_READ_RE = re.compile(
    r"AnotherOperationInProgress|ResourceNotFinalState|TooManyRequests|\b429\b|"
    r"temporar|timeout|timed out|connection reset|service unavailable",
    re.IGNORECASE,
)
TERMINAL_CODES = {
    "mesh-51": "ProvisioningState/failed/OSProvisioningClientError",
    "mesh-66": "ProvisioningState/failed/OSProvisioningInternalError",
    "mesh-79": "ProvisioningState/failed/OSProvisioningClientError",
    "mesh-89": "ProvisioningState/failed/OSProvisioningInternalError",
}
ROLE_SETTINGS = {
    "mesh-51": {
        "cluster": "clustermesh-51", "source_pool": "default", "pool": "cniv5",
        "count": 2, "mode": "System", "max_pods": 110,
        "failed_node": "aks-default-37313277-vmss000000",
        "failed_uid": "b46adff9-4d4f-45e5-87c5-b1241df1c372",
        "mock_placements": (61, 39), "terminating_mocks": 61,
    },
    "mesh-66": {
        "cluster": "clustermesh-66", "source_pool": "default", "pool": "cniv5",
        "count": 2, "mode": "System", "max_pods": 110,
        "failed_node": "aks-default-42633075-vmss000001",
        "failed_uid": "5cca193f-ea99-4c6c-8f32-63b0aefa3e6b",
        "mock_placements": (52, 48), "terminating_mocks": 48,
    },
    "mesh-79": {
        "cluster": "clustermesh-79", "source_pool": "default", "pool": "cniv5",
        "count": 2, "mode": "System", "max_pods": 110,
        "failed_node": "aks-default-23134330-vmss000001",
        "failed_uid": "dfc1de6c-ea9d-4e3c-a2b8-da8b68d19a2f",
        "mock_placements": (46, 40, 14), "terminating_mocks": 40,
    },
    "mesh-89": {
        "cluster": "clustermesh-89", "source_pool": "prompool", "pool": "promv5",
        "count": 1, "mode": "User", "max_pods": 250,
        "failed_node": "aks-prompool-25156573-vmss000000",
        "failed_uid": "94f92ed9-1793-455b-bba3-c6d721a3797c",
        "mock_placements": (51, 25, 24), "terminating_mocks": 0,
    },
}
EXPECTED_ERRORS = (
    workers.ReconcileError, OSError, ValueError, TypeError, KeyError, json.JSONDecodeError,
)


def require(condition, message):
    if not condition:
        raise workers.ReconcileError(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value):
    encoded = value if isinstance(value, bytes) else canonical(value).encode()
    return hashlib.sha256(encoded).hexdigest()


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def read_json(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"{path}: duplicate JSON key {key}")
            result[key] = value
        return result

    content = Path(path).read_bytes()
    require(0 < len(content) <= 32 * 1024 * 1024, f"{path}: JSON file size is invalid")
    return json.loads(content, object_pairs_hook=unique)


def hash_tree(directory):
    root = Path(directory).resolve()
    require(root.is_dir() and not Path(directory).is_symlink(),
            "Source diagnostics directory is missing or symlinked")
    result = {}
    for path in sorted(root.rglob("*")):
        require(not path.is_symlink(), "Source diagnostics tree contains a symlink")
        if path.is_file():
            require(path.stat().st_size <= 32 * 1024 * 1024,
                    "Source diagnostics file exceeds the 32MiB bound")
            result[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
        else:
            require(path.is_dir(), "Source diagnostics tree contains a nonregular entry")
    require(result, "Source diagnostics tree is empty")
    return result


def object_uid(row):
    value = str((row.get("metadata") or {}).get("uid") or "")
    require(maintenance.UUID_RE.fullmatch(value), "Kubernetes object UID is missing or malformed")
    return value


def resource_equal(left, right):
    return isinstance(left, str) and isinstance(right, str) and left.rstrip("/").lower() == right.rstrip("/").lower()


def node_contract(node):
    metadata = node.get("metadata") or {}
    spec = copy.deepcopy(node.get("spec") or {})
    provider = spec.get("providerID")
    if isinstance(provider, str):
        spec["providerID"] = provider.lower()
    spec["taints"] = [
        row for row in spec.get("taints") or []
        if row.get("key") not in {
            "node.kubernetes.io/unreachable", "node.kubernetes.io/not-ready",
            "node.cilium.io/agent-not-ready", "node.cloudprovider.kubernetes.io/uninitialized",
        }
    ]
    return {
        "uid": object_uid(node), "labels": copy.deepcopy(metadata.get("labels") or {}),
        "spec": spec,
        "reported_node_info": {key: (node.get("status", {}).get("nodeInfo") or {}).get(key)
                               for key in ("bootID", "kubeletVersion", "operatingSystem", "osImage")},
    }


def pod_contract(pod):
    metadata = pod.get("metadata") or {}
    return {
        "uid": object_uid(pod), "namespace": metadata.get("namespace"),
        "name": metadata.get("name"), "spec_sha256": digest(pod.get("spec") or {}),
        "owners": copy.deepcopy(metadata.get("ownerReferences") or []),
        "deleting": bool(metadata.get("deletionTimestamp")),
    }


def vmss_contract(row):
    return stalled.arm_canonical({key: copy.deepcopy(row.get(key)) for key in ("id", "name", "sku", "tags")})


def pool_contract(row):
    return arm.pool_configuration(row)


def network_record(row, *, allow_pending=False):
    metadata = row.get("metadata") or {}
    status = row.get("status") or {}
    containers = status.get("networkContainers")
    if allow_pending and (not containers or not status.get("assignedIPCount")):
        return None
    require(isinstance(containers, list) and len(containers) == 1,
            "NNC network container identity is missing or ambiguous")
    owners = [
        owner for owner in metadata.get("ownerReferences") or []
        if owner.get("kind") == "Node" and owner.get("controller") is True
    ]
    require(len(owners) == 1 and owners[0].get("uid")
            and owners[0].get("name") == metadata.get("name")
            and metadata.get("namespace") == "kube-system" and not metadata.get("deletionTimestamp"),
            "NNC Node ownership is not exact")
    container = containers[0]
    assignments = container.get("ipAssignments") or []
    addresses = sorted(
        entry.get("ip") for entry in assignments
        if isinstance(entry, dict) and isinstance(entry.get("ip"), str)
    )
    count = status.get("assignedIPCount")
    version = container.get("version")
    require(isinstance(count, int) and not isinstance(count, bool) and count > 0
            and isinstance(version, int) and not isinstance(version, bool) and version >= 0
            and len(set(addresses)) == len(addresses) == count
            and all(ipaddress.ip_address(address).version == 4 for address in addresses),
            "NNC concrete IPv4 allocation is malformed")
    return {
        "name": metadata.get("name"), "uid": object_uid(row),
        "node_uid": owners[0]["uid"], "network_container_id": container.get("id"),
        "version": version, "assigned_ip_count": count, "ip_addresses": addresses,
    }


def terminal_failure(view, expected_code):
    rows = view.get("statuses")
    require(isinstance(rows, list), "Failed VM instance view statuses are missing")
    matches = [row for row in rows if row.get("code") == expected_code]
    require(len(matches) == 1 and matches[0].get("level") == "Error"
            and matches[0].get("time") and matches[0].get("message"),
            "Failed VM no longer has the exact terminal provisioning failure")
    guest = (view.get("vmAgent") or {}).get("statuses")
    require(isinstance(guest, list) and len(guest) == 1
            and guest[0].get("code") == "ProvisioningState/Unavailable",
            "Failed VM guest is no longer explicitly unavailable")
    return copy.deepcopy(matches[0])


def ready_guest(view):
    statuses = view.get("statuses")
    codes = {row.get("code") for row in statuses or [] if isinstance(row, dict)}
    guest = (view.get("vmAgent") or {}).get("statuses")
    extensions = view.get("extensions")
    return (
        {"ProvisioningState/succeeded", "PowerState/running"} <= codes
        and not any(str(code).startswith("ProvisioningState/failed") for code in codes)
        and isinstance(guest, list) and len(guest) == 1
        and guest[0].get("code") == "ProvisioningState/succeeded"
        and guest[0].get("displayStatus") == "Ready"
        and stalled.guest_state(view, max_age_seconds=300) == "ready"
        and isinstance(extensions, list) and bool(extensions)
        and all(
            row.get("name") and isinstance(row.get("statuses"), list) and row["statuses"]
            and all(status.get("code") == "ProvisioningState/succeeded"
                    for status in row["statuses"])
            for row in extensions
        )
    )


def mock_agents(payload):
    rows = [
        row for row in mocks._items(payload, "Pod inventory")
        if (row.get("metadata") or {}).get("namespace") == "mock-clustermesh"
        and (row.get("metadata") or {}).get("labels", {}).get("app") == "mock-cilium-agent"
    ]
    result = {(row["metadata"]["name"]): row for row in rows}
    require(len(rows) == len(result) == 100, "Exactly 100 uniquely named mock agents are required")
    return result


def kwok_nodes(payload):
    rows = [
        row for row in mocks._items(payload, "Node inventory")
        if (row.get("metadata") or {}).get("labels", {}).get("type") == "kwok"
    ]
    result = {row["metadata"]["name"]: row for row in rows}
    require(len(rows) == len(result) == 100, "Exactly 100 uniquely named KWOK Nodes are required")
    return result


def real_nodes(payload):
    rows = [
        row for row in mocks._items(payload, "Node inventory")
        if workers.provider_identity(row) is not None
    ]
    result = {row["metadata"]["name"]: row for row in rows}
    require(len(rows) == len(result), "Real Node inventory contains duplicate names")
    return result


def required_source_files(root, role):
    directory = root / role
    required = {
        "summary.json", "nodes.json", "pods.json", "pools.json", "vmsses.json",
        "nnc.json", "pdbs.json", "events.json", "default-operation.json",
        "node-resource-group.json", "cilium-daemonset.json", "credential-read.json",
    }
    names = {path.name for path in directory.iterdir() if path.is_file()}
    require(directory.is_dir() and required <= names, f"{role}: source diagnostics are incomplete")


def load_role_source(root, role, cluster):
    settings = ROLE_SETTINGS[role]
    required_source_files(root, role)
    directory = root / role
    role_summary = read_json(directory / "summary.json")
    require(role_summary.get("role") == role and role_summary.get("read_only") is True
            and resource_equal(role_summary.get("cluster_id"), cluster["id"]),
            f"{role}: source summary is not the read-only scoped diagnostic")
    pools = read_json(directory / "pools.json")
    vmsses = read_json(directory / "vmsses.json")
    nodes_payload = read_json(directory / "nodes.json")
    pods_payload = read_json(directory / "pods.json")
    nnc_payload = read_json(directory / "nnc.json")
    pdbs = read_json(directory / "pdbs.json")
    operation = read_json(directory / "default-operation.json")
    node_group = read_json(directory / "node-resource-group.json")
    require(isinstance(pools, list) and {row.get("name") for row in pools} == {"default", "prompool"}
            and all(row.get("provisioningState") == "Succeeded" for row in pools),
            f"{role}: source pool inventory is not the exact pre-capacity layout")
    by_pool = {row["name"]: row for row in pools}
    source_pool = by_pool[settings["source_pool"]]
    require(source_pool.get("currentOrchestratorVersion") == PATCH
            and source_pool.get("kubeletDiskType") == "OS"
            and source_pool.get("vnetSubnetId") and source_pool.get("podSubnetId"),
            f"{role}: source pool lacks the exact patch, OS kubelet disk, or subnet pins")
    require(isinstance(vmsses, list) and len(vmsses) == 2
            and len({workers.vmss_pool_name(row) for row in vmsses}) == 2,
            f"{role}: source VMSS inventory is not exact")
    vmss_by_pool = {workers.vmss_pool_name(row): row for row in vmsses}
    failed_vmss = vmss_by_pool[settings["source_pool"]]
    require(failed_vmss.get("provisioningState") == "Failed",
            f"{role}: source failed VMSS is not Failed")
    all_instances = {}
    instance_files = {}
    instance_vmss = {}
    for vmss in vmsses:
        name = vmss["name"]
        path = directory / f"{name}-instances.json"
        require(path.is_file(), f"{role}: source instance inventory is missing for {name}")
        rows = read_json(path)
        require(isinstance(rows, list) and len({str(row.get("instanceId")) for row in rows}) == len(rows),
                f"{role}: source VM instances are malformed")
        instance_files[name] = rows
        for row in rows:
            require(row.get("computerName") not in all_instances,
                    f"{role}: source VM computer names are duplicated")
            all_instances[row["computerName"]] = row
            instance_vmss[row["computerName"]] = name
    failed_instance = all_instances.get(settings["failed_node"])
    require(isinstance(failed_instance, dict) and failed_instance.get("provisioningState") == "Failed"
            and maintenance.UUID_RE.fullmatch(str(failed_instance.get("vmId") or "")),
            f"{role}: exact failed VM identity is absent")
    failed_view = read_json(directory / f"{settings['failed_node']}-instance-view.json")
    failure = terminal_failure(failed_view, TERMINAL_CODES[role])
    nodes = real_nodes(nodes_payload)
    failed_node = nodes.get(settings["failed_node"])
    require(failed_node is not None and object_uid(failed_node) == settings["failed_uid"]
            and not workers.node_is_ready(failed_node)
            and resource_equal((failed_node.get("spec") or {}).get("providerID"),
                               "azure://" + failed_instance["id"]),
            f"{role}: source failed Node identity is not exact")
    kwok = kwok_nodes(nodes_payload)
    agents = mock_agents(pods_payload)
    placements = sorted(
        sum(1 for pod in agents.values() if (pod.get("spec") or {}).get("nodeName") == name)
        for name in nodes
        if any((pod.get("spec") or {}).get("nodeName") == name for pod in agents.values())
    )
    require(tuple(sorted(settings["mock_placements"])) == tuple(placements)
            and sum(bool((pod.get("metadata") or {}).get("deletionTimestamp")) for pod in agents.values())
            == settings["terminating_mocks"],
            f"{role}: source mock placement/deletion evidence differs from build 80022")
    nnc_rows = mocks._items(nnc_payload, "source NNC inventory")
    networks = {row["metadata"]["name"]: network_record(row) for row in nnc_rows}
    require(set(nodes) == set(networks), f"{role}: source real Node/NNC inventory differs")
    healthy = [
        name for name, node in nodes.items()
        if name != settings["failed_node"] and workers.node_is_ready(node)
    ]
    require(healthy, f"{role}: source lacks a healthy DaemonSet reference worker")
    daemonsets = maintenance._derive_applicable_daemonsets(pods_payload, [healthy[0]])
    require({name for _, name, _ in daemonsets} >= {"cilium", "azure-cns"},
            f"{role}: healthy source worker lacks Cilium/CNS")
    resident_ips = {
        name: sorted({
            (pod.get("status") or {}).get("podIP")
            for pod in mocks._items(pods_payload, "source Pod inventory")
            if (pod.get("spec") or {}).get("nodeName") == name
            and not (pod.get("spec") or {}).get("hostNetwork")
            and (pod.get("status") or {}).get("podIP")
        })
        for name in nodes
    }
    for name, addresses in resident_ips.items():
        require(set(addresses) <= set(networks[name]["ip_addresses"]),
                f"{role}: source resident Pod IP falls outside its NNC")
    require(operation.get("status") == "Succeeded" and operation.get("name")
            and not operation.get("errorCode"),
            f"{role}: source latest pool operation is not quiescent")
    require(resource_equal(node_group.get("id"),
                           f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{cluster['nodeResourceGroup']}")
            and resource_equal(node_group.get("managedBy"), cluster["id"])
            and str(node_group.get("location", "")).lower() == REGION,
            f"{role}: source node resource-group ownership changed")
    desired = {
        "name": settings["pool"], "count": settings["count"], "mode": settings["mode"],
        "vmSize": VM_SIZE, "maxPods": settings["max_pods"], "osType": "Linux",
        "osSku": "Ubuntu", "osDiskType": "Managed", "osDiskSizeGb": 256,
        "kubeletDiskType": "OS", "enableAutoScaling": False, "enableFips": False,
        "enableEncryptionAtHost": False, "enableNodePublicIp": False,
        "nodeLabels": {"prometheus": "true"} if role == "mesh-89" else {},
        "nodeTaints": None, "vnetSubnetId": source_pool["vnetSubnetId"],
        "podSubnetId": source_pool["podSubnetId"], "kubernetes_patch": PATCH,
    }
    source = {
        "role": role, "cluster": copy.deepcopy(cluster),
        "node_group": cluster["nodeResourceGroup"], "pools": copy.deepcopy(pools),
        "pool_contracts": {name: pool_contract(row) for name, row in by_pool.items()},
        "vmsses": copy.deepcopy(vmsses),
        "vmss_contracts": {workers.vmss_pool_name(row): vmss_contract(row) for row in vmsses},
        "instances": copy.deepcopy(all_instances), "instance_files": copy.deepcopy(instance_files),
        "instance_vmss": instance_vmss,
        "failed": {
            "node_name": settings["failed_node"], "node_uid": settings["failed_uid"],
            "node_contract": node_contract(failed_node), "vm_id": failed_instance["vmId"],
            "resource_id": failed_instance["id"].lower(),
            "instance_id": str(failed_instance["instanceId"]),
            "vmss_name": failed_vmss["name"], "terminal_failure": failure,
        },
        "node_contracts": {name: node_contract(node) for name, node in nodes.items()},
        "kwok_contracts": {name: node_contract(node) for name, node in kwok.items()},
        "mock_contracts": {name: pod_contract(pod) for name, pod in agents.items()},
        "ready_mock_names": sorted(name for name, pod in agents.items()
                                   if stalled.base.pod_ready(pod) and not pod["metadata"].get("deletionTimestamp")),
        "ready_kwok_names": sorted(name for name, node in kwok.items() if workers.node_is_ready(node)),
        "networks": networks, "resident_ips": resident_ips,
        "pdb_sha256": digest(stalled.base.frozen_pdbs({"pdbs": pdbs})), "daemonsets": sorted(daemonsets),
        "healthy_monitoring": {
            row["metadata"]["name"]: pod_contract(row)
            for row in mocks._items(pods_payload, "source monitoring Pods")
            if row["metadata"].get("namespace") == "monitoring"
            and row.get("spec", {}).get("nodeName") != settings["failed_node"]
            and stalled.base.pod_ready(row) and not row["metadata"].get("deletionTimestamp")
        },
        "desired": desired,
    }
    source["pin_sha256"] = digest(source)
    return source


def load_source(args):
    root = Path(args.source_directory).resolve()
    hashes = hash_tree(root)
    required = {"summary.json", "account.json", "resource-group.json", "clusters.json"}
    require(required <= set(hashes), "Build 80022 source root is incomplete")
    summary = read_json(root / "summary.json")
    require(summary.get("read_only") is True and summary.get("resource_mutations") == 0
            and summary.get("health_claimed") is False
            and summary.get("source_build_id") == DIAGNOSED_BUILD
            and summary.get("roles") == ["mesh-2", "mesh-51", "mesh-66", "mesh-79", "mesh-89", "mesh-94"],
            "Source is not the exact read-only build 80022 diagnostic of build 80017")
    account = read_json(root / "account.json")
    group = read_json(root / "resource-group.json")
    clusters = read_json(root / "clusters.json")
    require(str(account.get("id", "")).lower() == SUBSCRIPTION
            and group.get("name") == RESOURCE_GROUP
            and str(group.get("location", "")).lower() == REGION
            and (group.get("properties") or {}).get("provisioningState") == "Succeeded"
            and (group.get("tags") or {}).get("clustermesh_debug_tfvars_sha256") == TFVARS_SHA,
            "Source account/resource-group/region/tfvars identity is not exact")
    require(isinstance(clusters, list) and len(clusters) == 100,
            "Source must contain all 100 preserved AKS resources")
    by_role = {}
    for cluster in clusters:
        role = (cluster.get("tags") or {}).get("role")
        require(role and role not in by_role, "Source cluster roles are missing or duplicated")
        by_role[role] = cluster
    roles = {}
    for role in ROLES:
        cluster = by_role.get(role)
        require(cluster is not None and cluster.get("name") == ROLE_SETTINGS[role]["cluster"]
                and cluster.get("provisioningState") == "Succeeded"
                and str(cluster.get("location", "")).lower() == REGION,
                f"{role}: source AKS identity/state is not exact")
        roles[role] = load_role_source(root, role, cluster)
    require(hash_tree(root) == hashes, "Source diagnostics tree changed while loading")
    return {
        "root": root, "hashes": hashes, "tree_sha256": digest(hashes),
        "resource_group": group, "clusters": clusters, "roles": roles,
    }


def pool_add_command(source):
    desired = source["desired"]
    command = [
        "az", "aks", "nodepool", "add", "--resource-group", RESOURCE_GROUP,
        "--cluster-name", source["cluster"]["name"], "--name", desired["name"],
        "--node-count", str(desired["count"]), "--node-vm-size", VM_SIZE,
        "--mode", desired["mode"], "--os-type", "Linux", "--os-sku", "Ubuntu",
        "--node-osdisk-type", "Managed", "--node-osdisk-size", "256",
        "--max-pods", str(desired["maxPods"]), "--max-surge", "10%",
        "--vnet-subnet-id", desired["vnetSubnetId"],
        "--pod-subnet-id", desired["podSubnetId"],
        "--kubernetes-version", PATCH,
    ]
    if desired["nodeLabels"]:
        command.extend(["--labels", "prometheus=true"])
    command.extend(["--no-wait", "--only-show-errors", "--output", "none"])
    require("--kubelet-disk-type" not in command, "Unsupported kubelet disk flag is forbidden")
    return command


def empty_action():
    return {
        "attempted": False, "submission_started": False, "accepted": None,
        "ambiguous": False, "automatic_retry_allowed": False,
    }


class RoleRecovery(maintenance.ClusterOperator):
    """One role's journaled add and read-only convergence observation."""

    def __init__(self, args, source_bundle, source, summary, runner, deadline):
        role = source["role"]
        role_args = copy.copy(args)
        role_args.kubeconfig = str(Path(args.kubeconfig_directory) / f"{role}.config")
        role_args.context = source["cluster"]["name"]
        super().__init__(role_args, source["cluster"]["name"], runner,
                         deadline - FINAL_RESERVE_SECONDS, deadline)
        self.source_bundle = source_bundle
        self.source = source
        self.summary = summary
        self.role = role
        self.token = uuid.uuid4().hex
        self.journal_name = f"{JOURNAL_PREFIX}-{role}"
        self.journal_uid = ""
        self.journal_rv = ""
        self.journal_pin = None
        self.existing_journals = None
        self.new_vmss = ""
        self.new_identities = {}
        self.new_networks = {}
        self.protected_networks = copy.deepcopy(source["networks"])

    def save(self):
        mocks.write_json_atomic(self.args.summary_file, self.summary)

    def unchanged_source(self):
        require(hash_tree(self.args.source_directory) == self.source_bundle["hashes"],
                "Immutable build 80022 source tree changed")

    def run_read(self, command, timeout_seconds=READ_SECONDS):
        for attempt in range(1, 4):
            try:
                return self.run(command, timeout_seconds)
            except workers.ReconcileError as error:
                if attempt == 3 or TRANSIENT_READ_RE.search(str(error)) is None:
                    raise
                time.sleep(min(2, self.remaining_seconds(2)))
        raise workers.ReconcileError("Transient read retry exhausted")

    def az_read(self, *command, timeout_seconds=READ_SECONDS):
        output = self.run_read(
            ["az", *command, "--output", "json", "--only-show-errors"],
            timeout_seconds,
        )
        return workers.parse_json(output, f"{self.role} Azure read")

    def kube_read(self, *command):
        output = self.run_read(
            ["kubectl", f"--request-timeout={self.args.request_timeout_seconds}s", *command],
            self.args.request_timeout_seconds,
        )
        return workers.parse_json(output, f"{self.role} Kubernetes read")

    def journal_inventory(self, payload):
        selected = {}
        for row in mocks._items(payload, "ConfigMap inventory"):
            metadata = row.get("metadata") or {}
            data = row.get("data") or {}
            name = metadata.get("name")
            labels = metadata.get("labels") or {}
            is_journal = (
                name == self.journal_name or "journal" in str(name).lower()
                or any("journal" in str(key).lower() for key in labels)
                or ("owner" in data and ("token" in data or "record" in data))
            )
            if not is_journal:
                continue
            require(isinstance(name, str) and name not in selected,
                    f"{self.role}: journal names are ambiguous")
            selected[name] = {
                "uid": object_uid(row), "resourceVersion": metadata.get("resourceVersion"),
                "data": copy.deepcopy(data), "deletionTimestamp": metadata.get("deletionTimestamp"),
                "ownerReferences": copy.deepcopy(metadata.get("ownerReferences") or []),
            }
        return selected

    def capture(self):
        source = self.source
        ready = self.run_read(
            ["kubectl", f"--request-timeout={self.args.request_timeout_seconds}s", "get", "--raw=/readyz"],
            self.args.request_timeout_seconds,
        )
        snapshot = {
            "nodes": self.kube_read("get", "nodes", "-o", "json"),
            "pods": self.kube_read("get", "pods", "-A", "-o", "json"),
            "nnc": self.kube_read("get", "nodenetworkconfigs", "-n", "kube-system", "-o", "json"),
            "pdbs": self.kube_read("get", "pdb", "-A", "-o", "json"),
            "configmaps": self.kube_read("get", "configmaps", "-n", "kube-system", "-o", "json"),
        }
        self.summary["per_role"][self.role]["kubernetes_diagnostics"] = stalled.safe_diagnostics(snapshot)
        self.save()
        account = self.az_read("account", "show", "--query", "{id:id}")
        group = self.az_read("group", "show", "--name", RESOURCE_GROUP)
        cluster = self.az_read(
            "aks", "show", "--resource-group", RESOURCE_GROUP, "--name", source["cluster"]["name"],
        )
        node_group = self.az_read("group", "show", "--name", source["node_group"])
        pools = self.az_read(
            "aks", "nodepool", "list", "--resource-group", RESOURCE_GROUP,
            "--cluster-name", source["cluster"]["name"],
        )
        vmsses = self.az_read("vmss", "list", "--resource-group", source["node_group"], "--query", VMSS_QUERY)
        instances = {}
        views = {}
        vmss_models = {}
        for vmss in vmsses:
            name = vmss.get("name")
            require(isinstance(name, str) and name, f"{self.role}: VMSS name is missing")
            vmss_models[name] = self.az_read(
                "vmss", "show", "--resource-group", source["node_group"], "--name", name,
                "--query", VMSS_MODEL_QUERY,
            )
            rows = self.az_read(
                "vmss", "list-instances", "--resource-group", source["node_group"], "--name", name,
                "--query", VM_QUERY,
            )
            instances[name] = rows
            for row in rows:
                instance = str(row.get("instanceId"))
                views[f"{name}/{instance}"] = self.az_read(
                    "vmss", "get-instance-view", "--resource-group", source["node_group"],
                    "--name", name, "--instance-id", instance,
                    "--query", VIEW_QUERY,
                )
        operations = {}
        for pool in pools:
            name = pool.get("name")
            try:
                operations[name] = self.az_read(
                    "aks", "operation", "show-latest", "--resource-group", RESOURCE_GROUP,
                    "--name", source["cluster"]["name"], "--nodepool-name", name,
                )
            except workers.ReconcileError as error:
                action = self.summary["per_role"][self.role]["action"]
                require(name == source["desired"]["name"] and action.get("accepted") is True
                        and action.get("ambiguous") is False and pool.get("provisioningState") in ("Creating", "Updating")
                        and re.search(r"\bNotFound\b|ResourceNotFound|OperationNotFound|\b404\b", str(error)),
                        str(error))
                operations[name] = None
        observed = {
            "account": account, "resource_group": group, "cluster": cluster,
            "node_resource_group": node_group, "pools": pools, "vmsses": vmsses,
            "vmss_models": vmss_models, "instances": instances,
            "instance_views": views, "operations": operations,
            "readyz": ready.strip(), "kubernetes": snapshot,
        }
        self.summary["per_role"][self.role]["diagnostics"] = stalled.safe_diagnostics(observed)
        self.save()
        return observed

    def guard_source(self, observed, *, allow_new=False):
        source = self.source
        desired = source["desired"]
        require(str(observed["account"].get("id", "")).lower() == SUBSCRIPTION
                and observed["resource_group"].get("name") == RESOURCE_GROUP
                and str(observed["resource_group"].get("location", "")).lower() == REGION
                and (observed["resource_group"].get("tags") or {}).get(
                    "clustermesh_debug_tfvars_sha256") == TFVARS_SHA,
                f"{self.role}: live scope/tfvars changed")
        tags = observed["resource_group"].get("tags") or {}
        require(tags.get("run_id") == RESOURCE_GROUP and tags.get("clustermesh_debug_preserved") == "true"
                and tags.get("scenario") == "perf-eval-clustermesh-scale"
                and tags.get("clustermesh_debug_expected_clusters") == "100",
                f"{self.role}: preserved resource-group ownership tags changed")
        prepared.require_lease(observed["resource_group"], self.args.timeout_seconds)
        prepared.require_lease(observed["node_resource_group"], self.args.timeout_seconds)
        cluster = observed["cluster"]
        require(resource_equal(cluster.get("id"), source["cluster"]["id"])
                and str(cluster.get("nodeResourceGroup", "")).lower() == source["node_group"].lower()
                and cluster.get("provisioningState") == "Succeeded"
                and (cluster.get("powerState") or {}).get("code") in (None, "Running")
                and cluster.get("currentKubernetesVersion") == PATCH
                and cluster.get("kubernetesVersion") in (PATCH, "1.35"),
                f"{self.role}: AKS identity/state/patch changed")
        require(resource_equal(observed["node_resource_group"].get("id"),
                               f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{source['node_group']}")
                and resource_equal(observed["node_resource_group"].get("managedBy"), cluster["id"])
                and str(observed["node_resource_group"].get("location", "")).lower() == REGION,
                f"{self.role}: node resource-group identity changed")
        pools = observed["pools"]
        vmsses = observed["vmsses"]
        require(isinstance(pools, list) and len({row.get("name") for row in pools}) == len(pools)
                and isinstance(vmsses, list)
                and len({row.get("name") for row in vmsses}) == len(vmsses),
                f"{self.role}: provider inventory is malformed")
        pool_names = {row.get("name") for row in pools}
        vmss_pools = {workers.vmss_pool_name(row) for row in vmsses}
        expected = {"default", "prompool"}
        allowed = expected | ({desired["name"]} if allow_new else set())
        require(expected <= pool_names <= allowed and expected <= vmss_pools <= allowed,
                f"{self.role}: unrelated pool or VMSS appeared")
        by_pool = {row["name"]: row for row in pools}
        vmss_by_pool = {workers.vmss_pool_name(row): row for row in vmsses}
        pending_protected = False
        for name in expected:
            pool = by_pool[name]
            require(pool_contract(pool) == source["pool_contracts"][name],
                    f"{self.role}: protected {name} pool configuration changed")
            state = pool.get("provisioningState")
            require(state in ("Succeeded", "Updating"),
                    f"{self.role}: protected {name} pool entered an unsupported state")
            pending_protected |= state == "Updating"
            vmss = vmss_by_pool[name]
            require(vmss_contract(vmss) == source["vmss_contracts"][name],
                    f"{self.role}: protected {name} VMSS identity/model/capacity changed")
            state = vmss.get("provisioningState")
            if name == ROLE_SETTINGS[self.role]["source_pool"]:
                require(state in ("Failed", "Updating", "Succeeded"),
                        f"{self.role}: failed-source VMSS entered an unrelated state")
            else:
                require(state in ("Succeeded", "Updating"),
                        f"{self.role}: healthy protected VMSS entered an unsupported state")
            pending_protected |= state == "Updating"
        current_instances = {}
        source_vmss_names = {row["name"] for row in source["vmsses"]}
        for vmss_name, rows in observed["instances"].items():
            require(isinstance(rows, list), f"{self.role}: VM instance inventory is malformed")
            if vmss_name not in source_vmss_names:
                continue
            for row in rows:
                name = row.get("computerName")
                require(name and name not in current_instances,
                        f"{self.role}: VM computer names are duplicated")
                current_instances[name] = row
            if vmss_name in source_vmss_names:
                expected_names = {
                    name for name, expected_vmss in source["instance_vmss"].items()
                    if expected_vmss == vmss_name
                }
                require({row.get("computerName") for row in rows} == expected_names,
                        f"{self.role}: protected VMSS instance inventory changed")
        for name, expected_instance in source["instances"].items():
            current = current_instances.get(name)
            require(current is not None and current.get("vmId") == expected_instance.get("vmId")
                    and str(current.get("instanceId")) == str(expected_instance.get("instanceId"))
                    and resource_equal(current.get("id"), expected_instance.get("id")),
                    f"{self.role}: protected VM identity changed")
            if name != source["failed"]["node_name"]:
                require(current.get("latestModelApplied") is True or pending_protected,
                        f"{self.role}: protected VM no longer applies its stable model")
        failed = source["failed"]
        failed_instance = current_instances[failed["node_name"]]
        require(failed_instance.get("provisioningState") == "Failed"
                and failed_instance.get("vmId") == failed["vm_id"],
                f"{self.role}: old failed VM is no longer the exact known terminal target")
        failure = terminal_failure(
            observed["instance_views"][f"{failed['vmss_name']}/{failed['instance_id']}"],
            TERMINAL_CODES[self.role],
        )
        require(failure == failed["terminal_failure"],
                f"{self.role}: old failed VM terminal evidence changed")
        for name, expected_instance in source["instances"].items():
            if name == failed["node_name"]:
                continue
            require(current_instances[name].get("provisioningState") in (
                ("Succeeded", "Updating") if pending_protected else ("Succeeded",)
            )
                    and ready_guest(observed["instance_views"][
                        f"{source['instance_vmss'][name]}/{expected_instance['instanceId']}"
                    ]),
                    f"{self.role}: protected healthy VM guest/extensions changed")
        snapshot = observed["kubernetes"]
        require(observed["readyz"] == "ok", f"{self.role}: Kubernetes API is not ready")
        nodes = real_nodes(snapshot["nodes"])
        expected_nodes = set(source["node_contracts"])
        new_nodes = set(nodes) - expected_nodes
        require(expected_nodes <= set(nodes)
                and new_nodes <= ({name for name in nodes
                                   if mocks._node_pool_name(nodes[name]) == desired["name"]}
                                  if allow_new else set()),
                f"{self.role}: protected or unrelated real Node inventory changed")
        for name, contract in source["node_contracts"].items():
            require(node_contract(nodes[name]) == contract,
                    f"{self.role}: protected real Node UID/spec/labels changed")
            if name != failed["node_name"]:
                require(workers.node_is_ready(nodes[name]) and not nodes[name]["metadata"].get("deletionTimestamp")
                        and set(map(tuple, source["daemonsets"]))
                        <= maintenance._healthy_system_daemonsets_on_node(snapshot["pods"], name),
                        f"{self.role}: protected healthy Node/system DaemonSet readiness changed")
        require(not workers.node_is_ready(nodes[failed["node_name"]]),
                f"{self.role}: old failed Node unexpectedly changed identity/health")
        kwok = kwok_nodes(snapshot["nodes"])
        require({name: node_contract(node) for name, node in kwok.items()}
                == source["kwok_contracts"],
                f"{self.role}: KWOK Node UID/spec inventory changed")
        require(all(workers.node_is_ready(kwok[name]) for name in source["ready_kwok_names"]),
                f"{self.role}: a previously Ready KWOK Node became unready")
        agents = mock_agents(snapshot["pods"])
        require({name: pod_contract(pod) for name, pod in agents.items()}
                == source["mock_contracts"],
                f"{self.role}: mock UID/spec/placement/deletion inventory changed")
        require(all(stalled.base.pod_ready(agents[name]) for name in source["ready_mock_names"]),
                f"{self.role}: a previously Ready mock agent became unready")
        require(digest(stalled.base.frozen_pdbs(snapshot)) == source["pdb_sha256"],
                f"{self.role}: PDB inventory changed")
        monitoring = {row["metadata"]["name"]: row for row in mocks._items(snapshot["pods"], "monitoring Pods")
                      if row["metadata"].get("namespace") == "monitoring"}
        require(all(name in monitoring and pod_contract(monitoring[name]) == contract
                    and stalled.base.pod_ready(monitoring[name])
                    for name, contract in source["healthy_monitoring"].items()),
                f"{self.role}: protected healthy monitoring Pod changed")
        nnc_rows = mocks._items(snapshot["nnc"], "NNC inventory")
        require(len({row["metadata"]["name"] for row in nnc_rows}) == len(nnc_rows),
                f"{self.role}: NNC names are duplicated")
        current_networks = {
            row["metadata"]["name"]: network_record(row)
            for row in nnc_rows if row["metadata"]["name"] in expected_nodes
        }
        require(set(current_networks) == expected_nodes,
                f"{self.role}: protected NNC inventory changed")
        for name, original in source["networks"].items():
            current = current_networks[name]
            previous = self.protected_networks[name]
            require(all(current[key] == original[key]
                        for key in ("uid", "node_uid", "network_container_id"))
                    and current["version"] >= previous["version"]
                    and (current["version"] > previous["version"]
                         or current["ip_addresses"] == previous["ip_addresses"])
                    and set(source["resident_ips"][name]) <= set(current["ip_addresses"]),
                    f"{self.role}: protected NNC identity/version/resident IP contract changed")
            resident = {
                (pod.get("status") or {}).get("podIP")
                for pod in mocks._items(snapshot["pods"], "Pod inventory")
                if (pod.get("spec") or {}).get("nodeName") == name
                and not (pod.get("spec") or {}).get("hostNetwork")
                and (pod.get("status") or {}).get("podIP")
            }
            require(resident <= set(current["ip_addresses"]),
                    f"{self.role}: protected NNC lost a current resident Pod IP")
            self.protected_networks[name] = copy.deepcopy(current)
        journal_inventory = self.journal_inventory(snapshot["configmaps"])
        own = journal_inventory.pop(self.journal_name, None)
        if self.journal_uid:
            require(own is not None, f"{self.role}: owned journal disappeared")
        else:
            require(own is None, f"{self.role}: existing owned-name journal forbids replay/adoption")
        require(all(row["uid"] and row["resourceVersion"] and not row["deletionTimestamp"]
                    and not row["ownerReferences"] for row in journal_inventory.values()),
                f"{self.role}: an existing journal is malformed or deleting")
        if self.existing_journals is None:
            self.existing_journals = copy.deepcopy(journal_inventory)
        require(journal_inventory == self.existing_journals,
                f"{self.role}: an existing journal UID/RV/data changed")
        if self.journal_uid:
            self.owned_journal()
        return {
            "pending_protected": pending_protected, "new_nodes": new_nodes,
            "nodes": nodes, "current_networks": current_networks,
        }

    def journal_data(self):
        role_summary = self.summary["per_role"][self.role]
        return {
            "owner": OWNER, "token": self.token, "role": self.role,
            "source_build_id": str(DIAGNOSTIC_BUILD),
            "source_tree_sha256": self.source_bundle["tree_sha256"],
            "source_pin_sha256": self.source["pin_sha256"],
            "desired_sha256": digest(self.source["desired"]),
            "record": canonical({
                "status": role_summary["status"], "action": role_summary["action"],
                "capacity_created": role_summary["capacity_created"],
                "initial_network_ready": role_summary["initial_network_ready"],
                "capacity_qualified": False,
                "new_identities": role_summary.get("new_identities", {}),
            }),
        }

    def owned_journal(self, row=None):
        if row is None:
            row = self.kube_read("get", "configmap", self.journal_name, "-n", "kube-system", "-o", "json")
        metadata = row.get("metadata") or {}
        require(object_uid(row) == self.journal_uid
                and metadata.get("resourceVersion") == self.journal_rv
                and not metadata.get("deletionTimestamp")
                and not metadata.get("ownerReferences")
                and row.get("data") == self.journal_pin,
                f"{self.role}: owned journal UID/RV/data/lifecycle changed")
        return row

    def write(self, command, timeout_seconds):
        require(self.args.execute, "Plan mode cannot mutate")
        require(command == pool_add_command(self.source) or command[:4] in (
            ["kubectl", "create", "configmap", self.journal_name],
            ["kubectl", "patch", "configmap", self.journal_name],
        ), f"{self.role}: write escaped the per-role journal and sole pool-add whitelist")
        if command[0] == "kubectl":
            require("-n" in command and command[command.index("-n") + 1] == "kube-system",
                    f"{self.role}: journal write escaped kube-system")
        self.summary["mutation_started"] = True
        self.save()
        return self.run(command, timeout_seconds)

    def acquire(self):
        role_summary = self.summary["per_role"][self.role]
        journal = role_summary["journal"]
        require(not journal["attempted"] and not self.journal_uid,
                f"{self.role}: journal acquisition cannot repeat")
        journal.update(attempted=True, accepted=None, ambiguous=True, requested_at=utc_now())
        self.save()
        data = self.journal_data()
        output = self.write([
            "kubectl", "create", "configmap", self.journal_name, "-n", "kube-system",
            *(f"--from-literal={key}={value}" for key, value in data.items()), "-o", "json",
        ], self.args.request_timeout_seconds)
        row = workers.parse_json(output, f"{self.role} journal creation")
        self.journal_uid = object_uid(row)
        self.journal_rv = str((row.get("metadata") or {}).get("resourceVersion") or "")
        self.journal_pin = data
        require(self.journal_rv and row.get("data") == data,
                f"{self.role}: journal creation response is ambiguous")
        self.owned_journal()
        journal.update(
            uid=self.journal_uid, resource_version=self.journal_rv,
            data_sha256=digest(data), accepted=True, ambiguous=False, accepted_at=utc_now(),
        )
        self.persist()

    def persist(self):
        self.unchanged_source()
        self.owned_journal()
        desired = self.journal_data()
        if desired == self.journal_pin:
            self.summary["per_role"][self.role]["journal"].update(
                resource_version=self.journal_rv, data_sha256=digest(desired),
                noop_update_skipped=True,
            )
            self.save()
            return
        output = self.write([
            "kubectl", "patch", "configmap", self.journal_name, "-n", "kube-system",
            "--type=json", "-p", json.dumps([
                {"op": "test", "path": "/metadata/uid", "value": self.journal_uid},
                {"op": "test", "path": "/metadata/resourceVersion", "value": self.journal_rv},
                {"op": "test", "path": "/data/token", "value": self.token},
                {"op": "test", "path": "/data", "value": self.journal_pin},
                {"op": "add", "path": "/data", "value": desired},
            ]), "-o", "json",
        ], self.args.request_timeout_seconds)
        row = workers.parse_json(output, f"{self.role} journal CAS")
        metadata = row.get("metadata") or {}
        new_rv = str(metadata.get("resourceVersion") or "")
        require(object_uid(row) == self.journal_uid and row.get("data") == desired
                and new_rv and new_rv != self.journal_rv,
                f"{self.role}: changed-data journal CAS result is ambiguous")
        self.journal_pin = copy.deepcopy(desired)
        self.journal_rv = new_rv
        self.summary["per_role"][self.role]["journal"].update(
            resource_version=new_rv, data_sha256=digest(desired), noop_update_skipped=False,
        )
        self.owned_journal()
        self.save()

    def submit(self, observed):
        role_summary = self.summary["per_role"][self.role]
        action = role_summary["action"]
        require(self.journal_uid and not action["attempted"],
                f"{self.role}: nodepool add cannot repeat or escape its journal")
        previous = observed["operations"].get(self.source["desired"]["name"])
        require(previous is None, f"{self.role}: desired pool already has an operation")
        action.update(
            attempted=True, accepted=None, ambiguous=True, command=pool_add_command(self.source),
            previous_operation_name=None, requested_at=utc_now(),
        )
        role_summary["status"] = "add-reserved"
        self.persist()
        self.unchanged_source()
        observed = self.capture()
        guarded = self.guard_source(observed)
        require(not guarded["pending_protected"],
                f"{self.role}: no pool add is authorized while protected resources are Updating")
        action["submission_started"] = True
        action["submission_started_at"] = utc_now()
        role_summary["status"] = "submission-started"
        self.persist()
        try:
            self.write(action["command"], PROVIDER_SUBMIT_SECONDS)
        except EXPECTED_ERRORS:
            action["returned_at"] = utc_now()
            role_summary["status"] = "add-ambiguous"
            self.save()
            try:
                self.persist()
            except EXPECTED_ERRORS as error:
                role_summary["ambiguous_journal_error"] = str(error)
                self.save()
            raise
        action.update(accepted=True, accepted_at=utc_now(), returned_at=utc_now())
        role_summary["status"] = "add-accepted-journal-pending"
        self.persist()
        action["ambiguous"] = False
        role_summary["status"] = "add-accepted"
        try:
            self.persist()
        except EXPECTED_ERRORS:
            action["ambiguous"] = True
            role_summary["status"] = "add-accepted-ambiguity-not-cleared"
            self.save()
            raise

    def new_pool_state(self, observed, guarded):
        desired = self.source["desired"]
        pools = {row.get("name"): row for row in observed["pools"]}
        vmss_by_pool = {workers.vmss_pool_name(row): row for row in observed["vmsses"]}
        pool = pools.get(desired["name"])
        vmss = vmss_by_pool.get(desired["name"])
        if pool is None or vmss is None:
            return False, "waiting for owned pool/VMSS materialization"
        allowed = {"Creating", "Updating", "Scaling", "Succeeded"}
        require(pool.get("provisioningState") in allowed
                and vmss.get("provisioningState") in allowed,
                f"{self.role}: new pool or VMSS entered a terminal provider state")
        require(pool.get("name") == desired["name"] and pool.get("count") == desired["count"]
                and pool.get("mode") == desired["mode"] and pool.get("vmSize") == VM_SIZE
                and pool.get("maxPods") == desired["maxPods"]
                and pool.get("osType") == "Linux" and pool.get("osSku") == "Ubuntu"
                and pool.get("osDiskType") == "Managed"
                and {value for value in (pool.get("osDiskSizeGb"), pool.get("osDiskSizeGB"))
                     if value is not None} == {256}
                and pool.get("kubeletDiskType") == "OS"
                and pool.get("enableAutoScaling") is False
                and pool.get("enableFips") is False
                and pool.get("enableEncryptionAtHost") is False
                and pool.get("enableNodePublicIp") is False
                and pool.get("nodeTaints") in (None, [])
                and (pool.get("nodeLabels") or {}) == desired["nodeLabels"]
                and resource_equal(pool.get("vnetSubnetId"), desired["vnetSubnetId"])
                and resource_equal(pool.get("podSubnetId"), desired["podSubnetId"])
                and pool.get("currentOrchestratorVersion") in (
                    (None, PATCH) if pool.get("provisioningState") in ("Creating", "Updating") else (PATCH,)
                ),
                f"{self.role}: new pool configuration differs from the approved DSv5 delta")
        require((vmss.get("sku") or {}).get("name") == VM_SIZE
                and (vmss.get("sku") or {}).get("capacity") == desired["count"]
                and str(vmss.get("location", "")).lower() == REGION,
                f"{self.role}: new VMSS SKU/capacity/scope differs")
        require(resource_equal(pool.get("id"), f"{self.source['cluster']['id']}/agentPools/{desired['name']}")
                and resource_equal(vmss.get("id"),
                                   f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{self.source['node_group']}"
                                   f"/providers/Microsoft.Compute/virtualMachineScaleSets/{vmss['name']}"),
                f"{self.role}: new provider resource identity escaped the selected scope")
        model = observed["vmss_models"].get(vmss["name"])
        disk = (model or {}).get("osDisk") or {}
        image = (model or {}).get("imageReference") or {}
        sizes = {
            value for value in (disk.get("diskSizeGb"), disk.get("diskSizeGB"))
            if value is not None
        }
        require(model and resource_equal(model.get("id"), vmss.get("id"))
                and disk.get("osType") == "Linux" and sizes == {256}
                and disk.get("diffDiskOption") is None
                and (disk.get("managedDisk") or {}).get("storageAccountType")
                in ("Standard_LRS", "StandardSSD_LRS", "Premium_LRS")
                and isinstance(image, dict) and bool(image),
                f"{self.role}: new VMSS is not a Linux managed 256GiB OS-disk model")
        if pool.get("nodeImageVersion"):
            require(modern.image_matches_pool(image, pool["nodeImageVersion"]),
                    f"{self.role}: new VMSS image does not match the AKS pool image")
        if not self.new_vmss:
            self.new_vmss = vmss["name"]
        require(vmss.get("name") == self.new_vmss,
                f"{self.role}: new VMSS identity changed")
        rows = observed["instances"].get(self.new_vmss) or []
        require(len(rows) <= desired["count"], f"{self.role}: new VMSS has excess instances")
        complete_instances = len(rows) == desired["count"]
        identities = {}
        for row in rows:
            instance_id = str(row.get("instanceId"))
            name = row.get("computerName")
            if name is None or row.get("vmId") is None:
                require(row.get("provisioningState") in ("Creating", "Updating")
                        and instance_id.isdecimal()
                        and resource_equal(row.get("id"), f"{vmss['id']}/virtualMachines/{instance_id}"),
                        f"{self.role}: incomplete new VM identity is not an owned initialization")
                complete_instances = False
                continue
            require(name and maintenance.UUID_RE.fullmatch(str(row.get("vmId") or ""))
                    and row.get("provisioningState") in allowed
                    and resource_equal(row.get("id"),
                                       f"{vmss['id']}/virtualMachines/{instance_id}"),
                    f"{self.role}: new VM identity/state is malformed")
            view = observed["instance_views"][f"{self.new_vmss}/{instance_id}"]
            status_rows = view.get("statuses")
            require(status_rows in (None, []) or all(
                str(status.get("code", "")).startswith(
                    ("ProvisioningState/creating", "ProvisioningState/updating",
                     "ProvisioningState/succeeded", "ProvisioningState/osProvisioningComplete",
                     "PowerState/starting", "PowerState/running")
                ) for status in status_rows
            ), f"{self.role}: new VM instance view reports a terminal state")
            identity = {
                "instance_id": instance_id, "node_name": name, "vm_id": row["vmId"],
                "provider_id": ("azure://" + row["id"]).lower(),
            }
            prior = self.new_identities.get(name)
            require(prior is None or all(prior.get(key) == value for key, value in identity.items()),
                    f"{self.role}: new VM identity changed")
            identities[name] = {**(prior or {}), **identity}
        require(set(self.new_identities) <= set(identities),
                f"{self.role}: observed new VM disappeared")
        self.new_identities = identities
        nodes = guarded["nodes"]
        new_nodes = {
            name: row for name, row in nodes.items()
            if name in guarded["new_nodes"]
            and mocks._node_pool_name(row) == desired["name"]
        }
        for name in set(new_nodes) - set(identities):
            provider = (new_nodes[name].get("spec") or {}).get("providerID")
            require(any(resource_equal(provider, "azure://" + row["id"])
                        and row.get("provisioningState") in ("Creating", "Updating")
                        for row in rows),
                    f"{self.role}: new Node has no matching owned VM")
        new_nnc_rows = {
            row["metadata"]["name"]: row
            for row in mocks._items(observed["kubernetes"]["nnc"], "NNC inventory")
            if row["metadata"]["name"] in identities
        }
        initial_network_ready = complete_instances and len(new_nodes) == desired["count"]
        all_source_ips = {
            address for network in guarded["current_networks"].values()
            for address in network["ip_addresses"]
        }
        source_nc_ids = {row["network_container_id"] for row in source_networks(self.source).values()}
        source_node_uids = {row["uid"] for row in self.source["node_contracts"].values()}
        source_vm_ids = {row["vmId"] for row in self.source["instances"].values()}
        for name, node in new_nodes.items():
            if name not in identities:
                initial_network_ready = False
                continue
            identity = identities[name]
            node_uid = object_uid(node)
            info = (node.get("status") or {}).get("nodeInfo") or {}
            labels = (node.get("metadata") or {}).get("labels") or {}
            require(resource_equal((node.get("spec") or {}).get("providerID"), identity["provider_id"])
                    and node_uid not in source_node_uids
                    and identity["vm_id"] not in source_vm_ids
                    and labels.get("agentpool") == labels.get(
                        "kubernetes.azure.com/agentpool") == desired["name"]
                    and (self.role != "mesh-89" or labels.get("prometheus") == "true")
                    and info.get("kubeletVersion") in (None, "", f"v{PATCH}")
                    and info.get("operatingSystem") in (None, "", "linux")
                    and (not info.get("osImage") or str(info["osImage"]).startswith("Ubuntu"))
                    and not (node.get("spec") or {}).get("unschedulable"),
                    f"{self.role}: new Node identity/labels/version/OS is invalid")
            require(not identity.get("node_uid") or identity["node_uid"] == node_uid,
                    f"{self.role}: observed new Node UID changed")
            require(not identity.get("boot_id") or identity["boot_id"] == info.get("bootID"),
                    f"{self.role}: observed new Node boot ID changed")
            identity["node_uid"] = node_uid
            identity["boot_id"] = info.get("bootID")
            blocking = [
                row for row in (node.get("spec") or {}).get("taints") or []
                if row.get("effect") in ("NoSchedule", "NoExecute")
            ]
            if not workers.node_is_ready(node) or blocking or not all(
                    info.get(key) for key in ("bootID", "kubeletVersion", "operatingSystem", "osImage")):
                initial_network_ready = False
                continue
            require(maintenance.UUID_RE.fullmatch(identity["boot_id"])
                    and not node.get("metadata", {}).get("deletionTimestamp"),
                    f"{self.role}: new Node boot or lifecycle is invalid")
            raw_network = new_nnc_rows.get(name)
            if raw_network is None:
                initial_network_ready = False
                continue
            network = network_record(raw_network, allow_pending=True)
            if network is None:
                initial_network_ready = False
                continue
            require(network["node_uid"] == node_uid
                    and network["network_container_id"] not in source_nc_ids
                    and not set(network["ip_addresses"]).intersection(all_source_ips),
                    f"{self.role}: new NNC conflicts with a protected identity/allocation")
            prior = self.new_networks.get(name)
            if prior is not None:
                require(all(network[key] == prior[key]
                            for key in ("uid", "node_uid", "network_container_id"))
                        and network["version"] >= prior["version"]
                        and (network["version"] > prior["version"]
                             or network["ip_addresses"] == prior["ip_addresses"]),
                        f"{self.role}: new NNC identity/version/allocation regressed")
            self.new_networks[name] = copy.deepcopy(network)
            resident = {
                (pod.get("status") or {}).get("podIP")
                for pod in mocks._items(observed["kubernetes"]["pods"], "Pod inventory")
                if (pod.get("spec") or {}).get("nodeName") == name
                and not (pod.get("spec") or {}).get("hostNetwork")
                and (pod.get("status") or {}).get("podIP")
            }
            require(resident <= set(network["ip_addresses"]),
                    f"{self.role}: new NNC lost a resident Pod IP")
            if network["assigned_ip_count"] < 16:
                initial_network_ready = False
                continue
            present = maintenance._healthy_system_daemonsets_on_node(
                observed["kubernetes"]["pods"], name,
            )
            if not set(map(tuple, self.source["daemonsets"])) <= present:
                initial_network_ready = False
        new_addresses = [address for name, network in self.new_networks.items()
                         if name in new_nodes for address in network["ip_addresses"]]
        require(len(new_addresses) == len(set(new_addresses)),
                f"{self.role}: new workers have overlapping NNC allocations")
        operation = observed["operations"].get(desired["name"])
        if operation is None:
            initial_network_ready = False
        else:
            require(operation.get("name") and not operation.get("errorCode") and not operation.get("error")
                    and operation.get("status") in ("InProgress", "Running", "Succeeded")
                    and operation.get("operationType") in {
                        "PutAgentPool", "CreateAgentPool", "CreateOrUpdateAgentPool",
                        "AgentPoolCreate", "AgentPoolCreateOrUpdate",
                    },
                    f"{self.role}: new pool operation is failed or unrelated")
            action = self.summary["per_role"][self.role]["action"]
            require(not action.get("operation_name") or action["operation_name"] == operation["name"],
                    f"{self.role}: a different operation followed the accepted pool add")
            action["operation_name"] = operation["name"]
            started = datetime.fromisoformat(
                str(operation.get("startTime")).replace("Z", "+00:00")
            )
            requested = datetime.fromisoformat(
                str(action["submission_started_at"]).replace("Z", "+00:00")
            )
            require(requested <= started <= datetime.now(timezone.utc),
                    f"{self.role}: new pool operation time is not causally owned")
            if operation.get("status") == "Succeeded":
                ended = datetime.fromisoformat(
                    str(operation.get("endTime")).replace("Z", "+00:00")
                )
                require(started <= ended <= datetime.now(timezone.utc),
                        f"{self.role}: new pool operation completion time is invalid")
            initial_network_ready &= operation.get("status") == "Succeeded"
        provider_ready = (
            complete_instances and pool.get("provisioningState") == "Succeeded"
            and vmss.get("provisioningState") == "Succeeded"
            and all(row.get("provisioningState") == "Succeeded"
                    and row.get("latestModelApplied") is True
                    and ready_guest(observed["instance_views"][
                        f"{self.new_vmss}/{row['instanceId']}"
                    ]) for row in rows)
        )
        initial_network_ready &= provider_ready and not guarded["pending_protected"]
        return initial_network_ready, (
            "" if initial_network_ready else
            "waiting for provider/guest/Node/NNC/system-DaemonSet/protected-VMSS convergence"
        )

    def wait_ready(self):
        role_summary = self.summary["per_role"][self.role]
        while True:
            self.unchanged_source()
            observed = self.capture()
            guarded = self.guard_source(observed, allow_new=True)
            ready, reason = self.new_pool_state(observed, guarded)
            role_summary["new_identities"] = copy.deepcopy(self.new_identities)
            role_summary["new_networks"] = copy.deepcopy(self.new_networks)
            role_summary["wait_reason"] = reason
            role_summary["capacity_created"] = ready
            role_summary["initial_network_ready"] = ready
            role_summary["capacity_qualified"] = False
            role_summary["status"] = "initial-network-ready" if ready else "capacity-pending"
            if not guarded["pending_protected"]:
                self.persist()
            else:
                self.save()
            print(f"{utc_now()}: {self.role} initial_network_ready={ready}; {reason}", flush=True)
            if ready:
                return
            require(time.monotonic() < self.work_deadline,
                    f"{self.role}: secondary capacity readiness exceeded the bounded wait")
            time.sleep(min(POLL_SECONDS, self.remaining_seconds(POLL_SECONDS)))

    def plan(self):
        self.unchanged_source()
        observed = self.capture()
        guarded = self.guard_source(observed)
        require(not guarded["pending_protected"],
                f"{self.role}: protected provider resources must be quiescent before planning")
        role_summary = self.summary["per_role"][self.role]
        role_summary.update(
            status="plan-valid", plan_valid=True,
            source_pin_sha256=self.source["pin_sha256"],
            desired_configuration=copy.deepcopy(self.source["desired"]),
            command=pool_add_command(self.source),
        )
        self.unchanged_source()
        self.save()
        return observed


def source_networks(source):
    return source["networks"]


def capacity_read(args, runner, required_cores, *, outer_deadline=None):
    deadline = time.monotonic() + min(args.timeout_seconds, 600)
    if outer_deadline is not None:
        deadline = min(deadline, outer_deadline)
    operator_args = copy.copy(args)
    operator_args.kubeconfig = str(Path(args.kubeconfig_directory) / "mesh-51.config")
    operator_args.context = ROLE_SETTINGS["mesh-51"]["cluster"]
    operator = maintenance.ClusterOperator(
        operator_args, operator_args.context, runner, deadline, deadline,
    )

    def read(command, timeout):
        for attempt in range(1, 4):
            try:
                return workers.parse_json(operator.run(command, timeout), "regional capacity read")
            except workers.ReconcileError as error:
                if attempt == 3 or TRANSIENT_READ_RE.search(str(error)) is None:
                    raise
                time.sleep(2)
        raise workers.ReconcileError("Regional capacity read retry exhausted")

    usage = read([
        "az", "vm", "list-usage", "--location", REGION,
        "--query", "[].{name:name.value,currentValue:currentValue,limit:limit}",
        "--output", "json", "--only-show-errors",
    ], READ_SECONDS)
    require(isinstance(usage, list), "Regional quota response is malformed")
    counters = {}
    for name in (QUOTA_FAMILY, "cores"):
        rows = [row for row in usage if row.get("name") == name]
        require(len(rows) == 1, f"Regional quota lacks exactly one {name} counter")
        current, limit = rows[0].get("currentValue"), rows[0].get("limit")
        require(isinstance(current, int) and not isinstance(current, bool)
                and isinstance(limit, int) and not isinstance(limit, bool)
                and 0 <= current <= limit,
                f"Regional quota counter {name} is malformed")
        counters[name] = {
            "currentValue": current, "limit": limit, "remaining": limit - current,
        }
    require(all(row["remaining"] >= required_cores for row in counters.values()),
            f"Fresh DSv5/general regional quota headroom is below {required_cores} cores")
    sku = read([
        "az", "vm", "list-skus", "--location", REGION,
        "--resource-type", "virtualMachines", "--size", VM_SIZE, "--all",
        "--query", f"[?name=='{VM_SIZE}'].{{name:name,family:family,resourceType:resourceType,"
        "locations:locations,restrictions:restrictions,capabilities:capabilities}",
        "--output", "json", "--only-show-errors",
    ], SKU_READ_SECONDS)
    require(isinstance(sku, list) and len(sku) == 1, "Exact DSv5 SKU discovery is ambiguous")
    row = sku[0]
    capabilities = {
        item.get("name"): item.get("value") for item in row.get("capabilities") or []
    }
    require(row.get("name") == VM_SIZE and row.get("family") == QUOTA_FAMILY
            and row.get("resourceType") == "virtualMachines"
            and REGION in [str(value).lower() for value in row.get("locations") or []]
            and row.get("restrictions") == []
            and capabilities.get("vCPUs") == "8"
            and capabilities.get("MemoryGB") == "32"
            and capabilities.get("PremiumIO") == "True"
            and int(capabilities.get("OSVhdSizeMB", "0")) >= 256 * 1024,
            "Standard_D8s_v5 is restricted or cannot satisfy the managed 256GiB OS-disk contract")
    return {
        "checked_at": utc_now(), "required_cores": required_cores,
        "counters": counters, "sku": copy.deepcopy(row),
        "deprecated_dv3_quota_read": False,
    }


def validate_args(args):
    require(args.resource_group == args.confirm_resource_group == RESOURCE_GROUP
            and args.expected_subscription.lower() == SUBSCRIPTION
            and args.expected_region.lower() == REGION
            and args.expected_tfvars_sha.lower() == TFVARS_SHA,
            "Secondary capacity scope/subscription/region/tfvars confirmation mismatch")
    require(args.source_build_id == DIAGNOSTIC_BUILD
            and isinstance(args.timeout_seconds, int) and not isinstance(args.timeout_seconds, bool)
            and 600 <= args.timeout_seconds <= 7200
            and isinstance(args.request_timeout_seconds, int)
            and 10 <= args.request_timeout_seconds <= 120,
            "Only build 80022 and bounded 600..7200 second execution are supported")
    source = Path(args.source_directory).resolve()
    kube = Path(args.kubeconfig_directory).resolve()
    output = Path(args.summary_file).resolve()
    require(source.is_dir() and kube.is_dir()
            and not Path(args.source_directory).is_symlink()
            and not Path(args.kubeconfig_directory).is_symlink()
            and len({source, kube, output}) == 3
            and output not in source.parents and output not in kube.parents
            and source not in output.parents and kube not in output.parents
            and not output.exists(),
            "Source, private kubeconfigs, and new output must be separate")
    files = {path.name for path in kube.iterdir() if path.is_file()}
    require(files == {f"{role}.config" for role in ROLES}
            and all(not path.is_symlink() for path in kube.iterdir()),
            "Kubeconfig directory must contain exactly the four private role.config files")


def execute_recovery(args, summary, runner=workers.run_command):
    validate_args(args)
    summary.update(
        schema_version=1, execute=args.execute, source_build_id=DIAGNOSTIC_BUILD,
        diagnosed_build_id=DIAGNOSED_BUILD, mutation_started=False,
        plan_valid=False, success=False, status="validating-source",
        workloads_ready=False, completed_global_baseline=False,
        capacity_qualified=False, started_at=utc_now(), finished_at=None,
        automatic_resume_or_adoption=False, per_role={},
    )
    try:
        bundle = load_source(args)
        summary.update(
            source_tree_hashes=bundle["hashes"], source_tree_sha256=bundle["tree_sha256"],
            roles=list(ROLES),
        )
        for role in ROLES:
            source = bundle["roles"][role]
            summary["per_role"][role] = {
                "role": role, "cluster": source["cluster"]["name"],
                "status": "validating", "plan_valid": False,
                "capacity_created": False, "initial_network_ready": False,
                "capacity_qualified": False, "workloads_ready": False,
                "journal": {
                    "name": f"{JOURNAL_PREFIX}-{role}", "namespace": "kube-system",
                    "retained": True, "attempted": False, "accepted": None, "ambiguous": False,
                },
                "action": empty_action(),
                "qualification_required": [
                    "token/UID-owned non-host-network HTTP probes on every new Node",
                    "NNC version and concrete IP allocation growth beyond the initial 16 addresses",
                    "CPU/memory/pod-slot placement proof for the later failed-host workload",
                    "owned probe cleanup with UID-bound deletion receipts",
                ],
            }
        deadline = time.monotonic() + args.timeout_seconds
        summary["capacity"] = capacity_read(args, runner, TOTAL_CORES,
                                             outer_deadline=deadline - FINAL_RESERVE_SECONDS)
        recoveries = {
            role: RoleRecovery(
                args, bundle, bundle["roles"][role], summary, runner, deadline,
            )
            for role in ROLES
        }
        plans = {role: recoveries[role].plan() for role in ROLES}
        require(hash_tree(args.source_directory) == bundle["hashes"],
                "Source diagnostics changed after planning")
        summary.update(plan_valid=True, status="plan-valid", success=not args.execute)
        for role in ROLES:
            summary["per_role"][role]["diagnostics_captured_before_health_guards"] = True
        mocks.write_json_atomic(args.summary_file, summary)
        if not args.execute:
            return
        outstanding = TOTAL_CORES
        for role in ROLES:
            recovery = recoveries[role]
            summary["capacity"] = capacity_read(args, runner, outstanding,
                                                 outer_deadline=deadline - FINAL_RESERVE_SECONDS)
            recovery.acquire()
            recovery.submit(plans[role])
            recovery.wait_ready()
            outstanding -= 8 * bundle["roles"][role]["desired"]["count"]
        summary.update(
            success=True, status="secondary-capacity-initial-network-ready",
            capacity_created=True, initial_network_ready=True,
            capacity_qualified=False, workloads_ready=False,
            completed_global_baseline=False,
        )
    except EXPECTED_ERRORS as error:
        summary.update(
            success=False, status="failed-closed", error=str(error),
            capacity_qualified=False, workloads_ready=False,
            completed_global_baseline=False, automatic_resume_or_adoption=False,
        )
        raise
    finally:
        summary["finished_at"] = utc_now()
        summary["capacity_qualified"] = False
        summary["workloads_ready"] = False
        summary["completed_global_baseline"] = False
        mocks.write_json_atomic(args.summary_file, summary)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "source-directory", "resource-group", "confirm-resource-group",
        "expected-subscription", "expected-region", "expected-tfvars-sha",
        "kubeconfig-directory", "summary-file",
    ):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--source-build-id", required=True, type=int)
    parser.add_argument("--timeout-seconds", type=int, default=3600)
    parser.add_argument("--request-timeout-seconds", type=int, default=60)
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    def interrupted(signum, _frame):
        raise workers.ReconcileError(
            f"Interrupted ({signum}); retain all journals and never replay an ambiguous add"
        )

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, interrupted)
    args, summary = parse_args(argv), {}
    try:
        execute_recovery(args, summary)
    except EXPECTED_ERRORS as error:
        print(f"Secondary capacity recovery failed closed: {error}", file=sys.stderr)
        return 1
    print(
        f"{summary['status']}; capacity_qualified=false; workloads_ready=false",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
