#!/usr/bin/env python3
"""Add mesh-96 promv5 after build 80001, then retire only empty prompool."""

# pylint: disable=protected-access,too-many-lines,too-many-boolean-expressions,too-many-instance-attributes

from __future__ import annotations

import argparse
import copy
import json
import re
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import capacity_first_qualification as qualification
import cni_worker_maintenance as maintenance
import modern_pool_baseline as baseline
import modern_prom_recovery as modern
import mock_cni_recovery as mocks
import qualified_failed_worker_retirement as retirement
import stalled_retained_worker_recovery as stalled


base = retirement.base
workers = retirement.workers
prepared = retirement.capacity.prepared
require = retirement.require
uid = base.object_uid

RETIREMENT_BUILD = 80001
RETIREMENT_JOURNAL_UID = "cabb75e5-7260-4202-bbc7-d3863f5388c3"
JOURNAL = "mesh96-post-retirement-prom-recovery"
JOURNAL_OWNER = "post-retirement-prom-recovery"
OLD_POOL = "prompool"
NEW_POOL = modern.POOL_NAME
OLD_VMSS = base.PROM_VMSS
PATCH = "1.35.7"
OPERATOR_NAME = "prometheus-operator-7c59d5d8c4-qf9t5"
OPERATOR_UID = "7d5407d0-d766-4470-aa10-78eb91de568e"
OPERATOR_RS_UID = "4ca2daa1-72d8-4327-9ce5-fea99be1a82d"
GRAFANA_UID = "622c2703-b855-4411-a8e9-705b129ab766"
RETIRED_NODE = "aks-default-28928250-vmss000001"
RETIRED_NODE_UID = "c673a142-17ac-44c7-92cc-32efc0d34c61"
RETIRED_VM_ID = "d81b78a9-fe40-468d-91ec-d66f0456bfa7"
SOURCE_NODE = base.SOURCE_NODE
SOURCE_NODE_UID = "6e4ad0c4-1cde-451a-966b-aefc04ca59d4"
SOURCE_VM_ID = "c9f1454f-144e-4281-916b-7612f89a31e6"
SOURCE_BOOT_ID = "fbecbcc6-d934-4c29-adb5-1331497b3706"
PROM_MEMORY_RESERVE = 16 * 1024**3
MEMORY_SAFETY_RESERVE = 512 * 1024**2
PROVIDER_SUBMIT_SECONDS = 180
POLL_SECONDS = 10
FINAL_RESERVE_SECONDS = 120
CREATING = {"Creating", "Updating", "Scaling"}
DELETING = {"Deleting", "Updating"}
CREATE_OPERATION_TYPES = {
    "PutAgentPool", "CreateAgentPool", "CreateOrUpdateAgentPool",
    "AgentPoolCreate", "AgentPoolCreateOrUpdate",
}
DELETE_OPERATION_TYPES = {"DeleteAgentPool", "AgentPoolDelete"}
EXPECTED_ERRORS = retirement.EXPECTED_ERRORS
HISTORICAL_JOURNALS = {
    retirement.JOURNAL, qualification.JOURNAL,
    retirement.capacity.JOURNAL, retirement.stalled.JOURNAL,
}
RETIREMENT_REQUIRED_FILES = {
    "retirement.json", "plan.json", "inputs.sha256", "inputs-plan.sha256", "inputs-execute.sha256",
    "qualification-input/qualification.json", "qualification-input/prior-qualification.json",
    "qualification-input/plan.json", "worker-state/current-nodes.json",
    "worker-state/current-pods.json", "worker-state/current-controllers.json",
    "worker-state/current-pdbs.json", "worker-state/current-nnc.json",
    "worker-state/pools.json", "worker-state/vmsses.json",
    "worker-state/default-instances.json", "worker-state/cniv5-instances.json",
    "worker-state/prom-instances.json",
}
VMSS_MODEL_QUERY = (
    "{id:id,osDisk:virtualMachineProfile.storageProfile.osDisk."
    "{osType:osType,diskSizeGb:diskSizeGb,diskSizeGB:diskSizeGB,"
    "managedDisk:managedDisk.{storageAccountType:storageAccountType},"
    "diffDiskOption:diffDiskSettings.option},"
    "imageReference:virtualMachineProfile.storageProfile.imageReference}"
)


def empty_action() -> dict:
    """Return a never-submitted provider action receipt."""

    return {
        "attempted": False, "submission_started": False, "accepted": None,
        "ambiguous": False, "automatic_retry_allowed": False,
    }


def pool_add_command() -> list[str]:
    """Return the single approved GA promv5 creation command."""

    return modern.pool_add_command(PATCH)


def pool_delete_command() -> list[str]:
    """Return the single approved deletion of the old empty pool object."""

    return list(modern.RETIRE_COMMAND)


def _disk_size(row: dict) -> object:
    values = [row.get(name) for name in ("osDiskSizeGb", "osDiskSizeGB", "diskSizeGb", "diskSizeGB")
              if row.get(name) is not None]
    require(values and len(set(values)) == 1, "Managed OS disk size aliases are missing or disagree")
    return values[0]


def _action_complete(action: dict) -> bool:
    return (
        action.get("attempted") is True and action.get("submission_started") is True
        and action.get("accepted") is True and action.get("ambiguous") is False
    )


def _validate_retirement_receipt(receipt: dict) -> None:
    require(
        isinstance(receipt, dict) and receipt.get("schema_version") == 1
        and receipt.get("execute") is True and receipt.get("plan_valid") is True
        and receipt.get("mutation_started") is True and receipt.get("success") is True
        and receipt.get("native_fencing_proven") is True
        and receipt.get("source_retired") is True
        and receipt.get("replacements_ready") is True
        and receipt.get("placement_hold_removed") is True
        and receipt.get("current_mock_ready") == 100
        and receipt.get("kwok_ready") == 100
        and receipt.get("workloads_ready") is False
        and receipt.get("bootstrap_complete") is False
        and receipt.get("cleanup_errors") == []
        and receipt.get("plan_sha256") == baseline.PLAN_SHA,
        "Only the genuine successful build 80001 retirement may authorize monitoring recovery",
    )
    require(receipt.get("target") == {
        "node_name": RETIRED_NODE, "node_uid": RETIRED_NODE_UID, "vm_id": RETIRED_VM_ID,
    }, "Build 80001 retired a different worker")
    native = receipt.get("native") or {}
    require(
        native.get("attempted") is True and native.get("submission_started") is True
        and native.get("accepted") is True and native.get("ambiguous") is False
        and native.get("automatic_retry_allowed") is False
        and native.get("operation_name") == "25c65dfe-cdcf-428c-9685-53ec1b3982ec"
        and native.get("vm_absence_observed_at"),
        "Build 80001 lacks positive, nonambiguous native VM fencing",
    )
    journal = receipt.get("journal") or {}
    require(
        journal.get("name") == retirement.JOURNAL
        and journal.get("uid") == RETIREMENT_JOURNAL_UID
        and journal.get("retained") is True and journal.get("accepted") is True
        and journal.get("ambiguous") is False,
        "The completed retirement journal identity changed",
    )
    hold = receipt.get("hold") or {}
    require(
        hold.get("node_name") == SOURCE_NODE and hold.get("node_uid") == SOURCE_NODE_UID
        and hold.get("applied") is False and hold.get("cleanup_started") is True
        and (hold.get("remove") or {}).get("accepted") is True,
        "The source0 retirement placement hold was not cleanly removed",
    )
    for key, count in (
        ("current_mock_uids", 100), ("preserved_kwok_uids", 100),
        ("protected_mock_uids", 44), ("original_target_mock_uids", 56),
        ("controller_replacements", 56),
    ):
        value = receipt.get(key)
        exact = isinstance(value, dict) and len(value) == count
        if key != "controller_replacements":
            exact = exact and len(set(value.values())) == count and all(
                isinstance(row_uid, str) and maintenance.UUID_RE.fullmatch(row_uid)
                for row_uid in value.values()
            )
        require(exact,
                f"Build 80001 {key} is incomplete")
    replacements = receipt["controller_replacements"]
    names = {f"kwok-node-{index}" for index in range(100)}
    require(set(receipt["current_mock_uids"]) == set(receipt["preserved_kwok_uids"]) == names
            and set(receipt["protected_mock_uids"]).isdisjoint(receipt["original_target_mock_uids"])
            and set(receipt["protected_mock_uids"]) | set(receipt["original_target_mock_uids"]) == names
            and all(receipt["current_mock_uids"][name] == value
                    for name, value in receipt["protected_mock_uids"].items()),
            "Build 80001 does not account for exactly 44 preserved and 56 replaced identities")
    require(set(replacements) == set(receipt["original_target_mock_uids"]),
            "Build 80001 replacement names are incomplete")
    for name, row in replacements.items():
        require(
            isinstance(row, dict)
            and row.get("old_uid") == receipt["original_target_mock_uids"][name]
            and row.get("new_uid") == receipt["current_mock_uids"][name]
            and row.get("old_uid") != row.get("new_uid")
            and row.get("ready") is True and row.get("fencing_proven") is True,
            f"{name}: build 80001 replacement proof changed",
        )


def _operator(pods: dict) -> dict:
    rows = [
        row for row in mocks._items(pods, "Pod inventory")
        if (row.get("metadata") or {}).get("namespace") == "monitoring"
        and (row.get("metadata") or {}).get("name") == OPERATOR_NAME
    ]
    require(len(rows) == 1 and uid(rows[0]) == OPERATOR_UID,
            "The exact existing prometheus-operator Pod UID is required")
    owner = base.controller_owner(rows[0], "ReplicaSet")
    require(owner.get("uid") == OPERATOR_RS_UID,
            "The prometheus-operator ReplicaSet ownership changed")
    selector = (rows[0].get("spec") or {}).get("nodeSelector")
    require(selector == {"kubernetes.io/os": "linux", "prometheus": "true"},
            "The prometheus-operator selector changed")
    require(base.pvc_free(rows[0].get("spec") or {})
            and not (rows[0].get("spec") or {}).get("hostNetwork"),
            "The operator must remain a non-host-network, PVC-free Pod")
    return rows[0]


def _raw_semantic_spec(recorded, raw, description):
    expected = retirement.semantic_pod_spec(recorded)
    actual = retirement.semantic_pod_spec(raw)
    require(stalled.safe_diagnostics(actual) == expected,
            f"{description}: raw source spec does not match the completed retirement diagnostics")
    return actual


def load_retirement(args) -> dict:
    """Load and pin the entire completed build 80001 artifact."""

    hashes = qualification.hash_tree(args.retirement_directory)
    require(RETIREMENT_REQUIRED_FILES <= set(hashes),
            "Whole build 80001 artifact is missing raw retirement/qualification/worker-state inputs")
    root = Path(args.retirement_directory).resolve()
    receipt = stalled.read_json(root / "retirement.json")
    _validate_retirement_receipt(receipt)
    for prefix, key in (("worker-state", "worker_state_input_hashes"),
                        ("qualification-input", "qualification_input_hashes")):
        actual = {name.removeprefix(prefix + "/"): digest for name, digest in hashes.items()
                  if name.startswith(prefix + "/")}
        require(isinstance(receipt.get(key), dict) and actual and actual == receipt[key],
                f"Build 80001 {prefix} input hashes changed from the native retirement receipt")
    current = receipt.get("current_kubernetes_diagnostics")
    native = receipt.get("native_observation")
    require(isinstance(current, dict) and isinstance(native, dict),
            "Build 80001 final Kubernetes/ARM evidence is missing")
    raw_controllers = stalled.read_json(root / "worker-state/current-controllers.json")
    raw_pods = stalled.read_json(root / "worker-state/current-pods.json")
    require(stalled.controllers_pin(stalled.safe_diagnostics(raw_controllers))
            == stalled.controllers_pin(current["controllers"]),
            "Raw controller source does not match the completed retirement diagnostics")
    agents = maintenance._agent_map(current["pods"])
    raw_agents = maintenance._agent_map(raw_pods)
    require(set(agents) == set(receipt["current_mock_uids"]), "Final mock names differ from build 80001")
    require(set(raw_agents) == set(agents), "Raw source mock names differ from build 80001")
    mock_specs = {}
    for name, expected_uid in receipt["current_mock_uids"].items():
        pod = agents[name]
        require(uid(pod) == expected_uid and base.pod_ready(pod)
                and not pod["metadata"].get("deletionTimestamp"),
                f"{name}: build 80001 final mock identity/readiness is invalid")
        require(pod["spec"].get("nodeName") == (
            SOURCE_NODE if name in receipt["protected_mock_uids"]
            else receipt["controller_replacements"][name].get("node_name")
        ), f"{name}: final placement differs from the native retirement proof")
        require(uid(raw_agents[name]) in {
            expected_uid, receipt["original_target_mock_uids"].get(name),
        }, f"{name}: raw source mock UID is not in the retirement lineage")
        mock_specs[name] = _raw_semantic_spec(pod["spec"], raw_agents[name]["spec"], name)
    maintenance._require_all_kwok_ready(current["nodes"], receipt["preserved_kwok_uids"])
    real_nodes = maintenance._real_node_map(current["nodes"])
    require(len(real_nodes) == 3 and SOURCE_NODE in real_nodes
            and RETIRED_NODE not in real_nodes, "Build 80001 final real Node inventory is not exact")
    networks = {}
    allocated_ips = set()
    for row in mocks._items(current["nnc"], "build 80001 NNC inventory"):
        network = qualification.concrete_network(row)
        require(network["name"] not in networks
                and not allocated_ips.intersection(network["ip_addresses"]),
                "Build 80001 NNC names or concrete IP allocations overlap")
        networks[network["name"]] = network
        allocated_ips.update(network["ip_addresses"])
    require(set(networks) == set(real_nodes), "Build 80001 final NNC inventory is not exact")
    real_pins = {}
    for name, node in real_nodes.items():
        real_pins[name] = {
            "node_name": name, "node_uid": uid(node), "boot_id": base.node_boot(node),
            "provider_id": node["spec"]["providerID"].lower(),
            "pool_name": mocks._node_pool_name(node),
            "nnc_uid": networks[name]["uid"], "network_container_id": networks[name]["network_container_id"],
            "nnc_version": networks[name]["version"],
            "nnc_ip_addresses": copy.deepcopy(networks[name]["ip_addresses"]),
            "logical_node": stalled.logical_node(node),
        }
    require(real_pins[SOURCE_NODE]["node_uid"] == SOURCE_NODE_UID
            and real_pins[SOURCE_NODE]["boot_id"] == SOURCE_BOOT_ID
            and real_pins[SOURCE_NODE]["pool_name"] == "default",
            "Healthy source0 identity differs from the approved build 80001 result")
    operator = _operator(current["pods"])
    raw_operator = _operator(raw_pods)
    require(
        not (operator.get("spec") or {}).get("nodeName")
        and (operator.get("status") or {}).get("phase") == "Pending"
        and not (operator.get("status") or {}).get("podIP")
        and not operator["metadata"].get("deletionTimestamp"),
        "Build 80001 must contain the same naturally Pending, unassigned operator Pod",
    )
    grafana_rows = [
        row for row in mocks._items(current["pods"], "build 80001 Pod inventory")
        if uid(row) == GRAFANA_UID
    ]
    require(len(grafana_rows) == 1 and base.pod_ready(grafana_rows[0])
            and grafana_rows[0]["spec"].get("nodeName") == SOURCE_NODE,
            "Build 80001 does not preserve the healthy source0 Grafana Pod")
    raw_grafana = [row for row in mocks._items(raw_pods, "raw source Pods") if uid(row) == GRAFANA_UID]
    require(len(raw_grafana) == 1, "Raw source does not preserve the exact Grafana UID")
    grafana_pin = {
        "name": grafana_rows[0]["metadata"]["name"], "uid": GRAFANA_UID,
        "node_name": SOURCE_NODE,
        "semantic_spec": _raw_semantic_spec(grafana_rows[0]["spec"], raw_grafana[0]["spec"], "Grafana"),
    }
    pools = native.get("pools")
    vmsses = native.get("vmsses")
    require(isinstance(pools, list) and {row.get("name") for row in pools}
            == {"default", "cniv5", OLD_POOL}, "Build 80001 pool evidence is not exact")
    require(isinstance(vmsses, list) and len(vmsses) == 3,
            "Build 80001 VMSS evidence is not exact")
    pool_pins = {row["name"]: prepared.pool_configuration(row) for row in pools}
    vmss_pins = {}
    for row in vmsses:
        pool_name = workers.vmss_pool_name(row)
        model = copy.deepcopy(row)
        model.pop("provisioningState", None)
        model["sku"].pop("capacity", None)
        vmss_pins[pool_name] = model
    instances = {
        row["computerName"]: {
            "vm_id": row["vmId"], "resource_id": row["id"].lower(),
            "instance_id": str(row["instanceId"]),
        }
        for row in [*(native.get("default_instances") or []), *(native.get("new_instances") or [])]
    }
    require(set(instances) == set(real_nodes)
            and instances[SOURCE_NODE]["vm_id"] == SOURCE_VM_ID,
            "Build 80001 final VM identities are incomplete")
    require(qualification.hash_tree(args.retirement_directory) == hashes,
            "Build 80001 artifact changed while loading")
    return {
        "hashes": hashes, "receipt": receipt,
        "controllers": stalled.controllers_pin(raw_controllers),
        "pdbs": base.frozen_pdbs(current),
        "mock_specs": mock_specs, "real_pins": real_pins,
        "pool_pins": pool_pins, "vmss_pins": vmss_pins, "instances": instances,
        "operator_spec": _raw_semantic_spec(operator["spec"], raw_operator["spec"], "prometheus-operator"),
        "grafana": grafana_pin,
    }


class PromRecovery(maintenance.ClusterOperator):
    """Bounded post-retirement add/readiness/empty-delete protocol."""

    def __init__(self, args, bundle, summary, runner):
        deadline = time.monotonic() + args.timeout_seconds
        super().__init__(args, base.CLUSTER, runner, deadline - FINAL_RESERVE_SECONDS, deadline)
        self.bundle = bundle
        self.summary = summary
        self.token = uuid.uuid4().hex
        self.journal_uid = ""
        self.journal_resource_version = ""
        self.journal_data_pin = None
        self.authority_pin = None
        self.old_journals = None
        self.initial_operations = {}
        self.existing_nnc = copy.deepcopy(bundle["real_pins"])
        self.new_network = None
        self.candidate_node = None
        self.daemonsets = set()
        self.new_identity = None
        self.new_vmss = ""
        self.phase = "plan"

    def save(self):
        mocks.write_json_atomic(self.args.summary_file, self.summary)

    def unchanged_inputs(self):
        require(qualification.hash_tree(self.args.retirement_directory) == self.bundle["hashes"],
                "Immutable build 80001 inputs changed")

    def az_json_retry(self, *command):
        """Retry only transient failures from the two bounded capacity reads."""

        for attempt in range(1, 4):
            try:
                return self.az_json(*command)
            except workers.ReconcileError as error:
                if attempt == 3 or modern.recovery.arm.TRANSIENT_READ_RE.search(str(error)) is None:
                    raise
                time.sleep(min(2, self.remaining_seconds(2)))
        raise workers.ReconcileError("Capacity read retry exhausted without an observation")

    def kube(self, *command):
        return workers.parse_json(
            self.run(["kubectl", f"--request-timeout={self.args.request_timeout_seconds}s", *command],
                     self.args.request_timeout_seconds),
            "post-retirement Kubernetes read",
        )

    def snapshot(self):
        ready = self.run([
            "kubectl", f"--request-timeout={self.args.request_timeout_seconds}s",
            "get", "--raw=/readyz",
        ], self.args.request_timeout_seconds)
        require(ready.strip() == "ok", "The selected mesh-96 Kubernetes API is not ready")
        snapshot = {
            "nodes": self.kube("get", "nodes", "-o", "json"),
            "pods": self.kube("get", "pods", "-A", "-o", "json"),
            "nnc": self.kube("get", "nodenetworkconfigs", "-n", "kube-system", "-o", "json"),
            "controllers": self.kube("get", "deployments,replicasets,daemonsets,statefulsets", "-A", "-o", "json"),
            "pdbs": self.kube("get", "pdb", "-A", "-o", "json"),
        }
        self.summary["current_kubernetes_diagnostics"] = stalled.safe_diagnostics(snapshot)
        self.save()
        return snapshot

    def authority(self):
        account = self.az_json("account", "show", "--query", "{id:id}")
        require(str(account.get("id", "")).lower() == base.SUBSCRIPTION,
                "Current Azure subscription changed")
        group = self.az_json("group", "show", "--name", base.RESOURCE_GROUP)
        clusters = self.az_json("aks", "list", "--resource-group", base.RESOURCE_GROUP,
                                "--query", base.CLUSTER_QUERY)
        members = self.az_json("fleet", "member", "list", "--resource-group", base.RESOURCE_GROUP,
                               "--fleet-name", "clustermesh-flt")
        self.summary["scope_diagnostics"] = stalled.safe_diagnostics({
            "account": account, "resource_group": group, "clusters": clusters, "members": members,
        })
        self.save()
        selected, identities = prepared.validate_scope(self.args, group, clusters, members)
        require(all(row.get("provisioningState") == "Succeeded"
                    and (row.get("powerState") or {}).get("code") == "Running" for row in clusters),
                "All 100 preserved AKS resources must remain Running/Succeeded")
        require(selected["name"] == base.CLUSTER
                and selected["nodeResourceGroup"].lower() == base.NODE_GROUP,
                "Selected cluster or node resource group changed")
        node_group = self.az_json("group", "show", "--name", base.NODE_GROUP)
        require(
            prepared.resource_equal(node_group.get("id"),
                                    f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.NODE_GROUP}")
            and prepared.resource_equal(node_group.get("managedBy"), selected["id"])
            and str(node_group.get("location", "")).lower() == base.REGION,
            "Node resource-group ownership or region changed",
        )
        prepared.require_lease(node_group, self.args.timeout_seconds)
        pin = {
            "clusters": {row["tags"]["role"]: row["id"].lower() for row in clusters},
            "identities": sorted(identities, key=lambda row: row["role"]),
        }
        require(self.authority_pin is None or pin == self.authority_pin,
                "Preserved AKS/Fleet identity map drifted")
        self.authority_pin = pin
        self.summary.update(authoritative_identities=pin["identities"], fleet_connected=True,
                            lease_checked_at=workers.utc_now())
        self.save()
        return selected

    def _journal_inventory(self):
        payload = self.kube("-n", "kube-system", "get", "configmaps", "-o", "json")
        rows = mocks._items(payload, "ConfigMap inventory")
        selected = {}
        for row in rows:
            metadata = row.get("metadata") or {}
            name = metadata.get("name")
            labels = metadata.get("labels") or {}
            if name == JOURNAL or name in HISTORICAL_JOURNALS or "journal" in str(name).lower() or any(
                    "journal" in str(key).lower() for key in labels):
                require(isinstance(name, str) and name not in selected,
                        "Operation journal names are ambiguous")
                selected[name] = {
                    "uid": uid(row), "resourceVersion": metadata.get("resourceVersion"),
                    "data": copy.deepcopy(row.get("data") or {}),
                    "deletionTimestamp": metadata.get("deletionTimestamp"),
                    "ownerReferences": copy.deepcopy(metadata.get("ownerReferences") or []),
                }
        return selected

    def journals(self, *, allow_own=False):
        current = self._journal_inventory()
        own = current.pop(JOURNAL, None)
        if allow_own:
            require(own is not None, "Owned monitoring journal disappeared")
        else:
            require(own is None, "Existing monitoring journal blocks replay or adoption")
        retirement_row = current.get(retirement.JOURNAL)
        require(HISTORICAL_JOURNALS <= set(current)
                and retirement_row is not None and retirement_row["uid"] == RETIREMENT_JOURNAL_UID,
                "Retained build 80001 retirement journal changed")
        require(all(row["uid"] and row["resourceVersion"] and not row["deletionTimestamp"]
                    and not row["ownerReferences"] for row in current.values()),
                "An existing recovery journal is deleting or malformed")
        if self.old_journals is None:
            self.old_journals = copy.deepcopy(current)
        require(current == self.old_journals,
                "An existing journal UID/resourceVersion/data/lifecycle changed")
        if allow_own:
            self.owned_journal()

    def journal_data(self):
        return {
            "owner": JOURNAL_OWNER, "token": self.token,
            "retirement_build_id": str(RETIREMENT_BUILD),
            "retirement_tree_sha256": base.digest(self.bundle["hashes"]),
            "plan_sha256": baseline.PLAN_SHA,
            "record": json.dumps({
                "status": self.summary["status"],
                "pool_add": self.summary["pool_add"],
                "old_pool_delete": self.summary["old_pool_delete"],
                "new_identity": self.summary.get("new_prom_identity"),
            }, sort_keys=True, separators=(",", ":")),
        }

    def owned_journal(self):
        row = self.kube("-n", "kube-system", "get", "configmap", JOURNAL, "-o", "json")
        metadata = row.get("metadata") or {}
        require(
            uid(row) == self.journal_uid and metadata.get("name") == JOURNAL
            and metadata.get("namespace") == "kube-system"
            and metadata.get("resourceVersion") == self.journal_resource_version
            and not metadata.get("deletionTimestamp")
            and not metadata.get("ownerReferences") and row.get("data") == self.journal_data_pin,
            "Owned monitoring journal UID/data/lifecycle changed",
        )
        return row

    def raw_write(self, command):
        require(self.args.execute, "Plan mode cannot mutate")
        allowed = (
            command[:6] == ["kubectl", "-n", "kube-system", "create", "configmap", JOURNAL]
            or command[:6] == ["kubectl", "-n", "kube-system", "patch", "configmap", JOURNAL]
            or command == pool_add_command()
            or command == pool_delete_command()
        )
        require(allowed, "Write escaped the new-journal/one-add/one-empty-delete whitelist")
        self.summary["mutation_started"] = True
        self.save()
        timeout = PROVIDER_SUBMIT_SECONDS if command[0] == "az" else self.args.request_timeout_seconds
        return super().run(command, timeout)

    def acquire(self):
        self.journals()
        record = self.summary["journal"]
        require(record["attempted"] is False, "Monitoring journal creation cannot be repeated")
        record.update(attempted=True, accepted=None, ambiguous=True, requested_at=workers.utc_now())
        self.save()
        data = self.journal_data()
        output = self.raw_write([
            "kubectl", "-n", "kube-system", "create", "configmap", JOURNAL,
            *[f"--from-literal={key}={value}" for key, value in data.items()], "-o", "json",
        ])
        row = workers.parse_json(output, "exclusive monitoring journal creation")
        require(uid(row) and row.get("data") == data,
                "Monitoring journal creation was not authoritatively confirmed")
        self.journal_uid = uid(row)
        self.journal_resource_version = row["metadata"].get("resourceVersion")
        self.journal_data_pin = data
        require(self.journal_resource_version, "Monitoring journal resourceVersion is missing")
        self.owned_journal()
        record.update(uid=self.journal_uid, accepted=True, ambiguous=False,
                      accepted_at=workers.utc_now())
        self.persist_journal()

    def persist_journal(self):
        self.unchanged_inputs()
        current = self.owned_journal()
        desired = self.journal_data()
        output = self.raw_write([
            "kubectl", "-n", "kube-system", "patch", "configmap", JOURNAL,
            "--type=json", "-p", json.dumps([
                {"op": "test", "path": "/metadata/uid", "value": self.journal_uid},
                {"op": "test", "path": "/metadata/resourceVersion",
                 "value": current["metadata"]["resourceVersion"]},
                {"op": "test", "path": "/data", "value": self.journal_data_pin},
                {"op": "add", "path": "/data", "value": desired},
            ]), "-o", "json",
        ])
        updated = workers.parse_json(output, "monitoring journal CAS")
        metadata = updated.get("metadata") or {}
        require(
            uid(updated) == self.journal_uid and updated.get("data") == desired
            and metadata.get("resourceVersion")
            and metadata["resourceVersion"] != self.journal_resource_version,
            "Monitoring journal CAS result is ambiguous",
        )
        self.journal_data_pin = copy.deepcopy(desired)
        self.journal_resource_version = metadata["resourceVersion"]
        self.owned_journal()
        self.save()

    def operation(self, pool_name, action=None):
        operation = self.az_json(
            "aks", "operation", "show-latest", "--resource-group", base.RESOURCE_GROUP,
            "--name", base.CLUSTER, "--nodepool-name", pool_name, "--query", base.OPERATION_QUERY,
        )
        require(isinstance(operation, dict) and operation.get("name") and not operation.get("errorCode"),
                f"{pool_name}: child agentPool operation is absent, failed, or ambiguous")
        start = base.timestamp(operation.get("startTime"), f"{pool_name} operation start")
        require(start <= datetime.now(timezone.utc), f"{pool_name}: operation starts in the future")
        terminal = operation.get("status") == "Succeeded"
        if terminal:
            end = base.timestamp(operation.get("endTime"), f"{pool_name} operation end")
            require(start <= end <= datetime.now(timezone.utc),
                    f"{pool_name}: operation completion timestamp is invalid")
        if action is None:
            require(terminal, f"{pool_name}: pre-existing operation is not quiescent")
            prior = self.initial_operations.setdefault(pool_name, operation["name"])
            require(prior == operation["name"], f"{pool_name}: unrelated operation appeared")
            return operation
        require(_action_complete(action), f"{pool_name}: provider observation lacks an accepted request")
        previous = action.get("previous_operation_name")
        if operation["name"] == previous:
            require(terminal, f"{pool_name}: prior operation became busy")
            return operation
        allowed = CREATE_OPERATION_TYPES if pool_name == NEW_POOL else DELETE_OPERATION_TYPES
        require(
            operation.get("operationType") in allowed
            and operation.get("status") in {"Succeeded", "InProgress", "Running", *CREATING, *DELETING}
            and start >= base.timestamp(action["requested_at"], f"{pool_name} request")
            and (not action.get("operation_name") or action["operation_name"] == operation["name"]),
            f"{pool_name}: operation is not causally bound to the sole accepted request",
        )
        action["operation_name"] = operation["name"]
        self.save()
        return operation

    def _validate_existing_models(self, pools, vmsses):
        by_pool = {row.get("name"): row for row in pools}
        scales = {workers.vmss_pool_name(row): row for row in vmsses}
        for name in ("default", "cniv5"):
            pool = by_pool.get(name)
            scale = scales.get(name)
            expected_count = 1 if name == "default" else 2
            expected_sku = "Standard_D8_v3" if name == "default" else modern.VM_SIZE
            require(
                pool and scale and pool.get("count") == expected_count
                and pool.get("mode") == "System" and pool.get("vmSize") == expected_sku
                and pool.get("enableAutoScaling") is False
                and pool.get("provisioningState") == "Succeeded"
                and (pool.get("powerState") or {}).get("code") == "Running"
                and prepared.pool_configuration(pool) == self.bundle["pool_pins"][name],
                f"{name}: protected pool count/configuration/health changed",
            )
            model = copy.deepcopy(scale)
            model.pop("provisioningState", None)
            capacity = model["sku"].pop("capacity", None)
            require(
                capacity == expected_count and scale.get("provisioningState") == "Succeeded"
                and model == self.bundle["vmss_pins"][name],
                f"{name}: protected VMSS identity/model/capacity changed",
            )
            instances = self.az_json("vmss", "list-instances", "--resource-group", base.NODE_GROUP,
                                     "--name", scale["name"], "--query", base.VM_QUERY)
            require(isinstance(instances, list) and len(instances) == expected_count,
                    f"{name}: protected VM inventory changed")
            for instance in instances:
                node_name = instance.get("computerName")
                pin = self.bundle["instances"].get(node_name)
                require(
                    pin and pin["vm_id"] == instance.get("vmId")
                    and pin["instance_id"] == str(instance.get("instanceId"))
                    and prepared.resource_equal(instance.get("id"), pin["resource_id"])
                    and instance.get("provisioningState") == "Succeeded"
                    and instance.get("latestModelApplied") is True,
                    f"{name}: protected VM identity/model changed",
                )
                view = self.az_json(
                    "vmss", "get-instance-view", "--resource-group", base.NODE_GROUP,
                    "--name", scale["name"], "--instance-id", str(instance["instanceId"]),
                    "--query", stalled.VIEW_QUERY,
                )
                require(
                    {row.get("code") for row in view.get("statuses") or []}
                    == {"ProvisioningState/succeeded", "PowerState/running"}
                    and stalled.guest_state(view, max_age_seconds=300) == "ready"
                    and stalled.extensions_ready(view),
                    f"{node_name}: protected guest or extensions changed",
                )
            self.operation(name)

    def _validate_old_zero(self, pools, vmsses, *, absent=False, deleting=False):
        by_pool = {row.get("name"): row for row in pools}
        scales = {workers.vmss_pool_name(row): row for row in vmsses}
        pool, scale = by_pool.get(OLD_POOL), scales.get(OLD_POOL)
        if absent:
            require(pool is None and scale is None, "Old prompool provider objects still exist")
            return
        if deleting:
            require(pool is not None or scale is not None,
                    "Old prompool deletion state cannot validate an already absent pair")
        else:
            require(pool is not None and scale is not None,
                    "Old empty prompool/VMSS disappeared unexpectedly")
        allowed = {"Succeeded", *DELETING} if deleting else {"Succeeded"}
        if pool is not None:
            require(
                pool.get("count") == 0 and pool.get("mode") == "User"
                and pool.get("vmSize") == "Standard_D8_v3"
                and pool.get("provisioningState") in allowed
                and prepared.pool_configuration(pool) == self.bundle["pool_pins"][OLD_POOL],
                "Old prompool is not the pinned empty object",
            )
        if scale is not None:
            model = copy.deepcopy(scale)
            model.pop("provisioningState", None)
            capacity = model["sku"].pop("capacity", None)
            require(
                capacity == 0 and scale.get("provisioningState") in allowed
                and model == self.bundle["vmss_pins"][OLD_POOL],
                "Old prompool VMSS is not the pinned zero-capacity object",
            )
            instances = self.az_json("vmss", "list-instances", "--resource-group", base.NODE_GROUP,
                                     "--name", scale["name"], "--query", base.VM_QUERY)
            require(instances == [], "Old prompool has a VM and cannot be deleted")
        if not deleting:
            self.operation(OLD_POOL)

    def _validate_new_model(self, pools, vmsses, operation):
        by_pool = {row.get("name"): row for row in pools}
        scales = {workers.vmss_pool_name(row): row for row in vmsses}
        pool, scale = by_pool.get(NEW_POOL), scales.get(NEW_POOL)
        if pool is None or scale is None:
            require(self.phase == "creating"
                    and (operation is None or operation.get("status") != "Succeeded"),
                    "Accepted promv5 creation lost its provider objects")
            return False
        transitional = operation is None or operation.get("status") != "Succeeded"
        states = {"Succeeded", *CREATING} if transitional else {"Succeeded"}
        require(
            prepared.resource_equal(pool.get("id"),
                                    f"{self.authority_pin['clusters'][base.ROLE]}/agentPools/{NEW_POOL}")
            and pool.get("name") == NEW_POOL and pool.get("count") == 1
            and pool.get("mode") == "User" and pool.get("vmSize") == modern.VM_SIZE
            and pool.get("maxPods") == 250 and pool.get("osType") == "Linux"
            and pool.get("osSku") == "Ubuntu" and pool.get("osDiskType") == "Managed"
            and _disk_size(pool) == 256 and pool.get("kubeletDiskType") == "OS"
            and pool.get("nodeLabels") == {"prometheus": "true"}
            and pool.get("nodeTaints") is None and pool.get("enableAutoScaling") is False
            and pool.get("enableFips") is False and pool.get("enableEncryptionAtHost") is False
            and pool.get("enableNodePublicIp") is False
            and pool.get("availabilityZones") is None and pool.get("kubeletConfig") is None
            and pool.get("linuxOsConfig") is None
            and prepared.resource_equal(pool.get("vnetSubnetId"), modern.POOL_SETTINGS["vnetSubnetId"])
            and prepared.resource_equal(pool.get("podSubnetId"), modern.POOL_SETTINGS["podSubnetId"])
            and isinstance(pool.get("upgradeSettings"), dict)
            and pool["upgradeSettings"].get("maxSurge") == "10%"
            and str(pool["upgradeSettings"].get("maxUnavailable")) == "0"
            and pool.get("orchestratorVersion") == PATCH
            and pool.get("currentOrchestratorVersion") in ((PATCH, None) if transitional else (PATCH,))
            and pool.get("provisioningState") in states
            and (pool.get("powerState") or {}).get("code") == "Running",
            "promv5 does not match the exact approved User/DSv5/Ubuntu/managed-disk contract",
        )
        name = scale.get("name")
        require(
            isinstance(name, str) and workers.vmss_pool_name(scale) == NEW_POOL
            and prepared.resource_equal(
                scale.get("id"),
                f"/subscriptions/{base.SUBSCRIPTION}/resourceGroups/{base.NODE_GROUP}"
                f"/providers/Microsoft.Compute/virtualMachineScaleSets/{name}",
            ) and str(scale.get("location", "")).lower() == base.REGION
            and scale.get("orchestrationMode") == "Uniform"
            and (scale.get("sku") or {}).get("name") == modern.VM_SIZE
            and (scale.get("sku") or {}).get("capacity") == 1
            and scale.get("provisioningState") in states,
            "promv5 VMSS ownership/SKU/capacity/state is invalid",
        )
        require(not self.new_vmss or self.new_vmss == name, "Observed promv5 VMSS identity changed")
        self.new_vmss = name
        model = self.az_json("vmss", "show", "--resource-group", base.NODE_GROUP,
                             "--name", name, "--query", VMSS_MODEL_QUERY)
        self.summary["new_prom_provider_diagnostics"] = stalled.safe_diagnostics({
            "pool": pool, "vmss": scale, "model": model, "operation": operation,
        })
        self.save()
        disk = model.get("osDisk") or {}
        image = model.get("imageReference") or {}
        require(
            prepared.resource_equal(model.get("id"), scale["id"])
            and disk.get("osType") == "Linux" and _disk_size(disk) == 256
            and disk.get("diffDiskOption") is None
            and (disk.get("managedDisk") or {}).get("storageAccountType")
            in {"Standard_LRS", "StandardSSD_LRS", "Premium_LRS"}
            and isinstance(image, dict) and image,
            "promv5 VMSS does not use the required Linux managed 256GiB OS disk",
        )
        require(not pool.get("nodeImageVersion")
                or modern.image_matches_pool(image, pool["nodeImageVersion"]),
                "promv5 VMSS image differs from the AKS Ubuntu node image")
        scale_view = self.az_json(
            "vmss", "get-instance-view", "--resource-group", base.NODE_GROUP,
            "--name", name, "--query", base.SCALE_VIEW_QUERY,
        )
        self.summary["new_prom_provider_diagnostics"]["scale_view"] = stalled.safe_diagnostics(scale_view)
        self.save()
        scale_codes = [
            row.get("code") for row in [
                *(scale_view.get("statuses") or []), *(scale_view.get("virtualMachines") or []),
            ] if isinstance(row, dict)
        ]
        scale_statuses = modern.replacement.status_rows(scale_view, "promv5 VMSS")
        scale_counts = scale_view.get("virtualMachines")
        require(not any("failed" in str(code).lower() for code in scale_codes),
                "promv5 VMSS aggregate reports a terminal failure")
        instances = self.az_json("vmss", "list-instances", "--resource-group", base.NODE_GROUP,
                                 "--name", name, "--query", base.VM_QUERY)
        self.summary["new_prom_provider_diagnostics"]["instances"] = stalled.safe_diagnostics(instances)
        self.save()
        require(isinstance(instances, list) and len(instances) <= 1,
                "promv5 contains more than one VM")
        if not instances:
            require(transitional, "Succeeded promv5 has no VM")
            return False
        instance = instances[0]
        instance_id = str(instance.get("instanceId", ""))
        expected_name = f"{name}{int(instance_id):06d}" if instance_id.isdecimal() else ""
        initializing = transitional or instance.get("provisioningState") in CREATING
        require(
            instance_id.isdecimal() and str(int(instance_id)) == instance_id
            and prepared.resource_equal(instance.get("id"), f"{scale['id']}/virtualMachines/{instance_id}")
            and instance.get("provisioningState") in ({"Succeeded", *CREATING} if initializing else {"Succeeded"})
            and instance.get("latestModelApplied") in ({True, False, None} if initializing else {True})
            and (instance.get("computerName") is None or instance.get("computerName") == expected_name)
            and (instance.get("vmId") is None or maintenance.UUID_RE.fullmatch(instance["vmId"])),
            "promv5 VM initialization/identity is invalid",
        )
        complete_identity = instance.get("computerName") == expected_name and bool(instance.get("vmId"))
        if complete_identity:
            identity = {
                "pool_name": NEW_POOL, "vmss_name": name, "instance_id": instance_id,
                "node_name": expected_name, "vm_id": instance["vmId"],
                "resource_id": instance["id"].lower(),
                "provider_id": f"azure://{instance['id'].lower()}",
            }
            require(instance["vmId"] not in {RETIRED_VM_ID, *(row["vm_id"] for row in self.bundle["instances"].values())}
                    and expected_name not in self.bundle["real_pins"],
                    "promv5 reused a protected or retired VM/Node identity")
            require(self.new_identity is None or all(
                self.new_identity.get(key) == value for key, value in identity.items()),
                "Observed promv5 VM identity changed")
            self.new_identity = {**(self.new_identity or {}), **identity}
        view = self.az_json(
            "vmss", "get-instance-view", "--resource-group", base.NODE_GROUP,
            "--name", name, "--instance-id", instance_id, "--query", stalled.VIEW_QUERY,
        )
        self.summary["new_prom_provider_diagnostics"]["instance_view"] = stalled.safe_diagnostics(view)
        self.save()
        statuses = [row.get("code") for row in view.get("statuses") or [] if isinstance(row, dict)]
        extensions = view.get("extensions")
        codes = [*statuses, *[
            status.get("code") for extension in extensions or [] if isinstance(extension, dict)
            for status in extension.get("statuses") or [] if isinstance(status, dict)
        ]]
        require(not any("failed" in str(code).lower() for code in codes),
                "promv5 VM or extension reports a terminal failure")
        healthy = (
            operation is not None and operation.get("status") == "Succeeded"
            and pool.get("provisioningState") == scale.get("provisioningState") == "Succeeded"
            and isinstance(pool.get("nodeImageVersion"), str)
            and pool["nodeImageVersion"].startswith("AKSUbuntu-")
            and len(scale_statuses) == 1
            and scale_statuses[0].get("code") == "ProvisioningState/succeeded"
            and isinstance(scale_counts, list) and len(scale_counts) == 1
            and scale_counts[0].get("code") == "ProvisioningState/succeeded"
            and scale_counts[0].get("count") == 1
            and instance.get("provisioningState") == "Succeeded"
            and instance.get("latestModelApplied") is True and complete_identity
            and set(statuses) == {"ProvisioningState/succeeded", "PowerState/running"}
            and stalled.guest_state(view, initializing=not self.new_identity.get("operator_uid"),
                                    max_age_seconds=300) == "ready"
            and isinstance(extensions, list) and bool(extensions)
            and all(isinstance(row.get("statuses"), list) and row["statuses"]
                    and all(status.get("code") == "ProvisioningState/succeeded"
                            for status in row["statuses"]) for row in extensions)
        )
        self.summary["new_pool_receipt"] = {
            "resource_id": pool["id"], "configuration_sha256": base.digest(prepared.pool_configuration(pool)),
            "mode": pool["mode"], "count": pool["count"], "vm_size": pool["vmSize"],
            "os_disk_size_gib": _disk_size(pool), "kubernetes_patch": pool.get("currentOrchestratorVersion"),
            "node_image_version": pool.get("nodeImageVersion"),
        }
        self.save()
        return healthy

    def models(self):
        cluster_operation = self.az_json(
            "aks", "operation", "show-latest", "--resource-group", base.RESOURCE_GROUP,
            "--name", base.CLUSTER, "--query", base.OPERATION_QUERY,
        )
        require(
            isinstance(cluster_operation, dict) and cluster_operation.get("status") == "Succeeded"
            and cluster_operation.get("name") and not cluster_operation.get("errorCode"),
            "Top-level AKS operation is not quiescent",
        )
        cluster = self.az_json("aks", "show", "--resource-group", base.RESOURCE_GROUP,
                               "--name", base.CLUSTER)
        require(
            prepared.resource_equal(cluster.get("id"), self.authority_pin["clusters"][base.ROLE])
            and cluster.get("currentKubernetesVersion") == PATCH
            and cluster.get("kubernetesVersion") in (PATCH, "1.35"),
            "mesh-96 AKS identity or exact 1.35.7 patch changed",
        )
        pools = self.az_json("aks", "nodepool", "list", "--resource-group", base.RESOURCE_GROUP,
                             "--cluster-name", base.CLUSTER)
        vmsses = self.az_json("vmss", "list", "--resource-group", base.NODE_GROUP,
                              "--query", base.VMSS_QUERY)
        self.summary["arm_diagnostics"] = stalled.safe_diagnostics({
            "cluster_operation": cluster_operation, "cluster": cluster,
            "pools": pools, "vmsses": vmsses,
        })
        self.save()
        require(isinstance(pools, list) and isinstance(vmsses, list)
                and len({row.get("name") for row in pools}) == len(pools)
                and len({workers.vmss_pool_name(row) for row in vmsses}) == len(vmsses),
                "Pool/VMSS inventory is malformed or duplicated")
        self._validate_existing_models(pools, vmsses)
        names = {row.get("name") for row in pools}
        scale_names = {workers.vmss_pool_name(row) for row in vmsses}
        if self.phase == "plan":
            require(names == scale_names == {"default", "cniv5", OLD_POOL},
                    "Preflight requires exact default1/cniv5-2/empty-prompool layout")
            self._validate_old_zero(pools, vmsses)
            return False, pools, vmsses
        try:
            create_operation = self.operation(NEW_POOL, self.summary["pool_add"])
        except workers.ReconcileError as error:
            by_pool = {row.get("name"): row for row in pools}
            pending = by_pool.get(NEW_POOL)
            require(
                self.phase == "creating"
                and re.search(r"ResourceNotFound|OperationNotFound|\b404\b", str(error)) is not None
                and (pending is None or pending.get("provisioningState") in CREATING),
                str(error),
            )
            self.summary["pool_add"]["child_operation_pending"] = True
            self.save()
            create_operation = None
        new_ready = self._validate_new_model(pools, vmsses, create_operation)
        if self.phase == "creating":
            require({"default", "cniv5", OLD_POOL} <= names
                    <= {"default", "cniv5", OLD_POOL, NEW_POOL}
                    and {"default", "cniv5", OLD_POOL} <= scale_names
                    <= {"default", "cniv5", OLD_POOL, NEW_POOL},
                    "Only the causally created promv5 may appear")
            self._validate_old_zero(pools, vmsses)
        elif self.phase == "retiring":
            require({"default", "cniv5", NEW_POOL} <= names
                    <= {"default", "cniv5", OLD_POOL, NEW_POOL}
                    and {"default", "cniv5", NEW_POOL} <= scale_names
                    <= {"default", "cniv5", OLD_POOL, NEW_POOL},
                    "Only the old empty prompool may disappear")
            if OLD_POOL in names or OLD_POOL in scale_names:
                self._validate_old_zero(pools, vmsses, deleting=True)
        else:
            require(names == scale_names == {"default", "cniv5", NEW_POOL},
                    "Final provider layout still references old prompool")
        return new_ready, pools, vmsses

    def _no_retired_references(self, snapshot):
        nodes = mocks._items(snapshot["nodes"], "Node inventory")
        pods = mocks._items(snapshot["pods"], "Pod inventory")
        nncs = mocks._items(snapshot["nnc"], "NNC inventory")
        require(
            not any(
                row["metadata"].get("name") == RETIRED_NODE
                or uid(row) == RETIRED_NODE_UID
                or RETIRED_VM_ID.lower() in str((row.get("spec") or {}).get("providerID", "")).lower()
                for row in nodes
            ) and not any((row.get("spec") or {}).get("nodeName") == RETIRED_NODE for row in pods)
            and not any(row["metadata"].get("name") == RETIRED_NODE
                        or any(owner.get("uid") == RETIRED_NODE_UID
                               for owner in row["metadata"].get("ownerReferences") or [])
                        for row in nncs),
            "The natively retired default1 Node/VM/NNC reference reappeared",
        )

    def _old_references_absent(self, snapshot):
        nodes = mocks._items(snapshot["nodes"], "Node inventory")
        pods = mocks._items(snapshot["pods"], "Pod inventory")
        nncs = mocks._items(snapshot["nnc"], "NNC inventory")
        require(
            not any(mocks._node_pool_name(row) == OLD_POOL
                    or row["metadata"].get("name", "").startswith(OLD_VMSS) for row in nodes)
            and not any(str((row.get("spec") or {}).get("nodeName", "")).startswith(OLD_VMSS)
                        for row in pods)
            and not any(row["metadata"].get("name", "").startswith(OLD_VMSS)
                        or any(str(owner.get("name", "")).startswith(OLD_VMSS)
                               for owner in row["metadata"].get("ownerReferences") or [])
                        for row in nncs),
            "Old empty prompool still has Node/NNC/Pod references",
        )

    def guard(self, snapshot, *, require_new=False):
        self.unchanged_inputs()
        self._no_retired_references(snapshot)
        self._old_references_absent(snapshot)
        require(stalled.controllers_pin(snapshot["controllers"]) == self.bundle["controllers"],
                "Controller UID/functional-spec contracts changed")
        require(base.frozen_pdbs(snapshot) == self.bundle["pdbs"],
                "PDB UID/spec contracts changed")
        maintenance._require_all_kwok_ready(snapshot["nodes"], self.bundle["receipt"]["preserved_kwok_uids"])
        agents = maintenance._agent_map(snapshot["pods"])
        require(set(agents) == set(self.bundle["receipt"]["current_mock_uids"]),
                "Current mock names changed")
        for name, expected_uid in self.bundle["receipt"]["current_mock_uids"].items():
            pod = agents[name]
            require(
                uid(pod) == expected_uid and base.pod_ready(pod)
                and retirement.semantic_pod_spec(pod["spec"]) == self.bundle["mock_specs"][name],
                f"{name}: current mock UID/spec/readiness changed",
            )
        nodes = maintenance._real_node_map(snapshot["nodes"])
        expected = set(self.bundle["real_pins"])
        extras = set(nodes) - expected
        require(expected <= set(nodes) and len(extras) <= (1 if require_new else 0),
                "Protected real Node inventory changed or an unrelated Node appeared")
        for name in extras:
            node = nodes[name]
            maintenance._validate_real_node_scope(
                node, subscription=base.SUBSCRIPTION, node_resource_group=base.NODE_GROUP,
            )
            candidate = {
                "node_name": name, "node_uid": uid(node),
                "provider_id": str(node.get("spec", {}).get("providerID", "")).lower(),
                "pool_name": mocks._node_pool_name(node),
            }
            require(
                candidate["node_uid"] and candidate["pool_name"] == NEW_POOL
                and not node["metadata"].get("deletionTimestamp")
                and (self.candidate_node is None or candidate == self.candidate_node),
                "Only one stable Node from the owned promv5 creation may initialize",
            )
            self.candidate_node = candidate
        nnc_rows = mocks._items(snapshot["nnc"], "NNC inventory")
        raw_nnc = {row["metadata"]["name"]: row for row in nnc_rows}
        candidate_names = {self.new_identity["node_name"]} if self.new_identity else set()
        require(len(raw_nnc) == len(nnc_rows) and set(raw_nnc) <= expected | extras | candidate_names,
                "NNC inventory contains duplicate or unrelated allocations")
        networks = {name: qualification.concrete_network(row) for name, row in raw_nnc.items() if name in expected}
        for name, pin in self.bundle["real_pins"].items():
            node = nodes[name]
            network = networks.get(name)
            require(
                uid(node) == pin["node_uid"] and base.node_boot(node) == pin["boot_id"]
                and str(node["spec"].get("providerID", "")).lower() == pin["provider_id"]
                and mocks._node_pool_name(node) == pin["pool_name"]
                and stalled.logical_node(node) == pin["logical_node"]
                and (node.get("status", {}).get("nodeInfo") or {}).get("kubeletVersion") == f"v{PATCH}"
                and retirement.HOLD_KEY not in maintenance._annotations(node)
                and not any(row.get("key") == retirement.HOLD_KEY
                            for row in maintenance._taints(node))
                and workers.node_is_ready(node) and not node["metadata"].get("deletionTimestamp")
                and network and network["uid"] == pin["nnc_uid"]
                and network["node_uid"] == pin["node_uid"]
                and network["network_container_id"] == pin["network_container_id"]
                and not raw_nnc[name]["metadata"].get("deletionTimestamp"),
                f"{name}: protected Node/boot/provider/NNC identity or readiness changed",
            )
            resident = {
                pod.get("status", {}).get("podIP") for pod in snapshot["pods"]["items"]
                if pod.get("spec", {}).get("nodeName") == name
                and not pod.get("spec", {}).get("hostNetwork")
                and pod.get("status", {}).get("podIP")
                and not pod.get("metadata", {}).get("deletionTimestamp")
            }
            require(resident <= set(network["ip_addresses"]),
                    f"{name}: protected NNC lost a resident Pod IP")
            previous = self.existing_nnc[name]
            require(
                network["version"] >= previous["nnc_version"]
                and (network["version"] > previous["nnc_version"]
                     or network["ip_addresses"] == previous["nnc_ip_addresses"]),
                f"{name}: protected NNC version/allocation changed without a version advance",
            )
            previous["nnc_version"] = network["version"]
            previous["nnc_ip_addresses"] = copy.deepcopy(network["ip_addresses"])
        if not self.daemonsets:
            self.daemonsets = maintenance._derive_applicable_daemonsets(snapshot["pods"], [SOURCE_NODE])
            require({name for _, name, _ in self.daemonsets} >= {"cilium", "azure-cns"},
                    "Healthy source0 lacks applicable Cilium/CNS DaemonSets")
        for name in expected:
            require(self.daemonsets <= maintenance._healthy_system_daemonsets_on_node(snapshot["pods"], name),
                    f"{name}: applicable system DaemonSet readiness changed")
        operator = _operator(snapshot["pods"])
        require(retirement.semantic_pod_spec(operator["spec"]) == self.bundle["operator_spec"],
                "The existing operator Pod semantic spec changed")
        grafana_rows = [
            row for row in snapshot["pods"]["items"] if uid(row) == self.bundle["grafana"]["uid"]
        ]
        require(
            len(grafana_rows) == 1 and base.pod_ready(grafana_rows[0])
            and grafana_rows[0]["metadata"]["name"] == self.bundle["grafana"]["name"]
            and grafana_rows[0]["spec"].get("nodeName") == SOURCE_NODE
            and retirement.semantic_pod_spec(grafana_rows[0]["spec"])
            == self.bundle["grafana"]["semantic_spec"],
            "The healthy source0 Grafana UID/spec/readiness/placement changed",
        )
        operator_ready = False
        if not require_new:
            require(not operator["spec"].get("nodeName")
                    and operator.get("status", {}).get("phase") == "Pending"
                    and not operator.get("status", {}).get("podIP"),
                    "Preflight operator is no longer naturally Pending and unassigned")
        else:
            operator_ready = self.new_monitoring_ready(snapshot, nodes, raw_nnc, networks, operator)
        self.summary.update(
            current_mock_uids={name: uid(pod) for name, pod in agents.items()},
            preserved_kwok_uids=copy.deepcopy(self.bundle["receipt"]["preserved_kwok_uids"]),
            observed_node_ids={name: uid(node) for name, node in nodes.items()},
            current_mock_ready=sum(base.pod_ready(pod) for pod in agents.values()),
            kwok_ready=100,
        )
        self.save()
        return operator_ready

    def pending_new(self, reason):
        self.summary["new_prom_wait_reason"] = reason
        require(not (self.new_identity or {}).get("operator_uid"),
                f"Qualified promv5 monitoring regressed: {reason}")
        return False

    def new_monitoring_ready(self, snapshot, nodes, raw_nnc, protected_networks, operator):
        if not self.new_identity or self.new_identity["node_name"] not in nodes:
            return self.pending_new("waiting for the owned VM and Kubernetes Node registration")
        name = self.new_identity["node_name"]
        node = nodes[name]
        info = node.get("status", {}).get("nodeInfo") or {}
        require(
            uid(node) not in {row["node_uid"] for row in self.bundle["real_pins"].values()}
            and prepared.resource_equal(node["spec"].get("providerID"), self.new_identity["provider_id"])
            and self.candidate_node is not None and self.candidate_node["node_uid"] == uid(node)
            and mocks._node_pool_name(node) == NEW_POOL
            and (node["metadata"].get("labels") or {}).get("prometheus") == "true"
            and info.get("kubeletVersion") in (None, "", f"v{PATCH}")
            and info.get("operatingSystem") in (None, "", "linux")
            and (not info.get("osImage") or str(info["osImage"]).startswith("Ubuntu"))
            and not node["spec"].get("unschedulable"),
            "promv5 Node identity/label/version/OS/schedulability is invalid",
        )
        node_uid = uid(node)
        require(not self.new_identity.get("node_uid") or self.new_identity["node_uid"] == node_uid,
                "Observed promv5 Node UID changed")
        self.new_identity["node_uid"] = node_uid
        boot = info.get("bootID")
        if boot:
            require(maintenance.UUID_RE.fullmatch(boot)
                    and boot not in {row["boot_id"] for row in self.bundle["real_pins"].values()}
                    and (not self.new_identity.get("boot_id") or self.new_identity["boot_id"] == boot),
                    "Observed promv5 boot ID changed or reused a protected identity")
            self.new_identity["boot_id"] = boot
        blocking = [row for row in maintenance._taints(node) if row.get("effect") in ("NoSchedule", "NoExecute")]
        require(all(row.get("key") in {
            *stalled.HEALTH_TAINTS, "node.cilium.io/agent-not-ready", "node.cloudprovider.kubernetes.io/uninitialized",
        } for row in blocking), "promv5 has an unexpected custom scheduling taint")
        if not workers.node_is_ready(node) or blocking or not all(
            info.get(key) for key in ("bootID", "kubeletVersion", "operatingSystem", "osImage")
        ):
            return self.pending_new("waiting for new Node startup readiness and startup-taint removal")
        raw = raw_nnc.get(name)
        if raw is None:
            return self.pending_new("waiting for the new NodeNetworkConfig")
        require(uid(raw) and not raw.get("metadata", {}).get("deletionTimestamp")
                and (not self.new_identity.get("nnc_uid") or self.new_identity["nnc_uid"] == uid(raw)),
                "Observed promv5 NNC UID changed or is deleting")
        self.new_identity["nnc_uid"] = uid(raw)
        owners = raw.get("metadata", {}).get("ownerReferences") or []
        if not owners:
            return self.pending_new("waiting for new NNC Node ownership")
        require(base.controller_owner(raw, "Node").get("uid") == node_uid,
                "promv5 NNC belongs to a different Node")
        status = raw.get("status") or {}
        containers = status.get("networkContainers")
        if containers in (None, []) or status.get("assignedIPCount") in (None, 0):
            return self.pending_new("waiting for concrete new NNC addresses")
        network = qualification.concrete_network(raw)
        require(
            network["node_uid"] == node_uid
            and network["network_container_id"] not in {
                row["network_container_id"] for row in self.bundle["real_pins"].values()
            }
            and not set(network["ip_addresses"]).intersection(
                address for row in protected_networks.values() for address in row["ip_addresses"]
            ), "promv5 NNC ownership/container/IP allocation conflicts with a protected worker",
        )
        resident = {
            pod.get("status", {}).get("podIP") for pod in snapshot["pods"]["items"]
            if pod.get("spec", {}).get("nodeName") == name
            and not pod.get("spec", {}).get("hostNetwork")
            and pod.get("status", {}).get("podIP") and not pod.get("metadata", {}).get("deletionTimestamp")
        }
        require(resident <= set(network["ip_addresses"]),
                "promv5 NNC allocation does not retain every resident non-host-network Pod IP")
        if self.new_network is not None:
            require(all(network[key] == self.new_network[key] for key in ("uid", "node_uid", "network_container_id"))
                    and network["version"] >= self.new_network["version"]
                    and (network["version"] > self.new_network["version"]
                         or network["ip_addresses"] == self.new_network["ip_addresses"]),
                    "promv5 NNC identity/version/allocation changed without a version advance")
        self.new_network = copy.deepcopy(network)
        if network["assigned_ip_count"] < 16:
            return self.pending_new("waiting for the full initial concrete IP allocation")
        if not self.daemonsets <= maintenance._healthy_system_daemonsets_on_node(snapshot["pods"], name):
            return self.pending_new("waiting for new Cilium/CNS/system DaemonSet readiness")
        require(operator["spec"].get("nodeName") in (None, "", name),
                "The protected operator was assigned outside the dedicated monitoring worker")
        if operator["spec"].get("nodeName") != name or not base.pod_ready(operator):
            return self.pending_new("waiting for the existing operator to become Ready naturally")
        require(operator["status"]["podIP"] in network["ip_addresses"],
                "Ready operator Pod IP is outside its concrete NNC allocation")
        self.new_identity.update(network_container_id=network["network_container_id"],
                                 operator_uid=OPERATOR_UID, operator_pod_ip=operator["status"]["podIP"])
        self.summary["new_prom_identity"] = copy.deepcopy(self.new_identity)
        self.summary["new_prom_wait_reason"] = ""
        return True

    def capacity(self):
        usage = self.az_json_retry("vm", "list-usage", "--location", base.REGION,
                                   "--query", modern.capacity.USAGE_QUERY)
        self.summary["capacity_diagnostics"] = {"usage": stalled.safe_diagnostics(usage)}
        self.save()
        require(isinstance(usage, list), "Regional quota response is malformed")
        counters = {}
        for name in (modern.QUOTA_FAMILY, "cores"):
            rows = [row for row in usage if row.get("name") == name]
            require(len(rows) == 1, f"Regional quota lacks exactly one {name} counter")
            used, limit = (modern.capacity.quota_counter(rows[0].get(key))
                           for key in ("currentValue", "limit"))
            counters[name] = {"currentValue": used, "limit": limit, "remaining": limit - used}
        require(all(row["remaining"] >= 8 for row in counters.values()),
                "Fresh DSv5/general regional quota headroom is below 8 cores")
        rows = self.az_json_retry(
            "vm", "list-skus", "--location", base.REGION, "--resource-type", "virtualMachines",
            "--size", modern.VM_SIZE, "--all", "--query", modern.SKU_QUERY,
        )
        self.summary["capacity_diagnostics"]["sku"] = stalled.safe_diagnostics(rows)
        self.save()
        require(isinstance(rows, list) and len(rows) == 1, "Exact Standard_D8s_v5 SKU read is ambiguous")
        sku = rows[0]
        caps = {row.get("name"): row.get("value") for row in sku.get("capabilities") or []}
        require(
            sku.get("name") == modern.VM_SIZE and sku.get("family") == modern.QUOTA_FAMILY
            and sku.get("resourceType") == "virtualMachines"
            and base.REGION in [str(row).lower() for row in sku.get("locations") or []]
            and sku.get("restrictions") == [] and caps.get("vCPUs") == "8"
            and caps.get("MemoryGB") == "32" and caps.get("PremiumIO") == "True",
            "Standard_D8s_v5 is unavailable, restricted, or has unexpected capabilities",
        )
        self.summary["capacity"] = {
            "checked_at": workers.utc_now(), "required_cores": 8,
            "counters": counters, "sku": modern.VM_SIZE,
        }
        self.save()

    def memory_headroom(self):
        require(self.new_identity and self.new_identity.get("operator_uid") == OPERATOR_UID,
                "Operator readiness is required before memory proof")
        name = self.new_identity["node_name"]
        snapshot = self.snapshot()
        self.guard(snapshot, require_new=True)
        node = maintenance._real_node_map(snapshot["nodes"])[name]
        metric = self.kube("get", f"--raw=/apis/metrics.k8s.io/v1beta1/nodes/{name}")
        require(maintenance._headroom_ok(
            node, metric, threshold_percent=85,
            effective_reserved_memory_bytes=PROM_MEMORY_RESERVE + MEMORY_SAFETY_RESERVE,
            next_memory_bytes=0,
        ), "promv5 cannot safely reserve the campaign's 16Gi native Prometheus budget")
        active = [
            pod for pod in snapshot["pods"]["items"]
            if pod.get("spec", {}).get("nodeName") == name
            and pod.get("status", {}).get("phase") not in ("Succeeded", "Failed")
        ]
        allocatable = node.get("status", {}).get("allocatable") or {}
        require(len(active) + 5 <= int(allocatable.get("pods", 0)),
                "promv5 lacks safe Pod slots for the later native Prometheus resources")
        self.summary["prometheus_capacity_reserve"] = {
            "checked_at": workers.utc_now(), "threshold_percent": 85,
            "reserved_memory_bytes": PROM_MEMORY_RESERVE,
            "safety_memory_bytes": MEMORY_SAFETY_RESERVE,
            "not_a_scheduler_binding_or_resource_reservation": True,
        }
        self.save()

    def preflight(self):
        snapshot = self.snapshot()
        self.authority()
        self.journals(allow_own=bool(self.journal_uid))
        self.models()
        self.guard(snapshot)
        self.capacity()
        self.unchanged_inputs()
        self.summary.update(plan_valid=True, status="planned-read-only")
        self.save()

    def submit(self, key, command):
        action = self.summary[key]
        require(not action["attempted"] and self.journal_uid,
                f"{key}: duplicate or unjournaled provider request is forbidden")
        action.update(
            attempted=True, submission_started=False, accepted=None, ambiguous=True,
            requested_at=workers.utc_now(),
            previous_operation_name=self.initial_operations.get(
                NEW_POOL if key == "pool_add" else OLD_POOL),
            command=copy.deepcopy(command),
        )
        self.persist_journal()
        self.unchanged_inputs()
        self.authority()
        self.journals(allow_own=True)
        provider_ready, pools, vmsses = self.models()
        if key == "pool_add":
            self.guard(self.snapshot())
            self.capacity()
        else:
            self._validate_old_zero(pools, vmsses)
            require(provider_ready and self.guard(self.snapshot(), require_new=True),
                    "Fresh provider and operator readiness are required before empty-pool deletion")
        action["submission_started"] = True
        action["submission_started_at"] = workers.utc_now()
        self.persist_journal()
        print(f"{workers.utc_now()}: submitting the sole {key} request", flush=True)
        self.raw_write(command)
        action.update(accepted=True, ambiguous=True, accepted_at=workers.utc_now())
        self.persist_journal()
        action["ambiguous"] = False
        try:
            self.persist_journal()
        except EXPECTED_ERRORS:
            action["ambiguous"] = True
            self.save()
            raise

    def wait_new_ready(self):
        self.phase = "creating"
        while True:
            self.unchanged_inputs()
            snapshot = self.snapshot()
            self.authority()
            self.journals(allow_own=True)
            provider_ready, _, _ = self.models()
            operator_ready = self.guard(snapshot, require_new=True)
            print(f"{workers.utc_now()}: promv5 provider_ready={provider_ready} operator_ready={operator_ready}"
                  f" {self.summary.get('new_prom_wait_reason', '')}", flush=True)
            if provider_ready and operator_ready:
                self.memory_headroom()
                self.summary["status"] = "promv5-and-existing-operator-ready"
                self.persist_journal()
                return
            require(time.monotonic() < self.work_deadline,
                    "promv5/VM/guest/extensions/Node/NNC/DaemonSets/operator exceeded the bounded wait")
            time.sleep(min(POLL_SECONDS, self.remaining_seconds(POLL_SECONDS)))

    def wait_old_absent(self):
        self.phase = "retiring"
        while True:
            self.unchanged_inputs()
            snapshot = self.snapshot()
            self.authority()
            self.journals(allow_own=True)
            provider_ready, pools, vmsses = self.models()
            operator_ready = self.guard(snapshot, require_new=True)
            old_absent = (
                OLD_POOL not in {row.get("name") for row in pools}
                and OLD_POOL not in {workers.vmss_pool_name(row) for row in vmsses}
            )
            action = self.summary["old_pool_delete"]
            operation = None
            try:
                operation = self.operation(OLD_POOL, action)
            except workers.ReconcileError as error:
                require(old_absent and _action_complete(action)
                        and re.search(r"ResourceNotFound|OperationNotFound|\b404\b", str(error)) is not None,
                        str(error))
                action["child_operation_unavailable_after_absence"] = True
            print(f"{workers.utc_now()}: old prompool absent={old_absent}"
                  f" monitoring_ready={provider_ready and operator_ready}", flush=True)
            if provider_ready and operator_ready and old_absent and (
                operation is None or operation.get("status") == "Succeeded"
            ):
                action["pool_and_vmss_absent_at"] = workers.utc_now()
                self.persist_journal()
                self.phase = "complete"
                return
            require(time.monotonic() < self.work_deadline,
                    "Old empty prompool deletion exceeded the bounded wait")
            time.sleep(min(POLL_SECONDS, self.remaining_seconds(POLL_SECONDS)))

    def finalize(self):
        self.unchanged_inputs()
        snapshot = self.snapshot()
        self.authority()
        self.journals(allow_own=True)
        provider_ready, pools, vmsses = self.models()
        self._validate_old_zero(pools, vmsses, absent=True)
        require(provider_ready and self.guard(snapshot, require_new=True),
                "Final monitoring readiness regressed")
        self.memory_headroom()
        by_name = {row["name"]: row for row in pools}
        layout = {
            "schema_version": 1, "role": base.ROLE,
            "expected_total_pool_count": baseline.MODERN_POOL_COUNT,
            "pools": {
                name: {
                    "count": by_name[name]["count"], "mode": by_name[name]["mode"],
                    "vm_size": by_name[name]["vmSize"], "resource_id": by_name[name]["id"],
                } for name in ("default", "cniv5", NEW_POOL)
            },
        }
        baseline.validate_layout(
            layout, run_id=base.RESOURCE_GROUP, subscription_id=base.SUBSCRIPTION,
            expected_pool_count=baseline.MODERN_POOL_COUNT,
        )
        self.summary.update(
            status="post-retirement-prom-recovery-complete-workloads-not-started",
            success=True, repaired=True, workloads_ready=False,
            original_plan_sha256=baseline.PLAN_SHA,
            modern_cni={
                "completed": True, "source_retired": True, "pool_name": "cniv5",
                "default_pool_count": 1, "destination_pool_count": 2,
                "default_role_worker_count": 3,
                "retirement_build_id": RETIREMENT_BUILD,
                "network_qualification_build_ids": [79971, 79986],
            },
            baseline_pool_layout=layout,
            old_empty_pool_retired=True,
        )
        self.persist_journal()

    def execute(self):
        self.preflight()
        if not self.args.execute:
            return
        self.acquire()
        self.preflight()
        self.submit("pool_add", pool_add_command())
        self.wait_new_ready()
        self.submit("old_pool_delete", pool_delete_command())
        self.wait_old_absent()
        self.finalize()


def validate_args(args):
    require(
        args.resource_group == args.confirm_resource_group == base.RESOURCE_GROUP
        and args.expected_subscription.lower() == base.SUBSCRIPTION
        and args.expected_region.lower() == base.REGION
        and retirement.capacity.quantities.valid_sha(args.expected_tfvars_sha),
        "Monitoring recovery scope/tfvars mismatch",
    )
    require(args.retirement_build_id == RETIREMENT_BUILD
            and base.integer(args.timeout_seconds) and 600 <= args.timeout_seconds <= 3600
            and base.integer(args.request_timeout_seconds) and 10 <= args.request_timeout_seconds <= 120,
            "Only build 80001 and bounded timeout values are supported")
    require(args.kubeconfig and args.context == base.CLUSTER,
            "Private mesh96 kubeconfig and exact context are required")
    root = Path(args.retirement_directory).resolve()
    output = Path(args.summary_file).resolve()
    config = Path(args.kubeconfig).resolve()
    require(root.is_dir() and not Path(args.retirement_directory).is_symlink()
            and len({root, output, config}) == 3
            and root not in output.parents and not output.exists(),
            "Output must be new and separate from immutable inputs/private credentials")
    args.role = base.ROLE


def execute_recovery(args, summary, runner=workers.run_command):
    """Execute or plan the exact post-retirement monitoring repair."""

    validate_args(args)
    summary.update(
        schema_version=1, execute=args.execute, plan_valid=False,
        mutation_started=False, success=False, repaired=False, workloads_ready=False,
        status="validating-build-80001", started_at=workers.utc_now(), finished_at=None,
        plan_sha256=baseline.PLAN_SHA, retirement_build_id=RETIREMENT_BUILD,
        pool_add=empty_action(), old_pool_delete=empty_action(),
        journal={
            "name": JOURNAL, "namespace": "kube-system", "retained": True,
            "attempted": False, "accepted": None, "ambiguous": False,
        },
        cleanup_errors=[],
    )
    try:
        bundle = load_retirement(args)
        summary.update(
            retirement_input_hashes=bundle["hashes"],
            retirement_sha256=bundle["hashes"]["retirement.json"],
            retirement_journal={
                "name": retirement.JOURNAL, "uid": RETIREMENT_JOURNAL_UID,
                "retained": True, "modified": False,
            },
            current_mock_uids=copy.deepcopy(bundle["receipt"]["current_mock_uids"]),
            preserved_kwok_uids=copy.deepcopy(bundle["receipt"]["preserved_kwok_uids"]),
            controller_replacements=copy.deepcopy(bundle["receipt"]["controller_replacements"]),
        )
        PromRecovery(args, bundle, summary, runner).execute()
    except EXPECTED_ERRORS as error:
        summary.update(
            success=False, repaired=False, workloads_ready=False, status="failed-closed",
            error=str(error), automatic_retry_allowed=False,
        )
        raise
    finally:
        summary["finished_at"] = workers.utc_now()
        summary["workloads_ready"] = False
        mocks.write_json_atomic(args.summary_file, summary)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "resource-group", "confirm-resource-group", "expected-subscription",
        "expected-region", "expected-tfvars-sha", "retirement-directory",
        "kubeconfig", "summary-file",
    ):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--retirement-build-id", type=int, required=True)
    parser.add_argument("--context", default=base.CLUSTER)
    parser.add_argument("--timeout-seconds", type=int, default=3600)
    parser.add_argument("--request-timeout-seconds", type=int, default=45)
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    def interrupted(signum, _frame):
        raise workers.ReconcileError(
            f"Interrupted ({signum}); retain the exclusive journal and never replay an ambiguous request"
        )

    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, interrupted)
    args, summary = parse_args(argv), {}
    try:
        execute_recovery(args, summary)
    except EXPECTED_ERRORS as error:
        print(f"Post-retirement Prom recovery failed closed: {error}", file=sys.stderr)
        return 1
    print(f"{summary['status']}; workloads_ready=false")
    return 0


if __name__ == "__main__":
    sys.exit(main())
