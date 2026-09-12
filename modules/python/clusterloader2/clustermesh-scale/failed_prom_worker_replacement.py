"""One native replacement of the exact host whose accepted reimage failed.

The original plan and reimage marker remain lineage, never replacement identity.
There is no retry, forced Kubernetes deletion, surge, or automatic rollback.
"""

# pylint: disable=protected-access,too-many-lines

from __future__ import annotations

import copy
import json
import time
import uuid
from datetime import datetime, timezone

import unreachable_prom_worker_recovery as recovery


REPLACEMENT_KEY = "mock-clustermesh/unreachable-prom-replacement"
FAILED_NETWORK_CONTAINER = "747c5d32-7b25-441b-8d11-5a7211bd9313"
FAILURE_CODE = "ProvisioningState/failed/OSProvisioningInternalError"
DELETE_SECONDS = 900
RESTORE_SECONDS = 1200
ORIGINAL_HOST_PODS = 18
require = recovery.require


def status_rows(view, description):
    require(isinstance(view, dict), f"{description}: instance view is malformed")
    rows = view.get("statuses")
    require(isinstance(rows, list) and all(
        isinstance(row, dict) and isinstance(row.get("code"), str) for row in rows
    ), f"{description}: status inventory is malformed")
    return rows


def provisioning(rows, description):
    codes = [row["code"] for row in rows if row["code"].startswith("ProvisioningState/")]
    require(len(codes) == 1, f"{description}: latest provisioning status is ambiguous")
    return codes[0]


def extension_states(view, *, pending=False, allow_missing=False):
    rows = view.get("extensions")
    if rows is None or rows == []:
        require(allow_missing, "VM extension inventory is missing")
        return {}
    require(isinstance(rows, list) and rows, "VM extension inventory is missing")
    result = {}
    for row in rows:
        require(isinstance(row, dict) and isinstance(row.get("name"), str) and row["name"]
                and row["name"] not in result, "VM extension ownership is ambiguous")
        statuses = row.get("statuses")
        if statuses is None:
            require(pending, "A healthy VM has uninitialized guest extension statuses")
            statuses = []
        require(isinstance(statuses, list) and (pending or bool(statuses)),
                "VM extension statuses are uninitialized or malformed")
        allowed = {"ProvisioningState/succeeded"}
        if pending:
            allowed.update(("ProvisioningState/creating", "ProvisioningState/updating",
                            "ProvisioningState/transitioning"))
        require(all(isinstance(status, dict) and status.get("code") in allowed for status in statuses),
                "VM extension has a failed or unsupported status")
        result[row["name"]] = sorted(status["code"] for status in statuses)
    return result


def initialized_network(network):
    return (
        network["assigned_ip_count"] > 0 and network["version"] > 0
        and len(set(network["ip_addresses"])) == len(network["ip_addresses"]) == network["assigned_ip_count"]
    )


class ReplacementRecovery(recovery.Recovery):
    """Delete the pinned failed User-pool machine, prove zero, then scale once."""

    def __init__(self, args, plan, summary, runner, delete_pod):
        super().__init__(args, plan, summary, runner, delete_pod)
        self.stage = "original"
        self.accepted = None
        self.original_marker = ""
        self.replacement_marker = ""
        self.failure_times = {}
        self.original_pods = None
        self.original_nnc_uid = None
        self.extension_names = None
        self.candidate_vm = None
        self.candidate_node = None
        self.derived = None
        self.live = {}
        self.record = {
            "delete": {"attempted": False, "accepted": False, "ambiguous": False},
            "restore": {"attempted": False, "accepted": False, "ambiguous": False},
            "marker_write": {"attempted": False, "accepted": False, "ambiguous": False},
            "replacement_completed": False,
            "automatic_retry_allowed": False,
        }
        summary["replacement"] = self.record

    def operation(self):
        operation = self.az_json(
            "aks", "operation", "show-latest", "--resource-group", recovery.RESOURCE_GROUP,
            "--name", recovery.CLUSTER, "--query", recovery.OPERATION_QUERY,
        )
        self.summary["arm_metadata"] = {"operation": operation}
        self.save()
        require(isinstance(operation, dict) and operation.get("name") and not operation.get("errorCode"),
                "Latest AKS operation is missing, failed, or ambiguous")
        start = recovery.timestamp(operation.get("startTime"), "AKS operation start")
        require(start <= datetime.now(timezone.utc), "Latest AKS operation starts in the future")
        if operation.get("status") == "Succeeded":
            end = recovery.timestamp(operation.get("endTime"), "AKS operation completion")
            require(start <= end and (end - datetime.now(timezone.utc)).total_seconds() <= 30,
                    "Latest AKS operation timestamps are ambiguous")
            return True
        types = {
            "deleting": {"DeleteMachines", "DeleteAgentPoolMachines"},
            "restoring": {"PutAgentPool", "ScaleAgentPool", "UpdateAgentPool"},
        }
        action = self.record["delete" if self.stage == "deleting" else "restore"]
        require(self.stage in types and action["accepted"] is True
                and operation.get("status") in ("InProgress", "Running")
                and operation.get("operationType") in types[self.stage]
                and not operation.get("endTime")
                and start >= recovery.timestamp(action["requested_at"], "owned native request"),
                "Latest AKS operation is not quiescent or this owned native transition")
        return False

    def failure(self, rows, key):
        require(provisioning(rows, key) == FAILURE_CODE,
                "Only the new terminal OSProvisioningInternalError is a replacement candidate")
        row = next(row for row in rows if row["code"] == FAILURE_CODE)
        self.summary.setdefault("terminal_failure_observations", {})[key] = {
            "code": row["code"], "time": row.get("time"), "stage": self.stage,
        }
        self.save()
        observed = recovery.timestamp(row.get("time"), f"{key} terminal failure")
        requested = recovery.timestamp(self.accepted["restart"]["requested_at"], "accepted reimage")
        require(requested < observed and (datetime.now(timezone.utc) - observed).total_seconds() >= 300,
                "The new terminal failure must follow the accepted reimage and be at least five minutes old")
        pin = observed.isoformat()
        require(key not in self.failure_times or self.failure_times[key] == pin,
                "A new provisioning failure appeared during native deletion")
        self.failure_times[key] = pin

    def instance(self, vm, vmss_name, *, original=False, default=False):
        creating = self.stage == "restoring" and not original and not default
        initializing = creating and vm.get("provisioningState") in ("Creating", "Updating")
        instance_id = str(vm.get("instanceId", ""))
        vmss_id = (
            f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{recovery.NODE_GROUP}/"
            f"providers/Microsoft.Compute/virtualMachineScaleSets/{vmss_name}"
        )
        require(instance_id.isascii() and instance_id.isdecimal()
                and str(int(instance_id)) == instance_id
                and recovery.prepared.resource_equal(vm.get("id"), f"{vmss_id}/virtualMachines/{instance_id}")
                and isinstance(vm.get("computerName"), str)
                and recovery.NAME_RE.fullmatch(vm["computerName"])
                and (vm.get("latestModelApplied") is True or (
                    initializing and (vm.get("latestModelApplied") is None or vm.get("latestModelApplied") is False)
                ))
                and isinstance(vm.get("vmId"), str)
                and recovery.maintenance.UUID_RE.fullmatch(vm["vmId"]),
                "VM provider/computer/VMID or latest applied model is not exact")
        identity = {
            "node_name": vm["computerName"], "instance_id": instance_id,
            "vm_id": vm["vmId"], "resource_id": vm["id"].lower(),
            "provider_id": f"azure://{vm['id'].lower()}",
        }
        if default:
            name = f"{recovery.DEFAULT_VMSS}{int(instance_id):06d}"
            prior = self.accepted["arm_metadata"]["instances"].get(name) or {}
            require(instance_id in ("0", "1") and vm["computerName"] == name
                    and vm["vmId"] == prior.get("vm_id")
                    and recovery.prepared.resource_equal(vm["id"], prior.get("id", "")),
                    "An original default VM identity changed since the accepted action")
        elif original:
            require(instance_id == "0" and vm["computerName"] == recovery.PROM_NODE
                    and vm["vmId"] == recovery.FAILED_PROM_VM_ID,
                    "The failed original VM identity changed")
        else:
            require(instance_id != "0" and vm["computerName"] != recovery.PROM_NODE
                    and vm["computerName"].startswith(recovery.PROM_VMSS)
                    and vm["vmId"] not in {
                        recovery.FAILED_PROM_VM_ID,
                        *(row["vm_id"] for row in self.model_pin["defaults"].values()),
                    }, "Restoration reused an original Node, provider, or VM identity")
            require(self.candidate_vm is None or identity == self.candidate_vm,
                    "The replacement VM identity changed or another instance appeared")
            self.candidate_vm = identity
        view = self.az_json(
            "vmss", "get-instance-view", "--resource-group", recovery.NODE_GROUP,
            "--name", vmss_name, "--instance-id", instance_id, "--query", recovery.VIEW_QUERY,
        )
        require(isinstance(view, dict), "VM instance view is malformed")
        rows = [] if initializing and view.get("statuses") is None else status_rows(view, vm["computerName"])
        codes = [row["code"] for row in rows]
        powers = [code for code in codes if code.startswith("PowerState/")]
        if original:
            if self.stage == "original" or provisioning(rows, "original VM") == FAILURE_CODE:
                self.failure(rows, "VM")
            else:
                require(self.stage == "deleting" and provisioning(rows, "original VM")
                        == "ProvisioningState/deleting", "Original VM entered an unrelated transition")
            states = {"Failed"} if self.stage == "original" else {"Failed", "Deleting"}
            power_states = {"PowerState/running"}
            if self.stage == "deleting":
                power_states.update(("PowerState/stopping", "PowerState/stopped",
                                     "PowerState/deallocating", "PowerState/deallocated"))
            require(vm.get("provisioningState") in states and len(powers) == 1 and powers[0] in power_states,
                    "Original failed VM has an unsupported state or power status")
        elif creating:
            require(vm.get("provisioningState") in ("Creating", "Updating", "Succeeded"),
                    "Replacement VM entered a new terminal failure")
            require(len(powers) <= 1 and all(code in ("PowerState/starting", "PowerState/running")
                                           for code in powers),
                    "Replacement VM has an unsupported power state")
            require(not rows or provisioning(rows, "replacement VM") in (
                "ProvisioningState/creating", "ProvisioningState/updating", "ProvisioningState/succeeded",
            ), "Replacement VM has a failed or unsupported provisioning status")
        else:
            require(vm.get("provisioningState") == "Succeeded"
                    and provisioning(rows, vm["computerName"]) == "ProvisioningState/succeeded"
                    and powers == ["PowerState/running"], "Healthy VM is not Running/Succeeded")
        require(all(code.startswith(("PowerState/", "ProvisioningState/")) for code in codes),
                "VM contains an unsupported instance status")
        extensions = extension_states(view, pending=original or creating, allow_missing=initializing)
        if not default:
            require(self.extension_names is None or (
                set(extensions) <= self.extension_names if initializing else set(extensions) == self.extension_names
            ),
                    "Replacement extension configuration changed")
            if self.extension_names is None:
                self.extension_names = set(extensions)
        healthy = (
            vm.get("provisioningState") == "Succeeded"
            and vm.get("latestModelApplied") is True and bool(extensions)
            and set(codes) == {"PowerState/running", "ProvisioningState/succeeded"}
            and all(value and set(value) == {"ProvisioningState/succeeded"} for value in extensions.values())
        )
        evidence = {
            **identity, "provisioning_state": vm.get("provisioningState"),
            "status_codes": sorted(codes), "extensions": extensions,
            "latest_model_applied": vm.get("latestModelApplied"),
            "instance_view_initializing": initializing and (not rows or not extensions),
        }
        self.summary["arm_metadata"]["instances"][vm["computerName"]] = evidence
        self.save()
        return evidence, healthy

    def models(self, *, restarting=False, allow_failed_os=False):
        """Validate the original two pools, never the base class's old-host model."""

        require(not restarting and not allow_failed_os, "Native replacement must not use restart/reimage transitions")
        quiescent = self.operation()
        pools = self.az_json("aks", "nodepool", "list", "--resource-group", recovery.RESOURCE_GROUP,
                             "--cluster-name", recovery.CLUSTER)
        vmsses = self.az_json("vmss", "list", "--resource-group", recovery.NODE_GROUP,
                              "--query", recovery.VMSS_QUERY)
        require(isinstance(pools, list) and len(pools) == 2 and all(isinstance(row, dict) for row in pools)
                and {row.get("name") for row in pools} == {"default", "prompool"},
                "The pool inventory is not exactly the original two pools")
        require(isinstance(vmsses, list) and len(vmsses) == 2 and all(isinstance(row, dict) for row in vmsses)
                and {row.get("name") for row in vmsses} == {recovery.DEFAULT_VMSS, recovery.PROM_VMSS},
                "The original owned VMSS must remain present; another or missing VMSS is unsupported")
        evidence = self.summary["arm_metadata"]
        evidence.update(pools={}, vmsses={}, instances={})
        pin = {"pools": {}, "vmsses": {}, "defaults": {}}
        healthy = quiescent
        for pool in pools:
            name = pool["name"]
            prom = name == "prompool"
            vmss_name = recovery.PROM_VMSS if prom else recovery.DEFAULT_VMSS
            vmss = next(row for row in vmsses if row["name"] == vmss_name)
            counts = {0, 1} if prom and self.stage in ("deleting", "restoring") else {
                0 if prom and self.stage == "empty" else 1 if prom else 2,
            }
            pool_states = {"Succeeded"}
            vmss_states = {"Succeeded"}
            if prom and self.stage in ("original", "deleting"):
                vmss_states.add("Failed")
            if prom and self.stage == "deleting":
                pool_states.add("DeletingMachines")
                vmss_states.add("Updating")
            if prom and self.stage == "restoring":
                pool_states.update(("Scaling", "Updating", "Creating"))
                vmss_states.update(("Updating", "Creating"))
            pool_hash = recovery.digest(recovery.prepared.pool_configuration(pool))
            prior = (self.accepted["arm_metadata"].get("pools") or {}).get(name) or {}
            require(pool_hash == prior.get("configuration_sha256"),
                    f"{name}: pool configuration changed since the accepted reimage")
            require(recovery.integer(pool.get("count")) and pool["count"] in counts
                    and pool.get("mode") == ("User" if prom else "System")
                    and pool.get("enableAutoScaling") is False
                    and pool.get("provisioningState") in pool_states
                    and (pool.get("powerState") or {}).get("code") == "Running"
                    and isinstance(pool.get("nodeImageVersion"), str) and pool["nodeImageVersion"]
                    and recovery.prepared.resource_equal(
                        pool.get("id"), f"{self.authority_pin['clusters'][recovery.ROLE]}/agentPools/{name}"),
                    f"{name}: requires the fixed-count original User/System pool, Running and safely provisioned")
            vmss_id = (
                f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{recovery.NODE_GROUP}/"
                f"providers/Microsoft.Compute/virtualMachineScaleSets/{vmss_name}"
            )
            capacity = (vmss.get("sku") or {}).get("capacity")
            require(recovery.prepared.resource_equal(vmss.get("id"), vmss_id)
                    and str(vmss.get("location", "")).lower() == recovery.REGION
                    and recovery.workers.vmss_pool_name(vmss) == name
                    and vmss.get("orchestrationMode") == "Uniform"
                    and vmss.get("provisioningState") in vmss_states
                    and recovery.integer(capacity) and capacity in counts
                    and (vmss.get("sku") or {}).get("name") == pool.get("vmSize"),
                    f"{name}: VMSS ownership, capacity, configuration, or state changed")
            configuration = copy.deepcopy(vmss)
            configuration.pop("provisioningState", None)
            configuration["sku"].pop("capacity", None)
            pin["pools"][name] = pool_hash
            pin["vmsses"][vmss_name] = recovery.digest(configuration)
            evidence["pools"][name] = {
                "mode": pool["mode"], "count": pool["count"], "configuration_sha256": pool_hash,
                "provisioning_state": pool["provisioningState"], "node_image_version": pool["nodeImageVersion"],
            }
            evidence["vmsses"][vmss_name] = {
                "id": vmss["id"], "capacity": capacity, "provisioning_state": vmss["provisioningState"],
                "configuration_sha256": pin["vmsses"][vmss_name],
            }
            self.save()
            instances = self.az_json(
                "vmss", "list-instances", "--resource-group", recovery.NODE_GROUP,
                "--name", vmss_name, "--query", recovery.VM_QUERY,
            )
            require(isinstance(instances, list) and all(isinstance(row, dict) for row in instances)
                    and len(instances) in counts, f"{name}: VM instance count changed")
            if not prom:
                require({str(row.get("instanceId")) for row in instances} == {"0", "1"},
                        "The two original default instances must remain exact")
            if prom and self.stage == "restoring" and self.candidate_vm:
                require(len(instances) == 1, "The observed replacement VM disappeared")
            for vm in instances:
                state, stable = self.instance(
                    vm, vmss_name, original=prom and self.stage in ("original", "deleting"), default=not prom,
                )
                healthy = healthy and stable
                if not prom:
                    pin["defaults"][state["node_name"]] = state
            if prom:
                self.live = {"pool": pool, "vmss": vmss, "instances": instances}
                scale = self.az_json(
                    "vmss", "get-instance-view", "--resource-group", recovery.NODE_GROUP,
                    "--name", recovery.PROM_VMSS, "--query", recovery.SCALE_VIEW_QUERY,
                )
                rows = status_rows(scale, "prompool VMSS")
                code = provisioning(rows, "prompool VMSS")
                if self.stage == "original" or code == FAILURE_CODE:
                    require(instances and self.stage in ("original", "deleting"),
                            "A failed VMSS without the exact old failed VM is unsupported")
                    self.failure(rows, "VMSS")
                    require(self.stage == "deleting" or vmss["provisioningState"] == "Failed",
                            "Terminal VMSS status disagrees with its resource model")
                else:
                    allowed = {"ProvisioningState/succeeded"}
                    if self.stage in ("deleting", "restoring"):
                        allowed.update(("ProvisioningState/updating", "ProvisioningState/creating"))
                    require(code in allowed and (vmss["provisioningState"] != "Failed"
                            or (self.stage == "deleting" and bool(instances))),
                            "VMSS returned a new terminal failure or unsupported transition")
                counts_view = scale.get("virtualMachines")
                require(len(rows) == 1 and isinstance(counts_view, list) and all(
                    isinstance(row, dict) and recovery.integer(row.get("count")) and 0 <= row["count"] <= 1
                    and row.get("code") in (
                        {"ProvisioningState/failed", "ProvisioningState/deleting"}
                        if instances and self.stage in ("original", "deleting") else
                        {"ProvisioningState/succeeded", "ProvisioningState/creating", "ProvisioningState/updating"}
                    ) for row in counts_view
                ) and sum(row["count"] for row in counts_view) <= 1
                    and len({row["code"] for row in counts_view}) == len(counts_view),
                    "VMSS instance status summary contains unrelated failure or capacity")
                evidence["scale_set_statuses"] = [{key: row.get(key) for key in ("code", "time")} for row in rows]
                self.live["empty"] = (
                    quiescent and pool["count"] == capacity == len(instances) == 0
                    and pool["provisioningState"] == vmss["provisioningState"] == "Succeeded"
                    and code == "ProvisioningState/succeeded"
                    and all(row["count"] == 0 for row in counts_view)
                )
                healthy = healthy and (
                    pool["count"] == capacity == len(instances) == 1
                    and pool["provisioningState"] == vmss["provisioningState"] == "Succeeded"
                    and code == "ProvisioningState/succeeded"
                    and all(row["code"] == "ProvisioningState/succeeded" for row in counts_view)
                )
        require(self.model_pin is None or self.model_pin == pin,
                "Original default VM states or pool/VMSS configuration drifted")
        self.model_pin = pin
        self.summary["original_model_pins"] = copy.deepcopy(pin)
        self.summary["terminal_failure_times"] = dict(self.failure_times)
        self.save()
        return healthy

    def guard(self, snapshot, *, host_unready=False, host_optional=False):
        nodes, agents = super().guard(snapshot, host_unready=host_unready, host_optional=host_optional)
        if self.initial is not None:
            original_nodes = {row["metadata"]["name"]: row for row in self.initial["nodes"]["items"]}
            for name in {*self.plan["kwok_node_uids"], *set(recovery.REAL_UIDS) - {recovery.PROM_NODE}}:
                current = copy.deepcopy(nodes[name]["spec"])
                baseline = copy.deepcopy(original_nodes[name]["spec"])
                owned = [
                    {"key": recovery.EXCLUSION_KEY, "value": entry["token"], "effect": "NoSchedule"}
                    for entry in self.exclusions if entry["name"] == name
                ]
                current["taints"] = [row for row in current.get("taints", []) if row not in owned]
                baseline["taints"] = baseline.get("taints", [])
                require(current == baseline, f"{name}: protected Node specification changed")
            network = recovery.maintenance._nnc_map(snapshot["nnc"])
            original_network = recovery.maintenance._nnc_map(self.initial["nnc"])
            for name in set(recovery.REAL_UIDS) - {recovery.PROM_NODE}:
                require(name in network and all(network[name][key] == original_network[name][key]
                        for key in ("uid", "node_uid", "network_container_id")),
                        "An original default network-container identity changed")
        if self.derived:
            require(self.old_resources_absent(snapshot), "Original host resources reappeared after restoration")
            host = nodes[self.host_node]
            require(recovery.node_boot(host) == self.derived["boot_id"]
                    and (host["metadata"].get("labels") or {}).get("kubernetes.azure.com/node-image-version")
                    == self.derived["node_image_version"], "Replacement boot or original image changed")
            network = recovery.maintenance._nnc_map(snapshot["nnc"])
            nnc = self.network_identity(snapshot, self.host_node, self.real_uids[self.host_node])
            require(self.host_node in network and all(network[self.host_node][key] == self.derived[field]
                    for key, field in (("uid", "nnc_uid"), ("network_container_id", "network_container_id"))),
                    "The derived replacement network-container identity changed")
            require(nnc is not None and not nnc["metadata"].get("deletionTimestamp")
                    and initialized_network(network[self.host_node]),
                    "The replacement network container lost initialization or is terminating")
        return nodes, agents

    @staticmethod
    def network_identity(snapshot, name, node_uid):
        rows = recovery.mocks._items(snapshot["nnc"], "network-container inventory")
        require(len({row["metadata"]["name"] for row in rows}) == len(rows), "Duplicate NodeNetworkConfig names")
        matches = [row for row in rows if row["metadata"]["name"] == name]
        if not matches:
            return None
        row = matches[0]
        owner = recovery.controller_owner(row, "Node")
        require(owner["name"] == name and owner["uid"] == node_uid and recovery.object_uid(row),
                "Network container is not owned by the exact Node")
        return row

    def original_host(self, snapshot, *, complete):
        nodes, agents = self.guard(snapshot, host_unready=complete, host_optional=not complete)
        host = nodes.get(recovery.PROM_NODE)
        if host:
            recovery.prove_unreachable(host)
            annotations = recovery.maintenance._annotations(host)
            require(annotations.get(recovery.MARKER_KEY) == self.original_marker
                    and recovery.node_boot(host) == self.accepted["restart"]["previous_boot_id"],
                    "Original boot or exact accepted reimage marker changed")
            if self.replacement_marker:
                require(annotations.get(REPLACEMENT_KEY) == self.replacement_marker
                        and host["spec"].get("unschedulable") is True,
                        "Owned replacement marker or cordon was not confirmed")
            else:
                require(REPLACEMENT_KEY not in annotations, "A prior replacement marker forbids another host action")
            require((host["metadata"].get("labels") or {}).get("kubernetes.azure.com/node-image-version")
                    == self.live["pool"]["nodeImageVersion"], "Original host image differs from the pinned pool")
        pods = [row for row in recovery.mocks._items(snapshot["pods"], "old host Pods")
                if (row.get("spec") or {}).get("nodeName") == recovery.PROM_NODE]
        identities = {recovery.object_uid(row): [row["metadata"].get("namespace"), row["metadata"]["name"]]
                      for row in pods}
        require(len(identities) == len(pods), "Old host Pod identities are ambiguous")
        require(len(pods) <= ORIGINAL_HOST_PODS, "The original host exceeds the bounded 18-Pod inventory")
        for pod in pods:
            conditions = [row for row in (pod.get("status") or {}).get("conditions", [])
                          if row.get("type") == "Ready"]
            require(len(conditions) == 1 and conditions[0].get("status") == "False",
                    "Every remaining original-host Pod must explicitly be PodReady false")
        # Already terminating failed Pods may disappear naturally; no new UID may arrive.
        require(self.original_pods is None or all(
            self.original_pods.get(key) == value for key, value in identities.items()
        ), "An original host Pod changed or an unpinned Pod arrived")
        if self.original_pods is None:
            self.original_pods = identities
        nnc = self.network_identity(snapshot, recovery.PROM_NODE, recovery.REAL_UIDS[recovery.PROM_NODE])
        require(nnc is not None or not complete, "Original failed network container is missing")
        if nnc is not None:
            network = recovery.maintenance._nnc_map({"items": [nnc]})[recovery.PROM_NODE]
            require(network["network_container_id"] == FAILED_NETWORK_CONTAINER
                    and (self.original_nnc_uid is None or network["uid"] == self.original_nnc_uid),
                    "Original failed network-container identity changed")
            self.original_nnc_uid = network["uid"]
        return nodes, agents

    def old_resources_absent(self, snapshot):
        nodes = recovery.mocks._items(snapshot["nodes"], "native Node removal")
        pods = recovery.mocks._items(snapshot["pods"], "native Pod removal")
        nncs = recovery.mocks._items(snapshot["nnc"], "native network-container removal")
        return not (
            any(row["metadata"]["name"] == recovery.PROM_NODE
                or recovery.object_uid(row) == recovery.REAL_UIDS[recovery.PROM_NODE]
                or recovery.prepared.resource_equal((row.get("spec") or {}).get("providerID"), recovery.PROVIDER)
                for row in nodes)
            or any((row.get("spec") or {}).get("nodeName") == recovery.PROM_NODE
                   or recovery.object_uid(row) in self.original_pods for row in pods)
            or any(row["metadata"]["name"] == recovery.PROM_NODE
                   or recovery.object_uid(row) == self.original_nnc_uid
                   or any(owner.get("uid") == recovery.REAL_UIDS[recovery.PROM_NODE]
                          for owner in row["metadata"].get("ownerReferences", []))
                   or any(container.get("id") == FAILED_NETWORK_CONTAINER
                          for container in (row.get("status") or {}).get("networkContainers") or [])
                   for row in nncs)
        )

    def targets_ready_to_plan(self, snapshot):
        require(len(self.targets) == 5, "Replacement requires all five originally approved framework targets")
        decisions = []
        for target in self.targets:
            state, _ = self.target_state(snapshot, target)
            decisions.append({**target, "decision": state})
        self.dns_ready(snapshot)
        return decisions

    def submit(self, action, command):
        receipt = self.record[action]
        require(not receipt["attempted"], f"A native {action} request cannot be repeated")
        receipt.update(attempted=True, accepted=None, ambiguous=True, requested_at=recovery.workers.utc_now())
        self.save()
        try:
            self.write(command)
            receipt.update(accepted=True, ambiguous=False)
        finally:
            receipt["returned_at"] = recovery.workers.utc_now()
            self.save()
            print(json.dumps({
                "role": recovery.ROLE, "native_replacement_action": action,
                "attempted": receipt["attempted"], "accepted": receipt["accepted"],
                "ambiguous": receipt["ambiguous"], "returned_at": receipt["returned_at"],
            }), flush=True)

    def mark_original(self, snapshot):
        host = next(row for row in snapshot["nodes"]["items"] if row["metadata"]["name"] == recovery.PROM_NODE)
        marker = {
            "schema_version": 1, "owner": recovery.OWNER, "action": "native-failed-host-replacement",
            "token": str(uuid.uuid4()), "plan_sha256": self.summary["plan_sha256"],
            "node_uid": recovery.REAL_UIDS[recovery.PROM_NODE], "provider_id": recovery.PROVIDER,
            "vm_id": recovery.FAILED_PROM_VM_ID, "recorded_at": recovery.workers.utc_now(),
            "accepted_reimage_token": self.accepted["restart"]["marker"]["token"],
            "accepted_reimage_requested_at": self.accepted["restart"]["requested_at"],
            "accepted_reimage_marker_sha256": recovery.digest(self.original_marker),
        }
        self.replacement_marker = json.dumps(marker, sort_keys=True, separators=(",", ":"))
        self.record["marker"] = marker
        self.submit("marker_write", [
            "kubectl", "patch", "node", recovery.PROM_NODE, "--type=json", "-p", json.dumps([
                {"op": "test", "path": "/metadata/uid", "value": recovery.REAL_UIDS[recovery.PROM_NODE]},
                {"op": "test", "path": "/metadata/resourceVersion", "value": host["metadata"]["resourceVersion"]},
                {"op": "test", "path": f"/metadata/annotations/{recovery.MARKER_KEY.replace('/', '~1')}",
                 "value": self.original_marker},
                {"op": "test", "path": "/metadata/annotations", "value": recovery.maintenance._annotations(host)},
                {"op": "add", "path": f"/metadata/annotations/{REPLACEMENT_KEY.replace('/', '~1')}",
                 "value": self.replacement_marker},
                {"op": "add", "path": "/spec/unschedulable", "value": True},
            ]),
        ])

    def remove_original(self):
        self.authority()
        self.models()
        snapshot = self.snapshot()
        self.original_host(snapshot, complete=True)
        self.targets_ready_to_plan(snapshot)
        self.mark_original(snapshot)
        self.authority()
        self.models()
        snapshot = self.snapshot()
        self.original_host(snapshot, complete=True)
        self.targets_ready_to_plan(snapshot)
        self.summary["status"] = "removing-failed-prom-host"
        self.submit("delete", [
            "az", "aks", "nodepool", "delete-machines", "--resource-group", recovery.RESOURCE_GROUP,
            "--cluster-name", recovery.CLUSTER, "--name", "prompool",
            "--machine-names", recovery.PROM_NODE, "--no-wait", "--only-show-errors", "--output", "none",
        ])
        self.stage = "deleting"
        deadline = min(self.work_deadline, time.monotonic() + DELETE_SECONDS)
        while True:
            self.authority()
            self.models()
            snapshot = self.snapshot()
            self.original_host(snapshot, complete=False)
            absent = self.old_resources_absent(snapshot)
            self.record["removal_observation"] = {
                "observed_at": recovery.workers.utc_now(), "pool_count": self.live["pool"]["count"],
                "arm_empty": self.live["empty"], "old_resources_absent": absent,
            }
            self.save()
            if self.live["empty"] and absent:
                self.stage = "empty"
                self.record["native_removal"] = {
                    "verified_at": recovery.workers.utc_now(), "pool_count": 0, "vmss_capacity": 0,
                    "old_node_pods_nnc_absent": True, "original_marker_removed_by": "native-node-removal",
                    "manual_marker_clearance": False,
                }
                self.save()
                return
            self.wait(deadline, "Native failed-host deletion and garbage collection")

    def discover(self, snapshot, stable):
        require(self.old_resources_absent(snapshot), "Original resources returned or were not natively removed")
        if self.derived:
            nodes, _ = self.guard(snapshot)
            host = nodes[self.host_node]
            return (
                stable and recovery.workers.node_is_ready(host)
                and not host["spec"].get("unschedulable")
                and not any(row.get("effect") in ("NoSchedule", "NoExecute")
                            for row in recovery.maintenance._taints(host))
                and self.system_ready(snapshot)
            )
        rows = recovery.mocks._items(snapshot["nodes"], "replacement Nodes")
        retained = set(self.plan["kwok_node_uids"]) | (set(recovery.REAL_UIDS) - {recovery.PROM_NODE})
        candidates = [row for row in rows if row["metadata"]["name"] not in retained]
        require(len(candidates) <= 1, "More than one unexpected replacement Node appeared")
        candidate = candidates[0] if candidates else None
        network = None
        if candidate is not None:
            require(self.candidate_vm is not None, "A new Node has no authoritative replacement VM")
            vm = self.candidate_vm
            name, node_uid = candidate["metadata"]["name"], recovery.object_uid(candidate)
            recovery.maintenance._validate_real_node_scope(
                candidate, subscription=recovery.SUBSCRIPTION, node_resource_group=recovery.NODE_GROUP,
            )
            require(name == vm["node_name"] and recovery.prepared.resource_equal(
                candidate["spec"].get("providerID"), vm["provider_id"])
                and recovery.mocks._node_pool_name(candidate) == "prompool"
                and recovery.maintenance.UUID_RE.fullmatch(node_uid)
                and node_uid not in set(self.plan["kwok_node_uids"].values()) | set(recovery.REAL_UIDS.values())
                and not candidate["metadata"].get("deletionTimestamp"),
                "Replacement Node is not the distinct, live, VM-derived User-pool identity")
            boot = recovery.node_boot(candidate)
            require(boot != self.accepted["restart"]["previous_boot_id"], "Replacement reused the original boot ID")
            node_pin = {"node_name": name, "node_uid": node_uid, "boot_id": boot}
            require(self.candidate_node is None or node_pin == self.candidate_node,
                    "Observed replacement Node UID or boot changed")
            self.candidate_node = node_pin
            nnc = self.network_identity(snapshot, name, node_uid)
            if nnc is not None:
                require(not nnc["metadata"].get("deletionTimestamp"), "New network container is terminating")
                network = recovery.maintenance._replacement_nnc_map(snapshot["nnc"], name, node_uid).get(name)
        else:
            require(self.candidate_node is None, "The observed replacement Node disappeared")
        # Only the independently VM-bound candidate is withheld until its Node/NC
        # proof is complete. All original protected objects still pass the guard.
        projected = dict(snapshot)
        projected["nodes"] = {"items": [row for row in rows if row is not candidate]}
        projected["nnc"] = {"items": [
            row for row in snapshot["nnc"]["items"]
            if candidate is None or row["metadata"]["name"] != candidate["metadata"]["name"]
        ]}
        self.guard(projected, host_optional=True)
        if candidate is None or not stable or network is None:
            return False
        require(network["network_container_id"] != FAILED_NETWORK_CONTAINER
                and network["uid"] != self.original_nnc_uid,
                "Replacement reused the failed network-container identity")
        if not (initialized_network(network) and recovery.workers.node_is_ready(candidate)):
            return False
        require(not candidate["spec"].get("unschedulable") and not any(
            row.get("effect") in ("NoSchedule", "NoExecute") for row in recovery.maintenance._taints(candidate)
        ), "Replacement Node still has scheduling holds")
        image = (candidate["metadata"].get("labels") or {}).get("kubernetes.azure.com/node-image-version")
        require(image == self.live["pool"]["nodeImageVersion"], "Replacement did not retain the original node image")
        require(not any(key in recovery.maintenance._annotations(candidate)
                        for key in (recovery.MARKER_KEY, REPLACEMENT_KEY)),
                "Replacement Node has an unexpected original/action marker")
        self.derived = {
            **self.candidate_vm, **self.candidate_node, "nnc_uid": network["uid"],
            "network_container_id": network["network_container_id"], "node_image_version": image,
            "derived_at": recovery.workers.utc_now(),
        }
        self.host_node, self.host_provider_id = self.derived["node_name"], self.derived["provider_id"]
        self.real_uids = {name: uid for name, uid in recovery.REAL_UIDS.items() if name != recovery.PROM_NODE}
        self.real_uids[self.host_node] = self.derived["node_uid"]
        self.stage = "replaced"
        self.summary["replacement_derived_identity"] = copy.deepcopy(self.derived)
        self.summary["replacement_derived_manifest"] = {
            "schema_version": 1, "original_plan_sha256": self.summary["plan_sha256"],
            "real_node_uids": dict(self.real_uids), "replacement": copy.deepcopy(self.derived),
            "kwok_node_uids": dict(self.plan["kwok_node_uids"]),
            "mock_pod_uids": dict(self.plan["mock_pod_uids"]),
            "ready_mock_pod_uids": dict(self.plan["ready_mock_pod_uids"]),
        }
        self.save()
        self.guard(snapshot)
        return self.system_ready(snapshot)

    def restore(self):
        self.authority()
        self.models()
        snapshot = self.snapshot()
        self.guard(snapshot, host_optional=True)
        require(self.stage == "empty" and self.live["empty"] and self.old_resources_absent(snapshot),
                "Restoration requires a fresh authoritative zero with native old-resource removal")
        self.summary["status"] = "restoring-prom-capacity"
        self.submit("restore", [
            "az", "aks", "nodepool", "scale", "--resource-group", recovery.RESOURCE_GROUP,
            "--cluster-name", recovery.CLUSTER, "--name", "prompool", "--node-count", "1",
            "--no-wait", "--only-show-errors", "--output", "none",
        ])
        self.stage = "restoring"
        deadline = min(self.work_deadline, time.monotonic() + RESTORE_SECONDS)
        while True:
            self.authority()
            stable = self.models()
            snapshot = self.snapshot()
            if self.discover(snapshot, stable):
                self.record.update(replacement_completed=True, completed_at=recovery.workers.utc_now())
                self.summary["restart"].update(
                    skipped_reason="replaced-not-restarted", host_proven=True,
                    current_boot_id=self.derived["boot_id"], requested_at=self.record["restore"]["requested_at"],
                )
                self.save()
                return
            self.wait(deadline, "Native replacement Node, initialized NC, and system readiness")

    def execute(self):
        with open(self.args.replace_failed_host, encoding="utf-8") as handle:
            self.accepted = json.load(handle)
        action, marker = recovery.validate_accepted_reimage(self.accepted, self.summary["plan_sha256"])
        metadata = self.accepted["arm_metadata"]
        require(isinstance(metadata.get("pools"), dict) and set(metadata["pools"]) == {"default", "prompool"}
                and all(isinstance(row, dict) for row in metadata["pools"].values())
                and set(metadata["instances"]) == set(recovery.REAL_UIDS)
                and all(isinstance(row, dict) for row in metadata["instances"].values()),
                "Accepted action lacks the original pool configuration and default VM identity pins")
        self.original_marker = json.dumps(marker, sort_keys=True, separators=(",", ":"))
        self.record["accepted_reimage_lineage"] = {
            "action": "reimage", "requested_at": action["requested_at"], "marker": copy.deepcopy(marker),
            "vm_id": recovery.FAILED_PROM_VM_ID,
        }
        self.authority()
        self.models()
        self.open_cluster()
        self.initial = self.snapshot()
        _, agents = self.original_host(self.initial, complete=True)
        self.summary["initial_system_ready"] = self.system_ready(self.initial)
        decisions = self.targets_ready_to_plan(self.initial)
        self.summary.update(
            plan_valid=True, effective_targets=self.targets,
            original_identity={
                "node_name": recovery.PROM_NODE, "node_uid": recovery.REAL_UIDS[recovery.PROM_NODE],
                "provider_id": recovery.PROVIDER, "instance_id": "0", "vm_id": recovery.FAILED_PROM_VM_ID,
                "boot_id": action["previous_boot_id"], "network_container_id": FAILED_NETWORK_CONTAINER,
                "nnc_uid": self.original_nnc_uid, "host_pod_uids": copy.deepcopy(self.original_pods),
            },
            planned_actions={
                "host_action": "native-failed-host-replacement", "restart_required": False,
                "pool": "prompool", "machine_names": [recovery.PROM_NODE], "pool_counts": [1, 0, 1],
                "restore_requires_verified_native_removal": True, "pods": decisions,
            },
            controller_pins=recovery.frozen_controllers(self.initial), pdb_pins=recovery.frozen_pdbs(self.initial),
            initial_mock_ready=sum(recovery.pod_ready(pod) for pod in agents.values()),
            capacity_and_actual_ip_proof_required_at_execution=True,
        )
        self.save()
        if not self.args.execute:
            self.summary["status"] = "plan_valid"
            return
        self.remove_original()
        self.restore()
        for target in self.targets:
            self.move_pod(target)
        self.cleanup()
        require(not self.summary["cleanup_errors"], "Cleanup failed; replacement cannot be certified")
        self.postproof()
        self.summary.update(repaired=True, status="repaired")
