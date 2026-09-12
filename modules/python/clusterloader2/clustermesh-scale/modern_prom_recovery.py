"""One explicitly different DSv5 monitoring pool for the preserved mesh-96.

The original deletion/failed Dv3 restoration remain immutable history. This mode
does not restore, resize, reimage, or adopt that pool. An exclusive retained
journal owns one GA pool creation and, only after strict framework proof, one
deletion of the still-empty original pool.
"""

# pylint: disable=protected-access,too-many-lines,too-many-boolean-expressions

from __future__ import annotations

import copy
import json
import re
import time
import uuid
from datetime import datetime, timezone

import failed_prom_capacity_resume as capacity
import failed_prom_worker_replacement as replacement
import unreachable_prom_worker_recovery as recovery


POOL_NAME = "promv5"
VM_SIZE = "Standard_D8s_v5"
QUOTA_FAMILY = "standardDSv5Family"
REQUIRED_CORES = 24
GUARD_NAME = "mesh96-modern-prom-recovery"
SUBNET_BASE = (
    f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{recovery.RESOURCE_GROUP}/"
    "providers/Microsoft.Network/virtualNetworks/clustermesh-shared-vnet/subnets"
)
PATCH_QUERY = "{id:id,kubernetesVersion:kubernetesVersion,currentKubernetesVersion:currentKubernetesVersion}"
SKU_QUERY = (
    "[?name=='Standard_D8s_v5'].{name:name,family:family,resourceType:resourceType,"
    "locations:locations,restrictions:restrictions,capabilities:capabilities}"
)
VMSS_MODEL_QUERY = (
    "{id:id,osDisk:virtualMachineProfile.storageProfile.osDisk."
    "{osType:osType,diskSizeGb:diskSizeGb,managedDisk:managedDisk."
    "{storageAccountType:storageAccountType},diffDiskOption:diffDiskSettings.option},"
    "imageReference:virtualMachineProfile.storageProfile.imageReference}"
)
POOL_SETTINGS = {
    "mode": "User", "vmSize": VM_SIZE, "maxPods": 250,
    "osType": "Linux", "osSku": "Ubuntu", "osDiskType": "Managed", "osDiskSizeGb": 256,
    "kubeletDiskType": "OS", "enableFips": False, "enableEncryptionAtHost": False,
    "enableNodePublicIp": False, "enableAutoScaling": False,
    "availabilityZones": None, "kubeletConfig": None, "linuxOsConfig": None,
    "nodeLabels": {"prometheus": "true"}, "nodeTaints": None,
    "vnetSubnetId": f"{SUBNET_BASE}/clustermesh-96-node",
    "podSubnetId": f"{SUBNET_BASE}/clustermesh-96-pod",
}
RETIRE_COMMAND = [
    "az", "aks", "nodepool", "delete", "--resource-group", recovery.RESOURCE_GROUP,
    "--cluster-name", recovery.CLUSTER, "--name", "prompool",
    "--no-wait", "--only-show-errors", "--output", "none",
]
require = recovery.require


def empty_action():
    return {"attempted": False, "accepted": False, "ambiguous": False}


def pool_add_command(patch):
    """These are GA `az aks nodepool add` flags, not an in-place VM-size update."""
    require(isinstance(patch, str) and re.fullmatch(r"[1-9][0-9]*\.[0-9]+\.[0-9]+", patch),
            "A full fresh Kubernetes patch must be pinned before pool creation")
    return [
        "az", "aks", "nodepool", "add", "--resource-group", recovery.RESOURCE_GROUP,
        "--cluster-name", recovery.CLUSTER, "--name", POOL_NAME, "--node-count", "1",
        "--node-vm-size", VM_SIZE, "--mode", "User", "--labels", "prometheus=true",
        "--os-type", "Linux", "--os-sku", "Ubuntu", "--node-osdisk-type", "Managed",
        "--node-osdisk-size", "256", "--max-pods", "250", "--max-surge", "10%",
        "--vnet-subnet-id", POOL_SETTINGS["vnetSubnetId"],
        "--pod-subnet-id", POOL_SETTINGS["podSubnetId"], "--kubernetes-version", patch,
        "--no-wait", "--only-show-errors", "--output", "none",
    ]


def vmss_configuration(vmss):
    result = copy.deepcopy(vmss)
    result.pop("provisioningState", None)
    result["sku"].pop("capacity", None)
    return recovery.digest(result)


def image_matches_pool(reference, pool_image):
    image_id = reference.get("id")
    if isinstance(image_id, str):
        match = re.search(r"/galleries/([^/]+)/images/([^/]+)/versions/([^/]+)$", image_id, re.IGNORECASE)
        return match is not None and "-".join(match.groups()).lower() == pool_image.lower()
    version = reference.get("exactVersion") or reference.get("version")
    return (
        isinstance(version, str) and version != "latest" and pool_image.endswith(f"-{version}")
        and "ubuntu" in f"{reference.get('offer', '')}/{reference.get('sku', '')}".lower()
    )


class ModernPromRecovery(capacity.CapacityResumeRecovery):
    """Retain both default workers and every mock/KWOK UID while repairing phase 1."""

    def __init__(self, args, plan, summary, runner, delete_pod):
        super().__init__(args, plan, summary, runner, delete_pod)
        self.modern = self.continuation
        summary.pop("capacity_resume")
        summary["modern_prom_recovery"] = self.modern
        self.modern.update({
            "mode": "explicit-modern-family-repair", "automatic_retry_allowed": False,
            "create": empty_action(), "retire": empty_action(), "frameworks_proven": False,
            "old_empty_pool_retired": False, "host_ip_proven": False,
            "attempt_guard": {
                "name": GUARD_NAME, "namespace": "kube-system", "create": empty_action(),
                "retained_owned_non_workload_record": False,
            },
        })
        self.source_pool = None
        self.desired = None
        self.new_vmss = None
        self.new_model_pin = None
        self.partial_deadline = None
        self.retired = False

    def run(self, command, timeout_seconds=45, *, cleanup=False):
        extra = [
            ["az", "aks", "show", "--resource-group", recovery.RESOURCE_GROUP,
             "--name", recovery.CLUSTER, "--query", PATCH_QUERY],
            ["az", "vm", "list-skus", "--location", recovery.REGION,
             "--resource-type", "virtualMachines", "--all", "--query", SKU_QUERY],
        ]
        if self.new_vmss is not None:
            extra.append([
                "az", "vmss", "show", "--resource-group", recovery.NODE_GROUP,
                "--name", self.new_vmss, "--query", VMSS_MODEL_QUERY,
            ])
        if list(command) not in [row + ["--output", "json", "--only-show-errors"] for row in extra]:
            return super().run(command, timeout_seconds, cleanup=cleanup)

        def once(arguments, timeout):
            try:
                return recovery.maintenance.ClusterOperator.run(
                    self, arguments, timeout, cleanup=cleanup or self.cleanup_mode,
                )
            except recovery.workers.ReconcileError as error:
                if recovery.AUTH_ERROR.search(str(error)):
                    raise
                raise recovery.arm.ReconcileError(str(error)) from error

        try:
            return recovery.arm.run_read_with_retries(
                list(command), once, timeout_seconds=timeout_seconds, attempts=3, retry_seconds=2,
            )
        except recovery.arm.ReconcileError as error:
            raise recovery.workers.ReconcileError(str(error)) from error

    def write(self, command, *, cleanup=False):
        if command[0] == "az":
            allowed = (
                self.desired is not None and list(command) == pool_add_command(self.desired["kubernetes_patch"])
                and self.modern["create"]["attempted"] and self.modern["create"]["accepted"] is None
            ) or (
                list(command) == RETIRE_COMMAND and self.modern["frameworks_proven"]
                and self.modern["retire"]["attempted"] and self.modern["retire"]["accepted"] is None
            )
            require(allowed, "Modern recovery forbids legacy restoration, reimage, quota, or other Azure writes")
        return super().write(command, cleanup=cleanup)

    def authority(self, *, strict=False):
        # Reuse the complete structural validator, never edit a member's health to
        # make it pass. Only this run's proven new host can observe known partials.
        account = self.az_json("account", "show", "--query", "{id:id}")
        require(str(account.get("id", "")).lower() == recovery.SUBSCRIPTION, "Current Azure subscription changed")
        group = self.az_json("group", "show", "--name", recovery.RESOURCE_GROUP)
        clusters = self.az_json(
            "aks", "list", "--resource-group", recovery.RESOURCE_GROUP, "--query", recovery.CLUSTER_QUERY,
        )
        members = self.az_json(
            "fleet", "member", "list", "--resource-group", recovery.RESOURCE_GROUP, "--fleet-name", "clustermesh-flt",
        )
        connected, partial = True, False
        try:
            selected, identities = recovery.prepared.validate_scope(self.args, group, clusters, members)
        except recovery.prepared.FleetNotConnected as error:
            require(not strict and len(error.members) == 1 and error.members[0]["name"] == recovery.ROLE,
                    "Only the original mesh-96 Fleet failure may be observed")
            member = error.members[0]
            status = member["meshProperties"]["status"]
            code = (status.get("error") or {}).get("code")
            partial = (
                member["provisioningState"] == "Succeeded" and member["labels"].get("mesh") == "true"
                and code == "PartialConnectivity" and self.modern["create"]["accepted"] is True
                and self.derived is not None
            )
            require((status["state"] == "Failed" and code == "ConnectivityTimeout") or partial,
                    "Mesh-96 is neither the original failure nor this owned host's recovering PartialConnectivity")
            if partial:
                if self.partial_deadline is None:
                    self.partial_deadline = min(self.work_deadline, time.monotonic() + recovery.POSTPROOF_SECONDS)
                require(time.monotonic() < self.partial_deadline, "Mesh-96 PartialConnectivity observation expired")
                self.modern["partial_connectivity_observation"] = {
                    "observed_at": recovery.workers.utc_now(), "state": status["state"],
                    "error_code": code, "connected": False,
                }
            selected = next(row for row in clusters if row["tags"]["role"] == recovery.ROLE)
            identities, connected = error.identities, False
        require(all(row.get("provisioningState") == "Succeeded"
                    and (row.get("powerState") or {}).get("code") == "Running" for row in clusters),
                "All preserved AKS resources must remain Running/Succeeded")
        require(selected["name"] == recovery.CLUSTER
                and selected["nodeResourceGroup"].lower() == recovery.NODE_GROUP,
                "Selected cluster or node resource group differs from the approved scope")
        node_group = self.az_json("group", "show", "--name", recovery.NODE_GROUP)
        require(recovery.prepared.resource_equal(
            node_group.get("id"), f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{recovery.NODE_GROUP}",
        ) and recovery.prepared.resource_equal(node_group.get("managedBy"), selected["id"])
            and str(node_group.get("location", "")).lower() == recovery.REGION,
            "Node resource-group identity, managedBy, or region changed")
        recovery.prepared.require_lease(node_group, self.args.timeout_seconds)
        pin = {
            "clusters": {row["tags"]["role"]: row["id"].lower() for row in clusters},
            "identities": sorted(identities, key=lambda row: row["role"]),
        }
        require(self.authority_pin is None or pin == self.authority_pin, "Preserved/Fleet identity map drifted")
        self.authority_pin, self.identities = pin, identities
        self.summary.update(authoritative_identities=pin["identities"], fleet_connected=connected,
                            lease_checked_at=recovery.workers.utc_now())
        self.save()
        if partial:
            self.models()
            self.guard(self.snapshot())
            self.wait(self.partial_deadline, "Read-only mesh-96 PartialConnectivity observation for literal Connected")
            return self.authority(strict=strict)
        return selected, connected

    def fresh_zero(self):
        decisions = super().fresh_zero()
        require(self.old_pool_references_absent(self.snapshot()),
                "The empty original pool still has Kubernetes references")
        require(not super().guard_objects(), "A legacy capacity attempt appeared during modern zero qualification")
        if self.source_pool is None:
            self.source_pool = copy.deepcopy(self.live["pool"])
        self.summary["original_model_pins"] = copy.deepcopy(self.model_pin)
        return decisions

    def plan_configuration(self):
        require(self.source_pool is not None, "A freshly proven original zero is required")
        for key, value in POOL_SETTINGS.items():
            expected = "Standard_D8_v3" if key == "vmSize" else value
            require(key in self.source_pool and self.source_pool[key] == expected
                    and (not isinstance(expected, bool) or self.source_pool[key] is expected),
                    f"Original prompool setting {key} differs from the approved managed Ubuntu baseline")
        cluster = self.az_json(
            "aks", "show", "--resource-group", recovery.RESOURCE_GROUP,
            "--name", recovery.CLUSTER, "--query", PATCH_QUERY,
        )
        require(isinstance(cluster, dict) and recovery.prepared.resource_equal(
            cluster.get("id"), self.authority_pin["clusters"][recovery.ROLE],
        ), "Kubernetes patch evidence belongs to another cluster")
        patch = cluster.get("currentKubernetesVersion")
        pool_add_command(patch)
        require(cluster.get("kubernetesVersion") in (patch, patch.rsplit(".", 1)[0]),
                "AKS requested/current Kubernetes versions disagree")
        snapshot = self.snapshot()
        nodes, _ = self.guard(snapshot, host_optional=True)
        for name in self.model_pin["defaults"]:
            require((nodes[name]["status"].get("nodeInfo") or {}).get("kubeletVersion") == f"v{patch}",
                    "Both unchanged healthy default Nodes must prove the same current Kubernetes patch")
        require(self.source_pool.get("orchestratorVersion") in (patch, patch.rsplit(".", 1)[0]),
                "Original monitoring pool Kubernetes version differs from the current patch")
        desired = {
            "name": POOL_NAME, "count": 1, **copy.deepcopy(POOL_SETTINGS), "kubernetes_patch": patch,
            "upgradeSettings": {
                "maxSurge": "10%", "maxUnavailable": "0",
                "drainTimeoutInMinutes": None, "nodeSoakDurationInMinutes": None,
                "maxBlockedNodes": None, "undrainableNodeBehavior": None,
            },
        }
        require(self.desired is None or self.desired == desired, "Pinned modern pool configuration or patch changed")
        self.desired = desired
        self.modern["desired_configuration"] = copy.deepcopy(desired)
        self.summary["modern_baseline_delta"] = {
            "intentional_baseline_change": True, "original_baseline_unchanged": False,
            "original_pool": "prompool", "new_pool": POOL_NAME,
            "vm_size": {"before": "Standard_D8_v3", "after": VM_SIZE},
            "kubernetes_patch": patch, "default_pool_count": 2, "new_user_pool_count": 1,
            "image_delta": {"before": self.source_pool["nodeImageVersion"], "after": None, "changed": None},
            "old_empty_pool_retired": False,
        }
        self.save()

    def read_quota(self):
        self.summary["quota_ready"] = self.modern["quota_ready"] = False
        self.save()
        rows = self.az_json("vm", "list-usage", "--location", recovery.REGION, "--query", capacity.USAGE_QUERY)
        require(isinstance(rows, list) and all(isinstance(row, dict) and isinstance(row.get("name"), str)
                                             for row in rows), "Regional quota response is malformed")
        selected = {}
        for name in (QUOTA_FAMILY, "cores"):
            matches = [row for row in rows if row["name"] == name]
            require(len(matches) == 1, f"Regional quota lacks exactly one {name} counter")
            used, limit = (capacity.quota_counter(matches[0].get(key)) for key in ("currentValue", "limit"))
            selected[name] = {"name": name, "currentValue": used, "limit": limit, "remaining": limit - used}
        ready = all(row["remaining"] >= REQUIRED_CORES for row in selected.values())
        self.modern["quota"] = {
            "observed_at": recovery.workers.utc_now(), "region": recovery.REGION,
            "required_cores": REQUIRED_CORES, "prom_cores": 8, "reserved_for_later_cni_cores": 16,
            "counters": selected, "ready": ready, "reservation_is_not_an_azure_allocation": True,
        }
        self.summary["quota_ready"] = self.modern["quota_ready"] = ready
        self.save()
        return ready

    def read_capacity(self):
        ready = self.read_quota()
        rows = self.az_json(
            "vm", "list-skus", "--location", recovery.REGION,
            "--resource-type", "virtualMachines", "--all", "--query", SKU_QUERY,
        )
        require(isinstance(rows, list) and len(rows) == 1 and isinstance(rows[0], dict),
                "Exactly one actual regional Standard_D8s_v5 SKU is required")
        sku = rows[0]
        self.modern["sku"] = {"observed_at": recovery.workers.utc_now(), **copy.deepcopy(sku),
                              "managed_os_disk_required": True}
        self.save()
        require(sku.get("name") == VM_SIZE and sku.get("family") == QUOTA_FAMILY
                and sku.get("resourceType") == "virtualMachines"
                and isinstance(sku.get("locations"), list)
                and recovery.REGION in [str(value).lower() for value in sku["locations"]]
                and sku.get("restrictions") == [], "The selected DSv5 SKU is restricted or belongs to another scope")
        caps = sku.get("capabilities")
        require(isinstance(caps, list) and all(isinstance(row, dict) and isinstance(row.get("name"), str)
                                             for row in caps), "SKU capabilities are malformed")
        values = {row["name"]: row.get("value") for row in caps}
        require(len(values) == len(caps) and values.get("vCPUs") == "8" and values.get("MemoryGB") == "32"
                and values.get("vCPUsAvailable", "8") == "8" and values.get("CpuArchitectureType") == "x64"
                and values.get("PremiumIO") == "True" and values.get("VMDeploymentTypes") == "IaaS"
                and values.get("EphemeralOSDiskSupported") == "False"
                and capacity.quota_counter(values.get("OSVhdSizeMB")) >= 256 * 1024,
                "DSv5 must provide exactly 8 vCPU/32Gi and support the managed 256Gi OS disk, not ephemeral")
        return ready

    def guard_objects(self):
        payload = self.kube("get", "configmaps", "-n", "kube-system",
                            "--field-selector", f"metadata.name={GUARD_NAME}", "-o", "json")
        rows = recovery.mocks._items(payload, "exclusive modern attempt guard")
        require(payload.get("apiVersion") == "v1" and payload.get("kind") in ("List", "ConfigMapList")
                and not (payload.get("metadata") or {}).get("continue") and len(rows) <= 1
                and all(row["metadata"].get("name") == GUARD_NAME
                        and row["metadata"].get("namespace") == "kube-system" for row in rows),
                "Modern attempt guard absence/identity is ambiguous")
        return rows

    def guard_absent(self):
        require(not self.guard_objects(), "An existing modern attempt guard forbids another attempt or blind adoption")
        require(not super().guard_objects(), "A previous legacy capacity attempt forbids modern creation")

    def owned_guard(self, row=None):
        if row is None:
            rows = self.guard_objects()
            require(len(rows) == 1, "Owned modern guard disappeared")
            row = rows[0]
        meta = row.get("metadata") or {}
        require(row.get("kind") == "ConfigMap" and row.get("apiVersion") == "v1"
                and meta.get("name") == GUARD_NAME and meta.get("namespace") == "kube-system"
                and capacity.valid_uuid(self.guard_uid) and meta.get("uid") == self.guard_uid
                and isinstance(meta.get("resourceVersion"), str) and meta["resourceVersion"]
                and not meta.get("deletionTimestamp") and not meta.get("ownerReferences")
                and row.get("data") == self.guard_data, "Owned modern guard UID/token/immutable contract changed")
        return row

    def create_guard(self):
        self.guard_absent()
        require(self.desired is not None and self.modern["quota_ready"], "Modern creation lacks a capacity/config contract")
        token = str(uuid.uuid4())
        self.guard_data = {
            "owner": recovery.OWNER, "token": token,
            "contract": capacity.canonical({
                "schema_version": 1, "action": "explicit-modern-prom-recovery",
                "plan_sha256": self.summary["plan_sha256"], **self.source_hashes,
                "original_identity": self.summary["original_identity"],
                "original_model_pins": self.model_pin, "desired_configuration": self.desired,
                "native_removal": self.record["native_removal"],
                "previous_restore_disambiguation": self.modern["previous_restore_disambiguation"],
            }),
            "record": capacity.canonical({"state": "reserved", "create": self.modern["create"],
                                          "retire": self.modern["retire"]}),
        }
        audit = self.modern["attempt_guard"]
        audit.update(token=token, record_may_exist=True)
        receipt = audit["create"]
        require(not receipt["attempted"], "Exclusive modern guard creation cannot be repeated")
        receipt.update(attempted=True, accepted=None, ambiguous=True, requested_at=recovery.workers.utc_now())
        self.save()
        try:
            output = self.write([
                "kubectl", "create", "configmap", GUARD_NAME, "-n", "kube-system",
                *(f"--from-literal={key}={value}" for key, value in self.guard_data.items()), "-o", "json",
            ])
            row = recovery.workers.parse_json(output, "exclusive modern guard creation")
            self.guard_uid = recovery.object_uid(row)
            audit["uid"] = self.guard_uid
            self.owned_guard(row)
            self.owned_guard()
            audit["retained_owned_non_workload_record"] = True
            receipt.update(accepted=True, ambiguous=False)
        finally:
            receipt["returned_at"] = recovery.workers.utc_now()
            self.save()

    def patch_guard(self, state):
        current = self.owned_guard()
        record = json.loads(self.guard_data["record"])
        transitions = {
            "create-attempted": "reserved", "create-accepted": "create-attempted",
            "create-ambiguous": "create-attempted", "frameworks-proven": "create-accepted",
            "retire-attempted": "frameworks-proven", "retire-accepted": "retire-attempted",
            "retire-ambiguous": "retire-attempted", "completed": "retire-accepted",
        }
        require(state in transitions and record["state"] == transitions[state],
                "Modern guard transition would repeat or adopt a provider request")
        record.update(state=state, recorded_at=recovery.workers.utc_now(),
                      create=copy.deepcopy(self.modern["create"]), retire=copy.deepcopy(self.modern["retire"]),
                      new_identity=copy.deepcopy(self.derived), new_model_pins=copy.deepcopy(self.new_model_pin))
        if state.endswith("-attempted"):
            record[state.split("-")[0]] = {"attempted": True, "accepted": None, "ambiguous": True}
        desired = {**self.guard_data, "record": capacity.canonical(record)}
        receipt = {"attempted": True, "accepted": None, "ambiguous": True, "state": state}
        self.modern["attempt_guard"][f"{state}_write"] = receipt
        self.save()
        try:
            self.write(["kubectl", "patch", "configmap", GUARD_NAME, "-n", "kube-system",
                        "--type=json", "-p", json.dumps([
                            {"op": "test", "path": "/metadata/uid", "value": self.guard_uid},
                            {"op": "test", "path": "/metadata/resourceVersion",
                             "value": current["metadata"]["resourceVersion"]},
                            {"op": "test", "path": "/data/token", "value": self.guard_data["token"]},
                            {"op": "test", "path": "/data", "value": self.guard_data},
                            {"op": "add", "path": "/data", "value": desired},
                        ])])
            self.guard_data = desired
            self.owned_guard()
            receipt.update(accepted=True, ambiguous=False)
        finally:
            receipt["returned_at"] = recovery.workers.utc_now()
            self.save()

    def submit_pool(self, action, command):
        receipt = self.modern[action]
        require(action in ("create", "retire") and not receipt["attempted"]
                and json.loads(self.owned_guard()["data"]["record"])["state"] == f"{action}-attempted",
                "A modern pool provider request cannot be repeated or escape its journal")
        receipt.update(attempted=True, accepted=None, ambiguous=True, requested_at=recovery.workers.utc_now())
        self.save()
        try:
            self.write(command)
        except recovery.EXPECTED_ERRORS:
            receipt["returned_at"] = recovery.workers.utc_now()
            self.save()
            try:
                self.patch_guard(f"{action}-ambiguous")
            except recovery.EXPECTED_ERRORS as error:
                self.modern["ambiguous_journal_update_error"] = str(error)
                self.save()
            raise
        receipt.update(accepted=True, ambiguous=False, returned_at=recovery.workers.utc_now())
        self.save()
        self.patch_guard(f"{action}-accepted")

    def operation(self):
        if self.stage == "empty":
            return super().operation()
        action = self.modern["retire" if self.stage == "retiring" or self.retired else "create"]
        operation = self.az_json(
            "aks", "operation", "show-latest", "--resource-group", recovery.RESOURCE_GROUP,
            "--name", recovery.CLUSTER, "--query", recovery.OPERATION_QUERY,
        )
        self.summary["arm_metadata"] = {"operation": operation}
        self.save()
        require(isinstance(operation, dict) and operation.get("name") and not operation.get("errorCode"),
                "Latest modern AKS operation is missing, failed, or ambiguous")
        start = recovery.timestamp(operation.get("startTime"), "modern AKS operation start")
        require(start <= datetime.now(timezone.utc), "Latest AKS operation starts in the future")
        owned = action["accepted"] is True and start >= recovery.timestamp(
            action["requested_at"], "owned modern request",
        )
        types = (
            {"DeleteAgentPool"} if self.stage == "retiring" or self.retired else
            {"PutAgentPool", "CreateAgentPool", "ScaleAgentPool", "UpdateAgentPool"}
        )
        if operation.get("status") == "Succeeded":
            end = recovery.timestamp(operation.get("endTime"), "modern AKS operation completion")
            require(start <= end and (end - datetime.now(timezone.utc)).total_seconds() <= 30,
                    "Latest AKS operation completion is ambiguous")
            if not owned:
                require(self.stage == "creating" and self.new_model_pin is None,
                        "Latest operation no longer belongs to this modern request")
                return False
            require(operation.get("operationType") in types, "A different AKS operation followed this request")
            return True
        require(owned and self.stage in ("creating", "retiring")
                and operation.get("status") in ("InProgress", "Running")
                and operation.get("operationType") in types and not operation.get("endTime"),
                "Latest AKS operation is not this owned bounded modern transition")
        return False

    def protected_models(self, pools, vmsses):
        evidence = self.summary["arm_metadata"]
        for name, vmss_name, count in (
            ("default", recovery.DEFAULT_VMSS, 2), ("prompool", recovery.PROM_VMSS, 0),
        ):
            pool_rows = [row for row in pools if row.get("name") == name]
            scale_rows = [row for row in vmsses if row.get("name") == vmss_name]
            retiring = name == "prompool" and (self.stage == "retiring" or self.retired)
            require(len(pool_rows) <= 1 and len(scale_rows) <= 1, "Duplicate protected pool or VMSS")
            require(retiring or len(pool_rows) == len(scale_rows) == 1, "An original pool or VMSS disappeared")
            require(not self.retired or not retiring or not (pool_rows or scale_rows),
                    "The retired original pool or VMSS reappeared")
            allowed = {"Succeeded", "Deleting", "Updating"} if retiring else {"Succeeded"}
            if pool_rows:
                pool = pool_rows[0]
                require(recovery.digest(recovery.prepared.pool_configuration(pool)) == self.model_pin["pools"][name]
                        and recovery.integer(pool.get("count")) and pool["count"] == count
                        and pool.get("mode") == ("System" if count else "User")
                        and pool.get("provisioningState") in allowed
                        and (pool.get("powerState") or {}).get("code") == "Running",
                        f"{name}: original pool configuration, count, or health changed")
                evidence["pools"][name] = {
                    "count": count, "mode": pool["mode"], "configuration_sha256": self.model_pin["pools"][name],
                    "provisioning_state": pool["provisioningState"], "node_image_version": pool["nodeImageVersion"],
                }
            if not scale_rows:
                continue
            vmss = scale_rows[0]
            require(vmss_configuration(vmss) == self.model_pin["vmsses"][vmss_name]
                    and recovery.integer((vmss.get("sku") or {}).get("capacity"))
                    and vmss["sku"]["capacity"] == count and vmss.get("provisioningState") in allowed,
                    f"{name}: original VMSS identity, model, capacity, or health changed")
            instances = self.az_json(
                "vmss", "list-instances", "--resource-group", recovery.NODE_GROUP,
                "--name", vmss_name, "--query", recovery.VM_QUERY,
            )
            require(isinstance(instances, list) and len(instances) == count
                    and all(isinstance(row, dict) for row in instances), f"{name}: original VM inventory changed")
            if count:
                require({str(row.get("instanceId")) for row in instances} == {"0", "1"},
                        "The two original default VM instances must remain exact")
                actual = {}
                for vm in instances:
                    state, healthy = super().instance(vm, vmss_name, default=True)
                    require(healthy, "An original default VM lost health")
                    actual[state["node_name"]] = state
                require(actual == self.model_pin["defaults"], "Original default VM states or identities changed")
            else:
                view = self.az_json(
                    "vmss", "get-instance-view", "--resource-group", recovery.NODE_GROUP,
                    "--name", vmss_name, "--query", recovery.SCALE_VIEW_QUERY,
                )
                rows = replacement.status_rows(view, "empty original VMSS")
                statuses = {"ProvisioningState/succeeded"}
                if retiring:
                    statuses.update(("ProvisioningState/deleting", "ProvisioningState/updating"))
                require(len(rows) == 1 and rows[0]["code"] in statuses
                        and (view.get("virtualMachines") is None or view["virtualMachines"] == []
                             or (isinstance(view["virtualMachines"], list) and all(
                                 isinstance(row, dict) and recovery.integer(row.get("count"))
                                 and row["count"] == 0 and row.get("code") in statuses
                                 for row in view["virtualMachines"]))),
                        "The original empty VMSS is not quiescent zero or this owned empty retirement")
            evidence["vmsses"][vmss_name] = {
                "id": vmss["id"], "capacity": count, "provisioning_state": vmss["provisioningState"],
                "configuration_sha256": self.model_pin["vmsses"][vmss_name],
            }

    def modern_pool(self, pool):
        require(recovery.prepared.resource_equal(
            pool.get("id"), f"{self.authority_pin['clusters'][recovery.ROLE]}/agentPools/{POOL_NAME}",
        ) and pool.get("name") == POOL_NAME and recovery.integer(pool.get("count")) and pool["count"] == 1
            and all(key in pool and pool[key] == value
                    and (not isinstance(value, bool) or pool[key] is value) for key, value in POOL_SETTINGS.items()),
            "New User pool does not match the explicit managed Ubuntu DSv5 configuration")
        current = recovery.prepared.pool_configuration(pool)
        original = recovery.prepared.pool_configuration(self.source_pool)
        intentional = {"id", "name", "vmSize", "orchestratorVersion", "currentOrchestratorVersion",
                       "nodeImageVersion", "upgradeSettings"}
        require(all(current.get(key) == original.get(key) for key in set(current) | set(original)
                    if key not in intentional), "New pool changed a setting outside the explicit baseline delta")
        require(pool.get("orchestratorVersion") == self.desired["kubernetes_patch"]
                and pool.get("currentOrchestratorVersion") in (
                    self.desired["kubernetes_patch"], None if self.stage == "creating" else "",
                ), "New pool does not use the freshly pinned exact Kubernetes patch")
        upgrade = pool.get("upgradeSettings")
        require(isinstance(upgrade, dict) and upgrade.get("maxSurge") == "10%"
                and str(upgrade.get("maxUnavailable")) == "0"
                and set(upgrade) <= set(self.desired["upgradeSettings"])
                and all(upgrade.get(key) == value for key, value in self.desired["upgradeSettings"].items()
                        if key not in ("maxSurge", "maxUnavailable")),
                "New pool must retain normal upgrade/draining settings without PDB relaxation")
        image = pool.get("nodeImageVersion")
        require((self.stage == "creating" and image is None)
                or (isinstance(image, str) and image.startswith("AKSUbuntu-")),
                "New monitoring pool image must actually be Ubuntu")

    def modern_instance(self, vm):
        initializing = self.stage == "creating" and vm.get("provisioningState") in ("Creating", "Updating")
        instance_id = str(vm.get("instanceId", ""))
        vmss_id = (
            f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{recovery.NODE_GROUP}/"
            f"providers/Microsoft.Compute/virtualMachineScaleSets/{self.new_vmss}"
        )
        require(instance_id.isascii() and instance_id.isdecimal() and str(int(instance_id)) == instance_id
                and recovery.prepared.resource_equal(vm.get("id"), f"{vmss_id}/virtualMachines/{instance_id}")
                and vm.get("provisioningState") in (("Creating", "Updating", "Succeeded") if initializing else ("Succeeded",))
                and (vm.get("latestModelApplied") is True
                     or (initializing and (vm.get("latestModelApplied") is None
                                          or vm.get("latestModelApplied") is False))),
                "New VM provider, applied model, or provisioning state is invalid")
        name, vm_id = vm.get("computerName"), vm.get("vmId")
        complete = name is not None and vm_id is not None
        require((initializing or complete) and (name is None or (
            isinstance(name, str) and recovery.NAME_RE.fullmatch(name) and name.startswith(self.new_vmss)
        )) and (vm_id is None or capacity.valid_uuid(vm_id)), "New VM guest identity is malformed or uninitialized")
        if complete:
            identity = {"node_name": name, "instance_id": instance_id, "vm_id": vm_id,
                        "resource_id": vm["id"].lower(), "provider_id": f"azure://{vm['id'].lower()}"}
            require(name not in set(self.plan["kwok_node_uids"]) | set(recovery.REAL_UIDS)
                    and vm_id not in {recovery.FAILED_PROM_VM_ID,
                                      *(row["vm_id"] for row in self.model_pin["defaults"].values())}
                    and not recovery.prepared.resource_equal(identity["provider_id"], recovery.PROVIDER),
                    "Modern creation reused an original Node, provider, or VM identity")
            require(self.candidate_vm is None or self.candidate_vm == identity, "Observed modern VM identity changed")
            self.candidate_vm = identity
        else:
            require(self.candidate_vm is None, "Previously initialized modern VM identity disappeared")
        view = self.az_json(
            "vmss", "get-instance-view", "--resource-group", recovery.NODE_GROUP,
            "--name", self.new_vmss, "--instance-id", instance_id, "--query", recovery.VIEW_QUERY,
        )
        require(isinstance(view, dict), "New VM instance view is malformed")
        rows = [] if initializing and view.get("statuses") is None else replacement.status_rows(view, "modern VM")
        codes = [row["code"] for row in rows]
        allowed = {"PowerState/running", "ProvisioningState/succeeded"}
        if initializing:
            allowed.update(("PowerState/starting", "ProvisioningState/creating", "ProvisioningState/updating"))
        require(len(codes) == len(set(codes)) and set(codes) <= allowed
                and len([code for code in codes if code.startswith("PowerState/")]) <= 1
                and (not rows or replacement.provisioning(rows, "modern VM") in allowed),
                "New VM has a terminal failure or unsupported status")
        extensions = replacement.extension_states(view, pending=initializing, allow_missing=initializing)
        if not initializing:
            require(self.extension_names is None or set(extensions) == self.extension_names,
                    "Modern VM extension inventory changed")
            self.extension_names = set(extensions)
        healthy = (complete and not initializing and vm.get("latestModelApplied") is True
                   and set(codes) == {"PowerState/running", "ProvisioningState/succeeded"}
                   and bool(extensions) and all(value == ["ProvisioningState/succeeded"] for value in extensions.values()))
        evidence = {**(self.candidate_vm or {"instance_id": instance_id}),
                    "provisioning_state": vm.get("provisioningState"), "status_codes": sorted(codes),
                    "extensions": extensions, "latest_model_applied": vm.get("latestModelApplied"),
                    "instance_view_initializing": initializing, "healthy": healthy}
        self.summary["arm_metadata"]["instances"][name or f"{self.new_vmss}/{instance_id}"] = evidence
        self.save()
        return healthy

    def models(self, *, restarting=False, allow_failed_os=False):
        require(not restarting and not allow_failed_os, "Modern creation never uses restart/reimage transitions")
        self.check_sources()
        if self.guard_uid is not None:
            self.owned_guard()
        if self.stage == "empty":
            return super().models()
        require(self.modern["create"]["accepted"] is True, "Only this run's accepted new-pool request may be observed")
        quiescent = self.operation()
        pools = self.az_json("aks", "nodepool", "list", "--resource-group", recovery.RESOURCE_GROUP,
                             "--cluster-name", recovery.CLUSTER)
        vmsses = self.az_json("vmss", "list", "--resource-group", recovery.NODE_GROUP, "--query", recovery.VMSS_QUERY)
        require(isinstance(pools, list) and all(isinstance(row, dict) for row in pools)
                and len({row.get("name") for row in pools}) == len(pools)
                and {row.get("name") for row in pools} <= {"default", "prompool", POOL_NAME},
                "Unexpected or duplicate pool in the preserved cluster")
        require(isinstance(vmsses, list) and all(isinstance(row, dict) for row in vmsses)
                and len({row.get("name") for row in vmsses}) == len(vmsses),
                "VMSS inventory is malformed or duplicated")
        extra = [row for row in vmsses if row.get("name") not in (recovery.DEFAULT_VMSS, recovery.PROM_VMSS)]
        require(len(extra) <= 1, "More than one new VMSS appeared")
        self.summary["arm_metadata"].update(pools={}, vmsses={}, instances={})
        self.protected_models(pools, vmsses)
        new = [row for row in pools if row["name"] == POOL_NAME]
        if not new:
            require(self.stage == "creating" and self.new_vmss is None and not extra,
                    "Observed modern pool disappeared or an unowned VMSS appeared")
            return False
        pool = new[0]
        self.modern_pool(pool)
        allowed = {"Succeeded", "Creating", "Updating", "Scaling"} if self.stage == "creating" else {"Succeeded"}
        require(pool.get("provisioningState") in allowed
                and ((pool.get("powerState") or {}).get("code") == "Running"
                     or (self.stage == "creating" and not pool.get("powerState"))),
                "Modern pool entered a terminal failure or unsupported state")
        if not extra:
            require(self.stage == "creating" and self.new_vmss is None, "Observed modern VMSS disappeared")
            return False
        vmss = extra[0]
        name = vmss.get("name")
        require(isinstance(name, str) and recovery.NAME_RE.fullmatch(name)
                and recovery.workers.vmss_pool_name(vmss) == POOL_NAME
                and recovery.prepared.resource_equal(
                    vmss.get("id"), f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{recovery.NODE_GROUP}/"
                    f"providers/Microsoft.Compute/virtualMachineScaleSets/{name}",
                ) and str(vmss.get("location", "")).lower() == recovery.REGION
                and vmss.get("orchestrationMode") == "Uniform"
                and (vmss.get("sku") or {}).get("name") == VM_SIZE
                and recovery.integer(vmss["sku"].get("capacity"))
                and vmss["sku"]["capacity"] in ({0, 1} if self.stage == "creating" else {1})
                and vmss.get("provisioningState") in allowed,
                "New VMSS ownership, SKU, capacity, or provisioning state is invalid")
        require(self.new_vmss is None or self.new_vmss == name, "Observed modern VMSS identity changed")
        self.new_vmss = name
        model = self.az_json(
            "vmss", "show", "--resource-group", recovery.NODE_GROUP, "--name", name, "--query", VMSS_MODEL_QUERY,
        )
        require(isinstance(model, dict) and recovery.prepared.resource_equal(model.get("id"), vmss["id"]),
                "New VMSS guest model belongs to a different provider resource")
        disk, image = model.get("osDisk") or {}, model.get("imageReference") or {}
        require(disk.get("osType") == "Linux" and recovery.integer(disk.get("diskSizeGb"))
                and disk["diskSizeGb"] == 256 and disk.get("diffDiskOption") is None
                and (disk.get("managedDisk") or {}).get("storageAccountType")
                in ("Standard_LRS", "StandardSSD_LRS", "Premium_LRS")
                and isinstance(image, dict) and (isinstance(image.get("id"), str) and image["id"]
                    or all(isinstance(image.get(key), str) and image[key]
                           for key in ("publisher", "offer", "sku", "version"))),
                "Modern VMSS must actually use a Linux managed 256Gi OS disk and an initialized image")
        require(pool.get("nodeImageVersion") is None or image_matches_pool(image, pool["nodeImageVersion"]),
                "Actual modern VMSS image reference differs from the reported Ubuntu pool image")
        instances = self.az_json("vmss", "list-instances", "--resource-group", recovery.NODE_GROUP,
                                 "--name", name, "--query", recovery.VM_QUERY)
        require(isinstance(instances, list) and len(instances) <= 1
                and all(isinstance(row, dict) for row in instances), "New pool must never contain more than one VM")
        require(instances or (self.stage == "creating" and self.candidate_vm is None),
                "Observed modern VM disappeared")
        instance_ready = bool(instances) and self.modern_instance(instances[0])
        view = self.az_json("vmss", "get-instance-view", "--resource-group", recovery.NODE_GROUP,
                            "--name", name, "--query", recovery.SCALE_VIEW_QUERY)
        initializing = self.stage == "creating" and vmss["provisioningState"] in ("Creating", "Updating")
        require(isinstance(view, dict), "Modern VMSS instance view is malformed")
        rows = [] if initializing and view.get("statuses") is None else replacement.status_rows(view, "modern VMSS")
        statuses = {"ProvisioningState/succeeded"}
        if self.stage == "creating":
            statuses.update(("ProvisioningState/creating", "ProvisioningState/updating"))
        counts = view.get("virtualMachines")
        require((initializing and not rows or len(rows) == 1 and rows[0]["code"] in statuses)
                and (counts is None and (initializing or not instances and self.stage == "creating")
                     or isinstance(counts, list) and all(isinstance(row, dict) and row.get("code") in statuses
                             and recovery.integer(row.get("count")) and 0 <= row["count"] <= 1 for row in counts)
                     and len({row["code"] for row in counts}) == len(counts)
                     and sum(row["count"] for row in counts) <= 1),
                "New VMSS contains a terminal failure, ambiguous summary, or extra capacity")
        stable = (quiescent and instance_ready and pool["provisioningState"] == vmss["provisioningState"] == "Succeeded"
                  and vmss["sku"]["capacity"] == 1 and pool.get("currentOrchestratorVersion")
                  == self.desired["kubernetes_patch"] and isinstance(pool.get("nodeImageVersion"), str)
                  and len(rows) == 1 and rows[0]["code"] == "ProvisioningState/succeeded"
                  and counts == [{"code": "ProvisioningState/succeeded", "count": 1}])
        pin = {"pool": recovery.digest(recovery.prepared.pool_configuration(pool)),
               "vmss": vmss_configuration(vmss), "guest_model": recovery.digest(model)}
        require(self.new_model_pin is None or self.new_model_pin == pin, "Proven modern pool or VMSS model changed")
        if stable:
            self.new_model_pin = pin
        self.modern["new_model_pins"] = copy.deepcopy(self.new_model_pin)
        self.summary["arm_metadata"]["pools"][POOL_NAME] = {
            "mode": "User", "count": pool["count"], "configuration_sha256": pin["pool"],
            "provisioning_state": pool["provisioningState"], "node_image_version": pool.get("nodeImageVersion"),
            "kubernetes_patch": pool.get("currentOrchestratorVersion"),
            "upgrade_settings": copy.deepcopy(pool["upgradeSettings"]),
        }
        self.summary["arm_metadata"]["vmsses"][name] = {
            "id": vmss["id"], "capacity": vmss["sku"]["capacity"], "configuration_sha256": pin["vmss"],
            "guest_model_sha256": pin["guest_model"], "provisioning_state": vmss["provisioningState"],
        }
        self.live = {"pool": pool, "vmss": vmss, "instances": instances,
                     "old_pool_absent": not any(row["name"] == "prompool" for row in pools),
                     "old_vmss_absent": not any(row["name"] == recovery.PROM_VMSS for row in vmsses)}
        self.save()
        return stable

    def old_pool_references_absent(self, snapshot):
        return self.old_resources_absent(snapshot) and not (
            any(recovery.mocks._node_pool_name(row) == "prompool"
                or row["metadata"]["name"].startswith(recovery.PROM_VMSS) for row in snapshot["nodes"]["items"])
            or any(str((row.get("spec") or {}).get("nodeName", "")).startswith(recovery.PROM_VMSS)
                   for row in snapshot["pods"]["items"])
            or any(row["metadata"]["name"].startswith(recovery.PROM_VMSS)
                   or any(str(owner.get("name", "")).startswith(recovery.PROM_VMSS)
                          for owner in row["metadata"].get("ownerReferences") or [])
                   for row in snapshot["nnc"]["items"])
        )

    def guard(self, snapshot, *, host_unready=False, host_optional=False):
        self.check_sources()
        if self.guard_uid is not None:
            self.owned_guard()
        nodes, agents = super().guard(snapshot, host_unready=host_unready, host_optional=host_optional)
        if self.initial is not None:
            original = {row["metadata"]["name"]: row for row in self.initial["nodes"]["items"]}
            for name in set(self.plan["kwok_node_uids"]) | set(self.model_pin["defaults"]):
                require(nodes[name]["metadata"].get("labels") == original[name]["metadata"].get("labels"),
                        "A protected default/KWOK Node label configuration changed")
        require(self.old_pool_references_absent(snapshot), "Original empty pool Kubernetes resources reappeared")
        if self.derived:
            host = nodes[self.host_node]
            info = host["status"].get("nodeInfo") or {}
            labels = host["metadata"].get("labels") or {}
            require(info.get("kubeletVersion") == f"v{self.desired['kubernetes_patch']}"
                    and info.get("operatingSystem") == "linux" and str(info.get("osImage", "")).startswith("Ubuntu")
                    and labels.get("prometheus") == "true"
                    and labels.get("agentpool") == labels.get("kubernetes.azure.com/agentpool") == POOL_NAME,
                    "Modern Node Kubernetes patch, Ubuntu OS, or monitoring label changed")
            require(recovery.workers.node_is_ready(host) and not host["spec"].get("unschedulable")
                    and not any(row.get("effect") in ("NoSchedule", "NoExecute")
                                for row in recovery.maintenance._taints(host)),
                    "The proven modern Node lost readiness or acquired scheduling holds")
        return nodes, agents

    def target_state(self, snapshot, target, *, after_delete=False):
        state, pod = super().target_state(snapshot, target, after_delete=after_delete)
        template = self.bindings[recovery.target_key(target)]["template"]
        require(not template.get("schedulingGates") and not template.get("readinessGates")
                and (pod is None or (not pod["spec"].get("schedulingGates")
                                     and not pod["spec"].get("readinessGates"))),
                "Pinned framework Pods must not have scheduling or readiness gates")
        return state, pod

    def discover(self, snapshot, stable):
        if self.derived:
            nodes, _ = self.guard(snapshot)
            return stable and recovery.mocks._node_ready_and_schedulable(nodes[self.host_node]) and self.system_ready(snapshot)
        retained = set(self.plan["kwok_node_uids"]) | set(self.model_pin["defaults"])
        candidates = [row for row in snapshot["nodes"]["items"] if row["metadata"]["name"] not in retained]
        require(len(candidates) <= 1, "More than one unexpected modern Node appeared")
        candidate = candidates[0] if candidates else None
        network = None
        if candidate is not None:
            require(self.candidate_vm is not None, "A new Node has no initialized authoritative modern VM identity")
            name, node_uid = candidate["metadata"]["name"], recovery.object_uid(candidate)
            recovery.maintenance._validate_real_node_scope(
                candidate, subscription=recovery.SUBSCRIPTION, node_resource_group=recovery.NODE_GROUP,
            )
            require(name == self.candidate_vm["node_name"] and recovery.prepared.resource_equal(
                candidate["spec"].get("providerID"), self.candidate_vm["provider_id"],
            ) and candidate["metadata"]["labels"].get("agentpool")
                == candidate["metadata"]["labels"].get("kubernetes.azure.com/agentpool") == POOL_NAME
                and capacity.valid_uuid(node_uid)
                and node_uid not in set(self.plan["kwok_node_uids"].values()) | set(recovery.REAL_UIDS.values())
                and not candidate["metadata"].get("deletionTimestamp"),
                "New Node is not the distinct, actual VM-derived modern User-pool identity")
            info = candidate["status"].get("nodeInfo") or {}
            boot = info.get("bootID")
            require(self.candidate_node is None or (
                self.candidate_node["node_name"] == name and self.candidate_node["node_uid"] == node_uid
            ), "Observed new Node UID changed")
            if boot:
                require(capacity.valid_uuid(boot) and boot not in {
                    self.summary["original_identity"]["boot_id"],
                    *self.modern["current_default_boot_ids"].values(),
                }, "Modern Node reused an original boot ID")
                pin = {"node_name": name, "node_uid": node_uid, "boot_id": boot}
                require(self.candidate_node is None or self.candidate_node["boot_id"] in (None, boot),
                        "Observed new Node boot changed")
                self.candidate_node = pin
            else:
                require(self.candidate_node is None or self.candidate_node["boot_id"] is None,
                        "Observed modern Node boot identity disappeared")
                self.candidate_node = {"node_name": name, "node_uid": node_uid, "boot_id": None}
            nnc = self.network_identity(snapshot, name, node_uid)
            if nnc is not None:
                require(not nnc["metadata"].get("deletionTimestamp"), "Modern Node network container is terminating")
                network = recovery.maintenance._replacement_nnc_map(snapshot["nnc"], name, node_uid).get(name)
            for pod in snapshot["pods"]["items"]:
                if pod["spec"].get("nodeName") == name:
                    require(recovery.pvc_free(pod["spec"]), "Modern host Pod has a PVC or ephemeral claim")
        else:
            require(self.candidate_node is None, "Observed modern Node disappeared")
        projected = dict(snapshot)
        projected["nodes"] = {"items": [row for row in snapshot["nodes"]["items"] if row is not candidate]}
        projected["nnc"] = {"items": [
            row for row in snapshot["nnc"]["items"]
            if candidate is None or row["metadata"]["name"] != candidate["metadata"]["name"]
        ]}
        self.guard(projected, host_optional=True)
        if candidate is None or not stable or network is None or not self.candidate_node["boot_id"]:
            return False
        original_network = recovery.maintenance._nnc_map(self.initial["nnc"])
        require(network["network_container_id"] not in {
            replacement.FAILED_NETWORK_CONTAINER, *(row["network_container_id"] for row in original_network.values()),
        } and network["uid"] not in {self.original_nnc_uid, *(row["uid"] for row in original_network.values())}
            and capacity.valid_uuid(network["uid"]), "Modern creation reused an original network-container identity")
        if not replacement.initialized_network(network) or not recovery.workers.node_is_ready(candidate):
            return False
        require(recovery.mocks._node_ready_and_schedulable(candidate)
                and not any(row.get("effect") in ("NoSchedule", "NoExecute")
                            for row in recovery.maintenance._taints(candidate)),
                "Modern Node still has scheduling holds")
        image = candidate["metadata"]["labels"].get("kubernetes.azure.com/node-image-version")
        require(image == self.live["pool"]["nodeImageVersion"]
                and info.get("kubeletVersion") == f"v{self.desired['kubernetes_patch']}"
                and info.get("operatingSystem") == "linux" and str(info.get("osImage", "")).startswith("Ubuntu"),
                "Modern Node does not prove the actual Ubuntu image and pinned Kubernetes patch")
        require(not any(key in recovery.maintenance._annotations(candidate)
                        for key in (recovery.MARKER_KEY, replacement.REPLACEMENT_KEY)),
                "Modern Node has an unexpected legacy action marker")
        self.derived = {
            **self.candidate_vm, **self.candidate_node, "pool_name": POOL_NAME, "vmss_name": self.new_vmss,
            "nnc_uid": network["uid"], "network_container_id": network["network_container_id"],
            "node_image_version": image, "kubernetes_patch": self.desired["kubernetes_patch"],
            "derived_at": recovery.workers.utc_now(),
        }
        self.host_node, self.host_provider_id, self.host_pool_name = (
            self.derived["node_name"], self.derived["provider_id"], POOL_NAME,
        )
        self.real_uids = {name: row_uid for name, row_uid in recovery.REAL_UIDS.items() if name != recovery.PROM_NODE}
        self.real_uids[self.host_node] = self.derived["node_uid"]
        self.stage = "active"
        self.modern["new_identity"] = copy.deepcopy(self.derived)
        self.summary["modern_prom"] = {
            **{key: self.derived[key] for key in (
                "pool_name", "vmss_name", "instance_id", "node_name", "node_uid",
                "vm_id", "provider_id", "network_container_id",
            )},
            "pool_resource_id": self.live["pool"]["id"],
            "pool_configuration_sha256": self.new_model_pin["pool"],
            "legacy_empty_pool_retired": False,
        }
        self.summary["modern_derived_manifest"] = {
            "schema_version": 1, "original_plan_sha256": self.summary["plan_sha256"],
            "real_node_uids": dict(self.real_uids), "modern_host": copy.deepcopy(self.derived),
            "kwok_node_uids": dict(self.plan["kwok_node_uids"]),
            "mock_pod_uids": dict(self.plan["mock_pod_uids"]),
            "ready_mock_pod_uids": dict(self.plan["ready_mock_pod_uids"]),
        }
        self.summary["modern_baseline_delta"]["image_delta"].update(
            after=image, changed=image != self.source_pool["nodeImageVersion"],
        )
        self.save()
        self.guard(snapshot)
        return self.system_ready(snapshot)

    def prove_memory(self, node, metric, reserve, *, pod_uid="", probe=False):
        require(recovery.timestamp(metric.get("timestamp"), "modern node metrics")
                >= recovery.timestamp(self.modern["create"]["requested_at"], "modern pool request"),
                "Node metrics predate the modern pool creation")
        return super().prove_memory(node, metric, reserve, pod_uid=pod_uid, probe=probe)

    def restore(self):
        raise recovery.workers.ReconcileError("Modern mode must never replay legacy Dv3 restoration")

    def create_pool(self):
        self.fresh_zero()
        self.plan_configuration()
        require(self.read_capacity(), "DSv5/regional 24-core headroom disappeared before any write")
        self.create_guard()
        self.fresh_zero()
        self.plan_configuration()
        require(self.read_capacity(), "DSv5/regional 24-core headroom disappeared before journaling creation")
        self.patch_guard("create-attempted")
        self.fresh_zero()
        self.plan_configuration()
        require(self.read_capacity(), "DSv5/regional 24-core headroom disappeared before the sole pool add")
        self.submit_pool("create", pool_add_command(self.desired["kubernetes_patch"]))
        self.stage = "creating"
        self.summary["status"] = "creating-modern-prom-pool"
        deadline = min(self.work_deadline, time.monotonic() + replacement.RESTORE_SECONDS)
        while True:
            self.authority()
            stable = self.models()
            if self.discover(self.snapshot(), stable):
                break
            self.wait(deadline, "Modern pool/VM/Node, initialized NC, and every applicable system DaemonSet")
        self.add_exclusions()
        self.probe_ip(self.snapshot(), self.targets[0])
        self.modern.update(host_ip_proven=True, host_proven_at=recovery.workers.utc_now())
        self.summary["restart"].update(
            skipped_reason="modern-pool-created-not-restarted", host_proven=True,
            current_boot_id=self.derived["boot_id"],
        )
        self.save()

    def retirement_preflight(self):
        require(self.modern["frameworks_proven"] and self.modern["host_ip_proven"],
                "The old empty pool cannot be retired before full framework and actual-IP proof")
        self.check_sources()
        self.owned_guard()
        self.authority(strict=True)
        require(self.models(), "Modern host model readiness regressed before empty-pool retirement")
        snapshot = self.snapshot()
        self.guard(snapshot)
        require(self.old_pool_references_absent(snapshot)
                and not self.live["old_pool_absent"] and not self.live["old_vmss_absent"]
                and self.dns_ready(snapshot) and self.system_ready(snapshot)
                and not self.exclusions and self.probe is None and not self.summary["cleanup_errors"],
                "Retirement requires the proven original empty pool and healthy clean frameworks")
        for target in self.targets:
            state, pod = self.target_state(snapshot, target)
            require(state != "delete-pinned" and recovery.pod_ready(pod), "Framework readiness regressed before retirement")

    def retire_empty_pool(self):
        self.retirement_preflight()
        self.patch_guard("retire-attempted")
        self.retirement_preflight()
        self.submit_pool("retire", list(RETIRE_COMMAND))
        self.stage = "retiring"
        self.summary["status"] = "retiring-original-empty-prom-pool"
        deadline = min(self.work_deadline, time.monotonic() + replacement.DELETE_SECONDS)
        while True:
            self.authority()
            stable = self.models()
            snapshot = self.snapshot()
            self.guard(snapshot)
            require(self.dns_ready(snapshot) and self.system_ready(snapshot),
                    "Framework or system readiness regressed while observing empty-pool retirement")
            if stable and self.live["old_pool_absent"] and self.live["old_vmss_absent"]:
                self.retired = True
                self.stage = "retired"
                self.modern.update(old_empty_pool_retired=True, retired_at=recovery.workers.utc_now())
                self.summary["modern_prom"]["legacy_empty_pool_retired"] = True
                self.summary["modern_baseline_delta"]["old_empty_pool_retired"] = True
                self.save()
                return
            self.wait(deadline, "Native empty prompool deletion and VMSS disappearance")

    def execute(self):
        require(getattr(self.args, "modern_prom_recovery", False)
                and self.args.replace_failed_host and self.args.resume_replacement,
                "Modern family repair requires explicit mode and both immutable original/native receipts")
        self.load_receipts()
        decisions = self.fresh_zero()
        self.guard_absent()
        self.plan_configuration()
        self.read_capacity()
        self.summary.update(
            plan_valid=True, effective_targets=self.targets,
            planned_actions={
                "host_action": "explicit-modern-prom-pool-creation", "restart_required": False,
                "legacy_restore_required": False, "pool": POOL_NAME, "pool_counts": [0, 1],
                "new_pool_configuration": copy.deepcopy(self.desired), "durable_guard": GUARD_NAME,
                "pods": decisions, "retire_only_empty_original_pool_after_strict_postproof": "prompool",
            },
            capacity_and_actual_ip_proof_required_at_execution=True,
        )
        self.save()
        if not self.args.execute:
            self.summary["status"] = "plan_valid"
            return
        deadline = min(self.work_deadline, time.monotonic() + self.args.quota_wait_seconds)
        while not self.modern["quota_ready"]:
            self.summary["status"] = "waiting-for-modern-family-quota"
            self.save()
            self.wait(deadline, "Fresh DSv5 and regional 24-core headroom")
            self.fresh_zero()
            self.guard_absent()
            self.plan_configuration()
            self.read_capacity()
        self.create_pool()
        for target in self.targets:
            self.check_sources()
            self.owned_guard()
            self.move_pod(target)
        self.cleanup()
        require(not self.summary["cleanup_errors"], "Owned cleanup failed; modern repair cannot be certified")
        self.postproof()
        self.modern.update(frameworks_proven=True, frameworks_proven_at=recovery.workers.utc_now())
        self.patch_guard("frameworks-proven")
        self.retire_empty_pool()
        self.postproof()
        self.check_sources()
        self.patch_guard("completed")
        self.summary.update(repaired=True, status="repaired", phase1_only=True, workloads_ready=False)
        self.save()
