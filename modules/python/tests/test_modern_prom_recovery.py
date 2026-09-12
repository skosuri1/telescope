"""Offline modern-pool repair tests; the runner rejects all unspecified writes."""

# pylint: disable=protected-access,too-many-lines

from __future__ import annotations

import copy
import importlib
import json
import sys
from pathlib import Path

import pytest

from . import test_failed_prom_capacity_resume as capacity_tests


base = capacity_tests.base
native_tests = capacity_tests.native_tests
recovery = base.recovery
sys.path.insert(0, str(base.MODULE_DIR))
try:
    modern = importlib.import_module("modern_prom_recovery")
finally:
    sys.path.pop(0)

PATCH = "1.35.4"
OLD_IMAGE = "AKSUbuntu-2404containerd-202608.26.0"
NEW_IMAGE = "AKSUbuntu-2404containerd-202609.10.0"
NEW_VMSS = "aks-promv5-73458291-vmss"
NEW_NODE = f"{NEW_VMSS}000000"
NEW_UID = base.uid("modern-prom-node")
NEW_VM_ID = base.uid("modern-prom-vm")
NEW_NC = base.uid("modern-prom-nc")
NEW_NNC_UID = base.uid("modern-prom-nnc-object")


def guest_model(image):
    return {
        "osDisk": {"osType": "Linux", "diskSizeGb": 256,
                   "managedDisk": {"storageAccountType": "StandardSSD_LRS"}},
        "imageReference": {
            "id": f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/aks-images/"
                  f"providers/Microsoft.Compute/galleries/AKSUbuntu/images/2404containerd/versions/{image}",
        },
    }


class ModernCloud(capacity_tests.CapacityCloud):
    """Keep the real native lineage, but model only one modern add/empty delete."""

    def __init__(self, plan, args):
        super().__init__(plan, args)
        self.adds, self.retirements = [], []
        self.on_add = None
        self.on_retire = None
        self.add_error = False
        self.retire_error = False
        self.legacy_guards = []
        self.instance_id = "0"
        self.new_name = NEW_NODE
        self.skus = [{
            "name": modern.VM_SIZE, "family": modern.QUOTA_FAMILY, "resourceType": "virtualMachines",
            "locations": ["EastUS2EUAP"], "restrictions": [],
            "capabilities": [
                {"name": name, "value": value} for name, value in {
                    "vCPUs": "8", "vCPUsAvailable": "8", "MemoryGB": "32",
                    "CpuArchitectureType": "x64", "PremiumIO": "True", "VMDeploymentTypes": "IaaS",
                    "EphemeralOSDiskSupported": "False", "OSVhdSizeMB": "1047552",
                }.items()
            ],
        }]
        self.modern_scale_view = {}
        for pool in self.pools:
            default = pool["name"] == "default"
            pool.update(copy.deepcopy(modern.POOL_SETTINGS))
            pool.update(mode="System" if default else "User", maxPods=110 if default else 250,
                        nodeLabels=None if default else {"prometheus": "true"}, vmSize="Standard_D8_v3",
                        orchestratorVersion="1.35", currentOrchestratorVersion=PATCH,
                        type="Microsoft.ContainerService/managedClusters/agentPools",
                        upgradeSettings={
                            "maxSurge": "10%", "maxUnavailable": "0", "maxBlockedNodes": None,
                            "drainTimeoutInMinutes": None, "nodeSoakDurationInMinutes": None,
                            "undrainableNodeBehavior": None,
                        })
        for name in recovery.REAL_UIDS:
            self.nodes[name]["status"]["nodeInfo"].update(
                kubeletVersion=f"v{PATCH}", operatingSystem="linux", osImage="Ubuntu 24.04.3 LTS",
            )
        for vmss in self.vmsses:
            vmss["virtualMachineProfile"] = {
                "storageProfile": guest_model("202608.26.0"),
                "osProfile": {"customData": "must-not-be-published"},
                "extensionProfile": {"extensions": [{"protectedSettings": {"secret": "must-not-be-published"}}]},
            }
        for instances in self.instances.values():
            for vm in instances:
                vm["osProfile"] = {"computerName": vm["computerName"], "customData": "must-not-be-published"}

    def azure(self, command):
        route = command[1:3]
        if route == ["aks", "show"]:
            assert self.value(command, "--resource-group") == recovery.RESOURCE_GROUP
            assert self.value(command, "--name") == recovery.CLUSTER
            assert self.value(command, "--query") == modern.PATCH_QUERY
            return base.jmespath.search(modern.PATCH_QUERY, self.clusters[95])
        if route == ["vm", "list-skus"]:
            assert self.value(command, "--location") == recovery.REGION and "--all" in command
            assert self.value(command, "--resource-type") == "virtualMachines"
            assert self.value(command, "--query") == modern.SKU_QUERY
            return base.jmespath.search(modern.SKU_QUERY, self.skus)
        if route == ["vmss", "list"]:
            return base.jmespath.search(self.value(command, "--query"), self.vmsses)
        if route == ["aks", "list"]:
            return base.jmespath.search(self.value(command, "--query"), self.clusters)
        if route == ["vmss", "list-instances"]:
            assert self.value(command, "--query") == recovery.VM_QUERY
            rows = self.instances[self.value(command, "--name")]
            return base.jmespath.search(recovery.VM_QUERY, rows)
        if route == ["vmss", "show"]:
            assert self.value(command, "--name") == NEW_VMSS
            assert self.value(command, "--resource-group") == recovery.NODE_GROUP
            assert self.value(command, "--query") == modern.VMSS_MODEL_QUERY
            row = next(vmss for vmss in self.vmsses if vmss["name"] == NEW_VMSS)
            return base.jmespath.search(modern.VMSS_MODEL_QUERY, row)
        if route == ["vmss", "get-instance-view"] and self.value(command, "--name") == NEW_VMSS:
            if "--instance-id" in command:
                assert self.value(command, "--instance-id") == self.instance_id
                return base.jmespath.search(
                    recovery.VIEW_QUERY, self.views[(NEW_VMSS, self.instance_id)],
                )
            return base.jmespath.search(recovery.SCALE_VIEW_QUERY, {
                "statuses": self.modern_scale_view.get("statuses"),
                "virtualMachine": {"statusesSummary": self.modern_scale_view.get("virtualMachines")},
            })
        if command[1:4] == ["aks", "nodepool", "add"]:
            assert command == [*modern.pool_add_command(PATCH), "--subscription", recovery.SUBSCRIPTION]
            assert not self.adds and len(self.configmaps) == 1
            assert self.pools[1]["count"] == self.vmsses[1]["sku"]["capacity"] == 0
            assert self.instances[recovery.PROM_VMSS] == []
            receipt = base.receipt()
            request = receipt["modern_prom_recovery"]["create"]
            journal = json.loads(self.configmaps[0]["data"]["record"])
            assert request["attempted"] and request["accepted"] is None and request["ambiguous"]
            assert journal["state"] == "create-attempted" and journal["create"]["ambiguous"]
            assert receipt["modern_prom_recovery"]["quota"]["required_cores"] == 24
            assert receipt["quota_ready"] and receipt["original_identity"]["node_name"] == recovery.PROM_NODE
            assert not receipt["replacement"]["restore"]["attempted"]
            self.adds.append(command)
            self.writes.append(command)
            (self.on_add or self.finish_add)()
            if self.add_error:
                raise recovery.workers.ReconcileError("modern add response lost")
            return ""
        if command[1:4] == ["aks", "nodepool", "delete"]:
            assert command == [*modern.RETIRE_COMMAND, "--subscription", recovery.SUBSCRIPTION]
            assert len(self.adds) == 1 and not self.retirements
            receipt = base.receipt()
            request = receipt["modern_prom_recovery"]["retire"]
            journal = json.loads(self.configmaps[0]["data"]["record"])
            assert request["attempted"] and request["accepted"] is None and request["ambiguous"]
            assert journal["state"] == "retire-attempted"
            assert receipt["modern_prom_recovery"]["frameworks_proven"]
            assert receipt["modern_prom_recovery"]["host_ip_proven"]
            assert receipt["fleet_connected"] and receipt["cilium_proof"]["healthy"]
            assert len(receipt["pod_moves"]) == 5 and receipt["dns_proof"]["ready_replicas"] == 5
            assert not receipt["temporary_exclusions"] and not receipt["cleanup_errors"]
            assert not receipt.get("probe_cleanup_pending")
            assert next(row for row in self.pools if row["name"] == "prompool")["count"] == 0
            assert not self.instances[recovery.PROM_VMSS] and recovery.PROM_NODE not in self.nodes
            assert not any(row["spec"].get("nodeName", "").startswith(recovery.PROM_VMSS) for row in self.pods)
            self.retirements.append(command)
            self.writes.append(command)
            if self.retire_error:
                raise recovery.workers.ReconcileError("empty retirement response lost")
            (self.on_retire or self.finish_retirement)()
            return ""
        if self.continuing:
            assert command[1:4] not in (["aks", "nodepool", "scale"], ["aks", "nodepool", "delete-machines"])
        return super().azure(command)

    def finish_add(self):
        source = next(row for row in self.pools if row["name"] == "prompool")
        pool = copy.deepcopy(source)
        pool.update(
            id=source["id"].rsplit("/", 1)[0] + f"/{modern.POOL_NAME}", name=modern.POOL_NAME,
            count=1, vmSize=modern.VM_SIZE, orchestratorVersion=PATCH, currentOrchestratorVersion=PATCH,
            nodeImageVersion=NEW_IMAGE, provisioningState="Succeeded",
        )
        self.pools[:] = [row for row in self.pools if row["name"] != modern.POOL_NAME] + [pool]
        vmss_id = (
            f"/subscriptions/{recovery.SUBSCRIPTION}/resourceGroups/{recovery.NODE_GROUP}/"
            f"providers/Microsoft.Compute/virtualMachineScaleSets/{NEW_VMSS}"
        )
        self.vmsses[:] = [row for row in self.vmsses if row["name"] != NEW_VMSS] + [{
            "id": vmss_id, "name": NEW_VMSS, "location": recovery.REGION,
            "tags": {"aks-managed-poolName": modern.POOL_NAME}, "orchestrationMode": "Uniform",
            "provisioningState": "Succeeded", "sku": {"name": modern.VM_SIZE, "capacity": 1},
            "virtualMachineProfile": {
                "storageProfile": guest_model("202609.10.0"),
                "osProfile": {"customData": "must-not-be-published"},
            },
        }]
        resource = f"{vmss_id}/virtualMachines/{self.instance_id}"
        self.instances[NEW_VMSS] = [{
            "id": resource, "name": f"{NEW_VMSS}_{self.instance_id}", "instanceId": self.instance_id,
            "osProfile": {"computerName": self.new_name}, "vmId": NEW_VM_ID,
            "latestModelApplied": True, "provisioningState": "Succeeded",
        }]
        self.views[(NEW_VMSS, self.instance_id)] = {
            "statuses": [{"code": "PowerState/running"}, {"code": "ProvisioningState/succeeded"}],
            "extensions": [{"name": "vmssCSE", "statuses": [{"code": "ProvisioningState/succeeded"}]}],
        }
        self.modern_scale_view = {
            "statuses": [{"code": "ProvisioningState/succeeded"}],
            "virtualMachines": [{"code": "ProvisioningState/succeeded", "count": 1}],
        }
        node = base.make_node(self.new_name, NEW_UID, pool=modern.POOL_NAME, instance=self.instance_id)
        node["spec"]["providerID"] = f"azure://{resource}"
        node["metadata"]["labels"].update(
            {"kubernetes.azure.com/node-image-version": NEW_IMAGE, "prometheus": "true"},
        )
        node["status"]["nodeInfo"].update(
            kubeletVersion=f"v{PATCH}", operatingSystem="linux", osImage="Ubuntu 24.04.3 LTS",
        )
        self.nodes[self.new_name] = node
        self.nncs = [row for row in self.nncs if row["metadata"]["name"] != self.new_name] + [{
            "metadata": {
                **base.metadata(self.new_name, "kube-system", NEW_NNC_UID),
                "ownerReferences": [base.reference("Node", self.new_name, NEW_UID)],
            },
            "spec": {"requestedIPCount": 64},
            "status": {"assignedIPCount": 64, "networkContainers": [{
                "id": NEW_NC, "version": 1,
                "ipAssignments": [{"ip": f"10.96.1.{index}"} for index in range(1, 65)],
            }]},
        }]
        self.pods = [row for row in self.pods if row["spec"].get("nodeName") != self.new_name]
        owners = set()
        for original in self.old_system_pods:
            owner = original["metadata"]["ownerReferences"][0]
            if owner["uid"] in owners:
                continue
            owners.add(owner["uid"])
            pod = copy.deepcopy(original)
            name = f"{owner['name']}-modern-prom"
            pod["metadata"].update(name=name, uid=base.uid(name), resourceVersion="1")
            pod["metadata"].pop("deletionTimestamp", None)
            pod["spec"]["nodeName"] = self.new_name
            container_name = pod["status"]["containerStatuses"][0]["name"]
            pod["status"] = base.ready_status()
            pod["status"]["containerStatuses"][0]["name"] = container_name
            self.pods.append(pod)
        self.operation.clear()
        self.operation.update({
            "name": base.uid("modern-operation"), "status": "Succeeded", "operationType": "PutAgentPool",
            "startTime": base.now(), "endTime": base.now(), "errorCode": None,
        })

    def finish_retirement(self):
        self.pools[:] = [row for row in self.pools if row["name"] != "prompool"]
        self.vmsses[:] = [row for row in self.vmsses if row["name"] != recovery.PROM_VMSS]
        del self.instances[recovery.PROM_VMSS]
        self.operation.clear()
        self.operation.update({
            "name": base.uid("modern-retirement"), "status": "Succeeded", "operationType": "DeleteAgentPool",
            "startTime": base.now(), "endTime": base.now(), "errorCode": None,
        })

    def kubernetes(self, command):
        if "get" in command and "configmaps" in command:
            assert self.value(command, "-n") == "kube-system"
            field = self.value(command, "--field-selector")
            assert field in (f"metadata.name={modern.GUARD_NAME}", f"metadata.name={capacity_tests.resume.GUARD_NAME}")
            rows = self.configmaps if field == f"metadata.name={modern.GUARD_NAME}" else self.legacy_guards
            return {"apiVersion": "v1", "kind": "ConfigMapList", "metadata": {}, "items": rows}
        if "create" in command and "configmap" in command:
            assert command[command.index("configmap") + 1] == modern.GUARD_NAME
            assert self.value(command, "-n") == "kube-system"
            self.writes.append(command)
            request = base.receipt()["modern_prom_recovery"]["attempt_guard"]["create"]
            assert request["attempted"] and request["accepted"] is None and request["ambiguous"]
            if self.create_conflict:
                self.configmaps = [{"metadata": base.metadata(modern.GUARD_NAME, "kube-system")}]
            if self.configmaps:
                raise recovery.workers.ReconcileError("ConfigMap AlreadyExists")
            data = dict(word.removeprefix("--from-literal=").split("=", 1)
                        for word in command if word.startswith("--from-literal="))
            row = {"apiVersion": "v1", "kind": "ConfigMap",
                   "metadata": base.metadata(modern.GUARD_NAME, "kube-system"), "data": data}
            self.configmaps.append(row)
            if self.create_error:
                raise recovery.workers.ReconcileError("modern guard response lost")
            return row
        if "patch" in command and "configmap" in command:
            assert command[command.index("configmap") + 1] == modern.GUARD_NAME
            self.writes.append(command)
            operations = json.loads(self.value(command, "-p"))
            assert {row["path"] for row in operations if row["op"] == "test"} == {
                "/metadata/uid", "/metadata/resourceVersion", "/data/token", "/data",
            }
            self.apply_patch(self.configmaps[0], operations)
            state = json.loads(self.configmaps[0]["data"]["record"])["state"]
            if self.patch_error == state:
                raise recovery.workers.ReconcileError("modern journal response lost")
            return self.configmaps[0]
        return super().kubernetes(command)

    def delete(self, cluster, **kwargs):
        callback = self.delete_callback
        self.delete_callback = None
        name = kwargs["name"]
        if not name.startswith("prom-recovery-ip-"):
            receipt = base.receipt()
            assert receipt["modern_prom_recovery"]["host_ip_proven"]
            assert receipt["modern_prom_recovery"]["new_identity"]["node_uid"] == NEW_UID
            assert receipt["ip_proofs"] and len(self.adds) == 1 and not self.retirements
        try:
            base.FakeCloud.delete(self, cluster, **kwargs)
        finally:
            self.delete_callback = callback
        if not name.startswith("prom-recovery-ip-"):
            pod = self.get_pod(f"{name}-replacement")
            pod["spec"]["nodeName"] = self.new_name
            if callback:
                callback(None, pod)


@pytest.fixture(name="environment")
def modern_environment(tmp_path, monkeypatch):
    monkeypatch.setattr(capacity_tests, "CapacityCloud", ModernCloud)
    monkeypatch.setattr(native_tests, "IMAGE", OLD_IMAGE)
    args, plan, fake = capacity_tests.capacity_environment.__wrapped__(tmp_path, monkeypatch)
    args.modern_prom_recovery = True
    fake.clusters[95].update(kubernetesVersion="1.35", currentKubernetesVersion=PATCH)
    fake.usage = [
        {"name": {"value": modern.QUOTA_FAMILY, "localizedValue": "Standard DSv5 Family vCPUs"},
         "currentValue": "100", "limit": "1000"},
        {"name": {"value": "cores", "localizedValue": "Total Regional vCPUs"},
         "currentValue": 6785, "limit": 10000},
        {"name": {"value": "standardDv3Family"}, "currentValue": 5464, "limit": 5000},
    ]
    return args, plan, fake


def run(environment, *, execute=False):
    return base.run(environment, execute=execute)


def no_writes(fake):
    assert not fake.writes and not fake.deleted and not fake.adds and not fake.retirements
    assert not base.receipt()["mutation_started"]


@pytest.mark.parametrize("ready", [True, False])
def test_plan_is_zero_write_and_honest_about_modern_delta(environment, ready):
    args, plan, fake = environment
    plan_before = copy.deepcopy(plan)
    native = capacity_tests.source(environment)
    original_bytes = Path(args.replace_failed_host).read_bytes()
    native_bytes = Path(args.resume_replacement).read_bytes()
    if not ready:
        fake.usage[0].update(currentValue="1001", limit="1000")
    summary = run(environment)
    assert summary["status"] == "plan_valid" and summary["plan_valid"] and summary["quota_ready"] is ready
    assert summary["original_identity"] == native["original_identity"]
    assert summary["original_model_pins"] == native["original_model_pins"]
    assert summary["replacement"]["delete"] == native["replacement"]["delete"]
    assert summary["replacement"]["previous_restore"] == native["replacement"]["restore"]
    assert not summary["replacement"]["restore"]["attempted"]
    assert summary["controller_pins"] == native["controller_pins"] and summary["pdb_pins"] == native["pdb_pins"]
    desired = summary["modern_prom_recovery"]["desired_configuration"]
    assert desired["name"] == "promv5" and desired["vmSize"] == "Standard_D8s_v5"
    assert desired["kubernetes_patch"] == PATCH and desired["osDiskType"] == "Managed"
    assert summary["modern_baseline_delta"]["intentional_baseline_change"]
    assert not summary["modern_baseline_delta"]["original_baseline_unchanged"]
    assert summary["modern_baseline_delta"]["image_delta"]["after"] is None
    assert "modern_prom" not in summary
    assert summary["modern_prom_recovery"]["quota"]["counters"][modern.QUOTA_FAMILY]["remaining"] == (900 if ready else -1)
    assert plan == plan_before and json.loads(Path(args.plan_file).read_text(encoding="utf-8")) == plan_before
    assert Path(args.replace_failed_host).read_bytes() == original_bytes
    assert Path(args.resume_replacement).read_bytes() == native_bytes
    assert not summary["repaired"] and not summary["workloads_ready"] and summary["phase1_only"]
    assert "must-not-be-published" not in json.dumps(summary)
    no_writes(fake)


@pytest.mark.parametrize("instance,name", [("0", NEW_NODE), ("35", f"{NEW_VMSS}00000z")])
def test_one_add_five_pinned_moves_then_one_empty_retirement(environment, instance, name):
    args, plan, fake = environment
    fake.instance_id, fake.new_name = instance, name
    defaults = {key: copy.deepcopy(value) for key, value in fake.nodes.items() if key in recovery.REAL_UIDS}
    mock_uids = {row["metadata"]["name"]: recovery.object_uid(row) for row in fake.pods
                 if row["metadata"].get("namespace") == "mock-clustermesh"}
    native_bytes = Path(args.resume_replacement).read_bytes()
    summary = run(environment, execute=True)
    record = summary["modern_prom_recovery"]
    assert summary["repaired"] and summary["success"] and summary["status"] == "repaired"
    assert summary["phase1_only"] and summary["workloads_ready"] is False
    assert len(fake.adds) == len(fake.retirements) == 1 and not fake.new_scales
    assert record["create"]["accepted"] and record["retire"]["accepted"] and record["old_empty_pool_retired"]
    assert not record["automatic_retry_allowed"] and record["host_ip_proven"] and record["frameworks_proven"]
    assert record["new_identity"]["instance_id"] == instance and record["new_identity"]["node_name"] == name
    assert record["new_identity"]["node_uid"] == NEW_UID and record["new_identity"]["vm_id"] == NEW_VM_ID
    assert record["new_identity"]["network_container_id"] == NEW_NC
    assert record["new_identity"]["provider_id"].lower().endswith(f"/{NEW_VMSS}/virtualmachines/{instance}")
    actual_pool = next(row for row in fake.pools if row["name"] == modern.POOL_NAME)
    assert summary["modern_prom"] == {
        "pool_name": "promv5", "pool_resource_id": actual_pool["id"], "vmss_name": NEW_VMSS,
        "instance_id": instance, "node_name": name, "node_uid": NEW_UID, "vm_id": NEW_VM_ID,
        "provider_id": fake.nodes[name]["spec"]["providerID"].lower(), "network_container_id": NEW_NC,
        "pool_configuration_sha256": recovery.digest(recovery.prepared.pool_configuration(actual_pool)),
        "legacy_empty_pool_retired": True,
    }
    assert summary["original_identity"]["provider_id"] == recovery.PROVIDER
    assert summary["modern_baseline_delta"]["image_delta"] == {
        "before": OLD_IMAGE, "after": NEW_IMAGE, "changed": True,
    }
    assert summary["modern_baseline_delta"]["old_empty_pool_retired"]
    assert {row["name"] for row in fake.pools} == {"default", "promv5"}
    assert {row["name"] for row in fake.vmsses} == {recovery.DEFAULT_VMSS, NEW_VMSS}
    assert summary["original_model_pins"] == capacity_tests.source(environment)["original_model_pins"]
    assert [(row["deployment_name"], row["state"]) for row in summary["pod_moves"]] == [
        ("coredns", "moved"), ("coredns", "moved"), ("clustermesh-apiserver", "moved"),
        ("kube-state-metrics", "moved"), ("grafana", "moved"),
    ]
    assert len(summary["ip_proofs"]) == 6 and all(row["ready"] and row["node_uid"] == NEW_UID
                                                for row in summary["ip_proofs"])
    commitments = summary["memory_commitments"]
    assert all(row["reserved_memory_bytes"] == 8 * 1024**3 for key, row in commitments.items()
               if "/clustermesh-apiserver-" in key or "/grafana-" in key)
    assert sum(row["reserved_memory_bytes"] for row in commitments.values()) >= 18 * 1024**3
    assert summary["final_mock_ready"] == 71 and summary["final_mock_pending"] == 29
    assert summary["final_kwok_ready"] == 100 and summary["cilium_proof"]["cilium_agent_count"] == 3
    assert summary["fleet_connected"] and not summary["restart"]["attempted"]
    assert not summary["temporary_exclusions"] and not summary["cleanup_errors"]
    assert {row["metadata"]["name"]: recovery.object_uid(row) for row in fake.pods
            if row["metadata"].get("namespace") == "mock-clustermesh"} == mock_uids
    for key, original in defaults.items():
        assert fake.nodes[key]["spec"] == original["spec"]
        assert fake.nodes[key]["status"]["nodeInfo"] == original["status"]["nodeInfo"]
        assert recovery.object_uid(fake.nodes[key]) == plan["real_node_uids"][key]
    assert len(fake.configmaps) == 1
    journal = json.loads(fake.configmaps[0]["data"]["record"])
    contract = json.loads(fake.configmaps[0]["data"]["contract"])
    assert journal["state"] == "completed" and journal["new_identity"] == record["new_identity"]
    assert contract["original_identity"] == summary["original_identity"]
    assert contract["desired_configuration"] == record["desired_configuration"]
    assert Path(args.resume_replacement).read_bytes() == native_bytes
    assert "must-not-be-published" not in json.dumps(summary)


@pytest.mark.parametrize("counter", ["family", "regional"])
@pytest.mark.parametrize("remaining", [-464, 0, 8, 16, 23])
def test_twenty_four_fresh_cores_are_required_before_any_write(environment, counter, remaining):
    fake = environment[2]
    fake.usage[0 if counter == "family" else 1].update(limit="1000", currentValue=str(1000 - remaining))
    with pytest.raises(recovery.workers.ReconcileError, match="headroom"):
        run(environment, execute=True)
    no_writes(fake)


@pytest.mark.parametrize("value", [True, False, -1, 1.0, "1.0", "-1", "+24", " 24", "٢٤", None])
def test_malformed_quota_counters_are_never_capacity(environment, value):
    fake = environment[2]
    fake.usage[0]["limit"] = value
    with pytest.raises(recovery.workers.ReconcileError, match="counter"):
        run(environment, execute=True)
    no_writes(fake)


@pytest.mark.parametrize("fault", ["restricted", "wrong-family", "wrong-region", "wrong-size", "duplicate", "ephemeral",
                                  "cpu", "memory", "disk"])
def test_fresh_actual_sku_is_required_even_with_quota(environment, fault):
    fake = environment[2]
    sku = fake.skus[0]
    if fault == "restricted":
        sku["restrictions"] = [{"type": "Location", "reasonCode": "NotAvailableForSubscription"}]
    elif fault == "wrong-family":
        sku["family"] = "standardDv3Family"
    elif fault == "wrong-region":
        sku["locations"] = ["westus2"]
    elif fault == "wrong-size":
        sku["name"] = "Standard_D8ds_v5"
    elif fault == "duplicate":
        fake.skus.append(copy.deepcopy(sku))
    else:
        key, value = {
            "ephemeral": ("EphemeralOSDiskSupported", "True"), "cpu": ("vCPUs", "16"),
            "memory": ("MemoryGB", "16"), "disk": ("OSVhdSizeMB", "65536"),
        }[fault]
        next(row for row in sku["capabilities"] if row["name"] == key)["value"] = value
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    no_writes(fake)


@pytest.mark.parametrize("fault", ["pool", "modern-marker", "legacy-marker", "default-uid", "default-boot-unready",
                                  "mock-uid", "healthy-mock", "kwok-uid", "controller", "pdb", "source-nc",
                                  "old-pod", "patch-missing", "patch-disagreement", "framework-pvc", "scheduling-gate",
                                  "partial-before-create", "other-fleet"])
def test_unsafe_initial_state_never_writes(environment, fault):
    _, _, fake = environment
    if fault == "pool":
        fake.finish_add()
    elif fault in ("modern-marker", "legacy-marker"):
        name = modern.GUARD_NAME if fault == "modern-marker" else capacity_tests.resume.GUARD_NAME
        row = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": base.metadata(name, "kube-system"),
               "data": {"owner": "foreign"}}
        (fake.configmaps if fault == "modern-marker" else fake.legacy_guards).append(row)
    elif fault == "default-uid":
        fake.nodes[recovery.SOURCE_NODE]["metadata"]["uid"] = base.uid("changed-default")
    elif fault == "default-boot-unready":
        fake.nodes[recovery.SOURCE_NODE]["status"]["conditions"][0]["status"] = "False"
    elif fault == "mock-uid":
        fake.get_pod("kwok-node-99")["metadata"]["uid"] = base.uid("changed-pending-mock")
    elif fault == "healthy-mock":
        fake.get_pod("kwok-node-0")["status"] = base.ready_status(False)
    elif fault == "kwok-uid":
        fake.nodes["kwok-node-99"]["metadata"]["uid"] = base.uid("changed-kwok")
    elif fault == "controller":
        fake.get_controller("azure-cns", "DaemonSet")["spec"]["template"]["spec"]["containers"][0]["image"] = "changed"
    elif fault == "pdb":
        fake.pdbs[0]["spec"]["minAvailable"] = 0
    elif fault == "source-nc":
        fake.nncs[0]["status"]["networkContainers"][0]["id"] = base.uid("changed-source")
    elif fault == "old-pod":
        pod = copy.deepcopy(fake.get_pod("kwok-node-99"))
        pod["spec"]["nodeName"] = recovery.PROM_NODE
        fake.pods.append(pod)
    elif fault == "patch-missing":
        fake.clusters[95]["currentKubernetesVersion"] = "1.35"
    elif fault == "patch-disagreement":
        fake.nodes[recovery.SOURCE_NODE]["status"]["nodeInfo"]["kubeletVersion"] = "v1.35.3"
    elif fault == "framework-pvc":
        fake.get_pod(fake.plan["api_pod_name"])["spec"]["volumes"] = [
            {"name": "data", "persistentVolumeClaim": {"claimName": "unsafe"}},
        ]
    elif fault == "scheduling-gate":
        fake.get_pod(fake.plan["api_pod_name"])["spec"]["schedulingGates"] = [{"name": "gate"}]
    else:
        index = 95 if fault == "partial-before-create" else 94
        fake.members[index]["meshProperties"]["status"] = {
            "state": "Disconnected", "error": {"code": "PartialConnectivity"},
        }
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    no_writes(fake)


@pytest.mark.parametrize("fault", ["vm-id", "provider", "node-uid", "node-boot", "node-provider", "node-pool",
                                  "node-image", "node-patch", "node-os", "vm-os", "vm-disk", "vm-ephemeral",
                                  "pool-image", "pool-size", "pool-label", "pool-upgrade", "pool-patch",
                                  "vmss-owner", "extra-vm", "default-count", "default-vm", "default-boot",
                                  "source-nc", "nc-owner", "old-nc", "healthy-mock", "controller", "pdb",
                                  "terminal-vm", "terminal-extension", "new-pvc", "tainted", "legacy-marker",
                                  "vmss-image"])
def test_owned_add_does_not_authorize_invalid_host_or_healthy_drift(environment, fault):
    fake = environment[2]

    def corrupt():
        fake.finish_add()
        vm = fake.instances[NEW_VMSS][0]
        node = fake.nodes[NEW_NODE]
        pool = fake.pools[-1]
        vmss = fake.vmsses[-1]
        network = next(row for row in fake.nncs if row["metadata"]["name"] == NEW_NODE)
        if fault == "vm-id":
            vm["vmId"] = recovery.FAILED_PROM_VM_ID
        elif fault == "provider":
            vm["id"] = recovery.PROVIDER.removeprefix("azure://")
        elif fault == "node-uid":
            node["metadata"]["uid"] = recovery.REAL_UIDS[recovery.PROM_NODE]
        elif fault == "node-boot":
            node["status"]["nodeInfo"]["bootID"] = fake.nodes[recovery.SOURCE_NODE]["status"]["nodeInfo"]["bootID"]
        elif fault == "node-provider":
            node["spec"]["providerID"] = recovery.PROVIDER
        elif fault == "node-pool":
            node["metadata"]["labels"]["agentpool"] = "prompool"
        elif fault == "node-image":
            node["metadata"]["labels"]["kubernetes.azure.com/node-image-version"] = OLD_IMAGE
        elif fault == "node-patch":
            node["status"]["nodeInfo"]["kubeletVersion"] = "v1.35.5"
        elif fault == "node-os":
            node["status"]["nodeInfo"]["osImage"] = "Azure Linux"
        elif fault in ("vm-os", "vm-disk", "vm-ephemeral"):
            disk = vmss["virtualMachineProfile"]["storageProfile"]["osDisk"]
            disk.update({"vm-os": {"osType": "Windows"}, "vm-disk": {"diskSizeGb": 128},
                         "vm-ephemeral": {"diffDiskSettings": {"option": "Local"}}}[fault])
        elif fault == "pool-image":
            pool["nodeImageVersion"] = "AKSAzureLinux-202609.10.0"
        elif fault == "pool-size":
            pool["vmSize"] = "Standard_D8_v3"
        elif fault == "pool-label":
            pool["nodeLabels"] = {"prometheus": "false"}
        elif fault == "pool-upgrade":
            pool["upgradeSettings"]["maxUnavailable"] = "1"
        elif fault == "pool-patch":
            pool["orchestratorVersion"] = "1.35"
        elif fault == "vmss-owner":
            vmss["tags"]["aks-managed-poolName"] = "default"
        elif fault == "extra-vm":
            fake.instances[NEW_VMSS].append(copy.deepcopy(vm))
        elif fault == "default-count":
            fake.pools[0]["count"] = 3
        elif fault == "default-vm":
            fake.instances[recovery.DEFAULT_VMSS][0]["vmId"] = base.uid("changed-default-vm")
        elif fault == "default-boot":
            fake.nodes[recovery.SOURCE_NODE]["status"]["nodeInfo"]["bootID"] = base.uid("changed-default-boot")
        elif fault == "source-nc":
            fake.nncs[0]["status"]["networkContainers"][0]["id"] = base.uid("changed-source-nc")
        elif fault == "nc-owner":
            network["metadata"]["ownerReferences"][0]["uid"] = base.uid("wrong-nc-owner")
        elif fault == "old-nc":
            network["status"]["networkContainers"][0]["id"] = native_tests.replacement.FAILED_NETWORK_CONTAINER
        elif fault == "healthy-mock":
            fake.get_pod("kwok-node-1")["status"] = base.ready_status(False)
        elif fault == "controller":
            fake.get_controller("azure-cns", "DaemonSet")["metadata"]["uid"] = base.uid("foreign-ds")
        elif fault == "pdb":
            fake.pdbs[0]["spec"]["minAvailable"] = 0
        elif fault == "terminal-vm":
            vm["provisioningState"] = "Failed"
        elif fault == "terminal-extension":
            fake.views[(NEW_VMSS, "0")]["extensions"][0]["statuses"][0]["code"] = "ProvisioningState/failed"
        elif fault == "new-pvc":
            fake.get_pod("cilium-modern-prom")["spec"]["volumes"] = [
                {"name": "unsafe", "ephemeral": {"volumeClaimTemplate": {}}},
            ]
        elif fault == "tainted":
            node["spec"]["taints"] = [{"key": "hold", "effect": "NoSchedule"}]
        elif fault == "legacy-marker":
            node["metadata"]["annotations"][recovery.MARKER_KEY] = "foreign-action"
        elif fault == "vmss-image":
            vmss["virtualMachineProfile"]["storageProfile"]["imageReference"]["id"] = guest_model(
                "202608.26.0",
            )["imageReference"]["id"]

    fake.on_add = corrupt
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    assert len(fake.adds) == 1 and not fake.retirements and not fake.deleted
    assert not base.receipt()["modern_prom_recovery"]["host_ip_proven"]
    assert not base.receipt()["repaired"]


@pytest.mark.parametrize("fault", ["nc-uninitialized", "nc-addresses", "node-not-ready", "daemonset-missing",
                                  "probe-not-ready", "metrics-old", "metrics-missing", "memory", "cpu"])
def test_readiness_and_real_ip_memory_failures_never_retire(environment, monkeypatch, fault):
    fake = environment[2]
    base.abort_wait(monkeypatch)

    def not_ready():
        fake.finish_add()
        network = next(row for row in fake.nncs if row["metadata"]["name"] == NEW_NODE)
        if fault == "nc-uninitialized":
            network["status"]["networkContainers"] = []
        elif fault == "nc-addresses":
            network["status"]["networkContainers"][0]["ipAssignments"] = []
        elif fault == "node-not-ready":
            fake.nodes[NEW_NODE]["status"]["conditions"][0]["status"] = "False"
        elif fault == "daemonset-missing":
            fake.pods.remove(fake.get_pod("cloud-node-manager-modern-prom"))

    fake.on_add = not_ready
    if fault == "probe-not-ready":
        fake.probe_ready = False
    elif fault == "metrics-old":
        fake.metrics_timestamp = "2000-01-01T00:00:00Z"
    elif fault == "metrics-missing":
        fake.metrics_missing = True
    elif fault == "memory":
        fake.memory = "26Gi"
    elif fault == "cpu":
        fake.cpu = "7700m"
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    assert len(fake.adds) == 1 and not fake.retirements
    assert not [row for row in fake.deleted if not row[1].startswith("prom-recovery-ip-")]
    assert not base.receipt()["modern_prom_recovery"]["host_ip_proven"]
    assert not base.receipt()["workloads_ready"]


@pytest.mark.parametrize("summary_missing", [False, True])
def test_creating_guest_metadata_is_only_observed_never_called_healthy(environment, monkeypatch, summary_missing):
    fake = environment[2]
    observations = []

    def pending():
        fake.finish_add()
        vm = fake.instances[NEW_VMSS][0]
        vm.update(vmId=None, provisioningState="Creating", latestModelApplied=None)
        vm["osProfile"]["computerName"] = None
        fake.views[(NEW_VMSS, "0")] = {"statuses": None, "extensions": None}
        fake.pools[-1]["provisioningState"] = "Creating"
        fake.vmsses[-1]["provisioningState"] = "Creating"
        fake.modern_scale_view = {
            "statuses": [{"code": "ProvisioningState/creating"}],
            "virtualMachines": [{"code": "ProvisioningState/creating", "count": 1}],
        }
        if summary_missing:
            fake.modern_scale_view = {"statuses": None, "virtualMachines": None}
        del fake.nodes[NEW_NODE]
        fake.nncs = [row for row in fake.nncs if row["metadata"]["name"] != NEW_NODE]
        fake.pods = [row for row in fake.pods if row["spec"].get("nodeName") != NEW_NODE]
        fake.operation.update(status="InProgress", endTime=None)

    def advance(_self, _deadline, description):
        assert "Modern pool" in description and not observations
        receipt = base.receipt()
        assert not receipt["modern_prom_recovery"].get("new_identity")
        assert not receipt["modern_prom_recovery"]["host_ip_proven"]
        assert receipt["arm_metadata"]["instances"][f"{NEW_VMSS}/0"]["healthy"] is False
        observations.append(description)
        fake.finish_add()

    fake.on_add = pending
    monkeypatch.setattr(recovery.Recovery, "wait", advance)
    summary = run(environment, execute=True)
    assert observations and summary["repaired"] and len(fake.adds) == 1


def test_already_ready_frameworks_are_read_only_adoptions_but_new_host_still_needs_ip_probe(environment):
    fake = environment[2]
    targets = {fake.plan["api_pod_uid"], *(row["pod_uid"] for row in fake.plan["framework_pods"])}
    for pod in fake.pods:
        if recovery.object_uid(pod) in targets:
            pod["status"] = base.ready_status()
    fake.members[95]["meshProperties"]["status"] = {"state": "Connected"}
    summary = run(environment, execute=True)
    assert summary["repaired"] and len(summary["ip_proofs"]) == 1
    assert len(fake.deleted) == 1 and fake.deleted[0][1].startswith("prom-recovery-ip-")
    assert {row["ready_pod_uid"] for row in summary["pod_moves"]} == targets
    assert all(not row["delete_attempted"] for row in summary["pod_moves"])
    assert summary["modern_derived_manifest"]["real_node_uids"][NEW_NODE] == NEW_UID
    assert summary["modern_derived_manifest"]["mock_pod_uids"] == fake.plan["mock_pod_uids"]


@pytest.mark.parametrize("outcome", ["connected", "timeout", "unrelated", "identity-drift"])
def test_recovering_partial_connectivity_is_bounded_read_only_not_a_health_waiver(environment, monkeypatch, outcome):
    fake = environment[2]
    observed = []

    def partial_after_api(_old, pod):
        if pod["metadata"]["name"].startswith("clustermesh-apiserver-"):
            fake.members[95]["meshProperties"]["status"] = {
                "state": "Disconnected", "error": {"code": "PartialConnectivity"},
            }

    def observe(_self, _deadline, description):
        assert "Read-only mesh-96 PartialConnectivity" in description
        summary = base.receipt()
        assert not summary["fleet_connected"]
        assert summary["modern_prom_recovery"]["partial_connectivity_observation"]["connected"] is False
        assert not fake.retirements
        assert len([row for row in fake.deleted if not row[1].startswith("prom-recovery-ip-")]) == 3
        observed.append(len(fake.writes))
        if outcome == "timeout":
            raise recovery.workers.ReconcileError("PartialConnectivity bounded deadline")
        fake.members[95]["meshProperties"]["status"] = {"state": "Connected"}
        if outcome == "unrelated":
            fake.members[94]["meshProperties"]["status"] = {
                "state": "Disconnected", "error": {"code": "PartialConnectivity"},
            }
        elif outcome == "identity-drift":
            fake.members[94]["meshProperties"]["ciliumProperties"]["name"] = "foreign-identity"

    fake.delete_callback = partial_after_api
    monkeypatch.setattr(recovery.Recovery, "wait", observe)
    if outcome == "connected":
        summary = run(environment, execute=True)
        assert summary["repaired"] and summary["fleet_connected"] and len(fake.retirements) == 1
    else:
        with pytest.raises(recovery.workers.ReconcileError):
            run(environment, execute=True)
        assert not fake.retirements and not base.receipt()["repaired"]
        assert len([row for row in fake.deleted if not row[1].startswith("prom-recovery-ip-")]) == 3
    assert len(observed) == 1


def test_empty_retirement_is_observed_once_without_draining_or_retry(environment, monkeypatch):
    fake = environment[2]
    observations = []

    def retiring():
        fake.operation.update(
            name=base.uid("retirement-in-progress"), operationType="DeleteAgentPool", status="InProgress",
            startTime=base.now(), endTime=None,
        )
        next(row for row in fake.pools if row["name"] == "prompool")["provisioningState"] = "Deleting"
        next(row for row in fake.vmsses if row["name"] == recovery.PROM_VMSS)["provisioningState"] = "Deleting"
        fake.scale_view = {"statuses": [{"code": "ProvisioningState/deleting"}], "virtualMachines": None}

    def advance(_self, _deadline, description):
        assert "Native empty prompool deletion" in description and not observations
        observations.append(description)
        assert len(fake.retirements) == 1 and not base.receipt()["modern_prom_recovery"]["old_empty_pool_retired"]
        fake.finish_retirement()

    fake.on_retire = retiring
    monkeypatch.setattr(recovery.Recovery, "wait", advance)
    summary = run(environment, execute=True)
    assert observations and summary["repaired"] and summary["modern_prom_recovery"]["old_empty_pool_retired"]


@pytest.mark.parametrize("fault", ["guard-create", "guard-conflict", "create-journal", "add", "accepted-journal",
                                  "retire", "retire-journal"])
def test_ambiguity_is_durable_and_never_retried_or_rolled_back(environment, fault):
    fake = environment[2]
    if fault == "guard-create":
        fake.create_error = True
    elif fault == "guard-conflict":
        fake.create_conflict = True
    elif fault == "create-journal":
        fake.patch_error = "create-attempted"
    elif fault == "add":
        fake.add_error = True
    elif fault == "accepted-journal":
        fake.patch_error = "create-accepted"
    elif fault == "retire":
        fake.retire_error = True
    else:
        fake.patch_error = "retire-attempted"
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    first = copy.deepcopy(base.receipt())
    assert first["status"] == "failed" and not first["repaired"] and len(fake.configmaps) == 1
    assert len(fake.adds) <= 1 and len(fake.retirements) <= 1 and not fake.new_scales
    if fault in ("guard-create", "guard-conflict"):
        assert not first["modern_prom_recovery"]["attempt_guard"]["retained_owned_non_workload_record"]
    if fault in ("add", "retire"):
        action = "create" if fault == "add" else "retire"
        assert first["modern_prom_recovery"][action]["ambiguous"]
        assert first["modern_prom_recovery"][action]["accepted"] is None
        assert json.loads(fake.configmaps[0]["data"]["record"])["state"] == f"{action}-ambiguous"
    if fake.adds:
        assert any(row["name"] == "promv5" for row in fake.pools)
    if "modern_prom" in first:
        assert first["modern_prom"]["pool_name"] == "promv5"
        assert first["modern_prom"]["legacy_empty_pool_retired"] is False
    writes = copy.deepcopy(fake.writes)
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    assert fake.writes == writes


@pytest.mark.parametrize("fault", ["quota", "source", "guard-token", "guard-uid", "contract"])
def test_last_moment_drift_after_guard_prevents_add(environment, fault):
    args, _, fake = environment
    changed = []

    def hook(command):
        if not fake.configmaps or changed:
            return
        if command[1:3] == ["vm", "list-usage"]:
            changed.append(fault)
            if fault == "quota":
                fake.usage[0].update(currentValue="1000", limit="1000")
            elif fault == "source":
                Path(args.resume_replacement).write_text("{}", encoding="utf-8")
            elif fault == "guard-token":
                fake.configmaps[0]["data"]["token"] = base.uid("foreign")
            elif fault == "guard-uid":
                fake.configmaps[0]["metadata"]["uid"] = base.uid("foreign")
            else:
                fake.configmaps[0]["data"]["contract"] = "{}"

    fake.hook = hook
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    assert changed and len(fake.configmaps) == 1 and not fake.adds and not fake.retirements


@pytest.mark.parametrize("fault", ["fleet", "peers", "dns-sibling", "pdb", "memory", "old-pool-count"])
def test_no_empty_retirement_without_strict_framework_proof(environment, monkeypatch, fault):
    fake = environment[2]
    base.abort_wait(monkeypatch)
    if fault == "fleet":
        fake.fleet_stuck = True
    elif fault == "peers":
        fake.peer_fault = "disconnected"
    else:
        def regress(_old, pod):
            if not pod["metadata"]["name"].startswith("grafana-"):
                return
            if fault == "dns-sibling":
                fake.get_pod(f"{recovery.DNS_REPLICA_SET}-healthy-2")["status"] = base.ready_status(False)
            elif fault == "pdb":
                fake.pdbs[0]["spec"]["minAvailable"] = 0
            elif fault == "memory":
                fake.memory = "26Gi"
            else:
                next(row for row in fake.pools if row["name"] == "prompool")["count"] = 1
        fake.delete_callback = regress
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    assert len(fake.adds) == 1 and not fake.retirements and not base.receipt()["repaired"]
    assert any(row["name"] == "promv5" for row in fake.pools)


@pytest.mark.parametrize("path,value", [
    (("original_identity", "vm_id"), NEW_VM_ID),
    (("replacement", "delete", "accepted"), None),
    (("replacement", "native_removal", "pool_count"), 1),
    (("replacement", "restore", "accepted"), True),
    (("replacement", "native_removal", "manual_marker_clearance"), True),
    (("controller_pins",), {}),
    (("pdb_pins",), {}),
    (("original_model_pins", "defaults"), {}),
    (("pod_moves",), [{"delete_attempted": True}]),
])
def test_original_native_receipt_contract_is_not_weakened(environment, path, value):
    receipt = capacity_tests.source(environment)
    parent = receipt
    for part in path[:-1]:
        parent = parent[part]
    parent[path[-1]] = value
    capacity_tests.write_source(environment, receipt)
    with pytest.raises(recovery.workers.ReconcileError):
        run(environment, execute=True)
    no_writes(environment[2])
    assert not environment[2].commands


def test_historical_receipts_parse_on_python310_without_authorizing_live_state(environment, monkeypatch):
    artifacts = Path(
        "/home/skosuri/.copilot/session-state/478bd706-9d9d-436f-a721-2f32d3afcb77/files/mesh96-scoped-recovery",
    )
    plan_path = artifacts / "input-plan.json"
    if not plan_path.is_file():
        pytest.skip("Optional historical mesh-96 receipts are not present")
    args = copy.copy(environment[0])
    args.execute = False
    args.plan_file = str(plan_path)
    args.replace_failed_host = str(artifacts / "launch-f0500e3/artifact/recovery.json")
    args.resume_replacement = str(artifacts / "replace-12247445e178/artifact/recovery.json")
    parsed_arguments = base.use_python310_datetime(monkeypatch)
    plan = recovery.load_plan(args.plan_file)
    summary = {"plan_sha256": recovery.digest(plan)}

    def no_live_reads(*_args, **_kwargs):
        raise AssertionError("Historical receipts are lineage, never live authorization")

    operator = modern.ModernPromRecovery(args, plan, summary, no_live_reads, no_live_reads)
    operator.load_receipts()
    assert len(summary["controller_pins"]) == 58 and len(summary["pdb_pins"]) == 4
    assert summary["plan_sha256"] == "1a4385e2db5a0a5b38d750a6a82fcb9b3d4c683e801bf12bd8111c75355e380a"
    assert not operator.modern["quota_ready"] and operator.desired is None and operator.initial is None
    assert parsed_arguments
