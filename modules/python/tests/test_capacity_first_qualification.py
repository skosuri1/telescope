"""Synthetic real-schema qualification tests; no private evidence or live services."""

# pylint: disable=protected-access,too-many-lines

from __future__ import annotations

import copy
import importlib.util
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import jmespath
import pytest

from .test_capacity_first_worker_recovery import CapacityCloud, NAMES
from .test_stalled_retained_worker_recovery import metadata, now, status, uid


MODULE_DIR = Path(__file__).resolve().parents[1] / "clusterloader2" / "clustermesh-scale"
SPEC = importlib.util.spec_from_file_location("capacity_first_qualification", MODULE_DIR / "capacity_first_qualification.py")
qualification = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = qualification
sys.path.insert(0, str(MODULE_DIR))
try:
    SPEC.loader.exec_module(qualification)
finally:
    sys.path.pop(0)
capacity = qualification.capacity
base = qualification.base
stalled = qualification.stalled


class QualificationCloud(CapacityCloud):
    """Already-created capacity plus only journal/probe writes in this phase."""

    def __init__(self, args):
        original = SimpleNamespace(**vars(args))
        original.source_state_directory = str(Path(args.capacity_directory) / "source-state")
        original.restart_checkpoint = str(Path(args.capacity_directory) / "accepted-restart.json")
        super().__init__(original)
        self.public_args = args
        for row in self.pods:
            if row["metadata"].get("namespace") == "mock-clustermesh":
                row["spec"]["containers"][0]["resources"] = {
                    "requests": {"cpu": "100m", "memory": "256Mi"}, "limits": {"memory": "1Gi"},
                }
        self.controllers[0]["spec"]["template"]["spec"]["containers"][0]["resources"] = {
            "requests": {"cpu": "100m", "memory": "256Mi"}, "limits": {"memory": "1Gi"},
        }
        self.deleted = []
        self.no_growth = False
        self.no_version_growth = False
        self.http_wrong = False
        self.create_error = False
        self.delete_error = False
        self.missing_uid_on_create = False
        self.large_rss = False
        self.stale_metrics = False
        self.after_probe = None
        self.lease_error = False
        self.config_denied = False
        self.stages_unavailable = False
        self.node_leases_denied = False
        self.missing_node_lease = False
        self.node_memory = {name: "2Gi" for name in NAMES}
        self.qual_journal_error = False
        self.operation_record = None
        self.creation_receipt = None
        for row in self.nncs:
            index = 0 if row["metadata"]["name"] == stalled.SOURCE else 1
            row["status"]["assignedIPCount"] = 80
            row["status"]["networkContainers"][0].update(
                version=11 if index == 0 else 10,
                ipAssignments=[{"ip": f"10.50.{index}.{number}"} for number in range(1, 81)],
            )

    def initialize_artifacts(self):
        root = Path(self.public_args.capacity_directory)
        root.mkdir()
        super().write_source()
        # Record the actual original 38+6/56 source first; the six then self-heal.
        for row in self.pods:
            if row["metadata"].get("namespace") == "mock-clustermesh" and row["spec"].get("nodeName") == stalled.SOURCE:
                row["status"] = status(True)
        old_kwok = next(row for row in self.pods if row["metadata"]["name"] == "kwok-controller-old")
        old_kwok["metadata"]["uid"] = uid("ready-kwok-replacement")
        old_kwok["metadata"]["name"] = "kwok-controller-ready"
        old_kwok["metadata"].pop("deletionTimestamp")
        old_kwok["spec"]["nodeName"] = stalled.SOURCE
        old_kwok["status"] = status(True)
        super().add_new_pool()
        self.operation_record = {
            "name": uid("created-agent-pool-operation"), "operationType": "PutAgentPool", "status": "Succeeded",
            "startTime": (datetime.now(timezone.utc) - timedelta(minutes=4)).isoformat(),
            "endTime": (datetime.now(timezone.utc) - timedelta(minutes=3)).isoformat(), "errorCode": None,
        }
        self.new_scale["tags"]["aks-managed-createOperationID"] = self.operation_record["name"]
        for name in NAMES:
            self.nodes[name]["status"]["allocatable"] = {"cpu": "7820m", "memory": "28Gi", "pods": "110"}
            self.nodes[name]["status"]["nodeInfo"].update(operatingSystem="linux", osImage="Ubuntu 24.04")
            self.nodes[name]["status"]["conditions"][0]["lastHeartbeatTime"] = now()
        for row in self.nncs:
            if row["metadata"]["name"] in NAMES:
                row["status"]["networkContainers"][0]["version"] = 0
        for view in self.new_views.values():
            view["vmAgent"] = {"statuses": [{"code": "ProvisioningState/succeeded", "displayStatus": "Ready",
                                           "message": "Guest Agent is running", "time": now()}]}
        source_hashes = stalled.file_hashes(root / "source-state")
        prior = {"create": {"attempted": True, "submission_started": False, "accepted": None, "ambiguous": True}}
        (root / "prior-capacity.json").write_text(json.dumps(prior), encoding="utf-8")
        prior_hash = capacity.checkpoint_hash(root / "prior-capacity.json")
        creation = {
            "attempted": True, "submission_started": True, "accepted": True, "ambiguous": False,
            "automatic_retry_allowed": False, "command": capacity.add_command("1.35.7"),
            "requested_at": (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),
            "accepted_at": (datetime.now(timezone.utc) - timedelta(minutes=4) + timedelta(seconds=1)).isoformat(),
        }
        self.creation_receipt = {
            "schema_version": 1, "phase": "capacity-first-registration-only", "pool_created": True,
            "execute": True, "plan_sha256": stalled.PLAN_SHA, "create": creation,
            "arm_diagnostics": {"new_instances": jmespath.search(base.VM_QUERY, self.new_instances)},
            "desired_pool": capacity.pool_settings("1.35.7"), "source_hashes": source_hashes,
            "source_state_sha256": capacity.digest(source_hashes),
            "restart_checkpoint_sha256": capacity.checkpoint_hash(root / "accepted-restart.json"),
            "continuation": {"source_build": 79959, "checkpoint_sha256": prior_hash},
            "journal": {"uid": capacity.RESERVED_JOURNAL_UID},
            "original_mock_pod_uids": {row["metadata"]["name"]: row["metadata"]["uid"]
                                      for row in self.pods if row["metadata"].get("namespace") == "mock-clustermesh"},
            "preserved_kwok_node_uids": {name: self.nodes[name]["metadata"]["uid"] for name in capacity.maintenance.EXPECTED_AGENT_NAMES},
        }
        (root / "recovery.json").write_text(json.dumps(self.creation_receipt), encoding="utf-8")
        self.journals[capacity.JOURNAL] = {
            "metadata": metadata(capacity.JOURNAL, "kube-system", capacity.RESERVED_JOURNAL_UID),
            "data": {"owner": capacity.OWNER, "token": "a" * 32, "create": json.dumps(creation),
                     "failure_correlation": capacity.CORRELATION,
                     "source_state_sha256": capacity.digest(source_hashes),
                     "restart_checkpoint_sha256": self.creation_receipt["restart_checkpoint_sha256"],
                     "desired_pool_sha256": capacity.digest(self.creation_receipt["desired_pool"]),
                     "prior_checkpoint_sha256": prior_hash, "prior_build_id": "79959",
                     "prior_unsubmitted_create": json.dumps(prior["create"])},
        }
        observation = Path(self.public_args.observation_directory)
        observation.mkdir()
        files = {f"current-{key}.json": value for key, value in self.snapshot().items()}
        files.update({
            "cniv5-observation.json": {"observation_only": True, "mutation_started": False,
                                       "creation_receipt_reference_build": 79971},
            "cniv5-operation.json": self.operation_record,
            "cniv5-capacity-journal.json": self.journals[capacity.JOURNAL],
            "cniv5-instances.json": jmespath.search(base.VM_QUERY, self.new_instances),
            "cniv5-vmss-model.json": {
                **self.new_scale, "osDisk": self.new_model["virtualMachineProfile"]["storageProfile"]["osDisk"],
                "imageReference": self.new_model["virtualMachineProfile"]["storageProfile"]["imageReference"],
            },
        })
        for name, value in files.items():
            (observation / name).write_text(json.dumps(value), encoding="utf-8")

    def azure(self, command):
        assert command[1:4] != ["aks", "nodepool", "add"] and command[1:3] != ["vmss", "restart"], "No Azure writes!"
        if command[1:4] == ["aks", "operation", "show-latest"] and "--nodepool-name" in command:
            return self.operation_record
        return super().azure(command)

    def assign_probe_ip(self, node_name):
        row = next(row for row in self.nncs if row["metadata"]["name"] == node_name)
        network = row["status"]["networkContainers"][0]
        used = {pod["status"].get("podIP") for pod in self.pods if pod["spec"].get("nodeName") == node_name}
        addresses = [entry["ip"] for entry in network["ipAssignments"]]
        free = [address for address in addresses if address not in used]
        if free:
            return free[0]
        extra = [f"10.100.{NAMES.index(node_name)}.{index}" for index in range(len(addresses) + 1, len(addresses) + 17)]
        if not self.no_growth:
            network["ipAssignments"].extend({"ip": address} for address in extra)
            row["status"]["assignedIPCount"] += 16
        if not self.no_version_growth:
            network["version"] += 1
        return extra[0]

    def kube(self, command):
        if "create" in command or "patch" in command:
            assert qualification.JOURNAL in command and capacity.JOURNAL not in command and stalled.JOURNAL not in command
            self.writes.append(command)
            if "create" in command:
                assert qualification.JOURNAL not in self.journals
                self.journals[qualification.JOURNAL] = {
                    "metadata": metadata(qualification.JOURNAL, "kube-system"),
                    "data": dict(item.removeprefix("--from-literal=").split("=", 1)
                                 for item in command if item.startswith("--from-literal=")),
                }
                if self.qual_journal_error:
                    raise capacity.workers.ReconcileError("ambiguous qualification journal")
            else:
                patch = json.loads(self.value(command, "-p"))
                current = self.journals[qualification.JOURNAL]
                assert patch[0]["value"] == current["metadata"]["uid"]
                assert patch[1]["value"] == current["metadata"]["resourceVersion"]
                assert patch[2] == {"op": "test", "path": "/data", "value": current["data"]}
                current["data"] = patch[3]["value"]
                current["metadata"]["resourceVersion"] = str(int(current["metadata"]["resourceVersion"]) + 1)
            return self.journals[qualification.JOURNAL]
        if "run" in command:
            self.writes.append(command)
            assert self.value(command, "-n") == "mock-clustermesh"
            name = self.value(command, "run")
            overrides = json.loads(next(item.split("=", 1)[1] for item in command if item.startswith("--overrides=")))
            key, token = next(item.split("=", 1)[1] for item in command if item.startswith("--labels=")).split("=", 1)
            pod = {"metadata": metadata(name, "mock-clustermesh"), "spec": overrides["spec"],
                   "status": status(True)}
            pod["metadata"]["labels"] = {key: token}
            pod["status"]["podIP"] = self.assign_probe_ip(pod["spec"]["nodeName"])
            self.pods.append(pod)
            if self.after_probe:
                self.after_probe(pod)
            if self.create_error:
                raise capacity.workers.ReconcileError("lost probe create response")
            if self.missing_uid_on_create:
                response = copy.deepcopy(pod)
                response["metadata"].pop("uid")
                return response
            return pod
        if "logs" in command:
            assert "--tail=200" in command and "--limit-bytes=32768" in command and "--timestamps=true" in command
            assert not any(item.startswith("--since") for item in command)
            return "leader election lease update error token=should-not-be-published\n"
        if self.value(command, "get") == "leases":
            if self.value(command, "-n") == "kube-node-lease":
                if self.node_leases_denied:
                    raise capacity.workers.ReconcileError("Forbidden kube-node-lease")
                return {"items": [{
                    "metadata": {**metadata(name, "kube-node-lease"),
                                 "ownerReferences": [{"kind": "Node", "name": name,
                                                      "uid": self.nodes[name]["metadata"]["uid"]}]},
                    "spec": {"holderIdentity": name, "leaseDurationSeconds": 40, "renewTime": now()},
                } for name in sorted(capacity.maintenance.EXPECTED_AGENT_NAMES)
                    if not self.missing_node_lease or name != "kwok-node-0"]}
            if self.lease_error:
                raise capacity.workers.ReconcileError("Forbidden leases")
            return {"items": [{"metadata": metadata("kwok-controller", "kube-system"),
                               "spec": {"holderIdentity": "kwok-controller-ready", "renewTime": now()}}]}
        if self.value(command, "get") == "configmap" and "kwok" in command:
            if self.config_denied:
                raise capacity.workers.ReconcileError("Forbidden ConfigMap kwok")
            return {
                "metadata": metadata("kwok", "kube-system"),
                "data": {"kwok.yaml": (
                    "apiVersion: config.kwok.x-k8s.io/v1alpha1\nkind: KwokConfiguration\noptions:\n"
                    "  manageNodesWithAnnotationSelector: kwok.x-k8s.io/node=fake\n"
                    "  enableCRDs: [Stage, Metric]\n  kubeconfig: credential-content-not-public\n"
                )},
                "binaryData": {"credential": "not-public"},
            }
        if self.value(command, "get") == "stages.kwok.x-k8s.io":
            if self.stages_unavailable:
                raise capacity.workers.ReconcileError('the server doesn\'t have a resource type "stages.kwok.x-k8s.io"')
            return {"items": [{
                "apiVersion": "kwok.x-k8s.io/v1alpha1", "kind": "Stage", "metadata": metadata("node-heartbeat"),
                "spec": {"resourceRef": {"apiVersion": "v1", "kind": "Node"},
                         "selector": {"matchAnnotations": {"kwok.x-k8s.io/node": "fake"}},
                         "next": {"statusTemplate": '{"conditions":[{"type":"Ready","status":"True"}]}'}},
            }]}
        if self.value(command, "get") == "configmaps":
            return {"items": [self.journals[qualification.JOURNAL]] if qualification.JOURNAL in self.journals else []}
        if self.value(command, "get") == "--raw":
            path = self.value(command, "--raw")
            if path.endswith("/hostname"):
                name = path.split("/pods/")[1].split(":")[0]
                return "wrong-host" if self.http_wrong else name
            stamp = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat() if self.stale_metrics else now()
            if path.endswith("/nodes"):
                return {"items": [{"metadata": {"name": name}, "timestamp": stamp,
                                   "usage": {"cpu": "400m", "memory": self.node_memory[name]}} for name in NAMES]}
            assert path.endswith("/pods")
            return {"items": [{
                "metadata": {"name": row["metadata"]["name"], "namespace": "mock-clustermesh"},
                "timestamp": stamp, "containers": [{"name": "mock", "usage": {"memory": "2Gi" if self.large_rss else "300Mi"}}],
            } for row in self.pods if row["metadata"].get("namespace") == "mock-clustermesh"
                and row["spec"].get("nodeName") == stalled.SOURCE]}
        return super().kube(command)

    def delete_probe(self, _cluster, *, namespace, name, uid: str, timeout_seconds, attempts, retry_seconds):  # pylint: disable=redefined-outer-name
        assert namespace == "mock-clustermesh" and name.startswith("cni-maint-probe-")
        assert attempts == 1 and retry_seconds == 0 and 0 < timeout_seconds <= 45
        assert not any(entry[0] == name for entry in self.deleted)
        pod = next(row for row in self.pods if row["metadata"]["name"] == name)
        assert pod["metadata"]["uid"] == uid
        self.deleted.append((name, uid))
        if self.delete_error:
            raise qualification.mocks.RecoveryError("uncertain UID cleanup")
        self.pods.remove(pod)


@pytest.fixture(name="environment")
def setup_environment(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    args = SimpleNamespace(
        resource_group=base.RESOURCE_GROUP, confirm_resource_group=base.RESOURCE_GROUP,
        expected_subscription=base.SUBSCRIPTION, expected_region=base.REGION, expected_tfvars_sha="a" * 64,
        observation_directory="observation", observation_build_id=79975, capacity_directory="capacity",
        capacity_build_id=79971, kubeconfig="private", context=base.CLUSTER,
        timeout_seconds=2400, summary_file="qualification.json", execute=False,
    )
    cloud = QualificationCloud(args)
    cloud.initialize_artifacts()
    return args, cloud


def run(environment, *, execute=False):
    args, cloud = environment
    args.execute = execute
    summary = {}
    qualification.execute_qualification(args, summary, runner=cloud.run, delete_pod=cloud.delete_probe)
    assert summary == cloud.receipt()
    return summary


def test_readonly_plan_accepts_explicit_version_zero_without_claiming_networking(environment):
    _, cloud = environment
    summary = run(environment)
    assert summary["plan_valid"] and not cloud.writes and not cloud.deleted
    assert summary["memory_projection"]["remaining_count"] == 56
    assert len(summary["memory_projection"]["placements"]) == 56
    assert all(row["before"]["version"] == 0 and row["probe_count"] == 17 for row in summary["ip_growth"].values())
    assert not summary["capacity_qualified"] and not summary["mutation_started"]
    assert not summary["actual_ip_growth_proven"] and not summary["workloads_ready"]
    assert "should-not-be-published" not in json.dumps(summary)
    assert "credential-content-not-public" not in json.dumps(summary)
    diagnostics = summary["kwok_diagnostics"]
    assert diagnostics["ready"] is True and diagnostics["bootstrap_health_established"] is False
    assert diagnostics["configmap"]["configuration_status"] == "parsed"
    assert diagnostics["configmap"]["documents"][0]["options"]["kubeconfig"] == "<redacted>"
    assert diagnostics["stages"]["items"][0]["metadata"]["name"] == "node-heartbeat"
    assert "token=<redacted>" in diagnostics["redacted_log_excerpt"][0]
    assert len(summary["all_nnc_allocations"]) == 4
    assert len(diagnostics["node_leases"]["items"]) == 100
    assert diagnostics["node_leases"]["missing_names"] == []
    assert len(diagnostics["kwok_node_heartbeats"]) == 100
    assert diagnostics["unknown_readiness_explained"] is False


def test_real_growth_http_all56_headroom_and_uid_cleanup_preserve_all_production(environment):
    _, cloud = environment
    old_nodes, old_pods = copy.deepcopy(cloud.nodes), copy.deepcopy(cloud.pods)
    old_capacity = copy.deepcopy(cloud.journals[capacity.JOURNAL])
    old_restart = copy.deepcopy(cloud.journals[stalled.JOURNAL])
    summary = run(environment, execute=True)
    assert summary["capacity_qualified"] and summary["actual_ip_growth_proven"] and summary["actual_memory_headroom_proven"]
    assert not summary["workloads_ready"] and not summary["bootstrap_complete"]
    assert cloud.nodes == old_nodes and cloud.pods == old_pods
    assert cloud.journals[capacity.JOURNAL] == old_capacity and cloud.journals[stalled.JOURNAL] == old_restart
    assert len(cloud.deleted) == 34 and not summary["probe_cleanup_pending"]
    assert not any(command[0] == "az" for command in cloud.writes)
    assert summary["memory_projection"]["remaining_count"] == 56
    assert sum(summary["memory_projection"]["placement_counts"].values()) == 56
    assert summary["memory_projection"]["memory_per_agent_bytes"] >= 300 * 1024**2
    assert all(proof["after"]["version"] > 0 and proof["after"]["assigned_ip_count"] == 32 and proof["http_proven"]
               for proof in summary["ip_growth"].values())


def test_projected_56_placement_uses_actual_unequal_memory_not_arbitrary_half(environment):
    _, cloud = environment
    cloud.node_memory[NAMES[0]] = "17Gi"
    summary = run(environment)
    counts = summary["memory_projection"]["placement_counts"]
    assert sum(counts.values()) == 56 and counts[NAMES[0]] < counts[NAMES[1]]


@pytest.mark.parametrize("fault", ["no_growth", "no_version_growth", "http_wrong", "large_rss", "stale_metrics", "lease_error"])
def test_missing_real_proof_or_denied_reads_cannot_qualify(environment, monkeypatch, fault):
    _, cloud = environment
    setattr(cloud, fault, True)
    clock = [qualification.time.monotonic()]
    monkeypatch.setattr(qualification.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(qualification.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + max(seconds, 610)))
    with pytest.raises(qualification.EXPECTED_ERRORS):
        run(environment, execute=True)
    assert not cloud.receipt()["capacity_qualified"] and not cloud.receipt()["workloads_ready"]
    assert not any(command[0] == "az" for command in cloud.writes)


@pytest.mark.parametrize("fault", ["lost-create", "missing-create-uid", "cleanup", "journal"])
def test_ambiguous_probe_actions_are_never_replayed_or_success_shaped(environment, monkeypatch, fault):
    _, cloud = environment
    if fault == "lost-create":
        cloud.create_error = True
    elif fault == "missing-create-uid":
        cloud.missing_uid_on_create = True
    elif fault == "cleanup":
        cloud.delete_error = True
        clock = [qualification.time.monotonic()]
        monkeypatch.setattr(qualification.time, "monotonic", lambda: clock[0])
        monkeypatch.setattr(qualification.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + max(seconds, 310)))
    else:
        cloud.qual_journal_error = True
    with pytest.raises(qualification.EXPECTED_ERRORS):
        run(environment, execute=True)
    receipt = cloud.receipt()
    assert not receipt["capacity_qualified"]
    assert len(cloud.deleted) == len(set(cloud.deleted))
    if fault == "lost-create":
        assert len(cloud.deleted) == 1
        record = next(iter(receipt["probe_receipts"].values()))
        assert record["create_ambiguous"] and record["uid_resolved_after_ambiguous_create"]
        assert not receipt["probe_cleanup_pending"]
    if fault == "cleanup":
        assert receipt["probe_cleanup_pending"] and receipt["cleanup_errors"]


@pytest.mark.parametrize("fault", ["vm-id", "node-uid", "nc-id", "version-missing", "old-uid", "lease", "quota", "controller", "pdb",
                                  "old-journal", "existing-qualification", "existing-probe"])
def test_fresh_scope_and_identity_drift_fail_before_writes(environment, fault):
    _, cloud = environment
    if fault == "vm-id":
        cloud.new_instances[0]["vmId"] = uid("foreign")
    elif fault == "node-uid":
        cloud.nodes[NAMES[0]]["metadata"]["uid"] = uid("foreign")
    elif fault in ("nc-id", "version-missing"):
        network = next(row for row in cloud.nncs if row["metadata"]["name"] == NAMES[0])["status"]["networkContainers"][0]
        if fault == "nc-id":
            network["id"] = uid("foreign-nc")
        else:
            network.pop("version")
    elif fault == "old-uid":
        next(row for row in cloud.pods if row["metadata"]["name"] == "kwok-node-40")["metadata"]["uid"] = uid("changed-healed-agent")
    elif fault == "lease":
        cloud.node_group["tags"]["deletion_due_time"] = now()
    elif fault == "quota":
        cloud.usage[0]["limit"] = "1"
    elif fault == "controller":
        cloud.controllers[0]["spec"]["replicas"] = 99
    elif fault == "pdb":
        cloud.pdbs[0]["spec"]["minAvailable"] = 0
    elif fault == "old-journal":
        cloud.journals[capacity.JOURNAL]["data"]["create"] = "{}"
    elif fault == "existing-qualification":
        cloud.journals[qualification.JOURNAL] = {"metadata": metadata(qualification.JOURNAL, "kube-system"), "data": {}}
    else:
        cloud.pods[-1]["metadata"]["labels"][qualification.maintenance.PROBE_LABEL_KEY] = "foreign"
    with pytest.raises(qualification.EXPECTED_ERRORS):
        run(environment, execute=True)
    assert not cloud.writes and not cloud.deleted


def test_protected_44_healed_agent_regression_during_probe_batch_stops_qualification(environment):
    _, cloud = environment
    def regress(_pod):
        next(row for row in cloud.pods if row["metadata"]["name"] == "kwok-node-40")["status"] = status(False)
    cloud.after_probe = regress
    with pytest.raises(qualification.EXPECTED_ERRORS):
        run(environment, execute=True)
    assert not cloud.receipt()["capacity_qualified"] and not cloud.receipt()["probe_cleanup_pending"]


def test_read_only_phase_rejects_output_alias_and_wrong_build(environment):
    args, cloud = environment
    args.summary_file = str(Path(args.capacity_directory) / "not-created-yet.json")
    with pytest.raises(qualification.EXPECTED_ERRORS):
        run(environment)
    assert not cloud.commands


def test_python310_and_exact_cli():
    import ast  # pylint: disable=import-outside-toplevel
    ast.parse((MODULE_DIR / "capacity_first_qualification.py").read_text(encoding="utf-8"), feature_version=(3, 10))
    args = qualification.parse_args([
        "--resource-group", base.RESOURCE_GROUP, "--confirm-resource-group", base.RESOURCE_GROUP,
        "--expected-subscription", base.SUBSCRIPTION, "--expected-region", base.REGION, "--expected-tfvars-sha", "a" * 64,
        "--observation-directory", "observation", "--observation-build-id", "79975",
        "--capacity-directory", "capacity", "--capacity-build-id", "79971",
        "--kubeconfig", "private", "--summary-file", "out.json", "--timeout-seconds", "2400",
    ])
    assert not args.execute and args.timeout_seconds == 2400 and args.context == base.CLUSTER


@pytest.mark.parametrize("fault", ["config-denied", "stage-unavailable"])
def test_optional_kwok_diagnostic_access_is_reported_without_bootstrap_health(environment, fault):
    _, cloud = environment
    cloud.config_denied = fault == "config-denied"
    cloud.stages_unavailable = fault == "stage-unavailable"
    summary = run(environment)
    key = "configmap" if fault == "config-denied" else "stages"
    expected = "denied" if fault == "config-denied" else "unavailable"
    assert summary["kwok_diagnostics"][key]["read_status"] == expected
    assert not summary["kwok_diagnostics"][key]["bootstrap_health_established"]
    assert not summary["bootstrap_complete"] and not summary["capacity_qualified"] and not cloud.writes


@pytest.mark.parametrize("pair", [(stalled.SOURCE, stalled.TARGET), (stalled.SOURCE, NAMES[0]),
                                 (stalled.TARGET, NAMES[1]), (NAMES[0], NAMES[1])])
def test_all_four_nnc_allocations_must_be_conflict_free(environment, pair):
    _, cloud = environment
    mapping = {row["metadata"]["name"]: row for row in cloud.nncs}
    mapping[pair[1]]["status"]["networkContainers"][0]["ipAssignments"][0] = copy.deepcopy(
        mapping[pair[0]]["status"]["networkContainers"][0]["ipAssignments"][0])
    with pytest.raises(qualification.EXPECTED_ERRORS, match="allocations conflict"):
        run(environment, execute=True)
    assert not cloud.writes and not cloud.deleted


def test_cross_worker_allocation_conflict_during_growth_prevents_qualification(environment):
    _, cloud = environment
    def conflict(_pod):
        old = next(row for row in cloud.nncs if row["metadata"]["name"] == stalled.SOURCE)
        new = next(row for row in cloud.nncs if row["metadata"]["name"] == NAMES[0])
        old["status"]["networkContainers"][0]["ipAssignments"][0] = copy.deepcopy(
            new["status"]["networkContainers"][0]["ipAssignments"][0])
    cloud.after_probe = conflict
    with pytest.raises(qualification.EXPECTED_ERRORS, match="allocations conflict"):
        run(environment, execute=True)
    assert not cloud.receipt()["capacity_qualified"] and not cloud.receipt()["probe_cleanup_pending"]


def test_kwok_diagnostic_redaction_preserves_configuration_not_credentials():
    value = {
        "kind": "KwokConfiguration", "options": {"enableCRDs": ["Stage"], "client-key-data": "private-key"},
        "logs": ['{"token":"sensitive"} Authorization: Bearer opaque-secret',
                 "https://user:password@server/path", "-----BEGIN PRIVATE KEY-----\nprivate-material\n"],
        "env": [{"name": "SECRET_VALUE", "value": "sensitive"}],
    }
    rendered = json.dumps(qualification.redact_diagnostic(value))
    assert all(secret not in rendered for secret in ("sensitive", "opaque-secret", "user:password", "private-material"))
    assert "enableCRDs" in rendered and "Stage" in rendered and "redacted" in rendered


@pytest.mark.parametrize("fault", ["missing", "denied"])
def test_kwok_node_lease_diagnostics_never_explain_unknown_as_healthy(environment, fault):
    _, cloud = environment
    cloud.nodes["kwok-node-0"]["metadata"]["annotations"] = {"kwok.x-k8s.io/node": "fake"}
    cloud.nodes["kwok-node-0"]["status"]["phase"] = "Running"
    cloud.nodes["kwok-node-0"]["status"]["conditions"][0]["lastHeartbeatTime"] = (
        datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    cloud.missing_node_lease = fault == "missing"
    cloud.node_leases_denied = fault == "denied"
    summary = run(environment)
    diagnostic = summary["kwok_diagnostics"]["node_leases"]
    assert diagnostic["expected_node_count"] == 100
    assert diagnostic["bootstrap_health_established"] is False
    if fault == "missing":
        assert diagnostic["missing_names"] == ["kwok-node-0"] and len(diagnostic["items"]) == 99
    else:
        assert diagnostic["read_status"] == "denied" and "items" not in diagnostic
    assert not summary["bootstrap_complete"] and not summary["kwok_diagnostics"]["unknown_readiness_explained"]
    node = next(row for row in summary["kwok_diagnostics"]["kwok_node_heartbeats"] if row["name"] == "kwok-node-0")
    assert node["phase"] == "Running" and node["kwok_annotation"] == "fake"
    assert node["ready_conditions"][0]["status"] == "Unknown"
    assert not cloud.writes


def test_observed_vm_must_match_the_accepted_creation_identity(environment):
    args, cloud = environment
    path = Path(args.capacity_directory) / "recovery.json"
    receipt = json.loads(path.read_text(encoding="utf-8"))
    receipt["arm_diagnostics"]["new_instances"][0]["vmId"] = uid("different-created-vm")
    path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(qualification.EXPECTED_ERRORS, match="single accepted creation"):
        run(environment, execute=True)
    assert not cloud.commands and not cloud.writes


@pytest.mark.parametrize("name", NAMES)
def test_new_worker_heartbeat_must_still_be_fresh(environment, name):
    _, cloud = environment
    cloud.nodes[name]["status"]["conditions"][0]["lastHeartbeatTime"] = (
        datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    with pytest.raises(qualification.EXPECTED_ERRORS):
        run(environment, execute=True)
    assert not cloud.writes


@pytest.mark.parametrize("field", ["owner", "input_sha256", "probe_receipts", "state"])
def test_full_journal_data_changes_cannot_be_overwritten(environment, field):
    _, cloud = environment
    changed = []

    def tamper(command):
        journal = cloud.journals.get(qualification.JOURNAL)
        if journal and "get" in command and qualification.JOURNAL in command and not changed:
            changed.append(True)
            journal["data"][field] = "changed"

    cloud.hook = tamper
    with pytest.raises(qualification.EXPECTED_ERRORS, match="journal UID/data"):
        run(environment, execute=True)
    assert changed and cloud.journals[qualification.JOURNAL]["data"][field] == "changed"
    assert not cloud.deleted and not any("run" in command for command in cloud.writes)


def test_fresh_allocation_growth_before_plan_is_not_a_frozen_version_failure(environment):
    _, cloud = environment
    raw = next(row for row in cloud.nncs if row["metadata"]["name"] == NAMES[0])
    container = raw["status"]["networkContainers"][0]
    container["ipAssignments"].extend({"ip": f"10.100.0.{index}"} for index in range(17, 33))
    container["version"] = 1
    raw["status"]["assignedIPCount"] = 32
    summary = run(environment)
    assert summary["ip_growth"][NAMES[0]]["before"]["version"] == 1
    assert summary["ip_growth"][NAMES[0]]["probe_count"] == 33
    assert not summary["capacity_qualified"] and not cloud.writes


def test_changed_probe_owner_is_not_adopted_or_deleted(environment):
    _, cloud = environment

    def assign_owner(pod):
        pod["metadata"]["ownerReferences"] = [{"kind": "Job", "name": "foreign", "uid": uid("foreign-owner")}]

    cloud.after_probe = assign_owner
    with pytest.raises(qualification.EXPECTED_ERRORS, match="Probe ownership"):
        run(environment, execute=True)
    assert not cloud.deleted and cloud.receipt()["probe_cleanup_pending"]


def test_accepted_cleanup_is_observed_after_read_error_without_duplicate_deletes(environment):
    _, cloud = environment
    original_delete, original_kube = cloud.delete_probe, cloud.kube
    state = {"raised": False, "reads": 0, "pending": None}

    def delay_last(cluster, **kwargs):
        row = copy.deepcopy(next(pod for pod in cloud.pods if pod["metadata"]["name"] == kwargs["name"]))
        original_delete(cluster, **kwargs)
        if len(cloud.deleted) == 34:
            row["metadata"]["deletionTimestamp"] = now()
            cloud.pods.append(row)
            state["pending"] = row

    def recover_read(command):
        if "get" in command and cloud.value(command, "get") == "pods" and len(cloud.deleted) == 34:
            if not state["raised"]:
                state["raised"] = True
                raise qualification.workers.ReconcileError("temporary cleanup read failure")
            state["reads"] += 1
            if state["reads"] == 2:
                cloud.pods.remove(state["pending"])
        return original_kube(command)

    cloud.delete_probe, cloud.kube = delay_last, recover_read
    with pytest.raises(qualification.EXPECTED_ERRORS, match="temporary cleanup read failure"):
        run(environment, execute=True)
    assert len(cloud.deleted) == 34 and state["reads"] == 2
    assert not cloud.receipt()["probe_cleanup_pending"] and not cloud.receipt()["cleanup_errors"]
    assert not cloud.receipt()["capacity_qualified"]
