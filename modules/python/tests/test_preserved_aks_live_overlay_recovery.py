"""Local-only coverage of early preserved recovery, including the real full-fleet probe."""

import copy
import importlib.util
import json
import sys
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import pytest


SCRIPT_DIR = Path(__file__).resolve().parents[1] / "clusterloader2" / "clustermesh-scale"


def load_script(name):
    spec = importlib.util.spec_from_file_location(f"{name}_early_recovery_tests", SCRIPT_DIR / f"{name}.py")
    if spec is None or spec.loader is None:
        raise ImportError(name)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


arm = load_script("preserved_aks_arm_reconcile")
overlay = load_script("preserved_live_overlay")


@pytest.fixture(name="fleet")
def fleet_fixture(tmp_path, monkeypatch):
    run_id = "12345-deadbeef"
    prefix = f"/subscriptions/s/resourceGroups/{run_id}"
    fleet_id = f"{prefix}/providers/Microsoft.ContainerService/fleets/clustermesh-flt"
    rows, members, groups = [], [], []
    for number in range(1, 101):
        role, name = f"mesh-{number}", f"clustermesh-{number}"
        cluster_id = f"{prefix}/providers/Microsoft.ContainerService/managedClusters/{name}"
        node_group = f"MC_{run_id}_{name}_eastus2euap"
        pool_name = "prompool" if number == 60 else "default"
        rows.append({
            "id": cluster_id, "name": name, "resourceGroup": run_id,
            "location": "eastus2euap", "nodeResourceGroup": node_group,
            "networkProfile": {"networkDataplane": "cilium", "networkPolicy": "cilium"},
            "tags": {"role": role, "run_id": run_id}, "provisioningState": "Succeeded",
            "powerState": {"code": "Running"},
            "agentPoolProfiles": [{
                "id": f"{cluster_id}/agentPools/{pool_name}", "name": pool_name,
                "provisioningState": "Failed" if number in (60, 89) else "Succeeded",
                "powerState": {"code": "Running"}, "count": 3, "enableAutoScaling": False,
                "vmSize": "Standard_D8_v3", "upgradeSettings": {"maxSurge": "10%"},
            }],
        })
        members.append({
            "name": role, "clusterResourceId": cluster_id, "provisioningState": "Succeeded",
            "labels": {"mesh": "true"}, "meshProperties": {
                "status": {"state": "Connected"},
                "ciliumProperties": {"id": number, "name": f"mesh-{number}{number}"},
            },
        })
        groups.append({
            "name": node_group, "managedBy": cluster_id, "location": "eastus2euap",
            "deletion_due_time": "2099-01-01T00:00:00Z",
        })
    state = {
        "rows": rows, "members": members, "applied": copy.deepcopy(members), "groups": groups,
        "group": {
            "id": prefix, "name": run_id, "location": "eastus2euap",
            "tags": {
                "run_id": run_id, "scenario": "perf-eval-clustermesh-scale",
                "clustermesh_debug_preserved": "true", "clustermesh_debug_expected_clusters": "100",
                "clustermesh_debug_tfvars_sha256": "expected-sha", "deletion_due_time": "2099-01-01T00:00:00Z",
            },
        },
        "fleet": {"id": fleet_id, "provisioningState": "Succeeded"},
        "profile": {"id": f"{fleet_id}/clusterMeshProfiles/clustermesh-cmp",
                    "properties": {"provisioningState": "Succeeded"}},
        "commands": [], "children": [], "events": [], "pool_updates": [], "credential_roles": set(),
        "drift": False, "repair_rc": 0, "probe_phase": "initial", "pool_reads": {},
    }
    args = arm.parse_args([
        "--resource-group", run_id, "--expected-subscription", "s",
        "--expected-region", "eastus2euap", "--expected-count", "100",
        "--expected-tfvars-sha", "expected-sha", "--summary-file", str(tmp_path / "summary.json"),
        "--failed-pool-repair-enabled", "--live-overlay-repair-enabled",
        "--quiescence-timeout-seconds", "10", "--poll-seconds", "4",
    ])
    state["args"] = args
    clock = [0.0]
    state["clock"] = clock
    monkeypatch.setattr(arm.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(arm.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    monkeypatch.setattr(arm, "parse_args", lambda _argv: args)

    def option(command, name):
        return command[command.index(name) + 1]

    def has_pool_fault(role, fault):
        requested = state.get("pool_fault")
        return role == "mesh-60" and (
            requested == fault
            or (requested == f"post-{fault}" and role in state["pool_updates"])
        )

    def nodes(role):
        number = int(role[5:])
        row = rows[number - 1]
        return [{
            "metadata": {"name": f"{role}-node-{index}", "labels": {
                "kubernetes.azure.com/agentpool": row["agentPoolProfiles"][0]["name"],
            }},
            "spec": {"providerID": (
                f"azure:///subscriptions/s/resourceGroups/{row['nodeResourceGroup']}/providers/"
                f"Microsoft.Compute/virtualMachineScaleSets/{role}-vmss/virtualMachines/{index}"
            )},
            "status": {"conditions": [{"type": "Ready", "status": (
                "False" if has_pool_fault(role, "workers") else "True"
            )}]},
        } for index in range(3)]

    def runner(command, _timeout):
        state["commands"].append(command)
        if command[:3] == ["az", "account", "show"]:
            return state.get("subscription", "s")
        if command[:3] == ["az", "group", "show"]:
            return json.dumps(state["group"])
        if command[:3] == ["az", "group", "exists"]:
            return "true"
        if command[:4] == ["az", "aks", "operation", "show-latest"]:
            return json.dumps(state.get("latest_operation", {
                "name": "failed-addon-operation", "status": "Failed",
                "operationType": "PutExtensionAddon", "error": {"code": "OverlaymgrReconcileError"},
            }))
        if command[:3] == ["az", "group", "list"]:
            return json.dumps(state["groups"])
        if command[:3] == ["az", "aks", "list"]:
            return json.dumps(state["rows"])
        if command[:3] == ["az", "fleet", "show"]:
            return json.dumps(state["fleet"])
        if command[:4] == ["az", "fleet", "clustermeshprofile", "show"]:
            return json.dumps(state["profile"])
        if command[:4] == ["az", "fleet", "clustermeshprofile", "list-members"]:
            return json.dumps(state["applied"])
        if command[:4] == ["az", "fleet", "member", "list"]:
            return json.dumps(state["members"])
        if command[:3] == ["az", "aks", "get-credentials"]:
            role = option(command, "--name").replace("clustermesh-", "mesh-")
            if state.get("credential_failure") == role:
                raise arm.ReconcileError(f"{role}: AuthorizationFailed")
            Path(option(command, "--file")).write_text("fake kubeconfig\n", encoding="utf-8")
            state["credential_roles"].add(role)
            return ""
        if command[:3] == ["az", "aks", "nodepool"]:
            role = option(command, "--cluster-name").replace("clustermesh-", "mesh-")
            pool = rows[int(role[5:]) - 1]["agentPoolProfiles"][0]
            if command[3] == "show":
                state["pool_reads"][role] = state["pool_reads"].get(role, 0) + 1
                result = copy.deepcopy(pool)
                if role == "mesh-60" and state.get("pool_fault") == "configuration" and state["pool_reads"][role] == 2:
                    result["count"] += 1
                return json.dumps(result)
            assert command[3] == "update"
            assert not any(flag in command for flag in ("--node-count", "--node-image-only", "--force", "--max-surge"))
            state["events"].append(f"pool-update:{role}")
            state["pool_updates"].append(role)
            if state.get("pool_fault") == "mutation":
                raise arm.ReconcileError("PDB blocked resumed node-image upgrade")
            pool["provisioningState"] = "Succeeded"
            return ""
        if command[:3] == ["az", "vmss", "show"]:
            failed = has_pool_fault("mesh-60", "vmss") and option(command, "--name") == "mesh-60-vmss"
            return json.dumps({"provisioningState": "Failed" if failed else "Succeeded", "sku": {"capacity": 3}})
        if command[0] == "kubectl":
            if "--raw=/readyz" in command:
                return "ok"
            if "deployment" in command:
                return '{"status":{"conditions":[{"type":"Available","status":"True"}]}}'
            assert "nodes" in command
            return json.dumps({"items": nodes(Path(option(command, "--kubeconfig")).stem)})
        if command[0] == sys.executable and command[1].endswith("cilium_agent_health.py"):
            role = option(command, "--role")
            healthy = not (
                (state["drift"] and not state.get("converged"))
                or has_pool_fault(role, "cilium")
            )
            health = {
                "healthy": healthy,
                "agents": [{
                    "pod_name": f"{role}-cilium-{index}",
                    "node_name": f"{role}-node-{index}", "healthy": healthy,
                    "remote_count": 99, "ready_remote_count": 99 if healthy else 98,
                    "not_ready_remote_names": [] if healthy else ["mesh-5353"],
                    "missing_remote_names": [], "unexpected_remote_names": [], "duplicate_remote_names": [],
                } for index in range(3)],
            }
            if has_pool_fault(role, "coverage"):
                health["agents"].pop()
            Path(option(command, "--summary-file")).write_text(json.dumps(health), encoding="utf-8")
            if not healthy:
                raise arm.ReconcileError("Cilium peer proof failed")
            return ""
        raise AssertionError(f"Unexpected command (no real cloud access is allowed): {command}")

    def kube_runner(command, _timeout):
        role = Path(option(command, "--kubeconfig")).stem
        phase = state["probe_phase"]
        if "configmap" in command:
            if state.get("identity_failure") == role:
                raise overlay.ProbeError(f"{role}: Forbidden cilium-config")
            identity = next(member for member in members if member["name"] == role)["meshProperties"]["ciliumProperties"]
            cluster_id = 200 if state.get("wrong_live_identity") == role else identity["id"]
            return json.dumps({"data": {"cluster-name": identity["name"], "cluster-id": str(cluster_id)}})
        if state.get("agent_failure") == (phase, role):
            raise overlay.ProbeError(f"{role}: Forbidden kubectl exec")
        if "pods" in command:
            return json.dumps({"items": [{
                "metadata": {"name": f"{role}-cilium-{index}"}, "spec": {"nodeName": f"{role}-node-{index}"},
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [{"name": "cilium-agent", "ready": True}],
                },
            } for index in range(3)]})
        assert "cilium-dbg" in command
        remotes = [{
            "name": member["meshProperties"]["ciliumProperties"]["name"],
            "ready": True, "connected": True, "config": {"required": True, "retrieved": True},
        } for member in members if member["name"] != role]
        stale = state["drift"] if phase == "initial" else state.get("post_drift", False)
        if stale and role == "mesh-60":
            remote = next(item for item in remotes if item["name"] == "mesh-5353")
            remote.update(ready=False, config={"required": True, "retrieved": False})
        return json.dumps({"cluster-mesh": {"clusters": remotes}})

    original_probe = overlay.probe

    def full_probe(clusters, **kwargs):
        assert len(clusters) == 100
        assert kwargs["attempts"] in (5, 40)
        # Exercise the real identity/all-agent/vertex-cover logic once per phase;
        # retry timing is not the integration under test.
        kwargs.update(attempts=1, retry_seconds=0)
        return original_probe(clusters, runner=kube_runner, **kwargs)

    def child(command, timeout, log_path, environment=None):
        state["children"].append((command, timeout, environment))
        if command[0] == "bash":
            state["events"].append("fleet-repair")
            assert command[1].endswith("/reuse/repair-existing-fleet-overlay.sh")
            assert environment["CLUSTERMESH_DEBUG_EXPECTED_CLUSTER_COUNT"] == "100"
            assert environment["CLUSTERMESH_DEBUG_MAX_REPAIR_MEMBERS"] == "20"
            assert environment["CMP_MEMBER_LABEL_KEY"] == "mesh"
            assert environment["CMP_MEMBER_LABEL_VALUE"] == "true"
            assert environment["CMP_MEMBER_REPAIR_LABEL_VALUE"] == "repairing"
            state["forced_roles"] = Path(environment["CLUSTERMESH_DEBUG_FORCE_REPAIR_ROLES_FILE"]).read_text(
                encoding="utf-8",
            ).splitlines()
            Path(log_path).write_text("original Fleet repair stdout/stderr\n", encoding="utf-8")
            if state.get("repair_exhausts_deadline"):
                clock[0] = args.live_overlay_timeout_seconds + 1
            if state.get("repair_exception"):
                raise arm.ReconcileError("unable to execute Fleet repair")
            return state["repair_rc"]
        phase = "initial" if option(command, "--attempts") == "5" else "after_repair"
        state["events"].append(phase)
        state["probe_phase"] = phase
        assert len(state["credential_roles"]) == 100
        assert option(command, "--retry-seconds") == "30"
        assert option(command, "--command-timeout-seconds") == "30"
        assert option(command, "--max-concurrent") == "10"
        with open(log_path, "w", encoding="utf-8") as log, redirect_stdout(log), redirect_stderr(log):
            result = overlay.main(command[2:])
        if phase == "after_repair" and result == 0:
            state["converged"] = True
        if phase == "after_repair" and state.get("after_repair"):
            state["after_repair"]()
        if state.get("proof_fault") == phase:
            Path(option(command, "--summary-file")).write_text("{broken-json", encoding="utf-8")
        if phase == "initial" and state.get("after_initial"):
            state["after_initial"]()
        return result

    monkeypatch.setattr(overlay, "probe", full_probe)
    monkeypatch.setattr(arm, "run_command", runner)
    monkeypatch.setattr(arm, "run_live_overlay_command", child)
    return state


def saved_summary(fleet):
    return json.loads(Path(fleet["args"].summary_file).read_text(encoding="utf-8"))


def test_healthy_overlay_needs_no_fleet_rejoin_and_keeps_pool_gates(fleet):
    assert arm.main([]) == 0
    assert fleet["events"] == ["initial", "pool-update:mesh-60", "pool-update:mesh-89"]
    proof = saved_summary(fleet)["live_overlay_recovery"]
    assert proof["status"] == "healthy"
    assert proof["initial"]["proof"]["cluster_count"] == 100
    assert "fleet_repair" not in proof
    inventory = json.loads((Path(proof["directory"]) / "clusters.json").read_text(encoding="utf-8"))
    assert len(inventory) == 100
    assert all(not Path(row["kubeconfig"]).exists() for row in inventory)
    assert not list(Path(proof["directory"]).rglob("*.config"))


def test_genuine_98_of_99_directed_peer_drift_repairs_remote_role_once(fleet, monkeypatch):
    fleet["drift"] = True
    monkeypatch.setenv("CLUSTERMESH_DEBUG_EXPECTED_CLUSTER_COUNT", "2")
    monkeypatch.setenv("CLUSTERMESH_DEBUG_FORCE_REPAIR_ROLES_FILE", "/untrusted-old-roles")
    monkeypatch.setenv("CMP_MEMBER_LABEL_VALUE", "wrong-selector")
    assert arm.main([]) == 0
    assert fleet["events"] == ["initial", "fleet-repair", "after_repair", "pool-update:mesh-60", "pool-update:mesh-89"]
    assert fleet["forced_roles"] == ["mesh-53"]
    proof = saved_summary(fleet)["live_overlay_recovery"]
    assert proof["status"] == "repaired"
    drift = proof["initial"]["proof"]["drift"]
    assert len(drift) == 1 and drift[0]["role"] == "mesh-60"
    assert drift[0]["not_ready_remote_names"] == ["mesh-5353"]
    assert len(drift[0]["agent_drift"]) == 3
    assert all(agent["missing_remote_names"] == [] for agent in drift[0]["agent_drift"])
    assert proof["fleet_repair"]["attempts"] == 1
    assert proof["after_repair"]["proof"]["healthy"] is True


def test_no_failed_pools_does_not_probe_or_repair_the_full_fleet(fleet):
    for row in fleet["rows"]:
        row["agentPoolProfiles"][0]["provisioningState"] = "Succeeded"
    assert arm.main([]) == 0
    assert fleet["children"] == [] and fleet["credential_roles"] == set()
    assert "live_overlay_recovery" not in saved_summary(fleet)


def test_early_recovery_is_opt_in_and_cannot_skip_original_unhealthy_pool_proof(fleet):
    fleet["args"].live_overlay_repair_enabled = False
    fleet["drift"] = True
    assert arm.main([]) == 1
    assert fleet["children"] == [] and fleet["pool_updates"] == []
    assert all(not item["mutation_started"] for item in saved_summary(fleet)["pool_repairs"])


@pytest.mark.parametrize("fault", [
    "partial-inventory", "duplicate-role", "wrong-subscription", "wrong-region", "wrong-tfvars",
    "wrong-aks-id", "wrong-run-id", "missing-node-group", "wrong-managed-by", "expired-lease",
    "invalid-lease", "short-child-lease", "fleet-busy", "profile-busy", "member-busy",
    "wrong-member-id", "wrong-selector", "partial-applied", "duplicate-fleet-identity", "provider-busy",
    "malformed-tags", "missing-fleet-identity",
])
def test_unsafe_authority_prevents_even_the_initial_live_probe(fleet, fault):
    if fault == "partial-inventory":
        fleet["rows"].pop()
    elif fault == "duplicate-role":
        fleet["rows"][0] = fleet["rows"][1]
    elif fault == "wrong-subscription":
        fleet["subscription"] = "other"
    elif fault == "wrong-region":
        fleet["group"]["location"] = "other"
    elif fault == "wrong-tfvars":
        fleet["group"]["tags"]["clustermesh_debug_tfvars_sha256"] = "other"
    elif fault == "wrong-aks-id":
        fleet["rows"][0]["id"] = fleet["rows"][1]["id"]
    elif fault == "wrong-run-id":
        fleet["rows"][0]["tags"]["run_id"] = "other"
    elif fault == "missing-node-group":
        fleet["groups"].pop()
    elif fault == "wrong-managed-by":
        fleet["groups"][0]["managedBy"] = "unowned"
    elif fault in ("expired-lease", "invalid-lease"):
        fleet["group"]["tags"]["deletion_due_time"] = "2000-01-01T00:00:00Z" if fault == "expired-lease" else 123
    elif fault == "short-child-lease":
        fleet["groups"][0]["deletion_due_time"] = "2098-01-01T00:00:00Z"
    elif fault == "fleet-busy":
        fleet["fleet"]["provisioningState"] = "Updating"
    elif fault == "profile-busy":
        fleet["profile"]["properties"]["provisioningState"] = "Updating"
    elif fault == "member-busy":
        fleet["members"][0]["provisioningState"] = "Updating"
    elif fault == "wrong-member-id":
        fleet["members"][0]["clusterResourceId"] = "unowned"
    elif fault == "wrong-selector":
        fleet["members"][0]["labels"]["mesh"] = "repairing"
    elif fault == "partial-applied":
        fleet["applied"].pop()
    elif fault == "provider-busy":
        fleet["latest_operation"] = {"status": "InProgress"}
    elif fault == "malformed-tags":
        fleet["rows"][0]["tags"] = ["role", "mesh-1"]
    elif fault == "missing-fleet-identity":
        fleet["members"][0]["meshProperties"]["ciliumProperties"] = None
    else:
        fleet["members"][0]["meshProperties"]["ciliumProperties"]["id"] = 2
    assert arm.main([]) == 1
    assert fleet["children"] == [] and fleet["pool_updates"] == []
    assert saved_summary(fleet)["healthy"] is False


@pytest.mark.parametrize("fault", ["credentials", "identity", "agent", "malformed-proof", "wrong-live-identity"])
def test_authorization_and_unreadable_probes_never_authorize_fleet_mutation(fleet, fault):
    fleet["drift"] = True
    if fault == "credentials":
        fleet["credential_failure"] = "mesh-53"
    elif fault == "identity":
        fleet["identity_failure"] = "mesh-53"
    elif fault == "agent":
        fleet["agent_failure"] = ("initial", "mesh-60")
    elif fault == "malformed-proof":
        fleet["proof_fault"] = "initial"
    else:
        fleet["wrong_live_identity"] = "mesh-53"
    assert arm.main([]) == 1
    assert "fleet-repair" not in fleet["events"] and fleet["pool_updates"] == []
    evidence = saved_summary(fleet)["live_overlay_recovery"]
    assert evidence["status"] == "failed"
    if fault == "agent":
        assert evidence["initial"]["exit_code"] == 2
        assert "Forbidden" in evidence["initial"]["proof"]["drift"][0]["command_error"]
        assert "unreadable" in evidence["error"]


@pytest.mark.parametrize("conflict", ["aks", "fleet", "identity", "provider"])
def test_recheck_blocks_operations_or_identity_changes_that_started_during_probe(fleet, conflict):
    fleet["drift"] = True

    def change():
        if conflict == "aks":
            fleet["rows"][0]["agentPoolProfiles"][0]["provisioningState"] = "Upgrading"
        elif conflict == "fleet":
            fleet["fleet"]["provisioningState"] = "Updating"
        elif conflict == "provider":
            fleet["latest_operation"] = {"status": "InProgress"}
        else:
            fleet["members"][0]["meshProperties"]["ciliumProperties"]["id"] = 200

    fleet["after_initial"] = change
    assert arm.main([]) == 1
    assert fleet["events"] == ["initial"] and fleet["pool_updates"] == []
    assert saved_summary(fleet)["live_overlay_recovery"]["initial"]["exit_code"] == 2


def test_even_a_healthy_probe_cannot_use_a_stale_provider_inventory(fleet):
    def change():
        fleet["fleet"]["provisioningState"] = "Updating"

    fleet["after_initial"] = change
    assert arm.main([]) == 1
    assert fleet["events"] == ["initial"] and fleet["pool_updates"] == []


def test_fleet_member_list_order_is_not_identity_drift(fleet):
    fleet["drift"] = True
    fleet["after_initial"] = lambda: fleet["members"].reverse()
    assert arm.main([]) == 0
    assert fleet["forced_roles"] == ["mesh-53"]


def test_failed_cluster_operation_gate_is_rechecked_after_overlay_recovery(fleet):
    fleet["rows"][1]["provisioningState"] = "Failed"
    fleet["drift"] = True

    def change():
        fleet["latest_operation"] = {"status": "InProgress", "operationType": "Upgrade"}

    fleet["after_repair"] = change
    assert arm.main([]) == 1
    evidence = saved_summary(fleet)
    assert evidence["failure_evidence"]["mesh-2"]["operation_id"] == "failed-addon-operation"
    assert evidence["live_overlay_recovery"]["status"] == "failed"
    assert "provider operation is not safely terminal" in evidence["fatal_error"]
    assert (
        evidence["live_overlay_recovery"]["post_repair_failure_evidence"]["latest_operation"]["status"]
        == "InProgress"
    )
    assert fleet["pool_updates"] == []


@pytest.mark.parametrize("exception", [False, True])
def test_failed_mutation_keeps_original_repair_log_and_postproof_without_pool_updates(fleet, exception):
    fleet.update(drift=True, repair_rc=17, repair_exception=exception)
    assert arm.main([]) == 1
    assert fleet["events"] == ["initial", "fleet-repair", "after_repair"]
    evidence = saved_summary(fleet)["live_overlay_recovery"]
    assert evidence["status"] == "failed"
    assert evidence["initial"]["proof"]["repair_roles"] == ["mesh-53"]
    assert Path(evidence["fleet_repair"]["log_file"]).read_text(encoding="utf-8") == "original Fleet repair stdout/stderr\n"
    assert evidence["after_repair"]["proof"]["healthy"] is True
    assert "Fleet repair" in evidence["fleet_repair"]["error"]


@pytest.mark.parametrize("fault", ["drift", "agent", "malformed-proof"])
def test_failed_postproof_stops_without_a_second_rejoin(fleet, fault):
    fleet["drift"] = True
    if fault == "drift":
        fleet["post_drift"] = True
    elif fault == "agent":
        fleet["agent_failure"] = ("after_repair", "mesh-53")
    else:
        fleet["proof_fault"] = "after_repair"
    assert arm.main([]) == 1
    assert fleet["events"] == ["initial", "fleet-repair", "after_repair"]
    evidence = saved_summary(fleet)["live_overlay_recovery"]
    assert evidence["fleet_repair"]["exit_code"] == 0
    assert evidence["post_repair_error"]
    assert Path(evidence["initial"]["summary_file"]).is_file()
    assert Path(evidence["after_repair"]["summary_file"]).is_file()


def test_mutation_and_postproof_errors_are_both_retained(fleet):
    fleet.update(drift=True, repair_rc=17, post_drift=True)
    assert arm.main([]) == 1
    evidence = saved_summary(fleet)["live_overlay_recovery"]
    assert "exit=17" in evidence["error"]
    assert "did not converge" in evidence["error"]
    assert evidence["fleet_repair"]["exit_code"] == 17
    assert evidence["after_repair"]["exit_code"] == 2
    assert fleet["events"].count("fleet-repair") == 1 and fleet["pool_updates"] == []


def test_shared_deadline_exhaustion_before_repair_never_mutates(fleet):
    fleet["drift"] = True
    fleet["after_initial"] = lambda: fleet["clock"].__setitem__(0, fleet["args"].live_overlay_timeout_seconds + 1)
    assert arm.main([]) == 1
    assert fleet["events"] == ["initial"] and fleet["pool_updates"] == []
    assert "deadline exhausted" in saved_summary(fleet)["fatal_error"]


def test_timed_out_repair_retains_evidence_and_records_why_postproof_could_not_run(fleet):
    fleet.update(drift=True, repair_rc=124, repair_exhausts_deadline=True)
    assert arm.main([]) == 1
    evidence = saved_summary(fleet)["live_overlay_recovery"]
    assert fleet["events"] == ["initial", "fleet-repair"]
    assert evidence["fleet_repair"]["exit_code"] == 124
    assert Path(evidence["fleet_repair"]["log_file"]).is_file()
    assert Path(evidence["initial"]["summary_file"]).is_file()
    assert "deadline exhausted" in evidence["post_repair_error"]
    assert fleet["pool_updates"] == []


@pytest.mark.parametrize("fault", ["cilium", "workers", "vmss", "coverage", "configuration", "mutation"])
def test_successful_overlay_repair_never_weakens_original_pool_preconditions(fleet, fault):
    fleet.update(drift=True, pool_fault=fault)
    assert arm.main([]) == 1
    evidence = saved_summary(fleet)
    assert evidence["live_overlay_recovery"]["status"] == "repaired"
    if fault == "mutation":
        assert fleet["pool_updates"] == ["mesh-60"]
        assert evidence["pool_repairs"][0]["mutation_started"] is True
        assert len(evidence["pool_repairs"]) == 1
    else:
        assert fleet["pool_updates"] == ["mesh-89"]
        assert evidence["pool_repairs"][0]["mutation_started"] is False
        assert evidence["pool_repairs"][1]["status"] == "repaired"


@pytest.mark.parametrize("fault", ["cilium", "workers", "vmss", "coverage"])
def test_succeeded_arm_state_never_overrides_failed_pool_posthealth(fleet, fault):
    fleet.update(drift=True, pool_fault=f"post-{fault}")
    assert arm.main([]) == 1
    evidence = saved_summary(fleet)
    assert evidence["healthy"] is False
    assert evidence["live_overlay_recovery"]["status"] == "repaired"
    assert fleet["pool_updates"] == ["mesh-60"]
    assert len(evidence["pool_repairs"]) == 1
    repair = evidence["pool_repairs"][0]
    assert repair["observed_states"] == ["Succeeded"]
    assert repair["configuration_after"] == repair["configuration_before"]
    assert repair["health_before"]["healthy"] is True
    assert repair["status"] == "failed" and repair["mutation_started"] is True
    if fault == "cilium":
        assert repair["failure_evidence"]["cilium_health"]["healthy"] is False
    assert Path(evidence["live_overlay_recovery"]["after_repair"]["summary_file"]).is_file()


def test_logged_command_retains_complete_output_on_nonzero_exit(tmp_path):
    log_path = tmp_path / "failed-child.log"
    result = arm.run_live_overlay_command(
        [sys.executable, "-c", "import sys; print('before failure'); print('details' * 1000, file=sys.stderr); sys.exit(7)"],
        10, str(log_path),
    )
    assert result == 7
    output = log_path.read_text(encoding="utf-8")
    assert "before failure" in output and "details" * 1000 in output


def test_logged_command_timeout_keeps_partial_output(tmp_path):
    log_path = tmp_path / "timed-out-child.log"
    result = arm.run_live_overlay_command(
        [sys.executable, "-c", "import time; print('started', flush=True); time.sleep(10)"],
        1, str(log_path),
    )
    assert result == 124
    assert log_path.read_text(encoding="utf-8") == "started\n"


def test_logged_command_start_failure_keeps_diagnostics(tmp_path, monkeypatch):
    def fail(*_args, **_kwargs):
        raise OSError("Permission denied")

    monkeypatch.setattr(arm.subprocess, "run", fail)
    log_path = tmp_path / "not-started.log"
    with pytest.raises(arm.ReconcileError, match="Permission denied"):
        arm.run_live_overlay_command(["not-executed"], 1, str(log_path))
    assert "Permission denied" in log_path.read_text(encoding="utf-8")


@pytest.mark.parametrize("extra", [
    ["--live-overlay-repair-enabled"],
    ["--live-overlay-repair-enabled", "--failed-pool-repair-enabled", "--expected-count", "2"],
    ["--live-overlay-max-repair-roles", "21"],
    ["--live-overlay-max-repair-roles", "0"],
    ["--live-overlay-timeout-seconds", "0"],
])
def test_early_recovery_cli_rejects_unsafe_scope_or_budgets(tmp_path, extra):
    with pytest.raises(SystemExit):
        arm.parse_args([
            "--resource-group", "12345-deadbeef", "--expected-subscription", "s",
            "--expected-region", "eastus2euap", "--expected-count", "100",
            "--expected-tfvars-sha", "expected-sha", "--summary-file", str(tmp_path / "summary.json"),
            *extra,
        ])


@pytest.mark.parametrize("fault", ["partial", "duplicate", "stale-roles", "too-many-roles", "false-success"])
def test_overlay_subprocess_proof_requires_exact_current_bounded_output(tmp_path, fault):
    identities = [{"role": f"mesh-{number}", "cluster_name": f"mesh-{number}{number}", "cluster_id": number}
                  for number in range(1, 101)]
    proof = {
        "healthy": False, "cluster_count": 100, "identities": copy.deepcopy(identities),
        "drift": [{"role": "mesh-60", "not_ready_remote_names": ["mesh-5353"], "command_error": None}],
        "repair_roles": ["mesh-53"],
        "repair_selection": {"cover_within_limit": True, "repair_roles": ["mesh-53"]},
    }
    if fault == "partial":
        proof["identities"].pop()
    elif fault == "duplicate":
        proof["identities"][0] = proof["identities"][1]
    elif fault == "too-many-roles":
        proof["repair_roles"] = [item["role"] for item in identities[:21]]
        proof["repair_selection"]["repair_roles"] = proof["repair_roles"]
    roles = tmp_path / "roles.txt"
    roles.write_text(
        "mesh-99\n" if fault == "stale-roles" else "".join(f"{role}\n" for role in proof["repair_roles"]),
        encoding="utf-8",
    )
    with pytest.raises(arm.ReconcileError):
        arm.validate_live_overlay_proof(proof, 0 if fault == "false-success" else 2, str(roles), identities, 20)
