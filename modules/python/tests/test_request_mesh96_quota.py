"""Offline tests for the fixed-scope quota request; no Azure calls are permitted."""

import copy
import importlib.util
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "clusterloader2" / "clustermesh-scale" / "request_mesh96_quota.py"
)
SPEC = importlib.util.spec_from_file_location("request_mesh96_quota", MODULE_PATH)
quota = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = quota
sys.path.insert(0, str(MODULE_PATH.parent))
try:
    SPEC.loader.exec_module(quota)
finally:
    sys.path.pop(0)
REQUEST_ID = "2b5c8515-37d8-4b6a-879b-cd641a2cf605"
LOCATION = f"{quota.HOST}{quota.QUOTA_ROOT}/quotaRequests/{REQUEST_ID}?api-version=2025-09-01"
OPERATION = LOCATION.replace("/quotaRequests/", "/operationsStatus/")
PRIVATE = "PRIVATE_SERVICE_CONNECTION_TOKEN_AND_RESPONSE"


def test_quota_evidence_uses_shared_azure_timestamp_precision():
    assert quota.timestamp("2026-09-12T08:42:20.6101413Z").isoformat() == "2026-09-12T08:42:20.610141+00:00"


def checkpoint():
    return {
        "execute": True, "mutation_started": True, "success": False, "status": "failed",
        "plan_sha256": quota.PLAN_SHA256,
        "original_identity": {
            "vm_id": quota.VM_ID, "node_uid": quota.NODE_UID, "node_name": f"{quota.PROM_VMSS}000000",
            "instance_id": "0", "provider_id": quota.PROVIDER_ID,
        },
        "replacement": {
            "automatic_retry_allowed": False, "replacement_completed": False,
            "delete": {
                "attempted": True, "accepted": True, "ambiguous": False,
                "requested_at": "2026-09-12T11:10:52Z", "returned_at": "2026-09-12T11:10:55Z",
            },
            "native_removal": {
                "old_node_pods_nnc_absent": True, "manual_marker_clearance": False,
                "original_marker_removed_by": "native-node-removal", "pool_count": 0,
                "vmss_capacity": 0, "verified_at": "2026-09-12T11:12:10Z",
            },
            "removal_observation": {"arm_empty": True, "old_resources_absent": True, "pool_count": 0},
            "restore": {
                "attempted": True, "accepted": None, "ambiguous": True,
                "requested_at": "2026-09-12T11:12:43Z", "returned_at": "2026-09-12T11:12:48Z",
            },
        },
        "error": f"ErrCode_InsufficientVCPUQuota {PRIVATE}",
    }


def resource(kind="quotas", limit="5000", used="5464"):
    props = {"name": {"value": quota.FAMILY}, "unit": "Count", "resourceType": "dedicated"}
    if kind == "quotas":
        props["limit"] = {"limitObjectType": "LimitValue", "value": limit}
    else:
        props["usages"] = {"usagesType": "Individual", "value": used}
    return {
        "id": f"{quota.QUOTA_ROOT}/{kind}/{quota.FAMILY}", "name": quota.FAMILY,
        "type": f"Microsoft.Quota/{kind}", "properties": props, "systemData": {"private": PRIVATE},
    }


def history(state="Accepted", *, desired=5500, request_id=REQUEST_ID, family=quota.FAMILY):
    return {
        "id": f"{quota.QUOTA_ROOT}/quotaRequests/{request_id}", "name": request_id,
        "type": "Microsoft.Quota/quotaRequests",
        "properties": {
            "requestSubmitTime": datetime.now(timezone.utc).isoformat(),
            "provisioningState": state, "message": PRIVATE,
            "value": [{
                "name": {"value": family}, "limit": {"limitObjectType": "LimitValue", "value": desired},
                "provisioningState": state, "message": PRIVATE,
            }],
        },
    }


class Clock:
    def __init__(self):
        self.value = 0
        self.waits = []

    def __call__(self):
        return self.value

    def sleep(self, seconds):
        self.waits.append(seconds)
        self.value += seconds


class FakeAzure:
    def __init__(self, args):
        self.args = args
        self.clock = Clock()
        self.commands, self.calls, self.puts = [], [], []
        self.limit, self.compute_limit, self.used = "5000", "5000", "5464"
        self.regional_used, self.regional_limit = "8682", "11897"
        self.quota_override, self.usage_override = None, None
        self.history_values = []
        self.history_sequence = None
        self.history_calls = 0
        self.apply = False
        self.put_reply = quota.Reply(202, None, {"location": LOCATION, "retry-after": "30"})
        self.poll_reply = quota.Reply(200, history(), {})
        self.get_error = None
        self.put_error = None
        self.read_override = {}
        self.cli_error = None

    def runner(self, command, timeout):
        assert 0 < timeout <= 45
        assert command[0] == "az" and "--debug" not in command
        self.commands.append(command)
        if self.cli_error:
            raise self.cli_error
        key = tuple(command[1:4]) if command[1:3] == ["aks", "nodepool"] else tuple(command[1:3])
        if key in self.read_override:
            return copy.deepcopy(self.read_override[key])
        if key == ("account", "show"):
            return {"id": quota.SUBSCRIPTION, "state": "Enabled"}
        if key == ("group", "show"):
            return {
                "id": quota.GROUP_ID, "location": quota.REGION,
                "tags": {
                    "run_id": quota.RESOURCE_GROUP, "clustermesh_debug_preserved": "true",
                    "deletion_due_time": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
                    "private": PRIVATE,
                },
            }
        if key == ("aks", "show"):
            return {
                "id": quota.CLUSTER_ID, "name": quota.CLUSTER, "location": quota.REGION,
                "nodeResourceGroup": quota.NODE_GROUP, "tags": {"run_id": quota.RESOURCE_GROUP, "role": "mesh-96"},
            }
        if key == ("aks", "nodepool", "list"):
            return [{
                "name": name, "id": f"{quota.CLUSTER_ID}/agentPools/{name}",
                "count": 0 if name == "prompool" else 2, "mode": "User" if name == "prompool" else "System",
                "provisioningState": "Succeeded", "powerState": {"code": "Running"},
                "vmSize": "Standard_D8_v3", "enableAutoScaling": False,
            } for name in ("default", "prompool")]
        if key == ("vmss", "show"):
            return {"id": quota.VMSS_ID, "sku": {"capacity": 0}, "provisioningState": "Succeeded"}
        if key == ("vmss", "list-instances"):
            return []
        if key == ("provider", "show"):
            return {"namespace": "Microsoft.Quota", "registrationState": "Registered"}
        if key == ("vm", "list-usage"):
            return [
                {"name": quota.FAMILY, "unit": "Count", "currentValue": self.used, "limit": self.compute_limit},
                {"name": "cores", "unit": "Count", "currentValue": self.regional_used, "limit": self.regional_limit},
            ]
        raise AssertionError(f"Unexpected CLI command: {command}")

    def request(self, method, url, *, timeout, body=None, correlation=None):
        assert 0 < timeout <= 45
        self.calls.append((method, url))
        if method == "PUT":
            self.puts.append((url, copy.deepcopy(body), correlation))
            journal = Path(self.args.native_checkpoint).with_name("request_mesh96_quota.attempt.json")
            saved = json.loads(journal.read_text(encoding="utf-8"))
            assert saved["desired_limit"] == 5500 and saved["request_scope"] == quota.SCOPE
            assert saved["request"]["attempted"] and saved["request"]["ambiguous"]
            assert saved["request"]["correlation_id"] == correlation
            assert json.loads(Path(self.args.summary_file).read_text(encoding="utf-8"))["mutation_started"] is True
            if self.put_error:
                raise self.put_error
            if self.apply:
                self.limit = self.compute_limit = "5500"
            return copy.deepcopy(self.put_reply)
        assert method == "GET"
        if self.get_error:
            return quota.Reply(self.get_error, {"error": {"message": PRIVATE}}, {})
        if url == quota.QUOTA_URL:
            return quota.Reply(200, self.quota_override or resource(limit=self.limit), {})
        if url == quota.USAGE_URL:
            return quota.Reply(200, self.usage_override or resource("usages", used=self.used), {})
        if url == quota.HISTORY_URL:
            values = self.history_values
            if self.history_sequence is not None:
                values = self.history_sequence[min(self.history_calls, len(self.history_sequence) - 1)]
            self.history_calls += 1
            return quota.Reply(200, {"value": copy.deepcopy(values)}, {})
        assert url in {LOCATION, OPERATION}, f"Unexpected HTTP URL: {url}"
        return copy.deepcopy(self.poll_reply)

    def run(self):
        return quota.run(self.args, runner=self.runner, client=self, clock=self.clock, sleep=self.clock.sleep)


@pytest.fixture(name="options")
def options_fixture(tmp_path):
    native = tmp_path / "native.json"
    native.write_text(json.dumps(checkpoint()), encoding="utf-8")
    return quota.parse_args([
        "--resource-group", quota.RESOURCE_GROUP, "--confirm-resource-group", quota.RESOURCE_GROUP,
        "--expected-subscription", quota.SUBSCRIPTION, "--expected-region", quota.REGION,
        "--native-checkpoint", str(native), "--summary-file", str(tmp_path / "summary.json"),
        "--timeout-seconds", "100",
    ])


@pytest.mark.parametrize("value,expected", [(0, 0), (5464, 5464), ("0", 0), ("5464", 5464)])
def test_unsigned_integer_normalization(value, expected):
    assert quota.unsigned(value) == expected


@pytest.mark.parametrize("value", [True, False, -1, 1.0, 1.5, "-1", "+1", "01", " 1", "1 ", "1.0", "1e3", "", None, {}, "١"])
def test_unsigned_rejects_ambiguous_values(value):
    with pytest.raises(quota.Blocked, match="invalid_nonnegative_integer"):
        quota.unsigned(value)


def test_default_plan_is_read_only_and_handles_negative_headroom(options):
    azure = FakeAzure(options)
    result = azure.run()
    assert result["success"] and result["plan_valid"] and result["status"] == "planned"
    assert not result["execute"] and not result["mutation_started"] and not result["request"]["attempted"]
    assert not result["capacity_available"]
    assert result["quota"]["family_remaining"] == -464 and result["quota"]["regional_remaining"] == 3215
    assert result["quota"]["proposed_remaining"] == 36 and result["required_vcpus"] == 24
    assert not azure.puts and all(method == "GET" for method, _ in azure.calls)
    assert not Path(result["attempt_journal"]).exists()
    assert result["proposed_body"]["properties"]["limit"] == {"limitObjectType": "LimitValue", "value": 5500}


@pytest.mark.parametrize("field,value", [
    ("resource_group", "other"), ("confirm_resource_group", "other"),
    ("expected_subscription", "other"), ("expected_region", "eastus"),
    ("timeout_seconds", 1801), ("timeout_seconds", 0), ("timeout_seconds", True),
])
def test_argument_guards_do_not_contact_azure(options, field, value):
    setattr(options, field, value)
    options.execute = True
    azure = FakeAzure(options)
    assert not azure.run()["success"]
    assert not azure.commands and not azure.calls


@pytest.mark.parametrize("path,value", [
    (("plan_sha256",), "a" * 64),
    (("original_identity", "vm_id"), "other"),
    (("original_identity", "node_uid"), "other"),
    (("original_identity", "provider_id"), quota.PROVIDER_ID.replace(quota.RESOURCE_GROUP, "other")),
    (("replacement", "delete", "accepted"), False),
    (("replacement", "delete", "ambiguous"), True),
    (("replacement", "native_removal", "vmss_capacity"), True),
    (("replacement", "native_removal", "old_node_pods_nnc_absent"), False),
    (("replacement", "native_removal", "manual_marker_clearance"), True),
    (("replacement", "removal_observation", "arm_empty"), False),
    (("replacement", "restore", "attempted"), False),
    (("error",), "unrelated failure"),
])
def test_checkpoint_guards_no_calls(options, path, value):
    data = checkpoint()
    target = data
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value
    Path(options.native_checkpoint).write_text(json.dumps(data), encoding="utf-8")
    azure = FakeAzure(options)
    options.execute = True
    assert not azure.run()["success"]
    assert azure.calls == azure.commands == []


def test_malformed_checkpoint_and_output_collision_are_safe(options):
    Path(options.native_checkpoint).write_text("{broken", encoding="utf-8")
    azure = FakeAzure(options)
    assert azure.run()["blocked_reason"] == "invalid_local_evidence"
    options.summary_file = options.native_checkpoint
    assert azure.run()["blocked_reason"] == "unsafe_local_paths"
    assert Path(options.native_checkpoint).read_text(encoding="utf-8") == "{broken"


@pytest.mark.parametrize("limit,used,regional_used,expected", [
    ("5000", "5477", "8682", "fixed_ceiling_insufficient"),
    ("5500", "5490", "8682", "fixed_ceiling_insufficient"),
    ("5000", "5464", "11874", "regional_headroom_insufficient"),
    ("5000", "5464", "11900", "regional_headroom_insufficient"),
])
def test_fixed_family_and_regional_ceiling(options, limit, used, regional_used, expected):
    options.execute = True
    azure = FakeAzure(options)
    azure.limit = azure.compute_limit = limit
    azure.used, azure.regional_used = used, regional_used
    result = azure.run()
    assert result["blocked_reason"] == expected and not result["success"]
    assert not azure.puts


@pytest.mark.parametrize("limit,used", [("6000", "5500"), ("5500", "5464"), ("5000", "4976")])
def test_already_sufficient_never_reduces_or_requests(options, limit, used):
    options.execute = True
    azure = FakeAzure(options)
    azure.limit = azure.compute_limit = limit
    azure.used = used
    result = azure.run()
    assert result["status"] == "already_sufficient" and result["capacity_available"]
    assert result["success"] and not azure.puts and not result["mutation_started"]


@pytest.mark.parametrize("key,payload,reason", [
    (("account", "show"), {"id": "other", "state": "Enabled"}, "account_scope_mismatch"),
    (("group", "show"), {"id": quota.GROUP_ID, "location": quota.REGION, "tags": {}},
     "resource_group_ownership_mismatch"),
    (("aks", "show"), {"id": quota.CLUSTER_ID, "name": quota.CLUSTER, "nodeResourceGroup": "other"},
     "cluster_scope_mismatch"),
    (("vmss", "list-instances"), [{"id": "old-machine"}], "native_vmss_instances_present"),
    (("provider", "show"), {"namespace": "Microsoft.Quota", "registrationState": "NotRegistered"},
     "provider_not_registered"),
])
def test_fresh_scope_and_registration_guards(options, key, payload, reason):
    options.execute = True
    azure = FakeAzure(options)
    azure.read_override[key] = payload
    result = azure.run()
    assert result["blocked_reason"] == reason and not result["success"] and not azure.puts


@pytest.mark.parametrize("http_status", [401, 403, 500])
def test_read_errors_do_not_write_or_publish_private_body(options, http_status):
    options.execute = True
    azure = FakeAzure(options)
    azure.get_error = http_status
    result = azure.run()
    assert not result["success"] and not azure.puts and PRIVATE not in json.dumps(result)


def test_cli_rbac_error_is_sanitized(options, monkeypatch):
    monkeypatch.setattr(quota.subprocess, "run", lambda *a, **k: SimpleNamespace(
        returncode=1, stderr=f"AuthorizationFailed {PRIVATE}", stdout=PRIVATE,
    ))
    with pytest.raises(quota.Blocked, match="^authorization_unavailable$"):
        quota.run_cli(["az", "account", "show"], 1)
    azure = FakeAzure(options)
    azure.cli_error = quota.AzureError(PRIVATE)
    assert PRIVATE not in json.dumps(azure.run())
    assert not azure.puts


@pytest.mark.parametrize("field,value", [
    ("id", f"/subscriptions/other/providers/Microsoft.Quota/quotas/{quota.FAMILY}"),
    ("name", "standardDv2Family"), ("type", "Microsoft.Compute/usages"),
    ("unit", "Bytes"), ("resourceType", "lowPriority"), ("limitObjectType", "Shared"),
    ("value", "5e3"),
])
def test_quota_response_scope_type_and_units(options, field, value):
    options.execute = True
    azure = FakeAzure(options)
    response = resource()
    if field in {"id", "name", "type"}:
        response[field] = value
    elif field in {"value", "limitObjectType"}:
        response["properties"]["limit"][field] = value
    else:
        response["properties"][field] = value
    azure.quota_override = response
    assert not azure.run()["success"] and not azure.puts


def test_views_must_both_have_usable_headroom(options):
    options.execute = True
    azure = FakeAzure(options)
    azure.limit = "5500"
    result = azure.run()
    assert result["blocked_reason"] == "quota_not_effective"
    assert not result["capacity_available"] and not azure.puts


@pytest.mark.parametrize("counter", ["limit", "currentValue"])
@pytest.mark.parametrize("name", [quota.FAMILY, "cores"])
@pytest.mark.parametrize("bad", [True, 5464.0, "05464", "5e3", "-4"])
def test_compute_decimal_counter_validation_is_enforced(options, counter, name, bad):
    options.execute = True
    azure = FakeAzure(options)
    rows = azure.runner(["az", "vm", "list-usage"], 1)
    next(row for row in rows if row["name"] == name)[counter] = bad
    azure.read_override[("vm", "list-usage")] = rows
    assert azure.run()["blocked_reason"] == "invalid_nonnegative_integer"
    assert not azure.puts


@pytest.mark.parametrize("state,status", [
    ("Accepted", "request_pending"), ("InProgress", "request_pending"),
    ("Succeeded", "approved_not_effective"), ("Failed", "request_failed"), ("Invalid", "request_failed"),
])
@pytest.mark.parametrize("execute", [False, True])
def test_existing_request_is_observed_never_resubmitted(options, state, status, execute):
    options.execute = execute
    azure = FakeAzure(options)
    azure.history_values = [history(state)]
    result = azure.run()
    assert result["status"] == status and not result["success"]
    assert not azure.puts and not result["mutation_started"] and not result["request"]["attempted"]
    assert result["observed_request"]["state"] == state
    assert azure.clock.value < options.timeout_seconds


def test_fresh_pre_put_history_prevents_duplicate(options):
    options.execute = True
    azure = FakeAzure(options)
    azure.history_sequence = [[], [history()]]
    assert azure.run()["status"] == "request_pending"
    assert not azure.puts and azure.history_calls >= 2


def test_other_family_pending_does_not_change_requested_family(options):
    azure = FakeAzure(options)
    azure.history_values = [history(family="standardDv2Family")]
    assert azure.run()["status"] == "planned"
    assert azure.run()["proposed_body"]["properties"]["name"] == {"value": quota.FAMILY}
    assert not azure.puts


def test_pending_batch_does_not_hide_failed_target_subrequest(options):
    options.execute = True
    azure = FakeAzure(options)
    record = history()
    record["properties"]["value"][0]["provisioningState"] = "Failed"
    azure.history_values = [record]
    assert azure.run()["status"] == "request_failed"
    assert not azure.puts


def test_exactly_one_normal_put_and_effective_quota(options):
    options.execute = True
    azure = FakeAzure(options)
    azure.apply = True
    result = azure.run()
    assert result["success"] and result["status"] == "quota_available"
    assert result["request"]["accepted"] and not result["request"]["ambiguous"]
    assert result["quota"]["family_remaining"] == 36
    assert len(azure.puts) == 1 and azure.history_calls >= 3
    url, body, correlation = azure.puts[0]
    assert url == quota.QUOTA_URL and correlation
    assert body == {"properties": {
        "name": {"value": quota.FAMILY}, "limit": {"limitObjectType": "LimitValue", "value": 5500},
        "unit": "Count", "resourceType": "dedicated",
    }}
    assert PRIVATE not in Path(options.summary_file).read_text(encoding="utf-8")
    assert PRIVATE not in Path(result["attempt_journal"]).read_text(encoding="utf-8")


def test_plan_then_execute_same_summary_only_executes_once(options):
    azure = FakeAzure(options)
    assert azure.run()["status"] == "planned"
    options.execute = True
    azure.apply = True
    assert azure.run()["success"]
    assert azure.run()["success"]
    assert len(azure.puts) == 1


def test_checkpoint_change_after_plan_blocks_execution(options):
    azure = FakeAzure(options)
    assert azure.run()["success"]
    data = checkpoint()
    data["extra"] = "changed"
    Path(options.native_checkpoint).write_text(json.dumps(data), encoding="utf-8")
    options.execute = True
    assert azure.run()["blocked_reason"] == "checkpoint_changed_since_plan"
    assert not azure.puts


@pytest.mark.parametrize("location", [
    f"http://management.azure.com{quota.QUOTA_ROOT}/quotaRequests/{REQUEST_ID}?api-version=2025-09-01",
    LOCATION.replace("management.azure.com", "evil.example"),
    LOCATION.replace("management.azure.com", "management.azure.com.evil.example"),
    LOCATION.replace("management.azure.com", "user:password@management.azure.com"),
    LOCATION.replace(quota.SUBSCRIPTION, "00000000-0000-0000-0000-000000000000"),
    LOCATION.replace(quota.REGION, "eastus"),
    LOCATION.replace("/quotaRequests/", "/providers/Microsoft.Compute/virtualMachines/"),
    LOCATION.replace("/quotaRequests/", "/quotaRequests/../quotaRequests/"),
    LOCATION.replace("/quotaRequests/", "/quotaRequests/%2e%2e/quotaRequests/"),
    LOCATION + "&access_token=" + PRIVATE,
    LOCATION + "#fragment",
    LOCATION.replace("management.azure.com", "management.azure.com:443"),
])
def test_unsafe_location_never_followed_and_never_retried(options, location):
    options.execute = True
    azure = FakeAzure(options)
    azure.put_reply.headers["location"] = location
    result = azure.run()
    assert result["request"]["accepted"] and result["request"]["ambiguous"]
    assert not result["success"] and len(azure.puts) == 1
    assert ("GET", location) not in azure.calls
    assert PRIVATE not in Path(options.summary_file).read_text(encoding="utf-8")
    assert azure.run()["request"]["ambiguous"] and len(azure.puts) == 1


@pytest.mark.parametrize("reply", [
    quota.Reply(202, None, {}),
    quota.Reply(200, {"private": PRIVATE}, {}),
    quota.Reply(503, {"error": {"message": PRIVATE}}, {}),
    quota.Reply(307, None, {"location": LOCATION}),
])
def test_uncertain_responses_are_durable_no_retry(options, reply):
    options.execute = True
    azure = FakeAzure(options)
    azure.put_reply = reply
    result = azure.run()
    assert result["status"] == "request_ambiguous" and not result["success"]
    assert azure.run()["request"]["ambiguous"] and len(azure.puts) == 1


def test_transport_timeout_survives_new_summary_and_missing_journal(options):
    options.execute = True
    azure = FakeAzure(options)
    azure.put_error = TimeoutError(PRIVATE)
    result = azure.run()
    assert result["request"]["ambiguous"] and not result["success"]
    options.summary_file = str(Path(options.summary_file).with_name("inspection.json"))
    assert azure.run()["request"]["ambiguous"] and len(azure.puts) == 1
    Path(result["attempt_journal"]).unlink()
    for _ in range(2):
        result = azure.run()
        assert result["blocked_reason"] == "previous_attempt_journal_missing"
        assert result["request"]["attempted"] and result["request"]["ambiguous"]
        assert len(azure.puts) == 1 and PRIVATE not in json.dumps(result)


@pytest.mark.parametrize("http_status", [400, 401, 403, 409, 422])
def test_definite_rejection_is_truthful_and_not_retried(options, http_status):
    options.execute = True
    azure = FakeAzure(options)
    azure.put_reply = quota.Reply(http_status, {
        "error": {"code": "AuthorizationFailed", "message": PRIVATE, "details": [{"secret": PRIVATE}]},
    }, {"authorization": PRIVATE, "x-ms-request-id": PRIVATE})
    result = azure.run()
    assert result["status"] == "request_rejected" and not result["success"]
    assert not result["request"]["accepted"] and not result["request"]["ambiguous"]
    assert result["request"]["provider_error_code"] == "AuthorizationFailed"
    assert PRIVATE not in json.dumps(result)
    assert not azure.run()["success"] and len(azure.puts) == 1


@pytest.mark.parametrize("http_status", [200, 202])
def test_accepted_is_not_effective_quota_or_approval(options, http_status):
    options.execute = True
    azure = FakeAzure(options)
    azure.put_reply = quota.Reply(http_status, resource(limit=5500) if http_status == 200 else None,
                                  {"location": LOCATION, "retry-after": "30"})
    result = azure.run()
    assert result["request"]["accepted"] and result["status"] == "request_pending"
    assert not result["success"] and not result["capacity_available"] and len(azure.puts) == 1
    assert azure.clock.value < options.timeout_seconds


@pytest.mark.parametrize("state,status", [("Succeeded", "approved_not_effective"), ("Failed", "request_failed")])
def test_request_status_poll_is_not_confused_with_capacity(options, state, status):
    options.execute = True
    azure = FakeAzure(options)
    azure.poll_reply = quota.Reply(200, history(state), {})
    result = azure.run()
    assert result["status"] == status and not result["success"] and len(azure.puts) == 1
    assert any(url == LOCATION for method, url in azure.calls if method == "GET")


@pytest.mark.parametrize("state,status", [("Succeeded", "request_pending"), ("Failed", "request_failed")])
def test_operation_status_success_is_not_quota_approval(options, state, status):
    options.execute = True
    azure = FakeAzure(options)
    azure.put_reply.headers["location"] = OPERATION
    azure.poll_reply = quota.Reply(200, {"status": state, "private": PRIVATE}, {})
    result = azure.run()
    assert result["status"] == status and not result["success"] and not result["capacity_available"]
    assert len(azure.puts) == 1 and PRIVATE not in json.dumps(result)


def test_bounded_wait_honors_large_retry_after_without_sleep(options):
    options.execute = True
    azure = FakeAzure(options)
    azure.put_reply.headers["retry-after"] = "1800"
    result = azure.run()
    assert result["status"] == "request_pending" and not azure.clock.waits
    assert len(azure.puts) == 1


def test_unknown_history_state_never_authorizes_request(options):
    options.execute = True
    azure = FakeAzure(options)
    azure.history_values = [history("Success")]
    assert azure.run()["blocked_reason"] == "unknown_quota_request_state"
    assert not azure.puts


@pytest.mark.parametrize("next_link", [
    quota.HISTORY_URL,
    quota.HISTORY_URL.replace("management.azure.com", "evil.example"),
    quota.HISTORY_URL.replace(quota.REGION, "westus"),
])
def test_history_cycle_and_unsafe_pagination_never_authorize_put(options, next_link):
    options.execute = True
    azure = FakeAzure(options)
    original = azure.request

    def response(method, url, **kwargs):
        if url == quota.HISTORY_URL:
            return quota.Reply(200, {"value": [], "nextLink": next_link}, {})
        return original(method, url, **kwargs)

    azure.request = response
    assert not azure.run()["success"]
    assert not azure.puts


def test_own_request_does_not_inherit_an_unrelated_approval(options):
    options.execute = True
    azure = FakeAzure(options)
    unrelated = history("Succeeded", request_id="11111111-1111-1111-1111-111111111111")
    azure.history_sequence = [[], [], [unrelated]]
    result = azure.run()
    assert result["status"] == "request_pending"
    assert "observed_request" not in result and len(azure.puts) == 1


def test_incomplete_journal_never_authorizes_request(options):
    options.execute = True
    journal = Path(options.native_checkpoint).with_name("request_mesh96_quota.attempt.json")
    journal.write_text("", encoding="utf-8")
    azure = FakeAzure(options)
    assert not azure.run()["success"]
    assert not azure.calls and not azure.commands


def test_duplicate_checkpoint_keys_are_malformed(options):
    Path(options.native_checkpoint).write_text('{"execute":false,"execute":true}', encoding="utf-8")
    azure = FakeAzure(options)
    assert azure.run()["blocked_reason"] == "duplicate_evidence_key"
    assert not azure.commands and not azure.calls


def test_exclusive_journal_collision_cannot_issue_put(options, monkeypatch):
    options.execute = True
    azure = FakeAzure(options)
    save = quota.save_json

    def concurrent_writer(path, value, *, exclusive=False):
        if exclusive:
            save(path, {"another_process": "already attempted"}, exclusive=True)
        save(path, value, exclusive=exclusive)

    monkeypatch.setattr(quota, "save_json", concurrent_writer)
    result = azure.run()
    assert not result["success"] and result["request"]["ambiguous"]
    assert not azure.puts
    assert json.loads(Path(result["attempt_journal"]).read_text(encoding="utf-8")) == {
        "another_process": "already attempted",
    }


def test_absent_optional_unit_is_not_invented_in_put(options):
    options.execute = True
    azure = FakeAzure(options)
    response = resource()
    del response["properties"]["unit"]
    del response["properties"]["resourceType"]
    azure.quota_override = response
    result = azure.run()
    assert result["request"]["accepted"] and len(azure.puts) == 1
    assert set(azure.puts[0][1]["properties"]) == {"name", "limit"}


def test_arm_client_has_no_redirects_retries_or_credential_output(monkeypatch, capsys):
    sent, credentials, mounts = [], [], []

    class Credential:
        def __init__(self, **kwargs):
            credentials.append(kwargs)

        def get_token(self, scope):
            assert scope == "https://management.azure.com/.default"
            return SimpleNamespace(token=PRIVATE)

    class Session:
        def mount(self, scheme, adapter):
            mounts.append((scheme, adapter))

        def request(self, *args, **kwargs):
            sent.append((args, kwargs))
            return SimpleNamespace(status_code=200, headers={"Authorization": PRIVATE},
                                   json=lambda: resource())

    monkeypatch.setitem(sys.modules, "azure.identity", SimpleNamespace(AzureCliCredential=Credential))
    monkeypatch.setitem(sys.modules, "requests", SimpleNamespace(
        Session=Session, adapters=SimpleNamespace(HTTPAdapter=lambda **kwargs: kwargs),
    ))
    client = quota.ArmClient()
    reply = client.request("GET", quota.QUOTA_URL, timeout=5)
    assert mounts == [("https://", {"max_retries": 0})]
    assert credentials == [{"process_timeout": 5}]
    assert len(sent) == 1 and sent[0][1]["allow_redirects"] is False
    assert sent[0][1]["headers"]["Authorization"].split(" ", 1)[0] == "Bearer"
    assert sent[0][1]["headers"] == {"Authorization": f"Bearer {PRIVATE}"}
    assert reply.headers == {} and not capsys.readouterr().out
    with pytest.raises(quota.Blocked):
        client.request("PUT", quota.USAGE_URL, timeout=5, body={})
    assert len(sent) == 1


def test_unknown_provider_error_identifier_is_preserved_without_private_payload(options):
    options.execute = True
    azure = FakeAzure(options)
    azure.put_reply = quota.Reply(400, {"error": {
        "code": "QuotaRequestNotEnabledForRegion", "message": PRIVATE,
    }}, {})
    result = azure.run()
    assert result["request"]["provider_error_code"] == "QuotaRequestNotEnabledForRegion"
    assert PRIVATE not in json.dumps(result) and len(azure.puts) == 1


@pytest.mark.parametrize("private_message", [False, True])
def test_rejected_request_inspection_preserves_journal_and_never_resubmits(options, private_message):
    options.execute = True
    azure = FakeAzure(options)
    azure.put_reply = quota.Reply(400, {"error": {"code": "InvalidQuotaRequest"}}, {})
    original = azure.run()
    journal = Path(original["attempt_journal"])
    before = journal.read_bytes()
    options.execute = False
    options.inspect_rejection = True
    options.summary_file = str(Path(options.summary_file).with_name("inspection.json"))
    event = {
        "resourceId": f"{quota.QUOTA_ROOT}/quotas/{quota.FAMILY}",
        "eventTimestamp": original["request"]["attempted_at"], "eventDataId": REQUEST_ID,
        "correlationId": REQUEST_ID, "status": "Failed",
        "statusMessage": json.dumps({"error": {
            "code": "QuotaRequestNotSupported",
            "message": PRIVATE if private_message else "Quota increases are not supported for this resource.",
        }}),
    }
    azure.read_override[("monitor", "activity-log")] = [event]
    result = azure.run()
    assert result["success"] and result["inspection_only"] and result["status"] == "rejection_inspected"
    assert not result["mutation_started"] and result["request"]["accepted"] is False
    assert len(azure.puts) == 1 and journal.read_bytes() == before
    error = result["rejection_activity"][0]["errors"][0]
    assert error["code"] == "QuotaRequestNotSupported"
    assert error["message"] == ("[redacted]" if private_message else
                                "Quota increases are not supported for this resource.")
    assert PRIVATE not in json.dumps(result)
    command = next(row for row in azure.commands if row[1:3] == ["monitor", "activity-log"])
    assert command[command.index("--resource-id") + 1] == f"{quota.QUOTA_ROOT}/quotas/{quota.FAMILY}"
    assert command[command.index("--max-events") + 1] == "100"


@pytest.mark.parametrize("fault", ["execute", "no-journal", "foreign-resource", "wrong-time"])
def test_rejection_inspection_cannot_authorize_a_new_request_or_unscoped_reads(options, fault):
    azure = FakeAzure(options)
    if fault != "no-journal":
        options.execute = True
        azure.put_reply = quota.Reply(400, None, {})
        original = azure.run()
        azure.read_override[("monitor", "activity-log")] = [{
            "resourceId": f"{quota.QUOTA_ROOT}/quotas/{quota.FAMILY}",
            "eventTimestamp": original["request"]["attempted_at"],
            "statusMessage": None,
        }]
    options.execute = fault == "execute"
    options.inspect_rejection = True
    if fault == "foreign-resource":
        azure.read_override[("monitor", "activity-log")][0]["resourceId"] = quota.GROUP_ID
    if fault == "wrong-time":
        azure.read_override[("monitor", "activity-log")][0]["eventTimestamp"] = "2000-01-01T00:00:00Z"
    before = len(azure.puts)
    assert not azure.run()["success"] and len(azure.puts) == before
