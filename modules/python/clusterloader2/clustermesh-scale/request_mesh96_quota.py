#!/usr/bin/env python3
"""Request only the fixed mesh-96 Dv3 quota increase; never change workload capacity.

REST schemas: Microsoft Learn rest-quota-2025-09-01, Quota/CreateOrUpdate,
Usages/Get and QuotaRequestStatus/List. Keep the adjacent attempt journal with
the native checkpoint on subsequent runs, including after an interrupted PUT.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

from azure.core.exceptions import AzureError
from requests.exceptions import RequestException

import unreachable_prom_worker_recovery as recovery


SUBSCRIPTION = "37deca37-c375-4a14-b90a-043849bd2bf1"
REGION = "eastus2euap"
RESOURCE_GROUP = "78751-f36f3d5a"
CLUSTER = "clustermesh-96"
NODE_GROUP = f"mc_{RESOURCE_GROUP}_{CLUSTER}_{REGION}"
PROM_VMSS = "aks-prompool-38822163-vmss"
VM_ID = "d731b838-501d-438d-a087-fd4545f1d607"
NODE_UID = "9d8a9811-0e9a-4ff5-9a94-db89c42d223b"
PLAN_SHA256 = "1a4385e2db5a0a5b38d750a6a82fcb9b3d4c683e801bf12bd8111c75355e380a"
GROUP_ID = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{RESOURCE_GROUP}"
CLUSTER_ID = f"{GROUP_ID}/providers/Microsoft.ContainerService/managedClusters/{CLUSTER}"
VMSS_ID = (
    f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{NODE_GROUP}"
    f"/providers/Microsoft.Compute/virtualMachineScaleSets/{PROM_VMSS}"
)
PROVIDER_ID = f"azure://{VMSS_ID}/virtualMachines/0"
FAMILY = "standardDv3Family"
DESIRED_LIMIT = 5500
REQUIRED_VCPUS = 24  # Eight for restoration, sixteen for the existing bounded CNI surge.
SCOPE = f"/subscriptions/{SUBSCRIPTION}/providers/Microsoft.Compute/locations/{REGION}"
QUOTA_ROOT = f"{SCOPE}/providers/Microsoft.Quota"
HOST = "https://management.azure.com"
API_VERSION = "2025-09-01"
QUOTA_URL = f"{HOST}{QUOTA_ROOT}/quotas/{FAMILY}?api-version={API_VERSION}"
USAGE_URL = f"{HOST}{QUOTA_ROOT}/usages/{FAMILY}?api-version={API_VERSION}"
HISTORY_URL = f"{HOST}{QUOTA_ROOT}/quotaRequests?api-version={API_VERSION}&$top=100"
PENDING = {"Accepted", "InProgress"}
FAILED = {"Failed", "Invalid"}
STATES = PENDING | FAILED | {"Succeeded"}
GUID = r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
ERROR_CODES = {
    "AuthorizationFailed", "AuthenticationFailed", "Forbidden", "QuotaNotAvailableForResource",
    "ResourceNotAvailableForOffer", "QuotaReductionNotSupported", "InvalidQuotaRequest",
    "MissingSubscriptionRegistration", "SubscriptionNotRegistered", "RequestDisallowedByPolicy",
    "InvalidResourceName", "QuotaExceeded", "TooManyRequests", "OperationNotAllowed",
}
SENSITIVE_TEXT = re.compile(r"authorization|bearer|token|secret|password|credential|private.key|[?&]sig=|://[^/\s]+@", re.I)


class Blocked(Exception):
    """A fixed, nonsecret reason that cannot authorize a request."""


def require(condition, reason):
    if not condition:
        raise Blocked(reason)


def unsigned(value):
    """Accept JSON integers or canonical unsigned decimal strings, never coercions."""
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    if isinstance(value, str) and re.fullmatch(r"0|[1-9][0-9]*", value):
        try:
            return int(value)
        except ValueError:
            pass
    raise Blocked("invalid_nonnegative_integer")


def same_id(value, expected):
    return isinstance(value, str) and value.lower() == expected.lower()


def provider_code(value):
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{0,127}", value) is not None \
        and (value in ERROR_CODES or SENSITIVE_TEXT.search(value) is None)


def error_details(value):
    result = []
    if isinstance(value, dict):
        code = value.get("code")
        if provider_code(code):
            row = {"code": code}
            message = value.get("message")
            if isinstance(message, str) and len(message) <= 2048:
                row["message"] = "[redacted]" if SENSITIVE_TEXT.search(message) else message
            result.append(row)
        for key in ("error", "details", "innererror"):
            if key in value:
                result.extend(error_details(value[key]))
    elif isinstance(value, list):
        for row in value[:100]:
            result.extend(error_details(row))
    return result


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def timestamp(value):
    require(isinstance(value, str), "invalid_timestamp")
    try:
        return recovery.timestamp(value, "quota evidence")
    except recovery.workers.ReconcileError as error:
        raise Blocked("invalid_timestamp") from error


def save_json(path, value, *, exclusive=False):
    """Fsync the pre-send record before any PUT; never truncate an existing journal."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    target = path if exclusive else path.with_name(f".{path.name}.{uuid.uuid4().hex}.write")
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        if not exclusive:
            os.replace(target, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if not exclusive and target.exists():
            target.unlink()


def unique_keys(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate_evidence_key")
        result[key] = value
    return result


def load_json(path, *, with_digest=False):
    with Path(path).open("rb") as stream:
        content = stream.read(1024 * 1024 + 1)
    require(len(content) <= 1024 * 1024, "local_evidence_too_large")
    try:
        value = json.loads(content, object_pairs_hook=unique_keys)
    except (ValueError, UnicodeError) as error:
        raise Blocked("invalid_local_evidence") from error
    require(isinstance(value, dict), "invalid_local_evidence")
    return (value, hashlib.sha256(content).hexdigest()) if with_digest else value


def validate_checkpoint(data):
    original = data.get("original_identity") or {}
    replacement = data.get("replacement") or {}
    deletion = replacement.get("delete") or {}
    removal = replacement.get("native_removal") or {}
    observation = replacement.get("removal_observation") or {}
    restore = replacement.get("restore") or {}
    require(
        data.get("execute") is True and data.get("mutation_started") is True
        and data.get("success") is False and data.get("status") == "failed"
        and data.get("plan_sha256") == PLAN_SHA256
        and original.get("vm_id") == VM_ID and original.get("node_uid") == NODE_UID
        and original.get("node_name") == f"{PROM_VMSS}000000"
        and original.get("instance_id") == "0"
        and same_id(original.get("provider_id"), PROVIDER_ID),
        "native_checkpoint_identity_mismatch",
    )
    require(
        deletion.get("attempted") is True and deletion.get("accepted") is True
        and deletion.get("ambiguous") is False
        and replacement.get("automatic_retry_allowed") is False
        and replacement.get("replacement_completed") is False
        and removal.get("old_node_pods_nnc_absent") is True
        and removal.get("manual_marker_clearance") is False
        and removal.get("original_marker_removed_by") == "native-node-removal"
        and unsigned(removal.get("pool_count")) == unsigned(removal.get("vmss_capacity")) == 0
        and observation.get("arm_empty") is True and observation.get("old_resources_absent") is True
        and unsigned(observation.get("pool_count")) == 0,
        "native_zero_proof_missing",
    )
    require(
        restore.get("attempted") is True and restore.get("accepted") is None
        and restore.get("ambiguous") is True
        and isinstance(data.get("error"), str)
        and "ErrCode_InsufficientVCPUQuota" in data["error"],
        "native_restore_quota_failure_missing",
    )
    require(
        timestamp(deletion.get("requested_at")) <= timestamp(deletion.get("returned_at"))
        <= timestamp(removal.get("verified_at")) <= timestamp(restore.get("requested_at"))
        <= timestamp(restore.get("returned_at")) <= datetime.now(timezone.utc),
        "native_checkpoint_order_invalid",
    )


def validate_url(url, *, poll=False, history=False):
    """Reject redirects, credentials, arbitrary paths and alternate ARM scopes."""
    require(isinstance(url, str) and len(url) <= 8192
            and all(32 < ord(char) < 127 for char in url) and "\\" not in url, "unsafe_response_url")
    parsed = urlsplit(url)
    require(parsed.scheme == "https" and parsed.netloc.lower() == "management.azure.com"
            and not parsed.fragment, "unsafe_response_url")
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    query = dict(pairs)
    require(len(pairs) == len(query)
            and re.fullmatch(r"20[0-9]{2}-[0-9]{2}-[0-9]{2}", query.get("api-version", "")),
            "unsafe_response_url")
    base = QUOTA_ROOT.lower()
    path = parsed.path.lower()
    is_history = path == f"{base}/quotarequests"
    is_poll = bool(re.fullmatch(f"{re.escape(base)}/(?:quotarequests|operationsstatus)/{GUID}", path))
    require(
        (is_history if history else is_poll if poll else
         is_history or is_poll or url in {QUOTA_URL, USAGE_URL})
        and set(query) <= ({"api-version", "$top", "$skiptoken"} if is_history else {"api-version"}),
        "unsafe_response_url",
    )
    return url


@dataclass
class Reply:
    status: int
    payload: object
    headers: dict


class ArmClient:
    """Use only the pipeline's existing CLI login, with no HTTP retries or redirects."""

    def __init__(self):
        # These are existing requirements; keep imports lazy for offline testing.
        from azure.identity import AzureCliCredential  # pylint: disable=import-outside-toplevel
        import requests  # pylint: disable=import-outside-toplevel

        self.credential_type = AzureCliCredential
        self.session = requests.Session()
        self.session.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))

    def request(self, method, url, *, timeout, body=None, correlation=None):
        validate_url(url)
        require(method in {"GET", "PUT"} and (method != "PUT" or (
            url == QUOTA_URL and isinstance(body, dict) and set(body) == {"properties"}
            and body["properties"].get("name") == {"value": FAMILY}
            and body["properties"].get("limit") == {"limitObjectType": "LimitValue", "value": DESIRED_LIMIT}
            and set(body["properties"]) <= {"name", "limit", "unit", "resourceType"}
            and body["properties"].get("unit", "Count") == "Count"
            and body["properties"].get("resourceType", "dedicated") == "dedicated"
        )), "client_write_not_authorized")
        started = time.monotonic()
        token = self.credential_type(process_timeout=min(30, timeout)).get_token(
            "https://management.azure.com/.default"
        )
        timeout -= time.monotonic() - started
        require(timeout > 0, "observation_timeout")
        headers = {"Authorization": f"Bearer {token.token}"}
        if correlation:
            headers["x-ms-client-request-id"] = correlation
        response = self.session.request(
            method, url, headers=headers, json=body, timeout=timeout, allow_redirects=False,
        )
        try:
            payload = response.json()
        except ValueError:
            payload = None
        return Reply(response.status_code, payload, {
            key.lower(): response.headers[key]
            for key in ("Location", "Retry-After", "x-ms-request-id") if key in response.headers
        })


def run_cli(command, timeout):
    result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=timeout)
    if result.returncode:
        auth = any(code in result.stderr for code in ("AuthorizationFailed", "Forbidden", "AADSTS"))
        raise Blocked("authorization_unavailable" if auth else "azure_cli_read_failed")
    try:
        return json.loads(result.stdout)
    except ValueError as error:
        raise Blocked("invalid_azure_cli_response") from error


def resource_properties(payload, kind):
    require(isinstance(payload, dict)
            and same_id(payload.get("id"), f"{QUOTA_ROOT}/{kind}/{FAMILY}")
            and payload.get("name") == FAMILY
            and same_id(payload.get("type"), f"Microsoft.Quota/{kind}"), "quota_resource_scope_mismatch")
    props = payload.get("properties")
    require(isinstance(props, dict) and (props.get("name") or {}).get("value") == FAMILY,
            "quota_resource_name_mismatch")
    require(props.get("unit", "Count") == "Count"
            and props.get("resourceType", "dedicated") == "dedicated"
            and props.get("isQuotaApplicable", True) is True, "quota_unit_or_type_invalid")
    return props


def quota_limit(props):
    limit = props.get("limit") or {}
    require(isinstance(limit, dict) and limit.get("limitObjectType") == "LimitValue",
            "quota_limit_type_invalid")
    return unsigned(limit.get("value"))


def history_rows(payload):
    require(isinstance(payload, dict) and isinstance(payload.get("value"), list), "invalid_quota_history")
    records = []
    for row in payload["value"]:
        require(isinstance(row, dict) and isinstance(row.get("name"), str)
                and re.fullmatch(GUID, row["name"])
                and same_id(row.get("id"), f"{QUOTA_ROOT}/quotaRequests/{row['name']}")
                and same_id(row.get("type"), "Microsoft.Quota/quotaRequests"), "quota_history_scope_mismatch")
        props = row.get("properties") or {}
        require(isinstance(props.get("value"), list) and props["value"], "invalid_quota_history")
        for item in props["value"]:
            require(isinstance(item, dict) and isinstance(item.get("name"), dict)
                    and isinstance(item["name"].get("value"), str), "invalid_quota_history")
            if item["name"]["value"] != FAMILY:
                continue
            state, parent = item.get("provisioningState"), props.get("provisioningState")
            require(state in STATES and parent in STATES, "unknown_quota_request_state")
            require(item.get("unit", "Count") == "Count"
                    and item.get("resourceType", "dedicated") == "dedicated", "quota_history_unit_invalid")
            submitted = timestamp(props.get("requestSubmitTime"))
            require(submitted <= datetime.now(timezone.utc) + timedelta(seconds=30), "invalid_quota_history_time")
            # A batch is not completed while its parent is still pending or failed.
            effective = parent if parent in FAILED else state if state in FAILED else (
                parent if parent in PENDING else state
            )
            records.append({
                "id": row["id"], "name": row["name"].lower(), "requested_limit": quota_limit(item),
                "state": effective, "subrequest_state": state,
                "submitted_at": submitted.isoformat(),
            })
    return records


class Request:
    """One bounded, scope-pinned request and read-only observation."""

    def __init__(self, args, summary, runner, client, clock, sleep):
        self.args, self.summary, self.runner, self.client = args, summary, runner, client
        self.clock, self.sleep = clock, sleep
        self.deadline = clock() + args.timeout_seconds
        self.journal = Path(args.native_checkpoint).with_name("request_mesh96_quota.attempt.json")
        self.owns_journal = False
        self.can_replace_summary = False
        self.delay = 30

    def remaining(self):
        left = self.deadline - self.clock()
        require(left > 0, "observation_timeout")
        return min(45, left)

    def save(self):
        save_json(self.args.summary_file, self.summary)

    def journal_data(self):
        return {
            "schema_version": 1, "request_scope": SCOPE, "resource_name": FAMILY,
            "desired_limit": DESIRED_LIMIT,
            "native_checkpoint_sha256": self.summary["native_checkpoint_sha256"],
            "request": self.summary["request"],
        }

    def save_receipt(self, *, exclusive=False):
        save_json(self.journal, self.journal_data(), exclusive=exclusive)
        self.save()

    def az(self, *parts):
        return self.runner(
            ["az", *parts, "--subscription", SUBSCRIPTION, "--only-show-errors", "--output", "json"],
            self.remaining(),
        )

    def http(self, method, url, **kwargs):
        validate_url(url)
        if self.client is None:
            self.client = ArmClient()
        return self.client.request(method, url, timeout=self.remaining(), **kwargs)

    def get(self, url):
        reply = self.http("GET", url)
        require(reply.status == 200, "authorization_unavailable" if reply.status in {401, 403}
                else "quota_read_failed")
        return reply.payload

    def scope(self):
        account = self.az("account", "show", "--query", "{id:id,state:state}")
        require(isinstance(account, dict) and account.get("id") == SUBSCRIPTION and account.get("state") == "Enabled",
                "account_scope_mismatch")
        group = self.az("group", "show", "--name", RESOURCE_GROUP)
        require(isinstance(group, dict), "resource_group_ownership_mismatch")
        tags = group.get("tags") or {}
        require(same_id(group.get("id"), GROUP_ID) and group.get("location") == REGION
                and tags.get("run_id") == RESOURCE_GROUP
                and tags.get("clustermesh_debug_preserved") == "true", "resource_group_ownership_mismatch")
        lease = timestamp(tags.get("deletion_due_time"))
        require((lease - datetime.now(timezone.utc)).total_seconds() > self.args.timeout_seconds + 300,
                "preserved_lease_too_short")
        cluster = self.az("aks", "show", "--resource-group", RESOURCE_GROUP, "--name", CLUSTER,
                          "--query", "{id:id,name:name,location:location,nodeResourceGroup:nodeResourceGroup,tags:tags}")
        require(isinstance(cluster, dict) and same_id(cluster.get("id"), CLUSTER_ID) and cluster.get("name") == CLUSTER
                and cluster.get("location") == REGION
                and same_id(cluster.get("nodeResourceGroup"), NODE_GROUP)
                and (cluster.get("tags") or {}).get("role") == "mesh-96"
                and (cluster.get("tags") or {}).get("run_id") == RESOURCE_GROUP, "cluster_scope_mismatch")
        pools = self.az("aks", "nodepool", "list", "--resource-group", RESOURCE_GROUP, "--cluster-name", CLUSTER)
        require(isinstance(pools, list) and len(pools) == 2
                and {row.get("name") for row in pools} == {"default", "prompool"}, "pool_inventory_changed")
        for pool in pools:
            name = pool["name"]
            require(same_id(pool.get("id"), f"{CLUSTER_ID}/agentPools/{name}")
                    and unsigned(pool.get("count")) == (0 if name == "prompool" else 2)
                    and pool.get("mode") == ("User" if name == "prompool" else "System")
                    and pool.get("provisioningState") == "Succeeded"
                    and (pool.get("powerState") or {}).get("code") == "Running"
                    and pool.get("vmSize") == "Standard_D8_v3"
                    and pool.get("enableAutoScaling") is False, "native_pool_state_changed")
        vmss = self.az("vmss", "show", "--resource-group", NODE_GROUP, "--name", PROM_VMSS,
                       "--query", "{id:id,sku:sku,provisioningState:provisioningState}")
        require(same_id(vmss.get("id"), VMSS_ID) and unsigned((vmss.get("sku") or {}).get("capacity")) == 0
                and vmss.get("provisioningState") == "Succeeded", "native_vmss_not_zero")
        require(self.az("vmss", "list-instances", "--resource-group", NODE_GROUP, "--name", PROM_VMSS,
                        "--query", "[].{id:id,vmId:vmId}") == [], "native_vmss_instances_present")
        provider = self.az("provider", "show", "--namespace", "Microsoft.Quota",
                           "--query", "{namespace:namespace,registrationState:registrationState}")
        require(provider.get("namespace") == "Microsoft.Quota", "provider_scope_mismatch")
        state = provider.get("registrationState")
        self.summary["provider"] = {
            "namespace": "Microsoft.Quota",
            "registration_state": state if state in {"Registered", "NotRegistered", "Registering",
                                                     "Unregistered", "Unregistering"} else "Unknown",
        }
        require(state == "Registered", "provider_not_registered")
        self.summary["scope_verified"] = {"cluster_id": CLUSTER_ID, "node_resource_group": NODE_GROUP}

    def current(self):
        props = resource_properties(self.get(QUOTA_URL), "quotas")
        limit = quota_limit(props)
        usage = resource_properties(self.get(USAGE_URL), "usages")
        quota_used = unsigned((usage.get("usages") or {}).get("value"))
        rows = self.az(
            "vm", "list-usage", "--location", REGION, "--query",
            "[?name.value=='standardDv3Family' || name.value=='cores']."
            "{name:name.value,currentValue:currentValue,limit:limit,unit:unit}",
        )
        require(isinstance(rows, list) and len(rows) == 2
                and {row.get("name") for row in rows} == {FAMILY, "cores"}, "compute_usage_missing")
        counters = {}
        for row in rows:
            require(row.get("unit") == "Count", "compute_usage_unit_invalid")
            used, maximum = unsigned(row.get("currentValue")), unsigned(row.get("limit"))
            counters[row["name"]] = {"used": used, "limit": maximum, "remaining": maximum - used}
        family = counters[FAMILY]
        used = max(quota_used, family["used"])
        evidence = {
            "quota_api_limit": limit, "quota_api_used": quota_used,
            "compute": counters, "family_limit": min(limit, family["limit"]), "family_used": used,
            "family_remaining": min(limit, family["limit"]) - used,
            "regional_remaining": counters["cores"]["remaining"],
            "proposed_remaining": DESIRED_LIMIT - used, "observed_at": utc_now(),
        }
        self.summary["quota"] = evidence
        self.summary["capacity_available"] = (
            evidence["family_remaining"] >= REQUIRED_VCPUS and evidence["regional_remaining"] >= REQUIRED_VCPUS
        )
        body = {"properties": {"name": {"value": FAMILY},
                               "limit": {"limitObjectType": "LimitValue", "value": DESIRED_LIMIT}}}
        for key in ("unit", "resourceType"):
            if key in props:
                body["properties"][key] = props[key]
        self.summary["proposed_body"] = body
        return evidence

    def history(self):
        url, records, seen = HISTORY_URL, [], set()
        for _ in range(20):
            require(url not in seen, "quota_history_pagination_cycle")
            seen.add(url)
            payload = self.get(validate_url(url, history=True))
            records.extend(history_rows(payload))
            url = payload.get("nextLink")
            if not url:
                self.summary["existing_requests"] = records
                return records
        raise Blocked("quota_history_incomplete")

    def reason(self, evidence):
        if evidence["regional_remaining"] < REQUIRED_VCPUS:
            return "regional_headroom_insufficient"
        if self.summary["capacity_available"]:
            return None
        if evidence["proposed_remaining"] < REQUIRED_VCPUS:
            return "fixed_ceiling_insufficient"
        if max(evidence["quota_api_limit"], evidence["compute"][FAMILY]["limit"]) >= DESIRED_LIMIT:
            return "quota_not_effective"
        return "needs_request"

    def existing(self, records):
        recent = datetime.now(timezone.utc) - timedelta(days=30)
        relevant = [
            row for row in records if row["state"] in PENDING or (
                row["requested_limit"] >= DESIRED_LIMIT and timestamp(row["submitted_at"]) >= recent
            )
        ]
        if relevant:
            return max(relevant, key=lambda row: (row["state"] in PENDING, timestamp(row["submitted_at"])))
        return None

    def result(self, status, reason=None, *, success=False):
        self.summary.update(status=status, success=success, blocked_reason=reason)
        self.save()

    def observe(self):
        for _ in range(120):
            evidence, records = self.current(), self.history()
            receipt = self.summary["request"]
            matched = [row for row in records if row["name"] == receipt.get("request_id")]
            require(len(matched) <= 1, "quota_request_history_ambiguous")
            existing = (matched[0] if matched else None) if receipt.get("request_id") else self.existing(records)
            if matched:
                require(matched[0]["requested_limit"] == DESIRED_LIMIT, "quota_request_response_mismatch")
            if existing:
                self.summary["observed_request"] = existing
            state = existing["state"] if existing else receipt.get("state")
            if receipt["ambiguous"]:
                self.result("request_ambiguous", "previous_attempt_requires_read_only_reconciliation")
                return
            if state in FAILED or receipt.get("state") in FAILED:
                self.result("request_failed", "quota_request_failed")
                return
            reason = self.reason(evidence)
            if reason is None:
                self.result("quota_available", success=True)
                return
            if reason not in {"needs_request", "quota_not_effective"}:
                self.result("quota_not_usable", reason)
                return
            status = "approved_not_effective" if state == "Succeeded" else "request_pending"
            self.result(status, "approved_limit_not_effective" if state == "Succeeded" else "quota_request_pending")
            left = self.deadline - self.clock()
            if not self.args.execute or left <= self.delay:
                return
            self.sleep(self.delay)
            location = receipt.get("location")
            if location:
                reply = self.http("GET", validate_url(location, poll=True))
                require(reply.status in {200, 202}, "quota_request_observation_failed")
                if "/quotarequests/" in urlsplit(location).path.lower() and reply.status == 200:
                    rows = history_rows({"value": [reply.payload]})
                    require(len(rows) == 1 and rows[0]["name"] == receipt["request_id"]
                            and rows[0]["requested_limit"] == DESIRED_LIMIT,
                            "quota_request_response_mismatch")
                    receipt["state"] = rows[0]["state"]
                elif isinstance(reply.payload, dict):
                    # Operation completion alone is NOT quota approval or usable headroom.
                    operation_state = reply.payload.get("status")
                    if operation_state in {"Failed", "Canceled"}:
                        receipt["state"] = "Failed"
                self.retry_after(reply)
                if self.owns_journal:
                    self.save_receipt()

    def retry_after(self, reply):
        if "retry-after" in reply.headers:
            self.delay = max(5, unsigned(reply.headers["retry-after"]))

    def inspect_rejection(self):
        receipt = self.summary["request"]
        require(not self.args.execute and receipt["attempted"] and receipt["accepted"] is False
                and receipt["ambiguous"] is False and receipt.get("state") in FAILED,
                "inspection_requires_definitely_rejected_journal")
        attempted = timestamp(receipt["attempted_at"])
        start = attempted - timedelta(minutes=2)
        end = min(datetime.now(timezone.utc), attempted + timedelta(minutes=15))
        rows = self.az(
            "monitor", "activity-log", "list", "--resource-id", f"{QUOTA_ROOT}/quotas/{FAMILY}",
            "--start-time", start.isoformat(), "--end-time", end.isoformat(), "--max-events", "100",
            "--query", "[].{eventDataId:eventDataId,eventTimestamp:eventTimestamp,correlationId:correlationId,"
            "resourceId:resourceId,operation:operationName.value,status:status.value,statusMessage:properties.statusMessage}",
        )
        require(isinstance(rows, list) and all(isinstance(row, dict) for row in rows), "invalid_activity_log")
        observations = []
        for row in rows:
            require(same_id(row.get("resourceId"), f"{QUOTA_ROOT}/quotas/{FAMILY}"),
                    "activity_log_scope_mismatch")
            event_time = timestamp(row.get("eventTimestamp"))
            require(start <= event_time <= end, "activity_log_time_mismatch")
            raw = row.get("statusMessage")
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except ValueError:
                    raw = None
            observations.append({
                "at": event_time.isoformat(),
                "status": row.get("status") if row.get("status") in {"Started", "Accepted", "Succeeded", "Failed"} else "Unknown",
                "event_id": row.get("eventDataId") if re.fullmatch(GUID, str(row.get("eventDataId", ""))) else None,
                "correlation_id": row.get("correlationId") if re.fullmatch(GUID, str(row.get("correlationId", ""))) else None,
                "errors": error_details(raw),
            })
        self.summary["rejection_activity"] = observations
        self.summary["inspection_only"] = True
        self.result("rejection_inspected", success=True)

    def submit(self):
        receipt = self.summary["request"]
        receipt.update(attempted=True, accepted=None, ambiguous=True, attempted_at=utc_now(),
                       correlation_id=str(uuid.uuid4()))
        self.summary["mutation_started"] = True
        self.save_receipt(exclusive=True)
        self.owns_journal = True
        reply = self.http("PUT", QUOTA_URL, body=self.summary["proposed_body"],
                          correlation=receipt["correlation_id"])
        receipt["http_status"] = reply.status
        request_id = reply.headers.get("x-ms-request-id")
        if isinstance(request_id, str) and re.fullmatch(GUID, request_id):
            receipt["provider_request_id"] = request_id
        if isinstance(reply.payload, dict) and isinstance(reply.payload.get("error"), dict):
            code = reply.payload["error"].get("code")
            if provider_code(code):
                receipt["provider_error_code"] = code
        if reply.status not in {200, 202}:
            if reply.status in {400, 401, 403, 404, 409, 422}:
                receipt.update(accepted=False, ambiguous=False, state="Failed")
            self.save_receipt()
            self.result("request_rejected" if not receipt["ambiguous"] else "request_ambiguous",
                        "authorization_unavailable" if reply.status in {401, 403} else "quota_request_not_accepted")
            return
        receipt.update(accepted=True, state="Accepted")
        location = reply.headers.get("location")
        if location:
            receipt["location"] = validate_url(location, poll=True)
            receipt["request_id"] = urlsplit(location).path.rsplit("/", 1)[-1].lower()
        require(reply.status != 202 or location, "accepted_without_safe_location")
        if reply.status == 200:
            receipt["response_limit"] = quota_limit(resource_properties(reply.payload, "quotas"))
        self.retry_after(reply)
        receipt["ambiguous"] = False
        self.save_receipt()
        self.observe()

    def execute(self):
        args = self.args
        require(args.resource_group == args.confirm_resource_group == RESOURCE_GROUP
                and args.expected_subscription == SUBSCRIPTION and args.expected_region == REGION,
                "arguments_scope_mismatch")
        require(isinstance(args.timeout_seconds, int) and not isinstance(args.timeout_seconds, bool)
                and 0 < args.timeout_seconds <= 1800,
                "timeout_out_of_bounds")
        require(not getattr(args, "inspect_rejection", False) or not args.execute,
                "rejection_inspection_is_read_only")
        checkpoint, output = Path(args.native_checkpoint), Path(args.summary_file)
        require(not output.is_symlink() and not checkpoint.is_symlink() and not self.journal.is_symlink()
                and len({checkpoint.resolve(), output.resolve(), self.journal.resolve()}) == 3,
                "unsafe_local_paths")
        data, checkpoint_sha = load_json(checkpoint, with_digest=True)
        validate_checkpoint(data)
        self.summary["native_checkpoint_sha256"] = checkpoint_sha
        self.summary["plan_sha256"] = PLAN_SHA256
        self.summary["attempt_journal"] = str(self.journal)
        if output.exists():
            prior = load_json(output)
            require(prior.get("native_checkpoint_sha256") in {None, self.summary["native_checkpoint_sha256"]},
                    "checkpoint_changed_since_plan")
            if (prior.get("request") or {}).get("attempted") and not self.journal.exists():
                self.summary["request"].update(attempted=True, accepted=None, ambiguous=True)
                self.can_replace_summary = True
                raise Blocked("previous_attempt_journal_missing")
        if self.journal.exists():
            prior = load_json(self.journal)
            require(all(prior.get(key) == value for key, value in self.journal_data().items() if key != "request"),
                    "attempt_journal_scope_mismatch")
            receipt = prior.get("request") or {}
            require(receipt.get("attempted") is True and isinstance(receipt.get("ambiguous"), bool)
                    and (receipt.get("accepted") is None or isinstance(receipt.get("accepted"), bool))
                    and re.fullmatch(GUID, receipt.get("correlation_id", "")), "invalid_attempt_journal")
            self.summary["request"] = {
                key: receipt[key] for key in ("attempted", "accepted", "ambiguous", "correlation_id")
            }
            self.summary["request"]["attempted_at"] = timestamp(receipt.get("attempted_at")).isoformat()
            if receipt.get("location"):
                location = validate_url(receipt["location"], poll=True)
                self.summary["request"].update(
                    location=location, request_id=urlsplit(location).path.rsplit("/", 1)[-1].lower(),
                )
            if receipt.get("state") in STATES:
                self.summary["request"]["state"] = receipt["state"]
            for key in ("http_status", "response_limit"):
                if key in receipt:
                    self.summary["request"][key] = unsigned(receipt[key])
            if provider_code(receipt.get("provider_error_code")):
                self.summary["request"]["provider_error_code"] = receipt["provider_error_code"]
            provider_id = receipt.get("provider_request_id")
            if isinstance(provider_id, str) and re.fullmatch(GUID, provider_id):
                self.summary["request"]["provider_request_id"] = provider_id
            self.owns_journal = True
        self.can_replace_summary = True
        self.scope()
        evidence, records = self.current(), self.history()
        if getattr(args, "inspect_rejection", False):
            self.inspect_rejection()
            return
        if self.summary["request"]["attempted"]:
            self.observe()
            return
        for fresh in (False, True):
            if fresh:
                evidence, records = self.current(), self.history()
            existing, reason = self.existing(records), self.reason(evidence)
            if existing:
                self.summary["observed_request"] = existing
                self.observe()
                return
            if reason is None:
                self.result("already_sufficient", success=True)
                return
            if reason != "needs_request":
                self.result("blocked", reason)
                return
            if not args.execute:
                self.summary["plan_valid"] = True
                self.result("planned", success=True)
                return
        self.submit()


def run(args, *, runner=run_cli, client=None, clock=time.monotonic, sleep=time.sleep):
    summary = {
        "schema_version": 1, "execute": args.execute, "mutation_started": False,
        "request": {"attempted": False, "accepted": False, "ambiguous": False},
        "request_scope": SCOPE, "resource_name": FAMILY, "desired_limit": DESIRED_LIMIT,
        "required_vcpus": REQUIRED_VCPUS, "automatic_retry_allowed": False,
        "success": False, "status": "blocked", "blocked_reason": None,
        "plan_valid": False, "capacity_available": False, "started_at": utc_now(),
    }
    operation = Request(args, summary, runner, client, clock, sleep)
    try:
        operation.execute()
    except (Blocked, OSError, ValueError, TypeError, KeyError, subprocess.TimeoutExpired,
            AzureError, RequestException) as error:
        # Expected client/evidence failures are sanitized; programming errors propagate.
        reason = str(error) if isinstance(error, Blocked) else "unreadable_or_unavailable_evidence"
        ambiguous = summary["request"]["ambiguous"]
        summary.update(success=False, status="request_ambiguous" if ambiguous else "blocked", blocked_reason=reason)
        if operation.owns_journal:
            operation.save_receipt()
    finally:
        summary["finished_at"] = utc_now()
        # Refuse to overwrite the evidence input even when argument validation failed.
        if (operation.can_replace_summary or not Path(args.summary_file).exists()) and Path(args.summary_file).resolve() not in {
            Path(args.native_checkpoint).resolve(), operation.journal.resolve()
        } and not Path(args.summary_file).is_symlink():
            operation.save()
    return summary


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resource-group", required=True)
    parser.add_argument("--confirm-resource-group", required=True)
    parser.add_argument("--expected-subscription", required=True)
    parser.add_argument("--expected-region", required=True)
    parser.add_argument("--native-checkpoint", required=True)
    parser.add_argument("--summary-file", required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--inspect-rejection", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    summary = run(parse_args(argv))
    print(json.dumps({key: summary[key] for key in ("status", "success", "blocked_reason")}))
    return 0 if summary["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
