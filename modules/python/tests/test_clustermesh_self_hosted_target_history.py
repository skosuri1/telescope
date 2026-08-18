"""Historical target coverage tests for the self-hosted telemetry audit."""

import importlib.util
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "clusterloader2"
    / "clustermesh-scale"
    / "telemetry"
    / "audit_self_hosted.py"
)
MODULE_SPEC = importlib.util.spec_from_file_location(
    "clustermesh_self_hosted_target_history",
    MODULE_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise ImportError(f"Unable to load module from {MODULE_PATH}")
audit_module = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(audit_module)


def test_historical_mock_targets_cover_deleted_monitor():
    historical_targets = [
        {
            "labels": {
                "job": "monitoring/mock-cilium-agent",
                "mock_node": f"kwok-node-{index}",
            },
            "health": "up",
        }
        for index in range(100)
    ]
    report = audit_module.build_audit(
        [],
        [],
        expected_mock_agent_targets=100,
        historical_targets=historical_targets,
    )
    check = next(
        item
        for item in report["checks"]
        if item["name"] == "target:mock-cilium-agent"
    )

    assert check["status"] == "covered"
    assert check["target_count"] == 0
    assert check["historical_target_count"] == 100
    assert check["historical_target_evidence"] is True

    live_down = audit_module.build_audit(
        [],
        [
            {
                "labels": {"job": "monitoring/mock-cilium-agent-0"},
                "health": "down",
            }
        ],
        expected_mock_agent_targets=100,
        historical_targets=historical_targets,
    )
    check = next(
        item
        for item in live_down["checks"]
        if item["name"] == "target:mock-cilium-agent"
    )
    assert check["status"] == "missing"


def test_historical_hubble_target_covers_deleted_monitor():
    report = audit_module.build_audit(
        list(audit_module.ACNS_METRICS),
        [],
        require_acns=True,
        historical_targets=[
            {
                "labels": {"job": "monitoring/hubble-metrics-0"},
                "health": "up",
            }
        ],
    )
    check = next(
        item
        for item in report["checks"]
        if item["name"] == "target:acns-hubble"
    )

    assert check["status"] == "covered"
    assert check["target_count"] == 0
    assert check["historical_target_evidence"] is True
    assert report["acns_complete"] is True


def test_historical_mock_targets_group_recreated_pod_series_by_logical_node():
    historical_targets = []
    for index in range(100):
        historical_targets.append(
            {
                "labels": {
                    "job": "monitoring/mock-cilium-agent",
                    "mock_node": f"kwok-node-{index}",
                    "instance": f"10.0.0.{index}:9962",
                },
                "health": "up",
            }
        )
    historical_targets.append(
        {
            "labels": {
                "job": "monitoring/mock-cilium-agent",
                "mock_node": "kwok-node-42",
                "instance": "10.1.0.42:9962",
            },
            "health": "down",
        }
    )

    report = audit_module.build_audit(
        [],
        [],
        expected_mock_agent_targets=100,
        historical_targets=historical_targets,
    )
    check = next(
        item
        for item in report["checks"]
        if item["name"] == "target:mock-cilium-agent"
    )

    assert check["status"] == "covered"
    assert check["historical_target_count"] == 100
    assert check["historical_up_targets"] == 100
    assert check["historical_down_targets"] == 0
    assert check["historical_raw_target_series"] == 101


def test_historical_mock_targets_keep_real_coverage_gap_hard_failure():
    historical_targets = [
        {
            "labels": {
                "job": "monitoring/mock-cilium-agent",
                "mock_node": f"kwok-node-{index}",
            },
            "health": "up",
        }
        for index in range(96)
    ]

    report = audit_module.build_audit(
        [],
        [],
        expected_mock_agent_targets=100,
        historical_targets=historical_targets,
    )
    check = next(
        item
        for item in report["checks"]
        if item["name"] == "target:mock-cilium-agent"
    )

    assert check["status"] == "missing"
    assert check["historical_target_count"] == 96
    assert check["historical_up_targets"] == 96


def test_historical_hubble_stale_down_only_series_are_advisory():
    historical_targets = [
        {
            "labels": {
                "job": "monitoring/hubble-metrics",
                "pod": f"cilium-{index}",
            },
            "health": "up",
        }
        for index in range(30)
    ]
    historical_targets.extend(
        {
            "labels": {
                "job": "monitoring/hubble-metrics",
                "pod": f"retired-cilium-{index}",
            },
            "health": "down",
        }
        for index in range(10)
    )

    report = audit_module.build_audit(
        list(audit_module.ACNS_METRICS),
        [],
        require_acns=True,
        historical_targets=historical_targets,
    )
    check = next(
        item
        for item in report["checks"]
        if item["name"] == "target:acns-hubble"
    )

    assert check["status"] == "covered"
    assert check["historical_up_targets"] == 30
    assert check["historical_down_targets"] == 10
    assert check["historical_stale_down_advisory"] is True


def test_live_hubble_target_wins_over_retired_historical_targets():
    report = audit_module.build_audit(
        list(audit_module.ACNS_METRICS),
        [
            {
                "labels": {"job": "monitoring/hubble-metrics-0"},
                "health": "up",
            }
        ],
        require_acns=True,
        historical_targets=[
            {
                "labels": {"job": "monitoring/hubble-metrics-0"},
                "health": "down",
            }
            for _ in range(3)
        ],
    )
    check = next(
        item
        for item in report["checks"]
        if item["name"] == "target:acns-hubble"
    )

    assert check["status"] == "covered"
    assert check["target_count"] == 1
    assert check["up_targets"] == 1
    assert check["historical_down_targets"] == 1
    assert check["historical_raw_target_series"] == 3
    assert report["acns_complete"] is True
