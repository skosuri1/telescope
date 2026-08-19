"""Tests for preserved live ClusterMesh overlay drift detection."""

import importlib.util
import json
import sys
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "clusterloader2"
    / "clustermesh-scale"
    / "preserved_live_overlay.py"
)
MODULE_SPEC = importlib.util.spec_from_file_location(
    "preserved_live_overlay",
    MODULE_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise ImportError(f"Unable to load module from {MODULE_PATH}")
overlay = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = overlay
MODULE_SPEC.loader.exec_module(overlay)


def identity(role, cluster_name, cluster_id):
    return overlay.ClusterIdentity(
        role=role,
        cluster_name=cluster_name,
        cluster_id=cluster_id,
    )


def ready_remote(name):
    return {
        "name": name,
        "ready": True,
        "connected": True,
        "config": {"required": True, "retrieved": True},
    }


def test_missing_concat_named_remote_repairs_target_member():
    identities = {
        item.cluster_name: item
        for item in (
            identity("mesh-21", "mesh-2121", 21),
            identity("mesh-91", "mesh-9191", 91),
            identity("mesh-100", "mesh-100100", 100),
        )
    }
    drift = overlay.analyze_status(
        identities["mesh-2121"],
        [ready_remote("mesh-100100")],
        identities,
    )

    assert drift.missing_remote_names == ["mesh-9191"]
    assert overlay.repair_roles_for_drift([drift], identities) == ["mesh-91"]


def test_remote_with_missing_configuration_repairs_target_member():
    identities = {
        item.cluster_name: item
        for item in (
            identity("mesh-21", "mesh-2121", 21),
            identity("mesh-91", "mesh-9191", 91),
        )
    }
    drift = overlay.analyze_status(
        identities["mesh-2121"],
        [
            {
                "name": "mesh-9191",
                "ready": False,
                "connected": True,
                "config": {"required": True, "retrieved": False},
            }
        ],
        identities,
    )

    assert drift.not_ready_remote_names == ["mesh-9191"]
    assert overlay.repair_roles_for_drift([drift], identities) == ["mesh-91"]


def test_unknown_or_duplicate_projection_repairs_local_member():
    identities = {
        item.cluster_name: item
        for item in (
            identity("mesh-1", "mesh-11", 1),
            identity("mesh-2", "mesh-22", 2),
        )
    }
    drift = overlay.analyze_status(
        identities["mesh-11"],
        [ready_remote("mesh-22"), ready_remote("stale-peer"), ready_remote("stale-peer")],
        identities,
    )

    assert drift.unexpected_remote_names == ["stale-peer"]
    assert drift.duplicate_remote_names == ["stale-peer"]
    assert overlay.repair_roles_for_drift([drift], identities) == ["mesh-1"]


def test_command_failure_repairs_local_member():
    identities = {
        "mesh-11": identity("mesh-1", "mesh-11", 1),
        "mesh-22": identity("mesh-2", "mesh-22", 2),
    }
    drift = overlay.ClusterDrift(
        role="mesh-2",
        cluster_name="mesh-22",
        command_error="kubectl exec timed out",
    )

    assert overlay.repair_roles_for_drift([drift], identities) == ["mesh-2"]


def test_remote_readiness_requires_retrieved_configuration():
    assert overlay.remote_is_ready(
        {
            "ready": True,
            "connected": True,
            "config": {"required": True, "retrieved": True},
        }
    )
    assert not overlay.remote_is_ready(
        {
            "ready": True,
            "connected": True,
            "config": {"required": True, "retrieved": False},
        }
    )


def test_load_clusters_rejects_duplicate_roles(tmp_path):
    inventory = tmp_path / "clusters.json"
    inventory.write_text(
        json.dumps(
            [
                {"role": "mesh-1", "name": "a", "rg": "rg"},
                {"role": "mesh-1", "name": "b", "rg": "rg"},
            ]
        ),
        encoding="utf-8",
    )

    try:
        overlay.load_clusters(str(inventory))
    except overlay.ProbeError as exc:
        assert "duplicate cluster role" in str(exc)
    else:
        raise AssertionError("duplicate role should fail")
