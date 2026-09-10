"""Focused tests for bounded post-scenario health recovery."""

import json
import os
import stat
import subprocess
import textwrap
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "clusterloader2"
    / "clustermesh-scale"
    / "config"
    / "scenario-health-recovery.sh"
)


def _write_executable(path: Path, body: str) -> None:
    path.write_text(textwrap.dedent(body), encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _run_recovery(
    tmp_path: Path,
    *,
    worker_rc: int = 0,
    cleanup_rc: int = 0,
    cleanup_budget: int = 2,
    cleanup_exists: bool = True,
):
    clusters = tmp_path / "clusters.json"
    clusters.write_text(
        json.dumps(
            [
                {
                    "role": "mesh-1",
                    "name": "cluster-1",
                    "rg": "run-rg",
                    "kubeconfig": "/fake/mesh-1.config",
                },
                {
                    "role": "mesh-2",
                    "name": "cluster-2",
                    "rg": "run-rg",
                    "kubeconfig": "/fake/mesh-2.config",
                },
            ]
        ),
        encoding="utf-8",
    )
    report_dir = tmp_path / "report"
    calls = tmp_path / "calls.log"
    gate_count = tmp_path / "gate-count"
    gate_count.write_text("0\n", encoding="utf-8")

    health_gate = tmp_path / "health-gate.sh"
    _write_executable(
        health_gate,
        """\
        #!/usr/bin/env bash
        set -euo pipefail
        count=$(cat "$FAKE_GATE_COUNT")
        count=$((count + 1))
        printf '%s\n' "$count" > "$FAKE_GATE_COUNT"
        summary=""
        while [ "$#" -gt 0 ]; do
          if [ "$1" = "--summary-file" ]; then
            summary="$2"
            break
          fi
          shift
        done
        printf 'gate:%s\n' "$count" >> "$FAKE_CALLS"
        if [ "$count" -eq 1 ]; then
          printf '%s\n' '{"success":false,"termination_reason":"cycle-limit","completed_cycle_count":1,"clusters":[{"role":"mesh-1","healthy":true},{"role":"mesh-2","healthy":false}]}' > "$summary"
          exit 3
        fi
        printf '%s\n' '{"success":true,"termination_reason":"healthy"}' > "$summary"
        """,
    )

    worker = tmp_path / "worker.sh"
    _write_executable(
        worker,
        f"""\
        #!/usr/bin/env bash
        set -euo pipefail
        printf 'worker:%s\n' "$1" >> "$FAKE_CALLS"
        printf '%s\n' '{{"healthy":{str(worker_rc == 0).lower()}}}' > "$2"
        exit {worker_rc}
        """,
    )

    mock = tmp_path / "mock.sh"
    _write_executable(
        mock,
        """\
        #!/usr/bin/env bash
        set -euo pipefail
        printf 'mock:%s\n' "$3" >> "$FAKE_CALLS"
        printf '%s\n' '{"success":true}' > "$2"
        """,
    )

    cleanup = tmp_path / "cleanup.py"
    if cleanup_exists:
        _write_executable(
            cleanup,
            """\
            #!/usr/bin/env python3
            import json
            import os
            import sys

            cleanup_rc = int(os.environ["FAKE_CLEANUP_RC"])
            arguments = iter(sys.argv[1:])
            parsed = {}
            for argument in arguments:
                if argument.startswith("--"):
                    parsed[argument] = next(arguments)
            with open(os.environ["FAKE_CALLS"], "a", encoding="utf-8") as handle:
                handle.write(
                    "cleanup:"
                    f"{parsed['--clusters']}:"
                    f"{parsed['--scenario']}:"
                    f"{parsed['--max-concurrent']}\\n"
                )
            with open(parsed["--summary-file"], "w", encoding="utf-8") as handle:
                json.dump({"success": cleanup_rc == 0}, handle)
            raise SystemExit(cleanup_rc)
            """,
        )

    environment = os.environ.copy()
    environment.update(
        {
            "SCENARIO": "pod-churn-combined",
            "HEALTH_GATE_TIMEOUT_SECONDS": "200",
            "HEALTH_GATE_CYCLE_TIMEOUT_SECONDS": "2",
            "HEALTH_GATE_CLUSTER_TIMEOUT_SECONDS": "1",
            "HEALTH_GATE_QUIET_WINDOW_SECONDS": "1",
            "HEALTH_GATE_POLL_INTERVAL_SECONDS": "1",
            "HEALTH_GATE_CONCURRENCY": "2",
            "HEALTH_GATE_COMPLETION_MARGIN_SECONDS": "1",
            "EXPECTED_MOCK_COUNT": "100",
            "EXPECTED_REMOTE_COUNT": "1",
            "CL2_HEALTH_GATE_REPAIR_ENABLED": "true",
            "CL2_HEALTH_GATE_INITIAL_CYCLES": "1",
            "CL2_HEALTH_GATE_MAX_REPAIR_ROLES": "5",
            "CL2_HEALTH_GATE_WORKER_REPAIR_BUDGET_SECONDS": "2",
            "CL2_HEALTH_GATE_CLEANUP_REPAIR_BUDGET_SECONDS": str(cleanup_budget),
            "CL2_MOCK_RECONCILE_BUDGET_SECONDS": "2",
            "CL2_MOCK_MODE": "true",
            "CLUSTERMESH_PRESERVED_WORKER_RECOVERY_ENABLED": "true",
            "HEALTH_GATE_SCRIPT": str(health_gate),
            "SCENARIO_CLEANUP_RECONCILER": str(cleanup),
            "PRESERVED_WORKER_RECONCILE_WRAPPER": str(worker),
            "MOCK_RECONCILE_WRAPPER": str(mock),
            "FAKE_GATE_COUNT": str(gate_count),
            "FAKE_CALLS": str(calls),
            "FAKE_CLEANUP_RC": str(cleanup_rc),
        }
    )
    result = subprocess.run(
        ["bash", str(SCRIPT_PATH), str(clusters), str(report_dir), "2"],
        capture_output=True,
        text=True,
        check=False,
        env=environment,
        timeout=10,
    )
    summary = json.loads(
        (report_dir / "scenario-health-recovery.json").read_text(
            encoding="utf-8"
        )
    )
    return result, summary, report_dir, calls.read_text(encoding="utf-8")


def test_health_recovery_repairs_only_observed_unhealthy_mock_roles(tmp_path):
    result, summary, report_dir, calls = _run_recovery(tmp_path)

    assert result.returncode == 0
    assert summary == {
        "schema_version": 1,
        "success": True,
        "scenario": "pod-churn-combined",
        "health_observation_rc": 3,
        "health_repair_role_count": 1,
        "health_repair_attempted": True,
        "health_repair_valid": True,
        "mock_repair_valid": True,
        "cleanup_repair_attempted": True,
        "cleanup_repair_rc": 0,
        "cleanup_gate_rc": 0,
    }
    repair_inventory = json.loads(
        (report_dir / "scenario-health-repair-clusters.json").read_text(
            encoding="utf-8"
        )
    )
    assert [cluster["role"] for cluster in repair_inventory] == ["mesh-2"]
    assert (
        f"cleanup:{report_dir / 'scenario-health-repair-clusters.json'}:"
        "pod-churn-combined:1"
        in calls
    )
    assert calls.index("cleanup:") < calls.index("worker:") < calls.index("mock:")
    assert calls.count("gate:") == 2


def test_health_recovery_stops_after_unsafe_worker_result(tmp_path):
    result, summary, _, calls = _run_recovery(tmp_path, worker_rc=1)

    assert result.returncode == 1
    assert summary["cleanup_gate_rc"] == 0
    assert summary["health_repair_valid"] is False
    assert summary["mock_repair_valid"] is True
    assert summary["cleanup_repair_attempted"] is True
    assert summary["cleanup_repair_rc"] == 0
    assert "cleanup:" in calls
    assert "worker:" in calls
    assert "mock:" not in calls
    assert calls.count("gate:") == 2


def test_final_gate_can_certify_after_cleanup_repair_command_failure(tmp_path):
    result, summary, _, calls = _run_recovery(tmp_path, cleanup_rc=1)

    assert result.returncode == 0
    assert summary["success"] is True
    assert summary["cleanup_repair_attempted"] is True
    assert summary["cleanup_repair_rc"] == 1
    assert summary["cleanup_gate_rc"] == 0
    assert "cleanup:" in calls
    assert "worker:" in calls
    assert "mock:" in calls


def test_cleanup_repair_budget_shortfall_does_not_block_other_repairs(tmp_path):
    result, summary, _, calls = _run_recovery(tmp_path, cleanup_budget=100)

    assert result.returncode == 0
    assert summary["success"] is True
    assert summary["cleanup_repair_attempted"] is False
    assert summary["cleanup_repair_rc"] == 0
    assert "cleanup:" not in calls
    assert "worker:" in calls
    assert "mock:" in calls


def test_missing_cleanup_reconciler_does_not_abort_final_certification(tmp_path):
    result, summary, _, calls = _run_recovery(tmp_path, cleanup_exists=False)

    assert result.returncode == 0
    assert summary["success"] is True
    assert summary["cleanup_repair_attempted"] is True
    assert summary["cleanup_repair_rc"] == 127
    assert "cleanup:" not in calls
    assert "worker:" in calls
    assert "mock:" in calls
