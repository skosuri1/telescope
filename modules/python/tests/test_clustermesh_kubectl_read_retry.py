"""Bounded preflight retries must preserve strict readiness and clean JSON."""

import json
import os
import stat
import subprocess
import textwrap
import time
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "steps/topology/clustermesh-scale/kubectl-read-retry.sh"
VALIDATE = SCRIPT.with_name("validate-resources.yml")


def _run(tmp_path, outcomes, *, command=None, budget=10, attempts=5, pause=0):
    executable = tmp_path / "kubectl"
    executable.write_text(textwrap.dedent("""\
        #!/usr/bin/env python3
        import json
        import os
        import sys
        import time
        from pathlib import Path

        log = Path(os.environ["CALLS"])
        calls = log.read_text().splitlines() if log.exists() else []
        with log.open("a") as handle:
            handle.write(json.dumps(sys.argv[1:]) + "\\n")
        outcomes = json.loads(os.environ["OUTCOMES"])
        result = outcomes[min(len(calls), len(outcomes) - 1)]
        time.sleep(result.get("sleep", 0))
        print(result.get("stdout", ""), end="")
        print(result.get("stderr", ""), end="", file=sys.stderr)
        sys.exit(result.get("rc", 0))
        """), encoding="utf-8")
    executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
    log = tmp_path / "calls.jsonl"
    environment = {
        **os.environ,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "CALLS": str(log),
        "OUTCOMES": json.dumps(outcomes),
        "CLUSTERMESH_KUBECTL_READ_ATTEMPTS": str(attempts),
        "CLUSTERMESH_KUBECTL_READ_RETRY_SECONDS": str(pause),
        "CLUSTERMESH_KUBECTL_READ_REQUEST_TIMEOUT_SECONDS": "30",
        "CLUSTERMESH_KUBECTL_READ_COMMAND_TIMEOUT_SECONDS": "45",
    }
    started = time.monotonic()
    result = subprocess.run(
        ["bash", str(SCRIPT), str(budget), *(command or ["get", "pods", "-o", "json"])],
        capture_output=True, text=True, check=False, env=environment, timeout=10,
    )
    elapsed = time.monotonic() - started
    calls = [
        json.loads(line)
        for line in (log.read_text(encoding="utf-8").splitlines() if log.exists() else [])
    ]
    return result, calls, elapsed


@pytest.mark.parametrize("message", [
    "The connection to the server example:443 was refused - did you specify the right host or port?",
    "Unable to connect to the server: dial tcp: connection refused",
    "read: connection reset by peer",
    "Unable to connect to the server: i/o timeout",
    "net/http: TLS handshake timeout",
    "http2: client connection lost",
    "Error from server (ServiceUnavailable): the server is currently unable to handle the request",
    "Error from server (TooManyRequests): too many requests",
    "Unable to connect to the server: EOF",
])
def test_transient_read_recovers_without_polluting_json(tmp_path, message):
    result, calls, _ = _run(tmp_path, [
        {"rc": 1, "stdout": "discard this partial response", "stderr": message},
        {"stdout": '{"items":[]}'},
    ])

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"items": []}
    assert len(calls) == 2
    assert "transient API error" in result.stderr


@pytest.mark.parametrize("message", [
    'Error from server (Forbidden): daemonsets.apps "cilium" is forbidden',
    "Unauthorized: token invalid; earlier connection refused",
    "Unable to connect to the server: x509: certificate has expired",
    'Error from server (NotFound): daemonsets.apps "cilium" not found',
    "error: timed out waiting for the condition",
    "error: deployment exceeded its progress deadline",
    "error: unknown flag: --wrong",
    "unclassified error",
])
def test_structural_and_readiness_errors_do_not_retry(tmp_path, message):
    result, calls, _ = _run(
        tmp_path, [{"rc": 1, "stderr": message}],
        command=["rollout", "status", "ds/cilium", "-n", "kube-system"],
    )

    assert result.returncode == 1
    assert len(calls) == 1
    assert "transient API error" not in result.stderr


def test_rollout_retries_use_only_the_remaining_deadline(tmp_path):
    result, calls, _ = _run(tmp_path, [
        {"rc": 1, "stderr": "connection refused", "sleep": 1.1},
        {"stdout": 'daemon set "cilium" successfully rolled out\n'},
    ], command=["rollout", "status", "ds/cilium"], budget=6)

    assert result.returncode == 0, result.stderr
    assert len(calls) == 2
    limits = [int(call[-1].removeprefix("--timeout=").removesuffix("s")) for call in calls]
    assert 0 < limits[1] < limits[0] <= 6
    assert all(call[0] == f"--request-timeout={limit}s" for call, limit in zip(calls, limits))


def test_rollout_health_failure_after_transport_recovery_is_not_bypassed(tmp_path):
    result, calls, _ = _run(tmp_path, [
        {"rc": 1, "stderr": "connection refused"},
        {"rc": 1, "stderr": "error: timed out waiting for the condition"},
        {"stdout": "must not reach success"},
    ], command=["rollout", "status", "ds/cilium"])

    assert result.returncode == 1
    assert len(calls) == 2
    assert result.stdout == ""


def test_persistent_transport_failure_exhausts_attempts(tmp_path):
    result, calls, _ = _run(
        tmp_path, [{"rc": 1, "stderr": "connection refused"}], attempts=3,
    )

    assert result.returncode == 1
    assert len(calls) == 3
    assert "after 3 attempt(s)" in result.stderr


def test_slow_command_cannot_exceed_shared_deadline_or_return_partial_json(tmp_path):
    result, calls, elapsed = _run(
        tmp_path, [{"sleep": 4, "stdout": '{"items":[]}'}], budget=1,
    )

    assert result.returncode == 124
    assert len(calls) == 1
    assert elapsed < 3
    assert result.stdout == ""
    assert "deadline exhausted" in result.stderr


def test_retry_backoff_is_clamped_to_remaining_deadline(tmp_path):
    result, calls, elapsed = _run(
        tmp_path, [{"rc": 1, "stderr": "connection refused"}], budget=1, pause=10,
    )

    assert result.returncode == 124
    assert len(calls) == 1
    assert elapsed < 3


@pytest.mark.parametrize("command", [["delete", "pods", "--all"], ["rollout", "restart", "ds/cilium"]])
def test_mutations_are_refused(tmp_path, command):
    result, calls, _ = _run(tmp_path, [{"stdout": "must not execute"}], command=command)

    assert result.returncode == 2
    assert calls == []


def test_pipeline_keeps_rollout_and_all_agent_identity_gates():
    document = yaml.safe_load(VALIDATE.read_text(encoding="utf-8"))
    step = next(
        step for step in document["steps"]
        if step.get("displayName") == "Validate Cilium + ClusterMesh on every cluster"
    )
    script = step["script"]

    assert step["env"]["KUBECTL_READ_RETRY"].endswith("/kubectl-read-retry.sh")
    assert '_cm=$(KUBECONFIG="$_kc" bash "$KUBECTL_READ_RETRY" 90' in script
    assert 'bash "$KUBECTL_READ_RETRY" 90 get pods -n kube-system -l k8s-app=cilium' in script
    rollout = 'bash "$KUBECTL_READ_RETRY" 300 rollout status ds/cilium -n kube-system'
    assert rollout in script
    assert f"{rollout} ||" not in script
    assert script.index(rollout) < script.index('python3 "$CILIUM_AGENT_HEALTH_PROBE"')
    assert '--identity-inventory "$cilium_identity_inventory"' in script
    assert '--expected-remote-count "$expected_remote"' in script
    assert 'if [ "$failures" -gt 0 ]; then' in script
