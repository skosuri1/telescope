"""Tests for authoritative cross-cluster smoke cleanup."""

import json
import os
import stat
import subprocess
import textwrap
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[3]
    / "steps"
    / "topology"
    / "clustermesh-scale"
    / "cross-cluster-smoke.sh"
)


def _run_smoke(tmp_path, mode):
    home = tmp_path / "home"
    kube = home / ".kube"
    kube.mkdir(parents=True)
    (kube / "clustermesh-clusters.json").write_text(
        json.dumps(
            [
                {"role": "mesh-1", "name": "cluster-1", "rg": "rg"},
                {"role": "mesh-2", "name": "cluster-2", "rg": "rg"},
            ]
        ),
        encoding="utf-8",
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    clock = tmp_path / "clock"
    clock.write_text("0\n", encoding="utf-8")
    kubectl_log = tmp_path / "kubectl.log"

    fake_date = fake_bin / "date"
    fake_date.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -euo pipefail
            now=$(cat "$FAKE_CLOCK")
            printf '%s\n' "$now"
            printf '%s\n' "$((now + 1))" > "$FAKE_CLOCK"
            """
        ),
        encoding="utf-8",
    )
    fake_sleep = fake_bin / "sleep"
    fake_sleep.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_kubectl = fake_bin / "kubectl"
    fake_kubectl.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -euo pipefail
            printf '%s\n' "$*" >> "$KUBECTL_LOG"
            args="$*"
            if [[ " $args " == *" apply -f "* ]] ||
               [[ " $args " == *" rollout status deploy/echo "* ]] ||
               [[ " $args " == *" wait --for=condition=Ready pod/curl "* ]] ||
               [[ " $args " == *" delete ns cm-smoke "* ]]; then
              exit 0
            fi
            if [[ " $args " == *" exec curl -- curl "* ]]; then
              echo 'echo-pod'
              exit 0
            fi
            if [[ " $args " == *" get namespace cm-smoke "* ]]; then
              if [ "$FAKE_MODE" = "cleanup-stuck" ]; then
                echo 'namespace/cm-smoke'
                exit 0
              fi
              echo 'Error from server (NotFound): namespaces "cm-smoke" not found' >&2
              exit 1
            fi
            echo "unexpected kubectl command: $args" >&2
            exit 1
            """
        ),
        encoding="utf-8",
    )
    for tool in (fake_date, fake_sleep, fake_kubectl):
        tool.chmod(tool.stat().st_mode | stat.S_IXUSR)

    env = os.environ.copy()
    env.update(
        {
            "HOME": str(home),
            "PATH": f"{fake_bin}:{env['PATH']}",
            "FAKE_CLOCK": str(clock),
            "FAKE_MODE": mode,
            "KUBECTL_LOG": str(kubectl_log),
            "CLUSTERMESH_CROSS_CLUSTER_CLEANUP_TIMEOUT_SECONDS": "2",
        }
    )
    result = subprocess.run(
        ["bash", str(SCRIPT_PATH)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
        timeout=10,
    )
    return result, kubectl_log.read_text(encoding="utf-8")


def test_cross_cluster_smoke_requires_namespace_absence(tmp_path):
    result, kubectl_log = _run_smoke(tmp_path, "healthy")

    assert result.returncode == 0, result.stderr
    assert "Cross-cluster curl succeeded on attempt 1" in result.stdout
    assert kubectl_log.count("get namespace cm-smoke") == 2


def test_cross_cluster_smoke_fails_when_cleanup_does_not_converge(tmp_path):
    result, _ = _run_smoke(tmp_path, "cleanup-stuck")

    assert result.returncode != 0
    assert "smoke succeeded but cleanup failed" in result.stdout
    assert "cleanup did not converge" in result.stderr
