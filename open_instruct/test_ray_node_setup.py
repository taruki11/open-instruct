import os
import pathlib
import shutil
import subprocess

import pytest

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "configs" / "beaker_configs" / "ray_node_setup.sh"

# Resolve bash once and run it by absolute path: on Windows, a bare "bash" can resolve to the WSL stub.
BASH = shutil.which("bash")

pytestmark = pytest.mark.skipif(BASH is None, reason="requires bash")

FAKE_COMMANDS = {
    "python": "echo 010125_000000",
    "getent": "echo '10.0.0.1 head'",
    "sleep": ":",
    "ray": """
case "$1" in
    start)
        echo "ray $*" >> "$FAKE_DIR/ray_calls"
        exit "${FAKE_RAY_START_RC:-0}"
        ;;
    status)
        count=$(cat "$FAKE_DIR/status_count" 2>/dev/null || echo 0)
        if [ "$count" -lt "${FAKE_RAY_STATUS_OK_COUNT:-0}" ]; then
            echo $((count + 1)) > "$FAKE_DIR/status_count"
            exit 0
        fi
        exit 1
        ;;
esac
exit 0
""",
}


def run_ray_node_setup(tmp_path: pathlib.Path, **env: str) -> subprocess.CompletedProcess:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in FAKE_COMMANDS.items():
        # newline="\n" keeps the shebang valid when the test runs on Windows.
        (bin_dir / name).write_text(f"#!/bin/bash\n{body}\n", newline="\n")
        (bin_dir / name).chmod(0o755)
    full_env = {
        **os.environ,
        "PATH": f"{bin_dir.as_posix()}{os.pathsep}{os.environ['PATH']}",
        "FAKE_DIR": tmp_path.as_posix(),
        "HOME": tmp_path.as_posix(),
        **env,
    }
    return subprocess.run(
        [BASH, SCRIPT.as_posix()],
        env=full_env,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
        check=False,
    )


def test_worker_without_leader_hostname_exits_1(tmp_path):
    result = run_ray_node_setup(tmp_path, BEAKER_REPLICA_RANK="1", BEAKER_LEADER_REPLICA_HOSTNAME="")
    assert result.returncode == 1, result.stdout + result.stderr
    assert "Cannot resolve the Ray head" in result.stdout
    assert not (tmp_path / "ray_calls").exists()


def test_worker_exits_1_when_ray_start_fails(tmp_path):
    result = run_ray_node_setup(
        tmp_path, BEAKER_REPLICA_RANK="1", BEAKER_LEADER_REPLICA_HOSTNAME="head", FAKE_RAY_START_RC="1"
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "failed. Exiting 1" in result.stdout


def test_worker_exits_1_when_head_is_never_reachable(tmp_path):
    result = run_ray_node_setup(
        tmp_path, BEAKER_REPLICA_RANK="1", BEAKER_LEADER_REPLICA_HOSTNAME="head", FAKE_RAY_STATUS_OK_COUNT="0"
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "was never reachable" in result.stdout


def test_worker_exits_0_when_reachable_head_goes_away(tmp_path):
    result = run_ray_node_setup(
        tmp_path, BEAKER_REPLICA_RANK="1", BEAKER_LEADER_REPLICA_HOSTNAME="head", FAKE_RAY_STATUS_OK_COUNT="3"
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Head is unreachable. Stopping worker and exiting 0." in result.stdout


def test_head_node_starts_ray_head(tmp_path):
    result = run_ray_node_setup(tmp_path, BEAKER_REPLICA_RANK="0", BEAKER_LEADER_REPLICA_HOSTNAME="head")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "--head" in (tmp_path / "ray_calls").read_text()
