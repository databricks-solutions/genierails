"""Ctrl-C during a data_access apply must not kill Terraform through its tee.

terraform_layer.sh copies data_access plan/apply output through tee. If
SIGINT (or TERM/HUP) killed tee, Terraform's graceful shutdown would write
"saving state" into a closed pipe, die of SIGPIPE and lose the state.
"""

import os
import signal
import subprocess
import time
from pathlib import Path

import pytest

RUNNER = Path(__file__).parents[1] / "scripts" / "terraform_layer.sh"

# Stands in for terraform: init succeeds; apply waits for the signal, then
# keeps writing for a while (like saving state) and exits 7.
FAKE_TERRAFORM = """#!/bin/bash
[ "$1" = "apply" ] || exit 0
trap 'echo SIGPIPE >> "$MARK"' PIPE
trap 'echo "got $SIG" >> "$MARK"; stop=1' INT TERM HUP
echo "Applying..."
echo ready >> "$MARK"
while [ -z "$stop" ]; do sleep 0.05; done
for i in $(seq 1 300); do
  echo "Saving state $i" || { echo "write failed" >> "$MARK"; exit 99; }
done
echo finished >> "$MARK"
exit 7
"""


@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM, signal.SIGHUP])
def test_signal_during_apply_lets_terraform_finish_writing(tmp_path, sig):
    env_dir = tmp_path / "data_access"
    env_dir.mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "terraform").write_text(FAKE_TERRAFORM)
    (bin_dir / "terraform").chmod(0o755)
    mark = tmp_path / "mark"
    env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
           "LAYER_ENV_DIR": str(env_dir), "MARK": str(mark), "SIG": sig.name}

    # Its own process group, signalled as a whole, like a terminal's Ctrl-C.
    process = subprocess.Popen([str(RUNNER), "data_access", "dev", "apply", "-auto-approve"],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                               env=env, start_new_session=True)
    try:
        deadline = time.monotonic() + 30
        while not (mark.exists() and "ready" in mark.read_text()):
            assert process.poll() is None and time.monotonic() < deadline, "fake apply never started"
            time.sleep(0.05)
        os.killpg(process.pid, sig)
        stdout, _ = process.communicate(timeout=60)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()

    marks = mark.read_text().split("\n")
    assert f"got {sig.name}" in marks
    assert "SIGPIPE" not in marks and "write failed" not in marks
    assert "finished" in marks
    assert "Saving state 300" in stdout
    assert process.returncode == 7  # Terraform's own exit status
