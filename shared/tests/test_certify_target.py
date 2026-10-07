"""The old command remains a thin, deprecated alias for unified release."""

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).parents[2]


def test_certify_alias_prints_note_and_runs_release(tmp_path):
    stub = tmp_path / "make"
    log = tmp_path / "log"
    stub.write_text(f"#!/bin/sh\nprintf '%s\\n' \"$*\" > '{log}'\n")
    stub.chmod(0o755)
    env = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL")}
    result = subprocess.run(
        ["make", "certify", "ENV=prod", f"MAKE={stub}"], cwd=ROOT / "aws",
        text=True, capture_output=True, env=env,
    )
    assert result.returncode == 0
    assert "DEPRECATED" in result.stdout
    assert log.read_text().strip() == "--no-print-directory release ENV=prod"
