"""make certify is removed, not aliased to release.

certify used to stop before granting access; aliased to release it would grant
at the step a pipeline runs before its approval (certify -> approval ->
release). It must fail, run nothing, and say what to run instead.
"""

import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_certify_fails_with_a_migration_message_and_runs_nothing(tmp_path, cloud):
    stub = tmp_path / "make"
    log = tmp_path / "log"
    stub.write_text(f"#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{log}'\n")
    stub.chmod(0o755)
    env = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL")}
    result = subprocess.run(
        ["make", "certify", "ENV=prod", f"MAKE={stub}"], cwd=ROOT / cloud,
        text=True, capture_output=True, env=env,
    )
    assert result.returncode != 0
    assert ("make certify was removed; run make release ENV=prod (it runs the coverage check, "
            "audit and verification") in result.stderr
    assert not log.exists()  # no release, no sub-make at all


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_changelog_records_the_removal(cloud):
    changelog = (ROOT / cloud / "CHANGELOG.md").read_text()
    unreleased = changelog[changelog.index("## [Unreleased]"):]
    unreleased = unreleased[:unreleased.index("\n## ", 1)]
    assert "`make certify`" in unreleased and "make release ENV=<env>" in unreleased
