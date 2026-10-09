"""Live SQL/reference parity test; opt in with DATABRICKS_LIVE_TESTS=1."""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("DATABRICKS_LIVE_TESTS") != "1",
    reason="set DATABRICKS_LIVE_TESTS=1 for warehouse validation",
)


def test_live_mask_library():
    # The executable lifecycle, including UC-secret creation and guaranteed
    # scratch cleanup, lives in one reusable module for local and CI runs.
    from scripts.live_mask_library import run_from_environment
    result = run_from_environment()
    assert result["mismatches"] == 0
    assert result["bodies_tested"] > 0
