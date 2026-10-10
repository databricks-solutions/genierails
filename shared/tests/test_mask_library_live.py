"""Live SQL/reference parity test; opt in with DATABRICKS_LIVE_TESTS=1."""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("DATABRICKS_LIVE_TESTS") != "1",
    reason="set DATABRICKS_LIVE_TESTS=1 for warehouse validation",
)


def test_live_mask_library():
    # Read-only: every SQL body runs inline against exact expected values.
    from scripts.live_mask_library import BODY_CASES, run_from_environment
    result = run_from_environment()
    # Every case asserts its exact expected value; details name any mismatch.
    assert result["details"] == []
    assert result["bodies_tested"] == sum(len(cases) for cases in BODY_CASES.values())
