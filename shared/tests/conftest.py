"""Shared pytest fixtures for the unit test suite.

No Databricks connection, LLM call, or Terraform is required.
"""
import pytest
import hcl2
from pathlib import Path


@pytest.fixture
def tmp_tfvars(tmp_path):
    """Return a factory that writes content to a temp .tfvars file."""
    def _write(content: str) -> Path:
        p = tmp_path / "abac.auto.tfvars"
        p.write_text(content)
        return p
    return _write


@pytest.fixture
def tmp_sql(tmp_path):
    """Return a factory that writes content to a temp .sql file."""
    def _write(content: str) -> Path:
        p = tmp_path / "masking_functions.sql"
        p.write_text(content)
        return p
    return _write


def assert_valid_hcl(path: Path) -> dict:
    """Parse path with hcl2 and return the config dict.

    Raises AssertionError with a clear message if the file is invalid HCL.
    """
    try:
        return hcl2.loads(path.read_text())
    except Exception as exc:
        raise AssertionError(
            f"File is not valid HCL after autofix:\n"
            f"  file: {path}\n"
            f"  error: {exc}\n\n"
            f"Content:\n{path.read_text()}"
        ) from exc
