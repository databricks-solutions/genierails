"""Regression tests for `make setup` next steps (dev_to_prod champion flow)."""

import os
import re
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).parents[2]
SHARED_ROOT = ROOT / "shared"
CLOUDS = ("aws", "azure")


def _clean_env():
    return {
        key: value
        for key, value in os.environ.items()
        if key not in ("MAKEFLAGS", "MAKELEVEL", "ENV", "ACCOUNT_ADMIN_ENV")
    }


def _make(cloud, tmp_path, *args):
    cloud_root = tmp_path / cloud
    cloud_root.mkdir(exist_ok=True)
    return subprocess.run(
        [
            "make",
            "--no-print-directory",
            *args,
            f"CLOUD_ROOT={cloud_root}",
            f"SHARED_ROOT={SHARED_ROOT}",
        ],
        cwd=ROOT / cloud,
        text=True,
        capture_output=True,
        env=_clean_env(),
    )


def _defined_targets():
    makefile = (SHARED_ROOT / "Makefile.shared").read_text()
    return {
        line.split(":", 1)[0]
        for line in makefile.splitlines()
        if line and not line[0].isspace() and ":" in line and "=" not in line.split(":", 1)[0]
    }


def _mentioned_targets(output):
    # Commands appear as "Run: make <t>", "or: make <t>", or indented on their own line.
    return re.findall(r"(?:^\s+|: )make ([a-z][a-z-]*)", output, flags=re.MULTILINE)


@pytest.mark.parametrize("cloud", CLOUDS)
def test_setup_dev_prints_champion_phase_1_steps(cloud, tmp_path):
    result = _make(cloud, tmp_path, "setup", "ENV=dev")
    assert result.returncode == 0, result.stderr
    out = result.stdout

    assert "cp ../shared/examples/dev_to_prod/env.auto.tfvars.example envs/dev/env.auto.tfvars" in out
    assert "envs/dev/auth.auto.tfvars" in out
    assert (
        "Note: envs/account/env.auto.tfvars already has manage_groups = false"
    ) in out
    assert "Leave it unless this is a demo/greenfield account." in out
    assert "Edit envs/account/env.auto.tfvars" not in out
    assert not re.search(r"^\s+\d+\..*manage_groups", out, flags=re.MULTILINE)
    assert "  3. Edit envs/dev/env.auto.tfvars" in out
    assert "existing agent: add genie_space_id to genie_spaces" in out
    assert (
        "make generate ENV=dev MODE=genie "
        "GENERATE_ARGS='--groups \"<most_privileged>,...,<least_privileged>\"'"
    ) in out
    assert (
        'tables auto-discovered; uc_tables not needed. '
        'sql_warehouse_id: your warehouse id, or "" to auto-create'
    ) in out
    assert "no agent yet: run the sample env setup" in out
    assert "../shared/examples/dev_to_prod/SAMPLE_ENV.md" in out
    assert "uc_tables, sql_warehouse_id (or blank), genie_spaces" not in out
    assert "make enable-classification ENV=dev" in out
    assert "  4. Enable classification" in out
    assert "make generate ENV=dev GENERATE_ARGS='--groups " in out
    assert "  5. Run: make generate" in out
    assert "make rehearse ENV=dev VERIFY_KEY_COLUMN=" in out
    assert "  6. Run: make rehearse" in out
    assert "shared/examples/dev_to_prod/README.md" in out
    # The champion flow relies on native Data Classification, not the country overlay.
    assert "APJ" not in out
    assert "country" not in out
    # Plain apply skips the coverage gate; setup must not suggest it for dev.
    assert "make apply" not in out
    assert (
        out.index("envs/dev/auth.auto.tfvars")
        < out.index("manage_groups = false")
        < out.index("existing agent:")
        < out.index("MODE=genie")
        < out.index("enable-classification")
        < out.rindex("make generate")
        < out.index("make rehearse")
    )


@pytest.mark.parametrize("cloud", CLOUDS)
def test_setup_prod_prints_promote_certify_apply_steps(cloud, tmp_path):
    result = _make(cloud, tmp_path, "setup", "ENV=prod")
    assert result.returncode == 0, result.stderr
    out = result.stdout

    order = [
        "make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP=",
        "envs/prod/auth.auto.tfvars",
        "make enable-classification ENV=prod",
        "make certify ENV=prod",
        "business_access_enabled = true",
        "make apply ENV=prod",
        "make verify-access ENV=prod VERIFY_KEY_COLUMN=",
    ]
    positions = [out.index(step) for step in order]
    assert positions == sorted(positions)
    assert "make rehearse" not in out
    assert "make generate" not in out
    assert "shared/examples/dev_to_prod/README.md" in out


@pytest.mark.parametrize("env", ["dev", "prod", "account"])
def test_setup_mentions_only_existing_make_targets(env, tmp_path):
    result = _make("aws", tmp_path, "setup", f"ENV={env}")
    assert result.returncode == 0, result.stderr
    mentioned = _mentioned_targets(result.stdout)
    assert mentioned
    assert set(mentioned) <= _defined_targets()


def test_setup_does_not_create_account_admin_env(tmp_path):
    admin_env = tmp_path / "account-admin.aws.env"
    result = _make("aws", tmp_path, "setup", "ENV=dev", f"ACCOUNT_ADMIN_ENV={admin_env}")
    assert result.returncode == 0, result.stderr
    assert not admin_env.exists()
    assert "account-admin" not in result.stdout
    assert "test-ci" not in result.stdout


def test_test_ci_guard_rejects_missing_custom_account_admin_env(tmp_path):
    admin_env = tmp_path / "missing.env"
    result = _make("aws", tmp_path, "test-ci", f"ACCOUNT_ADMIN_ENV={admin_env}")
    assert result.returncode != 0
    assert f"ACCOUNT_ADMIN_ENV file not found: {admin_env}" in result.stdout
    assert "CI Pipeline" not in result.stdout
    assert not admin_env.exists()


def test_test_ci_guard_creates_default_account_admin_env_and_stops(tmp_path):
    admin_env = tmp_path / "account-admin.aws.env"
    result = _make(
        "aws",
        tmp_path,
        "test-ci",
        f"_DEFAULT_ACCOUNT_ADMIN_ENV={admin_env}",
        f"ACCOUNT_ADMIN_ENV={admin_env}",
    )
    assert result.returncode != 0
    assert f"Created {admin_env}" in result.stdout
    assert "CI Pipeline" not in result.stdout
    assert admin_env.read_text() == (SHARED_ROOT / "scripts" / "account-admin.aws.env.example").read_text()
