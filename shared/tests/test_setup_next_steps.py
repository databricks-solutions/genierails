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
    assert "manage_groups" not in out
    assert "Edit envs/account/env.auto.tfvars" not in out
    assert (
        "  3. Edit envs/dev/env.auto.tfvars — set genie_space_id to your Genie agent ID and\n"
        "     access_tier_groups to your IdP groups, most to least privileged\n"
    ) in out
    # One plain generate imports the agent; no separate MODE=genie step.
    assert "MODE=genie" not in out
    # access_tier_groups makes --groups unnecessary on every champion command.
    assert "GENERATE_ARGS" not in out
    assert "--groups" not in out
    assert "     (No agent yet? See ../shared/examples/dev_to_prod/SAMPLE_ENV.md)\n" in out
    # rehearse saves the key itself; setup never asks for a hand edit.
    assert "     The saved key (verify_key_column) is carried by promote; make release needs it.\n" in out
    assert "Save the key as verify_key_column" not in out
    # The champion flow needs only the agent ID; tables are discovered.
    assert "uc_tables" not in out
    assert "existing agent:" not in out
    assert "pick one" not in out
    assert "  4. In Catalog Explorer, open the catalog > Details tab > Data classification: turn it on.\n" in out
    assert "     When the scan finishes, review the detections and exclude any false positives, then turn on\n" in out
    assert "     auto-tagging. Wait until the class.* tags appear.\n" in out
    assert "     Prefer a script? make enable-classification ENV=dev turns it on (you still review detections in the UI).\n" in out
    assert "  5. Run: make generate ENV=dev   (one run: imports the agent, finds its tables, drafts rules)\n" in out
    assert "  6. Run: make rehearse ENV=dev VERIFY_KEY_COLUMN=<key_column>   (key saved after a passing run)\n" in out
    assert "business_access_enabled" not in out
    assert "     (grants business SELECT + Genie CAN_RUN once its coverage check passes; no access flag to set)\n" in out
    assert re.findall(r"^  (\d+)\. ", out, flags=re.MULTILINE) == ["1", "2", "3", "4", "5", "6"]
    assert "shared/examples/dev_to_prod/README.md" in out
    # The champion flow relies on native Data Classification, not the country overlay.
    assert "APJ" not in out
    assert "country" not in out
    # Setup points dev at rehearse (derive, validate and gate first), not plain apply.
    assert "make apply" not in out
    assert (
        out.index("envs/dev/auth.auto.tfvars")
        < out.index("set genie_space_id")
        < out.index("access_tier_groups")
        < out.index("SAMPLE_ENV.md")
        < out.index("enable-classification")
        < out.rindex("make generate")
        < out.index("make rehearse")
    )


@pytest.mark.parametrize("cloud", CLOUDS)
def test_setup_omits_completed_walkthrough_copy_and_renumbers(cloud, tmp_path):
    first = _make(cloud, tmp_path, "setup", "ENV=dev")
    assert first.returncode == 0
    env_file = tmp_path / cloud / "envs/dev/env.auto.tfvars"
    env_file.write_text((SHARED_ROOT / "examples/dev_to_prod/env.auto.tfvars.example").read_text())

    result = _make(cloud, tmp_path, "setup", "ENV=dev")
    assert result.returncode == 0, result.stderr
    assert "cp ../shared/examples/dev_to_prod/env.auto.tfvars.example" not in result.stdout
    assert re.findall(r"^  (\d+)\. ", result.stdout, flags=re.MULTILINE) == ["1", "2", "3", "4", "5"]


@pytest.mark.parametrize("cloud", CLOUDS)
def test_setup_prod_prints_promote_release_maintain_steps(cloud, tmp_path):
    result = _make(cloud, tmp_path, "setup", "ENV=prod")
    assert result.returncode == 0, result.stderr
    out = result.stdout

    assert "Next steps (production — walkthrough Phases 2-5; finish the dev rehearsal first):" in out
    order = [
        "Set promote_from and catalog_map in envs/prod/env.auto.tfvars",
        'make promote-to ENV=prod\n',
        "     (FROM=/CATALOG_MAP= remain optional command-line overrides.)\n",
        "envs/prod/auth.auto.tfvars",
        "In Catalog Explorer, open the catalog > Details tab > Data classification: turn it on",
        "Prefer a script? make enable-classification ENV=prod turns it on",
        "  5. Run: make release ENV=prod   (the verify key comes from dev)\n",
        "make maintain ENV=prod",
    ]
    positions = [out.index(step) for step in order]
    assert positions == sorted(positions)
    assert "make rehearse" not in out
    assert "make generate" not in out
    assert "VERIFY_KEY_COLUMN" not in out
    assert "DEST_CATALOG_MAP" not in out
    assert "     Without a promoted verify_key_column, release refuses before applying.\n" in out
    assert "Set business_access_enabled" not in out
    assert "business_access_enabled" not in out
    assert "     (grants business SELECT + Genie CAN_RUN once its coverage check passes; no access flag to set)\n" in out
    assert "make apply ENV=prod" not in out
    assert "make verify-access ENV=prod" not in out
    assert "make certify" not in out
    assert re.findall(r"^  (\d+)\. ", out, flags=re.MULTILINE) == ["1", "2", "3", "4", "5", "6"]
    assert "shared/examples/dev_to_prod/README.md" in out
    env_text = (tmp_path / cloud / "envs/prod/env.auto.tfvars").read_text()
    assert 'promote_from = "dev"' in env_text
    assert 'catalog_map  = { "<dev_catalog>" = "<prod_catalog>" }' in env_text


@pytest.mark.parametrize("cloud", CLOUDS)
def test_setup_non_default_env_seeds_promotion_settings(cloud, tmp_path):
    result = _make(cloud, tmp_path, "setup", "ENV=stg")
    assert result.returncode == 0, result.stderr
    env_text = (tmp_path / cloud / "envs/stg/env.auto.tfvars").read_text()
    assert 'promote_from = "dev"' in env_text
    assert 'catalog_map  = { "<dev_catalog>" = "<stg_catalog>" }' in env_text


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


def test_cross_env_promote_points_to_release_not_apply():
    makefile = (SHARED_ROOT / "Makefile.shared").read_text()
    block = makefile[makefile.index("=== Promote complete:"):]
    block = block[:block.index("trap - EXIT")]

    assert "make release ENV=$(DEST_ENV)" in block
    assert "make apply ENV=$(DEST_ENV)" not in block
