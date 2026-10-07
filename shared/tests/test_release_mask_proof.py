"""make release must prove masking: with column masks but no row-pairing key,
verify-access could only skip every mask check, so release refuses before it
applies anything (and its verify-access runs with --require-mask-checks)."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

SHARED = Path(__file__).resolve().parents[1]
ROOT = SHARED.parent
sys.path.insert(0, str(SHARED / "scripts"))

import release_helpers as rh  # noqa: E402

MASK_POLICY = '''fgac_policies = [
  {
    name = "gr_mask_redact"
    policy_type = "POLICY_TYPE_COLUMN_MASK"
    catalog = "cat"
    to_principals = ["analysts"]
    match_condition = "hasTagValue('gr_treatment', 'redact')"
    match_alias = "gr_treatment_redact"
    function_name = "mask_redact"
    function_catalog = "cat"
    function_schema = "sch"
  },
]
'''
ROW_FILTER_ONLY = MASK_POLICY.replace("POLICY_TYPE_COLUMN_MASK", "POLICY_TYPE_ROW_FILTER")


def _env(tmp_path, policies=MASK_POLICY, layer="data_access", key=""):
    env = tmp_path / "prod"
    (env / layer).mkdir(parents=True)
    (env / layer / "abac.auto.tfvars").write_text(policies)
    (env / "env.auto.tfvars").write_text(f'verify_key_column = "{key}"\n')
    return env


@pytest.mark.parametrize("layer", ["data_access", "generated"])
def test_masks_without_a_key_refuse(tmp_path, capsys, layer):
    env = _env(tmp_path, layer=layer)
    assert rh.require_mask_proof(env, "prod", "", "") == 1
    err = capsys.readouterr().err
    assert "could not prove its 1 column mask(s); nothing was applied" in err
    assert "verify_key_column" in err and "VERIFY_KEY_COLUMN" in err and "VERIFY_SPEC" in err


@pytest.mark.parametrize("key, spec", [("customer_id", ""), ("", "spec.json")])
def test_a_key_or_spec_lets_release_proceed(tmp_path, key, spec):
    assert rh.require_mask_proof(_env(tmp_path), "prod", key, spec) == 0


@pytest.mark.parametrize("policies", [ROW_FILTER_ONLY, "fgac_policies = []\n", ""])
def test_nothing_to_pair_needs_no_key(tmp_path, policies):
    assert rh.require_mask_proof(_env(tmp_path, policies=policies), "prod", "", "") == 0


def test_release_checks_before_deriving_and_verifies_strictly():
    makefile = (SHARED / "Makefile.shared").read_text()
    body = makefile[makefile.index("\nrelease:"):]
    body = body[:body.index("\n\n")]
    assert body.index("require-mask-proof") < body.index("derive-assignments") < body.index("$(MAKE) apply")
    assert 'resolve_env_config.py" verify-key --env-dir "$(ENV_DIR)" --explicit "$(VERIFY_KEY_COLUMN)"' in body
    assert "verify-access ENV=\"$(ENV)\" VERIFY_REQUIRE_MASKS=1" in body
    assert "_VERIFY_REQUIRE_FLAG = $(if $(filter 1,$(VERIFY_REQUIRE_MASKS)),--require-mask-checks,)" in makefile
    verify = makefile[makefile.index("\nverify-access:"):]
    assert "$(_VERIFY_REQUIRE_FLAG)" in verify[:verify.index("\n\n")]


def _clean_env():
    return {k: v for k, v in os.environ.items() if k not in ("GNUMAKEFLAGS", "MAKEFLAGS", "MAKELEVEL")}


def test_make_release_without_a_key_stops_before_anything_runs(tmp_path):
    env = _env(tmp_path)
    (env / "generated").mkdir()
    result = subprocess.run(["make", "--no-print-directory", "release", "ENV=prod", f"ENV_DIR={env}"],
                            cwd=ROOT / "aws", text=True, capture_output=True, env=_clean_env(), timeout=120)
    assert result.returncode != 0
    assert "nothing was applied" in result.stderr
    assert "Derive Assignments" not in result.stdout + result.stderr
    assert "Terraform" not in result.stdout
    assert not (env / "generated" / ".governance.lock").exists()
