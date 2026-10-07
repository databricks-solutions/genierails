"""make release must prove masking: with column masks but no row-pairing key,
or a VERIFY_SPEC without keyed checks for every masked column, verify-access
could not prove the masks, so release refuses before it takes its lock (and
again before it applies), and its verify-access runs with
--require-mask-checks."""

import json
import os
import shlex
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
# The live-derived assignments release promotes before it applies.
TAGGED = MASK_POLICY + '''tag_assignments = [
  { entity_type = "columns", entity_name = "cat.sch.customers.ssn", tag_key = "gr_treatment", tag_value = "redact" },
  { entity_type = "columns", entity_name = "cat.sch.notes.free_text", tag_key = "gr_treatment", tag_value = "redact" },
]
'''


def _env(tmp_path, policies=MASK_POLICY, layer="data_access", key=""):
    env = tmp_path / "prod"
    (env / layer).mkdir(parents=True)
    (env / layer / "abac.auto.tfvars").write_text(policies)
    (env / "env.auto.tfvars").write_text(f'verify_key_column = "{key}"\n')
    return env


def _mask(table, column, key="id"):
    return {"table": table, "column": column, "key_column": key,
            "masked_principals": ["analysts"], "unmasked_principals": ["__admin__"]}


ROW_FILTER_SPEC = {"column_masks": [], "row_filters": [
    {"table": "cat.sch.customers", "restricted_principals": ["analysts"], "unrestricted_principals": ["__admin__"]}]}
FULL_SPEC = {"column_masks": [_mask("cat.sch.customers", "ssn"), _mask("cat.sch.notes", "free_text")],
             "row_filters": []}


def _spec(tmp_path, spec) -> str:
    path = tmp_path / "spec.json"
    path.write_text(json.dumps(spec) if isinstance(spec, dict) else spec)
    return str(path)


# ── no VERIFY_SPEC: the key decides ─────────────────────────────────────────

@pytest.mark.parametrize("layer", ["data_access", "generated"])
def test_masks_without_a_key_refuse(tmp_path, capsys, layer):
    env = _env(tmp_path, layer=layer)
    assert rh.require_mask_proof(env, "prod", "", "") == 1
    err = capsys.readouterr().err
    assert "could not prove its 1 column mask(s); nothing was applied" in err
    assert "verify_key_column" in err and "VERIFY_KEY_COLUMN" in err and "VERIFY_SPEC" in err


def test_a_key_lets_release_proceed(tmp_path):
    assert rh.require_mask_proof(_env(tmp_path), "prod", "customer_id", "") == 0


@pytest.mark.parametrize("policies", [ROW_FILTER_ONLY, "fgac_policies = []\n", ""])
def test_nothing_to_pair_needs_no_key(tmp_path, policies):
    assert rh.require_mask_proof(_env(tmp_path, policies=policies), "prod", "", "") == 0


# ── VERIFY_SPEC: its contents decide, not its path ──────────────────────────

@pytest.mark.parametrize("spec, message", [
    (ROW_FILTER_SPEC, "has no column-mask checks"),
    ({"column_masks": [], "row_filters": []}, "has no column-mask checks"),
    ({"column_masks": [_mask("cat.sch.customers", "ssn", key=""), _mask("cat.sch.notes", "free_text")]},
     "without a key_column: cat.sch.customers.ssn"),
    ({"column_masks": [_mask("cat.sch.customers", "ssn")]}, "does not check masked column(s): cat.sch.notes.free_text"),
    ("not json", "could not be read"),
])
@pytest.mark.parametrize("key", ["", "customer_id"])  # a key never rescues the spec verify-access runs
def test_a_spec_that_cannot_prove_every_mask_refuses(tmp_path, capsys, spec, message, key):
    env = _env(tmp_path, policies=TAGGED)
    assert rh.require_mask_proof(env, "prod", key, _spec(tmp_path, spec)) == 1
    err = capsys.readouterr().err
    assert message in err and "nothing was applied" in err


def test_a_missing_spec_file_refuses(tmp_path, capsys):
    assert rh.require_mask_proof(_env(tmp_path), "prod", "", str(tmp_path / "absent.json")) == 1
    assert "could not be read" in capsys.readouterr().err


def test_a_keyed_spec_covering_every_masked_column_passes(tmp_path):
    env = _env(tmp_path, policies=TAGGED)
    assert rh.require_mask_proof(env, "prod", "", _spec(tmp_path, FULL_SPEC)) == 0


def test_before_derivation_a_keyed_mask_spec_passes_and_is_rechecked_later(tmp_path):
    # Prod's promoted config has no tag assignments until release derives them,
    # so the pre-lock check can only demand keyed mask checks; the re-check
    # after release promotes the derived config demands full coverage.
    env = _env(tmp_path)
    one = _spec(tmp_path, {"column_masks": [_mask("cat.sch.customers", "ssn")]})
    assert rh.require_mask_proof(env, "prod", "", one) == 0
    (env / "data_access" / "abac.auto.tfvars").write_text(TAGGED)
    assert rh.require_mask_proof(env, "prod", "", one) == 1


def test_a_relative_spec_resolves_from_shared_like_verify_access(tmp_path, monkeypatch):
    env = _env(tmp_path, policies=TAGGED)
    spec = SHARED / "tests" / f".tmp-spec-{os.getpid()}.json"
    spec.write_text(json.dumps(FULL_SPEC))
    try:
        monkeypatch.chdir(tmp_path)  # make runs this from aws/ or azure/
        assert rh.require_mask_proof(env, "prod", "", f"tests/{spec.name}") == 0
    finally:
        spec.unlink()


# ── make release: refuse before the lock; re-check before apply ─────────────

def _clean_env():
    return {k: v for k, v in os.environ.items()
            if k not in ("GNUMAKEFLAGS", "MAKEFLAGS", "MAKELEVEL", "VERIFY_KEY_COLUMN", "VERIFY_SPEC")}


def _recorders(tmp_path):
    """A make stub, a lock helper and a release helper that record their calls
    (the release helper then runs the real one)."""
    log = tmp_path / "calls"
    make = tmp_path / "make-stub"
    make.write_text(f"#!/bin/sh\nprintf 'make %s\\n' \"$*\" >> '{log}'\nexit 0\n")
    lock = tmp_path / "lock-helper"
    lock.write_text(f"#!/bin/sh\nprintf 'lock %s\\n' \"$1\" >> '{log}'\nexit 0\n")
    helper = tmp_path / "release-helper"
    helper.write_text(f"#!/bin/sh\nprintf 'release-helper %s\\n' \"$1\" >> '{log}'\n"
                      f"exec python3 '{SHARED / 'scripts' / 'release_helpers.py'}' \"$@\"\n")
    for path in (make, lock, helper):
        path.chmod(0o755)
    return log, [f"MAKE={make}", f"_ENV_LOCK_HELPER={lock}", f"_RELEASE_HELPER={helper}"]


def _release(env, overrides, *extra):
    return subprocess.run(["make", "--no-print-directory", "release", "ENV=prod", f"ENV_DIR={env}",
                           *overrides, *extra],
                          cwd=ROOT / "aws", text=True, capture_output=True, env=_clean_env(), timeout=120)


def _calls(log):
    return [shlex.split(line) for line in log.read_text().splitlines()] if log.exists() else []


def _names(log):
    """Each call's target or helper command, without make's flags."""
    out = []
    for call in _calls(log):
        words = [w for w in call[1:] if not w.startswith("--")]
        out.append(f"{call[0]} {words[0] if words else ''}".strip())
    return out


@pytest.mark.parametrize("extra", [(), ("VERIFY_SPEC={spec}",)])
def test_release_refuses_before_its_lock(tmp_path, extra):
    # No key, or (the release-level regression) a row-filter-only VERIFY_SPEC
    # against a config with masks: the only call is the refusing check.
    env = _env(tmp_path)
    spec = _spec(tmp_path, ROW_FILTER_SPEC)
    log, overrides = _recorders(tmp_path)
    result = _release(env, overrides, *(e.format(spec=spec) for e in extra))
    assert result.returncode != 0
    assert "nothing was applied" in result.stderr
    assert _names(log) == ["release-helper require-mask-proof"]


def test_release_checks_before_locking_and_again_before_applying(tmp_path):
    env = _env(tmp_path, key="customer_id")
    log, overrides = _recorders(tmp_path)
    result = _release(env, overrides)
    assert result.returncode == 0, result.stdout + result.stderr
    names = _names(log)
    assert names[:3] == ["release-helper require-mask-proof", "lock lock", "release-helper clear-old-receipts"], names
    promote = names.index("make promote")
    assert names[promote + 1] == "release-helper require-mask-proof", names  # re-check on the derived config
    assert names.index("make apply") > promote + 1
    assert names[-2:] == ["make verify-access", "lock unlock"], names
    assert "VERIFY_REQUIRE_MASKS=1" in _calls(log)[-2]


def test_release_verifies_strictly():
    makefile = (SHARED / "Makefile.shared").read_text()
    assert "_VERIFY_REQUIRE_FLAG = $(if $(filter 1,$(VERIFY_REQUIRE_MASKS)),--require-mask-checks,)" in makefile
    verify = makefile[makefile.index("\nverify-access:"):]
    assert "$(_VERIFY_REQUIRE_FLAG)" in verify[:verify.index("\n\n")]
