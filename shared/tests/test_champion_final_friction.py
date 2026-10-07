"""Last first-time-champion friction in the dev-to-prod walkthrough.

Covers: generate prunes masks it just drafted that no policy uses (never a
reviewed or hand-written one), keeps reviewed group descriptions on re-run,
and reports what each autofixed policy finally became; data_access plan and
apply explain a masking_functions-only replacement; the account layer warns
about the retired business_access_enabled like the workspace envs do.
"""

import os
import subprocess
import sys
from pathlib import Path

import hcl2
import pytest

import generate_abac
from scripts.merge_space_configs import (
    keep_reviewed_group_descriptions,
    prune_unused_new_functions,
)
from tests.test_sticky_reviewed_rules import (  # noqa: F401  (env_dir is a fixture)
    EMAIL,
    LIMIT,
    _coverage_gate,
    E2E_DDL,
    _model_response,
    env_dir,
)

SHARED = Path(__file__).parents[1]
RUNNER = SHARED / "scripts/terraform_layer.sh"
NOTE_SCRIPT = SHARED / "scripts/masking_replace_note.py"
NOTE = ("Note: replacing terraform_data.masking_functions only re-runs CREATE OR REPLACE; "
        "it never drops live mask functions.")

sys.path.insert(0, str(SHARED / "scripts"))
from masking_replace_note import needs_note  # noqa: E402

UNUSED_FN = """
-- Drafted but never used by a policy.
CREATE OR REPLACE FUNCTION mask_unused_draft(val STRING)
RETURNS STRING
RETURN CONCAT('x;', val);
"""


def _response_with(extra_sql="", descriptions=("Full access", "Masked")):
    response = _model_response({EMAIL: EMAIL, LIMIT: LIMIT})
    response = response.replace("RETURN ROUND(amount, -2);\n```", "RETURN ROUND(amount, -2);\n" + extra_sql + "```")
    return (response
            .replace('description = "Full access"', f'description = "{descriptions[0]}"')
            .replace('description = "Masked"', f'description = "{descriptions[1]}"'))


def _generate_response(env_dir, monkeypatch, response, *extra):
    monkeypatch.setattr(generate_abac, "_POLICY_AUTOFIXES", set())
    monkeypatch.setattr(generate_abac, "fetch_tables_from_databricks",
                        lambda refs, cfg: (E2E_DDL, [("dev_fin", "payments")]))
    monkeypatch.setattr(generate_abac, "call_with_retries", lambda *a, **k: response)
    monkeypatch.setattr(sys, "argv", [
        "generate_abac.py", "--auth-file", str(env_dir / "auth.auto.tfvars"),
        "--groups", "payments_ops,viewers", "--out-dir", str(env_dir / "generated"), *extra,
    ])
    generate_abac.main()


# ---------------------------------------------------------------------------
# 2a. Unused masking functions the model just drafted are pruned
# ---------------------------------------------------------------------------

def test_generate_prunes_a_new_unused_function_and_the_validator_stays_quiet(env_dir, monkeypatch, capfd):
    _generate_response(env_dir, monkeypatch, _response_with(UNUSED_FN))
    out = capfd.readouterr().out
    sql = (env_dir / "generated/masking_functions.sql").read_text()
    assert "mask_unused_draft" not in sql
    assert "Drafted but never used" not in sql
    assert "[AUTOFIX] Removed 1 new masking function(s) no policy uses: mask_unused_draft" in out
    assert "functions not used by any policy" not in out
    assert "RESULT: PASS" in out
    # Every function a policy uses is still there.
    cfg = hcl2.loads((env_dir / "generated/abac.auto.tfvars").read_text())
    used = {p["function_name"] for p in cfg["fgac_policies"]}
    assert used and all(f"FUNCTION {name}(" in sql for name in used)
    _coverage_gate(env_dir)


def test_generate_never_prunes_a_function_that_was_already_in_the_sql(env_dir, monkeypatch, capfd):
    _generate_response(env_dir, monkeypatch, _response_with())
    sql_path = env_dir / "generated/masking_functions.sql"
    hand_written = ("\nCREATE OR REPLACE FUNCTION mask_hand_written(val STRING)\n"
                    "RETURNS STRING\nRETURN '***';\n")
    sql_path.write_text(sql_path.read_text() + hand_written)
    capfd.readouterr()

    # The model re-drafts it too (unused either way): it stays because it predates this run.
    _generate_response(env_dir, monkeypatch, _response_with(hand_written + UNUSED_FN))
    out = capfd.readouterr().out
    sql = sql_path.read_text()
    assert "mask_hand_written" in sql
    assert "mask_unused_draft" not in sql
    assert "no policy uses: mask_unused_draft\n" in out


def test_prune_keeps_referenced_unparseable_and_reviewed_functions(tmp_path):
    abac = tmp_path / "abac.auto.tfvars"
    abac.write_text('fgac_policies = [{ name = "p", function_name = "Mask_Used" }]\n')
    sql = tmp_path / "masking_functions.sql"
    sql.write_text(
        "USE CATALOG c;\nUSE SCHEMA s;\n\n"
        "CREATE OR REPLACE FUNCTION mask_used(v STRING) RETURNS STRING RETURN v;\n\n"
        "-- old\nCREATE OR REPLACE FUNCTION mask_reviewed(v STRING) RETURNS STRING RETURN v;\n\n"
        "-- new\nCREATE OR REPLACE FUNCTION mask_new(v STRING) RETURNS STRING RETURN 'a;b';\n"
        "USE SCHEMA t;\n\n"
        "CREATE OR REPLACE FUNCTION mask_broken(v STRING) RETURNS STRING RETURN 'unterminated\n"
    )
    assert prune_unused_new_functions(sql, abac, {"mask_reviewed"}) == ["mask_new"]
    text = sql.read_text()
    assert "mask_new" not in text and "-- new" not in text and "'a;b'" not in text
    # The context it carried for later functions is kept.
    assert "USE SCHEMA t;" in text
    for name in ("mask_used", "mask_reviewed", "mask_broken"):
        assert f"FUNCTION {name}(" in text
    # An unreadable policy file prunes nothing.
    abac.write_text("fgac_policies = [")
    assert prune_unused_new_functions(sql, abac, set()) == []


# ---------------------------------------------------------------------------
# 2b. Reviewed group descriptions stick on re-run
# ---------------------------------------------------------------------------

def test_rerun_keeps_reviewed_group_descriptions(env_dir, monkeypatch, capfd):
    _generate_response(env_dir, monkeypatch, _response_with())
    abac = env_dir / "generated/abac.auto.tfvars"
    first = hcl2.loads(abac.read_text())["groups"]
    capfd.readouterr()

    _generate_response(env_dir, monkeypatch, _response_with(descriptions=("Everything", "Masked view")))
    out = capfd.readouterr().out
    assert hcl2.loads(abac.read_text())["groups"] == first
    assert "kept the reviewed description of 2 group(s): payments_ops, viewers" in out
    assert "RESULT: PASS" in out

    # --allow-rule-changes accepts the new wording.
    _generate_response(env_dir, monkeypatch, _response_with(descriptions=("Everything", "Masked view")),
                       "--allow-rule-changes")
    groups = hcl2.loads(abac.read_text())["groups"]
    assert groups["payments_ops"]["description"] == "Everything"


def test_group_description_merge_replaces_only_the_string(tmp_path):
    abac = tmp_path / "abac.auto.tfvars"
    abac.write_text('groups = {\n  "ops" = { description = "New \\"ops\\"" }\n  new_group = { description = "n" }\n}\n'
                    'tag_policies = []\n')
    reviewed_text = 'groups = {\n  ops = { description = "Reviewed \\"ops\\" $${x} \\\\ y" }\n  gone = {}\n}\n'
    reviewed = hcl2.loads(reviewed_text)
    assert keep_reviewed_group_descriptions(reviewed, abac) == [
        "  kept the reviewed description of 1 group(s): ops"]
    groups = hcl2.loads(abac.read_text())["groups"]
    assert groups == {"ops": reviewed["groups"]["ops"], "new_group": {"description": "n"}}
    assert '"ops" = { description = "Reviewed \\"ops\\" $${x} \\\\ y" }' in abac.read_text()
    assert keep_reviewed_group_descriptions(reviewed, abac) == []


# ---------------------------------------------------------------------------
# 2c. One result line per autofixed policy
# ---------------------------------------------------------------------------

def _policies_file(path, function_schema):
    path.write_text(
        'fgac_policies = [{\n  name = "mask_email"\n  policy_type = "POLICY_TYPE_COLUMN_MASK"\n'
        f'  function_name = "mask_email"\n  function_catalog = "dev_fin"\n  function_schema = "{function_schema}"\n}}]\n'
    )


def test_autofixed_policy_restored_by_a_reviewed_rule_reports_the_final_function(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(generate_abac, "_POLICY_AUTOFIXES", set())
    sql = tmp_path / "masking_functions.sql"
    sql.write_text("USE CATALOG dev_fin;\nUSE SCHEMA other;\n\n"
                   "CREATE OR REPLACE FUNCTION mask_email(v STRING) RETURNS STRING RETURN v;\n")
    abac = tmp_path / "abac.auto.tfvars"
    _policies_file(abac, "payments")
    assert generate_abac.autofix_invalid_function_refs(abac, sql) == 1
    assert "[AUTOFIX] Fixed function ref in policy 'mask_email'" in capsys.readouterr().out
    fixed = hcl2.loads(abac.read_text())

    # A reviewed rule then puts the reviewed policy back.
    _policies_file(abac, "payments")
    reviewed = hcl2.loads(abac.read_text())
    assert generate_abac.policy_autofix_results(abac, reviewed) == [
        "  result: policy 'mask_email' uses dev_fin.payments.mask_email in abac.auto.tfvars "
        "(reviewed rule kept; the autofix above was discarded)"]
    # Without a reviewed rule the autofix stands.
    _policies_file(abac, "other")
    assert generate_abac.policy_autofix_results(abac, {}) == [
        "  result: policy 'mask_email' uses dev_fin.other.mask_email in abac.auto.tfvars "
        "(the autofix above applies)"]
    assert hcl2.loads(abac.read_text()) == fixed
    abac.write_text("fgac_policies = []\n")
    assert generate_abac.policy_autofix_results(abac, reviewed) == [
        "  result: policy 'mask_email' is not in abac.auto.tfvars (removed by the autofix above)"]


# ---------------------------------------------------------------------------
# 3. masking_functions replacement note after data_access plan/apply
# ---------------------------------------------------------------------------

def _plan(*actions, destroy=None):
    body = [f"  # {address} {action}" for address, action in actions]
    count = len(actions) if destroy is None else destroy
    return body + [f"Plan: {count} to add, 0 to change, {count} to destroy."]


MASKING = "module.data_access.terraform_data.masking_functions[0]"


@pytest.mark.parametrize("lines, expected", [
    (_plan((MASKING, "must be replaced")), True),
    (["\x1b[1m  # " + MASKING + "\x1b[0m must be \x1b[1m\x1b[31mreplaced\x1b[0m",
      "\x1b[1mPlan:\x1b[0m 1 to add, 0 to change, 1 to destroy."], True),
    (_plan((MASKING, "is tainted, so must be replaced")), True),
    (_plan((MASKING, "must be replaced"), ("module.data_access.databricks_grant.x", "will be destroyed")), False),
    (_plan((MASKING, "must be replaced"), ("module.data_access.databricks_policy_info.p", "must be replaced")), False),
    (_plan((MASKING, "will be destroyed")), False),
    (_plan((MASKING, "must be replaced"), destroy=2), False),
    (["No changes. Your infrastructure matches the configuration."], False),
])
def test_note_only_when_every_destroy_replaces_masking_functions(lines, expected):
    assert needs_note(lines) is expected


def _fake_terraform(tmp_path, plan_output, rc=0):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    out = tmp_path / "plan.txt"
    out.write_text(plan_output)
    fake = bin_dir / "terraform"
    fake.write_text(f'#!/bin/sh\n[ "$1" = init ] && exit 0\ncat "{out}"\nexit {rc}\n')
    fake.chmod(0o755)
    return {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"}


@pytest.mark.parametrize("command", ["plan", "apply"])
@pytest.mark.parametrize("rc", [0, 1])
def test_layer_runner_passes_output_through_and_adds_the_note(tmp_path, command, rc):
    plan = (f"Terraform will perform the following actions:\n\n"
            f"  # {MASKING} must be replaced\n-/+ resource \"terraform_data\" \"masking_functions\" {{\n    }}\n\n"
            "Plan: 1 to add, 0 to change, 1 to destroy.\n")
    env = _fake_terraform(tmp_path, plan, rc)
    env["LAYER_ENV_DIR"] = str(tmp_path / "env")
    result = subprocess.run([str(RUNNER), "data_access", "dev", command], env=env, text=True, capture_output=True)
    assert result.returncode == rc
    # Terraform's own output is passed through byte for byte.
    assert plan in result.stdout
    assert result.stdout.rstrip().endswith(NOTE)


def test_layer_runner_adds_no_note_for_other_destroys(tmp_path):
    plan = ("  # module.data_access.databricks_grant.g will be destroyed\n"
            "Plan: 0 to add, 0 to change, 1 to destroy.\n")
    env = _fake_terraform(tmp_path, plan)
    env["LAYER_ENV_DIR"] = str(tmp_path / "env")
    result = subprocess.run([str(RUNNER), "data_access", "dev", "plan"], env=env, text=True, capture_output=True)
    assert result.returncode == 0
    assert plan in result.stdout
    assert "Note: replacing" not in result.stdout


# ---------------------------------------------------------------------------
# 5. Retired business_access_enabled in envs/account
# ---------------------------------------------------------------------------

def _account_plan(tmp_path, env_file, environ=None):
    account = tmp_path / "envs/account"
    account.mkdir(parents=True)
    (account / "env.auto.tfvars").write_text(env_file)
    runner = tmp_path / "runner"
    runner.write_text('#!/bin/sh\necho "runner $*"\n')
    runner.chmod(0o755)
    env = {k: v for k, v in os.environ.items()
           if k not in ("MAKEFLAGS", "MAKELEVEL", "APPLY_FLAGS", "TF_VAR_business_access_enabled")}
    env.update(environ or {})
    return subprocess.run(
        ["make", "--no-print-directory", "plan", "ENV=account", f"CLOUD_ROOT={tmp_path}",
         f"SHARED_ROOT={SHARED}", f"ROOT_RUNNER={runner}"],
        cwd=SHARED.parent / "aws", text=True, capture_output=True, env=env,
    )


@pytest.mark.parametrize("value", ["true", "false"])
def test_account_env_with_the_retired_flag_warns_once_and_does_not_fail(tmp_path, value):
    result = _account_plan(tmp_path, f"business_access_enabled = {value}\n")
    assert result.returncode == 0, result.stdout + result.stderr
    warnings = [line for line in (result.stdout + result.stderr).splitlines() if "business_access_enabled" in line]
    assert warnings == [
        "WARNING: business_access_enabled (envs/account/env.auto.tfvars) is deprecated and ignored "
        "(business access follows the coverage check; setting it false does not revoke access). "
        "Remove it; to withdraw access, remove the groups or acl_groups entries."]
    assert (tmp_path / "envs/account/env.auto.tfvars").read_text() == f"business_access_enabled = {value}\n"


def test_account_env_without_the_flag_is_quiet_even_with_tf_var_set(tmp_path):
    # The account root never reads TF_VAR_business_access_enabled; the workspace guard reports it.
    result = _account_plan(tmp_path, "", environ={"TF_VAR_business_access_enabled": "true"})
    assert result.returncode == 0, result.stdout + result.stderr
    assert "business_access_enabled" not in result.stdout + result.stderr


def test_account_warning_runs_once_per_plan_apply_and_apply_governance():
    makefile = (SHARED / "Makefile.shared").read_text()
    assert makefile.count("$(_WARN_ACCOUNT_RETIRED_FLAG);") == 4
    guard = makefile[makefile.index("\n_guard-account-config:"):]
    guard = guard[:guard.index("\n\n")]
    # Not in the guard itself: sync-tags and wait-tag-policies call it on every apply.
    assert "warn-retired-flag" not in guard
