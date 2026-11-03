"""Old code applies a legacy env; the new code migrates it, with real Terraform and mock providers.

The pre-migration data_access module (BASE_COMMIT, from git history) applies
a legacy deployment: a gr_mask_<catalog>_<value> mask on a value-keyed
gr_treatment tag, and SELECT grants. Then the current module, with the moves
scripts/migration.py generates for that state rendered as ``moved`` blocks
(the cutover runs the same pairs as ``terraform state mv``), applies the
staged phases its controller renders (create, switch, grant) on the very same
state, and a later release plans the retire phase.

Every phase destroys or replaces no grant, policy or tag. The tag keeps its
legacy value at its stable key until the switch updates it in place, the new
policies exist before it, and the legacy policy stays until retire: no column
is ever unmasked or doubly masked. An env that never opts in plans no change.
"""

from __future__ import annotations

import io
import json
import os
import re
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest

from governance_policies import build_model
from scripts import migration
from scripts import policy_generations as pg
from scripts.staged_policy_apply import _hcl_value
from tests.terraform_helpers import MASKING_REKEY, is_masking_rekey, tf, tf_env, tf_init

ROOT = Path(__file__).parents[2]
SHARED = ROOT / "shared"
# origin/main before the migration (rollout steps 1-9): what legacy envs run today.
BASE_COMMIT = "7042a76"
TIERS = ["raw", "regional", "masked"]
COLUMN = "cat.sch.customers.email"
TABLE = "cat.sch.customers"
LEGACY_VALUE = "email_partial"
LEGACY_POLICY = {
    "name": f"gr_mask_cat_{LEGACY_VALUE}", "policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": "cat",
    "to_principals": ["regional", "masked"], "comment": f"GenieRails derived treatment mask: {LEGACY_VALUE}",
    "match_condition": f"hasTagValue('gr_treatment', '{LEGACY_VALUE}')", "match_alias": f"gr_treatment_{LEGACY_VALUE}",
    "function_name": "mask_email", "function_catalog": "cat", "function_schema": "sch",
}
LEGACY_TAG = {"entity_type": "columns", "entity_name": COLUMN, "tag_key": "gr_treatment", "tag_value": LEGACY_VALUE}
GRANTS = [f"{TABLE}|masked", f"{TABLE}|regional"]
PROTECTED = ("databricks_policy_info.", "databricks_entity_tag_assignment.", "databricks_grant.",
             "terraform_data.masking_functions_drop")
CHANGE = re.compile(r"# (\S+) (will be (?:created|destroyed|updated in-place|read during apply)|must be replaced)")
MOCK_COLUMN_MASK_USING_NULL_REKEY = {
    ('databricks_policy_info.policies["gr_mask_cat_email_partial"]', "will be updated in-place"),
}

pytestmark = pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")


def legacy_state(tmp_path: Path) -> Path:
    """The state the legacy apply leaves, as scripts/migration.py reads it (provider attribute shapes)."""
    env_dir = tmp_path / "envs" / "prod"
    (env_dir / "data_access").mkdir(parents=True)
    policy = LEGACY_POLICY
    attrs = {
        "name": f"cat_{policy['name']}", "on_securable_fullname": "cat", "policy_type": policy["policy_type"],
        "to_principals": policy["to_principals"], "except_principals": None, "comment": policy["comment"],
        "column_mask": {"function_name": "cat.sch.mask_email", "on_column": policy["match_alias"], "using": None},
        "match_columns": [{"condition": policy["match_condition"], "alias": policy["match_alias"]}],
        "when_condition": None, "row_filter": None,
    }
    key = "|".join(LEGACY_TAG.values())

    def resource(rtype, name, instances):
        return {"module": "module.data_access", "mode": "managed", "type": rtype, "name": name,
                "instances": [{"index_key": k, "attributes": a} for k, a in instances.items()]}
    (env_dir / "data_access" / "terraform.tfstate").write_text(json.dumps({"version": 4, "resources": [
        resource("databricks_policy_info", "policies", {policy["name"]: attrs}),
        resource("databricks_entity_tag_assignment", "assignments", {key: LEGACY_TAG}),
        resource("databricks_grant", "table_access", {g: {} for g in GRANTS}),
    ]}))
    return env_dir


def renders(env_dir: Path) -> tuple[dict, list[tuple[str, str]]]:
    """Each phase's render (what the controller writes) and the generated tag moves."""
    dep = migration.deployment(env_dir)
    legacy = migration.legacy_generation(dep)
    model = build_model({COLUMN: ["email_address"]}, {COLUMN: "STRING"}, {"access_tier_groups": TIERS})
    desired = {"fingerprint": "deterministic", "fgac_policies": model.fgac_policies,
               "tags": {t["entity_name"]: t["tag_value"] for t in model.tag_assignments},
               "functions_sql": model.functions_sql}
    stages = {
        "create": pg.plan_stage("create", desired, legacy, None),
        "switch": pg.plan_stage("switch", desired, legacy, None),
        # A later release: the deterministic generation is active, the legacy one retires.
        "retire": pg.plan_stage("retire", desired, desired, legacy),
    }
    for name, staged in stages.items():
        assert staged["problems"] == [], (name, staged["problems"])
    assert stages["switch"]["allowed_policy_deletes"] == []  # the legacy policy outlives the switch
    assert stages["retire"]["allowed_policy_deletes"] == [LEGACY_POLICY["name"]]
    return {name: staged["rendered"] for name, staged in stages.items()} | {"desired": desired}, dep["moves"]


def _vars(rendered: dict) -> str:
    tags = [{"entity_type": "columns", "entity_name": c, "tag_key": "gr_treatment", "tag_value": v}
            for c, v in sorted(rendered["tags"].items())]
    policies = [{k: v for k, v in p.items() if k in LEGACY_POLICY or k == "except_principals"}
                for p in rendered["fgac_policies"]]
    return f"tag_assignments = {_hcl_value(tags)}\n    fgac_policies = {_hcl_value(policies)}"


def _test_body(stages: dict) -> str:
    deterministic = (f'governance_mode = "deterministic"\n    access_tier_groups = {_hcl_value(TIERS)}\n'
                     f'    table_readers = {{ "{TABLE}" = ["regional", "masked"] }}')
    # What the root passes from the layer's state; a probe (empty state) has none.
    applied = f"applied_table_grants = {_hcl_value(GRANTS)}"
    desired_value = stages["desired"]["tags"][COLUMN]
    policy_names = sorted(p["name"] for p in stages["desired"]["fgac_policies"])
    providers = ("  providers = { databricks.account = databricks.account, databricks.workspace = "
                 "databricks.workspace, time = time }\n")

    def gate(run: str, probe: str) -> str:
        return f'''
run "{run}" {{
  module {{
    source = "./tests/file_writer"
  }}
  variables {{
    files = {{
      "tests/.tmp/m/ddl/_fetched.sql"                = "CREATE TABLE {TABLE} (\\n  id BIGINT,\\n  email STRING\\n);\\n"
      "tests/.tmp/m/data_access/.coverage_gate.json" = jsonencode({{ status = "pass", fingerprint = run.{probe}.coverage_gate_inputs.fingerprint, refreshed_at = "@NOW@" }})
    }}
  }}
}}
'''

    phase_args: list = []

    def phase(name: str, rendered: dict, *, command: str = "apply", withhold: bool = True, asserts: str = "") -> str:
        phase_args[:] = [name, rendered, withhold, asserts]
        probe = f'''
run "{name}_probe" {{
  state_key = "probe_{name}"
  command   = plan
  module {{
    source = "./migrated/data_access"
  }}
{providers}  variables {{
    {deterministic}
    withhold_new_table_grants = {str(withhold).lower()}
    {_vars(rendered)}
  }}
}}
'''
        # The plan run shows the phase's diff; the apply run (same inputs) moves the state on.
        runs = [(f"{name}_plan", "plan")] + ([(name, command)] if command == "apply" else [])
        return probe + gate(f"{name}_gate", f"{name}_probe") + "".join(body(run, cmd) for run, cmd in runs)

    def body(run: str, command: str) -> str:
        name, rendered, withhold, asserts = phase_args
        return f'''
run "{run}" {{
  state_key = "migrated"
  command   = {command}
  module {{
    source = "./migrated/data_access"
  }}
{providers}  variables {{
    {deterministic}
    {applied}
    withhold_new_table_grants = {str(withhold).lower()}
    {_vars(rendered)}
  }}
  assert {{
    condition     = jsonencode(sort(keys(databricks_grant.table_access))) == jsonencode({_hcl_value(GRANTS)})
    error_message = "every SELECT GenieRails granted stays at its key"
  }}
  assert {{
    condition     = jsonencode(keys(databricks_entity_tag_assignment.treatment)) == jsonencode(["columns|{COLUMN}|gr_treatment"]) && length(databricks_entity_tag_assignment.assignments) == 0
    error_message = "the treatment tag sits at its stable key"
  }}
{asserts}}}
'''

    legacy_vars = f"tag_assignments = {_hcl_value([LEGACY_TAG])}\n    fgac_policies = {_hcl_value([LEGACY_POLICY])}"
    with_legacy = f'''  assert {{
    condition     = contains(keys(databricks_policy_info.policies), "{LEGACY_POLICY['name']}") && alltrue([for n in {_hcl_value(policy_names)} : contains(keys(databricks_policy_info.policies), n)])
    error_message = "the legacy policy and every new policy are live together"
  }}
'''
    return f'''
mock_provider "databricks" {{
  alias = "account"
}}
mock_provider "databricks" {{
  alias = "workspace"
}}
mock_provider "time" {{}}

variables {{
  databricks_account_id     = "account"
  databricks_client_id      = "service-principal"
  databricks_client_secret  = "secret"
  databricks_workspace_host = "https://example.invalid"
  sql_warehouse_id          = "warehouse"
  masking_sql_file          = "tests/fixtures/mask_email.sql"
  deploy_masking_script     = "tests/fixtures/noop_deploy_masking.py"
  auth_file                 = "tests/.tmp/m/data_access/auth.auto.tfvars"
  coverage_gate_file        = "tests/.tmp/m/data_access/.coverage_gate.json"
  coverage_ddl_file         = "tests/.tmp/m/ddl/_fetched.sql"
  groups                    = {{ regional = {{}}, masked = {{}} }}
  uc_tables                 = ["{TABLE}"]
  admin_uc_tables           = ["{TABLE}"]
}}

# The DDL is a gate input: it exists before any probe reads the fingerprint.
run "setup" {{
  module {{
    source = "./tests/file_writer"
  }}
  variables {{
    files = {{
      "tests/.tmp/m/ddl/_fetched.sql"                = "CREATE TABLE {TABLE} (\\n  id BIGINT,\\n  email STRING\\n);\\n"
      "tests/.tmp/m/data_access/.coverage_gate.json" = null
    }}
  }}
}}

run "legacy_probe" {{
  state_key = "probe_legacy"
  command   = plan
  module {{
    source = "./legacy/data_access"
  }}
{providers}  variables {{
    {legacy_vars}
  }}
}}
{gate("legacy_gate", "legacy_probe")}
run "legacy_apply" {{
  state_key = "migrated"
  module {{
    source = "./legacy/data_access"
  }}
{providers}  variables {{
    {legacy_vars}
  }}
  assert {{
    condition     = jsonencode(sort(keys(databricks_grant.table_access))) == jsonencode({_hcl_value(GRANTS)}) && jsonencode(keys(databricks_entity_tag_assignment.assignments)) == jsonencode(["{'|'.join(LEGACY_TAG.values())}"])
    error_message = "the legacy apply must have tagged the column and granted SELECT"
  }}
}}

run "untouched_legacy_apply" {{
  state_key = "untouched"
  module {{
    source = "./legacy/data_access"
  }}
{providers}  variables {{
    {legacy_vars}
  }}
}}

# An env that never opts in: the current code plans no change.
run "untouched_replan" {{
  state_key = "untouched"
  command   = plan
{providers}  variables {{
    {legacy_vars}
  }}
}}
{phase("create", stages["create"], asserts=with_legacy + f"""  assert {{
    condition     = databricks_entity_tag_assignment.treatment["columns|{COLUMN}|gr_treatment"].tag_value == "{LEGACY_VALUE}"
    error_message = "before the switch the column keeps its legacy value (and its legacy mask)"
  }}
""")}
{phase("switch", stages["switch"], asserts=with_legacy + f"""  assert {{
    condition     = databricks_entity_tag_assignment.treatment["columns|{COLUMN}|gr_treatment"].tag_value == "{desired_value}"
    error_message = "the switch retags the column in place"
  }}
""")}
{phase("grant", stages["switch"], withhold=False, asserts=with_legacy)}
{phase("retire", stages["retire"], command="plan", withhold=False, asserts=f"""  assert {{
    condition     = !contains(keys(databricks_policy_info.policies), "{LEGACY_POLICY['name']}") && jsonencode(sort(keys(databricks_policy_info.policies))) == jsonencode({_hcl_value(policy_names)})
    error_message = "a later release retires only the legacy policy"
  }}
""")}
'''


def _base_modules(tmp_path: Path) -> Path:
    present = subprocess.run(["git", "cat-file", "-e", f"{BASE_COMMIT}^{{commit}}"], cwd=ROOT, capture_output=True)
    if present.returncode != 0:
        message = f"base commit {BASE_COMMIT} is not in this clone (shallow checkout?); fetch full history"
        if os.environ.get("REQUIRE_TERRAFORM_TESTS") == "1":
            pytest.fail(message)
        pytest.skip(message)
    archive = subprocess.run(["git", "archive", "--format=tar", BASE_COMMIT, "shared/modules"],
                             cwd=ROOT, capture_output=True, check=True)
    with tarfile.open(fileobj=io.BytesIO(archive.stdout)) as tar:
        tar.extractall(tmp_path / "base", filter="data")
    return tmp_path / "base" / "shared" / "modules"


def _moved_blocks(moves: list[tuple[str, str]]) -> str:
    """The generated state moves as moved blocks of the data_access module itself."""
    def local(address: str) -> str:
        return address.removeprefix("module.data_access.")
    return "".join(f"moved {{\n  from = {local(s)}\n  to   = {local(t)}\n}}\n" for s, t in moves)


def _sections(output: str) -> dict[str, str]:
    sections: dict[str, str] = {}
    current = None
    for line in output.splitlines():
        match = re.match(r'\s*run "([^"]+)"\.\.\. (\w+)', line)
        if match:
            current = match.group(1)
            sections[current] = match.group(2) + "\n"
        elif current:
            sections[current] += line + "\n"
    return sections


def _destroyed(section: str) -> set[tuple[str, str]]:
    return {(a, c) for a, c in CHANGE.findall(section)
            if c in ("will be destroyed", "must be replaced") and a.startswith(PROTECTED)}


def test_old_code_apply_then_new_code_migration_destroys_nothing_and_leaves_no_gap(tmp_path):
    stages, moves = renders(legacy_state(tmp_path))
    assert moves == [(
        f'module.data_access.databricks_entity_tag_assignment.assignments["{"|".join(LEGACY_TAG.values())}"]',
        f'module.data_access.databricks_entity_tag_assignment.treatment["columns|{COLUMN}|gr_treatment"]')]
    current = tmp_path / "current" / "modules"
    shutil.copytree(SHARED / "modules", current, ignore=shutil.ignore_patterns(".terraform", ".tmp", "*.tfstate*"))
    root = current / "data_access"
    for name in ("sql_tokenizer.py", "masking_sql_blocks.py"):
        shutil.copy(SHARED / name, current.parent / name)
        shutil.copy(SHARED / name, root / name)  # parents[2] of the nested copies' normalizer
    shutil.copytree(_base_modules(tmp_path), root / "legacy", ignore=shutil.ignore_patterns("tests"))
    shutil.copytree(SHARED / "modules", root / "migrated", ignore=shutil.ignore_patterns("tests", ".terraform", ".tmp"))
    (root / "migrated" / "data_access" / "migration_moved.tf").write_text(_moved_blocks(moves))
    shutil.copytree(SHARED / "roots/data_access/tests/file_writer", root / "tests/file_writer")
    for existing in (root / "tests").glob("*.tftest.hcl"):
        existing.unlink()
    (root / "tests/migration.tftest.hcl").write_text(_test_body(stages))
    env = tf_env(tmp_path)
    tf_init(root, env=env)
    result = tf(root, "test", "-no-color", "-verbose", env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    runs = _sections(result.stdout)
    assert all(section.startswith("pass") for section in runs.values()), result.stdout

    # Never opted in: nothing changes but the one-time masking re-key (the
    # legacy module recorded checkout paths in masking_functions' triggers).
    # The historical mock-provider apply stores column_mask.using as [].
    # Real-provider refresh returns null and is covered by
    # test_column_mask_using_null_matches_live_null_and_reverts_non_empty_using.
    assert set(CHANGE.findall(runs["untouched_replan"])) == MASKING_REKEY | MOCK_COLUMN_MASK_USING_NULL_REKEY, runs["untouched_replan"]
    assert is_masking_rekey(runs["untouched_replan"]), runs["untouched_replan"]
    # Create: the tag moves to its stable key (no destroy/create), the new policies are added,
    # the legacy policy and every grant are untouched.
    create = runs["create_plan"]
    assert f"{moves[0][0].removeprefix('module.data_access.')} has moved to " \
           f"{moves[0][1].removeprefix('module.data_access.')}" in create, create
    for name in ("create", "switch", "grant"):
        assert _destroyed(runs[f"{name}_plan"]) == set(), (name, runs[f"{name}_plan"])
    # The two new policies, and the one-time masking re-key on this legacy state.
    assert set(CHANGE.findall(runs["create_plan"])) >= MASKING_REKEY and is_masking_rekey(runs["create_plan"])
    assert re.search(r"Plan: 3 to add, [23] to change, 1 to destroy\.", runs["create_plan"])
    assert "Plan: 0 to add, 1 to change, 0 to destroy." in runs["switch_plan"]  # the retag
    assert "No changes." in runs["grant_plan"]
    changes = {phase: dict((a, c) for a, c in CHANGE.findall(runs[f"{phase}_plan"]))
               for phase in ("create", "switch", "grant")}
    legacy_address = f'databricks_policy_info.policies["{LEGACY_POLICY["name"]}"]'
    assert changes["create"].get(legacy_address) in (None, "will be updated in-place"), changes
    assert all(legacy_address not in changes[phase] for phase in ("switch", "grant")), changes
    assert all(not a.startswith("databricks_grant.table_access") for c in changes.values() for a in c), changes
    new_policies = {f'databricks_policy_info.policies["{p["name"]}"]' for p in stages["desired"]["fgac_policies"]}
    assert {a for a, c in changes["create"].items() if c == "will be created"} >= new_policies
    tag = f'databricks_entity_tag_assignment.treatment["columns|{COLUMN}|gr_treatment"]'
    assert tag not in changes["create"]  # keeps its legacy value through create
    assert changes["switch"][tag] == "will be updated in-place"  # retagged in place, never removed
    assert not any(a.startswith(PROTECTED[:2]) for a in changes["grant"]), changes["grant"]
    # A later release removes the legacy policy, and only it.
    assert _destroyed(runs["retire_plan"]) == {(legacy_address, "will be destroyed")}, runs["retire_plan"]
