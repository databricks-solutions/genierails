"""Promoted ABAC policy names carry the destination catalog, never the source.

The data_access module keys ``databricks_policy_info.policies`` by the policy
``name`` and names the remote policy ``<catalog>_<name>``. Derived masks are
named ``gr_mask_<catalog>_<treatment>`` at generate time; promote rewrites that
catalog token. A policy the destination already has keeps its name, because a
rename is a delete plus a create in Terraform (and provider 1.111 cannot rename
in place).
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import hcl2
import pytest

SHARED = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED))
sys.path.insert(0, str(SHARED / "scripts"))

from generate_abac import _render_fgac_policy_block  # noqa: E402
from remap_generated_config import (  # noqa: E402
    load_deployed_policy_names,
    remap_hcl,
    remap_policy_name,
    remap_policy_names,
)
from treatment_derivation import derive_treatment_model, load_treatment_config  # noqa: E402

REMAP = SHARED / "scripts" / "remap_generated_config.py"
SPLIT = SHARED / "scripts" / "split_abac_config.py"
ROOT = SHARED / "roots" / "data_access"

DEV_1, DEV_2 = "genierails_dev_1", "genierails_dev_2"
PROD_1, PROD_2 = "genierails_prod_1", "genierails_prod_2"
MAPS = ["--map", f"{DEV_1}={PROD_1}", "--map", f"{DEV_2}={PROD_2}"]


def _dev_generated_config() -> dict:
    """A dev draft after treatment derivation, spanning two catalogs."""
    cfg = {
        "tag_policies": [
            {"key": "pii_level", "values": ["redacted_address"]},
            {"key": "pci_level", "values": ["redacted_cvv"]},
        ],
        "tag_assignments": [
            {"entity_type": "columns", "entity_name": f"{DEV_1}.payments.payments.address",
             "tag_key": "pii_level", "tag_value": "redacted_address"},
            {"entity_type": "columns", "entity_name": f"{DEV_2}.cards.cards.cvv",
             "tag_key": "pci_level", "tag_value": "redacted_cvv"},
        ],
        "fgac_policies": [
            {"name": "mask_address", "policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": DEV_1,
             "to_principals": ["analysts"], "match_condition": "hasTagValue('pii_level', 'redacted_address')",
             "function_name": "mask_redact", "function_schema": "payments"},
            {"name": "mask_cvv", "policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": DEV_2,
             "to_principals": ["analysts"], "match_condition": "hasTagValue('pci_level', 'redacted_cvv')",
             "function_name": "mask_redact", "function_schema": "cards"},
            {"name": f"region_filter_{DEV_1}", "policy_type": "POLICY_TYPE_ROW_FILTER", "catalog": DEV_1,
             "to_principals": ["analysts"], "when_condition": "hasTag('region')",
             "function_name": "filter_region", "function_catalog": DEV_1, "function_schema": "payments"},
        ],
    }
    derived, _ = derive_treatment_model(cfg, load_treatment_config())
    return derived


def _render(cfg: dict) -> str:
    blocks = ",\n".join(_render_fgac_policy_block(p) for p in cfg["fgac_policies"])
    return "tag_assignments = []\n\nfgac_policies = [\n" + blocks + ",\n]\n"


def _promote(tmp_path: Path, source_text: str, *deployed: Path) -> tuple[subprocess.CompletedProcess, Path]:
    src = tmp_path / "dev" / "generated"
    src.mkdir(parents=True, exist_ok=True)
    (src / "abac.auto.tfvars").write_text(source_text)
    out = tmp_path / "prod" / "generated" / "abac.auto.tfvars"
    flags = [arg for path in deployed for arg in ("--deployed", str(path))]
    result = subprocess.run(
        [sys.executable, str(REMAP), str(src / "abac.auto.tfvars"), str(src / "missing.sql"),
         str(out), str(out.with_name("masking_functions.sql")), *MAPS, *flags],
        text=True, capture_output=True,
    )
    return result, out


def _policies(path: Path) -> list[dict]:
    return hcl2.loads(path.read_text())["fgac_policies"]


def _remote_names(policies: list[dict]) -> list[str]:
    # Mirrors modules/data_access: name = "${catalog}_${key}".
    return [f"{p['catalog']}_{p['name']}" for p in policies]


def _legacy_prod_config(path: Path, names: list[str]) -> Path:
    """What a pre-fix promote left in prod: dev-catalog names on prod catalogs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    blocks = ",\n".join(
        f'  {{\n    name = "{name}"\n    policy_type = "POLICY_TYPE_COLUMN_MASK"\n'
        f'    catalog = "{PROD_1}"\n    to_principals = ["analysts"]\n'
        f'    function_name = "mask_redact"\n    function_catalog = "{PROD_1}"\n'
        f'    function_schema = "payments"\n  }}'
        for name in names
    )
    path.write_text("fgac_policies = [\n" + blocks + "\n]\n")
    return path


def test_generate_keeps_catalog_scoped_derived_mask_names():
    # Generate-time names are unchanged, so dev deployments and the reviewed-rule
    # merge (matched by name) see no rename.
    names = {p["name"] for p in _dev_generated_config()["fgac_policies"]}
    assert f"gr_mask_{DEV_1}_redact" in names
    assert f"gr_mask_{DEV_2}_redact" in names


def test_generate_then_promote_names_carry_no_dev_catalog(tmp_path):
    result, out = _promote(tmp_path, _render(_dev_generated_config()))
    assert result.returncode == 0, result.stdout + result.stderr

    policies = _policies(out)
    names = [p["name"] for p in policies]
    remote = _remote_names(policies)
    assert not [n for n in names + remote if "_dev_" in n], remote
    assert f"gr_mask_{PROD_1}_redact" in names
    assert f"gr_mask_{PROD_2}_redact" in names
    assert f"region_filter_{PROD_1}" in names
    assert f"{PROD_1}_gr_mask_{PROD_1}_redact" in remote
    # Unique per env (Terraform key) and per catalog (remote name).
    assert len(set(names)) == len(names)
    assert len(set(remote)) == len(remote)


def test_promote_renames_only_the_policys_own_catalog_token():
    assert remap_policy_name("gr_mask_cat_v2_redact", "cat_v2", "p2") == "gr_mask_p2_redact"
    assert remap_policy_name("gr_mask_xcat_redact", "cat", "prod") == "gr_mask_xcat_redact"
    assert remap_policy_name("cat_mask", "cat", "prod") == "prod_mask"
    assert remap_policy_name("mask_pii", "cat", "prod") == "mask_pii"


def test_policy_already_deployed_in_prod_keeps_its_name(tmp_path):
    legacy = f"gr_mask_{DEV_1}_redact"
    deployed = _legacy_prod_config(tmp_path / "prod" / "data_access" / "abac.auto.tfvars", [legacy])

    result, out = _promote(tmp_path, _render(_dev_generated_config()), deployed)
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"Keeping deployed policy name {PROD_1}_{legacy}" in result.stdout

    names = [p["name"] for p in _policies(out)]
    assert legacy in names
    assert f"gr_mask_{PROD_1}_redact" not in names, "kept and renamed copies must not both land"
    # Policies prod does not have yet get the prod catalog.
    assert f"gr_mask_{PROD_2}_redact" in names
    assert f"region_filter_{PROD_1}" in names

    # Stable across re-promotes: the kept name stays, renamed ones stay renamed.
    again, out_again = _promote(tmp_path, _render(_dev_generated_config()), deployed, out)
    assert again.returncode == 0, again.stdout + again.stderr
    assert sorted(p["name"] for p in _policies(out_again)) == sorted(names)


def test_terraform_state_alone_marks_a_policy_deployed(tmp_path):
    legacy = f"gr_mask_{DEV_1}_redact"
    state = tmp_path / "terraform.tfstate"
    state.write_text(json.dumps({"version": 4, "resources": [{
        "module": "module.data_access", "mode": "managed",
        "type": "databricks_policy_info", "name": "policies",
        "instances": [{"index_key": legacy, "attributes": {"on_securable_fullname": PROD_1}}],
    }]}))
    assert load_deployed_policy_names([state, tmp_path / "absent.tfvars"]) == {(PROD_1, legacy)}

    result, out = _promote(tmp_path, _render(_dev_generated_config()), state)
    assert result.returncode == 0, result.stdout + result.stderr
    assert legacy in [p["name"] for p in _policies(out)]


def test_unreadable_deployed_evidence_fails_closed(tmp_path):
    bad = tmp_path / "abac.auto.tfvars"
    bad.write_text("fgac_policies = [ {\n")
    result, out = _promote(tmp_path, _render(_dev_generated_config()), bad)
    assert result.returncode == 1
    assert "Cannot parse" in result.stdout
    assert not out.exists()


def test_promoted_names_must_stay_unique():
    source = (
        'fgac_policies = [\n'
        f'  {{\n    name = "gr_mask_{DEV_1}_redact"\n    catalog = "{DEV_1}"\n  }},\n'
        f'  {{\n    name = "gr_mask_{PROD_1}_redact"\n    catalog = "{PROD_1}"\n  }},\n'
        ']\n'
    )
    pairs = [(DEV_1, PROD_1)]
    with pytest.raises(ValueError, match="share a name"):
        remap_policy_names(source, remap_hcl(source, pairs), pairs)


def test_promote_passes_destination_evidence_to_the_remap():
    makefile = (SHARED / "Makefile.shared").read_text()
    body = makefile[makefile.index("remap_generated_config.py\" \\"):]
    body = body[: body.index("derive_genie_acls.py")]
    assert '--deployed "$$dest_env_dir/$(DATA_ACCESS_SUBDIR)/abac.auto.tfvars"' in body
    assert '--deployed "$$dest_env_dir/$(DATA_ACCESS_SUBDIR)/terraform.tfstate"' in body
    assert '--deployed "$$dest_env_dir/generated/abac.auto.tfvars"' in body


# ---------------------------------------------------------------------------
# Plan level: the real data_access root and provider, against a seeded state
# that holds the live prod policy under its pre-fix name. -refresh=false keeps
# the plan offline.
# ---------------------------------------------------------------------------

_SEEDED_ADDRESS = 'module.data_access.databricks_policy_info.policies["{key}"]'


def _seed_state(path: Path, key: str) -> None:
    attributes = {
        "id": "seeded", "name": f"{PROD_1}_{key}",
        "on_securable_type": "CATALOG", "on_securable_fullname": PROD_1,
        "policy_type": "POLICY_TYPE_COLUMN_MASK", "for_securable_type": "TABLE",
        "to_principals": ["account users"], "except_principals": None, "comment": "",
        "when_condition": None,
        "match_columns": [{"condition": "hasTagValue('gr_treatment', 'redact')", "alias": "gr_treatment_redact"}],
        "column_mask": {"function_name": f"{PROD_1}.payments.mask_redact",
                        "on_column": "gr_treatment_redact", "using": []},
        "row_filter": None, "created_at": 1, "created_by": "seed",
        "updated_at": 1, "updated_by": "seed", "provider_config": None,
    }
    path.write_text(json.dumps({
        "version": 4, "terraform_version": "1.11.4", "serial": 1, "lineage": "seed", "outputs": {},
        "resources": [{
            "module": "module.data_access", "mode": "managed",
            "type": "databricks_policy_info", "name": "policies",
            "provider": 'provider["registry.terraform.io/databricks/databricks"].workspace',
            "instances": [{"index_key": key, "schema_version": 0, "attributes": attributes}],
        }],
    }))


def _policy_plan(tmp_path: Path, generated: Path) -> dict[str, tuple[list[str], str | None, str | None]]:
    env = tmp_path / "plan_env"
    (env / "data_access").mkdir(parents=True)
    split = subprocess.run(
        [sys.executable, str(SPLIT), str(generated), str(env / "account.auto.tfvars"),
         str(env / "data_access" / "abac.auto.tfvars"), str(env / "abac.auto.tfvars")],
        text=True, capture_output=True,
    )
    assert split.returncode == 0, split.stdout + split.stderr
    da = env / "data_access"
    (da / "masking_functions.sql").write_text("SELECT 1;\n")
    (da / "auth.auto.tfvars").write_text(
        'databricks_account_id = "account"\ndatabricks_client_id = "sp"\n'
        'databricks_client_secret = "secret"\n'
        'databricks_workspace_host = "https://example.invalid"\n'
        'sql_warehouse_id = "warehouse"\n'
        f'uc_tables = ["{PROD_1}.payments.payments"]\n'
    )
    _seed_state(da / "terraform.tfstate", f"gr_mask_{DEV_1}_redact")

    tf_env = {**os.environ, "TF_DATA_DIR": str(da / ".terraform"), "TF_IN_AUTOMATION": "1"}
    init = subprocess.run(
        ["terraform", "init", "-input=false",
         f"-backend-config=path={da / 'terraform.tfstate'}"],
        cwd=ROOT, env=tf_env, text=True, capture_output=True,
    )
    assert init.returncode == 0, init.stdout + init.stderr
    plan_file = da / "plan.bin"
    plan = subprocess.run(
        ["terraform", "plan", "-refresh=false", "-lock=false", "-input=false", "-no-color",
         f"-out={plan_file}", f"-var=env_dir={da}",
         f"-var-file={da / 'auth.auto.tfvars'}", f"-var-file={da / 'abac.auto.tfvars'}"],
        cwd=ROOT, env=tf_env, text=True, capture_output=True,
    )
    assert plan.returncode == 0, plan.stdout + plan.stderr
    shown = subprocess.run(
        ["terraform", "show", "-json", str(plan_file)],
        cwd=ROOT, env=tf_env, text=True, capture_output=True,
    )
    assert shown.returncode == 0, shown.stderr
    return {
        rc["address"]: (
            rc["change"]["actions"],
            (rc["change"].get("before") or {}).get("name"),
            (rc["change"].get("after") or {}).get("name"),
        )
        for rc in json.loads(shown.stdout)["resource_changes"]
        if rc["type"] == "databricks_policy_info"
    }


@pytest.fixture
def legacy_prod(tmp_path):
    legacy = f"gr_mask_{DEV_1}_redact"
    deployed = _legacy_prod_config(tmp_path / "prod" / "data_access" / "abac.auto.tfvars", [legacy])
    return legacy, deployed


def test_migration_plan_never_deletes_or_renames_a_live_mask(tmp_path, legacy_prod):
    legacy, deployed = legacy_prod
    result, out = _promote(tmp_path, _render(_dev_generated_config()), deployed)
    assert result.returncode == 0, result.stdout + result.stderr

    changes = _policy_plan(tmp_path, out)
    live = _SEEDED_ADDRESS.format(key=legacy)
    actions, before, after = changes[live]
    # Principals may converge in place; the remote name must not move.
    assert actions in (["no-op"], ["update"]), actions
    assert before == after == f"{PROD_1}_{legacy}"
    for address, (actions, before, after) in changes.items():
        assert "delete" not in actions, address
        assert actions in (["no-op"], ["update"], ["create"]), (address, actions)
        if actions == ["update"]:
            assert before == after, (address, before, after)
        if address != live:
            assert "_dev_" not in after, after


def test_without_destination_evidence_the_same_promote_would_drop_the_live_mask(tmp_path):
    # Counterfactual: why promote reads the destination. A renamed key is a
    # delete of the live mask plus a create, with no ordering guarantee.
    result, out = _promote(tmp_path, _render(_dev_generated_config()))
    assert result.returncode == 0, result.stdout + result.stderr

    changes = _policy_plan(tmp_path, out)
    assert changes[_SEEDED_ADDRESS.format(key=f"gr_mask_{DEV_1}_redact")][0] == ["delete"]
