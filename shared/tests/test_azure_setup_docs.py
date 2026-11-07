"""Docs fixed from the Azure fresh-setup findings (F1-F6): each pins text a fresh setup relies on."""

from pathlib import Path

import hcl2
import pytest

ROOT = Path(__file__).parents[2]
SHARED = ROOT / "shared"
SAMPLE_ENV = (SHARED / "examples/dev_to_prod/SAMPLE_ENV.md").read_text()
PREREQS = (SHARED / "docs/prerequisites.md").read_text()
AZURE_PREREQS = (ROOT / "azure/docs/azure-prerequisites.md").read_text()
AUTH_TEMPLATE = (SHARED / "auth.auto.tfvars.example").read_text()


def _flat(text):
    return " ".join(text.split())


def test_sample_env_rerun_claim_is_scoped_to_the_clone_holding_the_state_file():
    text = _flat(SAMPLE_ENV)
    assert "Re-runs are safe." not in text
    assert "**Re-runs are safe from the same clone.**" in text
    assert "`.dev_to_prod_sample_env.json` at the repo root, a git-ignored file in *this clone*" in text
    assert "copy the earlier clone's `.dev_to_prod_sample_env.json` to the new clone's repo root" in text


def test_sample_env_state_file_name_matches_the_script():
    script = (SHARED / "examples/dev_to_prod/setup_sample_env.py").read_text()
    assert 'STATE_FILE = Path(__file__).resolve().parents[3] / STATE_FILE_NAME' in script
    assert 'LEGACY_STATE_FILE = Path(__file__).with_name(STATE_FILE_NAME)' in script
    # The doc and the error call it git-ignored.
    assert "/.dev_to_prod_sample_env.json" in (ROOT / ".gitignore").read_text().splitlines()
    assert ".dev_to_prod_sample_env.json" not in (SHARED / "examples/dev_to_prod/.gitignore").read_text().splitlines()


def test_prod_sample_command_uses_the_prod_profile_not_host_alone():
    assert ("python setup_sample_env.py --profile <prod-workspace-profile> --catalog prod_finance "
            "--warehouse-id <prod-warehouse-id> --skip-agent") in SAMPLE_ENV
    assert "setup_sample_env.py --host" not in SAMPLE_ENV


def test_auth_template_permissions_match_check_6_not_metastore_admin():
    text = _flat(AUTH_TEMPLATE.replace("#", ""))
    assert "Metastore Admin —" not in text
    assert "Authority over the governed catalog (not Metastore Admin)" in text
    assert "SELECT + MANAGE + APPLY TAG" in text
    assert "Account Tag Policy Creator + Manager" in text
    check_6 = _flat(PREREQS)
    assert "be granted `SELECT` + `MANAGE` + `APPLY TAG`" in check_6
    assert "**Tag Policy Creator + Manager**" in check_6


def test_auth_template_shows_both_cloud_host_forms_and_parses():
    assert "https://adb-<workspace-id>.<n>.azuredatabricks.net/ (Azure)" in AUTH_TEMPLATE
    assert "Azure only: https://accounts.azuredatabricks.net" in AUTH_TEMPLATE
    assert "databricks_account_host" not in hcl2.loads(AUTH_TEMPLATE)


@pytest.mark.parametrize("cloud", ["aws", "azure"])
def test_cloud_readme_role_table_has_no_metastore_admin_row(cloud):
    readme = (ROOT / cloud / "README.md").read_text()
    assert "| **Metastore Admin** |" not in readme
    assert "| Full (default) | **Authority over the governed catalog** (not Metastore Admin) |" in readme


def test_bootstrap_docs_name_sp_name_before_a_duplicate_sp_is_created():
    text = _flat(PREREQS)
    assert "**Already have a deployment SP? Pass its exact display name as `SP_NAME`.**" in text
    assert "or it creates a second SP named `genierails-deployer` and mints a new secret for it" in text
    assert "make bootstrap-sp ACCOUNT_ID=<id> SP_NAME=<deployer-sp-display-name>" in AZURE_PREREQS
    assert "pass `SP_NAME=<its name>` for an existing SP" in (ROOT / "README.md").read_text()


def test_plan_docs_list_what_plan_does_not_check_with_the_scripts_wording():
    script = (SHARED / "scripts/bootstrap_sp.py").read_text()
    assert '"PLAN NOT CHECKED: Account Admin role, account tag-policy creator and manager roles, "' in script
    assert "PLAN RESOLVED service principal: reuse" in script
    text = _flat(PREREQS)
    assert ("It does **not** check the Account Admin role, the tag-policy roles, workspace ADMIN, or "
            "(for a reused SP without `ROTATE_SECRET=1`) whether a secret would be minted; it lists these "
            "on a `PLAN NOT CHECKED:` line") in text
    assert "`PLAN NOT CHECKED:` line lists what it did not check" in _flat(AZURE_PREREQS)


def test_existing_secret_reuse_path_copies_with_0600_instead_of_rotating():
    text = _flat(PREREQS)
    assert "**Reusing an SP that already works elsewhere (a new clone or customer repo)?** Don't rotate its secret." in text
    assert "install -m 0600 <old-repo>/<cloud>/envs/dev/auth.auto.tfvars envs/dev/auth.auto.tfvars" in PREREQS
    assert "Never print or commit it." in text


def test_azure_account_host_is_documented_as_written_by_setup():
    assert "`make setup` in `azure/` writes this line into each new `auth.auto.tfvars`; keep it." in PREREQS
    assert "`make setup` writes this line into each new `auth.auto.tfvars`" in (ROOT / "azure/README.md").read_text()


def test_deterministic_sample_template_does_not_claim_verify_keys_are_saved():
    template = (SHARED / "examples/dev_to_prod/env.auto.tfvars.example").read_text()
    assert 'governance_mode = "deterministic"' in template
    text = _flat(template.replace("#", ""))
    assert "the proven keys are saved here" not in text
    assert "`make rehearse` only prints the proven keys; nothing is saved here." in text
    # Same behavior make setup describes for a deterministic env.
    assert "nothing is saved: committing that line is optional and pins those keys" in (
        SHARED / "Makefile.shared").read_text()
