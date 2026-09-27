from pathlib import Path


SHARED = Path(__file__).parents[1]
MODULE_MAIN = SHARED / "modules/data_access/main.tf"
MODULE_VARIABLES = SHARED / "modules/data_access/variables.tf"
ROOT_MAIN = SHARED / "roots/data_access/main.tf"
MAKEFILE = SHARED / "Makefile.shared"
VALIDATOR = SHARED / "scripts/validate_classification_config.py"


def test_classification_is_opt_in_and_forwarded_by_the_root():
    variables = MODULE_VARIABLES.read_text()
    start = variables.index('variable "enable_classification"')
    body = variables[start : variables.index("}\n", start) + 2]

    assert "default     = false" in body
    assert "enable_classification" in ROOT_MAIN.read_text()
    assert "= var.enable_classification" in ROOT_MAIN.read_text()


def test_classification_is_scoped_to_governed_uc_schemas():
    source = MODULE_MAIN.read_text()

    assert "var.enable_classification ? local.classification_catalog_schemas : {}" in source
    assert 'parent   = "catalogs/${each.key}"' in source
    assert "names = each.value" in source
    assert "distinct(local._uc_catalogs)" in source
    assert "classification_existing_schemas" in source


def test_classification_plan_enables_auto_tagging_for_champion_types():
    source = MODULE_MAIN.read_text()

    expected = {
        "class.card_security_code",
        "class.credit_card",
        "class.date_of_birth",
        "class.email_address",
        "class.name",
        "class.phone_number",
        "class.us_ssn",
    }
    assert all(f'"{tag}"' in source for tag in expected)
    assert "auto_tag_configs = [" in source
    assert 'auto_tagging_mode  = "AUTO_TAGGING_ENABLED"' in source


def test_enable_classification_target_is_a_classification_only_apply():
    source = MAKEFILE.read_text()
    start = source.index("enable-classification:")
    body = source[start : source.index("\ngenerate:", start)]

    assert "_bootstrap _guard-workspace-target" in body
    assert "validate_classification_config.py" in body
    assert "-target=module.data_access.databricks_data_classification_catalog_config.classification" in body
    assert "abac.auto.tfvars" not in body
    assert "masking_functions.sql" not in body
    assert "_apply-layer" not in body


def test_data_access_root_includes_space_tables_in_classification_footprint():
    source = ROOT_MAIN.read_text()

    assert 'variable "genie_spaces"' in source
    assert "flatten([for space in var.genie_spaces : space.uc_tables])" in source
    assert "for t in local.classification_uc_tables" in source


def test_classification_validator_requires_opt_in_and_accepts_space_footprint(tmp_path):
    import subprocess
    import sys

    config = tmp_path / "env.auto.tfvars"
    config.write_text('genie_spaces = [{ uc_tables = ["cat.schema.table"] }]\n')
    disabled = subprocess.run([sys.executable, VALIDATOR, config], capture_output=True, text=True)
    assert disabled.returncode == 1
    assert "enable_classification = true" in disabled.stderr

    config.write_text(
        'enable_classification = true\n'
        'genie_spaces = [{ uc_tables = ["cat.schema.table"] }]\n'
    )
    enabled = subprocess.run([sys.executable, VALIDATOR, config], capture_output=True, text=True)
    assert enabled.returncode == 0
