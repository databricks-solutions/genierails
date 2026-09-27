from pathlib import Path


SHARED = Path(__file__).parents[1]
MODULE_MAIN = SHARED / "modules/data_access/main.tf"
MODULE_VARIABLES = SHARED / "modules/data_access/variables.tf"
ROOT_MAIN = SHARED / "roots/data_access/main.tf"


def test_classification_is_opt_in_and_forwarded_by_the_root():
    variables = MODULE_VARIABLES.read_text()
    start = variables.index('variable "enable_classification"')
    body = variables[start : variables.index("}\n", start) + 2]

    assert "default     = false" in body
    assert "enable_classification     = var.enable_classification" in ROOT_MAIN.read_text()


def test_classification_is_scoped_to_governed_uc_schemas():
    source = MODULE_MAIN.read_text()

    assert "var.enable_classification ? local.classification_catalog_schemas : {}" in source
    assert 'parent   = "catalogs/${each.key}"' in source
    assert "names = each.value" in source
    assert "distinct(local._uc_catalogs)" in source


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
