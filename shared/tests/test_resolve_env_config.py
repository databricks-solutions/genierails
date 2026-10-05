import json
from pathlib import Path

import pytest

from scripts.resolve_env_config import resolve_verify_key, resolve_warehouse

SHARED = Path(__file__).resolve().parent.parent


def test_verify_key_prefers_explicit_then_persisted(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text('verify_key_column = "customer_id"\n')
    assert resolve_verify_key(tmp_path) == "customer_id"
    assert resolve_verify_key(tmp_path, " order_id ") == "order_id"


def test_verify_key_is_declared_in_every_root_that_loads_env_tfvars():
    for root in ("workspace", "data_access"):
        text = (SHARED / "roots" / root / "main.tf").read_text()
        assert 'variable "verify_key_column"' in text


def test_warehouse_resolution_order_and_unique_per_space(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text('''
sql_warehouse_id = ""
genie_spaces = [
  { name = "a", sql_warehouse_id = "space-wh" },
  { name = "b", sql_warehouse_id = "space-wh" },
]
''')
    assert resolve_warehouse(tmp_path) == "space-wh"
    assert resolve_warehouse(tmp_path, "explicit-wh") == "explicit-wh"


def test_unique_per_space_warehouse_ignores_inheriting_empty_spaces(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text('''
genie_spaces = [
  { name = "explicit", sql_warehouse_id = "space-wh" },
  { name = "inherits", sql_warehouse_id = "" },
]
''')
    assert resolve_warehouse(tmp_path) == "space-wh"


def test_warehouse_resolution_fails_on_ambiguity(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text('''
genie_spaces = [
  { name = "a", sql_warehouse_id = "wh-a" },
  { name = "b", sql_warehouse_id = "wh-b" },
]
''')
    with pytest.raises(ValueError, match="ambiguous warehouse"):
        resolve_warehouse(tmp_path)


def _write_state(env_dir, value, *, include_output=True):
    outputs = {}
    if include_output:
        outputs["sql_warehouse_id"] = {"value": value, "type": "string"}
    (env_dir / "terraform.tfstate").write_text(json.dumps({"version": 4, "outputs": outputs}))


def test_warehouse_reads_workspace_output_directly_from_local_state(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text('sql_warehouse_id = ""\n')
    _write_state(tmp_path, "auto-wh")
    assert resolve_warehouse(tmp_path) == "auto-wh"


@pytest.mark.parametrize("value,include_output", [
    (None, True),
    ("", True),
    (None, False),
])
def test_local_state_missing_null_or_empty_output_is_unresolved(
    tmp_path, value, include_output,
):
    (tmp_path / "env.auto.tfvars").write_text("")
    _write_state(tmp_path, value, include_output=include_output)
    with pytest.raises(ValueError, match="no SQL warehouse"):
        resolve_warehouse(tmp_path)


def test_missing_local_state_is_unresolved_even_with_missing_runner(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text("")
    with pytest.raises(ValueError, match="no SQL warehouse"):
        resolve_warehouse(
            tmp_path, terraform_runner=tmp_path / "missing-runner", env_name="dev"
        )


def test_malformed_local_state_has_clear_error(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text("")
    (tmp_path / "terraform.tfstate").write_text("not-json")
    with pytest.raises(ValueError, match="could not read Terraform state"):
        resolve_warehouse(tmp_path)


def test_resolver_never_executes_the_terraform_runner(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text("")
    _write_state(tmp_path, "state-wh")
    marker = tmp_path / "runner-was-called"
    runner = tmp_path / "runner"
    runner.write_text(f"#!/bin/sh\ntouch {marker}\n")
    runner.chmod(0o755)
    assert resolve_warehouse(
        tmp_path, terraform_runner=runner, env_name="dev"
    ) == "state-wh"
    assert not marker.exists()


def test_repository_uses_only_per_environment_local_state_backends():
    for root in ("workspace", "data_access", "account"):
        text = (SHARED / "roots" / root / "main.tf").read_text()
        assert 'backend "local"' in text
    runner = (SHARED / "scripts" / "terraform_layer.sh").read_text()
    assert '-backend-config="path=$ENV_DIR/terraform.tfstate"' in runner


def test_invalid_warehouse_id_is_rejected(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text('sql_warehouse_id = "Warning: nope"\n')
    with pytest.raises(ValueError, match="invalid SQL warehouse ID"):
        resolve_warehouse(tmp_path)


def test_invalid_warehouse_id_in_local_state_is_rejected(tmp_path):
    (tmp_path / "env.auto.tfvars").write_text("")
    _write_state(tmp_path, "Warning: nope")
    with pytest.raises(ValueError, match="invalid SQL warehouse ID"):
        resolve_warehouse(tmp_path)
