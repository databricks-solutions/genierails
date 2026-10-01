from pathlib import Path
import re
import os
import subprocess


SHARED = Path(__file__).parents[1]


def test_gate_defaults_false_in_both_roots_and_modules():
    for path in (
        SHARED / "modules/data_access/variables.tf",
        SHARED / "modules/workspace/variables.tf",
        SHARED / "roots/data_access/main.tf",
        SHARED / "roots/workspace/main.tf",
    ):
        source = path.read_text()
        start = source.index('variable "business_access_enabled"')
        body = source[start : source.index("}\n", start) + 2]
        assert "default     = false" in body


def test_workspace_business_acls_are_gated_but_creation_is_not():
    source = (SHARED / "modules/workspace/main.tf").read_text()

    assert source.count(
        "if var.business_access_enabled && "
        "contains(keys(local.genie_space_groups), k)"
    ) == 2
    assert source.count('GENIE_ALLOW_EMPTY_ACL    = "1"') == 2

    create_start = source.index('resource "null_resource" "genie_space_create"')
    create_end = source.index(
        'resource "null_resource" "genie_space_config"', create_start
    )
    assert "business_access_enabled" not in source[create_start:create_end]


def test_genie_creation_ignores_worktree_local_path_changes():
    source = (SHARED / "modules/workspace/main.tf").read_text()
    start = source.index('resource "null_resource" "genie_space_create"')
    end = source.index('resource "null_resource" "genie_space_config"', start)
    block = source[start:end]
    for trigger in ("id_file", "script", "host", "client_id", "client_secret"):
        assert f'triggers["{trigger}"]' in block
    assert 'command = "${var.genie_script_path} create"' in block
    assert "DATABRICKS_CLIENT_SECRET = var.databricks_client_secret" in block
    assert 'command = "bash ../../scripts/genie_space.sh trash"' in block
    assert "GENIE_ID_BASENAME = basename(self.triggers.id_file)" in block
    assert "GENIE_ID_FILE            = self.triggers.id_file" not in block


def test_created_space_acl_reapplies_after_space_recreation():
    source = (SHARED / "modules/workspace/main.tf").read_text()
    start = source.index('resource "null_resource" "genie_space_acls_created"')
    block = source[start:]
    assert "space_create_id = null_resource.genie_space_create[each.key].id" in block


def test_created_space_semantic_config_reapplies_after_space_recreation():
    source = (SHARED / "modules/workspace/main.tf").read_text()
    start = source.index('resource "null_resource" "genie_space_config"')
    end = source.index('resource "null_resource" "genie_space_acls_created"', start)
    block = source[start:end]
    assert "space_create_id = null_resource.genie_space_create[each.key].id" in block


def test_trash_fails_loud_when_current_id_file_is_missing(tmp_path):
    env_dir = tmp_path / "env"
    env_dir.mkdir()
    result = subprocess.run(
        ["bash", str(SHARED / "scripts/genie_space.sh"), "trash"],
        text=True,
        capture_output=True,
        env={
            **os.environ,
            "LAYER_ENV_DIR": str(env_dir),
            "GENIE_ID_BASENAME": ".genie_space_id_payments",
            "DATABRICKS_HOST": "https://example.invalid",
        },
    )
    assert result.returncode != 0
    assert "Cannot identify Genie agent to trash" in result.stderr
    assert "orphan" in result.stderr


def test_apply_fingerprint_includes_terraform_and_genie_code():
    makefile = (SHARED / "Makefile.shared").read_text()
    assert '$(SHARED_ROOT)/roots/$$layer' in makefile
    assert '$(SHARED_ROOT)/modules/$$layer' in makefile
    assert '$(SHARED_ROOT)/scripts/genie_space.sh' in makefile
    assert '$(SHARED_ROOT)/deploy_masking_functions.py' in makefile
    assert "-type f -name '*.tf'" in makefile
    assert "! -path '*/tests/*'" in makefile
    assert "! -path '*/.terraform/*'" in makefile


def test_roots_forward_the_same_gate_to_both_modules():
    assignment = r"business_access_enabled\s*=\s*var\.business_access_enabled"
    assert re.search(assignment, (SHARED / "roots/data_access/main.tf").read_text())
    assert re.search(assignment, (SHARED / "roots/workspace/main.tf").read_text())
