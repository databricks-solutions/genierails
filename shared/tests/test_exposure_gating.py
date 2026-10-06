import json
import os
from pathlib import Path
import re
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

    create_start = source.index('resource "terraform_data" "genie_space"')
    create_end = source.index(
        'resource "null_resource" "genie_space_config"', create_start
    )
    assert "business_access_enabled" not in source[create_start:create_end]


def test_genie_creation_replaces_only_on_host_and_keeps_no_credentials():
    source = (SHARED / "modules/workspace/main.tf").read_text()
    start = source.index('resource "terraform_data" "genie_space"')
    end = source.index('resource "null_resource" "genie_space_config"', start)
    block = source[start:end]
    resource = block[: block.index("\nremoved {")]
    triggers = re.search(r"triggers_replace = \{(.*?)\n  \}", resource, re.S).group(1)
    assert set(re.findall(r"^\s*(\w+)\s*=", triggers, re.M)) == {"host"}
    assert "ignore_changes" not in resource
    assert 'command = "${var.genie_script_path} create"' in resource
    # Create-time credentials come from variables and never enter state.
    assert "DATABRICKS_CLIENT_SECRET = var.databricks_client_secret" in resource
    assert "self.triggers_replace.client" not in resource and "self.input.client" not in resource
    assert 'command = "bash ../../scripts/genie_space.sh trash"' in resource
    assert re.search(r"GENIE_ID_BASENAME\s+= basename\(self\.input\.id_file\)", resource)
    assert "GENIE_EXPECTED_HOST = self.triggers_replace.host" in resource
    # The pre-migration resource is forgotten, never destroyed (that would trash the agent).
    removed = block[block.index("\nremoved {"):]
    assert "from = null_resource.genie_space_create" in removed
    assert re.search(r"lifecycle \{\s*destroy = false\s*\}", removed)


def test_created_space_acl_reapplies_after_space_recreation():
    source = (SHARED / "modules/workspace/main.tf").read_text()
    start = source.index('resource "null_resource" "genie_space_acls_created"')
    block = source[start:]
    assert "space_create_id = terraform_data.genie_space[each.key].id" in block


def test_created_space_semantic_config_reapplies_after_space_recreation():
    source = (SHARED / "modules/workspace/main.tf").read_text()
    start = source.index('resource "null_resource" "genie_space_config"')
    end = source.index('resource "null_resource" "genie_space_acls_created"', start)
    block = source[start:end]
    assert "space_create_id = terraform_data.genie_space[each.key].id" in block


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


def test_trash_targets_original_host_not_current_host_on_mismatch(tmp_path):
    id_file = tmp_path / "space.id"
    id_file.write_text("space-123\n")
    calls = tmp_path / "curl.calls"
    curl = tmp_path / "curl"
    curl.write_text(
        "#!/usr/bin/env bash\n"
        'printf "%s\\n" "$*" >> "$CURL_CALLS"\n'
        'url="${!#}"\n'
        'if [[ "$url" == "https://w1.example/oidc/v1/token" ]]; then\n'
        "  printf '{\"access_token\":\"token\"}\\n200\\n'\n"
        'elif [[ "$url" == "https://w1.example/api/2.0/genie/spaces/space-123" ]]; then\n'
        "  printf '{}\\n404\\n'\n"
        "else\n"
        "  printf '{}\\n404\\n'\n"
        "fi\n"
    )
    curl.chmod(0o755)
    result = subprocess.run(
        ["bash", str(SHARED / "scripts/genie_space.sh"), "trash"],
        text=True,
        capture_output=True,
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "CURL_CALLS": str(calls),
            "GENIE_ID_FILE": str(id_file),
            "GENIE_EXPECTED_HOST": "https://w1.example/",
            "DATABRICKS_HOST": "https://w2.example",
            "DATABRICKS_CLIENT_ID": "client",
            "DATABRICKS_CLIENT_SECRET": "secret",
        },
    )
    assert result.returncode == 0, result.stderr
    assert not id_file.exists()
    call_text = calls.read_text()
    assert "https://w1.example/oidc/v1/token" in call_text
    assert "https://w1.example/api/2.0/genie/spaces/space-123" in call_text
    assert "https://w2.example" not in call_text


def test_trash_mismatched_host_auth_failure_names_both_hosts(tmp_path):
    id_file = tmp_path / "space.id"
    id_file.write_text("space-123\n")
    curl = tmp_path / "curl"
    curl.write_text("#!/usr/bin/env bash\nprintf '{}\\n401\\n'\n")
    curl.chmod(0o755)
    result = subprocess.run(
        ["bash", str(SHARED / "scripts/genie_space.sh"), "trash"],
        text=True,
        capture_output=True,
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "GENIE_ID_FILE": str(id_file),
            "GENIE_EXPECTED_HOST": "https://w1.example",
            "DATABRICKS_HOST": "https://w2.example",
            "DATABRICKS_CLIENT_ID": "client",
            "DATABRICKS_CLIENT_SECRET": "secret",
        },
    )
    assert result.returncode != 0
    assert id_file.exists()
    assert "https://w1.example" in result.stderr
    assert "https://w2.example" in result.stderr


def _terraform_trigger_plan(tmp_path, changes):
    """Plan the production genie_space block (provisioners stripped) after a change."""
    source = (SHARED / "modules/workspace/main.tf").read_text()
    start = source.index('resource "terraform_data" "genie_space"')
    block = source[start: source.index("\n}\n", start) + 3]
    block = re.sub(r"\n  provisioner \"local-exec\" \{.*?\n  \}\n", "\n", block, flags=re.S)
    block = re.sub(r"\n  depends_on = \[.*?\n  \]\n", "\n", block, flags=re.S)
    block = block.replace("  for_each = local.new_spaces\n", "")
    block = block.replace("var.databricks_workspace_host", "var.host")
    block = block.replace("${var.genie_id_file_prefix}_${each.key}", "id")
    module = tmp_path / "trigger-plan"
    module.mkdir()
    (module / "main.tf").write_text(
        'variable "host" { type = string }\n'
        'variable "client_id" { type = string }\n'
        'variable "client_secret" {\n  type = string\n  sensitive = true\n}\n'
        + block
    )
    env = {**os.environ, "TF_IN_AUTOMATION": "1"}
    base = ["-var=host=https://w1", "-var=client_id=old", "-var=client_secret=old"]
    subprocess.run(["terraform", "init", "-backend=false", "-input=false"], cwd=module,
                   env=env, check=True, capture_output=True, text=True)
    subprocess.run(["terraform", "apply", "-auto-approve", "-input=false", *base],
                   cwd=module, env=env, check=True, capture_output=True, text=True)
    assert '"old"' not in (module / "terraform.tfstate").read_text()  # no credential in state
    args = {"host": "https://w1", "client_id": "old", "client_secret": "old", **changes}
    plan = module / "plan.bin"  # embeds prior state: owner-only, then deleted
    try:
        subprocess.run(["terraform", "plan", "-input=false", f"-out={plan}",
                        *[f"-var={key}={value}" for key, value in args.items()]],
                       cwd=module, env=env, check=True, capture_output=True, text=True)
        plan.chmod(0o600)
        shown = subprocess.run(["terraform", "show", "-json", str(plan)], cwd=module,
                               env=env, check=True, capture_output=True, text=True)
    finally:
        plan.unlink(missing_ok=True)
    return json.loads(shown.stdout)["resource_changes"][0]["change"]["actions"]


def test_host_change_plans_genie_space_replacement(tmp_path):
    assert _terraform_trigger_plan(tmp_path, {"host": "https://w2"}) == ["delete", "create"]


def test_credential_change_does_not_plan_genie_space_replacement(tmp_path):
    assert _terraform_trigger_plan(
        tmp_path, {"client_id": "new", "client_secret": "new"}
    ) == ["no-op"]


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
