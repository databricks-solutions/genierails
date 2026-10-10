import os
import re
import shutil
import subprocess
from pathlib import Path

import hcl2
import pytest

from tests.terraform_helpers import shared_copy, tf, tf_env, tf_init


MAIN_TF = Path(__file__).parents[1] / "modules" / "data_access" / "main.tf"
WORKSPACE_MAIN_TF = Path(__file__).parents[1] / "modules" / "workspace" / "main.tf"


def _resource_body(source: str, name: str) -> str:
    marker = f'resource "databricks_grant" "{name}" {{'
    start = source.index(marker) + len(marker)
    depth = 1
    for index in range(start, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start:index]
    raise AssertionError(f"unterminated resource {name}")


def _typed_resource_body(source: str, resource_type: str, name: str) -> str:
    marker = f'resource "{resource_type}" "{name}" {{'
    start = source.index(marker) + len(marker)
    depth = 1
    for index in range(start, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start:index]
    raise AssertionError(f"unterminated resource {resource_type}.{name}")


_OPENERS = {"{": "}", "[": "]", "(": ")"}


def _code_spans(source: str):
    """Yield (index, char, depth) for HCL code outside strings and comments.

    depth counts open brackets/braces/parens. String contents are skipped, but
    ${...} interpolations inside them are scanned as code, so nested strings
    and brackets in interpolations don't confuse the depth.
    """
    stack: list[str] = []  # closers for brackets, '"' for strings
    index = 0
    while index < len(source):
        char = source[index]
        if stack and stack[-1] == '"':
            if char == "\\":
                index += 2
                continue
            if char == '"':
                stack.pop()
            elif source.startswith("${", index) or source.startswith("%{", index):
                stack.append("}")
                index += 2
                continue
            index += 1
            continue
        if char == "#" or source.startswith("//", index):
            newline = source.find("\n", index)
            index = len(source) if newline == -1 else newline
            continue
        if source.startswith("/*", index):
            end = source.find("*/", index + 2)
            assert end != -1, "unterminated block comment"
            index = end + 2
            continue
        if char == '"':
            stack.append('"')
        elif char in _OPENERS:
            yield index, char, len(stack)
            stack.append(_OPENERS[char])
            index += 1
            continue
        elif char in _OPENERS.values():
            assert stack and stack[-1] == char, f"unbalanced {char!r} at offset {index}"
            stack.pop()
        yield index, char, len(stack)
        index += 1
    assert not stack, "unbalanced HCL: unclosed " + "".join(stack)


def _top_level_expression(body: str, name: str) -> str:
    """Return the expression text of the block body's top-level ``name =``.

    Indentation and formatting don't matter. Raises AssertionError unless the
    attribute appears exactly once at top level.
    """
    spans = list(_code_spans(body))
    starts = [
        position for position, (index, _char, depth) in enumerate(spans)
        if depth == 0
        and re.match(rf"{re.escape(name)}\s*=(?!=)", body[index:])
        and (index == 0 or not re.match(r"[\w.-]", body[index - 1]))
    ]
    assert len(starts) == 1, f"expected exactly one top-level {name}, found {len(starts)}"
    position = starts[0]
    equals = body.index("=", spans[position][0])
    expression_start = next(
        index for index, char, _depth in spans[position:]
        if index > equals and not char.isspace()
    )
    end = None
    for index, char, depth in spans:
        if index < expression_start:
            continue
        if depth == 0 and (char == "\n" or index == len(body) - 1):
            end = index + 1
            break
    assert end is not None, f"unterminated {name} expression"
    return body[expression_start:end].strip()


def _parsed_depends_on(source: str, resource_type: str, name: str) -> set[str]:
    """Parse a resource's real depends_on expression with python-hcl2.

    python-hcl2 7.3 cannot parse some valid parenthesized expressions elsewhere
    in this module, so isolate the resource's top-level depends_on attribute
    structurally (any indentation; brackets inside strings, interpolations or
    comments don't end it) and let the HCL parser decide which references are
    live. A missing or unparseable depends_on raises, so assertions about it
    can never pass vacuously.
    """
    body = _typed_resource_body(source, resource_type, name)
    expression = _top_level_expression(body, "depends_on")
    assert expression.startswith("["), f"depends_on is not a list: {expression!r}"
    parsed = hcl2.loads(f"depends_on = {expression}\n")
    references = parsed["depends_on"]
    assert isinstance(references, list) and references, f"empty depends_on: {expression!r}"
    return {reference.removeprefix("${").removesuffix("}")
            for reference in references}


def _parsed_resource(source: str, resource_type: str, name: str) -> dict:
    body = _typed_resource_body(source, resource_type, name)
    parsed = hcl2.loads(f'resource "{resource_type}" "{name}" {{{body}}}')
    return parsed["resource"][0][resource_type][name]


def test_group_grants_follow_catalog_schema_table_chain():
    source = MAIN_TF.read_text()
    catalog = _resource_body(source, "catalog_access")
    schema = _resource_body(source, "schema_access")
    table = _resource_body(source, "table_access")

    assert "setproduct(local.all_catalogs, local.access_principals)" in catalog
    assert 'privileges = ["USE_CATALOG"]' in catalog

    assert "setproduct(local.uc_schemas, local.access_principals)" in schema
    assert "schema     = each.value.schema" in schema
    assert 'privileges = ["USE_SCHEMA"]' in schema

    assert "for pair in local.table_access_pairs" in table
    assert "table      = each.value.table" in table
    assert 'privileges = ["SELECT"]' in table


def test_select_is_not_granted_at_namespace_level():
    source = MAIN_TF.read_text()

    assert '"SELECT"' not in _resource_body(source, "catalog_access")
    assert '"SELECT"' not in _resource_body(source, "schema_access")
    assert '"USE_CATALOG"' not in _resource_body(source, "table_access")
    assert '"USE_SCHEMA"' not in _resource_body(source, "table_access")


def test_deployment_sp_self_grant_includes_apply_tag():
    source = MAIN_TF.read_text()
    deployer = _resource_body(source, "terraform_sp_manage_catalog")

    match = re.search(r"privileges\s*=\s*\[([^]]+)\]", deployer)
    assert match is not None
    assert set(re.findall(r'"([A-Z_]+)"', match.group(1))) == {
        "USE_CATALOG",
        "USE_SCHEMA",
        "SELECT",
        "EXECUTE",
        "MANAGE",
        "CREATE_FUNCTION",
        "APPLY_TAG",
    }


def test_tag_assignments_wait_for_deployment_sp_grant():
    source = MAIN_TF.read_text()
    start = source.index('resource "databricks_entity_tag_assignment" "assignments" {')
    end = source.index('resource "time_sleep" "wait_for_tag_propagation"', start)

    assert "depends_on = [databricks_grant.terraform_sp_manage_catalog]" in source[start:end]


def test_masking_functions_and_policies_wait_for_deployment_sp_grant():
    source = MAIN_TF.read_text()
    for start_marker, end_marker in (
        ('resource "terraform_data" "masking_functions" {',
         'resource "databricks_policy_info" "policies" {'),
        ('resource "databricks_policy_info" "policies" {', None),
    ):
        start = source.index(start_marker)
        end = source.index(end_marker, start) if end_marker else len(source)
        assert "databricks_grant.terraform_sp_manage_catalog" in source[start:end]


def test_table_select_waits_for_complete_policy_enforcement_chain():
    source = MAIN_TF.read_text()
    table_dependencies = _parsed_depends_on(
        source, "databricks_grant", "table_access"
    )
    policy_dependencies = _parsed_depends_on(
        source, "databricks_policy_info", "policies"
    )
    wait_dependencies = _parsed_depends_on(
        source, "time_sleep", "wait_for_policy_enforcement"
    )

    # Whole-resource dependencies make any failed mask or policy instance block
    # every table grant, rather than only a matching for_each instance.
    assert table_dependencies == {
        "time_sleep.wait_for_tag_propagation",
        "terraform_data.masking_functions",
        "databricks_policy_info.policies",
        "databricks_entity_tag_assignment.treatment",
        "time_sleep.wait_for_policy_enforcement",
    }

    # Treatment retags happen after the policies; the wait covers them too.
    assert wait_dependencies == {"databricks_policy_info.policies", "databricks_entity_tag_assignment.treatment"}
    assert "databricks_grant.table_access" not in policy_dependencies


def test_policy_grant_dependency_graph_is_acyclic_and_fail_closed():
    source = MAIN_TF.read_text()
    dependencies = {
        "table": _parsed_depends_on(source, "databricks_grant", "table_access"),
        "policies": _parsed_depends_on(
            source, "databricks_policy_info", "policies"
        ),
        "policy_wait": _parsed_depends_on(
            source, "time_sleep", "wait_for_policy_enforcement"
        ),
    }

    # This is the relevant Terraform plan graph: grants have both failed-policy
    # and failed-mask nodes as ancestors. Terraform reverses these edges during
    # destroy, so table grants are removed before the wait and policies.
    assert "databricks_policy_info.policies" in dependencies["table"]
    assert "terraform_data.masking_functions" in dependencies["table"]
    assert "databricks_policy_info.policies" in dependencies["policy_wait"]
    assert "databricks_grant.table_access" not in dependencies["policies"]


def test_policy_enforcement_wait_restarts_only_when_enforcement_inputs_change():
    wait = _parsed_resource(
        MAIN_TF.read_text(), "time_sleep", "wait_for_policy_enforcement"
    )

    assert wait["create_duration"] == "30s"
    # Keyed on the deployment itself, so any masking_functions replacement
    # (SQL, warehouse, host or client ID change) restarts the wait.
    assert wait["triggers"] == {
        "policy_hash": "${sha256(jsonencode(local.fgac_policy_map))}",
        "masking_functions_id": "${terraform_data.masking_functions.id}",
    }


def test_policy_enforcement_wait_comment_does_not_promise_to_protect_existing_grants():
    source = MAIN_TF.read_text()
    comment = source[:source.index('resource "time_sleep" "wait_for_policy_enforcement"')]
    comment = comment[comment.rindex("\n\n"):]
    assert "Keep SELECT closed" not in comment
    assert "New table grants wait" in comment
    assert "already exist stay in place" in comment


def _with_policies_depending_on_table_access(source: str, indent: str) -> str:
    """The PR #70 review mutation: re-add table_access to the policies' depends_on."""
    body_start = source.index('resource "databricks_policy_info" "policies" {')
    marker = "  depends_on = [\n"
    at = source.index(marker, body_start)
    mutated = source[:at] + f"{indent}depends_on = [\n{indent}  databricks_grant.table_access,\n" + source[at + len(marker):]
    return mutated


@pytest.mark.parametrize("indent", ["  ", "    ", "\t", "      "])
def test_cycle_mutation_is_caught_at_any_indentation(indent):
    mutated = _with_policies_depending_on_table_access(MAIN_TF.read_text(), indent)
    assert "databricks_grant.table_access" in _parsed_depends_on(
        mutated, "databricks_policy_info", "policies"
    )


def test_cycle_mutation_on_one_line_is_caught():
    source = MAIN_TF.read_text()
    start = source.index('resource "databricks_policy_info" "policies" {')
    at = source.index("  depends_on = [\n", start)
    end = source.index("  ]\n", at) + len("  ]\n")
    mutated = source[:at] + "    depends_on = [databricks_grant.table_access, databricks_grant.catalog_access]\n" + source[end:]
    assert _parsed_depends_on(mutated, "databricks_policy_info", "policies") == {
        "databricks_grant.table_access", "databricks_grant.catalog_access",
    }


def test_depends_on_parser_raises_instead_of_returning_nothing():
    source = MAIN_TF.read_text()
    start = source.index('resource "databricks_policy_info" "policies" {')
    at = source.index("  depends_on = [\n", start)
    end = source.index("  ]\n", at) + len("  ]\n")
    without = source[:at] + source[end:]
    with pytest.raises(AssertionError, match="exactly one top-level depends_on"):
        _parsed_depends_on(without, "databricks_policy_info", "policies")
    garbled = source[:at] + "  depends_on = [databricks_grant.table_access,,]\n" + source[end:]
    with pytest.raises(Exception):
        _parsed_depends_on(garbled, "databricks_policy_info", "policies")


def test_depends_on_parser_ignores_brackets_in_strings_comments_and_nested_blocks():
    source = (
        'resource "databricks_grant" "probe" {\n'
        '  comment = "not ] the end ${lookup(var.m, "k]", "[")}"\n'
        '  lifecycle {\n'
        '    depends_on = [databricks_grant.nested_is_not_top_level]\n'
        '  }\n'
        '      depends_on = [ # closing ] in a comment\n'
        '        databricks_grant.a, /* ] */\n'
        '        databricks_grant.b,\n'
        '      ]\n'
        '}\n'
    )
    assert _parsed_depends_on(source, "databricks_grant", "probe") == {
        "databricks_grant.a", "databricks_grant.b",
    }


def test_business_select_requires_a_current_coverage_gate_pass():
    table = _resource_body(MAIN_TF.read_text(), "table_access")
    precondition = table[table.index("lifecycle {"):]
    assert 'condition     = local.coverage_gate_status == "pass"' in precondition
    # Whole-resource dependencies stay as they were: the gate adds a
    # precondition, not an edge, so no address or key changes.
    assert '"${pair.table}|${pair.principal}"' in table


def test_gate_fingerprint_covers_every_input_the_gate_judges():
    source = MAIN_TF.read_text()
    fingerprint = source[source.index("coverage_gate_fingerprint = sha256(jsonencode({"):]
    fingerprint = fingerprint[:fingerprint.index("}))")]
    for item in (
        "tag_assignments = sort(keys(local.tag_assignment_map))",
        "fgac_policies   = local.fgac_policy_map",
        "table_grants    = sort([for pair in local.table_access_pairs",
        "masking_sql     = filesha256(var.masking_sql_file)",
        "ddl             = fileexists(var.coverage_ddl_file) ? filesha256(var.coverage_ddl_file)",
        "acknowledged    = sort(distinct(",
    ):
        assert item in fingerprint, item
    # The retired flag never fed the fingerprint, so released passes stay valid.
    assert "business_access_enabled" not in fingerprint


def test_existing_grant_and_policy_resource_addresses_and_keys_are_unchanged():
    source = MAIN_TF.read_text()
    table = _resource_body(source, "table_access")
    policies = _typed_resource_body(source, "databricks_policy_info", "policies")

    assert "for pair in local.table_access_pairs" in table
    assert '"${pair.table}|${pair.principal}"' in table
    assert "for_each = local.fgac_policy_map" in policies
    assert 'name                  = "${each.value.catalog}_${each.key}"' in policies


def test_business_select_is_fail_closed_through_the_gate_not_a_flag():
    source = MAIN_TF.read_text()
    table = _resource_body(source, "table_access")

    # No flag empties the map; every planned grant is checked by the gate.
    assert "for_each = {\n    for pair in local.table_access_pairs :" in table
    assert "local.coverage_gate_status == \"pass\" || !contains(local.table_grants_needing_gate, each.key)" in table
    assert "var.business_access_enabled" not in source
    assert "var.business_access_enabled &&" not in source


def test_explicit_empty_agent_acl_is_fail_closed_in_both_layers():
    data_access = MAIN_TF.read_text()
    workspace = WORKSPACE_MAIN_TF.read_text()

    assert "length(local.scoped_table_access_principals[table]) == 0" not in data_access
    assert 'join(",", space.config.acl_groups)' in workspace
    assert 'join(",", keys(var.groups))' not in workspace.split(
        "genie_space_groups =", 1
    )[1].split("existing_spaces =", 1)[0]
    assert re.search(r'GENIE_ALLOW_EMPTY_ACL\s*= "1"', workspace)


def test_catalog_grants_are_serialized_without_authoritative_replacement():
    source = MAIN_TF.read_text()
    catalog_access = _resource_body(source, "catalog_access")
    assert "depends_on = [databricks_grant.terraform_sp_manage_catalog]" in catalog_access
    assert 'resource "databricks_grants"' not in source


def test_builtin_policy_targets_are_included_in_access_principals():
    source = MAIN_TF.read_text()
    normalized = " ".join(source.split())
    assert (
        "access_principals = distinct(concat( keys(var.groups), "
        "flatten([ for p in var.fgac_policies : p.to_principals "
        "if !startswith(p.comment, \"GenieRails treatment fallback; "
        "principals are masking-only\") ]), "
        "flatten(values(var.genie_space_acl_groups)), ))"
    ) in normalized
    for resource in ("catalog_access", "schema_access"):
        body = _resource_body(source, resource)
        assert "local.access_principals" in body
        assert "keys(var.groups)" not in body
    assert "local.table_access_pairs" in _resource_body(source, "table_access")


def test_table_select_principals_are_derived_per_table():
    source = MAIN_TF.read_text()

    assert "table_access_principals = {" in source
    assert "lookup(var.table_agents, table, [])" in source
    assert "lookup(var.genie_space_acl_groups, agent, [])" in source
    assert "contains(var.admin_uc_tables, table)" in source
    assert "contains(local.legacy_unattributed_discovered_tables, table)" in source
    assert "setproduct(local.effective_uc_tables, local.access_principals)" not in source


def test_masking_deployer_does_not_declassify_oauth_secret():
    source = MAIN_TF.read_text()

    masking = source[source.index('resource "terraform_data" "masking_functions" {'):
                     source.index('resource "databricks_policy_info" "policies" {')]
    # The deployer reads the secret from auth.auto.tfvars; it never enters state.
    assert "databricks_client_secret" not in masking
    assert "nonsensitive(var.databricks_client_secret)" not in source


def _verbose_runs(output: str) -> dict[str, str]:
    """Split `terraform test -verbose` output into each run's section."""
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


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
def test_reapplying_unchanged_inputs_keeps_the_wait_and_every_grant_mask_and_policy(tmp_path):
    root = shared_copy(tmp_path) / "modules" / "data_access"
    env = tf_env(tmp_path)
    tf_init(root, env=env)
    result = tf(root, "test", "-no-color", "-verbose",
                "-filter=tests/policy_enforcement_wait.tftest.hcl", env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    runs = _verbose_runs(result.stdout)

    # A second apply with the same inputs plans nothing: the wait doesn't
    # re-run and no grant, mask or policy is touched.
    unchanged = runs["replan_unchanged_inputs"]
    assert unchanged.startswith("pass")
    assert "No changes. Your infrastructure matches the configuration." in unchanged

    # Moving the masks to another warehouse redeploys them and restarts the
    # wait (filemd5 alone missed this); grants and policies stay in place.
    moved = runs["replan_after_warehouse_change"]
    assert moved.startswith("pass")
    replaced = set(re.findall(r"# (\S+) must be replaced", moved))
    assert replaced == {
        "terraform_data.masking_functions",
        "time_sleep.wait_for_policy_enforcement",
    }
    assert not re.search(r"# databricks_\S+ (will be destroyed|must be replaced)", moved)


@pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")
@pytest.mark.parametrize("module, test_file, run", [
    # An existing grant after the gate expires (data_access) ...
    ("data_access", "tests/retained_grants.tftest.hcl", "replan_unchanged_grant_after_expiry"),
    # ... and an existing non-empty CAN_RUN ACL after it expires (workspace).
    ("workspace", "tests/genie_exposure_precondition.tftest.hcl", "replan_unchanged_acl_after_the_gate_expires"),
])
def test_unchanged_access_replans_without_changes_after_the_gate_expires(tmp_path, module, test_file, run):
    root = shared_copy(tmp_path) / "modules" / module
    env = tf_env(tmp_path)
    tf_init(root, env=env)
    result = tf(root, "test", "-no-color", "-verbose", f"-filter={test_file}", env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    section = _verbose_runs(result.stdout)[run]
    assert section.startswith("pass")
    # No resource is created, changed, replaced or destroyed (outputs may change:
    # the recorded gate status is now "expired").
    assert not re.search(r"# \S+ (will be|must be)", section), section
