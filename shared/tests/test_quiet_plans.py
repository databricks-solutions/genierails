import copy
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "modules/data_access"))

import generate_abac
from normalize_masking_sql import normalized_definitions
from sql_tokenizer import SqlTokenizeError


def test_masking_normalization_ignores_comments_whitespace_and_order():
    first = """-- generated\nUSE CATALOG cat;\nUSE SCHEMA sch;\nCREATE OR REPLACE FUNCTION beta(v STRING) RETURNS STRING RETURN v;\n/* note */\nCREATE OR REPLACE FUNCTION alpha(v STRING)\nRETURNS STRING RETURN CONCAT(v, ' x ');\n"""
    second = """USE CATALOG cat;\nUSE SCHEMA sch;\n-- reordered\nCREATE OR REPLACE FUNCTION alpha ( v STRING ) RETURNS STRING RETURN CONCAT ( v , ' x ' ) ;\nCREATE OR REPLACE FUNCTION beta(v STRING) RETURNS STRING RETURN v;\n"""
    assert normalized_definitions(first) == normalized_definitions(second)


def test_masking_normalization_detects_real_body_change():
    before = "CREATE OR REPLACE FUNCTION mask(v STRING) RETURNS STRING RETURN 'x';\n"
    after = "CREATE OR REPLACE FUNCTION mask(v STRING) RETURNS STRING RETURN 'y';\n"
    assert normalized_definitions(before) != normalized_definitions(after)


def _contextual_function(catalog, schema, name, body):
    return (
        f"USE CATALOG {catalog};\nUSE SCHEMA {schema};\n"
        f"CREATE OR REPLACE FUNCTION {name}(v STRING) RETURNS STRING RETURN {body};\n"
    )


def test_masking_normalization_ignores_function_order_with_use_context():
    alpha = _contextual_function("cat", "sales", "alpha", "v")
    beta = _contextual_function("cat", "finance", "beta", "upper(v)")
    assert normalized_definitions(alpha + beta) == normalized_definitions(beta + alpha)


def test_contextual_masking_normalization_detects_body_change():
    before = _contextual_function("cat", "sales", "mask", "'x'")
    after = _contextual_function("cat", "sales", "mask", "'y'")
    assert normalized_definitions(before) != normalized_definitions(after)


@pytest.mark.parametrize(("catalog", "schema"), [("other", "sales"), ("cat", "finance")])
def test_contextual_masking_normalization_detects_resolved_target_change(catalog, schema):
    before = _contextual_function("cat", "sales", "mask", "v")
    after = _contextual_function(catalog, schema, "mask", "v")
    assert normalized_definitions(before) != normalized_definitions(after)


def test_contextual_masking_normalization_detects_added_or_removed_function():
    alpha = _contextual_function("cat", "sales", "alpha", "v")
    beta = _contextual_function("cat", "sales", "beta", "v")
    assert normalized_definitions(alpha) != normalized_definitions(alpha + beta)


def test_fully_qualified_function_hashes_execution_context_for_body_references():
    definition = "CREATE FUNCTION cat.sales.mask(v STRING) RETURNS STRING RETURN v;"
    assert normalized_definitions("USE CATALOG old;\nUSE SCHEMA old;\n" + definition) != normalized_definitions(
        "USE CATALOG other;\nUSE SCHEMA other;\n" + definition
    )


def test_duplicate_resolved_function_names_keep_file_order_across_use_blocks():
    first = _contextual_function("cat", "sales", "mask", "'first'")
    second = _contextual_function("cat", "sales", "`mask`", "'second'")
    assert normalized_definitions(first + second) != normalized_definitions(second + first)


def test_unclassified_statement_change_fails_closed_with_contextual_functions():
    functions = (
        _contextual_function("cat", "sales", "alpha", "v")
        + _contextual_function("cat", "finance", "beta", "v")
    )
    assert normalized_definitions("SET timezone = 'UTC';" + functions) != normalized_definitions(
        "SET timezone = 'Australia/Melbourne';" + functions
    )


def test_unclassified_statement_is_a_fail_closed_ordering_barrier():
    function = _contextual_function("cat", "sales", "mask", "v")
    statement = "SET timezone = 'UTC';"
    assert normalized_definitions(statement + function) != normalized_definitions(function + statement)


def test_use_catalog_keeps_deployer_schema_and_schema_change_moves_hash():
    prefix = "USE CATALOG a;\nUSE SCHEMA {schema};\nUSE CATALOG b;\n"
    function = "CREATE FUNCTION g() RETURNS STRING RETURN 'x';\n"
    assert normalized_definitions(prefix.format(schema="s") + function) != normalized_definitions(
        prefix.format(schema="t") + function
    )


def test_same_deployed_target_keeps_order_for_bare_and_schema_qualified_names():
    prefix = "USE CATALOG a;\nUSE SCHEMA s;\nUSE CATALOG b;\n"
    first = "CREATE FUNCTION f() RETURNS STRING RETURN 'first';\n"
    second = "CREATE FUNCTION s.f() RETURNS STRING RETURN 'second';\n"
    assert normalized_definitions(prefix + first + second) != normalized_definitions(prefix + second + first)


def test_spaced_dot_unknown_name_keeps_deployment_order():
    prefix = "USE CATALOG cat;\nUSE SCHEMA sch;\n"
    qualified = (
        "CREATE OR REPLACE FUNCTION cat . sch . mask(v STRING) "
        "RETURNS STRING RETURN 'x';\n"
    )
    bare = "CREATE OR REPLACE FUNCTION mask(v STRING) RETURNS STRING RETURN 'y';\n"
    assert normalized_definitions(prefix + qualified + bare) != normalized_definitions(
        prefix + bare + qualified
    )


def test_final_name_collision_keeps_order_for_bare_use_and_default_fqn():
    prefix = "USE t;\n"
    first = "CREATE FUNCTION f() RETURNS STRING RETURN 'first';\n"
    second = "CREATE FUNCTION main.default.f() RETURNS STRING RETURN 'second';\n"
    assert normalized_definitions(prefix + first + second) != normalized_definitions(prefix + second + first)


def test_deployer_splitter_difference_fails_closed():
    function = "CREATE FUNCTION f() RETURNS STRING RETURN 'x';\n"
    same_line = "USE CATALOG a; USE SCHEMA s;\n" + function
    separate_lines = "USE CATALOG a;\nUSE SCHEMA s;\n" + function
    assert normalized_definitions(same_line) != normalized_definitions(separate_lines)


def test_deployer_skipped_block_comment_use_fails_closed():
    suffix = "\nUSE SCHEMA s;\nCREATE FUNCTION f() RETURNS STRING RETURN 'x';\n"
    skipped = "/* c */ USE CATALOG b;" + suffix
    applied = "USE CATALOG b;" + suffix
    assert normalized_definitions(skipped) != normalized_definitions(applied)


def test_qualified_use_schema_is_fail_closed():
    function = "\nCREATE FUNCTION f() RETURNS STRING RETURN 'x';\n"
    assert normalized_definitions("USE SCHEMA x.y;" + function) != normalized_definitions(
        "USE SCHEMA x.z;" + function
    )


@pytest.mark.parametrize(("before", "after"), [
    ("RETURN v & 1", "RETURN v | 1"),
    ("RETURN v || 'x'", "RETURN v 'x'"),
    ("RETURN !v", "RETURN v"),
    ("RETURN v ^ 2", "RETURN v ~ 2"),
    ("LANGUAGE PYTHON AS $$\n  return v\n$$", "LANGUAGE PYTHON AS $$\n    return v\n$$"),
    ("LANGUAGE PYTHON AS $$return v--1$$", "LANGUAGE PYTHON AS $$return v--2$$"),
    (r"RETURN 'it\' -- AAA'", r"RETURN 'it\' -- BBB'"),
    ("LANGUAGE PYTHON AS $py$\nreturn '***'\n$py$", "LANGUAGE PYTHON AS $py$\n    return '***'\n$py$"),
    ("LANGUAGE PYTHON AS $py$return v--1$py$", "LANGUAGE PYTHON AS $py$return v--2$py$"),
    ("LANGUAGE PYTHON AS $PY$\nreturn '***'\n$PY$", "LANGUAGE PYTHON AS $PY$\n  return '***'\n$PY$"),
])
def test_masking_normalization_never_collapses_real_changes(before, after):
    prefix = "CREATE OR REPLACE FUNCTION cat.sch.mask(v STRING) RETURNS STRING "
    assert normalized_definitions(prefix + before + ";") != normalized_definitions(prefix + after + ";")


def test_non_function_statements_are_hashed_and_bound_function_reordering():
    first = "SET timezone = 'UTC'; CREATE FUNCTION b() RETURNS INT RETURN 2; CREATE FUNCTION a() RETURNS INT RETURN 1;"
    reordered = "SET timezone = 'UTC'; CREATE FUNCTION a() RETURNS INT RETURN 1; CREATE FUNCTION b() RETURNS INT RETURN 2;"
    changed_set = reordered.replace("'UTC'", "'Australia/Melbourne'")
    assert normalized_definitions(first) == normalized_definitions(reordered)
    assert normalized_definitions(reordered) != normalized_definitions(changed_set)


def test_ambiguous_masking_hash_is_stable_across_python_hash_seeds(tmp_path):
    sql_file = tmp_path / "ambiguous.sql"
    sql_file.write_text("""\
USE CATALOG a;
/* fallback */ CREATE FUNCTION alpha() RETURNS STRING RETURN 'a';
/* fallback */ CREATE FUNCTION beta() RETURNS STRING RETURN 'b';
USE CATALOG b;
/* fallback */ CREATE FUNCTION gamma() RETURNS STRING RETURN 'c';
/* fallback */ CREATE FUNCTION delta() RETURNS STRING RETURN 'd';
/* fallback */ CREATE FUNCTION epsilon() RETURNS STRING RETURN 'e';
""")
    script = Path(__file__).parent.parent / "modules" / "data_access" / "normalize_masking_sql.py"
    hashes = []
    for seed in ("1", "2"):
        proc = subprocess.run(
            [sys.executable, str(script)],
            input=json.dumps({"sql_file": str(sql_file)}),
            env={**os.environ, "PYTHONHASHSEED": seed},
            text=True,
            capture_output=True,
            check=True,
        )
        hashes.append(json.loads(proc.stdout)["hash"])
    assert hashes[0] == hashes[1]


def test_duplicate_logical_function_names_keep_file_order():
    prefix = "USE CATALOG cat; USE SCHEMA sch; "
    definitions = [
        "CREATE FUNCTION mask() RETURNS INT RETURN 1;",
        "CREATE FUNCTION `mask`() RETURNS INT RETURN 2;",
        "CREATE FUNCTION cat.sch.mask() RETURNS INT RETURN 3;",
    ]
    assert normalized_definitions(prefix + " ".join(definitions)) != normalized_definitions(
        prefix + " ".join(reversed(definitions))
    )


def test_backslash_continued_line_comment_fails_closed():
    sql = "CREATE FUNCTION mask() RETURNS STRING RETURN 'x'; -- continued\\\nRETURN 'value -- hidden';"
    with pytest.raises(SqlTokenizeError, match="backslash-continued"):
        normalized_definitions(sql)


@pytest.mark.parametrize("sql", ["SELECT $;", "SELECT $py$unclosed;"])
def test_invalid_dollar_quote_fails_closed(sql):
    with pytest.raises(SqlTokenizeError):
        normalized_definitions(sql)


def test_coverage_fingerprint_hashes_the_raw_masking_file():
    source = (Path(__file__).parents[1] / "modules/data_access/main.tf").read_text()
    assert source.count("masking_sql     = filesha256(var.masking_sql_file)") >= 2
    assert "masking_sql  = filesha256(var.masking_sql_file)" in source


def test_masking_replacement_path_never_drops():
    source = (Path(__file__).parents[1] / "modules/data_access/main.tf").read_text()
    replacement = source[source.index('resource "terraform_data" "masking_functions" {'):
                         source.index('resource "terraform_data" "masking_functions_drop" {')]
    assert "--drop" not in replacement
    assert "normalized_masking_sql.result.hash" in replacement


def test_reviewed_genie_benchmarks_and_snippets_stick(tmp_path):
    path = tmp_path / "abac.auto.tfvars"
    reviewed = {
        "benchmarks": [{"question": "reviewed", "sql": "SELECT 1"}],
        "sql_filters": [{"sql": "x = 1", "display_name": "mine"}],
        "sql_measures": [{"alias": "mine", "sql": "SUM(x)"}],
    }
    path.write_text(generate_abac.format_genie_space_configs_hcl({"Sales": reviewed}) + "\n")
    fresh = {"Sales": {
        "title": "Sales",
        "benchmarks": [{"question": "api", "sql": "SELECT 2"}],
        "sql_filters": [{"sql": "x = 2"}],
        "sql_expressions": [{"alias": "new", "sql": "x + 1"}],
    }}
    kept = generate_abac.keep_reviewed_genie_content(path, fresh)
    assert fresh["Sales"]["benchmarks"] == reviewed["benchmarks"]
    assert fresh["Sales"]["sql_filters"][0]["sql"] == "x = 1"
    assert fresh["Sales"]["sql_filters"][0]["display_name"] == "mine"
    assert fresh["Sales"]["sql_measures"][0]["sql"] == "SUM(x)"
    assert fresh["Sales"]["sql_expressions"] == [{"alias": "new", "sql": "x + 1"}]
    assert kept == ["Sales.benchmarks", "Sales.sql_filters", "Sales.sql_measures"]


def test_reviewed_genie_fields_are_restored_byte_for_byte():
    existing = '''genie_space_configs = {
  "Sales" = {
    benchmarks = [ # reviewed spacing/comment
      { question = "mine", sql = "SELECT 1" },
    ]
    sql_filters = [{ sql = "x = 1", display_name = "user edit" }]
  }
}
'''
    rendered = generate_abac.format_genie_space_configs_hcl({"Sales": {
        "benchmarks": [{"question": "api", "sql": "SELECT 2"}],
        "sql_filters": [{"sql": "x = 2", "display_name": "api"}],
    }})
    restored = generate_abac.restore_reviewed_genie_text(existing, rendered, ["Sales"])
    for field in ("benchmarks", "sql_filters"):
        old = generate_abac._space_field_span(existing, "Sales", field)
        new = generate_abac._space_field_span(restored, "Sales", field)
        assert existing[old[0]:old[1]] == restored[new[0]:new[1]]


def test_discovery_explicit_tables_explains_skip(tmp_path):
    env = tmp_path / "dev"
    env.mkdir()
    (env / "env.auto.tfvars").write_text(
        'enable_classification = true\nuc_tables = ["cat.sch.table"]\n'
        'genie_spaces = [{ genie_space_id = "space", uc_tables = ["cat.sch.table"] }]\n'
    )
    result = subprocess.run(
        [sys.executable, str(Path(__file__).parents[1] / "scripts/discover_agent_tables.py"), str(env)],
        text=True, capture_output=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "Using listed uc_tables instead of discovering tables from genie_space_id.\n"
