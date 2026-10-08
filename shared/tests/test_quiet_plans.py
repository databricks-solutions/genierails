import copy
import json
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
