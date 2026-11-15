import json
import os
from datetime import datetime, timezone

import pytest

import evidence_report
from evidence_report import assemble_report, collect_live_state, configured_tables, main, render_markdown, write_report


COL = {"catalog": "main", "schema": "sales", "table": "customers", "column": "email"}


@pytest.fixture(autouse=True)
def _in_checkout_state(monkeypatch):
    """Evidence goes to <cloud>/.genierails-state/<env>/evidence unless a test sets the state dir."""
    monkeypatch.delenv("GENIERAILS_STATE_DIR", raising=False)


def _out(env_dir):
    return env_dir.parents[1] / ".genierails-state" / env_dir.name / "evidence"


class NoPolicies:
    def list_policies(self, *_args, **_kwargs):
        return iter(())


def _auth_file(tmp_path):
    path = tmp_path / "auth.auto.tfvars"
    path.write_text(
        'databricks_workspace_host = "https://selected.example"\n'
        'databricks_client_id = "selected-client"\n'
        'databricks_client_secret = "selected-secret"\n'
    )
    return path


def test_assemble_report_joins_and_sorts_mocked_state():
    report = assemble_report(
        [COL],
        classifications=[{**COL, "status": "complete", "scanned_at": "2026-09-25T01:02:03Z"}],
        tags=[{**COL, "name": "pii", "value": "email"}],
        masks=[{**COL, "name": "main.governance.mask_email"}],
        policies=[{**COL, "name": "main.governance.region_filter"}],
        grants=[{**COL, "principal": "analysts", "privilege": "SELECT", "scope": "TABLE"}],
        generated_at=datetime(2026, 9, 26, tzinfo=timezone.utc),
        approved_by="compliance@example.com",
        approved_at="2026-09-26T00:00:00Z",
        source="mock-live",
    )
    assert report["schema_version"] == "1.0"
    assert report["header"]["generated_at"] == "2026-09-26T00:00:00Z"
    item = report["evidence"][0]
    assert item["classification"] == {"status": "complete", "scanned_at": "2026-09-25T01:02:03Z"}
    assert item["detected_tags"] == [{"name": "pii", "value": "email"}]
    assert item["applied_mask"] == "main.governance.mask_email"
    assert item["applied_policies"] == ["main.governance.region_filter"]
    assert item["grants"][0]["principal"] == "analysts"


def test_missing_live_inputs_have_explicit_offline_values():
    item = assemble_report([COL])["evidence"][0]
    assert item["classification"] == {"status": "not_run", "scanned_at": None}
    assert item["detected_tags"] == []
    assert item["applied_mask"] is None
    assert item["applied_policies"] == []
    assert item["grants"] == []


def test_json_and_markdown_outputs(tmp_path):
    report = assemble_report([COL], generated_at=datetime(2026, 9, 26, tzinfo=timezone.utc))
    json_path, md_path = write_report(report, tmp_path)
    assert json.loads(json_path.read_text())["report_type"] == "genierails.compliance_evidence"
    markdown = md_path.read_text()
    assert "# GenieRails Compliance Evidence" in markdown
    assert "main.sales.customers | email | not_run" in markdown
    assert render_markdown(report) == markdown


def test_cli_stays_offline_unless_integration_flag_is_set(tmp_path, monkeypatch):
    env_dir = tmp_path / "aws" / "envs" / "dev"
    (env_dir / "data_access").mkdir(parents=True)
    (env_dir / "data_access" / "abac.auto.tfvars").write_text('''
tag_assignments = [{
  entity_type = "columns"
  entity_name = "main.sales.customers.email"
  tag_key = "pii_level"
  tag_value = "email"
}]
''')
    monkeypatch.delenv("GENIERAILS_EVIDENCE_INTEGRATION", raising=False)

    def forbidden_collector(*args):
        raise AssertionError("offline mode made a live call")

    assert main(["--env-dir", str(env_dir)], collector=forbidden_collector) == 0
    artifact = json.loads((_out(env_dir) / "compliance-evidence-v1.0.json").read_text())
    assert artifact["header"]["source"] == "offline-config"
    assert artifact["evidence"][0]["detected_tags"] == [{"name": "pii_level", "value": "email"}]


def _offline_env(tmp_path, monkeypatch, env="prod"):
    monkeypatch.delenv("GENIERAILS_EVIDENCE_INTEGRATION", raising=False)
    env_dir = tmp_path / "aws" / "envs" / env
    env_dir.mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text('uc_tables = ["main.sales.customers"]\n')
    return env_dir


def _mode(path):
    return path.stat().st_mode & 0o777


def test_default_output_is_the_env_state_dir_owner_only_never_envs(tmp_path, monkeypatch, capsys):
    from scripts import release_gate as rg

    env_dir = _offline_env(tmp_path, monkeypatch)
    out = rg.env_state(tmp_path / "aws", "prod") / "evidence"
    out.mkdir(parents=True)
    out.chmod(0o755)
    (out / "compliance-evidence-v1.0.json").write_text("{}")
    (out / "compliance-evidence-v1.0.json").chmod(0o644)

    assert main(["--env-dir", str(env_dir)]) == 0

    assert evidence_report.evidence_dir(env_dir) == out == _out(env_dir)
    assert sorted(p.name for p in out.iterdir()) == ["compliance-evidence-v1.0.json", "compliance-evidence-v1.0.md"]
    assert _mode(out) == 0o700 and all(_mode(p) == 0o600 for p in out.iterdir())
    assert json.loads((out / "compliance-evidence-v1.0.json").read_text())["header"]["source"] == "offline-config"
    assert sorted(p.name for p in env_dir.rglob("*")) == ["env.auto.tfvars"]
    assert str(out / "compliance-evidence-v1.0.json") in capsys.readouterr().out


def test_state_dir_setting_holds_the_evidence(tmp_path, monkeypatch):
    env_dir = _offline_env(tmp_path, monkeypatch)
    state = tmp_path / "state"
    state.mkdir()
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(state))
    assert main(["--env-dir", str(env_dir)]) == 0
    assert (state / "aws" / "prod" / "evidence" / "compliance-evidence-v1.0.json").is_file()
    assert not (tmp_path / "aws" / ".genierails-state").exists()


def test_a_missing_state_dir_refuses_and_writes_nothing(tmp_path, monkeypatch, capsys):
    env_dir = _offline_env(tmp_path, monkeypatch)
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "unmounted"))
    assert main(["--env-dir", str(env_dir)]) == 2
    assert "No evidence was written" in capsys.readouterr().err
    assert not list(tmp_path.rglob("compliance-evidence-*"))


def test_live_without_auth_fails_clearly_and_points_at_offline(tmp_path, monkeypatch, capsys):
    env_dir = _offline_env(tmp_path, monkeypatch)
    monkeypatch.setenv("GENIERAILS_EVIDENCE_INTEGRATION", "1")
    assert main(["--env-dir", str(env_dir), "--warehouse-id", "warehouse-1"]) == 2
    err = capsys.readouterr().err
    assert "auth.auto.tfvars" in err and "OFFLINE=1" in err and "No evidence was written" in err
    assert not list(tmp_path.rglob("compliance-evidence-*"))


def test_an_earlier_envs_evidence_folder_is_left_alone_with_a_note(tmp_path, monkeypatch, capsys):
    env_dir = _offline_env(tmp_path, monkeypatch)
    old = env_dir / "generated" / "evidence"
    old.mkdir(parents=True)
    (old / "compliance-evidence-v1.0.json").write_text("{}")
    assert main(["--env-dir", str(env_dir)]) == 0
    assert (old / "compliance-evidence-v1.0.json").read_text() == "{}"
    assert f"NOTE: {old} is from an earlier GenieRails" in capsys.readouterr().out


def _git_repo(tmp_path, monkeypatch, gitignore=""):
    """A checkout at tmp_path/repo holding aws/envs/prod; returns (repo, env_dir)."""
    import subprocess

    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))
    repo = tmp_path / "repo"
    env_dir = _offline_env(repo, monkeypatch)
    if gitignore:
        (repo / ".gitignore").write_text(gitignore)
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    return repo, env_dir


def _git_status(repo):
    import subprocess

    return subprocess.run(["git", "-C", str(repo), "status", "--porcelain", "--untracked-files=all"],
                          text=True, capture_output=True, check=True).stdout


def _refused(capsys, tmp_path):
    err = capsys.readouterr().err
    assert "inside the git work tree" in err and "not git-ignored" in err and "No evidence was written" in err
    assert not list(tmp_path.rglob("compliance-evidence-*"))


def test_a_state_dir_inside_the_checkout_that_git_would_commit_is_refused(tmp_path, monkeypatch, capsys):
    repo, env_dir = _git_repo(tmp_path, monkeypatch, gitignore="/aws/.genierails-state/\n")
    (repo / "custom-state").mkdir()
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(repo / "custom-state"))
    assert main(["--env-dir", str(env_dir)]) == 2
    _refused(capsys, tmp_path)


def test_a_state_dir_symlinked_into_the_checkout_is_refused(tmp_path, monkeypatch, capsys):
    repo, env_dir = _git_repo(tmp_path, monkeypatch, gitignore="/aws/.genierails-state/\n")
    (repo / "custom-state").mkdir()
    (tmp_path / "state-link").symlink_to(repo / "custom-state")
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "state-link"))
    assert main(["--env-dir", str(env_dir)]) == 2
    _refused(capsys, tmp_path)
    # ...and the default folder, when it is a symlink to an unignored place in the checkout.
    monkeypatch.delenv("GENIERAILS_STATE_DIR")
    (repo / "aws" / ".genierails-state").symlink_to(repo / "custom-state")
    assert main(["--env-dir", str(env_dir)]) == 2
    _refused(capsys, tmp_path)


def test_a_git_ignored_state_dir_inside_the_checkout_is_allowed(tmp_path, monkeypatch):
    repo, env_dir = _git_repo(tmp_path, monkeypatch, gitignore="/custom-state/\n")
    (repo / "custom-state").mkdir()
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(repo / "custom-state"))
    assert main(["--env-dir", str(env_dir)]) == 0
    assert (repo / "custom-state/aws/prod/evidence/compliance-evidence-v1.0.json").is_file()
    assert "custom-state" not in _git_status(repo)


def test_a_tracked_report_in_an_ignored_folder_is_refused(tmp_path, monkeypatch, capsys):
    import subprocess

    repo, env_dir = _git_repo(tmp_path, monkeypatch, gitignore="/custom-state/\n")
    tracked = repo / "custom-state/aws/prod/evidence/compliance-evidence-v1.0.json"
    tracked.parent.mkdir(parents=True)
    tracked.write_text("{}")
    subprocess.run(["git", "-C", str(repo), "add", "-f", str(tracked)], check=True)
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(repo / "custom-state"))
    assert main(["--env-dir", str(env_dir)]) == 2
    assert "not git-ignored" in capsys.readouterr().err and tracked.read_text() == "{}"


def test_git_env_variables_cannot_point_the_check_at_another_repo(tmp_path, monkeypatch, capsys):
    """Bypass 1: GIT_DIR/GIT_WORK_TREE once sent discovery to another repo, so the record was written."""
    import subprocess

    repo, env_dir = _git_repo(tmp_path, monkeypatch, gitignore="/aws/.genierails-state/\n")
    (repo / "custom-state").mkdir()
    other = tmp_path / "other"
    subprocess.run(["git", "init", "-q", str(other)], check=True)
    (other / ".gitignore").write_text("*\n")
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(repo / "custom-state"))
    monkeypatch.setenv("GIT_DIR", str(other / ".git"))
    monkeypatch.setenv("GIT_WORK_TREE", str(other))
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(repo))
    assert main(["--env-dir", str(env_dir)]) == 2
    _refused(capsys, tmp_path)


def test_a_nested_repo_around_the_env_does_not_hide_the_customer_repo(tmp_path, monkeypatch, capsys):
    """Bypass 2: only the innermost repo (around env_dir) was checked."""
    import subprocess

    repo, _ = _git_repo(tmp_path, monkeypatch, gitignore="/aws/.genierails-state/\n")
    nested = repo / "nested"
    env_dir = _offline_env(nested, monkeypatch)
    subprocess.run(["git", "init", "-q", str(nested)], check=True)
    (repo / "custom-state").mkdir()
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(repo / "custom-state"))
    assert main(["--env-dir", str(env_dir)]) == 2
    _refused(capsys, tmp_path)
    # Inside both: ignored by the outer repo is not enough when the nested one would commit it...
    (repo / ".gitignore").write_text("/aws/.genierails-state/\n/nested/custom-state/\n")
    (nested / "custom-state").mkdir()
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(nested / "custom-state"))
    assert main(["--env-dir", str(env_dir)]) == 2
    _refused(capsys, tmp_path)
    # ...and passes once every work tree around it ignores it.
    (nested / ".gitignore").write_text("/custom-state/\n")
    assert main(["--env-dir", str(env_dir)]) == 0


def test_a_case_alias_of_the_checkout_path_is_still_inside_it(tmp_path, monkeypatch, capsys):
    """Bypass 3: on a case-insensitive filesystem, <Repo> spelled in another case passed a string prefix test."""
    repo, env_dir = _git_repo(tmp_path, monkeypatch, gitignore="/aws/.genierails-state/\n")
    (repo / "custom-state").mkdir()
    alias = tmp_path / "REPO"
    if not alias.exists():
        pytest.skip("case-sensitive filesystem: no case alias exists")
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(alias / "custom-state"))
    assert main(["--env-dir", str(env_dir)]) == 2
    _refused(capsys, tmp_path)


def test_missing_git_refuses_cleanly_inside_a_work_tree(tmp_path, monkeypatch, capsys):
    """Bypass 4: no git on PATH once raised a traceback (exit 1); it must refuse with exit 2."""
    repo, env_dir = _git_repo(tmp_path, monkeypatch, gitignore="/aws/.genierails-state/\n")
    monkeypatch.setenv("PATH", str(tmp_path / "no-git"))
    assert main(["--env-dir", str(env_dir)]) == 2
    err = capsys.readouterr().err
    assert "git can't be run" in err and "No evidence was written" in err and "Traceback" not in err
    assert not list(tmp_path.rglob("compliance-evidence-*"))
    # Outside every work tree git isn't needed.
    (tmp_path / "state").mkdir()
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "state"))
    assert main(["--env-dir", str(env_dir)]) == 0


def test_a_state_dir_outside_the_checkout_and_the_ignored_default_are_allowed(tmp_path, monkeypatch):
    repo, env_dir = _git_repo(tmp_path, monkeypatch, gitignore="/aws/.genierails-state/\n")
    (tmp_path / "state").mkdir()
    monkeypatch.setenv("GENIERAILS_STATE_DIR", str(tmp_path / "state"))
    assert main(["--env-dir", str(env_dir)]) == 0
    assert (tmp_path / "state/aws/prod/evidence/compliance-evidence-v1.0.md").is_file()
    monkeypatch.delenv("GENIERAILS_STATE_DIR")
    assert main(["--env-dir", str(env_dir)]) == 0
    assert (_out(env_dir) / "compliance-evidence-v1.0.md").is_file()
    assert _git_status(repo) == "?? .gitignore\n?? aws/envs/prod/env.auto.tfvars\n"


def test_configured_tables_includes_all_genie_tables_and_governance_fallback(tmp_path):
    (tmp_path / "data_access").mkdir()
    (tmp_path / "env.auto.tfvars").write_text('''
uc_tables = ["main.sales.orders"]
genie_spaces = [{ name = "support", uc_tables = ["main.support.tickets"] }]
''')
    (tmp_path / "data_access" / "abac.auto.tfvars").write_text('''
tag_assignments = [{ entity_type = "columns", entity_name = "main.hr.people.ssn", tag_key = "sensitivity", tag_value = "ssn" }]
''')
    assert configured_tables(tmp_path) == [
        "main.hr.people", "main.sales.orders", "main.support.tickets"
    ]


def test_configured_tables_includes_discovered_only_footprint(tmp_path):
    (tmp_path / "data_access").mkdir()
    (tmp_path / "env.auto.tfvars").write_text("genie_spaces = []\n")
    (tmp_path / "data_access/discovered_uc_tables.auto.tfvars").write_text(
        'discovered_uc_tables = ["main.discovered.orders"]\n'
    )
    assert configured_tables(tmp_path) == ["main.discovered.orders"]


def test_live_collector_uses_information_schema_mask_and_filter_columns(monkeypatch, tmp_path):
    statements = []

    class Response:
        statement_id = "statement-1"
        status = type("Status", (), {"state": "SUCCEEDED"})()

        def __init__(self, rows):
            self.result = type("Result", (), {"data_array": rows})()

    class Execution:
        def execute_statement(self, *, statement, warehouse_id, wait_timeout):
            statements.append(statement)
            rows = [["main", "sales", "customers", "email"]] if "information_schema.columns WHERE" in statement else []
            return Response(rows)

    class Client:
        def __init__(self, **kwargs):
            self.statement_execution = Execution()
            self.policies = NoPolicies()

    monkeypatch.setattr("scripts.auth_configured.workspace_client", lambda *_args, **_kwargs: Client())
    auth = _auth_file(tmp_path)
    collect_live_state([COL], ["main.sales.customers"], "warehouse-1", auth)

    mask_sql = next(sql for sql in statements if "information_schema.column_masks" in sql)
    filter_sql = next(sql for sql in statements if "information_schema.row_filters" in sql)
    assert "table_catalog" in mask_sql and "mask_name" in mask_sql
    assert "catalog_name" not in mask_sql and "mask_catalog" not in mask_sql
    assert "table_catalog" in filter_sql and "filter_name" in filter_sql
    assert "catalog_name" not in filter_sql and "filter_catalog" not in filter_sql


def test_live_collector_uses_catalog_scoped_column_privileges(monkeypatch, tmp_path):
    statements = []

    class Response:
        statement_id = "statement-1"
        status = type("Status", (), {"state": "SUCCEEDED"})()

        def __init__(self, rows):
            self.result = type("Result", (), {"data_array": rows})()

    class Execution:
        def execute_statement(self, *, statement, warehouse_id, wait_timeout):
            statements.append(statement)
            if "information_schema.columns WHERE" in statement:
                rows = [["odd`catalog", "sales", "customers", "email"]]
            elif statement.startswith("SHOW TABLES"):
                rows = [["information_schema", "column_privileges", "false"]]
            else:
                rows = []
            return Response(rows)

    class Client:
        def __init__(self, **kwargs):
            self.statement_execution = Execution()
            self.policies = NoPolicies()

    monkeypatch.setattr("scripts.auth_configured.workspace_client", lambda *_args, **_kwargs: Client())
    state = collect_live_state([COL], ["odd`catalog.sales.customers"], "warehouse-1", _auth_file(tmp_path))

    privilege_sql = next(sql for sql in statements if "information_schema.column_privileges" in sql)
    assert "FROM `odd``catalog`.information_schema.column_privileges" in privilege_sql
    assert "system.information_schema.column_privileges" not in privilege_sql
    assert state["column_privileges_availability"] == {"odd`catalog": "available"}


def test_live_collector_tolerates_missing_column_privileges(monkeypatch, tmp_path):
    statements = []

    class Response:
        statement_id = "statement-1"
        status = type("Status", (), {"state": "SUCCEEDED"})()

        def __init__(self, rows):
            self.result = type("Result", (), {"data_array": rows})()

    class Execution:
        def execute_statement(self, *, statement, warehouse_id, wait_timeout):
            statements.append(statement)
            rows = [["main", "sales", "customers", "email"]] if "information_schema.columns WHERE" in statement else []
            return Response(rows)

    class Client:
        def __init__(self, **kwargs):
            self.statement_execution = Execution()
            self.policies = NoPolicies()

    monkeypatch.setattr("scripts.auth_configured.workspace_client", lambda *_args, **_kwargs: Client())
    state = collect_live_state([COL], ["main.sales.customers"], "warehouse-1", _auth_file(tmp_path))

    assert state["grants"] == []
    assert state["column_privileges_availability"] == {"main": "not_available"}
    assert any(sql == "SHOW TABLES IN `main`.information_schema" for sql in statements)
    assert not any(".column_privileges WHERE" in sql for sql in statements)



# ---------------------------------------------------------------------------
# ABAC policies (deterministic governance): shaped like the AWS prod-2 state
# after a release — 15 catalog-level column-mask policies, gr_treatment tags,
# and nothing in information_schema.column_masks or row_filters.
# ---------------------------------------------------------------------------

PROD_CATALOG = "prod_retail"
TIERS = ["prod_tier_raw", "prod_tier_partial", "prod_tier_masked"]
DEPLOYER = "11111111-2222-3333-4444-555555555555"
SSN = "ssn__string__8277f91e"
CVV = "card_security_code__string__26d4b17e"
PROD_VALUES = [
    SSN, "email_partial__string__f86256b2", "phone_partial__string__0a1b2c3d",
    "full_name__string__11111111", "date_year__date__7d691823",
    "card_number__string__22222222", "account_number__string__33333333", CVV,
]
PROD_COLUMNS = {
    "prod_retail.crm.customers.customer_id": None,
    "prod_retail.crm.customers.ssn": SSN,
    "prod_retail.crm.customers.email": "email_partial__string__f86256b2",
    "prod_retail.crm.customers.phone": "phone_partial__string__0a1b2c3d",
    "prod_retail.crm.customers.full_name": "full_name__string__11111111",
    "prod_retail.crm.customers.birth_date": "date_year__date__7d691823",
    "prod_retail.payments.cards.card_number": "card_number__string__22222222",
    "prod_retail.payments.cards.cvv": CVV,
    "prod_retail.payments.cards.account_number": "account_number__string__33333333",
}
PROD_TABLES = ["prod_retail.crm.customers", "prod_retail.payments.cards"]


def _prod_policies():
    from databricks.sdk.service.catalog import (ColumnMaskOptions, MatchColumn, PolicyInfo,
                                                PolicyType, SecurableType)
    from governance_policies import build_policies

    rendered = build_policies(values_by_catalog={PROD_CATALOG: PROD_VALUES}, access_tier_groups=TIERS,
                              deployer_principal=DEPLOYER, function_schemas={PROD_CATALOG: "crm"})
    return [PolicyInfo(
        name=f"{policy['catalog']}_{policy['name']}",
        to_principals=policy["to_principals"], except_principals=policy["except_principals"] or None,
        policy_type=PolicyType.POLICY_TYPE_COLUMN_MASK, for_securable_type=SecurableType.TABLE,
        on_securable_type=SecurableType.CATALOG, on_securable_fullname=PROD_CATALOG,
        column_mask=ColumnMaskOptions(
            function_name=f"{policy['function_catalog']}.{policy['function_schema']}.{policy['function_name']}",
            on_column=policy["match_alias"]),
        match_columns=[MatchColumn(alias=policy["match_alias"], condition=policy["match_condition"])],
    ) for policy in rendered]


def _prod_client(monkeypatch, *, policies=None, list_error=None, masks=(), filters=(), table_tags=(),
                 table_tags_error=None):
    calls = []

    class Response:
        statement_id = "statement-1"
        status = type("Status", (), {"state": "SUCCEEDED"})()

        def __init__(self, rows):
            self.result = type("Result", (), {"data_array": rows})()

    class Execution:
        def execute_statement(self, *, statement, warehouse_id, wait_timeout):
            if "information_schema.columns WHERE" in statement:
                return Response([column.split(".") for column in PROD_COLUMNS])
            if "information_schema.column_tags" in statement:
                return Response([column.split(".") + ["gr_treatment", value]
                                 for column, value in PROD_COLUMNS.items() if value])
            if "information_schema.column_masks" in statement:
                return Response([list(row) for row in masks])
            if "information_schema.row_filters" in statement:
                return Response([list(row) for row in filters])
            if "information_schema.table_tags" in statement:
                if table_tags_error:
                    raise table_tags_error
                return Response([list(row) for row in table_tags])
            return Response([])

    class Policies:
        def list_policies(self, on_securable_type, on_securable_fullname, include_inherited=None):
            calls.append((on_securable_type, on_securable_fullname, include_inherited))
            if list_error:
                raise list_error
            return iter(_prod_policies() if policies is None else policies)

    class Client:
        def __init__(self, **kwargs):
            self.statement_execution = Execution()
            self.policies = Policies()

    monkeypatch.setattr("scripts.auth_configured.workspace_client", lambda *_args, **_kwargs: Client())
    return calls


def _evidence(report, column):
    catalog, schema, table, name = column.split(".")
    return next(item for item in report["evidence"] if (
        item["catalog"], item["schema"], item["table"], item["column"]) == (catalog, schema, table, name))


def _prod_report(tmp_path):
    state = collect_live_state([], PROD_TABLES, "warehouse-1", _auth_file(tmp_path))
    return state, assemble_report(state["columns"], tags=state["tags"], masks=state["masks"],
                                  policies=state["policies"], abac_policies=state["abac_policies"],
                                  grants=state["grants"])


def test_abac_policies_protect_ssn_and_cvv_without_information_schema_masks(monkeypatch, tmp_path):
    calls = _prod_client(monkeypatch)
    state, report = _prod_report(tmp_path)

    assert len(_prod_policies()) == 15
    assert calls == [("TABLE", table, True) for table in PROD_TABLES]
    assert state["masks"] == [] and state["policies"] == []
    assert state["abac_policy_listing"] == {"status": "complete", "tables": 2, "policies": 15}
    assert state["abac_policies_not_evaluated"] == []

    ssn = _evidence(report, "prod_retail.crm.customers.ssn")
    assert ssn["applied_mask"] is None
    assert ssn["applied_policies"] == [
        f"prod_retail_gr_mask_prod_retail_{SSN}_full", f"prod_retail_gr_mask_prod_retail_{SSN}_partial"]
    by_name = {policy["name"]: policy for policy in ssn["applied_abac_policies"]}
    full = by_name[f"prod_retail_gr_mask_prod_retail_{SSN}_full"]
    partial = by_name[f"prod_retail_gr_mask_prod_retail_{SSN}_partial"]
    assert full == {
        "name": f"prod_retail_gr_mask_prod_retail_{SSN}_full", "policy_type": "COLUMN_MASK",
        "on_securable": "prod_retail", "function": f"prod_retail.crm.gr_mask_{SSN}_full",
        "match_conditions": [f"hasTagValue('gr_treatment', '{SSN}')"], "when_condition": "",
        "to_principals": ["account users"],
        "except_principals": sorted(["prod_tier_raw", "prod_tier_partial", DEPLOYER]),
    }
    assert partial["to_principals"] == ["prod_tier_partial"]
    assert partial["except_principals"] == sorted(["prod_tier_raw", DEPLOYER])

    cvv = _evidence(report, "prod_retail.payments.cards.cvv")
    assert cvv["applied_policies"] == [f"prod_retail_gr_mask_prod_retail_{CVV}_full"]
    [cvv_policy] = cvv["applied_abac_policies"]
    assert cvv_policy["to_principals"] == ["account users"]
    assert cvv_policy["except_principals"] == []

    # Every tagged column carries exactly its own value's policies; the
    # untagged one carries none.
    for column, value in PROD_COLUMNS.items():
        item = _evidence(report, column)
        if value is None:
            assert item["applied_policies"] == [] and item["applied_abac_policies"] == []
        else:
            assert item["applied_policies"] and all(value in name for name in item["applied_policies"])


def test_abac_policies_reach_the_written_artifacts(monkeypatch, tmp_path, capsys):
    _prod_client(monkeypatch)
    env_dir = tmp_path / "aws" / "envs" / "prod"
    env_dir.mkdir(parents=True)
    _auth_file(env_dir)
    (env_dir / "env.auto.tfvars").write_text(f"uc_tables = {json.dumps(PROD_TABLES)}\n")
    monkeypatch.setenv("GENIERAILS_EVIDENCE_INTEGRATION", "1")

    assert main(["--env-dir", str(env_dir), "--warehouse-id", "warehouse-1"]) == 0

    out = _out(env_dir)
    artifact = json.loads((out / "compliance-evidence-v1.0.json").read_text())
    assert _evidence(artifact, "prod_retail.crm.customers.ssn")["applied_policies"] != []
    assert _evidence(artifact, "prod_retail.payments.cards.cvv")["applied_policies"] != []
    assert artifact["abac_policy_listing"] == {"status": "complete", "tables": 2, "policies": 15}
    markdown = (out / "compliance-evidence-v1.0.md").read_text()
    assert "- ABAC policies: `complete` (15 effective on 2 tables)" in markdown
    cvv_row = next(line for line in markdown.splitlines() if "| cvv |" in line)
    assert f"prod_retail_gr_mask_prod_retail_{CVV}_full (to: account users; except: none)" in cvv_row
    assert "selected-secret" not in capsys.readouterr().out + markdown + json.dumps(artifact)


def test_policy_listing_error_writes_no_evidence(monkeypatch, tmp_path, capsys):
    _prod_client(monkeypatch, list_error=PermissionError("User does not have MANAGE on catalog prod_retail"))
    env_dir = tmp_path / "aws" / "envs" / "prod"
    env_dir.mkdir(parents=True)
    _auth_file(env_dir)
    (env_dir / "env.auto.tfvars").write_text(f"uc_tables = {json.dumps(PROD_TABLES)}\n")
    monkeypatch.setenv("GENIERAILS_EVIDENCE_INTEGRATION", "1")
    rc = main(["--env-dir", str(env_dir), "--warehouse-id", "warehouse-1"])
    assert not _out(env_dir).exists() and not (env_dir / "generated").exists(), \
        "a report with empty applied_policies was written"
    assert rc == 2
    captured = capsys.readouterr()
    assert "could not list the ABAC policies effective on prod_retail.crm.customers" in captured.err
    assert "No evidence was written" in captured.err
    assert "selected-secret" not in captured.out + captured.err

    with pytest.raises(evidence_report.EvidenceUnavailable, match="prod_retail.crm.customers"):
        collect_live_state([], PROD_TABLES, "warehouse-1", _auth_file(tmp_path))


def _mask(name, *, condition=f"hasTagValue('gr_treatment', '{SSN}')", on_column="masked_col",
          matches=None, when=None, on=PROD_CATALOG):
    from databricks.sdk.service.catalog import ColumnMaskOptions, MatchColumn, PolicyInfo, PolicyType, SecurableType

    return PolicyInfo(
        name=name, to_principals=["account users"], policy_type=PolicyType.POLICY_TYPE_COLUMN_MASK,
        for_securable_type=SecurableType.TABLE, on_securable_fullname=on, when_condition=when,
        column_mask=ColumnMaskOptions(function_name="prod_retail.crm.ext_mask", on_column=on_column),
        match_columns=matches if matches is not None else [MatchColumn(alias=on_column, condition=condition)])


def _not_attached(report, name):
    return all(name not in item["applied_policies"]
               and name not in {p["name"] for p in item["applied_abac_policies"]} for item in report["evidence"])


def test_unevaluated_when_condition_is_not_shown_as_protection(monkeypatch, tmp_path):
    """Codex repro 1: an inherited mask gated on a table tag nothing shows the table has."""
    _prod_client(monkeypatch, policies=_prod_policies() + [
        _mask("regional_ssn", when="hasTagValue('region', 'us')")])
    state, report = _prod_report(tmp_path)
    assert _not_attached(report, "regional_ssn")
    # No table carries region=us, so the policy doesn't apply: it is neither
    # protection nor an unevaluated mapping.
    assert state["abac_policies_not_evaluated"] == []
    assert len(_evidence(report, "prod_retail.crm.customers.ssn")["applied_abac_policies"]) == 2


def test_unparseable_when_condition_is_listed_not_evaluated(monkeypatch, tmp_path):
    _prod_client(monkeypatch, policies=_prod_policies() + [
        _mask("regional_ssn", when="NOT hasTagValue('region', 'eu')")])
    state, report = _prod_report(tmp_path)
    assert _not_attached(report, "regional_ssn")
    assert state["abac_policy_listing"]["status"] == "incomplete"
    assert [(item["table"], item["policy"]) for item in state["abac_policies_not_evaluated"]] == [
        ("prod_retail.crm.customers", "regional_ssn"), ("prod_retail.payments.cards", "regional_ssn")]
    assert "when_condition" in state["abac_policies_not_evaluated"][0]["reason"]


def test_when_condition_matching_table_tags_attaches_and_non_matching_does_not(monkeypatch, tmp_path):
    _prod_client(monkeypatch, policies=_prod_policies() + [
        _mask("us_ssn", when="hasTagValue('region', 'us') OR hasTag('us_only')"),
        _mask("eu_ssn", when="hasTagValue('region', 'eu')")],
        table_tags=[["prod_retail", "crm", "customers", "region", "us"]])
    state, report = _prod_report(tmp_path)
    ssn = _evidence(report, "prod_retail.crm.customers.ssn")
    assert "us_ssn" in ssn["applied_policies"]
    us = next(p for p in ssn["applied_abac_policies"] if p["name"] == "us_ssn")
    assert us["when_condition"] == "hasTagValue('region', 'us') OR hasTag('us_only')"
    assert _not_attached(report, "eu_ssn")
    assert state["abac_policies_not_evaluated"] == []
    assert state["abac_policy_listing"]["status"] == "complete"


def test_mask_without_a_match_alias_for_on_column_is_not_mapped(monkeypatch, tmp_path):
    """Codex repro 2: no match column's alias is the mask's on_column."""
    from databricks.sdk.service.catalog import MatchColumn

    _prod_client(monkeypatch, policies=_prod_policies() + [
        _mask("misbound", on_column="target", matches=[
            MatchColumn(alias="other", condition=f"hasTagValue('gr_treatment', '{SSN}')")])])
    state, report = _prod_report(tmp_path)
    assert _not_attached(report, "misbound")
    assert state["abac_policy_listing"]["status"] == "incomplete"
    reasons = {item["reason"] for item in state["abac_policies_not_evaluated"] if item["policy"] == "misbound"}
    assert reasons and all("on_column" in reason for reason in reasons)


@pytest.mark.parametrize("condition", [
    "NOT hasTagValue('gr_treatment', 'x')",
    "hasTagValue('gr_treatment', 'x') AND hasTag('pii')",
    "",
])
def test_unevaluable_match_condition_is_listed_not_evaluated(monkeypatch, tmp_path, condition):
    _prod_client(monkeypatch, policies=_prod_policies() + [_mask("external_mask", condition=condition)])
    state, report = _prod_report(tmp_path)
    assert _not_attached(report, "external_mask")
    assert {item["policy"] for item in state["abac_policies_not_evaluated"]} == {"external_mask"}
    assert state["abac_policy_listing"]["status"] == "incomplete"
    # The governed columns keep their own, fully evaluated protection.
    assert len(_evidence(report, "prod_retail.payments.cards.cvv")["applied_abac_policies"]) == 1


def test_unknown_policy_type_is_listed_not_evaluated_and_flagged_in_markdown(monkeypatch, tmp_path):
    from databricks.sdk.service.catalog import PolicyInfo, PolicyType, SecurableType

    policy = PolicyInfo(name="deny_all", to_principals=["account users"], policy_type=PolicyType.POLICY_TYPE_DENY,
                        for_securable_type=SecurableType.TABLE, on_securable_fullname=PROD_CATALOG)
    _prod_client(monkeypatch, policies=_prod_policies() + [policy])
    env_dir = tmp_path / "aws" / "envs" / "prod"
    env_dir.mkdir(parents=True)
    _auth_file(env_dir)
    (env_dir / "env.auto.tfvars").write_text(f"uc_tables = {json.dumps(PROD_TABLES)}\n")
    monkeypatch.setenv("GENIERAILS_EVIDENCE_INTEGRATION", "1")
    assert main(["--env-dir", str(env_dir), "--warehouse-id", "warehouse-1"]) == 0
    out = _out(env_dir)
    artifact = json.loads((out / "compliance-evidence-v1.0.json").read_text())
    assert artifact["abac_policy_listing"] == {"status": "incomplete", "tables": 2, "policies": 16}
    assert {item["policy"] for item in artifact["abac_policies_not_evaluated"]} == {"deny_all"}
    assert _not_attached(artifact, "deny_all")
    markdown = (out / "compliance-evidence-v1.0.md").read_text()
    assert "- ABAC policies: `incomplete`" in markdown
    assert "WARNING: 2 effective ABAC policy mapping(s) could not be evaluated" in markdown
    assert "NOT shown as protection" in markdown
    assert "| prod_retail.crm.customers | deny_all | prod_retail |" in markdown


def test_principals_that_no_longer_resolve_are_warned_not_claimed_intact(monkeypatch, tmp_path, capsys):
    """F2: a deleted principal shows live as 'unknown'; the report must say so, not read 'complete' and intact."""
    policies = _prod_policies()
    ssn_full = next(p for p in policies if p.name == f"prod_retail_gr_mask_prod_retail_{SSN}_full")
    ssn_full.except_principals = sorted(["unknown" if p == "prod_tier_partial" else p
                                         for p in ssn_full.except_principals])
    cvv_full = next(p for p in policies if p.name == f"prod_retail_gr_mask_prod_retail_{CVV}_full")
    cvv_full.to_principals = ["unknown"]
    _prod_client(monkeypatch, policies=policies)
    env_dir = tmp_path / "aws" / "envs" / "prod"
    env_dir.mkdir(parents=True)
    _auth_file(env_dir)
    (env_dir / "env.auto.tfvars").write_text(f"uc_tables = {json.dumps(PROD_TABLES)}\n")
    monkeypatch.setenv("GENIERAILS_EVIDENCE_INTEGRATION", "1")

    # A warning, not a failure: the masks still apply, so make evidence still writes and exits 0.
    assert main(["--env-dir", str(env_dir), "--warehouse-id", "warehouse-1"]) == 0

    out = _out(env_dir)
    artifact = json.loads((out / "compliance-evidence-v1.0.json").read_text())
    # Catalog-level policies are effective on both tables but listed once each, sorted by securable and name.
    assert artifact["abac_unresolved_principals"] == [
        {"policy": f"prod_retail_gr_mask_prod_retail_{CVV}_full", "on_securable": "prod_retail",
         "to_principals": 1, "except_principals": 0},
        {"policy": f"prod_retail_gr_mask_prod_retail_{SSN}_full", "on_securable": "prod_retail",
         "to_principals": 0, "except_principals": 1},
    ]
    # The listing itself is still complete (every policy was evaluated); masks are still attached.
    assert artifact["abac_policy_listing"] == {"status": "complete", "tables": 2, "policies": 15}
    assert _evidence(artifact, "prod_retail.crm.customers.ssn")["applied_policies"] != []
    warning = (f"2 exception/target principal(s) no longer resolve (deleted?; listed as 'unknown'): "
               f"prod_retail_gr_mask_prod_retail_{CVV}_full (to), "
               f"prod_retail_gr_mask_prod_retail_{SSN}_full (except). Those masks still apply, but the "
               "exceptions/targets they document are NOT intact; fix the principals and re-run make evidence")
    markdown = (out / "compliance-evidence-v1.0.md").read_text()
    assert ("- ABAC policies: `complete` (15 effective on 2 tables; documented exceptions NOT intact, "
            "see the warning below)") in markdown
    assert "- ABAC policies: `complete` (15 effective on 2 tables)\n" not in markdown
    assert f"- **WARNING: {warning}.**" in markdown
    stdout = capsys.readouterr().out
    assert f"WARNING: {warning}." in stdout
    assert "selected-secret" not in stdout + markdown + json.dumps(artifact)


def test_resolved_principals_add_no_warning(monkeypatch, tmp_path, capsys):
    _prod_client(monkeypatch)
    env_dir = tmp_path / "aws" / "envs" / "prod"
    env_dir.mkdir(parents=True)
    _auth_file(env_dir)
    (env_dir / "env.auto.tfvars").write_text(f"uc_tables = {json.dumps(PROD_TABLES)}\n")
    monkeypatch.setenv("GENIERAILS_EVIDENCE_INTEGRATION", "1")
    assert main(["--env-dir", str(env_dir), "--warehouse-id", "warehouse-1"]) == 0
    out = _out(env_dir)
    artifact = json.loads((out / "compliance-evidence-v1.0.json").read_text())
    assert artifact["abac_unresolved_principals"] == []
    markdown = (out / "compliance-evidence-v1.0.md").read_text()
    assert "- ABAC policies: `complete` (15 effective on 2 tables)\n" in markdown
    assert "no longer resolve" not in markdown + capsys.readouterr().out


def test_table_tags_read_failure_writes_no_evidence(monkeypatch, tmp_path, capsys):
    _prod_client(monkeypatch, table_tags_error=RuntimeError("TABLE_OR_VIEW_NOT_FOUND table_tags"))
    with pytest.raises(evidence_report.EvidenceUnavailable, match="table tags"):
        collect_live_state([], PROD_TABLES, "warehouse-1", _auth_file(tmp_path))
    env_dir = tmp_path / "aws" / "envs" / "prod"
    env_dir.mkdir(parents=True)
    _auth_file(env_dir)
    (env_dir / "env.auto.tfvars").write_text(f"uc_tables = {json.dumps(PROD_TABLES)}\n")
    monkeypatch.setenv("GENIERAILS_EVIDENCE_INTEGRATION", "1")
    assert main(["--env-dir", str(env_dir), "--warehouse-id", "warehouse-1"]) == 2
    assert "could not read the table tags" in capsys.readouterr().err
    assert not _out(env_dir).exists() and not (env_dir / "generated").exists()


def test_listing_error_message_is_sanitized_and_capped(monkeypatch, tmp_path):
    # Built at runtime: a fake token, but shaped like a real one.
    token = "dapi" + "0123456789abcdef" * 2
    error = RuntimeError(f"403 Forbidden. Request headers: Authorization: Bearer {token} "
                         f"Cookie: session=abc; {'x' * 500}")
    _prod_client(monkeypatch, list_error=error)
    with pytest.raises(evidence_report.EvidenceUnavailable) as caught:
        collect_live_state([], PROD_TABLES, "warehouse-1", _auth_file(tmp_path))
    message = str(caught.value)
    assert "RuntimeError: 403 Forbidden." in message
    assert token not in message and "session=abc" not in message and "x" * 250 not in message
    assert len(message) < 400


def test_legacy_information_schema_masks_unchanged_and_shown_beside_abac(monkeypatch, tmp_path):
    email = ["prod_retail", "crm", "customers", "email", "prod_retail.crm.legacy_mask_email"]
    cvv = ["prod_retail", "payments", "cards", "cvv", "prod_retail.crm.legacy_mask_cvv"]
    region = ["prod_retail", "crm", "customers", "legacy_region_filter"]

    # A legacy env: no ABAC policy at all. information_schema drives the report as before.
    _prod_client(monkeypatch, policies=[], masks=[email], filters=[region])
    state, report = _prod_report(tmp_path)
    item = _evidence(report, "prod_retail.crm.customers.email")
    assert item["applied_mask"] == "prod_retail.crm.legacy_mask_email"
    assert item["applied_policies"] == ["legacy_region_filter"]
    assert item["applied_abac_policies"] == []
    assert _evidence(report, "prod_retail.payments.cards.cvv")["applied_policies"] == []
    assert state["abac_policy_listing"] == {"status": "complete", "tables": 2, "policies": 0}

    # Both at once: a column covered by both shows both.
    _prod_client(monkeypatch, masks=[cvv], filters=[region])
    _state, report = _prod_report(tmp_path)
    item = _evidence(report, "prod_retail.payments.cards.cvv")
    assert item["applied_mask"] == "prod_retail.crm.legacy_mask_cvv"
    assert item["applied_policies"] == [f"prod_retail_gr_mask_prod_retail_{CVV}_full"]
    ssn = _evidence(report, "prod_retail.crm.customers.ssn")
    assert "legacy_region_filter" in ssn["applied_policies"] and len(ssn["applied_abac_policies"]) == 2
    markdown = render_markdown(report)
    cvv_row = next(line for line in markdown.splitlines() if "| cvv |" in line)
    assert "prod_retail.crm.legacy_mask_cvv, prod_retail_gr_mask_prod_retail_" in cvv_row
