from types import SimpleNamespace
from unittest.mock import MagicMock

from scripts.bootstrap_sp import Config, MODEL_ENDPOINT, bootstrap


def _fake(existing=False):
    account = MagicMock()
    sp = SimpleNamespace(id="42", application_id="client-123", display_name="deploy")
    account.service_principals.list.return_value = [sp] if existing else []
    account.service_principals.create.return_value = sp
    account.groups.list.return_value = [SimpleNamespace(id="admins")]
    account.workspaces.get.return_value = SimpleNamespace(workspace_url="dbc.example.com")
    account.service_principal_secrets.create.return_value = SimpleNamespace(secret="super-secret")
    workspace = MagicMock()
    workspace.metastores.current.return_value = SimpleNamespace(metastore_id="meta-1")
    factory = MagicMock(return_value=workspace)
    return account, workspace, lambda _cfg: (account, factory)


def _cfg(**overrides):
    values = dict(account_id="acct", workspace_ids=(123,), sp_name="deploy", yes=True)
    values.update(overrides)
    return Config(**values)


def test_dry_run_makes_no_client_calls():
    factory = MagicMock()
    output = []
    assert bootstrap(_cfg(dry_run=True), client_factory=factory, emit=output.append) == 0
    factory.assert_not_called()
    assert output[-1] == "DRY RUN: no API calls were made."


def test_apply_creates_and_grants_expected_access():
    account, workspace, factory = _fake()
    output = []
    assert bootstrap(_cfg(), client_factory=factory, emit=output.append) == 0
    account.service_principals.create.assert_called_once_with(display_name="deploy", active=True)
    account.service_principal_secrets.create.assert_called_once_with(service_principal_id=42)
    account.workspace_assignment.update.assert_called_once()
    account.api_client.do.assert_called_once()
    workspace.grants.update.assert_called_once()
    workspace.api_client.do.assert_called_once_with(
        "PATCH", f"/api/2.0/permissions/serving-endpoints/{MODEL_ENDPOINT}",
        body={"access_control_list": [{
            "service_principal_name": "client-123", "permission_level": "CAN_QUERY"
        }]},
    )


def test_existing_sp_is_reused_without_secret_rotation():
    account, _workspace, factory = _fake(existing=True)
    output = []
    assert bootstrap(_cfg(), client_factory=factory, emit=output.append) == 0
    account.service_principals.create.assert_not_called()
    account.service_principal_secrets.create.assert_not_called()
    assert any("OAuth secret unchanged" in line for line in output)


def test_secret_appears_only_in_intended_warning_block_and_snippet():
    _account, _workspace, factory = _fake()
    output = []
    bootstrap(_cfg(), client_factory=factory, emit=output.append)
    secret_lines = [line for line in output if "super-secret" in line]
    assert secret_lines == [
        'databricks_client_secret = "super-secret"',
    ]
    warning_index = next(i for i, line in enumerate(output) if line.startswith("WARNING: STORE THIS NOW"))
    assert 'databricks_client_secret = "super-secret"' in output[warning_index + 3:]
