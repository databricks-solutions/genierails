from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.catalog import Privilege
from databricks.sdk.service.iam import WorkspacePermission

from scripts.bootstrap_sp import Config, bootstrap


def _fake(*, existing=False, existing_secrets=True, roles=()):
    account = MagicMock()
    sp = SimpleNamespace(
        id="42",
        application_id="client-123",
        display_name="deploy",
        roles=[SimpleNamespace(value=role) for role in roles],
    )
    account.service_principals.list.return_value = [sp] if existing else []
    account.service_principals.create.return_value = sp
    account.service_principal_secrets.list.return_value = (
        [SimpleNamespace(id="existing-secret")] if existing_secrets else []
    )
    account.service_principal_secrets.create.return_value = SimpleNamespace(secret="super-secret")
    account.access_control.get_rule_set.return_value = SimpleNamespace(
        etag="etag-1", grant_rules=[]
    )
    account.workspaces.get.return_value = SimpleNamespace(workspace_url="dbc.example.com")
    workspace = MagicMock()
    workspace.metastores.current.return_value = SimpleNamespace(metastore_id="meta-1")
    workspace_factory = MagicMock(return_value=workspace)
    return account, workspace, workspace_factory, lambda _cfg: (account, workspace_factory)


def _cfg(**overrides):
    values = dict(
        account_id="acct",
        workspace_ids=(123,),
        sp_name="deploy",
        yes=True,
        model_endpoint="custom-model",
    )
    values.update(overrides)
    return Config(**values)


def test_dry_run_makes_no_client_calls():
    factory = MagicMock()
    output = []
    assert bootstrap(_cfg(dry_run=True), client_factory=factory, emit=output.append) == 0
    factory.assert_not_called()
    assert output[-1] == "DRY RUN: no API calls were made."


def test_declining_confirmation_makes_no_client_calls():
    factory = MagicMock()
    output = []
    assert bootstrap(
        _cfg(yes=False), client_factory=factory, emit=output.append, ask=lambda _prompt: "no"
    ) == 1
    factory.assert_not_called()
    assert output[-1] == "Aborted; no changes were made."


def test_apply_uses_exact_scoped_grants():
    account, workspace, workspace_factory, factory = _fake()
    assert bootstrap(_cfg(), client_factory=factory, emit=MagicMock()) == 0

    account.service_principals.create.assert_called_once_with(display_name="deploy", active=True)
    account.service_principal_secrets.create.assert_called_once_with(service_principal_id=42)
    account.api_client.do.assert_called_once_with(
        "PATCH",
        "/api/2.0/accounts/acct/scim/v2/ServicePrincipals/42",
        body={
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{
                "op": "add",
                "path": "roles",
                "value": [{"value": "account_admin"}],
            }],
        },
    )
    account.workspace_assignment.update.assert_called_once_with(
        workspace_id=123,
        principal_id=42,
        permissions=[WorkspacePermission.ADMIN],
    )
    workspace_factory.assert_called_once_with("https://dbc.example.com")

    metastore_call = workspace.grants.update.call_args.kwargs
    assert metastore_call["securable_type"] == "metastore"
    assert metastore_call["full_name"] == "meta-1"
    assert len(metastore_call["changes"]) == 1
    assert metastore_call["changes"][0].principal == "client-123"
    assert metastore_call["changes"][0].add == [Privilege.CREATE_CATALOG]
    assert Privilege.ALL_PRIVILEGES not in metastore_call["changes"][0].add

    workspace.api_client.do.assert_called_once_with(
        "PATCH",
        "/api/2.0/permissions/serving-endpoints/custom-model",
        body={"access_control_list": [{
            "service_principal_name": "client-123", "permission_level": "CAN_QUERY"
        }]},
    )


def test_apply_grants_exact_account_tag_policy_roles():
    account, _workspace, _workspace_factory, factory = _fake()
    bootstrap(_cfg(), client_factory=factory, emit=MagicMock())

    rule_name = "accounts/acct/ruleSets/default"
    account.access_control.get_rule_set.assert_called_once_with(name=rule_name, etag="")
    call = account.access_control.update_rule_set.call_args
    assert call.kwargs["name"] == rule_name
    update = call.kwargs["rule_set"]
    assert update.name == rule_name
    assert update.etag == "etag-1"
    assert [(rule.role, rule.principals) for rule in update.grant_rules] == [
        ("roles/tagPolicy.creator", ["servicePrincipals/client-123"]),
        ("roles/tagPolicy.manager", ["servicePrincipals/client-123"]),
    ]


def test_existing_sp_and_grants_are_not_duplicated():
    account, _workspace, _workspace_factory, factory = _fake(
        existing=True, roles=("account_admin",)
    )
    principal = "servicePrincipals/client-123"
    account.access_control.get_rule_set.return_value = SimpleNamespace(
        etag="etag-1",
        grant_rules=[
            SimpleNamespace(role="roles/tagPolicy.creator", principals=[principal]),
            SimpleNamespace(role="roles/tagPolicy.manager", principals=[principal]),
        ],
    )
    output = []
    assert bootstrap(_cfg(), client_factory=factory, emit=output.append) == 0
    account.service_principals.create.assert_not_called()
    account.service_principal_secrets.create.assert_not_called()
    account.api_client.do.assert_not_called()
    account.access_control.update_rule_set.assert_not_called()
    assert any("OAuth secret unchanged" in line for line in output)


def test_existing_sp_without_a_secret_mints_one_before_grants():
    account, _workspace, _workspace_factory, factory = _fake(
        existing=True, existing_secrets=False
    )
    account.api_client.do.side_effect = RuntimeError("grant failed")
    output = []
    with pytest.raises(RuntimeError, match="grant failed"):
        bootstrap(_cfg(), client_factory=factory, emit=output.append)
    account.service_principal_secrets.create.assert_called_once_with(service_principal_id=42)
    assert output.index("client_secret = super-secret") < next(
        index for index, line in enumerate(output) if line.startswith("SP reused")
    ) + 4


def test_secret_is_printed_once_and_never_repeated_in_summary():
    _account, _workspace, _workspace_factory, factory = _fake()
    output = []
    bootstrap(_cfg(), client_factory=factory, emit=output.append)
    assert [line for line in output if "super-secret" in line] == [
        "client_secret = super-secret"
    ]
    warning_index = next(
        index for index, line in enumerate(output) if line.startswith("WARNING: STORE THIS NOW")
    )
    assert output[warning_index + 2] == "client_secret = super-secret"
    assert 'databricks_client_secret = "<newly-minted secret shown above>"' in output
