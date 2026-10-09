from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest
import requests
from databricks.sdk.config import Config as SdkConfig
from databricks.sdk.errors import NotFound, PermissionDenied, Unauthenticated
from databricks.sdk.errors.parser import _Parser
from databricks.sdk.service.catalog import (
    CatalogInfo,
    MetastoreAssignment,
    MetastoreInfo,
    Privilege,
)
from databricks.sdk.service.iam import ComplexValue, User, WorkspacePermission

from scripts.bootstrap_sp import (
    Config,
    _clients,
    _config_from_args,
    _is_auth_error,
    _is_not_found,
    _plan,
    _preflight_target_catalog,
    _workspace_host,
    bootstrap,
    main,
    parser,
)


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
    workspace.metastores.current.return_value = MetastoreAssignment(
        workspace_id=123, metastore_id="meta-1"
    )
    workspace.metastores.get.return_value = MetastoreInfo(
        metastore_id="meta-1", owner="caller@example.com"
    )
    workspace.catalogs.get.return_value = CatalogInfo(owner="caller@example.com")
    workspace.current_user.me.return_value = User(
        user_name="caller@example.com", display_name="Caller",
        groups=[ComplexValue(display="admins")],
    )
    workspace.grants.get_effective.return_value = SimpleNamespace(privilege_assignments=[])
    workspace.api_client.do.return_value = {"id": "abcdef1234567890"}
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


def _preflight(workspace):
    account = MagicMock()
    account.workspaces.get.return_value = SimpleNamespace(workspace_url="dbc.example.com")
    workspace_factory = MagicMock(return_value=workspace)
    _preflight_target_catalog(
        _cfg(target_catalog="existing_catalog"), account, workspace_factory
    )


def _not_metastore_owner(workspace):
    workspace.metastores.get.return_value = MetastoreInfo(
        metastore_id="meta-1", owner="metastore-owner@example.com"
    )


def test_workspace_client_derives_azure_host_from_deployment_name():
    account = MagicMock()
    account.workspaces.get.return_value = SimpleNamespace(
        deployment_name="adb-7405605806702166.6",
        cloud="azure",
    )
    host = _workspace_host(account, 7405605806702166)

    assert host == "https://adb-7405605806702166.6.azuredatabricks.net"


def test_workspace_client_derives_aws_host_from_deployment_name():
    account = MagicMock()
    account.workspaces.get.return_value = SimpleNamespace(
        deployment_name="dbc-b89659bd-e807",
        cloud="aws",
    )
    host = _workspace_host(account, 123)

    assert host == "https://dbc-b89659bd-e807.cloud.databricks.com"


def test_workspace_client_treats_missing_cloud_as_aws():
    account = MagicMock()
    account.workspaces.get.return_value = SimpleNamespace(
        deployment_name="dbc-b89659bd-e807",
    )
    host = _workspace_host(account, 123)

    assert host == "https://dbc-b89659bd-e807.cloud.databricks.com"


def test_workspace_client_preserves_aws_deployment_domain():
    account = MagicMock()
    account.workspaces.get.return_value = SimpleNamespace(
        deployment_name="dbc-b89659bd-e807.cloud.databricks.com",
        cloud="aws",
    )
    host = _workspace_host(account, 123)

    assert host == "https://dbc-b89659bd-e807.cloud.databricks.com"


def test_workspace_client_falls_back_to_dbc_workspace_id():
    account = MagicMock()
    account.workspaces.get.return_value = SimpleNamespace()
    host = _workspace_host(account, 456)

    assert host == "https://dbc-456.cloud.databricks.com"


def test_workspace_client_uses_azure_account_host_when_cloud_is_missing():
    account = MagicMock()
    account.config.host = "https://accounts.azuredatabricks.net"
    account.workspaces.get.return_value = SimpleNamespace(
        deployment_name="adb-7405605806702166.6",
    )
    assert _workspace_host(account, 7405605806702166) == (
        "https://adb-7405605806702166.6.azuredatabricks.net"
    )


def test_workspace_client_never_guesses_aws_host_for_azure_account():
    account = MagicMock()
    account.config.host = "https://accounts.azuredatabricks.net"
    account.workspaces.get.return_value = SimpleNamespace()
    with pytest.raises(ValueError, match="Workspace API returns one of those fields"):
        _workspace_host(account, 456)


@pytest.mark.parametrize("account_host, suffix", [
    ("https://accounts.azuredatabricks.us", "azuredatabricks.us"),
    ("https://accounts.databricks.azure.cn", "databricks.azure.cn"),
])
def test_workspace_client_derives_sovereign_azure_domain(account_host, suffix):
    account = MagicMock()
    account.config.host = account_host
    account.workspaces.get.return_value = SimpleNamespace(deployment_name="adb-123.4")
    assert _workspace_host(account, 123) == f"https://adb-123.4.{suffix}"


@pytest.mark.parametrize("full_host", [
    "adb-123.4.azuredatabricks.us",
    "adb-123.4.databricks.azure.cn",
])
def test_workspace_client_preserves_sovereign_azure_deployment_domain(full_host):
    account = MagicMock()
    account.workspaces.get.return_value = SimpleNamespace(deployment_name=full_host)
    assert _workspace_host(account, 123) == f"https://{full_host}"


def test_preflight_catalog_owner_passes_without_effective_grant_lookup():
    _account, workspace, _workspace_factory, _factory = _fake()

    _preflight(workspace)

    workspace.grants.get_effective.assert_not_called()


def test_preflight_metastore_owner_passes():
    _account, workspace, _workspace_factory, _factory = _fake()
    workspace.catalogs.get.return_value = CatalogInfo(owner="someone-else@example.com")
    workspace.metastores.current.return_value = MetastoreAssignment(
        workspace_id=123, metastore_id="meta-1"
    )
    workspace.metastores.get.return_value = MetastoreInfo(
        metastore_id="meta-1", owner="caller@example.com"
    )

    _preflight(workspace)

    workspace.grants.get_effective.assert_not_called()
    workspace.metastores.get.assert_called_once_with("meta-1")


def test_preflight_workspace_admin_owns_this_workspaces_default_catalog():
    _account, workspace, _workspace_factory, _factory = _fake()
    workspace.catalogs.get.return_value = CatalogInfo(
        owner="_workspace_admins_existing_catalog_123"
    )
    workspace.current_user.me.return_value = User(
        user_name="caller@example.com",
        groups=[ComplexValue(display="admins")],
    )

    _preflight(workspace)

    workspace.grants.get_effective.assert_not_called()


def test_preflight_workspace_admin_owner_name_can_differ_from_catalog_name():
    _account, workspace, _workspace_factory, _factory = _fake()
    workspace.catalogs.get.return_value = CatalogInfo(
        owner="_workspace_admins_original_workspace_name_123"
    )
    workspace.current_user.me.return_value = User(
        user_name="caller@example.com",
        groups=[ComplexValue(display="admins")],
    )

    _preflight(workspace)

    workspace.grants.get_effective.assert_not_called()


def test_preflight_workspace_admin_owner_requires_admins_group_membership():
    _account, workspace, _workspace_factory, _factory = _fake()
    workspace.catalogs.get.return_value = CatalogInfo(
        owner="_workspace_admins_existing_catalog_123"
    )
    workspace.current_user.me.return_value = User(
        user_name="caller@example.com", groups=[]
    )
    _not_metastore_owner(workspace)

    with pytest.raises(RuntimeError, match="caller 'caller@example.com'.*catalog owner"):
        _preflight(workspace)


def test_preflight_display_name_admins_does_not_grant_workspace_admin_authority():
    _account, workspace, _workspace_factory, _factory = _fake()
    workspace.catalogs.get.return_value = CatalogInfo(
        owner="_workspace_admins_existing_catalog_123"
    )
    workspace.current_user.me.return_value = User(
        user_name="caller@example.com", display_name="Admins", groups=[]
    )
    _not_metastore_owner(workspace)

    with pytest.raises(RuntimeError, match="caller 'caller@example.com'.*catalog owner"):
        _preflight(workspace)


def test_preflight_workspace_admin_group_for_another_workspace_does_not_own_catalog():
    _account, workspace, _workspace_factory, _factory = _fake()
    workspace.catalogs.get.return_value = CatalogInfo(
        owner="_workspace_admins_existing_catalog_456"
    )
    workspace.current_user.me.return_value = User(
        user_name="caller@example.com",
        groups=[ComplexValue(display="admins")],
    )
    _not_metastore_owner(workspace)

    with pytest.raises(RuntimeError, match="caller 'caller@example.com'.*catalog owner"):
        _preflight(workspace)


def test_preflight_metastore_get_permission_error_falls_through_to_manage():
    _account, workspace, _workspace_factory, _factory = _fake()
    workspace.catalogs.get.return_value = CatalogInfo(owner="someone-else@example.com")
    workspace.metastores.get.side_effect = PermissionDenied("cannot read metastore")

    with pytest.raises(
        RuntimeError,
        match=r"metastore owner is '<unavailable>'.*lacks effective MANAGE",
    ):
        _preflight(workspace)

    workspace.grants.get_effective.assert_called_once()


def test_preflight_metastore_get_non_permission_error_is_not_swallowed():
    _account, workspace, _workspace_factory, _factory = _fake()
    workspace.metastores.get.side_effect = RuntimeError("metastore lookup failed")

    with pytest.raises(RuntimeError, match="Cause: RuntimeError: metastore lookup failed"):
        _preflight(workspace)


def test_preflight_missing_metastore_id_skips_metastore_lookup():
    _account, workspace, _workspace_factory, _factory = _fake()
    workspace.catalogs.get.return_value = CatalogInfo(owner="someone-else@example.com")
    workspace.metastores.current.return_value = MetastoreAssignment(
        workspace_id=123, metastore_id=None
    )

    with pytest.raises(RuntimeError, match=r"no metastore is assigned"):
        _preflight(workspace)

    workspace.metastores.get.assert_not_called()


def test_preflight_non_owner_with_effective_manage_passes():
    _account, workspace, _workspace_factory, _factory = _fake()
    workspace.catalogs.get.return_value = SimpleNamespace(owner="someone-else@example.com")
    _not_metastore_owner(workspace)
    workspace.grants.get_effective.return_value = SimpleNamespace(
        privilege_assignments=[SimpleNamespace(
            privileges=[SimpleNamespace(privilege="MANAGE")]
        )]
    )

    _preflight(workspace)

    workspace.grants.get_effective.assert_called_once_with(
        securable_type="catalog",
        full_name="existing_catalog",
        principal="caller@example.com",
    )


def test_preflight_non_owner_without_manage_fails():
    _account, workspace, _workspace_factory, _factory = _fake()
    workspace.catalogs.get.return_value = SimpleNamespace(owner="someone-else@example.com")
    _not_metastore_owner(workspace)

    with pytest.raises(RuntimeError, match="preflight failed.*catalog owner"):
        _preflight(workspace)


def test_preflight_owner_ignores_effective_grant_lookup_failure():
    _account, workspace, _workspace_factory, _factory = _fake()
    workspace.grants.get_effective.side_effect = RuntimeError("unavailable")

    _preflight(workspace)

    workspace.grants.get_effective.assert_not_called()


def test_preflight_owner_match_is_case_insensitive():
    _account, workspace, _workspace_factory, _factory = _fake()
    workspace.catalogs.get.return_value = SimpleNamespace(owner="CALLER@EXAMPLE.COM")

    _preflight(workspace)

    workspace.grants.get_effective.assert_not_called()


def test_dry_run_runs_read_only_preflight_and_makes_no_writes():
    account, workspace, workspace_factory, factory = _fake()
    output = []
    assert bootstrap(_cfg(dry_run=True), client_factory=factory, emit=output.append) == 0
    workspace_factory.assert_called_once_with("https://dbc.example.com")
    workspace.api_client.do.assert_called_once_with(
        "GET", "/api/2.0/serving-endpoints/custom-model"
    )
    account.service_principals.create.assert_not_called()
    account.service_principal_secrets.create.assert_not_called()
    workspace.grants.update.assert_not_called()
    assert any("model access CAN_QUERY" in line for line in output)
    assert output[-1] == "DRY RUN: read-only preflight passed; no changes were made."


def test_declining_confirmation_makes_no_client_calls():
    factory = MagicMock()
    output = []
    assert bootstrap(
        _cfg(yes=False), client_factory=factory, emit=output.append, ask=lambda _prompt: "no"
    ) == 1
    factory.assert_not_called()
    assert output[-1] == "Aborted; no changes were made."


def test_target_catalog_flows_from_parser_to_config():
    args = parser().parse_args([
        "--account-id", "acct",
        "--workspace-id", "123,456",
        "--target-catalog", "existing_catalog",
    ])

    cfg = _config_from_args(args)

    assert cfg.workspace_ids == (123, 456)
    assert cfg.target_catalogs == ("existing_catalog", "existing_catalog")


def test_per_workspace_catalogs_align_with_workspace_ids():
    args = parser().parse_args([
        "--account-id", "acct", "--workspace-id", "123,456",
        "--target-catalog", "dev_cat,prod_cat",
    ])
    cfg = _config_from_args(args)
    assert cfg.target_catalogs == ("dev_cat", "prod_cat")
    output = []
    _plan(cfg, output.append)
    assert any("workspace 123" in line and "dev_cat" in line for line in output)
    assert any("workspace 456" in line and "prod_cat" in line for line in output)


def test_bad_target_catalog_count_exits_cleanly(capsys):
    result = main([
        "--account-id", "acct", "--workspace-id", "123,456,789",
        "--target-catalog", "dev_cat,prod_cat", "--dry-run",
    ])
    assert result == 2
    assert "2 catalog(s) for 3 workspace ID(s)" in capsys.readouterr().err


def test_empty_target_catalog_item_exits_cleanly(capsys):
    result = main([
        "--account-id", "acct", "--workspace-id", "123,456",
        "--target-catalog", "dev_cat,,prod_cat", "--dry-run",
    ])
    assert result == 2
    assert "contains an empty catalog name" in capsys.readouterr().err


def test_dry_run_falls_back_offline_only_when_credentials_are_absent():
    output = []
    factory = MagicMock(side_effect=ValueError(
        "default auth: cannot configure default credentials"
    ))
    assert bootstrap(_cfg(dry_run=True), client_factory=factory, emit=output.append) == 0
    assert output[-1].startswith("DRY RUN OFFLINE: credentials are absent")


def test_workspace_profile_flows_from_parser_to_config():
    args = parser().parse_args([
        "--account-id", "acct", "--workspace-id", "123,456",
        "--workspace-profile", "dev,prod",
    ])
    cfg = _config_from_args(args)
    assert cfg.workspace_profiles == ("dev", "prod")


def test_multiple_workspaces_require_one_profile_each():
    args = parser().parse_args([
        "--account-id", "acct", "--workspace-id", "123,456",
        "--workspace-profile", "dev",
    ])
    with pytest.raises(ValueError, match=r"1 profile\(s\).*2 workspace ID\(s\)"):
        _config_from_args(args)


def test_bad_workspace_profile_count_exits_cleanly(capsys):
    result = main([
        "--account-id", "acct",
        "--workspace-id", "123",
        "--workspace-profile", "dev,prod",
    ])

    assert result == 2
    assert (
        "ERROR: --workspace-profile supplied 2 profile(s) for 1 workspace ID(s); "
        "supply exactly one profile per workspace"
    ) in capsys.readouterr().err


def test_clients_reuses_m2m_credentials_for_workspace():
    sdk_config = SimpleNamespace(
        auth_type="oauth-m2m", client_id="client-id", client_secret="client-secret"
    )
    with patch("databricks.sdk.AccountClient"), \
         patch("databricks.sdk.WorkspaceClient") as workspace_client, \
         patch("databricks.sdk.config.Config", return_value=sdk_config):
        _account, factory = _clients(_cfg(profile="account"))
        factory("https://dbc.example.com")
    workspace_client.assert_called_once_with(
        host="https://dbc.example.com",
        client_id="client-id",
        client_secret="client-secret",
    )


def test_clients_reuses_azure_sp_profile_with_workspace_host():
    sdk_config = SimpleNamespace(
        auth_type="azure-client-secret",
        azure_client_id="azure-client",
        azure_client_secret="azure-secret",
        azure_tenant_id="azure-tenant",
        azure_environment="PUBLIC",
        azure_workspace_resource_id="/subscriptions/account-profile/workspaces/wrong",
    )
    with patch("databricks.sdk.AccountClient"), \
         patch("databricks.sdk.WorkspaceClient") as workspace_client, \
         patch("databricks.sdk.config.Config", return_value=sdk_config):
        _account, factory = _clients(_cfg(profile="azure-account-sp"))
        factory("https://adb-123.4.azuredatabricks.net")
    workspace_client.assert_called_once_with(
        host="https://adb-123.4.azuredatabricks.net",
        azure_client_id="azure-client",
        azure_client_secret="azure-secret",
        azure_tenant_id="azure-tenant",
        azure_environment="PUBLIC",
    )
    assert "azure_workspace_resource_id" not in workspace_client.call_args.kwargs


def test_m2m_builds_fresh_workspace_auth_without_mutating_account_config():
    account_host = "https://accounts.cloud.databricks.com"
    workspace_host = "https://dbc.example.com"
    discovered_urls = []

    def oidc_response(_client, _method, url, **_kwargs):
        discovered_urls.append(url)
        token_host = workspace_host if url.startswith(workspace_host) else account_host
        # databricks-sdk >= 0.148 first reads the host metadata and takes the
        # OIDC discovery URL from it; answer as real hosts do (older SDKs
        # never ask).
        if url.endswith("/.well-known/databricks-config"):
            oidc = "/oidc" if token_host == workspace_host else "/oidc/accounts/{account_id}"
            return {"oidc_endpoint": f"{token_host}{oidc}"}
        return {
            "authorization_endpoint": f"{token_host}/oidc/v1/authorize",
            "token_endpoint": f"{token_host}/oidc/v1/token",
        }

    token_response = MagicMock(ok=True)
    token_response.json.return_value = {
        "access_token": "workspace-token",
        "token_type": "Bearer",
        "expires_in": 3600,
    }
    with patch("databricks.sdk.oauth._BaseClient.do", autospec=True,
               side_effect=oidc_response), \
         patch("databricks.sdk.oauth.requests.post", return_value=token_response) as post:
        account_config = SdkConfig(
            host=account_host,
            account_id="acct",
            client_id="client-id",
            client_secret="client-secret",
            auth_type="oauth-m2m",
        )
        account_header_factory = account_config._header_factory

        with patch("databricks.sdk.AccountClient"), \
             patch("databricks.sdk.config.Config", return_value=account_config):
            _account, factory = _clients(_cfg(profile="account-m2m"))
            workspace = factory(workspace_host)
        headers = workspace.config.authenticate()

    assert headers["Authorization"] == "Bearer workspace-token"
    assert workspace.config._header_factory is not account_header_factory
    assert account_config.host == account_host
    assert account_config.account_id == "acct"
    assert f"{workspace_host}/oidc/.well-known/oauth-authorization-server" in discovered_urls
    assert post.call_args.args[0] == f"{workspace_host}/oidc/v1/token"


@pytest.mark.parametrize("account_auth_type", ["databricks-cli", "pat", "external-browser"])
def test_clients_uses_host_based_cli_auth_for_non_reusable_account_auth(account_auth_type):
    sdk_config = SimpleNamespace(
        host="https://accounts.cloud.databricks.com", auth_type=account_auth_type
    )
    with patch("databricks.sdk.AccountClient"), \
         patch("databricks.sdk.WorkspaceClient") as workspace_client, \
         patch("databricks.sdk.config.Config", return_value=sdk_config):
        _account, factory = _clients(_cfg(profile="account"))
        factory("https://dbc.example.com")
    workspace_client.assert_called_once_with(
        host="https://dbc.example.com", auth_type="databricks-cli"
    )


def test_clients_rejects_workspace_profile_for_another_host():
    configs = {
        "account": SimpleNamespace(auth_type="databricks-cli"),
        "wrong": SimpleNamespace(host="https://other.example.com"),
    }
    with patch("databricks.sdk.AccountClient"), \
         patch("databricks.sdk.WorkspaceClient"), \
         patch("databricks.sdk.config.Config", side_effect=lambda profile: configs[profile]):
        _account, factory = _clients(
            _cfg(profile="account", workspace_profiles=("wrong",))
        )
        with pytest.raises(RuntimeError, match="not 'https://dbc.example.com'"):
            factory("https://dbc.example.com")


def test_plan_distinguishes_greenfield_and_brownfield_grants():
    greenfield_output = []
    brownfield_output = []

    _plan(_cfg(), greenfield_output.append)
    _plan(_cfg(target_catalog="existing_catalog"), brownfield_output.append)

    assert any("CREATE_CATALOG on its metastore" in line for line in greenfield_output)
    assert not any("MANAGE + APPLY_TAG on catalog" in line for line in greenfield_output)
    assert any(
        "USE_CATALOG + USE_SCHEMA + SELECT + MANAGE + APPLY_TAG on catalog existing_catalog" in line
        for line in brownfield_output
    )
    assert not any("CREATE_CATALOG on its metastore" in line for line in brownfield_output)


def test_apply_uses_exact_scoped_grants():
    account, workspace, workspace_factory, factory = _fake()
    output = []
    assert bootstrap(_cfg(), client_factory=factory, emit=output.append) == 0

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
    assert all(
        call.kwargs["securable_type"] != "catalog"
        for call in workspace.grants.update.call_args_list
    )

    assert workspace.api_client.do.call_args_list == [
        call(
            "GET",
            "/api/2.0/serving-endpoints/custom-model",
        ),
        call(
            "PATCH",
            "/api/2.0/permissions/serving-endpoints/abcdef1234567890",
            body={"access_control_list": [{
                "service_principal_name": "client-123", "permission_level": "CAN_QUERY"
            }]},
        ),
    ]
    assert "GRANTED workspace 123: CAN_QUERY on custom-model" in output


def _fmapi_endpoint():
    return {
        "endpoint_type": "FOUNDATION_MODEL_API",
        "config": {"served_entities": [{
            "foundation_model": {"name": "system.ai.databricks-claude-sonnet-4-6"}
        }]},
    }


@pytest.mark.parametrize("execute_source", ["account users", "reused sp"])
def test_fmapi_with_inherited_execute_is_unchanged_without_permissions_patch(
    execute_source,
):
    _account, workspace, _workspace_factory, factory = _fake(existing=True)
    workspace.api_client.do.return_value = _fmapi_endpoint()
    workspace.metastores.get.return_value = MetastoreInfo(
        metastore_id="meta-1", owner="joseph@example.com"
    )
    workspace.catalogs.get.return_value = CatalogInfo(
        owner="_workspace_admins_louis_serverless_123"
    )
    workspace.functions.get.return_value = SimpleNamespace(owner="System user")

    def effective(*, principal, **_kwargs):
        if principal == "account users" and execute_source == "account users":
            raise PermissionDenied(
                "User does not have READ METADATA on Routine or Model"
            )
        if principal == "caller@example.com" and execute_source == "account users":
            # Exact shape observed from Azure: querying the caller succeeds and
            # identifies the inherited account-users assignment and its ancestor.
            return {
                "privilege_assignments": [{
                    "principal": "account users",
                    "privileges": [{
                        "inherited_from_name": "system.ai",
                        "inherited_from_type": "SCHEMA",
                        "privilege": "EXECUTE",
                    }],
                }],
            }
        if principal == "client-123" and execute_source == "reused sp":
            return SimpleNamespace(privilege_assignments=[SimpleNamespace(
                principal="client-123",
                privileges=[SimpleNamespace(privilege=Privilege.EXECUTE)],
            )])
        return SimpleNamespace(privilege_assignments=[])

    workspace.grants.get_effective.side_effect = effective
    dry_output = []
    assert bootstrap(
        _cfg(model_endpoint="databricks-claude-sonnet-4-6", dry_run=True,
             target_catalog="existing_catalog"),
        client_factory=factory,
        emit=dry_output.append,
    ) == 0
    output = []

    bootstrap(_cfg(model_endpoint="databricks-claude-sonnet-4-6",
                   target_catalog="existing_catalog"),
              client_factory=factory, emit=output.append)

    assert any(
        "model access UNCHANGED via UC EXECUTE (inherited)" in line
        for line in dry_output
    )
    function_updates = [item for item in workspace.grants.update.call_args_list
                        if item.kwargs["securable_type"] == "function"]
    assert function_updates == []
    assert any(line.startswith("UNCHANGED workspace 123: EXECUTE on system.ai.")
               for line in output)


def test_fmapi_caller_effective_lookup_failure_is_actionable():
    _account, workspace, _workspace_factory, factory = _fake()
    workspace.api_client.do.return_value = _fmapi_endpoint()
    workspace.metastores.get.return_value = MetastoreInfo(
        metastore_id="meta-1", owner="joseph@example.com"
    )
    workspace.catalogs.get.return_value = CatalogInfo(
        owner="_workspace_admins_louis_serverless_123"
    )
    workspace.functions.get.return_value = SimpleNamespace(owner="System user")

    def effective(*, principal, **_kwargs):
        if principal == "caller@example.com":
            raise PermissionDenied("cannot inspect grants")
        return SimpleNamespace(privilege_assignments=[])

    workspace.grants.get_effective.side_effect = effective

    with pytest.raises(
        RuntimeError,
        match=(r"cannot prove MANAGE on function .*"
               r"Cause: PermissionDenied: cannot inspect grants"),
    ):
        bootstrap(
            _cfg(model_endpoint="databricks-claude-sonnet-4-6", dry_run=True,
                 target_catalog="existing_catalog"),
            client_factory=factory,
            emit=MagicMock(),
        )


def test_fmapi_without_execute_grants_execute_on_function():
    _account, workspace, _workspace_factory, factory = _fake()
    workspace.api_client.do.return_value = _fmapi_endpoint()

    bootstrap(_cfg(model_endpoint="databricks-claude-sonnet-4-6"),
              client_factory=factory, emit=MagicMock())

    function_call = workspace.grants.update.call_args_list[-1].kwargs
    assert function_call["securable_type"] == "function"
    assert function_call["full_name"] == "system.ai.databricks-claude-sonnet-4-6"
    assert function_call["changes"][0].principal == "client-123"
    assert function_call["changes"][0].add == [Privilege.EXECUTE]
    assert workspace.api_client.do.call_count == 1


def test_fmapi_grant_failure_names_endpoint_securable_workspace_and_host():
    _account, workspace, _workspace_factory, factory = _fake()
    workspace.api_client.do.return_value = _fmapi_endpoint()
    def fail_function_grant(*, securable_type, **_kwargs):
        if securable_type == "function":
            raise PermissionDenied("grant denied")

    workspace.grants.update.side_effect = fail_function_grant

    with pytest.raises(RuntimeError, match=(
        r"Foundation Model API endpoint 'databricks-claude-sonnet-4-6'.*"
        r"'system.ai.databricks-claude-sonnet-4-6'.*workspace 123 "
        r"\(https://dbc.example.com\).*metastore admin grant EXECUTE"
    )):
        bootstrap(_cfg(model_endpoint="databricks-claude-sonnet-4-6"),
                  client_factory=factory, emit=MagicMock())


def test_fmapi_rerun_is_unchanged_after_execute_grant():
    _account, workspace, _workspace_factory, factory = _fake(existing=True)
    workspace.api_client.do.return_value = _fmapi_endpoint()
    granted = False

    def effective(*, principal, **_kwargs):
        privileges = (
            [SimpleNamespace(privilege=Privilege.EXECUTE)]
            if principal == "client-123" and granted else []
        )
        return SimpleNamespace(privilege_assignments=[SimpleNamespace(
            privileges=privileges
        )])

    def update(*, securable_type, **_kwargs):
        nonlocal granted
        if securable_type == "function":
            granted = True

    workspace.grants.get_effective.side_effect = effective
    workspace.grants.update.side_effect = update

    bootstrap(_cfg(model_endpoint="databricks-claude-sonnet-4-6"),
              client_factory=factory, emit=MagicMock())
    output = []
    bootstrap(_cfg(model_endpoint="databricks-claude-sonnet-4-6"),
              client_factory=factory, emit=output.append)

    function_updates = [item for item in workspace.grants.update.call_args_list
                        if item.kwargs["securable_type"] == "function"]
    assert len(function_updates) == 1
    assert any(line.startswith("UNCHANGED workspace 123: EXECUTE") for line in output)


@pytest.mark.parametrize(
    ("lookup_result", "lookup_error", "expected"),
    [
        (None, NotFound("endpoint missing"), r"Cause: NotFound: endpoint missing"),
        ({}, None, r"endpoint lookup returned no ID"),
    ],
)
def test_serving_endpoint_lookup_failure_is_actionable(
    lookup_result, lookup_error, expected
):
    _account, workspace, _workspace_factory, factory = _fake()
    if lookup_error:
        workspace.api_client.do.side_effect = lookup_error
    else:
        workspace.api_client.do.return_value = lookup_result

    with pytest.raises(
        RuntimeError,
        match=(
            r"could not resolve serving endpoint 'custom-model' in workspace 123 "
            r"\(https://dbc.example.com\).*" + expected
        ),
    ):
        bootstrap(_cfg(), client_factory=factory, emit=MagicMock())


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


def test_target_catalog_grants_brownfield_privileges():
    _account, workspace, _workspace_factory, factory = _fake()
    workspace.catalogs.get.return_value = SimpleNamespace(owner="someone-else@example.com")
    workspace.grants.get_effective.return_value = SimpleNamespace(
        privilege_assignments=[SimpleNamespace(
            privileges=[SimpleNamespace(privilege=Privilege.MANAGE)]
        )]
    )
    output = []

    assert bootstrap(
        _cfg(target_catalog="existing_catalog"),
        client_factory=factory,
        emit=output.append,
    ) == 0

    workspace.metastores.current.assert_called_once_with()
    workspace.grants.update.assert_called_once()
    catalog_call = workspace.grants.update.call_args.kwargs
    assert catalog_call["securable_type"] == "catalog"
    assert catalog_call["full_name"] == "existing_catalog"
    assert len(catalog_call["changes"]) == 1
    assert catalog_call["changes"][0].principal == "client-123"
    assert catalog_call["changes"][0].add == [
        Privilege.USE_CATALOG,
        Privilege.USE_SCHEMA,
        Privilege.SELECT,
        Privilege.MANAGE,
        Privilege.APPLY_TAG,
    ]
    assert any(
        "USE_CATALOG + USE_SCHEMA + SELECT + MANAGE + APPLY_TAG on catalog existing_catalog" in line
        for line in output
    )
    assert workspace.api_client.do.call_count == 2


def test_target_catalog_preflight_fails_before_sp_or_secret_creation():
    account, workspace, _workspace_factory, factory = _fake()
    workspace.catalogs.get.return_value = SimpleNamespace(owner="someone-else@example.com")
    _not_metastore_owner(workspace)

    with pytest.raises(RuntimeError, match="preflight failed.*catalog owner"):
        bootstrap(
            _cfg(target_catalog="existing_catalog"),
            client_factory=factory,
            emit=MagicMock(),
        )

    account.service_principals.list.assert_called_once_with(filter='displayName eq "deploy"')
    account.service_principals.create.assert_not_called()
    account.service_principal_secrets.create.assert_not_called()
    account.workspace_assignment.update.assert_not_called()


def test_metastore_preflight_failure_happens_before_every_write():
    account, workspace, _workspace_factory, factory = _fake()
    _not_metastore_owner(workspace)
    workspace.grants.get_effective.return_value = SimpleNamespace(
        privilege_assignments=[]
    )

    with pytest.raises(RuntimeError, match=r"cannot grant CREATE CATALOG.*Nothing was changed"):
        bootstrap(_cfg(), client_factory=factory, emit=MagicMock())

    account.service_principals.list.assert_called_once_with(filter='displayName eq "deploy"')
    account.service_principals.create.assert_not_called()
    account.service_principal_secrets.create.assert_not_called()
    account.api_client.do.assert_not_called()
    account.access_control.update_rule_set.assert_not_called()
    account.workspace_assignment.update.assert_not_called()
    workspace.grants.update.assert_not_called()


def test_missing_workspace_login_fails_before_sp_or_secret_creation():
    account, workspace, _workspace_factory, factory = _fake()
    workspace.current_user.me.side_effect = Unauthenticated("Invalid access token")

    with pytest.raises(RuntimeError, match=(
        r"cannot authenticate to workspace 123 .*databricks auth login --host "
        r"https://dbc.example.com"
    )):
        bootstrap(_cfg(), client_factory=factory, emit=MagicMock())

    account.service_principals.list.assert_called_once_with(filter='displayName eq "deploy"')
    account.service_principals.create.assert_not_called()
    account.service_principal_secrets.create.assert_not_called()


def test_workspace_client_construction_auth_failure_is_actionable_and_exits_two(capsys):
    account = MagicMock()
    account.workspaces.get.return_value = SimpleNamespace(
        workspace_url="dbc.example.com"
    )
    workspace_factory = MagicMock(side_effect=ValueError(
        "default auth: cannot configure default credentials; "
        "Config: host=https://dbc.example.com, auth_type=databricks-cli"
    ))

    with patch(
        "scripts.bootstrap_sp._clients", return_value=(account, workspace_factory)
    ):
        result = main([
            "--account-id", "acct",
            "--workspace-id", "123",
            "--sp-name", "deploy",
            "--yes",
        ])

    assert result == 2
    assert (
        "cannot authenticate to workspace 123 (https://dbc.example.com). Run: "
        "databricks auth login --host https://dbc.example.com"
    ) in capsys.readouterr().err
    account.service_principals.list.assert_called_once_with(filter='displayName eq "deploy"')
    account.service_principals.create.assert_not_called()
    account.service_principal_secrets.create.assert_not_called()


def test_dry_run_missing_workspace_credentials_is_not_reported_as_offline(capsys):
    account = MagicMock()
    account.service_principals.list.return_value = []
    account.workspaces.get.return_value = SimpleNamespace(
        workspace_url="dbc.example.com"
    )
    workspace_factory = MagicMock(side_effect=ValueError(
        "default auth: cannot configure default credentials; "
        "Config: host=https://dbc.example.com, auth_type=databricks-cli"
    ))

    with patch(
        "scripts.bootstrap_sp._clients", return_value=(account, workspace_factory)
    ):
        result = main([
            "--account-id", "acct", "--workspace-id", "123", "--dry-run",
        ])

    captured = capsys.readouterr()
    assert result == 2
    assert "cannot authenticate to workspace 123" in captured.err
    assert "DRY RUN OFFLINE" not in captured.out


def test_account_workspace_lookup_failure_is_not_reported_as_workspace_auth():
    account = MagicMock()
    account.workspaces.get.side_effect = RuntimeError("account lookup failed")

    with pytest.raises(RuntimeError, match="account lookup failed"):
        bootstrap(
            _cfg(),
            client_factory=lambda _cfg: (account, MagicMock()),
            emit=MagicMock(),
        )


def test_workspace_auth_available_allows_preflight_to_proceed():
    account, workspace, _workspace_factory, factory = _fake()
    bootstrap(
        _cfg(target_catalog="existing_catalog"),
        client_factory=factory,
        emit=MagicMock(),
    )
    account.service_principals.list.assert_called_once()
    assert workspace.current_user.me.call_count >= 1


def test_preflight_not_found_is_distinct_from_authority_failure():
    _account, workspace, _workspace_factory, _factory = _fake()
    workspace.catalogs.get.side_effect = NotFound(
        "Catalog existing_catalog does not exist.",
        error_code="CATALOG_DOES_NOT_EXIST",
    )
    with pytest.raises(RuntimeError, match=r"catalog 'existing_catalog' was not found.*Cause"):
        _preflight(workspace)


def test_preflight_authority_error_includes_cause():
    _account, workspace, _workspace_factory, _factory = _fake()
    workspace.catalogs.get.side_effect = PermissionDenied("grant denied")
    with pytest.raises(RuntimeError, match=r"lacks grant authority.*Cause: PermissionDenied: grant denied"):
        _preflight(workspace)
    workspace.catalogs.get.assert_called_once_with("existing_catalog")


def test_effective_grants_permission_denied_is_authority_failure():
    _account, workspace, _workspace_factory, _factory = _fake()
    workspace.catalogs.get.return_value = SimpleNamespace(owner="someone-else@example.com")
    _not_metastore_owner(workspace)
    workspace.grants.get_effective.side_effect = PermissionDenied("cannot inspect grants")

    with pytest.raises(
        RuntimeError,
        match=r"lacks MANAGE.*Cause: PermissionDenied: cannot inspect grants",
    ):
        _preflight(workspace)


@pytest.mark.parametrize(
    ("status", "body", "classifier", "expected_type"),
    [
        (401, b'{"error_code":"401","message":"Invalid access token"}',
         _is_auth_error, Unauthenticated),
        (404, b'{"error_code":"CATALOG_DOES_NOT_EXIST",'
              b'"message":"Catalog c does not exist."}', _is_not_found, NotFound),
    ],
)
def test_sdk_error_parser_classes_are_recognized(status, body, classifier, expected_type):
    response = requests.Response()
    response.status_code = status
    response._content = body
    response.headers["Content-Type"] = "application/json"
    response.url = "https://dbc.example.com/api/2.1/test"
    response.request = requests.Request("GET", response.url).prepare()

    error = _Parser().get_api_error(response)

    assert isinstance(error, expected_type)
    assert classifier(error)


def test_target_catalog_grant_fails_loudly_when_caller_lacks_authority():
    _account, workspace, _workspace_factory, factory = _fake()
    workspace.grants.update.side_effect = PermissionError("denied")

    with pytest.raises(RuntimeError, match="catalog owner.*SELECT, MANAGE, and APPLY TAG"):
        bootstrap(
            _cfg(target_catalog="existing_catalog"),
            client_factory=factory,
            emit=MagicMock(),
        )

    workspace.api_client.do.assert_called_once_with(
        "GET", "/api/2.0/serving-endpoints/custom-model"
    )


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


def _sp_catalog_effective(sp_privileges, caller_privileges=()):
    """get_effective for the SP (client-123) and the caller, by principal."""
    def effective(*, principal, **_kwargs):
        held = sp_privileges if principal == "client-123" else caller_privileges
        return SimpleNamespace(privilege_assignments=[SimpleNamespace(
            privileges=[SimpleNamespace(privilege=p) for p in held]
        )])
    return effective


def _no_catalog_authority(workspace):
    workspace.catalogs.get.return_value = SimpleNamespace(owner="someone-else@example.com")
    _not_metastore_owner(workspace)


def test_existing_sp_with_every_catalog_privilege_is_unchanged_without_authority():
    account, workspace, _workspace_factory, factory = _fake(
        existing=True, roles=("account_admin",)
    )
    _no_catalog_authority(workspace)
    workspace.catalogs.get.side_effect = PermissionDenied("no access")
    workspace.grants.get_effective.side_effect = _sp_catalog_effective([
        Privilege.USE_CATALOG, Privilege.USE_SCHEMA, Privilege.SELECT,
        Privilege.MANAGE, Privilege.APPLY_TAG,
    ])
    output = []

    assert bootstrap(
        _cfg(target_catalog="existing_catalog"), client_factory=factory, emit=output.append
    ) == 0

    workspace.grants.update.assert_not_called()
    workspace.catalogs.get.assert_not_called()
    assert any("catalog existing_catalog privileges UNCHANGED" in line for line in output)
    assert any(
        line.startswith("UNCHANGED workspace 123: USE_CATALOG + USE_SCHEMA + SELECT + "
                        "MANAGE + APPLY_TAG on catalog existing_catalog")
        for line in output
    )


def test_existing_sp_missing_select_is_granted_only_select():
    _account, workspace, _workspace_factory, factory = _fake(
        existing=True, roles=("account_admin",)
    )
    workspace.grants.get_effective.side_effect = _sp_catalog_effective([
        Privilege.USE_CATALOG, Privilege.USE_SCHEMA, Privilege.MANAGE, Privilege.APPLY_TAG,
    ])
    output = []

    assert bootstrap(
        _cfg(target_catalog="existing_catalog"), client_factory=factory, emit=output.append
    ) == 0

    workspace.grants.update.assert_called_once()
    change = workspace.grants.update.call_args.kwargs["changes"][0]
    assert change.principal == "client-123"
    assert change.add == [Privilege.SELECT]
    assert any("GRANTED workspace 123: SELECT on catalog existing_catalog" in line
               for line in output)


def test_existing_sp_missing_a_privilege_needs_caller_authority_before_any_write():
    account, workspace, _workspace_factory, factory = _fake(
        existing=True, roles=("account_admin",)
    )
    _no_catalog_authority(workspace)
    workspace.grants.get_effective.side_effect = _sp_catalog_effective([
        Privilege.USE_CATALOG, Privilege.USE_SCHEMA, Privilege.MANAGE, Privilege.APPLY_TAG,
    ])

    with pytest.raises(RuntimeError, match="preflight failed.*cannot grant SELECT on catalog"):
        bootstrap(
            _cfg(target_catalog="existing_catalog"), client_factory=factory, emit=MagicMock()
        )

    workspace.grants.update.assert_not_called()
    account.workspace_assignment.update.assert_not_called()
    account.service_principal_secrets.create.assert_not_called()
    account.access_control.update_rule_set.assert_not_called()


def test_unreadable_sp_catalog_grants_fail_closed_to_a_full_grant():
    _account, workspace, _workspace_factory, factory = _fake(
        existing=True, roles=("account_admin",)
    )

    def effective(*, principal, **_kwargs):
        if principal == "client-123":
            raise PermissionDenied("cannot inspect another principal")
        return SimpleNamespace(privilege_assignments=[])

    workspace.grants.get_effective.side_effect = effective

    assert bootstrap(
        _cfg(target_catalog="existing_catalog"), client_factory=factory, emit=MagicMock()
    ) == 0

    assert workspace.grants.update.call_args.kwargs["changes"][0].add == [
        Privilege.USE_CATALOG, Privilege.USE_SCHEMA, Privilege.SELECT,
        Privilege.MANAGE, Privilege.APPLY_TAG,
    ]


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
