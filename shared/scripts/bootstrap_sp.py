#!/usr/bin/env python3
"""Bootstrap the account-admin service principal used to deploy GenieRails."""

from __future__ import annotations

import argparse
import dataclasses
import os
import sys
from collections.abc import Callable
from typing import Any


MODEL_ENDPOINT = "databricks-claude-sonnet-4-6"
# Granted to the deployment SP on a TARGET_CATALOG, in grant order.
CATALOG_PRIVILEGES = ("USE_CATALOG", "USE_SCHEMA", "SELECT", "MANAGE", "APPLY_TAG")


@dataclasses.dataclass(frozen=True)
class Config:
    account_id: str
    workspace_ids: tuple[int, ...]
    sp_name: str
    profile: str = "DEFAULT"
    workspace_profiles: tuple[str, ...] = ()
    dry_run: bool = False
    yes: bool = False
    rotate_secret: bool = False
    model_endpoint: str = MODEL_ENDPOINT
    target_catalogs: tuple[str, ...] = ()
    # Kept for callers constructing Config directly; CLI input is normalized above.
    target_catalog: str | None = None

    def catalog_for(self, workspace_index: int) -> str | None:
        return self.target_catalogs[workspace_index] if self.target_catalogs else self.target_catalog


def _workspace_ids(value: str) -> tuple[int, ...]:
    try:
        ids = tuple(dict.fromkeys(int(item.strip()) for item in value.split(",") if item.strip()))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("workspace IDs must be comma-separated integers") from exc
    if not ids:
        raise argparse.ArgumentTypeError("at least one workspace ID is required")
    return ids


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Create/reuse the GenieRails deployer SP and grant its required access.",
        epilog=("The selected Databricks CLI profile must already be an account admin. "
                "This command cannot elevate the caller."),
    )
    p.add_argument("--account-id", required=True, help="Databricks account ID")
    p.add_argument("--workspace-id", required=True, type=_workspace_ids,
                   help="target workspace ID, or a comma-separated list")
    p.add_argument("--sp-name", default="genierails-deployer", help="SP display name")
    p.add_argument("--profile", default="DEFAULT", help="authorized Databricks CLI profile")
    p.add_argument(
        "--workspace-profile",
        help="workspace CLI profile, or comma-separated profiles matching --workspace-id",
    )
    p.add_argument("--dry-run", action="store_true", help="print the plan without API calls")
    p.add_argument("--yes", action="store_true", help="apply without an interactive confirmation")
    p.add_argument("--rotate-secret", action="store_true",
                   help="mint a new secret even when reusing an existing SP")
    p.add_argument("--model-endpoint", default=os.environ.get("MODEL_ENDPOINT", MODEL_ENDPOINT),
                   help=("serving endpoint to grant query access (CAN_QUERY, or UC EXECUTE "
                         "for Foundation Model API endpoints; env: MODEL_ENDPOINT)"))
    p.add_argument(
        "--target-catalog",
        help=("existing catalog to grant USE_CATALOG, USE_SCHEMA, SELECT, MANAGE, and APPLY_TAG; "
              "supply one value for every workspace or comma-separated values matching "
              "--workspace-id"),
    )
    return p


def _plan(cfg: Config, emit: Callable[[str], None]) -> None:
    emit("GenieRails service-principal bootstrap plan")
    emit(f"  account: {cfg.account_id}")
    emit(f"  service principal: {cfg.sp_name!r} (create or reuse by exact display name)")
    emit("  grant: Account Admin (account_admin role on the service principal)")
    emit("  grant: account tag-policy creator and manager roles")
    for workspace_index, workspace_id in enumerate(cfg.workspace_ids):
        target_catalog = cfg.catalog_for(workspace_index)
        emit(f"  workspace {workspace_id}: grant ADMIN")
        if target_catalog:
            emit(
                f"  workspace {workspace_id}: grant USE_CATALOG + USE_SCHEMA + "
                f"SELECT + MANAGE + APPLY_TAG on catalog {target_catalog}"
            )
        else:
            emit(f"  workspace {workspace_id}: grant CREATE_CATALOG on its metastore")
        emit(
            f"  workspace {workspace_id}: grant query access on {cfg.model_endpoint} "
            "(CAN_QUERY, or UC EXECUTE for Foundation Model API endpoints)"
        )
    if cfg.rotate_secret:
        emit("  secret: mint/rotate OAuth M2M secret")
    else:
        emit("  secret: mint for a new SP or when the reused SP has no secret")


def _clients(cfg: Config) -> tuple[Any, Callable[[str], Any]]:
    from databricks.sdk import AccountClient, WorkspaceClient
    from databricks.sdk.config import Config as SdkConfig

    account = AccountClient(account_id=cfg.account_id, profile=cfg.profile)
    account_config = SdkConfig(profile=cfg.profile)

    def workspace_client(host: str, workspace_index: int = 0) -> Any:
        if cfg.workspace_profiles:
            profile = cfg.workspace_profiles[workspace_index]
            profile_config = SdkConfig(profile=profile)
            if _normalize_host(profile_config.host) != _normalize_host(host):
                raise RuntimeError(
                    f"workspace profile {profile!r} is configured for {profile_config.host!r}, "
                    f"not {host!r}"
                )
            return WorkspaceClient(host=host, profile=profile)
        if account_config.auth_type == "oauth-m2m":
            return WorkspaceClient(
                host=host,
                client_id=account_config.client_id,
                client_secret=account_config.client_secret,
            )
        if account_config.auth_type == "azure-client-secret":
            azure_options = {
                name: getattr(account_config, name, None)
                for name in (
                    "azure_client_id",
                    "azure_client_secret",
                    "azure_tenant_id",
                    "azure_environment",
                )
                if getattr(account_config, name, None)
            }
            return WorkspaceClient(host=host, **azure_options)
        # With no profile, the SDK's databricks-cli provider asks the CLI token cache
        # for this workspace host. Other account auth types must not leak an
        # account-scoped token source to a workspace; callers can alternatively pass
        # an explicit WORKSPACE_PROFILE.
        return WorkspaceClient(host=host, auth_type="databricks-cli")

    return account, workspace_client


def _normalize_host(host: str | None) -> str:
    return str(host or "").rstrip("/").casefold()


def _workspace_profiles(value: str | None, workspace_count: int) -> tuple[str, ...]:
    if not value:
        return ()
    profiles = tuple(item.strip() for item in value.split(",") if item.strip())
    if len(profiles) == 1 and workspace_count == 1:
        return profiles
    if len(profiles) != workspace_count:
        raise ValueError(
            f"--workspace-profile supplied {len(profiles)} profile(s) for "
            f"{workspace_count} workspace ID(s); supply exactly one profile per workspace"
        )
    return profiles


def _target_catalogs(value: str | None, workspace_count: int) -> tuple[str, ...]:
    if not value:
        return ()
    raw_catalogs = value.split(",")
    if any(not item.strip() for item in raw_catalogs):
        raise ValueError(
            "--target-catalog contains an empty catalog name; remove the extra comma "
            "or supply one non-empty catalog per workspace"
        )
    catalogs = tuple(item.strip() for item in raw_catalogs)
    if len(catalogs) == 1:
        return catalogs * workspace_count
    if len(catalogs) != workspace_count:
        raise ValueError(
            f"--target-catalog supplied {len(catalogs)} catalog(s) for "
            f"{workspace_count} workspace ID(s); supply one catalog for all workspaces "
            "or exactly one catalog per workspace"
        )
    return catalogs


def _value(obj: Any, name: str) -> Any:
    return obj.get(name) if isinstance(obj, dict) else getattr(obj, name, None)


def _foundation_model_name(endpoint: Any) -> str | None:
    config = _value(endpoint, "config")
    for entity in _value(config, "served_entities") or []:
        model_name = _value(_value(entity, "foundation_model"), "name")
        if model_name:
            return str(model_name)
    return None


def _has_effective_privilege(effective: Any, privilege_name: str) -> bool:
    return any(
        str(getattr(_value(privilege, "privilege"), "value",
                    _value(privilege, "privilege"))).upper() == privilege_name
        for assignment in _value(effective, "privilege_assignments") or []
        for privilege in _value(assignment, "privileges") or []
    )


def _principal_has_effective_privilege(
    effective: Any, principal_name: str, privilege_name: str
) -> bool:
    return any(
        str(_value(assignment, "principal") or "").casefold()
        == principal_name.casefold()
        and any(
            str(getattr(_value(privilege, "privilege"), "value",
                        _value(privilege, "privilege"))).upper() == privilege_name
            for privilege in _value(assignment, "privileges") or []
        )
        for assignment in _value(effective, "privilege_assignments") or []
    )


def _privilege_phrase(privileges: tuple[str, ...]) -> str:
    """'USE CATALOG, SELECT, and APPLY TAG' for error messages."""
    names = [privilege.replace("_", " ") for privilege in privileges]
    if len(names) <= 2:
        return " and ".join(names)
    return ", ".join(names[:-1]) + ", and " + names[-1]


def _role_values(sp: Any) -> set[str]:
    roles = sp.get("roles", []) if isinstance(sp, dict) else getattr(sp, "roles", None) or []
    return {str(_value(role, "value")) for role in roles}


def _workspace_host(account: Any, workspace_id: int) -> str:
    workspace = account.workspaces.get(workspace_id=workspace_id)
    host = (
        workspace.get("workspace_url")
        if isinstance(workspace, dict)
        else getattr(workspace, "workspace_url", None)
    )
    if not host:
        deployment_name = _value(workspace, "deployment_name")
        account_host = _normalize_host(_value(_value(account, "config"), "host"))
        cloud = str(_value(workspace, "cloud") or "").lower()
        azure_suffix = ""
        if (
            "azure" in cloud
            or "azuredatabricks" in account_host
            or account_host.endswith("accounts.databricks.azure.cn")
            or account_host.endswith("accounts.azure.cn")
        ):
            azure_suffix = (
                "azuredatabricks.us" if account_host.endswith("azuredatabricks.us")
                else "databricks.azure.cn" if account_host.endswith("azure.cn")
                else "azuredatabricks.net"
            )
        if deployment_name:
            deployment_name = str(deployment_name)
            if (
                ".azuredatabricks.net" in deployment_name
                or ".azuredatabricks.us" in deployment_name
                or ".databricks.azure.cn" in deployment_name
                or ".cloud.databricks.com" in deployment_name
            ):
                host = deployment_name
            elif azure_suffix:
                host = f"{deployment_name}.{azure_suffix}"
            else:
                host = f"{deployment_name}.cloud.databricks.com"
        elif azure_suffix:
            raise ValueError(
                f"workspace {workspace_id} metadata has no workspace_url or deployment_name; "
                "ensure the account Workspace API returns one of those fields before running bootstrap"
            )
        else:
            host = f"dbc-{workspace_id}.cloud.databricks.com"
    host = str(host)
    if not host.startswith("http"):
        host = "https://" + host
    return host


def _error_details(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"


def _error_code(exc: Exception) -> str:
    return str(getattr(exc, "error_code", "") or "").upper()


def _status_code(exc: Exception) -> int | None:
    value = getattr(exc, "status_code", None)
    if value is None:
        response = getattr(exc, "response", None)
        value = getattr(response, "status_code", None)
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _is_auth_error(exc: Exception) -> bool:
    from databricks.sdk.errors import Unauthenticated

    code = _error_code(exc)
    message = str(exc).casefold()
    return (
        isinstance(exc, Unauthenticated)
        or _status_code(exc) == 401
        or code in {"UNAUTHENTICATED", "UNAUTHORIZED"}
        or "cannot configure default credentials" in message
    )


def _is_not_found(exc: Exception) -> bool:
    from databricks.sdk.errors import NotFound

    return isinstance(exc, NotFound) or _status_code(exc) == 404 or _error_code(exc) in {
        "NOT_FOUND", "RESOURCE_DOES_NOT_EXIST",
    }


def _is_permission_denied(exc: Exception) -> bool:
    from databricks.sdk.errors import PermissionDenied

    return (
        isinstance(exc, PermissionDenied)
        or _status_code(exc) == 403
        or _error_code(exc) in {"PERMISSION_DENIED", "FORBIDDEN"}
    )


def _credentials_absent(exc: Exception) -> bool:
    message = str(exc).casefold()
    return (
        "cannot configure default credentials" in message
        or "profile" in message and ("not found" in message or "does not exist" in message)
        or "no credentials" in message
    )


def _authenticate_workspaces(
    cfg: Config,
    account: Any,
    workspace_client: Callable[..., Any],
) -> dict[int, tuple[Any, str]]:
    resolved = {}
    for index, workspace_id in enumerate(cfg.workspace_ids):
        # Account-side workspace discovery is deliberately outside this try: failures
        # there are not workspace authentication failures.
        host = _workspace_host(account, workspace_id)
        try:
            workspace = workspace_client(host, index) if index else workspace_client(host)
            workspace.current_user.me()
        except Exception as exc:
            if _is_auth_error(exc):
                raise RuntimeError(
                    f"cannot authenticate to workspace {workspace_id} ({host}). Run: "
                    f"databricks auth login --host {host} (or pass WORKSPACE_PROFILE). "
                    f"Cause: {_error_details(exc)}"
                ) from exc
            raise
        resolved[workspace_id] = (workspace, host)
    return resolved


@dataclasses.dataclass(frozen=True)
class WorkspacePreflight:
    workspace: Any
    host: str
    metastore_id: str
    target_catalog: str | None
    model_access: str
    model_resource: str
    model_grant_needed: bool
    # Target-catalog privileges the deployment SP still lacks (all for a new SP).
    catalog_missing: tuple[str, ...] = CATALOG_PRIVILEGES


def _missing_catalog_privileges(
    workspace: Any, catalog: str, existing_sp: Any | None
) -> tuple[str, ...]:
    """Privileges the existing SP lacks on catalog; all of them if unknown."""
    if existing_sp is None:
        return CATALOG_PRIVILEGES
    try:
        effective = workspace.grants.get_effective(
            securable_type="catalog",
            full_name=catalog,
            principal=str(_value(existing_sp, "application_id")),
        )
    except Exception:
        # Fail closed: an unreadable state is never treated as already granted;
        # the caller-authority check below then guards the grant.
        return CATALOG_PRIVILEGES
    return tuple(
        privilege for privilege in CATALOG_PRIVILEGES
        if not _has_effective_privilege(effective, privilege)
    )


def _caller_context(workspace: Any) -> tuple[str, set[str], bool]:
    caller = workspace.current_user.me()
    caller_name = str(_value(caller, "user_name"))
    groups = _value(caller, "groups") or []
    principals = {caller_name}
    for group in groups:
        principals.update(
            str(value) for value in (_value(group, "display"), _value(group, "value"))
            if value
        )
    return (
        caller_name,
        {principal.casefold() for principal in principals},
        any(str(_value(group, "display")).casefold() == "admins" for group in groups),
    )


def _preflight(
    cfg: Config,
    workspaces: dict[int, tuple[Any, str]],
    emit: Callable[[str], None],
    existing_sp: Any | None = None,
) -> dict[int, WorkspacePreflight]:
    results = {}
    for workspace_index, workspace_id in enumerate(cfg.workspace_ids):
        workspace, host = workspaces[workspace_id]
        target_catalog = cfg.catalog_for(workspace_index)
        caller_name, caller_principals, is_workspace_admin = _caller_context(workspace)
        assignment = workspace.metastores.current()
        metastore_id = str(_value(assignment, "metastore_id") or "")
        if not metastore_id:
            raise RuntimeError(
                f"preflight failed in workspace {workspace_id} ({host}): no metastore is "
                "assigned; cannot grant catalog privileges"
            )
        metastore_owner = "<unavailable>"
        try:
            metastore_owner = str(_value(workspace.metastores.get(metastore_id), "owner"))
        except Exception as exc:
            if not _is_permission_denied(exc):
                raise RuntimeError(
                    f"preflight failed in workspace {workspace_id}: could not inspect "
                    f"metastore owner. Cause: {_error_details(exc)}"
                ) from exc

        securable_type = "catalog" if target_catalog else "metastore"
        full_name = target_catalog or metastore_id
        catalog_missing = (
            _missing_catalog_privileges(workspace, target_catalog, existing_sp)
            if target_catalog else CATALOG_PRIVILEGES
        )
        # An SP that already holds every catalog privilege needs no grant, so
        # the caller needs no authority over the catalog either.
        needs_scope_authority = not target_catalog or bool(catalog_missing)
        scope_owner = metastore_owner
        if target_catalog and needs_scope_authority:
            try:
                scope_owner = str(_value(workspace.catalogs.get(target_catalog), "owner"))
            except Exception as exc:
                if _is_not_found(exc):
                    raise RuntimeError(
                        f"preflight failed: catalog {target_catalog!r} was not found in "
                        f"workspace {workspace_id} ({host}). Cause: {_error_details(exc)}"
                    ) from exc
                raise RuntimeError(
                    f"preflight failed for catalog {target_catalog!r} in workspace "
                    f"{workspace_id}: caller lacks grant authority or it cannot be inspected. "
                    f"Cause: {_error_details(exc)}"
                ) from exc

        workspace_admin_owner = bool(
            target_catalog
            and scope_owner.casefold().startswith("_workspace_admins_")
            and scope_owner.casefold().endswith(f"_{workspace_id}")
            and is_workspace_admin
        )
        owns_scope = (
            scope_owner.casefold() in caller_principals
            or metastore_owner.casefold() in caller_principals
            or workspace_admin_owner
        )
        can_manage = False
        if not owns_scope and target_catalog and needs_scope_authority:
            try:
                effective = workspace.grants.get_effective(
                    securable_type=securable_type,
                    full_name=full_name,
                    principal=caller_name,
                )
                can_manage = _has_effective_privilege(effective, "MANAGE")
            except Exception as exc:
                raise RuntimeError(
                    f"preflight failed in workspace {workspace_id}: could not inspect "
                    f"effective grant rights and cannot prove MANAGE on "
                    f"{securable_type} {full_name!r}; caller lacks MANAGE unless this "
                    "check succeeds. "
                    f"Cause: {_error_details(exc)}"
                ) from exc
        if needs_scope_authority and not (owns_scope or can_manage):
            if not target_catalog:
                raise RuntimeError(
                    f"preflight failed in workspace {workspace_id}: caller "
                    f"{caller_name!r} cannot grant CREATE CATALOG on metastore "
                    f"{metastore_id!r}; metastore owner is {metastore_owner!r}. Have the "
                    "metastore owner run bootstrap or grant CREATE CATALOG to the "
                    "deployment service principal. Nothing was changed."
                )
            requested = (
                _privilege_phrase(catalog_missing) if target_catalog else "CREATE CATALOG"
            )
            owner_label = "catalog owner" if target_catalog else "metastore owner"
            raise RuntimeError(
                f"preflight failed in workspace {workspace_id}: caller {caller_name!r} "
                f"cannot grant {requested} on {securable_type} {full_name!r}; "
                f"{owner_label} is {scope_owner!r}, metastore owner is "
                f"{metastore_owner!r}, and the caller lacks effective MANAGE. Have the "
                f"{owner_label} run bootstrap or grant the required privilege to the "
                "deployment service principal. Nothing was changed."
            )

        try:
            endpoint = workspace.api_client.do(
                "GET", f"/api/2.0/serving-endpoints/{cfg.model_endpoint}"
            )
        except Exception as exc:
            raise RuntimeError(
                f"could not resolve serving endpoint {cfg.model_endpoint!r} in workspace "
                f"{workspace_id} ({host}) during preflight. Cause: {_error_details(exc)}"
            ) from exc
        endpoint_id = _value(endpoint, "id")
        foundation_model = _foundation_model_name(endpoint)
        is_foundation_model_api = (
            str(_value(endpoint, "endpoint_type") or "").upper() == "FOUNDATION_MODEL_API"
            or (not endpoint_id and foundation_model is not None)
        )
        if is_foundation_model_api:
            if not foundation_model:
                raise RuntimeError(
                    f"preflight failed: Foundation Model API endpoint {cfg.model_endpoint!r} "
                    f"in workspace {workspace_id} has no backing UC function"
                )
            model_access, model_resource = "UC EXECUTE", foundation_model
            model_grant_needed = True
            inherited_execute = False
            caller_function_effective = None
            try:
                account_users_effective = workspace.grants.get_effective(
                    securable_type="function",
                    full_name=foundation_model,
                    principal="account users",
                )
                inherited_execute = _principal_has_effective_privilege(
                    account_users_effective, "account users", "EXECUTE"
                )
            except Exception:
                # Azure can deny inspecting another principal on system.ai even though
                # the caller's own effective response exposes the inherited assignment.
                pass
            if not inherited_execute:
                try:
                    caller_function_effective = workspace.grants.get_effective(
                        securable_type="function",
                        full_name=foundation_model,
                        principal=caller_name,
                    )
                    inherited_execute = _principal_has_effective_privilege(
                        caller_function_effective, "account users", "EXECUTE"
                    )
                except Exception:
                    # Caller authority below reports a clear failure if no other
                    # read-only check proves that a grant is unnecessary.
                    pass
            if not inherited_execute and existing_sp is not None:
                client_id = str(_value(existing_sp, "application_id"))
                try:
                    sp_effective = workspace.grants.get_effective(
                        securable_type="function",
                        full_name=foundation_model,
                        principal=client_id,
                    )
                    inherited_execute = _has_effective_privilege(sp_effective, "EXECUTE")
                except Exception:
                    pass
            if inherited_execute:
                model_grant_needed = False
            elif metastore_owner.casefold() not in caller_principals:
                function_owner = "<unavailable>"
                try:
                    function_owner = str(
                        _value(workspace.functions.get(foundation_model), "owner")
                    )
                except Exception as exc:
                    if not _is_permission_denied(exc):
                        raise RuntimeError(
                            f"preflight failed: cannot inspect owner of UC function "
                            f"{foundation_model!r} in workspace {workspace_id}. "
                            f"Cause: {_error_details(exc)}"
                        ) from exc
                if caller_function_effective is None:
                    try:
                        caller_function_effective = workspace.grants.get_effective(
                            securable_type="function",
                            full_name=foundation_model,
                            principal=caller_name,
                        )
                    except Exception as exc:
                        raise RuntimeError(
                            f"preflight failed in workspace {workspace_id}: could not "
                            f"inspect effective grant rights and cannot prove MANAGE on "
                            f"function {foundation_model!r}; caller lacks MANAGE unless "
                            f"this check succeeds. Cause: {_error_details(exc)}"
                        ) from exc
                if (
                    function_owner.casefold() not in caller_principals
                    and not _has_effective_privilege(
                        caller_function_effective, "MANAGE"
                    )
                ):
                    raise RuntimeError(
                        f"preflight failed in workspace {workspace_id}: model access uses "
                        f"UC EXECUTE on function {foundation_model!r}, but caller "
                        f"{caller_name!r} is not its owner ({function_owner!r}), is not "
                        f"metastore owner ({metastore_owner!r}), and lacks effective "
                        "MANAGE. Have the function or metastore owner grant EXECUTE to "
                        "the deployment service principal. Nothing was changed."
                    )
        else:
            if not endpoint_id:
                raise RuntimeError(
                    f"could not resolve serving endpoint {cfg.model_endpoint!r} in workspace "
                    f"{workspace_id} ({host}) during preflight: endpoint lookup returned no ID"
                )
            model_access, model_resource = "CAN_QUERY", str(endpoint_id)
            model_grant_needed = True
            if not is_workspace_admin:
                permissions = workspace.api_client.do(
                    "GET", f"/api/2.0/permissions/serving-endpoints/{endpoint_id}"
                )
                can_manage_endpoint = False
                for entry in _value(permissions, "access_control_list") or []:
                    principal = next((
                        _value(entry, name) for name in (
                            "user_name", "group_name", "service_principal_name"
                        ) if _value(entry, name)
                    ), None)
                    levels = [_value(entry, "permission_level")]
                    levels.extend(
                        _value(item, "permission_level")
                        for item in (_value(entry, "all_permissions") or [])
                    )
                    if (
                        str(principal).casefold() in caller_principals
                        and any(str(level).upper() == "CAN_MANAGE" for level in levels)
                    ):
                        can_manage_endpoint = True
                        break
                if not can_manage_endpoint:
                    raise RuntimeError(
                        f"preflight failed in workspace {workspace_id}: model access uses "
                        f"CAN_QUERY on endpoint {cfg.model_endpoint!r}, but caller "
                        f"{caller_name!r} is not a workspace admin and lacks CAN_MANAGE. "
                        "Have an endpoint manager grant CAN_QUERY to the deployment "
                        "service principal. Nothing was changed."
                    )
        resolved_access = (
            f"{model_access} (inherited)"
            if model_access == "UC EXECUTE" and not model_grant_needed
            else model_access
        )
        if target_catalog:
            emit(
                f"PLAN RESOLVED workspace {workspace_id}: catalog {target_catalog} "
                + (
                    f"privileges to grant: {' + '.join(catalog_missing)}"
                    if catalog_missing else "privileges UNCHANGED (already granted)"
                )
            )
        if model_access == "UC EXECUTE" and not model_grant_needed:
            emit(
                f"PLAN RESOLVED workspace {workspace_id}: model access UNCHANGED via "
                f"{resolved_access}"
            )
        else:
            emit(
                f"PLAN RESOLVED workspace {workspace_id}: model access path "
                f"{resolved_access}"
            )
        emit(
            f"PREFLIGHT OK workspace {workspace_id} ({host}): authenticated as "
            f"{caller_name}; grant scope {securable_type} {full_name}; model access "
            f"{resolved_access} on {cfg.model_endpoint}"
        )
        results[workspace_id] = WorkspacePreflight(
            workspace, host, metastore_id, target_catalog, model_access, model_resource,
            model_grant_needed, catalog_missing,
        )
    return results


def _preflight_target_catalog(
    cfg: Config,
    account: Any,
    workspace_client: Callable[..., Any],
) -> None:
    """Compatibility shim for direct callers; the complete preflight is used by bootstrap."""
    workspaces = _authenticate_workspaces(cfg, account, workspace_client)
    _preflight(cfg, workspaces, lambda _line: None)


def _grant_tag_policy_roles(account: Any, cfg: Config, client_id: str) -> bool:
    from databricks.sdk.service.iam import GrantRule, RuleSetUpdateRequest

    name = f"accounts/{cfg.account_id}/ruleSets/default"
    current = account.access_control.get_rule_set(name=name, etag="")
    rules = list(current.grant_rules or [])
    principal = f"servicePrincipals/{client_id}"
    changed = False
    for role in ("roles/tagPolicy.creator", "roles/tagPolicy.manager"):
        rule = next((item for item in rules if item.role == role), None)
        if rule is None:
            rules.append(GrantRule(role=role, principals=[principal]))
            changed = True
        elif principal not in (rule.principals or []):
            rule.principals = [*(rule.principals or []), principal]
            changed = True
    if changed:
        account.access_control.update_rule_set(
            name=name,
            rule_set=RuleSetUpdateRequest(name=name, etag=current.etag, grant_rules=rules),
        )
    return changed


def bootstrap(
    cfg: Config,
    *,
    client_factory: Callable[[Config], tuple[Any, Callable[[str], Any]]] | None = None,
    emit: Callable[[str], None] = print,
    ask: Callable[[str], str] = input,
) -> int:
    _plan(cfg, emit)
    if (not cfg.dry_run and not cfg.yes
            and ask("Apply these admin grants? Type 'yes' to continue: ").strip().lower() != "yes"):
        emit("Aborted; no changes were made.")
        return 1

    try:
        account, workspace_client = (client_factory or _clients)(cfg)
        escaped_name = cfg.sp_name.replace('"', '\\"')
        existing = list(
            account.service_principals.list(filter=f'displayName eq "{escaped_name}"')
        )
    except Exception as exc:
        if cfg.dry_run and _credentials_absent(exc):
            emit(
                "DRY RUN OFFLINE: credentials are absent, so no API calls could be made; "
                "hosts, authentication, endpoint presence/access path, and grant rights "
                "were not verified."
            )
            return 0
        raise
    if len(existing) > 1:
        raise RuntimeError(f"multiple service principals have display name {cfg.sp_name!r}")

    workspaces = _authenticate_workspaces(cfg, account, workspace_client)
    preflight = _preflight(
        cfg, workspaces, emit, existing_sp=existing[0] if existing else None
    )
    if cfg.dry_run:
        emit("DRY RUN: read-only preflight passed; no changes were made.")
        return 0
    created = not existing
    sp = existing[0] if existing else account.service_principals.create(
        display_name=cfg.sp_name, active=True
    )
    sp_id, client_id = str(_value(sp, "id")), str(_value(sp, "application_id"))
    emit(f"SP {'created' if created else 'reused'}: {cfg.sp_name} (client_id={client_id})")

    # Mint before any grant: a failed grant must not strand a new SP without a usable secret.
    existing_secrets = [] if created else list(
        account.service_principal_secrets.list(service_principal_id=sp_id)
    )
    secret = None
    if created or cfg.rotate_secret or not existing_secrets:
        secret_response = account.service_principal_secrets.create(service_principal_id=int(sp_id))
        secret = str(_value(secret_response, "secret"))
        emit("WARNING: STORE THIS NOW. The OAuth client secret below is shown once and cannot be retrieved.")
        emit(f"client_id = {client_id}")
        emit(f"client_secret = {secret}")
    else:
        emit("OAuth secret unchanged (use --rotate-secret to mint a replacement).")

    if "account_admin" not in _role_values(sp):
        account.api_client.do(
            "PATCH",
            f"/api/2.0/accounts/{cfg.account_id}/scim/v2/ServicePrincipals/{sp_id}",
            body={
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": [{
                    "op": "add", "path": "roles", "value": [{"value": "account_admin"}]
                }],
            },
        )
        emit("GRANTED Account Admin")
    else:
        emit("UNCHANGED Account Admin (already granted)")

    changed = _grant_tag_policy_roles(account, cfg, client_id)
    emit(("GRANTED" if changed else "UNCHANGED") +
         " account tag-policy creator and manager roles")

    workspace_summaries: list[tuple[int, str]] = []
    for workspace_index, workspace_id in enumerate(cfg.workspace_ids):
        from databricks.sdk.service.iam import WorkspacePermission
        from databricks.sdk.service.catalog import PermissionsChange, Privilege

        account.workspace_assignment.update(
            workspace_id=workspace_id,
            principal_id=int(sp_id),
            permissions=[WorkspacePermission.ADMIN],
        )
        emit(f"GRANTED workspace {workspace_id}: ADMIN")
        checked = preflight[workspace_id]
        w, host = checked.workspace, checked.host
        target_catalog = checked.target_catalog
        if target_catalog and not checked.catalog_missing:
            emit(
                f"UNCHANGED workspace {workspace_id}: "
                f"{' + '.join(CATALOG_PRIVILEGES)} on catalog {target_catalog} "
                "(already granted)"
            )
        elif target_catalog:
            missing = checked.catalog_missing
            try:
                w.grants.update(
                    securable_type="catalog",
                    full_name=target_catalog,
                    changes=[PermissionsChange(
                        principal=client_id,
                        add=[Privilege[privilege] for privilege in missing],
                    )],
                )
            except Exception as exc:
                raise RuntimeError(
                    f"could not grant {' + '.join(missing)} on "
                    f"catalog {target_catalog!r} "
                    f"in workspace {workspace_id}. The bootstrap caller lacks authority or "
                    "the catalog is unavailable; have the catalog owner grant the deployment "
                    f"service principal {client_id!r} {_privilege_phrase(missing)}."
                ) from exc
            emit(
                f"GRANTED workspace {workspace_id}: {' + '.join(missing)} on catalog "
                f"{target_catalog}"
            )
        else:
            metastore_id = checked.metastore_id
            w.grants.update(
                securable_type="metastore",
                full_name=metastore_id,
                changes=[PermissionsChange(
                    principal=client_id, add=[Privilege.CREATE_CATALOG]
                )],
            )
            emit(
                f"GRANTED workspace {workspace_id}: CREATE_CATALOG on metastore "
                f"{metastore_id}"
            )
        endpoint_id = checked.model_resource if checked.model_access == "CAN_QUERY" else None
        foundation_model = checked.model_resource if checked.model_access == "UC EXECUTE" else None
        if checked.model_access == "UC EXECUTE":
            if not checked.model_grant_needed:
                emit(
                    f"UNCHANGED workspace {workspace_id}: EXECUTE on {foundation_model} "
                    f"(query access for {cfg.model_endpoint})"
                )
                workspace_summaries.append((workspace_id, host))
                continue
            try:
                effective = w.grants.get_effective(
                    securable_type="function",
                    full_name=foundation_model,
                    principal=client_id,
                )
                if _has_effective_privilege(effective, "EXECUTE"):
                    emit(
                        f"UNCHANGED workspace {workspace_id}: EXECUTE on {foundation_model} "
                        f"(query access for {cfg.model_endpoint})"
                    )
                else:
                    w.grants.update(
                        securable_type="function",
                        full_name=foundation_model,
                        changes=[PermissionsChange(
                            principal=client_id,
                            add=[Privilege.EXECUTE],
                        )],
                    )
                    emit(
                        f"GRANTED workspace {workspace_id}: EXECUTE on {foundation_model} "
                        f"(query access for {cfg.model_endpoint})"
                    )
            except Exception as exc:
                raise RuntimeError(
                    f"could not grant query access for Foundation Model API endpoint "
                    f"{cfg.model_endpoint!r} via EXECUTE on {foundation_model!r} in workspace "
                    f"{workspace_id} ({host}). Have a metastore admin grant EXECUTE on "
                    f"{foundation_model!r} to service principal {client_id!r}. "
                    f"Cause: {_error_details(exc)}"
                ) from exc
            workspace_summaries.append((workspace_id, host))
            continue
        if not endpoint_id:
            raise RuntimeError(
                f"could not resolve serving endpoint {cfg.model_endpoint!r} in workspace "
                f"{workspace_id} ({host}): the endpoint lookup returned no ID"
            )
        w.api_client.do(
            "PATCH",
            f"/api/2.0/permissions/serving-endpoints/{endpoint_id}",
            body={"access_control_list": [{
                "service_principal_name": client_id,
                "permission_level": "CAN_QUERY",
            }]},
        )
        emit(f"GRANTED workspace {workspace_id}: CAN_QUERY on {cfg.model_endpoint}")
        workspace_summaries.append((workspace_id, host))

    emit("\nSummary: GenieRails deployer access is configured.")
    for workspace_id, host in workspace_summaries:
        if secret is None:
            shown_secret = "<existing-secret-not-retrievable>"
        else:
            shown_secret = "<newly-minted secret shown above>"
        emit(f"\n# envs/<env>/auth.auto.tfvars — workspace {workspace_id}")
        emit(f'databricks_client_id     = "{client_id}"')
        emit(f'databricks_client_secret = "{shown_secret}"')
        emit(f'databricks_workspace_host = "{host}"')
        emit(f'databricks_workspace_id   = "{workspace_id}"')
    return 0


def _config_from_args(args: argparse.Namespace) -> Config:
    return Config(
        account_id=args.account_id,
        workspace_ids=args.workspace_id,
        sp_name=args.sp_name,
        profile=args.profile,
        workspace_profiles=_workspace_profiles(args.workspace_profile, len(args.workspace_id)),
        dry_run=args.dry_run,
        yes=args.yes,
        rotate_secret=args.rotate_secret,
        model_endpoint=args.model_endpoint,
        target_catalogs=_target_catalogs(args.target_catalog, len(args.workspace_id)),
    )


def main(argv: list[str] | None = None) -> int:
    try:
        cfg = _config_from_args(parser().parse_args(argv))
        return bootstrap(cfg)
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
