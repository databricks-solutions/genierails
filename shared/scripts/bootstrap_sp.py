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


@dataclasses.dataclass(frozen=True)
class Config:
    account_id: str
    workspace_ids: tuple[int, ...]
    sp_name: str
    profile: str = "DEFAULT"
    dry_run: bool = False
    yes: bool = False
    rotate_secret: bool = False
    model_endpoint: str = MODEL_ENDPOINT
    target_catalog: str | None = None


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
    p.add_argument("--dry-run", action="store_true", help="print the plan without API calls")
    p.add_argument("--yes", action="store_true", help="apply without an interactive confirmation")
    p.add_argument("--rotate-secret", action="store_true",
                   help="mint a new secret even when reusing an existing SP")
    p.add_argument("--model-endpoint", default=os.environ.get("MODEL_ENDPOINT", MODEL_ENDPOINT),
                   help="serving endpoint to grant CAN_QUERY (env: MODEL_ENDPOINT)")
    p.add_argument(
        "--target-catalog",
        help="existing catalog to grant USE_CATALOG, USE_SCHEMA, MANAGE, and APPLY_TAG",
    )
    return p


def _plan(cfg: Config, emit: Callable[[str], None]) -> None:
    emit("GenieRails service-principal bootstrap plan")
    emit(f"  account: {cfg.account_id}")
    emit(f"  service principal: {cfg.sp_name!r} (create or reuse by exact display name)")
    emit("  grant: Account Admin (account_admin role on the service principal)")
    emit("  grant: account tag-policy creator and manager roles")
    for workspace_id in cfg.workspace_ids:
        emit(f"  workspace {workspace_id}: grant ADMIN")
        if cfg.target_catalog:
            emit(
                f"  workspace {workspace_id}: grant USE_CATALOG + USE_SCHEMA + "
                f"MANAGE + APPLY_TAG on catalog {cfg.target_catalog}"
            )
        else:
            emit(f"  workspace {workspace_id}: grant CREATE_CATALOG on its metastore")
        emit(f"  workspace {workspace_id}: grant CAN_QUERY on {cfg.model_endpoint}")
    if cfg.rotate_secret:
        emit("  secret: mint/rotate OAuth M2M secret")
    else:
        emit("  secret: mint for a new SP or when the reused SP has no secret")


def _clients(cfg: Config) -> tuple[Any, Callable[[str], Any]]:
    from databricks.sdk import AccountClient, WorkspaceClient

    account = AccountClient(account_id=cfg.account_id, profile=cfg.profile)
    return account, lambda host: WorkspaceClient(host=host, profile=cfg.profile)


def _value(obj: Any, name: str) -> Any:
    return obj.get(name) if isinstance(obj, dict) else getattr(obj, name)


def _role_values(sp: Any) -> set[str]:
    roles = sp.get("roles", []) if isinstance(sp, dict) else getattr(sp, "roles", None) or []
    return {str(_value(role, "value")) for role in roles}


def _workspace_client(account: Any, workspace_client: Callable[[str], Any], workspace_id: int) -> Any:
    workspace = account.workspaces.get(workspace_id=workspace_id)
    host = (
        workspace.get("workspace_url")
        if isinstance(workspace, dict)
        else getattr(workspace, "workspace_url", None)
    )
    if not host:
        deployment_name = _value(workspace, "deployment_name")
        cloud = str(_value(workspace, "cloud") or "").lower()
        if deployment_name and "azure" in cloud:
            host = f"{deployment_name}.azuredatabricks.net"
        elif deployment_name and "aws" in cloud:
            host = f"{deployment_name}.cloud.databricks.com"
    if not host:
        raise RuntimeError(
            f"workspace {workspace_id} did not return a usable workspace URL"
        )
    host = str(host)
    if not host.startswith("http"):
        host = "https://" + host
    return workspace_client(host), host


def _preflight_target_catalog(
    cfg: Config,
    account: Any,
    workspace_client: Callable[[str], Any],
) -> None:
    if not cfg.target_catalog:
        return
    for workspace_id in cfg.workspace_ids:
        error_message = (
            f"preflight failed for catalog {cfg.target_catalog!r} in workspace "
            f"{workspace_id}: the catalog is unavailable or the bootstrap caller lacks "
            "grant authority. Have the catalog owner run bootstrap or grant the deployment "
            "service principal USE CATALOG, USE SCHEMA, MANAGE, and APPLY TAG."
        )
        try:
            workspace, _host = _workspace_client(account, workspace_client, workspace_id)
            catalog = workspace.catalogs.get(cfg.target_catalog)
            caller = workspace.current_user.me()
            caller_name = str(_value(caller, "user_name"))
            caller_principals = {caller_name, str(_value(caller, "display_name"))}
            for group in _value(caller, "groups") or []:
                caller_principals.update(
                    str(value) for value in (
                        _value(group, "display"),
                        _value(group, "value"),
                    ) if value
                )
            caller_principals = {principal.casefold() for principal in caller_principals}
            metastore = workspace.metastores.current()
            owns_scope = (
                str(_value(catalog, "owner")).casefold() in caller_principals
                or str(_value(metastore, "owner")).casefold() in caller_principals
            )
        except Exception as exc:
            raise RuntimeError(error_message) from exc

        if owns_scope:
            continue

        try:
            effective = workspace.grants.get_effective(
                securable_type="catalog",
                full_name=cfg.target_catalog,
                principal=caller_name,
            )
            can_manage = any(
                str(getattr(_value(privilege, "privilege"), "value",
                            _value(privilege, "privilege"))) == "MANAGE"
                for assignment in effective.privilege_assignments or []
                for privilege in assignment.privileges or []
            )
        except Exception:
            can_manage = False

        if not can_manage:
            raise RuntimeError(error_message)


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
    client_factory: Callable[[Config], tuple[Any, Callable[[str], Any]]] = _clients,
    emit: Callable[[str], None] = print,
    ask: Callable[[str], str] = input,
) -> int:
    _plan(cfg, emit)
    if cfg.dry_run:
        emit("DRY RUN: no API calls were made.")
        return 0
    if not cfg.yes and ask("Apply these admin grants? Type 'yes' to continue: ").strip().lower() != "yes":
        emit("Aborted; no changes were made.")
        return 1

    account, workspace_client = client_factory(cfg)
    _preflight_target_catalog(cfg, account, workspace_client)
    escaped_name = cfg.sp_name.replace('"', '\\"')
    existing = list(account.service_principals.list(filter=f'displayName eq "{escaped_name}"'))
    if len(existing) > 1:
        raise RuntimeError(f"multiple service principals have display name {cfg.sp_name!r}")
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
    for workspace_id in cfg.workspace_ids:
        from databricks.sdk.service.iam import WorkspacePermission
        from databricks.sdk.service.catalog import PermissionsChange, Privilege

        account.workspace_assignment.update(
            workspace_id=workspace_id,
            principal_id=int(sp_id),
            permissions=[WorkspacePermission.ADMIN],
        )
        emit(f"GRANTED workspace {workspace_id}: ADMIN")
        w, host = _workspace_client(account, workspace_client, workspace_id)
        if cfg.target_catalog:
            try:
                w.grants.update(
                    securable_type="catalog",
                    full_name=cfg.target_catalog,
                    changes=[PermissionsChange(
                        principal=client_id,
                        add=[
                            Privilege.USE_CATALOG,
                            Privilege.USE_SCHEMA,
                            Privilege.MANAGE,
                            Privilege.APPLY_TAG,
                        ],
                    )],
                )
            except Exception as exc:
                raise RuntimeError(
                    f"could not grant USE_CATALOG + USE_SCHEMA + MANAGE + APPLY_TAG on "
                    f"catalog {cfg.target_catalog!r} "
                    f"in workspace {workspace_id}. The bootstrap caller lacks authority or "
                    "the catalog is unavailable; have the catalog owner grant the deployment "
                    f"service principal {client_id!r} USE CATALOG, USE SCHEMA, MANAGE, and "
                    "APPLY TAG."
                ) from exc
            emit(
                f"GRANTED workspace {workspace_id}: USE_CATALOG + USE_SCHEMA + MANAGE + "
                f"APPLY_TAG on catalog {cfg.target_catalog}"
            )
        else:
            metastore_id = str(_value(w.metastores.current(), "metastore_id"))
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
        w.api_client.do(
            "PATCH",
            f"/api/2.0/permissions/serving-endpoints/{cfg.model_endpoint}",
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
        dry_run=args.dry_run,
        yes=args.yes,
        rotate_secret=args.rotate_secret,
        model_endpoint=args.model_endpoint,
        target_catalog=args.target_catalog,
    )


def main(argv: list[str] | None = None) -> int:
    cfg = _config_from_args(parser().parse_args(argv))
    try:
        return bootstrap(cfg)
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
