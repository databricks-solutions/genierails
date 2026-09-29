#!/usr/bin/env python3
"""Bootstrap the account-admin service principal used to deploy GenieRails."""

from __future__ import annotations

import argparse
import dataclasses
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
    return p


def _plan(cfg: Config, emit: Callable[[str], None]) -> None:
    emit("GenieRails service-principal bootstrap plan")
    emit(f"  account: {cfg.account_id}")
    emit(f"  service principal: {cfg.sp_name!r} (create or reuse by exact display name)")
    emit("  grant: Account Admin (account admins group membership)")
    for workspace_id in cfg.workspace_ids:
        emit(f"  workspace {workspace_id}: grant ADMIN")
        emit(f"  workspace {workspace_id}: grant metastore ALL PRIVILEGES")
        emit(f"  workspace {workspace_id}: grant CAN_QUERY on {MODEL_ENDPOINT}")
    if cfg.rotate_secret:
        emit("  secret: mint/rotate OAuth M2M secret")
    else:
        emit("  secret: mint only when the SP is newly created")


def _clients(cfg: Config) -> tuple[Any, Callable[[str], Any]]:
    from databricks.sdk import AccountClient, WorkspaceClient

    account = AccountClient(account_id=cfg.account_id, profile=cfg.profile)
    return account, lambda host: WorkspaceClient(host=host, profile=cfg.profile)


def _value(obj: Any, name: str) -> Any:
    return obj.get(name) if isinstance(obj, dict) else getattr(obj, name)


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

    # Account admins is the system group that carries the Account Admin role.
    groups = list(account.groups.list(filter='displayName eq "account admins"'))
    if len(groups) != 1:
        raise RuntimeError("could not uniquely resolve the account admins system group")
    group_id = str(_value(groups[0], "id"))
    account.api_client.do(
        "PATCH",
        f"/api/2.0/accounts/{cfg.account_id}/scim/v2/Groups/{group_id}",
        body={
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{"op": "add", "path": "members", "value": [{"value": sp_id}]}],
        },
    )
    emit("GRANTED Account Admin")

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
        workspace = account.workspaces.get(workspace_id=workspace_id)
        host = str(_value(workspace, "workspace_url"))
        if not host.startswith("http"):
            host = "https://" + host
        w = workspace_client(host)
        metastore_id = str(_value(w.metastores.current(), "metastore_id"))
        w.grants.update(
            securable_type="metastore",
            full_name=metastore_id,
            changes=[PermissionsChange(principal=client_id, add=[Privilege.ALL_PRIVILEGES])],
        )
        emit(f"GRANTED workspace {workspace_id}: ALL PRIVILEGES on metastore {metastore_id}")
        w.api_client.do(
            "PATCH",
            f"/api/2.0/permissions/serving-endpoints/{MODEL_ENDPOINT}",
            body={"access_control_list": [{
                "service_principal_name": client_id,
                "permission_level": "CAN_QUERY",
            }]},
        )
        emit(f"GRANTED workspace {workspace_id}: CAN_QUERY on {MODEL_ENDPOINT}")
        workspace_summaries.append((workspace_id, host))

    secret = None
    if created or cfg.rotate_secret:
        secret_response = account.service_principal_secrets.create(service_principal_id=int(sp_id))
        secret = str(_value(secret_response, "secret"))
    else:
        emit("OAuth secret unchanged (use --rotate-secret to mint a replacement).")

    emit("\nSummary: GenieRails deployer access is configured.")
    if secret is not None:
        emit("WARNING: STORE THIS NOW. The OAuth client secret below is shown once and cannot be retrieved.")
    for index, (workspace_id, host) in enumerate(workspace_summaries):
        if secret is None:
            shown_secret = "<existing-secret-not-retrievable>"
        elif index == 0:
            shown_secret = secret
        else:
            shown_secret = "<same newly-minted secret shown above>"
        emit(f"\n# envs/<env>/auth.auto.tfvars — workspace {workspace_id}")
        emit(f'databricks_client_id     = "{client_id}"')
        emit(f'databricks_client_secret = "{shown_secret}"')
        emit(f'databricks_workspace_host = "{host}"')
        emit(f'databricks_workspace_id   = "{workspace_id}"')
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    cfg = Config(
        account_id=args.account_id,
        workspace_ids=args.workspace_id,
        sp_name=args.sp_name,
        profile=args.profile,
        dry_run=args.dry_run,
        yes=args.yes,
        rotate_secret=args.rotate_secret,
    )
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
