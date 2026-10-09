#!/usr/bin/env python3
"""Deploy or drop masking functions via Databricks Statement Execution API.

Called by Terraform (terraform_data + local-exec) during apply and destroy.
The SP credentials are read from the layer's auth.auto.tfvars (--auth-file),
never from Terraform state, so a rotated secret is always current.

Usage:
  python3 deploy_masking_functions.py --sql-file masking_functions.sql \
      --warehouse-id <id> --auth-file auth.auto.tfvars --host <workspace-url>
  python3 deploy_masking_functions.py ... --drop
"""

import argparse
import os
import subprocess
import sys

from masking_sql_blocks import extract_function_name, parse_sql_blocks

PRODUCT_NAME = "genierails"
PRODUCT_VERSION = "0.1.0"

REQUIRED_PACKAGES = {"databricks-sdk": "databricks.sdk"}


def _ensure_packages():
    missing = []
    for pip_name, import_name in REQUIRED_PACKAGES.items():
        try:
            __import__(import_name)
        except ImportError:
            missing.append(pip_name)
    if missing:
        print(f"  Installing missing packages: {', '.join(missing)}...")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", *missing],
        )
    try:
        __import__("databricks.sdk.useragent")
    except (ImportError, ModuleNotFoundError):
        print("  Upgrading databricks-sdk (need databricks.sdk.useragent)...")
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--quiet",
                "--upgrade",
                "databricks-sdk",
            ],
        )


_ensure_packages()

from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.service.catalog import (  # noqa: E402
    PermissionsChange,
    Privilege,
    SecurableType,
)
from databricks.sdk.service.sql import (  # noqa: E402
    StatementState,
)


def _get_existing_privileges(
    w: WorkspaceClient, securable_type: SecurableType, full_name: str, principal: str
) -> set[Privilege]:
    try:
        resp = w.grants.get(
            securable_type=securable_type,
            full_name=full_name,
            principal=principal,
        )
    except Exception:
        return set()

    for assignment in resp.privilege_assignments or []:
        if assignment.principal == principal:
            return set(assignment.privileges or [])

    return set()


def _ensure_drop_permissions(
    w: WorkspaceClient, blocks: list[tuple[str, str, str]]
) -> list[tuple[str, str, list[Privilege]]]:
    principal = os.environ.get("DATABRICKS_CLIENT_ID", "").strip()
    if not principal:
        return []

    grants_added: list[tuple[str, str, list[Privilege]]] = []
    catalogs = sorted({catalog for catalog, _, _ in blocks if catalog})
    schemas = sorted(
        {
            (catalog, schema)
            for catalog, schema, _ in blocks
            if catalog and schema
        }
    )

    for catalog in catalogs:
        existing = _get_existing_privileges(w, SecurableType.CATALOG, catalog, principal)
        missing = (
            [Privilege.USE_CATALOG]
            if Privilege.USE_CATALOG not in existing
            else []
        )
        if not missing:
            continue
        print(
            f"  Ensuring SP access on catalog {catalog} "
            f"({', '.join(p.value for p in missing)})..."
        )
        try:
            w.grants.update(
                securable_type=SecurableType.CATALOG,
                full_name=catalog,
                changes=[PermissionsChange(principal=principal, add=missing)],
            )
        except Exception as exc:
            exc_lower = str(exc).lower()
            if "not found" in exc_lower or "does not exist" in exc_lower:
                print(f"  Catalog {catalog} does not exist — skipping.")
                continue
            if (
                "not a valid securable type" in exc_lower
                or "invalid" in exc_lower
                or "securabletype" in exc_lower
            ):
                # Some catalog types (e.g. managed catalogs) reject
                # grants.update() with CATALOG securable_type.  The SP likely
                # already has permission; proceed and let DROP fail naturally.
                print(
                    f"  WARNING: Could not ensure USE_CATALOG on {catalog} "
                    f"({exc}); proceeding without it."
                )
                continue
            raise
        grants_added.append((SecurableType.CATALOG, catalog, missing))

    for catalog, schema in schemas:
        full_name = f"{catalog}.{schema}"
        existing = _get_existing_privileges(w, SecurableType.SCHEMA, full_name, principal)
        missing = (
            [Privilege.USE_SCHEMA]
            if Privilege.USE_SCHEMA not in existing
            else []
        )
        if not missing:
            continue
        print(
            f"  Ensuring SP access on schema {full_name} "
            f"({', '.join(p.value for p in missing)})..."
        )
        try:
            w.grants.update(
                securable_type=SecurableType.SCHEMA,
                full_name=full_name,
                changes=[PermissionsChange(principal=principal, add=missing)],
            )
        except Exception as exc:
            exc_lower = str(exc).lower()
            if "not found" in exc_lower or "does not exist" in exc_lower:
                print(f"  Schema {full_name} does not exist — skipping.")
                continue
            if (
                "not a valid securable type" in exc_lower
                or "invalid" in exc_lower
                or "securabletype" in exc_lower
            ):
                print(
                    f"  WARNING: Could not ensure USE_SCHEMA on {full_name} "
                    f"({exc}); proceeding without it."
                )
                continue
            raise
        grants_added.append((SecurableType.SCHEMA, full_name, missing))

    return grants_added


def _cleanup_drop_permissions(
    w: WorkspaceClient, grants_added: list[tuple[SecurableType, str, list[Privilege]]]
) -> None:
    for securable_type, full_name, privileges in reversed(grants_added):
        try:
            w.grants.update(
                securable_type=securable_type,
                full_name=full_name,
                changes=[
                    PermissionsChange(
                        principal=os.environ["DATABRICKS_CLIENT_ID"],
                        remove=privileges,
                    )
                ],
            )
        except Exception as exc:
            print(
                f"  WARNING: failed to remove temporary {securable_type.value.lower()} "
                f"grants on {full_name}: {exc}"
            )


def deploy(sql_file: str, warehouse_id: str) -> None:
    w = WorkspaceClient(product=PRODUCT_NAME, product_version=PRODUCT_VERSION)

    with open(sql_file) as f:
        sql_text = f.read()

    blocks = parse_sql_blocks(sql_text)
    if not blocks:
        print("  No CREATE statements found in SQL file — nothing to deploy.")
        return

    total = len(blocks)
    print(f"  Deploying {total} function(s) via Statement Execution API...")

    failed = 0
    max_retries = 3
    for i, (catalog, schema, stmt) in enumerate(blocks, 1):
        func_name = extract_function_name(stmt)
        target = f"{catalog}.{schema}" if catalog and schema else "<default>"
        print(f"  [{i}/{total}] {target}.{func_name} ...", end=" ", flush=True)

        succeeded = False
        for attempt in range(1, max_retries + 1):
            try:
                resp = w.statement_execution.execute_statement(
                    warehouse_id=warehouse_id,
                    statement=stmt,
                    catalog=catalog,
                    schema=schema,
                    wait_timeout="30s",
                )
            except Exception as e:
                if attempt < max_retries:
                    import time as _t
                    print(f"RETRY ({e}) ...", end=" ", flush=True)
                    _t.sleep(5 * attempt)
                    continue
                print(f"ERROR: {e}")
                break

            state = resp.status.state
            if state == StatementState.SUCCEEDED:
                print("OK")
                succeeded = True
                break
            else:
                error_msg = ""
                if resp.status.error:
                    error_msg = resp.status.error.message or str(resp.status.error)
                # Retry on transient service errors, not on SQL/schema errors
                is_transient = any(k in error_msg.lower() for k in
                                   ["service", "timeout", "throttl", "temporarily", "unavailable"])
                if is_transient and attempt < max_retries:
                    import time as _t
                    print(f"RETRY ({error_msg[:60]}) ...", end=" ", flush=True)
                    _t.sleep(5 * attempt)
                    continue
                print(f"FAILED ({state.value}): {error_msg}")
                break

        if not succeeded:
            failed += 1

    print()
    if failed:
        print(f"  {failed}/{total} statement(s) failed.")
        sys.exit(1)
    else:
        print(f"  All {total} function(s) deployed successfully.")


def drop(sql_file: str, warehouse_id: str) -> None:
    w = WorkspaceClient(product=PRODUCT_NAME, product_version=PRODUCT_VERSION)

    with open(sql_file) as f:
        sql_text = f.read()

    blocks = parse_sql_blocks(sql_text)
    if not blocks:
        print("  No functions found in SQL file — nothing to drop.")
        return

    grants_added = _ensure_drop_permissions(w, blocks)
    total = len(blocks)
    print(f"  Dropping {total} function(s) via Statement Execution API...")

    failed = 0
    try:
        for i, (catalog, schema, stmt) in enumerate(blocks, 1):
            func_name = extract_function_name(stmt)
            fqn = (
                f"{catalog}.{schema}.{func_name}"
                if catalog and schema
                else func_name
            )
            target = (
                f"{catalog}.{schema}" if catalog and schema else "<default>"
            )
            print(f"  [{i}/{total}] DROP {target}.{func_name} ...", end=" ", flush=True)

            drop_stmt = f"DROP FUNCTION IF EXISTS {fqn}"
            try:
                resp = w.statement_execution.execute_statement(
                    warehouse_id=warehouse_id,
                    statement=drop_stmt,
                    catalog=catalog,
                    schema=schema,
                    wait_timeout="30s",
                )
            except Exception as e:
                err_str = str(e).lower()
                if "not found" in err_str or "does not exist" in err_str:
                    print("SKIP (not found)")
                    continue
                print(f"ERROR: {e}")
                failed += 1
                continue

            state = resp.status.state
            if state == StatementState.SUCCEEDED:
                print("OK")
            else:
                error_msg = ""
                if resp.status.error:
                    error_msg = resp.status.error.message or str(resp.status.error)
                err_lower = error_msg.lower()
                if "not found" in err_lower or "does not exist" in err_lower:
                    print("SKIP (not found)")
                    continue
                print(f"FAILED ({state.value}): {error_msg}")
                failed += 1
    finally:
        _cleanup_drop_permissions(w, grants_added)

    print()
    if failed:
        print(f"  {failed}/{total} drop(s) failed.")
        sys.exit(1)
    else:
        print(f"  All {total} function(s) dropped successfully.")


AUTH_KEYS = (
    "databricks_workspace_host",
    "databricks_client_id",
    "databricks_client_secret",
)


def load_credentials(auth_file: str, host: str) -> None:
    """Export the current SP credentials from auth_file, or exit.

    The destroy-time provisioner can only read Terraform state, so state must
    not be the source of credentials: a rotated secret would be stale there.
    Any problem with auth_file stops the run before a statement executes.
    """
    def fail(problem: str) -> None:
        sys.exit(
            f"ERROR: {problem}\n"
            f"  Masking functions need the current SP credentials from {auth_file}\n"
            f"  ({', '.join(AUTH_KEYS)}). Fix that file and re-run the make target."
        )

    if not os.path.isfile(auth_file):
        fail(f"credentials file not found: {auth_file}")
    try:
        import hcl2

        with open(auth_file) as f:
            auth = hcl2.load(f)
    except Exception as exc:
        fail(f"could not parse {auth_file} ({type(exc).__name__})")
    values = {key: str(auth.get(key) or "").strip() for key in AUTH_KEYS}
    for key, value in values.items():
        if not value:
            fail(f"{auth_file} has no value for {key}")
    file_host = values["databricks_workspace_host"].rstrip("/")
    if file_host != host.rstrip("/"):
        fail(
            f"{auth_file} sets databricks_workspace_host = {file_host}, but these "
            f"masking functions belong to {host.rstrip('/')}"
        )
    os.environ["DATABRICKS_HOST"] = file_host
    os.environ["DATABRICKS_CLIENT_ID"] = values["databricks_client_id"]
    os.environ["DATABRICKS_CLIENT_SECRET"] = values["databricks_client_secret"]


def main():
    parser = argparse.ArgumentParser(
        description="Deploy or drop masking functions via "
        "Databricks Statement Execution API"
    )
    parser.add_argument(
        "--sql-file",
        required=True,
        help="Path to masking_functions.sql",
    )
    parser.add_argument(
        "--warehouse-id",
        required=True,
        help="SQL warehouse ID for statement execution",
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help=(
            "Drop functions instead of creating them "
            "(used during terraform destroy)"
        ),
    )
    parser.add_argument(
        "--auth-file",
        required=True,
        help="The layer's auth.auto.tfvars holding the current SP credentials",
    )
    parser.add_argument(
        "--host",
        required=True,
        help="Workspace URL the functions belong to; must match --auth-file",
    )
    args = parser.parse_args()
    load_credentials(args.auth_file, args.host)

    if args.drop:
        drop(args.sql_file, args.warehouse_id)
    else:
        deploy(args.sql_file, args.warehouse_id)


if __name__ == "__main__":
    main()
