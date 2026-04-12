#!/usr/bin/env python3
"""Deploy or drop masking functions via Databricks Statement Execution API.

Called by Terraform (null_resource + local-exec) during apply and destroy.
Auth is read from environment variables set by the provisioner:
  DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET

Usage:
  python3 deploy_masking_functions.py \
      --sql-file masking_functions.sql --warehouse-id <id>
  python3 deploy_masking_functions.py \
      --sql-file masking_functions.sql --warehouse-id <id> --drop
"""

import argparse
import os
import re
import subprocess
import sys

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


def parse_sql_blocks(sql_text: str) -> list:
    """Parse a SQL file into (catalog, schema, statement) tuples.

    Tracks USE CATALOG / USE SCHEMA directives to determine the execution
    context for each CREATE statement.
    """
    catalog, schema = None, None
    blocks = []

    for raw_stmt in re.split(r";\s*(?:--[^\n]*)?\n", sql_text):
        lines = [
            line for line in raw_stmt.split("\n")
            if line.strip() and not line.strip().startswith("--")
        ]
        stmt = "\n".join(lines).strip()
        if not stmt:
            continue

        m = re.match(r"USE\s+CATALOG\s+(\S+)", stmt, re.IGNORECASE)
        if m:
            catalog = m.group(1)
            continue

        m = re.match(r"USE\s+SCHEMA\s+(\S+)", stmt, re.IGNORECASE)
        if m:
            schema = m.group(1)
            continue

        if stmt.upper().startswith("CREATE"):
            blocks.append((catalog, schema, stmt))

    return blocks


def extract_function_name(stmt: str) -> str:
    """Extract function name from a CREATE FUNCTION statement."""
    m = re.search(
        r"FUNCTION\s+(\S+)\s*\(", stmt, re.IGNORECASE
    )
    return m.group(1) if m else "<unknown>"


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
    args = parser.parse_args()

    if args.drop:
        drop(args.sql_file, args.warehouse_id)
    else:
        deploy(args.sql_file, args.warehouse_id)


if __name__ == "__main__":
    main()
