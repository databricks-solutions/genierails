#!/usr/bin/env python3
"""Deploy or drop masking functions via Databricks Statement Execution API.

Called by Terraform (terraform_data + local-exec) during apply and destroy.
The SP credentials are read from the layer's auth.auto.tfvars (--auth-file),
never from Terraform state, so a rotated secret is always current.

Usage:
  python3 deploy_masking_functions.py --sql-file masking_functions.sql \
      --warehouse-id <id> --auth-file auth.auto.tfvars --host <workspace-url>
  python3 deploy_masking_functions.py ... --drop --expect-sql-hash <sql_hash>
  python3 deploy_masking_functions.py ... --drop --expect-sql-hash <sql_hash> --base-dir <env-dir>

terraform_data.masking_functions_drop records --sql-file and --auth-file
relative to the env dir, so its destroy-time drop passes --base-dir (the
GENIERAILS_MASKING_BASE_DIR terraform_layer.sh exports) and runs from any
checkout. Absolute paths are used as given while they exist; one recorded
under a checkout that is gone is looked up at the same place under this
shared/ (--relocate-from names the old one) or the env dir.
--expect-sql-hash is the normalized SQL hash the last apply recorded: the
drop hashes the SQL it resolved and refuses, before it connects, on a
mismatch (another env's SQL, or a file changed since the apply). A drop
without one (all state from before it was recorded, whether its paths still
exist or not) refuses until an apply from this checkout re-keys it.
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

from masking_sql_blocks import extract_function_name, parse_sql_blocks

PRODUCT_NAME = "genierails"
PRODUCT_VERSION = "0.1.0"
_AUTH_FILE: Path | None = None

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
    from scripts.auth_configured import workspace_client
    if _AUTH_FILE is None:
        raise RuntimeError("masking auth file was not loaded")
    w = workspace_client(_AUTH_FILE, product=PRODUCT_NAME, product_version=PRODUCT_VERSION)

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
    from scripts.auth_configured import workspace_client
    if _AUTH_FILE is None:
        raise RuntimeError("masking auth file was not loaded")
    w = workspace_client(_AUTH_FILE, product=PRODUCT_NAME, product_version=PRODUCT_VERSION)

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

    global _AUTH_FILE
    path = Path(auth_file)
    from scripts.auth_configured import AuthStatus, auth_status
    status = auth_status(path)
    if status is AuthStatus.UNREADABLE:
        fail(f"{auth_file} can't be read")
    if status is AuthStatus.MALFORMED:
        fail(f"{auth_file} is not valid HCL")
    if status is AuthStatus.INCOMPLETE:
        fail(f"fill in {auth_file} (workspace host, client ID, and client secret)")
    from scripts.hcl2_guard import import_hcl2

    hcl2 = import_hcl2()

    try:
        with open(auth_file) as f:
            auth = hcl2.load(f)
    except Exception:
        auth = None
    if auth is None:
        # Reported outside the handler with no parser text, which can echo a
        # malformed secret (also via __context__).
        fail(f"{auth_file} is not valid HCL")
    values = {}
    for key in AUTH_KEYS:
        raw = auth.get(key, "")
        if not isinstance(raw, str):
            fail(f"fill in {auth_file} (workspace host, client ID, and client secret)")
        values[key] = raw.strip()
    file_host = values["databricks_workspace_host"].rstrip("/")
    if file_host != host.rstrip("/"):
        fail(
            f"{auth_file} sets databricks_workspace_host = {file_host}, but these "
            f"masking functions belong to {host.rstrip('/')}"
        )
    os.environ["DATABRICKS_HOST"] = file_host
    os.environ["DATABRICKS_CLIENT_ID"] = values["databricks_client_id"]
    os.environ["DATABRICKS_CLIENT_SECRET"] = values["databricks_client_secret"]
    _AUTH_FILE = path


def _refuse(problem: str) -> None:
    print(f"ERROR: {problem}; nothing was dropped.", file=sys.stderr)
    sys.exit(1)


def _under(path: str, base: str, resolve=os.path.realpath) -> bool:
    """Whether path is under base, symlinks resolved (abspath for a path that is gone)."""
    path, base = resolve(path), resolve(base)
    return os.path.commonpath([path, base]) == base


def _relocated(path: str, args):
    """Where a recorded absolute path that is gone sits in this checkout, if it can be told.

    Older states recorded the files under the checkout that applied them. The
    same file is at the same place under this script's shared/ (when it was
    under the old one, --relocate-from) or, by its last two parts
    (data_access/<file>), under the env dir when the dir above those is named
    like it (the env). A place counts only if, symlinks resolved, it is still
    under the dir it was found in and under --base-dir when one is given.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = []
    if args.relocate_from and _under(path, args.relocate_from, os.path.abspath):
        candidates.append((os.path.join(here, os.path.relpath(path, args.relocate_from)), here))
    parts = os.path.normpath(path).split(os.sep)
    if args.base_dir and len(parts) > 3 and parts[-3] == os.path.basename(os.path.normpath(args.base_dir)):
        candidates.append((os.path.join(args.base_dir, *parts[-2:]), args.base_dir))
    for candidate, anchor in candidates:
        if (os.path.isfile(candidate) and _under(candidate, anchor)
                and (not args.base_dir or _under(candidate, args.base_dir))):
            return candidate
    return None


def _refuse_unidentified(sql_file: str) -> None:
    _refuse(
        "masking_functions_drop records no sql_hash (state from an earlier version), so the "
        f"drop can't tell whether {sql_file} is the SQL it deployed: a reused or replaced "
        "checkout may hold another env's at that path. Run make apply once from this checkout "
        "(it re-keys masking_functions_drop to relative paths and records sql_hash, dropping "
        "nothing; see docs/deterministic-policies.md), then make destroy"
    )


def resolve_paths(args) -> None:
    """Resolve --sql-file/--auth-file for this checkout, or exit.

    A drop needs the --expect-sql-hash the last apply recorded, which is what
    tells this env's SQL from another's: all legacy state, which has none,
    refuses, whether its absolute paths still exist or not. Relative paths
    resolve against --base-dir (given but empty refuses). An absolute path
    that is gone is relocated (_relocated) or refuses.
    """
    base_dir = getattr(args, "base_dir", None)
    if getattr(args, "drop", False) and not getattr(args, "expect_sql_hash", ""):
        _refuse_unidentified(args.sql_file)
    for key in ("sql_file", "auth_file"):
        path = getattr(args, key)
        if not os.path.isabs(path):
            if base_dir is None:
                continue
            if not base_dir:
                _refuse(
                    f"masking_functions_drop records {path} relative to the env dir, but "
                    "GENIERAILS_MASKING_BASE_DIR is unset, so the drop can't find it. Run the "
                    "destroy through terraform_layer.sh (make destroy), which sets it"
                )
            setattr(args, key, os.path.join(base_dir, path))
        elif not os.path.exists(path) and getattr(args, "drop", False):
            moved = _relocated(path, args)
            if moved is None:
                _refuse(
                    f"masking_functions_drop records {path}, which no longer exists, and "
                    "this checkout has no file at the same place"
                    + (f" under {base_dir}" if base_dir else "")
                    + ". Restore that checkout, or re-run make apply from this one to record "
                    "its paths, then destroy"
                )
            print(f"  {path} is gone; using {moved} from this checkout.")
            setattr(args, key, moved)


def verify_sql_identity(sql_file: str, expected: str) -> None:
    """Refuse unless sql_file hashes to the normalized hash the last apply recorded (none refuses)."""
    if not expected:
        _refuse_unidentified(sql_file)
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "modules", "data_access"))
    import hashlib

    from normalize_masking_sql import normalized_definitions

    try:
        with open(sql_file) as f:
            actual = hashlib.sha256(normalized_definitions(f.read()).encode()).hexdigest()
    except Exception as exc:
        _refuse(f"could not hash {sql_file} ({type(exc).__name__}: {exc})")
    if actual != expected:
        _refuse(
            f"{sql_file} is not the SQL the last apply deployed (sql_hash {actual}, state "
            f"records {expected}): it is another env's, or it changed since that apply. "
            "Point GENIERAILS_MASKING_BASE_DIR at this env's dir (make destroy sets it), "
            "or restore the SQL, then destroy"
        )


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
    parser.add_argument(
        "--base-dir",
        default=None,
        help="Directory relative --sql-file and --auth-file are resolved "
        "against (the env dir); given but empty, relative paths refuse",
    )
    parser.add_argument(
        "--expect-sql-hash",
        default="",
        help="With --drop: the normalized SQL hash state records; a "
        "--sql-file that hashes otherwise refuses before connecting",
    )
    parser.add_argument(
        "--relocate-from",
        default="",
        help="With --drop: the shared/ dir older state recorded absolute "
        "paths under, to find them in this checkout when they are gone",
    )
    args = parser.parse_args()
    resolve_paths(args)
    if args.drop:
        verify_sql_identity(args.sql_file, args.expect_sql_hash)
    load_credentials(args.auth_file, args.host)

    if args.drop:
        drop(args.sql_file, args.warehouse_id)
    else:
        deploy(args.sql_file, args.warehouse_id)


if __name__ == "__main__":
    main()
