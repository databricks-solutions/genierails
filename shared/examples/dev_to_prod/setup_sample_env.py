#!/usr/bin/env python3
"""Create optional sample data and a Genie agent for dev_to_prod."""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable

try:
    from databricks.sdk import AccountClient, WorkspaceClient
except ImportError:  # Keep --help useful before dependencies are installed.
    AccountClient = WorkspaceClient = None  # type: ignore[assignment,misc]

DEFAULT_SCHEMA, DEFAULT_ROWS = "dev_to_prod_demo", 200
STATE_FILE = Path(__file__).with_name(".dev_to_prod_sample_env.json")
DOMAINS, REGIONS = ("gmail.com", "hotmail.com", "outlook.com", "yahoo.com"), ("APAC", "EMEA", "AMER")
FIRST_NAMES = ("Olivia", "Liam", "Emma", "Noah", "Amelia", "Mateo", "Sophia", "Ethan", "Isabella", "Lucas", "Mia", "Benjamin", "Ava", "Daniel", "Harper", "James", "Camila", "Henry", "Layla", "Alexander")
LAST_NAMES = ("Anderson", "Patel", "Nguyen", "Williams", "Garcia", "Johnson", "Martinez", "Brown", "Kim", "Wilson", "Taylor", "Thomas", "Lee", "Hernandez", "Clark", "Lewis", "Walker", "Hall", "Young", "King")
TABLES = {
    "customers": "customer_id STRING, full_name STRING, email STRING, phone STRING, date_of_birth DATE, ssn STRING, region_code STRING",
    "payments": "payment_id STRING, customer_id STRING, credit_card_number STRING, cvv STRING, amount DECIMAL(12,2), cardholder_name STRING",
    "notes": "note_id STRING, customer_id STRING, free_text STRING",
}
# Demo access tiers for --create-groups, most to least privileged.
SAMPLE_GROUPS = ("dev_to_prod_payments_ops", "dev_to_prod_regional_analysts", "dev_to_prod_viewers")


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Optionally bootstrap or tear down the dev_to_prod sample environment.")
    p.add_argument("--profile", default=os.getenv("DATABRICKS_CONFIG_PROFILE"), help="Databricks CLI profile (env: DATABRICKS_CONFIG_PROFILE).")
    p.add_argument("--host", default=os.getenv("DATABRICKS_HOST"), help="Workspace URL (env: DATABRICKS_HOST).")
    p.add_argument("--catalog", default=os.getenv("DEV_TO_PROD_CATALOG"), help="Existing UC catalog (required; env: DEV_TO_PROD_CATALOG).")
    p.add_argument("--schema", default=os.getenv("DEV_TO_PROD_SCHEMA", DEFAULT_SCHEMA), help=f"Sample schema (default: {DEFAULT_SCHEMA}; env: DEV_TO_PROD_SCHEMA).")
    p.add_argument("--warehouse-id", default=os.getenv("DATABRICKS_WAREHOUSE_ID"), help="Existing SQL warehouse ID (required; env: DATABRICKS_WAREHOUSE_ID).")
    p.add_argument("--rows", default=os.getenv("DEV_TO_PROD_ROWS", str(DEFAULT_ROWS)), help=f"Rows per table (default: {DEFAULT_ROWS}; env: DEV_TO_PROD_ROWS).")
    p.add_argument("--create-groups", action="store_true", help=f"Also create the demo access-tier account groups ({', '.join(SAMPLE_GROUPS)}) for an account with no IdP-synced groups; requires --account-id.")
    p.add_argument("--account-id", default=os.getenv("DATABRICKS_ACCOUNT_ID"), help="Databricks account ID for --create-groups (env: DATABRICKS_ACCOUNT_ID). Teardown uses the account recorded at creation and refuses a different one.")
    p.add_argument("--account-profile", default=os.getenv("DATABRICKS_ACCOUNT_PROFILE"), help="Account-level CLI profile for --create-groups; default: environment credentials (env: DATABRICKS_ACCOUNT_PROFILE).")
    p.add_argument("--skip-agent", action="store_true", help="Seed the tables only, without a Genie agent (e.g. the prod catalog: make promote creates prod's agent).")
    p.add_argument("--teardown", action="store_true", help="Remove only resources recorded as created by this script.")
    p.add_argument("--delete-legacy-groups-by-name", action="store_true", help="Teardown only: also delete groups recorded by name alone (state written before group IDs were recorded), by exact display name. Can remove a different group that reused the name.")
    return p


def _ident(value: str) -> str:
    if not value or "\x00" in value:
        raise ValueError("catalog/schema names must be non-empty and contain no NUL characters")
    return "`" + value.replace("`", "``") + "`"


def _string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _literal(value: Any) -> str:
    return str(value) if isinstance(value, (int, float)) else _string(str(value))


def _chunks(values: list[tuple[Any, ...]], size: int = 50) -> Iterable[list[tuple[Any, ...]]]:
    for start in range(0, len(values), size):
        yield values[start:start + size]


def _client(args: argparse.Namespace) -> Any:
    if WorkspaceClient is None:
        raise RuntimeError("databricks-sdk is not installed; run: pip install -r requirements.txt")
    kwargs: dict[str, Any] = {"product": "genierails-dev-to-prod", "product_version": "1.0"}
    if args.profile:
        kwargs["profile"] = args.profile
    if args.host:
        kwargs["host"] = args.host
    return WorkspaceClient(**kwargs)


def _account_host(workspace_host: str) -> str:
    host = workspace_host.lower()
    if "azuredatabricks.net" in host:
        return "https://accounts.azuredatabricks.net"
    if "gcp.databricks.com" in host:
        return "https://accounts.gcp.databricks.com"
    return "https://accounts.cloud.databricks.com"


def _account_client(args: argparse.Namespace, client: Any) -> Any:
    """Account client for --create-groups (and the opt-in legacy name cleanup)."""
    if AccountClient is None:
        raise RuntimeError("databricks-sdk is not installed; run: pip install -r requirements.txt")
    if not args.account_id:
        raise RuntimeError("--create-groups needs --account-id (or DATABRICKS_ACCOUNT_ID)")
    kwargs: dict[str, Any] = {"account_id": args.account_id, "product": "genierails-dev-to-prod", "product_version": "1.0"}
    if args.account_profile:
        kwargs["profile"] = args.account_profile
    else:
        kwargs["host"] = os.getenv("DATABRICKS_ACCOUNT_HOST") or _account_host(str(getattr(client.config, "host", "")))
    return AccountClient(**kwargs)


def _same_host(a: str, b: str) -> bool:
    return str(a).rstrip("/").lower() == str(b).rstrip("/").lower()


def _recorded_account_client(args: argparse.Namespace, state: dict[str, Any]) -> Any:
    """Account client for the account the groups were created in; nothing else.

    The recorded account ID and host are authoritative. An --account-id,
    DATABRICKS_ACCOUNT_ID, DATABRICKS_ACCOUNT_HOST or --account-profile that
    points at a different account is refused rather than used to delete there.
    """
    if AccountClient is None:
        raise RuntimeError("databricks-sdk is not installed; run: pip install -r requirements.txt")
    recorded_id, recorded_host = state.get("account_id"), state.get("account_host")
    if not recorded_id or not recorded_host:
        raise RuntimeError("ownership state records group IDs but not their account; refusing to delete them")
    conflicts = []
    if args.account_id and args.account_id != recorded_id:
        conflicts.append(f"account ID {args.account_id} (--account-id / DATABRICKS_ACCOUNT_ID)")
    env_host = os.getenv("DATABRICKS_ACCOUNT_HOST")
    if env_host and not _same_host(env_host, recorded_host):
        conflicts.append(f"account host {env_host} (DATABRICKS_ACCOUNT_HOST)")
    kwargs: dict[str, Any] = {"account_id": recorded_id, "product": "genierails-dev-to-prod", "product_version": "1.0"}
    if args.account_profile:
        kwargs["profile"] = args.account_profile
    else:
        kwargs["host"] = recorded_host
    account = None if conflicts else AccountClient(**kwargs)
    if account is not None and not (
        account.config.account_id == recorded_id and _same_host(account.config.host, recorded_host)
    ):
        conflicts.append(
            f"account {account.config.account_id} at {account.config.host} (--account-profile {args.account_profile})"
        )
    if conflicts:
        raise RuntimeError(
            f"the sample groups were created in account {recorded_id} at {recorded_host}, but you pointed teardown at "
            + " and ".join(conflicts)
            + "; refusing to delete groups under another account. Unset or correct that setting and re-run. Nothing was removed."
        )
    return account


def _preflight_account(account: Any, group_id: str | None, group_name: str | None = None) -> None:
    """Prove the credentials can act on the account with one read, before any deletion.

    Reads a recorded group by ID (a 'not found' still proves access: that group
    is already gone) or, for the legacy path, lists one recorded name.
    """
    if account is None:
        return
    try:
        if group_id:
            try:
                account.groups.get(id=group_id)
            except Exception as exc:
                if not _missing(exc):
                    raise
        else:
            list(account.groups.list(filter=f'displayName eq "{group_name}"'))
    except Exception as exc:
        raise RuntimeError(
            f"could not read account {account.config.account_id} with these credentials ({exc}); "
            "check auth and Account Admin access. Nothing was removed."
        ) from exc


def _ensure_groups(account: Any, state: dict[str, Any]) -> None:
    """Create missing SAMPLE_GROUPS; record the account and the ID of each group created."""
    created = list(state.get("groups_created", []))
    group_ids = dict(state.get("group_ids", {}))
    state["account_id"] = account.config.account_id
    state["account_host"] = account.config.host
    for name in SAMPLE_GROUPS:
        if any(g.display_name == name for g in account.groups.list(filter=f'displayName eq "{name}"')):
            print(f"      Reusing existing account group {name}")
            continue
        group = account.groups.create(display_name=name)
        print(f"      Created account group {name}")
        if name not in created:
            created.append(name)
        group_ids[name] = group.id
    state["groups_created"] = created
    state["group_ids"] = group_ids


def _forget_group(state: dict[str, Any], name: str, save: Any) -> None:
    state["groups_created"].remove(name)
    state.get("group_ids", {}).pop(name, None)
    save()


def _remove_recorded_groups(account: Any, state: dict[str, Any], names: list[str], save: Any) -> None:
    """Delete groups by recorded ID in the verified recorded account.

    A 'not found' there means the group is already gone, so its record is dropped.
    """
    for name in names:
        group_id = state["group_ids"][name]
        print(f"Removing tracked account group {name} ({group_id}) ...")
        try:
            account.groups.delete(id=group_id)
        except Exception as exc:
            if not _missing(exc):
                raise RuntimeError(f"could not remove account group {name} ({group_id}): {exc}") from exc
            print(f"      Already gone: {name} ({group_id})")
        _forget_group(state, name, save)


def _remove_legacy_groups(account: Any, state: dict[str, Any], names: list[str], save: Any) -> list[str]:
    """Opt-in: delete legacy (names-only) groups by exact display name; return the ones kept."""
    print(
        "WARNING: --delete-legacy-groups-by-name deletes any account group with these exact names, "
        "even one created by someone else after this script ran: " + ", ".join(names)
    )
    kept = []
    for name in names:
        matches = [g for g in account.groups.list(filter=f'displayName eq "{name}"') if g.display_name == name]
        if len(matches) != 1:
            print(f"      Keeping the record for {name}: {len(matches)} group(s) have that name, so it could not be confirmed.")
            kept.append(name)
            continue
        print(f"Removing legacy account group {name} ({matches[0].id}) by name ...")
        account.groups.delete(id=matches[0].id)
        _forget_group(state, name, save)
    return kept


def _legacy_guidance(names: list[str]) -> str:
    return (
        "This ownership state was written before group IDs were recorded, so these groups are known by name only: "
        + ", ".join(names) + ". They were NOT deleted, because a different group may now use the same name. "
        "Remove them by hand (Account Console > User management > Groups), or re-run teardown with "
        "--delete-legacy-groups-by-name --account-id <account-id> to delete the exact-name matches."
    )


def _run_sql(client: Any, warehouse_id: str, statement: str) -> None:
    try:
        response = client.statement_execution.execute_statement(warehouse_id=warehouse_id, statement=statement, wait_timeout="50s")
    except Exception as exc:
        raise RuntimeError(f"SQL execution failed: {exc}") from exc
    statement_id = getattr(response, "statement_id", None)
    deadline = time.monotonic() + 900
    while True:
        status = getattr(response, "status", None)
        state = str(getattr(status, "state", "")).upper()
        if "SUCCEEDED" in state:
            return
        if any(terminal in state for terminal in ("FAILED", "CANCELED", "CLOSED")):
            raise RuntimeError(f"SQL statement ended in {state}: {getattr(status, 'error', None) or 'no detail'}")
        if not statement_id:
            raise RuntimeError(f"SQL statement returned {state or 'an unknown state'} without a statement ID")
        if time.monotonic() >= deadline:
            try:
                client.statement_execution.cancel_execution(statement_id=statement_id)
            except Exception as exc:
                raise RuntimeError(f"SQL statement {statement_id} did not finish within 15 minutes and could not be canceled: {exc}") from exc
            # Observe the terminal result: cancellation can race with successful
            # completion, and a successful CREATE must reach the ownership write.
            cancel_deadline = time.monotonic() + 60
            while time.monotonic() < cancel_deadline:
                time.sleep(2)
                response = client.statement_execution.get_statement(statement_id=statement_id)
                cancel_state = str(getattr(getattr(response, "status", None), "state", "")).upper()
                if "SUCCEEDED" in cancel_state:
                    return
                if any(terminal in cancel_state for terminal in ("FAILED", "CANCELED", "CLOSED")):
                    error = getattr(getattr(response, "status", None), "error", None)
                    raise RuntimeError(f"SQL statement ended in {cancel_state}: {error or 'no detail'}")
            raise RuntimeError(f"SQL statement {statement_id} did not report a terminal state after cancellation")
        time.sleep(2)
        try:
            response = client.statement_execution.get_statement(statement_id=statement_id)
        except Exception as exc:
            raise RuntimeError(f"could not poll SQL statement {statement_id}: {exc}") from exc


def _luhn_card(rng: random.Random) -> str:
    digits = [4] + [rng.randrange(10) for _ in range(14)]
    total = 0
    for index, digit in enumerate(reversed(digits)):
        if index % 2 == 0:
            digit = digit * 2 - 9 if digit * 2 > 9 else digit * 2
        total += digit
    return "".join(map(str, digits)) + str((-total) % 10)


def _ssn(rng: random.Random) -> str:
    area = rng.choice([n for n in range(1, 900) if n != 666])
    return f"{area:03d}-{rng.randint(1, 99):02d}-{rng.randint(1, 9999):04d}"


def _phone(rng: random.Random) -> str:
    return f"+1 ({rng.randint(200, 899):03d}) {rng.randint(200, 999):03d}-{rng.randint(0, 9999):04d}"


def _sample_rows(count: int) -> dict[str, list[tuple[Any, ...]]]:
    rng, start = random.Random(20260928), date(1945, 1, 1)
    span = (date(2003, 12, 31) - start).days
    result: dict[str, list[tuple[Any, ...]]] = {name: [] for name in TABLES}
    for i in range(1, count + 1):
        first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        name, phone = f"{first} {last}", _phone(rng)
        email = f"{first}.{last}{rng.randint(10, 9999)}@{rng.choice(DOMAINS)}".lower()
        customer_id = f"CUST-{i:06d}"
        dob = start + timedelta(days=rng.randint(0, span))
        result["customers"].append((customer_id, name, email, phone, dob.isoformat(), _ssn(rng), rng.choice(REGIONS)))
        result["payments"].append((f"PAY-{i:06d}", customer_id, _luhn_card(rng), f"{rng.randint(0, 999):03d}", f"{rng.uniform(8, 5000):.2f}", name))
        result["notes"].append((f"NOTE-{i:06d}", customer_id, f"Spoke with {name} about the account. Follow up at {email} or call {phone} next week."))
    return result


def _load_states() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"cannot read ownership state {STATE_FILE}: {exc}") from exc


def _save_states(states: dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(states, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _state_key(client: Any, catalog: str, schema: str) -> str:
    return f"{str(getattr(client.config, 'host', '')).rstrip('/')}|{catalog}|{schema}"


def _missing(exc: Exception) -> bool:
    return "404" in str(exc) or "RESOURCE_DOES_NOT_EXIST" in str(exc)


def _space_exists(client: Any, space_id: str) -> bool:
    try:
        client.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}")
        return True
    except Exception as exc:
        if _missing(exc):
            return False
        raise


def _space_payload(warehouse_id: str, tables: list[str], title: str) -> dict[str, str]:
    return {"warehouse_id": warehouse_id, "title": title,
            "description": "Optional synthetic PII environment for the GenieRails dev-to-prod walkthrough.",
            "serialized_space": json.dumps({"version": 2, "data_sources": {"tables": [{"identifier": table} for table in sorted(tables)]}}, separators=(",", ":"))}


def _create_space(client: Any, warehouse_id: str, tables: list[str], title: str) -> str:
    response = client.api_client.do("POST", "/api/2.0/genie/spaces",
                                    body=_space_payload(warehouse_id, tables, title))
    space_id = response.get("space_id", "")
    if not space_id:
        raise RuntimeError(f"Genie create API returned no space_id: {response}")
    return space_id


def _reconcile_existing_space(
    state: dict[str, Any], warehouse_id: str, tables: list[str], title: str
) -> None:
    """Validate an owned space on rerun without PATCHing its node graph.

    The Genie PATCH endpoint re-imports the space graph even for metadata-only
    bodies and rejects the already-present root ``.geniespace.json`` node.
    A same-config rerun therefore reuses the tracked space unchanged. Changing
    warehouses requires an explicit teardown/recreate rather than a partial,
    misleading reconciliation.
    """
    tracked_warehouse = str(state.get("warehouse_id", ""))
    if tracked_warehouse and tracked_warehouse != warehouse_id:
        raise RuntimeError(
            f"tracked Genie agent uses warehouse {tracked_warehouse}, not {warehouse_id}; "
            "run with --teardown before changing the sample warehouse"
        )
    drift = []
    if sorted(state.get("tables", [])) != sorted(tables):
        drift.append("tables")
    if state.get("title") != title:
        drift.append("title")
    if drift:
        print(
            "      WARNING: tracked Genie agent " + " and ".join(drift)
            + " differ from the requested configuration. Configuration is NOT re-applied; "
            "run with --teardown, then recreate the sample environment to change it."
        )


def _tfvars(space_id: str, tables: list[str], warehouse_id: str, groups: Iterable[str] = ()) -> str:
    lines = "\n".join(f'  "{table}",' for table in tables)
    tiers = ", ".join(f'"{group}"' for group in groups)
    groups_line = f"\n\naccess_tier_groups = [{tiers}]" if tiers else ""
    return f'''uc_tables = [
{lines}
]

genie_spaces = [
  {{ genie_space_id = "{space_id}" }}
]

sql_warehouse_id = "{warehouse_id}"{groups_line}'''


def setup(args: argparse.Namespace, client: Any) -> None:
    if not args.catalog:
        raise RuntimeError("missing --catalog (or DEV_TO_PROD_CATALOG); choose an existing UC catalog")
    if not args.warehouse_id:
        raise RuntimeError("missing --warehouse-id (or DATABRICKS_WAREHOUSE_ID); an existing SQL warehouse is required")
    if args.rows < 1:
        raise RuntimeError("--rows must be at least 1")
    states, key = _load_states(), _state_key(client, args.catalog, args.schema)
    state = states.get(key)
    schema_sql = f"{_ident(args.catalog)}.{_ident(args.schema)}"
    tables = [f"{args.catalog}.{args.schema}.{name}" for name in TABLES]
    if state is None:
        print(f"[1/4] Creating owned schema {args.catalog}.{args.schema} ...")
        try:
            _run_sql(client, args.warehouse_id, f"CREATE SCHEMA {schema_sql}")
        except RuntimeError as exc:
            detail = str(exc)
            if "ALREADY_EXISTS" in detail.upper() or "ALREADY EXISTS" in detail.upper():
                raise RuntimeError(
                    f"schema {args.catalog}.{args.schema} already exists but is not tracked as owned by this script; "
                    f"choose another --schema or restore the ownership state. Databricks said: {detail}"
                ) from exc
            raise RuntimeError(
                f"could not create schema {args.catalog}.{args.schema}. Databricks said: {detail}"
            ) from exc
        state = {"catalog": args.catalog, "schema": args.schema, "schema_created": True, "space_id": "", "warehouse_id": args.warehouse_id}
        states[key] = state
        _save_states(states)
    else:
        print(f"[1/4] Reusing tracked schema {args.catalog}.{args.schema} ...")
    print(f"[2/4] Creating exact footprint and seeding {args.rows} rows/table ...")
    generated = _sample_rows(args.rows)
    for name, definition in TABLES.items():
        target = f"{schema_sql}.{_ident(name)}"
        _run_sql(client, args.warehouse_id, f"CREATE TABLE IF NOT EXISTS {target} ({definition}) USING DELTA")
        _run_sql(client, args.warehouse_id, f"TRUNCATE TABLE {target}")
        for chunk in _chunks(generated[name]):
            values = ",\n".join("(" + ", ".join(_literal(v) for v in row) + ")" for row in chunk)
            _run_sql(client, args.warehouse_id, f"INSERT INTO {target} VALUES\n{values}")
    if getattr(args, "skip_agent", False):
        print(f"[3/4] Skipping the Genie agent (--skip-agent). Tables ready: {', '.join(tables)}")
        return
    print("[3/4] Creating or reusing the tracked Genie agent ...")
    space_id = state.get("space_id", "")
    title = f"GenieRails Dev-to-Prod Walkthrough ({args.catalog}.{args.schema})"
    if not space_id or not _space_exists(client, space_id):
        space_id = _create_space(client, args.warehouse_id, tables, title)
        state.update(
            space_id=space_id, warehouse_id=args.warehouse_id,
            tables=tables, title=title,
        )
        _save_states(states)
    else:
        _reconcile_existing_space(state, args.warehouse_id, tables, title)
        print(f"      Reusing tracked Genie agent {space_id} without re-importing its node graph.")
        state["warehouse_id"] = args.warehouse_id
        _save_states(states)
    groups: tuple[str, ...] = ()
    if getattr(args, "create_groups", False):
        print("      Creating or reusing the demo access-tier account groups ...")
        _ensure_groups(_account_client(args, client), state)
        _save_states(states)
        groups = SAMPLE_GROUPS
    replaced = "genie_spaces, sql_warehouse_id, and access_tier_groups" if groups else "genie_spaces and sql_warehouse_id"
    print(f"[4/4] Complete. In envs/dev/env.auto.tfvars, REPLACE the {replaced} lines with this snippet:\n")
    print(_tfvars(space_id, tables, args.warehouse_id, groups))
    print(f"\nGenie agent ID: {space_id}\nOwnership state: {STATE_FILE}")


def teardown(args: argparse.Namespace, client: Any) -> bool:
    """Remove what this script recorded as its own; return False if records remain."""
    if not args.catalog:
        raise RuntimeError("teardown requires --catalog (or DEV_TO_PROD_CATALOG)")
    states, key = _load_states(), _state_key(client, args.catalog, args.schema)
    state = states.get(key)
    if not state:
        print("Nothing to remove: no resources owned by this script are recorded for that host/catalog/schema.")
        return True

    def save() -> None:
        _save_states(states)

    created = list(state.get("groups_created", []))
    recorded = [name for name in created if state.get("group_ids", {}).get(name)]
    legacy = [name for name in created if name not in recorded]
    # Resolve and prove the accounts first: a conflict or an account the
    # credentials cannot act on must leave every resource in place.
    account = _recorded_account_client(args, state) if recorded else None
    legacy_account = None
    if legacy and getattr(args, "delete_legacy_groups_by_name", False):
        legacy_account = _recorded_account_client(args, state) if state.get("account_id") else _account_client(args, client)
    _preflight_account(account, state["group_ids"][recorded[0]] if recorded else None)
    _preflight_account(legacy_account, None, legacy[0] if legacy else None)

    def remove_space() -> None:
        space_id = state.get("space_id", "")
        if not space_id:
            return
        print(f"Removing tracked Genie agent {space_id} ...")
        try:
            client.api_client.do("DELETE", f"/api/2.0/genie/spaces/{space_id}")
        except Exception as exc:
            if not _missing(exc):
                raise RuntimeError(f"could not remove Genie agent {space_id}: {exc}") from exc
        state["space_id"] = ""
        save()

    def remove_schema() -> None:
        if not state.get("schema_created"):
            return
        warehouse_id = args.warehouse_id or state.get("warehouse_id", "")
        if not warehouse_id:
            raise RuntimeError("missing --warehouse-id and none was recorded; schema was not removed")
        print(f"Dropping tracked schema {args.catalog}.{args.schema} ...")
        _run_sql(client, warehouse_id, f"DROP SCHEMA IF EXISTS {_ident(args.catalog)}.{_ident(args.schema)} CASCADE")
        state["schema_created"] = False
        save()

    kept = legacy
    failure = None
    try:
        remove_space()
        if recorded:
            _remove_recorded_groups(account, state, recorded, save)
        if legacy and legacy_account is not None:
            kept = _remove_legacy_groups(legacy_account, state, legacy, save)
        remove_schema()
    except Exception as exc:  # stop at the first failed step; its record stays
        failure = exc
        save()
    remaining = (
        ([f"Genie agent {state['space_id']}"] if state.get("space_id") else [])
        + [f"account group {name}" for name in state.get("groups_created", [])]
        + ([f"schema {args.catalog}.{args.schema}"] if state.get("schema_created") else [])
    )
    if failure is not None or remaining:
        print("\nTeardown INCOMPLETE. Kept the ownership records for: " + ", ".join(remaining) + ".")
        if failure is not None:
            print(f"  Stopped because: {failure}")
            print("  Fix the cause and re-run teardown; it resumes from the kept records.")
        if legacy_account is None and kept and failure is None:
            print(_legacy_guidance(kept))
        return False
    states.pop(key, None)
    _save_states(states) if states else STATE_FILE.unlink(missing_ok=True)
    print("Teardown complete. Only resources recorded as owned by this script were removed.")
    return True


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        try:
            args.rows = int(args.rows)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"--rows/DEV_TO_PROD_ROWS must be an integer, got {args.rows!r}"
            ) from exc
        client = _client(args)
        if args.teardown:
            return 0 if teardown(args, client) else 1
        setup(args, client)
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
