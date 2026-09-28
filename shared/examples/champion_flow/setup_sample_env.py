#!/usr/bin/env python3
"""Create optional sample data and a Genie agent for champion_flow."""

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
    from databricks.sdk import WorkspaceClient
except ImportError:  # Keep --help useful before dependencies are installed.
    WorkspaceClient = None  # type: ignore[assignment,misc]

DEFAULT_SCHEMA, DEFAULT_ROWS = "champion_flow_demo", 200
STATE_FILE = Path(__file__).with_name(".champion_flow_sample_env.json")
DOMAINS, REGIONS = ("gmail.com", "hotmail.com", "outlook.com", "yahoo.com"), ("APAC", "EMEA", "AMER")
FIRST_NAMES = ("Olivia", "Liam", "Emma", "Noah", "Amelia", "Mateo", "Sophia", "Ethan", "Isabella", "Lucas", "Mia", "Benjamin", "Ava", "Daniel", "Harper", "James", "Camila", "Henry", "Layla", "Alexander")
LAST_NAMES = ("Anderson", "Patel", "Nguyen", "Williams", "Garcia", "Johnson", "Martinez", "Brown", "Kim", "Wilson", "Taylor", "Thomas", "Lee", "Hernandez", "Clark", "Lewis", "Walker", "Hall", "Young", "King")
TABLES = {
    "customers": "customer_id STRING, full_name STRING, email STRING, phone STRING, date_of_birth DATE, ssn STRING, region_code STRING",
    "payments": "payment_id STRING, customer_id STRING, credit_card_number STRING, cvv STRING, amount DECIMAL(12,2), cardholder_name STRING",
    "notes": "note_id STRING, customer_id STRING, free_text STRING",
}


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Optionally bootstrap or tear down the champion_flow sample environment.")
    p.add_argument("--profile", default=os.getenv("DATABRICKS_CONFIG_PROFILE"), help="Databricks CLI profile (env: DATABRICKS_CONFIG_PROFILE).")
    p.add_argument("--host", default=os.getenv("DATABRICKS_HOST"), help="Workspace URL (env: DATABRICKS_HOST).")
    p.add_argument("--catalog", default=os.getenv("CHAMPION_FLOW_CATALOG"), help="Existing UC catalog (required; env: CHAMPION_FLOW_CATALOG).")
    p.add_argument("--schema", default=os.getenv("CHAMPION_FLOW_SCHEMA", DEFAULT_SCHEMA), help=f"Sample schema (default: {DEFAULT_SCHEMA}; env: CHAMPION_FLOW_SCHEMA).")
    p.add_argument("--warehouse-id", default=os.getenv("DATABRICKS_WAREHOUSE_ID"), help="Existing SQL warehouse ID (required; env: DATABRICKS_WAREHOUSE_ID).")
    p.add_argument("--rows", default=os.getenv("CHAMPION_FLOW_ROWS", str(DEFAULT_ROWS)), help=f"Rows per table (default: {DEFAULT_ROWS}; env: CHAMPION_FLOW_ROWS).")
    p.add_argument("--teardown", action="store_true", help="Remove only resources recorded as created by this script.")
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
    kwargs: dict[str, Any] = {"product": "genierails-champion-flow", "product_version": "1.0"}
    if args.profile:
        kwargs["profile"] = args.profile
    if args.host:
        kwargs["host"] = args.host
    return WorkspaceClient(**kwargs)


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
            "description": "Optional synthetic PII environment for the GenieRails champion flow.",
            "serialized_space": json.dumps({"version": 2, "data_sources": {"tables": [{"identifier": table} for table in sorted(tables)]}}, separators=(",", ":"))}


def _create_space(client: Any, warehouse_id: str, tables: list[str], title: str) -> str:
    response = client.api_client.do("POST", "/api/2.0/genie/spaces",
                                    body=_space_payload(warehouse_id, tables, title))
    space_id = response.get("space_id", "")
    if not space_id:
        raise RuntimeError(f"Genie create API returned no space_id: {response}")
    return space_id


def _tfvars(space_id: str, tables: list[str], warehouse_id: str) -> str:
    lines = "\n".join(f'  "{table}",' for table in tables)
    return f'''uc_tables = [
{lines}
]

genie_spaces = [
  {{ genie_space_id = "{space_id}" }}
]

sql_warehouse_id = "{warehouse_id}"'''


def setup(args: argparse.Namespace, client: Any) -> None:
    if not args.catalog:
        raise RuntimeError("missing --catalog (or CHAMPION_FLOW_CATALOG); choose an existing UC catalog")
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
    print("[3/4] Creating or reusing the tracked Genie agent ...")
    space_id = state.get("space_id", "")
    title = f"GenieRails Champion Flow ({args.catalog}.{args.schema})"
    if not space_id or not _space_exists(client, space_id):
        space_id = _create_space(client, args.warehouse_id, tables, title)
        state.update(space_id=space_id, warehouse_id=args.warehouse_id)
        _save_states(states)
    else:
        client.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}",
                             body=_space_payload(args.warehouse_id, tables, title))
        state["warehouse_id"] = args.warehouse_id
        _save_states(states)
        print(f"      Genie agent {space_id} already exists; configuration refreshed.")
    print("[4/4] Complete. Paste this exact snippet into env.auto.tfvars:\n")
    print(_tfvars(space_id, tables, args.warehouse_id))
    print(f"\nGenie agent ID: {space_id}\nOwnership state: {STATE_FILE}")


def teardown(args: argparse.Namespace, client: Any) -> None:
    if not args.catalog:
        raise RuntimeError("teardown requires --catalog (or CHAMPION_FLOW_CATALOG)")
    states, key = _load_states(), _state_key(client, args.catalog, args.schema)
    state = states.get(key)
    if not state:
        print("Nothing to remove: no resources owned by this script are recorded for that host/catalog/schema.")
        return
    space_id = state.get("space_id", "")
    if space_id:
        print(f"Removing tracked Genie agent {space_id} ...")
        try:
            client.api_client.do("DELETE", f"/api/2.0/genie/spaces/{space_id}")
        except Exception as exc:
            if not _missing(exc):
                raise RuntimeError(f"could not remove Genie agent {space_id}: {exc}") from exc
    if state.get("schema_created"):
        warehouse_id = args.warehouse_id or state.get("warehouse_id", "")
        if not warehouse_id:
            raise RuntimeError("missing --warehouse-id and none was recorded; schema was not removed")
        print(f"Dropping tracked schema {args.catalog}.{args.schema} ...")
        _run_sql(client, warehouse_id, f"DROP SCHEMA IF EXISTS {_ident(args.catalog)}.{_ident(args.schema)} CASCADE")
    states.pop(key, None)
    _save_states(states) if states else STATE_FILE.unlink(missing_ok=True)
    print("Teardown complete. Only resources recorded as owned by this script were removed.")


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        try:
            args.rows = int(args.rows)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"--rows/CHAMPION_FLOW_ROWS must be an integer, got {args.rows!r}"
            ) from exc
        client = _client(args)
        teardown(args, client) if args.teardown else setup(args, client)
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
