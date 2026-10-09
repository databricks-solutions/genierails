#!/usr/bin/env python3
"""Run every deterministic mask body against a Databricks SQL warehouse."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import sys
import time
from decimal import Decimal
from pathlib import Path

import hcl2
from databricks.sdk import WorkspaceClient

SHARED = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED))
from mask_library import SQL_BODIES, hmac_function_ddl, keyed_hash  # noqa: E402
from scripts.hash_key import ensure_uc_secret, write_probe  # noqa: E402


def _execute(w: WorkspaceClient, warehouse_id: str, statement: str):
    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=statement, wait_timeout="50s"
    )
    state = str(response.status.state)
    if not state.endswith("SUCCEEDED"):
        raise RuntimeError(f"SQL statement failed ({state}): {response.status.error}")
    return (response.result.data_array or []) if response.result else []


CASES = {
    "redact_string": ("CAST('secret' AS STRING)", "[REDACTED]"),
    "null_string": ("CAST('secret' AS STRING)", None),
    "null_date": ("DATE'2024-02-03'", None),
    "null_timestamp": ("TIMESTAMP'2024-02-03 04:05:06'", None),
    "null_numeric": ("CAST(12.5 AS DECIMAL(10,2))", None),
    "last4_string": ("CAST('4111-1111-1111-1234' AS STRING)", "************1234"),
    "email_partial_string": ("CAST('Jane.Doe@Example.COM' AS STRING)", "J***@example.com"),
    "initials_string": ("CAST('Elodie van Lee' AS STRING)", "EVL"),
    "year_date": ("DATE'2024-12-31'", "2024-01-01"),
    "year_timestamp": ("TIMESTAMP'2024-12-31 04:05:06'", "2024-01-01T00:00:00.000Z"),
    "age_band_10_numeric": ("CAST(10 AS DOUBLE)", "10-19"),
    "credit_score_band_50_numeric": ("CAST(650 AS DOUBLE)", "650-699"),
    "rounded_numeric": ("CAST(-1500 AS DOUBLE)", -2000.0),
    "location_1dp_numeric": ("CAST(-1.25 AS DOUBLE)", -1.3),
    "ip_network_string": ("CAST('2001:db8:abcd:12:1234::1' AS STRING)", "2001:db8:abcd:12::/64"),
    "mac_vendor_string": ("CAST('aa-bb-cc-dd-ee-ff' AS STRING)", "AA:BB:CC:**:**:**"),
    "url_domain_string": ("CAST('https://User:pass@EXAMPLE.com:443/a?q=1' AS STRING)", "example.com"),
    "prefix_3_string": ("CAST('2000' AS STRING)", "200***"),
    "raw_string": ("CAST('x' AS STRING)", "x"),
    "raw_date": ("DATE'2024-02-03'", "2024-02-03"),
    "raw_timestamp": ("TIMESTAMP'2024-02-03 04:05:06'", "2024-02-03T04:05:06.000Z"),
    "raw_numeric": ("CAST(12.5 AS DECIMAL(10,2))", 12.5),
}


def run(w: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, generated_dir: Path) -> dict:
    key_text = os.environ.get("GENIERAILS_HASH_KEY")
    if not key_text:
        key_text = secrets.token_hex(32)
        os.environ["GENIERAILS_HASH_KEY"] = key_text
    mismatches: list[dict] = []
    tested = 0
    started = time.monotonic()
    _execute(w, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    try:
        ensure_uc_secret(w, catalog, schema, generated_dir)
        for name, (literal, expected) in CASES.items():
            body = SQL_BODIES[name]
            # JSON conversion gives stable DATE/TIMESTAMP/DECIMAL comparison.
            rows = _execute(w, warehouse_id, f"SELECT to_json(named_struct('v', {body})) FROM (SELECT {literal} AS value)")
            actual = json.loads(rows[0][0]).get("v")
            if actual != expected:
                mismatches.append({"body": name, "expected": expected, "actual": actual})
            tested += 1
        for statement in hmac_function_ddl(catalog, schema).split(";\n"):
            if statement.strip():
                _execute(w, warehouse_id, statement)
        rows = _execute(w, warehouse_id, f"SELECT `{catalog}`.`{schema}`.`gr_hmac_sha256`('genierails-probe')")
        probe = rows[0][0]
        expected_probe = keyed_hash("genierails-probe", key_text.encode())
        if probe != expected_probe:
            mismatches.append({"body": "hmac_string", "expected": expected_probe, "actual": probe})
        write_probe(generated_dir / "hash_probe.json", probe)
        tested += 1
        rows = _execute(w, warehouse_id, f"SELECT `{catalog}`.`{schema}`.`gr_hmac_sha256_numeric`(CAST(123.4500 AS DECIMAL(38,18)))")
        numeric_hash = rows[0][0]
        expected_numeric = keyed_hash(Decimal("123.4500"), key_text.encode(), numeric=True)
        if numeric_hash != expected_numeric:
            mismatches.append({"body": "hmac_numeric", "expected": expected_numeric, "actual": numeric_hash})
        tested += 1
        return {"bodies_tested": tested, "mismatches": len(mismatches), "details": mismatches, "elapsed_seconds": round(time.monotonic() - started, 2)}
    finally:
        try:
            _execute(w, warehouse_id, f"DROP FUNCTION IF EXISTS `{catalog}`.`{schema}`.`gr_hmac_sha256_numeric`")
            _execute(w, warehouse_id, f"DROP FUNCTION IF EXISTS `{catalog}`.`{schema}`.`gr_hmac_sha256_numeric_python`")
            _execute(w, warehouse_id, f"DROP FUNCTION IF EXISTS `{catalog}`.`{schema}`.`gr_hmac_sha256`")
            _execute(w, warehouse_id, f"DROP FUNCTION IF EXISTS `{catalog}`.`{schema}`.`gr_hmac_sha256_python`")
        finally:
            try:
                w.api_client.do("DELETE", f"/api/2.1/unity-catalog/secrets/{catalog}.{schema}.hmac_key")
            except Exception:
                pass
            _execute(w, warehouse_id, f"DROP SCHEMA IF EXISTS `{catalog}`.`{schema}` CASCADE")


def run_from_environment() -> dict:
    required = ["DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET", "DATABRICKS_WAREHOUSE_ID"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError("missing live-test environment: " + ", ".join(missing))
    w = WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"], client_id=os.environ["DATABRICKS_CLIENT_ID"],
        client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
    )
    return run(w, os.environ["DATABRICKS_WAREHOUSE_ID"], os.environ.get("DATABRICKS_TEST_CATALOG", "genierails_dev_1"), os.environ.get("DATABRICKS_TEST_SCHEMA", "gr_dg_libtest"), Path(os.environ.get("DATABRICKS_GENERATED_DIR", ".")))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--auth-file", type=Path)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--catalog", default="genierails_dev_1")
    parser.add_argument("--schema", default="gr_dg_libtest")
    parser.add_argument("--generated-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.auth_file and args.env_file:
        auth = hcl2.load(args.auth_file.open())
        env = hcl2.load(args.env_file.open())
        w = WorkspaceClient(
            host=auth["databricks_workspace_host"], client_id=auth["databricks_client_id"],
            client_secret=auth["databricks_client_secret"],
        )
        os.environ.setdefault("GENIERAILS_HASH_KEY", secrets.token_hex(32))
        result = run(w, env["sql_warehouse_id"], args.catalog, args.schema, args.generated_dir)
    else:
        result = run_from_environment()
    print(json.dumps(result, indent=2))
    return 1 if result["mismatches"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
