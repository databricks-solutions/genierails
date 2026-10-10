#!/usr/bin/env python3
"""Run every deterministic mask body against a Databricks SQL warehouse."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from decimal import Decimal
from pathlib import Path

SHARED = Path(__file__).resolve().parents[1]
UTC = dt.timezone.utc
PST = dt.timezone(dt.timedelta(hours=-8))
sys.path.insert(0, str(SHARED))
from mask_library import SQL_BODIES  # noqa: E402

# The Statement Execution API opens a fresh session per call, so a separate
# SET TIME ZONE never reaches the next statement. Each SELECT instead renders
# its result in a non-UTC zone through to_json options, which do apply per
# statement. TIMESTAMP inputs carry explicit offsets and straddle a year
# boundary in UTC, so the expected values below are time-zone correct.
RENDER_ZONE = "America/Los_Angeles"
TO_JSON_OPTIONS = f"map('timeZone', '{RENDER_ZONE}', 'timestampNTZFormat', 'yyyy-MM-dd HH:mm:ss')"


def _execute(w, warehouse_id: str, statement: str):
    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=statement, wait_timeout="50s"
    )
    state = str(response.status.state)
    if not state.endswith("SUCCEEDED"):
        raise RuntimeError(f"SQL statement failed ({state}): {response.status.error}")
    return (response.result.data_array or []) if response.result else []


# Exact expected output per shipped SQL body: (SQL input, Python input, JSON
# value of to_json(named_struct('v', body), TO_JSON_OPTIONS)). The live run
# executes the SQL; the offline tests run the Python reference on the same rows.
BODY_CASES = {
    "redact_string": [("CAST('secret' AS STRING)", "secret", "[REDACTED]"), ("CAST(NULL AS STRING)", None, None)],
    "null_string": [("CAST('secret' AS STRING)", "secret", None)],
    "null_date": [("DATE'2024-02-03'", dt.date(2024, 2, 3), None)],
    "null_timestamp": [("TIMESTAMP'2024-02-03 04:05:06Z'", dt.datetime(2024, 2, 3, 4, 5, 6, tzinfo=UTC), None)],
    "null_timestamp_ntz": [("TIMESTAMP_NTZ'2024-02-03 04:05:06'", dt.datetime(2024, 2, 3, 4, 5, 6), None)],
    "null_numeric": [("CAST(12.5 AS DECIMAL(10,2))", Decimal("12.5"), None)],
    "last4_string": [("CAST('4111-1111-1111-1234' AS STRING)", "4111-1111-1111-1234", "************1234"),
                     ("CAST('12 34' AS STRING)", "12 34", "[REDACTED]")],
    "email_partial_string": [("CAST('Jane.Doe@Example.COM' AS STRING)", "Jane.Doe@Example.COM", "J***@example.com"),
                             ("CAST('bad@@example' AS STRING)", "bad@@example", "[REDACTED]")],
    "initials_string": [("CAST('Elodie van Lee' AS STRING)", "Elodie van Lee", "EVL"),
                        ("CAST('Alice' AS STRING)", "Alice", "[REDACTED]")],
    "year_date": [("DATE'2024-12-31'", dt.date(2024, 12, 31), "2024-01-01")],
    # 20:00 at -08:00 is 2025 in UTC; 01:00 at +02:00 is still 2023 in UTC.
    "year_timestamp": [("TIMESTAMP'2024-12-31 20:00:00-08:00'", dt.datetime(2024, 12, 31, 20, tzinfo=PST), "2024-12-31T16:00:00.000-08:00"),
                       ("TIMESTAMP'2024-01-01 01:00:00+02:00'", dt.datetime(2024, 1, 1, 1, tzinfo=dt.timezone(dt.timedelta(hours=2))), "2022-12-31T16:00:00.000-08:00")],
    "year_timestamp_ntz": [("TIMESTAMP_NTZ'2024-12-31 23:30:00'", dt.datetime(2024, 12, 31, 23, 30), "2024-01-01 00:00:00")],
    "age_band_10_numeric": [("CAST(10 AS DOUBLE)", 10.0, 10), ("CAST(27 AS INT)", 27, 20), ("CAST(-1 AS DOUBLE)", -1.0, None)],
    "credit_score_band_50_numeric": [("CAST(649 AS DOUBLE)", 649.0, 600), ("CAST(650 AS DOUBLE)", 650.0, 650)],
    "rounded_numeric": [("CAST(-1500 AS DOUBLE)", -1500.0, Decimal("-2000")), ("CAST(1499 AS DOUBLE)", 1499.0, Decimal("1000"))],
    "location_1dp_numeric": [("CAST(-1.25 AS DOUBLE)", -1.25, Decimal("-1.3")), ("CAST(1.24 AS DOUBLE)", 1.24, Decimal("1.2"))],
    "ip_network_string": [("CAST('192.168.2.99' AS STRING)", "192.168.2.99", "192.168.2.0/24"),
                          ("CAST('192.168.2.09' AS STRING)", "192.168.2.09", "[REDACTED]"),
                          ("CAST('999.1.2.3' AS STRING)", "999.1.2.3", "[REDACTED]"),
                          ("CAST('2001:db8:abcd:12:1234::1' AS STRING)", "2001:db8:abcd:12:1234::1", "[REDACTED]")],
    "mac_vendor_string": [("CAST('aa-bb-cc-dd-ee-ff' AS STRING)", "aa-bb-cc-dd-ee-ff", "AA:BB:CC:**:**:**"),
                          ("CAST('zz:bb:cc:dd:ee:ff' AS STRING)", "zz:bb:cc:dd:ee:ff", "[REDACTED]")],
    "url_domain_string": [("CAST('https://User:pass@EXAMPLE.com:443/a?q=1' AS STRING)", "https://User:pass@EXAMPLE.com:443/a?q=1", "example.com"),
                          ("CAST('javascript:alert(1)' AS STRING)", "javascript:alert(1)", "[REDACTED]")],
    "prefix_3_string": [("CAST('2000' AS STRING)", "2000", "200***"), ("CAST('abc' AS STRING)", "abc", "[REDACTED]")],
    "raw_string": [("CAST('x' AS STRING)", "x", "x")],
    "raw_date": [("DATE'2024-02-03'", dt.date(2024, 2, 3), "2024-02-03")],
    "raw_timestamp": [("TIMESTAMP'2024-02-03 04:05:06Z'", dt.datetime(2024, 2, 3, 4, 5, 6, tzinfo=UTC), "2024-02-02T20:05:06.000-08:00")],
    "raw_numeric": [("CAST(12.5 AS DECIMAL(10,2))", Decimal("12.5"), Decimal("12.5"))],
}


def comparable(value):
    """Numbers compare as exact decimals, so 10, 10.0 and 1E+1 are equal."""
    return Decimal(str(value)) if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool) else value


def run(w, warehouse_id: str) -> dict:
    """Read-only: every body is evaluated inline, so nothing is created."""
    mismatches: list[dict] = []
    tested = 0
    started = time.monotonic()
    for name, cases in BODY_CASES.items():
        for literal, _python_input, expected in cases:
            rows = _execute(w, warehouse_id, f"SELECT to_json(named_struct('v', {SQL_BODIES[name]}), {TO_JSON_OPTIONS}) FROM (SELECT {literal} AS value)")
            actual = json.loads(rows[0][0], parse_float=Decimal).get("v")
            if comparable(actual) != comparable(expected):
                mismatches.append({"body": name, "input": literal, "expected": str(expected), "actual": str(actual)})
            tested += 1
    return {"bodies_tested": tested, "mismatches": len(mismatches), "details": mismatches, "elapsed_seconds": round(time.monotonic() - started, 2)}


def run_from_environment() -> dict:
    from databricks.sdk import WorkspaceClient

    required = ["DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET", "DATABRICKS_WAREHOUSE_ID"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError("missing live-test environment: " + ", ".join(missing))
    w = WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"], client_id=os.environ["DATABRICKS_CLIENT_ID"],
        client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
    )
    return run(w, os.environ["DATABRICKS_WAREHOUSE_ID"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--auth-file", type=Path)
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    if args.auth_file and args.env_file:
        import hcl2
        from databricks.sdk import WorkspaceClient

        auth = hcl2.load(args.auth_file.open())
        env = hcl2.load(args.env_file.open())
        w = WorkspaceClient(
            host=auth["databricks_workspace_host"], client_id=auth["databricks_client_id"],
            client_secret=auth["databricks_client_secret"],
        )
        result = run(w, env["sql_warehouse_id"])
    else:
        result = run_from_environment()
    print(json.dumps(result, indent=2))
    return 1 if result["mismatches"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
