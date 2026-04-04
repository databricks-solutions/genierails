#!/usr/bin/env python3
"""
Australian Bank Demo — Setup and Teardown

Provisions a complete demo environment for the GenieRails champion flow:
  - Dev workspace + prod workspace (fresh, isolated)
  - Unity Catalog metastore with cloud storage
  - Sample Australian banking tables (customers, accounts, transactions, credit_cards)
  - An ungoverned Genie Space pointing at the dev tables

After setup, follow the README.md to run the demo.

Usage
-----
  # Provision everything (from the cloud wrapper directory: aws/ or azure/)
  python shared/examples/aus_bank_demo/setup_demo.py provision \\
      --env-file shared/scripts/account-admin.aws.env

  # Check status
  python shared/examples/aus_bank_demo/setup_demo.py status

  # Tear down everything
  python shared/examples/aus_bank_demo/setup_demo.py teardown

Prerequisites
-------------
  - shared/scripts/account-admin.<cloud>.env with Account Admin SP credentials
  - See shared/docs/prerequisites.md for full requirements
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
SHARED_DIR = SCRIPT_DIR.parent.parent  # shared/
SCRIPTS_DIR = SHARED_DIR / "scripts"
CLOUD_ROOT = Path(os.environ.get("CLOUD_ROOT", SHARED_DIR.parent / "aws"))

_default_cloud = os.environ.get("CLOUD_PROVIDER", "aws").lower()
STATE_FILE = SCRIPT_DIR / f".demo_state.{_default_cloud}.json"

# ---------------------------------------------------------------------------
# Catalog + table definitions
# ---------------------------------------------------------------------------
DEV_CATALOG = "dev_bank"
PROD_CATALOG = "prod_bank"
SCHEMA = "retail"

SETUP_SQL = f"""
-- ── Customers ────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE {DEV_CATALOG}.{SCHEMA}.customers (
  customer_id     BIGINT    COMMENT 'Unique customer identifier',
  first_name      STRING    COMMENT 'Customer first name',
  last_name       STRING    COMMENT 'Customer last name',
  email           STRING    COMMENT 'Contact email address',
  phone           STRING    COMMENT 'Australian phone number (+61 format)',
  address         STRING    COMMENT 'Residential street address',
  suburb          STRING    COMMENT 'Suburb or locality',
  state           STRING    COMMENT 'Australian state (NSW, VIC, QLD, etc.)',
  postcode        STRING    COMMENT 'Australian postcode (4 digits)',
  tfn             STRING    COMMENT 'Tax File Number — highly sensitive Australian PII (9 digits)',
  medicare_number STRING    COMMENT 'Medicare card number — sensitive Australian health identifier',
  date_of_birth   DATE      COMMENT 'Date of birth',
  bsb             STRING    COMMENT 'Bank-State-Branch number (6 digits, format XXX-XXX)',
  account_number  STRING    COMMENT 'Bank account number'
)
USING delta
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

-- ── Accounts ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE {DEV_CATALOG}.{SCHEMA}.accounts (
  account_id      BIGINT       COMMENT 'Unique account identifier',
  customer_id     BIGINT       COMMENT 'FK to customers',
  bsb             STRING       COMMENT 'Bank-State-Branch number (6 digits)',
  account_number  STRING       COMMENT 'Bank account number',
  account_type    STRING       COMMENT 'SAVINGS, EVERYDAY, TERM_DEPOSIT, HOME_LOAN',
  balance         DECIMAL(18,2) COMMENT 'Current account balance in AUD',
  opened_date     DATE         COMMENT 'Date account was opened',
  branch          STRING       COMMENT 'Branch name (e.g. Sydney CBD, Melbourne Central)'
)
USING delta
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

-- ── Transactions ─────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE {DEV_CATALOG}.{SCHEMA}.transactions (
  transaction_id  BIGINT       COMMENT 'Unique transaction identifier',
  account_id      BIGINT       COMMENT 'FK to accounts',
  transaction_date TIMESTAMP   COMMENT 'Date and time of transaction',
  amount          DECIMAL(18,2) COMMENT 'Transaction amount in AUD',
  merchant        STRING       COMMENT 'Merchant or payee name',
  category        STRING       COMMENT 'Transaction category (RETAIL, TRANSFER, ATM, INTERNATIONAL)',
  aml_risk_flag   STRING       COMMENT 'AML risk assessment: CLEAR, REVIEW, HIGH_RISK, BLOCKED',
  cross_border    BOOLEAN      COMMENT 'True if international transaction',
  country         STRING       COMMENT 'Destination country code (AU, NZ, SG, etc.)'
)
USING delta
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

-- ── Credit Cards ─────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE {DEV_CATALOG}.{SCHEMA}.credit_cards (
  card_id         BIGINT       COMMENT 'Unique card identifier',
  customer_id     BIGINT       COMMENT 'FK to customers',
  card_number     STRING       COMMENT 'Full credit card PAN — PCI-DSS sensitive',
  cvv             STRING       COMMENT 'Card verification value — PCI-DSS sensitive',
  expiry_date     STRING       COMMENT 'Card expiry (MM/YY)',
  credit_limit    DECIMAL(18,2) COMMENT 'Credit limit in AUD',
  card_type       STRING       COMMENT 'VISA, MASTERCARD, AMEX',
  status          STRING       COMMENT 'ACTIVE, BLOCKED, EXPIRED'
)
USING delta
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
"""

SAMPLE_DATA_SQL = f"""
-- ── Customers (realistic Australian data) ────────────────────────────────
INSERT INTO {DEV_CATALOG}.{SCHEMA}.customers VALUES
(1001, 'Sarah',    'Chen',       'sarah.chen@email.com.au',      '+61 412 345 678', '42 George St',      'Sydney',       'NSW', '2000', '123 456 789', '2123 45670 1', '1985-03-14', '062-000', '12345678'),
(1002, 'James',    'O''Brien',   'james.obrien@email.com.au',    '+61 423 456 789', '15 Collins St',     'Melbourne',    'VIC', '3000', '234 567 890', '3234 56781 2', '1978-07-22', '063-000', '23456789'),
(1003, 'Priya',    'Sharma',     'priya.sharma@email.com.au',    '+61 434 567 890', '8 Queen St',        'Brisbane',     'QLD', '4000', '345 678 901', '4345 67892 3', '1992-11-05', '064-000', '34567890'),
(1004, 'David',    'Williams',   'david.williams@email.com.au',  '+61 445 678 901', '23 King William St','Adelaide',     'SA',  '5000', '456 789 012', '5456 78903 4', '1970-01-30', '065-000', '45678901'),
(1005, 'Mei',      'Nguyen',     'mei.nguyen@email.com.au',      '+61 456 789 012', '5 Hay St',          'Perth',        'WA',  '6000', '567 890 123', '6567 89014 5', '1988-09-18', '066-000', '56789012'),
(1006, 'Tom',      'Wilson',     'tom.wilson@email.com.au',      '+61 467 890 123', '12 Liverpool St',   'Hobart',       'TAS', '7000', '678 901 234', '7678 90125 6', '1995-04-12', '067-000', '67890123'),
(1007, 'Anh',      'Tran',       'anh.tran@email.com.au',        '+61 478 901 234', '31 Smith St',       'Darwin',       'NT',  '0800', '789 012 345', '8789 01236 7', '1982-12-25', '068-000', '78901234'),
(1008, 'Emily',    'Jones',      'emily.jones@email.com.au',     '+61 489 012 345', '7 Northbourne Ave', 'Canberra',     'ACT', '2600', '890 123 456', '9890 12347 8', '1990-06-08', '062-001', '89012345'),
(1009, 'Ravi',     'Patel',      'ravi.patel@email.com.au',      '+61 490 123 456', '19 Pitt St',        'Sydney',       'NSW', '2000', '901 234 567', '2901 23458 9', '1975-08-20', '062-002', '90123456'),
(1010, 'Jessica',  'Brown',      'jessica.brown@email.com.au',   '+61 401 234 567', '4 Swanston St',     'Melbourne',    'VIC', '3000', '012 345 678', '3012 34569 0', '1998-02-14', '063-001', '01234567');

-- ── Accounts ─────────────────────────────────────────────────────────────
INSERT INTO {DEV_CATALOG}.{SCHEMA}.accounts VALUES
(2001, 1001, '062-000', '12345678', 'EVERYDAY',     15420.50,  '2015-03-10', 'Sydney CBD'),
(2002, 1001, '062-000', '12345679', 'SAVINGS',     142500.00,  '2015-03-10', 'Sydney CBD'),
(2003, 1002, '063-000', '23456789', 'EVERYDAY',      8730.25,  '2018-07-15', 'Melbourne Central'),
(2004, 1003, '064-000', '34567890', 'EVERYDAY',     23100.80,  '2020-01-05', 'Brisbane City'),
(2005, 1003, '064-000', '34567891', 'TERM_DEPOSIT', 50000.00,  '2022-06-01', 'Brisbane City'),
(2006, 1004, '065-000', '45678901', 'HOME_LOAN',  -485000.00,  '2019-09-20', 'Adelaide Central'),
(2007, 1005, '066-000', '56789012', 'SAVINGS',      67800.00,  '2021-04-12', 'Perth CBD'),
(2008, 1006, '067-000', '67890123', 'EVERYDAY',      3200.15,  '2023-01-08', 'Hobart'),
(2009, 1007, '068-000', '78901234', 'SAVINGS',      28900.00,  '2017-11-30', 'Darwin'),
(2010, 1008, '062-001', '89012345', 'EVERYDAY',     11500.75,  '2022-03-22', 'Canberra');

-- ── Transactions ─────────────────────────────────────────────────────────
INSERT INTO {DEV_CATALOG}.{SCHEMA}.transactions VALUES
(3001, 2001, '2024-11-15 10:23:00', -85.50,    'Woolworths Sydney',    'RETAIL',        'CLEAR',     false, 'AU'),
(3002, 2001, '2024-11-15 14:10:00', -250.00,   'Qantas Airways',       'RETAIL',        'CLEAR',     false, 'AU'),
(3003, 2002, '2024-11-14 09:00:00', -15000.00, 'ANZ Bank Transfer',    'TRANSFER',      'REVIEW',    true,  'NZ'),
(3004, 2003, '2024-11-15 16:45:00', -42.80,    'Coles Melbourne',      'RETAIL',        'CLEAR',     false, 'AU'),
(3005, 2004, '2024-11-15 08:30:00', 5200.00,   'Salary Deposit',       'TRANSFER',      'CLEAR',     false, 'AU'),
(3006, 2005, '2024-11-13 11:00:00', -50000.00, 'Crypto Exchange Ltd',  'TRANSFER',      'HIGH_RISK', true,  'SG'),
(3007, 2006, '2024-11-15 00:00:00', -2450.00,  'Home Loan Repayment',  'TRANSFER',      'CLEAR',     false, 'AU'),
(3008, 2007, '2024-11-14 20:15:00', -180.00,   'Harvey Norman Perth',  'RETAIL',        'CLEAR',     false, 'AU'),
(3009, 2001, '2024-11-12 03:00:00', -8500.00,  'Offshore Holdings BVI','TRANSFER',      'HIGH_RISK', true,  'VG'),
(3010, 2008, '2024-11-15 12:00:00', -55.00,    'Uber Eats Hobart',     'RETAIL',        'CLEAR',     false, 'AU'),
(3011, 2009, '2024-11-15 07:30:00', -320.00,   'Dan Murphy Darwin',    'RETAIL',        'CLEAR',     false, 'AU'),
(3012, 2010, '2024-11-15 13:20:00', -125.00,   'JB Hi-Fi Canberra',   'RETAIL',        'CLEAR',     false, 'AU'),
(3013, 2003, '2024-11-10 22:00:00', -25000.00, 'Wire to Unknown',      'TRANSFER',      'BLOCKED',   true,  'MM'),
(3014, 2001, '2024-11-15 15:00:00', -200.00,   'ATM Withdrawal',       'ATM',           'CLEAR',     false, 'AU'),
(3015, 2002, '2024-11-14 06:00:00', -3200.00,  'SWIFT Transfer',       'INTERNATIONAL', 'REVIEW',    true,  'HK');

-- ── Credit Cards ─────────────────────────────────────────────────────────
INSERT INTO {DEV_CATALOG}.{SCHEMA}.credit_cards VALUES
(4001, 1001, '4000 1234 5678 9010', '123', '12/26', 15000.00, 'VISA',       'ACTIVE'),
(4002, 1002, '5100 2345 6789 0121', '456', '03/27', 20000.00, 'MASTERCARD', 'ACTIVE'),
(4003, 1003, '3700 345 678 901',    '7890','06/25', 10000.00, 'AMEX',       'ACTIVE'),
(4004, 1004, '4000 4567 8901 2343', '234', '09/26', 25000.00, 'VISA',       'ACTIVE'),
(4005, 1005, '5100 5678 9012 3454', '567', '01/28', 12000.00, 'MASTERCARD', 'ACTIVE'),
(4006, 1006, '4000 6789 0123 4565', '890', '11/25', 8000.00,  'VISA',       'EXPIRED'),
(4007, 1007, '5100 7890 1234 5676', '012', '07/27', 30000.00, 'MASTERCARD', 'ACTIVE'),
(4008, 1008, '4000 8901 2345 6787', '345', '04/26', 18000.00, 'VISA',       'ACTIVE'),
(4009, 1009, '3700 901 234 567',    '6789','08/25', 50000.00, 'AMEX',       'BLOCKED'),
(4010, 1010, '4000 0123 4567 8909', '678', '02/28', 10000.00, 'VISA',       'ACTIVE');
"""

# Prod catalog gets same schema but no data
PROD_SETUP_SQL = f"""
CREATE OR REPLACE TABLE {PROD_CATALOG}.{SCHEMA}.customers AS SELECT * FROM {DEV_CATALOG}.{SCHEMA}.customers WHERE 1=0;
CREATE OR REPLACE TABLE {PROD_CATALOG}.{SCHEMA}.accounts AS SELECT * FROM {DEV_CATALOG}.{SCHEMA}.accounts WHERE 1=0;
CREATE OR REPLACE TABLE {PROD_CATALOG}.{SCHEMA}.transactions AS SELECT * FROM {DEV_CATALOG}.{SCHEMA}.transactions WHERE 1=0;
CREATE OR REPLACE TABLE {PROD_CATALOG}.{SCHEMA}.credit_cards AS SELECT * FROM {DEV_CATALOG}.{SCHEMA}.credit_cards WHERE 1=0;
"""


# ---------------------------------------------------------------------------
# Helpers (reused from provision_test_env.py and setup_test_data.py)
# ---------------------------------------------------------------------------

def _step(msg: str) -> None:
    print(f"\n\033[36m──\033[0m {msg}")


def _green(s: str) -> str:
    return f"\033[32m{s}\033[0m"


def _red(s: str) -> str:
    return f"\033[31m{s}\033[0m"


def _yellow(s: str) -> str:
    return f"\033[33m{s}\033[0m"


def _load_env_file(path: Path) -> dict[str, str]:
    """Parse a KEY=VALUE env file (ignoring comments and blank lines)."""
    cfg: dict[str, str] = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                cfg[k.strip()] = v.strip()
    return cfg


def _save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


def _load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_provision(env_file: Path) -> None:
    """Provision dev + prod workspaces (shared metastore), create tables, create Genie Space."""
    cfg = _load_env_file(env_file)
    cloud = _default_cloud

    print("=" * 64)
    print("  Australian Bank Demo — Setup")
    print("=" * 64)
    print(f"  Cloud: {cloud}")
    print(f"  Credentials: {env_file}")
    print()

    # Phase 1: Provision dev workspace + metastore + storage via provision_test_env.py
    _step("Provisioning dev workspace + shared metastore (this takes ~3-5 minutes)...")
    dev_state_file = SCRIPT_DIR / f".demo_dev_state.{cloud}.json"
    result = subprocess.run(
        [sys.executable, str(SCRIPTS_DIR / "provision_test_env.py"),
         "provision", "--force", "--env-file", str(env_file)],
        cwd=str(CLOUD_ROOT),
        env={
            **os.environ,
            "CLOUD_PROVIDER": cloud,
            "CLOUD_ROOT": str(CLOUD_ROOT),
            "_PARALLEL_STATE_FILE": str(dev_state_file),
        },
        text=True,
    )
    if result.returncode != 0:
        print(f"\n  {_red('ERROR')} Dev workspace provisioning failed (exit {result.returncode})")
        sys.exit(1)

    dev_state = json.loads(dev_state_file.read_text())
    dev_host = dev_state.get("workspace_host", "")
    dev_ws_id = dev_state.get("workspace_id", "")
    run_id = dev_state.get("run_id", "")
    metastore_id = dev_state.get("metastore_id", "")
    print(f"  {_green('✓')} Dev workspace ready: {dev_host}")
    print(f"  {_green('✓')} Shared metastore: {metastore_id}")

    # Phase 2: Create prod workspace and assign it to the SAME metastore
    _step("Creating prod workspace (sharing the same metastore)...")
    prod_host, prod_ws_id = _create_prod_workspace(cfg, cloud, metastore_id, dev_state)

    # Phase 3: Create tables via SDK (both dev data + prod empty schema)
    _step("Creating Australian banking tables in dev workspace...")
    warehouse_id = _create_tables_via_sdk(dev_state)

    # Create prod catalog (same schema, no data) — uses same metastore
    _step("Creating prod catalog (empty schema for promotion)...")
    _create_prod_catalog_via_sdk(dev_state)  # same workspace client — shared metastore

    # Phase 4: Create Genie Space
    _step("Creating Genie Space 'Kookaburra Bank Analytics'...")
    genie_space_id = _create_genie_space(dev_state)

    # Save state
    state = {
        "cloud": cloud,
        "run_id": run_id,
        "metastore_id": metastore_id,
        "dev": {
            "workspace_host": dev_host,
            "workspace_id": dev_ws_id,
            "state_file": str(dev_state_file),
            "envs_dir": dev_state.get("test_envs_dir", ""),
        },
        "prod": {
            "workspace_host": prod_host,
            "workspace_id": prod_ws_id,
        },
        "genie_space_id": genie_space_id,
        "warehouse_id": warehouse_id,
        "dev_catalog": DEV_CATALOG,
        "prod_catalog": PROD_CATALOG,
    }
    _save_state(state)

    # Auto-configure envs/dev/ so the user can go straight to `make generate`
    _step("Configuring envs/dev/ for the demo...")
    test_envs = Path(dev_state.get("test_envs_dir", ""))
    import shutil

    # Clean stale Terraform state from previous runs, then run make setup
    for env_name in ["dev", "prod"]:
        env_dir = CLOUD_ROOT / "envs" / env_name
        for stale in env_dir.rglob("terraform.tfstate*"):
            stale.unlink()
        for stale in env_dir.rglob(".terraform"):
            shutil.rmtree(stale, ignore_errors=True)
        account_dir = CLOUD_ROOT / "envs" / "account"
        for stale in account_dir.rglob("terraform.tfstate*"):
            stale.unlink()
        for stale in account_dir.rglob(".terraform"):
            shutil.rmtree(stale, ignore_errors=True)
        subprocess.run(
            ["make", "--no-print-directory", "setup", f"ENV={env_name}"],
            cwd=str(CLOUD_ROOT), capture_output=True, text=True,
        )

    # Copy auth credentials for dev (uses dev workspace host)
    for src, dst in [
        (test_envs / "dev" / "auth.auto.tfvars", CLOUD_ROOT / "envs" / "dev" / "auth.auto.tfvars"),
        (test_envs / "account" / "auth.auto.tfvars", CLOUD_ROOT / "envs" / "account" / "auth.auto.tfvars"),
    ]:
        if src.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)

    # Create prod auth with prod workspace host
    # (same SP credentials, different workspace)
    dev_auth = CLOUD_ROOT / "envs" / "dev" / "auth.auto.tfvars"
    prod_auth = CLOUD_ROOT / "envs" / "prod" / "auth.auto.tfvars"
    if dev_auth.exists():
        prod_auth.parent.mkdir(parents=True, exist_ok=True)
        auth_text = dev_auth.read_text()
        # Replace dev workspace host/id with prod
        import re
        auth_text = re.sub(
            r'databricks_workspace_host\s*=\s*"[^"]*"',
            f'databricks_workspace_host = "{prod_host}"',
            auth_text,
        )
        auth_text = re.sub(
            r'databricks_workspace_id\s*=\s*"[^"]*"',
            f'databricks_workspace_id = "{prod_ws_id}"',
            auth_text,
        )
        prod_auth.write_text(auth_text)

    # Write env.auto.tfvars with just genie_space_id.
    # Tables are auto-discovered from the Genie Space API
    # (via include_serialized_space=true query parameter).
    env_tfvars = CLOUD_ROOT / "envs" / "dev" / "env.auto.tfvars"
    tables = [
        f"{DEV_CATALOG}.{SCHEMA}.customers",
        f"{DEV_CATALOG}.{SCHEMA}.accounts",
        f"{DEV_CATALOG}.{SCHEMA}.transactions",
        f"{DEV_CATALOG}.{SCHEMA}.credit_cards",
    ]
    tables_hcl = "\n".join(f'      "{t}",' for t in tables)
    if genie_space_id:
        env_tfvars.write_text(f"""\
genie_spaces = [
  {{
    genie_space_id = "{genie_space_id}"
    uc_tables = [
{tables_hcl}
    ]
  }},
]

sql_warehouse_id = "{warehouse_id}"
""")
    else:
        # Genie Space creation failed — write tables so user can create Space manually
        env_tfvars.write_text(f"""\
uc_tables = [
  "{DEV_CATALOG}.{SCHEMA}.customers",
  "{DEV_CATALOG}.{SCHEMA}.accounts",
  "{DEV_CATALOG}.{SCHEMA}.transactions",
  "{DEV_CATALOG}.{SCHEMA}.credit_cards",
]

# Create a Genie Space manually in the UI, then paste the ID:
# genie_spaces = [
#   {{
#     genie_space_id = ""
#   }},
# ]
""")
    print(f"  {_green('✓')} envs/dev/ configured (auth + env.auto.tfvars)")

    # Print summary
    print()
    print("=" * 64)
    print("  Demo Environment Ready")
    print("=" * 64)
    print(f"  Dev workspace:   {dev_host}")
    print(f"  Prod workspace:  {prod_host}")
    print(f"  Genie Space ID:  {genie_space_id}")
    print(f"  Dev catalog:     {DEV_CATALOG}")
    print(f"  Prod catalog:    {PROD_CATALOG}")
    print(f"  State file:      {STATE_FILE}")
    print()
    print("  Next steps:")
    print("    1. Run: make generate ENV=dev COUNTRY=ANZ INDUSTRY=financial_services")
    print("    2. Follow ../shared/examples/aus_bank_demo/README.md for the demo")
    print()


def _create_tables_via_sdk(dev_state: dict) -> str:
    """Create tables directly via Databricks SDK. Returns warehouse ID."""
    import hcl2 as _hcl2

    auth_file = Path(dev_state.get("test_envs_dir", "")) / "dev" / "auth.auto.tfvars"
    with open(auth_file) as f:
        cfg = _hcl2.load(f)

    _s = lambda v: (v[0] if isinstance(v, list) else (v or "")).strip()
    host = _s(cfg.get("databricks_workspace_host", ""))
    client_id = _s(cfg.get("databricks_client_id", ""))
    client_secret = _s(cfg.get("databricks_client_secret", ""))

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    w = WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)

    # Find or create warehouse
    wh_id = ""
    for wh in w.warehouses.list():
        if wh.id:
            wh_id = wh.id
            break
    if not wh_id:
        wh = w.warehouses.create(
            name="Demo Warehouse",
            cluster_size="2X-Small",
            warehouse_type="PRO",
            auto_stop_mins=15,
            enable_serverless_compute=True,
        ).result()
        wh_id = wh.id

    # Ensure catalogs exist
    catalog_storage_base = _s(cfg.get("catalog_storage_base", ""))
    for catalog_name in [DEV_CATALOG, PROD_CATALOG]:
        try:
            storage = f"{catalog_storage_base.rstrip('/')}/{catalog_name}" if catalog_storage_base else None
            w.catalogs.create(name=catalog_name, comment=f"Australian bank demo — {catalog_name}",
                              storage_root=storage)
        except Exception:
            pass  # already exists
        try:
            w.schemas.create(name=SCHEMA, catalog_name=catalog_name)
        except Exception:
            pass  # already exists

    # Wait for catalog/schema to propagate before running DDL
    print(f"  Waiting for catalog {DEV_CATALOG}.{SCHEMA} to propagate...")
    for _wait in range(12):
        try:
            r = w.statement_execution.execute_statement(
                warehouse_id=wh_id,
                statement=f"DESCRIBE SCHEMA {DEV_CATALOG}.{SCHEMA}",
                wait_timeout="30s",
            )
            if r.status and r.status.state.value == "SUCCEEDED":
                break
        except Exception:
            pass
        time.sleep(5)

    # Run SQL — all statements use fully qualified names (catalog.schema.table)
    all_sql = SETUP_SQL + "\n" + SAMPLE_DATA_SQL
    stmts = []
    for raw in all_sql.split(";"):
        # Strip leading comment lines (keep SQL that follows comments)
        lines = [l for l in raw.strip().splitlines() if l.strip() and not l.strip().startswith("--")]
        cleaned = "\n".join(lines).strip()
        if cleaned:
            stmts.append(cleaned)
    for stmt in stmts:
        r = w.statement_execution.execute_statement(
            warehouse_id=wh_id, statement=stmt, wait_timeout="50s",
        )
        max_wait = 120
        start = time.time()
        while True:
            state = r.status.state
            if state == StatementState.SUCCEEDED:
                break
            if state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
                err = r.status.error
                print(f"  {_yellow('WARN')} SQL failed: {err}")
                break
            if time.time() - start > max_wait:
                print(f"  {_yellow('WARN')} SQL timed out")
                break
            time.sleep(2)
            r = w.statement_execution.get_statement(r.statement_id)

    print(f"  {_green('✓')} Tables created")
    return wh_id


def _create_prod_workspace(cfg: dict, cloud: str, metastore_id: str, dev_state: dict) -> tuple[str, str]:
    """Create a second workspace and assign it to the shared metastore.

    Returns (prod_host, prod_workspace_id).
    """
    from databricks.sdk import AccountClient

    account_host = "https://accounts.azuredatabricks.net" if cloud == "azure" else "https://accounts.cloud.databricks.com"
    a = AccountClient(
        host=account_host,
        account_id=cfg.get("DATABRICKS_ACCOUNT_ID", ""),
        client_id=cfg.get("DATABRICKS_CLIENT_ID", ""),
        client_secret=cfg.get("DATABRICKS_CLIENT_SECRET", ""),
    )

    import secrets
    prod_ws_name = f"genie-demo-prod-{secrets.token_hex(5)}"
    region = dev_state.get("region", cfg.get("DATABRICKS_AWS_REGION",
                           cfg.get("AZURE_REGION", "ap-southeast-2")))

    # Build workspace creation kwargs per cloud
    if cloud == "aws":
        ws_kwargs = {"aws_region": region}
    else:
        ws_kwargs = {
            "location": region,
            "managed_resource_group_id": (
                f"/subscriptions/{cfg.get('AZURE_SUBSCRIPTION_ID', '')}"
                f"/resourceGroups/{prod_ws_name}-managed"
            ),
        }

    print(f"  Creating workspace: {prod_ws_name} in {region}...")
    try:
        from databricks.sdk.service.provisioning import (
            CustomerFacingComputeMode,
            PricingTier,
        )
        ws = a.workspaces.create_and_wait(
            workspace_name=prod_ws_name,
            pricing_tier=PricingTier.ENTERPRISE,
            compute_mode=CustomerFacingComputeMode.SERVERLESS,
            **ws_kwargs,
        )
    except (ImportError, TypeError):
        # Fallback for older SDK versions without compute_mode
        ws = a.workspaces.create(workspace_name=prod_ws_name, **ws_kwargs).result()

    prod_host = (f"https://{ws.deployment_name}.cloud.databricks.com"
                 if cloud == "aws" else (ws.workspace_url or ""))
    prod_ws_id = str(ws.workspace_id)
    print(f"  {_green('✓')} Prod workspace created: {prod_host}")

    # Assign shared metastore
    print(f"  Assigning shared metastore {metastore_id}...")
    try:
        a.metastore_assignments.create(
            workspace_id=int(prod_ws_id),
            metastore_id=metastore_id,
        )
        print(f"  {_green('✓')} Metastore assigned to prod workspace")
    except Exception as e:
        print(f"  {_yellow('WARN')} Metastore assignment: {e}")

    # Grant SP workspace admin on prod
    sp_id = cfg.get("DATABRICKS_CLIENT_ID", "")
    if sp_id:
        try:
            time.sleep(15)  # wait for workspace identity propagation
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient(host=prod_host, client_id=sp_id,
                                client_secret=cfg.get("DATABRICKS_CLIENT_SECRET", ""))
            # Verify connectivity
            me = w.current_user.me()
            print(f"  {_green('✓')} SP authenticated on prod workspace as {me.user_name}")
        except Exception as e:
            print(f"  {_yellow('WARN')} Prod workspace auth: {e}")

    return prod_host, prod_ws_id


def _create_prod_catalog_via_sdk(dev_state: dict) -> None:
    """Create prod catalog in the shared metastore (via dev workspace client)."""
    import hcl2 as _hcl2

    auth_file = Path(dev_state.get("test_envs_dir", "")) / "dev" / "auth.auto.tfvars"
    if not Path(auth_file).exists():
        print(f"  {_yellow('WARN')} Auth file not found: {auth_file}")
        return
    with open(auth_file) as f:
        cfg = _hcl2.load(f)

    _s = lambda v: (v[0] if isinstance(v, list) else (v or "")).strip()
    host = _s(cfg.get("databricks_workspace_host", ""))
    client_id = _s(cfg.get("databricks_client_id", ""))
    client_secret = _s(cfg.get("databricks_client_secret", ""))
    catalog_storage_base = _s(cfg.get("catalog_storage_base", ""))

    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)

    storage = f"{catalog_storage_base.rstrip('/')}/{PROD_CATALOG}" if catalog_storage_base else None
    try:
        w.catalogs.create(name=PROD_CATALOG, comment="Australian bank demo — prod",
                          storage_root=storage)
    except Exception:
        pass  # already exists
    try:
        w.schemas.create(name=SCHEMA, catalog_name=PROD_CATALOG)
    except Exception:
        pass  # already exists

    # Create empty tables in prod catalog
    from databricks.sdk.service.sql import StatementState
    wh_id = ""
    for wh in w.warehouses.list():
        if wh.id:
            wh_id = wh.id
            break

    if wh_id:
        for stmt in [s.strip() for s in PROD_SETUP_SQL.split(";") if s.strip() and not s.strip().startswith("--")]:
            try:
                r = w.statement_execution.execute_statement(
                    warehouse_id=wh_id, statement=stmt, wait_timeout="50s")
                while r.status.state not in (StatementState.SUCCEEDED, StatementState.FAILED,
                                              StatementState.CANCELED, StatementState.CLOSED):
                    time.sleep(2)
                    r = w.statement_execution.get_statement(r.statement_id)
            except Exception:
                pass

    print(f"  {_green('✓')} Prod catalog {PROD_CATALOG} ready (empty tables)")


def _create_genie_space(dev_state: dict) -> str:
    """Create a Genie Space pointing at dev bank tables via REST API directly."""
    import hcl2 as _hcl2
    import urllib.request
    import urllib.error

    auth_file = Path(dev_state.get("test_envs_dir", "")) / "dev" / "auth.auto.tfvars"
    with open(auth_file) as f:
        cfg = _hcl2.load(f)

    _s = lambda v: (v[0] if isinstance(v, list) else (v or "")).strip()
    host = _s(cfg.get("databricks_workspace_host", "")).rstrip("/")
    client_id = _s(cfg.get("databricks_client_id", ""))
    client_secret = _s(cfg.get("databricks_client_secret", ""))

    # Get OAuth token
    import urllib.parse
    token_url = f"{host}/oidc/v1/token"
    token_data = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "scope": "all-apis",
    }).encode()
    token_req = urllib.request.Request(
        token_url, data=token_data, method="POST",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Basic " + __import__("base64").b64encode(
                f"{client_id}:{client_secret}".encode()
            ).decode(),
        },
    )
    import ssl
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with urllib.request.urlopen(token_req, context=ctx) as resp:
        token = json.loads(resp.read())["access_token"]

    # Find warehouse
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)
    wh_id = ""
    for wh in w.warehouses.list():
        if wh.id:
            wh_id = wh.id
            break

    # Build Genie Space payload
    tables = [
        f"{DEV_CATALOG}.{SCHEMA}.customers",
        f"{DEV_CATALOG}.{SCHEMA}.accounts",
        f"{DEV_CATALOG}.{SCHEMA}.transactions",
        f"{DEV_CATALOG}.{SCHEMA}.credit_cards",
    ]
    serialized_space = json.dumps({
        "version": 2,
        "data_sources": {
            "tables": [{"identifier": t} for t in sorted(tables)]
        },
    }, separators=(",", ":"))

    body = json.dumps({
        "warehouse_id": wh_id,
        "title": "Kookaburra Bank Analytics",
        "serialized_space": serialized_space,
    })

    # Create via REST API
    create_req = urllib.request.Request(
        f"{host}/api/2.0/genie/spaces",
        data=body.encode(),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(create_req, context=ctx) as resp:
            result = json.loads(resp.read())
            space_id = result.get("space_id", "")
            if not space_id:
                print(f"  {_yellow('WARN')} Created but no space_id in response: {result}")
                return ""
            print(f"  {_green('✓')} Genie Space created: {space_id}")

            # PATCH to persist tables — the POST create ignores serialized_space
            patch_req = urllib.request.Request(
                f"{host}/api/2.0/genie/spaces/{space_id}",
                data=json.dumps({"serialized_space": serialized_space}).encode(),
                method="PATCH",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
            )
            try:
                with urllib.request.urlopen(patch_req, context=ctx) as patch_resp:
                    print(f"  {_green('✓')} Tables added to Genie Space via PATCH")
            except Exception as pe:
                print(f"  {_yellow('WARN')} PATCH tables: {pe}")

            return space_id
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()[:300]
        print(f"  {_red('ERROR')} Genie Space creation failed (HTTP {e.code}): {error_body}")
        print(f"  The SP may need to be granted Genie Space permissions in the workspace.")
        print(f"  Try opening {host} in a browser and creating the Space manually.")
        return ""


def cmd_status() -> None:
    """Show current demo state."""
    state = _load_state()
    if not state:
        print("  No demo environment provisioned.")
        return
    print("=" * 64)
    print("  Australian Bank Demo — Status")
    print("=" * 64)
    print(f"  Cloud:          {state.get('cloud', '?')}")
    print(f"  Dev workspace:  {state.get('dev', {}).get('workspace_host', '?')}")
    print(f"  Prod workspace: {state.get('prod', {}).get('workspace_host', '?')}")
    print(f"  Genie Space ID: {state.get('genie_space_id', '?')}")
    print(f"  Dev catalog:    {state.get('dev_catalog', '?')}")
    print(f"  Prod catalog:   {state.get('prod_catalog', '?')}")
    print(f"  State file:     {STATE_FILE}")
    print("=" * 64)


def cmd_teardown(env_file: Path) -> None:
    """Tear down both workspaces and all cloud resources."""
    state = _load_state()
    if not state:
        print("  No demo environment to tear down.")
        return

    cloud = state.get("cloud", _default_cloud)
    cfg = _load_env_file(env_file)

    print("=" * 64)
    print("  Australian Bank Demo — Teardown")
    print("=" * 64)

    # Step 1: Delete prod workspace via Account API
    # (prod was created directly, not via provision_test_env.py)
    prod_ws_id = state.get("prod", {}).get("workspace_id", "")
    if prod_ws_id:
        _step("Deleting prod workspace...")
        try:
            from databricks.sdk import AccountClient
            account_host = "https://accounts.azuredatabricks.net" if cloud == "azure" else "https://accounts.cloud.databricks.com"
            a = AccountClient(
                host=account_host,
                account_id=cfg.get("DATABRICKS_ACCOUNT_ID", ""),
                client_id=cfg.get("DATABRICKS_CLIENT_ID", ""),
                client_secret=cfg.get("DATABRICKS_CLIENT_SECRET", ""),
            )
            a.workspaces.delete(workspace_id=int(prod_ws_id))
            print(f"  {_green('✓')} Prod workspace deleted")
        except Exception as e:
            print(f"  {_yellow('WARN')} Prod workspace deletion: {e}")

    # Step 2: Tear down dev workspace + metastore + storage via provision_test_env.py
    dev_state_file = state.get("dev", {}).get("state_file", "")
    if dev_state_file and Path(dev_state_file).exists():
        _step("Tearing down dev workspace + metastore + storage...")
        result = subprocess.run(
            [sys.executable, str(SCRIPTS_DIR / "provision_test_env.py"),
             "teardown", "--env-file", str(env_file)],
            cwd=str(CLOUD_ROOT),
            env={
                **os.environ,
                "CLOUD_PROVIDER": cloud,
                "CLOUD_ROOT": str(CLOUD_ROOT),
                "_PARALLEL_STATE_FILE": dev_state_file,
            },
            text=True,
        )
        if result.returncode == 0:
            print(f"  {_green('✓')} Dev workspace + metastore torn down")
        else:
            print(f"  {_yellow('WARN')} Dev teardown had errors (exit {result.returncode})")

    # Clean local state files
    for f in SCRIPT_DIR.glob(".demo_*"):
        f.unlink()
    if STATE_FILE.exists():
        STATE_FILE.unlink()

    print(f"\n  {_green('✓')} Teardown complete")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Australian Bank Demo — Setup and Teardown",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("command", choices=["provision", "status", "teardown"],
                       help="provision: create demo env | status: show state | teardown: destroy everything")
    parser.add_argument("--env-file", default=str(SCRIPTS_DIR / f"account-admin.{_default_cloud}.env"),
                       help="Path to account admin credentials env file")
    args = parser.parse_args()

    env_file = Path(args.env_file).resolve()

    if args.command == "provision":
        if not env_file.exists():
            print(f"ERROR: Credentials file not found: {env_file}")
            sys.exit(1)
        cmd_provision(env_file)
    elif args.command == "status":
        cmd_status()
    elif args.command == "teardown":
        cmd_teardown(env_file)


if __name__ == "__main__":
    main()
