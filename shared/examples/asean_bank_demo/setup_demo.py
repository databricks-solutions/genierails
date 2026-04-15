#!/usr/bin/env python3
"""
ASEAN Bank Demo — Setup and Teardown

Provisions a complete demo environment for the GenieRails champion flow:
  - Dev workspace + prod workspace (fresh, isolated)
  - Unity Catalog metastore with cloud storage
  - Sample ASEAN banking tables (customers, accounts, transactions, credit_cards)
  - An ungoverned Genie Space pointing at the dev tables

After setup, follow the README.md to run the demo.

Usage
-----
  # Provision everything (from the cloud wrapper directory: aws/ or azure/)
  python shared/examples/asean_bank_demo/setup_demo.py provision \
      --env-file shared/scripts/account-admin.aws.env

  # Check status
  python shared/examples/asean_bank_demo/setup_demo.py status

  # Tear down everything
  python shared/examples/asean_bank_demo/setup_demo.py teardown

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
DEV_CATALOG = "dev_asean_bank"
PROD_CATALOG = "prod_asean_bank"
SCHEMA = "retail"

SETUP_SQL = f"""
-- ── Customers ────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE {DEV_CATALOG}.{SCHEMA}.customers (
  customer_id     BIGINT    COMMENT 'Unique customer identifier',
  first_name      STRING    COMMENT 'Customer first name',
  last_name       STRING    COMMENT 'Customer last name',
  email           STRING    COMMENT 'Contact email address',
  phone           STRING    COMMENT 'Phone number (country-specific format)',
  address         STRING    COMMENT 'Residential address',
  city            STRING    COMMENT 'City',
  country         STRING    COMMENT 'Country code (SG, MY, TH, ID, PH, VN)',
  postal_code     STRING    COMMENT 'Postal/ZIP code',
  nric            STRING    COMMENT 'Singapore NRIC — National Registration Identity Card (9 chars, e.g. S8712345D)',
  mykad           STRING    COMMENT 'Malaysian MyKad — national IC number (12 digits, encodes date of birth)',
  thai_id         STRING    COMMENT 'Thai National ID — 13-digit citizen identification number',
  nik             STRING    COMMENT 'Indonesian NIK — Nomor Induk Kependudukan (16 digits, encodes date of birth and district)',
  philsys         STRING    COMMENT 'Philippine PhilSys national ID — 12-digit Philippine Identification System number',
  cccd            STRING    COMMENT 'Vietnamese CCCD — Can Cuoc Cong Dan citizen identity card (12 digits)',
  date_of_birth   DATE      COMMENT 'Date of birth'
)
USING delta
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

-- ── Accounts ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE {DEV_CATALOG}.{SCHEMA}.accounts (
  account_id      BIGINT       COMMENT 'Unique account identifier',
  customer_id     BIGINT       COMMENT 'FK to customers',
  account_number  STRING       COMMENT 'Bank account number',
  account_type    STRING       COMMENT 'SAVINGS, CURRENT, FIXED_DEPOSIT, MORTGAGE',
  currency        STRING       COMMENT 'Account currency (SGD, MYR, THB, IDR, PHP, VND)',
  balance         DECIMAL(18,2) COMMENT 'Current balance in account currency',
  opened_date     DATE         COMMENT 'Date account was opened',
  branch          STRING       COMMENT 'Branch name and country'
)
USING delta
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

-- ── Transactions ─────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE {DEV_CATALOG}.{SCHEMA}.transactions (
  transaction_id  BIGINT       COMMENT 'Unique transaction identifier',
  account_id      BIGINT       COMMENT 'FK to accounts',
  transaction_date TIMESTAMP   COMMENT 'Date and time of transaction',
  amount          DECIMAL(18,2) COMMENT 'Transaction amount in account currency',
  currency        STRING       COMMENT 'Transaction currency',
  merchant        STRING       COMMENT 'Merchant or payee name',
  category        STRING       COMMENT 'Transaction category (RETAIL, TRANSFER, ATM, REMITTANCE, CROSS_BORDER)',
  aml_risk_flag   STRING       COMMENT 'AML risk assessment: CLEAR, REVIEW, HIGH_RISK, BLOCKED',
  cross_border    BOOLEAN      COMMENT 'True if cross-border transaction',
  source_country  STRING       COMMENT 'Originating country code',
  dest_country    STRING       COMMENT 'Destination country code'
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
  credit_limit    DECIMAL(18,2) COMMENT 'Credit limit in SGD equivalent',
  currency        STRING       COMMENT 'Card billing currency',
  card_type       STRING       COMMENT 'VISA, MASTERCARD, UNIONPAY, JCB',
  status          STRING       COMMENT 'ACTIVE, BLOCKED, EXPIRED'
)
USING delta
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
"""

SAMPLE_DATA_SQL = f"""
-- ── Customers (realistic ASEAN data — 2 per country) ───────────────────
INSERT INTO {DEV_CATALOG}.{SCHEMA}.customers VALUES
(1001, 'Wei Liang',  'Tan',              'weiliang.tan@email.sg',        '+65 9123 4567',     '42 Orchard Road',         'Singapore',       'SG', '068912',  'S8712345D',       NULL,             NULL,              NULL,               NULL,             NULL,             '1987-03-14'),
(1002, 'Mei Ling',   'Wong',             'meiling.wong@email.sg',        '+65 8234 5678',     '15 Marina Boulevard',     'Singapore',       'SG', '238859',  'T0198765A',       NULL,             NULL,              NULL,               NULL,             NULL,             '2001-06-22'),
(1003, 'Ahmad',      'bin Ismail',       'ahmad.ismail@email.my',        '+60 12-345 6789',   '8 Jalan Bukit Bintang',   'Kuala Lumpur',    'MY', '50450',   NULL,              '850615085123',   NULL,              NULL,               NULL,             NULL,             '1985-06-15'),
(1004, 'Nurul Huda', 'binti Abdullah',   'nurul.huda@email.my',          '+60 13-456 7890',   '23 Gurney Drive',         'Penang',          'MY', '10050',   NULL,              '920304146234',   NULL,              NULL,               NULL,             NULL,             '1992-03-04'),
(1005, 'Somchai',    'Wongprasert',      'somchai.wong@email.th',        '+66 81 234 5678',   '5 Sukhumvit Soi 11',      'Bangkok',         'TH', '10110',   NULL,              NULL,             '1100112345678',   NULL,               NULL,             NULL,             '1980-01-12'),
(1006, 'Siriporn',   'Chaiyasit',        'siriporn.chai@email.th',       '+66 89 345 6789',   '12 Nimmanhaemin Road',    'Chiang Mai',      'TH', '50200',   NULL,              NULL,             '5340100567890',   NULL,               NULL,             NULL,             '1993-04-10'),
(1007, 'Budi',       'Santoso',          'budi.santoso@email.id',        '+62 812 3456 7890', '31 Jalan Sudirman',       'Jakarta',         'ID', '10110',   NULL,              NULL,             NULL,              '3201151290870001', NULL,             NULL,             '1990-12-15'),
(1008, 'Dewi',       'Kartika',          'dewi.kartika@email.id',        '+62 813 4567 8901', '7 Jalan Basuki Rahmat',   'Surabaya',        'ID', '60271',   NULL,              NULL,             NULL,              '3578064508950002', NULL,             NULL,             '1995-08-04'),
(1009, 'Juan',       'dela Cruz',        'juan.delacruz@email.ph',       '+63 917 123 4567',  '19 Ayala Avenue',         'Manila',          'PH', '1000',    NULL,              NULL,             NULL,              NULL,               '123456789012',   NULL,             '1988-09-20'),
(1010, 'Maria',      'Santos',           'maria.santos@email.ph',        '+63 918 234 5678',  '4 Osmena Boulevard',      'Cebu',            'PH', '6000',    NULL,              NULL,             NULL,              NULL,               '234567890123',   NULL,             '1975-02-14'),
(1011, 'Nguyen Van', 'Minh',             'nguyen.minh@email.vn',         '+84 90 123 4567',   '12 Nguyen Hue Street',    'Ho Chi Minh City','VN', '700000',  NULL,              NULL,             NULL,              NULL,               NULL,             '001085012345',   '1985-10-08'),
(1012, 'Tran Thi',   'Lan',              'tran.lan@email.vn',            '+84 91 234 5678',   '8 Hoan Kiem',             'Hanoi',           'VN', '100000',  NULL,              NULL,             NULL,              NULL,               NULL,             '024092045678',   '1992-05-16');

-- ── Accounts (1 per customer, multi-currency) ───────────────────────────
INSERT INTO {DEV_CATALOG}.{SCHEMA}.accounts VALUES
(2001, 1001, '0012345678', 'SAVINGS',       'SGD',   85000.00,   '2015-03-10', 'Singapore Marina Bay'),
(2002, 1002, '0023456789', 'CURRENT',       'SGD',   12500.50,   '2021-07-15', 'Singapore Orchard'),
(2003, 1003, '1034567890', 'CURRENT',       'MYR',  125000.00,   '2018-01-05', 'Kuala Lumpur KLCC'),
(2004, 1004, '1045678901', 'SAVINGS',       'MYR',   48000.75,   '2020-06-12', 'Penang Georgetown'),
(2005, 1005, '2056789012', 'FIXED_DEPOSIT', 'THB', 1500000.00,   '2019-09-20', 'Bangkok Silom'),
(2006, 1006, '2067890123', 'SAVINGS',       'THB',  320000.00,   '2022-04-08', 'Chiang Mai Old City'),
(2007, 1007, '3078901234', 'CURRENT',       'IDR', 250000000.00, '2017-11-30', 'Jakarta Sudirman'),
(2008, 1008, '3089012345', 'SAVINGS',       'IDR',  75000000.00, '2023-01-22', 'Surabaya Tunjungan'),
(2009, 1009, '4090123456', 'SAVINGS',       'PHP', 2500000.00,   '2016-08-15', 'Manila Makati'),
(2010, 1010, '4001234567', 'MORTGAGE',      'PHP',  -3500000.00, '2020-03-01', 'Cebu IT Park'),
(2011, 1011, '5012345678', 'CURRENT',       'VND', 850000000.00, '2019-05-10', 'Ho Chi Minh District 1'),
(2012, 1012, '5023456789', 'SAVINGS',       'VND', 150000000.00, '2021-12-03', 'Hanoi Hoan Kiem');

-- ── Transactions (15 transactions, cross-border ASEAN emphasis) ─────────
INSERT INTO {DEV_CATALOG}.{SCHEMA}.transactions VALUES
(3001, 2001, '2024-11-15 10:23:00', -350.00,     'SGD', 'DBS PayLah',           'RETAIL',       'CLEAR',     false, 'SG', 'SG'),
(3002, 2001, '2024-11-14 14:10:00', -5000.00,    'SGD', 'SWIFT to KL',          'CROSS_BORDER', 'CLEAR',     true,  'SG', 'MY'),
(3003, 2003, '2024-11-14 09:00:00', -2500.00,    'MYR', 'Grab Malaysia',        'RETAIL',       'CLEAR',     false, 'MY', 'MY'),
(3004, 2003, '2024-11-13 16:45:00', -15000.00,   'MYR', 'Remit to Jakarta',     'REMITTANCE',   'REVIEW',    true,  'MY', 'ID'),
(3005, 2005, '2024-11-15 08:30:00', -25000.00,   'THB', 'PromptPay Transfer',   'TRANSFER',     'CLEAR',     false, 'TH', 'TH'),
(3006, 2005, '2024-11-12 11:00:00', -180000.00,  'THB', 'Wire to Hanoi',        'CROSS_BORDER', 'HIGH_RISK', true,  'TH', 'VN'),
(3007, 2007, '2024-11-15 00:00:00', -5000000.00, 'IDR', 'Shopee Indonesia',     'RETAIL',       'CLEAR',     false, 'ID', 'ID'),
(3008, 2007, '2024-11-14 20:15:00', -85000000.00,'IDR', 'Offshore Holdings BVI','TRANSFER',     'HIGH_RISK', true,  'ID', 'VG'),
(3009, 2009, '2024-11-15 12:00:00', -15000.00,   'PHP', 'GCash',                'TRANSFER',     'CLEAR',     false, 'PH', 'PH'),
(3010, 2009, '2024-11-13 07:30:00', -50000.00,   'PHP', 'Remit to Singapore',   'REMITTANCE',   'CLEAR',     true,  'PH', 'SG'),
(3011, 2011, '2024-11-15 13:20:00', -2500000.00, 'VND', 'MoMo Payment',         'RETAIL',       'CLEAR',     false, 'VN', 'VN'),
(3012, 2011, '2024-11-12 06:00:00', -45000000.00,'VND', 'Wire to Bangkok',      'CROSS_BORDER', 'REVIEW',    true,  'VN', 'TH'),
(3013, 2002, '2024-11-10 22:00:00', -8000.00,    'SGD', 'Lazada Singapore',     'RETAIL',       'CLEAR',     false, 'SG', 'SG'),
(3014, 2004, '2024-11-15 15:00:00', -500.00,     'MYR', 'ATM Withdrawal',       'ATM',          'CLEAR',     false, 'MY', 'MY'),
(3015, 2008, '2024-11-11 03:00:00', -250000000.00,'IDR','Suspicious Wire',      'TRANSFER',     'BLOCKED',   true,  'ID', 'MM');

-- ── Credit Cards (12 cards, multi-currency) ──────────────────────────────
INSERT INTO {DEV_CATALOG}.{SCHEMA}.credit_cards VALUES
(4001, 1001, '4000 1234 5678 9010', '123', '12/26', 25000.00,  'SGD', 'VISA',       'ACTIVE'),
(4002, 1002, '5100 2345 6789 0121', '456', '03/27', 15000.00,  'SGD', 'MASTERCARD', 'ACTIVE'),
(4003, 1003, '4000 3456 7890 1232', '789', '06/25', 30000.00,  'MYR', 'VISA',       'EXPIRED'),
(4004, 1004, '3530 4567 8901 2343', '012', '09/27', 20000.00,  'MYR', 'JCB',        'ACTIVE'),
(4005, 1005, '6200 5678 9012 3454', '345', '01/28', 50000.00,  'THB', 'UNIONPAY',   'ACTIVE'),
(4006, 1006, '5100 6789 0123 4565', '678', '11/26', 35000.00,  'THB', 'MASTERCARD', 'ACTIVE'),
(4007, 1007, '4000 7890 1234 5676', '901', '07/27', 20000.00,  'IDR', 'VISA',       'ACTIVE'),
(4008, 1008, '5100 8901 2345 6787', '234', '04/26', 15000.00,  'IDR', 'MASTERCARD', 'BLOCKED'),
(4009, 1009, '4000 9012 3456 7898', '567', '08/27', 18000.00,  'PHP', 'VISA',       'ACTIVE'),
(4010, 1010, '3530 0123 4567 8909', '890', '02/28', 12000.00,  'PHP', 'JCB',        'ACTIVE'),
(4011, 1011, '6200 1234 5678 9011', '123', '05/27', 22000.00,  'VND', 'UNIONPAY',   'ACTIVE'),
(4012, 1012, '5100 2345 6789 0122', '456', '10/25', 10000.00,  'VND', 'MASTERCARD', 'EXPIRED');
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
    print("  ASEAN Bank Demo — Setup")
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
    _step("Creating ASEAN banking tables in dev workspace...")
    warehouse_id = _create_tables_via_sdk(dev_state)

    # Create prod catalog (same schema, no data) — uses same metastore
    _step("Creating prod catalog (empty schema for promotion)...")
    _create_prod_catalog_via_sdk(dev_state)  # same workspace client — shared metastore

    # Phase 4: Create Genie Space
    _step("Creating Genie Space 'ASEAN Regional Banking Analytics'...")
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
    tables_hcl = "\n".join(
        f'      "{DEV_CATALOG}.{SCHEMA}.{t}",' for t in ["customers", "accounts", "transactions", "credit_cards"]
    )
    if genie_space_id:
        env_tfvars.write_text(f"""\
genie_spaces = [
  {{
    genie_space_id = "{genie_space_id}"
    name           = "ASEAN Regional Banking Analytics"
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
    print("    1. Run: make generate ENV=dev COUNTRY=SEA INDUSTRY=financial_services")
    print("    2. Follow ../shared/examples/asean_bank_demo/README.md for the demo")
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
            w.catalogs.create(name=catalog_name, comment=f"ASEAN bank demo — {catalog_name}",
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

    # The SP that creates the workspace via AccountClient is automatically
    # workspace admin. Verify connectivity before continuing.
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
        w.catalogs.create(name=PROD_CATALOG, comment="ASEAN bank demo — prod",
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

    # Build Genie Space payload — pre-populate with sample questions, instructions,
    # benchmarks, SQL expressions, measures, filters, and join specs to simulate a
    # Genie Space that a user has already configured in the UI. The generate command
    # imports these verbatim from the API and does not regenerate them.
    _id_counter = [0]

    def _gen_id():
        """Generate a monotonically increasing UUID-like ID for serialized_space entries.

        The Genie API requires all ID-bearing lists (join_specs, sample_questions,
        benchmarks, etc.) to be sorted by id. Using a counter ensures natural
        insertion order is already sorted.
        """
        _id_counter[0] += 1
        # Use counter as the low bits to guarantee sort order
        hi = 0x0000000000001000  # minimal valid UUID v1 high bits
        lo = 0x8000000000000000 | _id_counter[0]
        return f"{hi:016x}{lo:016x}"

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
        "config": {
            "sample_questions": [
                {"id": _gen_id(), "question": [q]} for q in [
                    "How many customers do we have in each country?",
                    "What is the total balance by currency?",
                    "Show me all cross-border transactions between Singapore and Malaysia",
                    "List all customers with HIGH_RISK or BLOCKED AML flags",
                    "What are the top remittance corridors by transaction volume?",
                ]
            ],
        },
        "instructions": {
            "text_instructions": [{
                "id": _gen_id(),
                "content": [
                    "You are a banking analytics assistant for ASEAN Regional Bank, "
                    "headquartered in Singapore with operations across 6 ASEAN countries: "
                    "Singapore (SG), Malaysia (MY), Thailand (TH), Indonesia (ID), "
                    "Philippines (PH), and Vietnam (VN). The bank supports multiple "
                    "currencies: SGD (Singapore Dollar), MYR (Malaysian Ringgit), THB "
                    "(Thai Baht), IDR (Indonesian Rupiah), PHP (Philippine Peso), VND "
                    "(Vietnamese Dong). Customer identity columns are country-specific — "
                    "each customer has exactly ONE national ID populated based on their "
                    "country: nric (Singapore), mykad (Malaysia), thai_id (Thailand), "
                    "nik (Indonesia), philsys (Philippines), cccd (Vietnam). All other "
                    "ID columns are NULL. IMPORTANT: Do NOT aggregate balances or amounts "
                    "across different currencies without conversion. AML risk flags: "
                    "CLEAR (no concerns), REVIEW (under investigation), HIGH_RISK "
                    "(escalated), BLOCKED (frozen). Cross-border transactions between "
                    "ASEAN countries are common — use source_country and dest_country "
                    "for corridor analysis."
                ],
            }],
            "sql_snippets": {
                "filters": [
                    {"id": _gen_id(), "display_name": "Singapore customers only", "sql": ["country = 'SG'"]},
                    {"id": _gen_id(), "display_name": "Cross-border only", "sql": ["cross_border = true"]},
                    {"id": _gen_id(), "display_name": "Active cards only", "sql": ["status = 'ACTIVE'"]},
                ],
                "expressions": [
                    {"id": _gen_id(), "alias": "customer_full_name", "sql": ["first_name || ' ' || last_name"]},
                    {"id": _gen_id(), "alias": "remittance_corridor", "sql": ["source_country || ' -> ' || dest_country"]},
                    {"id": _gen_id(), "alias": "transaction_year_month", "sql": ["DATE_FORMAT(transaction_date, 'yyyy-MM')"]},
                ],
                "measures": [
                    {"id": _gen_id(), "alias": "total_balance", "sql": ["SUM(balance)"]},
                    {"id": _gen_id(), "alias": "avg_transaction_amount", "sql": ["AVG(ABS(amount))"]},
                    {"id": _gen_id(), "alias": "transaction_count", "sql": ["COUNT(DISTINCT transaction_id)"]},
                ],
            },
            # join_specs omitted — the Genie API's proto parser is strict about
            # the SQL format and rejects most join conditions. Genie infers joins
            # from foreign key relationships in the table schemas automatically.
        },
        "benchmarks": {
            "questions": [
                {
                    "id": _gen_id(),
                    "question": ["How many customers are in each country?"],
                    "answer": [{"format": "SQL", "content": [
                        f"SELECT country, COUNT(*) as customer_count FROM {DEV_CATALOG}.{SCHEMA}.customers GROUP BY country ORDER BY customer_count DESC"
                    ]}],
                },
                {
                    "id": _gen_id(),
                    "question": ["What is the total balance by currency?"],
                    "answer": [{"format": "SQL", "content": [
                        f"SELECT currency, SUM(balance) as total_balance FROM {DEV_CATALOG}.{SCHEMA}.accounts GROUP BY currency ORDER BY total_balance DESC"
                    ]}],
                },
                {
                    "id": _gen_id(),
                    "question": ["Show all HIGH_RISK or BLOCKED transactions"],
                    "answer": [{"format": "SQL", "content": [
                        f"SELECT t.*, c.first_name, c.last_name, c.country FROM {DEV_CATALOG}.{SCHEMA}.transactions t "
                        f"JOIN {DEV_CATALOG}.{SCHEMA}.accounts a ON t.account_id = a.account_id "
                        f"JOIN {DEV_CATALOG}.{SCHEMA}.customers c ON a.customer_id = c.customer_id "
                        f"WHERE t.aml_risk_flag IN ('HIGH_RISK', 'BLOCKED') ORDER BY t.transaction_date DESC"
                    ]}],
                },
            ],
        },
    }, separators=(",", ":"))

    body = json.dumps({
        "warehouse_id": wh_id,
        "title": "ASEAN Regional Banking Analytics",
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

            # PATCH to persist full config — the POST create ignores serialized_space.
            # This populates tables, sample questions, instructions, benchmarks,
            # SQL expressions, measures, filters, and join specs — simulating a
            # user who has configured the Space in the UI before running GenieRails.
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
                    print(f"  {_green('✓')} Genie Space configured (tables, instructions, benchmarks, SQL config)")
            except Exception as pe:
                print(f"  {_yellow('WARN')} PATCH config: {pe}")

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
    print("  ASEAN Bank Demo — Status")
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
    print("  ASEAN Bank Demo — Teardown")
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

    # Clean generated config in envs/ so the next provision starts fresh.
    # Remove auth, env, abac configs, terraform state, and generated/ dirs
    # for dev, prod, and account environments.
    import shutil
    for env_name in ["dev", "prod", "account"]:
        env_dir = CLOUD_ROOT / "envs" / env_name
        if not env_dir.exists():
            continue
        for pattern in [
            "auth.auto.tfvars",
            "env.auto.tfvars",
            "abac.auto.tfvars",
            "terraform.tfstate",
            "terraform.tfstate.backup",
            ".terraform",
            ".*.apply.sha",
        ]:
            for stale in env_dir.rglob(pattern):
                if stale.is_dir():
                    shutil.rmtree(stale, ignore_errors=True)
                else:
                    stale.unlink(missing_ok=True)
        # Remove generated/ and ddl/ dirs
        for subdir in ["generated", "ddl"]:
            d = env_dir / subdir
            if d.exists():
                shutil.rmtree(d, ignore_errors=True)
    print(f"  {_green('✓')} Cleaned envs/dev, envs/prod, envs/account")

    print(f"\n  {_green('✓')} Teardown complete")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="ASEAN Bank Demo — Setup and Teardown",
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
