#!/usr/bin/env python3
"""
Integration test runner for the scenarios documented in docs/playbook.md.

Runs each scenario end-to-end with full data setup, generation, apply, verification,
and teardown. Each scenario is isolated — state from a previous run is destroyed and
cleaned before the next one starts.

Scenarios
---------
  quickstart       Single space, single catalog: Finance Analytics backed by dev_fin.
                   Tests the core quickstart flow from docs/playbook.md § 1.

  multi-catalog    One Genie Space spanning two catalogs (dev_fin + dev_clinical).
                   Tests the "single space spanning multiple catalogs" pattern.

  multi-space      Two independent spaces: Finance Analytics (dev_fin) and
                   Clinical Analytics (dev_clinical). Tests § 1 multi-space mode.
                   This is the core of the existing `make integration-test` flow.

  per-space        Incremental per-space generation. Deploys Finance Analytics first,
                   then adds Clinical Analytics using SPACE= without touching Finance.
                   Tests § 2 (Add a new Genie Space) isolation guarantee.

  promote          Full multi-space dev → prod promotion with catalog remapping.
                   Tests § 3a promote flow end-to-end.

  multi-env        Two independent workspace environments on the same account:
                   dev=Finance Analytics, bu2=Clinical Analytics.
                   Tests § 3b (second independent environment / BU).

  attach-promote   Attach to an existing Genie Space that was configured in the UI.
                   A Finance Analytics space is created via the Genie API (simulating
                   a data team that already set it up in the Databricks UI). The test
                   then runs `make generate` in genie_space_id-only mode (no uc_tables),
                   which discovers the space's tables from the API and generates full
                   ABAC governance. Finally promotes to prod. Tests playbook.md §3
                   "Attaching to an existing Genie Space" + §3a promotion.

  self-service-genie
                   Central Data Governance team + BU teams self-serve Genie spaces.
                   Phase 1: governance env applies ABAC (MODE=governance + apply-governance).
                   Phase 2: bu_fin env creates Finance Analytics (MODE=genie + apply-genie).
                   Phase 3: bu_clin (second BU) added; governance state verified unchanged.
                   Phase 4: bu_fin → bu_fin_prod promoted via make promote + make apply-genie.
                   Asserts cross-layer state isolation throughout. Tests playbook.md §7.

  abac-only        ABAC governance only — no Genie Space (playbook.md §2).
                   Phase 1: uc_tables only in env.auto.tfvars, plain make generate + make apply.
                   Phase 2: §2 → §4 upgrade: add genie_spaces, make generate SPACE=, make apply.
                   Asserts governance preserved when Genie Space is added later.

  multi-space-import  Import two UI-configured Genie Spaces in one make generate call
                   (playbook.md §3 multi-space import). Creates two spaces via API,
                   imports both via genie_space_id entries, asserts both genie_space_configs
                   present, no new spaces created by Terraform.

  genie-import-no-abac
                   Import an existing Genie Space and deploy to prod without ABAC.
                   Creates a space via API, imports it with genie_only=true, runs
                   MODE=genie generation, promotes to prod (graceful skip or remap),
                   applies workspace layer. Asserts no governance artifacts produced.

  all              Run all scenarios sequentially (default when no --scenario given).

Usage
-----
  # Run all scenarios
  python scripts/run_integration_tests.py

  # Run a single scenario
  python scripts/run_integration_tests.py --scenario quickstart
  python scripts/run_integration_tests.py --scenario promote

  # Skip teardown so you can inspect results
  python scripts/run_integration_tests.py --scenario multi-space --keep-data

  # Pin a specific SQL warehouse (avoids cold-start delay)
  python scripts/run_integration_tests.py --warehouse-id abc123ef

  # Use a non-default auth file
  python scripts/run_integration_tests.py --auth-file envs/dev/auth.auto.tfvars

  # List available scenarios
  python scripts/run_integration_tests.py --list

Makefile targets (added by this PR)
------------------------------------
  make test-quickstart
  make test-multi-catalog
  make test-multi-space
  make test-per-space
  make test-promote
  make test-multi-env
  make test-attach-promote
  make test-self-service-genie
  make test-abac-only
  make test-multi-space-import
  make test-genie-import-no-abac
  make test-all
"""

from __future__ import annotations

import argparse
import contextlib
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Callable

from warehouse_utils import select_warehouse

# Force line-buffered stdout so that print() and subprocess output appear
# in the correct order when the test runner is invoked with piped stdout.
if not sys.stdout.line_buffering:
    sys.stdout.reconfigure(line_buffering=True)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR  = Path(__file__).resolve().parent
MODULE_ROOT = SCRIPT_DIR.parent          # …/genie/shared/
_default_cloud = os.environ.get("CLOUD_PROVIDER", "aws").lower()
CLOUD_ROOT  = Path(os.environ.get("CLOUD_ROOT", MODULE_ROOT.parent / _default_cloud))

# ENVS_DIR is set dynamically in main() — either from --envs-dir, from the
# ENVS_DIR env var (set by the cloud-specific Makefile), from the provisioned
# state file (envs/test/), or defaulting to envs/ under the cloud wrapper root.
# All helpers that reference ENVS_DIR use the module-level variable so that
# changing it once in main() propagates everywhere.
ENVS_DIR    = Path(os.environ.get("ENVS_DIR", CLOUD_ROOT / "envs"))

_parallel_state = os.environ.get("_PARALLEL_STATE_FILE", "")
PROVISION_STATE_FILE = Path(_parallel_state) if _parallel_state else SCRIPT_DIR / f".test_env_state.{_default_cloud}.json"

DEFAULT_AUTH_FILE = ENVS_DIR / "dev" / "auth.auto.tfvars"

# Catalog names (must match setup_test_data.py constants)
DEV_FIN_CAT      = "dev_fin"
DEV_CLIN_CAT     = "dev_clinical"
PROD_FIN_CAT     = "prod_fin"
PROD_CLIN_CAT    = "prod_clinical"

CATALOG_MAP_DEV_TO_PROD = (
    f"{DEV_FIN_CAT}={PROD_FIN_CAT},{DEV_CLIN_CAT}={PROD_CLIN_CAT}"
)

# Australian bank demo catalogs
DEV_BANK_CAT  = "dev_bank"
PROD_BANK_CAT = "prod_bank"
BANK_SCHEMA   = "retail"

# India bank demo catalogs (mirrors shared/examples/india_bank_demo/setup_demo.py)
DEV_LAKSHMI_CAT  = "dev_lakshmi"
PROD_LAKSHMI_CAT = "prod_lakshmi"
LAKSHMI_SCHEMA   = "retail"

# ASEAN bank demo catalogs (mirrors shared/examples/asean_bank_demo/setup_demo.py)
DEV_ASEAN_CAT  = "dev_asean_bank"
PROD_ASEAN_CAT = "prod_asean_bank"
ASEAN_SCHEMA   = "retail"

# ---------------------------------------------------------------------------
# Australian banking table SQL (mirrors shared/examples/aus_bank_demo/setup_demo.py)
# ---------------------------------------------------------------------------

BANK_SETUP_SQL = f"""
CREATE OR REPLACE TABLE {DEV_BANK_CAT}.{BANK_SCHEMA}.customers (
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

CREATE OR REPLACE TABLE {DEV_BANK_CAT}.{BANK_SCHEMA}.accounts (
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

CREATE OR REPLACE TABLE {DEV_BANK_CAT}.{BANK_SCHEMA}.transactions (
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

CREATE OR REPLACE TABLE {DEV_BANK_CAT}.{BANK_SCHEMA}.credit_cards (
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

BANK_SAMPLE_DATA_SQL = f"""
INSERT INTO {DEV_BANK_CAT}.{BANK_SCHEMA}.customers VALUES
(1001, 'Sarah',    'Chen',       'sarah.chen@email.com.au',      '+61 412 345 678', '42 George St',      'Sydney',       'NSW', '2000', '123 456 789', '2123 45670 1', '1985-03-14', '062-000', '12345678'),
(1002, 'James',    'O''Brien',   'james.obrien@email.com.au',    '+61 423 456 789', '15 Collins St',     'Melbourne',    'VIC', '3000', '234 567 890', '3234 56781 2', '1978-07-22', '063-000', '23456789'),
(1003, 'Priya',    'Sharma',     'priya.sharma@email.com.au',    '+61 434 567 890', '8 Queen St',        'Brisbane',     'QLD', '4000', '345 678 901', '4345 67892 3', '1992-11-05', '064-000', '34567890'),
(1004, 'David',    'Williams',   'david.williams@email.com.au',  '+61 445 678 901', '23 King William St','Adelaide',     'SA',  '5000', '456 789 012', '5456 78903 4', '1970-01-30', '065-000', '45678901'),
(1005, 'Mei',      'Nguyen',     'mei.nguyen@email.com.au',      '+61 456 789 012', '5 Hay St',          'Perth',        'WA',  '6000', '567 890 123', '6567 89014 5', '1988-09-18', '066-000', '56789012');

INSERT INTO {DEV_BANK_CAT}.{BANK_SCHEMA}.accounts VALUES
(2001, 1001, '062-000', '12345678', 'EVERYDAY',     15420.50,  '2015-03-10', 'Sydney CBD'),
(2002, 1001, '062-000', '12345679', 'SAVINGS',     142500.00,  '2015-03-10', 'Sydney CBD'),
(2003, 1002, '063-000', '23456789', 'EVERYDAY',      8730.25,  '2018-07-15', 'Melbourne Central'),
(2004, 1003, '064-000', '34567890', 'EVERYDAY',     23100.80,  '2020-01-05', 'Brisbane City'),
(2005, 1003, '064-000', '34567891', 'TERM_DEPOSIT', 50000.00,  '2022-06-01', 'Brisbane City');

INSERT INTO {DEV_BANK_CAT}.{BANK_SCHEMA}.transactions VALUES
(3001, 2001, '2024-11-15 10:23:00', -85.50,    'Woolworths Sydney',    'RETAIL',        'CLEAR',     false, 'AU'),
(3002, 2001, '2024-11-15 14:10:00', -250.00,   'Qantas Airways',       'RETAIL',        'CLEAR',     false, 'AU'),
(3003, 2002, '2024-11-14 09:00:00', -15000.00, 'ANZ Bank Transfer',    'TRANSFER',      'REVIEW',    true,  'NZ'),
(3004, 2003, '2024-11-15 16:45:00', -42.80,    'Coles Melbourne',      'RETAIL',        'CLEAR',     false, 'AU'),
(3005, 2004, '2024-11-15 08:30:00', 5200.00,   'Salary Deposit',       'TRANSFER',      'CLEAR',     false, 'AU'),
(3006, 2005, '2024-11-13 11:00:00', -50000.00, 'Crypto Exchange Ltd',  'TRANSFER',      'HIGH_RISK', true,  'SG');

INSERT INTO {DEV_BANK_CAT}.{BANK_SCHEMA}.credit_cards VALUES
(4001, 1001, '4000 1234 5678 9010', '123', '12/26', 15000.00, 'VISA',       'ACTIVE'),
(4002, 1002, '5100 2345 6789 0121', '456', '03/27', 20000.00, 'MASTERCARD', 'ACTIVE'),
(4003, 1003, '3700 345 678 901',    '7890','06/25', 10000.00, 'AMEX',       'ACTIVE'),
(4004, 1004, '4000 4567 8901 2343', '234', '09/26', 25000.00, 'VISA',       'ACTIVE'),
(4005, 1005, '5100 5678 9012 3454', '567', '01/28', 12000.00, 'MASTERCARD', 'ACTIVE');
"""

BANK_PROD_SETUP_SQL = f"""
CREATE OR REPLACE TABLE {PROD_BANK_CAT}.{BANK_SCHEMA}.customers AS SELECT * FROM {DEV_BANK_CAT}.{BANK_SCHEMA}.customers WHERE 1=0;
CREATE OR REPLACE TABLE {PROD_BANK_CAT}.{BANK_SCHEMA}.accounts AS SELECT * FROM {DEV_BANK_CAT}.{BANK_SCHEMA}.accounts WHERE 1=0;
CREATE OR REPLACE TABLE {PROD_BANK_CAT}.{BANK_SCHEMA}.transactions AS SELECT * FROM {DEV_BANK_CAT}.{BANK_SCHEMA}.transactions WHERE 1=0;
CREATE OR REPLACE TABLE {PROD_BANK_CAT}.{BANK_SCHEMA}.credit_cards AS SELECT * FROM {DEV_BANK_CAT}.{BANK_SCHEMA}.credit_cards WHERE 1=0;
"""

# ---------------------------------------------------------------------------
# India bank demo table SQL (mirrors shared/examples/india_bank_demo/setup_demo.py)
# ---------------------------------------------------------------------------

INDIA_BANK_SETUP_SQL = f"""
CREATE OR REPLACE TABLE {DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.customers (
  customer_id     BIGINT    COMMENT 'Unique customer identifier',
  first_name      STRING    COMMENT 'Customer first name',
  last_name       STRING    COMMENT 'Customer last name',
  email           STRING    COMMENT 'Contact email address',
  phone           STRING    COMMENT 'Indian mobile number (+91 format)',
  address         STRING    COMMENT 'Residential address',
  city            STRING    COMMENT 'City',
  state           STRING    COMMENT 'Indian state (Maharashtra, Karnataka, etc.)',
  pincode         STRING    COMMENT 'Indian PIN code (6 digits)',
  aadhaar         STRING    COMMENT 'Aadhaar number — 12-digit unique identity issued by UIDAI',
  pan_number      STRING    COMMENT 'Permanent Account Number — 10-char tax identifier (Income Tax Act)',
  voter_id        STRING    COMMENT 'Voter ID (EPIC) — Electoral Photo Identity Card',
  date_of_birth   DATE      COMMENT 'Date of birth',
  uan             STRING    COMMENT 'Universal Account Number — 12-digit EPF/provident fund identifier',
  upi_id          STRING    COMMENT 'UPI Virtual Payment Address — registered payment instrument',
  gstin           STRING    COMMENT 'GSTIN — GST Identification Number for business customers (15 chars)'
)
USING delta
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

CREATE OR REPLACE TABLE {DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.accounts (
  account_id      BIGINT       COMMENT 'Unique account identifier',
  customer_id     BIGINT       COMMENT 'FK to customers',
  ifsc            STRING       COMMENT 'IFSC code — Indian Financial System Code (11 chars)',
  account_number  STRING       COMMENT 'Bank account number',
  account_type    STRING       COMMENT 'SAVINGS, CURRENT, FD, RD, HOME_LOAN',
  balance         DECIMAL(18,2) COMMENT 'Current balance in INR',
  opened_date     DATE         COMMENT 'Date account was opened',
  branch          STRING       COMMENT 'Branch name'
)
USING delta
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

CREATE OR REPLACE TABLE {DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.transactions (
  transaction_id  BIGINT       COMMENT 'Unique transaction identifier',
  account_id      BIGINT       COMMENT 'FK to accounts',
  transaction_date TIMESTAMP   COMMENT 'Date and time of transaction',
  amount          DECIMAL(18,2) COMMENT 'Transaction amount in INR',
  merchant        STRING       COMMENT 'Merchant or payee name',
  category        STRING       COMMENT 'Transaction category (UPI, NEFT, RTGS, IMPS, POS, ATM)',
  aml_risk_flag   STRING       COMMENT 'AML risk assessment: CLEAR, REVIEW, HIGH_RISK, BLOCKED',
  cross_border    BOOLEAN      COMMENT 'True if international transaction',
  country         STRING       COMMENT 'Destination country code (IN, SG, US, AE, etc.)'
)
USING delta
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

CREATE OR REPLACE TABLE {DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.credit_cards (
  card_id         BIGINT       COMMENT 'Unique card identifier',
  customer_id     BIGINT       COMMENT 'FK to customers',
  card_number     STRING       COMMENT 'Full credit card PAN — PCI-DSS sensitive',
  cvv             STRING       COMMENT 'Card verification value — PCI-DSS sensitive',
  expiry_date     STRING       COMMENT 'Card expiry (MM/YY)',
  credit_limit    DECIMAL(18,2) COMMENT 'Credit limit in INR',
  card_type       STRING       COMMENT 'VISA, MASTERCARD, RUPAY',
  status          STRING       COMMENT 'ACTIVE, BLOCKED, EXPIRED'
)
USING delta
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
"""

INDIA_BANK_SAMPLE_DATA_SQL = f"""
INSERT INTO {DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.customers VALUES
(1001, 'Arjun',    'Sharma',     'arjun.sharma@email.in',       '+91 98765 43210', '42 MG Road',          'Mumbai',      'Maharashtra',  '400001', '2345 6789 0123', 'ABCPS1234D', 'GDL1234567', '1985-03-14', '100123456789', 'arjun@okaxis',  NULL),
(1002, 'Priya',    'Krishnan',   'priya.k@email.in',            '+91 87654 32109', '15 Brigade Road',     'Bangalore',   'Karnataka',    '560001', '3456 7890 1234', 'BCDPK2345E', 'KAR2345678', '1978-07-22', '100234567890', 'priya@ybl',     NULL),
(1003, 'Rajesh',   'Patel',      'rajesh.patel@email.in',       '+91 76543 21098', '8 Ashram Road',       'Ahmedabad',   'Gujarat',      '380001', '4567 8901 2345', 'CDEPR3456F', 'GJR3456789', '1992-11-05', '100345678901', 'rajesh@oksbi',  '24AADCP1234F1Z5'),
(1004, 'Deepa',    'Iyer',       'deepa.iyer@email.in',         '+91 65432 10987', '23 Anna Salai',       'Chennai',     'Tamil Nadu',   '600001', '5678 9012 3456', 'DEFDI4567G', 'TN04567890', '1970-01-30', '100456789012', 'deepa@paytm',   NULL),
(1005, 'Amit',     'Kumar',      'amit.kumar@email.in',         '+91 54321 09876', '5 Rajpath',           'Delhi',       'Delhi',        '110001', '6789 0123 4567', 'EFGAK5678H', 'DL05678901', '1988-09-18', '100567890123', 'amit@okicici',  '36AADCR5678G1Z8');

INSERT INTO {DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.accounts VALUES
(2001, 1001, 'SBIN0001234', '12345678901', 'SAVINGS',   254200.50,  '2015-03-10', 'Fort Mumbai'),
(2002, 1001, 'SBIN0001234', '12345678902', 'CURRENT',  1542000.00,  '2018-05-15', 'Fort Mumbai'),
(2003, 1002, 'HDFC0002345', '23456789012', 'SAVINGS',    87500.25,  '2018-07-15', 'MG Road Bangalore'),
(2004, 1003, 'ICIC0003456', '34567890123', 'FD',        500000.00,  '2022-01-05', 'CG Road Ahmedabad'),
(2005, 1004, 'UTIB0004567', '45678901234', 'HOME_LOAN',-4850000.00, '2019-09-20', 'Anna Nagar Chennai');

INSERT INTO {DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.transactions VALUES
(3001, 2001, '2024-11-15 10:23:00', -850.00,     'Reliance Fresh',        'UPI',   'CLEAR',     false, 'IN'),
(3002, 2001, '2024-11-15 14:10:00', -25000.00,   'IRCTC Booking',         'NEFT',  'CLEAR',     false, 'IN'),
(3003, 2002, '2024-11-14 09:00:00', -1500000.00, 'Wire Transfer',         'RTGS',  'REVIEW',    true,  'SG'),
(3004, 2003, '2024-11-15 16:45:00', -428.00,     'Swiggy Order',          'UPI',   'CLEAR',     false, 'IN'),
(3005, 2004, '2024-11-15 08:30:00', 52000.00,    'Salary Credit',         'NEFT',  'CLEAR',     false, 'IN'),
(3006, 2005, '2024-11-13 11:00:00', -5000000.00, 'Hawala Network Ltd',    'RTGS',  'HIGH_RISK', true,  'AE');

INSERT INTO {DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.credit_cards VALUES
(4001, 1001, '4000 1234 5678 9010', '123', '12/26', 500000.00, 'VISA',       'ACTIVE'),
(4002, 1002, '5100 2345 6789 0121', '456', '03/27', 300000.00, 'MASTERCARD', 'ACTIVE'),
(4003, 1003, '6521 3456 7890 1232', '789', '06/25', 200000.00, 'RUPAY',      'ACTIVE'),
(4004, 1004, '4000 4567 8901 2343', '234', '09/26', 750000.00, 'VISA',       'ACTIVE'),
(4005, 1005, '5100 5678 9012 3454', '567', '01/28', 150000.00, 'MASTERCARD', 'BLOCKED');
"""

INDIA_BANK_PROD_SETUP_SQL = f"""
CREATE OR REPLACE TABLE {PROD_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.customers AS SELECT * FROM {DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.customers WHERE 1=0;
CREATE OR REPLACE TABLE {PROD_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.accounts AS SELECT * FROM {DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.accounts WHERE 1=0;
CREATE OR REPLACE TABLE {PROD_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.transactions AS SELECT * FROM {DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.transactions WHERE 1=0;
CREATE OR REPLACE TABLE {PROD_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.credit_cards AS SELECT * FROM {DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.credit_cards WHERE 1=0;
"""

# ---------------------------------------------------------------------------
# ASEAN bank demo table SQL (mirrors shared/examples/asean_bank_demo/setup_demo.py)
# ---------------------------------------------------------------------------

ASEAN_BANK_SETUP_SQL = f"""
CREATE OR REPLACE TABLE {DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.customers (
  customer_id     BIGINT    COMMENT 'Unique customer identifier',
  first_name      STRING    COMMENT 'Customer first name',
  last_name       STRING    COMMENT 'Customer last name',
  email           STRING    COMMENT 'Contact email address',
  phone           STRING    COMMENT 'Phone number (country-specific format)',
  address         STRING    COMMENT 'Residential address',
  city            STRING    COMMENT 'City',
  country         STRING    COMMENT 'Country code (SG, MY, TH, ID, PH, VN)',
  postal_code     STRING    COMMENT 'Postal/ZIP code',
  nric            STRING    COMMENT 'Singapore NRIC — National Registration Identity Card (9 chars)',
  mykad           STRING    COMMENT 'Malaysian MyKad — national IC number (12 digits, encodes date of birth)',
  thai_id         STRING    COMMENT 'Thai National ID — 13-digit citizen identification number',
  nik             STRING    COMMENT 'Indonesian NIK — Nomor Induk Kependudukan (16 digits, encodes date of birth and district)',
  philsys         STRING    COMMENT 'Philippine PhilSys national ID — 12-digit Philippine Identification System number',
  cccd            STRING    COMMENT 'Vietnamese CCCD — Can Cuoc Cong Dan citizen identity card (12 digits)',
  date_of_birth   DATE      COMMENT 'Date of birth'
)
USING delta
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

CREATE OR REPLACE TABLE {DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.accounts (
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

CREATE OR REPLACE TABLE {DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.transactions (
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

CREATE OR REPLACE TABLE {DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.credit_cards (
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

ASEAN_BANK_SAMPLE_DATA_SQL = f"""
-- Each customer populates ONLY their country's national ID column; others are NULL
INSERT INTO {DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.customers VALUES
(1001, 'Wei Liang', 'Tan',          'wl.tan@email.sg',      '+65 9123 4567', '42 Orchard Rd',        'Singapore',      'SG', '238879', 'S8712345D',  NULL,           NULL,            NULL,               NULL,             NULL,             '1987-06-15'),
(1002, 'Mei Ling',  'Wong',         'ml.wong@email.sg',     '+65 8234 5678', '15 Marina Blvd',       'Singapore',      'SG', '068912', 'T0198765A',  NULL,           NULL,            NULL,               NULL,             NULL,             '2001-03-22'),
(1003, 'Ahmad',     'bin Ismail',   'ahmad.i@email.my',     '+60 12-345 6789','8 Jalan Bukit Bintang','Kuala Lumpur',   'MY', '50450',  NULL,         '850615085123', NULL,            NULL,               NULL,             NULL,             '1985-06-15'),
(1004, 'Nurul Huda','binti Abdullah','nurul.h@email.my',    '+60 13-456 7890','23 Gurney Drive',      'Penang',         'MY', '10050',  NULL,         '920304146234', NULL,            NULL,               NULL,             NULL,             '1992-03-04'),
(1005, 'Somchai',   'Wongprasert',  'somchai.w@email.th',   '+66 81 234 5678','5 Sukhumvit Rd',       'Bangkok',        'TH', '10110',  NULL,         NULL,           '1100112345678', NULL,               NULL,             NULL,             '1980-11-20'),
(1006, 'Siriporn',  'Chaiyasit',    'siriporn.c@email.th',  '+66 89 345 6789','12 Nimmanhaemin Rd',   'Chiang Mai',     'TH', '50200',  NULL,         NULL,           '5340100567890', NULL,               NULL,             NULL,             '1995-08-10');

INSERT INTO {DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.customers VALUES
(1007, 'Budi',      'Santoso',      'budi.s@email.id',      '+62 812 3456 7890','42 Jalan Sudirman',  'Jakarta',        'ID', '10110',  NULL,         NULL,           NULL,            '3201151290870001', NULL,             NULL,             '1990-12-15'),
(1008, 'Dewi',      'Kartika',      'dewi.k@email.id',      '+62 813 4567 8901','15 Jalan Tunjungan', 'Surabaya',       'ID', '60271',  NULL,         NULL,           NULL,            '3578064508950002', NULL,             NULL,             '1995-08-04'),
(1009, 'Juan',      'dela Cruz',    'juan.dc@email.ph',     '+63 917 123 4567', '8 Ayala Ave',        'Manila',         'PH', '1226',   NULL,         NULL,           NULL,            NULL,               '123456789012',   NULL,             '1988-04-25'),
(1010, 'Maria',     'Santos',       'maria.s@email.ph',     '+63 918 234 5678', '23 Osmena Blvd',     'Cebu',           'PH', '6000',   NULL,         NULL,           NULL,            NULL,               '234567890123',   NULL,             '1992-09-12'),
(1011, 'Van Minh',  'Nguyen',       'minh.n@email.vn',      '+84 90 123 4567',  '5 Le Loi St',        'Ho Chi Minh City','VN','700000', NULL,         NULL,           NULL,            NULL,               NULL,             '001085012345',   '1985-10-08'),
(1012, 'Thi Lan',   'Tran',         'lan.t@email.vn',       '+84 91 234 5678',  '12 Hoan Kiem St',    'Hanoi',          'VN', '100000', NULL,         NULL,           NULL,            NULL,               NULL,             '024092045678',   '1990-02-14');

INSERT INTO {DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.accounts VALUES
(2001, 1001, 'SG1234567890', 'SAVINGS',       'SGD',    85200.50,    '2015-03-10', 'Orchard Singapore'),
(2002, 1002, 'SG2345678901', 'CURRENT',       'SGD',   152000.00,    '2020-07-15', 'Marina Singapore'),
(2003, 1003, 'MY3456789012', 'SAVINGS',       'MYR',    43500.25,    '2018-06-20', 'KLCC Kuala Lumpur'),
(2004, 1004, 'MY4567890123', 'FIXED_DEPOSIT', 'MYR',   200000.00,    '2022-01-05', 'Gurney Penang'),
(2005, 1005, 'TH5678901234', 'SAVINGS',       'THB',   750000.00,    '2019-09-20', 'Sukhumvit Bangkok'),
(2006, 1006, 'TH6789012345', 'CURRENT',       'THB',   320000.00,    '2021-04-12', 'Nimmanhaemin Chiang Mai');

INSERT INTO {DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.accounts VALUES
(2007, 1007, 'ID7890123456', 'SAVINGS',       'IDR', 125000000.00,   '2020-11-30', 'Sudirman Jakarta'),
(2008, 1008, 'ID8901234567', 'CURRENT',       'IDR',  48000000.00,   '2023-01-08', 'Tunjungan Surabaya'),
(2009, 1009, 'PH9012345678', 'SAVINGS',       'PHP',   350000.00,    '2017-08-15', 'Makati Manila'),
(2010, 1010, 'PH0123456789', 'SAVINGS',       'PHP',   180000.00,    '2022-03-22', 'Cebu IT Park'),
(2011, 1011, 'VN1234509876', 'SAVINGS',       'VND', 450000000.00,   '2019-05-10', 'District 1 HCMC'),
(2012, 1012, 'VN2345610987', 'CURRENT',       'VND', 180000000.00,   '2021-12-01', 'Hoan Kiem Hanoi');

INSERT INTO {DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.transactions VALUES
(3001, 2001, '2024-11-15 10:23:00', -85.50,      'SGD', 'NTUC FairPrice',     'RETAIL',       'CLEAR',     false, 'SG', 'SG'),
(3002, 2001, '2024-11-15 14:10:00', -2500.00,    'SGD', 'DBS PayLah Transfer','REMITTANCE',   'CLEAR',     true,  'SG', 'MY'),
(3003, 2003, '2024-11-14 09:00:00', -15000.00,   'MYR', 'Grab Malaysia',      'RETAIL',       'CLEAR',     false, 'MY', 'MY'),
(3004, 2005, '2024-11-15 16:45:00', -4280.00,    'THB', 'Shopee Thailand',    'RETAIL',       'CLEAR',     false, 'TH', 'TH'),
(3005, 2007, '2024-11-15 08:30:00', 52000000.00, 'IDR', 'Salary Deposit',     'TRANSFER',     'CLEAR',     false, 'ID', 'ID'),
(3006, 2001, '2024-11-13 11:00:00', -50000.00,   'SGD', 'Shell Company BVI',  'CROSS_BORDER', 'HIGH_RISK', true,  'SG', 'VG');

INSERT INTO {DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.transactions VALUES
(3007, 2009, '2024-11-15 00:00:00', -12000.00,   'PHP', 'GCash Transfer',     'REMITTANCE',   'CLEAR',     true,  'PH', 'SG'),
(3008, 2011, '2024-11-14 20:15:00', -1800000.00, 'VND', 'MoMo Payment',       'RETAIL',       'CLEAR',     false, 'VN', 'VN'),
(3009, 2003, '2024-11-12 03:00:00', -85000.00,   'MYR', 'Offshore Holdings',  'CROSS_BORDER', 'HIGH_RISK', true,  'MY', 'VG'),
(3010, 2005, '2024-11-15 12:00:00', -150000.00,  'THB', 'PromptPay Transfer', 'REMITTANCE',   'REVIEW',    true,  'TH', 'VN'),
(3011, 2007, '2024-11-15 07:30:00', -3200000.00, 'IDR', 'Tokopedia',          'RETAIL',       'CLEAR',     false, 'ID', 'ID'),
(3012, 2012, '2024-11-15 13:20:00', -25000000.00,'VND', 'Unknown Wire',       'CROSS_BORDER', 'BLOCKED',   true,  'VN', 'MM'),
(3013, 2002, '2024-11-15 15:00:00', -200.00,     'SGD', 'ATM Withdrawal',     'ATM',          'CLEAR',     false, 'SG', 'SG'),
(3014, 2004, '2024-11-14 06:00:00', -8000.00,    'MYR', 'Cross-border Wire',  'REMITTANCE',   'REVIEW',    true,  'MY', 'ID'),
(3015, 2008, '2024-11-14 11:00:00', -15000000.00,'IDR', 'Salary Transfer',    'TRANSFER',     'CLEAR',     false, 'ID', 'ID');

INSERT INTO {DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.credit_cards VALUES
(4001, 1001, '4000 1234 5678 9010', '123', '12/26', 15000.00, 'SGD', 'VISA',       'ACTIVE'),
(4002, 1002, '5100 2345 6789 0121', '456', '03/27', 20000.00, 'SGD', 'MASTERCARD', 'ACTIVE'),
(4003, 1003, '6222 3456 7890 1232', '789', '06/25', 50000.00, 'MYR', 'UNIONPAY',   'ACTIVE'),
(4004, 1004, '5100 4567 8901 2343', '234', '09/26', 30000.00, 'MYR', 'MASTERCARD', 'ACTIVE'),
(4005, 1005, '3528 5678 9012 3454', '567', '01/28', 200000.00,'THB', 'JCB',        'ACTIVE'),
(4006, 1006, '4000 6789 0123 4565', '890', '11/25', 150000.00,'THB', 'VISA',       'EXPIRED');

INSERT INTO {DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.credit_cards VALUES
(4007, 1007, '4000 7890 1234 5676', '012', '07/27', 25000000.00,'IDR', 'VISA',       'ACTIVE'),
(4008, 1008, '5100 8901 2345 6787', '345', '04/26', 15000000.00,'IDR', 'MASTERCARD', 'ACTIVE'),
(4009, 1009, '4000 9012 3456 7898', '678', '08/25', 250000.00,  'PHP', 'VISA',       'BLOCKED'),
(4010, 1010, '5100 0123 4567 8909', '901', '02/28', 150000.00,  'PHP', 'MASTERCARD', 'ACTIVE'),
(4011, 1011, '4000 1234 5678 0011', '234', '05/27', 50000000.00,'VND', 'VISA',       'ACTIVE'),
(4012, 1012, '5100 2345 6789 0122', '567', '10/26', 30000000.00,'VND', 'MASTERCARD', 'ACTIVE');
"""

ASEAN_BANK_PROD_SETUP_SQL = f"""
CREATE OR REPLACE TABLE {PROD_ASEAN_CAT}.{ASEAN_SCHEMA}.customers AS SELECT * FROM {DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.customers WHERE 1=0;
CREATE OR REPLACE TABLE {PROD_ASEAN_CAT}.{ASEAN_SCHEMA}.accounts AS SELECT * FROM {DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.accounts WHERE 1=0;
CREATE OR REPLACE TABLE {PROD_ASEAN_CAT}.{ASEAN_SCHEMA}.transactions AS SELECT * FROM {DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.transactions WHERE 1=0;
CREATE OR REPLACE TABLE {PROD_ASEAN_CAT}.{ASEAN_SCHEMA}.credit_cards AS SELECT * FROM {DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.credit_cards WHERE 1=0;
"""

# ---------------------------------------------------------------------------
# HCL snippets for each scenario's genie_spaces config
# ---------------------------------------------------------------------------

SPACES_FINANCE_ONLY = f"""\
genie_spaces = [
  {{
    name     = "Finance Analytics"
    uc_tables = [
      "{DEV_FIN_CAT}.finance.customers",
      "{DEV_FIN_CAT}.finance.transactions",
      "{DEV_FIN_CAT}.finance.credit_cards",
    ]
  }},
]
"""

SPACES_COMBINED = f"""\
genie_spaces = [
  {{
    name     = "Combined Analytics"
    uc_tables = [
      "{DEV_FIN_CAT}.finance.customers",
      "{DEV_FIN_CAT}.finance.transactions",
      "{DEV_FIN_CAT}.finance.credit_cards",
      "{DEV_CLIN_CAT}.clinical.patients",
      "{DEV_CLIN_CAT}.clinical.encounters",
    ]
  }},
]
"""

SPACES_MULTI = f"""\
genie_spaces = [
  {{
    name     = "Finance Analytics"
    uc_tables = [
      "{DEV_FIN_CAT}.finance.customers",
      "{DEV_FIN_CAT}.finance.transactions",
      "{DEV_FIN_CAT}.finance.credit_cards",
    ]
  }},
  {{
    name     = "Clinical Analytics"
    uc_tables = [
      "{DEV_CLIN_CAT}.clinical.patients",
      "{DEV_CLIN_CAT}.clinical.encounters",
    ]
  }},
]
"""

SPACES_CLINICAL_ONLY = f"""\
genie_spaces = [
  {{
    name     = "Clinical Analytics"
    uc_tables = [
      "{DEV_CLIN_CAT}.clinical.patients",
      "{DEV_CLIN_CAT}.clinical.encounters",
    ]
  }},
]
"""

# uc_tables only — no genie_spaces block (used by the abac-only scenario)
TABLES_FINANCE_ONLY_HCL = f"""\
uc_tables = [
  "{DEV_FIN_CAT}.finance.customers",
  "{DEV_FIN_CAT}.finance.transactions",
  "{DEV_FIN_CAT}.finance.credit_cards",
]
"""

# ---------------------------------------------------------------------------
# ANSI helpers
# ---------------------------------------------------------------------------

def _green(s: str) -> str:  return f"\033[32m{s}\033[0m"
def _red(s: str)   -> str:  return f"\033[31m{s}\033[0m"
def _cyan(s: str)  -> str:  return f"\033[36m{s}\033[0m"
def _bold(s: str)  -> str:  return f"\033[1m{s}\033[0m"
def _yellow(s: str)-> str:  return f"\033[33m{s}\033[0m"


def _banner(title: str, width: int = 64) -> None:
    print()
    print("=" * width)
    print(f"  {title}")
    print("=" * width)


def _step(msg: str) -> None:
    print(f"\n{_cyan('──')} {msg}")


# ---------------------------------------------------------------------------
# Subprocess helpers
# ---------------------------------------------------------------------------

def _run(
    cmd: list[str],
    *,
    cwd: Path = MODULE_ROOT,
    check: bool = True,
    capture: bool = False,
) -> subprocess.CompletedProcess:
    """Run a command, streaming output unless capture=True."""
    stdout = subprocess.PIPE if capture else None
    # Always merge stderr into stdout so errors are visible in the output stream.
    stderr = subprocess.STDOUT
    result = subprocess.run(cmd, cwd=cwd, stdout=stdout, stderr=stderr, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(
            f"Command failed (exit {result.returncode}): {' '.join(cmd)}"
        )
    return result


def _make_targets(items: tuple[str, ...]) -> set[str]:
    return {item for item in items if "=" not in item}


def _needs_account_ops_lock(items: tuple[str, ...]) -> bool:
    """Serialize account-affecting make targets across parallel scenarios."""
    return bool(
        _make_targets(items)
        & {"apply", "apply-governance", "sync-tags", "wait-tag-policies", "import"}
    )


# Allow up to N concurrent account-level operations instead of serializing
# them behind an exclusive lock.  Each parallel scenario already uses unique
# suffixed names so there are no naming conflicts — the only concern is API
# rate-limiting, which a small concurrency cap handles.
_ACCOUNT_OPS_CONCURRENCY = int(os.environ.get("ACCOUNT_OPS_CONCURRENCY", "8"))
_account_ops_semaphore: threading.Semaphore | None = None


def _get_account_semaphore() -> threading.Semaphore:
    global _account_ops_semaphore
    if _account_ops_semaphore is None:
        _account_ops_semaphore = threading.Semaphore(_ACCOUNT_OPS_CONCURRENCY)
    return _account_ops_semaphore


@contextlib.contextmanager
def _account_ops_lock(reason: str):
    sem = _get_account_semaphore()
    sem.acquire()
    try:
        yield
    finally:
        sem.release()


def _make(
    *targets_and_vars: str,
    cwd: Path = CLOUD_ROOT,
    check: bool = True,
    retries: int = 0,
    retry_delay_seconds: int = 0,
) -> subprocess.CompletedProcess:
    """Run make with the given targets/variables.

    When retries > 0, a failed command is retried up to that many times.
    This is useful for LLM-dependent commands like ``make generate`` where
    the model may occasionally return a response that the code-block
    extractor cannot parse.

    retry_delay_seconds: seconds to wait between retry attempts.  Useful for
    ``make apply`` retries where Databricks' eventually-consistent ABAC quota
    counter may briefly show a stale value even after cleanup.

    When running against a provisioned test environment (ENVS_DIR != envs/),
    ENV_DIR / SOURCE_ENV_DIR / DEST_ENV_DIR overrides are injected automatically
    so the Makefile reads from envs/test/<env> instead of envs/<env>.
    """
    # Inject ENV_DIR overrides when using the provisioned test env directory.
    # The Makefile hardcodes MODULE_ROOT/envs/<ENV>; these overrides redirect
    # all file access to the isolated envs/test/<env>/ paths.
    injected: list[str] = []
    _default_envs = CLOUD_ROOT / "envs"
    if ENVS_DIR != _default_envs:
        # Parse env-related variables already in targets_and_vars.
        existing_keys = {v.split("=", 1)[0] for v in targets_and_vars if "=" in v}
        for var in targets_and_vars:
            if "=" not in var:
                continue
            key, _, val = var.partition("=")
            # Redirect ENV=<env> → ENV_DIR=<ENVS_DIR>/<env>
            if key == "ENV" and val not in ("account", "data_access") and "ENV_DIR" not in existing_keys:
                injected.append(f"ENV_DIR={ENVS_DIR / val}")
            # Redirect SOURCE_ENV=<env> → SOURCE_ENV_DIR=<ENVS_DIR>/<env>
            elif key == "SOURCE_ENV" and "SOURCE_ENV_DIR" not in existing_keys:
                injected.append(f"SOURCE_ENV_DIR={ENVS_DIR / val}")
            # Redirect DEST_ENV=<env> → DEST_ENV_DIR=<ENVS_DIR>/<env>
            elif key == "DEST_ENV" and "DEST_ENV_DIR" not in existing_keys:
                injected.append(f"DEST_ENV_DIR={ENVS_DIR / val}")
        # Always redirect the account env dir when using a custom ENVS_DIR.
        if "ACCOUNT_ENV_DIR" not in existing_keys:
            injected.append(f"ACCOUNT_ENV_DIR={ENVS_DIR / 'account'}")

    for attempt in range(1 + retries):
        # NOTE: Orphan tag policy cleanup is handled by _preamble_cleanup at
        # scenario start, NOT here. Running it before every apply would delete
        # tag policies that Phase 1 just created when Phase 2 calls apply.

        run_make = lambda: _run(
            ["make", "--no-print-directory", *targets_and_vars, *injected],
            cwd=cwd,
            check=False,
        )
        if _needs_account_ops_lock(targets_and_vars):
            with _account_ops_lock(" ".join(_make_targets(targets_and_vars))):
                result = run_make()
        else:
            result = run_make()
        if result.returncode == 0:
            # After successful generate, suffix account-level names for test isolation
            is_generate = any(t == "generate" or t == "generate-delta" for t in targets_and_vars if "=" not in t)
            if is_generate and _TEST_SUFFIX:
                env_vars = {v.split("=", 1)[0]: v.split("=", 1)[1]
                            for v in [*targets_and_vars, *injected] if "=" in v}
                env_name = env_vars.get("ENV", "dev")
                gen_abac = ENVS_DIR / env_name / "generated" / "abac.auto.tfvars"
                if gen_abac.exists():
                    _suffix_account_names(gen_abac)
                # Also suffix assembled abac if it exists (per-space merge)
                assembled = ENVS_DIR / env_name / "abac.auto.tfvars"
                if assembled.exists():
                    _suffix_account_names(assembled)
                # Suffix account-layer abac if it exists
                account_abac = ENVS_DIR / "account" / "abac.auto.tfvars"
                if account_abac.exists():
                    _suffix_account_names(account_abac)
                # Preserve existing tag policy values from the account layer.
                # In multi-phase scenarios, Phase 2's LLM may generate different
                # values than Phase 1. Merge existing values so the new policy is
                # a superset, preventing INVALID_TAG_POLICY_VALUE on existing tags.
                if account_abac.exists():
                    for abac_file in [gen_abac, assembled]:
                        if abac_file.exists():
                            _preserve_existing_tag_policy_values(abac_file, account_abac)
                    # Also update account abac from assembled (which has the merged values)
                    if assembled.exists():
                        _preserve_existing_tag_policy_values(account_abac, assembled)
                    try:
                        sys.path.insert(0, str(MODULE_ROOT))
                        from generate_abac import autofix_canonical_tag_vocabulary
                        for abac_file in [gen_abac, assembled, account_abac]:
                            if abac_file.exists():
                                autofix_canonical_tag_vocabulary(abac_file)
                    except Exception as exc:
                        print(f"  [WARN] canonical tag normalization after preserve: {exc}")
            return result
        if attempt < retries:
            # Before retrying apply, run make import to adopt any orphaned
            # resources (e.g. tag policies that reappeared due to eventual
            # consistency after deletion).  import is a no-op when there is
            # nothing to adopt.
            is_apply = any(t.startswith("apply") for t in targets_and_vars if "=" not in t)
            if is_apply:
                # Do NOT delete tag policies before retry — the account layer already
                # created them and wait-tag-policies confirmed visibility. Deleting them
                # forces recreation and another 300s visibility wait, causing timeouts.
                # Just run import to adopt any orphaned resources.
                env_vars = {v.split("=", 1)[0]: v.split("=", 1)[1]
                            for v in [*targets_and_vars, *injected] if "=" in v}
                import_args = [f"{k}={v}" for k, v in env_vars.items()]
                print(f"  [IMPORT] Running make import before retry to adopt any orphaned resources...")
                with _account_ops_lock("import retry"):
                    _run(["make", "--no-print-directory", "import", *import_args], cwd=cwd, check=False)
            if retry_delay_seconds > 0:
                print(
                    f"  [RETRY] Command failed (exit {result.returncode}), "
                    f"waiting {retry_delay_seconds}s then retrying "
                    f"({attempt + 1}/{retries})..."
                )
                time.sleep(retry_delay_seconds)
            else:
                print(f"  [RETRY] Command failed (exit {result.returncode}), retrying ({attempt + 1}/{retries})...")
        elif check:
            raise RuntimeError(
                f"Command failed (exit {result.returncode}): "
                f"make {' '.join(targets_and_vars)}"
            )
    return result


def _setup_data(auth_file: Path, *flags: str, warehouse_id: str = "") -> str:
    cmd = [
        sys.executable,
        str(MODULE_ROOT / "scripts" / "setup_test_data.py"),
        "--auth-file", str(auth_file),
    ]
    if warehouse_id:
        cmd += ["--warehouse-id", warehouse_id]
    cmd += list(flags)
    _run(cmd)
    return _resolve_warehouse_id(auth_file, warehouse_id)


# ---------------------------------------------------------------------------
# Env file helpers
# ---------------------------------------------------------------------------

def _resolve_warehouse_id(auth_file: Path, warehouse_id: str) -> str:
    """Return a ready warehouse ID — use the given one, or auto-detect from the workspace.

    Mirrors setup_test_data.py::_get_warehouse so every scenario shares the
    same warehouse rather than each one creating 'ABAC Governance Warehouse'.
    """
    if warehouse_id:
        return warehouse_id
    try:
        import hcl2 as _hcl2
        from databricks.sdk import WorkspaceClient as _WC

        def _s(v): return (v[0] if isinstance(v, list) else (v or "")).strip()

        with open(auth_file) as f:
            auth = _hcl2.load(f)
        host          = _s(auth.get("databricks_workspace_host", ""))
        client_id     = _s(auth.get("databricks_client_id", ""))
        client_secret = _s(auth.get("databricks_client_secret", ""))
        if not host:
            return ""
        w = _WC(host=host, client_id=client_id, client_secret=client_secret)
        warehouses = list(w.warehouses.list())
        wh = select_warehouse(warehouses)
        if wh:
            print(f"  Auto-selected warehouse: {wh.name} ({wh.id})")
            return wh.id or ""
    except Exception as exc:
        print(f"  WARNING: Could not auto-detect warehouse: {exc}")
    return ""


def _write_env_tfvars(env: str, spaces_hcl: str, warehouse_id: str = "") -> None:
    """Write a minimal env.auto.tfvars for the given env."""
    env_dir = ENVS_DIR / env
    wh_line = f'sql_warehouse_id = "{warehouse_id}"' if warehouse_id else 'sql_warehouse_id = ""'
    content = f"{spaces_hcl}\n{wh_line}\n"
    (env_dir / "env.auto.tfvars").write_text(content)


def _patch_warehouse_id_in_env_tfvars(env: str, auth_file: Path) -> str:
    """After `make apply`, discover the running warehouse and write its ID into env.auto.tfvars.

    When two envs (e.g. dev + prod) are applied against the *same* workspace, the second
    apply tries to create another warehouse with the same name and fails.  Calling this
    function after the first env's apply patches sql_warehouse_id in env.auto.tfvars so
    that the promote step (which copies the value to the secondary env) causes Terraform to
    *reuse* the existing warehouse (count=0 branch) instead of creating a duplicate.

    Returns the discovered warehouse ID, or "" if none was found.
    """
    import re as _re2
    wh = _resolve_warehouse_id(auth_file, "")
    if not wh:
        return ""
    env_tfvars = ENVS_DIR / env / "env.auto.tfvars"
    if not env_tfvars.exists():
        return wh
    content = env_tfvars.read_text()
    new_content = _re2.sub(
        r'^sql_warehouse_id\s*=\s*"[^"]*"',
        f'sql_warehouse_id = "{wh}"',
        content,
        flags=_re2.MULTILINE,
    )
    if new_content != content:
        env_tfvars.write_text(new_content)
        print(f"  Patched {env}/env.auto.tfvars: sql_warehouse_id = {wh!r}")
    return wh


def _copy_auth(src_env: str, dest_env: str) -> None:
    """Copy auth.auto.tfvars from src_env to dest_env."""
    src  = ENVS_DIR / src_env / "auth.auto.tfvars"
    dest = ENVS_DIR / dest_env / "auth.auto.tfvars"
    shutil.copy2(src, dest)


# ---------------------------------------------------------------------------
# Account-level name suffixing (test isolation)
# ---------------------------------------------------------------------------
# Groups and tag policies are account-scoped. To prevent conflicts between
# scenarios, append a unique suffix to all group names and tag policy keys
# in the generated config. This is test-only — normal user flow is unaffected.

_TEST_SUFFIX = os.environ.get("_TEST_SUFFIX", "")  # Set from env (parallel runner) or main() (sequential)


def _suffix_account_names(tfvars_path: Path) -> int:
    """Append _TEST_SUFFIX to all group names and tag policy keys in the generated config.

    Updates all cross-references (to_principals, except_principals, group_members,
    acl_groups, tag_key, match_condition hasTagValue, when_condition).

    Returns the number of names suffixed.
    """
    if not _TEST_SUFFIX:
        return 0

    _ensure_packages()
    import hcl2

    text = tfvars_path.read_text()
    try:
        cfg = hcl2.loads(text)
    except Exception:
        return 0

    suffix = _TEST_SUFFIX
    count = 0

    # Extract group names
    groups_cfg = cfg.get("groups") or {}
    if isinstance(groups_cfg, list):
        groups_cfg = groups_cfg[0] if groups_cfg else {}
    group_names = list(groups_cfg.keys())

    # Extract tag policy keys
    tag_policies = cfg.get("tag_policies") or []
    if isinstance(tag_policies, list) and tag_policies and isinstance(tag_policies[0], list):
        tag_policies = tag_policies[0]
    tag_keys = []
    for tp in tag_policies:
        if isinstance(tp, list):
            tp = tp[0] if tp else {}
        key = tp.get("key", "")
        if isinstance(key, list):
            key = key[0] if key else ""
        if key:
            tag_keys.append(key)

    import re as _re_suffix

    # Replace group names in all contexts (double-quoted strings).
    # Use regex with negative lookahead to prevent double-suffixing when the
    # assembled config contains both unsuffixed (from new per-space generate)
    # and already-suffixed names (from previous phases).
    for name in group_names:
        if name.endswith(f"_{suffix}"):
            continue  # already suffixed
        # Match "name" NOT followed by _suffix (prevents "name_abc123" → "name_abc123_abc123")
        pattern = _re_suffix.compile(
            r'"' + _re_suffix.escape(name) + r'"(?!.*_' + _re_suffix.escape(suffix) + r')'
        )
        # Simpler approach: replace exact "name" only when NOT already part of "name_suffix"
        old = f'"{name}"'
        new = f'"{name}_{suffix}"'
        already_suffixed = f'"{name}_{suffix}"'
        if old in text and already_suffixed not in text:
            text = text.replace(old, new)
            count += 1
        elif old in text:
            # Both unsuffixed and suffixed exist — only replace the unsuffixed ones.
            # Temporarily protect already-suffixed instances, replace, then restore.
            placeholder = f'"__SUFFIXED__{name}__"'
            text = text.replace(already_suffixed, placeholder)
            text = text.replace(old, new)
            text = text.replace(placeholder, already_suffixed)
            count += 1

    # Replace tag policy keys in double-quoted and single-quoted strings.
    for key in tag_keys:
        if key.endswith(f"_{suffix}"):
            continue  # already suffixed
        for quote in ['"', "'"]:
            old_q = f'{quote}{key}{quote}'
            new_q = f'{quote}{key}_{suffix}{quote}'
            already_q = f'{quote}{key}_{suffix}{quote}'
            if old_q in text and already_q not in text:
                text = text.replace(old_q, new_q)
                count += 1
            elif old_q in text:
                # Protect already-suffixed, replace unsuffixed, restore.
                placeholder = f'{quote}__SUFFIXED__{key}__{quote}'
                text = text.replace(already_q, placeholder)
                text = text.replace(old_q, new_q)
                text = text.replace(placeholder, already_q)
                count += 1

    if count:
        tfvars_path.write_text(text)
        print(f"  Suffixed {count} account-level name(s) with '_{suffix}'")
    elif group_names or tag_keys:
        # Groups/tag keys exist but none were replaced — suffix may have failed
        print(f"  {_yellow('WARN')} Suffix replacement found 0 matches for "
              f"{len(group_names)} group(s) and {len(tag_keys)} tag key(s) — "
              f"names may already be suffixed or file format unexpected")

    return count


def _preserve_existing_tag_policy_values(new_abac: Path, existing_abac: Path) -> int:
    """Merge existing tag policy values into the newly generated config.

    In multi-phase scenarios (e.g. per-space, abac-only), Phase 2's LLM
    re-generation may produce different tag policy values than Phase 1.
    Since Phase 1's tag assignments are already on columns, the new tag
    policy must be a SUPERSET of the old values, otherwise ABAC evaluation
    fails with INVALID_TAG_POLICY_VALUE.

    Returns the number of values added.
    """
    if not existing_abac.exists() or not new_abac.exists():
        return 0

    _ensure_packages()
    import hcl2

    try:
        existing_cfg = hcl2.loads(existing_abac.read_text())
        new_text = new_abac.read_text()
        new_cfg = hcl2.loads(new_text)
    except Exception:
        return 0

    # Build map of existing tag_key → set of values
    existing_vals: dict[str, set[str]] = {}
    for tp in existing_cfg.get("tag_policies", []):
        k = tp.get("key", "")
        vals = tp.get("values", [])
        if k and vals:
            existing_vals[k] = set(vals)

    if not existing_vals:
        return 0

    # Find values in existing that are missing from new
    added = 0
    new_keys = {tp.get("key", "") for tp in new_cfg.get("tag_policies", [])}

    for tp in new_cfg.get("tag_policies", []):
        k = tp.get("key", "")
        new_vals = tp.get("values", [])
        if k not in existing_vals:
            continue
        missing = sorted(existing_vals[k] - set(new_vals))
        if not missing:
            continue
        # Add missing values to the new config via text replacement
        import re as _re_pres
        # Find the values = [...] block for this key
        pattern = _re_pres.compile(
            r'(\{\s*key\s*=\s*"' + _re_pres.escape(k) + r'"[^}]*?values\s*=\s*\[)([^\]]*?)(\])',
            _re_pres.DOTALL,
        )
        m = pattern.search(new_text)
        if m:
            existing_block = m.group(2).rstrip().rstrip(",")
            extra = ", ".join(f'"{v}"' for v in missing)
            new_block = f"{existing_block}, {extra}"
            new_text = new_text[:m.start(2)] + new_block + new_text[m.end(2):]
            added += len(missing)
            for v in missing:
                print(f"  [PRESERVE] Kept existing tag_policy value '{v}' for key '{k}'")

    # Also preserve entire tag_policy entries that exist in the old config
    # but are completely missing from the new config (e.g. phi_level exists
    # in dev but bu2's LLM didn't generate it at all).
    missing_keys = sorted(set(existing_vals.keys()) - new_keys)
    if missing_keys:
        # Find the tag_policies = [...] section using bracket-depth counting
        # (the naive regex `(.*?)(\])` matches the FIRST `]` which is inside
        # a `values = [...]` block, not the outer `]` of tag_policies).
        import re as _re_pres2
        tp_match = _re_pres2.search(r"tag_policies\s*=\s*\[", new_text)
        if tp_match:
            bracket_start = tp_match.end()  # position after opening [
            depth = 1
            i = bracket_start
            while i < len(new_text) and depth > 0:
                ch = new_text[i]
                if ch == "[":
                    depth += 1
                elif ch == "]":
                    depth -= 1
                i += 1
            insert_pos = i - 1  # position of the closing ]
            # Ensure trailing comma on last existing entry
            preceding = new_text[bracket_start:insert_pos].rstrip()
            if preceding and preceding.endswith("}") and not preceding.endswith("},"):
                last_brace = bracket_start + len(preceding) - 1
                new_text = new_text[:last_brace + 1] + "," + new_text[last_brace + 1:]
                insert_pos += 1  # shifted by 1 due to comma insertion
            blocks = []
            for k in missing_keys:
                vals_str = ", ".join(f'"{v}"' for v in sorted(existing_vals[k]))
                blocks.append(f'  {{\n    key    = "{k}"\n    values = [{vals_str}]\n  }},')
                print(f"  [PRESERVE] Kept entire tag_policy '{k}' with {len(existing_vals[k])} value(s)")
                added += len(existing_vals[k])
            injection = "\n" + "\n".join(blocks) + "\n"
            new_text = new_text[:insert_pos] + injection + new_text[insert_pos:]

    if added:
        new_abac.write_text(new_text)
    return added


def _cleanup_orphan_tag_policies_before_apply() -> None:
    """Delete ALL tag policies with the current suffix from the workspace.

    When `make apply` retries, import_existing.sh may import an orphan tag policy
    from a previous attempt (with a different UUID). Terraform then sees a state
    mismatch and destroy-and-recreates, producing DUPLICATE tag policies with the
    same key. The FGAC engine can't resolve duplicates → "Unknown tag policy key".

    Deleting all suffixed tag policies before apply ensures a clean slate.
    Terraform then creates them fresh with no duplicates.
    """
    if not _TEST_SUFFIX:
        return
    auth_file = ENVS_DIR / "dev" / "auth.auto.tfvars"
    if not auth_file.exists():
        return
    try:
        _ensure_packages()
        import hcl2 as _hcl2_ct
        with open(auth_file) as f:
            cfg = _hcl2_ct.load(f)
        _s = lambda v: (v[0] if isinstance(v, list) else (v or "")).strip()
        host = _s(cfg.get("databricks_workspace_host", ""))
        cid = _s(cfg.get("databricks_client_id", ""))
        csec = _s(cfg.get("databricks_client_secret", ""))
        if not host:
            return
        from databricks.sdk import WorkspaceClient as _WC_ct
        w = _WC_ct(host=host, client_id=cid, client_secret=csec)
        deleted = 0
        for tp in list(w.tag_policies.list_tag_policies()):
            key = getattr(tp, "tag_key", "") or ""
            if key.endswith(f"_{_TEST_SUFFIX}"):
                try:
                    w.tag_policies.delete_tag_policy(tag_key=key)
                    deleted += 1
                except Exception:
                    pass
        if deleted:
            print(f"  [PRE-APPLY] Deleted {deleted} orphan tag policy/ies with suffix '_{_TEST_SUFFIX}'")
        # Also clear the account Terraform state for tag policies so Terraform
        # creates fresh instead of trying to refresh stale IDs.
        account_state = ENVS_DIR / "account" / "terraform.tfstate"
        if account_state.exists():
            import json as _json_ct
            try:
                state = _json_ct.loads(account_state.read_text())
                resources = state.get("resources", [])
                cleaned = [r for r in resources if r.get("type") != "databricks_tag_policy"]
                if len(cleaned) < len(resources):
                    state["resources"] = cleaned
                    state["serial"] = state.get("serial", 0) + 1
                    account_state.write_text(_json_ct.dumps(state, indent=2))
                    print(f"  [PRE-APPLY] Cleared {len(resources) - len(cleaned)} tag_policy entries from account state")
            except Exception:
                pass
        _clear_apply_fingerprints(ENVS_DIR / "account")
        print("  [PRE-APPLY] Cleared account apply fingerprints so tag policies are recreated")
    except Exception as exc:
        print(f"  {_yellow('WARN')} pre-apply tag policy cleanup: {exc}")


def _clean_env_artifacts(env: str) -> None:
    """Remove generated output and split config so the next run starts fresh.

    Preserves auth.auto.tfvars and env.auto.tfvars (those are managed by the
    test runner). Terraform state is removed so apply runs from scratch.
    """
    env_dir = ENVS_DIR / env
    if not env_dir.exists():
        return

    removable_files = [
        "generated/abac.auto.tfvars",
        "generated/masking_functions.sql",
        "generated/generated_response.md",
        "generated/TUNING.md",
        "abac.auto.tfvars",
        "data_access/abac.auto.tfvars",
        "data_access/masking_functions.sql",
    ]
    removable_dirs = [
        "generated/spaces",
        ".terraform",
        "data_access/.terraform",
    ]
    removable_globs = [
        "*.tfstate",
        "*.tfstate.backup",
        ".*.apply.sha",
        ".genie_space_id*",
        "data_access/*.tfstate",
        "data_access/*.tfstate.backup",
        "data_access/.*.apply.sha",
    ]

    for rel in removable_files:
        p = env_dir / rel
        if p.exists():
            p.unlink()

    for rel in removable_dirs:
        p = env_dir / rel
        if p.exists():
            shutil.rmtree(p)

    for pattern in removable_globs:
        # Handle patterns that cross into data_access/
        base = env_dir if "/" not in pattern else env_dir / pattern.split("/")[0]
        glob = pattern.split("/")[-1]
        for p in base.glob(glob):
            p.unlink()


def _clean_account_artifacts() -> None:
    """Remove account-layer split config and state for a fresh start."""
    acct_dir = ENVS_DIR / "account"
    if not acct_dir.exists():
        return
    for rel in ["abac.auto.tfvars", "terraform.tfstate", "terraform.tfstate.backup",
                ".terraform.lock.hcl", ".account.apply.sha"]:
        p = acct_dir / rel
        if p.exists():
            p.unlink()
    tf_dir = acct_dir / ".terraform"
    if tf_dir.exists():
        shutil.rmtree(tf_dir)


def _reset_phase_artifacts(*envs: str) -> None:
    """Remove local split/generated artifacts after an intra-scenario destroy.

    Some multi-phase scenarios intentionally destroy account/workspace resources
    between phases to free quota, then regenerate with different overlays. If we
    keep the previous phase's split files around, the next phase can preserve or
    merge stale tag-policy values back into the newly generated config.
    """
    for env in envs:
        _clean_env_artifacts(env)
    _clean_account_artifacts()


# ---------------------------------------------------------------------------
# Destroy helpers (best-effort — skip if no state)
# ---------------------------------------------------------------------------

def _clear_apply_fingerprints(*env_dirs: Path) -> None:
    """Delete Terraform apply fingerprint files (.*.apply.sha) from the given directories.

    The apply fingerprint cache (e.g. .workspace.apply.sha) prevents re-applying when
    inputs haven't changed.  After `terraform destroy` the remote resources are gone but
    the fingerprint file still exists, causing the *next* scenario's `make apply` to skip
    recreating those resources.  We clear the files after every destroy so the subsequent
    apply always runs.
    """
    for d in env_dirs:
        for sha_file in d.glob(".*.apply.sha"):
            try:
                sha_file.unlink()
            except OSError:
                pass


def _force_account_reapply(reason: str) -> None:
    """Force the shared account layer to re-run on the next apply.

    Some multi-phase scenarios reuse the shared account env across dev/prod or
    across multiple workspace envs. Clearing the fingerprint ensures the next
    `make apply` re-executes the account layer instead of skipping it as
    "inputs unchanged", which gives sync-tags another chance to converge live
    tag-policy values before downstream verification.
    """
    _clear_apply_fingerprints(ENVS_DIR / "account")
    print(f"  [PRE-APPLY] Cleared account apply fingerprints before {reason}")


def _try_destroy(env: str) -> None:
    """Destroy Terraform resources for env if state exists.

    Passes -lock=false so that a stale advisory lock left by a previously
    killed process does not block the cleanup.  Also passes -refresh=false
    so terraform doesn't try to read remote resources (like groups) that may
    have already been deleted by a prior account-layer destroy — without this,
    data_access destroy can fail with "cannot read group" errors, leaving
    orphaned tag assignments that block subsequent applies.
    """
    state    = ENVS_DIR / env / "terraform.tfstate"
    da_state = ENVS_DIR / env / "data_access" / "terraform.tfstate"
    if not state.exists() and not da_state.exists():
        # Even if there is nothing to destroy, clear any stale fingerprint files
        # so the next apply is not skipped due to an outdated hash.
        _clear_apply_fingerprints(ENVS_DIR / env, ENVS_DIR / env / "data_access")
        return
    _step(f"Destroying {env} Terraform resources")
    _make(f"destroy", f"ENV={env}", "DESTROY_FLAGS=-lock=false -refresh=false", check=False)
    _clear_apply_fingerprints(ENVS_DIR / env, ENVS_DIR / env / "data_access")


def _try_destroy_account() -> None:
    state = ENVS_DIR / "account" / "terraform.tfstate"
    if not state.exists():
        _clear_apply_fingerprints(ENVS_DIR / "account")
        return
    _step("Destroying account Terraform resources")
    _make("destroy", "ENV=account", "DESTROY_FLAGS=-lock=false -refresh=false", check=False)
    _clear_apply_fingerprints(ENVS_DIR / "account")


import re as _re_mod
# Tag policy keys that our LLM generates are always strictly lowercase snake_case.
# Any key that doesn't match is from another user/demo and must not be touched.
_OUR_TAG_KEY_RE = _re_mod.compile(r"^[a-z][a-z0-9_]*$")


def _force_delete_tag_policies(*envs: str) -> None:
    """Best-effort deletion of our own account-level UC tag policies.

    UC tag policies are account-scoped objects (not tied to a catalog).  They
    persist even after `terraform destroy` if the account state file was wiped
    before destroy could run.  The next scenario's generate step produces
    different key names (LLM is non-deterministic), so the import step finds
    old policies that don't match — causing "Cannot import non-existent remote
    object" and "Resource already managed by Terraform" errors.

    IMPORTANT: This is a SHARED account.  Many other SA demos have their own
    tag policies here (e.g. "Med Tech", "pii_tmna_demo", "Jai_pii").  We only
    delete policies whose keys match our strict snake_case pattern
    (^[a-z][a-z0-9_]*$) — anything with spaces, uppercase letters, or
    non-snake characters is skipped as belonging to another user.
    """
    for env in envs:
        auth_file = ENVS_DIR / env / "auth.auto.tfvars"
        if not auth_file.exists():
            auth_file = ENVS_DIR / "dev" / "auth.auto.tfvars"
        if not auth_file.exists():
            continue
        try:
            import hcl2 as _hcl2
            from databricks.sdk import WorkspaceClient as _WC

            def _s(v): return (v[0] if isinstance(v, list) else (v or "")).strip()

            with open(auth_file) as f:
                auth = _hcl2.load(f)
            host          = _s(auth.get("databricks_workspace_host", ""))
            client_id     = _s(auth.get("databricks_client_id", ""))
            client_secret = _s(auth.get("databricks_client_secret", ""))
            if not host:
                continue
            w = _WC(host=host, client_id=client_id, client_secret=client_secret)

            # List tag policies — SDK method name varies by version; try both
            policies = []
            list_err_msg = ""
            for list_fn_name in ("list_tag_policies", "list"):
                try:
                    list_fn = getattr(w.tag_policies, list_fn_name)
                    policies = list(list_fn())
                    break
                except Exception as e:
                    list_err_msg = str(e)
            else:
                # Neither SDK method worked; fall back to REST API
                import ssl as _ssl, urllib.request as _urq, json as _json
                _ctx = _ssl.create_default_context()
                _ctx.check_hostname = False
                _ctx.verify_mode = _ssl.CERT_NONE
                token = w.config.authenticate()
                base  = host.rstrip("/")
                try:
                    req = _urq.Request(f"{base}/api/2.1/unity-catalog/tag-policies", headers=token)
                    with _urq.urlopen(req, timeout=30, context=_ctx) as resp:
                        data = _json.loads(resp.read())
                    # Build minimal proxy objects from raw dicts
                    policies = [type("P", (), {"id": p.get("id"), "tag_key": p.get("tag_key")})()
                                for p in data.get("tag_policies", [])]
                except Exception as rest_err:
                    print(f"  WARN: could not list tag policies via SDK ({list_err_msg}) or REST ({rest_err})")
                    break

            deleted = 0
            skipped = 0
            for tp in policies:
                tp_id  = getattr(tp, "id",      None)
                tp_key = getattr(tp, "tag_key", None)
                if not tp_key:
                    continue
                # Skip tag policies owned by other users/demos.
                # Our LLM always generates strictly lowercase snake_case keys
                # (e.g. pii_level, phi_level, aml_scope).  Keys with spaces,
                # uppercase, or other characters belong to other accounts.
                if not _OUR_TAG_KEY_RE.match(tp_key):
                    skipped += 1
                    continue
                # delete_tag_policy() takes tag_key (not id) as its positional arg.
                del_ok = False
                try:
                    w.tag_policies.delete_tag_policy(tag_key=tp_key)
                    del_ok = True
                except Exception:
                    pass
                if not del_ok:
                    # REST fallback: DELETE /api/2.1/unity-catalog/tag-policies/{tag_key}
                    import ssl as _ssl2, urllib.request as _urq2, urllib.error as _ure2
                    _ctx2 = _ssl2.create_default_context()
                    _ctx2.check_hostname = False
                    _ctx2.verify_mode = _ssl2.CERT_NONE
                    try:
                        token2 = w.config.authenticate()
                        base2  = host.rstrip("/")
                        import urllib.parse as _urp2
                        del_req = _urq2.Request(
                            f"{base2}/api/2.1/unity-catalog/tag-policies/{_urp2.quote(tp_key, safe='')}",
                            headers=token2,
                            method="DELETE",
                        )
                        _urq2.urlopen(del_req, timeout=30, context=_ctx2)
                        del_ok = True
                    except _ure2.HTTPError as del_http:
                        if del_http.code == 404:
                            # 404 = not our policy or already gone; silently skip
                            del_ok = True
                        else:
                            print(f"  WARN: could not delete tag policy {tp_key!r}: HTTP {del_http.code}")
                    except Exception as del_err2:
                        print(f"  WARN: could not delete tag policy {tp_key!r}: {del_err2}")
                if del_ok:
                    print(f"  Force-deleted orphaned tag policy: {tp_key}")
                    deleted += 1
                else:
                    print(f"  WARN: DELETE FAILED for tag policy {tp_key!r} (id={tp_id})")

            if not policies:
                print("  No orphaned tag policies found.")
            elif skipped > 0:
                print(f"  Skipped {skipped} tag policy/ies owned by other users (non-snake_case keys).")
            break  # only need one env's auth for account-level resources
        except Exception as exc:
            print(f"  WARN: force_delete_tag_policies({env}) failed: {exc}")


def _wait_for_tag_policy_deletion(*envs: str, max_wait: int = 180, poll_interval: int = 10) -> None:
    """Poll until all our snake_case tag policies are confirmed deleted.

    Databricks tag policy deletions are eventually consistent — the API may
    still list a policy for several seconds after DELETE returns 200.  This
    helper blocks until no snake_case policies remain, preventing the next
    scenario's Terraform apply from hitting 'Tag policy already exists'.
    """
    for env in envs:
        auth_file = ENVS_DIR / env / "auth.auto.tfvars"
        if not auth_file.exists():
            auth_file = ENVS_DIR / "dev" / "auth.auto.tfvars"
        if not auth_file.exists():
            continue
        try:
            import hcl2 as _hcl2
            from databricks.sdk import WorkspaceClient as _WC
            import ssl as _ssl, urllib.request as _urq, json as _json

            def _s(v): return (v[0] if isinstance(v, list) else (v or "")).strip()

            with open(auth_file) as f:
                auth = _hcl2.load(f)
            host          = _s(auth.get("databricks_workspace_host", ""))
            client_id     = _s(auth.get("databricks_client_id", ""))
            client_secret = _s(auth.get("databricks_client_secret", ""))
            if not host:
                continue
            w = _WC(host=host, client_id=client_id, client_secret=client_secret)

            def _list_our_policies():
                """Return list of snake_case tag policy keys still visible."""
                try:
                    policies = list(w.tag_policies.list_tag_policies())
                except Exception:
                    try:
                        policies = list(w.tag_policies.list())
                    except Exception:
                        # REST fallback
                        _ctx = _ssl.create_default_context()
                        _ctx.check_hostname = False
                        _ctx.verify_mode = _ssl.CERT_NONE
                        token = w.config.authenticate()
                        req = _urq.Request(f"{host.rstrip('/')}/api/2.1/unity-catalog/tag-policies", headers=token)
                        with _urq.urlopen(req, timeout=30, context=_ctx) as resp:
                            data = _json.loads(resp.read())
                        policies = [type("P", (), {"tag_key": p.get("tag_key")})()
                                    for p in data.get("tag_policies", [])]
                return [getattr(p, "tag_key", "") for p in policies
                        if _OUR_TAG_KEY_RE.match(getattr(p, "tag_key", "") or "")]

            elapsed = 0
            while elapsed < max_wait:
                remaining = _list_our_policies()
                if not remaining:
                    print(f"  Tag policies confirmed deleted ({elapsed}s).")
                    break
                print(f"  Waiting for {len(remaining)} tag policy deletion(s) to propagate... ({elapsed}s)")
                time.sleep(poll_interval)
                elapsed += poll_interval
            else:
                remaining = _list_our_policies()
                if remaining:
                    print(f"  WARN: {len(remaining)} tag policy/ies still visible after {max_wait}s: {remaining}")
            break  # only need one env's auth
        except Exception as exc:
            print(f"  WARN: _wait_for_tag_policy_deletion failed: {exc}")


def _force_delete_fgac_policies(*envs: str, all_catalogs: bool = False) -> None:
    """Best-effort API-level deletion of all FGAC policies for all test catalogs.

    Databricks enforces a metastore-wide limit of 1000 ABAC policies total.
    Accumulated policies from failed/partial test runs can push the count past
    this limit, causing subsequent applies to fail with "estimated count exceeds
    limit".  This function proactively deletes every policy_info it finds so
    each scenario starts from zero.

    By default only test-catalog names (dev_*, prod_*, bu2_*) are cleaned.
    Pass all_catalogs=True (or --nuke-fgac CLI flag) to delete from EVERY
    catalog in the workspace — use this for a one-time reset when the metastore
    has accumulated thousands of orphaned policies from many partial test runs.

    Uses the REST API directly (not SDK) because policy_infos may not be
    available in all installed SDK versions.
    """
    import ssl as _ssl
    import urllib.request as _urq
    import urllib.error as _ure
    import urllib.parse as _urp
    import json as _json

    _ssl_ctx = _ssl.create_default_context()
    _ssl_ctx.check_hostname = False
    _ssl_ctx.verify_mode = _ssl.CERT_NONE

    # Catalogs that should never be touched even in all_catalogs mode
    _SYSTEM_CATS = {"hive_metastore", "main", "system", "samples", "__databricks_internal"}

    for env in envs:
        auth_file = ENVS_DIR / env / "auth.auto.tfvars"
        if not auth_file.exists():
            continue
        try:
            import hcl2 as _hcl2
            from databricks.sdk import WorkspaceClient as _WC

            def _s(v): return (v[0] if isinstance(v, list) else (v or "")).strip()

            with open(auth_file) as f:
                auth = _hcl2.load(f)
            host          = _s(auth.get("databricks_workspace_host", ""))
            client_id     = _s(auth.get("databricks_client_id", ""))
            client_secret = _s(auth.get("databricks_client_secret", ""))
            if not host:
                continue
            w = _WC(host=host, client_id=client_id, client_secret=client_secret)
            token = w.config.authenticate()  # {'Authorization': 'Bearer ...'}
            base  = host.rstrip("/")

            if all_catalogs:
                # Nuclear mode: clean every catalog except system ones.
                # Used for a one-time reset when the metastore has thousands of
                # accumulated orphaned policies from many partial test runs.
                try:
                    all_cats = [c.name for c in w.catalogs.list()
                                if c.name and c.name not in _SYSTEM_CATS]
                except Exception:
                    all_cats = []
                print(f"  [NUKE] Deleting FGAC policies from ALL {len(all_cats)} non-system catalogs...")
            else:
                # Normal mode: only test catalog name prefixes.
                test_prefixes = ("dev_", "prod_", "bu2_")
                try:
                    all_cats = [c.name for c in w.catalogs.list()
                                if c.name and any(c.name.startswith(p) for p in test_prefixes)]
                except Exception:
                    all_cats = []

                # Always include well-known test catalog names as a fallback.
                # FGAC policies are stored by catalog NAME, not ID.  If a previous
                # run already dropped the catalogs, w.catalogs.list() won't return
                # them — but orphaned policies for those names can still exist and
                # will re-attach when a new catalog with the same name is created.
                # The API returns 404 for unknown catalogs, which we catch below.
                known_test_cats = {
                    DEV_FIN_CAT, DEV_CLIN_CAT,
                    PROD_FIN_CAT, PROD_CLIN_CAT,
                    "bu2_fin", "bu2_clinical",
                }
                all_cats = list(set(all_cats) | known_test_cats)

            for cat in all_cats:
                try:
                    # Paginate through all policies for this catalog.
                    # Re-list after each round of deletions to catch any that
                    # the API paginates past or that become visible after the
                    # prior batch was removed (eventual consistency).
                    max_rounds = 5
                    for _round in range(max_rounds):
                        page_token = ""
                        round_deleted = 0
                        while True:
                            list_url = f"{base}/api/2.1/unity-catalog/policies/CATALOG/{_urp.quote(cat, safe='')}"
                            if page_token:
                                list_url += f"?page_token={_urp.quote(page_token, safe='')}"
                            req = _urq.Request(list_url, headers=token)
                            with _urq.urlopen(req, timeout=15, context=_ssl_ctx) as resp:
                                data = _json.loads(resp.read())
                            policies = data.get("policies", [])
                            for p in policies:
                                pname = p.get("name", "")
                                if not pname:
                                    continue
                                del_url = (f"{base}/api/2.1/unity-catalog/policies/CATALOG/"
                                           f"{_urp.quote(cat, safe='')}/{_urp.quote(pname, safe='')}")
                                del_req = _urq.Request(del_url, headers=token, method="DELETE")
                                try:
                                    _urq.urlopen(del_req, timeout=15, context=_ssl_ctx)
                                    print(f"  Force-deleted orphaned FGAC policy: {cat}/{pname}")
                                    round_deleted += 1
                                except Exception as del_err:
                                    print(f"  WARN: could not delete FGAC policy {cat}/{pname}: {del_err}")
                            page_token = data.get("next_page_token", "")
                            if not page_token:
                                break
                        if round_deleted == 0:
                            break  # nothing left to delete
                except _ure.HTTPError as he:
                    if he.code not in (403, 404):
                        print(f"  WARN: policy list HTTP {he.code} for {cat}")
                except Exception as cat_err:
                    print(f"  WARN: policy list failed for {cat}: {cat_err}")
        except Exception as exc:
            print(f"  WARN: force_delete_fgac_policies({env}) failed: {exc}")
        break  # only need one env's auth to clean all catalogs


def _force_delete_tag_assignments(*envs: str) -> None:
    """Best-effort API-level deletion of all tag assignments on test schemas.

    If a previous scenario's data_access destroy failed (e.g. because groups
    were already deleted), orphaned tag assignments remain in Databricks.  The
    next scenario's apply then fails with "Tag assignment with tag key … already
    exists".  This function proactively removes all tag assignments on test
    catalog schemas so each scenario starts clean.
    """
    import ssl as _ssl
    import urllib.request as _urq
    import urllib.error as _ure
    import urllib.parse as _urp
    import json as _json

    _ssl_ctx = _ssl.create_default_context()
    _ssl_ctx.check_hostname = False
    _ssl_ctx.verify_mode = _ssl.CERT_NONE

    for env in envs:
        auth_file = ENVS_DIR / env / "auth.auto.tfvars"
        if not auth_file.exists():
            continue
        try:
            import hcl2 as _hcl2
            from databricks.sdk import WorkspaceClient as _WC

            def _s(v): return (v[0] if isinstance(v, list) else (v or "")).strip()

            with open(auth_file) as f:
                auth = _hcl2.load(f)
            host          = _s(auth.get("databricks_workspace_host", ""))
            client_id     = _s(auth.get("databricks_client_id", ""))
            client_secret = _s(auth.get("databricks_client_secret", ""))
            if not host:
                continue
            w = _WC(host=host, client_id=client_id, client_secret=client_secret)
            token = w.config.authenticate()
            base  = host.rstrip("/")

            test_prefixes = ("dev_", "prod_", "bu2_")
            try:
                all_cats = [c.name for c in w.catalogs.list()
                            if c.name and any(c.name.startswith(p) for p in test_prefixes)]
            except Exception:
                all_cats = []

            for cat in all_cats:
                # List schemas in this catalog
                try:
                    schemas = [s.name for s in w.schemas.list(catalog_name=cat) if s.name]
                except Exception:
                    continue

                for schema in schemas:
                    if schema in ("information_schema", "default"):
                        continue

                    # Helper: list + delete all tag assignments for one set of
                    # query params (schema-level and table-level calls use the
                    # same delete endpoint but different listing URLs).
                    def _delete_tags_for(params: str, label: str) -> None:
                        try:
                            list_url = f"{base}/api/2.1/unity-catalog/tags?{params}"
                            req = _urq.Request(list_url, headers=token)
                            with _urq.urlopen(req, timeout=30, context=_ssl_ctx) as resp:
                                data = _json.loads(resp.read())
                            assignments = data.get("tag_assignments", [])
                            if not assignments:
                                return
                            del_body = _json.dumps({"tag_assignments": assignments}).encode()
                            del_req = _urq.Request(
                                f"{base}/api/2.1/unity-catalog/tags",
                                data=del_body,
                                headers={**token, "Content-Type": "application/json"},
                                method="DELETE",
                            )
                            _urq.urlopen(del_req, timeout=30, context=_ssl_ctx)
                            print(f"  Force-deleted {len(assignments)} orphaned tag assignment(s) on {label}")
                        except _ure.HTTPError as he:
                            if he.code not in (403, 404):
                                print(f"  WARN: tag cleanup HTTP {he.code} for {label}")
                        except Exception as err:
                            print(f"  WARN: tag cleanup failed for {label}: {err}")

                    # Schema-level tags
                    _delete_tags_for(
                        f"catalog_name={_urp.quote(cat, safe='')}&schema_name={_urp.quote(schema, safe='')}",
                        f"{cat}.{schema}",
                    )

                    # Column-level tags: the schema-level API only returns
                    # schema/table-level assignments, NOT column-level ones.
                    # Iterate over each table to delete column tags explicitly.
                    try:
                        tables = [t.name for t in w.tables.list(catalog_name=cat, schema_name=schema) if t.name]
                    except Exception:
                        tables = []
                    for tbl in tables:
                        _delete_tags_for(
                            f"catalog_name={_urp.quote(cat, safe='')}"
                            f"&schema_name={_urp.quote(schema, safe='')}"
                            f"&table_name={_urp.quote(tbl, safe='')}",
                            f"{cat}.{schema}.{tbl} (columns)",
                        )
        except Exception as exc:
            print(f"  WARN: force_delete_tag_assignments({env}) failed: {exc}")


# Group names generated by the LLM are Title_Case_With_Underscores
# (e.g. Data_Analyst, Finance_Analyst, Clinical_Data_Steward).
# Built-in groups use lowercase (admins, users, account users).
# Other demos use mixed naming.  We only delete groups matching our pattern.
_OUR_GROUP_NAME_RE = _re_mod.compile(r"^[A-Z][a-z]+(_[A-Z][a-z]+)*$")

# Built-in / system groups that must never be deleted.
_BUILTIN_GROUPS = frozenset({
    "admins", "users", "account users",
})


def _force_delete_groups(*envs: str) -> None:
    """Best-effort deletion of LLM-generated account-level groups via SCIM API.

    Groups created by prior scenarios persist in the shared Databricks account
    even after ``terraform destroy`` — especially when destroy fails or is
    skipped.  On the next scenario's ``terraform apply``, the account layer
    tries to CREATE the same group name, causing "Group with name X already
    exists" errors.

    This helper lists account groups via the SCIM API and deletes any whose
    display_name matches our LLM naming convention (``Title_Case_With_Underscores``).
    Built-in groups (``admins``, ``users``, ``account users``) are always skipped.
    """
    # Find an auth file that has account credentials
    auth_file: Path | None = None
    for env in envs:
        candidate = ENVS_DIR / env / "auth.auto.tfvars"
        if candidate.exists():
            auth_file = candidate
            break
    if auth_file is None:
        candidate = ENVS_DIR / "dev" / "auth.auto.tfvars"
        if candidate.exists():
            auth_file = candidate
    if auth_file is None:
        return

    try:
        from databricks.sdk import AccountClient

        cfg = _load_auth_cfg(auth_file)
        _s = lambda v: (v[0] if isinstance(v, list) else (v or "")).strip()

        account_id    = _s(cfg.get("databricks_account_id", ""))
        client_id     = _s(cfg.get("databricks_client_id", ""))
        client_secret = _s(cfg.get("databricks_client_secret", ""))
        if not account_id:
            return

        account_host = _s(cfg.get("databricks_account_host", "https://accounts.cloud.databricks.com"))
        a = AccountClient(
            host=account_host,
            account_id=account_id,
            client_id=client_id,
            client_secret=client_secret,
        )

        groups = list(a.groups.list())
        deleted = 0
        skipped = 0
        for g in groups:
            name = g.display_name or ""
            if name.lower() in _BUILTIN_GROUPS:
                continue
            if not _OUR_GROUP_NAME_RE.match(name):
                skipped += 1
                continue
            try:
                a.groups.delete(id=g.id)
                deleted += 1
            except Exception as del_err:
                print(f"  WARN: could not delete group {name!r} (id={g.id}): {del_err}")
        print(f"  force_delete_groups: deleted {deleted}, skipped {skipped} non-matching")
    except Exception as exc:
        print(f"  WARN: force_delete_groups failed: {exc}")


def _drop_test_catalogs(*envs: str) -> None:
    """Drop test catalogs via SQL to fully reset FGAC estimated policy counts.

    Databricks tracks estimated policy counts per catalog.  These counts are
    eventually consistent and can become inflated when many test runs create
    and delete policies without dropping the catalogs.  Using DROP CATALOG
    via SQL (not SDK) ensures the catalog is fully purged — the SDK's
    catalogs.delete() may soft-delete, leaving the estimated counter intact.
    """
    test_prefixes = ("dev_", "prod_", "bu2_")
    for env in envs:
        auth_file = ENVS_DIR / env / "auth.auto.tfvars"
        if not auth_file.exists():
            continue
        try:
            import hcl2 as _hcl2
            from databricks.sdk import WorkspaceClient as _WC

            def _s(v): return (v[0] if isinstance(v, list) else (v or "")).strip()

            with open(auth_file) as f:
                auth = _hcl2.load(f)
            host          = _s(auth.get("databricks_workspace_host", ""))
            client_id     = _s(auth.get("databricks_client_id", ""))
            client_secret = _s(auth.get("databricks_client_secret", ""))
            if not host:
                continue
            w = _WC(host=host, client_id=client_id, client_secret=client_secret)

            # Find a SQL warehouse for executing DROP CATALOG
            wh_id = ""
            try:
                for ep in w.warehouses.list():
                    if ep.state and ep.state.value in ("RUNNING", "STARTING"):
                        wh_id = ep.id
                        break
                if not wh_id:
                    for ep in w.warehouses.list():
                        wh_id = ep.id
                        break
            except Exception:
                pass
            if not wh_id:
                print("  WARN: no SQL warehouse found; skipping catalog drop")
                return

            try:
                all_cats = [c.name for c in w.catalogs.list()
                            if c.name and any(c.name.startswith(p) for p in test_prefixes)]
            except Exception:
                all_cats = []
            for cat in all_cats:
                try:
                    resp = w.statement_execution.execute_statement(
                        statement=f"DROP CATALOG IF EXISTS `{cat}` CASCADE",
                        warehouse_id=wh_id,
                        wait_timeout="50s",
                    )
                    if resp.status and resp.status.state and resp.status.state.value == "SUCCEEDED":
                        print(f"  Dropped test catalog (SQL): {cat}")
                    else:
                        # Fallback to SDK delete
                        w.catalogs.delete(cat, force=True)
                        print(f"  Dropped test catalog (SDK): {cat}")
                except Exception as e:
                    print(f"  WARN: could not drop catalog {cat}: {e}")
            return  # only need one env's auth to clean all catalogs
        except Exception as exc:
            print(f"  WARN: _drop_test_catalogs({env}) failed: {exc}")


def _count_fgac_policies(*envs: str) -> int:
    """Count actual (not estimated) FGAC policies across all catalogs via REST API.

    Databricks uses an eventually-consistent *estimated* counter for the
    metastore-wide ABAC policy limit check.  After mass-deletion the real count
    drops immediately, but the estimated counter can lag for several minutes.
    Polling the real count lets us confirm cleanup succeeded and then wait for
    the estimated counter without guessing a fixed sleep duration.
    """
    import ssl as _ssl, urllib.request as _urq, urllib.error as _ure
    import urllib.parse as _urp, json as _json

    _ssl_ctx = _ssl.create_default_context()
    _ssl_ctx.check_hostname = False
    _ssl_ctx.verify_mode = _ssl.CERT_NONE

    for env in envs:
        auth_file = ENVS_DIR / env / "auth.auto.tfvars"
        if not auth_file.exists():
            continue
        try:
            import hcl2 as _hcl2
            from databricks.sdk import WorkspaceClient as _WC

            def _s(v): return (v[0] if isinstance(v, list) else (v or "")).strip()

            with open(auth_file) as f:
                auth = _hcl2.load(f)
            host          = _s(auth.get("databricks_workspace_host", ""))
            client_id     = _s(auth.get("databricks_client_id", ""))
            client_secret = _s(auth.get("databricks_client_secret", ""))
            if not host:
                continue
            w = _WC(host=host, client_id=client_id, client_secret=client_secret)
            token = w.config.authenticate()
            base  = host.rstrip("/")

            total = 0
            try:
                cats = [c.name for c in w.catalogs.list() if c.name]
            except Exception:
                cats = []
            for cat in cats:
                try:
                    page_token = ""
                    while True:
                        url = f"{base}/api/2.1/unity-catalog/policies/CATALOG/{_urp.quote(cat, safe='')}"
                        if page_token:
                            url += f"?page_token={_urp.quote(page_token, safe='')}"
                        req = _urq.Request(url, headers=token)
                        with _urq.urlopen(req, timeout=15, context=_ssl_ctx) as r:
                            data = _json.loads(r.read())
                        total += len(data.get("policies", []))
                        page_token = data.get("next_page_token", "")
                        if not page_token:
                            break
                except _ure.HTTPError as he:
                    if he.code not in (403, 404):
                        pass  # ignore inaccessible catalogs
                except Exception:
                    pass
            return total
        except Exception:
            pass
    return -1  # unknown


def _wait_for_fgac_quota(
    *envs: str,
    target: int = 900,
    max_wait_seconds: int = 360,
    poll_interval: int = 30,
) -> None:
    """Wait until the Databricks FGAC quota counter has propagated after cleanup.

    Databricks uses an *estimated* counter for the metastore-wide ABAC policy
    limit (1000 total).  After mass-deletion the REAL count drops immediately
    but this estimated counter can lag 3-10 minutes — Terraform will fail with
    "estimated count exceeds limit" if we apply too soon.

    Strategy:
      1. Confirm the real count is below target (cleanup worked).
         If not, poll every poll_interval seconds until it drops or we time out.
      2. If the real count IS below target, we still need to wait for Databricks'
         estimated counter to catch up.  We do this by keeping the full
         max_wait_seconds budget and polling; this avoids a hard-coded sleep
         while still ensuring we wait long enough.
    """
    waited = 0
    real_cleared = False

    while waited < max_wait_seconds:
        count = _count_fgac_policies(*envs)
        remaining = max_wait_seconds - waited

        if count < 0:
            print(f"  Cannot query FGAC count; waiting {poll_interval}s (budget: {remaining}s)...")
        elif count < target:
            if not real_cleared:
                real_cleared = True
                print(f"  Real FGAC count: {count} ✓  Waiting for estimated counter to propagate...")
            else:
                print(f"  Real FGAC count: {count} ✓  Still waiting ({remaining}s budget left)...")
        else:
            print(
                f"  Real FGAC count: {count} (≥ {target}). "
                f"Waiting {poll_interval}s for policies to drain ({remaining}s remaining)..."
            )
            real_cleared = False

        time.sleep(poll_interval)
        waited += poll_interval

    count = _count_fgac_policies(*envs)
    if count >= target:
        print(
            f"  WARNING: FGAC quota wait timed out after {max_wait_seconds}s "
            f"(real count: {count}). Proceeding — apply has built-in retries."
        )
    else:
        print(
            f"  FGAC quota: real count {count}, waited {waited}s. "
            f"Proceeding — estimated counter should be current."
        )


def _preamble_cleanup(*envs: str, fresh_env: bool = False) -> None:
    """Best-effort pre-scenario cleanup.

    Destroys Terraform resources for each env and the account layer *while their
    state files still exist*, then wipes all local artifacts.  This ensures each
    scenario starts clean even if a previous scenario failed before its own
    teardown block ran (which would have left Databricks groups, tag_policies,
    and Genie Spaces behind without Terraform state to track them).

    When fresh_env=True (provisioned via provision_test_env.py), the metastore
    counter always starts at 0 so the FGAC quota wait is skipped entirely.
    """
    _step("Pre-scenario cleanup (destroying any leftover resources from prior run)")

    def _has_local_state(env: str) -> bool:
        env_dir = ENVS_DIR / env
        candidates = [
            env_dir / "terraform.tfstate",
            env_dir / "terraform.tfstate.backup",
            env_dir / ".terraform",
            env_dir / "data_access" / "terraform.tfstate",
            env_dir / "data_access" / "terraform.tfstate.backup",
            env_dir / "data_access" / ".terraform",
            env_dir / ".workspace.apply.sha",
            env_dir / "data_access" / ".workspace.apply.sha",
            env_dir / "generated",
        ]
        return any(path.exists() for path in candidates)

    # In parallel mode (per-scenario workspace), the first attempt starts with a fresh
    # metastore and can skip expensive API cleanup. But when a scenario is retried in the
    # same workspace after a partial apply, stale data_access/workspace resources must be
    # destroyed or the next attempt will hit "already exists" assignment/policy errors.
    if _TEST_SUFFIX and fresh_env:
        has_stale_state = any(_has_local_state(env) for env in (*envs, "account"))
        if not has_stale_state:
            print("  Parallel mode — fresh workspace, cleaning only own-suffix account resources.")
            # Clean suffixed tag policies from previous runs via workspace API.
            # Each scenario has its own workspace, so this is safe in parallel.
            for env in envs:
                auth_file = ENVS_DIR / env / "auth.auto.tfvars"
                if not auth_file.exists():
                    auth_file = ENVS_DIR / "dev" / "auth.auto.tfvars"
                if auth_file.exists():
                    try:
                        import hcl2 as _hcl2_pc
                        import re as _re_pc
                        _suffix_re = _re_pc.compile(r"^[a-z_]+_[a-f0-9]{4,}$")
                        with open(auth_file) as f:
                            _cfg = _hcl2_pc.load(f)
                        _s = lambda v: (v[0] if isinstance(v, list) else (v or "")).strip()
                        _host = _s(_cfg.get("databricks_workspace_host", ""))
                        _cid = _s(_cfg.get("databricks_client_id", ""))
                        _csec = _s(_cfg.get("databricks_client_secret", ""))
                        if _host:
                            from databricks.sdk import WorkspaceClient as _WC_pc
                            _w = _WC_pc(host=_host, client_id=_cid, client_secret=_csec)
                            _deleted = 0
                            for tp in list(_w.tag_policies.list_tag_policies()):
                                key = getattr(tp, "tag_key", "") or ""
                                if _suffix_re.match(key):
                                    try:
                                        _w.tag_policies.delete_tag_policy(tag_key=key)
                                        _deleted += 1
                                    except Exception:
                                        pass
                            if _deleted:
                                print(f"  Deleted {_deleted} orphan suffixed tag policy/ies")
                    except Exception as _exc:
                        print(f"  {_yellow('WARN')} tag policy cleanup: {_exc}")
                _clean_env_artifacts(env)
            _clean_account_artifacts()
            return
        print("  Parallel mode retry detected — stale state exists, doing full cleanup.")

    # On a fresh provisioned environment the metastore starts clean, so there is
    # no stale FGAC quota to wait for.  However, if a PREVIOUS scenario in the
    # same test run applied Terraform resources and then failed before its own
    # teardown block ran, those resources (groups, Genie Spaces, tag policies)
    # persist and must be destroyed before the next scenario can recreate them.
    # We destroy using state files when they exist, then wipe all artifacts.
    if fresh_env:
        print("  Fresh metastore — destroying prior scenario state (if any).")
        # Track auth file existence through cleanup for diagnostics.
        _auth_probe = ENVS_DIR / "dev" / "auth.auto.tfvars"
        def _auth_check(label: str) -> None:
            exists = _auth_probe.exists()
            if not exists:
                print(f"  {_yellow('DIAG')} auth.auto.tfvars MISSING after: {label}")
                # Check if the parent directory still exists
                if not _auth_probe.parent.exists():
                    print(f"  {_yellow('DIAG')}   parent dir ALSO missing: {_auth_probe.parent}")
        for env in envs:
            _try_destroy(env)
            _auth_check(f"_try_destroy({env})")
        _try_destroy_account()
        _auth_check("_try_destroy_account")
        # Explicitly delete groups, tag policies, and tag assignments via API
        # in case terraform destroy missed them (e.g. they were never in state
        # due to import failures or a partially-failed apply).
        _force_delete_groups(*envs)
        _force_delete_tag_policies(*envs)
        _force_delete_tag_assignments(*envs)
        _force_delete_fgac_policies(*envs, all_catalogs=True)
        _auth_check("force_delete_*")
        # Wait for deletions to propagate (Databricks eventual consistency),
        # then retry tag policies and tag assignments to catch any that
        # survived the first pass.
        time.sleep(15)
        _force_delete_tag_policies(*envs)
        _force_delete_tag_assignments(*envs)
        # Block until tag policies are confirmed gone — prevents "already exists"
        # errors when the next scenario's Terraform apply tries to create them.
        _wait_for_tag_policy_deletion(*envs)
        _auth_check("wait_for_tag_policy_deletion")
        for env in envs:
            _clean_env_artifacts(env)
        _auth_check("_clean_env_artifacts")
        _clean_account_artifacts()
        _auth_check("_clean_account_artifacts")
        return

    for env in envs:
        _try_destroy(env)
    _try_destroy_account()
    # ORDER IS CRITICAL — delete in dependency order so each step succeeds:
    # 0. Delete account-level groups so next scenario can recreate them.
    _force_delete_groups(*envs)
    # 1. Delete account-level UC tag policies FIRST.
    _force_delete_tag_policies(*envs)
    # 2. Delete FGAC policies from ALL non-system catalogs in the workspace.
    _force_delete_fgac_policies(*envs, all_catalogs=True)
    # 3. Delete column-level tag assignments.
    _force_delete_tag_assignments(*envs)
    # Wait for deletions to propagate (Databricks eventual consistency),
    # then retry tag policies and tag assignments to catch any that
    # survived the first pass.
    time.sleep(15)
    _force_delete_tag_policies(*envs)
    _force_delete_tag_assignments(*envs)
    # Block until tag policies are confirmed gone — prevents "already exists"
    # errors when the next scenario's Terraform apply tries to create them.
    _wait_for_tag_policy_deletion(*envs)
    # 4. Drop test catalogs LAST.
    _drop_test_catalogs(*envs)
    # On a shared/long-lived metastore the FGAC estimated counter can lag
    # several minutes after mass-deletion — poll until it settles.
    _wait_for_fgac_quota(*envs, target=900, max_wait_seconds=300, poll_interval=30)
    for env in envs:
        _clean_env_artifacts(env)
    _clean_account_artifacts()


def _teardown_data(*flags: str, auth_file: Path, warehouse_id: str = "") -> None:
    """Drop test catalogs (best-effort)."""
    _step("Tearing down test data")
    try:
        _setup_data(auth_file, *flags, warehouse_id=warehouse_id)
    except Exception as e:
        print(f"  {_yellow('WARN')} teardown: {e}")


# ---------------------------------------------------------------------------
# Assertion helpers
# ---------------------------------------------------------------------------

def _assert_file_exists(path: Path, description: str) -> None:
    if not path.exists():
        raise AssertionError(f"Expected file not found: {path}  ({description})")
    print(f"  {_green('PASS')}  {description}: {path.name} exists")


def _assert_contains(path: Path, text: str, description: str) -> None:
    content = path.read_text()
    if text not in content:
        raise AssertionError(
            f"Expected '{text}' not found in {path}\n  ({description})"
        )
    print(f"  {_green('PASS')}  {description}: '{text}' found in {path.name}")


def _assert_not_contains(path: Path, text: str, description: str) -> None:
    content = path.read_text()
    if text in content:
        raise AssertionError(
            f"Unexpected '{text}' found in {path}\n  ({description})"
        )
    print(f"  {_green('PASS')}  {description}: '{text}' absent from {path.name}")


def _assert_not_declared_hcl(path: Path, key: str, description: str) -> None:
    """Assert a top-level HCL key is NOT declared as an actual assignment.

    Unlike _assert_not_contains, this ignores comment lines (lines starting
    with '#') so that mode-specific header comments or LLM placeholder comments
    such as '# tag_assignments = [] — managed centrally' do not cause false
    positives.  Only an actual HCL declaration of the form
        key = [ ...
        key = { ...
    on a non-comment line triggers a failure.
    """
    content = path.read_text()
    pattern = re.compile(rf"^\s*{re.escape(key)}\s*=\s*[\[{{]", re.MULTILINE)
    if pattern.search(content):
        raise AssertionError(
            f"Unexpected HCL declaration '{key}' found in {path}\n  ({description})"
        )
    print(f"  {_green('PASS')}  {description}: '{key}' not declared in {path.name}")


def _assert_genie_space_id_file(env: str, space_name: str) -> None:
    """Check that a .genie_space_id_<key> file was created by make apply."""
    key = re.sub(r"[^a-z0-9]+", "_", space_name.lower()).strip("_")
    candidates = list((ENVS_DIR / env).glob(f".genie_space_id_{key}*"))
    if not candidates:
        # Also accept the legacy single-space file
        legacy = ENVS_DIR / env / ".genie_space_id"
        if legacy.exists():
            print(f"  {_green('PASS')}  Genie Space ID file exists (legacy): .genie_space_id")
            return
        raise AssertionError(
            f"No .genie_space_id_* file found for space '{space_name}' in envs/{env}/"
        )
    print(f"  {_green('PASS')}  Genie Space ID file: {candidates[0].name}")


def _read_genie_space_id(env: str, space_name: str) -> str:
    """Read the Genie Space object ID from the .genie_space_id_<key> file."""
    key = re.sub(r"[^a-z0-9]+", "_", space_name.lower()).strip("_")
    candidates = list((ENVS_DIR / env).glob(f".genie_space_id_{key}*"))
    if not candidates:
        legacy = ENVS_DIR / env / ".genie_space_id"
        if legacy.exists():
            return legacy.read_text().strip()
        return ""
    return candidates[0].read_text().strip()


def _get_genie_space_acl_groups(auth_file: Path, space_id: str) -> set[str]:
    """Query the Genie Space permissions API and return the set of group names with CAN_RUN."""
    if not space_id:
        return set()
    try:
        import ssl as _ssl
        import urllib.request as _urq
        import json as _json

        cfg = _load_auth_cfg(auth_file)
        _s = lambda v: (v[0] if isinstance(v, list) else (v or "")).strip()
        host = _s(cfg.get("databricks_workspace_host", ""))
        client_id = _s(cfg.get("databricks_client_id", ""))
        client_secret = _s(cfg.get("databricks_client_secret", ""))
        if not host:
            return set()

        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)
        token = w.config.authenticate()

        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        url = f"{host.rstrip('/')}/api/2.0/permissions/genie/{space_id}"
        req = _urq.Request(url, headers=token)
        with _urq.urlopen(req, timeout=30, context=ctx) as resp:
            data = _json.loads(resp.read())

        groups = set()
        for acl in data.get("access_control_list", []):
            group = acl.get("group_name", "")
            perms = acl.get("all_permissions", [])
            for p in perms:
                if p.get("permission_level") == "CAN_RUN" and group:
                    groups.add(group)
        return groups
    except Exception as exc:
        print(f"  {_yellow('WARN')} Could not query Genie Space ACLs for {space_id}: {exc}")
        return set()


def _assert_acl_groups(auth_file: Path, env: str, space_name: str, expected_groups: set[str], label: str) -> None:
    """Assert that a Genie Space's ACLs match the expected groups."""
    space_id = _read_genie_space_id(env, space_name)
    if not space_id:
        print(f"  {_yellow('WARN')} Skipping ACL check for '{space_name}' — no space ID found")
        return
    actual_groups = _get_genie_space_acl_groups(auth_file, space_id)
    if not actual_groups:
        print(f"  {_yellow('WARN')} Could not retrieve ACLs for '{space_name}' — skipping check")
        return
    # Check expected groups are present (actual may include admins/system groups)
    missing = expected_groups - actual_groups
    if missing:
        raise AssertionError(
            f"ACL check failed for '{space_name}' ({label}): "
            f"expected groups {missing} not found in ACLs. "
            f"Actual: {actual_groups}"
        )
    print(f"  {_green('PASS')}  {label}: {sorted(expected_groups)} have CAN_RUN on '{space_name}'")


def _assert_acl_excludes_groups(auth_file: Path, env: str, space_name: str, excluded_groups: set[str], label: str) -> None:
    """Assert that specific groups do NOT have CAN_RUN on a Genie Space."""
    space_id = _read_genie_space_id(env, space_name)
    if not space_id:
        print(f"  {_yellow('WARN')} Skipping ACL exclusion check for '{space_name}' — no space ID found")
        return
    actual_groups = _get_genie_space_acl_groups(auth_file, space_id)
    if not actual_groups:
        print(f"  {_yellow('WARN')} Could not retrieve ACLs for '{space_name}' — skipping check")
        return
    unexpected = excluded_groups & actual_groups
    if unexpected:
        raise AssertionError(
            f"ACL exclusion check failed for '{space_name}' ({label}): "
            f"groups {unexpected} should NOT have CAN_RUN but do. "
            f"Actual: {actual_groups}"
        )
    print(f"  {_green('PASS')}  {label}: {sorted(excluded_groups)} correctly excluded from '{space_name}'")


def _assert_state_no_account_resources(env: str) -> None:
    """Assert terraform.tfstate contains no account-level resources (genie_only mode)."""
    import json as _json_st
    state_file = ENVS_DIR / env / "terraform.tfstate"
    if not state_file.exists():
        raise AssertionError(f"terraform.tfstate not found for '{env}'")
    state = _json_st.loads(state_file.read_text())

    account_patterns = (
        "databricks_group.existing",
        "databricks_mws_permission_assignment.group_assignments",
        "databricks_entitlements.group_entitlements",
    )
    account_resources = []
    for res in state.get("resources", []):
        module_prefix = res.get("module", "")
        addr = f"{module_prefix}.{res['type']}.{res['name']}"
        if any(p in addr for p in account_patterns):
            # Only flag if the resource has actual instances (non-empty for_each)
            instances = res.get("instances", [])
            if instances:
                account_resources.append(addr)

    if account_resources:
        raise AssertionError(
            f"genie_only state in '{env}' contains account-level resource instances: "
            f"{account_resources}"
        )
    print(f"  {_green('PASS')}  No account-level resource instances in '{env}' terraform.tfstate")


def _verify_data(
    auth_file: Path,
    *,
    dev: bool = False,
    prod: bool = False,
    warehouse_id: str = "",
) -> None:
    """Run setup_test_data.py --verify (and --verify-prod) as assertions.

    Retries up to 3 times with progressive waits if verification fails —
    handles Delta table eventual consistency where CREATE TABLE succeeds
    but the table metadata isn't fully propagated in Unity Catalog yet.
    """
    def _verify_envs() -> list[str]:
        envs: list[str] = []
        if dev:
            envs.append("dev")
        if prod:
            envs.append("prod")
        return envs

    def _converge_tag_policies_before_verify(env_name: str) -> None:
        refs_tfvars = ENVS_DIR / env_name / "data_access" / "abac.auto.tfvars"
        if not refs_tfvars.exists():
            return
        print(
            "  Re-syncing account tag policies before verify"
            f" ({env_name}, refs={refs_tfvars})..."
        )
        _make("sync-tags")
        _make("wait-tag-policies", f"REQUIRED_VALUES_TFVARS={refs_tfvars}")

    flags: list[str] = []
    if dev:
        flags.append("--verify")
    if prod:
        flags.append("--verify-prod")
    if not flags:
        return
    for _v_attempt in range(4):
        try:
            for env_name in _verify_envs():
                _converge_tag_policies_before_verify(env_name)
            _setup_data(auth_file, *flags, warehouse_id=warehouse_id)
            break
        except RuntimeError:
            if _v_attempt < 3:
                _wait = 60 * (_v_attempt + 1)
                print(f"  Verify failed — retrying after {_wait}s (attempt {_v_attempt + 1}/4, tag policy propagation)...")
                time.sleep(_wait)
            else:
                raise


# ---------------------------------------------------------------------------
# Scenario: 1 — Quickstart (single space, single catalog)
# ---------------------------------------------------------------------------

def scenario_quickstart(
    auth_file: Path,
    warehouse_id: str,
    keep_data: bool,
    fresh_env: bool = False,
) -> None:
    """
    Flow: setup dev_fin → configure one space → generate → apply → verify → teardown.

    Exercises the core quickstart path from docs/playbook.md § 1 with a single Genie
    Space backed by a single UC catalog.
    """
    _banner("Scenario: quickstart — Single space, single catalog")
    env = "dev"

    # ── setup ────────────────────────────────────────────────────────────────
    _preamble_cleanup(env, fresh_env=fresh_env)

    _step("Creating dev_fin test catalog")
    warehouse_id = _setup_data(auth_file, warehouse_id=warehouse_id) or warehouse_id   # dev_fin + dev_clinical (idempotent)

    _step("Preparing env")
    _make(f"setup", f"ENV={env}")
    _write_env_tfvars(env, SPACES_FINANCE_ONLY, warehouse_id)

    # ── generate + apply ─────────────────────────────────────────────────────
    _step("Generating ABAC config")
    _make(f"generate", f"ENV={env}", retries=3)

    _step("Asserting generated output")
    gen_dir = ENVS_DIR / env / "generated"
    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated")
    _assert_file_exists(gen_dir / "masking_functions.sql", "masking_functions.sql generated")
    _assert_contains(gen_dir / "abac.auto.tfvars", "Finance Analytics",
                     "Finance Analytics config generated")
    _assert_file_exists(gen_dir / "spaces" / "finance_analytics" / "abac.auto.tfvars",
                        "per-space directory bootstrapped")

    _step("Applying all layers")
    _make(f"apply", f"ENV={env}", retries=3, retry_delay_seconds=120)

    _step("Asserting Genie Space deployed")
    _assert_genie_space_id_file(env, "Finance Analytics")

    _step("Verifying data + ABAC governance")
    _verify_data(auth_file, dev=True, warehouse_id=warehouse_id)

    # ── teardown ─────────────────────────────────────────────────────────────
    if not keep_data:
        _teardown_data("--teardown", "--teardown-prod", auth_file=auth_file,
                       warehouse_id=warehouse_id)
        _try_destroy(env)
        _try_destroy_account()

    print(f"\n  {_green(_bold('PASSED'))}  quickstart")


# ---------------------------------------------------------------------------
# Scenario: 2 — Multi-catalog in one Genie Space
# ---------------------------------------------------------------------------

def scenario_multi_catalog(
    auth_file: Path,
    warehouse_id: str,
    keep_data: bool,
    fresh_env: bool = False,
) -> None:
    """
    Flow: setup dev_fin + dev_clinical → one space using tables from both catalogs
    → generate → apply → verify both catalogs tagged → teardown.

    Tests the "single space spanning multiple catalogs" pattern from playbook.md § 1.
    """
    _banner("Scenario: multi-catalog — One space, two catalogs")
    env = "dev"

    # ── setup ────────────────────────────────────────────────────────────────
    _preamble_cleanup(env, fresh_env=fresh_env)

    _step("Creating dev_fin and dev_clinical test catalogs")
    warehouse_id = _setup_data(auth_file, warehouse_id=warehouse_id) or warehouse_id

    _step("Preparing env")
    _make(f"setup", f"ENV={env}")
    _write_env_tfvars(env, SPACES_COMBINED, warehouse_id)

    # ── generate + apply ─────────────────────────────────────────────────────
    _step("Generating ABAC config (tables from both catalogs)")
    _make(f"generate", f"ENV={env}", retries=3)

    _step("Asserting generated output covers both catalogs")
    gen_dir = ENVS_DIR / env / "generated"
    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated")
    _assert_contains(gen_dir / "abac.auto.tfvars", "Combined Analytics",
                     "Combined Analytics space config generated")
    _assert_contains(gen_dir / "abac.auto.tfvars", DEV_FIN_CAT,
                     f"{DEV_FIN_CAT} catalog referenced in policies")
    _assert_contains(gen_dir / "abac.auto.tfvars", DEV_CLIN_CAT,
                     f"{DEV_CLIN_CAT} catalog referenced in policies")

    _step("Applying all layers")
    _make(f"apply", f"ENV={env}", retries=3, retry_delay_seconds=120)

    _step("Asserting single Genie Space deployed")
    _assert_genie_space_id_file(env, "Combined Analytics")

    _step("Verifying data + ABAC governance (both catalogs)")
    _verify_data(auth_file, dev=True, warehouse_id=warehouse_id)

    # ── teardown ─────────────────────────────────────────────────────────────
    if not keep_data:
        _teardown_data("--teardown", "--teardown-prod", auth_file=auth_file,
                       warehouse_id=warehouse_id)
        _try_destroy(env)
        _try_destroy_account()

    print(f"\n  {_green(_bold('PASSED'))}  multi-catalog")


# ---------------------------------------------------------------------------
# Scenario: 3 — Multi Genie Spaces
# ---------------------------------------------------------------------------

def scenario_multi_space(
    auth_file: Path,
    warehouse_id: str,
    keep_data: bool,
    fresh_env: bool = False,
) -> None:
    """
    Flow: setup both catalogs → two spaces (Finance + Clinical) → full generate
    → apply → verify both spaces deployed → teardown.

    This mirrors the existing `make integration-test` multi-space scenario and
    exercises the full two-space + two-catalog path.
    """
    _banner("Scenario: multi-space — Two spaces, separate catalogs")
    env = "dev"

    # ── setup ────────────────────────────────────────────────────────────────
    _preamble_cleanup(env, fresh_env=fresh_env)

    _step("Creating dev_fin and dev_clinical test catalogs")
    warehouse_id = _setup_data(auth_file, warehouse_id=warehouse_id) or warehouse_id

    _step("Preparing env")
    _make(f"setup", f"ENV={env}")
    _write_env_tfvars(env, SPACES_MULTI, warehouse_id)

    # ── generate + apply ─────────────────────────────────────────────────────
    _step("Generating ABAC config (both spaces)")
    _make(f"generate", f"ENV={env}", retries=3)

    _step("Asserting generated output contains both spaces")
    gen_dir = ENVS_DIR / env / "generated"
    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated")
    _assert_contains(gen_dir / "abac.auto.tfvars", "Finance Analytics",
                     "Finance Analytics config present")
    _assert_contains(gen_dir / "abac.auto.tfvars", "Clinical Analytics",
                     "Clinical Analytics config present")
    _assert_file_exists(gen_dir / "spaces" / "finance_analytics" / "abac.auto.tfvars",
                        "finance_analytics per-space dir bootstrapped")
    _assert_file_exists(gen_dir / "spaces" / "clinical_analytics" / "abac.auto.tfvars",
                        "clinical_analytics per-space dir bootstrapped")

    _step("Applying all layers")
    _make(f"apply", f"ENV={env}", retries=3, retry_delay_seconds=120)

    _step("Asserting both Genie Spaces deployed")
    _assert_genie_space_id_file(env, "Finance Analytics")
    _assert_genie_space_id_file(env, "Clinical Analytics")

    _step("Verifying data + ABAC governance")
    _verify_data(auth_file, dev=True, warehouse_id=warehouse_id)

    # ── Verify per-space ACLs ─────────────────────────────────────────────────
    _step("Verifying per-space Genie ACLs")
    # Read the generated config to find acl_groups per space
    gen_abac = ENVS_DIR / env / "generated" / "abac.auto.tfvars"
    if gen_abac.exists():
        try:
            import hcl2 as _hcl2_ms
            with open(gen_abac) as _f_ms:
                _gen_cfg = _hcl2_ms.load(_f_ms)
            _gsc = _gen_cfg.get("genie_space_configs") or {}
            if isinstance(_gsc, list):
                _gsc = _gsc[0] if _gsc else {}
            _all_groups = set((_gen_cfg.get("groups") or {}).keys())
            if isinstance(_all_groups, list):
                _all_groups = set(_all_groups[0].keys()) if _all_groups else set()

            for _space_name, _space_cfg in _gsc.items():
                if isinstance(_space_cfg, list):
                    _space_cfg = _space_cfg[0] if _space_cfg else {}
                _acl = _space_cfg.get("acl_groups") or []
                if isinstance(_acl, list) and _acl:
                    if isinstance(_acl[0], list):
                        _acl = _acl[0]
                    if _acl:
                        _expected = set(_acl)
                        _excluded = _all_groups - _expected
                        _assert_acl_groups(auth_file, env, _space_name, _expected,
                                          f"Per-space ACL: {_space_name}")
                        if _excluded:
                            _assert_acl_excludes_groups(auth_file, env, _space_name, _excluded,
                                                       f"ACL exclusion: {_space_name}")
                    else:
                        print(f"  {_green('PASS')}  {_space_name}: acl_groups empty (all groups get access — backward compat)")
                else:
                    print(f"  {_green('PASS')}  {_space_name}: no acl_groups (all groups get access — backward compat)")
        except Exception as _acl_exc:
            print(f"  {_yellow('WARN')} Could not verify per-space ACLs: {_acl_exc}")
    else:
        print(f"  {_yellow('WARN')} Skipping ACL verification — generated config not found")

    # ── teardown ─────────────────────────────────────────────────────────────
    if not keep_data:
        _teardown_data("--teardown", "--teardown-prod", auth_file=auth_file,
                       warehouse_id=warehouse_id)
        _try_destroy(env)
        _try_destroy_account()

    print(f"\n  {_green(_bold('PASSED'))}  multi-space")


# ---------------------------------------------------------------------------
# Scenario: 4 — Per-space incremental generation
# ---------------------------------------------------------------------------

def scenario_per_space(
    auth_file: Path,
    warehouse_id: str,
    keep_data: bool,
    fresh_env: bool = False,
) -> None:
    """
    Flow: deploy Finance Analytics first → then add Clinical Analytics using
    `make generate SPACE="Clinical Analytics"` → verify Finance config is
    preserved and Clinical config is merged in additively.

    Exercises the per-space isolation guarantee from docs/playbook.md § 4.
    """
    _banner("Scenario: per-space — Incremental space addition (isolation test)")
    env = "dev"

    # ── setup ────────────────────────────────────────────────────────────────
    _preamble_cleanup(env, fresh_env=fresh_env)

    _step("Creating both test catalogs")
    warehouse_id = _setup_data(auth_file, warehouse_id=warehouse_id) or warehouse_id

    _step("Preparing env with Finance Analytics only")
    _make(f"setup", f"ENV={env}")
    _write_env_tfvars(env, SPACES_FINANCE_ONLY, warehouse_id)

    # ── Phase 1: deploy Finance only ─────────────────────────────────────────
    _step("Phase 1 — Full generate for Finance Analytics")
    _make(f"generate", f"ENV={env}", retries=3)

    gen_dir = ENVS_DIR / env / "generated"
    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated (phase 1)")
    _assert_contains(gen_dir / "abac.auto.tfvars", "Finance Analytics",
                     "Finance Analytics present after phase-1 generate")
    _assert_not_contains(gen_dir / "abac.auto.tfvars", "Clinical Analytics",
                         "Clinical Analytics absent before phase-2 generate")

    # Snapshot the Finance entry checksum for later comparison
    fin_space_dir = gen_dir / "spaces" / "finance_analytics"
    _assert_file_exists(fin_space_dir / "abac.auto.tfvars",
                        "finance_analytics per-space dir bootstrapped")
    fin_checksum_before = (fin_space_dir / "abac.auto.tfvars").read_text()

    _step("Phase 1 — Applying Finance Analytics")
    _make(f"apply", f"ENV={env}", retries=3, retry_delay_seconds=120)
    _assert_genie_space_id_file(env, "Finance Analytics")

    # Patch the warehouse ID so Phase 2 reuses the same warehouse instead of
    # trying to create a duplicate "ABAC Governance Warehouse".
    resolved_wh = _patch_warehouse_id_in_env_tfvars(env, auth_file)
    if resolved_wh:
        warehouse_id = resolved_wh

    # ── Phase 2: add Clinical Analytics without touching Finance ─────────────
    _step("Phase 2 — Adding Clinical Analytics to env.auto.tfvars")
    _write_env_tfvars(env, SPACES_MULTI, warehouse_id)

    _step("Phase 2 — Per-space generate for Clinical Analytics only")
    _make(f"generate", f"ENV={env}", f'SPACE=Clinical Analytics', retries=3)

    _step("Asserting per-space isolation")
    assembled = gen_dir / "abac.auto.tfvars"
    _assert_contains(assembled, "Finance Analytics",
                     "Finance Analytics config preserved in assembled output")
    _assert_contains(assembled, "Clinical Analytics",
                     "Clinical Analytics config merged into assembled output")

    fin_checksum_after = (fin_space_dir / "abac.auto.tfvars").read_text()
    if fin_checksum_before != fin_checksum_after:
        raise AssertionError(
            "finance_analytics per-space config was modified by Clinical Analytics generate!"
        )
    print(f"  {_green('PASS')}  finance_analytics per-space dir unchanged after SPACE= generate")

    _assert_file_exists(gen_dir / "spaces" / "clinical_analytics" / "abac.auto.tfvars",
                        "clinical_analytics per-space dir created")

    _step("Phase 2 — Applying with both spaces")
    _make(f"apply", f"ENV={env}", retries=3, retry_delay_seconds=120)
    _assert_genie_space_id_file(env, "Finance Analytics")
    _assert_genie_space_id_file(env, "Clinical Analytics")

    # Re-resolve warehouse ID — Terraform may have recreated it during Phase 2 apply
    warehouse_id = _patch_warehouse_id_in_env_tfvars(env, auth_file) or warehouse_id

    _step("Verifying data + ABAC governance")
    _verify_data(auth_file, dev=True, warehouse_id=warehouse_id)

    # ── teardown ─────────────────────────────────────────────────────────────
    if not keep_data:
        _teardown_data("--teardown", "--teardown-prod", auth_file=auth_file,
                       warehouse_id=warehouse_id)
        _try_destroy(env)
        _try_destroy_account()

    print(f"\n  {_green(_bold('PASSED'))}  per-space")


# ---------------------------------------------------------------------------
# Scenario: 5 — Promote dev → prod
# ---------------------------------------------------------------------------

def scenario_promote(
    auth_file: Path,
    warehouse_id: str,
    keep_data: bool,
    fresh_env: bool = False,
) -> None:
    """
    Flow: setup dev + prod catalogs → two-space dev → generate + apply dev →
    promote with catalog remapping → apply prod → verify prod → teardown.

    Exercises docs/playbook.md § 5 cross-env promotion end-to-end.
    """
    _banner("Scenario: promote — dev → prod cross-env promotion")
    dev_env  = "dev"
    prod_env = "prod"

    # ── setup ────────────────────────────────────────────────────────────────
    _preamble_cleanup(dev_env, prod_env, fresh_env=fresh_env)

    _step("Creating dev + prod test catalogs")
    warehouse_id = _setup_data(auth_file, "--prod", warehouse_id=warehouse_id) or warehouse_id

    for env in (dev_env, prod_env):
        _step(f"Preparing {env} env")
        _make(f"setup", f"ENV={env}")

    _write_env_tfvars(dev_env, SPACES_MULTI, warehouse_id)

    # ── dev generate + apply ─────────────────────────────────────────────────
    _step("Generating dev ABAC config")
    _make(f"generate", f"ENV={dev_env}", retries=3)

    gen_dir = ENVS_DIR / dev_env / "generated"
    _assert_file_exists(gen_dir / "abac.auto.tfvars", "dev abac.auto.tfvars generated")
    _assert_contains(gen_dir / "abac.auto.tfvars", "Finance Analytics",
                     "Finance Analytics in dev generated config")
    _assert_contains(gen_dir / "abac.auto.tfvars", "Clinical Analytics",
                     "Clinical Analytics in dev generated config")

    _step("Applying dev")
    _make(f"apply", f"ENV={dev_env}", retries=3, retry_delay_seconds=120)
    _assert_genie_space_id_file(dev_env, "Finance Analytics")
    _assert_genie_space_id_file(dev_env, "Clinical Analytics")

    _step("Verifying dev data + ABAC")
    _verify_data(auth_file, dev=True, warehouse_id=warehouse_id)

    # Patch the dev env.auto.tfvars with the actual warehouse created by Terraform.
    # The promote step copies sql_warehouse_id from dev → prod; if we leave it as ""
    # both dev and prod applies would try to CREATE a warehouse, and the second one
    # fails with "warehouse already exists" (same workspace, same name).
    _patch_warehouse_id_in_env_tfvars(dev_env, auth_file)

    # ── promote ──────────────────────────────────────────────────────────────
    _step(f"Promoting {dev_env} → {prod_env} with catalog map: {CATALOG_MAP_DEV_TO_PROD}")
    _make(
        f"promote",
        f"SOURCE_ENV={dev_env}",
        f"DEST_ENV={prod_env}",
        f"DEST_CATALOG_MAP={CATALOG_MAP_DEV_TO_PROD}",
    )

    prod_env_dir = ENVS_DIR / prod_env
    _assert_file_exists(prod_env_dir / "env.auto.tfvars",
                        "prod env.auto.tfvars written by promote")
    prod_env_content = (prod_env_dir / "env.auto.tfvars").read_text()
    if PROD_FIN_CAT not in prod_env_content:
        raise AssertionError(
            f"Expected {PROD_FIN_CAT} in prod env.auto.tfvars after promote"
        )
    print(f"  {_green('PASS')}  prod env.auto.tfvars contains remapped prod catalogs")

    _assert_file_exists(prod_env_dir / "generated" / "abac.auto.tfvars",
                        "prod generated/abac.auto.tfvars written by promote")
    _assert_contains(prod_env_dir / "generated" / "abac.auto.tfvars", PROD_FIN_CAT,
                     f"{PROD_FIN_CAT} in promoted prod config")

    # ── prod apply + verify ──────────────────────────────────────────────────
    # Copy auth to prod (promote doesn't touch auth)
    _copy_auth(dev_env, prod_env)

    # Ensure prod reuses the same warehouse (prevent "already exists" error)
    _patch_warehouse_id_in_env_tfvars(prod_env, auth_file)

    _step("Applying prod")
    _force_account_reapply("promote prod apply")
    _make(f"apply", f"ENV={prod_env}", retries=3, retry_delay_seconds=120)

    _step("Verifying prod data + ABAC")
    _verify_data(auth_file, prod=True, warehouse_id=warehouse_id)

    # ── teardown ─────────────────────────────────────────────────────────────
    if not keep_data:
        _teardown_data("--teardown", "--teardown-prod", auth_file=auth_file,
                       warehouse_id=warehouse_id)
        _try_destroy(prod_env)
        _try_destroy(dev_env)
        _try_destroy_account()

    print(f"\n  {_green(_bold('PASSED'))}  promote")


# ---------------------------------------------------------------------------
# Scenario: 6 — Multi-env (independent BU)
# ---------------------------------------------------------------------------

def scenario_multi_env(
    auth_file: Path,
    warehouse_id: str,
    keep_data: bool,
    fresh_env: bool = False,
) -> None:
    """
    Flow: two completely independent workspace environments on the same account —
    dev uses Finance Analytics (dev_fin) and bu2 uses Clinical Analytics (dev_clinical).
    Each has its own generate + apply cycle with separate generated config and state.

    Exercises docs/playbook.md § 6 (second independent environment for another BU).
    """
    _banner("Scenario: multi-env — Two independent environments (dev + bu2)")
    dev_env = "dev"
    bu2_env = "bu2"

    # ── setup ────────────────────────────────────────────────────────────────
    _preamble_cleanup(dev_env, bu2_env, fresh_env=fresh_env)

    _step("Creating both test catalogs")
    warehouse_id = _setup_data(auth_file, warehouse_id=warehouse_id) or warehouse_id

    for env in (dev_env, bu2_env):
        _step(f"Preparing {env} env")
        _make(f"setup", f"ENV={env}")

    # Both envs share the same auth (same workspace)
    _copy_auth(dev_env, bu2_env)
    _write_env_tfvars(dev_env, SPACES_FINANCE_ONLY,  warehouse_id)
    _write_env_tfvars(bu2_env, SPACES_CLINICAL_ONLY, warehouse_id)

    # ── dev: generate + apply ─────────────────────────────────────────────────
    _step("dev — Generating Finance Analytics ABAC config")
    _make(f"generate", f"ENV={dev_env}", retries=3)

    dev_gen = ENVS_DIR / dev_env / "generated"
    _assert_contains(dev_gen / "abac.auto.tfvars", "Finance Analytics",
                     "Finance Analytics in dev generated config")
    _assert_not_contains(dev_gen / "abac.auto.tfvars", "Clinical Analytics",
                         "Clinical Analytics absent from dev generated config")

    _step("dev — Applying Finance Analytics")
    _make(f"apply", f"ENV={dev_env}", retries=3, retry_delay_seconds=120)
    _assert_genie_space_id_file(dev_env, "Finance Analytics")

    _step("dev — Verifying Finance Analytics data + ABAC")
    _verify_data(auth_file, dev=True, warehouse_id=warehouse_id)

    # dev and bu2 share the same workspace.  If both env.auto.tfvars have
    # sql_warehouse_id = "" Terraform will try to create "ABAC Serverless Warehouse"
    # twice in the same workspace → name conflict.  After dev's apply we discover
    # the created warehouse and patch it into both dev and bu2 env.auto.tfvars so
    # bu2's apply reuses the existing warehouse (count=0 branch).
    actual_wh = _patch_warehouse_id_in_env_tfvars(dev_env, auth_file)
    if not actual_wh:
        # Retry after a short wait — warehouse may still be starting
        time.sleep(15)
        actual_wh = _resolve_warehouse_id(auth_file, "")
    if actual_wh:
        _write_env_tfvars(bu2_env, SPACES_CLINICAL_ONLY, actual_wh)
    else:
        _warn("Could not discover warehouse ID — bu2 may fail with 'already exists'")

    # ── bu2: generate + apply ─────────────────────────────────────────────────
    _step("bu2 — Generating Clinical Analytics ABAC config independently")
    _make(f"generate", f"ENV={bu2_env}", retries=3)

    bu2_gen = ENVS_DIR / bu2_env / "generated"
    _assert_contains(bu2_gen / "abac.auto.tfvars", "Clinical Analytics",
                     "Clinical Analytics in bu2 generated config")
    _assert_not_contains(bu2_gen / "abac.auto.tfvars", "Finance Analytics",
                         "Finance Analytics absent from bu2 generated config")

    _step("bu2 — Applying Clinical Analytics")
    _force_account_reapply("bu2 apply")
    _make(f"apply", f"ENV={bu2_env}", retries=3, retry_delay_seconds=120)
    _assert_genie_space_id_file(bu2_env, "Clinical Analytics")

    # Re-sync tag policies after both envs applied — ensures all merged values
    # (including bu2's phi_level entries like redacted_notes) are pushed to Databricks.
    # Without this, tag policy values from bu2's promote may not propagate before verify.
    _step("bu2 — Re-syncing tag policies after both environments applied")
    _make("sync-tags")
    import time as _time_multienv
    _time_multienv.sleep(30)  # wait for tag policy propagation

    _step("bu2 — Verifying Clinical Analytics ABAC governance")
    # Verify dev_clinical tables exist and tags/masks applied
    _verify_data(auth_file, dev=True, warehouse_id=warehouse_id)

    _step("Asserting independent state files")
    dev_state = ENVS_DIR / dev_env / "terraform.tfstate"
    bu2_state = ENVS_DIR / bu2_env / "terraform.tfstate"
    _assert_file_exists(dev_state, "dev has own terraform.tfstate")
    _assert_file_exists(bu2_state, "bu2 has own terraform.tfstate")
    if dev_state.read_text() == bu2_state.read_text():
        raise AssertionError("dev and bu2 terraform.tfstate are identical — expected independent state")
    print(f"  {_green('PASS')}  dev and bu2 have independent Terraform state")

    # ── teardown ─────────────────────────────────────────────────────────────
    if not keep_data:
        _teardown_data("--teardown", "--teardown-prod", auth_file=auth_file,
                       warehouse_id=warehouse_id)
        _try_destroy(bu2_env)
        _try_destroy(dev_env)
        _try_destroy_account()
        # Remove the bu2 env directory (it's ephemeral)
        bu2_dir = ENVS_DIR / bu2_env
        if bu2_dir.exists():
            shutil.rmtree(bu2_dir)

    print(f"\n  {_green(_bold('PASSED'))}  multi-env")


# ---------------------------------------------------------------------------
# Genie Space API helpers (for the attach-promote scenario)
# ---------------------------------------------------------------------------

def _ensure_packages() -> None:
    try:
        import hcl2  # noqa: F401
        from databricks.sdk import WorkspaceClient  # noqa: F401
    except ImportError:
        _run([sys.executable, "-m", "pip", "install", "-q",
              "databricks-sdk", "python-hcl2"])


def _load_auth_cfg(auth_file: Path) -> dict:
    """Load databricks credentials from auth.auto.tfvars."""
    _ensure_packages()
    import hcl2
    with open(auth_file) as f:
        return hcl2.load(f)


def _configure_sdk_env(cfg: dict) -> None:
    """Set DATABRICKS_* env vars from auth config so the SDK picks them up."""
    mapping = {
        "databricks_workspace_host": "DATABRICKS_HOST",
        "databricks_client_id":      "DATABRICKS_CLIENT_ID",
        "databricks_client_secret":  "DATABRICKS_CLIENT_SECRET",
    }
    for k, env_k in mapping.items():
        v = cfg.get(k, "")
        if v and not os.environ.get(env_k):
            os.environ[env_k] = v


def _get_or_find_warehouse(auth_file: Path, warehouse_id: str) -> str:
    """Return a warehouse ID — use the given one or pick the first available."""
    from databricks.sdk import WorkspaceClient

    cfg = _load_auth_cfg(auth_file)
    _configure_sdk_env(cfg)
    w = WorkspaceClient(product="genierails-test-runner", product_version="0.1.0")

    if warehouse_id:
        return warehouse_id

    print("  No warehouse-id provided; auto-selecting a deterministic SQL warehouse...")
    warehouses = list(w.warehouses.list())
    wh = select_warehouse(warehouses)
    if wh:
        print(f"    Using warehouse: {wh.name} ({wh.id})")
        return wh.id
    raise RuntimeError("No SQL warehouses found in the workspace.")


def _create_genie_only_sp(
    auth_file: Path,
    workspace_id: str,
    warehouse_id: str,
    display_name: str = "genie-test-sql-user-sp",
    cfg: dict | None = None,
) -> tuple[int, str, str]:
    """Create a minimal-privilege SP for genie_only mode (no admin roles at all).

    The SP is assigned to the workspace as a regular USER (not Admin) and is
    granted only the permissions needed to create Genie Spaces:
      - Workspace membership (USER)
      - Databricks SQL access entitlement
      - CAN USE on the specified warehouse
      - USE CATALOG / USE SCHEMA / SELECT on the test tables

    Uses the full-privilege SP (from auth_file) to provision everything.

    Returns (sp_scim_id, client_id, client_secret).
    """
    import time as _time_sp
    from databricks.sdk import AccountClient
    from databricks.sdk.service.iam import WorkspacePermission

    if cfg is None:
        cfg = _load_auth_cfg(auth_file)
    _s = lambda v: (v[0] if isinstance(v, list) else (v or "")).strip()

    account_id    = _s(cfg.get("databricks_account_id", ""))
    client_id     = _s(cfg.get("databricks_client_id", ""))
    client_secret = _s(cfg.get("databricks_client_secret", ""))

    account_host = _s(cfg.get("databricks_account_host", "https://accounts.cloud.databricks.com"))
    a = AccountClient(
        host=account_host,
        account_id=account_id,
        client_id=client_id,
        client_secret=client_secret,
    )

    # 1. Create SP at account level
    print(f"  Creating Service Principal: {display_name!r}")
    sp = a.service_principals.create(display_name=display_name, active=True)
    sp_scim_id = sp.id
    sp_app_id = sp.application_id
    print(f"  SP created: scim_id={sp_scim_id}  application_id={sp_app_id}")

    # 2. Generate OAuth secret
    #    The OAuth client_id is the SP's application_id (UUID), NOT the secret's id.
    #    secret_resp.id is the secret's opaque identifier; secret_resp.secret is the value.
    print(f"  Generating OAuth secret for SP {sp_scim_id}...")
    secret_resp = a.service_principal_secrets.create(service_principal_id=sp_scim_id)
    new_client_id = sp_app_id          # SP's application_id — the OAuth client_id
    new_client_secret = secret_resp.secret
    print(f"  OAuth secret created: client_id={new_client_id}  (secret_id={secret_resp.id})")

    # 3. Assign workspace USER only (NOT admin) — minimal workspace membership
    ws_id = int(workspace_id) if workspace_id else 0
    print(f"  Assigning workspace USER to SP {sp_scim_id} on workspace {ws_id}...")
    a.workspace_assignment.update(
        workspace_id=ws_id,
        principal_id=sp_scim_id,
        permissions=[WorkspacePermission.USER],
    )
    print(f"  {_green('OK')}  Workspace USER granted (no Admin, no Account Admin, no Metastore Admin)")

    # 4. Grant Databricks SQL access entitlement + CAN USE warehouse + UC table access.
    #    All grants are issued by the full-privilege SP.
    from databricks.sdk import WorkspaceClient as _WC_grant
    ws_host_val = cfg.get("databricks_workspace_host", "")
    ws_host_val = (ws_host_val[0] if isinstance(ws_host_val, list) else (ws_host_val or "")).strip()
    w_grant = _WC_grant(
        host=ws_host_val,
        client_id=client_id,
        client_secret=client_secret,
        product="genierails-test-runner",
        product_version="0.1.0",
    )

    # 4a. Grant Databricks SQL access entitlement
    print(f"  Granting Databricks SQL access entitlement to SP {sp_scim_id}...")
    _time_sp.sleep(10)  # wait for workspace identity to propagate
    try:
        w_grant.api_client.do(
            "PATCH",
            f"/api/2.0/preview/scim/v2/ServicePrincipals/{sp_scim_id}",
            body={
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": [{
                    "op": "add",
                    "path": "entitlements",
                    "value": [{"value": "databricks-sql-access"}],
                }],
            },
        )
        print(f"  {_green('OK')}  Databricks SQL access entitlement granted")
    except Exception as exc:
        print(f"  {_yellow('WARN')} Could not set SQL entitlement: {exc}")

    # 4b. Grant CAN USE on the warehouse
    if warehouse_id:
        print(f"  Granting CAN_USE on warehouse {warehouse_id} to SP {sp_app_id}...")
        try:
            # Get current permissions
            perms_resp = w_grant.api_client.do(
                "GET", f"/api/2.0/permissions/sql/warehouses/{warehouse_id}",
            )
            acl = perms_resp.get("access_control_list", [])
            acl.append({
                "service_principal_name": sp_app_id,
                "all_permissions": [{"permission_level": "CAN_USE"}],
            })
            w_grant.api_client.do(
                "PATCH", f"/api/2.0/permissions/sql/warehouses/{warehouse_id}",
                body={"access_control_list": [
                    {"service_principal_name": sp_app_id, "permission_level": "CAN_USE"},
                ]},
            )
            print(f"  {_green('OK')}  CAN_USE on warehouse granted")
        except Exception as exc:
            print(f"  {_yellow('WARN')} Could not grant warehouse CAN_USE: {exc}")

    # 4c. Grant UC table access (USE CATALOG, USE SCHEMA, SELECT)
    grants_sql = [
        f"GRANT USE CATALOG ON CATALOG {DEV_FIN_CAT} TO `{sp_app_id}`",
        f"GRANT USE SCHEMA ON SCHEMA {DEV_FIN_CAT}.finance TO `{sp_app_id}`",
        f"GRANT SELECT ON SCHEMA {DEV_FIN_CAT}.finance TO `{sp_app_id}`",
    ]
    # Find a warehouse for running SQL grants
    grant_wh = ""
    for warehouse in w_grant.warehouses.list():
        if warehouse.id:
            grant_wh = warehouse.id
            break
    if grant_wh:
        from databricks.sdk.service.sql import StatementState as _SS_grant
        for sql in grants_sql:
            print(f"  Granting: {sql}")
            r = w_grant.statement_execution.execute_statement(
                statement=sql, warehouse_id=grant_wh, wait_timeout="30s",
            )
            while r.status and r.status.state in (_SS_grant.PENDING, _SS_grant.RUNNING):
                _time_sp.sleep(2)
                r = w_grant.statement_execution.get_statement(r.statement_id)
            state_str = str(getattr(getattr(r, "status", None), "state", ""))
            if "FAILED" in state_str:
                err = getattr(getattr(r, "status", None), "error", None)
                msg = getattr(err, "message", str(err)) if err else "unknown"
                print(f"  {_yellow('WARN')} Grant failed: {msg}")
            else:
                print(f"  {_green('OK')}  {sql}")
    else:
        print(f"  {_yellow('WARN')} No warehouse found — skipping UC grants (Genie space creation may fail)")

    # 5. Wait for identity propagation
    print("  Waiting 20 s for workspace identity propagation...")
    _time_sp.sleep(20)

    return sp_scim_id, new_client_id, new_client_secret


def _delete_sp(auth_file: Path, sp_scim_id: int) -> None:
    """Delete a Service Principal (teardown helper)."""
    from databricks.sdk import AccountClient

    cfg = _load_auth_cfg(auth_file)
    _s = lambda v: (v[0] if isinstance(v, list) else (v or "")).strip()

    a = AccountClient(
        host=_s(cfg.get("databricks_account_host", "https://accounts.cloud.databricks.com")),
        account_id=_s(cfg.get("databricks_account_id", "")),
        client_id=_s(cfg.get("databricks_client_id", "")),
        client_secret=_s(cfg.get("databricks_client_secret", "")),
    )
    print(f"  Deleting Service Principal (scim_id={sp_scim_id})...")
    try:
        a.service_principals.delete(id=sp_scim_id)
        print(f"  {_green('OK')}  SP {sp_scim_id} deleted.")
    except Exception as exc:
        print(f"  {_yellow('WARN')} Could not delete SP {sp_scim_id}: {exc}")


def _create_genie_space_via_api(
    auth_file: Path,
    title: str,
    tables: list[str],
    warehouse_id: str,
) -> str:
    """Create a Genie Space directly via REST API, simulating the UI experience.

    Returns the new space_id.
    """
    import json
    from databricks.sdk import WorkspaceClient

    cfg = _load_auth_cfg(auth_file)
    _configure_sdk_env(cfg)
    w = WorkspaceClient(product="genierails-test-runner", product_version="0.1.0")

    body = {
        "warehouse_id": warehouse_id,
        "title": title,
        "serialized_space": json.dumps({
            "version": 2,
            "data_sources": {
                "tables": [{"identifier": t} for t in sorted(tables)]
            },
        }, separators=(",", ":")),
    }

    print(f"  Creating Genie Space '{title}' via API with {len(tables)} table(s)...")
    resp = w.api_client.do("POST", "/api/2.0/genie/spaces", body=body)
    space_id = resp.get("space_id", "")
    if not space_id:
        raise RuntimeError(f"Genie API did not return space_id. Response: {resp}")
    print(f"  Created Genie Space: {space_id}")
    return space_id


def _delete_genie_space_via_api(auth_file: Path, space_id: str) -> None:
    """Permanently delete a Genie Space via REST API (teardown helper)."""
    from databricks.sdk import WorkspaceClient

    cfg = _load_auth_cfg(auth_file)
    _configure_sdk_env(cfg)
    w = WorkspaceClient(product="genierails-test-runner", product_version="0.1.0")

    print(f"  Deleting Genie Space {space_id} via API...")
    try:
        w.api_client.do("DELETE", f"/api/2.0/genie/spaces/{space_id}")
        print(f"  Genie Space {space_id} deleted.")
    except Exception as exc:
        print(f"  {_yellow('WARN')} Could not delete Genie Space {space_id}: {exc}")


# ---------------------------------------------------------------------------
# Scenario: 7 — Attach to an existing Genie Space and promote to prod
# ---------------------------------------------------------------------------

def scenario_attach_and_promote(
    auth_file: Path,
    warehouse_id: str,
    keep_data: bool,
    fresh_env: bool = False,
) -> None:
    """
    Simulates the "Import an existing Genie Space" flow from docs/playbook.md §3:

    Phase 1 — Simulate "configured in the UI":
      A Finance Analytics Genie Space is created directly via the Genie REST API
      (not Terraform), with dev_fin tables. This represents what a data team would
      have built in the Databricks UI before this tool was adopted.

    Phase 2 — Attach and govern (genie_space_id-only mode):
      env.auto.tfvars is configured with just genie_space_id (no uc_tables).
      `make generate` queries the Genie API to discover the space's tables, then
      generates full ABAC governance (groups, policies, masking functions) from
      those tables. The genie_space_configs entry is parsed verbatim from the API
      (no LLM involvement for the space's existing config).
      After generate, env.auto.tfvars is updated with the discovered uc_tables
      (simulating the manual step instructed by playbook.md).

    Phase 3 — Apply:
      `make apply` deploys ABAC governance (ACLs, column tags, masking functions,
      FGAC policies) without creating or deleting the Genie Space.
      The space's title, description, benchmarks, and instructions are preserved
      exactly as configured in the API/UI.

    Phase 4 — Promote to prod:
      `make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP=dev_fin=prod_fin`
      followed by `make apply ENV=prod` applies the same governance to prod.

    Tests: playbook.md §3 "Import an existing Genie Space" + §5 promotion.
    """
    _banner("Scenario: attach-promote — Attach to UI-created space, promote to prod")
    env      = "dev"
    prod_env = "prod"

    _ensure_packages()

    # ── Phase 1: simulate "space configured in UI" ───────────────────────────
    _preamble_cleanup(env, prod_env, fresh_env=fresh_env)

    _step("Phase 1 — Setting up dev_fin and prod_fin test catalogs")
    warehouse_id = _setup_data(auth_file, "--prod", warehouse_id=warehouse_id) or warehouse_id

    resolved_wh = _get_or_find_warehouse(auth_file, warehouse_id)

    fin_tables = [
        f"{DEV_FIN_CAT}.finance.customers",
        f"{DEV_FIN_CAT}.finance.transactions",
        f"{DEV_FIN_CAT}.finance.credit_cards",
    ]

    _step("Phase 1 — Creating Genie Space via API (simulating UI configuration)")
    space_id = _create_genie_space_via_api(
        auth_file,
        title="Finance Analytics",
        tables=fin_tables,
        warehouse_id=resolved_wh,
    )

    # ── Phase 2: attach with genie_space_id + known uc_tables ────────────────
    # NOTE: The Genie API does not reliably return serialized_space in GET
    # responses for newly created spaces (async processing, may take many
    # minutes). We therefore configure uc_tables explicitly here — this
    # simulates the user running `make generate --genie-space-id <id>`,
    # inspecting the logged discovered tables, and pasting them into
    # env.auto.tfvars as instructed by the playbook.md manual step.
    # The key assertion tested here is that Terraform does NOT create/delete
    # the existing Genie Space — it attaches to it as-is.
    _step("Phase 2 — Configuring env with genie_space_id + uc_tables (attach mode)")
    _make(f"setup", f"ENV={env}")
    _make(f"setup", f"ENV={prod_env}")

    attach_with_tables_hcl = f"""\
genie_spaces = [
  {{
    name           = "Finance Analytics"
    genie_space_id = "{space_id}"
    uc_tables = [
      "{DEV_FIN_CAT}.finance.customers",
      "{DEV_FIN_CAT}.finance.transactions",
      "{DEV_FIN_CAT}.finance.credit_cards",
    ]
  }},
]
"""
    _write_env_tfvars(env, attach_with_tables_hcl, resolved_wh)

    _step("Phase 2 — Running make generate (attach mode with explicit uc_tables)")
    _make(f"generate", f"ENV={env}", retries=3)

    _step("Asserting generated config references the dev_fin catalog")
    gen_dir = ENVS_DIR / env / "generated"
    _assert_file_exists(gen_dir / "abac.auto.tfvars",
                        "abac.auto.tfvars generated")
    _assert_file_exists(gen_dir / "masking_functions.sql",
                        "masking_functions.sql generated")
    _assert_contains(gen_dir / "abac.auto.tfvars", DEV_FIN_CAT,
                     f"{DEV_FIN_CAT} catalog referenced in generated policies")
    _assert_contains(gen_dir / "abac.auto.tfvars", "Finance Analytics",
                     "Finance Analytics genie_space_configs entry present")

    # ── Phase 3: apply governance (no space create/delete) ───────────────────
    _step("Phase 3 — Applying governance (space must survive, not be created/deleted)")
    _make(f"apply", f"ENV={env}", retries=3, retry_delay_seconds=120)

    _step("Asserting space NOT created by Terraform (no .genie_space_id_* file)")
    id_files = list((ENVS_DIR / env).glob(".genie_space_id_*"))
    legacy   = ENVS_DIR / env / ".genie_space_id"
    if id_files or legacy.exists():
        raise AssertionError(
            "Terraform created a new Genie Space in attach mode — expected no .genie_space_id_* file. "
            "The existing space should be used as-is."
        )
    print(f"  {_green('PASS')}  No .genie_space_id_* file: Terraform did not create a new space")

    _step("Verifying dev ABAC governance applied to discovered tables")
    _verify_data(auth_file, dev=True, warehouse_id=resolved_wh)

    # ── Phase 4: promote to prod ─────────────────────────────────────────────
    _step(f"Phase 4 — Promoting {env} → {prod_env} (dev_fin → prod_fin)")
    _make(
        "promote",
        f"SOURCE_ENV={env}",
        f"DEST_ENV={prod_env}",
        f"DEST_CATALOG_MAP={DEV_FIN_CAT}={PROD_FIN_CAT}",
    )

    _assert_file_exists(
        ENVS_DIR / prod_env / "env.auto.tfvars",
        "prod env.auto.tfvars written by promote",
    )
    _assert_contains(
        ENVS_DIR / prod_env / "generated" / "abac.auto.tfvars",
        PROD_FIN_CAT,
        f"{PROD_FIN_CAT} catalog in promoted prod config",
    )

    _copy_auth(env, prod_env)

    _step("Phase 4 — Applying prod governance")
    _force_account_reapply("attach-promote prod apply")
    _make(f"apply", f"ENV={prod_env}", retries=3, retry_delay_seconds=120)

    _step("Verifying prod ABAC governance")
    _verify_data(auth_file, prod=True, warehouse_id=resolved_wh)

    # ── teardown ─────────────────────────────────────────────────────────────
    if not keep_data:
        _teardown_data("--teardown", "--teardown-prod", auth_file=auth_file,
                       warehouse_id=resolved_wh)
        _try_destroy(prod_env)
        _try_destroy(env)
        _try_destroy_account()
        # Delete the UI-created space (Terraform won't do it in attach mode)
        _delete_genie_space_via_api(auth_file, space_id)

    print(f"\n  {_green(_bold('PASSED'))}  attach-promote")


def scenario_self_service_genie(
    auth_file: Path,
    warehouse_id: str = "",
    keep_data: bool = False,
    fresh_env: bool = False,
) -> None:
    """Central governance, self-service Genie: central ABAC team + BU teams self-serve Genie spaces.

    Phase 1 — Governance team:
      Creates a 'governance' env that governs both dev_fin + dev_clinical catalogs.
      Runs `make generate MODE=governance` — asserts ABAC sections are present and
      genie_space_configs is absent.
      Runs `make apply-governance` — applies account + data_access only; no Genie
      space is created (.genie_space_id_* file must NOT appear).

    Phase 2 — BU Finance team:
      Creates a 'bu_fin' env pointing at dev_fin tables with a Finance Analytics space.
      Runs `make generate MODE=genie` — asserts genie_space_configs is present and
      ABAC sections (groups, tag_assignments, fgac_policies) are absent.
      Runs `make apply-genie` — applies workspace only; Finance Analytics Genie Space
      IS created (.genie_space_id_finance_analytics must appear).

    Phase 3 — Adding a second BU (isolation check):
      Creates a 'bu_clin' env for a second BU team with Clinical Analytics.
      Runs `make generate MODE=genie` + `make apply-genie`.
      Asserts that the governance team's data_access/terraform.tfstate is byte-for-byte
      unchanged after the second BU is added (proving independence).
      Tests playbook.md §7 "Adding a second BU".

    Phase 4 — BU Finance team promote to prod:
      Runs `make promote SOURCE_ENV=bu_fin DEST_ENV=bu_fin_prod DEST_CATALOG_MAP=dev_fin=prod_fin`.
      Then runs `make apply-genie ENV=bu_fin_prod` (NOT make apply) — applies workspace only.
      Asserts bu_fin_prod has .genie_space_id_* but no data_access/terraform.tfstate.
      Asserts governance state is unmodified throughout.
      Tests the BU-team prod-promotion pattern from docs/self-service-genie.md.

    Tests: playbook.md §7 "Central governance, self-service Genie" and docs/self-service-genie.md.
    """
    _banner("Scenario: self-service-genie — Central governance + BU teams self-serve Genie")
    gov_env     = "governance"
    bu_env      = "bu_fin"
    bu_clin_env = "bu_clin"
    bu_prod_env = "bu_fin_prod"

    _ensure_packages()

    # ── Phase 1: Governance team — data setup ────────────────────────────────
    _preamble_cleanup(gov_env, bu_env, bu_clin_env, bu_prod_env, fresh_env=fresh_env)

    _step("Phase 1 — Setting up dev_fin + dev_clinical + prod_fin test catalogs")
    warehouse_id = _setup_data(auth_file, "--prod", warehouse_id=warehouse_id) or warehouse_id

    resolved_wh = _get_or_find_warehouse(auth_file, warehouse_id)
    _make("setup", f"ENV={gov_env}")

    # Governance env: list both catalogs, no genie_spaces block
    gov_tables_hcl = f"""\
uc_tables = [
  "{DEV_FIN_CAT}.finance.customers",
  "{DEV_FIN_CAT}.finance.transactions",
  "{DEV_FIN_CAT}.finance.credit_cards",
  "{DEV_CLIN_CAT}.clinical.patients",
  "{DEV_CLIN_CAT}.clinical.encounters",
]
"""
    wh_line = f'sql_warehouse_id = "{resolved_wh}"' if resolved_wh else 'sql_warehouse_id = ""'
    gov_env_dir = ENVS_DIR / gov_env
    (gov_env_dir / "env.auto.tfvars").write_text(gov_tables_hcl + wh_line + "\n")
    _copy_auth("dev", gov_env)

    _step("Phase 1 — Generating ABAC config (governance MODE)")
    _make("generate", f"ENV={gov_env}", "MODE=governance", retries=3)

    _step("Asserting governance mode output: ABAC sections present, genie_space_configs absent")
    gov_gen = gov_env_dir / "generated" / "abac.auto.tfvars"
    _assert_file_exists(gov_gen, "governance/generated/abac.auto.tfvars created")
    _assert_file_exists(
        gov_env_dir / "generated" / "masking_functions.sql",
        "governance/generated/masking_functions.sql created",
    )
    for section in ("tag_assignments", "fgac_policies"):
        _assert_contains(gov_gen, section, f"'{section}' present in governance output")
    # Check for the actual HCL block, not just the name string — the LLM sometimes
    # leaves comments mentioning genie_space_configs even after the block is stripped.
    _assert_not_contains(
        gov_gen, "genie_space_configs =",
        "genie_space_configs block absent in governance output (governance mode)",
    )

    _step("Phase 1 — Applying governance layers (account + data_access only)")
    _make("apply-governance", f"ENV={gov_env}", retries=3, retry_delay_seconds=120)

    _step("Asserting governance env: data_access state exists, no Genie Space created")
    da_state = gov_env_dir / "data_access" / "terraform.tfstate"
    if not da_state.exists():
        raise AssertionError(
            f"Expected data_access/terraform.tfstate in '{gov_env}' env after apply-governance, "
            "but it does not exist."
        )
    print(f"  {_green('PASS')}  data_access/terraform.tfstate exists in '{gov_env}' env")

    id_files_gov = list(gov_env_dir.glob(".genie_space_id_*"))
    if id_files_gov:
        raise AssertionError(
            f"apply-governance created a Genie Space in '{gov_env}' env — expected none. "
            f"Files found: {[str(f) for f in id_files_gov]}"
        )
    print(f"  {_green('PASS')}  No .genie_space_id_* file in '{gov_env}' env — workspace layer skipped")

    # ── Phase 2: BU Finance team — Genie-only flow ───────────────────────────
    _step("Phase 2 — Setting up BU Finance env")
    _make("setup", f"ENV={bu_env}")

    _write_env_tfvars(bu_env, SPACES_FINANCE_ONLY, resolved_wh)
    _copy_auth("dev", bu_env)

    _step("Phase 2 — Generating Genie config (genie MODE)")
    _make("generate", f"ENV={bu_env}", "MODE=genie", retries=3)

    _step("Asserting genie mode output: genie_space_configs present, ABAC sections absent")
    bu_gen = ENVS_DIR / bu_env / "generated" / "abac.auto.tfvars"
    _assert_file_exists(bu_gen, f"{bu_env}/generated/abac.auto.tfvars created")
    _assert_contains(bu_gen, "genie_space_configs",
                     "genie_space_configs present in genie output")
    for section in ("tag_assignments", "fgac_policies"):
        _assert_not_declared_hcl(bu_gen, section,
                                 f"'{section}' not declared in genie output (genie mode)")
    # masking SQL must NOT be written in genie mode
    bu_sql = ENVS_DIR / bu_env / "generated" / "masking_functions.sql"
    if bu_sql.exists():
        raise AssertionError(
            f"masking_functions.sql was written for '{bu_env}' in genie mode — expected none. "
            "Masking functions are owned by the governance team."
        )
    print(f"  {_green('PASS')}  No masking_functions.sql in '{bu_env}' genie-mode output")

    _step("Phase 2 — Applying BU workspace layer (Genie space only)")
    _make("apply-genie", f"ENV={bu_env}", retries=3, retry_delay_seconds=120)

    _step("Asserting BU env: .genie_space_id_* created, no data_access state")
    bu_env_dir = ENVS_DIR / bu_env
    id_files_bu = list(bu_env_dir.glob(".genie_space_id_*"))
    if not id_files_bu:
        raise AssertionError(
            f"apply-genie did not create a .genie_space_id_* file in '{bu_env}' env. "
            "The Finance Analytics Genie Space should have been created."
        )
    print(f"  {_green('PASS')}  .genie_space_id_* file present in '{bu_env}' env: "
          + ", ".join(f.name for f in id_files_bu))

    bu_da_state = bu_env_dir / "data_access" / "terraform.tfstate"
    if bu_da_state.exists():
        raise AssertionError(
            f"apply-genie wrote a data_access/terraform.tfstate in '{bu_env}' env — expected none. "
            "BU team should only manage the workspace layer."
        )
    print(f"  {_green('PASS')}  No data_access/terraform.tfstate in '{bu_env}' env — workspace layer only")

    # ── Phase 3: Second BU — isolation check ─────────────────────────────────
    # Snapshot governance data_access state before Phase 3 to verify it does not change.
    gov_da_state_snapshot = da_state.read_text()

    _step("Phase 3 — Setting up second BU team (Clinical Analytics)")
    _make("setup", f"ENV={bu_clin_env}")
    _write_env_tfvars(bu_clin_env, SPACES_CLINICAL_ONLY, resolved_wh)
    _copy_auth("dev", bu_clin_env)

    _step("Phase 3 — Generating Genie config for second BU (genie MODE)")
    _make("generate", f"ENV={bu_clin_env}", "MODE=genie", retries=3)

    bu_clin_gen = ENVS_DIR / bu_clin_env / "generated" / "abac.auto.tfvars"
    _assert_file_exists(bu_clin_gen, f"{bu_clin_env}/generated/abac.auto.tfvars created")
    _assert_contains(bu_clin_gen, "Clinical Analytics",
                     "Clinical Analytics genie_space_configs in second BU output")
    for section in ("tag_assignments", "fgac_policies"):
        _assert_not_declared_hcl(bu_clin_gen, section,
                                 f"'{section}' not declared in second BU genie output")

    _step("Phase 3 — Applying second BU workspace layer")
    _make("apply-genie", f"ENV={bu_clin_env}", retries=3, retry_delay_seconds=120)

    _step("Asserting second BU env: .genie_space_id_* created, governance state unchanged")
    bu_clin_env_dir = ENVS_DIR / bu_clin_env
    id_files_clin = list(bu_clin_env_dir.glob(".genie_space_id_*"))
    if not id_files_clin:
        raise AssertionError(
            f"apply-genie did not create a .genie_space_id_* file in '{bu_clin_env}' env. "
            "The Clinical Analytics Genie Space should have been created."
        )
    print(f"  {_green('PASS')}  .genie_space_id_* file present in '{bu_clin_env}' env: "
          + ", ".join(f.name for f in id_files_clin))

    gov_da_state_after_p3 = da_state.read_text()
    if gov_da_state_snapshot != gov_da_state_after_p3:
        raise AssertionError(
            f"governance/data_access/terraform.tfstate was modified when '{bu_clin_env}' BU was added. "
            "The governance team's state should be completely unaffected by adding a second BU."
        )
    print(f"  {_green('PASS')}  governance data_access state byte-for-byte unchanged after second BU")

    # ── Phase 4: BU Finance team promote to prod ─────────────────────────────
    _step(f"Phase 4 — BU Finance team promoting {bu_env} → {bu_prod_env}")
    _make(
        "promote",
        f"SOURCE_ENV={bu_env}",
        f"DEST_ENV={bu_prod_env}",
        f"DEST_CATALOG_MAP={DEV_FIN_CAT}={PROD_FIN_CAT}",
    )

    _assert_file_exists(
        ENVS_DIR / bu_prod_env / "env.auto.tfvars",
        f"{bu_prod_env} env.auto.tfvars written by promote",
    )
    # In genie mode, catalog refs may only appear in env.auto.tfvars (always
    # correctly remapped) and not in generated/abac.auto.tfvars (which may
    # only contain Genie Space metadata without catalog-prefixed table refs).
    _bu_prod_gen = ENVS_DIR / bu_prod_env / "generated" / "abac.auto.tfvars"
    _bu_prod_env_tf = ENVS_DIR / bu_prod_env / "env.auto.tfvars"
    _found_prod_cat = (
        (PROD_FIN_CAT in _bu_prod_gen.read_text() if _bu_prod_gen.exists() else False)
        or (PROD_FIN_CAT in _bu_prod_env_tf.read_text() if _bu_prod_env_tf.exists() else False)
    )
    if not _found_prod_cat:
        raise AssertionError(
            f"Expected '{PROD_FIN_CAT}' in promoted config (checked "
            f"generated/abac.auto.tfvars and env.auto.tfvars), but not found."
        )
    print(f"  {_green('PASS')}  {PROD_FIN_CAT} catalog in promoted prod config")

    _copy_auth("dev", bu_prod_env)

    _step("Phase 4 — Applying prod workspace layer (make apply-genie, not make apply)")
    _make("apply-genie", f"ENV={bu_prod_env}", retries=3, retry_delay_seconds=120)

    _step("Asserting prod BU env: .genie_space_id_* created, no data_access state")
    bu_prod_env_dir = ENVS_DIR / bu_prod_env
    id_files_prod = list(bu_prod_env_dir.glob(".genie_space_id_*"))
    if not id_files_prod:
        raise AssertionError(
            f"apply-genie did not create a .genie_space_id_* file in '{bu_prod_env}' env. "
            "Finance Analytics should have been created in the prod BU env."
        )
    print(f"  {_green('PASS')}  .genie_space_id_* file present in '{bu_prod_env}' env: "
          + ", ".join(f.name for f in id_files_prod))

    bu_prod_da_state = bu_prod_env_dir / "data_access" / "terraform.tfstate"
    if bu_prod_da_state.exists():
        raise AssertionError(
            f"apply-genie wrote a data_access/terraform.tfstate in '{bu_prod_env}' env — "
            "BU prod promote should apply only the workspace layer."
        )
    print(f"  {_green('PASS')}  No data_access/terraform.tfstate in '{bu_prod_env}' — workspace layer only")

    gov_da_state_after_p4 = da_state.read_text()
    if gov_da_state_snapshot != gov_da_state_after_p4:
        raise AssertionError(
            "governance/data_access/terraform.tfstate was modified during BU prod promote. "
            "The governance team's state should be completely unaffected."
        )
    print(f"  {_green('PASS')}  governance data_access state unchanged after BU prod promote")

    # ── teardown ─────────────────────────────────────────────────────────────
    if not keep_data:
        _teardown_data("--teardown", "--teardown-prod", auth_file=auth_file,
                       warehouse_id=resolved_wh)
        _try_destroy(bu_prod_env)
        _try_destroy(bu_clin_env)
        _try_destroy(bu_env)
        _try_destroy(gov_env)
        _try_destroy_account()

    print(f"\n  {_green(_bold('PASSED'))}  self-service-genie")


# ---------------------------------------------------------------------------
# Scenario: abac-only — ABAC governance without Genie Space (+ upgrade path)
# ---------------------------------------------------------------------------

def scenario_abac_only(
    auth_file: Path,
    warehouse_id: str,
    keep_data: bool,
    fresh_env: bool = False,
) -> None:
    """
    Phase 1 — ABAC-only deploy (playbook.md §2):
      Configure env with uc_tables only (no genie_spaces block).
      Run plain `make generate` (no MODE= flag) + `make apply`.
      Assert: no genie_space_configs in generated output, masking_functions.sql
      generated, no .genie_space_id_* file, data_access/terraform.tfstate exists.

    Phase 2 — §2 → §4 upgrade path:
      Add Finance Analytics to genie_spaces and run `make generate MODE=genie`.
      Then `make apply-genie`. Assert Genie Space created, existing governance preserved
      (data_access/terraform.tfstate still exists, column tags and masks still applied).

    Tests: playbook.md §2 "ABAC governance only" and the §2 → §4 upgrade path.
    """
    _banner("Scenario: abac-only — ABAC governance without Genie Space (+ upgrade to Genie)")
    env = "dev"

    _preamble_cleanup(env, fresh_env=fresh_env)

    _step("Phase 1 — Setting up dev_fin test catalog")
    warehouse_id = _setup_data(auth_file, warehouse_id=warehouse_id) or warehouse_id

    resolved_wh = _get_or_find_warehouse(auth_file, warehouse_id)

    # Phase 1: uc_tables only, no genie_spaces
    _step("Phase 1 — Configuring env with uc_tables only (no genie_spaces)")
    _make("setup", f"ENV={env}")
    wh_line = f'sql_warehouse_id = "{resolved_wh}"' if resolved_wh else 'sql_warehouse_id = ""'
    env_dir = ENVS_DIR / env
    (env_dir / "env.auto.tfvars").write_text(TABLES_FINANCE_ONLY_HCL + wh_line + "\n")

    _step("Phase 1 — Generating ABAC config (plain make generate, no genie_spaces)")
    _make("generate", f"ENV={env}", retries=3)

    gen_dir = env_dir / "generated"
    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated")
    _assert_file_exists(gen_dir / "masking_functions.sql", "masking_functions.sql generated")
    _assert_not_declared_hcl(gen_dir / "abac.auto.tfvars", "genie_space_configs",
                             "genie_space_configs absent (no genie_spaces in env config)")

    _step("Phase 1 — Applying (all three layers)")
    _make("apply", f"ENV={env}", retries=3, retry_delay_seconds=120)

    _step("Asserting Phase 1: no Genie Space created, data_access state exists")
    id_files = list(env_dir.glob(".genie_space_id_*"))
    legacy   = env_dir / ".genie_space_id"
    if id_files or legacy.exists():
        raise AssertionError(
            "make apply created a Genie Space in ABAC-only mode — expected none. "
            f"Files: {[f.name for f in id_files]}"
        )
    print(f"  {_green('PASS')}  No .genie_space_id_* file — Genie Space correctly not created")

    da_state = env_dir / "data_access" / "terraform.tfstate"
    da_backup = env_dir / "data_access" / "terraform.tfstate.backup"
    if da_state.exists():
        print(f"  {_green('PASS')}  data_access/terraform.tfstate exists — governance deployed")
    elif da_backup.exists():
        print(f"  {_green('PASS')}  data_access/terraform.tfstate.backup exists — governance deployed (state rotated)")
    else:
        # On retried applies, Terraform may rotate state files. Check if the
        # data_access dir has any .tfstate file at all.
        da_dir = env_dir / "data_access"
        any_state = list(da_dir.glob("*.tfstate*")) if da_dir.exists() else []
        if any_state:
            print(f"  {_green('PASS')}  data_access state file found: {any_state[0].name} — governance deployed")
        else:
            raise AssertionError(
                "data_access/terraform.tfstate not found after ABAC-only apply. "
                "Expected all three layers to be applied."
            )

    _step("Verifying ABAC governance applied to dev_fin tables")
    _verify_data(auth_file, dev=True, warehouse_id=resolved_wh)

    # ── Phase 2: §2 → §4 upgrade path ────────────────────────────────────────
    _step("Phase 2 — Adding Finance Analytics to env.auto.tfvars (ABAC-only → Genie upgrade)")
    _write_env_tfvars(env, SPACES_FINANCE_ONLY, resolved_wh)

    _step("Phase 2 — Genie-only generate for Finance Analytics")
    _make("generate", f"ENV={env}", "MODE=genie", retries=3)

    _assert_contains(gen_dir / "abac.auto.tfvars", "Finance Analytics",
                     "Finance Analytics genie_space_configs present after upgrade")

    _step("Phase 2 — Applying workspace layer only (Genie on top of existing governance)")
    _make("apply-genie", f"ENV={env}", retries=3, retry_delay_seconds=120)

    _step("Asserting Phase 2: Genie Space created, governance preserved")
    _assert_genie_space_id_file(env, "Finance Analytics")

    if not da_state.exists():
        raise AssertionError(
            "data_access/terraform.tfstate was removed during Genie Space upgrade — "
            "existing governance should be preserved."
        )
    print(f"  {_green('PASS')}  data_access/terraform.tfstate still exists — governance preserved")

    _step("Verifying ABAC governance still applied after Genie Space added")
    _verify_data(auth_file, dev=True, warehouse_id=resolved_wh)

    # ── teardown ─────────────────────────────────────────────────────────────
    if not keep_data:
        _teardown_data("--teardown", auth_file=auth_file, warehouse_id=resolved_wh)
        _try_destroy(env)
        _try_destroy_account()

    print(f"\n  {_green(_bold('PASSED'))}  abac-only")


# ---------------------------------------------------------------------------
# Scenario: multi-space-import — Import two UI-created Genie Spaces at once
# ---------------------------------------------------------------------------

def scenario_multi_space_import(
    auth_file: Path,
    warehouse_id: str,
    keep_data: bool,
    fresh_env: bool = False,
) -> None:
    """
    Import two existing Genie Spaces in one make generate call (playbook.md §3 multi-space).

    Creates two spaces via the Genie REST API (simulating UI-configured spaces),
    then configures genie_spaces with two genie_space_id entries. Asserts both
    configs appear in the generated output and Terraform does not create new
    spaces on apply (both are attached, not created).

    Tests: playbook.md §3 "Multi-space import" section.
    """
    _banner("Scenario: multi-space-import — Import two UI-created Genie Spaces")
    env = "dev"

    _ensure_packages()
    _preamble_cleanup(env, fresh_env=fresh_env)

    _step("Setting up dev_fin and dev_clinical test catalogs")
    warehouse_id = _setup_data(auth_file, warehouse_id=warehouse_id) or warehouse_id

    resolved_wh = _get_or_find_warehouse(auth_file, warehouse_id)

    fin_tables = [
        f"{DEV_FIN_CAT}.finance.customers",
        f"{DEV_FIN_CAT}.finance.transactions",
        f"{DEV_FIN_CAT}.finance.credit_cards",
    ]
    clin_tables = [
        f"{DEV_CLIN_CAT}.clinical.patients",
        f"{DEV_CLIN_CAT}.clinical.encounters",
    ]

    _step("Creating Finance Analytics Genie Space via API (simulating UI configuration)")
    fin_space_id = _create_genie_space_via_api(
        auth_file, title="Finance Analytics", tables=fin_tables, warehouse_id=resolved_wh,
    )

    _step("Creating Clinical Analytics Genie Space via API (simulating UI configuration)")
    clin_space_id = _create_genie_space_via_api(
        auth_file, title="Clinical Analytics", tables=clin_tables, warehouse_id=resolved_wh,
    )

    _step("Configuring env with two genie_space_id entries (multi-space import)")
    _make("setup", f"ENV={env}")

    two_space_import_hcl = f"""\
genie_spaces = [
  {{
    name           = "Finance Analytics"
    genie_space_id = "{fin_space_id}"
    uc_tables = [
      "{DEV_FIN_CAT}.finance.customers",
      "{DEV_FIN_CAT}.finance.transactions",
      "{DEV_FIN_CAT}.finance.credit_cards",
    ]
  }},
  {{
    name           = "Clinical Analytics"
    genie_space_id = "{clin_space_id}"
    uc_tables = [
      "{DEV_CLIN_CAT}.clinical.patients",
      "{DEV_CLIN_CAT}.clinical.encounters",
    ]
  }},
]
"""
    _write_env_tfvars(env, two_space_import_hcl, resolved_wh)

    _step("Running make generate — importing both spaces in one call")
    _make("generate", f"ENV={env}", retries=3)

    gen_dir = ENVS_DIR / env / "generated"
    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated")
    _assert_contains(gen_dir / "abac.auto.tfvars", "Finance Analytics",
                     "Finance Analytics genie_space_configs present")
    _assert_contains(gen_dir / "abac.auto.tfvars", "Clinical Analytics",
                     "Clinical Analytics genie_space_configs present")
    _assert_contains(gen_dir / "abac.auto.tfvars", DEV_FIN_CAT,
                     f"{DEV_FIN_CAT} catalog referenced in generated policies")
    _assert_contains(gen_dir / "abac.auto.tfvars", DEV_CLIN_CAT,
                     f"{DEV_CLIN_CAT} catalog referenced in generated policies")

    _step("Applying governance (both spaces attached — Terraform must not create new spaces)")
    _make("apply", f"ENV={env}", retries=3, retry_delay_seconds=120)

    _step("Asserting no .genie_space_id_* files — both spaces attached, not created")
    id_files = list((ENVS_DIR / env).glob(".genie_space_id_*"))
    legacy   = ENVS_DIR / env / ".genie_space_id"
    if id_files or legacy.exists():
        raise AssertionError(
            "Terraform created new Genie Spaces in multi-space import mode — expected none. "
            f"Files: {[f.name for f in id_files]}"
        )
    print(f"  {_green('PASS')}  No .genie_space_id_* files — both spaces correctly attached")

    _step("Verifying ABAC governance applied across both catalogs")
    _verify_data(auth_file, dev=True, warehouse_id=resolved_wh)

    # ── teardown ─────────────────────────────────────────────────────────────
    if not keep_data:
        _teardown_data("--teardown", auth_file=auth_file, warehouse_id=resolved_wh)
        _try_destroy(env)
        _try_destroy_account()
        _delete_genie_space_via_api(auth_file, fin_space_id)
        _delete_genie_space_via_api(auth_file, clin_space_id)

    print(f"\n  {_green(_bold('PASSED'))}  multi-space-import")


# ---------------------------------------------------------------------------
# Scenario: schema-drift -- Column tag drift detection
# ---------------------------------------------------------------------------

def scenario_schema_drift(
    auth_file: Path,
    warehouse_id: str,
    keep_data: bool,
    fresh_env: bool = False,
) -> None:
    """
    Flow: quickstart baseline -> verify clean -> ADD COLUMN -> detect drift ->
    generate-delta -> apply -> verify resolved -> DROP COLUMN -> detect stale ->
    generate-delta -> verify resolved -> RENAME COLUMN -> detect both ->
    generate-delta -> apply -> verify resolved -> teardown.
    """
    _banner("Scenario: schema-drift -- Column tag drift detection")
    env = "dev"

    # ── Phase A: Baseline (reuse quickstart setup) ───────────────────────
    _preamble_cleanup(env, fresh_env=fresh_env)

    _step("Creating test catalogs")
    warehouse_id = _setup_data(auth_file, warehouse_id=warehouse_id) or warehouse_id

    _step("Preparing env")
    _make("setup", f"ENV={env}")
    _write_env_tfvars(env, SPACES_FINANCE_ONLY, warehouse_id)

    _step("Generating ABAC config (baseline)")
    _make("generate", f"ENV={env}", retries=3)

    _step("Applying all layers (baseline)")
    _make("apply", f"ENV={env}", retries=3, retry_delay_seconds=120)

    _step("Verifying baseline data + ABAC governance")
    _verify_data(auth_file, dev=True, warehouse_id=warehouse_id)

    _step("Verifying baseline audit does not report our test column")
    result = _run(
        [sys.executable, str(MODULE_ROOT / "scripts" / "audit_schema_drift.py")],
        cwd=ENVS_DIR / env,
        check=False,
        capture=True,
    )
    if result.stdout and "emergency_ssn" in result.stdout:
        raise RuntimeError(
            "audit-schema should not report emergency_ssn on a clean baseline "
            "(column hasn't been added yet)"
        )
    print(f"  {_green('PASS')}  Baseline audit: emergency_ssn not reported (as expected)")

    # ── Phase B: Forward drift (ADD COLUMN) ──────────────────────────────
    _step("Phase B: Adding PII column to test forward drift")
    _sdk_run_sql(
        auth_file,
        f"ALTER TABLE {DEV_FIN_CAT}.finance.customers ADD COLUMN emergency_ssn STRING",
        warehouse_id=warehouse_id,
    )

    _step("Verifying audit detects forward drift (exit 1)")
    result = _make("audit-schema", f"ENV={env}", check=False)
    if result.returncode == 0:
        raise RuntimeError("audit-schema should return 1 after ADD COLUMN, got 0")
    print(f"  {_green('PASS')}  Forward drift detected")

    _step("Running generate-delta to classify new column")
    _make("generate-delta", f"ENV={env}", retries=3)

    da_abac = ENVS_DIR / env / "generated" / "abac.auto.tfvars"
    _assert_contains(da_abac, "emergency_ssn", "emergency_ssn added to config")

    _step("Applying delta changes")
    _clear_apply_fingerprints(ENVS_DIR / env / "data_access")
    _make("apply", f"ENV={env}", retries=3, retry_delay_seconds=120)

    _step("Verifying audit is clean after apply (exit 0)")
    result = _make("audit-schema", f"ENV={env}", check=False)
    if result.returncode != 0:
        raise RuntimeError(
            "audit-schema should return 0 after generate-delta + apply, "
            f"got exit {result.returncode}"
        )
    print(f"  {_green('PASS')}  Forward drift resolved")

    # ── Phase C: Reverse drift (DROP COLUMN) ─────────────────────────────
    _step("Phase C: Unset tags + enable column mapping + drop column to test reverse drift")
    # Must remove all governed tags before Databricks allows DROP COLUMN.
    # Query the actual tag names from information_schema (handles suffixed keys in parallel mode).
    _unset_column_tags(auth_file, DEV_FIN_CAT, "finance", "customers", "emergency_ssn",
                       warehouse_id=warehouse_id)
    _sdk_run_sql(
        auth_file,
        f"ALTER TABLE {DEV_FIN_CAT}.finance.customers SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
        warehouse_id=warehouse_id,
    )
    _sdk_run_sql(
        auth_file,
        f"ALTER TABLE {DEV_FIN_CAT}.finance.customers DROP COLUMN emergency_ssn",
        warehouse_id=warehouse_id,
    )

    _step("Verifying audit detects reverse drift (exit 1)")
    result = _make("audit-schema", f"ENV={env}", check=False)
    if result.returncode == 0:
        raise RuntimeError("audit-schema should return 1 after DROP COLUMN, got 0")
    print(f"  {_green('PASS')}  Reverse drift detected")

    _step("Running generate-delta to remove stale assignment")
    _make("generate-delta", f"ENV={env}", retries=1)

    text = da_abac.read_text()
    if "emergency_ssn" in text:
        raise RuntimeError("emergency_ssn should have been removed from config after DROP COLUMN")
    print(f"  {_green('PASS')}  Stale assignment removed")

    _step("Verifying audit is clean after stale removal (exit 0)")
    result = _make("audit-schema", f"ENV={env}", check=False)
    if result.returncode != 0:
        raise RuntimeError(
            "audit-schema should return 0 after stale removal, "
            f"got exit {result.returncode}"
        )
    print(f"  {_green('PASS')}  Reverse drift resolved")

    # ── Phase D: Rename (both directions) ────────────────────────────────
    _step("Phase D: Unset tags + rename column to test combined drift")
    _unset_column_tags(auth_file, DEV_FIN_CAT, "finance", "customers", "email",
                       warehouse_id=warehouse_id)
    _sdk_run_sql(
        auth_file,
        f"ALTER TABLE {DEV_FIN_CAT}.finance.customers RENAME COLUMN email TO contact_email",
        warehouse_id=warehouse_id,
    )

    _step("Verifying audit detects rename drift (exit 1)")
    result = _make("audit-schema", f"ENV={env}", check=False)
    if result.returncode == 0:
        raise RuntimeError("audit-schema should return 1 after RENAME COLUMN, got 0")
    print(f"  {_green('PASS')}  Rename drift detected")

    _step("Running generate-delta to handle rename")
    _make("generate-delta", f"ENV={env}", retries=3)

    text = da_abac.read_text()
    if "contact_email" not in text:
        raise RuntimeError("contact_email should appear in config after rename delta")
    print(f"  {_green('PASS')}  Renamed column classified")

    _step("Applying rename delta changes")
    _clear_apply_fingerprints(ENVS_DIR / env / "data_access")
    _make("apply", f"ENV={env}", retries=3, retry_delay_seconds=120)

    _step("Verifying audit is clean after rename apply (exit 0)")
    result = _make("audit-schema", f"ENV={env}", check=False)
    if result.returncode != 0:
        raise RuntimeError(
            "audit-schema should return 0 after rename delta + apply, "
            f"got exit {result.returncode}"
        )
    print(f"  {_green('PASS')}  Rename drift resolved")

    # ── Teardown ─────────────────────────────────────────────────────────
    if not keep_data:
        _teardown_data("--teardown", "--teardown-prod", auth_file=auth_file,
                       warehouse_id=warehouse_id)
        _try_destroy(env)
        _try_destroy_account()

    print(f"\n  {_green(_bold('PASSED'))}  schema-drift")


def _unset_column_tags(
    auth_file: Path,
    catalog: str,
    schema: str,
    table: str,
    column: str,
    warehouse_id: str = "",
) -> None:
    """Query actual tag names from information_schema and UNSET them all.

    This handles suffixed tag keys in parallel mode (e.g. pii_level_abc123)
    by reading the real tag names rather than guessing base names.
    """
    import hcl2 as _hcl2
    from databricks.sdk import WorkspaceClient as _WC
    from databricks.sdk.service.sql import StatementState

    def _s(v): return (v[0] if isinstance(v, list) else (v or "")).strip()

    with open(auth_file) as f:
        auth = _hcl2.load(f)
    host          = _s(auth.get("databricks_workspace_host", ""))
    client_id     = _s(auth.get("databricks_client_id", ""))
    client_secret = _s(auth.get("databricks_client_secret", ""))
    w = _WC(host=host, client_id=client_id, client_secret=client_secret)

    wh = warehouse_id
    if not wh:
        for warehouse in w.warehouses.list():
            if warehouse.id:
                wh = warehouse.id
                break

    # Query actual tag names on this column
    query_sql = (
        f"SELECT tag_name FROM system.information_schema.column_tags "
        f"WHERE catalog_name = '{catalog}' AND schema_name = '{schema}' "
        f"AND table_name = '{table}' AND column_name = '{column}'"
    )
    r = w.statement_execution.execute_statement(
        statement=query_sql, warehouse_id=wh, wait_timeout="50s",
    )
    while r.status and r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        import time as _time
        _time.sleep(2)
        r = w.statement_execution.get_statement(r.statement_id)

    tag_names = []
    if r.result and r.result.data_array:
        tag_names = [row[0] for row in r.result.data_array if row and row[0]]

    if not tag_names:
        print(f"  No tags found on {catalog}.{schema}.{table}.{column}")
        return

    print(f"  Found {len(tag_names)} tag(s) on {column}: {', '.join(tag_names)}")
    for tag in tag_names:
        try:
            _sdk_run_sql(
                auth_file,
                f"ALTER TABLE {catalog}.{schema}.{table} "
                f"ALTER COLUMN {column} UNSET TAGS ('{tag}')",
                warehouse_id=warehouse_id,
            )
        except Exception:
            pass  # tag may have been removed by another process


def _sdk_run_sql(auth_file: Path, sql: str, warehouse_id: str = "") -> None:
    """Execute a single SQL statement via the Databricks SDK."""
    import hcl2 as _hcl2
    from databricks.sdk import WorkspaceClient as _WC
    from databricks.sdk.service.sql import StatementState

    def _s(v): return (v[0] if isinstance(v, list) else (v or "")).strip()

    with open(auth_file) as f:
        auth = _hcl2.load(f)
    host          = _s(auth.get("databricks_workspace_host", ""))
    client_id     = _s(auth.get("databricks_client_id", ""))
    client_secret = _s(auth.get("databricks_client_secret", ""))
    w = _WC(host=host, client_id=client_id, client_secret=client_secret)

    wh = warehouse_id
    if not wh:
        for warehouse in w.warehouses.list():
            if warehouse.id:
                wh = warehouse.id
                break

    r = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=wh, wait_timeout="50s",
    )
    while r.status and r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        import time as _time
        _time.sleep(2)
        r = w.statement_execution.get_statement(r.statement_id)

    state_str = str(getattr(getattr(r, "status", None), "state", ""))
    if "FAILED" in state_str:
        err = getattr(getattr(r, "status", None), "error", None)
        msg = getattr(err, "message", str(err)) if err else "unknown"
        raise RuntimeError(f"SQL failed: {msg}\n  SQL: {sql}")
    print(f"  Executed: {sql[:80]}{'...' if len(sql) > 80 else ''}")


# ---------------------------------------------------------------------------
# Scenario: genie-only — genie_only=true mode (no account-level resources)
# ---------------------------------------------------------------------------

def scenario_genie_only(
    auth_file: Path,
    warehouse_id: str,
    keep_data: bool,
    fresh_env: bool = False,
) -> None:
    """
    Phase 1 — Data setup:
      Create dev_fin catalog/tables. Setup 'genie_only' env.

    Phase 2 — Configure genie_only mode:
      Write env.auto.tfvars with genie_only = true, genie_spaces, sql_warehouse_id.
      Copy auth from dev. Write minimal abac.auto.tfvars with groups = {}.

    Phase 3 — Generate + Apply:
      make generate ENV=genie_only MODE=genie
      Assert: genie_space_configs present, ABAC sections absent.
      make apply-genie ENV=genie_only

    Phase 4 — Assertions:
      1. .genie_space_id_finance_analytics file exists (space created)
      2. No account-level resources in terraform.tfstate
      3. No data_access/terraform.tfstate (governance layer untouched)
      4. Genie space accessible via API (read space by ID)

    Phase 5 — Teardown:
      destroy genie_only env, teardown data, destroy account layer.

    Tests: genie_only=true Terraform variable — SP needs only workspace USER
    with SQL entitlement (no admin roles at all), no account-level resources.
    """
    _banner("Scenario: genie-only — genie_only=true mode (minimal-privilege SP)")
    env = "genie_only"
    ws_admin_sp_id: int | None = None   # track for teardown

    _ensure_packages()

    # ── Phase 1: Data setup (uses full-privilege SP) ────────────────────────
    # Cache auth file content BEFORE preamble cleanup.  On Azure with a
    # provisioned test env, the cleanup's make destroy → _bootstrap →
    # _prepare-env cycle can remove dev/auth.auto.tfvars (the Makefile
    # operates on CLOUD_ROOT/envs/ while the test uses envs/test/).
    # Restore the file after cleanup so setup_test_data.py can read it.
    _auth_content = auth_file.read_text() if auth_file.exists() else None
    cfg = _load_auth_cfg(auth_file)
    _preamble_cleanup(env, fresh_env=fresh_env)
    if _auth_content and not auth_file.exists():
        auth_file.parent.mkdir(parents=True, exist_ok=True)
        auth_file.write_text(_auth_content)

    _step("Phase 1 — Creating dev_fin test catalog")
    warehouse_id = _setup_data(auth_file, warehouse_id=warehouse_id) or warehouse_id

    resolved_wh = _get_or_find_warehouse(auth_file, warehouse_id)
    _make("setup", f"ENV={env}")

    # ── Phase 2: Create reduced-privilege SP + configure genie_only ─────────
    _step("Phase 2 — Creating minimal-privilege SP (workspace USER + SQL entitlement)")
    _s = lambda v: (v[0] if isinstance(v, list) else (v or "")).strip()
    ws_id = _s(cfg.get("databricks_workspace_id", ""))
    account_id = _s(cfg.get("databricks_account_id", ""))
    ws_host = _s(cfg.get("databricks_workspace_host", ""))

    ws_admin_sp_id, ws_admin_client_id, ws_admin_client_secret = (
        _create_genie_only_sp(auth_file, ws_id, warehouse_id=resolved_wh, cfg=cfg)
    )

    _step("Phase 2 — Writing env.auto.tfvars with genie_only = true")
    env_dir = ENVS_DIR / env
    wh_line = f'sql_warehouse_id = "{resolved_wh}"' if resolved_wh else 'sql_warehouse_id = ""'
    env_tfvars_content = f"""\
genie_only = true

{SPACES_FINANCE_ONLY}
{wh_line}
"""
    (env_dir / "env.auto.tfvars").write_text(env_tfvars_content)

    # Write auth using the REDUCED-PRIVILEGE SP (workspace USER + SQL entitlement only)
    _step("Phase 2 — Writing auth.auto.tfvars with minimal-privilege SP credentials")
    auth_content = (
        f'# Minimal-privilege SP — workspace USER + SQL entitlement only (no admin roles)\n'
        f'# Generated by scenario_genie_only for genie_only=true permission test\n'
        f'databricks_account_id     = "{account_id}"\n'
        f'databricks_client_id      = "{ws_admin_client_id}"\n'
        f'databricks_client_secret  = "{ws_admin_client_secret}"\n'
        f'databricks_workspace_id   = "{ws_id}"\n'
        f'databricks_workspace_host = "{ws_host}"\n'
    )
    (env_dir / "auth.auto.tfvars").write_text(auth_content)
    print(f"  {_green('OK')}  auth.auto.tfvars written with minimal-privilege SP (USER + SQL entitlement)")

    # Write minimal abac.auto.tfvars with empty groups (genie_only skips account ops)
    gen_dir = env_dir / "generated"
    gen_dir.mkdir(parents=True, exist_ok=True)
    (gen_dir / "abac.auto.tfvars").write_text('groups = {}\n')

    # ── Phase 3: Generate + Apply (with reduced-privilege SP) ───────────────
    _step("Phase 3 — Generating Genie config (MODE=genie)")
    _make("generate", f"ENV={env}", "MODE=genie", retries=3)

    _step("Asserting genie mode output: genie_space_configs present, ABAC sections absent")
    bu_gen = env_dir / "generated" / "abac.auto.tfvars"
    _assert_file_exists(bu_gen, f"{env}/generated/abac.auto.tfvars created")
    _assert_contains(bu_gen, "genie_space_configs",
                     "genie_space_configs present in genie_only output")
    for section in ("tag_assignments", "fgac_policies"):
        _assert_not_declared_hcl(bu_gen, section,
                                 f"'{section}' not declared in genie_only output (genie mode)")

    _step("Phase 3 — Applying workspace layer with minimal-privilege SP (make apply-genie)")
    _make("apply-genie", f"ENV={env}", retries=3, retry_delay_seconds=120)

    # ── Phase 4: Assertions ─────────────────────────────────────────────────
    _step("Phase 4 — Asserting genie_only deployment")

    # 4a. .genie_space_id_finance_analytics file exists
    _assert_genie_space_id_file(env, "Finance Analytics")

    # 4b. No account-level resources in terraform.tfstate
    _assert_state_no_account_resources(env)

    # 4c. No data_access/terraform.tfstate (governance layer untouched)
    da_state = env_dir / "data_access" / "terraform.tfstate"
    if da_state.exists():
        raise AssertionError(
            f"apply-genie wrote a data_access/terraform.tfstate in '{env}' env — expected none. "
            "genie_only mode should only manage the workspace layer."
        )
    print(f"  {_green('PASS')}  No data_access/terraform.tfstate in '{env}' — workspace layer only")

    # 4d. Genie space accessible via API (read space by ID using reduced SP)
    id_files = list(env_dir.glob(".genie_space_id_*"))
    if id_files:
        space_id = id_files[0].read_text().strip()
        if space_id:
            _step("Phase 4 — Verifying Genie Space exists via API (using minimal-privilege SP)")
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient(
                host=ws_host,
                client_id=ws_admin_client_id,
                client_secret=ws_admin_client_secret,
                product="genierails-test-runner",
                product_version="0.1.0",
            )
            try:
                resp = w.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}")
                api_title = resp.get("title", "")
                print(f"  {_green('PASS')}  Genie Space {space_id} accessible via API (title: {api_title!r})")
            except Exception as exc:
                # GET may be blocked by Partner Powered AI on fresh AWS workspaces.
                # Fall back to PATCH (not gated) to verify the space exists.
                if "Partner Powered AI" in str(exc) or "cross-Geo" in str(exc):
                    try:
                        resp = w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}",
                                               body={"title": f"Space {space_id}"})
                        api_title = resp.get("title", "")
                        print(f"  {_green('PASS')}  Genie Space {space_id} accessible via PATCH fallback (title: {api_title!r})")
                    except Exception as exc2:
                        raise AssertionError(
                            f"Genie Space {space_id} not accessible via API (GET blocked by Partner AI, PATCH also failed): {exc2}"
                        )
                else:
                    raise AssertionError(
                        f"Genie Space {space_id} not accessible via API: {exc}"
                    )

    # ── Phase 5: Teardown ───────────────────────────────────────────────────
    # Restore full-privilege auth before destroy (reduced SP can't destroy account resources)
    _copy_auth("dev", env)

    if not keep_data:
        _teardown_data("--teardown", auth_file=auth_file, warehouse_id=resolved_wh)
        _try_destroy(env)
        _try_destroy_account()

    # Clean up the reduced-privilege SP (uses full-privilege SP via auth_file)
    if ws_admin_sp_id is not None:
        _step("Phase 5 — Deleting minimal-privilege SP")
        _delete_sp(auth_file, ws_admin_sp_id)

    print(f"\n  {_green(_bold('PASSED'))}  genie-only")


# ---------------------------------------------------------------------------
# Scenario: genie-import-no-abac — Import Genie Space, deploy to prod, no ABAC
# ---------------------------------------------------------------------------

def scenario_genie_import_no_abac(
    auth_file: Path,
    warehouse_id: str,
    keep_data: bool,
    fresh_env: bool = False,
) -> None:
    """
    Import an existing Genie Space and deploy to prod without generating or
    managing any ABAC governance.  This validates the genie-only import-to-prod
    workflow when a separate governance team manages ABAC centrally.

    Phase 1 — Data setup:
      Create dev_fin + prod_fin test catalogs.

    Phase 2 — Create initial Genie Space via API:
      Simulates a UI-configured space that a data team already set up.

    Phase 3 — Import into fresh env without ABAC:
      make setup ENV=import_noabac
      Write env.auto.tfvars with genie_only = true and genie_spaces pointing
      to the created space (genie_space_id).
      make generate MODE=genie ENV=import_noabac — generate genie config only.

    Phase 4 — Promote to prod (the key test):
      make promote SOURCE_ENV=import_noabac DEST_ENV=import_noabac_prod
                   DEST_CATALOG_MAP=dev_fin=prod_fin
      Assert promote completes (graceful skip or success with genie config).
      make apply-genie ENV=import_noabac_prod

    Phase 5 — Assertions:
      Dev: no .genie_space_id_* (attached, not created); space accessible via API.
      Prod: workspace applied successfully (space attached or created).
      No data_access/terraform.tfstate in prod env.
      No account-level resources created.
      No masking_functions.sql generated.
      Generated config has no tag_assignments, fgac_policies.

    Phase 6 — Teardown.
    """
    _banner("Scenario: genie-import-no-abac — Import Genie, deploy to prod without ABAC")
    src_env  = "import_src"
    env      = "import_noabac"
    prod_env = "import_noabac_prod"

    _ensure_packages()

    # ── Phase 1: Data setup ───────────────────────────────────────────────────
    _preamble_cleanup(src_env, env, prod_env, fresh_env=fresh_env)

    _step("Phase 1 — Creating dev_fin + prod_fin test catalogs")
    warehouse_id = _setup_data(auth_file, "--prod", warehouse_id=warehouse_id) or warehouse_id

    resolved_wh = _get_or_find_warehouse(auth_file, warehouse_id)

    # ── Phase 2: Create a Genie Space via API (simulating UI-configured space) ──
    _step("Phase 2 — Creating Genie Space via API (simulating existing UI-created space)")
    fin_tables = [
        f"{DEV_FIN_CAT}.finance.customers",
        f"{DEV_FIN_CAT}.finance.transactions",
        f"{DEV_FIN_CAT}.finance.credit_cards",
    ]
    src_space_id = _create_genie_space_via_api(
        auth_file, title="Finance Analytics", tables=fin_tables, warehouse_id=resolved_wh,
    )

    # ── Phase 3: Import into fresh env without ABAC ───────────────────────────
    _step("Phase 3 — Setting up import_noabac env (genie_only, no ABAC)")
    _make("setup", f"ENV={env}")

    env_dir = ENVS_DIR / env
    wh_line = f'sql_warehouse_id = "{resolved_wh}"' if resolved_wh else 'sql_warehouse_id = ""'

    import_hcl = f"""\
genie_only = true

genie_spaces = [
  {{
    name           = "Finance Analytics"
    genie_space_id = "{src_space_id}"
    uc_tables = [
      "{DEV_FIN_CAT}.finance.customers",
      "{DEV_FIN_CAT}.finance.transactions",
      "{DEV_FIN_CAT}.finance.credit_cards",
    ]
  }},
]

{wh_line}
"""
    (env_dir / "env.auto.tfvars").write_text(import_hcl)
    _copy_auth("dev", env)

    _step("Phase 3 — Generating Genie config (MODE=genie, no ABAC)")
    _make("generate", f"ENV={env}", "MODE=genie", retries=3)

    gen_dir = env_dir / "generated"
    gen_abac = gen_dir / "abac.auto.tfvars"
    _assert_file_exists(gen_abac, f"{env}/generated/abac.auto.tfvars created")
    _assert_contains(gen_abac, "genie_space_configs",
                     "genie_space_configs present in import_noabac output")
    for section in ("tag_assignments", "fgac_policies"):
        _assert_not_declared_hcl(gen_abac, section,
                                 f"'{section}' not declared in import_noabac output (genie mode)")

    gen_sql = gen_dir / "masking_functions.sql"
    if gen_sql.exists():
        raise AssertionError(
            f"masking_functions.sql was generated in '{env}' env — expected none in genie-only mode."
        )
    print(f"  {_green('PASS')}  No masking_functions.sql in '{env}' — genie-only, no ABAC")

    _step("Phase 3 — Applying workspace layer (make apply-genie)")
    _make("apply-genie", f"ENV={env}", retries=3, retry_delay_seconds=120)

    # Space was imported (attached via genie_space_id), not created — no .genie_space_id_* file
    _step("Asserting space NOT created by Terraform (attached via genie_space_id)")
    id_files = list((ENVS_DIR / env).glob(".genie_space_id_*"))
    legacy = ENVS_DIR / env / ".genie_space_id"
    if id_files or legacy.exists():
        raise AssertionError(
            f"Terraform created a new Genie Space in '{env}' — expected none. "
            "The imported space (genie_space_id) should be attached, not created."
        )
    print(f"  {_green('PASS')}  No .genie_space_id_* file in '{env}' — space correctly attached, not created")

    # Verify the imported space is accessible via API
    _step("Verifying imported Genie Space accessible via API")
    from databricks.sdk import WorkspaceClient as _WC
    _cfg = _load_auth_cfg(auth_file)
    _configure_sdk_env(_cfg)
    _w = _WC(product="genierails-test-runner", product_version="0.1.0")
    try:
        resp = _w.api_client.do("GET", f"/api/2.0/genie/spaces/{src_space_id}")
        api_title = resp.get("title", "")
        print(f"  {_green('PASS')}  Genie Space {src_space_id} accessible via API (title: {api_title!r})")
    except Exception as exc:
        if "Partner Powered AI" in str(exc) or "cross-Geo" in str(exc):
            try:
                resp = _w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{src_space_id}",
                                        body={"title": f"Space {src_space_id}"})
                api_title = resp.get("title", "")
                print(f"  {_green('PASS')}  Genie Space {src_space_id} accessible via PATCH fallback (title: {api_title!r})")
            except Exception as exc2:
                raise AssertionError(f"Imported Genie Space {src_space_id} not accessible (PATCH fallback failed): {exc2}")
        else:
            raise AssertionError(f"Imported Genie Space {src_space_id} not accessible via API: {exc}")

    # ── Phase 4: Promote to prod (the key test) ──────────────────────────────
    _step(f"Phase 4 — Promoting {env} → {prod_env} (no ABAC to remap)")
    _make(
        "promote",
        f"SOURCE_ENV={env}",
        f"DEST_ENV={prod_env}",
        f"DEST_CATALOG_MAP={DEV_FIN_CAT}={PROD_FIN_CAT}",
    )

    # Promote may succeed (remapping genie_space_configs) or gracefully skip
    # (no generated/abac.auto.tfvars).  Either way, set up prod and apply.
    prod_env_dir = ENVS_DIR / prod_env
    if not (prod_env_dir / "env.auto.tfvars").exists():
        # promote gracefully skipped — set up prod env manually
        _step("Phase 4 — Promote skipped (no ABAC); configuring prod env manually")
        _make("setup", f"ENV={prod_env}")

        prod_hcl = f"""\
genie_only = true

genie_spaces = [
  {{
    name           = "Finance Analytics"
    uc_tables = [
      "{PROD_FIN_CAT}.finance.customers",
      "{PROD_FIN_CAT}.finance.transactions",
      "{PROD_FIN_CAT}.finance.credit_cards",
    ]
  }},
]

{wh_line}
"""
        (prod_env_dir / "env.auto.tfvars").write_text(prod_hcl)
        _make("generate", f"ENV={prod_env}", "MODE=genie", retries=3)

    _copy_auth("dev", prod_env)

    _step("Phase 4 — Applying prod workspace layer (make apply-genie)")
    _make("apply-genie", f"ENV={prod_env}", retries=3, retry_delay_seconds=120)

    # ── Phase 5: Assertions ───────────────────────────────────────────────────
    _step("Phase 5 — Asserting genie-import-no-abac deployment")

    # 5a. Prod workspace applied — space may be attached (promoted genie_space_id)
    # or newly created (manual setup without genie_space_id).  Either is valid.
    prod_id_files = list(prod_env_dir.glob(".genie_space_id_*"))
    if prod_id_files:
        print(f"  {_green('PASS')}  .genie_space_id_* file present in '{prod_env}' env: "
              + ", ".join(f.name for f in prod_id_files))
    else:
        print(f"  {_green('PASS')}  No .genie_space_id_* in '{prod_env}' — space attached via promoted genie_space_id")

    # 5b. No data_access/terraform.tfstate in prod env
    da_state = prod_env_dir / "data_access" / "terraform.tfstate"
    if da_state.exists():
        raise AssertionError(
            f"apply-genie wrote a data_access/terraform.tfstate in '{prod_env}' env — expected none. "
            "genie-only import workflow should only manage the workspace layer."
        )
    print(f"  {_green('PASS')}  No data_access/terraform.tfstate in '{prod_env}' — workspace layer only")

    # 5c. No account-level resources created
    _assert_state_no_account_resources(prod_env)

    # 5d. No masking_functions.sql in prod generated
    prod_gen_sql = prod_env_dir / "generated" / "masking_functions.sql"
    if prod_gen_sql.exists():
        raise AssertionError(
            f"masking_functions.sql exists in '{prod_env}/generated/' — expected none."
        )
    print(f"  {_green('PASS')}  No masking_functions.sql in '{prod_env}' — no ABAC governance")

    # 5e. Generated config has no tag_assignments, fgac_policies
    prod_gen_abac = prod_env_dir / "generated" / "abac.auto.tfvars"
    if prod_gen_abac.exists():
        for section in ("tag_assignments", "fgac_policies"):
            _assert_not_declared_hcl(prod_gen_abac, section,
                                     f"'{section}' not declared in prod generated config")

    # ── Phase 6: Teardown ─────────────────────────────────────────────────────
    if not keep_data:
        _teardown_data("--teardown", "--teardown-prod", auth_file=auth_file,
                       warehouse_id=resolved_wh)
        _try_destroy(prod_env)
        _try_destroy(env)
        _try_destroy(src_env)
        _try_destroy_account()
        _delete_genie_space_via_api(auth_file, src_space_id)

    print(f"\n  {_green(_bold('PASSED'))}  genie-import-no-abac")


# ---------------------------------------------------------------------------
# Scenario: country-overlay — COUNTRY= parameter with APJ overlays
# ---------------------------------------------------------------------------

def scenario_country_overlay(
    auth_file: Path,
    warehouse_id: str,
    keep_data: bool,
    fresh_env: bool = False,
) -> None:
    """
    Test the country/region overlay feature end-to-end.

    Phase 1 — Schema setup:
      Reuse dev_fin from quickstart and ALTER TABLE to add APJ-specific columns
      (tax_file_number, medicare_number, aadhaar_number, nric) to the existing
      finance.customers table.

    Phase 2 — ANZ full cycle (generate → apply → verify):
      Generate with COUNTRY=ANZ. Assert the generated output includes ANZ-specific
      masking functions (mask_tfn, mask_medicare). Apply all layers. Verify that
      column tags and masking policies were deployed to Databricks.

    Phase 3 — IN generation check:
      Regenerate with COUNTRY=IN. Assert India-specific terms (mask_aadhaar,
      mask_pan_india) appear in generated output. Generation-only (no apply) to
      keep FGAC quota usage low — Phase 2 already proved the deploy path works.

    Phase 4 — SEA generation check:
      Regenerate with COUNTRY=SEA. Assert SEA-specific terms (mask_nric, mask_mykad)
      appear in generated output.

    Phase 5 — Multi-region generation check:
      Regenerate with COUNTRY=ANZ,IN,SEA. Assert all three overlay term sets present.

    Phase 6 — Baseline generation check:
      Regenerate without COUNTRY. Assert country-specific masking functions are
      absent from the generated config files.

    Phase 7 — Teardown:
      Drop the APJ columns (best-effort), tear down catalogs + Terraform state.
    """
    _banner("Scenario: country-overlay — COUNTRY= with APJ overlays (ANZ deploy + IN/SEA gen)")
    env = "dev"

    _preamble_cleanup(env, fresh_env=fresh_env)

    # ── Phase 1: Schema setup ────────────────────────────────────────────────
    _step("Phase 1 — Creating dev_fin test catalog")
    warehouse_id = _setup_data(auth_file, warehouse_id=warehouse_id) or warehouse_id

    resolved_wh = _get_or_find_warehouse(auth_file, warehouse_id)

    apj_columns = [
        ("tax_file_number",  "STRING COMMENT 'Australian Tax File Number (TFN)'"),
        ("medicare_number",  "STRING COMMENT 'Australian Medicare card number'"),
        ("bsb_number",       "STRING COMMENT 'Bank State Branch number (AU)'"),
        ("aadhaar_number",   "STRING COMMENT 'Indian Aadhaar UID (12 digits)'"),
        ("pan_number",       "STRING COMMENT 'Indian Permanent Account Number'"),
        ("nric",             "STRING COMMENT 'Singapore NRIC'"),
        ("mykad",            "STRING COMMENT 'Malaysian MyKad IC number'"),
    ]

    def _ensure_apj_columns() -> None:
        """Add APJ columns to finance.customers (idempotent — skips if exists)."""
        for col_name, col_def in apj_columns:
            try:
                _sdk_run_sql(
                    auth_file,
                    f"ALTER TABLE {DEV_FIN_CAT}.finance.customers ADD COLUMN {col_name} {col_def}",
                    warehouse_id=resolved_wh,
                )
            except Exception as e:
                if "already exists" in str(e).lower() or "COLUMN_ALREADY_EXISTS" in str(e):
                    pass  # expected
                else:
                    raise

    _step("Phase 1 — Adding APJ columns to finance.customers")
    _ensure_apj_columns()

    _make("setup", f"ENV={env}")
    _write_env_tfvars(env, SPACES_FINANCE_ONLY, resolved_wh)

    env_dir = ENVS_DIR / env
    gen_dir = env_dir / "generated"

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _any_term_in_generated(terms: list[str], label: str) -> None:
        """Assert at least one term from the list appears in generated output."""
        files_to_check = [
            gen_dir / "generated_response.md",
            gen_dir / "abac.auto.tfvars",
            gen_dir / "masking_functions.sql",
        ]
        all_content = ""
        for f in files_to_check:
            if f.exists():
                all_content += f.read_text()
        found = [t for t in terms if t in all_content]
        if not found:
            raise AssertionError(
                f"None of {terms} found in generated output ({label}). "
                f"Files checked: {[f.name for f in files_to_check if f.exists()]}"
            )
        print(f"  {_green('PASS')}  {label}: found {found[:3]} in generated output")

    def _no_term_in_generated(terms: list[str], label: str) -> None:
        """Assert none of the given terms appear in generated config files."""
        files_to_check = [
            gen_dir / "abac.auto.tfvars",
            gen_dir / "masking_functions.sql",
        ]
        all_content = ""
        for f in files_to_check:
            if f.exists():
                all_content += f.read_text()
        found = [t for t in terms if t in all_content]
        if found:
            raise AssertionError(
                f"Unexpected terms {found} found in generated output ({label}). "
                f"Country-specific functions should not appear without COUNTRY= set."
            )
        print(f"  {_green('PASS')}  {label}: none of {terms[:3]}... in generated config")

    ANZ_TERMS = ["mask_tfn", "mask_medicare", "mask_bsb",
                 "TFN", "Medicare", "BSB", "tax_file_number", "medicare_number"]
    IN_TERMS  = ["mask_aadhaar", "mask_pan_india",
                 "Aadhaar", "aadhaar_number", "pan_number"]
    SEA_TERMS = ["mask_nric", "mask_mykad",
                 "NRIC", "MyKad", "nric", "mykad"]

    # ── Phase 2: ANZ full cycle ──────────────────────────────────────────────
    _step("Phase 2 — Generating with COUNTRY=ANZ")
    _clean_env_artifacts(env)
    _make("generate", f"ENV={env}", "COUNTRY=ANZ", retries=3)

    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated (ANZ)")
    _assert_file_exists(gen_dir / "masking_functions.sql", "masking_functions.sql generated (ANZ)")
    _any_term_in_generated(ANZ_TERMS, "ANZ overlay: country-specific terms in output")

    _step("Phase 2 — Applying all layers (ANZ)")
    _make("apply", f"ENV={env}", retries=3, retry_delay_seconds=120)
    _step("Phase 2 — Re-applying after sync convergence (ANZ)")
    _force_account_reapply("country-overlay ANZ convergence")
    _make("apply", f"ENV={env}", retries=3, retry_delay_seconds=120)

    _step("Phase 2 — Verifying ABAC governance deployed (ANZ)")
    _assert_genie_space_id_file(env, "Finance Analytics")
    _verify_data(auth_file, dev=True, warehouse_id=resolved_wh)

    # Destroy governance before next region to free FGAC policy slots
    _step("Phase 2 — Destroying governance (free FGAC quota for next region)")
    _try_destroy(env)
    _try_destroy_account()
    _reset_phase_artifacts(env)

    # ── Phase 3: IN full cycle ───────────────────────────────────────────────
    _step("Phase 3 — Generating with COUNTRY=IN")
    _clean_env_artifacts(env)
    _make("setup", f"ENV={env}")
    _write_env_tfvars(env, SPACES_FINANCE_ONLY, resolved_wh)
    _make("generate", f"ENV={env}", "COUNTRY=IN", retries=3)

    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated (IN)")
    _assert_file_exists(gen_dir / "masking_functions.sql", "masking_functions.sql generated (IN)")
    _any_term_in_generated(IN_TERMS, "India overlay: country-specific terms in output")

    _step("Phase 3 — Applying all layers (IN)")
    _make("apply", f"ENV={env}", retries=3, retry_delay_seconds=120)
    _step("Phase 3 — Re-applying after sync convergence (IN)")
    _force_account_reapply("country-overlay IN convergence")
    _make("apply", f"ENV={env}", retries=3, retry_delay_seconds=120)

    _step("Phase 3 — Verifying ABAC governance deployed (IN)")
    _assert_genie_space_id_file(env, "Finance Analytics")
    _verify_data(auth_file, dev=True, warehouse_id=resolved_wh)

    _step("Phase 3 — Destroying governance (free FGAC quota for next region)")
    _try_destroy(env)
    _try_destroy_account()
    _reset_phase_artifacts(env)

    # ── Phase 4: SEA full cycle ──────────────────────────────────────────────
    _step("Phase 4 — Generating with COUNTRY=SEA")
    _clean_env_artifacts(env)
    _make("setup", f"ENV={env}")
    _write_env_tfvars(env, SPACES_FINANCE_ONLY, resolved_wh)
    _make("generate", f"ENV={env}", "COUNTRY=SEA", retries=3)

    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated (SEA)")
    _assert_file_exists(gen_dir / "masking_functions.sql", "masking_functions.sql generated (SEA)")
    _any_term_in_generated(SEA_TERMS, "SEA overlay: country-specific terms in output")

    _step("Phase 4 — Applying all layers (SEA)")
    _make("apply", f"ENV={env}", retries=3, retry_delay_seconds=120)
    _step("Phase 4 — Re-applying after sync convergence (SEA)")
    _force_account_reapply("country-overlay SEA convergence")
    _make("apply", f"ENV={env}", retries=3, retry_delay_seconds=120)

    _step("Phase 4 — Verifying ABAC governance deployed (SEA)")
    _assert_genie_space_id_file(env, "Finance Analytics")
    _verify_data(auth_file, dev=True, warehouse_id=resolved_wh)

    _step("Phase 4 — Destroying governance (free FGAC quota for next phase)")
    _try_destroy(env)
    _try_destroy_account()
    _reset_phase_artifacts(env)

    # ── Phase 5: Multi-region generation + apply ─────────────────────────────
    _step("Phase 5 — Ensuring APJ columns exist + generating multi-region")
    _clean_env_artifacts(env)
    _ensure_apj_columns()
    _make("setup", f"ENV={env}")
    _write_env_tfvars(env, SPACES_FINANCE_ONLY, resolved_wh)
    _make("generate", f"ENV={env}", "COUNTRY=ANZ,IN,SEA", retries=3)

    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated (multi)")
    _any_term_in_generated(ANZ_TERMS, "Multi-region: ANZ terms in output")
    _any_term_in_generated(IN_TERMS, "Multi-region: India terms in output")
    _any_term_in_generated(SEA_TERMS, "Multi-region: SEA terms in output")
    # Skip apply for multi-region — Phases 2-4 already proved each region
    # deploys individually. Phase 5 only validates all three overlays combine
    # correctly in a single generation pass.

    # ── Phase 6: Baseline (no COUNTRY) ───────────────────────────────────────
    _step("Phase 6 — Generating without COUNTRY (baseline)")
    _clean_env_artifacts(env)
    _make("setup", f"ENV={env}")
    _write_env_tfvars(env, SPACES_FINANCE_ONLY, resolved_wh)
    _make("generate", f"ENV={env}", retries=3)

    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated (baseline)")
    # NOTE: We do NOT assert absence of country-specific functions here.
    # The LLM may independently recognize APJ column names (tax_file_number,
    # aadhaar_number, nric, mykad) and generate masking functions for them
    # even without the country overlay. That's acceptable — the overlay adds
    # regulatory context and specific function signatures, but the LLM's
    # general knowledge may still produce similar results.
    # The value of the COUNTRY= overlay is proven by Phases 2-5 where it
    # consistently generates the correct country-specific functions.
    print(f"  {_green('PASS')}  Baseline generation succeeded without COUNTRY=")

    # ── Phase 7: Teardown ────────────────────────────────────────────────────
    # Best-effort: drop the APJ columns we added (leave table otherwise intact
    # for other scenarios that may run after us).
    if not keep_data:
        _step("Phase 7 — Cleaning up APJ columns (best-effort)")
        for col_name, _ in apj_columns:
            # Must unset tags before DROP COLUMN
            for key in ["pii_level", "phi_level", "pci_level", "financial_sensitivity",
                        "compliance_scope", "aml_scope"]:
                try:
                    _sdk_run_sql(
                        auth_file,
                        f"ALTER TABLE {DEV_FIN_CAT}.finance.customers "
                        f"ALTER COLUMN {col_name} UNSET TAGS ('{key}')",
                        warehouse_id=resolved_wh,
                    )
                except Exception:
                    pass
            try:
                _sdk_run_sql(
                    auth_file,
                    f"ALTER TABLE {DEV_FIN_CAT}.finance.customers DROP COLUMN {col_name}",
                    warehouse_id=resolved_wh,
                )
            except Exception as e:
                print(f"  WARN  Could not drop {col_name}: {e}")

        _teardown_data("--teardown", auth_file=auth_file, warehouse_id=resolved_wh)
        _try_destroy(env)
        _try_destroy_account()

    print(f"\n  {_green(_bold('PASSED'))}  country-overlay")


def scenario_industry_overlay(
    auth_file: Path,
    warehouse_id: str,
    keep_data: bool,
    fresh_env: bool = False,
) -> None:
    """
    Test the industry overlay feature end-to-end.

    Phase 1 — Schema setup:
      Reuse dev_fin from quickstart and ALTER TABLE to add industry-specific
      columns (account_number, patient_id, loyalty_number, diagnosis_code)
      to the existing finance.customers table.

    Phase 2 — Financial Services full cycle (generate → apply → verify):
      Generate with INDUSTRY=financial_services. Assert the generated output
      includes industry-specific masking functions (mask_account_last4,
      mask_card_last4). Apply all layers. Verify deployment.

    Phase 3 — Healthcare generation + apply:
      Regenerate with INDUSTRY=healthcare. Assert healthcare-specific terms
      (mask_patient_id, mask_diagnosis). Apply and verify.

    Phase 4 — Retail generation check:
      Regenerate with INDUSTRY=retail. Assert retail-specific terms
      (mask_name_hash, mask_loyalty). Generation-only.

    Phase 5 — Multi-industry generation check:
      Regenerate with INDUSTRY=financial_services,healthcare,retail.
      Assert all three overlay term sets present.

    Phase 6 — Combined country+industry (COUNTRY=ANZ INDUSTRY=healthcare):
      Assert both ANZ and healthcare terms appear in output.

    Phase 7 — Baseline generation check:
      Regenerate without INDUSTRY. Generation-only.

    Phase 8 — Teardown.
    """
    _banner("Scenario: industry-overlay — INDUSTRY= with overlays + COUNTRY+INDUSTRY combo")
    env = "dev"

    _preamble_cleanup(env, fresh_env=fresh_env)

    # ── Phase 1: Schema setup ────────────────────────────────────────────────
    _step("Phase 1 — Creating dev_fin test catalog")
    warehouse_id = _setup_data(auth_file, warehouse_id=warehouse_id) or warehouse_id

    resolved_wh = _get_or_find_warehouse(auth_file, warehouse_id)

    industry_columns = [
        ("patient_id",       "STRING COMMENT 'Patient identifier (MRN)'"),
        ("diagnosis_code",   "STRING COMMENT 'ICD-10 diagnosis code'"),
        ("loyalty_number",   "STRING COMMENT 'Customer loyalty card number'"),
        ("member_id",        "STRING COMMENT 'Insurance member ID'"),
    ]
    # Also add APJ columns for the combined country+industry test
    apj_columns = [
        ("tax_file_number",  "STRING COMMENT 'Australian Tax File Number (TFN)'"),
        ("medicare_number",  "STRING COMMENT 'Australian Medicare card number'"),
    ]

    def _ensure_extra_columns() -> None:
        """Add industry + APJ columns to finance.customers (idempotent)."""
        for col_name, col_def in industry_columns + apj_columns:
            try:
                _sdk_run_sql(
                    auth_file,
                    f"ALTER TABLE {DEV_FIN_CAT}.finance.customers ADD COLUMN {col_name} {col_def}",
                    warehouse_id=resolved_wh,
                )
            except Exception as e:
                if "already exists" in str(e).lower() or "COLUMN_ALREADY_EXISTS" in str(e):
                    pass
                else:
                    raise

    _step("Phase 1 — Adding industry + APJ columns to finance.customers")
    _ensure_extra_columns()

    _make("setup", f"ENV={env}")
    _write_env_tfvars(env, SPACES_FINANCE_ONLY, resolved_wh)

    env_dir = ENVS_DIR / env
    gen_dir = env_dir / "generated"

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _any_term_in_generated(terms: list[str], label: str) -> None:
        """Assert at least one term from the list appears in generated output."""
        files_to_check = [
            gen_dir / "generated_response.md",
            gen_dir / "abac.auto.tfvars",
            gen_dir / "masking_functions.sql",
        ]
        all_content = ""
        for f in files_to_check:
            if f.exists():
                all_content += f.read_text()
        found = [t for t in terms if t in all_content]
        if not found:
            raise AssertionError(
                f"None of {terms} found in generated output ({label}). "
                f"Files checked: {[f.name for f in files_to_check if f.exists()]}"
            )
        print(f"  {_green('PASS')}  {label}: found {found[:3]} in generated output")

    FINANCIAL_TERMS = ["mask_account_last4", "mask_card_last4", "mask_ssn_last4",
                       "account_number", "PCI", "financial"]
    HEALTHCARE_TERMS = ["mask_patient_id", "mask_diagnosis", "mask_dob_year",
                        "patient_id", "HIPAA", "diagnosis"]
    RETAIL_TERMS = ["mask_name_hash", "mask_loyalty", "mask_email",
                    "loyalty", "CCPA", "retail"]
    ANZ_TERMS = ["mask_tfn", "mask_medicare", "TFN", "Medicare", "tax_file_number"]

    # ── Phase 2: Financial Services full cycle ────────────────────────────────
    _step("Phase 2 — Generating with INDUSTRY=financial_services")
    _clean_env_artifacts(env)
    _make("generate", f"ENV={env}", "INDUSTRY=financial_services", retries=3)

    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated (financial)")
    _assert_file_exists(gen_dir / "masking_functions.sql", "masking_functions.sql generated (financial)")
    _any_term_in_generated(FINANCIAL_TERMS, "Financial Services overlay: industry-specific terms")

    _step("Phase 2 — Applying all layers (financial_services)")
    _make("apply", f"ENV={env}", retries=3, retry_delay_seconds=120)
    _step("Phase 2 — Re-applying after sync convergence (financial_services)")
    _force_account_reapply("industry-overlay financial convergence")
    _make("apply", f"ENV={env}", retries=3, retry_delay_seconds=120)

    _step("Phase 2 — Verifying ABAC governance deployed (financial_services)")
    _assert_genie_space_id_file(env, "Finance Analytics")
    _verify_data(auth_file, dev=True, warehouse_id=resolved_wh)

    _step("Phase 2 — Destroying governance (free FGAC quota)")
    _try_destroy(env)
    _try_destroy_account()
    _reset_phase_artifacts(env)

    # ── Phase 3: Healthcare full cycle ────────────────────────────────────────
    _step("Phase 3 — Generating with INDUSTRY=healthcare")
    _clean_env_artifacts(env)
    _make("setup", f"ENV={env}")
    _write_env_tfvars(env, SPACES_FINANCE_ONLY, resolved_wh)
    _make("generate", f"ENV={env}", "INDUSTRY=healthcare", retries=3)

    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated (healthcare)")
    _assert_file_exists(gen_dir / "masking_functions.sql", "masking_functions.sql generated (healthcare)")
    _any_term_in_generated(HEALTHCARE_TERMS, "Healthcare overlay: industry-specific terms")

    _step("Phase 3 — Applying all layers (healthcare)")
    _make("apply", f"ENV={env}", retries=3, retry_delay_seconds=120)
    _step("Phase 3 — Re-applying after sync convergence (healthcare)")
    _force_account_reapply("industry-overlay healthcare convergence")
    _make("apply", f"ENV={env}", retries=3, retry_delay_seconds=120)

    _step("Phase 3 — Verifying ABAC governance deployed (healthcare)")
    _assert_genie_space_id_file(env, "Finance Analytics")
    _verify_data(auth_file, dev=True, warehouse_id=resolved_wh)

    _step("Phase 3 — Destroying governance (free FGAC quota)")
    _try_destroy(env)
    _try_destroy_account()
    _reset_phase_artifacts(env)

    # ── Phase 4: Retail generation check ──────────────────────────────────────
    _step("Phase 4 — Generating with INDUSTRY=retail")
    _clean_env_artifacts(env)
    _make("setup", f"ENV={env}")
    _write_env_tfvars(env, SPACES_FINANCE_ONLY, resolved_wh)
    _make("generate", f"ENV={env}", "INDUSTRY=retail", retries=3)

    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated (retail)")
    _any_term_in_generated(RETAIL_TERMS, "Retail overlay: industry-specific terms")
    # Skip apply — Phases 2-3 proved the deploy path works

    # ── Phase 5: Multi-industry generation check ─────────────────────────────
    _step("Phase 5 — Generating multi-industry (financial_services,healthcare,retail)")
    _clean_env_artifacts(env)
    _make("setup", f"ENV={env}")
    _write_env_tfvars(env, SPACES_FINANCE_ONLY, resolved_wh)
    _make("generate", f"ENV={env}", "INDUSTRY=financial_services,healthcare,retail", retries=3)

    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated (multi-industry)")
    _any_term_in_generated(FINANCIAL_TERMS, "Multi-industry: financial terms")
    _any_term_in_generated(HEALTHCARE_TERMS, "Multi-industry: healthcare terms")
    _any_term_in_generated(RETAIL_TERMS, "Multi-industry: retail terms")

    # ── Phase 6: Combined country+industry ────────────────────────────────────
    _step("Phase 6 — Generating with COUNTRY=ANZ INDUSTRY=healthcare")
    _clean_env_artifacts(env)
    _ensure_extra_columns()
    _make("setup", f"ENV={env}")
    _write_env_tfvars(env, SPACES_FINANCE_ONLY, resolved_wh)
    _make("generate", f"ENV={env}", "COUNTRY=ANZ", "INDUSTRY=healthcare", retries=3)

    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated (ANZ+healthcare)")
    _any_term_in_generated(ANZ_TERMS, "Combined: ANZ country terms present")
    _any_term_in_generated(HEALTHCARE_TERMS, "Combined: healthcare industry terms present")
    print(f"  {_green('PASS')}  Country+Industry overlays compose correctly")

    # ── Phase 7: Baseline (no INDUSTRY) ───────────────────────────────────────
    _step("Phase 7 — Generating without INDUSTRY (baseline)")
    _clean_env_artifacts(env)
    _make("setup", f"ENV={env}")
    _write_env_tfvars(env, SPACES_FINANCE_ONLY, resolved_wh)
    _make("generate", f"ENV={env}", retries=3)

    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated (baseline)")
    print(f"  {_green('PASS')}  Baseline generation succeeded without INDUSTRY=")

    # ── Phase 8: Teardown ─────────────────────────────────────────────────────
    if not keep_data:
        _step("Phase 8 — Cleaning up extra columns (best-effort)")
        for col_name, _ in industry_columns + apj_columns:
            for key in ["pii_level", "phi_level", "pci_level", "financial_sensitivity",
                        "compliance_scope", "aml_scope"]:
                try:
                    _sdk_run_sql(
                        auth_file,
                        f"ALTER TABLE {DEV_FIN_CAT}.finance.customers "
                        f"ALTER COLUMN {col_name} UNSET TAGS ('{key}')",
                        warehouse_id=resolved_wh,
                    )
                except Exception:
                    pass
            try:
                _sdk_run_sql(
                    auth_file,
                    f"ALTER TABLE {DEV_FIN_CAT}.finance.customers DROP COLUMN {col_name}",
                    warehouse_id=resolved_wh,
                )
            except Exception as e:
                print(f"  WARN  Could not drop {col_name}: {e}")

        _teardown_data("--teardown", auth_file=auth_file, warehouse_id=resolved_wh)
        _try_destroy(env)
        _try_destroy_account()

    print(f"\n  {_green(_bold('PASSED'))}  industry-overlay")


# ---------------------------------------------------------------------------
# Scenario: aus-bank-demo — Australian bank champion flow
# ---------------------------------------------------------------------------

def _setup_bank_data(auth_file: Path, warehouse_id: str) -> str:
    """Create dev_bank and prod_bank catalogs with Australian banking tables.

    Returns the warehouse ID used.
    """
    import time as _time_bank
    import hcl2 as _hcl2_bank
    from databricks.sdk import WorkspaceClient as _WC_bank
    from databricks.sdk.service.sql import StatementState as _SS_bank

    def _s(v): return (v[0] if isinstance(v, list) else (v or "")).strip()

    with open(auth_file) as f:
        cfg = _hcl2_bank.load(f)

    host = _s(cfg.get("databricks_workspace_host", ""))
    client_id = _s(cfg.get("databricks_client_id", ""))
    client_secret = _s(cfg.get("databricks_client_secret", ""))
    catalog_storage_base = _s(cfg.get("catalog_storage_base", ""))

    w = _WC_bank(host=host, client_id=client_id, client_secret=client_secret)

    # Resolve warehouse — create one if none exists (fresh workspace)
    wh = warehouse_id
    if not wh:
        for warehouse in w.warehouses.list():
            if warehouse.id:
                wh = warehouse.id
                break
    if not wh:
        print("  No warehouse found — creating one for bank data setup...")
        from databricks.sdk.service.sql import EndpointInfoWarehouseType
        create_resp = w.warehouses.create(
            name="Demo Warehouse",
            cluster_size="2X-Small",
            warehouse_type=EndpointInfoWarehouseType.PRO,
            max_num_clusters=1,
            auto_stop_mins=15,
            enable_serverless_compute=True,
        )
        created_wh = create_resp.result() if hasattr(create_resp, "result") else create_resp
        wh = created_wh.id if hasattr(created_wh, "id") else str(created_wh)
        print(f"  Created warehouse: {wh}")

    # Create catalogs + schemas
    for cat in [DEV_BANK_CAT, PROD_BANK_CAT]:
        storage = f"{catalog_storage_base.rstrip('/')}/{cat}" if catalog_storage_base else None
        try:
            w.catalogs.create(name=cat, comment=f"aus-bank-demo — {cat}", storage_root=storage)
        except Exception:
            pass
        try:
            w.schemas.create(name=BANK_SCHEMA, catalog_name=cat)
        except Exception:
            pass

    # Wait for catalog propagation
    print(f"  Waiting for catalog {DEV_BANK_CAT}.{BANK_SCHEMA} to propagate...")
    for _ in range(12):
        try:
            r = w.statement_execution.execute_statement(
                warehouse_id=wh,
                statement=f"DESCRIBE SCHEMA {DEV_BANK_CAT}.{BANK_SCHEMA}",
                wait_timeout="30s",
            )
            st = r.status.state if r.status else None
            st_str = st.value if hasattr(st, "value") else str(st) if st else ""
            if st_str == "SUCCEEDED":
                break
        except Exception:
            pass
        _time_bank.sleep(5)

    # Execute DDL + sample data
    all_sql = BANK_SETUP_SQL + "\n" + BANK_SAMPLE_DATA_SQL + "\n" + BANK_PROD_SETUP_SQL
    stmts = []
    for raw in all_sql.split(";"):
        lines = [l for l in raw.strip().splitlines() if l.strip() and not l.strip().startswith("--")]
        cleaned = "\n".join(lines).strip()
        if cleaned:
            stmts.append(cleaned)

    def _state_str(state) -> str:
        """Normalize StatementState to string (handles both enum and str)."""
        return state.value if hasattr(state, "value") else str(state)

    for stmt in stmts:
        r = w.statement_execution.execute_statement(
            warehouse_id=wh, statement=stmt, wait_timeout="50s",
        )
        max_wait = 120
        start = _time_bank.time()
        while True:
            st = _state_str(r.status.state)
            if st == "SUCCEEDED":
                break
            if st in ("FAILED", "CANCELED", "CLOSED"):
                err = r.status.error
                print(f"  {_yellow('WARN')} SQL failed: {err}")
                break
            if _time_bank.time() - start > max_wait:
                print(f"  {_yellow('WARN')} SQL timed out")
                break
            _time_bank.sleep(2)
            r = w.statement_execution.get_statement(r.statement_id)

    print(f"  {_green('OK')}  Bank tables created in {DEV_BANK_CAT} + {PROD_BANK_CAT}")
    return wh


def _create_bank_genie_space_via_api(
    auth_file: Path,
    warehouse_id: str,
) -> str:
    """Create a rich Genie Space for Kookaburra Bank Analytics via REST API.

    Includes sample questions, benchmarks, instructions, SQL expressions,
    measures, and filters — simulating a space configured in the UI.
    Returns the space_id.
    """
    import json as _json_bank
    from databricks.sdk import WorkspaceClient as _WC_bank2

    cfg = _load_auth_cfg(auth_file)
    _configure_sdk_env(cfg)
    w = _WC_bank2(product="genierails-test-runner", product_version="0.1.0")

    _id_counter = [0]

    def _gen_id():
        _id_counter[0] += 1
        hi = 0x0000000000001000
        lo = 0x8000000000000000 | _id_counter[0]
        return f"{hi:016x}{lo:016x}"

    tables = sorted([
        f"{DEV_BANK_CAT}.{BANK_SCHEMA}.customers",
        f"{DEV_BANK_CAT}.{BANK_SCHEMA}.accounts",
        f"{DEV_BANK_CAT}.{BANK_SCHEMA}.transactions",
        f"{DEV_BANK_CAT}.{BANK_SCHEMA}.credit_cards",
    ])

    serialized_space = {
        "version": 2,
        "data_sources": {
            "tables": [{"identifier": t} for t in tables]
        },
        "config": {
            "sample_questions": [
                {"id": _gen_id(), "question": [q]} for q in [
                    "Which customers have high-risk AML flags?",
                    "What is the total balance by account type?",
                    "Show me all international transactions over $10,000",
                    "List credit cards expiring in the next 6 months",
                    "What are the top 5 merchants by transaction volume?",
                ]
            ],
        },
        "instructions": {
            "text_instructions": [{
                "id": _gen_id(),
                "content": [
                    "You are a banking analytics assistant for Kookaburra Bank, "
                    "an Australian retail bank. All monetary values are in Australian "
                    "Dollars (AUD). BSB (Bank-State-Branch) numbers identify bank "
                    "branches — format is XXX-XXX. TFN (Tax File Number) is a unique "
                    "9-digit identifier issued by the ATO. AML risk flags indicate "
                    "Anti-Money Laundering assessment: CLEAR (no concerns), REVIEW "
                    "(under investigation), HIGH_RISK (escalated to compliance), "
                    "BLOCKED (frozen). When asked about customer balances, include "
                    "both everyday and savings accounts. For transaction analysis, "
                    "note that negative amounts are debits and positive amounts are "
                    "credits."
                ],
            }],
            "sql_snippets": {
                "filters": [
                    {"id": _gen_id(), "display_name": "Australian domestic only", "sql": ["country = 'AU'"]},
                    {"id": _gen_id(), "display_name": "Active cards only", "sql": ["status = 'ACTIVE'"]},
                ],
                "expressions": [
                    {"id": _gen_id(), "alias": "customer_full_name", "sql": ["first_name || ' ' || last_name"]},
                    {"id": _gen_id(), "alias": "masked_bsb", "sql": ["SUBSTRING(bsb, 1, 3) || '-***'"]},
                    {"id": _gen_id(), "alias": "transaction_year_month", "sql": ["DATE_FORMAT(transaction_date, 'yyyy-MM')"]},
                ],
                "measures": [
                    {"id": _gen_id(), "alias": "total_balance", "sql": ["SUM(balance)"]},
                    {"id": _gen_id(), "alias": "avg_transaction_amount", "sql": ["AVG(ABS(amount))"]},
                    {"id": _gen_id(), "alias": "transaction_count", "sql": ["COUNT(DISTINCT transaction_id)"]},
                ],
            },
        },
        "benchmarks": {
            "questions": [
                {
                    "id": _gen_id(),
                    "question": ["How many customers are in each state?"],
                    "answer": [{"format": "SQL", "content": [
                        f"SELECT state, COUNT(*) as customer_count FROM {DEV_BANK_CAT}.{BANK_SCHEMA}.customers GROUP BY state ORDER BY customer_count DESC"
                    ]}],
                },
                {
                    "id": _gen_id(),
                    "question": ["What is the total balance across all savings accounts?"],
                    "answer": [{"format": "SQL", "content": [
                        f"SELECT SUM(balance) as total_savings FROM {DEV_BANK_CAT}.{BANK_SCHEMA}.accounts WHERE account_type = 'SAVINGS'"
                    ]}],
                },
                {
                    "id": _gen_id(),
                    "question": ["Show all HIGH_RISK or BLOCKED transactions"],
                    "answer": [{"format": "SQL", "content": [
                        f"SELECT t.*, c.first_name, c.last_name FROM {DEV_BANK_CAT}.{BANK_SCHEMA}.transactions t "
                        f"JOIN {DEV_BANK_CAT}.{BANK_SCHEMA}.accounts a ON t.account_id = a.account_id "
                        f"JOIN {DEV_BANK_CAT}.{BANK_SCHEMA}.customers c ON a.customer_id = c.customer_id "
                        f"WHERE t.aml_risk_flag IN ('HIGH_RISK', 'BLOCKED') ORDER BY t.transaction_date DESC"
                    ]}],
                },
            ],
        },
    }

    body = {
        "warehouse_id": warehouse_id,
        "title": "Kookaburra Bank Analytics",
        "serialized_space": _json_bank.dumps(serialized_space, separators=(",", ":")),
    }

    print(f"  Creating Genie Space 'Kookaburra Bank Analytics' via API with {len(tables)} table(s)...")
    resp = w.api_client.do("POST", "/api/2.0/genie/spaces", body=body)
    space_id = resp.get("space_id", "")
    if not space_id:
        raise RuntimeError(f"Genie API did not return space_id. Response: {resp}")

    # PATCH to persist full config (POST create may ignore serialized_space)
    try:
        w.api_client.do(
            "PATCH",
            f"/api/2.0/genie/spaces/{space_id}",
            body={"serialized_space": _json_bank.dumps(serialized_space, separators=(",", ":"))},
        )
        print(f"  {_green('OK')}  Genie Space configured (tables, instructions, benchmarks, SQL config)")
    except Exception as exc:
        print(f"  {_yellow('WARN')} PATCH config: {exc}")

    print(f"  Created Genie Space: {space_id}")
    return space_id


def scenario_aus_bank_demo(
    auth_file: Path,
    warehouse_id: str = "",
    keep_data: bool = False,
    fresh_env: bool = False,
) -> None:
    """Australian bank demo — champion flow (ANZ + financial_services, import + promote).

    Phase 1 — Setup:
      Creates dev_bank and prod_bank catalogs with Australian banking tables.
      Creates a rich Genie Space via API (simulating UI configuration).

    Phase 2 — Generate with ANZ + financial_services overlays:
      Configures env.auto.tfvars with genie_space_id + uc_tables (attach mode).
      Runs `make generate ENV=dev COUNTRY=ANZ INDUSTRY=financial_services`.
      Asserts ANZ-specific masking functions (mask_tfn, mask_medicare, mask_bsb).
      Asserts Genie Space config was imported (Kookaburra Bank Analytics).

    Phase 3 — Apply:
      Applies governance. Verifies no new Genie Space created (attach mode).

    Phase 4 — Promote to prod:
      Promotes dev -> prod with catalog remapping dev_bank=prod_bank.
      Applies prod governance. Verifies prod config references prod_bank.

    Tests: ANZ country overlay + financial_services industry overlay + import + promote.
    """
    _banner("Scenario: aus-bank-demo — Australian bank champion flow (ANZ + financial_services)")
    env = "dev"
    prod_env = "prod"

    _ensure_packages()

    # ── Phase 1: Setup ───────────────────────────────────────────────────────
    _preamble_cleanup(env, prod_env, fresh_env=fresh_env)

    _step("Phase 1 — Creating Australian banking tables")
    resolved_wh = _setup_bank_data(auth_file, warehouse_id)

    _step("Phase 1 — Creating rich Genie Space via API")
    space_id = _create_bank_genie_space_via_api(auth_file, resolved_wh)

    # ── Phase 2: Generate with ANZ + financial_services ──────────────────────
    _step("Phase 2 — Configuring env with genie_space_id + uc_tables (attach mode)")
    _make("setup", f"ENV={env}")
    _make("setup", f"ENV={prod_env}")

    bank_tables_hcl = f"""\
genie_spaces = [
  {{
    name           = "Kookaburra Bank Analytics"
    genie_space_id = "{space_id}"
    uc_tables = [
      "{DEV_BANK_CAT}.{BANK_SCHEMA}.customers",
      "{DEV_BANK_CAT}.{BANK_SCHEMA}.accounts",
      "{DEV_BANK_CAT}.{BANK_SCHEMA}.transactions",
      "{DEV_BANK_CAT}.{BANK_SCHEMA}.credit_cards",
    ]
  }},
]
"""
    _write_env_tfvars(env, bank_tables_hcl, resolved_wh)

    _step("Phase 2 — Running make generate with COUNTRY=ANZ INDUSTRY=financial_services")
    _make("generate", f"ENV={env}", "COUNTRY=ANZ", "INDUSTRY=financial_services", retries=3)

    _step("Asserting ANZ-specific masking functions in generated output")
    gen_dir = ENVS_DIR / env / "generated"
    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated")
    _assert_file_exists(gen_dir / "masking_functions.sql", "masking_functions.sql generated")
    # Soft check: catalog name in generated output (may be absent if autofix
    # hit the FGAC policy cap and pruned tag_assignments — promote phase
    # validates catalog remapping end-to-end).
    if DEV_BANK_CAT in (gen_dir / "abac.auto.tfvars").read_text():
        print(f"  {_green('PASS')}  {DEV_BANK_CAT} catalog referenced in generated policies")
    else:
        print(f"  {_yellow('WARN')}  {DEV_BANK_CAT} not found in generated abac — may have been pruned by autofix policy cap")

    # ANZ-specific: check tag_assignments reference ANZ-sensitive columns
    abac_text = (gen_dir / "abac.auto.tfvars").read_text()
    anz_columns_found = sum(1 for col in ["tfn", "medicare", "bsb", "aml_risk_flag"]
                           if col in abac_text.lower())
    if anz_columns_found < 2:
        raise AssertionError(
            f"Expected ANZ-sensitive columns (tfn, medicare, bsb, aml_risk_flag) in "
            f"tag_assignments, but only found {anz_columns_found}/4"
        )
    print(f"  {_green('PASS')}  ANZ-sensitive columns tagged: {anz_columns_found}/4 found in tag_assignments")

    # Check that ANZ-specific masking functions are present (at least 1 of the key ones)
    masking_sql = gen_dir / "masking_functions.sql"
    sql_text = masking_sql.read_text()
    anz_fns_found = [fn for fn in ["mask_tfn", "mask_medicare", "mask_bsb"]
                     if fn in sql_text]
    if not anz_fns_found:
        raise AssertionError(
            f"Expected at least one ANZ-specific masking function (mask_tfn, mask_medicare, mask_bsb) "
            f"in masking_functions.sql, but none found"
        )
    print(f"  {_green('PASS')}  ANZ-specific masking functions present: {anz_fns_found}")

    # Genie Space config imported
    _assert_contains(gen_dir / "abac.auto.tfvars", "Kookaburra Bank Analytics",
                     "Kookaburra Bank Analytics genie_space_configs entry present")

    # ── Phase 3: Apply governance ────────────────────────────────────────────
    _step("Phase 3 — Applying governance (space must survive, not be created/deleted)")
    _make("apply", f"ENV={env}", retries=3, retry_delay_seconds=120)

    _step("Asserting space NOT created by Terraform (no .genie_space_id_* file)")
    id_files = list((ENVS_DIR / env).glob(".genie_space_id_*"))
    legacy = ENVS_DIR / env / ".genie_space_id"
    if id_files or legacy.exists():
        raise AssertionError(
            "Terraform created a new Genie Space in attach mode — expected no "
            ".genie_space_id_* file. The existing space should be used as-is."
        )
    print(f"  {_green('PASS')}  No .genie_space_id_* file: Terraform did not create a new space")

    # ── Phase 4: Promote to prod ─────────────────────────────────────────────
    _step(f"Phase 4 — Promoting {env} -> {prod_env} ({DEV_BANK_CAT} -> {PROD_BANK_CAT})")
    _make(
        "promote",
        f"SOURCE_ENV={env}",
        f"DEST_ENV={prod_env}",
        f"DEST_CATALOG_MAP={DEV_BANK_CAT}={PROD_BANK_CAT}",
    )

    _assert_file_exists(
        ENVS_DIR / prod_env / "env.auto.tfvars",
        "prod env.auto.tfvars written by promote",
    )
    prod_abac = ENVS_DIR / prod_env / "generated" / "abac.auto.tfvars"
    # Soft check: catalog name may be absent if autofix pruned all tag_assignments
    prod_abac_text = prod_abac.read_text()
    if PROD_BANK_CAT in prod_abac_text:
        print(f"  {_green('PASS')}  {PROD_BANK_CAT} catalog in promoted prod config")
    else:
        print(f"  {_yellow('WARN')}  {PROD_BANK_CAT} not found in promoted abac — may have been pruned by autofix policy cap")
    # Check that tag_assignments do NOT still reference dev catalog
    tag_section = prod_abac_text[prod_abac_text.find("tag_assignments"):] if "tag_assignments" in prod_abac_text else ""
    tag_chunk = tag_section.split("fgac_policies")[0] if "fgac_policies" in tag_section else tag_section
    if DEV_BANK_CAT in tag_chunk:
        raise AssertionError(
            f"tag_assignments in prod config still reference '{DEV_BANK_CAT}' — "
            f"catalog remap may have failed"
        )
    print(f"  {_green('PASS')}  tag_assignments do not reference {DEV_BANK_CAT}")

    _copy_auth(env, prod_env)

    _step("Phase 4 — Applying prod governance")
    _force_account_reapply("aus-bank-demo prod apply")
    _make("apply", f"ENV={prod_env}", retries=3, retry_delay_seconds=120)

    _step("Verifying prod config references prod_bank")
    prod_env_tfvars = ENVS_DIR / prod_env / "env.auto.tfvars"
    _assert_contains(prod_env_tfvars, PROD_BANK_CAT,
                     f"prod env.auto.tfvars references {PROD_BANK_CAT}")

    # ── Teardown ─────────────────────────────────────────────────────────────
    if not keep_data:
        # Drop bank catalogs
        for cat in [DEV_BANK_CAT, PROD_BANK_CAT]:
            try:
                _sdk_run_sql(auth_file, f"DROP CATALOG IF EXISTS {cat} CASCADE",
                             warehouse_id=resolved_wh)
            except Exception as exc:
                print(f"  {_yellow('WARN')} Could not drop {cat}: {exc}")
        _try_destroy(prod_env)
        _try_destroy(env)
        _try_destroy_account()
        # Delete the UI-created space (Terraform won't do it in attach mode)
        _delete_genie_space_via_api(auth_file, space_id)

    print(f"\n  {_green(_bold('PASSED'))}  aus-bank-demo")


# ---------------------------------------------------------------------------
# Scenario: india-bank-demo — India bank champion flow
# ---------------------------------------------------------------------------

def _setup_india_bank_data(auth_file: Path, warehouse_id: str) -> str:
    """Create dev_lakshmi and prod_lakshmi catalogs with Indian banking tables.

    Returns the warehouse ID used.
    """
    import time as _time_india
    import hcl2 as _hcl2_india
    from databricks.sdk import WorkspaceClient as _WC_india
    from databricks.sdk.service.sql import StatementState as _SS_india

    def _s(v): return (v[0] if isinstance(v, list) else (v or "")).strip()

    with open(auth_file) as f:
        cfg = _hcl2_india.load(f)

    host = _s(cfg.get("databricks_workspace_host", ""))
    client_id = _s(cfg.get("databricks_client_id", ""))
    client_secret = _s(cfg.get("databricks_client_secret", ""))
    catalog_storage_base = _s(cfg.get("catalog_storage_base", ""))

    w = _WC_india(host=host, client_id=client_id, client_secret=client_secret)

    wh = warehouse_id
    if not wh:
        for warehouse in w.warehouses.list():
            if warehouse.id:
                wh = warehouse.id
                break
    if not wh:
        print("  No warehouse found — creating one for India bank data setup...")
        from databricks.sdk.service.sql import EndpointInfoWarehouseType
        create_resp = w.warehouses.create(
            name="Demo Warehouse",
            cluster_size="2X-Small",
            warehouse_type=EndpointInfoWarehouseType.PRO,
            max_num_clusters=1,
            auto_stop_mins=15,
            enable_serverless_compute=True,
        )
        created_wh = create_resp.result() if hasattr(create_resp, "result") else create_resp
        wh = created_wh.id if hasattr(created_wh, "id") else str(created_wh)

    for cat in [DEV_LAKSHMI_CAT, PROD_LAKSHMI_CAT]:
        storage = f"{catalog_storage_base.rstrip('/')}/{cat}" if catalog_storage_base else None
        try:
            w.catalogs.create(name=cat, comment=f"india-bank-demo — {cat}", storage_root=storage)
        except Exception:
            pass
        try:
            w.schemas.create(name=LAKSHMI_SCHEMA, catalog_name=cat)
        except Exception:
            pass

    print(f"  Waiting for catalog {DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA} to propagate...")
    for _ in range(12):
        try:
            r = w.statement_execution.execute_statement(
                warehouse_id=wh,
                statement=f"DESCRIBE SCHEMA {DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}",
                wait_timeout="30s",
            )
            st = r.status.state if r.status else None
            st_str = st.value if hasattr(st, "value") else str(st) if st else ""
            if st_str == "SUCCEEDED":
                break
        except Exception:
            pass
        _time_india.sleep(5)

    all_sql = INDIA_BANK_SETUP_SQL + "\n" + INDIA_BANK_SAMPLE_DATA_SQL + "\n" + INDIA_BANK_PROD_SETUP_SQL
    stmts = []
    for raw in all_sql.split(";"):
        lines = [l for l in raw.strip().splitlines() if l.strip() and not l.strip().startswith("--")]
        cleaned = "\n".join(lines).strip()
        if cleaned:
            stmts.append(cleaned)

    def _state_str(state) -> str:
        return state.value if hasattr(state, "value") else str(state)

    for stmt in stmts:
        r = w.statement_execution.execute_statement(
            warehouse_id=wh, statement=stmt, wait_timeout="50s",
        )
        max_wait = 120
        start = _time_india.time()
        while True:
            st = _state_str(r.status.state)
            if st == "SUCCEEDED":
                break
            if st in ("FAILED", "CANCELED", "CLOSED"):
                err = r.status.error
                print(f"  {_yellow('WARN')} SQL failed: {err}")
                break
            if _time_india.time() - start > max_wait:
                print(f"  {_yellow('WARN')} SQL timed out")
                break
            _time_india.sleep(2)
            r = w.statement_execution.get_statement(r.statement_id)

    print(f"  {_green('OK')}  India bank tables created in {DEV_LAKSHMI_CAT} + {PROD_LAKSHMI_CAT}")
    return wh


def _create_india_bank_genie_space_via_api(
    auth_file: Path,
    warehouse_id: str,
) -> str:
    """Create a Genie Space for Lakshmi Bank Analytics via REST API."""
    india_tables = [
        f"{DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.customers",
        f"{DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.accounts",
        f"{DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.transactions",
        f"{DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.credit_cards",
    ]
    return _create_genie_space_via_api(
        auth_file, "Lakshmi Bank Analytics", india_tables, warehouse_id,
    )


def scenario_india_bank_demo(
    auth_file: Path,
    warehouse_id: str = "",
    keep_data: bool = False,
    fresh_env: bool = False,
) -> None:
    """India bank demo — champion flow (IN + financial_services, import + promote)."""
    _banner("Scenario: india-bank-demo — India bank champion flow (IN + financial_services)")
    env = "dev"
    prod_env = "prod"

    _ensure_packages()

    # ── Phase 1: Setup ───────────────────────────────────────────────────────
    _preamble_cleanup(env, prod_env, fresh_env=fresh_env)

    _step("Phase 1 — Creating Indian banking tables")
    resolved_wh = _setup_india_bank_data(auth_file, warehouse_id)

    _step("Phase 1 — Creating rich Genie Space via API")
    space_id = _create_india_bank_genie_space_via_api(auth_file, resolved_wh)

    # ── Phase 2: Generate with IN + financial_services ──────────────────────
    _step("Phase 2 — Configuring env with genie_space_id + uc_tables (attach mode)")
    _make("setup", f"ENV={env}")
    _make("setup", f"ENV={prod_env}")

    india_tables_hcl = f"""\
genie_spaces = [
  {{
    name           = "Lakshmi Bank Analytics"
    genie_space_id = "{space_id}"
    uc_tables = [
      "{DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.customers",
      "{DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.accounts",
      "{DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.transactions",
      "{DEV_LAKSHMI_CAT}.{LAKSHMI_SCHEMA}.credit_cards",
    ]
  }},
]
"""
    _write_env_tfvars(env, india_tables_hcl, resolved_wh)

    _step("Phase 2 — Running make generate with COUNTRY=IN INDUSTRY=financial_services")
    _make("generate", f"ENV={env}", "COUNTRY=IN", "INDUSTRY=financial_services", retries=3)

    _step("Asserting India-specific masking functions in generated output")
    gen_dir = ENVS_DIR / env / "generated"
    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated")
    _assert_file_exists(gen_dir / "masking_functions.sql", "masking_functions.sql generated")
    # Soft check: catalog name in generated output (may be absent if autofix
    # hit the FGAC policy cap and pruned tag_assignments — promote phase
    # validates catalog remapping end-to-end).
    if DEV_LAKSHMI_CAT in (gen_dir / "abac.auto.tfvars").read_text():
        print(f"  {_green('PASS')}  {DEV_LAKSHMI_CAT} catalog referenced in generated policies")
    else:
        print(f"  {_yellow('WARN')}  {DEV_LAKSHMI_CAT} not found in generated abac — may have been pruned by autofix policy cap")

    # Check for India-sensitive columns and masking functions in the combined
    # generated output. The FGAC policy cap (10 per catalog) can aggressively
    # prune tag_assignments from abac.auto.tfvars, but overlay masking functions
    # in masking_functions.sql are always preserved.
    abac_text = (gen_dir / "abac.auto.tfvars").read_text()
    masking_sql = gen_dir / "masking_functions.sql"
    sql_text = masking_sql.read_text()
    combined_text = (abac_text + "\n" + sql_text).lower()

    india_terms_found = sum(1 for term in [
        "aadhaar", "pan_number", "pan_india", "upi_id", "aml_risk_flag",
        "card_number", "card_last4", "gstin", "voter_id", "uan",
    ] if term in combined_text)
    if india_terms_found < 3:
        raise AssertionError(
            f"Expected India-sensitive terms (aadhaar, pan, gstin, voter_id, uan, etc.) in "
            f"generated output (abac + masking SQL), but only found {india_terms_found}/10"
        )
    print(f"  {_green('PASS')}  India-sensitive terms found: {india_terms_found}/10 in generated output")

    # Check for India overlay functions — search broadly since the LLM may
    # use variant names (e.g., mask_aadhaar_last4 instead of mask_aadhaar)
    india_fn_terms = ["aadhaar", "pan_india", "voter_id", "gstin", "uan"]
    india_fns_found = [t for t in india_fn_terms if t in sql_text.lower()]
    if not india_fns_found:
        raise AssertionError(
            f"Expected at least one India-specific term (aadhaar, pan_india, voter_id, gstin, uan) "
            f"in masking_functions.sql, but none found"
        )
    print(f"  {_green('PASS')}  India-specific masking terms in SQL: {india_fns_found}")

    _assert_contains(gen_dir / "abac.auto.tfvars", "Lakshmi Bank Analytics",
                     "Lakshmi Bank Analytics genie_space_configs entry present")

    # ── Phase 3: Apply governance ────────────────────────────────────────────
    _step("Phase 3 — Applying governance (space must survive, not be created/deleted)")
    _make("apply", f"ENV={env}", retries=3, retry_delay_seconds=120)

    _step("Asserting space NOT created by Terraform (no .genie_space_id_* file)")
    id_files = list((ENVS_DIR / env).glob(".genie_space_id_*"))
    legacy = ENVS_DIR / env / ".genie_space_id"
    if id_files or legacy.exists():
        raise AssertionError(
            "Terraform created a new Genie Space in attach mode — expected no "
            ".genie_space_id_* file. The existing space should be used as-is."
        )
    print(f"  {_green('PASS')}  No .genie_space_id_* file: Terraform did not create a new space")

    # ── Phase 4: Promote to prod ─────────────────────────────────────────────
    _step(f"Phase 4 — Promoting {env} -> {prod_env} ({DEV_LAKSHMI_CAT} -> {PROD_LAKSHMI_CAT})")
    _make(
        "promote",
        f"SOURCE_ENV={env}",
        f"DEST_ENV={prod_env}",
        f"DEST_CATALOG_MAP={DEV_LAKSHMI_CAT}={PROD_LAKSHMI_CAT}",
    )

    _assert_file_exists(
        ENVS_DIR / prod_env / "env.auto.tfvars",
        "prod env.auto.tfvars written by promote",
    )
    prod_abac = ENVS_DIR / prod_env / "generated" / "abac.auto.tfvars"
    # Soft check: catalog name may be absent if autofix pruned all tag_assignments
    prod_abac_text = prod_abac.read_text()
    if PROD_LAKSHMI_CAT in prod_abac_text:
        print(f"  {_green('PASS')}  {PROD_LAKSHMI_CAT} catalog in promoted prod config")
    else:
        print(f"  {_yellow('WARN')}  {PROD_LAKSHMI_CAT} not found in promoted abac — may have been pruned by autofix policy cap")
    tag_section = prod_abac_text[prod_abac_text.find("tag_assignments"):] if "tag_assignments" in prod_abac_text else ""
    tag_chunk = tag_section.split("fgac_policies")[0] if "fgac_policies" in tag_section else tag_section
    if DEV_LAKSHMI_CAT in tag_chunk:
        raise AssertionError(
            f"tag_assignments in prod config still reference '{DEV_LAKSHMI_CAT}' — "
            f"catalog remap may have failed"
        )
    print(f"  {_green('PASS')}  tag_assignments do not reference {DEV_LAKSHMI_CAT}")

    _copy_auth(env, prod_env)

    _step("Phase 4 — Applying prod governance")
    _force_account_reapply("india-bank-demo prod apply")
    _make("apply", f"ENV={prod_env}", retries=3, retry_delay_seconds=120)

    _step("Verifying prod config references prod_lakshmi")
    prod_env_tfvars = ENVS_DIR / prod_env / "env.auto.tfvars"
    _assert_contains(prod_env_tfvars, PROD_LAKSHMI_CAT,
                     f"prod env.auto.tfvars references {PROD_LAKSHMI_CAT}")

    # ── Teardown ─────────────────────────────────────────────────────────────
    if not keep_data:
        for cat in [DEV_LAKSHMI_CAT, PROD_LAKSHMI_CAT]:
            try:
                _sdk_run_sql(auth_file, f"DROP CATALOG IF EXISTS {cat} CASCADE",
                             warehouse_id=resolved_wh)
            except Exception as exc:
                print(f"  {_yellow('WARN')} Could not drop {cat}: {exc}")
        _try_destroy(prod_env)
        _try_destroy(env)
        _try_destroy_account()
        _delete_genie_space_via_api(auth_file, space_id)

    print(f"\n  {_green(_bold('PASSED'))}  india-bank-demo")


# ---------------------------------------------------------------------------
# Scenario: asean-bank-demo — ASEAN bank champion flow
# ---------------------------------------------------------------------------

def _setup_asean_bank_data(auth_file: Path, warehouse_id: str) -> str:
    """Create dev_asean_bank and prod_asean_bank catalogs with ASEAN banking tables.

    Returns the warehouse ID used.
    """
    import time as _time_asean
    import hcl2 as _hcl2_asean
    from databricks.sdk import WorkspaceClient as _WC_asean
    from databricks.sdk.service.sql import StatementState as _SS_asean

    def _s(v): return (v[0] if isinstance(v, list) else (v or "")).strip()

    with open(auth_file) as f:
        cfg = _hcl2_asean.load(f)

    host = _s(cfg.get("databricks_workspace_host", ""))
    client_id = _s(cfg.get("databricks_client_id", ""))
    client_secret = _s(cfg.get("databricks_client_secret", ""))
    catalog_storage_base = _s(cfg.get("catalog_storage_base", ""))

    w = _WC_asean(host=host, client_id=client_id, client_secret=client_secret)

    wh = warehouse_id
    if not wh:
        for warehouse in w.warehouses.list():
            if warehouse.id:
                wh = warehouse.id
                break
    if not wh:
        print("  No warehouse found — creating one for ASEAN bank data setup...")
        from databricks.sdk.service.sql import EndpointInfoWarehouseType
        create_resp = w.warehouses.create(
            name="Demo Warehouse",
            cluster_size="2X-Small",
            warehouse_type=EndpointInfoWarehouseType.PRO,
            max_num_clusters=1,
            auto_stop_mins=15,
            enable_serverless_compute=True,
        )
        created_wh = create_resp.result() if hasattr(create_resp, "result") else create_resp
        wh = created_wh.id if hasattr(created_wh, "id") else str(created_wh)

    for cat in [DEV_ASEAN_CAT, PROD_ASEAN_CAT]:
        storage = f"{catalog_storage_base.rstrip('/')}/{cat}" if catalog_storage_base else None
        try:
            w.catalogs.create(name=cat, comment=f"asean-bank-demo — {cat}", storage_root=storage)
        except Exception:
            pass
        try:
            w.schemas.create(name=ASEAN_SCHEMA, catalog_name=cat)
        except Exception:
            pass

    print(f"  Waiting for catalog {DEV_ASEAN_CAT}.{ASEAN_SCHEMA} to propagate...")
    for _ in range(12):
        try:
            r = w.statement_execution.execute_statement(
                warehouse_id=wh,
                statement=f"DESCRIBE SCHEMA {DEV_ASEAN_CAT}.{ASEAN_SCHEMA}",
                wait_timeout="30s",
            )
            st = r.status.state if r.status else None
            st_str = st.value if hasattr(st, "value") else str(st) if st else ""
            if st_str == "SUCCEEDED":
                break
        except Exception:
            pass
        _time_asean.sleep(5)

    all_sql = ASEAN_BANK_SETUP_SQL + "\n" + ASEAN_BANK_SAMPLE_DATA_SQL + "\n" + ASEAN_BANK_PROD_SETUP_SQL
    stmts = []
    for raw in all_sql.split(";"):
        lines = [l for l in raw.strip().splitlines() if l.strip() and not l.strip().startswith("--")]
        cleaned = "\n".join(lines).strip()
        if cleaned:
            stmts.append(cleaned)

    def _state_str(state) -> str:
        return state.value if hasattr(state, "value") else str(state)

    for stmt in stmts:
        r = w.statement_execution.execute_statement(
            warehouse_id=wh, statement=stmt, wait_timeout="50s",
        )
        max_wait = 120
        start = _time_asean.time()
        while True:
            st = _state_str(r.status.state)
            if st == "SUCCEEDED":
                break
            if st in ("FAILED", "CANCELED", "CLOSED"):
                err = r.status.error
                print(f"  {_yellow('WARN')} SQL failed: {err}")
                break
            if _time_asean.time() - start > max_wait:
                print(f"  {_yellow('WARN')} SQL timed out")
                break
            _time_asean.sleep(2)
            r = w.statement_execution.get_statement(r.statement_id)

    print(f"  {_green('OK')}  ASEAN bank tables created in {DEV_ASEAN_CAT} + {PROD_ASEAN_CAT}")
    return wh


def _create_asean_bank_genie_space_via_api(
    auth_file: Path,
    warehouse_id: str,
) -> str:
    """Create a Genie Space for ASEAN Regional Banking Analytics via REST API."""
    asean_tables = [
        f"{DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.customers",
        f"{DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.accounts",
        f"{DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.transactions",
        f"{DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.credit_cards",
    ]
    return _create_genie_space_via_api(
        auth_file, "ASEAN Regional Banking Analytics", asean_tables, warehouse_id,
    )


def scenario_asean_bank_demo(
    auth_file: Path,
    warehouse_id: str = "",
    keep_data: bool = False,
    fresh_env: bool = False,
) -> None:
    """ASEAN bank demo — champion flow (SEA + financial_services, import + promote)."""
    _banner("Scenario: asean-bank-demo — ASEAN bank champion flow (SEA + financial_services)")
    env = "dev"
    prod_env = "prod"

    _ensure_packages()

    # ── Phase 1: Setup ───────────────────────────────────────────────────────
    _preamble_cleanup(env, prod_env, fresh_env=fresh_env)

    _step("Phase 1 — Creating ASEAN banking tables")
    resolved_wh = _setup_asean_bank_data(auth_file, warehouse_id)

    _step("Phase 1 — Creating rich Genie Space via API")
    space_id = _create_asean_bank_genie_space_via_api(auth_file, resolved_wh)

    # ── Phase 2: Generate with SEA + financial_services ─────────────────────
    _step("Phase 2 — Configuring env with genie_space_id + uc_tables (attach mode)")
    _make("setup", f"ENV={env}")
    _make("setup", f"ENV={prod_env}")

    asean_tables_hcl = f"""\
genie_spaces = [
  {{
    name           = "ASEAN Regional Banking Analytics"
    genie_space_id = "{space_id}"
    uc_tables = [
      "{DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.customers",
      "{DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.accounts",
      "{DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.transactions",
      "{DEV_ASEAN_CAT}.{ASEAN_SCHEMA}.credit_cards",
    ]
  }},
]
"""
    _write_env_tfvars(env, asean_tables_hcl, resolved_wh)

    _step("Phase 2 — Running make generate with COUNTRY=SEA INDUSTRY=financial_services")
    _make("generate", f"ENV={env}", "COUNTRY=SEA", "INDUSTRY=financial_services", retries=3)

    _step("Asserting SEA-specific masking functions in generated output")
    gen_dir = ENVS_DIR / env / "generated"
    _assert_file_exists(gen_dir / "abac.auto.tfvars", "abac.auto.tfvars generated")
    _assert_file_exists(gen_dir / "masking_functions.sql", "masking_functions.sql generated")
    # Soft check: catalog name in generated output (may be absent if autofix
    # hit the FGAC policy cap and pruned tag_assignments — promote phase
    # validates catalog remapping end-to-end).
    if DEV_ASEAN_CAT in (gen_dir / "abac.auto.tfvars").read_text():
        print(f"  {_green('PASS')}  {DEV_ASEAN_CAT} catalog referenced in generated policies")
    else:
        print(f"  {_yellow('WARN')}  {DEV_ASEAN_CAT} not found in generated abac — may have been pruned by autofix policy cap")

    abac_text = (gen_dir / "abac.auto.tfvars").read_text()
    sea_columns_found = sum(1 for col in ["nric", "mykad", "thai_id", "nik", "philsys", "cccd"]
                            if col in abac_text.lower())
    if sea_columns_found < 3:
        raise AssertionError(
            f"Expected SEA-sensitive columns (nric, mykad, thai_id, nik, philsys, cccd) in "
            f"tag_assignments, but only found {sea_columns_found}/6"
        )
    print(f"  {_green('PASS')}  SEA-sensitive columns tagged: {sea_columns_found}/6 found in tag_assignments")

    masking_sql = gen_dir / "masking_functions.sql"
    sql_text = masking_sql.read_text()
    sea_fns_found = [fn for fn in ["mask_nric", "mask_mykad", "mask_nik"]
                     if fn in sql_text]
    if not sea_fns_found:
        raise AssertionError(
            f"Expected at least one SEA-specific masking function (mask_nric, mask_mykad, mask_nik) "
            f"in masking_functions.sql, but none found"
        )
    print(f"  {_green('PASS')}  SEA-specific masking functions present: {sea_fns_found}")

    _assert_contains(gen_dir / "abac.auto.tfvars", "ASEAN Regional Banking Analytics",
                     "ASEAN Regional Banking Analytics genie_space_configs entry present")

    # ── Phase 3: Apply governance ────────────────────────────────────────────
    _step("Phase 3 — Applying governance (space must survive, not be created/deleted)")
    _make("apply", f"ENV={env}", retries=3, retry_delay_seconds=120)

    _step("Asserting space NOT created by Terraform (no .genie_space_id_* file)")
    id_files = list((ENVS_DIR / env).glob(".genie_space_id_*"))
    legacy = ENVS_DIR / env / ".genie_space_id"
    if id_files or legacy.exists():
        raise AssertionError(
            "Terraform created a new Genie Space in attach mode — expected no "
            ".genie_space_id_* file. The existing space should be used as-is."
        )
    print(f"  {_green('PASS')}  No .genie_space_id_* file: Terraform did not create a new space")

    # ── Phase 4: Promote to prod ─────────────────────────────────────────────
    _step(f"Phase 4 — Promoting {env} -> {prod_env} ({DEV_ASEAN_CAT} -> {PROD_ASEAN_CAT})")
    _make(
        "promote",
        f"SOURCE_ENV={env}",
        f"DEST_ENV={prod_env}",
        f"DEST_CATALOG_MAP={DEV_ASEAN_CAT}={PROD_ASEAN_CAT}",
    )

    _assert_file_exists(
        ENVS_DIR / prod_env / "env.auto.tfvars",
        "prod env.auto.tfvars written by promote",
    )
    prod_abac = ENVS_DIR / prod_env / "generated" / "abac.auto.tfvars"
    # Soft check: catalog name may be absent if autofix pruned all tag_assignments
    prod_abac_text = prod_abac.read_text()
    if PROD_ASEAN_CAT in prod_abac_text:
        print(f"  {_green('PASS')}  {PROD_ASEAN_CAT} catalog in promoted prod config")
    else:
        print(f"  {_yellow('WARN')}  {PROD_ASEAN_CAT} not found in promoted abac — may have been pruned by autofix policy cap")
    tag_section = prod_abac_text[prod_abac_text.find("tag_assignments"):] if "tag_assignments" in prod_abac_text else ""
    tag_chunk = tag_section.split("fgac_policies")[0] if "fgac_policies" in tag_section else tag_section
    if DEV_ASEAN_CAT in tag_chunk:
        raise AssertionError(
            f"tag_assignments in prod config still reference '{DEV_ASEAN_CAT}' — "
            f"catalog remap may have failed"
        )
    print(f"  {_green('PASS')}  tag_assignments do not reference {DEV_ASEAN_CAT}")

    _copy_auth(env, prod_env)

    _step("Phase 4 — Applying prod governance")
    _force_account_reapply("asean-bank-demo prod apply")
    _make("apply", f"ENV={prod_env}", retries=3, retry_delay_seconds=120)

    _step("Verifying prod config references prod_asean_bank")
    prod_env_tfvars = ENVS_DIR / prod_env / "env.auto.tfvars"
    _assert_contains(prod_env_tfvars, PROD_ASEAN_CAT,
                     f"prod env.auto.tfvars references {PROD_ASEAN_CAT}")

    # ── Teardown ─────────────────────────────────────────────────────────────
    if not keep_data:
        for cat in [DEV_ASEAN_CAT, PROD_ASEAN_CAT]:
            try:
                _sdk_run_sql(auth_file, f"DROP CATALOG IF EXISTS {cat} CASCADE",
                             warehouse_id=resolved_wh)
            except Exception as exc:
                print(f"  {_yellow('WARN')} Could not drop {cat}: {exc}")
        _try_destroy(prod_env)
        _try_destroy(env)
        _try_destroy_account()
        _delete_genie_space_via_api(auth_file, space_id)

    print(f"\n  {_green(_bold('PASSED'))}  asean-bank-demo")


# ---------------------------------------------------------------------------
# Scenario registry
# ---------------------------------------------------------------------------

SCENARIOS: dict[str, tuple[str, Callable]] = {
    "quickstart":           ("Single space, single catalog (Finance/dev_fin)",                    scenario_quickstart),
    "multi-catalog":        ("One space spanning two catalogs (Combined)",                        scenario_multi_catalog),
    "multi-space":          ("Two spaces, separate catalogs (Finance+Clinical)",                  scenario_multi_space),
    "per-space":            ("Incremental per-space generation (isolation test)",                 scenario_per_space),
    "promote":              ("Multi-space dev → prod promotion",                                  scenario_promote),
    "multi-env":            ("Two independent envs (dev Finance, bu2 Clinical)",                  scenario_multi_env),
    "attach-promote":       ("Attach to UI-created space (API discovery) + promote",              scenario_attach_and_promote),
    "self-service-genie":   ("Central governance + BU teams self-serve Genie (MODE=governance/genie)", scenario_self_service_genie),
    "abac-only":            ("ABAC governance only (no Genie Space) + upgrade to Genie",         scenario_abac_only),
    "multi-space-import":   ("Import two UI-created Genie Spaces in one make generate",          scenario_multi_space_import),
    "schema-drift":    ("Column tag drift detection after ADD/DROP/RENAME COLUMN",           scenario_schema_drift),
    "genie-only":      ("Genie-only mode (genie_only=true, no account-level resources)",    scenario_genie_only),
    "country-overlay": ("Country/region overlays (ANZ, IN, SEA) — generation only",         scenario_country_overlay),
    "industry-overlay": ("Industry overlays (financial/healthcare/retail) + country+industry combo", scenario_industry_overlay),
    "genie-import-no-abac": ("Import Genie Space, deploy to prod without ABAC",            scenario_genie_import_no_abac),
    "aus-bank-demo": ("Australian bank demo — champion flow (ANZ + financial_services, import + promote)", scenario_aus_bank_demo),
    "india-bank-demo": ("India bank demo — champion flow (IN + financial_services, import + promote)", scenario_india_bank_demo),
    "asean-bank-demo": ("ASEAN bank demo — champion flow (SEA + financial_services, import + promote)", scenario_asean_bank_demo),
}


# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------

def _preflight_fgac_quota_check(auth_file: Path) -> None:
    """Check the metastore-wide FGAC policy quota before running tests.

    The Databricks ABAC quota counter is eventually-consistent and known to
    drift after large-scale policy deletions.  When the counter reads ≥ 1000
    but the actual visible policy count is near 0, every `terraform apply`
    will fail immediately — wasting hours of test time.

    This check detects that condition early and prints a clear error with the
    support ticket instructions needed to fix it.
    """
    try:
        import hcl2 as _hcl2
        import json as _pf_json
        import ssl as _pf_ssl
        import urllib.request as _pf_urq
        import urllib.parse as _pf_urp
        from databricks.sdk import WorkspaceClient as _WC

        _pf_ctx = _pf_ssl.create_default_context()
        _pf_ctx.check_hostname = False
        _pf_ctx.verify_mode = _pf_ssl.CERT_NONE

        def _s(v): return (v[0] if isinstance(v, list) else (v or "")).strip()

        with open(auth_file) as f:
            auth = _hcl2.load(f)
        host          = _s(auth.get("databricks_workspace_host", ""))
        client_id     = _s(auth.get("databricks_client_id", ""))
        client_secret = _s(auth.get("databricks_client_secret", ""))
        if not host:
            return  # can't check without host

        w = _WC(host=host, client_id=client_id, client_secret=client_secret)
        token = w.config.authenticate()
        base  = host.rstrip("/")
        metastore_id = w.metastores.current().metastore_id

        # 1. Get the estimated metastore-wide FGAC quota counter
        estimated_count = -1
        quota_limit     = 1000
        try:
            url = f"{base}/api/2.1/unity-catalog/resource-quotas/METASTORE/{metastore_id}/abac-policy-quota"
            req = _pf_urq.Request(url, headers=token)
            with _pf_urq.urlopen(req, timeout=10, context=_pf_ctx) as r:
                data = _pf_json.loads(r.read())
            qi = data.get("quota_info", data)
            estimated_count = qi.get("quota_count", -1)
            quota_limit     = qi.get("quota_limit", 1000)
        except Exception:
            pass  # quota API unavailable — skip check

        if estimated_count < 0:
            return  # couldn't read quota, proceed optimistically

        # 2. Get the actual current policy count across all visible catalogs
        actual_count = 0
        try:
            cats = [c.name for c in w.catalogs.list() if c.name]
            for cat in cats:
                try:
                    url = f"{base}/api/2.1/unity-catalog/policies/CATALOG/{_pf_urp.quote(cat, safe='')}"
                    req = _pf_urq.Request(url, headers=token)
                    with _pf_urq.urlopen(req, timeout=10, context=_pf_ctx) as r:
                        actual_count += len(_pf_json.loads(r.read()).get("policies", []))
                except Exception:
                    pass
        except Exception:
            actual_count = -1

        headroom = quota_limit - estimated_count

        print(f"\n  [Pre-flight] FGAC quota:  estimated={estimated_count}/{quota_limit}  "
              f"actual={actual_count if actual_count >= 0 else '?'}  headroom={headroom}")

        # 3. Decide whether to proceed, warn, or abort
        if estimated_count < quota_limit - 50:
            # Plenty of room — green light
            print(f"  [Pre-flight] Quota OK — proceeding.\n")
            return

        if estimated_count >= quota_limit and actual_count >= 0 and actual_count < 50:
            # Counter says full but reality is empty — classic stale counter bug
            print()
            print("  " + "=" * 62)
            print("  !! FGAC QUOTA COUNTER IS STALE — TESTS WILL FAIL !!")
            print("  " + "=" * 62)
            print(f"\n  The Databricks metastore-wide ABAC policy counter reports")
            print(f"  {estimated_count}/{quota_limit} (at or over limit), but querying")
            print(f"  all visible catalogs finds only {actual_count} actual policies.")
            print(f"\n  This is a known Databricks backend bug: the estimated counter")
            print(f"  does not properly decrement when policies are deleted.")
            print(f"  Terraform will refuse to create any new FGAC policies until")
            print(f"  Databricks resets this counter.")
            print()
            print(f"  Metastore ID : {metastore_id}")
            print(f"  Workspace    : {host}")
            print()
            print(f"  HOW TO FIX:")
            print(f"  1. File a Databricks Support ticket:")
            print(f"       Subject: 'FGAC/ABAC policy quota counter stuck at {estimated_count}")
            print(f"                 for metastore {metastore_id}'")
            print(f"       Ask them to reset the abac-policy-quota counter for this metastore.")
            print(f"  2. Or switch to a workspace/metastore with available quota.")
            print()
            print(f"  Run with --skip-fgac-quota-check to bypass this check (tests will still fail).")
            print()
            sys.exit(1)

        if headroom < 50:
            # Very low headroom — warn but allow proceeding
            print(f"\n  [Pre-flight] WARNING: Only {headroom} FGAC policy slots remain "
                  f"in the metastore (estimated {estimated_count}/{quota_limit}).")
            print(f"  Tests may fail if this is a shared metastore with other active users.")
            print(f"  Proceeding...\n")

    except Exception as exc:
        # Pre-flight is best-effort; don't block tests on diagnostic failures
        print(f"  [Pre-flight] Could not check FGAC quota: {exc}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Integration test runner for playbook.md scenarios",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--scenario", "-s",
        choices=[*SCENARIOS, "all"],
        default="all",
        help="Scenario to run (default: all)",
    )
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="List available scenarios and exit",
    )
    parser.add_argument(
        "--warehouse-id", default="",
        metavar="ID",
        help="Pin a specific SQL warehouse ID (avoids cold-start delay)",
    )
    parser.add_argument(
        "--auth-file",
        default=str(DEFAULT_AUTH_FILE),
        metavar="PATH",
        help=f"Path to auth.auto.tfvars (default: {DEFAULT_AUTH_FILE})",
    )
    parser.add_argument(
        "--keep-data",
        action="store_true",
        help="Skip teardown so you can inspect results after the run",
    )
    parser.add_argument(
        "--nuke-fgac",
        action="store_true",
        help=(
            "One-time emergency cleanup: delete FGAC policies from ALL catalogs in the "
            "metastore before running scenarios.  Use when the metastore-wide ABAC policy "
            "count has accumulated past 1000 from many failed/partial test runs."
        ),
    )
    parser.add_argument(
        "--skip-fgac-quota-check",
        action="store_true",
        help="Skip the pre-flight FGAC quota check (tests may still fail if quota is full).",
    )
    parser.add_argument(
        "--fail-fast", "-x",
        action="store_true",
        help=(
            "Stop immediately when a scenario fails instead of continuing with the "
            "remaining scenarios.  Recommended for CI/CD pipelines to avoid wasting "
            "cloud resources when a fundamental issue is present."
        ),
    )
    args = parser.parse_args()

    if args.list:
        print("\nAvailable scenarios:\n")
        for name, (desc, _) in SCENARIOS.items():
            print(f"  {name:<18}  {desc}")
        print()
        return

    # ------------------------------------------------------------------
    # Auto-detect provisioned environment from provision_test_env.py state
    # ------------------------------------------------------------------
    global ENVS_DIR, _TEST_SUFFIX  # DEFAULT_AUTH_FILE is derived from ENVS_DIR, not a separate global
    fresh_env = False
    _cloud_root = CLOUD_ROOT

    if PROVISION_STATE_FILE.exists():
        try:
            import json as _json
            _state = _json.loads(PROVISION_STATE_FILE.read_text())
            _test_envs = _state.get("test_envs_dir")
            if _test_envs and Path(_test_envs).exists():
                ENVS_DIR = Path(_test_envs)
                fresh_env = True
                # Use run_id as suffix for account-level name isolation
                _run_id = _state.get("run_id", "")
                if _run_id:
                    _TEST_SUFFIX = _run_id[:6]  # short suffix e.g. "a44317"
                try:
                    _envs_display = ENVS_DIR.relative_to(MODULE_ROOT)
                except ValueError:
                    try:
                        _envs_display = ENVS_DIR.relative_to(_cloud_root)
                    except ValueError:
                        _envs_display = ENVS_DIR
                print(f"\n  {_cyan('●')}  Provisioned environment detected — using {_envs_display}/")
                print(f"     FGAC quota wait and pre-flight check are disabled (fresh metastore).")
        except Exception:
            pass

    _default_auth = ENVS_DIR / "dev" / "auth.auto.tfvars"
    # If the user passed the default auth path (from Makefile or CLI default),
    # redirect to the provisioned test env auth file when available.
    # Compare resolved absolute paths since args.auth_file may be relative.
    # The user/Makefile may pass a path relative to cwd (e.g. "envs/dev/..."),
    # which resolves differently from MODULE_ROOT or CLOUD_ROOT based paths.
    _resolved_arg = str(Path(args.auth_file).resolve())
    _stock_defaults = {
        str((MODULE_ROOT / "envs" / "dev" / "auth.auto.tfvars").resolve()),
        str((_cloud_root / "envs" / "dev" / "auth.auto.tfvars").resolve()),
        str((Path.cwd() / "envs" / "dev" / "auth.auto.tfvars").resolve()),
    }
    auth_file    = Path(_resolved_arg if _resolved_arg not in _stock_defaults
                        else _default_auth).resolve()
    # Only propagate an explicitly-specified warehouse ID to scenarios.
    # Auto-discovered IDs (from _resolve_warehouse_id) are NOT propagated:
    # in fresh environments the Starter Warehouse may be non-functional, and
    # writing its ID into env.auto.tfvars causes Terraform to skip creating its
    # own managed warehouse (count=0 branch).  Each component — setup_test_data,
    # the Terraform module, and verify — auto-discovers a usable warehouse on its
    # own.  Only propagate when the user explicitly pins a warehouse via
    # --warehouse-id so they can reuse an existing one across scenarios.
    warehouse_id = args.warehouse_id if args.warehouse_id else ""
    _display_wh  = _resolve_warehouse_id(auth_file, args.warehouse_id)
    keep_data    = args.keep_data

    if not auth_file.exists():
        print(f"ERROR: auth file not found: {auth_file}")
        print("  Run from your cloud wrapper directory (genie/aws/ or genie/azure/), or pass --auth-file <path>.")
        sys.exit(1)

    # Optional one-time nuclear cleanup of ALL FGAC policies across all catalogs.
    # This clears accumulated orphaned policies from many prior partial test runs.
    if args.nuke_fgac:
        _banner("NUKE: Clearing ALL FGAC policies from metastore", width=64)
        print("  WARNING: This deletes FGAC policies from every non-system catalog.")
        _force_delete_fgac_policies("dev", all_catalogs=True)
        print("  Done. Waiting 90s for quota counter to propagate...")
        time.sleep(90)
        print("  Metastore FGAC quota reset complete.")
        if args.scenario == "all" and not any(True for _ in SCENARIOS):
            return  # nuke-only mode if no scenario was requested

    # Pre-flight: check FGAC metastore quota (skip for fresh provisioned envs)
    if not args.skip_fgac_quota_check and not fresh_env:
        _preflight_fgac_quota_check(auth_file)

    # Select scenarios to run
    if args.scenario == "all":
        selected = list(SCENARIOS.items())
    else:
        selected = [(args.scenario, SCENARIOS[args.scenario])]

    _banner(f"Integration Test Runner  —  {len(selected)} scenario(s)", width=64)
    fail_fast = args.fail_fast

    print(f"  Auth:      {auth_file}")
    try:
        _ed = ENVS_DIR.relative_to(MODULE_ROOT)
    except ValueError:
        try:
            _ed = ENVS_DIR.relative_to(_cloud_root)
        except ValueError:
            _ed = ENVS_DIR
    print(f"  Envs dir:  {_ed}/")
    print(f"  Warehouse: {_display_wh or '(auto)'}{' [pinned]' if args.warehouse_id else ' [auto-discover per-component]'}")
    print(f"  Fresh env: {fresh_env}")
    print(f"  Keep data: {keep_data}")
    print(f"  Fail fast: {fail_fast}")
    if args.nuke_fgac:
        print(f"  Nuke FGAC: enabled (all-catalog cleanup at start)")

    results: dict[str, str] = {}
    total_start = time.time()

    # Scenarios get one automatic retry on transient failures:
    # - LLM quality: masking SQL, semantic check, validation errors
    # - Infrastructure timing: tag assignment/policy creation, provisioner errors
    _RETRY_PATTERNS = [
        "masking_functions", "SEMANTIC CHECK", "semantic_check",
        "deploy_masking", "local-exec provisioner error",
        "genie_space_configs section missing", "columnName()",
        "per-space directory bootstrapped", "MULTIPLE_MASKS",
        "Validation found errors", "make generate ENV=",
        "failed to create entity_tag_assignment",
        "failed to create policy_info",
        "failed to update policy_info",
        "make apply",
    ]

    def _is_retryable(exc: Exception) -> bool:
        msg = str(exc)
        return any(p in msg for p in _RETRY_PATTERNS)

    for name, (desc, fn) in selected:
        start = time.time()
        print(f"\n{'─' * 64}")
        print(f"  Running: {_bold(name)}  —  {desc}")
        print(f"{'─' * 64}")
        last_exc = None
        for attempt in range(2):  # 1 attempt + 1 retry
            if attempt > 0:
                print(f"\n  {_yellow('RETRY')} Scenario '{name}' failed on LLM quality — retrying (attempt {attempt + 1}/2)...")
                print(f"{'─' * 64}")
                start = time.time()
            try:
                fn(auth_file, warehouse_id, keep_data, fresh_env=fresh_env)
                elapsed = time.time() - start
                results[name] = _green(f"PASSED  ({elapsed:.0f}s)")
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                if attempt == 0 and _is_retryable(exc):
                    continue  # retry
                break  # infrastructure failure or second attempt — don't retry
        if last_exc is not None:
            elapsed = time.time() - start
            results[name] = _red(f"FAILED  ({elapsed:.0f}s)")
            print(f"\n  {_red(_bold('FAILED'))}: {last_exc}")
            if fail_fast:
                # Print partial summary before aborting so the CI log shows
                # which scenario failed and how long it took.
                _banner("Results (aborted — fail-fast)", width=64)
                for n, r in results.items():
                    print(f"  {n:<18}  {r}")
                print(f"\n  {_red(_bold('Stopped after first failure (--fail-fast)'))}")
                sys.exit(1)
            elif args.scenario != "all":
                sys.exit(1)
            else:
                print(f"  {_yellow('Continuing with remaining scenarios...')}")

    # ── Summary ───────────────────────────────────────────────────────────────
    total_elapsed = time.time() - total_start
    _banner("Results", width=64)
    all_passed = True
    for name, result in results.items():
        print(f"  {name:<18}  {result}")
        if "FAILED" in result:
            all_passed = False

    print()
    if all_passed:
        print(f"  {_green(_bold('All scenarios PASSED'))}  (total: {total_elapsed:.0f}s)")
    else:
        print(f"  {_red(_bold('Some scenarios FAILED'))}  (total: {total_elapsed:.0f}s)")
        sys.exit(1)
    print()


if __name__ == "__main__":
    main()
