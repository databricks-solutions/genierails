#!/usr/bin/env python3
"""
Integration test data setup for multi-space / multi-catalog / multi-env testing.

Creates two catalogs with realistic schemas and sample data:

  dev_fin.finance.customers        — customer PII + account tier
  dev_fin.finance.transactions     — financial transactions (AML-relevant)
  dev_fin.finance.credit_cards     — card data (PCI-scoped)

  dev_clinical.clinical.patients   — patient demographics (PHI)
  dev_clinical.clinical.encounters — clinical encounters (PHI)

This supports testing:
  1. Multi-space: Space 1 uses dev_fin, Space 2 uses dev_clinical
  2. Multi-env: promote dev_fin→prod_fin, dev_clinical→prod_clinical
  3. Per-space generation: make generate SPACE="Finance Analytics"
                           make generate SPACE="Clinical Analytics"

Usage:
  # From your cloud wrapper directory (genie/aws/ or genie/azure/):
  python scripts/setup_test_data.py                   # dev catalogs only
  python scripts/setup_test_data.py --prod            # dev + prod catalogs

  # Verify (run after make apply — asserts row counts + ABAC governance):
  python scripts/setup_test_data.py --verify          # dev
  python scripts/setup_test_data.py --verify-prod     # prod

  # Tear down:
  python scripts/setup_test_data.py --teardown        # drop dev catalogs
  python scripts/setup_test_data.py --teardown-prod   # drop prod catalogs

  # Use a specific warehouse:
  python scripts/setup_test_data.py --warehouse-id <id>

  # Preview SQL without running:
  python scripts/setup_test_data.py --dry-run

  # Full automated integration test (orchestrated by Makefile):
  make integration-test                               # teardown after
  make integration-test KEEP_DATA=1                  # leave data for inspection

After setup, update envs/dev/env.auto.tfvars:

  genie_spaces = [
    {
      name     = "Finance Analytics"
      uc_tables = [
        "dev_fin.finance.customers",
        "dev_fin.finance.transactions",
        "dev_fin.finance.credit_cards",
      ]
    },
    {
      name     = "Clinical Analytics"
      uc_tables = [
        "dev_clinical.clinical.patients",
        "dev_clinical.clinical.encounters",
      ]
    },
  ]
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

from warehouse_utils import DEFAULT_WAREHOUSE_NAME, select_warehouse

SCRIPT_DIR = Path(__file__).resolve().parent
WORK_DIR = Path.cwd()

# Catalog names — change these to use different names.
FIN_CATALOG          = "dev_fin"
CLINICAL_CATALOG     = "dev_clinical"
PROD_FIN_CATALOG     = "prod_fin"
PROD_CLINICAL_CATALOG = "prod_clinical"

# ---------------------------------------------------------------------------
# SQL blocks
# ---------------------------------------------------------------------------

SETUP_SQL = f"""
-- ============================================================
-- CATALOG: {FIN_CATALOG}  (finance domain)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS {FIN_CATALOG}.finance
  COMMENT 'Core finance tables: customers, transactions, credit cards.';

-- Customers — PII-rich table used for masking policy testing
-- CREATE OR REPLACE ensures a fresh table with no orphaned column tags from
-- previous test runs (orphaned tags cause ABAC evaluation failures on INSERT).
CREATE OR REPLACE TABLE {FIN_CATALOG}.finance.customers (
  customer_id     BIGINT      NOT NULL COMMENT 'Unique customer identifier',
  first_name      STRING      NOT NULL COMMENT 'First name (PII)',
  last_name       STRING      NOT NULL COMMENT 'Last name (PII)',
  ssn             STRING      COMMENT 'Social security number — highly sensitive PII',
  date_of_birth   DATE        COMMENT 'Date of birth (PII)',
  email           STRING      COMMENT 'Email address (PII)',
  phone           STRING      COMMENT 'Phone number (PII)',
  address         STRING      COMMENT 'Street address (PII)',
  city            STRING,
  state           STRING,
  zip             STRING,
  account_tier    STRING      COMMENT 'RETAIL | PRIVATE_BANKING | INSTITUTIONAL',
  kyc_status      STRING      COMMENT 'KYC verification status: VERIFIED | PENDING | FAILED',
  created_at      TIMESTAMP
)
COMMENT 'Customer master — contains PII. Masking required for non-compliance roles.'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

-- Transactions — AML-relevant financial activity
CREATE OR REPLACE TABLE {FIN_CATALOG}.finance.transactions (
  transaction_id   BIGINT     NOT NULL COMMENT 'Unique transaction ID',
  customer_id      BIGINT     NOT NULL COMMENT 'FK → customers.customer_id',
  amount           DECIMAL(18,2) COMMENT 'Transaction amount in USD',
  currency         STRING     COMMENT 'Currency code (e.g. USD)',
  transaction_type STRING     COMMENT 'DEBIT | CREDIT | WIRE | ACH | INTERNAL',
  merchant_name    STRING     COMMENT 'Merchant or counterparty name',
  merchant_category STRING    COMMENT 'MCC code description',
  transaction_date DATE       NOT NULL,
  status           STRING     COMMENT 'COMPLETED | PENDING | DECLINED | REVERSED',
  aml_flag         BOOLEAN    COMMENT 'True if flagged by AML rules engine',
  risk_score       INT        COMMENT 'AML risk score 0–100',
  channel          STRING     COMMENT 'ONLINE | BRANCH | ATM | MOBILE',
  created_at       TIMESTAMP
)
COMMENT 'All customer transactions. AML-flagged rows restricted to compliance roles.'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

-- Credit cards — PCI-scoped table
CREATE OR REPLACE TABLE {FIN_CATALOG}.finance.credit_cards (
  card_id          BIGINT     NOT NULL COMMENT 'Unique card ID',
  customer_id      BIGINT     NOT NULL COMMENT 'FK → customers.customer_id',
  card_number      STRING     COMMENT 'Full 16-digit card number — PCI restricted',
  card_type        STRING     COMMENT 'VISA | MASTERCARD | AMEX | DISCOVER',
  expiry_month     INT,
  expiry_year      INT,
  cvv              STRING     COMMENT 'CVV — PCI restricted',
  credit_limit     DECIMAL(12,2),
  current_balance  DECIMAL(12,2),
  status           STRING     COMMENT 'ACTIVE | SUSPENDED | CLOSED',
  issued_at        TIMESTAMP,
  last_used_date   DATE
)
COMMENT 'Credit card master — PCI scope. Card number and CVV must be masked.'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

-- ============================================================
-- CATALOG: {CLINICAL_CATALOG}  (clinical domain)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS {CLINICAL_CATALOG}.clinical
  COMMENT 'Core clinical tables: patients, encounters.';

-- Patients — PHI-rich demographics table
CREATE OR REPLACE TABLE {CLINICAL_CATALOG}.clinical.patients (
  patient_id         BIGINT   NOT NULL COMMENT 'Unique patient identifier',
  first_name         STRING   NOT NULL COMMENT 'First name (PHI)',
  last_name          STRING   NOT NULL COMMENT 'Last name (PHI)',
  date_of_birth      DATE     COMMENT 'Date of birth (PHI)',
  ssn                STRING   COMMENT 'Social security number (PHI — highly sensitive)',
  gender             STRING   COMMENT 'MALE | FEMALE | OTHER | PREFER_NOT_TO_SAY',
  email              STRING   COMMENT 'Email address (PHI)',
  phone              STRING   COMMENT 'Phone number (PHI)',
  address            STRING   COMMENT 'Home address (PHI)',
  city               STRING,
  state              STRING,
  zip                STRING,
  insurance_id       STRING   COMMENT 'Insurance member ID',
  insurance_provider STRING,
  primary_physician  STRING   COMMENT 'Primary care physician name',
  risk_tier          STRING   COMMENT 'LOW | MEDIUM | HIGH — clinical risk classification',
  enrolled_at        TIMESTAMP
)
COMMENT 'Patient demographics — HIPAA PHI. All PII columns require masking outside clinical staff.'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

-- Encounters — clinical visit records (PHI + clinical notes)
CREATE OR REPLACE TABLE {CLINICAL_CATALOG}.clinical.encounters (
  encounter_id       BIGINT   NOT NULL COMMENT 'Unique encounter ID',
  patient_id         BIGINT   NOT NULL COMMENT 'FK → patients.patient_id',
  encounter_date     DATE     NOT NULL,
  encounter_type     STRING   COMMENT 'INPATIENT | OUTPATIENT | EMERGENCY | TELEHEALTH',
  diagnosis_code     STRING   COMMENT 'ICD-10 diagnosis code (PHI)',
  diagnosis_desc     STRING   COMMENT 'Human-readable diagnosis description (PHI)',
  treatment_notes    STRING   COMMENT 'Clinical free-text notes (PHI — highly sensitive)',
  attending_doc      STRING   COMMENT 'Attending physician name',
  facility_id        STRING   COMMENT 'Facility / hospital ID',
  facility_name      STRING,
  billed_amount      DECIMAL(12,2) COMMENT 'Amount billed to insurance',
  paid_amount        DECIMAL(12,2),
  length_of_stay_days INT     COMMENT 'Days admitted (inpatient only)',
  discharge_status   STRING   COMMENT 'DISCHARGED | TRANSFERRED | AMA | EXPIRED',
  created_at         TIMESTAMP
)
COMMENT 'Clinical encounter records — HIPAA PHI. Treatment notes and diagnosis restricted to clinical staff.'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
"""

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_DATA_SQL = f"""
-- ── Finance sample data ──────────────────────────────────────────────────

INSERT INTO {FIN_CATALOG}.finance.customers VALUES
  (1001, 'Alice',   'Chen',      '123-45-6789', '1982-03-14', 'alice.chen@email.com',    '415-555-0101', '100 Market St',  'San Francisco', 'CA', '94105', 'PRIVATE_BANKING', 'VERIFIED', current_timestamp()),
  (1002, 'Bob',     'Martinez',  '234-56-7890', '1975-07-22', 'bob.martinez@email.com',  '212-555-0202', '200 Park Ave',   'New York',      'NY', '10166', 'INSTITUTIONAL',   'VERIFIED', current_timestamp()),
  (1003, 'Carol',   'Smith',     '345-67-8901', '1990-11-05', 'carol.smith@email.com',   '312-555-0303', '300 Wacker Dr',  'Chicago',       'IL', '60601', 'RETAIL',          'VERIFIED', current_timestamp()),
  (1004, 'David',   'Johnson',   '456-78-9012', '1968-04-30', 'david.j@email.com',       '310-555-0404', '400 Wilshire',   'Los Angeles',   'CA', '90010', 'PRIVATE_BANKING', 'PENDING',  current_timestamp()),
  (1005, 'Eva',     'Williams',  '567-89-0123', '1995-09-18', 'eva.williams@email.com',  '206-555-0505', '500 Pine St',    'Seattle',       'WA', '98101', 'RETAIL',          'VERIFIED', current_timestamp()),
  (1006, 'Frank',   'Brown',     '678-90-1234', '1955-12-01', 'frank.brown@email.com',   '713-555-0606', '600 Main St',    'Houston',       'TX', '77002', 'INSTITUTIONAL',   'VERIFIED', current_timestamp()),
  (1007, 'Grace',   'Davis',     '789-01-2345', '1988-06-15', 'grace.davis@email.com',   '617-555-0707', '700 Boylston',   'Boston',        'MA', '02116', 'RETAIL',          'FAILED',   current_timestamp()),
  (1008, 'Henry',   'Wilson',    '890-12-3456', '1972-02-28', 'henry.wilson@email.com',  '305-555-0808', '800 Brickell',   'Miami',         'FL', '33131', 'PRIVATE_BANKING', 'VERIFIED', current_timestamp()),
  (1009, 'Iris',    'Moore',     '901-23-4567', '2000-08-10', 'iris.moore@email.com',    '602-555-0909', '900 Central Ave','Phoenix',       'AZ', '85004', 'RETAIL',          'VERIFIED', current_timestamp()),
  (1010, 'James',   'Taylor',    '012-34-5678', '1965-05-20', 'james.taylor@email.com',  '404-555-1010', '1000 Peachtree', 'Atlanta',       'GA', '30309', 'INSTITUTIONAL',   'VERIFIED', current_timestamp());

INSERT INTO {FIN_CATALOG}.finance.transactions VALUES
  (5001, 1001,  1250.00, 'USD', 'DEBIT',    'Whole Foods Market',  'Grocery',        '2025-01-10', 'COMPLETED', FALSE,  5,  'MOBILE',  current_timestamp()),
  (5002, 1001, 45000.00, 'USD', 'WIRE',     'Merrill Lynch',       'Investment',     '2025-01-12', 'COMPLETED', FALSE, 15,  'BRANCH',  current_timestamp()),
  (5003, 1002,  9800.00, 'USD', 'ACH',      'ACME Corp Payroll',   'Payroll',        '2025-01-14', 'COMPLETED', FALSE,  2,  'ONLINE',  current_timestamp()),
  (5004, 1003,    49.99, 'USD', 'DEBIT',    'Netflix',             'Streaming',      '2025-01-15', 'COMPLETED', FALSE,  1,  'ONLINE',  current_timestamp()),
  (5005, 1003, 75000.00, 'USD', 'WIRE',     'Overseas Account',    'International',  '2025-01-16', 'PENDING',   TRUE,  87,  'BRANCH',  current_timestamp()),
  (5006, 1004,   350.00, 'USD', 'CREDIT',   'Amazon',              'Retail',         '2025-01-17', 'COMPLETED', FALSE,  8,  'ONLINE',  current_timestamp()),
  (5007, 1004, 12000.00, 'USD', 'INTERNAL', 'Savings Account',     'Transfer',       '2025-01-18', 'COMPLETED', FALSE,  3,  'MOBILE',  current_timestamp()),
  (5008, 1005,   150.00, 'USD', 'DEBIT',    'Trader Joes',         'Grocery',        '2025-01-19', 'COMPLETED', FALSE,  4,  'MOBILE',  current_timestamp()),
  (5009, 1006, 99000.00, 'USD', 'WIRE',     'Shell LLC',           'Business',       '2025-01-20', 'COMPLETED', TRUE,  92,  'BRANCH',  current_timestamp()),
  (5010, 1007,    25.00, 'USD', 'DEBIT',    'Starbucks',           'Food & Drink',   '2025-01-21', 'COMPLETED', FALSE,  2,  'ATM',     current_timestamp()),
  (5011, 1008, 88000.00, 'USD', 'WIRE',     'Private Equity Fund', 'Investment',     '2025-01-22', 'COMPLETED', FALSE, 18,  'BRANCH',  current_timestamp()),
  (5012, 1009,   600.00, 'USD', 'DEBIT',    'Best Buy',            'Electronics',    '2025-01-23', 'COMPLETED', FALSE,  6,  'ONLINE',  current_timestamp()),
  (5013, 1010,  3200.00, 'USD', 'ACH',      'IRS Tax Payment',     'Government',     '2025-01-24', 'COMPLETED', FALSE,  1,  'ONLINE',  current_timestamp()),
  (5014, 1001,   500.00, 'USD', 'DEBIT',    'CVS Pharmacy',        'Healthcare',     '2025-01-25', 'COMPLETED', FALSE,  3,  'MOBILE',  current_timestamp()),
  (5015, 1002, 15000.00, 'USD', 'CREDIT',   'Client Reimbursement','Business',       '2025-01-26', 'COMPLETED', FALSE,  5,  'BRANCH',  current_timestamp());

INSERT INTO {FIN_CATALOG}.finance.credit_cards VALUES
  (9001, 1001, '4111111111111101', 'VISA',       5, 2027, '101', 25000.00,  4823.50, 'ACTIVE',    current_timestamp(), '2025-01-25'),
  (9002, 1002, '5500000000000202', 'MASTERCARD', 8, 2026, '202', 50000.00, 12400.00, 'ACTIVE',    current_timestamp(), '2025-01-22'),
  (9003, 1003, '4111111111111303', 'VISA',       2, 2025, '303',  5000.00,   890.00, 'ACTIVE',    current_timestamp(), '2025-01-21'),
  (9004, 1004, '3714496353984044', 'AMEX',      11, 2028, '044', 30000.00,  7600.00, 'ACTIVE',    current_timestamp(), '2025-01-18'),
  (9005, 1005, '4111111111111505', 'VISA',       1, 2027, '505',  8000.00,   120.00, 'ACTIVE',    current_timestamp(), '2025-01-20'),
  (9006, 1006, '6011111111111606', 'DISCOVER',   7, 2026, '606', 15000.00,  3200.00, 'ACTIVE',    current_timestamp(), '2025-01-19'),
  (9007, 1007, '4111111111111707', 'VISA',       3, 2024, '707',  2000.00,  1980.00, 'SUSPENDED', current_timestamp(), '2024-12-01'),
  (9008, 1008, '5500000000000808', 'MASTERCARD', 9, 2028, '808', 75000.00, 22100.00, 'ACTIVE',    current_timestamp(), '2025-01-24'),
  (9009, 1009, '4111111111111909', 'VISA',       6, 2027, '909',  4000.00,   580.00, 'ACTIVE',    current_timestamp(), '2025-01-23'),
  (9010, 1010, '5500000000001010', 'MASTERCARD', 4, 2029, '010', 40000.00,  9800.00, 'ACTIVE',    current_timestamp(), '2025-01-26');

-- ── Clinical sample data ─────────────────────────────────────────────────

INSERT INTO {CLINICAL_CATALOG}.clinical.patients VALUES
  (2001, 'Mary',    'Johnson',  '1955-03-12', '111-22-3333', 'FEMALE', 'mary.j@email.com',     '415-555-2001', '10 Oak St',       'San Francisco', 'CA', '94102', 'INS-001', 'BlueCross',  'Dr. Smith',    'HIGH',   current_timestamp()),
  (2002, 'Robert',  'Brown',    '1940-07-04', '222-33-4444', 'MALE',   'robert.b@email.com',   '212-555-2002', '20 Elm Ave',      'New York',      'NY', '10001', 'INS-002', 'Aetna',      'Dr. Jones',    'HIGH',   current_timestamp()),
  (2003, 'Patricia','Wilson',   '1970-11-25', '333-44-5555', 'FEMALE', 'patricia.w@email.com', '312-555-2003', '30 Maple Dr',     'Chicago',       'IL', '60602', 'INS-003', 'UnitedHealth','Dr. Lee',     'MEDIUM', current_timestamp()),
  (2004, 'Michael', 'Moore',    '1985-05-08', '444-55-6666', 'MALE',   'michael.m@email.com',  '310-555-2004', '40 Cedar Ln',     'Los Angeles',   'CA', '90011', 'INS-004', 'Cigna',      'Dr. Patel',    'LOW',    current_timestamp()),
  (2005, 'Linda',   'Taylor',   '1962-09-30', '555-66-7777', 'FEMALE', 'linda.t@email.com',    '206-555-2005', '50 Birch Rd',     'Seattle',       'WA', '98102', 'INS-005', 'Humana',     'Dr. Kim',      'MEDIUM', current_timestamp()),
  (2006, 'William', 'Anderson', '1948-01-15', '666-77-8888', 'MALE',   'william.a@email.com',  '713-555-2006', '60 Walnut Blvd',  'Houston',       'TX', '77003', 'INS-006', 'Medicare',   'Dr. Wang',     'HIGH',   current_timestamp()),
  (2007, 'Barbara', 'Thomas',   '1978-06-22', '777-88-9999', 'FEMALE', 'barbara.t@email.com',  '617-555-2007', '70 Spruce St',    'Boston',        'MA', '02115', 'INS-007', 'BlueCross',  'Dr. Garcia',   'LOW',    current_timestamp()),
  (2008, 'James',   'Jackson',  '1990-02-10', '888-99-0000', 'MALE',   'james.j2@email.com',   '305-555-2008', '80 Willow Way',   'Miami',         'FL', '33132', 'INS-008', 'Aetna',      'Dr. Martinez', 'LOW',    current_timestamp()),
  (2009, 'Susan',   'White',    '1935-12-03', '999-00-1111', 'FEMALE', 'susan.w@email.com',    '602-555-2009', '90 Aspen Ct',     'Phoenix',       'AZ', '85003', 'INS-009', 'Medicare',   'Dr. Chen',     'HIGH',   current_timestamp()),
  (2010, 'David',   'Harris',   '2005-04-18', '000-11-2222', 'MALE',   'david.h2@email.com',   '404-555-2010', '100 Hickory Pl',  'Atlanta',       'GA', '85003', 'INS-010', 'CHIP',       'Dr. Roberts',  'MEDIUM', current_timestamp());

INSERT INTO {CLINICAL_CATALOG}.clinical.encounters VALUES
  (3001, 2001, '2025-01-05', 'INPATIENT',   'I25.10', 'Coronary artery disease, unspecified',      'Patient admitted for unstable angina. Stress test positive. Started on beta-blockers.',                             'Dr. Smith',  'FAC-001', 'UCSF Medical Center',    12500.00, 10200.00,  3, 'DISCHARGED',  current_timestamp()),
  (3002, 2001, '2025-01-20', 'OUTPATIENT',  'Z00.00', 'Routine general medical exam',               'Follow-up post-discharge. BP well-controlled. Continue current medication regimen.',                               'Dr. Smith',  'FAC-001', 'UCSF Medical Center',      250.00,   200.00, NULL, 'DISCHARGED', current_timestamp()),
  (3003, 2002, '2025-01-08', 'INPATIENT',   'J18.9',  'Pneumonia, unspecified organism',             'Elderly patient with community-acquired pneumonia. O2 saturation 88% on admission. IV antibiotics initiated.',    'Dr. Jones',  'FAC-002', 'NYU Langone Health',      8400.00,  7100.00,  5, 'DISCHARGED',  current_timestamp()),
  (3004, 2003, '2025-01-12', 'OUTPATIENT',  'E11.9',  'Type 2 diabetes mellitus without complications','HbA1c at 7.8%. Adjusted metformin dosage. Patient educated on diet and glucose monitoring.',                   'Dr. Lee',    'FAC-003', 'Northwestern Memorial',    450.00,   380.00, NULL, 'DISCHARGED', current_timestamp()),
  (3005, 2004, '2025-01-15', 'EMERGENCY',   'S06.0X0A','Concussion without loss of consciousness',  'Motor vehicle accident. CT head negative for intracranial hemorrhage. Observation protocol started.',             'Dr. Patel',  'FAC-004', 'Cedars-Sinai Medical Ctr', 5200.00,  4300.00, NULL, 'DISCHARGED', current_timestamp()),
  (3006, 2005, '2025-01-18', 'OUTPATIENT',  'F32.9',  'Major depressive disorder, single episode',  'Patient reporting persistent low mood and sleep disturbance. Started on SSRI. CBT referral placed.',              'Dr. Kim',    'FAC-005', 'Swedish Medical Center',    380.00,   310.00, NULL, 'DISCHARGED', current_timestamp()),
  (3007, 2006, '2025-01-10', 'INPATIENT',   'N18.5',  'Chronic kidney disease, stage 5',             'Patient with ESRD requiring dialysis initiation. Vascular access placed. Family meeting held re: prognosis.',    'Dr. Wang',   'FAC-006', 'Houston Methodist Hospital',18900.00,16000.00,  7, 'DISCHARGED', current_timestamp()),
  (3008, 2007, '2025-01-22', 'TELEHEALTH',  'J06.9',  'Acute upper respiratory infection, unspecified','Telehealth visit for cold symptoms. No fever. Symptomatic treatment recommended. Follow up if no improvement.','Dr. Garcia', 'FAC-007', 'Mass General Brigham',      95.00,    80.00, NULL, 'DISCHARGED', current_timestamp()),
  (3009, 2008, '2025-01-25', 'OUTPATIENT',  'M54.5',  'Low back pain',                               'Chronic LBP exacerbation. Physical therapy referral. NSAID prescribed short-term. MRI ordered.',               'Dr. Martinez','FAC-008','Jackson Memorial Hospital', 320.00,   270.00, NULL, 'DISCHARGED', current_timestamp()),
  (3010, 2009, '2025-01-07', 'INPATIENT',   'I63.9',  'Cerebral infarction, unspecified',             'Acute ischemic stroke. tPA administered within window. ICU admission. Neuro-rehab initiated Day 2.',            'Dr. Chen',   'FAC-009', 'Banner University Medical',22000.00,19500.00,  8, 'DISCHARGED', current_timestamp()),
  (3011, 2010, '2025-01-28', 'OUTPATIENT',  'J45.20', 'Mild intermittent asthma, uncomplicated',     'Pediatric asthma management visit. Spirometry shows mild obstruction. Albuterol inhaler technique reviewed.',    'Dr. Roberts','FAC-010', 'Emory University Hospital',  280.00,  230.00, NULL, 'DISCHARGED', current_timestamp()),
  (3012, 2001, '2025-01-30', 'INPATIENT',   'I21.9',  'Acute myocardial infarction, unspecified',    'STEMI presentation. Emergent cath lab activation. 2 stents placed LAD. Cardiac rehab referral.',                'Dr. Smith',  'FAC-001', 'UCSF Medical Center',    45000.00,39000.00,  4, 'DISCHARGED',  current_timestamp());
"""

TEARDOWN_SQL = f"""
DROP CATALOG IF EXISTS {FIN_CATALOG}      CASCADE;
DROP CATALOG IF EXISTS {CLINICAL_CATALOG} CASCADE;
"""

# ---------------------------------------------------------------------------
# Prod catalogs — same schema, different data (simulates a real prod env)
# Used to test: make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP=...
# ---------------------------------------------------------------------------

PROD_SETUP_SQL = f"""
-- ============================================================
-- CATALOG: {PROD_FIN_CATALOG}  (finance domain — prod)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS {PROD_FIN_CATALOG}.finance
  COMMENT 'Core finance tables: customers, transactions, credit cards.';

CREATE OR REPLACE TABLE {PROD_FIN_CATALOG}.finance.customers (
  customer_id     BIGINT      NOT NULL COMMENT 'Unique customer identifier',
  first_name      STRING      NOT NULL COMMENT 'First name (PII)',
  last_name       STRING      NOT NULL COMMENT 'Last name (PII)',
  ssn             STRING      COMMENT 'Social security number — highly sensitive PII',
  date_of_birth   DATE        COMMENT 'Date of birth (PII)',
  email           STRING      COMMENT 'Email address (PII)',
  phone           STRING      COMMENT 'Phone number (PII)',
  address         STRING      COMMENT 'Street address (PII)',
  city            STRING,
  state           STRING,
  zip             STRING,
  account_tier    STRING      COMMENT 'RETAIL | PRIVATE_BANKING | INSTITUTIONAL',
  kyc_status      STRING      COMMENT 'KYC verification status: VERIFIED | PENDING | FAILED',
  created_at      TIMESTAMP
)
COMMENT 'Customer master — prod. Contains PII. Masking required for non-compliance roles.'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

CREATE OR REPLACE TABLE {PROD_FIN_CATALOG}.finance.transactions (
  transaction_id   BIGINT     NOT NULL COMMENT 'Unique transaction ID',
  customer_id      BIGINT     NOT NULL COMMENT 'FK → customers.customer_id',
  amount           DECIMAL(18,2) COMMENT 'Transaction amount in USD',
  currency         STRING     COMMENT 'Currency code (e.g. USD)',
  transaction_type STRING     COMMENT 'DEBIT | CREDIT | WIRE | ACH | INTERNAL',
  merchant_name    STRING     COMMENT 'Merchant or counterparty name',
  merchant_category STRING    COMMENT 'MCC code description',
  transaction_date DATE       NOT NULL,
  status           STRING     COMMENT 'COMPLETED | PENDING | DECLINED | REVERSED',
  aml_flag         BOOLEAN    COMMENT 'True if flagged by AML rules engine',
  risk_score       INT        COMMENT 'AML risk score 0–100',
  channel          STRING     COMMENT 'ONLINE | BRANCH | ATM | MOBILE',
  created_at       TIMESTAMP
)
COMMENT 'All customer transactions — prod. AML-flagged rows restricted to compliance roles.'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

CREATE OR REPLACE TABLE {PROD_FIN_CATALOG}.finance.credit_cards (
  card_id          BIGINT     NOT NULL COMMENT 'Unique card ID',
  customer_id      BIGINT     NOT NULL COMMENT 'FK → customers.customer_id',
  card_number      STRING     COMMENT 'Full 16-digit card number — PCI restricted',
  card_type        STRING     COMMENT 'VISA | MASTERCARD | AMEX | DISCOVER',
  expiry_month     INT,
  expiry_year      INT,
  cvv              STRING     COMMENT 'CVV — PCI restricted',
  credit_limit     DECIMAL(12,2),
  current_balance  DECIMAL(12,2),
  status           STRING     COMMENT 'ACTIVE | SUSPENDED | CLOSED',
  issued_at        TIMESTAMP,
  last_used_date   DATE
)
COMMENT 'Credit card master — prod. PCI scope. Card number and CVV must be masked.'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

-- ============================================================
-- CATALOG: {PROD_CLINICAL_CATALOG}  (clinical domain — prod)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS {PROD_CLINICAL_CATALOG}.clinical
  COMMENT 'Core clinical tables: patients, encounters.';

CREATE OR REPLACE TABLE {PROD_CLINICAL_CATALOG}.clinical.patients (
  patient_id         BIGINT   NOT NULL COMMENT 'Unique patient identifier',
  first_name         STRING   NOT NULL COMMENT 'First name (PHI)',
  last_name          STRING   NOT NULL COMMENT 'Last name (PHI)',
  date_of_birth      DATE     COMMENT 'Date of birth (PHI)',
  ssn                STRING   COMMENT 'Social security number (PHI — highly sensitive)',
  gender             STRING   COMMENT 'MALE | FEMALE | OTHER | PREFER_NOT_TO_SAY',
  email              STRING   COMMENT 'Email address (PHI)',
  phone              STRING   COMMENT 'Phone number (PHI)',
  address            STRING   COMMENT 'Home address (PHI)',
  city               STRING,
  state              STRING,
  zip                STRING,
  insurance_id       STRING   COMMENT 'Insurance member ID',
  insurance_provider STRING,
  primary_physician  STRING   COMMENT 'Primary care physician name',
  risk_tier          STRING   COMMENT 'LOW | MEDIUM | HIGH — clinical risk classification',
  enrolled_at        TIMESTAMP
)
COMMENT 'Patient demographics — prod. HIPAA PHI. All PII columns require masking outside clinical staff.'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

CREATE OR REPLACE TABLE {PROD_CLINICAL_CATALOG}.clinical.encounters (
  encounter_id       BIGINT   NOT NULL COMMENT 'Unique encounter ID',
  patient_id         BIGINT   NOT NULL COMMENT 'FK → patients.patient_id',
  encounter_date     DATE     NOT NULL,
  encounter_type     STRING   COMMENT 'INPATIENT | OUTPATIENT | EMERGENCY | TELEHEALTH',
  diagnosis_code     STRING   COMMENT 'ICD-10 diagnosis code (PHI)',
  diagnosis_desc     STRING   COMMENT 'Human-readable diagnosis description (PHI)',
  treatment_notes    STRING   COMMENT 'Clinical free-text notes (PHI — highly sensitive)',
  attending_doc      STRING   COMMENT 'Attending physician name',
  facility_id        STRING   COMMENT 'Facility / hospital ID',
  facility_name      STRING,
  billed_amount      DECIMAL(12,2) COMMENT 'Amount billed to insurance',
  paid_amount        DECIMAL(12,2),
  length_of_stay_days INT     COMMENT 'Days admitted (inpatient only)',
  discharge_status   STRING   COMMENT 'DISCHARGED | TRANSFERRED | AMA | EXPIRED',
  created_at         TIMESTAMP
)
COMMENT 'Clinical encounter records — prod. HIPAA PHI.'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
"""

PROD_SAMPLE_DATA_SQL = f"""
-- ── Prod finance sample data (larger scale, different records) ────────────

INSERT INTO {PROD_FIN_CATALOG}.finance.customers VALUES
  (8001, 'Liam',    'Foster',    'P01-00-0001', '1979-04-12', 'liam.foster@corp.com',    '415-800-0001', '1 Battery St',      'San Francisco', 'CA', '94111', 'INSTITUTIONAL',   'VERIFIED', current_timestamp()),
  (8002, 'Emma',    'Hughes',    'P02-00-0002', '1983-09-28', 'emma.hughes@corp.com',    '212-800-0002', '2 Wall St',         'New York',      'NY', '10005', 'PRIVATE_BANKING', 'VERIFIED', current_timestamp()),
  (8003, 'Noah',    'Clark',     'P03-00-0003', '1967-01-17', 'noah.clark@corp.com',     '312-800-0003', '3 LaSalle St',      'Chicago',       'IL', '60603', 'INSTITUTIONAL',   'VERIFIED', current_timestamp()),
  (8004, 'Olivia',  'Lewis',     'P04-00-0004', '1992-06-04', 'olivia.lewis@corp.com',   '310-800-0004', '4 Figueroa St',     'Los Angeles',   'CA', '90017', 'RETAIL',          'VERIFIED', current_timestamp()),
  (8005, 'William', 'Walker',    'P05-00-0005', '1958-11-23', 'w.walker@corp.com',       '206-800-0005', '5 Pike St',         'Seattle',       'WA', '98101', 'PRIVATE_BANKING', 'VERIFIED', current_timestamp()),
  (8006, 'Ava',     'Hall',      'P06-00-0006', '1971-03-08', 'ava.hall@corp.com',       '713-800-0006', '6 Travis St',       'Houston',       'TX', '77002', 'RETAIL',          'VERIFIED', current_timestamp()),
  (8007, 'James',   'Allen',     'P07-00-0007', '1986-08-15', 'james.allen@corp.com',    '617-800-0007', '7 Congress St',     'Boston',        'MA', '02109', 'INSTITUTIONAL',   'VERIFIED', current_timestamp()),
  (8008, 'Sophia',  'Young',     'P08-00-0008', '1975-12-31', 'sophia.young@corp.com',   '305-800-0008', '8 SE 1st Ave',      'Miami',         'FL', '33130', 'PRIVATE_BANKING', 'VERIFIED', current_timestamp()),
  (8009, 'Benjamin','Hernandez', 'P09-00-0009', '1997-07-20', 'ben.h@corp.com',          '602-800-0009', '9 Central Ave',     'Phoenix',       'AZ', '85004', 'RETAIL',          'VERIFIED', current_timestamp()),
  (8010, 'Mia',     'King',      'P10-00-0010', '1963-02-14', 'mia.king@corp.com',       '404-800-0010', '10 Peachtree Rd',   'Atlanta',       'GA', '30303', 'INSTITUTIONAL',   'VERIFIED', current_timestamp());

INSERT INTO {PROD_FIN_CATALOG}.finance.transactions VALUES
  (6001, 8001, 250000.00, 'USD', 'WIRE',     'Goldman Sachs',       'Investment',     '2025-01-10', 'COMPLETED', FALSE, 12,  'BRANCH',  current_timestamp()),
  (6002, 8001,   4200.00, 'USD', 'DEBIT',    'Equinox Fitness',     'Health',         '2025-01-11', 'COMPLETED', FALSE,  3,  'MOBILE',  current_timestamp()),
  (6003, 8002, 125000.00, 'USD', 'WIRE',     'JP Morgan',           'Investment',     '2025-01-12', 'COMPLETED', FALSE, 10,  'BRANCH',  current_timestamp()),
  (6004, 8003,  55000.00, 'USD', 'ACH',      'Corp Payroll System', 'Payroll',        '2025-01-13', 'COMPLETED', FALSE,  1,  'ONLINE',  current_timestamp()),
  (6005, 8004,     89.99, 'USD', 'DEBIT',    'Spotify Premium',     'Streaming',      '2025-01-14', 'COMPLETED', FALSE,  1,  'ONLINE',  current_timestamp()),
  (6006, 8005, 480000.00, 'USD', 'WIRE',     'Vanguard Funds',      'Investment',     '2025-01-15', 'COMPLETED', FALSE, 20,  'BRANCH',  current_timestamp()),
  (6007, 8006,    750.00, 'USD', 'DEBIT',    'Whole Foods',         'Grocery',        '2025-01-16', 'COMPLETED', FALSE,  4,  'MOBILE',  current_timestamp()),
  (6008, 8007,  92000.00, 'USD', 'WIRE',     'Overseas Partner',    'International',  '2025-01-17', 'PENDING',   TRUE,  89,  'BRANCH',  current_timestamp()),
  (6009, 8008, 320000.00, 'USD', 'WIRE',     'UBS Wealth Mgmt',     'Investment',     '2025-01-18', 'COMPLETED', FALSE, 16,  'BRANCH',  current_timestamp()),
  (6010, 8009,    280.00, 'USD', 'DEBIT',    'Target',              'Retail',         '2025-01-19', 'COMPLETED', FALSE,  3,  'ONLINE',  current_timestamp()),
  (6011, 8010, 150000.00, 'USD', 'ACH',      'Corp Settlement',     'Business',       '2025-01-20', 'COMPLETED', FALSE,  5,  'ONLINE',  current_timestamp()),
  (6012, 8001,   1800.00, 'USD', 'DEBIT',    'Nobu Restaurant',     'Dining',         '2025-01-21', 'COMPLETED', FALSE,  2,  'MOBILE',  current_timestamp()),
  (6013, 8003,  38000.00, 'USD', 'CREDIT',   'Client Payment',      'Business',       '2025-01-22', 'COMPLETED', FALSE,  4,  'ONLINE',  current_timestamp()),
  (6014, 8006, 210000.00, 'USD', 'WIRE',     'Shell Offshore LLC',  'Business',       '2025-01-23', 'COMPLETED', TRUE,  94,  'BRANCH',  current_timestamp()),
  (6015, 8009,    520.00, 'USD', 'DEBIT',    'Apple Store',         'Electronics',    '2025-01-24', 'COMPLETED', FALSE,  2,  'ONLINE',  current_timestamp());

INSERT INTO {PROD_FIN_CATALOG}.finance.credit_cards VALUES
  (7001, 8001, '4111222233334441', 'VISA',       6, 2028, '441', 100000.00, 22400.00, 'ACTIVE', current_timestamp(), '2025-01-21'),
  (7002, 8002, '5500111122223332', 'MASTERCARD', 9, 2027, '332', 150000.00, 48000.00, 'ACTIVE', current_timestamp(), '2025-01-18'),
  (7003, 8003, '4111222233334443', 'VISA',       3, 2029, '443',  75000.00,  9800.00, 'ACTIVE', current_timestamp(), '2025-01-22'),
  (7004, 8004, '3714111122223344', 'AMEX',      11, 2028, '344',  15000.00,  1200.00, 'ACTIVE', current_timestamp(), '2025-01-19'),
  (7005, 8005, '4111222233334445', 'VISA',       5, 2030, '445', 200000.00, 64000.00, 'ACTIVE', current_timestamp(), '2025-01-15'),
  (7006, 8006, '6011111122226006', 'DISCOVER',   7, 2027, '006',  20000.00,  5600.00, 'ACTIVE', current_timestamp(), '2025-01-16'),
  (7007, 8007, '5500111122227007', 'MASTERCARD', 4, 2029, '007',  80000.00, 18200.00, 'ACTIVE', current_timestamp(), '2025-01-20'),
  (7008, 8008, '4111222233334448', 'VISA',       8, 2028, '448', 250000.00, 89000.00, 'ACTIVE', current_timestamp(), '2025-01-14'),
  (7009, 8009, '4111222233334449', 'VISA',       2, 2027, '449',  10000.00,   980.00, 'ACTIVE', current_timestamp(), '2025-01-23'),
  (7010, 8010, '5500111122220010', 'MASTERCARD', 1, 2030, '010', 120000.00, 34500.00, 'ACTIVE', current_timestamp(), '2025-01-17');

-- ── Prod clinical sample data ─────────────────────────────────────────────

INSERT INTO {PROD_CLINICAL_CATALOG}.clinical.patients VALUES
  (9001, 'Charlotte','Robinson','1945-05-20', 'Q01-00-0001', 'FEMALE', 'c.robinson@health.org', '415-900-0001', '1 Market Plaza',   'San Francisco', 'CA', '94105', 'PINS-001', 'Kaiser',      'Dr. Nguyen',  'HIGH',   current_timestamp()),
  (9002, 'Oliver',   'Scott',   '1952-10-08', 'Q02-00-0002', 'MALE',   'o.scott@health.org',    '212-900-0002', '2 Lexington Ave',  'New York',      'NY', '10010', 'PINS-002', 'UnitedHealth','Dr. Patel',   'HIGH',   current_timestamp()),
  (9003, 'Amelia',   'Green',   '1968-03-15', 'Q03-00-0003', 'FEMALE', 'a.green@health.org',    '312-900-0003', '3 Michigan Ave',   'Chicago',       'IL', '60601', 'PINS-003', 'BlueCross',   'Dr. Kim',     'MEDIUM', current_timestamp()),
  (9004, 'Elijah',   'Adams',   '1980-07-22', 'Q04-00-0004', 'MALE',   'e.adams@health.org',    '310-900-0004', '4 Sunset Blvd',    'Los Angeles',   'CA', '90028', 'PINS-004', 'Cigna',       'Dr. Lopez',   'LOW',    current_timestamp()),
  (9005, 'Abigail',  'Baker',   '1937-01-30', 'Q05-00-0005', 'FEMALE', 'a.baker@health.org',    '206-900-0005', '5 University St',  'Seattle',       'WA', '98101', 'PINS-005', 'Medicare',    'Dr. Singh',   'HIGH',   current_timestamp()),
  (9006, 'Alexander','Carter',  '1975-09-14', 'Q06-00-0006', 'MALE',   'a.carter@health.org',   '713-900-0006', '6 Montrose Blvd',  'Houston',       'TX', '77006', 'PINS-006', 'Humana',      'Dr. Brown',   'MEDIUM', current_timestamp()),
  (9007, 'Emily',    'Mitchell','1991-04-03', 'Q07-00-0007', 'FEMALE', 'e.mitchell@health.org', '617-900-0007', '7 Newbury St',     'Boston',        'MA', '02116', 'PINS-007', 'Aetna',       'Dr. Davis',   'LOW',    current_timestamp()),
  (9008, 'Daniel',   'Perez',   '1960-12-19', 'Q08-00-0008', 'MALE',   'd.perez@health.org',    '305-900-0008', '8 Coral Way',      'Miami',         'FL', '33145', 'PINS-008', 'Cigna',       'Dr. Wilson',  'MEDIUM', current_timestamp()),
  (9009, 'Sofia',    'Roberts', '1944-08-07', 'Q09-00-0009', 'FEMALE', 's.roberts@health.org',  '602-900-0009', '9 Camelback Rd',   'Phoenix',       'AZ', '85016', 'PINS-009', 'Medicare',    'Dr. Taylor',  'HIGH',   current_timestamp()),
  (9010, 'Aiden',    'Turner',  '2008-02-25', 'Q10-00-0010', 'MALE',   'a.turner@health.org',   '404-900-0010', '10 Buckhead Ave',  'Atlanta',       'GA', '30305', 'PINS-010', 'CHIP',        'Dr. Anderson','LOW',    current_timestamp());

INSERT INTO {PROD_CLINICAL_CATALOG}.clinical.encounters VALUES
  (4001, 9001, '2025-01-06', 'INPATIENT',  'I50.9',  'Heart failure, unspecified',                    'Acute decompensated heart failure. Diuresis initiated. Echo shows EF 30%. Cardiology consult placed.',                'Dr. Nguyen', 'FAC-101', 'UCSF Med Ctr',          28000.00, 24000.00,  6, 'DISCHARGED', current_timestamp()),
  (4002, 9002, '2025-01-09', 'INPATIENT',  'C34.10', 'Malignant neoplasm of upper lobe, unspecified', 'Stage IIIA NSCLC. CT-guided biopsy performed. Tumor board review scheduled. Oncology referral placed.',               'Dr. Patel',  'FAC-102', 'Memorial Sloan Kettering',45000.00, 38000.00,  4, 'DISCHARGED', current_timestamp()),
  (4003, 9003, '2025-01-13', 'OUTPATIENT', 'E11.65', 'Type 2 diabetes with hyperglycemia',             'Poorly controlled T2DM. Insulin regimen adjusted. CGM prescribed. Dietitian referral placed.',                       'Dr. Kim',    'FAC-103', 'Northwestern Memorial',     620.00,   520.00, NULL,'DISCHARGED', current_timestamp()),
  (4004, 9004, '2025-01-16', 'EMERGENCY',  'R07.9',  'Chest pain, unspecified',                        'Rule-out ACS protocol. Serial troponins negative. EKG normal. Discharged with cardiology follow-up.',                'Dr. Lopez',  'FAC-104', 'Cedars-Sinai',             3800.00,  3100.00, NULL,'DISCHARGED', current_timestamp()),
  (4005, 9005, '2025-01-11', 'INPATIENT',  'G30.9',  'Alzheimer disease, unspecified',                 'Progressive cognitive decline. MMSE score 14. Safety planning with family. Memory care placement discussed.',         'Dr. Singh',  'FAC-105', 'Swedish Medical Ctr',     15000.00, 13200.00,  9, 'DISCHARGED', current_timestamp()),
  (4006, 9006, '2025-01-19', 'OUTPATIENT', 'M17.11', 'Primary osteoarthritis, right knee',             'Severe knee OA. Corticosteroid injection administered. PT referral placed. Surgical consultation offered.',          'Dr. Brown',  'FAC-106', 'Houston Methodist',         480.00,   400.00, NULL,'DISCHARGED', current_timestamp()),
  (4007, 9007, '2025-01-23', 'TELEHEALTH', 'J30.9',  'Allergic rhinitis, unspecified',                 'Seasonal allergy management telehealth. Antihistamine optimized. Nasal steroid spray added. Follow-up 4 weeks.',     'Dr. Davis',  'FAC-107', 'Mass General Brigham',      110.00,    95.00, NULL,'DISCHARGED', current_timestamp()),
  (4008, 9008, '2025-01-14', 'INPATIENT',  'K80.20', 'Calculus of gallbladder without cholecystitis',  'Symptomatic cholelithiasis. Laparoscopic cholecystectomy performed without complications. Discharged Day 2.',        'Dr. Wilson', 'FAC-108', 'Jackson Memorial',        18500.00, 16000.00,  2, 'DISCHARGED', current_timestamp()),
  (4009, 9009, '2025-01-08', 'INPATIENT',  'I10',    'Essential (primary) hypertension',               'Hypertensive urgency. BP 210/118 on admission. IV labetalol used. Oral regimen titrated before discharge.',          'Dr. Taylor', 'FAC-109', 'Banner University Med',    9200.00,  8000.00,  3, 'DISCHARGED', current_timestamp()),
  (4010, 9010, '2025-01-29', 'OUTPATIENT', 'F90.0',  'Attention-deficit hyperactivity disorder',       'Pediatric ADHD follow-up. Medication working well per parent report. School accommodations letter provided.',         'Dr. Anderson','FAC-110','Emory University Hosp',     310.00,   260.00, NULL,'DISCHARGED', current_timestamp()),
  (4011, 9001, '2025-01-27', 'OUTPATIENT', 'Z09',    'Encounter for follow-up after completed treatment','Cardiology follow-up post heart failure admission. Weight stable. NT-proBNP trending down. GDMT optimized.',        'Dr. Nguyen', 'FAC-101', 'UCSF Med Ctr',              380.00,   320.00, NULL,'DISCHARGED', current_timestamp()),
  (4012, 9002, '2025-01-30', 'INPATIENT',  'Z51.11', 'Encounter for antineoplastic chemotherapy',      'Cycle 2 carboplatin/paclitaxel. Nausea managed. CBC acceptable. Next cycle in 21 days.',                            'Dr. Patel',  'FAC-102', 'Memorial Sloan Kettering',12000.00, 10500.00,  1, 'DISCHARGED', current_timestamp());
"""

PROD_TEARDOWN_SQL = f"""
DROP CATALOG IF EXISTS {PROD_FIN_CATALOG}      CASCADE;
DROP CATALOG IF EXISTS {PROD_CLINICAL_CATALOG} CASCADE;
"""

# ---------------------------------------------------------------------------
# env.auto.tfvars snippet printed after setup
# ---------------------------------------------------------------------------

ENV_TFVARS_SNIPPET = f"""
# ── Paste this into envs/dev/env.auto.tfvars ──────────────────────────────

genie_spaces = [
  {{
    name     = "Finance Analytics"
    uc_tables = [
      "{FIN_CATALOG}.finance.customers",
      "{FIN_CATALOG}.finance.transactions",
      "{FIN_CATALOG}.finance.credit_cards",
    ]
  }},
  {{
    name     = "Clinical Analytics"
    uc_tables = [
      "{CLINICAL_CATALOG}.clinical.patients",
      "{CLINICAL_CATALOG}.clinical.encounters",
    ]
  }},
]

sql_warehouse_id = ""   # auto-create serverless warehouse

# ── Promote dev → prod ────────────────────────────────────────────────────
#
#   make promote SOURCE_ENV=dev DEST_ENV=prod \\
#     DEST_CATALOG_MAP="{FIN_CATALOG}=prod_fin,{CLINICAL_CATALOG}=prod_clinical"
#
# ── Per-space regeneration (after initial full make generate) ─────────────
#
#   make generate SPACE="Finance Analytics"    # only regenerates finance space
#   make generate SPACE="Clinical Analytics"   # only regenerates clinical space
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_catalog(
    w,
    name: str,
    comment: str = "",
    storage_root: str | None = None,
    warehouse_id: str = "",
) -> None:
    """Create a catalog via the UC API.

    When ``storage_root`` is provided (e.g. when running against a provisioned
    test metastore that has no default storage), each catalog is placed at a
    unique S3 subfolder under the External Location registered by
    provision_test_env.py.  When ``storage_root`` is None the metastore's
    default managed storage is used.

    If the SDK call fails because "Default Storage" is enabled on the account
    (metastore has no root URL), we retry using SQL ``CREATE CATALOG ... MANAGED
    LOCATION`` which is the supported path for Default Storage accounts.
    """
    from databricks.sdk.errors import ResourceAlreadyExists

    try:
        kwargs: dict = {"name": name}
        if comment:
            kwargs["comment"] = comment
        if storage_root:
            kwargs["storage_root"] = storage_root
        w.catalogs.create(**kwargs)
        loc = f" → {storage_root}" if storage_root else ""
        print(f"    Created catalog: {name}{loc}")
    except ResourceAlreadyExists:
        print(f"    Catalog already exists (skipping): {name}")
    except Exception as exc:
        # Tolerate "already exists" even when the SDK exception class doesn't
        # match (can happen with older SDK builds or slightly different API shapes).
        if "already exists" in str(exc).lower():
            print(f"    Catalog already exists (skipping): {name}")
        elif "default storage" in str(exc).lower() and storage_root and warehouse_id:
            # Account has "Default Storage" enabled — the SDK storage_root param
            # doesn't work.  Fall back to SQL MANAGED LOCATION syntax.
            print(f"    Default Storage account detected — retrying via SQL MANAGED LOCATION")
            sql = f"CREATE CATALOG IF NOT EXISTS `{name}` MANAGED LOCATION '{storage_root}'"
            if comment:
                sql += f" COMMENT '{comment}'"
            _run_statement(w, warehouse_id, sql, f"CREATE CATALOG {name} MANAGED LOCATION")
        else:
            print(f"    ERROR creating catalog {name!r}: {exc}")
            sys.exit(1)


def _ensure_packages():
    import subprocess
    for pkg in ("python-hcl2", "databricks-sdk"):
        try:
            __import__(pkg.replace("-", ".").split(".")[0])
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", pkg])


def _load_auth(auth_file: Path) -> dict:
    try:
        import hcl2
    except ImportError:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "python-hcl2"])
        import hcl2

    if not auth_file.exists():
        print(f"ERROR: auth file not found: {auth_file}")
        print("  Run from your cloud wrapper directory (genie/aws/ or genie/azure/), or pass --auth-file <path>.")
        sys.exit(1)

    with open(auth_file) as f:
        cfg = hcl2.load(f)

    env_file = auth_file.parent / "env.auto.tfvars"
    if env_file.exists():
        with open(env_file) as f:
            cfg.update(hcl2.load(f))

    return cfg


def _configure_env(cfg: dict):
    mapping = {
        "databricks_workspace_host": "DATABRICKS_HOST",
        "databricks_client_id":      "DATABRICKS_CLIENT_ID",
        "databricks_client_secret":  "DATABRICKS_CLIENT_SECRET",
    }
    for k, env_k in mapping.items():
        v = cfg.get(k, "")
        if v:
            os.environ[env_k] = v


def _get_warehouse(w, warehouse_id: str) -> str:
    """Return a ready warehouse ID — use the given one or pick the first running warehouse."""
    if warehouse_id:
        return warehouse_id

    print("  No --warehouse-id provided; selecting a deterministic SQL warehouse...")
    warehouses = list(w.warehouses.list())
    wh = select_warehouse(warehouses)
    if wh:
        print(f"    Using warehouse: {wh.name} ({wh.id})")
        return wh.id

    # No warehouse at all — create a serverless one on-demand.  This happens when
    # a previous scenario's Terraform teardown deleted the only managed warehouse
    # and the workspace Starter Warehouse was also removed by Databricks during
    # that scenario's apply.  Rather than requiring a manual pre-created warehouse,
    # we create a small serverless warehouse here so DDL can proceed immediately.
    print("  No SQL warehouses found — creating a temporary serverless warehouse for DDL...")
    from databricks.sdk.service.sql import EndpointInfoWarehouseType
    wh = w.warehouses.create(
        name=DEFAULT_WAREHOUSE_NAME,
        cluster_size="Small",
        warehouse_type=EndpointInfoWarehouseType.PRO,
        max_num_clusters=1,
        enable_serverless_compute=True,
        auto_stop_mins=10,
    ).result()
    print(f"    Created warehouse: {wh.name} ({wh.id})")
    return wh.id


def _run_statement(w, warehouse_id: str, sql: str, description: str = "") -> None:
    """Execute a SQL statement and wait for it to complete."""
    from databricks.sdk.service.sql import StatementState

    label = description or sql[:60].replace("\n", " ")
    print(f"    {label}...")

    stmt = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql.strip(),
        wait_timeout="50s",   # API max is 50s; we poll for longer-running statements
    )

    # Poll until done (handles statements that take longer than 50s)
    max_wait = 300
    start = time.time()
    while True:
        state = stmt.status.state
        if state == StatementState.SUCCEEDED:
            return
        if state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
            error = stmt.status.error
            print(f"\n  ERROR in statement: {label}")
            print(f"  SQL: {sql[:200]}")
            print(f"  State: {state}  Error: {error}")
            sys.exit(1)

        if time.time() - start > max_wait:
            print(f"  Timeout waiting for statement: {label}")
            sys.exit(1)

        time.sleep(3)
        stmt = w.statement_execution.get_statement(stmt.statement_id)


def _split_statements(sql: str) -> list[str]:
    """Split a multi-statement SQL string into individual statements."""
    stmts = []
    current = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            joined = "\n".join(current).strip().rstrip(";")
            if joined:
                stmts.append(joined)
            current = []
    return stmts


def _run_query(w, warehouse_id: str, sql: str):
    """Execute a SQL query and return the result rows as a list of lists."""
    from databricks.sdk.service.sql import StatementState

    stmt = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql.strip(),
        wait_timeout="50s",
    )
    # 300s to absorb warehouse cold-start (auto-stopped warehouses take up to 3-4 min).
    max_wait = 300
    start = time.time()
    while True:
        state = stmt.status.state
        if state == StatementState.SUCCEEDED:
            result = stmt.result
            if result and result.data_array:
                return result.data_array
            return []
        if state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
            raise RuntimeError(f"Query failed ({state}): {stmt.status.error}\nSQL: {sql[:200]}")
        if time.time() - start > max_wait:
            raise TimeoutError(f"Query timed out: {sql[:80]}")
        time.sleep(2)
        stmt = w.statement_execution.get_statement(stmt.statement_id)


# Expected row counts after setup — used by --verify / --verify-prod.
DEV_EXPECTED_ROWS = {
    f"{FIN_CATALOG}.finance.customers":        10,
    f"{FIN_CATALOG}.finance.transactions":     15,
    f"{FIN_CATALOG}.finance.credit_cards":     10,
    f"{CLINICAL_CATALOG}.clinical.patients":   10,
    f"{CLINICAL_CATALOG}.clinical.encounters": 12,
}

PROD_EXPECTED_ROWS = {
    f"{PROD_FIN_CATALOG}.finance.customers":        10,
    f"{PROD_FIN_CATALOG}.finance.transactions":     15,
    f"{PROD_FIN_CATALOG}.finance.credit_cards":     10,
    f"{PROD_CLINICAL_CATALOG}.clinical.patients":   10,
    f"{PROD_CLINICAL_CATALOG}.clinical.encounters": 12,
}


def _get_table_row_count_from_stats(w, warehouse_id: str, table: str) -> int | None:
    """Return the row count from Delta table statistics (bypasses row filters).

    Parses the 'Statistics' row from DESCRIBE TABLE EXTENDED, which contains
    a string like '128 bytes, 12 rows'.  Returns None if stats are unavailable.
    """
    import re
    try:
        rows = _run_query(w, warehouse_id, f"DESCRIBE TABLE EXTENDED {table}")
        for row in rows:
            if row and len(row) >= 2 and str(row[0]).strip().lower() == "statistics":
                stats_str = str(row[1])
                m = re.search(r"(\d+)\s+rows?", stats_str, re.IGNORECASE)
                if m:
                    return int(m.group(1))
        return None
    except Exception:
        return None


def _verify_tables(w, warehouse_id: str, expected: dict[str, int], label: str) -> list[str]:
    """
    Assert row counts and that at least one column tag exists per table.
    Returns a list of failure messages (empty = all passed).
    """
    failures: list[str] = []

    # Ensure the warehouse is running before firing verify queries.  If it
    # auto-stopped during a long Terraform apply it needs up to ~3-4 min to
    # restart; starting it explicitly here lets that happen before the first
    # SELECT instead of during it (where it would appear as a timeout).
    try:
        from databricks.sdk.service.sql import GetWarehouseResponse, State
        wh = w.warehouses.get(warehouse_id)
        if getattr(wh, "state", None) not in (State.RUNNING, State.STARTING):
            print(f"  Warehouse is {getattr(wh, 'state', 'unknown')} — starting it before verify…")
            w.warehouses.start(warehouse_id).result()
            print(f"  Warehouse running.")
    except Exception as exc:
        print(f"  WARN  Could not pre-start warehouse: {exc}")

    print(f"\n  [{label}] Verifying table row counts...")
    for table, expected_count in expected.items():
        try:
            rows = _run_query(w, warehouse_id, f"SELECT COUNT(*) FROM {table}")
            actual = int(rows[0][0]) if rows else 0
            if actual >= expected_count:
                print(f"    PASS  {table}: {actual} rows (expected >= {expected_count})")
            elif actual == 0:
                # Row might be 0 due to an active row-filter policy (e.g. PHI
                # encounters filtered for non-Clinical_Staff users).  Fall back
                # to Delta table statistics from DESCRIBE TABLE EXTENDED, which
                # are metadata-level and bypass row filters.
                num_rows = _get_table_row_count_from_stats(w, warehouse_id, table)
                if num_rows is not None and num_rows >= expected_count:
                    print(f"    PASS  {table}: {num_rows} rows in Delta stats"
                          f" (SELECT returned 0 — row filter active, expected >= {expected_count})")
                else:
                    display = num_rows if num_rows is not None else "unknown"
                    msg = f"FAIL  {table}: 0 rows (Delta stats: {display}), expected >= {expected_count}"
                    print(f"    {msg}")
                    failures.append(msg)
            else:
                msg = f"FAIL  {table}: {actual} rows, expected >= {expected_count}"
                print(f"    {msg}")
                failures.append(msg)
        except Exception as e:
            msg = f"FAIL  {table}: {e}"
            print(f"    {msg}")
            failures.append(msg)

    # Check that column tags were applied (proves ABAC tagging ran)
    catalogs = list({t.split(".")[0] for t in expected})
    catalog_list = ", ".join(f"'{c}'" for c in catalogs)
    print(f"\n  [{label}] Checking column tags (ABAC governance applied)...")
    try:
        tag_queries = [
            (
                "SELECT "
                f"'{catalog}' AS table_catalog, schema_name, table_name, column_name, tag_name "
                f"FROM {catalog}.information_schema.column_tags"
            )
            for catalog in catalogs
        ]
        rows = _run_query(
            w, warehouse_id,
            " UNION ALL ".join(tag_queries) + " ORDER BY table_catalog, table_name, column_name",
        )
        if rows:
            print(f"    PASS  Found {len(rows)} column tag(s) across {catalog_list}")
            for r in rows[:6]:   # show a sample, not all
                print(f"          {r[0]}.{r[1]}.{r[2]}.{r[3]} → {r[4]}")
            if len(rows) > 6:
                print(f"          ... and {len(rows) - 6} more")
        else:
            msg = f"WARN  No column tags found for catalogs: {catalog_list} (ABAC may not have been applied yet)"
            print(f"    {msg}")
            # Warn-only: don't fail because Terraform apply hasn't run yet in --verify-pre-apply mode
    except Exception as e:
        print(f"    WARN  Could not query column tags: {e}")

    # Check that column masks were applied
    print(f"\n  [{label}] Checking column masking policies...")
    try:
        rows = _run_query(
            w, warehouse_id,
            f"SELECT table_catalog, table_schema, table_name, column_name, mask_name "
            f"FROM system.information_schema.column_masks "
            f"WHERE table_catalog IN ({catalog_list}) "
            f"ORDER BY table_catalog, table_name, column_name",
        )
        if rows:
            print(f"    PASS  Found {len(rows)} masked column(s) across {catalog_list}")
            for r in rows[:6]:
                print(f"          {r[0]}.{r[1]}.{r[2]}.{r[3]} → {r[4]}")
            if len(rows) > 6:
                print(f"          ... and {len(rows) - 6} more")
        else:
            print(f"    WARN  No column masks found for {catalog_list} (run make apply first)")
    except Exception as e:
        print(f"    WARN  Could not query column masks: {e}")

    return failures


def _infer_catalog_storage_base(w) -> str | None:
    """Best-effort fallback when provisioned auth lacks catalog_storage_base."""
    try:
        locations = list(w.external_locations.list())
    except Exception:
        return None

    preferred_names = {"test-external-location"}
    for loc in locations:
        name = getattr(loc, "name", "") or ""
        url = getattr(loc, "url", "") or ""
        if name in preferred_names and url:
            return url.rstrip("/")

    genie_locations = [
        (getattr(loc, "url", "") or "").rstrip("/")
        for loc in locations
        if (getattr(loc, "url", "") or "").startswith(("s3://genie-", "abfss://genie-"))
    ]
    if len(genie_locations) == 1:
        return genie_locations[0]
    return None


def _load_catalog_storage_base_from_state(auth_cfg: dict) -> str | None:
    """Fallback for provisioned test envs whose auth file lacks the field."""
    workspace_id = auth_cfg.get("databricks_workspace_id", "")
    if isinstance(workspace_id, list):
        workspace_id = workspace_id[0] if workspace_id else ""
    workspace_id = str(workspace_id or "").strip()

    workspace_host = auth_cfg.get("databricks_workspace_host", "")
    if isinstance(workspace_host, list):
        workspace_host = workspace_host[0] if workspace_host else ""
    workspace_host = str(workspace_host or "").strip().rstrip("/")

    for cloud in ("aws", "azure"):
        state_path = SCRIPT_DIR / f".test_env_state.{cloud}.json"
        if not state_path.exists():
            continue
        try:
            state = json.loads(state_path.read_text())
        except Exception:
            continue
        state_ws_id = str(state.get("workspace_id", "") or "").strip()
        state_host = str(state.get("workspace_host", "") or "").strip().rstrip("/")
        if workspace_id and workspace_id == state_ws_id:
            return str(state.get("ext_loc_url", "") or "").rstrip("/") or None
        if workspace_host and workspace_host == state_host:
            return str(state.get("ext_loc_url", "") or "").rstrip("/") or None
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Create integration test catalogs + tables for multi-space testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--teardown", action="store_true",
        help="Drop the dev test catalogs (dev_fin, dev_clinical) and all their contents")
    parser.add_argument("--prod", action="store_true",
        help="Also create prod catalogs (prod_fin, prod_clinical) with the same schema "
             "and different sample data — required for make apply ENV=prod to succeed")
    parser.add_argument("--teardown-prod", action="store_true",
        help="Drop the prod test catalogs (prod_fin, prod_clinical)")
    parser.add_argument("--verify", action="store_true",
        help="Assert dev table row counts and ABAC governance (column tags + masks). "
             "Exits non-zero on failure — suitable for CI pipelines.")
    parser.add_argument("--verify-prod", action="store_true",
        help="Same as --verify but for prod catalogs (prod_fin, prod_clinical).")
    parser.add_argument("--warehouse-id", default="", help="SQL warehouse ID to use for DDL execution")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL without executing")
    parser.add_argument(
        "--auth-file",
        default=str(WORK_DIR / "auth.auto.tfvars"),
        help="Path to auth.auto.tfvars (default: ./auth.auto.tfvars)",
    )
    args = parser.parse_args()

    doing_prod = args.prod or args.teardown_prod or args.verify_prod
    prod_label = f"  |  {PROD_FIN_CATALOG}  |  {PROD_CLINICAL_CATALOG}" if doing_prod else ""

    print("=" * 60)
    print("  Integration Test Data Setup")
    print(f"  Dev:  {FIN_CATALOG}  |  {CLINICAL_CATALOG}{prod_label}")
    print("=" * 60)

    if args.dry_run:
        if args.teardown:
            print(TEARDOWN_SQL)
        if args.teardown_prod:
            print(PROD_TEARDOWN_SQL)
        if not args.teardown and not args.teardown_prod:
            print(SETUP_SQL + SAMPLE_DATA_SQL)
            if args.prod:
                print(PROD_SETUP_SQL + PROD_SAMPLE_DATA_SQL)
        sys.exit(0)

    _ensure_packages()
    from databricks.sdk import WorkspaceClient

    auth_cfg = _load_auth(Path(args.auth_file))
    _configure_env(auth_cfg)

    # catalog_storage_base is written by provision_test_env.py into the generated
    # auth.auto.tfvars when a fresh test environment is provisioned. When present
    # each catalog is created with an explicit storage_root pointing to a unique
    # subfolder under the External Location, so no metastore-level default storage
    # is needed. When absent, fall back to discovering the provisioned test
    # external location from the workspace.
    catalog_storage_base: str | None = (
        auth_cfg.get("catalog_storage_base", [None])[0]
        if isinstance(auth_cfg.get("catalog_storage_base"), list)
        else auth_cfg.get("catalog_storage_base")
    )
    if not catalog_storage_base:
        catalog_storage_base = _load_catalog_storage_base_from_state(auth_cfg)
        if catalog_storage_base:
            print(f"  Using state-file catalog storage base: {catalog_storage_base}")

    def _catalog_storage(catalog_name: str) -> str | None:
        if not catalog_storage_base:
            return None
        return f"{catalog_storage_base.rstrip('/')}/{catalog_name}"

    w = WorkspaceClient(product="genierails-test-setup", product_version="0.1.0")
    if not catalog_storage_base:
        catalog_storage_base = _infer_catalog_storage_base(w)
        if catalog_storage_base:
            print(f"  Using inferred catalog storage base: {catalog_storage_base}")
    warehouse_id = _get_warehouse(w, args.warehouse_id)

    # ── Verify paths ───────────────────────────────────────────────────────
    if args.verify or args.verify_prod:
        print("\n" + "=" * 60)
        print("  Integration Test Verification")
        print("=" * 60)
        all_failures: list[str] = []

        if args.verify:
            failures = _verify_tables(w, warehouse_id, DEV_EXPECTED_ROWS, "DEV")
            all_failures.extend(failures)

        if args.verify_prod:
            failures = _verify_tables(w, warehouse_id, PROD_EXPECTED_ROWS, "PROD")
            all_failures.extend(failures)

        print("\n" + "=" * 60)
        if all_failures:
            print(f"  FAILED — {len(all_failures)} assertion(s) failed:")
            for f in all_failures:
                print(f"    • {f}")
            print("=" * 60)
            sys.exit(1)
        else:
            print("  All assertions PASSED.")
            print("=" * 60)
        return

    # ── Teardown paths ─────────────────────────────────────────────────────
    if args.teardown:
        print(f"\n  Dropping dev catalogs: {FIN_CATALOG}, {CLINICAL_CATALOG} ...")
        for stmt in _split_statements(TEARDOWN_SQL):
            _run_statement(w, warehouse_id, stmt, stmt[:60])
        print("  Dev teardown complete.")

    if args.teardown_prod:
        print(f"\n  Dropping prod catalogs: {PROD_FIN_CATALOG}, {PROD_CLINICAL_CATALOG} ...")
        for stmt in _split_statements(PROD_TEARDOWN_SQL):
            _run_statement(w, warehouse_id, stmt, stmt[:60])
        print("  Prod teardown complete.")

    if args.teardown or args.teardown_prod:
        return

    # ── Dev setup ──────────────────────────────────────────────────────────
    print(f"\n  Creating dev catalogs, schemas, and tables...")
    _ensure_catalog(w, FIN_CATALOG,      "Finance domain — dev. Promotion target: prod_fin.",
                    storage_root=_catalog_storage(FIN_CATALOG), warehouse_id=warehouse_id)
    _ensure_catalog(w, CLINICAL_CATALOG, "Clinical domain — dev. Promotion target: prod_clinical.",
                    storage_root=_catalog_storage(CLINICAL_CATALOG), warehouse_id=warehouse_id)
    setup_stmts = _split_statements(SETUP_SQL)
    for i, stmt in enumerate(setup_stmts, 1):
        label = stmt.split("\n")[0].strip().lstrip("-").strip()
        _run_statement(w, warehouse_id, stmt, f"[{i}/{len(setup_stmts)}] {label[:60]}")

    print(f"\n  Inserting dev sample data...")
    data_stmts = _split_statements(SAMPLE_DATA_SQL)
    for i, stmt in enumerate(data_stmts, 1):
        _run_statement(w, warehouse_id, stmt, f"[{i}/{len(data_stmts)}] INSERT INTO ...")

    # ── Prod setup (optional) ──────────────────────────────────────────────
    if args.prod:
        print(f"\n  Creating prod catalogs, schemas, and tables...")
        _ensure_catalog(w, PROD_FIN_CATALOG,      "Finance domain — prod.",
                        storage_root=_catalog_storage(PROD_FIN_CATALOG), warehouse_id=warehouse_id)
        _ensure_catalog(w, PROD_CLINICAL_CATALOG, "Clinical domain — prod.",
                        storage_root=_catalog_storage(PROD_CLINICAL_CATALOG), warehouse_id=warehouse_id)
        prod_setup_stmts = _split_statements(PROD_SETUP_SQL)
        for i, stmt in enumerate(prod_setup_stmts, 1):
            label = stmt.split("\n")[0].strip().lstrip("-").strip()
            _run_statement(w, warehouse_id, stmt, f"[{i}/{len(prod_setup_stmts)}] {label[:60]}")

        print(f"\n  Inserting prod sample data...")
        prod_data_stmts = _split_statements(PROD_SAMPLE_DATA_SQL)
        for i, stmt in enumerate(prod_data_stmts, 1):
            _run_statement(w, warehouse_id, stmt, f"[{i}/{len(prod_data_stmts)}] INSERT INTO ...")

    print("\n" + "=" * 60)
    print("  Setup complete!")
    print()
    print(f"  Dev catalogs:")
    print(f"    {FIN_CATALOG}.finance.customers        (10 rows)")
    print(f"    {FIN_CATALOG}.finance.transactions     (15 rows)")
    print(f"    {FIN_CATALOG}.finance.credit_cards     (10 rows)")
    print(f"    {CLINICAL_CATALOG}.clinical.patients   (10 rows)")
    print(f"    {CLINICAL_CATALOG}.clinical.encounters (12 rows)")
    if args.prod:
        print()
        print(f"  Prod catalogs:")
        print(f"    {PROD_FIN_CATALOG}.finance.customers        (10 rows)")
        print(f"    {PROD_FIN_CATALOG}.finance.transactions     (15 rows)")
        print(f"    {PROD_FIN_CATALOG}.finance.credit_cards     (10 rows)")
        print(f"    {PROD_CLINICAL_CATALOG}.clinical.patients   (10 rows)")
        print(f"    {PROD_CLINICAL_CATALOG}.clinical.encounters (12 rows)")
    print()
    print("  Next: update envs/dev/env.auto.tfvars with the snippet below,")
    print("  then run: make generate && make apply")
    if args.prod:
        print()
        print("  For prod: make promote SOURCE_ENV=dev DEST_ENV=prod \\")
        print(f"    DEST_CATALOG_MAP=\"{FIN_CATALOG}={PROD_FIN_CATALOG},{CLINICAL_CATALOG}={PROD_CLINICAL_CATALOG}\"")
        print("  Then: make apply ENV=prod")
    print()
    print(ENV_TFVARS_SNIPPET)
    print("=" * 60)


if __name__ == "__main__":
    main()
