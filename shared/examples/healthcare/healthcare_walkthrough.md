# Healthcare ABAC — Walkthrough

A step-by-step example applying GenieRails to a healthcare scenario with four clinical tables. This shows the complete flow from table DDL to a governed Genie Space.

For the general workflow, see [From UI to Production](../../docs/from-ui-to-production.md) or [Quickstart](../../docs/quickstart.md).

---

## The tables

Four tables from a hospital data platform (DDL files in [`ddl/`](ddl/)):

| Table | Description | Sensitive columns |
|-------|-------------|-------------------|
| `patients` | Demographics, contact info, insurance | SSN, MRN, name, email, phone, address, insurance ID |
| `encounters` | Visits, diagnoses, clinical notes | Diagnosis code/description, treatment notes |
| `prescriptions` | Medications and dosages | Drug name, dosage |
| `billing` | Financial records, insurance claims | Total amount, insurance paid, patient owed |

## Step 1 — Set up credentials

```bash
cd aws   # or: cd azure

# Copy and fill in your credentials
cp shared/auth.auto.tfvars.example envs/dev/auth.auto.tfvars
# Edit envs/dev/auth.auto.tfvars with your service principal credentials
```

See [Prerequisites](../../docs/prerequisites.md) for what credentials you need.

## Step 2 — Configure your tables

Edit `envs/dev/env.auto.tfvars`:

```hcl
uc_tables = [
  "hc_catalog.clinical.patients",
  "hc_catalog.clinical.encounters",
  "hc_catalog.clinical.prescriptions",
  "hc_catalog.clinical.billing",
]

genie_spaces = [
  {
    name = "Clinical Analytics"
    config = {
      title       = "Clinical Analytics"
      description = "AI-powered analytics for clinical data with HIPAA-compliant governance."
    }
  },
]

sql_warehouse_id = ""   # auto-create serverless warehouse
```

## Step 3 — Generate ABAC config

```bash
make generate ENV=dev
```

This calls the Databricks Foundation Model to analyze your table schemas and generate:
- `envs/dev/generated/abac.auto.tfvars` — groups, tag policies, tag assignments, FGAC policies, Genie Space config
- `envs/dev/generated/masking_functions.sql` — SQL UDFs for column masking

## Step 4 — Review and tune

Open `envs/dev/generated/abac.auto.tfvars` and review:

- **Groups** — are the access tiers right for your organization?
- **Tag assignments** — did the AI correctly identify all sensitive columns?
- **FGAC policies** — are the masking rules appropriate (e.g., Nurses see partial PII, Billing Clerks see no clinical notes)?
- **Genie Space config** — are the instructions and sample questions useful?

See `envs/dev/generated/TUNING.md` for tuning guidance.

## Step 5 — Apply

```bash
make apply ENV=dev
```

This promotes the config into three layers and applies them:

1. **Account layer** (`envs/account/`) — creates groups and tag policies
2. **Data access layer** (`envs/dev/data_access/`) — applies tag assignments, deploys masking functions, creates FGAC policies
3. **Workspace layer** (`envs/dev/`) — creates the Genie Space with ACLs

## What each group sees after deployment

| Column | Nurse | Physician | Billing Clerk | CMO |
|--------|-------|-----------|---------------|-----|
| `patients.first_name` | `J***n` | John | `J***n` | John |
| `patients.ssn` | `***-**-1234` | 123-45-1234 | `***-**-1234` | 123-45-1234 |
| `patients.mrn` | `****5678` | MRN005678 | `****5678` | MRN005678 |
| `encounters.diagnosis_code` | E11.65 | E11.65 | `E11.xx` | E11.65 |
| `encounters.treatment_notes` | _full text_ | _full text_ | `[REDACTED]` | _full text_ |
| `billing.total_amount` | `$1,200` | `$1,234.56` | `$1,234.56` | `$1,234.56` |
| `patients.insurance_id` | `ACCT-a1b2c3...` | `ACCT-a1b2c3...` | INS-9876543 | INS-9876543 |
| **US_East_Staff rows** | US_EAST only | All | All | All |

## Key design decisions

1. **Four sensitivity dimensions**: `phi_level`, `pii_level`, `financial_sensitivity`, `compliance_scope` — mapped to HIPAA categories
2. **Nurse vs Billing separation**: Nurses see clinical data but masked financials; Billing Clerks see financials but redacted clinical notes — HIPAA minimum necessary principle
3. **CMO as unrestricted**: `Chief_Medical_Officer` is excluded via `except_principals` — no masking applied
4. **Regional row filters**: `US_East_Staff` / `US_West_Staff` see only their facility's data
5. **Insurance ID tokenized**: Deterministic SHA-256 hash so non-billing staff can join across tables without seeing the real policy number

## Pre-built example files

If you want to skip AI generation and use the pre-built healthcare config directly:

```
examples/healthcare/
├── ddl/                                   # Table DDL files
├── masking_functions.sql                  # Pre-built masking SQL UDFs
├── account/abac.auto.tfvars.example       # Account layer config
├── data_access/abac.auto.tfvars.example   # Data access layer config
├── env.auto.tfvars.example                # Environment config
└── abac.auto.tfvars.example               # Workspace layer (Genie Space) config
```

Copy these into your `envs/` directory structure and run `make apply ENV=dev`.

## Next steps

- [Promote dev to prod](../../docs/playbook.md#promote-dev--prod) — replicate governance to production with catalog remapping
- [Version control your configs](../../docs/version-control.md) — what to commit and how to manage changes
- [CI/CD integration](../../docs/cicd.md) — automate validation and deployment
