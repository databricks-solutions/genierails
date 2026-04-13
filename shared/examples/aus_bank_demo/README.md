# Australian Bank Demo — End-to-End

An end-to-end demo of GenieRails for an Australian retail bank. Start with an existing Genie Space, import it into code, generate ABAC governance with ANZ + financial services overlays, deploy, and promote to production.

**What you'll show:**
- ANZ-specific masking: Tax File Numbers (TFN), Medicare numbers, BSB codes
- Financial services governance: PCI-DSS card masking, AML risk row filters, transaction amount rounding
- Role-based access: 5 groups (Bank Teller → Compliance Officer) with different views of the same data
- Dev → prod promotion with catalog remapping across workspaces

**Time:** ~20 minutes (5 min setup, 15 min demo)

**Prerequisites:** [Prerequisites](../../docs/prerequisites.md) — Python 3, Terraform, account admin credentials. Azure users: also see [Azure Prerequisites](../../../azure/docs/azure-prerequisites.md).

---

## Setup (~5 min)

The setup script provisions two isolated workspaces and creates sample Australian banking data. It works on both AWS and Azure.

### 1. Create credentials file

#### AWS

```bash
cd aws

# Copy the example and fill in your Account Admin SP credentials
cp ../shared/scripts/account-admin.aws.env.example ../shared/scripts/account-admin.aws.env
vi ../shared/scripts/account-admin.aws.env
```

You need:
- `DATABRICKS_ACCOUNT_ID` — your Databricks account UUID ([Account Console](https://accounts.cloud.databricks.com) → top-right menu)
- `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` — Account Admin service principal OAuth credentials
- `DATABRICKS_AWS_REGION` — AWS region (default: `ap-southeast-2`)
- AWS credentials (IAM keys, SSO profile, or instance role) — for S3 bucket + IAM role creation

See [Prerequisites](../../docs/prerequisites.md) for detailed setup instructions.

#### Azure

```bash
cd azure

# Copy the example and fill in your credentials
cp ../shared/scripts/account-admin.azure.env.example ../shared/scripts/account-admin.azure.env
vi ../shared/scripts/account-admin.azure.env
```

You need:
- `DATABRICKS_ACCOUNT_ID` — your Databricks account UUID ([Account Console](https://accounts.azuredatabricks.net) → top-right menu)
- `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` — Account Admin service principal OAuth credentials
- `AZURE_SUBSCRIPTION_ID` — your Azure subscription
- `AZURE_RESOURCE_GROUP` — resource group for test resources (must already exist)
- `AZURE_REGION` — Azure region matching your workspace (e.g., `australiaeast`, `eastus2`)
- `AZURE_TENANT_ID` — your Azure AD tenant ID
- `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` — Azure AD App Registration credentials

The Azure SP needs **Contributor** and **Storage Blob Data Contributor** RBAC roles on the resource group. See [Azure Prerequisites](../../../azure/docs/azure-prerequisites.md) for details.

### 2. Provision the demo

#### AWS

```bash
python ../shared/examples/aus_bank_demo/setup_demo.py provision \
    --env-file ../shared/scripts/account-admin.aws.env
```

#### Azure

```bash
CLOUD_PROVIDER=azure CLOUD_ROOT=$(pwd) \
python ../shared/examples/aus_bank_demo/setup_demo.py provision \
    --env-file ../shared/scripts/account-admin.azure.env
```

When complete, you'll see:
```
  Dev workspace:   https://dbc-xxx.cloud.databricks.com          (AWS)
                   https://adb-xxx.12.azuredatabricks.net        (Azure)
  Prod workspace:  https://dbc-yyy.cloud.databricks.com          (AWS)
                   https://adb-yyy.12.azuredatabricks.net        (Azure)
  Genie Space ID:  01ef7b3c2a4d5e6f
```

**Before the demo:** Open the dev workspace and show the Genie Space in the UI. Point out that anyone can see raw TFN numbers, full credit card PANs, and AML risk flags — no governance at all.

---

## Part 1: The Challenge (2 min)

Open the Genie Space "Kookaburra Bank Analytics" in the dev workspace UI. Show the four tables — customers, accounts, transactions, credit cards.

**Set the scene:**

> _"The data platform team has built this Genie Space for our retail banking analysts. The tables are ready, the Space is configured — but we can't onboard business users yet. Why? Because these tables contain highly sensitive data:"_

Point out what's in the tables:
- **Tax File Numbers (TFN)** — protected under Privacy Act 1988
- **Medicare numbers** — protected under My Health Records Act
- **Full credit card PANs and CVVs** — PCI-DSS regulated
- **AML risk flags** (`HIGH_RISK`, `BLOCKED`) — restricted to the compliance team
- **BSB + account numbers** — financial identity risk

> _"We need to onboard 5 different teams — tellers, relationship managers, compliance, marketing, and branch managers. Each team needs different access levels. A teller should see masked TFN and last-4 of cards. A compliance officer needs full access to investigate AML flags. Marketing should only see anonymized, aggregated data."_

> _"Setting this up manually — groups, tag policies, masking functions, row filters, ACLs, entitlements — would take weeks. Let's do it in 10 minutes."_

---

## Part 2: Import & Generate (5 min)

### 2a. Generate ABAC governance

The setup script already configured `envs/dev/` with auth credentials, tables, and the Genie Space ID.

```bash
make generate ENV=dev COUNTRY=ANZ INDUSTRY=financial_services
```

**What happens:** GenieRails discovers the 4 tables from the existing Genie Space, fetches their schemas from Unity Catalog, and calls the Databricks Foundation Model with ANZ country + financial services industry overlays.

**Show the output** (`envs/dev/generated/abac.auto.tfvars`):

- **Groups generated:** Bank_Teller, Relationship_Manager, Compliance_Officer, Marketing_Analyst, Branch_Manager
- **ANZ-specific tags:** `pii_level=masked_tfn`, `pii_level=masked_medicare`, `pii_level=masked_bsb`
- **Financial tags:** `pci_level=masked_card_last4`, `compliance_scope=aml_restricted`, `financial_sensitivity=rounded_amounts`
- **Masking functions:** `mask_tfn()` shows last 3 digits, `mask_medicare()` shows last 4, `mask_bsb()` partially masks BSB

**Show the masking SQL** (`envs/dev/generated/masking_functions.sql`):

```sql
-- ANZ-specific: Tax File Number masking
CREATE OR REPLACE FUNCTION mask_tfn(tfn STRING) ...
-- Shows: XXX XXX 789 (last 3 digits visible per ATO guidelines)

-- ANZ-specific: Medicare number masking
CREATE OR REPLACE FUNCTION mask_medicare(medicare STRING) ...
-- Shows: XXXX XXXX X (fully masked per My Health Records Act)

-- PCI-DSS: Credit card last 4
CREATE OR REPLACE FUNCTION mask_card_last4(card STRING) ...
-- Shows: **** **** **** 9010
```

**Key message:** _"The AI knows Australian regulations — TFN, Medicare, BSB masking are all generated automatically from the column names and the ANZ overlay. Meanwhile, the Genie Space config you already set up in the UI — instructions, sample questions, benchmarks, SQL expressions — is imported verbatim, not regenerated."_

### 2b. Review, tune & validate

The generated config is a draft, not a final answer. Review it before applying:

- Open `envs/dev/generated/abac.auto.tfvars` — check groups, tag policies, tag assignments, FGAC policies
- Open `envs/dev/generated/masking_functions.sql` — check the SQL UDF implementations
- See `envs/dev/generated/TUNING.md` for guidance on what to adjust

Once you're satisfied, validate the generated config:

```bash
make validate-generated ENV=dev COUNTRY=ANZ INDUSTRY=financial_services
```

This checks for:
- Tag assignments referencing undefined tag policies
- FGAC policies referencing missing masking functions
- Masking function SQL syntax issues
- Missing or inconsistent group references

Fix any issues the validator flags, then proceed to apply.

---

## Part 3: Apply Governance (5 min)

```bash
make apply ENV=dev
```

This deploys everything in one command:
1. Creates 5 groups in the Databricks account
2. Creates tag policies (pii_level, pci_level, financial_sensitivity, compliance_scope)
3. Assigns tags to sensitive columns
4. Deploys masking SQL functions
5. Creates FGAC policies (column masks + row filters)
6. Updates Genie Space with per-group ACLs and consumer entitlements

### Show the "after" state

Open the Genie Space and query as different groups:

| Data | Bank Teller | Compliance Officer | Marketing Analyst |
|------|-------------|-------------------|-------------------|
| TFN | `XXX XXX 789` | `123 456 789` | `[REDACTED]` |
| Medicare | `XXXX XXXX X` | `2123 45670 1` | `[REDACTED]` |
| Card number | `**** **** **** 9010` | `4000 1234 5678 9010` | Not visible |
| Transaction amount | `$15,000` → `$15,000` | `$15,000.00` | `$15,000` |
| AML risk flag | Not visible | `HIGH_RISK` | Not visible |
| BSB | `062-***` | `062-000` | `[REDACTED]` |

**Key message:** _"Same Genie Space, same tables, but every group sees exactly what they should. The compliance officer investigates AML-flagged transactions, the teller serves customers with masked PII, and marketing only sees aggregated anonymized data."_

---

## Part 4: Promote to Production (3 min)

```bash
# Promote dev config to prod with catalog remapping
make promote SOURCE_ENV=dev DEST_ENV=prod \
    DEST_CATALOG_MAP="dev_bank=prod_bank"

# Deploy to production (creates a NEW Genie Space in prod workspace)
# Auth credentials for prod are already configured by setup_demo.py
make apply ENV=prod
```

**What happens:**
- All governance config is remapped: `dev_bank.retail.*` → `prod_bank.retail.*`
- A new "Kookaburra Bank Analytics" Genie Space is created in the prod workspace
- Same groups, same masking, same ACLs — fully governed from day one

**Key message:** _"One command to replicate governance to production. No manual configuration, no risk of missing a masking rule."_

---

## Cleanup

```bash
# Remove Terraform-managed resources (same for both clouds)
make destroy ENV=prod
make destroy ENV=dev
```

#### AWS

```bash
python ../shared/examples/aus_bank_demo/setup_demo.py teardown \
    --env-file ../shared/scripts/account-admin.aws.env
```

#### Azure

```bash
CLOUD_PROVIDER=azure CLOUD_ROOT=$(pwd) \
python ../shared/examples/aus_bank_demo/setup_demo.py teardown \
    --env-file ../shared/scripts/account-admin.azure.env
```

---

## Talking Points

### For compliance / risk audiences
- _"GenieRails ensures every Genie Space has governance from day one — not as an afterthought"_
- _"ANZ country overlay automatically identifies TFN, Medicare, and BSB columns — no manual classification needed"_
- _"AML-flagged transactions are row-filtered to the compliance team only"_

### For data platform teams
- _"Everything is Terraform — version controlled, auditable, reproducible"_
- _"Dev → prod promotion in one command with catalog remapping"_
- _"No vendor lock-in — after generation, it's standard Databricks resources"_

### For executive sponsors
- _"Time to governed Genie Space: 15 minutes, not 15 days"_
- _"Consistent governance across all Genie Spaces — same groups, same policies, same masking"_
- _"Scales to hundreds of Genie Spaces with the same pattern"_
