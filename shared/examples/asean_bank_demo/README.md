# ASEAN Bank Demo — End-to-End

An end-to-end demo of GenieRails for a regional ASEAN bank headquartered in Singapore. Start with an existing Genie Space, import it into code, generate ABAC governance with Southeast Asia + financial services overlays, deploy, and promote to production.

**What you'll show:**
- Multi-country masking: NRIC (SG), MyKad (MY), Thai National ID, NIK (ID), PhilSys (PH), CCCD (VN)
- Embedded demographic data protection: MyKad and NIK encode date of birth — aggressive masking prevents inference
- Financial services governance: PCI-DSS card masking, AML risk row filters, transaction amount rounding
- 6 different data protection laws in one overlay: PDPA (SG), PDPA (MY), PDPA (TH), PDP Law (ID), DPA (PH), PDPD (VN)
- Role-based access: 5 groups with different views of the same multi-country data
- Dev → prod promotion with catalog remapping across workspaces

**Time:** ~20 minutes (5 min setup, 15 min demo)

**Prerequisites:** [Prerequisites](../../docs/prerequisites.md) — Python 3, Terraform, account admin credentials. Azure users: also see [Azure Prerequisites](../../../azure/docs/azure-prerequisites.md).

---

## Setup (~5 min)

The setup script provisions two isolated workspaces and creates sample ASEAN banking data with customers from all 6 countries. It works on both AWS and Azure.

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
- `DATABRICKS_AWS_REGION` — AWS region (default: `ap-southeast-1`)
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
- `AZURE_REGION` — Azure region matching your workspace (e.g., `southeastasia`, `eastus2`)
- `AZURE_TENANT_ID` — your Azure AD tenant ID
- `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` — Azure AD App Registration credentials

The Azure SP needs **Contributor** and **Storage Blob Data Contributor** RBAC roles on the resource group. See [Azure Prerequisites](../../../azure/docs/azure-prerequisites.md) for details.

### 2. Provision the demo

#### AWS

```bash
python ../shared/examples/asean_bank_demo/setup_demo.py provision \
    --env-file ../shared/scripts/account-admin.aws.env
```

#### Azure

```bash
CLOUD_PROVIDER=azure CLOUD_ROOT=$(pwd) \
python ../shared/examples/asean_bank_demo/setup_demo.py provision \
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

**Before the demo:** Open the dev workspace and show the Genie Space in the UI. Point out that anyone can see raw NRIC numbers, full MyKad (which encodes date of birth!), and AML risk flags — no governance at all.

---

## Part 1: The Challenge (2 min)

Open the Genie Space "ASEAN Regional Banking Analytics" in the dev workspace UI. Show the four tables — customers, accounts, transactions, credit cards.

**Set the scene:**

> _"Our regional bank operates across 6 ASEAN countries with a consolidated customer database. Each customer has a country-specific national ID — NRIC for Singapore, MyKad for Malaysia, Thai National ID, NIK for Indonesia, PhilSys for the Philippines, and CCCD for Vietnam. The tables are ready, but we can't onboard business users yet:"_

Point out what's in the tables:
- **NRIC (Singapore)** — protected under PDPA 2012, fines up to SGD 1M
- **MyKad (Malaysia)** — encodes date of birth in the number itself (YYMMDD format)
- **Thai National ID** — 13-digit citizen ID, protected under Thailand PDPA B.E. 2562
- **NIK (Indonesia)** — encodes date of birth AND district — protected under PDP Law No. 27/2022
- **PhilSys (Philippines)** — protected under Data Privacy Act (RA 10173)
- **CCCD (Vietnam)** — protected under Decree 13/2023/ND-CP
- **Full credit card PANs and CVVs** — PCI-DSS regulated
- **AML risk flags** (`HIGH_RISK`, `BLOCKED`) — restricted to compliance team
- **Cross-border transactions** — ASEAN remittance corridors

> _"6 countries, 6 different data protection laws, 6 different national ID formats. Setting this up manually would be a compliance nightmare. Let's do it in 10 minutes."_

---

## Part 2: Import & Generate (5 min)

### 2a. Generate ABAC governance

The setup script already configured `envs/dev/` with auth credentials, tables, and the Genie Space ID.

```bash
make generate ENV=dev COUNTRY=SEA INDUSTRY=financial_services
```

**What happens:** GenieRails discovers the 4 tables from the existing Genie Space, fetches their schemas from Unity Catalog, and calls the Databricks Foundation Model with SEA country + financial services industry overlays.

**Show the output** (`envs/dev/generated/abac.auto.tfvars`):

- **Groups generated:** Bank_Teller, Relationship_Manager, Compliance_Officer, Marketing_Analyst, Branch_Manager
- **SEA-specific tags:** `pii_level=masked_nric`, `pii_level=masked_mykad`, `pii_level=masked_thai_id`, `pii_level=masked_nik`, `pii_level=masked_philsys`, `pii_level=masked_cccd`
- **Financial tags:** `pci_level=masked_card_last4`, `compliance_scope=aml_restricted`
- **Masking functions:** 16 SEA functions + 6 financial services functions generated automatically

**Show the masking SQL** (`envs/dev/generated/masking_functions.sql`):

```sql
-- Singapore: NRIC masking (PDPA 2012)
CREATE OR REPLACE FUNCTION mask_nric(nric STRING) ...
-- Shows: S****567D (first letter + last 4 visible)

-- Malaysia: MyKad masking (PDPA 2010)
CREATE OR REPLACE FUNCTION mask_mykad(mykad STRING) ...
-- Shows: ********5123 (last 4 ONLY — aggressive masking due to embedded DOB)

-- Indonesia: NIK masking (PDP Law 27/2022)
CREATE OR REPLACE FUNCTION mask_nik(nik STRING) ...
-- Shows: ************0001 (last 4 ONLY — aggressive masking due to embedded DOB + district)

-- Vietnam: CCCD masking (Decree 13/2023)
CREATE OR REPLACE FUNCTION mask_cccd(cccd STRING) ...
-- Shows: ********2345 (last 4 visible)

-- PCI-DSS: Credit card last 4
CREATE OR REPLACE FUNCTION mask_card_last4(card STRING) ...
-- Shows: **** **** **** 9010
```

**Key message:** _"The AI knows all 6 ASEAN data protection laws. MyKad and NIK get aggressive masking (last 4 digits only) because they encode date of birth in their structure — showing more digits would leak demographic data. Singapore NRIC shows the first letter and last 4, which is the standard partial masking format. 16 country-specific masking functions, plus 6 from the financial services overlay — all generated in one command."_

### 2b. Review, tune & validate

The generated config is a draft, not a final answer. Review it before applying:

- Open `envs/dev/generated/abac.auto.tfvars` — check groups, tag policies, tag assignments, FGAC policies
- Open `envs/dev/generated/masking_functions.sql` — check the SQL UDF implementations
- See `envs/dev/generated/TUNING.md` for guidance on what to adjust

Once you're satisfied, validate the generated config:

```bash
make validate-generated ENV=dev COUNTRY=SEA INDUSTRY=financial_services
```

This checks for:
- Tag assignments referencing undefined tag policies
- FGAC policies referencing missing masking functions
- Masking function SQL syntax issues
- Missing or inconsistent group references
- Cross-country conflicts (e.g., NRIC disambiguation between Singapore and Malaysia formats)

Fix any issues the validator flags, then proceed to apply.

---

## Part 3: Apply Governance (5 min)

```bash
make apply ENV=dev
```

This deploys everything in one command:
1. Creates 5 groups in the Databricks account
2. Creates tag policies (pii_level, pci_level, financial_sensitivity, compliance_scope)
3. Assigns tags to sensitive columns across all 6 national ID types
4. Deploys masking SQL functions
5. Creates FGAC policies (column masks + row filters)
6. Updates Genie Space with per-group ACLs and consumer entitlements

### Show the "after" state

Open the Genie Space and query as different groups:

| Data | Bank Teller | Compliance Officer | Marketing Analyst |
|------|-------------|-------------------|-------------------|
| NRIC (SG) | `S****567D` | `S8712345D` | `[REDACTED]` |
| MyKad (MY) | `********5123` | `850615085123` | `[REDACTED]` |
| Thai ID | `*********5678` | `1100112345678` | `[REDACTED]` |
| NIK (ID) | `************0001` | `3201151290870001` | `[REDACTED]` |
| PhilSys (PH) | `********9012` | `123456789012` | `[REDACTED]` |
| CCCD (VN) | `********2345` | `001085012345` | `[REDACTED]` |
| Card number | `**** **** **** 9010` | `4000 1234 5678 9010` | Not visible |
| AML risk flag | Not visible | `HIGH_RISK` | Not visible |

**Key message:** _"Same Genie Space, same tables, 6 countries — but every group sees exactly what they should. The compliance officer sees full national IDs to investigate AML flags, the teller sees partially masked IDs for customer service, and marketing only sees aggregated anonymized data. MyKad and NIK are masked more aggressively than NRIC because they encode date of birth."_

---

## Part 4: Promote to Production (3 min)

```bash
# Promote dev config to prod with catalog remapping
make promote SOURCE_ENV=dev DEST_ENV=prod \
    DEST_CATALOG_MAP="dev_asean_bank=prod_asean_bank"

# Deploy to production (creates a NEW Genie Space in prod workspace)
# Auth credentials for prod are already configured by setup_demo.py
make apply ENV=prod
```

**What happens:**
- All governance config is remapped: `dev_asean_bank.retail.*` → `prod_asean_bank.retail.*`
- A new "ASEAN Regional Banking Analytics" Genie Space is created in the prod workspace
- Same groups, same masking, same ACLs — fully governed from day one

**Key message:** _"One command to replicate governance to production. 6 countries, 16 masking functions, 22 FGAC policies — all promoted in seconds."_

---

## Cleanup

```bash
# Remove Terraform-managed resources (same for both clouds)
make destroy ENV=prod
make destroy ENV=dev
```

#### AWS

```bash
python ../shared/examples/asean_bank_demo/setup_demo.py teardown \
    --env-file ../shared/scripts/account-admin.aws.env
```

#### Azure

```bash
CLOUD_PROVIDER=azure CLOUD_ROOT=$(pwd) \
python ../shared/examples/asean_bank_demo/setup_demo.py teardown \
    --env-file ../shared/scripts/account-admin.azure.env
```

---

## Talking Points

### For compliance / risk audiences
- _"GenieRails ensures every Genie Space has governance from day one — not as an afterthought"_
- _"SEA overlay covers 6 data protection laws: PDPA (SG/MY/TH), PDP Law (ID), DPA (PH), PDPD (VN)"_
- _"MyKad and NIK embed date of birth in their structure — aggressive masking (last 4 only) prevents demographic inference"_
- _"Indonesia's PDP Law (Oct 2024 enforcement, 2% annual revenue penalties) and Thailand's PDPA (THB 5M penalties) — both active"_
- _"AML-flagged cross-border transactions are row-filtered to the compliance team only"_

### For multi-country / regional audiences
- _"One command governs 6 countries — 24 identifiers, 16 masking functions, 6 PDPAs"_
- _"NRIC disambiguation between Singapore (9-char alphanumeric) and Malaysia (12-digit numeric) is handled automatically"_
- _"Cross-border remittance corridors (SG→MY, TH→VN, ID→SG) are visible in transaction data — compliance can monitor all corridors"_
- _"Each country's national ID gets the masking format prescribed by its own PDPA — not a one-size-fits-all approach"_

### For data platform teams
- _"Everything is Terraform — version controlled, auditable, reproducible"_
- _"Dev → prod promotion in one command with catalog remapping"_
- _"No vendor lock-in — after generation, it's standard Databricks resources"_
- _"Overlays compose: `COUNTRY=SEA,ANZ,IN INDUSTRY=financial_services` — pan-APJ in one command"_

### For executive sponsors
- _"Time to governed Genie Space: 15 minutes, not 15 days"_
- _"Consistent governance across all 6 countries — same groups, same policies, country-appropriate masking"_
- _"Scales to hundreds of Genie Spaces with the same pattern"_
- _"One regional bank, one overlay, complete ASEAN compliance"_
