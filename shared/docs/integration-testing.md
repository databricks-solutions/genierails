# Integration Testing

This guide covers the automated integration test scenarios that validate the
playbooks documented in `docs/playbook.md`, the `setup_test_data.py` helper, and the
`run_integration_tests.py` test runner.

---

## Recommended Testing Workflow

### One command for CI/CD

For automated pipelines (PR checks, nightly runs), use `make test-ci`:

```bash
make test-ci
```

This single command runs the full pipeline in order:

1. **Unit tests** — fast Python-only checks, no cloud resources needed
2. **Provision** — fresh isolated Databricks workspace + metastore (~10–15 min)
3. **Integration tests** — all scenarios (~90 min)
4. **Teardown** — always runs, even if tests fail, so no cloud resources are left behind

Exit code is non-zero if any phase fails. Teardown is **always** executed.

Options:

```bash
# Use a custom credentials file (useful for CI secrets injection)
make test-ci ACCOUNT_ADMIN_ENV=/path/to/credentials.env

# Run only one scenario (faster iteration)
make test-ci SCENARIO=quickstart

# Pin a specific SQL warehouse
make test-ci WAREHOUSE_ID=abc123
```

**GitHub Actions example:**

```yaml
- name: Integration tests
  run: make test-ci
  env:
    # Write the credentials file from a secret, then pass the path
    ACCOUNT_ADMIN_ENV: ${{ runner.temp }}/account-admin.env
```

### Manual step-by-step (for local development)

Use unit tests first to catch logic bugs quickly (< 1 second, no credentials
required), then provision a **fresh isolated environment** for integration tests
to avoid stale quota counter issues in a shared metastore.

```
make test-unit                              # fast — pure Python, no LLM/Terraform/Databricks
       ↓ (all pass)
python scripts/provision_test_env.py provision   # ~10-15 min — creates fresh workspace + metastore
       ↓
python scripts/run_integration_tests.py          # slow — deploys real resources (~hours)
       ↓
python scripts/provision_test_env.py teardown    # wipe the environment when done
```

### Why provision a fresh environment?

Databricks metastore-wide FGAC policy quotas use an **eventually consistent
counter** that can lag behind actual policy deletions by several minutes.
In a long-lived shared metastore, the counter can accumulate drift and
incorrectly block new policy creation even when no policies actually exist.
Provisioning a fresh workspace + metastore for each test run gives a clean
counter that always starts at zero.

### Unit Tests

The `tests/` directory contains pytest-based unit tests for the core Python
functions — all autofix functions in `generate_abac.py` and all validation
functions in `validate_abac.py`.

**Run:**

```bash
# Install deps once (if not already installed)
pip install pytest python-hcl2

# Run all 60+ unit tests (~1 second, no Databricks connection needed)
make test-unit

# Or invoke pytest directly for richer output
python3 -m pytest tests/ -v
python3 -m pytest tests/test_generate_abac.py -v   # autofix functions only
python3 -m pytest tests/test_validate_abac.py -v   # validation functions only
python3 -m pytest tests/ -k "TagPolicies" -v        # filter by name
```

**What is tested:**

| Test file | Functions covered |
|---|---|
| `tests/test_generate_abac.py` | `fix_hcl_syntax`, `autofix_tag_policies`, `autofix_invalid_tag_values`, `autofix_undefined_tag_refs`, `autofix_missing_fgac_policies`, `autofix_fgac_policy_count` |
| `tests/test_validate_abac.py` | `validate_groups`, `validate_tag_policies`, `validate_tag_assignments`, `validate_fgac_policies`, `parse_sql_functions`, `parse_sql_function_arg_counts`, `_condition_matches_tags` |
| `tests/test_schema_drift.py` | PII column pattern regex, env file parsing (both `uc_tables` and `genie_spaces` shapes), governed-key resolution (4-level fallback), delta merge/dedup, delta validation (reject unknown keys/values), stale assignment removal |

Unit tests catch the most common failure categories without incurring the
cost of a full LLM + Terraform run:

- LLM output contains missing commas between HCL objects → `fix_hcl_syntax`
- LLM uses a tag value not in the allowed list → `autofix_tag_policies`
- LLM generates an assignment with a typo'd value → `autofix_invalid_tag_values`
- LLM references a tag key that was never defined → `autofix_undefined_tag_refs`
- An uncovered sensitive column is left without an FGAC policy → `autofix_missing_fgac_policies`
- Too many FGAC policies for one catalog → `autofix_fgac_policy_count`

---

## Provisioning a Fresh Integration-Test Environment

`scripts/provision_test_env.py` creates a brand-new serverless Databricks
workspace and Unity Catalog metastore specifically for integration testing, then
writes all `auth.auto.tfvars` files so the test runner uses that environment.

### One-time setup

Run `make setup` from the `aws/` or `azure/` directory — it automatically copies the matching example file to `scripts/account-admin.<cloud>.env` if it does not yet exist. Then fill in your credentials:

```bash
# AWS
vi scripts/account-admin.aws.env

# Azure
vi scripts/account-admin.azure.env
```

The file has two sections — shared Databricks credentials and cloud-specific credentials.

#### Section 1 — Databricks credentials (both clouds)

| Key | Where to find it |
|---|---|
| `DATABRICKS_ACCOUNT_ID` | Account Console → top-right menu → Account ID |
| `DATABRICKS_CLIENT_ID` | Account Console → User Management → Service Principals → `<SP>` → Application ID |
| `DATABRICKS_CLIENT_SECRET` | Same SP → OAuth Secrets → Generate Secret |

> **Note:** The Account Console URL differs by cloud:
> - AWS: `https://accounts.cloud.databricks.com`
> - Azure: `https://accounts.azuredatabricks.net`

#### Section 2 — AWS credentials (`account-admin.aws.env` only)

| Key | Where to find it |
|---|---|
| `DATABRICKS_AWS_REGION` | AWS region for the new workspace (e.g. `ap-southeast-2`) |
| `AWS_ACCESS_KEY_ID` | AWS credentials with IAM + S3 write permissions (see below) |
| `AWS_SECRET_ACCESS_KEY` | Same IAM user or role |
| `AWS_SESSION_TOKEN` | Only needed for temporary STS credentials (see recommendation below) |

**AWS credential type recommendation:**

> **Use long-lived IAM user credentials** (`AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`, no `AWS_SESSION_TOKEN`) whenever possible. Temporary STS tokens expire after 1–12 hours, which can cause teardown to fail if the full test run (~90 min + review time) outlasts the token lifetime.

| Credential type | `AWS_SESSION_TOKEN` required | Expires | Recommended for |
|---|---|---|---|
| IAM user access keys | No | Never | **Local dev and CI/CD** |
| AWS SSO / `aws sso login` | Yes (auto-set by CLI) | 1–8 h | Interactive use only |
| STS `AssumeRole` | Yes | 15 min – 12 h | Short-lived pipelines |

**Required IAM permissions:**

`iam:CreateRole`, `iam:DeleteRole`, `iam:PutRolePolicy`, `iam:DeleteRolePolicy`,
`iam:ListRolePolicies`, `iam:ListAttachedRolePolicies`, `iam:DetachRolePolicy`,
`iam:UpdateAssumeRolePolicy`, `sts:GetCallerIdentity`

**Required S3 permissions:**

`s3:CreateBucket`, `s3:DeleteBucket`, `s3:PutPublicAccessBlock`,
`s3:ListBucketVersions`, `s3:DeleteObject`, `s3:DeleteObjectVersion`

**Auto-created AWS resources:**

The provision script auto-creates an S3 bucket named `genie-uc-test-<aws-account-id>` in the configured region. The bucket is reused across test runs and only deleted on teardown if the script created it.

| Step | What the script creates |
|---|---|
| `provision` | A unique **S3 prefix** inside the bucket: `s3://<bucket>/genie-test-<run-id>/` |
| `provision` | An **AWS IAM role** (`genie-test-uc-role-<run-id>`) scoped to that prefix |
| `provision` | A Databricks **storage credential** backed by the IAM role |
| `provision` | A Databricks **External Location** covering `s3://<bucket>/genie-test-<run-id>/` |
| Integration tests | Each catalog gets its own subfolder: `.../genie-test-<run-id>/<catalog-name>/` |
| `teardown` | Deletes the IAM role; the metastore deletion cascades to catalogs/schemas/policies |

S3 objects written during the test are **not deleted by teardown** — the metastore and workspace are destroyed at the Databricks layer, but the underlying S3 prefixes remain. They are cheap (a few MB of small Delta files) and isolated by `run-id`. Clean them up periodically with:

```bash
aws s3 rm s3://<your-bucket>/ --recursive --exclude "*" --include "genie-test-*"
```

---

#### Section 2 — Azure credentials (`account-admin.azure.env` only)

| Key | Where to find it |
|---|---|
| `AZURE_SUBSCRIPTION_ID` | Azure Portal → Subscriptions → Subscription ID |
| `AZURE_RESOURCE_GROUP` | Azure Portal → Resource Groups → name (must already exist) |
| `AZURE_REGION` | Must match the workspace region (e.g. `eastus2`, `australiaeast`, `westeurope`) |
| `AZURE_TENANT_ID` | Azure Portal → Microsoft Entra ID → Overview → Tenant ID |
| `AZURE_CLIENT_ID` | Azure Portal → Microsoft Entra ID → App registrations → Application (client) ID |
| `AZURE_CLIENT_SECRET` | Same App registration → Certificates & secrets → New client secret |

> **Note:** Azure client secrets expire (default 6 months or 2 years). If teardown fails with an auth error, generate a new secret and re-run.

**Required Azure RBAC roles on the resource group:**

| Role | Why it's needed |
|---|---|
| `Contributor` | Create/delete storage accounts, access connectors |
| `Storage Blob Data Contributor` | Manage blob data in ADLS Gen2 containers |
| `User Access Administrator` (optional) | Assign managed-identity roles; if absent, the script falls back to your local `az login` for role assignments |

Assign roles with:

```bash
az role assignment create \
  --assignee <AZURE_CLIENT_ID> \
  --role "Contributor" \
  --scope "/subscriptions/<SUBSCRIPTION_ID>/resourceGroups/<RESOURCE_GROUP>"

az role assignment create \
  --assignee <AZURE_CLIENT_ID> \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/<SUBSCRIPTION_ID>/resourceGroups/<RESOURCE_GROUP>"
```

**Auto-created Azure resources:**

| Step | What the script creates |
|---|---|
| `provision` | An **ADLS Gen2 storage account** (`genietest<run-id>`) with a blob container |
| `provision` | A **Databricks Access Connector** with a managed identity |
| `provision` | A Databricks **storage credential** backed by the Access Connector |
| `provision` | A Databricks **External Location** pointing to the ADLS container |
| `teardown` | Deletes all Azure resources (storage account, access connector, role assignments) |

### Provision

```bash
python scripts/provision_test_env.py provision
```

This will:

1. Look up the SP's SCIM identity in the Databricks account.
2. Create a **serverless workspace** (`genie-test-<id>`).
3. Create a fresh **Unity Catalog metastore** with a unique storage path.
4. Assign the metastore to the workspace.
5. Create cloud-specific storage infrastructure:
   - **AWS:** An IAM role scoped to the test S3 prefix, registered as a storage credential
   - **Azure:** An ADLS Gen2 storage account + Access Connector with managed identity, registered as a storage credential
6. Create an **External Location** so catalogs can be created without a metastore root.
7. Set the SP as **metastore admin** and **workspace admin**.
8. Write `auth.auto.tfvars` for all env directories (`dev`, `bu2`, `prod`, `account`).
9. Save a state file (`scripts/.test_env_state.json`) for teardown.

Workspace provisioning typically takes **10–15 minutes**.

### Run tests

```bash
# Check what environment is provisioned
python scripts/provision_test_env.py status

# Run all scenarios (warehouse is auto-detected from the workspace)
python scripts/run_integration_tests.py

# Run a specific scenario
python scripts/run_integration_tests.py --scenario quickstart
```

### Tear down

```bash
python scripts/provision_test_env.py teardown
```

This deletes cloud-specific resources, the workspace, metastore (and all catalogs/schemas/policies inside it), admin group, and removes the generated `auth.auto.tfvars` files.

- **AWS:** Deletes the IAM role created during provisioning. S3 objects remain (see cleanup note above).
- **Azure:** Deletes the storage account, access connector, and any role assignments.

> **If teardown fails with an auth error:**
>
> *AWS — "ExpiredToken":* Your STS session token expired during the test run. Export fresh credentials and re-run:
>
> ```bash
> export AWS_ACCESS_KEY_ID=...
> export AWS_SECRET_ACCESS_KEY=...
> export AWS_SESSION_TOKEN=...   # omit if using long-lived keys
> python scripts/provision_test_env.py teardown
> ```
>
> *Azure — "ClientSecretExpired" or "InvalidAuthenticationToken":* Your Azure client secret has expired. Generate a new secret in the Azure Portal (Microsoft Entra ID → App registrations → Certificates & secrets), update `account-admin.azure.env`, and re-run teardown.
>
> The Databricks workspace and metastore are always deleted by teardown regardless of whether
> the cloud resource cleanup succeeds.

### Options

| Flag | Description |
|---|---|
| `--env-file PATH` | Path to credentials file (default: `scripts/account-admin.<cloud>.env`) |
| `--dry-run` | Print what would happen without creating/deleting anything |
| `--force` | With `provision`: overwrite an existing provisioned environment |

---

## Scenarios

`scripts/run_integration_tests.py` runs each playbook.md scenario end-to-end with
full data setup, LLM generation, Terraform apply, assertions, and teardown. Each
scenario is isolated — state from a previous run is destroyed and cleaned before
the next one starts.

| Scenario | playbook.md section | What it validates |
|---|---|---|
| **quickstart** | § 1 | Single Genie Space backed by a single UC catalog (`dev_fin`) |
| **multi-catalog** | § 1 (multi-catalog) | One Genie Space drawing tables from two catalogs (`dev_fin` + `dev_clinical`) |
| **multi-space** | § 1 (multi-space) | Two Genie Spaces with separate catalogs — Finance Analytics + Clinical Analytics |
| **per-space** | § 4 | Add Clinical Analytics incrementally without touching Finance Analytics (isolation guarantee) |
| **promote** | § 5 | Full dev → prod promotion with catalog remapping across both spaces |
| **multi-env** | § 6 | Two independent envs on the same account: `dev` (Finance), `bu2` (Clinical) |
| **attach-promote** | § 3 | Import a Genie Space already configured in the UI — govern it, then promote to prod |
| **self-service-genie** | § 7 | Central governance team + two BU Genie teams self-serve; second BU isolation check; BU promote to prod via `apply-genie`; governance state verified unchanged throughout |
| **abac-only** | § 2 | ABAC governance only (no Genie Space) + §2→§4 upgrade path: add Genie Space later without disturbing governance |
| **multi-space-import** | § 3 (multi-space) | Import two UI-configured Genie Spaces in one `make generate`; assert both configs present, Terraform creates no new spaces |
| **schema-drift** | — | Detects and classifies new columns after initial ABAC deployment; tests `make audit-schema` and `make generate-delta` across ADD/DROP/RENAME COLUMN scenarios |
| **genie-only** | § 7 (genie\_only) | Minimal-privilege SP (workspace USER + SQL entitlement) creates Genie Space with `genie_only=true`; no account-level resources |
| **genie-import-no-abac** | § 3 + § 7 | Import an existing Genie Space and deploy to prod **without any ABAC governance** — validates the genie-only import-to-prod workflow when a separate team manages ABAC centrally |
| **country-overlay** | — | Country/region overlays (ANZ, IN, SEA) — full cycle per region + multi-region generation |
| **industry-overlay** | — | Industry overlays (financial\_services, healthcare, retail) — full cycle per industry + multi-industry + country+industry composition (COUNTRY=ANZ INDUSTRY=healthcare) |

---

## Test Catalogs

`setup_test_data.py` creates the following Unity Catalog resources:

### Dev

| Catalog | Schema | Table | Rows | Sensitive data |
|---|---|---|---|---|
| `dev_fin` | `finance` | `customers` | 10 | SSN, DOB, email, phone (PII) |
| `dev_fin` | `finance` | `transactions` | 15 | AML flag, risk score (AML) |
| `dev_fin` | `finance` | `credit_cards` | 10 | card number, CVV (PCI) |
| `dev_clinical` | `clinical` | `patients` | 10 | SSN, DOB, insurance ID (PHI) |
| `dev_clinical` | `clinical` | `encounters` | 12 | diagnosis, treatment notes (PHI) |

### Prod (mirror schema, different data)

| Catalog | Schema | Table | Rows |
|---|---|---|---|
| `prod_fin` | `finance` | `customers` | 10 |
| `prod_fin` | `finance` | `transactions` | 15 |
| `prod_fin` | `finance` | `credit_cards` | 10 |
| `prod_clinical` | `clinical` | `patients` | 10 |
| `prod_clinical` | `clinical` | `encounters` | 12 |

---

## Quick Start

### Prerequisites

- `envs/dev/auth.auto.tfvars` configured with workspace credentials
- A SQL warehouse available in the workspace (or pass `WAREHOUSE_ID=<id>` to
  avoid cold-start delay)

### Run all scenarios

```bash
# Run all six scenarios sequentially (full teardown after each)
make test-all

# Keep data and Terraform resources after the run for inspection
make test-all KEEP_DATA=1

# Pin a warehouse to avoid cold-start delay
make test-all WAREHOUSE_ID=abc123ef
```

### Run a single scenario

```bash
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
make test-genie-only
make test-genie-import-no-abac
make test-country-overlay
make test-industry-overlay

# All targets accept WAREHOUSE_ID= and KEEP_DATA=1
make test-promote WAREHOUSE_ID=abc123ef KEEP_DATA=1
```

### Run directly with Python

```bash
# List available scenarios
python scripts/run_integration_tests.py --list

# Run all
python scripts/run_integration_tests.py

# Run one scenario
python scripts/run_integration_tests.py --scenario quickstart
python scripts/run_integration_tests.py --scenario per-space --keep-data

# Pin a warehouse
python scripts/run_integration_tests.py --warehouse-id abc123ef

# Non-default auth file
python scripts/run_integration_tests.py --auth-file envs/dev/auth.auto.tfvars
```

---

## Scenario Details

### 1. quickstart — Single space, single catalog

Validates the core quickstart from docs/playbook.md § 1 with a single Genie
Space backed by `dev_fin`.

**Steps:**

| Step | Action |
|---|---|
| 1 | Create `dev_fin` test catalogs and sample data |
| 2 | Configure `dev` env: one space "Finance Analytics" with `dev_fin.*` tables |
| 3 | `make generate ENV=dev` — LLM generates ABAC config and masking functions |
| 4 | Assert `generated/abac.auto.tfvars` and `generated/spaces/finance_analytics/` created |
| 5 | `make apply ENV=dev` — deploys account, data_access, workspace layers |
| 6 | Assert `.genie_space_id_finance_analytics` file exists |
| 7 | `setup_test_data.py --verify` — row counts, column tags, column masks |
| 8 | Teardown data + Terraform resources |

**Key assertions:**
- `generated/abac.auto.tfvars` contains `Finance Analytics` genie_space_configs entry
- `generated/spaces/finance_analytics/abac.auto.tfvars` exists (per-space dir bootstrapped)
- `.genie_space_id_*` file created after apply
- Row counts ≥ expected for all `dev_fin` tables
- Column tags and masking policies applied

---

### 2. multi-catalog — One space spanning two catalogs

Validates the "single space spanning multiple catalogs" pattern from playbook.md § 1.
One space ("Combined Analytics") draws tables from both `dev_fin` and `dev_clinical`.

**Key assertions:**
- `generated/abac.auto.tfvars` contains `Combined Analytics` and references both `dev_fin` and `dev_clinical`
- Only one Genie Space deployed
- Column tags applied across both catalogs

---

### 3. multi-space — Two spaces, separate catalogs

Validates the two-space multi-catalog flow from playbook.md § 1. Finance Analytics
uses `dev_fin`; Clinical Analytics uses `dev_clinical`. This is the core of the
original `make integration-test` flow.

**Key assertions:**
- `generated/abac.auto.tfvars` contains both `Finance Analytics` and `Clinical Analytics` entries
- `generated/spaces/finance_analytics/` and `generated/spaces/clinical_analytics/` both bootstrapped
- Two `.genie_space_id_*` files created
- Row counts and ABAC verified for both catalogs

---

### 4. per-space — Incremental space addition (isolation test)

Validates the per-space generation isolation guarantee from playbook.md § 4.

**Phase 1:** Deploy Finance Analytics only.

**Phase 2:** Add Clinical Analytics using `make generate SPACE="Clinical Analytics"` —
without triggering a full LLM re-run over Finance Analytics.

**Key assertions:**
- After full generate: `Finance Analytics` in assembled output, `Clinical Analytics` absent
- `generated/spaces/finance_analytics/abac.auto.tfvars` content is **byte-for-byte unchanged**
  after the per-space generate for Clinical Analytics
- Assembled `generated/abac.auto.tfvars` contains **both** spaces after merge
- `generated/spaces/clinical_analytics/abac.auto.tfvars` created by SPACE= generate
- Both Genie Spaces deployed after final apply

---

### 5. promote — dev → prod cross-env promotion

Validates the full dev → prod promotion from playbook.md § 5.

**Catalog mapping:** `dev_fin → prod_fin`, `dev_clinical → prod_clinical`

**Steps:**

| Step | Action |
|---|---|
| 1 | Create dev + prod test catalogs |
| 2 | Configure dev: two spaces (Finance + Clinical) |
| 3 | `make generate ENV=dev` + `make apply ENV=dev` |
| 4 | Verify dev data + ABAC |
| 5 | `make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP=dev_fin=prod_fin,dev_clinical=prod_clinical` |
| 6 | `make apply ENV=prod` |
| 7 | `setup_test_data.py --verify-prod` |
| 8 | Teardown both envs |

**Key assertions:**
- `envs/prod/env.auto.tfvars` written by promote with `prod_fin` catalog references
- `envs/prod/generated/abac.auto.tfvars` contains remapped prod catalog names
- Prod column tags and masking policies applied

---

### 6. multi-env — Two independent envs (BU scenario)

Validates the independent second environment from playbook.md § 6.

- `dev` env: Finance Analytics backed by `dev_fin`
- `bu2` env: Clinical Analytics backed by `dev_clinical`
- Both envs use the same Databricks workspace and account
- Each has its own `make generate` + `make apply` cycle with completely separate
  generated config and Terraform state

**Key assertions:**
- `dev/generated/abac.auto.tfvars` contains Finance Analytics, not Clinical Analytics
- `bu2/generated/abac.auto.tfvars` contains Clinical Analytics, not Finance Analytics
- `envs/dev/terraform.tfstate` and `envs/bu2/terraform.tfstate` exist and differ
- Finance Analytics Genie Space deployed in dev, Clinical Analytics deployed in bu2

---

### 7. attach-promote — Attach to UI-created space and promote to prod

Validates the "Import an existing Genie Space" flow from playbook.md § 3, combined
with a dev → prod promotion. This is the adoption story: a data team already built a
Genie Space in the Databricks UI and now wants to bring it under ABAC governance.

**Phase 1 — Simulate UI configuration:**

A Finance Analytics Genie Space is created directly via the Genie REST API
(`POST /api/2.0/genie/spaces`) with `dev_fin` tables. This represents the space a
data team built in the UI before this tool was adopted.

**Phase 2 — Attach with explicit `uc_tables`:**

`env.auto.tfvars` is configured with `genie_space_id` **and** explicit `uc_tables`. The
Genie API's `serialized_space` field is not immediately available for newly-created spaces
(async processing, can take several minutes), so the test provides the table list directly
rather than relying on auto-discovery. This simulates the playbook.md manual step: the user
runs `make generate` (which logs discovered tables), then copies them into
`data_access/env.auto.tfvars`.

`make generate` imports the space's config (instructions, benchmarks, sample questions)
verbatim from the Genie API response — not re-generated by the LLM. The ABAC governance
(groups, tag policies, masking functions) is generated fresh from the table DDLs.

**Phase 3 — Apply:**

`make apply` deploys ABAC governance (group ACLs, column tags, masking functions,
FGAC policies) **without** creating or deleting the Genie Space. Terraform operates
only on `existing_spaces` resources — no `genie_space_create` provisioner runs.

**Phase 4 — Promote:**

`make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP=dev_fin=prod_fin` followed
by `make apply ENV=prod` applies the same governance to prod.

**Steps:**

| Step | Action |
|---|---|
| 1 | Create `dev_fin` + `prod_fin` test catalogs |
| 2 | Create a Genie Space via `POST /api/2.0/genie/spaces` (simulating UI setup) |
| 3 | Configure `dev` env: `genie_space_id = "<id>"`, no `uc_tables` |
| 4 | `make generate ENV=dev` — discovers tables from Genie API, generates ABAC |
| 5 | Assert generated config references `dev_fin` catalog and `Finance Analytics` |
| 6 | Update `env.auto.tfvars` with discovered `uc_tables` (simulating playbook.md manual step) |
| 7 | `make apply ENV=dev` — applies ABAC; no new space created |
| 8 | Assert **no** `.genie_space_id_*` file was created (space not created by Terraform) |
| 9 | `setup_test_data.py --verify` — row counts, column tags, masks |
| 10 | `make promote ... DEST_CATALOG_MAP=dev_fin=prod_fin` |
| 11 | `make apply ENV=prod` |
| 12 | `setup_test_data.py --verify-prod` |
| 13 | Delete the UI-created space via `DELETE /api/2.0/genie/spaces/{id}` (teardown) |

**Key assertions:**
- `generated/abac.auto.tfvars` contains `dev_fin` catalog references (tables discovered from API)
- `generated/abac.auto.tfvars` contains `Finance Analytics` `genie_space_configs` entry
- **No** `.genie_space_id_*` file exists after `make apply` — Terraform did not create a new space
- `envs/prod/generated/abac.auto.tfvars` contains `prod_fin` after promote
- Column tags and masking policies applied in both dev and prod

---

### 8. self-service-genie — Central governance + BU teams self-serve Genie

Validates the self-service Genie pattern from playbook.md § 7 and [self-service-genie.md](self-service-genie.md).

**Phase 1 — Governance team:**

A `governance` env is set up with both `dev_fin` + `dev_clinical` table references and **no** `genie_spaces` block. `make generate MODE=governance` is run — only ABAC content is generated. `make apply-governance` applies the account and data_access layers without touching the workspace layer.

**Phase 2 — BU Finance team:**

A `bu_fin` env is set up with a Finance Analytics space pointing at `dev_fin` tables. `make generate MODE=genie` is run — only `genie_space_configs` is generated (no ABAC, no masking SQL). `make apply-genie` applies only the workspace layer and creates the Genie Space.

**Phase 3 — Adding a second BU (isolation check):**

A `bu_clin` env is set up with a Clinical Analytics space. `make generate MODE=genie` + `make apply-genie` runs for `bu_clin`. The test then asserts that `governance/data_access/terraform.tfstate` is byte-for-byte unchanged — proving that adding a second BU team has zero effect on governance state.

**Phase 4 — BU Finance team promote to prod:**

`make promote SOURCE_ENV=bu_fin DEST_ENV=bu_fin_prod DEST_CATALOG_MAP=dev_fin=prod_fin` followed by `make apply-genie ENV=bu_fin_prod` (not `make apply`). Asserts `bu_fin_prod` has a `.genie_space_id_*` file but no `data_access/terraform.tfstate`, and that the governance state remains unmodified.

**Steps:**

| Step | Action |
|---|---|
| 1 | Create `dev_fin` + `dev_clinical` + `prod_fin` test catalogs |
| 2 | Configure `governance` env with both dev catalogs' tables (no `genie_spaces`) |
| 3 | `make generate ENV=governance MODE=governance` |
| 4 | Assert `tag_assignments` + `fgac_policies` present; `genie_space_configs` absent |
| 5 | `make apply-governance ENV=governance` |
| 6 | Assert `data_access/terraform.tfstate` exists; no `.genie_space_id_*` file |
| 7 | Configure `bu_fin` env with Finance Analytics space |
| 8 | `make generate ENV=bu_fin MODE=genie` |
| 9 | Assert `genie_space_configs` present; `tag_assignments` + `fgac_policies` absent; no `masking_functions.sql` |
| 10 | `make apply-genie ENV=bu_fin` |
| 11 | Assert `.genie_space_id_finance_analytics` file exists; no `data_access/terraform.tfstate` |
| 12 | **Snapshot** `governance/data_access/terraform.tfstate` content |
| 13 | Configure `bu_clin` env with Clinical Analytics space; `make generate MODE=genie` + `make apply-genie` |
| 14 | Assert `bu_clin` has `.genie_space_id_clinical_analytics`; governance state byte-for-byte unchanged |
| 15 | `make promote SOURCE_ENV=bu_fin DEST_ENV=bu_fin_prod DEST_CATALOG_MAP=dev_fin=prod_fin` |
| 16 | `make apply-genie ENV=bu_fin_prod` |
| 17 | Assert `bu_fin_prod` has `.genie_space_id_*`; no `data_access/terraform.tfstate`; governance state still unchanged |
| 18 | Teardown all envs |

**Key assertions:**
- `governance/generated/abac.auto.tfvars` contains `tag_assignments` and `fgac_policies`
- `governance/generated/abac.auto.tfvars` does NOT contain `genie_space_configs`
- `governance/generated/masking_functions.sql` exists
- `bu_fin/generated/abac.auto.tfvars` contains `genie_space_configs`
- `bu_fin/generated/abac.auto.tfvars` does NOT contain `tag_assignments` or `fgac_policies`
- `bu_fin/generated/masking_functions.sql` does NOT exist (governance team owns it)
- Cross-layer state isolation: governance has data_access state; BU envs have workspace state only
- `governance/data_access/terraform.tfstate` byte-for-byte unchanged after second BU + BU prod promote

---

### 9. abac-only — ABAC governance only (no Genie Space) + upgrade path

Validates the "ABAC governance only" flow from playbook.md § 2 and the § 2 → § 4 upgrade path.

**Phase 1 — ABAC-only deploy:**

`env.auto.tfvars` is configured with `uc_tables` only — no `genie_spaces` block. Plain `make generate` (no `MODE=` flag) generates groups, tag policies, tag assignments, FGAC policies, and masking functions, but no `genie_space_configs`. `make apply` applies all three layers — account, data_access, and workspace — but creates no Genie Space.

**Phase 2 — § 2 → § 4 upgrade path:**

A `genie_spaces` block is added to `env.auto.tfvars` and `make generate SPACE="Finance Analytics"` is run (per-space generation, not a full re-generate). `make apply` then creates the Genie Space. The test asserts that the existing `data_access/terraform.tfstate` is preserved (governance not disturbed) and ABAC verification still passes.

**Key assertions:**
- `generated/abac.auto.tfvars` does NOT declare `genie_space_configs` after Phase 1
- `generated/masking_functions.sql` IS generated (full ABAC mode)
- No `.genie_space_id_*` file after Phase 1 apply
- `data_access/terraform.tfstate` exists after Phase 1 apply (all layers applied)
- `generated/abac.auto.tfvars` contains `Finance Analytics` after Phase 2 generate
- `.genie_space_id_finance_analytics` file exists after Phase 2 apply
- `data_access/terraform.tfstate` still exists after Phase 2 apply (governance preserved)
- Column tags and masks verified after both phases

---

### 10. multi-space-import — Import two UI-created Genie Spaces at once

Validates the multi-space import pattern from playbook.md § 3.

Two Genie Spaces are created directly via the Genie REST API (simulating spaces built in the Databricks UI). The `env.auto.tfvars` is configured with two `genie_space_id` entries. `make generate` imports both spaces' configs verbatim from the API and generates shared ABAC governance. `make apply` attaches to both spaces (applies governance and ACLs) without creating any new spaces.

**Steps:**

| Step | Action |
|---|---|
| 1 | Create `dev_fin` + `dev_clinical` test catalogs |
| 2 | Create Finance Analytics Genie Space via `POST /api/2.0/genie/spaces` |
| 3 | Create Clinical Analytics Genie Space via `POST /api/2.0/genie/spaces` |
| 4 | Configure `dev` env: two `genie_space_id` entries, each with explicit `uc_tables` |
| 5 | `make generate ENV=dev` — imports both spaces, generates ABAC for both catalogs |
| 6 | Assert both `Finance Analytics` and `Clinical Analytics` in `generated/abac.auto.tfvars` |
| 7 | `make apply ENV=dev` — applies governance; no new spaces created |
| 8 | Assert **no** `.genie_space_id_*` files (both spaces attached, not created) |
| 9 | `setup_test_data.py --verify` — column tags and masks applied across both catalogs |
| 10 | Teardown: delete both API-created spaces + destroy Terraform resources |

**Key assertions:**
- `generated/abac.auto.tfvars` contains `Finance Analytics` and `Clinical Analytics` `genie_space_configs` entries
- `generated/abac.auto.tfvars` references both `dev_fin` and `dev_clinical` catalogs
- **No** `.genie_space_id_*` files after apply — Terraform attached, not created
- Column tags and masking policies applied across both catalogs
### 9. schema-drift — Column tag drift detection

Validates the schema evolution workflow: detecting new untagged columns, stale tag assignments for deleted columns, and combined drift from column renames. Tests `make audit-schema` and `make generate-delta`.

**Phase A — Baseline:**

Uses the `quickstart` setup (Finance Analytics with `dev_fin` tables). After `make generate` + `make apply`, verifies the baseline audit does not report `emergency_ssn` (the test column that will be added later).

**Phase B — Forward drift (ADD COLUMN):**

`ALTER TABLE dev_fin.finance.customers ADD COLUMN emergency_ssn STRING` adds a new PII column. `make audit-schema` detects it as forward drift (exit code 1). `make generate-delta` classifies it using the LLM (constrained to existing governed keys/values) and merges the new `tag_assignment` into `generated/abac.auto.tfvars`. `make apply` deploys the tag. Re-running `make audit-schema` confirms drift is resolved (exit code 0).

**Phase C — Reverse drift (DROP COLUMN):**

Tags are unset, then `ALTER TABLE DROP COLUMN emergency_ssn`. `make audit-schema` detects the stale `tag_assignment` in config that references the now-deleted column. `make generate-delta` removes it automatically (no LLM call needed). Re-running `make audit-schema` confirms the stale assignment is gone.

**Phase D — Combined drift (RENAME COLUMN):**

Tags are unset on `email`, then `ALTER TABLE RENAME COLUMN email TO contact_email`. `make audit-schema` detects both reverse drift (stale `email` assignment) and forward drift (untagged `contact_email`). `make generate-delta` removes the old and classifies the new. `make apply` deploys. Audit confirms clean.

| Step | Action |
|---|---|
| 1 | Quickstart baseline: setup data, generate, apply, verify |
| 2 | `make audit-schema` — assert `emergency_ssn` not reported |
| 3 | `ALTER TABLE ADD COLUMN emergency_ssn STRING` |
| 4 | `make audit-schema` — assert exit 1, `emergency_ssn` in output |
| 5 | `make generate-delta` — assert new `tag_assignment` added |
| 6 | `make apply` — assert tag applied in `column_tags` |
| 7 | `make audit-schema` — assert exit 0 |
| 8 | Unset tags + `ALTER TABLE DROP COLUMN emergency_ssn` |
| 9 | `make audit-schema` — assert exit 1 (stale assignment) |
| 10 | `make generate-delta` — assert stale assignment removed |
| 11 | `make audit-schema` — assert exit 0 |
| 12 | Unset tags + `ALTER TABLE RENAME COLUMN email TO contact_email` |
| 13 | `make audit-schema` — assert exit 1 (both directions) |
| 14 | `make generate-delta` — old removed, new classified |
| 15 | `make apply` — assert tag on `contact_email` |
| 16 | `make audit-schema` — assert exit 0 |

### 10. genie-import-no-abac — Import Genie Space, deploy to prod without ABAC

Validates the full workflow of importing an existing Genie Space and deploying it to production without generating or managing any ABAC governance. This is a valid use case when a separate governance team manages ABAC centrally.

**Steps:**

| Step | Action |
|---|---|
| 1 | Create `dev_fin` + `prod_fin` test catalogs |
| 2 | Create a Genie Space via REST API (simulating a UI-configured space) |
| 3 | `make setup ENV=import_noabac` — scaffold env |
| 4 | Write `env.auto.tfvars` with `genie_only = true` and `genie_space_id` pointing to the API-created space |
| 5 | `make generate ENV=import_noabac MODE=genie` — generate genie config only |
| 6 | Assert: `genie_space_configs` present, `tag_assignments` / `fgac_policies` absent, no `masking_functions.sql` |
| 7 | `make apply-genie ENV=import_noabac` — deploy workspace layer |
| 8 | Assert: no `.genie_space_id_*` file (space attached, not created); space accessible via API |
| 9 | `make promote SOURCE_ENV=import_noabac DEST_ENV=import_noabac_prod DEST_CATALOG_MAP=dev_fin=prod_fin` — promote remaps genie config or gracefully skips |
| 10 | `make apply-genie ENV=import_noabac_prod` — deploy prod workspace |
| 11 | Assert: no `data_access/terraform.tfstate`, no account resources, no `masking_functions.sql`, no `tag_assignments` / `fgac_policies` |

**Key assertions:**

- Imported space is attached (not created) — no `.genie_space_id_*` in dev env; space verified via API
- `make promote` either remaps genie config or exits 0 with a skip message (not a hard error)
- No governance artifacts are produced at any stage
- Only the workspace layer is managed — no account or data_access state

---

## Verify Checks (setup_test_data.py --verify)

| Check | Source | Pass condition |
|---|---|---|
| Row counts | `SELECT COUNT(*) FROM <table>` | Actual ≥ expected |
| Column tags | `system.information_schema.column_tags` | At least 1 tag per catalog |
| Column masks | `system.information_schema.column_masks` | At least 1 mask per catalog |

---

## Using `setup_test_data.py` Standalone

Run from your cloud wrapper root directory (`genie/aws/` or `genie/azure/`).

### Setup

```bash
# Dev catalogs only
python scripts/setup_test_data.py --auth-file envs/dev/auth.auto.tfvars

# Dev + prod catalogs (needed before make apply ENV=prod)
python scripts/setup_test_data.py --auth-file envs/dev/auth.auto.tfvars --prod
```

### Verify (run after `make apply`)

```bash
python scripts/setup_test_data.py --auth-file envs/dev/auth.auto.tfvars --verify
python scripts/setup_test_data.py --auth-file envs/dev/auth.auto.tfvars --verify-prod
```

### Teardown

```bash
python scripts/setup_test_data.py --auth-file envs/dev/auth.auto.tfvars \
  --teardown --teardown-prod
```

### CLI reference

| Flag | Description |
|---|---|
| `--auth-file <path>` | Path to `auth.auto.tfvars` (default: `./auth.auto.tfvars`) |
| `--prod` | Also create prod catalogs (`prod_fin`, `prod_clinical`) |
| `--verify` | Assert dev table row counts + ABAC governance; exits non-zero on failure |
| `--verify-prod` | Same as `--verify` but for prod catalogs |
| `--teardown` | Drop dev catalogs (`dev_fin`, `dev_clinical`) |
| `--teardown-prod` | Drop prod catalogs (`prod_fin`, `prod_clinical`) |
| `--warehouse-id <id>` | Use a specific SQL warehouse instead of auto-selecting |
| `--dry-run` | Print SQL to stdout without executing |

---

## Legacy: `make integration-test`

The original monolithic integration test is still available. It combines the
multi-space and promote scenarios (playbook.md § 1 + § 5) into a single pipeline without isolation:

```bash
# Full run — destroys everything at the end
make integration-test

# Keep data and deployed resources for inspection
make integration-test KEEP_DATA=1

# Pin a warehouse
make integration-test WAREHOUSE_ID=abc123ef
```

**Pipeline steps:**

| Step | Command | Purpose |
|---|---|---|
| 1 | `setup_test_data.py --prod` | Create dev + prod UC catalogs and sample data |
| 2 | `make setup` | Scaffold env directories |
| 3 | `make apply ENV=account` | Deploy groups and tag policies |
| 4 | `make generate ENV=dev` | Full LLM generation (both spaces) |
| 5 | `make apply ENV=dev` | Deploy dev governance |
| 6 | `setup_test_data.py --verify` | Assert dev ABAC governance |
| 7 | `make generate SPACE="Finance Analytics"` | Per-space isolation check |
| 8 | `make promote ... DEST_CATALOG_MAP=...` | Remap dev → prod catalogs |
| 9 | `make apply ENV=prod` | Deploy prod governance |
| 10 | `setup_test_data.py --verify-prod` | Assert prod ABAC governance |
| 11 | Teardown | Drop data + destroy Terraform (skipped if `KEEP_DATA=1`) |

Use `make test-all` instead for isolated, individually-reportable scenarios.

---

## Cleanup

```bash
# Drop test data only (leave Terraform resources in place)
python scripts/setup_test_data.py --auth-file envs/dev/auth.auto.tfvars \
  --teardown --teardown-prod

# Destroy Terraform resources only (leaves UC catalogs in place)
make destroy ENV=prod
make destroy ENV=dev
make destroy ENV=account

# Full cleanup — data + Terraform
python scripts/setup_test_data.py --auth-file envs/dev/auth.auto.tfvars \
  --teardown --teardown-prod && \
make destroy ENV=prod && make destroy ENV=dev && make destroy ENV=account
```

> **Note:** Always run `make destroy` before dropping UC catalogs. If catalogs
> are dropped first, the `deploy_masking_functions` destroy provisioner will
> fail with `Catalog not found`. If this happens, remove the stuck resource
> with `terraform state rm module.data_access.null_resource.deploy_masking_functions`
> in the affected env's `data_access/` directory, then re-run `make destroy`.

---

## Troubleshooting

### AWS: IAM role not deleted — `ExpiredToken`

**Symptom:**
```
⚠  Could not delete IAM role 'genie-test-uc-role-*': An error occurred (ExpiredToken)
   when calling the ListRolePolicies operation: The security token included in the request is expired
```

**Cause:** You are using temporary AWS STS credentials (`AWS_SESSION_TOKEN`). The integration
test suite takes ~90 minutes; if the STS token lifetime is shorter than the total run time
(provision + test + teardown), the token expires before teardown can delete the IAM role.

**Fix (preferred) — switch to long-lived IAM user keys:**

Remove `AWS_SESSION_TOKEN` from `scripts/account-admin.aws.env` and replace `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` with permanent IAM user credentials. Long-lived keys never expire and
work reliably across the full CI pipeline.

**Fix (immediate) — refresh the token and re-run teardown:**

```bash
# Export fresh credentials in your shell (overrides the stale file values)
export AWS_ACCESS_KEY_ID=ASIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...

# Re-run teardown — it will pick up the fresh env vars
python scripts/provision_test_env.py teardown
```

**Fix (manual) — delete the role directly in AWS:**

If you cannot obtain fresh credentials, delete the orphaned role in the AWS Console:

1. Go to **IAM → Roles**
2. Search for `genie-test-uc-role-`
3. Select the role → **Delete**

The Databricks workspace and metastore are always removed by teardown regardless of whether
the IAM step succeeds, so only the IAM role requires manual cleanup.

**Prevention for `make test-ci`:** If your organisation requires STS tokens, extend the session
duration to at least 4 hours before starting the pipeline:

```bash
# Request a longer-lived token (max depends on your IAM policy, up to 12 h for roles)
aws sts assume-role --role-arn arn:aws:iam::<account>:role/<role> \
  --role-session-name genie-ci --duration-seconds 14400   # 4 hours
```

---

### Azure: Storage account or access connector not deleted — auth error

**Symptom:**
```
⚠  Could not delete storage account 'genietest*': The client secret has expired.
```

**Cause:** Azure AD client secrets have a finite lifetime (default 6 months or 2 years). If the secret expires between provisioning and teardown, Azure API calls fail.

**Fix — generate a new client secret and re-run teardown:**

1. Azure Portal → Microsoft Entra ID → App registrations → your app → Certificates & secrets
2. Generate a new client secret
3. Update `AZURE_CLIENT_SECRET` in `scripts/account-admin.azure.env`
4. Re-run teardown:
   ```bash
   python scripts/provision_test_env.py teardown
   ```

**Fix (manual) — delete resources directly in Azure Portal:**

1. Go to **Resource Groups → your RG**
2. Search for `genietest` — delete the storage account and access connector
3. Go to **Microsoft Entra ID → Enterprise applications** — remove any test managed identities

**Prevention:** Use a client secret with a longer expiry (2 years), or automate secret rotation in your CI pipeline.

---

### Orphaned workspace or metastore after failed teardown

If teardown fails completely, check the Databricks Account Console:

- **Workspaces**: Account Console → Workspaces → filter by name `genie-test-*` → Delete
- **Metastores**: Account Console → Data → Unity Catalog → filter by name `genie-test-*` → Delete (check "Force delete")
- **Groups**: Account Console → User Management → Groups → filter by name `genie-test-admins-*` → Delete

After manually cleaning up, remove the stale state file so subsequent runs start clean:

```bash
rm -f scripts/.test_env_state.json
```
