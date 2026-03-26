# Playbook

This document covers the main use cases for deploying and managing Genie Spaces with ABAC governance.

**Pick the scenario that fits your situation** — these are independent starting points, not sequential steps.

## Quick reference

| Scenario | When to use | Key commands |
| -------- | ----------- | ------------ |
| [A. Quickstart](#scenario-a-quickstart-your-first-environment-dev) | First deployment, single team owns everything | `make setup` → `make generate` → `make apply` |
| [B. ABAC governance only](#scenario-b-abac-governance-only-no-genie-space) | Not ready for Genie yet — set up groups, tags, masking, grants only | `make setup` → `make generate` → `make apply` (no `genie_spaces`) |
| [C. Import an existing Genie Space](#scenario-c-import-an-existing-genie-space-govern--promote) | Bring a UI-configured space under code governance (optionally promote to prod) | `make generate` (with `genie_space_id`) → `make apply` → `make promote` |
| [D. Add a Genie Space](#scenario-d-add-a-new-genie-space-incremental) | Add a second space without re-generating existing ones | `make generate SPACE="Space B"` → `make apply` |
| [E. Promote dev → prod](#scenario-e-promote-dev--prod) | Replicate dev governance to prod with renamed catalogs | `make promote` → `make apply ENV=prod` |
| [F. Independent BU environment](#scenario-f-create-an-independent-bu-environment) | BU needs its own groups, governance, and Genie spaces | `make setup ENV=bu2` → `make generate ENV=bu2` → `make apply ENV=bu2` |
| [G. Central governance, self-service Genie](#scenario-g-central-governance-self-service-genie) | Central ABAC team + BU teams self-serve Genie spaces | `make generate MODE=governance` / `make generate MODE=genie` |
| [H. Import Genie Space to prod (no ABAC)](#scenario-h-import-genie-space-to-prod-without-abac) | Import a UI-created Genie Space and deploy to prod when ABAC is managed separately | `make generate MODE=genie` (with `genie_space_id` + `genie_only=true`) → `make apply-genie` → `make promote` |
| [I. APJ / non-US region](#scenario-i-apj--non-us-region-country-overlays) | Dataset contains non-US PII (ANZ, India, Southeast Asia) | Set `country = "ANZ"` → `make generate` → `make apply` |

### How layers are applied

`make apply ENV=<name>` always applies in this order:

1. `envs/account/` — shared account layer (groups, tag policies)
2. `envs/<name>/data_access/` — env-scoped governance (tags, masking, grants)
3. `envs/<name>/` — workspace layer (Genie Spaces, ACLs)

Bare commands default to `ENV=dev`.

---

## Scenario A: Quickstart your first environment (`dev`)

Use this when you are starting from scratch and want one working environment quickly.

```bash
make setup
vi envs/dev/auth.auto.tfvars   # enter workspace credentials for the service principal

vi envs/dev/env.auto.tfvars
# Define your Genie Spaces. All table names must be fully qualified (catalog.schema.table).
#
# Example:
#   genie_spaces = [
#     {
#       name      = "Finance & Clinical Analytics"
#       uc_tables = [
#         "dev_catalog.finance.transactions",
#         "dev_catalog.clinical.encounters",
#       ]
#     },
#   ]
#
# Optional: replace envs/account/auth.auto.tfvars or
# envs/dev/data_access/auth.auto.tfvars if shared layers need different credentials.

make generate
vi envs/dev/generated/abac.auto.tfvars
# Review and iterate on the generated governance and Genie config:
#   - groups
#   - tag policies and tag assignments
#   - FGAC policies
#   - genie_space_configs (title, instructions, benchmarks, filters, measures per space)

vi envs/dev/generated/masking_functions.sql
# Review and iterate on the generated masking and row-filter functions.

make validate-generated
make apply
```

What happens end-to-end:

1. `make setup` creates `envs/account/`, `envs/dev/data_access/`, and `envs/dev/`
2. `make generate` fetches DDLs from Unity Catalog, calls the LLM, and writes a draft into `envs/dev/generated/`
3. You tune the generated governance and Genie config
4. `make apply` splits the generated draft into layered configs and applies all three layers

### Space attachment behaviour

Each entry in `genie_spaces` operates in one of two modes based on whether `genie_space_id` is set:

| `genie_space_id` | What the tool does |
| --- | --- |
| **empty** (default) | Creates and fully manages the space: title, benchmarks, instructions, group ACLs, full lifecycle. Requires `uc_tables`. |
| **set** | Attaches to the existing space. Never creates or deletes it. ABAC governance and group ACLs are applied. Tables are discovered automatically from the Genie API, or supplied via `uc_tables` if auto-discovery is not yet available. |

> **Note:** This is a per-space setting in `env.auto.tfvars` and is separate from the `MODE=` flag for `make generate`, which controls [team responsibility separation](self-service-genie.md).

### Multiple Genie Spaces and multiple catalogs

You can define multiple spaces in one environment, and each space can draw tables from multiple catalogs:

```hcl
# sql_warehouse_id works at two levels:
#   top-level          → shared fallback for all spaces (empty = auto-create serverless)
#   inside genie_spaces → per-space override; omit to use the top-level warehouse
genie_spaces = [
  {
    name             = "Finance Analytics"
    sql_warehouse_id = ""           # optional: overrides the top-level warehouse for this space
    uc_tables = [
      "dev_fin.finance.transactions",
      "dev_fin.finance.customers",
      "dev_fin.accounts.*",
    ]
  },
  {
    name     = "Clinical Analytics"
    uc_tables = [
      "dev_clinical.clinical.encounters",
      "dev_clinical.clinical.diagnoses",
    ]
  },
]

sql_warehouse_id = ""   # shared fallback warehouse
```

The `name` is the human-readable Genie Space title shown in the Databricks UI. It also:
- Links each space's infrastructure settings (in `env.auto.tfvars`) to its semantic config (in `abac.auto.tfvars` under `genie_space_configs`) — the keys must match exactly
- Determines the internal Terraform resource key (sanitized to lowercase alphanumeric + underscores, e.g. `"Finance Analytics"` → `finance_analytics`)

Renaming a space causes Terraform to destroy and re-create it.

---

## Scenario B: ABAC governance only (no Genie Space)

Use this when you want to apply data governance — groups, tag policies, column masking, row filters, catalog grants — without creating any Genie Spaces yet. You can add Genie Spaces later without changing the governance setup.

```bash
make setup
vi envs/dev/auth.auto.tfvars   # workspace credentials

vi envs/dev/env.auto.tfvars
# List the tables to govern. Do NOT add a genie_spaces block.
# Example:
#   uc_tables = [
#     "dev_catalog.finance.*",
#     "dev_catalog.clinical.*",
#   ]
#   sql_warehouse_id = ""

make generate
# The LLM generates groups, tag policies, tag assignments, FGAC policies,
# and masking functions. No genie_space_configs is produced.

vi envs/dev/generated/abac.auto.tfvars      # review groups, tags, policies
vi envs/dev/generated/masking_functions.sql # review masking and row-filter functions

make validate-generated
make apply
# Applies all three layers:
#   account    → groups, tag policy definitions
#   data_access → tag assignments, masking functions, FGAC policies, catalog grants
#   workspace   → group workspace assignments and entitlements (no Genie Space created)
```

When you are ready to add a Genie Space later, simply add a `genie_spaces` block to `env.auto.tfvars` and run `make generate SPACE="My Space"` (see [Scenario D](#scenario-d-add-a-new-genie-space-incremental)). The existing governance is preserved.

---

## Scenario C: Import an existing Genie Space (govern + promote)

Use this when a Genie Space was already configured in the Databricks UI or another tool, and you want to:

- bring it under ABAC governance (groups, tag policies, masking functions, catalog grants)
- manage its configuration as code (instructions, benchmarks, sample questions, SQL measures)
- promote the whole thing — governance **and** Genie config — to a prod workspace

### What gets imported

`make generate` queries the Genie Space API and captures the full space configuration verbatim — no LLM re-writing. The following are extracted exactly as configured in the UI:

| Field | Captured from API |
| ----- | ---------------- |
| Instructions (text) | ✓ |
| Sample questions | ✓ |
| Benchmarks (question + SQL) | ✓ |
| SQL filters, measures, expressions | ✓ |
| Join specs | ✓ |
| Table list | ✓ — auto-discovered from the space |
| Space title and description | ✓ |
| SQL warehouse | — kept as-is in the existing space |

The ABAC governance (groups, tag policies, tag assignments, masking functions) is generated fresh by the LLM from the discovered table DDLs.

### Step-by-step

**Step 1 — Point at the existing space**

Find the Genie Space ID in the URL when viewing the space in the Databricks UI (e.g. `...genie/rooms/01ef7b3c2a4d5e6f`).

```bash
make setup
vi envs/dev/auth.auto.tfvars   # workspace credentials
vi envs/dev/env.auto.tfvars
```

```hcl
# envs/dev/env.auto.tfvars
genie_spaces = [
  {
    genie_space_id = "01ef7b3c2a4d5e6f"   # the only required field; find it in the Genie Space URL
    # name omitted     → defaults to the space title returned by the API
    # uc_tables omitted → discovered automatically from the Genie API
  },
]
```

**Step 2 — Import config and generate governance**

```bash
make generate
```

This does in one step:
1. Queries the Genie Space API — discovers tables and imports existing config verbatim
2. Fetches DDLs from Unity Catalog for those tables
3. LLM generates ABAC governance (groups, tag policies, tag assignments, masking functions)
4. Writes everything to `envs/dev/generated/abac.auto.tfvars` — the imported Genie config replaces any LLM-generated Genie content

You will see output like:

```
  Querying existing Genie Space 'Finance Analytics' for config...
    Discovered 3 table(s): dev_fin.finance.customers, dev_fin.finance.transactions, ...
  Injected genie_space_configs from Genie API for: Finance Analytics

  Next steps:
    1. Review generated/TUNING.md
    2. Review and tune generated/abac.auto.tfvars
    3. make apply
```

> **Required manual step:** Copy the discovered tables into `envs/dev/data_access/env.auto.tfvars` so that UC grants and masking functions are applied to them:
>
> ```hcl
> # envs/dev/data_access/env.auto.tfvars
> uc_tables = [
>   "dev_fin.finance.customers",
>   "dev_fin.finance.transactions",
>   # ... (as printed by make generate)
> ]
> ```

**Step 3 — Review, tune, and apply**

```bash
vi envs/dev/generated/abac.auto.tfvars
# - Review the imported genie_space_configs (instructions, benchmarks, etc.)
# - Review and tune the generated groups, tag_assignments, fgac_policies
# - The imported Genie config is now HCL — edit it here to manage it as code

vi envs/dev/generated/masking_functions.sql
# Review and iterate on generated masking and row-filter functions.

make validate-generated
make apply
```

> `make apply` attaches to the existing space (does not create or delete it), applies ABAC governance, and pushes any changes to the space config (instructions, benchmarks, etc.) back to the API.

**Step 4 — Promote to prod**

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod \
  DEST_CATALOG_MAP="dev_fin=prod_fin"

vi envs/prod/auth.auto.tfvars   # prod workspace credentials
vi envs/prod/env.auto.tfvars
```

For prod, leave `genie_space_id` empty to create a brand-new prod space from the promoted config, or set it to an existing prod space ID to attach:

```hcl
# envs/prod/env.auto.tfvars  (written by make promote, edit as needed)
genie_spaces = [
  {
    name           = "Finance Analytics"
    genie_space_id = ""            # empty → tool creates new prod space with the promoted config
    uc_tables = [
      "prod_fin.finance.customers",
      "prod_fin.finance.transactions",
    ]
  },
]
```

```bash
make apply ENV=prod
# Creates the prod Genie Space with the full promoted config:
# governance (groups, tags, masking) + Genie content (instructions, benchmarks, SQL)
```

### Multi-space import

Import multiple spaces in a single `make generate` by listing them all with `genie_space_id`:

```hcl
genie_spaces = [
  {
    genie_space_id = "01ef7b3c2a4d5e6f"   # Finance Analytics
  },
  {
    genie_space_id = "02ab9c1d3e4f5a6b"   # Executive Dashboard
  },
]
```

Each space's config is fetched independently. All spaces get their governance generated in the same LLM call, and all are promotable together.

### Caveats

- **API async delay**: The Genie API's `serialized_space` field may take 1–3 minutes to populate for newly created spaces. The tool retries automatically for up to ~4 minutes. If the space was just created moments ago, wait a minute before running `make generate`.
- **Destroy safety**: `make destroy` never deletes an attached space (`genie_space_id` set). Only spaces created by this tool (empty `genie_space_id`) are destroyed.
- **Config drift**: After the first import, `abac.auto.tfvars` is the source of truth. Changes made in the UI will not automatically sync back — re-run `make generate` (with `genie_space_id`) to re-import if needed.

---

## Scenario D: Add a new Genie Space (incremental)

Use this when you have a working `dev` environment with Space A fully tuned and applied, and you want to add Space B — without re-running the LLM over Space A or overwriting its hand-tuned benchmarks, masking functions, or FGAC policies.

### When to use per-space vs. full generation

| Situation | Command |
| --------- | ------- |
| Adding a new space without touching existing ones | `make generate SPACE="Space B"` |
| Re-tuning a single space from scratch | `make generate SPACE="Finance Analytics"` |
| Adding new groups or changing shared tag policies | `make generate` (full — reviews all spaces) |
| Recovering from a corrupt assembled config | `make generate` (full — rewrites from scratch) |

> Full `make generate` (no `SPACE=`) always replaces the assembled `generated/abac.auto.tfvars` entirely. Only use it when you want to regenerate everything, or for the very first run.

### How per-space generation works

After the first `make generate`, the tool creates a per-space directory structure alongside the assembled output:

```
envs/dev/generated/
  spaces/
    finance_analytics/         # bootstrapped by make generate (full run)
      abac.auto.tfvars         # genie_space_configs entry for this space
    clinical_analytics/
      abac.auto.tfvars
  abac.auto.tfvars             # assembled — what make apply uses (unchanged interface)
  masking_functions.sql        # assembled
  TUNING.md
```

When you run `make generate SPACE="Space B"`:
- Only Space B's tables are fetched from Unity Catalog
- The LLM generates config only for Space B (genie_space_configs, tag_assignments, fgac_policies, masking functions)
- Existing groups are auto-loaded from `envs/account/abac.auto.tfvars` so the LLM reuses them
- The assembled `generated/abac.auto.tfvars` is **patched** (not replaced): Space B's entries are added; Space A's content is untouched

### Step-by-step

```bash
# 1. Add Space B to env.auto.tfvars
vi envs/dev/env.auto.tfvars
```

```hcl
genie_spaces = [
  {
    name      = "Finance Analytics"    # Space A — already deployed and tuned
    uc_tables = [
      "dev_fin.finance.transactions",
      "dev_fin.finance.customers",
    ]
  },
  {
    name      = "Clinical Analytics"   # Space B — new
    uc_tables = [
      "dev_clinical.clinical.encounters",
      "dev_clinical.clinical.diagnoses",
    ]
  },
]
```

```bash
# 2. Generate ONLY Space B's config — Space A's tuned config is preserved
make generate SPACE="Clinical Analytics"

# 3. Review the per-space draft and the assembled output
vi envs/dev/generated/spaces/clinical_analytics/abac.auto.tfvars
vi envs/dev/generated/abac.auto.tfvars   # assembled — verify Space A is unchanged

# 4. Validate and apply
make validate-generated
make apply
```

> Per-space generation does **not** modify groups or tag_policies. If you genuinely need new groups for Space B, run full `make generate` (no `SPACE=`).

---

## Scenario E: Promote `dev` → `prod`

Use this when `prod` should reuse the same table set, groups, and governance design as `dev`, but point at different catalog names.

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod \
  DEST_CATALOG_MAP="dev_catalog=prod_catalog"

vi envs/prod/auth.auto.tfvars   # enter prod workspace credentials

make apply ENV=prod
```

For multiple catalogs across spaces, list a mapping for every source catalog:

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod \
  DEST_CATALOG_MAP="dev_fin=prod_fin,dev_clinical=prod_clinical"
```

**How `DEST_CATALOG_MAP` works:**

- Comma-separated `src_catalog=dest_catalog` pairs
- The promote command auto-detects all source catalog names from `genie_spaces[*].uc_tables`
- Every detected catalog must have a mapping — the command fails clearly if any are missing

**What promote does:**

1. Validates `DEST_CATALOG_MAP` against detected source catalogs
2. Writes `envs/prod/env.auto.tfvars` with catalog names substituted
3. Remaps catalog references in the generated `abac.auto.tfvars` and `masking_functions.sql`
4. Splits the remapped config into account, data_access, and workspace artifacts
5. `make apply ENV=prod` applies all three layers

> Use `make generate ENV=prod` instead of `make promote` only when prod needs a fully separate LLM-generated design.

---

## Scenario F: Create an independent BU environment

Use this when a second business unit needs its own groups, governance, Genie spaces, and possibly its own tables — rather than a promotion of `dev`. Choose this over [Scenario E (promote)](#scenario-e-promote-dev--prod) when:

- The BU has different tables, schemas, or catalogs
- The BU needs different groups or governance rules
- Genie prompts, measures, or benchmarks should be generated independently
- The governance design should evolve separately from `dev`

```bash
make setup ENV=bu2
vi envs/bu2/auth.auto.tfvars   # enter BU workspace credentials

vi envs/bu2/env.auto.tfvars
# Define the BU's genie_spaces with its own catalog and table list.

make generate ENV=bu2
vi envs/bu2/generated/abac.auto.tfvars      # review generated policies, groups, Genie config
vi envs/bu2/generated/masking_functions.sql # review masking functions

make validate-generated ENV=bu2
make apply ENV=bu2
```

---

## Scenario G: Central governance, self-service Genie

Use this when a **central Data Governance team** owns ABAC policies, groups, and masking functions, while **BU teams self-serve their own Genie spaces**.

> For the full reference guide including Git strategies, CI/CD integration, and FAQ, see [`docs/self-service-genie.md`](self-service-genie.md).

### Who owns what

| Layer | Governance team | BU team |
| ----- | --------------- | ------- |
| `account` | Owns — creates groups, tag policies | Reads — looks up group names |
| `data_access` | Owns — tag assignments, FGAC policies, masking functions, catalog grants | Skips entirely |
| `workspace` | Skips — no Genie spaces | Owns — Genie spaces, workspace ACLs |

### Governance team flow

The governance team's environment can be named anything (e.g. `governance`, `central`, `shared`). It is independent of the BU team environment names.

```bash
make setup ENV=governance
vi envs/governance/auth.auto.tfvars

vi envs/governance/env.auto.tfvars
# List all governed tables. No genie_spaces block needed.

make generate ENV=governance MODE=governance
# Generates: groups, tag_policies, tag_assignments, fgac_policies, masking_functions.sql
# Suppresses: genie_space_configs

vi envs/governance/generated/abac.auto.tfvars
vi envs/governance/generated/masking_functions.sql

make apply-governance ENV=governance
# Applies: account layer (groups) + data_access layer (tags, policies, masks, grants)
# Skips:   workspace layer
```

> **Handoff to BU teams:** After `make apply-governance`, share the group names from `envs/account/abac.auto.tfvars` with BU teams. The BU `make generate MODE=genie` step auto-loads these group names from that file.

### BU team flow

```bash
make setup ENV=bu1
vi envs/bu1/auth.auto.tfvars
# Workspace USER SP + SQL entitlement — databricks_account_id can be left empty

vi envs/bu1/env.auto.tfvars
# Define Genie spaces using tables already governed by the central team.
# Set genie_only = true for least-privilege SP access (no admin roles needed).
# Requires sql_warehouse_id (BYO warehouse) — SP cannot create warehouses.

make generate ENV=bu1 MODE=genie
# Generates: genie_space_configs only (instructions, benchmarks, measures, sample questions)
# Suppresses: groups, tag_policies, tag_assignments, fgac_policies, masking SQL
# Auto-loads: existing groups from envs/account/abac.auto.tfvars

vi envs/bu1/generated/abac.auto.tfvars   # only genie_space_configs — tune Genie content

make apply-genie ENV=bu1
# Applies: workspace layer only (Genie spaces + config)
# Skips:   account and data_access layers
# With genie_only = true: also skips group lookup, workspace assignment, entitlements
```

> **Least privilege:** When `genie_only = true`, the BU team's SP only needs **workspace USER** membership and the **Databricks SQL access** entitlement — no admin roles at all. The governance team must grant the BU SP `CAN USE` on a warehouse, plus `USE CATALOG`, `USE SCHEMA`, and `SELECT` on the referenced tables (the Genie API validates table access at space creation). `sql_warehouse_id` is required (BYO warehouse). Groups must be empty in `abac.auto.tfvars` — the governance team manages group assignments separately. See [Self-Service Genie — Least-Privilege SP](self-service-genie.md#least-privilege-service-principal-for-bu-teams).

### Adding a second BU

```bash
make setup ENV=bu2
# Copy auth, define bu2's genie_spaces in env.auto.tfvars
make generate ENV=bu2 MODE=genie
make apply-genie ENV=bu2
# Governance team's data_access state is completely unaffected.
```

### Destroy (role-aware)

```bash
# BU teams remove their Genie spaces first:
make destroy-genie ENV=bu1
make destroy-genie ENV=bu2

# Governance team removes ABAC policies (after BU workspaces are destroyed):
make destroy-governance ENV=governance
make destroy ENV=account
```

---

## Scenario H: Import Genie Space to prod without ABAC

Use this when a data team has already created a Genie Space in the Databricks UI and you want to deploy it to production **without generating or managing any ABAC governance** — because a separate governance team handles tags, FGAC policies, and masking functions centrally.

This combines `genie_only = true` (from [§ G](#scenario-g-central-governance-self-service-genie)) with `genie_space_id` import (from [§ C](#scenario-c-import-an-existing-genie-space-govern--promote)) and the promote workflow (from [§ E](#scenario-e-promote-dev--prod)).

### Steps

```bash
# 1. Set up a new BU environment
make setup ENV=bu_import

# 2. Configure env.auto.tfvars:
#    - genie_only = true (no account-level resources)
#    - genie_space_id = "<existing-space-id>" (import, don't create)
#    - uc_tables = [...] (tables the space queries)
#    - sql_warehouse_id = "<warehouse-id>" (BYO warehouse required in genie_only mode)
vi envs/bu_import/env.auto.tfvars

# 3. Set up auth (BU team's service principal — only needs workspace USER + SQL entitlement)
vi envs/bu_import/auth.auto.tfvars

# 4. Generate genie config only (no ABAC generation)
make generate ENV=bu_import MODE=genie

# 5. Apply workspace layer only
make apply-genie ENV=bu_import

# 6. Promote to prod
#    `make promote` remaps genie config or gracefully skips when no
#    generated/abac.auto.tfvars exists.
make promote SOURCE_ENV=bu_import DEST_ENV=bu_import_prod \
  DEST_CATALOG_MAP="dev_catalog=prod_catalog"

# If promote printed "Skipping promote (genie-only workflow)", configure prod manually:
make setup ENV=bu_import_prod
vi envs/bu_import_prod/env.auto.tfvars   # same as dev but with prod catalog names
make generate ENV=bu_import_prod MODE=genie
vi envs/bu_import_prod/auth.auto.tfvars   # prod workspace credentials
make apply-genie ENV=bu_import_prod
```

### What this workflow does NOT create

- No tag policies or tag assignments
- No FGAC policies
- No masking functions
- No account-level resources (groups, workspace assignments)
- No `data_access/terraform.tfstate`

All governance is managed separately by the central team via `make apply-governance`.

> **Tested:** `make test-genie-import-no-abac` validates this workflow end-to-end. See [integration-testing.md](integration-testing.md) for details.

---

## Scenario I: APJ / non-US region (country overlays)

Use this when your dataset contains non-US personally identifiable information — for example, Australian TFNs, Indian Aadhaar numbers, or Singaporean NRICs. The country overlay system injects region-specific identifier knowledge, masking functions, and regulatory context into the LLM prompt so it produces governance appropriate for your region.

This works with any scenario above (quickstart, multi-space, promote, etc.) — just add the `country` setting.

### Supported regions

| Code | Region | Key identifiers |
|------|--------|--------------------|
| `ANZ` | Australia & New Zealand | TFN, Medicare, BSB, IRD, NHI |
| `IN` | India | Aadhaar, PAN, GSTIN, IFSC, UPI |
| `SEA` | Singapore & Malaysia | NRIC, FIN, MyKad, UEN, EPF |

### Steps

```bash
# Option 1: Set in env.auto.tfvars (persistent)
# Edit envs/dev/env.auto.tfvars and set:
#   country = "ANZ"            # single region
#   country = "ANZ,SEA"        # multi-region dataset

make generate
make apply

# Option 2: Override via CLI (one-off, takes priority over env.auto.tfvars)
make generate COUNTRY=ANZ
make generate COUNTRY=ANZ,IN,SEA    # multi-region
```

### What changes

- `masking_functions.sql` includes country-specific UDFs (e.g. `mask_tfn`, `mask_aadhaar`, `mask_nric`)
- `abac.auto.tfvars` includes tag assignments and FGAC policies referencing those functions
- Validation checks against extended country-specific column patterns

### Tuning an existing country or adding a new one

Each country overlay is a self-contained YAML file in `shared/countries/` — no Python, Terraform, or Makefile changes are needed.

- **Tune an existing region:** Edit the YAML file (e.g. `shared/countries/ANZ.yaml`) to add identifiers, adjust column hints, refine masking functions, or improve the prompt overlay text.
- **Add a new region:** Create `shared/countries/<CODE>.yaml` using an existing file as a template (e.g. copy `ANZ.yaml` → `JP.yaml`).

See [country-overlays.md](country-overlays.md) for the full contributor guide — YAML structure, field reference, masking function guidelines, prompt overlay writing tips, and FAQ.

> **Tested:** `make test-country-overlay` validates ANZ, IN, SEA, and multi-region generation end-to-end.

---

## Destroy and reset

```bash
# Destroy a workspace environment (workspace layer, then data_access)
make destroy ENV=dev
make destroy ENV=prod

# Self-service Genie targeted destroys:
make destroy-genie ENV=bu1        # workspace layer only
make destroy-governance ENV=governance  # data_access layer only

# Destroy the shared account layer
make destroy ENV=account

# Remove local generated files and Terraform state (keeps checked-in config)
make clean ENV=dev

# Remove all env directories under envs/
make clean-all
```

Rules:
- `make destroy ENV=<workspace>` destroys only that environment's workspace and `data_access` layers
- `make destroy ENV=account` destroys shared account resources (groups, tag policy definitions)
- Destroying one environment does not affect other environments
- `make clean` removes local state and generated artifacts but keeps your config files
- `make clean-all` removes the full `envs/` tree including environment configs

---

## How it works

The core loop is:

```
inputs → make generate → review generated/ → make validate-generated → make apply
```

**Step 1 — You provide inputs**

- `auth.auto.tfvars`: secrets and workspace connection details
- `env.auto.tfvars`: one or more Genie Space definitions with fully-qualified table names, an optional shared warehouse, and optional per-space warehouse overrides

**Step 2 — `make generate` creates a draft**

- Fetches DDLs from Unity Catalog
- Sends the prompt plus DDLs to the configured LLM
- Writes draft outputs into `envs/<env>/generated/`

**Step 3 — You review, then `make apply` deploys**

- Tune `generated/abac.auto.tfvars` (groups, policies, `genie_space_configs`)
- Tune `generated/masking_functions.sql`
- Run `make validate-generated`
- Run `make apply`, which splits the draft into layered configs and applies them

### Generated draft outputs

| File | What it contains |
| ---- | ---------------- |
| `envs/<env>/generated/abac.auto.tfvars` | **Assembled** — groups, tag policies, tag assignments, FGAC policies, and `genie_space_configs`. This is what `make apply` reads. |
| `envs/<env>/generated/masking_functions.sql` | **Assembled** — all SQL masking and row-filter functions across all spaces. |
| `envs/<env>/generated/spaces/<key>/abac.auto.tfvars` | Per-space draft — bootstrapped by full generation, updated by `make generate SPACE="..."`. |
| `envs/<env>/generated/spaces/<key>/masking_functions.sql` | Per-space masking functions — written by `make generate SPACE="..."`. |

### What `make apply` creates

| Layer | Creates in Databricks |
| ----- | --------------------- |
| Account | Account groups, optional group membership, tag policy definitions |
| Data access | Tag assignments, masking functions, FGAC policies, catalog grants |
| Workspace | Workspace assignment, entitlements, optional warehouse, Genie Spaces and ACLs |

### Schema drift detection

After ABAC governance is deployed, table schemas may evolve — new columns added, existing columns dropped or renamed. The quickstart provides two commands to detect and handle schema drift without requiring a full `make generate` re-run:

**`make audit-schema`** — reports untagged sensitive columns (forward drift) and stale tag assignments referencing columns that no longer exist (reverse drift). Exits `1` if drift is found, `0` if clean. CI-friendly.

```bash
make audit-schema ENV=dev
```

**`make generate-delta`** — detects drift, removes stale assignments automatically (no LLM call), then classifies new untagged columns using the LLM (constrained to your existing governed tag keys/values). Merges the result into your config additively — existing tag assignments are never touched.

```bash
make generate-delta ENV=dev
make apply ENV=dev
```

The delta flow handles three schema change scenarios:

| Schema change | What happens |
| --- | --- |
| `ALTER TABLE ADD COLUMN patient_ssn STRING` | Forward drift: `audit-schema` detects the new PII column; `generate-delta` classifies it and adds a `tag_assignment` |
| `ALTER TABLE DROP COLUMN old_ssn` | Reverse drift: `audit-schema` detects the stale config entry; `generate-delta` removes it |
| `ALTER TABLE RENAME COLUMN ssn TO tax_id` | Both: old assignment removed, new column classified |
