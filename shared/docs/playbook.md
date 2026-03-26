# Playbook

GenieRails puts Genie onboarding on rails — import your existing Genie Space, generate ABAC governance, and promote to production.

## Pick your starting point

| Starting point | You have... | Go to |
|---|---|---|
| **I already have a Genie Space** | A space configured in the Databricks UI that needs governance and promotion to prod | [Import, Govern, and Promote](#import-govern-and-promote) |
| **I'm starting from scratch** | Tables in Unity Catalog, no Genie Space yet | [Quickstart](#quickstart-from-scratch) |

---

## Import, Govern, and Promote

> **This is the recommended starting point.** Most users already have a Genie Space in the Databricks UI and want to add governance (groups, tags, masking) and deploy it to production.

### What you'll achieve

1. Import your existing Genie Space configuration into code (instructions, benchmarks, SQL measures — all captured verbatim)
2. Generate ABAC governance: groups, tag policies, column masking, row filters, catalog grants
3. Review and tune the generated governance
4. Apply everything to your dev workspace
5. Promote the whole setup — governance and Genie config — to production

### What gets imported

`make generate` queries the Genie Space API and captures the full space configuration verbatim — no LLM re-writing:

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

### Step 1 — Point at your existing space

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

### Step 2 — Import config and generate governance

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

### Step 3 — Review, tune, and apply

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

### Step 4 — Promote to prod

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

## Quickstart (from scratch)

Use this when you don't have an existing Genie Space yet and want to create everything from scratch.

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

When you're ready to deploy to production, see [Promote dev → prod](#promote-dev--prod).

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

### Space attachment behaviour

Each entry in `genie_spaces` operates in one of two modes based on whether `genie_space_id` is set:

| `genie_space_id` | What the tool does |
| --- | --- |
| **empty** (default) | Creates and fully manages the space: title, benchmarks, instructions, group ACLs, full lifecycle. Requires `uc_tables`. |
| **set** | Attaches to the existing space. Never creates or deletes it. ABAC governance and group ACLs are applied. Tables are discovered automatically from the Genie API, or supplied via `uc_tables` if auto-discovery is not yet available. |

> **Note:** This is a per-space setting in `env.auto.tfvars` and is separate from the `MODE=` flag for `make generate`, which controls [team responsibility separation](self-service-genie.md).

---

## What's next?

After completing either starting path, these are the most common next steps.

### Add another Genie Space

Add a second space without re-generating existing ones:

```bash
# 1. Add Space B to env.auto.tfvars alongside your existing space(s)
vi envs/dev/env.auto.tfvars

# 2. Generate ONLY Space B's config — existing spaces are preserved
make generate SPACE="Clinical Analytics"

# 3. Review and apply
vi envs/dev/generated/abac.auto.tfvars   # verify existing spaces are unchanged
make validate-generated
make apply
```

| Situation | Command |
| --------- | ------- |
| Adding a new space without touching existing ones | `make generate SPACE="Space B"` |
| Re-tuning a single space from scratch | `make generate SPACE="Finance Analytics"` |
| Adding new groups or changing shared tag policies | `make generate` (full — reviews all spaces) |

> Per-space generation does **not** modify groups or tag_policies. If you genuinely need new groups, run full `make generate` (no `SPACE=`).

### Promote dev → prod

If you followed the [Import, Govern, and Promote](#import-govern-and-promote) path, promotion was already covered in Step 4. For the quickstart path or any other environment:

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod \
  DEST_CATALOG_MAP="dev_catalog=prod_catalog"

vi envs/prod/auth.auto.tfvars   # enter prod workspace credentials

make apply ENV=prod
```

For multiple catalogs:

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod \
  DEST_CATALOG_MAP="dev_fin=prod_fin,dev_clinical=prod_clinical"
```

**How `DEST_CATALOG_MAP` works:**

- Comma-separated `src_catalog=dest_catalog` pairs
- The promote command auto-detects all source catalog names from `genie_spaces[*].uc_tables`
- Every detected catalog must have a mapping — the command fails clearly if any are missing

### Country and industry overlays

Add region-specific or industry-specific governance context:

```bash
# Country overlays (ANZ, India, Southeast Asia)
make generate COUNTRY=ANZ
make generate COUNTRY=ANZ,IN,SEA

# Industry overlays (Financial Services, Healthcare, Retail)
make generate INDUSTRY=healthcare
make generate INDUSTRY=financial_services,retail

# Both combined
make generate COUNTRY=ANZ INDUSTRY=healthcare
```

These work with any scenario — just add the flag. See [Country Overlays](country-overlays.md) and [Industry Overlays](industry-overlays.md) for details.

### Schema drift detection

After ABAC governance is deployed, table schemas may evolve. Two commands handle drift without a full `make generate` re-run:

```bash
# Detect untagged columns and stale assignments
make audit-schema ENV=dev

# Auto-classify new columns and remove stale ones
make generate-delta ENV=dev
make apply ENV=dev
```

| Schema change | What happens |
| --- | --- |
| `ALTER TABLE ADD COLUMN patient_ssn STRING` | `generate-delta` classifies and tags it |
| `ALTER TABLE DROP COLUMN old_ssn` | `generate-delta` removes the stale assignment |
| `ALTER TABLE RENAME COLUMN ssn TO tax_id` | Old assignment removed, new column classified |

---

## Advanced scenarios

These cover less common deployment patterns. Most users won't need them on day one.

### ABAC governance only (no Genie Space)

Set up groups, tag policies, column masking, row filters, and catalog grants — without creating any Genie Space. Add Genie later without changing the governance setup.

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
vi envs/dev/generated/abac.auto.tfvars      # review groups, tags, policies
vi envs/dev/generated/masking_functions.sql # review masking and row-filter functions

make validate-generated
make apply
```

When you are ready to add a Genie Space later, add a `genie_spaces` block to `env.auto.tfvars` and run `make generate SPACE="My Space"`.

### Independent BU environment

A second business unit needs its own groups, governance, and Genie spaces — not a promotion of `dev`.

```bash
make setup ENV=bu2
vi envs/bu2/auth.auto.tfvars   # enter BU workspace credentials
vi envs/bu2/env.auto.tfvars    # define the BU's genie_spaces

make generate ENV=bu2
vi envs/bu2/generated/abac.auto.tfvars
vi envs/bu2/generated/masking_functions.sql

make validate-generated ENV=bu2
make apply ENV=bu2
```

### Central governance, self-service Genie

A **central Data Governance team** owns ABAC policies, while **BU teams self-serve their own Genie spaces**.

> For the full reference guide see [`docs/self-service-genie.md`](self-service-genie.md).

| Layer | Governance team | BU team |
| ----- | --------------- | ------- |
| `account` | Owns — creates groups, tag policies | Reads — looks up group names |
| `data_access` | Owns — tag assignments, FGAC policies, masking functions, catalog grants | Skips entirely |
| `workspace` | Skips — no Genie spaces | Owns — Genie spaces, workspace ACLs |

**Governance team:**

```bash
make setup ENV=governance
vi envs/governance/auth.auto.tfvars
vi envs/governance/env.auto.tfvars   # list all governed tables, no genie_spaces

make generate ENV=governance MODE=governance
vi envs/governance/generated/abac.auto.tfvars
make apply-governance ENV=governance
```

**BU team:**

```bash
make setup ENV=bu1
vi envs/bu1/auth.auto.tfvars   # workspace USER SP + SQL entitlement
vi envs/bu1/env.auto.tfvars    # genie_only = true, sql_warehouse_id required

make generate ENV=bu1 MODE=genie
vi envs/bu1/generated/abac.auto.tfvars   # only genie_space_configs
make apply-genie ENV=bu1
```

> **Least privilege:** When `genie_only = true`, the BU SP only needs **workspace USER** membership and **Databricks SQL access** — no admin roles. See [Self-Service Genie — Least-Privilege SP](self-service-genie.md#least-privilege-service-principal-for-bu-teams).

### Import Genie Space to prod without ABAC

Import a UI-created Genie Space and deploy to production when ABAC is managed separately by a governance team.

```bash
make setup ENV=bu_import
vi envs/bu_import/env.auto.tfvars
# genie_only = true, genie_space_id = "<id>", sql_warehouse_id = "<id>"
vi envs/bu_import/auth.auto.tfvars

make generate ENV=bu_import MODE=genie
make apply-genie ENV=bu_import

make promote SOURCE_ENV=bu_import DEST_ENV=bu_import_prod \
  DEST_CATALOG_MAP="dev_catalog=prod_catalog"
make apply-genie ENV=bu_import_prod
```

This creates no tag policies, FGAC policies, masking functions, or account-level resources.

> **Tested:** `make test-genie-import-no-abac` validates this workflow end-to-end.

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

### How layers are applied

`make apply ENV=<name>` always applies in this order:

1. `envs/account/` — shared account layer (groups, tag policies)
2. `envs/<name>/data_access/` — env-scoped governance (tags, masking, grants)
3. `envs/<name>/` — workspace layer (Genie Spaces, ACLs)

Bare commands default to `ENV=dev`.

### The core loop

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
