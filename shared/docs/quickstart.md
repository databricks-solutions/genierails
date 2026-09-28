# Quickstart: Create a Genie agent from Scratch

> **Already have a Genie agent?** Most users do — see [From UI to Production](from-ui-to-production.md) instead.

> **Using country or industry overlays?** Add `COUNTRY=ANZ` and/or `INDUSTRY=financial_services` to your `make generate` command for region-specific masking. See [Country Overlays](country-overlays.md) and [Industry Overlays](industry-overlays.md).

Use this when you don't have an existing Genie agent yet and want to create everything from scratch.

## Step-by-step

> **Prerequisite:** Complete Steps 1-2 in your cloud README ([AWS](../../aws/README.md) or [Azure](../../azure/README.md)) to set up credentials before continuing.

```bash
vi envs/dev/env.auto.tfvars
# Define your Genie agents. All table names must be fully qualified (catalog.schema.table).
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

# In envs/dev/env.auto.tfvars, opt into native classification:
#   enable_classification = true
# The classification footprint is the union of top-level uc_tables and each
# genie_spaces[*].uc_tables entry.
make enable-classification ENV=dev
# Poll system.information_schema.column_tags or system.data_classification.results
# until class.* tags have landed for the configured footprint.

make generate
vi envs/dev/generated/abac.auto.tfvars
# Review and iterate on the generated governance and Genie config:
#   - groups
#   - tag policies and tag assignments
#   - FGAC policies
#   - genie_space_configs (title, instructions, benchmarks, filters, measures per space)
#   - acl_groups per space (which groups can run each Genie agent)

vi envs/dev/generated/masking_functions.sql
# Review and iterate on the generated masking and row-filter functions.

make validate-generated
# Confirm `make audit-schema` is clean, then release business access in
# envs/dev/env.auto.tfvars:
#   business_access_enabled = true
make apply
```

## What happens end-to-end

1. `make setup` creates `envs/account/`, `envs/dev/data_access/`, and `envs/dev/`
2. `make enable-classification` applies only the UC catalog classification configuration and auto-tag settings; it does not need generated ABAC or masking files
3. You wait for the asynchronous Databricks scan to land `class.*` tags
4. `make generate` fetches DDLs and native classification, then writes a draft into `envs/dev/generated/`
5. You tune the generated governance and Genie config
6. `make apply` splits the generated draft into layered configs and applies all three layers

Generation remains fail-closed: after enabling classification, wait for native tags before
running it. The explicit `--allow-llm-sensitivity` escape hatch is unchanged.

Business exposure is fail-closed. With the default `business_access_enabled = false`,
apply creates the enforcement scaffolding and may create/configure Genie agents, but
it withholds business-group table `SELECT` and Genie `CAN_RUN` ACLs. Set the flag to
`true` only after the coverage validation and schema drift check are green. Space
creation remains ungated so administrators can finish and inspect its configuration
before releasing it to business users.

## Multiple Genie agents and multiple catalogs

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

The `name` is the human-readable Genie agent title shown in the Databricks UI. It also:
- Links each space's infrastructure settings (in `env.auto.tfvars`) to its semantic config (in `abac.auto.tfvars` under `genie_space_configs`) — the keys must match exactly
- Determines the internal Terraform resource key (sanitized to lowercase alphanumeric + underscores, e.g. `"Finance Analytics"` → `finance_analytics`)

Renaming a space causes Terraform to destroy and re-create it.

## Space attachment behaviour

Each entry in `genie_spaces` operates in one of two modes based on whether `genie_space_id` is set:

| `genie_space_id` | What the tool does |
| --- | --- |
| **empty** (default) | Creates and fully manages the space: title, benchmarks, instructions, group ACLs, full lifecycle. Requires `uc_tables`. |
| **set** | Attaches to the existing space. Never creates or deletes it. See [From UI to Production](from-ui-to-production.md). |

---

## What's next?

- [Promote dev → prod](playbook.md#promote-dev--prod) — replicate governance to production with catalog remapping
- [Add another Genie agent](playbook.md#add-another-genie-space) — incremental generation without touching existing spaces
- [Country & industry overlays](playbook.md#country-and-industry-overlays) — region-specific or industry-specific governance
- [Advanced scenarios](playbook.md#advanced-scenarios) — ABAC-only, self-service Genie, independent BU environments
- [Version control your configs](version-control.md) — what to commit, version pinning, running Terraform directly
