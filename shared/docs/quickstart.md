# Quickstart: Create a Genie Space from Scratch

> **Already have a Genie Space?** Most users do — see [From UI to Production](from-ui-to-production.md) instead.

Use this when you don't have an existing Genie Space yet and want to create everything from scratch.

## Step-by-step

Pick your cloud and set up credentials:

| My workspace is on... | Start here |
| --- | --- |
| AWS   | [`aws/README.md`](../../aws/README.md) |
| Azure | [`azure/README.md`](../../azure/README.md) |

```bash
cd aws/   # or azure/
make setup
vi envs/dev/auth.auto.tfvars   # enter workspace credentials (see cloud README above)

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

## What happens end-to-end

1. `make setup` creates `envs/account/`, `envs/dev/data_access/`, and `envs/dev/`
2. `make generate` fetches DDLs from Unity Catalog, calls the LLM, and writes a draft into `envs/dev/generated/`
3. You tune the generated governance and Genie config
4. `make apply` splits the generated draft into layered configs and applies all three layers

## Multiple Genie Spaces and multiple catalogs

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

## Space attachment behaviour

Each entry in `genie_spaces` operates in one of two modes based on whether `genie_space_id` is set:

| `genie_space_id` | What the tool does |
| --- | --- |
| **empty** (default) | Creates and fully manages the space: title, benchmarks, instructions, group ACLs, full lifecycle. Requires `uc_tables`. |
| **set** | Attaches to the existing space. Never creates or deletes it. See [From UI to Production](from-ui-to-production.md). |

---

## What's next?

- [Promote dev → prod](playbook.md#promote-dev--prod) — replicate governance to production with catalog remapping
- [Add another Genie Space](playbook.md#add-another-genie-space) — incremental generation without touching existing spaces
- [Country & industry overlays](playbook.md#country-and-industry-overlays) — region-specific or industry-specific governance
- [Advanced scenarios](playbook.md#advanced-scenarios) — ABAC-only, self-service Genie, independent BU environments
