# Quickstart: Create a Genie agent from Scratch

> **Already have a Genie agent?** Most users do — [import it from the UI into code](import-genie-agent-from-ui.md) first.

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
#   enable_classification = true      # turns on scanning
#   enable_auto_tagging   = false     # default; flip to true after reviewing detections
# The classification footprint is the union of top-level uc_tables and each
# genie_spaces[*].uc_tables entry.
make enable-classification ENV=dev   # or enable it in the Databricks UI (recommended)
# Scanning populates system.data_classification.results (review detections in the UI).
# class.* column tags are written only once enable_auto_tagging = true and you re-apply;
# then poll system.information_schema.column_tags until tags land for the footprint.

# Generation consumes your existing IdP-synced groups (setup scaffolds
# manage_groups = false); pass one group per access tier, strictest first.
make generate GENERATE_ARGS='--groups "<idp-tier-1>,<idp-tier-2>,<idp-tier-3>"'
vi envs/dev/generated/abac.auto.tfvars
# Review and iterate on the generated governance and Genie config:
#   - groups            (references to your IdP-synced groups — not created here)
#   - tag assignments   (one gr_treatment derived per classified column)
#   - FGAC policies
#   - genie_space_configs (title, instructions, benchmarks, filters, measures per agent)
# Durable ACLs are not owned by this generated draft. Set acl_groups only on
# matching genie_spaces[] entries in env.auto.tfvars: omit/null derives from
# current policies, [] grants nobody, and a non-empty list is an explicit override.

vi envs/dev/generated/masking_functions.sql
# Review and iterate on the generated masking and row-filter functions.

make coverage-gate       # fails if any classified sensitive column has no protection ("says NO")
make validate-generated
make rehearse VERIFY_KEY_COLUMN=<key>   # apply (masks first; grants only if coverage passes), then prove masking as each tier
```

## What happens end-to-end

1. `make setup` creates `envs/account/`, `envs/dev/data_access/`, and `envs/dev/`
2. `make enable-classification` applies only the UC catalog classification configuration (scanning); auto-tagging is opt-in via `enable_auto_tagging` (default off). It does not need generated ABAC or masking files
3. You review detections, set `enable_auto_tagging = true`, re-apply, then wait for the scan to write `class.*` tags
4. `make generate` fetches DDLs and native classification, then writes a draft into `envs/dev/generated/`
5. You tune generated governance and semantic config; durable agent ACL intent remains in `env.auto.tfvars`
6. `make coverage-gate` fails if any classified sensitive column has no protection
7. `make rehearse` splits the generated draft into layered configs, applies all three layers (masks and policies before grants; business access only if the coverage check passes), then runs `verify-access`

Generation remains fail-closed: after enabling classification, wait for native tags before
running it. The explicit `--allow-llm-sensitivity` escape hatch is unchanged.

Business exposure is fail-closed without a manual switch. Every apply re-reads live tags
and runs the coverage check first; Terraform refuses to plan new or wider business-group
table `SELECT` or Genie `CAN_RUN` unless a recent coverage result passed. A table's first
grant is also blocked while it has a sensitive-looking column with no tag (wait for the
scan, tag it, or list it in `coverage_acknowledged_columns`). Agent creation doesn't wait for the check,
so administrators can finish and inspect an agent's configuration first.

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
| **set** | Attaches to the existing space. Never creates or deletes it. See [Import a Genie Agent from UI into Code](import-genie-agent-from-ui.md). |

---

## What's next?

- [Promote dev → prod](playbook.md#promote-dev--prod) — replicate governance to production with catalog remapping
- [Add another Genie agent](playbook.md#add-another-genie-agent) — incremental generation without touching existing agents
- [Country & industry overlays](playbook.md#country-and-industry-overlays) — region-specific or industry-specific governance
- [Advanced scenarios](playbook.md#advanced-scenarios) — ABAC-only, self-service Genie, independent BU environments
- [Version control your configs](version-control.md) — what to commit, version pinning, running Terraform directly
