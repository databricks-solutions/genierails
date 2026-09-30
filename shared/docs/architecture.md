# Architecture

This document explains the layered state model, config files, and resource ownership.

## Governance model — four invariants

Everything below rests on four invariants of the dev-to-prod walkthrough:

1. **Unity Catalog is the sensitivity source of truth.** Native Data Classification writes `class.*` tags on sensitive columns; GenieRails does not guess by default. When `enable_classification=true`, generation is fail-closed — unreadable/empty native results abort rather than fall back to LLM inference (unless the operator explicitly passes the `--allow-llm-sensitivity` escape hatch).
2. **One `gr_treatment` per column.** GenieRails collapses a column's `class.*` findings deterministically to exactly one enforcement treatment (`gr_treatment`), so exactly one column mask ever resolves; masks are keyed to that treatment vocabulary.
3. **A blocking coverage gate.** `make coverage-gate` is an offline check that reads the generated `abac.auto.tfvars` + `masking_functions.sql` and **exits non-zero** if a classification finding has no treatment mapping, a classified column has no covering column-mask policy, or a treatment's masking function is missing — and it never drops tags or policies to force a pass. It is a *separate* step (`make apply` does not run it), so run it as an explicit gate before applying.
4. **The exposure gate controls release.** `business_access_enabled` withholds business `SELECT` and Genie `CAN_RUN` until set to `true`; enforcement resources can exist while access stays withheld. Prod re-derives its own facts (`derive-assignments`) before the gate opens.

## Layer Model

The quickstart edits files in `envs/<env>/`, while Terraform itself runs from fixed roots in `roots/account`, `roots/data_access`, and `roots/workspace`. Make passes the correct `-var-file` inputs for you and keeps the split config synchronized.

| Layer | Path | Owns | Does not own |
| ----- | ---- | ---- | ------------ |
| Account | `envs/account/` | Account groups, optional group membership, tag policy definitions | Masking functions, FGAC policies, Genie resources |
| Data access | `envs/<env>/data_access/` | Env-scoped tag assignments, masking functions, FGAC policies, catalog grants | Account tag policy definitions, workspace entitlements, Genie lifecycle |
| Workspace | `envs/<env>/` | Workspace assignment, entitlements, optional warehouse, optional Genie agent and ACLs | Account groups, tag policies, FGAC policies |

### Self-service Genie operating mode

The layers are designed so that different teams can own different layers independently. In self-service Genie deployments, a central Data Governance team owns the account + data_access layers while BU teams own only their workspace layers. See [self-service-genie.md](self-service-genie.md) for the full guide and CI/CD integration patterns.

## Directory Contract

- `envs/account/` is the only shared layer across all environments
- `envs/<workspace>/data_access/` is isolated per environment
- `envs/<workspace>/` is also isolated per environment and owns generation-time workspace artifacts
- Root `scripts/`, root `*.py`, `roots/`, and `modules/` are shared implementation code and should not be copied into `envs/`

| File | What goes here | Tracked in git? |
| ---- | -------------- | --------------- |
| `roots/account/main.tf` | Stable Terraform root for account identities | **Yes** |
| `roots/data_access/main.tf` | Stable Terraform root for env-scoped governance | **Yes** |
| `roots/workspace/main.tf` | Stable Terraform root for workspace-local resources | **Yes** |
| `envs/<env>/auth.auto.tfvars` | Workspace credentials only (account ID, client ID/secret, workspace) | No (secrets) |
| `envs/<env>/env.auto.tfvars` | `uc_catalog`, `uc_tables`, `sql_warehouse_id`, `genie_space_id` | **Yes** |
| `envs/<env>/generated/` | Workspace-only generated draft outputs from `make generate` | No |
| `envs/<env>/ddl/` | Workspace-only local or fetched DDL snapshots used during generation | No |
| `envs/account/abac.auto.tfvars` | Shared account-owned config: `groups`, optional `group_members`, `tag_policies` | **Yes** |
| `envs/<env>/data_access/abac.auto.tfvars` | Env-scoped governance config: `groups`, tag assignments, FGAC policies | **Yes** |
| `envs/<env>/data_access/masking_functions.sql` | Env-scoped masking SQL deployed by the governance layer | **Yes** |
| `envs/<env>/abac.auto.tfvars` | Workspace-owned config: group lookups and Genie config only | **Yes** |

> **See also:** [Version Control & Standalone Terraform](version-control.md) for detailed guidance on what to commit, how to set up git tracking, version pinning, and running Terraform independently.

Examples:

- `envs/dev/auth.auto.tfvars`
- `envs/dev/env.auto.tfvars`
- `envs/dev/abac.auto.tfvars`

## Config Files

### `auth.auto.tfvars`

This file is gitignored and holds credentials:

```hcl
databricks_account_id    = "..."
databricks_account_host  = "https://..."   # required for Azure; defaults to AWS if omitted
databricks_client_id     = "..."
databricks_client_secret = "..."
databricks_workspace_id  = "..."
databricks_workspace_host = "https://..."
```

Configure these values in `envs/<env>/auth.auto.tfvars`. By default, `make setup` also creates `envs/account/auth.auto.tfvars` and `envs/<env>/data_access/auth.auto.tfvars` as links to the same workspace auth file, so one service principal can drive all three layers. Replace either shared-layer file only if account or governance resources need different credentials.

Where to find each field:

| Field | What it is | Where to find it |
| ----- | ---------- | ---------------- |
| `databricks_account_id` | Databricks account ID | Account Console, account selector, or account URL |
| `databricks_account_host` | Account console API base URL | `https://accounts.cloud.databricks.com` (AWS) or `https://accounts.azuredatabricks.net` (Azure). Defaults to AWS if omitted — **Azure users must set this explicitly**. |
| `databricks_client_id` | Service principal application/client ID | Account Console -> User management -> Service principals |
| `databricks_client_secret` | OAuth secret for that service principal | Same service principal record |
| `databricks_workspace_id` | Numeric target workspace ID | Account Console -> Workspaces, or `o=<workspace_id>` from the workspace URL |
| `databricks_workspace_host` | Workspace browser/API base URL | Workspace browser URL, for example `https://dbc-....cloud.databricks.com/` (AWS) or `https://adb-....azuredatabricks.net` (Azure) |

Service principal requirements:

- `Account Admin` to look up the IdP-synced access-tier groups and assign them to the workspace (and, only in the opt-in demo/greenfield create path with `manage_groups = true`, to create account groups)
- `Workspace Admin` to configure workspace entitlements, warehouses, and Genie resources
- `Metastore Admin` to create tag policies, FGAC policies, grants, and masking functions

If you already use the Databricks CLI, the same service principal details are often visible in the account console pages you used to create the CLI credentials. This quickstart intentionally stores them in `auth.auto.tfvars` instead of reading `~/.databrickscfg`.

### `env.auto.tfvars`

This file is checked in and holds environment-level settings:

```hcl
genie_spaces = [
  {
    name      = "Finance Analytics"
    uc_tables = [
      "dev_catalog.finance.customers",
      "dev_catalog.finance.transactions",
      "dev_catalog.finance.*",   # wildcard expands all tables in the schema
    ]
    # genie_space_id = ""   # omit or leave empty to create; set to attach to existing
    # sql_warehouse_id = "" # optional per-space override
  },
]

sql_warehouse_id        = ""     # shared fallback; empty = auto-create serverless
enable_classification   = true   # turn on UC native Data Classification for the footprint
business_access_enabled = false  # exposure gate: withholds SELECT + Genie CAN_RUN; open only after coverage-gate passes
```

`manage_groups` defaults to `false` on every layer (account, `data_access`, workspace): groups are **consumed** — looked up by name from the IdP-synced account groups — not created. This is the normal path. Only for a demo/greenfield account with no IdP-synced groups should `envs/account/env.auto.tfvars` set `manage_groups = true` (opt-in group creation); workspace and `data_access` env files always stay on the lookup-only default. See [IdP-Synced Groups](advanced.md#idp-synced-groups-default).

### `abac.auto.tfvars`

Generated by `make generate` in `envs/<env>/generated/`. `make promote` then splits it into:

- `envs/account/abac.auto.tfvars` for shared groups, optional group membership, and tag policy definitions
- `envs/<env>/data_access/abac.auto.tfvars` for FGAC policy rules and group lookup names

Tag assignments are classification facts, not promotion artifacts. Cross-environment
promotion carries rules (tag and FGAC policies, masking functions, classifier/config,
and group mappings), but does not copy dev's per-column assignments. Production runs
its own classification scan against production data and re-derives those facts; the
data-access module ignores classifier-driven assignment changes in Terraform state.
- `envs/<env>/data_access/masking_functions.sql` for env-scoped masking UDF deployment
- `envs/<env>/abac.auto.tfvars` for workspace group lookup names and Genie config

Tune the generated draft before applying. See `generated/TUNING.md` for guidance.

## Genie agent Behavior

Each entry in `genie_spaces` behaves based on whether `genie_space_id` is set:

| `genie_space_id` in entry | What happens on `make apply` |
| ------------------------- | ---------------------------- |
| Empty (default) | Creates a new Genie agent, configures it fully (title, instructions, benchmarks, ACLs), trashes it on `make destroy` |
| Set | Attaches to the existing agent — never creates or deletes it; applies ACLs and pushes config changes back to the API |

> **Exposure gate:** an agent can be *created and configured* while `business_access_enabled=false`, but its `CAN_RUN` ACLs (and business-user table `SELECT`) are withheld until the gate is opened — so an agent is never reachable by users before coverage is proven.

When `make generate` creates the ABAC config, it also generates Genie agent config in `abac.auto.tfvars`:

| Variable | Purpose |
| -------- | ------- |
| `genie_space_title` | AI-generated title for the Genie agent |
| `genie_space_description` | Short summary of the space's scope and audience |
| `genie_sample_questions` | Conversation starters shown in the Genie UI |
| `genie_instructions` | Domain-specific guidance and business defaults |
| `genie_benchmarks` | Question + SQL pairs for evaluating Genie accuracy |
| `genie_sql_filters` | Default filters that guide Genie's SQL generation |
| `genie_sql_measures` | Standard aggregate metrics |
| `genie_sql_expressions` | Computed dimensions |
| `genie_join_specs` | Table relationships and join conditions |

All nine fields are included in the `serialized_space` when a new Genie agent is created. Review and tune them in `generated/abac.auto.tfvars` alongside the ABAC policies before applying.

## Make Targets

| Target | Description |
| ------ | ----------- |
| `make setup` | Prepare `envs/account`, `envs/<env>/data_access`, and the selected `envs/<env>` |
| `make init-env` | Explicitly bootstrap env directories and default config files |
| `make generate` | (dev) Run `generate_abac.py`: read native `class.*`, derive one `gr_treatment`/column, draft rules + Genie content (LLM drafts rules/content; sensitivity is native) |
| `make enable-classification` | Turn on UC native Data Classification (scanning) for the footprint — the as-code alternative to enabling it in the Databricks UI (recommended); auto-tagging is opt-in via `enable_auto_tagging` (default off), no generated files needed |
| `make derive-assignments` | (prod) Re-derive **only** tag assignments from live `class.*`, reusing the promoted rules — no LLM |
| `make coverage-gate` | **Block** the release if any classified sensitive column has no covering mask (separate step; not run by `apply`) |
| `make verify-access` | Prove masking/row filters by querying as per-tier test principals |
| `make validate-generated` | Validate `envs/<env>/generated/` files after tuning |
| `make validate` | Validate the selected split config (`account`, `data_access`, or `workspace`) |
| `make promote` | Split `generated/` into account + data_access + workspace configs (same-env) |
| `make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG=prod_catalog` | Cross-env promote: remap catalog references from dev to prod, then split |
| `make plan` | Run `terraform plan` in the selected layer root |
| `make apply` | For `ENV=<workspace>`: promote (same-env split), then apply account -> data_access -> workspace; releases gated access only when `business_access_enabled=true` |
| `make apply-governance` | Apply account + data_access only (enforcement; no Genie agent) |
| `make apply-genie` | Apply the workspace layer only (Genie agent + ACLs) |
| `make audit-schema` / `make audit-rulebook` | Drift checks (untagged sensitive columns / applied tags with no covering rule) |
| `make import` | Import resources into the selected layer state (`account`, `data_access`, or workspace) |
| `make migrate-state` | Move legacy state into the new module/layer addresses |
| `make destroy` | Destroy only the selected layer state |
| `make clean` | Remove generated files and Terraform state for one env directory |
| `make clean-all` | Remove all `envs/` workspaces |
| `make migrate-root-to-env` | Move an old root-based workspace into `envs/<env>/` |

Notes:

- `make plan ENV=<workspace>` assumes the referenced groups already exist — normally because they are IdP-synced (via AIM, or SCIM where AIM isn't available), or, in an opt-in demo/greenfield deployment, because `make apply ENV=account` created them with `manage_groups = true`
