# Central Governance, Self-Service Genie

This document covers the self-service Genie operating model, where a central **Data Governance team** owns ABAC policies and groups, while independent **BU teams** self-serve their own Genie spaces.

For the quick step-by-step, see [playbook.md — Central governance, self-service Genie](playbook.md#central-governance-self-service-genie). This document covers the reasoning, Git strategies, CI/CD integration, and FAQ.

---

## When to use this pattern

| Situation | Recommended mode |
| --------- | ---------------- |
| Single team controls data access and Genie spaces end-to-end | `make generate` + `make apply` (default, no change) |
| Central governance team + BU teams creating Genie spaces | `MODE=governance` / `apply-governance` + `MODE=genie` / `apply-genie` |
| Two independent BU teams, each owning ABAC for their own catalogs | `make generate` per-BU + `abac_managed_catalogs` (see [advanced.md](advanced.md)) |

Use the self-service Genie pattern when:

- Your organization has a dedicated Data Governance or Data Platform team that standardizes access policies across the company.
- Business units want self-service Genie space creation without needing governance expertise.
- You want to prevent BU teams from accidentally modifying ABAC policies, tag assignments, or masking functions.
- Different teams have different deployment cadences (governance policies change rarely; Genie spaces evolve quickly).

---

## Architecture

The three Terraform layers are already independent states. The self-service Genie mode exposes them as separate operational roles:

```
Account layer      →  Groups + Tag Policies
                        owned by: Governance team
                        applied by: make apply-governance (or make apply ENV=account)

Data Access layer  →  Tag Assignments + FGAC Policies + Masking Functions + Catalog Grants
                        owned by: Governance team
                        applied by: make apply-governance ENV=<env>

Workspace layer    →  Workspace Assignment + Entitlements + Genie Spaces + ACLs
                        owned by: BU team
                        applied by: make apply-genie ENV=<bu-env>
```

The workspace module (`modules/workspace/main.tf`) looks up groups by name — it never creates them. This means BU teams can reference groups created by the governance team without any additional coordination.

Catalog grants (`USE_CATALOG`, `USE_SCHEMA`, `SELECT`) are applied by the governance team's data_access layer. Once in place, BU teams' Genie spaces can query those catalogs immediately.

---

## Roles and responsibilities

### Central Data Governance team

Owns and runs:

- `envs/account/` — account groups, tag policy definitions
- `envs/<env>/data_access/` — tag assignments, FGAC policies, masking functions, catalog grants

What they commit to Git:
- `envs/account/abac.auto.tfvars` — groups and tag policies
- `envs/<env>/data_access/abac.auto.tfvars` — tag assignments and FGAC policies
- `envs/<env>/data_access/masking_functions.sql`

Commands they run:
```bash
make generate ENV=<env> MODE=governance   # LLM generates ABAC config only
make apply-governance ENV=<env>           # applies account + data_access
make destroy-governance ENV=<env>         # tears down data_access only
```

### BU teams (one per business unit)

Own and run their workspace env only:

- `envs/<bu-env>/env.auto.tfvars` — Genie space definitions (tables, warehouse)
- `envs/<bu-env>/abac.auto.tfvars` — genie_space_configs (instructions, benchmarks, etc.)

What they commit to Git:
- `envs/<bu-env>/env.auto.tfvars`
- `envs/<bu-env>/abac.auto.tfvars` (after promote)
- `envs/<bu-env>/auth.auto.tfvars` (gitignored — contains credentials)

Commands they run:
```bash
make generate ENV=<bu-env> MODE=genie   # LLM generates genie_space_configs only
make apply-genie ENV=<bu-env>           # applies workspace layer only
make destroy-genie ENV=<bu-env>         # tears down workspace layer only
```

---

## Least-privilege service principal for BU teams

By default, the workspace layer looks up groups at the account level, which requires the SP to have Account Admin. In self-service Genie mode, BU teams can use a SP with **only workspace USER membership and the Databricks SQL access entitlement** by setting `genie_only = true`. No admin roles are needed.

### Setup

1. Create a service principal and assign it to the workspace as a **USER** (not Admin). Grant it the **Databricks SQL access** entitlement.

2. Grant the BU team's SP `CAN USE` on an existing SQL warehouse. The SP cannot create warehouses without admin privileges, so a BYO warehouse is required.

3. Grant the BU team's SP access to the governed UC tables. The Genie API validates table access at space creation time, so the governance team must run these grants:
   ```sql
   GRANT USE CATALOG ON CATALOG <catalog> TO `<bu-sp-application-id>`;
   GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<bu-sp-application-id>`;
   GRANT SELECT ON SCHEMA <catalog>.<schema> TO `<bu-sp-application-id>`;
   ```

4. Configure the BU team's `envs/<bu-env>/auth.auto.tfvars`:
   ```hcl
   # databricks_account_id is not needed in genie_only mode
   databricks_account_id    = ""
   databricks_client_id     = "<bu-sp-client-id>"
   databricks_client_secret = "<bu-sp-secret>"
   databricks_workspace_id  = "<workspace-id>"
   databricks_workspace_host = "https://<workspace>.cloud.databricks.com/"  # or https://adb-<id>.<region>.azuredatabricks.net for Azure
   ```

5. Set `genie_only = true` in `envs/<bu-env>/env.auto.tfvars`:
   ```hcl
   genie_only = true

   genie_spaces = [
     {
       name      = "Finance Analytics"
       uc_tables = ["prod_catalog.finance.transactions", "prod_catalog.finance.customers"]
     },
   ]

   sql_warehouse_id = "<existing-warehouse-id>"   # required — BYO warehouse
   ```

6. Ensure `envs/<bu-env>/abac.auto.tfvars` has **empty groups** (or no groups block):
   ```hcl
   groups = {}
   genie_space_configs = { ... }
   ```

7. Generate and apply:
   ```bash
   make generate ENV=<bu-env> MODE=genie
   make apply-genie ENV=<bu-env>
   ```

### What changes with genie_only = true

| Resource | Full mode | Genie-only mode |
| -------- | --------- | --------------- |
| Account group lookup | Yes (account API) | Skipped |
| Workspace group assignment | Yes (account API) | Skipped |
| Group entitlements | Yes (workspace API) | Skipped |
| SQL warehouse | Auto-create or BYO | BYO only (`sql_warehouse_id` required) |
| Genie Space create/config | Yes | Yes |
| Genie Space ACLs | Yes (per group) | Skipped (no groups) |
| UC table access | Implicit (SP is metastore admin) | Explicit grants required (step 3) |
| SP role required | Account Admin + Workspace Admin + Metastore Admin | Workspace USER + SQL entitlement |

The governance team manages groups, workspace assignments, entitlements, warehouses, UC grants, and Genie Space ACLs via `make apply-governance`. The BU team only manages Genie Space creation and configuration.

> **Tested:** The `genie-only` integration test (`make test-genie-only`) creates a minimal-privilege SP with only workspace USER + SQL entitlement (no admin roles), grants it CAN USE on a warehouse and UC table access, and verifies the full `genie_only = true` flow end-to-end — including confirming that zero account-level resources appear in Terraform state.

---

## Git repository strategies

### Mono-repo (recommended for simplicity)

All environments live under `envs/` in a single repository. Use directory-level CODEOWNERS to enforce ownership:

```
# .github/CODEOWNERS
envs/account/           @data-governance-team
envs/*/data_access/     @data-governance-team
envs/bu_finance/        @bu-finance-team
envs/bu_clinical/       @bu-clinical-team
```

### Split repos (advanced)

Governance team maintains a separate repo containing `envs/account/` and `envs/*/data_access/`. BU teams maintain their own repos containing only their workspace envs. Use a shared `auth.auto.tfvars` symlink or CI secret injection to avoid credential duplication.

---

## CI/CD integration

See [cicd.md](cicd.md) for general CI/CD setup. For self-service Genie mode:

**Governance pipeline** (triggered by changes to `envs/account/` or `envs/*/data_access/`):
```yaml
- run: make apply-governance ENV=prod
```

**BU pipeline** (triggered by changes to `envs/bu_finance/`):
```yaml
- run: make apply-genie ENV=bu_finance
```

Each pipeline only touches its own Terraform state files. The governance pipeline never writes to `envs/bu_*/terraform.tfstate`. The BU pipeline never writes to `envs/*/data_access/terraform.tfstate`.

---

## Promotion in self-service Genie mode

BU teams can promote their Genie spaces from dev to prod using a modified flow:

```bash
# Promote genie_space_configs from bu_finance_dev to bu_finance_prod:
make promote SOURCE_ENV=bu_finance_dev DEST_ENV=bu_finance_prod \
  DEST_CATALOG_MAP="dev_catalog=prod_catalog"

# Apply only the workspace layer in prod:
make apply-genie ENV=bu_finance_prod
```

Governance runs separately for the prod environment — the promotion only carries `genie_space_configs`, not ABAC.

### Import an existing Genie Space to prod (no ABAC)

If a data team has already created a Genie Space in the UI and you want to bring it to prod without managing any ABAC governance (because a central team handles that separately), use the genie-only import workflow:

```bash
# 1. Set up a new env
make setup ENV=bu_import

# 2. Write env.auto.tfvars with genie_only = true and the existing space's ID
cat > envs/bu_import/env.auto.tfvars <<'EOF'
genie_only = true

genie_spaces = [
  {
    name           = "Finance Analytics"
    genie_space_id = "<existing-space-id>"
    uc_tables = [
      "dev_catalog.finance.customers",
      "dev_catalog.finance.transactions",
    ]
  },
]

sql_warehouse_id = "<warehouse-id>"
EOF

# 3. Generate genie config only (no ABAC)
make generate ENV=bu_import MODE=genie

# 4. Apply workspace layer
make apply-genie ENV=bu_import

# 5. Promote to prod — remaps genie config or gracefully skips ABAC
make promote SOURCE_ENV=bu_import DEST_ENV=bu_import_prod \
  DEST_CATALOG_MAP="dev_catalog=prod_catalog"

# 6. If promote skipped (no generated/abac.auto.tfvars), set up prod manually:
make setup ENV=bu_import_prod
# Write prod env.auto.tfvars with remapped catalogs, then:
make generate ENV=bu_import_prod MODE=genie
make apply-genie ENV=bu_import_prod
```

> **Tested:** The `genie-import-no-abac` integration test (`make test-genie-import-no-abac`) validates this exact workflow end-to-end — creating a space via API, importing it with `genie_only=true`, generating with `MODE=genie`, promoting to prod, and asserting that no governance artifacts (tags, FGAC policies, masking functions) are produced at any stage.

---

## FAQ

**What if a BU needs a new group?**

New groups must be requested from the governance team. The governance team adds the group to `envs/account/abac.auto.tfvars`, runs `make apply-governance`, and the group becomes available for BU teams to reference in their Genie space ACLs. BU teams can then add the group name to their `env.auto.tfvars` (genie_spaces ACLs) and `make apply-genie`.

**Can a BU team see what groups are available?**

Yes — the group names are in `envs/account/abac.auto.tfvars`. In `genie` mode, `make generate` auto-loads those names and includes them in the prompt so the LLM suggests the right group references.

**Can a BU team run `make apply` (full) instead of `make apply-genie`?**

Yes — `make apply` applies all three layers. In self-service Genie mode this is safe if the BU env's `data_access/abac.auto.tfvars` only contains what the BU owns (e.g., just group lookups, no tag_assignments). However, `make apply-genie` is the recommended guard — it makes the role boundary explicit and prevents accidental ABAC changes.

**What if we want to move from centralized to self-service Genie?**

1. Identify which tables/catalogs the governance team will own.
2. The governance team adopts the existing `data_access` Terraform state — no state migration needed.
3. BU teams' workspace states are already separate — they switch to `make apply-genie`.
4. Remove any ABAC config from BU teams' `envs/<bu-env>/abac.auto.tfvars` (it should only contain `genie_space_configs` and `groups` lookup).

**What if the BU team references a table that's not governed?**

`make apply-genie` will succeed (the workspace layer doesn't check catalog grants), but Genie queries against ungoverned tables will fail at query time due to missing `SELECT` grants. The governance team must add the tables to their governed set and re-run `make apply-governance`.

**Can two BUs share the same Genie space?**

No — a Genie space is workspace-specific. If two BUs need the same data surface, each creates their own Genie space pointing at the same governed tables. The ABAC governance applies identically to both since it's catalog-level.
