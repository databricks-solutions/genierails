# GenieRails — Azure

> **On AWS?** Go to [`../aws/README.md`](../aws/README.md) instead.

GenieRails gets business users into Genie quickly. You provide a list of Unity Catalog tables and a Genie Space name. GenieRails:

1. Reads your table schemas and uses AI to generate ABAC groups, row-level security policies, and data masking functions tailored to your data
2. Lets you review and adjust everything before anything is deployed
3. Deploys all components to your Azure Databricks workspace with a single `make apply`

You never write Terraform. You never manually configure Unity Catalog grants or Genie Space permissions.

## What Gets Deployed

After `make apply`, your workspace will have:

| Component | What it is |
| --------- | ---------- |
| **Account groups** | Unity Catalog groups scoped to your Genie Space (e.g. `sales-analyst`, `sales-manager`) |
| **UC grants** | `USE CATALOG`, `USE SCHEMA`, `SELECT` grants on the tables you listed |
| **Tag policies** | Column-level sensitivity tags (PII, confidential, etc.) |
| **FGAC policies** | Row-level access policies so each group sees only the rows it's allowed to |
| **Masking functions** | SQL UDFs that mask or redact sensitive columns for lower-privilege groups |
| **SQL warehouse** | A serverless SQL warehouse for Genie to run queries (auto-created unless you point to an existing one) |
| **Genie Space** | The Genie Space itself, configured with your tables and group permissions |

## Prerequisites

- Your tables must already exist in Unity Catalog before running `make generate`
- An Azure Databricks workspace with Unity Catalog enabled
- A Databricks service principal with the roles below

### Which service principal roles do I need?

| Mode | Role | Why it's needed |
| ---- | ---- | --------------- |
| Full (default) | **Account Admin** | Create groups, assign groups to workspaces, manage group membership |
| Full (default) | **Workspace Admin** | Grant entitlements, create warehouses, manage Genie Spaces and permissions |
| Full (default) | **Metastore Admin** | Create tag policies, FGAC policies, grants, and masking functions |
| Genie-only | **Workspace USER** + **Databricks SQL access** entitlement | Create Genie Spaces only — set `genie_only = true` and provide `sql_warehouse_id` in `env.auto.tfvars`. Also requires `CAN USE` on the warehouse and UC table access (`USE CATALOG`, `USE SCHEMA`, `SELECT`) granted by the governance team. No admin roles needed. |

**Not sure which mode to use?**

- **Full mode** is the default and handles everything end-to-end: GenieRails creates the UC groups, sets up row-level security and masking policies, and builds the Genie Space. Use this if you are starting from scratch or your governance team is comfortable giving the service principal admin roles.
- **Genie-only mode** is for teams where UC governance (groups, grants, policies) is already managed separately — for example, by a central platform team. GenieRails only creates and configures the Genie Space. The service principal needs no admin roles, but UC access must be pre-granted by whoever manages your governance layer. Enable it with `genie_only = true` in `env.auto.tfvars`.

For Azure-specific resource setup (Azure AD App Registration, RBAC roles, storage accounts), see [Azure Prerequisites](docs/azure-prerequisites.md).

## Quickstart

### Step 1 — Set up your environment

```bash
cd azure/     # always run from here, never from shared/
make setup
```

This creates `envs/dev/` with two template files for you to fill in.

### Step 2 — Fill in credentials

Edit `envs/dev/auth.auto.tfvars`:

```hcl
databricks_account_id     = "your-account-id"
databricks_account_host   = "https://accounts.azuredatabricks.net"
databricks_client_id      = "your-sp-client-id"
databricks_client_secret  = "your-sp-secret"
databricks_workspace_id   = "your-workspace-id"
databricks_workspace_host = "https://adb-1234567890.12.azuredatabricks.net"
```

> **Azure-specific URLs:** Both URLs differ from AWS and must be set explicitly.
> - `databricks_account_host` must be `accounts.azuredatabricks.net` (the Terraform provider default is the AWS URL)
> - `databricks_workspace_host` uses the Azure format: `adb-<workspace-id>.<region-id>.azuredatabricks.net`

### Step 3 — Configure your Genie Space

Edit `envs/dev/env.auto.tfvars`. Choose one of:

**Already have a Genie Space?** (recommended) — import it by ID:

```hcl
genie_spaces = [
  {
    genie_space_id = "01ef7b3c2a4d5e6f"   # find this in the Genie Space URL
  },
]
```

Tables, instructions, and benchmarks are discovered automatically from the API.

**Starting from scratch?** — define tables for a new space:

```hcl
genie_spaces = [
  {
    name      = "Sales Analytics"
    uc_tables = [
      "dev_catalog.sales.orders",
      "dev_catalog.sales.customers",
      "dev_catalog.finance.*",   # wildcard — includes all tables in the schema
    ]
  },
]
```

All table names must be fully qualified (`catalog.schema.table` or `catalog.schema.*`). A serverless SQL warehouse is created automatically — see the [Playbook](../shared/docs/playbook.md) for warehouse and multi-space options.

### Step 4 — Generate

```bash
make generate
```

GenieRails inspects your table schemas and uses AI to produce two files:

- `envs/dev/generated/abac.auto.tfvars` — groups, row-filter policies, Genie Space configuration
- `envs/dev/generated/masking_functions.sql` — SQL UDFs for column masking

**Review both files before continuing.** This is your chance to adjust group names, tweak row-filter conditions, or remove masking rules you don't need.

### Step 5 — Validate and apply

```bash
make validate-generated   # checks for schema drift and config errors
make apply                # deploys everything to your workspace
```

`make apply` runs in three phases:

1. Account-level groups and optional group membership
2. UC grants, tag policies, FGAC policies, and masking functions
3. Genie Space creation, configuration, and group permissions

## Next Steps

**Promote `dev` to `prod`:**

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP="dev_catalog=prod_catalog"
vi envs/prod/auth.auto.tfvars
make apply ENV=prod
```

**Add a separate environment (different BU, different tables):**

```bash
make setup ENV=bu2 && vi envs/bu2/auth.auto.tfvars && vi envs/bu2/env.auto.tfvars
make generate ENV=bu2 && make apply ENV=bu2
```

## How It Works (Architecture)

GenieRails uses a shared module architecture. All Terraform modules, scripts, and Python tools live in `../shared/`. This `azure/` directory is a thin wrapper — you never edit anything in `shared/` directly.

```
genierails/
├── azure/          ← you work here
│   ├── Makefile    sets CLOUD=azure, delegates everything to shared/Makefile.shared
│   ├── envs/       your per-environment configs (auth, tables, generated artifacts)
│   └── docs/       Azure-specific prerequisites
└── shared/         all logic lives here — never run commands directly from here
```

For AWS, see [`../aws/README.md`](../aws/README.md).

## Testing

```bash
make test-unit   # unit tests — ~1 second, no credentials required
make test-ci     # full CI pipeline: provision → integration tests → teardown
```

For integration tests, configure Azure credentials:

```bash
# make setup creates ../shared/scripts/account-admin.azure.env automatically.
# Fill in your Databricks and Azure credentials before running test-ci.
vi ../shared/scripts/account-admin.azure.env
```

See [Integration Testing](../shared/docs/integration-testing.md) for setup, credentials, scenarios, and troubleshooting.

## Documentation

- [Azure Prerequisites](docs/azure-prerequisites.md) — Azure-specific resource setup, RBAC roles, storage accounts
- [Playbook](../shared/docs/playbook.md) — start here: import your existing Genie Space, add governance, promote to prod (also covers quickstart, advanced scenarios)
- [Architecture](../shared/docs/architecture.md) — layers, artifact ownership, config files, Genie Space lifecycle
- [Central Governance, Self-Service Genie](../shared/docs/self-service-genie.md) — central ABAC team + BU teams self-serve Genie spaces
- [CI/CD Integration](../shared/docs/cicd.md) — validate and deploy from a pipeline
- [Troubleshooting](../shared/docs/troubleshooting.md) — imports, provider quirks, brownfield workflows
- [Advanced Usage](../shared/docs/advanced.md) — IDP-synced groups, ABAC-only mode, masking UDF reuse, legacy migration
- [Integration Testing](../shared/docs/integration-testing.md) — unit tests, integration scenarios, test data

## Roadmap

- Genie Workbench integration
- Telemetry enablement
- Full schema evolution support
