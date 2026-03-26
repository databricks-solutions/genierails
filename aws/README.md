# GenieRails — AWS

> **On Azure?** Go to [`../azure/README.md`](../azure/README.md) instead.

## Prerequisites

- Your tables must already exist in Unity Catalog before running `make generate`
- An AWS Databricks workspace with Unity Catalog enabled
- A Databricks service principal with the roles below

### Which service principal roles do I need?

| Mode | Role | Why it's needed |
| ---- | ---- | --------------- |
| Full (default) | **Account Admin** | Create groups, assign groups to workspaces, manage group membership |
| Full (default) | **Workspace Admin** | Grant entitlements, create warehouses, manage Genie Spaces and permissions |
| Full (default) | **Metastore Admin** | Create tag policies, FGAC policies, grants, and masking functions |
| Genie-only | **Workspace USER** + **Databricks SQL access** entitlement | Create Genie Spaces only — set `genie_only = true` and provide `sql_warehouse_id` in `env.auto.tfvars`. No admin roles needed. |

## Step 1 — Set up your environment

```bash
cd aws/     # always run from here, never from shared/
make setup
```

This creates `envs/dev/` with two template files for you to fill in.

## Step 2 — Fill in credentials

Edit `envs/dev/auth.auto.tfvars`:

```hcl
databricks_account_id     = "your-account-id"
databricks_client_id      = "your-sp-client-id"
databricks_client_secret  = "your-sp-secret"
databricks_workspace_id   = "your-workspace-id"
databricks_workspace_host = "https://dbc-xxxxxxxx-xxxx.cloud.databricks.com"
```

> **Note:** No `databricks_account_host` is needed for AWS — the Terraform provider defaults to `accounts.cloud.databricks.com`.

## Step 3 — Follow the guide for your scenario

| Starting point | You have... | Guide |
|---|---|---|
| **I already have a Genie Space** | A space configured in the Databricks UI that needs governance and promotion to prod | [From UI to Production](../shared/docs/from-ui-to-production.md) |
| **I'm starting from scratch** | Tables in Unity Catalog, no Genie Space yet | [Quickstart](../shared/docs/quickstart.md) |

---

## Testing

```bash
make test-unit   # unit tests — ~1 second, no credentials required
make test-ci     # full CI pipeline: provision → integration tests → teardown
```

See [Integration Testing](../shared/docs/integration-testing.md) for setup and credentials.

## Documentation

- [From UI to Production](../shared/docs/from-ui-to-production.md) — import your existing Genie Space, add governance, promote to prod
- [Quickstart](../shared/docs/quickstart.md) — create a Genie Space from scratch
- [Playbook](../shared/docs/playbook.md) — after first deployment: add spaces, promote, overlays, advanced scenarios
- [Architecture](../shared/docs/architecture.md) — layers, artifact ownership, config files, Genie Space lifecycle
- [All documentation](../shared/docs/) — full list
