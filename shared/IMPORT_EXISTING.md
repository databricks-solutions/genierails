# Import Existing Resources (Overwrite / Adopt)

If groups, tag policies, tag assignments, or FGAC policies **already exist**, Terraform will fail with "already exists". Use the import workflow below so Terraform can adopt them into the correct 3-layer state.

## Prerequisites

Before running imports, ensure:

1. The relevant `envs/<layer-or-workspace>/auth.auto.tfvars` contains valid credentials.
2. The target `abac.auto.tfvars` contains the resources you want Terraform to manage.
3. You have already run `make setup` for the workspace you are working on.

## Layer ownership

- `ENV=account`: shared account groups and optional membership
- `ENV=<workspace>/data_access`: env-scoped tag policies, tag assignments, FGAC policies
- `ENV=<workspace>`: workspace-local resources such as existing Genie ACL targets

## Usage

From your cloud wrapper directory (`genie/aws/` or `genie/azure/`):

```bash
# Import account identities
make import ENV=account

# Import env-scoped governance + workspace-local resources for the default dev env
make import
make import ENV=prod

# Preview imports directly from the env directory
cd envs/account && ../../scripts/import_existing.sh --groups-only --dry-run
cd envs/dev/data_access && ../../../scripts/import_existing.sh --tags-only --dry-run
cd envs/dev/data_access && ../../../scripts/import_existing.sh --fgac-only
cd envs/dev/data_access && ../../../scripts/import_existing.sh --tag-assignments-only
```

The script reads the target env's `abac.auto.tfvars` and imports into the module address used by that layer:

- `envs/account` imports `module.account.databricks_group.*`
- `envs/<workspace>/data_access` imports `module.data_access.databricks_tag_policy.*`, `module.data_access.databricks_entity_tag_assignment.*`, and `module.data_access.databricks_policy_info.*`
- `envs/<workspace>` imports workspace-local resources through `module.workspace.*`

## Optional: reuse an existing warehouse

To use an existing warehouse instead of auto-creating one, set in the relevant `env.auto.tfvars`:

```hcl
sql_warehouse_id = "<WAREHOUSE_ID>"
```

- In `envs/<workspace>/data_access/env.auto.tfvars`, this reuses the warehouse for masking function deployment.
- In `envs/<workspace>/env.auto.tfvars`, this reuses the warehouse for the workspace layer / Genie Space.
