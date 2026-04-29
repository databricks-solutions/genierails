# Guided Setup Workflow

## Step 1: Ensure Correct Directory

Check if the user is in `aws/` or `azure/`. If they're at the repo root:
```
GenieRails commands must run from a cloud directory.
Run `cd aws` for AWS or `cd azure` for Azure.
```

## Step 2: Bootstrap Environment

```bash
make setup
```

This creates:
- `envs/account/` — shared account layer (groups, tag policies)
- `envs/dev/` — workspace environment
- `envs/dev/data_access/` — governance layer
- Template files: `auth.auto.tfvars`, `env.auto.tfvars`

## Step 3: Configure Credentials

### Auto-Detection

Check these sources in order for existing Databricks credentials:

1. **Environment variables**: check for `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, `DATABRICKS_ACCOUNT_ID`
2. **Databricks CLI config**: read `~/.databrickscfg` for profiles containing `host`, `client_id`, or `token`
3. **Existing auth file**: check if `envs/dev/auth.auto.tfvars` already has non-empty values

If credentials are found, offer to populate `auth.auto.tfvars` automatically. If not, guide the user through finding each value.

### Required Fields

| Field | Where to find it |
|---|---|
| `databricks_account_id` | Account Console → top-right profile menu → account ID |
| `databricks_client_id` | Account Console → User management → Service principals → Application ID |
| `databricks_client_secret` | Same SP → OAuth secrets → Create new secret |
| `databricks_workspace_id` | Workspace URL contains `?o=<workspace_id>` |
| `databricks_workspace_host` | Your workspace URL, e.g. `https://dbc-xxx.cloud.databricks.com` |

### Azure-Specific

For Azure, always add:
```hcl
databricks_account_host = "https://accounts.azuredatabricks.net"
```
This is REQUIRED. Without it, the SDK defaults to the AWS account host and all account-level operations fail silently.

### Write and Secure

After populating `envs/dev/auth.auto.tfvars`, set permissions:
```bash
chmod 600 envs/dev/auth.auto.tfvars
```

## Step 4: Configure Environment

Edit `envs/dev/env.auto.tfvars` to define Genie Spaces:

### New Space (most common)
```hcl
genie_spaces = [
  {
    name      = "Sales Analytics"
    uc_tables = [
      "dev_catalog.sales.orders",
      "dev_catalog.sales.customers",
    ]
  },
]
```

### Import Existing Space
```hcl
genie_spaces = [
  {
    genie_space_id = "01ef7b3c2a4d5e6f"  # from the Genie Space URL
    # name and uc_tables auto-discovered from Genie API during make generate
  },
]
```

Ask the user:
- "Which Unity Catalog tables should this Genie Space cover?" (must be fully qualified: `catalog.schema.table` or `catalog.schema.*`)
- "Do you have an existing Genie Space to import, or are you creating a new one?"

## Step 5: Validate Connectivity

Test that credentials work:
```bash
python3 -c "
import os
os.environ['DATABRICKS_HOST'] = '$(grep databricks_workspace_host envs/dev/auth.auto.tfvars | cut -d'\"' -f2)'
os.environ['DATABRICKS_CLIENT_ID'] = '$(grep databricks_client_id envs/dev/auth.auto.tfvars | cut -d'\"' -f2)'
os.environ['DATABRICKS_CLIENT_SECRET'] = '$(grep databricks_client_secret envs/dev/auth.auto.tfvars | cut -d'\"' -f2)'
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
print(f'Connected as: {w.current_user.me().user_name}')
"
```

If this fails, check the error:
- `invalid_client` → wrong client_id or client_secret
- `404` or connection error → wrong workspace_host
- `permission denied` → SP needs Account Admin + Workspace Admin + Metastore Admin

## Step 6: Next Steps

"Setup complete! Your environment is configured at `envs/dev/`. Next steps:
1. Run `make generate` to create ABAC governance config (or `/genierails generate`)
2. Review the generated files in `envs/dev/generated/`
3. Run `make validate-generated` then `make apply` to deploy"

## Example Interaction Flow

```
User: "help me set up genierails"

1. Detect cwd is aws/
2. Run `make setup` → creates envs/ structure
3. Check ~/.databrickscfg → find DEFAULT profile with host
4. Ask: "I found workspace https://dbc-xxx.cloud.databricks.com. What's the client_id for your service principal?"
5. User provides client_id + secret
6. Write auth.auto.tfvars, test connectivity → "Connected as: my-sp@..."
7. Ask: "Which tables should this Genie Space cover? (e.g. catalog.schema.table)"
8. User: "dev_fin.finance.transactions, dev_fin.finance.customers"
9. Write env.auto.tfvars with genie_spaces
10. "Setup complete! Run /genierails generate to create governance config."
```
