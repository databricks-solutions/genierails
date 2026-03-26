# Azure Prerequisites

This guide covers Azure-specific setup required before deploying GenieRails on Azure Databricks.

## Azure Resources

1. **Azure Subscription** with an active resource group
2. **Azure AD App Registration** for Azure resource management (storage accounts, access connectors)
3. **Databricks Account** on Azure with at least one workspace

## Service Principal Setup

### Databricks Service Principal
- Must have **Account Admin** role in the Databricks account
- Configured with OAuth M2M credentials (client_id + client_secret)

### Azure Service Principal (for `provision_test_env.py`)
Required Azure RBAC roles on the resource group:
- `Contributor` — create/delete storage accounts, access connectors
- `Storage Blob Data Contributor` — manage blob data
- `User Access Administrator` — assign roles to managed identities

### Required Azure Permissions (for CI/CD)
- `Storage Blob Data Contributor` on the state storage account (for Terraform state)

## Configuration

### `account-admin.azure.env` (for integration tests)

`make setup` creates this file automatically from the example template. Fill in:

```env
# Databricks
DATABRICKS_ACCOUNT_ID       =
DATABRICKS_CLIENT_ID        =
DATABRICKS_CLIENT_SECRET    =

# Azure
AZURE_SUBSCRIPTION_ID =
AZURE_RESOURCE_GROUP  =
AZURE_REGION          = australiaeast
AZURE_TENANT_ID       =
AZURE_CLIENT_ID       =
AZURE_CLIENT_SECRET   =
```

### `auth.auto.tfvars`

The Databricks auth credentials are the same format as AWS, with two URL differences:

```hcl
databricks_account_id     = "your-account-id"
databricks_account_host   = "https://accounts.azuredatabricks.net"
databricks_client_id      = "your-sp-client-id"
databricks_client_secret  = "your-sp-secret"
databricks_workspace_id   = "your-workspace-id"
databricks_workspace_host = "https://adb-1234567890.12.azuredatabricks.net"
```

Note: Two URLs differ from AWS:
- `databricks_account_host` — Azure uses `accounts.azuredatabricks.net` (AWS uses `accounts.cloud.databricks.com`). This field is **required** for Azure; the Terraform default is the AWS URL.
- `databricks_workspace_host` — Azure uses `adb-<workspace-id>.<region-id>.azuredatabricks.net` (AWS uses `*.cloud.databricks.com`).

## Terraform State Storage

For CI/CD, create an Azure Storage Account for Terraform state:

```bash
az storage account create \
  --name mycompanytfstate \
  --resource-group my-rg \
  --location australiaeast \
  --sku Standard_LRS

az storage container create \
  --name tfstate \
  --account-name mycompanytfstate
```

Configure these as GitHub Secrets:
- `TF_STATE_STORAGE_ACCOUNT`
- `TF_STATE_CONTAINER`

## Differences from AWS

| Feature | AWS | Azure |
|---------|-----|-------|
| Account Console URL | accounts.cloud.databricks.com | accounts.azuredatabricks.net |
| Storage | S3 bucket | ADLS Gen2 (Storage Account + Container) |
| IAM for UC | IAM Role + Trust Policy | Access Connector + Managed Identity |
| State backend | S3 | Azure Blob Storage |
| Workspace URL | `*.cloud.databricks.com` | `*.azuredatabricks.net` |

See `../shared/docs/` for architecture and deployment guides that apply to both clouds.
