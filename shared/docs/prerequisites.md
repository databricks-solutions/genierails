# Prerequisites

Everything you need before running GenieRails.

## Operating System

| OS | Supported | Notes |
|----|-----------|-------|
| **Linux** | Yes | Any modern distribution |
| **macOS** | Yes | Intel or Apple Silicon |
| **Windows** | Via WSL only | Requires Windows Subsystem for Linux (bash, sed, grep needed) |

## Software

### Required

| Tool | Version | Check | Install |
|------|---------|-------|---------|
| **GNU Make** | Any | `make --version` | Preinstalled on most Linux; on macOS see the note below |
| **Python** | 3.9+ | `python3 --version` | [python.org](https://www.python.org/downloads/) |
| **Terraform** | >= 1.0 | `terraform --version` | [terraform.io](https://developer.hashicorp.com/terraform/install) |
| **Git** | Any | `git --version` | [git-scm.com](https://git-scm.com/) |

> **macOS note:** Apple's `/usr/bin/make` (and Homebrew) can be blocked by an unaccepted Xcode license — `make` then errors with a license/agreement message. If you hit that, install GNU Make another way (e.g. `conda install make`) and put it first on your `PATH`.

### Python Packages (auto-installed)

These are installed automatically when you first run `make generate` or `make apply`:

| Package | Purpose |
|---------|---------|
| `python-hcl2` | Parse Terraform HCL configurations |
| `databricks-sdk` | Databricks Python SDK |

For integration testing (`make test-ci`), cloud-specific packages are also auto-installed:

| Package | Cloud | Purpose |
|---------|-------|---------|
| `boto3` | AWS | S3 bucket and IAM role management |
| `azure-identity` | Azure | Service principal authentication |
| `azure-mgmt-storage` | Azure | Storage account management |
| `azure-mgmt-authorization` | Azure | RBAC role assignments |
| `azure-mgmt-databricks` | Azure | Workspace management |

### Terraform Providers (auto-downloaded)

Downloaded automatically on first `terraform init`:

| Provider | Version | Source |
|----------|---------|--------|
| `databricks/databricks` | ~> 1.91.0 | registry.terraform.io |
| `hashicorp/null` | ~> 3.2 | registry.terraform.io |
| `hashicorp/time` | ~> 0.12 | registry.terraform.io |

## Network Access

GenieRails requires outbound HTTPS (port 443) to:

| Endpoint | Purpose |
|----------|---------|
| `registry.terraform.io` | Download Terraform providers (first run only) |
| Your Databricks workspace URL | All API calls (generate, apply, verify) |
| `accounts.cloud.databricks.com` | AWS account API (group/tag policy management) |
| `accounts.azuredatabricks.net` | Azure account API (group/tag policy management) |

No VPN is required unless your Databricks workspace is on a private network.

## Databricks Account

### Required Features

- **Unity Catalog** — must be enabled on the target workspace
- **SQL Warehouse** — serverless (auto-created) or existing warehouse
- **Genie agents** — for the Genie agent governance workflow

### Identity Provider group sync (required)

GenieRails **consumes** the access-tier groups your identity provider owns; it does not create them in the normal path. Before running `make generate` / `make apply`, make sure your IdP groups are synced into the Databricks account:

- **AIM (Automatic Identity Management)** — the preferred path. Databricks automatically provisions users and groups from your IdP (Okta, Azure AD/Entra ID, etc.).
- **SCIM provisioning** — use where AIM isn't available for your IdP. Configure a SCIM connector from the IdP to the Databricks account.

Ownership is split: the **IdP owns groups and membership**; **GenieRails owns grants and ABAC** (tags, FGAC policies, Genie ACLs). `make generate` preflights the referenced group→tier mapping and fails loudly if a group isn't synced. See [IdP-Synced Groups](advanced.md#idp-synced-groups-default).

> **Demo / greenfield only:** if no IdP is syncing groups yet, `make generate GENERATE_ARGS='--create-groups'` plus `manage_groups = true` in `envs/account/env.auto.tfvars` lets GenieRails mint the groups itself (opt-in, off by default).

### Service Principal

Create a service principal (SP) in the Databricks Account Console with:

Alternatively, an already-authorized account admin can create and grant the deployment SP with
`make bootstrap-sp ACCOUNT_PROFILE=<profile> ACCOUNT_ID=<id> WORKSPACE_ID=<id> SP_NAME=<name> PLAN=1`.
Review the dry-run, then replace `PLAN=1` with `YES=1` to apply. The command prints the one-time
OAuth secret and an `auth.auto.tfvars` snippet; it cannot elevate a non-admin caller.

| Role | Scope | Required for |
|------|-------|-------------|
| **Account Admin** | Account | Creating groups and managing verification SPs |
| **Tag Policy Creator + Manager** | Account | Creating and maintaining governed tag policies |
| **Workspace Admin** | Target workspace | Deploying governance resources |
| **CREATE CATALOG** | Workspace's Unity Catalog metastore | Creating owned catalogs; ownership then covers grants, masks, FGAC, and tag assignments |

The bootstrap does **not** change metastore ownership or add the SP to a metastore-owner group. (To govern catalogs the SP does **not** own, that catalog's owner must grant it `MANAGE`.)

> **Genie-only mode**: If you only need Genie agents without ABAC governance,
> set `genie_only = true` in `env.auto.tfvars`. This requires only **Workspace Admin**
> (no Account Admin or Metastore Admin needed).

### Credentials

You'll need these values for `auth.auto.tfvars`:

| Credential | Where to find |
|-----------|---------------|
| `databricks_account_id` | Account Console → top-right profile menu |
| `databricks_account_host` | AWS: `https://accounts.cloud.databricks.com` / Azure: `https://accounts.azuredatabricks.net` |
| `databricks_client_id` | Account Console → User Management → Service Principals → Application ID |
| `databricks_client_secret` | Same SP → OAuth Secrets → Generate Secret |
| `databricks_workspace_id` | Account Console → Workspaces, or `?o=` parameter in workspace URL |
| `databricks_workspace_host` | Your workspace URL (e.g., `https://dbc-xxx.cloud.databricks.com`) |

## Cloud-Specific Requirements

### AWS

**Credentials** (one of):
- `AWS_PROFILE` environment variable pointing to a named profile in `~/.aws/credentials`
- `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (+ optional `AWS_SESSION_TOKEN`)
- Default boto3 credential chain (instance profile, SSO, etc.)

**IAM Permissions** (for `make test-ci` provisioning only):
- `iam:CreateRole`, `iam:DeleteRole`, `iam:PutRolePolicy`, `iam:DeleteRolePolicy`
- `s3:CreateBucket`, `s3:DeleteBucket`, `s3:PutPublicAccessBlock`
- `sts:GetCallerIdentity`

> Standard `make generate` + `make apply` usage does NOT require AWS IAM permissions —
> only a Databricks service principal.

### Azure

**Credentials** (one of):
- Service principal: `AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET` + `AZURE_TENANT_ID`
- `DefaultAzureCredential` (Azure CLI login, managed identity, etc.)

**Additional config** (for `make test-ci` provisioning only):
- `AZURE_SUBSCRIPTION_ID`
- `AZURE_RESOURCE_GROUP`
- `AZURE_REGION` (e.g., `australiaeast`)

**Azure RBAC Roles** (for provisioning only):
- `Contributor` on resource group
- `Storage Blob Data Contributor`
- `User Access Administrator`

> Standard `make generate` + `make apply` usage does NOT require Azure RBAC roles —
> only a Databricks service principal.

## Quick Verification

After installing Python and Terraform, verify your setup:

```bash
# Clone the repo
git clone https://github.com/databricks-solutions/genierails.git
cd genierails

# Pick your cloud
cd aws   # or: cd azure

# Copy and fill in credentials
cp shared/auth.auto.tfvars.example envs/dev/auth.auto.tfvars
# Edit envs/dev/auth.auto.tfvars with your credentials

# Verify connectivity
make setup ENV=dev
make validate ENV=dev
```

If `make validate` shows all `[PASS]` checks, you're ready to go.
See [From UI to Production](from-ui-to-production.md) or [Quickstart](quickstart.md) for next steps.
