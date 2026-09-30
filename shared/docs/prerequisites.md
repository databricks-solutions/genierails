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

GenieRails runs **as a service principal (SP)**. There are two phases with different permission needs:

- **One-time setup** — an **Account Admin** creates the SP and grants it the roles below (creating an account SP, granting account-level roles, and creating governed tag policies all require Account Admin). Catalog authority is granted by the catalog's owner, who may be a different person. Do this manually or with `make bootstrap-sp`.
- **Running it** (`generate` / `apply` / `certify` / `verify-access`) — you authenticate *as* the SP (its OAuth credentials in `auth.auto.tfvars`), so **the person running GenieRails afterward needs no Databricks roles of their own** — only access to the SP's OAuth secret (see [Credentials](#credentials)) and network access.

The SP needs:

| Role / authority | Scope | Why |
|---|---|---|
| **Account Admin** | Account | Create groups; manage the temporary verification SPs |
| **Tag Policy Creator + Manager** | Account | Create and maintain governed tag policies |
| **Workspace Admin** | Target workspace | Deploy governance resources |
| **Authority over the target catalog** | The catalog you govern | **Own it, or** be granted `MANAGE` + `APPLY TAG` (plus `ASSIGN` on the governed tags GenieRails applies). This lets it deploy tag assignments, masking functions, FGAC policies, and grants — and self-grant its own `USE CATALOG` / `USE SCHEMA` / `EXECUTE` / `CREATE FUNCTION`. |

> The SP governs an **existing** catalog — `make apply` never creates one — so it needs authority *on that catalog*, **not** metastore `CREATE CATALOG`. (Metastore `CREATE CATALOG` matters only for greenfield/demo, where GenieRails creates a fresh catalog it then owns.)

**Create and grant the SP — two ways:**

1. **Manually** (as an **Account Admin**) — create the SP in the Account Console and grant it the account/workspace roles above; the target catalog's owner grants it `MANAGE` + `APPLY TAG`.
2. **`make bootstrap-sp`** (run by an already-authorized account admin — it can't elevate a non-admin caller). Point it at your **existing catalog** with `TARGET_CATALOG`:

   ```
   make bootstrap-sp ACCOUNT_PROFILE=<profile> ACCOUNT_ID=<id> WORKSPACE_ID=<id> SP_NAME=<name> TARGET_CATALOG=<catalog> PLAN=1
   ```

   Review the dry-run, then swap `PLAN=1` for `YES=1` to apply; it prints the one-time OAuth secret and an `auth.auto.tfvars` snippet. `TARGET_CATALOG` grants the SP `USE CATALOG`, `USE SCHEMA`, `MANAGE`, and `APPLY TAG` on that existing catalog. A pre-flight first checks the catalog exists and that *you* can grant on it (you own the catalog/metastore, or hold effective `MANAGE`); if not, it stops **before** creating the SP or minting a secret and asks you to have the catalog owner run it. One `TARGET_CATALOG` applies to every workspace in `WORKSPACE_ID` — run it separately per catalog. `make destroy` later revokes these Terraform-managed catalog grants, so the SP loses `MANAGE`/`APPLY TAG` until you re-run `bootstrap-sp` or the owner re-grants them.

   <details>
   <summary><strong>Greenfield</strong> (GenieRails creates its own catalog) — rarely needed</summary>

   Omit `TARGET_CATALOG`, and bootstrap grants the SP metastore `CREATE CATALOG` instead, so it can create and own a fresh catalog. Use this only for demo/test setups where you don't already have a catalog to govern.
   </details>

> **Genie-only mode:** if you only need Genie agents without ABAC governance, set `genie_only = true` in `env.auto.tfvars`. **Workspace Admin** is sufficient (no Account Admin or Metastore Admin); for least privilege the SP can instead have workspace **USER** + a Databricks SQL entitlement, **CAN USE** on a bring-your-own warehouse, and read access to the target tables.

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
