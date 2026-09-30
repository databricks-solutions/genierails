# Prerequisites

Everything you need before running GenieRails. Work through these checks in order; each section is collapsed by default so you can see the full checklist at a glance.

<a id="operating-system"></a>
<details>
<summary><strong>Check 1 — Operating system is supported</strong></summary>

**Requirements:**

| OS | Supported | Notes |
|----|-----------|-------|
| **Linux** | Yes | Any modern distribution |
| **macOS** | Yes | Intel or Apple Silicon |
| **Windows** | Via WSL only | Requires Windows Subsystem for Linux (bash, sed, grep needed) |

</details>

---

<a id="software"></a>
<details>
<summary><strong>Check 2 — Required local software is installed</strong></summary>

**Requirements:**

| Tool | Version | Check | Install |
|------|---------|-------|---------|
| **GNU Make** | Any | `make --version` | Preinstalled on most Linux; on macOS see the note below |
| **Python** | 3.9+ | `python3 --version` | [python.org](https://www.python.org/downloads/) |
| **Terraform** | >= 1.0 | `terraform --version` | [terraform.io](https://developer.hashicorp.com/terraform/install) |
| **Git** | Any | `git --version` | [git-scm.com](https://git-scm.com/) |

> **macOS note:** Apple's `/usr/bin/make` (and Homebrew) can be blocked by an unaccepted Xcode license — `make` then errors with a license/agreement message. If you hit that, install GNU Make another way (e.g. `conda install make`) and put it first on your `PATH`.

<details>
<summary><strong>Details — Auto-installed dependencies</strong></summary>

GenieRails auto-installs its Python packages (first `make generate` / `make apply`) and auto-downloads its Terraform providers (first `terraform init`) — nothing to install by hand for the core flow. (`make test-ci` additionally needs `pytest`; see the note below.)

**Python packages** — on first `make generate` or `make apply`:

| Package | Purpose |
|---------|---------|
| `python-hcl2` | Parse Terraform HCL configurations |
| `databricks-sdk` | Databricks Python SDK |
| `pyyaml` | Parse YAML overlay / config files |

For integration testing (`make test-ci`), cloud-specific packages are also auto-installed:

| Package | Cloud | Purpose |
|---------|-------|---------|
| `boto3` | AWS | S3 bucket and IAM role management |
| `azure-identity` | Azure | Service principal authentication |
| `azure-mgmt-storage` | Azure | Storage account management |
| `azure-mgmt-authorization` | Azure | RBAC role assignments |
| `azure-mgmt-databricks` | Azure | Workspace management |

> **`make test-ci`** also needs **`pytest`** (and `python-hcl2`) present — these are *not* auto-installed. Run `pip install pytest python-hcl2` first.

**Terraform providers** — on first `terraform init`:

| Provider | Version | Source |
|----------|---------|--------|
| `databricks/databricks` | ~> 1.111.0 | registry.terraform.io |
| `hashicorp/null` | ~> 3.2 | registry.terraform.io |
| `hashicorp/time` | ~> 0.12 | registry.terraform.io |
</details>

</details>

---

<a id="network-access"></a>
<details>
<summary><strong>Check 3 — Required network endpoints are reachable</strong></summary>

**Requirements:**

GenieRails requires outbound HTTPS (port 443) to:

| Endpoint | Purpose |
|----------|---------|
| `github.com` | Clone the repository |
| `pypi.org` (or your Python package index) | Auto-install Python packages (first run) |
| `registry.terraform.io` | Download Terraform providers (first run only) |
| Your Databricks workspace URL | All API calls (generate, apply, verify) |
| `accounts.cloud.databricks.com` | AWS account API (group/tag policy management) |
| `accounts.azuredatabricks.net` | Azure account API (group/tag policy management) |

No VPN is required unless your Databricks workspace is on a private network. (`make test-ci` also reaches your cloud's management endpoints, e.g. `management.azure.com`.)

</details>

---

<a id="required-features"></a>
<details>
<summary><strong>Check 4 — Required Databricks features are enabled</strong></summary>

**Requirements:**

- **Unity Catalog** — must be enabled on the target workspace
- **SQL Warehouse** — serverless (auto-created) or existing warehouse
- **Genie agents** — for the Genie agent governance workflow

</details>

---

<a id="identity-provider-group-sync-required"></a>
<details>
<summary><strong>Check 5 — Identity provider groups are synced</strong></summary>

**Requirements:**

GenieRails **consumes** the access-tier groups your identity provider owns; it does not create them in the normal path. Before running `make generate` / `make apply`, make sure your IdP groups are synced into the Databricks account:

- **AIM (Automatic Identity Management)** — the preferred path. Databricks automatically provisions users and groups from your IdP (Okta, Azure AD/Entra ID, etc.).
- **SCIM provisioning** — use where AIM isn't available for your IdP. Configure a SCIM connector from the IdP to the Databricks account.

Ownership is split: the **IdP owns groups and membership**; **GenieRails owns grants and ABAC** (tags, FGAC policies, Genie ACLs). `make generate` preflights the referenced group→tier mapping and fails loudly if a group isn't synced. See [IdP-Synced Groups](advanced.md#idp-synced-groups-default).

<details>
<summary><strong>Alternative — Demo/greenfield group creation</strong></summary>

`make generate GENERATE_ARGS='--create-groups'` plus `manage_groups = true` in `envs/account/env.auto.tfvars` lets GenieRails mint the groups itself (opt-in, off by default). Use this only for a demo/greenfield account — prefer AIM/SCIM sync (above) for anything real.
</details>

</details>

---

<a id="service-principal"></a>
<details>
<summary><strong>Check 6 — Service principal has the required authority</strong></summary>

**Requirements:**

GenieRails runs **as a service principal (SP)**. Confirm both identities involved:

- **Setup actors** — an Account Admin creates the SP and assigns its account/workspace roles; the target catalog's owner grants catalog authority. One person can perform both parts when they hold both authorities; otherwise the two owners coordinate. The automated method requires a caller with both.
- **Runtime identity** — `generate`, `apply`, `certify`, and `verify-access` authenticate as the SP using `auth.auto.tfvars`. The person invoking those commands needs the SP's OAuth secret (see [Credentials](#credentials)) and network access, but no additional personal Databricks roles.

The SP needs:

| Role / authority | Scope | Why |
|------------------|-------|-----|
| **Account Admin** | Account | Create groups when explicitly requested; create, assign, and delete the per-tier test SPs used by live `verify-access` |
| **Tag Policy Creator + Manager** | Account | Create and maintain governed tag policies |
| **Workspace Admin** | Target workspace | Deploy governance resources |
| **Authority over the target catalog** | The catalog you govern | **Own it, or** be granted `MANAGE` + `APPLY TAG` (plus `ASSIGN` on the governed tags GenieRails applies). This lets it deploy tag assignments, masking functions, FGAC policies, and grants — and self-grant its own `USE CATALOG` / `USE SCHEMA` / `EXECUTE` / `CREATE FUNCTION`. |
| **Query the model serving endpoint** | Workspace | `CAN QUERY` on `databricks-claude-sonnet-4-6` — generation calls a foundation model (an external Anthropic/OpenAI provider works too). |

<details>
<summary><strong>Details — Per-tier test service principals</strong></summary>

Databricks cannot impersonate a user for a query, so live `verify-access` creates one dedicated service principal for each access tier, adds it to that tier's group, and runs the same SQL using each principal's own OAuth credentials. This proves that authorized tiers see raw values while restricted tiers see masked values and filtered rows. The test SPs are deleted automatically when verification finishes unless `KEEP_PRINCIPALS=1` is set for debugging. They are separate from the GenieRails deployment SP.

</details>

<details>
<summary><strong>Details — Existing catalog authority</strong></summary>

The SP governs an **existing** catalog — `make apply` never creates one — so it needs authority *on that catalog*, **not** metastore `CREATE CATALOG`. Metastore `CREATE CATALOG` matters only for greenfield/demo, where GenieRails creates a fresh catalog it then owns.

</details>

**Provision the SP — choose one method:**

1. **Manually** — the Account Admin creates the SP in the Account Console and assigns the account and workspace roles in the table above. The target catalog's owner grants it `MANAGE` + `APPLY TAG`.
2. **With `make bootstrap-sp`** — an already-authorized Account Admin runs the command below. It cannot elevate a non-admin caller.

   `ACCOUNT_PROFILE` is the name of a Databricks CLI profile for the **bootstrap caller**, not the deployment SP. [Install the Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) if needed, then create the profile below (on Azure, use `https://accounts.azuredatabricks.net` as the host):

   ```bash
   databricks auth login \
     --host https://accounts.cloud.databricks.com \
     --account-id <account-id> \
     --skip-workspace \
     --profile genierails-bootstrap
   ```

   ```bash
   make bootstrap-sp ACCOUNT_PROFILE=genierails-bootstrap ACCOUNT_ID=<id> WORKSPACE_ID=<id> SP_NAME=<name> TARGET_CATALOG=<catalog> PLAN=1
   ```

   | Parameter | Required | Value / where to find it |
   |-----------|----------|--------------------------|
   | `ACCOUNT_PROFILE` | No | Profile name in `~/.databrickscfg`; defaults to `DEFAULT`. Use the Account Admin profile created above. |
   | `ACCOUNT_ID` | Yes | Databricks Account Console → top-right profile menu. |
   | `WORKSPACE_ID` | Yes | Numeric ID in Account Console → **Workspaces**, or the workspace URL's `?o=` value. Use commas for multiple workspaces. |
   | `SP_NAME` | No | Display name for the deployment SP; defaults to `genierails-deployer`. |
   | `TARGET_CATALOG` | Recommended | Exact name of the existing Unity Catalog catalog GenieRails will govern, from Catalog Explorer. Omit only for the greenfield alternative below. |
   | `MODEL_ENDPOINT` | No | Model serving endpoint to grant `CAN QUERY`; defaults to `databricks-claude-sonnet-4-6`. |
   | `PLAN=1` / `YES=1` | No | Use `PLAN=1` to preview, then rerun with `YES=1` to apply without an interactive confirmation. |

   - A preflight confirms the catalog exists and the caller can grant access. It stops before making changes if either check fails.
   - On success, it grants the required catalog permissions and prints the `auth.auto.tfvars` values, including a new OAuth secret when one is created.

   <details>
   <summary><strong>Alternative — Greenfield catalog creation</strong></summary>

   Omit `TARGET_CATALOG`, and bootstrap grants the SP metastore `CREATE CATALOG` instead, so it can create and own a fresh catalog. Use this only for demo/test setups where you don't already have a catalog to govern.
   </details>

</details>

---

<a id="credentials"></a>
<details>
<summary><strong>Check 7 — Databricks credentials and workspace values are ready</strong></summary>

**Requirements:**

You'll need these values for `auth.auto.tfvars`:

| Credential | Where to find |
|-----------|---------------|
| `databricks_account_id` | Account Console → top-right profile menu |
| `databricks_account_host` | AWS: `https://accounts.cloud.databricks.com` / Azure: `https://accounts.azuredatabricks.net` |
| `databricks_client_id` | Account Console → User Management → Service Principals → Application ID |
| `databricks_client_secret` | Same SP → OAuth Secrets → Generate Secret |
| `databricks_workspace_id` | Account Console → Workspaces, or `?o=` parameter in workspace URL |
| `databricks_workspace_host` | Your workspace URL (e.g., `https://dbc-xxx.cloud.databricks.com`) |

</details>

---

<a id="quick-check"></a>
<details>
<summary><strong>Check 8 — Required local tools respond successfully</strong></summary>

**Requirements:**

Confirm the required tools are present:

```bash
make --version && python3 --version && terraform --version && git --version
```

**Done when —** all four commands exit successfully. Then follow the **[Dev-to-Prod Walkthrough](../examples/dev_to_prod/README.md)** to clone the repository and create your first environment. Already have a Genie agent built in the Databricks UI? [Import it into code first](from-ui-to-production.md), then follow the same walkthrough.

</details>

---

<a id="cloud-specific-requirements"></a>
<details>
<summary><strong>Contributor only — test-ci cloud provisioning access</strong></summary>

This section is **not required to use GenieRails**. It applies only to contributors and maintainers who run the integration-test provisioning harness (`make test-ci`), which creates and removes cloud test resources.

<details>
<summary><strong>AWS — Required test-ci credentials and permissions</strong></summary>

**Credentials** (one of):
- `AWS_PROFILE` environment variable pointing to a named profile in `~/.aws/credentials`
- `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (+ optional `AWS_SESSION_TOKEN`)
- Default boto3 credential chain (instance profile, SSO, etc.)

**IAM / S3 permissions** — create/update/list/delete on the test IAM roles, role policies, S3 buckets, and objects, plus `sts:GetCallerIdentity`.
</details>

<details>
<summary><strong>Azure — Required test-ci credentials and permissions</strong></summary>

**Credentials** (one of):
- Service principal: `AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET` + `AZURE_TENANT_ID`
- `DefaultAzureCredential` (Azure CLI login, managed identity, etc.)

**Additional config:**
- `AZURE_SUBSCRIPTION_ID`
- `AZURE_RESOURCE_GROUP`
- `AZURE_REGION` (e.g., `australiaeast`)

**Azure RBAC roles:**
- `Contributor` on the resource group
- `Storage Blob Data Contributor`
- `User Access Administrator` — optional (only if the SP itself assigns roles; otherwise it falls back to your Azure CLI login)
</details>

</details>
