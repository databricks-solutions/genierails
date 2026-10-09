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
>
> Contributors: a few unit tests exercise GNU Make 4+ options (`-Oline`, `--output-sync`). Apple's `make` is 3.81, so on macOS they run with `gmake` if present (`brew install make`) and are skipped otherwise. GenieRails itself works with either.

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

GenieRails runs **as a service principal (SP)**. There are two separate jobs here — who sets the SP up, and who runs GenieRails with it:

- **Setting it up (one time)** — an **Account Admin** creates the SP and gives it its roles, and the **owner of your catalog** gives it access to that catalog. If one person is both, they can do it all (including with `make bootstrap-sp`); otherwise the two of them each do their part.
- **Running it (day to day)** — GenieRails commands log in *as the SP*, using its secret in `auth.auto.tfvars`. Whoever runs them just needs that secret (see [Credentials](#credentials)) and network access — no Databricks roles of their own.

The SP needs:

| Role / authority | Scope | Why |
|------------------|-------|-----|
| **Account Admin** | Account | Create groups when explicitly requested; create, assign, and delete the per-tier test SPs used by live `verify-access` |
| **Tag Policy Creator + Manager** | Account | Create and maintain governed tag policies |
| **Workspace Admin** | Target workspace | Deploy governance resources |
| **Authority over the target catalog** | The catalog you govern | **Own it, or** be granted `SELECT` + `MANAGE` + `APPLY TAG` (plus `ASSIGN` on the governed tags GenieRails applies). This lets it deploy tag assignments, masking functions, FGAC policies, and grants — and self-grant its own `USE CATALOG` / `USE SCHEMA` / `EXECUTE` / `CREATE FUNCTION`. `SELECT` lets `verify-access` prove row-pairing keys as the SP, which `make release` does before its first apply. |
| **Query the model serving endpoint** | Workspace | `CAN QUERY` on `databricks-claude-sonnet-4-6` — generation calls a foundation model (an external Anthropic/OpenAI provider works too). |

*Optional background — skip the two sections below unless you want the details. Everything you need to act on is in the table above and the steps that follow.*

<details>
<summary><strong>Details — Per-tier test service principals</strong></summary>

Databricks cannot impersonate a user for a query, so live `verify-access` creates one dedicated service principal for each access tier, adds it to that tier's group, and runs the same SQL using each principal's own OAuth credentials. This proves that authorized tiers see raw values while restricted tiers see masked values and filtered rows. The test SPs are deleted automatically when verification finishes unless `KEEP_PRINCIPALS=1` is set for debugging. They are separate from the GenieRails deployment SP.

</details>

<details>
<summary><strong>Details — Existing catalog authority</strong></summary>

The SP governs an **existing** catalog — `make apply` never creates one — so it needs authority *on that catalog*, **not** metastore `CREATE CATALOG`. Metastore `CREATE CATALOG` matters only for greenfield/demo, where GenieRails creates a fresh catalog it then owns.

</details>

**Provision the SP — choose one method:**

1. **Manually** — the Account Admin creates the SP in the Account Console and assigns the account and workspace roles in the table above. The target catalog's owner grants it `SELECT` + `MANAGE` + `APPLY TAG`.
2. **With `make bootstrap-sp`** — an already-authorized Account Admin runs the command below. It cannot elevate a non-admin caller. Run it from your cloud's folder in a clone of the repo:

   ```bash
   git clone https://github.com/databricks-solutions/genierails.git
   cd genierails/aws           # or: cd genierails/azure
   ```

   `ACCOUNT_PROFILE` is the name of a Databricks CLI profile for the **bootstrap caller**, not the deployment SP. [Install the Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) if needed, then create the profile below (on Azure, use `https://accounts.azuredatabricks.net` as the host):

   ```bash
   databricks auth login \
     --host https://accounts.cloud.databricks.com \
     --account-id <account-id> \
     --skip-workspace \
     --profile genierails-bootstrap
   ```

   The account login does not authenticate the caller to a workspace. Log in to each
   target workspace as well (use the actual dev and prod workspace URLs):

   ```bash
   databricks auth login --host <dev-workspace-url>
   databricks auth login --host <prod-workspace-url>
   ```

   ```bash
   # dev workspace + dev catalog: creates the SP (or reuses an existing one); a new secret is printed once, so save it
   make bootstrap-sp ACCOUNT_PROFILE=genierails-bootstrap ACCOUNT_ID=<id> WORKSPACE_ID=<dev-workspace-id> TARGET_CATALOG=<dev-catalog> YES=1

   # prod workspace + prod catalog: reuses the same SP (same default SP_NAME)
   make bootstrap-sp ACCOUNT_PROFILE=genierails-bootstrap ACCOUNT_ID=<id> WORKSPACE_ID=<prod-workspace-id> TARGET_CATALOG=<prod-catalog> YES=1

   # Or bootstrap both at once; catalog values align positionally with workspace IDs
   make bootstrap-sp ACCOUNT_PROFILE=genierails-bootstrap ACCOUNT_ID=<id> WORKSPACE_ID=<dev-workspace-id>,<prod-workspace-id> TARGET_CATALOG=<dev-catalog>,<prod-catalog> YES=1
   ```

   To preview the changes first, run the same command with `PLAN=1` instead of `YES=1`.
   Plan mode performs the same read-only authentication, endpoint, access-path, and
   grant-authority preflight as apply mode. If credentials are absent, it explicitly
   reports that it is showing an offline, unverified plan.

   | Parameter | Required | Value / where to find it |
   |-----------|----------|--------------------------|
   | `ACCOUNT_PROFILE` | No | Profile name in `~/.databrickscfg`; defaults to `DEFAULT`. Use the Account Admin profile created above. |
   | `WORKSPACE_PROFILE` | No | Workspace profile whose host matches the target workspace. For multiple workspaces, pass a comma-separated profile per `WORKSPACE_ID`. If omitted, OAuth M2M or Azure client-secret SP credentials are used to create fresh workspace authentication; all other account auth types use host-based Databricks CLI login. |
   | `ACCOUNT_ID` | Yes | Databricks Account Console → top-right profile menu. |
   | `WORKSPACE_ID` | Yes | Numeric ID in Account Console → **Workspaces**, or the workspace URL's `?o=` value. Use commas for multiple workspaces. |
   | `SP_NAME` | No | Display name for the deployment SP; defaults to `genierails-deployer`. |
   | `TARGET_CATALOG` | Recommended | Exact name of the existing Unity Catalog catalog GenieRails will govern, from Catalog Explorer. With multiple `WORKSPACE_ID` values, supply either one catalog for every workspace (back-compatible) or one comma-separated catalog per workspace in the same order. Omit only for the greenfield alternative below. |
   | `MODEL_ENDPOINT` | No | Model serving endpoint to grant query access; bootstrap grants `CAN QUERY` on custom endpoints or Unity Catalog `EXECUTE` on the backing `system.ai` function for Foundation Model API endpoints. Defaults to `databricks-claude-sonnet-4-6`. |
   | `PLAN=1` / `YES=1` | No | `PLAN=1` previews without changing anything; `YES=1` applies without an interactive prompt. |

   - A preflight confirms the catalog exists and the caller can grant access. It stops before making changes if either check fails.
   - On success, it grants the required catalog permissions and prints the `auth.auto.tfvars` values, including a new OAuth secret when one is created.
   - Run once per environment, or combine environments positionally: `WORKSPACE_ID=<dev-id>,<prod-id> TARGET_CATALOG=<dev-catalog>,<prod-catalog>`. A single catalog value still applies to every workspace for backward compatibility.
   - The first run creates the SP and prints its OAuth secret; the second run reuses the SP and doesn't print a new secret. Use the same `client_id` / `client_secret` in both `envs/dev/auth.auto.tfvars` and `envs/prod/auth.auto.tfvars`.

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

You'll need these values in each `envs/<env>/auth.auto.tfvars`.

**Used `make bootstrap-sp`?** It prints a ready-to-paste block for each workspace with `databricks_client_id`, `databricks_client_secret`, `databricks_workspace_host` and `databricks_workspace_id`. Paste it, then add:

- `databricks_account_id` — the same `ACCOUNT_ID` you passed to `bootstrap-sp`.
- `databricks_account_host` — **Azure only:** `https://accounts.azuredatabricks.net`. On AWS you can omit it (defaults to `https://accounts.cloud.databricks.com`).

The OAuth secret is shown only on the run that creates it — save it then. A later run shows `<existing-secret-not-retrievable>`; use `ROTATE_SECRET=1` to mint a new one.

<details>
<summary><strong>Created the SP manually? Where to find each value</strong></summary>

| Credential | Where to find |
|-----------|---------------|
| `databricks_account_id` | Account Console → top-right profile menu |
| `databricks_account_host` | AWS: `https://accounts.cloud.databricks.com` / Azure: `https://accounts.azuredatabricks.net` |
| `databricks_client_id` | Account Console → User Management → Service Principals → Application ID |
| `databricks_client_secret` | Same SP → OAuth Secrets → Generate Secret |
| `databricks_workspace_id` | Account Console → Workspaces, or `?o=` parameter in workspace URL |
| `databricks_workspace_host` | Your workspace URL (e.g., `https://dbc-xxx.cloud.databricks.com`) |

</details>

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

**Done when —** all four commands exit successfully. Then follow the **[Dev-to-Prod Walkthrough](../examples/dev_to_prod/README.md)** to clone the repository and create your first environment. Already have a Genie agent built in the Databricks UI? [Import it into code first](import-genie-agent-from-ui.md), then follow the same walkthrough.

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

The Databricks Account Admin SP is automatically assigned as an admin of the new test workspace. The harness then creates a temporary serverless usage policy bound only to that workspace so Data Classification can run without manual UI setup, and deletes the policy during teardown.
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
