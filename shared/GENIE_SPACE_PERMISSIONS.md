# Permissions Required for a Genie Space

This document lists everything that must be in place for business users (the groups defined in `abac.auto.tfvars`) to use an AI/BI Genie Space.

## 1. Identity

- **Business groups:** Created at account level (Terraform: `module.account.databricks_group` via `roots/account/main.tf`).
  Groups are defined dynamically in `abac.auto.tfvars` under the `groups` variable.
- **Workspace assignment:** Account-level groups are assigned to the workspace (Terraform: `module.workspace.databricks_mws_permission_assignment` with `USER` in `roots/workspace/main.tf`).

## 2. Entitlements (Consumer = Databricks One UI only)

- **Consumer access:** When `workspace_consume` is the **only** entitlement for a user/group, they get the **Databricks One UI** experience (dashboards, Genie spaces, apps) and do **not** get the full workspace UI (clusters, notebooks, etc.).
- **Terraform:** `module.workspace.databricks_entitlements` in `roots/workspace/main.tf` sets `workspace_consume = true` for each group. No other entitlements are set so that consumers see One UI only.

## 3. Compute

- **SQL warehouse:** The shared `data_access` layer uses one warehouse for masking function deployment. The workspace layer uses a warehouse for Genie Space configuration. End users do **not** need explicit **CAN USE** on the warehouse.
- **Terraform:**
  - `module.data_access.databricks_sql_endpoint` in `roots/data_access/main.tf` handles governance execution warehouse resolution
  - `module.workspace.databricks_sql_endpoint` in `roots/workspace/main.tf` handles workspace / Genie warehouse resolution

## 4. Data access

- **Unity Catalog:** At least **SELECT** (and **USE CATALOG** / **USE SCHEMA**) on all UC objects used by the Genie Space. Catalogs are auto-derived from fully-qualified table names in `tag_assignments` and `fgac_policies`. ABAC policies further restrict what each group sees at query time.
- **Terraform:** The shared `data_access` layer grants `USE_CATALOG`, `USE_SCHEMA`, and `SELECT` on all relevant catalogs to all configured groups, deploys masking functions, creates tag policies, assigns tags, and creates FGAC policies.

## 5. Genie Space (create + ACLs)

- **Genie Space:** Create a Genie Space with the tables from `uc_tables` (in `env.auto.tfvars`) and grant at least **CAN VIEW** and **CAN RUN** to all groups.
- **Automation:** Terraform manages Genie Space lifecycle via `module.workspace`:
  - **`genie_space_id` empty** (greenfield): `terraform apply` auto-creates a Genie Space from `uc_tables`, sets ACLs, and trashes the space on `terraform destroy`.
  - **`genie_space_id` set** (existing): `terraform apply` only applies CAN_RUN ACLs to the existing space.

### Auto-create mode

Set `genie_space_id = ""` in `env.auto.tfvars` and ensure `uc_tables` is non-empty. Terraform runs `genie_space.sh create` automatically during apply. Wildcards (`catalog.schema.*`) are expanded via the UC Tables API.

### Existing space mode

Set `genie_space_id` to your Genie Space ID in `env.auto.tfvars`. Terraform runs `genie_space.sh set-acls` to grant CAN_RUN to all configured groups.

### Manual script usage

The script can also be used independently outside of Terraform:

```bash
# Create
GENIE_GROUPS_CSV=$(../../scripts/terraform_layer.sh workspace dev output -raw genie_groups_csv) \
GENIE_TABLES_CSV="cat.schema.t1,cat.schema.t2" \
./scripts/genie_space.sh create

# Set ACLs only
GENIE_GROUPS_CSV=$(../../scripts/terraform_layer.sh workspace dev output -raw genie_groups_csv) \
GENIE_SPACE_OBJECT_ID=<space_id> \
./scripts/genie_space.sh set-acls

# Trash
GENIE_ID_FILE=.genie_space_id ./scripts/genie_space.sh trash
```

## Genie-only mode (least privilege)

When `genie_only = true` is set in `env.auto.tfvars`, the workspace layer skips all account-level operations. The service principal needs only **workspace USER** membership and the **Databricks SQL access** entitlement — no Workspace Admin, no Account Admin, no Metastore Admin.

**A BYO warehouse is required** — the SP cannot create warehouses without admin privileges. Set `sql_warehouse_id` in `env.auto.tfvars` to a warehouse the SP has `CAN USE` on.

**UC table access is still required.** The Genie API validates table access at space creation time. The governance team (or a metastore admin) must grant the BU team's SP the following:

```sql
-- UC table access
GRANT USE CATALOG ON CATALOG <catalog> TO `<bu-sp-application-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<bu-sp-application-id>`;
GRANT SELECT ON SCHEMA <catalog>.<schema> TO `<bu-sp-application-id>`;
```

The governance team must also grant via workspace admin APIs:
- **Workspace assignment** (USER) for the SP
- **Databricks SQL access** entitlement on the SP
- **CAN USE** permission on the SQL warehouse

In this mode:
- Identity (groups, workspace assignment) and entitlements are managed by the governance team via `make apply-governance`
- Data access (UC grants, FGAC, masking) is managed by the governance team
- The BU team only manages Genie Space creation and configuration via `make apply-genie`
- Genie Space ACLs are skipped (groups are empty); the governance team sets ACLs when applying the full workspace layer

This mode is **integration tested** with a minimal-privilege SP (see `make test-genie-only`). The test creates a dedicated SP with only workspace USER + SQL entitlement (no admin roles), grants it CAN USE on a warehouse and UC table access, and verifies Genie Space creation succeeds with zero account-level resources in Terraform state.

See [Central Governance, Self-Service Genie](docs/self-service-genie.md) for the full setup guide.

## Summary checklist

| Requirement            | Implemented in                                                                 |
|------------------------|--------------------------------------------------------------------------------|
| Groups                 | Terraform: `roots/account/main.tf` -> `module.account`                          |
| Workspace assignment   | Terraform: `roots/workspace/main.tf` -> `module.workspace`                      |
| Consumer (One UI only) | Terraform: `roots/workspace/main.tf` -> `module.workspace`                      |
| Warehouse              | Terraform: `roots/data_access/main.tf` and `roots/workspace/main.tf`            |
| UC data (SELECT, etc.) | Terraform: `roots/data_access/main.tf` -> `module.data_access`                  |
| Genie Space + ACLs     | Terraform: `roots/workspace/main.tf` -> `module.workspace`                      |
