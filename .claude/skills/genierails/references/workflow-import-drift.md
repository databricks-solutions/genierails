# Import Existing Spaces & Schema Drift

## Import an Existing Genie Space

Use this when you already have a Genie Space configured in the Databricks UI and want to add governance + deploy to production.

### Step 1: Get the Space ID

The ID is in the Genie Space URL: `...genie/rooms/<space_id>`. For example: `01ef7b3c2a4d5e6f`.

### Step 2: Configure env.auto.tfvars

```hcl
genie_spaces = [
  {
    genie_space_id = "01ef7b3c2a4d5e6f"
    # name and uc_tables are auto-discovered from the Genie API
  },
]
```

### Step 3: Generate

```bash
make generate [COUNTRY=...] [INDUSTRY=...]
```

This queries the Genie Space API and:
- Discovers all tables the space uses
- Captures instructions, benchmarks, SQL measures, filters verbatim (no LLM rewriting)
- Generates ABAC governance (groups, tags, masking) from the discovered table DDLs

### Step 4: Copy Discovered Tables

After generation, the output prints the discovered tables. Copy them into `envs/dev/data_access/env.auto.tfvars` so UC grants and masking are scoped correctly:

```hcl
# envs/dev/data_access/env.auto.tfvars
uc_tables = [
  "catalog.schema.table1",
  "catalog.schema.table2",
]
```

### Step 5: Review and Apply

Review `envs/dev/generated/abac.auto.tfvars` and `masking_functions.sql`, then:
```bash
make validate-generated
make apply
```

The existing Genie Space is never modified or deleted — only group ACLs are applied.

---

## Schema Drift

Use this when tables have changed (new columns added, columns removed, types changed) after initial deployment.

### Step 1: Audit

```bash
make audit-schema ENV=dev
```

This compares the current DDL in Unity Catalog against the DDL snapshot used during last generation. Reports:
- New columns (untagged — potential governance gap)
- Removed columns (stale tag assignments)
- Modified columns (type changes)

### Step 2: Decide — Delta vs Full Regen

| Situation | Command |
|---|---|
| Few new columns, want to keep existing tuning | `make generate-delta` |
| Major schema changes, new tables added | `make generate` (full) |
| New overlay needed (added country/industry data) | `make generate COUNTRY=... INDUSTRY=...` |

### Step 3: Validate and Apply

```bash
make validate-generated
make apply
```

---

## Brownfield Import

Use this when account-level resources (groups, tag policies) already exist in Databricks and you want Terraform to manage them.

### Import Existing Resources

```bash
make import ENV=account    # Import account groups + tag policies
make import ENV=dev        # Import env-scoped governance + workspace resources
```

This runs `terraform import` for each existing resource, preventing "already exists" errors during `make apply`.

### Selective Import

For more control:
```bash
cd envs/account && ../../scripts/import_existing.sh --groups-only --dry-run
cd envs/account && ../../scripts/import_existing.sh --tags-only
cd envs/dev/data_access && ../../../scripts/import_existing.sh --fgac-only
```

### Brownfield Workflow

Full workflow for environments with existing ABAC infrastructure:
```bash
make generate
# Review generated files
make promote
make import ENV=account
make import
make plan             # Dry run — verify no destructive changes
make apply
```
