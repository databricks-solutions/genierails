# Config Reference

## 3-Layer Architecture

| Layer | Path | Owns | Does NOT own |
|---|---|---|---|
| Account | `envs/account/` | Groups, group membership, tag policy definitions | Masking functions, FGAC policies, Genie Spaces |
| Data access | `envs/<env>/data_access/` | Tag assignments, masking functions, FGAC policies, catalog grants | Tag policy definitions, Genie Spaces |
| Workspace | `envs/<env>/` | Warehouse, Genie Spaces, ACLs, entitlements | Groups, tag policies, FGAC policies |

The account layer is shared across all environments. Data access and workspace layers are per-environment.

## File Contract

| File | Contents | Git tracked? |
|---|---|---|
| `envs/<env>/auth.auto.tfvars` | Credentials (account ID, client ID/secret, workspace) | **No** (gitignored) |
| `envs/<env>/env.auto.tfvars` | genie_spaces, sql_warehouse_id, country, genie_only | **Yes** |
| `envs/<env>/generated/abac.auto.tfvars` | Draft output from `make generate` | No |
| `envs/<env>/generated/masking_functions.sql` | Draft masking SQL from `make generate` | No |
| `envs/account/abac.auto.tfvars` | Groups, group_members, tag_policies | **Yes** |
| `envs/<env>/data_access/abac.auto.tfvars` | Tag assignments, FGAC policies | **Yes** |
| `envs/<env>/data_access/masking_functions.sql` | Masking SQL | **Yes** |
| `envs/<env>/abac.auto.tfvars` | Group lookups, genie_space_configs | **Yes** |

## auth.auto.tfvars Templates

### AWS
```hcl
databricks_account_id    = ""   # Account Console → top-right → account ID
databricks_client_id     = ""   # SP Application ID
databricks_client_secret = ""   # SP OAuth secret
databricks_workspace_id  = ""   # from workspace URL ?o=<id>
databricks_workspace_host = ""  # https://dbc-xxx.cloud.databricks.com
```

### Azure
```hcl
databricks_account_id    = ""
databricks_account_host  = "https://accounts.azuredatabricks.net"  # REQUIRED
databricks_client_id     = ""
databricks_client_secret = ""
databricks_workspace_id  = ""
databricks_workspace_host = ""  # https://adb-xxx.azuredatabricks.net
```

## env.auto.tfvars Examples

### Single space (new)
```hcl
genie_spaces = [
  {
    name      = "Sales Analytics"
    uc_tables = ["dev_catalog.sales.orders", "dev_catalog.sales.customers"]
  },
]
sql_warehouse_id = ""    # empty = auto-create serverless
country = ""             # ANZ, IN, SEA, or empty
genie_only = false
```

### Import existing space
```hcl
genie_spaces = [
  {
    genie_space_id = "01ef7b3c2a4d5e6f"  # from Genie Space URL
  },
]
```

### Multi-space, multi-catalog
```hcl
genie_spaces = [
  {
    name      = "Finance"
    uc_tables = ["dev_fin.finance.transactions", "dev_fin.finance.customers"]
  },
  {
    name      = "Clinical"
    uc_tables = ["dev_clinical.clinical.encounters", "dev_clinical.clinical.patients"]
  },
]
```

### Per-space warehouse override
```hcl
genie_spaces = [
  { name = "Finance", sql_warehouse_id = "abc123", uc_tables = [...] },
  { name = "HR",      sql_warehouse_id = "def456", uc_tables = [...] },
]
sql_warehouse_id = ""   # top-level fallback (unused when all spaces override)
```

## Parameters

| Param | Targets | Values | Example |
|---|---|---|---|
| `ENV` | apply, validate, destroy, import, plan | dev, prod, account, custom | `make apply ENV=prod` |
| `SPACE` | generate | space name string | `make generate SPACE="Finance"` |
| `MODE` | generate | governance, genie | `make generate MODE=governance` |
| `COUNTRY` | generate, validate | ANZ, IN, SEA (comma-sep) | `make generate COUNTRY=ANZ,SEA` |
| `INDUSTRY` | generate, validate | financial_services, healthcare, retail | `make generate INDUSTRY=healthcare` |
| `SOURCE_ENV` | promote | env name | `make promote SOURCE_ENV=dev` |
| `DEST_ENV` | promote | env name | `make promote DEST_ENV=prod` |
| `DEST_CATALOG_MAP` | promote | src=dest pairs | `DEST_CATALOG_MAP="dev_fin=prod_fin"` |
| `KEEP_DATA` | integration-test | any value | `make integration-test KEEP_DATA=1` |
| `LAYER` | plan | account, data_access, workspace | `make plan LAYER=data_access` |

## Available Overlays

### Countries (`shared/countries/`)
- **ANZ** — Australia & NZ: TFN, Medicare, BSB, IRD, NHI, driver licence, ABN, CRN, passport
- **IN** — India: Aadhaar, PAN, GSTIN, voter ID, driving licence, UAN, ration card
- **SEA** — SE Asia: NRIC, FIN, MyKad (SG/MY), Thai ID, NIK, NPWP (TH/ID), PhilSys, TIN (PH), CCCD, MST (VN)

### Industries (`shared/industries/`)
- **financial_services** — PCI masking, AML row filters, credit card redaction, account number tokenization
- **healthcare** — HIPAA compliance, PHI masking, MRN redaction, clinical notes filtering
- **retail** — loyalty program PII, order data masking, customer profile protection
