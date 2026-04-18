# GenieRails

GenieRails automates Databricks Unity Catalog governance (ABAC tagging, column masking, row filters) and Genie Space deployment using Terraform + LLM generation.

## Working Directory

Always run commands from `aws/` or `azure/` — never from the repo root. Each cloud directory has a `Makefile` that includes `shared/Makefile.shared`.

## Happy Path

```
make setup                    # Scaffold envs/account/, envs/dev/, envs/dev/data_access/
vi envs/dev/auth.auto.tfvars  # Databricks credentials (account ID, client ID/secret, workspace)
vi envs/dev/env.auto.tfvars   # Define genie_spaces with uc_tables
make generate                 # LLM generates abac.auto.tfvars + masking_functions.sql
vi envs/dev/generated/...     # Review generated governance and Genie config
make validate-generated       # Check before applying
make apply                    # Deploy: account → data_access → workspace layers
```

Optional overlays: `make generate COUNTRY=ANZ INDUSTRY=financial_services`
Per-space generation: `make generate SPACE="Finance Analytics"`
Promote to prod: `make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP="dev_cat=prod_cat"`

## Key Commands

| Command | Purpose |
|---|---|
| `make setup` | Bootstrap environment directories |
| `make generate` | LLM-driven ABAC + Genie config generation |
| `make validate-generated` | Validate generated files before apply |
| `make apply` | Deploy all 3 Terraform layers |
| `make promote` | Remap catalogs and promote dev → prod |
| `make audit-schema` | Detect schema drift (new/removed columns) |
| `make generate-delta` | Incremental regen after schema drift |
| `make import` | Import existing Databricks resources into Terraform state |
| `make destroy` | Tear down deployed resources |

## 3-Layer Architecture

| Layer | Path | Owns |
|---|---|---|
| Account | `envs/account/` | Groups, group membership, tag policy definitions |
| Data access | `envs/<env>/data_access/` | Tag assignments, masking functions, FGAC policies, grants |
| Workspace | `envs/<env>/` | Warehouse, Genie Spaces, ACLs, entitlements |

## Safety Rules

- **Never** display `databricks_client_secret` values in output
- **Never** commit `auth.auto.tfvars` — it is gitignored for security
- **Always** run `make validate-generated` before `make apply`
- **Always** confirm with the user before running `make destroy`
- **Always** confirm before `make apply` on production environments

## Tests

```bash
python -m pytest shared/tests/ -v     # Unit tests (324 tests)
```

## Project Layout

```
aws/                     # AWS cloud wrapper (thin Makefile)
azure/                   # Azure cloud wrapper (thin Makefile)
shared/
  Makefile.shared        # All make targets and orchestration
  generate_abac.py       # LLM-driven ABAC generator
  validate_abac.py       # Config validation
  docs/                  # 18 documentation guides
  countries/             # ANZ.yaml, IN.yaml, SEA.yaml overlays
  industries/            # financial_services.yaml, healthcare.yaml, retail.yaml
  modules/               # Terraform modules (account, data_access, workspace)
  roots/                 # Terraform roots (entry points for each layer)
  scripts/               # Support scripts (provisioning, testing, utilities)
  examples/              # Bank demos (aus, india, asean)
```

## Guided Workflows

For interactive guided workflows (setup, generate, promote, troubleshooting), use `/genierails`.
