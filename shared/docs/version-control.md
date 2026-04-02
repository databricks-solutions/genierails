# Version Control & Standalone Terraform

After running `make generate` and `make apply`, your governance configuration is plain Terraform — you own it, you version-control it, and you can run it independently of GenieRails if needed.

## Where your configs live

After `make generate` + `make promote`, the single LLM-generated draft is split into three stable layers:

```
envs/
├── account/                        ← ACCOUNT LAYER (shared across all envs)
│   └── abac.auto.tfvars           ← groups, tag_policies
│
├── dev/                            ← WORKSPACE + DATA ACCESS (per environment)
│   ├── env.auto.tfvars            ← input config: tables, warehouse, genie_spaces
│   ├── abac.auto.tfvars           ← Genie Space configs (title, instructions,
│   │                                 benchmarks, SQL measures, sample questions)
│   ├── auth.auto.tfvars           ← credentials (NEVER commit)
│   ├── generated/                 ← ephemeral LLM draft (gitignored)
│   └── data_access/
│       ├── abac.auto.tfvars       ← tag_assignments, fgac_policies
│       └── masking_functions.sql  ← SQL UDFs for column masking
│
└── prod/                           ← same structure after cross-env promote
```

**What lives where:**

| Concern | Layer | File |
|---------|-------|------|
| Groups and tag policies | Account | `envs/account/abac.auto.tfvars` |
| Tag assignments and FGAC policies | Data Access | `envs/<env>/data_access/abac.auto.tfvars` |
| Masking SQL functions | Data Access | `envs/<env>/data_access/masking_functions.sql` |
| Genie Space configuration | Workspace | `envs/<env>/abac.auto.tfvars` |
| Environment input (tables, warehouse) | Workspace | `envs/<env>/env.auto.tfvars` |

The `generated/` folder is ephemeral — it holds the raw LLM draft for review and tuning. After `make promote` splits it into the three layers, the split configs become the source of truth.

## What to version control

### Commit these

| File | Why |
|------|-----|
| `envs/account/abac.auto.tfvars` | Shared groups and tag policies — the governance baseline |
| `envs/<env>/env.auto.tfvars` | Your environment config (tables, warehouse, spaces) |
| `envs/<env>/abac.auto.tfvars` | Genie Space configuration (tuned instructions, benchmarks) |
| `envs/<env>/data_access/abac.auto.tfvars` | Tag assignments and FGAC policies |
| `envs/<env>/data_access/masking_functions.sql` | Masking SQL UDFs deployed to the warehouse |

### Never commit

| File | Why |
|------|-----|
| `auth.auto.tfvars` | Contains service principal credentials |
| `generated/` | Ephemeral LLM drafts — regenerated on every `make generate` |
| `*.tfstate`, `*.tfstate.backup` | Terraform state may contain secrets |
| `.terraform/` | Provider binaries and cache |
| `.genie_space_id` | Auto-managed by Terraform lifecycle |

### Setting up git tracking

The default `.gitignore` excludes `envs/` entirely. To track your configs:

```bash
# Option 1: Remove the blanket exclusion, add specific ignores
# Edit .gitignore: remove the "envs/" line, then add:
envs/*/auth.auto.tfvars
envs/**/generated/
envs/**/.terraform/
envs/**/*.tfstate*
envs/**/.genie_space_id

# Option 2: Force-track specific files (leave .gitignore as-is)
git add -f envs/account/abac.auto.tfvars
git add -f envs/dev/env.auto.tfvars
git add -f envs/dev/abac.auto.tfvars
git add -f envs/dev/data_access/abac.auto.tfvars
git add -f envs/dev/data_access/masking_functions.sql
```

## Pinning to a GenieRails version

After deploying, pin your GenieRails version to avoid unexpected changes from upstream updates:

```bash
# Use a tagged release
git checkout v1.0.0

# Or pin to a specific commit
git checkout abc1234
```

**Why this matters:** The Terraform modules in `shared/modules/` and `shared/roots/` define resource structure. If a future GenieRails version changes a module (adds a resource, renames a variable), running `make apply` against your existing state could modify or destroy deployed resources.

**Safety checklist after updating GenieRails:**

1. `git diff` — review what changed in modules and scripts
2. `make plan ENV=dev` — preview what Terraform would change
3. Only `make apply` if the plan looks safe

The Terraform Databricks provider is already pinned (`~> 1.91.0` in each root), so provider updates won't surprise you. GenieRails module changes are the main thing to watch.

## Running Terraform without GenieRails

After `make promote`, the split configs are plain `.tfvars` files. You can run Terraform directly:

```bash
# 1. Account layer (groups, tag policies)
cd shared/roots/account
terraform init -backend-config="path=../../../envs/account/terraform.tfstate"
terraform apply \
  -var-file="../../../envs/account/auth.auto.tfvars" \
  -var-file="../../../envs/account/abac.auto.tfvars"

# 2. Data access layer (tag assignments, FGAC policies, masking UDFs)
cd shared/roots/data_access
terraform init -backend-config="path=../../../envs/dev/data_access/terraform.tfstate"
terraform apply \
  -var-file="../../../envs/dev/auth.auto.tfvars" \
  -var-file="../../../envs/dev/env.auto.tfvars" \
  -var-file="../../../envs/dev/data_access/abac.auto.tfvars" \
  -var="env_dir=$(pwd)/../../../envs/dev/data_access"

# 3. Workspace layer (Genie Spaces, ACLs)
cd shared/roots/workspace
terraform init -backend-config="path=../../../envs/dev/terraform.tfstate"
terraform apply \
  -var-file="../../../envs/dev/auth.auto.tfvars" \
  -var-file="../../../envs/dev/env.auto.tfvars" \
  -var-file="../../../envs/dev/abac.auto.tfvars" \
  -var="env_dir=$(pwd)/../../../envs/dev"
```

**Important:** Apply layers in order — account first, then data access, then workspace. Tag policies must exist before FGAC policies can reference them.

### What the Makefile adds

Running standalone Terraform works, but you lose these conveniences:

| Feature | What it does |
|---------|-------------|
| Layer ordering with waits | 30s propagation delay between layers |
| Tag policy visibility check | Verifies tag policies are visible to FGAC engine before proceeding |
| Auto-import | Imports existing resources to prevent "already exists" errors |
| Fingerprint skip | Skips apply if config hasn't changed since last run |
| `TF_DATA_DIR` isolation | Prevents Terraform state conflicts between layers |

### Python dependencies in Terraform

Two Terraform resources use `local-exec` provisioners that call Python scripts:

| Layer | Script | Purpose |
|-------|--------|---------|
| Data Access | `deploy_masking_functions.py` | Deploys masking SQL UDFs to the warehouse |
| Workspace | `genie_space.sh` | Creates/updates Genie Spaces via REST API |

These scripts must be available at their expected paths (relative to `shared/scripts/`). If you relocate files, update the `genie_script_path` and `masking_script_path` variables in the Terraform roots.

## Next steps

- [CI/CD Integration](cicd.md) — automate validation and deployment from a pipeline
- [Architecture](architecture.md) — understand the three-layer model in depth
