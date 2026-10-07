# Brownfield Migration Guide

How to adopt GenieRails when you already have ABAC governance (groups, tag policies, masking functions, FGAC policies) in your Databricks environment.

## Overview

A "brownfield" environment has existing governance resources (groups, tag policies, masks, FGAC policies, grants) that Terraform doesn't know about. Without importing them, `terraform apply` fails with "already exists" errors. This guide adopts them into GenieRails **without exposing users or silently changing sensitivity decisions**.

Two adoptions to keep straight:
- **Brownfield adoption** (this guide) = pull existing *governance resources* + Terraform state under management.
- **[Import a Genie Agent from UI into Code](import-genie-agent-from-ui.md)** = import an existing Genie *agent's content*. They're complementary; neither replaces the other.

Going forward, sensitivity comes from **native UC classification** (`class.*`), GenieRails derives one `gr_treatment` per column, and the coverage check must pass before any business access is granted — adopting brownfield state does not bypass those gates.

## Before You Start

**Assess your current state:**

```bash
# List existing groups
databricks groups list --output JSON | jq '.[].displayName'

# List existing tag policies
databricks unity-catalog tag-policies list

# List existing FGAC policies (requires SDK)
python3 -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
for p in w.fgac_policies.list():
    print(f'{p.name}: {p.policy_type} on {p.catalog}')
"
```

**Decision: Generate-then-import vs. Import-only**

| Approach | When to use | Trade-offs |
|---|---|---|
| **Generate-then-import** | You want GenieRails to manage governance going forward | LLM may generate config that differs from existing; you merge manually |
| **Import-only** | You want to codify existing governance exactly as-is | No LLM involvement; you write tfvars manually to match existing state |

## Generate-Then-Import (Recommended)

### Step 1: Enable classification, then generate ABAC config

Enable native classification and wait for `class.*` tags first (sensitivity is native, not LLM-guessed); overlays only add regional/industry rule context:

```bash
make enable-classification ENV=dev   # or the Databricks UI (recommended); then wait for class.* tags
make generate ENV=dev GENERATE_ARGS='--groups "<your-idp-groups>"' COUNTRY=ANZ INDUSTRY=financial_services
```

### Step 2: Review and align with existing governance

Compare the generated config against your existing resources:

```bash
# Check generated groups vs. existing
grep -A1 'groups' envs/dev/generated/abac.auto.tfvars | head -20
databricks groups list --output JSON | jq '.[].displayName'

# Check generated tag policies vs. existing
grep 'tag_policies' envs/dev/generated/abac.auto.tfvars
```

**Common conflicts:**
- Generated groups have different names than existing ones → edit `abac.auto.tfvars` to use existing names
- Generated tag policy values differ from existing → merge values manually
- Generated masking functions differ from existing UDFs → keep your existing functions (see Option 2 in [Custom Masking Functions](custom-masking-functions.md))

### Step 3: Promote to layers

```bash
make promote ENV=dev
```

### Step 4: Import existing resources

```bash
# Import account-level resources (groups, tag policies)
make import ENV=account

# Import workspace-level resources (tag assignments, FGAC policies, grants)
make import ENV=dev
```

### Step 5: Verify alignment

```bash
# Plan should show minimal or zero changes
make plan ENV=account
make plan ENV=dev
```

If `make plan` shows unexpected changes:
- **"will be updated"** — your config differs from the live state. Edit tfvars to match, or accept the update.
- **"will be created"** — a resource in your config doesn't exist yet. This is expected for new governance.
- **"will be destroyed"** — a live resource isn't in your config. Import it or add it to config.

### Step 6: Prove coverage, then apply

```bash
make coverage-gate ENV=dev   # BLOCKS if any classified column is unprotected
make rehearse ENV=dev        # apply (grants only if coverage passes) + verify-access as each tier
```

## Import-Only (Exact Match)

Use this when you want Terraform to manage existing resources without changing them.

### Step 1: Write tfvars manually

Create `envs/dev/generated/abac.auto.tfvars` to exactly match your existing resources:

```hcl
groups = [
  { name = "existing_analysts" },
  { name = "existing_managers" },
]

tag_policies = [
  {
    key    = "pii_level"
    values = ["public", "masked", "sensitive"]
  },
]

# ... match your existing tag_assignments, fgac_policies, etc.
```

### Step 2: Import and plan

```bash
make promote ENV=dev
make import ENV=account
make import ENV=dev
make plan ENV=dev
# Should show zero changes
```

## Handling Existing Masking Functions

If you have existing masking functions in your catalog, include them in the SQL file: copy your existing function definitions into `envs/<env>/generated/masking_functions.sql` so Terraform manages them alongside the generated ones, and point `function_name` / `function_catalog` / `function_schema` in `abac.auto.tfvars` at them. See [Custom Masking Functions](custom-masking-functions.md).

## Handling Conflicts

### Generated config has more policies than existing

The LLM may generate additional policies you don't need. Remove them from `abac.auto.tfvars` before applying.

### Generated config has fewer policies than existing

Your existing governance has policies the LLM didn't generate. Add them to `abac.auto.tfvars` manually, or import them after the first apply.

### Tag policy values don't match

If the generated tag policy has `values = ["masked", "redacted"]` but your existing policy has `values = ["masked", "sensitive", "restricted"]`:

1. Edit `abac.auto.tfvars` to include ALL values (union of both)
2. Run `make import ENV=account` to import the existing policy
3. Run `make plan` to verify no unexpected changes

## State Migration

If you previously managed some resources outside GenieRails (e.g., via a separate Terraform workspace or manual scripts):

```bash
# Move state from old workspace to GenieRails structure
make migrate-state ENV=dev
```

This is only needed when merging Terraform state from a previous GenieRails layout. Skip this for first-time brownfield adoption.

## Rollback

If the migration goes wrong:

1. `make plan` shows what Terraform would change — review before applying
2. `terraform state rm <resource>` removes a resource from Terraform management without deleting it
3. ⚠️ **Once you import a resource, it IS Terraform-managed** — `make destroy` will destroy it, including formerly-manual resources you adopted. To release a resource from management *without* deleting it, use `terraform state rm <resource>` (step 2), not `make destroy`.
4. Keep a backup of your existing governance state before starting migration

## Incremental Migration

You don't have to migrate everything at once. Start with one catalog:

```bash
# Generate for just one space
make generate ENV=dev SPACE="Finance Analytics"

# Import just that space's resources
make import ENV=dev
make apply ENV=dev
```

Then add more agents over time. For new columns, prefer letting native classification tag them and re-deriving with `make derive-assignments` (no LLM); `make generate-delta` is an exceptional/legacy LLM path.
