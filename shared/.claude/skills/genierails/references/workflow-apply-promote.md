# Validate, Apply, and Promote Workflow

## Validate

Run validation before applying:

```bash
make validate-generated [COUNTRY=ANZ] [INDUSTRY=financial_services]
```

Include COUNTRY/INDUSTRY if they were used during generation — the validator uses them for country-aware column category inference.

### Parse Results

The validator outputs a report with PASS/WARN/FAIL lines. Common fixable errors:

| Error | Fix |
|---|---|
| "non-public tag not covered by fgac_policy" | Add a masking/filter policy for that tag value, or remove the tag assignment |
| "function not in SQL file" | Add the missing function to `masking_functions.sql`, or change the policy to use an existing function |
| "duplicate tag assignment" | Remove the duplicate entry from `abac.auto.tfvars` |
| "undefined group in principals" | Add the group to the `groups` block, or fix the typo in the policy's `principals` |
| "tag key not defined in tag_policies" | Add the tag key to `tag_policies`, or fix the key name in tag_assignments |
| "per-catalog policy limit exceeded" | Remove lower-priority policies or consolidate overlapping ones |

After fixing errors, re-run `make validate-generated` until clean.

## Apply

```bash
make apply [ENV=dev]
```

This runs Terraform in order across 3 layers:
1. **Account** (`envs/account/`): creates groups, tag policies
2. **Data access** (`envs/<env>/data_access/`): applies tag assignments, masking functions, FGAC policies
3. **Workspace** (`envs/<env>/`): creates/updates Genie Spaces, sets ACLs, deploys entitlements

### First apply

On first apply, `make apply` automatically:
- Splits `generated/abac.auto.tfvars` into the 3 layers
- Copies `masking_functions.sql` to `data_access/`
- Runs `terraform init` + `apply` for each layer

### Errors during apply

- "already exists" → run `make import ENV=<env>` to adopt existing resources, then retry
- "Provider produced inconsistent result" → re-run `make apply` (tag policy state reconciliation)
- "oauth-m2m: invalid_request" → check credentials in `auth.auto.tfvars`
- Tag policy visibility delay → wait 5 min or run `make wait-tag-policies`, then retry

See `references/troubleshooting.md` for more error recovery patterns.

## Promote (Dev → Prod)

### Step 1: Auto-Detect Catalogs

Read `envs/<source>/env.auto.tfvars` and extract catalog names from `genie_spaces[*].uc_tables`. For example, if tables are:
```
dev_fin.finance.transactions
dev_fin.finance.customers
dev_clinical.clinical.encounters
```
The source catalogs are: `dev_fin`, `dev_clinical`.

### Step 2: Build Catalog Map

For each source catalog, ask the user for the production catalog name. Suggest convention: `dev_X` → `prod_X`.

```
Source catalogs detected: dev_fin, dev_clinical
Suggested mapping: dev_fin=prod_fin, dev_clinical=prod_clinical
Is this correct? (or provide your own mapping)
```

### Step 3: Execute Promotion

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod \
  DEST_CATALOG_MAP="dev_fin=prod_fin,dev_clinical=prod_clinical"
```

This:
- Creates `envs/prod/` directory structure
- Remaps all catalog references from dev → prod
- Splits config into account/data_access/workspace layers
- Validates the promoted config

### Step 4: Configure Prod Credentials

Remind the user:
```
Edit envs/prod/auth.auto.tfvars with your production workspace credentials.
The account_id and client_id/secret may be the same, but workspace_id and workspace_host must point to the prod workspace.
```

### Step 5: Apply to Prod

```bash
make apply ENV=prod
```

**Always confirm with the user before applying to production.**

### Post-Deploy Verification

After successful apply:
1. Read `workspace_host` from `envs/<env>/auth.auto.tfvars`
2. Tell the user: "Your Genie Space is deployed. Open <workspace_host> and navigate to Genie to verify the space is accessible and governance is working."
3. Suggest testing: "Ask a question in the Genie Space. Verify that masked columns show redacted values for non-privileged users."
