# Troubleshooting

This document covers import flows, brownfield adoption, and common provider issues.

## Importing Existing Resources (Brownfield)

If groups, tag policies, tag assignments, or FGAC policies already exist in Databricks, import them so Terraform can manage them without `already exists` errors:

```bash
make import ENV=account      # import account groups + tag policies into module.account
make import                  # import env-scoped governance + workspace-local resources for ENV=dev
make import ENV=prod         # import env-scoped governance + workspace-local resources for ENV=prod

cd envs/account && ../../scripts/import_existing.sh --groups-only --dry-run
cd envs/account && ../../scripts/import_existing.sh --tags-only --dry-run   # tag policies live in account
cd envs/dev/data_access && ../../../scripts/import_existing.sh --fgac-only
cd envs/dev/data_access && ../../../scripts/import_existing.sh --tag-assignments-only
```

### Brownfield workflow

For environments with existing ABAC infrastructure:

```bash
make generate
vi envs/dev/generated/masking_functions.sql
vi envs/dev/generated/abac.auto.tfvars
make promote
make import ENV=account
make import
make migrate-state           # only needed if this env already has old mixed state
make plan
make apply
```

## Common Issues

### "Provider produced inconsistent result after apply" (tag policies)

A known Databricks provider bug can reorder tag policy values after creation, causing a Terraform state mismatch. The tag policies themselves are usually created correctly; the failure is in provider/state reconciliation.

`make apply` reduces this significantly by:

- running `make sync-tags` through the Databricks SDK against the shared account layer before applying it
- keeping `ignore_changes = [values]` on `databricks_tag_policy`

That said, you may still occasionally see this error during the account apply, especially when creating or adopting policies for the first time. In that case:

1. Re-run `make apply`
2. If it still fails, import the affected policies into the account state and retry

Manual recovery:

```bash
cd envs/account

python3 -c "import hcl2; d=hcl2.load(open('abac.auto.tfvars')); [print(tp['key']) for tp in d.get('tag_policies',[])]" | \
  while read key; do
    ../../scripts/terraform_layer.sh account account state-rm "module.account.databricks_tag_policy.policies[\"$key\"]" 2>/dev/null || true
    ../../scripts/terraform_layer.sh account account import "module.account.databricks_tag_policy.policies[\"$key\"]" "$key" || true
  done

make apply
```

### "already exists"

Resources such as groups or tag policies already exist in Databricks. Import them so Terraform can manage them:

```bash
make import ENV=dev
```

### Destroy fails while dropping masking functions

If a previous partial destroy removed the Terraform-managed SP grant before masking functions were dropped, rerun with the current code first. The current implementation keeps the SP grant ordered correctly during destroy and can temporarily re-establish the required catalog and schema access during masking-function teardown.

If you are still recovering an older partial state:

1. Re-run `make destroy ENV=<workspace>`
2. If needed, `make apply ENV=<workspace>` first, then destroy again
3. Only destroy `ENV=account` after the workspace environments that depend on it are gone

### LLM generation fails with "groups is missing or empty"

The Foundation Model API sometimes returns truncated output, especially with complex schemas (many tables/columns) or long overlay prompts.

**Solutions:**
1. Re-run `make generate` — LLM output is non-deterministic, retries often succeed
2. Reduce prompt complexity: use fewer tables (`SPACE="Single Space"`) or fewer overlays
3. Try `make generate --dry-run` to inspect the prompt without calling the LLM
4. If using country + industry overlays together, try generating with just one overlay first
5. Keep each Genie Space to 4-8 tables for reliable generation

### LLM generates wrong masking functions for columns

The LLM may misclassify columns (e.g., applying `mask_pan_india` to credit card PAN instead of India tax PAN).

**Solutions:**
1. Use unambiguous column names: `pan_number` instead of `pan`, `card_number` instead of `pan`
2. Add clear column COMMENTs in your DDL — the LLM reads these during generation
3. Edit `envs/<env>/generated/abac.auto.tfvars` to fix misclassifications, then run `make validate-generated`
4. The autofix system catches some mismatches (function category vs. column category), but not all

### FGAC policy limit exceeded (max 10 per catalog)

Databricks enforces a platform limit of 10 FGAC policies per catalog. If your schema has many sensitive columns, the LLM may generate more than 10 policies.

**Symptoms:** `make validate-generated` errors with "exceeds Databricks platform limit of 10", or `make apply` fails with a provider error.

**Solutions:**
1. Consolidate policies: use one masking function for multiple columns with the same sensitivity level
2. Use tag-based conditions to group columns (e.g., all `pii_level=masked` columns share one policy)
3. Split tables across multiple catalogs if governance requirements differ
4. The autofix system automatically drops excess policies — review which survived in the generated config

### Terraform state conflicts or corruption

If `make apply` fails partway through, Terraform state may be inconsistent with actual cloud resources.

**Solutions:**
1. Run `make plan` to see what Terraform thinks needs to change
2. If resources exist but aren't in state: `make import ENV=<env>`
3. If state references deleted resources: run `terraform state rm <resource_address>` in the appropriate layer directory
4. As a last resort: `make destroy ENV=<env>` and re-apply from scratch
5. Never edit `.tfstate` files directly

### SQL warehouse not found or fails to create

The governance warehouse may not exist, be stopped, or fail to create.

**Solutions:**
1. If using an existing warehouse: verify `sql_warehouse_id` in `env.auto.tfvars` is correct
2. If auto-creating: ensure the SP has `CAN_MANAGE` entitlement on SQL warehouses
3. Check warehouse status in the Databricks UI — it may be stopped or in error state
4. For serverless warehouses: ensure serverless compute is enabled for your workspace

### Unity Catalog permission errors

Permission errors when fetching table DDL or applying governance.

**Solutions:**
1. Verify the SP has `MANAGE` permission on the catalog (required for tag assignments)
2. For account-level operations (groups, tag policies): the SP needs Account Admin role
3. Check `auth.auto.tfvars` credentials match the correct workspace
4. Run `make setup ENV=<env>` to verify the SP can connect

### Genie Space API errors (rate limiting, timeouts)

The Genie Space REST API may return 429 (rate limit) or timeout errors during import or config push.

**Solutions:**
1. Re-run `make generate` — transient API errors resolve on retry
2. If consistent 403 errors: the SP may not have permission to manage Genie Spaces
3. For large spaces with many tables: the API may timeout — reduce the number of tables per space
4. Check workspace network connectivity if behind a firewall/VPN

### Masking functions not found after deployment

FGAC policies reference masking functions that don't exist in the catalog.

**Solutions:**
1. Run `make apply` again — the masking function deployment may have failed silently on the first attempt
2. Verify the SQL file: `cat envs/<env>/generated/masking_functions.sql` — check for syntax errors
3. Check the catalog and schema exist: functions are created in the same catalog/schema as your tables
4. Verify the SP has `CREATE FUNCTION` privilege on the schema

### Column tags not appearing after apply

Tag assignments were applied but don't appear in the Databricks UI.

**Solutions:**
1. Wait 30-60 seconds — tag propagation is eventually consistent
2. Run `make sync-tags` to force synchronization via the SDK
3. Check `make plan` to see if Terraform thinks the tags need to be created
4. Verify the tag policy exists in the account layer: `make plan ENV=account`
