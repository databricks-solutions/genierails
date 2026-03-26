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
