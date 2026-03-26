# CI/CD Integration

This document explains how to integrate the quickstart into a CI/CD process.

## Recommended Model

Use this split of responsibilities:

- Local or developer workflow:
  - run `make generate`
  - review and tune `generated/abac.auto.tfvars`
  - review and tune `generated/masking_functions.sql`
  - run `make validate-generated`
  - commit the reviewed config changes
- CI workflow:
  - validate committed config
  - run `make plan` for the target environment
  - run `make apply ENV=<env>` on approved branches

This keeps LLM-driven generation and human review out of the automated deployment path, while still letting CI own repeatable validation and rollout.

## What Should Be Committed

Commit the reviewed environment and layer config:

- `envs/account/abac.auto.tfvars`
- `envs/<env>/abac.auto.tfvars`
- `envs/<env>/data_access/abac.auto.tfvars`
- `envs/<env>/data_access/masking_functions.sql`
- `envs/<env>/env.auto.tfvars`

Do not commit secrets or local state:

- `envs/<env>/auth.auto.tfvars`
- `envs/account/auth.auto.tfvars`
- Terraform state files
- local apply fingerprint files
- fetched local DDL snapshots unless you intentionally want them in version control

## Secrets in CI

Your pipeline should inject credentials at runtime rather than committing `auth.auto.tfvars`.

For each target environment, create `envs/<env>/auth.auto.tfvars` during the job from secret values such as:

- `databricks_account_id`
- `databricks_client_id`
- `databricks_client_secret`
- `databricks_workspace_id`
- `databricks_workspace_host`

If the account layer or `data_access` layer needs different credentials, write those layer-specific auth files separately instead of relying on the default symlinks.

## Recommended Pipeline Stages

## 1. Validate on pull requests

Use PR validation to make sure the committed config is internally consistent.

Typical steps:

```bash
make validate ENV=dev
make validate ENV=prod
```

If the change includes fresh generated drafts that have not yet been split, also run:

```bash
make validate-generated ENV=dev
```

## 2. Plan before deploy

For a target environment, create the auth file from CI secrets, then run:

```bash
make plan ENV=dev
make plan ENV=prod
```

This shows the net change across the layered state model:

1. shared `account`
2. env-scoped `data_access`
3. env-scoped `workspace`

## 3. Apply on approved branches

After approval, deploy with:

```bash
make apply ENV=dev
make apply ENV=prod
```

`make apply ENV=<workspace>` already handles the required ordering:

1. shared account layer
2. env-scoped governance layer
3. env-scoped workspace layer

## Promotion in CI/CD

There are two common models:

### Model A: Promote locally, deploy in CI

Recommended for most teams.

1. A developer runs:

   ```bash
   make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP="dev_catalog=prod_catalog"
   ```

2. The promoted config is reviewed and committed
3. CI runs `make plan ENV=prod`
4. CI runs `make apply ENV=prod` after approval

This is the best model when you want promotion to stay explicit and reviewable in Git.

### Model B: Generate independent environments

Use this for separate business units or environments that should not inherit `dev` governance.

1. A developer runs:

   ```bash
   make generate ENV=bu2
   ```

2. The generated config is reviewed and committed
3. CI validates and applies `ENV=bu2`

## Ready-to-Use GitHub Actions Workflows

Template workflows are included at `.github/workflows/` inside each cloud wrapper (`aws/` and `azure/`):

- `validate.yml` — runs `make validate` on every pull request; no Databricks credentials needed.
- `deploy.yml` — runs `make apply` on merge to `main`; writes `auth.auto.tfvars` from GitHub Secrets and syncs Terraform state.

### AWS (`aws/.github/workflows/`)
- Uses `aws-actions/configure-aws-credentials@v4` for S3 state backend
- State stored at `s3://<TF_STATE_BUCKET>/genie-aws/envs/...`

### Azure (`azure/.github/workflows/`)
- Uses `azure/login@v2` with OIDC federated credentials for Azure Blob Storage state backend
- State stored at `https://<TF_STATE_STORAGE_ACCOUNT>.blob.core.windows.net/<TF_STATE_CONTAINER>/genie-azure/envs/...`

Because GitHub Actions workflows must live at `.github/workflows/` **at the repository root**, you need to copy them there to activate them:

```bash
# From the repository root (AWS):
cp -r uc-quickstart/utils/genie/aws/.github/workflows/validate.yml .github/workflows/genie-aws-validate.yml
cp -r uc-quickstart/utils/genie/aws/.github/workflows/deploy.yml   .github/workflows/genie-aws-deploy.yml

# Azure:
cp -r uc-quickstart/utils/genie/azure/.github/workflows/validate.yml .github/workflows/genie-azure-validate.yml
cp -r uc-quickstart/utils/genie/azure/.github/workflows/deploy.yml   .github/workflows/genie-azure-deploy.yml
```

If this folder is later promoted to its own top-level repository, the workflows are ready as-is — place `.github/workflows/` at the new repo root and they will activate without modification.

---

## Schema Drift Detection in CI

Use `make audit-schema` as a scheduled CI check to detect when new columns need governance:

```bash
make audit-schema ENV=prod
```

This exits `1` if untagged sensitive columns are found (forward drift) or if existing tag assignments reference deleted columns (reverse drift). Use GitHub's built-in failed-run notifications to alert when drift is detected.

When drift is found, a developer runs `make generate-delta ENV=prod` locally to classify new columns and remove stale assignments, then commits the result for CI to deploy.

---

## Notes and Gotchas

- Avoid running `make generate` automatically in CI unless you intentionally want LLM output in the pipeline. Most teams should generate locally, review, then commit the result.
- `make apply ENV=<workspace>` also applies the shared account layer, so your CI user must be authorized for both account and workspace operations.
- If you deploy multiple environments from the same repo, parameterize `ENV` and inject the matching workspace secrets per environment.
- Destroy should usually be a separate manual workflow, for example `make destroy ENV=dev`, rather than part of the normal deployment pipeline.
- If you are adopting existing Databricks resources, run the import workflow first and let CI manage them only after they are in state.
