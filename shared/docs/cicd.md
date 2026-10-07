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
  - validate committed config **and run `make coverage-gate`** (block the build if any classified sensitive column has no covering mask)
  - for prod: enable/wait for native classification, then `make derive-assignments ENV=prod` (re-derive facts from prod's own tags — no LLM), then `make coverage-gate ENV=prod`
  - run `make plan`, then on approved branches `make release ENV=prod` (re-derive → validate → coverage check → rulebook audit → apply → verify-access, under a lock)
  - the shipped `.github/workflows/ci.yml` only runs tests and validation; to deploy from CI, add your own deployment job that runs `make release ENV=prod`, and if you need a human approval before business users get access, attach a protected `environment:` (approval rule) to that job. There is no separate exposure switch

This keeps LLM-driven generation and human review out of the automated deployment path, keeps the LLM out of prod entirely (prod re-derives deterministically), and makes coverage an explicit, enforced check rather than something you hope happened.

> **Prerequisite:** Your configs should be version-controlled before setting up CI/CD. See [Version Control & Standalone Terraform](version-control.md) for what to commit and how to set up git tracking.

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

If the change includes fresh generated drafts that have not yet been split, also run the blocking coverage check and generated-config validation — the coverage check fails the PR if any classified sensitive column has no covering mask:

```bash
make coverage-gate ENV=dev
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

After approval, deploy. **For prod, re-derive facts from prod's own classification first** — never re-run `generate` in prod (that re-invokes the LLM and could drift from the reviewed rules):

```bash
# prod facts: enable/wait for native classification first
make release ENV=prod VERIFY_KEY_COLUMN=<key>
```

`make release` re-derives assignments from prod's own tags (no LLM), validates, runs the coverage check and the rulebook audit, applies all layers in order (masks and policies before grants), then proves masking with `verify-access`. It never re-generates via the LLM. Terraform itself refuses new or wider business `SELECT` / Genie `CAN_RUN` without a recent passing coverage result, so no path can grant access past the gate. The shipped `.github/workflows/ci.yml` has no deployment job, so add one that runs this command; if you want a human approval before a deployment can add or widen access, give that job a protected `environment:`.

## Promotion in CI/CD

There are two common models:

### Model A: Promote locally, deploy in CI

Recommended for most teams.

1. A developer runs:

   ```bash
   make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP="dev_catalog=prod_catalog"
   ```

2. The promoted config is reviewed and committed
3. CI enables/waits for prod native classification, runs `make derive-assignments ENV=prod`, then `make coverage-gate ENV=prod` and `make plan ENV=prod`
4. After approval, CI runs `make release ENV=prod VERIFY_KEY_COLUMN=<key>`

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

When drift is found, prefer letting native classification tag the new columns, then re-derive deterministically with `make derive-assignments ENV=prod` (reuses the promoted rules, no LLM). `make generate-delta` is an exceptional/legacy remediation that invokes the LLM — it is not the routine dev-to-prod path.

---

## Notes and Gotchas

- Avoid running `make generate` automatically in CI — and never in prod. Dev drafts locally with the LLM (reviewed, committed); prod re-derives facts with `make derive-assignments` (no LLM), so prod enforcement can't drift from the reviewed rules.
- Every plan/apply (`make apply`, `plan`, `apply-governance`, `apply-genie`, and `release`) re-reads live UC state and runs the enforced coverage check. Still keep `make coverage-gate` as a required PR check, so uncovered columns fail early on the generated config.
- `make apply ENV=<workspace>` also applies the shared account layer, so your CI user must be authorized for both account and workspace operations.
- If you deploy multiple environments from the same repo, parameterize `ENV` and inject the matching workspace secrets per environment.
- Destroy should usually be a separate manual workflow, for example `make destroy ENV=dev`, rather than part of the normal deployment pipeline.
- If you are adopting existing Databricks resources, run the import workflow first and let CI manage them only after they are in state.
