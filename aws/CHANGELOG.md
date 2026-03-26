# Changelog

All notable changes to GenieRails are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [Unreleased]

### Added

- **`genie_only = true` minimal-privilege SP** (`make test-genie-only`): Reduced
  the required SP role from Workspace Admin to **workspace USER + SQL entitlement**.
  The integration test creates a dedicated SP with no admin roles, grants it
  CAN USE on a warehouse and UC table access, and verifies end-to-end that
  `genie_only = true` produces zero account-level resources in Terraform state.
  BYO warehouse (`sql_warehouse_id`) is required in this mode.
- **UC table grant documentation**: `genie_only` mode requires explicit
  `USE CATALOG`, `USE SCHEMA`, `SELECT`, and `CAN USE` warehouse grants from
  the governance team. Updated GENIE_SPACE_PERMISSIONS.md, self-service-genie.md,
  and README.md.

## [0.2.0] - 2026-03-12

### Added

- **Multi-state Terraform architecture**: Split Terraform into three independent
  state layers — `account` (shared tag policies/groups), `env` (catalog-scoped
  governance), and `workspace` (Genie Spaces/masking functions). Each layer can
  be planned, applied, and destroyed independently, enabling safe dev → prod
  promotion and isolated teardown.
- **Multi-space / multi-catalog support**: `generate_abac.py` gains a `--space`
  flag for incremental per-space ABAC regeneration. New
  `scripts/merge_space_configs.py` additively merges a single space's config
  into the shared `abac.auto.tfvars` without touching other spaces. Terraform
  workspace module deploys multiple Genie Spaces via `for_each`.
  `DEST_CATALOG_MAP` supports comma-separated multi-catalog mappings.
- **GitHub Actions CI/CD**: `validate.yml` runs `make validate` on every PR
  (no Databricks credentials required); `deploy.yml` runs `make apply` on merge
  to main, writing auth from GitHub Secrets and syncing Terraform state via S3.
  Workflows live inside `aws/` so they travel with the codebase.
- **Integration test pipeline**: `scripts/setup_test_data.py` creates dev/prod
  Unity Catalog catalogs with realistic PII/PHI/PCI schemas and sample data.
  `Makefile` `integration-test` target orchestrates the full pipeline:
  setup → generate → apply → verify → promote → apply prod → verify prod →
  teardown.
- **Genie Space SQL snippets and join specs**: Added `genie_sql_filters`,
  `genie_sql_measures`, `genie_sql_expressions`, and `genie_join_specs` to the
  serialized Genie Space config for improved SQL generation accuracy.
- **Databricks telemetry**: All API calls (Python SDK, curl) now carry a
  `genie-abac-quickstart/0.1.0` User-Agent product identifier via
  `databricks.sdk.useragent` and `DATABRICKS_USER_AGENT_EXTRA`.
- **Two-step Genie Space create-then-patch**: Creation now uses a two-step
  pattern (CREATE, then PATCH) since the CREATE endpoint does not accept
  SQL snippets or join specs.
- **`docs/cicd.md`**: Pipeline activation guide with workflow configuration
  reference.
- **`docs/integration-testing.md`**: CLI flag reference, full pipeline
  walkthrough, and individual test scenario documentation.

### Changed

- **Project renamed** from OneReady to **GenieRails**.
- `terraform.tfvars` renamed to `abac.auto.tfvars` for Terraform auto-loading.
- `ABAC_PROMPT.md` updated with unambiguous benchmark rules, business default
  instructions, and domain-adaptive generation guidance.
- `TUNING.md` restructured around a Genie accuracy review checklist.
- Quickstart docs split into focused per-topic guides; prerequisites moved to a
  top-level section with Service Principal role details.
- Databricks FMAPI timeout increased to 600s for larger prompt responses.
- Terraform default parallelism set to `-parallelism=1` for safer applies.

### Fixed

- FGAC policies capped at 8 per catalog to respect the platform limit of 10.
- Switched FGAC policy operations to REST API (Databricks SDK lacks
  `policy_infos` support).
- Rewrote `extract_grants()` to use REST API instead of SDK `grants.get()`.
- Added SSL context to group SCIM lookups to avoid certificate verification
  failures.
- Corrected `databricks_policy_info` import ID format and tag assignment import
  format.
- Added SQL pre-cleanup to remove stale entity tag assignments before import.
- Pre-check API/SQL before attempting grants and FGAC policy imports.
- Skip tag policy imports for policies that do not exist in the account.
- Auto-import account resources before `apply` to avoid state drift.
- Look up group SCIM IDs for import; fix attach-promote scenario.
- Pass `-lock=false` to `terraform destroy` to bypass stale state locks.
- Three idempotency fixes for integration test stability (preamble cleanup
  between scenarios, Genie title-node conflict handling, space name injection
  into ABAC prompt with `serialized_space` retry).
- Removed duplicate `ua.with_extra`/`with_product` calls that caused duplicate
  User-Agent entries.
- Prevent multiple column masks per column in ABAC prompt and tag policies.
- Resolved tag policy reordering bug that caused non-deterministic `abac.auto.tfvars` diffs.

---

## [0.1.0] - 2026-02-20

### Added

- Initial ABAC Terraform module with entity tag assignments and FGAC policies.
- AI-assisted ABAC generation via `generate_abac.py` and `ABAC_PROMPT.md`.
- `validate_abac.py` to check AI-generated configs before `terraform apply`.
- Genie Space lifecycle automation (create, configure ACL, destroy) via Terraform.
- Multi-catalog ABAC with auto-deploy and `destroy` support.
- Finance and healthcare domain examples.
- Genie Space AI config (SQL snippets seed), three-file config split.
- Streamlined onboarding via `make setup && make generate && make apply`.
