# Changelog

All notable changes to GenieRails are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [Unreleased]

### Added

- **Optional AI helpers** (design 18.4, decision 5): `make suggest-treatment
  ENV=dev CLASS=<class>`, `make draft-genie-content ENV=dev SPACE=<agent>` and
  `make draft-custom-mask ENV=dev TREATMENT=<name> TYPE=<sql-type>` ask a
  model for a draft and write it only under `envs/dev/drafts/`, for review
  and commit; `make test-custom-mask ENV=dev TREATMENT=<name>` runs the mask
  library's exact-output checks on a drafted mask against the dev SQL
  warehouse. They are the only commands that call a model, run only with
  `ENV=dev` at the checkout's real `envs/dev`, refuse in CI, never follow a
  symlink, send the model only schema (never data or existing agent text),
  and need `ai_helper_endpoint` in `envs/dev/env.auto.tfvars`. Fixture inputs
  must be typed literals. Nothing reads `drafts/`: committing a draft changes no
  rehearse fingerprint and no `make check` result.
- **Adopting a custom mask** (design 18.4.1): `make adopt-custom-mask ENV=dev
  TREATMENT=custom_<name>` copies a draft that `make test-custom-mask` passed
  into the governance-owned `envs/dev/custom_masks/custom_<name>.json` and
  prints the `tag_treatments` line to add; it calls no model and refuses in
  CI (no opt-out), outside the real `envs/dev`, without the `custom_` prefix,
  without a pass record matching the draft, when the offline rules fail, or
  when a never-raw class maps to it. `generate`, `validate`,
  `scaffold-treatments` and `verify-access` treat an adopted mask like a
  library treatment (its full version is always the library's redaction);
  `validate` and `make check` refuse a file whose `tested_sha256` no longer
  matches; rehearse and release re-run its cases on the env's warehouse;
  changing which custom SQL a partial tier sees needs a `weaken`
  acknowledgement (`custom_mask_changed`); `make promote-to` copies
  `custom_masks/` as governance files. Draft names now need the `custom_`
  prefix. Envs without `custom_masks/` are unchanged.
- **Persona file ownership** (design 18.1): agent settings live only in
  `envs/<env>/agents/` files and every reader merges them (`generate`, the
  Genie ACL sidecar, `promote`), so an env with its agents in files promotes;
  `generate` never imports a captured agent's live config into `generated/`.
  `make promote-to` prints the files it changed by owner: an agent-only
  promotion changes only `agents/` files and needs no governance approval.
  `shared/examples/CODEOWNERS.example` is a sample that gives `agents/` to
  data citizens and the rest of `envs/` and `shared/` to governance engineers.
- **Governance sees every agent table change** (design 18.1): an agent owns
  its table list, and `generated/governed_tables.json` now also lists each
  file-defined agent's tables (`agent_tables`). `make generate`, `make
  capture` and `make promote-to` rewrite it, so adding or removing a table,
  even one with no classified column, is a diff in a governance-owned file,
  and `make check` fails while it is stale. It grants nothing; `SELECT` still
  comes only from `table_readers`. A table only an agent file lists is no
  longer copied into the promoted env's `env.auto.tfvars` `uc_tables`. Envs
  whose agents are all inline keep their record as it was. **Upgrade:** an
  env with agent files fails `make check` until you run `make generate`
  (and `make promote-to` for its targets) and commit the result.
- **`make split-agent-files ENV=<env>`**: the explicit migration that moves an
  env's inline `genie_spaces` into `agents/<agent>.auto.tfvars`. It refuses,
  writing nothing, any move that would change what Terraform plans. Envs that
  don't run it keep working unchanged; `make promote-to` refuses while the
  target still keeps inline an agent the source has in a file.
  Unnamed and filename-unsafe display names now split safely: the agent file
  records the old Terraform key independently, preserving every resource
  address, ID file and the UI title (two titles that sanitize alike included).
  `AGENT_FILE_NAMES='old-key=file-name'` overrides the filename. Every script
  (ID files, adoption markers, delete acks, release receipts) and promote-to
  use that key, and promote-to carries the agent's remapped display name.
- **`tag_treatments`** (deterministic envs): `"class.<name>" = "<treatment>"`
  maps a customer class to a mask-library treatment in the env's own
  `env.auto.tfvars`; promotion carries it. `make scaffold-treatments
  ENV=<env> TREATMENT=<treatment>` writes it (for a promote-to target, in its
  source env) and never changes `shared/`.

### Changed

- **`tag_treatments` keys other than `class.<name>`** (for example
  `"sensitivity=confidential"`) are now refused: they were accepted but never
  applied, so the columns stayed unmasked by them. Also refused: remapping a
  class the mask library already maps, and a value that isn't a library
  treatment. **Migration:** a deterministic env that committed such a key now
  fails `make validate` and every command that builds its policies (such as
  `make generate` and `make release`), naming the key. Delete the entry (it
  never masked anything), or tag the column with a `class.*` tag and map that
  (`make scaffold-treatments ENV=<env> TREATMENT=<treatment>`), and review the
  resulting governance diff.
- **`make scaffold-treatments` in a deterministic env** needs `TREATMENT=`; it
  no longer writes REVIEW stubs into `shared/` for those envs (legacy envs are
  unchanged).

### Fixed

- **The champion walkthrough sets who may run the agent**: an agent given by ID
  without `acl_groups` is refused by `make generate` (decision 4), but the
  dev-to-prod template, README and live champion scenarios never set it, and
  the template promised that omitting it derives it. Each agent now has its own
  file, `envs/dev/agents/<agent name>.auto.tfvars`, from the new
  `shared/examples/dev_to_prod/agent.auto.tfvars.example` (its `genie_space_id`
  and `acl_groups`; `[]` means nobody), and the env template keeps
  `genie_spaces = []`. The refusal is unchanged. The placeholder guard also
  checks agent files (an unfilled ID or `acl_groups` group stops every guarded
  target), `make generate` captures a new agent before writing
  `generated/governed_tables.json` so its first run records the agent's tables,
  and `make enable-classification` accepts an agent kept in a file.
  `make generate` imports only agents whose capture was never committed: if a
  committed `agents/<agent>.space.json` is gone, it stops before writing
  anything and points to `make capture ENV=dev SPACE="<agent>"`, instead of
  quietly re-capturing the live dev UI config. The e2e
  driver's champion writes agent A's file from the required `dev_agent_id` and
  `acl_groups` values. **Upgrade:** none; existing env files are not changed.

- **A Genie agent configured only by `genie_space_id` promotes from a clean
  clone**: `make generate` (and `capture`) now also record such an agent's
  discovered tables in the committed `generated/governed_tables.json`, under
  `agent_tables` keyed by its `genie_space_id`. `make promote-to`,
  `make check` and the access and coverage checks of `make rehearse`,
  `release-plan` and `release` read them from there in a deterministic env,
  never from the
  git-ignored `data_access/discovered_uc_tables.auto.tfvars` or the Genie API,
  so CI and a fresh clone no longer fail with `no tables found for agent`.
  Without that entry they refuse with `run make generate ENV=dev and commit
  generated/`. `make check` and `make promote-to CHECK=1` now copy only the
  env files git tracks into their scratch copy, so an ignored or untracked
  local file can't change their result. Legacy envs and inline agents that
  list `uc_tables` are unchanged. **Upgrade:** for an env whose agent is given
  by ID only, run `make generate ENV=dev` and commit `generated/`.

### Removed

- **`make certify`**: removed. It had become an alias for `make release`, which
  grants business access, so a pipeline running `certify` → approval →
  `release` would have granted access before the approval. `make certify` now
  exits non-zero with: *certify was removed; run `make release ENV=<env>` (it
  runs the coverage check, audit and verification)*. To review before granting,
  run `make plan ENV=<env>` and put `make release` behind your approval step.

### Changed

- **Coverage-gap messages**: when `release`, `maintain`, `coverage-gate` or
  `audit-rulebook` stop on an unmapped `class.*` tag, they print both fixes
  (reuse an existing treatment, or a new one) with exact commands and the
  treatments that fit each column's type. `release` and `maintain` now report
  rulebook drift as drift; they used to call every audit failure an error,
  because a failed nested `make` always exits 2.
- **`make scaffold-treatments ENV=prod`** keeps prod's promoted masks under their
  deployed names when it adds a new treatment. It used to rebuild prod's
  generated masks from assignments, which promotion leaves empty, so only the
  new mask was left.
- **Masking SQL quiet-plan hash** now uses the deployer's exact statement
  blocks and execution contexts. Upgrading changes the hash format once more,
  causing at most one drop-free `CREATE OR REPLACE` pass; reorder-only generated
  functions are quiet after that transition.
- **Migration — UI-managed Data Classification auto-tagging**: new environment
  templates leave `enable_auto_tagging` unset so rehearse/release/maintain preserve
  the Catalog Explorer setting. If an environment created from an older template
  contains `enable_auto_tagging = false`, delete that line before the next run.
  GenieRails now refuses to disable live UI auto-tagging, and `make promote-to`
  refuses to preserve explicit `false`, unless the operator explicitly passes
  `ALLOW_DISABLE_AUTO_TAGGING=1`.

- **No row-pairing key to choose**: `verify-access` picks a provably safe key
  per masked table: an explicit `verify_key_columns` entry, else
  `VERIFY_KEY_COLUMN` / `verify_key_column` when the table has it, else its
  single-column `PRIMARY KEY`, else an unmasked id-like column (`<table>_id`,
  `id`, other `*_id`). A column that looks sensitive (the coverage check's
  name rule, or a sensitive `class.*` tag) is never auto-picked; an override
  that does is used with a warning. Every key, an override included, must
  prove unique, non-null and unmasked. A failing auto-pick falls through to
  the next candidate; a failing override is reported. A table with nothing
  provable fails the run (rehearse and release alike) and is named.
  `make rehearse ENV=dev` and `make release ENV=prod` need no key flag. Only
  a run that proved every masked table's key saves them, as exactly that
  run's `verify_key_columns` (stale saved entries removed, your own kept), and
  promote carries them with table names remapped. `make release` proves every
  masked table's key as the admin (`make verify-access-keys`) before it takes
  its lock and again before it applies any access, and no longer refuses up
  front for want of a configured key.
- **Withdrawing access never waits for the coverage check**: without a pass,
  Terraform withholds only new table `SELECT` grants and new Genie `CAN_RUN`
  groups; removals, unchanged access and other agents still apply, and `make`
  exits non-zero naming what it withheld. Keeping a grant whose protection was
  weakened (a tag, policy or mask removed or changed) still needs a pass.
- **Removing a group or Genie agent revokes its `CAN_RUN`**: Terraform takes back
  the `CAN_RUN` GenieRails granted (only those groups; other direct entries and
  inherited permissions are kept) instead of leaving it live.
- **`make maintain`** now runs `audit-rulebook` before `apply-governance`: drift or
  an audit error stops before anything is applied.

### Added

- **Migration from legacy governance** (`make migrate-plan ENV=<env>`): a
  read-only dry run that reports, per column and principal, what changes when a
  legacy env opts in to deterministic governance, every `SELECT` and `CAN_RUN`
  the cutover revokes, and a proposed `table_readers` reproducing today's grants.
  The cutover runs through `rehearse` (dev) or `release-plan` / `release
  MANIFEST=` (one manifest per phase): legacy tags move to their stable keys
  with `terraform state mv`, new policies are created first, columns are
  retagged in place, and the legacy policies stay until a later release.
  Weakenings and revocations need `ack.txt` entries (`<env>:revoke:...`);
  legacy row filters stay frozen. See `shared/docs/legacy-migration.md`.

- **Fixing a coverage gap prod found**: `make scaffold-treatments ENV=<env>
  TREATMENT=<existing>` maps each unmapped `class.*` tag to an existing
  treatment in `shared/treatment_config.json`. It refuses before writing if the
  treatment doesn't exist or its masking function's input type doesn't fit the
  column, or that derivation would replace for that column (an unreadable
  type refuses too; `ALLOW_UNKNOWN_TYPE=1` overrides). It changes only
  `class_labels`.
  For a new kind of mask, `make materialize-treatment ENV=dev TREATMENT=<t>`
  adds that treatment's mask policy and function for every governed dev catalog,
  without tag assignments (never in prod or another promotion target), so
  `rehearse` → `promote-to` → `release` carries it
  to prod. No matching dev column is needed and no rules are copied by hand.
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
  governance), and `workspace` (Genie agents/masking functions). Each layer can
  be planned, applied, and destroyed independently, enabling safe dev → prod
  promotion and isolated teardown.
- **Multi-space / multi-catalog support**: `generate_abac.py` gains a `--space`
  flag for incremental per-space ABAC regeneration. New
  `scripts/merge_space_configs.py` additively merges a single space's config
  into the shared `abac.auto.tfvars` without touching other spaces. Terraform
  workspace module deploys multiple Genie agents via `for_each`.
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
- **Genie agent SQL snippets and join specs**: Added `genie_sql_filters`,
  `genie_sql_measures`, `genie_sql_expressions`, and `genie_join_specs` to the
  serialized Genie agent config for improved SQL generation accuracy.
- **Databricks telemetry**: All API calls (Python SDK, curl) now carry a
  `genie-abac-quickstart/0.1.0` User-Agent product identifier via
  `databricks.sdk.useragent` and `DATABRICKS_USER_AGENT_EXTRA`.
- **Two-step Genie agent create-then-patch**: Creation now uses a two-step
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
- Genie agent lifecycle automation (create, configure ACL, destroy) via Terraform.
- Multi-catalog ABAC with auto-deploy and `destroy` support.
- Finance and healthcare domain examples.
- Genie agent AI config (SQL snippets seed), three-file config split.
- Streamlined onboarding via `make setup && make generate && make apply`.
