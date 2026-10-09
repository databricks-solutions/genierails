# Changelog — Azure

## [Unreleased]

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
- **Australian Bank Demo**: End-to-end demo documentation now covers Azure
  alongside AWS. Run the full dev-to-prod walkthrough (provision, generate, apply, promote,
  teardown) from `cd azure/` with `account-admin.azure.env` credentials.
  See [`shared/examples/legacy/aus_bank_demo/`](../shared/examples/legacy/aus_bank_demo/).

## 0.1.0 (2026-03-23)

- Initial Azure support
- Shared module architecture with cloud-specific wrappers
- ADLS Gen2 storage with Access Connector for Unity Catalog
- Azure Blob Storage for CI/CD Terraform state
- All Terraform modules, scripts, and Python tools shared with AWS
