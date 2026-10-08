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

- **Migration — UI-managed Data Classification auto-tagging**: new environment
  templates leave `enable_auto_tagging` unset so rehearse/release/maintain preserve
  the Catalog Explorer setting. If an environment created from an older template
  contains `enable_auto_tagging = false`, delete that line before the next run.
  GenieRails now refuses to disable live UI auto-tagging unless the operator
  explicitly passes `ALLOW_DISABLE_AUTO_TAGGING=1`.

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
