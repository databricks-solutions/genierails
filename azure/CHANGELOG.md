# Changelog — Azure

## [Unreleased]

### Added

- **Australian Bank Demo**: End-to-end demo documentation now covers Azure
  alongside AWS. Run the full champion flow (provision, generate, apply, promote,
  teardown) from `cd azure/` with `account-admin.azure.env` credentials.
  See [`shared/examples/aus_bank_demo/`](../shared/examples/aus_bank_demo/).

## 0.1.0 (2026-03-23)

- Initial Azure support
- Shared module architecture with cloud-specific wrappers
- ADLS Gen2 storage with Access Connector for Unity Catalog
- Azure Blob Storage for CI/CD Terraform state
- All Terraform modules, scripts, and Python tools shared with AWS
