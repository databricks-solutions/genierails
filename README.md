# GenieRails

Put Genie onboarding on rails — with built-in guardrails. GenieRails generates ABAC governance, masking functions, and Genie Spaces from a small set of input files so you can get business users into Genie quickly without editing Terraform.

## Getting Started

Pick whichever cloud your Databricks workspace runs on:

| My workspace is on… | Start here |
| ------------------- | ---------- |
| AWS   | [`aws/README.md`](aws/README.md) |
| Azure | [`azure/README.md`](azure/README.md) |

### Repository Layout

```
genie/
├── aws/            Cloud wrapper for AWS deployments
├── azure/          Cloud wrapper for Azure deployments
└── shared/         All shared code (Terraform modules, scripts, tests, docs)
```

`aws/` and `azure/` are the entry points — always run `make` commands from one of these directories. You never run commands from `shared/` directly; it holds all the Terraform modules, Python scripts, and docs, and is invoked automatically through the cloud wrapper.

### Quickstart (same for both clouds)

```bash
cd aws/   # or azure/
make setup
vi envs/dev/auth.auto.tfvars      # service principal credentials
vi envs/dev/env.auto.tfvars       # your tables and Genie Space name

make generate
make validate-generated
make apply
```

## Documentation

All docs live in `shared/docs/`:

- [Playbook](shared/docs/playbook.md) — use cases: quickstart, ABAC-only, multi-space, existing spaces, promotion, self-service Genie, destroy
- [Architecture](shared/docs/architecture.md) — layers, artifact ownership, config files, Genie Space lifecycle
- [Central Governance, Self-Service Genie](shared/docs/self-service-genie.md) — central ABAC team + BU teams self-serve Genie spaces
- [CI/CD Integration](shared/docs/cicd.md) — validate and deploy from a pipeline
- [Troubleshooting](shared/docs/troubleshooting.md) — imports, provider quirks, brownfield workflows
- [Advanced Usage](shared/docs/advanced.md) — IDP-synced groups, ABAC-only mode, masking UDF reuse, legacy migration
- [Country & Region Overlays (APJ)](shared/docs/country-overlays.md) — using, tuning, or adding country-specific PII governance (ANZ, India, Southeast Asia); contributor guide for new regions
- [Integration Testing](shared/docs/integration-testing.md) — unit tests, integration scenarios, test data

## Testing

```bash
cd aws/              # or azure/
make test-unit       # fast unit tests (~1s, no credentials)
make test-ci         # full CI: provision → integration tests → teardown
```
