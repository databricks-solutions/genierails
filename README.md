# GenieRails

Put Genie onboarding on rails — with built-in guardrails. GenieRails imports your existing Genie Space, generates ABAC governance (groups, tags, masking, row filters), and promotes everything to production. No Terraform to write.

## Getting Started

**Already have a Genie Space?** Most users do — follow the [Playbook](shared/docs/playbook.md) to import it, add governance, and promote to prod.

**Starting from scratch?** See the [Quickstart](shared/docs/playbook.md#quickstart-from-scratch) in the Playbook.

Pick your cloud and set up credentials:

| My workspace is on… | Start here |
| ------------------- | ---------- |
| AWS   | [`aws/README.md`](aws/README.md) |
| Azure | [`azure/README.md`](azure/README.md) |

### Repository Layout

```
genierails/
├── aws/            Cloud wrapper for AWS deployments
├── azure/          Cloud wrapper for Azure deployments
└── shared/         All shared code (Terraform modules, scripts, tests, docs)
```

`aws/` and `azure/` are the entry points — always run `make` commands from one of these directories. `shared/` holds all Terraform modules, Python scripts, and docs, and is invoked automatically through the cloud wrapper.

### Quick commands (same for both clouds)

```bash
cd aws/   # or azure/
make setup
vi envs/dev/auth.auto.tfvars      # service principal credentials
vi envs/dev/env.auto.tfvars       # your Genie Space ID (or tables for a new space)

make generate
make validate-generated
make apply
```

## Documentation

All docs live in `shared/docs/`:

- [Playbook](shared/docs/playbook.md) — start here: import your existing Genie Space, add governance, promote to prod (also covers quickstart, advanced scenarios)
- [Architecture](shared/docs/architecture.md) — layers, artifact ownership, config files, Genie Space lifecycle
- [Central Governance, Self-Service Genie](shared/docs/self-service-genie.md) — central ABAC team + BU teams self-serve Genie spaces
- [CI/CD Integration](shared/docs/cicd.md) — validate and deploy from a pipeline
- [Troubleshooting](shared/docs/troubleshooting.md) — imports, provider quirks, brownfield workflows
- [Advanced Usage](shared/docs/advanced.md) — IDP-synced groups, ABAC-only mode, masking UDF reuse, legacy migration
- [Country & Region Overlays (APJ)](shared/docs/country-overlays.md) — using, tuning, or adding country-specific PII governance (ANZ, India, Southeast Asia); contributor guide for new regions
- [Industry Overlays](shared/docs/industry-overlays.md) — industry-specific masking, group templates, and access patterns (Financial Services, Healthcare, Retail); contributor guide for new industries
- [Integration Testing](shared/docs/integration-testing.md) — unit tests, integration scenarios, test data

## Testing

```bash
cd aws/              # or azure/
make test-unit       # fast unit tests (~1s, no credentials)
make test-ci         # full CI: provision → integration tests → teardown
```
