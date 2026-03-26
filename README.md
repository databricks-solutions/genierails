# GenieRails

Put Genie onboarding on rails — with built-in guardrails. GenieRails imports your existing Genie Space, generates ABAC governance (groups, tags, masking, row filters), and promotes everything to production. No Terraform to write.

## What you get

- **Groups and access control** — role-based groups (e.g. `analyst`, `manager`) with fine-grained row and column policies
- **Column masking** — AI-generated SQL UDFs that mask sensitive data (SSN, credit cards, PII) per group
- **Row-level security** — filter rows by region, department, or any business dimension
- **Tag-based governance** — Unity Catalog tag policies that classify and protect sensitive columns
- **Consumer entitlements** — workspace consume access granted to each group automatically
- **Genie Space ACLs** — `CAN_RUN` permissions so each group can use the Genie Space
- **Genie Space configuration as code** — instructions, benchmarks, SQL measures, all version-controlled
- **Dev → prod promotion** — one command to replicate governance to production with catalog remapping

## Getting Started

Pick your cloud to set up credentials, then follow the guide for your scenario:

| My workspace is on... | Start here |
| --- | --- |
| AWS   | [`aws/README.md`](aws/README.md) |
| Azure | [`azure/README.md`](azure/README.md) |

## Repository Layout

```
genierails/
├── aws/            Cloud wrapper for AWS deployments
├── azure/          Cloud wrapper for Azure deployments
└── shared/         All shared code (Terraform modules, scripts, tests, docs)
```

`aws/` and `azure/` are the entry points — always run `make` commands from one of these directories. `shared/` holds all Terraform modules, Python scripts, and docs, and is invoked automatically through the cloud wrapper.

## Documentation

**Guides:**
- [From UI to Production](shared/docs/from-ui-to-production.md) — import your existing Genie Space, add governance, promote to prod
- [Quickstart](shared/docs/quickstart.md) — create a Genie Space from scratch
- [Playbook](shared/docs/playbook.md) — after first deployment: add spaces, promote, overlays, advanced scenarios

**Reference:**
- [Architecture](shared/docs/architecture.md) — layers, artifact ownership, config files, Genie Space lifecycle
- [Country & Region Overlays](shared/docs/country-overlays.md) — region-specific PII governance (ANZ, India, Southeast Asia)
- [Industry Overlays](shared/docs/industry-overlays.md) — industry-specific masking and access patterns (Financial Services, Healthcare, Retail)
- [Central Governance, Self-Service Genie](shared/docs/self-service-genie.md) — central ABAC team + BU teams self-serve Genie spaces
- [Advanced Usage](shared/docs/advanced.md) — IDP-synced groups, ABAC-only mode, masking UDF reuse, legacy migration
- [CI/CD Integration](shared/docs/cicd.md) — validate and deploy from a pipeline
- [Troubleshooting](shared/docs/troubleshooting.md) — imports, provider quirks, brownfield workflows
- [Integration Testing](shared/docs/integration-testing.md) — unit tests, integration scenarios, test data
