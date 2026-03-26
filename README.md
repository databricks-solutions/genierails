# GenieRails

Put Genie onboarding on rails — with built-in guardrails. GenieRails imports your existing Genie Space, generates ABAC governance (groups, tags, masking, row filters), and promotes everything to production. No Terraform to write.

## Getting Started

| Starting point | You have... | Guide |
|---|---|---|
| **I already have a Genie Space** | A space configured in the Databricks UI that needs governance and promotion to prod | [From UI to Production](shared/docs/from-ui-to-production.md) |
| **I'm starting from scratch** | Tables in Unity Catalog, no Genie Space yet | [Quickstart](shared/docs/quickstart.md) |

Pick your cloud and set up credentials:

| My workspace is on... | Start here |
| --- | --- |
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

## Documentation

- [From UI to Production](shared/docs/from-ui-to-production.md) — import your existing Genie Space, add governance, promote to prod
- [Quickstart](shared/docs/quickstart.md) — create a Genie Space from scratch
- [Playbook](shared/docs/playbook.md) — next steps after first deployment: add spaces, promote, overlays, advanced scenarios
- [Architecture](shared/docs/architecture.md) — layers, artifact ownership, config files, Genie Space lifecycle
- [Central Governance, Self-Service Genie](shared/docs/self-service-genie.md) — central ABAC team + BU teams self-serve Genie spaces
- [CI/CD Integration](shared/docs/cicd.md) — validate and deploy from a pipeline
- [Troubleshooting](shared/docs/troubleshooting.md) — imports, provider quirks, brownfield workflows
- [Advanced Usage](shared/docs/advanced.md) — IDP-synced groups, ABAC-only mode, masking UDF reuse, legacy migration
- [Country & Region Overlays](shared/docs/country-overlays.md) — region-specific PII governance (ANZ, India, Southeast Asia)
- [Industry Overlays](shared/docs/industry-overlays.md) — industry-specific masking and access patterns (Financial Services, Healthcare, Retail)
- [Integration Testing](shared/docs/integration-testing.md) — unit tests, integration scenarios, test data
