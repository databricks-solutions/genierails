# GenieRails

Put Genie onboarding on rails — with built-in guardrails. Point GenieRails at your tables, and it generates everything you need to run a governed Genie Space: groups, tag policies, column masks, row filters, ACLs, entitlements, and the Space itself. No Terraform to write.

## What you get

- **Role-based groups** — e.g. `Finance_Analyst`, `Compliance_Officer`, each with tailored data access
- **Tag-based governance** — Unity Catalog tag policies that classify sensitive columns (PII, PCI, PHI)
- **Column masking** — AI-generated SQL UDFs that mask sensitive data (SSN, credit cards, emails) per group
- **Row-level security** — filter rows by region, department, compliance scope, or any business dimension
- **Consumer entitlements** — workspace consume access granted to each group automatically
- **Per-space Genie ACLs** — `CAN_RUN` permissions scoped per space, so each group only accesses the spaces it needs
- **Genie Space as code** — instructions, benchmarks, SQL measures, all version-controlled
- **Dev → prod promotion** — one command to replicate governance to production with catalog remapping

## Getting Started

Check the [Prerequisites](shared/docs/prerequisites.md) first (Python, Terraform, Databricks account setup), then pick your cloud:

| My workspace is on... | Start here |
| --- | --- |
| AWS   | [`aws/README.md`](aws/README.md) |
| Azure | [`azure/README.md`](azure/README.md) |

> **Want to see it in action first?** The [Australian Bank Demo](shared/examples/aus_bank_demo/) provisions a complete environment and walks through the full flow in ~20 minutes — ANZ-specific masking, PCI compliance, AML row filters, and dev-to-prod promotion.

## Repository Layout

```
genierails/
├── aws/            Cloud wrapper for AWS deployments
├── azure/          Cloud wrapper for Azure deployments
└── shared/         All shared code (Terraform modules, scripts, tests, docs)
```

`aws/` and `azure/` are the entry points — always run `make` commands from one of these directories. `shared/` holds all Terraform modules, Python scripts, and docs, and is invoked automatically through the cloud wrapper.

## Documentation

**Getting Started:**
- [Prerequisites](shared/docs/prerequisites.md) — OS, Python, Terraform, network, Databricks account, cloud credentials
- [From UI to Production](shared/docs/from-ui-to-production.md) — import your existing Genie Space, add governance, promote to prod
- [Quickstart](shared/docs/quickstart.md) — create a Genie Space from scratch
- [Playbook](shared/docs/playbook.md) — after first deployment: add spaces, promote, overlays, advanced scenarios

**Reference:**
- [Version Control & Standalone Terraform](shared/docs/version-control.md) — what to commit, version pinning, running Terraform directly
- [Architecture](shared/docs/architecture.md) — layers, artifact ownership, config files, Genie Space lifecycle
- [Country & Region Overlays](shared/docs/country-overlays.md) — region-specific PII governance (ANZ, India, Southeast Asia)
- [Industry Overlays](shared/docs/industry-overlays.md) — industry-specific masking and access patterns (Financial Services, Healthcare, Retail)
- [Central Governance, Self-Service Genie](shared/docs/self-service-genie.md) — central ABAC team + BU teams self-serve Genie spaces
- [Advanced Usage](shared/docs/advanced.md) — IDP-synced groups, ABAC-only mode, masking UDF reuse, legacy migration
- [CI/CD Integration](shared/docs/cicd.md) — validate and deploy from a pipeline
- [Troubleshooting](shared/docs/troubleshooting.md) — imports, provider quirks, brownfield workflows
- [Integration Testing](shared/docs/integration-testing.md) — unit tests, integration scenarios, test data
