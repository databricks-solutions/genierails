<p align="center">
  <img src="shared/docs/genierails-logo.png" alt="GenieRails" width="500">
</p>

# GenieRails

Put Genie onboarding on rails — with built-in guardrails. Point GenieRails at your tables, and it generates everything you need to run a governed Genie agent: groups, tag policies, column masks, row filters, ACLs, entitlements, and the agent itself. No Terraform to write.

## What you get

- **Role-based groups** — e.g. `Finance_Analyst`, `Compliance_Officer`, each with tailored data access
- **Tag-based governance** — Unity Catalog tag policies that classify sensitive columns (PII, PCI, PHI)
- **Column masking** — AI-generated SQL UDFs that mask sensitive data (SSN, credit cards, emails) per group
- **Row-level security** — filter rows by region, department, compliance scope, or any business dimension
- **Consumer entitlements** — workspace consume access granted to each group automatically
- **Per-space Genie ACLs** — `CAN_RUN` permissions scoped per space, so each group only accesses the spaces it needs
- **Genie agent as code** — instructions, benchmarks, SQL measures, all version-controlled
- **Dev → prod promotion** — one command to replicate governance to production with catalog remapping

## Getting Started

Check the [Prerequisites](shared/docs/prerequisites.md) first (Python, Terraform, Databricks account setup), then pick your cloud:

| My workspace is on... | Start here |
| --- | --- |
| AWS   | [`aws/README.md`](aws/README.md) |
| Azure | [`azure/README.md`](azure/README.md) |

> **▶ Start here — the champion flow:** [Champion Flow — Native-Classification-Driven Governance, End-to-End](shared/examples/champion_flow/) — the canonical walkthrough. Unity Catalog decides what's sensitive, GenieRails derives one enforcement treatment per column, a coverage gate blocks promotion until every sensitive column is covered, and the agent is exposed only after the prod gate passes.

**Where to start:**

| Your situation | Start here | Time |
|---|---|---|
| **Want the end-to-end champion flow** | [**Champion Flow** — native classification → coverage gate → safe dev→prod promotion](shared/examples/champion_flow/) | ~30 min |
| Have an existing Genie agent in the UI | [From UI to Production](shared/docs/from-ui-to-production.md) | ~45 min |
| Starting from scratch (no Genie agent yet) | [Quickstart](shared/docs/quickstart.md) | ~30 min |
| Need the full reference | [Playbook](shared/docs/playbook.md) | Reference |

## Blocking sensitive-column coverage gate

After generation, run `make coverage-gate ENV=<environment>` from `aws/` or
`azure/`. The offline gate reads `generated/abac.auto.tfvars` and
`generated/masking_functions.sql` and exits non-zero if a classification finding
has no treatment mapping, a classified column has no covering column-mask
policy, or a treatment's masking function is absent. Native classification is
read live only during generation through the existing classification source.

GenieRails never deletes sensitive tags or mask policies to make output deploy.
The Unity Catalog 100-policy-per-catalog quota is a hard error listing the
affected policies. Option-B treatment derivation substantially reduces pressure
on this quota by emitting one policy per treatment and catalog.

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
- [From UI to Production](shared/docs/from-ui-to-production.md) — import your existing Genie agent, add governance, promote to prod
- [Quickstart](shared/docs/quickstart.md) — create a Genie agent from scratch
- [Playbook](shared/docs/playbook.md) — after first deployment: add spaces, promote, overlays, advanced scenarios

**Reference:**
- [Version Control & Standalone Terraform](shared/docs/version-control.md) — what to commit, version pinning, running Terraform directly
- [Architecture](shared/docs/architecture.md) — layers, artifact ownership, config files, Genie agent lifecycle
- [Country & Region Overlays](shared/docs/country-overlays.md) — region-specific PII governance (ANZ, India, Southeast Asia)
- [Industry Overlays](shared/docs/industry-overlays.md) — industry-specific masking and access patterns (Financial Services, Healthcare, Retail)
- [Central Governance, Self-Service Genie](shared/docs/self-service-genie.md) — central ABAC team + BU teams self-serve Genie agents
- [Advanced Usage](shared/docs/advanced.md) — IDP-synced groups, ABAC-only mode, masking UDF reuse, legacy migration
- [CI/CD Integration](shared/docs/cicd.md) — validate and deploy from a pipeline
- [Troubleshooting](shared/docs/troubleshooting.md) — imports, provider quirks, brownfield workflows
- [Integration Testing](shared/docs/integration-testing.md) — unit tests, integration scenarios, test data
- [Effective-Access Verification](shared/docs/effective-access-verification.md) — prove masking/row filters take effect by querying as per-tier test principals (`make verify-access`)
