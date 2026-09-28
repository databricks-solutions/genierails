<p align="center">
  <img src="shared/docs/genierails-logo.png" alt="GenieRails" width="500">
</p>

# GenieRails

Put Genie onboarding on rails — with built-in guardrails. Take a Genie agent from dev to production without exposing sensitive data: Unity Catalog's built-in classifier decides *what* is sensitive, GenieRails derives *how* it's protected and applies it as code — groups, column masks, row filters, ACLs, entitlements, and the agent itself — and a **coverage gate blocks the release** until every sensitive column the agent can reach is covered. No Terraform to write.

## What you get

- **Native classification as the source of truth** — Unity Catalog's Data Classification decides what's sensitive (`class.*` labels); GenieRails never guesses.
- **One protection per column** — a single enforcement treatment (`gr_treatment`) is derived deterministically from each column's label, then applied as UC tag-condition column masks + row filters (SSN, credit cards, emails, region/department/compliance scope, …).
- **A blocking coverage gate** — the release fails ("says NO") until every classified sensitive column is protected.
- **Access tiers from your IdP** — mapped to your existing IdP-synced groups; GenieRails *consumes* them, it never invents them.
- **Consumer entitlements** — workspace consume access granted to each group.
- **Per-agent Genie ACLs** — `CAN_RUN` scoped per agent, released only when the coverage gate is green (exposed last).
- **Genie agent as code** — instructions, benchmarks, SQL measures, all version-controlled.
- **Safe dev → prod promotion** — promote the *rules*, re-derive the *facts* from prod's own classification (no LLM re-generation), with one-command catalog remapping.

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
| Have an existing Genie agent in the UI | [From UI to Production](shared/docs/from-ui-to-production.md) — import it, then govern it via the champion flow | ~30 min |
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
Two platform limits can surface as hard errors: the **100 FGAC/ABAC policies
per catalog** limit (Option-B treatment derivation keeps you well under it by
emitting one policy per treatment per catalog), and the separate
**account-level governed-tag-policy cap** (each governed tag is an account tag
policy, so large shared accounts can hit it). Both are reported clearly, never
worked around by dropping protection.

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
- [**Champion Flow**](shared/examples/champion_flow/) — the canonical end-to-end walkthrough: native classification → coverage gate → safe dev→prod promotion
- [From UI to Production](shared/docs/from-ui-to-production.md) — import an existing UI-built agent, then govern it via the champion flow
- [Quickstart](shared/docs/quickstart.md) — create a Genie agent from scratch
- [Playbook](shared/docs/playbook.md) — after first deployment: add agents, promote, overlays, advanced scenarios

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
