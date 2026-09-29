<p align="center">
  <img src="shared/docs/genierails-logo.png" alt="GenieRails" width="500">
</p>

# GenieRails

Put Genie onboarding on rails — with built-in guardrails. Take a Genie agent from dev to production without exposing sensitive data: Unity Catalog's built-in classifier decides *what* is sensitive, GenieRails derives *how* it's protected and applies it as code — groups, column masks, row filters, ACLs, entitlements, and the agent itself — and a **coverage gate blocks the release** until every *classified* sensitive column the agent can reach is covered. No Terraform to write.

## How it works

1. **Unity Catalog decides what's sensitive.** Its built-in [Data Classification](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification) scanner reads your data and labels each sensitive column (`class.*`, e.g. `class.email_address`).
2. **GenieRails decides how it's protected.** From those labels it derives one enforcement treatment per column — column masks, row filters, and access rules — and applies it as Terraform, so you don't write any.
3. **A coverage gate is the safety net.** The release fails ("says NO") if any classified-sensitive column has no protection, so an ungoverned agent can't reach users.
4. **Dev rehearses; prod is the real thing.** Build and test in dev, promote the *rules* to prod, let prod classify its *own* data, prove coverage, and open the agent to users **last**.

→ Walk it end-to-end in the **[Champion Flow](shared/examples/champion_flow/)**.

## What you get

- **Native classification as the source of truth** — Unity Catalog decides what's sensitive; GenieRails doesn't guess by default.
- **One protection per column** — a single treatment derived from each column's label, applied as column masks + row filters.
- **A blocking coverage gate** — the release fails ("says NO") until every classified sensitive column is protected.
- **Access tiers from your IdP** — GenieRails consumes your existing groups by default; it doesn't invent them.
- **The agent and its access, as code** — per-agent Genie `CAN_RUN`, consumer entitlements, and the agent's config, all version-controlled and released only after the coverage gate passes.
- **Safe dev → prod promotion** — promote the rules; re-derive the facts from prod's own classification.

## Getting Started

Check the [Prerequisites](shared/docs/prerequisites.md) first, then pick your cloud — [`aws/README.md`](aws/README.md) or [`azure/README.md`](azure/README.md) — and follow the guide for your situation:

| Your situation | Start here | Time |
|---|---|---|
| **Want the end-to-end champion flow** (recommended) | [**Champion Flow** — native classification → coverage gate → safe dev→prod promotion](shared/examples/champion_flow/) | ~30 min |
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

New here? Use the [Getting Started](#getting-started) routes above. Full reference in [`shared/docs/`](shared/docs/):

- **Set up & operate** — [Prerequisites](shared/docs/prerequisites.md) · [Architecture](shared/docs/architecture.md) · [Version Control & Standalone Terraform](shared/docs/version-control.md) · [CI/CD](shared/docs/cicd.md)
- **Customize** — [Country & Region Overlays](shared/docs/country-overlays.md) · [Industry Overlays](shared/docs/industry-overlays.md) · [Central Governance / Self-Service Genie](shared/docs/self-service-genie.md) · [Advanced Usage](shared/docs/advanced.md)
- **Verify & troubleshoot** — [Effective-Access Verification](shared/docs/effective-access-verification.md) · [Integration Testing](shared/docs/integration-testing.md) · [Troubleshooting](shared/docs/troubleshooting.md)
