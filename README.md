<p align="center">
  <img src="shared/docs/genierails-logo.png" alt="GenieRails" width="500">
</p>

# GenieRails

Put Genie onboarding on rails — with built-in guardrails. Take a Genie agent from dev to production without exposing sensitive data: Unity Catalog's built-in classifier decides *what* is sensitive, GenieRails derives *how* it's protected and applies it as code — groups, column masks, row filters, ACLs, entitlements, and the agent itself — and a **coverage gate blocks the release** until every *classified* sensitive column the agent can reach is covered. No Terraform to write.

## How it works

1. **Unity Catalog decides what's sensitive.** Its built-in [Data Classification](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification) scanner reads your data and labels each sensitive column (`class.*`, e.g. `class.email_address`).
2. **GenieRails decides how it's protected.** From those labels it derives one treatment per column — column masks and row filters — plus access rules mapped to your existing IdP groups (it *consumes* your groups, never invents them), and applies it all as Terraform, so you don't write any.
3. **A coverage gate is the safety net.** The release fails ("says NO") if any classified-sensitive column has no protection, so an ungoverned agent can't reach users.
4. **Dev rehearses; prod is the real thing.** Build and test in dev, promote the *rules* to prod, let prod classify its *own* data, prove coverage, and open the agent to users **last**.

→ Walk it end-to-end in the **[Champion Flow](shared/examples/champion_flow/)**.

## Getting Started

Check the [Prerequisites](shared/docs/prerequisites.md) first, then pick your cloud — [`aws/`](aws/README.md) or [`azure/`](azure/README.md), where you run all `make` commands (`shared/` is invoked automatically).

**→ Follow the [Champion Flow](shared/examples/champion_flow/)** — the canonical end-to-end walkthrough (native classification → coverage gate → safe dev→prod promotion), ~30 min. It ships an optional sample environment, so you can run the whole thing even without your own tables or agent.

Already have a Genie agent built in the Databricks UI? [Import it first](shared/docs/from-ui-to-production.md), then follow the same flow.

## Documentation

Full reference in [`shared/docs/`](shared/docs/):

- **Guides** — [Champion Flow](shared/examples/champion_flow/) · [From UI to Production](shared/docs/from-ui-to-production.md) · [Quickstart](shared/docs/quickstart.md) · [Playbook](shared/docs/playbook.md)
- **Set up & operate** — [Prerequisites](shared/docs/prerequisites.md) · [Architecture](shared/docs/architecture.md) · [Version Control & Standalone Terraform](shared/docs/version-control.md) · [CI/CD](shared/docs/cicd.md)
- **Customize** — [Country & Region Overlays](shared/docs/country-overlays.md) · [Industry Overlays](shared/docs/industry-overlays.md) · [Central Governance / Self-Service Genie](shared/docs/self-service-genie.md) · [Advanced Usage](shared/docs/advanced.md)
- **Verify & troubleshoot** — [Effective-Access Verification](shared/docs/effective-access-verification.md) · [Integration Testing](shared/docs/integration-testing.md) · [Troubleshooting](shared/docs/troubleshooting.md)
