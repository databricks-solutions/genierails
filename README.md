<p align="center">
  <img src="shared/docs/genierails-logo.png" alt="GenieRails" width="500">
</p>

# GenieRails

Put Genie onboarding on rails — with built-in guardrails. Take a Genie agent from dev to production without exposing sensitive data: Unity Catalog's built-in classifier decides *what* is sensitive, GenieRails derives *how* it's protected and applies it as code — groups, column masks, row filters, ACLs, entitlements, and the agent itself — and **no new business access is granted** until every *classified* sensitive column the agent can reach is covered. No Terraform to write.

**▶ Start here — the [Dev-to-Prod Walkthrough](shared/examples/dev_to_prod/):** the canonical end-to-end guide (native classification → coverage check → safe dev→prod promotion, ~30 min). Point it at the Genie agent and catalog you already have.

## How it works

1. **Unity Catalog decides what's sensitive.** Its built-in [Data Classification](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification) scanner reads your data and tags each sensitive column.
2. **GenieRails decides how it's protected.** From those tags it derives one treatment per column — column masks and row filters — plus access rules mapped to your existing IdP groups, and applies it all as Terraform, so you don't write any.
3. **A coverage check is the safety net.** Any apply that would add or widen business access stops ("says NO") if a classified-sensitive column has no protection, so an ungoverned agent can't reach users.
4. **Dev rehearses; prod is the real thing.** Build and test in dev, promote the *rules* to prod, let prod classify its *own* data, and release: masks go on **before** any user is granted access.

## Documentation

**[Browse all docs →](shared/docs/)** — the full index: walkthrough, quickstart, playbook, architecture, CI/CD, overlays, effective-access verification, and troubleshooting.
