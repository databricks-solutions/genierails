<p align="center">
  <img src="shared/docs/genierails-logo.png" alt="GenieRails" width="500">
</p>

# GenieRails

Put Genie onboarding on rails — with built-in guardrails. Take a Genie agent from dev to production without exposing sensitive data: Unity Catalog's built-in classifier decides *what* is sensitive, GenieRails derives *how* it's protected and applies it as code — groups, column masks, row filters, ACLs, entitlements, and the agent itself — and **no new business access is granted** until every *classified* sensitive column the agent can reach is covered.

## Pick your path

| You want to | Start with |
|---|---|
| **Govern and promote agents** (the champion flow): ship a Genie agent from dev to prod with its sensitive columns masked | the [Dev-to-Prod Walkthrough](shared/examples/dev_to_prod/) (~30 min), then the [agents](shared/docs/playbooks/agents.md) playbook |
| **Govern tables only**: for a central data governance team that defines the table rules (masks on classified columns, row filters, who reads each table), reviews them as code and promotes them from dev to prod, for every consumer of those tables, Genie agents included. Also for platform teams securing catalogs before Genie rolls out | [Govern tables only](shared/docs/playbooks/governance.md#govern-tables-only), then the [governance](shared/docs/playbooks/governance.md) playbook |

After the first deployment: [one team doing both](shared/docs/playbooks/one-team.md), [pipelines](shared/docs/pipelines.md) and the [deployment machine](shared/docs/deployment-machine.md).

Lost? Run `make next ENV=<env>` (from `aws/` or `azure/`): it prints the one next command and why; `make status ENV=<env>` shows the whole picture. Both are read-only.

Git provenance is managed by GenieRails for you: normal repos release only reviewed commits on `origin/main`; a repo with no remotes at all can still deploy from its clean committed local `main`. If any remote exists, the `origin/main` gate applies.

## How it works

1. **Unity Catalog decides what's sensitive.** Its built-in [Data Classification](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification) scanner reads your data and tags each sensitive column.
2. **GenieRails decides how it's protected.** From those tags it derives one treatment per column — column masks and row filters — plus access rules mapped to your existing IdP groups, and applies it all as Terraform, so you don't write any.
3. **A coverage check is the safety net.** Any `make` apply that would add or widen business `SELECT` or `CAN_RUN` withholds that new access (or fails closed) unless a recent coverage check passed, meaning every classified-sensitive column has protection. Protection still applies. So no one gains new access to unprotected sensitive data.
4. **Dev rehearses; prod is the real thing.** Build and test in dev, promote the *rules* to prod, let prod classify its *own* data, and release a rehearsed commit (`make release ENV=prod`: a y/N per manifest, round by round): masks go on **before** any user is granted access.

## Documentation

**[Browse all docs →](shared/docs/)** — the full index: walkthrough, playbooks, pipelines, architecture, effective-access verification, and troubleshooting.

**[Command and settings reference →](REFERENCE.md)** — every `make` target and every `env.auto.tfvars` / `auth.auto.tfvars` setting.

Bootstrap note: `make bootstrap-sp` prints a new OAuth secret by default; `BOOTSTRAP_AUTH_ENV=<env>` writes a newly minted secret directly to that env's private `auth.auto.tfvars` instead. See the command reference for the guardrails. It reuses an SP only by exact display name (`SP_NAME`, default `genierails-deployer`): pass `SP_NAME=<its name>` for an existing SP, or it creates a second one.
