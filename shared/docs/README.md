# GenieRails Documentation

New here? Start in the [project overview](../../README.md#get-started): **govern and promote agents** starts with the **[Dev-to-Prod Walkthrough](../examples/dev_to_prod/)**; **govern tables only** starts with [Govern tables only](playbooks/governance.md#govern-tables-only). For how releases check git (reviewed `origin/main` commits, or a local-only repo's committed `main`), see [`make release-plan`](../../REFERENCE.md) and the [deployment machine](deployment-machine.md); for the coverage check that withholds new access, see [Architecture](architecture.md).

### Start
- **[Dev-to-Prod Walkthrough](../examples/dev_to_prod/)** — the champion flow: one agent, end to end.
- **[Import a Genie Agent from UI into Code](import-genie-agent-from-ui.md)** — import a UI-built Genie agent, then govern it via the walkthrough.
- **[Quickstart](quickstart.md)** — create a Genie agent from scratch.

### Playbooks, by persona
- **[Agents](playbooks/agents.md)** — data citizen: add, update, promote and remove an agent.
- **[Governance](playbooks/governance.md)** — governance engineer: masks, classes, tiers, `table_readers`, row filters, `ungovern`, acknowledgements.
- **[One team doing both](playbooks/one-team.md)** — both kinds of change in one PR.

### Set up & operate
- **[Prerequisites](prerequisites.md)** — OS, Python, Terraform, network, Databricks account, cloud credentials.
- **[Pipelines](pipelines.md)** — what to commit, CODEOWNERS, `bootstrap-pipelines`, OIDC, the GitHub Actions and Azure DevOps samples.
- **[Deployment machine](deployment-machine.md)** — releasing prod in v1: persistent checkout, `GENIERAILS_STATE_DIR`, locks, `release-plan` and `release`.
- **[Architecture](architecture.md)** — layers, artifact ownership, config files, Genie agent lifecycle.
- **[Access](access.md)** — `table_readers`, `acl_groups`, `group_map`, the deployer SP.
- **[Scheduled governance](scheduled-governance.md)** — scheduling maintain and the audits yourself; the legacy-only (deprecated) scheduled job.

### Customize
- **[Advanced Usage](advanced.md)** — IdP-synced groups, independent environments, masking-UDF reuse.
- **[Mask library reference](../REFERENCE.md)** — every treatment and `tag_treatments`.

### Verify & troubleshoot
- **[Effective-Access Verification](effective-access-verification.md)** — prove masking/row filters take effect by querying as per-tier principals.
- **[Integration Testing](integration-testing.md)** — unit tests, integration scenarios, test data.
- **[Troubleshooting](troubleshooting.md)** — imports, provider quirks, common errors.

### Legacy mode
- **[Legacy mode](legacy-mode.md)** — the older AI-drafted mode, as a migration source only; [migrating](legacy-migration.md). Legacy-only: [country](country-overlays.md) and [industry](industry-overlays.md) overlays.

---

← Back to the [project overview](../../README.md).
