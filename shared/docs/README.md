# GenieRails Documentation

New here? Start with the **[Dev-to-Prod Walkthrough](../examples/dev_to_prod/)** — the canonical end-to-end guide (native classification → coverage check → safe dev→prod promotion). The full reference is grouped below.

### Guides
- **[Dev-to-Prod Walkthrough](../examples/dev_to_prod/)** — the canonical end-to-end walkthrough.
- **[Import a Genie Agent from UI into Code](import-genie-agent-from-ui.md)** — import a UI-built Genie agent, then govern it via the walkthrough.
- **[Quickstart](quickstart.md)** — create a Genie agent from scratch.
- **[Playbook](playbook.md)** — after your first deployment: add agents, promote, overlays, advanced scenarios.

### Set up & operate
- **[Prerequisites](prerequisites.md)** — OS, Python, Terraform, network, Databricks account, cloud credentials.
- **[Architecture](architecture.md)** — layers, artifact ownership, config files, Genie agent lifecycle.
- **[Version Control & Standalone Terraform](version-control.md)** — what to commit, version pinning, running Terraform directly.
- **[CI/CD](cicd.md)** — validate and deploy from a pipeline.

### Customize
- **[Country & Region Overlays](country-overlays.md)** — region-specific PII governance (ANZ, India, Southeast Asia).
- **[Industry Overlays](industry-overlays.md)** — industry-specific masking and access patterns.
- **[Central Governance / Self-Service Genie](self-service-genie.md)** — central ABAC team + BU teams self-serve agents.
- **[Advanced Usage](advanced.md)** — IdP-synced groups, ABAC-only mode, masking-UDF reuse, brownfield migration.

### Verify & troubleshoot
- **[Effective-Access Verification](effective-access-verification.md)** — prove masking/row filters take effect by querying as per-tier principals.
- **[Integration Testing](integration-testing.md)** — unit tests, integration scenarios, test data.
- **[Troubleshooting](troubleshooting.md)** — imports, provider quirks, common errors.

---

← Back to the [project overview](../../README.md).
