<p align="center">
  <img src="shared/docs/genierails-logo.png" alt="GenieRails" width="500">
</p>

# GenieRails

GenieRails lets you give business users a Genie agent over your data without exposing sensitive columns.

Databricks [Data Classification](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification) finds the sensitive columns, like emails and card numbers. GenieRails turns what it finds into masks and access rules, tests them in dev, then releases the same tested rules to prod. You don't write any Terraform.

## Why use it

- **No access before protection.** Nobody gets new access to an agent's data until every sensitive column it can reach has a mask. Only the groups you choose see raw values.
- **You prove it in dev first.** Dev queries the data as each test group and checks that the masks hide it.
- **Every change is reviewed and recorded.** Prod only takes changes committed to git, you approve each step, and `make evidence` saves a record for audit.
- **A few `make` commands replace hand-written Terraform and SQL.**
- **Works on AWS and Azure.**

## How it works

1. **Classify.** Databricks scans your tables and tags each sensitive column.
2. **Generate rules.** GenieRails reads those tags and writes a mask for each sensitive column. It also writes who can read each table and run each agent, using your existing groups.
3. **Rehearse in dev.** It applies the rules to dev and queries as each group to prove the masks work.
4. **Release to prod.** You copy the tested rules to prod and prod scans its own data. Then `make release ENV=prod` applies the rules one approved step at a time. Masks go on before anyone gets access.

## Get started

Start with the **[Dev-to-Prod walkthrough](shared/examples/dev_to_prod/)**. It takes about 30 minutes and ships one Genie agent from dev to prod. The main commands, run from `aws/` or `azure/`:

```bash
make setup ENV=dev
make generate ENV=dev
make rehearse ENV=dev
make setup ENV=prod
make promote-to ENV=prod
make release ENV=prod
```

Stuck? Run `make next ENV=<env>`. It prints the next command to run.

## Other ways to use it

- **Govern tables only.** Your data team owns the masks and table access for everyone, Genie included: [govern tables only](shared/docs/playbooks/governance.md#govern-tables-only).
- **One team doing both.** Change agents and table rules in one pull request: [one team](shared/docs/playbooks/one-team.md).
- **Pipelines and a deployment machine.** Run checks in CI and release prod from one machine: [pipelines](shared/docs/pipelines.md) and the [deployment machine](shared/docs/deployment-machine.md).

## Docs

- **[All docs](shared/docs/)**: the walkthrough, playbooks, architecture and troubleshooting.
- **[Command and settings reference](REFERENCE.md)**: every `make` command and setting.
