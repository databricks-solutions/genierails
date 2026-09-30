# From UI to Production

Import an existing Databricks Genie agent into code, then govern and promote it with the [dev-to-prod walkthrough](../examples/dev_to_prod/README.md).

## Before you begin

- Complete the [prerequisites](prerequisites.md), including cloud setup and Unity Catalog Data Classification.
- Sync the access groups GenieRails will use from your identity provider.
- Find the agent ID in the Genie UI: open the agent, click **Configure**, and copy the **Agent ID** from the **About this agent** panel. (It's also in the agent's URL: `.../genie/rooms/01ef7b3c2a4d5e6f`.)

## 1. Configure the agent

Add the agent ID to the development environment:

```hcl
# envs/dev/env.auto.tfvars
genie_spaces = [
  { genie_space_id = "01ef7b3c2a4d5e6f" },
]
```

## 2. Import the agent

Run generation once to import the supported agent configuration and discover its tables:

```bash
make generate ENV=dev
```

Generation writes the aggregate table list to the tool-owned
`envs/dev/data_access/discovered_uc_tables.auto.tfvars`. Terraform automatically
unions it with user-authored `uc_tables` for classification, grants, and masking;
`env.auto.tfvars` is never rewritten.

A full generation reflects the current aggregate discovery exactly and reports
added, already-present, and disappeared tables. A per-agent `SPACE=...` run is
merge-only so it cannot erase tables previously discovered for other agents.
If an API fetch fails, the run also preserves the previous aggregate rather than
treating a failed lookup as a legitimate removal.

## 3. Continue to production

Continue at [Phase 1 of the dev-to-prod walkthrough](../examples/dev_to_prod/README.md#phase-1--dev-scan-draft-the-rules-test-them). The remaining workflow is unchanged: classify, generate, validate coverage, promote, re-derive in production, and expose last.

For an imported agent:

- `acl_groups` are derived from the IdP groups and policies covering the agent's tables. Review them before applying.
- `make apply` updates the attached agent's configuration and ACLs without creating or deleting it.
- In production, leave `genie_space_id` empty to create a new agent, or set an existing production agent ID to attach to it.

## Importing multiple agents

Add each agent to `genie_spaces`, then run generation once. GenieRails combines their tables into one governance footprint while keeping separate configuration and `CAN_RUN` ACLs for each agent.

Before continuing:

1. Confirm that every agent imported. A failed fetch is warning-only and does not stop the remaining agents.
2. Review the persisted aggregate and the added/disappeared summary.
3. After promotion, set any required production warehouse IDs. Discovery is an
   environment fact: development's discovered file is not promoted, and the
   production file is populated by `make generate ENV=prod`.

<details>
<summary>What is imported?</summary>

GenieRails imports the agent's title, description, tables, instructions, sample questions, benchmarks, SQL filters, measures, expressions, and join specifications where supported.

This is a supported projection, not a byte-for-byte copy. API metadata, object IDs, and some comments are not preserved. Governance is not imported: groups, tag policies, masks, and row filters are derived from native classification.
</details>

<details>
<summary>Agent lifecycle and drift</summary>

- `make destroy` does not delete an attached agent; it deletes only agents created by GenieRails.
- After import, code is the source of truth. Re-run generation to import later UI changes.
</details>

For detailed commands and concepts, see the walkthrough's [reference](../examples/dev_to_prod/REFERENCE.md).
