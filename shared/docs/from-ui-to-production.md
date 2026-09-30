# From UI to Production

Import an existing Databricks Genie agent into code, then govern and promote it with the [dev-to-prod walkthrough](../examples/dev_to_prod/README.md).

## Before you begin

- Complete the [prerequisites](prerequisites.md), including cloud setup and Unity Catalog Data Classification.
- Sync the access groups GenieRails will use from your identity provider.
- Find the agent ID in its Databricks URL, for example `.../genie/rooms/01ef7b3c2a4d5e6f`.

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

Copy the printed table list into the data-access configuration. This is required because later classification, grants, and masking steps read tables from code rather than rediscovering them from the agent.

```hcl
# envs/dev/data_access/env.auto.tfvars
uc_tables = [
  "dev_fin.finance.customers",
  "dev_fin.finance.transactions",
]
```

## 3. Continue to production

Continue at Phase 1 of the [dev-to-prod walkthrough](../examples/dev_to_prod/README.md). The remaining workflow is unchanged: classify, generate, validate coverage, promote, re-derive in production, and expose last.

For an imported agent:

- `acl_groups` are derived from the IdP groups and policies covering the agent's tables. Review them before applying.
- `make apply` updates the attached agent's configuration and ACLs without creating or deleting it.
- In production, leave `genie_space_id` empty to create a new agent, or set an existing production agent ID to attach to it.

## Importing multiple agents

Add each agent to `genie_spaces`, then run generation once. GenieRails combines their tables into one governance footprint while keeping separate configuration and `CAN_RUN` ACLs for each agent.

Before continuing:

1. Confirm that every agent imported. A failed fetch is warning-only and does not stop the remaining agents.
2. Persist the complete table list in `uc_tables`.
3. After promotion, set any required production warehouse IDs. The promoted configuration omits development agent and warehouse IDs.

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
