# Import a Genie Agent from UI into Code

Import an existing Databricks Genie agent's supported configuration and table footprint into code. Then use the separate [dev-to-prod walkthrough](../examples/dev_to_prod/README.md) to govern and promote it.

<a id="before-you-start"></a>
<details>
<summary><strong>Before you start — Complete prerequisites and get the Agent ID</strong></summary>

- Complete the shared [prerequisites checklist](prerequisites.md).
- Complete steps 1–2 of [Phase 0 — Dev: Set up](../examples/dev_to_prod/README.md#phase-0--dev-set-up), so `envs/dev/` exists and `auth.auto.tfvars` is filled in. This guide is Phase 0's step 3 for an existing agent.
- Find the agent ID in the Genie UI: open the agent, click **Configure**, and copy the **Agent ID** from the **About this agent** panel. (It's also in the agent's URL: `.../genie/rooms/01ef7b3c2a4d5e6f`.)

</details>

---

<a id="step-1--add-the-agent-id-to-dev-configuration"></a>
<details>
<summary><strong>Step 1 — Add the Agent ID to dev configuration</strong></summary>

Add the agent ID to the development environment:

```hcl
# envs/dev/env.auto.tfvars
genie_spaces = [
  {
    genie_space_id = "01ef7b3c2a4d5e6f"
    # Optional: omit to derive fresh from policies; [] means nobody.
    # acl_groups = ["payments_ops"]
  },
]
```

</details>

---

<a id="step-2--import-configuration-and-discover-tables"></a>
<details>
<summary><strong>Step 2 — Import configuration and discover tables</strong></summary>

Run Genie-only generation once to import the supported agent configuration and discover its tables. Supply your existing access-tier groups, most-privileged first:

```bash
make generate ENV=dev MODE=genie \
  GENERATE_ARGS='--groups "payments_ops,regional_analysts,viewers"'
```

`MODE=genie` deliberately skips governance generation at this stage, so native
classification does not need to have finished yet. Phase 1 later runs normal
generation after reviewed `class.*` tags are available.

Generation writes the aggregate table list to the tool-owned
`envs/dev/data_access/discovered_uc_tables.auto.tfvars`. Terraform automatically
unions it with user-authored `uc_tables` for classification, grants, and masking;
`env.auto.tfvars` is never rewritten.

A full generation reflects the current aggregate discovery exactly and reports
added, already-present, and disappeared tables. A per-agent `SPACE=...` run is
merge-only so it cannot erase tables previously discovered for other agents.
If an API fetch fails, the run also preserves the previous aggregate rather than
treating a failed lookup as a legitimate removal.

</details>

---

<a id="step-3--continue-through-dev-to-prod"></a>
<details>
<summary><strong>Step 3 — Continue through dev-to-prod</strong></summary>

Continue at [Phase 1 of the dev-to-prod walkthrough](../examples/dev_to_prod/README.md#phase-1--dev-scan-draft-and-test-rules). The remaining workflow is unchanged: classify, generate, validate coverage, promote, re-derive in production, and expose last.

For an imported agent:

- Omit `acl_groups` on the agent's `genie_spaces[]` entry to derive it fresh from current policies. To override derivation, set the user-owned field there; an explicit `[]` means nobody. Generated drafts are not an authoritative ACL input.
- `make apply` updates the attached agent's configuration and ACLs without creating or deleting it.
- In production, leave `genie_space_id` empty to create a new agent, or set an existing production agent ID to attach to it.

</details>

---

<a id="importing-multiple-agents"></a>
<details>
<summary><strong>Optional — Import multiple agents</strong></summary>

Add each agent to `genie_spaces`, then run generation once. GenieRails combines their tables into one governance footprint while keeping separate configuration and `CAN_RUN` ACLs for each agent.

Before continuing:

1. Confirm that every agent imported. A failed fetch is warning-only and does not stop the remaining agents.
2. Review the persisted aggregate and the added/disappeared summary.
3. After promotion, set any required production warehouse IDs. Discovery is an
   environment fact: development's discovered file is not promoted, and the
   production file is populated by `make generate ENV=prod`.

</details>

---

<a id="details--imported-content"></a>
<details>
<summary><strong>Details — Imported content</strong></summary>

GenieRails imports the agent's title, description, tables, instructions, sample questions, benchmarks, SQL filters, measures, expressions, and join specifications where supported.

This is a supported projection, not a byte-for-byte copy. API metadata, object IDs, and some comments are not preserved. Governance is not imported: groups, tag policies, masks, and row filters are derived from native classification.
</details>

---

<a id="details--agent-lifecycle-and-drift"></a>
<details>
<summary><strong>Details — Agent lifecycle and drift</strong></summary>

- `make destroy` does not delete an attached agent; it deletes only agents created by GenieRails.
- After import, code is the source of truth. Re-run generation to import later UI changes.
</details>

---

<a id="reference"></a>
<details>
<summary><strong>Reference — Review detailed commands and concepts</strong></summary>

See the dev-to-prod walkthrough's [command reference and glossary](../examples/dev_to_prod/REFERENCE.md).

</details>
