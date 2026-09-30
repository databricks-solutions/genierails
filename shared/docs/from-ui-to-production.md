# From UI to Production

> **Already built your Genie agent in the Databricks UI?** This is the on-ramp: it imports your existing agent's configuration into code, then governs it with the **[dev-to-prod walkthrough](../examples/dev_to_prod/README.md)**. It is *not* a separate governance model — after the import step you follow the dev-to-prod walkthrough exactly (Unity Catalog decides what's sensitive, GenieRails derives one protection per column, and a coverage gate blocks the release until every *classified* sensitive column is covered).

## What this does

1. **Imports** your existing Genie agent's configuration from the Genie API into code (a supported subset — see below) and **auto-discovers the tables** it uses.
2. Hands off to the **dev-to-prod walkthrough** for governance: native classification → one `gr_treatment` per column → a blocking coverage gate → safe dev→prod promotion → expose last.

The only thing unique to this doc is the **import** in Step 1. Everything after it *is* the dev-to-prod walkthrough.

## What gets imported (a supported subset — not verbatim)

When a `genie_spaces` entry has `genie_space_id` set, `make generate` queries the Genie API and imports a **supported subset** of the agent's serialized configuration:

| Field | Imported |
| ----- | -------- |
| Instructions (the first text instruction) | ✓ |
| Sample questions | ✓ |
| Benchmarks (question + first SQL answer) | ✓ |
| SQL filters, measures, expressions | ✓ |
| Join specs | ✓ |
| Table list | ✓ — auto-discovered from the agent |
| Title / description | ✓ |

> **It's a projection, not a byte-for-byte copy.** Object IDs and some UI/API metadata are not preserved, and filter/measure *comments* aren't imported. If the agent was created moments ago, the API's `serialized_space` can take 1–3 minutes to populate (the tool retries for ~4 minutes).

**Governance is NOT imported.** Groups, tag policies, masks, and row filters are *derived* — from native classification, not copied from the UI. Setting `genie_space_id` does **not** switch sensitivity back to LLM guessing; the imported tables flow into the same native-classification path as the dev-to-prod walkthrough.

## Prerequisites

- **Cloud setup done** — complete Steps 1–2 in your cloud README ([AWS](../../aws/README.md) or [Azure](../../azure/README.md)) so credentials are configured.
- **Groups synced from your IdP** (AIM/SCIM). `make setup` scaffolds `manage_groups = false` — GenieRails *consumes* your IdP groups; it never invents them. You supply the group names at generate time.
- **UC Data Classification** available in the workspace.

## Step 1 — Point at your agent and persist its tables

Find the agent ID in the Databricks UI URL (e.g. `.../genie/rooms/01ef7b3c2a4d5e6f`):

```hcl
# envs/dev/env.auto.tfvars
genie_spaces = [
  { genie_space_id = "01ef7b3c2a4d5e6f" },   # the only required field
]
```

**Important — discover and persist the tables first.** An ID-only import can't jump straight to `enable-classification`: that command reads `uc_tables` from your config and does **not** call the Genie API. And table discovery only populates the tables *in memory* during a generate run — it doesn't write them back. So run generate once to discover them, then copy them in:

```bash
make generate ENV=dev        # discovers + prints the agent's tables (and imports its config)
```

```hcl
# envs/dev/data_access/env.auto.tfvars
uc_tables = [
  "dev_fin.finance.customers",
  "dev_fin.finance.transactions",
  # ... (exactly as make generate printed)
]
```

## Step 2 — Hand back to the walkthrough

That's the whole import. You now have what the walkthrough's Phase 0 asks for — `genie_spaces` (your agent) and `uc_tables` (its tables). **Return to the [dev-to-prod walkthrough](../examples/dev_to_prod/README.md) at Phase 1 and follow it to the end** (classify → generate → coverage gate → promote → prod re-derive → expose last). It runs identically for an imported agent — don't repeat the commands here.

Just three things are import-specific as you go:

- **`acl_groups`** (which groups can run each agent) are **derived** — from your generated groups and the FGAC policies covering each agent's tables. Review them; they reference your IdP groups, they aren't invented.
- **`make apply` attaches** to your existing agent — it applies governance + per-space ACLs and pushes config changes back to the API, but never creates or deletes the agent.
- **In prod**, leave `genie_space_id` empty to create a fresh prod agent from the promoted config, or set it to an existing prod agent ID to attach.

## Multi-agent import

List several agents in one `make generate`, each with its own `genie_space_id` — each is fetched independently, and all are governed and promotable together: their tables merge into one classification + coverage footprint, and each agent keeps its own per-space `CAN_RUN` ACLs.

A few multi-agent specifics:

- **Verify they all imported.** A failed or slow fetch for one agent is warning-only — generation continues with the rest — so confirm every agent's tables appear before you proceed.
- **Persist the union of tables.** As in Step 1, copy the discovered `uc_tables` into config for *all* agents — grants and masks deploy from config, not from the in-memory discovery.
- **Promotion resets per-agent warehouses.** `make promote` creates fresh prod agents and clears each space's `sql_warehouse_id` (and its dev `genie_space_id`); set prod warehouse ids as needed.

## Good to know

- **Destroy safety:** `make destroy` never deletes an *attached* agent (`genie_space_id` set) — only agents this tool created (empty `genie_space_id`).
- **Config drift:** after the first import, the code is the source of truth. Changes made later in the UI don't sync back automatically — re-run `make generate` (with `genie_space_id`) to re-import.
- **The full model, every command, and the glossary:** see the **[dev-to-prod walkthrough README](../examples/dev_to_prod/README.md)** and its **[REFERENCE](../examples/dev_to_prod/REFERENCE.md)**.
