# From UI to Production

> **Already built your Genie agent in the Databricks UI?** This is the on-ramp: it imports your existing agent's configuration into code, then governs it with the **[champion flow](../examples/champion_flow/README.md)**. It is *not* a separate governance model — after the import step you follow the champion flow exactly (Unity Catalog decides what's sensitive, GenieRails derives one protection per column, and a coverage gate blocks the release until every sensitive column is covered).

## What this does

1. **Imports** your existing Genie agent's configuration from the Genie API into code (a supported subset — see below) and **auto-discovers the tables** it uses.
2. Hands off to the **champion flow** for governance: native classification → one `gr_treatment` per column → a blocking coverage gate → safe dev→prod promotion → expose last.

The only thing unique to this doc is the **import** in Step 1. Everything after it *is* the champion flow.

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

**Governance is NOT imported.** Groups, tag policies, masks, and row filters are *derived* — from native classification, not copied from the UI. Setting `genie_space_id` does **not** switch sensitivity back to LLM guessing; the imported tables flow into the same native-classification path as the champion flow.

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

## Step 2 — Follow the champion flow

With the tables persisted, run the **[champion flow](../examples/champion_flow/README.md)** from **Phase 1** — it works identically for an imported agent:

```bash
make enable-classification ENV=dev        # turn on native classification; wait for class.* tags
make generate ENV=dev GENERATE_ARGS='--groups "<your IdP group names>"'
make coverage-gate ENV=dev                # blocks if any classified column is unprotected
make validate-generated ENV=dev
make apply ENV=dev                         # business_access_enabled=false; attaches to your existing agent (never recreates it)
```

Two import-specific notes as you review `generated/`:

- **`acl_groups`** (which groups can run each agent) are **derived** — populated from your generated groups and the FGAC policies that cover each agent's tables. Review them; they reference your IdP groups, they aren't invented.
- `make apply` **attaches** to the existing agent (applies governance + per-space ACLs and pushes any config changes back to the API); it does not create or delete it.

Then continue the champion flow through promotion, prod re-derivation, and exposure:

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP="dev_fin=prod_fin"
# prod: fill envs/prod/auth.auto.tfvars, run make enable-classification ENV=prod, wait for prod class.* tags, then:
make derive-assignments ENV=prod   # re-derives tag assignments from prod's live tags; reuses the promoted rules, no LLM
make coverage-gate ENV=prod
make apply-governance ENV=prod     # enforcement only; exposure gate still closed
# open exposure only after the gate is green:
#   envs/prod/env.auto.tfvars -> business_access_enabled = true
make apply ENV=prod                # releases business SELECT + Genie CAN_RUN
make verify-access ENV=prod VERIFY_KEY_COLUMN=<key>
```

For prod, leave `genie_space_id` empty to create a fresh prod agent from the promoted config, or set it to an existing prod agent ID to attach. (Warehouse `CAN_USE` is not managed by GenieRails — grant it yourself.)

## Multi-agent import

Import several agents in one `make generate` by listing them all with `genie_space_id`; each is fetched independently, and all are governed and promotable together.

## Good to know

- **Destroy safety:** `make destroy` never deletes an *attached* agent (`genie_space_id` set) — only agents this tool created (empty `genie_space_id`).
- **Config drift:** after the first import, the code is the source of truth. Changes made later in the UI don't sync back automatically — re-run `make generate` (with `genie_space_id`) to re-import.
- **The full model, every command, and the glossary:** see the **[champion flow README](../examples/champion_flow/README.md)** and its **[REFERENCE](../examples/champion_flow/REFERENCE.md)**.
