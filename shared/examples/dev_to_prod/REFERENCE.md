# GenieRails Dev-to-Prod Walkthrough — reference

Lookup companion to the **[Dev-to-Prod Walkthrough](README.md)**: the full command reference, how the enforcement works under the hood, and a glossary of every term. You don't need to read this top-to-bottom — jump in when the walkthrough links you here.

---

## How it works (under the hood)

**The exposure gate is mechanical.** `business_access_enabled` holds back exactly the two things that let a user reach data through the agent; everything else applies regardless:

| Control | When it applies |
|---|---|
| Table `SELECT` grant | **held until `business_access_enabled=true`** |
| Genie run permission (`CAN_RUN`) | **held until `business_access_enabled=true`** |
| Workspace assignment + consume entitlement | applied on **every** apply (harmless without `SELECT`/`CAN_RUN`) |
| Warehouse `CAN_USE` | **not managed by GenieRails** — you grant it (Phase 5) |

So "expose last" isn't a policy you hope holds — there is simply no `SELECT` and no `CAN_RUN` until the gate is opened.

**Three layers of governance, and the Terraform layers that build them:**

| Governance layer (what it controls) | Built by Terraform layer | Contains |
|---|---|---|
| **Who can reach a table** | `account` + `data_access` | groups + the `USE CATALOG → USE SCHEMA → SELECT` grant chain |
| **What they see through it** | `account` + `data_access` | governed tag policies + column masks + row filters (attribute-based access control) |
| **Whether they can open/run the agent** | `workspace` | the Genie agent, its run permissions, workspace assignment + entitlement |

(So the `data_access` Terraform layer covers both *access* and *masking*; the `workspace` layer is the agent itself.)

**One mask per column.** The single enforcement key is **`gr_treatment`** — GenieRails derives exactly **one** value per column from its `class.*` tags (strictest tag wins; a free-text column with multiple tags escalates to full redaction), so Unity Catalog's "only one mask may apply per column" rule is never violated.

**Why prod keeps the classifier's tags.** The Terraform resource that records tag assignments carries `ignore_changes = all` — a standard Terraform *lifecycle* setting meaning "once these exist, don't change or delete them." That lets the **classifier own the `class.*` tags** in prod: when a scan writes a tag, Terraform leaves it alone instead of reverting it. The classifier owns the tags; GenieRails owns the rules.

---

## Command reference

| Command | Phase | What it does |
|---|---|---|
| `make setup` / `make init-env ENV=<e>` | 0 | Create local env dirs + default config files (no Databricks calls) |
| `make enable-classification ENV=<e>` | 1/3 | Turn on UC Data Classification (scanning) — as-code alternative to the Databricks UI (recommended); auto-tagging is opt-in |
| `make generate ENV=<e>` | 1 | (dev) Draft masks + access rules from the model and derive one `gr_treatment`/column from native `class.*` (fail-closed); groups come from `access_tier_groups` in `env.auto.tfvars` (or `GENERATE_ARGS='--groups "..."'`, saved there on first use). Re-runs keep already-reviewed rules and add only new ones; `GENERATE_ARGS='--allow-rule-changes'` accepts the model's changes |
| `make derive-assignments ENV=<e>` | 4 | (prod) Re-derive **only** `tag_assignments` from live `class.*`, reusing the promoted rules unchanged — no model call (fail-closed; requires a prior `promote`) |
| `make coverage-gate ENV=<e>` | 1/4 | **Block** if any tagged-sensitive column has no mask (the "says NO" check) |
| `make validate-generated ENV=<e>` | 1/4 | Static validation incl. the one-mask-per-column guard |
| `make apply ENV=<e>` | 1/5 | Full stack (account → data_access → workspace; auto-promotes same-env first); creates the Genie agent; releases gated access when `business_access_enabled=true` |
| `make apply-governance ENV=<e>` | 4 | Enforcement only (account + data_access); no Genie agent |
| `make rehearse ENV=dev VERIFY_KEY_COLUMN=<pk>` | 1 | (dev) coverage-gate → validate-generated → apply → verify-access, stopping at the first failure |
| `make certify ENV=prod` | 4 | (prod) derive-assignments → coverage-gate → validate-generated → apply-governance → audit-rulebook; records the certification `make release` requires |
| `make release ENV=prod VERIFY_KEY_COLUMN=<pk>` | 5 | (prod) Refuses unless certification is current; creates the Genie agent, releases gated access, saves `business_access_enabled = true`, runs `verify-access` |
| `make maintain ENV=prod` | 6 | (prod, scheduled) audit-schema → derive-assignments → coverage-gate → validate-generated → apply-governance → audit-rulebook; renews certification, never changes access or Genie |
| `make promote SOURCE_ENV DEST_ENV DEST_CATALOG_MAP` | 2 | Promote **rules only** (leaves tag assignments behind); creates + writes prod `env.auto.tfvars` |
| `make verify-access ENV=<e> VERIFY_KEY_COLUMN=<pk>` | 1/5 | Prove masking by querying as per-tier test principals (**needs the gate open**) |
| `make audit-rulebook ENV=<e>` | 4/6 | Drift check — tags with no covering rule |
| `make audit-schema ENV=<e>` | 6 | Untagged-column audit (also the first step of `make maintain`) |
| `make generate-delta ENV=<e>` | — | [Legacy] model-based incremental tag assignments; the champion flow uses `make maintain` instead |
| `make evidence ENV=<e>` | 5 | Compliance evidence record (`GENIERAILS_EVIDENCE_INTEGRATION=1` + `WAREHOUSE_ID`) |

Successful validation reports are compact by default. Add `VERBOSE=1` to a
`make` command to restore the full PASS reports and informational detail;
warnings and failures are always printed in full.

Key config & code: [`treatment_config.json`](../../treatment_config.json) (the `gr_treatment` precedence rules — shared across envs), [`sensitivity_source.py`](../../sensitivity_source.py) (native `class.*` source), [`treatment_derivation.py`](../../treatment_derivation.py) (one treatment/column), [`verify_effective_access.py`](../../verify_effective_access.py) (masked-vs-raw), [`scripts/audit_schema_drift.py`](../../scripts/audit_schema_drift.py) (drift).

---

## Glossary

- **access tier** — a group of users who should see data at the same level (e.g. full / masked / least). You map one IdP group to each tier.
- **ABAC (attribute-based access control)** — masks/filters that apply based on a column's *tag*, not its name — so a rule covers any column carrying that tag.
- **`CAN_RUN` / `CAN_USE`** — Databricks permissions: `CAN_RUN` lets a group open and run a Genie agent (released by the exposure gate); `CAN_USE` lets a group run a SQL warehouse (you grant it yourself).
- **`class.*` tag** — a tag Unity Catalog's classifier writes on a column it finds sensitive (e.g. `class.email_address`).
- **coverage gate** — `make coverage-gate`; the blocking check that fails if any tagged-sensitive column has no covering mask/policy. The "tool says NO" step.
- **drift** — a gap between what's tagged and what's protected; `audit-rulebook` reports it.
- **entitlement / workspace assignment** — what lets a group *into* a workspace at all (applied every apply; harmless without a data grant).
- **evidence** — the compliance record `make evidence` produces (what was scanned, tagged, protected, and approved).
- **exposure gate** — `business_access_enabled`; releases the `SELECT` grant + Genie run permission only when `true`.
- **facts vs rules** — *facts* = which columns got tagged in *this* workspace (from the scan); *rules* = the mapping + policies (portable, promoted).
- **fail-closed** — if native classification can't be read, `generate` aborts rather than guessing.
- **FGAC (fine-grained access control)** — Unity Catalog column masks + row filters.
- **footprint** — the exact tables the agent can reach (your `uc_tables` / Genie agent tables).
- **Genie agent** — the Databricks Genie experience users query; "the agent." *(Formerly "Genie space"; the config key and API id are still `genie_spaces` / `genie_space_id`.)*
- **`gr_treatment`** — the one GenieRails-owned tag whose value picks a column's mask.
- **grant chain** — `USE CATALOG → USE SCHEMA → SELECT`, the layered grants needed to read a table.
- **IdP (identity provider)** — Entra ID / Okta; **AIM / SCIM** are how it syncs groups into Databricks. GenieRails consumes those groups.
- **masking** — transforming a sensitive value for unauthorized tiers (e.g. card → `****-****-****-4464`) while authorized tiers see the raw value.
- **principal** — an identity a query runs as (a user, group, or service principal); `verify-access` uses temporary test principals per tier.
- **rulebook / rules** — the mapping `class.* → gr_treatment → mask` plus the access/row-filter policies (the portable, promoted part).
- **row filter** — a rule that limits *which rows* a tier can see (business logic; not every flow uses one).
