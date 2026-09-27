# GenieRails Champion Flow — Native-Classification-Driven Governance, End-to-End

The canonical GenieRails flow: take a **curated Genie agent in dev** and ship it to **production** without ever exposing sensitive data — the platform decides *what* is sensitive (native UC Data Classification), GenieRails codifies *how* it's enforced, and a coverage gate blocks promotion until every sensitive column the agent can reach is provably covered.

> Validated **live end-to-end on real AWS Databricks workspaces** (a dev and a prod workspace). It supersedes the older LLM-overlay demo (`aus_bank_demo`) as the reference flow: sensitivity now comes from **native UC Data Classification**, not from an LLM reading DDL.

---

## The mental model (read this first)

> **Prod decides what's sensitive and is the final gate. Dev is the rehearsal.**
> You promote the **rules**; you re-derive the **facts**. Expose the agent **last**, only after a passing prod coverage check. Fail closed, never fail open.

| | What it is | Where it lives | Promoted dev→prod? |
|---|---|---|---|
| **Rules** | `class.* → gr_treatment → masking function`, group→tier, row filters | version-controlled config | ✅ promoted |
| **Facts** | which columns the scanner tagged in *this* workspace | the workspace, via the scan | ❌ re-derived per env |
| **Identity** | groups + membership | your IdP (AIM/SCIM) | ❌ consumed, never minted |

Governance is **three layers, not one** — GenieRails generates all three as code:
- **Layer 1 — Data access:** the grant chain `USE CATALOG → USE SCHEMA → SELECT` decides *who can reach a table*.
- **Layer 2 — Masking/ABAC:** column masks + row filters decide *what they see through it*.
- **Layer 3 — Agent access (workspace + Genie):** workspace assignment (`USER`) + the consumer **entitlement** (`workspace_consume`), warehouse **`CAN_USE`**, and per-space Genie **`CAN_RUN`** ACLs decide *whether a group can reach the workspace, run the warehouse, and open the Genie space at all*. These are released by the same exposure gate (Phase 5).

The single enforcement key is **`gr_treatment`** — GenieRails derives exactly **one** value per column from the column's sensitivity findings, so Unity Catalog's "one mask per column" rule is never violated.

---

## The example scenario

An APJ finance Genie agent over three tables in one schema:

```
<catalog>.genierails_e2e.customers   -- name, email, phone, date_of_birth, ssn, region_code
<catalog>.genierails_e2e.payments    -- credit_card_number, cvv, amount
<catalog>.genierails_e2e.notes       -- free_text (customer notes: may embed email + name + phone)
```

Live-validated catalogs (yours will differ):
- **Dev:** `serverless_stable_pyecip_catalog.genierails_e2e`
- **Prod:** `finclear_sdp_demo_catalog.genierails_e2e`

The `notes.free_text` column is the interesting one — the scanner tags it `class.email_address` (and `class.name`), which map to partial masks, but a **free-text escalation rule** overrides that to a full `redact` (you don't want a "partial email" mask leaking an embedded phone number). That's the champion-flow crux: one column, many findings, exactly one — correctly strict — treatment.

---

## Prerequisites

1. **UC Data Classification enabled** on the catalog. GenieRails wires this via the `databricks_data_classification_catalog_config` resource (Databricks provider **`~> 1.111.0`**, i.e. ≥1.111.0 <1.112.0) when `enable_classification = true`, and — critically — also emits `auto_tag_configs = AUTO_TAGGING_ENABLED` so the scan actually **writes** `class.*` tags (without it, classification is "on" but nothing gets tagged). Auto-tagging is enabled for these `class.*` types (which cover this example's PII): `card_security_code, credit_card, date_of_birth, email_address, name, phone_number, us_ssn`. To govern additional types (e.g. `iban_code`, `us_bank_number`), extend `classification_auto_tags` in `shared/modules/data_access/main.tf`.
2. **The deploying principal needs `APPLY TAG` on the catalog and `ASSIGN` on the `class.*` tags** — enabling auto-tagging requires these, or `apply` fails.
3. **Groups synced from your IdP via AIM** (GA for Entra ID across clouds; Okta on AWS/GCP; SCIM where AIM isn't available). GenieRails **consumes** these groups by name — it never mints them. *(APJ/Azure note: AIM needs Premium tier + single tenant; cross-tenant estates stay on SCIM.)* These groups must already be **assigned to the workspace** before the data_access layer runs; `manage_groups = false` (consume, not mint) is the module default. Pass their names via `--groups` — never reserved names like `admins`/`users`.
4. **Per-tier test principals** (one service principal per access tier) so effective access can be verified by querying *as* each tier.
5. **Account-admin auth for the deploy steps.** `make generate` / `coverage-gate` / `validate-generated` run with just the workspace OAuth profile, but `make apply` / `apply-governance` (account + data_access layers: groups, tag policies, tag assignments) require an **Account-Admin OAuth profile or token for the _same_ Databricks account as the workspace** — a workspace-only keyring profile fails account-level operations (`Unable to load OAuth Config` / `Workspace not in account`). Existing classification config is not auto-imported; the first `apply` creates it.
6. Python 3, Terraform — see [Prerequisites](../../docs/prerequisites.md).

> All `make` commands below run from the cloud root: **`cd aws`** (or **`cd azure`**), where the `Makefile` and `envs/` live.

---

## Phase 0 — The contract: `gr_treatment` mapping (author once)

Confirm the version-controlled mapping in [`shared/treatment_config.json`](../../treatment_config.json). It is **ordered strictest-first** and maps each sensitivity finding to exactly one enforcement treatment + masking function (abridged):

```jsonc
{
  "tag_key": "gr_treatment",
  "description": "GenieRails-owned enforcement treatment derived from sensitivity findings. Entries are ordered strictest-first.",
  "treatments": [
    {"value": "redact",        "masking_function": "mask_redact",             "sources": [["pci_level","redacted_cvv"], ["pii_level","redacted_mixed"], ["phi_level","redacted"] /* ...+ redacted_card_full, redacted_address, Restricted_PHI, redacted_notes */]},
    {"value": "ssn_last4",     "masking_function": "mask_ssn",                "sources": [["pii_level","masked_ssn"]]},
    {"value": "card_last4",    "masking_function": "mask_credit_card_last4",  "sources": [["pci_level","masked_card_last4"]]},
    {"value": "account_last4", "masking_function": "mask_account_number",     "sources": [["pii_level","masked_account"]]},
    {"value": "email_partial", "masking_function": "mask_email",              "sources": [["pii_level","masked_email"]]},
    {"value": "phone_partial", "masking_function": "mask_phone",              "sources": [["pii_level","masked_phone"]]},
    {"value": "name_partial",  "masking_function": "mask_full_name",          "sources": [["pii_level","masked_name"]]},
    {"value": "date_year",     "masking_function": "mask_date_to_year",       "sources": [["pii_level","masked_dob"]]}
    // ... regional treatments (tfn/medicare/bsb/aadhaar), round_amount, generic_partial — see the real file
  ]
}
```

**Why this is the load-bearing artifact:**
- The classifier owns the `class.*` **facts** (what's sensitive). GenieRails owns `gr_treatment` (how it's enforced). Two keys, two writers, no collision.
- A column with multiple findings resolves to a **single** `gr_treatment` — precedence + free-text escalation (see the `free_text` crux above). One mask resolves; UC never errors.
- **This mapping is the promotable rule.** The tagged columns are not.

You confirm/extend this file — you don't author it from scratch.

---

## Phase 1 — Dev: rehearse the enforcement

Dev is **not** where you discover what's sensitive (prod is). Dev is where you prove the masks fire, the agent still answers, and you produce a reviewable artifact — safely, off live PII.

### 1a. Configure the env

`envs/dev/env.auto.tfvars` (see [`env.auto.tfvars.example`](env.auto.tfvars.example)):

```hcl
sql_warehouse_id        = "8f5f5348bf5b6bc7"
uc_tables = [
  "serverless_stable_pyecip_catalog.genierails_e2e.customers",
  "serverless_stable_pyecip_catalog.genierails_e2e.payments",
  "serverless_stable_pyecip_catalog.genierails_e2e.notes",
]
enable_classification   = true      # native UC Data Classification + auto-tagging
business_access_enabled = false     # exposure gate: hold the business SELECT until the gate is green
genie_spaces            = [ { genie_space_id = "" } ]   # paste your curated dev Space id, or [] for a table-only footprint
```

> If dev data is sparse, **seed representative synthetic PII** — the scanner only tags *format-matchable* values (fake `example.com` emails and `000-` SSNs are ignored). After first enabling classification, tags are not backfilled instantly: they land on the **next scan (up to ~24h)**. `make generate` is fail-closed, so running it before tags land will correctly abort with "0 class.* findings" — wait for the scan.

### 1b. Generate from native classification

Consume-by-default requires a **group→tier mapping**, so pass your IdP group names (forwarded to `generate_abac.py --groups` via `GENERATE_ARGS`):

```bash
make generate ENV=dev GENERATE_ARGS='--groups "payments_ops,regional_analysts,viewers"'
```

With `enable_classification = true`, generation reads the authoritative **native `class.*`** tags and derives one `gr_treatment` per column. It is **fail-closed**: if native classification is enabled but unreadable or empty, generation **aborts** rather than silently falling back to LLM/DDL inference:

```
ERROR: Native classification read succeeded but returned 0 class.* findings for the declared footprint.
  Re-run with --allow-llm-sensitivity only to explicitly accept LLM/DDL inference.
```

`--allow-llm-sensitivity` (also via `GENERATE_ARGS`) is the **only** way to opt into inference — never the default.

### 1c. Coverage gate — the "tool says NO"

```bash
make coverage-gate ENV=dev
```

Blocks (non-zero exit) on any **classified-but-unprotected** column — a `gr_treatment` column with no covering mask. This is the gate the whole flow exists for.

### 1d. Validate, apply, and prove it works

```bash
make validate-generated ENV=dev   # static validation incl. overlap guard (reject >1 mask/column)
make apply ENV=dev                 # groups (consumed) → tag policies → gr_treatment assignments → masks → FGAC → workspace assignment/entitlement + warehouse CAN_USE + per-space Genie CAN_RUN ACLs  # needs account-admin auth (see Prereqs 5); consume groups must be workspace-assigned first
```

Then verify enforcement **by effect** — but note effective-access tests read the **promoted/split** config and require a released grant + a shared key column, so they run *after* `apply` (which promotes first):

```bash
make verify-access-spec ENV=dev VERIFY_KEY_COLUMN=<pk shared by all footprint tables>   # derived checks, no cluster
make verify-access      ENV=dev VERIFY_KEY_COLUMN=<pk>                                   # LIVE: query AS each tier's test principal
```

`verify-access` needs the tables' `SELECT` grant to exist (so `business_access_enabled=true` in dev, or a temporary grant to the test principals) and a `VERIFY_KEY_COLUMN` common to all three tables. Also confirm the agent still answers useful questions under masking.

**Dev's deliverable = validated rules + a working agent.** Not dev's tag assignments.

---

## Phase 2 — Promote the RULES only (dev → prod)

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod \
    DEST_CATALOG_MAP="serverless_stable_pyecip_catalog=finclear_sdp_demo_catalog"
```

Promotes the mapping, masking functions, ABAC/row-filter policies, and group→tier mapping. **Promote strips `tag_assignments`** from the promoted generated config, and `databricks_entity_tag_assignment` carries `lifecycle { ignore_changes = all }` — so Terraform creates the *generated* assignments on first apply but never fights the classifier's later re-tagging. (Dev's specific tag assignments are **not** carried; prod re-derives its own facts.)

> **`make promote` writes `envs/prod/env.auto.tfvars` itself** (with the discovered Genie space `name` and catalog-remapped `uc_tables`). It does **not** carry `enable_classification`, `business_access_enabled`, or `sql_warehouse_id` — you set those in prod (Phase 3). **Edit** the promote-written file; don't overwrite it, or you'll lose the `genie_spaces` block.

Because a tag-condition policy names no columns, it can sit in prod *before* prod is scanned and activate the instant a matching tag lands.

---

## Phase 3 — Prod: re-derive the FACTS

Edit the promote-written `envs/prod/env.auto.tfvars` to add the prod runtime settings:

```hcl
# (genie_spaces + uc_tables were written by `make promote` — keep them)
sql_warehouse_id        = "757ed630ccf6bac1"
enable_classification   = true
business_access_enabled = false     # still withheld until the gate is green
```

```bash
make apply-governance ENV=prod   # account + data_access (classification config + auto-tagging, policies, masks) — NO Genie space yet
```

Enabling classification turns on **auto-tagging** and the platform scans the footprint. Tags land on the **next scan (up to ~24h)** and are not backfilled instantly. There is no force-scan API — wait for tags to land before generating/gating in prod.

---

## Phase 4 — The hard gate (never skip)

Once prod tags land, regenerate from the **real prod facts**, then run the blocking checks:

```bash
make generate ENV=prod GENERATE_ARGS='--groups "payments_ops,regional_analysts,viewers"'   # reads LIVE prod class.* tags
make coverage-gate  ENV=prod    # fail on any classified-but-unprotected column
make apply          ENV=prod    # promote+apply the (re-derived) prod config
make audit-rulebook ENV=prod    # drift-vs-rulebook: prod-applied tags with NO covering policy/mask (reads promoted config; run after apply)
make verify-access  ENV=prod VERIFY_KEY_COLUMN=<pk>   # impersonation: analyst masked, operator raw
```

> The coverage gate is a **static** check of the freshly-generated config — so `make generate ENV=prod` (which reads live prod tags) **must** run first, or the gate has nothing to check and passes vacuously.

If prod surfaces a sensitive type your mapping doesn't cover, that's a **rule change**: update `treatment_config.json`, re-derive, re-gate. **You never proceed with an uncovered detected tag.**

---

## Phase 5 — Expose Genie LAST (the gate releases the grant)

Only after the gate is green, flip the exposure gate and re-apply. The business `SELECT` grant lives in the **data_access** layer, so you must run `make apply` (account → data_access → workspace), not `apply-genie` alone:

```hcl
# envs/prod/env.auto.tfvars — edit the promote-written genie_spaces entry to include prod tables
business_access_enabled = true
genie_spaces = [
  { name = "APJ Finance Agent", genie_space_id = "", uc_tables = [
      "finclear_sdp_demo_catalog.genierails_e2e.customers",
      "finclear_sdp_demo_catalog.genierails_e2e.payments",
      "finclear_sdp_demo_catalog.genierails_e2e.notes" ] },
]
```

```bash
make apply    ENV=prod    # on green, releases the withheld ACCESS: business SELECT (data_access) + workspace assignment/entitlement + warehouse CAN_USE + per-space Genie CAN_RUN ACLs, and creates the prod Genie space (workspace layer)
make evidence ENV=prod    # versioned compliance evidence: scan → tag → gr_treatment → policy → grant → approval
```

Exposing the agent **is** releasing the withheld access layer: the business `SELECT` **and** the per-space Genie `CAN_RUN` ACLs are both gated on `business_access_enabled` (the workspace-layer Genie ACLs only apply when it's `true`), so fail-closed is mechanical — no data grant *and* no Genie run access until the gate is green. Workspace assignment, the `workspace_consume` entitlement, and warehouse `CAN_USE` land here too. (A Genie space is created only when its entry has `genie_space_id = ""` **and** at least one `uc_tables` entry.)

---

## Phase 6 — Steady state

New sensitive data keeps arriving. Run the same checks on a schedule (the repo ships a scheduled governance job):

```bash
make audit-schema   ENV=prod    # untagged sensitive columns + stale assignments
make audit-rulebook ENV=prod    # newly-detected tags with no covering rule → add rule, re-derive
make generate-delta ENV=prod    # incremental tag_assignments after ALTER TABLE ADD/DROP/RENAME
```

A newly-tagged column is a **masking** gap, not an access breach (UC granted nothing you didn't ask for) — so no revoke; just add/derive the rule. For your most sensitive data, prefer "locked down until proven safe" over "open until tagged."

---

## Command reference

| Command | Phase | What it does |
|---|---|---|
| `make generate ENV=<e> GENERATE_ARGS='--groups "..."'` | 1/4 | Read native `class.*`, derive one `gr_treatment`/column, emit ABAC + masks (fail-closed on unreadable native; needs a group→tier mapping) |
| `make coverage-gate ENV=<e>` | 1/4 | **Block** on any classified-but-unprotected column — the "tool says NO" (static check of freshly-generated config) |
| `make validate-generated ENV=<e>` | 1 | Static validation (overlap guard: reject >1 mask/column) |
| `make apply ENV=<e>` | 1/4/5 | Full stack account → data_access → workspace (releases SELECT + creates Genie space) |
| `make apply-governance ENV=<e>` | 3 | account + data_access only (no Genie space) |
| `make promote SOURCE_ENV DEST_ENV DEST_CATALOG_MAP` | 2 | Promote **rules only** (strips tag_assignments), catalog remap; **writes** prod `env.auto.tfvars` |
| `make verify-access ENV=<e> VERIFY_KEY_COLUMN=<pk>` | 4 | Effective access via per-tier test principals (LIVE; reads promoted config; needs released grant) |
| `make verify-access-spec ENV=<e> VERIFY_KEY_COLUMN=<pk>` | 1/4 | Derived effective-access checks (no cluster; needs promoted config) |
| `make audit-rulebook ENV=<e>` | 4/6 | Drift vs rulebook — prod tags with no covering policy/mask (reads promoted config; run after apply) |
| `make audit-schema ENV=<e>` | 6 | Untagged sensitive columns + stale assignments |
| `make generate-delta ENV=<e>` | 6 | Incremental tag assignments after schema drift |
| `make evidence ENV=<e>` | 5 | Versioned compliance evidence report (`GENIERAILS_EVIDENCE_INTEGRATION=1` for live) |

`--allow-llm-sensitivity` (fail-closed opt-out) is passed the same way: `make generate ENV=<e> GENERATE_ARGS='--groups "..." --allow-llm-sensitivity'`.

Key config & code:
- [`shared/treatment_config.json`](../../treatment_config.json) — the `gr_treatment` precedence mapping (Phase 0 contract)
- [`shared/sensitivity_source.py`](../../sensitivity_source.py) — reads native `class.*` as authoritative source
- [`shared/treatment_derivation.py`](../../treatment_derivation.py) — collapses findings → one `gr_treatment` (incl. free-text escalation)
- [`shared/verify_effective_access.py`](../../verify_effective_access.py) — masked-vs-raw by impersonation
- [`shared/scripts/audit_schema_drift.py`](../../scripts/audit_schema_drift.py) — drift + drift-vs-rulebook

---

## Proven live (real workspaces)

On **prod** (`finclear_sdp_demo_catalog`), against **7 real platform-produced `class.*` tags** on realistic PII:

- Native detection → one `gr_treatment` per column (`free_text` → `redact` via free-text escalation)
- Coverage gate PASS on classified columns; WARN (surfaced, not hidden) on unclassified columns
- Enforcement applied; on green, the **business SELECT + workspace entitlement + warehouse CAN_USE + per-space Genie CAN_RUN ACLs** were all released (Genie space created and ACLed)
- Masked-vs-raw confirmed by query: card `****-4464` vs `5349 1210 3503 4464`, email `c***@…` vs full, cvv/`free_text` `[REDACTED]` vs raw

Dev (`serverless_stable_pyecip_catalog`) proved the same enforcement path plus a region-based row filter (unprivileged sees only in-region rows; privileged exception sees all).

---

## What this does — and does NOT — do

**It does:** discover the agent's footprint, read native classification, derive one enforcement treatment per column, prove coverage with a blocking gate, verify masking by impersonation, and release exposure only when green.

**It does not:**
- decide what's sensitive — **Unity Catalog's classifier does** (GenieRails reads it);
- remove the need for human review — generated rules are a **reviewable draft** you approve;
- make you legally compliant — it proves **coverage**, not compliance sign-off;
- replace Unity Catalog — it runs **on top of** it.

**Honest boundaries:**
- Auto-tags land on the **next scan (up to ~24h)** and aren't backfilled instantly; there is no force-scan API. Generating/gating before tags land correctly fail-closes.
- Auto-tagging is enabled for the 7 `class.*` types listed in Prerequisites (which cover this example). Other detected types (e.g. `iban_code`, `passport`) won't be auto-tagged unless you extend `classification_auto_tags`.
- Region-scoped classifiers only run in-region — out-of-region PII physically present in a workspace may go **undetected**; close that with a custom classifier or scan in the matching region.
- The scanner only tags **format-matchable** values — seed realistic synthetic PII in dev, or detection will under-report.
