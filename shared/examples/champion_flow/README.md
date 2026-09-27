# GenieRails Champion Flow — Native-Classification-Driven Governance, End-to-End

The canonical GenieRails flow: take a **curated Genie agent in dev** and ship it to **production** without ever exposing sensitive data — where the platform decides *what* is sensitive, GenieRails codifies *how* it's enforced, and a coverage gate blocks promotion until every sensitive column the agent can reach is provably covered.

> This walkthrough has been **validated live end-to-end on real AWS Databricks workspaces** (a dev and a prod workspace). It supersedes the older LLM-overlay demo (`aus_bank_demo`) as the reference flow: sensitivity now comes from **native UC Data Classification**, not from an LLM reading DDL.

---

## The mental model (read this first)

> **Prod decides what's sensitive and is the final gate. Dev is the rehearsal.**
> You promote the **rules**; you re-derive the **facts**. Expose the agent **last**, only after a passing prod coverage check. Fail closed, never fail open.

Three things flow through the pipeline differently:

| | What it is | Where it lives | Promoted? |
|---|---|---|---|
| **Rules** | `class.* → gr_treatment → masking function`, group→tier, row filters | version-controlled config | ✅ promoted dev→prod |
| **Facts** | which columns the scanner tagged in *this* workspace | the workspace, via the scan | ❌ re-derived per env |
| **Identity** | groups + membership | your IdP (AIM/SCIM) | ❌ consumed, never minted |

Governance is **two locks, not one**:
- **Lock 1 — Access:** the grant chain `USE CATALOG → USE SCHEMA → SELECT` decides *who can reach a table*.
- **Lock 2 — Masking/ABAC:** column masks + row filters decide *what they see through it*.

The single enforcement key is **`gr_treatment`** — GenieRails derives exactly **one** value per column from the column's native `class.*` findings (via strictest-first precedence), so Unity Catalog's "one mask per column" rule is never violated.

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

The `notes.free_text` column is the interesting one — the scanner tags it with **multiple** `class.*` types (email + name), and GenieRails must collapse that to **one** treatment (`redact`). That's the champion-flow crux.

---

## Prerequisites

1. **Enable UC Data Classification** on the catalog (the platform, not an LLM, decides sensitivity). GenieRails wires this via the `databricks_data_classification_catalog_config` Terraform resource (Databricks provider **≥ 1.111.0**) when `enable_classification = true`.
2. **Sync groups from your IdP via AIM** (GA for Entra ID across clouds; Okta on AWS/GCP; SCIM where AIM isn't available). GenieRails **consumes** these groups by name — it never mints them. *(APJ/Azure note: AIM needs Premium tier + single tenant; cross-tenant estates stay on SCIM.)*
3. **Provision per-tier test principals** (one service principal per access tier) so effective-access can be verified by querying *as* each tier — not by inspecting config.
4. Python 3, Terraform, and account-admin credentials — see [Prerequisites](../../docs/prerequisites.md).

---

## Phase 0 — The contract: `gr_treatment` mapping (author once)

Before any generation, confirm the version-controlled mapping in [`shared/treatment_config.json`](../../treatment_config.json). It is **ordered strictest-first** and maps each sensitivity finding to exactly one enforcement treatment + masking function:

```jsonc
{
  "tag_key": "gr_treatment",
  "description": "GenieRails-owned enforcement treatment derived from sensitivity findings. Ordered strictest-first.",
  "treatments": [
    {"value": "redact",        "masking_function": "mask_redact",             "sources": [["pci_level","redacted_cvv"], ["pii_level","redacted_mixed"], ["phi_level","redacted"]]},
    {"value": "card_last4",    "masking_function": "mask_credit_card_last4",  "sources": [["pci_level","masked_card_last4"]]},
    {"value": "ssn_last4",     "masking_function": "mask_ssn",                "sources": [["pii_level","masked_ssn"]]},
    {"value": "email_partial", "masking_function": "mask_email",              "sources": [["pii_level","masked_email"]]},
    {"value": "phone_partial", "masking_function": "mask_phone",              "sources": [["pii_level","masked_phone"]]},
    {"value": "date_year",     "masking_function": "mask_date_to_year",       "sources": [["pii_level","masked_dob"]]}
    // ... regional treatments (tfn/medicare/bsb/aadhaar), round_amount, generic_partial
  ]
}
```

**Why this is the load-bearing artifact:**
- The classifier owns the `class.*` **facts** (what's sensitive). GenieRails owns `gr_treatment` (how it's enforced). Two keys, two writers, no collision.
- `free_text` tagged `class.email_address` + `class.name` → both map into the list → **strictest-first wins** → single `gr_treatment = redact`. One mask resolves; UC never errors.
- **This mapping is the promotable rule.** The tagged columns are not.

You confirm/extend this file — you don't author it from scratch. It ships with defaults for the common `class.*` types.

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
enable_classification   = true      # turn on native UC Data Classification + auto-tagging
business_access_enabled = false     # exposure gate: hold the business SELECT until the gate is green
genie_spaces            = [ { genie_space_id = "" } ]   # paste your curated dev Space id
```

> If dev data is sparse, **seed representative synthetic PII** so the scanner has realistic values to detect (fake `example.com` emails and `000-` SSNs will *not* be detected — the scanner ignores non-format-matching values).

### 1b. Generate from native classification

```bash
make generate ENV=dev
```

With `enable_classification = true`, generation reads the authoritative **native `class.*`** tags and derives one `gr_treatment` per column. It is **fail-closed**: if native classification is enabled but unreadable or empty, generation **aborts** rather than silently falling back to LLM/DDL guessing.

```
ERROR: native classification enabled but no class.* tags could be read for the footprint.
  Re-run with --allow-llm-sensitivity only to explicitly accept LLM/DDL inference.
```

`--allow-llm-sensitivity` is the **only** way to opt into inference — never the default.

To consume your IdP groups (the default), pass their names; GenieRails looks them up and refuses to invent groups:

```bash
make generate ENV=dev GROUPS="payments_ops,regional_analysts,viewers"
```

### 1c. Coverage gate — the "tool says NO"

```bash
make coverage-gate ENV=dev
```

Blocks (non-zero exit) on any **classified-but-unprotected** column — a `class.*`/`gr_treatment` column with no covering mask. This is the gate the whole flow exists for; the old silent "drop uncovered tags to fit a policy cap" behavior is **removed**.

### 1d. Apply + prove it works

```bash
make apply ENV=dev            # groups (consumed) → tag policies → gr_treatment assignments → masks → FGAC → Genie ACLs
make verify-access ENV=dev    # query AS each tier's test principal: masked-for-unprivileged, raw-for-privileged
```

`verify-access` confirms enforcement by **effect**, not by inspecting config (`verify-access-spec` prints the derived checks without a live cluster). Also confirm the agent still answers useful questions under masking.

**Dev's deliverable = validated rules + a working agent.** Not dev's tag assignments.

---

## Phase 2 — Promote the RULES only (dev → prod)

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod \
    DEST_CATALOG_MAP="serverless_stable_pyecip_catalog=finclear_sdp_demo_catalog"
```

Promotes the mapping, masking functions, ABAC/row-filter policies, classifier config, group→tier mapping, and structural grants — with `tag_owner = classifier` (dev tag assignments are **never** promoted; Terraform does not manage classifier-produced assignments).

**Deliberately withheld:** dev tag assignments (prod re-derives), group *definitions* (arrive via AIM/SCIM per env), and the **business SELECT grant** (`business_access_enabled` stays `false` until the prod gate passes).

Because a tag-condition policy names no columns, it can sit in prod *before* prod is scanned and activate the instant a matching tag lands.

---

## Phase 3 — Prod: re-derive the FACTS

`envs/prod/env.auto.tfvars`:

```hcl
sql_warehouse_id        = "757ed630ccf6bac1"
uc_tables = [
  "finclear_sdp_demo_catalog.genierails_e2e.customers",
  "finclear_sdp_demo_catalog.genierails_e2e.payments",
  "finclear_sdp_demo_catalog.genierails_e2e.notes",
]
enable_classification   = true
business_access_enabled = false     # still withheld
```

```bash
make apply-governance ENV=prod   # lay down account + data_access (classification config, policies, masks) — NO Genie space yet
```

Enabling classification turns on **auto-tagging** and the platform scans the footprint. An **initial** scan on a fresh footprint lands tags fast (~minutes); re-classifying *changed* data in already-scanned tables follows the slower ~24h incremental cadence. There is no force-scan API — wait for tags to land.

> ⚠️ **Real gotcha the live e2e caught:** enabling classification *scope* is not the same as enabling *auto-tagging*. Confirm `auto_tag_configs` are `AUTO_TAGGING_ENABLED` for the footprint — otherwise classification is "on" but **nothing ever gets tagged** (a silent fail-open). GenieRails now emits this configuration.

---

## Phase 4 — The hard gate (never skip)

Once prod tags land, run the blocking checks against the **real** prod footprint:

```bash
make coverage-gate  ENV=prod    # fail on any classified-but-unprotected column, unmapped tag, missing mask
make audit-rulebook ENV=prod    # drift-vs-rulebook: prod-applied tags (class.* + governed) with NO covering policy/mask
make verify-access  ENV=prod    # impersonation: analyst sees masked, operator sees raw, no one reaches a forbidden table
```

If prod surfaces a sensitive type your mapping doesn't cover, that's a **rule change**: update `treatment_config.json`, re-derive, re-gate. **You never proceed with an uncovered detected tag.**

---

## Phase 5 — Expose Genie LAST (the gate releases the grant)

Only after the gate is green, flip the exposure gate and apply the Genie layer:

```hcl
# envs/prod/env.auto.tfvars
business_access_enabled = true
genie_spaces            = [ { genie_space_id = "" } ]   # created/promoted here
```

```bash
make apply-genie ENV=prod     # creates the prod Genie space + releases the business SELECT grant
make evidence     ENV=prod    # versioned compliance evidence: scan → tag → gr_treatment → policy → grant → approval
```

Exposing the agent **is** issuing the withheld SELECT — so fail-closed is mechanical, not a policy you hope holds: no grant until the gate is green.

---

## Phase 6 — Steady state

New sensitive data keeps arriving. Run the same checks on a schedule (the repo ships a scheduled governance job):

```bash
make audit-schema   ENV=prod    # untagged sensitive columns + stale assignments
make audit-rulebook ENV=prod    # newly-detected tags with no covering rule → add rule, re-derive (binds immediately; no re-scan)
make generate-delta ENV=prod    # incremental tag_assignments after ALTER TABLE ADD/DROP/RENAME
```

A newly-tagged column is a **masking** gap, not an access breach (UC granted nothing you didn't ask for) — so no revoke; just add/derive the rule. For your most sensitive data, prefer "locked down until proven safe" over "open until tagged."

---

## Command reference

| Command | Phase | What it does |
|---|---|---|
| `make generate ENV=<e>` | 1/3 | Read native `class.*`, derive one `gr_treatment`/column, emit ABAC + masks (fail-closed on unreadable native) |
| `make coverage-gate ENV=<e>` | 1/4 | **Block** on any classified-but-unprotected column — the "tool says NO" |
| `make validate-generated ENV=<e>` | 1 | Static validation (overlap guard: reject >1 mask/column) |
| `make apply ENV=<e>` | 1 | Full stack: groups → tag policies → `gr_treatment` → masks → FGAC → Genie ACLs |
| `make apply-governance ENV=<e>` | 3 | account + data_access only (no Genie space) |
| `make apply-genie ENV=<e>` | 5 | workspace layer only — Genie space + release business grant |
| `make promote SOURCE_ENV DEST_ENV DEST_CATALOG_MAP` | 2 | Promote **rules only**, `tag_owner=classifier`, catalog remap |
| `make verify-access ENV=<e>` | 1/4 | Effective access via per-tier test principals (LIVE) |
| `make verify-access-spec ENV=<e>` | 1/4 | Derived effective-access checks (no cluster) |
| `make audit-rulebook ENV=<e>` | 4/6 | Drift vs rulebook — prod tags with no covering policy/mask |
| `make audit-schema ENV=<e>` | 6 | Untagged sensitive columns + stale assignments |
| `make generate-delta ENV=<e>` | 6 | Incremental tag assignments after schema drift |
| `make evidence ENV=<e>` | 5 | Versioned compliance evidence report (`GENIERAILS_EVIDENCE_INTEGRATION=1` for live) |

Key config & code:
- [`shared/treatment_config.json`](../../treatment_config.json) — the `gr_treatment` precedence mapping (Phase 0 contract)
- [`shared/sensitivity_source.py`](../../sensitivity_source.py) — reads native `class.*` as authoritative source (#30)
- [`shared/treatment_derivation.py`](../../treatment_derivation.py) — collapses findings → one `gr_treatment` (Option-B)
- [`shared/verify_effective_access.py`](../../verify_effective_access.py) — masked-vs-raw by impersonation
- [`shared/scripts/audit_schema_drift.py`](../../scripts/audit_schema_drift.py) — drift + drift-vs-rulebook

---

## Proven live (real workspaces)

On **prod** (`finclear_sdp_demo_catalog`), against **7 real platform-produced `class.*` tags** on realistic PII:

- Native detection → one `gr_treatment` per column (`free_text` email+name → single `redact`)
- Coverage gate PASS on classified columns; WARN (surfaced, not hidden) on unclassified `ssn`/`full_name`
- Enforcement applied; **business SELECT released only after the gate passed**
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
- An **initial** scan on a fresh footprint is fast; **re-classifying changed data** in scanned tables is the ~24h incremental cadence (no force-scan API).
- Region-scoped classifiers only run in-region — out-of-region PII physically present in a workspace may go **undetected**; close that with a custom classifier or scan in the matching region.
- The scanner only tags **format-matchable** values — seed realistic synthetic PII in dev, or detection will under-report.
