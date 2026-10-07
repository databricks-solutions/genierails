# GenieRails Dev-to-Prod Walkthrough — reference

Lookup companion to the **[Dev-to-Prod Walkthrough](README.md)**: the full command reference, how the enforcement works under the hood, and a glossary of every term. You don't need to read this top-to-bottom — jump in when the walkthrough links you here.

---

## How it works (under the hood)

**Exposure follows the coverage check, mechanically.** Terraform itself holds back exactly the two things that let a user reach data through the agent until a recent passing coverage check (including the first-exposure check) covers them; everything else applies regardless:

| Control | When it applies |
|---|---|
| Table `SELECT` grant | **only after the masks exist and a recent coverage check passes** for it |
| Genie run permission (`CAN_RUN`) | **only once its groups' `SELECT` grants are applied** through that check |
| Workspace assignment + consume entitlement | applied on **every** apply (harmless without `SELECT`/`CAN_RUN`) |
| Warehouse `CAN_USE` | **not managed by GenieRails** — you grant it (Phase 5) |

So "expose last" isn't a policy you hope holds — there is simply no new or wider `SELECT` or `CAN_RUN` without a passing check. There is no on/off flag: the old `business_access_enabled` setting is deprecated and ignored (make warns while it is set; `false` does **not** revoke access). To withdraw access, remove the groups or `acl_groups` entries and apply.

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
| `make generate ENV=<e>` | 1 | (dev) Draft masks + access rules from the model and derive one `gr_treatment`/column from native `class.*` (fail-closed); groups come from `access_tier_groups` in `env.auto.tfvars` (or `GENERATE_ARGS='--groups "..."'`, saved there on first use). Re-runs keep reviewed rules and add rules only for uncovered columns; `GENERATE_ARGS='--allow-rule-changes'` accepts the model's changes |
| `make derive-assignments ENV=<e>` | 4 | (prod) Re-derive **only** `tag_assignments` from live `class.*`, reusing the promoted rules unchanged — no model call (fail-closed; requires a prior `promote`) |
| `make coverage-gate ENV=<e>` | 1/4 | Fail if any tagged-sensitive column has no mask (the "says NO" check). Every plan/apply also runs it against live tags |
| `make validate-generated ENV=<e>` | 1/4 | Static validation incl. the one-mask-per-column guard |
| `make apply ENV=<e>` | 1/5 | Full stack (account → data_access → workspace; auto-promotes same-env first); creates the Genie agent; grants business access only through the coverage check |
| `make apply-governance ENV=<e>` | — | Governance-team command: enforcement only (account + data_access); no Genie agent |
| `make genie-adopt-preflight ENV=<e>` | — | Read-only. Before the one-time upgrade to secret-free Genie state, checks that every Genie agent created by an earlier version can be adopted with its current ID (ID file, workspace, GET 200). `make apply` runs it first and stops if any agent fails. |
| `make rehearse ENV=dev VERIFY_KEY_COLUMN=<pk>` | 1 | (dev) live derive → validate-generated → coverage-gate → apply → verify-access, stopping at the first failure |
| `make release ENV=prod VERIFY_KEY_COLUMN=<pk>` | 5 | (prod) Placeholder guard → lock → live derive → validate → coverage → promote → read-only rulebook audit → all-layer apply → `verify-access` |
| `make maintain ENV=prod` | 6 | (prod, scheduled) audit-schema → derive-assignments → coverage-gate → validate-generated → apply-governance → audit-rulebook; never changes access or Genie |
| `make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP="dev_cat=prod_cat"` | 2 | Promote **rules only** (leaves tag assignments behind); creates + writes prod `env.auto.tfvars`. Policy names take the prod catalog (`gr_mask_<prod_catalog>_<treatment>`) only when a read-only policy listing of the prod catalog (prod `auth.auto.tfvars`) shows neither the old nor the new name and prod's state doesn't hold the old key; otherwise it keeps its name, since renaming a live policy would drop and recreate it |
| `make verify-access ENV=<e> VERIFY_KEY_COLUMN=<pk>` | 1/5 | Prove masking by querying as per-tier test principals (**needs the business grants applied**) |
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
- **`CAN_RUN` / `CAN_USE`** — Databricks permissions: `CAN_RUN` lets a group open and run a Genie agent (granted only through the coverage check); `CAN_USE` lets a group run a SQL warehouse (you grant it yourself).
- **`class.*` tag** — a tag Unity Catalog's classifier writes on a column it finds sensitive (e.g. `class.email_address`).
- **coverage check** — `make coverage-gate`; the blocking check that fails if any tagged-sensitive column has no covering mask/policy. The "tool says NO" step.
- **drift** — a gap between what's tagged and what's protected; `audit-rulebook` reports it.
- **entitlement / workspace assignment** — what lets a group *into* a workspace at all (applied every apply; harmless without a data grant).
- **evidence** — the compliance record `make evidence` produces (what was scanned, tagged, protected, and approved).
- **exposure gate (retired)** — the old on/off `business_access_enabled` switch, now ignored. Exposure follows the coverage check instead, which Terraform enforces: no new or wider `SELECT` grant or Genie run permission without a recent pass. (The former `business_access_enabled` flag is retired and ignored.)
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
