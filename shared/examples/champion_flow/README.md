# GenieRails Champion Flow — Native-Classification-Driven Governance, End-to-End

Take a curated Genie agent in **dev** and ship it to **production** without ever exposing sensitive data: Unity Catalog decides *what* is sensitive (native Data Classification), GenieRails derives *how* it's enforced, and a **coverage gate blocks promotion** until every sensitive column the agent can reach is provably covered.

> **No Genie agent yet?** The optional, self-contained
> [`setup_sample_env.py`](setup_sample_env.py) creates the exact three-table
> footprint, realistic synthetic PII, and a sample Genie Space. See
> [Sample Environment Setup (Optional)](#sample-environment-setup-optional).

> **New to GenieRails? Read this top-to-bottom once.** Every command runs from the cloud root (`cd aws` or `cd azure`).

---

## The mental model

> **Prod decides what's sensitive and is the final gate. Dev is the rehearsal.** You promote the **rules**; you re-derive the **facts**. Expose the agent **last**, only after a passing prod coverage check. Fail closed, never fail open.

| | What it is | Promoted dev→prod? |
|---|---|---|
| **Rules** | `class.* → gr_treatment → masking function`, group→tier, row filters | ✅ promoted |
| **Facts** | which columns the scanner tagged in *this* workspace | ❌ re-derived per env |
| **Identity** | groups + membership | ❌ consumed from your IdP, never minted |

**Governance is three layers — GenieRails generates all three as code:**
- **Layer 1 — Data access:** the grant chain `USE CATALOG → USE SCHEMA → SELECT` decides *who can reach a table*.
- **Layer 2 — Masking/ABAC:** column masks + row filters decide *what they see through it*.
- **Layer 3 — Agent access:** workspace assignment (`USER`) + the `workspace_consume` entitlement + per-space Genie `CAN_RUN` ACLs decide *whether a group can open and run the Genie space*.

**What the exposure gate (`business_access_enabled`) actually holds** — this matters, so it's exact:

| Control | When it applies |
|---|---|
| Table `SELECT` grant | **held until `business_access_enabled=true`** (the coverage gate) |
| Genie `CAN_RUN` ACL | **held until `business_access_enabled=true`** |
| Workspace assignment (`USER`) + `workspace_consume` entitlement | applied on **every** `apply` (not gated) — harmless without `SELECT`/`CAN_RUN` |
| Warehouse `CAN_USE` | **not managed by GenieRails** — grant it to your tier groups yourself (see Phase 5) |

So "expose last" is mechanical: no `SELECT` **and** no Genie `CAN_RUN` until the gate passes.

The single enforcement key is **`gr_treatment`** — GenieRails derives exactly **one** value per column from its `class.*` findings (strictest-first, with free-text escalation), so Unity Catalog's "one mask per column" rule is never violated.

---

## At a glance — the phase map

| Phase | Where | What you're doing | Commands you run | You're done when | Users can query it? |
|---|---|---|---|---|---|
| **0 Setup** | dev | Add your credentials and create the per-environment config folders. | `make setup`, `make init-env ENV=dev` | `envs/dev/auth.auto.tfvars` and `env.auto.tfvars` are filled in | No |
| **1 Build & test in dev** | dev | Databricks scans your data and labels the sensitive columns; GenieRails drafts the protection rules from those labels, checks that every sensitive column is covered, applies the protections, and you confirm the masking actually works — all in a safe dev copy first. | `enable-classification` → *(wait for the scan)* → `generate` → `coverage-gate` → `validate-generated` → `apply` → `verify-access` | The coverage check passes and you've seen sensitive values come back masked | No — turn it on only briefly to run the masking check, then off |
| **2 Promote the rules** | dev→prod | Copy only the *rules* to production — never the dev data, and never dev's column labels. | `make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP=...` | `envs/prod/env.auto.tfvars` is written | — |
| **3 Scan in prod** | prod | Let production scan its *own* real data and label its own sensitive columns. | fill `envs/prod/auth.auto.tfvars` → `enable-classification` → *(wait for the scan)* | Production's sensitive columns are labeled | No |
| **4 Prove coverage** | prod | Re-check that every sensitive column production found is protected, apply the protections, and confirm the masking works. This is the check that blocks the release if anything is still uncovered. | `generate` → `coverage-gate` → `apply-governance` → `audit-rulebook` → `verify-access` | The coverage check passes and masking is confirmed | No |
| **5 Open to users** | prod | Only now — with coverage proven — release access so business users can query the agent. | set `business_access_enabled=true` → `make apply ENV=prod` → `make evidence` | Business users can reach the agent (query + run access released) | Yes |
| **6 Keep it covered** | prod | On a schedule, re-scan and re-check so sensitive data that arrives later stays protected. | scheduled `audit-schema` / `audit-rulebook` / `generate-delta` | — | Yes |

> **New here?** Each command is walked through step-by-step in its phase below, and jargon like *coverage check* (`coverage-gate`), *masking*, and *rules vs. facts* is defined in the [Glossary](#glossary).

---

## Prerequisites

1. **Databricks Terraform provider `~> 1.111.0`** (auto-selected) and **GNU Make** (`make`) + Python 3 + Terraform on your PATH. *(On macOS, Apple's `/usr/bin/make` and Homebrew may be blocked by an unaccepted Xcode license; install GNU Make another way — e.g. `conda install make` — and ensure it's first on `PATH`.)*
2. **A deploying Service Principal** whose `client_id` / `client_secret` you put in `envs/<env>/auth.auto.tfvars`. GenieRails authenticates with **this SP's credentials, not a CLI profile.** The SP needs, on the **same** account as the workspace:
   - **Account Admin** (create/read groups, workspace assignment), **Workspace Admin** (Genie, warehouse), **Metastore Admin** (tags, FGAC),
   - **`EXECUTE` on `system.ai.databricks-claude-sonnet-4-6`** (`generate` calls the Foundation Model),
   - Catalog **`APPLY TAG`** + **`ASSIGN`** on the `class.*` tags (so auto-tagging can write tags).
3. **UC Data Classification** available on the catalog (enabled per-env by `make enable-classification`, below).
4. **Groups synced from your IdP** (AIM — GA for Entra ID across clouds, Okta on AWS/GCP; SCIM otherwise). GenieRails **consumes** them by name; `manage_groups = false` (consume, not mint) — see Step 0.
5. For `verify-access`: a **shared key column** across the footprint tables. `verify-access` creates and then deletes temporary `genierails-verify-<tier>` test principals (needs account-admin; your IdP sync must tolerate a transient non-IdP group member).

See [Prerequisites](../../docs/prerequisites.md) for SP setup details.

---

## Fill these in before you start

Gather these once — every phase reuses them:

| Value | Where it comes from |
|---|---|
| **Deploying Service Principal** `client_id` + `client_secret` | An SP with **Account Admin + Workspace Admin + Metastore Admin** on the *same* account (see Prerequisites). Goes in `envs/<env>/auth.auto.tfvars`. |
| **Dev / prod catalog names** | Your UC catalogs (e.g. `dev_finance` / `prod_finance`). |
| **SQL warehouse id** (per env) | An existing serverless warehouse id — **or leave blank** to auto-create a serverless PRO warehouse. |
| **Curated Genie space** (needed for the agent) | From the Genie UI URL. **To deploy the Genie agent and get Layer-3 `CAN_RUN`, you MUST configure a `genie_spaces` entry** (an existing space id, or `genie_space_id=""` + `uc_tables` to create one). `genie_spaces = []` governs *data only* — no agent, no Layer 3. |
| **IdP group names** (one per access tier, strictest first) | Your AIM/SCIM-synced groups, e.g. `payments_ops,regional_analysts,viewers`. GenieRails consumes them — it never creates them. |
| **Shared key column** | One column present in **all** footprint tables (e.g. `customer_id`) — needed by `verify-access` to pair rows. |

---

## Sample Environment Setup (Optional)

This is demo tooling only; skip it when using your own tables and Genie Space.
It uses no external data source and has one dependency:

```bash
cd shared/examples/champion_flow
python -m pip install -r requirements.txt
python setup_sample_env.py --profile DEFAULT --catalog my_catalog --warehouse-id abc123
```

The script prints the exact `uc_tables`, `genie_spaces`, and
`sql_warehouse_id` snippet to paste into `env.auto.tfvars`. Re-runs are safe.
Teardown relies on a local ownership record and does not infer resources to
delete:

```bash
python teardown_sample_env.py --profile DEFAULT --catalog my_catalog
# Equivalent: add --teardown to the setup command.
```

Use `--help` to see the `--host`, `--schema`, `--rows`, and environment-variable
alternatives.

Once those three values are in `env.auto.tfvars`, follow the rest of this guide from **Phase 0** below unchanged — the sample footprint (`customers` / `payments` / `notes`, seeded with realistic synthetic PII) is exactly what every phase here assumes.

---

## Step 0 — Bootstrap (dev)

```bash
cd aws                      # or: cd azure
make setup
make init-env ENV=dev
cp ../shared/examples/champion_flow/env.auto.tfvars.example envs/dev/env.auto.tfvars
```

Here `make setup` prepares the cloud root (pins the Terraform provider, etc.), and **`make init-env ENV=dev` creates the local `envs/dev/` config folder and fills it with default/template files** — this is purely local scaffolding on your machine and makes **no** calls to Databricks. The `cp` line seeds `env.auto.tfvars` from the champion-flow example. Those commands just give you empty config files to fill in — so next you edit three of them:
- **`envs/dev/auth.auto.tfvars`** — the deploying SP `client_id` / `client_secret` + workspace host/id.
- **`envs/dev/env.auto.tfvars`** — `uc_tables`, `sql_warehouse_id` (or blank), `enable_classification = true`, `business_access_enabled = false`.
- **`envs/account/env.auto.tfvars`** — change the generated `manage_groups = true` to **`manage_groups = false`** (this flow consumes IdP groups; it does not mint them).

---

## Phase 1 — Dev: enable classification, then rehearse the rules

**Why dev:** not to discover what's sensitive (prod does that) — to prove the masks fire, the agent still answers, and you produce a reviewable artifact, safely off live PII.

**1a. Enable classification first** (this is the key greenfield step — do it *before* `generate`):
```bash
make enable-classification ENV=dev
```
This applies only the UC Data Classification + auto-tagging config for your footprint. No masks/policies/grants yet.

**1b. Wait for the scan, then check it landed.** The initial scan is asynchronous — anywhere from a few minutes to ~24h, and there is no force-scan API. Re-run this query until your recognizable PII columns show up with `class.*` tags:
```sql
SELECT table_name, column_name, tag_name
FROM system.information_schema.column_tags
WHERE catalog_name = '<your-catalog>' AND schema_name = '<your-schema>'
  AND tag_name LIKE 'class.%';
```

**What the result means:**

- ✅ **Landed — go to 1c.** The query returns one row per recognizable PII column, each carrying a `class.*` tag. For the sample footprint you would expect roughly:
  ```
  table_name   column_name          tag_name
  customers    email                class.email_address
  customers    phone                class.phone_number
  customers    ssn                  class.us_ssn
  payments     credit_card_number   class.credit_card
  payments     cvv                  class.card_security_code
  notes        free_text            class.email_address
  ```
  Once your obvious sensitive columns are tagged, the scan has run — proceed.
- ⏳ **Not yet — wait and re-run.** **Zero rows** (or your obvious PII missing) almost always just means the async scan has not finished. Wait a few minutes and run it again (it can take up to ~24h).
- ⚠️ **Do not expect every column.** The scanner only tags values it can *format-match*, so some columns (free-text, unusual formats, or a type the built-in classifier does not cover) may stay untagged — that is expected. The `coverage-gate` in step 1d is what enforces completeness and blocks the release if anything sensitive is still uncovered. If the query stays empty even though the data was clearly scanned, your values probably are not format-matchable: seed **realistic** synthetic PII (the scanner ignores fake `example.com` emails / `000-` SSNs) and it re-scans.

**1c. Generate the rules from native classification:**
```bash
make generate ENV=dev GENERATE_ARGS='--groups "payments_ops,regional_analysts,viewers"'
```
**About `--groups` — these are *your own* groups, not names GenieRails defines.** They are account groups synced from your identity provider (Entra ID / Okta) via **AIM or SCIM**. The `payments_ops,regional_analysts,viewers` shown above are just **example placeholders** — replace them with the real group names in your workspace (the ones you listed under [Fill these in before you start](#fill-these-in-before-you-start)). Key points:
- **One group per access tier** (usually 2–5), listed in tier order — this is the group→tier mapping GenieRails applies (e.g. one tier sees full values, another sees masked, another sees the least).
- The groups **must already exist**. GenieRails *consumes* them by exact name and never creates or renames them (`manage_groups = false`); if a name isn't found, generation stops with a clear preflight error.
- **No tiered IdP groups to point at yet (e.g. just trying the demo)?** Use `--create-groups` *instead of* `--groups` — a demo/greenfield-only opt-in that lets the tool invent and create the tier groups for you (requires `manage_groups = true`). Production should always consume real IdP-synced groups via `--groups`.

Generation then reads the authoritative `class.*` tags and is **fail-closed**: if classification is enabled but unreadable/empty it aborts (opt into LLM inference only with `GENERATE_ARGS='--groups "..." --allow-llm-sensitivity'`).

**1d. Gate → validate → apply → verify (in order):**

First, the one term you'll act on here: **`business_access_enabled`** is the **exposure gate** — a single `true`/`false` setting in `envs/<env>/env.auto.tfvars`. While it is `false` (the default), GenieRails applies every mask and policy but **withholds** the business-user `SELECT` grant and the Genie `CAN_RUN` permission, so no one can reach the agent yet. Setting it to `true` and re-running `make apply` is what **releases** that access. Keep it `false` while you build and test; turn it on only **after** `coverage-gate` passes — briefly here in dev to run the masking check, then for real in prod at Phase 5. **To change it:** open `envs/dev/env.auto.tfvars` in a text editor and set `business_access_enabled = true`.

```bash
make coverage-gate      ENV=dev                          # expect: PASS — N columns fully protected
make validate-generated ENV=dev                          # static checks incl. overlap guard
make apply              ENV=dev                          # gate CLOSED: masks applied; SELECT + Genie CAN_RUN withheld

# --- only after coverage-gate PASSED, flip the gate to exercise verify-access ---
# edit envs/dev/env.auto.tfvars:  business_access_enabled = true
make apply              ENV=dev                          # releases SELECT (+ Genie CAN_RUN if a space is configured)
make verify-access      ENV=dev VERIFY_KEY_COLUMN=customer_id   # query AS each tier: masked vs raw
```
> Do **not** flip `business_access_enabled = true` until the dev `coverage-gate` passes. Also confirm the agent still answers useful questions under masking.
>
> **Warehouse access:** if a tier group must run the Genie space's warehouse, grant it `CAN_USE` yourself — GenieRails does not manage warehouse permissions.

Dev's deliverable = **validated rules + a working agent** (not dev's tag assignments).

---

## Phase 2 — Promote the RULES only (dev → prod)

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP="dev_finance=prod_finance"
```
Promotes the **rules** — the mapping, masking functions, ABAC/row-filter policies, and group→tier mapping — and deliberately **leaves dev's tag assignments behind** (which specific columns got tagged is a *fact* about dev's data; prod re-derives its own from its own scan in Phase 3).

*What is `ignore_changes = all`?* It is a standard Terraform **lifecycle** setting placed on the tag-assignment resource (`databricks_entity_tag_assignment`). It tells Terraform: **once these tags exist, do not try to change or delete them.** That is what lets the **classifier own the `class.*` tags** in prod — when the scan (re)writes a tag, Terraform leaves it alone instead of reverting it to its own copy. In short: the classifier owns the tags, GenieRails owns the rules, and the two never overwrite each other.

> **`make promote` writes `envs/prod/env.auto.tfvars` for you** (with the discovered `genie_spaces` + catalog-remapped `uc_tables`, and `sql_warehouse_id = ""`). **Edit** that file — do not overwrite it:
> - **Replace** the `sql_warehouse_id = ""` line with your prod warehouse id (or leave `""` to auto-create).
> - **Add** `enable_classification = true` and `business_access_enabled = false`.

---

## Phase 3 — Prod: set up, enable classification, re-derive the facts

```bash
make init-env ENV=prod          # then fill envs/prod/auth.auto.tfvars (prod SP + workspace host/id)
make enable-classification ENV=prod
```
`make init-env ENV=prod` does the same local scaffolding as Step 0, now for prod — it creates the `envs/prod/` folder with default config files (no Databricks calls); you then fill `envs/prod/auth.auto.tfvars` with the prod deploying SP (`client_id` / `client_secret`) and the prod workspace host + id. `make enable-classification ENV=prod` is the same action as step 1a, now pointed at prod.

Wait for prod's scan and confirm `class.*` tags on the prod footprint (same SQL as 1b, prod catalog). Real customer PII only exists in prod — this is where the true facts land.

---

## Phase 4 — The hard gate (never skip)

```bash
make generate       ENV=prod GENERATE_ARGS='--groups "payments_ops,regional_analysts,viewers"'   # reads LIVE prod class.* tags
make coverage-gate  ENV=prod                          # blocks on any classified-but-unprotected column
make apply-governance ENV=prod                        # account + data_access ONLY (no Genie space yet)
make audit-rulebook ENV=prod                          # drift: prod tags with no covering policy/mask
make verify-access  ENV=prod VERIFY_KEY_COLUMN=customer_id
```
The `--groups` flag works exactly as in **step 1c** above — they are *your own* IdP group names, and `payments_ops,regional_analysts,viewers` are just placeholders. Use the **same** tier groups you used in dev (your IdP syncs the same groups into the prod workspace).

> Use **`apply-governance`** here, not `make apply` — a full `apply` runs the workspace layer and would create the Genie space before the gate passes. If prod surfaces a type your mapping doesn't cover, update `treatment_config.json`, re-`generate`, re-gate.

---

## Phase 5 — Expose Genie LAST (the gate releases access)

Only after the prod gate is green, flip the exposure gate and run the **full** apply:
```hcl
# envs/prod/env.auto.tfvars
business_access_enabled = true
```
```bash
make apply ENV=prod
```
This creates the prod Genie space and **releases the withheld access**: the business `SELECT` grant **and** the per-space Genie `CAN_RUN` ACLs (both gated on `business_access_enabled`). The workspace assignment + `workspace_consume` entitlement were already applied.

Then, so business users can run the warehouse behind the space, **grant the tier groups `CAN_USE` on the warehouse** (GenieRails doesn't manage this — do it via the warehouse permissions UI/API). Finally, capture evidence:
```bash
GENIERAILS_EVIDENCE_INTEGRATION=1 GENIERAILS_EVIDENCE_APPROVED_BY="<you>" \
  make evidence ENV=prod WAREHOUSE_ID=<prod-warehouse-id>
```

---

## Phase 6 — Steady state

New sensitive data keeps arriving. Run on a schedule (the repo ships a scheduled governance job):
```bash
make audit-schema   ENV=prod    # untagged sensitive columns + stale assignments
make audit-rulebook ENV=prod    # newly-detected tags with no covering rule → add rule, re-derive
make generate-delta ENV=prod    # incremental tag assignments after ALTER TABLE ADD/DROP/RENAME
```
A newly-tagged column is a **masking** gap, not an access breach (UC granted nothing you didn't ask for) — add/derive the rule. For your most sensitive data, prefer "locked down until proven safe" over "open until tagged."

---

## Command reference

| Command | Phase | What it does |
|---|---|---|
| `make setup` / `make init-env ENV=<e>` | 0 | Create env dirs + default config files |
| `make enable-classification ENV=<e>` | 1/3 | Enable UC Data Classification + auto-tagging for the footprint (no ABAC/masking needed) |
| `make generate ENV=<e> GENERATE_ARGS='--groups "..."'` | 1/4 | Read native `class.*`, derive one `gr_treatment`/column, emit ABAC + masks (fail-closed) |
| `make coverage-gate ENV=<e>` | 1/4 | **Block** on any classified-but-unprotected column (static check of `generated/`) |
| `make validate-generated ENV=<e>` | 1 | Static validation incl. overlap guard (reject >1 mask/column) |
| `make apply ENV=<e>` | 1/5 | Full stack account → data_access → workspace (auto-promotes; creates Genie space; releases gated access) |
| `make apply-governance ENV=<e>` | 4 | account + data_access only (no Genie space) |
| `make promote SOURCE_ENV DEST_ENV DEST_CATALOG_MAP` | 2 | Promote **rules only** (strips tag assignments); **writes** prod `env.auto.tfvars` |
| `make verify-access ENV=<e> VERIFY_KEY_COLUMN=<pk>` | 1/4 | Effective access via per-tier test principals (LIVE; needs the `SELECT` grant released) |
| `make audit-rulebook ENV=<e>` | 4/6 | Drift vs rulebook — prod tags with no covering policy/mask |
| `make audit-schema ENV=<e>` / `make generate-delta ENV=<e>` | 6 | Untagged-column audit / incremental tag assignments after schema drift |
| `make evidence ENV=<e>` | 5 | Compliance evidence (`GENIERAILS_EVIDENCE_INTEGRATION=1` + `WAREHOUSE_ID` for live) |

Key config & code: [`treatment_config.json`](../../treatment_config.json) (the `gr_treatment` precedence map), [`sensitivity_source.py`](../../sensitivity_source.py) (native `class.*` source), [`treatment_derivation.py`](../../treatment_derivation.py) (one treatment/column), [`verify_effective_access.py`](../../verify_effective_access.py) (masked-vs-raw), [`scripts/audit_schema_drift.py`](../../scripts/audit_schema_drift.py) (drift).

---

## Limits you might hit

- **Classification scan latency** — initial scans are async (minutes to ~24h); no force-scan API. `generate` before tags land correctly fail-closes.
- **Governed tag-policy account cap** — each governed tag (`gr_treatment`, plus any source-family tags) is an account tag policy; large accounts can hit the cap (`make apply` reports it as a hard error). Free unused policies or raise the quota.
- **FGAC policy limits** — per catalog/schema/table/metastore; see [Troubleshooting](../../docs/troubleshooting.md).
- **Region-scoped classifiers** run in-region only — out-of-region PII physically present may go undetected (add a custom classifier or scan in-region).

---

## What this does — and does NOT — do

**It does:** discover the footprint, read native classification, derive one enforcement treatment per column, prove coverage with a blocking gate, verify masking by impersonation, and release Genie/data exposure only when the gate is green.

**It does not:** decide what's sensitive (Unity Catalog's classifier does); remove human review (generated rules are a reviewable draft); manage warehouse `CAN_USE` (you grant it); make you legally compliant (it proves coverage, not sign-off); or replace Unity Catalog (it runs on top of it).

---

## Glossary

- **`gr_treatment`** — the one GenieRails-owned governed tag whose value picks a column's mask.
- **facts vs rules** — *facts* = which columns got tagged (per workspace, from the scan); *rules* = the mapping + policies (portable, promoted).
- **footprint** — the exact tables the agent can reach (your `uc_tables` / Genie space tables).
- **fail-closed** — if native classification can't be read, `generate` aborts rather than guessing.
- **exposure gate** — `business_access_enabled`; releases `SELECT` + Genie `CAN_RUN` only when `true`.
- **AIM / SCIM** — how your IdP syncs groups into Databricks; GenieRails consumes those groups.
