# GenieRails Champion Flow — take a Genie agent from dev to production, safely

Take a curated Genie agent in **dev** and ship it to **production** without ever exposing sensitive data. Unity Catalog's built-in classifier decides *what* is sensitive; GenieRails derives *how* it's protected and applies it as code; and a **coverage check blocks the release** until every sensitive column the agent can reach is provably covered.

> **What you'll end up with:** a production Genie space where an authorized tier sees real values and every other tier sees masked ones — plus a proof that every sensitive column is covered and an audit/evidence record. Nothing is reachable by users until that coverage is proven.

> **Just want to run it?** → **[Quick start — every command in order](#quick-start--every-command-in-order)**.
> **No Genie agent or tables of your own yet?** → do **[Phase 0](#phase-0--set-up-dev)**, then the optional **[Sample Environment Setup](#sample-environment-setup-optional)**, then continue.

Terms in `code` (and words like *coverage check*, *masking*, *access tier*) are defined in the **[Glossary](#glossary)** at the bottom — skim it first if any term is unfamiliar.

---

## The idea in four sentences

1. **Unity Catalog decides what's sensitive.** Its built-in *Data Classification* scanner reads your data and puts a `class.*` label on each sensitive column (e.g. `class.email_address`).
2. **GenieRails decides how it's protected.** From those labels it derives one *masking* rule per column and the access rules, and applies them as Terraform.
3. **The coverage check is the safety gate.** It fails ("says NO") if any labelled-sensitive column has no protection — blocking the release until you fix it.
4. **Dev is the rehearsal; prod is the real thing.** You build and test in dev, promote the *rules* to prod, let prod scan its *own* data, prove coverage, and open the agent to users **last**.

---

## Quick start — every command in order

The whole flow as a checklist. Each line is a command to run, a one-time file edit, or a wait. Plain "what it does" is in parentheses; each **Phase below explains it in depth**. Run everything from the cloud root (`cd aws` or `cd azure`).

**Set up (dev)**
1. `cd aws` *(or `cd azure`)* — work from the cloud folder.
2. `make setup` *(prepares the project)* then `make init-env ENV=dev` *(creates the local `envs/dev/` config folder — no calls to Databricks)*.
3. `cp ../shared/examples/champion_flow/env.auto.tfvars.example envs/dev/env.auto.tfvars` *(seed the config)*, then edit `envs/dev/auth.auto.tfvars` (your service-principal login), `envs/dev/env.auto.tfvars` (your tables + settings), and set `manage_groups = false` in `envs/account/env.auto.tfvars` *(leave it `true` only if you'll create demo groups with `--create-groups`)*. *(No tables/agent of your own? Run the optional [Sample Environment Setup](#sample-environment-setup-optional) now — it creates a sample set and prints these values.)*

**Build & test in dev**
4. `make enable-classification ENV=dev` — turn on Databricks' scanner so it labels sensitive columns.
5. **Wait for the scan** (minutes to ~24h — a genuine *stop-and-resume-later* point), then confirm labels landed (SQL in [Phase 1](#phase-1--dev-scan-draft-the-rules-test-them)).
6. `make generate ENV=dev GENERATE_ARGS='--groups "<your IdP groups>"'` — GenieRails drafts the protection rules from the labels.
7. `make coverage-gate ENV=dev` — the safety check: fails if any sensitive column is unprotected.
8. `make validate-generated ENV=dev` — static sanity checks on the generated config.
9. *(Optional but recommended)* open `envs/dev/generated/` and review the drafted rules.
10. `make apply ENV=dev` — deploy the masks/policies. Users still can't see data (access stays withheld).
11. Edit `envs/dev/env.auto.tfvars`: `business_access_enabled = true` → `make apply ENV=dev` again → grant your tier groups `CAN_USE` on the dev warehouse (so `verify-access` can query), then `make verify-access ENV=dev VERIFY_KEY_COLUMN=<key>` — prove masking works (unprivileged sees masked, authorized sees raw). Then set `business_access_enabled = false` and `make apply ENV=dev` again to re-close dev.

**Promote & prove in prod**
12. `make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP="dev_finance=prod_finance"` — copy the *rules* to prod (not the data, not dev's labels). This **creates `envs/prod/` and writes `envs/prod/env.auto.tfvars`**.
13. Edit the prod files: fill `envs/prod/auth.auto.tfvars` (prod SP + workspace host/id), and in `envs/prod/env.auto.tfvars` set your prod `sql_warehouse_id` (or leave `""` to auto-create), `enable_classification = true`, `business_access_enabled = false`.
14. `make enable-classification ENV=prod` — scan prod's *own* real data.
15. **Wait for prod's scan**, then confirm labels (same SQL, prod catalog) — another stop-and-resume point.
16. `make generate ENV=prod GENERATE_ARGS='--groups "<same IdP groups>"'` → `make coverage-gate ENV=prod` → `make validate-generated ENV=prod` → `make apply-governance ENV=prod` → `make audit-rulebook ENV=prod` — re-derive from prod's tags, prove coverage (review `envs/prod/generated/`), deploy the enforcement (no agent yet), check for gaps.

**Open to users**
17. Edit `envs/prod/env.auto.tfvars`: `business_access_enabled = true` → `make apply ENV=prod` — creates the Genie space and releases access (business `SELECT` + Genie run).
18. Grant your tier groups `CAN_USE` on the SQL warehouse (Databricks UI/API — GenieRails doesn't manage warehouse permissions) so they (and `verify-access`'s test principals) can run queries. *(Auto-created warehouse? get its id, from the cloud root: `ENVS_DIR="$PWD/envs" ../shared/scripts/terraform_layer.sh workspace prod output -raw sql_warehouse_id`.)*
19. `make verify-access ENV=prod VERIFY_KEY_COLUMN=<key>` — confirm masked-vs-raw live (gate is open now), then `make evidence ENV=prod WAREHOUSE_ID=<id>` — capture the audit record.

**Keep it covered**
20. On a schedule: `make audit-schema ENV=prod`, `make audit-rulebook ENV=prod`, `make generate-delta ENV=prod` — catch sensitive data that arrives later.

> **Two things to know before you start:** (a) `verify-access` only works with the gate **open** (`business_access_enabled=true`) — that's why it comes *after* you flip the gate, in dev step 11 and prod step 19. (b) `make generate ENV=prod` (step 16) re-runs generation against prod's own tags — see [Phase 4](#phase-4--prove-coverage-the-gate) for what that does and doesn't preserve.

---

## Prerequisites & what to gather

**Tools:** the Databricks Terraform provider `~> 1.111.0` (auto-selected), **GNU Make**, Python 3, and Terraform on your `PATH`. *(On macOS, Apple's `/usr/bin/make` and Homebrew may be blocked by an unaccepted Xcode license — install GNU Make another way, e.g. `conda install make`, and put it first on `PATH`.)*

Gather these once — every phase reuses them:

| Value | What it is / where it comes from |
|---|---|
| **Deploying Service Principal** (`client_id` + `client_secret`) | The identity GenieRails runs as — **not** a CLI profile. Needs, on the **same account** as the workspace: **Account Admin** (groups, workspace assignment), **Workspace Admin** (Genie, warehouse), **Metastore Admin** (tags, fine-grained access control), **`EXECUTE` on `system.ai.databricks-claude-sonnet-4-6`** (generation calls a foundation model), and catalog **`APPLY TAG` + `ASSIGN`** (so the scanner can write `class.*` tags). Goes in `envs/<env>/auth.auto.tfvars`. See [Prerequisites](../../docs/prerequisites.md). |
| **Dev / prod catalog names** | Your Unity Catalog catalogs, e.g. `dev_finance` / `prod_finance`. |
| **SQL warehouse id** (per env) | An existing serverless warehouse id — **or leave blank** to auto-create one. |
| **Curated Genie space** | The agent itself. To deploy it (and get *agent access*), you **must** set a `genie_spaces` entry — an existing space id, or `genie_space_id=""` + `uc_tables` to create one. `genie_spaces = []` governs *data only* — no agent. |
| **IdP group names** (one per *access tier*) | Your existing groups, synced from your identity provider (Entra ID / Okta) via **AIM/SCIM**. GenieRails **consumes** them by name — it never creates them. e.g. `payments_ops,regional_analysts,viewers`. |
| **Shared key column** | One column present in **all** your tables (e.g. `customer_id`) — `verify-access` uses it to line up the same rows across tiers. |
| **UC Data Classification** | Available on the catalog; you turn it on per-env with `make enable-classification` (below). |

> **`verify-access` side effects:** it creates and then deletes temporary `genierails-verify-<tier>` service principals, adds them to your tier groups for the duration of the test, and needs **account-admin**; your IdP sync must tolerate a transient non-IdP group member.

> **Identity note:** GenieRails does **not** create groups — it consumes the ones your IdP already syncs in (`manage_groups = false`). If you have no tiered groups yet (e.g. just trying the demo), you can let it create demo groups with `--create-groups` instead of `--groups` (see [Phase 1](#phase-1--dev-scan-draft-the-rules-test-them)).

---

## Phase 0 — Set up (dev)

**What you're doing:** creating the local config folders and filling in your credentials + settings. Nothing here touches Databricks yet.

```bash
cd aws                      # or: cd azure
make setup                  # prepares the cloud root (pins the Terraform provider, etc.)
make init-env ENV=dev       # creates the local envs/dev/ folder + template config files (no Databricks calls)
cp ../shared/examples/champion_flow/env.auto.tfvars.example envs/dev/env.auto.tfvars
```

`make init-env` is purely local scaffolding — it creates `envs/dev/` and drops in default/template files for you to fill in, and never calls Databricks. The `cp` seeds `env.auto.tfvars` from the champion-flow example. Now edit three files:

- **`envs/dev/auth.auto.tfvars`** — the deploying SP `client_id` / `client_secret` + workspace host & id.
- **`envs/dev/env.auto.tfvars`** — `uc_tables`, `sql_warehouse_id` (or blank), `genie_spaces`, `enable_classification = true`, `business_access_enabled = false`.
- **`envs/account/env.auto.tfvars`** — set `manage_groups = false` (this flow *consumes* IdP groups; it doesn't create them). **There is one shared `envs/account/` config** used by both dev and prod — you edit it here, once.

**How you know it worked:** `ls envs/dev` shows `auth.auto.tfvars` and `env.auto.tfvars`, both filled in.

---

## Sample Environment Setup (Optional)

**Do [Phase 0](#phase-0--set-up-dev) first, then this, then continue to [Phase 1](#phase-1--dev-scan-draft-the-rules-test-them).** This is demo tooling for when you *don't* have your own tables or a Genie agent — skip it entirely if you do. It creates a sample schema (three tables of realistic synthetic PII) and a sample Genie Space, and prints the exact values to paste into `envs/dev/env.auto.tfvars`.

```bash
cd ../shared/examples/champion_flow         # from the cloud root (aws/ or azure/); return with 'cd ../../../aws' afterward
python -m pip install -r requirements.txt
python setup_sample_env.py --catalog dev_finance --warehouse-id <your-warehouse-id>
```

> **Auth for this helper is optional to specify.** It authenticates with a **Databricks CLI profile** — separate from the deploying Service Principal that `make` uses (that one lives in `auth.auto.tfvars`). With no flag it uses your **default** CLI profile (or `DATABRICKS_HOST`/`DATABRICKS_TOKEN` env vars); add `--profile <name>` only if you authenticate with a *named* profile.

The script prints the exact `uc_tables`, `genie_spaces`, and `sql_warehouse_id` snippet — paste it into `envs/dev/env.auto.tfvars`. It does **not** create any access-tier groups, so in Phase 1 either pass `--groups` with existing group names or use `--create-groups`. Re-runs are safe. To remove everything it created (only that — it uses a local ownership record):

```bash
python teardown_sample_env.py --catalog dev_finance
# Equivalent: add --teardown to the setup command.
```

Use `--help` for `--host`, `--schema`, `--rows`, and env-var alternatives. Then `cd ../../../aws` (or the equivalent azure path) and continue.

---

## Phase 1 — Dev: scan, draft the rules, test them

**Why dev:** not to discover what's sensitive (prod does that on real data) — but to *rehearse* safely: prove the masks fire, confirm the agent still answers, and produce a reviewable draft, off live PII.

**1a. Turn on the scanner.**
```bash
make enable-classification ENV=dev
```
This applies **only** the UC Data Classification + auto-tagging config for your tables — no masks, policies, or grants yet.

**1b. Wait for the scan, then check the labels landed.** The first scan is asynchronous — minutes to ~24h, with no way to force it (a real *stop-and-resume-later* point). Re-run this query (in a SQL editor / notebook, on any warehouse) until your recognizable PII columns show `class.*` tags:
```sql
SELECT table_name, column_name, tag_name
FROM system.information_schema.column_tags
WHERE catalog_name = '<your-catalog>' AND schema_name = '<your-schema>'
  AND tag_name LIKE 'class.%';
```
- ✅ **Landed → go to 1c.** You get one row per recognizable PII column, each with a `class.*` tag, e.g.:
  ```
  table_name   column_name          tag_name
  customers    email                class.email_address
  customers    ssn                  class.us_ssn
  payments     credit_card_number   class.credit_card
  notes        free_text            class.email_address
  ```
- ⏳ **Zero rows → wait and re-run.** It almost always just means the scan hasn't finished.
- ⚠️ **Don't expect every column.** The scanner only tags values it can *format-match* — free-text or unusual formats may stay untagged, and that's expected; the `coverage-gate` (1d) is what enforces completeness. If it stays empty after a clear scan, your data isn't format-matchable: seed **realistic** PII (the scanner ignores fake `example.com` emails / `000-` SSNs).

**1c. Draft the protection rules.**
```bash
make generate ENV=dev GENERATE_ARGS='--groups "payments_ops,regional_analysts,viewers"'
```
`--groups` are **your own** IdP-synced groups (Entra ID / Okta), **one per access tier**, most-privileged first (the example runs `payments_ops` = full → `regional_analysts` = masked → `viewers` = least) — `payments_ops,regional_analysts,viewers` are just placeholders; use your real group names. GenieRails *consumes* them by exact name (never creates them); a missing name stops generation with a clear error. **No tiered groups yet?** Use `--create-groups` instead (demo/greenfield only — it creates the groups; needs `manage_groups = true`). Generation reads the authoritative `class.*` tags and is **fail-closed**: if classification is on but unreadable/empty it aborts (opt into LLM inference with `--allow-llm-sensitivity`).

**1d. Prove coverage, apply, and verify.** First, the one knob you'll flip: **`business_access_enabled`** is the *exposure gate* — a `true`/`false` in `envs/<env>/env.auto.tfvars`. While `false` (default), GenieRails applies every mask/policy but **withholds** the business `SELECT` grant and the Genie run permission, so no one can reach the agent. Setting it `true` and re-applying **releases** that access.

```bash
make coverage-gate      ENV=dev   # PASS = every labelled-sensitive column has a mask; FAIL = it stops you here
make validate-generated ENV=dev   # static checks (e.g. no two masks collide on one column)
make apply              ENV=dev   # deploy masks/policies — access still withheld (gate closed)

# --- prove masking works: open the gate, verify, then close it again ---
# edit envs/dev/env.auto.tfvars:  business_access_enabled = true
make apply         ENV=dev
make verify-access ENV=dev VERIFY_KEY_COLUMN=customer_id   # queries AS each tier: unprivileged=masked, authorized=raw
# edit envs/dev/env.auto.tfvars:  business_access_enabled = false
make apply         ENV=dev                                 # re-close dev after the check
```

**What each command does:** `coverage-gate` is the safety check at the heart of the flow — it confirms every column the scanner labelled sensitive has a protection covering it, and **fails (non-zero exit) and stops you** if even one is uncovered ("the tool says NO"); it changes nothing. `validate-generated` runs static checks on the drafted config. `apply` deploys the masks/policies. `verify-access` proves it *by effect*: it queries as each tier's test principal and shows the unprivileged tier gets masked values while an authorized tier gets raw — this **needs the gate open** (that's why you flip it to `true` first), so in dev you open it just for the check and close it again.

> `verify-access` queries the warehouse as test principals in your tier groups — grant those groups `CAN_USE` on the (dev) warehouse first, or the queries fail (GenieRails doesn't manage warehouse permissions). Also confirm the agent still answers useful questions under masking. Dev's deliverable = **validated rules + a working agent** (not dev's column labels — those stay in dev).

---

## Phase 2 — Promote the rules to prod

**What you're doing:** copying the *rules* to production — never the dev data, and never which columns dev happened to label.

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP="dev_finance=prod_finance"
```

This **creates `envs/prod/` and writes `envs/prod/env.auto.tfvars`** (with the discovered `genie_spaces` + catalog-remapped `uc_tables`, and `sql_warehouse_id = ""`). It carries the rules — the mapping, masking functions, access/row-filter policies, and group→tier mapping — and **leaves dev's tag assignments behind** (which columns got labelled is a *fact* about dev's data; prod re-derives its own in Phase 3).

**How you know it worked:** open `envs/prod/env.auto.tfvars` — `uc_tables` now points at your prod catalog (`prod_finance`).

**Then edit `envs/prod/env.auto.tfvars`** (don't recreate it): replace `sql_warehouse_id = ""` with your prod warehouse id (or leave `""` to auto-create), and add `enable_classification = true` and `business_access_enabled = false`.

> You do **not** touch the account config again — `envs/account/` is shared and you already set `manage_groups = false` in Phase 0.

---

## Phase 3 — Prod: scan real data

**What you're doing:** letting production scan its *own* real data and label its own sensitive columns — this is where the true facts land, because real customer PII only exists in prod.

Promotion (Phase 2) already created `envs/prod/` with a template `auth.auto.tfvars` — just fill it in (prod SP `client_id`/`client_secret` + prod workspace host/id), then turn on prod's scanner:

```bash
# fill envs/prod/auth.auto.tfvars first (prod SP + workspace host/id), then:
make enable-classification ENV=prod   # same as step 1a, now on prod
```

`make enable-classification ENV=prod` turns on prod's scanner. **Wait for prod's scan** and confirm `class.*` tags on the prod catalog (the same SQL as 1b, with your prod catalog/schema) — another stop-and-resume point. Zero rows = the scan hasn't finished; wait and re-check.

---

## Phase 4 — Prove coverage (the gate)

**What you're doing:** deriving prod's protections from prod's own labels, proving coverage, and deploying the enforcement — but **not** the agent yet.

```bash
make generate         ENV=prod GENERATE_ARGS='--groups "payments_ops,regional_analysts,viewers"'   # reads prod's LIVE class.* tags
make coverage-gate    ENV=prod   # blocks on any labelled-but-unprotected column
make validate-generated ENV=prod # static checks on the prod-generated config
make apply-governance ENV=prod   # deploy enforcement ONLY (account + data_access) — no Genie space yet
make audit-rulebook   ENV=prod   # flags any prod tag with no covering rule (drift)
```

`--groups` are the **same** IdP groups you used in dev (your IdP syncs them into the prod workspace too). **What each command does:** `apply-governance` deploys the enforcement — groups, tag policies, masking functions, access/row-filter policies, grants — but **not** the workspace layer, so the Genie space isn't created yet (that's Phase 5, after the gate). `audit-rulebook` is a **drift check**: it reports any prod `class.*`/`gr_treatment` tag with **no covering policy or mask** (e.g. a rule dropped in promotion, or a prod-only type your mapping doesn't handle) — a clean run means every tag maps to a rule.

> **`verify-access` is not here** — it needs the exposure gate open, so it runs in Phase 5 after you release access. The masks are already applied by `apply-governance`, so opening the gate then verifying is safe.

> ⚠️ **What `make generate` in prod does — and doesn't — preserve.** It re-derives *facts* from prod's authoritative `class.*` tags (sensitivity findings + tag assignments), but it is a **fresh generation run**, not a byte-for-byte replay of dev's rules: the broader governance draft still involves the model, so prod's generated policies/functions **can differ** from the ones you reviewed in dev. That's why you re-run `coverage-gate` + `validate-generated` + `audit-rulebook` here, and should **review `envs/prod/generated/`** before applying. If prod surfaces a type your mapping doesn't cover, update `treatment_config.json`, re-`generate`, and re-gate. **Use `apply-governance` here, not `make apply`** — a full `apply` runs the workspace layer and would create the Genie space before the gate passes.

**How you know it worked:** `coverage-gate` exits PASS, `audit-rulebook` reports no uncovered tags.

---

## Phase 5 — Open to users (the gate releases access), then verify

**What you're doing:** only now — with coverage proven — releasing access, creating the agent, and confirming masking live.

```hcl
# envs/prod/env.auto.tfvars
business_access_enabled = true
```
```bash
make apply ENV=prod    # creates the Genie space + RELEASES the withheld business SELECT and Genie run access
```

Now grant your tier groups **`CAN_USE`** on the SQL warehouse (Databricks UI/API — GenieRails does not manage warehouse permissions) so they, and `verify-access`'s test principals, can actually run queries. If you auto-created the warehouse (`sql_warehouse_id=""`), get its id first — **run this from the cloud root** (`aws/` or `azure/`):

```bash
ENVS_DIR="$PWD/envs" ../shared/scripts/terraform_layer.sh workspace prod output -raw sql_warehouse_id
```

Then confirm masking live and capture the evidence record:

```bash
make verify-access ENV=prod VERIFY_KEY_COLUMN=customer_id   # unprivileged=masked, authorized=raw (the gate is open now)
GENIERAILS_EVIDENCE_INTEGRATION=1 GENIERAILS_EVIDENCE_APPROVED_BY="<you>" \
  make evidence ENV=prod WAREHOUSE_ID=<prod-warehouse-id>
```

**How you know it worked:** `verify-access` shows masked values for the unprivileged tier and raw for the authorized tier; business users can open the Genie space and get useful, masked answers.

---

## Phase 6 — Keep it covered

New sensitive data keeps arriving. Run these on a schedule (the repo ships a scheduled governance job):
```bash
make audit-schema   ENV=prod    # untagged sensitive columns + stale assignments
make audit-rulebook ENV=prod    # newly-detected tags with no covering rule → add a rule, re-derive
make generate-delta ENV=prod    # incremental tag assignments after ALTER TABLE ADD/DROP/RENAME
```
A newly-tagged column is a *masking* gap, not an access breach (Unity Catalog granted nothing you didn't ask for) — add/derive the rule. For your most sensitive data, prefer "locked down until proven safe" over "open until tagged."

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
| **Whether they can open/run the agent** | `workspace` | the Genie space, its run permissions, workspace assignment + entitlement |

(So the `data_access` Terraform layer covers both *access* and *masking*; the `workspace` layer is the agent itself.)

**One mask per column.** The single enforcement key is **`gr_treatment`** — GenieRails derives exactly **one** value per column from its `class.*` labels (strictest label wins; a free-text column with multiple labels escalates to full redaction), so Unity Catalog's "only one mask may apply per column" rule is never violated.

**Why prod keeps the classifier's tags.** The Terraform resource that records tag assignments carries `ignore_changes = all` — a standard Terraform *lifecycle* setting meaning "once these exist, don't change or delete them." That lets the **classifier own the `class.*` tags** in prod: when a scan writes a tag, Terraform leaves it alone instead of reverting it. The classifier owns the tags; GenieRails owns the rules.

---

## Command reference

| Command | Phase | What it does |
|---|---|---|
| `make setup` / `make init-env ENV=<e>` | 0 | Create local env dirs + default config files (no Databricks calls) |
| `make enable-classification ENV=<e>` | 1/3 | Turn on UC Data Classification + auto-tagging for your tables |
| `make generate ENV=<e> GENERATE_ARGS='--groups "..."'` | 1/4 | Read native `class.*`, derive one `gr_treatment`/column, draft masks + access rules (fail-closed) |
| `make coverage-gate ENV=<e>` | 1/4 | **Block** if any labelled-sensitive column has no mask (the "says NO" check) |
| `make validate-generated ENV=<e>` | 1/4 | Static validation incl. the one-mask-per-column guard |
| `make apply ENV=<e>` | 1/5 | Full stack (account → data_access → workspace; auto-promotes same-env first); creates the Genie space; releases gated access when `business_access_enabled=true` |
| `make apply-governance ENV=<e>` | 4 | Enforcement only (account + data_access); no Genie space |
| `make promote SOURCE_ENV DEST_ENV DEST_CATALOG_MAP` | 2 | Promote **rules only** (leaves tag assignments behind); creates + writes prod `env.auto.tfvars` |
| `make verify-access ENV=<e> VERIFY_KEY_COLUMN=<pk>` | 1/5 | Prove masking by querying as per-tier test principals (**needs the gate open**) |
| `make audit-rulebook ENV=<e>` | 4/6 | Drift check — tags with no covering rule |
| `make audit-schema ENV=<e>` / `make generate-delta ENV=<e>` | 6 | Untagged-column audit / incremental tag assignments after schema changes |
| `make evidence ENV=<e>` | 5 | Compliance evidence record (`GENIERAILS_EVIDENCE_INTEGRATION=1` + `WAREHOUSE_ID`) |

Key config & code: [`treatment_config.json`](../../treatment_config.json) (the `gr_treatment` precedence rules — shared across envs), [`sensitivity_source.py`](../../sensitivity_source.py) (native `class.*` source), [`treatment_derivation.py`](../../treatment_derivation.py) (one treatment/column), [`verify_effective_access.py`](../../verify_effective_access.py) (masked-vs-raw), [`scripts/audit_schema_drift.py`](../../scripts/audit_schema_drift.py) (drift).

---

## Limits you might hit

- **Scan latency** — the first scan is async (minutes to ~24h); no force-scan API. `generate` before tags land correctly fail-closes.
- **Governed tag-policy account cap** — each governed tag is an account tag policy; large accounts can hit the cap (`make apply` reports it as a hard error). Free unused policies or raise the quota.
- **Fine-grained access-control limits** — per catalog/schema/table/metastore; see [Troubleshooting](../../docs/troubleshooting.md).
- **Region-scoped classifiers** run in-region only — out-of-region PII physically present may go undetected (add a custom classifier or scan in-region).

---

## What this does — and does NOT — do

**It does:** discover the tables the agent can reach, read native classification, derive one protection per column, prove coverage with a blocking gate, verify masking by querying as real principals, and release the agent only when the gate is green.

**It does not:** decide what's sensitive (Unity Catalog's classifier does); remove human review (the generated rules are a draft you review); manage warehouse `CAN_USE` (you grant it); make you legally compliant (it proves coverage, not sign-off); or replace Unity Catalog (it runs on top of it).

---

## Glossary

- **access tier** — a group of users who should see data at the same level (e.g. full / masked / least). You map one IdP group to each tier.
- **ABAC (attribute-based access control)** — masks/filters that apply based on a column's *tag*, not its name — so a rule covers any column carrying that tag.
- **`CAN_RUN` / `CAN_USE`** — Databricks permissions: `CAN_RUN` lets a group open and run a Genie space (released by the exposure gate); `CAN_USE` lets a group run a SQL warehouse (you grant it yourself).
- **`class.*` tag** — a label Unity Catalog's classifier writes on a column it finds sensitive (e.g. `class.email_address`).
- **coverage gate** — `make coverage-gate`; the blocking check that fails if any labelled-sensitive column has no covering mask/policy. The "tool says NO" step.
- **drift** — a gap between what's tagged and what's protected; `audit-rulebook` reports it.
- **entitlement / workspace assignment** — what lets a group *into* a workspace at all (applied every apply; harmless without a data grant).
- **evidence** — the compliance record `make evidence` produces (what was scanned, tagged, protected, and approved).
- **exposure gate** — `business_access_enabled`; releases the `SELECT` grant + Genie run permission only when `true`.
- **facts vs rules** — *facts* = which columns got tagged in *this* workspace (from the scan); *rules* = the mapping + policies (portable, promoted).
- **fail-closed** — if native classification can't be read, `generate` aborts rather than guessing.
- **FGAC (fine-grained access control)** — Unity Catalog column masks + row filters.
- **footprint** — the exact tables the agent can reach (your `uc_tables` / Genie space tables).
- **Genie space / agent** — the Databricks Genie experience users query; "the agent."
- **`gr_treatment`** — the one GenieRails-owned tag whose value picks a column's mask.
- **grant chain** — `USE CATALOG → USE SCHEMA → SELECT`, the layered grants needed to read a table.
- **IdP (identity provider)** — Entra ID / Okta; **AIM / SCIM** are how it syncs groups into Databricks. GenieRails consumes those groups.
- **masking** — transforming a sensitive value for unauthorized tiers (e.g. card → `****-****-****-4464`) while authorized tiers see the raw value.
- **principal** — an identity a query runs as (a user, group, or service principal); `verify-access` uses temporary test principals per tier.
- **rulebook / rules** — the mapping `class.* → gr_treatment → mask` plus the access/row-filter policies (the portable, promoted part).
- **row filter** — a rule that limits *which rows* a tier can see (business logic; not every flow uses one).
