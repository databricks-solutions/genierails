# GenieRails Champion Flow — take a Genie agent from dev to production, safely

Take a curated Genie agent in **dev** and ship it to **production** without ever exposing sensitive data. Unity Catalog's built-in classifier decides *what* is sensitive; GenieRails derives *how* it's protected and applies it as code; and a **coverage check blocks the release** until every *classified* sensitive column the agent can reach is provably covered.

> **What you'll end up with:** a production Genie agent where an authorized tier sees real values and every other tier sees masked ones — plus a proof that every *classified* sensitive column is covered and an audit/evidence record. Nothing is reachable by users until you open the exposure gate — which you do only after coverage passes.

> **Just want to run it?** → **[Quick start — every command in order](#quick-start--every-command-in-order)**.
> **No Genie agent or tables of your own yet?** → do **[Phase 0](#phase-0--set-up-dev)**, then the optional **[Sample Environment Setup](#sample-environment-setup-optional)**, then continue.

Terms in `code` (and words like *coverage check*, *masking*, *access tier*) are defined in the **[Glossary](REFERENCE.md#glossary)** on the companion reference page — skim it first if any term is unfamiliar.

---

## The idea

1. **Unity Catalog decides what's sensitive.** Its built-in *Data Classification* scanner reads your data and puts a `class.*` label on each sensitive column (e.g. `class.email_address`).
2. **GenieRails decides how it's protected.** From those labels it derives one *masking* rule per column and the access rules, and applies them as Terraform.
3. **The coverage check is the safety gate.** It fails ("says NO") if any labelled-sensitive column has no protection — blocking the release until you fix it.
4. **Dev is the rehearsal; prod is the real thing.** You build and test in dev, promote the *rules* to prod, let prod scan its *own* data, prove coverage, and open the agent to users **last**.

---

## Quick start — every command in order

Every command in order — each step is a command to run, a one-time file edit, or a **wait**. The matching **Phase** below explains each in depth. Run everything from the cloud root (`cd aws` or `cd azure`).

**Phase 0 · Set up (dev)**
1. `cd aws` *(or `cd azure`)* — work from the cloud folder.
2. `make setup` — prepare the project.
3. `make init-env ENV=dev` — create the local `envs/dev/` config folder (no Databricks calls).
4. `cp ../shared/examples/champion_flow/env.auto.tfvars.example envs/dev/env.auto.tfvars` — seed the config.
5. Edit your config — `envs/dev/auth.auto.tfvars` (service-principal login), `envs/dev/env.auto.tfvars` (tables + settings), and set `manage_groups = false` in `envs/account/env.auto.tfvars`. *(No tables/agent of your own? Run the optional [Sample Environment Setup](#sample-environment-setup-optional) now — it creates a sample set and prints these values.)*

**Phase 1 · Dev — scan, draft the rules, test them**
1. `make enable-classification ENV=dev` — turn on Databricks' scanner to label sensitive columns.
2. **Wait for the scan** (minutes to ~24h — a genuine *stop-and-resume-later* point), then confirm labels landed (SQL in [Phase 1](#phase-1--dev-scan-draft-the-rules-test-them)).
3. `make generate ENV=dev GENERATE_ARGS='--groups "<your IdP groups>"'` — draft the protection rules from the labels.
4. `make coverage-gate ENV=dev` — safety check: fails if any sensitive column is unprotected.
5. `make validate-generated ENV=dev` — static sanity checks on the generated config.
6. *(Optional but recommended)* open `envs/dev/generated/` and review the drafted rules.
7. `make apply ENV=dev` — deploy the masks/policies. Users still can't see data (access stays withheld).
8. **Flip the gate to test masking:** set `business_access_enabled = true` in `envs/dev/env.auto.tfvars`, then `make apply ENV=dev`.
9. Grant your tier groups `CAN_USE` on the dev warehouse (so `verify-access` can query).
10. `make verify-access ENV=dev VERIFY_KEY_COLUMN=<key>` — prove masking works (unprivileged sees masked, authorized sees raw).
11. **Re-close dev:** set `business_access_enabled = false`, then `make apply ENV=dev`.

**Phase 2 · Promote the rules to prod**
1. `make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP="dev_finance=prod_finance"` — copy the *rules* to prod (not the data, not dev's labels). Creates `envs/prod/` and writes `envs/prod/env.auto.tfvars`.
2. Edit the prod files — `envs/prod/auth.auto.tfvars` (prod SP + workspace host/id); in `envs/prod/env.auto.tfvars` set `sql_warehouse_id` (or `""` to auto-create), `enable_classification = true`, `business_access_enabled = false`.

**Phase 3 · Prod — scan real data**
1. `make enable-classification ENV=prod` — scan prod's *own* real data.
2. **Wait for prod's scan**, then confirm labels (same SQL, prod catalog) — another stop-and-resume point.

**Phase 4 · Prove coverage (the gate)**
1. `make derive-assignments ENV=prod` — re-derive prod's tag assignments from prod's live tags, **reusing the promoted rules byte-for-byte** (no model call — masks/policies can't drift from dev).
2. `make coverage-gate ENV=prod` — prove coverage.
3. `make validate-generated ENV=prod` — static checks.
4. `make apply-governance ENV=prod` — deploy the enforcement (no Genie agent yet).
5. `make audit-rulebook ENV=prod` — check for gaps.

**Phase 5 · Open to users**
1. Set `business_access_enabled = true` in `envs/prod/env.auto.tfvars`, then `make apply ENV=prod` — creates the Genie agent and releases access (business `SELECT` + Genie run).
2. Grant your tier groups `CAN_USE` on the SQL warehouse (Databricks UI/API — GenieRails doesn't manage warehouse permissions). *(Auto-created warehouse? get its id from the cloud root: `ENVS_DIR="$PWD/envs" ../shared/scripts/terraform_layer.sh workspace prod output -raw sql_warehouse_id`.)*
3. `make verify-access ENV=prod VERIFY_KEY_COLUMN=<key>` — confirm masked-vs-raw live (gate is open now).
4. `make evidence ENV=prod WAREHOUSE_ID=<id>` — capture the audit record.

**Phase 6 · Keep it covered**
1. On a schedule: `make audit-schema ENV=prod`, `make audit-rulebook ENV=prod`, `make generate-delta ENV=prod` — catch sensitive data that arrives later.

> **Two things to know before you start:** (a) `verify-access` only works with the gate **open** (`business_access_enabled=true`) — that's why it runs *after* you flip the gate on, in both dev (Phase 1) and prod (Phase 5). (b) prod does **not** re-run `generate` — Phase 4 uses `make derive-assignments`, which re-derives only the `tag_assignments` from prod's live tags and keeps the promoted rules byte-for-byte (no model call). See [Phase 4](#phase-4--prove-coverage-the-gate).

---

## Prerequisites & what to gather

> **AWS or Azure?** This flow is cloud-neutral — run it from either `aws/` or `azure/`; all Terraform, scripts, and `make` targets are shared. **Azure users:** in each `envs/<env>/auth.auto.tfvars` you must set `databricks_account_host = "https://accounts.azuredatabricks.net"` and use your Azure-format workspace host (`https://adb-<id>.<n>.azuredatabricks.net`). The provider defaults to the **AWS** account host, so the account-layer steps (groups, tag policies) fail on Azure if you leave it unset. See [Azure prerequisites](../../azure/docs/azure-prerequisites.md). Everything else in this walkthrough is identical on both clouds.


**Tools:** the Databricks Terraform provider `~> 1.111.0` (auto-selected), **GNU Make**, Python 3, and Terraform on your `PATH`. *(On macOS, Apple's `/usr/bin/make` and Homebrew may be blocked by an unaccepted Xcode license — install GNU Make another way, e.g. `conda install make`, and put it first on `PATH`.)*

Gather these once — every phase reuses them:

| Value | What it is / where it comes from |
|---|---|
| **Deploying Service Principal** (`client_id` + `client_secret`) | The identity GenieRails runs as — **not** a CLI profile. Needs, on the **same account** as the workspace: **Account Admin** (groups, workspace assignment), **Workspace Admin** (Genie, warehouse), **Metastore Admin** (tags, fine-grained access control), **`EXECUTE` on `system.ai.databricks-claude-sonnet-4-6`** (generation calls a foundation model), and catalog **`APPLY TAG` + `ASSIGN`** (so the scanner can write `class.*` tags). Goes in `envs/<env>/auth.auto.tfvars`. See [Prerequisites](../../docs/prerequisites.md). |
| **Dev / prod catalog names** | Your Unity Catalog catalogs, e.g. `dev_finance` / `prod_finance`. |
| **SQL warehouse id** (per env) | An existing serverless warehouse id — **or leave blank** to auto-create one. |
| **Curated Genie agent** | The agent itself. To deploy it (and get *agent access*), you **must** set a `genie_spaces` entry — an existing space id, or `genie_space_id=""` + `uc_tables` to create one. `genie_spaces = []` governs *data only* — no agent. |
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

**Do [Phase 0](#phase-0--set-up-dev) first, then this, then continue to [Phase 1](#phase-1--dev-scan-draft-the-rules-test-them).** This is demo tooling for when you *don't* have your own tables or a Genie agent — skip it entirely if you do. It creates a sample schema (three tables of realistic synthetic PII) and a sample Genie agent, and prints the exact values to paste into `envs/dev/env.auto.tfvars`.

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
- ⏳ **Zero rows → wait and re-run.** It almost always just means the scan hasn't finished. *(On **Azure** the initial scan runs materially slower than on AWS — typically tens of minutes rather than a few — so give it more time before concluding it didn't run.)*
- ⚠️ **Don't expect every column.** The scanner only tags values it can *format-match* — free-text or unusual formats may stay untagged, and that's expected. The `coverage-gate` (1d) blocks on any *classified* column left unprotected, but it **cannot** gate a column the scanner never tagged (a documented fail-open) — which is why you keep the exposure gate closed and prefer a restrictive default for high-sensitivity data. If it stays empty after a clear scan, your data isn't format-matchable: seed **realistic** PII (the scanner ignores fake `example.com` emails / `000-` SSNs).

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
make derive-assignments ENV=prod   # re-derive tag_assignments from prod's LIVE class.* tags; REUSES the promoted rules unchanged (no model call)
make coverage-gate    ENV=prod   # blocks on any labelled-but-unprotected column
make validate-generated ENV=prod # static checks on the prod-generated config
make apply-governance ENV=prod   # deploy enforcement ONLY (account + data_access) — no Genie agent yet
make audit-rulebook   ENV=prod   # flags any prod tag with no covering rule (drift)
```

**What each command does:** `derive-assignments` reads prod's live `class.*` tags and re-derives exactly one `gr_treatment` per column, writing **only** the `tag_assignments` — it reuses the masks, policies, row filters, and group→tier mapping you promoted **unchanged** (no `--groups`, no model call). `apply-governance` deploys the enforcement — groups, tag policies, masking functions, access/row-filter policies, grants — but **not** the workspace layer, so the Genie agent isn't created yet (that's Phase 5, after the gate). `audit-rulebook` is a **drift check**: it reports any prod `class.*`/`gr_treatment` tag with **no covering policy or mask** — a clean run means every tag maps to a rule.

> **`verify-access` is not here** — it needs the exposure gate open, so it runs in Phase 5 after you release access. The masks are already applied by `apply-governance`, so opening the gate then verifying is safe.

> **Prod enforces the exact rules you reviewed in dev.** `derive-assignments` reuses the promoted `generated/abac.auto.tfvars` verbatim and rewrites **only** the `tag_assignments` from prod's live `class.*` tags — no model call, so the masks/policies/row-filters/groups cannot drift from dev. It is **fail-closed**: it aborts if prod's native tags are unreadable or empty, if a finding is unmapped, or if a derived treatment has no covering mask in the promoted rules. If prod genuinely surfaces a *new* sensitive type your mapping doesn't cover, that's a **rule change** — update `treatment_config.json` and re-promote from dev; don't hand-edit prod. **Use `apply-governance` here, not `make apply`** — a full `apply` runs the workspace layer and would create the Genie agent before the gate passes.

**How you know it worked:** `coverage-gate` exits PASS, `audit-rulebook` reports no uncovered tags.

---

## Phase 5 — Open to users (you release access after the gate passes), then verify

**What you're doing:** only now — with coverage proven — releasing access, creating the agent, and confirming masking live.

```hcl
# envs/prod/env.auto.tfvars
business_access_enabled = true
```
```bash
make apply ENV=prod    # creates the Genie agent + RELEASES the withheld business SELECT and Genie run access
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

**How you know it worked:** `verify-access` shows masked values for the unprivileged tier and raw for the authorized tier; business users can open the Genie agent and get useful, masked answers.

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

## Limits you might hit

- **Scan latency** — the first scan is async (minutes to ~24h); no force-scan API, and **Azure's initial scan is materially slower than AWS's** (tens of minutes vs. a few). `generate` before tags land correctly fail-closes.
- **Governed tag-policy account cap** — each governed tag is an account tag policy; large accounts can hit the cap (`make apply` reports it as a hard error). Free unused policies or raise the quota.
- **Fine-grained access-control limits** — per catalog/schema/table/metastore; see [Troubleshooting](../../docs/troubleshooting.md).
- **Region-scoped classifiers** run in-region only — out-of-region PII physically present may go undetected (add a custom classifier or scan in-region).

---

## What this does — and does NOT — do

**It does:** discover the tables the agent can reach, read native classification, derive one protection per column, prove coverage with a blocking gate, verify masking by querying as real principals, and release the agent — a deliberate step you take only after the coverage gate passes.

**It does not:** decide what's sensitive (Unity Catalog's classifier does); remove human review (the generated rules are a draft you review); manage warehouse `CAN_USE` (you grant it); make you legally compliant (it proves coverage, not sign-off); or replace Unity Catalog (it runs on top of it).

---

## Reference & glossary

Kept out of this walkthrough so it stays scannable — all in **[REFERENCE.md](REFERENCE.md)**:

- **[Command reference](REFERENCE.md#command-reference)** — every `make` target in one table.
- **[How it works (under the hood)](REFERENCE.md#how-it-works-under-the-hood)** — the exposure gate, the three governance layers, and one-mask-per-column, explained.
- **[Glossary](REFERENCE.md#glossary)** — every term used here (`gr_treatment`, `class.*`, coverage gate, ABAC, …).
