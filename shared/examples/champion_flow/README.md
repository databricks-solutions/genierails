# GenieRails Champion Flow — take a Genie agent from dev to production, safely

Take a curated Genie agent in **dev** and ship it to **production** without ever exposing sensitive data. Unity Catalog's built-in classifier decides *what* is sensitive; GenieRails derives *how* it's protected and applies it as code; and a **coverage check blocks the release** until every *classified* sensitive column the agent can reach is provably covered.

> **What you'll end up with:** a production Genie agent where users see only what their group is cleared to — the groups you authorize see real values, everyone else sees masked ones (e.g. your payments-ops group sees a full card number, while analysts see `****-****-****-1234`). These *access tiers* are simply your identity-provider groups mapped to access levels, which you set up in Phase 1. Plus a proof that every *classified* sensitive column is covered, and an audit/evidence record. Nothing is reachable by users until you open the exposure gate — which you do only after coverage passes.

> **Want the whole flow at a glance?** → **[The flow at a glance](#the-flow-at-a-glance)**; then work through the **Phases** below — each is self-contained, with the exact commands.
> **No Genie agent or tables of your own yet?** → do **[Phase 0](#phase-0--set-up-dev)**, then the optional **[Sample Environment Setup](#sample-environment-setup-optional)**, then continue.

Terms in `code` (and words like *coverage check*, *masking*, *access tier*) are defined in the **[Glossary](REFERENCE.md#glossary)** on the companion reference page — skim it first if any term is unfamiliar.

---

## The idea

**The platform classifies → GenieRails enforces → a coverage gate blocks the release until every classified column is covered → dev rehearses, prod is the real thing.** For the full mental model, see [**How it works**](../../../README.md#how-it-works) on the landing page. The [phases below](#the-flow-at-a-glance) are the step-by-step.

---

## The flow at a glance

The whole flow is a 7-phase map (each row links to its self-contained runbook below). Expand it for orientation, or just work through the **Phases**. Run everything from the cloud root (`cd aws` or `cd azure`).

<details>
<summary><strong>The 7-phase map</strong> — Phase · what happens · command · done-when</summary>

| Phase | What happens | Signature commands | Done when |
|---|---|---|---|
| **[0 · Set up (dev)](#phase-0--set-up-dev)** | create local config; fill in creds + settings (no Databricks calls) | `make setup` → `make init-env ENV=dev` → edit tfvars | `envs/dev/` config filled in |
| **[1 · Dev — scan, draft, test](#phase-1--dev-scan-draft-the-rules-test-them)** | scan dev, review detections, draft the rules, prove masking works | `make enable-classification` → review → opt into tags → `make generate` → `make rehearse` | gate PASS + masking proven in dev |
| **[2 · Promote to prod](#phase-2--promote-the-rules-to-prod)** | copy the *rules* to prod (not the data, not dev's labels) | `make promote …` | `envs/prod/` points at your prod catalog |
| **[3 · Prod — scan real data](#phase-3--prod-scan-real-data)** | prod scans its *own* data → review detections, then opt into tags | `make enable-classification ENV=prod` → review → set `enable_auto_tagging=true` → re-apply | prod `class.*` tags land |
| **[4 · Prove coverage (the gate)](#phase-4--prove-coverage-the-gate)** | re-derive prod facts, prove coverage, deploy enforcement (masks/policies) — the Genie agent isn't created until Phase 5 | `make certify` | gate PASS, no drift |
| **[5 · Open to users](#phase-5--open-to-users-you-release-access-after-the-gate-passes-then-verify)** | release access **last**, create the agent, verify live | edit tfvars (`business_access_enabled=true`) → `make apply` → `make verify-access` → `make evidence` | masked-vs-raw confirmed live |
| **[6 · Keep it covered](#phase-6--keep-it-covered)** | catch sensitive data that arrives later | *(scheduled)* `make audit-schema` · `make audit-rulebook` · `make generate-delta` | runs on a schedule |
</details>

**Two things that trip people up:** (a) `make verify-access` only works with the exposure gate **open** (`business_access_enabled = true`) — so it runs *after* you flip the gate on (dev Phase 1, prod Phase 5). (b) Prod does **not** re-run `make generate` — Phase 4 uses `make derive-assignments`, which re-derives only the `tag_assignments` from prod's live tags and keeps the promoted rules byte-for-byte (no model call).

---

## Prerequisites & what to gather

<details>
<summary><strong>AWS or Azure?</strong> Cloud-neutral — <strong>Azure needs one extra setting</strong> (the account host)</summary>

Run from either `aws/` or `azure/`; all Terraform, scripts, and `make` targets are shared. **Azure users:** in each `envs/<env>/auth.auto.tfvars` set `databricks_account_host = "https://accounts.azuredatabricks.net"` and use your Azure-format workspace host (`https://adb-<id>.<n>.azuredatabricks.net`) — the provider defaults to the AWS account host, so account-layer steps (groups, tag policies) fail on Azure if you leave it unset. See [Azure prerequisites](../../azure/docs/azure-prerequisites.md). Everything else is identical on both clouds.
</details>


**Tools:** GNU Make, Python 3, and Terraform on your `PATH` (`make setup` pins the Databricks provider for you). See [Prerequisites](../../docs/prerequisites.md) for versions and install help.

Gather these once — every phase reuses them:

| Value | What it is / where it comes from |
|---|---|
| **Deploying Service Principal** (`client_id` + `client_secret`) | The identity GenieRails runs as — **not** a CLI profile. Needs, on the **same account** as the workspace: **Account Admin** (groups, workspace assignment), **Workspace Admin** (Genie, warehouse), **Metastore Admin** (tags, fine-grained access control), **`EXECUTE` on `system.ai.databricks-claude-sonnet-4-6`** (generation calls a foundation model), and catalog **`APPLY TAG` + `ASSIGN`** (so the scanner can write `class.*` tags). Goes in `envs/<env>/auth.auto.tfvars`. See [Prerequisites](../../docs/prerequisites.md). |
| **Dev / prod catalog names** | Your Unity Catalog catalogs, e.g. `dev_finance` / `prod_finance`. |
| **SQL warehouse id** (per env) | The serverless warehouse the **Genie agent runs its SQL on** (and that `verify-access` uses to test masking). Give an existing warehouse's id, **or leave blank** to auto-create one. |
| **Curated Genie agent** | The agent itself. To deploy it (and get *agent access*), you **must** set a `genie_spaces` entry — an existing space id, or `genie_space_id=""` + `uc_tables` to create one. `genie_spaces = []` governs *data only* — no agent. |
| **IdP group names** (one per *access tier*) | Your existing groups, synced from your identity provider (Entra ID / Okta) via **AIM/SCIM**. GenieRails **consumes** them by name — it never creates them. e.g. `payments_ops,regional_analysts,viewers`. |
| **Row-pairing key** (`VERIFY_KEY_COLUMN`) | The column `verify-access` uses to line up the same rows across tiers for the **column-mask** checks — it must exist in the tables that *have* masks (e.g. `customer_id`), **not necessarily every table**. If your masked tables don't all share one column, `verify-access` reports the ones it couldn't pair as failures (the underlying script also accepts per-table keys via a `--spec` file). |
| **UC Data Classification** | Available on the catalog; you turn it on per-env with `make enable-classification` (below). Learn more in the Databricks docs: [Data Classification (AWS)](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification) / [(Azure)](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-classification). |

<details>
<summary><strong>More prerequisite notes</strong> — <code>verify-access</code> side effects & identity/groups</summary>

**`verify-access` side effects:** it creates and then deletes temporary `genierails-verify-<tier>` service principals, adds them to your tier groups for the duration of the test, and needs **account-admin**; your IdP sync must tolerate a transient non-IdP group member.

**Identity:** GenieRails does **not** create groups — it consumes the ones your IdP already syncs in (`manage_groups = false`). No tiered groups yet (e.g. just trying the demo)? Let it create demo groups with `--create-groups` instead of `--groups` (see [Phase 1](#phase-1--dev-scan-draft-the-rules-test-them)).
</details>

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
- **`envs/dev/env.auto.tfvars`** — `uc_tables`, `sql_warehouse_id` (or blank), `genie_spaces`, `enable_classification = true`, `enable_auto_tagging = false`, `business_access_enabled = false`.
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
This applies **only** the UC Data Classification config for your tables — no masks, policies, or grants yet. Auto-tagging follows `enable_auto_tagging` and defaults off.

**1b. Review detections, opt into tags, then confirm they landed.** The first scan is asynchronous — minutes to ~24h, with no way to force it (a real *stop-and-resume-later* point). Two easy steps, mostly in the Databricks UI:

- **Review (UI).** Open [Review detections](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification#review-detections) to see what the scanner found on your columns and **exclude any false positives**. Nothing is tagged yet — auto-tagging defaults off.
- **Opt in.** After review, set `enable_auto_tagging = true` in `envs/dev/env.auto.tfvars` and re-run `make enable-classification ENV=dev`. The `class.*` tags then land on the reviewed columns (again, minutes to ~24h — on **Azure** the initial scan runs materially slower than on AWS, tens of minutes rather than a few). You can see the applied tags on each column in **Catalog Explorer** — then go to **1c**.

<details>
<summary>⚠️ <strong>Don't expect every column tagged</strong> — the scanner only matches recognizable formats, and the gate can't catch what was never tagged</summary>

The scanner only tags values it can *format-match* — free-text or unusual formats may stay untagged, and that's expected. The `coverage-gate` (1d) blocks on any *classified* column left unprotected, but it **cannot** gate a column the scanner never tagged (a documented fail-open) — which is why you keep the exposure gate closed and prefer a restrictive default for high-sensitivity data. If nothing gets tagged after a clear scan, your data isn't format-matchable: seed **realistic** PII (the scanner ignores fake `example.com` emails / `000-` SSNs).
</details>

<details>
<summary><strong>Prefer to verify with SQL?</strong> (reads the exact source the coverage gate uses)</summary>

Re-run this until your recognizable PII columns show `class.*` tags. It reads `system.information_schema.column_tags` — the same source `make coverage-gate` reads — so rows here mean the gate will see them:
```sql
SELECT table_name, column_name, tag_name
FROM system.information_schema.column_tags
WHERE catalog_name = '<your-catalog>' AND schema_name = '<your-schema>'
  AND tag_name LIKE 'class.%';
```
One row per recognizable PII column, e.g.:
```
table_name   column_name          tag_name
customers    email                class.email_address
customers    ssn                  class.us_ssn
payments     credit_card_number   class.credit_card
notes        free_text            class.email_address
```
Zero rows usually just means the scan hasn't finished — wait and re-run.
</details>

**1c. Draft the protection rules.**
```bash
make generate ENV=dev GENERATE_ARGS='--groups "payments_ops,regional_analysts,viewers"'
```
`--groups` are **your own** IdP-synced groups, **one per access tier, most-privileged first** (`payments_ops`=full → `regional_analysts`=masked → `viewers`=least — placeholders; use your real names). GenieRails *consumes* them by exact name, never creates them.

<details>
<summary>No tiered groups yet, or want the fail-closed / LLM-fallback details?</summary>

A missing group name stops generation with a clear error. **No tiered groups yet?** Use [`--create-groups`](../../docs/advanced.md#opt-in-group-creation-demo--greenfield-only) instead (demo/greenfield only — it creates the groups; needs `manage_groups = true`). Generation reads the authoritative `class.*` tags and is **fail-closed** — if classification is on but the results are unreadable or empty, it **aborts rather than silently guessing**. (You can opt into LLM inference with [`--allow-llm-sensitivity`](../../docs/troubleshooting.md#champion-flow-issues) — not recommended in prod.)
</details>

**1d. Prove coverage, apply, and verify.** First, the one knob you'll flip: **`business_access_enabled`** is the *exposure gate* — a `true`/`false` in `envs/<env>/env.auto.tfvars`. While `false` (default), GenieRails applies every mask/policy but **withholds** the business `SELECT` grant and the Genie run permission, so no one can reach the agent. Setting it `true` and re-applying **releases** that access.

```bash
# dev is a rehearsal: open the gate once, prove masking works, and leave it open — the masks protect the data either way
# edit envs/dev/env.auto.tfvars:  business_access_enabled = true

make rehearse ENV=dev VERIFY_KEY_COLUMN=customer_id   # one command: coverage-gate → validate-generated → apply → verify-access (stops at the first failure)
```

`make rehearse` runs those four steps in order and **stops at the first failure**. **`VERIFY_KEY_COLUMN` is optional but recommended** — omit it (`make rehearse ENV=dev`) and it still gates → validates → applies, then *skips* the live masking check with a reminder to pass a key column.

<details>
<summary><strong>Explicit stages + the <code>verify-access</code> warehouse prerequisite</strong> (run stages individually — e.g. for CI, where <code>coverage-gate</code> must be its own blocking step)</summary>

```bash
make coverage-gate      ENV=dev                            # blocks unless every classified column has a mask ("says NO")
make validate-generated ENV=dev                            # static checks (e.g. no two masks collide on one column)
make apply              ENV=dev                            # deploy masks/policies + open access
make verify-access      ENV=dev VERIFY_KEY_COLUMN=customer_id   # queries AS each tier: unprivileged=masked, authorized=raw
```

**What each command does:** `coverage-gate` is the safety check at the heart of the flow — it confirms every column the scanner labelled sensitive has a protection covering it, and **fails (non-zero exit) and stops you** if even one is uncovered ("the tool says NO"); it changes nothing. `validate-generated` runs static checks on the drafted config. `apply` deploys the masks/policies (and, with the gate set to `true`, opens access in the same step). `verify-access` proves it *by effect*: it queries as each tier's test principal and shows the unprivileged tier gets masked values while an authorized tier gets raw — this **needs the gate open**, so in dev you open the gate once and leave it — it's a rehearsal, and the masks protect the data regardless. (Prod is stricter: Phase 4 deploys governance with the gate **closed**, and Phase 5 opens it only after the gate passes — that's the real "expose last".)

**Warehouse prerequisite — applies to `rehearse` too.** `verify-access` queries the warehouse as test principals in your tier groups — grant those groups `CAN_USE` on the (dev) warehouse first, or the queries fail (GenieRails doesn't manage warehouse permissions). Also confirm the agent still answers useful questions under masking. Dev's deliverable = **validated rules + a working agent** (not dev's column labels — those stay in dev).
</details>

---

## Phase 2 — Promote the rules to prod

**What you're doing:** copying the *rules* to production — never the dev data, and never which columns dev happened to label.

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP="dev_finance=prod_finance"
```

This **creates `envs/prod/` and writes `envs/prod/env.auto.tfvars`** (with the discovered `genie_spaces` + catalog-remapped `uc_tables`, and `sql_warehouse_id = ""`). It carries the rules — the mapping, masking functions, access/row-filter policies, and group→tier mapping — and **leaves dev's tag assignments behind** (which columns got labelled is a *fact* about dev's data; prod re-derives its own in Phase 3).

**How you know it worked:** open `envs/prod/env.auto.tfvars` — `uc_tables` now points at your prod catalog (`prod_finance`).

**Then edit `envs/prod/env.auto.tfvars`** (don't recreate it): replace `sql_warehouse_id = ""` with your prod warehouse id (or leave `""` to auto-create), and add `enable_classification = true`, `enable_auto_tagging = false`, and `business_access_enabled = false`.

> You do **not** touch the account config again — `envs/account/` is shared and you already set `manage_groups = false` in Phase 0.

---

## Phase 3 — Prod: scan real data

**What you're doing:** letting production scan its *own* real data and label its own sensitive columns — this is where the true facts land, because real customer PII only exists in prod.

Promotion (Phase 2) already created `envs/prod/` with a template `auth.auto.tfvars` — just fill it in (prod SP `client_id`/`client_secret` + prod workspace host/id), then turn on prod's scanner:

```bash
# fill envs/prod/auth.auto.tfvars first (prod SP + workspace host/id), then:
make enable-classification ENV=prod   # same as step 1a, now on prod
```

`make enable-classification ENV=prod` turns on prod's scanner **without writing tags** (auto-tagging defaults off — promotion doesn't carry it over, so prod gets its *own* review, just like dev). Then, exactly as in dev's **1b**:

- **Review (UI).** Open [Review detections](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification#review-detections) on the prod catalog and **exclude any false positives** — prod's real data may surface sensitive types dev never saw.
- **Opt in.** Set `enable_auto_tagging = true` in `envs/prod/env.auto.tfvars` (add the line if promote didn't write it) and re-run `make enable-classification ENV=prod`. The `class.*` tags then land.

**Wait for prod's tags** and confirm them on the prod catalog (the same SQL as 1b, with your prod catalog/schema) — another stop-and-resume point. Zero rows means tags haven't landed; wait and re-check.

---

## Phase 4 — Prove coverage (the gate)

**What you're doing:** deriving prod's protections from prod's own labels, proving coverage, and deploying the enforcement — but **not** the agent yet.

```bash
make certify ENV=prod   # one command: derive-assignments → coverage-gate → validate-generated → apply-governance → audit-rulebook (stops at the first failure)
```

`make certify` re-derives prod's `tag_assignments` from its **live** `class.*` tags and **reuses the rules you reviewed in dev unchanged** — no model call, so the masks/policies/row-filters/groups can't drift — then proves coverage, deploys **enforcement only** (no Genie agent — that's Phase 5), and drift-checks. It is **fail-closed**: it aborts if prod's native tags are unreadable/empty, a finding is unmapped, or a derived treatment has no covering mask in the promoted rules. (If prod surfaces a *new* sensitive type your mapping doesn't cover, that's a rule change — see below.)

<details>
<summary><strong>Run the stages individually</strong> (e.g. for CI, where <code>coverage-gate</code> must be its own blocking step)</summary>

```bash
make derive-assignments ENV=prod   # re-derive tag_assignments from prod's LIVE class.* tags; REUSES the promoted rules unchanged (no model call)
make coverage-gate    ENV=prod   # blocks on any labelled-but-unprotected column
make validate-generated ENV=prod # static checks on the prod-generated config
make apply-governance ENV=prod   # deploy enforcement ONLY (account + data_access) — no Genie agent yet
make audit-rulebook   ENV=prod   # flags any prod tag with no covering rule (drift)
```

**What each command does:** `derive-assignments` reads prod's live `class.*` tags and re-derives exactly one `gr_treatment` per column, writing **only** the `tag_assignments` — it reuses the masks, policies, row filters, and group→tier mapping you promoted **unchanged** (no `--groups`, no model call). `apply-governance` deploys the enforcement — groups, tag policies, masking functions, access/row-filter policies, grants — but **not** the workspace layer, so the Genie agent isn't created yet (that's Phase 5, after the gate). `audit-rulebook` is a **drift check**: it reports any prod `class.*`/`gr_treatment` tag with **no covering policy or mask** — a clean run means every tag maps to a rule.

**`verify-access` is not here** — it needs the exposure gate open, so it runs in Phase 5 after you release access (the masks are already applied, so opening the gate then verifying is safe). And **use `apply-governance`, not `make apply`** — a full `apply` runs the workspace layer and would create the Genie agent before the gate passes (`make certify` already uses `apply-governance`).
</details>

**How you know it worked:** `coverage-gate` exits PASS, `audit-rulebook` reports no uncovered tags.

**If the gate fails or `audit-rulebook` reports drift** — prod surfaced a sensitive tag your promoted rules don't cover (a type the classifier found only in prod, or a rule dropped in promotion). This is a **rule change — made in dev, never hand-edited in prod**. Loop back:

1. **Scaffold the missing mappings** (instead of hand-editing JSON): `make scaffold-treatments ENV=prod` reads the labels prod surfaced and adds, for each, a **fail-safe full-redaction** `gr_treatment` + mask **stub** to the shared `treatment_config.json`, every entry marked `REVIEW`. Then **review each stub** — keep the redaction, or implement a type-appropriate mask. (You can still edit `treatment_config.json` by hand.) This changes the shared *rulebook*, not prod's live state — you still validate it in dev and re-promote below.
2. **Re-validate in dev:** `make generate ENV=dev` → `make coverage-gate ENV=dev`.
3. **Re-promote:** `make promote …` (carries the updated rules to prod — same command as [Phase 2](#phase-2--promote-the-rules-to-prod)).
4. **Re-run this phase:** `make certify ENV=prod`.

Repeat until the gate passes and drift is clean. The agent stays uncreated and closed to users throughout — that's the point of exposing last.

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
