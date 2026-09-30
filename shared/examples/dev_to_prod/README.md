# GenieRails Dev-to-Prod Walkthrough — ship a Genie agent to production, safely

Take a curated Genie agent in **dev** and ship it to **production** without ever exposing sensitive data. Unity Catalog's built-in classifier decides *what* is sensitive; GenieRails derives *how* it's protected and applies it as code; and a **coverage check blocks the release** until every *classified* sensitive column the agent can reach is provably covered.

---

## The idea

Databricks' built-in classifier samples your column values *inside your own workspace* and tags which columns hold sensitive types (emails, SSNs, card numbers) — it records only the tags, never the values. GenieRails turns those tags into protections — column masks and access rules — as code, and a **coverage check refuses to release the agent** until every sensitive column is covered. You rehearse the whole thing safely in **dev**, then do it for real in **prod**. (Full mental model: [How it works](../../../README.md#how-it-works).)

---

## Prerequisites & what to gather

Gather these once — every phase reuses them:

| Value | What it is / where it comes from |
|---|---|
| **Deploying Service Principal** (`client_id` + `client_secret`) | The identity GenieRails runs as (not a CLI profile), on the **same account** as the workspace: **Account Admin**, **Workspace Admin**, and **authority over your existing catalog** (own it, or `MANAGE` + `APPLY TAG`), plus permission to **query the `databricks-claude-sonnet-4-6` serving endpoint** (generation calls a foundation model — an Anthropic/OpenAI provider works too). **Create it manually, or point `make bootstrap-sp` at your catalog** (`TARGET_CATALOG=<catalog>`) — see [Prerequisites](../../docs/prerequisites.md) for exact roles. |
| **Dev / prod catalog names** | Your Unity Catalog catalogs, e.g. `dev_finance` / `prod_finance`. |
| **SQL warehouse id** (per env) | The serverless warehouse the **Genie agent runs its SQL on**. Give an existing warehouse's id, **or leave blank** to auto-create one. |
| **Curated Genie agent** | The Genie agent you’re shipping — set a `genie_spaces` entry with its existing space id (from the Genie UI URL). |
| **IdP group names** (one per *access tier*) | Your existing groups, synced from your identity provider (Entra ID / Okta) via **AIM/SCIM**. GenieRails **consumes** them by name — it never creates them. e.g. `payments_ops,regional_analysts,viewers`. |
| **Row-pairing key** (`VERIFY_KEY_COLUMN`) | One column `verify-access` uses to pair the same rows across tiers for the mask checks (e.g. `customer_id`); it just needs to exist on the masked tables. Tables that don't share one column → use a per-table `VERIFY_SPEC` JSON instead ([details](../../docs/effective-access-verification.md)). |
| **UC Data Classification** | Turn it on per-env — in the **Databricks UI** (Catalog Explorer → your catalog → enable classification) or reproducibly with `make enable-classification` (Phase 1). Docs: [AWS](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification) / [Azure](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-classification). |
| **Serverless usage policy** (new serverless workspaces) | On a workspace with no serverless usage (budget) policy, enabling classification can fail with `Usage policy ID must not be empty` — a known provider issue ([terraform-provider-databricks#5985](https://github.com/databricks/terraform-provider-databricks/issues/5985)). Create/attach a **serverless usage policy** first; per Databricks docs, **creating one requires Workspace Admin** (non-admins need the *Serverless usage policy: Manager* permission). See serverless usage policies — [AWS](https://docs.databricks.com/aws/en/admin/usage/budget-policies) / [Azure](https://learn.microsoft.com/en-us/azure/databricks/admin/usage/budget-policies). |

---

## Phase 0 — Set up (dev)

**Goal —** create the local config folders and fill in your creds + settings. Nothing here touches Databricks yet.

```bash
cd aws                      # or: cd azure
make setup                  # prepares the cloud root (pins the Terraform provider, etc.)
make init-env ENV=dev       # creates the local envs/dev/ folder + template config files (no Databricks calls)
cp ../shared/examples/dev_to_prod/env.auto.tfvars.example envs/dev/env.auto.tfvars
```

Then edit three files:

- **`envs/dev/auth.auto.tfvars`** — the deploying SP `client_id` / `client_secret` + workspace host & id.
- **`envs/dev/env.auto.tfvars`** — `uc_tables`, `sql_warehouse_id` (or blank), `genie_spaces`, `enable_classification = true`, `enable_auto_tagging = false`, `business_access_enabled = false`.
- **`envs/account/env.auto.tfvars`** — set `manage_groups = false` (this flow *consumes* IdP groups; it doesn't create them). **There is one shared `envs/account/` config** used by both dev and prod — you edit it here, once.

> **Where do `genie_spaces` / `uc_tables` come from?** Already built the agent in the Databricks UI → import it into code first: [From UI to Production](../../docs/from-ui-to-production.md) captures the agent *and* its tables. No agent or tables of your own yet → use the optional [Sample Environment Setup](SAMPLE_ENV.md) below. Either path hands you the exact values to paste above.

**Done when —** `ls envs/dev` shows `auth.auto.tfvars` and `env.auto.tfvars`, both filled in.

---

## Sample environment (optional)

No tables or Genie agent of your own? A one-command script creates a sample schema (realistic synthetic PII) + a sample agent and prints the exact `uc_tables` / `genie_spaces` / `sql_warehouse_id` to paste into `envs/dev/env.auto.tfvars`. **[→ Sample Environment Setup](SAMPLE_ENV.md)** — skip it if you have your own.

---

## Phase 1 — Dev: scan, draft the rules, test them

**Goal —** *rehearse* safely on dev: prove the masks fire, confirm the agent still answers, and produce a reviewable draft — off live PII. (Prod discovers what's actually sensitive later.)

**1a. Turn on the scanner.** Enable UC Data Classification on your catalog — easiest in the **Databricks UI** (Catalog Explorer → your catalog → *Enable* classification), or reproducibly as code:
```bash
make enable-classification ENV=dev   # scans only — nothing is tagged until you opt in (1b)
```
Either way, keep `enable_classification = true` in your tfvars — it's the *fail-closed signal* that makes the next step abort rather than guess if classification results aren't readable.

**1b. Review detections, opt into tags, then confirm they landed.** The first scan is asynchronous (minutes to ~24h) — kick it off, grab a coffee ☕, and come back. Two easy steps, mostly in the Databricks UI:

- **Review (UI).** Open [Review detections](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification#review-detections) to see what the scanner found on your columns and **exclude any false positives**. Nothing is tagged yet — auto-tagging defaults off.
- **Opt in.** After review, enable automatic tagging — in the UI, or set `enable_auto_tagging = true` in `envs/dev/env.auto.tfvars` and re-run `make enable-classification ENV=dev`. The `class.*` tags then land on the reviewed columns (visible in **Catalog Explorer**). Then continue to **1c**.

**1c. Draft the protection rules.**
```bash
make generate ENV=dev GENERATE_ARGS='--groups "payments_ops,regional_analysts,viewers"'
```
`--groups` are **your own** IdP-synced groups, **one per access tier, most-privileged first** (`payments_ops`=full → `regional_analysts`=masked → `viewers`=least — placeholders; use your real names). GenieRails *consumes* them by exact name, never creates them.

**1d. Prove coverage, apply, and verify — one command.**
```bash
make rehearse ENV=dev VERIFY_KEY_COLUMN=customer_id
```
`make rehearse` runs **coverage-gate → validate-generated → apply → verify-access** in order, stopping at the first failure. The last step, `verify-access`, proves masking *by effect*: it queries the data **as each access tier** and confirms the unprivileged tier sees masked values while an authorized tier sees raw. In dev you **don't touch the exposure gate** — rehearse opens it just for this check (the masks protect the data either way; prod opens it deliberately in Phase 5). `VERIFY_KEY_COLUMN` is the single column used to pair rows across tiers — optional (omit it and the masking check is skipped) but recommended; for tables that don't share one key, pass a [`VERIFY_SPEC` JSON](../../docs/effective-access-verification.md) instead.

> **One prerequisite:** grant your tier groups `CAN_USE` on the dev SQL warehouse first — `verify-access` runs its queries *as* those groups' principals, so without it they can't execute (GenieRails doesn't manage warehouse permissions).

---

## Phase 2 — Promote the rules to prod

**Goal —** copy the *rules* (masks, access policies, mappings) to production.

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP="dev_finance=prod_finance"
```

`DEST_CATALOG_MAP` renames each dev catalog to its prod name (`dev_finance=prod_finance`; comma-separate multiple).

**Done when —** `envs/prod/` now exists, pointing at your prod catalog.

**Then edit `envs/prod/env.auto.tfvars`** (don't recreate it): set `sql_warehouse_id` (or leave `""` to auto-create), and add `enable_classification = true`, `enable_auto_tagging = false`, `business_access_enabled = false`.

<details>
<summary><strong>What promote carries vs. leaves behind</strong> (rules travel, facts don't)</summary>

It carries the **rules** — the mapping, masking functions, access/row-filter policies, and group→tier mapping — and **leaves dev's tag assignments behind** (which columns got tagged is a *fact* about dev's data; prod re-derives its own in Phase 3).
</details>

---

## Phase 3 — Prod: scan real data

**Goal —** let production scan its *own* real data and tag its sensitive columns — the true facts land here (real customer PII only exists in prod).

Promotion (Phase 2) already created `envs/prod/` with a template `auth.auto.tfvars` — just fill it in (prod SP `client_id`/`client_secret` + prod workspace host/id), then turn on prod's scanner — in the **Databricks UI** on the prod catalog, or as code:

```bash
# fill envs/prod/auth.auto.tfvars first (prod SP + workspace host/id), then:
make enable-classification ENV=prod   # same as step 1a, now on prod
```

`make enable-classification ENV=prod` turns on prod's scanner **without writing tags** (auto-tagging defaults off, so prod gets its *own* review, just like dev). Then, exactly as in dev's **1b**:

- **Review (UI).** Open [Review detections](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification#review-detections) on the prod catalog and **exclude any false positives** — prod's real data may surface sensitive types dev never saw.
- **Opt in.** Set `enable_auto_tagging = true` in `envs/prod/env.auto.tfvars` (add the line if promote didn't write it) and re-run `make enable-classification ENV=prod`. The `class.*` tags then land.

**Done when —** prod's `class.*` tags appear on the prod catalog (check in Catalog Explorer / Review detections). The scan is async — grab a coffee ☕ and re-check; nothing yet just means it hasn't finished.

---

## Phase 4 — Prove coverage (the gate)

**Goal —** derive prod's protections from its own `class.*` tags, prove coverage, and deploy the enforcement (masks + access policies). The Genie agent itself isn't created yet — that's Phase 5.

```bash
make certify ENV=prod   # one command: derive-assignments → coverage-gate → validate-generated → apply-governance → audit-rulebook (stops at the first failure)
```

`certify` reuses the exact rules you reviewed in dev — it re-derives *which prod columns* get which protection from prod's own tags, but never regenerates the rules (no model call, so nothing drifts from what you reviewed). It proves coverage and deploys the masks + access policies, but **not** the agent. It's **fail-closed**: it stops if prod's tags can't be read, or a detected type has no rule covering it.

**Done when —** `coverage-gate` exits PASS and `audit-rulebook` reports no uncovered tags.

**If the gate fails or `audit-rulebook` reports drift** — prod surfaced a sensitive tag your promoted rules don't cover (a type the classifier found only in prod, or a rule dropped in promotion). This is a **rule change — made in dev, never hand-edited in prod**. Loop back:

1. **Scaffold the missing mappings** — `make scaffold-treatments ENV=prod` adds a **safe default** (full redaction, marked `REVIEW`) for each tag prod surfaced, so you don't hand-edit anything. Then **review each** — keep the redaction, or set a type-appropriate mask. This changes the shared *rulebook* (not prod's live state), so you validate it in dev and re-promote below.
2. **Re-validate in dev:** `make generate ENV=dev` → `make coverage-gate ENV=dev`.
3. **Re-promote:** `make promote …` (carries the updated rules to prod — same command as [Phase 2](#phase-2--promote-the-rules-to-prod)).
4. **Re-run this phase:** `make certify ENV=prod`.

Repeat until the gate passes and drift is clean. The agent stays uncreated and closed to users throughout — that's the point of exposing last.

---

## Phase 5 — Open to users (you release access after the gate passes), then verify

**Goal —** with coverage proven, release access, create the agent, and confirm masking live.

```hcl
# envs/prod/env.auto.tfvars
business_access_enabled = true
```
```bash
make apply ENV=prod    # creates the Genie agent + RELEASES the withheld business SELECT and Genie run access
```

Grant your tier groups **`CAN_USE`** on the prod SQL warehouse (Databricks UI/API — GenieRails doesn't manage warehouse permissions), so `verify-access` can query *as* them. Then confirm masking live and capture the evidence report:

```bash
make verify-access ENV=prod VERIFY_KEY_COLUMN=customer_id   # unprivileged = masked, authorized = raw (the gate is open now)
make evidence      ENV=prod                                 # writes a compliance report (JSON + Markdown) to envs/prod/generated/evidence/
```

> Auto-created the warehouse (`sql_warehouse_id=""`)? Get its id from the cloud root: `ENVS_DIR="$PWD/envs" ../shared/scripts/terraform_layer.sh workspace prod output -raw sql_warehouse_id`.
> For a **live, approver-signed** compliance snapshot (queries the deployed masks/grants rather than the config), run: `GENIERAILS_EVIDENCE_INTEGRATION=1 GENIERAILS_EVIDENCE_APPROVED_BY="<you>" make evidence ENV=prod WAREHOUSE_ID=<id>`.

**Done when —** `verify-access` shows masked values for the unprivileged tier and raw for the authorized tier; business users can open the Genie agent and get useful, masked answers.

---

## Phase 6 — Keep it covered

**Goal —** catch sensitive data that arrives after go-live. Run these on a schedule (the repo ships a scheduled governance job):

- **`make audit-schema ENV=prod`** — flags untagged sensitive-looking columns and stale assignments. A clean run prints `No drift detected.`; a finding lists the columns → classify them (usually via `generate-delta`) and re-apply.
- **`make audit-rulebook ENV=prod`** — flags any prod tag with no covering policy/mask (a rule dropped in promotion, or a brand-new type). A finding means: add the rule in dev, re-generate/validate, re-promote, and re-certify.
- **`make generate-delta ENV=prod`** — *mutating*: removes stale assignments and assigns newly-detected sensitive columns, **constrained to your existing governed tags** (it can't invent a new type). Review the merged assignments, then `make apply`.

A newly-tagged column is a *masking* gap, not an access breach (Unity Catalog granted nothing you didn't ask for). For your most sensitive data, prefer "locked down until proven safe" over "open until tagged."

---

## Reference & glossary

<details>
<summary><strong>Reference & glossary</strong> — commands, how-it-works, and term definitions (in <a href="REFERENCE.md">REFERENCE.md</a>)</summary>

Kept out of this walkthrough so it stays scannable — all in **[REFERENCE.md](REFERENCE.md)**:

- **[Command reference](REFERENCE.md#command-reference)** — every `make` target in one table.
- **[How it works (under the hood)](REFERENCE.md#how-it-works-under-the-hood)** — the exposure gate, the three governance layers, and one-mask-per-column, explained.
- **[Glossary](REFERENCE.md#glossary)** — every term used here (`gr_treatment`, `class.*`, coverage gate, ABAC, …).
</details>
