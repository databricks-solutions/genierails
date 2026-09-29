# GenieRails Dev-to-Prod Walkthrough — ship a Genie agent to production, safely

Take a curated Genie agent in **dev** and ship it to **production** without ever exposing sensitive data. Unity Catalog's built-in classifier decides *what* is sensitive; GenieRails derives *how* it's protected and applies it as code; and a **coverage check blocks the release** until every *classified* sensitive column the agent can reach is provably covered.

---

## The idea

Databricks scans your data and labels what's sensitive. GenieRails turns those labels into protections — column masks and access rules — as code, and a **coverage check refuses to release the agent** until every sensitive column is covered. You rehearse the whole thing safely in **dev**, then do it for real in **prod**. (Full mental model: [How it works](../../../README.md#how-it-works).)

---

## Prerequisites & what to gather

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
| **Serverless budget policy** | On a newly provisioned serverless workspace, confirm an account budget policy is bound to the workspace before enabling classification. Without one, the classification API fails with `Usage policy ID must not be empty`. Creating or binding a policy requires the account-level `CreateBudgetPolicyPermission` / `UpdateBudgetPolicyPermission`, which can be separate from the Account Admin role. |

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

> **Where do `genie_spaces` / `uc_tables` come from?** Already built the agent in the Databricks UI → import it into code first: [From UI to Production](../../docs/from-ui-to-production.md) captures the agent *and* its tables. No agent or tables of your own yet → use the optional [Sample Environment Setup](#sample-environment-setup-optional) below. Either path hands you the exact values to paste above.

**Done when —** `ls envs/dev` shows `auth.auto.tfvars` and `env.auto.tfvars`, both filled in.

---

## Sample Environment Setup (Optional)

**No tables or Genie agent of your own?** Expand this to create a sample schema (three tables of realistic synthetic PII) + a sample agent and get the exact values to paste into `envs/dev/env.auto.tfvars`. **Skip it if you have your own.**

<details>
<summary><strong>Set up a sample environment</strong> (optional demo tooling — do Phase 0 first)</summary>

Do [Phase 0](#phase-0--set-up-dev) first, then this, then continue to [Phase 1](#phase-1--dev-scan-draft-the-rules-test-them).

```bash
cd ../shared/examples/dev_to_prod          # from the cloud root (aws/ or azure/); return with 'cd ../../../aws' afterward
python -m pip install -r requirements.txt
python setup_sample_env.py --catalog dev_finance --warehouse-id <your-warehouse-id>
```

**Auth is optional to specify.** It uses a **Databricks CLI profile** — separate from the deploying Service Principal `make` uses (that lives in `auth.auto.tfvars`). No flag → your **default** CLI profile (or `DATABRICKS_HOST`/`DATABRICKS_TOKEN`); add `--profile <name>` only for a *named* profile.

The script prints the `uc_tables`, `genie_spaces`, and `sql_warehouse_id` snippet — paste it into `envs/dev/env.auto.tfvars`. It does **not** create access-tier groups, so in Phase 1 pass `--groups` with existing names or use `--create-groups`. Re-runs are safe. To remove only what it created:

```bash
python teardown_sample_env.py --catalog dev_finance
# Equivalent: add --teardown to the setup command.
```

Use `--help` for `--host`, `--schema`, `--rows`, and env-var alternatives. Then `cd ../../../aws` (or the azure path) and continue.
</details>

---

## Phase 1 — Dev: scan, draft the rules, test them

**Goal —** *rehearse* safely on dev: prove the masks fire, confirm the agent still answers, and produce a reviewable draft — off live PII. (Prod discovers what's actually sensitive later.)

**1a. Turn on the scanner.**
```bash
make enable-classification ENV=dev
```
Turns on the scanner only — nothing is tagged or masked yet, and auto-tagging stays off until you opt in (1b).

**1b. Review detections, opt into tags, then confirm they landed.** The first scan is asynchronous — minutes to ~24h, with no way to force it (a real *stop-and-resume-later* point). Two easy steps, mostly in the Databricks UI:

- **Review (UI).** Open [Review detections](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification#review-detections) to see what the scanner found on your columns and **exclude any false positives**. Nothing is tagged yet — auto-tagging defaults off.
- **Opt in.** After review, set `enable_auto_tagging = true` in `envs/dev/env.auto.tfvars` and re-run `make enable-classification ENV=dev`. The `class.*` tags then land on the reviewed columns (again, minutes to ~24h — on **Azure** the initial scan runs materially slower than on AWS, tens of minutes rather than a few). You can see the applied tags on each column in **Catalog Explorer** — then go to **1c**.

**1c. Draft the protection rules.**
```bash
make generate ENV=dev GENERATE_ARGS='--groups "payments_ops,regional_analysts,viewers"'
```
`--groups` are **your own** IdP-synced groups, **one per access tier, most-privileged first** (`payments_ops`=full → `regional_analysts`=masked → `viewers`=least — placeholders; use your real names). GenieRails *consumes* them by exact name, never creates them.

**1d. Prove coverage, apply, and verify.** One knob: **`business_access_enabled`** in `envs/dev/env.auto.tfvars` is the *exposure gate* — `false` (default) deploys the masks but **withholds** user access; `true` **releases** it. **In dev you set it `true` once and leave it** — dev is a rehearsal (the masks protect the data either way), so there's no closing and no back-and-forth. (The gate earns its keep in prod's [Phase 5](#phase-5--open-to-users-you-release-access-after-the-gate-passes-then-verify): you open access *last*, only after coverage is proven on real data.)

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

**Goal —** copy the *rules* to production — never the dev data, and never which columns dev labelled.

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP="dev_finance=prod_finance"
```

**Done when —** `envs/prod/env.auto.tfvars`'s `uc_tables` now points at your prod catalog (`prod_finance`).

**Then edit `envs/prod/env.auto.tfvars`** (don't recreate it): set `sql_warehouse_id` (or leave `""` to auto-create), and add `enable_classification = true`, `enable_auto_tagging = false`, `business_access_enabled = false`.

<details>
<summary><strong>What promote carries vs. leaves behind</strong> (rules travel, facts don't)</summary>

It carries the **rules** — the mapping, masking functions, access/row-filter policies, and group→tier mapping — and **leaves dev's tag assignments behind** (which columns got labelled is a *fact* about dev's data; prod re-derives its own in Phase 3). You do **not** touch the account config again — `envs/account/` is shared and you already set `manage_groups = false` in Phase 0.
</details>

---

## Phase 3 — Prod: scan real data

**Goal —** let production scan its *own* real data and label its sensitive columns — the true facts land here (real customer PII only exists in prod).

Promotion (Phase 2) already created `envs/prod/` with a template `auth.auto.tfvars` — just fill it in (prod SP `client_id`/`client_secret` + prod workspace host/id), then turn on prod's scanner:

```bash
# fill envs/prod/auth.auto.tfvars first (prod SP + workspace host/id), then:
make enable-classification ENV=prod   # same as step 1a, now on prod
```

`make enable-classification ENV=prod` turns on prod's scanner **without writing tags** (auto-tagging defaults off, so prod gets its *own* review, just like dev). Then, exactly as in dev's **1b**:

- **Review (UI).** Open [Review detections](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification#review-detections) on the prod catalog and **exclude any false positives** — prod's real data may surface sensitive types dev never saw.
- **Opt in.** Set `enable_auto_tagging = true` in `envs/prod/env.auto.tfvars` (add the line if promote didn't write it) and re-run `make enable-classification ENV=prod`. The `class.*` tags then land.

**Done when —** prod's `class.*` tags appear on the prod catalog (same SQL as 1b). The scan is async (another stop-and-resume point) — zero rows just means it hasn't finished; wait and re-check.

---

## Phase 4 — Prove coverage (the gate)

**Goal —** derive prod's protections from its own labels, prove coverage, and deploy the enforcement — but **not** the agent yet.

```bash
make certify ENV=prod   # one command: derive-assignments → coverage-gate → validate-generated → apply-governance → audit-rulebook (stops at the first failure)
```

One command that **reuses the rules you reviewed in dev unchanged** (no model call → no drift), proves coverage, and deploys **enforcement only** — no Genie agent yet (Phase 5). It is **fail-closed**: it aborts if prod's live tags are unreadable/empty, a finding is unmapped, or a treatment has no covering mask.

<details>
<summary><strong>What <code>certify</code> runs — and running the stages individually</strong> (e.g. for CI, where <code>coverage-gate</code> must be its own blocking step)</summary>

```bash
make derive-assignments ENV=prod   # re-derive tag_assignments from prod's LIVE class.* tags; REUSES the promoted rules unchanged (no model call)
make coverage-gate    ENV=prod   # blocks on any labelled-but-unprotected column
make validate-generated ENV=prod # static checks on the prod-generated config
make apply-governance ENV=prod   # deploy enforcement ONLY (account + data_access) — no Genie agent yet
make audit-rulebook   ENV=prod   # flags any prod tag with no covering rule (drift)
```

`derive-assignments` reads prod's live `class.*` tags and re-derives exactly one `gr_treatment` per column, writing **only** the `tag_assignments` — reusing the masks, policies, row filters, and group→tier mapping you promoted **unchanged** (no `--groups`, no model call). `apply-governance` deploys the enforcement (groups, tag policies, masking functions, access/row-filter policies, grants) but **not** the workspace layer, so the Genie agent isn't created yet (Phase 5). `audit-rulebook` reports any prod `class.*`/`gr_treatment` tag with no covering policy/mask (drift).

**`verify-access` is not here** — it needs the exposure gate open, so it runs in Phase 5. And **use `apply-governance`, not `make apply`** — a full `apply` would create the Genie agent before the gate passes (`make certify` already uses `apply-governance`).
</details>

**Done when —** `coverage-gate` exits PASS and `audit-rulebook` reports no uncovered tags.

**If the gate fails or `audit-rulebook` reports drift** — prod surfaced a sensitive tag your promoted rules don't cover (a type the classifier found only in prod, or a rule dropped in promotion). This is a **rule change — made in dev, never hand-edited in prod**. Loop back:

1. **Scaffold the missing mappings** — `make scaffold-treatments ENV=prod` adds a **safe default** (full redaction, marked `REVIEW`) for each label prod surfaced, so you don't hand-edit anything. Then **review each** — keep the redaction, or set a type-appropriate mask. This changes the shared *rulebook* (not prod's live state), so you validate it in dev and re-promote below.
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

**Done when —** `verify-access` shows masked values for the unprivileged tier and raw for the authorized tier; business users can open the Genie agent and get useful, masked answers.

---

## Phase 6 — Keep it covered

**Goal —** catch sensitive data that arrives after go-live. Run these on a schedule (the repo ships a scheduled governance job):
```bash
make audit-schema   ENV=prod    # untagged sensitive columns + stale assignments
make audit-rulebook ENV=prod    # newly-detected tags with no covering rule → add a rule, re-derive
make generate-delta ENV=prod    # incremental tag assignments after ALTER TABLE ADD/DROP/RENAME
```
A newly-tagged column is a *masking* gap, not an access breach (Unity Catalog granted nothing you didn't ask for) — add/derive the rule. For your most sensitive data, prefer "locked down until proven safe" over "open until tagged."

---

## Reference & glossary

<details>
<summary><strong>Reference & glossary</strong> — commands, how-it-works, and term definitions (in <a href="REFERENCE.md">REFERENCE.md</a>)</summary>

Kept out of this walkthrough so it stays scannable — all in **[REFERENCE.md](REFERENCE.md)**:

- **[Command reference](REFERENCE.md#command-reference)** — every `make` target in one table.
- **[How it works (under the hood)](REFERENCE.md#how-it-works-under-the-hood)** — the exposure gate, the three governance layers, and one-mask-per-column, explained.
- **[Glossary](REFERENCE.md#glossary)** — every term used here (`gr_treatment`, `class.*`, coverage gate, ABAC, …).
</details>
