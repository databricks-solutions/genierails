# GenieRails Dev-to-Prod Walkthrough — ship a Genie agent to production, safely

Take a curated Genie agent in **dev** and ship it to **production** without ever exposing sensitive data. Databricks' built-in classifier — running inside your own workspace — decides *what* is sensitive by sampling your column values (emails, SSNs, card numbers); GenieRails derives *how* it's protected (column masks and access rules) and applies it all as code; and **no new business access is granted** until every classified sensitive column the agent can reach is provably covered. You rehearse the whole thing safely in **dev**, then do it for real in **prod**.

**The whole flow** (each phase below explains its step):

```
# dev
make setup ENV=dev                   # then set genie_space_id in envs/dev/env.auto.tfvars
make enable-classification ENV=dev   # or turn it on in the UI; wait for class.* tags
make generate ENV=dev                # ONE run: imports the agent, finds its tables, drafts rules
make rehearse ENV=dev
# prod (same pattern for stg or any env)
# Set promote_from/catalog_map in envs/prod/env.auto.tfvars first.
make promote-to ENV=prod
make enable-classification ENV=prod
make release ENV=prod
make maintain ENV=prod
```

---

<a id="prerequisites--gather-required-values"></a>
<details>
<summary><strong>Before you start — Complete checks and gather inputs</strong></summary>

First clone the repo and move into your cloud's folder. Every `make` command in this walkthrough, including `make bootstrap-sp` in the prerequisites, runs from here:

```bash
git clone https://github.com/databricks-solutions/genierails.git
cd genierails/aws           # or: cd genierails/azure
```

Then complete the shared **[Prerequisites checklist](../../docs/prerequisites.md)**. It is the single source of truth for required software, network access, Databricks features, IdP group sync, Service Principal authority, credentials, and local tool checks.

Finally, gather the inputs specific to this walkthrough:

| Value | What it is / where to find it |
|---|---|
| **Dev / prod catalog names** | Your Unity Catalog catalogs, e.g. `dev_finance` / `prod_finance`. |
| **SQL warehouse id** (per Genie agent) | The serverless warehouse the Genie agent runs its SQL on — an existing warehouse's id, **or leave blank** to auto-create one. Set it on the agent's `genie_spaces` entry; agents can also share the environment-level warehouse as a fallback. |
| **Curated Genie agent** | The agent you're shipping. In the Genie UI, open the agent, click **Configure**, and copy the **Agent ID** from **About this agent**. It is also in the URL (`.../genie/rooms/01ef7b3c2a4d5e6f`) and goes in `genie_spaces`; its tables are discovered automatically. |
| **Access-tier group names** | Choose the IdP-synced groups from the shared prerequisite check, ordered most- to least-privileged; you enter them once, as `access_tier_groups` (Phase 0). Example: `payments_ops` = full/raw; `regional_analysts` = region-scoped + masked; `viewers` = least-privileged + all sensitive columns masked. The generated policies define the actual access, and each agent's tables are `SELECT`-granted only to the tiers authorized to run that agent (a table shared by several agents gets the union). |
| **Row-pairing key** | Nothing to set. `verify-access` picks a key per masked table to line up rows across tiers (its primary key, else an untagged id-like column such as `customer_id`), proves it unique, non-null and unmasked, saves the proven keys after a passing run, and promotion carries them to prod. Override per table only if needed. [How it picks](../../docs/effective-access-verification.md#how-genierails-picks-the-row-pairing-key). |

</details>

---

<a id="phase-0--dev-set-up"></a>
<details>
<summary><strong>Phase 0 — Dev: Set up</strong></summary>

**Goal —** create the local config folders, fill in credentials, and point dev at the Genie agent you're shipping.

**1. Create the dev config** — from the `genierails/aws` (or `genierails/azure`) folder you cloned in **Before you start**:

```bash
make setup ENV=dev          # creates envs/dev/ config templates (local only — no Databricks calls)
cp ../shared/examples/dev_to_prod/env.auto.tfvars.example envs/dev/env.auto.tfvars
```

**2. Fill in credentials** — edit **`envs/dev/auth.auto.tfvars`**: the deploying SP `client_id` / `client_secret` + workspace host & id. Phase 1 calls Databricks with these.

**3. Set your Genie agent ID and access tiers.** In `envs/dev/env.auto.tfvars`, replace `<your-genie-space-id>` with your **Agent ID** and set your access-tier groups, most-privileged first (keep the template's safety defaults):

```hcl
access_tier_groups = ["payments_ops", "regional_analysts", "viewers"]
```

Every later `make generate` reads `access_tier_groups`. The first promote seeds it in prod; re-promotes preserve prod's reviewed value. To change prod tiers or a space's `acl_groups`, edit `envs/prod/env.auto.tfvars` in a PR and let the pipeline apply it. (Prefer the CLI? Leave it `[]` and pass `GENERATE_ARGS='--groups "payments_ops,regional_analysts,viewers"'` once — the first run saves it there. A later `--groups` that differs applies to that run only and prints how to update the setting.)

No `uc_tables` needed: the agent's tables are discovered from its ID. *No agent yet?* Use the [Sample Environment Setup](SAMPLE_ENV.md).

**Done when —** `envs/dev/auth.auto.tfvars` is filled in and `genie_space_id` and `access_tier_groups` are set.

</details>

---

<a id="phase-1--dev-scan-draft-and-test-rules"></a>
<details>
<summary><strong>Phase 1 — Dev: Scan, draft, and test rules</strong></summary>

**Goal —** *rehearse* safely on dev: prove the masks fire, confirm the agent still answers, and produce a reviewable draft — off live PII. (Prod discovers what's actually sensitive later.)

**1a. Turn on the scanner.**

```bash
make enable-classification ENV=dev   # scans only — nothing is tagged until you opt in (1b)
```

It finds the agent's tables from its ID (read-only; saved to `envs/dev/data_access/discovered_uc_tables.auto.tfvars`), then enables UC Data Classification on them. Or enable it in the **Databricks UI** — Catalog Explorer → your catalog → *Enable* classification.

<details>
<summary><strong>Note — <code>Usage policy ID must not be empty</code></strong></summary>

The as-code path applies the `databricks_data_classification_catalog_config` resource. On a workspace without a serverless usage policy it can fail with `Usage policy ID must not be empty` ([terraform-provider-databricks#5985](https://github.com/databricks/terraform-provider-databricks/issues/5985)); the UI path avoids this issue. If needed, create or attach a serverless usage policy first. Creating one requires Workspace Admin (non-admins need *Serverless usage policy: Manager*). Docs: [AWS](https://docs.databricks.com/aws/en/admin/usage/budget-policies) / [Azure](https://learn.microsoft.com/en-us/azure/databricks/admin/usage/budget-policies).
</details>

**1b. Review detections.** The first scan is asynchronous (minutes to ~24h) — kick it off, grab a coffee ☕, and come back. Open [Review detections](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification#review-detections) to see what the scanner found on your columns and **exclude any false positives**. Nothing is tagged yet — auto-tagging defaults off.

<details>
<summary><strong>Next — Enable automatic tagging after review</strong></summary>

After review, **enable automatic tagging in the UI** (per class). The `class.*` tags then land on the reviewed columns and appear in **Catalog Explorer**. Then continue to **1c**.

<details>
<summary><strong>Alternative — Enable automatic tagging as code</strong></summary>

As code, set `enable_auto_tagging = true` in `envs/dev/env.auto.tfvars` and re-run `make enable-classification ENV=dev`.

</details>

</details>

**1c. Import the agent and draft the protection rules — one run.**
```bash
make generate ENV=dev
```
It imports the agent's config, finds its tables, and drafts masks and rules from the `class.*` tags. If the tags aren't there yet it stops before calling the model (fail-closed) and tells you to enable classification, wait for `class.*` tags, and re-run `make generate ENV=dev`.

It uses the `access_tier_groups` you set in Phase 0 — **your own** IdP-synced groups, **one per access tier, most-privileged first** (`payments_ops`=full/raw → `regional_analysts`=region-scoped + masked → `viewers`=least-privileged, with all sensitive columns masked — placeholders; use your real names). GenieRails *consumes* them by exact name, never creates them; the generated policies define each tier's actual access.

Re-running `make generate ENV=dev` keeps the reviewed rules in `envs/dev/generated/` and adds rules only for uncovered columns. It prints one `kept reviewed rule …` line for each rule the model tried to drop or change, and one `dropped stale reviewed rule …` line for each rule whose table or column no longer exists. To accept the model's changes, re-run with `GENERATE_ARGS='--allow-rule-changes'` (or edit the generated files by hand).

**1d. Prove coverage, apply, and verify — one command.**
```bash
make rehearse ENV=dev
```
`make rehearse` runs **live derive → validate-generated → coverage-gate → apply → verify-access** in order, stopping at the first failure. The last step, `verify-access`, proves masking *by effect*: it creates a test SP for each access tier, grants each test SP temporary `CAN_USE` on the selected warehouse, and confirms the unprivileged tier sees masked values while an authorized tier sees raw. No separate warehouse-permission command is required. There is no access flag to set: business `SELECT` and Genie run access are granted only when the coverage check passes, and dev keeps that access after rehearse (the masks protect the data either way).

To pair rows across tiers, `verify-access` picks a row-pairing key for each masked table: its single-column primary key, else an untagged id-like column (`<table>_id`, then `id`, then other `*_id`). It proves each key is unique, non-null and unmasked before relying on it, and prints which column it chose for each table (never values). After a passing run the proven keys are saved as `verify_key_columns` in `envs/dev/env.auto.tfvars`, and promotion carries them to prod. If a table has no provable key, the run names it and how to fix it; set its entry in `verify_key_columns` (or `VERIFY_KEY_COLUMN=<col>` for every table that has that column). [How it picks and how to override](../../docs/effective-access-verification.md#how-genierails-picks-the-row-pairing-key).

</details>

---

<a id="phase-2--prod-set-up-and-promote-rules"></a>
<details>
<summary><strong>Phase 2 — Prod: Set up and promote rules</strong></summary>

**Goal —** create the production configuration, add its credentials and settings, and copy the reviewed rules (masks, access policies, mappings) from dev.

```hcl
promote_from = "dev"
catalog_map  = { "<dev_catalog>" = "<prod_catalog>" } # replace both placeholders
```

Then run:

```bash
make promote-to ENV=prod
```

`catalog_map` renames every source catalog to its target name; use one map entry per catalog. The legacy string form (`"dev_finance=prod_finance"`) remains readable. `FROM=` and `CATALOG_MAP=` are optional overrides and win for that successful promote, which saves the canonical map form back to the target. For a staging chain, set `promote_from = "dev"` in `envs/stg/env.auto.tfvars`, promote stg, then set `promote_from = "stg"` in `envs/prod/env.auto.tfvars` (with the corresponding catalog maps).

`make setup ENV=prod` creates `envs/prod/` with configuration templates. Fill in both files before promotion:

- **`envs/prod/auth.auto.tfvars`** — the deployment SP `client_id` / `client_secret` + prod workspace host & id. You may reuse the dev SP when both workspaces are in the same Databricks account and it is authorized in prod; use a separate prod SP when your security policy requires environment isolation. Separate Databricks accounts require separate SPs.
- **`envs/prod/env.auto.tfvars`** — don't recreate it; replace the promotion placeholders and set `sql_warehouse_id` (or leave `""` to auto-create). A successful promotion writes safe classification/access defaults and dev's proven row-pairing keys (`verify_key_columns`, renamed to prod's catalogs) while preserving the target-owned promotion settings.

Using the [sample environment](SAMPLE_ENV.md)? Seed the prod catalog with its tables now (`--skip-agent`; see SAMPLE_ENV.md).

**Done when —** `envs/prod/` points at the prod catalog and both production configuration files are filled in.

<details>
<summary><strong>Details — What promotion carries and leaves behind</strong></summary>

It carries the **rules** — the mapping, masking functions, access/row-filter policies, group→tier mapping, and any reviewed per-column `treatment_overrides` — and **leaves dev's tag assignments behind** (which columns got tagged is a *fact* about dev's data; prod re-derives its own when you release). Override column names are remapped through `CATALOG_MAP`; they never carry or widen ACLs. `make promote-to` is the same promotion as the older `make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP=…`, which still works.
</details>

</details>

---

<a id="phase-3--prod-scan-real-data"></a>
<details>
<summary><strong>Phase 3 — Prod: Scan real data</strong></summary>

**Goal —** let production scan its *own* real data and tag its sensitive columns — the true facts land here (real customer PII only exists in prod).

```bash
make enable-classification ENV=prod   # same as step 1a, now on prod (or use the UI)
```

It scans **without writing tags** (auto-tagging defaults off, so prod gets its *own* review, just like dev). Then, exactly as in dev's **1b**:

**Review (UI).** Open [Review detections](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification#review-detections) on the prod catalog and **exclude any false positives** — prod's real data may surface sensitive types dev never saw.

<details>
<summary><strong>Next — Enable automatic tagging after review</strong></summary>

**Enable automatic tagging in the UI** (per class); the `class.*` tags then land.

<details>
<summary><strong>Alternative — Enable automatic tagging as code</strong></summary>

Set `enable_auto_tagging = true` in `envs/prod/env.auto.tfvars` and re-run `make enable-classification ENV=prod`.

</details>

</details>

**Done when —** prod's `class.*` tags appear on the prod catalog (check in Catalog Explorer / Review detections). The scan is async — grab a coffee ☕ and re-check; nothing yet just means it hasn't finished.

</details>

---

<a id="phase-4--prod-release-and-verify"></a>
<details>
<summary><strong>Phase 4 — Prod: Release and verify</strong></summary>

**Goal —** check live coverage, apply prod (masks first, then access), and confirm masking live.

```bash
make release ENV=prod
```

`release` must prove every mask, and needs no key from you. It uses dev's proven keys that promotion carried over, and picks the rest the same way rehearse does. Before it applies anything it proves every masked table's key as the admin (`make verify-access-keys`: no test principals, no grants). If a table has no provable key, `release` refuses then and names the table, instead of skipping its mask checks ([override](../../docs/effective-access-verification.md#how-genierails-picks-the-row-pairing-key)).

One command: placeholder guard → lock → live UC re-read/`derive-assignments` → validation → coverage check → promote the derived config into its Terraform layers → row-pairing key check (admin only) → read-only `audit-rulebook` → all-layer apply → `verify-access`. The derivation reuses the exact rules you reviewed in dev and never calls a model; a promoted per-column override is merged strictest-wins with the native result, so it can strengthen but never weaken native protection. The audit runs before the access-granting apply, so drift or an audit error leaves existing access unchanged and blocks any new or wider business `SELECT` or Genie run access. There is no access flag to set or save: Terraform grants business `SELECT` and Genie run access only through the passing coverage check, and re-promoting never closes access that is already live.

**If the coverage check fails or `audit-rulebook` reports drift** — prod surfaced a sensitive tag your promoted rules don't cover (a type the classifier found only in prod, or a rule dropped in promotion). Nothing new was applied. This is a **rule change — made in dev, never hand-edited in prod**:

1. **Scaffold the missing mappings** — `make scaffold-treatments ENV=prod` adds a **safe default** (full redaction, marked `REVIEW`) for each tag prod surfaced. **Review each** — keep the redaction, or set a type-appropriate mask.
2. **Re-validate in dev:** `make generate ENV=dev` (keeps reviewed rules; adds rules only for uncovered columns) → `make rehearse ENV=dev`.
3. **Re-promote:** `make promote-to ENV=prod`, then re-run `make release ENV=prod`.

If `release` fails after it started applying, or you interrupt it, access may be partly applied — but only access that passed the coverage check. Follow the steps it prints. To withdraw access, remove the groups (or set `acl_groups = []`) in `envs/prod/env.auto.tfvars` and run `make apply ENV=prod`; `business_access_enabled` is retired and setting it to `false` does not revoke anything.

<details>
<summary><strong>Optional — Capture compliance evidence</strong></summary>

Run `make evidence ENV=prod` to write a configuration-based JSON and Markdown report to `envs/prod/generated/evidence/`.

For a live, approver-signed snapshot of the deployed masks and grants, supply the warehouse ID:

```bash
GENIERAILS_EVIDENCE_INTEGRATION=1 \
GENIERAILS_EVIDENCE_APPROVED_BY="<you>" \
make evidence ENV=prod WAREHOUSE_ID=<id>
```

If GenieRails auto-created the warehouse, get its ID from the cloud root:

```bash
ENVS_DIR="$PWD/envs" ../shared/scripts/terraform_layer.sh workspace prod output -raw sql_warehouse_id
```

</details>

**Done when —** `verify-access` shows masked values for the unprivileged tier and raw for the authorized tier; business users can open the Genie agent and get useful, masked answers.

</details>

---

<a id="phase-5--prod-maintain-coverage"></a>
<details>
<summary><strong>Phase 5 — Prod: Maintain coverage</strong></summary>

**Goal —** catch sensitive data that arrives after go-live. Run this on a schedule (cron or CI):

```bash
make maintain ENV=prod   # audit-schema → derive-assignments → coverage-gate → validate-generated → audit-rulebook → apply-governance
```

It protects newly tagged columns using prod's own `class.*` tags, and audits the rulebook before it applies anything. It reconciles governance, including `SELECT` for tables already covered by the passing coverage check; it never widens access past that check or changes the Genie agent. When generated inputs are unchanged, the apply is skipped, so `maintain` does not repair grants revoked outside Terraform.

- **Stops at `audit-schema`** — a sensitive-looking column has no `class.*` tag yet. Review it in native classification (`make enable-classification ENV=prod`) or tag it in Unity Catalog, then re-run `make maintain ENV=prod`.
- **Stops at `coverage-gate` or `audit-rulebook`** (before applying anything) — prod has a tag your rules don't cover. Add the rule in dev, rehearse, and `make promote-to ENV=prod` before running `make release ENV=prod` again.

A newly-tagged column is a *masking* gap, not an access breach (Unity Catalog granted nothing you didn't ask for). For your most sensitive data, prefer "locked down until proven safe" over "open until tagged."

</details>

---

<a id="reference--commands-concepts-and-glossary"></a>
<details>
<summary><strong>Reference — Commands, concepts, and glossary</strong></summary>

Kept out of this walkthrough so it stays scannable — all in **[REFERENCE.md](REFERENCE.md)**:

- **[Command reference](REFERENCE.md#command-reference)** — every `make` target in one table.
- **[How it works (under the hood)](REFERENCE.md#how-it-works-under-the-hood)** — how access follows the coverage check, the three governance layers, and one-mask-per-column, explained.
- **[Glossary](REFERENCE.md#glossary)** — every term used here (`gr_treatment`, `class.*`, coverage check, ABAC, …).
</details>
