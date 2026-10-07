# GenieRails Dev-to-Prod Walkthrough — ship a Genie agent to production, safely

Take a curated Genie agent in **dev** and ship it to **production** without ever exposing sensitive data. Databricks' built-in classifier — running inside your own workspace — decides *what* is sensitive by sampling your column values (emails, SSNs, card numbers); GenieRails derives *how* it's protected (column masks and access rules) and applies it all as code; and **no new business access is granted** until every classified sensitive column the agent can reach is provably covered. You rehearse the whole thing safely in **dev**, then do it for real in **prod**.

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
| **Row-pairing key** (`VERIFY_KEY_COLUMN`) | A stable, unique, **non-sensitive** (never masked) id column present on your masked tables (e.g. `customer_id`) — `verify-access` uses it to line up rows. Save it once as `verify_key_column` in `env.auto.tfvars`. [How to choose](../../docs/effective-access-verification.md#choosing-the-row-pairing-key-verify_key_column). |

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

**2. Fill in credentials** — edit **`envs/dev/auth.auto.tfvars`**: the deploying SP `client_id` / `client_secret` + workspace host & id. Step 3 calls Databricks with these.

**3. Set your Genie agent ID and access tiers, then import the agent.** In `envs/dev/env.auto.tfvars`, replace `<your-genie-space-id>` with your **Agent ID** and set your access-tier groups, most-privileged first (keep the template's safety defaults):

```hcl
access_tier_groups = ["payments_ops", "regional_analysts", "viewers"]
```

Then [import the agent](../../docs/import-genie-agent-from-ui.md):

```bash
make generate ENV=dev MODE=genie
```

Every later `make generate` reads `access_tier_groups`, and `make promote` carries it to prod, so you never retype the groups. (Prefer the CLI? Leave it `[]` and pass `GENERATE_ARGS='--groups "payments_ops,regional_analysts,viewers"'` once — the first run saves it there. A later `--groups` that differs applies to that run only and prints how to update the setting.)

The import discovers the agent's tables into `envs/dev/data_access/discovered_uc_tables.auto.tfvars` — no `uc_tables` needed. *No agent yet?* Use the [Sample Environment Setup](SAMPLE_ENV.md).

**Done when —** `envs/dev/auth.auto.tfvars` is filled in, `genie_space_id` and `access_tier_groups` are set, and `discovered_uc_tables.auto.tfvars` lists the agent's tables.

</details>

---

<a id="phase-1--dev-scan-draft-and-test-rules"></a>
<details>
<summary><strong>Phase 1 — Dev: Scan, draft, and test rules</strong></summary>

**Goal —** *rehearse* safely on dev: prove the masks fire, confirm the agent still answers, and produce a reviewable draft — off live PII. (Prod discovers what's actually sensitive later.)

**1a. Turn on the scanner.** Enable UC Data Classification on your catalog in the **Databricks UI** — Catalog Explorer → your catalog → *Enable* classification. The scan just runs; nothing is tagged until you opt in (1b).

<details>
<summary><strong>Alternative — Enable classification as code</strong></summary>

```bash
make enable-classification ENV=dev   # scans only — nothing is tagged until you opt in (1b)
```

This alternative uses the template's `enable_classification = true` safety setting, which also makes generation fail closed if classification results cannot be read.

The as-code path applies the `databricks_data_classification_catalog_config` resource. On a workspace without a serverless usage policy it can fail with `Usage policy ID must not be empty` ([terraform-provider-databricks#5985](https://github.com/databricks/terraform-provider-databricks/issues/5985)); the UI path above avoids this issue. If needed, create or attach a serverless usage policy first. Creating one requires Workspace Admin (non-admins need *Serverless usage policy: Manager*). Docs: [AWS](https://docs.databricks.com/aws/en/admin/usage/budget-policies) / [Azure](https://learn.microsoft.com/en-us/azure/databricks/admin/usage/budget-policies).
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

**1c. Draft the protection rules.**
```bash
make generate ENV=dev
```
It uses the `access_tier_groups` you set in Phase 0 — **your own** IdP-synced groups, **one per access tier, most-privileged first** (`payments_ops`=full/raw → `regional_analysts`=region-scoped + masked → `viewers`=least-privileged, with all sensitive columns masked — placeholders; use your real names). GenieRails *consumes* them by exact name, never creates them; the generated policies define each tier's actual access.

Re-running `make generate ENV=dev` keeps the reviewed rules in `envs/dev/generated/` and adds rules only for uncovered columns. It prints one `kept reviewed rule …` line for each rule the model tried to drop or change, and one `dropped stale reviewed rule …` line for each rule whose table or column no longer exists. To accept the model's changes, re-run with `GENERATE_ARGS='--allow-rule-changes'` (or edit the generated files by hand).

**1d. Prove coverage, apply, and verify — one command.**
```bash
make rehearse ENV=dev VERIFY_KEY_COLUMN=customer_id
```
`make rehearse` runs **live derive → validate-generated → coverage-gate → apply → verify-access** in order, stopping at the first failure. The last step, `verify-access`, proves masking *by effect*: it creates a test SP for each access tier, grants each test SP temporary `CAN_USE` on the selected warehouse, and confirms the unprivileged tier sees masked values while an authorized tier sees raw. No separate warehouse-permission command is required. There is no access flag to set: business `SELECT` and Genie run access are granted only when the coverage check passes, and dev keeps that access after rehearse (the masks protect the data either way). `VERIFY_KEY_COLUMN` is the single column used to pair rows across tiers — optional, but if you omit it the masking check is skipped, so always set it ([how to choose](../../docs/effective-access-verification.md#choosing-the-row-pairing-key-verify_key_column); save it as `verify_key_column` in `env.auto.tfvars` to drop the flag); for tables that don't share one key, pass a [`VERIFY_SPEC` JSON](../../docs/effective-access-verification.md) instead.

</details>

---

<a id="phase-2--prod-set-up-and-promote-rules"></a>
<details>
<summary><strong>Phase 2 — Prod: Set up and promote rules</strong></summary>

**Goal —** create the production configuration, add its credentials and settings, and copy the reviewed rules (masks, access policies, mappings) from dev.

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP="dev_finance=prod_finance"
```

`DEST_CATALOG_MAP` renames each dev catalog to its prod name (`dev_finance=prod_finance`; comma-separate multiple).

Promotion creates `envs/prod/` with configuration templates. Fill in both files:

- **`envs/prod/auth.auto.tfvars`** — the deployment SP `client_id` / `client_secret` + prod workspace host & id. You may reuse the dev SP when both workspaces are in the same Databricks account and it is authorized in prod; use a separate prod SP when your security policy requires environment isolation. Separate Databricks accounts require separate SPs.
- **`envs/prod/env.auto.tfvars`** — don't recreate it; set `sql_warehouse_id` (or leave `""` to auto-create). Promotion has already written the safe classification and access defaults.

Using the [sample environment](SAMPLE_ENV.md)? Seed the prod catalog with its tables now (`--skip-agent`; see SAMPLE_ENV.md).

**Done when —** `envs/prod/` points at the prod catalog and both production configuration files are filled in.

<details>
<summary><strong>Details — What promotion carries and leaves behind</strong></summary>

It carries the **rules** — the mapping, masking functions, access/row-filter policies, group→tier mapping, and any reviewed per-column `treatment_overrides` — and **leaves dev's tag assignments behind** (which columns got tagged is a *fact* about dev's data; prod re-derives its own in Phase 3). Override column names are remapped through `DEST_CATALOG_MAP`; they never carry or widen ACLs.
</details>

</details>

---

<a id="phase-3--prod-scan-real-data"></a>
<details>
<summary><strong>Phase 3 — Prod: Scan real data</strong></summary>

**Goal —** let production scan its *own* real data and tag its sensitive columns — the true facts land here (real customer PII only exists in prod).

With the production configuration from Phase 2 in place, **turn on prod's scanner in the Databricks UI** on the prod catalog (same as step 1a). It scans **without writing tags** (auto-tagging defaults off, so prod gets its *own* review, just like dev). Then, exactly as in dev's **1b**:

<details>
<summary><strong>Alternative — Enable classification as code</strong></summary>

```bash
make enable-classification ENV=prod   # same as step 1a, now on prod
```

This alternative uses the promoted file's `enable_classification = true` safety setting.
</details>

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

<a id="phase-4--prod-prove-coverage"></a>
<details>
<summary><strong>Phase 4 — Prod: Review prod coverage</strong></summary>

**Goal —** confirm prod classification has finished and preview the live derivation and coverage check that `make release` repeats before it applies.

```bash
make derive-assignments ENV=prod
make coverage-gate ENV=prod
```

The derivation reuses the exact rules you reviewed in dev and never calls a model. A promoted per-column override is merged strictest-wins with the native result, so it can strengthen but never weaken native protection.

These commands are read-only apart from refreshing generated local files: they do not deploy governance or the Genie agent. If prod's tags cannot be read, or a detected data type has no protection rule, the coverage check stops.

**Done when —** `coverage-gate` exits PASS. `make release` repeats this check and then runs `audit-rulebook` against the promoted rules before it can apply new or wider access.

**If the check fails here, or `audit-rulebook` reports drift during Phase 5** — prod surfaced a sensitive tag your promoted rules don't cover (a type the classifier found only in prod, or a rule dropped in promotion). This is a **rule change — made in dev, never hand-edited in prod**. Loop back:

1. **Scaffold the missing mappings** — `make scaffold-treatments ENV=prod` adds a **safe default** (full redaction, marked `REVIEW`) for each tag prod surfaced, so you don't hand-edit anything. Then **review each** — keep the redaction, or set a type-appropriate mask. This changes the shared *rulebook* (not prod's live state), so you validate it in dev and re-promote below.
2. **Re-validate in dev:** `make generate ENV=dev` (reuses `access_tier_groups`; keeps reviewed rules; adds rules only for uncovered columns) → `make coverage-gate ENV=dev`.
3. **Re-promote:** `make promote …` (carries the updated rules to prod — same command as [Phase 2](#phase-2--prod-set-up-and-promote-rules)).
4. **Re-run the checks above**, then continue to Phase 5.

Repeat until the check passes and drift is clean. These checks don't apply anything, so prod access doesn't change while you loop.

</details>

---

<a id="phase-5--prod-release-access-and-verify"></a>
<details>
<summary><strong>Phase 5 — Prod: Release and verify</strong></summary>

**Goal —** re-check live coverage, apply prod (masks first, then access), and confirm masking live.

```bash
make release ENV=prod VERIFY_KEY_COLUMN=customer_id
```

One command: placeholder guard → lock → live UC re-read/`derive-assignments` → validation → coverage check → promote the derived config into its Terraform layers → read-only `audit-rulebook` → all-layer apply → `verify-access`. The audit runs before the access-granting apply, so drift or an audit error leaves existing access unchanged and blocks any new or wider business `SELECT` or Genie run access. There is no access flag to set or save: Terraform grants business `SELECT` and Genie run access only through the passing coverage check, and re-running `promote` never closes access that is already live.

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

<a id="phase-6--prod-maintain-coverage"></a>
<details>
<summary><strong>Phase 6 — Prod: Maintain coverage</strong></summary>

**Goal —** catch sensitive data that arrives after go-live. Run this on a schedule (cron or CI):

```bash
make maintain ENV=prod   # audit-schema → derive-assignments → coverage-gate → validate-generated → apply-governance → audit-rulebook
```

It protects newly tagged columns using prod's own `class.*` tags. It never changes business access or the Genie agent.

- **Stops at `audit-schema`** — a sensitive-looking column has no `class.*` tag yet. Review it in native classification (`make enable-classification ENV=prod`) or tag it in Unity Catalog, then re-run `make maintain ENV=prod`.
- **Stops at `coverage-gate` or `audit-rulebook`** — prod has a tag your rules don't cover. Add the rule in dev, rehearse, and re-promote before running `make release ENV=prod` again.

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
