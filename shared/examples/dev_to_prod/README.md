# GenieRails Dev-to-Prod Walkthrough — ship a Genie agent to production, safely

Take a curated Genie agent in **dev** and ship it to **production** without ever exposing sensitive data. Databricks' built-in classifier — running inside your own workspace — decides *what* is sensitive by sampling your column values (emails, SSNs, card numbers); GenieRails derives *how* it's protected (column masks and access rules) and applies it all as code; and a **coverage check blocks the release** until every classified sensitive column the agent can reach is provably covered. You rehearse the whole thing safely in **dev**, then do it for real in **prod**.

---

<a id="prerequisites--gather-required-values"></a>
<details>
<summary><strong>Before you start — Complete checks and gather inputs</strong></summary>

First complete the shared **[Prerequisites checklist](../../docs/prerequisites.md)**. It is the single source of truth for required software, network access, Databricks features, IdP group sync, Service Principal authority, credentials, and local tool checks.

Then gather the inputs specific to this walkthrough:

| Value | What it is / where to find it |
|---|---|
| **Dev / prod catalog names** | Your Unity Catalog catalogs, e.g. `dev_finance` / `prod_finance`. |
| **SQL warehouse id** (per Genie agent) | The serverless warehouse the Genie agent runs its SQL on — an existing warehouse's id, **or leave blank** to auto-create one. Set it on the agent's `genie_spaces` entry; agents can also share the environment-level warehouse as a fallback. |
| **Curated Genie agent** | The agent you're shipping. In the Genie UI, open the agent, click **Configure**, and copy the **Agent ID** from **About this agent**. It is also in the URL (`.../genie/rooms/01ef7b3c2a4d5e6f`) and goes in `genie_spaces`. |
| **Access-tier group names** | Choose the IdP-synced groups from the shared prerequisite check, ordered most- to least-privileged. Example: `payments_ops` = full/raw; `regional_analysts` = region-scoped + masked; `viewers` = least-privileged + all sensitive columns masked. The generated policies define the actual access. |
| **Row-pairing key** (`VERIFY_KEY_COLUMN`) | A stable, **non-sensitive** id column present on your masked tables (e.g. `customer_id`) — `verify-access` uses it to line up rows. [Details](../../docs/effective-access-verification.md). |

</details>

---

<a id="phase-0--dev-set-up"></a>
## Phase 0 — Dev: Set up

**Run:**

```bash
git clone https://github.com/databricks-solutions/genierails.git
cd genierails/aws           # or: cd genierails/azure
make setup ENV=dev          # creates envs/dev/ config templates (local only — no Databricks calls)
cp ../shared/examples/dev_to_prod/env.auto.tfvars.example envs/dev/env.auto.tfvars
```

**Then edit:** `envs/dev/auth.auto.tfvars`, `envs/dev/env.auto.tfvars`, and `envs/account/env.auto.tfvars`.

<details>
<summary><strong>Goal, file contents, and agent source</strong></summary>

Create the local config folders and fill in your credentials and settings. Nothing here touches Databricks yet.

- **`envs/dev/auth.auto.tfvars`** — the deploying SP `client_id` / `client_secret` + workspace host & id.
- **`envs/dev/env.auto.tfvars`** — `uc_tables`, `sql_warehouse_id` (or blank), and `genie_spaces`. Keep the template's safety defaults unchanged.
- **`envs/account/env.auto.tfvars`** — set `manage_groups = false` (this flow *consumes* IdP groups; it doesn't create them). **There is one shared `envs/account/` config** used by both dev and prod — you edit it here, once.

**Choose one source for `genie_spaces` and `uc_tables`:**

- **Existing Genie agent** — follow [From UI to Production](../../docs/from-ui-to-production.md) to import the agent and discover its tables.
- **No agent or tables yet** — use the optional [Sample Environment Setup](SAMPLE_ENV.md) to create them.

Either path provides the values to add to `envs/dev/env.auto.tfvars`.

**Done when —** `ls envs/dev` shows `auth.auto.tfvars` and `env.auto.tfvars`, both filled in.

</details>

---

<a id="phase-1--dev-scan-draft-and-test-rules"></a>
## Phase 1 — Dev: Scan, draft, and test rules

**Run in order:**

1. **UI:** Catalog Explorer → dev catalog → **Enable classification**.
2. **UI:** When the scan finishes, open **Review detections** and exclude false positives; then enable automatic tagging for each reviewed class.
3. **CLI:** Draft the protection rules:

   ```bash
   make generate ENV=dev GENERATE_ARGS='--groups "payments_ops,regional_analysts,viewers"'
   ```

4. **CLI:** Prove coverage, apply, and verify:

   ```bash
   make rehearse ENV=dev VERIFY_KEY_COLUMN=customer_id
   ```

<details>
<summary><strong>Goal, UI guidance, command details, and as-code alternatives</strong></summary>

Rehearse safely on dev: prove the masks fire, confirm the agent still answers, and produce a reviewable draft without live PII. Prod discovers what is actually sensitive later.

The first scan is asynchronous (minutes to ~24h). [Review detections](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification#review-detections), exclude any false positives, then enable automatic tagging per class. The `class.*` tags appear on the reviewed columns in Catalog Explorer.

<details>
<summary><strong>Alternative — Enable classification as code</strong></summary>

```bash
make enable-classification ENV=dev   # scans only — nothing is tagged until you opt in (1b)
```

This alternative uses the template's `enable_classification = true` safety setting, which also makes generation fail closed if classification results cannot be read.

The as-code path applies the `databricks_data_classification_catalog_config` resource. On a workspace without a serverless usage policy it can fail with `Usage policy ID must not be empty` ([terraform-provider-databricks#5985](https://github.com/databricks/terraform-provider-databricks/issues/5985)); the UI path above avoids this issue. If needed, create or attach a serverless usage policy first. Creating one requires Workspace Admin (non-admins need *Serverless usage policy: Manager*). Docs: [AWS](https://docs.databricks.com/aws/en/admin/usage/budget-policies) / [Azure](https://learn.microsoft.com/en-us/azure/databricks/admin/usage/budget-policies).
</details>

<details>
<summary><strong>Alternative — Enable automatic tagging as code</strong></summary>

As code, set `enable_auto_tagging = true` in `envs/dev/env.auto.tfvars` and re-run `make enable-classification ENV=dev`.

</details>

`--groups` are **your own** IdP-synced groups, **one per access tier, most-privileged first** (`payments_ops`=full/raw → `regional_analysts`=region-scoped + masked → `viewers`=least-privileged, with all sensitive columns masked — placeholders; use your real names). GenieRails *consumes* them by exact name, never creates them; the generated policies define each tier's actual access.

`make rehearse` runs **coverage-gate → validate-generated → apply → verify-access** in order, stopping at the first failure. The last step, `verify-access`, proves masking *by effect*: it creates a test SP for each access tier, grants each test SP temporary `CAN_USE` on the selected warehouse, and confirms the unprivileged tier sees masked values while an authorized tier sees raw. No separate warehouse-permission command is required. In dev you **don't touch the exposure gate** — rehearse opens it just for this check (the masks protect the data either way; prod opens it deliberately in Phase 5). `VERIFY_KEY_COLUMN` is the single column used to pair rows across tiers — optional (omit it and the masking check is skipped) but recommended; for tables that don't share one key, pass a [`VERIFY_SPEC` JSON](../../docs/effective-access-verification.md) instead.

</details>

---

<a id="phase-2--prod-set-up-and-promote-rules"></a>
## Phase 2 — Prod: Set up and promote rules

**Run:**

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP="dev_finance=prod_finance"
```

**Then edit:** `envs/prod/auth.auto.tfvars` and `envs/prod/env.auto.tfvars`.

<details>
<summary><strong>Goal, production settings, and promotion behavior</strong></summary>

Create the production configuration, add its credentials and settings, and copy the reviewed rules (masks, access policies, mappings) from dev.

`DEST_CATALOG_MAP` renames each dev catalog to its prod name (`dev_finance=prod_finance`; comma-separate multiple).

Promotion creates `envs/prod/` with configuration templates. Fill in both files:

- **`envs/prod/auth.auto.tfvars`** — the deployment SP `client_id` / `client_secret` + prod workspace host & id. You may reuse the dev SP when both workspaces are in the same Databricks account and it is authorized in prod; use a separate prod SP when your security policy requires environment isolation. Separate Databricks accounts require separate SPs.
- **`envs/prod/env.auto.tfvars`** — don't recreate it; set `sql_warehouse_id` (or leave `""` to auto-create). Promotion has already written the safe classification and access defaults.

**Done when —** `envs/prod/` points at the prod catalog and both production configuration files are filled in.

It carries the **rules** — the mapping, masking functions, access/row-filter policies, and group→tier mapping — and **leaves dev's tag assignments behind** (which columns got tagged is a *fact* about dev's data; prod re-derives its own in Phase 3).

</details>

---

<a id="phase-3--prod-scan-real-data"></a>
## Phase 3 — Prod: Scan real data

**Complete in order:**

1. **UI:** Catalog Explorer → prod catalog → **Enable classification**.
2. **UI:** When the scan finishes, open **Review detections** and exclude false positives; then enable automatic tagging for each reviewed class.

<details>
<summary><strong>Goal, UI guidance, completion check, and as-code alternatives</strong></summary>

Let production scan its own real data and tag its sensitive columns. Real customer PII may surface types that dev data did not contain. The initial scan does not write tags until you enable automatic tagging after review.

<details>
<summary><strong>Alternative — Enable classification as code</strong></summary>

```bash
make enable-classification ENV=prod   # same as step 1a, now on prod
```

This alternative uses the promoted file's `enable_classification = true` safety setting.
</details>

Open [Review detections](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification#review-detections) on the prod catalog and exclude any false positives.

<details>
<summary><strong>Alternative — Enable automatic tagging as code</strong></summary>

Set `enable_auto_tagging = true` in `envs/prod/env.auto.tfvars` and re-run `make enable-classification ENV=prod`.

</details>

**Done when —** prod's `class.*` tags appear on the prod catalog (check in Catalog Explorer / Review detections). The scan is async — grab a coffee ☕ and re-check; nothing yet just means it hasn't finished.

</details>

---

<a id="phase-4--prod-prove-coverage"></a>
## Phase 4 — Prod: Prove coverage

**Run:**

```bash
make certify ENV=prod   # one command: derive-assignments → coverage-gate → validate-generated → apply-governance → audit-rulebook (stops at the first failure)
```

<details>
<summary><strong>Goal, certification behavior, and failure recovery</strong></summary>

Derive prod's protections from its own `class.*` tags, prove coverage, and deploy the enforcement (masks + access policies). The Genie agent itself is not created yet; that happens in Phase 5.

`certify` reuses the exact rules you reviewed in dev — it re-derives *which prod columns* get which protection from prod's own tags, but never regenerates the rules (no model call, so nothing drifts from what you reviewed). It verifies coverage and deploys only the governance protections: masks and access policies. It does not deploy or update the Genie agent. If prod's tags cannot be read, or a detected data type has no protection rule, the command stops instead of applying incomplete protection.

**Done when —** `coverage-gate` exits PASS and `audit-rulebook` reports no uncovered tags.

**If the gate fails or `audit-rulebook` reports drift** — prod surfaced a sensitive tag your promoted rules don't cover (a type the classifier found only in prod, or a rule dropped in promotion). This is a **rule change — made in dev, never hand-edited in prod**. Loop back:

1. **Scaffold the missing mappings** — `make scaffold-treatments ENV=prod` adds a **safe default** (full redaction, marked `REVIEW`) for each tag prod surfaced, so you don't hand-edit anything. Then **review each** — keep the redaction, or set a type-appropriate mask. This changes the shared *rulebook* (not prod's live state), so you validate it in dev and re-promote below.
2. **Re-validate in dev:** `make generate ENV=dev` → `make coverage-gate ENV=dev`.
3. **Re-promote:** `make promote …` (carries the updated rules to prod — same command as [Phase 2](#phase-2--prod-set-up-and-promote-rules)).
4. **Re-run this phase:** `make certify ENV=prod`.

Repeat until the gate passes and drift is clean. The agent stays uncreated and closed to users throughout — that's the point of exposing last.

</details>

---

<a id="phase-5--prod-release-access-and-verify"></a>
## Phase 5 — Prod: Release access and verify

**Set:**

```hcl
# envs/prod/env.auto.tfvars
business_access_enabled = true
```

**Then run in order:**

```bash
make apply ENV=prod    # creates the Genie agent + RELEASES the withheld business SELECT and Genie run access
make verify-access ENV=prod VERIFY_KEY_COLUMN=customer_id   # unprivileged = masked, authorized = raw
```

<details>
<summary><strong>Goal, live verification, completion check, and optional evidence</strong></summary>

With coverage proven, release access, create the agent, and confirm masking live. `verify-access` automatically grants its temporary per-tier test SPs `CAN_USE` on the selected warehouse; it does not change the tier groups' permanent warehouse ACLs.

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
## Phase 6 — Prod: Maintain coverage

**Run on a schedule:**

```bash
make audit-schema ENV=prod
make audit-rulebook ENV=prod
```

**Only after reviewing a finding that needs new assignments:**

```bash
make generate-delta ENV=prod
# review the generated changes
make apply ENV=prod
```

<details>
<summary><strong>Goal, command behavior, and handling findings</strong></summary>

Catch sensitive data that arrives after go-live. The repository includes a scheduled governance job.

- **`make audit-schema ENV=prod`** — flags untagged sensitive-looking columns and stale assignments. A clean run prints `No drift detected.`; a finding lists the columns → classify them (usually via `generate-delta`) and re-apply.
- **`make audit-rulebook ENV=prod`** — flags any prod tag with no covering policy/mask (a rule dropped in promotion, or a brand-new type). A finding means: add the rule in dev, re-generate/validate, re-promote, and re-certify.
- **`make generate-delta ENV=prod`** — *mutating*: removes stale assignments and assigns newly-detected sensitive columns, **constrained to your existing governed tags** (it can't invent a new type). Review the merged assignments, then `make apply`.

A newly-tagged column is a *masking* gap, not an access breach (Unity Catalog granted nothing you didn't ask for). For your most sensitive data, prefer "locked down until proven safe" over "open until tagged."

</details>

---

<a id="reference--commands-concepts-and-glossary"></a>
<details>
<summary><strong>Reference — Commands, concepts, and glossary</strong></summary>

Kept out of this walkthrough so it stays scannable — all in **[REFERENCE.md](REFERENCE.md)**:

- **[Command reference](REFERENCE.md#command-reference)** — every `make` target in one table.
- **[How it works (under the hood)](REFERENCE.md#how-it-works-under-the-hood)** — the exposure gate, the three governance layers, and one-mask-per-column, explained.
- **[Glossary](REFERENCE.md#glossary)** — every term used here (`gr_treatment`, `class.*`, coverage gate, ABAC, …).
</details>
