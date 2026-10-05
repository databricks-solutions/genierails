# GenieRails Dev-to-Prod Walkthrough — ship a Genie agent to production, safely

Take a curated Genie agent in **dev** and ship it to **production** without ever exposing sensitive data. Databricks' built-in classifier — running inside your own workspace — decides *what* is sensitive by sampling your column values (emails, SSNs, card numbers); GenieRails derives *how* it's protected (column masks and access rules) and applies it all as code; and a **coverage check blocks the release** until every classified sensitive column the agent can reach is provably covered. You rehearse the whole thing safely in **dev**, then do it for real in **prod**.

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
| **Curated Genie agent** | The agent you're shipping. In the Genie UI, open the agent, click **Configure**, and copy the **Agent ID** from **About this agent**. It is also in the URL (`.../genie/rooms/01ef7b3c2a4d5e6f`) and goes in `genie_spaces`. |
| **Access-tier group names** | Choose the IdP-synced groups from the shared prerequisite check, ordered most- to least-privileged. Example: `payments_ops` = full/raw; `regional_analysts` = region-scoped + masked; `viewers` = least-privileged + all sensitive columns masked. The generated policies define the actual access, and each agent's tables are `SELECT`-granted only to the tiers authorized to run that agent (a table shared by several agents gets the union). |
| **Row-pairing key** (`VERIFY_KEY_COLUMN`) | A stable, **non-sensitive** id column present on your masked tables (e.g. `customer_id`) — `verify-access` uses it to line up rows. [Details](../../docs/effective-access-verification.md). |

</details>

---

<a id="phase-0--dev-set-up"></a>
<details>
<summary><strong>Phase 0 — Dev: Set up</strong></summary>

**Goal —** create the local config folders, fill in credentials, and point dev at the Genie agent and tables you're shipping.

**1. Create the dev config** — from the `genierails/aws` (or `genierails/azure`) folder you cloned in **Before you start**:

```bash
make setup ENV=dev          # creates envs/dev/ config templates (local only — no Databricks calls)
cp ../shared/examples/dev_to_prod/env.auto.tfvars.example envs/dev/env.auto.tfvars
```

**2. Fill in credentials** — edit **`envs/dev/auth.auto.tfvars`**: the deploying SP `client_id` / `client_secret` + workspace host & id. Step 3 calls Databricks with these.

> **No group setting to change.** `make setup` already writes `manage_groups = false` into the shared **`envs/account/env.auto.tfvars`** (one file for both dev and prod), so GenieRails *consumes* your IdP-synced groups rather than creating them. Leave it as is; set it to `true` only for a demo/greenfield account with no IdP groups.

**3. Point dev at your agent and tables — choose one path.** Both edit `envs/dev/env.auto.tfvars`; keep the template's safety defaults unchanged.

- **Existing Genie agent** — follow [Import a Genie Agent from UI into Code](../../docs/import-genie-agent-from-ui.md) Steps 1–2: add the agent's `genie_space_id` to `genie_spaces`, then run `make generate ENV=dev MODE=genie` to import it. Its tables are discovered into `envs/dev/data_access/discovered_uc_tables.auto.tfvars` automatically, so you **don't** need to list them in `uc_tables`. Set `sql_warehouse_id` (or leave it blank to auto-create).
- **No agent or tables yet** — run the optional [Sample Environment Setup](SAMPLE_ENV.md). It creates sample tables and an agent, then prints the `uc_tables`, `genie_spaces`, and `sql_warehouse_id` snippet to paste into `envs/dev/env.auto.tfvars`.

**Done when —** `envs/dev/auth.auto.tfvars` is filled in and `envs/dev/env.auto.tfvars` has your `genie_spaces` entry — plus either a discovered table list (import path) or the pasted `uc_tables` (sample path).

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
make generate ENV=dev GENERATE_ARGS='--groups "payments_ops,regional_analysts,viewers"'
```
`--groups` are **your own** IdP-synced groups, **one per access tier, most-privileged first** (`payments_ops`=full/raw → `regional_analysts`=region-scoped + masked → `viewers`=least-privileged, with all sensitive columns masked — placeholders; use your real names). GenieRails *consumes* them by exact name, never creates them; the generated policies define each tier's actual access.

**1d. Prove coverage, apply, and verify — one command.**
```bash
make rehearse ENV=dev VERIFY_KEY_COLUMN=customer_id
```
`make rehearse` runs **coverage-gate → validate-generated → apply → verify-access** in order, stopping at the first failure. The last step, `verify-access`, proves masking *by effect*: it creates a test SP for each access tier, grants each test SP temporary `CAN_USE` on the selected warehouse, and confirms the unprivileged tier sees masked values while an authorized tier sees raw. No separate warehouse-permission command is required. In dev you **don't touch the exposure gate** — rehearse opens it just for this check (the masks protect the data either way; prod opens it deliberately in Phase 5). `VERIFY_KEY_COLUMN` is the single column used to pair rows across tiers — optional (omit it and the masking check is skipped) but recommended; for tables that don't share one key, pass a [`VERIFY_SPEC` JSON](../../docs/effective-access-verification.md) instead.

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
<summary><strong>Phase 4 — Prod: Prove coverage</strong></summary>

**Goal —** derive prod's protections from its own `class.*` tags, prove coverage, and deploy the enforcement (masks + access policies). The Genie agent itself isn't created yet — that's Phase 5.

```bash
make certify ENV=prod   # one command: derive-assignments → coverage-gate → validate-generated → apply-governance → audit-rulebook (stops at the first failure)
```

`certify` reuses the exact rules you reviewed in dev — it re-derives *which prod columns* get which protection from prod's own tags, but never regenerates the rules (no model call, so nothing drifts from what you reviewed). A promoted per-column override is merged strictest-wins with the native result, so it can strengthen but never weaken native protection. It also remains active when that governed prod column has no native tag (fail-closed). If its table/column was removed from the declared governed footprint, certification warns and skips the stale rule rather than entering an error loop. Every resulting treatment must still have a promoted mask or certification fails closed. Overrides do not change mask principals, grants, or `SELECT` scope.

It verifies coverage and deploys only the governance protections: masks and access policies. It does not deploy or update the Genie agent. If prod's tags cannot be read, or a detected data type has no protection rule, the command stops instead of applying incomplete protection.

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
<details>
<summary><strong>Phase 5 — Prod: Release access and verify</strong></summary>

**Goal —** with coverage proven, release access, create the agent, and confirm masking live.

```hcl
# envs/prod/env.auto.tfvars
business_access_enabled = true
```
```bash
make apply ENV=prod    # creates the Genie agent + RELEASES the withheld business SELECT and Genie run access
```

Confirm masking live. `verify-access` automatically grants its temporary per-tier test SPs `CAN_USE` on the selected warehouse; it does not change the tier groups' permanent warehouse ACLs.

```bash
make verify-access ENV=prod VERIFY_KEY_COLUMN=customer_id   # unprivileged = masked, authorized = raw (the gate is open now)
```

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

**Goal —** catch sensitive data that arrives after go-live. Run these on a schedule (the repo ships a scheduled governance job):

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
