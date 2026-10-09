# GenieRails Dev-to-Prod Walkthrough — reference

Lookup companion to the **[Dev-to-Prod Walkthrough](README.md)**: the full command reference, how the enforcement works under the hood, and a glossary of every term. You don't need to read this top-to-bottom — jump in when the walkthrough links you here.

---

## How it works (under the hood)

**Exposure follows the coverage check, mechanically.** Terraform itself holds back exactly the two things that let a user reach data through the agent until a recent passing coverage check (including the first-exposure check) covers them; everything else applies regardless:

| Control | When it applies |
|---|---|
| Table `SELECT` grant | **only after the masks exist and a recent coverage check passes** for it |
| Genie run permission (`CAN_RUN`) | **only once its groups' `SELECT` grants are applied** through that check |
| Workspace assignment + consume entitlement | applied on **every** apply (harmless without `SELECT`/`CAN_RUN`) |
| Warehouse `CAN_USE` | **not needed for business users**: Genie runs queries with the compute credentials of whoever set the agent's warehouse (the deploying service principal), while data access is still checked as each end user ([Databricks docs](https://docs.databricks.com/aws/en/genie/set-up)) |

So "expose last" isn't a policy you hope holds — there is simply no new or wider `SELECT` or `CAN_RUN` without a passing check. There is no on/off flag: the old `business_access_enabled` setting is deprecated and ignored (make warns while it is set; `false` does **not** revoke access). To withdraw access, remove the groups or `acl_groups` entries (or the agent) and apply: that revokes their `SELECT` and the Genie `CAN_RUN` GenieRails granted them, and never waits for the coverage check (other direct entries and inherited permissions on the agent are left alone).

**Three layers of governance, and the Terraform layers that build them:**

| Governance layer (what it controls) | Built by Terraform layer | Contains |
|---|---|---|
| **Who can reach a table** | `account` + `data_access` | groups + the `USE CATALOG → USE SCHEMA → SELECT` grant chain |
| **What they see through it** | `account` + `data_access` | governed tag policies + column masks + row filters (attribute-based access control) |
| **Whether they can open/run the agent** | `workspace` | the Genie agent, its run permissions, workspace assignment + entitlement |

(So the `data_access` Terraform layer covers both *access* and *masking*; the `workspace` layer is the agent itself.)

**One mask per column.** The single enforcement key is **`gr_treatment`** — GenieRails derives exactly **one** value per column from its `class.*` tags (strictest tag wins; a free-text column with multiple tags escalates to full redaction), so Unity Catalog's "only one mask may apply per column" rule is never violated.

**Why prod keeps the classifier's tags.** The Terraform resource that records tag assignments carries `ignore_changes = all` — a standard Terraform *lifecycle* setting meaning "once these exist, don't change or delete them." That lets the **classifier own the `class.*` tags** in prod: when a scan writes a tag, Terraform leaves it alone instead of reverting it. The classifier owns the tags; GenieRails owns the rules.

---

## Command reference

| Command | Step (0 setup, 1 dev, 2 promote, 3 prod classify, 4 release, 5 maintain) | What it does |
|---|---|---|
| `make setup` / `make init-env ENV=<e>` | 0 | Create local env dirs + default config files (no Databricks calls) |
| `make enable-classification ENV=<e>` | 1/3 | Optional scripted way to turn on UC Data Classification for the footprint; review detections, exclude false positives, and enable auto-tagging in the UI |
| `make generate ENV=<e>` | 1 | (dev) One run: import the agent's config, find its tables, draft masks + access rules from the model, and derive one `gr_treatment`/column from native `class.*` (fail-closed: without `class.*` tags it stops before any model call); groups come from `access_tier_groups` in `env.auto.tfvars` (or `GENERATE_ARGS='--groups "..."'`, saved there on first use). Re-runs keep reviewed rules and add rules only for uncovered columns; `GENERATE_ARGS='--allow-rule-changes'` accepts the model's changes |
| `make derive-assignments ENV=<e>` | 4/5 | Re-derive **only** `tag_assignments` from live `class.*`, reusing the promoted rules unchanged — no model call (fail-closed). `release` and `maintain` run it for you |
| `make coverage-gate ENV=<e>` | 1/4/5 | Fail if any tagged-sensitive column has no mask (the "says NO" check). Every plan/apply also runs it against live tags |
| `make validate-generated ENV=<e>` | 1/4/5 | Static validation incl. the one-mask-per-column guard |
| `make apply ENV=<e>` | 1/4 | Full stack (account → data_access → workspace; auto-promotes same-env first); creates the Genie agent; grants business access only through the coverage check |
| `make apply-governance ENV=<e>` | — | Governance-team command: enforcement only (account + data_access); no Genie agent |
| `make genie-adopt-preflight ENV=<e>` | — | Read-only. Before the one-time upgrade to secret-free Genie state, checks that every Genie agent created by an earlier version can be adopted with its current ID (ID file, workspace, GET 200). `make apply` runs it first and stops if any agent fails. |
| `make rehearse ENV=dev` | 1 | (dev) live derive → validate-generated → coverage-gate → apply → verify-access, stopping at the first failure. verify-access picks and proves a row-pairing key per masked table; the proven keys are saved as `verify_key_columns` after a pass |
| `make release ENV=prod` | 4 | (prod) Placeholder guard → read-only row-pairing key check (admin only; refuses before the lock) → lock → live derive → validate → coverage → promote → `verify-access-keys` (admin-only proof of every masked table's row-pairing key; refuses before any apply) → read-only rulebook audit → all-layer apply → `verify-access` |
| `make maintain ENV=prod` | 5 | (prod, scheduled) audit-schema → derive-assignments → coverage-gate → validate-generated → audit-rulebook → apply-governance; reconciles governance including SELECT for already-covered tables, never widens access past a passing coverage check, and never changes Genie. Skips unchanged inputs, so it does not repair grants revoked outside Terraform |
| `make promote-to ENV=prod` | 2 | Promote **rules only** using `promote_from` and `catalog_map` from prod `env.auto.tfvars` (leaves tag assignments behind). `FROM=`/`CATALOG_MAP=` are optional overrides. Policy names take the prod catalog (`gr_mask_<prod_catalog>_<treatment>`) only when a read-only policy listing of the prod catalog (prod `auth.auto.tfvars`) shows neither the old nor the new name and prod's state doesn't hold the old key; otherwise it keeps its name, since renaming a live policy would drop and recreate it |
| `make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP="dev_cat=prod_cat"` | — | The same promotion with explicit arguments every time (nothing saved); `make promote ENV=<e>` alone splits `generated/` into layers |
| `make verify-access ENV=<e>` | 1/4 | Prove masking by querying as per-tier test principals (**needs the business grants applied**); the row-pairing key is picked per table (optional overrides: `verify_key_columns`, `VERIFY_KEY_COLUMN=<col>`) |
| `make verify-access-keys ENV=<e>` | 4 | Admin only, no grants: pick and prove a row-pairing key for every masked table (`make release` runs a read-only form before its lock, and this before applying) |
| `make audit-rulebook ENV=<e>` | 4/5 | Drift check — tags with no covering rule |
| `make audit-schema ENV=<e>` | 5 | Untagged-column audit (also the first step of `make maintain`) |
| `make generate-delta ENV=<e>` | — | [Legacy] model-based incremental tag assignments; the champion flow uses `make maintain` instead |
| `make evidence ENV=<e>` | 4 | Compliance evidence record (`GENIERAILS_EVIDENCE_INTEGRATION=1` + `WAREHOUSE_ID`) |

Successful validation reports are compact by default. Add `VERBOSE=1` to a
`make` command to restore the full PASS reports and informational detail;
warnings and failures are always printed in full.

Key config & code: [`treatment_config.json`](../../treatment_config.json) (the `gr_treatment` precedence rules — shared across envs), [`sensitivity_source.py`](../../sensitivity_source.py) (native `class.*` source), [`treatment_derivation.py`](../../treatment_derivation.py) (one treatment/column), [`verify_effective_access.py`](../../verify_effective_access.py) (masked-vs-raw), [`scripts/audit_schema_drift.py`](../../scripts/audit_schema_drift.py) (drift).

### Deterministic-governance settings

These environment-owned settings are validated now. Their values are deliberately
unused until the rollout step shown, so adding them cannot change current behavior.

| Setting | Shape and meaning | Used from step |
|---|---|---|
| `governance_mode` | `"legacy"` (default) or `"deterministic"`; gates rollout behavior so existing deployments remain unchanged. | 2 |
| `access_tier_groups` | Ordered group names: first sees raw, last sees full masking, and groups between see partial masking. One group is raw-only; two are raw/full. Empty remains the legacy unset value. | 5 |
| `raw_exempt_principals` | Environment-owned principals that see raw, except for never-raw treatments; the deployer SP is always exempt. | 5 |
| `treatment_versions` | `{ treatment = { partial = "version" } }`; only `partial` may be selected, `redacted` is valid for every treatment, and treatment-wide `raw` is refused. | 3 |
| `tier_access_overrides` | `{ treatment = { group = "raw" \| "partial" \| "full" } }`; groups must occur in `access_tier_groups`, and the named tier must exist. | 5 |
| `column_overrides` | Per-column `{ partial = "version" }`, `{ treatment = "stricter_treatment" }`, or `{ keep_current = true }`. Setting `full` is refused. | 3 (`keep_current`: 6) |
| `hash_fallback` | Unset for a hard stop, or `"redact"` to explicitly fail closed when keyed hashing is unavailable. | 3 |
| `row_filters` | List of `{ table, column, values_by_group = map(group -> list(string)) }`; `table` is mandatory, literals are strings, tier-1 groups are refused, rules on one table are ANDed, and a multi-group caller receives the union of its named values. | 8 |
| `genie_spaces[*].acl_groups` | Explicit `[]` means nobody. Set `require_acl_groups = true` to refuse a missing value now; step 5 makes that rule the default. | 5 |
| `genie_spaces[*].delete` | `true` requests deletion instead of the default detach when an agent is removed. | 9 |
| `ACK_UNCLASSIFIED` | Environment variable formatted as comma-separated `cat.sch.tbl.col` entries. Validation is format-only until the completeness check lands. | 2 |
| `ACK_WEAKEN` | Environment variable formatted as comma-separated `cat.sch.tbl.col:principal` entries. Validation is format-only until refuse-weakening lands. | 7 |

Resolution first fixes access: tier 1 is raw, the last tier and out-of-tier
principals are full, and the most privileged group membership wins. A group
access override can change only an intermediate tier. Only when the resulting
access is partial does version precedence apply: `column_overrides`, then
`treatment_versions`, then the shipped library default. A column override never
changes what the full tier sees. Checking that a `treatment` column override is
strictly stronger needs class-derived protection data and lands in step 3;
`keep_current` is interpreted by migration in step 6.

The treatments `card_security_code`, `card_pin`, `card_track_data`, and `secret`
are never raw. No override may grant them raw values; only the deployer service
principal is exempt. Tier-1 groups may not appear in `row_filters.values_by_group`.

### Applying from CI

GenieRails v1 runs every Terraform apply on the deployment machine. Applying
targets—including `enable-classification`, `rehearse`, `release`, `maintain`,
`apply`, `apply-governance`, `apply-genie`, `_apply-layer`, `destroy`,
`destroy-governance`, `destroy-genie`, `_destroy-layer`, `import`,
`migrate-state`, `integration-test`, `test-champion`, `test-all`, `test-ci`, and
`test-ci-parallel`—refuse common CI markers (`CI=true/1/yes`, Azure Pipelines,
Jenkins, GitLab, Buildkite, or CircleCI). CI may continue to run plans,
validation, unit tests, coverage checks, and audits. The
`GENIERAILS_ALLOW_CI_APPLY=1` escape hatch is an internal switch used only by
GenieRails' own throwaway integration-test jobs; never set it on a deployment
job. Run `make release ENV=prod` from the persistent deployment workspace.

---

## Step details

What each walkthrough step does, for when you need more than the [walkthrough](README.md).

**Setup and inputs**
- `make setup ENV=<env>` only creates local files; it makes no Databricks calls.
- Agent tables are discovered from the agent ID, so you don't list `uc_tables`.
- The Agent ID is also in the agent's URL (`…/genie/rooms/<id>`).
- Tiers: e.g. `payments_ops` sees raw values, `regional_analysts` sees region-scoped masked data, `viewers` sees every sensitive column masked. Each agent's tables are `SELECT`-granted only to the tiers allowed to run that agent (a table shared by several agents gets the union). See [architecture](../../docs/architecture.md).
- **The order of `access_tier_groups` grants access.** An agent's access list (Genie run permission, plus `SELECT` on its tables) is each of your configured groups that a mask or filter policy names, **plus every tier listed above any such group**. Built-in principals such as `account users` are never added. A tier a policy doesn't name isn't masked by it, so it sees those columns raw. Tiers below every named group aren't added. If a space sets `acl_groups` explicitly (even `[]`), that list wins and nothing is added. New access from this still waits for a passing coverage check.
- `access_tier_groups` is read by every `make generate`. The first promote copies it to prod; later promotes keep prod's value. To change prod tiers or a space's `acl_groups`, edit `envs/prod/env.auto.tfvars` in a PR. GenieRails uses your groups by exact name and never creates them. (CLI alternative: leave it `[]` and pass `GENERATE_ARGS='--groups "a,b,c"'` once; that run saves it. A later `--groups` that differs applies to that run only.)
- `sql_warehouse_id` on an agent's `genie_spaces` entry picks its warehouse; leave it `""` to auto-create one. Agents can share the environment-level warehouse.

**Classification**
- The first scan is asynchronous and can take up to about 24 hours. [Review detections](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification#review-detections) shows what it found.
- The UI path is the default. `make enable-classification ENV=<env>` is a scripted alternative for turning it on; you still review detections in the UI.
- Leave `enable_auto_tagging` out of `env.auto.tfvars` to keep the UI's auto-tagging settings. An explicit `false` is refused while UI auto-tagging is on, unless you pass `ALLOW_DISABLE_AUTO_TAGGING=1`. Scripted auto-tagging: set `enable_auto_tagging = true` and re-run `make enable-classification ENV=<env>`.
- The scripted path can fail with `Usage policy ID must not be empty` on a workspace without a serverless usage policy ([terraform-provider-databricks#5985](https://github.com/databricks/terraform-provider-databricks/issues/5985)). Use the UI, or attach a serverless usage policy first (creating one needs Workspace Admin, or *Serverless usage policy: Manager*; [AWS](https://docs.databricks.com/aws/en/admin/usage/budget-policies) / [Azure](https://learn.microsoft.com/en-us/azure/databricks/admin/usage/budget-policies)).

**`make generate ENV=dev`**
- Imports the agent's config, finds its tables, and drafts masks and access rules from the `class.*` tags. Without tags it stops before any model call.
- Re-runs keep reviewed rules in `envs/dev/generated/` and add rules only for uncovered columns, printing `kept reviewed rule …` or `dropped stale reviewed rule …`. To accept the model's changes, pass `GENERATE_ARGS='--allow-rule-changes'` or edit the files.

**`make rehearse ENV=dev`**
- Runs live derive → validate-generated → coverage-gate → apply → verify-access, stopping at the first failure.
- `verify-access` creates a test service principal per tier, gives each temporary `CAN_USE` on the warehouse, and checks a bounded sample of rows: unprivileged tiers must see masked values, the authorized tier raw ones.
- To pair rows across tiers it picks a key per masked table (single-column primary key, else an untagged id-like column such as `customer_id`), proves it unique, non-null and unmasked, and saves the proven keys as `verify_key_columns` after a pass. [Details and overrides](../../docs/effective-access-verification.md#how-genierails-picks-the-row-pairing-key).
- Dev keeps business access after rehearse; the masks protect the data either way.

**`make promote-to ENV=prod`**
- Reads `promote_from` (default `dev`) and `catalog_map` from the target's `env.auto.tfvars`; `FROM=` and `CATALOG_MAP=` override them, and a successful promote saves the values you passed. One map entry per catalog; the old `"dev=prod"` string form still works.
- Promoted override columns are renamed through the catalog map; they never carry or widen ACLs. Re-promoting never closes access that's already live.
- Don't recreate prod's `env.auto.tfvars`: `make setup ENV=prod` seeds it with `promote_from` and a placeholder `catalog_map`, and promotion keeps prod's own settings.
- Copies the rules: masking functions, policies, the group-to-tier mapping and reviewed `treatment_overrides`. It leaves dev's tag assignments behind, because prod derives its own from its own data. It carries the proven row-pairing keys, renamed to prod's catalogs, and keeps prod's own agent ID, groups and warehouse.
- Staging chain: set `promote_from = "dev"` in `envs/stg`, promote stg, then `promote_from = "stg"` in `envs/prod`.
- `make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP=…` is the same promotion with explicit arguments every time.
- Prod's service principal can be the dev one if both workspaces are in the same account and it's authorized in prod; use a separate one if your policy requires isolation. Separate Databricks accounts need separate service principals.

**`make release ENV=prod`**
- Order: placeholder guard → read-only key check (refuses before the lock) → lock → live derive → validate → coverage check → promote into layers → key proof → read-only `audit-rulebook` → apply all layers → `verify-access`.
- Derivation reuses the reviewed rules and never calls a model. Overrides merge strictest-wins, so they can strengthen but never weaken protection.
- Drift or an audit error stops it before the apply: existing access stays, and no new or wider `SELECT` or Genie run access is granted.
- If it fails after it started applying, only access that passed the coverage check can be applied; follow the printed steps.
- To withdraw access, remove the groups (or set `acl_groups = []`) and run `make apply ENV=prod`. `business_access_enabled` is retired; setting it to `false` revokes nothing.
- Evidence: `make evidence ENV=prod` writes a config-based report to `envs/prod/generated/evidence/`. For a live, signed snapshot: `GENIERAILS_EVIDENCE_INTEGRATION=1 GENIERAILS_EVIDENCE_APPROVED_BY="<you>" make evidence ENV=prod WAREHOUSE_ID=<id>`.

**`make maintain ENV=prod`**
- Runs audit-schema → derive-assignments → coverage-gate → validate-generated → audit-rulebook → apply-governance. It audits before applying, keeps `SELECT` for already-covered tables, never widens access past a passing check, and never changes the Genie agent.
- When inputs are unchanged the apply is skipped, so it doesn't repair grants revoked outside Terraform.
- Stops at `audit-schema`: a sensitive-looking column has no `class.*` tag. Review it in Catalog Explorer, then re-run.
- Stops at `coverage-gate` or `audit-rulebook`: add the rule in dev, rehearse, `promote-to`, `release` (see "Fixing a coverage gap" below).

**Fixing a coverage gap found in prod**
- `make scaffold-treatments ENV=prod` writes a suggested rule into prod's `generated/abac.auto.tfvars` and `generated/masking_functions.sql`, plus the shared mapping in `shared/treatment_config.json` and `shared/tag_vocabulary_registry.json`. The next `promote-to` replaces prod's generated files from dev, so the prod-local rule is only a preview: the rule has to live in dev.
- Preferred: tag a matching column in dev and run `make generate ENV=dev`. It uses the mapping the prod scaffold added to `shared/treatment_config.json` and drafts the policy and masking function. (`scaffold-treatments ENV=dev` would do nothing here: it only acts on labels that aren't mapped yet.)
- No matching dev column: copy the new mask policy into `envs/dev/generated/abac.auto.tfvars` (with the dev catalog) and its function into `envs/dev/generated/masking_functions.sql`. Don't copy prod's `tag_assignments`.
- `make generate ENV=dev` isn't enough when dev has no column with that class: the mapping exists, but no dev policy is created for promotion.
- Then `make rehearse ENV=dev`, `make promote-to ENV=prod`, `make release ENV=prod`.
- A newly tagged column is a masking gap, not an access breach. For your most sensitive data, prefer "locked down until proven safe" over "open until tagged".

---

## If something stops

Every command prints what to do when it stops. The common cases:

| Message | What to do |
|---|---|
| No `class.*` tags yet | Finish the Catalog Explorer review and auto-tagging, wait, re-run |
| Coverage check failed / rulebook drift | See "Fixing a coverage gap found in prod" above |
| No provable row-pairing key for a table | Set `verify_key_columns = { "<cat.sch.tbl>" = "<column>" }` ([how keys are picked](../../docs/effective-access-verification.md#how-genierails-picks-the-row-pairing-key)) |
| Genie agent ID file missing | Follow the printed recovery steps; never re-create the agent by hand |
| Env is locked | Another `release`/`maintain` is running; wait for it |

To change who has access in prod, edit `envs/prod/env.auto.tfvars`, commit it, and let your deployment pipeline apply it. Don't change access by hand in the UI.

---

## Glossary

- **access tier** — a group of users who should see data at the same level (e.g. full / masked / least). You map one IdP group to each tier.
- **ABAC (attribute-based access control)** — masks/filters that apply based on a column's *tag*, not its name — so a rule covers any column carrying that tag.
- **`CAN_RUN` / `CAN_USE`** — Databricks permissions: `CAN_RUN` lets a group open and run a Genie agent (granted only through the coverage check); `CAN_USE` lets a principal run a SQL warehouse (needed by the deploying service principal and by `verify-access`'s temporary test principals, not by Genie end users).
- **`class.*` tag** — a tag Unity Catalog's classifier writes on a column it finds sensitive (e.g. `class.email_address`).
- **coverage check** — `make coverage-gate`; the blocking check that fails if any tagged-sensitive column has no covering mask/policy. The "tool says NO" step.
- **drift** — a gap between what's tagged and what's protected; `audit-rulebook` reports it.
- **entitlement / workspace assignment** — what lets a group *into* a workspace at all (applied every apply; harmless without a data grant).
- **evidence** — the compliance record `make evidence` produces (what was scanned, tagged, protected, and approved).
- **exposure gate (retired)** — the old on/off `business_access_enabled` switch, now ignored. Exposure follows the coverage check instead, which Terraform enforces: no new or wider `SELECT` grant or Genie run permission without a recent pass.
- **facts vs rules** — *facts* = which columns got tagged in *this* workspace (from the scan); *rules* = the mapping + policies (portable, promoted).
- **fail-closed** — if native classification can't be read, `generate` aborts rather than guessing.
- **FGAC (fine-grained access control)** — Unity Catalog column masks + row filters.
- **footprint** — the exact tables the agent can reach (your `uc_tables` / Genie agent tables).
- **Genie agent** — the Databricks Genie experience users query; "the agent." *(Formerly "Genie space"; the config key and API id are still `genie_spaces` / `genie_space_id`.)*
- **`gr_treatment`** — the one GenieRails-owned tag whose value picks a column's mask.
- **grant chain** — `USE CATALOG → USE SCHEMA → SELECT`, the layered grants needed to read a table.
- **IdP (identity provider)** — Entra ID / Okta; **AIM / SCIM** are how it syncs groups into Databricks. GenieRails consumes those groups.
- **masking** — transforming a sensitive value for unauthorized tiers (e.g. card → `****-****-****-4464`) while authorized tiers see the raw value.
- **principal** — an identity a query runs as (a user, group, or service principal); `verify-access` uses temporary test principals per tier.
- **rulebook / rules** — the mapping `class.* → gr_treatment → mask` plus the access/row-filter policies (the portable, promoted part).
- **row filter** — a rule that limits *which rows* a tier can see (business logic; not every flow uses one).
