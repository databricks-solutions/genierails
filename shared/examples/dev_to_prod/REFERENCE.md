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

**One mask per column.** The single enforcement key is **`gr_treatment`** — GenieRails derives exactly **one** value per column from its `class.*` tags (strictest tag wins; a column whose tags need different partial formats, such as a free-text note tagged email, name and phone, is fully redacted), so Unity Catalog's "only one mask may apply per column" rule is never violated.

Single-class free text is a review limit: `notes` tagged only
`class.phone_number` still gets the phone partial mask, exposing its last four
characters and length to the partial tier. Generate warns and the coverage
gate gives a non-blocking advisory for free-text-looking STRING names/comments;
the heuristic never changes output. Use `column_overrides = {
"catalog.schema.table.notes" = { treatment = "redact" } }` for one column, or
`treatment_versions = { phone_partial = { partial = "redacted" } }` for every
column on that treatment.

Compatibility limit: the coverage gate reads multi-class free-text escalation
from the live-refresh class record, `generated/.live_refresh.json`
(`column_classes`). DDL-only envs and pre-#223 records fall back to the
generate marker comment, and a hand-edited record could let a partial mask
through. Run a live refresh through the normal `make apply`/`make rehearse`
path (or `make release-plan`, `make release`, `make maintain`) so the gate
uses live `class.*` tags; never hand-edit the record.

**Why prod keeps the classifier's tags.** The Terraform resource that records tag assignments carries `ignore_changes = all` — a standard Terraform *lifecycle* setting meaning "once these exist, don't change or delete them." That lets the **classifier own the `class.*` tags** in prod: when a scan writes a tag, Terraform leaves it alone instead of reverting it. The classifier owns the tags; GenieRails owns the rules.

---

## Command reference

| Command | Step (0 setup, 1 dev, 2 promote, 3 prod classify, 4 release, 5 maintain) | What it does |
|---|---|---|
| `make setup` / `make init-env ENV=<e>` | 0 | Create local env dirs + default config files (no Databricks calls); in your own repo (an `origin` that isn't `databricks-solutions/genierails`, even with a GenieRails `upstream`) `setup` writes the `.gitignore` block that commits `envs/`, then `git add .gitignore envs/` (`GITIGNORE=customer` forces it, `GITIGNORE=off` skips) |
| `make enable-classification ENV=<e>` | 1/3 | Optional scripted way to turn on UC Data Classification for the footprint; review detections, exclude false positives, and enable auto-tagging in the UI |
| `make generate ENV=<e>` | 1 | (dev) One run: import the agent's config, find its tables, draft masks + access rules from the model, and derive one `gr_treatment`/column from native `class.*` (fail-closed: without `class.*` tags it stops before any model call); groups come from `access_tier_groups` in `env.auto.tfvars` (or `GENERATE_ARGS='--groups "..."'`, saved there on first use). Deterministic envs: every run replaces the column masks, `gr_treatment` tags, `gr_treatment` tag policy and mask-library functions in `generated/` (hand edits to them are lost; change treatments with `column_overrides`, `treatment_versions` or `tag_treatments`); refused in a promoted env (prod, or `promote_from` set): change the source env, then `make promote-to ENV=<e>`. Legacy envs: re-runs keep reviewed rules and add rules only for uncovered columns; `GENERATE_ARGS='--allow-rule-changes'` accepts the model's changes |
| `make derive-assignments ENV=<e>` | 4/5 | Re-derive **only** `tag_assignments` from live `class.*`, reusing the promoted rules unchanged — no model call (fail-closed). `release` and `maintain` run it for you |
| `make coverage-gate ENV=<e>` | 1/4/5 | Fail if any tagged-sensitive column has no mask (the "says NO" check). Every plan/apply also runs it against live tags |
| `make validate-generated ENV=<e>` | 1/4/5 | Static validation incl. the one-mask-per-column guard |
| `make apply ENV=<e>` | 1/4 | (legacy envs; a deterministic env runs it only inside `make rehearse` (dev), and a promoted one refuses it: release through `release-plan`/`release`) Full stack (account → data_access → workspace; auto-promotes same-env first); creates the Genie agent; grants business access only through the coverage check |
| `make apply-governance ENV=<e>` | — | (legacy envs; a deterministic env runs it only inside `rehearse`/`ungovern` (dev) or `maintain` (promoted envs)) Governance-team command: enforcement only (account + data_access); no Genie agent |
| `make genie-adopt-preflight ENV=<e>` | — | Read-only. Before the one-time upgrade to secret-free Genie state, checks that every Genie agent created by an earlier version can be adopted with its current ID (ID file, workspace, GET 200). `make apply` runs it first and stops if any agent fails. |
| `make rehearse ENV=dev` | 1 | (dev) live derive → validate-generated → coverage-gate → apply → verify-access (`_verify-access-gate`; pass: `ALL EFFECTIVE`), stopping at the first failure. Does **not** run `make check`. verify-access picks and proves a row-pairing key per masked table; the proven keys are saved as `verify_key_columns` after a pass (a deterministic env prints them to commit instead, and takes the env lock and writes a rehearse receipt for the commit; unchanged code and dev inputs since a passing rehearsal write the receipt without re-applying) |
| `make release ENV=prod` | 4 | (legacy prod only; a deterministic prod uses the `release-plan` and manifest `release` rows below) Placeholder guard → read-only row-pairing key check (admin only; refuses before the lock) → lock → live derive → validate → coverage → promote → `verify-access-keys` (admin-only proof of every masked table's row-pairing key; refuses before any apply) → read-only rulebook audit → all-layer apply → `verify-access` |
| `make release-plan ENV=prod` | 4 | (deterministic prod) Refuses unless HEAD has a passing clean `make rehearse ENV=dev` receipt (dev is prod's `promote_from` here; a `promote_from = "stg"` prod needs `make rehearse ENV=stg`), the checkout is clean, HEAD is on freshly fetched `origin/main`, the dev fingerprint and the `envs/prod/generated` digest match the receipt, and HEAD isn't older than the last release (unless `prod:rollback:<commit>` is acknowledged on `main`). Then, under the prod lock: live derive into the ignored overlay with deployed policy names kept → validate → coverage → reader and refuse-weakening checks → a saved plan for every root → one immutable manifest. Changes nothing in Databricks |
| `make release ENV=prod` | 4 | (deterministic prod) Releases the current manifest (the one the last `release-plan` wrote), printing its path, commit and changes first: applies only its saved plans (data_access through the staged policy controller) → `verify-access` → release receipt. Refuses, asking for a fresh `release-plan`, if there is no current manifest, it was already released, a newer `release-plan` superseded it, or the commit, a state serial, a saved plan, the effective config or live UC changed. Optional `MANIFEST=<path>` (pipelines) names the manifest explicitly; a superseded or released one refuses. A first release takes several rounds (an import-only manifest if needed, create, switch, grant, then the deferred workspace): each round but the last prints `=== Release phase applied (prod) ===` and `Next: make release-plan ENV=prod, approve the new manifest, make release ENV=prod.`; repeat until `=== Release complete (prod) ===` |
| `make maintain ENV=prod` | 5 | (prod, scheduled; a deterministic env only from the last released commit, with matching digests) audit-schema → derive-assignments → coverage-gate → validate-generated → audit-rulebook → apply-governance; reconciles governance including SELECT for already-covered tables, never widens access past a passing coverage check, and never changes Genie. Skips unchanged inputs, so it does not repair grants revoked outside Terraform |
| `make promote-to ENV=prod` | 2 | Promote **rules only** using `promote_from` and `catalog_map` from prod `env.auto.tfvars` (leaves tag assignments behind). `FROM=`/`CATALOG_MAP=` are optional overrides. Policy names take the prod catalog (`gr_mask_<prod_catalog>_<treatment>`) only when a read-only policy listing of the prod catalog (prod `auth.auto.tfvars`) shows neither the old nor the new name and prod's state doesn't hold the old key; otherwise it keeps its name, since renaming a live policy would drop and recreate it |
| `make promote-to ENV=prod CHECK=1` | PR check | Read-only. Re-runs `promote-to` in a scratch copy of `envs/` with no credentials or Terraform state, from the committed files only, and fails if any committed file it writes differs (diff shown). Writes nothing. Policy names come out as a first promote names them, since the copy has no prod state |
| `make check ENV=<e>` | PR check | Read-only; safe in CI. In a scratch copy of the cloud dir: `validate` → generated files current (deterministic envs: re-split `generated/` and re-record the governed tables, then compare; with `promote_from` set or `ENV=prod`, also `promote-to CHECK=1`; legacy envs skip this with a note) → `validate-generated` → offline coverage check → mask-library unit tests → a **structural** `plan` (committed config against empty state, live tags not re-read; no env has remote state yet). Stops at the first failure and exits non-zero. Never writes `envs/` or calls a Databricks write API. Reads only **committed** env files; not run by `rehearse` or `release-plan` (see the walkthrough's Dev step 2 / Commit it) |
| `make promote SOURCE_ENV=dev DEST_ENV=prod DEST_CATALOG_MAP="dev_cat=prod_cat"` | — | The same promotion with explicit arguments every time (nothing saved); `make promote ENV=<e>` alone splits `generated/` into layers |
| `make verify-access ENV=<e>` | 1/4 | Prove masking by querying as per-tier test principals (**needs the business grants applied**); pass prints `ALL EFFECTIVE`. Standalone re-check; `rehearse` and `release` already run it as `_verify-access-gate`. The row-pairing key is picked per table (optional overrides: `verify_key_columns`, `VERIFY_KEY_COLUMN=<col>`) |
| `make verify-access-keys ENV=<e>` | 4 | Admin only, no grants: pick and prove a row-pairing key for every masked table (`make release` runs a read-only form before its lock, and this before applying) |
| `make audit-rulebook ENV=<e>` | 4/5 | Drift check — tags with no covering rule |
| `make audit-schema ENV=<e>` | 5 | Untagged-column audit (also the first step of `make maintain`) |
| `make generate-delta ENV=<e>` | — | [Legacy] model-based incremental tag assignments; the champion flow uses `make maintain` instead |
| `make evidence ENV=<e>` | 4 | Compliance evidence record; `GENIERAILS_EVIDENCE_INTEGRATION=1` reads live state through the env's warehouse, resolved automatically (`WAREHOUSE_ID=<id>` optional) |

Successful validation reports are compact by default. Add `VERBOSE=1` to a
`make` command to restore the full PASS reports and informational detail;
warnings and failures are always printed in full.

Key config & code: [`treatment_config.json`](../../treatment_config.json) (the `gr_treatment` precedence rules — shared across envs), [`sensitivity_source.py`](../../sensitivity_source.py) (native `class.*` source), [`treatment_derivation.py`](../../treatment_derivation.py) (one treatment/column), [`verify_effective_access.py`](../../verify_effective_access.py) (masked-vs-raw), [`scripts/audit_schema_drift.py`](../../scripts/audit_schema_drift.py) (drift).

### Deterministic-governance settings

These environment-owned settings are validated now. Their values are deliberately
unused until the rollout step shown, so adding them cannot change current behavior.

| Setting | Shape and meaning | Used from step |
|---|---|---|
| `governance_mode` | `"deterministic"` in the champion template and newly scaffolded workspace envs. Omitted still means `"legacy"`, so existing deployments remain unchanged. | 2 |
| `access_tier_groups` | Ordered group names: first sees raw, last sees full masking, and groups between see partial masking. One group is raw-only; two are raw/full. Empty remains the legacy unset value. | 5 |
| `raw_exempt_principals` | Environment-owned principals that see raw, except for never-raw treatments; the deployer SP also sees raw, except for never-raw treatments. | 5 |
| `deployer_principal_id` | Deterministic envs (required): the application ID of the service principal that applies this env, committed (not secret). Generation, validation, `make check` and `promote-to` read it instead of `auth.auto.tfvars`; env-owned (`promote-to` swaps dev's for prod's in the promoted EXCEPT lists and never copies it); rehearse, release-plan, release and maintain refuse before the lock unless `databricks_client_id` matches it. See [access.md](../../docs/access.md). | 8 |
| `treatment_versions` | `{ treatment = { partial = "version" } }`; only `partial` may be selected, `redacted` is valid for every treatment, and treatment-wide `raw` and the deferred keyed hash (`hmac_sha256`) are refused. | 3 |
| `tier_access_overrides` | `{ treatment = { group = "raw" \| "partial" \| "full" } }`; groups must occur in `access_tier_groups`, and the named tier must exist. | 5 |
| `column_overrides` | Per-column `{ partial = "version" }`, `{ treatment = "stricter_treatment" }`, or `{ keep_current = true }`. Setting `full` is refused. | 3 (`keep_current`: 6) |
| `row_filters` | List of `{ table, column, values_by_group = map(group -> list(string)) }`; `table` is mandatory, literals are strings, tier-1 groups are refused, rules on one table are ANDed, and a multi-group caller receives the union of its named values. | 8 |
| `acl_groups` (agent file, or `genie_spaces[*]`) | Explicit `[]` means nobody. Deterministic envs: the only source of `CAN_RUN`, a missing value stops the run, and every group must be able to read the agent's tables through `table_readers`. Legacy: set `require_acl_groups = true` to refuse a missing value. | 5 (deterministic: 8) |
| `table_readers` | Deterministic envs: `{ "cat.sch.tbl" or "cat.sch.*" = [tier groups or raw_exempt_principals] }`, the only source of `SELECT`; a table's readers are the union of matching entries. Env-owned (seeded by the first promote, groups through `group_map`). See [access.md](../../docs/access.md). | 8 |
| `group_map` | Deterministic destinations: `{ dev_group = "prod_group" }`, env-owned. `promote-to` maps tiers (promoted from dev), row-filter groups, policy principals and the first seed of `acl_groups` / `table_readers`; never `raw_exempt_principals`; many-to-one refused; every resulting group must exist (checked by rehearse/release); a change needs `<env>:weaken:group_map:<dev>=<target>` in `ack.txt`. See [access.md](../../docs/access.md). | 8 |
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
are never raw, for every principal including tier 1 and the deployer service
principal; a column carrying any of their classes is never raw even when another
class tag wins strictest-wins. No override may grant them raw values. Tier-1 groups may not appear in `row_filters.values_by_group`.

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
`make check` and `make promote-to CHECK=1` are read-only and run in CI; see
[Pipelines](../../docs/pipelines.md#pipeline-samples-where-each-command-runs) for the
pipeline samples.

---

## Step details

What each walkthrough step does, for when you need more than the [walkthrough](README.md).

**Setup and inputs**
- `make setup ENV=<env>` only creates local files; it makes no Databricks calls.
- Agent tables are discovered from the agent ID, so you don't list `uc_tables`.
- The Agent ID is also in the agent's URL (`…/genie/rooms/<id>`).
- Tiers: e.g. `payments_ops` sees raw values, `regional_analysts` sees region-scoped masked data, `viewers` sees every sensitive column masked. Each agent's tables are `SELECT`-granted only to the tiers allowed to run that agent (a table shared by several agents gets the union). See [architecture](../../docs/architecture.md).
- **Who may run an agent is its `acl_groups`**. **Preferred:** its agent file (`envs/<env>/agents/<agent>.auto.tfvars`). **Also accepted:** the same fields inline in `env.auto.tfvars` as `genie_spaces = [{ ... }]` (leave that list empty when using agent files). It is required: a missing value stops `make generate`, and `[]` means nobody. It grants only Genie `CAN_RUN`; `SELECT` comes only from `table_readers`, and every `acl_groups` group must be able to read the agent's tables through it. New access still waits for a passing coverage check. (Legacy envs only: an agent without `acl_groups` gets each configured group a policy names plus every tier listed above it.)
- `access_tier_groups` is read by every `make generate`. A deterministic promotion maps dev's tier groups through prod's `group_map` and writes them over prod's, so leave prod's placeholders; each promotion records the tiers it wrote, with a digest, in `envs/prod/generated/promoted_access_tiers.json` (commit it; don't edit it). If prod's tiers name a group that is neither in dev's mapped tiers nor in that record, `promote-to` stops before writing and prints the `group_map` line to add; a missing or edited record counts for nothing, so prod's tiers are then all prod's own. In a legacy env the first promote copies them and later promotes keep prod's value. The first `promote-to` also seeds `envs/prod/agents/<agent>.auto.tfvars` with dev's `acl_groups` (through `group_map` in a deterministic env); later promotions keep prod's. In a legacy env, change prod tiers by editing `envs/prod/env.auto.tfvars` in a PR; to change who may run a prod agent, edit its `acl_groups` in `envs/prod/agents/`. GenieRails uses your groups by exact name and never creates them. (CLI alternative: leave it `[]` and pass `GENERATE_ARGS='--groups "a,b,c"'` once; that run saves it. A later `--groups` that differs applies to that run only.)
- `sql_warehouse_id` on an agent's `genie_spaces` entry (file or inline) picks its warehouse; leave it `""` to auto-create one. Agents can share the environment-level warehouse.

**Classification**
- The first scan is asynchronous and can take up to about 24 hours. [Review detections](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification#review-detections) shows what it found.
- The UI path is the default. `make enable-classification ENV=<env>` is a scripted alternative for turning it on; you still review detections in the UI.
- Leave `enable_auto_tagging` out of `env.auto.tfvars` to keep the UI's auto-tagging settings. An explicit `false` is refused while UI auto-tagging is on, unless you pass `ALLOW_DISABLE_AUTO_TAGGING=1`. Scripted auto-tagging: set `enable_auto_tagging = true` and re-run `make enable-classification ENV=<env>`.
- The scripted path can fail with `Usage policy ID must not be empty` on a fresh workspace ([terraform-provider-databricks#5985](https://github.com/databricks/terraform-provider-databricks/issues/5985)). A **Workspace Admin** for that workspace (or a principal with *Serverless usage policy: Manager*) must create a serverless budget/usage policy in the account console and bind it to the workspace; on AWS, Account Admin alone can still receive `missing CreateBudgetPolicyPermission`. Put the policy UUID in `auth.auto.tfvars` as `serverless_usage_policy_id = "<policy-id>"`, then rerun. Leave it empty when the workspace already has a usable policy or when using the UI classification path. Instructions: [AWS](https://docs.databricks.com/aws/en/admin/usage/budget-policies) / [Azure](https://learn.microsoft.com/en-us/azure/databricks/admin/usage/budget-policies).

**`make generate ENV=dev`**
- Imports the agent's config, finds its tables, and drafts masks and access rules from the `class.*` tags. Without tags it stops before any model call.
- Deterministic envs: every run recomputes the column masks, the `gr_treatment` column tags, the `gr_treatment` tag policy and the mask-library functions in `masking_functions.sql` from the `class.*` tags and the mask library (`governance_policies.apply_model`), replacing hand edits to them in `envs/dev/generated/` without a warning; row filters, table tags and any custom SQL above the `-- === GenieRails deterministic mask library ===` marker in `masking_functions.sql` are kept. Change a treatment through [the settings](#deterministic-governance-settings) in `envs/dev/env.auto.tfvars` (`column_overrides`, `treatment_versions`, `tag_treatments`), then re-run `make generate ENV=dev`.
- Legacy envs: re-runs keep reviewed rules in `envs/dev/generated/` and add rules only for uncovered columns, printing `kept reviewed rule …` or `dropped stale reviewed rule …`. To accept the model's changes, pass `GENERATE_ARGS='--allow-rule-changes'` or edit the files.

**`make rehearse ENV=dev`**
- Runs live derive → validate-generated → coverage-gate → apply → verify-access (`_verify-access-gate`), stopping at the first failure. Does **not** run `make check` (run that on committed files first; see the walkthrough).
- `verify-access` creates a test service principal per tier, gives each temporary `CAN_USE` on the warehouse, and checks a bounded sample of rows: unprivileged tiers must see masked values, the authorized tier raw ones. Pass prints `ALL EFFECTIVE`. Standalone: `make verify-access ENV=dev`.
- To pair rows across tiers it picks a key per masked table (single-column primary key, else an untagged id-like column such as `customer_id`), proves it unique, non-null and unmasked, and saves the proven keys as `verify_key_columns` after a pass (a deterministic env prints them for you to commit instead). [Details and overrides](../../docs/effective-access-verification.md#how-genierails-picks-the-row-pairing-key).
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
- Deterministic envs: `make release` applies only the current manifest that `make release-plan` wrote, and a first release takes several rounds (see the [command reference](#command-reference)). The live derive and checks run in `make release-plan`.
- Legacy envs, order: placeholder guard → read-only key check (refuses before the lock) → lock → live derive → validate → coverage check → promote into layers → key proof → read-only `audit-rulebook` → apply all layers → `verify-access`.
- Derivation reuses the reviewed rules and never calls a model. Overrides merge strictest-wins, so they can strengthen but never weaken protection.
- Drift or an audit error stops it before the apply: existing access stays, and no new or wider `SELECT` or Genie run access is granted.
- If it fails after it started applying, only access that passed the coverage check can be applied; follow the printed steps.
- To withdraw access, remove the groups (or set `acl_groups = []`), then run `make apply ENV=prod` (legacy) or commit it and run `make release-plan ENV=prod` and `make release ENV=prod` (deterministic, where `make apply ENV=prod` refuses). `business_access_enabled` is retired; setting it to `false` revokes nothing.
- Evidence (walkthrough Prod step 4, after the release): `GENIERAILS_EVIDENCE_INTEGRATION=1 make evidence ENV=prod` writes a live snapshot to `envs/prod/generated/evidence/`; add `GENIERAILS_EVIDENCE_APPROVED_BY="<you>"` to sign it. The warehouse is resolved from prod's config or applied workspace (`WAREHOUSE_ID=<id>` only pins one). Without the variable it writes a config-only report. Archive it and remove the folder before `make maintain` or the next `release-plan`: both need a clean checkout ([evidence report](../../docs/evidence-report.md)).

**`make maintain ENV=prod`**
- Runs audit-schema → derive-assignments → coverage-gate → validate-generated → audit-rulebook → apply-governance. It audits before applying, keeps `SELECT` for already-covered tables, never widens access past a passing check, and never changes the Genie agent.
- When inputs are unchanged the apply is skipped, so it doesn't repair grants revoked outside Terraform.
- Stops at `audit-schema`: a sensitive-looking column has no `class.*` tag. Review it in Catalog Explorer, then re-run.
- Stops at `coverage-gate` or `audit-rulebook`: add the rule in dev, rehearse, `promote-to`, then commit and push to `origin/main`, `make rehearse ENV=dev` on that commit, `make release-plan ENV=prod`, `make release ENV=prod` (legacy envs have no `release-plan`: `promote-to`, then `make release ENV=prod`; see "Fixing a coverage gap" below).

<a id="fixing-a-coverage-gap-found-in-prod"></a>
**Fixing a coverage gap found in prod**
- The stop names each uncovered `class.*` tag, prints both fixes for this env, and lists the existing treatments whose masking function fits each column's data type.
- Reuse: `make scaffold-treatments ENV=prod TREATMENT=<name>` adds the tag to that treatment's `class_labels` in `shared/treatment_config.json`. Nothing else changes, so prod's deployed policies keep their names. Before writing, it refuses an unknown treatment, a function whose input type doesn't match the column's type (read from `ddl/_fetched.sql`, else live), and a treatment derivation would replace for that column (e.g. `card_last4` on a column that doesn't look like a card number becomes `redact`); the message names the treatment to use. If a type can't be read it refuses too; `ALLOW_UNKNOWN_TYPE=1` overrides once you've checked. If that treatment has no mask in prod's catalog yet, it prints the `materialize-treatment` steps instead of `release`.
- New kind of mask: `make scaffold-treatments ENV=prod` adds a `<class>_redacted` treatment with a full-redaction `REVIEW` stub to `shared/treatment_config.json` and `shared/tag_vocabulary_registry.json`. It also writes a prod preview, which the next `promote-to` replaces. Review the stub.
- Deterministic envs (`governance_mode = "deterministic"`) never change `shared/` for a customer class: `make scaffold-treatments ENV=prod TREATMENT=<mask-library treatment>` writes `"class.<name>" = "<treatment>"` to `tag_treatments` in `envs/dev/env.auto.tfvars` (prod's source), then commit it, `make rehearse ENV=dev`, `make promote-to ENV=prod`, commit and push that, `make rehearse ENV=dev` again on that commit, `make release-plan ENV=prod` and `make release ENV=prod`. Without `TREATMENT` it refuses; `TREATMENT=redact` keeps full redaction. See [the mask library reference](../../REFERENCE.md#customer-classes-tag_treatments).
- `make materialize-treatment ENV=dev TREATMENT=<new>` writes that treatment's mask policy for every governed dev catalog, plus its function, into `envs/dev/generated/`. It adds no tag assignments, so no dev column needs the class. Re-running it changes nothing. A policy or function already there for the treatment is kept as reviewed. It refuses past Unity Catalog's 100 policies per catalog, and refuses prod or any env with `promote_from` (also via `ENV_DIR` or a symlinked env dir). Later `make generate ENV=dev` runs keep the mask.
- Then `make rehearse ENV=dev`, `make promote-to ENV=prod`, commit and push to `origin/main`, `make rehearse ENV=dev` again on that commit, `make release-plan ENV=prod` and `make release ENV=prod` (legacy envs have no `release-plan`: `promote-to`, then `make release ENV=prod`).
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
