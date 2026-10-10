# Design: deterministic governance, decoupled from Genie agents

Status: **agreed**, revised after the feasibility review (section 10 lists what changed).

## Goal

Table governance (which columns are masked, how, and for whom) is computed only from classification tags and reviewed configuration. It is the same for every Genie agent and needs no AI. Genie agents only add their own configuration and access.

Today the AI drafts parts of governance in every `make generate`: mask SQL for six treatments, which groups each mask applies to, and row filters. Agent access is then derived from those AI-chosen groups, which couples the two lifecycles.

## Decisions (agreed)

| # | Decision |
|---|---|
| 1 | Three access tiers by default: full access (raw), partial (partial mask), fully masked. |
| 2 | Identifiers (national, tax, document and device IDs) default to a one-way hash for the partial tier, and full redaction for the fully masked tier. |
| 3 | Row filters are supported only when declared in config. GenieRails never invents one. |
| 4 | An agent can only be run by the groups assigned to it (`acl_groups`). There is no default; a missing `acl_groups` stops the run. |
| 5 | The AI is not used for governance. It stays available as a helper for Genie content and for suggesting a mask for an unmapped tag, which a person reviews and commits. |

## 1. Table governance

For every table any configured agent uses:

1. **Treatment per column** comes from its `class.*` tag through the shipped mask library (section 2). A per-column `column_overrides` entry can make it stricter, or set an explicit, reviewed partial version for that column (e.g. a postcode prefix); it never changes what tier 3 sees.
2. **Per catalog and treatment, one policy per masked tier** (section 3), each using a fixed, caller-independent function from the library.
3. **Who sees what** comes from the tier rule (section 3).
4. **Row filters** only where declared (section 4).
5. **Coverage check** unchanged: a `class.*` tag with no mapping blocks new access until a mapping is added (`make scaffold-treatments`).

Same inputs give the same output. Re-running changes nothing. Adding an agent whose tables are already governed changes nothing in governance.

## 2. The mask library

GenieRails ships a mapping for all 93 Databricks classification classes, each to a treatment with a fixed, reviewed **partial** and **full** version (typed variants where the column isn't text). Customers can override any treatment in config; overrides survive upgrades. Customer-defined tags (e.g. `sensitivity=confidential`) can be mapped to a treatment in config.

| Classes | Partial (tier 2) | Full (tier 3) |
|---|---|---|
| `credit_card` | last 4 | redacted |
| `card_security_code`, `card_pin`, `card_track_data`, `card_expiration_date` | redacted | redacted |
| `bank_number`, `iban_code`, `swift_code`, `us_bank_number`, `uk_sort_code` | last 4 | redacted |
| `us_ssn`, `us_itin`, `us_passport`, `us_driver_license`, `passport`, `driver_license` | one-way hash | redacted |
| All regional national, tax, social, health and voter IDs (`ae_*`, `au_*`, `br_*`, `ca_*`, `ch_*`, `de_*`, `dk_*`, `es_*`, `fr_*`, `il_*`, `in_*`, `it_*`, `jp_*`, `mx_*`, `nl_*`, `no_*`, `se_*`, `uk_nhs`, `uk_nino`, `uk_utr`) | one-way hash | redacted |
| `health_plan_beneficiary_number`, `medical_record_number`, `medical_license`, `medical_device_id`, `imei`, `vin`, `license_plate` | one-way hash | redacted |
| `email_address` | partial (`j***@example.com`) | redacted |
| `phone_number` | last 4 | redacted |
| `name` | initials | redacted |
| `date_of_birth`, `medical_date` | year only | NULL |
| `age` | 10-year band | NULL |
| `credit_score` | 50-point band | NULL |
| `compensation` | rounded | NULL |
| `ip_address` | network only (last octet zeroed) | redacted |
| `mac_address` | vendor prefix only | redacted |
| `url` | domain only | redacted |
| `location`, STRING columns (addresses, free text) | redacted | redacted |
| `location`, numeric latitude/longitude (DOUBLE or DECIMAL) | rounded to 1 decimal place (about 11 km) | NULL |
| `health_data`, `biometric_data`, `genetic_data`, `ethnicity`, `religious_belief`, `political_opinion`, `sexual_data`, `sexual_orientation`, `trade_union_membership`, `criminal_background`, `marital_status`, `employment_status` | redacted | redacted |
| `secret` | redacted | redacted |

"Redacted" means a fixed placeholder for text and NULL for other types. The one-way hash is a **keyed** hash (HMAC-SHA-256). A plain hash is not acceptable: small ID spaces such as SSNs (about a billion values) can be reversed by trying every value. Implementation (Databricks' documented pattern):

- The key's source of truth is one deployment secret generated once by `make init-hash-key` and held by the operator as `GENIERAILS_HASH_KEY` (section 11, G8). `rehearse` and `release` create a **Unity Catalog secret** in their own catalog's governance schema from it **if absent**; once the UC secret exists, the env var isn't needed. Dev and prod use identical key bytes, proven by comparing a probe hash. The key never appears in git, Terraform state or logs. Nothing about the key is needed until a treatment actually uses the hash.
- A `LANGUAGE PYTHON` UDF declared with `SECRETS (...)` computes the HMAC; a SQL wrapper is the mask function. Neither is declared `DETERMINISTIC`, because rotating the key changes every output.
- Input is normalised before hashing: trimmed, Unicode NFKC, upper-cased, and spaces and hyphens removed. NULL stays NULL.
- **Fail closed:** if UC secrets or Python UDFs are unavailable on the warehouse, hashed treatments fall back to **redacted** for tier 2, with a clear warning. They never fall back to raw.
- **Verified live on AWS dev (spike):** UC secret creation, the Python HMAC UDF with `SECRETS (...)` and `environment_version = '6'`, and the SQL wrapper as a column mask all work on a serverless Pro warehouse. Callers need only `EXECUTE`; the secret is read with the function owner's permission. Output was stable 64-character hex, NULL stayed NULL, duplicates matched.
- **Read the key once at module scope** (`HANDLER` function, key fetched outside it): verified to work through ABAC with a SQL wrapper, and it cut overhead by about 60%.
- **Latency (measured, AWS dev serverless):** about **8 seconds of fixed overhead per query** that touches a hashed column, roughly the same at 10k and 100k rows; unmasked queries took about 0.6 s. Only tier-2 queries touching hashed columns pay this (tier 1 sees raw; tier 3's full version is plain SQL). A team can switch a treatment to `redacted` for tier 2 with `treatment_versions` if latency matters more than joinability.
- **Decision (2026-10-10): the keyed hash stays the tier-2 default for identifier classes** (`identifier_partial_default = "hmac_sha256"`), accepting the ~8 s per-query overhead in exchange for joinable, countable IDs. `redacted` remains a per-treatment opt-out via `treatment_versions`. Plain built-in hashes (`sha2`, `md5`, `xxhash64`) were rejected: identifier spaces like SSNs are small enough to reverse by hashing every value, and a salt embedded in the function body would live in git and may be readable via the function definition.
- **Tier shape verified live with real groups:** in no tier gives full; tier 2 only gives the hash; tier 1 only gives raw; tier 1 + tier 2 gives raw; tier 2 + another group gives the hash. Exactly one mask resolved every time, with no "multiple masks" error.
- **Group membership propagation:** about **5 minutes** before ABAC sees a membership change. `verify-access` waits for it.
- **Tag policy capacity:** the AWS test account has reached its maximum number of governed tag policies. GenieRails uses a single treatment tag key, so no new tag policy is needed per treatment, but a deployment that needs a new tag policy will fail on such an account; bootstrap reports remaining capacity.
- **Rotation** is explicit and disruptive (`make rotate-hash-key`): every hash changes at once, so stored or exported hashes stop joining. It is documented as a planned cutover.

**Change from today:** `us_ssn` and `us_itin` move from last 4 to the keyed hash, following decision 2. Last 4 stays available as an explicit opt-in:

```hcl
treatment_versions = { ssn = { partial = "last4" } }
```

**Exact behaviour contract, per function** (enforced by unit tests on every function): NULL in gives NULL out; malformed input gets the full version; IPv4 keeps the /24 network, IPv6 keeps the /64 prefix; numbers round half away from zero; DATE becomes 1 January of the year, TIMESTAMP becomes the start of the year in UTC; bands are inclusive on the lower bound; NaN and infinity become NULL. Supported column types are listed per treatment; any other type gets the full version.

**Location opt-ins.** Where a customer knows exactly what a column holds, a per-column override sets the partial version, reviewed in git like any governance change:

```hcl
column_overrides = {
  "cat.sch.customers.postcode" = { partial = "prefix_3" }   # e.g. "200***"
  "cat.sch.customers.city"     = { partial = "raw" }
}
```

A column override may only set the **partial** version; any attempt to set the full version is refused.

## 3. The tier rule

```hcl
access_tier_groups = ["payments_ops", "regional_analysts", "viewers"]  # full, partial, fully masked
```

- First group: raw values. Last group: full version. Any groups in between: partial version.
- Two groups: raw and full. One group: raw only.
- **Policy shape.** For each catalog and treatment GenieRails creates up to two policies, each with a caller-independent function:
  - **partial policy**: `TO <tier-2 groups>` `EXCEPT <tier-1 groups>`;
  - **full policy**: `TO account users` `EXCEPT <tier-1 and tier-2 groups>`.

  So exactly one mask resolves for every user, a user in several tiers gets the **most privileged** one, and anyone outside the tiers who somehow has `SELECT` gets the full version. **Precedence for who sees raw:** never-raw treatments (G11) beat everything except the deployer SP; then `raw_exempt_principals` (G6) and tier 1 see raw; overrides naming a never-raw treatment are refused.
- **Group-level override**, e.g. let tier 2 see email raw (moves those groups into the EXCEPT list of the partial policy):

```hcl
tier_access_overrides = {
  email_partial = { regional_analysts = "raw" }
}
```

**Precedence**, highest first: `column_overrides` → `treatment_versions` → `tier_access_overrides` → library default. An override naming a tier that doesn't exist in a one- or two-group setup is refused.

## 4. Row filters (declared only)

A row filter hides whole rows, e.g. regional analysts only see APAC rows. GenieRails creates one only when configured:

```hcl
row_filters = [
  { table = "cat.sch.payments", column = "region_code",
    values_by_group = { regional_analysts = ["APAC"] } }
]
```

- No configuration means no row filters, and a missing row filter is never a coverage gap.
- All declared rules for one table compile into **one** filter function and policy (Unity Catalog allows only one row filter to resolve per user and table). Several rules on a table combine with AND.
- Groups named in `values_by_group` see only their values (case-sensitive, string literals, NULL never matches). Only tier-1 groups, `raw_exempt_principals` and the deployer SP see all rows; any other group not named sees **no** rows (fail closed, matching masks). A tier-1 group named in `values_by_group` is refused. Each table's filter is an ABAC policy on that **table** securable.
- A user in several named groups sees the union of their values.

## 5. Agent lifecycle

- **Config**: curated in the dev UI and captured into git on purpose with `make capture ENV=dev SPACE="<agent>"` (section 9); code-owned in prod.
- **Access**: each agent lists `acl_groups`. Those groups get `CAN_RUN` on the agent and `SELECT` on its tables; a table shared by several agents gets the union. Still gated by the coverage check.
- **`acl_groups` is environment-owned, not captured.** The first `promote-to` seeds prod's value from dev; later promotions keep prod's own value and print the dev/prod differences (as #81 does today).
- **Missing `acl_groups`**: `generate` and `release` stop with "agent X has no acl_groups — list the groups that may run it". An explicit `[]` is allowed and means nobody.
- **New table**: governed by section 1 first, then the agent gets access.
- Agent access is never derived from mask policies.

## 6. Migration for existing deployments

- The first `generate` after upgrading writes a **migration report**: for every column and **every principal holding SELECT** (tier groups, exempt principals, owners, service principals), what it sees today and what it will see. It also lists agents missing `acl_groups`.
- **Refuse weakening:** if any principal would see a column less protected than today, migration stops and names it. Changing that needs an explicit override.
- **Staged cutover:** new library functions are created under new names first; policies change in the tighten-before-loosen phases of section 11 (G2), never dropped then recreated; old functions are kept until no policy uses them.
- Nothing is applied until accepted (`ACCEPT_MIGRATION=1`), and a fresh classification read plus tier-aware verification runs before the cutover is reported done.
- Rollback reverts the commit and releases; the old functions are still there, so masks switch back in place.

## 7. Technical points to resolve in implementation

- **Tier-aware verification (required).** `verify-access` checks each tier against its **exact expected output**, not just "differs from raw": it reads paired raw rows, applies the pure partial or full function to them, and compares exactly. Functions stay caller-independent (section 3), so #97's fixed-point handling still applies. A sample where raw, partial and full can't be told apart is **inconclusive**, never a pass. A test principal in two tiers must see the most privileged tier's output.
- **Typed variants**: partial and full versions for DATE, TIMESTAMP, numeric and STRING where applicable.
- **Hash key**: as specified in section 2.

## 8. Rollout (separate PRs, each reviewed by a different vendor)

Every step keeps `main` working. Steps 1–4 change existing deployments only in **warn mode** (gated on `governance_mode`; see N5). **No existing deployment is cut over until step 6 (migration) lands**; until then the new generator only runs for envs with `governance_mode = "deterministic"` and no deployed AI-drafted policies.

1. **Schemas + CI guard:** settings, precedence and validation; every applying target refuses when `CI=true` (N3).
2. **Tier-aware verification** and the prod-classification completeness check with the promote-side manifest (G4, N4); warn-only for existing envs.
3. **Mask library + keyed hash:** all 93 classes, typed functions with exact output tests, never-raw treatments, `init-hash-key` (a no-op until a hashed treatment is used); latency spike at 10k/100k rows and on Azure.
4. **Stable tags and sticky governance** for deterministic envs: separate treatment-tag resource keyed by `table.column`, persisted governed-table set, `make ungovern` (G1, N2).
5. **Deterministic policies (new envs only)** together with **required `acl_groups` and no policy-derived access** (N1): per-tier policies, `raw_exempt_principals`, tighten-before-loosen phases, per-agent files with the loader (G14, N7), tiers promoted as governance (G7).
6. **Migration:** protection-order report over every SELECT holder, refuse weakening, tag `state mv`, staged cutover keeping deployed policy names, frozen AI row filters (G3, G11, N2).
7. **Version control:** root `.gitignore` commits env files and generated output (G15, N8); permanent refuse-weakening on every promote and release (N9); `maintain` fail-safe redaction for unmapped new classes (N10).
8. **Declared row filters** on table securables (replace frozen AI ones only when declared).
9. **Capture + prod ownership:** `make capture`, JSON-aware remap on promote, committed prod agent IDs, drift report, detach by default (G9, G12, G13, N15, N16).
10. **Per-agent live snapshot** in rehearse, per-agent fingerprints and per-table SELECT gates (G10, N11).
11. **Team workflow (section 13):** remote Terraform state with locking for dev; a post-merge `rehearse ENV=dev` job (the only CI-guard exception); a rehearsal record; `promote-to`/`release` refuse a `main` commit without a passing rehearse; a stale-promotion-PR check; sample GitHub and Azure DevOps workflows.
12. **Live tests on AWS** for every scenario in section 9, then Azure, then docs.

## 9. Change lifecycle (agreed)

There are two kinds of change, and git is the record of what is ready.

| | Governance | Agent |
|---|---|---|
| Covers | mask library, tier rule, row filters, mappings for new tags | one agent's instructions, examples, SQL snippets, table list, `acl_groups` |
| Belongs to | tables (the same for every agent) | one agent |
| Changed in | files in git | the dev Genie UI, then captured into git |
| Reaches prod | when merged and released | when that agent is captured, merged and released |

### Rules

1. **Capture is explicit and per agent.** `make capture ENV=dev SPACE="<agent>"` writes that agent's full exported config into git. A plain `make generate ENV=dev` only imports agents not yet in git; it never re-captures an existing agent. Unfinished UI work therefore never reaches git or prod.
2. **Dev agent config is never overwritten.** In dev, GenieRails applies governance and access, and creates an agent only if it does not exist. In prod, every `release` overwrites the agent's config from git (full replacement); it never recreates the agent or changes its ID, so conversations are kept.
3. **One pipeline.** A promotion PR (`make promote-to ENV=prod`) carries whatever is in git, governance changes, agent changes or both, and shows which. After it is approved and merged, **the operator runs `make release ENV=prod` on the deployment machine** (v1 runs every applying target there; CI runs tests and read-only checks only — section 11, G5/N3).
4. **Removing an agent or table removes access only.** Masks stay; deleting a mask is a deliberate governance change.
5. **Dev governance covers every table a configured live dev agent uses,** including tables added in the UI but not captured yet, so work in progress is protected in dev. During `rehearse`, GenieRails reads each configured dev agent through the Genie API, records its ID, a digest of its config and its table list, and includes that in the coverage fingerprint; it re-reads just before granting `CAN_RUN` and stops if anything changed. Agents not listed in config are outside GenieRails' guarantee. Prod governance covers only tables of captured agents.

### Champion flow (first deployment)

1. `make setup ENV=dev`; set `access_tier_groups` (full, partial, fully masked) and each agent's `genie_space_id` and `acl_groups`.
2. Classify the dev catalog in the UI: review detections, turn on auto-tagging, wait for `class.*` tags.
3. `make generate ENV=dev`: imports each agent and derives masks for their tables from tags and the library (no AI). Review and commit.
4. `make rehearse ENV=dev`: applies masks and access in dev and proves each tier.
5. Set `catalog_map` in `envs/prod/env.auto.tfvars`; `make promote-to ENV=prod` opens the promotion PR.
6. Classify the prod catalog in the UI.
7. Approve and merge the promotion PR; the operator runs `make release ENV=prod` on the deployment machine.
8. `make maintain ENV=prod` on a schedule (governance only).

### Scenarios

| Scenario | What happens |
|---|---|
| Promote agent A while B has unfinished dev edits | Capture A only, rehearse, merge, promote. B's last captured version is re-pushed unchanged to prod B. |
| A adds a new table | Capture records it; its masks are derived and appear in the same promotion PR; prod applies masks, then A's access, still behind the coverage check. |
| A and B share a table | The table's masks are shared and unchanged; only A's config and access change. |
| Mask change needed while B is mid-curation | Change governance in git, rehearse, promote. Prod B gets the new mask on its tables; its config is unchanged. Dev B sees the new mask; its UI work is untouched. |
| New sensitive column in prod | Prod scan tags it; `maintain` applies the library mask. Unmapped class: `maintain`/`release` stop; add a mapping in git and promote. |
| Change a mask default or tiers | Governance change; the promotion PR shows the function or policy diff. |
| Grant another group access to A in prod | Edit `envs/prod/agents/A.auto.tfvars`, PR, release. B untouched. |
| Remove agent A | Delete its entry in dev, promote. By default the prod agent is **detached** (access revoked, agent kept); deleting it needs `delete = true` and shows in the promotion PR. Table `SELECT` for A's groups is removed unless another agent grants it. The tables stay governed (masks stay) until `make ungovern`. |
| Remove a table from A | Capture A, promote. `SELECT` removed if no other agent needs it. |
| Roll back | Revert code and config together and release. Mask changes follow the tighten-before-loosen phases; old functions are kept for 3 releases so masks can switch back; agent config returns to the previous version. |
| Urgent fix | No hand edits in prod; same path, prioritised. |
| Concurrent promotions | v1 runs on one deployment machine, where the env lock allows one `release` at a time; every applying target refuses in CI. |
| Upgrade GenieRails | Treated as a governance change: upgrade in dev, rehearse, promote. |
| Several teams | Each agent's captured config lives in its own folder, so code owners can require each team's approval; a central team approves governance. |

## 10. Changes after the feasibility review

| Review finding | Resolution |
|---|---|
| A SQL mask can't read a secret | UC secret + Python HMAC UDF + SQL wrapper, created automatically; fail closed to redacted (section 2) |
| Verifier can't tell partial from full | Exact expected-output checks per tier; inconclusive when indistinguishable (section 7) |
| One mask per column with two masked tiers | One policy per masked tier with ordered `EXCEPT` lists; caller-independent functions; most privileged tier wins; unknown principals get full (section 3) |
| "Every live dev agent" was unbounded | Configured dev agents only, read live during `rehearse` and fingerprinted (section 9, rule 5) |
| Migration could weaken a mask mid-cutover | Per-column, per-principal report; refuse weakening; staged new-name functions; old functions kept (section 6) |
| `treatment_tier_overrides` named two things | Split into `treatment_versions`, `tier_access_overrides`, `column_overrides`, with precedence (sections 2, 3) |
| Row filters underspecified | Table-scoped, compiled to one filter per table, AND across rules (section 4) |
| `acl_groups` ownership conflicted with #81 | Environment-owned, not captured; prod keeps its own after the first promotion (section 5) |
| Plain `generate` re-captures today; dev overwrites agent config today | Separate `make capture`; environment-specific config ownership (rollout 7, 8) |
| "Full overwrite" isn't a faithful export today | Capture stores the agent's `serialized_space` verbatim in a versioned file, minus IDs and ACLs; promote remaps catalog names and the warehouse, then prod import sends that remapped config; a rejected field fails the release instead of being silently dropped (rollout 8) |

**Champion impact:** one `acl_groups` line per agent file; everything else is unchanged from the current flow. The hash key is not a champion step: the first `rehearse`/`release` that uses a hashed treatment creates the Unity Catalog secret if missing, `release` provisions the same key into later environments and checks dev and prod produce the same hash before granting access, and stops (or uses `hash_fallback`) if it can't. Hashing itself only happens inside the mask function at query time; table data is never rewritten. `make init-hash-key` stays as an optional operator command for pre-provisioning and rotation.

## 11. Changes after the scenario review

These rules take precedence over earlier sections where they differ.

| Gap | Resolution |
|---|---|
| **G1** Treatment change deletes and recreates the column tag; tags exist only for configured agents' tables, so removing a table drops its mask | Each column has **one** treatment tag resource keyed by `table.column` (not by value); a treatment change updates the value **in place**. GenieRails keeps a persisted **governed-table set**: a table stays governed after it leaves every agent, until an explicit, reviewed `make ungovern TABLE=...`. "Masks stay" and "swap in place" therefore hold. |
| **G2** Partial and full policy changes run in one unordered apply | **Tighten before loosen.** A change that moves any principal between tiers is applied in phases: (1) add or strengthen masks, (2) verify, (3) remove or loosen. A brief "more than one mask" query error is acceptable (fail closed); a raw window is not. Rollback follows the same phases. |
| **G3** Rollout cut over existing deployments before the migration guard | Rollout reordered (section 8): existing deployments keep today's policies until step 6, and AI-drafted row filters stay frozen until declared ones replace them. |
| **G4** Release doesn't check prod classification is complete | `release` blocks if any column tagged in dev (mapped through `catalog_map`) has no `class.*` tag in prod, and lists them. `ACK_UNCLASSIFIED="cat.sch.t.col,..."` acknowledges known exceptions. The existing sensitive-name check stays. |
| **G5** Two CI releases can run at once | **v1 supports one deployment machine.** The local env lock covers it. `release` refuses to run in CI (`CI=true`) unless a remote backend with locking is configured, with a message explaining why. Remote state and CI concurrency are a separate project. |
| **G6** Principals outside the tiers (ETL SPs, owners) go from raw to fully masked | New `raw_exempt_principals` (env-owned, reviewed) is added to every policy's EXCEPT list. The deployer SP is always exempt. The migration report lists every principal holding SELECT, not only tier groups, and flags anyone whose view changes. |
| **G7** `access_tier_groups` ownership contradicts main | Tiers are **governance**: defined in dev, promoted to prod like any rule. `acl_groups` per agent and `raw_exempt_principals` stay env-owned. |
| **G8** Hash key reachability, approval and drift | The key's source of truth is one **deployment secret** generated once by `make init-hash-key` and held by the operator or CI secret store as `GENIERAILS_HASH_KEY`. `rehearse` and `release` create the UC secret in their own catalog **if absent** (never at promote time). A probe hash of a fixed string is compared between dev and prod to prove both use the same key, without exposing it. Missing UC secrets or Python UDFs is a **hard stop** unless `hash_fallback = "redact"` is set explicitly; dev and prod must use the same mode. The secret is owned by the deployer SP; only it holds READ SECRET. `make rotate-hash-key` rotates dev and prod together and keeps the previous version until confirmed. |
| **G9** Captured config can't be sent unchanged; CI has no ID files | The capture file stores dev's `serialized_space` with IDs stripped; `promote-to` remaps catalog names (including SQL text) and the warehouse with the existing remap code. After the first create, `release` records the prod agent ID in `envs/prod/genie_space_ids.auto.tfvars`, which is committed, so any checkout updates the same agent. Title adoption (#79) stays as the fallback. |
| **G10** One fingerprint over every agent's config couples A and B | Live snapshots and fingerprints are **per agent**. A's grants depend only on A's snapshot and the governance of A's tables. |
| **G11** Refuse-weakening has no protection order; nothing can mask tier 1 | Protection order, weakest to strongest: raw < partial versions (last 4, initials, year, band, prefix) < keyed hash < redacted/NULL. Migration compares each principal and column with that order. `column_overrides` gains `keep_current = true` to freeze today's protection on a column. Treatments for `card_security_code`, `card_pin`, `card_track_data` and `secret` are **never raw**: tier 1 sees the full version too. |
| **G12** Removing an agent deletes the prod agent and its conversations | Default is **detach**: access is revoked and the agent is left in place. Deleting needs `delete = true` on that agent's entry and appears in the promotion PR. |
| **G13** Prod UI edits are lost silently | `release` reads the live prod agent, prints a field-level drift report against what was last applied, then overwrites. Docs recommend removing `CAN_EDIT` on prod agents from everyone except the deployer SP. |
| **G14** One shared prod ACL file doesn't fit several teams | Each agent's env-owned settings live in `envs/<env>/agents/<agent>.auto.tfvars`, so code owners can be set per agent. |
| **G15** `envs/` is gitignored | `make setup` writes a `.gitignore` that **commits** `env.auto.tfvars`, `agents/`, `generated/`, capture files and agent-ID files, and **ignores** `auth.auto.tfvars`, state and locks. Docs updated. |
| **G16** Row filters underspecified | Literals are strings; the table is mandatory; a tier-1 group in `values_by_group` is refused; `verify-access` creates one test principal per named group. |
| **G17** Several `class.*` tags on one column; numeric IDs | The **strictest** treatment wins (protection order above). Identifiers stored as numbers are hashed as their canonical decimal string. Section 1 and section 2 agree: a column override sets the partial version or a stricter treatment, never the full version. |
| **G18** Rotation rollback | Covered by G8: rotation keeps the previous key version until `make rotate-hash-key CONFIRM=1`. |

## 12. Changes after the second scenario review

| Item | Resolution |
|---|---|
| **N1** `TO account users` full policies would feed today's access derivation | For deterministic envs, required `acl_groups` and **no policy-derived access** land in **step 5**, together with the per-tier policies. Access is never derived from `account users`, from EXCEPT lists, or from any policy principal. Migrated envs get the same in step 6. |
| **N2** G1 still had unmask windows | Treatment tags move to a **separate resource keyed by `table.column`, without `ignore_changes`**, so value changes are in-place updates. Migration generates `terraform state mv` for each existing tag. In the phases of G2, policies for a **new** treatment value are created in an earlier phase than the retag, so the new value is matched the moment it lands. |
| **N3** CI refusal contradicted the pipeline | v1: the operator runs every applying target (`release`, `maintain`, `apply`, `rehearse`) on the deployment machine; all of them refuse when `CI=true`. CI runs tests, validation and plans. This guard lands in **step 1**. |
| **N4** Classification check needs dev's tags in prod | `promote-to` writes `envs/<dest>/generated/expected_classification.json`: dev's classified columns remapped through `catalog_map`. `release` compares it with prod's live tags. |
| **N5** Steps 2 and 4 could change existing envs | Steps 2 and 4 are gated on `governance_mode = "deterministic"`; existing envs get warnings only. The tag-state move runs in step 6. |
| **N6** Row filters failed open for outsiders | Resolved in section 4: only tier 1, exempt principals and the deployer SP see all rows; other unnamed groups see none. |
| **N7** Per-agent files aren't loaded and Terraform can't merge them | A loader merges `envs/<env>/agents/*.auto.tfvars` into one generated `genie_spaces` variable file before plan; `remap_env_config.py` and validation read the same merge. |
| **N8** `.gitignore` | The **root** `.gitignore` changes in step 7 to re-include committed env files; `generated/.live_refresh.json`, `auth.auto.tfvars`, state and locks stay ignored. |
| **N9** Refuse-weakening only at migration | The protection-order comparison against live state runs on **every** `promote-to` and `release`. Any weakening needs `ACK_WEAKEN="cat.sch.t.col:principal,..."`. |
| **N10** Unmapped new prod class stays raw | `maintain` applies a fail-safe **redacted** treatment to tiers 2 and 3 for a newly tagged column whose class has no mapping, and reports it, until a mapping is promoted. |
| **N11** Per-agent gating needs per-table SELECT gates | Step 10 changes the Terraform gate from one env-wide status to per-table status. |
| **N12** Precedence of never-raw / exempt / overrides | Resolved in section 3. |
| **N13** Verify during a tier move | The verify phase of G2 expects "more than one mask" errors only for principals being moved, and fails on any raw value. |
| **N14** Different dev/prod groups | v1 requires the **same account groups** in dev and prod (tiers are promoted). |
| **N15** Capture remap and ID file | Capture remap is JSON-aware (it remaps string values, including SQL inside them, never keys). `genie_space_ids.auto.tfvars` is added to the var-file list; `release` writes it and prints "commit envs/prod/genie_space_ids.auto.tfvars" for the operator. |
| **N16** Detach needs a per-instance state removal | `release` runs a generated `terraform state rm` for the detached agent's resources before apply. |
| **N17** Row-filter securable | Row-filter policies are created **on the table** securable. |
| **N18** Key residuals | The probe is the hash of the fixed string `genierails-probe`, stored in `generated/hash_probe.json`. If the UC secret already exists, the env var isn't read. Residual risk documented: metastore admins and holders of `MANAGE` on the governance schema can grant themselves READ SECRET. |
| **N19** Stale earlier text | Earlier sections edited to match sections 11 and 12. |

## 13. Team workflow (agreed, KISS)

Several people (agent owners, the governance team) work in the same repo. Each edits their own files: one file per agent, governance files separately.

| Stage | What runs | Touches dev? |
|---|---|---|
| **PR** | read-only: validate, generated files current, coverage check, mask exact-output tests, `make plan ENV=dev` | no, so PRs run in parallel |
| **Merge to `main`** | one job runs `make rehearse ENV=dev` on the new `main` | yes, only from `main`, one at a time |
| **Promote** | `promote-to` and `release` accept only a `main` commit whose rehearse passed | — |

Rules:
1. **Generated files are never hand-merged.** On conflict, run `make generate ENV=dev` on the latest `main` and commit.
2. **Rehearse records what it proved:** commit, input fingerprint, result, stored with dev's Terraform state. It gates promotion only; it never grants access.
3. **A failed post-merge rehearse** blocks promotion until a fix PR merges and rehearse passes. `main` is not "deployable"; only a rehearsed commit is.
4. **Promotion PRs are generated output.** CI re-runs `promote-to` from the recorded rehearsed commit and fails if the PR differs; a stale one is regenerated, never hand-edited.
5. **Prod is unchanged:** after the promotion PR is approved, the operator runs `make release ENV=prod` from the deployment machine.

Prerequisite: remote Terraform state with locking for dev, so the post-merge job can apply. Until it exists, the operator runs the post-merge rehearse from the deployment machine. A merge queue (pre-merge rehearse, always-green `main`) is an optional later addition, not v1.

