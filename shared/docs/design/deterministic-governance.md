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

- The key is a **Unity Catalog secret** in a GenieRails governance schema, created automatically with random bytes on the first dev apply. Dev and prod form one **deployment pair** and use identical key bytes: on a shared metastore it is the same secret; on separate metastores `promote-to` copies it through the secrets API. The key never appears in git, Terraform state or logs.
- A `LANGUAGE PYTHON` UDF declared with `SECRETS (...)` computes the HMAC; a SQL wrapper is the mask function. Neither is declared `DETERMINISTIC`, because rotating the key changes every output.
- Input is normalised before hashing: trimmed, Unicode NFKC, upper-cased, and spaces and hyphens removed. NULL stays NULL.
- **Fail closed:** if UC secrets or Python UDFs are unavailable on the warehouse, hashed treatments fall back to **redacted** for tier 2, with a clear warning. They never fall back to raw.
- **Verified live on AWS dev (spike):** UC secret creation, the Python HMAC UDF with `SECRETS (...)` and `environment_version = '6'`, and the SQL wrapper as a column mask all work on a serverless Pro warehouse. Callers need only `EXECUTE`; the secret is read with the function owner's permission. Output was stable 64-character hex, NULL stayed NULL, duplicates matched.
- **Latency:** the Python UDF made a 1,000-row masked query about 10x slower (0.6 s to 6.5 s median). Only tier-2 queries touching hashed columns pay this (tier 1 sees raw, tier 3's full version is plain SQL). The cost is documented, and a team can switch a treatment to `redacted` for tier 2 with `treatment_versions` if latency matters more than joinability.
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

  So exactly one mask resolves for every user, a user in several tiers gets the **most privileged** one, and anyone outside the tiers who somehow has `SELECT` gets the full version, never raw.
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
- Groups named in `values_by_group` see only their values (case-sensitive, string literals, NULL never matches). Tier-1 groups and groups not named see all rows.
- A user in several named groups sees the union of their values.

## 5. Agent lifecycle

- **Config**: curated in the dev UI and captured into git on purpose with `make capture ENV=dev SPACE="<agent>"` (section 9); code-owned in prod.
- **Access**: each agent lists `acl_groups`. Those groups get `CAN_RUN` on the agent and `SELECT` on its tables; a table shared by several agents gets the union. Still gated by the coverage check.
- **`acl_groups` is environment-owned, not captured.** The first `promote-to` seeds prod's value from dev; later promotions keep prod's own value and print the dev/prod differences (as #81 does today).
- **Missing `acl_groups`**: `generate` and `release` stop with "agent X has no acl_groups — list the groups that may run it". An explicit `[]` is allowed and means nobody.
- **New table**: governed by section 1 first, then the agent gets access.
- Agent access is never derived from mask policies.

## 6. Migration for existing deployments

- The first `generate` after upgrading writes a **migration report**: for every column and every tier group, what it sees today and what it will see. It also lists agents missing `acl_groups`.
- **Refuse weakening:** if any principal would see a column less protected than today, migration stops and names it. Changing that needs an explicit override.
- **Staged cutover:** new library functions are created under new names first; policies are switched one at a time, never dropped then recreated; old functions are kept until no policy uses them.
- Nothing is applied until accepted (`ACCEPT_MIGRATION=1`), and a fresh classification read plus tier-aware verification runs before the cutover is reported done.
- Rollback reverts the commit and releases; the old functions are still there, so masks switch back in place.

## 7. Technical points to resolve in implementation

- **Tier-aware verification (required).** `verify-access` checks each tier against its **exact expected output**, not just "differs from raw": it reads paired raw rows, applies the pure partial or full function to them, and compares exactly. Functions stay caller-independent (section 3), so #97's fixed-point handling still applies. A sample where raw, partial and full can't be told apart is **inconclusive**, never a pass. A test principal in two tiers must see the most privileged tier's output.
- **Typed variants**: partial and full versions for DATE, TIMESTAMP, numeric and STRING where applicable.
- **Hash key**: as specified in section 2.

## 8. Rollout (separate PRs, each reviewed by a different vendor)

1. **Schemas:** treatment and override formats, precedence and validation.
2. **Tier-aware verification** in `verify-access` and the coverage check (before any new mask lands).
3. **Mask library:** all 93 classes, typed partial and full functions with exact output tests, and the keyed-hash infrastructure.
4. **Deterministic policies:** tag → treatment → per-tier policies; no AI in governance.
5. **Required `acl_groups`:** remove access derived from policies; keep #81 prod ownership.
6. **Declared row filters.**
7. **Capture:** a versioned capture format and `make capture`; plain `generate` stops re-capturing.
8. **Dev/prod config ownership:** dev never overwrites agent config; prod fully overwrites it, with round-trip tests.
9. **Live agent snapshot:** `rehearse` reads configured dev agents' table lists and includes them in the coverage fingerprint.
10. **Migration:** report, refuse-weakening, staged cutover, rollback.
11. **Live tests** on AWS (all key scenarios in section 9), then Azure, then docs.

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
3. **One pipeline.** A promotion PR (`make promote-to ENV=prod`) carries whatever is in git, governance changes, agent changes or both, and shows which. Merging it with the required approval runs `make release ENV=prod`.
4. **Removing an agent or table removes access only.** Masks stay; deleting a mask is a deliberate governance change.
5. **Dev governance covers every table a configured live dev agent uses,** including tables added in the UI but not captured yet, so work in progress is protected in dev. During `rehearse`, GenieRails reads each configured dev agent through the Genie API, records its ID, a digest of its config and its table list, and includes that in the coverage fingerprint; it re-reads just before granting `CAN_RUN` and stops if anything changed. Agents not listed in config are outside GenieRails' guarantee. Prod governance covers only tables of captured agents.

### Champion flow (first deployment)

1. `make setup ENV=dev`; set `access_tier_groups` (full, partial, fully masked) and each agent's `genie_space_id` and `acl_groups`.
2. Classify the dev catalog in the UI: review detections, turn on auto-tagging, wait for `class.*` tags.
3. `make generate ENV=dev`: imports each agent and derives masks for their tables from tags and the library (no AI). Review and commit.
4. `make rehearse ENV=dev`: applies masks and access in dev and proves each tier.
5. Set `catalog_map` in `envs/prod/env.auto.tfvars`; `make promote-to ENV=prod` opens the promotion PR.
6. Classify the prod catalog in the UI.
7. Approve and merge; the pipeline runs `make release ENV=prod`.
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
| Grant another group access to A in prod | Edit A's `acl_groups` in `envs/prod`, PR, release. B untouched. |
| Remove agent A | Delete its entry in dev, promote. Created agent deleted, attached agent loses access; table `SELECT` for A's groups removed unless another agent grants it. Masks stay. |
| Remove a table from A | Capture A, promote. `SELECT` removed if no other agent needs it. |
| Roll back | Revert the commit and release. Masks swap in place; agent config returns to the previous version. |
| Urgent fix | No hand edits in prod; same path, prioritised. |
| Concurrent promotions | The env lock allows one `release` at a time. |
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
| "Full overwrite" isn't a faithful export today | Capture stores the agent's `serialized_space` verbatim in a versioned file, minus IDs and ACLs; prod import sends it unchanged; a rejected field fails the release instead of being silently dropped (rollout 8) |

**Champion impact kept minimal:** the hash key is created and shared automatically (no champion step); `acl_groups` is one line per agent; everything else is unchanged from the current flow.
