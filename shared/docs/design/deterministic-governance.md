# Design: deterministic governance, decoupled from Genie agents

Status: **agreed** (normative spec). Review history is in the PR description.

## Goal

GenieRails has two layers with separate lifecycles:

| | Governance layer | Agent layer |
|---|---|---|
| Owns | treatment tags, mask functions, ABAC policies, row filters, coverage check | Genie config, `acl_groups`, `CAN_RUN`, `SELECT` on the agent's tables |
| Input | the governed set of tables (section 1) and reviewed config | the configured agents |
| Needs the other? | no | yes: its tables are governed first |

Governance is computed only from classification tags and reviewed config. It is the same for every agent and uses no AI. The champion flow uses both layers; governance can also run on its own (section 11).

## Decisions

| # | Decision |
|---|---|
| 1 | Three tiers: full access (raw), partial, fully masked. |
| 2 | Identifiers (national, tax, document and device IDs) are **redacted** for both masked tiers. The keyed hash is deferred (section 15). |
| 3 | Row filters only when declared. GenieRails never invents one. |
| 4 | An agent is run only by its `acl_groups`. A missing `acl_groups` stops the run; `[]` means nobody. |
| 5 | No AI in governance. The AI only helps draft Genie content or suggest a mapping for an unmapped tag, which a person reviews. |
| 6 | Governance works without any agent, using the existing `uc_tables`. |
| 7 | `SELECT` and `CAN_RUN` come **only** from agents' `acl_groups`. Governance never grants access. |

## 1. The governed set

Governed set = top-level `uc_tables` ∪ tables of configured agents ∪ tables already governed (the governed-table list, so a table stays governed until `make ungovern`).

- `uc_tables` keeps its existing syntax: `schema.table` (relative to `uc_catalog`), `catalog.schema.table`, or `catalog.schema.*`.
- `uc_tables` **governs and never grants**, with or without agents.
- `make ungovern TABLE=...` is the only way out. It expands wildcards first and refuses while any agent, `uc_tables` entry or wildcard still covers the table.

For each table in the set:

1. **Treatment per column** from its `class.*` tag through the mask library (section 2). Several classes on one column: the strictest treatment wins (protection order, section 6).
2. **One policy per catalog, treatment and masked tier** (section 3), using a fixed, caller-independent function.
3. **Row filters** only where declared (section 4).
4. **Coverage check:** every `class.*` tag needs a mapping (section 6).

Same inputs give the same output. Adding an agent whose tables are already governed changes nothing in governance.

## 2. The mask library

GenieRails ships a mapping for all 93 Databricks classes. Each treatment has a fixed, reviewed **partial** and **full** version, typed for STRING, numeric, DATE, TIMESTAMP and TIMESTAMP_NTZ where it applies; any other type gets the full version. Policies are created only for treatments in use.

| Classes | Partial (tier 2) | Full (tier 3) |
|---|---|---|
| `credit_card` | last 4 | redacted |
| `bank_number`, `iban_code`, `swift_code`, `us_bank_number`, `uk_sort_code` | last 4 | redacted |
| `card_security_code`, `card_pin`, `card_track_data`, `secret` | **never raw**: redacted for every tier, including tier 1 | redacted |
| `card_expiration_date` | redacted | redacted |
| `us_ssn`, `us_itin`, passports, driver licences, all regional national, tax, social, health and voter IDs, `medical_record_number`, `health_plan_beneficiary_number`, `medical_license`, `medical_device_id`, `imei`, `vin`, `license_plate` | redacted | redacted |
| `email_address` | `j***@example.com` | redacted |
| `phone_number` | last 4 | redacted |
| `name` | initials | redacted |
| `date_of_birth`, `medical_date` | year only | NULL |
| `age` | 10-year band | NULL |
| `credit_score` | 50-point band | NULL |
| `compensation` | rounded | NULL |
| `ip_address` | network only (IPv4 /24; IPv6 redacted) | redacted |
| `mac_address` | vendor prefix | redacted |
| `url` | domain only | redacted |
| `location`, STRING | redacted | redacted |
| `location`, numeric lat/long | 1 decimal place (about 11 km) | NULL |
| health, biometric, genetic, ethnicity, religion, politics, sexual data and orientation, trade union, criminal background, marital and employment status | redacted | redacted |

"Redacted" is a fixed placeholder for text and NULL otherwise.

**Behaviour contract** (tested with a table of exact expected outputs per function, and live on a non-UTC session): NULL in gives NULL out; malformed input gets the full version; numbers round half away from zero; DATE becomes 1 January of the year; TIMESTAMP becomes the start of the year in UTC; bands include the lower bound; NaN and infinity become NULL.

**Changes from today:** `us_ssn` and `us_itin` move from last 4 to redacted for tier 2; IPv6 addresses are redacted for tier 2 instead of keeping a prefix.

**Config** (each treatment is configurable on its own; the library uses the same treatment names as the config):

```hcl
treatment_versions = { ssn = { partial = "last4" } }      # pick another shipped version for a treatment
column_overrides   = {                                      # one column: a stricter treatment, or a reviewed partial version
  "cat.sch.customers.postcode" = { partial = "prefix_3" }
}
tag_treatments     = { "sensitivity=confidential" = "redact" }   # customer-defined tags
```

A column override never changes the full version and can't make a never-raw treatment raw. Setting `partial = "raw"` anywhere counts as weakening (section 6).

## 3. The tier rule

```hcl
access_tier_groups    = ["payments_ops", "regional_analysts", "viewers"]  # full, partial, fully masked
raw_exempt_principals = ["etl-sp", "bi-service-account"]                  # optional, env-owned
```

- First group raw; last group full; any between partial. Two groups: raw and full. One group: raw only.
- **Policy shape** per catalog and treatment, each with a caller-independent function:
  - partial policy: `TO <tier-2 groups> EXCEPT <tier-1 groups, raw_exempt_principals>`;
  - full policy: `TO account users EXCEPT <tier-1 and tier-2 groups, raw_exempt_principals>`.

  Exactly one mask resolves per user. A user in several tiers gets the most privileged one. Anyone outside the tiers gets the full version.
- **Precedence**, one rule: who sees what is decided first (never-raw > tier 1 and `raw_exempt_principals` > tier rule), then which version (`column_overrides` > `treatment_versions` > library). The deployer SP is in the tier-1 EXCEPT lists for ordinary treatments (verification needs raw rows), but **not** for never-raw treatments; never-raw columns are verified by checking every tier sees the redacted output.
- Tiers are governance: defined in dev and promoted. Dev and prod use the same account groups.

## 4. Row filters (declared only)

```hcl
row_filters = [
  { table = "cat.sch.payments", column = "region_code",
    values_by_group = { regional_analysts = ["APAC"] } }
]
```

- No config, no row filter, and no coverage gap.
- All rules for one table compile into one function and one policy on that **table**. Rules on the same table must name the same groups; otherwise the config is refused.
- Named groups see only their values (string literals, case-sensitive, NULL never matches); a user in several named groups sees the union. Tier 1 and `raw_exempt_principals` see all rows; every other group sees **none**. Naming a tier-1 group is refused.

## 5. Agents

- **Capture is explicit:** `make capture ENV=dev SPACE="<agent>"` writes that agent's exported `serialized_space` (IDs and ACLs stripped) to `envs/dev/agents/<agent>.space.json`. A plain `generate` only imports agents not yet in git.
- **Per-agent files:** `envs/<env>/agents/<agent>.auto.tfvars` holds `genie_space_id`, `acl_groups` and `delete`. The prod agent ID is written there by `release` and committed. A loader merges these files before plan.
- **Access:** `acl_groups` get `CAN_RUN` on the agent and `SELECT` on its tables; a shared table gets the union. Gated by the coverage check. Never derived from policies, EXCEPT lists or `account users`.
- **`acl_groups` is env-owned:** the first `promote-to` seeds prod from dev; later promotions keep prod's value and print the differences.
- **Dev:** agent config is never overwritten; GenieRails applies governance and access for captured tables only, and creates an agent only if missing.
- **Prod:** every `release` overwrites the agent's config from git (full replacement). It prints a field-level report of what it overwrote, never recreates the agent and never changes its ID, so conversations stay.
- **Promotion remap** is JSON-aware: catalog names and the warehouse are replaced on identifier boundaries inside string values, never in keys or prose.
- **Removing an agent** detaches it by default: `release` revokes its access and runs a generated `terraform state rm` for the agent's resources, so Terraform never destroys it and its conversations stay. `delete = true` deletes it and shows in the promotion PR.

## 6. Safety rules

| Rule | Behaviour |
|---|---|
| Coverage check | New or wider access needs a recent pass. |
| Unmapped class | Blocks new access. `maintain` and `release` apply a fail-safe **redacted** treatment (tiers 2 and 3) to the column and report it until a mapping is promoted. |
| Prod classification complete | `release` refuses if a column classified in dev (remapped through `catalog_map`) has no `class.*` tag in prod. `promote-to` writes the expected list to `generated/expected_classification.json`. |
| Protection order | raw < partial versions < redacted/NULL. |
| Refuse weakening | Every `promote-to`, `release` and migration compares each SELECT holder and column against live state. Any weakening, including new `raw_exempt_principals`, `partial = "raw"`, or removing or widening a row filter, stops the run unless listed in `ACK`. |
| SELECT-holder check | Before the first deterministic apply of an env, and whenever governance newly covers a table, GenieRails lists every principal that can read the table and that GenieRails didn't grant: explicit `SELECT` holders at table, schema or catalog level, owners, and `ALL PRIVILEGES`/`MANAGE` holders. Every holder (group, user or service principal) must be in a tier, in `raw_exempt_principals`, or in `ACK`; otherwise the run stops. Groups that a row filter would leave with zero rows are listed the same way. |
| Tighten before loosen | A change that moves a principal between tiers applies in phases: add or strengthen masks, verify, then loosen. A brief "more than one mask" error is acceptable only for the principals being moved; any raw value fails the run. Run tier moves as maintenance windows. |
| Tags | One treatment tag key for everything. Each column has one treatment-tag resource keyed by `table.column`; value changes update in place (verified in the provider). Order for a column moving to a new treatment: add the value to the tag policy, create the new treatment's policies, then retag. A column is never tagged with a value no policy matches. |
| CI | Every applying target refuses in CI. The only exception is the optional post-merge `rehearse ENV=dev` job, and only with remote locked state (section 10). |

**One acknowledgement variable:**

```bash
ACK="weaken:cat.sch.t.col:etl-sp,unclassified:cat.sch.t.col,holder:bi-sp"
```

**Residual risks (stated, not solved):**
- Readers with schema- or catalog-level `SELECT` can read a new table raw until it is classified and `maintain` runs. Run `maintain` on a schedule.
- In dev, tables added to an agent in the UI but not yet captured are not governed.

## 7. Verification

`verify-access` checks each tier against its exact expected output: it reads paired raw rows, applies the pure partial or full function, and compares exactly. Where two tiers' expected outputs are identical (for example redacted IDs), they are compared by equality, not reported as inconclusive. A sample that can't distinguish tiers whose expected outputs differ is inconclusive, never a pass. Temporary test principals are created per tier and per row-filter group; group-membership waits (about 5 minutes) are batched into one wait per run.

## 8. Migration for existing deployments

- A report-only dry run comes first, on the live env.
- The report covers every column and every SELECT holder: what they see today and after. It lists agents missing `acl_groups`, and every `SELECT` grant GenieRails will **revoke** because `uc_tables` no longer grants (in any env, with or without agents).
- Weakening and revocations stop the run unless listed in `ACK`. There is no separate accept flag.
- An inventory of masks GenieRails didn't create (`ALTER … SET MASK`, old policies) runs first; they block cutover until removed or acknowledged.
- New functions are created under new names; policies change in tighten-before-loosen phases; old functions are kept until no policy uses them. Tag state moves with generated `terraform state mv`.
- Existing AI-drafted row filters are frozen as they are until declared ones replace them.
- Cutover is reported done only after a fresh classification read and a passing tier-aware verification.
- Rollback: revert and release; the old functions still exist, so masks switch back in place.

## 9. Files and version control

`make setup` writes a `.gitignore` that commits `env.auto.tfvars`, `agents/`, `generated/` and capture files, and ignores `auth.auto.tfvars`, state, locks and `generated/.live_refresh.json`. Generated files are never hand-merged: on conflict, run `make generate` on the latest `main`. `promote-to` writes only the files it owns; env-owned keys (`acl_groups`, `raw_exempt_principals`) are never overwritten and are excluded from the generated-file equality check in section 10. Changes to them are still subject to refuse-weakening.

## 10. Change lifecycle and team workflow

| | Governance change | Agent change |
|---|---|---|
| Made in | files in git | the dev UI, then `make capture` |
| Reaches prod | when merged, rehearsed and released | when captured, merged, rehearsed and released |

| Stage | What runs | Touches dev? |
|---|---|---|
| PR | read-only: validate, generated files current, coverage check, mask tests, `make plan ENV=dev` | no |
| Merge to `main` | `make rehearse ENV=dev` on the new `main` | yes, one at a time |
| Promote | `make promote-to ENV=prod` opens the promotion PR | no |
| Release | after the promotion PR is approved **and merged**, and rehearse has passed on that merge commit, the operator runs `make release ENV=prod` from it on the deployment machine | prod |

- **Rehearse records what it proved:** commit, input fingerprint, result, stored with dev's state.
- **`release` runs only from a commit that itself passed rehearse,** and refuses unless `envs/prod/generated` equals what `promote-to` produces from that commit. Code merged after the rehearse can't reach prod untested.
- **Unfinished agents** never reach git, because capture is explicit. Promoting A carries B's last captured version unchanged.
- **Removing an agent or table removes access only.** Masks stay until `make ungovern`.
- A failed rehearse blocks release until a fix merges and rehearse passes. CI also regenerates each open promotion PR and fails it if it differs.
- v1 runs every applying target from one deployment machine; the env lock allows one run at a time. No hand edits in prod; remove `CAN_EDIT` on prod agents from everyone except the deployer SP.
- Until remote state exists, the operator runs the post-merge rehearse.

## 11. Flows

**Champion flow (with agents):**

1. `make setup ENV=dev`; set `access_tier_groups`; per agent, set `genie_space_id` and `acl_groups` in its agent file.
2. Classify the dev catalog in the UI; turn on auto-tagging; wait for `class.*` tags.
3. `make generate ENV=dev`: imports the agents and derives governance. Review and commit.
4. `make rehearse ENV=dev`.
5. Set `catalog_map`; `make promote-to ENV=prod`; classify prod in the UI.
6. Approve and merge the promotion PR; once rehearse passes on that commit, the operator runs `make release ENV=prod`.
7. `make maintain ENV=prod` on a schedule.

**Governance-only flow (no agents):** the same commands with `uc_tables` set and no agents. GenieRails creates no agent and grants nothing; masks apply to whoever already has access, after the SELECT-holder check (section 6).

## 12. Scenarios

| Scenario | What happens |
|---|---|
| Promote A while B has unfinished dev edits | Capture A, merge, rehearse, promote, release. Prod B gets its last captured version unchanged. |
| A adds a table | Capture records it; governance derives its masks; the promotion PR shows both; prod applies masks, then A's access. |
| A and B share a table | Masks are shared and unchanged; only A changes. |
| Mask change while B is mid-curation | Governance change; rehearse, promote, release. B's config is untouched in dev and prod. |
| New sensitive column in prod | `maintain` applies the library mask, or the fail-safe redaction if unmapped. |
| Grant another group access to A in prod | Edit `envs/prod/agents/A.auto.tfvars`, PR, release. |
| Remove agent A | Detached by default; `SELECT` removed unless another agent grants it; tables stay governed. |
| Stop governing a table | `make ungovern TABLE=...`, refused while anything still covers it. |
| Roll back | Revert and release; masks switch back in place. |
| Upgrade GenieRails | A governance change: upgrade, rehearse, promote, release. |
| Several teams | Per-agent files with code owners; a central team owns governance files. |

## 13. What a user learns

- **Settings:** `access_tier_groups`, `uc_tables`, `catalog_map`, per-agent `genie_space_id` / `acl_groups` / `delete`, and optionally `raw_exempt_principals`, `treatment_versions`, `column_overrides`, `tag_treatments`, `row_filters`. `governance_mode` exists only during migration.
- **One variable:** `ACK`.
- **Commands:** `setup`, `generate`, `capture`, `rehearse`, `promote-to`, `release`, `maintain`, `ungovern`, `scaffold-treatments`.

## 14. Checks before step 5

- ABAC policy limits per securable and per metastore, and the tag-policy allowed-value limit, against a realistic governance-only env.
- Genie round-trip fidelity: export, import into a scratch space, export again; the diff must be empty, and conversations must survive an update.

## 15. Deferred

- **Keyed hash** for identifiers. It needs its own design: a UC secret, a Python UDF, fallback rules and key handling, plus about 8 s per query measured on AWS dev.
- Group-level overrides of the tier rule.
- Merge queue, CI releases to prod, and remote state for prod.

## 16. Rollout

Every step keeps `main` working and ships its own live AWS test where it touches Databricks. Existing deployments are untouched until step 6.

1. **Schemas, CI guard and committed `envs/`** (done in #108, plus the `.gitignore`).
2. **Tier-aware verification** and the prod classification check (done in #109; add equality for identical tiers).
3. **Mask library** (#110).
4. **Stable tags and the governed set** from `uc_tables` and the governed-table list; `make ungovern` (#112).
5. **Deterministic policies with access only from `acl_groups`:** per-tier policies, `raw_exempt_principals`, tighten before loosen, per-agent files, `uc_tables` never grants (with a test that no grant targets `account users`), the SELECT-holder check, governance-only flow. Run the section 14 checks first.
6. **Migration:** dry run, refuse weakening with `ACK`, revocation list, mask inventory, frozen AI row filters, tag state moves, staged cutover; fail-safe redaction in `maintain` and `release`.
7. **Declared row filters.**
8. **Capture, prod ownership and release gating:** `make capture`, JSON-aware remap, prod IDs in agent files, overwrite report, detach by default, rehearsal record, and `release` only from a rehearsed commit.
9. **Optional:** remote state for dev and the post-merge rehearse job, with one sample CI workflow.
10. **Docs**, then a full live run on AWS and Azure.
