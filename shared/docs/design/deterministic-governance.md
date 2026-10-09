# Design: deterministic governance, decoupled from Genie agents

Status: **proposed**, for sign-off before implementation.

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

1. **Treatment per column** comes from its `class.*` tag through the shipped mask library (section 2). A per-column `treatment_overrides` entry can only make it stricter.
2. **One policy per catalog and treatment**, using that treatment's fixed function from the library.
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
| `location` | redacted | redacted |
| `health_data`, `biometric_data`, `genetic_data`, `ethnicity`, `religious_belief`, `political_opinion`, `sexual_data`, `sexual_orientation`, `trade_union_membership`, `criminal_background`, `marital_status`, `employment_status` | redacted | redacted |
| `secret` | redacted | redacted |

"Redacted" means a fixed placeholder for text and NULL for other types. The one-way hash is a salted SHA-256 so values stay joinable and countable within a deployment but can't be reversed.

**Change from today:** `us_ssn` and `us_itin` move from last 4 to one-way hash, following decision 2.

## 3. The tier rule

```hcl
access_tier_groups = ["payments_ops", "regional_analysts", "viewers"]  # full, partial, fully masked
```

- First group: raw values. Last group: full version. Any groups in between: partial version.
- Two groups: raw and full. One group: raw only.
- Optional per-treatment override, e.g. let tier 2 see email raw:

```hcl
treatment_tier_overrides = {
  email_partial = { regional_analysts = "raw" }
}
```

## 4. Row filters (declared only)

A row filter hides whole rows, e.g. regional analysts only see APAC rows. GenieRails creates one only when configured:

```hcl
row_filters = [
  { column = "region_code", values_by_group = { regional_analysts = ["APAC"] } }
]
```

No configuration means no row filters.

## 5. Agent lifecycle

- **Config**: captured from the dev UI with `make generate ENV=dev SPACE="<agent>"`; code-owned in prod.
- **Access**: each agent lists `acl_groups`. Those groups get `CAN_RUN` on the agent and `SELECT` on its tables; a table shared by several agents gets the union. Still gated by the coverage check.
- **Missing `acl_groups`**: `generate` and `release` stop with "agent X has no acl_groups — list the groups that may run it".
- **New table**: governed by section 1 first, then the agent gets access.
- Agent access is never derived from mask policies.

## 6. Migration for existing deployments

- The first `generate` after upgrading prints a migration report: mask functions replaced by library versions (with how each output changes), policies whose groups change under the tier rule, and agents missing `acl_groups`.
- Nothing is applied until accepted with an explicit flag. Masks are never weakened without acceptance, and functions are replaced in place with `CREATE OR REPLACE` (no drop).

## 7. Technical points to resolve in implementation

- **One mask per column with two masked tiers.** Unity Catalog allows only one mask on a column, so tier 2 (partial) and tier 3 (full) must come from one function that branches on group membership (`is_account_group_member`). That makes the mask function caller-sensitive, which `verify-access`'s fixed-point exclusion (#97) deliberately refuses. Verification must therefore check tier 2 and tier 3 against their own expected outputs, rather than relying on that exclusion.
- **Typed variants**: partial and full versions are needed for DATE, TIMESTAMP, numeric and STRING columns where applicable.
- **Hash salt**: where it lives (a secret per deployment) and how it carries from dev to prod.

## 8. Rollout (separate PRs, each reviewed by a different vendor)

1. Mask library: all 93 classes with partial and full functions, plus tests for each function's output.
2. Tier rule and deterministic policy generation (no AI in governance).
3. Required `acl_groups` per agent; access no longer derived from policies.
4. Declared row filters.
5. Migration report and acceptance flag.
6. Docs, then a live check on AWS and Azure.
