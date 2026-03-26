# ABAC Configuration Generator — AI Prompt Template

Copy everything below the line into ChatGPT, Claude, or Cursor. Paste your table DDL / `DESCRIBE TABLE` output where indicated. The AI will generate:

1. **`masking_functions.sql`** — SQL UDFs for your masking and row-filter requirements
2. **`abac.auto.tfvars`** — A complete variable file ready for `terraform apply`

---

## Prompt (copy from here)

You are an expert in Databricks Unity Catalog Attribute-Based Access Control (ABAC). I will give you my table schemas from any industry or domain. You will analyze the columns for sensitivity (PII, financial, health, compliance, proprietary, etc.), then generate two files:

### What is ABAC?

ABAC uses governed **tags** on tables/columns and **FGAC policies** (column masks + row filters) to control data access based on **group membership**. The flow is:

1. Create **groups** (access tiers like "Junior_Analyst", "Admin")
2. Create **tag policies** (e.g., `sensitivity` with values `public`, `confidential`, `restricted`)
3. Assign **tags** to tables and columns
4. Create **FGAC policies** that match tagged columns/tables and apply masking functions for specific groups

### Available Masking Function Patterns

Use these signatures. Replace `{catalog}.{schema}` with the user's catalog and schema.

**PII:**

- `mask_pii_partial(input STRING) RETURNS STRING` — first + last char visible, middle masked
- `mask_ssn(ssn STRING) RETURNS STRING` — last 4 digits of SSN visible
- `mask_email(email STRING) RETURNS STRING` — masks local part, keeps domain
- `mask_phone(phone STRING) RETURNS STRING` — last 4 digits visible
- `mask_full_name(name STRING) RETURNS STRING` — reduces to initials

**Financial:**

- `mask_credit_card_full(card_number STRING) RETURNS STRING` — all digits hidden
- `mask_credit_card_last4(card_number STRING) RETURNS STRING` — last 4 visible
- `mask_account_number(account_id STRING) RETURNS STRING` — deterministic SHA-256 token
- `mask_amount_rounded(amount DECIMAL(18,2)) RETURNS DECIMAL(18,2)` — round to nearest 10/100
- `mask_iban(iban STRING) RETURNS STRING` — country code + last 4

**Health:**

- `mask_mrn(mrn STRING) RETURNS STRING` — last 4 digits of MRN
- `mask_diagnosis_code(code STRING) RETURNS STRING` — ICD category visible, specifics hidden

**General:**

- `mask_redact(input STRING) RETURNS STRING` — replace with `[REDACTED]`
- `mask_hash(input STRING) RETURNS STRING` — full SHA-256 hash
- `mask_nullify(input STRING) RETURNS STRING` — return NULL

**Non-STRING types (parameter type AND return type must match the column's data type exactly):**

- `mask_date_to_year(dt DATE) RETURNS DATE` — round to Jan 1 of the year (e.g. 1987-03-22 → 1987-01-01)
- `mask_timestamp_to_day(ts TIMESTAMP) RETURNS TIMESTAMP` — truncate to midnight of the day
- `mask_nullify_date(dt DATE) RETURNS DATE` — return NULL for DATE columns
- `mask_nullify_int(input INT) RETURNS INT` — return NULL for INT columns
- `mask_nullify_long(input BIGINT) RETURNS BIGINT` — return NULL for BIGINT columns
- `mask_nullify_double(input DOUBLE) RETURNS DOUBLE` — return NULL for DOUBLE columns
- `mask_nullify_boolean(input BOOLEAN) RETURNS BOOLEAN` — return NULL for BOOLEAN columns

**CRITICAL — type matching**: NEVER apply a STRING-typed masking function to a DATE, numeric, or BOOLEAN column. Check the column's data type in the DDL and select (or create) a function whose parameter type and return type match exactly. If no appropriate function exists in the library, create a new one following the same NULL-safe CASE pattern.

**Row Filters (zero-argument, must be self-contained):**

Row filter functions take no arguments and return BOOLEAN. They must be **fully
self-contained** — every function they call must either be a Databricks built-in
or must also be defined in the same SQL file (before the caller). Do NOT reference
undefined helper functions like `get_current_user_metadata`.

**CRITICAL — RETURN TRUE means no restriction**: A row filter that returns `TRUE`
for every row allows the principal to see ALL rows — it restricts nothing. Only
generate a row filter when you have logic that actually suppresses rows.

If the required logic depends on a lookup table or user-metadata source that is NOT
present in the DDL, prefer one of these outcomes instead of silently shipping a no-op
control:

1. Omit the row filter entirely and explain in a comment why it could not be generated
2. Generate a clearly labeled STUB function only if the surrounding comments make it
   explicit that the function is not enforcing anything yet and must be replaced

**Preferred pattern — group membership check (most useful):**

```sql
-- Restricts rows to members of the Compliance_Officer group only
CREATE OR REPLACE FUNCTION filter_compliance_only()
RETURNS BOOLEAN
COMMENT 'Only members of Compliance_Officer can see AML-flagged rows'
RETURN is_account_group_member('Compliance_Officer');

-- Time-limited access window
CREATE OR REPLACE FUNCTION filter_trading_hours()
RETURNS BOOLEAN
COMMENT 'Allow access only outside market hours (before 9am or after 4pm)'
RETURN HOUR(NOW()) < 9 OR HOUR(NOW()) > 16;

-- Expiry-based access
CREATE OR REPLACE FUNCTION filter_audit_expiry()
RETURNS BOOLEAN
COMMENT 'Time-limited audit access — expires 2025-12-31'
RETURN CURRENT_DATE() <= DATE('2025-12-31');
```

**Stub pattern — use ONLY when the user explicitly wants a placeholder and no real logic can be derived from the DDL:**

```sql
-- STUB: returns TRUE (no rows are restricted) until logic is implemented
-- TODO: replace RETURN TRUE with real logic, e.g. is_account_group_member('Region_US')
CREATE OR REPLACE FUNCTION filter_by_region_us()
RETURNS BOOLEAN
COMMENT 'STUB — placeholder for US-region row filtering; implement before apply'
RETURN TRUE;
```

If you emit a STUB row filter, also include a warning comment in `abac.auto.tfvars`
next to the corresponding `fgac_policy` indicating that the filter is a placeholder.

Note: The semicolon must be the **last character** on the RETURN line. Do NOT add inline comments after it (e.g., `RETURN TRUE; -- comment` breaks automated deployment).

If a row filter needs user-specific metadata (e.g. the current user's region),
define a helper function in the same SQL file **before** the filter that calls it.
For example, define `get_current_user_metadata(key STRING) RETURNS STRING` that
queries a `user_metadata` table or returns a stub `CAST(NULL AS STRING)`, then
reference it from the filter.

These are common patterns. If the user's data requires masking not covered above (e.g., vehicle VINs, student IDs, device serial numbers, product SKUs), create a new function following the same pattern (NULL-safe CASE expression, COMMENT describing usage).

### Output Format — File 1: `masking_functions.sql`

Group functions by target schema. Only create each function in the schema(s) where
it is referenced by `function_schema` in fgac_policies. If a function is used by
policies targeting multiple schemas, include it in each schema that needs it.

**CRITICAL — SQL formatting rules:**

- Each function MUST end with a semicolon (`;`) as the **last character on that line**
- Do NOT put inline comments after the semicolon (e.g., `RETURN TRUE; -- comment` will break parsing)
- Put comments on separate lines above the function or in the COMMENT clause

```sql
-- === schema_a functions ===
USE CATALOG my_catalog;
USE SCHEMA schema_a;

CREATE OR REPLACE FUNCTION mask_diagnosis_code(code STRING)
RETURNS STRING
COMMENT 'description'
RETURN CASE ... END;

-- Row filter — use is_account_group_member() for real logic, or stub with RETURN TRUE + STUB label
CREATE OR REPLACE FUNCTION filter_compliance_only()
RETURNS BOOLEAN
COMMENT 'Only members of Compliance_Officer can see restricted rows'
RETURN is_account_group_member('Compliance_Officer');

-- === schema_b functions ===
USE CATALOG my_catalog;
USE SCHEMA schema_b;

CREATE OR REPLACE FUNCTION mask_credit_card_full(card_number STRING)
RETURNS STRING
COMMENT 'description'
RETURN CASE ... END;
```

Only include functions the user actually needs. If a library function works as-is, still include it so the user has a self-contained SQL file.

### Output Format — File 2: `abac.auto.tfvars`

```hcl
groups = {
  "GroupName" = { description = "What this group can see" }
}

tag_policies = [
  { key = "tag_name", description = "...", values = ["val1", "val2"] },
]

# entity_name: always use fully qualified names (catalog.schema.table for tables,
# catalog.schema.table.column for columns).
tag_assignments = [
  # Table-level tags (optional — scope column masks or row filters to specific tables, or for governance):
  # { entity_type = "tables",  entity_name = "catalog.schema.Table",     tag_key = "tag_name", tag_value = "val1" },
  { entity_type = "columns", entity_name = "catalog.schema.Table.Column", tag_key = "tag_name", tag_value = "val1" },
]

# IMPORTANT: each entity may have AT MOST ONE value for a given tag_key.
# Valid:
#   customers.email -> pii_level = "masked_email"
# Invalid:
#   customers.email -> pii_level = "masked_email"
#   customers.email -> pii_level = "restricted"
# If a column needs a stronger policy, choose a single final value for that tag_key.

fgac_policies = [
  # Column mask — Pattern A: list only restricted groups in to_principals (simple)
  # Admin groups are simply absent from to_principals and see unmasked data.
  {
    name             = "policy_name"
    policy_type      = "POLICY_TYPE_COLUMN_MASK"
    catalog          = "my_catalog"
    to_principals    = ["Analyst", "Standard_User"]
    comment          = "Description"
    match_condition  = "hasTagValue('tag_name', 'val1')"
    match_alias      = "tag_name_val1"
    function_name    = "function_name"
    function_catalog = "my_catalog"
    function_schema  = "my_schema"
  },
  # Column mask — Pattern B: include all users, exempt admins via except_principals (explicit)
  # Use this when you want to document the exemption clearly or when future policies
  # might catch all users (safer in multi-policy environments).
  {
    name              = "policy_name_with_exception"
    policy_type       = "POLICY_TYPE_COLUMN_MASK"
    catalog           = "my_catalog"
    to_principals     = ["account users"]
    except_principals = ["Admin_Group"]
    comment           = "Mask for all users except admins"
    match_condition   = "hasTagValue('tag_name', 'val1')"
    match_alias       = "tag_name_val1"
    function_name     = "function_name"
    function_catalog  = "my_catalog"
    function_schema   = "my_schema"
  },
  # Row filter — scoped to specific tables using when_condition:
  {
    name             = "filter_name"
    policy_type      = "POLICY_TYPE_ROW_FILTER"
    catalog          = "my_catalog"
    to_principals    = ["GroupName"]
    comment          = "Description"
    when_condition   = "hasTagValue('tag_name', 'val1')"
    function_name    = "filter_function"
    function_catalog = "my_catalog"
    function_schema  = "my_schema"
  },
]

# --- fgac_policy name ---
# name must be unique across ALL fgac_policies in the same tfvars file.
# Use a descriptive pattern: "<action>_<tag_key>_<tag_value>"
# e.g. "mask_pii_masked_email", "filter_phi_restricted"
# AVOID generic names like "mask_pii" or "filter_rows" — duplicates cause Terraform errors.

# --- match_alias ---
# match_alias is the internal identifier the ABAC engine uses to track where the
# column mask applies. Rules:
# - Must be unique within the catalog across all column mask policies
# - Use snake_case reflecting the tag value: e.g. "pii_ssn", "pci_card", "phi_mrn"
# - AVOID generic names like "name", "value", "data" — these risk silent conflicts
#   if two policies within the same catalog use the same alias

# --- to_principals / except_principals patterns ---
# Pattern A (simple): list only restricted groups in to_principals.
#   Admin/privileged groups are omitted — they see unmasked data by default.
#   Use when: role set is small and stable.
# Pattern B (explicit): set to_principals = ["account users"] and list exempt groups
#   in except_principals. Use when: you want an auditable record of who is exempt,
#   or when other policies might introduce "all users" scope in the future.
# NEVER include a group in both to_principals and except_principals.

# --- when_condition decision ---
# - Row filter: OMIT when_condition to apply the filter to ALL tables accessible to
#   the principal (e.g., business-hours restrictions). INCLUDE when_condition to scope
#   the filter to specific tagged tables only (e.g., only tables tagged compliance_scope
#   = 'aml_restricted'). If when_condition is used, the referenced tag must be a
#   TABLE-level assignment (entity_type = "tables").
# - Column mask: OMIT when_condition to let match_condition select columns across ALL
#   tables. INCLUDE when_condition to additionally scope to specific tagged tables.
# - If you use when_condition, the referenced tags must be assigned at the TABLE level
#   (entity_type = "tables" in tag_assignments).

# group_members: optional — auto-populated by the code when syncing group membership from an IdP.
# Leave as {} in the generated output. Do NOT statically list user IDs here.
group_members = {}
```

### Validation

After generating both files, the user should validate them before running `terraform apply`:

```bash
pip install python-hcl2
python validate_abac.py abac.auto.tfvars masking_functions.sql
```

This checks cross-references (groups, tags, functions), naming conventions, and structure. Fix any `[FAIL]` errors before proceeding.

### CRITICAL — Valid Condition Syntax

The `match_condition` and `when_condition` fields ONLY support these functions:

- `hasTagValue('tag_key', 'tag_value')` — matches entities with a specific tag value
- `hasTag('tag_key')` — matches entities that have the tag (any value)
- Combine with `AND` / `OR`

**FORBIDDEN** — the following will cause compilation errors:

- `columnName() = '...'` — NOT supported
- `columnName() IN (...)` — NOT supported
- `tableName() = '...'` — NOT supported
- Any comparison operators (`=`, `!=`, `<`, `>`, `IN`)

To target specific columns, use **distinct tag values** assigned to those columns, not `columnName()`. For example, instead of `hasTagValue('phi_level', 'full_phi') AND columnName() = 'MRN'`, create a separate tag value like `phi_level = 'mrn_restricted'` and assign it only to the MRN column.

### CRITICAL — One Mask Per Column Per Group

Each column must be matched by **at most one** column mask policy per principal group. If two policies with the same `to_principals` both match a column, Databricks will reject the query with `MULTIPLE_MASKS`. This means:

1. **No overlapping match conditions**: If two column mask policies target the same group and their `match_condition` values both evaluate to true for any column, you'll get a conflict. For example, `hasTagValue('phi_level', 'masked_phi')` and `hasTagValue('phi_level', 'masked_phi') AND hasTag('phi_level')` are logically identical — the `AND hasTag(...)` is always true when `hasTagValue(...)` already matches — so both policies would apply to the same columns.

2. **One tag value = one masking function**: Every column mask policy has a `match_condition` that selects columns by tag value, and ALL columns matching that value get the SAME masking function. You cannot use `columnName()` to differentiate — it is not supported. Therefore, if columns need different masking functions, they MUST have different tag values, even if they belong to the same sensitivity category.

   **Common mistake (WRONG):** Tagging FirstName, Email, and AccountID all as `pii_level = 'masked'`, then creating three separate policies — `mask_pii_partial`, `mask_email`, and `mask_account_number` — each matching `hasTagValue('pii_level', 'masked')`. This causes all three masks to apply to all three columns.

   **Correct approach:** Use distinct tag values per masking need:
   - FirstName, LastName → `pii_level = 'masked_name'` → policy uses `mask_pii_partial`
   - Email → `pii_level = 'masked_email'` → policy uses `mask_email`
   - AccountID → `pii_level = 'masked_account'` → policy uses `mask_account_number`

   Remember to add all new tag values to the `tag_policies` `values` list.

   **Type-heterogeneous columns (common mistake)**: Columns in the same sensitivity
   category that have different data shapes require different masking functions and
   therefore MUST have different tag values — even if they feel logically equivalent.

   **WRONG:** Tagging both `email STRING` and `phone STRING` as `pii_level = 'masked_contact'`
   and applying `mask_email` to all of them. `mask_email` splits on `@` — when applied
   to a phone number it silently returns the original value (RLIKE check fails) or
   produces garbage. `validate_abac.py` will NOT catch this — the function exists and
   the tag value is defined, so validation passes. The error only appears at query time.

   **Correct approach:**
   - email → `pii_level = 'masked_email'` → policy uses `mask_email`
   - phone → `pii_level = 'masked_phone'` → policy uses `mask_phone`

3. **Quick check**: For every pair of column mask policies that share any group in `to_principals`, verify that their `match_condition` values cannot both be true for the same column. If they can, either merge the policies or split the tag values. The number of distinct tag values in `tag_policies` should be >= the number of distinct masking functions you want to apply for that tag key.

### CRITICAL — Internal Consistency

Every tag value used in `tag_assignments` and in `match_condition` / `when_condition` MUST be defined in `tag_policies`. Before generating, cross-check:

1. Every `tag_value` in `tag_assignments` must appear in the `values` list of the corresponding `tag_key` in `tag_policies`
2. Every `hasTagValue('key', 'value')` in `match_condition` or `when_condition` must reference a `key` and `value` that exist in `tag_policies`
3. Every `function_name` in `fgac_policies` must have a corresponding `CREATE OR REPLACE FUNCTION` in `masking_functions.sql` **in the same schema** as specified by `function_schema`. A function defined under `USE SCHEMA finance` does NOT satisfy a policy with `function_schema = "clinical"` — each schema where a function is referenced must have its own `CREATE OR REPLACE FUNCTION` block for that function. General-purpose functions like `mask_nullify` or `mask_redact` that are used across multiple schemas must be created once per schema under the appropriate `USE SCHEMA` block.
4. Every group in `to_principals` / `except_principals` must be defined in `groups` — EXCEPTION: `"account users"` is a Databricks builtin principal. Do NOT add it to the `groups` dict; it is only valid in `to_principals` for Pattern B policies
5. If any generated function calls another non-built-in function (e.g. a helper like `get_current_user_metadata`), that helper MUST also be defined in `masking_functions.sql` **before** the function that calls it. Never reference undefined functions

Violating any of these causes validation failures. Double-check consistency across all three sections (`tag_policies`, `tag_assignments`, `fgac_policies`) before outputting.

**Common mistake 1 — cross-key value leakage**: Do NOT use a value from one tag policy in a different tag policy. For example, if `pii_level` has value `"masked"` but `compliance_level` does not, you MUST NOT write `tag_key = "compliance_level", tag_value = "masked"`. Each tag assignment and condition must use only the values defined for that specific tag key.

**Common mistake 2 — generic fallback values**: Do NOT use a generic value like `"masked"` in a tag assignment or match_condition unless that exact string appears in the `values` list for that tag key. If you created distinct values (e.g., `"masked_diagnosis"`, `"masked_notes"`) for a tag policy, you MUST use one of those — not a shortened or generic form. For example, if `phi_level` has values `["public", "masked_diagnosis", "masked_notes", "restricted"]`, writing `tag_value = "masked"` will fail validation because `"masked"` is not in the list.

**Common mistake 3 — function in wrong schema**: Do NOT define a function only in one schema and then reference it from a policy whose `function_schema` points to a different schema. For example, if `mask_nullify` is only defined under `USE SCHEMA finance` but a policy has `function_schema = "clinical"`, Databricks will fail with "Routine does not exist" even though the function exists elsewhere. Fix: add a `CREATE OR REPLACE FUNCTION mask_nullify(...)` block under `USE SCHEMA clinical` as well.

**Final check before outputting**: Enumerate every unique `tag_value` across all `tag_assignments` entries and every value referenced in `hasTagValue()` calls in `match_condition` / `when_condition`. For each one, confirm it appears in the `values` list of its corresponding `tag_key` in `tag_policies`. If any value is missing, either add it to the tag policy or change the assignment/condition to use an existing value.

**Final check — function/schema alignment**: For every entry in `fgac_policies`, confirm that `masking_functions.sql` contains a `CREATE OR REPLACE FUNCTION {function_name}` definition under a `USE SCHEMA {function_schema}` block. Cross-reference each policy's `(function_catalog, function_schema, function_name)` triple against the `USE CATALOG` / `USE SCHEMA` / `CREATE OR REPLACE FUNCTION` sequence in the SQL file.

### CRITICAL — Coverage Gap Check

**Every non-public tag value used in `tag_assignments` MUST have a corresponding FGAC policy.** Tagging a column as `pii_level = 'restricted'` without a matching `fgac_policy` entry gives a false sense of security: the tag is applied, but the data is visible to all users without restriction.

Before outputting, perform this coverage check:

1. Collect every `tag_value` used in `tag_assignments` (column-level entries only)
2. For each assignment, check whether at least one `fgac_policy` has a condition that can evaluate to true for that assignment. This can be:
   - an exact `hasTagValue('that_key', 'that_value')`, OR
   - a broader but still valid condition that includes that tag value and any additional scoping conditions
3. If a tag value has no matching policy, you MUST either:
   - Add an `fgac_policy` entry for it, OR
   - If the 8-policy limit is already hit, explicitly note in a comment inside `tag_assignments` that this value is intentionally unmasked (e.g., "restricted — no policy due to quota limit; address this manually")

**Untagged columns are always visible to all users** — there is no default masking. Only columns with tags that match an active FGAC policy condition will have their data masked. Tagging alone is not enough.

### Instructions

1. Generate `masking_functions.sql` with functions **grouped by target schema**. Use separate `USE CATALOG` / `USE SCHEMA` blocks for each schema. Only deploy each function to the schema(s) where it is referenced by `function_schema` in fgac_policies — do NOT duplicate all functions into every schema. Do NOT include `uc_catalog_name`, `uc_schema_name`, or authentication variables (databricks_account_id, etc.) in the generated abac.auto.tfvars. Every `fgac_policies` entry MUST include `catalog`, `function_catalog`, and `function_schema` — set them to the catalog/schema that each policy's table belongs to.
2. Analyze each column in the user's tables for sensitivity. Common categories include but are not limited to:
   - PII (names, emails, SSN, phone, address, date of birth, national IDs)
   - Financial (credit cards, account numbers, amounts, IBAN, trading data)
   - Health / PHI (MRN, diagnosis codes, clinical notes, insurance IDs)
   - Regional / residency (region columns that need row filtering)
   - Confidential business data (proprietary scores, internal metrics, trade secrets)
   - Compliance-driven fields (audit logs, access timestamps, regulatory identifiers)
   Adapt to whatever domain the user's tables belong to — retail, manufacturing, education, telecom, government, etc. Do NOT limit analysis to healthcare or finance.
   Use these default decisions unless the domain strongly suggests otherwise:

   - direct regulated secrets / identifiers (`ssn`, `mrn`, card PAN, government IDs, CVV, access tokens) → prefer `mask_nullify`, `mask_redact`, or domain-specific full redaction
   - quasi-identifiers (`name`, `email`, `phone`) → prefer partial/domain-aware masking
   - dates of birth / sensitive timestamps → prefer generalization (e.g. year-only or day truncation) instead of exposing full precision
   - free-text notes / narrative clinical text / support case text → prefer full redaction
3. Propose groups — typically 2-5 access tiers (e.g., restricted, standard, privileged, admin)
4. Design tag policies — one per sensitivity dimension (e.g., `pii_level`, `pci_clearance`)
5. Map tags to the user's specific columns. **Use distinct tag values to differentiate columns that need different masking** — do NOT use `columnName()` in conditions. Table-level tags (entity_type = "tables") are optional — use them to scope column masks or row filters to specific tables, or for governance. **Always use fully qualified entity names** (e.g. `catalog.schema.Table` for tables, `catalog.schema.Table.Column` for columns). **Do not assign more than one value of the same `tag_key` to the same entity.**
6. Select masking functions from the library above (or create new ones)
7. Generate both output files. For entity names in tag_assignments, always use **fully qualified** names (`catalog.schema.table` or `catalog.schema.table.column`). For function_name in fgac_policies, use relative names only (e.g. `mask_pii`). Every fgac_policy MUST include `catalog`, `function_catalog`, and `function_schema`. **CRITICAL**: set `function_schema` to the schema where the tagged columns actually live — do NOT default all policies to the first schema. In `masking_functions.sql`, group the `CREATE FUNCTION` statements by schema with separate `USE SCHEMA` blocks. **For every fgac_policy, the function must be defined under the `USE SCHEMA` block that matches `function_schema` exactly.** If the same function (e.g. `mask_nullify`) is used by policies in different schemas, duplicate the `CREATE OR REPLACE FUNCTION` definition once per schema that needs it. **HARD LIMIT**: generate at most **8 fgac_policies per catalog** (Databricks enforces a platform limit of 10; staying at 8 leaves headroom). If the limit would be exceeded, apply this priority order — drop from the bottom first:
   1. **Regulatory direct identifiers** — columns that regulations (PCI DSS, HIPAA, GDPR, SOX) explicitly require to be masked (card numbers, SSN, MRN, government IDs, biometrics)
   2. **High-risk financial data** — account numbers, IBAN, trading positions, credit limits
   3. **Indirect PII clusters** — names, email, phone (mask when combined exposure creates re-identification risk)
   4. **Row filters for compliance-flagged tables** — AML, HIPAA, audit-restricted tables
   5. **Derived / lower-sensitivity masks** — amount rounding, date truncation to year, score bucketing
8. Preserve identifiers and types exactly as they appear in the DDL. Do NOT singularize/pluralize table names, normalize case, rename columns, or infer alternate join keys. This applies to:
   - `tag_assignments.entity_name`
   - `genie_join_specs.left_table`, `right_table`, and join SQL
   - benchmark SQL
   - masking function parameter and return types
9. Every `match_condition` and `when_condition` MUST only use `hasTagValue()` and/or `hasTag()` — no other functions or operators
10. Generate Genie Space config — all nine fields below. **Derive everything from the user's actual tables, columns, and domain** — do NOT copy the finance/healthcare examples below if the user's data is from a different industry. Adapt terminology, metrics, filters, and joins to whatever vertical the tables belong to (retail, manufacturing, telecom, education, logistics, etc.).

Fields to include:

- `genie_space_title` — a concise, descriptive title reflecting the user's domain (e.g., finance → "Financial Compliance Analytics", retail → "Retail Sales & Inventory Explorer", telecom → "Network Performance Dashboard")
- `genie_space_description` — 1–2 sentence summary of what the space covers and who it's for
- `genie_sample_questions` — 5–10 natural-language questions a business user in that domain would ask (shown as conversation starters in the UI). Must reference the user's actual table/column names.
- `genie_instructions` — domain-specific guidance for the Genie LLM. **Must include business defaults** — look at status/state columns in the user's tables and define which values are the default filter (e.g., if a table has `OrderStatus` with values like 'Fulfilled'/'Cancelled'/'Pending', instruct: "default to fulfilled orders"). Also cover date conventions, metric calculations, terminology, and masking awareness relevant to the user's domain.
- `genie_benchmarks` — 3–5 benchmark questions with ground-truth SQL. **Each question must be unambiguous and self-contained** — include explicit qualifiers so the question and SQL agree on scope (e.g., "What is the average risk score for active customers?" not "What is the average customer risk score?"). Avoid questions that could reasonably be interpreted with different WHERE clauses.
- `genie_sql_filters` — default WHERE clauses derived from the user's status/state columns (e.g., active records, completed transactions, open orders). Each filter has `sql`, `display_name`, `comment`, and `instruction`.
- `genie_sql_measures` — standard aggregate metrics derived from the user's numeric columns (e.g., sums, averages, counts that are meaningful in the domain). Each measure has `alias`, `sql`, `display_name`, `comment`, and `instruction`.
- `genie_sql_expressions` — computed dimensions derived from the user's date/category columns (e.g., year extraction, bucketing, status grouping). Each expression has `alias`, `sql`, `display_name`, `comment`, and `instruction`.
- `genie_join_specs` — relationships between the user's tables based on foreign key columns (look for matching ID columns like `CustomerID`, `OrderID`, `ProductID`). Each join has `left_table`, `left_alias`, `right_table`, `right_alias`, `sql`, `comment`, and `instruction`.
- `genie_acl_groups` — list of group names that should have CAN_RUN access to this Genie Space. **Only include groups whose FGAC policies reference this space's tables.** For example, if Finance_Analyst has policies on `dev_fin.finance.*` tables, include them in the Finance Analytics space but NOT in the Clinical Analytics space. This ensures each space is only accessible to relevant roles.

### Output Format — Genie Space Config (in `abac.auto.tfvars`)

Include these variables alongside groups, tag_policies, etc.

**IMPORTANT**: Derive ALL values from the user's actual tables, columns, status/state values,
and domain. The structural example below uses generic placeholder names — do NOT reuse the
field values; replace every value with content specific to the user's DDL and industry.

```hcl
genie_space_title       = "<Domain> Analytics"
genie_space_description = "1-2 sentence summary of what the space covers and who it is for."

# 5-10 natural-language questions a business user in this domain would actually ask.
# Reference real table and column names from the user's DDL.
genie_sample_questions = [
  "What is the total <metric> by <dimension> for last quarter?",
  "Show the top 10 <entities> by <numeric_column>",
  "How many <records> were <status_value> last month?",
]

# Business defaults: look at status/state columns in the DDL and define which values
# are the default filter. Cover date conventions and masking awareness.
genie_instructions = "When asked about '<primary_entity>' without a status qualifier, default to <status_column> = '<default_status>'. 'Last month' means the previous calendar month. <Any masked columns> are masked for <restricted roles>."

# 3-5 benchmark Q&A pairs. Each question must be unambiguous — include explicit
# qualifiers (status, date range, entity type) so the question and SQL agree exactly.
genie_benchmarks = [
  {
    question = "What is the total <metric> for <explicit_qualifier> <entities>?"
    sql      = "SELECT <aggregate> FROM <catalog.schema.table> WHERE <status_column> = '<value>'"
  },
  {
    question = "How many <entities> were <status_value> last month?"
    sql      = "SELECT COUNT(*) FROM <catalog.schema.table> WHERE <date_col> >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL 1 MONTH) AND <date_col> < DATE_TRUNC('month', CURRENT_DATE) AND <status_col> = '<value>'"
  },
]

# Default WHERE clauses derived from the user's status/state columns.
genie_sql_filters = [
  {
    sql          = "<table_alias>.<status_column> = '<default_value>'"
    display_name = "<human readable filter name>"
    comment      = "Only include <entities> with <status> status"
    instruction  = "Apply when the user asks about <entities> without specifying a status"
  },
]

# Standard aggregate metrics from the user's numeric columns.
genie_sql_measures = [
  {
    alias        = "<snake_case_metric_name>"
    sql          = "<AGGREGATE>(<table_alias>.<numeric_column>)"
    display_name = "<human readable metric name>"
    comment      = "<What this metric measures>"
    instruction  = "Use when asked about <metric description>"
  },
]

# Computed dimensions from date/category columns.
genie_sql_expressions = [
  {
    alias        = "<snake_case_expression_name>"
    sql          = "<FUNCTION>(<table_alias>.<date_or_category_column>)"
    display_name = "<human readable name>"
    comment      = "<What this expression computes>"
    instruction  = "Use for <analysis type>"
  },
]

# Join relationships derived from matching foreign key columns in the DDL.
genie_join_specs = [
  {
    left_table  = "<catalog.schema.left_table>"
    left_alias  = "<left_alias>"
    right_table = "<catalog.schema.right_table>"
    right_alias = "<right_alias>"
    sql         = "<left_alias>.<fk_column> = <right_alias>.<pk_column>"
    comment     = "Join <left_table> to <right_table> on <key>"
    instruction = "Use when you need <right_table> context for <left_table> queries"
  },
]

# Groups that should have CAN_RUN access to this Genie Space.
# Only include groups whose FGAC policies reference this space's tables.
genie_acl_groups = [
  "<Group_Name_1>",
  "<Group_Name_2>",
]
```

---

### MY TABLES (paste below)

Tables are provided with fully qualified names (catalog.schema.table).
Derive the catalog and schema for each policy from the table's fully qualified name.

```text
-- Table DDLs are auto-fetched and pasted here.
-- Each table is fully qualified: my_catalog.my_schema.my_table
```
