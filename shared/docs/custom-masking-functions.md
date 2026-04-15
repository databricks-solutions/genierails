# Custom Masking Functions

How to author, test, and deploy custom SQL masking functions for use with GenieRails ABAC governance.

## When to Write Custom Functions

GenieRails generates masking functions automatically using country and industry overlays. Write custom functions when:

- Your data has domain-specific formats not covered by overlays (e.g., internal employee IDs, proprietary account formats)
- You need deterministic masking (same input always produces same output) for join-safe anonymization
- You need performance-optimized masking for high-volume queries
- Regulatory requirements mandate specific masking behavior

## Function Structure

Masking functions are SQL UDFs deployed to Unity Catalog. Each function takes a column value and returns the masked version:

```sql
CREATE OR REPLACE FUNCTION catalog.schema.mask_employee_id(val STRING)
  RETURNS STRING
  COMMENT 'Masks internal employee ID — shows department prefix only'
  RETURN CASE
    WHEN val IS NULL THEN NULL
    ELSE CONCAT(SUBSTRING(val, 1, 3), '-****')
  END;
```

### Key Requirements

1. **Always handle NULL** — return NULL for NULL input (masking NULL is meaningless and can break queries)
2. **Return the same type** — input STRING → output STRING, input DECIMAL → output DECIMAL
3. **Be deterministic** — same input should produce same output (required for query caching)
4. **Include a COMMENT** — explains the masking behavior for audit purposes

## Common Masking Patterns

### Pattern 1: Partial Reveal (Last N digits)

```sql
CREATE OR REPLACE FUNCTION mask_last4(val STRING)
  RETURNS STRING
  RETURN CASE
    WHEN val IS NULL THEN NULL
    WHEN LENGTH(val) <= 4 THEN REPEAT('*', LENGTH(val))
    ELSE CONCAT(REPEAT('*', LENGTH(val) - 4), RIGHT(val, 4))
  END;
```

### Pattern 2: Full Redaction

```sql
CREATE OR REPLACE FUNCTION mask_redact(val STRING)
  RETURNS STRING
  RETURN CASE WHEN val IS NULL THEN NULL ELSE '[REDACTED]' END;
```

### Pattern 3: Deterministic Hash (Join-Safe)

```sql
CREATE OR REPLACE FUNCTION mask_hash(val STRING)
  RETURNS STRING
  COMMENT 'SHA-256 hash — deterministic, join-safe across tables'
  RETURN CASE
    WHEN val IS NULL THEN NULL
    ELSE CONCAT('HASH_', SUBSTRING(SHA2(val, 256), 1, 12))
  END;
```

### Pattern 4: Amount Rounding

```sql
CREATE OR REPLACE FUNCTION mask_amount_round(val DECIMAL(18,2))
  RETURNS DECIMAL(18,2)
  COMMENT 'Rounds to nearest thousand for non-privileged users'
  RETURN CASE
    WHEN val IS NULL THEN NULL
    ELSE ROUND(val, -3)
  END;
```

### Pattern 5: Date Truncation (Year Only)

```sql
CREATE OR REPLACE FUNCTION mask_date_year(val DATE)
  RETURNS DATE
  COMMENT 'Truncates to January 1 of the same year (HIPAA Safe Harbor)'
  RETURN CASE
    WHEN val IS NULL THEN NULL
    ELSE DATE_TRUNC('YEAR', val)
  END;
```

## Adding Custom Functions to GenieRails

### Option 1: Edit the Generated SQL (Recommended)

After running `make generate`, edit `envs/<env>/generated/masking_functions.sql` to add your custom functions. Then reference them in `abac.auto.tfvars`:

```hcl
fgac_policies = [
  {
    name          = "mask_employee_id_policy"
    policy_type   = "COLUMN_MASK"
    function_name = "mask_employee_id"
    principals    = ["analysts", "marketing"]
    match_condition = {
      has_tag_value = { tag_key = "pii_level", tag_value = "masked_employee_id" }
    }
  },
]
```

### Option 2: Use Existing Functions

If masking functions already exist in your catalog, reference them directly in `abac.auto.tfvars` without adding them to the SQL file. Set `existing_masking_functions` in your env config:

```hcl
existing_masking_functions = [
  "my_catalog.my_schema.mask_employee_id",
]
```

## Testing Custom Functions

Test your functions before deploying to production:

```sql
-- Test with sample data
SELECT mask_employee_id('ENG-12345') AS masked;
-- Expected: ENG-****

-- Test NULL handling
SELECT mask_employee_id(NULL) AS masked_null;
-- Expected: NULL

-- Test edge cases
SELECT mask_employee_id('AB') AS short_input;
-- Expected: ** (or appropriate behavior for short inputs)
```

## Performance Considerations

- **Avoid expensive operations** in masking functions — they execute per-row on every query
- **Use built-in functions** (SUBSTRING, CONCAT, SHA2) rather than Python UDFs for better performance
- **Mark functions as DETERMINISTIC** if possible — enables query caching
- **Test with realistic data volumes** — a function that works on 100 rows may be slow on 100M rows
- Row filters are more expensive than column masks — use them sparingly

See [Performance & Scaling](performance-scaling.md) for detailed benchmarks.

## Registering in Overlay YAML (Advanced)

To make your custom function available to the LLM during generation, add it to a country or industry overlay YAML file under `masking_functions`:

```yaml
masking_functions:
  - name: mask_employee_id
    signature: "(val STRING) RETURNS STRING"
    description: "Masks internal employee ID — shows department prefix only"
    column_hints:
      - employee_id
      - emp_id
      - staff_number
```

This teaches the LLM to use your function when it sees matching column names.
