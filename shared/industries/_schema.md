# Industry Overlay Schema

Each YAML file in this directory defines an industry overlay for GenieRails.
The filename (without `.yaml`) is the code users set in `env.auto.tfvars` via
the `industry` field (e.g. `industry = "financial_services"`).

## File structure

```yaml
code: "financial_services"          # matches the filename (lowercase, underscores)
name: "Financial Services"
regulations:
  - "PCI DSS"
  - "SOX (Sarbanes-Oxley)"

identifiers:
  - name: "Account Number"
    column_hints:                   # lowercase substrings matched against column names
      - account_number
      - acct_no
    format: "Variable length numeric"   # human description of the format
    sensitivity: restricted             # restricted | confidential | public
    masking_function: mask_account_last4  # function name in masking_functions below
    category: financial_id              # validation category

masking_functions:
  - name: mask_account_last4
    signature: "mask_account_last4(acct STRING) RETURNS STRING"
    comment: "Account number — show last 4 digits only"
    body: |
      CASE
        WHEN acct IS NULL THEN NULL
        ...
      END

# Suggested group definitions for this industry (injected into prompt as guidance).
# The LLM may adapt these based on the actual table structure.
group_templates:
  fraud_team:
    description: "Full access to all financial data for fraud investigation"
    access_level: full        # full | masked | anonymized | de-identified | partial
  analyst:
    description: "Masked access to sensitive financial identifiers"
    access_level: masked

# Named access patterns specific to this industry.
# Injected into the prompt so the LLM can generate appropriate FGAC policies.
access_patterns:
  - name: audit_trail
    description: "All access to financial data must be logged for SOX compliance"
    guidance: "Ensure row-level audit logging is enabled for tables tagged with financial_id"

# Free-form markdown injected into the LLM prompt before ### MY TABLES.
# Should cover: identifiers, industry conventions, regulatory context,
# and guidance on when to prefer industry-specific masking functions.
prompt_overlay: |
  ### Industry-Specific Identifiers: Financial Services
  ...
```

## Adding a new industry

1. Create `<code>.yaml` following the structure above (use lowercase with underscores).
2. No code changes needed -- `generate_abac.py` discovers overlay files by filename.
3. Test with: `make generate INDUSTRY=<code> GENERATE_ARGS='--dry-run'`

## Composing with country overlays

Industry and country overlays are independent dimensions that compose additively:
```bash
make generate COUNTRY=ANZ INDUSTRY=healthcare
```
Both overlays are injected into the LLM prompt (countries first, then industries).

## Naming conventions

- Use lowercase with underscores: `financial_services`, `healthcare`, `retail`.
- Keep names descriptive but concise.
