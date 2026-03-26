# Country / Region Overlay Schema

Each YAML file in this directory defines a country or sub-region overlay for
GenieRails. The filename (without `.yaml`) is the code users set in
`env.auto.tfvars` via the `country` field (e.g. `country = "ANZ"`).

## File structure

```yaml
code: "ANZ"                    # matches the filename
name: "Australia & New Zealand"
regulations:
  - "Privacy Act 1988 (AU)"
  - "Privacy Act 2020 (NZ)"

identifiers:
  - name: "Tax File Number (TFN)"
    country: "AU"                       # which country within the region
    column_hints:                       # lowercase substrings matched against column names
      - tfn
      - tax_file_number
    format: "9 digits (NNN NNN NNN)"    # human description of the format
    sensitivity: restricted             # restricted | confidential | public
    masking_function: mask_tfn          # function name in masking_functions below
    category: government_id             # validation category (government_id | health_id | financial_id | ...)

masking_functions:
  - name: mask_tfn
    signature: "mask_tfn(tfn STRING) RETURNS STRING"
    comment: "Australian Tax File Number — show last 3 digits only"
    body: |
      CASE
        WHEN tfn IS NULL THEN NULL
        ...
      END

# Free-form markdown injected into the LLM prompt before ### MY TABLES.
# Should cover: identifiers, column naming conventions, regulatory context,
# and guidance on when to prefer country-specific masking functions.
prompt_overlay: |
  ### Country-Specific Identifiers: Australia & New Zealand
  ...
```

## Adding a new region

1. Create `<CODE>.yaml` following the structure above.
2. No code changes needed — `generate_abac.py` discovers overlay files by filename.
3. Test with: `make generate COUNTRY=<CODE> GENERATE_ARGS='--dry-run'`

## Naming conventions

- **Single country**: Use ISO 3166-1 alpha-2 (`JP`, `KR`, `DE`).
- **Sub-region**: Use a short mnemonic (`ANZ`, `SEA`, `DACH`).
- **Broad region**: Use a standard label (`EMEA`, `LATAM`, `APAC`).
