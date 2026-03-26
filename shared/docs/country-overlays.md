# Country / Region Overlays

> **Contributors:** Jump to [Adding a new country](#adding-a-new-country) for a step-by-step guide. No Python, Terraform, or Makefile changes are needed — just a single YAML file.

---

## Overview

By default, the ABAC generator produces governance rules using US-centric PII patterns (SSN, credit card, HIPAA). The **country overlay** system injects region-specific identifier knowledge — column patterns, masking functions, and regulatory context — into the LLM prompt so it produces governance appropriate for non-US datasets.

Each overlay is a self-contained YAML file under `shared/countries/`.

### Supported regions

| Code | Region | Key identifiers |
|------|--------|-----------------|
| `ANZ` | Australia & New Zealand | TFN, Medicare, BSB, IRD, NHI |
| `IN` | India | Aadhaar, PAN, GSTIN, IFSC, UPI |
| `SEA` | Singapore & Malaysia | NRIC, FIN, MyKad, UEN, EPF |

---

## How to use

### 1. Set the country in your environment

In `envs/<env>/env.auto.tfvars`:

```hcl
country = "ANZ"            # Single region
country = "ANZ,SEA"        # Multi-region dataset
country = ""               # US/global defaults (no overlay)
```

### 2. Generate and apply

```bash
make generate ENV=dev
make apply ENV=dev
```

Or override the country via CLI without editing the file:

```bash
make generate ENV=dev COUNTRY=ANZ
make generate ENV=dev COUNTRY=ANZ,IN,SEA
```

CLI `COUNTRY=` takes priority over the `country` field in `env.auto.tfvars`.

The generated output will include country-specific masking UDFs (e.g. `mask_tfn`, `mask_aadhaar`) and ABAC policies referencing those functions. Country overlays work with all modes (`MODE=governance`, `MODE=genie`, or full ABAC).

---

## How it works

The YAML overlay plugs into the generate and validate stages — the apply stage is unchanged.

```
┌─────────────────────────────────────────────────────────────────┐
│  1. CONFIGURE                                                   │
│                                                                 │
│  env.auto.tfvars:  country = "ANZ"                              │
│  (or CLI:          make generate COUNTRY=ANZ)                   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  2. GENERATE  (make generate)                                   │
│                                                                 │
│  ┌──────────────────────┐    ┌────────────────────────────────┐ │
│  │ shared/countries/    │    │ LLM prompt                     │ │
│  │   ANZ.yaml           │───▶│                                │ │
│  │                      │    │ [US defaults]                  │ │
│  │ • identifiers (TFN,  │    │ + [ANZ overlay: TFN, Medicare, │ │
│  │   Medicare, BSB...)  │    │    BSB, regulations, masking   │ │
│  │ • masking functions  │    │    function signatures]        │ │
│  │ • prompt_overlay     │    │ + [your table DDLs]            │ │
│  └──────────────────────┘    └───────────────┬────────────────┘ │
│                                              │                  │
│                                              ▼                  │
│                              ┌────────────────────────────────┐ │
│                              │ LLM output                     │ │
│                              │ • masking_functions.sql         │ │
│                              │ • abac.auto.tfvars              │ │
│                              └───────────────┬────────────────┘ │
└──────────────────────────────────────────────┼──────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  3. VALIDATE  (make validate)                                   │
│                                                                 │
│  ANZ.yaml identifiers extend the validation rules:              │
│  • Column hints:    tfn, tax_file_number  →  government_id      │
│  • Function checks: mask_tfn must cover government_id columns   │
│  • All US rules still apply — ANZ rules are additive            │
└──────────────────────────────────────────────┬──────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  4. APPLY  (make apply)                                         │
│                                                                 │
│  Deploys to Databricks (no country-specific logic here):        │
│  • Creates masking UDFs, tag assignments, FGAC policies         │
│  • Sets up Genie Spaces                                         │
└─────────────────────────────────────────────────────────────────┘
```

**Key insight:** The YAML file teaches the LLM about your region's identifiers (generate) and extends the validation rules (validate). The apply stage deploys whatever the LLM produced — no country-specific logic.

---

## Adding a new country

### Step 1: Create the YAML file

Create `shared/countries/<CODE>.yaml` using an existing file (e.g. `ANZ.yaml`) as a template. Use a short uppercase code — typically an ISO code (`JP`, `KR`, `BR`) or a region code (`SEA`, `ANZ`, `EU`).

Here's the structure with inline comments explaining each field:

```yaml
code: ANZ                              # Must match filename (ANZ.yaml → code: ANZ)
name: Australia & New Zealand          # Human-readable name

regulations:                           # Key data protection laws for the region
  - Privacy Act 1988 (AU)
  - Privacy Act 2020 (NZ)

identifiers:                           # Country-specific PII/sensitive identifiers
  - name: Tax File Number (TFN)       # Human-readable identifier name
    country: AU                        # ISO country code within region
    column_hints:                      # Lowercase substrings matched against column names
      - tfn                            #   (include common misspellings!)
      - tax_file_number
      - tax_file_no
    format: "9 digits (NNN NNN NNN)"   # Optional — informational, included in prompt
    sensitivity: restricted            # Optional — restricted | confidential | public
    masking_function: mask_tfn         # UDF name from masking_functions, or null if public
    category: government_id            # One of: government_id, health_id,
                                       #   financial_id, business_id

masking_functions:                     # SQL UDF definitions (Databricks SQL)
  - name: mask_tfn
    signature: "mask_tfn(val STRING) RETURNS STRING"
    comment: "Masks Australian Tax File Number — reveals last 3 digits"
    body: |
      CASE
        WHEN val IS NULL THEN NULL
        WHEN LENGTH(REGEXP_REPLACE(val, '[^0-9]', '')) < 9 THEN '***-***-***'
        ELSE CONCAT('***-***-', RIGHT(REGEXP_REPLACE(val, '[^0-9]', ''), 3))
      END

prompt_overlay: |                      # Markdown injected into the LLM prompt (>100 chars)
  ### Country-Specific Identifiers: Australia & New Zealand

  **Regulatory context:** ...

  **Identifier summary:**
  | Identifier | Column hints | Masking function |
  |------------|-------------|-----------------|
  | TFN | tfn, tax_file_number | mask_tfn |

  **Available masking functions:**
  - `mask_tfn(val STRING)` — Masks TFN, reveals last 3 digits

  **Disambiguation warnings:**
  - <Gotcha: e.g. "PAN in India is a tax ID, not a credit card PAN">
```

**Tips for masking functions:**
- Always handle `NULL` as the first branch
- Strip non-digit characters before length checks: `REGEXP_REPLACE(val, '[^0-9]', '')`
- Reveal the minimum necessary — typically last 3-4 digits
- If an identifier encodes sensitive data in its structure (like MyKad encoding DOB), mask more aggressively

**Tips for prompt_overlay:**
- This is the most important part — it's what the LLM actually reads
- Include disambiguation warnings for identifiers that could be confused (e.g. India's PAN vs credit card PAN)
- If the LLM consistently misses an identifier, add a stronger instruction (e.g. "You MUST use mask_tfn for any column matching tfn")

### Step 2: Test it

The system auto-discovers YAML files — no code changes needed:

```bash
make generate ENV=dev COUNTRY=<CODE>
make validate ENV=dev COUNTRY=<CODE>
```

The existing `TestYamlFileIntegrity` test class automatically validates new YAML files (structure, required fields, cross-references). You can optionally add content-specific tests to `shared/tests/test_country_overlays.py` — see existing country test classes for the pattern.

---

## FAQ

**What if the LLM ignores the country overlay?**
Make `column_hints` more specific, add disambiguation warnings in `prompt_overlay`, or add stronger instructions (e.g. "You MUST use mask_tfn for any column matching tfn").

**What if an identifier doesn't need masking?**
Set `masking_function: null`. The identifier is still listed so the LLM knows not to mask it unnecessarily (e.g. India's IFSC code is public bank routing info).

**Do I need to update Terraform?**
No. Country overlays only affect generation and validation. Terraform deploys whatever the LLM produces.
