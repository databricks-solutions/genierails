# Industry Overlays

> **Contributors:** Jump to [Adding a new industry](#adding-a-new-industry) for a step-by-step guide. No Python, Terraform, or Makefile changes are needed — just a single YAML file.

---

## Overview

> **Overlays tune the *rules*, not the *sensitivity decision* or your *groups*.** In the champion flow, **Unity Catalog native Data Classification** (`class.*`) is the authoritative source of what's sensitive (one `gr_treatment`/column derived from it), and your **access-tier groups come from your IdP** (consumed via `--groups`, `manage_groups=false`). An industry overlay only adds **industry-specific masking functions, regulatory context, and prompt hints** to generation — it never decides sensitivity, never creates groups, and never proves coverage (that's `make coverage-gate`).

The **industry overlay** system injects industry-specific identifier knowledge — column patterns, masking functions, group templates, access patterns, and regulatory context — into the generation prompt so GenieRails drafts industry-appropriate rules. Each overlay is a self-contained YAML file under `shared/industries/`.

### Supported industries

| Code | Industry | Key identifiers | Key regulations |
|------|----------|-----------------|-----------------|
| `financial_services` | Financial Services | Account Number, Credit Card, Transaction Amount, SSN | PCI DSS, SOX, GLBA |
| `healthcare` | Healthcare | Patient ID, Diagnosis Code, Medical Record, DOB | HIPAA, HITECH, 42 CFR Part 2 |
| `retail` | Retail & E-Commerce | Email, Phone, Address, Loyalty Card, IP Address | CCPA, GDPR, CAN-SPAM |

### What's different from country overlays?

Industry overlays share the same core structure (identifiers, masking functions, prompt overlay) but add two additional sections:

- **Group templates** — suggested access-tier *shapes* (e.g. `fraud_team: full`, `analyst: masked`), injected into the prompt as guidance only. They are **not** created and **not** enforced: your real groups come from your IdP (`--groups`), and the overlay conveys the *idea* of tiers, not group identity.
- **Access patterns** — named patterns like `break_glass` (healthcare) or `pci_isolation` (financial services) with implementation guidance.

Industry and country overlays are independent dimensions that compose additively. Use both together when your dataset spans a specific region *and* industry.

---

## How to use

### 1. Set the industry in your environment

In `envs/<env>/env.auto.tfvars`:

```hcl
industry = "healthcare"              # Single industry
industry = "financial_services,retail"  # Multi-industry dataset
industry = ""                        # No industry overlay (default)
```

### 2. Generate and apply

Enable native classification first (sensitivity is native; the overlay only adds industry rule context), then prove coverage before applying:

```bash
make enable-classification ENV=dev   # then wait for class.* tags
make generate ENV=dev GENERATE_ARGS='--groups "<your-idp-groups>"'
make coverage-gate ENV=dev           # blocks if any classified column is unprotected
make apply ENV=dev                   # business_access_enabled stays false until you verify + open the gate
```

Or override the industry via CLI without editing the file:

```bash
make generate ENV=dev INDUSTRY=healthcare
make generate ENV=dev INDUSTRY=financial_services,retail
```

CLI `INDUSTRY=` takes priority over the `industry` field in `env.auto.tfvars`.

### 3. Compose with country overlays

```bash
# ANZ healthcare dataset
make generate ENV=dev COUNTRY=ANZ INDUSTRY=healthcare

# Indian financial services
make generate ENV=dev COUNTRY=IN INDUSTRY=financial_services
```

Both overlays are injected into the generation prompt (countries first, then industries) so the LLM drafts rules informed by both regulatory contexts. Sensitivity still comes from native classification, and `make coverage-gate` still gates the result.

---

## How it works

The YAML overlay plugs into the generate and validate stages — the apply stage is unchanged.

```
┌─────────────────────────────────────────────────────────────────┐
│  1. CONFIGURE                                                   │
│                                                                 │
│  env.auto.tfvars:  industry = "healthcare"                      │
│  (or CLI:          make generate INDUSTRY=healthcare)           │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  2. GENERATE  (make generate)                                   │
│                                                                 │
│  ┌──────────────────────┐    ┌────────────────────────────────┐ │
│  │ shared/industries/   │    │ LLM prompt                     │ │
│  │   healthcare.yaml    │───▶│                                │ │
│  │                      │    │ [US defaults]                  │ │
│  │ • identifiers        │    │ + [country overlay, if set]    │ │
│  │ • masking functions  │    │ + [industry overlay: Patient   │ │
│  │ • group templates    │    │    ID, Diagnosis, break-glass, │ │
│  │ • access patterns    │    │    group templates, HIPAA]     │ │
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
│  healthcare.yaml identifiers extend the validation rules:       │
│  • Column hints:    patient_id, mrn  →  patient_id category     │
│  • Function checks: mask_patient_id must cover patient columns  │
│  • All US rules still apply — industry rules are additive       │
└──────────────────────────────────────────────┬──────────────────┘
                                               │
                                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  4. APPLY  (make apply)                                         │
│                                                                 │
│  Deploys to Databricks (no industry-specific logic here):       │
│  • Creates masking UDFs, tag assignments, FGAC policies         │
│  • Sets up Genie agents                                         │
└─────────────────────────────────────────────────────────────────┘
```

**Key insight:** The YAML overlay adds industry-specific masking functions, group-tier *shapes*, and prompt context to *generation*, and extends *validation*. It does **not** decide sensitivity (native `class.*` does) or create groups (your IdP does), and `make coverage-gate` proves coverage separately. The apply stage deploys whatever was generated — no industry-specific logic.

---

## Industry details

### Financial Services

**Regulations:** PCI DSS, SOX, GLBA, BSA/AML

**Masking approach:** Partial masking — show last 4 digits for account numbers and cards (PCI compliant). Transaction amounts rounded to nearest thousand for non-privileged users.

**Group templates:**
| Group | Access Level | Purpose |
|-------|-------------|---------|
| `fraud_team` | full | AML/fraud investigation |
| `senior_analyst` | full | Risk analysis |
| `analyst` | masked | Masked PII, rounded amounts |
| `marketing` | anonymized | No raw identifiers |
| `compliance_officer` | full | SOX/AML audit compliance |

**Access patterns:**
- `audit_trail` — SOX-compliant audit logging for financial data access
- `pci_isolation` — PCI DSS isolation of cardholder data

### Healthcare

**Regulations:** HIPAA, HITECH, 42 CFR Part 2

**Masking approach:** Full masking by default — patient IDs and medical records are fully redacted. Diagnosis codes show category only (first 3 characters). DOB shows year only (HIPAA Safe Harbor).

**Group templates:**
| Group | Access Level | Purpose |
|-------|-------------|---------|
| `physician` | full | Full patient data access |
| `nurse` | full | Demographics and clinical data |
| `analyst` | de-identified | No direct identifiers |
| `billing_clerk` | partial | Insurance/procedure codes only |
| `researcher` | de-identified | HIPAA Safe Harbor datasets |

**Access patterns:**
- `break_glass` — Emergency override for clinical staff with mandatory audit review
- `minimum_necessary` — HIPAA minimum necessary standard enforcement
- `substance_use_protection` — Extra protection for 42 CFR Part 2 records

### Retail

**Regulations:** CCPA, GDPR, CAN-SPAM, PCI DSS

**Masking approach:** Light masking — hashing for names (preserves join consistency), partial masking for phone/email, IP anonymization via last-octet zeroing.

**Group templates:**
| Group | Access Level | Purpose |
|-------|-------------|---------|
| `marketing` | anonymized | Hashed identifiers for campaigns |
| `customer_support` | partial | Contact info for active cases |
| `ops` | partial | Order/logistics, masked PII |
| `data_science` | anonymized | Hashed identifiers for modeling |
| `store_manager` | masked | Store-level aggregates |

**Access patterns:**
- `right_to_deletion` — CCPA/GDPR erasure request support
- `consent_based_access` — Marketing access gated on customer consent

---

## Adding a new industry

### Step 1: Create the YAML file

Create `shared/industries/<code>.yaml` using an existing file (e.g. `healthcare.yaml`) as a template. Use lowercase with underscores (e.g. `financial_services`, `insurance`, `telecom`).

Here's the structure with inline comments:

```yaml
code: healthcare                      # Must match filename (lowercase, underscores)
name: Healthcare                      # Human-readable name

regulations:                          # Key data protection laws for the industry
  - HIPAA
  - HITECH

identifiers:                          # Industry-specific sensitive identifiers
  - name: Patient ID                  # Human-readable identifier name
    column_hints:                     # Lowercase substrings matched against column names
      - patient_id
      - mrn
    format: "Alphanumeric"            # Optional — informational
    sensitivity: restricted           # restricted | confidential | public
    masking_function: mask_patient_id # UDF name from masking_functions below
    category: patient_id              # Validation category

masking_functions:                    # SQL UDF definitions (Databricks SQL)
  - name: mask_patient_id
    signature: "mask_patient_id(pid STRING) RETURNS STRING"
    comment: "Patient ID — full redaction (HIPAA PHI)"
    body: |
      CASE
        WHEN pid IS NULL THEN NULL
        ELSE '[REDACTED]'
      END

group_templates:                      # Suggested ABAC group definitions
  physician:
    description: "Full access to all patient data"
    access_level: full                # full | masked | anonymized | de-identified | partial
  analyst:
    description: "De-identified access only"
    access_level: de-identified

access_patterns:                      # Named industry-specific access patterns
  - name: break_glass
    description: "Emergency clinical access override"
    guidance: "Create a dedicated group with temporary full access..."

prompt_overlay: |                     # Markdown injected into the LLM prompt (>100 chars)
  ### Industry-Specific Identifiers: Healthcare
  ...
```

**Tips for group templates:**
- These are suggestions, not enforced — the LLM may adapt them based on actual table structure
- Include a range of access levels (full, masked, anonymized) to model realistic role hierarchies
- Name groups using common industry terminology

**Tips for access patterns:**
- Describe the pattern and provide concrete implementation guidance
- Include audit and compliance requirements where applicable
- The LLM uses this guidance when designing FGAC policies

### Step 2: Test it

The system auto-discovers YAML files — no code changes needed:

```bash
# Dry run (no LLM call, just check overlay loading)
make generate ENV=dev INDUSTRY=<code> GENERATE_ARGS='--dry-run'

# Full generation
make generate ENV=dev INDUSTRY=<code>
make validate ENV=dev INDUSTRY=<code>
```

Run the unit tests to validate your YAML structure:

```bash
pytest shared/tests/test_industry_overlays.py -v
```

The existing `TestIndustryYamlFileIntegrity` test class automatically validates new YAML files (structure, required fields, cross-references). You can optionally add content-specific tests — see existing industry test classes for the pattern.

### Integration testing

The `industry-overlay` integration test scenario runs end-to-end against a live Databricks workspace:

```bash
# Run the industry overlay scenario only
cd aws/   # or azure/
make test-industry-overlay

# Or run all scenarios (includes industry-overlay)
make test-ci
```

The scenario tests:
1. Each industry individually (financial\_services, healthcare, retail) — full generate + apply + verify cycle
2. Multi-industry composition (`INDUSTRY=financial_services,healthcare,retail`)
3. Country + industry composition (`COUNTRY=ANZ INDUSTRY=healthcare`)
4. Baseline without `INDUSTRY=` (no regression)

See [Integration Testing](integration-testing.md) for full details.

---

## FAQ

**What if an industry identifier isn't getting masked?**
First confirm native classification tagged the column (`class.*`) — that's what drives protection. The overlay only supplies masking functions and prompt context; if a type isn't recognized by UC classification, add a custom classifier. Strengthening `column_hints`/`prompt_overlay` only affects the LLM's rule drafting.

**What if an identifier doesn't need masking?**
Set `masking_function: null`. The identifier is still listed so the LLM knows not to mask it unnecessarily.

**Can I use multiple industries?**
Yes: `make generate INDUSTRY=financial_services,retail`. All overlays are merged additively.

**Can I combine industry and country overlays?**
Yes: `make generate COUNTRY=ANZ INDUSTRY=healthcare`. Country overlays are injected first, then industry overlays. The LLM sees both contexts.

**Do I need to update Terraform?**
No Terraform *code* changes. The overlay's masking functions are written into the generated `masking_functions.sql` and deployed, but there's no industry-specific Terraform logic, and `make coverage-gate` still gates it.

**How do group templates work?**
They are prompt suggestions for access-tier *shapes* only — GenieRails does **not** create groups. In the champion consume model your real groups come from your IdP (supplied via `--groups`, `manage_groups=false`); the templates just help the LLM reason about tiering. Map your actual IdP group names to tiers at generate time.

**What about access patterns like break-glass?**
Access patterns provide implementation guidance to the LLM. For break-glass, the LLM will typically create a dedicated group with `except_principals` to override masking. You should review the generated output to ensure the pattern is correctly implemented.
