# Playbook

GenieRails puts Genie onboarding on rails: Unity Catalog's classifier decides what's sensitive, GenieRails derives one protection per column and proves coverage with a blocking check, then promotes the rules safely to production. This playbook covers common tasks after your first deployment; for the full end-to-end flow, see the **[Dev-to-Prod Walkthrough](../examples/dev_to_prod/README.md)**.

## Pick your starting point

| Starting point | You have... | Guide |
|---|---|---|
| **I already have a Genie agent** | An agent configured in the Databricks UI that needs governance and promotion to prod | [Import a Genie Agent from UI into Code](import-genie-agent-from-ui.md) |
| **I'm starting from scratch** | Tables in Unity Catalog, no Genie agent yet | [Quickstart](quickstart.md) |

> Both routes converge on the **[dev-to-prod walkthrough](../examples/dev_to_prod/README.md)** — the canonical end-to-end walkthrough (native classification → coverage check → safe dev→prod promotion).

---

## After your first deployment

### Add another Genie agent

Add a second space without re-generating existing ones:

```bash
# 1. Add Space B to env.auto.tfvars alongside your existing space(s)
vi envs/dev/env.auto.tfvars

# 2. Generate ONLY Space B's config — existing spaces are preserved
make generate SPACE="Clinical Analytics"

# 3. Review, prove coverage, and apply
vi envs/dev/generated/abac.auto.tfvars   # verify existing agents are unchanged
make coverage-gate ENV=dev               # blocks if any classified column is unprotected
make validate-generated
make rehearse ENV=dev                    # apply (grants only if coverage passes) + verify-access
```

| Situation | Command |
| --------- | ------- |
| Adding a new space without touching existing ones | `make generate SPACE="Space B"` |
| Re-tuning a single space from scratch | `make generate SPACE="Finance Analytics"` |
| Adding new groups or changing shared tag policies | `make generate` (full — reviews all spaces) |

> Per-agent generation does **not** modify groups or tag_policies. Groups are your IdP-synced groups (consumed, not created); a full `make generate` re-selects them across all agents.

### Promote dev → prod

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod \
  DEST_CATALOG_MAP="dev_catalog=prod_catalog"

vi envs/prod/auth.auto.tfvars             # enter prod workspace credentials

# prod re-derives its OWN facts — never re-run generate in prod
make enable-classification ENV=prod       # or the Databricks UI (recommended); then wait for prod class.* tags
make release ENV=prod VERIFY_KEY_COLUMN=<key>   # derive (no LLM) → validate → coverage check → audit → apply → verify-access
```

Promotion carries the reviewed **rules** (mapping, masks, policies, Genie config) — *not* dev's tag assignments. Prod establishes its own facts from its own classification scan, so dev data never decides what's protected in prod.

For multiple catalogs:

```bash
make promote SOURCE_ENV=dev DEST_ENV=prod \
  DEST_CATALOG_MAP="dev_fin=prod_fin,dev_clinical=prod_clinical"
```

**How `DEST_CATALOG_MAP` works:**

- Comma-separated `src_catalog=dest_catalog` pairs
- The promote command auto-detects all source catalog names from `genie_spaces[*].uc_tables`
- Every detected catalog must have a mapping — the command fails clearly if any are missing

> If you imported an existing agent with [Import a Genie Agent from UI into Code](import-genie-agent-from-ui.md), continue with the same promotion steps below.

### Country and industry overlays

Add region-specific or industry-specific governance context:

```bash
# Country overlays (ANZ, India, Southeast Asia)
make generate COUNTRY=ANZ
make generate COUNTRY=ANZ,IN,SEA

# Industry overlays (Financial Services, Healthcare, Retail)
make generate INDUSTRY=healthcare
make generate INDUSTRY=financial_services,retail

# Both combined
make generate COUNTRY=ANZ INDUSTRY=healthcare
```

Overlays **tune the rules** (region/industry-specific masking context and functions) applied *after* classification — they do **not** decide what's sensitive. Unity Catalog's native `class.*` classification remains the authoritative sensitivity source. These work with any scenario — just add the flag. See [Country Overlays](country-overlays.md) and [Industry Overlays](industry-overlays.md) for details.

### Schema drift detection

After ABAC governance is deployed, table schemas may evolve. These commands handle drift without a full `make generate` re-run:

```bash
# Detect untagged columns and stale assignments
make audit-schema ENV=dev

# Detect prod-applied tags that no policy or mask covers ("new prod tag, no rule")
make audit-rulebook ENV=dev

# Preferred: let native classification tag new columns, then re-derive deterministically (no LLM)
make derive-assignments ENV=dev
make apply ENV=dev
# (make generate-delta is an exceptional/legacy LLM path — not the routine dev-to-prod walkthrough)
```

`audit-schema` and `audit-rulebook` are two complementary directions of drift:

| Command | Direction | Detects |
| --- | --- | --- |
| `audit-schema` (forward) | column → rule | sensitive columns with no governed tag |
| `audit-schema` (reverse) | rule → column | tag assignments whose column no longer exists |
| `audit-rulebook` | applied tag → rule | tags actually applied in the workspace (`class.*` classification + governed keys) that no `tag_policy` declares and no `fgac_policy` mask/row-filter references |

`make audit-rulebook` reads `system.information_schema.column_tags` for the managed
tables and compares each applied tag key/value against the RULEBOOK (`tag_policies` +
column-mask `fgac_policies` in the config). It catches the case where an automatic
classification or an out-of-band `SET TAGS` lands a tag in production that the
governance config never anticipated — so no mask will act on it. Coverage is matched
per catalog (a mask in one catalog does not cover a tag in another) and only column
masks count; a row filter's `when_condition` targets table tags, not column tags.

The `make` targets each run a fixed mode (`audit-schema` → forward + reverse,
`audit-rulebook` → rulebook). To run all three checks in one pass, invoke the script
directly from the env directory:

```bash
cd envs/dev && python3 "$SHARED_ROOT/scripts/audit_schema_drift.py" --mode all
```

| Schema change | What happens |
| --- | --- |
| `ALTER TABLE ADD COLUMN patient_ssn STRING` | `generate-delta` classifies and tags it |
| `ALTER TABLE DROP COLUMN old_ssn` | `generate-delta` removes the stale assignment |
| `ALTER TABLE RENAME COLUMN ssn TO tax_id` | Old assignment removed, new column classified |
| Auto-classifier sets `class.pii` on a new column | `audit-rulebook` flags it — no covering policy or mask |

---

## Advanced scenarios

These cover less common deployment patterns. Most users won't need them on day one. **The commands below show the scenario-specific mechanics only** — each still runs through the dev-to-prod checks: consume your IdP groups (`--groups`, `manage_groups=false`), rehearse in dev, and in prod use `make release` (which re-derives with `derive-assignments`, not `generate`, and grants only when the coverage check passes).

### ABAC governance only (no Genie agent)

Set up groups, tag policies, column masking, row filters, and catalog grants — without creating any Genie agent. Add Genie later without changing the governance setup.

```bash
make setup
vi envs/dev/env.auto.tfvars   # list tables in uc_tables, no genie_spaces block

make generate
make validate-generated
make apply
```

### Independent BU environment

A second business unit needs its own groups, governance, and Genie agents — not a promotion of `dev`.

```bash
make setup ENV=bu2
vi envs/bu2/env.auto.tfvars    # define the BU's genie_spaces

make generate ENV=bu2
make validate-generated ENV=bu2
make apply ENV=bu2
```

### Central governance, self-service Genie

A central Data Governance team owns ABAC policies, while BU teams self-serve their own Genie agents. See [self-service-genie.md](self-service-genie.md) for the full guide.

```bash
# Governance team
make generate ENV=governance MODE=governance
make apply-governance ENV=governance

# BU team
make generate ENV=bu1 MODE=genie
make apply-genie ENV=bu1
```

### Import Genie agent to prod without ABAC

Import a UI-created Genie agent and deploy to production when ABAC is managed separately. See [self-service-genie.md](self-service-genie.md) for context.

```bash
make generate ENV=bu_import MODE=genie   # genie_only=true in env.auto.tfvars
make apply-genie ENV=bu_import
make promote SOURCE_ENV=bu_import DEST_ENV=bu_import_prod DEST_CATALOG_MAP="dev=prod"
make apply-genie ENV=bu_import_prod
```

---

## Destroy and reset

```bash
make destroy ENV=dev               # workspace + data_access layers
make destroy ENV=prod
make destroy ENV=account           # shared account layer (groups, tag policies)
make destroy-genie ENV=bu1         # workspace layer only
make destroy-governance ENV=gov    # data_access layer only
make clean ENV=dev                 # remove local state, keep config
make clean-all                     # remove all envs/
```

---

## How it works

See [Architecture](architecture.md) for the full reference. Quick summary:

`make apply ENV=<name>` applies in this order:

1. `envs/account/` — shared account layer (groups, tag policies)
2. `envs/<name>/data_access/` — env-scoped governance (tags, masking, grants)
3. `envs/<name>/` — workspace layer (Genie agents, ACLs)

The core loop (dev-to-prod walkthrough):

```
enable-classification → wait for class.* → make generate (--groups) → coverage-gate
  → review generated/ → validate-generated → make rehearse   (prod: make release ENV=prod)
```
Prod swaps `generate` for `derive-assignments` (re-derive facts from prod's own tags, no LLM).

| File | What it contains |
| ---- | ---------------- |
| `generated/abac.auto.tfvars` | Groups, tag policies, tag assignments, FGAC policies, and semantic `genie_space_configs`; it does not own per-space ACL intent |
| `generated/masking_functions.sql` | SQL masking and row-filter functions |
| `generated/spaces/<key>/` | Per-space drafts (used by `make generate SPACE="..."`) |

Per-space `acl_groups` is user-owned on `genie_spaces[]` in `env.auto.tfvars`.
Omitted or `null` means derive fresh from current policy principals (including
`except_principals`); `[]` explicitly means nobody; a non-empty list is a
durable override. Generated or imported semantic drafts cannot override it.
