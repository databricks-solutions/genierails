# Advanced Usage

This document covers optional and advanced workflows that most first-time users can skip.

## Generation Options

```bash
make generate GENERATE_ARGS='--tables a.b.* c.d.e'
make generate GENERATE_ARGS='--dry-run'
```

If you want to run the script directly, do it from inside the env workspace and call the root-owned script:

```bash
cd envs/dev
python ../../generate_abac.py --tables "a.b.*" "c.d.e"
python ../../generate_abac.py --dry-run
```

Pass group names through Make:

```bash
make generate GENERATE_ARGS='--groups "Finance_Analyst,Clinical_Staff"'
```

## IDP-Synced Groups

When groups are managed by an identity provider such as Okta or Azure AD, keep group ownership out of workspace and `data_access` envs entirely. Those layers already look groups up by name. The only place that mentions `manage_groups` is `envs/account/env.auto.tfvars`, where it remains `true` if Terraform should own account-level group creation.

This changes behavior:

- Groups are looked up by name instead of created
- Workspace assignment and entitlements still run
- Any account-level `group_members` should stay empty in `envs/account/abac.auto.tfvars`

Use `--groups` to tell the LLM your exact IDP group names:

```bash
make generate GENERATE_ARGS='--groups "acme-finance-readers,acme-clinical-staff,acme-compliance"'
```

The LLM uses these exact names in generated FGAC policies, tag assignments, and Genie Space ACLs.

## ABAC-Only Mode (No Genie Space)

See [playbook.md — ABAC governance only](playbook.md#abac-governance-only-no-genie-space) for the full step-by-step.

## Existing Masking Functions

If you have pre-existing masking SQL UDFs, the tool can incorporate them:

1. Run `make generate` so the AI creates `masking_functions.sql` and `abac.auto.tfvars` in `envs/dev/generated/`
2. Edit `envs/dev/generated/masking_functions.sql` and replace generated UDF definitions with your existing functions
3. Update `function_name`, `function_catalog`, and `function_schema` in `envs/dev/generated/abac.auto.tfvars` to match your existing UDFs
4. Run `make apply`

## Multi-Environment File Layout

For day-to-day workflows (promote, independent BU, self-service Genie) see [playbook.md](playbook.md). This section documents the directory structure those workflows produce.

Workspace environment names can be anything: `dev`, `staging`, `prod`, `bu2`, or something business-unit-specific. `account` and `data_access` are reserved names.

```text
envs/
  account/
    auth.auto.tfvars
    env.auto.tfvars
    abac.auto.tfvars
    terraform.tfstate

  dev/
    auth.auto.tfvars
    env.auto.tfvars
    data_access/
      auth.auto.tfvars
      env.auto.tfvars
      abac.auto.tfvars
      masking_functions.sql
      terraform.tfstate
    abac.auto.tfvars
    ddl/
    generated/
    .genie_space_id
    terraform.tfstate

  prod/
    (same structure as dev/)

roots/
  account/
  data_access/
  workspace/
```

Each env keeps its own Terraform state and local artifacts. `account` is the only shared layer; governance and workspace files are isolated per environment under `envs/<env>/data_access/` and `envs/<env>/`.

`make sync-tags` runs against the shared account layer because tag policy definitions are account-scoped.

## Migrating an Existing Root-Based Workspace

If you already used the old root-local workflow, migrate it once before using the new default env dispatch:

```bash
make migrate-root-to-env ENV=dev
make migrate-state ENV=dev
```

That moves root working files into `envs/dev/` and rewrites any legacy top-level Terraform addresses into the new layered module addresses so future `make generate` and `make apply` commands continue from the same environment layout without forced recreation.

## Examples

Pre-built examples with 3-layer configs (account, data access, workspace) are available in:
- `examples/finance/` — 5-group finance demo with PII, PCI, and AML governance
- `examples/healthcare/` — 6-group healthcare demo with HIPAA-compliant PHI, PII, and regional row filters ([walkthrough](../examples/healthcare/healthcare_walkthrough.md))
