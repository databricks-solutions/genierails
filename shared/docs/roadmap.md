# Roadmap: Future Improvements

Planned features and improvements identified during the comprehensive project review. Each item includes a design sketch and estimated effort.

## 1. Rollback Mechanism

**Status:** Planned | **Effort:** Medium

Currently, there's no `make rollback` command. Users must manually manage Terraform state to undo changes.

**Design:**
- `make rollback ENV=<env> GENERATION=<n>` restores a previous generated config
- Store generation history in `envs/<env>/generated/.history/` (timestamped copies of abac.auto.tfvars + masking_functions.sql)
- Rollback = copy historical version back to `generated/`, then `make apply`
- Keep last 5 generations by default (configurable via `MAX_GENERATIONS`)

**Implementation notes:**
- Add a `_save_generation_snapshot()` call in `generate_abac.py` after successful generation
- Add a `rollback` target to `Makefile.shared` that lists snapshots and restores the selected one
- Terraform handles the actual resource changes via normal `apply`

## 2. Live State Validation

**Status:** Planned | **Effort:** Medium

Currently, validation only runs against the generated `.tfvars` files. If someone manually edits governance in the Databricks UI, Terraform will overwrite it on next `apply`.

**Design:**
- `make validate-live ENV=<env>` compares deployed Databricks state against config
- Checks: tag assignments exist, FGAC policies match, masking functions exist, Genie Space ACLs match
- Reports drift as a structured diff (added/removed/modified)

**Implementation notes:**
- Use Databricks SDK to query live state (`w.tag_assignments.list()`, `w.fgac_policies.list()`)
- Compare against parsed `abac.auto.tfvars`
- Output: table of differences with suggested `make import` or `make apply` commands

## 3. Multi-Workspace Governance

**Status:** Planned | **Effort:** High

GenieRails is designed for single workspace per `ENV`. Large organizations need shared governance across workspaces.

**Design:**
- Account layer (groups, tag policies) is already shared across workspaces — no change needed
- Data access layer (tag assignments, FGAC policies) is per-catalog, not per-workspace — no change needed
- Workspace layer (Genie Spaces, ACLs) IS per-workspace — needs a "workspace mesh" mode

**Proposed "workspace mesh" mode:**
```
envs/
  account/          # shared across all workspaces
  dev/
    data_access/    # shared governance for dev catalogs
    workspace_a/    # Genie Spaces for workspace A
    workspace_b/    # Genie Spaces for workspace B
  prod/
    data_access/    # shared governance for prod catalogs
    workspace_c/    # Genie Spaces for workspace C
```

**Implementation notes:**
- Each workspace subdirectory has its own `auth.auto.tfvars` (different workspace host)
- `make apply ENV=dev` applies data_access once, then loops through workspace subdirs
- Self-service Genie mode already supports this pattern — generalize it

## 4. Masking Observability

**Status:** Planned | **Effort:** High

No built-in metrics on masking function execution. Users can't tell if functions are slow, erroring, or being bypassed.

**Design:**
- Optional instrumented masking functions that log execution stats to a Delta table
- `make generate --instrumented` generates wrapper functions that log to `<catalog>.governance.masking_audit`
- Audit table schema: `timestamp, function_name, column_name, caller_group, latency_ms, row_count`
- Dashboard template for monitoring (Databricks AI/BI dashboard)

**Implementation notes:**
- Wrapper pattern: original function + logging function that calls original and logs
- Performance impact: ~5% overhead for logging (async write to Delta)
- Retention: 30 days by default, configurable
- Privacy: audit table itself should NOT contain the actual data values, only metadata

## 5. Stronger Integration Test Assertions

**Status:** Planned | **Effort:** Medium

Current integration tests check file existence and basic content. Missing: masking enforcement verification via actual SQL queries.

**Design:**
- After `make apply`, execute test queries as different groups to verify masking works:
  - Query as `analyst` group → verify PII columns return masked values
  - Query as `compliance` group → verify full access
  - Query with row filter → verify restricted rows are hidden
- Add `_verify_governance_effective()` helper to integration test framework

**Implementation notes:**
- Requires creating test users or impersonating groups via SP
- SQL queries against governed tables with `SELECT` + assertion on result format
- Adds ~2-3 minutes per scenario but provides end-to-end confidence
