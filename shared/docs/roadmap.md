# Roadmap: Future Improvements

Planned features and improvements identified during the comprehensive project review. Each item includes a design sketch and estimated effort.

## Delivered since this roadmap was written

The native-classification governance model has since shipped and is the current recommended flow — see the **[Dev-to-Prod Walkthrough](../examples/dev_to_prod/README.md)**. The items below are *remaining* future work, distinct from what has already landed:

- **Native classification as the sensitivity source** — Unity Catalog `class.*` tags decide what's sensitive (no LLM guessing); fail-closed if unreadable.
- **Deterministic treatment derivation** — one `gr_treatment` per column, so exactly one mask resolves.
- **Blocking coverage check** — `make coverage-gate` fails the release until every classified column is protected.
- **Safe dev→prod promotion** — `make derive-assignments` re-derives prod facts from prod's own classification (no LLM re-generation), reusing the promoted rules.
- **Coverage-checked exposure** — Terraform refuses new or wider business `SELECT` + Genie `CAN_RUN` without a recent passing coverage result, and masks are created before grants. The old `business_access_enabled` switch is deprecated and ignored.
- **Consume-IdP groups by default** — GenieRails consumes IdP-synced groups (`manage_groups=false`) rather than inventing them.
- **Effective-access verification** — `make verify-access` proves masking/row filters by querying as per-tier principals (item 5 below).
- **Scheduled steady-state governance** and a **compliance evidence report**.

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

**Status:** Partially delivered | **Effort:** Medium

`make audit-rulebook` (flags tags with no covering rule) and `make audit-schema` (untagged sensitive columns) provide drift detection today; the full deployed-vs-config live diff described below is still planned.

Currently, structured validation only runs against the generated `.tfvars` files. If someone manually edits governance in the Databricks UI, Terraform will overwrite it on next `apply`.

**Design:**
- `make validate-live ENV=<env>` compares deployed Databricks state against config
- Checks: tag assignments exist, FGAC policies match, masking functions exist, Genie agent ACLs match
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
- Workspace layer (Genie agents, ACLs) IS per-workspace — needs a "workspace mesh" mode

**Proposed "workspace mesh" mode:**
```
envs/
  account/          # shared across all workspaces
  dev/
    data_access/    # shared governance for dev catalogs
    workspace_a/    # Genie agents for workspace A
    workspace_b/    # Genie agents for workspace B
  prod/
    data_access/    # shared governance for prod catalogs
    workspace_c/    # Genie agents for workspace C
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

**Status:** Implemented | **Effort:** Medium

Current integration tests check file existence and basic content. Missing: masking enforcement verification via actual SQL queries.

**Delivered:** `shared/verify_effective_access.py` + `make verify-access`. It
verifies masking and row filtering **by effect** — running the same query as a
dedicated per-tier test service principal and comparing the values each tier
gets back — rather than only checking that masks/tags exist. See
[`effective-access-verification.md`](effective-access-verification.md).

**Design (as built):**
- After `make apply`, `make verify-access ENV=<env>` provisions one service
  principal per access tier (each a member of that tier's group), runs `SELECT`
  as each, and asserts:
  - a lower-tier principal sees the **masked** value while a higher-tier
    principal sees the **raw** value for the same row, and
  - a row-filtered table returns **fewer rows** to a restricted principal.
- The value-comparison logic is pure and unit-tested with mocked query results
  (`tests/test_verify_effective_access.py`); the live workspace path is guarded
  behind `--live` / `GENIERAILS_LIVE_VERIFY=1`.

**Note:** Databricks has no general per-user query impersonation, so dedicated
per-tier test principals are the mechanism (documented in the guide). Wiring
`make verify-access` into the `run_integration_tests.py` scenarios as an
automatic post-apply step is a follow-up.
