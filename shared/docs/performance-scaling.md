# Performance & Scaling

Guidelines for deploying GenieRails governance at scale.

## Platform Limits

| Resource | Limit | Scope | Notes |
|----------|-------|-------|-------|
| FGAC policies | 100 | Per catalog | Column masks + row filters. Option-B treatment derivation keeps you well under it (one policy per treatment per catalog). |
| Tag policies | Unlimited | Per metastore | Each policy has a value limit |
| Tag policy values | 1000 | Per tag policy key | e.g., `pii_level` can have up to 1000 distinct values |
| Groups | 10,000 | Per account | Shared across all workspaces |
| Tag assignments | Unlimited | Per catalog | One tag key per column per assignment |
| Genie agents | No hard limit | Per workspace | Each space consumes warehouse resources at query time |
| Tables per Genie agent | ~20 recommended | Per space | LLM context window limits generation quality beyond ~20 tables |

## Generation & Governance Performance

> **What actually scales.** In the dev-to-prod walkthrough, *sensitivity* comes from native classification and *enforcement* is derived deterministically — neither depends on LLM prompt size. The LLM only drafts the **rulebook and Genie content** in dev `generate`. Two cost models:
>
> - **Governance scaling (the important one):** classification **scan latency** (async, eventual-consistency before `derive-assignments`); deterministic **derivation** (scales with classified columns × catalog/treatment combinations, not tokens); **coverage-check** cost (classified columns × relevant mask policies/functions); and **exposure rollout** (a passing `make release` grants `SELECT` + Genie `CAN_RUN`, so stage releases per env/BU). Prod uses `derive-assignments` — **no LLM**.
> - **Generation scaling (dev only):** the LLM prompt-size table below governs *dev rule/Genie-content drafting quality*, not production enforcement.
>
> No defensible numeric benchmarks exist for scan latency or derive/check throughput — measure in your own environment rather than assuming fixed timings.

### LLM Prompt Size vs. Quality (dev generation only)

| Tables | Columns | Approx. Prompt Size | Generation Quality | Time |
|--------|---------|--------------------|--------------------|------|
| 1-4 | <40 | ~8K tokens | Excellent | 5-15s |
| 5-8 | 40-80 | ~15K tokens | Good | 10-30s |
| 9-15 | 80-150 | ~25K tokens | Fair (may need retries) | 15-60s |
| 16-20 | 150-200 | ~35K tokens | Poor (frequent retries) | 30-120s |
| 20+ | 200+ | >40K tokens | Not recommended | Unreliable |

**Recommendation:** Keep each Genie agent to 4-8 tables for reliable generation. Use `SPACE="Space Name"` for per-space generation to control prompt size.

### Country + Industry Overlay Impact

Each overlay adds ~2-4K tokens to the prompt. Combining overlays increases prompt size:

| Configuration | Additional Tokens | Impact |
|---|---|---|
| No overlays | 0 | Baseline |
| 1 country (e.g., `ANZ`) | ~2K | Minimal |
| 1 industry (e.g., `financial_services`) | ~3K | Minimal |
| Country + industry | ~5K | Low |
| 3 countries (`ANZ,IN,SEA`) | ~8K | Moderate — reduces headroom for table DDL |
| 3 countries + industry | ~11K | High — keep tables to 4-6 |

## SQL Warehouse Sizing

### For Governance Deployment (`make apply`)

Masking functions are deployed via SQL statements. A `2X-Small` warehouse is sufficient for deployment. Configure via:

```hcl
# In your Terraform module call or tfvars
warehouse_cluster_size = "2X-Small"
```

### For Governed Queries (Runtime)

Masking functions execute per-row on every query. Size your warehouse based on:

| Data Volume | Concurrent Users | Recommended Size | Serverless |
|------------|-----------------|------------------|------------|
| <1M rows | <10 | Small | Yes |
| 1-100M rows | 10-50 | Medium | Yes |
| 100M-1B rows | 50-200 | Large | Yes |
| >1B rows | >200 | X-Large+ | Yes |

**Row filters are more expensive than column masks.** A row filter with a complex condition (e.g., joining to a group membership table) adds latency to every query. Keep row filter conditions simple.

## Masking Function Performance

### Best Practices

- **Use SQL built-ins** (SUBSTRING, CONCAT, REPLACE) — they're vectorized and fast
- **Avoid Python UDFs** for masking — ~10-100x slower than SQL UDFs
- **DETERMINISTIC functions** enable query caching — mark functions as deterministic when possible
- **NULL handling** should be the first check — short-circuit before expensive operations

### Function Execution Overhead

| Function Type | Overhead per Row | Notes |
|---|---|---|
| Simple string masking (SUBSTRING) | <1us | Negligible |
| SHA-256 hash | ~5us | Acceptable for most volumes |
| Regex replacement | ~10us | Avoid on very high-volume columns |
| Python UDF | ~100-500us | Avoid for production masking |
| Row filter (simple condition) | ~1us | Acceptable |
| Row filter (subquery/join) | ~50-500us | Use sparingly |

See [Custom Masking Functions](custom-masking-functions.md) for authoring guidance.

## Multi-Space Scaling

### Recommended Architecture

For organizations with many teams:

```
Account Layer (shared)
+-- Groups: 5-20 organization-wide
+-- Tag Policies: 3-5 (pii_level, pci_level, compliance_scope, etc.)
|
+-- BU 1: Finance
|   +-- Genie agent: Finance Analytics (4 tables)
|   +-- Genie agent: Risk Dashboard (3 tables)
|   +-- ABAC: 8 FGAC policies across 2 catalogs
|
+-- BU 2: Clinical
|   +-- Genie agent: Clinical Analytics (5 tables)
|   +-- ABAC: 6 FGAC policies in 1 catalog
|
+-- BU 3: Marketing
    +-- Genie agent: Campaign Analytics (3 tables)
    +-- ABAC: 4 FGAC policies in 1 catalog
```

### Self-Service Model

Use `MODE=governance` for the central team and `MODE=genie` for BU teams. See [Self-Service Genie](self-service-genie.md) for details.

## CI/CD Performance

### Test Execution Times (Typical)

| Scenario | AWS | Azure | Notes |
|---|---|---|---|
| Unit tests | ~10s | ~10s | No infrastructure required |
| Single legacy demo (e.g., aus-bank-demo) | ~15 min | ~20 min | 1 workspace + generate + apply + promote (legacy bank demo) |
| Full parallel CI (18 scenarios) | ~4 hours | ~5 hours | All scenarios concurrently; includes the legacy bank/overlay demos |
| Country overlay (6 phases) | ~3 hours | ~3-4 hours | Longest single scenario |

> A live **dev-to-prod** run has phases these older scenarios don't isolate — chiefly the **async classification scan wait** (minutes to ~24h; not compressible) before `derive-assignments`, plus the coverage check and staged exposure. Budget the scan wait as wall-clock, separate from generate/apply time.

### Optimizing CI Time

1. Use `SCENARIOS=quickstart,promote` for fast validation of core flows
2. Reserve full CI (`make test-ci-parallel`) for pre-merge validation
3. Pin `WAREHOUSE_ID` to avoid cold-start delays
4. Use `--keep-envs` during development to avoid re-provisioning
