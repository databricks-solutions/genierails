# Promoting Genie Spaces to Production with Databricks Asset Bundles

**Date:** 2026-08-11
**Audience:** Databricks customer promoting Genie spaces from a dev workspace to a prod workspace, run locally.

## Problem

A customer curates Genie spaces in a dev workspace and needs them in prod. The spaces
must point at prod Unity Catalog tables, not dev ones. Prod may or may not already
contain hand-made copies of these spaces. The customer runs the promotion from their
own laptop; CI comes later.

## Verified platform facts

These were confirmed against the `databricks/cli` source at tag `v1.11.0`, not from
documentation summaries:

- `genie_spaces` is a native DAB resource (`bundle/config/resources.go`).
- It first appeared in CLI **v1.3.0**. Versions 1.0.0-1.2.0 have no `genie_spaces` key
  at all, and a bundle using it fails to validate.
- It is implemented **only in the `direct` deploy engine**
  (`bundle/direct/dresources/genie_space.go`). `bundle/deploy/terraform/convert.go`
  contains zero Genie references. A bundle pinned to `engine: terraform` will not
  deploy Genie spaces. `direct` is the default (`bundle/config/engine/engine.go`:
  `const Default = EngineDirect`).
- Export command is `databricks bundle generate genie-space --existing-id <id> --key <key>`.
  The `--existing-genie-space-id` alias was removed in v1.4.0.
- Generated output is two files per space:
  - `resources/<key>.genie_space.yml` containing only `title`, `warehouse_id`,
    `file_path`, and optionally `description` and `parent_path`. Output-only fields
    (`space_id`, `etag`) are deliberately excluded (`bundle/generate/genie_space.go`).
  - `src/<key>.geniespace.json` containing the serialized space body.
- `serialized_space` is typed `any` in `GenieSpaceConfig` so it may be inlined as YAML,
  but `generate` always writes it to a separate JSON file and references it via
  `file_path`.
- On create, a missing `parent_path` folder is created and the request retried
  (`DoCreate` + `isMissingGenieParentPathError`).
- On update, `serialized_space` is **always** sent: "the bundle is the source of truth."
  Update is a full overwrite, not a merge.
- Etag is not used as an `If-Match` guard (the backend bumps it during schema
  migration). Drift is surfaced on read via `OverrideChangeDesc`, which marks the
  resource `Update` when the stored etag differs from remote.
- Deleting or recreating a Genie space is classified as destructive and requires
  approval, so unattended runs need `--auto-approve`.
- `DoDelete` calls `TrashSpace` — a destroy trashes the space rather than hard-deleting.

## Decisions

| Question | Decision |
|---|---|
| Topology | Separate dev and prod workspaces, two CLI profiles |
| Table references | Catalog differs between dev and prod; must be rewritten |
| Deliverable | Bundle scaffold plus a local wrapper script |
| Prod state | Mixed/unknown — detect existing spaces, never create duplicates |
| Scale | A handful of spaces (2-10), driven from a config file |
| CI/CD | Out of scope for now; keep the script CI-compatible |

## Architecture

```
genie-prod-promotion/
├── databricks.yml          bundle definition, dev + prod targets, variables
├── spaces.yml              customer-edited: space IDs + catalog mapping
├── promote_genie.sh        entrypoint
├── lib/
│   └── rewrite_catalog.py  serialized_space catalog rewriter (unit-testable)
├── resources/              generated <key>.genie_space.yml
├── src/                    generated <key>.geniespace.json
└── tests/                  fixture-based tests for the rewriter
```

Four idempotent phases:

1. **Preflight.** Assert CLI >= 1.3.0; assert engine is not `terraform`; assert both
   profiles authenticate; assert `spaces.yml` parses.
2. **Export.** Per space, `bundle generate genie-space --existing-id <dev_id> --key <key>`
   against the dev profile.
3. **Parameterize.** Rewrite dev catalog references inside `src/*.geniespace.json`.
   Replace the literal `warehouse_id` and `parent_path` in `resources/*.yml` with
   `${var.warehouse_id}` / `${var.parent_path}` so per-target values apply. Print a
   diff of every replacement.
4. **Reconcile and deploy.** List prod spaces by title. For each apparent match, print
   the `bundle deployment bind` command and skip it. Then `bundle validate` and
   `bundle plan -t prod`. Only with `--apply` does it run `bundle deploy -t prod`.

## Catalog rewrite

Table FQNs appear in `serialized_space` in multiple places: structured data-source
entries, example SQL, and free-text curated instructions. The internal schema of this
blob is not a public contract and Databricks changes it (the source comments reference
migrating `serialized_space` to newer schema versions).

**Chosen approach: recursive walk over every string value in the JSON**, replacing
`<dev_catalog>.<schema>` and bare `<dev_catalog>.` prefixes on word boundaries.

Rejected: targeting known keys such as `data_sources[].table_name`. It is precise but
silently misses catalog references embedded in example SQL and instructions — exactly
how a promoted space ends up querying dev data from prod. It also depends on guessing
key names that were not verifiable against a live payload.

Word-boundary matching prevents `dev_analytics` from matching inside
`dev_analytics_staging`. Every replacement is printed with its JSON path so the
customer can inspect before deploying.

## Idempotency and prod-side matching

DABs does not match resources by name. An unbound bundle resource whose title equals
an existing prod space creates a **second space with the same title**. Therefore:

- The script lists prod spaces and compares titles against the bundle's spaces.
- On a match it prints `databricks bundle deployment bind <key> <prod_space_id> --auto-approve`
  and excludes that space from the deploy until bound.
- After binding, deploys converge prod to the bundle. This overwrites any prod-side UI
  edits. The script warns about this explicitly, once per bound space.

## Error handling

- **Missing prod tables or grants.** The single most common silent failure: the space
  deploys cleanly and then answers nothing. Preflight runs a `SELECT 1 FROM <table> LIMIT 0`
  against each rewritten table using the prod warehouse and reports each failure without
  aborting the rest.
- **Missing prod warehouse.** Validated before deploy; fails with the warehouse ID.
- **Expired auth.** Detected in preflight with the exact `databricks auth login` command.
- **Unmapped catalogs.** After rewriting, any remaining reference to a known dev catalog
  is reported as an error, so a partial mapping cannot reach prod.
- **Secrets.** No tokens are written to `spaces.yml` or any generated file. Auth comes
  from CLI profiles, or from environment variables in CI.

## Testing

- The rewriter is a standalone module tested against synthetic `.geniespace.json`
  fixtures: catalog in a structured field, in example SQL, in instruction prose, a
  near-miss name that must not be rewritten, and an already-prod payload that must be
  unchanged (idempotence).
- Preflight version comparison is tested against 1.0.0 / 1.2.0 / 1.3.0 / 1.11.0.
- Export and deploy phases require live workspaces and cannot be executed locally;
  they are exercised only through preflight checks and `bundle validate`.

## Out of scope

- CI/CD workflow files (deferred at the customer's request).
- Creating prod tables, schemas, or grants.
- Promoting dashboards, jobs, or other resources alongside the spaces.
- Migrating Genie benchmarks/evaluation history, which the space API does not carry.
