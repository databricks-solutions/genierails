# Migrating a legacy env to deterministic governance

An env that GenieRails deployed in legacy mode (AI-drafted masks on
`gr_treatment`) moves to deterministic governance by committing the opt-in and
running the normal stages. An env that never opts in is untouched: its plans
show no change. Design: [deterministic-governance.md, section 8](design/deterministic-governance.md#8-migration-for-existing-deployments).

```bash
make migrate-plan ENV=prod        # 1. read-only dry run on the live env; proposes table_readers
                                  #    (before step 2 it exits 1 and prints these next steps)
# 2. dev: in envs/dev/env.auto.tfvars set governance_mode = "deterministic",
#    deployer_principal_id (dev's deployer SP application ID), access_tier_groups,
#    table_readers and each agent's acl_groups; then
make rebuild-governed-tables ENV=dev   # 2a. writes generated/governed_tables.json (commit it)
make generate ENV=dev                  # 2b. dev: the deterministic draft (no model is called)
make promote ENV=dev                   # 2b.i. refresh split files before `make check ENV=dev`
#    prod: in envs/prod/env.auto.tfvars set governance_mode = "deterministic"
#    and deployer_principal_id (prod's own deployer SP) yourself (and group_map
#    if prod's group names differ); promote-to never sets either. Then:
make rebuild-governed-tables ENV=prod  # 2c. prod's record (refused until prod sets the mode)
make promote-to ENV=prod               # 2d. prod's draft from dev's; add the reviewed ack.txt lines
make migrate-plan ENV=prod        # 3. repeat until "the cutover may proceed"; PR, review, merge
make rehearse ENV=dev             # 4. dev migrates here (and writes the receipt prod needs)
make release-plan ENV=prod        # 5. one manifest per phase: approve, then
make release ENV=prod             #    repeat 5 until "Release complete"
```

Steps 2a and 2c are required. A legacy env already has treatment tags deployed but no
`generated/governed_tables.json` (the deterministic record of governed tables),
so the first deterministic `generate`, and `migrate-plan`, `rehearse` and
`release-plan` in prod, refuse with:

```
ERROR: <path>/envs/dev/generated/governed_tables.json is missing but this env has recorded governance (treatment tags are deployed (data_access/terraform.tfstate); its generated or promoted config has treatment tags); rebuild it with: make rebuild-governed-tables ENV=dev
```

In prod, step 2d brings the rest of the opt-in from dev: `access_tier_groups`
always, and, where prod has none yet, `table_readers` and each agent's
`acl_groups` (group names through prod's `group_map`). Values prod already has
are kept, so review the promotion's diff and its printed differences.

`generate` owns `generated/`; during this legacy transition the existing split
files are still separate promotion outputs. Run `make promote ENV=dev` after
`make generate ENV=dev` so `make check ENV=dev` compares current split files.

`make rebuild-governed-tables ENV=<env>` writes the record from the deployed
treatment tags and the generated or promoted config. It is local and changes
nothing in Databricks. Run it once per env after opting in, then commit the
record with the rest of the opt-in.

## The dry run

`make migrate-plan ENV=<env>` reads the env's Terraform state (what GenieRails
deployed) and the committed config; it changes nothing in git or Databricks.
It reports:

- per column and per principal: what each sees today (the legacy mask) and after
  (the tier rule), with the legacy function's SQL next to the library's;
- every legacy policy's `TO`/`EXCEPT` principals next to the new partial and full policies;
- every `SELECT` and `CAN_RUN` GenieRails grants today that the config drops (a
  **revocation**), and the `USE_CATALOG`/`USE_SCHEMA` grants that go with them;
- weakenings, frozen legacy row filters, agents without `acl_groups`, masks
  GenieRails didn't create, and the generated `terraform state mv` lines.

It writes a proposed `table_readers` that reproduces every `SELECT` GenieRails
grants today to `<state dir>/<env>/migration/table_readers.proposed.hcl` (and
`report.json`). Run it before opting in for the proposal; after, for the full
comparison. Exit code 0 means the cutover may proceed. Before opting in it
always exits 1, after writing the proposal and printing the opt-in steps.

## Refusals

| Refused | To accept it |
|---|---|
| a weakening (a principal sees more than today) | `<env>:weaken:<column>:<principal>` in `envs/<env>/ack.txt` |
| a revoked `SELECT` | `<env>:revoke:<catalog.schema.table>:<principal>`, one line per principal, also when the config drops the table from `table_readers` altogether (never a `weaken` line) |
| a revoked `CAN_RUN` | `<env>:revoke:<agent key>:<group>` (the key the report prints) |
| a legacy row filter missing or changed | none: legacy row filters stay frozen until declared `row_filters` replace them |
| a mask GenieRails didn't create | none: remove it. If the inventory can't run (for example, no `generated/governed_tables.json`), migrate-plan names the reason instead: fix that and re-run |
| an agent without `acl_groups`, a legacy generated config, a legacy mask not on `gr_treatment` | fix the config |

The same checks run again when the cutover starts (rehearse, release-plan), so
nothing is moved or applied while one fails. After the cutover the
acknowledgements stay accounted for, so they never turn stale.

## The cutover

The staged policy controller ([deterministic-policies.md](deterministic-policies.md))
adopts the legacy deployment as the live generation:

1. **adopt**: under the env lock, each legacy treatment tag moves to its
   stable key with `terraform state mv` (never destroyed). Legacy sensitivity
   tags the config drops are forgotten with `terraform state rm`; they stay on
   the column. A marker written before the first move makes the env refuse
   legacy applies from then on, even if the adoption is interrupted.
2. **create**: the new functions and policies are added under new names; every
   column keeps its legacy value and mask. Verified live and with the legacy
   verify-access checks. The coverage check accepts the legacy values here
   (the live refresh recorded the deterministic ones the switch applies) only
   while the adopted migration record, the policy journal and the legacy
   snapshot agree; the switch is checked against the refreshed values. Like
   every staged phase, it renders into the ignored
   `generated/.runtime/data_access/`, so the committed `data_access/` files
   never change.
3. **switch**: each column is retagged in place, from its legacy value to its
   deterministic one. One tag per column, so no column is ever unmasked or
   doubly masked. Verified against the tier rule.
4. **grant**: `SELECT` from `table_readers` and `CAN_RUN` from `acl_groups`.

`release-plan` changes nothing: in prod the first manifest holds only the
approved state moves, which `release` runs before stopping; the next
`release-plan` plans **create** against the moved state. In dev, `rehearse`
adopts and runs every phase in one go.

The old-Terraform-address layout guard defers only for this untouched pending
legacy migration; the staged controller owns both the state moves and every
deterministic `gr_treatment` retag. `make plan` may describe the same pending
migration without a lock, but all mutation remains behind the env lock.

In prod each changing phase is one `release-plan` manifest and approval, with
release-plan's provenance (clean checkout on freshly fetched `origin/main`, or
the committed local HEAD when the repo has no remotes at all; a matching
passing dev receipt; not older than the last release unless
`prod:rollback:<commit>` is acknowledged). The legacy policies stay live until
a later successful release retires them; legacy functions are never dropped.
Once migrated, an env can't be applied in legacy mode again; undo a change by
rolling back within deterministic governance.
