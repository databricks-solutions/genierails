# Governance playbook (governance engineer)

You own how tables are protected and who can read them: treatments, tiers,
`table_readers`, row filters, `tag_treatments`, `group_map`, the governed set
and the acknowledgements. You own `envs/<env>/env.auto.tfvars`,
`envs/<env>/generated/`, `envs/<env>/ack.txt` and `shared/`
([who owns which file](../pipelines.md#who-owns-which-file)). Nothing in the flow
below calls a model; only the optional dev-only helpers at the end do.

## The whole flow

Every governance change takes the same path:

```bash
# dev: edit envs/dev/env.auto.tfvars, then
make generate ENV=dev          # re-derives envs/dev/generated/ from the tags and your config; review the diff
make check ENV=dev
git add envs/dev/ && git commit           # PR; after merge the dev pipeline rehearses it

# prod
make promote-to ENV=prod       # the promotion PR shows only mask and policy changes
make check ENV=prod
git add envs/prod/ && git commit          # PR; merge it
# on the deployment machine (it reuses dev's rehearsal):
make release ENV=prod          # at a terminal: per round, plans, shows the manifest, asks y/N, applies it
#   separate approver: per round make release-plan ENV=prod, approve, make release ENV=prod MANIFEST=<path>
```

On a PR branch, `make generate` and `make promote-to` don't offer to commit
(the offer appears only on `main`, at a terminal): commit yourself, as above.
`make check` is the PR check; `make rehearse` and `make release-plan` run it
again themselves.

A governance change applies to every table and agent that uses it, and changes
no agent's config, so it never waits for an agent that's mid-curation.

## Before you start

- Run `make` from `aws/` (or `azure/`).
- The champion template and newly scaffolded workspace envs set
  `governance_mode = "deterministic"`; each env also sets its own
  `deployer_principal_id` (the application ID of the SP that applies it) in its
  `env.auto.tfvars`. `promote-to` never sets either in prod. An env deployed
  before in legacy mode migrates first: [legacy mode](../legacy-mode.md).
- Settings and their precedence: [dev-to-prod reference](../../examples/dev_to_prod/REFERENCE.md#deterministic-governance-settings);
  the mask library: [REFERENCE.md](../../REFERENCE.md).

<a id="govern-tables-only"></a>
## Govern tables only

Use this when GenieRails manages table protection by itself, for example:

- **A central data governance team** defines the rules for the organisation's
  tables (which classified columns are masked and how, row filters, who reads
  each table), reviews them in a PR and promotes them from dev to prod. Agent
  teams build on the protected tables without owning those rules.
- **A platform team securing catalogs ahead of Genie**, so the tables are
  already protected when the first agent arrives. Adding agents later doesn't
  disturb the governance that's in place.
- **Tables read by more than Genie**: Unity Catalog enforces the masks and row
  filters on every query, so SQL, notebook and BI users get the same
  protection.

```bash
make setup ENV=dev REPO=<your-repo-url>    # REPO= optional, as in the walkthrough; offers to commit (y)
#   envs/dev/env.auto.tfvars: governance_mode, deployer_principal_id, access_tier_groups,
#   genie_spaces = [], uc_tables, and table_readers if GenieRails should grant SELECT
#   Catalog Explorer: Data classification on, review detections, auto-tagging on; wait for class.* tags
make generate ENV=dev                      # offers to commit + push envs/dev/ (y)
make rehearse ENV=dev                      # checks; first deploy: account plan y/N; applies dev, proves the masks
make setup ENV=prod                        # set governance_mode, deployer_principal_id, catalog_map
make promote-to ENV=prod                   # offers to commit + push envs/prod/ (y)
#   Catalog Explorer: the same classification review on the prod catalog
make release ENV=prod                      # each round: plans (reusing dev's rehearsal), shows the manifest, asks y/N, applies it
make maintain ENV=prod                     # on a schedule
```

1. **Configure dev.** In `envs/dev/env.auto.tfvars`:
   ```hcl
   governance_mode       = "deterministic"
   deployer_principal_id = "<dev deployer SP application ID>"
   access_tier_groups    = ["payments_ops", "regional_analysts", "viewers"]   # raw, partial, fully masked
   genie_spaces          = []
   uc_tables             = ["dev_finance.payments.customers", "dev_finance.payments.transactions"]
   ```
   `uc_tables` governs and never grants. Without `table_readers`, masks apply
   to whoever already has access, after the reader check.
2. **Classify** dev in Catalog Explorer, as in the [walkthrough](../../examples/dev_to_prod/README.md#dev-classify).
3. **Generate and rehearse.** Answer `y` to generate's commit offer (or commit
   `envs/dev/` in a PR and merge it); rehearse runs `make check` first.
   **Pass looks like:** `ALL EFFECTIVE`.
4. **Prod.** Set `governance_mode`, prod's `deployer_principal_id` and
   `catalog_map = { "dev_finance" = "prod_finance" }`, promote (answer `y`),
   classify prod; then `make release ENV=prod` (its `release-plan` reuses the
   source env's rehearsal, or names the command to run). **Pass looks
   like:** `ALL EFFECTIVE` and
   `=== Release complete (prod) ===`.

GenieRails creates nothing in the workspace layer: no group assignment,
warehouse or agent. Adding agents later is the [agents playbook](agents.md).

## Change a mask

```hcl
treatment_versions = { ssn = { partial = "last4" } }        # another shipped version for a treatment
column_overrides   = {
  "dev_finance.payments.customers.postcode" = { partial = "prefix_3" }   # one column
}
```

- A column override never changes the full version and can't make a never-raw
  treatment raw. `partial = "raw"` anywhere is a weakening and needs an
  [acknowledgement](#acknowledge-an-exception).
- A changed mask gets new function and policy names, so a live one is never
  replaced. Rehearse runs the staged phases (create, verify, switch, verify,
  then retire later); in prod each changing phase is one `release-plan`
  manifest and approval ([staged policy changes](../deterministic-policies.md#staged-policy-changes-rollout-step-6)).

<a id="map-a-new-class"></a>
## Map a new class

A class the library doesn't map gets the fail-safe never-raw redaction, and new
access is blocked until it's mapped. Map it in dev, never in `shared/`:

```bash
make scaffold-treatments ENV=dev TREATMENT=identifier   # any mask-library treatment, e.g. redact
```

It writes `"class.<name>" = "<treatment>"` to `tag_treatments` in
`envs/dev/env.auto.tfvars` for every unmapped class it finds. Run with
`ENV=prod` when prod's release stopped on the class: it writes the env prod is
promoted from. Then generate, check, PR, rehearse, `promote-to`, merge to
`origin/main`, `release-plan` (it reuses the source env's rehearsal, or names
the command to run), `release`.
Refused: a class the library already maps (use `column_overrides`), a value that
isn't a library treatment, and keys other than `class.*`
([tag_treatments](../../REFERENCE.md#customer-classes-tag_treatments)).

## Tiers and group names

- **Tiers** are `access_tier_groups`, most to least privileged: first raw, last
  fully masked, any between partial. They are defined in dev and promoted.
  Moving a group between tiers is a staged change: run it as a maintenance
  window, one manifest per phase in prod.
- **`raw_exempt_principals`** (env-owned, e.g. an ETL SP) see raw, except for
  never-raw treatments. Adding one is a weakening.
- **`group_map`**, when prod's group names differ, goes in
  `envs/prod/env.auto.tfvars`:
  ```hcl
  group_map = { dev_analysts = "prod_analysts", dev_viewers = "prod_viewers" }
  ```
  Every resulting group must exist in prod (checked by `release-plan`). Adding,
  removing or changing a mapping needs `prod:weaken:group_map:<dev>=<target>`
  in `envs/prod/ack.txt` ([group_map](../access.md#group-names-in-prod-group_map)).

## Who can read a table

`table_readers` is the only source of `SELECT`:

```hcl
table_readers = {
  "prod_finance.payments.customers" = ["payments_ops", "regional_analysts"]
  "prod_finance.payments.*"         = ["payments_ops"]   # every governed table in the schema
}
```

- A table's readers are the union of every matching entry. Every reader must
  be a tier group or in `raw_exempt_principals`.
- Env-owned: the first `promote-to` seeds prod from dev; later ones keep
  prod's and print the differences. Change prod's directly in
  `envs/prod/env.auto.tfvars`, in a PR.
- Adding a reader is widening, checked by the coverage check and
  refuse-weakening. Removing an entry revokes the `SELECT` it granted; masks
  stay.
- When a new agent is imported, `make generate` proposes entries for its
  uncovered tables. Review them in the agent's PR: you approve every change
  to the governed set ([access.md](../access.md#table_readers)).

## Row filters

Declared `row_filters` are validated and promoted (groups through
`group_map`), but not yet compiled into a policy or function (design
rollout step 10):

```hcl
row_filters = [
  { table = "prod_finance.payments.transactions", column = "region_code",
    values_by_group = { regional_analysts = ["APAC"] } }
]
```

Until step 10 ships they live only as declared config in
`env.auto.tfvars` (and in the promote-to rewrite). `make generate` does
not emit a row-filter policy in `generated/abac.auto.tfvars` or a
function in `masking_functions.sql` for them. Removing or widening one
is still a weakening.

## Stop governing a table

```bash
make ungovern ENV=dev TABLE=dev_finance.payments.archive    # asks to confirm; YES=1 skips
```

It's the only way out of the governed set, and it refuses while a configured
agent still uses the table: remove the table from every agent and from
`uc_tables` first. In dev it removes the table from
`generated/governed_tables.json` and applies governance, so run it with dev's
deployer credentials. In prod (`ENV=prod`) it only edits the record: commit it,
and the next release applies the removal. Removing a table from an agent never
drops its masks.

<a id="acknowledge-an-exception"></a>
## Acknowledge an exception

When `release-plan` (or rehearse) stops on a weakening, a reader or an
unclassified column, it names each entry. Add the reviewed lines to that env's
`ack.txt` in a PR. Each line repeats the env, so a file copied to another env
fails closed:

```text
prod:weaken:prod_finance.payments.customers.email:etl-sp
prod:unclassified:prod_finance.payments.customers.notes
prod:reader:prod_finance.payments.customers:bi-sp
prod:reader:prod_finance.payments.customers:prod_finance.marts.legacy_view
prod:rollback:<commit>
prod:delete-agent:<agent key>
```

- `ack.txt` must be governance-owned in `CODEOWNERS`.
- An entry that matches nothing after all checks is refused, so stale lines
  can't pile up.

## Keep it safe between changes

```bash
make audit-schema ENV=dev      # untagged sensitive-looking columns, tags on deleted columns
make audit-rulebook ENV=dev    # applied tags no policy or mask covers
```

In prod, `make maintain ENV=prod` runs on a schedule from the
[deployment machine](../deployment-machine.md#keep-prod-protected). It masks
newly tagged columns and reports new readers and unmapped classes.

## Upgrade GenieRails

An upgrade is a governance change. Merge the new GenieRails release into your
repo in a PR (`shared/` is governance-owned), run `make generate ENV=dev` and
`make check ENV=dev`. If `make check ENV=prod` fails because prod's promoted
output changed, run `make promote-to ENV=prod` in the same PR. Then merge,
rehearse the source env (`promote_from`, dev by default) on the merged commit,
`release-plan` and `release`. Read the [CHANGELOG](../../../aws/CHANGELOG.md) first.

## If it stops

| Message | What to do |
|---|---|
| coverage check failed, or an unmapped class | [map the class](#map-a-new-class) |
| reader check sees only the views the deployer can see, in catalogs bound to the workspace it runs in | run prod's `release-plan` as the metastore admin from a workspace bound to every catalog that could hold views over governed tables; a view it can't see isn't reported ([design](../design/deterministic-governance.md#6-safety-rules)) |
| reader check: a current reader isn't in a tier | add it to a tier or `raw_exempt_principals`, or [acknowledge](#acknowledge-an-exception) it |
| reader check: cannot list the views that read governed tables | the deployer needs `SELECT` on `system.information_schema` and must be able to read every view, materialized view and metric view it can see, and the functions they call (metastore admin, or owner/`MANAGE`/`BROWSE` on the view's catalog) |
| reader check: view `<view>` may read `<table>` (no recorded `view_dependencies`, or it calls a function with no recorded `routine_dependencies`) | recreate it (`CREATE OR REPLACE VIEW` / `FUNCTION`) or drop it; once reviewed, [acknowledge](#acknowledge-an-exception) it as `<env>:reader:<table>:<view>` |
| refuse-weakening | revert the change, or [acknowledge](#acknowledge-an-exception) it |
| a mask GenieRails didn't create is on a governed table | remove it; GenieRails supports only governance it generates |
| prod has a column classified in dev but no `class.*` tag | finish prod's classification review, wait, re-run |
| a group in `acl_groups` can't read an agent's table | add it to `table_readers`, or the agent's owner drops it |
| a principal (the deployer or a tier group) was deleted and recreated while a policy change is unfinished: policies show `unknown`, and `make policy-abandon` refuses | update the config for the recreated principals ([legacy-migration.md](../legacy-migration.md)), then `make policy-rebind ENV=dev` and `make policy-abandon ENV=dev`, then rehearse ([deterministic-policies.md](../deterministic-policies.md)) |
| `make check` says generated files differ | run `make generate ENV=dev` (or `make promote-to ENV=prod`) on the latest `main` and commit; never hand-merge generated files. In dev, after a deployer or group change, also run `make promote ENV=dev` before `make check ENV=dev` so the split files are current ([legacy-migration.md](../legacy-migration.md)) |

> **Optional AI helpers (dev only).** They write files under `envs/dev/drafts/`
> for review, are never run by `generate`, `rehearse`, `release` or `maintain`,
> and refuse in CI and in prod:
> - `make suggest-treatment ENV=dev CLASS=<class>` proposes a library treatment for an unmapped class.
> - `make draft-custom-mask ENV=dev TREATMENT=custom_<name> TYPE=<sql-type>` drafts
>   a custom mask and its exact-output test fixture under `envs/dev/drafts/custom_masks/`.
> - `make test-custom-mask ENV=dev TREATMENT=custom_<name>` runs the mask library's
>   exact-output tests on that draft. A pass does not adopt it: `tag_treatments`
>   can't use a custom mask until it is adopted.
> - `make adopt-custom-mask ENV=dev TREATMENT=custom_<name>` (no model) copies a
>   passed draft into `envs/dev/custom_masks/`, which you own and review like
>   `env.auto.tfvars`, and prints the `tag_treatments` line to add
>   ([reference](../../REFERENCE.md#adopted-custom-masks)).
