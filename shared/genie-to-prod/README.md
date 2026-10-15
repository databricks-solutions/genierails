# Promote Genie Spaces to Production

Move Genie spaces from a dev Databricks workspace to a prod workspace using
Databricks Asset Bundles, run from your own machine.

Point it at your two workspaces, pick the spaces you want, and it exports each
space, rewrites the table names so prod reads prod data, shows you exactly what
would change, and deploys only after you say yes.

- Nothing is written to prod until you explicitly confirm.
- Spaces you do not pick are never touched.
- You never have to look up an ID or edit a file by hand.

---

## Step 1 — Install the Databricks CLI

Version **1.3.0 or newer** is required: Genie spaces are not a bundle resource in
earlier versions, and the tool will refuse to run on them.

```bash
brew tap databricks/tap && brew install databricks   # first time
brew upgrade databricks                              # if already installed
databricks --version
```

Not on macOS? See
[the CLI install guide](https://docs.databricks.com/dev-tools/cli/install.html).

You also need **Python 3** (`python3 --version`). No pip packages are needed.

## Step 2 — Log in to both workspaces

Each workspace becomes a named CLI profile. The names are yours to choose; `dev`
and `prod` are the defaults the tool looks for.

```bash
databricks auth login --host https://your-dev-workspace.cloud.databricks.com  --profile dev
databricks auth login --host https://your-prod-workspace.cloud.databricks.com --profile prod
```

Each command opens a browser once. Credentials are handled entirely by the
Databricks CLI — this tool only ever passes it a profile name, and never reads or
stores a token.

Check both worked:

```bash
databricks current-user me -p dev
databricks current-user me -p prod
```

## Step 3 — Get the prod side ready

The tool rewrites table *names*; it does not create tables or grant access. Before
you promote, make sure:

- the tables your spaces reference **exist in prod**, under whatever catalog and
  schema you are mapping to
- whoever deploys has **`SELECT`** on those tables and **`CAN USE`** on the prod
  SQL warehouse

Skip this and the space still deploys cleanly — it just cannot answer any
questions. The tool checks each table and warns you, so you will know.

## Step 4 — Run it

```bash
cd genie-prod-promotion
./promote
```

That is the whole thing. The wizard walks you through it:

| Step | What it asks |
|---|---|
| 1 | Nothing — checks your CLI version |
| 2 | Which profile is dev, which is prod (picked from a list) |
| 3 | Which spaces to promote (your dev spaces, filterable) |
| 4 | How dev tables map to prod, per space (catalogs picked from a list) |
| 5 | Prod warehouse, folder, owner (warehouse picked from a list) |
| 6 | Review, then dry run, then deploy |

**In any list:** type a number to pick, text to filter, `n`/`p` to page,
`-` to clear a filter, and `!` to type a name by hand. In the space list, numbers
toggle on and off and **Enter** confirms.

The dry run shows you the plan and a field-level diff of each space. Nothing has
touched prod at that point. Only when you answer yes to *"Deploy to prod now?"*
does anything change.

## Step 5 — Check it worked

Open each promoted space in the prod workspace and ask it a question. If it
answers with prod data, you are done.

Then commit `resources/` and `src/` to your own git repo — those exported
definitions are what make the promotion reproducible.

---

## Running it again

Later runs reuse the saved setup. The workspaces, warehouse, folder and table
mappings come straight from the files last time wrote, and you go directly to
picking spaces:

```
[2/6] Using your saved setup
  Source      https://your-dev-workspace...
  Target      https://your-prod-workspace...
  Warehouse   655d03e9f2d3e99c
  Mappings    dev_analytics → prod_analytics

  ? Use this setup? [Y/n]:
```

Press Enter to keep it. Answer `n` to change **just the workspaces**, **just the
mappings**, or **everything**.

Promoting a change to one space is therefore: `./promote`, Enter, pick the space,
Enter, review, deploy.

To skip the reuse screen entirely and redo the full setup:

```bash
./promote --reconfigure
```

## Running the tests

These need no workspace and no credentials, so they are a safe way to see the
logic work before you point anything at prod:

```bash
python3 tests/test_rewrite_catalog.py     # table name rewriting
python3 tests/test_config_and_params.py   # config parsing
python3 tests/test_binding_state.py       # prod link detection
python3 tests/test_space_diff.py          # local vs prod diff
tests/test_scoping.sh                     # unselected spaces stay untouched
```

## Wiring this into CI/CD

Ready-to-copy pipelines are in `ci/`:

| File | Copy to |
|---|---|
| `ci/github-actions.yml` | `.github/workflows/promote-genie.yml` |
| `ci/azure-pipelines.yml` | `azure-pipelines.yml` |

### Split the work the way the tool expects

Do **not** have CI export from dev. A pipeline that re-exports at merge time
deploys whatever dev happens to look like then, not what was reviewed. Instead:

1. **A person** curates the space in the dev UI and syncs it into the repo:
   ```bash
   databricks bundle generate genie-space --resource sales_genie --force -t dev -p dev
   git add resources/ src/ && git commit && git push
   ```
   (`--watch` instead of `--force` keeps syncing while you edit in the UI.)
2. **The pull request** shows the diff of the space definition, and CI runs
   `bundle validate` and `bundle plan` so reviewers see what prod would do.
3. **On merge**, CI runs `bundle deploy -t prod`.

This means `resources/` and `src/` must be committed in your repo. The template
gitignores them because it ships with a sample config; delete those two lines from
`.gitignore` in your own repo.

### Authenticate with a service principal

A CI runner has no `~/.databrickscfg`, so profile-based auth cannot work. Create a
service principal, then set three variables:

```
DATABRICKS_HOST           https://your-prod-workspace...
DATABRICKS_CLIENT_ID      service principal application ID
DATABRICKS_CLIENT_SECRET  OAuth secret
```

GitHub: repository secrets. Azure DevOps: a variable group, with the secret marked
as secret. On Azure the client ID/secret may be a Microsoft Entra ID service
principal.

The service principal needs **`CAN MANAGE`** on the spaces, **`CAN USE`** on the
prod SQL warehouse, and **`SELECT`** on every table the spaces reference. Grant
those before the first run, or the deploy succeeds and the spaces answer nothing.

To run `promote_genie.sh` itself in CI, set both profiles to empty so it drops the
`-p` flag and lets the CLI use those variables:

```bash
DEV_PROFILE='' PROD_PROFILE='' ./promote_genie.sh --apply --yes
```

`--yes` is required for unattended runs: without it the script refuses to
overwrite prod-only changes in a non-interactive shell rather than guessing.

### Two things to get right

**Pin the CLI version.** Genie spaces need 1.3.0+, and pinning means a CLI release
cannot change deployment behaviour without you choosing it. Both sample pipelines
pin 1.11.0.

**Do not set `engine: terraform`.** The Terraform engine has no Genie space
implementation, so the spaces silently will not deploy. The default `direct`
engine is what you want.

### Approval gates

Both samples plan on pull requests and deploy on merge. For an explicit human
approval before prod, use a GitHub **environment** with a required reviewer, or an
Azure DevOps **environment** with approvals and checks — both samples already
declare the environment, so you only add the reviewer.

Remember that a deploy **overwrites** each space with the committed definition.
Anything someone edited in the prod UI is replaced, which is the point of managing
them as code, but worth stating in your runbook.

## If you prefer the non-interactive path

`./promote` writes two plain-text files, `databricks.yml` and `spaces.yml`. Once
they exist you can skip the wizard:

```bash
./promote_genie.sh                 # dry run: writes nothing to prod
./promote_genie.sh --apply         # deploy to prod
```

Useful options: `--dev-profile NAME`, `--prod-profile NAME`, `--spaces FILE`,
`--skip-table-check`.

## What happens under the hood

1. **Preflight** — CLI version, deployment engine, both profiles authenticate.
2. **Export** — `databricks bundle generate genie-space` pulls each dev space
   into `resources/<key>.genie_space.yml` plus `src/<key>.geniespace.json`.
3. **Rewrite** — dev table references are replaced with prod ones everywhere
   they appear: the structured table list, example SQL, and curated
   instructions. Every change is printed so you can check it. See
   [Mapping table names](#mapping-table-names).
4. **Parameterize** — `warehouse_id` and `parent_path` become target variables
   so prod uses prod values.
5. **Check tables** — each rewritten table gets a `SELECT 1 ... LIMIT 0` against
   the prod warehouse. Failures are reported but do not stop the run.
6. **Bind** — spaces that already exist in prod are adopted (see below).
7. **Plan, then deploy** — `bundle plan` always; `bundle deploy` only with
   `--apply`. Both are scoped to the spaces you picked.

Every mapping table below shows `spaces.yml` syntax, but you never have to write
it: the wizard produces the file for you. Reading it is useful when reviewing a
promotion or wiring this into CI.

## Mapping table names

Prod rarely mirrors dev exactly. A mapping key works at whichever level you need:

| What differs | Mapping in `spaces.yml` | Effect on `dev_analytics.sales.orders` |
|---|---|---|
| Catalog | `dev_analytics: prod_analytics` | → `prod_analytics.sales.orders` |
| Catalog and schema | `dev_analytics.sales: prod_analytics.retail` | → `prod_analytics.retail.orders` |
| Table name too | `dev_analytics.sales.orders: prod_analytics.retail.txns` | → `prod_analytics.retail.txns` |

The wizard asks which of the three applies, then lets you **pick each part from
your workspace** rather than typing it. Catalogs, schemas, and tables are listed
live from Unity Catalog on both sides.

Long lists are searchable: type any text to filter, `n`/`p` to page, `-` to clear
the filter, a number to select, or `!` to type a name by hand if it isn't listed.

It also reads the tables your chosen spaces actually reference and marks those
`← used by your spaces`, sorted first — so the catalogs you care about are at the
top instead of buried among sandboxes. Referenced tables that match no mapping are
listed as a warning before anything is written.

**Levels combine.** More specific keys are applied first, so this is valid:

```yaml
catalog_map:
  dev_analytics: prod_analytics                                  # everything else
  dev_analytics.sales.orders: prod_analytics.retail.txns         # this one table
```

`orders` becomes `retail.txns`, while every other `dev_analytics` table keeps its
schema and name under `prod_analytics`. Without the specific-first ordering the
catalog rule would rewrite the prefix and the table rule could never match.

Matching is on identifier boundaries, so `dev_analytics` does not match inside
`dev_analytics_staging`, and a table rule for `orders` does not touch
`orders_archive`.

Anything still referencing a mapped dev catalog after rewriting is reported with
its JSON path and blocks the deploy — a half-finished mapping cannot reach prod.
Note the converse: a dev catalog you never mentioned in `catalog_map` is not
scanned for, which is what the prod table-readability check in step 5 is there to
catch.

## Spaces you do not select are never touched

Pick one space and only that space is created, updated, or deleted. Nothing else
is sent to the API at all.

`spaces.yml` remembers every space you have ever configured, so their prod links
survive, but each run acts only on what you picked. Two things make that safe:

- every `bundle plan` and `bundle deploy` is scoped with
  `--select genie_spaces.<key>` for exactly the picked spaces, so an unselected
  space never appears in the plan
- spaces the bundle tracks but the run excludes are still written to disk, taken
  from **prod's own definition**, because `--select` cannot load a bundle whose
  config references a missing resource

Getting this wrong is easy and destructive, so it is covered by
`tests/test_scoping.sh`. Two earlier versions failed it: one emitted
`delete genie_spaces.<key>` for an excluded space, which would have trashed a
live prod space, and one showed a spurious `update` because the bundle stores an
etag per space that a regenerated config cannot match.

If a delete ever does reach the plan, the script aborts rather than applying it.

## Seeing what will change before you deploy

Two layers, both shown on every dry run.

**`bundle plan`** — the Terraform-style resource summary:

```
  + create genie_spaces.finance_genie
  ~ update genie_spaces.sales_genie
```

**A field-level diff** of each space that already exists in prod. `bundle plan`
tells you a space will be updated but not what differs inside it, so this fetches
prod's live definition and compares:

```
  + table prod_cat.retail.customers
  - table prod_cat.retail.legacy (present in prod, not in yours)
  - instructions: Someone added this in the prod UI  (in prod only, will be lost)
  ~ version: 2 -> 3
```

`+` is what your definition adds. `-` is what exists **only in prod** and will be
destroyed by the deploy.

### Changes made in prod without the bundle

Yes, those are caught. Two independent mechanisms:

- The bundle stores each space's **etag**. On the next plan it compares the stored
  etag against prod's and marks the space as changing when they differ — so an
  edit made in the prod UI shows up rather than being silently clobbered.
- The field-level diff above names the specific tables and instructions that
  exist only in prod.

When prod-only content is found, `--apply` **stops and asks** before overwriting:

```
  warning these spaces have content that exists only in prod:
    sales_genie
  Overwrite the prod-only content listed above? [y/N]:
```

Pass `--yes` to skip that confirmation (and note it is required for unattended
runs, where the script refuses to guess).

You can also run the diff on its own:

```bash
python3 lib/space_diff.py src/sales_genie.geniespace.json <prod_space_id> --profile prod
```

Exit code 3 means differences were found, 0 means prod already matches.

## New spaces vs. spaces that already exist in prod

You do not have to have started with this tool. Whatever state you are in, the
wizard works it out for you.

For each space you pick, it asks the bundle what it already tracks in prod, using
`bundle summary --force-pull`. That reads deployment state from the **workspace**,
not from local files, so a promotion done months ago — or from a colleague's
laptop — is still recognised. Three cases follow:

**Already linked.** The bundle knows this space's prod ID. It is updated in
place. Nothing to decide, and no bind is needed.

**Not linked yet.** The wizard says so plainly, then lists the Genie spaces that
actually exist in prod and asks whether you want to create a new space or update
one of those. A prod space with the same title as your dev space is flagged
`← same title as dev`, since that is usually the one you mean. Spaces already
claimed by another resource are left out of the list — two bundle resources
pointing at one space would overwrite each other on every deploy.

**Nothing in prod.** Created new, no questions asked.

Picking an existing space records it as `prod_id` in `spaces.yml`, and the script
runs `databricks bundle deployment bind` for you before deploying. Leaving it out
means a fresh space, whose new ID the deploy records in bundle state by itself.

This matters because DABs does **not** match spaces by name. A bundle resource
with no recorded prod ID creates a *second* space with the same title rather than
updating the existing one.

> **Once a space is bound, every deploy overwrites it with dev's content.**
> Instructions, example SQL, and curated questions edited in the prod UI are
> replaced. The bundle is the source of truth. Make your edits in dev and
> promote them.

The bundle also notices when someone changed a bound space in prod outside the
bundle, and shows it in `bundle plan` before you overwrite.

## Making a change later

Edit the space in the dev UI, then pull the change into the bundle and re-promote:

```bash
databricks bundle generate genie-space --resource sales_genie --force -t dev -p dev
./promote_genie.sh --apply
```

`--watch` instead of `--force` keeps syncing while you edit in the UI.

Commit `resources/` and `src/` to git so each promotion is reproducible.

## Files

| Path | What it is |
|---|---|
| `promote` | Interactive wizard. Start here. |
| `promote_genie.sh` | Non-interactive entrypoint. Also what CI would call. |
| `spaces.yml` | Which spaces to promote and the catalog mapping. |
| `databricks.yml` | Bundle definition: dev and prod targets. |
| `lib/rewrite_catalog.py` | Rewrites catalog names inside a space. |
| `lib/parameterize.py` | Swaps literal warehouse/folder for target variables. |
| `lib/binding_state.py` | Asks the bundle which prod spaces it already tracks. |
| `lib/space_diff.py` | Field-level diff of a local space against the prod one. |
| `lib/fetch_prod_space.py` | Materializes an excluded space from prod so it is left alone. |
| `lib/read_config.py` | Reads the two config files; no dependencies required. |
| `resources/`, `src/` | Generated by the export step. Commit these. |
| `tests/` | Run them all: see [Running the tests](#running-the-tests) |

## Troubleshooting

**"databricks CLI 1.0.0 is too old"** — `brew upgrade databricks`. Genie spaces
need 1.3.0+.

**"engine: terraform ... cannot deploy Genie spaces"** — remove that line from
`databricks.yml`. Genie spaces only work with the default `direct` engine.

**"dev catalog reference(s) survived the rewrite"** — a catalog in the space is
missing from `catalog_map`. The listed JSON paths show which. Re-run `./promote`
and add the mapping.

**The space deploys but answers nothing** — almost always missing prod tables or
missing `SELECT` grants. The table check in step 5 reports this; it is a warning,
not a failure, because the space itself deployed fine.

**Two spaces with the same title in prod** — a space was created without a
`prod_id` when one already existed. Trash the duplicate, put the surviving ID in
`spaces.yml` as `prod_id`, and re-run.

**"already linked to prod space X but spaces.yml says prod_id: Y"** — the bundle
already tracks that space under a different ID. Either drop the `prod_id` line to
keep the existing link, or unbind first:
```bash
databricks bundle deployment unbind <key> -t prod -p prod
```

## Not covered

- Creating prod catalogs, schemas, tables, or grants. The tool rewrites table
  names and reports what is unreadable; it does not provision anything.
- Creating the CI service principal or granting it access.
- Genie benchmarks and evaluation history, which the space API does not carry.
