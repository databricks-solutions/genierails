# Ship a Genie agent from dev to prod

Take a Genie agent you've curated in **dev** and ship it to **prod** with its sensitive columns masked. Databricks Data Classification finds the sensitive columns; GenieRails turns that into masks and access rules as code. No new business access is granted until every classified sensitive column the agent can reach is covered by a mask policy, checked against freshly read tags.

## The whole flow

```bash
# dev
make setup ENV=dev REPO=<your-repo-url>   # first run: origin -> genierails (kept for upgrades), origin = your repo; writes the .gitignore and env templates, then offers to commit + push them; set deployer_principal_id and groups; per agent, its ID and acl_groups
#   Catalog Explorer: turn on Data classification, review detections, enable auto-tagging
make generate ENV=dev       # imports the agent, finds its tables, drafts the rules; shows the diff, offers to commit + push (y)
make account-plan           # first deploy: plan the shared account layer (gr_treatment tag policy) for approval
make apply-account          # applies exactly that approved account manifest (rehearse refuses a non-empty account plan)
make rehearse ENV=dev       # checks the committed files, applies in dev and proves the masks work (verify-access: ALL EFFECTIVE)

# prod (same for stg or any env)
make setup ENV=prod         # set deployer_principal_id, catalog_map and group_map; leave access_tier_groups placeholders
make promote-to ENV=prod    # copies the reviewed rules; shows the diff, offers to commit + push (y)
#   Catalog Explorer: same classification review on the prod catalog
make release-plan ENV=prod  # reuses dev's rehearsal for this commit (dev isn't re-applied) after checking the committed files, plans prod, writes the manifest to approve
make release ENV=prod       # applies exactly the approved plan, proves the masks; a first release repeats release-plan + release until "Release complete"
make evidence ENV=prod      # live compliance record; finds the warehouse itself
make maintain ENV=prod      # deployment machine, cron, at the last released commit
```

<a id="prerequisites--gather-required-values"></a>
## Before you start

```bash
git clone https://github.com/databricks-solutions/genierails.git
cd genierails/aws          # or: cd azure — every make command runs from the cloud folder
```
Have an empty repo you own ready: [Dev step 1](#phase-0--dev-set-up)'s `make setup ENV=dev REPO=<your-repo-url>` makes this clone yours. It renames `origin` (GenieRails) to `genierails`, kept for upgrades, and adds `origin` = your repo. It never pushes, and re-running it with the same `REPO=` changes nothing; a different `REPO=` once `origin` exists is refused (setup never repoints a remote), and so is a checkout nested inside another repo. Only `REPO=` on the command line counts (or one inherited through `MAKEFLAGS`, which make can't tell apart); an exported shell `REPO` is ignored.
Prefer to manage remotes yourself? Run `git remote rename origin genierails` and `git remote add origin <your-repo-url>` before setup and leave `REPO=` off.
No repo of your own yet? Leave `REPO=` off: setup still renames `origin` and keeps a local-only repo (no remote, no GitHub account), where you commit, generate and rehearse. `make account-plan` and `make release-plan` still need your repo as `origin` for now, so add it with `make setup ENV=dev REPO=<your-repo-url>` before the first deploy.

| You need | Notes |
|---|---|
| The [prerequisites](../../docs/prerequisites.md) | Tools, a deploying service principal (`make bootstrap-sp`), IdP-synced groups |
| Your Genie **Agent ID** | Genie UI → open the agent → **Configure** → *About this agent*. *No agent yet?* Use the [sample environment](SAMPLE_ENV.md) |
| Who may run the agent | The groups that get Genie `CAN_RUN` on it (its `acl_groups`), chosen from your access-tier groups. Required: `[]` means nobody |
| Deploying SP application ID | The `databricks_client_id` from each env's `auth.auto.tfvars`; it is committed as `deployer_principal_id` (not secret) |
| Access-tier groups | Your existing groups, most- to least-privileged, e.g. `payments_ops`, `regional_analysts`, `viewers`. Order matters: a tier listed above a masked tier also gets the agent and sees those columns unmasked |
| Dev and prod catalog names | e.g. `dev_finance` and `prod_finance` |

<a id="phase-0--dev-set-up"></a>
<a id="phase-1--dev-scan-draft-and-test-rules"></a>
## Dev

**1. Set up.**
```bash
make setup ENV=dev REPO=<your-repo-url>
cp ../shared/examples/dev_to_prod/env.auto.tfvars.example envs/dev/env.auto.tfvars
mkdir -p envs/dev/agents
cp ../shared/examples/dev_to_prod/agent.auto.tfvars.example "envs/dev/agents/Finance Analytics.auto.tfvars"
```
Fill in `envs/dev/auth.auto.tfvars` (service principal and workspace). The template starts new champions with `governance_mode = "deterministic"`; existing env files are not changed. In `envs/dev/env.auto.tfvars`, replace the deployer and group placeholders:
```hcl
deployer_principal_id = "<the dev auth.auto.tfvars databricks_client_id>"
access_tier_groups = ["payments_ops", "regional_analysts", "viewers"]
```
**Agents (preferred):** one file per agent in `envs/dev/agents/`; the file name is the agent's name. In it, set the agent's ID and who may run it:
```hcl
genie_space_id = "01ef0000000000000000000000000000"   # Genie UI > agent > Configure > About this agent
acl_groups     = ["regional_analysts", "viewers"]      # who gets CAN_RUN on the agent; [] means nobody
```
**Also accepted:** the same fields inline in `env.auto.tfvars` as `genie_spaces = [{ name = "...", genie_space_id = "...", acl_groups = [...] }]` (leave `genie_spaces = []` when using agent files). Prefer `agents/` files so agent and governance PRs stay separate; `make split-agent-files` moves an inline list into files later.
`acl_groups` is required: `make generate` stops while an agent has none. It grants only `CAN_RUN` on the agent. `SELECT` on the agent's tables comes only from `table_readers` in `env.auto.tfvars`, which `make generate` proposes for you (filled with `acl_groups`) for each table nothing covers yet. Every `acl_groups` group must be able to read every table of the agent, so pick them from `access_tier_groups` (only tier groups and `raw_exempt_principals` can be readers); otherwise `generate`, `rehearse` and `release` stop and name the table and group.

**2. Commit it (own repo).** At the end of step 1, `make setup` previews exactly the `.gitignore` and env files it created, then asks whether to commit and push them. Answer `y`; credentials and runtime files such as `auth.auto.tfvars` are never staged. With no `origin`, it commits locally only. `make check` reads only committed env files, a rehearse receipt counts only for a clean checkout, and `make release-plan` needs that commit on your `origin/main`. Commit (and push) again after each later step that changes `envs/`; `make generate` and `make promote-to` make the same offer. Later changes go through PRs: [Commit it](#commit-it).

<a id="dev-classify"></a>
**3. Classify (in the UI).** In **Catalog Explorer**, open the dev catalog → **Details** → **Data classification**: turn it on. When the scan finishes (the first one can take up to about a day), review the detections, exclude false positives, and turn on auto-tagging. Then wait until the columns show `class.*` tags such as `class.email_address`: open a table in Catalog Explorer and check its **Columns** tab.

**4. Generate.**
```bash
make generate ENV=dev
```
It captures each new agent to `envs/dev/agents/<agent>.space.json` and proposes its `table_readers`. Review those and the drafted rules in `envs/dev/generated/`. If the tags aren't there yet, it stops and tells you to wait; re-run it later. Then it shows `git diff --stat` for `.gitignore` and `envs/dev/` and asks `Commit these changes and push them to origin/main? [y/N]`: `y` commits exactly those paths and pushes (never forced; with no `origin` it commits locally only), `n` leaves git unchanged. Off a terminal, in CI or with `COMMIT=0` it asks nothing: commit and push `envs/` yourself. What a re-run keeps depends on the env's `governance_mode`:
- **Deterministic** (the template's default): every run recomputes the column masks, the `gr_treatment` column tags, the `gr_treatment` tag policy and the mask-library functions in `masking_functions.sql` from the `class.*` tags and the mask library. It replaces hand edits to those in `envs/dev/generated/` without a warning; row filters, table tags and any custom SQL above the `-- === GenieRails deterministic mask library ===` marker in `masking_functions.sql` are kept. To change a treatment, set it in `envs/dev/env.auto.tfvars` and re-run `make generate ENV=dev`: `column_overrides` for one column, `treatment_versions` for a treatment's partial version, `tag_treatments` to map a class to another treatment ([settings](REFERENCE.md#deterministic-governance-settings)).
- **Legacy**: re-runs keep whatever is already in `envs/dev/generated/`, including your edits, and only add rules for new columns.

**5. Rehearse.**
On a fresh repo, apply the shared account layer first. `make rehearse` (like `make release`) only adds allowed treatment-tag values to it, and stops while the account plan has anything else to apply (`ERROR: account plan is non-empty`): on a first deploy, the `gr_treatment` tag policy (and the groups, with `manage_groups = true`). From a clean checkout at your pushed `origin/main`:
```bash
make account-plan           # saves the account plan and writes the account manifest to approve; changes nothing live
make apply-account          # applies exactly that manifest
```
Run the pair again whenever a later `rehearse` or `release-plan` stops on a non-empty account plan. Then:
```bash
make rehearse ENV=dev
```
It first runs `make check ENV=dev`: a read-only check of what git tracks under `envs/`, in a scratch copy (validate, generated files current, validate-generated, the offline coverage check, mask tests and a structural plan). If a step fails, it refuses with that step's message before it takes the lock, writes anything or reads live dev. The same `make check` runs alone as the PR check.
It applies in dev, then runs `verify-access` as its last gate (`_verify-access-gate`), querying as each test tier. A first deploy applies in phases: the masking functions and policies first, then the column tags, then (once the masks are verified live) the tier groups' `USE_CATALOG`, `USE_SCHEMA` and `SELECT`; Genie `CAN_RUN` follows the SELECT. If it stops at the first phase, nothing is live yet: fix what it reports and re-run `make rehearse ENV=dev` (it resumes), or drop the change with `make policy-abandon ENV=dev`.
**Pass looks like:** `ALL EFFECTIVE`: each test tier sees masked values, the authorized tier sees raw ones. To re-run only that proof later: `make verify-access ENV=dev`.
You don't set a row-pairing key: it picks one per masked table (a single-column primary key, else an untagged id-like column such as `customer_id`) and proves it unique, non-null and unmasked. A deterministic env then prints the proven `verify_key_columns` line; committing it is optional and pins those keys. Only a table with no provable key needs `verify_key_columns` set ([how keys are picked](../../docs/effective-access-verification.md#how-genierails-picks-the-row-pairing-key)).

<a id="phase-2--prod-set-up-and-promote-rules"></a>
<a id="phase-3--prod-scan-real-data"></a>
<a id="phase-4--prod-release-and-verify"></a>
## Prod

**1. Set up and promote.**
```bash
make setup ENV=prod
```
Fill in `envs/prod/auth.auto.tfvars` (same service principal as dev? leave it blank and add `REUSE_SOURCE_AUTH=1 WORKSPACE_HOST=<prod host> WORKSPACE_ID=<prod id>` to the `promote-to` below: it copies dev's account ID, client ID and secret into the still-blank file and never dev's host), replace its `deployer_principal_id` placeholder and the seeded `catalog_map` line (`make setup` wrote one with placeholders; a second `catalog_map` line is refused), and set the rest:
```hcl
catalog_map = { "dev_finance" = "prod_finance" }   # dev catalog = prod catalog
sql_warehouse_id = "<existing-prod-warehouse-id>"   # or "" to create a new serverless warehouse
group_map   = { "payments_ops" = "prod_payments_ops" }   # dev group = prod group, only where they differ
```
Each `catalog_map` entry must name a distinct prod catalog. `"dev_finance" = "dev_finance"` is refused before anything is written: on a shared metastore prod's state would adopt dev's masks, and a later dev retire would drop prod's. If dev and prod use separate metastores and keep the same catalog names, add `allow_identity_catalog_map = true` to `envs/prod/env.auto.tfvars`.
Leave the `access_tier_groups` placeholders: `promote-to` writes prod's tiers from dev's (and seeds `table_readers` and agent `acl_groups`), through `group_map`.
If existing prod tiers cannot be traced to dev's mapped tiers, promotion refuses
before writing and prints the missing `group_map` entry to add.
Then:
```bash
make promote-to ENV=prod
```
It copies dev's reviewed rules into prod, renamed to prod's catalogs: masks and policies, `tag_treatments`, adopted custom masks, the agent capture and the proven `verify_key_columns`; dev's tier groups are mapped through prod's `group_map`. It leaves dev's tag assignments behind (prod derives its own from its own tags) and keeps prod's own agent ID, warehouse, `acl_groups` and `table_readers` once set, and the names of policies already live in prod.
Every promotion writes `enable_classification = true` for prod, for both
deterministic and legacy environments. A prod value of `false` does not persist;
in a deterministic environment, committing it also fails the promotion-current
check.
Using the sample environment? Load its tables into prod first (`--skip-agent`, see [SAMPLE_ENV.md](SAMPLE_ENV.md)).
It then offers to commit and push `.gitignore`, `envs/prod/` and `envs/account/` (the shared account layer it writes) the same way (answer `y`; off a terminal, commit and push them yourself): `make setup ENV=prod` added prod's lines to `.gitignore`, and `make rehearse` and `make release-plan` need a clean checkout.

**2. Classify (in the UI).** Same as dev, on the prod catalog. Prod's real data may surface sensitive types dev never had.

**3. Plan, release.** Plan and review the manifest, then apply exactly what was approved:
```bash
make release-plan ENV=prod
make release ENV=prod
```
`make release-plan ENV=prod` plans only a commit on your `origin/main` that has a passing `make rehearse ENV=dev` of that same commit (dev is prod's `promote_from` in this walkthrough; with `promote_from = "stg"` it is `make rehearse ENV=stg`); the dev step 5 rehearsal was of an earlier commit. Since the promotion changed only `envs/prod/`, release-plan reuses that rehearsal itself and applies nothing in dev: it prints `reusing dev's passing rehearsal of <earlier commit> (nothing is applied)`, then `code and dev inputs are unchanged since the passing rehearsal of <earlier commit>; wrote a receipt for <this commit> without re-applying` and the `promote-to ENV=prod digest` it recorded. If code or `envs/dev/` changed since (or the checkout isn't clean, or dev was rehearsed only in CI), it stops with `did not reuse a rehearsal for this commit: <why>; run make rehearse ENV=dev on this commit` and `no passing rehearsal of <commit>`: run the full `make rehearse ENV=dev`, then `make release-plan ENV=prod` again.
Legacy envs skip `make release-plan`: run `make release ENV=prod` directly (`release-plan` refuses them with `Legacy envs keep: make release ENV=prod`).
`make release-plan` first runs `make check ENV=prod`, which for a promoted env also runs `promote-to CHECK=1`; if a step fails, it refuses before any lock, live call or write. `make release` releases the manifest the last `release-plan` wrote, printing its path, commit and changes first. It refuses if that manifest was already released, if a newer `release-plan` replaced it, or if anything changed since it was planned. A pipeline can name the manifest explicitly with `MANIFEST=<path>`.
**A first release takes several rounds.** Each deterministic `make release ENV=prod` applies one manifest, and a first release needs one per step: the masking functions and policies, then the column tags, then the tier groups' `USE_CATALOG`, `USE_SCHEMA` and `SELECT`, then (for an env with agents) Genie `CAN_RUN`, which is planned only once that `SELECT` is live. A prod with existing resources that aren't in Terraform state yet starts with one more manifest that only imports them. Every round but the last prints `=== Release phase applied (prod) ===`, then a line saying what it applied (for the import round `This manifest imported existing resources into Terraform state; nothing else was applied.`, for a policy phase `This manifest held one phase of a staged policy change; it is applied and verified.`, for the `SELECT` round `This manifest opened the SELECT the Genie agents need; the workspace (CAN_RUN) is planned with it live.`), then `Next: make release-plan ENV=prod, approve the new manifest, make release ENV=prod.`. Do exactly that on the same commit (its dev rehearsal still counts) until `make release` prints `=== Release complete (prod) ===`. Later policy replacements and tier moves take one round per phase too ([staged policy changes](../../docs/deterministic-policies.md#staged-policy-changes-rollout-step-6)).
**Expect owner and admin reader refusals on the first `release-plan`.** The reader check refuses every principal that can read a governed table outside the tiers, `raw_exempt_principals` and prod's own `deployer_principal_id`: typically the metastore admin and the owner of the prod catalog, schema and tables (`ERROR: unacknowledged reader prod:reader:<table>:<principal>: ...`). The tier groups prod's `table_readers` grants are its reviewed contract and aren't refused. Review each printed line; for each you accept, add its key (`prod:reader:<table>:<principal>`) to `envs/prod/ack.txt`, then commit, push, and `make release-plan ENV=prod` again (it reuses the dev rehearsal: `ack.txt` isn't a dev input). The lines stay valid for every round; after `Release complete`, the next `release-plan` reports them as stale (those tables are no longer newly governed): delete them then.
**Pass looks like:** `ALL EFFECTIVE` (from its `verify-access` gate) and "Release complete". Business users can now open the agent and get masked answers. To re-run only that proof later: `make verify-access ENV=prod`.

**If it stops on coverage or drift**, prod found a sensitive tag your rules don't cover. Nothing new was granted. The message prints both fixes with exact commands:
- **Reuse an existing mask** (the common case): `make scaffold-treatments ENV=prod TREATMENT=<name>` adds the class to `tag_treatments` in `envs/dev/env.auto.tfvars` (`shared/` is unchanged). Commit it, then `make generate ENV=dev`, `make rehearse ENV=dev`, `make promote-to ENV=prod`, `make release-plan ENV=prod` (it reuses the dev rehearsal, or names the command to run) and `make release ENV=prod`, committing and pushing after each step that changes `envs/`.
- **A new kind of mask**: draft and test it with the dev-only helpers (`make draft-custom-mask` / `make test-custom-mask`, see the [governance playbook](../../docs/playbooks/governance.md)). Once it passes, `make adopt-custom-mask ENV=dev TREATMENT=custom_<name>` copies it into `envs/dev/custom_masks/` and prints the `tag_treatments` line to add to `envs/dev/env.auto.tfvars`. Commit both, then continue from `make generate ENV=dev` as above. The bare `make scaffold-treatments` stub and `materialize-treatment` are legacy-mode only ([legacy mode](../../docs/legacy-mode.md)).

Details: [Fixing a coverage gap found in prod](REFERENCE.md#fixing-a-coverage-gap-found-in-prod).

**4. Record evidence.**
```bash
make evidence ENV=prod
```
It reads the live tags, masks and grants of prod's agent tables and writes a JSON record and Markdown summary to `.genierails-state/prod/evidence/` (git-ignored, next to the release receipts; under `$GENIERAILS_STATE_DIR` when that is set, which must be outside every git checkout or git-ignored in each one). It finds the SQL warehouse itself (prod's `sql_warehouse_id`, else the one the release applied); `WAREHOUSE_ID=<id>` only pins another, and `OFFLINE=1` writes a config-only report. The checkout stays clean, so `make maintain` and the next `release-plan` run as usual. Archive the record (e.g. with the change ticket): the next run replaces it. [Details](../../docs/evidence-report.md).

<a id="phase-5--prod-maintain-coverage"></a>
## Keep it safe

```bash
make maintain ENV=prod      # deployment machine, cron, at the last released commit
```
Run it from the [deployment machine](../../docs/deployment-machine.md). It masks newly tagged columns, never grants access past a passing coverage check, and never changes the Genie agent. In a deterministic env it runs only from the last released commit, on a clean checkout. If it stops, it prints what to do: usually review a new detection in Catalog Explorer, or add a missing rule in dev and re-promote.

<a id="commit-it"></a>
## Commit it

[Dev step 1](#phase-0--dev-set-up)'s `make setup ENV=dev REPO=<your-repo-url>` puts the checkout in your own repo and writes `.gitignore`; Dev step 2 commits it with `envs/`. Do this before `make check`, `make rehearse` or `make release-plan`.
If you already ran `make setup` without `REPO=`, re-run `make setup ENV=dev REPO=<your-repo-url>`
(it adds `origin`), then commit `.gitignore` and `envs/`.

From then on every change goes through a PR into `main`, with `make check` as its check (`make release-plan` plans only a commit on `origin/main`): see [Pipelines](../../docs/pipelines.md) to set up CI, and the [agents](../../docs/playbooks/agents.md), [governance](../../docs/playbooks/governance.md) or [one-team](../../docs/playbooks/one-team.md) playbook for day-2 changes.
