# Ship a Genie agent from dev to prod

Take a Genie agent you've curated in **dev** and ship it to **prod** with its sensitive columns masked.

This page is the happy path; each step links to its [details, edge cases and CI](DETAILS.md).

<a id="how-it-works"></a>
## How it works in 1 minute

- **Rules are files in your repo.** Databricks Data Classification tags your sensitive columns. GenieRails turns the tags into masks and access rules, saved as files.
- **Test in dev, ship the same files.** You prove the rules in dev; prod gets the same files, renamed to prod's catalogs and groups.
- **Why commit.** A commit is a frozen, fingerprinted copy of what you tested. Prod releases only a commit that passed in dev, so it gets exactly what dev proved. Teammates review changes in a pull request; git keeps the history.
- **Why a manifest.** A manifest is a saved plan of exactly what a step will change. You read it and answer `y`; GenieRails applies exactly that plan, and refuses if anything changed since you saw it.
- **Masks first, access last.** No one new gets access until every classified column the agent can reach has a mask and a test proves the masks work. Otherwise a column could leak, so GenieRails stops.
- **So each sensitive column needs a decision:** mask it or, if the detection is wrong, exclude it in the classification review. [Options](DETAILS.md#if-prod-stops-on-coverage-or-drift); [why no raw-for-everyone switch](DETAILS.md#why-no-unmasked-sensitive-column).

<a id="glossary"></a>
## Words used on this page

All other terms: [full glossary](REFERENCE.md#glossary).

| Word | In plain words |
|---|---|
| **env** | One place you deploy to, e.g. `dev` or `prod`, with its own `envs/<env>/` folder |
| **tier** (access-tier group) | An existing group whose users see data at one level, e.g. `payments_ops` raw, `viewers` masked |
| **classification tag** (`class.*`) | The tag Data Classification puts on a sensitive column, e.g. `class.email_address` |
| **treatment / mask** | How a column is hidden from a tier, e.g. a card shown as `****-****-****-4464` |
| **rehearse** | The full test in dev: apply the rules, then prove each tier sees what it should |
| **manifest** | A saved plan of exactly what a step will change; you approve it with `y` |
| **receipt** | The record that a rehearse or release passed: the commit and, for a release, who approved each round |
| **ack** (`ack.txt`) | A committed line saying "I reviewed this access and accept it" |

## The whole flow

```bash
# dev
make setup ENV=dev REPO=<your-repo-url>   # REPO= optional; makes the clone yours; offers to commit (y)
#   fill in the env + agent files setup copied from the templates (Dev step 1)
#   Catalog Explorer: turn on Data classification, review detections, enable auto-tagging
make generate ENV=dev       # drafts the rules from the class.* tags; offers to commit + push (y)
make rehearse ENV=dev       # checks; first deploy: account plan y/N; applies dev, proves the masks

# prod (same for stg or any env)
make setup ENV=prod         # then set deployer_principal_id, catalog_map, group_map
make promote-to ENV=prod    # copies the reviewed rules; offers to commit + push (y)
#   Catalog Explorer: same classification review on the prod catalog
make release ENV=prod       # each round: plans, shows the manifest, asks y/N, applies it; until "Release complete" (first: account plan y/N, if it has changes)
make evidence ENV=prod      # live compliance record
make maintain ENV=prod      # deployment machine, cron, at the last released commit

# lost? make next ENV=<env>
```

<a id="what-each-command-asks"></a>
### What each command does and what it asks

Questions appear only at a terminal ([CI / scripted](#ci--scripted)). Every detail: [the full table](DETAILS.md#what-each-command-does-in-full).

| Command | What it does | What it asks | What your `y` approves |
|---|---|---|---|
| `make setup ENV=dev REPO=<url>` | Makes the clone yours and creates dev's env and agent files | `Commit these changes and push them to origin/main? [y/N]` (no `origin`: `... and keep them local (no origin remote)? [y/N]`) | Save these files as a version in your repo |
| `make generate ENV=dev` | Drafts the rules in `envs/dev/generated/` | The same commit question | Same: save this step's files |
| `make rehearse ENV=dev` | Checks, applies dev and proves the masks | `Approve and apply this account manifest? [y/N]`, only when the account plan has changes | The account-wide setup shown, e.g. the masks' tag policy |
| `make setup ENV=prod` | Creates prod's env files (no agent file: `promote-to` writes prod's) | The same commit question | Same: save this step's files |
| `make promote-to ENV=prod` | Copies dev's reviewed rules into prod | The same commit question | Same: save this step's files |
| `make release ENV=prod` | Plans, applies and proves one manifest per round | `Approve and apply this account manifest? [y/N]` first, only when the account plan has changes; then `Approve and release manifest <id> (commit <sha>, round <n> of at most 8)? [y/N]`, once per round | Account: as above. Each round: exactly that round's prod changes |
| `make evidence ENV=prod` | Records prod's live tags, masks and grants | nothing | — |
| `make maintain ENV=prod` | Masks newly tagged columns | nothing | — |
| `make next ENV=<env>` / `make status ENV=<env>` | Read-only: the next command / the whole picture | nothing | — |

What stays a human step: the Catalog Explorer review, filling in the env files, and committing `envs/prod/ack.txt` when prod's reader check names owners or admins you accept (an ack is never a `y/N`).

<a id="prerequisites--gather-required-values"></a>
## Before you start

```bash
git clone https://github.com/databricks-solutions/genierails.git
cd genierails/aws          # or: cd azure — every make command runs from the cloud folder
```

- Have an empty repo you own ready for dev step 1's `make setup ENV=dev REPO=<your-repo-url>`.
- No repo of your own yet? Leave `REPO=` off. The commit offers then commit locally.
- More: [your repo and remotes](DETAILS.md#your-repo-and-remotes).

| You need | Notes |
|---|---|
| The [prerequisites](../../docs/prerequisites.md) | Tools, a deploying service principal (`make bootstrap-sp`), IdP-synced groups |
| Your Genie **Agent ID** | Genie UI → agent → **Configure** → *About this agent*. *No agent yet?* Use the [sample environment](SAMPLE_ENV.md) |
| Who may run the agent | Its `acl_groups`: the groups that get Genie `CAN_RUN`, chosen from your access-tier groups |
| Deploying SP application ID | Each env's `databricks_client_id` (`auth.auto.tfvars`), committed as `deployer_principal_id` (not secret) |
| Access-tier groups | Your existing groups, most- to least-privileged. Order matters: a tier listed above a masked tier also gets the agent and sees those columns unmasked |
| Dev and prod catalog names | e.g. `dev_finance` and `prod_finance` |

<a id="phase-0--dev-set-up"></a>
<a id="phase-1--dev-scan-draft-and-test-rules"></a>
## Dev

**1. Set up.** Create dev's files.

*Why:* with `REPO=`, your changes go to your own repo, where teammates can review them.

```bash
make setup ENV=dev REPO=<your-repo-url>
```
- Setup copies this walkthrough's templates into dev: [`env.auto.tfvars.example`](env.auto.tfvars.example) to `envs/dev/env.auto.tfvars` and [`agent.auto.tfvars.example`](agent.auto.tfvars.example) to `envs/dev/agents/Finance Analytics.auto.tfvars`. It copies only files that don't exist yet, so it never overwrites your edits and a re-run changes nothing.
- Fill in `envs/dev/auth.auto.tfvars` (service principal, workspace). In `envs/dev/env.auto.tfvars`, replace the deployer and group placeholders:
  ```hcl
  deployer_principal_id = "<the dev auth.auto.tfvars databricks_client_id>"
  access_tier_groups = ["payments_ops", "regional_analysts", "viewers"]
  ```
- **Agents (preferred):** rename setup's `Finance Analytics.auto.tfvars` to your agent's name. Set its ID and `acl_groups` (from `access_tier_groups`):
  ```hcl
  genie_space_id = "01ef0000000000000000000000000000"   # Genie UI > agent > Configure > About this agent
  acl_groups     = ["regional_analysts", "viewers"]      # who gets CAN_RUN on the agent; [] means nobody
  ```

More: [more agents, inline `genie_spaces`, the `acl_groups` rules](DETAILS.md#dev-step-1-set-up).

**2. Commit it (own repo).** Save what setup created.

*Why:* the commit is the version you test in dev and ship to prod.

- At the end of `make setup`, GenieRails previews the `.gitignore`, placeholder env defaults and agent file it created and asks to commit and push them. Answer `y`.
- Credentials (`auth.auto.tfvars`) are never staged.
- Your edits after setup are committed later, by `make generate`'s own offer.

More: [why the commit matters](DETAILS.md#dev-step-2-commit-it).

<a id="dev-classify"></a>
**3. Classify (in the UI).** Tag dev's sensitive columns.

*Why:* GenieRails builds the masks from these tags.

- In **Catalog Explorer**, open the dev catalog → **Details** → **Data classification**: turn it on.
- When the scan finishes (the first one can take up to about a day), review the detections, exclude false positives, and turn on auto-tagging.
- Wait until the columns show `class.*` tags: check a table's **Columns** tab in Catalog Explorer.

**4. Generate.** Draft the rules from the tags.

*Why:* it drafts the rules from what classification found; you review instead of writing them.

```bash
make generate ENV=dev
```
- Review the `table_readers` it proposes in `envs/dev/env.auto.tfvars` and the drafted rules in `envs/dev/generated/`.
- No tags yet? It stops and says to wait; re-run it later.
- It then offers to commit and push `.gitignore` and `envs/dev/`. Answer `y`.

To change a mask, edit `envs/dev/env.auto.tfvars` and re-run `make generate ENV=dev`. In the template's default mode a re-run replaces hand edits to the masks in `envs/dev/generated/`. More: [what a re-run keeps](DETAILS.md#dev-step-4-generate).

**5. Rehearse.** Apply dev and prove the masks work.

*Why:* it proves in dev that the masks really hide the data before anyone gets access.

From a clean checkout at your pushed `origin/main`, in a terminal:
```bash
make rehearse ENV=dev
```
- It first runs `make check ENV=dev`. If a check fails, it stops before it takes the lock, writes anything or reads live dev.
- A first deploy shows the shared account plan and asks `Approve and apply this account manifest? [y/N]`. That is the account-wide setup all envs share, e.g. the masks' tag policy. Answer `y`.
- It applies dev, in phases on a first deploy: masks before access. Then it runs `verify-access` as its last gate (`_verify-access-gate`).

**Pass looks like:** `ALL EFFECTIVE`: each test tier sees masked values, the authorized tier sees raw ones. To re-run only that proof later: `make verify-access ENV=dev`.

More: [the account layer, phases, row-pairing keys](DETAILS.md#dev-step-5-rehearse).

<a id="phase-2--prod-set-up-and-promote-rules"></a>
<a id="phase-3--prod-scan-real-data"></a>
<a id="phase-4--prod-release-and-verify"></a>
## Prod

**1. Set up and promote.** Create prod's files and copy dev's reviewed rules.

*Why:* `promote-to` copies the exact tested rules to prod.

```bash
make setup ENV=prod
```
- Fill in `envs/prod/auth.auto.tfvars`.
- In `envs/prod/env.auto.tfvars`, replace the `deployer_principal_id` placeholder and the seeded `catalog_map` line, and set the rest:
  ```hcl
  catalog_map = { "dev_finance" = "prod_finance" }   # dev catalog = prod catalog
  sql_warehouse_id = "<existing-prod-warehouse-id>"   # or "" to create a new serverless warehouse
  group_map   = { "payments_ops" = "prod_payments_ops" }   # dev group = prod group, only where they differ
  ```
- Leave the `access_tier_groups` placeholders: `promote-to` writes prod's tiers from dev's, through `group_map`.

```bash
make promote-to ENV=prod
```
- It then offers to commit and push `.gitignore`, `envs/prod/` and `envs/account/`: answer `y`.

More: [reusing dev's service principal, `catalog_map` rules, what `promote-to` copies](DETAILS.md#prod-step-1-set-up-and-promote).

**2. Classify (in the UI).** Same as dev, on the prod catalog.

*Why:* prod's real data may hold sensitive types dev never had.

**3. Plan, release.** Release prod, one approved round at a time.

*Why:* it applies the rules to prod one safe step at a time; each step shows you its plan to approve.

One command, at a terminal on the deployment machine:
```bash
make release ENV=prod
```
- Each round shows a manifest and asks `Approve and release manifest <id> ...? [y/N]`: `y` applies exactly it. Anything else applies nothing and stops.
- A first release takes several rounds, masks before access. Answer `y` each round until it prints `=== Release complete (prod) ===`.
- It reuses dev's rehearsal; it never rehearses dev. If it says `no passing rehearsal of <commit>`, run `make rehearse ENV=dev` first.

**Expect owner and admin reader refusals on the first round.** Owners and admins usually can read prod's tables outside your tiers; GenieRails doesn't manage that access, so you accept it in writing. Add each `prod:reader:<table>:<principal>` key you accept to `envs/prod/ack.txt`, e.g.:
```text
# reviewed: owner group of prod_finance
prod:reader:prod_finance.payments.transactions:finance_admins
```
Commit and push it yourself, then run `make release ENV=prod` again.

**Pass looks like:** `ALL EFFECTIVE` (from its `verify-access` gate) and "Release complete". To re-run only that proof later: `make verify-access ENV=prod`.

More: [each round, the account layer, rehearsal reuse, reader acks](DETAILS.md#prod-step-3-plan-release).

**If it stops on coverage or drift**, prod found a sensitive tag your rules don't cover. Nothing new was granted; the message prints the fix. More: [both fixes](DETAILS.md#if-prod-stops-on-coverage-or-drift).

**4. Record evidence.** Write a compliance record of what is live.

*Why:* a record for auditors of what is live in prod.

```bash
make evidence ENV=prod
```
- It writes a JSON record and Markdown summary to `.genierails-state/prod/evidence/` (git-ignored).
- Archive the record, e.g. with the change ticket: the next run replaces it.

More: [warehouse, offline mode, state folder](DETAILS.md#prod-step-4-record-evidence).

<a id="phase-5--prod-maintain-coverage"></a>
## Keep it safe

```bash
make maintain ENV=prod      # deployment machine, cron, at the last released commit
```

*Why:* it catches columns tagged later and masks them.

- Run it from the [deployment machine](../../docs/deployment-machine.md). Schedule it, e.g. with cron.
- In a deterministic env it runs only from the last released commit, on a clean checkout.
- It never grants access past a passing coverage check and never changes the Genie agent. If it stops, it prints what to do.

More: [what maintain does](DETAILS.md#keep-it-safe).

<a id="ci--scripted"></a>
## CI / scripted

Off a terminal (a pipeline, CI, piped output) nothing asks, and no flag or variable answers an approval `y` for you. Pipelines and four-eyes approval use the low-level steps: [CI / scripted](DETAILS.md#ci--scripted).

<a id="commit-it"></a>
## Commit it

Commit by hand only what no command offers: `envs/prod/ack.txt`, edits on a branch for a PR, and anything you answered `n` to. Later changes go through a PR into `main`, with `make check` as its check. More: [Commit it](DETAILS.md#commit-it), [Pipelines](../../docs/pipelines.md), and the [agents](../../docs/playbooks/agents.md), [governance](../../docs/playbooks/governance.md) and [one-team](../../docs/playbooks/one-team.md) playbooks.
