# The deployment machine

In v1, every prod and account apply runs from one deployment machine, with the
same commands and checks a pipeline would run. Pipelines run the read-only PR
check and, once dev has remote state, the dev rehearsal
([pipelines](pipelines.md#who-runs-what)). Prod moves to CI when prod remote
state exists (design section 15).

## The whole routine

```bash
# once
export GENIERAILS_STATE_DIR=/protected/genierails-state   # in the operator's shell profile
git clone <your-repo-url> /srv/genierails && cd /srv/genierails/aws   # the one persistent checkout
#   fill envs/prod/auth.auto.tfvars (and envs/dev, envs/account) with the deployer SPs

# every release: move to the commit the pipeline names
git fetch && git checkout --force --detach <sha>
git status --porcelain                         # pass looks like: prints nothing
make release-plan ENV=prod                     # prints the manifest path; changes nothing
#   get the manifest approved
make release ENV=prod MANIFEST=<manifest>      # applies exactly that manifest

# on a schedule, at the last released commit
make maintain ENV=prod
```

## Set it up once

1. **One persistent checkout.** Clone your repo once and always work there.
   Don't delete its ignored files: the `.state-moved` marker and the auth
   files live in it. With `GENIERAILS_STATE_DIR` set (step 2), nothing else a
   later run needs is in the checkout, so a fresh clean checkout of the
   commit being applied works too.
2. **State outside the checkout.** Create the state dir once
   (`mkdir -m 700 /protected/genierails-state`, or let `make migrate-state-dir`
   create it) and set `GENIERAILS_STATE_DIR` for every command (in the
   operator's shell profile, or a wrapper). Then Terraform state, receipts,
   manifests, the env locks and the runtime records (the staged policy
   journal, the migration record, Genie agent ID files and adoption markers)
   live under `$GENIERAILS_STATE_DIR/<cloud>/<env>/`, and the account layer
   under `<cloud>/account/` ([below](#state-outside-the-checkout)).
3. **Credentials.** Fill each env's `auth.auto.tfvars` with its deployer SP;
   `databricks_client_id` must equal the env's committed
   `deployer_principal_id`, or applying commands refuse before the lock.
4. **Protect it.** Anyone who can write the env files or the state dir here can
   run Terraform against prod directly. A saved plan carries the deployer
   credentials, so manifests are owner-only (`0700` dirs, `0400` files).

## Release prod

1. **Wait for the pipeline.** The prod release pipeline names a commit: a
   merge to `main` that touched `envs/prod/**` or `shared/**` and passed the dev
   rehearsal.
2. **Move the checkout.** `git fetch && git checkout --force --detach <sha>`.
   `--force` drops any edit to tracked files and leaves ignored state alone.
   **Pass looks like:** `git status --porcelain` prints nothing.
3. **Plan.** `make release-plan ENV=prod`. **Pass looks like:**
   `=== Release plan (prod) ===` and `Manifest (immutable): <path>`, then the
   exact `make release` command to run.
4. **Approve.** A governance code owner reviews the manifest's reports
   (readers, weakening, field-level agent overwrite) and saved plans.
5. **Release.** `make release ENV=prod MANIFEST=<manifest>`. **Pass looks
   like:** `ALL EFFECTIVE` and `=== Release complete (prod) ===`.
6. **More phases?** If it says to run `release-plan` again (a staged policy
   change, tier move or recorded imports), repeat steps 3 to 5 until
   "Release complete".

**If it stops** before the manifest, nothing changed. The message names the
cause:

| Message | What to do |
|---|---|
| no passing receipt for HEAD | the dev rehearsal hasn't passed on this commit; wait for it, or run `make rehearse ENV=dev` here until dev has remote state |
| not on freshly fetched `origin/main`, or older than the last release | release the newer commit; to roll back on purpose, merge `prod:rollback:<commit>` into `envs/prod/ack.txt` first |
| checkout is not clean | step 2 again |
| reader or weakening check | add the reviewed lines to `envs/prod/ack.txt` in a PR, or fix the config ([governance](playbooks/governance.md#acknowledge-an-exception)) |
| coverage check, or an unmapped class | map it in dev and promote ([governance](playbooks/governance.md#map-a-new-class)) |
| the account plan isn't empty | `make apply-account` first ([operations](#operations)) |
| env is locked | another command holds the env lock; wait for it |
| `release` refuses the manifest | something moved since `release-plan`; run `release-plan` again and get the new manifest approved |

The gates themselves: [release gating](pipelines.md#release-gating-deterministic-envs).

## Keep prod protected

```bash
make maintain ENV=prod      # cron, at the last released commit
```

It masks newly tagged columns with the library mask (or the fail-safe redaction
for an unmapped class), never grants access past a passing coverage check, and
never changes a Genie agent. It refuses unless HEAD is the last released
commit, so run it from the checkout as `release` left it. When it stops, it
prints what to do: usually review a detection in Catalog Explorer, or map a
class in dev and promote. Alert the governance team on failure.

## Operations

One approval per operation, each from a clean checkout of `main`:

| Operation | Command | Provenance it checks |
|---|---|---|
| Forget a detached agent | `make detach-agent ENV=prod SPACE="<agent>"` | HEAD is the last released commit, and that release had the agent with `detached = true` and `acl_groups = []` ([agents](playbooks/agents.md#remove-an-agent)) |
| Change the account layer | `make apply-account` | takes the account lock; the only command that removes tag-policy values |
| Migrate a legacy env | `make migrate-plan`, then `release-plan` / `release` per phase | [legacy-migration.md](legacy-migration.md) |
| Inspect or drop a staged policy change | `make policy-status ENV=prod`, `make policy-abandon ENV=prod` | `policy-abandon` refuses once a column was retagged or a principal moved |

## State outside the checkout

With `GENIERAILS_STATE_DIR` unset, state, receipts and the env lock live under
the checkout (`envs/<env>/` and `<cloud>/.genierails-state/<env>/`, all
gitignored). That only works for one checkout that is never re-cloned. With it
set, `$GENIERAILS_STATE_DIR/<cloud>/<env>/` holds `layers/<layer>/` (state and
`.terraform`), `locks/`, `receipts/`, `manifests/` and `records/`: the staged
policy journal and generation snapshots (`records/data_access/.policy_generations/`,
including the migration record) and group_map's last applied mapping, so a
fresh checkout sees a pending or failed staged change, and Genie agent ID
files and adoption markers (`records/.genie_space_id_<key>`,
`.genie_adopted_<key>`, `.genie_adopt_required_<key>`), so it knows which
agents GenieRails created. Only what each run re-derives
(`generated/.runtime/`, the coverage check's files) stays in the checkout.
Every reader of Terraform state goes through `scripts/state_layout.py`; a
state that should exist but can't be read refuses.

With `GENIERAILS_STATE_DIR` set, the directory it names must already exist:
a mistyped or unmounted path refuses every command (state, records, receipts
and locks alike) instead of reading as an empty store. Only
`make migrate-state-dir` creates it; the dev rehearsal samples create their
runner's throwaway one in a pipeline step. A record reader also refuses when
the state dir can't be read, and when a record is still in the checkout (alone
or next to the state dir's copy): run `make migrate-state-dir`.

Move existing local state once:

```bash
GENIERAILS_STATE_DIR=/protected/genierails-state make migrate-state-dir ENV=dev
GENIERAILS_STATE_DIR=/protected/genierails-state make migrate-state-dir ENV=prod
GENIERAILS_STATE_DIR=/protected/genierails-state make migrate-state-dir ENV=account
```

It moves Terraform state and the runtime records above. For each Genie ID
file it first records, in `records/.genie_id_moves.json`, the in-checkout path
the workspace state names, so the agent's create resource (and any
create-to-ID handoff) is still recognised and nothing is recreated; the next
workspace apply records the new path in place. It refuses while the
env lock is held, never overwrites a different file, and
copies, fsyncs and verifies each file before removing its source, so a re-run
after a crash resumes. It writes `envs/<env>/.state-moved` first: from then on
a run without `GENIERAILS_STATE_DIR` refuses instead of planning to create
everything. Run the older `make migrate-state` (pre-layer layouts) before it.

## Locks

- **One env lock per env** (`locks/.governance.lock` in the state dir).
  `rehearse`, `release-plan`, `release`, `maintain`, `ungovern` and
  `detach-agent` take it; a second command on the same env refuses until the
  first finishes. Because it lives in the state dir, every checkout that shares
  `GENIERAILS_STATE_DIR` shares it.
- **The account lock** is the env lock of `ENV=account`; `make apply-account`
  takes it.
- A lock records a random nonce, the holder's pid and that pid's start time, so
  a leftover lock from a crashed run never blocks or passes by accident.

## Tear down

```bash
make destroy ENV=prod CONFIRM_DESTROY=prod   # a promoted deterministic env needs the confirmation
make destroy ENV=dev
make destroy ENV=account                     # shared account layer, last
```

`make destroy-governance` and `make destroy-genie` remove one layer.
