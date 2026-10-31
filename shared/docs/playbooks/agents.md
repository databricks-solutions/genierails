# Agents playbook (data citizen)

You curate a Genie agent in the dev UI and ship it to prod. You own
`envs/<env>/agents/<agent>.*`; governance owns everything else, including who
can read each table ([who owns which file](../pipelines.md#who-owns-which-file)).

## The whole flow

```bash
# dev: curate the agent in the Genie UI, then
make capture ENV=dev SPACE="Sales"     # writes envs/dev/agents/Sales.space.json (deterministic mode also refreshes
                                       #   the agent's tables in generated/governed_tables.json)
make generate ENV=dev                  # only if its tables changed: their masks and the governed-table record
make check ENV=dev                     # the PR check, locally
git add envs/dev/ && git commit        # open a PR; after merge the dev pipeline rehearses it

# prod
make promote-to ENV=prod               # carries the capture to envs/prod/agents/
make check ENV=prod
git add envs/prod/ && git commit       # the promotion PR; merge it
# on the deployment machine (it reuses dev's rehearsal):
make release ENV=prod                  # at a terminal: per round, plans, shows the manifest, asks y/N, applies it
#   separate approver: per round make release-plan ENV=prod, approve, make release ENV=prod MANIFEST=<path>
```

On a PR branch, `make generate` and `make promote-to` don't offer to commit
(the offer appears only on `main`, at a terminal): commit yourself, as above.
`make check` is the PR check; `make rehearse` and `make release-plan` run it
again themselves.

## Before you start

- Run `make` from `aws/` (or `azure/`).
- The envs are deterministic: `governance_mode = "deterministic"` in
  `envs/dev/env.auto.tfvars` (the champion template and new workspace setup use
  this default). Existing envs with no such line remain legacy: see
  [legacy mode](../legacy-mode.md) to migrate first.
- Your agents are in `agents/` files. Still inline in `genie_spaces`? Ask
  governance to run [`make split-agent-files`](../pipelines.md#who-owns-which-file).

## Add an agent

1. **Create its file**, `envs/dev/agents/<agent>.auto.tfvars` (the file name is
   the agent's name):
   ```hcl
   genie_space_id = "01ef0000000000000000000000000000"   # Genie UI > agent > Configure > About this agent
   acl_groups     = ["payments_ops", "regional_analysts"]   # who gets CAN_RUN; [] means nobody
   ```
2. **Import it.** `make generate ENV=dev`. It captures the agent to
   `agents/<agent>.space.json`, records its tables in
   `generated/governed_tables.json`, and proposes a `table_readers` entry
   (filled with `acl_groups`) for each of its tables that nothing covers yet.
   The proposal is governance's to approve in the same PR.
3. **Check and open a PR.** `make check ENV=dev`; commit `envs/dev/`.
   **Pass looks like:** `=== make check ENV=dev: passed ===`.
4. **Merge.** The dev pipeline runs `make rehearse ENV=dev` (until dev has
   remote state, the operator runs it). **Pass looks like:** `ALL EFFECTIVE`.
5. **[Promote](#promote-to-prod).**

## Update an agent

GenieRails never overwrites a dev agent's config, so curate freely in the UI.
Nothing reaches git until you capture it.

1. **Capture.** `make capture ENV=dev SPACE="<agent>"`. **Pass looks like:**
   `Captured: <agent> (<id>) -> agents/<agent>.space.json` (or `Unchanged`).
2. **Tables changed?** Run `make generate ENV=dev` too, so the PR shows the new
   tables' masks. A table change also changes `generated/governed_tables.json`,
   so the PR needs a governance approval; one that keeps the tables doesn't.
3. **Check, PR, merge, [promote](#promote-to-prod).**

Agent B's unfinished UI edits stay in dev: capture is per agent, and prod B
keeps its last captured version.

## Promote to prod

1. **Promote.** `make promote-to ENV=prod`. It rewrites
   `envs/prod/agents/<agent>.space.json` from the dev capture (catalogs and the
   warehouse remapped), and prints the changed files by owner. The first time,
   it also seeds `envs/prod/agents/<agent>.auto.tfvars` with dev's `acl_groups`
   and an empty `genie_space_id` (prod's ID lives in prod's state and release
   receipt). Later promotions keep prod's file and print how its `acl_groups`
   differ from dev's. A detached prod file stays detached. If that file was
   paired with the governance-owned `prod:detached-agent:<key>` tombstone in
   `ack.txt` while its capture is removed. Reactivation requires removing that
   ack line in a governance-reviewed PR.
2. **Check and open the promotion PR.** `make check ENV=prod`; commit
   `envs/prod/`. It can carry the dev change too, in one PR.
3. **Merge.** The operator runs `make release-plan ENV=prod`. When only
   `envs/prod/` changed it reuses the source env's (`promote_from`, dev by
   default) earlier rehearsal; when the PR also changed the source env, it
   names `make rehearse` to run on the merge commit first. At a terminal
   `make release ENV=prod` does that round by round, asking `y/N` per
   manifest; with a separate approver, get the manifest approved and run
   `make release ENV=prod MANIFEST=<path>` ([deployment machine](../deployment-machine.md#release-prod)).
   **Pass looks like:** `ALL EFFECTIVE` and `=== Release complete (prod) ===`.

Every release replaces the prod agent's config with the capture in git and
prints a field-level report of what it overwrote. It never recreates the agent
or changes its ID, so conversations stay. Never edit a prod agent in the UI.

## Change who can run an agent in prod

Edit `acl_groups` in `envs/prod/agents/<agent>.auto.tfvars`: prod owns it and
`promote-to` never overwrites it. If a new group can't read the agent's tables,
governance adds it to `table_readers` in the same PR. Then PR, merge,
`release-plan` (a prod-only change: it reuses the source env's rehearsal, so
no rehearse is needed) and `release`.

## Remove an agent

The agent and its conversations stay; only `CAN_RUN` goes.

```bash
# 1. PR: in envs/prod/agents/<agent>.auto.tfvars (and the same in envs/dev/agents/)
#      acl_groups = []
#      detached   = true
#    merge, rehearse dev on the merge commit, release-plan, release  ->  CAN_RUN revoked
# 2. the operator, at that released commit:
make detach-agent ENV=prod SPACE="<agent>"   # forgets it from Terraform state; deletes nothing
```

- **Pass looks like:** `Detached <agent>: N state address(es) removed; the
  agent and its conversations stay.`
- Table access is unchanged, because it belongs to `table_readers`. Masks stay
  until governance runs `make ungovern`.
- **Removal PR:** add `prod:detached-agent:<key>` to governance-owned `ack.txt`,
  keep the detached `.auto.tfvars`, and remove only `.space.json`. Later
  run `make promote-to ENV=prod` and commit the changed
  `generated/governed_tables.json`; promotions then keep that removal stable. To revive, remove the ack line in a
  governance-reviewed PR; an edit or deletion only under `agents/` is refused.
- `make detach-agent` needs a release receipt, so it runs in prod, not dev.
- **To delete the agent instead** (and its conversations): set `delete = true`
  and add the committed line `prod:delete-agent:<key>` to `envs/prod/ack.txt`
  (governance-owned), in one PR.

## If it stops

| Message | What to do |
|---|---|
| a group in `acl_groups` can't read one of the agent's tables | governance adds it to `table_readers` (an exact entry, or `catalog.schema.*`), in your PR |
| the agent has no `acl_groups` | set it; `[]` means nobody |
| `make generate` stops: no `class.*` tags yet | classification hasn't finished for a new table; wait for the scan and auto-tagging in Catalog Explorer, re-run |
| `make check` says `governed_tables.json` differs | run `make capture` (or `make generate ENV=dev`) again and commit the result |
| `not captured in dev yet` during `promote-to` | `make capture ENV=dev SPACE="<agent>"` first |
| a field the Genie API rejects fails the release | fix it in the dev UI, capture, promote again; nothing is skipped silently |

> **Optional AI helper.** `make draft-genie-content ENV=dev SPACE=<agent>`
> drafts instructions, sample questions and benchmarks for an agent that has
> none, under `envs/dev/drafts/`, for you to review and commit. It's dev-only,
> sends the model only schema, and never runs in CI or prod.
