# One team doing both

A small or medium team that curates agents and owns governance makes both kinds
of change, often in one PR. Same commands as the [agents](agents.md) and
[governance](governance.md) playbooks; one team owns both sets of files.

## The whole flow

```bash
# dev: curate agent A in the Genie UI and edit envs/dev/env.auto.tfvars, then
make capture ENV=dev SPACE="Agent A"   # A's config to git; nothing else
make generate ENV=dev                  # governance from the tags and config; review envs/dev/generated/
make check ENV=dev
make promote-to ENV=prod               # same PR: dev change and its promotion together
make check ENV=prod
git add envs/ && git commit            # one PR; after merge the dev pipeline rehearses it
# on the deployment machine, after rehearse passes on the merge commit:
make release ENV=prod                  # at a terminal: per round, plans, shows the manifest, asks y/N, applies it
#   separate approver: per round make release-plan ENV=prod, approve, make release ENV=prod MANIFEST=<path>
```

On a PR branch, `make generate` and `make promote-to` don't offer to commit
(the offer appears only on `main`, at a terminal): commit yourself, as above.
`make check` is the PR check; `make rehearse` and `make release-plan` run it
again themselves.

1. **Curate and edit.** Change agent A in the dev UI and governance in
   `envs/dev/env.auto.tfvars`. Agent B's unfinished edits stay in dev.
2. **Capture and generate.** `make capture ENV=dev SPACE="Agent A"`, then
   `make generate ENV=dev`. Review the diff in `envs/dev/agents/` and
   `envs/dev/generated/`.
3. **Promote in the same PR.** `make promote-to ENV=prod`. It prints the changed
   files by owner.
4. **Check both envs.** **Pass looks like:** `=== make check ENV=dev: passed ===`
   and `=== make check ENV=prod: passed ===`.
5. **Merge.** The dev pipeline (or the operator, until dev has remote state)
   runs `make rehearse ENV=dev` on the merge commit. **Pass looks like:**
   `ALL EFFECTIVE`.
6. **Release.** On the [deployment machine](../deployment-machine.md#release-prod):
   `make release ENV=prod` at a terminal (a `y/N` per manifest), or with a
   separate approver `make release-plan ENV=prod`, approve the manifest,
   `make release ENV=prod MANIFEST=<path>`.
   **Pass looks like:** `ALL EFFECTIVE` and `=== Release complete (prod) ===`.
   If A now uses a new table, its masks are applied before anyone gets access.

Prefer smaller PRs? Merge the dev change first, then open the promotion PR from
the new `main`. Both ways reach prod through the same stages.

## The rules

1. **Prod is code only.** Never edit a prod agent, mask or grant by hand. Every
   release puts the prod agent back to what's in git; remove `CAN_EDIT` on prod
   agents from everyone except the deployer SP.
2. **Dev is where you curate.** GenieRails never overwrites a dev agent's
   config.
3. **Capture an agent when it's ready.** Unfinished UI work stays in dev.
4. **Every prod change is a PR.** A promotion PR is a PR whose diff includes
   `promote-to` output. Prod-only changes (`acl_groups`, `table_readers`,
   `ack.txt` in `envs/prod`) follow the same stages.
5. **Never hand-merge generated files.** On a conflict, run `make generate
   ENV=dev` (and `make promote-to ENV=prod`) on the latest `main` and commit.

## Common cases

| You want to | Do this |
|---|---|
| Add an agent | its `agents/` file, `make generate ENV=dev`, review the proposed `table_readers`; [details](agents.md#add-an-agent) |
| Give another group an agent in prod | edit `acl_groups` in `envs/prod/agents/<agent>.auto.tfvars`, and `table_readers` if needed; PR, merge, `release-plan` (a prod-only change: it reuses dev's rehearsal), `release` |
| Remove an agent | `acl_groups = []` and `detached = true`; PR, merge, rehearse dev on the merge commit, `release-plan`, `release`, then `make detach-agent`; [details](agents.md#remove-an-agent) |
| Remove a table from an agent | remove it in the dev UI, capture, generate, promote; PR, merge, rehearse dev on the merge commit, `release-plan`, `release`; masks stay |
| Change who can read a table | edit `table_readers`, in dev and promote, or in prod directly; [details](governance.md#who-can-read-a-table) |
| Change a mask | `treatment_versions` or `column_overrides`; [details](governance.md#change-a-mask) |
| Map a new class | `make scaffold-treatments ENV=dev TREATMENT=<name>`; [details](governance.md#map-a-new-class) |
| Stop governing a table | `make ungovern`, deliberately; [details](governance.md#stop-governing-a-table) |
| Roll back | one revert PR covering the change and its promotion, with `prod:rollback:<commit>` in `envs/prod/ack.txt`; masks switch back in place |
| Fix something urgently | the same path, prioritised; no hand edits in prod |
| Upgrade GenieRails | a governance change; [details](governance.md#upgrade-genierails) |
| Split into two teams later | turn on the [CODEOWNERS split](../pipelines.md#branch-protection-and-approvals) |

## If it stops

Each command prints what to do. The ones you hit most:

| Message | What to do |
|---|---|
| `make check ENV=prod` says the promoted output differs | run `make promote-to ENV=prod` again and commit |
| a group in `acl_groups` can't read one of the agent's tables | add it to `table_readers` in the same PR |
| coverage check, or an unmapped class | [map the class](governance.md#map-a-new-class) in dev and promote |
| `release-plan` stops on a reader or weakening | [acknowledge it](governance.md#acknowledge-an-exception) in a PR, or fix the config |
| no passing receipt for HEAD | wait for the dev rehearsal on that commit |
