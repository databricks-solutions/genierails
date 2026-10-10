# Running GenieRails in production

> **Draft.** Describes the deterministic-governance release (design: [PR #107](https://github.com/databricks-solutions/genierails/pull/107)). Commands such as `make capture` don't exist until that release ships.

After the [first deployment](../examples/dev_to_prod/README.md), you keep changing two things: **agents** and **governance**. They move to prod independently. Git is the record of what's ready.

| | Agent change | Governance change |
|---|---|---|
| What | one agent's instructions, examples, SQL, tables, `acl_groups` | masks, tiers, row filters, mappings for new classes |
| Belongs to | that agent | tables (the same for every agent) |
| You change it in | the **dev Genie UI**, then `make capture` | files in git |

## The rules

1. **Prod is code-only.** Never edit a prod agent, mask or grant by hand. Every `release` puts the prod agent back to what's in git.
2. **Dev is where you curate.** GenieRails never overwrites a dev agent's config.
3. **Capture an agent when it's ready.** Unfinished UI work stays in dev until you capture it.
4. **Every prod change is a promotion PR.** Someone approves it, then the operator runs `release` from the deployment machine.

## Change an agent

```bash
# curate in the dev Genie UI, then:
make capture ENV=dev SPACE="Agent A"   # writes Agent A's config to git; nothing else
make rehearse ENV=dev                  # proves the masks and access in dev
# commit, open a PR, merge
make promote-to ENV=prod               # opens the promotion PR: only Agent A changes
# after approval, on the deployment machine:
make release ENV=prod
```

Agent B's unfinished edits stay in dev. Prod B keeps its last captured version. If Agent A now uses a new table, that table's masks appear in the same promotion PR and are applied before anyone gets access.

## Change governance

```bash
# edit the mask library, tiers, row filters or a class mapping in git
make rehearse ENV=dev
# commit, open a PR, merge
make promote-to ENV=prod               # the promotion PR shows only mask/policy changes
make release ENV=prod
```

The change applies to every table and agent that uses it. No agent's config changes, so a mask fix never waits for an agent that's mid-curation.

## Keep prod protected

```bash
make maintain ENV=prod                 # on a schedule; governance only
```

When prod's scan tags a new sensitive column, `maintain` masks it. If the class has no mapping yet, it stops and tells you what to add in git.

## Working as a team

Agent owners and the governance team share one repo. Each edits their own files.

1. **Open a PR.** CI runs read-only checks and `make plan ENV=dev`. Nothing touches dev.
2. **Merge.** `make rehearse ENV=dev` runs on the new `main`.
3. **Promote only a rehearsed `main`.** `promote-to` and `release` refuse anything that hasn't passed rehearse.

If two PRs conflict on generated files, don't merge them by hand: run `make generate ENV=dev` on the latest `main` and commit.

## Common cases

| You want to | Do this |
|---|---|
| Add a new agent | Add it to `genie_spaces` with its `acl_groups`, `make generate ENV=dev`, rehearse, promote, release |
| Give another group an agent in prod | Edit that agent's `acl_groups` in `envs/prod`, PR, release |
| Remove an agent | Delete its entry in dev, promote, release. Prod access is revoked and the agent is kept; set `delete = true` to delete it |
| Remove a table from an agent | Remove it in the dev UI, capture, promote, release. Masks stay |
| Stop governing a table | `make ungovern`, deliberately. Removing it from agents never drops its masks |
| Roll back | Revert the commit, release. Masks switch back in place, with no gap |
| Fix something urgently | Same path, prioritised. No hand edits in prod |
| Upgrade GenieRails | Treat it as a governance change: upgrade in dev, rehearse, promote, release |
| Several teams | Each agent has its own file, so code owners can require each team to approve its own agent |

## What to commit

Commit `envs/` (config, captured agents, generated rules and prod agent IDs). Never commit `auth.auto.tfvars` or Terraform state.

## Limits in this version

- **Prod releases run from one deployment machine.** CI only runs checks and the post-merge dev rehearse (once dev has remote state; until then the operator runs it).
- **Prod UI edits aren't reported.** They're simply overwritten on the next release.
