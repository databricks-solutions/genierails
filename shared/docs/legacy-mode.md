# Legacy mode

Legacy mode is the older, AI-drafted way GenieRails governed tables: a model
drafted the masks and policies on `gr_treatment`. It exists only as something
to migrate from. New envs and every playbook use deterministic governance.

## Is my env legacy?

```bash
grep governance_mode envs/dev/env.auto.tfvars   # no line, or "legacy": legacy
```

An env without `governance_mode = "deterministic"` is legacy. Nothing changes
for it until you opt in: its plans show no change.

## Migrate

```bash
make migrate-plan ENV=prod     # read-only dry run: per column and principal, revocations, proposed table_readers
```

Then follow [legacy-migration.md](legacy-migration.md): opt in dev and prod,
rebuild the governed-table record, regenerate, review the revocation report,
and cut over through the normal stages. Once migrated, an env can't go back to
legacy.

## While you migrate

These commands and settings belong to legacy envs. Deterministic envs refuse
them or don't need them; the replacement is on the right.

<a id="legacy-commands"></a>
| Legacy | Deterministic replacement |
|---|---|
| `make generate SPACE="<agent>"` (re-draft one agent; refused in deterministic envs) | `make capture ENV=dev SPACE="<agent>"` ([agents](playbooks/agents.md#update-an-agent)) |
| `make generate MODE=governance`, `make apply-governance` / `MODE=genie`, `make apply-genie` (central governance, self-service Genie) | one repo, per-agent files and a [CODEOWNERS split](pipelines.md#who-owns-which-file) ([agents](playbooks/agents.md), [governance](playbooks/governance.md)) |
| `make generate COUNTRY=... INDUSTRY=...` ([country](country-overlays.md), [industry](industry-overlays.md) overlays tune the model's draft) | the fixed [mask library](../REFERENCE.md), `tag_treatments` for customer classes |
| `make generate-delta` | `make maintain ENV=prod` |
| `make release ENV=prod` (one step) | `make release ENV=prod` at a terminal (per round: plan, `y/N`, apply exactly that manifest); pipelines: `make release-plan ENV=prod`, then `make release ENV=prod MANIFEST=<path>` (releases the approved manifest) |
| `make promote SOURCE_ENV=... DEST_ENV=... DEST_CATALOG_MAP=...` (legacy destinations only; a deterministic one refuses it) | `make promote-to ENV=prod` |
| `make apply ENV=prod` | refused in a promoted deterministic env |
| `make scaffold-treatments` stubs in `shared/treatment_config.json` | `tag_treatments` in the env ([map a new class](playbooks/governance.md#map-a-new-class)) |
| layer files `envs/<env>/abac.auto.tfvars`, `data_access/` committed | runtime outputs, not committed ([what to commit](pipelines.md#what-to-commit)) |

<a id="standalone-terraform"></a>
## Standalone Terraform (legacy only)

A legacy env's split layer files are plain `.tfvars`, so Terraform can run them
directly, account layer first, then data_access, then workspace. You lose the
Makefile's layer waits, tag-policy visibility check, auto-import and
fingerprint skip. Deterministic envs refuse this side door: a promoted env
changes only through `release` (a manifest's saved plans) and `maintain`
([release gating](pipelines.md#release-gating-deterministic-envs)).

```bash
cd shared/roots/account
terraform init -backend-config="path=../../../envs/account/terraform.tfstate"
terraform apply -var-file="../../../envs/account/auth.auto.tfvars" -var-file="../../../envs/account/abac.auto.tfvars"
# then shared/roots/data_access and shared/roots/workspace with the env's files
```

<a id="genie-only"></a>
## Genie-only mode (legacy only)

`genie_only = true` lets a business team's SP manage only its agents, with
workspace `USER` and the Databricks SQL entitlement and no admin role. The
governance team grants it `USE CATALOG`, `USE SCHEMA` and `SELECT` on the
governed tables and `CAN USE` on an existing warehouse (`sql_warehouse_id` is
required). Account group lookups, entitlements and agent ACLs are skipped, and
`groups` must be `{}`. Run it with `make generate ENV=<env> MODE=genie` and
`make apply-genie ENV=<env>`; `make test-genie-only` covers it. In a
deterministic env an agent's tables are governed first, and agent owners work
through the [agents playbook](playbooks/agents.md).
