# Deterministic mask library reference

`mask_library.json` maps all 93 Databricks `class.*` classifications to fixed,
caller-independent partial and full versions.

Treatment names are the shared `treatment_versions` vocabulary, so each one
can be configured on its own (for example `ssn = { partial = "last4" }`).

| Treatment | Classes | Partial | Full | Types |
|---|---:|---|---|---|
| `card_last4` | 1 | last 4 | redacted | STRING |
| `card_security_code`, `card_pin`, `card_track_data` | 1 each | redacted (never raw) | redacted | STRING |
| `account_last4` | 5 | last 4 | redacted | STRING |
| `ssn`, `tfn_partial`, `medicare_partial`, `aadhaar_partial` | 2, 1, 1, 1 | redacted (`last4` selectable) | redacted | STRING, numeric |
| `identifier` (other government, health, vehicle and device IDs) | 53 | redacted | redacted | STRING, numeric |
| `email_partial` / `phone_partial` / `name_partial` | 1 each | email partial / last 4 / initials | redacted | STRING |
| `date_year` | 2 | year | NULL | DATE, TIMESTAMP, TIMESTAMP_NTZ |
| `age` / `credit_score` / `compensation_redacted` | 1 each | 10-point band / 50-point band / rounded | NULL | numeric |
| `ip_address` / `mac_address` / `url` | 1 each | IPv4 /24 network (IPv6 redacted) / vendor / domain | redacted | STRING |
| `location` | 1 | STRING redacted; numeric 1 decimal place | typed redacted | STRING, numeric |
| `redact` (card expiry and sensitive attributes) | 13 | redacted | redacted | STRING, DATE, TIMESTAMP, TIMESTAMP_NTZ |
| `secret` | 1 | redacted (never raw) | redacted | all supported types |

`card_security_code`, `card_pin`, `card_track_data`, and `secret` are never raw:
their classes map to treatments of the same names, and a column carrying any
of them gets the full version for every principal, including tier 1,
`raw_exempt_principals` and the deployer SP, even when another class tag wins
strictest-wins (`resolve_column_access` carries that flag into access). When several class tags occur on one column, the strongest
partial treatment wins (`raw < partial < redacted/NULL`); a tie goes to the
greater treatment name, so tag order never matters. The exception is a string
column whose classes keep two or more different partial treatments, such as a
free-text note tagged `email_address`, `name` and `phone_number`: no one
partial format is safe for all of them (a last-4 of the note leaks its length
and its last characters), so the column gets `redact` and every masked tier
sees `[REDACTED]`, even beside a redacted class. `column_overrides` and
`treatment_versions` can't change that: an override to anything but `redact`
or a never-raw treatment is not applied, and generate and the live refresh
print a warning naming the column. The coverage check reads each column's
live `class.*` tags from the live refresh and fails if a mixed column's
`gr_treatment` is anything but `redact` or never raw; it also checks the
`gr.classification_mixed_partial` markers in the generated rules. Several classes of one treatment
(`bank_number` and `iban_code`) keep that treatment. Unsupported types receive
the full version.

No mask can fail a query. Numeric masks read the column only through
`try_cast(value AS DECIMAL(38, 18))`, which is NULL for NaN, infinity and
values of 1E20 or more, and the final typed `TRY_CAST` is NULL when the
result does not fit the column type (for example a rounded `BIGINT` maximum).

A single class has no such conflict signal. For example, `notes` tagged only
`class.phone_number` still receives the phone partial mask, so a partial tier
can see the note's last four characters and infer its length. `make generate`
warns, and `make coverage-gate` emits a non-blocking advisory, when a STRING
column's name or comment suggests free text but its resolved treatment keeps a
partial version. Names/comments are only a review heuristic and never change
the generated treatment. To redact one column, set:

```hcl
column_overrides = {
  "catalog.schema.table.notes" = { treatment = "redact" }
}
```

To make every column using a treatment redact at the partial tier, use
`treatment_versions = { phone_partial = { partial = "redacted" } }`.

The keyed hash (`hmac_sha256`) is deferred from v1: identifiers are redacted for
tiers 2 and 3, and `treatment_versions` or `column_overrides` naming
`hmac_sha256` is refused with "keyed hash is not available in this version".

`scripts/live_mask_library.py` checks every shipped SQL body against exact
expected values on a warehouse (`DATABRICKS_LIVE_TESTS=1`); offline tests check
the Python reference against the same values. Results are rendered in
`America/Los_Angeles` per statement, because the Statement Execution API does
not keep session settings between calls. The run is read-only: it creates no
schema, function or secret.

## Customer classes: `tag_treatments`

A class the library doesn't ship (for example a custom class from Data
Classification) is unmapped: its columns get the fail-safe never-raw
redaction and new access is blocked until it is mapped. Map it in the env's
own `env.auto.tfvars`, never in `shared/` (GenieRails product code, replaced
on upgrade):

```hcl
tag_treatments = {
  "class.loyalty_id" = "identifier"   # any mask-library treatment, e.g. redact
}
```

`make scaffold-treatments ENV=<env> TREATMENT=<treatment>` writes the entry
for every unmapped class it finds (in the env's generated rules, a stopped
release's runtime overlay, or live). For a promote-to target it writes the
env it is promoted from, and `make promote-to` carries the setting. Keys are
`class.<name>`, lowercase as the `class.*` tag names it. Refused: a class the
library already maps (change one column with `column_overrides`), a value
that is not a library treatment, and keys for tags other than `class.*`,
which this version does not read (the mapping would mask nothing).

## Optional AI helpers

Three dev-only commands ask a model for a draft; a person reviews it and
commits it like any change (design 18.4, decision 5). They are the only
commands that call a model. `generate`, `rehearse`, `release`, `maintain` and
`check` never do, and a test fails if any of them reaches the model client.

| Command | Writes (only) |
|---|---|
| `make suggest-treatment ENV=dev CLASS=<class>` | `envs/dev/drafts/treatments/<class>.json`: a mask-library treatment for a class nothing maps yet, with the library's partial and full versions, or a pointer to `draft-custom-mask` when none fits |
| `make draft-genie-content ENV=dev SPACE="<agent>"` | `envs/dev/drafts/genie/<agent>.json`: instructions, sample questions and benchmarks the captured agent doesn't have yet. The model sees only table names, column types and which parts already exist; never data, the agent's name or its existing text |
| `make draft-custom-mask ENV=dev TREATMENT=custom_<name> TYPE=<sql-type>` | `envs/dev/drafts/custom_masks/custom_<name>.sql` (one expression over `value`) and `custom_<name>.json` (exact-output fixture, always with a NULL case). The name must carry the reserved `custom_` prefix |
| `make test-custom-mask ENV=dev TREATMENT=custom_<name>` | `envs/dev/drafts/custom_masks/custom_<name>.passed.json`, only on a pass. Calls no model |
| `make adopt-custom-mask ENV=dev TREATMENT=custom_<name>` | `envs/dev/custom_masks/custom_<name>.json`: governance config (see [Adopted custom masks](#adopted-custom-masks)). Calls no model |

- **Model endpoint:** `ai_helper_endpoint` in the env's `env.auto.tfvars` (a
  Databricks model serving endpoint, called with the env's workspace
  credentials). Without it the drafting commands stop with an error naming
  the setting. Promotion never carries it.
- **Where they run:** only `ENV=dev`, whose physical path must be this
  checkout's `<cloud>/envs/dev` (no `ENV_DIR` or `CLOUD_ROOT` redirection, no
  symlinked `envs/` or `envs/dev`, and no `promote_from`). Never in CI: `CI`
  (`true`, `1` or `yes`, from the environment, the command line or `make -e`)
  or any of `GITHUB_ACTIONS`, `TF_BUILD`, `BUILD_BUILDID`, `JENKINS_URL`,
  `GITLAB_CI`, `BUILDKITE`, `CIRCLECI`, `TEAMCITY_VERSION`, `TRAVIS`,
  `BITBUCKET_BUILD_NUMBER`, `CODEBUILD_BUILD_ID`, `DRONE`, `SEMAPHORE` or
  `APPVEYOR` set; `GENIERAILS_ALLOW_CI_APPLY` does not lift this.
- **Files:** they read, write and remove only real files: a symlink in any
  path component under `envs/dev` (including `drafts/` itself, a folder in it,
  a draft, or the agent capture they read) is refused. Drafts are staged
  beside their destination, created exclusively without following links, and
  moved into place only after every file is staged.
- **Drafts are never read.** Nothing in GenieRails reads `drafts/`: the
  rehearse fingerprint and `make check`'s scratch copy skip it. A draft takes
  effect only when a person copies it into config in a reviewed PR: a
  treatment into `tag_treatments`, Genie content into the dev Genie UI and
  then `make capture`. A changed draft is replaced only with `FORCE=1`.
- **Custom masks:** `test-custom-mask` runs the mask library's exact-output
  rules on the draft. Offline: one caller-independent, deterministic
  ASCII expression over `value` (no query, statement, `current_user`, group
  check, randomness, current time, `secret`, `http_request`, `ai_*`,
  `IDENTIFIER`, `read_files` or reflection), and a fixture with NULL in giving
  NULL out plus at least two non-null cases. Every fixture input must be one
  typed literal (`NULL`, a number, `'text'`, `DATE'…'`, `TIMESTAMP'…'`,
  `TIMESTAMP_NTZ'…'`, or `CAST` of a number, text or `NULL` to a scalar
  type), so a fixture can never run SQL; the reviewed mask is the only SQL
  that runs. Then every case on the env's
  `sql_warehouse_id`, read-only, wrapped in the library's typed cast and
  rendered in `America/Los_Angeles` like `scripts/live_mask_library.py`. A
  pass records the SHA-256 of the `.sql` and `.json` it tested, so a reviewer
  can see the committed draft is the one that passed; a failing run deletes
  the record. `tag_treatments` can't reference a draft: only a mask adopted
  with `make adopt-custom-mask` (below).

### Adopted custom masks

`make adopt-custom-mask ENV=dev TREATMENT=custom_<name>` (design 18.4.1)
turns a draft that passed `test-custom-mask` into governance config. It runs
under the same guards as the AI helpers (never in CI, with no opt-out; only
`ENV=dev` at this checkout's real `envs/dev`; no symlinks) and calls no
model. It refuses unless:

- the name has the reserved `custom_` prefix, no `__`, and isn't a mask-library name;
- the `test-custom-mask` pass record matches the draft's `.sql` and `.json` bytes;
- the offline rules pass again (one expression over `value`, no query,
  `current_user`, group check, randomness or time; NULL in gives NULL out; at
  least two non-null cases, all typed literals);
- no never-raw class (`secret`, `card_pin`, ...) maps to it in `tag_treatments`;
- an adopted type family is replaced only with `FORCE=1`.

It writes `envs/dev/custom_masks/custom_<name>.json` and only prints the
`tag_treatments` line to add; it never edits `env.auto.tfvars` or deletes
drafts. The file holds, per type family, the partial `sql`, its
`tested_type`, the literal-only `cases` and `tested_sha256` (the SHA-256 of
`[treatment, tested_type, sql, cases]`). `full` is always the library's
`redacted` (`[REDACTED]`, or NULL for other types), so tier 3 and anyone
outside the tiers never meet custom SQL. `never_raw` must be `false`:
tier 1, `raw_exempt_principals` and the deployer see a custom-masked column
raw, so map a class that must never be raw to `secret` instead.

- **One lookup:** `generate`, `validate`, `scaffold-treatments` and
  `verify-access` all read the mask library plus the env's `custom_masks/`.
  The partial SQL is part of the generation hash, so an edit gives new tag
  values, functions and policies through the usual staged change.
  `treatment_versions` can only make a custom mask stricter (`redacted`).
- **validate and check** recompute `tested_sha256` and refuse a mismatch, so
  hand-edited SQL can't govern until it is re-tested and re-adopted.
- **rehearse and release** (`release-plan` and `release MANIFEST=`) re-run
  every adopted case, read-only, on that env's `sql_warehouse_id`, before
  anything is applied.
- **Weakening:** a partial tier moving to a different custom value (a library
  mask swapped for a custom one, one custom mask for another, or an edit to a
  custom mask's SQL) is reported as `custom_mask_changed` and needs a
  `<env>:weaken:<column>:<principal>` acknowledgement. In a multi-round staged
  release, a verified earlier phase manifest keeps that exact `weaken` entry
  valid for the same policy-journal run and from/to generations until
  `Release complete`; it is stale again after completion, abandon, or a
  different run.
- **Promotion:** `make promote-to` copies `custom_masks/` byte for byte
  (removing files the source no longer has) and reports it as governance
  files. The account tag policy accepts any `custom_*` value, so
  `make apply-account` adds new values as usual.
