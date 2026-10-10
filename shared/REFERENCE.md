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
greater treatment name, so tag order never matters. Unsupported types receive
the full version.

No mask can fail a query. Numeric masks read the column only through
`try_cast(value AS DECIMAL(38, 18))`, which is NULL for NaN, infinity and
values of 1E20 or more, and the final typed `TRY_CAST` is NULL when the
result does not fit the column type (for example a rounded `BIGINT` maximum).

The keyed hash (`hmac_sha256`) is deferred from v1: identifiers are redacted for
tiers 2 and 3, and `treatment_versions` or `column_overrides` naming
`hmac_sha256` is refused with "keyed hash is not available in this version".

`scripts/live_mask_library.py` checks every shipped SQL body against exact
expected values on a warehouse (`DATABRICKS_LIVE_TESTS=1`); offline tests check
the Python reference against the same values. Results are rendered in
`America/Los_Angeles` per statement, because the Statement Execution API does
not keep session settings between calls. The run is read-only: it creates no
schema, function or secret.
