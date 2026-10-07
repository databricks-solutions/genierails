# Judges a coverage-gate result (envs/<env>/data_access/.coverage_gate.json,
# written by scripts/coverage_gate.py) for both the data_access layer (business
# SELECT) and the workspace layer (Genie CAN_RUN), so the two can't drift.
# Creates no resources.
#
# A result passes only if it records a pass for the expected fingerprint and a
# live refresh of tags and DDL (refreshed_at) no older than max_age at plan
# time. Terraform can't re-read Unity Catalog; this bounds how long a raw
# terraform run may rely on the last refresh. max_age is part of the
# data_access fingerprint, so changing it (-var, TF_VAR_) makes the result
# stale until the gate re-runs, and it can never exceed the ceiling below.

variable "gate_file" {
  type        = string
  description = "Path to the coverage-gate result."
}

variable "expected_fingerprint" {
  type        = string
  description = "Fingerprint the result must record: the data_access inputs now (data_access), or the ones the last data_access apply used (workspace)."
}

variable "max_age" {
  type        = string
  description = "Oldest live refresh the pass may rest on, as a Terraform duration, at most local.max_age_ceiling."
}

locals {
  # Hard ceiling: no variable can raise it.
  max_age_ceiling = "24h"
  # Tolerated clock skew for refresh times in the future.
  future_skew = "5m"
  _epoch      = "2000-01-01T00:00:00Z"

  max_age_valid = try(
    timecmp(timeadd(local._epoch, var.max_age), local._epoch) > 0
    && timecmp(timeadd(local._epoch, var.max_age), timeadd(local._epoch, local.max_age_ceiling)) <= 0,
    false
  )

  _result = fileexists(var.gate_file) ? try(jsondecode(file(var.gate_file)), null) : null

  # Everything but the refresh-time checks. plantimestamp() is unknown under
  # terraform console, so scripts/coverage_gate.py can-run-check reads this and
  # applies the two time checks below itself, from refreshed_at and fresh_until.
  static_status = (
    !fileexists(var.gate_file) ? "missing" :
    local._result == null ? "unreadable" :
    !local.max_age_valid ? "invalid_max_age" :
    try(local._result.fingerprint, "") != var.expected_fingerprint ? "stale" :
    try(local._result.status, "") != "pass" ? "failed" :
    "pass"
  )
  # "" when refreshed_at is missing or malformed.
  refreshed_at = try(timeadd(local._result.refreshed_at, "0s"), "")
  fresh_until  = try(timeadd(local._result.refreshed_at, var.max_age), "")

  status = (
    local.static_status != "pass" ? local.static_status :
    # A missing or malformed refreshed_at, or one in the future (beyond clock
    # skew), is not a live refresh.
    try(timecmp(local._result.refreshed_at, timeadd(plantimestamp(), local.future_skew)) > 0, true) ? "unrefreshed" :
    try(timecmp(timeadd(local._result.refreshed_at, var.max_age), plantimestamp()) < 0, true) ? "expired" :
    "pass"
  )

  problems = {
    missing         = "no coverage-gate result exists (${var.gate_file})"
    unreadable      = "the coverage-gate result is not valid JSON (${var.gate_file})"
    invalid_max_age = "coverage_gate_max_age ${jsonencode(var.max_age)} is not a positive duration of at most ${local.max_age_ceiling}"
    stale           = "the inputs changed after the coverage gate ran (config, tags, masks, DDL, grants, coverage_gate_max_age or -var overrides)"
    failed          = "the last coverage gate FAILED (see its report)"
    unrefreshed     = "the passing result records no live refresh of tags and DDL from Unity Catalog"
    expired         = "the live refresh of tags and DDL behind the pass is older than coverage_gate_max_age (${var.max_age})"
    pass            = ""
  }
}

output "status" {
  description = "missing, unreadable, invalid_max_age, stale, failed, unrefreshed, expired or pass."
  value       = local.status
}

output "problem" {
  description = "Why the result doesn't pass, or \"\"."
  value       = local.problems[local.status]
}

output "static_status" {
  description = "status without the refresh-time checks: missing, unreadable, invalid_max_age, stale, failed or pass. Known under terraform console."
  value       = local.static_status
}

output "static_problem" {
  description = "Why static_status doesn't pass, or \"\"."
  value       = local.problems[local.static_status]
}

output "refreshed_at" {
  description = "The result's live refresh time (RFC 3339), or \"\" if missing or malformed."
  value       = local.refreshed_at
}

output "fresh_until" {
  description = "refreshed_at + max_age (RFC 3339), or \"\"."
  value       = local.fresh_until
}

output "problems" {
  description = "Message for each status."
  value       = local.problems
}

output "max_age_ceiling" {
  description = "The hard ceiling on coverage_gate_max_age."
  value       = local.max_age_ceiling
}
