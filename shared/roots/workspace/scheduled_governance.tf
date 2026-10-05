# ─────────────────────────────────────────────────────────────────────────────
# Scheduled steady-state governance job (roadmap: scheduled prod re-scan)
# ─────────────────────────────────────────────────────────────────────────────
# Opt-in Databricks Job that runs read-only native-first schema and rulebook
# audits on a cron. It never invokes make or Terraform and never writes to the
# workspace. The old audit -> generate-delta -> coverage flow remains available
# only through the explicit legacy mode.
#
# The job is DISABLED by default (enable_scheduled_governance = false) so adding
# this file changes nothing about `terraform apply` for existing environments.
# Enable it per environment by setting `enable_scheduled_governance = true`
# (see docs/scheduled-governance.md and scheduled_governance.auto.tfvars.example).
#
# This file is auto-loaded as part of the workspace root — no change to main.tf
# is required. The resource uses the workspace-scoped provider alias.
# ─────────────────────────────────────────────────────────────────────────────

# ── Opt-in + configuration variables (all default to the disabled, no-op path) ─

variable "enable_scheduled_governance" {
  type        = bool
  default     = false
  description = "Opt-in: when true, create a scheduled Databricks Job that runs read-only native-first governance checks on a cron. Default false keeps existing apply behavior unchanged."
}

variable "scheduled_governance_mode" {
  type        = string
  default     = "check"
  description = "Scheduled flow: check runs read-only schema/rulebook audits; legacy runs audit -> generate-delta -> coverage and may rewrite checkout files."

  validation {
    condition     = contains(["check", "legacy"], var.scheduled_governance_mode)
    error_message = "scheduled_governance_mode must be \"check\" or \"legacy\"."
  }
}

variable "scheduled_governance_env" {
  type        = string
  default     = "prod"
  description = "Name of the environment the scheduled scan targets (informational; surfaced in the job name and passed to the tasks as a parameter). Defaults to prod, the intended steady-state target."
}

variable "scheduled_governance_cloud" {
  type        = string
  default     = "aws"
  description = "Cloud subdirectory in the repo that holds envs/ (aws or azure). Used to build the repo-relative env directory the tasks run from."

  validation {
    condition     = contains(["aws", "azure"], var.scheduled_governance_cloud)
    error_message = "scheduled_governance_cloud must be \"aws\" or \"azure\"."
  }
}

variable "scheduled_governance_catalog" {
  type        = string
  default     = ""
  description = "Legacy-only: optional catalog threaded to generate-delta. Ignored in the default check mode."
}

variable "scheduled_governance_config_source" {
  type        = string
  default     = ""
  description = "Runtime-visible config source copied before scanning. Prefer an envs root with account/ and <env>/ to run both checks; an old flat target-env source remains compatible but check mode warns and skips rulebook audit. Required when enabled because envs/ is gitignored."

  validation {
    condition     = !var.enable_scheduled_governance || var.scheduled_governance_config_source != ""
    error_message = "scheduled_governance_config_source must be set when enable_scheduled_governance = true — the Git checkout does not contain the .gitignore'd envs/<env>/ config."
  }
}

variable "scheduled_governance_cron" {
  type        = string
  default     = "0 0 6 * * ?"
  description = "Quartz cron expression for the scan cadence. Default is daily at 06:00. Example (weekly, Mondays 06:00): \"0 0 6 ? * MON\"."
}

variable "scheduled_governance_timezone" {
  type        = string
  default     = "UTC"
  description = "IANA timezone id the cron schedule is evaluated in (e.g. UTC, America/Los_Angeles)."
}

variable "scheduled_governance_git_url" {
  type        = string
  default     = ""
  description = "HTTPS URL of the Git repo the job checks out to run the steady-state scripts (e.g. https://github.com/<org>/<repo>). Required when enable_scheduled_governance = true."

  validation {
    condition     = !var.enable_scheduled_governance || var.scheduled_governance_git_url != ""
    error_message = "scheduled_governance_git_url must be set when enable_scheduled_governance = true."
  }
}

variable "scheduled_governance_git_branch" {
  type        = string
  default     = "main"
  description = "Git branch the job checks out for the scheduled scan."
}

variable "scheduled_governance_git_provider" {
  type        = string
  default     = "gitHub"
  description = "Git provider hosting the repo (gitHub, gitLab, bitbucketCloud, azureDevOpsServices, ...), as understood by the Databricks Jobs Git source."
}

variable "scheduled_governance_serverless_client" {
  type        = string
  default     = "2"
  description = "Serverless environment client version used by the job tasks."
}

variable "scheduled_governance_dependencies" {
  type = list(string)
  default = [
    "python-hcl2",
    "databricks-sdk",
    "pyyaml",
    "requests",
  ]
  description = "PyPI packages installed into the serverless environment so the read-only checks can read config and query the workspace."
}

variable "scheduled_governance_notification_emails" {
  type        = list(string)
  default     = []
  description = "Email addresses notified when the job fails. Notifications link to the run output containing finding details. Empty = no email notifications."
}

variable "scheduled_governance_job_name" {
  type        = string
  default     = ""
  description = "Override the generated job name. Empty = \"GenieRails steady-state governance (<env>)\"."
}

# ── Job definition ─────────────────────────────────────────────────────────────

locals {
  scheduled_governance_job_name = (
    var.scheduled_governance_job_name != ""
    ? var.scheduled_governance_job_name
    : "GenieRails steady-state governance (${var.scheduled_governance_env})"
  )

  # Working directory the steady-state scripts expect: the target env directory,
  # relative to the checked-out repo root. Matches how `make audit-schema
  # ENV=<env>` runs the scripts from envs/<env>/.
  scheduled_governance_env_dir = "${var.scheduled_governance_cloud}/envs/${var.scheduled_governance_env}"
}

resource "databricks_job" "scheduled_governance" {
  count    = var.enable_scheduled_governance ? 1 : 0
  provider = databricks.workspace

  name                = local.scheduled_governance_job_name
  max_concurrent_runs = 1

  # Check out this repo so the tasks can invoke the existing entrypoints. The
  # checkout provides the CODE only — the repo's envs/ is .gitignore'd, so the
  # per-env config (auth/env/data_access/generated tfvars) is NOT present. The
  # wrapper materializes it from scheduled_governance_config_source (a UC Volume
  # / workspace path) into the env dir before scanning.
  git_source {
    url      = var.scheduled_governance_git_url
    provider = var.scheduled_governance_git_provider
    branch   = var.scheduled_governance_git_branch
  }

  # Serverless compute for the Python task — no cloud-specific node types, so
  # this stays portable across the aws/ and azure/ roots. The steady-state
  # scripts need these packages to read config (python-hcl2) and query the
  # workspace (databricks-sdk); without them the audit would silently find no
  # managed tables instead of auditing.
  environment {
    environment_key = "governance"
    spec {
      client       = var.scheduled_governance_serverless_client
      dependencies = var.scheduled_governance_dependencies
    }
  }

  # The default check mode is serverless-safe and read-only. Legacy mode is
  # retained for explicit compatibility and may rewrite files in its checkout.
  task {
    task_key        = "steady_state_governance"
    environment_key = "governance"

    spark_python_task {
      python_file = "shared/scripts/run_scheduled_governance.py"
      source      = "GIT"
      parameters = concat(
        ["--env-dir", local.scheduled_governance_env_dir, "--step", var.scheduled_governance_mode == "check" ? "check" : "all"],
        var.scheduled_governance_catalog != "" ? ["--catalog", var.scheduled_governance_catalog] : [],
        var.scheduled_governance_config_source != "" ? ["--config-source", var.scheduled_governance_config_source] : [],
      )
    }
  }

  schedule {
    quartz_cron_expression = var.scheduled_governance_cron
    timezone_id            = var.scheduled_governance_timezone
    pause_status           = "UNPAUSED"
  }

  dynamic "email_notifications" {
    for_each = length(var.scheduled_governance_notification_emails) > 0 ? [1] : []
    content {
      on_failure = var.scheduled_governance_notification_emails
    }
  }

  tags = {
    project   = "genierails"
    component = "steady-state-governance"
    env       = var.scheduled_governance_env
  }
}

output "scheduled_governance_job_id" {
  description = "ID of the scheduled steady-state governance job (null when disabled)."
  value       = var.enable_scheduled_governance ? databricks_job.scheduled_governance[0].id : null
}

output "scheduled_governance_job_url" {
  description = "Workspace URL of the scheduled steady-state governance job (null when disabled)."
  value       = var.enable_scheduled_governance ? "${var.databricks_workspace_host}/jobs/${databricks_job.scheduled_governance[0].id}" : null
}
