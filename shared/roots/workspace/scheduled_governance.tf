# ─────────────────────────────────────────────────────────────────────────────
# Scheduled steady-state governance job (roadmap: scheduled prod re-scan)
# ─────────────────────────────────────────────────────────────────────────────
# Opt-in Databricks Job that delegates to `make maintain ENV=<env>` on a cron.
# This is the native-first dev-to-prod steady state, including its per-env lock,
# gates, governance-only apply, audit, and certification receipt renewal. The
# wrapper retains the old audit -> generate-delta -> coverage flow as an
# explicitly selected legacy mode, but the shipped job does not use it.
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
  description = "Opt-in: when true, create a scheduled Databricks Job that runs the native-first make maintain flow on a cron. Default false keeps existing apply behavior unchanged."
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
  description = "Legacy-only: optional catalog threaded to generate-delta when the wrapper is run with --step all. The shipped native-first job ignores it."
}

variable "scheduled_governance_config_source" {
  type        = string
  default     = ""
  description = "Runtime-visible path the job copies the env config from before scanning (a Unity Catalog Volume like /Volumes/<cat>/<schema>/<vol>/prod, workspace files, or a DBFS mount). Required when enabled: the repo's envs/ is .gitignore'd, so the Git checkout does NOT contain envs/<env>/. The source must hold that env's auth.auto.tfvars, env.auto.tfvars, data_access/ and (optionally) generated/."

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
  description = "PyPI packages installed into the serverless environment so the native-first maintain steps can read config and query the workspace."
}

variable "scheduled_governance_notification_emails" {
  type        = list(string)
  default     = []
  description = "Email addresses notified when the scheduled governance job fails (e.g. drift detected but delta could not resolve it). Empty = no email notifications."
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

  # One task delegates to make maintain. Its exit code and native-classification
  # remediation hint pass through to the job output and failure notifications.
  task {
    task_key        = "steady_state_governance"
    environment_key = "governance"

    spark_python_task {
      python_file = "shared/scripts/run_scheduled_governance.py"
      source      = "GIT"
      parameters = concat(
        ["--env-dir", local.scheduled_governance_env_dir, "--step", "maintain"],
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
