variable "manage_groups" {
  type        = bool
  default     = true
  description = "Account module must manage groups directly."

  validation {
    condition     = var.manage_groups
    error_message = "Account state requires manage_groups = true."
  }
}

variable "groups" {
  type = map(object({
    description = optional(string, "")
  }))
  description = "Map of group name -> config. Each key becomes an account-level Databricks group."
}

variable "group_members" {
  type        = map(list(string))
  default     = {}
  description = "Map of group name -> list of account-level user IDs."
}

variable "tag_policies" {
  type = list(object({
    key         = string
    description = optional(string, "")
    values      = list(string)
  }))
  default     = []
  description = "Account-scoped tag policy definitions, shared across all workspace environments."
}
