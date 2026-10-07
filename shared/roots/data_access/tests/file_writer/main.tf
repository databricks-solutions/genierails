# Test helper: writes (or, for a null value, deletes) local files from a
# terraform test run, so later runs can plan against a gate result, DDL or
# state. Files are removed again when the test tears down. "@NOW@" in a
# content is replaced with this run's plan time (RFC 3339, UTC), for gate
# results whose live refresh must be recent without a fixed date going stale.

# Declared only so the test files' mock "databricks"/"time" providers resolve
# to the same provider types as in the root under test.
terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
    time = {
      source = "hashicorp/time"
    }
    null = {
      source = "hashicorp/null"
    }
  }
}
variable "files" {
  type        = map(string)
  description = "Path (relative to the root under test) => content; null deletes the file."
}

locals {
  now = formatdate("YYYY-MM-DD'T'hh:mm:ss'Z'", plantimestamp())
  files = {
    for path, content in var.files : path => content == null ? null : replace(content, "@NOW@", local.now)
  }
}

resource "terraform_data" "file" {
  for_each = local.files

  triggers_replace = { path = each.key, content = each.value }
  input            = each.key

  provisioner "local-exec" {
    command = each.value == null ? "rm -f \"$FILE_PATH\"" : "mkdir -p \"$(dirname \"$FILE_PATH\")\" && printf '%s' \"$FILE_CONTENT\" > \"$FILE_PATH\""
    environment = {
      FILE_PATH    = each.key
      FILE_CONTENT = each.value == null ? "" : each.value
    }
  }

  provisioner "local-exec" {
    when    = destroy
    command = "rm -f \"$FILE_PATH\""
    environment = {
      FILE_PATH = self.input
    }
  }
}
