# Test helper: writes (or, for a null value, deletes) local files from a
# terraform test run, so later runs can plan against a gate result, DDL or
# state. Files are removed again when the test tears down.

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

resource "terraform_data" "file" {
  for_each = var.files

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
