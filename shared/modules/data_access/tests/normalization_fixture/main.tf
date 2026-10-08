terraform {
  required_providers {
    external = { source = "hashicorp/external", version = "~> 2.3" }
  }
}

variable "masking_sql_file" { type = string }
variable "legacy_raw_hash" {
  type    = bool
  default = false
}

data "external" "normalized" {
  program = ["python3", "${path.module}/../../normalize_masking_sql.py"]
  query   = { sql_file = var.masking_sql_file }
}

output "hash" { value = data.external.normalized.result.hash }

resource "terraform_data" "masking" {
  triggers_replace = {
    sql_hash = var.legacy_raw_hash ? filemd5(var.masking_sql_file) : data.external.normalized.result.hash
  }
}

output "deployment_id" { value = terraform_data.masking.id }
