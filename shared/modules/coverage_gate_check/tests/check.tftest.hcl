# The one judgement of a coverage-gate result, shared by the data_access
# (business SELECT) and workspace (Genie CAN_RUN) layers. Pass results are
# refreshed at plan time ("@NOW@"); old and future results use fixed dates, so
# nothing depends on the wall clock.

variables {
  gate_file            = "tests/.tmp/check/.coverage_gate.json"
  expected_fingerprint = "fp"
  max_age              = "6h"
}

run "no_result" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/check/.coverage_gate.json" = null
    }
  }
}

run "missing_result" {
  command = plan
  assert {
    condition     = output.status == "missing"
    error_message = "expected missing, got ${output.status}: ${output.problem}"
  }
}

run "garbage" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/check/.coverage_gate.json" = "{not json"
    }
  }
}

run "unreadable_result" {
  command = plan
  assert {
    condition     = output.status == "unreadable"
    error_message = "expected unreadable, got ${output.status}: ${output.problem}"
  }
}

run "fresh_pass" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/check/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "fp", refreshed_at = "@NOW@" })
    }
  }
}

run "fresh_pass_passes" {
  command = plan
  assert {
    condition     = output.status == "pass"
    error_message = "expected pass, got ${output.status}: ${output.problem}"
  }
}

run "other_fingerprint_is_stale" {
  command = plan
  variables {
    expected_fingerprint = "other"
  }
  assert {
    condition     = output.status == "stale"
    error_message = "expected stale, got ${output.status}: ${output.problem}"
  }
}

run "max_age_at_the_ceiling_passes" {
  command = plan
  variables {
    max_age = "24h"
  }
  assert {
    condition     = output.status == "pass"
    error_message = "expected pass, got ${output.status}: ${output.problem}"
  }
}

run "max_age_above_the_ceiling_is_invalid" {
  command = plan
  variables {
    max_age = "876000h"
  }
  assert {
    condition     = output.status == "invalid_max_age"
    error_message = "expected invalid_max_age, got ${output.status}: ${output.problem}"
  }
}

run "max_age_just_above_the_ceiling_is_invalid" {
  command = plan
  variables {
    max_age = "24h1s"
  }
  assert {
    condition     = output.status == "invalid_max_age"
    error_message = "expected invalid_max_age, got ${output.status}: ${output.problem}"
  }
}

run "max_age_zero_is_invalid" {
  command = plan
  variables {
    max_age = "0s"
  }
  assert {
    condition     = output.status == "invalid_max_age"
    error_message = "expected invalid_max_age, got ${output.status}: ${output.problem}"
  }
}

run "max_age_negative_is_invalid" {
  command = plan
  variables {
    max_age = "-1h"
  }
  assert {
    condition     = output.status == "invalid_max_age"
    error_message = "expected invalid_max_age, got ${output.status}: ${output.problem}"
  }
}

run "max_age_malformed_is_invalid" {
  command = plan
  variables {
    max_age = "six hours"
  }
  assert {
    condition     = output.status == "invalid_max_age"
    error_message = "expected invalid_max_age, got ${output.status}: ${output.problem}"
  }
}

run "max_age_empty_is_invalid" {
  command = plan
  variables {
    max_age = ""
  }
  assert {
    condition     = output.status == "invalid_max_age"
    error_message = "expected invalid_max_age, got ${output.status}: ${output.problem}"
  }
}

run "failed_result" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/check/.coverage_gate.json" = jsonencode({ status = "fail", fingerprint = "fp", refreshed_at = "@NOW@" })
    }
  }
}

run "failed_result_fails" {
  command = plan
  assert {
    condition     = output.status == "failed"
    error_message = "expected failed, got ${output.status}: ${output.problem}"
  }
}

run "pass_without_refresh" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/check/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "fp" })
    }
  }
}

run "no_refresh_is_unrefreshed" {
  command = plan
  assert {
    condition     = output.status == "unrefreshed"
    error_message = "expected unrefreshed, got ${output.status}: ${output.problem}"
  }
}

run "pass_with_malformed_refresh" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/check/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "fp", refreshed_at = "yesterday" })
    }
  }
}

run "malformed_refresh_is_unrefreshed" {
  command = plan
  assert {
    condition     = output.status == "unrefreshed"
    error_message = "expected unrefreshed, got ${output.status}: ${output.problem}"
  }
}

run "pass_from_the_future" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/check/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "fp", refreshed_at = "2999-01-01T00:00:00Z" })
    }
  }
}

run "future_refresh_is_unrefreshed" {
  command = plan
  assert {
    condition     = output.status == "unrefreshed"
    error_message = "expected unrefreshed, got ${output.status}: ${output.problem}"
  }
}

run "old_pass" {
  module {
    source = "../../roots/data_access/tests/file_writer"
  }
  variables {
    files = {
      "tests/.tmp/check/.coverage_gate.json" = jsonencode({ status = "pass", fingerprint = "fp", refreshed_at = "2000-01-01T00:00:00Z" })
    }
  }
}

run "old_refresh_is_expired" {
  command = plan
  assert {
    condition     = output.status == "expired"
    error_message = "expected expired, got ${output.status}: ${output.problem}"
  }
}

run "old_refresh_is_expired_even_at_the_ceiling" {
  command = plan
  variables {
    max_age = "24h"
  }
  assert {
    condition     = output.status == "expired"
    error_message = "expected expired, got ${output.status}: ${output.problem}"
  }
}
