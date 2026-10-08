run "baseline" {
  command = apply
  variables {
    masking_sql_file = "../fixtures/mask_email.sql"
  }
}

run "comments_and_formatting_are_quiet" {
  command = apply
  variables {
    masking_sql_file = "../fixtures/mask_email_reformatted.sql"
  }
  assert {
    condition     = output.hash == run.baseline.hash
    error_message = "comments/formatting changed the normalized definition hash"
  }
  assert {
    condition     = output.deployment_id == run.baseline.deployment_id
    error_message = "comments/formatting replaced the masking deployment"
  }
}

run "body_change_is_meaningful" {
  command = apply
  variables {
    masking_sql_file = "../fixtures/mask_email_changed.sql"
  }
  assert {
    condition     = output.hash != run.baseline.hash
    error_message = "a real function-body change did not change the normalized hash"
  }
  assert {
    condition     = output.deployment_id != run.comments_and_formatting_are_quiet.deployment_id
    error_message = "a real function-body change did not replace the masking deployment"
  }
}

run "current_main_state" {
  command = apply
  variables {
    masking_sql_file = "../fixtures/mask_email.sql"
    legacy_raw_hash  = true
  }
}

run "upgrade_from_current_main_state" {
  command = apply
  variables {
    masking_sql_file = "../fixtures/mask_email.sql"
  }
  assert {
    condition     = output.deployment_id != run.current_main_state.deployment_id
    error_message = "the filemd5-to-normalized-hash state migration was not exercised"
  }
}
