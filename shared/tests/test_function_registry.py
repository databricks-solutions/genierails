from function_registry import FUNCTION_REGISTRY


def test_normalize_hcl_only_rewrites_function_name_attributes():
    source = '''
tag_assignments = [{
  tag_key = "gr_treatment"
  tag_value = "redact"
}]
fgac_policies = [{
  function_name = "redact"
  match_condition = "hasTagValue('gr_treatment', 'redact')"
}]
'''

    normalized, count = FUNCTION_REGISTRY.normalize_hcl(source)

    assert count == 1
    assert 'tag_value = "redact"' in normalized
    assert "hasTagValue('gr_treatment', 'redact')" in normalized
    assert 'function_name = "mask_redact"' in normalized
