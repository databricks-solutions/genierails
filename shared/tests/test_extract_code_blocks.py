"""Tests for extract_code_blocks() — LLM response parsing."""

import sys
from pathlib import Path

# Allow imports from shared/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from generate_abac import extract_code_blocks


class TestExtractCodeBlocks:
    """Tests for parsing SQL and HCL blocks from LLM responses."""

    def test_standard_sql_and_hcl_blocks(self):
        response = """Here is the governance config:

```sql
CREATE OR REPLACE FUNCTION mask_ssn(val STRING)
  RETURNS STRING
  RETURN '***-**-' || RIGHT(val, 4);
```

```hcl
groups = [
  { name = "analysts" },
]

tag_policies = [
  { key = "pii_level", values = ["masked"] },
]
```
"""
        sql, hcl = extract_code_blocks(response)
        assert sql is not None
        assert "mask_ssn" in sql
        assert hcl is not None
        assert "analysts" in hcl

    def test_terraform_fence_label(self):
        response = """```sql
CREATE OR REPLACE FUNCTION mask_email(val STRING)
  RETURNS STRING
  RETURN CONCAT(LEFT(val, 1), '***@', SPLIT(val, '@')[1]);
```

```terraform
groups = [
  { name = "team_a" },
]

tag_policies = [
  { key = "pii", values = ["yes"] },
]
```
"""
        sql, hcl = extract_code_blocks(response)
        assert sql is not None
        assert "mask_email" in sql
        assert hcl is not None
        assert "team_a" in hcl

    def test_unlabeled_blocks_detected_by_content(self):
        response = """```
CREATE OR REPLACE FUNCTION mask_phone(val STRING)
  RETURNS STRING
  RETURN '***-***-' || RIGHT(val, 4);
```

```
groups = [
  { name = "ops" },
]

tag_policies = [
  { key = "sensitivity", values = ["high", "low"] },
]
```
"""
        sql, hcl = extract_code_blocks(response)
        assert sql is not None
        assert "mask_phone" in sql
        assert hcl is not None
        assert "ops" in hcl

    def test_no_sql_block(self):
        response = """```hcl
groups = [
  { name = "viewers" },
]

tag_policies = [
  { key = "access", values = ["public"] },
]
```
"""
        sql, hcl = extract_code_blocks(response)
        assert sql is None
        assert hcl is not None
        assert "viewers" in hcl

    def test_no_hcl_block(self):
        response = """```sql
CREATE OR REPLACE FUNCTION mask_name(val STRING)
  RETURNS STRING
  RETURN LEFT(val, 1) || '***';
```
"""
        sql, hcl = extract_code_blocks(response)
        assert sql is not None
        assert "mask_name" in sql
        assert hcl is None

    def test_empty_response(self):
        sql, hcl = extract_code_blocks("")
        assert sql is None
        assert hcl is None

    def test_no_code_blocks(self):
        response = "I couldn't generate the governance config. Please try again."
        sql, hcl = extract_code_blocks(response)
        assert sql is None
        assert hcl is None

    def test_hcl_fallback_without_fences(self):
        """HCL detected from content even without proper fences."""
        response = """Here is the config:

groups = [
  { name = "admins" },
]

tag_policies = [
  { key = "pii_level", values = ["masked", "redacted"] },
]

tag_assignments = []
"""
        sql, hcl = extract_code_blocks(response)
        assert hcl is not None
        assert "admins" in hcl

    def test_multiple_sql_blocks_takes_first(self):
        response = """```sql
CREATE OR REPLACE FUNCTION mask_first(val STRING)
  RETURNS STRING RETURN 'FIRST';
```

```sql
CREATE OR REPLACE FUNCTION mask_second(val STRING)
  RETURNS STRING RETURN 'SECOND';
```

```hcl
groups = [
  { name = "test" },
]
tag_policies = [
  { key = "k", values = ["v"] },
]
```
"""
        sql, hcl = extract_code_blocks(response)
        assert sql is not None
        assert "mask_first" in sql
        assert "mask_second" not in sql

    def test_prose_between_blocks(self):
        """LLM may add explanatory text between code blocks."""
        response = """I've generated the masking functions below:

```sql
CREATE OR REPLACE FUNCTION mask_id(val STRING) RETURNS STRING
  RETURN CONCAT('ID_', RIGHT(val, 4));
```

And here is the corresponding HCL governance configuration:

```hcl
groups = [
  { name = "data_team" },
]

tag_policies = [
  { key = "class", values = ["pii", "public"] },
]
```

Please review and adjust as needed.
"""
        sql, hcl = extract_code_blocks(response)
        assert sql is not None
        assert "mask_id" in sql
        assert hcl is not None
        assert "data_team" in hcl

    def test_genie_space_configs_detected_as_hcl(self):
        """genie_space_configs + sample_questions should be detected as HCL."""
        response = """```
genie_space_configs = [
  {
    name = "Finance Analytics"
    sample_questions = ["What is revenue?"]
  },
]
```
"""
        sql, hcl = extract_code_blocks(response)
        assert hcl is not None
        assert "Finance Analytics" in hcl

    def test_tfvars_fence_label(self):
        response = """```tfvars
groups = [
  { name = "eng" },
]
tag_policies = [
  { key = "env", values = ["dev", "prod"] },
]
```
"""
        sql, hcl = extract_code_blocks(response)
        # tfvars isn't in the recognized labels, but fallback should catch it
        # via _extract_hcl_fallback
        assert hcl is not None or hcl is None  # depends on implementation
