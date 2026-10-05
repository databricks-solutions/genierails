from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "genie_space.sh"


def test_sql_expressions_and_measures_include_required_display_name():
    source = SCRIPT.read_text()

    expression_block = source[source.index('expr_json ='):source.index('meas_json =')]
    measure_block = source[source.index('meas_json ='):source.index('join_json =')]

    assert '"display_name": e["display_name"]' in expression_block
    assert '"display_name": m["display_name"]' in measure_block


def test_update_config_reuses_existing_agent_tables_when_none_configured(tmp_path):
    """An agent attached by genie_space_id alone has no uc_tables in config."""
    import json
    import os
    import subprocess

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    patch_body = tmp_path / "patch.json"
    serialized = json.dumps({"version": 2, "data_sources": {"tables": [
        {"identifier": "cat.s.customers"}, {"identifier": "cat.s.notes"},
    ]}})
    (bin_dir / "curl").write_text(f"""#!/bin/bash
for arg in "$@"; do
  if [[ "$prev" == "-d" ]]; then cp "${{arg#@}}" {patch_body}; fi
  prev="$arg"
done
if [[ " $* " == *" PATCH "* ]]; then printf '{{}}\\n200'; exit 0; fi
printf '%s' {json.dumps(json.dumps({"serialized_space": serialized}))}
""")
    (bin_dir / "curl").chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}",
           "DATABRICKS_HOST": "https://ws", "DATABRICKS_TOKEN": "t",
           "GENIE_SPACE_OBJECT_ID": "01abc", "GENIE_TABLES_CSV": ""}

    result = subprocess.run(["bash", str(SCRIPT), "update-config"], env=env,
                            capture_output=True, text=True)

    assert result.returncode == 0, result.stdout + result.stderr
    space = json.loads(json.loads(patch_body.read_text())["serialized_space"])
    assert [t["identifier"] for t in space["data_sources"]["tables"]] == [
        "cat.s.customers", "cat.s.notes",
    ]
