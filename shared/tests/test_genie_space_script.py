from pathlib import Path

import json
import os
import subprocess


SCRIPT = Path(__file__).parents[1] / "scripts" / "genie_space.sh"


def _create_with_fake_api(
    tmp_path, pages, title='Finance "北"', list_status=200, raw_list_body=""
):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "calls.jsonl"
    fake = bin_dir / "curl"
    fake.write_text(r'''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
url = next((a for a in reversed(args) if a.startswith("http")), "")
method = args[args.index("-X") + 1] if "-X" in args else "GET"
with open(os.environ["CALLS"], "a") as f:
    f.write(json.dumps({"method": method, "url": url, "args": args}) + "\n")
status, body = 200, {}
if method == "GET" and url.endswith("/api/2.0/genie/spaces"):
    status = int(os.environ.get("LIST_STATUS", "200"))
    pages = json.loads(os.environ["PAGES"])
    token = ""
    for i, arg in enumerate(args):
        if arg == "--data-urlencode" and i + 1 < len(args) and args[i + 1].startswith("page_token="):
            token = args[i + 1].split("=", 1)[1]
    body = pages[int(token or "0")] if status == 200 else {"message": "list failed"}
elif method == "POST" and url.endswith("/api/2.0/genie/spaces"):
    status, body = 201, {"space_id": "created-id"}
elif method == "GET" and "/api/2.0/genie/spaces/" in url:
    status, body = 404, {"message": "gone"}
text = os.environ.get("RAW_LIST_BODY", "") if (
    method == "GET" and url.endswith("/api/2.0/genie/spaces")
    and os.environ.get("RAW_LIST_BODY")
) else json.dumps(body, ensure_ascii=False)
if "-o" in args and args[args.index("-o") + 1] == "/dev/null":
    text = ""
if "-w" in args:
    sys.stdout.write(text + ("\n" if text else "") + str(status))
else:
    sys.stdout.write(text)
''')
    fake.chmod(0o755)
    id_file = tmp_path / ".genie_space_id_finance"
    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "CALLS": str(calls),
        "PAGES": json.dumps(pages, ensure_ascii=False),
        "LIST_STATUS": str(list_status),
        "RAW_LIST_BODY": raw_list_body,
        "DATABRICKS_HOST": "https://target",
        "DATABRICKS_TOKEN": "token",
        "GENIE_ID_FILE": str(id_file),
        "GENIE_TABLES_CSV": "cat.schema.table",
        "GENIE_WAREHOUSE_ID": "warehouse",
        "GENIE_TITLE": title,
    }
    result = subprocess.run(
        ["bash", str(SCRIPT), "create"], env=env, capture_output=True, text=True
    )
    recorded = [json.loads(line) for line in calls.read_text().splitlines()]
    return result, id_file, recorded


def test_create_posts_only_when_exact_title_is_absent(tmp_path):
    result, id_file, calls = _create_with_fake_api(
        tmp_path, [{"spaces": [{"space_id": "other", "title": "finance \"北\""}]}]
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert id_file.read_text().strip() == "created-id"
    assert [c["method"] for c in calls].count("POST") == 1


def test_create_adopts_one_exact_unicode_quoted_title_match(tmp_path):
    title = 'Finance "北"'
    result, id_file, calls = _create_with_fake_api(
        tmp_path, [{"spaces": [{"space_id": "existing-id", "title": title}]}], title
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert id_file.read_text().strip() == "existing-id"
    assert f'Adopted existing Genie agent existing-id titled "{title}" instead of creating a duplicate' in result.stdout
    assert not any(c["method"] == "POST" for c in calls)


def test_adopted_id_is_used_by_normal_config_and_acl_updates(tmp_path):
    title = "Existing"
    result, id_file, _ = _create_with_fake_api(
        tmp_path, [{"spaces": [{"space_id": "adopted-id", "title": title}]}], title
    )
    assert result.returncode == 0, result.stdout + result.stderr
    env = {
        **os.environ,
        "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}",
        "CALLS": str(tmp_path / "calls.jsonl"),
        "PAGES": "[]",
        "DATABRICKS_HOST": "https://target",
        "DATABRICKS_TOKEN": "token",
        "GENIE_ID_FILE": str(id_file),
        "GENIE_TABLES_CSV": "cat.schema.table",
        "GENIE_TITLE": title,
        "GENIE_GROUPS_CSV": "analysts",
    }
    for command in ("update-config", "set-acls"):
        updated = subprocess.run(
            ["bash", str(SCRIPT), command], env=env, capture_output=True, text=True
        )
        assert updated.returncode == 0, updated.stdout + updated.stderr
    calls = [json.loads(line) for line in (tmp_path / "calls.jsonl").read_text().splitlines()]
    assert any(c["method"] == "PATCH" and c["url"].endswith("/adopted-id") for c in calls)
    assert any(c["method"] == "PUT" and c["url"].endswith("/adopted-id") for c in calls)


def test_create_refuses_multiple_exact_title_matches(tmp_path):
    title = "Same"
    result, id_file, calls = _create_with_fake_api(tmp_path, [{"spaces": [
        {"space_id": "id-1", "title": title}, {"space_id": "id-2", "title": title},
    ]}], title)
    assert result.returncode != 0
    assert "id-1, id-2" in result.stderr
    assert "set genie_space_id" in result.stderr
    assert not id_file.exists()
    assert not any(c["method"] == "POST" for c in calls)


def test_create_refuses_when_list_fails(tmp_path):
    result, id_file, calls = _create_with_fake_api(tmp_path, [{}], list_status=503)
    assert result.returncode != 0
    assert "duplicate detection failed" in result.stderr
    assert not id_file.exists()
    assert not any(c["method"] == "POST" for c in calls)


def test_create_refuses_when_list_response_is_malformed(tmp_path):
    result, id_file, calls = _create_with_fake_api(
        tmp_path, [{}], raw_list_body="not-json"
    )
    assert result.returncode != 0
    assert "Could not parse the Genie agent list" in result.stderr
    assert not id_file.exists()
    assert not any(c["method"] == "POST" for c in calls)


def test_create_checks_every_page_before_adopting(tmp_path):
    title = "Paged"
    result, id_file, calls = _create_with_fake_api(tmp_path, [
        {"spaces": [{"space_id": "other", "title": "Other"}], "next_page_token": "1"},
        {"spaces": [{"space_id": "paged-id", "title": title}]},
    ], title)
    assert result.returncode == 0, result.stdout + result.stderr
    assert id_file.read_text().strip() == "paged-id"
    assert len([c for c in calls if c["method"] == "GET"]) == 2
    assert not any(c["method"] == "POST" for c in calls)


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
