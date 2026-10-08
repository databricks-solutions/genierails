from pathlib import Path

import json
import os
import subprocess


SCRIPT = Path(__file__).parents[1] / "scripts" / "genie_space.sh"


def _create_with_fake_api(
    tmp_path, pages, title='Finance "北"', list_status=200, raw_list_body="",
    existing_status=404,
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
    status, body = int(os.environ.get("EXISTING_STATUS", "404")), {"message": "existing check"}
elif method == "GET" and "/api/2.0/permissions/genie/" in url:
    status, body = 200, {"access_control_list": []}
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
        "EXISTING_STATUS": str(existing_status),
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
    assert (tmp_path / ".genie_adopted_finance").exists()


def test_destroy_unmanages_title_adopted_agent_without_delete(tmp_path):
    title = "Existing"
    result, id_file, _ = _create_with_fake_api(
        tmp_path, [{"spaces": [{"space_id": "adopted-id", "title": title}]}], title
    )
    assert result.returncode == 0
    before = len((tmp_path / "calls.jsonl").read_text().splitlines())
    env = {**os.environ, "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}",
           "CALLS": str(tmp_path / "calls.jsonl"), "PAGES": "[]",
           "DATABRICKS_HOST": "https://target", "DATABRICKS_TOKEN": "token",
           "GENIE_ID_FILE": str(id_file)}
    destroyed = subprocess.run(["bash", str(SCRIPT), "trash"], env=env,
                               capture_output=True, text=True)
    assert destroyed.returncode == 0, destroyed.stdout + destroyed.stderr
    assert "Unmanaging adopted Genie agent adopted-id" in destroyed.stdout
    new_calls = [json.loads(x) for x in (tmp_path / "calls.jsonl").read_text().splitlines()[before:]]
    assert not any(call["method"] == "DELETE" for call in new_calls)
    assert not id_file.exists()


def test_migration_id_file_adopt_remains_created_and_trash_deletes(tmp_path):
    id_file = tmp_path / ".genie_space_id_finance"
    id_file.write_text("created-id\n")
    result, id_file, _ = _create_with_fake_api(
        tmp_path, [{"spaces": []}], title="Finance", existing_status=200
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert not (tmp_path / ".genie_adopted_finance").exists()
    before = len((tmp_path / "calls.jsonl").read_text().splitlines())
    env = {**os.environ, "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}",
           "CALLS": str(tmp_path / "calls.jsonl"), "PAGES": "[]",
           "DATABRICKS_HOST": "https://target", "DATABRICKS_TOKEN": "token",
           # Even the old bypass name must not suppress the production DELETE.
           "GENIE_ID_FILE": str(id_file), "GENIERAILS_TERRAFORM_TEST": "1"}
    destroyed = subprocess.run(["bash", str(SCRIPT), "trash"], env=env,
                               capture_output=True, text=True)
    assert destroyed.returncode == 0, destroyed.stdout + destroyed.stderr
    calls = [json.loads(x) for x in (tmp_path / "calls.jsonl").read_text().splitlines()[before:]]
    assert any(call["method"] == "DELETE" for call in calls)


def test_acl_replace_prints_each_direct_removed_principal_but_not_inherited(tmp_path):
    bin_dir = tmp_path / "bin"; bin_dir.mkdir()
    (bin_dir / "curl").write_text(r'''#!/bin/sh
if echo " $* " | grep -q " PUT "; then printf '{}\n200'; else cat <<'EOF'
{"access_control_list":[{"group_name":"configured","display_name":"Configured Team","all_permissions":[{"permission_level":"CAN_MANAGE","inherited":false}]},{"group_name":"manual","display_name":"Manual Team","all_permissions":[{"permission_level":"CAN_MANAGE","inherited":true},{"permission_level":"CAN_RUN","inherited":false}]},{"user_name":"person@example.com","display_name":"Person Name","all_permissions":[{"permission_level":"CAN_MANAGE","inherited":false}]},{"service_principal_name":"00000000-0000-0000-0000-000000000001","display_name":"Deploy Bot","all_permissions":[{"permission_level":"CAN_RUN","inherited":false}]},{"group_name":"admins","all_permissions":[{"permission_level":"CAN_MANAGE","inherited":true}]}]}
200
EOF
fi
''')
    (bin_dir / "curl").chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}",
           "DATABRICKS_HOST": "https://target", "DATABRICKS_TOKEN": "token",
           "GENIE_SPACE_OBJECT_ID": "space", "GENIE_GROUPS_CSV": "configured"}
    result = subprocess.run(["bash", str(SCRIPT), "set-acls"], env=env,
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Changing configured Genie access: group:configured [Configured Team] (CAN_MANAGE -> CAN_RUN)" in result.stdout
    assert "Removing hand-added Genie access: group:manual [Manual Team] (CAN_RUN) — not in config" in result.stdout
    assert "Removing hand-added Genie access: user:person@example.com [Person Name] (CAN_MANAGE) — not in config" in result.stdout
    assert "Removing hand-added Genie access: sp:00000000-0000-0000-0000-000000000001 [Deploy Bot] (CAN_RUN) — not in config" in result.stdout
    assert "admins" not in result.stdout


def test_empty_withheld_acl_removes_hand_added_access_without_granting(tmp_path):
    bin_dir = tmp_path / "bin"; bin_dir.mkdir()
    request_body = tmp_path / "put-body.json"
    (bin_dir / "curl").write_text(r'''#!/bin/sh
if echo " $* " | grep -q " PUT "; then
  while [ "$#" -gt 0 ]; do
    if [ "$1" = "-d" ]; then printf '%s' "$2" > "$PUT_BODY"; break; fi
    shift
  done
  printf '{}\n200'
else
  printf '%s\n200' '{"access_control_list":[{"group_name":"configured","display_name":"Configured Team","all_permissions":[{"permission_level":"CAN_RUN","inherited":false}]},{"group_name":"manual","display_name":"Manual Team","all_permissions":[{"permission_level":"CAN_RUN","inherited":false}]}]}'
fi
''')
    (bin_dir / "curl").chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}",
           "PUT_BODY": str(request_body), "DATABRICKS_HOST": "https://target",
           "DATABRICKS_TOKEN": "token", "GENIE_SPACE_OBJECT_ID": "adopted-space",
           "GENIE_GROUPS_CSV": "", "GENIE_CONFIGURED_GROUPS_CSV": "configured",
           "GENIE_ALLOW_EMPTY_ACL": "1"}
    result = subprocess.run(["bash", str(SCRIPT), "set-acls"], env=env,
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Removing configured Genie access: group:configured [Configured Team] (CAN_RUN) — not granted: CAN_RUN withheld until the agent access checks pass" in result.stdout
    assert "Removing hand-added Genie access: group:manual [Manual Team] (CAN_RUN) — not in config" in result.stdout
    assert json.loads(request_body.read_text()) == {"access_control_list": []}


def test_acl_replace_fails_closed_on_get_error_unless_force_is_printed(tmp_path):
    bin_dir = tmp_path / "bin"; bin_dir.mkdir()
    calls = tmp_path / "calls"
    (bin_dir / "curl").write_text(f'''#!/bin/sh
echo "$*" >> {calls}
if echo " $* " | grep -q " PUT "; then printf '{{}}\\n200'; else printf '{{"message":"down"}}\\n503'; fi
''')
    (bin_dir / "curl").chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}",
           "DATABRICKS_HOST": "https://target", "DATABRICKS_TOKEN": "token",
           "GENIE_SPACE_OBJECT_ID": "space", "GENIE_GROUPS_CSV": "configured"}
    refused = subprocess.run(["bash", str(SCRIPT), "set-acls"], env=env,
                             capture_output=True, text=True)
    assert refused.returncode != 0
    assert "refusing authoritative PUT" in refused.stderr
    assert " PUT " not in f" {calls.read_text()} "
    forced = subprocess.run(["bash", str(SCRIPT), "set-acls"],
                            env={**env, "GENIE_ACL_FORCE": "1"}, capture_output=True, text=True)
    assert forced.returncode == 0, forced.stdout + forced.stderr
    assert "GENIE_ACL_FORCE=1" in forced.stderr


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


def test_create_refuses_repeated_pagination_token(tmp_path):
    result, id_file, calls = _create_with_fake_api(tmp_path, [
        {"spaces": [], "next_page_token": "1"},
        {"spaces": [], "next_page_token": "1"},
    ], "Paged")
    assert result.returncode != 0
    assert "repeated page token '1'" in result.stderr
    assert "refusing to create" in result.stderr
    assert not id_file.exists()
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


def _update_config_warehouse_body(tmp_path, *, adopted, explicit):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    patch_body = tmp_path / "patch.json"
    fake = bin_dir / "curl"
    fake.write_text(r'''#!/usr/bin/env python3
import json, os, shutil, sys
args = sys.argv[1:]
method = args[args.index("-X") + 1] if "-X" in args else "GET"
if method == "PATCH":
    data_arg = args[args.index("-d") + 1]
    shutil.copyfile(data_arg.removeprefix("@"), os.environ["PATCH_BODY"])
    print("{}")
    print("200")
else:
    print(json.dumps({"warehouse_id": "live-warehouse"}))
''')
    fake.chmod(0o755)
    id_file = tmp_path / ".genie_space_id_sales"
    id_file.write_text("space-1\n")
    if adopted:
        (tmp_path / ".genie_adopted_sales").touch()
    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "PATCH_BODY": str(patch_body),
        "DATABRICKS_HOST": "https://target",
        "DATABRICKS_TOKEN": "token",
        "GENIE_ID_FILE": str(id_file),
        "GENIE_TABLES_CSV": "cat.schema.table",
        "GENIE_WAREHOUSE_ID": "configured-warehouse",
        "GENIE_WAREHOUSE_CREATED_DEFAULT": "1",
        "GENIE_WAREHOUSE_EXPLICIT": "1" if explicit else "0",
    }
    result = subprocess.run(["bash", str(SCRIPT), "update-config"], env=env,
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(patch_body.read_text())


def test_update_config_title_adopted_agent_ignores_created_default_warehouse(tmp_path):
    body = _update_config_warehouse_body(tmp_path, adopted=True, explicit=False)
    assert "warehouse_id" not in body


def test_update_config_non_adopted_agent_sends_explicit_per_space_warehouse(tmp_path):
    body = _update_config_warehouse_body(tmp_path, adopted=False, explicit=True)
    assert body["warehouse_id"] == "configured-warehouse"
