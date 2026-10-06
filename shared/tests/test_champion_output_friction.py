"""First-time champion output in the dev-to-prod walkthrough stays minimal and right.

Covers: genie-mode next steps follow the walkthrough (not apply-genie), one
per-agent folder for one imported agent, enable-classification hides only the
benign -target warnings, and its review pointer is the Catalog Explorer UI.
"""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import generate_abac
from generate_abac import (
    SpaceFolderError,
    bootstrap_per_space_dirs,
    canonical_space_name,
    generate_next_steps,
    load_genie_space_id_to_name,
    resolve_space_folders,
)
from scripts import remap_env_config
from walkthrough_marker import MARKER, PROMOTED_HEADER, follows_walkthrough

SHARED = Path(__file__).parents[1]
TEMPLATE = SHARED / "examples/dev_to_prod/env.auto.tfvars.example"
MAKEFILE = SHARED / "Makefile.shared"
FILTER = SHARED / "scripts/filter_target_warnings.py"
TF_CAPTURE = SHARED / "tests/fixtures/terraform_target_apply.txt"  # real Terraform 1.11 -target apply
REPO = SHARED.parent

sys.path.insert(0, str(SHARED / "scripts"))
from filter_target_warnings import filter_lines  # noqa: E402

SPACE_ID = "01ef7b3c2a4d5e6f"
TITLE = "APJ Finance Agent"


# ── 1. Genie-mode next steps ────────────────────────────────────────────────

def test_template_carries_the_walkthrough_marker():
    assert MARKER in TEMPLATE.read_text().splitlines()[1]


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        (TEMPLATE.read_text(), True),                                   # copied template
        (PROMOTED_HEADER + "\ngenie_spaces = []\n", True),              # promoted prod env
        ('access_tier_groups = ["ops", "viewers"]\ngenie_spaces = []\n', False),  # self-service
        (f'genie_spaces = [{{ name = "{MARKER}" }}]\n', False),          # marker only in a value
    ],
    ids=["template", "promoted", "self-service-with-tiers", "not-a-comment"],
)
def test_follows_walkthrough_needs_the_explicit_marker(tmp_path, content, expected):
    env_file = tmp_path / "env.auto.tfvars"
    env_file.write_text(content)
    assert follows_walkthrough(env_file) is expected


def test_follows_walkthrough_without_env_file(tmp_path):
    assert follows_walkthrough(tmp_path / "missing.tfvars") is False


@pytest.mark.parametrize("source_has_marker", [True, False])
def test_promote_carries_the_marker_only_from_a_walkthrough_env(tmp_path, monkeypatch, source_has_marker):
    source, dest = tmp_path / "dev", tmp_path / "prod"
    source.mkdir()
    header = TEMPLATE.read_text().splitlines()[1] + "\n" if source_has_marker else ""
    (source / "env.auto.tfvars").write_text(header + 'uc_tables = ["dev.sales.customers"]\n')
    monkeypatch.setattr(sys, "argv", ["remap_env_config.py", str(source), str(dest), "dev=prod"])

    remap_env_config.main()

    assert follows_walkthrough(dest / "env.auto.tfvars") is source_has_marker


def test_champion_genie_next_steps_are_phase_1(tmp_path):
    out = "\n".join(generate_next_steps(tmp_path, "genie", "dev", has_sql=False, champion_flow=True))

    assert out == (
        "  Next steps (walkthrough Phase 1):\n"
        "    1. Enable classification on your catalog (Databricks UI, or: make enable-classification ENV=dev),\n"
        "       review detections, then enable automatic tagging and wait for class.* tags\n"
        "    2. make generate ENV=dev\n"
        "    3. make rehearse ENV=dev VERIFY_KEY_COLUMN=<key_column>"
    )
    assert "apply-genie" not in out


def test_self_service_env_with_access_tier_groups_keeps_apply_genie(tmp_path):
    env_file = tmp_path / "env.auto.tfvars"
    env_file.write_text('access_tier_groups = ["ops", "viewers"]\ngenie_spaces = []\n')

    lines = generate_next_steps(
        tmp_path, "genie", "bu_fin", has_sql=False, champion_flow=follows_walkthrough(env_file)
    )

    assert lines[-1] == "    4. make apply-genie ENV=bu_fin   (applies workspace layer only)"


def test_self_service_genie_next_steps_unchanged(tmp_path):
    out = "\n".join(generate_next_steps(tmp_path, "genie", "bu_fin", has_sql=False, champion_flow=False))

    assert "make validate-generated ENV=bu_fin" in out
    assert "    4. make apply-genie ENV=bu_fin   (applies workspace layer only)" in out
    assert "enable-classification" not in out


@pytest.mark.parametrize(
    ("env_name", "next_cmd"),
    [("dev", "make rehearse ENV=dev VERIFY_KEY_COLUMN=<key_column>"), ("prod", "make certify ENV=prod")],
)
def test_champion_full_mode_points_to_rehearse_or_certify(tmp_path, env_name, next_cmd):
    lines = generate_next_steps(tmp_path, "full", env_name, has_sql=True, champion_flow=True)

    assert lines[-1] == f"    2. {next_cmd}"
    assert f"       {tmp_path}/masking_functions.sql" in lines
    assert not any(re.search(r"make apply\b", line) for line in lines)


@pytest.mark.parametrize(("mode", "target"), [("full", "apply"), ("governance", "apply-governance")])
def test_non_champion_full_and_governance_unchanged(tmp_path, mode, target):
    lines = generate_next_steps(tmp_path, mode, "dev", has_sql=True, champion_flow=False)
    assert lines[-1].startswith(f"    4. make {target}   ")


def test_main_wires_champion_flow_into_header_and_next_steps():
    source = Path(generate_abac.__file__).read_text()
    main_body = source[source.index("def main():"):source.index("def generate_next_steps(")]

    assert "champion_flow = follows_walkthrough(env_file)" in main_body
    assert "access_tier_groups_set" not in source
    assert 'if args.mode == "genie" and champion_flow:' in main_body
    assert "champion_flow=champion_flow" in main_body


# ── 2. One per-agent folder for one imported agent ──────────────────────────

def _imported_generated(tmp_path):
    out_dir = tmp_path / "generated"
    out_dir.mkdir()
    (out_dir / "abac.auto.tfvars").write_text(
        f'genie_space_configs = {{\n  "{TITLE}" = {{\n    title = "{TITLE}"\n'
        '    description = "Imported"\n  }\n}\n\n'
        f'genie_space_id_to_name = {{\n  "{SPACE_ID}" = "{TITLE}"\n}}\n'
    )
    return out_dir


def test_imported_agent_bootstraps_exactly_one_folder(tmp_path, capsys):
    out_dir = _imported_generated(tmp_path)

    bootstrap_per_space_dirs(out_dir, {"genie_spaces": [{"genie_space_id": SPACE_ID}]}, "")

    folders = sorted(p.name for p in (out_dir / "spaces").iterdir())
    assert folders == ["apj_finance_agent"]
    written = (out_dir / "spaces/apj_finance_agent/abac.auto.tfvars").read_text()
    assert 'description = "Imported"' in written      # the real config, not a stub
    assert f'make generate SPACE="{TITLE}"' in written
    assert capsys.readouterr().out.strip() == (
        "Per-agent config: generated/spaces/apj_finance_agent/abac.auto.tfvars"
    )


def _stub(space_dir, name):
    space_dir.mkdir(parents=True)
    (space_dir / "abac.auto.tfvars").write_text(
        f"# Per-space config for: {name}\n"
        f'# Bootstrapped by full generation. Re-run: make generate SPACE="{name}"\n'
    )


def _real(space_dir):
    space_dir.mkdir(parents=True, exist_ok=True)
    (space_dir / "abac.auto.tfvars").write_text("# per-space draft from make generate SPACE=\n")
    (space_dir / "masking_functions.sql").write_text("-- customized\n")


IMPORTED = {"genie_spaces": [{"genie_space_id": SPACE_ID}]}


def test_stale_id_folder_stub_is_removed(tmp_path):
    out_dir = _imported_generated(tmp_path)
    _stub(out_dir / "spaces" / SPACE_ID, SPACE_ID)

    bootstrap_per_space_dirs(out_dir, IMPORTED, "")

    assert sorted(p.name for p in (out_dir / "spaces").iterdir()) == ["apj_finance_agent"]


def test_id_folder_with_real_content_and_no_title_folder_is_kept(tmp_path, capsys):
    out_dir = _imported_generated(tmp_path)
    id_dir = out_dir / "spaces" / SPACE_ID
    _real(id_dir)

    bootstrap_per_space_dirs(out_dir, IMPORTED, "")

    assert sorted(p.name for p in (out_dir / "spaces").iterdir()) == [SPACE_ID]
    assert (id_dir / "masking_functions.sql").read_text() == "-- customized\n"
    out = capsys.readouterr().out
    assert f"NOTE: keeping generated/spaces/{SPACE_ID}/ for '{TITLE}' (it has per-agent content)." in out
    assert f"Per-agent config: generated/spaces/{SPACE_ID}/abac.auto.tfvars" in out
    # make generate SPACE= picks the same folder.
    folders, _ = resolve_space_folders(IMPORTED["genie_spaces"], {SPACE_ID: TITLE}, out_dir / "spaces")
    assert [f.key for f in folders] == [SPACE_ID]


def test_id_folder_kept_over_a_title_stub(tmp_path):
    out_dir = _imported_generated(tmp_path)
    _real(out_dir / "spaces" / SPACE_ID)
    _stub(out_dir / "spaces/apj_finance_agent", TITLE)

    bootstrap_per_space_dirs(out_dir, IMPORTED, "")

    assert sorted(p.name for p in (out_dir / "spaces").iterdir()) == [SPACE_ID]


def test_both_folders_with_real_content_stop_with_instructions(tmp_path, capsys):
    out_dir = _imported_generated(tmp_path)
    _real(out_dir / "spaces" / SPACE_ID)
    _real(out_dir / "spaces/apj_finance_agent")
    before = {p: p.read_text() for p in (out_dir / "spaces").rglob("*") if p.is_file()}

    with pytest.raises(SystemExit) as exc:
        bootstrap_per_space_dirs(out_dir, IMPORTED, "")

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert (
        f"generated/spaces/{SPACE_ID}/ and generated/spaces/apj_finance_agent/ both hold "
        f"per-agent content for '{TITLE}' ({SPACE_ID}). Keep generated/spaces/apj_finance_agent/: "
        f"move or merge anything you still need from generated/spaces/{SPACE_ID}/ into it, "
        f"delete generated/spaces/{SPACE_ID}/, then re-run make generate."
    ) in out
    assert {p: p.read_text() for p in (out_dir / "spaces").rglob("*") if p.is_file()} == before


def test_two_agents_with_the_same_title_stop_naming_both_ids(tmp_path):
    spaces = [{"genie_space_id": "01aaa"}, {"genie_space_id": "01bbb"}]

    with pytest.raises(SpaceFolderError) as exc:
        resolve_space_folders(spaces, {"01aaa": TITLE, "01bbb": TITLE}, tmp_path / "spaces")

    assert "Genie agents 01aaa and 01bbb are both named 'APJ Finance Agent'" in str(exc.value)
    assert 'name = "..."' in str(exc.value)


def test_names_that_sanitize_alike_get_a_stable_id_suffix(tmp_path, capsys):
    out_dir = tmp_path / "generated"
    out_dir.mkdir()
    (out_dir / "abac.auto.tfvars").write_text(
        'genie_space_configs = {\n  "Finance & HR" = { title = "Finance & HR" }\n'
        '  "Finance HR" = { title = "Finance HR" }\n}\n'
    )
    auth_cfg = {"genie_spaces": [
        {"name": "Finance & HR", "genie_space_id": "01aaa"},
        {"name": "Finance HR", "genie_space_id": "01bbb"},
    ]}

    bootstrap_per_space_dirs(out_dir, auth_cfg, "")
    bootstrap_per_space_dirs(out_dir, auth_cfg, "")  # stable across runs

    spaces = out_dir / "spaces"
    assert sorted(p.name for p in spaces.iterdir()) == ["finance_hr", "finance_hr--01bbb"]
    assert "Finance & HR" in (spaces / "finance_hr/abac.auto.tfvars").read_text()
    assert '"Finance HR"' in (spaces / "finance_hr--01bbb/abac.auto.tfvars").read_text()


def test_space_folder_names_never_feed_terraform_keys():
    # Folder names are local to generate/merge; no Terraform root or module
    # reads generated/spaces/, so the folder choice can't move resource keys.
    for tf in (SHARED / "roots").rglob("*.tf"):
        assert "spaces/" not in tf.read_text().replace("genie/spaces/", ""), tf
    for tf in (SHARED / "modules").rglob("*.tf"):
        assert "generated/spaces" not in tf.read_text(), tf


def test_canonical_space_name_mirrors_terraform_lookup(tmp_path):
    id_to_name = load_genie_space_id_to_name(_imported_generated(tmp_path) / "abac.auto.tfvars")

    assert id_to_name == {SPACE_ID: TITLE}
    assert canonical_space_name({"genie_space_id": SPACE_ID}, id_to_name) == TITLE
    assert canonical_space_name({"genie_space_id": SPACE_ID, "name": "Named"}, id_to_name) == "Named"
    assert canonical_space_name({"genie_space_id": SPACE_ID}, {}) == SPACE_ID
    assert load_genie_space_id_to_name(tmp_path / "missing.tfvars") == {}


def test_terraform_canonical_name_uses_the_same_lookup():
    root = (SHARED / "roots/workspace/main.tf").read_text()
    assert "lookup(var.genie_space_id_to_name, s.genie_space_id, s.genie_space_id)" in root


# ── 3. -target warnings filtered, real errors kept ──────────────────────────

CAPTURE = TF_CAPTURE.read_text().splitlines(keepends=True)


def _boxes(lines):
    """Split the capture into (outside lines, [box line lists])."""
    boxes, outside, box = [], [], None
    for line in lines:
        plain = re.sub(r"\x1b\[[0-9;]*m", "", line).strip()
        if box is None and plain == "╷":
            box = [line]
        elif box is not None:
            box.append(line)
            if plain == "╵":
                boxes.append(box)
                box = None
        else:
            outside.append(line)
    return outside, boxes


OUTSIDE, (TARGETING, INCOMPLETE) = _boxes(CAPTURE)
ERROR = ["╷\n", "│ Error: Usage policy ID must not be empty\n", "│ \n", "│   with module.data_access...\n", "╵\n"]
OTHER_WARNING = ["╷\n", "│ Warning: Deprecated attribute\n", "│ \n", "│ use X instead\n", "╵\n"]


def test_filter_drops_only_the_real_target_warnings():
    out = "".join(filter_lines(CAPTURE + OTHER_WARNING + ERROR))

    assert out == "".join(OUTSIDE + OTHER_WARNING + ERROR)
    assert "Apply complete! Resources: 1 added" in out


def test_filter_matches_uncolored_and_rewrapped_text():
    plain = [re.sub(r"\x1b\[[0-9;]*m", "", line) for line in TARGETING]
    words = " ".join(l.strip().lstrip("│").strip() for l in plain[1:-1]).split()
    rewrapped = ["╷\n"] + [f"│ {' '.join(words[i:i + 7])}\n" for i in range(0, len(words), 7)] + ["╵\n"]
    assert list(filter_lines(plain + rewrapped)) == []


def test_filter_keeps_a_warning_box_with_an_interleaved_error():
    box = TARGETING[:3] + ["Error: Usage policy ID must not be empty\n"] + TARGETING[3:]
    assert list(filter_lines(box)) == box


def test_filter_keeps_a_known_title_with_unknown_body():
    box = TARGETING[:2] + ["│ Something else Terraform added.\n"] + TARGETING[-1:]
    assert list(filter_lines(box)) == box


def test_filter_keeps_an_unknown_warning():
    assert list(filter_lines(OTHER_WARNING)) == OTHER_WARNING


def test_filter_emits_an_unterminated_box_at_eof_verbatim():
    assert list(filter_lines(TARGETING[:-1])) == TARGETING[:-1]


def test_filter_script_streams_unbuffered():
    source = FILTER.read_text()
    assert "sys.stdout.flush()" in source
    assert 'python3 -u "$(SHARED_ROOT)/scripts/filter_target_warnings.py"' in MAKEFILE.read_text()


def test_makefile_shared_runs_recipes_in_bash_with_pipefail():
    source = MAKEFILE.read_text()
    assert re.search(r"^SHELL := /bin/bash$", source, flags=re.MULTILINE)
    body = source[source.index("enable-classification:"):source.index("\ngenerate:")]
    assert "@set -o pipefail; LAYER_ENV_DIR=" in body


# The REAL target through the repo's aws/ Makefile -> Makefile.shared ->
# terraform_layer.sh, with only `terraform` (on PATH) and the Databricks SDK
# (on PYTHONPATH) faked.
FAKE_TERRAFORM = """#!/usr/bin/env bash
case "$1" in
  init|state) exit 0 ;;
  apply) cat "$FAKE_TF_STDOUT"; [ -z "$FAKE_TF_STDERR" ] || printf '%s\\n' "$FAKE_TF_STDERR" >&2
         exit "${FAKE_TF_EXIT:-0}" ;;
esac
"""
FAKE_SDK = """from .errors import NotFound
class _Classification:
    def get_catalog_config(self, name):
        raise NotFound(name)
class WorkspaceClient:
    def __init__(self, **kwargs):
        self.data_classification = _Classification()
"""


def _clean_env():
    return {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL", "ENV", "MODE")}


@pytest.fixture
def real_cloud(tmp_path):
    if shutil.which("make") is None:
        pytest.skip("make not installed")
    cloud_root = tmp_path / "aws"
    cloud_root.mkdir()
    args = [f"CLOUD_ROOT={cloud_root}", f"SHARED_ROOT={SHARED}"]
    setup = subprocess.run(["make", "--no-print-directory", "setup", "ENV=dev", *args],
                           cwd=REPO / "aws", text=True, capture_output=True, env=_clean_env())
    assert setup.returncode == 0, setup.stdout + setup.stderr
    (cloud_root / "envs/dev/env.auto.tfvars").write_text(
        TEMPLATE.read_text().replace("<your-genie-space-id>", SPACE_ID)
        + '\nuc_tables = ["dev_finance.genierails_e2e.customers"]\n'
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "terraform").write_text(FAKE_TERRAFORM)
    (fake_bin / "terraform").chmod(0o755)
    sdk = tmp_path / "sdk/databricks/sdk"
    sdk.mkdir(parents=True)
    (sdk.parent / "__init__.py").write_text("")
    (sdk / "__init__.py").write_text(FAKE_SDK)
    (sdk / "errors.py").write_text("class NotFound(Exception):\n    pass\n")

    def run(**fake):
        env = _clean_env()
        env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
        env["PYTHONPATH"] = str(tmp_path / "sdk")
        env.update({"FAKE_TF_STDOUT": str(TF_CAPTURE), **fake})
        return subprocess.run(
            ["make", "--no-print-directory", "enable-classification", "ENV=dev", *args],
            cwd=REPO / "aws", text=True, capture_output=True, env=env, timeout=120,
        )
    return run


def test_real_target_hides_target_warnings_and_points_to_catalog_explorer(real_cloud):
    result = real_cloud()
    out = result.stdout + result.stderr

    assert result.returncode == 0, out
    assert "Apply complete! Resources: 1 added" in out
    assert "Resource targeting is in effect" not in out
    assert "Applied changes may be incomplete" not in out
    assert "system.data_classification" not in out
    assert "Catalog Explorer, open your catalog > Data classification > Review detections" in out
    assert "envs/dev/env.auto.tfvars and re-run: make enable-classification ENV=dev" in out


def test_real_target_surfaces_terraform_errors_and_fails(real_cloud):
    result = real_cloud(FAKE_TF_STDERR="Error: Usage policy ID must not be empty", FAKE_TF_EXIT="1")
    out = result.stdout + result.stderr

    assert result.returncode != 0, out
    assert "Error: Usage policy ID must not be empty" in out
    assert "Resource targeting is in effect" not in out
    assert "Classification is enabled" not in out


def test_real_target_with_auto_tagging_points_to_generate(real_cloud, tmp_path):
    env_file = tmp_path / "aws/envs/dev/env.auto.tfvars"
    env_file.write_text(env_file.read_text().replace("enable_auto_tagging = false", "enable_auto_tagging = true"))

    result = real_cloud()

    assert result.returncode == 0, result.stdout + result.stderr
    assert "Wait for class.* tags to appear in Catalog Explorer, then run:\n  make generate ENV=dev" in result.stdout
