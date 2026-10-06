"""First-time champion output in the dev-to-prod walkthrough stays minimal and right.

Covers: genie-mode next steps follow the walkthrough (not apply-genie), one
per-agent folder for one imported agent, enable-classification hides only the
benign -target warnings, and its review pointer is the Catalog Explorer UI.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

import generate_abac
from generate_abac import (
    CHAMPION_TEMPLATE_MARKER,
    bootstrap_per_space_dirs,
    canonical_space_name,
    follows_champion_flow,
    generate_next_steps,
    load_genie_space_id_to_name,
)

SHARED = Path(__file__).parents[1]
TEMPLATE = SHARED / "examples/dev_to_prod/env.auto.tfvars.example"
MAKEFILE = SHARED / "Makefile.shared"
FILTER = SHARED / "scripts/filter_target_warnings.py"

sys.path.insert(0, str(SHARED / "scripts"))
from filter_target_warnings import filter_lines  # noqa: E402

SPACE_ID = "01ef7b3c2a4d5e6f"
TITLE = "APJ Finance Agent"


# ── 1. Genie-mode next steps ────────────────────────────────────────────────

def test_template_carries_the_champion_marker():
    assert CHAMPION_TEMPLATE_MARKER in TEMPLATE.read_text().splitlines()[1]


@pytest.mark.parametrize(
    ("content", "groups_set", "expected"),
    [
        ("sql_warehouse_id = \"\"\n", True, True),          # access_tier_groups set
        (TEMPLATE.read_text(), False, True),                # copied from the template
        ("genie_spaces = []\n", False, False),              # self-service BU env
    ],
    ids=["access-tier-groups", "template", "self-service"],
)
def test_follows_champion_flow(tmp_path, content, groups_set, expected):
    env_file = tmp_path / "env.auto.tfvars"
    env_file.write_text(content)
    assert follows_champion_flow(env_file, groups_set) is expected


def test_follows_champion_flow_without_env_file(tmp_path):
    assert follows_champion_flow(tmp_path / "missing.tfvars", False) is False


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

    assert "champion_flow = follows_champion_flow(env_file," in main_body
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


def test_stale_id_folder_stub_is_removed_but_real_content_is_kept(tmp_path):
    out_dir = _imported_generated(tmp_path)
    stale = out_dir / "spaces" / SPACE_ID
    stale.mkdir(parents=True)
    (stale / "abac.auto.tfvars").write_text(
        f"# Per-space config for: {SPACE_ID}\n"
        f'# Bootstrapped by full generation. Re-run: make generate SPACE="{SPACE_ID}"\n'
    )

    bootstrap_per_space_dirs(out_dir, {"genie_spaces": [{"genie_space_id": SPACE_ID}]}, "")
    assert not stale.exists()

    # A folder holding anything beyond the bootstrap stub is never deleted.
    stale.mkdir()
    (stale / "abac.auto.tfvars").write_text("# Bootstrapped by full generation.\n")
    (stale / "masking_functions.sql").write_text("-- per-space draft\n")
    bootstrap_per_space_dirs(out_dir, {"genie_spaces": [{"genie_space_id": SPACE_ID}]}, "")
    assert (stale / "masking_functions.sql").exists()


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

def _diag(title, body, color=True):
    if color:
        y, r, b = "\x1b[33m", "\x1b[0m", "\x1b[1m"
        return [
            f"{y}╷{r}{r}\n",
            f"{y}│{r} {r}{b}{y}Warning: {r}{r}{b}{title}{r}\n",
            f"{y}│{r} {r}\n",
            f"{y}│{r} {r}{r}{body}\n",
            f"{y}╵{r}{r}\n",
        ]
    return ["╷\n", f"│ {title}\n", "│ \n", f"│ {body}\n", "╵\n"]


TARGETING = _diag("Resource targeting is in effect", "You are creating a plan with the -target option")
INCOMPLETE = _diag("Applied changes may be incomplete", "The plan was created with the -target option")
ERROR = _diag("Error: Usage policy ID must not be empty", "with module.data_access...", color=False)
OTHER_WARNING = _diag("Deprecated attribute", "use X instead")


def test_filter_drops_only_the_benign_target_warnings():
    lines = (
        ["Plan: 1 to add, 0 to change, 0 to destroy.\n"]
        + TARGETING
        + ["Apply complete! Resources: 1 added, 0 changed, 0 destroyed.\n"]
        + INCOMPLETE
        + OTHER_WARNING
        + ERROR
    )

    out = "".join(filter_lines(lines))

    assert "Resource targeting is in effect" not in out
    assert "Applied changes may be incomplete" not in out
    assert "Plan: 1 to add" in out and "Apply complete!" in out
    assert "".join(OTHER_WARNING) in out
    assert "".join(ERROR) in out


def test_filter_drops_uncolored_target_warnings():
    lines = (
        _diag("Warning: Resource targeting is in effect", "-target", color=False)
        + ["Apply complete!\n"]
        + _diag("Warning: Applied changes may be incomplete", "-target", color=False)
    )
    assert list(filter_lines(lines)) == ["Apply complete!\n"]


def test_filter_never_swallows_an_unterminated_block():
    assert list(filter_lines(TARGETING[:-1])) == TARGETING[:-1]


def _classification_recipe():
    """The enable-classification apply + message lines, verbatim from Makefile.shared."""
    source = MAKEFILE.read_text()
    start = source.index("enable-classification:")
    body = source[start:source.index("\ngenerate:", start)]
    lines = body.splitlines()
    first = next(i for i, line in enumerate(lines) if "=== Enable UC Data Classification" in line)
    return "\n".join(lines[first:]).rstrip() + "\n"


def _run_recipe(tmp_path, runner_script, env_tfvars):
    env_dir = tmp_path / "envs/dev"
    (env_dir / "data_access").mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text(env_tfvars)
    runner = tmp_path / "runner.sh"
    runner.write_text("#!/usr/bin/env bash\n" + runner_script)
    runner.chmod(0o755)
    makefile = tmp_path / "Makefile"
    makefile.write_text(
        "SHELL := /bin/bash\n"
        f"ENV := dev\nCLOUD_ROOT := {tmp_path}\nENV_DIR := {env_dir}\n"
        f"DATA_ACCESS_SUBDIR := data_access\nROOT_RUNNER := {runner}\nSHARED_ROOT := {SHARED}\n"
        "_SETUP_ENV_REL = $(patsubst $(CLOUD_ROOT)/%,%,$(ENV_DIR))\n"
        "enable-classification:\n" + _classification_recipe()
    )
    env = {k: v for k, v in os.environ.items() if k not in ("MAKEFLAGS", "MAKELEVEL", "ENV")}
    return subprocess.run(
        ["make", "--no-print-directory", "-f", str(makefile), "enable-classification"],
        cwd=tmp_path, text=True, capture_output=True, env=env, timeout=60,
    )


def _printf(lines):
    return "printf '%s' " + " ".join("'" + line.replace("'", "'\\''") + "'" for line in lines) + "\n"


def test_enable_classification_hides_target_warnings_and_points_to_ui(tmp_path):
    result = _run_recipe(
        tmp_path,
        _printf(TARGETING + ["Apply complete! Resources: 1 added.\n"] + INCOMPLETE),
        "enable_auto_tagging = false\n",
    )
    out = result.stdout + result.stderr

    assert result.returncode == 0, out
    assert "Apply complete!" in out
    assert "Resource targeting is in effect" not in out
    assert "Applied changes may be incomplete" not in out
    assert "system.data_classification" not in out
    assert "Catalog Explorer, open your catalog > Data classification > Review detections" in out
    assert "envs/dev/env.auto.tfvars and re-run: make enable-classification ENV=dev" in out


def test_enable_classification_still_surfaces_real_errors_and_fails(tmp_path):
    result = _run_recipe(
        tmp_path,
        _printf(TARGETING) + "{ " + _printf(ERROR) + "} >&2\nexit 1\n",
        "enable_auto_tagging = false\n",
    )
    out = result.stdout + result.stderr

    assert result.returncode != 0, out
    assert "Error: Usage policy ID must not be empty" in out
    assert "Resource targeting is in effect" not in out
    assert "Classification is enabled" not in out


def test_enable_classification_with_auto_tagging_points_to_generate(tmp_path):
    result = _run_recipe(tmp_path, "echo 'Apply complete!'\n", "enable_auto_tagging = true\n")

    assert result.returncode == 0, result.stdout + result.stderr
    assert "Wait for class.* tags to appear in Catalog Explorer, then run:\n  make generate ENV=dev" in result.stdout
