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
USAGE_POLICY_CAPTURE = SHARED / "tests/fixtures/terraform_usage_policy_error.txt"
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
        "  Next steps (the walkthrough's Dev section):\n"
        "    1. In Catalog Explorer, open the catalog > Details tab > Data classification: turn it on.\n"
        "       When the scan finishes, review the detections and exclude any false positives, then turn on\n"
        "       auto-tagging. Wait until the class.* tags appear.\n"
        "       Prefer a script? make enable-classification ENV=dev turns it on (you still review detections in the UI).\n"
        "    2. make generate ENV=dev\n"
        "    3. make rehearse ENV=dev   (picks a verification key automatically)"
    )
    assert "VERIFY_KEY_COLUMN" not in out
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
    [("dev", "make rehearse ENV=dev"), ("prod", "make release ENV=prod")],
)
def test_champion_full_mode_points_to_rehearse_or_release(tmp_path, env_name, next_cmd):
    lines = generate_next_steps(tmp_path, "full", env_name, has_sql=True, champion_flow=True)

    assert lines[-2] == (
        f"    2. git add envs/ && git commit, then: make check ENV={env_name}   (reads committed files only)"
    )
    assert lines[-1].startswith(f"    3. {next_cmd}")
    assert not any("VERIFY_KEY_COLUMN" in line for line in lines)
    assert f"       {tmp_path}/masking_functions.sql" in lines
    assert not any(re.search(r"make apply\b", line) for line in lines)


@pytest.mark.parametrize(("mode", "target"), [("full", "apply"), ("governance", "apply-governance")])
def test_legacy_full_and_governance_keep_validate_then_apply(tmp_path, mode, target):
    lines = generate_next_steps(tmp_path, mode, "dev", has_sql=True, champion_flow=False, tuning=True)
    assert lines[-1].startswith(f"    4. make {target}   ")
    assert any("make validate-generated" in line for line in lines)


@pytest.mark.parametrize(
    ("mode", "env_name", "tail"),
    [
        ("full", "dev", ["    3. make rehearse ENV=dev   (applies, then proves the masks: ALL EFFECTIVE)"]),
        ("governance", "dev", ["    3. make rehearse ENV=dev   (applies, then proves the masks: ALL EFFECTIVE)"]),
        ("full", "prod", [
            "    3. git push (or merge the PR) so the commit is on origin/main",
            "    4. make release-plan ENV=prod   (it reuses the dev rehearsal, or names the command to run)",
            "    5. make release ENV=prod",
        ]),
    ],
)
def test_deterministic_next_steps_match_walkthrough(tmp_path, mode, env_name, tail):
    """A deterministic draft follows the walkthrough: commit, then rehearse or release-plan (both run make check)."""
    lines = generate_next_steps(tmp_path, mode, env_name, has_sql=True, champion_flow=False, tuning=False)

    assert lines[: -len(tail)][-1] == (
        "    2. git add envs/ && git commit   (rehearse and release-plan run make check on the committed files)"
    )
    assert not any("make check ENV=" in line for line in lines)
    assert lines[-len(tail):] == tail
    assert not any("validate-generated" in line for line in lines)
    assert not any(re.search(r"make apply\b", line) for line in lines)
    assert not any("TUNING.md" in line for line in lines)


def test_deterministic_prod_banner_pushes_then_release_plan_reuses_the_source_rehearsal(tmp_path):
    """release-plan needs HEAD on origin/main and a source rehearsal of that commit, which it writes itself
    from the source's earlier rehearsal (or names make rehearse to run); the README's Prod step 3 says the same."""
    lines = generate_next_steps(tmp_path, "full", "prod", has_sql=True, champion_flow=False, tuning=False,
                                source_env="stg")
    text = "\n".join(lines)

    order = ["git commit", "origin/main", "make release-plan ENV=prod", "it reuses the stg rehearsal",
             "make release ENV=prod"]
    positions = [text.index(step) for step in order]
    assert positions == sorted(positions)
    assert "make rehearse" not in text
    readme = (SHARED / "examples/dev_to_prod/README.md").read_text()
    prod = readme[readme.index("## Prod"):readme.index("## Keep it safe")]
    step = prod[prod.index("**3. Plan, release.**"):prod.index("**4. Record evidence.**")]
    block = step[step.index("```bash"):step.index("```", step.index("```bash") + 3)]
    # At a terminal release runs release-plan (and make check) each round: the block is one line.
    assert block.split("\n")[1:] == ["make release ENV=prod", ""]
    flat = " ".join(step.split())
    order = ["plans only a commit on your `origin/main` (or committed local `main` in an option-A local-only repo) that has a passing `make rehearse ENV=dev` of that same commit",
             "release-plan reuses that rehearsal itself and applies nothing in dev",
             "run make rehearse ENV=dev on this commit",
             "run the full `make rehearse ENV=dev`, then `make release ENV=prod` again"]
    positions = [flat.index(step) for step in order]
    assert positions == sorted(positions)


def test_main_passes_promote_from_as_the_banner_source_env():
    source = Path(generate_abac.__file__).read_text()
    main_body = source[source.index("def main():"):source.index("def generate_next_steps(")]
    assert 'source_env=str(auth_cfg.get("promote_from") or "dev")' in main_body


def test_deterministic_genie_import_points_to_classification_then_generate():
    lines = generate_next_steps(Path("/x"), "genie", "dev", has_sql=False, champion_flow=False, tuning=False)

    assert lines == generate_abac.champion_genie_next_steps("dev")
    assert not any("apply-genie" in line for line in lines)


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


COLLIDING = [
    {"name": "Finance & HR", "genie_space_id": "01aaa"},
    {"name": "Finance HR", "genie_space_id": "01bbb"},
]


def _colliding_generated(tmp_path):
    out_dir = tmp_path / "generated"
    out_dir.mkdir()
    (out_dir / "abac.auto.tfvars").write_text(
        'genie_space_configs = {\n  "Finance & HR" = { title = "Finance & HR" }\n'
        '  "Finance HR" = { title = "Finance HR" }\n}\n'
    )
    return out_dir


def _owners(spaces):
    return {
        p.name: re.search(r"# Per-space config for: (.*)", (p / "abac.auto.tfvars").read_text()).group(1)
        for p in spaces.iterdir()
    }


def test_names_that_sanitize_alike_all_get_their_id_suffix(tmp_path):
    out_dir = _colliding_generated(tmp_path)

    bootstrap_per_space_dirs(out_dir, {"genie_spaces": COLLIDING}, "")

    assert _owners(out_dir / "spaces") == {
        "finance_hr--01aaa": "Finance & HR",
        "finance_hr--01bbb": "Finance HR",
    }


def test_reordering_colliding_agents_keeps_each_agents_folder(tmp_path):
    out_dir = _colliding_generated(tmp_path)
    spaces = out_dir / "spaces"

    bootstrap_per_space_dirs(out_dir, {"genie_spaces": COLLIDING}, "")
    first = _owners(spaces)
    (spaces / "finance_hr--01bbb/masking_functions.sql").write_text("-- Finance HR only\n")

    bootstrap_per_space_dirs(out_dir, {"genie_spaces": list(reversed(COLLIDING))}, "")

    assert _owners(spaces) == first
    assert (spaces / "finance_hr--01bbb/masking_functions.sql").read_text() == "-- Finance HR only\n"
    for order in (COLLIDING, list(reversed(COLLIDING))):
        folders, _ = resolve_space_folders(order, {}, spaces)
        assert {f.name: f.key for f in folders} == {
            "Finance & HR": "finance_hr--01aaa", "Finance HR": "finance_hr--01bbb",
        }


def test_colliding_new_agents_get_name_hash_suffixes_independent_of_order(tmp_path):
    new = [{"name": "Finance & HR", "genie_space_id": ""}, {"name": "Finance HR", "genie_space_id": ""}]

    keys = [
        {f.name: f.key for f in resolve_space_folders(order, {}, tmp_path / "spaces")[0]}
        for order in (new, list(reversed(new)))
    ]

    assert keys[0] == keys[1]
    assert len(set(keys[0].values())) == 2
    assert all(re.fullmatch(r"finance_hr--new_[0-9a-f]{8}", key) for key in keys[0].values())


def test_shared_bare_folder_stub_is_removed_but_real_content_stops(tmp_path, capsys):
    out_dir = _colliding_generated(tmp_path)
    spaces = out_dir / "spaces"
    _stub(spaces / "finance_hr", "Finance & HR")  # what bde28b6 bootstrapped for the first agent

    bootstrap_per_space_dirs(out_dir, {"genie_spaces": COLLIDING}, "")
    assert sorted(p.name for p in spaces.iterdir()) == ["finance_hr--01aaa", "finance_hr--01bbb"]

    _real(spaces / "finance_hr")
    with pytest.raises(SystemExit):
        bootstrap_per_space_dirs(out_dir, {"genie_spaces": COLLIDING}, "")
    out = capsys.readouterr().out
    assert (
        "generated/spaces/finance_hr/ has per-agent content but is shared by "
        "'Finance & HR' (01aaa), 'Finance HR' (01bbb). Each now gets its own folder "
        "(generated/spaces/finance_hr--01aaa/, generated/spaces/finance_hr--01bbb/)."
    ) in out
    assert (spaces / "finance_hr/masking_functions.sql").exists()


# An agent's collision folder name changes when it is created (--new_<hash>
# -> --<id>) and when its group shrinks to one (--<id> -> bare key). The old
# folder follows the same rules as the raw-ID folder of an imported agent.

NEW_THEN_CREATED = (
    [{"name": "Finance & HR", "genie_space_id": ""}, {"name": "Finance HR", "genie_space_id": "01bbb"}],
    [{"name": "Finance & HR", "genie_space_id": "01aaa"}, {"name": "Finance HR", "genie_space_id": "01bbb"}],
)
GROUP_THEN_SINGLETON = (COLLIDING, COLLIDING[:1])


def _new_key():
    return "finance_hr--new_" + generate_abac._name_hash("Finance & HR")


LIFECYCLE = {
    # transition: (configs before/after, old folder, new folder)
    "new_hash_to_id": (NEW_THEN_CREATED, _new_key(), "finance_hr--01aaa"),
    "group_to_singleton": (GROUP_THEN_SINGLETON, "finance_hr--01aaa", "finance_hr"),
}


def _bootstrap_before(tmp_path, transition):
    (before, after), old, _new = LIFECYCLE[transition]
    out_dir = _colliding_generated(tmp_path)
    bootstrap_per_space_dirs(out_dir, {"genie_spaces": before}, "")
    assert (out_dir / "spaces" / old / "abac.auto.tfvars").exists()
    # The next full generation only emits configs for the agents still listed.
    (out_dir / "abac.auto.tfvars").write_text(
        "genie_space_configs = {\n"
        + "".join(f'  "{sp["name"]}" = {{ title = "{sp["name"]}" }}\n' for sp in after)
        + "}\n"
    )
    return out_dir


def _folder_of(spaces, name, spaces_dir):
    folders, notes = resolve_space_folders(spaces, {}, spaces_dir)
    return next(f.key for f in folders if f.name == name), notes


@pytest.mark.parametrize("transition", LIFECYCLE)
def test_placeholder_only_old_collision_folder_is_removed(tmp_path, transition):
    (_before, after), old, new = LIFECYCLE[transition]
    out_dir = _bootstrap_before(tmp_path, transition)
    spaces = out_dir / "spaces"

    bootstrap_per_space_dirs(out_dir, {"genie_spaces": after}, "")

    assert not (spaces / old).exists()
    assert _owners(spaces)[new] == "Finance & HR"
    assert _folder_of(after, "Finance & HR", spaces) == (new, [])


@pytest.mark.parametrize("transition", LIFECYCLE)
def test_old_collision_folder_with_real_content_is_kept(tmp_path, transition, capsys):
    (_before, after), old, new = LIFECYCLE[transition]
    out_dir = _bootstrap_before(tmp_path, transition)
    spaces = out_dir / "spaces"
    _real(spaces / old)
    capsys.readouterr()

    bootstrap_per_space_dirs(out_dir, {"genie_spaces": after}, "")

    assert not (spaces / new).exists()
    assert (spaces / old / "masking_functions.sql").read_text() == "-- customized\n"
    assert _owners(spaces)[old] == "Finance & HR"
    note = f"keeping generated/spaces/{old}/ for 'Finance & HR' (it has per-agent content)."
    assert f"NOTE: {note}" in capsys.readouterr().out
    # make generate SPACE= picks the same folder.
    assert _folder_of(after, "Finance & HR", spaces) == (old, [note])


@pytest.mark.parametrize("transition", LIFECYCLE)
def test_old_and_new_collision_folders_with_real_content_stop_writing_nothing(tmp_path, transition, capsys):
    (_before, after), old, new = LIFECYCLE[transition]
    out_dir = _bootstrap_before(tmp_path, transition)
    spaces = out_dir / "spaces"
    _real(spaces / old)
    _real(spaces / new)
    before = {p: p.read_text() for p in spaces.rglob("*") if p.is_file()}
    expected = (
        f"generated/spaces/{old}/ and generated/spaces/{new}/ both hold per-agent content for "
        f"'Finance & HR' (01aaa). Keep generated/spaces/{new}/: move or merge anything you still "
        f"need from generated/spaces/{old}/ into it, delete generated/spaces/{old}/, then re-run "
        "make generate."
    )

    # The pre-model-call check in main() raises the same error...
    with pytest.raises(SpaceFolderError) as exc:
        resolve_space_folders(after, {}, spaces)
    assert str(exc.value) == expected
    # ...and the bootstrap writes and deletes nothing.
    with pytest.raises(SystemExit):
        bootstrap_per_space_dirs(out_dir, {"genie_spaces": after}, "")
    assert expected in capsys.readouterr().out
    assert {p: p.read_text() for p in spaces.rglob("*") if p.is_file()} == before


# Ownership is global: a folder that more than one agent could claim (as its
# current name or an earlier one) is never adopted or cleaned up by guesswork.

def _snapshot(spaces):
    return {p: p.read_text() for p in spaces.rglob("*") if p.is_file()}


def _bootstrap(tmp_path, genie_spaces):
    out_dir = tmp_path / "generated"
    out_dir.mkdir(exist_ok=True)
    (out_dir / "abac.auto.tfvars").write_text(
        "genie_space_configs = {\n"
        + "".join(f'  "{sp["name"]}" = {{ title = "{sp["name"]}" }}\n' for sp in genie_spaces)
        + "}\n"
    )
    bootstrap_per_space_dirs(out_dir, {"genie_spaces": genie_spaces}, "")
    return out_dir / "spaces"


def test_real_content_in_a_shared_bare_folder_is_adopted_by_no_agent(tmp_path, capsys):
    spaces = tmp_path / "generated" / "spaces"
    _real(spaces / "finance_hr")
    before = _snapshot(spaces)

    with pytest.raises(SpaceFolderError) as exc:
        resolve_space_folders(COLLIDING, {}, spaces)
    assert "generated/spaces/finance_hr/ has per-agent content but is shared by" in str(exc.value)
    with pytest.raises(SystemExit):
        _bootstrap(tmp_path, COLLIDING)
    assert _snapshot(spaces) == before


# Agent A's raw ID sanitizes to B's current (bare) folder name.
ID_IS_OTHER_NAME = [
    {"name": "Finance", "genie_space_id": "01abc"},
    {"name": "01abc", "genie_space_id": ""},
]


def test_raw_id_equal_to_another_agents_folder_with_real_content_stops(tmp_path, capsys):
    spaces = tmp_path / "generated" / "spaces"
    _real(spaces / "01abc")
    before = _snapshot(spaces)

    with pytest.raises(SpaceFolderError) as exc:
        resolve_space_folders(ID_IS_OTHER_NAME, {}, spaces)
    assert str(exc.value) == (
        "generated/spaces/01abc/ has per-agent content and is the folder for '01abc' (new), "
        "but it is also an earlier folder name for 'Finance' (01abc), so it is unclear whose "
        "content it is. If it belongs to 'Finance', move it to generated/spaces/finance/. "
        "If it belongs to '01abc', give that agent a distinct name (name = \"...\") in "
        "env.auto.tfvars and rename the folder to match. Then re-run make generate."
    )
    with pytest.raises(SystemExit):
        _bootstrap(tmp_path, ID_IS_OTHER_NAME)
    assert _snapshot(spaces) == before


def test_raw_id_stub_that_is_another_agents_folder_is_not_removed(tmp_path):
    spaces = tmp_path / "generated" / "spaces"
    _stub(spaces / "01abc", "01abc")   # A's pre-title raw-ID name, and B's folder

    folders, _ = resolve_space_folders(ID_IS_OTHER_NAME, {}, spaces)
    assert {f.name: f.key for f in folders} == {"Finance": "finance", "01abc": "01abc"}
    assert all("01abc" not in f.stale_keys for f in folders)

    _bootstrap(tmp_path, ID_IS_OTHER_NAME)
    assert _owners(spaces) == {"finance": "Finance", "01abc": "01abc"}


# Two genie_spaces entries with the same genie_space_id in a collision group
# both resolve to "<key>--<id>".
SAME_ID = [
    {"name": "Finance & HR", "genie_space_id": "01aaa"},
    {"name": "Finance HR", "genie_space_id": "01aaa"},
]


def test_id_suffixed_folder_claimed_by_two_agents_stops(tmp_path):
    spaces = tmp_path / "generated" / "spaces"
    with pytest.raises(SpaceFolderError) as exc:
        resolve_space_folders(SAME_ID, {}, spaces)
    assert str(exc.value).startswith(
        "Genie agents 'Finance & HR' (01aaa) and 'Finance HR' (01aaa) would share "
        "generated/spaces/finance_hr--01aaa/."
    )

    _real(spaces / "finance_hr--01aaa")
    with pytest.raises(SpaceFolderError) as exc:
        resolve_space_folders(SAME_ID, {}, spaces)
    assert "generated/spaces/finance_hr--01aaa/ has per-agent content and is the folder for" in str(exc.value)


def test_new_hash_folder_claimed_by_two_agents_stops(tmp_path, monkeypatch):
    # Name hashes of distinct names collide only in theory; force it.
    monkeypatch.setattr(generate_abac, "_name_hash", lambda name: "deadbeef")
    new = [{"name": "Finance & HR", "genie_space_id": ""}, {"name": "Finance HR", "genie_space_id": ""}]

    with pytest.raises(SpaceFolderError) as exc:
        resolve_space_folders(new, {}, tmp_path / "spaces")
    assert "would share generated/spaces/finance_hr--new_deadbeef/" in str(exc.value)


def test_shared_stub_is_removed_only_for_the_agent_it_was_written_for(tmp_path):
    spaces = tmp_path / "generated" / "spaces"
    _stub(spaces / "finance_hr", "Someone Else")   # claimed by both, owned by neither

    folders, _ = resolve_space_folders(COLLIDING, {}, spaces)
    assert all("finance_hr" not in f.stale_keys for f in folders)
    _bootstrap(tmp_path, COLLIDING)
    assert (spaces / "finance_hr" / "abac.auto.tfvars").exists()

    (spaces / "finance_hr" / "abac.auto.tfvars").unlink()
    (spaces / "finance_hr").rmdir()
    _stub(spaces / "finance_hr", "Finance HR")
    folders, _ = resolve_space_folders(COLLIDING, {}, spaces)
    assert {f.name: "finance_hr" in f.stale_keys for f in folders} == {
        "Finance & HR": False, "Finance HR": True,
    }
    _bootstrap(tmp_path, COLLIDING)
    assert not (spaces / "finance_hr").exists()


def test_unrelated_agents_resolve_exactly_as_before(tmp_path):
    spaces = tmp_path / "generated" / "spaces"
    plain = [{"name": "Sales", "genie_space_id": "01s"}, {"name": "HR", "genie_space_id": ""}]
    _stub(spaces / "01s", "01s")

    folders, notes = resolve_space_folders(plain, {}, spaces)

    assert {f.name: (f.key, f.stale_keys) for f in folders} == {
        "Sales": ("sales", ["01s", "sales--new_" + generate_abac._name_hash("Sales"), "sales--01s"]),
        "HR": ("hr", ["hr--new_" + generate_abac._name_hash("HR")]),
    }
    assert notes == []


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
ERROR = USAGE_POLICY_CAPTURE.read_text().splitlines(keepends=True)
OTHER_WARNING = ["╷\n", "│ Warning: Deprecated attribute\n", "│ \n", "│ use X instead\n", "╵\n"]
USAGE_POLICY_ADVICE = (
    "Enable classification in the UI, or create and attach a serverless usage policy: "
    "https://docs.databricks.com/aws/en/admin/usage/budget-policies"
)


def test_filter_drops_only_the_real_target_warnings():
    out = "".join(filter_lines(CAPTURE + OTHER_WARNING + ERROR))

    assert out == "".join(OUTSIDE + OTHER_WARNING + ERROR) + f"\n{USAGE_POLICY_ADVICE}\n"
    assert "Apply complete! Resources: 1 added" in out


def test_filter_matches_uncolored_and_rewrapped_text():
    plain = [re.sub(r"\x1b\[[0-9;]*m", "", line) for line in TARGETING]
    words = " ".join(l.strip().lstrip("│").strip() for l in plain[1:-1]).split()
    rewrapped = ["╷\n"] + [f"│ {' '.join(words[i:i + 7])}\n" for i in range(0, len(words), 7)] + ["╵\n"]
    assert list(filter_lines(plain + rewrapped)) == []


def test_filter_keeps_a_warning_box_with_an_interleaved_error():
    box = TARGETING[:3] + ["Error: another failure\n"] + TARGETING[3:]
    assert list(filter_lines(box)) == box


def test_filter_keeps_a_known_title_with_unknown_body():
    box = TARGETING[:2] + ["│ Something else Terraform added.\n"] + TARGETING[-1:]
    assert list(filter_lines(box)) == box


def test_filter_keeps_an_unknown_warning():
    assert list(filter_lines(OTHER_WARNING)) == OTHER_WARNING


def test_filter_emits_an_unterminated_box_at_eof_verbatim():
    assert list(filter_lines(TARGETING[:-1])) == TARGETING[:-1]


def test_classification_filter_hides_only_successful_outputs_block():
    lines = ["Apply complete! Resources: 1 added.\n", "\n", "Outputs:\n",
             "classification = { noisy = true }\n"]
    assert list(filter_lines(lines, hide_outputs=True)) == lines[:2]
    assert list(filter_lines(lines, hide_outputs=False)) == lines


def test_generated_remap_can_suppress_duplicate_success_line(tmp_path):
    source_abac = tmp_path / "source.tfvars"
    source_sql = tmp_path / "source.sql"
    out_abac = tmp_path / "out.tfvars"
    out_sql = tmp_path / "out.sql"
    source_abac.write_text('uc_tables = ["dev.sales.orders"]\ntag_assignments = []\n')
    source_sql.write_text("USE CATALOG dev;\n")

    result = subprocess.run(
        [sys.executable, str(SHARED / "scripts/remap_generated_config.py"),
         str(source_abac), str(source_sql), str(out_abac), str(out_sql),
         "--map", "dev=prod", "--quiet-remaps"],
        text=True, capture_output=True,
    )

    assert result.returncode == 0, result.stderr
    assert "Catalog remap:" not in result.stdout
    assert "prod.sales.orders" in out_abac.read_text()


def test_filter_script_streams_unbuffered():
    source = FILTER.read_text()
    assert "sys.stdout.flush()" in source
    assert 'python3 -u "$(SHARED_ROOT)/scripts/filter_target_warnings.py"' in MAKEFILE.read_text()
    assert '--cloud "$(CLOUD)"' in MAKEFILE.read_text()


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
         [ -z "$FAKE_TF_BREAK" ] || printf 'broken = "s3cr3t-token-value" {\\n' >> "$FAKE_TF_BREAK"
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
        if os.environ.get("REQUIRE_TERRAFORM_TESTS") == "1":
            pytest.fail("REQUIRE_TERRAFORM_TESTS=1 but make is not installed")
        pytest.skip("make not installed")
    cloud_root = tmp_path / "aws"
    cloud_root.mkdir()
    args = [f"CLOUD_ROOT={cloud_root}", f"SHARED_ROOT={SHARED}"]
    setup = subprocess.run(["make", "--no-print-directory", "setup", "ENV=dev", *args],
                           cwd=REPO / "aws", text=True, capture_output=True, env=_clean_env())
    assert setup.returncode == 0, setup.stdout + setup.stderr
    (cloud_root / "envs/dev/env.auto.tfvars").write_text(
        TEMPLATE.read_text().replace("<your-genie-space-id>", SPACE_ID)
        .replace("<your-deployer-principal-id>", "d0000000-0000-0000-0000-00000000d001")
        .replace("<most-privileged-group>", "ops")
        .replace("<least-privileged-group>", "viewers")
        + '\nuc_tables = ["dev_finance.genierails_e2e.customers"]\n'
    )
    (cloud_root / "envs/dev/auth.auto.tfvars").write_text(
        'databricks_workspace_host = "https://selected.example"\n'
        'databricks_client_id = "selected-client"\n'
        'databricks_client_secret = "selected-secret"\n'
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    (fake_bin / "terraform").write_text(FAKE_TERRAFORM)
    (fake_bin / "terraform").chmod(0o755)
    sdk = tmp_path / "sdk/databricks/sdk"
    sdk.mkdir(parents=True)
    (sdk.parent / "__init__.py").write_text("")
    (sdk / "__init__.py").write_text(FAKE_SDK)
    (sdk / "config.py").write_text(
        "class Config:\n"
        "    def __init__(self, **kwargs):\n"
        "        self.__dict__.update(kwargs)\n"
    )
    (sdk / "errors.py").write_text("class NotFound(Exception):\n    pass\n")

    def run(env_name="dev", **fake):
        env = _clean_env()
        env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
        env["PYTHONPATH"] = str(tmp_path / "sdk")
        env.update({"FAKE_TF_STDOUT": str(TF_CAPTURE), **fake})
        return subprocess.run(
            ["make", "--no-print-directory", "enable-classification", f"ENV={env_name}", *args],
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
    assert "auto-tagging follows the current UI setting" in out
    assert "review detections and exclude false positives" in out
    assert "nothing is tagged yet" not in out


def test_real_target_surfaces_terraform_errors_and_fails(real_cloud):
    result = real_cloud(FAKE_TF_STDERR=USAGE_POLICY_CAPTURE.read_text(), FAKE_TF_EXIT="1")
    out = result.stdout + result.stderr
    plain_out = re.sub(r"\x1b\[[0-9;]*m", "", out)

    assert result.returncode != 0, out
    assert "Error: failed to create data_classification_catalog_config" in plain_out
    assert "Usage policy ID must not be empty." in plain_out
    assert USAGE_POLICY_ADVICE in out
    assert "Resource targeting is in effect" not in out
    assert "Classification is enabled" not in out


def test_real_target_with_auto_tagging_points_to_generate(real_cloud, tmp_path):
    env_file = tmp_path / "aws/envs/dev/env.auto.tfvars"
    env_file.write_text(env_file.read_text() + "\nenable_auto_tagging = true\n")

    result = real_cloud()

    assert result.returncode == 0, result.stdout + result.stderr
    assert "Wait for class.* tags to appear in Catalog Explorer, then run:\n  make generate ENV=dev" in result.stdout


def test_real_target_refuses_ambient_credentials_without_env_auth(real_cloud, tmp_path):
    auth_file = tmp_path / "aws/envs/dev/auth.auto.tfvars"
    auth_file.write_text('databricks_workspace_host = ""\n')

    result = real_cloud(
        DATABRICKS_HOST="https://ambient.example",
        DATABRICKS_CLIENT_ID="ambient-id",
        DATABRICKS_CLIENT_SECRET="ambient-secret",
    )
    out = result.stdout + result.stderr

    assert result.returncode != 0, out
    assert f"fill in {auth_file}" in out
    assert "Apply complete!" not in out


def _auto_tagging_env(tmp_path, name, *, promote_from=None, mode="deterministic"):
    """A copy of the fixture's dev env as `name`, with auto-tagging on."""
    envs = tmp_path / "aws/envs"
    if name != "dev":
        shutil.copytree(envs / "dev", envs / name)
    env_file = envs / name / "env.auto.tfvars"
    text = env_file.read_text().replace('governance_mode = "deterministic"', f'governance_mode = "{mode}"')
    if promote_from:
        text += f'\npromote_from = "{promote_from}"\n'
    env_file.write_text(text + "\nenable_auto_tagging = true\n")


# Finding 27b: the hint follows env-role (prod, or any env with promote_from, is
# promoted) and the governance mode, not the literal name "prod".
@pytest.mark.parametrize("name, promote_from, mode, expected, absent", [
    ("prod", None, "deterministic",
     "  make release-plan ENV=prod   (after make rehearse ENV=dev on the same commit)\n"
     "  Get that manifest approved, then: make release ENV=prod\n", "make generate"),
    ("stg", "dev", "deterministic",
     "  make release-plan ENV=stg   (after make rehearse ENV=dev on the same commit)\n"
     "  Get that manifest approved, then: make release ENV=stg\n", "make generate"),
    ("prod", "stg", "deterministic",
     "  make release-plan ENV=prod   (after make rehearse ENV=stg on the same commit)\n", "make rehearse ENV=dev"),
    ("stg", "dev", "legacy", "then run:\n  make release ENV=stg\n", "generate"),
], ids=["prod", "stg-promoted", "prod-from-stg", "stg-promoted-legacy"])
def test_real_target_auto_tagging_hint_follows_env_role_and_mode(real_cloud, tmp_path, name, promote_from, mode,
                                                                  expected, absent):
    _auto_tagging_env(tmp_path, name, promote_from=promote_from, mode=mode)

    result = real_cloud(env_name=name)

    out = result.stdout
    assert result.returncode == 0, out + result.stderr
    assert "Wait for class.* tags to appear in Catalog Explorer, then run:\n" in out
    assert expected in out
    assert absent not in out[out.index("then run:"):]


# The placeholder guard parses every env file (#240), <...> in it or not, so it
# reports a malformed one before the classification validator runs.
@pytest.mark.parametrize("keep_template", [True, False], ids=["with-placeholder", "without-placeholder"])
def test_real_target_with_a_malformed_env_file_refuses_clearly_before_applying(real_cloud, tmp_path, keep_template):
    """Nothing can be enabled without a readable config: refuse in one line, no traceback or file contents."""
    env_file = tmp_path / "aws/envs/dev/env.auto.tfvars"
    base = env_file.read_text() if keep_template else 'governance_mode = "deterministic"\nenable_classification = true\n'
    assert ("<" in base) is keep_template
    env_file.write_text(base + '\nnote = "s3cr3t-token-value" {\n')

    result = real_cloud()

    out = result.stdout + result.stderr
    assert result.returncode != 0, out
    assert "ERROR: fix envs/dev/env.auto.tfvars: it is not valid HCL\n" in result.stderr
    assert "Traceback" not in out and "UnexpectedToken" not in out
    assert "s3cr3t" not in out
    assert "Apply complete" not in out


def test_classification_validator_refuses_a_malformed_env_file_in_one_line(tmp_path):
    """Run on its own (the guard normally reports first), it names the file and never quotes it."""
    env_file = tmp_path / "envs/dev/env.auto.tfvars"
    env_file.parent.mkdir(parents=True)
    env_file.write_text('governance_mode = "deterministic"\nenable_classification = true\n'
                        'note = "s3cr3t-token-value" {\n')

    result = subprocess.run([sys.executable, str(SHARED / "scripts/validate_classification_config.py"),
                             str(env_file)], capture_output=True, text=True)

    assert result.returncode == 1, result.stdout + result.stderr
    assert result.stdout == ""
    assert result.stderr == "ERROR: envs/dev/env.auto.tfvars is not valid HCL; fix it, then re-run this command\n"


def test_real_target_hint_falls_back_when_env_role_fails_after_the_apply(real_cloud, tmp_path):
    """The apply succeeded; if env-role then fails, the hint suggests only commands safe in every role and mode."""
    _auto_tagging_env(tmp_path, "prod")
    env_file = tmp_path / "aws/envs/prod/env.auto.tfvars"

    result = real_cloud(env_name="prod", FAKE_TF_BREAK=str(env_file))

    out = result.stdout + result.stderr
    assert result.returncode == 0, out
    assert "Apply complete! Resources: 1 added" in out
    assert ("WARNING: could not tell whether envs/prod is a rehearsal or a promoted env (env-role failed; "
            "is envs/prod/env.auto.tfvars valid HCL?).\n") in result.stderr
    hint = result.stdout[result.stdout.index("Automatic tagging is on."):]
    assert hint == ("Automatic tagging is on. Wait for class.* tags to appear in Catalog Explorer. Then fix "
                    "envs/prod/env.auto.tfvars\nso it parses, and run make setup ENV=prod for this env's next step "
                    "(make check ENV=prod checks the committed files).\n")
    assert "Traceback" not in out and "env-role failed:" not in out and "s3cr3t" not in out
