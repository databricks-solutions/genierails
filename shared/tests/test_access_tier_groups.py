"""access_tier_groups: the group->tier mapping is entered once and reused.

Covers the generate precedence (--groups > access_tier_groups > account
auto-load > error) in every mode, auto-persist on first use, the mismatch
warning, promotion to prod, and the template/docs/Terraform contract.
"""

import sys
from pathlib import Path

import hcl2
import pytest

import generate_abac
from access_tier_groups import (
    persist_access_tier_groups,
    persisted_access_tier_groups,
    promoted_lines,
)
from genie_space_placeholder import PLACEHOLDER
from scripts import remap_env_config


SHARED = Path(__file__).parents[1]
TEMPLATE = SHARED / "examples/dev_to_prod/env.auto.tfvars.example"
AGENT_ID = "01ef7b3c2a4d5e6f"
TIERS = ["payments_ops", "regional_analysts", "viewers"]
SETTING_LOCATION = "access_tier_groups in envs/dev/env.auto.tfvars"
MODES = {
    "full": [],
    "genie": ["--mode", "genie"],
    "governance": ["--mode", "governance"],
    "space": ["--space", AGENT_ID],
}


class _Stop(Exception):
    """Raised by the first table fetch: group resolution is done by then."""


def _dev_env(tmp_path, groups=None, genie_space_id=AGENT_ID):
    env_dir = tmp_path / "envs" / "dev"
    (env_dir / "data_access").mkdir(parents=True)
    # Most tests in this module exercise the legacy group-resolution contract,
    # with the agent inline as legacy envs keep it; the template's deterministic
    # defaults (agent files) have their own contract tests.
    text = TEMPLATE.read_text().replace(
        "genie_spaces = []", f'genie_spaces = [\n  {{ genie_space_id = "{genie_space_id}" }},\n]')
    text = text.replace('governance_mode = "deterministic"\n', "")
    text = text.replace('deployer_principal_id = "<your-deployer-principal-id>"\n', "")
    text = text.replace(
        'access_tier_groups = ["<most-privileged-group>", "<least-privileged-group>"]',
        "access_tier_groups = []",
    )
    if groups is not None:
        text = text.replace("access_tier_groups = []", groups)
    (env_dir / "env.auto.tfvars").write_text(text)
    (env_dir / "auth.auto.tfvars").write_text(
        'databricks_workspace_host = "https://example.invalid"\n'
        'databricks_client_id = "client"\ndatabricks_client_secret = "secret"\n'
    )
    return env_dir


def _run_generate(monkeypatch, env_dir, *args, account_groups=None, idp=None):
    """Run generate_abac.main() up to the first table fetch.

    Returns (exit_code, preflighted group names or None).
    """
    preflighted = []
    monkeypatch.chdir(env_dir)
    monkeypatch.setattr(
        generate_abac, "load_groups_from_account_config",
        lambda **k: list(account_groups or []),
    )
    monkeypatch.setattr(
        generate_abac, "list_account_group_names",
        lambda *a, **k: list(idp) if idp is not None else None,
    )
    real_preflight = generate_abac.preflight_consume_groups

    def record_preflight(referenced, existing, **kwargs):
        preflighted.append(list(referenced))
        return real_preflight(referenced, existing, **kwargs)

    monkeypatch.setattr(generate_abac, "preflight_consume_groups", record_preflight)

    def stop(*a, **k):
        raise _Stop()

    monkeypatch.setattr(generate_abac, "fetch_tables_from_genie_space", stop)
    monkeypatch.setattr(generate_abac, "fetch_tables_from_databricks", stop)
    monkeypatch.setattr(sys, "argv", [
        "generate_abac.py", "--auth-file", str(env_dir / "auth.auto.tfvars"), *args,
    ])
    try:
        generate_abac.main()
    except _Stop:
        return 0, (preflighted[0] if preflighted else None)
    except SystemExit as exc:
        return exc.code, (preflighted[0] if preflighted else None)
    raise AssertionError("generate_abac.main() returned before fetching tables")


def _saved(env_dir):
    return hcl2.loads((env_dir / "env.auto.tfvars").read_text()).get("access_tier_groups")


# ── helper module ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("config", [{}, {"access_tier_groups": None}, {"access_tier_groups": []}])
def test_unset_or_empty_setting_reads_as_no_groups(config, tmp_path):
    assert persisted_access_tier_groups(config, tmp_path / "env.auto.tfvars") == []


@pytest.mark.parametrize("value", ["payments_ops", ["ok", ""], ["ok", 3], ["a", "b", "a"]])
def test_malformed_setting_is_rejected(value, tmp_path):
    with pytest.raises(ValueError, match="access_tier_groups"):
        persisted_access_tier_groups({"access_tier_groups": value}, tmp_path / "env.auto.tfvars")


def test_persist_fills_the_template_line_in_place(tmp_path):
    env_dir = _dev_env(tmp_path)
    before = (env_dir / "env.auto.tfvars").read_text()

    persist_access_tier_groups(env_dir / "env.auto.tfvars", TIERS)

    after = (env_dir / "env.auto.tfvars").read_text()
    assert after == before.replace(
        "access_tier_groups = []",
        'access_tier_groups = ["payments_ops", "regional_analysts", "viewers"]',
    )


def test_persist_replaces_all_placeholder_groups_in_the_champion_template(tmp_path):
    path = tmp_path / "env.auto.tfvars"
    path.write_text(TEMPLATE.read_text())

    persist_access_tier_groups(path, TIERS)

    cfg = hcl2.loads(path.read_text())
    assert cfg["access_tier_groups"] == TIERS
    assert "<most-privileged-group>" not in path.read_text()


def _persist(tmp_path, text, groups=TIERS):
    path = tmp_path / "env.auto.tfvars"
    path.write_text(text)
    persist_access_tier_groups(path, groups)
    return path.read_text()


def test_persist_appends_once_when_absent(tmp_path):
    text = (
        '# access_tier_groups = ["commented"]\n'
        'genie_spaces = [{ name = "access_tier_groups = [x]" }]\n'
        'notes = <<EOT\naccess_tier_groups = [\nEOT\n'
    )
    out = _persist(tmp_path, text)

    assert out == text + (
        "\n# Access-tier groups, most to least privileged (saved by make generate).\n"
        'access_tier_groups = ["payments_ops", "regional_analysts", "viewers"]\n'
    )
    assert hcl2.loads(out)["access_tier_groups"] == TIERS
    assert out.count("\naccess_tier_groups = [\"") == 1
    # Idempotent: saving the same groups again changes nothing.
    persist_access_tier_groups(tmp_path / "env.auto.tfvars", TIERS)
    assert (tmp_path / "env.auto.tfvars").read_text() == out


def test_persist_multiline_list_keeps_a_hash_comment_containing_a_bracket(tmp_path):
    text = 'a = 1\naccess_tier_groups = [\n  # old tiers ] were here\n]\nb = "x ]"\n'
    out = _persist(tmp_path, text)

    assert out == (
        'a = 1\naccess_tier_groups = [\n  # old tiers ] were here\n'
        '  "payments_ops", "regional_analysts", "viewers",\n]\nb = "x ]"\n'
    )
    assert hcl2.loads(out) == {"a": 1, "access_tier_groups": TIERS, "b": "x ]"}


@pytest.mark.parametrize("text, kept", [
    ('access_tier_groups = [] // trailing ] note\nc = 2\n', "// trailing ] note\nc = 2\n"),
    ('access_tier_groups = [] /* block ] */\n/* ] */ c = 2\n', "/* block ] */\n/* ] */ c = 2\n"),
    ('access_tier_groups = null # unset ]\nc = 2\n', "# unset ]\nc = 2\n"),
])
def test_persist_preserves_slash_and_block_comments(text, kept, tmp_path):
    out = _persist(tmp_path, text)

    assert out == 'access_tier_groups = ["payments_ops", "regional_analysts", "viewers"] ' + kept
    assert hcl2.loads(out) == {"access_tier_groups": TIERS, "c": 2}


def test_persist_handles_group_names_containing_brackets(tmp_path):
    groups = ["ops]team", "[viewers]"]
    path = tmp_path / "env.auto.tfvars"
    path.write_text("access_tier_groups = []\nz = 1\n")
    persist_access_tier_groups(path, groups)
    persist_access_tier_groups(path, groups)  # idempotent

    out = path.read_text()
    assert out == 'access_tier_groups = ["ops]team", "[viewers]"]\nz = 1\n'
    assert hcl2.loads(out) == {"access_tier_groups": groups, "z": 1}


def test_persist_refuses_to_overwrite_a_different_saved_value(tmp_path):
    path = tmp_path / "env.auto.tfvars"
    path.write_text('access_tier_groups = ["payments_ops"]\n')
    with pytest.raises(ValueError, match="already set"):
        persist_access_tier_groups(path, ["viewers"])
    assert path.read_text() == 'access_tier_groups = ["payments_ops"]\n'


# ── generate precedence, every mode ──────────────────────────────────────────

@pytest.mark.parametrize("mode_args", MODES.values(), ids=MODES.keys())
def test_every_mode_uses_persisted_groups_over_account_autoload(
    mode_args, tmp_path, monkeypatch, capsys
):
    env_dir = _dev_env(
        tmp_path, 'access_tier_groups = ["payments_ops", "regional_analysts", "viewers"]'
    )
    code, preflighted = _run_generate(
        monkeypatch, env_dir, *mode_args, account_groups=["acct_only"], idp=TIERS
    )
    out = capsys.readouterr().out

    assert code == 0, out
    assert preflighted == TIERS
    assert f"Groups:   payments_ops, regional_analysts, viewers ({SETTING_LOCATION})" in out
    assert "WARNING" not in out


@pytest.mark.parametrize("mode_args", MODES.values(), ids=MODES.keys())
def test_first_explicit_groups_are_saved_then_reused(mode_args, tmp_path, monkeypatch, capsys):
    env_dir = _dev_env(tmp_path)
    code, preflighted = _run_generate(
        monkeypatch, env_dir, *mode_args, "--groups", ",".join(TIERS), idp=TIERS
    )
    out = capsys.readouterr().out

    assert code == 0, out
    assert preflighted == TIERS
    assert "IdP preflight: all 3 referenced group(s) found" in out
    assert _saved(env_dir) == TIERS
    assert f"Saved --groups to {SETTING_LOCATION}; later runs can omit --groups." in out

    # The next run, without --groups, in any mode, picks the saved tiers up.
    for args in MODES.values():
        code, preflighted = _run_generate(monkeypatch, env_dir, *args, idp=TIERS)
        assert code == 0
        assert preflighted == TIERS


def test_differing_groups_win_for_one_run_and_never_rewrite_the_setting(
    tmp_path, monkeypatch, capsys
):
    env_dir = _dev_env(
        tmp_path, 'access_tier_groups = ["payments_ops", "regional_analysts", "viewers"]'
    )
    before = (env_dir / "env.auto.tfvars").read_text()
    reordered = ["viewers", "regional_analysts", "payments_ops"]

    code, preflighted = _run_generate(
        monkeypatch, env_dir, "--groups", ",".join(reordered), idp=TIERS
    )
    out = capsys.readouterr().out

    assert code == 0, out
    assert preflighted == reordered
    assert (env_dir / "env.auto.tfvars").read_text() == before
    assert "WARNING: --groups (viewers, regional_analysts, payments_ops) differs from" in out
    assert "using --groups for this run only" in out
    assert "edit access_tier_groups" in out
    assert "Saved --groups" not in out


def test_matching_groups_neither_warn_nor_rewrite(tmp_path, monkeypatch, capsys):
    env_dir = _dev_env(
        tmp_path, 'access_tier_groups = ["payments_ops", "regional_analysts", "viewers"]'
    )
    before = (env_dir / "env.auto.tfvars").read_text()
    code, _ = _run_generate(monkeypatch, env_dir, "--groups", " , ".join(TIERS), idp=TIERS)
    out = capsys.readouterr().out

    assert code == 0, out
    assert "WARNING" not in out and "Saved --groups" not in out
    assert (env_dir / "env.auto.tfvars").read_text() == before


def test_groups_failing_idp_preflight_are_not_saved(tmp_path, monkeypatch, capsys):
    env_dir = _dev_env(tmp_path)
    code, _ = _run_generate(monkeypatch, env_dir, "--groups", "Ghost_Group", idp=TIERS)

    assert code == 1
    assert _saved(env_dir) == []


def test_groups_are_used_but_not_saved_when_idp_preflight_is_skipped(
    tmp_path, monkeypatch, capsys
):
    env_dir = _dev_env(tmp_path)
    code, _ = _run_generate(monkeypatch, env_dir, "--groups", ",".join(TIERS), idp=None)
    out = capsys.readouterr().out

    assert code == 0
    assert f"Groups:   {', '.join(TIERS)} (--groups CLI)" in out
    assert _saved(env_dir) == []
    assert (
        "--groups not saved to access_tier_groups — the groups couldn't be verified"
        in out
    )
    assert "Saved --groups" not in out


def test_unsafe_rewrite_is_reported_not_raised(tmp_path, monkeypatch, capsys):
    env_dir = _dev_env(tmp_path)
    before = (env_dir / "env.auto.tfvars").read_text()

    def refuse(*a, **k):
        raise ValueError("could not safely update access_tier_groups; set it by hand")

    monkeypatch.setattr(generate_abac, "persist_access_tier_groups", refuse)
    code, _ = _run_generate(monkeypatch, env_dir, "--groups", ",".join(TIERS), idp=TIERS)

    assert code == 0
    assert "--groups not saved: could not safely update access_tier_groups" in (
        capsys.readouterr().out
    )
    assert (env_dir / "env.auto.tfvars").read_text() == before


def test_dry_run_does_not_save_groups(tmp_path, monkeypatch, capsys):
    env_dir = _dev_env(tmp_path)
    code, _ = _run_generate(
        monkeypatch, env_dir, "--groups", ",".join(TIERS), "--dry-run", idp=TIERS
    )

    assert code == 0
    assert _saved(env_dir) == []
    assert "dry run — not saving --groups" in capsys.readouterr().out


@pytest.mark.parametrize("mode_args", [MODES["genie"], MODES["space"]], ids=["genie", "space"])
def test_account_autoload_remains_the_fallback(mode_args, tmp_path, monkeypatch, capsys):
    env_dir = _dev_env(tmp_path)
    code, preflighted = _run_generate(
        monkeypatch, env_dir, *mode_args, account_groups=["acct_a", "acct_b"],
        idp=["acct_a", "acct_b"],
    )

    assert code == 0
    assert preflighted == ["acct_a", "acct_b"]
    assert "(auto-loaded from account config)" in capsys.readouterr().out
    assert _saved(env_dir) == []


@pytest.mark.parametrize("mode", ["full", "governance"])
def test_full_and_governance_never_auto_load_account_groups(mode, tmp_path, monkeypatch, capsys):
    # The account config is these modes' own promoted output, not a declared
    # tier order: refuse, and show it as a paste-ready setting instead.
    env_dir = _dev_env(tmp_path)
    code, preflighted = _run_generate(
        monkeypatch, env_dir, *MODES[mode], account_groups=["acct_a", "acct_b"],
        idp=["acct_a", "acct_b"],
    )
    out = capsys.readouterr().out

    assert code == 1
    assert preflighted is None
    assert "consume-by-default requires a group->tier mapping" in out
    assert f"{mode} mode does not auto-load them" in out
    assert '    access_tier_groups = ["acct_a", "acct_b"]\n' in out
    assert "Auto-loaded" not in out
    assert _saved(env_dir) == []


@pytest.mark.parametrize("mode_args", MODES.values(), ids=MODES.keys())
def test_no_mapping_anywhere_errors_and_names_the_setting(
    mode_args, tmp_path, monkeypatch, capsys
):
    env_dir = _dev_env(tmp_path)
    code, preflighted = _run_generate(monkeypatch, env_dir, *mode_args)
    out = capsys.readouterr().out

    assert code == 1
    assert preflighted is None
    assert "consume-by-default requires a group->tier mapping" in out
    assert f"Set {SETTING_LOCATION}" in out
    assert "saved to access_tier_groups for later runs" in out


def test_malformed_persisted_value_fails_before_any_fetch(tmp_path, monkeypatch, capsys):
    env_dir = _dev_env(tmp_path, 'access_tier_groups = ["a", "a"]')
    code, _ = _run_generate(monkeypatch, env_dir, idp=["a"])

    assert code == 1
    assert "access_tier_groups in envs/dev/env.auto.tfvars lists a more than once" in (
        capsys.readouterr().out
    )


def test_create_groups_ignores_the_persisted_mapping(tmp_path, monkeypatch, capsys):
    env_dir = _dev_env(tmp_path, 'access_tier_groups = ["payments_ops"]')
    code, preflighted = _run_generate(monkeypatch, env_dir, "--create-groups")

    assert code == 0
    assert preflighted is None
    assert "ignoring access_tier_groups" in capsys.readouterr().out


def test_placeholder_guard_runs_before_group_resolution(tmp_path, monkeypatch, capsys):
    env_dir = _dev_env(tmp_path, genie_space_id=PLACEHOLDER)
    before = (env_dir / "env.auto.tfvars").read_text()
    code, preflighted = _run_generate(
        monkeypatch, env_dir, "--groups", ",".join(TIERS), idp=TIERS
    )
    out = capsys.readouterr().out

    assert code == 1
    assert f"replace placeholder {PLACEHOLDER} in envs/dev/env.auto.tfvars" in out
    assert preflighted is None
    assert (env_dir / "env.auto.tfvars").read_text() == before


# ── promotion: prod consumes the same tiers ──────────────────────────────────

def _promote(monkeypatch, source, dest):
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev_catalog=prod_catalog",
    ])
    remap_env_config.main()


def test_promote_carries_access_tier_groups_and_prod_generate_uses_them(
    tmp_path, monkeypatch, capsys
):
    source = tmp_path / "envs" / "dev"
    dest = tmp_path / "envs" / "prod"
    source.mkdir(parents=True)
    (source / "env.auto.tfvars").write_text(
        'genie_spaces = []\nuc_tables = ["dev_catalog.s.t"]\n'
        'access_tier_groups = ["payments_ops", "regional_analysts", "viewers"]\n'
    )
    _promote(monkeypatch, source, dest)

    prod_cfg = hcl2.loads((dest / "env.auto.tfvars").read_text())
    assert prod_cfg["access_tier_groups"] == TIERS
    assert prod_cfg["uc_tables"] == ["prod_catalog.s.t"]
    assert "business_access_enabled" not in prod_cfg

    # derive-assignments / certify load prod config through load_auth_config.
    (dest / "auth.auto.tfvars").write_text(
        'databricks_workspace_host = "https://x.invalid"\n'
        'databricks_client_id = "client"\n'
        'databricks_client_secret = "test-only-secret"\n'
    )
    (dest / "data_access").mkdir()
    loaded = generate_abac.load_auth_config(dest / "auth.auto.tfvars")
    assert loaded["access_tier_groups"] == TIERS

    code, preflighted = _run_generate(monkeypatch, dest, idp=TIERS)
    assert code == 0
    assert preflighted == TIERS
    assert "(access_tier_groups in envs/prod/env.auto.tfvars)" in capsys.readouterr().out


def test_promote_without_the_setting_writes_none(tmp_path, monkeypatch):
    source = tmp_path / "dev"
    source.mkdir()
    (source / "env.auto.tfvars").write_text(
        'genie_spaces = []\nuc_tables = ["dev_catalog.s.t"]\n'
    )
    _promote(monkeypatch, source, tmp_path / "prod")

    text = (tmp_path / "prod" / "env.auto.tfvars").read_text()
    assert "access_tier_groups" not in text


def test_repromote_preserves_differing_destination_tiers(tmp_path, monkeypatch):
    source = tmp_path / "dev"
    dest = tmp_path / "prod"
    source.mkdir()
    dest.mkdir()
    (source / "env.auto.tfvars").write_text(
        'genie_spaces = []\nuc_tables = ["dev_catalog.s.t"]\n'
        'access_tier_groups = ["a", "b"]\n'
    )
    (dest / "env.auto.tfvars").write_text('access_tier_groups = ["stale"]\n')
    _promote(monkeypatch, source, dest)

    assert hcl2.loads((dest / "env.auto.tfvars").read_text())["access_tier_groups"] == ["stale"]


def test_promote_rejects_a_malformed_source_setting(tmp_path, monkeypatch, capsys):
    source = tmp_path / "dev"
    source.mkdir()
    (source / "env.auto.tfvars").write_text(
        'genie_spaces = []\nuc_tables = ["dev_catalog.s.t"]\n'
        'access_tier_groups = "payments_ops"\n'
    )
    with pytest.raises(SystemExit) as excinfo:
        _promote(monkeypatch, source, tmp_path / "prod")

    assert excinfo.value.code == 1
    assert "access_tier_groups" in capsys.readouterr().out
    assert not (tmp_path / "prod" / "env.auto.tfvars").exists()


def test_promoted_lines_render_valid_hcl(tmp_path):
    rendered = "\n".join(promoted_lines({"access_tier_groups": TIERS}, tmp_path / "e"))
    assert hcl2.loads(rendered + "\n") == {"access_tier_groups": TIERS}


# ── Terraform, template, and docs ────────────────────────────────────────────

@pytest.mark.parametrize("root", ["data_access", "workspace"])
def test_roots_loading_the_workspace_env_file_declare_the_setting(root):
    source = (SHARED / "roots" / root / "main.tf").read_text()
    start = source.index('variable "access_tier_groups"')
    body = source[start : source.index("}\n", start)]
    assert "list(string)" in body
    assert "default     = []" in body


def test_template_ships_access_tier_group_placeholders_next_to_the_agent():
    text = TEMPLATE.read_text()
    cfg = hcl2.loads(text)

    assert cfg["access_tier_groups"] == ["<most-privileged-group>", "<least-privileged-group>"]
    assert text.index("genie_spaces = [") < text.index("access_tier_groups = [") < text.index(
        "sql_warehouse_id ="
    )
    assert "MOST to LEAST" in text
    assert "--groups \"payments_ops" not in text.split("GREENFIELD SEQUENCE")[1]
    greenfield = text.split("GREENFIELD SEQUENCE")[1]
    assert "MODE=genie" not in greenfield
    assert "#   make generate ENV=dev                 # ONE run: imports the agent, finds its tables, drafts rules\n" in greenfield


def test_champion_docs_drop_generate_args_from_generate_commands():
    readme = (SHARED / "examples/dev_to_prod/README.md").read_text()
    import_doc = (SHARED / "docs/import-genie-agent-from-ui.md").read_text()

    for text in (readme, import_doc):
        assert 'access_tier_groups = ["payments_ops", "regional_analysts", "viewers"]' in text
        assert "make generate ENV=dev" in text
        generate_lines = [
            line for line in text.splitlines() if "make generate" in line and "`" not in line
        ]
        assert not any("GENERATE_ARGS" in line for line in generate_lines)
    # The champion walkthrough has one plain generate; MODE=genie stays for other flows.
    assert "\nmake generate ENV=dev\n" in readme
    assert "MODE=genie" not in readme
    assert "`make generate ENV=dev MODE=genie`" in import_doc


def test_champion_walkthrough_covers_check_commit_verify_and_agent_forms():
    """Gaps a live Azure run found: make check, an early commit, verify-access inside rehearse, both agent forms."""
    readme = (SHARED / "examples/dev_to_prod/README.md").read_text()
    ref = (SHARED / "examples/dev_to_prod/REFERENCE.md").read_text()
    flow = readme[readme.index("## The whole flow"):readme.index("## Before you start")]
    before = readme[readme.index("## Before you start"):readme.index("## Dev")]
    dev = readme[readme.index("## Dev"):readme.index("## Prod")]
    prod = readme[readme.index("## Prod"):readme.index("## Keep it safe")]

    # The whole flow: own repo (setup REPO=) and first commit first; rehearse and release-plan run
    # make check themselves, so the flow has no separate check line.
    order = ["make setup ENV=dev REPO=<your-repo-url>", "offers to commit + push those defaults",
             "make generate ENV=dev", "make rehearse ENV=dev       # checks the committed files",
             "make promote-to ENV=prod", "make release-plan ENV=prod", "after checking the committed files",
             "make release ENV=prod"]
    positions = [flow.index(step) for step in order]
    assert positions == sorted(positions)
    assert "make check ENV=" not in flow
    assert "Data classification" in flow
    assert (
        "make setup ENV=prod         # set deployer_principal_id, catalog_map and group_map; "
        "leave access_tier_groups placeholders"
    ) in flow

    # Detailed walkthrough: setup's REPO= (explained before you start), then commit before generate/check/rehearse.
    assert "git remote" not in flow
    assert "make setup ENV=dev REPO=<your-repo-url>" in before and "leave `REPO=` off" in before
    step_1 = dev[dev.index("**1. Set up.**"):dev.index("**2. Commit it (own repo).**")]
    committed = dev[dev.index("**2. Commit it (own repo).**"):]
    assert "make setup ENV=dev REPO=<your-repo-url>" in step_1
    assert "GenieRails previews the `.gitignore` and placeholder env defaults" in committed
    assert "git add .gitignore envs/" not in committed
    steps = ["GenieRails previews the `.gitignore` and placeholder env defaults", "**4. Generate.**", "**5. Rehearse.**", "make rehearse ENV=dev",
             "It first runs `make check ENV=dev`", "before it takes the lock, writes anything or reads live dev"]
    positions = [committed.index(step) for step in steps]
    assert positions == sorted(positions)
    assert "**5. Check.**" not in dev and "**6. Rehearse.**" not in dev
    detailed_positions = [readme.index("make setup ENV=dev REPO=", readme.index("## Dev")),
                          readme.index("GenieRails previews the `.gitignore` and placeholder env defaults", readme.index("**2. Commit it (own repo).**"))]
    assert detailed_positions == sorted(detailed_positions)
    assert "The same `make check` runs alone as the PR check." in dev
    assert "`verify-access` as its last gate (`_verify-access-gate`)" in dev
    assert "**Pass looks like:** `ALL EFFECTIVE`" in dev and "make verify-access ENV=dev" in dev
    assert "**Agents (preferred):**" in dev and "envs/dev/agents/" in dev
    assert "**Also accepted:**" in dev and "genie_spaces = [{" in dev

    assert ("`make release-plan` first runs `make check ENV=prod`, which for a promoted env also runs "
            "`promote-to CHECK=1`; if a step fails, it refuses before any lock, live call or write.") in prod
    assert "make verify-access ENV=prod" in prod
    assert "`rehearse` (deterministic envs) and `release-plan` run it first" in ref
    assert "Does **not** run `make check`" not in ref
    assert "`make check ENV=dev` first (refuses before the lock, any write or live call if it fails)" in ref
    assert "**Preferred:**" in ref and "**Also accepted:**" in ref


def test_champion_walkthrough_lets_release_plan_reuse_the_dev_rehearsal_on_the_promotion_commit():
    """release-plan needs a passing dev rehearsal of HEAD and writes it itself from dev step 5's (dev unchanged)."""
    readme = (SHARED / "examples/dev_to_prod/README.md").read_text()
    gate = (SHARED / "scripts/release_gate.py").read_text()
    flow = readme[readme.index("## The whole flow"):readme.index("## Before you start")]
    prod = readme[readme.index("## Prod"):readme.index("## Keep it safe")]
    gap = prod[prod.index("**Reuse an existing mask**"):prod.index("**A new kind of mask**")]

    # The whole flow: promote-to (which offers the commit + push), then release-plan (no second dev rehearse).
    promoted = flow.index("offers to commit + push (y)", flow.index("make promote-to ENV=prod"))
    assert flow.index("make promote-to ENV=prod") < promoted < flow.index("make release-plan ENV=prod")
    assert "make rehearse ENV=dev" not in flow[promoted:]

    # Prod: promote-to offers to commit and push the promoted files, then plan; release-plan reuses dev's rehearsal.
    steps = ["It then offers to commit and push `.gitignore`, `envs/prod/`", "**3. Plan, release.**",
             "make release-plan ENV=prod", "make release ENV=prod"]
    positions = [prod.index(step) for step in steps]
    assert positions == sorted(positions)
    step3 = prod[prod.index("**3. Plan, release.**"):]
    assert "make rehearse ENV=dev" not in step3[:step3.index("make release ENV=prod")]
    # What the user sees, in release_gate.py's own words.
    for quoted in ("no passing rehearsal of", "without re-applying", "did not reuse a rehearsal for this commit"):
        assert quoted in gate and quoted in prod, quoted
    assert "reusing {source}'s passing" in gate and "reusing dev's passing rehearsal of" in prod

    # A coverage-gap fix commits a new promotion too: release-plan reuses dev's rehearsal of the fix commit.
    after_promote = gap[gap.index("`make promote-to ENV=prod`"):]
    assert "`make rehearse ENV=dev`" not in after_promote[:after_promote.index("`make release ENV=prod`")]
    assert "`make release-plan ENV=prod` (it reuses the dev rehearsal, or names the command to run)" in after_promote
