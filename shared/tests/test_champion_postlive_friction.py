"""Output friction from the live AWS/Azure runs of the 8-command champion flow (2026-11-05).

1. make check's structural plan (a scratch copy against empty state) printed
   ``[FAIL] COVERAGE CHECK — zero classified/treatment columns were supplied``
   although check passed: that gate run is not check's verdict (step 4 is).
2. A staged create/tighten render keeps the active generation's treatment tags;
   the rest arrive at the switch, so its check judged a render with fewer
   tags and read like a gap. The wording says so; the decision is unchanged.
3. make evidence's header said ``Approved by: unapproved`` while the release
   receipt recorded the terminal approval.
4. DETAILS.md says to make import an env that already holds live resources.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

from tests.test_coverage_gate_script import _gate_file, _inputs, env_dir, stub_runner  # noqa: F401

SHARED = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED))

import evidence_report  # noqa: E402
from scripts import check_env  # noqa: E402
from scripts import coverage_gate as cg  # noqa: E402
from scripts import release_gate as rg  # noqa: E402
from scripts.state_layout import STRUCTURAL_CHECK_TOKEN_ENV, mint_structural_check_token  # noqa: E402

FAIL_LINE = "[FAIL] COVERAGE CHECK — zero classified/treatment columns were supplied; refusing a vacuous pass"


@pytest.fixture
def report_validator(tmp_path, monkeypatch):
    """A validator that prints a report like validate_abac.py's and exits ``rc``."""
    validator = tmp_path / "report_validator.py"

    def configure(rc):
        validator.write_text(
            "import sys\n"
            "print('=' * 60)\nprint('  ABAC Configuration Validation Report')\n"
            f"print('  ' + {FAIL_LINE!r} if {rc} else '  [PASS] coverage ok')\n"
            f"print('RESULT: ' + ('FAIL' if {rc} else 'PASS'))\n"
            f"raise SystemExit({rc})\n"
        )
        monkeypatch.setattr(cg, "VALIDATOR", validator)

    return configure


# -- 1. make check's structural plan ---------------------------------------------------------------

def test_the_structural_plans_gate_failure_is_not_reported_as_a_fail(env_dir, stub_runner, report_validator,
                                                                     monkeypatch, capfd):
    runner, _log = stub_runner(_inputs(needs_gate=False, new_grants=["cat.sch.customers|analysts"]))
    report_validator(1)
    token = mint_structural_check_token(env_dir)
    monkeypatch.setenv(STRUCTURAL_CHECK_TOKEN_ENV, str(token))
    try:
        assert cg.run_gate(env_dir, "prod", runner, "", False) == 0
        assert token.exists()  # the workspace plan still consumes it
    finally:
        token.unlink(missing_ok=True)
    out = capfd.readouterr()
    assert "[FAIL]" not in out.out + out.err and "coverage check FAILED" not in out.out + out.err
    assert "Coverage check of make check's structural plan (data_access:prod): did not pass" in out.out
    assert "This is not make check's coverage verdict" in out.out
    assert "This plan withholds 1 new SELECT grant(s)." in out.out
    assert json.loads(_gate_file(env_dir).read_text())["status"] == "fail"  # the decision is unchanged


def test_verbose_shows_the_structural_plans_report(env_dir, stub_runner, report_validator, monkeypatch, capfd):
    runner, _log = stub_runner(_inputs(needs_gate=False))
    report_validator(1)
    token = mint_structural_check_token(env_dir)
    monkeypatch.setenv(STRUCTURAL_CHECK_TOKEN_ENV, str(token))
    try:
        assert cg.run_gate(env_dir, "prod", runner, "", True) == 0
    finally:
        token.unlink(missing_ok=True)
    out = capfd.readouterr().out
    assert FAIL_LINE in out and "This is not make check's coverage verdict" in out


def test_a_structural_failure_that_stops_the_plan_still_shows_the_report(env_dir, stub_runner, report_validator,
                                                                         monkeypatch, capfd):
    runner, _log = stub_runner(_inputs(needs_gate=True))
    report_validator(1)
    token = mint_structural_check_token(env_dir)
    monkeypatch.setenv(STRUCTURAL_CHECK_TOKEN_ENV, str(token))
    try:
        assert cg.run_gate(env_dir, "prod", runner, "", False) == 1
    finally:
        token.unlink(missing_ok=True)
    out = capfd.readouterr()
    assert FAIL_LINE in out.out and "nothing is applied" in out.err


@pytest.mark.parametrize("token", [None, "forged"])
def test_without_a_valid_token_the_gate_reports_as_before(env_dir, stub_runner, report_validator, monkeypatch,
                                                          capfd, tmp_path, token):
    runner, _log = stub_runner(_inputs(needs_gate=False))
    report_validator(1)
    if token:
        forged = tmp_path / "forged-token"
        forged.write_text("somewhere else\n")
        forged.chmod(0o600)
        monkeypatch.setenv(STRUCTURAL_CHECK_TOKEN_ENV, str(forged))
    else:
        monkeypatch.delenv(STRUCTURAL_CHECK_TOKEN_ENV, raising=False)
    assert cg.run_gate(env_dir, "prod", runner, "", False) == 0
    out = capfd.readouterr()
    assert FAIL_LINE in out.out and "WARNING: coverage check FAILED for data_access:prod" in out.err
    assert "structural plan" not in out.out


class _FakeScratch:
    """check_env.Scratch without make: every target passes but those in ``failing``."""

    failing: set = set()
    calls: list = []

    def __init__(self, cloud_dir, makefile, make_bin, committed=False):
        self.root = cloud_dir.parent
        self.envs = cloud_dir / "envs"

    def add_credentials(self, env):
        pass

    def make(self, target, *args, extra_env=None):
        type(self).calls.append(target)
        return 1 if target in type(self).failing else 0

    def close(self):
        pass


def _legacy_env(tmp_path: Path) -> Path:
    cloud = tmp_path / "aws"
    env = cloud / "envs" / "dev"
    (env / "generated").mkdir(parents=True)
    (env / "env.auto.tfvars").write_text('governance_mode = "legacy"\n')
    (env / "abac.auto.tfvars").write_text("groups = {}\n")
    (env / "generated" / "abac.auto.tfvars").write_text("tag_assignments = []\n")
    (env / "generated" / "masking_functions.sql").write_text("-- masks\n")
    return cloud


@pytest.mark.parametrize("failing, expected", [({"coverage-gate"}, "coverage check"), ({"plan"}, "plan"),
                                               (set(), None)])
def test_a_real_coverage_gap_still_fails_make_check(tmp_path, monkeypatch, failing, expected):
    cloud = _legacy_env(tmp_path)
    monkeypatch.setattr(check_env, "Scratch", _FakeScratch)
    monkeypatch.setattr(_FakeScratch, "failing", failing)
    monkeypatch.setattr(_FakeScratch, "calls", [])
    monkeypatch.setattr(check_env, "SHARED_ROOT", tmp_path / "no-shared")  # no mask-library tests to run
    monkeypatch.setattr(check_env, "marker", lambda _environ: False)
    args = type("Args", (), {"env": "dev", "cloud_dir": cloud, "makefile": cloud / "Makefile", "make": "make"})()
    if expected:
        with pytest.raises(check_env.CheckFailed, match=f"^{expected}$"):
            check_env._check(args)
    else:
        check_env._check(args)
    assert "coverage-gate" in _FakeScratch.calls
    assert check_env.main(["check", "--env", "dev", "--cloud-dir", str(cloud), "--makefile",
                           str(cloud / "Makefile")]) == (1 if expected else 0)


# -- 2. a staged render before its switch ------------------------------------------------------------

TREATED = ("cat.sch.customers.email", "cat.sch.customers.ssn")


def _tags(columns) -> str:
    items = "".join(f'  {{ entity_type = "columns", entity_name = "{c}", tag_key = "gr_treatment", '
                    f'tag_value = "redact_full" }},\n' for c in columns)
    return f"tag_assignments = [\n{items}]\n"


@pytest.fixture
def staged_env(env_dir):  # noqa: F811
    (env_dir / "generated" / "abac.auto.tfvars").write_text(_tags(TREATED))
    render = env_dir / "render"
    render.mkdir()
    (render / "abac.auto.tfvars").write_text("tag_assignments = []\n")
    (render / "masking_functions.sql").write_text("-- masks\n")
    return env_dir, render


@pytest.mark.parametrize("stage", ["create", "tighten"])
def test_a_staged_render_before_the_switch_says_the_tags_come_at_the_switch(staged_env, stub_runner,
                                                                            report_validator, monkeypatch,
                                                                            capfd, stage):
    env, render = staged_env
    monkeypatch.delenv(STRUCTURAL_CHECK_TOKEN_ENV, raising=False)
    runner, _log = stub_runner(_inputs(needs_gate=False))
    report_validator(1)
    staged = {"stage": stage, "old_fingerprint": "det-old", "keep_new_tags": False}
    assert cg.run_gate(env, "prod", runner, "", False, render, staged) == 0
    out = capfd.readouterr()
    assert (f"{stage.capitalize()} phase: this render tags 0 of the 2 column(s) the generated config governs; "
            "gr_treatment tags are applied at the switch") in out.out
    assert "New SELECT stays withheld until the grant phase" in out.out
    assert f"This is the {stage} render: 2 gr_treatment tag(s) are applied at the switch" in out.err
    assert json.loads(_gate_file(env).read_text())["status"] == "fail"  # the same decision as before


def test_no_staged_note_once_the_render_carries_every_tag_or_at_the_switch(staged_env, stub_runner,
                                                                          report_validator, monkeypatch, capfd):
    env, render = staged_env
    monkeypatch.delenv(STRUCTURAL_CHECK_TOKEN_ENV, raising=False)
    runner, _log = stub_runner(_inputs(needs_gate=False))
    report_validator(1)
    assert cg.run_gate(env, "prod", runner, "", False, render, {"stage": "switch", "old_fingerprint": "det-old"}) == 0
    (render / "abac.auto.tfvars").write_text(_tags(TREATED))
    assert cg.run_gate(env, "prod", runner, "", False, render, {"stage": "create", "old_fingerprint": "det-old"}) == 0
    out = capfd.readouterr()
    assert "applied at the switch" not in out.out + out.err
    assert cg.deferred_treatments(env, {"stage": "create"}, render / "abac.auto.tfvars") is None


def test_rehearse_and_release_word_the_staged_check_from_one_place():
    """Both run the staged controller (policy_generations._rerun_coverage_gate), so one wording."""
    source = (SHARED / "scripts" / "policy_generations.py").read_text()
    assert source.count('str(COVERAGE_GATE), "run"') == 1
    assert hasattr(cg, "deferred_treatments")


# -- 3. the evidence approval header ----------------------------------------------------------------------

def _prod(tmp_path, monkeypatch) -> Path:
    for name in ("GENIERAILS_STATE_DIR", "GENIERAILS_EVIDENCE_INTEGRATION", "GENIERAILS_EVIDENCE_APPROVED_BY",
                 "GENIERAILS_EVIDENCE_APPROVED_AT"):
        monkeypatch.delenv(name, raising=False)
    env_dir = tmp_path / "aws" / "envs" / "prod"
    env_dir.mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text('uc_tables = ["main.sales.customers"]\n')
    return env_dir


RECEIPT = "2026-11-05T002641298188Z-6d463a4340c8.json"


def _receipt(env_dir: Path, approval, *, phases=None):
    directory = rg.release_dir(env_dir.parents[1], "prod")
    directory.mkdir(parents=True, exist_ok=True)
    receipt = {"kind": "release", "version": rg.VERSION, "env": "prod", "commit": "6d463a4340c8" + "0" * 28,
               "created_at": "2026-11-05T00:26:41.298188Z"}
    if approval is not None:
        receipt["approval"] = approval
    if phases is not None:
        receipt["phase_approvals"] = phases
    (directory / RECEIPT).write_text(json.dumps(receipt))


def _evidence(env_dir) -> tuple[dict, str]:
    out = evidence_report.evidence_dir(env_dir)
    assert evidence_report.main(["--env-dir", str(env_dir)]) == 0
    return (json.loads((out / "compliance-evidence-v1.0.json").read_text())["header"],
            (out / "compliance-evidence-v1.0.md").read_text())


TERMINAL = {"via": "terminal", "user": "louis.chen", "git_user": {"name": "Louis Chen", "email": "louis@example.com"},
            "approved_at": "2026-11-05T00:25:10.000000Z", "tty": "/dev/ttys004",
            "question": "Approve and release manifest X?", "run_sha256": "f" * 64, "host": "laptop-xyz",
            "asker": {"pid": 424242}}


def test_the_header_reads_the_terminal_approval_from_the_release_receipt(tmp_path, monkeypatch):
    env_dir = _prod(tmp_path, monkeypatch)
    _receipt(env_dir, TERMINAL, phases={"p1": {"via": "terminal"}, "p2": {"via": "terminal"},
                                        "p3": {"via": "external"}})
    header, markdown = _evidence(env_dir)
    assert header["approved_by"] == ("Louis Chen <louis@example.com> (OS user louis.chen; y typed at a terminal, "
                                     "self-reported)")
    assert header["approved_at"] == "2026-11-05T00:25:10.000000Z"
    assert header["approval_record"]["receipt"] == RECEIPT
    assert header["approval_record"]["phase_approvals"] == {"external": 1, "terminal": 2}
    assert "unapproved" not in markdown and "pending" not in markdown
    assert "- Approved by: `Louis Chen <louis@example.com>" in markdown
    assert (f"- Approval record: release receipt `{RECEIPT}` (commit `6d463a4340c8`; "
            "final round: terminal, earlier phases: 1 external, 2 terminal)") in markdown
    everything = json.dumps(header) + markdown
    for private in ("/dev/ttys004", "Approve and release", "f" * 64, "424242", "laptop-xyz"):
        assert private not in everything


def test_an_external_approval_says_so(tmp_path, monkeypatch):
    env_dir = _prod(tmp_path, monkeypatch)
    _receipt(env_dir, {"via": "external"})
    header, markdown = _evidence(env_dir)
    assert header["approved_by"].startswith("external (approved outside GenieRails")
    assert header["approved_at"] is None and "- Approved at: `unknown`" in markdown
    assert "unapproved" not in markdown


@pytest.mark.parametrize("approval", [None, "missing"])
def test_no_recorded_approval_is_unknown(tmp_path, monkeypatch, approval):
    env_dir = _prod(tmp_path, monkeypatch)
    if approval == "missing":
        _receipt(env_dir, None)  # a receipt from before #279 carries no approval field
    header, markdown = _evidence(env_dir)
    assert header["approved_by"] == "unknown" and "unapproved" not in markdown
    detail = "no release receipt for this env" if approval is None else "the release receipt records no approval"
    assert (f"- Approval record: {detail}" if approval is None else
            f"- Approval record: release receipt `{RECEIPT}` (commit `6d463a4340c8`; final round: unknown); {detail}"
            ) in markdown


def test_an_explicit_signature_still_wins(tmp_path, monkeypatch):
    env_dir = _prod(tmp_path, monkeypatch)
    _receipt(env_dir, TERMINAL)
    monkeypatch.setenv("GENIERAILS_EVIDENCE_APPROVED_BY", "compliance@example.com")
    header, _markdown = _evidence(env_dir)
    assert header["approved_by"] == "compliance@example.com" and header["approval_record"] == {"source": "signed"}


# -- 4. DETAILS.md: import an env that already holds live resources ------------------------------------------

DETAILS = (SHARED / "examples" / "dev_to_prod" / "DETAILS.md").read_text()
MAKEFILE = (SHARED / "Makefile.shared").read_text()


def _import_note() -> str:
    start = DETAILS.index('<a id="existing-live-resources"></a>')
    return DETAILS[start:DETAILS.index("<a id=", start + 1)]


def test_details_says_to_import_before_the_first_rehearse_or_release():
    note = _import_note()
    assert "make import ENV=account" in note and "make import ENV=<env>" in note
    assert "make rehearse ENV=dev" in note and "make release ENV=prod" in note
    assert "make promote ENV=dev" in note  # the account import reads the split account file
    assert "](#existing-live-resources)" in DETAILS  # reachable from the dev step


def test_the_import_note_matches_the_makefile_and_the_messages_it_quotes():
    note = _import_note()
    # make import ENV=account runs the account layer; any other ENV imports data_access then the workspace.
    target = MAKEFILE[MAKEFILE.index("\nimport: "):MAKEFILE.index("\nmigrate-state: ")]
    assert target.startswith("\nimport: _guard-not-ci-apply ") and "refused in CI" in note
    assert 'cd "$(ACCOUNT_ENV_DIR)" && "$(SHARED_ROOT)/scripts/import_existing.sh"' in target
    assert '$(ENV_DIR)/$(DATA_ACCESS_SUBDIR)" && "$(SHARED_ROOT)/scripts/import_existing.sh"' in target
    overlay = (SHARED / "scripts" / "runtime_overlay.py").read_text()
    assert "already has a policy named" in overlay
    assert "remove it or import it into the prod data_access state, then re-run" in overlay
    assert re.search(r"import \{count\} verified live gr_treatment assignment\(s\) into state",
                     (SHARED / "scripts" / "release_gate.py").read_text())
    assert "import <n> verified live gr_treatment assignment(s) into state" in note
