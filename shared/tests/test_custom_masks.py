"""Adopting a custom mask into governance (design 18.4.1).

make adopt-custom-mask copies a draft that make test-custom-mask passed into
envs/dev/custom_masks/custom_<name>.json, calls no model, and only prints the
tag_treatments line. Governance then treats it like a library treatment
through one lookup (custom_masks.effective_library): generate, validate,
scaffold and verify-access; rehearse and release re-run its cases; the
weakening check flags any change to which custom SQL a partial tier sees;
promote-to copies the folder as governance-owned. An env without
custom_masks/ sees no change. Every test uses a fake warehouse: no network.
"""

import hashlib
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from deterministic_governance import AccessResolution, resolve_precedence, validate_config
from governance_policies import (
    ColumnTreatment, build_model, is_library_treatment_value, parse_treatment_value, tag_treatment_errors,
)
from mask_library import load_library
from scripts import ai_helpers, custom_masks
from scripts.promotion_txn import GOVERNANCE, owner
from tests.test_ai_helpers import (  # noqa: F401 (fixtures)
    MASK_REPLY, FakeModel, FakeWarehouse, _drafted_mask, _make as _helper_make, _model_calls, _not_ci, _run, envs,
    repo,
)
from weakening import GovernanceState, Protection, check_weakenings, find_weakenings

ROOT = Path(__file__).resolve().parents[2]
SHARED = ROOT / "shared"
NAME = "custom_postcode_area"
TIERS = ["t1", "t2", "t3"]
COLUMN = "dev_fin.pay.customers.postcode"


def _passed(envs, reply=MASK_REPLY):
    """A custom_postcode_area draft that test-custom-mask passed; returns the fake warehouse answers."""
    assert _run(envs, "draft-custom-mask", FakeModel(reply), treatment=NAME, type="STRING", force=True) == 0
    fixture = json.loads(custom_masks.mask_paths(envs / "dev", NAME)[1].read_text())
    answers = {case["input"]: case["expected"] for case in fixture["cases"]}
    assert ai_helpers.check_custom_mask(envs / "dev", NAME, (FakeWarehouse(answers), "wh"))["passed"]
    return answers


def _adopt(envs, *extra, treatment=NAME, env="dev", env_dir=None):
    return custom_masks.main(["adopt-custom-mask", "--env", env, "--env-dir", str(env_dir or envs / env),
                              "--envs-dir", str(envs), "--cloud-dir", str(envs.parent),
                              "--treatment", treatment, *extra])


def _mask_file(name=NAME, sql=MASK_REPLY["sql"], cases=None, sql_type="STRING", **fields):
    cases = cases if cases is not None else [{"input": "CAST(NULL AS STRING)", "expected": None}] + [
        {"input": c["input"], "expected": c["expected"]} for c in MASK_REPLY["cases"]]
    family = "STRING" if sql_type == "STRING" else "NUMERIC"
    data = {"schema_version": 1, "treatment": name, "description": "d", "never_raw": False, "full": "redacted",
            "partial": {family: {"sql": sql, "tested_type": sql_type, "cases": cases,
                                 "tested_sha256": custom_masks.tested_sha256(name, sql_type, sql, cases)}}}
    data.update(fields)
    return data


def _write_mask(env_dir, data=None, name=NAME):
    folder = env_dir / "custom_masks"
    folder.mkdir(exist_ok=True)
    (folder / f"{name}.json").write_text(json.dumps(data or _mask_file(name), indent=2) + "\n")
    return folder / f"{name}.json"


def _tree(path):
    return {str(p.relative_to(path)): p.read_bytes() for p in sorted(path.rglob("*")) if p.is_file()}


# ── adopt-custom-mask ───────────────────────────────────────────────────────

def test_adopt_writes_the_tested_mask_and_only_prints_the_mapping(envs, capsys):
    dev = envs / "dev"
    _passed(envs)
    sql_path, json_path, _passed_path = custom_masks.mask_paths(dev, NAME)
    before = _tree(dev)

    assert _adopt(envs) == 0

    out = capsys.readouterr().out
    assert f"ADOPTED envs/dev/custom_masks/{NAME}.json" in out
    assert f'"class.<your_class>" = "{NAME}"' in out
    assert "adopt-custom-mask never edits that file" in out
    data = json.loads((dev / "custom_masks" / f"{NAME}.json").read_text())
    entry = data["partial"]["STRING"]
    assert (data["treatment"], data["never_raw"], data["full"], data["schema_version"]) == (NAME, False, "redacted", 1)
    assert entry["sql"] == MASK_REPLY["sql"] and entry["tested_type"] == "STRING"
    assert entry["cases"][0] == {"input": "CAST(NULL AS STRING)", "expected": None}
    assert all(set(c) == {"input", "expected"} for c in entry["cases"])
    canonical = json.dumps([NAME, "STRING", entry["sql"], entry["cases"]], sort_keys=True, separators=(",", ":"))
    assert entry["tested_sha256"] == hashlib.sha256(canonical.encode()).hexdigest()
    assert entry["adopted_from"] == {"sql_sha256": hashlib.sha256(sql_path.read_bytes()).hexdigest(),
                                     "fixture_sha256": hashlib.sha256(json_path.read_bytes()).hexdigest()}
    # Only the new file: env.auto.tfvars, generated/ and the drafts are untouched.
    after = _tree(dev)
    assert set(after) - set(before) == {f"custom_masks/{NAME}.json"}
    assert {k: v for k, v in after.items() if k in before} == before
    assert custom_masks.mask_file_problems(data, NAME) == []


def _refused(envs, capsys, message, *extra, **kwargs):
    assert _adopt(envs, *extra, **kwargs) == 1
    err = capsys.readouterr().err
    assert message in err, err
    assert not (envs / "dev" / "custom_masks").exists()


@pytest.mark.parametrize("marker, value", [("CI", "true"), ("CI", "1"), ("GITHUB_ACTIONS", "true"),
                                           ("TF_BUILD", "True"), ("GITLAB_CI", "true")])
def test_adopt_refuses_in_ci_with_no_opt_out(envs, capsys, monkeypatch, marker, value):
    _passed(envs)
    monkeypatch.setenv(marker, value)
    monkeypatch.setenv("GENIERAILS_ALLOW_CI_APPLY", "1")  # lifts nothing here
    _refused(envs, capsys, f"{marker} is set, and custom masks are adopted only on your own machine, never in CI")


@pytest.mark.parametrize("env, message", [
    ("prod", "ENV=prod is not allowed (it is promoted)"),
    ("staging", "ENV=staging is not allowed"),
    ("qa", "ENV=qa is not allowed (custom-mask adoption and the AI helpers run only in the dev env)"),
])
def test_adopt_refuses_every_env_but_dev(envs, capsys, env, message):
    _passed(envs)
    _refused(envs, capsys, message, env=env)
    assert not (envs / env / "custom_masks").exists()


def test_adopt_refuses_a_redirected_or_symlinked_dev(envs, capsys, tmp_path):
    _passed(envs)
    _refused(envs, capsys, "is not envs/dev of this checkout", env_dir=envs / "prod")
    outside = tmp_path / "outside"
    shutil.move(envs / "dev", outside)
    (envs / "dev").symlink_to(outside)
    assert _adopt(envs) == 1
    assert "is a symlink or not a directory" in capsys.readouterr().err
    assert not (outside / "custom_masks").exists()


def test_adopt_refuses_a_promoted_dev(envs, capsys):
    _passed(envs)
    env_file = envs / "dev" / "env.auto.tfvars"
    env_file.write_text(env_file.read_text() + 'promote_from = "sandbox"\n')
    _refused(envs, capsys, "envs/dev is promoted (its promote_from is sandbox)")


@pytest.mark.parametrize("treatment, message", [
    ("postcode_area", "is not a custom mask name: it must be custom_<name>"),
    ("redact", "is not a custom mask name"),
    ("custom_post__code", "no '__'"),
    ("custom_", "is not a custom mask name"),
    ("Custom_postcode", "is not a custom mask name"),
    ("custom_" + "x" * 64, "is longer than 64 characters"),
    ("", "TREATMENT=custom_<name> is required"),
])
def test_adopt_refuses_a_name_without_the_reserved_prefix(envs, capsys, treatment, message):
    _passed(envs)
    _refused(envs, capsys, message, treatment=treatment)


def test_adopt_refuses_a_name_the_library_ships(envs, capsys, monkeypatch):
    _passed(envs)
    library = load_library()
    library["treatments"][NAME] = dict(library["treatments"]["redact"])
    monkeypatch.setattr(custom_masks, "load_library", lambda: library)
    _refused(envs, capsys, f"{NAME} is a mask-library name; a custom mask needs its own")


def test_no_library_treatment_or_version_uses_the_custom_prefix():
    library = load_library()
    assert not [name for name in [*library["treatments"], *library["versions"]] if name.startswith("custom_")]


def test_adopt_refuses_without_a_pass_record(envs, capsys):
    _drafted_mask(envs)  # drafted, never tested
    _refused(envs, capsys, "has no pass record. Run make test-custom-mask ENV=dev TREATMENT=custom_postcode_area")


def test_adopt_refuses_when_the_draft_changed_after_it_passed(envs, capsys):
    _passed(envs)
    sql_path, json_path, _ = custom_masks.mask_paths(envs / "dev", NAME)
    sql_path.write_text(sql_path.read_text().replace("'**'", "'##'"))
    _refused(envs, capsys, "has a pass record that doesn't match the draft files")


def test_adopt_refuses_when_the_fixture_changed_after_it_passed(envs, capsys):
    _passed(envs)
    _sql, json_path, _ = custom_masks.mask_paths(envs / "dev", NAME)
    fixture = json.loads(json_path.read_text())
    fixture["cases"][1]["expected"] = "2000"
    json_path.write_text(json.dumps(fixture))
    _refused(envs, capsys, "has a pass record that doesn't match the draft files")


def test_adopt_refuses_a_forged_pass_record(envs, capsys):
    _passed(envs)
    _sql, _json, passed = custom_masks.mask_paths(envs / "dev", NAME)
    record = json.loads(passed.read_text())
    passed.write_text(json.dumps(record | {"sql_sha256": "0" * 64}))
    _refused(envs, capsys, "has a pass record that doesn't match the draft files")


def _forge_pass(envs, sql=None, cases=None):
    """Rewrite the passed draft and a pass record that matches it (as an older rule set might have)."""
    dev = envs / "dev"
    sql_path, json_path, passed = custom_masks.mask_paths(dev, NAME)
    if sql is not None:
        sql_path.write_text(f"-- forged\n{sql}\n")
    if cases is not None:
        fixture = json.loads(json_path.read_text())
        json_path.write_text(json.dumps(fixture | {"cases": cases}))
    record = json.loads(passed.read_text())
    passed.write_text(json.dumps(record | {"sql_sha256": hashlib.sha256(sql_path.read_bytes()).hexdigest(),
                                           "fixture_sha256": hashlib.sha256(json_path.read_bytes()).hexdigest()}))
    assert custom_masks.passed_record(dev, NAME) is not None


@pytest.mark.parametrize("sql, message", [
    ("CASE WHEN current_user() = 'a' THEN value ELSE '[REDACTED]' END", "caller-independent"),
    ("CASE WHEN is_account_group_member('t1') THEN value ELSE '[REDACTED]' END", "is_account_group_member"),
    ("concat(left(value, 2), cast(rand() AS STRING))", "deterministic: rand"),
    ("concat(value, cast(now() AS STRING))", "deterministic: now"),
    ("(SELECT max(x) FROM t WHERE x = value)", "not a query or statement (SELECT)"),
    ("value; DROP TABLE t", "one expression"),
    ("'[REDACTED]'", "must read the column as value"),
])
def test_adopt_reruns_the_offline_rules_on_the_expression(envs, capsys, sql, message):
    _passed(envs)
    _forge_pass(envs, sql=sql)
    _refused(envs, capsys, "breaks the mask library's rules, so it can't be adopted")
    _forge_pass(envs, sql=sql)
    assert _adopt(envs) == 1 and message in capsys.readouterr().err


@pytest.mark.parametrize("cases, message", [
    ([{"input": "CAST(NULL AS STRING)", "expected": None}, {"input": "CAST('2000' AS STRING)", "expected": "20**"}],
     "at least two cases with a non-null expected value"),
    ([{"input": "CAST('2000' AS STRING)", "expected": "20**"}, {"input": "CAST('3141' AS STRING)", "expected": "31**"}],
     "NULL in gives NULL out"),
    ([{"input": "CAST(NULL AS STRING)", "expected": None}, {"input": "CAST('2000' AS STRING)", "expected": "20**"},
      {"input": "upper('x')", "expected": "X"}], "must be one typed SQL literal"),
])
def test_adopt_reruns_the_offline_rules_on_the_cases(envs, capsys, cases, message):
    _passed(envs)
    _forge_pass(envs, cases=cases)
    assert _adopt(envs) == 1
    err = capsys.readouterr().err
    assert "breaks the mask library's rules" in err and message in err, err
    assert not (envs / "dev" / "custom_masks").exists()


@pytest.mark.parametrize("never_raw_class", ["secret", "card_pin"])
def test_adopt_refuses_when_a_never_raw_class_maps_to_it(envs, capsys, never_raw_class):
    _passed(envs)
    env_file = envs / "dev" / "env.auto.tfvars"
    env_file.write_text(env_file.read_text() + f'tag_treatments = {{ "class.{never_raw_class}" = "{NAME}" }}\n')
    _refused(envs, capsys, f"maps never-raw class(es) class.{never_raw_class} to {NAME}; if a class must never be "
                           "raw, map it to the mask library's secret")


def test_adopt_merges_families_and_replaces_one_only_with_force(envs, capsys):
    dev = envs / "dev"
    _passed(envs)
    assert _adopt(envs) == 0
    target = dev / "custom_masks" / f"{NAME}.json"
    first = target.read_text()
    assert _adopt(envs) == 0 and target.read_text() == first  # re-adopting the same draft is a no-op
    numeric = {"description": "Hundreds.", "sql": "CASE WHEN value IS NULL THEN NULL ELSE floor(value / 100) * 100 END",
               "cases": [{"input": "CAST(250 AS INT)", "expected": 200}, {"input": "CAST(99 AS INT)", "expected": 0}]}
    assert _run(envs, "draft-custom-mask", FakeModel(numeric), treatment=NAME, type="INT", force=True) == 0
    fixture = json.loads(custom_masks.mask_paths(dev, NAME)[1].read_text())
    answers = {c["input"]: c["expected"] for c in fixture["cases"]}
    assert ai_helpers.check_custom_mask(dev, NAME, (FakeWarehouse(answers), "wh"))["passed"]
    assert _adopt(envs) == 0
    data = json.loads(target.read_text())
    assert sorted(data["partial"]) == ["NUMERIC", "STRING"]
    # A different STRING mask is a new generation: only with FORCE=1.
    _passed(envs, dict(MASK_REPLY, sql=MASK_REPLY["sql"].replace("left(trim(value), 2)", "left(trim(value), 1)"),
                       cases=[{"input": "CAST('2000' AS STRING)", "expected": "2**"},
                              {"input": "CAST('3141' AS STRING)", "expected": "3**"}]))
    capsys.readouterr()
    assert _adopt(envs) == 1
    assert "already has a different STRING mask" in capsys.readouterr().err
    assert json.loads(target.read_text()) == data
    assert _adopt(envs, "--force") == 0
    replaced = json.loads(target.read_text())
    assert "left(trim(value), 1)" in replaced["partial"]["STRING"]["sql"]
    assert replaced["partial"]["NUMERIC"] == data["partial"]["NUMERIC"]


def test_make_adopt_custom_mask_calls_no_model(repo, envs):
    _passed(envs)
    result = _helper_make(repo, "adopt-custom-mask", "ENV=dev", f"TREATMENT={NAME}")
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"ADOPTED envs/dev/custom_masks/{NAME}.json" in result.stdout
    assert _model_calls(repo) == []


@pytest.mark.parametrize("ci", [("CI", "true"), ("GITHUB_ACTIONS", "true"), ("TF_BUILD", "True")])
def test_make_adopt_custom_mask_refuses_in_ci(repo, envs, ci):
    _passed(envs)
    result = _helper_make(repo, "adopt-custom-mask", "ENV=dev", f"TREATMENT={NAME}", "GENIERAILS_ALLOW_CI_APPLY=1", ci=ci)
    assert result.returncode != 0
    assert "never in CI (no opt-out)" in result.stderr
    assert not (envs / "dev" / "custom_masks").exists()


@pytest.mark.parametrize("args, message", [
    (["ENV=prod", f"TREATMENT={NAME}"], "ENV=prod is not allowed"),
    (["ENV=dev", "ENV_DIR=../envs/prod", f"TREATMENT={NAME}"], "is not envs/dev of this checkout"),
    (["ENV=dev", "TREATMENT=postcode_area"], "must be custom_<name>"),
])
def test_make_adopt_custom_mask_refusals(repo, envs, args, message):
    _passed(envs)
    result = _helper_make(repo, "adopt-custom-mask", *args)
    assert result.returncode != 0 and message in result.stderr, result.stderr
    assert not list(envs.glob("*/custom_masks/*"))


# ── The adopted file's rules (validate and check) ───────────────────────────

def _validate(env_dir, tag_treatments=None, extra=""):
    text = (env_dir / "env.auto.tfvars").read_text()
    if tag_treatments:
        text += "tag_treatments = {\n" + "".join(f'  "{k}" = "{v}"\n' for k, v in tag_treatments.items()) + "}\n"
    (env_dir / "env.auto.tfvars").write_text(text + extra)
    return subprocess.run([sys.executable, str(SHARED / "scripts" / "validate_env_config.py"),
                           str(env_dir / "env.auto.tfvars")], text=True, capture_output=True)


def test_validate_accepts_an_adopted_mask_and_the_mapping_to_it(envs):
    dev = envs / "dev"
    _write_mask(dev)
    result = _validate(dev, {"class.postcode": NAME})
    assert result.returncode == 0, result.stderr
    assert "env config validation: PASS" in result.stdout and "WARNING" not in result.stderr


def test_validate_refuses_mapping_a_custom_mask_that_is_not_adopted(envs):
    result = _validate(envs / "dev", {"class.postcode": NAME})
    assert result.returncode == 1
    assert f"names '{NAME}', which is not adopted in this env's custom_masks/" in result.stderr
    assert f"make adopt-custom-mask ENV=dev TREATMENT={NAME}" in result.stderr


def test_validate_warns_about_an_adopted_mask_nothing_maps(envs):
    _write_mask(envs / "dev")
    result = _validate(envs / "dev")
    assert result.returncode == 0
    assert f"WARNING: custom_masks/{NAME}.json is adopted, but no tag_treatments or column_overrides entry names it" \
        in result.stderr


def _edited_sql(data):
    data["partial"]["STRING"]["sql"] = data["partial"]["STRING"]["sql"].replace("left(trim(value), 2)", "trim(value)")
    return data


def _edited_case(data):
    data["partial"]["STRING"]["cases"][1]["expected"] = "2000"
    return data


@pytest.mark.parametrize("edit, message", [
    (_edited_sql, "tested_sha256 doesn't match its sql and cases"),
    (_edited_case, "tested_sha256 doesn't match its sql and cases"),
    (lambda d: d | {"never_raw": True}, "never_raw must be false; if a class must never be raw, map it to the mask library's secret"),
    (lambda d: d | {"full": "custom"}, 'full must be "redacted"'),
    (lambda d: d | {"treatment": "custom_other"}, f"treatment must be {NAME} (the file name)"),
    (lambda d: d | {"schema_version": 2}, "schema_version must be 1"),
    (lambda d: d | {"partial": {}}, "partial must map at least one type family"),
    (lambda d: d | {"partial": {"BOOLEAN": d["partial"]["STRING"]}}, "type families are STRING, NUMERIC"),
    (lambda d: _mask_file(sql="CASE WHEN current_user() = 'x' THEN value END"), "current_user is not allowed"),
    (lambda d: _mask_file(cases=[{"input": "CAST(NULL AS STRING)", "expected": None},
                                 {"input": "CAST('2000' AS STRING)", "expected": "20**"}]),
     "at least two cases with a non-null expected value"),
])
def test_validate_refuses_an_adopted_file_that_breaks_the_rules(envs, edit, message):
    dev = envs / "dev"
    _write_mask(dev, edit(_mask_file()))
    result = _validate(dev, {"class.postcode": NAME})
    assert result.returncode == 1
    assert message in result.stderr, result.stderr


@pytest.mark.parametrize("name", ["postcode_area", "custom_x__y", "redact"])
def test_validate_refuses_a_file_without_a_custom_name(envs, name):
    dev = envs / "dev"
    _write_mask(dev, _mask_file(name), name=name)
    result = _validate(dev)
    assert result.returncode == 1 and "is not a custom mask name" in result.stderr, result.stderr


def test_validate_refuses_other_files_and_symlinks_in_custom_masks(envs, tmp_path):
    dev = envs / "dev"
    _write_mask(dev)
    (dev / "custom_masks" / "notes.txt").write_text("x")
    result = _validate(dev)
    assert result.returncode == 1 and "custom_masks/ holds only custom_<name>.json files" in result.stderr
    (dev / "custom_masks" / "notes.txt").unlink()
    outside = tmp_path / "elsewhere.json"
    outside.write_text(json.dumps(_mask_file("custom_other")))
    (dev / "custom_masks" / "custom_other.json").symlink_to(outside)
    result = _validate(dev)
    assert result.returncode == 1 and "is a symlink" in result.stderr


def test_validate_refuses_custom_masks_in_a_legacy_env(envs):
    dev = envs / "dev"
    (dev / "env.auto.tfvars").write_text('access_tier_groups = ["t1", "t2", "t3"]\n')
    _write_mask(dev)
    result = _validate(dev)
    assert result.returncode == 1 and 'custom_masks/ needs governance_mode = "deterministic"' in result.stderr


def test_validate_refuses_a_never_raw_class_mapped_to_a_custom_mask(envs):
    dev = envs / "dev"
    _write_mask(dev)
    result = _validate(dev, {"class.secret": NAME})
    assert result.returncode == 1
    assert "maps a never-raw class to custom_postcode_area" in result.stderr
    assert "the mask library already maps class.secret" in result.stderr


def test_treatment_versions_can_only_make_a_custom_mask_stricter(envs):
    library = custom_masks.effective_library(_write_mask(envs / "dev").parent.parent)
    base = {"access_tier_groups": TIERS, "tag_treatments": {"class.postcode": NAME}}
    assert validate_config(base | {"treatment_versions": {NAME: {"partial": "redacted"}}}, library=library) == []
    assert validate_config(base | {"treatment_versions": {NAME: {"partial": NAME}}}, library=library) == []
    errors = validate_config(base | {"treatment_versions": {NAME: {"partial": "last4"}}}, library=library)
    assert errors == [f"treatment_versions[\"{NAME}\"].partial names unknown version 'last4'"]
    errors = validate_config(base | {"treatment_versions": {NAME: {"partial": "raw"}}}, library=library)
    assert any("may not be raw" in e for e in errors)
    # Without the env's custom masks the name is unknown everywhere.
    assert any("names unknown treatment" in e for e in validate_config(
        {"access_tier_groups": TIERS, "treatment_versions": {NAME: {"partial": "redacted"}}}))


# ── make check and make promote-to (real make) ──────────────────────────────

if shutil.which("make") is not None and shutil.which("git") is not None:
    from tests.test_check_target import (  # noqa: F401 (fixture)
        _commit, _envs, _make as _check_make, _state, repo as check_repo,
    )

    @pytest.fixture
    def promoted(check_repo, tmp_path):
        """test_check_target's promoted fixture, over its repo (this module's repo is the AI helpers')."""
        prod = _envs(check_repo) / "prod"
        prod.mkdir()
        (prod / "env.auto.tfvars").write_text('governance_mode = "deterministic"\n'
                                              'deployer_principal_id = "d0000000-0000-0000-0000-00000000d002"\n')
        result = _check_make(check_repo, tmp_path, "promote-to", "ENV=prod", "FROM=dev", "CATALOG_MAP=paycat=ppay")
        assert result.returncode == 0, result.stdout + result.stderr
        _commit(check_repo, "promote")
        return check_repo

    def test_check_recomputes_tested_sha256_and_refuses_a_mismatch(check_repo, tmp_path):
        dev = _envs(check_repo) / "dev"
        _write_mask(dev)
        _commit(check_repo, "adopt")
        result = _check_make(check_repo, tmp_path, "check", "ENV=dev", CI="true")
        assert result.returncode == 0, result.stdout + result.stderr
        assert f"WARNING: custom_masks/{NAME}.json is adopted" in result.stderr
        _write_mask(dev, _edited_sql(_mask_file()))
        _commit(check_repo, "hand edit")
        before = _state(check_repo)
        result = _check_make(check_repo, tmp_path, "check", "ENV=dev", CI="true")
        assert result.returncode != 0
        assert "tested_sha256 doesn't match its sql and cases" in result.stderr
        assert _state(check_repo) == before

    def test_promote_to_copies_custom_masks_verbatim_as_governance(promoted, tmp_path):
        envs = _envs(promoted)
        source = _write_mask(envs / "dev")
        source.write_text(source.read_text() + "\n")  # odd bytes survive: a copy, not a rewrite
        result = _check_make(promoted, tmp_path, "promote-to", "ENV=prod")
        out = result.stdout + result.stderr
        assert result.returncode == 0, out
        assert (envs / "prod" / "custom_masks" / f"{NAME}.json").read_bytes() == source.read_bytes()
        assert "Custom masks (governance-owned): copied envs/dev/custom_masks/ to envs/prod/custom_masks/" in out
        governance = next(line for line in out.splitlines() if line.startswith("  governance files (governance owners):"))
        assert f"envs/prod/custom_masks/{NAME}.json" in governance, out
        _commit(promoted, "promote mask")
        assert _check_make(promoted, tmp_path, "promote-to", "ENV=prod", "CHECK=1").returncode == 0
        # A hand edit in prod is caught, and a mask dev drops is dropped in prod.
        prod_copy = envs / "prod" / "custom_masks" / f"{NAME}.json"
        prod_copy.write_text(json.dumps(_edited_sql(_mask_file())))
        _commit(promoted, "hand edit prod")
        result = _check_make(promoted, tmp_path, "promote-to", "ENV=prod", "CHECK=1")
        assert result.returncode != 0
        source.unlink()
        source.parent.rmdir()
        result = _check_make(promoted, tmp_path, "promote-to", "ENV=prod")
        assert result.returncode == 0, result.stdout + result.stderr
        assert not (envs / "prod" / "custom_masks").exists()


def test_promote_copies_and_prunes_and_is_a_no_op_without_custom_masks(tmp_path):
    src, dest = tmp_path / "dev", tmp_path / "prod"
    src.mkdir()
    dest.mkdir()
    assert custom_masks.promote(src, dest) == [] and not (dest / "custom_masks").exists()
    _write_mask(src)
    _write_mask(src, _mask_file("custom_other"), name="custom_other")
    _write_mask(dest, _mask_file("custom_stale"), name="custom_stale")
    assert custom_masks.promote(src, dest) == ["custom_other.json", f"{NAME}.json", "custom_stale.json"]
    assert _tree(dest / "custom_masks") == _tree(src / "custom_masks")
    assert custom_masks.promote(src, dest) == []


def test_custom_masks_are_governance_owned():
    assert owner(f"custom_masks/{NAME}.json") == GOVERNANCE
    codeowners = (SHARED / "examples" / "CODEOWNERS.example").read_text()
    assert "envs/<env>/custom_masks/" in codeowners


# ── One lookup: generate, scaffold, tag values and verify-access ───────────

def _model(env_dir, classes=("class.postcode",), sql_type="STRING", **settings):
    library = custom_masks.effective_library(env_dir)
    cfg = {"access_tier_groups": TIERS, "tag_treatments": {"class.postcode": NAME}} | settings
    return build_model({COLUMN: list(classes)}, {COLUMN: sql_type}, cfg, deployer_principal="deployer",
                       library=library)


def test_generate_uses_the_adopted_sql_for_the_partial_tier_and_the_library_full(envs):
    dev = envs / "dev"
    _write_mask(dev)
    model = _model(dev)
    item = model.columns[COLUMN]
    assert (item.treatment, item.partial, item.full, item.never_raw) == (NAME, NAME, "redacted", False)
    assert item.partial_sql == f"CAST(({MASK_REPLY['sql']}) AS STRING)"
    assert item.full_sql == ColumnTreatment("redact", "STRING", "redacted", "redacted", False).full_sql
    assert parse_treatment_value(item.value)[:2] == (NAME, "string")
    names = {p["name"]: p for p in model.fgac_policies}
    partial = names[f"gr_mask_dev_fin_{item.value}_partial"]
    assert partial["to_principals"] == ["t2"] and partial["except_principals"] == ["t1", "deployer"]
    assert names[f"gr_mask_dev_fin_{item.value}_full"]["except_principals"] == ["t1", "t2", "deployer"]
    assert f"gr_mask_{item.value}_partial(value STRING) RETURNS STRING RETURN CAST(({MASK_REPLY['sql']}) AS STRING);" \
        in model.functions_sql
    assert model.unmapped == []


def test_a_custom_sql_edit_is_a_new_generation_and_other_families_get_the_full_version(envs):
    dev = envs / "dev"
    _write_mask(dev)
    before = _model(dev).columns[COLUMN].value
    _write_mask(dev, _edited_sql(_mask_file()))
    data = json.loads((dev / "custom_masks" / f"{NAME}.json").read_text())
    entry = data["partial"]["STRING"]
    entry["tested_sha256"] = custom_masks.tested_sha256(NAME, "STRING", entry["sql"], entry["cases"])
    _write_mask(dev, data)
    after = _model(dev).columns[COLUMN].value
    assert before != after and parse_treatment_value(before)[0] == parse_treatment_value(after)[0] == NAME
    dated = _model(dev, sql_type="DATE").columns[COLUMN]
    assert dated.partial == "redacted" and dated.partial_sql == "CAST(NULL AS DATE)"


def test_a_never_raw_class_on_the_same_column_still_wins(envs):
    dev = envs / "dev"
    _write_mask(dev)
    item = _model(dev, classes=("class.postcode", "class.secret")).columns[COLUMN]
    assert item.never_raw and item.treatment == "secret" and item.custom_partial_sql is None


def test_generate_refuses_a_tampered_mask(envs):
    dev = envs / "dev"
    _write_mask(dev, _edited_sql(_mask_file()))
    with pytest.raises(custom_masks.CustomMaskError, match="tested_sha256 doesn't match"):
        _model(dev)


def test_an_env_without_custom_masks_sees_no_change(envs, capsys):
    dev = envs / "dev"
    assert custom_masks.effective_library(dev) == load_library()
    cfg = {"access_tier_groups": TIERS}
    classes = {"c.s.t.email": ["email_address"], "c.s.t.ip": ["ip_address"], "c.s.t.x": ["class.unknown"]}
    types = {"c.s.t.email": "STRING", "c.s.t.ip": "STRING", "c.s.t.x": "STRING"}
    assert build_model(classes, types, cfg, library=custom_masks.effective_library(dev)) == build_model(classes, types, cfg)
    assert custom_masks.live_check(dev, "dev", warehouse=(None, "unused")) == 0
    assert custom_masks.promote(dev, envs / "prod") == []
    assert capsys.readouterr() == ("", "")
    assert custom_masks.env_errors(dev, {"governance_mode": "deterministic"}) == (load_library(), [], [])
    # A library value's generation is unchanged by the new field.
    item = ColumnTreatment("email_partial", "STRING", "partial", "redacted", False)
    payload = json.dumps(["email_partial", "STRING", False, item.partial_sql, item.full_sql], separators=(",", ":"))
    assert item.custom_partial_sql is None and item.generation == hashlib.sha256(payload.encode()).hexdigest()[:8]


def test_tag_treatments_name_only_adopted_custom_masks(envs):
    dev = envs / "dev"
    assert any("not adopted in this env's custom_masks/" in e for e in tag_treatment_errors({"class.postcode": NAME}))
    _write_mask(dev)
    assert tag_treatment_errors({"class.postcode": NAME}, custom_masks.effective_library(dev)) == []


def test_scaffold_treatments_maps_to_an_adopted_custom_mask(envs, monkeypatch):
    from scripts import scaffold_treatments

    dev = envs / "dev"
    monkeypatch.setattr(scaffold_treatments, "_offline_markers", lambda _p: [(COLUMN, "class.postcode", "postcode")])
    with pytest.raises(ValueError, match=f"Adopt it first: make adopt-custom-mask ENV=dev TREATMENT={NAME}"):
        scaffold_treatments.map_deterministic(dev / "generated" / "abac.auto.tfvars", dev, NAME)
    _write_mask(dev)
    result = scaffold_treatments.map_deterministic(dev / "generated" / "abac.auto.tfvars", dev, NAME)
    assert result["labels"] == ["class.postcode"] and result["types"] == ["STRING"]
    assert f'"class.postcode" = "{NAME}"' in (dev / "env.auto.tfvars").read_text()


def test_scaffold_in_prod_accepts_a_custom_mask_adopted_only_in_dev(envs, monkeypatch):
    """The mapping goes into dev's tag_treatments, so dev's adopted masks are the ones that count."""
    from scripts import scaffold_treatments

    dev, prod = envs / "dev", envs / "prod"
    prod_before = (prod / "env.auto.tfvars").read_bytes()
    _write_mask(dev)
    monkeypatch.setattr(scaffold_treatments, "_offline_markers", lambda _p: [(COLUMN, "class.postcode", "postcode")])
    result = scaffold_treatments.map_deterministic(prod / "generated" / "abac.auto.tfvars", prod, NAME)
    assert result["labels"] == ["class.postcode"] and result["target"] == dev
    assert f'"class.postcode" = "{NAME}"' in (dev / "env.auto.tfvars").read_text()
    assert (prod / "env.auto.tfvars").read_bytes() == prod_before


def test_scaffold_in_prod_refuses_a_custom_mask_dev_has_not_adopted(envs, monkeypatch):
    """A mask only prod has would be written to dev, where tag_treatment_errors then rejects it."""
    from scripts import scaffold_treatments

    dev, prod = envs / "dev", envs / "prod"
    dev_before = (dev / "env.auto.tfvars").read_bytes()
    _write_mask(prod)
    monkeypatch.setattr(scaffold_treatments, "_offline_markers", lambda _p: [(COLUMN, "class.postcode", "postcode")])
    with pytest.raises(ValueError, match=re.escape(
            f"It is not adopted in envs/dev/custom_masks/, where this mapping goes. "
            f"Adopt it first: make adopt-custom-mask ENV=dev TREATMENT={NAME}")):
        scaffold_treatments.map_deterministic(prod / "generated" / "abac.auto.tfvars", prod, NAME)
    assert (dev / "env.auto.tfvars").read_bytes() == dev_before


def test_scaffold_through_a_promoted_chain_maps_in_dev_with_devs_custom_masks(envs, monkeypatch):
    """uat <- staging <- dev: promote-to rewrites staging's tag_treatments, so the mapping goes to dev."""
    from scripts import scaffold_treatments

    dev, staging, uat = envs / "dev", envs / "staging", envs / "uat"
    uat.mkdir()
    (uat / "env.auto.tfvars").write_text((dev / "env.auto.tfvars").read_text() + 'promote_from = "staging"\n')
    staging_before = (staging / "env.auto.tfvars").read_bytes()
    monkeypatch.setattr(scaffold_treatments, "_offline_markers", lambda _p: [(COLUMN, "class.postcode", "postcode")])
    with pytest.raises(ValueError, match=re.escape(
            f"It is not adopted in envs/dev/custom_masks/, where this mapping goes. "
            f"Adopt it first: make adopt-custom-mask ENV=dev TREATMENT={NAME}")):
        scaffold_treatments.map_deterministic(uat / "generated" / "abac.auto.tfvars", uat, NAME)
    _write_mask(dev)
    result = scaffold_treatments.map_deterministic(uat / "generated" / "abac.auto.tfvars", uat, NAME)
    assert result["target"] == dev and f'"class.postcode" = "{NAME}"' in (dev / "env.auto.tfvars").read_text()
    assert (staging / "env.auto.tfvars").read_bytes() == staging_before


def test_scaffold_from_a_root_other_than_dev_refuses_custom_masks_it_can_never_get(envs, monkeypatch):
    """qa has no promote_from: adopt-custom-mask runs only in dev, and nothing carries dev's masks to qa."""
    from scripts import scaffold_treatments

    qa, rel = envs / "qa", envs / "rel"
    rel.mkdir()
    (rel / "env.auto.tfvars").write_text((qa / "env.auto.tfvars").read_text() + 'promote_from = "qa"\n')
    _write_mask(envs / "dev")
    qa_before = (qa / "env.auto.tfvars").read_bytes()
    monkeypatch.setattr(scaffold_treatments, "_offline_markers", lambda _p: [(COLUMN, "class.postcode", "postcode")])
    with pytest.raises(ValueError, match=re.escape(
            "It is not adopted in envs/qa/custom_masks/, where this mapping goes. Custom masks are adopted only "
            "in envs/dev (make adopt-custom-mask ENV=dev) and reach another env only by promote-to from it; "
            "envs/qa has no promote_from, so map a mask-library treatment there")):
        scaffold_treatments.map_deterministic(rel / "generated" / "abac.auto.tfvars", rel, NAME)
    assert (qa / "env.auto.tfvars").read_bytes() == qa_before


def test_the_account_tag_policy_accepts_custom_values_in_every_env():
    from scripts.split_abac_config import merge_tag_policies

    value = f"{NAME}__string__0123abcd"
    assert is_library_treatment_value("gr_treatment", value)
    assert not is_library_treatment_value("gr_treatment", "postcode_area__string__0123abcd")
    assert not is_library_treatment_value("other_key", value)
    # prod's split re-normalizes the account union, which dev's adopted value is already in.
    merged = merge_tag_policies([{"key": "gr_treatment", "values": [value]}],
                                [{"key": "gr_treatment", "values": ["email_partial__string__0123abcd"]}])
    assert merged[0]["values"] == [value, "email_partial__string__0123abcd"]


def test_verify_access_expects_custom_columns_by_tier():
    args = dict(column="c.s.t.x", treatment=NAME, library_default=NAME, access_tier_groups=TIERS,
                custom_treatments={NAME})
    assert resolve_precedence(**args, group="t1") == AccessResolution("raw")
    assert resolve_precedence(**args, group="t2") == AccessResolution("partial", NAME)
    assert resolve_precedence(**args, group="t3") == AccessResolution("full")
    assert resolve_precedence(**args, group="outsider") == AccessResolution("full")
    assert resolve_precedence(**args, group=(), principal="d", deployer_principal="d") == AccessResolution("raw")
    # Unknown to this env: fails closed for everyone.
    assert resolve_precedence(**{**args, "custom_treatments": ()}, group="t1") == AccessResolution("full")


def test_verify_access_spec_reads_the_envs_custom_masks(envs):
    from verify_effective_access import load_spec_from_tfvars

    dev = envs / "dev"
    _write_mask(dev)
    (dev / "env.auto.tfvars").write_text((dev / "env.auto.tfvars").read_text()
                                         + f'tag_treatments = {{ "class.postcode" = "{NAME}" }}\n')
    model = _model(dev)
    cfg = {"tag_assignments": model.tag_assignments, "fgac_policies": model.fgac_policies}
    hcl = dev / "split.auto.tfvars"
    hcl.write_text("".join(f"{key} = {json.dumps(value)}\n" for key, value in cfg.items()))
    spec = load_spec_from_tfvars(hcl, env_file=dev / "env.auto.tfvars", key_column="customer_id")
    (check,) = spec.column_masks
    expected = dict(check.expected_tiers)
    assert (expected["t1"], expected["t2"], expected["t3"]) == ("raw", "partial", "full")


# ── rehearse and release: the adopted cases, live and read-only ─────────────

def test_live_check_reruns_every_adopted_case_read_only(envs, capsys):
    dev = envs / "dev"
    _write_mask(dev)
    data = _mask_file()
    answers = {c["input"]: c["expected"] for c in data["partial"]["STRING"]["cases"]}
    warehouse = FakeWarehouse(answers)
    assert custom_masks.live_check(dev, "dev", warehouse=(warehouse, "wh-dev")) == 0
    out = capsys.readouterr().out
    assert f"PASS {NAME} (STRING): 4 exact-output cases" in out
    assert len(warehouse.statements) == 4
    assert all(s.startswith("SELECT to_json(named_struct('v', CAST((") and " FROM (SELECT " in s
               for _w, s in warehouse.statements)
    answers["CAST('2000' AS STRING)"] = "2000"
    assert custom_masks.live_check(dev, "dev", warehouse=(FakeWarehouse(answers), "wh-dev")) == 1
    err = capsys.readouterr().err
    assert f"FAIL {NAME} (STRING) input CAST('2000' AS STRING): expected '20**', got '2000'" in err
    assert "custom mask cases FAILED on envs/dev's warehouse" in err


def test_live_check_needs_a_warehouse_and_a_valid_file(envs):
    dev = envs / "dev"
    _write_mask(dev)
    (dev / "env.auto.tfvars").write_text('governance_mode = "deterministic"\n')
    with pytest.raises(custom_masks.Refused, match="set sql_warehouse_id"):
        custom_masks.live_check(dev, "dev")
    _write_mask(dev, _edited_sql(_mask_file()))
    with pytest.raises(custom_masks.CustomMaskError, match="tested_sha256"):
        custom_masks.live_check(dev, "dev", warehouse=(None, "wh"))


def _recipe(target):
    text = (SHARED / "Makefile.shared").read_text()
    match = re.search(rf"^{re.escape(target)}:.*?\n((?:\t.*\n)+)", text, re.MULTILINE)
    assert match, target
    return match.group(1)


def test_rehearse_and_release_run_the_cases_before_the_lock():
    makefile = (SHARED / "Makefile.shared").read_text()
    assert ('override _CUSTOM_MASK_CASES = python3 "$(SHARED_ROOT)/scripts/custom_masks.py" live-check '
            '--env-dir "$(ENV_DIR)" --env "$(ENV)"') in makefile
    # Before the env lock (and so before every step under it) in rehearse, release-plan and release.
    steps = makefile.split("override _REHEARSE_STEPS = ", 1)[1].split("\n\n", 1)[0]
    assert "_CUSTOM_MASK_CASES" not in steps
    for target in ("rehearse", "release-plan", "_release-manifest"):
        recipe = _recipe(target)
        assert "$(_CUSTOM_MASK_CASES) || exit 1" in recipe, target
        assert recipe.index("$(_CUSTOM_MASK_CASES)") < recipe.index("$(call _ENV_LOCK,"), target


# ── Weakening: custom_mask_changed ──────────────────────────────────────────

def _value(treatment, generation):
    return f"{treatment}__string__{generation}"


def _state_with(value, level="partial", principal="t2"):
    shown = f"{parse_treatment_value(value)[0]}"
    return GovernanceState(columns={COLUMN: {principal: Protection(level, f"{level} ({shown})", value)}})


@pytest.mark.parametrize("before, after", [
    (_value("email_partial", "11111111"), _value(NAME, "22222222")),     # library -> custom
    (_value(NAME, "11111111"), _value(NAME, "22222222")),                # custom SQL edit
    (_value("custom_other", "11111111"), _value(NAME, "11111111")),      # one custom for another
])
def test_a_custom_mask_change_is_a_weakening_to_acknowledge(before, after):
    (issue,) = find_weakenings(_state_with(before), _state_with(after), "prod")
    assert issue.category == "custom_mask_changed"
    assert issue.ack_key == f"prod:weaken:{COLUMN}:t2"
    unacked = check_weakenings(_state_with(before), _state_with(after), "prod", set())
    assert not unacked.ok
    acked = check_weakenings(_state_with(before), _state_with(after), "prod", {f"prod:weaken:{COLUMN}:t2"})
    assert acked.ok and acked.acknowledged == (issue,)


@pytest.mark.parametrize("before, after", [
    (_value(NAME, "11111111"), _value(NAME, "11111111")),                # unchanged
    (_value(NAME, "11111111"), _value("email_partial", "22222222")),     # custom -> library
    (_value("email_partial", "11111111"), _value("email_partial", "22222222")),  # library generation
])
def test_no_custom_mask_change_is_not_flagged(before, after):
    assert find_weakenings(_state_with(before), _state_with(after), "prod") == ()


def test_raw_and_redacted_tiers_are_not_custom_mask_changes():
    before, after = _value("email_partial", "11111111"), _value(NAME, "22222222")
    for level in ("raw", "redacted"):
        assert find_weakenings(_state_with(before, level), _state_with(after, level), "prod") == ()
    # redacted -> custom partial is the existing rank drop.
    (issue,) = find_weakenings(_state_with(before, "redacted"), _state_with(after), "prod")
    assert issue.category == "protection"


def test_release_gate_threads_the_value_and_names_the_custom_generation():
    from scripts.release_gate import governance_states

    def side(value):
        policies = [{"name": f"gr_mask_c_{value}_partial", "value": value, "to": ["t2"], "except": ["t1"]},
                    {"name": f"gr_mask_c_{value}_full", "value": value, "to": ["account users"], "except": ["t1", "t2"]}]
        return {"tags": {COLUMN: value}, "policies": policies, "settings": {}, "table_readers": {}}

    before, after = governance_states(side(_value("email_partial", "11111111")), side(_value(NAME, "22222222")))
    assert after.columns[COLUMN]["t2"] == Protection("partial", f"partial ({NAME}@22222222)", _value(NAME, "22222222"))
    (issue,) = find_weakenings(before, after, "prod")
    assert issue.category == "custom_mask_changed"
    assert issue.consequence == f"partial (email_partial) -> partial ({NAME}@22222222)"


def test_the_weakening_cli_reads_the_value(tmp_path):
    from weakening import main

    def state(value):
        return {"columns": {COLUMN: {"t2": {"level": "partial", "consequence": "p", "value": value}}}}

    before, after, ack = tmp_path / "b.json", tmp_path / "a.json", tmp_path / "ack.txt"
    before.write_text(json.dumps(state(_value(NAME, "11111111"))))
    after.write_text(json.dumps(state(_value(NAME, "22222222"))))
    ack.write_text("")
    assert main(["--env", "prod", "--before", str(before), "--after", str(after), "--ack-file", str(ack)]) == 1


# ── No model in governance ──────────────────────────────────────────────────

def _model_import_names(names):
    return {
        name for name in names
        if name.split(".")[0] in {"anthropic", "openai"}
        or "ai_helpers" in name.split(".")
    }


@pytest.mark.parametrize(("statement", "expected"), [
    ("from scripts import ai_helpers\n", "scripts.ai_helpers"),
    ("import scripts.ai_helpers as client\n", "scripts.ai_helpers"),
    ("import anthropic.types\n", "anthropic.types"),
    ("from openai import OpenAI\n", "openai"),
])
def test_the_custom_mask_model_import_guard_catches_mutations(tmp_path, statement, expected):
    assert _model_import_names({
        "openai",
        "anthropic.types",
        "scripts.ai_helpers",
        "scripts.ai_helpers.Client",
        "foo.ai_helpers",
        "governance_ai_helpers_notes",
    }) == {"openai", "anthropic.types", "scripts.ai_helpers", "scripts.ai_helpers.Client", "foo.ai_helpers"}

    from tests.test_no_model_in_governance import _import_closure

    mutant = tmp_path / "mutant.py"
    mutant.write_text(statement)
    imports = _import_closure([mutant])[mutant]
    assert expected in imports
    assert expected in _model_import_names(imports)


def test_custom_masks_never_imports_the_model_client():
    from tests.test_no_model_in_governance import _import_closure

    closure = _import_closure([SHARED / "scripts" / "custom_masks.py"])
    model_imports = {
        path: _model_import_names(names) for path, names in closure.items()
        if _model_import_names(names)
    }
    assert model_imports == {}, closure
    assert SHARED / "governed_tables_record.py" in closure
    assert "serving_endpoints" not in (SHARED / "scripts" / "custom_masks.py").read_text()
