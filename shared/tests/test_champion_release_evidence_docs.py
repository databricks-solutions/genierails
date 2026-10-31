"""The champion walkthrough after release: live evidence, then maintain.

make evidence is live by default and resolves the SQL warehouse itself
(resolve_env_config.py warehouse), so the documented command carries no
GENIERAILS_EVIDENCE_INTEGRATION and no WAREHOUSE_ID. Its record goes to the
git-ignored state dir, so no doc asks to delete an evidence folder first. The README
also once said adopt-custom-mask was not on main; it is. make maintain runs
on the deployment machine (cron) at the last released commit, never in CI.
Dev step 4 says what a generate re-run keeps in each governance mode.
"""

import re
from pathlib import Path

SHARED = Path(__file__).parents[1]
README = SHARED / "examples/dev_to_prod/README.md"
REFERENCE = SHARED / "examples/dev_to_prod/REFERENCE.md"
EVIDENCE_DOC = SHARED / "docs/evidence-report.md"
MAKEFILE = SHARED / "Makefile.shared"

EVIDENCE = "make evidence ENV=prod"


def _section(text: str, start: str, end: str) -> str:
    return text[text.index(start):text.index(end)]


def test_the_whole_flow_records_evidence_after_release_and_before_maintain():
    flow = _section(README.read_text(), "## The whole flow", "## Before you start")
    lines = [line.split("#")[0].strip() for line in flow.splitlines()]
    assert EVIDENCE in lines
    # make release runs release-plan itself, round by round: no separate release-plan line.
    assert "make release-plan ENV=prod" not in lines
    order = ["make promote-to ENV=prod", "make release ENV=prod", EVIDENCE, "make maintain ENV=prod"]
    assert [lines.index(step) for step in order] == sorted(lines.index(step) for step in order)
    assert "make evidence ENV=prod      # live compliance record" in flow.splitlines()


def test_prod_step_4_records_evidence_after_the_release_and_its_stop_advice():
    prod = _section(README.read_text(), "## Prod", "## Keep it safe")
    step = prod[prod.index("**4. Record evidence.**"):]
    assert prod.index("make release ENV=prod") < prod.index("**If it stops on coverage or drift**") \
        < prod.index("**4. Record evidence.**")
    assert f"```bash\n{EVIDENCE}\n```" in step
    assert "writes a JSON record and Markdown summary to `.genierails-state/prod/evidence/` (git-ignored" in step
    assert "`OFFLINE=1` writes a config-only report" in step
    assert "The checkout stays clean, so `make maintain` and the next `release-plan` run as usual." in step


def test_no_doc_asks_to_delete_the_evidence_folder_or_set_the_old_variable():
    reference = REFERENCE.read_text()
    for path in (README, REFERENCE, EVIDENCE_DOC, SHARED.parent / "REFERENCE.md"):
        text = path.read_text()
        assert "generated/evidence/` before" not in text and "delete that folder" not in text, path
        assert "remove the folder before" not in text and "then delete" not in text, path
        assert not re.search(r"GENIERAILS_EVIDENCE_INTEGRATION=1 make evidence", text), path
        assert "envs/prod/generated/evidence" not in text, path
    assert ".genierails-state/prod/evidence/" in reference
    doc = EVIDENCE_DOC.read_text()
    assert "`.genierails-state/<env>/evidence/`" in doc and "`$GENIERAILS_STATE_DIR/<cloud>/<env>/evidence/`" in doc
    assert "`OFFLINE=1` writes a config-only report" in doc
    # Only the default folder is git-ignored; a custom state dir must be outside every checkout or ignored.
    for rule in ("finds every git work tree around it, nested repos included", "a personal `core.excludesFile` doesn't count",
                 "`GIT_*` environment variables such as `GIT_DIR` are ignored for this check",
                 "If any check fails, or git can't be run, it exits 2 and writes nothing."):
        assert rule in doc, rule
    assert "The default folder is git-ignored" in doc and "The folder is git-ignored" not in doc
    custom = "custom `GENIERAILS_STATE_DIR` must be outside every git checkout or git-ignored in each one"
    assert custom in doc and custom in reference
    assert "`GENIERAILS_STATE_DIR`, which must be outside every git checkout or git-ignored in each one" in (SHARED.parent / "REFERENCE.md").read_text()
    assert "`$GENIERAILS_STATE_DIR` when that is set, which must be outside every git checkout or git-ignored in each one" in README.read_text()
    assert "`GENIERAILS_EVIDENCE_INTEGRATION=1`, which older versions needed for live collection, is still accepted" in doc
    # ...because make evidence writes there, live unless OFFLINE=1.
    makefile = MAKEFILE.read_text()
    recipe = makefile[makefile.index("\nevidence:"):makefile.index("\naudit-rulebook:")]
    assert 'GENIERAILS_EVIDENCE_INTEGRATION=1 python3 "$(SHARED_ROOT)/evidence_report.py" --warehouse-id' in recipe
    assert '"$(OFFLINE)"' in recipe
    assert "root / Path(env_dir).resolve().name / \"evidence\"" in (SHARED / "evidence_report.py").read_text()


def test_no_doc_requires_an_explicit_warehouse_for_evidence():
    for path in (README, REFERENCE, EVIDENCE_DOC):
        text = path.read_text()
        assert not re.search(r"make evidence[^\n`]*WAREHOUSE_ID=", text), path
        assert "+ `WAREHOUSE_ID`" not in text, path
    assert f"```bash\n{EVIDENCE}\n```" in EVIDENCE_DOC.read_text()
    assert "`WAREHOUSE_ID=<id>` is optional" in EVIDENCE_DOC.read_text()


def test_make_evidence_resolves_the_warehouse_it_documents():
    makefile = MAKEFILE.read_text()
    recipe = makefile[makefile.index("\nevidence:"):makefile.index("\naudit-rulebook:")]
    assert 'resolve_env_config.py" warehouse --env-dir "$(ENV_DIR)" --explicit "$(WAREHOUSE_ID)"' in recipe


def test_readme_no_longer_says_adopt_custom_mask_is_not_on_main():
    readme = README.read_text()
    assert "is not on `main` yet" not in readme
    assert "make adopt-custom-mask ENV=dev TREATMENT=custom_<name>" in readme
    # ...because the target is on main.
    assert re.search(r"^adopt-custom-mask:", MAKEFILE.read_text(), re.MULTILINE)


def test_maintain_runs_on_the_deployment_machine_at_the_last_released_commit_not_ci():
    readme = README.read_text()
    flow = _section(readme, "## The whole flow", "## Before you start")
    safe = _section(readme, "## Keep it safe", "## Commit it")
    line = "make maintain ENV=prod      # deployment machine, cron, at the last released commit"
    assert line in flow.splitlines() and line in safe.splitlines()
    assert "Run it from the [deployment machine](../../docs/deployment-machine.md)." in safe
    assert "In a deterministic env it runs only from the last released commit, on a clean checkout." in safe
    # No walkthrough line puts maintain in CI.
    for path in (README, REFERENCE):
        for text in path.read_text().splitlines():
            if "maintain" in text:
                assert not re.search(r"\b(in|from|on) CI\b|CI (job|pipeline|schedule)|schedule[^.]*\bCI\b", text), (path, text)
    applying = _section(REFERENCE.read_text(), "### Applying from CI", "## Step details")
    assert "`maintain`" in applying and "refuse common CI markers" in applying
    # ...because maintain refuses in CI and, deterministic, off the last release.
    makefile = MAKEFILE.read_text()
    assert re.search(r"^maintain: _guard-not-ci-apply ", makefile, re.MULTILINE)
    recipe = makefile[makefile.index("\nmaintain:"):]
    assert '$(_RELEASE_GATE) maintain-check --env-dir "$(ENV_DIR)" --env "$(ENV)"' in recipe[:recipe.index("\n\n")]


def _generate_step(readme: str) -> str:
    dev = _section(readme, "## Dev", "## Prod")
    return dev[dev.index("**4. Generate.**"):dev.index("**5. Rehearse.**")]


def test_dev_step_4_says_a_deterministic_generate_replaces_masks_and_points_at_the_settings():
    step = _generate_step(README.read_text())
    assert "What a re-run keeps depends on the env's `governance_mode`:" in step
    det = next(line for line in step.splitlines() if line.startswith("- **Deterministic**"))
    for replaced in ("the column masks", "the `gr_treatment` column tags", "the `gr_treatment` tag policy",
                     "the mask-library functions in `masking_functions.sql`"):
        assert replaced in det, replaced
    assert "It replaces hand edits to those in `envs/dev/generated/` without a warning" in det
    for setting in ("`column_overrides` for one column", "`treatment_versions` for a treatment's partial version",
                    "`tag_treatments` to map a class to another treatment"):
        assert setting in det, setting
    assert "[settings](REFERENCE.md#deterministic-governance-settings)" in det
    assert "### Deterministic-governance settings" in REFERENCE.read_text()
    # ...and what it keeps, custom SQL above the mask-library marker included.
    marker = "-- === GenieRails deterministic mask library ==="
    assert f"row filters, table tags and any custom SQL above the `{marker}` marker in `masking_functions.sql` are kept" in det
    legacy = next(line for line in step.splitlines() if line.startswith("- **Legacy**"))
    assert "including your edits" in legacy
    # The edits-survive promise is only made for legacy.
    assert step.count("including your edits") == 1
    assert "edit the rules if you need to" not in step
    # ...because deterministic generate replaces every column mask and the gr_treatment tag policy.
    policies = (SHARED / "governance_policies.py").read_text()
    apply_model = policies[policies.index("def apply_model("):policies.index("def replace_sql_library(")]
    assert 'policy.get("policy_type") != "POLICY_TYPE_COLUMN_MASK"' in apply_model
    assert 'policy.get("key") != TAG_KEY' in apply_model
    assert '"key": TAG_KEY, "description": TAG_POLICY_DESCRIPTION, "values": model.tag_values' in apply_model
    from governance_policies import SQL_MARKER, replace_sql_library
    assert SQL_MARKER == marker
    replaced = replace_sql_library(f"-- custom\nCREATE FUNCTION mine() RETURN 1;\n\n{marker}\nold library\n", "new library")
    assert replaced.startswith("-- custom\nCREATE FUNCTION mine() RETURN 1;") and "old library" not in replaced


def test_reference_generate_says_what_a_re_run_keeps_per_mode():
    reference = REFERENCE.read_text()
    row = next(line for line in reference.splitlines() if line.startswith("| `make generate ENV=<e>` |"))
    assert "Deterministic envs: every run replaces the column masks" in row
    assert "hand edits to them are lost" in row
    assert "Legacy envs: re-runs keep reviewed rules" in row
    details = reference[reference.index("**`make generate ENV=dev`**"):reference.index("**`make rehearse ENV=dev`**")]
    assert "- Deterministic envs: every run recomputes the column masks" in details
    assert "- Legacy envs: re-runs keep reviewed rules" in details
    assert "\n- Re-runs keep reviewed rules" not in details
    assert "any custom SQL above the `-- === GenieRails deterministic mask library ===` marker" in details


def _rows(reference: str, command: str) -> list[str]:
    return [line for line in reference.splitlines() if line.startswith(f"| `{command}` |")]


def test_reference_scopes_the_direct_release_and_apply_paths_to_legacy():
    reference = REFERENCE.read_text()
    legacy, manifest = _rows(reference, "make release ENV=prod")
    assert legacy.startswith("| `make release ENV=prod` | 4 | (legacy prod only; a deterministic prod uses the "
                             "`release-plan` and manifest `release` rows below) Placeholder guard"), legacy
    assert manifest.startswith("| `make release ENV=prod` | 4 | (deterministic prod) At a terminal (outside CI, no "
                               "`MANIFEST=`): the whole release, round by round"), manifest
    assert "Otherwise (pipelines, CI, no terminal) one round: releases the current manifest" in manifest
    [apply] = _rows(reference, "make apply ENV=<e>")
    assert "(legacy envs; a deterministic env runs it only inside `make rehearse` (dev), and a promoted one " \
        "refuses it: release through `release-plan`/`release`)" in apply
    [governance] = _rows(reference, "make apply-governance ENV=<e>")
    assert "(legacy envs; a deterministic env runs it only inside `rehearse`/`ungovern` (dev) or `maintain` " \
        "(promoted envs))" in governance
    details = reference[reference.index("**`make release ENV=prod`**"):reference.index("**`make maintain ENV=prod`**")]
    bullets = [line for line in details.splitlines() if line.startswith("- ")]
    assert bullets[0].startswith("- Deterministic envs: at a terminal, `make release` runs the whole release round "
                                 "by round: `make release-plan`, the manifest, `y/N`, then exactly that manifest. "
                                 "Off a terminal (or with `MANIFEST=`) it applies only the current manifest that "
                                 "`make release-plan` wrote, one round per call."), bullets[0]
    assert bullets[1].startswith("- Legacy envs, order: placeholder guard"), bullets[1]
    withdraw = next(line for line in bullets if line.startswith("- To withdraw access"))
    assert "run `make apply ENV=prod` (legacy) or commit it and run `make release ENV=prod` (deterministic, " \
        "where `make apply ENV=prod` refuses; a pipeline runs `make release-plan ENV=prod`, then " \
        "`make release ENV=prod MANIFEST=<path>`)" in withdraw
    # ...because a deterministic release is the manifest path and the apply guard owns apply there.
    makefile = MAKEFILE.read_text()
    release = makefile[makefile.index("\nrelease:"):]
    release = release[:release.index("\n\n")]
    # At a terminal (no MANIFEST on the command line) the round loop; otherwise exactly one manifest release.
    loop = release.index('if [ "$(origin MANIFEST)" = undefined ]; then')
    assert loop < release.index('python3 "$(SHARED_ROOT)/scripts/release_loop.py" --env "$(ENV)"') \
        < release.index("$(MAKE) --no-print-directory _release-manifest")
    from scripts import release_gate as rg
    assert rg.APPLY_OWNERS[rg.PROMOTED]["apply"] == ()
    assert rg.APPLY_OWNERS[rg.PROMOTED]["apply-governance"] == ("maintain",)
    assert rg.APPLY_OWNERS[rg.REHEARSE]["apply"] == ("rehearse",)
    assert rg.APPLY_OWNERS[rg.REHEARSE]["apply-governance"] == ("rehearse", "ungovern")
