"""A coverage gap's printed recovery sequence, followed end to end in a customer repo with a staging chain.

prod stops on a class.* tag no rule maps. make scaffold-treatments ENV=prod prints
"Next: ..." from coverage_fix.recovery_steps; each test runs that line, step by
step, through the real recipes (tests/test_champion_prod_walkthrough.py's
customer repo: Terraform is a stub, rehearse and release-plan run with the
stubbed recursive make and live checks). make generate is the one emulated
step: it needs live Unity Catalog, so the test rebuilds the env's model the way
generate does (build_model + write_deterministic_model, then the same-env
split and governed-tables record) and make check verifies the result.

Two staging chains:
  - stg has no promote_from (prod promotes from a rehearsed stg): stg owns
    tag_treatments, and the sequence reaches a release manifest;
  - stg is promoted from dev: promote-to ENV=stg rewrites stg's tag_treatments
    from dev, so dev owns the mapping (and the custom mask); make rehearse
    refuses a promoted stg, so the sequence stops after check ENV=prod and says
    why instead of printing a rehearse that can't run.
"""

from __future__ import annotations

import re
import subprocess
import sys

import hcl2
import pytest

from generate_abac import write_deterministic_model
from governance_policies import build_model, column_types_from_ddl
from scripts import coverage_fix, custom_masks, release_gate as rg
from tests.test_champion_prod_walkthrough import SHARED, customer  # noqa: F401  (the customer fixture)
from tests.test_custom_masks import NAME, _write_mask
from tests.test_deterministic_check_promote import CLASSES, DDL, DEPLOYER, DEV_ENV, TIERS, _auth, _write_generated

LABEL = "class.postcode"
STEP = re.compile(r"commit and push envs/|commit (envs/\S+?),|make [a-z-]+ ENV=[a-z]+")


def _push_if_changed(repo, message: str) -> None:
    if repo.git("status", "--porcelain"):
        repo.git("add", "-A")
        repo.git("commit", "-q", "-m", message)
        repo.git("push", "-q")


def _record(env_dir) -> None:
    subprocess.run([sys.executable, str(SHARED / "scripts" / "sticky_governance.py"), "record",
                    "--env-dir", str(env_dir), "--config", str(env_dir / "generated" / "abac.auto.tfvars")],
                   check=True)


def _generate(repo, env: str) -> None:
    """make generate ENV=<root env>, offline: the env's model from its own settings and adopted masks."""
    env_dir = repo.cloud / "envs" / env
    settings = hcl2.loads((env_dir / "env.auto.tfvars").read_text())
    model = build_model(CLASSES, column_types_from_ddl(DDL), settings, deployer_principal=DEPLOYER,
                        library=custom_masks.effective_library(env_dir))
    write_deterministic_model(env_dir / "generated" / "abac.auto.tfvars",
                              env_dir / "generated" / "masking_functions.sql", model)
    result = repo.make("promote", f"ENV={env}")
    assert result.returncode == 0, result.stdout + result.stderr
    _record(env_dir)


def _stub(repo, target: str, env: str):
    # rehearse and release-plan run the custom-mask cases on the env's warehouse first.
    return repo.stubbed(target, env, cwd=repo.cloud,
                        stubs={"custom_masks.py live-check": "true"})


def _setup(repo, env: str, source: str, catalog_map: str) -> None:
    """make setup ENV=<env>, then what the walkthrough asks for before promote-to."""
    result = repo.make("setup", f"ENV={env}")
    assert result.returncode == 0, result.stdout + result.stderr
    env_dir = repo.cloud / "envs" / env
    (env_dir / "auth.auto.tfvars").write_text(_auth(DEPLOYER, host=""))
    text = (env_dir / "env.auto.tfvars").read_text()
    text = re.sub(r'deployer_principal_id\s*=\s*"[^"]*"', f'deployer_principal_id = "{DEPLOYER}"', text)
    text = re.sub(r"access_tier_groups\s*=\s*\[[^\]]*\]", f"access_tier_groups = {TIERS!r}".replace("'", '"'), text)
    text = re.sub(r"^(promote_from|catalog_map)\s*=.*$", "", text, flags=re.M)
    (env_dir / "env.auto.tfvars").write_text(text + f'promote_from = "{source}"\ncatalog_map = {catalog_map}\n')
    result = repo.make("promote-to", f"ENV={env}")
    assert result.returncode == 0, result.stdout + result.stderr


def _root_stg(repo) -> None:
    """stg as a second rehearsed env with its own rules (no promote_from), like dev."""
    result = repo.make("setup", "ENV=stg")
    assert result.returncode == 0, result.stdout + result.stderr
    stg = repo.cloud / "envs" / "stg"
    (stg / "env.auto.tfvars").write_text(DEV_ENV)
    (stg / "auth.auto.tfvars").write_text(_auth(DEPLOYER))
    (stg / "generated").rmdir()
    _write_generated(stg)
    result = repo.make("promote", "ENV=stg")
    assert result.returncode == 0, result.stdout + result.stderr
    _record(stg)
    _push_if_changed(repo, "Govern stg")
    assert _stub(repo, "rehearse", "stg").returncode == 0


def _scaffold(repo, column: str, treatment: str):
    """prod's generate output records the unmapped class; scaffold-treatments maps it."""
    generated = repo.cloud / "envs" / "prod" / "generated" / "abac.auto.tfvars"
    text = generated.read_text()
    generated.write_text(f"# gr.classification_unmapped: {column}|{LABEL}\n" + text)
    result = repo.make("scaffold-treatments", "ENV=prod", f"TREATMENT={treatment}")
    generated.write_text(text)
    return result


def _follow(repo, next_line: str) -> dict[str, str]:
    """Run every step of a printed recovery sequence, in order; committing and pushing after each change."""
    out = {}
    for match in STEP.finditer(next_line):
        step = match.group(0)
        if step.startswith("commit"):
            _push_if_changed(repo, step)
            out[step] = ""
            continue
        target, env = step.split()[1], step.split("=")[1]
        if target == "generate":
            _generate(repo, env)
            result = None
        elif target in ("rehearse", "release-plan"):
            result = _stub(repo, target, env)
        elif target == "release":
            break  # release applies; release-plan's manifest is the end of what runs offline
        else:
            result = repo.make(target, f"ENV={env}")
        text = out[step] = (result.stdout + result.stderr) if result else ""
        if target == "check":
            # Offline, check's plan step needs the env's live classification config.
            assert f"OK: generated files (envs/{env}): committed files match." in text, text
            if env != "dev" and env != coverage_fix.promotion_chain(repo.cloud / "envs" / env)[0]:
                assert f"OK: promote-to ENV={env} CHECK=1: committed files match." in text, text
            assert result.returncode == 0 or f"=== make check ENV={env}: FAILED (plan) ===" in text, text
        elif result is not None:
            assert result.returncode == 0, f"{step}:\n{text}"
        _push_if_changed(repo, step)
    return out


def _next(result) -> str:
    assert result.returncode == 0, result.stdout + result.stderr
    return next(line for line in result.stdout.splitlines() if line.startswith("Next: "))


def test_a_prod_promoted_from_a_rehearsed_stg_follows_the_printed_steps_to_a_release_manifest(customer):
    repo = customer
    _root_stg(repo)
    _setup(repo, "prod", "stg", '{ "paycat" = "ppay" }')
    _push_if_changed(repo, "Promote stg to prod")
    stg, prod = repo.cloud / "envs" / "stg", repo.cloud / "envs" / "prod"

    # A custom mask can't reach a root stg: adopt-custom-mask runs only in dev, and nothing promotes dev to stg.
    _write_mask(repo.cloud / "envs" / "dev")
    refused = _scaffold(repo, "ppay.s.t.email", NAME)
    assert refused.returncode != 0 and "envs/stg has no promote_from, so map a mask-library treatment there" \
        in refused.stdout + refused.stderr
    (repo.cloud / "envs" / "dev" / "custom_masks" / f"{NAME}.json").unlink()
    (repo.cloud / "envs" / "dev" / "custom_masks").rmdir()

    next_line = _next(_scaffold(repo, "ppay.s.t.email", "redact"))
    assert hcl2.loads((stg / "env.auto.tfvars").read_text())["tag_treatments"] == {LABEL: "redact"}
    assert re.findall(r"make [a-z-]+ ENV=[a-z]+", next_line) == [
        "make generate ENV=stg", "make check ENV=stg", "make rehearse ENV=stg", "make promote-to ENV=prod",
        "make check ENV=prod", "make rehearse ENV=stg", "make release-plan ENV=prod", "make release ENV=prod"]

    out = _follow(repo, next_line)

    assert hcl2.loads((prod / "env.auto.tfvars").read_text())["tag_treatments"] == {LABEL: "redact"}
    assert repo.git("status", "--porcelain") == ""
    assert "without re-applying" in out["make rehearse ENV=stg"] or "Rehearse (stg)" in out["make rehearse ENV=stg"]
    assert rg.passing_receipt(repo.cloud, "stg", repo.head())
    manifest = rg.current_manifest(prod, "prod")
    assert manifest and manifest.is_file(), out["make release-plan ENV=prod"]


def test_a_prod_promoted_through_a_promoted_stg_maps_and_adopts_in_dev_then_stops_where_rehearse_refuses(customer):
    repo = customer
    _setup(repo, "stg", "dev", '{ "paycat" = "spay" }')
    _setup(repo, "prod", "stg", '{ "spay" = "ppay" }')
    _push_if_changed(repo, "Promote dev to stg to prod")
    dev, stg, prod = (repo.cloud / "envs" / name for name in ("dev", "stg", "prod"))

    # The old advice: map in stg. promote-to owns stg's tag_treatments, so make check ENV=stg rejects the edit.
    stg_file = (stg / "env.auto.tfvars").read_text()
    (stg / "env.auto.tfvars").write_text(stg_file + f'tag_treatments = {{ "{LABEL}" = "redact" }}\n')
    _push_if_changed(repo, "hand-map in stg")
    hand_edit = repo.make("check", "ENV=stg")
    assert hand_edit.returncode != 0 and "Run make promote-to ENV=stg" in hand_edit.stdout + hand_edit.stderr
    repo.git("revert", "--no-edit", "HEAD")
    repo.git("push", "-q")

    # The custom mask goes where adopt-custom-mask writes it: envs/dev/custom_masks/.
    refused = _scaffold(repo, "ppay.s.t.email", NAME)
    assert "Adopt it first: make adopt-custom-mask ENV=dev TREATMENT=" + NAME in refused.stdout + refused.stderr
    _write_mask(dev)
    next_line = _next(_scaffold(repo, "ppay.s.t.email", NAME))
    assert hcl2.loads((dev / "env.auto.tfvars").read_text())["tag_treatments"] == {LABEL: NAME}
    assert (stg / "env.auto.tfvars").read_text() == stg_file
    assert re.findall(r"make [a-z-]+ ENV=[a-z]+", next_line) == [
        "make generate ENV=dev", "make check ENV=dev", "make rehearse ENV=dev", "make promote-to ENV=stg",
        "make check ENV=stg", "make promote-to ENV=prod", "make check ENV=prod"]
    assert "release-plan ENV=prod needs a passing rehearsal of stg" in next_line

    _follow(repo, next_line)

    for env_dir in (stg, prod):  # promote-to carried both the mapping and the adopted mask
        assert hcl2.loads((env_dir / "env.auto.tfvars").read_text())["tag_treatments"] == {LABEL: NAME}
        assert (env_dir / "custom_masks" / f"{NAME}.json").read_bytes() == \
            (dev / "custom_masks" / f"{NAME}.json").read_bytes()
    assert repo.git("status", "--porcelain") == ""
    # Where the printed sequence stops, and why: no rehearse of a promoted stg, so no release-plan of prod.
    rehearse = _stub(repo, "rehearse", "stg")
    assert rehearse.returncode != 0 and "ENV=stg is promoted" in rehearse.stdout + rehearse.stderr
    plan = _stub(repo, "release-plan", "prod")
    assert plan.returncode != 0 and "no passing rehearsal" in plan.stdout + plan.stderr


@pytest.mark.parametrize("stg_from, owner", [("", "stg"), ("dev", "dev")])
def test_every_fix_for_a_staging_chain_names_the_owner_from_promotion_chain(customer, stg_from, owner):
    repo = customer
    if stg_from:
        _setup(repo, "stg", "dev", '{ "paycat" = "spay" }')
    else:
        _root_stg(repo)
    _setup(repo, "prod", "stg", '{ "spay" = "ppay" }' if stg_from else '{ "paycat" = "ppay" }')
    prod = repo.cloud / "envs" / "prod"
    chain = coverage_fix.promotion_chain(prod)
    assert chain[0] == owner and chain[-1] == "prod"
    steps = coverage_fix.recovery_steps(chain, f"envs/{owner}/env.auto.tfvars")
    for text in ("\n".join(coverage_fix.fix_lines("prod", prod, [("ppay.s.t.email", LABEL)])),
                 "\n".join(coverage_fix.materialize_lines("prod", prod, ["redact"]))):
        assert f"then {steps}" in text
        assert ("make adopt-custom-mask ENV=dev TREATMENT=custom_<name>" in text) == (owner == "dev")
        assert ("envs/stg has no promote_from, so map a mask-library treatment there" in text) == (owner == "stg")
