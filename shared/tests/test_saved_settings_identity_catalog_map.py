"""A catalog_map entry that maps a catalog to itself (finding S2).

On a shared metastore, paycat=paycat gives prod the policy names dev already
deploys, so prod's state could adopt dev's masks and a later dev retire would
drop prod's. Every entry point refuses it before writing anything, unless the
destination's own env.auto.tfvars sets allow_identity_catalog_map = true (for
envs on separate metastores).
"""

import sys

import hcl2
import pytest

import genie_space_placeholder
from scripts import remap_env_config, runtime_overlay, saved_settings, staged_policy_apply, validate_catalog_map
from tests.test_champion_command_simplify import (  # noqa: F401  (promote_cloud is a fixture)
    _env_cfg,
    _promotion_settings,
    _snapshot,
    promote_cloud,
)

OPT_IN = "allow_identity_catalog_map = true\n"


def _two_catalog_dev(envs):
    (envs / "dev/env.auto.tfvars").write_text(
        'genie_spaces = [\n  { genie_space_id = "s1", uc_tables = ["paycat.s.t", "hrcat.s.u"] },\n]\n')


@pytest.mark.parametrize("settings, args, shown", [
    ('promote_from = "dev"\ncatalog_map = { "paycat" = "paycat" }\n', (),
     "catalog_map in envs/prod/env.auto.tfvars maps a catalog to itself (paycat=paycat)"),
    ('promote_from = "dev"\n', ("FROM=dev", "CATALOG_MAP=paycat=paycat"),
     "CATALOG_MAP maps a catalog to itself (paycat=paycat)"),
    # UC catalog names ignore case: PayCat is paycat.
    ('promote_from = "dev"\ncatalog_map = { "paycat" = "PayCat" }\n', (),
     "maps a catalog to itself (paycat=PayCat)"),
    # Only the HCL boolean true opts in.
    ('promote_from = "dev"\ncatalog_map = { "paycat" = "paycat" }\nallow_identity_catalog_map = "true"\n', (),
     "maps a catalog to itself (paycat=paycat)"),
])
def test_promote_to_refuses_an_identity_map_and_writes_nothing(promote_cloud, settings, args, shown):
    prod = _promotion_settings(promote_cloud.envs, settings)
    before = _snapshot(promote_cloud.envs)

    result = promote_cloud("promote-to", "ENV=prod", *args)

    out = result.stdout + result.stderr
    assert result.returncode != 0, out
    assert shown in out
    assert 'Map each to a distinct prod catalog (e.g. "paycat" = "<prod_catalog>")' in out
    assert "only if the envs use separate metastores, set allow_identity_catalog_map = true in envs/prod/env.auto.tfvars" in out
    assert "Cross-env promote" not in out
    assert _snapshot(promote_cloud.envs) == before
    assert sorted(path.name for path in prod.iterdir()) == ["env.auto.tfvars"]


def test_promote_to_refuses_a_mixed_map_with_one_identity_entry(promote_cloud):
    _two_catalog_dev(promote_cloud.envs)
    prod = _promotion_settings(
        promote_cloud.envs, 'promote_from = "dev"\ncatalog_map = { "paycat" = "ppay", "hrcat" = "hrcat" }\n')
    before = _snapshot(promote_cloud.envs)

    result = promote_cloud("promote-to", "ENV=prod")

    out = result.stdout + result.stderr
    assert result.returncode != 0, out
    assert "maps a catalog to itself (hrcat=hrcat)" in out
    assert "paycat=ppay" not in out.split("maps a catalog to itself", 1)[1].split(".", 1)[0]
    assert _snapshot(promote_cloud.envs) == before
    assert sorted(path.name for path in prod.iterdir()) == ["env.auto.tfvars"]


def test_promote_to_refusal_after_a_successful_promote_restores_nothing_changed(promote_cloud):
    """A later identity CATALOG_MAP leaves the promoted prod tree exactly as it was."""
    assert promote_cloud("promote-to", "ENV=prod", "FROM=dev", "CATALOG_MAP=paycat=ppay").returncode == 0
    before = _snapshot(promote_cloud.envs)

    result = promote_cloud("promote-to", "ENV=prod", "CATALOG_MAP=paycat=paycat")

    assert result.returncode != 0
    assert "CATALOG_MAP maps a catalog to itself (paycat=paycat)" in result.stdout + result.stderr
    assert _snapshot(promote_cloud.envs) == before
    assert _env_cfg(promote_cloud.envs, "prod")["catalog_map"] == {"paycat": "ppay"}


def test_promote_to_check_refuses_an_identity_map(promote_cloud):
    _promotion_settings(promote_cloud.envs, 'promote_from = "dev"\ncatalog_map = { "paycat" = "paycat" }\n')
    promote_cloud.commit()
    before = _snapshot(promote_cloud.envs)

    result = promote_cloud("promote-to", "ENV=prod", "CHECK=1")

    assert result.returncode != 0
    assert "maps a catalog to itself (paycat=paycat)" in result.stdout + result.stderr
    assert _snapshot(promote_cloud.envs) == before


def test_promote_to_with_the_destination_opt_in_promotes_and_keeps_it(promote_cloud):
    _promotion_settings(promote_cloud.envs, 'promote_from = "dev"\ncatalog_map = { "paycat" = "paycat" }\n' + OPT_IN)

    first = promote_cloud("promote-to", "ENV=prod")

    assert first.returncode == 0, first.stdout + first.stderr
    prod = _env_cfg(promote_cloud.envs, "prod")
    assert prod["uc_tables"] == ["paycat.s.t"]
    assert prod["catalog_map"] == {"paycat": "paycat"}
    assert prod["allow_identity_catalog_map"] is True
    # The opt-in survives the rewrite, so the next promote needs no flag.
    again = promote_cloud("promote-to", "ENV=prod")
    assert again.returncode == 0, again.stdout + again.stderr
    assert _env_cfg(promote_cloud.envs, "prod")["allow_identity_catalog_map"] is True


def test_an_opt_in_in_the_source_env_does_not_count(promote_cloud):
    dev = promote_cloud.envs / "dev/env.auto.tfvars"
    dev.write_text(dev.read_text() + OPT_IN)
    _promotion_settings(promote_cloud.envs, 'promote_from = "dev"\ncatalog_map = { "paycat" = "paycat" }\n')
    before = _snapshot(promote_cloud.envs)

    result = promote_cloud("promote-to", "ENV=prod")

    assert result.returncode != 0
    assert "maps a catalog to itself (paycat=paycat)" in result.stdout + result.stderr
    assert _snapshot(promote_cloud.envs) == before


def test_cross_env_promote_refuses_an_identity_dest_catalog_map(promote_cloud):
    prod = _promotion_settings(promote_cloud.envs, 'governance_mode = "legacy"\n')
    # make promote's bootstrap prerequisite may seed dev and account first; the
    # destination is never touched.
    before = _snapshot(prod)

    result = promote_cloud("promote", "ENV=dev", "SOURCE_ENV=dev", "DEST_ENV=prod", "DEST_CATALOG_MAP=paycat=paycat")

    out = result.stdout + result.stderr
    assert result.returncode != 0, out
    assert "DEST_CATALOG_MAP maps a catalog to itself (paycat=paycat)" in out
    assert "Cross-env promote" not in out
    assert _snapshot(prod) == before
    assert sorted(path.name for path in prod.iterdir()) == ["env.auto.tfvars"]


def test_promote_resolve_names_every_identity_entry(tmp_path):
    envs = tmp_path / "envs"
    (envs / "dev").mkdir(parents=True)
    (envs / "prod").mkdir()
    (envs / "dev/env.auto.tfvars").write_text('uc_tables = ["a.s.t", "b.s.t", "c.s.t"]\n')
    (envs / "prod/env.auto.tfvars").write_text('promote_from = "dev"\ncatalog_map = { a = "a", b = "pb", c = "c" }\n')

    with pytest.raises(ValueError, match=r"maps a catalog to itself \(a=a, c=c\)"):
        saved_settings.resolve_promote("prod", envs / "prod", envs, "", "")
    (envs / "prod/env.auto.tfvars").write_text(
        'promote_from = "dev"\ncatalog_map = { a = "a", b = "pb", c = "c" }\n' + OPT_IN)
    assert saved_settings.resolve_promote("prod", envs / "prod", envs, "", "") == "dev a=a,b=pb,c=c"


def _validate(monkeypatch, *args):
    monkeypatch.setattr(sys, "argv", ["validate_catalog_map.py", *map(str, args)])
    validate_catalog_map.main()


def test_validate_catalog_map_refuses_identity_unless_the_destination_opts_in(tmp_path, monkeypatch, capsys):
    source, dest = tmp_path / "dev", tmp_path / "prod"
    source.mkdir(); dest.mkdir()
    (source / "env.auto.tfvars").write_text('uc_tables = ["paycat.s.t", "hrcat.s.u"]\n')

    for args in ((source, "paycat=paycat"), (source, "paycat=ppay,hrcat=hrcat", dest)):
        with pytest.raises(SystemExit) as exc:
            _validate(monkeypatch, *args)
        assert exc.value.code == 1
        assert "DEST_CATALOG_MAP maps a catalog to itself" in capsys.readouterr().out

    (dest / "env.auto.tfvars").write_text(OPT_IN)
    _validate(monkeypatch, source, "paycat=paycat,hrcat=hrcat", dest)
    _validate(monkeypatch, source, "paycat=ppay,hrcat=phr")


def test_remap_env_config_keeps_only_the_destination_opt_in(tmp_path, monkeypatch):
    source, dest = tmp_path / "dev", tmp_path / "prod"
    source.mkdir(); dest.mkdir()
    (source / "env.auto.tfvars").write_text('uc_tables = ["paycat.s.t"]\n' + OPT_IN)
    (dest / "env.auto.tfvars").write_text('uc_tables = ["paycat.s.t"]\n')
    monkeypatch.setattr(sys, "argv", ["remap_env_config.py", str(source), str(dest), "paycat=paycat"])
    remap_env_config.main()
    assert "allow_identity_catalog_map" not in hcl2.loads((dest / "env.auto.tfvars").read_text())

    (dest / "env.auto.tfvars").write_text('uc_tables = ["paycat.s.t"]\n' + OPT_IN)
    remap_env_config.main()
    assert hcl2.loads((dest / "env.auto.tfvars").read_text())["allow_identity_catalog_map"] is True


def test_promote_to_command_line_map_replaces_a_saved_identity_map(promote_cloud):
    """The guard leaves a promotion destination's saved map to promote-to, so CATALOG_MAP= can fix it."""
    _promotion_settings(promote_cloud.envs, 'promote_from = "dev"\ncatalog_map = { "paycat" = "paycat" }\n')

    result = promote_cloud("promote-to", "ENV=prod", "CATALOG_MAP=paycat=ppay")

    assert result.returncode == 0, result.stdout + result.stderr
    assert _env_cfg(promote_cloud.envs, "prod")["catalog_map"] == {"paycat": "ppay"}


@pytest.mark.parametrize("catalog_map", [
    '{ "paycat" = "paycat" }',
    '"ppay=other,paycat=paycat"',  # the legacy string
    '{ "paycat" = "paycat", "bad name" = "x" }',  # otherwise invalid: still refused
])
def test_env_file_check_reads_the_map_leniently(tmp_path, catalog_map, capsys):
    env_file = tmp_path / "prod" / "env.auto.tfvars"
    env_file.parent.mkdir()
    env_file.write_text(f"catalog_map = {catalog_map}\n")

    assert "maps a catalog to itself (paycat=paycat)" in saved_settings.env_identity_catalog_map_error(env_file)
    assert saved_settings.main(["identity-check", "--env-file", str(env_file)]) == 1
    assert "maps a catalog to itself" in capsys.readouterr().err
    env_file.write_text(f"catalog_map = {catalog_map}\n" + OPT_IN)
    assert saved_settings.env_identity_catalog_map_error(env_file) == ""


def test_the_shared_guard_refuses_except_for_a_promotion_destination(tmp_path, monkeypatch):
    env_file = tmp_path / "prod" / "env.auto.tfvars"
    env_file.parent.mkdir()
    env_file.write_text('catalog_map = { "paycat" = "paycat" }\n')

    monkeypatch.setattr(sys, "argv", ["genie_space_placeholder.py", str(env_file)])
    assert genie_space_placeholder.main() == 1
    monkeypatch.setattr(sys, "argv", ["genie_space_placeholder.py", "--promotion-destination", str(env_file)])
    assert genie_space_placeholder.main() == 0


def test_missing_catalog_map_error_lists_all_source_catalogs(tmp_path):
    source = tmp_path / "envs" / "dev"
    source.mkdir(parents=True)
    (source / "env.auto.tfvars").write_text(
        'uc_tables = ["paycat.s.t", "hrcat.s.u"]\n'
    )
    env_file = tmp_path / "envs" / "prod" / "env.auto.tfvars"
    env_file.parent.mkdir()
    env_file.write_text('catalog_map = { "paycat" = "prod_pay" }\n')

    with pytest.raises(ValueError) as exc:
        saved_settings.validate_source_catalogs(source, "paycat=prod_pay", env_file)
    message = str(exc.value)
    assert "missing source catalog(s): hrcat" in message
    assert "source catalogs: hrcat, paycat" in message


def test_the_staged_controller_and_the_overlay_refuse_before_writing(tmp_path, capsys):
    prod = tmp_path / "envs" / "prod"
    prod.mkdir(parents=True)
    (prod / "env.auto.tfvars").write_text(
        'governance_mode = "deterministic"\ncatalog_map = { "paycat" = "paycat" }\n')
    before = sorted(p.name for p in prod.rglob("*"))

    for command in ("run", "plan", "apply-saved", "confirm", "abandon", "rebind"):
        assert staged_policy_apply.main([command, "--env-dir", str(prod), "--env-name", "prod"]) == 1, command
        assert "maps a catalog to itself (paycat=paycat)" in capsys.readouterr().err
    with pytest.raises(runtime_overlay.OverlayError, match=r"maps a catalog to itself \(paycat=paycat\)"):
        runtime_overlay.build_kept_names(prod, state_keys=set(), live_names=lambda c: set())
    assert sorted(p.name for p in prod.rglob("*")) == before


@pytest.mark.parametrize("text", ["catalog_map = {\n", 'governance_mode = "legacy"\ncatalog_map = { "paycat" = \n',
                                  "governance_mode = {\n"])
@pytest.mark.parametrize("flags", [[], ["--promotion-destination"]], ids=["guard", "promotion-destination"])
def test_the_guard_reports_a_malformed_env_file_with_or_without_the_destination_flag(tmp_path, monkeypatch,
                                                                                     capsys, text, flags):
    """--promotion-destination only drops the identity check; the file is still parsed and reported."""
    env_file = tmp_path / "envs" / "prod" / "env.auto.tfvars"
    env_file.parent.mkdir(parents=True)
    env_file.write_text(text)

    monkeypatch.setattr(sys, "argv", ["genie_space_placeholder.py", *flags, str(env_file), str(env_file)])
    assert genie_space_placeholder.main() == 1
    err = capsys.readouterr().err
    assert err.count("fix envs/prod/env.auto.tfvars: it is not valid HCL") == 1, err
    assert "maps a catalog to itself" not in err
