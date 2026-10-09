"""Integration-level wiring tests for environment-local Genie ACL sidecars."""

import subprocess
import sys
from pathlib import Path

import hcl2
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from generate_abac import (  # noqa: E402
    autofix_acl_groups,
    strip_draft_genie_acl_fields,
    strip_multi_space_legacy_genie_keys,
)
from scripts.split_abac_config import (  # noqa: E402
    build_data_access_config,
    build_workspace_config,
    load_generated_config,
)


SHARED = Path(__file__).parent.parent


def test_cross_env_promote_rederives_destination_acl_before_split(tmp_path):
    dest = tmp_path / "prod"
    generated = dest / "generated"
    generated.mkdir(parents=True)
    (dest / "env.auto.tfvars").write_text('''genie_spaces = [
  { name = "Pay", uc_tables = ["prod_pay.s.t"] }
]
''')
    (generated / "abac.auto.tfvars").write_text('''
groups = { pay_g = {} other_g = {} }
fgac_policies = [
  { name = "pay" catalog = "prod_pay" to_principals = ["pay_g"] }
]
genie_space_configs = { Pay = { title = "Pay" } }
''')

    result = subprocess.run(
        [
            sys.executable,
            str(SHARED / "scripts/derive_genie_acls.py"),
            str(generated / "abac.auto.tfvars"),
            str(dest / "env.auto.tfvars"),
        ],
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    with open(generated / "genie_space_derived_acl_groups.auto.tfvars") as handle:
        derived = hcl2.load(handle)["genie_space_derived_acl_groups"]
    assert derived == {"Pay": ["pay_g"]}
    resolved = build_data_access_config(
        load_generated_config(generated / "abac.auto.tfvars")
    )["genie_space_acl_groups"]
    assert resolved == {"Pay": ["pay_g"]}
    assert all(resolved.values())

    makefile = (SHARED / "Makefile.shared").read_text()
    cross_env = makefile[makefile.index("=== Cross-env promote:") :]
    cross_env = cross_env[:cross_env.index("=== Promote complete:")]
    assert cross_env.index("remap_generated_config.py") < cross_env.index(
        "derive_genie_acls.py"
    ) < cross_env.index("split_abac_config.py")


def test_per_space_assembled_derivation_replaces_stale_sidecar(tmp_path):
    env_dir = tmp_path / "dev"
    generated = env_dir / "generated"
    generated.mkdir(parents=True)
    abac = generated / "abac.auto.tfvars"
    env = env_dir / "env.auto.tfvars"
    abac.write_text('''
groups = { renamed_g = {} }
fgac_policies = [
  { name = "renamed" catalog = "renamed_cat" to_principals = ["renamed_g"] }
]
genie_space_configs = { Renamed = { title = "Renamed" } }
''')
    env.write_text('''genie_spaces = [
  { name = "Renamed", uc_tables = ["renamed_cat.s.t"] }
]
''')
    sidecar = generated / "genie_space_derived_acl_groups.auto.tfvars"
    sidecar.write_text(
        'genie_space_derived_acl_groups = { Old = ["old_g"] }\n'
    )

    assert autofix_acl_groups(abac, env) == 1

    with open(sidecar) as handle:
        assert hcl2.load(handle)["genie_space_derived_acl_groups"] == {
            "Renamed": ["renamed_g"]
        }
    source = (SHARED / "generate_abac.py").read_text()
    merge_path = source[source.index("merge_script =") : source.index(
        "# ── Full generation", source.index("merge_script =")
    )]
    assert merge_path.index("merge_script") < merge_path.index(
        "n_acl_assembled = autofix_acl_groups"
    )


def test_full_generation_ignores_model_and_previous_output_acl(tmp_path):
    generated = tmp_path / "generated"
    generated.mkdir()
    abac = generated / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    canonical = tmp_path / "abac.auto.tfvars"
    abac.write_text('''
groups = { gA = {}, gB = {} }
fgac_policies = [
  { name = "payments" catalog = "pay" to_principals = ["gA"] },
  { name = "hr" catalog = "hr" to_principals = ["gB"] }
]
genie_space_configs = {
  payments = { title = "Payments", acl_groups = ["gA", "gB"] }
  hr = { title = "HR", acl_groups = ["gA", "gB"] }
}
''')
    env.write_text('''genie_spaces = [
  { name = "payments", uc_tables = ["pay.payments.t"] },
  { name = "hr", uc_tables = ["hr.hr.t"] },
]
''')
    canonical.write_text('''genie_space_configs = {
  payments = { acl_groups = ["gA", "gB"] }
  hr = { acl_groups = ["gA", "gB"] }
}
''')

    assert canonical.exists()  # mutation fixture: previous output must not be read
    assert autofix_acl_groups(abac, env) == 2
    with (generated / "genie_space_derived_acl_groups.auto.tfvars").open() as handle:
        assert hcl2.load(handle)["genie_space_derived_acl_groups"] == {
            "hr": ["gB"],
            "payments": ["gA"],
        }
    loaded = load_generated_config(abac)
    assert build_data_access_config(loaded)["genie_space_acl_groups"] == {
        "payments": ["gA"],
        "hr": ["gB"],
    }
    assert {
        name: value["acl_groups"]
        for name, value in build_workspace_config(loaded)[
            "genie_space_configs"
        ].items()
    } == {"payments": ["gA"], "hr": ["gB"]}


def test_user_owned_explicit_empty_acl_wins_over_model_and_policy(tmp_path):
    generated = tmp_path / "generated"
    generated.mkdir()
    abac = generated / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    abac.write_text('''
groups = { gA = {} }
fgac_policies = [{ name = "mask" catalog = "cat" except_principals = ["gA"] }]
genie_space_configs = { Private = { title = "Private", acl_groups = [] } }
''')
    env.write_text(
        'genie_spaces = [{ name = "Private", uc_tables = ["cat.s.t"], acl_groups = [] }]\n'
    )

    assert autofix_acl_groups(abac, env) == 1
    with (generated / "genie_space_derived_acl_groups.auto.tfvars").open() as handle:
        assert hcl2.load(handle)["genie_space_derived_acl_groups"] == {"Private": []}


def test_policy_change_rederives_acl_without_previous_output_freezing(tmp_path):
    generated = tmp_path / "generated"
    generated.mkdir()
    abac = generated / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    previous_output = tmp_path / "abac.auto.tfvars"
    env.write_text('genie_spaces = [{ name = "Pay", uc_tables = ["cat.s.t"] }]\n')
    previous_output.write_text(
        'genie_space_configs = { Pay = { acl_groups = ["old_wide_group"] } }\n'
    )
    abac.write_text('''
groups = { old_group = {}, new_group = {} }
fgac_policies = [{ name = "p", catalog = "cat", to_principals = ["old_group"] }]
genie_space_configs = { Pay = { acl_groups = ["model_group"] } }
''')
    assert autofix_acl_groups(abac, env) == 1
    sidecar = generated / "genie_space_derived_acl_groups.auto.tfvars"
    with sidecar.open() as handle:
        assert hcl2.load(handle)["genie_space_derived_acl_groups"] == {
            "Pay": ["old_group"]
        }

    abac.write_text(abac.read_text().replace('["old_group"]', '["new_group"]'))
    assert autofix_acl_groups(abac, env) == 1
    with sidecar.open() as handle:
        assert hcl2.load(handle)["genie_space_derived_acl_groups"] == {
            "Pay": ["new_group"]
        }


def test_multispace_legacy_shaped_draft_never_uses_flat_wide_acl(tmp_path):
    generated = tmp_path / "generated"
    generated.mkdir()
    abac = generated / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    abac.write_text('''
groups = { pay_g = {}, hr_g = {}, wide_g = {} }
fgac_policies = [
  { name = "pay" catalog = "paycat" to_principals = ["pay_g"] },
  { name = "hr" catalog = "hrcat" to_principals = ["hr_g"] },
]
genie_space_title = "Pay"
genie_acl_groups = ["wide_g"]
''')
    env.write_text('''genie_spaces = [
  { name = "Pay", uc_tables = ["paycat.s.t"], acl_groups = [] },
  { name = "HR", uc_tables = ["hrcat.s.t"] },
]
''')

    assert autofix_acl_groups(abac, env, reject_draft_acls=True) == 2
    loaded = load_generated_config(abac)
    expected = {"HR": ["hr_g"], "Pay": []}
    assert loaded["genie_space_legacy_mode"] is False
    assert build_data_access_config(loaded)["genie_space_acl_groups"] == expected
    assert {
        name: value["acl_groups"]
        for name, value in build_workspace_config(loaded)["genie_space_configs"].items()
    } == expected
    assert "wide_g" not in str(expected)


def test_multispace_legacy_shaped_draft_derives_when_env_acl_unset(tmp_path):
    generated = tmp_path / "generated"
    generated.mkdir()
    abac = generated / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    abac.write_text('''
groups = { pay_g = {}, wide_g = {} }
fgac_policies = [{ name = "pay" catalog = "paycat" to_principals = ["pay_g"] }]
genie_space_title = "Pay"
genie_acl_groups = ["wide_g"]
''')
    env.write_text('genie_spaces = [{ name = "Pay", uc_tables = ["paycat.s.t"] }]\n')

    assert autofix_acl_groups(abac, env, reject_draft_acls=True) == 1
    loaded = load_generated_config(abac)
    assert build_data_access_config(loaded)["genie_space_acl_groups"] == {
        "Pay": ["pay_g"]
    }


def test_governance_multispace_strip_removes_all_flat_legacy_genie_fields():
    draft = '''
genie_space_title = "Pay"
genie_space_description = "old"
genie_instructions = "old"
genie_acl_groups = ["wide_g"]
genie_sample_questions = ["old"]
genie_benchmarks = { b = {} }
'''
    cleaned = strip_multi_space_legacy_genie_keys(draft)
    assert "genie_" not in cleaned


def test_existing_draft_acl_fails_loud_until_moved_to_env(tmp_path):
    generated = tmp_path / "generated"
    generated.mkdir()
    abac = generated / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    abac.write_text('''
groups = { pay_g = {}, hr_g = {}, auditor_g = {} }
fgac_policies = [
  { name = "pay" catalog = "paycat" to_principals = ["pay_g"] except_principals = ["auditor_g"] },
  { name = "hr" catalog = "hrcat" to_principals = ["hr_g"] },
]
genie_space_configs = {
  Pay = { acl_groups = ["pay_g"] }
  HR = { acl_groups = [] }
}
''')
    env.write_text('''genie_spaces = [
  { name = "Pay", uc_tables = ["paycat.s.t"] },
  { name = "HR", uc_tables = ["hrcat.s.t"] },
]
''')
    result = subprocess.run(
        [sys.executable, str(SHARED / "scripts/derive_genie_acls.py"), str(abac), str(env)],
        text=True, capture_output=True,
    )
    assert result.returncode == 1
    assert "Genie space 'Pay'" in result.stdout
    assert "['pay_g']" in result.stdout
    assert "genie_spaces[] entry in env.auto.tfvars" in result.stdout
    assert not (generated / "genie_space_derived_acl_groups.auto.tfvars").exists()

    env.write_text('''genie_spaces = [
  { name = "Pay", uc_tables = ["paycat.s.t"], acl_groups = ["pay_g"] },
  { name = "HR", uc_tables = ["hrcat.s.t"], acl_groups = [] },
]
''')
    result = subprocess.run(
        [sys.executable, str(SHARED / "scripts/derive_genie_acls.py"), str(abac), str(env)],
        text=True, capture_output=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert build_data_access_config(load_generated_config(abac))[
        "genie_space_acl_groups"
    ] == {"Pay": ["pay_g"], "HR": []}


def test_fresh_draft_acl_fields_are_stripped_before_persistence(tmp_path):
    abac = tmp_path / "abac.auto.tfvars"
    abac.write_text('''genie_space_configs = {
  Pay = { title = "Pay", acl_groups = ["wide_g"] }
  HR = { title = "HR", genie_acl_groups = [] }
}
''')
    assert strip_draft_genie_acl_fields(abac) == 2
    parsed = hcl2.loads(abac.read_text())["genie_space_configs"]
    assert parsed == {"Pay": {"title": "Pay"}, "HR": {"title": "HR"}}
    assert "acl_groups" not in abac.read_text()


def test_null_env_acl_is_unset_and_derives_from_policy(tmp_path):
    generated = tmp_path / "generated"
    generated.mkdir()
    abac = generated / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    abac.write_text('''
groups = { pay_g = {} }
fgac_policies = [{ name = "pay" catalog = "paycat" to_principals = ["pay_g"] }]
genie_space_configs = { Pay = { title = "Pay" } }
''')
    env.write_text('genie_spaces = [{ name = "Pay", uc_tables = ["paycat.s.t"], acl_groups = null }]\n')
    assert autofix_acl_groups(abac, env) == 1
    assert build_data_access_config(load_generated_config(abac))[
        "genie_space_acl_groups"
    ] == {"Pay": ["pay_g"]}


def test_shared_catalog_policy_derivation_fails_closed_when_spaces_are_ambiguous(tmp_path):
    generated = tmp_path / "generated"
    generated.mkdir()
    abac = generated / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    abac.write_text('''
groups = { gA = {}, gB = {} }
fgac_policies = [
  { name = "shared_mask" catalog = "shared" to_principals = ["gA", "gB"] }
]
genie_space_configs = { payments = {}, hr = {} }
''')
    env.write_text('''genie_spaces = [
  { name = "payments", uc_tables = ["shared.payments.t"] },
  { name = "hr", uc_tables = ["shared.hr.t"] },
]
''')

    with pytest.raises(ValueError, match="shares catalog.*Set acl_groups"):
        autofix_acl_groups(abac, env)

    assert not (generated / "genie_space_derived_acl_groups.auto.tfvars").exists()


def test_shared_catalog_error_precedes_missing_policy_group_error(tmp_path):
    generated = tmp_path / "generated"
    generated.mkdir()
    abac = generated / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    abac.write_text('''
groups = { gA = {}, gB = {} }
fgac_policies = [{ name = "mask" catalog = "cat" to_principals = ["account users"] }]
genie_space_configs = { payments = {}, hr = {} }
''')
    env.write_text('''genie_spaces = [
  { name = "payments", uc_tables = ["cat.payments.t"] },
  { name = "hr", uc_tables = ["cat.hr.t"] },
]
''')

    with pytest.raises(ValueError, match="shares catalog"):
        autofix_acl_groups(abac, env)


def test_shared_catalog_superset_groups_still_fail_closed(tmp_path):
    generated = tmp_path / "generated"
    generated.mkdir()
    abac = generated / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    abac.write_text('''
groups = { gA = {}, gB = {}, gC = {} }
fgac_policies = [
  { name = "c1" catalog = "c1" to_principals = ["gA", "gB"] },
  { name = "c2" catalog = "c2" to_principals = ["gC"] },
]
genie_space_configs = { A = {}, B = {} }
''')
    env.write_text('''genie_spaces = [
  { name = "A", uc_tables = ["c1.s.a", "c2.s.a"] },
  { name = "B", uc_tables = ["c1.s.b"] },
]
''')
    with pytest.raises(ValueError, match="shares catalog.*Set acl_groups"):
        autofix_acl_groups(abac, env)


def test_user_acl_space_does_not_make_shared_catalog_safe_for_derived_peer(tmp_path):
    generated = tmp_path / "generated"
    generated.mkdir()
    abac = generated / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    abac.write_text('''
groups = { gA = {}, gB = {} }
fgac_policies = [{ name = "shared" catalog = "shared" to_principals = ["gA", "gB"] }]
genie_space_configs = { A = {}, B = {} }
''')
    env.write_text('''genie_spaces = [
  { name = "A", uc_tables = ["shared.s.a"], acl_groups = ["gA"] },
  { name = "B", uc_tables = ["shared.s.b"] },
]
''')
    with pytest.raises(ValueError, match="Genie space 'B'.*shares catalog"):
        autofix_acl_groups(abac, env)


@pytest.mark.parametrize(
    "spaces",
    [
        '[{ name = "A", uc_tables = ["c1.s.a"] }]',
        '[{ name = "A", uc_tables = ["c1.s.a"] }, { name = "B", uc_tables = ["c2.s.b"] }]',
    ],
)
def test_policy_derivation_allows_single_or_catalog_disjoint_spaces(tmp_path, spaces):
    generated = tmp_path / "generated"
    generated.mkdir()
    abac = generated / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    abac.write_text('''
groups = { gA = {}, gB = {} }
fgac_policies = [
  { name = "a" catalog = "c1" to_principals = ["gA"] },
  { name = "b" catalog = "c2" to_principals = ["gB"] },
]
genie_space_configs = { A = {}, B = {} }
''')
    env.write_text(f"genie_spaces = {spaces}\n")
    assert autofix_acl_groups(abac, env) == len(hcl2.loads(env.read_text())["genie_spaces"])


def test_per_space_merge_rejects_sibling_legacy_acl_before_formatter_wipes_it(tmp_path):
    generated = tmp_path / "generated"
    per_space = generated / "spaces" / "hr"
    per_space.mkdir(parents=True)
    env = tmp_path / "env.auto.tfvars"
    env.write_text('''genie_spaces = [
  { name = "Pay", uc_tables = ["shared.pay.t"] },
  { name = "HR", uc_tables = ["shared.hr.t"], acl_groups = [] },
]
''')
    assembled = generated / "abac.auto.tfvars"
    original = '''
groups = { pay_g = {}, auditor_g = {}, hr_g = {} }
fgac_policies = [{ name = "shared" catalog = "shared" to_principals = ["pay_g", "auditor_g", "hr_g"] }]
genie_space_configs = { Pay = { title = "Pay", acl_groups = ["pay_g"] } HR = { title = "HR" } }
'''
    assembled.write_text(original)
    (per_space / "abac.auto.tfvars").write_text(
        'genie_space_configs = { HR = { title = "HR" } }\n'
    )
    (generated / "masking_functions.sql").write_text("")
    (per_space / "masking_functions.sql").write_text("")

    result = subprocess.run(
        [sys.executable, str(SHARED / "scripts/merge_space_configs.py"), str(generated), "hr"],
        text=True, capture_output=True,
    )
    assert result.returncode != 0
    assert "Genie space 'Pay' has legacy draft acl_groups=['pay_g']" in result.stderr
    assert "genie_spaces[] entry in env.auto.tfvars" in result.stderr
    assert assembled.read_text() == original
    assert not (generated / "genie_space_derived_acl_groups.auto.tfvars").exists()


def test_no_acl_path_reads_previous_workspace_output():
    generator = (SHARED / "generate_abac.py").read_text()
    cli = (SHARED / "scripts/derive_genie_acls.py").read_text()
    makefile = (SHARED / "Makefile.shared").read_text()
    forbidden = ("_workspace_acl_hints", "canonical_workspace", "--canonical-workspace", "--ignore-explicit")
    assert all(token not in generator + cli + makefile for token in forbidden)
    assert "genie_space_derived_acl_groups_authoritative" not in generator + cli + makefile


def test_id_only_space_uses_resolved_title_for_both_consumers(tmp_path):
    generated = tmp_path / "generated"
    generated.mkdir()
    abac = generated / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    abac.write_text('''
groups = { pay_g = {} }
fgac_policies = [{ name = "pay" catalog = "pay_cat" to_principals = ["pay_g"] }]
genie_space_id_to_name = { "space-1" = "Payments" }
genie_space_configs = { Payments = { title = "Payments" } }
''')
    env.write_text('genie_spaces = [{ genie_space_id = "space-1", uc_tables = ["pay_cat.s.t"] }]\n')

    assert autofix_acl_groups(abac, env) == 1
    loaded = load_generated_config(abac)
    data_acl = build_data_access_config(loaded)["genie_space_acl_groups"]
    workspace_acl = {
        key: value["acl_groups"]
        for key, value in build_workspace_config(loaded)["genie_space_configs"].items()
    }
    assert data_acl == workspace_acl == {"Payments": ["pay_g"]}


def test_derive_cli_reports_one_line_error_without_traceback(tmp_path):
    abac = tmp_path / "abac.auto.tfvars"
    env = tmp_path / "env.auto.tfvars"
    abac.write_text('genie_space_configs = { Pay = { title = "Pay" } }\n')
    env.write_text('genie_spaces = [{ name = "Pay", uc_tables = ["new.s.t"] }]\n')
    result = subprocess.run(
        [sys.executable, str(SHARED / "scripts/derive_genie_acls.py"), str(abac), str(env)],
        text=True, capture_output=True,
    )
    assert result.returncode == 1
    assert result.stdout.startswith("ERROR: Cannot derive ACL")
    assert "Traceback" not in result.stdout + result.stderr


def test_promote_and_apply_genie_fail_closed_wiring():
    source = (SHARED / "Makefile.shared").read_text()
    promote = source[source.index("promote:"):source.index("_prepare-classification:")]
    assert "@set -e;" in promote
    assert promote.count("trap '") >= 2
    assert "genie_space_derived_acl_groups.auto.tfvars" in promote
    apply_genie = source[source.index("apply-genie:"):source.index("destroy-governance:")]
    assert apply_genie.index("derive_genie_acls.py") < apply_genie.index("split_abac_config.py")


@pytest.mark.parametrize("failure", ["derive", "split"])
def test_promote_failure_is_nonzero_and_invalidates_stale_layers(tmp_path, failure):
    cloud = tmp_path / "cloud"
    env = cloud / "envs" / "dev"
    generated = env / "generated"
    data_access = env / "data_access"
    account = cloud / "envs" / "account"
    generated.mkdir(parents=True)
    data_access.mkdir()
    account.mkdir(parents=True)
    (tmp_path / "Makefile").write_text(
        f"SHARED_ROOT := {SHARED}\nCLOUD_ROOT := {cloud}\nCLOUD := aws\n"
        f"include {SHARED / 'Makefile.shared'}\n"
    )
    (env / "env.auto.tfvars").write_text(
        'genie_spaces = [{ name = "Pay", uc_tables = ["newcat.s.t"] }]\n'
    )
    if failure == "derive":
        (generated / "abac.auto.tfvars").write_text(
            'genie_space_configs = { Pay = { title = "Pay" } }\n'
        )
        run_env = None
    else:
        (generated / "abac.auto.tfvars").write_text(
            'genie_space_configs = { Pay = { title = "Pay", acl_groups = [] } }\n'
        )
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        python = bin_dir / "python3"
        python.write_text(
            "#!/bin/sh\ncase \"$1\" in *split_abac_config.py) exit 9;; esac\n"
            f"exec {sys.executable} \"$@\"\n"
        )
        python.chmod(0o755)
        import os
        run_env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"}
    sidecar = generated / "genie_space_derived_acl_groups.auto.tfvars"
    workspace_layer = env / "abac.auto.tfvars"
    data_layer = data_access / "abac.auto.tfvars"
    sidecar.write_text('genie_space_derived_acl_groups = { Pay = ["stale"] }\n')
    workspace_layer.write_text("stale = true\n")
    data_layer.write_text("stale = true\n")

    result = subprocess.run(
        ["make", "promote", "ENV=dev"], cwd=tmp_path,
        text=True, capture_output=True, env=run_env,
    )
    assert result.returncode != 0, result.stdout + result.stderr
    assert not sidecar.exists()
    assert not workspace_layer.exists()
    assert not data_layer.exists()


def test_per_space_rename_removes_old_key_atomically(tmp_path):
    generated = tmp_path / "generated"
    per_space = generated / "spaces" / "old_name"
    per_space.mkdir(parents=True)
    (tmp_path / "env.auto.tfvars").write_text(
        'genie_spaces = [{ name = "New Name", uc_tables = [], acl_groups = [] }]\n'
    )
    assembled = generated / "abac.auto.tfvars"
    assembled.write_text(
        'genie_space_configs = { "Old Name" = { title = "Old Name" } }\n'
    )
    (per_space / "abac.auto.tfvars").write_text(
        'genie_space_configs = { "New Name" = { title = "New Name" } }\n'
    )
    subprocess.run(
        [sys.executable, str(SHARED / "scripts/merge_space_configs.py"), str(generated), "old_name"],
        check=True, text=True, capture_output=True,
    )
    with assembled.open() as handle:
        configs = hcl2.load(handle)["genie_space_configs"]
    assert set(configs) == {"New Name"}


def test_per_space_id_only_rename_drops_orphan_config_with_warning(tmp_path):
    generated = tmp_path / "generated"
    per_space = generated / "spaces" / "new_name"
    per_space.mkdir(parents=True)
    (tmp_path / "env.auto.tfvars").write_text(
        'genie_spaces = [{ genie_space_id = "s1", uc_tables = [], acl_groups = [] }]\n'
    )
    assembled = generated / "abac.auto.tfvars"
    assembled.write_text('''
genie_space_configs = { "Old Name" = { title = "Old Name", acl_groups = ["hr_g"] } }
genie_space_id_to_name = { s1 = "Old Name" }
''')
    (per_space / "abac.auto.tfvars").write_text('''
genie_space_configs = { "New Name" = { title = "New Name", acl_groups = ["pay_g"] } }
genie_space_id_to_name = { s1 = "New Name" }
''')
    result = subprocess.run(
        [sys.executable, str(SHARED / "scripts/merge_space_configs.py"), str(generated), "new_name"],
        check=True, text=True, capture_output=True,
    )
    with assembled.open() as handle:
        config = hcl2.load(handle)
    assert set(config["genie_space_configs"]) == {"New Name"}
    assert config["genie_space_id_to_name"] == {"s1": "New Name"}
    assert "Dropped orphan genie_space_configs entry 'Old Name'" in result.stdout


def test_per_space_merge_aborts_on_invalid_environment_without_writing(tmp_path):
    generated = tmp_path / "generated"
    per_space = generated / "spaces" / "pay"
    per_space.mkdir(parents=True)
    (tmp_path / "env.auto.tfvars").write_text("genie_spaces = [ broken\n")
    assembled = generated / "abac.auto.tfvars"
    original = 'genie_space_configs = { Existing = { acl_groups = ["safe"] } }\n'
    assembled.write_text(original)
    (per_space / "abac.auto.tfvars").write_text(
        'genie_space_configs = { Pay = { acl_groups = ["pay"] } }\n'
    )
    result = subprocess.run(
        [sys.executable, str(SHARED / "scripts/merge_space_configs.py"), str(generated), "pay"],
        text=True, capture_output=True,
    )
    assert result.returncode != 0
    assert "invalid HCL" in result.stderr
    assert assembled.read_text() == original


def test_unknown_id_only_space_fails_closed_without_canonical_name(tmp_path):
    generated = tmp_path / "generated"
    per_space = generated / "spaces" / "pay"
    per_space.mkdir(parents=True)
    (tmp_path / "env.auto.tfvars").write_text(
        'genie_spaces = [{ genie_space_id = "unknown-id", uc_tables = [] }]\n'
    )
    assembled = generated / "abac.auto.tfvars"
    assembled.write_text(
        'genie_space_configs = { Curated = { instructions = "keep" } }\n'
    )
    (per_space / "abac.auto.tfvars").write_text(
        'genie_space_configs = { Pay = { title = "Pay" } }\n'
    )
    original = assembled.read_text()
    result = subprocess.run(
        [sys.executable, str(SHARED / "scripts/merge_space_configs.py"), str(generated), "pay"],
        text=True, capture_output=True,
    )
    assert result.returncode != 0
    assert "has no canonical name mapping" in result.stderr
    assert assembled.read_text() == original


def test_just_merged_config_is_never_pruned_as_orphan(tmp_path):
    generated = tmp_path / "generated"
    per_space = generated / "spaces" / "new"
    per_space.mkdir(parents=True)
    (tmp_path / "env.auto.tfvars").write_text(
        'genie_spaces = [{ name = "Active", uc_tables = [], acl_groups = [] }]\n'
    )
    assembled = generated / "abac.auto.tfvars"
    assembled.write_text(
        'genie_space_configs = { Active = { title = "Active" }, Old = { title = "Old" } }\n'
    )
    (per_space / "abac.auto.tfvars").write_text(
        'genie_space_configs = { New = { title = "New" } }\n'
    )
    subprocess.run(
        [sys.executable, str(SHARED / "scripts/merge_space_configs.py"), str(generated), "new"],
        check=True, text=True, capture_output=True,
    )
    with assembled.open() as handle:
        configs = hcl2.load(handle)["genie_space_configs"]
    assert set(configs) == {"Active", "New"}


def test_per_space_failed_acl_derivation_preserves_assembled_file(tmp_path):
    generated = tmp_path / "generated"
    per_space = generated / "spaces" / "pay"
    per_space.mkdir(parents=True)
    assembled = generated / "abac.auto.tfvars"
    original = 'genie_space_configs = { Pay = { title = "Pay", acl_groups = ["safe"] } }\n'
    assembled.write_text(original)
    (per_space / "abac.auto.tfvars").write_text(
        'genie_space_configs = { Pay = { title = "Pay" } }\n'
    )
    result = subprocess.run(
        [sys.executable, str(SHARED / "scripts/merge_space_configs.py"), str(generated), "pay"],
        text=True, capture_output=True,
    )
    assert result.returncode != 0
    assert assembled.read_text() == original


def test_cross_env_promote_preserves_id_only_canonical_acl_in_both_layers(tmp_path):
    cloud = tmp_path / "cloud"
    dev = cloud / "envs" / "dev"
    generated = dev / "generated"
    account = cloud / "envs" / "account"
    generated.mkdir(parents=True)
    account.mkdir(parents=True)
    (tmp_path / "Makefile").write_text(
        f"SHARED_ROOT := {SHARED}\nCLOUD_ROOT := {cloud}\nCLOUD := aws\n"
        f"include {SHARED / 'Makefile.shared'}\n"
    )
    (dev / "env.auto.tfvars").write_text('''genie_spaces = [
  { genie_space_id = "s1", uc_tables = ["paycat.s.t"], acl_groups = ["pay_override"] },
  { name = "HR", genie_space_id = "s2", uc_tables = ["hrcat.s.t"], acl_groups = [] },
]
''')
    (generated / "abac.auto.tfvars").write_text('''
groups = { pay_group = {}, hr_group = {}, pay_override = {} }
fgac_policies = [
  { name = "pay" catalog = "paycat" to_principals = ["pay_group"] },
  { name = "hr" catalog = "hrcat" to_principals = ["hr_group"] },
]
genie_space_configs = {
  Payments = { title = "Payments" }
  HR = { title = "HR" }
}
genie_space_id_to_name = { s1 = "Payments", s2 = "HR" }
''')
    (generated / "masking_functions.sql").write_text("-- none\n")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    python = bin_dir / "python3"
    python.write_text(
        "#!/bin/sh\ncase \"$1\" in *validate_abac.py) exit 0;; esac\n"
        f"exec {sys.executable} \"$@\"\n"
    )
    python.chmod(0o755)
    import os
    result = subprocess.run(
        ["make", "promote", "SOURCE_ENV=dev", "DEST_ENV=prod",
         "DEST_CATALOG_MAP=paycat=ppay,hrcat=phr"],
        cwd=tmp_path, text=True, capture_output=True,
        env={**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    prod = cloud / "envs" / "prod"
    with (prod / "env.auto.tfvars").open() as handle:
        prod_spaces = hcl2.load(handle)["genie_spaces"]
    assert [(s["name"], s["genie_space_id"]) for s in prod_spaces] == [
        ("Payments", ""), ("HR", "")
    ]
    with (prod / "data_access" / "abac.auto.tfvars").open() as handle:
        data_cfg = hcl2.load(handle)
    with (prod / "abac.auto.tfvars").open() as handle:
        workspace_cfg = hcl2.load(handle)
    assert prod_spaces[0]["acl_groups"] == ["pay_override"]
    assert prod_spaces[1]["acl_groups"] == []
    expected = {"Payments": ["pay_override"], "HR": []}
    assert data_cfg["genie_space_acl_groups"] == expected
    assert {
        name: cfg["acl_groups"]
        for name, cfg in workspace_cfg["genie_space_configs"].items()
    } == expected


def _id_only_agent_env(tmp_path, abac_text):
    """Champion layout: env names the agent by ID; tables come from discovery."""
    env = tmp_path / "env.auto.tfvars"
    env.write_text('genie_spaces = [\n  { genie_space_id = "01abc" }\n]\n')
    (tmp_path / "data_access").mkdir()
    (tmp_path / "data_access" / "discovered_uc_tables.auto.tfvars").write_text('''
discovered_uc_tables = ["dev_cat.s.customers"]
discovered_table_agents = {
  "dev_cat.s.customers" = ["Sample Agent"]
}
''')
    generated = tmp_path / "generated"
    generated.mkdir()
    abac = generated / "abac.auto.tfvars"
    abac.write_text(abac_text + '''
genie_space_id_to_name = { "01abc" = "Sample Agent" }
genie_space_configs = { "Sample Agent" = { title = "Sample Agent" } }
''')
    return abac, env


def test_id_only_agent_takes_catalogs_from_discovered_tables(tmp_path):
    abac, env = _id_only_agent_env(tmp_path, '''
groups = { tier_a = {} tier_b = {} }
fgac_policies = [
  { name = "mask" catalog = "dev_cat" to_principals = ["tier_b"] except_principals = ["tier_a"] }
]
''')

    assert autofix_acl_groups(abac, env) == 1

    with open(abac.with_name("genie_space_derived_acl_groups.auto.tfvars")) as handle:
        derived = hcl2.load(handle)["genie_space_derived_acl_groups"]
    assert derived == {"Sample Agent": ["tier_a", "tier_b"]}


def test_genie_mode_defers_acl_when_draft_has_no_policies(tmp_path, capsys):
    abac, env = _id_only_agent_env(tmp_path, "")

    with pytest.raises(ValueError, match="no policy groups"):
        autofix_acl_groups(abac, env)
    assert autofix_acl_groups(abac, env, defer_unmapped=True) == 0
    assert "next full `make generate` derives it" in capsys.readouterr().out


def _genie_mode_import(tmp_path):
    """What `make generate MODE=genie` leaves for an agent-ID-only space: Genie
    config, no access policies, and the ACL deferred (no sidecar entry)."""
    cloud = tmp_path / "cloud"
    env = cloud / "envs" / "dev"
    (env / "generated").mkdir(parents=True)
    (env / "data_access").mkdir()
    (cloud / "envs" / "account").mkdir(parents=True)
    (tmp_path / "Makefile").write_text(
        f"SHARED_ROOT := {SHARED}\nCLOUD_ROOT := {cloud}\nCLOUD := aws\n"
        f"include {SHARED / 'Makefile.shared'}\n"
    )
    (env / "env.auto.tfvars").write_text(
        'genie_spaces = [{ genie_space_id = "01abc" }]\n'
        'access_tier_groups = ["tier_a", "tier_b"]\n'
    )
    (env / "data_access" / "discovered_uc_tables.auto.tfvars").write_text(
        'discovered_uc_tables = ["dev_cat.s.t"]\n'
        'discovered_table_agents = { "dev_cat.s.t" = ["Sample Agent"] }\n'
    )
    (env / "generated" / "abac.auto.tfvars").write_text(
        'genie_space_id_to_name = { "01abc" = "Sample Agent" }\n'
        'genie_space_configs = { "Sample Agent" = { title = "Sample Agent" } }\n'
    )
    (env / "generated" / "genie_space_derived_acl_groups.auto.tfvars").write_text(
        "genie_space_legacy_mode = false\ngenie_space_derived_acl_groups = {}\n"
    )
    runner_log = tmp_path / "terraform.log"
    runner = tmp_path / "runner.sh"
    runner.write_text(f'#!/bin/sh\necho "$@" >> {runner_log}\n')
    runner.chmod(0o755)
    return env, runner, runner_log


def _live_refresh_stub(tmp_path):
    """Opening access re-reads live UC first; stand in for a successful read."""
    (tmp_path / "live_refresh_stub.py").write_text(
        "import argparse, sys\nfrom pathlib import Path\n"
        f"sys.path.insert(0, {str(SHARED)!r})\n"
        "from scripts.coverage_gate import write_refresh_record\n"
        "p = argparse.ArgumentParser()\n"
        "for f in ('--auth-file', '--env-file', '--config', '--write-ddl', '--refresh-record'):\n"
        "    p.add_argument(f)\n"
        "p.add_argument('--ddl-only', action='store_true')\n"
        "a = p.parse_args()\n"
        "Path(a.write_ddl).write_text('CREATE TABLE dev_cat.s.t (\\n  id BIGINT\\n);\\n')\n"
        "write_refresh_record(Path(a.refresh_record), mode='ddl', ddl_path=Path(a.write_ddl), config_path=None)\n"
    )
    return tmp_path / "live_refresh_stub.py"


@pytest.mark.parametrize("target", ["apply", "apply-governance", "apply-genie"])
def test_genie_mode_import_with_deferred_acl_cannot_be_applied(tmp_path, target):
    """A deferred ACL must fail closed: no Terraform runs, so no SELECT or CAN_RUN."""
    env, runner, runner_log = _genie_mode_import(tmp_path)

    result = subprocess.run(
        ["make", target, "ENV=dev", f"ROOT_RUNNER={runner}",
         # ... so the deferred ACL, not the live read, is what stops it.
         f"DERIVE_ASSIGNMENTS_SCRIPT={_live_refresh_stub(tmp_path)}"],
        cwd=tmp_path, text=True, capture_output=True,
    )

    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    assert "Cannot derive ACL" in output or "has no resolved ACL" in output, output
    assert not runner_log.exists() or " apply" not in runner_log.read_text()
    assert not (env / "abac.auto.tfvars").exists()
    assert not (env / "data_access" / "abac.auto.tfvars").exists()


def test_genie_mode_import_with_deferred_acl_cannot_be_released(tmp_path):
    """Unified release fails closed on a deferred ACL before Terraform apply."""
    env, runner, runner_log = _genie_mode_import(tmp_path)

    release = subprocess.run(
        ["make", "release", "ENV=dev", f"ROOT_RUNNER={runner}"],
        cwd=tmp_path, text=True, capture_output=True,
    )

    assert release.returncode != 0, release.stdout + release.stderr
    assert "no tag_assignments section" in release.stdout + release.stderr
    assert not runner_log.exists() or " apply" not in runner_log.read_text()
    # Release writes no exposure flag (there is none to open).
    assert "business_access_enabled" not in (env / "env.auto.tfvars").read_text()


def _tiered_acl(tmp_path, tiers_line, policies):
    env_dir = tmp_path / "dev"
    generated = env_dir / "generated"
    generated.mkdir(parents=True)
    (env_dir / "env.auto.tfvars").write_text(f'''genie_spaces = [
  {{ name = "Pay", uc_tables = ["dev_pay.s.t"] }}
]
{tiers_line}
''')
    abac = generated / "abac.auto.tfvars"
    abac.write_text(f'''
groups = {{ ops_g = {{}} analysts_g = {{}} viewers_g = {{}} }}
fgac_policies = [
{policies}
]
genie_space_configs = {{ Pay = {{ title = "Pay" }} }}
''')
    autofix_acl_groups(abac, env_dir / "env.auto.tfvars")
    with open(generated / "genie_space_derived_acl_groups.auto.tfvars") as handle:
        return hcl2.load(handle)["genie_space_derived_acl_groups"]["Pay"]


def test_masks_naming_only_masked_tiers_keep_the_full_access_tier(tmp_path):
    acl = _tiered_acl(
        tmp_path,
        'access_tier_groups = ["ops_g", "analysts_g", "viewers_g"]',
        '  { name = "ssn" catalog = "dev_pay" to_principals = ["analysts_g", "viewers_g"] },\n'
        '  { name = "email" catalog = "dev_pay" to_principals = ["viewers_g"] }',
    )
    assert acl == ["analysts_g", "ops_g", "viewers_g"]


def test_tier_below_every_policy_group_is_not_added(tmp_path):
    acl = _tiered_acl(
        tmp_path,
        'access_tier_groups = ["ops_g", "analysts_g", "viewers_g"]',
        '  { name = "ssn" catalog = "dev_pay" to_principals = ["analysts_g"] }',
    )
    assert acl == ["analysts_g", "ops_g"]


def test_without_access_tiers_acl_is_only_the_policy_groups(tmp_path):
    acl = _tiered_acl(
        tmp_path,
        "",
        '  { name = "ssn" catalog = "dev_pay" to_principals = ["analysts_g", "viewers_g"] }',
    )
    assert acl == ["analysts_g", "viewers_g"]
