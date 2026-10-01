"""Integration-level wiring tests for environment-local Genie ACL sidecars."""

import subprocess
import sys
from pathlib import Path

import hcl2
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from generate_abac import autofix_acl_groups  # noqa: E402
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

    with pytest.raises(ValueError, match="Cannot safely derive distinct Genie ACLs"):
        autofix_acl_groups(abac, env)

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
        'genie_space_configs = { "Old Name" = { title = "Old Name", acl_groups = [] } }\n'
    )
    (per_space / "abac.auto.tfvars").write_text(
        'genie_space_configs = { "New Name" = { title = "New Name", acl_groups = [] } }\n'
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
    assert "Cannot safely prune orphan Genie configs" in result.stderr
    assert assembled.read_text() == original


def test_unknown_id_only_space_keeps_all_configs_and_warns(tmp_path):
    generated = tmp_path / "generated"
    per_space = generated / "spaces" / "pay"
    per_space.mkdir(parents=True)
    (tmp_path / "env.auto.tfvars").write_text(
        'genie_spaces = [{ genie_space_id = "unknown-id", uc_tables = [] }]\n'
    )
    assembled = generated / "abac.auto.tfvars"
    assembled.write_text(
        'genie_space_configs = { Curated = { instructions = "keep", acl_groups = [] } }\n'
    )
    (per_space / "abac.auto.tfvars").write_text(
        'genie_space_configs = { Pay = { acl_groups = [] } }\n'
    )
    result = subprocess.run(
        [sys.executable, str(SHARED / "scripts/merge_space_configs.py"), str(generated), "pay"],
        check=True, text=True, capture_output=True,
    )
    with assembled.open() as handle:
        configs = hcl2.load(handle)["genie_space_configs"]
    assert set(configs) == {"Curated", "Pay"}
    assert "Keeping all configs" in result.stdout


def test_just_merged_config_is_never_pruned_as_orphan(tmp_path):
    generated = tmp_path / "generated"
    per_space = generated / "spaces" / "new"
    per_space.mkdir(parents=True)
    (tmp_path / "env.auto.tfvars").write_text(
        'genie_spaces = [{ name = "Active", uc_tables = [], acl_groups = [] }]\n'
    )
    assembled = generated / "abac.auto.tfvars"
    assembled.write_text(
        'genie_space_configs = { Active = { acl_groups = [] }, Old = { acl_groups = [] } }\n'
    )
    (per_space / "abac.auto.tfvars").write_text(
        'genie_space_configs = { New = { acl_groups = [] } }\n'
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
