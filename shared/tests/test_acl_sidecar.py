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
