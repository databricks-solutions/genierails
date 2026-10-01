"""Integration-level wiring tests for environment-local Genie ACL sidecars."""

import subprocess
import sys
from pathlib import Path

import hcl2

sys.path.insert(0, str(Path(__file__).parent.parent))
from generate_abac import autofix_acl_groups  # noqa: E402
from scripts.split_abac_config import build_data_access_config, load_generated_config  # noqa: E402


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
