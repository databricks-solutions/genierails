"""Promoted ABAC policy names carry the destination catalog, never the source.

The data_access module keys ``databricks_policy_info.policies`` by the policy
``name`` and names the remote policy ``<catalog>_<name>``. Derived masks are
named ``gr_mask_<catalog>_<treatment>`` at generate time; promote rewrites that
catalog token, but only when the destination's Terraform state or a live
listing shows the policy isn't deployed: a rename is a delete plus a create in
Terraform, and provider 1.111 cannot rename in place.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import hcl2
import pytest

SHARED = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SHARED))
sys.path.insert(0, str(SHARED / "scripts"))

from generate_abac import _render_fgac_policy_block  # noqa: E402
import remap_generated_config  # noqa: E402
from remap_generated_config import (  # noqa: E402
    live_policy_lister,
    read_state_policy_keys,
    remap_hcl,
    remap_policy_name,
    remap_policy_names,
)
from treatment_derivation import derive_treatment_model, load_treatment_config  # noqa: E402

SPLIT = SHARED / "scripts" / "split_abac_config.py"
ROOT = SHARED / "roots" / "data_access"

DEV_1, DEV_2 = "genierails_dev_1", "genierails_dev_2"
PROD_1, PROD_2 = "genierails_prod_1", "genierails_prod_2"
MAPS = ["--map", f"{DEV_1}={PROD_1}", "--map", f"{DEV_2}={PROD_2}"]


def _dev_generated_config() -> dict:
    """A dev draft after treatment derivation, spanning two catalogs."""
    cfg = {
        "tag_policies": [
            {"key": "pii_level", "values": ["redacted_address"]},
            {"key": "pci_level", "values": ["redacted_cvv"]},
        ],
        "tag_assignments": [
            {"entity_type": "columns", "entity_name": f"{DEV_1}.payments.payments.address",
             "tag_key": "pii_level", "tag_value": "redacted_address"},
            {"entity_type": "columns", "entity_name": f"{DEV_2}.cards.cards.cvv",
             "tag_key": "pci_level", "tag_value": "redacted_cvv"},
        ],
        "fgac_policies": [
            {"name": "mask_address", "policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": DEV_1,
             "to_principals": ["analysts"], "match_condition": "hasTagValue('pii_level', 'redacted_address')",
             "function_name": "mask_redact", "function_schema": "payments"},
            {"name": "mask_cvv", "policy_type": "POLICY_TYPE_COLUMN_MASK", "catalog": DEV_2,
             "to_principals": ["analysts"], "match_condition": "hasTagValue('pci_level', 'redacted_cvv')",
             "function_name": "mask_redact", "function_schema": "cards"},
            {"name": f"region_filter_{DEV_1}", "policy_type": "POLICY_TYPE_ROW_FILTER", "catalog": DEV_1,
             "to_principals": ["analysts"], "when_condition": "hasTag('region')",
             "function_name": "filter_region", "function_catalog": DEV_1, "function_schema": "payments"},
        ],
    }
    derived, _ = derive_treatment_model(cfg, load_treatment_config())
    return derived


def _render(cfg: dict) -> str:
    blocks = ",\n".join(_render_fgac_policy_block(p) for p in cfg["fgac_policies"])
    return "tag_assignments = []\n\nfgac_policies = [\n" + blocks + ",\n]\n"


LEGACY = f"gr_mask_{DEV_1}_redact"
LEGACY_REMOTE = f"{PROD_1}_{LEGACY}"


def _lister(live: dict[str, set[str]] | None):
    """Stub of the SDK listing: None means the API is unavailable."""
    return lambda _auth: (lambda catalog: None if live is None else live.get(catalog, set()))


def _promote(tmp_path, monkeypatch, capsys, source_text, *, state=None, live=None):
    src = tmp_path / "dev" / "generated"
    src.mkdir(parents=True, exist_ok=True)
    (src / "abac.auto.tfvars").write_text(source_text)
    out = tmp_path / "prod" / "generated" / "abac.auto.tfvars"
    argv = ["remap_generated_config.py", str(src / "abac.auto.tfvars"), str(src / "missing.sql"),
            str(out), str(out.with_name("masking_functions.sql")), *MAPS,
            "--auth", str(tmp_path / "prod" / "auth.auto.tfvars")]
    if state is not None:
        argv += ["--state", str(state)]
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setattr(remap_generated_config, "live_policy_lister", _lister(live))
    code = 0
    try:
        remap_generated_config.main()
    except SystemExit as exc:
        code = exc.code or 0
    return code, capsys.readouterr().out, out


def _policies(path: Path) -> list[dict]:
    return hcl2.loads(path.read_text())["fgac_policies"]


def _names(path: Path) -> list[str]:
    return [p["name"] for p in _policies(path)]


def _remote_names(policies: list[dict]) -> list[str]:
    # Mirrors modules/data_access: name = "${catalog}_${key}".
    return [f"{p['catalog']}_{p['name']}" for p in policies]


def _state(path: Path, keys: list[str], catalog: str = PROD_1, extra: list[dict] = ()) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"version": 4, "resources": [{
        "module": "module.data_access", "mode": "managed",
        "type": "databricks_policy_info", "name": "policies",
        "instances": [{"index_key": k, "attributes": {"on_securable_fullname": catalog}} for k in keys],
    }, *extra]}))
    return path


def _assert_all_renamed(out: Path) -> None:
    policies = _policies(out)
    names = [p["name"] for p in policies]
    remote = _remote_names(policies)
    assert not [n for n in names + remote if "_dev_" in n], remote
    assert f"gr_mask_{PROD_1}_redact" in names
    assert f"gr_mask_{PROD_2}_redact" in names
    assert f"region_filter_{PROD_1}" in names
    assert f"{PROD_1}_gr_mask_{PROD_1}_redact" in remote
    # Unique per env (Terraform key) and per catalog (remote name).
    assert len(set(names)) == len(names)
    assert len(set(remote)) == len(remote)


def _assert_no_live_mask_dropped(tmp_path: Path, out: Path, changes: dict | None = None) -> None:
    changes = changes if changes is not None else _policy_plan(tmp_path, out)
    live = _SEEDED_ADDRESS.format(key=LEGACY)
    actions, before, after = changes[live]
    # Principals may converge in place; the remote name must not move.
    assert actions in (["no-op"], ["update"]), actions
    assert before == after == LEGACY_REMOTE
    for address, (actions, before, after) in changes.items():
        assert "delete" not in actions, address
        assert actions in (["no-op"], ["update"], ["create"]), (address, actions)
        if actions == ["update"]:
            assert before == after, (address, before, after)


def test_generate_keeps_catalog_scoped_derived_mask_names():
    # Generate-time names are unchanged, so dev deployments and the reviewed-rule
    # merge (matched by name) see no rename.
    names = {p["name"] for p in _dev_generated_config()["fgac_policies"]}
    assert f"gr_mask_{DEV_1}_redact" in names
    assert f"gr_mask_{DEV_2}_redact" in names


def test_promote_renames_only_the_policys_own_catalog_token():
    assert remap_policy_name("gr_mask_cat_v2_redact", "cat_v2", "p2") == "gr_mask_p2_redact"
    assert remap_policy_name("gr_mask_xcat_redact", "cat", "prod") == "gr_mask_xcat_redact"
    assert remap_policy_name("cat_mask", "cat", "prod") == "prod_mask"
    assert remap_policy_name("mask_pii", "cat", "prod") == "mask_pii"


def test_state_without_the_key_but_api_unavailable_keeps_names(tmp_path, monkeypatch, capsys):
    # A state that doesn't manage the old policy can't prove it's absent remotely.
    state = _state(tmp_path / "prod" / "data_access" / "terraform.tfstate", ["unrelated"])
    code, stdout, out = _promote(tmp_path, monkeypatch, capsys, _render(_dev_generated_config()), state=state)
    assert code == 0, stdout
    assert f"kept policy name {LEGACY_REMOTE} (couldn't confirm it isn't deployed in {PROD_1})" in stdout
    assert set(_names(out)) == {p["name"] for p in _dev_generated_config()["fgac_policies"]}
    _assert_no_live_mask_dropped(tmp_path, out)


def test_api_shows_old_and_new_names_absent_renames(tmp_path, monkeypatch, capsys):
    code, stdout, out = _promote(
        tmp_path, monkeypatch, capsys, _render(_dev_generated_config()), live={PROD_1: {"other"}}
    )
    assert code == 0, stdout
    _assert_all_renamed(out)
    assert "kept policy name" not in stdout


def test_unmanaged_policy_under_the_new_name_aborts_promote(tmp_path, monkeypatch, capsys):
    # The old policy is absent (state and listing agree), and an unmanaged
    # policy already holds the new name. Keeping the old name would create a
    # second mask beside it; taking the new name would collide.
    new_remote = f"{PROD_1}_gr_mask_{PROD_1}_redact"
    state = _state(tmp_path / "prod" / "data_access" / "terraform.tfstate", ["unrelated"])
    code, stdout, out = _promote(
        tmp_path, monkeypatch, capsys, _render(_dev_generated_config()), state=state,
        live={PROD_1: {new_remote}, PROD_2: set()},
    )
    assert code == 1
    assert (
        f"{PROD_1} already has a policy named {new_remote} that GenieRails doesn't manage; "
        "remove it or import it into the prod data_access state, then re-run make promote"
    ) in stdout
    assert not out.exists() and not out.with_name("masking_functions.sql").exists()
    assert not list((tmp_path / "prod").rglob("*.tfvars"))


def test_unmanaged_new_name_with_the_old_policy_deployed_keeps_the_old_name(
    tmp_path, monkeypatch, capsys
):
    # Case 1 wins: the old policy is live, so keeping its name changes nothing.
    new_remote = f"{PROD_1}_gr_mask_{PROD_1}_redact"
    code, stdout, out = _promote(
        tmp_path, monkeypatch, capsys, _render(_dev_generated_config()),
        live={PROD_1: {LEGACY_REMOTE, new_remote}, PROD_2: set()},
    )
    assert code == 0, stdout
    assert f"kept policy name {LEGACY_REMOTE} (deployed in {PROD_1})" in stdout
    assert LEGACY in _names(out)
    _assert_no_live_mask_dropped(tmp_path, out)


def test_existing_new_name_managed_under_the_new_key_is_reused(tmp_path, monkeypatch, capsys):
    new_key = f"gr_mask_{PROD_1}_redact"
    state = _state(tmp_path / "prod" / "data_access" / "terraform.tfstate", [new_key])
    code, stdout, out = _promote(
        tmp_path, monkeypatch, capsys, _render(_dev_generated_config()), state=state,
        live={PROD_1: {f"{PROD_1}_{new_key}"}, PROD_2: set()},
    )
    assert code == 0, stdout
    _assert_all_renamed(out)


def test_draft_only_policy_is_renamed_when_confirmed_not_live(tmp_path, monkeypatch, capsys):
    # An earlier, never-applied promote left the old name in prod's drafts.
    draft = tmp_path / "prod" / "generated" / "abac.auto.tfvars"
    da = tmp_path / "prod" / "data_access" / "abac.auto.tfvars"
    for path in (draft, da):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f'fgac_policies = [\n  {{\n    name = "{LEGACY}"\n    catalog = "{PROD_1}"\n  }}\n]\n')
    code, stdout, out = _promote(
        tmp_path, monkeypatch, capsys, _render(_dev_generated_config()), live={PROD_1: set()}
    )
    assert code == 0, stdout
    _assert_all_renamed(out)


def test_state_absent_and_api_unavailable_keeps_names(tmp_path, monkeypatch, capsys):
    code, stdout, out = _promote(tmp_path, monkeypatch, capsys, _render(_dev_generated_config()))
    assert code == 0, stdout
    assert f"kept policy name {LEGACY_REMOTE} (couldn't confirm it isn't deployed in {PROD_1})" in stdout
    names = _names(out)
    # Pre-#68 names throughout: no key moves, so no delete is possible.
    assert set(names) == {p["name"] for p in _dev_generated_config()["fgac_policies"]}
    _assert_no_live_mask_dropped(tmp_path, out)


def test_unreadable_state_and_api_unavailable_keeps_names(tmp_path, monkeypatch, capsys):
    state = tmp_path / "prod" / "data_access" / "terraform.tfstate"
    state.parent.mkdir(parents=True)
    state.write_text("{ not json")
    assert read_state_policy_keys(state) is None
    assert read_state_policy_keys(tmp_path / "absent.tfstate") is None
    code, stdout, out = _promote(tmp_path, monkeypatch, capsys, _render(_dev_generated_config()), state=state)
    assert code == 0, stdout
    assert LEGACY in _names(out)
    assert "couldn't confirm" in stdout


def test_api_says_deployed_keeps_name(tmp_path, monkeypatch, capsys):
    code, stdout, out = _promote(
        tmp_path, monkeypatch, capsys, _render(_dev_generated_config()),
        live={PROD_1: {LEGACY_REMOTE}, PROD_2: set()},
    )
    assert code == 0, stdout
    assert f"kept policy name {LEGACY_REMOTE} (deployed in {PROD_1})" in stdout
    names = _names(out)
    assert LEGACY in names
    assert f"gr_mask_{PROD_1}_redact" not in names, "kept and renamed copies must not both land"
    assert f"gr_mask_{PROD_2}_redact" in names
    assert f"region_filter_{PROD_1}" in names
    _assert_no_live_mask_dropped(tmp_path, out)


def test_state_keys_are_qualified_by_catalog_and_resource(tmp_path, monkeypatch, capsys):
    other = {"module": "module.other", "mode": "managed", "type": "databricks_policy_info",
             "name": "policies", "instances": [{"index_key": LEGACY,
                                                "attributes": {"on_securable_fullname": PROD_1}}]}
    state = _state(tmp_path / "prod" / "data_access" / "terraform.tfstate", [LEGACY],
                   catalog="some_other_catalog", extra=[other])
    assert read_state_policy_keys(state) == {("some_other_catalog", LEGACY)}
    code, stdout, out = _promote(
        tmp_path, monkeypatch, capsys, _render(_dev_generated_config()), state=state,
        live={PROD_1: set(), PROD_2: set()},
    )
    assert code == 0, stdout
    _assert_all_renamed(out)


def test_state_says_deployed_overrides_an_api_miss(tmp_path, monkeypatch, capsys):
    state = _state(tmp_path / "prod" / "data_access" / "terraform.tfstate", [LEGACY])
    code, stdout, out = _promote(
        tmp_path, monkeypatch, capsys, _render(_dev_generated_config()), state=state, live={PROD_1: set()}
    )
    assert code == 0, stdout
    assert LEGACY in _names(out)
    _assert_no_live_mask_dropped(tmp_path, out)

    # Stable across re-promotes.
    again_code, again_stdout, again = _promote(
        tmp_path, monkeypatch, capsys, _render(_dev_generated_config()), state=state, live={PROD_1: set()}
    )
    assert again_code == 0, again_stdout
    assert sorted(_names(again)) == sorted(_names(out))


def test_lister_is_unavailable_without_real_credentials(tmp_path, monkeypatch):
    auth = tmp_path / "auth.auto.tfvars"
    assert live_policy_lister(auth)(PROD_1) is None
    auth.write_text('databricks_workspace_host = "<your_workspace_host>"\ndatabricks_client_id = "<your_client_id>"\n')
    assert live_policy_lister(auth)(PROD_1) is None


def test_lister_api_error_is_unavailable(tmp_path, monkeypatch):
    import databricks.sdk
    import databricks.sdk.core

    auth = tmp_path / "auth.auto.tfvars"
    auth.write_text(
        'databricks_workspace_host = "https://example.invalid"\n'
        'databricks_client_id = "sp"\ndatabricks_client_secret = "secret"\n'
    )
    listed = []

    class Policies:
        def list_policies(self, on_securable_type, on_securable_fullname):
            listed.append((on_securable_type, on_securable_fullname))
            if on_securable_fullname == "broken":
                raise PermissionError("denied")
            return [type("P", (), {"name": LEGACY_REMOTE})()]

    class Config:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class Client:
        def __init__(self, config):
            assert config.kwargs["host"] == "https://example.invalid"
            assert config.kwargs["client_secret"] == "secret"
            self.policies = Policies()

    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", Client)
    monkeypatch.setattr(databricks.sdk.core, "Config", Config)
    lister = live_policy_lister(auth)
    assert lister(PROD_1) == {LEGACY_REMOTE}
    assert listed == [("CATALOG", PROD_1)]
    assert lister("broken") is None


def test_lister_gives_up_on_a_hung_host(tmp_path, monkeypatch):
    import threading
    import databricks.sdk

    auth = tmp_path / "auth.auto.tfvars"
    auth.write_text(
        'databricks_workspace_host = "https://example.invalid"\n'
        'databricks_client_id = "sp"\ndatabricks_client_secret = "secret"\n'
    )
    release = threading.Event()

    def hang(**_kwargs):
        release.wait(30)
        raise TimeoutError

    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", hang)
    lister = live_policy_lister(auth, timeout=0.2)
    try:
        assert lister(PROD_1) is None
        assert lister(PROD_2) is None, "a hung API stays unavailable for the run"
    finally:
        release.set()


def test_promoted_names_must_stay_unique():
    source = (
        'fgac_policies = [\n'
        f'  {{\n    name = "gr_mask_{DEV_1}_redact"\n    catalog = "{DEV_1}"\n  }},\n'
        f'  {{\n    name = "gr_mask_{PROD_1}_redact"\n    catalog = "{PROD_1}"\n  }},\n'
        ']\n'
    )
    pairs = [(DEV_1, PROD_1)]
    with pytest.raises(ValueError, match="share a name"):
        remap_policy_names(source, remap_hcl(source, pairs), pairs, set(), lambda _c: set())


def test_promote_passes_destination_state_and_auth_only():
    makefile = (SHARED / "Makefile.shared").read_text()
    body = makefile[makefile.index("remap_generated_config.py\" \\"):]
    body = body[: body.index("derive_genie_acls.py")]
    assert '--state "$$dest_env_dir/$(DATA_ACCESS_SUBDIR)/terraform.tfstate"' in body
    assert '--auth "$$dest_env_dir/$(DATA_ACCESS_SUBDIR)/auth.auto.tfvars"' in body
    assert "--deployed" not in body


# ---------------------------------------------------------------------------
# Plan level: the real data_access root and provider, against a seeded state
# that holds the live prod policy under its pre-fix name. -refresh=false keeps
# the plan offline.
# ---------------------------------------------------------------------------

_SEEDED_ADDRESS = 'module.data_access.databricks_policy_info.policies["{key}"]'


def _seed_state(path: Path, key: str) -> None:
    attributes = {
        "id": "seeded", "name": f"{PROD_1}_{key}",
        "on_securable_type": "CATALOG", "on_securable_fullname": PROD_1,
        "policy_type": "POLICY_TYPE_COLUMN_MASK", "for_securable_type": "TABLE",
        "to_principals": ["account users"], "except_principals": None, "comment": "",
        "when_condition": None,
        "match_columns": [{"condition": "hasTagValue('gr_treatment', 'redact')", "alias": "gr_treatment_redact"}],
        "column_mask": {"function_name": f"{PROD_1}.payments.mask_redact",
                        "on_column": "gr_treatment_redact", "using": []},
        "row_filter": None, "created_at": 1, "created_by": "seed",
        "updated_at": 1, "updated_by": "seed", "provider_config": None,
    }
    path.write_text(json.dumps({
        "version": 4, "terraform_version": "1.11.4", "serial": 1, "lineage": "seed", "outputs": {},
        "resources": [{
            "module": "module.data_access", "mode": "managed",
            "type": "databricks_policy_info", "name": "policies",
            "provider": 'provider["registry.terraform.io/databricks/databricks"].workspace',
            "instances": [{"index_key": key, "schema_version": 0, "attributes": attributes}],
        }],
    }))


def _policy_plan(tmp_path: Path, generated: Path) -> dict[str, tuple[list[str], str | None, str | None]]:
    env = tmp_path / "plan_env"
    (env / "data_access").mkdir(parents=True)
    split = subprocess.run(
        [sys.executable, str(SPLIT), str(generated), str(env / "account.auto.tfvars"),
         str(env / "data_access" / "abac.auto.tfvars"), str(env / "abac.auto.tfvars")],
        text=True, capture_output=True,
    )
    assert split.returncode == 0, split.stdout + split.stderr
    da = env / "data_access"
    (da / "masking_functions.sql").write_text("SELECT 1;\n")
    (da / "auth.auto.tfvars").write_text(
        'databricks_account_id = "account"\ndatabricks_client_id = "sp"\n'
        'databricks_client_secret = "secret"\n'
        'databricks_workspace_host = "https://example.invalid"\n'
        'sql_warehouse_id = "warehouse"\n'
        f'uc_tables = ["{PROD_1}.payments.payments"]\n'
    )
    _seed_state(da / "terraform.tfstate", f"gr_mask_{DEV_1}_redact")

    tf_env = {**os.environ, "TF_DATA_DIR": str(da / ".terraform"), "TF_IN_AUTOMATION": "1"}
    init = subprocess.run(
        ["terraform", "init", "-input=false",
         f"-backend-config=path={da / 'terraform.tfstate'}"],
        cwd=ROOT, env=tf_env, text=True, capture_output=True,
    )
    assert init.returncode == 0, init.stdout + init.stderr
    plan_file = da / "plan.bin"
    plan = subprocess.run(
        ["terraform", "plan", "-refresh=false", "-lock=false", "-input=false", "-no-color",
         f"-out={plan_file}", f"-var=env_dir={da}",
         f"-var-file={da / 'auth.auto.tfvars'}", f"-var-file={da / 'abac.auto.tfvars'}"],
        cwd=ROOT, env=tf_env, text=True, capture_output=True,
    )
    assert plan.returncode == 0, plan.stdout + plan.stderr
    shown = subprocess.run(
        ["terraform", "show", "-json", str(plan_file)],
        cwd=ROOT, env=tf_env, text=True, capture_output=True,
    )
    assert shown.returncode == 0, shown.stderr
    return {
        rc["address"]: (
            rc["change"]["actions"],
            (rc["change"].get("before") or {}).get("name"),
            (rc["change"].get("after") or {}).get("name"),
        )
        for rc in json.loads(shown.stdout)["resource_changes"]
        if rc["type"] == "databricks_policy_info"
    }


