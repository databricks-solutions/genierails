"""Tests for IdP-CONSUME-BY-DEFAULT (#31).

Two layers, both without any Databricks / LLM / Terraform runtime:

  1. The generate_abac.py group preflight — consume-by-default verifies that
     referenced access-tier groups already exist as IdP-synced account groups,
     failing loudly (naming the missing group) otherwise; the opt-in create
     path is a no-op.

  2. The account Terraform module/root wiring — groups are looked up by name
     (not created) by default, membership is not managed by default, and the
     opt-in create path still mints groups. Asserted structurally by parsing
     the .tf files with hcl2 (no terraform binary required).
"""
import sys
from pathlib import Path

import hcl2
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import generate_abac
from generate_abac import (
    GroupPreflightError,
    build_prompt,
    find_missing_idp_groups,
    main,
    preflight_consume_groups,
)

SHARED = Path(__file__).parent.parent
ACCOUNT_MODULE = SHARED / "modules" / "account"
ACCOUNT_ROOT = SHARED / "roots" / "account"


class TestPrepareEnvConsumeByDefault:
    def test_scaffolds_account_manage_groups_false(self):
        makefile = (SHARED / "Makefile.shared").read_text()
        prepare_env = makefile.split("_prepare-env:", 1)[1].split("\n_bootstrap:", 1)[0]
        assert "manage_groups = false" in prepare_env
        assert "manage_groups = true" not in prepare_env

    def test_preserves_explicit_create_groups_opt_in(self):
        makefile = (SHARED / "Makefile.shared").read_text()
        prepare_env = makefile.split("_prepare-env:", 1)[1].split("\n_bootstrap:", 1)[0]
        scaffold = prepare_env.split('if [ ! -e "$$env_dir/env.auto.tfvars" ]; then', 1)
        assert len(scaffold) == 2
        assert "manage_groups = false" in scaffold[1]


# ---------------------------------------------------------------------------
# generate_abac.py — group preflight
# ---------------------------------------------------------------------------
class TestFindMissingIdpGroups:
    def test_none_missing(self):
        assert find_missing_idp_groups(["A", "B"], ["A", "B", "C"]) == []

    def test_reports_missing(self):
        assert find_missing_idp_groups(["A", "X"], ["A", "B"]) == ["X"]

    def test_preserves_order_and_dedupes(self):
        assert find_missing_idp_groups(["X", "Y", "X", "A"], ["A"]) == ["X", "Y"]

    def test_empty_referenced(self):
        assert find_missing_idp_groups([], ["A"]) == []

    def test_ignores_falsy_names(self):
        assert find_missing_idp_groups(["", "A"], ["", "B"]) == ["A"]


class TestPreflightConsumeGroups:
    def test_all_present_is_noop(self):
        # No exception when every referenced group exists.
        preflight_consume_groups(["Finance_Analyst"], ["Finance_Analyst", "Admin"])

    def test_missing_group_fails_loudly_and_names_it(self):
        with pytest.raises(GroupPreflightError) as excinfo:
            preflight_consume_groups(
                ["Finance_Analyst", "Ghost_Group"],
                ["Finance_Analyst"],
                create_groups=False,
            )
        msg = str(excinfo.value)
        # The missing group is named, the present one is not flagged.
        assert "Ghost_Group" in msg
        assert "preflight failed" in msg.lower()
        # Actionable remediation is surfaced.
        assert "AIM" in msg or "SCIM" in msg
        assert "--create-groups" in msg

    def test_create_groups_opt_in_skips_preflight(self):
        # In the demo/greenfield create path, a not-yet-existing group is fine.
        preflight_consume_groups(
            ["Brand_New_Group"], [], create_groups=True
        )


# ---------------------------------------------------------------------------
# Terraform wiring — parsed structurally with hcl2 (no terraform binary)
# ---------------------------------------------------------------------------
def _load_tf(path: Path) -> dict:
    with open(path) as f:
        return hcl2.load(f)


def _variable_default(tf: dict, name: str):
    for v in tf.get("variable", []):
        if name in v:
            return v[name].get("default")
    raise AssertionError(f"variable {name!r} not found")


def _block_for_each(tf: dict, kind: str, block_type: str, block_name: str) -> str:
    for block in tf.get(kind, []):
        named = block.get(block_type)
        if named and block_name in named:
            return named[block_name].get("for_each", "")
    raise AssertionError(f"{kind} {block_type}.{block_name} not found")


def _parse_ternary(expr: str) -> tuple[str, str, str]:
    """Parse ``${cond ? true_branch : false_branch}`` into its three parts.

    Asserting the branch orientation (not just substring presence) is the point:
    a reversed ternary like ``var.manage_groups ? {} : var.groups`` must FAIL the
    consume/create wiring tests, which a substring check would not catch.
    """
    inner = expr.strip()
    assert inner.startswith("${") and inner.endswith("}"), f"not an interpolation: {expr!r}"
    inner = inner[2:-1].strip()
    assert "?" in inner and ":" in inner, f"not a ternary: {expr!r}"
    cond, rest = inner.split("?", 1)
    true_branch, false_branch = rest.split(":", 1)
    return cond.strip(), true_branch.strip(), false_branch.strip()


class TestAccountModuleConsumeByDefault:
    def test_module_manage_groups_defaults_to_false(self):
        tf = _load_tf(ACCOUNT_MODULE / "variables.tf")
        assert _variable_default(tf, "manage_groups") is False

    def test_root_manage_groups_defaults_to_false(self):
        tf = _load_tf(ACCOUNT_ROOT / "main.tf")
        assert _variable_default(tf, "manage_groups") is False

    def test_consumed_data_source_orientation_consume_on_false(self):
        # data.databricks_group.consumed drives the CONSUME (lookup) path.
        # Orientation must be: manage_groups ? {} : var.groups
        #   true  (managing/create) -> {}         (no lookup)
        #   false (default/consume) -> var.groups (look them up)
        tf = _load_tf(ACCOUNT_MODULE / "main.tf")
        cond, true_b, false_b = _parse_ternary(
            _block_for_each(tf, "data", "databricks_group", "consumed")
        )
        assert "manage_groups" in cond
        assert true_b == "{}", f"expected empty lookup when managing, got {true_b!r}"
        assert "var.groups" in false_b, f"expected consume on false, got {false_b!r}"

    def test_group_resource_orientation_create_on_true(self):
        # resource.databricks_group.groups drives the CREATE path.
        # Orientation must be: manage_groups ? var.groups : {}
        #   true  (create) -> var.groups (mint them)
        #   false (default) -> {}        (mint nothing)
        tf = _load_tf(ACCOUNT_MODULE / "main.tf")
        cond, true_b, false_b = _parse_ternary(
            _block_for_each(tf, "resource", "databricks_group", "groups")
        )
        assert "manage_groups" in cond
        assert "var.groups" in true_b, f"expected create on true, got {true_b!r}"
        assert false_b == "{}", f"expected no creation by default, got {false_b!r}"

    def test_membership_resource_orientation_manage_on_true(self):
        # Membership is only managed in the create path (true branch).
        tf = _load_tf(ACCOUNT_MODULE / "main.tf")
        cond, true_b, false_b = _parse_ternary(
            _block_for_each(tf, "resource", "databricks_group_member", "members")
        )
        assert "manage_groups" in cond
        assert "local.group_member_map" in true_b
        assert false_b == "{}", f"expected no membership by default, got {false_b!r}"

    def test_opt_in_create_path_still_present(self):
        # The databricks_group resource (create path) must not be deleted —
        # it is demoted to opt-in, not removed.
        tf = _load_tf(ACCOUNT_MODULE / "main.tf")
        names = {
            name
            for block in tf.get("resource", [])
            for typ, named in block.items()
            if typ == "databricks_group"
            for name in named
        }
        assert "groups" in names


# ---------------------------------------------------------------------------
# build_prompt — group-invention instructions gated behind create mode
# ---------------------------------------------------------------------------
DDL = "CREATE TABLE cat.sch.tbl (id INT, ssn STRING);"


class TestBuildPromptGrouping:
    def test_consume_mode_strips_invention_instructions(self):
        prompt = build_prompt(DDL, group_names=["Finance_Analyst", "Admin"], create_groups=False)
        # The template's invent-groups steps must be gone.
        assert "Propose groups" not in prompt
        assert "Create **groups** (access tiers" not in prompt
        # The supplied names are pinned and invention is forbidden.
        assert "REQUIRED GROUP NAMES" in prompt
        assert "Finance_Analyst" in prompt
        assert "do NOT propose or invent new groups" in prompt

    def test_consume_mode_is_the_default(self):
        # No create_groups kwarg => consume (invention stripped).
        prompt = build_prompt(DDL, group_names=["Finance_Analyst"])
        assert "Propose groups" not in prompt

    def test_create_mode_keeps_invention_instructions(self):
        prompt = build_prompt(DDL, create_groups=True)
        assert "Propose groups" in prompt
        assert "Create **groups** (access tiers" in prompt


# ---------------------------------------------------------------------------
# CLI wiring — argparse mutual exclusion + main() default/consume/create paths
#
# These exercise main() itself (not just the pure helper) so a regression that
# lets the default path invent groups, or bypasses the preflight, is caught.
# ---------------------------------------------------------------------------
def _patch_common(monkeypatch, *, account_groups=None):
    """Stub out the heavy deps so main() runs offline up to build_prompt."""
    monkeypatch.setattr(generate_abac, "load_auth_config", lambda *a, **k: {})
    monkeypatch.setattr(
        generate_abac, "fetch_tables_from_databricks",
        lambda *a, **k: (DDL, [("cat", "sch")]),
    )
    monkeypatch.setattr(
        generate_abac, "list_account_group_names",
        lambda *a, **k: account_groups,
    )


class TestCliGroupMode:
    def test_groups_and_create_groups_are_mutually_exclusive(self, monkeypatch):
        # argparse rejects the contradictory combination with exit code 2.
        monkeypatch.setattr(sys, "argv", [
            "generate_abac.py", "--groups", "Finance_Analyst", "--create-groups",
        ])
        with pytest.raises(SystemExit) as excinfo:
            main()
        assert excinfo.value.code == 2

    def test_default_no_flags_errors_and_does_not_invent(self, monkeypatch, tmp_path):
        # Neither --groups nor --create-groups: main must refuse (exit 1) BEFORE
        # ever building a prompt, so the LLM is never asked to invent groups.
        called = {"build_prompt": False}
        monkeypatch.setattr(
            generate_abac, "build_prompt",
            lambda *a, **k: called.__setitem__("build_prompt", True) or "",
        )
        _patch_common(monkeypatch)
        monkeypatch.setattr(sys, "argv", [
            "generate_abac.py", "--tables", "cat.sch.tbl", "--dry-run",
            "--ddl-dir", str(tmp_path), "--out-dir", str(tmp_path),
        ])
        with pytest.raises(SystemExit) as excinfo:
            main()
        assert excinfo.value.code == 1
        assert called["build_prompt"] is False, "must not reach prompt building"

    def test_consume_mode_invokes_preflight(self, monkeypatch, tmp_path):
        # With --groups, main must call the preflight on the consume path.
        preflight_calls = []
        monkeypatch.setattr(
            generate_abac, "preflight_consume_groups",
            lambda referenced, existing, **k: preflight_calls.append((list(referenced), list(existing))),
        )
        build_calls = {}
        monkeypatch.setattr(
            generate_abac, "build_prompt",
            lambda *a, **k: build_calls.update(k) or "",
        )
        _patch_common(monkeypatch, account_groups=["Finance_Analyst"])
        monkeypatch.setattr(sys, "argv", [
            "generate_abac.py", "--groups", "Finance_Analyst",
            "--tables", "cat.sch.tbl", "--dry-run",
            "--ddl-dir", str(tmp_path), "--out-dir", str(tmp_path),
        ])
        with pytest.raises(SystemExit) as excinfo:
            main()
        assert excinfo.value.code == 0  # dry-run success
        assert preflight_calls == [(["Finance_Analyst"], ["Finance_Analyst"])]
        assert build_calls.get("create_groups") is False

    def test_consume_mode_missing_group_fails_loudly(self, monkeypatch, tmp_path):
        # Real preflight (not stubbed): a referenced group absent from the account
        # must abort main with exit 1 before any prompt is built.
        called = {"build_prompt": False}
        monkeypatch.setattr(
            generate_abac, "build_prompt",
            lambda *a, **k: called.__setitem__("build_prompt", True) or "",
        )
        _patch_common(monkeypatch, account_groups=["Real_Group"])
        monkeypatch.setattr(sys, "argv", [
            "generate_abac.py", "--groups", "Ghost_Group",
            "--tables", "cat.sch.tbl", "--dry-run",
            "--ddl-dir", str(tmp_path), "--out-dir", str(tmp_path),
        ])
        with pytest.raises(SystemExit) as excinfo:
            main()
        assert excinfo.value.code == 1
        assert called["build_prompt"] is False

    def test_create_mode_skips_preflight_and_invents(self, monkeypatch, tmp_path):
        # --create-groups: preflight is not called and build_prompt gets create_groups=True.
        preflight_calls = []
        monkeypatch.setattr(
            generate_abac, "preflight_consume_groups",
            lambda *a, **k: preflight_calls.append(a),
        )
        build_calls = {}
        monkeypatch.setattr(
            generate_abac, "build_prompt",
            lambda *a, **k: build_calls.update(k) or "",
        )
        _patch_common(monkeypatch)
        monkeypatch.setattr(sys, "argv", [
            "generate_abac.py", "--create-groups",
            "--tables", "cat.sch.tbl", "--dry-run",
            "--ddl-dir", str(tmp_path), "--out-dir", str(tmp_path),
        ])
        with pytest.raises(SystemExit) as excinfo:
            main()
        assert excinfo.value.code == 0
        assert preflight_calls == []
        assert build_calls.get("create_groups") is True
        assert build_calls.get("group_names") is None


def test_idp_preflight_defaults_account_host_on_aws(monkeypatch):
    """prerequisites.md lets AWS omit databricks_account_host; preflight must still run."""
    import databricks.sdk

    seen = {}

    class FakeAccountClient:
        def __init__(self, **kwargs):
            seen.update(kwargs)
            self.groups = type("G", (), {"list": lambda self: []})()

    monkeypatch.setattr(databricks.sdk, "AccountClient", FakeAccountClient)
    auth = {"databricks_account_id": "acct", "databricks_client_id": "id",
            "databricks_client_secret": "s"}

    assert generate_abac.list_account_group_names(auth) == []
    assert seen["host"] == "https://accounts.cloud.databricks.com"
