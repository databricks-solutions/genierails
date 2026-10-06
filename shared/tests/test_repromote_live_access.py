"""Re-promoting to a live, certified prod keeps business access open.

Covers: the promote condition (live AND certified), the release record that
caps exposure at what the last `make release` opened, Terraform ordering
(masks before grants), certify failing before any apply, and release on an
already-open gate exposing the new footprint.
"""

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

import hcl2
import pytest

from scripts import certification_receipt as cr
from scripts import remap_env_config

ROOT = Path(__file__).parents[2]
SHARED = ROOT / "shared"
CLOUD_ROOTS = [ROOT / "aws", ROOT / "azure"]
LIVE_LINE = (
    "prod is live: business access stays open; run make certify ENV=prod, "
    "then make release ENV=prod to verify and expose any new tables."
)


def _clean_env():
    return {
        key: value
        for key, value in os.environ.items()
        if key not in ("VERIFY_KEY_COLUMN", "APPLY_FLAGS", "EXPOSURE_SCOPE",
                       "MAKEFLAGS", "MAKELEVEL")
    }


def _write_state(path: Path, resources: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"version": 4, "resources": resources}))


def _table_grants_state(env_dir: Path, keys: list[str]) -> None:
    _write_state(env_dir / "data_access" / "terraform.tfstate", [{
        "mode": "managed", "module": "module.data_access",
        "type": "databricks_grant", "name": "table_access",
        "instances": [{"index_key": key, "attributes": {}} for key in keys],
    }])


def _genie_acls_state(env_dir: Path, acls: dict[str, str]) -> None:
    _write_state(env_dir / "terraform.tfstate", [{
        "mode": "managed", "module": "module.workspace",
        "type": "null_resource", "name": "genie_space_acls",
        "instances": [
            {"index_key": key, "attributes": {"triggers": {"groups": groups}}}
            for key, groups in acls.items()
        ],
    }])


def _prod(tmp_path, gate="false"):
    env_dir = tmp_path / "prod"
    (env_dir / "generated").mkdir(parents=True)
    (env_dir / "data_access").mkdir()
    (env_dir / "generated" / "abac.auto.tfvars").write_text("tag_assignments = []\n")
    (env_dir / "generated" / "masking_functions.sql").write_text("-- masks\n")
    (env_dir / "env.auto.tfvars").write_text(
        'genie_spaces = [{ name = "Agent", uc_tables = ["prod.s.released"] }]\n'
        f'uc_tables = ["prod.s.released"]\nbusiness_access_enabled = {gate}\n'
    )
    return env_dir


def _released_prod(tmp_path):
    """Prod after a successful make release: gate open, receipt, release record."""
    env_dir = _prod(tmp_path, gate="true")
    _table_grants_state(env_dir, ["prod.s.released|analysts"])
    _genie_acls_state(env_dir, {"agent": "analysts"})
    cr.write_receipt(env_dir, "certify", "prod")
    receipt = json.loads(cr.receipt_path(env_dir).read_text())
    cr.write_release_record(env_dir, "prod", receipt["fingerprint"])
    return env_dir


def _promote(tmp_path, monkeypatch, dest):
    source = tmp_path / "dev"
    source.mkdir(exist_ok=True)
    (source / "env.auto.tfvars").write_text(
        'genie_spaces = [{ name = "Agent", uc_tables = '
        '["dev.s.released", "dev.s.new_table"] }]\n'
    )
    monkeypatch.setattr(sys, "argv", [
        "remap_env_config.py", str(source), str(dest), "dev=prod"
    ])
    remap_env_config.main()
    return hcl2.load((dest / "env.auto.tfvars").open())


# ── promote: the "live and certified" condition ───────────────────────────────


def test_repromote_to_live_certified_prod_keeps_gate_open(tmp_path, monkeypatch, capsys):
    dest = _released_prod(tmp_path)
    config = _promote(tmp_path, monkeypatch, dest)
    assert config["business_access_enabled"] is True
    assert "Kept destination business_access_enabled=true (released " in capsys.readouterr().out
    assert cr.release_path(dest).exists()
    # The rules changed: the receipt is stale, so make release refuses until certify.
    current, reason = cr.check_receipt(dest, "prod")
    assert not current and "config changed since certification" in reason


def test_repromote_after_a_failed_certify_still_keeps_gate_open(tmp_path, monkeypatch):
    """certify clears the receipt first; a failure leaves none, prod stays released."""
    dest = _released_prod(tmp_path)
    cr.clear_receipt(dest)
    assert _promote(tmp_path, monkeypatch, dest)["business_access_enabled"] is True


def test_first_promote_writes_gate_closed(tmp_path, monkeypatch):
    dest = tmp_path / "prod"
    dest.mkdir()
    config = _promote(tmp_path, monkeypatch, dest)
    assert config["business_access_enabled"] is False
    assert not cr.release_path(dest).exists()


def test_not_yet_released_prod_stays_closed(tmp_path, monkeypatch):
    dest = _prod(tmp_path, gate="false")
    cr.write_receipt(dest, "certify", "prod")  # certified but never released
    assert _promote(tmp_path, monkeypatch, dest)["business_access_enabled"] is False


def test_closed_gate_drops_a_stale_release_record(tmp_path, monkeypatch):
    dest = _released_prod(tmp_path)
    text = (dest / "env.auto.tfvars").read_text()
    (dest / "env.auto.tfvars").write_text(text.replace("= true", "= false"))
    assert _promote(tmp_path, monkeypatch, dest)["business_access_enabled"] is False
    assert not cr.release_path(dest).exists()


def test_hand_opened_gate_without_release_or_current_receipt_is_reset(
    tmp_path, monkeypatch, capsys
):
    dest = _prod(tmp_path, gate="true")
    assert _promote(tmp_path, monkeypatch, dest)["business_access_enabled"] is False
    assert "Reset destination business_access_enabled=true to false (not live and certified" in (
        capsys.readouterr().out
    )


def test_forged_release_record_is_not_trusted(tmp_path, monkeypatch):
    dest = _released_prod(tmp_path)
    cr.clear_receipt(dest)
    record = json.loads(cr.release_path(dest).read_text())
    record["table_grants"].append("prod.s.new_table|analysts")
    cr.release_path(dest).write_text(json.dumps(record))
    assert _promote(tmp_path, monkeypatch, dest)["business_access_enabled"] is False


def test_prod_released_before_release_records_is_kept_while_receipt_is_current(
    tmp_path, monkeypatch
):
    dest = _prod(tmp_path, gate="true")
    _table_grants_state(dest, ["prod.s.released|analysts"])
    cr.write_receipt(dest, "certify", "prod")
    assert _promote(tmp_path, monkeypatch, dest)["business_access_enabled"] is True
    record = cr.load_release_record(dest, "prod")
    assert record["table_grants"] == ["prod.s.released|analysts"]


def test_promote_prints_the_live_line_only_when_the_gate_stays_open():
    makefile = (SHARED / "Makefile.shared").read_text()
    start = makefile.index('echo "=== Promote complete: $(SOURCE_ENV) -> $(DEST_ENV) ==="')
    block = makefile[start:makefile.index("trap - EXIT", start)]
    gate_check = "grep -Eq '^[[:space:]]*business_access_enabled[[:space:]]*=[[:space:]]*true'"
    assert gate_check in block
    live_echo = block.split(gate_check, 1)[1].split("else", 1)[0]
    message = live_echo.split('echo "', 1)[1].split('";', 1)[0]
    assert message.replace("$(DEST_ENV)", "prod") == LIVE_LINE


# ── certify with the gate preserved ───────────────────────────────────────────


def _runner(tmp_path):
    log = tmp_path / "runner.log"
    runner = tmp_path / "record-runner"
    runner.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> \"{log}\"\n"
        'for arg in "$@"; do case "$arg" in -var-file=*) '
        f'cat "${{arg#-var-file=}}" >> "{log}.vars";; esac; done\n'
    )
    runner.chmod(0o755)
    return runner, log


def _apply_workspace_layer(env_dir, runner, *extra):
    (env_dir / "abac.auto.tfvars").write_text("# present\n")
    return subprocess.run(
        ["make", "--no-print-directory", "_apply-layer", "LAYER=workspace",
         "TARGET_ENV=prod", f"LAYER_ENV_DIR={env_dir}", f"ROOT_RUNNER={runner}",
         f"ACCOUNT_ENV_DIR={env_dir.parent / 'account'}", *extra],
        cwd=CLOUD_ROOTS[0], text=True, capture_output=True, env=_clean_env(),
    )


def test_apply_after_release_is_capped_at_the_released_footprint(tmp_path):
    env_dir = _released_prod(tmp_path)
    runner, log = _runner(tmp_path)
    result = _apply_workspace_layer(env_dir, runner)
    assert result.returncode == 0, result.stdout + result.stderr
    applies = [line for line in log.read_text().splitlines() if " apply " in f" {line} "]
    assert applies and all("-var-file=" in line for line in applies)
    capped = json.loads(cr._released_vars_path(env_dir, "workspace").read_text())
    assert capped == {"released_genie_acls": {"agent": ["analysts"]}}
    assert "new tables/agents stay withheld until make release" in result.stdout

    data_access = cr.write_released_vars(env_dir, "data_access", "prod")
    assert json.loads(data_access.read_text()) == {
        "released_table_grants": ["prod.s.released|analysts"]
    }


def test_release_apply_is_uncapped(tmp_path):
    env_dir = _released_prod(tmp_path)
    runner, log = _runner(tmp_path)
    result = _apply_workspace_layer(env_dir, runner, "EXPOSURE_SCOPE=full")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "-var-file=" not in log.read_text()


def test_never_released_env_is_uncapped(tmp_path):
    env_dir = _prod(tmp_path)
    runner, log = _runner(tmp_path)
    result = _apply_workspace_layer(env_dir, runner)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "-var-file=" not in log.read_text()
    assert not cr._released_vars_path(env_dir, "workspace").exists()


def test_corrupt_release_record_fails_the_apply_closed(tmp_path):
    env_dir = _released_prod(tmp_path)
    cr.release_path(env_dir).write_text("{not json")
    runner, log = _runner(tmp_path)
    result = _apply_workspace_layer(env_dir, runner)
    assert result.returncode != 0
    assert "malformed release record" in result.stderr
    assert not log.exists() or " apply " not in log.read_text()


def _resource_block(source, name):
    start = source.index(f'resource "{name.split(".")[0]}" "{name.split(".")[1]}"')
    end = source.find('\nresource "', start + 1)
    return source[start:end if end != -1 else None]


def test_masks_and_policies_are_applied_before_any_select_grant():
    source = (SHARED / "modules/data_access/main.tf").read_text()
    grants = _resource_block(source, "databricks_grant.table_access")
    policies = _resource_block(source, "databricks_policy_info.policies")
    assert "depends_on = [databricks_policy_info.policies]" in grants
    assert "databricks_grant.table_access" not in policies
    # policies already wait for the masking functions and tag propagation
    assert "null_resource.deploy_masking_functions" in policies
    assert "time_sleep.wait_for_tag_propagation" in policies


def test_capped_certify_keeps_existing_grants_and_withholds_new_tables():
    """Plan-level proof (mock providers) of the cap in both Terraform roots."""
    for root in ("data_access", "workspace"):
        cwd = SHARED / "roots" / root
        init = subprocess.run(["terraform", "init", "-backend=false", "-input=false"],
                              cwd=cwd, text=True, capture_output=True)
        assert init.returncode == 0, init.stdout + init.stderr
        result = subprocess.run(
            ["terraform", "test", "-no-color", "-filter=tests/exposure_cap.tftest.hcl"],
            cwd=cwd, text=True, capture_output=True,
        )
        assert result.returncode == 0, result.stdout + result.stderr


# ── certify failure / release on an open gate ────────────────────────────────


def _stub_make(tmp_path, fail_on=None, on=None):
    log = tmp_path / "recursive-make.log"
    stub = tmp_path / "record-make"
    body = ["#!/bin/sh", f"printf '%s\\n' \"$*\" >> \"{log}\""]
    for target, action in (on or {}).items():
        body.append(f'if [ "$1" = "{target}" ]; then {action}; fi')
    if fail_on:
        body.append(f'[ "$1" = "{fail_on}" ] && exit 1')
    body.append("exit 0")
    stub.write_text("\n".join(body) + "\n")
    stub.chmod(0o755)
    return stub, log


def _make(target, env_dir, stub, *extra):
    return subprocess.run(
        ["make", target, "ENV=prod", f"ENV_DIR={env_dir}",
         f"ACCOUNT_ENV_DIR={env_dir.parent / 'account'}", f"MAKE={stub}", *extra],
        cwd=CLOUD_ROOTS[0], text=True, capture_output=True, env=_clean_env(),
    )


def _calls(log):
    return [shlex.split(line) for line in log.read_text().splitlines()] if log.exists() else []


@pytest.mark.parametrize("failing", ["coverage-gate", "validate-generated"])
def test_failed_certify_on_live_prod_applies_nothing(tmp_path, failing):
    env_dir = _released_prod(tmp_path)
    record_before = cr.release_path(env_dir).read_text()
    stub, log = _stub_make(tmp_path, fail_on=failing)
    result = _make("certify", env_dir, stub)
    assert result.returncode != 0
    called = [call[0] for call in _calls(log)]
    assert called[-1] == failing
    assert not {"apply", "apply-governance", "apply-genie"} & set(called)
    # Live access is untouched: gate still open, release record (the cap) intact.
    assert cr.gate_open(env_dir)
    assert cr.release_path(env_dir).read_text() == record_before
    assert not cr.receipt_path(env_dir).exists()


def test_release_on_an_already_open_gate_exposes_the_new_footprint(tmp_path):
    env_dir = _released_prod(tmp_path)
    # A re-promote added a table; certify passed and wrote a fresh receipt.
    (env_dir / "generated" / "abac.auto.tfvars").write_text("tag_assignments = [1]\n")
    cr.write_receipt(env_dir, "certify", "prod")
    # The uncapped release apply grants the new table (simulated in state).
    grant_new = (
        f"python3 -c 'import json,sys; p=sys.argv[1]; s=json.load(open(p)); "
        f"s[\"resources\"][0][\"instances\"].append({{\"index_key\": "
        f"\"prod.s.new_table|analysts\", \"attributes\": {{}}}}); "
        f"json.dump(s, open(p, \"w\"))' '{env_dir / 'data_access' / 'terraform.tfstate'}'"
    )
    stub, log = _stub_make(tmp_path, on={"apply": grant_new})
    result = _make("release", env_dir, stub)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _calls(log)[0] == [
        "apply", "ENV=prod", "APPLY_FLAGS=-var=business_access_enabled=true",
        "EXPOSURE_SCOPE=full",
    ]
    assert _calls(log)[-1] == ["verify-access", "ENV=prod"]
    assert cr.gate_open(env_dir)
    assert cr.check_receipt(env_dir, "prod")[0]
    record = cr.load_release_record(env_dir, "prod")
    assert record["table_grants"] == [
        "prod.s.new_table|analysts", "prod.s.released|analysts",
    ]
    assert "Release record written" in result.stdout


def test_first_release_writes_the_release_record(tmp_path):
    env_dir = _prod(tmp_path)
    cr.write_receipt(env_dir, "certify", "prod")
    _table_grants_state(env_dir, ["prod.s.released|analysts"])
    stub, _log = _stub_make(tmp_path)
    result = _make("release", env_dir, stub)
    assert result.returncode == 0, result.stdout + result.stderr
    assert cr.load_release_record(env_dir, "prod")["table_grants"] == [
        "prod.s.released|analysts"
    ]
