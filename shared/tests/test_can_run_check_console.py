"""can-run-check against the real workspace root under terraform console.

terraform console can't evaluate plantimestamp(), so the gate's refresh-time
checks must not reach the console expression: with a current passing gate
(the normal rehearse / release path) the check used to fail on
"(known after apply)" and block every workspace apply.
"""

import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

SHARED = Path(__file__).resolve().parents[1]
ROOT = SHARED / "roots" / "workspace"
sys.path.insert(0, str(SHARED / "scripts"))

import coverage_gate as cg  # noqa: E402
from tests.terraform_helpers import TIMEOUT, shared_copy, skip_if_providers_unavailable  # noqa: E402

pytestmark = pytest.mark.skipif(shutil.which("terraform") is None, reason="terraform not installed")


@pytest.fixture(scope="module")
def data_dir(tmp_path_factory):
    # Init the way make does (terraform_layer.sh), so a fresh clone without a
    # lock file, or with a stale one, initializes too; in a copy of shared/,
    # so the real root never gets a lock file from a test.
    global ROOT
    copy = shared_copy(tmp_path_factory.mktemp("shared"))
    ROOT = copy / "roots" / "workspace"
    tf = tmp_path_factory.mktemp("tf")
    init = subprocess.run([str(copy / "scripts" / "terraform_layer.sh"), "workspace", "test", "print-cmd", "plan"],
                          env={**os.environ, "LAYER_ENV_DIR": str(tf), "TF_IN_AUTOMATION": "1"},
                          text=True, capture_output=True, timeout=TIMEOUT)
    skip_if_providers_unavailable(init)
    return tf / ".terraform"


def _env(tmp_path, data_dir, refreshed_at):
    env = tmp_path / "env"
    (env / "data_access").mkdir(parents=True)
    (env / "abac.auto.tfvars").write_text(
        'databricks_account_id = "account"\ndatabricks_client_id = "sp"\n'
        'databricks_client_secret = "secret"\ndatabricks_workspace_id = "123"\n'
        'databricks_workspace_host = "https://example.invalid"\nsql_warehouse_id = "warehouse"\n'
        'groups = { analysts = {} }\n'
        'genie_spaces = [{ name = "Sales", genie_space_id = "space-1", uc_tables = ["cat.sch.customers"] }]\n'
        'genie_space_configs = { Sales = { acl_groups = ["analysts"] } }\n'
    )
    (env / "data_access" / "terraform.tfstate").write_text(json.dumps({"version": 4, "outputs": {
        "coverage_gate": {"value": {"fingerprint": "applied", "status": "pass", "max_age": "6h",
                                    "table_grant_count": 1}},
        "table_grant_resource_keys": {"value": ["cat.sch.customers|analysts"]},
    }}))
    (env / "data_access" / ".coverage_gate.json").write_text(json.dumps({
        "status": "pass", "fingerprint": "applied",
        "refreshed_at": refreshed_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }))
    runner = tmp_path / "runner"
    runner.write_text(
        "#!/bin/sh\n"
        f"cd '{ROOT}' && TF_DATA_DIR='{data_dir}' exec terraform console "
        f"-var-file='{env / 'abac.auto.tfvars'}' -var='env_dir={env}'\n"
    )
    runner.chmod(0o755)
    return env, runner


@pytest.mark.parametrize("age, message", [
    (timedelta(minutes=1), None),
    (timedelta(hours=7), "older than coverage_gate_max_age (6h)"),
    (timedelta(hours=-1), "records no live refresh"),
])
def test_can_run_check_judges_the_refresh_time_itself(tmp_path, data_dir, capsys, age, message):
    env, runner = _env(tmp_path, data_dir, datetime.now(timezone.utc) - age)
    # Never refuses the apply: Terraform withholds the new groups and applies
    # the rest; make fails after the apply (withheld).
    assert cg.can_run_check(env, "prod", runner, "") == 0
    err = capsys.readouterr().err
    assert json.loads((env / cg.CAN_RUN_FILENAME).read_text())["desired"] == {"sales": "analysts"}
    if message:
        assert "Genie CAN_RUN withheld" in err and "sales: +analysts" in err and message in err
    else:
        assert "withheld" not in err and "blocked" not in err
