import os
import subprocess
import sys
from pathlib import Path

import hcl2
import generate_abac
import pytest


SHARED = Path(__file__).parents[1]


def _fake_sdk(root: Path) -> Path:
    sdk = root / "fake-sdk" / "databricks" / "sdk"
    sdk.mkdir(parents=True)
    (sdk.parent / "__init__.py").write_text("")
    (sdk / "useragent.py").write_text("")
    (sdk / "config.py").write_text(
        "class Config:\n"
        "    def __init__(self, **kwargs): self.__dict__.update(kwargs)\n"
    )
    (sdk / "__init__.py").write_text(
        "class WorkspaceClient:\n"
        "    def __init__(self, **kwargs):\n"
        "        raise RuntimeError('SDK MUST NOT BE CALLED')\n"
    )
    return sdk.parent.parent


def _run_generate(tmp_path: Path, auth_text: str | None) -> subprocess.CompletedProcess[str]:
    auth = tmp_path / "envs" / "dev" / "auth.auto.tfvars"
    auth.parent.mkdir(parents=True)
    if auth_text is not None:
        auth.write_text(auth_text)
    config = tmp_path / "ambient.cfg"
    config.write_text(
        "[DEFAULT]\n"
        "host = https://wrong-profile.example\n"
        "token = wrong-profile-token\n"
    )
    env = os.environ.copy()
    env.update({
        "HOME": str(tmp_path / "home"),
        "DATABRICKS_CONFIG_FILE": str(config),
        "DATABRICKS_HOST": "https://wrong-env.example",
        "DATABRICKS_TOKEN": "wrong-env-token",
        "PIP_NO_INDEX": "1",
        "PYTHONPATH": os.pathsep.join([str(_fake_sdk(tmp_path)), str(SHARED)]),
    })
    return subprocess.run(
        [sys.executable, str(SHARED / "generate_abac.py"), "--auth-file", str(auth)],
        text=True, capture_output=True, env=env, timeout=30,
    )


def test_empty_auth_refuses_before_sdk_even_with_ambient_auth(tmp_path):
    result = _run_generate(tmp_path, "# empty template\n")

    assert result.returncode == 1
    assert "fill in" in result.stderr
    assert "envs/dev/auth.auto.tfvars" in result.stderr
    assert "SDK MUST NOT BE CALLED" not in result.stdout + result.stderr
    assert "Traceback" not in result.stdout + result.stderr


def test_missing_auth_refuses_as_incomplete_before_sdk(tmp_path):
    result = _run_generate(tmp_path, None)

    assert result.returncode == 1
    assert "fill in" in result.stderr
    assert "is not valid HCL" not in result.stderr
    assert "SDK MUST NOT BE CALLED" not in result.stdout + result.stderr


def test_invalid_auth_never_prints_secret(tmp_path):
    secret = "never-echo-this-client-secret"
    result = _run_generate(
        tmp_path,
        'databricks_workspace_host = "https://env.example"\n'
        'databricks_client_id = "env-client"\n'
        f'databricks_client_secret = "{secret}\n',
    )

    assert result.returncode == 1
    assert "is not valid HCL" in result.stderr
    assert secret not in result.stdout + result.stderr
    assert "Traceback" not in result.stdout + result.stderr


def test_workspace_config_uses_env_auth_and_ignores_ambient(monkeypatch):
    import databricks.sdk
    import databricks.sdk.config

    captured = {}

    class FakeConfig:
        def __init__(self, **kwargs):
            captured["config"] = kwargs

    class FakeClient:
        def __init__(self, **kwargs):
            captured["client"] = kwargs

    monkeypatch.setenv("DATABRICKS_HOST", "https://wrong-env.example")
    monkeypatch.setenv("DATABRICKS_TOKEN", "wrong-env-token")
    monkeypatch.setattr(databricks.sdk.config, "Config", FakeConfig)
    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", FakeClient)
    auth = {
        "databricks_workspace_host": "https://selected-env.example",
        "databricks_client_id": "selected-client",
        "databricks_client_secret": "selected-secret",
    }

    try:
        generate_abac.fetch_tables_from_databricks([], auth)
    except SystemExit as exc:
        assert exc.code == 1

    assert captured["config"] == {
        "host": "https://selected-env.example",
        "client_id": "selected-client",
        "client_secret": "selected-secret",
        "auth_type": "oauth-m2m",
        "product": "genierails",
        "product_version": "0.1.0",
    }
    assert set(captured["client"]) == {"config"}


def test_make_generate_preflight_precedes_agent_sdk_hook():
    makefile = (SHARED / "Makefile.shared").read_text()
    target = makefile[makefile.index("generate:"):makefile.index("# Agents (", makefile.index("generate:"))]
    assert target.index("scripts/auth_configured.py") < target.index("import-new --check")


@pytest.mark.parametrize(
    "auth_text, expected, absent",
    [
        ("# empty template\n", "fill in envs/dev/auth.auto.tfvars", "is not valid HCL"),
        (None, "fill in envs/dev/auth.auto.tfvars", "is not valid HCL"),
        ('databricks_client_secret = "unterminated\n',
         "envs/dev/auth.auto.tfvars is not valid HCL", "fill in"),
    ],
    ids=["empty", "missing", "malformed"],
)
def test_discover_agent_tables_refuses_unusable_auth_before_sdk(tmp_path, auth_text, expected, absent):
    env_dir = tmp_path / "envs" / "dev"
    env_dir.mkdir(parents=True)
    if auth_text is not None:
        (env_dir / "auth.auto.tfvars").write_text(auth_text)
    (env_dir / "env.auto.tfvars").write_text(
        'enable_classification = true\n'
        'genie_spaces = [{ genie_space_id = "space-from-wrong-workspace" }]\n'
    )
    config = tmp_path / "ambient.cfg"
    config.write_text(
        "[DEFAULT]\nhost = https://wrong-profile.example\ntoken = wrong-profile-token\n"
    )
    env = os.environ.copy()
    env.update({
        "HOME": str(tmp_path / "home"),
        "DATABRICKS_CONFIG_FILE": str(config),
        "DATABRICKS_HOST": "https://wrong-env.example",
        "DATABRICKS_TOKEN": "wrong-env-token",
        "PIP_NO_INDEX": "1",
        "PYTHONPATH": os.pathsep.join([str(_fake_sdk(tmp_path)), str(SHARED)]),
    })

    result = subprocess.run(
        [sys.executable, str(SHARED / "scripts" / "discover_agent_tables.py"), str(env_dir)],
        text=True, capture_output=True, env=env, timeout=30,
    )

    assert result.returncode == 1
    assert expected in result.stderr
    assert absent not in result.stderr
    assert "SDK MUST NOT BE CALLED" not in result.stdout + result.stderr
    assert "Traceback" not in result.stdout + result.stderr


@pytest.mark.parametrize(
    "target, auth_text, expected, absent",
    [
        ("capture", None, "fill in", "is not valid HCL"),
        ("capture", "# empty template\n", "fill in", "is not valid HCL"),
        ("generate", "# empty template\n", "fill in", "is not valid HCL"),
        ("capture", 'databricks_client_secret = "unterminated\n', "is not valid HCL", "fill in"),
        ("generate", 'databricks_client_secret = "unterminated\n', "is not valid HCL", "fill in"),
    ],
    ids=["capture-missing", "capture-empty", "generate-empty", "capture-malformed", "generate-malformed"],
)
def test_make_generate_and_capture_refuse_unusable_auth_before_sdk(
    tmp_path, target, auth_text, expected, absent
):
    cloud = tmp_path / "cloud"
    cloud.mkdir()
    base_env = os.environ.copy()
    base_env.update({"GIT_CONFIG_GLOBAL": os.devnull, "PIP_NO_INDEX": "1"})
    setup = subprocess.run(
        ["make", "--no-print-directory", "setup", "ENV=dev", f"CLOUD_ROOT={cloud}", f"SHARED_ROOT={SHARED}"],
        cwd=SHARED.parent / "aws", text=True, capture_output=True, env=base_env, timeout=120,
    )
    assert setup.returncode == 0, setup.stdout + setup.stderr
    env_dir = cloud / "envs" / "dev"
    auth = env_dir / "auth.auto.tfvars"
    if auth_text is None:
        auth.unlink()
    else:
        auth.write_text(auth_text)
    (env_dir / "env.auto.tfvars").write_text(
        'governance_mode = "legacy"\n'
        'genie_spaces = [{ name = "Payments", genie_space_id = "space-1", '
        'uc_tables = ["cat.sch.tbl"], acl_groups = ["analysts"] }]\n'
    )
    (env_dir / "agents" / "Finance Analytics.auto.tfvars").unlink()  # the agent is inline here
    config = tmp_path / "ambient.cfg"
    config.write_text(
        "[DEFAULT]\nhost = https://wrong-profile.example\ntoken = wrong-profile-token\n"
    )
    env = base_env | {
        "HOME": str(tmp_path / "home"),
        "DATABRICKS_CONFIG_FILE": str(config),
        "DATABRICKS_HOST": "https://wrong-env.example",
        "DATABRICKS_TOKEN": "wrong-env-token",
        "PYTHONPATH": os.pathsep.join([
            str(_fake_sdk(tmp_path)), str(Path(hcl2.__file__).parents[1]), str(SHARED),
        ]),
    }

    result = subprocess.run(
        ["make", "--no-print-directory", target, "ENV=dev", "SPACE=Payments",
         f"CLOUD_ROOT={cloud}", f"SHARED_ROOT={SHARED}"],
        cwd=SHARED.parent / "aws", text=True, capture_output=True, env=env, timeout=120,
    )

    assert result.returncode != 0
    assert expected in result.stderr
    assert absent not in result.stderr
    assert f"before running make {target}" in result.stderr or "is not valid HCL" in result.stderr
    assert "SDK MUST NOT BE CALLED" not in result.stdout + result.stderr
    assert "Traceback" not in result.stdout + result.stderr


def test_capture_api_uses_explicit_env_config(monkeypatch, tmp_path):
    import databricks.sdk
    import databricks.sdk.config
    from scripts import agent_ownership

    env_dir = tmp_path / "envs" / "dev"
    env_dir.mkdir(parents=True)
    (env_dir / "auth.auto.tfvars").write_text(
        'databricks_workspace_host = "https://selected-env.example"\n'
        'databricks_client_id = "selected-client"\n'
        'databricks_client_secret = "selected-secret"\n'
    )
    captured = {}

    class FakeConfig:
        def __init__(self, **kwargs):
            captured["config"] = kwargs

    class FakeClient:
        def __init__(self, **kwargs):
            captured["client"] = kwargs
            self.api_client = object()

    monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "stale-profile")
    monkeypatch.setenv("DATABRICKS_HOST", "https://wrong-env.example")
    monkeypatch.setenv("DATABRICKS_TOKEN", "wrong-env-token")
    monkeypatch.setattr(databricks.sdk.config, "Config", FakeConfig)
    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", FakeClient)

    agent_ownership._api(env_dir)

    assert captured["config"] == {
        "host": "https://selected-env.example",
        "client_id": "selected-client",
        "client_secret": "selected-secret",
        "auth_type": "oauth-m2m",
        "product": "genierails",
        "product_version": "0.1.0",
    }
    assert set(captured["client"]) == {"config"}


def test_capture_api_reports_missing_auth_as_incomplete(tmp_path):
    from scripts import agent_ownership

    env_dir = tmp_path / "envs" / "dev"
    env_dir.mkdir(parents=True)
    with pytest.raises(agent_ownership.AgentError, match="fill in") as raised:
        agent_ownership._api(env_dir)
    assert "is not valid HCL" not in str(raised.value)
