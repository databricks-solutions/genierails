from __future__ import annotations

import os
import subprocess
import sys
import types
from pathlib import Path

import pytest

from scripts import hcl2_guard

ROOT = Path(__file__).resolve().parents[2]
SHARED = ROOT / "shared"
GUARD_LINE = (
    "ERROR: installed python-hcl2 8.1.4 is unsupported; "
    "fix with: pip install 'python-hcl2>=7.3.1,<8'"
)


def _fake_hcl2(root: Path, version: str, *, metadata: bool = True, module_version: str | None = None) -> Path:
    site = root / "fake-site"
    site.mkdir()
    (site / "hcl2.py").write_text(
        f"__version__ = {module_version if module_version is not None else version!r}\n"
        "def load(handle):\n"
        "    return {}\n"
        "def loads(text):\n"
        "    return {}\n"
    )
    if metadata:
        dist = site / f"python_hcl2-{version}.dist-info"
        dist.mkdir()
        (dist / "METADATA").write_text(f"Name: python-hcl2\nVersion: {version}\n")
    return site


@pytest.mark.parametrize(
    "version",
    ["8.0.0", "8.1.4", "8.0.0rc1", "8.0.0.dev3", "10.0"],
)
def test_guard_refuses_unsupported_major_versions(monkeypatch, version):
    monkeypatch.setattr(hcl2_guard.importlib.metadata, "version", lambda _name: version)
    with pytest.raises(hcl2_guard.Hcl2VersionError) as raised:
        hcl2_guard.assert_supported_hcl2()
    assert str(raised.value) == (
        f"ERROR: installed python-hcl2 {version} is unsupported; "
        "fix with: pip install 'python-hcl2>=7.3.1,<8'"
    )


@pytest.mark.parametrize("version", ["7.3.1", "7.99.0.dev0"])
def test_guard_allows_supported_major_versions(monkeypatch, version):
    monkeypatch.setattr(hcl2_guard.importlib.metadata, "version", lambda _name: version)
    hcl2_guard.assert_supported_hcl2()


def test_guard_falls_back_to_module_version_when_metadata_is_missing(monkeypatch):
    def missing(_name: str):
        raise hcl2_guard.importlib.metadata.PackageNotFoundError

    monkeypatch.setattr(hcl2_guard.importlib.metadata, "version", missing)
    monkeypatch.setitem(sys.modules, "hcl2", types.SimpleNamespace(__version__="8.1.4"))
    with pytest.raises(hcl2_guard.Hcl2VersionError) as raised:
        hcl2_guard.assert_supported_hcl2()
    assert str(raised.value) == GUARD_LINE


def test_guard_falls_back_to_module_version_when_metadata_is_empty(monkeypatch):
    monkeypatch.setattr(hcl2_guard.importlib.metadata, "version", lambda _name: "")
    monkeypatch.setitem(sys.modules, "hcl2", types.SimpleNamespace(__version__="7.3.1"))
    hcl2_guard.assert_supported_hcl2()


def test_auth_configured_cli_uses_dedicated_hcl2_version_exit_code(tmp_path):
    fake_site = _fake_hcl2(tmp_path, "8.1.4")
    auth = tmp_path / "auth.auto.tfvars"
    auth.write_text('databricks_workspace_host = "https://dbc.example"\n')
    result = subprocess.run(
        [sys.executable, str(SHARED / "scripts" / "auth_configured.py"), str(auth)],
        text=True,
        capture_output=True,
        env=os.environ | {"PYTHONPATH": str(fake_site)},
        timeout=30,
    )
    assert result.returncode == 4
    assert result.stdout == ""
    assert result.stderr == GUARD_LINE + "\n"


def test_make_capture_under_hcl2_8_prints_guard_once_without_false_hcl_error(tmp_path):
    fake_site = _fake_hcl2(tmp_path, "8.1.4")
    cloud = tmp_path / "cloud"
    env_dir = cloud / "envs" / "dev"
    env_dir.mkdir(parents=True)
    (env_dir / "auth.auto.tfvars").write_text(
        'databricks_workspace_host = "https://dbc.example"\n'
        'databricks_client_id = "client"\n'
        'databricks_client_secret = "secret"\n'
    )

    result = subprocess.run(
        [
            "make",
            "--no-print-directory",
            "capture",
            "ENV=dev",
            "SPACE=Payments",
            f"CLOUD_ROOT={cloud}",
            f"SHARED_ROOT={SHARED}",
        ],
        cwd=ROOT / "aws",
        text=True,
        capture_output=True,
        env=os.environ | {"PYTHONPATH": str(fake_site), "GIT_CONFIG_GLOBAL": os.devnull},
        timeout=60,
    )
    combined = result.stdout + result.stderr
    assert result.returncode != 0
    assert combined.count(GUARD_LINE) == 1
    assert "not valid HCL" not in combined
    assert "fill in" not in combined


@pytest.mark.parametrize(
    "script",
    [SHARED / "scripts" / "import_existing.sh", SHARED / "scripts" / "genie_space.sh"],
    ids=lambda path: path.name,
)
def test_direct_shell_entrypoints_refuse_hcl2_8_with_one_guard_line(tmp_path, script):
    fake_site = _fake_hcl2(tmp_path, "8.1.4")
    result = subprocess.run(
        ["bash", str(script), "--help"],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        env=os.environ | {"PYTHONPATH": str(fake_site)},
        timeout=30,
    )
    combined = result.stdout + result.stderr
    assert result.returncode != 0
    assert combined == GUARD_LINE + "\n"


def test_guarded_bootstrap_checks_hcl2_before_governance_mode_one_liner():
    makefile = (SHARED / "Makefile.shared").read_text()
    recipe = makefile.split("\n_guarded-bootstrap:\n", 1)[1].split("\n\n", 1)[0]
    assert "assert_supported_hcl2" in recipe
    assert recipe.index("assert_supported_hcl2") < recipe.index("_GOVERNANCE_MODE_OF")
