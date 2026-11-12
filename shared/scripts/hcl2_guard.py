"""Guard python-hcl2 versions before parsing user tfvars files."""
from __future__ import annotations

import importlib.metadata
import re

PACKAGE = "python-hcl2"
PIN = "python-hcl2>=7.3.1,<8"


class Hcl2VersionError(SystemExit):
    """The installed python-hcl2 version is known to parse tfvars incompatibly."""


def _major(version: str) -> int | None:
    match = re.match(r"\s*(\d+)", version)
    return int(match.group(1)) if match else None


def assert_supported_hcl2() -> None:
    """Refuse python-hcl2 8.x+, whose tfvars string output differs from 7.x."""
    try:
        version = importlib.metadata.version(PACKAGE)
    except importlib.metadata.PackageNotFoundError:
        return
    major = _major(version)
    if major is not None and major >= 8:
        raise Hcl2VersionError(
            f"ERROR: installed python-hcl2 {version} is unsupported; "
            f"fix with: pip install '{PIN}'"
        ) from None


def import_hcl2():
    """Return hcl2 after refusing unsupported python-hcl2 versions."""
    assert_supported_hcl2()
    import hcl2

    return hcl2
