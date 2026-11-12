#!/usr/bin/env python3
"""Exit successfully when workspace OAuth M2M credentials are non-placeholder values."""

import importlib
import sys
from enum import Enum
from pathlib import Path

try:
    from hcl2_guard import Hcl2VersionError, import_hcl2
except ModuleNotFoundError:  # imported as scripts.auth_configured
    _hcl2_guard = importlib.import_module("scripts.hcl2_guard")
    Hcl2VersionError = _hcl2_guard.Hcl2VersionError
    import_hcl2 = _hcl2_guard.import_hcl2

HCL2_VERSION_ERROR_EXIT = 4


class AuthStatus(Enum):
    CONFIGURED = "configured"
    INCOMPLETE = "incomplete"
    MALFORMED = "malformed"
    UNREADABLE = "unreadable"


def _configured(config: dict, name: str) -> bool:
    raw = config.get(name, "")
    if not isinstance(raw, str):
        return False
    value = raw.strip()
    normalized = value.lower().replace("-", "_").replace(" ", "_")
    placeholders = ("placeholder", "change_me", "changeme", "your_client_id",
                    "your_client_secret", "your_workspace_host")
    return bool(value) and "<" not in value and ">" not in value and not any(
        marker in normalized for marker in placeholders
    )


def auth_status(path: Path) -> AuthStatus:
    try:
        hcl2 = import_hcl2()
        with path.open() as handle:
            config = hcl2.load(handle)
    except Hcl2VersionError:
        raise
    except FileNotFoundError:
        return AuthStatus.INCOMPLETE
    except OSError:
        return AuthStatus.UNREADABLE
    except UnicodeError:
        return AuthStatus.MALFORMED
    except Exception:
        # Never let parser text (which can echo credential fragments) propagate.
        return AuthStatus.MALFORMED

    return AuthStatus.CONFIGURED if all(_configured(config, name) for name in (
        "databricks_workspace_host", "databricks_client_id", "databricks_client_secret"
    )) else AuthStatus.INCOMPLETE


def auth_configured(path: Path) -> bool:
    """Compatibility predicate for callers that only need yes/no."""
    return auth_status(path) is AuthStatus.CONFIGURED


def workspace_client(auth_file: Path, **kwargs):
    """Return an OAuth-M2M client pinned only to ``auth_file``."""
    path = Path(auth_file)
    status = auth_status(path)
    if status is AuthStatus.UNREADABLE:
        raise RuntimeError(f"{path} can't be read") from None
    if status is AuthStatus.MALFORMED:
        raise RuntimeError(f"{path} is not valid HCL") from None
    if status is AuthStatus.INCOMPLETE:
        raise RuntimeError(f"fill in {path} (workspace host, client ID, and client secret)") from None
    try:
        hcl2 = import_hcl2()
        with path.open() as handle:
            config = hcl2.load(handle)
    except Hcl2VersionError:
        raise
    except OSError:
        raise RuntimeError(f"{path} can't be read") from None
    except UnicodeError:
        raise RuntimeError(f"{path} is not valid HCL") from None
    except Exception:
        raise RuntimeError(f"{path} is not valid HCL") from None
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient(config=oauth_config(config, **kwargs))


def oauth_config(config: dict, **kwargs):
    """Build an OAuth-M2M SDK Config from already validated auth values."""
    required = ("databricks_workspace_host", "databricks_client_id",
                "databricks_client_secret")
    if not isinstance(config, dict) or not all(_configured(config, name) for name in required):
        raise RuntimeError(
            "fill in auth.auto.tfvars (workspace host, client ID, and client secret)"
        ) from None
    from databricks.sdk.config import Config
    return Config(
        host=config["databricks_workspace_host"].strip(),
        client_id=config["databricks_client_id"].strip(),
        client_secret=config["databricks_client_secret"].strip(),
        auth_type="oauth-m2m",
        **kwargs,
    )


if __name__ == "__main__":
    try:
        status = auth_status(Path(sys.argv[1])) if len(sys.argv) == 2 else AuthStatus.INCOMPLETE
    except Hcl2VersionError as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(HCL2_VERSION_ERROR_EXIT) from None
    raise SystemExit({AuthStatus.CONFIGURED: 0, AuthStatus.INCOMPLETE: 1,
                      AuthStatus.MALFORMED: 2, AuthStatus.UNREADABLE: 3}[status])
