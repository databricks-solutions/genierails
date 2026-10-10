#!/usr/bin/env python3
"""Initialize the deployment HMAC key and create its UC secret if absent."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import stat
from pathlib import Path


def init_key(path: Path) -> None:
    if path.exists():
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode != 0o600:
            raise SystemExit(f"Refusing key file with mode {mode:o}; expected 600: {path}")
        print(f"Hash key already exists at {path} (value not displayed).")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write(secrets.token_hex(32) + "\n")
    print(f"Created 0600 hash key file at {path} (value not displayed).")


def ensure_uc_secret(client, catalog: str, schema: str, generated_dir: Path) -> str:
    """Create hmac_key only when absent; never read its value from UC."""
    from databricks.sdk.errors import NotFound

    name = f"{catalog}.{schema}.hmac_key"
    try:
        client.api_client.do("GET", f"/api/2.1/unity-catalog/secrets/{name}")
        created = False
    except Exception as exc:
        if not isinstance(exc, NotFound):
            raise
        # TODO(step 5): production provisioning and cross-environment probe
        # comparison are wired when deterministic masks are deployed.
        key = os.environ.get("GENIERAILS_HASH_KEY")
        if not key:
            raise RuntimeError(
                f"UC secret {name} is absent; set GENIERAILS_HASH_KEY or set hash_fallback=redact"
            ) from None
        if not all(ch in "0123456789abcdefABCDEF" for ch in key) or len(key) != 64:
            raise RuntimeError("GENIERAILS_HASH_KEY must be exactly 32 random bytes encoded as 64 hex characters")
        client.api_client.do(
            "POST", "/api/2.1/unity-catalog/secrets",
            body={"catalog_name": catalog, "schema_name": schema, "name": "hmac_key", "value": key},
        )
        created = True
    # The probe proves key equality without persisting or printing the key. It
    # is computed by the caller from the SQL UDF after creation.
    generated_dir.mkdir(parents=True, exist_ok=True)
    return "created" if created else "present"


def write_probe(path: Path, digest: str) -> None:
    if not len(digest) == 64 or any(ch not in "0123456789abcdef" for ch in digest):
        raise ValueError("probe must be a lowercase SHA-256 hex digest")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"input": "genierails-probe", "hmac_sha256": digest}, indent=2) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--init", type=Path)
    args = parser.parse_args()
    if args.init:
        init_key(args.init)
        return 0
    parser.error("choose --init FILE")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
