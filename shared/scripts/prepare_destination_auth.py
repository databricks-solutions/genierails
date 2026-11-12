#!/usr/bin/env python3
"""Fill a promotion destination's blank auth.auto.tfvars from the source env's, when both use one SP.

make promote-to ENV=prod REUSE_SOURCE_AUTH=1 WORKSPACE_HOST=<prod host> WORKSPACE_ID=<prod id>
runs this right after the destination env is scaffolded, inside the promotion
transaction (a refused promotion restores the file with everything else).

It copies only the account-level credentials (databricks_account_id,
databricks_client_id, databricks_client_secret and, when set,
databricks_account_host). The workspace host and ID are always the
destination's: kept from the destination file when already filled in there,
otherwise WORKSPACE_HOST/WORKSPACE_ID, and refused when they are missing, differ
from what the file already holds, or name the source workspace.

It writes only a destination still in its template state: no keys beyond the
template's and no account ID, client ID or secret. Anything else is left
unchanged, as is a symlink. When the destination's committed
deployer_principal_id is set, it must equal the source client ID (the
same-SP claim). No credential value is ever printed: parse failures report
a fixed "is not valid HCL", and nothing chains the parser's exception.

Both files are opened with O_NOFOLLOW and must be regular files; a symlinked
or non-regular source is refused. Just before the replace, the destination is
re-read and must be the same file (device, inode, size, mtime) with the same
bytes, or nothing is written. (The replace itself is a rename, so a swap in the
instant between that check and the rename is the one window left.)

make runs this (and passes --reuse) only when REUSE_SOURCE_AUTH has origin
"command line", so a stale REUSE_SOURCE_AUTH in the shell never reuses
credentials. An exported MAKEFLAGS carrying REUSE_SOURCE_AUTH=1 counts as a
command-line opt-in too: make reports the same origin and value for both and
cannot tell them apart. The opt-in is a guard against accidental reuse, not a
security boundary: whoever controls this process's environment can already
read the source auth file.

  prepare_destination_auth.py --source <src auth> --dest <dest auth> [--dest-env-file <dest env.auto.tfvars>]
      --reuse 1 [--workspace-host <host>] [--workspace-id <id>]
"""

from __future__ import annotations

import argparse
import errno
import os
import re
import stat
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from auth_tfvars import template_text  # noqa: E402
from hcl2_guard import import_hcl2  # noqa: E402

TEMPLATE_KEYS = ("databricks_account_id", "databricks_client_id", "databricks_client_secret",
                 "databricks_workspace_id", "databricks_workspace_host")
CREDENTIAL_KEYS = ("databricks_account_id", "databricks_client_id", "databricks_client_secret")
REUSED_KEYS = (*CREDENTIAL_KEYS, "databricks_account_host")
HOST, WORKSPACE_ID = "databricks_workspace_host", "databricks_workspace_id"
# make setup in azure/ adds databricks_account_host to the template (auth_tfvars.py). A
# destination holding that key is still template state only when it is byte-for-byte that file.
AZURE_TEMPLATE = template_text(Path(__file__).resolve().parents[1] / "auth.auto.tfvars.example", "azure")
PLACEHOLDERS = ("placeholder", "change_me", "changeme", "your_")


class AuthError(Exception):
    pass


class NotPlainFile(Exception):
    pass


def _read_plain(path: Path) -> tuple[bytes, os.stat_result]:
    """The bytes and stat of path, read through one O_NOFOLLOW descriptor; NotPlainFile if not a regular file."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as exc:
        if exc.errno not in (errno.ELOOP, errno.EMLINK, errno.ENOENT):
            raise
        fd = None
    if fd is None:
        raise NotPlainFile(str(path))
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise NotPlainFile(str(path))
        chunks = []
        while chunk := os.read(fd, 65536):
            chunks.append(chunk)
        return b"".join(chunks), info
    finally:
        os.close(fd)


def _identity(info: os.stat_result) -> tuple:
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


def _parse(data: bytes, path: Path) -> dict:
    try:
        hcl2 = import_hcl2()
        value = hcl2.loads(data.decode())
    except Exception:
        value = None
    if not isinstance(value, dict):
        # Raised outside the handler with no parser text, which can echo a
        # malformed secret (also via __context__).
        raise AuthError(f"{path} is not valid HCL; nothing was written.") from None
    return value


def _load(path: Path) -> dict:
    try:
        hcl2 = import_hcl2()
        value = hcl2.loads(path.read_text())
    except Exception:
        value = None
    if not isinstance(value, dict):
        # Raised outside the handler with no parser text, which can echo a
        # malformed secret (also via __context__).
        raise AuthError(f"{path} is not valid HCL; nothing was written.") from None
    return value


def _text(config: dict, key: str) -> str:
    value = config.get(key)
    return value.strip() if isinstance(value, str) else ""


def _real(value: str) -> bool:
    normalized = value.lower().replace("-", "_").replace(" ", "_")
    return bool(value) and "<" not in value and ">" not in value and not any(p in normalized for p in PLACEHOLDERS)


def _same_host(a: str, b: str) -> bool:
    def norm(host: str) -> str:
        return re.sub(r"^https?://", "", host.strip().lower()).rstrip("/")
    return norm(a) == norm(b)


def _hcl_string(value: str) -> str:
    # python-hcl2 keeps $${ and %%{ escapes literally while Terraform unescapes
    # them, so a value with a template sequence or a control character is refused.
    if "${" in value or "%{" in value or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise AuthError("a reused credential holds characters this tool will not write; "
                        "fill the destination auth.auto.tfvars by hand.")
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _render(text: str, values: dict[str, str]) -> str:
    lines = text.splitlines()
    for key, value in values.items():
        line = f"{key} = {_hcl_string(value)}"
        pattern = re.compile(rf"^(\s*){re.escape(key)}(\s*)=.*$")
        for index, existing in enumerate(lines):
            match = pattern.match(existing)
            if match:
                lines[index] = f"{match.group(1)}{key}{match.group(2)}= {_hcl_string(value)}"
                break
        else:
            lines.append(line)
    return "\n".join(lines) + "\n"


def _unchanged(path: Path, data: bytes, identity: tuple) -> bool:
    """path is still the very file (and bytes) that was validated."""
    try:
        now, info = _read_plain(path)
    except (NotPlainFile, OSError):
        return False
    return _identity(info) == identity and now == data


def _write(path: Path, text: str, validated: bytes, identity: tuple) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".auth.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp, 0o600)
        if not _unchanged(path, validated, identity):
            raise AuthError(f"{path} changed while it was being prepared; nothing was written.")
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def prepare(source: Path, dest: Path, dest_env_file: Path | None, host: str, workspace_id: str) -> str:
    """Fill dest from source; return the line to print. Raises AuthError on refusal."""
    try:
        dest_data, dest_info = _read_plain(dest)
    except NotPlainFile:
        return (f"  REUSE_SOURCE_AUTH: {dest} is not a plain file (a symlink or missing); left unchanged.")
    dest_config = _parse(dest_data, dest)
    extra = sorted(set(dest_config) - set(TEMPLATE_KEYS))
    if extra == ["databricks_account_host"] and dest_data == AZURE_TEMPLATE:
        extra = []
    if extra or any(_text(dest_config, key) for key in CREDENTIAL_KEYS):
        return (f"  REUSE_SOURCE_AUTH: {dest} already holds credentials or settings; left unchanged.")

    try:
        source_data, _ = _read_plain(source)
    except NotPlainFile:
        source_data = None
    if source_data is None:
        raise AuthError(f"{source} is not a regular file (a symlink, missing or special); nothing was written.")
    source_config = _parse(source_data, source)
    if not all(_real(_text(source_config, key)) for key in ("databricks_client_id", "databricks_client_secret")):
        raise AuthError(f"{source} has no client ID and secret to reuse; nothing was written.")
    client_id = _text(source_config, "databricks_client_id")

    if dest_env_file is not None and dest_env_file.is_file():
        deployer = _text(_load(dest_env_file), "deployer_principal_id")
        if _real(deployer) and deployer != client_id:
            raise AuthError(f"{dest_env_file} deployer_principal_id is not the source's databricks_client_id, "
                            "so the envs do not share one service principal; fill the destination "
                            "auth.auto.tfvars by hand. Nothing was written.")

    resolved = {}
    for key, given, flag in ((HOST, host.strip(), "WORKSPACE_HOST"), (WORKSPACE_ID, workspace_id.strip(), "WORKSPACE_ID")):
        kept = _text(dest_config, key)
        if kept and not _real(kept):
            kept = ""
        same = _same_host if key == HOST else (lambda a, b: a == b)
        if kept and given and not same(kept, given):
            raise AuthError(f"{flag} differs from {key} already in {dest}; nothing was written.")
        value = kept or given
        if not value:
            raise AuthError(f"REUSE_SOURCE_AUTH=1 needs the destination's {key}: pass {flag}=<value> "
                            f"(or fill it in {dest}). Nothing was written.")
        if same(value, _text(source_config, key)):
            raise AuthError(f"the destination {key} is the source env's; pass the destination workspace's. "
                            "Nothing was written.")
        resolved[key] = value
    if not re.fullmatch(r"https://[^\s\"\\]+", resolved[HOST]):
        raise AuthError(f"{HOST} must be an https:// workspace URL; nothing was written.")
    if not re.fullmatch(r"[0-9]+", resolved[WORKSPACE_ID]):
        raise AuthError(f"{WORKSPACE_ID} must be the numeric workspace ID; nothing was written.")

    values = {key: _text(source_config, key) for key in REUSED_KEYS if _text(source_config, key)}
    values.update(resolved)
    rendered = _render(dest_data.decode(), values)
    try:
        hcl2 = import_hcl2()
        check = hcl2.loads(rendered)
    except Exception:
        check = None
    if not isinstance(check, dict) or any(check.get(key) != value for key, value in values.items()):
        raise AuthError("the reused credentials do not round-trip as HCL; fill the destination "
                        "auth.auto.tfvars by hand. Nothing was written.") from None
    _write(dest, rendered, dest_data, _identity(dest_info))
    reused = ", ".join(key.removeprefix("databricks_") for key in values if key in REUSED_KEYS)
    return (f"  REUSE_SOURCE_AUTH: filled {dest} from {source} ({reused}); "
            f"workspace host {resolved[HOST]} and ID {resolved[WORKSPACE_ID]} are the destination's.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--dest", required=True, type=Path)
    parser.add_argument("--dest-env-file", type=Path)
    parser.add_argument("--reuse", default="")
    parser.add_argument("--workspace-host", default="")
    parser.add_argument("--workspace-id", default="")
    args = parser.parse_args(argv)
    if args.reuse != "1":
        flags = "WORKSPACE_HOST/WORKSPACE_ID only apply with REUSE_SOURCE_AUTH=1" if not args.reuse \
            else "REUSE_SOURCE_AUTH takes only 1"
        print(f"ERROR: {flags}; nothing was written.", file=sys.stderr)
        return 1
    message = None
    try:
        message = prepare(args.source, args.dest, args.dest_env_file, args.workspace_host, args.workspace_id)
    except AuthError as exc:
        error = str(exc)
    except Exception:
        # Any other failure: a fixed message, never a traceback (locals can hold the secret).
        error = "could not prepare the destination auth.auto.tfvars; nothing was written."
    if message is None:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
