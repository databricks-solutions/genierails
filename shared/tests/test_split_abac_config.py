"""Tests for splitting generated ABAC configuration into ownership layers."""

import os
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from split_abac_config import (
    build_account_config,
    build_data_access_config,
    build_workspace_config,
    load_generated_config,
)


def test_same_environment_split_keeps_tag_assignments():
    generated = {
        "groups": {"analysts": {"description": "Data analysts"}},
        "tag_policies": [{"key": "sensitivity", "values": ["pii"]}],
        "tag_assignments": [
            {
                "entity_type": "columns",
                "entity_name": "dev_fin.finance.accounts.ssn",
                "tag_key": "sensitivity",
                "tag_value": "pii",
            }
        ],
        "fgac_policies": [{"name": "mask_pii", "catalog": "dev_fin"}],
    }

    account = build_account_config(generated, None)
    data_access = build_data_access_config(generated)

    assert account["tag_policies"][0]["key"] == "sensitivity"
    assert account["tag_policies"][0]["values"] == ["pii"]
    assert data_access["fgac_policies"] == generated["fgac_policies"]
    assert data_access["groups"] == generated["groups"]
    assert data_access["tag_assignments"] == generated["tag_assignments"]


def test_data_access_split_derives_only_agent_acl_map_from_workspace_config():
    generated = {
        "genie_space_configs": {
            "Agent A": {"title": "A", "acl_groups": ["a_group"]},
            "Agent B": {"title": "B", "acl_groups": ["b_group", "shared_group"]},
        },
        "genie_space_derived_acl_groups": {
            "Agent A": ["derived_a"],
            "Agent B": ["derived_b"],
        },
        "discovered_uc_tables": ["cat.schema.table"],
        "discovered_table_agents": {"cat.schema.table": ["Agent A"]},
    }

    data_access = build_data_access_config(generated)

    assert data_access == {
        "genie_space_acl_groups": {
            "Agent A": ["derived_a"],
            "Agent B": ["derived_b"],
        }
    }


def test_multi_space_model_legacy_acl_is_not_authoritative():
    generated = {
        "genie_space_configs": {
            "payments": {"genie_acl_groups": ["payments_group"]},
            "hr": {"genie_acl_groups": ["hr_group"]},
        }
    }
    with pytest.raises(ValueError, match="no resolved ACL"):
        build_data_access_config(generated)


def test_legacy_single_space_conversion_preserves_authored_acl():
    generated = {
        "genie_space_title": "Payments",
        "genie_acl_groups": ["shared_group"],
        "genie_space_legacy_mode": True,
    }
    workspace = build_workspace_config(generated)
    assert workspace["genie_space_configs"]["Payments"]["acl_groups"] == [
        "shared_group"
    ]
    assert build_data_access_config(generated)["genie_space_acl_groups"] == {
        "Payments": ["shared_group"],
    }


def test_legacy_single_space_conversion_preserves_explicit_empty_acl():
    generated = {
        "genie_space_title": "Nobody",
        "genie_acl_groups": [],
        "genie_space_legacy_mode": True,
    }
    assert build_workspace_config(generated)["genie_space_configs"]["Nobody"][
        "acl_groups"
    ] == []
    assert build_data_access_config(generated)["genie_space_acl_groups"] == {
        "Nobody": []
    }


def test_workspace_and_data_access_consumers_resolve_identical_acl_precedence():
    generated = {
        "genie_space_configs": {
            "canonical": {"acl_groups": []},
            "legacy": {"genie_acl_groups": ["legacy_group"]},
            "derived": {"title": "Derived"},
        },
        "genie_space_derived_acl_groups": {
            "canonical": ["must_not_widen"],
            "legacy": ["must_not_override"],
            "derived": ["derived_group"],
        },
    }

    data_access = build_data_access_config(generated)["genie_space_acl_groups"]
    workspace = build_workspace_config(generated)

    assert data_access == {
        "canonical": ["must_not_widen"],
        "legacy": ["must_not_override"],
        "derived": ["derived_group"],
    }
    assert {
        name: config["acl_groups"]
        for name, config in workspace["genie_space_configs"].items()
    } == data_access


def test_split_loads_tool_owned_acl_sidecar_for_both_consumers(tmp_path):
    source = tmp_path / "abac.auto.tfvars"
    source.write_text('genie_space_configs = { Pay = { title = "Pay" } }\n')
    (tmp_path / "genie_space_derived_acl_groups.auto.tfvars").write_text(
        'genie_space_derived_acl_groups = { Pay = ["pay_group"] }\n'
    )

    loaded = load_generated_config(source)
    data_access = build_data_access_config(loaded)["genie_space_acl_groups"]
    workspace = build_workspace_config(loaded)["genie_space_configs"]

    assert data_access == {"Pay": ["pay_group"]}
    assert workspace["Pay"]["acl_groups"] == data_access["Pay"]


def test_missing_sidecar_entry_fails_closed_for_both_consumers():
    generated = {"genie_space_configs": {"Pay": {"title": "Pay"}}}

    with pytest.raises(ValueError, match="Run `make generate`.*set acl_groups"):
        build_data_access_config(generated)
    with pytest.raises(ValueError, match="Run `make generate`.*set acl_groups"):
        build_workspace_config(generated)


@pytest.mark.parametrize("model_acl", [None, "${var.groups}", ["ok", 7], []])
def test_model_acl_is_ignored_and_missing_sidecar_fails_closed(model_acl):
    generated = {
        "genie_space_configs": {"Pay": {"acl_groups": model_acl}}
    }

    with pytest.raises(ValueError, match="no resolved ACL"):
        build_data_access_config(generated)
    with pytest.raises(ValueError, match="no resolved ACL"):
        build_workspace_config(generated)


# ── deterministic gr_treatment values (live AWS Phase A, N3) ────────────────

LEGACY_ACCOUNT = {"tag_policies": [{"key": "gr_treatment", "values": ["account_last4", "redact"]}]}
DETERMINISTIC = {"tag_policies": [{"key": "gr_treatment", "values": [
    "card_last4__string__951e00ac", "unmapped_redact__string__0123abcd"]}]}


@pytest.mark.parametrize("existing", [None, LEGACY_ACCOUNT, DETERMINISTIC])
def test_split_merges_deterministic_treatment_values_into_the_account_tag_policy(existing):
    account = build_account_config(DETERMINISTIC, existing)

    values = account["tag_policies"][0]["values"]
    legacy = ["account_last4", "redact"] if existing is LEGACY_ACCOUNT else []
    assert values == legacy + ["card_last4__string__951e00ac", "unmapped_redact__string__0123abcd"]


def test_split_keeps_legacy_values_from_an_existing_account_policy_unchanged():
    account = build_account_config(LEGACY_ACCOUNT, DETERMINISTIC)

    assert account["tag_policies"][0]["values"] == [
        "card_last4__string__951e00ac", "unmapped_redact__string__0123abcd", "account_last4", "redact"]


@pytest.mark.parametrize("value", [
    "not_a_treatment",                   # legacy-shaped, not in the registry
    "not_a_treatment__string__951e00ac", # deterministic-shaped, no such library treatment
    "card_last4__string__951e00a",       # malformed generation
])
@pytest.mark.parametrize("side", ["new", "existing"])
def test_split_still_refuses_an_unknown_treatment_value(value, side):
    bad = {"tag_policies": [{"key": "gr_treatment", "values": [value]}]}
    new, existing = (bad, LEGACY_ACCOUNT) if side == "new" else (LEGACY_ACCOUNT, bad)

    with pytest.raises(ValueError, match="unknown canonical value"):
        build_account_config(new, existing)


def test_split_refuses_a_deterministic_shaped_value_on_another_registry_key():
    bad = {"tag_policies": [{"key": "pii_level", "values": ["card_last4__string__951e00ac"]}]}

    with pytest.raises(ValueError, match="unknown canonical value"):
        build_account_config(bad, None)


def test_account_only_writes_exactly_the_full_splits_account_file_and_nothing_else(tmp_path, monkeypatch):
    """rehearse's and release's account step plans from it before the env's own split runs."""
    import subprocess

    monkeypatch.delenv("GENIERAILS_STATE_DIR", raising=False)

    script = Path(__file__).resolve().parent.parent / "scripts" / "split_abac_config.py"
    draft = tmp_path / "abac.auto.tfvars"
    draft.write_text('groups = {\n  analysts = { description = "A" }\n}\n'
                     'tag_policies = [\n  { key = "gr_treatment", description = "", values = ["card_last4__string__951e00ac"] },\n]\n'
                     'tag_assignments = [\n]\nfgac_policies = [\n]\n')
    existing = '# kept\ngroups = {\n  viewers = { description = "" }\n}\n'
    for name in ("only", "full"):
        (tmp_path / name).mkdir()
        (tmp_path / name / "account.tfvars").write_text(existing)
    only = subprocess.run([sys.executable, str(script), "--account-only", str(draft),
                           str(tmp_path / "only" / "account.tfvars")], text=True, capture_output=True)
    assert only.returncode == 0, only.stdout + only.stderr
    assert only.stdout == f"  Wrote shared account config: {tmp_path / 'only' / 'account.tfvars'}\n"
    full = subprocess.run([sys.executable, str(script), str(draft), str(tmp_path / "full" / "account.tfvars"),
                           str(tmp_path / "full" / "da.tfvars"), str(tmp_path / "full" / "ws.tfvars")],
                          text=True, capture_output=True)
    assert full.returncode == 0, full.stdout + full.stderr
    assert (tmp_path / "only" / "account.tfvars").read_bytes() == (tmp_path / "full" / "account.tfvars").read_bytes()
    # Only the account file (and the account lock's mutex file, generated/.governance.guard, ignored).
    assert sorted(p.name for p in (tmp_path / "only").iterdir()) == ["account.tfvars", "generated"]
    assert [p.name for p in (tmp_path / "only" / "generated").iterdir()] == [".governance.guard"]
    # Re-running it (the env's split under its lock, after the step) changes no byte.
    before = (tmp_path / "only" / "account.tfvars").read_bytes()
    subprocess.run([sys.executable, str(script), str(draft), str(tmp_path / "only" / "account.tfvars"),
                    str(tmp_path / "only" / "da.tfvars"), str(tmp_path / "only" / "ws.tfvars")], check=True,
                   capture_output=True)
    assert (tmp_path / "only" / "account.tfvars").read_bytes() == before


# ── the shared account file: one locked, atomic merge per env (PR #297 r2) ────

SPLIT = Path(__file__).resolve().parent.parent / "scripts" / "split_abac_config.py"


def _account_env(tmp_path, monkeypatch):
    monkeypatch.delenv("GENIERAILS_STATE_DIR", raising=False)
    account = tmp_path / "aws" / "envs" / "account"
    account.mkdir(parents=True)
    return account / "abac.auto.tfvars"


def _draft(tmp_path, env, group):
    path = tmp_path / "aws" / "envs" / env / "generated" / "abac.auto.tfvars"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f'groups = {{\n  {group} = {{ description = "" }}\n}}\n')
    return path


def _groups(path):
    import hcl2

    return sorted(hcl2.loads(path.read_text()).get("groups") or {})


def test_concurrent_merges_for_different_envs_lose_no_part(tmp_path, monkeypatch):
    """Codex r1 B1: two unlocked read-merge-writes; the second one's read missed the first one's part."""
    import threading
    import time

    import split_abac_config as split

    account = _account_env(tmp_path, monkeypatch)
    real = split.build_account_config

    def slow(full_cfg, existing):  # read, then take a while before writing
        merged = real(full_cfg, existing)
        time.sleep(0.4)
        return merged

    monkeypatch.setattr(split, "build_account_config", slow)
    threads = [threading.Thread(target=split.merge_account_file, args=({"groups": {g: {}}}, account))
               for g in ("dev_analysts", "prod_auditors")]
    threads[0].start()
    time.sleep(0.1)
    threads[1].start()
    for thread in threads:
        thread.join()
    assert _groups(account) == ["dev_analysts", "prod_auditors"]


def test_account_only_pre_writes_of_two_envs_wait_for_the_mutex_and_both_land(tmp_path, monkeypatch):
    import subprocess
    import time

    from scripts import environment_lock

    account = _account_env(tmp_path, monkeypatch)
    drafts = [_draft(tmp_path, "dev", "dev_analysts"), _draft(tmp_path, "prod", "prod_auditors")]
    with environment_lock.mutex(account.parent):
        runs = [subprocess.Popen([sys.executable, str(SPLIT), "--account-only", str(d), str(account)],
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) for d in drafts]
        time.sleep(1.0)
        assert all(run.poll() is None for run in runs)  # both wait for the mutex
        assert not account.exists()
    for run in runs:
        out, _ = run.communicate(timeout=60)
        assert run.returncode == 0, out
    assert _groups(account) == ["dev_analysts", "prod_auditors"]
    assert sorted(p.name for p in account.parent.iterdir()) == ["abac.auto.tfvars", "generated"]  # no temp file


@pytest.mark.parametrize("mode", ["account-only", "full"])
@pytest.mark.parametrize("shape", ["file-symlink", "dangling-symlink", "dir-symlink", "not-a-file"])
def test_the_account_file_is_never_written_through_a_link_or_into_a_non_file(tmp_path, monkeypatch, mode, shape):
    """Codex r1 B2: a git-ignored symlinked envs/account/abac.auto.tfvars; write_text followed it."""
    import subprocess

    account = _account_env(tmp_path, monkeypatch)
    target = tmp_path / "elsewhere" / "abac.auto.tfvars"
    target.parent.mkdir()
    target.write_text("# not GenieRails'\n")
    if shape == "file-symlink":
        account.symlink_to(target)
    elif shape == "dangling-symlink":
        account.symlink_to(tmp_path / "elsewhere" / "missing.tfvars")
    elif shape == "dir-symlink":
        account.parent.rmdir()
        account.parent.symlink_to(target.parent, target_is_directory=True)
    else:
        account.mkdir()
    dev = _draft(tmp_path, "dev", "dev_analysts")
    args = (["--account-only", str(dev), str(account)] if mode == "account-only"
            else [str(dev), str(account), str(dev.parent.parent / "da.tfvars"), str(dev.parent.parent / "ws.tfvars")])
    result = subprocess.run([sys.executable, str(SPLIT), *args], capture_output=True, text=True)
    assert result.returncode == 1, result.stdout + result.stderr
    assert "Nothing was written." in result.stdout
    assert target.read_text() == "# not GenieRails'\n"
    assert sorted(p.name for p in target.parent.iterdir()) == ["abac.auto.tfvars"]
    assert not (tmp_path / "elsewhere" / "missing.tfvars").exists()
    assert not (dev.parent.parent / "da.tfvars").exists() and not (dev.parent.parent / "ws.tfvars").exists()


def test_the_account_file_is_replaced_atomically_keeping_its_mode(tmp_path, monkeypatch):
    import os
    import split_abac_config as split

    account = _account_env(tmp_path, monkeypatch)
    account.write_text('groups = {\n  viewers = { description = "" }\n}\n')
    account.chmod(0o640)
    inode = account.stat().st_ino
    split.merge_account_file({"groups": {"analysts": {}}}, account)
    assert _groups(account) == ["analysts", "viewers"]
    assert account.stat().st_ino != inode and oct(account.stat().st_mode & 0o777) == oct(0o640)  # renamed into place
    assert not [p for p in account.parent.iterdir() if p.name.endswith(".tmp")]
    assert os.path.isfile(account) and not account.is_symlink()


# ── every writer of the account file respects the account lock (PR #297 r3) ──

def _full_split(dev, account, *extra):
    import subprocess

    layer = dev.parent.parent
    return subprocess.run([sys.executable, str(SPLIT), *extra, str(dev), str(account), str(layer / "da.tfvars"),
                           str(layer / "ws.tfvars")], capture_output=True, text=True)


def _holder(tmp_path):
    """A live process that holds the account lock, as another run's make recipe would."""
    import subprocess

    return subprocess.Popen(["sleep", "300"])


LOCKED = "the shared account layer is locked by another run (make rehearse, pid "


@pytest.mark.parametrize("mode", ["full", "account-only"])
def test_a_split_that_changes_the_account_file_is_refused_while_another_run_holds_the_account_lock(
        tmp_path, monkeypatch, mode):
    """Codex r2 (repro): acquire(account, holder_pid, 'rehearse'); a full split still wrote the account file."""
    import subprocess

    from scripts import environment_lock

    account = _account_env(tmp_path, monkeypatch)
    account.write_text('groups = {\n  viewers = { description = "" }\n}\n')
    before = account.read_bytes()
    dev = _draft(tmp_path, "dev", "dev_analysts")
    holder = _holder(tmp_path)
    try:
        ok, message, _ = environment_lock.acquire(account.parent, holder.pid, "rehearse")
        assert ok, message
        result = (_full_split(dev, account) if mode == "full" else subprocess.run(
            [sys.executable, str(SPLIT), "--account-only", str(dev), str(account)], capture_output=True, text=True))
        assert result.returncode == 1, result.stdout + result.stderr
        assert LOCKED + str(holder.pid) in result.stdout and "nothing was written" in result.stdout
        assert account.read_bytes() == before
        layer = dev.parent.parent
        assert not (layer / "da.tfvars").exists() and not (layer / "ws.tfvars").exists()
        assert environment_lock.owns_lock(account.parent, holder.pid)
    finally:
        holder.kill()
        holder.wait()


def test_the_lock_holder_writes_with_its_nonce_and_nothing_else_stands_in_for_it(tmp_path, monkeypatch):
    import os

    from scripts import environment_lock

    account = _account_env(tmp_path, monkeypatch)
    dev = _draft(tmp_path, "dev", "dev_analysts")
    ok, message, nonce = environment_lock.acquire(account.parent, os.getpid(), "rehearse")  # this process: an ancestor
    assert ok, message
    try:
        wrong = tmp_path / "wrong-nonce"
        wrong.write_text("0" * 64 + "\n")
        wrong.chmod(0o600)
        assert _full_split(dev, account, "--account-lock-nonce-file", str(wrong)).returncode == 1
        # Not inferable from the environment: the inherited nonce variable alone is no pass.
        monkeypatch.setenv(environment_lock.nonce_var(account.parent), nonce)
        refused = _full_split(dev, account)
        assert refused.returncode == 1 and "is locked by another run" in refused.stdout
        assert not account.exists()
        right = tmp_path / "nonce"
        right.write_text(nonce + "\n")
        right.chmod(0o600)
        written = _full_split(dev, account, "--account-lock-nonce-file", str(right))
        assert written.returncode == 0, written.stdout + written.stderr
        assert "Wrote shared account config" in written.stdout and _groups(account) == ["dev_analysts"]
    finally:
        environment_lock.release_lock(account.parent, os.getpid())


def test_an_unchanged_account_part_writes_nothing_and_needs_no_lock(tmp_path, monkeypatch):
    import split_abac_config as split
    from scripts import environment_lock

    account = _account_env(tmp_path, monkeypatch)
    dev = _draft(tmp_path, "dev", "dev_analysts")
    assert split.merge_account_file({"groups": {"dev_analysts": {"description": ""}}}, account) is True
    before, inode = account.read_bytes(), account.stat().st_ino
    holder = _holder(tmp_path)
    try:
        assert environment_lock.acquire(account.parent, holder.pid, "rehearse")[0]
        result = _full_split(dev, account)
        assert result.returncode == 0, result.stdout + result.stderr
        assert f"  Shared account config unchanged: {account}" in result.stdout
        assert account.read_bytes() == before and account.stat().st_ino == inode  # not even rewritten
        assert (dev.parent.parent / "da.tfvars").exists() and (dev.parent.parent / "ws.tfvars").exists()
    finally:
        holder.kill()
        holder.wait()


def test_a_stale_account_lock_after_kill_9_does_not_block_the_split(tmp_path, monkeypatch):
    import signal
    import subprocess

    from scripts import environment_lock

    account = _account_env(tmp_path, monkeypatch)
    dev = _draft(tmp_path, "dev", "dev_analysts")
    holder = subprocess.Popen(["sleep", "300"])
    assert environment_lock.acquire(account.parent, holder.pid, "rehearse")[0]
    holder.send_signal(signal.SIGKILL)
    holder.wait()
    result = _full_split(dev, account)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _groups(account) == ["dev_analysts"]
    # And the next lock taker still recovers it as before.
    ok, message, _ = environment_lock.acquire(account.parent, os.getpid(), "account-plan")
    assert ok and "removed stale lock from dead pid" in message
    environment_lock.release_lock(account.parent, os.getpid())


# ── the holder's nonce file must be a private regular file (PR #297 r4) ──────

@pytest.mark.parametrize("shape, message", [
    ("mode-0644", "has mode 0644; it must be exactly 0600"),
    ("mode-0640", "has mode 0640; it must be exactly 0600"),
    ("symlink", "is a symlink"),
    ("directory", "is not a regular file"),
    ("fifo", "is not a regular file"),
])
def test_the_holders_nonce_only_counts_from_a_private_regular_file(tmp_path, monkeypatch, shape, message):
    """Codex r3: the right nonce in a 0644 file, or behind a symlink, still let a non-holder write."""
    import subprocess

    from scripts import environment_lock

    account = _account_env(tmp_path, monkeypatch)
    dev = _draft(tmp_path, "dev", "dev_analysts")
    ok, held, nonce = environment_lock.acquire(account.parent, os.getpid(), "rehearse")  # this process: an ancestor
    assert ok, held
    try:
        private = tmp_path / "private-nonce"
        private.write_text(nonce + "\n")
        private.chmod(0o600)
        given = tmp_path / "given-nonce"
        if shape.startswith("mode-"):
            given.write_text(nonce + "\n")
            given.chmod(int(shape[5:], 8))
        elif shape == "symlink":
            given.symlink_to(private)  # the right nonce, in a private file, but through a link
        elif shape == "directory":
            given.mkdir()
            given.chmod(0o700)
        else:
            os.mkfifo(given, 0o600)
        result = subprocess.run([sys.executable, str(SPLIT), "--account-lock-nonce-file", str(given),
                                 "--account-only", str(dev), str(account)], capture_output=True, text=True, timeout=60)
        assert result.returncode == 1, result.stdout + result.stderr
        assert f"the account lock nonce file {given} {message}; nothing was written." in result.stdout
        assert not account.exists()
        # The same nonce in a private regular file is the holder's pass.
        passed = subprocess.run([sys.executable, str(SPLIT), "--account-lock-nonce-file", str(private),
                                 "--account-only", str(dev), str(account)], capture_output=True, text=True, timeout=60)
        assert passed.returncode == 0, passed.stdout + passed.stderr
        assert _groups(account) == ["dev_analysts"]
    finally:
        environment_lock.release_lock(account.parent, os.getpid())


def test_a_nonce_file_of_another_owner_is_refused(tmp_path, monkeypatch):
    """chown needs root, so the owner check is shown by running the check as another uid."""
    import split_abac_config as split

    nonce = tmp_path / "nonce"
    nonce.write_text("ab" * 32 + "\n")
    nonce.chmod(0o600)
    assert split.read_lock_nonce(nonce) == "ab" * 32
    other = os.getuid() + 1
    monkeypatch.setattr(split.os, "getuid", lambda: other)
    with pytest.raises(split.AccountFileError, match=rf"is owned by uid {os.stat(nonce).st_uid}, not this user \({other}\)"):
        split.read_lock_nonce(nonce)
