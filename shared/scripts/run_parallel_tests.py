#!/usr/bin/env python3
"""
Parallel integration test runner — provisions a fresh workspace per scenario.

Each scenario gets its own isolated Databricks workspace + metastore + unique
account-level name suffix, so scenarios run concurrently with zero conflicts.

Usage
-----
  # Run all scenarios in parallel (from cloud wrapper directory)
  python shared/scripts/run_parallel_tests.py --env-file shared/scripts/account-admin.aws.env

  # Run specific scenarios
  python shared/scripts/run_parallel_tests.py --scenarios quickstart,multi-space

  # Limit concurrency
  python shared/scripts/run_parallel_tests.py --max-parallel 5

  # Keep environments for inspection
  python shared/scripts/run_parallel_tests.py --keep-envs
"""
import argparse
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
MODULE_ROOT = SCRIPT_DIR.parent
_default_cloud = os.environ.get("CLOUD_PROVIDER", "aws").lower()
CLOUD_ROOT = Path(os.environ.get("CLOUD_ROOT", MODULE_ROOT.parent / _default_cloud))

SCENARIOS = [
    "quickstart", "multi-catalog", "multi-space", "per-space", "promote",
    "multi-env", "attach-promote", "self-service-genie", "abac-only",
    "multi-space-import", "schema-drift", "genie-only", "country-overlay",
    "industry-overlay", "genie-import-no-abac", "aus-bank-demo",
]


def _ts():
    return time.strftime("%H:%M:%S")

def _green(s): return f"\033[32m{s}\033[0m"
def _red(s): return f"\033[31m{s}\033[0m"
def _yellow(s): return f"\033[33m{s}\033[0m"
def _bold(s): return f"\033[1m{s}\033[0m"


def provision_for_scenario(scenario, env_file, cloud, suite_id, log_dir=None):
    state_file = SCRIPT_DIR / f".test_env_state.{cloud}.{suite_id}.{scenario}.json"
    if state_file.exists():
        state_file.unlink()

    env = os.environ.copy()
    env["CLOUD_PROVIDER"] = cloud
    env["CLOUD_ROOT"] = str(CLOUD_ROOT)
    env["_PARALLEL_STATE_FILE"] = str(state_file)
    env["_PARALLEL_SUITE_ID"] = suite_id
    # Clear Databricks SDK env vars — same fix as run_scenario.
    # Without this, inherited DATABRICKS_HOST causes the provision script's
    # WorkspaceClient to create external locations on the wrong workspace.
    for k in ["DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET",
              "DATABRICKS_TOKEN", "DATABRICKS_ACCOUNT_ID"]:
        env.pop(k, None)

    print(f"  [{_ts()}] {_bold(scenario)}: provisioning...")
    result = subprocess.run(
        [sys.executable, str(SCRIPT_DIR / "provision_test_env.py"),
         "provision", "--force", "--env-file", str(env_file)],
        cwd=str(CLOUD_ROOT), env=env, capture_output=True, text=True, timeout=1800,
    )
    if result.returncode != 0 or not state_file.exists():
        print(f"  [{_ts()}] {_red(scenario)}: provisioning FAILED")
        if log_dir:
            log_file = Path(log_dir) / f"{scenario}.provision.log"
            with open(log_file, "w") as f:
                f.write(f"# Provision FAILED for {scenario} ({cloud})\n")
                f.write(f"# Exit code: {result.returncode}\n")
                f.write("=" * 72 + "\n\n")
                f.write("=== STDOUT ===\n")
                f.write(result.stdout or "(empty)\n")
                f.write("\n=== STDERR ===\n")
                f.write(result.stderr or "(empty)\n")
            print(f"    | Full log: {log_file}")
        return {"scenario": scenario, "status": "provision_failed",
                "error": (result.stdout or "")[-300:]}

    state = json.loads(state_file.read_text())
    print(f"  [{_ts()}] {_green(scenario)}: provisioned ({state.get('workspace_name', '?')})")
    return {
        "scenario": scenario, "status": "provisioned", "state": state,
        "state_file": str(state_file),
        "auth_file": str(Path(state.get("test_envs_dir", "")) / "dev" / "auth.auto.tfvars"),
        "run_id": state.get("run_id", ""),
    }


def run_scenario(scenario, auth_file, state_file, run_id, cloud, suite_id, log_dir=None):
    env = os.environ.copy()
    env["CLOUD_PROVIDER"] = cloud
    env["CLOUD_ROOT"] = str(CLOUD_ROOT)
    env["_PARALLEL_STATE_FILE"] = state_file
    env["_PARALLEL_SUITE_ID"] = suite_id
    # Per-scenario suffix for account-level name isolation
    env["_TEST_SUFFIX"] = run_id[:6] if run_id else ""
    # Clear Databricks SDK env vars so each scenario reads from its own auth file.
    # Without this, inherited values from the parent shell prevent _configure_sdk_env()
    # from setting the correct per-workspace host/credentials.
    for k in ["DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET",
              "DATABRICKS_TOKEN", "DATABRICKS_ACCOUNT_ID"]:
        env.pop(k, None)

    suffix = run_id[:6] if run_id else "nosuffix"
    if cloud == "azure":
        # Azure workspace / SP auth can lag slightly behind provisioning.
        # Give the freshly created workspace a short settle window before
        # Terraform uses oauth-m2m against workspace APIs.
        print(f"  [{_ts()}] {_yellow(scenario)}: waiting 30s for Azure auth propagation...")
        time.sleep(30)
    print(f"  [{_ts()}] {_bold(scenario)}: running (suffix={suffix})...")
    start = time.time()
    try:
        result = subprocess.run(
            [sys.executable, str(SCRIPT_DIR / "run_integration_tests.py"),
             "--scenario", scenario, "--auth-file", auth_file],
            cwd=str(CLOUD_ROOT), env=env, capture_output=True, text=True, timeout=14400,  # 4 hours
        )
    except subprocess.TimeoutExpired as te:
        elapsed = time.time() - start
        print(f"  [{_ts()}] {_red(scenario)}: TIMED OUT after {elapsed:.0f}s")
        if log_dir:
            log_file = Path(log_dir) / f"{scenario}.{suffix}.log"
            with open(log_file, "w") as f:
                f.write(f"# Scenario: {scenario}  TIMED OUT after {elapsed:.0f}s\n")
                f.write("=" * 72 + "\n\n")
                f.write("=== STDOUT (partial) ===\n")
                stdout = te.stdout or b""
                f.write(stdout.decode("utf-8", errors="replace") if isinstance(stdout, bytes) else stdout or "(empty)")
                f.write("\n=== STDERR (partial) ===\n")
                stderr = te.stderr or b""
                f.write(stderr.decode("utf-8", errors="replace") if isinstance(stderr, bytes) else stderr or "(empty)")
            print(f"    | Full log: {log_file}")
        return {"scenario": scenario, "status": "failed", "elapsed": elapsed}
    elapsed = time.time() - start
    passed = result.returncode == 0

    # Save full output to log file for post-mortem debugging
    if log_dir:
        log_file = Path(log_dir) / f"{scenario}.{suffix}.log"
        with open(log_file, "w") as f:
            f.write(f"# Scenario: {scenario}  Suffix: {suffix}  Cloud: {cloud}\n")
            f.write(f"# Status: {'PASSED' if passed else 'FAILED'}  Elapsed: {elapsed:.0f}s\n")
            f.write(f"# Exit code: {result.returncode}\n")
            f.write("=" * 72 + "\n\n")
            f.write("=== STDOUT ===\n")
            f.write(result.stdout or "(empty)\n")
            f.write("\n=== STDERR ===\n")
            f.write(result.stderr or "(empty)\n")

    if passed:
        print(f"  [{_ts()}] {_green(scenario)}: PASSED ({elapsed:.0f}s)")
    else:
        stdout = result.stdout or ""
        error_lines = [l for l in stdout.split("\n") if "Error:" in l or "FAILED" in l][-3:]
        last_lines = stdout.strip().split("\n")[-5:]
        seen = set()
        deduped = [l for l in (error_lines + last_lines) if l.strip() and l not in seen and not seen.add(l)]
        print(f"  [{_ts()}] {_red(scenario)}: FAILED ({elapsed:.0f}s)")
        for l in deduped[-6:]:
            print(f"    | {l}")
        if log_dir:
            print(f"    | Full log: {log_file}")

    return {"scenario": scenario, "status": "passed" if passed else "failed", "elapsed": elapsed}


def _cleanup_account_resources(auth_file, suffix, cloud):
    """Delete account-level groups and tag policies matching this scenario's suffix."""
    if not suffix:
        return
    try:
        import re as _re_cl
        import hcl2

        with open(auth_file) as f:
            cfg = hcl2.load(f)
        _s = lambda v: (v[0] if isinstance(v, list) else (v or "")).strip()
        account_id = _s(cfg.get("databricks_account_id", ""))
        client_id = _s(cfg.get("databricks_client_id", ""))
        client_secret = _s(cfg.get("databricks_client_secret", ""))
        account_host = _s(cfg.get("databricks_account_host", ""))
        if not account_host:
            account_host = "https://accounts.azuredatabricks.net" if cloud == "azure" else "https://accounts.cloud.databricks.com"
        if not account_id:
            return

        from databricks.sdk import AccountClient
        a = AccountClient(host=account_host, account_id=account_id,
                         client_id=client_id, client_secret=client_secret)

        # Delete groups ending with our suffix
        for g in list(a.groups.list()):
            name = g.display_name or ""
            if name.endswith(f"_{suffix}"):
                try:
                    a.groups.delete(id=g.id)
                except Exception:
                    pass

        # Delete tag policies ending with our suffix
        # Tag policies are accessed via workspace client, but we need a workspace host.
        # Read it from the auth file.
        ws_host = _s(cfg.get("databricks_workspace_host", ""))
        if ws_host:
            try:
                from databricks.sdk import WorkspaceClient
                w = WorkspaceClient(host=ws_host, client_id=client_id, client_secret=client_secret)
                for tp in list(w.tag_policies.list_tag_policies()):
                    key = getattr(tp, "tag_key", "") or ""
                    if key.endswith(f"_{suffix}"):
                        try:
                            w.tag_policies.delete_tag_policy(tag_key=key)
                        except Exception:
                            pass
            except Exception:
                pass
    except Exception:
        pass  # best effort


def teardown_scenario(scenario, state_file, env_file, cloud, suffix=""):
    env = os.environ.copy()
    env["CLOUD_PROVIDER"] = cloud
    env["CLOUD_ROOT"] = str(CLOUD_ROOT)
    env["_PARALLEL_STATE_FILE"] = state_file

    print(f"  [{_ts()}] {scenario}: tearing down...")

    # Delete account-level groups/tag policies with this scenario's suffix
    state = {}
    try:
        state = json.loads(Path(state_file).read_text())
    except Exception:
        pass
    auth_file = str(Path(state.get("test_envs_dir", "")) / "dev" / "auth.auto.tfvars")
    if Path(auth_file).exists():
        _cleanup_account_resources(auth_file, suffix, cloud)

    # Teardown workspace + metastore + storage
    result = subprocess.run(
        [sys.executable, str(SCRIPT_DIR / "provision_test_env.py"),
         "teardown", "--env-file", str(env_file)],
        cwd=str(CLOUD_ROOT), env=env, capture_output=True, text=True, timeout=600,
    )
    if result.returncode == 0:
        print(f"  [{_ts()}] {scenario}: teardown complete")
    else:
        print(f"  [{_ts()}] {_yellow(scenario)}: teardown had errors")


def main():
    parser = argparse.ArgumentParser(description="Parallel integration tests")
    parser.add_argument("--env-file", required=True)
    parser.add_argument("--scenarios", default=",".join(SCENARIOS))
    parser.add_argument("--max-parallel", type=int, default=15,
                       help="Max concurrent scenario executions (default: 15). "
                            "Provisioning always runs all in parallel regardless.")
    parser.add_argument("--keep-envs", action="store_true")
    parser.add_argument("--no-fail-fast", action="store_true",
                       help="Continue running all scenarios even if one fails")
    args = parser.parse_args()

    env_file = Path(args.env_file).resolve()
    cloud = _default_cloud
    scenarios = [s.strip() for s in args.scenarios.split(",") if s.strip()]
    max_parallel = args.max_parallel

    # Create timestamped log directory for per-scenario output
    suite_id = time.strftime('%Y%m%d_%H%M%S')
    log_dir = SCRIPT_DIR / "logs" / f"{cloud}_{suite_id}"
    log_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 64)
    print(f"  Parallel Integration Test Runner")
    print("=" * 64)
    print(f"  Cloud:        {cloud}")
    print(f"  Scenarios:    {len(scenarios)}")
    print(f"  Max parallel: {max_parallel}")
    print(f"  Credentials:  {env_file}")
    print(f"  Logs:         {log_dir}")
    print()

    # ── Phase 0: Clean up orphan account resources from previous runs ────────
    print("── Phase 0: Cleaning orphan account resources from previous runs")
    try:
        import re as _re_pre
        import hcl2 as _hcl2_pre
        with open(env_file) as f:
            _cfg = {}
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    _cfg[k.strip()] = v.strip()

        _account_id = _cfg.get("DATABRICKS_ACCOUNT_ID", "")
        _client_id = _cfg.get("DATABRICKS_CLIENT_ID", "")
        _client_secret = _cfg.get("DATABRICKS_CLIENT_SECRET", "")
        _account_host = "https://accounts.azuredatabricks.net" if cloud == "azure" else "https://accounts.cloud.databricks.com"

        from databricks.sdk import AccountClient
        _a = AccountClient(host=_account_host, account_id=_account_id,
                          client_id=_client_id, client_secret=_client_secret)

        # Delete suffixed groups (pattern: Title_Case_hexsuffix)
        _suffix_re = _re_pre.compile(r"^[A-Z][a-z]+(_[A-Z][a-z]+)*_[a-f0-9]{6}$")
        _g_del = 0
        for g in list(_a.groups.list()):
            name = g.display_name or ""
            if _suffix_re.match(name):
                try:
                    _a.groups.delete(id=g.id)
                    _g_del += 1
                except Exception:
                    pass
        if _g_del:
            print(f"  Deleted {_g_del} orphan suffixed groups")

        # Delete suffixed tag policies (need a workspace — provision one temporarily or skip)
        # Tag policies are only accessible via workspace API; we can't clean them without a workspace.
        # They'll be cleaned by each scenario's _preamble_cleanup or by the scenario retry.
        print(f"  (Tag policies cleaned per-scenario during apply)")
    except Exception as exc:
        print(f"  {_yellow('WARN')} Cleanup failed: {exc}")
    print()

    # Phase 1: Unit tests
    print("── Phase 1: Unit Tests")
    r = subprocess.run([sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short"],
                      cwd=str(MODULE_ROOT), capture_output=True, text=True)
    if r.returncode != 0:
        print(f"  {_red('Unit tests FAILED')} — aborting")
        sys.exit(1)
    for line in r.stdout.split("\n"):
        if "passed" in line:
            print(f"  {_green(line.strip())}")
            break
    print()

    # Phase 2: Provision all in parallel (with 1 retry for failures)
    print(f"── Phase 2: Provisioning {len(scenarios)} environments (all concurrent)")
    provision_results = {}
    with ThreadPoolExecutor(max_workers=len(scenarios)) as executor:
        futures = {
            executor.submit(provision_for_scenario, s, env_file, cloud, suite_id, log_dir): s
            for s in scenarios
        }
        for f in as_completed(futures):
            s = futures[f]
            try:
                provision_results[s] = f.result()
            except Exception as exc:
                provision_results[s] = {"scenario": s, "status": "provision_failed", "error": str(exc)}

    # Retry any failed provisions once
    failed = {k: v for k, v in provision_results.items() if v["status"] != "provisioned"}
    if failed:
        print(f"\n  Retrying {len(failed)} failed provision(s)...")
        with ThreadPoolExecutor(max_workers=len(failed)) as executor:
            futures = {
                executor.submit(provision_for_scenario, s, env_file, cloud, suite_id, log_dir): s
                for s in failed
            }
            for f in as_completed(futures):
                s = futures[f]
                try:
                    result = f.result()
                    provision_results[s] = result
                except Exception as exc:
                    provision_results[s] = {"scenario": s, "status": "provision_failed", "error": str(exc)}

    provisioned = {k: v for k, v in provision_results.items() if v["status"] == "provisioned"}
    failed_prov = {k: v for k, v in provision_results.items() if v["status"] != "provisioned"}
    print(f"\n  Provisioned: {len(provisioned)}/{len(scenarios)}")
    if failed_prov:
        print(f"  Failed: {', '.join(failed_prov.keys())}")
    print()

    # Phase 3: Run + teardown per scenario
    print(f"── Phase 3: Running {len(provisioned)} scenarios (max {max_parallel} concurrent)")
    print(f"  Each workspace is torn down immediately after its scenario completes.")
    test_results = {}
    keep_envs = args.keep_envs
    fail_fast = not args.no_fail_fast
    _abort = False  # Set on first failure to skip remaining scenarios (if fail-fast)

    def run_and_teardown(scenario, info):
        nonlocal _abort
        suffix = info.get("run_id", "")[:6]
        if _abort and fail_fast:
            if not keep_envs and "state_file" in info:
                teardown_scenario(scenario, info["state_file"], env_file, cloud, suffix=suffix)
                try: Path(info["state_file"]).unlink()
                except Exception: pass
            return {"scenario": scenario, "status": "skipped", "elapsed": 0}
        # Validate auth file exists before running (catch missing files early)
        auth = info.get("auth_file", "")
        if not Path(auth).exists():
            print(f"  [{_ts()}] {_red(scenario)}: auth file not found: {auth}")
            if not keep_envs and "state_file" in info:
                teardown_scenario(scenario, info["state_file"], env_file, cloud, suffix=suffix)
                try: Path(info["state_file"]).unlink()
                except Exception: pass
            return {"scenario": scenario, "status": "failed", "elapsed": 0}
        result = run_scenario(
            scenario,
            auth,
            info["state_file"],
            info["run_id"],
            cloud,
            suite_id,
            log_dir=str(log_dir),
        )
        if result["status"] != "passed":
            _abort = True
        if not keep_envs and "state_file" in info:
            teardown_scenario(scenario, info["state_file"], env_file, cloud, suffix=suffix)
            try: Path(info["state_file"]).unlink()
            except Exception: pass
        return result

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {executor.submit(run_and_teardown, s, info): s for s, info in provisioned.items()}
        for f in as_completed(futures):
            s = futures[f]
            try:
                test_results[s] = f.result()
            except Exception as exc:
                print(f"  [{_ts()}] {_red(s)}: CRASHED — {exc}")
                test_results[s] = {"scenario": s, "status": "failed", "elapsed": 0}
                _abort = True
                # Save crash info to log
                if log_dir:
                    crash_log = Path(log_dir) / f"{s}.CRASH.log"
                    import traceback
                    with open(crash_log, "w") as cf:
                        cf.write(f"# Scenario {s} crashed\n")
                        cf.write(f"# Exception: {exc}\n\n")
                        traceback.print_exc(file=cf)

    print()

    # Results
    print("=" * 64)
    print("  Results")
    print("=" * 64)
    all_passed = True
    for s in scenarios:
        if s in failed_prov:
            print(f"  {s:<22}  {_red('PROVISION FAILED')}")
            all_passed = False
        elif s in test_results:
            r = test_results[s]
            e = r.get("elapsed", 0)
            if r["status"] == "passed":
                print(f"  {s:<22}  {_green(f'PASSED  ({e:.0f}s)')}")
            elif r["status"] == "skipped":
                print(f"  {s:<22}  {_yellow('SKIPPED  (fail-fast)')}")
                all_passed = False
            else:
                print(f"  {s:<22}  {_red(f'FAILED  ({e:.0f}s)')}")
                all_passed = False
        else:
            print(f"  {s:<22}  {_yellow('NOT RUN')}")
            all_passed = False

    print()
    if all_passed:
        print(f"  {_green(_bold('All scenarios PASSED'))}")
    else:
        p = sum(1 for r in test_results.values() if r["status"] == "passed")
        print(f"  {_red(_bold(f'{len(scenarios) - p} scenario(s) FAILED'))}")
    print(f"\n  Logs: {log_dir}")
    print("=" * 64)
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
