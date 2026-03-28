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
    "industry-overlay", "genie-import-no-abac",
]


def _ts():
    return time.strftime("%H:%M:%S")

def _green(s): return f"\033[32m{s}\033[0m"
def _red(s): return f"\033[31m{s}\033[0m"
def _yellow(s): return f"\033[33m{s}\033[0m"
def _bold(s): return f"\033[1m{s}\033[0m"


def provision_for_scenario(scenario, env_file, cloud):
    state_file = SCRIPT_DIR / f".test_env_state.{cloud}.{scenario}.json"
    if state_file.exists():
        state_file.unlink()

    env = os.environ.copy()
    env["CLOUD_PROVIDER"] = cloud
    env["CLOUD_ROOT"] = str(CLOUD_ROOT)
    env["_PARALLEL_STATE_FILE"] = str(state_file)

    print(f"  [{_ts()}] {_bold(scenario)}: provisioning...")
    result = subprocess.run(
        [sys.executable, str(SCRIPT_DIR / "provision_test_env.py"),
         "provision", "--force", "--env-file", str(env_file)],
        cwd=str(CLOUD_ROOT), env=env, capture_output=True, text=True, timeout=1800,
    )
    if result.returncode != 0 or not state_file.exists():
        print(f"  [{_ts()}] {_red(scenario)}: provisioning FAILED")
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


def run_scenario(scenario, auth_file, state_file, run_id, cloud):
    env = os.environ.copy()
    env["CLOUD_PROVIDER"] = cloud
    env["CLOUD_ROOT"] = str(CLOUD_ROOT)
    env["_PARALLEL_STATE_FILE"] = state_file
    # Per-scenario suffix for account-level name isolation
    env["_TEST_SUFFIX"] = run_id[:6] if run_id else ""

    print(f"  [{_ts()}] {_bold(scenario)}: running (suffix={run_id[:6]})...")
    start = time.time()
    result = subprocess.run(
        [sys.executable, str(SCRIPT_DIR / "run_integration_tests.py"),
         "--scenario", scenario, "--auth-file", auth_file],
        cwd=str(CLOUD_ROOT), env=env, capture_output=True, text=True, timeout=3600,
    )
    elapsed = time.time() - start
    passed = result.returncode == 0

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

    return {"scenario": scenario, "status": "passed" if passed else "failed", "elapsed": elapsed}


def teardown_scenario(scenario, state_file, env_file, cloud):
    env = os.environ.copy()
    env["CLOUD_PROVIDER"] = cloud
    env["CLOUD_ROOT"] = str(CLOUD_ROOT)
    env["_PARALLEL_STATE_FILE"] = state_file

    print(f"  [{_ts()}] {scenario}: tearing down...")
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
                       help="Max concurrent scenarios (default: 15)")
    parser.add_argument("--keep-envs", action="store_true")
    args = parser.parse_args()

    env_file = Path(args.env_file).resolve()
    cloud = _default_cloud
    scenarios = [s.strip() for s in args.scenarios.split(",") if s.strip()]
    max_parallel = args.max_parallel

    print("=" * 64)
    print(f"  Parallel Integration Test Runner")
    print("=" * 64)
    print(f"  Cloud:        {cloud}")
    print(f"  Scenarios:    {len(scenarios)}")
    print(f"  Max parallel: {max_parallel}")
    print(f"  Credentials:  {env_file}")
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

    # Phase 2: Provision all in parallel
    print(f"── Phase 2: Provisioning {len(scenarios)} environments (all concurrent)")
    provision_results = {}
    with ThreadPoolExecutor(max_workers=len(scenarios)) as executor:
        futures = {executor.submit(provision_for_scenario, s, env_file, cloud): s for s in scenarios}
        for f in as_completed(futures):
            s = futures[f]
            try:
                provision_results[s] = f.result()
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

    def run_and_teardown(scenario, info):
        result = run_scenario(scenario, info["auth_file"], info["state_file"], info["run_id"], cloud)
        if not keep_envs and "state_file" in info:
            teardown_scenario(scenario, info["state_file"], env_file, cloud)
            try:
                Path(info["state_file"]).unlink()
            except Exception:
                pass
        return result

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {executor.submit(run_and_teardown, s, info): s for s, info in provisioned.items()}
        for f in as_completed(futures):
            s = futures[f]
            try:
                test_results[s] = f.result()
            except Exception as exc:
                test_results[s] = {"scenario": s, "status": "failed", "elapsed": 0}

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
    print("=" * 64)
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
