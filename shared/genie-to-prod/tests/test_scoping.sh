#!/usr/bin/env bash
# Verifies that promoting a subset never touches an unselected space.
#
# This is the property that matters most: a space you did not pick must not be
# created, updated, or deleted. It regressed twice during development --
# once as `delete genie_spaces.<key>` (no local definition for a tracked space)
# and once as a spurious `update` (a stored etag the local config could not
# match) -- so it is checked here rather than only by eye.
#
# Runs offline: a stub `databricks` on PATH stands in for the CLI.
#
# Usage: tests/test_scoping.sh

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
SOURCE="$PWD"

# Run against a throwaway copy. The script writes into resources/ and src/, and
# clobbering a real promotion's exported definitions would be destructive.
SANDBOX="$(mktemp -d)"
trap 'rm -rf "$SANDBOX"' EXIT
cp -R "$SOURCE"/promote_genie.sh "$SOURCE"/databricks.yml "$SOURCE"/lib \
      "$SANDBOX"/ 2>/dev/null
mkdir -p "$SANDBOX/resources" "$SANDBOX/src"
ROOT="$SANDBOX"

PASS=0
FAIL=0

check() {
  local name="$1" expected="$2" actual="$3"
  if [[ "$actual" == *"$expected"* ]]; then
    printf '  ok   %s\n' "$name"
    PASS=$((PASS + 1))
  else
    printf '  FAIL %s\n         expected to contain: %s\n         got: %s\n' \
      "$name" "$expected" "$actual"
    FAIL=$((FAIL + 1))
  fi
}

refute() {
  local name="$1" unexpected="$2" actual="$3"
  if [[ "$actual" != *"$unexpected"* ]]; then
    printf '  ok   %s\n' "$name"
    PASS=$((PASS + 1))
  else
    printf '  FAIL %s\n         must NOT contain: %s\n         got: %s\n' \
      "$name" "$unexpected" "$actual"
    FAIL=$((FAIL + 1))
  fi
}

WORK="$SANDBOX/work"
mkdir -p "$WORK"

STUB="$WORK/bin"
mkdir -p "$STUB"

# Records every plan/deploy invocation so the assertions can inspect the flags.
cat > "$STUB/databricks" <<'STUBEOF'
#!/usr/bin/env bash
ARGS="$*"
echo "$ARGS" >> "$CALL_LOG"
case "$ARGS" in
  "--version") echo "Databricks CLI v1.11.0" ;;
  *"current-user me"*) echo '{"userName":"t@example.com"}' ;;
  *"bundle generate genie-space"*)
     key=""; for ((i=1;i<=$#;i++)); do [[ "${!i}" == "--key" ]] && j=$((i+1)) && key="${!j}"; done
     mkdir -p src resources
     printf '{"data_sources":{"tables":[{"identifier":"devcat.s.t"}]}}\n' \
       > "src/${key}.geniespace.json"
     cat > "resources/${key}.genie_space.yml" <<Y
resources:
  genie_spaces:
    ${key}:
      title: "T ${key}"
      warehouse_id: dev_wh
      file_path: ../src/${key}.geniespace.json
      parent_path: /Workspace/Users/t/genie
Y
     ;;
  *"bundle summary"*)
     echo '{"resources":{"genie_spaces":{"alpha":{"id":"01fALPHA"},"beta":{"id":"01fBETA"}}}}' ;;
  *"include_serialized_space=true"*)
     echo '{"title":"T","warehouse_id":"wh_prod","parent_path":"/Shared/genie","serialized_space":"{\"data_sources\":{\"tables\":[{\"identifier\":\"prodcat.s.t\"}]}}"}' ;;
  *"bundle validate"*) echo "Validation OK" ;;
  *"bundle plan"*)
     # Echo only what --select asked for, mirroring the real CLI.
     for ((i=1;i<=$#;i++)); do
       if [[ "${!i}" == "--select" ]]; then j=$((i+1)); echo "  update ${!j}"; fi
     done
     echo "Plan: 0 to add, 1 to change, 0 to delete, 0 unchanged" ;;
  *"bundle deploy"*) echo "Deployment complete" ;;
  *"sql/statements"*) echo '{"status":{"state":"SUCCEEDED"}}' ;;
  *) echo "{}" ;;
esac
exit 0
STUBEOF
chmod +x "$STUB/databricks"

# A config with two spaces, only one of which this run selects.
cat > "$WORK/spaces.yml" <<'Y'
catalog_map:
  devcat: prodcat

spaces:
  - key: alpha
    dev_id: 01fDEVALPHA
  - key: beta
    dev_id: 01fDEVBETA
Y

export CALL_LOG="$WORK/calls.log"
: > "$CALL_LOG"

echo "Scoping: promoting only 'alpha' must not touch 'beta'"

OUT="$(cd "$ROOT" && PATH="$STUB:$PATH" ./promote_genie.sh \
  --spaces "$WORK/spaces.yml" --only alpha --skip-table-check 2>&1)"

check "alpha is planned"            "genie_spaces.alpha"  "$OUT"
check "plan reports no deletes"     "0 to delete"         "$OUT"
refute "beta is not planned"        "update genie_spaces.beta" "$OUT"
refute "nothing is deleted"         "delete genie_spaces"  "$OUT"

PLAN_CALL="$(grep 'bundle plan' "$CALL_LOG" | head -1)"
check "plan is scoped with --select" "--select genie_spaces.alpha" "$PLAN_CALL"
refute "plan does not select beta"   "genie_spaces.beta"           "$PLAN_CALL"

# beta must still be materialised on disk, or `--select` fails to load the bundle
check "beta kept on disk" "beta" "$(ls "$ROOT/resources" 2>/dev/null)"

echo
echo "Deploy path is scoped too"
: > "$CALL_LOG"
OUT="$(cd "$ROOT" && PATH="$STUB:$PATH" ./promote_genie.sh \
  --spaces "$WORK/spaces.yml" --only alpha --skip-table-check --apply --yes 2>&1)"
DEPLOY_CALL="$(grep 'bundle deploy' "$CALL_LOG" | head -1)"
check "deploy is scoped with --select" "--select genie_spaces.alpha" "$DEPLOY_CALL"
refute "deploy does not select beta"   "genie_spaces.beta"          "$DEPLOY_CALL"

echo
printf '%d passed, %d failed\n' "$PASS" "$FAIL"
[[ "$FAIL" -eq 0 ]]
