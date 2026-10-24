#!/usr/bin/env bash
set -euo pipefail

# Terraform state can contain credentials and resource secrets. Make every
# file this runner creates owner-only regardless of the caller's umask.
umask 077

if [ "$#" -lt 3 ]; then
  echo "Usage: $0 <layer> <env> <command> [args...]" >&2
  exit 1
fi

LAYER="$1"
ENV_NAME="$2"
COMMAND="$3"
shift 3

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOTS_DIR="$PROJECT_ROOT/roots"
ENVS_DIR="${ENVS_DIR:-$PROJECT_ROOT/envs}"

case "$LAYER" in
  account)
    ROOT_DIR="$ROOTS_DIR/account"
    ENV_DIR="${LAYER_ENV_DIR:-$ENVS_DIR/account}"
    ;;
  data_access)
    ROOT_DIR="$ROOTS_DIR/data_access"
    if [ -n "${LAYER_ENV_DIR:-}" ]; then
      ENV_DIR="$LAYER_ENV_DIR"
    elif [ "$ENV_NAME" = "data_access" ] && [ -d "$ENVS_DIR/data_access" ]; then
      ENV_DIR="$ENVS_DIR/data_access"
    else
      ENV_DIR="$ENVS_DIR/$ENV_NAME/data_access"
    fi
    ;;
  workspace)
    ROOT_DIR="$ROOTS_DIR/workspace"
    ENV_DIR="${LAYER_ENV_DIR:-$ENVS_DIR/$ENV_NAME}"
    ;;
  *)
    echo "Unknown layer: $LAYER" >&2
    exit 1
    ;;
esac

# Provisioners and the provider must resolve credentials from the same env.
# Direct runner invocations do not come through make, so establish this here.
export LAYER_ENV_DIR="${LAYER_ENV_DIR:-$ENV_DIR}"

# The env dir that owns this layer's state and remote_state.hcl: the dir the
# layer runs in (envs/<env>, or envs/account), never one rebuilt from
# ENVS_DIR, so the CI guard (state_layout.py ci-allowed) and this runner read
# the same remote_state.hcl.
STATE_ENV_DIR="$ENV_DIR"
if [ "$LAYER" = "data_access" ] && [ "$(dirname "$ENV_DIR")" != "$ENVS_DIR" ]; then
  STATE_ENV_DIR="$(dirname "$ENV_DIR")"
fi
# A workspace env whose catalog_map maps a catalog to itself (finding S2) can't
# adopt (import/apply) or destroy policies by name, even run by hand: they may
# be the source env's. Only its own allow_identity_catalog_map = true lets it
# through (scripts/saved_settings.py). The grep keeps every other call cheap.
case "$LAYER:$COMMAND" in
  data_access:import|data_access:apply|data_access:apply-plan|data_access:destroy|workspace:import|workspace:apply|workspace:apply-plan|workspace:destroy)
    owner_env_file="$STATE_ENV_DIR/env.auto.tfvars"
    if [ -f "$owner_env_file" ] && grep -qE '^[[:space:]]*catalog_map[[:space:]]*=' "$owner_env_file"; then
      python3 "$SCRIPT_DIR/saved_settings.py" identity-check --env-file "$owner_env_file" || exit 1
    fi
    ;;
esac
# state-pull prints only the state on stdout (state_layout.py state_json reads it).
if [ "$COMMAND" = "state-pull" ]; then
  exec 3>&1 1>&2
fi
STATE_BASE="$ENV_DIR"
USE_STATE_LAYOUT=0
if [ -n "${GENIERAILS_STATE_DIR:-}" ] || [ -f "$STATE_ENV_DIR/remote_state.hcl" ]; then
  USE_STATE_LAYOUT=1
fi
# Refuses a moved-state marker without GENIERAILS_STATE_DIR, and a state dir
# that lacks a state the checkout still holds (a split, not an empty env).
if [ "$USE_STATE_LAYOUT" = 1 ] || [ -f "$STATE_ENV_DIR/.state-moved" ]; then
  STATE_BASE="$(python3 "$SCRIPT_DIR/state_layout.py" layer-dir --env-dir "$STATE_ENV_DIR" --env "$ENV_NAME" --layer "$LAYER")" || exit 2
fi
mkdir -p "$STATE_BASE"
python3 "$SCRIPT_DIR/state_file_permissions.py" "$STATE_BASE"

if [ ! -d "$ROOT_DIR" ]; then
  echo "Missing Terraform root: $ROOT_DIR" >&2
  exit 1
fi

mkdir -p "$ENV_DIR"

# A staged policy phase (scripts/policy_generations.py) renders the data_access
# config into the env's ignored generated/.runtime/data_access and points
# GENIERAILS_LAYER_CONFIG_DIR at it: its abac.auto.tfvars and
# masking_functions.sql stand in for the split ones, which stay untouched.
LAYER_CONFIG_DIR="$ENV_DIR"
if [ "$LAYER" = "data_access" ] && [ -n "${GENIERAILS_LAYER_CONFIG_DIR:-}" ]; then
  if [ ! -f "$GENIERAILS_LAYER_CONFIG_DIR/abac.auto.tfvars" ]; then
    echo "ERROR: GENIERAILS_LAYER_CONFIG_DIR=$GENIERAILS_LAYER_CONFIG_DIR has no abac.auto.tfvars" >&2
    exit 2
  fi
  LAYER_CONFIG_DIR="$GENIERAILS_LAYER_CONFIG_DIR"
fi
_layer_tfvars() {
  if [ "$1" = abac.auto.tfvars ]; then printf '%s/%s' "$LAYER_CONFIG_DIR" "$1"; else printf '%s/%s' "$ENV_DIR" "$1"; fi
}

# Use TF_DATA_DIR for per-env .terraform/ isolation. The .terraform.lock.hcl
# file is always in the working directory — use -lockfile=readonly during init
# to prevent concurrent writes from corrupting it. On first run (no lock file),
# skip -lockfile=readonly so Terraform can generate the lock file.
export TF_DATA_DIR="$STATE_BASE/.terraform"
if [ "$USE_STATE_LAYOUT" = 1 ]; then
  # A nested run for the same env and layer (this runner reading its own state
  # through state_layout.py state-pull) reuses the caller's root: rebuilding
  # it would delete the directory the caller's command runs in.
  if [ "${GENIERAILS_PREPARED_ROOT_KEY:-}" = "$STATE_ENV_DIR:$LAYER" ] && [ -d "${GENIERAILS_PREPARED_ROOT:-}" ]; then
    ROOT_DIR="$GENIERAILS_PREPARED_ROOT"
  else
    ROOT_DIR="$(python3 "$SCRIPT_DIR/state_layout.py" root --env-dir "$STATE_ENV_DIR" --env "$ENV_NAME" --layer "$LAYER" --source "$ROOT_DIR")"
    export GENIERAILS_PREPARED_ROOT="$ROOT_DIR" GENIERAILS_PREPARED_ROOT_KEY="$STATE_ENV_DIR:$LAYER"
  fi
fi
cd "$ROOT_DIR"

# env_dir and the state snapshots (workspace_state_file, data_access_state_file)
# say where the roots read Terraform state; only this runner sets them, after
# every caller option. A caller assignment to one (-var in either form, a
# -var-file, the env's tfvars, TF_CLI_ARGS*) refuses here, before Terraform.
case "$COMMAND" in
  plan|apply|destroy|import|console|print-cmd)
    RUNNER_VAR_FILES=()
    for tfvars in auth.auto.tfvars env.auto.tfvars abac.auto.tfvars classification.auto.tfvars discovered_uc_tables.auto.tfvars; do
      tfvars_path="$(_layer_tfvars "$tfvars")"
      if [ -f "$tfvars_path" ]; then RUNNER_VAR_FILES+=(--var-file "$tfvars_path"); fi
    done
    if ! python3 "$SCRIPT_DIR/state_layout.py" check-runner-vars --tf-command "$COMMAND" --env-dir "$STATE_ENV_DIR" ${RUNNER_VAR_FILES[@]+"${RUNNER_VAR_FILES[@]}"} -- "$@"; then
      echo "ERROR: refusing terraform $COMMAND: env_dir, workspace_state_file and data_access_state_file are set only by $(basename "$0") (where the roots read Terraform state), and policy_stage, held_access_grants and rebind_table_grants only by the staged policy controller (scripts/staged_policy_apply.py)." >&2
      exit 2
    fi
    ;;
esac

INIT_CMD=(
  terraform
  init
  -input=false
  -reconfigure
)
REMOTE_BACKEND_ARGS=()
if [ "$USE_STATE_LAYOUT" = 1 ]; then
  # An invalid remote_state.hcl fails here; it never falls back to local state.
  backend_lines="$(python3 "$SCRIPT_DIR/state_layout.py" backend-args --env-dir "$STATE_ENV_DIR" --env "$ENV_NAME" --layer "$LAYER")" || exit 2
  while IFS= read -r arg; do
    if [ -n "$arg" ]; then REMOTE_BACKEND_ARGS+=("$arg"); fi
  done <<< "$backend_lines"
fi
if [ "${#REMOTE_BACKEND_ARGS[@]}" -gt 0 ]; then
  INIT_CMD+=("${REMOTE_BACKEND_ARGS[@]}")
else
  ci_value="$(printf '%s' "${CI:-}" | tr '[:upper:]' '[:lower:]')"
  is_ci=
  case "$ci_value" in true|1|yes) is_ci=1 ;; esac
  [ -n "${TF_BUILD:-}${JENKINS_URL:-}${GITLAB_CI:-}${BUILDKITE:-}${CIRCLECI:-}" ] && is_ci=1
  # Second line behind the make guard: in CI a write to a local backend would
  # land in the runner's throwaway state.
  case "$COMMAND" in
    apply|apply-plan|destroy|import|state-rm|state-mv)
      if [ "$is_ci" = 1 ] && [ "${GENIERAILS_ALLOW_CI_APPLY:-}" != 1 ]; then
        echo "ERROR: refusing terraform $COMMAND in CI: $STATE_ENV_DIR has no remote_state.hcl, so the backend is local." >&2
        exit 2
      fi
      ;;
  esac
  INIT_CMD+=(-backend-config="path=$STATE_BASE/terraform.tfstate")
fi

# Only the env's auth.auto.tfvars authenticates Terraform (scripts/terraform_env.sh).
AZURERM_BACKEND=
for arg in ${REMOTE_BACKEND_ARGS[@]+"${REMOTE_BACKEND_ARGS[@]}"}; do
  case "$arg" in -backend-config=storage_account_name=*) AZURERM_BACKEND=azurerm ;; esac
done
# shellcheck source=terraform_env.sh
. "$SCRIPT_DIR/terraform_env.sh"
genierails_terraform_env "$AZURERM_BACKEND"
WRITABLE_INIT_CMD=("${INIT_CMD[@]}")
if [ -f .terraform.lock.hcl ]; then
  INIT_CMD+=(-lockfile=readonly)
fi

# Serialize terraform init per root directory — concurrent inits in the same
# working directory corrupt provider resolution even with isolated TF_DATA_DIR.
# Use mkdir as a portable lock (atomic on all POSIX systems including macOS).
INIT_LOCK="$ROOT_DIR/.terraform-init.lock.d"
INIT_LOCK_OWNER="$INIT_LOCK/owner"
INIT_LOCK_HOST="$(hostname)"
INIT_LOCK_TIMEOUT_SECONDS="${INIT_LOCK_TIMEOUT_SECONDS:-600}"
INIT_LOCK_ACQUIRED=0
case "$INIT_LOCK_TIMEOUT_SECONDS" in
  ''|*[!0-9]*) echo "INIT_LOCK_TIMEOUT_SECONDS must be a non-negative integer" >&2; exit 2 ;;
esac
_unlock_init() {
  if [ "$INIT_LOCK_ACQUIRED" = "1" ]; then
    rm -f "$INIT_LOCK_OWNER"
    rmdir "$INIT_LOCK" 2>/dev/null || true
    INIT_LOCK_ACQUIRED=0
  elif [ -f "$INIT_LOCK_OWNER" ]; then
    owner_pid="$(sed -n 's/^pid=//p' "$INIT_LOCK_OWNER" 2>/dev/null || true)"
    owner_host="$(sed -n 's/^host=//p' "$INIT_LOCK_OWNER" 2>/dev/null || true)"
    if [ "$owner_pid" = "$$" ] && [ "$owner_host" = "$INIT_LOCK_HOST" ]; then
      rm -f "$INIT_LOCK_OWNER"
      rmdir "$INIT_LOCK" 2>/dev/null || true
    fi
  fi
}
_lock_interrupted() {
  if [ "$INIT_LOCK_ACQUIRED" = "1" ]; then
    echo "Interrupted while holding Terraform init lock $INIT_LOCK" >&2
  else
    echo "Interrupted while waiting for Terraform init lock $INIT_LOCK" >&2
  fi
  exit "$1"
}
trap '_unlock_init' EXIT
trap '_lock_interrupted 130' INT
trap '_lock_interrupted 143' TERM
lock_wait_started="$(date +%s)"
while :; do
  if mkdir "$INIT_LOCK" 2>/dev/null; then
    INIT_LOCK_ACQUIRED=1
    break
  fi
  if [ -f "$INIT_LOCK_OWNER" ]; then
    owner_pid="$(sed -n 's/^pid=//p' "$INIT_LOCK_OWNER" 2>/dev/null || true)"
    owner_host="$(sed -n 's/^host=//p' "$INIT_LOCK_OWNER" 2>/dev/null || true)"
    if [ "$owner_host" = "$INIT_LOCK_HOST" ] && [ -n "$owner_pid" ] \
      && ! kill -0 "$owner_pid" 2>/dev/null && ! ps -p "$owner_pid" >/dev/null 2>&1; then
      stale_owner="$INIT_LOCK/owner.reclaim.$$"
      if mv "$INIT_LOCK_OWNER" "$stale_owner" 2>/dev/null; then
        claimed_pid="$(sed -n 's/^pid=//p' "$stale_owner" 2>/dev/null || true)"
        claimed_host="$(sed -n 's/^host=//p' "$stale_owner" 2>/dev/null || true)"
        if [ "$claimed_pid" = "$owner_pid" ] && [ "$claimed_host" = "$owner_host" ]; then
          echo "+ reclaiming stale Terraform init lock $INIT_LOCK (dead local PID $owner_pid)" >&2
          rm -f "$stale_owner"
          rmdir "$INIT_LOCK" 2>/dev/null || true
        else
          mv "$stale_owner" "$INIT_LOCK_OWNER" 2>/dev/null || true
        fi
      fi
      continue
    fi
  fi
  lock_now="$(date +%s)"
  if [ $((lock_now - lock_wait_started)) -ge "$INIT_LOCK_TIMEOUT_SECONDS" ]; then
    echo "Timed out after ${INIT_LOCK_TIMEOUT_SECONDS}s waiting for Terraform init lock $INIT_LOCK." >&2
    echo "  If its recorded owner is no longer running, clear it with: rm -rf '$INIT_LOCK'" >&2
    exit 1
  fi
  sleep 0.2
done
{
  printf 'pid=%s\n' "$$"
  printf 'host=%s\n' "$INIT_LOCK_HOST"
  printf 'started_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$INIT_LOCK_OWNER"
echo "+ ${INIT_CMD[*]}" >&2
# The lock file is generated locally (gitignored), so an upgrade that adds a
# provider (even one only a tests/ module declares) leaves it stale and the
# read-only init refuses it. Inits are serialized above, so re-run it once
# writable to record the new provider instead of failing every command.
if ! init_output="$("${INIT_CMD[@]}" 2>&1)"; then
  if [[ "$init_output" != *"Provider dependency changes detected"* ]]; then
    printf '%s\n' "$init_output" >&2
    exit 1
  fi
  echo "+ $ROOT_DIR/.terraform.lock.hcl lacks providers this version needs; updating it" >&2
  _lock_providers() {
    awk '/^provider "/ { p = $2; gsub(/"/, "", p) } /^  version / { v = $3; gsub(/"/, "", v); print p " " v }' .terraform.lock.hcl
  }
  locked_before="$(_lock_providers)"
  echo "+ ${WRITABLE_INIT_CMD[*]}" >&2
  "${WRITABLE_INIT_CMD[@]}" >/dev/null
  _lock_providers | while read -r provider version; do
    case "$locked_before" in
      *"$provider $version"*) ;;
      *) echo "+   recorded $provider $version" >&2 ;;
    esac
  done
fi
_unlock_init
trap - EXIT INT TERM

# The scheduled governance job moved from the workspace root to data_access:
# before any command on either root, move it between the env's state files
# (a no-op unless the workspace state still holds it), so neither root ever
# plans to destroy or recreate it. Not for state-pull: that is how
# move_scheduled_job.py reads a remote state.
if [ "$COMMAND" != "print-cmd" ] && [ "$COMMAND" != "state-pull" ]; then
  case "$LAYER" in
    workspace) JOB_WS_DIR="$ENV_DIR"; JOB_DA_DIR="$ENV_DIR/data_access" ;;
    data_access) JOB_WS_DIR="$(dirname "$ENV_DIR")"; JOB_DA_DIR="$ENV_DIR" ;;
    *) JOB_WS_DIR= ;;
  esac
  if [ -n "$JOB_WS_DIR" ] && [ "$(basename "$JOB_DA_DIR")" = "data_access" ]; then
    python3 "$SCRIPT_DIR/move_scheduled_job.py" --workspace-dir "$JOB_WS_DIR" --data-access-dir "$JOB_DA_DIR"
  fi
fi

VAR_ARGS=()
for tfvars in auth.auto.tfvars env.auto.tfvars abac.auto.tfvars classification.auto.tfvars discovered_uc_tables.auto.tfvars; do
  tfvars_path="$(_layer_tfvars "$tfvars")"
  if [ -f "$tfvars_path" ]; then
    VAR_ARGS+=(-var-file="$tfvars_path")
  fi
done

# Per-agent files (envs/<env>/agents/*.auto.tfvars) merged into genie_spaces,
# after env.auto.tfvars so the merge wins. Written to a temp file for this run
# only (agent_files.py); a saved plan carries the values it was made with.
AGENTS_TMP_DIR=""
case "$LAYER:$COMMAND" in
  workspace:plan|workspace:apply|workspace:destroy|workspace:import|workspace:console|data_access:plan|data_access:apply|data_access:destroy|data_access:import|data_access:console)
    workspace_env_dir="$ENV_DIR"
    if [ "$LAYER" = "data_access" ]; then workspace_env_dir="$(dirname "$ENV_DIR")"; fi
    if [ -d "$workspace_env_dir/agents" ]; then
      AGENTS_TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/genierails-agents.XXXXXX")"
      trap 'rm -rf "$AGENTS_TMP_DIR"' EXIT
      agents_rc=0
      python3 "$PROJECT_ROOT/agent_files.py" tfvars --env-dir "$workspace_env_dir" --layer "$LAYER" \
        --var-file "$AGENTS_TMP_DIR/agents.tfvars.json" || agents_rc=$?
      if [ "$agents_rc" -eq 0 ]; then
        VAR_ARGS+=(-var-file="$AGENTS_TMP_DIR/agents.tfvars.json")
      elif [ "$agents_rc" -ne 3 ]; then
        exit "$agents_rc"
      fi
    fi
    ;;
esac

RUNNER_VARS=()
if [ "$LAYER" != "account" ]; then
  RUNNER_VARS+=(-var="env_dir=$ENV_DIR")
fi
if [ "$LAYER:$COMMAND" = "workspace:plan" ] && \
   python3 -c 'import os, sys; from pathlib import Path; sys.path.insert(0, sys.argv[1]); from state_layout import STRUCTURAL_CHECK_TOKEN_ENV, consume_structural_check_token; raise SystemExit(0 if consume_structural_check_token(Path(sys.argv[2]), os.environ.get(STRUCTURAL_CHECK_TOKEN_ENV)) else 1)' "$SCRIPT_DIR" "$ENV_DIR"; then
  RUNNER_VARS+=(-var="expected_pre_release_withholding=true")
fi
# Genie ID files: with GENIERAILS_STATE_DIR, the env's records/ in the state
# dir (state_layout.py records-dir refuses while any is still in the checkout).
# Exported for genie_space.sh, whose destroy-time commands resolve the file
# when they run. Unset: envs/<env>, as before.
unset GENIERAILS_GENIE_ID_DIR
case "$COMMAND" in plan|apply|destroy|import|console|apply-plan|print-cmd) genie_ids_used=1 ;; *) genie_ids_used=0 ;; esac
if [ "$LAYER" = "workspace" ] && [ -n "${GENIERAILS_STATE_DIR:-}" ] && [ "$genie_ids_used" = 1 ]; then
  GENIERAILS_GENIE_ID_DIR="$(python3 "$SCRIPT_DIR/state_layout.py" records-dir --env-dir "$ENV_DIR" --env "$ENV_NAME")" || exit 2
  mkdir -p "$GENIERAILS_GENIE_ID_DIR"
  export GENIERAILS_GENIE_ID_DIR
  RUNNER_VARS+=(-var="genie_id_dir=$GENIERAILS_GENIE_ID_DIR")
fi
if [ "$LAYER_CONFIG_DIR" != "$ENV_DIR" ]; then
  RUNNER_VARS+=(-var="masking_sql_file=$LAYER_CONFIG_DIR/masking_functions.sql")
fi
# masking_functions_drop records its SQL and auth files relative to the env
# dir (the data_access root's dirname(env_dir)); its destroy-time drop
# resolves them here, in whichever checkout runs the destroy.
if [ "$LAYER" = "data_access" ]; then
  GENIERAILS_MASKING_BASE_DIR="$(dirname "$ENV_DIR")"
  export GENIERAILS_MASKING_BASE_DIR
fi

# The roots read state with file(): the workspace root its own and the
# data_access state (the CAN_RUN on record, the applied coverage check and
# grants), the data_access root its own (the SELECT and masks applied, which
# its guards protect). With a state dir or remote state those files aren't
# next to env_dir, so they get snapshots read through scripts/state_layout.py,
# in this layer's runtime dir. A state that can't be read stops here: planning
# from "nothing applied" could withhold, and so remove, access already
# granted. (In-checkout state: no change.)
if [ "$LAYER" != "account" ] && [ "$USE_STATE_LAYOUT" = 1 ]; then
  STATE_INPUT_LAYERS="data_access"
  [ "$LAYER" = "workspace" ] && STATE_INPUT_LAYERS="workspace data_access"
  case "$COMMAND" in
    plan|apply|destroy|import|console)
      STATE_INPUTS_DIR="$STATE_BASE/inputs"
      for input_layer in $STATE_INPUT_LAYERS; do
        if ! python3 "$SCRIPT_DIR/state_layout.py" state-file --env-dir "$STATE_ENV_DIR" --env "$ENV_NAME" \
          --layer "$input_layer" --out "$STATE_INPUTS_DIR/$input_layer.tfstate.json"; then
          echo "ERROR: refusing terraform $COMMAND: the $input_layer state can't be read, so the Genie CAN_RUN on record is unknown." >&2
          exit 2
        fi
      done
      if [ "$LAYER" = "workspace" ]; then
        RUNNER_VARS+=(-var="workspace_state_file=$STATE_INPUTS_DIR/workspace.tfstate.json")
      fi
      RUNNER_VARS+=(-var="data_access_state_file=$STATE_INPUTS_DIR/data_access.tfstate.json")
      ;;
  esac
fi

# Terraform reads options only before a command's positional arguments
# (import: ADDR ID), so the runner's variables go after the caller's options
# and before those, with a "--" ending the options.
_split_caller_args() {
  local positionals=0
  case "$1" in import) positionals=2 ;; esac
  shift
  CALLER_OPTIONS=("$@")
  CALLER_POSITIONALS=()
  if [ "$positionals" -gt 0 ] && [ "$#" -ge "$positionals" ]; then
    local split=$(($# - positionals))
    if [ "$split" -gt 0 ] && [ "${CALLER_OPTIONS[$((split - 1))]}" = "--" ]; then split=$((split - 1)); fi
    CALLER_POSITIONALS=("${CALLER_OPTIONS[@]:$split}")
    CALLER_OPTIONS=("${CALLER_OPTIONS[@]:0:$split}")
  fi
}

case "$COMMAND" in
  plan|apply|destroy|import|console)
    _split_caller_args "$COMMAND" "$@"
    # The runner's own variables last among the options, so no earlier value can win.
    CMD=(terraform "$COMMAND" ${VAR_ARGS[@]+"${VAR_ARGS[@]}"} ${CALLER_OPTIONS[@]+"${CALLER_OPTIONS[@]}"}
      ${RUNNER_VARS[@]+"${RUNNER_VARS[@]}"} ${CALLER_POSITIONALS[@]+"${CALLER_POSITIONALS[@]}"})
    ;;
  show)
    CMD=(terraform show "$@")
    ;;
  apply-plan)
    # A saved plan already carries its variables; Terraform refuses -var-file.
    CMD=(terraform apply -input=false "$@")
    ;;
  state-list)
    CMD=(terraform state list "$@")
    ;;
  state-show)
    CMD=(terraform state show "$@")
    ;;
  state-pull)
    echo "+ terraform state pull" >&2
    terraform state pull >&3
    exit $?
    ;;
  state-rm|state-mv)
    # Terraform backs up a local state before changing it, by default as
    # <state file>.<time>.backup in its working directory, shared/roots,
    # where a broad add stages it. Keep it next to the state instead
    # (STATE_BASE: the env's state dir, under GENIERAILS_STATE_DIR when set).
    # A remote backend writes no backup.
    CMD=(terraform state "${COMMAND#state-}" "-backup=$STATE_BASE/terraform.tfstate.$(date -u +%Y%m%dT%H%M%SZ).$$.backup" "$@")
    ;;
  output)
    CMD=(terraform output "$@")
    ;;
  print-cmd)
    printf 'terraform %s (in %s, TF_DATA_DIR=%s)' "$1" "$ROOT_DIR" "$TF_DATA_DIR"
    print_command="${1:-}"
    shift || true
    _split_caller_args "$print_command" "$@"
    for arg in ${VAR_ARGS[@]+"${VAR_ARGS[@]}"} ${CALLER_OPTIONS[@]+"${CALLER_OPTIONS[@]}"} \
      ${RUNNER_VARS[@]+"${RUNNER_VARS[@]}"} ${CALLER_POSITIONALS[@]+"${CALLER_POSITIONALS[@]}"}; do
      printf ' %q' "$arg"
    done
    printf '\n'
    exit 0
    ;;
  *)
    echo "Unsupported terraform command alias: $COMMAND" >&2
    exit 1
    ;;
esac

echo "+ ${CMD[*]}" >&2
if [ "$LAYER" = "data_access" ] && { [ "$COMMAND" = "plan" ] || [ "$COMMAND" = "apply" ]; }; then
  # Terraform's output passes through unchanged; a copy lets the note below
  # explain a plan whose only destroys replace terraform_data.masking_functions.
  OUTPUT_COPY="$(mktemp)"
  STATUS_FILE="$(mktemp)"
  trap 'rm -f "$OUTPUT_COPY" "$STATUS_FILE"; [ -z "$AGENTS_TMP_DIR" ] || rm -rf "$AGENTS_TMP_DIR"' EXIT
  set +e
  # On Ctrl-C (or TERM/HUP) Terraform shuts down gracefully and keeps writing
  # (saving state) through the pipe, so tee ignores those signals. This shell
  # and the group around Terraform trap them instead (a trap, unlike ignoring,
  # isn't inherited by Terraform) so they wait for it and return its status.
  trap ':' TERM HUP
  { trap ':' TERM HUP; "${CMD[@]}"; echo "$?" > "$STATUS_FILE"; } | (trap '' INT TERM HUP; exec tee "$OUTPUT_COPY")
  status="$(cat "$STATUS_FILE")"
  set -e
  python3 "$SCRIPT_DIR/masking_replace_note.py" "$OUTPUT_COPY" || true
  exit "${status:-1}"
fi
"${CMD[@]}"
