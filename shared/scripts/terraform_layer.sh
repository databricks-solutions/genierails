#!/usr/bin/env bash
set -euo pipefail

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

if [ ! -d "$ROOT_DIR" ]; then
  echo "Missing Terraform root: $ROOT_DIR" >&2
  exit 1
fi

mkdir -p "$ENV_DIR"

# Use TF_DATA_DIR for per-env .terraform/ isolation. The .terraform.lock.hcl
# file is always in the working directory — use -lockfile=readonly during init
# to prevent concurrent writes from corrupting it. On first run (no lock file),
# skip -lockfile=readonly so Terraform can generate the lock file.
export TF_DATA_DIR="$ENV_DIR/.terraform"
cd "$ROOT_DIR"

INIT_CMD=(
  terraform
  init
  -input=false
  -reconfigure
  -backend-config="path=$ENV_DIR/terraform.tfstate"
)
if [ -f .terraform.lock.hcl ]; then
  INIT_CMD+=(-lockfile=readonly)
fi

# Serialize terraform init per root directory — concurrent inits in the same
# working directory corrupt provider resolution even with isolated TF_DATA_DIR.
# Use mkdir as a portable lock (atomic on all POSIX systems including macOS).
INIT_LOCK="$ROOT_DIR/.terraform-init.lock.d"
_unlock_init() { rmdir "$INIT_LOCK" 2>/dev/null || true; }
while ! mkdir "$INIT_LOCK" 2>/dev/null; do sleep 0.2; done
trap _unlock_init EXIT
echo "+ ${INIT_CMD[*]}"
"${INIT_CMD[@]}" >/dev/null
_unlock_init
trap - EXIT

VAR_ARGS=()
for tfvars in auth.auto.tfvars env.auto.tfvars abac.auto.tfvars; do
  if [ -f "$ENV_DIR/$tfvars" ]; then
    VAR_ARGS+=(-var-file="$ENV_DIR/$tfvars")
  fi
done

if [ "$LAYER" != "account" ]; then
  VAR_ARGS+=(-var="env_dir=$ENV_DIR")
fi

case "$COMMAND" in
  plan|apply|destroy|import)
    CMD=(terraform "$COMMAND" "${VAR_ARGS[@]}" "$@")
    ;;
  state-list)
    CMD=(terraform state list "$@")
    ;;
  state-show)
    CMD=(terraform state show "$@")
    ;;
  state-rm)
    CMD=(terraform state rm "$@")
    ;;
  state-mv)
    CMD=(terraform state mv "$@")
    ;;
  output)
    CMD=(terraform output "$@")
    ;;
  print-cmd)
    printf 'terraform %s (in %s, TF_DATA_DIR=%s)' "$1" "$ROOT_DIR" "$TF_DATA_DIR"
    shift || true
    for arg in "${VAR_ARGS[@]}" "$@"; do
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

echo "+ ${CMD[*]}"
"${CMD[@]}"
