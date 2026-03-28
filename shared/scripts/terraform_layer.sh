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

# Create a per-env copy of the Terraform root so parallel scenarios don't
# corrupt each other's .terraform.lock.hcl (which is always written to the
# working directory, ignoring TF_DATA_DIR). Each scenario gets its own
# isolated working directory with symlinked .tf files.
WORK_DIR="$ENV_DIR/.tf_workdir"
mkdir -p "$WORK_DIR"
for tf_file in "$ROOT_DIR"/*.tf "$ROOT_DIR"/*.tf.json; do
  [ -e "$tf_file" ] && ln -sf "$tf_file" "$WORK_DIR/" 2>/dev/null
done
# Copy (not symlink) the lock file so concurrent writes don't collide
[ -f "$ROOT_DIR/.terraform.lock.hcl" ] && cp -n "$ROOT_DIR/.terraform.lock.hcl" "$WORK_DIR/" 2>/dev/null
# Link module directories if they exist
for subdir in "$ROOT_DIR"/*/; do
  [ -d "$subdir" ] && ln -sfn "$subdir" "$WORK_DIR/" 2>/dev/null
done

export TF_DATA_DIR="$ENV_DIR/.terraform"
cd "$WORK_DIR"

INIT_CMD=(
  terraform
  init
  -input=false
  -reconfigure
  -backend-config="path=$ENV_DIR/terraform.tfstate"
)

echo "+ ${INIT_CMD[*]}"
"${INIT_CMD[@]}" >/dev/null

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
