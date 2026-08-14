#!/usr/bin/env bash
# Promote Genie spaces from a dev workspace to a prod workspace via DABs.
#
# Dry run by default: exports from dev, rewrites catalog references, and shows
# the prod plan without writing anything to prod. Pass --apply to deploy.
#
# Most people should run ./promote instead, which asks for these values and
# calls this script. This script is the non-interactive entrypoint, suitable
# for CI later.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

APPLY=0
# In CI there is no ~/.databrickscfg, so passing `-p <profile>` makes every call
# fail. Set a profile to the empty string (DEV_PROFILE= PROD_PROFILE=) and the
# flag is omitted, letting the CLI authenticate from DATABRICKS_HOST plus
# DATABRICKS_CLIENT_ID/DATABRICKS_CLIENT_SECRET or DATABRICKS_TOKEN instead.
DEV_PROFILE="${DEV_PROFILE-dev}"
PROD_PROFILE="${PROD_PROFILE-prod}"
SPACES_FILE="${SPACES_FILE:-spaces.yml}"
SKIP_TABLE_CHECK=0
ASSUME_YES=0
ONLY_KEYS="${ONLY_KEYS:-}"

MIN_CLI_VERSION="1.3.0"

usage() {
  cat <<'EOF'
Usage: ./promote_genie.sh [options]

  --apply                Deploy to prod. Without this, nothing is written to prod.
  --dev-profile NAME     CLI profile for the dev workspace   (default: dev)
  --prod-profile NAME    CLI profile for the prod workspace  (default: prod)
  --spaces FILE          Config file                         (default: spaces.yml)
  --skip-table-check     Skip verifying prod tables are readable
  --only KEY[,KEY...]    Promote only these resource keys from the config
  -y, --yes              Overwrite prod-only changes without confirming
  -h, --help             Show this help

Phases: preflight -> export from dev -> rewrite catalogs -> bind -> diff ->
        plan/deploy
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply) APPLY=1; shift ;;
    --dev-profile) DEV_PROFILE="$2"; shift 2 ;;
    --prod-profile) PROD_PROFILE="$2"; shift 2 ;;
    --spaces) SPACES_FILE="$2"; shift 2 ;;
    --skip-table-check) SKIP_TABLE_CHECK=1; shift ;;
    --yes|-y) ASSUME_YES=1; shift ;;
    --only) ONLY_KEYS="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

# --- output helpers -----------------------------------------------------------

if [[ -t 1 ]]; then
  BOLD=$'\033[1m'; RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'
  BLUE=$'\033[34m'; DIM=$'\033[2m'; RESET=$'\033[0m'
else
  BOLD=""; RED=""; GREEN=""; YELLOW=""; BLUE=""; DIM=""; RESET=""
fi

step()  { printf '\n%s==> %s%s\n' "$BOLD$BLUE" "$*" "$RESET"; }
ok()    { printf '%s  ok%s %s\n' "$GREEN" "$RESET" "$*"; }
warn()  { printf '%s  warning%s %s\n' "$YELLOW" "$RESET" "$*"; }
fail()  { printf '%s  error%s %s\n' "$RED" "$RESET" "$*" >&2; }
die()   { fail "$*"; exit 1; }

# --- preflight ----------------------------------------------------------------

step "Preflight"

command -v databricks >/dev/null 2>&1 \
  || die "databricks CLI not found. Install: brew tap databricks/tap && brew install databricks"

command -v python3 >/dev/null 2>&1 || die "python3 not found."

CLI_VERSION="$(databricks --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)"
[[ -n "$CLI_VERSION" ]] || die "could not determine databricks CLI version"

# Genie spaces are not a bundle resource before CLI 1.3.0.
if [[ "$(printf '%s\n%s\n' "$MIN_CLI_VERSION" "$CLI_VERSION" | sort -V | head -1)" != "$MIN_CLI_VERSION" ]]; then
  die "databricks CLI $CLI_VERSION is too old. Genie spaces need >= $MIN_CLI_VERSION.
        Upgrade: brew upgrade databricks"
fi
ok "databricks CLI $CLI_VERSION"

# The Terraform engine has no Genie space implementation at all.
if grep -qE '^\s*engine:\s*terraform' databricks.yml 2>/dev/null; then
  die "databricks.yml sets engine: terraform, which cannot deploy Genie spaces.
        Remove that line to use the default 'direct' engine."
fi
ok "deployment engine: direct"

[[ -f "$SPACES_FILE" ]] || die "$SPACES_FILE not found. Run ./promote to create it."

if grep -q 'REPLACE' databricks.yml; then
  die "databricks.yml still has REPLACE placeholders. Run ./promote to fill them in."
fi

# An empty profile means "authenticate from the environment", so the flag has to
# disappear entirely rather than be passed as -p "".
DEV_P=(); [[ -n "$DEV_PROFILE" ]] && DEV_P=(-p "$DEV_PROFILE")
PROD_P=(); [[ -n "$PROD_PROFILE" ]] && PROD_P=(-p "$PROD_PROFILE")

# Python helpers take --profile; an empty value means "use the environment".
DEV_PY=(--profile "$DEV_PROFILE")
PROD_PY=(--profile "$PROD_PROFILE")

check_auth() {
  local role="$1" profile="$2"
  shift 2
  local label="profile '$profile'"
  [[ -n "$profile" ]] || label="$role (from the environment)"

  if ! databricks current-user me "$@" >/dev/null 2>&1; then
    if [[ -n "$profile" ]]; then
      die "$label cannot authenticate.
        Fix: databricks auth login --profile $profile"
    fi
    die "cannot authenticate for $role from the environment.
        Set DATABRICKS_HOST plus DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET
        (or DATABRICKS_TOKEN), or name a profile with --${role}-profile."
  fi

  local identity
  identity="$(databricks current-user me "$@" 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("userName","?"))' 2>/dev/null || echo '?')"
  ok "$label -> $identity"
}

check_auth dev "$DEV_PROFILE" ${DEV_P[@]+"${DEV_P[@]}"}
check_auth prod "$PROD_PROFILE" ${PROD_P[@]+"${PROD_P[@]}"}

# Parse spaces.yml once into shell-readable lines: key<TAB>dev_id<TAB>prod_id
SPACES_TSV="$(python3 lib/read_config.py "$SPACES_FILE" --spaces)"

# spaces.yml keeps every space ever configured so their prod links survive. When
# ./promote wrote a selection for this run, act only on those keys.
if [[ -z "$ONLY_KEYS" && -f .promote-selection ]]; then
  ONLY_KEYS="$(tr '\n' ',' < .promote-selection | sed 's/,$//')"
fi

if [[ -n "$ONLY_KEYS" ]]; then
  SPACES_TSV="$(printf '%s\n' "$SPACES_TSV" | awk -F'\037' -v keys=",$ONLY_KEYS," '
    { if (index(keys, "," $1 ",")) print }
  ')"
  [[ -n "$SPACES_TSV" ]] \
    || die "none of the requested spaces ($ONLY_KEYS) are in $SPACES_FILE"
fi

# databricks.yml includes resources/*.yml, so a file left over from a space that
# is not part of this run would still be deployed. Clear the generated
# directories and re-export only what was asked for.
rm -f resources/*.genie_space.yml src/*.geniespace.json 2>/dev/null || true
[[ -n "$SPACES_TSV" ]] || die "no spaces listed in $SPACES_FILE"

if ! printf '%s\n' "$SPACES_TSV" | awk -F'\037' '$4 != "" {found=1} END {exit !found}'; then
  warn "no catalog_map entries for any space; table names will not be rewritten"
fi

space_count="$(printf '%s\n' "$SPACES_TSV" | wc -l | tr -d ' ')"
ok "$space_count space(s) to promote"

# --- export from dev ----------------------------------------------------------

step "Export from dev (${DEV_PROFILE:-environment auth})"

while IFS=$'\x1f' read -r key dev_id prod_id space_map; do
  [[ -n "$key" ]] || continue
  printf '  %s%s%s (dev id %s)\n' "$BOLD" "$key" "$RESET" "$dev_id"
  if ! databricks bundle generate genie-space \
        --existing-id "$dev_id" --key "$key" --force \
        -t dev ${DEV_P[@]+"${DEV_P[@]}"} 2>&1 | sed 's/^/    /'; then
    die "export failed for $key. Check the dev space ID and your permissions on it."
  fi
done <<< "$SPACES_TSV"

ok "exported to resources/ and src/"

# Scope every plan and deploy to exactly the spaces in this run. Without this,
# a space the bundle manages but the run excludes has no local definition, which
# reads as "deleted" and would trash a live prod space.
#
# --select also means an unselected space is never sent to the API at all: not
# created, not updated, not deleted. Matching its config to prod is not enough on
# its own, because the bundle stores an etag per space and a space with no stored
# etag plans as an update even when every other field is byte-identical.
SELECT_ARGS=()
while IFS=$'\x1f' read -r key dev_id prod_id space_map; do
  [[ -n "$key" ]] && SELECT_ARGS+=(--select "genie_spaces.${key}")
done <<< "$SPACES_TSV"

# A definition must still exist on disk for every space the bundle tracks, or
# `bundle plan --select` errors with "no such resource" when the bundle loads.
if [[ -n "$ONLY_KEYS" ]]; then
  ALL_SPACES="$(python3 lib/read_config.py "$SPACES_FILE" --spaces)"
  TRACKED="$(python3 lib/binding_state.py --target prod "${PROD_PY[@]}" 2>/dev/null || true)"

  preserved=0
  while IFS=$'\x1f' read -r key dev_id prod_id space_map; do
    [[ -n "$key" ]] || continue
    # Already part of this run?
    printf '%s\n' "$SPACES_TSV" | awk -F'\037' -v k="$key" '$1==k {found=1} END {exit !found}' \
      && continue

    # Only spaces the bundle actually tracks in prod are at risk of deletion.
    printf '%s\n' "$TRACKED" \
      | awk -F'\t' -v k="$key" '$1==k && $2=="bound" && $3!="" {found=1} END {exit !found}' \
      || continue

    if [[ "$preserved" -eq 0 ]]; then
      step "Preserve spaces this run does not touch"
    fi

    # Take the definition from PROD, not dev. Re-exporting from dev would push
    # dev's current content -- including unmapped dev table names -- into prod,
    # which is the opposite of "not touching this space".
    if python3 lib/fetch_prod_space.py "$key" "${PROD_PY[@]}" \
         >/dev/null 2>&1; then
      printf '  %s%s%s %skept as it is in prod%s\n' \
        "$BOLD" "$key" "$RESET" "$DIM" "$RESET"
    else
      warn "could not read $key from prod; it is excluded from this run and the
          plan may show a delete. Include it in the run, or unbind it."
    fi
    preserved=$((preserved + 1))
  done <<< "$ALL_SPACES"
fi

# --- rewrite catalog references ----------------------------------------------

step "Rewrite dev catalog references"

while IFS=$'\x1f' read -r key dev_id prod_id space_map; do
  [[ -n "$key" ]] || continue
  space_file="src/${key}.geniespace.json"
  [[ -f "$space_file" ]] || die "expected $space_file after export but it is missing"

  # Each space carries its own mapping (read_config falls back to the top-level
  # one when a space defines none), since different spaces read different
  # catalogs.
  space_args=()
  if [[ -n "$space_map" ]]; then
    while IFS= read -r pair; do
      [[ -n "$pair" ]] && space_args+=(--map "$pair")
    done < <(printf '%s\n' "$space_map" | tr ';' '\n')
  fi

  printf '  %s%s%s\n' "$BOLD" "$key" "$RESET"
  if [[ ${#space_args[@]} -eq 0 ]]; then
    printf '    %sno mapping for this space; table names left as they are%s\n' \
      "$DIM" "$RESET"
    continue
  fi

  python3 lib/rewrite_catalog.py "$space_file" "${space_args[@]}" --in-place \
    || die "catalog rewrite failed for $key (see the unmapped references above)"
done <<< "$SPACES_TSV"

# Point the generated YAML at target variables so prod values apply.
step "Parameterize warehouse and folder per target"
while IFS=$'\x1f' read -r key dev_id prod_id space_map; do
  [[ -n "$key" ]] || continue
  python3 lib/parameterize.py "resources/${key}.genie_space.yml" \
    || die "could not parameterize resources/${key}.genie_space.yml"
done <<< "$SPACES_TSV"
ok "warehouse_id and parent_path now read from the target"

# --- verify prod tables are readable -----------------------------------------

if [[ "$SKIP_TABLE_CHECK" -eq 0 ]]; then
  step "Check prod tables are readable"
  prod_warehouse="$(python3 lib/read_config.py databricks.yml --warehouse prod)"
  if [[ -z "$prod_warehouse" || "$prod_warehouse" == *REPLACE* ]]; then
    warn "no prod warehouse_id in databricks.yml; skipping table check"
  else
    all_tables="$(while IFS=$'\x1f' read -r key dev_id prod_id space_map; do
        [[ -n "$key" ]] && python3 lib/rewrite_catalog.py "src/${key}.geniespace.json" --list-tables
      done <<< "$SPACES_TSV" | sort -u)"

    if [[ -z "$all_tables" ]]; then
      warn "no table names found in the exported spaces"
    else
      unreadable=0
      while IFS= read -r table; do
        [[ -n "$table" ]] || continue
        if databricks api post /api/2.0/sql/statements \
              ${PROD_P[@]+"${PROD_P[@]}"} --json "{
                \"warehouse_id\": \"$prod_warehouse\",
                \"statement\": \"SELECT 1 FROM $table LIMIT 0\",
                \"wait_timeout\": \"30s\"
              }" 2>/dev/null | grep -q '"state":"SUCCEEDED"'; then
          ok "$table"
        else
          fail "$table not readable in prod"
          unreadable=$((unreadable + 1))
        fi
      done <<< "$all_tables"

      if [[ "$unreadable" -gt 0 ]]; then
        warn "$unreadable table(s) unreadable. The spaces will deploy but cannot answer
          questions about those tables until the tables exist and the deploying
          principal has SELECT on them."
      fi
    fi
  fi
fi

# --- bind spaces that already exist in prod ----------------------------------

step "Bind pre-existing prod spaces"

# What the bundle's deployment state already tracks. Read from the workspace, so
# a promotion done earlier from another machine is still recognised here.
ALREADY_BOUND="$(python3 lib/binding_state.py --target prod "${PROD_PY[@]}" 2>/dev/null || true)"

bound_any=0
while IFS=$'\x1f' read -r key dev_id prod_id space_map; do
  [[ -n "$key" ]] || continue

  tracked_id="$(printf '%s\n' "$ALREADY_BOUND" \
    | awk -F'\t' -v k="$key" '$1==k && $2=="bound" {print $3}')"

  if [[ -n "$tracked_id" ]]; then
    printf '  %s%s%s %salready linked to %s -> updated in place, no bind needed%s\n' \
      "$BOLD" "$key" "$RESET" "$DIM" "$tracked_id" "$RESET"
    if [[ -n "$prod_id" && "$prod_id" != "$tracked_id" ]]; then
      die "$key is already linked to prod space $tracked_id but spaces.yml says
        prod_id: $prod_id. Remove the prod_id line, or unbind first:
          databricks bundle deployment unbind $key -t prod -p $PROD_PROFILE"
    fi
    continue
  fi

  if [[ -z "$prod_id" ]]; then
    printf '  %s%s%s %sno prod_id -> will be created new (no bind needed)%s\n' \
      "$BOLD" "$key" "$RESET" "$DIM" "$RESET"
    continue
  fi

  if [[ "$APPLY" -eq 0 ]]; then
    printf '  %s%s%s -> would bind to %s\n' "$BOLD" "$key" "$RESET" "$prod_id"
    bound_any=1
    continue
  fi

  printf '  %s%s%s -> binding to %s\n' "$BOLD" "$key" "$RESET" "$prod_id"
  warn "after binding, deploys overwrite the prod space with dev's content;
          any prod-side UI edits to '$key' are lost"
  if databricks bundle deployment bind "$key" "$prod_id" \
        --auto-approve -t prod ${PROD_P[@]+"${PROD_P[@]}"} 2>&1 | sed 's/^/    /'; then
    bound_any=1
  else
    die "bind failed for $key. Verify prod_id $prod_id exists in the prod workspace."
  fi
done <<< "$SPACES_TSV"

[[ "$bound_any" -eq 1 ]] || ok "nothing to bind"

# --- validate, then plan or deploy -------------------------------------------

step "Validate bundle"
databricks bundle validate -t prod ${PROD_P[@]+"${PROD_P[@]}"} 2>&1 | sed 's/^/  /' \
  || die "bundle validation failed"

step "Plan for prod"
PLAN_OUT="$(databricks bundle plan -t prod ${PROD_P[@]+"${PROD_P[@]}"} \
  ${SELECT_ARGS[@]+"${SELECT_ARGS[@]}"} 2>&1)" \
  || { printf '%s\n' "$PLAN_OUT" | sed 's/^/  /'; die "bundle plan failed"; }
printf '%s\n' "$PLAN_OUT" | sed 's/^/  /'

# Promoting a subset must never remove a space. A delete here means a resource
# the bundle manages has no local definition, which would trash a live prod
# space, so stop rather than let it through.
if printf '%s\n' "$PLAN_OUT" | grep -qE '^\s*delete genie_spaces\.'; then
  printf '\n'
  printf '%s  error%s the plan would DELETE a Genie space in prod:%s\n' \
    "$RED" "$RESET" ""
  printf '%s\n' "$PLAN_OUT" | grep -E '^\s*delete genie_spaces\.' | sed 's/^/    /'
  die "refusing to continue. A space the bundle manages has no local definition.
        Re-run ./promote and include that space, or unbind it first:
          databricks bundle deployment unbind <key> -t prod -p $PROD_PROFILE"
fi

# `bundle plan` says a space will be updated but not what differs inside it.
# Since a deploy replaces serialized_space wholesale, show the field-level diff
# for every space that already exists in prod.
step "What will change inside each space"

DRIFT_SPACES=""
while IFS=$'\x1f' read -r key dev_id prod_id space_map; do
  [[ -n "$key" ]] || continue

  target_id="$(printf '%s\n' "$ALREADY_BOUND" \
    | awk -F'\t' -v k="$key" '$1==k && $2=="bound" {print $3}')"
  [[ -n "$target_id" ]] || target_id="$prod_id"

  if [[ -z "$target_id" ]]; then
    printf '  %s%s%s %snew in prod, nothing to compare%s\n' \
      "$BOLD" "$key" "$RESET" "$DIM" "$RESET"
    continue
  fi

  printf '  %s%s%s vs prod space %s\n' "$BOLD" "$key" "$RESET" "$target_id"
  set +e
  diff_out="$(python3 lib/space_diff.py "src/${key}.geniespace.json" \
    "$target_id" "${PROD_PY[@]}" 2>&1)"
  diff_rc=$?
  set -e
  printf '%s\n' "$diff_out" | sed 's/^/  /'

  # Exit 3 means differences were found; anything containing prod-only items
  # means this deploy would destroy work done in the prod UI.
  if [[ "$diff_rc" -eq 3 ]] && printf '%s' "$diff_out" | grep -q 'in prod only\|present in prod'; then
    DRIFT_SPACES="$DRIFT_SPACES $key"
  fi
done <<< "$SPACES_TSV"

if [[ -n "$DRIFT_SPACES" ]]; then
  printf '\n%s  warning%s these spaces have content that exists only in prod:%s\n' \
    "$YELLOW" "$RESET" ""
  for key in $DRIFT_SPACES; do
    printf '    %s\n' "$key"
  done
  printf '  Someone changed them in prod outside the bundle. Deploying replaces\n'
  printf '  that content with your local definition.\n'
fi

if [[ "$APPLY" -eq 0 ]]; then
  printf '\n%sDry run complete. Nothing was written to prod.%s\n' "$BOLD" "$RESET"
  printf 'Re-run with %s--apply%s to deploy.\n' "$BOLD" "$RESET"
  exit 0
fi

# Overwriting prod-only content is not something to do on a flag alone.
if [[ -n "$DRIFT_SPACES" && "$ASSUME_YES" -eq 0 ]]; then
  if [[ ! -t 0 ]]; then
    die "prod has out-of-band changes and this is not an interactive shell.
        Re-run with --yes to overwrite them deliberately."
  fi
  printf '\n'
  read -r -p "  Overwrite the prod-only content listed above? [y/N]: " reply
  case "$reply" in
    y|Y|yes|YES) ;;
    *) die "stopped; nothing was written to prod" ;;
  esac
fi

step "Deploy to prod"
databricks bundle deploy -t prod ${PROD_P[@]+"${PROD_P[@]}"} \
  ${SELECT_ARGS[@]+"${SELECT_ARGS[@]}"} --auto-approve 2>&1 | sed 's/^/  /' \
  || die "deploy failed"

printf '\n%sDone.%s Genie spaces deployed to prod.\n' "$BOLD$GREEN" "$RESET"
printf 'Next: open each space in prod and ask a question to confirm it returns prod data.\n'
printf 'Commit resources/ and src/ to git so the promotion is reproducible.\n'
