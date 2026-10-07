#!/usr/bin/env bash
# =============================================================================
# Genie agent: create / update-config / set-acls / trash
# =============================================================================
# Commands:
#   create        Create a minimal Genie agent (tables + warehouse + title).
#                 Wildcards (catalog.schema.*) are expanded via UC Tables API.
#   update-config Update a Genie agent's full configuration via PATCH API.
#                 Reads space_id from GENIE_ID_FILE.
#   set-acls      Set CAN_RUN on a Genie agent for the configured groups.
#                 Reads space_id from GENIE_SPACE_OBJECT_ID or GENIE_ID_FILE.
#   trash         Move a Genie agent to trash. Reads space_id from GENIE_ID_FILE.
#
# Authentication (in order of precedence):
#   1. DATABRICKS_TOKEN (PAT) - if set, used directly
#   2. DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET (Service Principal OAuth M2M)
#      - Requires DATABRICKS_HOST to be set for token endpoint
#
# Configuration:
#   GENIE_GROUPS_CSV     Required for create/set-acls. Comma-separated group names.
#   GENIE_TABLES_CSV     Required for create. Comma-separated fully-qualified
#                        table names (catalog.schema.table). Wildcards (catalog.schema.*)
#                        are expanded via the UC Tables API.
#   GENIE_WAREHOUSE_ID   Warehouse ID for create. Falls back to sql_warehouse_id
#                        in env.auto.tfvars if not set.
#   GENIE_TITLE          Optional. Title for the new Genie agent (default: "ABAC Genie Space").
#   GENIE_DESCRIPTION    Optional. Description for the new Genie agent.
#   GENIE_SAMPLE_QUESTIONS  Optional. JSON array of sample question strings.
#   GENIE_INSTRUCTIONS   Optional. Text instructions for the Genie LLM.
#   GENIE_BENCHMARKS     Optional. JSON array of {question, sql} objects.
#   GENIE_SQL_FILTERS    Optional. JSON array of {sql, display_name, comment, instruction}.
#   GENIE_SQL_EXPRESSIONS Optional. JSON array of {alias, sql, display_name, comment, instruction}.
#   GENIE_SQL_MEASURES   Optional. JSON array of {alias, sql, display_name, comment, instruction}.
#   GENIE_JOIN_SPECS     Optional. JSON array of {left_table, left_alias, right_table, right_alias, sql, comment, instruction}.
#   GENIE_ID_FILE        Optional. File path to save the created space ID
#                        (used by Terraform for lifecycle management).
#   GENIE_ADOPT_REQUIRED Optional. 1 = create must adopt the agent in
#                        GENIE_ID_FILE (HTTP 200 only) and never create one.
#
# Usage:
#   ./genie_space.sh create [workspace_url] [token] [title] [warehouse_id]
#   ./genie_space.sh set-acls [workspace_url] [token] [space_id]
#   ./genie_space.sh trash
#
# Or set env and run: ./genie_space.sh create   or   ./genie_space.sh set-acls
# Re-running create adopts the agent already named in GENIE_ID_FILE.
# If that ID is absent or gone, create adopts the single target-workspace agent
# whose title exactly matches (case-sensitively); ambiguous matches fail closed.
# =============================================================================

set -e

UA_HEADER="User-Agent: genierails/0.1.0"

usage() {
  echo "Usage: $0 create [workspace_url] [token] [title] [warehouse_id]"
  echo "       $0 set-acls [workspace_url] [token] [space_id]"
  echo "       $0 trash"
  echo "  Or set DATABRICKS_HOST + DATABRICKS_TOKEN (or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET)"
  echo "  For create: set GENIE_WAREHOUSE_ID; for set-acls: set GENIE_SPACE_OBJECT_ID"
  exit 1
}

# ---------- Get OAuth token from Service Principal credentials ----------
get_sp_token() {
  local workspace_url="$1"
  local client_id="$2"
  local client_secret="$3"
  workspace_url="${workspace_url%/}"

  local token_endpoint="${workspace_url}/oidc/v1/token"

  local response
  response=$(curl -s -w "\n%{http_code}" -X POST \
    -H "${UA_HEADER}" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=client_credentials&client_id=${client_id}&client_secret=${client_secret}&scope=all-apis" \
    "${token_endpoint}")

  local http_code
  http_code=$(echo "$response" | tail -n1)
  local response_body
  response_body=$(echo "$response" | sed '$d')

  if [[ "$http_code" != "200" ]]; then
    echo "Failed to get OAuth token (HTTP ${http_code}). Check client_id/client_secret and workspace URL." >&2
    echo "Response: ${response_body}" >&2
    return 1
  fi

  local token
  token=$(echo "$response_body" | grep -o '"access_token"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"\([^"]*\)"$/\1/')
  if [[ -z "$token" ]]; then
    token=$(echo "$response_body" | jq -r '.access_token // empty' 2>/dev/null)
  fi

  if [[ -z "$token" ]]; then
    echo "Could not parse access_token from OAuth response." >&2
    return 1
  fi

  echo "$token"
}

# ---------- Resolve token: use DATABRICKS_TOKEN or get from SP credentials ----------
resolve_token() {
  local workspace_url="$1"
  local explicit_token="$2"

  if [[ -n "$explicit_token" ]]; then
    echo "$explicit_token"
    return 0
  fi

  if [[ -n "${DATABRICKS_TOKEN:-}" ]]; then
    echo "$DATABRICKS_TOKEN"
    return 0
  fi

  if [[ -n "${DATABRICKS_CLIENT_ID:-}" && -n "${DATABRICKS_CLIENT_SECRET:-}" ]]; then
    echo "Using Service Principal OAuth M2M authentication..." >&2
    get_sp_token "$workspace_url" "$DATABRICKS_CLIENT_ID" "$DATABRICKS_CLIENT_SECRET"
    return $?
  fi

  echo "No authentication found. Set DATABRICKS_TOKEN or DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET." >&2
  return 1
}

# ---------- Read sql_warehouse_id from env.auto.tfvars (fallback) ----------
read_warehouse_from_tfvars() {
  # Try ENV_DIR first (set by Makefile), then fall back to script-relative path.
  local tfvars=""
  if [[ -n "${ENV_DIR:-}" && -f "${ENV_DIR}/env.auto.tfvars" ]]; then
    tfvars="${ENV_DIR}/env.auto.tfvars"
  else
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    tfvars="${script_dir}/../env.auto.tfvars"
  fi
  if [[ -f "$tfvars" ]]; then
    grep -E '^\s*sql_warehouse_id\s*=' "$tfvars" \
      | sed 's/.*=\s*"\(.*\)".*/\1/' \
      | head -1
  fi
}

# ---------- Expand wildcard table entries via UC Tables API ----------
expand_tables() {
  local workspace_url="$1"
  local token="$2"
  local tables_csv="$3"
  workspace_url="${workspace_url%/}"

  IFS=',' read -ra RAW_ENTRIES <<< "$tables_csv"
  local expanded=()

  for entry in "${RAW_ENTRIES[@]}"; do
    entry="${entry#"${entry%%[![:space:]]*}"}"
    entry="${entry%"${entry##*[![:space:]]}"}"
    if [[ "$entry" == *.* && "$entry" == *.\* ]]; then
      # Wildcard: catalog.schema.*
      local catalog schema
      catalog=$(echo "$entry" | cut -d. -f1)
      schema=$(echo "$entry" | cut -d. -f2)
      echo "Expanding wildcard ${entry} via UC Tables API..." >&2

      local api_url="${workspace_url}/api/2.1/unity-catalog/tables?catalog_name=${catalog}&schema_name=${schema}"
      local resp
      resp=$(curl -s -H "${UA_HEADER}" -H "Authorization: Bearer ${token}" "${api_url}")

      local table_names
      table_names=$(echo "$resp" | grep -o '"full_name"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"\([^"]*\)"$/\1/')
      if [[ -z "$table_names" ]]; then
        table_names=$(echo "$resp" | jq -r '.tables[]?.full_name // empty' 2>/dev/null)
      fi

      if [[ -z "$table_names" ]]; then
        echo "WARNING: No tables found for ${catalog}.${schema}.* — skipping wildcard." >&2
        continue
      fi

      while IFS= read -r tbl; do
        [[ -n "$tbl" ]] && expanded+=("$tbl")
      done <<< "$table_names"
      echo "  Expanded to ${#expanded[@]} table(s) from ${catalog}.${schema}" >&2
    else
      expanded+=("$entry")
    fi
  done

  local IFS=','
  echo "${expanded[*]}"
}

# Print IDs of all agents whose title exactly (case-sensitively) matches $3.
# Listing is deliberately fail-closed: callers must not POST if any page cannot
# be fetched or parsed, because an unseen page could contain the same agent.
matching_space_ids_by_title() {
  local workspace_url="$1" token="$2" title="$3"
  local page_token="" response http_code response_body parsed next_token
  local page_count=0 matches_file seen_tokens_file
  matches_file=$(mktemp)
  seen_tokens_file=$(mktemp)

  while true; do
    page_count=$((page_count + 1))
    if [[ $page_count -gt 1000 ]]; then
      rm -f "$matches_file" "$seen_tokens_file"
      echo "ERROR: Genie agent listing exceeded 1000 pages; refusing to create because duplicate detection could not complete." >&2
      return 1
    fi
    local curl_args=(-s -w "\n%{http_code}" -G
      -H "${UA_HEADER}" -H "Authorization: Bearer ${token}")
    if [[ -n "$page_token" ]]; then
      curl_args+=(--data-urlencode "page_token=${page_token}")
    fi
    response=$(curl "${curl_args[@]}" "${workspace_url}/api/2.0/genie/spaces") || {
      rm -f "$matches_file" "$seen_tokens_file"
      echo "ERROR: Could not list Genie agents in ${workspace_url}; refusing to create because duplicate detection failed." >&2
      return 1
    }
    http_code=$(printf '%s\n' "$response" | tail -n1)
    response_body=$(printf '%s\n' "$response" | sed '$d')
    if [[ "$http_code" != "200" ]]; then
      rm -f "$matches_file" "$seen_tokens_file"
      echo "ERROR: Could not list Genie agents in ${workspace_url} (HTTP ${http_code}); refusing to create because duplicate detection failed." >&2
      return 1
    fi

    parsed=$(TITLE_TO_MATCH="$title" python3 -c '
import json, os, sys
try:
    body = json.load(sys.stdin)
    if not isinstance(body, dict) or not isinstance(body.get("spaces", []), list):
        raise ValueError("response must contain a spaces list")
    for space in body.get("spaces", []):
        if not isinstance(space, dict):
            raise ValueError("spaces entries must be objects")
        space_id, space_title = space.get("space_id"), space.get("title")
        if not isinstance(space_id, str) or not isinstance(space_title, str):
            raise ValueError("each space must contain string space_id and title")
        if space_title == os.environ["TITLE_TO_MATCH"]:
            print("MATCH\t" + space_id)
    token = body.get("next_page_token", "")
    if token is None:
        token = ""
    if not isinstance(token, str):
        raise ValueError("next_page_token must be a string")
    print("NEXT\t" + token)
except (ValueError, json.JSONDecodeError) as exc:
    print(f"Malformed Genie list response: {exc}", file=sys.stderr)
    sys.exit(1)
' <<< "$response_body") || {
      rm -f "$matches_file" "$seen_tokens_file"
      echo "ERROR: Could not parse the Genie agent list from ${workspace_url}; refusing to create because duplicate detection failed." >&2
      return 1
    }
    printf '%s\n' "$parsed" | sed -n 's/^MATCH\t//p' >> "$matches_file"
    next_token=$(printf '%s\n' "$parsed" | sed -n 's/^NEXT\t//p' | tail -n1)
    [[ -z "$next_token" ]] && break
    if grep -Fqx -- "$next_token" "$seen_tokens_file"; then
      rm -f "$matches_file" "$seen_tokens_file"
      echo "ERROR: Genie agent listing repeated page token '${next_token}'; refusing to create because duplicate detection could not complete." >&2
      return 1
    fi
    printf '%s\n' "$next_token" >> "$seen_tokens_file"
    page_token="$next_token"
  done

  cat "$matches_file"
  rm -f "$matches_file" "$seen_tokens_file"
}

# ---------- Set ACLs on a Genie agent (CAN_RUN for configured groups) ----------
# Print the agent's current table identifiers as CSV.
current_space_tables() {
  local workspace_url="$1" token="$2" space_id="$3"
  local response
  response=$(curl -sf -H "${UA_HEADER}" -H "Authorization: Bearer ${token}" \
    "${workspace_url}/api/2.0/genie/spaces/${space_id}?include_serialized_space=true") || {
    echo "ERROR: could not read Genie agent ${space_id} to reuse its tables." >&2
    return 1
  }
  printf '%s' "$response" | python3 -c '
import json, sys
space = json.loads(json.load(sys.stdin).get("serialized_space") or "{}")
tables = (space.get("data_sources") or {}).get("tables") or []
print(",".join(t["identifier"] for t in tables if t.get("identifier")))
'
}

set_genie_acls() {
  local workspace_url="$1"
  local token="$2"
  local space_id="$3"
  workspace_url="${workspace_url%/}"

  IFS=',' read -ra GENIE_GROUPS <<< "${GENIE_GROUPS_CSV}"

  local access_control=""
  for g in "${GENIE_GROUPS[@]}"; do
    access_control="${access_control}{\"group_name\": \"${g}\", \"permission_level\": \"CAN_RUN\"},"
  done
  access_control="[${access_control%,}]"

  local body="{\"access_control_list\": ${access_control}}"
  local path="/api/2.0/permissions/genie/${space_id}"

  # PUT is intentionally authoritative. Read first so every direct grant that
  # config will remove is visible; inherited grants are not part of PUT.
  local current current_code current_body
  current=$(curl -s -w "\n%{http_code}" \
    -H "${UA_HEADER}" -H "Authorization: Bearer ${token}" \
    "${workspace_url}${path}") || current=""
  current_code=$(printf '%s\n' "$current" | tail -n1)
  current_body=$(printf '%s\n' "$current" | sed '$d')
  local acl_audit=""
  if [[ "$current_code" == "200" ]]; then
    if ! acl_audit=$(CONFIGURED_GROUPS="$GENIE_GROUPS_CSV" python3 -c '
import json, os, sys
configured = {g for g in os.environ.get("CONFIGURED_GROUPS", "").split(",") if g}
body = json.load(sys.stdin)
if not isinstance(body, dict) or not isinstance(body.get("access_control_list"), list):
    raise ValueError("response must contain access_control_list")
for ace in body["access_control_list"]:
    if not isinstance(ace, dict) or not isinstance(ace.get("all_permissions"), list):
        raise ValueError("each ACL entry must contain all_permissions")
    direct = [p for p in ace["all_permissions"] if isinstance(p, dict) and not p.get("inherited")]
    if not direct:
        continue
    fields = (("group", "group_name"), ("user", "user_name"), ("sp", "service_principal_name"))
    kind, field = next(((kind, field) for kind, field in fields if ace.get(field)), (None, None))
    if not field:
        raise ValueError("ACL entry has no supported principal")
    principal = ace[field]
    display = ace.get("display_name")
    label = f"{kind}:{principal}" + (f" [{display}]" if display and display != principal else "")
    levels = sorted({p.get("permission_level", "UNKNOWN") for p in direct})
    if kind == "group" and principal in configured:
        for level in levels:
            if level != "CAN_RUN":
                print(f"Changing configured Genie access: {label} ({level} -> CAN_RUN)")
        continue
    for level in levels:
        print(f"Removing hand-added Genie access: {label} ({level}) — not in config")
' <<< "$current_body"); then
      if [[ "${GENIE_ACL_FORCE:-0}" != "1" ]]; then
        echo "ERROR: Could not parse current Genie ACL for ${space_id}; refusing authoritative PUT because removals cannot be audited. Set GENIE_ACL_FORCE=1 to override." >&2
        exit 1
      fi
      echo "WARNING: GENIE_ACL_FORCE=1: current Genie ACL for ${space_id} could not be parsed; proceeding with authoritative PUT without a complete removal audit." >&2
    else
      [[ -n "$acl_audit" ]] && printf '%s\n' "$acl_audit"
    fi
  else
    if [[ "${GENIE_ACL_FORCE:-0}" != "1" ]]; then
      echo "ERROR: Could not read current Genie ACL for ${space_id} (HTTP ${current_code:-unknown}); refusing authoritative PUT because removals cannot be audited. Set GENIE_ACL_FORCE=1 to override." >&2
      exit 1
    fi
    echo "WARNING: GENIE_ACL_FORCE=1: current Genie ACL for ${space_id} returned HTTP ${current_code:-unknown}; proceeding with authoritative PUT without a complete removal audit." >&2
  fi

  echo "Putting permissions on Genie agent ${space_id} for groups: ${GENIE_GROUPS[*]}"
  local response
  response=$(curl -s -w "\n%{http_code}" -X PUT \
    -H "${UA_HEADER}" \
    -H "Authorization: Bearer ${token}" \
    -H "Content-Type: application/json" \
    -d "${body}" \
    "${workspace_url}${path}")

  local http_code
  http_code=$(echo "$response" | tail -n1)
  local response_body
  response_body=$(echo "$response" | sed '$d')

  if [[ "$http_code" != "200" && "$http_code" != "201" ]]; then
    echo "Request failed (HTTP ${http_code}). Check workspace URL, token, and Genie agent ID."
    echo "API response: ${response_body}"
    exit 1
  fi
  echo "Genie agent ACLs updated successfully."
}

# ---------- Create Genie agent (minimal: tables + warehouse + title) ----------
create_genie_space() {
  local workspace_url="$1"
  local token="$2"
  local title="${3:-${GENIE_TITLE:-ABAC Genie Space}}"
  local warehouse_id="$4"
  workspace_url="${workspace_url%/}"

  # Adopt the agent this ID file already names (e.g. after its Terraform
  # address changed) rather than creating a duplicate under a new ID. Only a
  # confirmed 404 leads to a new agent; any other error stops here.
  # Adoption required (GENIE_ADOPT_REQUIRED=1, or the marker that
  # genie_adopt_preflight.py --arm writes during the migration) accepts only
  # HTTP 200 and never creates.
  local adopt_marker="" adopted_marker="" adopt_required=""
  if [[ -n "${GENIE_ID_FILE:-}" ]]; then
    adopt_marker="$(dirname "${GENIE_ID_FILE}")/.genie_adopt_required_$(basename "${GENIE_ID_FILE}" | sed 's/^\.genie_space_id_//')"
    adopted_marker="$(dirname "${GENIE_ID_FILE}")/.genie_adopted_$(basename "${GENIE_ID_FILE}" | sed 's/^\.genie_space_id_//')"
  fi
  if [[ "${GENIE_ADOPT_REQUIRED:-}" == "1" || ( -n "$adopt_marker" && -f "$adopt_marker" ) ]]; then
    adopt_required=1
    if [[ -z "${GENIE_ID_FILE:-}" || ! -s "${GENIE_ID_FILE}" ]]; then
      echo "ERROR: Adoption required, but the Genie agent ID file ${GENIE_ID_FILE:-<unset>} is missing or empty; not creating a new agent." >&2
      echo "  Write the existing agent ID (it is in the agent URL) into that file, then run make genie-adopt-preflight ENV=<env>." >&2
      exit 1
    fi
  fi
  if [[ -n "${GENIE_ID_FILE:-}" && -s "${GENIE_ID_FILE}" ]]; then
    local existing_id existing_code
    existing_id=$(tr -d '[:space:]' < "${GENIE_ID_FILE}")
    existing_code=$(curl -s -o /dev/null -w "%{http_code}" \
      -H "${UA_HEADER}" \
      -H "Authorization: Bearer ${token}" \
      "${workspace_url}/api/2.0/genie/spaces/${existing_id}")
    if [[ "$existing_code" == "200" ]]; then
      echo "Genie agent ${existing_id} (from ${GENIE_ID_FILE}) already exists; adopting it, no new agent created."
      [[ -n "$adopt_marker" ]] && rm -f "$adopt_marker"
      echo "Done. Genie agent ID: ${existing_id}"
      return 0
    elif [[ -n "$adopt_required" ]]; then
      echo "ERROR: Adoption required, but Genie agent ${existing_id} from ${GENIE_ID_FILE} returned HTTP ${existing_code} on ${workspace_url}; not creating a new agent." >&2
      echo "  Check the agent exists in this workspace and the SP can read it, then run make genie-adopt-preflight ENV=<env>." >&2
      exit 1
    elif [[ "$existing_code" != "404" ]]; then
      echo "ERROR: Cannot check Genie agent ${existing_id} from ${GENIE_ID_FILE} (HTTP ${existing_code}); not creating a duplicate." >&2
      exit 1
    fi
    echo "Genie agent ${existing_id} from ${GENIE_ID_FILE} no longer exists (HTTP 404); checking exact-title matches before creating."
  fi

  local title_matches
  title_matches=$(matching_space_ids_by_title "$workspace_url" "$token" "$title") || exit 1
  local matching_ids=()
  while IFS= read -r matching_id; do
    [[ -n "$matching_id" ]] && matching_ids+=("$matching_id")
  done <<< "$title_matches"
  if [[ ${#matching_ids[@]} -eq 1 ]]; then
    if [[ -z "${GENIE_ID_FILE:-}" ]]; then
      echo "ERROR: Found existing Genie agent ${matching_ids[0]} titled \"${title}\", but GENIE_ID_FILE is unset; refusing to create a duplicate." >&2
      exit 1
    fi
    printf '%s\n' "${matching_ids[0]}" > "$GENIE_ID_FILE"
    [[ -n "$adopt_marker" ]] && rm -f "$adopt_marker"
    [[ -n "$adopted_marker" ]] && : > "$adopted_marker"
    echo "Adopted existing Genie agent ${matching_ids[0]} titled \"${title}\" instead of creating a duplicate"
    echo "Done. Genie agent ID: ${matching_ids[0]}"
    return 0
  elif [[ ${#matching_ids[@]} -gt 1 ]]; then
    local ids_display
    ids_display=$(printf '%s\n' "${matching_ids[@]}" | paste -sd, - | sed 's/,/, /g')
    echo "ERROR: Multiple Genie agents have the exact title \"${title}\": ${ids_display}. Refusing to create a duplicate; set genie_space_id to the ID to manage." >&2
    exit 1
  fi

  if [[ -z "${GENIE_TABLES_CSV:-}" ]]; then
    echo "ERROR: GENIE_TABLES_CSV not set. Pass comma-separated fully-qualified table names." >&2
    echo "  Example: GENIE_TABLES_CSV='cat.schema.t1,cat.schema.t2' $0 create" >&2
    exit 1
  fi

  local resolved_csv
  resolved_csv=$(expand_tables "$workspace_url" "$token" "$GENIE_TABLES_CSV")
  IFS=',' read -ra TABLE_LIST <<< "$resolved_csv"

  local sorted_identifiers=()
  while IFS= read -r id; do
    [[ -n "$id" ]] && sorted_identifiers+=("$id")
  done < <(printf '%s\n' "${TABLE_LIST[@]}" | LC_ALL=C sort)

  if [[ ${#sorted_identifiers[@]} -eq 0 ]]; then
    echo "ERROR: No tables resolved after wildcard expansion. Nothing to create." >&2
    exit 1
  fi

  local tables_csv
  tables_csv=$(IFS=','; echo "${sorted_identifiers[*]}")

  local create_body
  create_body=$(python3 - "$warehouse_id" "$title" "$tables_csv" << 'PYEOF'
import json
import sys

warehouse_id, title, tables_csv = sys.argv[1:]
tables = [{"identifier": t} for t in sorted(tables_csv.split(",")) if t]
space = {"version": 2, "data_sources": {"tables": tables}}
body = {
    "warehouse_id": warehouse_id,
    "title": title,
    "serialized_space": json.dumps(space, separators=(',', ':'))
}
print(json.dumps(body))
PYEOF
  )

  local tables_display
  tables_display=$(printf '%s\n' "${sorted_identifiers[@]}" | tr '\n' ' ')
  echo "Creating Genie agent '${title}' with warehouse ${warehouse_id} and ${#sorted_identifiers[@]} tables: ${tables_display}"

  local tmpfile
  tmpfile=$(mktemp)
  echo "$create_body" > "$tmpfile"

  local response
  response=$(curl -s -w "\n%{http_code}" -X POST \
    -H "${UA_HEADER}" \
    -H "Authorization: Bearer ${token}" \
    -H "Content-Type: application/json" \
    -d @"${tmpfile}" \
    "${workspace_url}/api/2.0/genie/spaces")
  rm -f "$tmpfile"

  local http_code
  http_code=$(echo "$response" | tail -n1)
  local response_body
  response_body=$(echo "$response" | sed '$d')

  if [[ "$http_code" != "200" && "$http_code" != "201" ]]; then
    echo "Create Genie agent failed (HTTP ${http_code})."
    echo "API response: ${response_body}"
    exit 1
  fi

  local space_id
  space_id=$(echo "$response_body" | grep -o '"space_id"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"\([^"]*\)"$/\1/')
  if [[ -z "$space_id" ]]; then
    space_id=$(echo "$response_body" | jq -r '.space_id // empty' 2>/dev/null)
  fi
  if [[ -z "$space_id" ]]; then
    echo "Created space but could not parse space_id from response. Response: ${response_body}"
    exit 1
  fi

  echo "Genie agent created: ${space_id}"

  if [[ -n "${GENIE_ID_FILE:-}" ]]; then
    echo "$space_id" > "$GENIE_ID_FILE"
    [[ -n "$adopted_marker" ]] && rm -f "$adopted_marker"
    echo "Space ID saved to ${GENIE_ID_FILE}"
  fi

  echo "Done. Genie agent ID: ${space_id}"
}

# ---------- Update Genie agent config via PATCH ----------
update_genie_config() {
  local workspace_url="${DATABRICKS_HOST}"
  workspace_url="${workspace_url%/}"

  if [[ -z "$workspace_url" ]]; then
    echo "Need workspace URL. Set DATABRICKS_HOST." >&2
    exit 1
  fi

  local token
  token=$(resolve_token "$workspace_url" "") || exit 1

  local space_id=""

  # Prefer GENIE_SPACE_OBJECT_ID (set directly for existing spaces);
  # fall back to reading from GENIE_ID_FILE (used for auto-created spaces).
  if [[ -n "${GENIE_SPACE_OBJECT_ID:-}" ]]; then
    space_id="${GENIE_SPACE_OBJECT_ID}"
  elif [[ -n "${GENIE_ID_FILE:-}" && -f "${GENIE_ID_FILE}" ]]; then
    space_id=$(cat "${GENIE_ID_FILE}" | tr -d '[:space:]')
  fi

  if [[ -z "$space_id" ]]; then
    echo "ERROR: No Genie agent ID available for update-config." >&2
    echo "  Set GENIE_SPACE_OBJECT_ID (for existing spaces) or ensure GENIE_ID_FILE exists (for auto-created spaces)." >&2
    exit 1
  fi

  # Only offer a warehouse change when config explicitly selected one, and
  # only when it differs from the live attachment. This keeps unrelated config
  # updates from resetting adopted or hand-adjusted agents.
  local adopted_marker=""
  if [[ -n "${GENIE_ID_FILE:-}" ]]; then
    adopted_marker="$(dirname "${GENIE_ID_FILE}")/.genie_adopted_$(basename "${GENIE_ID_FILE}" | sed 's/^\.genie_space_id_//')"
  fi
  if [[ -n "${GENIE_WAREHOUSE_ID:-}" ]] && { [[ "${GENIE_WAREHOUSE_EXPLICIT:-0}" == "1" ]] || { [[ "${GENIE_WAREHOUSE_CREATED_DEFAULT:-0}" == "1" ]] && [[ ! -f "$adopted_marker" ]]; }; }; then
    local current_agent current_warehouse
    current_agent=$(curl -sf -H "${UA_HEADER}" -H "Authorization: Bearer ${token}" \
      "${workspace_url}/api/2.0/genie/spaces/${space_id}") || {
      echo "ERROR: could not read Genie agent ${space_id} to compare its warehouse." >&2
      exit 1
    }
    current_warehouse=$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("warehouse_id", ""))' <<< "$current_agent") || exit 1
    if [[ "$current_warehouse" == "$GENIE_WAREHOUSE_ID" ]]; then
      unset GENIE_WAREHOUSE_ID
    fi
  else
    unset GENIE_WAREHOUSE_ID
  fi

  # An agent attached by genie_space_id alone keeps the tables it already has
  # (its footprint was discovered from it); only an explicit list replaces them.
  if [[ -z "${GENIE_TABLES_CSV:-}" ]]; then
    GENIE_TABLES_CSV=$(current_space_tables "$workspace_url" "$token" "$space_id") || exit 1
  fi
  if [[ -z "${GENIE_TABLES_CSV:-}" ]]; then
    echo "ERROR: GENIE_TABLES_CSV not set and Genie agent ${space_id} has no tables." >&2
    exit 1
  fi

  local resolved_csv
  resolved_csv=$(expand_tables "$workspace_url" "$token" "$GENIE_TABLES_CSV")

  build_patch_body() {
    local skip_join_specs="${1:-0}"
    local skip_title="${2:-0}"
    GENIE_SKIP_JOIN_SPECS="$skip_join_specs" GENIE_SKIP_TITLE="$skip_title" python3 << PYEOF
import json, random, datetime, os

def gen_id():
    t = int((datetime.datetime.now() - datetime.datetime(1582,10,15)).total_seconds() * 1e7)
    hi = (t & 0xFFFFFFFFFFFF0000) | (1 << 12) | ((t & 0xFFFF) >> 4)
    lo = random.getrandbits(62) | 0x8000000000000000
    return f"{hi:016x}{lo:016x}"

tables = [{"identifier": t} for t in sorted("${resolved_csv}".split(",")) if t]
space = {"version": 2, "data_sources": {"tables": tables}}

sq_json = os.environ.get("GENIE_SAMPLE_QUESTIONS", "")
if sq_json and sq_json != "[]":
    try:
        questions = json.loads(sq_json)
        if questions:
            items = [{"id": gen_id(), "question": [q]} for q in questions]
            items.sort(key=lambda x: x["id"])
            space.setdefault("config", {})["sample_questions"] = items
    except json.JSONDecodeError:
        pass

instr = os.environ.get("GENIE_INSTRUCTIONS", "")
if instr:
    space.setdefault("instructions", {})["text_instructions"] = [
        {"id": gen_id(), "content": [instr]}
    ]

bm_json = os.environ.get("GENIE_BENCHMARKS", "")
if bm_json and bm_json != "[]":
    try:
        benchmarks = json.loads(bm_json)
        if benchmarks:
            items = []
            for bm in benchmarks:
                items.append({
                    "id": gen_id(),
                    "question": [bm["question"]],
                    "answer": [{"format": "SQL", "content": [bm["sql"]]}]
                })
            items.sort(key=lambda x: x["id"])
            space["benchmarks"] = {"questions": items}
    except json.JSONDecodeError:
        pass

instructions = space.get("instructions", {})

filt_json = os.environ.get("GENIE_SQL_FILTERS", "")
if filt_json and filt_json != "[]":
    try:
        filters = json.loads(filt_json)
        if filters:
            items = [{"id": gen_id(), "sql": [f["sql"]], "display_name": f["display_name"]} for f in filters]
            items.sort(key=lambda x: x["id"])
            instructions.setdefault("sql_snippets", {})["filters"] = items
    except json.JSONDecodeError:
        pass

expr_json = os.environ.get("GENIE_SQL_EXPRESSIONS", "")
if expr_json and expr_json != "[]":
    try:
        expressions = json.loads(expr_json)
        if expressions:
            items = [{
                "id": gen_id(),
                "alias": e["alias"],
                "sql": [e["sql"]],
                "display_name": e["display_name"],
            } for e in expressions]
            items.sort(key=lambda x: x["id"])
            instructions.setdefault("sql_snippets", {})["expressions"] = items
    except json.JSONDecodeError:
        pass

meas_json = os.environ.get("GENIE_SQL_MEASURES", "")
if meas_json and meas_json != "[]":
    try:
        measures = json.loads(meas_json)
        if measures:
            items = [{
                "id": gen_id(),
                "alias": m["alias"],
                "sql": [m["sql"]],
                "display_name": m["display_name"],
            } for m in measures]
            items.sort(key=lambda x: x["id"])
            instructions.setdefault("sql_snippets", {})["measures"] = items
    except json.JSONDecodeError:
        pass

join_json = os.environ.get("GENIE_JOIN_SPECS", "")
skip_join_specs = os.environ.get("GENIE_SKIP_JOIN_SPECS", "0") == "1"
if not skip_join_specs and join_json and join_json != "[]":
    try:
        joins = json.loads(join_json)
        if joins:
            items = []
            for j in joins:
                items.append({
                    "id": gen_id(),
                    "left": {"identifier": j["left_table"]},
                    "right": {"identifier": j["right_table"]},
                    "sql": [j["sql"]],
                })
            items.sort(key=lambda x: x["id"])
            instructions["join_specs"] = items
    except json.JSONDecodeError:
        pass

if instructions:
    space["instructions"] = instructions

warehouse_id = os.environ.get("GENIE_WAREHOUSE_ID", "")
title = os.environ.get("GENIE_TITLE", "")
desc = os.environ.get("GENIE_DESCRIPTION", "")
skip_title = os.environ.get("GENIE_SKIP_TITLE", "0") == "1"

body = {"serialized_space": json.dumps(space, separators=(',', ':'))}
if warehouse_id:
    body["warehouse_id"] = warehouse_id
if title and not skip_title:
    body["title"] = title
if desc:
    body["description"] = desc

print(json.dumps(body))
PYEOF
  }

  local patch_body
  patch_body=$(build_patch_body 0)

  echo "Updating Genie agent ${space_id} config..."

  local tmpfile
  tmpfile=$(mktemp)
  echo "$patch_body" > "$tmpfile"

  local response
  response=$(curl -s -w "\n%{http_code}" -X PATCH \
    -H "${UA_HEADER}" \
    -H "Authorization: Bearer ${token}" \
    -H "Content-Type: application/json" \
    -d @"${tmpfile}" \
    "${workspace_url}/api/2.0/genie/spaces/${space_id}")

  local http_code
  http_code=$(echo "$response" | tail -n1)
  local response_body
  response_body=$(echo "$response" | sed '$d')

  if [[ "$http_code" != "200" && "$http_code" != "201" && -n "${GENIE_JOIN_SPECS:-}" && "${GENIE_JOIN_SPECS}" != "[]" ]]; then
    if echo "$response_body" | grep -q 'Failed to parse export proto'; then
      echo "Join specs were rejected by the Genie API. Retrying update without join_specs..."
      patch_body=$(build_patch_body 1)
      echo "$patch_body" > "$tmpfile"
      response=$(curl -s -w "\n%{http_code}" -X PATCH \
        -H "${UA_HEADER}" \
        -H "Authorization: Bearer ${token}" \
        -H "Content-Type: application/json" \
        -d @"${tmpfile}" \
        "${workspace_url}/api/2.0/genie/spaces/${space_id}")
      http_code=$(echo "$response" | tail -n1)
      response_body=$(echo "$response" | sed '$d')
      if [[ "$http_code" == "200" || "$http_code" == "201" ]]; then
        echo "Genie agent ${space_id} config updated successfully without join_specs."
        echo "WARNING: join_specs were skipped because the Genie API rejected them."
        rm -f "$tmpfile"
        return 0
      fi
    fi
  fi

  # Retry without the title field if the Genie API rejects it because a title
  # node already exists in the space (title was set during create; PATCH must
  # not include it again or the API returns RESOURCE_ALREADY_EXISTS).
  if [[ "$http_code" != "200" && "$http_code" != "201" ]]; then
    if echo "$response_body" | grep -qE 'ALREADY_EXISTS|RESOURCE_ALREADY_EXISTS' && \
       echo "$response_body" | grep -q 'Node named'; then
      echo "Genie API: title node already exists in space (set during create). Retrying without title..."
      patch_body=$(build_patch_body 0 1)
      echo "$patch_body" > "$tmpfile"
      response=$(curl -s -w "\n%{http_code}" -X PATCH \
        -H "${UA_HEADER}" \
        -H "Authorization: Bearer ${token}" \
        -H "Content-Type: application/json" \
        -d @"${tmpfile}" \
        "${workspace_url}/api/2.0/genie/spaces/${space_id}")
      http_code=$(echo "$response" | tail -n1)
      response_body=$(echo "$response" | sed '$d')
      if [[ "$http_code" == "200" || "$http_code" == "201" ]]; then
        echo "Genie agent ${space_id} config updated successfully (title was already set)."
        rm -f "$tmpfile"
        return 0
      fi
      # If the no-title retry also fails with invalid join_specs, try once more
      # without both title and join_specs.
      if [[ -n "${GENIE_JOIN_SPECS:-}" && "${GENIE_JOIN_SPECS}" != "[]" ]]; then
        if echo "$response_body" | grep -q 'Failed to parse export proto'; then
          echo "Join specs were also rejected (no-title retry). Retrying without title and join_specs..."
          patch_body=$(build_patch_body 1 1)
          echo "$patch_body" > "$tmpfile"
          response=$(curl -s -w "\n%{http_code}" -X PATCH \
            -H "${UA_HEADER}" \
            -H "Authorization: Bearer ${token}" \
            -H "Content-Type: application/json" \
            -d @"${tmpfile}" \
            "${workspace_url}/api/2.0/genie/spaces/${space_id}")
          http_code=$(echo "$response" | tail -n1)
          response_body=$(echo "$response" | sed '$d')
          if [[ "$http_code" == "200" || "$http_code" == "201" ]]; then
            echo "Genie agent ${space_id} config updated successfully (title already set, join_specs skipped)."
            echo "WARNING: join_specs were skipped because the Genie API rejected them."
            rm -f "$tmpfile"
            return 0
          fi
        fi
      fi
    fi
  fi

  rm -f "$tmpfile"

  if [[ "$http_code" == "200" || "$http_code" == "201" ]]; then
    echo "Genie agent ${space_id} config updated successfully."
  else
    echo "Failed to update Genie agent config (HTTP ${http_code})."
    echo "API response: ${response_body}"
    exit 1
  fi
}

# ---------- Trash (delete) a Genie agent ----------
trash_genie_space() {
  # Destroy provisioners can outlive the worktree that created their state.
  # Resolve the current environment path and credentials at execution time.
  if [[ -n "${LAYER_ENV_DIR:-}" ]]; then
    if [[ -n "${GENIE_ID_BASENAME:-}" ]]; then
      GENIE_ID_FILE="${LAYER_ENV_DIR}/${GENIE_ID_BASENAME}"
    fi
    local auth_tfvars="${LAYER_ENV_DIR}/auth.auto.tfvars"
    if [[ -f "$auth_tfvars" ]]; then
      DATABRICKS_HOST=$(python3 -c 'import hcl2,sys; print(hcl2.load(open(sys.argv[1])).get("databricks_workspace_host", ""))' "$auth_tfvars")
      DATABRICKS_CLIENT_ID=$(python3 -c 'import hcl2,sys; print(hcl2.load(open(sys.argv[1])).get("databricks_client_id", ""))' "$auth_tfvars")
      DATABRICKS_CLIENT_SECRET=$(python3 -c 'import hcl2,sys; print(hcl2.load(open(sys.argv[1])).get("databricks_client_secret", ""))' "$auth_tfvars")
      export DATABRICKS_HOST DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET
    fi
  fi

  local auth_host="${DATABRICKS_HOST%/}"
  local expected_host="${GENIE_EXPECTED_HOST:-}"
  expected_host="${expected_host%/}"
  local workspace_url="$auth_host"

  if [[ -n "$expected_host" && "$expected_host" != "$auth_host" ]]; then
    echo "Genie agent belongs to ${expected_host}; current authentication host is ${auth_host}. Targeting the original host with current credentials."
    workspace_url="$expected_host"
  fi

  if [[ -z "$workspace_url" ]]; then
    echo "Need workspace URL. Set DATABRICKS_HOST." >&2
    exit 1
  fi

  local space_id=""

  # Read space_id from the ID file
  if [[ -n "${GENIE_ID_FILE:-}" && -f "${GENIE_ID_FILE}" ]]; then
    space_id=$(cat "${GENIE_ID_FILE}" | tr -d '[:space:]')
  fi

  if [[ -z "$space_id" ]]; then
    echo "ERROR: Cannot identify Genie agent to trash: ID file missing or empty at ${GENIE_ID_FILE:-<not set>}." >&2
    echo "Refusing to continue because this would orphan the live space." >&2
    exit 1
  fi

  local adopted_marker=""
  if [[ -n "${GENIE_ID_FILE:-}" ]]; then
    adopted_marker="$(dirname "${GENIE_ID_FILE}")/.genie_adopted_$(basename "${GENIE_ID_FILE}" | sed 's/^\.genie_space_id_//')"
  fi
  if [[ -n "$adopted_marker" && -f "$adopted_marker" ]]; then
    echo "Unmanaging adopted Genie agent ${space_id}; leaving the agent in place."
    rm -f "${GENIE_ID_FILE}" "$adopted_marker"
    return 0
  fi

  local token
  if ! token=$(resolve_token "$workspace_url" ""); then
    echo "ERROR: Cannot authenticate to Genie agent host ${workspace_url} with credentials from current auth host ${auth_host}." >&2
    exit 1
  fi

  echo "Trashing Genie agent ${space_id}..."
  local response
  response=$(curl -s -w "\n%{http_code}" -X DELETE \
    -H "${UA_HEADER}" \
    -H "Authorization: Bearer ${token}" \
    "${workspace_url}/api/2.0/genie/spaces/${space_id}")

  local http_code
  http_code=$(echo "$response" | tail -n1)
  local response_body
  response_body=$(echo "$response" | sed '$d')

  if [[ "$http_code" == "200" || "$http_code" == "204" ]]; then
    echo "Genie agent ${space_id} trashed successfully."
    rm -f "${GENIE_ID_FILE}"
  elif [[ "$http_code" == "404" && ( -z "$expected_host" || "$workspace_url" == "$expected_host" ) ]]; then
    echo "Genie agent ${space_id} not found (already deleted). Cleaning up ID file."
    rm -f "${GENIE_ID_FILE}"
  elif [[ "$http_code" == "401" || "$http_code" == "403" ]]; then
    echo "ERROR: Cannot authenticate to Genie agent host ${workspace_url} with credentials from current auth host ${auth_host} (HTTP ${http_code})." >&2
    echo "API response: ${response_body}" >&2
    exit 1
  else
    echo "Failed to trash Genie agent (HTTP ${http_code})."
    echo "API response: ${response_body}"
    exit 1
  fi
}

# ---------- Main ----------
COMMAND="${1:-create}"
shift || true

if [[ "$COMMAND" == "create" ]]; then
  WORKSPACE_URL="${1:-${DATABRICKS_HOST}}"
  EXPLICIT_TOKEN="${2:-}"
  TITLE="${3:-${GENIE_TITLE:-ABAC Genie Space}}"
  WAREHOUSE_ID="${4:-${GENIE_WAREHOUSE_ID:-}}"

  if [[ -z "$WORKSPACE_URL" ]]; then
    echo "Need workspace URL. Set DATABRICKS_HOST or pass as first argument."
    exit 1
  fi

  TOKEN=$(resolve_token "$WORKSPACE_URL" "$EXPLICIT_TOKEN") || exit 1

  if [[ -z "$WAREHOUSE_ID" ]]; then
    WAREHOUSE_ID=$(read_warehouse_from_tfvars)
  fi
  if [[ -z "$WAREHOUSE_ID" ]]; then
    echo "No warehouse ID found. Set GENIE_WAREHOUSE_ID, pass as argument, or configure sql_warehouse_id in env.auto.tfvars."
    exit 1
  fi

  create_genie_space "$WORKSPACE_URL" "$TOKEN" "$TITLE" "$WAREHOUSE_ID"

elif [[ "$COMMAND" == "update-config" ]]; then
  update_genie_config

elif [[ "$COMMAND" == "set-acls" ]]; then
  WORKSPACE_URL="${1:-${DATABRICKS_HOST}}"
  EXPLICIT_TOKEN="${2:-}"
  SPACE_ID="${3:-${GENIE_SPACE_OBJECT_ID:-}}"

  # Try reading space ID from file if not provided directly
  if [[ -z "$SPACE_ID" && -n "${GENIE_ID_FILE:-}" ]]; then
    if [[ ! -f "${GENIE_ID_FILE}" ]]; then
      echo "ERROR: Genie agent ID file not found at '${GENIE_ID_FILE}'." >&2
      echo "  The space may have been deleted outside Terraform." >&2
      echo "  To recover: terraform taint 'module.workspace.terraform_data.genie_space[\"<space key>\"]'" >&2
      exit 1
    fi
    SPACE_ID=$(cat "${GENIE_ID_FILE}" | tr -d '[:space:]')
  fi

  if [[ -z "$WORKSPACE_URL" ]]; then
    echo "Need workspace URL. Set DATABRICKS_HOST or pass as first argument."
    exit 1
  fi

  TOKEN=$(resolve_token "$WORKSPACE_URL" "$EXPLICIT_TOKEN") || exit 1

  if [[ -z "$SPACE_ID" ]]; then
    echo "Genie agent ID required. Set GENIE_SPACE_OBJECT_ID, GENIE_ID_FILE, or pass as third argument."
    exit 1
  fi

  if [[ -z "${GENIE_GROUPS_CSV:-}" && "${GENIE_ALLOW_EMPTY_ACL:-}" != "1" ]]; then
    echo "ERROR: GENIE_GROUPS_CSV not set. Pass comma-separated group names." >&2
    echo "  Example: GENIE_GROUPS_CSV='Analyst,Admin' $0 set-acls" >&2
    exit 1
  fi

  set_genie_acls "$WORKSPACE_URL" "$TOKEN" "$SPACE_ID"

elif [[ "$COMMAND" == "trash" ]]; then
  trash_genie_space

else
  usage
fi
