#!/usr/bin/env bash
# =============================================================================
# Import existing Databricks resources into Terraform state
# =============================================================================
# Imports groups, tag policies, and FGAC policies that already exist in
# Databricks so that Terraform can manage them without "already exists" errors.
#
# Prerequisites:
#   - auth.auto.tfvars configured with valid credentials
#   - env.auto.tfvars configured with uc_tables and environment settings
#   - abac.auto.tfvars configured with groups/tag_policies/fgac_policies
#   - terraform init already run
#
# Usage:
#   ./scripts/import_existing.sh              # import all resource types
#   ./scripts/import_existing.sh --groups-only # import only groups
#   ./scripts/import_existing.sh --tags-only   # import only tag policies
#   ./scripts/import_existing.sh --fgac-only   # import only FGAC policies
#   ./scripts/import_existing.sh --dry-run     # show commands without running
# =============================================================================

set -euo pipefail

MODULE_DIR="$(pwd)"
MODULE_BASENAME="$(basename "$MODULE_DIR")"
ENV_NAME="$MODULE_BASENAME"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_RUNNER="${TERRAFORM_RUNNER:-$SCRIPT_DIR/terraform_layer.sh}"

if [ "$MODULE_BASENAME" = "account" ]; then
  LAYER="account"
elif [ "$MODULE_BASENAME" = "data_access" ]; then
  LAYER="data_access"
  ENV_NAME="$(basename "$(dirname "$MODULE_DIR")")"
else
  LAYER="workspace"
fi

DRY_RUN=false
IMPORT_GROUPS=true
IMPORT_TAGS=true
IMPORT_FGAC=true
IMPORT_TAG_ASSIGNMENTS=true
IMPORT_GRANTS=true
IMPORT_WAREHOUSE=true

for arg in "$@"; do
  case "$arg" in
    --dry-run)     DRY_RUN=true ;;
    --groups-only) IMPORT_TAGS=false; IMPORT_FGAC=false; IMPORT_TAG_ASSIGNMENTS=false; IMPORT_GRANTS=false; IMPORT_WAREHOUSE=false ;;
    --tags-only)   IMPORT_GROUPS=false; IMPORT_FGAC=false; IMPORT_TAG_ASSIGNMENTS=false; IMPORT_GRANTS=false; IMPORT_WAREHOUSE=false ;;
    --fgac-only)   IMPORT_GROUPS=false; IMPORT_TAGS=false; IMPORT_TAG_ASSIGNMENTS=false; IMPORT_GRANTS=false; IMPORT_WAREHOUSE=false ;;
    --tag-assignments-only) IMPORT_GROUPS=false; IMPORT_TAGS=false; IMPORT_FGAC=false; IMPORT_GRANTS=false; IMPORT_WAREHOUSE=false ;;
    --grants-only) IMPORT_GROUPS=false; IMPORT_TAGS=false; IMPORT_FGAC=false; IMPORT_TAG_ASSIGNMENTS=false; IMPORT_WAREHOUSE=false ;;
    -h|--help)
      echo "Usage: $0 [--dry-run] [--groups-only|--tags-only|--fgac-only|--tag-assignments-only|--grants-only]"
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg"
      echo "Usage: $0 [--dry-run] [--groups-only|--tags-only|--fgac-only|--tag-assignments-only|--grants-only]"
      exit 1
      ;;
  esac
done

cd "$MODULE_DIR"

if [ ! -f abac.auto.tfvars ]; then
  echo "ERROR: abac.auto.tfvars not found. Configure it before importing."
  exit 1
fi

run_import() {
  local address="$1"
  local id="$2"

  if $DRY_RUN; then
    echo "  [DRY RUN] $TF_RUNNER '$LAYER' '$ENV_NAME' import '$address' '$id'"
    return 0
  fi

  # Fast-path: skip if already tracked in state. We list all resources and
  # grep for an exact match (not the address directly, because brackets are
  # treated as glob patterns by `terraform state list <address>`).
  if "$TF_RUNNER" "$LAYER" "$ENV_NAME" state list 2>/dev/null | grep -qF "$address"; then
    echo "  ↩ Already in state: $address (skipping)"
    return 0
  fi

  echo "  Importing: $address -> $id"
  local import_out
  import_out=$("$TF_RUNNER" "$LAYER" "$ENV_NAME" import "$address" "$id" 2>&1)
  local import_rc=$?
  if [ "$import_rc" -eq 0 ]; then
    echo "  ✓ Imported $address"
  elif echo "$import_out" | grep -q "Resource already managed by Terraform"; then
    # A previous retry already imported this resource — treat as success.
    echo "  ↩ Already managed: $address (skipping)"
  else
    echo "$import_out" >&2
    echo "  ✗ Failed to import $address (may not exist in Databricks)"
  fi
}

# Look up group display-names from abac.auto.tfvars and resolve their numeric
# account-level SCIM IDs via the Databricks SDK (required for terraform import).
# Outputs "name:scim_id" lines; falls back to "name:" (empty ID) if unreachable.
extract_group_name_id_pairs() {
  python3 - << 'GEOF'
import hcl2, sys, os, urllib.request, urllib.error, json as _json

def _str(v): return (v[0] if isinstance(v, list) else v or '').strip()

try:
    with open('abac.auto.tfvars') as f:
        cfg = hcl2.load(f)
    group_names = list(cfg.get('groups', {}).keys())
except Exception as e:
    sys.stderr.write(f'WARNING: Could not parse abac.auto.tfvars: {e}\n')
    sys.exit(0)

if not group_names:
    sys.exit(0)

auth = {}
for fname in ['auth.auto.tfvars', '../auth.auto.tfvars']:
    if os.path.exists(fname):
        with open(fname) as f:
            auth = hcl2.load(f)
        break

account_id    = _str(auth.get('databricks_account_id',    '')) or os.environ.get('DATABRICKS_ACCOUNT_ID', '')
client_id     = _str(auth.get('databricks_client_id',     '')) or os.environ.get('DATABRICKS_CLIENT_ID', '')
client_secret = _str(auth.get('databricks_client_secret', '')) or os.environ.get('DATABRICKS_CLIENT_SECRET', '')

if not account_id:
    sys.stderr.write('WARNING: No databricks_account_id found; skipping group ID lookup.\n')
    sys.exit(0)

try:
    from databricks.sdk import AccountClient
    account_host = _str(auth.get('databricks_account_host', '')) or 'https://accounts.cloud.databricks.com'
    a = AccountClient(
        host=account_host,
        account_id=account_id,
        client_id=client_id,
        client_secret=client_secret,
    )
    token_headers = a.config.authenticate()
    import ssl as _ssl, urllib.parse
    _ctx = _ssl.create_default_context()
    _ctx.check_hostname = False
    _ctx.verify_mode = _ssl.CERT_NONE
    base = f'{account_host}/api/2.0/accounts/{account_id}'
    for name in group_names:
        try:
            q = urllib.parse.quote(f'displayName eq "{name}"')
            url = f'{base}/scim/v2/Groups?filter={q}'
            req = urllib.request.Request(url, headers=token_headers)
            with urllib.request.urlopen(req, timeout=15, context=_ctx) as resp:
                data = _json.loads(resp.read())
            resources = data.get('Resources', [])
            if resources and resources[0].get('id'):
                print(name + ':' + str(resources[0]['id']))
            # If not found, print nothing — group doesn't exist, skip import
        except Exception as e:
            sys.stderr.write(f'WARNING: group lookup failed for {name!r}: {e}\n')
except Exception as e:
    sys.stderr.write(f'WARNING: AccountClient auth failed: {e}\n')
GEOF
}

# Only outputs tag keys that EXIST in the Databricks account, to avoid
extract_tag_keys() {
  python3 - << 'TKEOF'
import hcl2, sys, os

def _str(v): return (v[0] if isinstance(v, list) else v or '').strip()

with open('abac.auto.tfvars') as f:
    cfg = hcl2.load(f)
desired = set(tp.get('key', '') for tp in cfg.get('tag_policies', []))
if not desired:
    sys.exit(0)

auth = {}
for fname in ['auth.auto.tfvars', '../auth.auto.tfvars']:
    if os.path.exists(fname):
        with open(fname) as f:
            auth = hcl2.load(f)
        break

client_id     = _str(auth.get('databricks_client_id',     ''))
client_secret = _str(auth.get('databricks_client_secret', ''))
workspace_host = _str(auth.get('databricks_workspace_host', ''))

# Try importing ALL desired keys unconditionally.  terraform import fails
# harmlessly for non-existent resources (run_import catches the error),
# but skipping a key that DOES exist causes "already exists" errors on
# create — which are harder to recover from.  This avoids depending on
# the list API, which returns stale results after rapid delete/recreate
# cycles (eventual consistency).
for k in sorted(desired):
    print(k)
TKEOF
}

# Outputs TAB-separated lines: tf_key<TAB>import_id
# import_id format: entity_type,entity_name,tag_key  (commas — required by provider)
# Only emits assignments that EXIST in Databricks (checked via information_schema SQL).
extract_tag_assignments() {
  python3 - << 'TAEOF'
import hcl2, sys, os

def _str(v): return (v[0] if isinstance(v, list) else v or '').strip()

with open('abac.auto.tfvars') as f:
    cfg = hcl2.load(f)

desired = []
for ta in cfg.get('tag_assignments', []):
    etype = ta.get('entity_type', '')
    ename = ta.get('entity_name', '')
    tkey  = ta.get('tag_key', '')
    tval  = ta.get('tag_value', '')
    if etype and ename and tkey and tval:
        desired.append((etype, ename, tkey, tval))

if not desired:
    sys.exit(0)

# Build set of (entity_type, entity_name, tag_key) that actually exist via SQL
existing = set()
try:
    auth, env_cfg = {}, {}
    for fname in ['auth.auto.tfvars', '../auth.auto.tfvars']:
        if os.path.exists(fname):
            with open(fname) as f: auth = hcl2.load(f)
            break
    for fname in ['env.auto.tfvars', '../env.auto.tfvars']:
        if os.path.exists(fname):
            with open(fname) as f: env_cfg = hcl2.load(f)
            break

    host          = _str(auth.get('databricks_workspace_host', ''))
    client_id     = _str(auth.get('databricks_client_id', ''))
    client_secret = _str(auth.get('databricks_client_secret', ''))
    wh_id         = _str(env_cfg.get('sql_warehouse_id', ''))

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState
    w = WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)

    if not wh_id:
        for wh in w.warehouses.list():
            if wh.id:
                wh_id = wh.id
                break

    if wh_id:
        def run_sql(sql):
            import time
            r = w.statement_execution.execute_statement(
                statement=sql, warehouse_id=wh_id, wait_timeout='30s')
            while r.status and r.status.state in (
                    StatementState.PENDING, StatementState.RUNNING):
                time.sleep(1)
                r = w.statement_execution.get_statement(r.statement_id)
            rows = []
            if r.result and r.result.data_array:
                rows = r.result.data_array
            return rows

        # column tags: columns  entity_name = catalog.schema.table.column
        col_rows = run_sql("""
            SELECT
              concat(tag_catalog, '.', tag_schema, '.', tag_name, '.', column_name) AS entity_name,
              tag_key
            FROM system.information_schema.column_tags
        """)
        for row in col_rows:
            existing.add(('columns', row[0], row[1]))

        # table tags: tables  entity_name = catalog.schema.table
        tbl_rows = run_sql("""
            SELECT
              concat(tag_catalog, '.', tag_schema, '.', tag_name) AS entity_name,
              tag_key
            FROM system.information_schema.table_tags
        """)
        for row in tbl_rows:
            existing.add(('tables', row[0], row[1]))

        use_existing_check = True
    else:
        use_existing_check = False
except Exception as e:
    sys.stderr.write(f'WARNING: tag assignment check failed ({e}), falling back\n')
    use_existing_check = False

for (etype, ename, tkey, tval) in desired:
    if use_existing_check and (etype, ename, tkey) not in existing:
        continue  # does not exist — skip to avoid import error
    tf_key    = f'{etype}|{ename}|{tkey}|{tval}'
    import_id = f'{etype},{ename},{tkey}'
    print(tf_key + '\t' + import_id)
TAEOF
}

# Delete stale entity tag assignments (same entity+tag_key, possibly wrong tag_value)
# via SQL UNSET TAGS so that Terraform can create fresh assignments without conflicts.
cleanup_stale_tag_assignments() {
  python3 - << 'PYEOF'
import hcl2, os, sys

def _str(v): return (v[0] if isinstance(v, list) else v or '').strip()

# Load abac, auth, env configs
cfg, auth, env_cfg = {}, {}, {}
for fname in ['abac.auto.tfvars']:
    if os.path.exists(fname):
        try:
            with open(fname) as f: cfg = hcl2.load(f)
        except Exception: pass

for fname in ['auth.auto.tfvars', '../auth.auto.tfvars']:
    if os.path.exists(fname):
        try:
            with open(fname) as f: auth = hcl2.load(f)
        except Exception: pass
        break

for fname in ['env.auto.tfvars', '../env.auto.tfvars']:
    if os.path.exists(fname):
        try:
            with open(fname) as f: env_cfg = hcl2.load(f)
        except Exception: pass
        break

tag_assignments = cfg.get('tag_assignments', [])
if not tag_assignments:
    sys.exit(0)

host          = _str(auth.get('databricks_workspace_host', '')) or os.environ.get('DATABRICKS_HOST', '')
client_id     = _str(auth.get('databricks_client_id', ''))     or os.environ.get('DATABRICKS_CLIENT_ID', '')
client_secret = _str(auth.get('databricks_client_secret', '')) or os.environ.get('DATABRICKS_CLIENT_SECRET', '')
warehouse_id  = _str(env_cfg.get('sql_warehouse_id', ''))

if not warehouse_id:
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)
        for wh in w.warehouses.list():
            if 'RUNNING' in str(wh.state) or 'STARTING' in str(wh.state):
                warehouse_id = str(wh.id)
                break
    except Exception as e:
        sys.stderr.write(f'WARNING: warehouse lookup failed: {e}\n')

if not warehouse_id:
    sys.stderr.write('WARNING: no warehouse_id; skipping tag assignment pre-cleanup\n')
    sys.exit(0)

try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)
except Exception as e:
    sys.stderr.write(f'WARNING: SDK init failed: {e}\n')
    sys.exit(0)

cleaned = 0
for ta in tag_assignments:
    etype = ta.get('entity_type', '')
    ename = ta.get('entity_name', '')
    tkey  = ta.get('tag_key', '')
    if not (etype and ename and tkey):
        continue

    if etype == 'columns':
        parts = ename.rsplit('.', 1)
        if len(parts) != 2:
            continue
        table_fqn, col = parts
        sql = f"ALTER TABLE {table_fqn} ALTER COLUMN {col} UNSET TAGS ('{tkey}')"
    elif etype == 'tables':
        sql = f"ALTER TABLE {ename} UNSET TAGS ('{tkey}')"
    else:
        continue

    try:
        from databricks.sdk.service.sql import StatementState as _SS
        resp = w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=warehouse_id,
            wait_timeout='30s',
        )
        raw_state = getattr(getattr(resp, 'status', None), 'state', None)
        state_str = raw_state.value if hasattr(raw_state, 'value') else str(raw_state or '')
        if 'SUCCEEDED' in state_str:
            print(f'  Cleared tag {tkey} on {ename}')
            cleaned += 1
        elif 'FAILED' in state_str:
            raw_err = getattr(getattr(resp, 'status', None), 'error', None)
            err = str(getattr(raw_err, 'message', '') or raw_err or '')
            if not ('not found' in err.lower() or 'does not exist' in err.lower() or 'unset' in err.lower()):
                sys.stderr.write(f'  WARNING: could not clear {tkey} on {ename}: {err}\n')
    except Exception as e:
        sys.stderr.write(f'  WARNING: SQL failed for {ename}/{tkey}: {e}\n')

if cleaned:
    print(f'  Cleared {cleaned} stale tag assignment(s).')
PYEOF
}

# Outputs TAB-separated lines: grant_type<TAB>tf_key<TAB>import_id
# grant_type is "catalog_access" or "terraform_sp"
# tf_key may contain | (e.g. "dev_fin|Finance_Analyst") — use TAB IFS to parse correctly
# Only emits grants that ACTUALLY EXIST in Databricks (verified via SDK)
# to avoid "Cannot import non-existent remote object" noise.
extract_grants() {
  python3 - << 'GEOF'
import hcl2, sys, os

def _str(v): return (v[0] if isinstance(v, list) else v or '').strip()

auth = {}
for fname in ['auth.auto.tfvars', '../auth.auto.tfvars']:
    if os.path.exists(fname):
        try:
            with open(fname) as f:
                auth = hcl2.load(f)
        except Exception:
            pass
        break

host          = _str(auth.get('databricks_workspace_host', ''))
client_id     = _str(auth.get('databricks_client_id', '')) or os.environ.get('DATABRICKS_CLIENT_ID', '')
client_secret = _str(auth.get('databricks_client_secret', ''))

try:
    with open('abac.auto.tfvars') as f:
        cfg = hcl2.load(f)
    groups          = list(cfg.get('groups', {}).keys())
    tag_assignments = cfg.get('tag_assignments', [])
    fgac_policies   = cfg.get('fgac_policies', [])
    uc_tables       = cfg.get('uc_tables', []) or []
except Exception as e:
    sys.stderr.write(f'WARNING: extract_grants config load failed: {e}\n')
    sys.exit(0)

catalogs = set()
for ta in tag_assignments:
    ename = ta.get('entity_name', '')
    if ename:
        catalogs.add(ename.split('.')[0])
for p in fgac_policies:
    cat = p.get('catalog', '')
    if cat:
        catalogs.add(cat)
for t in uc_tables:
    if t.count('.') >= 2:
        catalogs.add(t.split('.')[0])

# Query Databricks REST API to find which principals actually have grants.
# Uses requests + token auth to avoid SDK version/method-name uncertainty.
existing_grants = {}  # catalog -> set(principal) or None (unknown=fallback)
try:
    import urllib.request, urllib.error, json as _json

    # Obtain a token via the SDK (handles M2M OAuth automatically).
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)
    token = w.config.authenticate()  # dict of headers, e.g. {'Authorization': 'Bearer ...'}

    import ssl as _ssl
    _ssl_ctx = _ssl.create_default_context()
    _ssl_ctx.check_hostname = False
    _ssl_ctx.verify_mode = _ssl.CERT_NONE

    base = host.rstrip('/')
    for catalog in sorted(catalogs):
        url = f'{base}/api/2.1/unity-catalog/permissions/catalog/{catalog}'
        req = urllib.request.Request(url, headers=token)
        try:
            with urllib.request.urlopen(req, timeout=15, context=_ssl_ctx) as resp:
                data = _json.loads(resp.read())
            principals = set()
            for pa in data.get('privilege_assignments', []):
                p = pa.get('principal', '')
                if p:
                    principals.add(p)
            existing_grants[catalog] = principals
        except urllib.error.HTTPError as he:
            if he.code in (403, 404):
                # 403 = SP has no privilege; 404 = catalog doesn't exist yet.
                # Treat as "no grants" — skip all imports for this catalog.
                existing_grants[catalog] = set()
            else:
                sys.stderr.write(f'WARNING: grants check for {catalog} returned HTTP {he.code}, falling back\n')
                existing_grants[catalog] = None
        except Exception as e:
            sys.stderr.write(f'WARNING: grants check for {catalog} failed: {e}, falling back\n')
            existing_grants[catalog] = None
except Exception as sdk_err:
    sys.stderr.write(f'WARNING: grants SDK/auth failed: {sdk_err}, falling back\n')
    existing_grants = {}  # all None => fall back for every catalog

sep = '\t'
for catalog in sorted(catalogs):
    known = existing_grants.get(catalog)  # None=unknown(fallback), set=verified
    for group in groups:
        if known is not None and group not in known:
            continue  # grant does not exist — skip to avoid import error
        tf_key    = catalog + '|' + group
        import_id = 'catalog/' + catalog + '/' + group
        sys.stdout.write('catalog_access' + sep + tf_key + sep + import_id + '\n')
    if client_id and (known is None or client_id in known):
        import_id = 'catalog/' + catalog + '/' + client_id
        sys.stdout.write('terraform_sp' + sep + catalog + sep + import_id + '\n')
GEOF
}

# Find the warehouse name used by this data_access env and return its ID.
# Outputs: <warehouse_id> (single line) or nothing if not found.
extract_warehouse_id_by_name() {
  python3 -c "
import hcl2, sys, os

def _str(v): return (v[0] if isinstance(v, list) else v or '').strip()

# Load env.auto.tfvars to get sql_warehouse_id (skip import if explicitly set)
env_cfg = {}
for fname in ['env.auto.tfvars', '../env.auto.tfvars']:
    if os.path.exists(fname):
        try:
            with open(fname) as f:
                env_cfg = hcl2.load(f)
        except Exception:
            pass
        break
if _str(env_cfg.get('sql_warehouse_id', '')):
    # Warehouse explicitly configured — no import needed
    sys.exit(0)

auth = {}
for fname in ['auth.auto.tfvars', '../auth.auto.tfvars']:
    if os.path.exists(fname):
        try:
            with open(fname) as f:
                auth = hcl2.load(f)
        except Exception:
            pass
        break

host          = _str(auth.get('databricks_workspace_host', '')) or os.environ.get('DATABRICKS_HOST', '')
client_id     = _str(auth.get('databricks_client_id', ''))     or os.environ.get('DATABRICKS_CLIENT_ID', '')
client_secret = _str(auth.get('databricks_client_secret', '')) or os.environ.get('DATABRICKS_CLIENT_SECRET', '')

# Warehouse name matches var.warehouse_name default in data_access module
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)
    for wh in w.warehouses.list():
        if wh.name == 'ABAC Governance Warehouse':
            print(str(wh.id))
            break
except Exception as e:
    sys.stderr.write(f'WARNING: warehouse lookup failed: {e}\n')
" 2>/dev/null || true
}

# Outputs TAB-separated lines: tf_key<TAB>import_id
# import_id format: on_securable_type,on_securable_fullname,name  (required by provider)
# Only emits policies that EXIST in Databricks (verified via REST API).
extract_fgac_names() {
  python3 - << 'FEOF'
import hcl2, sys, os

def _str(v): return (v[0] if isinstance(v, list) else v or '').strip()

with open('abac.auto.tfvars') as f:
    cfg = hcl2.load(f)

desired = []  # list of (name, catalog, sec_type, full_name)
for p in cfg.get('fgac_policies', []):
    name     = p.get('name', '')
    catalog  = p.get('catalog', '')
    sec_type = p.get('on_securable_type', 'CATALOG')
    if name and catalog:
        desired.append((name, catalog, sec_type, f'{catalog}_{name}'))

if not desired:
    sys.exit(0)

auth = {}
for fname in ['auth.auto.tfvars', '../auth.auto.tfvars']:
    if os.path.exists(fname):
        with open(fname) as f:
            auth = hcl2.load(f)
        break

existing_by_catalog = {}  # catalog -> set of full policy names, or None=unknown
try:
    import urllib.request, urllib.error, urllib.parse, json as _json, ssl as _ssl
    host          = _str(auth.get('databricks_workspace_host', ''))
    client_id     = _str(auth.get('databricks_client_id', ''))
    client_secret = _str(auth.get('databricks_client_secret', ''))
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)
    token = w.config.authenticate()
    base  = host.rstrip('/')
    ssl_ctx = _ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = _ssl.CERT_NONE
    for catalog in set(d[1] for d in desired):
        try:
            # Correct API: GET /api/2.1/unity-catalog/policies/CATALOG/{catalog}
            url = f'{base}/api/2.1/unity-catalog/policies/CATALOG/{urllib.parse.quote(catalog, safe="")}'
            req = urllib.request.Request(url, headers=token)
            with urllib.request.urlopen(req, timeout=15, context=ssl_ctx) as resp:
                data = _json.loads(resp.read())
            existing_by_catalog[catalog] = {
                p.get('name', '') for p in data.get('policies', []) if p.get('name')
            }
        except urllib.error.HTTPError as he:
            if he.code in (403, 404):
                existing_by_catalog[catalog] = set()  # no policies
            else:
                sys.stderr.write(f'WARNING: policy list HTTP {he.code} for {catalog}\n')
                existing_by_catalog[catalog] = None
        except Exception as e:
            sys.stderr.write(f'WARNING: policy list failed for {catalog}: {e}\n')
            existing_by_catalog[catalog] = None  # unknown — fall back
except Exception as e:
    sys.stderr.write(f'WARNING: policy check failed ({e}), falling back\n')

for (name, catalog, sec_type, full_name) in desired:
    known = existing_by_catalog.get(catalog)
    if known is not None and full_name not in known:
        continue  # policy does not exist yet — skip to avoid import error
    import_id = f'{sec_type},{catalog},{full_name}'
    print(name + '\t' + import_id)
FEOF
}

echo "============================================"
echo "  Import Existing Resources into Terraform"
echo "============================================"
echo ""

imported=0
skipped=0

if $IMPORT_GROUPS; then
  if [ "$LAYER" != "account" ]; then
    echo "--- Groups ---"
    echo "  Skipping group imports outside the shared account layer"
    echo ""
  else
    echo "--- Groups ---"
    group_pairs=$(extract_group_name_id_pairs)
    if [ -z "$group_pairs" ]; then
      echo "  No existing groups found to import."
    else
      while IFS=':' read -r name group_id; do
        [ -z "$name" ] || [ -z "$group_id" ] && continue
        run_import "module.account.databricks_group.groups[\"$name\"]" "$group_id"
        ((imported++)) || true
      done <<< "$group_pairs"
    fi
    echo ""
  fi
fi

if $IMPORT_TAGS; then
  echo "--- Tag Policies ---"
  if [ "$LAYER" != "account" ]; then
    echo "  Skipping tag policy imports outside envs/account (tag policies are account-scoped)"
  else
    tag_keys=$(extract_tag_keys)
    if [ -z "$tag_keys" ]; then
      echo "  No tag policies found in abac.auto.tfvars."
    else
      while IFS= read -r key; do
        [ -z "$key" ] && continue
        run_import "module.account.databricks_tag_policy.policies[\"$key\"]" "$key"
        ((imported++)) || true
      done <<< "$tag_keys"
    fi
  fi
  echo ""
fi

if $IMPORT_FGAC; then
  echo "--- FGAC Policies ---"
  if [ "$LAYER" != "data_access" ]; then
    echo "  Skipping FGAC policy imports outside envs/<workspace>/data_access"
  else
    fgac_entries=$(extract_fgac_names)
    if [ -z "$fgac_entries" ]; then
      echo "  No FGAC policies found in abac.auto.tfvars."
    else
      # TAB-separated: tf_key<TAB>import_id
      # import_id format: on_securable_type,on_securable_fullname,name
      while IFS=$'\t' read -r policy_key import_id; do
        [ -z "$policy_key" ] && continue
        run_import "module.data_access.databricks_policy_info.policies[\"$policy_key\"]" "$import_id"
        ((imported++)) || true
      done <<< "$fgac_entries"
    fi
  fi
  echo ""
fi

if $IMPORT_TAG_ASSIGNMENTS; then
  echo "--- Tag Assignments ---"
  if [ "$LAYER" != "data_access" ]; then
    echo "  Skipping tag assignment imports outside envs/<workspace>/data_access"
  else
    # Pre-cleanup: remove stale assignments (wrong tag_value from prior LLM run)
    echo "  Pre-cleanup: removing stale tag assignments via SQL..."
    cleanup_stale_tag_assignments 2>&1 | sed 's/^/  /'

    tag_assignment_entries=$(extract_tag_assignments)
    if [ -z "$tag_assignment_entries" ]; then
      echo "  No tag assignments found in abac.auto.tfvars."
    else
      # TAB-separated: tf_key<TAB>import_id
      while IFS=$'\t' read -r tf_key import_id; do
        [ -z "$tf_key" ] && continue
        run_import "module.data_access.databricks_entity_tag_assignment.assignments[\"$tf_key\"]" "$import_id"
        ((imported++)) || true
      done <<< "$tag_assignment_entries"
    fi
  fi
  echo ""
fi

if $IMPORT_GRANTS; then
  echo "--- Grants ---"
  if [ "$LAYER" != "data_access" ]; then
    echo "  Skipping grant imports outside envs/<workspace>/data_access"
  else
    grant_entries=$(extract_grants)
    if [ -z "$grant_entries" ]; then
      echo "  No grants derivable from abac.auto.tfvars."
    else
      # TAB-separated: grant_type<TAB>tf_key<TAB>import_id
      # tf_key may contain | (e.g. "dev_fin|Finance_Analyst") — TAB IFS handles this correctly
      while IFS=$'\t' read -r grant_type tf_key import_id; do
        [ -z "$grant_type" ] && continue
        if [ "$grant_type" = "catalog_access" ]; then
          run_import "module.data_access.databricks_grant.catalog_access[\"$tf_key\"]" "$import_id"
        elif [ "$grant_type" = "terraform_sp" ]; then
          run_import "module.data_access.databricks_grant.terraform_sp_manage_catalog[\"$tf_key\"]" "$import_id"
        fi
        ((imported++)) || true
      done <<< "$grant_entries"
    fi
  fi
  echo ""
fi

if $IMPORT_WAREHOUSE; then
  echo "--- SQL Warehouse ---"
  if [ "$LAYER" = "data_access" ]; then
    wh_id=$(extract_warehouse_id_by_name)
    if [ -z "$wh_id" ]; then
      echo "  No orphaned warehouse found (or sql_warehouse_id is already set)."
    else
      run_import "module.data_access.databricks_sql_endpoint.warehouse[0]" "$wh_id"
      ((imported++)) || true
    fi
  elif [ "$LAYER" = "workspace" ]; then
    wh_id=$(extract_warehouse_id_by_name)
    if [ -z "$wh_id" ]; then
      echo "  No orphaned warehouse found (or sql_warehouse_id is already set)."
    else
      run_import "module.workspace.databricks_sql_endpoint.warehouse[0]" "$wh_id"
      ((imported++)) || true
    fi
  else
    echo "  Skipping warehouse import outside data_access/workspace layers"
  fi
  echo ""
fi

echo "============================================"
if $DRY_RUN; then
  echo "  Dry run complete. $imported import(s) would be attempted."
else
  echo "  Done. $imported import(s) attempted."
fi
echo "  Next: terraform plan (to verify state is consistent)"
echo "============================================"
