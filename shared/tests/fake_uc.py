"""A fake Databricks SDK and Terraform layer runner for subprocess tests of import_existing.sh and make.

``install(tmp_path, live)`` writes a ``databricks.sdk`` package that answers
from ``live`` (column/table tags, catalog grants, policies, warehouses) and a
``terraform_layer.sh`` stand-in that lists, imports into and plans against
each layer's JSON ``terraform.tfstate``. Both append one JSON line per call to
the same events file, in order: ``{"src": "sql", "statement": ...}`` and
``{"src": "runner", "argv": [...]}``. Nothing reaches Databricks.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

SDK = r'''
import enum, io, json, os, re, urllib.request
from types import SimpleNamespace

LIVE = json.load(open(os.environ["FAKE_UC_LIVE"]))


def _event(**entry):
    with open(os.environ["FAKE_UC_EVENTS"], "a") as f:
        f.write(json.dumps(entry) + "\n")


class StatementState(enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


def _rows(sql):
    if "concat(" in sql:
        kind = "column_tags" if "column_tags" in sql else "table_tags"
        return [[entity, key] for entity, key, _ in LIVE.get(kind, [])]
    where = dict(re.findall(r"(\w+_name) = '([^']*)'", sql))
    if "column_tags" in sql:
        entity = ".".join(where[k] for k in ("catalog_name", "schema_name", "table_name", "column_name"))
        tags = LIVE.get("column_tags", [])
    else:
        entity = ".".join(where[k] for k in ("catalog_name", "schema_name", "table_name"))
        tags = LIVE.get("table_tags", [])
    return [[value] for e, key, value in tags if e == entity and key == where["tag_name"]][:1]


class _Statements:
    def execute_statement(self, *, statement, warehouse_id=None, **_):
        _event(src="sql", statement=" ".join(statement.split()), warehouse_id=warehouse_id)
        rows = _rows(statement) if statement.lstrip().upper().startswith("SELECT") else []
        return SimpleNamespace(statement_id="s", status=SimpleNamespace(state=StatementState.SUCCEEDED, error=None),
                               result=SimpleNamespace(data_array=rows))

    def get_statement(self, _statement_id):
        raise AssertionError("every fake statement finishes at once")


class _Warehouses:
    def list(self):
        return [SimpleNamespace(**w) for w in LIVE.get("warehouses", [])]


class WorkspaceClient:
    def __init__(self, **_):
        self.statement_execution = _Statements()
        self.warehouses = _Warehouses()
        self.config = SimpleNamespace(authenticate=lambda: {})


AccountClient = WorkspaceClient


def _urlopen(request, timeout=None, context=None):
    url = request.full_url if hasattr(request, "full_url") else str(request)
    if LIVE.get("rest_down"):
        raise OSError("fake REST outage")
    if "/permissions/catalog/" in url:
        catalog = url.rsplit("/", 1)[1]
        body = {"privilege_assignments": [{"principal": p} for p in LIVE.get("grants", {}).get(catalog, [])]}
    elif "/policies/CATALOG/" in url:
        catalog = url.rsplit("/", 1)[1]
        body = {"policies": [{"name": n} for n in LIVE.get("policies", {}).get(catalog, [])]}
    else:
        raise AssertionError(f"unexpected fake request {url}")
    return io.BytesIO(json.dumps(body).encode())


urllib.request.urlopen = _urlopen  # import_existing.sh lists grants and policies over REST
'''

RUNNER = r'''#!{python}
"""terraform_layer.sh stand-in: state-list / import / plan against the layer's JSON terraform.tfstate."""
import json, os, re, sys
layer, env_name, command, *args = sys.argv[1:]
layer_dir = os.environ.get("LAYER_ENV_DIR", "")
with open(os.environ["FAKE_UC_EVENTS"], "a") as f:
    f.write(json.dumps({{"src": "runner", "argv": sys.argv[1:], "layer_dir": layer_dir}}) + "\n")
path = os.path.join(layer_dir, "terraform.tfstate")
state = json.load(open(path)) if layer_dir and os.path.exists(path) else {{"version": 4, "resources": []}}


def address(resource, instance):
    key = instance.get("index_key")
    index = f"[{{json.dumps(key)}}]" if isinstance(key, str) else ("" if key is None else f"[{{key}}]")
    return f"{{resource['module'] + '.' if resource.get('module') else ''}}{{resource['type']}}.{{resource['name']}}{{index}}"


if command == "state":  # as the real runner: its alias is state-list
    sys.exit("Unsupported terraform command alias: state")
if command == "state-list":
    for resource in state.get("resources", []):
        for instance in resource.get("instances", []):
            print(address(resource, instance))
elif command == "import":
    if os.environ.get("FAKE_UC_FAIL_IMPORT"):
        sys.exit("fake import failure")
    if args and args[0] == "-no-color":
        args = args[1:]
    module, rtype, name, key = re.match(r'(module\.\w+)\.(\w+)\.(\w+)\["(.*)"\]$', args[0]).groups()
    for resource in state["resources"]:
        if (resource.get("module"), resource["type"], resource["name"]) == (module, rtype, name):
            break
    else:
        resource = {{"module": module, "mode": "managed", "type": rtype, "name": name, "instances": []}}
        state["resources"].append(resource)
    resource["instances"].append({{"index_key": key, "attributes": {{"id": args[1]}}}})
    json.dump(state, open(path, "w"))
elif command == "plan":
    for arg in args:
        if arg.startswith("-out="):
            open(arg[5:], "wb").write(b"fake plan")
'''


def install(tmp_path: Path, live: dict) -> dict[str, str]:
    """Write the fakes under tmp_path; returns the environment that puts them in use."""
    root = Path(tmp_path) / "fake-uc"
    sdk = root / "sdk" / "databricks" / "sdk"
    (sdk / "service").mkdir(parents=True, exist_ok=True)
    (sdk.parent / "__init__.py").write_text("")
    (sdk / "__init__.py").write_text(SDK)
    (sdk / "service" / "__init__.py").write_text("")
    (sdk / "service" / "sql.py").write_text("from databricks.sdk import StatementState\n")
    (sdk / "errors.py").write_text("class NotFound(Exception):\n    pass\n")
    (sdk / "useragent.py").write_text("def with_extra(*_a, **_k):\n    pass\n")  # no pip upgrade of the real SDK
    live_file = root / "live.json"
    live_file.write_text(json.dumps(live))
    events = root / "events.jsonl"
    runner = root / "terraform_layer.py"
    runner.write_text(RUNNER.format(python=sys.executable))
    runner.chmod(0o755)
    return {"PYTHONPATH": str(root / "sdk"), "PIP_NO_INDEX": "1", "FAKE_UC_LIVE": str(live_file),
            "FAKE_UC_EVENTS": str(events), "TERRAFORM_RUNNER": str(runner), "FAKE_UC_RUNNER": str(runner)}


def events(environ: dict) -> list[dict]:
    path = Path(environ["FAKE_UC_EVENTS"])
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def sql_writes(environ: dict) -> list[str]:
    return [e["statement"] for e in events(environ) if e["src"] == "sql" and not e["statement"].startswith("SELECT")]


def runner_imports(environ: dict) -> list[list[str]]:
    return [e["argv"] for e in events(environ) if e["src"] == "runner" and e["argv"][2] == "import"]
