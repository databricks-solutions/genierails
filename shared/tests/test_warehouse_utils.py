import sys
from pathlib import Path
from types import SimpleNamespace


SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from warehouse_utils import DEFAULT_WAREHOUSE_NAME, select_warehouse  # noqa: E402


def _warehouse(name: str, state: str, warehouse_id: str) -> SimpleNamespace:
    return SimpleNamespace(name=name, state=state, id=warehouse_id)


def test_select_warehouse_prefers_canonical_name():
    selected = select_warehouse(
        [
            _warehouse("ABAC Governance Warehouse", "RUNNING", "wh-2"),
            _warehouse(DEFAULT_WAREHOUSE_NAME, "STOPPED", "wh-1"),
        ]
    )
    assert selected.id == "wh-1"


def test_select_warehouse_is_deterministic_for_unknown_names():
    selected = select_warehouse(
        [
            _warehouse("zeta", "RUNNING", "wh-2"),
            _warehouse("alpha", "RUNNING", "wh-1"),
        ]
    )
    assert selected.id == "wh-1"
