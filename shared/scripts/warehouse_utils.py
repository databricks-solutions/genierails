from __future__ import annotations


DEFAULT_WAREHOUSE_NAME = "ABAC Serverless Warehouse"

# The serverless warehouse name is now the canonical target across Terraform
# and test harness flows. Older runs may still leave behind these names.
PREFERRED_WAREHOUSE_NAMES = (
    DEFAULT_WAREHOUSE_NAME,
    "ABAC Governance Warehouse",
    "ABAC Test Setup Warehouse",
)


def _state_rank(state: str) -> int:
    state = (state or "").upper()
    if "RUNNING" in state:
        return 0
    if "STARTING" in state:
        return 1
    return 2


def warehouse_sort_key(warehouse) -> tuple[int, int, str, str]:
    name = warehouse.name or ""
    try:
        name_rank = PREFERRED_WAREHOUSE_NAMES.index(name)
    except ValueError:
        name_rank = len(PREFERRED_WAREHOUSE_NAMES)
    return (
        name_rank,
        _state_rank(str(warehouse.state)),
        name,
        warehouse.id or "",
    )


def select_warehouse(warehouses: list) -> object | None:
    if not warehouses:
        return None
    return sorted(warehouses, key=warehouse_sort_key)[0]
