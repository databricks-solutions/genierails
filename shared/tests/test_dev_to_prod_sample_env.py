from pathlib import Path
import importlib.util


MODULE = Path(__file__).parents[1] / "examples" / "dev_to_prod" / "setup_sample_env.py"
SPEC = importlib.util.spec_from_file_location("setup_sample_env", MODULE)
sample = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(sample)


def test_existing_space_same_warehouse_is_a_noop():
    sample._reconcile_existing_space({"warehouse_id": "warehouse-1"}, "warehouse-1")


def test_existing_space_rejects_implicit_warehouse_change():
    import pytest

    with pytest.raises(RuntimeError, match="--teardown"):
        sample._reconcile_existing_space({"warehouse_id": "warehouse-1"}, "warehouse-2")
