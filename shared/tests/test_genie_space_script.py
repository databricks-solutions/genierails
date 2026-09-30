from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "genie_space.sh"


def test_sql_expressions_and_measures_include_required_display_name():
    source = SCRIPT.read_text()

    expression_block = source[source.index('expr_json ='):source.index('meas_json =')]
    measure_block = source[source.index('meas_json ='):source.index('join_json =')]

    assert '"display_name": e["display_name"]' in expression_block
    assert '"display_name": m["display_name"]' in measure_block
