import pandas as pd
import pandas.testing as pdt

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.add_range_column import AddRangeColumn


def test_add_range_column_default():
    df = pd.DataFrame({"value": ["a", "b", "c"]})
    tableset = Tableset([Table("input", df)])

    model = {"table": "input"}

    op = AddRangeColumn(model)
    result = op.operate(tableset)

    expected_df = pd.DataFrame(
        {
            "value": ["a", "b", "c"],
            "index": [0, 1, 2],
        }
    )

    pdt.assert_frame_equal(result.get_df("input"), expected_df)


def test_add_range_column_custom():
    df = pd.DataFrame({"x": [10, 20, 30, 40]})
    tableset = Tableset([Table("nums", df)])

    model = {
        "table": "nums",
        "column": "row_id",
        "start": 100,
        "step": 10,
    }

    op = AddRangeColumn(model)
    result = op.operate(tableset)

    expected_df = pd.DataFrame(
        {
            "x": [10, 20, 30, 40],
            "row_id": [100, 110, 120, 130],
        }
    )

    pdt.assert_frame_equal(result.get_df("nums"), expected_df)
