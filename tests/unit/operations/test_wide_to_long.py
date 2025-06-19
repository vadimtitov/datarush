import pandas as pd
import pandas.testing as pdt

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.wide_to_long import WideToLong


def test_wide_to_long_basic():
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "score1": [10, 20],
            "score2": [30, 40],
        }
    )
    tableset = Tableset([Table("input", df)])

    model = {
        "table": "input",
        "index_columns": ["id"],
        "value_column": "score",
        "variable_column": "time",
        "stubs": ["score"],
    }

    op = WideToLong(model)
    result = op.operate(tableset)
    result_df = result.get_df("input")

    expected_df = pd.DataFrame(
        {
            "id": [1, 1, 2, 2],
            "time": [1, 2, 1, 2],
            "score": [10, 30, 20, 40],
        }
    )

    # Ensure same column order
    result_df = result_df[expected_df.columns]

    result_df = result_df.sort_values(["id", "time"]).reset_index(drop=True)
    expected_df = expected_df.sort_values(["id", "time"]).reset_index(drop=True)

    pdt.assert_frame_equal(result_df.reset_index(drop=True), expected_df)
