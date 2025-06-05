import pandas as pd

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.melt_table import Melt


def test_melt_operation():
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "A": [10, 30],
            "B": [20, 40],
        }
    )
    tableset = Tableset([Table("wide_table", df)])
    expected = pd.DataFrame(
        {
            "id": [1, 2, 1, 2],
            "feature": ["A", "A", "B", "B"],
            "score": [10, 30, 20, 40],
        }
    )
    model = {
        "table": "wide_table",
        "id_vars": ["id"],
        "value_vars": ["A", "B"],
        "var_name": "feature",
        "value_name": "score",
        "output_table": "long_table",
    }

    op = Melt(model)
    result = op.operate(tableset)
    melted_df = result.get_df("long_table")

    pd.testing.assert_frame_equal(
        melted_df.sort_values(by=["id", "feature"]).reset_index(drop=True),
        expected.sort_values(by=["id", "feature"]).reset_index(drop=True),
    )
