import pandas as pd
import pandas.testing as pdt

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.copy_column import CopyColumn


def test_copy_column_creates_new_column():
    df = pd.DataFrame(
        {
            "a": [10, 20, 30],
        }
    )
    tableset = Tableset([Table("data", df)])

    model = {
        "table": "data",
        "source_column": "a",
        "target_column": "a_copy",
    }

    op = CopyColumn(model)
    result = op.operate(tableset)
    result_df = result.get_df("data")

    # Should contain both columns and values should match
    assert "a_copy" in result_df.columns
    pdt.assert_series_equal(result_df["a"], result_df["a_copy"], check_names=False)

    # Modify original to confirm deep copy
    result_df.loc[0, "a"] = 999
    assert result_df.loc[0, "a_copy"] == 10
