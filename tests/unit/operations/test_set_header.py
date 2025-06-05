import pandas as pd

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.set_header import SetHeader


def test_set_header_inplace():
    df = pd.DataFrame(
        [
            ["col1", "col2", "col3"],
            [1, 2, 3],
            [4, 5, 6],
        ]
    )
    tableset = Tableset([Table("raw_table", df)])

    model = {
        "table": "raw_table",
        "row_index": 0,
    }

    op = SetHeader(model)
    result = op.operate(tableset)
    new_df = result.get_df("raw_table")

    assert list(new_df.columns) == ["col1", "col2", "col3"]
    assert new_df.shape == (2, 3)
    assert new_df.iloc[0]["col1"] == 1
    assert new_df.iloc[1]["col2"] == 5
