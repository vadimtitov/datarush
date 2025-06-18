import pandas as pd
import pandas.testing as pdt
import pytest

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.columns_to_dict import ColumnsToDict


@pytest.mark.parametrize("drop", [False, True])
def test_columns_to_dict(drop):
    df = pd.DataFrame(
        {
            "a": [1, 2],
            "b": ["x", "y"],
            "c": [True, False],
        }
    )
    tableset = Tableset([Table("data", df)])

    model = {
        "table": "data",
        "columns": ["a", "b"],
        "output_column": "combined",
        "drop": drop,
    }

    op = ColumnsToDict(model)
    result = op.operate(tableset)

    expected = pd.DataFrame(
        {
            "a": [1, 2],
            "b": ["x", "y"],
            "c": [True, False],
            "combined": [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}],
        }
    )

    if drop:
        expected.drop(columns=["a", "b"], inplace=True)

    pdt.assert_frame_equal(result.get_df("data"), expected)
