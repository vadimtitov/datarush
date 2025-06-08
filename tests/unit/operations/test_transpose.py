import pandas as pd
import pandas.testing as pdt

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.transpose import Transpose


def test_transpose_operation_inplace():
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})

    tableset = Tableset([Table("input", df)])

    model = {"table": "input"}

    op = Transpose(model)
    result = op.operate(tableset)

    expected = pd.DataFrame(
        {
            0: [1, 3],
            1: [2, 4],
        },
        index=["A", "B"],
    )

    pdt.assert_frame_equal(result.get_df("input"), expected)
