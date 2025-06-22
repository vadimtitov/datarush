import pandas as pd
import pandas.testing as pdt

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.unset_header import UnsetHeader


def test_unset_header_multiple_tables():
    df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    df2 = pd.DataFrame({"x": ["u", "v"], "y": ["w", "z"]})

    tableset = Tableset(
        [
            Table("t1", df1),
            Table("t2", df2),
        ]
    )

    model = {"tables": ["t1", "t2"]}
    op = UnsetHeader(model)
    result = op.operate(tableset)

    expected1 = pd.DataFrame(
        [
            ["a", "b"],
            [1, 3],
            [2, 4],
        ]
    )
    expected2 = pd.DataFrame(
        [
            ["x", "y"],
            ["u", "w"],
            ["v", "z"],
        ]
    )
    expected1.columns = ["0", "1"]
    expected2.columns = ["0", "1"]

    pdt.assert_frame_equal(result.get_df("t1"), expected1)
    pdt.assert_frame_equal(result.get_df("t2"), expected2)
