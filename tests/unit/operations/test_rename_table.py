import pandas as pd
import pandas.testing as pdt

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.rename_table import RenameTable


def test_rename_table():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    tableset = Tableset([Table("old_name", df)])

    model = {
        "table": "old_name",
        "new_name": "new_name",
    }

    op = RenameTable(model)
    result = op.operate(tableset)

    assert "old_name" not in result
    assert "new_name" in result

    pdt.assert_frame_equal(result.get_df("new_name"), df)
