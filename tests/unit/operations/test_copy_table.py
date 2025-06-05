import pandas as pd
import pandas.testing as pdt

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.copy_table import CopyTable


def test_copy_table_creates_deep_copy():
    df = pd.DataFrame({"col": [1, 2, 3]})
    tableset = Tableset([Table("original", df)])

    model = {
        "source_table": "original",
        "target_table": "copy",
    }

    op = CopyTable(model)
    result = op.operate(tableset)

    # Check both tables exist and are equal in value
    original_df = result.get_df("original")
    copy_df = result.get_df("copy")

    pdt.assert_frame_equal(copy_df, original_df)

    # Modify original to ensure it's a deep copy
    original_df.loc[0, "col"] = 999
    assert copy_df.loc[0, "col"] == 1  # copy should not be affected
