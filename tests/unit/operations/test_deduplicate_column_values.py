import pandas as pd
import pandas.testing as pdt

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.deduplicate_column_values import (
    DeduplicateColumnValues,
)


def test_deduplicate_column_values():
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "tags": [
                ["a", "b", "a", "c"],
                [{"k": 1}, {"k": 1}, {"k": 2}],
            ],
        }
    )
    tableset = Tableset([Table("data", df)])

    model = {
        "table": "data",
        "column": "tags",
    }

    op = DeduplicateColumnValues(model)
    result = op.operate(tableset)

    expected = pd.DataFrame(
        {
            "id": [1, 2],
            "tags": [
                ["a", "b", "c"],
                [{"k": 1}, {"k": 2}],
            ],
        }
    )

    pdt.assert_frame_equal(result.get_df("data"), expected)
