import pandas as pd
import pandas.testing as pdt
import pytest

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.deduplicate_rows import DeduplicateRows


@pytest.mark.parametrize(
    "columns,keep,expected_ids",
    [
        (["name", "age"], "first", [0, 1, 3]),
        (["name", "age"], "last", [0, 2, 3]),
        (["name"], "first", [0, 1, 3]),
        (["name"], "last", [0, 2, 3]),
    ],
)
def test_deduplicate_rows(columns, keep, expected_ids):
    df = pd.DataFrame(
        {
            "name": ["Alice", "Bob", "Bob", "Charlie"],
            "age": [25, 30, 30, 40],
        },
        index=[0, 1, 2, 3],
    )
    tableset = Tableset([Table("people", df)])

    model = {
        "table": "people",
        "columns": columns,
        "keep": keep,
    }

    op = DeduplicateRows(model)
    result = op.operate(tableset)

    expected_df = df.loc[expected_ids].reset_index(drop=True)
    actual_df = result.get_df("people").reset_index(drop=True)

    pdt.assert_frame_equal(actual_df, expected_df)


def test_deduplicate_rows_keep_none():
    df = pd.DataFrame(
        {
            "x": [1, 2, 2, 3, 1],
            "y": ["a", "b", "b", "c", "a"],
        }
    )
    tableset = Tableset([Table("data", df)])

    model = {
        "table": "data",
        "columns": ["x", "y"],
        "keep": "none",
    }

    op = DeduplicateRows(model)
    result = op.operate(tableset)

    expected_df = pd.DataFrame(
        {
            "x": [3],
            "y": ["c"],
        }
    )

    pdt.assert_frame_equal(result.get_df("data").reset_index(drop=True), expected_df)
