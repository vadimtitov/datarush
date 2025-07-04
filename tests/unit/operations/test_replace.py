import pandas as pd
import pandas.testing as pdt
import pytest

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.replace import Replace


@pytest.mark.parametrize(
    "model,expected",
    [
        (
            # Exact match on one column
            {
                "table": "people",
                "columns": ["city"],
                "to_replace": {"London": "LON", "Paris": "PAR"},
                "regex": False,
            },
            pd.DataFrame(
                {
                    "name": ["Alice", "Bob", "Charlie", "123"],
                    "city": ["LON", "PAR", "LON", "Berlin"],
                    "notes": ["abc123", "xyz", "123abc", "000"],
                }
            ),
        ),
        (
            # Exact match across all columns
            {
                "table": "people",
                "columns": [],
                "to_replace": {"London": "LON", "Bob": "Robert"},
                "regex": False,
            },
            pd.DataFrame(
                {
                    "name": ["Alice", "Robert", "Charlie", "123"],
                    "city": ["LON", "Paris", "LON", "Berlin"],
                    "notes": ["abc123", "xyz", "123abc", "000"],
                }
            ),
        ),
        (
            # Regex replacement on one column
            {
                "table": "people",
                "columns": ["notes"],
                "to_replace": {r"\d+": "<NUM>"},
                "regex": True,
            },
            pd.DataFrame(
                {
                    "name": ["Alice", "Bob", "Charlie", "123"],
                    "city": ["London", "Paris", "London", "Berlin"],
                    "notes": ["abc<NUM>", "xyz", "<NUM>abc", "<NUM>"],
                }
            ),
        ),
        (
            # Regex replacement on all columns
            {
                "table": "people",
                "columns": [],
                "to_replace": {r"\d+": "###"},
                "regex": True,
            },
            pd.DataFrame(
                {
                    "name": ["Alice", "Bob", "Charlie", "###"],
                    "city": ["London", "Paris", "London", "Berlin"],
                    "notes": ["abc###", "xyz", "###abc", "###"],
                }
            ),
        ),
    ],
)
def test_replace_variants(model, expected):
    df = pd.DataFrame(
        {
            "name": ["Alice", "Bob", "Charlie", "123"],
            "city": ["London", "Paris", "London", "Berlin"],
            "notes": ["abc123", "xyz", "123abc", "000"],
        }
    )

    tableset = Tableset([Table("people", df.copy())])
    op = Replace(model)
    result = op.operate(tableset)

    pdt.assert_frame_equal(result.get_df("people"), expected)
