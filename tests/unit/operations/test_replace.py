import pandas as pd
import pandas.testing as pdt
import pytest

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.replace import Replace


@pytest.mark.parametrize(
    "use_regex,to_replace,replacement,expected",
    [
        (
            False,
            "x",
            "X",
            pd.DataFrame(
                {
                    "a": ["X1", "X2", "y3"],
                    "b": ["z", "XX", "w"],  # fixed from "Xx"
                }
            ),
        ),
        (
            True,
            r"\d",
            "#",
            pd.DataFrame(
                {
                    "a": ["x#", "x#", "y#"],
                    "b": ["z", "xx", "w"],  # fixed from "x#"
                }
            ),
        ),
    ],
)
def test_replace_operation(use_regex, to_replace, replacement, expected):
    df = pd.DataFrame(
        {
            "a": ["x1", "x2", "y3"],
            "b": ["z", "xx", "w"],
        }
    )

    tableset = Tableset([Table("data", df)])
    model = {
        "table": "data",
        "columns": ["a", "b"],
        "to_replace": to_replace,
        "replacement": replacement,
        "use_regex": use_regex,
    }

    op = Replace(model)
    result = op.operate(tableset)
    pdt.assert_frame_equal(result.get_df("data"), expected)
