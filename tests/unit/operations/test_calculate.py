import pandas as pd
import pytest

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.calculate import Calculate


@pytest.mark.parametrize(
    "expression,expected",
    [
        ("a + b", [3, 7, 11]),
        ("a * 2 + b", [4, 9, 14]),
        ("(a + b) / 2", [1.5, 3.5, 5.5]),
        ("a ** 2 + b ** 2", [5, 29, 73]),
    ],
)
def test_calculate_math_expressions(expression, expected):
    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [2, 5, 8],
        }
    )
    tableset = Tableset([Table("math", df)])

    model = {
        "table": "math",
        "target_column": "result",
        "expression": expression,
    }

    op = Calculate(model)
    result = op.operate(tableset)

    expected_df = df.copy()
    expected_df["result"] = expected
    pd.testing.assert_frame_equal(result.get_df("math"), expected_df)


def test_calculate_invalid_expression_raises():
    df = pd.DataFrame({"x": [1, 2]})
    tableset = Tableset([Table("fail", df)])

    model = {
        "table": "fail",
        "target_column": "out",
        "expression": "x + unknown_var",
    }

    op = Calculate(model)
    with pytest.raises(NameError):
        op.operate(tableset)
