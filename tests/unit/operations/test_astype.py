import pandas as pd
import pandas.testing as pdt
import pytest

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.astype import AsType


@pytest.mark.parametrize(
    "dtype,input_values,expected_values",
    [
        ("int", ["1", "2", "3"], [1, 2, 3]),
        ("float", ["1.5", "2.5", "3.0"], [1.5, 2.5, 3.0]),
        ("string", [1, 2, 3], ["1", "2", "3"]),
        ("boolean", [1, 0, None], [True, False, pd.NA]),
        ("category", ["a", "b", "a"], pd.Series(["a", "b", "a"], dtype="category").tolist()),
    ],
)
def test_cast_column_type_supported_dtypes(dtype, input_values, expected_values):
    df = pd.DataFrame({"col": input_values})
    tableset = Tableset([Table("input", df)])

    model = {
        "table": "input",
        "column": "col",
        "dtype": dtype,
        "errors": "raise",
    }

    op = AsType(model)
    result = op.operate(tableset)
    output_df = result.get_df("input")

    # Cast expected to dtype for consistency before comparison
    expected_series = pd.Series(expected_values, name="col")
    if dtype in ["category", "boolean", "string"]:
        expected_series = expected_series.astype(dtype)
    elif dtype in ["datetime64[ns]", "timedelta64[ns]"]:
        expected_series = pd.Series(expected_values, name="col")  # already correct type
    elif dtype in ["int", "float"]:
        expected_series = pd.Series(expected_values, name="col", dtype=dtype)

    pdt.assert_series_equal(output_df["col"], expected_series, check_dtype=True)


def test_cast_column_type_raises_on_error():
    df = pd.DataFrame({"col": ["1", "bad", "3"]})
    tableset = Tableset([Table("input", df)])
    model = {
        "table": "input",
        "column": "col",
        "dtype": "int",
        "errors": "raise",
    }

    op = AsType(model)
    with pytest.raises(ValueError):
        op.operate(tableset)


def test_cast_column_type_ignore_errors():
    df = pd.DataFrame({"col": ["1", "bad", "3"]})
    tableset = Tableset([Table("input", df)])
    model = {
        "table": "input",
        "column": "col",
        "dtype": "int",
        "errors": "ignore",
    }

    op = AsType(model)
    result = op.operate(tableset)
    output_df = result.get_df("input")

    # Should remain unchanged since 'bad' cannot be cast
    expected_df = pd.DataFrame({"col": ["1", "bad", "3"]})
    pdt.assert_frame_equal(output_df, expected_df)
