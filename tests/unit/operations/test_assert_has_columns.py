import pandas as pd
import pytest

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.assert_has_columns import AssertHasColumns


def test_assert_has_columns_pass_allow_extra():
    df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
    tableset = Tableset([Table("demo", df)])

    model = {
        "table": "demo",
        "columns": ["a", "b"],
        "allow_extra": True,
    }

    op = AssertHasColumns(model)
    result = op.operate(tableset)
    assert result.get_df("demo").equals(df)


def test_assert_has_columns_pass_exact():
    df = pd.DataFrame({"a": [1], "b": [2]})
    tableset = Tableset([Table("demo", df)])

    model = {
        "table": "demo",
        "columns": ["a", "b"],
        "allow_extra": False,
    }

    op = AssertHasColumns(model)
    result = op.operate(tableset)
    assert result.get_df("demo").equals(df)


def test_assert_has_columns_fail_missing():
    df = pd.DataFrame({"a": [1]})
    tableset = Tableset([Table("demo", df)])

    model = {
        "table": "demo",
        "columns": ["a", "b"],
        "allow_extra": True,
    }

    op = AssertHasColumns(model)
    with pytest.raises(ValueError, match="Missing columns.*b"):
        op.operate(tableset)


def test_assert_has_columns_fail_extra():
    df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
    tableset = Tableset([Table("demo", df)])

    model = {
        "table": "demo",
        "columns": ["a", "b"],
        "allow_extra": False,
    }

    op = AssertHasColumns(model)
    with pytest.raises(ValueError, match="Unexpected extra columns.*c"):
        op.operate(tableset)
