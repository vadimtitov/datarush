import pandas as pd
import pandas.testing as pdt
import pytest

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.concatenate_tables import ConcatenateTables


def test_concatenate_tables_rows():
    df1 = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    df2 = pd.DataFrame({"id": [3], "value": ["c"]})

    tableset = Tableset(
        [
            Table("t1", df1),
            Table("t2", df2),
        ]
    )

    model = {
        "tables": ["t1", "t2"],
        "output_table": "combined",
        "how": "rows",
    }

    op = ConcatenateTables(model)
    result = op.operate(tableset)

    expected_df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})

    pdt.assert_frame_equal(result.get_df("combined"), expected_df)


def test_concatenate_tables_columns():
    df1 = pd.DataFrame({"a": [1, 2, 3]})
    df2 = pd.DataFrame({"b": ["x", "y", "z"]})

    tableset = Tableset(
        [
            Table("t1", df1),
            Table("t2", df2),
        ]
    )

    model = {
        "tables": ["t1", "t2"],
        "output_table": "wide",
        "how": "columns",
    }

    op = ConcatenateTables(model)
    result = op.operate(tableset)

    expected_df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    pdt.assert_frame_equal(result.get_df("wide"), expected_df)


def test_concatenate_columns_mismatched_rows_raises():
    df1 = pd.DataFrame({"x": [1, 2]})
    df2 = pd.DataFrame({"y": ["a", "b", "c"]})  # different row count

    tableset = Tableset(
        [
            Table("x", df1),
            Table("y", df2),
        ]
    )

    model = {
        "tables": ["x", "y"],
        "output_table": "fail_case",
        "how": "columns",
    }

    op = ConcatenateTables(model)

    with pytest.raises(ValueError, match="same number of rows"):
        op.operate(tableset)


def test_concatenate_columns_duplicate_columns_raises():
    df1 = pd.DataFrame({0: [1, 2], 1: [3, 4]})
    df2 = pd.DataFrame({0: [5, 6], 1: [7, 8]})  # overlapping columns (0, 1)

    tableset = Tableset(
        [
            Table("left", df1),
            Table("right", df2),
        ]
    )

    model = {
        "tables": ["left", "right"],
        "output_table": "dup_fail",
        "how": "columns",
    }

    op = ConcatenateTables(model)

    with pytest.raises(ValueError, match="Duplicate column names"):
        op.operate(tableset)


def test_concatenate_drop_tables():
    df1 = pd.DataFrame({"col": [1]})
    df2 = pd.DataFrame({"col": [2]})

    tableset = Tableset(
        [
            Table("a", df1),
            Table("b", df2),
        ]
    )

    model = {
        "tables": ["a", "b"],
        "output_table": "result",
        "how": "rows",
        "drop": True,
    }

    op = ConcatenateTables(model)
    result = op.operate(tableset)

    assert "a" not in result
    assert "b" not in result
    pdt.assert_frame_equal(result.get_df("result"), pd.DataFrame({"col": [1, 2]}))
