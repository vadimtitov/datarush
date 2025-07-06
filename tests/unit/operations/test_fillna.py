"""Test fillna operation."""

import pandas as pd
import pandas.testing as pdt
import pytest

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.fillna import FillNa


def test_fillna_constant():
    """Test filling with constant value."""
    df = pd.DataFrame(
        {
            "col1": [1, None, 3, None, 5],
            "col2": ["a", "", "c", None, "e"],
            "col3": [1.1, None, 3.3, None, 5.5],
        }
    )

    model = {"table": "test_table", "method": "constant", "value": "0"}

    tableset = Tableset([Table("test_table", df.copy())])
    op = FillNa(model)
    result = op.operate(tableset)
    result_df = result.get_df("test_table")

    # Check results
    assert result_df["col1"].iloc[1] == 0
    assert result_df["col1"].iloc[3] == 0
    # Empty string should remain unchanged (not treated as NA)
    assert result_df["col2"].iloc[1] == ""
    assert result_df["col2"].iloc[3] == "0"
    assert result_df["col3"].iloc[1] == 0.0
    assert result_df["col3"].iloc[3] == 0.0


def test_fillna_ffill():
    """Test forward fill method."""
    df = pd.DataFrame({"col1": [1, None, 3, None, 5], "col2": ["a", None, "c", None, "e"]})

    model = {"table": "test_table", "method": "ffill"}

    tableset = Tableset([Table("test_table", df.copy())])
    op = FillNa(model)
    result = op.operate(tableset)
    result_df = result.get_df("test_table")

    # Check forward fill results
    assert result_df["col1"].iloc[1] == 1
    assert result_df["col1"].iloc[3] == 3
    assert result_df["col2"].iloc[1] == "a"
    assert result_df["col2"].iloc[3] == "c"


def test_fillna_bfill():
    """Test backward fill method."""
    df = pd.DataFrame({"col1": [1, None, 3, None, 5], "col2": ["a", None, "c", None, "e"]})

    model = {"table": "test_table", "method": "bfill"}

    tableset = Tableset([Table("test_table", df.copy())])
    op = FillNa(model)
    result = op.operate(tableset)
    result_df = result.get_df("test_table")

    # Check backward fill results
    assert result_df["col1"].iloc[1] == 3
    assert result_df["col1"].iloc[3] == 5
    assert result_df["col2"].iloc[1] == "c"
    assert result_df["col2"].iloc[3] == "e"


def test_fillna_mean():
    """Test mean fill method."""
    df = pd.DataFrame(
        {"col1": [1, None, 3, None, 5], "col2": ["a", None, "c", None, "e"]}  # Non-numeric
    )

    model = {"table": "test_table", "method": "mean"}

    tableset = Tableset([Table("test_table", df.copy())])
    op = FillNa(model)
    result = op.operate(tableset)
    result_df = result.get_df("test_table")

    # Check mean fill results
    expected_mean = (1 + 3 + 5) / 3  # 3.0
    assert result_df["col1"].iloc[1] == expected_mean
    assert result_df["col1"].iloc[3] == expected_mean
    # Non-numeric column should use mode as fallback
    assert result_df["col2"].iloc[1] == "a"  # mode of ['a', 'c', 'e']


def test_fillna_median():
    """Test median fill method."""
    df = pd.DataFrame({"col1": [1, None, 3, None, 5], "col2": ["a", None, "c", None, "e"]})

    model = {"table": "test_table", "method": "median"}

    tableset = Tableset([Table("test_table", df.copy())])
    op = FillNa(model)
    result = op.operate(tableset)
    result_df = result.get_df("test_table")

    # Check median fill results
    assert result_df["col1"].iloc[1] == 3.0  # median of [1, 3, 5]
    assert result_df["col1"].iloc[3] == 3.0
    # Non-numeric column should use mode as fallback
    assert result_df["col2"].iloc[1] == "a"


def test_fillna_mode():
    """Test mode fill method."""
    df = pd.DataFrame({"col1": [1, None, 1, None, 2], "col2": ["a", None, "b", None, "a"]})

    model = {"table": "test_table", "method": "mode"}

    tableset = Tableset([Table("test_table", df.copy())])
    op = FillNa(model)
    result = op.operate(tableset)
    result_df = result.get_df("test_table")

    # Check mode fill results
    assert result_df["col1"].iloc[1] == 1  # mode of [1, 1, 2]
    assert result_df["col1"].iloc[3] == 1
    assert result_df["col2"].iloc[1] == "a"  # mode of ['a', 'b', 'a']
    assert result_df["col2"].iloc[3] == "a"


def test_fillna_specific_columns():
    """Test filling specific columns only."""
    df = pd.DataFrame({"col1": [1, None, 3], "col2": ["a", None, "c"], "col3": [1.1, None, 3.3]})

    model = {
        "table": "test_table",
        "columns": ["col1", "col3"],
        "method": "constant",
        "value": "0",
    }

    tableset = Tableset([Table("test_table", df.copy())])
    op = FillNa(model)
    result = op.operate(tableset)
    result_df = result.get_df("test_table")

    # Check that only specified columns were filled
    assert result_df["col1"].iloc[1] == 0
    assert result_df["col3"].iloc[1] == 0.0
    # col2 should remain unchanged
    assert pd.isna(result_df["col2"].iloc[1])


def test_fillna_with_limit():
    """Test fillna with limit parameter."""
    df = pd.DataFrame({"col1": [1, None, None, None, 5], "col2": ["a", None, None, None, "e"]})

    model = {"table": "test_table", "method": "ffill", "limit": 1}

    tableset = Tableset([Table("test_table", df.copy())])
    op = FillNa(model)
    result = op.operate(tableset)
    result_df = result.get_df("test_table")

    # Check that only one NaN was filled (limit=1)
    assert result_df["col1"].iloc[1] == 1  # filled
    assert pd.isna(result_df["col1"].iloc[2])  # not filled (limit reached)
    assert pd.isna(result_df["col1"].iloc[3])  # not filled (limit reached)


def test_fillna_datetime_constant():
    """Test filling datetime columns with constant."""
    df = pd.DataFrame({"date_col": pd.to_datetime(["2023-01-01", None, "2023-01-03"])})

    model = {"table": "test_table", "method": "constant", "value": "2023-01-02"}

    tableset = Tableset([Table("test_table", df.copy())])
    op = FillNa(model)
    result = op.operate(tableset)
    result_df = result.get_df("test_table")

    # Check datetime fill
    expected_date = pd.to_datetime("2023-01-02")
    assert result_df["date_col"].iloc[1] == expected_date


def test_fillna_invalid_numeric_value():
    """Test error handling for invalid numeric values."""
    df = pd.DataFrame({"col1": [1, None, 3]})

    model = {"table": "test_table", "method": "constant", "value": "not_a_number"}

    tableset = Tableset([Table("test_table", df.copy())])
    op = FillNa(model)

    # Should raise ValueError for invalid numeric conversion
    with pytest.raises(ValueError, match="Cannot convert 'not_a_number' to numeric"):
        op.operate(tableset)


def test_fillna_invalid_datetime_value():
    """Test error handling for invalid datetime values."""
    df = pd.DataFrame({"date_col": pd.to_datetime(["2023-01-01", None, "2023-01-03"])})

    model = {"table": "test_table", "method": "constant", "value": "invalid_date"}

    tableset = Tableset([Table("test_table", df.copy())])
    op = FillNa(model)

    # Should raise ValueError for invalid datetime conversion
    with pytest.raises(ValueError, match="Cannot convert 'invalid_date' to datetime"):
        op.operate(tableset)


def test_fillna_summary():
    """Test operation summary generation."""
    model = {
        "table": "test_table",
        "columns": ["col1", "col2"],
        "method": "constant",
        "value": "0",
    }

    op = FillNa(model)
    summary = op.summary()
    expected = "Fill NA values in **col1**, **col2** of `test_table` using constant value '0'"
    assert summary == expected


def test_fillna_summary_all_columns():
    """Test summary when no specific columns are selected."""
    model = {"table": "test_table", "method": "mean"}

    op = FillNa(model)
    summary = op.summary()
    expected = "Fill NA values in all columns of `test_table` using mean"
    assert summary == expected
