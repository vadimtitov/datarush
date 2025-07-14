import pandas as pd
import pandas.testing as pdt
import pytest

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.rename_columns import RenameColumns


def test_rename_columns_single():
    """Test renaming a single column."""
    df = pd.DataFrame({"old_name": [1, 2, 3], "b": [4, 5, 6]})
    tableset = Tableset([Table("test_table", df)])

    model = {
        "table": "test_table",
        "column_mapping": {"old_name": "new_name"},
    }

    op = RenameColumns(model)
    result = op.operate(tableset)

    expected_df = pd.DataFrame({"new_name": [1, 2, 3], "b": [4, 5, 6]})
    pdt.assert_frame_equal(result.get_df("test_table"), expected_df)


def test_rename_columns_multiple():
    """Test renaming multiple columns."""
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4], "col3": [5, 6]})
    tableset = Tableset([Table("test_table", df)])

    model = {
        "table": "test_table",
        "column_mapping": {"col1": "first", "col2": "second", "col3": "third"},
    }

    op = RenameColumns(model)
    result = op.operate(tableset)

    expected_df = pd.DataFrame({"first": [1, 2], "second": [3, 4], "third": [5, 6]})
    pdt.assert_frame_equal(result.get_df("test_table"), expected_df)


def test_rename_columns_empty_mapping():
    """Test renaming with empty mapping (should not change anything)."""
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    tableset = Tableset([Table("test_table", df)])

    model = {
        "table": "test_table",
        "column_mapping": {},
    }

    op = RenameColumns(model)
    result = op.operate(tableset)

    # Should remain unchanged
    pdt.assert_frame_equal(result.get_df("test_table"), df)


def test_rename_columns_missing_column():
    """Test renaming with non-existent column (should raise error)."""
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    tableset = Tableset([Table("test_table", df)])

    model = {
        "table": "test_table",
        "column_mapping": {"missing_col": "new_name"},
    }

    op = RenameColumns(model)

    with pytest.raises(ValueError, match="Columns not found in table: \\['missing_col'\\]"):
        op.operate(tableset)


def test_rename_columns_partial_mapping():
    """Test renaming only some columns while leaving others unchanged."""
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
    tableset = Tableset([Table("test_table", df)])

    model = {
        "table": "test_table",
        "column_mapping": {"a": "x", "c": "z"},
    }

    op = RenameColumns(model)
    result = op.operate(tableset)

    expected_df = pd.DataFrame({"x": [1, 2], "b": [3, 4], "z": [5, 6]})
    pdt.assert_frame_equal(result.get_df("test_table"), expected_df)


def test_rename_columns_summary():
    """Test operation summary generation."""
    model = {
        "table": "test_table",
        "column_mapping": {"old1": "new1", "old2": "new2"},
    }

    op = RenameColumns(model)
    summary = op.summary()

    assert "Rename columns in `test_table`" in summary
    assert "**old1** → **new1**" in summary
    assert "**old2** → **new2**" in summary


def test_rename_columns_empty_summary():
    """Test operation summary with empty mapping."""
    model = {
        "table": "test_table",
        "column_mapping": {},
    }

    op = RenameColumns(model)
    summary = op.summary()

    assert "No columns to rename in `test_table`" in summary
