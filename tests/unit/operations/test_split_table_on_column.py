import pandas as pd
import pandas.testing as pdt
import pytest

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.split_table_on_column import SplitTableOnColumn


def test_split_table_on_column_basic():
    """Test basic table splitting functionality."""
    df = pd.DataFrame(
        {
            "name": ["Alice", "Bob", "Charlie", "David"],
            "department": ["IT", "HR", "IT", "Sales"],
            "salary": [50000, 45000, 55000, 60000],
        }
    )
    tableset = Tableset([Table("employees", df)])

    model = {
        "table": "employees",
        "split_column": "department",
        "drop_original_table": False,
        "drop_split_column": False,
    }

    op = SplitTableOnColumn(model)
    result = op.operate(tableset)

    # Check that original table still exists
    assert "employees" in result

    # Check that new tables were created
    assert "IT" in result
    assert "HR" in result
    assert "Sales" in result

    # Check IT table content
    it_df = result.get_df("IT")
    expected_it = pd.DataFrame(
        {
            "name": ["Alice", "Charlie"],
            "department": ["IT", "IT"],
            "salary": [50000, 55000],
        }
    )
    pdt.assert_frame_equal(it_df, expected_it)

    # Check HR table content
    hr_df = result.get_df("HR")
    expected_hr = pd.DataFrame(
        {
            "name": ["Bob"],
            "department": ["HR"],
            "salary": [45000],
        }
    )
    pdt.assert_frame_equal(hr_df, expected_hr)

    # Check Sales table content
    sales_df = result.get_df("Sales")
    expected_sales = pd.DataFrame(
        {
            "name": ["David"],
            "department": ["Sales"],
            "salary": [60000],
        }
    )
    pdt.assert_frame_equal(sales_df, expected_sales)


def test_split_table_on_column_drop_original():
    """Test splitting with dropping the original table."""
    df = pd.DataFrame(
        {
            "name": ["Alice", "Bob"],
            "department": ["IT", "HR"],
            "salary": [50000, 45000],
        }
    )
    tableset = Tableset([Table("employees", df)])

    model = {
        "table": "employees",
        "split_column": "department",
        "drop_original_table": True,
        "drop_split_column": False,
    }

    op = SplitTableOnColumn(model)
    result = op.operate(tableset)

    # Check that original table was removed
    assert "employees" not in result

    # Check that new tables were created
    assert "IT" in result
    assert "HR" in result


def test_split_table_on_column_drop_split_column():
    """Test splitting with dropping the split column from resulting tables."""
    df = pd.DataFrame(
        {
            "name": ["Alice", "Bob", "Charlie"],
            "department": ["IT", "HR", "IT"],
            "salary": [50000, 45000, 55000],
        }
    )
    tableset = Tableset([Table("employees", df)])

    model = {
        "table": "employees",
        "split_column": "department",
        "drop_original_table": False,
        "drop_split_column": True,
    }

    op = SplitTableOnColumn(model)
    result = op.operate(tableset)

    # Check IT table content (without department column)
    it_df = result.get_df("IT")
    expected_it = pd.DataFrame(
        {
            "name": ["Alice", "Charlie"],
            "salary": [50000, 55000],
        }
    )
    pdt.assert_frame_equal(it_df, expected_it)

    # Check HR table content (without department column)
    hr_df = result.get_df("HR")
    expected_hr = pd.DataFrame(
        {
            "name": ["Bob"],
            "salary": [45000],
        }
    )
    pdt.assert_frame_equal(hr_df, expected_hr)


def test_split_table_on_column_drop_both():
    """Test splitting with dropping both original table and split column."""
    df = pd.DataFrame(
        {
            "name": ["Alice", "Bob"],
            "department": ["IT", "HR"],
            "salary": [50000, 45000],
        }
    )
    tableset = Tableset([Table("employees", df)])

    model = {
        "table": "employees",
        "split_column": "department",
        "drop_original_table": True,
        "drop_split_column": True,
    }

    op = SplitTableOnColumn(model)
    result = op.operate(tableset)

    # Check that original table was removed
    assert "employees" not in result

    # Check IT table content (without department column)
    it_df = result.get_df("IT")
    expected_it = pd.DataFrame(
        {
            "name": ["Alice"],
            "salary": [50000],
        }
    )
    pdt.assert_frame_equal(it_df, expected_it)


def test_split_table_on_column_missing_column():
    """Test splitting with non-existent column (should raise error)."""
    df = pd.DataFrame({"name": ["Alice"], "salary": [50000]})
    tableset = Tableset([Table("employees", df)])

    model = {
        "table": "employees",
        "split_column": "missing_column",
        "drop_original_table": False,
        "drop_split_column": False,
    }

    op = SplitTableOnColumn(model)

    with pytest.raises(ValueError, match="Column 'missing_column' not found in table"):
        op.operate(tableset)


def test_split_table_on_column_single_value():
    """Test splitting when all values in split column are the same."""
    df = pd.DataFrame(
        {
            "name": ["Alice", "Bob", "Charlie"],
            "department": ["IT", "IT", "IT"],
            "salary": [50000, 45000, 55000],
        }
    )
    tableset = Tableset([Table("employees", df)])

    model = {
        "table": "employees",
        "split_column": "department",
        "drop_original_table": False,
        "drop_split_column": False,
    }

    op = SplitTableOnColumn(model)
    result = op.operate(tableset)

    # Should create only one new table
    assert "IT" in result
    assert len([t for t in result if t != "employees"]) == 1

    # Check IT table content
    it_df = result.get_df("IT")
    pdt.assert_frame_equal(it_df, df)


def test_split_table_on_column_summary():
    """Test operation summary generation."""
    model = {
        "table": "employees",
        "split_column": "department",
        "drop_original_table": True,
        "drop_split_column": False,
    }

    op = SplitTableOnColumn(model)
    summary = op.summary()

    assert "Split `employees` on column **department**" in summary
    assert "drop original" in summary
    assert "keep split column" in summary


def test_split_table_on_column_summary_keep_original():
    """Test operation summary when keeping original table."""
    model = {
        "table": "employees",
        "split_column": "department",
        "drop_original_table": False,
        "drop_split_column": True,
    }

    op = SplitTableOnColumn(model)
    summary = op.summary()

    assert "Split `employees` on column **department**" in summary
    assert "keep original" in summary
    assert "drop split column" in summary
