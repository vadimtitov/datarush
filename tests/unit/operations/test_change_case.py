import pandas as pd
import pandas.testing as pdt
import pytest

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.change_case import ChangeCase


def test_change_case_upper():
    """Test changing case to upper."""
    df = pd.DataFrame(
        {
            "name": ["alice", "bob", "charlie"],
            "department": ["it", "hr", "sales"],
            "salary": [50000, 45000, 55000],
        }
    )
    tableset = Tableset([Table("employees", df)])

    model = {
        "table": "employees",
        "columns": ["name", "department"],
        "case": "upper",
    }

    op = ChangeCase(model)
    result = op.operate(tableset)

    expected_df = pd.DataFrame(
        {
            "name": ["ALICE", "BOB", "CHARLIE"],
            "department": ["IT", "HR", "SALES"],
            "salary": [50000, 45000, 55000],
        }
    )
    pdt.assert_frame_equal(result.get_df("employees"), expected_df)


def test_change_case_lower():
    """Test changing case to lower."""
    df = pd.DataFrame(
        {
            "name": ["ALICE", "BOB", "CHARLIE"],
            "department": ["IT", "HR", "SALES"],
            "salary": [50000, 45000, 55000],
        }
    )
    tableset = Tableset([Table("employees", df)])

    model = {
        "table": "employees",
        "columns": ["name", "department"],
        "case": "lower",
    }

    op = ChangeCase(model)
    result = op.operate(tableset)

    expected_df = pd.DataFrame(
        {
            "name": ["alice", "bob", "charlie"],
            "department": ["it", "hr", "sales"],
            "salary": [50000, 45000, 55000],
        }
    )
    pdt.assert_frame_equal(result.get_df("employees"), expected_df)


def test_change_case_capitalize():
    """Test changing case to capitalize."""
    df = pd.DataFrame(
        {
            "name": ["alice smith", "bob jones", "charlie brown"],
            "department": ["it", "hr", "sales"],
        }
    )
    tableset = Tableset([Table("employees", df)])

    model = {
        "table": "employees",
        "columns": ["name"],
        "case": "capitalize",
    }

    op = ChangeCase(model)
    result = op.operate(tableset)

    expected_df = pd.DataFrame(
        {
            "name": ["Alice smith", "Bob jones", "Charlie brown"],
            "department": ["it", "hr", "sales"],
        }
    )
    pdt.assert_frame_equal(result.get_df("employees"), expected_df)


def test_change_case_title():
    """Test changing case to title."""
    df = pd.DataFrame(
        {
            "name": ["alice smith", "bob jones", "charlie brown"],
            "department": ["it", "hr", "sales"],
        }
    )
    tableset = Tableset([Table("employees", df)])

    model = {
        "table": "employees",
        "columns": ["name"],
        "case": "title",
    }

    op = ChangeCase(model)
    result = op.operate(tableset)

    expected_df = pd.DataFrame(
        {
            "name": ["Alice Smith", "Bob Jones", "Charlie Brown"],
            "department": ["it", "hr", "sales"],
        }
    )
    pdt.assert_frame_equal(result.get_df("employees"), expected_df)


def test_change_case_swapcase():
    """Test changing case to swapcase."""
    df = pd.DataFrame(
        {
            "name": ["Alice", "bOB", "CHARLIE"],
            "department": ["IT", "hr", "Sales"],
        }
    )
    tableset = Tableset([Table("employees", df)])

    model = {
        "table": "employees",
        "columns": ["name", "department"],
        "case": "swapcase",
    }

    op = ChangeCase(model)
    result = op.operate(tableset)

    expected_df = pd.DataFrame(
        {
            "name": ["aLICE", "Bob", "charlie"],
            "department": ["it", "HR", "sALES"],
        }
    )
    pdt.assert_frame_equal(result.get_df("employees"), expected_df)


def test_change_case_casefold():
    """Test changing case to casefold."""
    df = pd.DataFrame(
        {
            "name": ["ALICE", "Bob", "CHARLIE"],
            "department": ["IT", "hr", "Sales"],
        }
    )
    tableset = Tableset([Table("employees", df)])

    model = {
        "table": "employees",
        "columns": ["name", "department"],
        "case": "casefold",
    }

    op = ChangeCase(model)
    result = op.operate(tableset)

    expected_df = pd.DataFrame(
        {
            "name": ["alice", "bob", "charlie"],
            "department": ["it", "hr", "sales"],
        }
    )
    pdt.assert_frame_equal(result.get_df("employees"), expected_df)


def test_change_case_all_string_columns():
    """Test changing case on all string columns when no specific columns provided."""
    df = pd.DataFrame(
        {
            "name": ["alice", "bob", "charlie"],
            "department": ["it", "hr", "sales"],
            "salary": [50000, 45000, 55000],  # numeric column should be ignored
        }
    )
    tableset = Tableset([Table("employees", df)])

    model = {
        "table": "employees",
        "columns": [],  # empty list means all string columns
        "case": "upper",
    }

    op = ChangeCase(model)
    result = op.operate(tableset)

    expected_df = pd.DataFrame(
        {
            "name": ["ALICE", "BOB", "CHARLIE"],
            "department": ["IT", "HR", "SALES"],
            "salary": [50000, 45000, 55000],  # unchanged
        }
    )
    pdt.assert_frame_equal(result.get_df("employees"), expected_df)


def test_change_case_missing_column():
    """Test changing case with non-existent column (should raise error)."""
    df = pd.DataFrame({"name": ["alice", "bob"]})
    tableset = Tableset([Table("employees", df)])

    model = {
        "table": "employees",
        "columns": ["missing_column"],
        "case": "upper",
    }

    op = ChangeCase(model)

    with pytest.raises(ValueError, match="Columns not found in table: \\['missing_column'\\]"):
        op.operate(tableset)


def test_change_case_numeric_values():
    """Test changing case on numeric values (should convert to string first)."""
    df = pd.DataFrame(
        {
            "name": ["alice", "bob"],
            "numbers": [123, 456],
        }
    )
    tableset = Tableset([Table("employees", df)])

    model = {
        "table": "employees",
        "columns": ["numbers"],
        "case": "upper",
    }

    op = ChangeCase(model)
    result = op.operate(tableset)

    expected_df = pd.DataFrame(
        {
            "name": ["alice", "bob"],
            "numbers": ["123", "456"],  # converted to string and upper case
        }
    )
    pdt.assert_frame_equal(result.get_df("employees"), expected_df)


def test_change_case_summary():
    """Test operation summary generation."""
    model = {
        "table": "employees",
        "columns": ["name", "department"],
        "case": "upper",
    }

    op = ChangeCase(model)
    summary = op.summary()

    assert "Change case to **upper** in **name**, **department** of `employees`" in summary


def test_change_case_summary_all_columns():
    """Test operation summary when using all string columns."""
    model = {
        "table": "employees",
        "columns": [],
        "case": "lower",
    }

    op = ChangeCase(model)
    summary = op.summary()

    assert "Change case to **lower** in all string columns of `employees`" in summary
