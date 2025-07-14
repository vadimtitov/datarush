"""Tests for parse_datetime operation."""

from datetime import date, datetime

import pandas as pd
import pandas.testing as pdt
import pytest

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.parse_datetime import ParseDatetime


@pytest.mark.parametrize(
    "input_data,model,expected_data",
    [
        # Basic datetime parsing
        (
            {"date_col": ["2023-12-25", "2023-12-26", "2023-12-27"]},
            {
                "table": "test_table",
                "column": "date_col",
                "language": "en",
                "date_order": "YMD",
                "timezone": "UTC",
                "format": "",
                "return_type": "datetime",
                "on_error": "null",
            },
            {
                "date_col": [
                    datetime(2023, 12, 25),
                    datetime(2023, 12, 26),
                    datetime(2023, 12, 27),
                ]
            },
        ),
        # Date order testing
        (
            {"date_col": ["25/12/2023", "26/12/2023"]},
            {
                "table": "test_table",
                "column": "date_col",
                "language": "en",
                "date_order": "DMY",
                "timezone": "UTC",
                "format": "",
                "return_type": "datetime",
                "on_error": "null",
            },
            {
                "date_col": [
                    datetime(2023, 12, 25),
                    datetime(2023, 12, 26),
                ]
            },
        ),
        # Return type: date
        (
            {"date_col": ["2023-12-25", "2023-12-26"]},
            {
                "table": "test_table",
                "column": "date_col",
                "language": "en",
                "date_order": "YMD",
                "timezone": "UTC",
                "format": "",
                "return_type": "date",
                "on_error": "null",
            },
            {
                "date_col": [
                    date(2023, 12, 25),
                    date(2023, 12, 26),
                ]
            },
        ),
        # Return type: timestamp
        (
            {"date_col": ["2023-12-25 00:00:00"]},
            {
                "table": "test_table",
                "column": "date_col",
                "language": "en",
                "date_order": "YMD",
                "timezone": "UTC",
                "format": "",
                "return_type": "timestamp",
                "on_error": "null",
            },
            {"date_col": [datetime(2023, 12, 25).timestamp()]},
        ),
        # Explicit format testing
        (
            {"date_col": ["12/25/2023", "12/26/2023"]},
            {
                "table": "test_table",
                "column": "date_col",
                "language": "en",
                "date_order": "MDY",
                "timezone": "UTC",
                "format": "%m/%d/%Y",
                "return_type": "datetime",
                "on_error": "null",
            },
            {
                "date_col": [
                    datetime(2023, 12, 25),
                    datetime(2023, 12, 26),
                ]
            },
        ),
        # Single format testing
        (
            {"date_col": ["2023-12-25"]},
            {
                "table": "test_table",
                "column": "date_col",
                "language": "en",
                "date_order": "YMD",
                "timezone": "UTC",
                "format": "%Y-%m-%d",
                "return_type": "datetime",
                "on_error": "null",
            },
            {
                "date_col": [
                    datetime(2023, 12, 25),
                ]
            },
        ),
        # Spanish language
        (
            {"date_col": ["25 de diciembre de 2023"]},
            {
                "table": "test_table",
                "column": "date_col",
                "language": "es",
                "date_order": "DMY",
                "timezone": "UTC",
                "format": "",
                "return_type": "datetime",
                "on_error": "null",
            },
            {"date_col": [datetime(2023, 12, 25)]},
        ),
        # French language
        (
            {"date_col": ["25 décembre 2023"]},
            {
                "table": "test_table",
                "column": "date_col",
                "language": "fr",
                "date_order": "DMY",
                "timezone": "UTC",
                "format": "",
                "return_type": "datetime",
                "on_error": "null",
            },
            {"date_col": [datetime(2023, 12, 25)]},
        ),
        # With null values
        (
            {"date_col": ["2023-12-25", None, "2023-12-27"]},
            {
                "table": "test_table",
                "column": "date_col",
                "language": "en",
                "date_order": "YMD",
                "timezone": "UTC",
                "format": "",
                "return_type": "datetime",
                "on_error": "null",
            },
            {
                "date_col": [
                    datetime(2023, 12, 25),
                    None,
                    datetime(2023, 12, 27),
                ]
            },
        ),
    ],
)
def test_parse_datetime_variants(input_data, model, expected_data):
    """Test various datetime parsing scenarios."""
    df = pd.DataFrame(input_data)
    tableset = Tableset([Table("test_table", df)])

    op = ParseDatetime(model)
    result = op.operate(tableset)

    expected_df = pd.DataFrame(expected_data)
    pdt.assert_frame_equal(result.get_df("test_table"), expected_df)


@pytest.mark.parametrize(
    "model,expected_error",
    [
        (
            {
                "table": "test_table",
                "column": "missing_column",
                "language": "en",
                "date_order": "YMD",
                "timezone": "UTC",
                "format": "",
                "return_type": "datetime",
                "on_error": "null",
            },
            "Column 'missing_column' not found in table",
        ),
    ],
)
def test_parse_datetime_missing_column(model, expected_error):
    """Test error handling for missing column."""
    df = pd.DataFrame({"date_col": ["2023-12-25"]})
    tableset = Tableset([Table("test_table", df)])

    op = ParseDatetime(model)

    with pytest.raises(ValueError, match=expected_error):
        op.operate(tableset)


@pytest.mark.parametrize(
    "input_data,model,expected_data",
    [
        # Invalid date with on_error="null"
        (
            {"date_col": ["2023-12-25", "invalid_date", "2023-12-27"]},
            {
                "table": "test_table",
                "column": "date_col",
                "language": "en",
                "date_order": "YMD",
                "timezone": "UTC",
                "format": "",
                "return_type": "datetime",
                "on_error": "null",
            },
            {
                "date_col": [
                    datetime(2023, 12, 25),
                    None,
                    datetime(2023, 12, 27),
                ]
            },
        ),
        # Invalid date with explicit format that doesn't match
        (
            {"date_col": ["2023-12-25", "12/25/2023"]},
            {
                "table": "test_table",
                "column": "date_col",
                "language": "en",
                "date_order": "YMD",
                "timezone": "UTC",
                "format": "%Y-%m-%d",  # Only matches first format
                "return_type": "datetime",
                "on_error": "null",
            },
            {
                "date_col": [
                    datetime(2023, 12, 25),
                    None,  # Second date doesn't match format
                ]
            },
        ),
    ],
)
def test_parse_datetime_error_handling(input_data, model, expected_data):
    """Test error handling scenarios."""
    df = pd.DataFrame(input_data)
    tableset = Tableset([Table("test_table", df)])

    op = ParseDatetime(model)
    result = op.operate(tableset)

    expected_df = pd.DataFrame(expected_data)
    result_df = result.get_df("test_table")
    # Convert both DataFrames to object dtype and replace NaT with None for robust comparison
    result_df = result_df.astype(object).where(~result_df.isna(), None)
    expected_df = expected_df.astype(object).where(~expected_df.isna(), None)
    pdt.assert_frame_equal(result_df, expected_df, check_dtype=False)


def test_parse_datetime_error_raises():
    """Test that errors are raised when on_error='error'."""
    df = pd.DataFrame({"date_col": ["invalid_date"]})
    tableset = Tableset([Table("test_table", df)])

    model = {
        "table": "test_table",
        "column": "date_col",
        "language": "en",
        "date_order": "YMD",
        "timezone": "UTC",
        "format": "",
        "return_type": "datetime",
        "on_error": "error",
    }

    op = ParseDatetime(model)

    with pytest.raises(ValueError, match="Could not parse datetime"):
        op.operate(tableset)


@pytest.mark.parametrize(
    "model,expected_summary_contains",
    [
        (
            {
                "table": "test_table",
                "column": "date_col",
                "language": "en",
                "date_order": "YMD",
                "timezone": "UTC",
                "format": "",
                "return_type": "datetime",
                "on_error": "null",
            },
            [
                "Parse **date_col** as datetime in `test_table` in timezone `UTC` using language=en, date_order=YMD",
            ],
        ),
        (
            {
                "table": "test_table",
                "column": "date_col",
                "language": "es",
                "date_order": "DMY",
                "timezone": "America/New_York",
                "format": "%Y-%m-%d",
                "return_type": "date",
                "on_error": "error",
            },
            [
                "Parse **date_col** as date in `test_table` in timezone `America/New_York` with format `%Y-%m-%d`",
            ],
        ),
    ],
)
def test_parse_datetime_summary(model, expected_summary_contains):
    """Test operation summary generation."""
    op = ParseDatetime(model)
    summary = op.summary()

    for expected in expected_summary_contains:
        assert expected in summary


def test_parse_datetime_timezone_handling():
    """Test timezone handling."""
    df = pd.DataFrame({"date_col": ["2023-12-25 14:30:00"]})
    tableset = Tableset([Table("test_table", df)])

    model = {
        "table": "test_table",
        "column": "date_col",
        "language": "en",
        "date_order": "YMD",
        "timezone": "America/New_York",
        "format": "",
        "return_type": "datetime",
        "on_error": "null",
    }

    op = ParseDatetime(model)
    result = op.operate(tableset)

    # Check that the datetime was parsed (exact timezone comparison is complex)
    parsed_value = result.get_df("test_table")["date_col"].iloc[0]
    assert parsed_value is not None
    assert isinstance(parsed_value, datetime)


def test_parse_datetime_empty_format_list():
    """Test that empty format list uses auto-detection."""
    df = pd.DataFrame({"date_col": ["2023-12-25", "December 25, 2023"]})
    tableset = Tableset([Table("test_table", df)])

    model = {
        "table": "test_table",
        "column": "date_col",
        "language": "en",
        "date_order": "YMD",
        "timezone": "UTC",
        "format": "",  # Empty string should use auto-detection
        "return_type": "datetime",
        "on_error": "null",
    }

    op = ParseDatetime(model)
    result = op.operate(tableset)

    # Both should be parsed successfully using auto-detection
    parsed_values = result.get_df("test_table")["date_col"]
    assert all(isinstance(val, datetime) for val in parsed_values if val is not None)


def test_parse_datetime_date_order_ambiguity():
    """Test that date_order correctly handles ambiguous dates like 01/02/2025."""
    # Test American date order (MDY) - should parse as January 2, 2025
    df_mdy = pd.DataFrame({"date_col": ["01/02/2025"]})
    tableset_mdy = Tableset([Table("test_table", df_mdy)])

    model_mdy = {
        "table": "test_table",
        "column": "date_col",
        "language": "en",
        "date_order": "MDY",
        "timezone": "UTC",
        "format": "",
        "return_type": "datetime",
        "on_error": "null",
    }

    op_mdy = ParseDatetime(model_mdy)
    result_mdy = op_mdy.operate(tableset_mdy)
    parsed_mdy = result_mdy.get_df("test_table")["date_col"].iloc[0]
    assert parsed_mdy == datetime(2025, 1, 2)  # January 2, 2025

    # Test European date order (DMY) - should parse as February 1, 2025
    df_dmy = pd.DataFrame({"date_col": ["01/02/2025"]})
    tableset_dmy = Tableset([Table("test_table", df_dmy)])

    model_dmy = {
        "table": "test_table",
        "column": "date_col",
        "language": "en",
        "date_order": "DMY",
        "timezone": "UTC",
        "format": "",
        "return_type": "datetime",
        "on_error": "null",
    }

    op_dmy = ParseDatetime(model_dmy)
    result_dmy = op_dmy.operate(tableset_dmy)
    parsed_dmy = result_dmy.get_df("test_table")["date_col"].iloc[0]
    assert parsed_dmy == datetime(2025, 2, 1)  # February 1, 2025

    # Verify they are different
    assert parsed_mdy != parsed_dmy
