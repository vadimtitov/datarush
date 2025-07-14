"""Test strip operation."""

import pandas as pd
import pytest

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.strip import Strip


class TestStrip:
    """Test strip operation."""

    def test_strip_both_sides_default(self):
        """Test stripping whitespace from both sides."""
        df = pd.DataFrame(
            {"text": ["  hello  ", "  world  ", "  test  "], "other": ["  a  ", "  b  ", "  c  "]}
        )

        tableset = Tableset([Table("test_table", df)])

        model = {
            "table": "test_table",
            "strip_type": "both",
            "chars": "",
        }

        operation = Strip(model)
        result = operation.operate(tableset)

        expected = pd.DataFrame({"text": ["hello", "world", "test"], "other": ["a", "b", "c"]})

        pd.testing.assert_frame_equal(result.get_df("test_table"), expected)

    def test_strip_left_only(self):
        """Test stripping whitespace from left side only."""
        df = pd.DataFrame({"text": ["  hello  ", "  world  ", "  test  "]})

        tableset = Tableset([Table("test_table", df)])

        model = {
            "table": "test_table",
            "strip_type": "left",
        }

        operation = Strip(model)
        result = operation.operate(tableset)

        expected = pd.DataFrame({"text": ["hello  ", "world  ", "test  "]})

        pd.testing.assert_frame_equal(result.get_df("test_table"), expected)

    def test_strip_right_only(self):
        """Test stripping whitespace from right side only."""
        df = pd.DataFrame({"text": ["  hello  ", "  world  ", "  test  "]})

        tableset = Tableset([Table("test_table", df)])

        model = {
            "table": "test_table",
            "strip_type": "right",
        }

        operation = Strip(model)
        result = operation.operate(tableset)

        expected = pd.DataFrame({"text": ["  hello", "  world", "  test"]})

        pd.testing.assert_frame_equal(result.get_df("test_table"), expected)

    def test_strip_custom_characters(self):
        """Test stripping custom characters."""
        df = pd.DataFrame({"text": ["***hello***", "###world###", "---test---"]})

        tableset = Tableset([Table("test_table", df)])

        model = {
            "table": "test_table",
            "strip_type": "both",
            "chars": "*#-",
        }

        operation = Strip(model)
        result = operation.operate(tableset)

        expected = pd.DataFrame({"text": ["hello", "world", "test"]})

        pd.testing.assert_frame_equal(result.get_df("test_table"), expected)

    def test_strip_specific_columns(self):
        """Test stripping only specific columns."""
        df = pd.DataFrame(
            {"text": ["  hello  ", "  world  "], "other": ["  a  ", "  b  "], "number": [1, 2]}
        )

        tableset = Tableset([Table("test_table", df)])

        model = {
            "table": "test_table",
            "columns": ["text"],
            "strip_type": "both",
        }

        operation = Strip(model)
        result = operation.operate(tableset)

        expected = pd.DataFrame(
            {"text": ["hello", "world"], "other": ["  a  ", "  b  "], "number": [1, 2]}
        )

        pd.testing.assert_frame_equal(result.get_df("test_table"), expected)

    def test_strip_all_string_columns_when_empty(self):
        """Test stripping all string columns when columns list is empty."""
        df = pd.DataFrame(
            {"text": ["  hello  ", "  world  "], "other": ["  a  ", "  b  "], "number": [1, 2]}
        )

        tableset = Tableset([Table("test_table", df)])

        model = {
            "table": "test_table",
            "strip_type": "both",
        }

        operation = Strip(model)
        result = operation.operate(tableset)

        expected = pd.DataFrame(
            {"text": ["hello", "world"], "other": ["a", "b"], "number": [1, 2]}
        )

        pd.testing.assert_frame_equal(result.get_df("test_table"), expected)

    def test_strip_missing_column_error(self):
        """Test that missing columns raise an error."""
        df = pd.DataFrame({"text": ["  hello  ", "  world  "]})

        tableset = Tableset([Table("test_table", df)])

        model = {
            "table": "test_table",
            "columns": ["text", "missing"],
            "strip_type": "both",
        }

        operation = Strip(model)

        with pytest.raises(ValueError, match="Columns not found in table: \\['missing'\\]"):
            operation.operate(tableset)

    def test_strip_empty_strings(self):
        """Test stripping with empty strings."""
        df = pd.DataFrame({"text": ["", "  ", "hello", "  hello  "]})

        tableset = Tableset([Table("test_table", df)])

        model = {
            "table": "test_table",
            "strip_type": "both",
        }

        operation = Strip(model)
        result = operation.operate(tableset)

        expected = pd.DataFrame({"text": ["", "", "hello", "hello"]})

        pd.testing.assert_frame_equal(result.get_df("test_table"), expected)

    def test_strip_non_string_values(self):
        """Test stripping with non-string values (should convert to string)."""
        df = pd.DataFrame({"mixed": ["  hello  ", 123, 45.67, True]})

        tableset = Tableset([Table("test_table", df)])

        model = {
            "table": "test_table",
            "strip_type": "both",
        }

        operation = Strip(model)
        result = operation.operate(tableset)

        expected = pd.DataFrame({"mixed": ["hello", "123", "45.67", "True"]})

        pd.testing.assert_frame_equal(result.get_df("test_table"), expected)

    def test_summary_with_specific_columns(self):
        """Test summary generation with specific columns."""
        model = {
            "table": "test_table",
            "columns": ["col1", "col2"],
            "strip_type": "left",
        }
        operation = Strip(model)

        summary = operation.summary()
        assert "Strip left whitespace from **col1**, **col2** in `test_table`" == summary

    def test_summary_with_all_columns(self):
        """Test summary generation with all string columns."""
        model = {
            "table": "test_table",
            "strip_type": "right",
        }
        operation = Strip(model)

        summary = operation.summary()
        assert "Strip right whitespace from all string columns in `test_table`" == summary

    def test_summary_with_custom_characters(self):
        """Test summary generation with custom characters."""
        model = {
            "table": "test_table",
            "columns": ["col1"],
            "strip_type": "both",
            "chars": "*#",
        }
        operation = Strip(model)

        summary = operation.summary()
        assert "Strip both characters '*#' from **col1** in `test_table`" == summary
