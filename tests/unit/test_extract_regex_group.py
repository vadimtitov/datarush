import pandas as pd
import pandas.testing as pdt
import pytest

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.extract_regex_group import ExtractRegexGroup


def test_extract_named_group_success():
    df = pd.DataFrame({"email": ["alice@example.com", "bob@site.org"]})
    tableset = Tableset([Table("users", df)])

    model = {
        "table": "users",
        "column": "email",
        "regex": r"(?P<user>^[^@]+)@(?P<domain>[^@]+$)",
        "group": "user",
        "target_column": "username",
        "on_missing": "null",
    }

    op = ExtractRegexGroup(model)
    result = op.operate(tableset)

    expected = df.copy()
    expected["username"] = ["alice", "bob"]
    pdt.assert_frame_equal(result.get_df("users"), expected)


def test_extract_numbered_group_success():
    df = pd.DataFrame({"url": ["https://site.com/page1", "http://domain.net/home"]})
    tableset = Tableset([Table("web", df)])

    model = {
        "table": "web",
        "column": "url",
        "regex": r"https?://([^/]+)/",
        "group": "1",  # numeric group
        "target_column": "host",
        "on_missing": "null",
    }

    op = ExtractRegexGroup(model)
    result = op.operate(tableset)

    expected = df.copy()
    expected["host"] = ["site.com", "domain.net"]
    pdt.assert_frame_equal(result.get_df("web"), expected)


def test_extract_no_match_null_behavior():
    df = pd.DataFrame({"text": ["123abc", "no_digits"]})
    tableset = Tableset([Table("log", df)])

    model = {
        "table": "log",
        "column": "text",
        "regex": r"(\d+)",
        "group": "1",
        "target_column": "number",
        "on_missing": "null",
    }

    op = ExtractRegexGroup(model)
    result = op.operate(tableset)

    expected = df.copy()
    expected["number"] = ["123", None]
    pdt.assert_frame_equal(result.get_df("log"), expected)


def test_extract_no_match_error_behavior():
    df = pd.DataFrame({"text": ["abc", "no digits"]})
    tableset = Tableset([Table("log", df)])

    model = {
        "table": "log",
        "column": "text",
        "regex": r"(\d+)",
        "group": "1",
        "target_column": "digits",
        "on_missing": "error",
    }

    op = ExtractRegexGroup(model)

    with pytest.raises(ValueError, match="No match for value: abc"):
        op.operate(tableset)


def test_extract_missing_group_error():
    df = pd.DataFrame({"data": ["val:123"]})
    tableset = Tableset([Table("input", df)])

    model = {
        "table": "input",
        "column": "data",
        "regex": r"val:(\d+)",
        "group": "missing",  # no such named group
        "target_column": "output",
        "on_missing": "error",
    }

    op = ExtractRegexGroup(model)

    with pytest.raises(ValueError, match="Group 'missing' not found in regex match"):
        op.operate(tableset)
