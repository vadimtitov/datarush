import pandas as pd
import pandas.testing as pdt
import pytest

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.parse_json_column import ParseJSONColumn


def test_parse_json_column_success():
    df = pd.DataFrame({"payload": ['{"x": 1}', '{"y": 2}', '{"z": [1,2,3]}']})
    tableset = Tableset([Table("jsons", df)])

    model = {"table": "jsons", "column": "payload"}

    op = ParseJSONColumn(model)
    result = op.operate(tableset)

    expected = pd.DataFrame({"payload": [{"x": 1}, {"y": 2}, {"z": [1, 2, 3]}]})

    pdt.assert_frame_equal(result.get_df("jsons"), expected)


def test_parse_json_column_with_null_on_error():
    df = pd.DataFrame({"payload": ['{"x": 1}', "invalid", '{"y": 3}']})
    tableset = Tableset([Table("data", df)])

    model = {
        "table": "data",
        "column": "payload",
        "on_error": "null",
    }

    op = ParseJSONColumn(model)
    result = op.operate(tableset)

    expected = pd.DataFrame({"payload": [{"x": 1}, None, {"y": 3}]})

    pdt.assert_frame_equal(result.get_df("data"), expected)


def test_parse_json_column_raises_on_error():
    df = pd.DataFrame({"payload": ['{"valid": 1}', "not a json"]})
    tableset = Tableset([Table("example", df)])

    model = {
        "table": "example",
        "column": "payload",
        "on_error": "error",
    }

    op = ParseJSONColumn(model)

    with pytest.raises(ValueError, match="Failed to parse JSON value: not a json"):
        op.operate(tableset)
