import pandas as pd
import pandas.testing as pdt
import pytest
import responses

from datarush.core.dataflow import Tableset
from datarush.core.operations.sources.send_http_request import SendHttpRequest


@responses.activate
def test_send_http_request_raw():
    responses.add(responses.GET, "https://example.com", body="Hello, world!", status=200)

    tableset = Tableset([])

    model = {
        "method": "GET",
        "url": "https://example.com",
        "headers": {},
        "params": {},
        "body": "",
        "output_table": "resp",
        "response_format": "raw",
    }

    op = SendHttpRequest(model)
    result = op.operate(tableset)

    expected = pd.DataFrame([{"body": "Hello, world!"}])
    pdt.assert_frame_equal(result.get_df("resp"), expected)


@responses.activate
def test_send_http_request_json():
    responses.add(responses.GET, "https://example.com", json={"foo": "bar"}, status=200)

    tableset = Tableset([])

    model = {
        "method": "GET",
        "url": "https://example.com",
        "headers": {},
        "params": {},
        "body": "",
        "output_table": "resp",
        "response_format": "json",
    }

    op = SendHttpRequest(model)
    result = op.operate(tableset)

    expected = pd.DataFrame([{"foo": "bar"}])
    pdt.assert_frame_equal(result.get_df("resp"), expected)


@responses.activate
def test_send_http_request_json_parse_error():
    responses.add(responses.GET, "https://example.com", body="not json", status=200)

    tableset = Tableset([])

    model = {
        "method": "GET",
        "url": "https://example.com",
        "headers": {},
        "params": {},
        "body": "",
        "output_table": "resp",
        "response_format": "json",
    }

    op = SendHttpRequest(model)

    with pytest.raises(ValueError, match="Failed to parse JSON"):
        op.operate(tableset)
