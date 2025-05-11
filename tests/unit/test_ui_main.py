from unittest import mock

import pytest
from streamlit.testing.v1 import AppTest

from datarush.core.dataflow import Dataflow


@mock.patch("datarush.config.AppConfig.validate")
def test_main_full(mock_validate):
    at = AppTest.from_file("../../scripts/run_ui.py").run()
    at.session_state.dataflow == Dataflow()
