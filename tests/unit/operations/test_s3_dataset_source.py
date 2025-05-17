from unittest.mock import patch

import pandas as pd
import pytest

from datarush.core.dataflow import Tableset
from datarush.core.operations.sources.s3_dataset_source import S3DatasetSource
from datarush.utils.s3_client import DatasetDoesNotExistError


def test_operate_success(mock_s3_dataset, sample_df):
    # GIVEN
    parameters = {
        "bucket": "test-bucket",
        "path": "datasets/wrangler/my-dataset-2",
        "content_type": "CSV",
        "table_name": "s3_table",
        "error_on_empty": True,
    }
    tableset = Tableset([])
    operation = S3DatasetSource(parameters)

    # WHEN
    operation.operate(tableset)

    # THEN
    tableset_df = tableset.get_df("s3_table")
    pd.testing.assert_frame_equal(tableset_df, sample_df)


def test_operate_empty_error(mock_s3_dataset_empty):
    # GIVEN
    parameters = {
        "bucket": "test-bucket",
        "path": "datasets/wrangler/my-dataset-2",
        "content_type": "CSV",
        "table_name": "s3_table",
        "error_on_empty": True,
    }
    tableset = Tableset([])
    operation = S3DatasetSource(parameters)

    # WHEN
    with pytest.raises(DatasetDoesNotExistError):
        operation.operate(tableset)


def test_operate_empty_success(mock_s3_dataset_empty):
    # GIVEN
    parameters = {
        "bucket": "test-bucket",
        "path": "datasets/wrangler/my-dataset-2",
        "content_type": "CSV",
        "table_name": "s3_table",
        "error_on_empty": False,
    }
    tableset = Tableset([])
    operation = S3DatasetSource(parameters)

    # WHEN
    operation.operate(tableset)

    # THEN
    tableset_df = tableset.get_df("s3_table")
    assert tableset_df.empty
