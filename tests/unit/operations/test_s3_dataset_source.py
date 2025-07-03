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


def test_partition_filter_passed_as_dict(monkeypatch, sample_df):
    # GIVEN: a mocked read method that asserts filter logic
    def mock_read(self, partition_filter=None):
        assert partition_filter is not None

        # Filter should pass for matching value
        assert partition_filter({"region": "us-east-1"}) is True

        # Filter should fail for non-matching value
        assert partition_filter({"region": "eu-west-1"}) is False

        return sample_df

    monkeypatch.setattr("datarush.utils.s3_client.S3Dataset.read", mock_read)

    parameters = {
        "bucket": "test-bucket",
        "path": "datasets/example",
        "content_type": "CSV",
        "table_name": "s3_table",
        "error_on_empty": True,
        "partition_filter": {
            "combine": "and",
            "filters": [
                {
                    "column": "region",
                    "operator": "equals",
                    "value": "us-east-1",
                    "negate": False,
                }
            ],
        },
    }

    tableset = Tableset([])
    op = S3DatasetSource(parameters)

    # WHEN
    op.operate(tableset)

    # THEN
    result_df = tableset.get_df("s3_table")
    pd.testing.assert_frame_equal(result_df, sample_df)
