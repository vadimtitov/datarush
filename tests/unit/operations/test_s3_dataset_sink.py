from unittest.mock import patch

import pandas as pd
import pytest

from datarush.core.dataflow import Tableset
from datarush.core.operations.sinks.s3_dataset_sink import S3DatasetSink


def test_operate_success(mock_s3_dataset, sample_df):
    # GIVEN
    parameters = {
        "bucket": "test-bucket",
        "path": "datasets/wrangler/my-dataset-2",
        "content_type": "CSV",
        "mode": "overwrite",
        "table": "table",
        "partition_columns": ["datetime_column"],
        "unique_ids": [],
    }
    tableset = Tableset([])
    tableset.set_df("table", sample_df)
    _, mock_write = mock_s3_dataset

    operation = S3DatasetSink(parameters)

    # WHEN
    operation.operate(tableset)

    # THEN
    args, _ = mock_write.call_args
    pd.testing.assert_frame_equal(args[0], sample_df)
