from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from datarush.utils.s3_client import DatasetDoesNotExistError


@pytest.fixture
def sample_df():
    df = pd.DataFrame(
        {
            "int_column": [1, 2, 3, 4, 5],
            "float_column": [1.1, 2.2, 3.3, np.nan, 5.5],
            "str_column": ["a", "b", "c", "d", "e"],
            "datetime_column": pd.to_datetime(
                ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"]
            ),
            "category_column": pd.Series(
                ["low", "medium", "high", "medium", "low"], dtype="category"
            ),
            "bool_column": [True, False, True, False, True],
            "object_column": [{"x": 1}, [1, 2], "string", 3.14, None],
            "timedelta_column": pd.to_timedelta(
                ["1 days", "2 days", "3 days", "4 days", "5 days"]
            ),
            "nan_column": [np.nan, np.nan, np.nan, np.nan, np.nan],
        }
    )
    return df


@pytest.fixture
def mock_s3_dataset(sample_df):
    """Mock S3Dataset."""
    with patch("datarush.utils.s3_client.S3Dataset.read") as mock_read:
        mock_read.return_value = sample_df

        with patch("datarush.utils.s3_client.S3Dataset.write") as mock_write:
            mock_write.return_value = None

            yield mock_read, mock_write


@pytest.fixture
def mock_s3_dataset_empty():
    """Mock S3Dataset."""
    with patch("datarush.utils.s3_client.S3Dataset.read") as mock_read:
        mock_read.side_effect = DatasetDoesNotExistError("Dataset does not exist")

        with patch("datarush.utils.s3_client.S3Dataset.write") as mock_write:
            mock_write.return_value = None

            yield
