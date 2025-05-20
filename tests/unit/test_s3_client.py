from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from datarush.core.types import ContentType
from datarush.utils.s3_client import DatasetWriteMode, S3Client, S3Dataset


@pytest.fixture
def mock_boto3_client():
    with patch("datarush.utils.s3_client.boto3.client") as mock_client:
        yield mock_client


@pytest.fixture
def s3_client(mock_boto3_client):
    return S3Client()


def test_get_object(s3_client, mock_boto3_client):
    # Mock the get_object method
    mock_client_instance = mock_boto3_client.return_value
    mock_client_instance.get_object = MagicMock(return_value={"Body": BytesIO(b"test data")})

    # Call the method
    result = s3_client.get_object("test-bucket", "test-key")

    # Assertions
    assert result.read() == b"test data"
    mock_client_instance.get_object.assert_called_once_with(Bucket="test-bucket", Key="test-key")


def test_put_object(s3_client, mock_boto3_client):
    # Mock the put_object method
    mock_client_instance = mock_boto3_client.return_value
    mock_client_instance.put_object = MagicMock()

    # Call the method
    body = BytesIO(b"test data")
    s3_client.put_object("test-bucket", "test-key", body)

    # Assertions
    mock_client_instance.put_object.assert_called_once_with(
        Bucket="test-bucket", Key="test-key", Body=body
    )


def test_delete_object(s3_client, mock_boto3_client):
    # Mock the delete_object method
    mock_client_instance = mock_boto3_client.return_value
    mock_client_instance.delete_object = MagicMock()

    # Call the method
    s3_client.delete_object("test-bucket", "test-key")

    # Assertions
    mock_client_instance.delete_object.assert_called_once_with(
        Bucket="test-bucket", Key="test-key"
    )


def test_list_object_keys(s3_client, mock_boto3_client):
    # Mock the list_objects_v2 method
    mock_client_instance = mock_boto3_client.return_value
    mock_client_instance.list_objects_v2 = MagicMock(
        return_value={"Contents": [{"Key": "folder/file1.txt"}, {"Key": "folder/file2.txt"}]}
    )

    # Call the method
    result = s3_client.list_object_keys("test-bucket", "folder")

    # Assertions
    assert result == ["folder/file1.txt", "folder/file2.txt"]
    mock_client_instance.list_objects_v2.assert_called_once_with(
        Bucket="test-bucket", Prefix="folder"
    )


def test_list_object_keys_empty(s3_client, mock_boto3_client):
    # Mock the list_objects_v2 method with no contents
    mock_client_instance = mock_boto3_client.return_value
    mock_client_instance.list_objects_v2 = MagicMock(return_value={})

    # Call the method
    result = s3_client.list_object_keys("test-bucket", "folder")

    # Assertions
    assert result == []
    mock_client_instance.list_objects_v2.assert_called_once_with(
        Bucket="test-bucket", Prefix="folder"
    )


def test_list_folders(s3_client, mock_boto3_client):
    # Mock the list_objects_v2 method
    mock_client_instance = mock_boto3_client.return_value
    mock_client_instance.list_objects_v2 = MagicMock(
        return_value={
            "Contents": [
                {"Key": "folder1/file1.txt"},
                {"Key": "folder1/file2.txt"},
                {"Key": "folder2/file3.txt"},
            ]
        }
    )

    # Call the method
    result = s3_client.list_folders("test-bucket", "")

    # Assertions
    assert sorted(result) == ["folder1", "folder2"]
    mock_client_instance.list_objects_v2.assert_called_once_with(Bucket="test-bucket", Prefix="")


def test_list_folders_with_prefix(s3_client, mock_boto3_client):
    # Mock the list_objects_v2 method
    mock_client_instance = mock_boto3_client.return_value
    mock_client_instance.list_objects_v2 = MagicMock(
        return_value={
            "Contents": [
                {"Key": "prefix/folder1/file1.txt"},
                {"Key": "prefix/folder1/file2.txt"},
                {"Key": "prefix/folder2/file3.txt"},
            ]
        }
    )

    # Call the method
    result = s3_client.list_folders("test-bucket", "prefix")

    # Assertions
    assert sorted(result) == ["folder1", "folder2"]
    mock_client_instance.list_objects_v2.assert_called_once_with(
        Bucket="test-bucket", Prefix="prefix"
    )


# DATASET


@pytest.fixture
def mock_awswrangler():
    with patch("datarush.utils.s3_client.wr") as mock_wr:
        yield mock_wr


@pytest.fixture
def mock_boto3_session():
    with patch("datarush.utils.s3_client.boto3.Session") as mock_session:
        mock_session_instance = MagicMock()
        mock_session.return_value = mock_session_instance
        yield mock_session_instance


@pytest.fixture
def s3_dataset(mock_awswrangler, mock_boto3_session):
    return S3Dataset(
        bucket="test-bucket",
        prefix="test-prefix",
        content_type=ContentType.CSV,
        partition_columns=["col1"],
        unique_ids=["id"],
        write_mode=DatasetWriteMode.APPEND,
    )


def test_read_csv(mock_awswrangler, mock_boto3_session, s3_dataset):
    # Mock the read_csv method
    mock_awswrangler.s3.read_csv.return_value = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

    # Call the method
    result = s3_dataset.read()

    # Assertions
    mock_awswrangler.s3.read_csv.assert_called_once_with(
        boto3_session=mock_boto3_session,
        path="s3://test-bucket/test-prefix",
        dataset=True,
    )
    assert isinstance(result, pd.DataFrame)
    assert result.equals(pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}))


def test_write_csv(mock_awswrangler, mock_boto3_session, s3_dataset):
    # Mock the DataFrame
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4], "id": [10, 20]})
    # Mock the read method to return an existing DataFrame
    mock_awswrangler.s3.read_csv.return_value = pd.DataFrame(
        {"col1": [8], "id": [888], "col2": [8]}
    )

    # Call the method
    s3_dataset.write(df)

    # Assertions
    mock_awswrangler.s3.to_csv.assert_called_once()
    _, called_kwargs = mock_awswrangler.s3.to_csv.call_args

    assert_frame_equal(called_kwargs.pop("df"), df)
    assert called_kwargs == dict(
        path="s3://test-bucket/test-prefix",
        dataset=True,
        boto3_session=mock_boto3_session,
        partition_cols=["col1"],
        mode=DatasetWriteMode.APPEND.value,
        index=False,
    )


def test_write_empty_dataframe(mock_awswrangler, s3_dataset):
    # Mock an empty DataFrame
    df = pd.DataFrame()

    # Call the method
    s3_dataset.write(df)

    # Assertions
    mock_awswrangler.s3.to_csv.assert_not_called()  # No write should occur


def test_write_unique(mock_awswrangler, mock_boto3_session, s3_dataset):
    # Mock the DataFrame
    df = pd.DataFrame({"col1": [1, 2], "id": [10, 20], "col2": [3, 4]})
    to_append_expected = pd.DataFrame({"col1": [2], "id": [20], "col2": [4]})

    # Mock the read method to return an existing DataFrame
    mock_awswrangler.s3.read_csv.return_value = pd.DataFrame(
        {"col1": [1], "id": [10], "col2": [3]}
    )

    # Call the method
    s3_dataset.write(df)

    # Assertions
    mock_awswrangler.s3.to_csv.assert_called_once()
    _, called_kwargs = mock_awswrangler.s3.to_csv.call_args

    to_append_actual = called_kwargs.pop("df").reset_index(drop=True)
    assert_frame_equal(to_append_actual, to_append_expected)
    assert called_kwargs == dict(
        path="s3://test-bucket/test-prefix",
        dataset=True,
        boto3_session=mock_boto3_session,
        partition_cols=["col1"],
        mode=DatasetWriteMode.APPEND.value,
        index=False,
    )


def test_write_unique_missing_columns(mock_awswrangler, s3_dataset):
    # Mock the DataFrame with missing unique_ids
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

    # Call the method and expect an exception
    with pytest.raises(ValueError, match="Columns \\['id'\\] not found in DataFrame columns."):
        s3_dataset.write(df)
