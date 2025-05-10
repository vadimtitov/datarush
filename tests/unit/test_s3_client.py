from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from datarush.utils.s3_client import S3Client


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
