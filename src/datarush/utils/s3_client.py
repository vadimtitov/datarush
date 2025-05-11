"""S3 client wrapper for basic file and folder operations."""

from io import BytesIO

import boto3
from botocore.client import Config

from datarush.config import S3Config


class S3Client:
    """S3 Client."""

    def __init__(self, config: S3Config | None = None) -> None:
        """Initialize the S3 client with configuration."""
        config = config or S3Config.fromenv()
        self._client = boto3.client(
            "s3",
            endpoint_url=config.endpoint,
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key.reveal(),
            config=Config(signature_version="s3v4"),
        )

    def get_object(self, bucket: str, key: str) -> BytesIO:
        """Retrieve an object from S3 as BytesIO."""
        obj = self._client.get_object(Bucket=bucket, Key=key)
        return BytesIO(obj["Body"].read())

    def put_object(self, bucket: str, key: str, body: BytesIO) -> None:
        """Upload an object to S3."""
        self._client.put_object(Bucket=bucket, Key=key, Body=body)

    def delete_object(self, bucket: str, key: str) -> None:
        """Delete an object from S3."""
        self._client.delete_object(Bucket=bucket, Key=key)

    def list_object_keys(self, bucket: str, prefix: str) -> list[str]:
        """List object keys under a prefix in an S3 bucket."""
        prefix = prefix.strip("/")
        response = self._client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        objects = response.get("Contents", [])
        return [obj["Key"] for obj in objects]

    def list_folders(self, bucket: str, prefix: str) -> list[str]:
        """List folder names under a prefix in an S3 bucket."""
        prefix = prefix.strip("/") + "/"
        keys = self.list_object_keys(bucket, prefix)
        if prefix == "/":
            return list({key.split("/")[0] for key in keys})
        return list({key.split(prefix)[1].split("/")[0] for key in keys})
