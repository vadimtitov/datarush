from io import BytesIO
from typing import Any

import boto3
from botocore.client import Config

from datarush.config import S3Config


class S3Client:

    def __init__(self, config: S3Config | None = None) -> None:
        config = config or S3Config.fromenv()
        self._client = boto3.client(
            "s3",
            endpoint_url=config.endpoint,
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key.reveal(),
            config=Config(signature_version="s3v4"),
        )

    def get_object(self, bucket: str, key: str) -> BytesIO:
        obj = self._client.get_object(Bucket=bucket, Key=key)
        return obj["Body"]

    def put_object(self, bucket: str, key: str, body: BytesIO) -> None:
        self._client.put_object(Bucket=bucket, Key=key, Body=body)

    def delete_object(self, bucket: str, key: str) -> None:
        self._client.delete_object(Bucket=bucket, Key=key)

    def list_object_keys(self, bucket: str, prefix: str) -> list[dict[str, Any]]:
        prefix = prefix.strip("/")
        response = self._client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        objects = response.get("Contents", [])
        return [obj["Key"] for obj in objects]

    def list_folders(self, bucket: str, prefix: str) -> list[str]:
        prefix = prefix.strip("/") + "/"
        keys = self.list_object_keys(bucket, prefix)
        if prefix == "/":
            return list({key.split("/")[0] for key in keys})
        return list({key.split(prefix)[1].split("/")[0] for key in keys})
