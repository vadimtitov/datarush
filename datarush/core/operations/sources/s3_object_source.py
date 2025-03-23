from __future__ import annotations

import boto3
from botocore.client import Config
from pydantic import BaseModel, Field

from datarush.config import S3Config
from datarush.core.dataflow import Operation, Tableset
from datarush.core.types import ContentType
from datarush.utils.misc import read_file


class S3SourceModel(BaseModel):
    """S3 Source model"""

    bucket: str = Field(title="Bucket")
    object_key: str = Field(title="Object Key")
    content_type: ContentType = Field(title="Content Type")
    table_name: str = Field(title="Table Name", default="s3_table")


class S3ObjectSource(Operation):
    name = "s3_object"
    title = "S3 Object"
    description = "S3 Object Source"
    model: S3SourceModel

    def summary(self):
        return f"Load S3 object as `{self.model.table_name}` table"

    def operate(self, tableset: Tableset) -> Tableset:
        config = S3Config.fromenv()

        s3_client = boto3.client(
            "s3",
            endpoint_url=config.endpoint,
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key.reveal(),
            config=Config(signature_version="s3v4"),
            # region_name="us-east-1",
        )

        obj = s3_client.get_object(Bucket=self.model.bucket, Key=self.model.object_key)
        df = read_file(obj["Body"], self.model.content_type)
        tableset.set_df(self.model.table_name, df)
        return tableset
