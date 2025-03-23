from __future__ import annotations

from pydantic import BaseModel, Field

from datarush.core.dataflow import Operation, Tableset
from datarush.core.types import ContentType
from datarush.utils.misc import read_file, to_file
from datarush.utils.s3_client import S3Client


class S3SinkModel(BaseModel):
    """S3 Sink model"""

    bucket: str = Field(title="Bucket")
    object_key: str = Field(title="Object Key")
    content_type: ContentType = Field(title="Content Type")
    table: str = Field(title="Table to write")


class S3ObjectSink(Operation):
    name = "s3_object_sink"
    title = "S3 Object Sink"
    description = "S3 Object Sink"
    model: S3SinkModel

    def summary(self):
        return f"Write `{self.model.table}` to S3 {self.model.bucket}/{self.model.object_key} as {self.model.content_type.value}"

    def operate(self, tableset: Tableset) -> Tableset:
        df = tableset.get_df(self.model.table)
        file = to_file(df, self.model.content_type)
        S3Client().put_object(self.model.bucket, self.model.object_key, file)
        return tableset
