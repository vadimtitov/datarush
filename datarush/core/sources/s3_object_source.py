from __future__ import annotations

from io import BytesIO

import boto3
import pandas as pd
from streamlit.delta_generator import DeltaGenerator

from datarush.config import S3Config
from datarush.core.dataflow import Source, Table
from datarush.core.sources.local_file_source import read_file
from datarush.core.types import ContentType


class S3ObjectSource(Source):
    name = "s3_object"
    title = "S3 Object"
    description = "S3 Object Source"

    def __init__(
        self, bucket: str, object_key: str, content_type: ContentType, table_name: str
    ) -> None:
        self.bucket = bucket
        self.object_key = object_key
        self.content_type = content_type
        self.table_name = table_name

    def read(self) -> Table:
        config = S3Config.fromenv()
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key.reveal(),
        )
        response = s3_client.get_object(Bucket=self.bucket, Key=self.object_key)
        file = BytesIO(response["Body"].read())
        return Table(self.table_name, df=read_file(file, self.content_type))

    def update_from_streamlit(self, st: DeltaGenerator, key: str | int | None = None) -> bool:
        config = S3Config.fromenv()

        bucket = st.text_input(
            "Bucket", value=self.bucket or config.bucket or "", key=f"s3_bucket_{key}"
        )
        object_key = st.text_input(
            "Object key", value=self.object_key or "", key=f"s3_object_key_{key}"
        )
        content_type = ContentType(
            st.selectbox("Content Type", options=list(ContentType), key=f"s3_content_type_{key}"),
        )
        table_name = st.text_input(
            "Table Name", value=self.table_name or "s3_table", key=f"table_name_s3_{key}"
        )

        if not bucket or not object_key or not content_type or not table_name:
            return False

        fields = (bucket, object_key, content_type, table_name)

        if fields != (
            self.bucket,
            self.object_key,
            self.content_type,
            self.table_name,
        ):
            self.bucket, self.object_key, self.content_type, self.table_name = fields
            return True

        return False

    @classmethod
    def from_streamlit(
        cls, st: DeltaGenerator, key: str | int | None = None
    ) -> S3ObjectSource | None:
        src = cls(bucket="", object_key="", content_type="", table_name="")
        if src.update_from_streamlit(st, key):
            return src
        return None
