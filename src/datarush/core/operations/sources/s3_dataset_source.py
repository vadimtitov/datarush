"""S3 dataset source operation."""

from __future__ import annotations

import awswrangler as wr
import boto3
from pydantic import BaseModel, Field

from datarush.config import S3Config
from datarush.core.dataflow import Operation, Tableset
from datarush.core.types import ContentType


class S3DatasetSourceModel(BaseModel):
    """S3 dataset Source model."""

    bucket: str = Field(title="Bucket")
    path: str = Field(title="Dataset Path", default="datasets/wrangler/my-dataset-2")
    content_type: ContentType = Field(title="Content Type")
    table_name: str = Field(title="Table Name", default="s3_table")


class S3DatasetSource(Operation):
    """S3 dataset source operation."""

    name = "s3_dataset_source"
    title = "S3 Dataset Source"
    description = "Download dataset from S3"
    model: S3DatasetSourceModel

    def initialize(self):
        config = S3Config.fromenv()
        self._session = boto3.Session(
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key.reveal(),
        )
        wr.config.s3_endpoint_url = config.endpoint

    def summary(self) -> str:
        """Provide operation summary."""
        return f"Load S3 dataset as `{self.model.table_name}` table"

    def operate(self, tableset: Tableset) -> Tableset:
        """Run operation."""
        path = f"s3://{self.model.bucket}/{self.model.path.strip('/')}"

        if self.model.content_type == ContentType.JSON:
            df = wr.s3.read_json(
                boto3_session=self._session,
                path=path,
                dataset=True,
                # orient="columns"
            )
        elif self.model.content_type == ContentType.CSV:
            df = wr.s3.read_csv(
                boto3_session=self._session,
                path=path,
                dataset=True,
            )
        elif self.model.content_type == ContentType.PARQUET:
            df = wr.s3.read_parquet(
                boto3_session=self._session,
                path=path,
                dataset=True,
            )
        else:
            raise ValueError(f"Unsupported content type: {self.model.content_type}")

        tableset.set_df(self.model.table_name, df)
        return tableset
