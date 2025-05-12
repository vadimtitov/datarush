"""S3 dataset sink operation."""

from __future__ import annotations

import awswrangler as wr
import boto3
from pydantic import Field

from datarush.config import S3Config
from datarush.core.dataflow import Operation, Tableset
from datarush.core.types import BaseOperationModel, ColumnStr, ContentType, TableStr


class S3DatasetSinkModel(BaseOperationModel):
    """Pydantic model for S3 dataset sink operation."""

    bucket: str = Field(title="Bucket")
    path: str = Field(title="Dataset Path")
    content_type: ContentType = Field(title="Content Type")
    table: TableStr = Field(title="Table to write")
    partition_columns: list[ColumnStr] = Field(
        title="Partition Columns",
        default=None,
    )


class S3DatasetSink(Operation):
    """Operation that writes a table to an S3 dataset."""

    name = "s3_dataset_sink"
    title = "S3 Dataset Sink"
    description = "Write table as S3 dataset"
    model: S3DatasetSinkModel

    def initialize(self):
        config = S3Config.fromenv()
        self._session = boto3.Session(
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key.reveal(),
        )
        wr.config.s3_endpoint_url = config.endpoint

    def summary(self) -> str:
        """Return a short summary of the operation."""
        path = f"{self.model.bucket}/{self.model.path.strip('/')}"
        return (
            f"Write `{self.model.table}` to S3 dataset {path}"
            f" as {self.model.content_type.value}"
        )

    def operate(self, tableset: Tableset) -> Tableset:
        """Write table to S3 dataset and return unmodified tableset."""
        path = f"s3://{self.model.bucket}/{self.model.path.strip('/')}"
        df = tableset.get_df(self.model.table)

        if self.model.content_type == ContentType.JSON:
            wr.s3.to_json(
                df=df,
                path=path,
                dataset=True,
                boto3_session=self._session,
                partition_cols=self.model.partition_columns,
                mode="overwrite",
                orient="records",  # important
                lines=True,  # important
                index=False,  # important
            )
        elif self.model.content_type == ContentType.CSV:
            wr.s3.to_csv(
                df=df,
                path=path,
                dataset=True,
                boto3_session=self._session,
                partition_cols=self.model.partition_columns,
                mode="overwrite",
                index=False,  # important
            )
        elif self.model.content_type == ContentType.PARQUET:
            wr.s3.to_parquet(
                df=df,
                path=path,
                dataset=True,
                boto3_session=self._session,
                partition_cols=self.model.partition_columns,
                mode="overwrite",
                index=False,  # important
            )
        else:
            raise ValueError(f"Unsupported content type: {self.model.content_type}")

        return tableset
