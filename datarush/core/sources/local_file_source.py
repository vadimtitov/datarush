from __future__ import annotations

from functools import cache
from io import BytesIO

import pandas as pd
from pydantic import BaseModel, Field

from datarush.core.dataflow import Source, Table
from datarush.core.types import ContentType


class LocalFileModel(BaseModel):
    """HTTP Source model"""

    content_type: ContentType = Field(title="Content Type")
    file: bytes = Field(title="File")
    table_name: str = Field(title="Table Name", default="local_table")


class LocalFileSource(Source):
    name = "local_file"
    title = "Local File"
    description = "Local File Source"
    model: LocalFileModel

    @cache
    def read(self) -> Table:
        return Table(
            name=self.model.table_name,
            df=read_file(BytesIO(self.model.file), self.model.content_type),
        )


def read_file(file: BytesIO, content_type: ContentType) -> pd.DataFrame:
    if content_type == ContentType.CSV:
        return pd.read_csv(file)
    elif content_type == ContentType.JSON:
        return pd.read_json(file)
    elif content_type == ContentType.PARQUET:
        return pd.read_parquet(file)
    else:
        raise ValueError(f"Unsupported content type: {content_type}")
