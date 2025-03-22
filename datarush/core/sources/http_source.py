from io import BytesIO

import requests
from pydantic import BaseModel, Field

from datarush.core.dataflow import Source, Table
from datarush.core.sources.local_file_source import read_file
from datarush.core.types import ContentType


class HttpSourceModel(BaseModel):
    """HTTP Source model"""

    url: str = Field(title="URL", default="https://api.github.com/users/mralexgray/repos")
    content_type: ContentType = Field(title="Content Type")
    table_name: str = Field(title="table_name", default="http_table")


class HttpSource(Source):
    name = "http_request"
    title = "HTTP Request"
    description = "Send an HTTP request to get a file"
    model: HttpSourceModel

    def read(self) -> Table:
        response = requests.get(self.model.url)
        response.raise_for_status()
        file = BytesIO(response.content)
        return Table(name=self.model.table_name, df=read_file(file, self.model.content_type))
