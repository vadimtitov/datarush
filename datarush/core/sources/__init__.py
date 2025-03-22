from typing import Type

from datarush.core.dataflow import Source
from datarush.core.sources.http_source import HttpSource
from datarush.core.sources.local_file_source import LocalFileSource
from datarush.core.sources.s3_object_source import S3ObjectSource

SOURCE_TYPES: list[Source] = [
    LocalFileSource,
    S3ObjectSource,
    HttpSource,
]


def get_source_type_by_title(title: str) -> Type[Source]:
    return _TITLE_TO_SOURCE_TYPE[title]


_TITLE_TO_SOURCE_TYPE = {source.title: source for source in SOURCE_TYPES}
