from typing import Type

from datarush.core.dataflow import Operation
from datarush.core.operations.sinks import s3_sink
from datarush.core.operations.sources import http_source, local_file_source, s3_object_source
from datarush.core.operations.transformations import dropna, filter_row, select_columns, sort

DATA_SOURCE_OPERATION_TYPES: list[Type[Operation]] = [
    http_source.HttpSource,
    local_file_source.LocalFileSource,
    s3_object_source.S3ObjectSource,
]

TRANSFORM_OPERATION_TYPES: list[Type[Operation]] = [
    dropna.DropNaValues,
    sort.SortByColumn,
    filter_row.FilterByColumn,
    select_columns.SelectColumns,
]

DATA_SINK_OPERATION_TYPES: list[Type[Operation]] = [
    s3_sink.S3ObjectSink,
]

OPERATION_TYPES = (
    DATA_SOURCE_OPERATION_TYPES + TRANSFORM_OPERATION_TYPES + DATA_SINK_OPERATION_TYPES
)


def get_operation_type_by_title(title: str) -> Type[Operation]:
    return _TITLE_TO_OPERATION_TYPE[title]


def get_operation_type_by_name(name: str) -> Type[Operation]:
    return _NAME_TO_OPERATION_TYPE[name]


_TITLE_TO_OPERATION_TYPE = {op.title: op for op in OPERATION_TYPES}
_NAME_TO_OPERATION_TYPE = {op.name: op for op in OPERATION_TYPES}
