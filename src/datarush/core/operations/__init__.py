"""Operations API."""

from typing import Type

from datarush.core.dataflow import Operation
from datarush.core.operations.sinks import s3_dataset_sink, s3_sink
from datarush.core.operations.sources import (
    http_source,
    local_file_source,
    s3_dataset_source,
    s3_object_source,
)
from datarush.core.operations.transformations import (
    astype,
    calculate,
    calculate_hash,
    columns_to_dict,
    copy_column,
    copy_table,
    derive_column,
    dict_to_columns,
    dropna,
    extract_regex_group,
    filter_row,
    group_by,
    join,
    melt_table,
    parse_json_column,
    pivot_table,
    replace,
    select_columns,
    set_header,
    sort,
    transpose,
    unset_header,
)

_TITLE_TO_OPERATION_TYPE: dict[str, Type[Operation]] = {}
_NAME_TO_OPERATION_TYPE: dict[str, Type[Operation]] = {}


def register_operation_type(operation: Type[Operation]) -> None:
    """Register a new operation type."""
    _TITLE_TO_OPERATION_TYPE[operation.title] = operation  # type: ignore
    _NAME_TO_OPERATION_TYPE[operation.name] = operation  # type: ignore


def list_operation_types() -> list[Type[Operation]]:
    """List all available operation type."""
    return list(_NAME_TO_OPERATION_TYPE.values())


def get_operation_type_by_title(title: str) -> Type[Operation]:
    """Get operation type by operation title."""
    return _TITLE_TO_OPERATION_TYPE[title]


def get_operation_type_by_name(name: str) -> Type[Operation]:
    """Get operation type by operation name."""
    return _NAME_TO_OPERATION_TYPE[name]


# Register build-in operations
for _op_type in [
    # Source
    http_source.HttpSource,
    local_file_source.LocalFileSource,
    s3_object_source.S3ObjectSource,
    s3_dataset_source.S3DatasetSource,
    # Transform
    astype.AsType,
    dropna.DropNaValues,
    sort.SortByColumn,
    filter_row.FilterByColumn,
    select_columns.SelectColumns,
    join.JoinTables,
    group_by.GroupBy,
    pivot_table.PivotTable,
    melt_table.Melt,
    set_header.SetHeader,
    unset_header.UnsetHeader,
    calculate.Calculate,
    copy_column.CopyColumn,
    copy_table.CopyTable,
    transpose.Transpose,
    derive_column.DeriveColumn,
    calculate_hash.CalculateHash,
    extract_regex_group.ExtractRegexGroup,
    parse_json_column.ParseJSONColumn,
    columns_to_dict.ColumnsToDict,
    dict_to_columns.DictToColumns,
    replace.Replace,
    # Sink
    s3_sink.S3ObjectSink,
    s3_dataset_sink.S3DatasetSink,
]:
    register_operation_type(_op_type)  # type: ignore
