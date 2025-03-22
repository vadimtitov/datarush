from typing import Type

from datarush.core.dataflow import Operation
from datarush.core.operations.dropna import DropNaValues
from datarush.core.operations.filter_row import FilterByColumn
from datarush.core.operations.sort import SortByColumn

OPERATION_TYPES: list[Type[Operation]] = [
    DropNaValues,
    SortByColumn,
    FilterByColumn,
]


def get_operation_type_by_title(title: str) -> Type[Operation]:
    return _TITLE_TO_OPERATION_TYPE[title]


_TITLE_TO_OPERATION_TYPE = {op.title: op for op in OPERATION_TYPES}
