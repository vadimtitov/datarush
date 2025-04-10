from enum import StrEnum
from typing import Any

from pydantic import BaseModel, GetCoreSchemaHandler
from pydantic_core import core_schema


class ContentType(StrEnum):

    CSV = "CSV"
    JSON = "JSON"
    PARQUET = "PARQUET"

    def extension(self):
        return {
            ContentType.CSV: [".csv"],
            ContentType.JSON: [".json"],
            ContentType.PARQUET: [".parquet"],
        }[self]


class ParameterType(StrEnum):
    """Input parameters types."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    DATE = "date"
    DATETIME = "datetime"
    BOOLEAN = "boolean"


class ParameterSpec(BaseModel):
    """Parameter specification."""

    name: str
    type: ParameterType
    description: str
    default: str
    required: bool


class TableStr(str):
    """Special string type to mark field that take table name as an input."""

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler):
        return core_schema.no_info_after_validator_function(
            lambda v: TableStr(v), core_schema.str_schema()
        )


class ColumnStr(str):
    """Special string type to mark field that take colum name as an input."""

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler):
        return core_schema.no_info_after_validator_function(
            lambda v: ColumnStr(v), core_schema.str_schema()
        )


class BaseOperationModel(BaseModel):
    """Base model for operations."""
