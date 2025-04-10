from enum import StrEnum

from pydantic import BaseModel


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


class SpecialType(StrEnum):
    """Special type markers for the UI."""

    TABLE = "TABLES"
    TABLE_LIST = "TABLE_LIST"
    SELECTED_TABLE_COLUMN = "SELECTED_TABLE_COLUMN"
    SELECTED_TABLE_COLUMN_LIST = "SELECTED_TABLE_COLUMN_LIST"
    ALL_TABLES_COLUMN = "ALL_TABLES_COLUMN"
    ALL_TABLES_COLUMN_LIST = "ALL_TABLES_COLUMN_LIST"
