from enum import StrEnum


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


class SpecialType(StrEnum):
    """Special type markers for the UI."""

    TABLE = "TABLES"
    TABLE_LIST = "TABLE_LIST"
    SELECTED_TABLE_COLUMN = "SELECTED_TABLE_COLUMN"
    SELECTED_TABLE_COLUMN_LIST = "SELECTED_TABLE_COLUMN_LIST"
    ALL_TABLES_COLUMN = "ALL_TABLES_COLUMN"
    ALL_TABLES_COLUMN_LIST = "ALL_TABLES_COLUMN_LIST"
