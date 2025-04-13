from pydantic import Field

from datarush.core.dataflow import Operation, Tableset
from datarush.core.types import BaseOperationModel, ColumnStr, TableStr


class SelectColumnModel(BaseOperationModel):
    table: TableStr = Field(title="Table", description="Table to select columns from")
    columns: list[ColumnStr] = Field(title="Columns", description="Column to keep")


class SelectColumns(Operation):
    name = "select_columns"
    title = "Select Columns"
    description = "Select columns to keep from table"
    model: SelectColumnModel

    def summary(self) -> str:
        return f"Select columns {', '.join(self.model.columns)} from `{self.model.table}`"

    def operate(self, tableset: Tableset) -> Tableset:
        df = tableset.get_df(self.model.table)
        df = df[self.model.columns]
        tableset.set_df(self.model.table, df)
        return tableset
