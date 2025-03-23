from pydantic import BaseModel, Field

from datarush.core.dataflow import Operation, Tableset


class SelectColumnModel(BaseModel):
    table: str = Field(title="Table", description="Table to select columns from")
    columns: list[str] = Field(title="Columns", description="Column to keep")


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
