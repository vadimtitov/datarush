from pydantic import BaseModel, Field

from datarush.core.dataflow import Operation, Tableset


class FilterRowModel(BaseModel):
    table: str = Field(title="Table", description="Table to filter")
    column: str = Field(title="Column", description="Column to filter by")
    value: str = Field(title="Value", description="Value to filter for")


class FilterByColumn(Operation):
    name = "filter"
    title = "Filter"
    description = "Filter table rows by column value"

    model: FilterRowModel

    def summary(self) -> str:
        return f"Filter `{self.model.table}` where {self.model.column} is {self.model.value}"

    def operate(self, tableset: Tableset) -> Tableset:
        table, column, value = self.model.table, self.model.column, self.model.value
        df = tableset.get_df(table)
        filtered_df = df[df[column] == value]
        tableset.set_df(table, filtered_df)
        return tableset
