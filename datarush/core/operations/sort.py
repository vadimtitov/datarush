from pydantic import BaseModel, Field

from datarush.core.dataflow import Operation, Tableset


class SortColumnModel(BaseModel):

    table: str = Field(title="Table", description="Table to sort")
    column: str = Field(title="Column", description="Column to sort by")


class SortByColumn(Operation):
    name = "sort"
    title = "Sort"
    description = "Sort table by column"

    model: SortColumnModel

    def summary(self) -> str:
        return f"Sort {self.model.table} by {self.model.column}"

    def operate(self, tableset: Tableset) -> Tableset:
        table, column = self.model.table, self.model.column
        df = tableset.get_df(table)
        tableset.set_df(table, df.sort_values(by=column))
        return tableset
