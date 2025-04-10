from pydantic import Field

from datarush.core.dataflow import Operation, Tableset
from datarush.core.types import BaseOperationModel, ColumnStr, TableStr


class SortColumnModel(BaseOperationModel):

    table: TableStr = Field(title="Table", description="Table to sort")
    column: ColumnStr = Field(title="Column", description="Column to sort by", jinja=True)
    ascending: bool = Field(
        title="Ascending",
        description="Sort in ascending order",
        default=True,
    )


class SortByColumn(Operation):
    name = "sort"
    title = "Sort"
    description = "Sort table by column"

    model: SortColumnModel

    def summary(self) -> str:
        return f"Sort `{self.model.table}` by {self.model.column} in {'ascending' if self.model.ascending else 'descending'} order"

    def operate(self, tableset: Tableset) -> Tableset:
        table, column = self.model.table, self.model.column
        df = tableset.get_df(table)
        tableset.set_df(table, df.sort_values(by=column, ascending=self.model.ascending))
        return tableset
