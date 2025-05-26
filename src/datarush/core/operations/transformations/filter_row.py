"""Filer row operation."""

from pydantic import Field

from datarush.core.dataflow import Operation, Tableset
from datarush.core.types import BaseOperationModel, TableStr
from datarush.utils.conditions import RowCondition, match_conditions


class FilterRowModel(BaseOperationModel):
    """Filter row operation model."""

    table: TableStr = Field(title="Table", description="Table to filter")
    condition: RowCondition = Field(
        title="Condition",
        description="Condition to filter rows by",
    )


class FilterByColumn(Operation):
    """Filter row by column value operation."""

    name = "filter_rows"
    title = "Filter Rows"
    description = "Filter table rows by column value"
    model: FilterRowModel

    def summary(self) -> str:
        """Provide operation summary."""
        return f"Filter `{self.model.table}` where {self.model.condition.summary()}"

    def operate(self, tableset: Tableset) -> Tableset:
        """Run operation."""
        table = self.model.table
        df = tableset.get_df(table)

        mask = match_conditions(df, [self.model.condition])

        tableset.set_df(table, df[mask])
        return tableset
