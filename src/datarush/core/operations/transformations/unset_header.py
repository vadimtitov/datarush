"""Unset Header operation."""

import pandas as pd
from pydantic import Field

from datarush.core.dataflow import Operation, Tableset
from datarush.core.types import BaseOperationModel, TableStr


class UnsetHeaderModel(BaseOperationModel):
    """Unset header operation model."""

    table: TableStr = Field(title="Table", description="Table to unset header from")


class UnsetHeader(Operation):
    """Move current headers to the first row and reset headers to default integers."""

    name = "unset_header"
    title = "Unset Header"
    description = "Move column names to first row and replace headers with default integers"
    model: UnsetHeaderModel

    def summary(self) -> str:
        """Provide operation summary."""
        return f"Unset header of `{self.model.table}`"

    def operate(self, tableset: Tableset) -> Tableset:
        """Run operation."""
        df = tableset.get_df(self.model.table)

        # Step 1: capture current headers and create a new row with same column names
        header_row = pd.DataFrame([df.columns], columns=df.columns)

        # Step 2: concatenate with original df
        new_df = pd.concat([header_row, df], ignore_index=True)

        # Step 3: reset column names to default integers
        new_df.columns = list(range(new_df.shape[1]))

        # Set back into the tableset
        tableset.set_df(self.model.table, new_df)
        return tableset
