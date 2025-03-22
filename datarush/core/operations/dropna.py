from __future__ import annotations

from pydantic import BaseModel, Field

from datarush.core.dataflow import Operation, Tableset


class DropnaModel(BaseModel):
    table: str = Field(title="Table")


class DropNaValues(Operation):
    name = "dropna"
    title = "Drop NA"
    description = "Drop all rows with NA values"
    model: DropnaModel

    def summary(self) -> str:
        return f"Drop NA values in {self.model.table}"

    def operate(self, tableset: Tableset) -> Tableset:
        tableset[self.model.table].df.dropna(inplace=True)
        return tableset
