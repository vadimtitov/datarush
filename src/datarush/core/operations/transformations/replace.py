"""Replace text operation."""

from pydantic import Field

from datarush.core.dataflow import Operation, Tableset
from datarush.core.types import BaseOperationModel, ColumnStr, TableStr


class ReplaceModel(BaseOperationModel):
    """Replace operation model."""

    table: TableStr = Field(title="Table", description="Table to modify")
    columns: list[ColumnStr] = Field(
        title="Columns",
        description="Columns to apply the replacement to",
    )
    to_replace: str = Field(
        title="Value to Replace",
        description="Substring or regex pattern to be replaced",
    )
    replacement: str = Field(
        title="Replacement",
        description="String to replace with",
    )
    use_regex: bool = Field(
        title="Use Regex",
        description="Whether to treat 'Value to Replace' as a regex pattern",
        default=False,
    )


class Replace(Operation):
    """Replace values in one or more columns."""

    name = "replace"
    title = "Replace Values"
    description = "Replace string values in selected columns, optionally using regex"
    model: ReplaceModel

    def summary(self) -> str:
        """Provide operation summary."""
        method = "regex" if self.model.use_regex else "literal"
        return (
            f"Replace {method} `{self.model.to_replace}` with `{self.model.replacement}` "
            f"in columns {', '.join(f'**{col}**' for col in self.model.columns)} "
            f"of `{self.model.table}`"
        )

    def operate(self, tableset: Tableset) -> Tableset:
        """Run operation."""
        df = tableset.get_df(self.model.table)

        for col in self.model.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(
                    self.model.to_replace, self.model.replacement, regex=self.model.use_regex
                )
            )

        tableset.set_df(self.model.table, df)
        return tableset
