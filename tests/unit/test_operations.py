from pydantic import Field

from datarush.core.dataflow import Operation, Tableset
from datarush.core.operations import (
    get_operation_type_by_name,
    get_operation_type_by_title,
    list_operation_types,
    register_operation_type,
)
from datarush.core.types import BaseOperationModel, ColumnStr, TableStr

########################
####### FIXTURES #######
########################


class MockModel(BaseOperationModel):
    """Mock model."""

    table: TableStr = Field(title="Table", description="Table")
    column: ColumnStr = Field(title="Column", description="Column")
    arbitrary: str = Field(
        title="Arbitrary", description="Arbitrary field to test the model", default="default_value"
    )


class MockOperation(Operation):
    """Mock operation."""

    name = "mock_operation"
    title = "Mock Operation"
    description = "Mocking the operation for testing"

    model: MockModel

    def summary(self) -> str:
        """Provide summary."""
        return (
            f"Sort `{self.model.table}` by {self.model.column} in "
            f"{'ascending' if self.model.ascending else 'descending'} order"
        )

    def operate(self, tableset: Tableset) -> Tableset:
        """Run operation."""
        self.called = True
        return tableset


MOCK_OPERATION = MockOperation(
    model_dict={
        "table": "mock_table",
        "column": "mock_column",
        "arbitrary": "mock_value",
    }
)

register_operation_type(MockOperation)


def test_register_operation_type():
    assert MockOperation in list_operation_types()


def test_get_operation_type_by_title():
    assert get_operation_type_by_title("Mock Operation") == MockOperation


def test_get_operation_type_by_name():
    assert get_operation_type_by_name("mock_operation") == MockOperation
