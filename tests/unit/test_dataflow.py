from typing import Any

import pandas as pd
import pytest
from pydantic import Field

from datarush.core.dataflow import Dataflow, Operation, Table, Tableset
from datarush.core.types import BaseOperationModel, ColumnStr, ParameterSpec, TableStr
from datarush.exceptions import UnknownTableError

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
        self.called = True
        return tableset


MOCK_OPERATION = MockOperation(
    model_dict={
        "table": "mock_table",
        "column": "mock_column",
        "arbitrary": "mock_value",
    }
)

########################
#######  TESTS  ########
########################


def test_table_initialization():
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    table = Table(name="test_table", df=df)
    assert table.name == "test_table"
    pd.testing.assert_frame_equal(table.df, df)


def test_table_copy():
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    table = Table(name="test_table", df=df)
    copied_table = table.copy()
    assert copied_table.name == "test_table"
    pd.testing.assert_frame_equal(copied_table.df, df)
    assert copied_table is not table  # Ensure it's a deep copy


def test_tableset_initialization():
    df1 = pd.DataFrame({"col1": [1, 2]})
    df2 = pd.DataFrame({"col2": [3, 4]})
    table1 = Table(name="table1", df=df1)
    table2 = Table(name="table2", df=df2)
    tableset = Tableset([table1, table2])
    assert len(list(tableset)) == 2
    assert "table1" in tableset
    assert "table2" in tableset


def test_tableset_get_df():
    df = pd.DataFrame({"col1": [1, 2]})
    table = Table(name="table1", df=df)
    tableset = Tableset([table])
    pd.testing.assert_frame_equal(tableset.get_df("table1"), df)


def test_tableset_get_df_unknown_table():
    tableset = Tableset([])
    with pytest.raises(UnknownTableError):
        tableset.get_df("nonexistent_table")


def test_tableset_set_df():
    df = pd.DataFrame({"col1": [1, 2]})
    tableset = Tableset([])
    tableset.set_df("table1", df)
    pd.testing.assert_frame_equal(tableset.get_df("table1"), df)


def test_dataflow_initialization():
    dataflow = Dataflow()
    assert dataflow.parameters == []
    assert dataflow.operations == []


def test_dataflow_add_parameter():
    param = ParameterSpec(
        name="param1", type="string", description="", default="value", required=True
    )
    dataflow = Dataflow()
    dataflow.add_parameter(param)
    assert len(dataflow.parameters) == 1
    assert dataflow.parameters[0].name == "param1"


def test_dataflow_set_parameter_value():
    param = ParameterSpec(
        name="param1", type="string", description="", default="value", required=True
    )
    dataflow = Dataflow(parameters=[param])
    dataflow.set_parameter_value("param1", "new_value")
    assert dataflow.get_current_context()["parameters"]["param1"] == "new_value"


def test_dataflow_add_operation():
    operation = MOCK_OPERATION
    dataflow = Dataflow()
    dataflow.add_operation(operation)
    assert len(dataflow.operations) == 1
    assert dataflow.operations[0] is operation


def test_dataflow_run():
    operation = MOCK_OPERATION
    dataflow = Dataflow(operations=[operation])
    dataflow.run()
    assert operation.called


def test_dataflow_run_with_cache():
    # Prepare mock operations
    class MockOperationWithCache(Operation):
        """Mock operation with caching."""

        name = "mock_operation_with_cache"
        title = "Mock Operation With Cache"
        description = "Mocking the operation for testing with cache"

        model: MockModel

        def __init__(self, model_dict: dict[str, Any], advanced_mode: bool = False) -> None:
            super().__init__(model_dict, advanced_mode)
            self.called_count = 0

        def summary(self) -> str:
            """Provide summary."""
            return "Mock operation summary"

        def operate(self, tableset: Tableset) -> Tableset:
            self.called_count += 1
            return tableset

    operation1 = MockOperationWithCache(
        model_dict={
            "table": "mock_table_1",
            "column": "mock_column_1",
            "arbitrary": "mock_value_1",
        }
    )
    operation2 = MockOperationWithCache(
        model_dict={
            "table": "mock_table_2",
            "column": "mock_column_2",
            "arbitrary": "mock_value_2",
        }
    )
    operation3 = MockOperationWithCache(
        model_dict={
            "table": "mock_table_3",
            "column": "mock_column_3",
            "arbitrary": "mock_value_3",
        }
    )

    # Create a dataflow with the first operation
    dataflow = Dataflow(operations=[operation1])
    dataflow.run_with_cache()
    assert operation1.called_count == 1
    assert operation2.called_count == 0

    # Appending new operation does not re-run the first operation
    dataflow.add_operation(operation2)
    dataflow.run_with_cache()
    assert operation1.called_count == 1  # Cached result used
    assert operation2.called_count == 1  # New operation called

    # Changing parameter
    dataflow.add_parameter(
        ParameterSpec(name="param1", type="string", description="", default="value", required=True)
    )
    dataflow.set_parameter_value("param1", "new_value")
    dataflow.run_with_cache()
    assert operation1.called_count == 2
    assert operation2.called_count == 2

    # Adding a third operation
    dataflow.add_operation(operation3)
    dataflow.run_with_cache()
    assert operation1.called_count == 2
    assert operation2.called_count == 2
    assert operation3.called_count == 1

    # Swapping operations
    dataflow.move_operation(2, 1)
    dataflow.run_with_cache()
    assert operation1.called_count == 2
    assert operation2.called_count == 3
    assert operation3.called_count == 2
