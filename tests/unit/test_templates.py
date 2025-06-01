from unittest.mock import patch

import pytest
from pydantic import Field

from datarush.config import FilesystemTemplateStoreConfig, S3TemplateStoreConfig
from datarush.core.dataflow import Dataflow, Operation, Tableset
from datarush.core.operations import register_operation_type
from datarush.core.templates import (
    FilesystemTemplateManager,
    S3TemplateManager,
    dataflow_to_template,
    template_to_dataflow,
)
from datarush.core.types import BaseOperationModel, ColumnStr, ParameterSpec, TableStr

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


########################
#######  TESTS  ########
########################


@pytest.fixture
def mock_s3_client():
    with patch("datarush.utils.s3_client.S3Client") as MockS3Client:
        yield MockS3Client.return_value


@patch("datarush.core.templates.S3Client.list_folders", return_value=["template1", "template2"])
def test_s3_template_manager_list_templates(mock_list_folders):
    manager = S3TemplateManager(
        config=S3TemplateStoreConfig(bucket="sample-bucket", prefix="datarush")
    )
    templates = manager.list_templates()
    assert templates == ["template1", "template2"]


def test_filesystem_template_manager_list_templates(tmp_path):
    templates_path = tmp_path / "templates"
    templates_path.mkdir()
    (templates_path / "template1").mkdir()
    (templates_path / "template2").mkdir()

    manager = FilesystemTemplateManager(config=FilesystemTemplateStoreConfig(path="."))
    manager._path = str(tmp_path)
    templates = manager.list_templates()
    assert sorted(templates) == ["template1", "template2"]


def test_template_to_dataflow():
    template = {
        "parameters": [
            {
                "name": "param1",
                "type": "string",
                "description": "Test parameter",
                "default": "value",
                "required": True,
            }
        ],
        "operations": [
            {
                "name": "mock_operation",
                "data": {"key": "value"},
                "advanced_mode": False,
            }
        ],
    }

    register_operation_type(MockOperation)

    dataflow = template_to_dataflow(template)

    assert len(dataflow.parameters) == 1
    assert dataflow.parameters[0].name == "param1"
    assert len(dataflow.operations) == 1
    assert isinstance(dataflow.operations[0], MockOperation)


def test_dataflow_to_template():
    param = ParameterSpec(
        name="param1",
        type="string",
        description="Test parameter",
        default="value",
        required=True,
    )
    operation = MockOperation(model_dict={"key": "value"})
    dataflow = Dataflow(parameters=[param], operations=[operation])

    template = dataflow_to_template(dataflow)
    assert len(template["parameters"]) == 1
    assert template["parameters"][0]["name"] == "param1"
    assert len(template["operations"]) == 1
    assert template["operations"][0]["name"] == "mock_operation"
