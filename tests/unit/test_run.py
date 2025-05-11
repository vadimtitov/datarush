from unittest.mock import MagicMock, patch

import pytest

from datarush.core.types import ParameterSpec
from datarush.run import (
    _parse_parameter_values_from_specs,
    run_template,
    run_template_from_command_line,
)


@pytest.fixture
def mock_template_manager():
    with patch("datarush.run.get_template_manager") as mock_manager:
        yield mock_manager


@pytest.fixture
def mock_dataflow():
    with patch("datarush.run.template_to_dataflow") as mock_dataflow:
        mock_instance = MagicMock()
        mock_dataflow.return_value = mock_instance
        yield mock_instance


def test_run_template(mock_template_manager, mock_dataflow):
    # Mock template manager and dataflow
    mock_template_manager.return_value.read_template.return_value = {"mock": "template"}
    mock_dataflow.parameters = [
        ParameterSpec(
            name="param1",
            type="string",
            description="Test parameter",
            default="default_value",
            required=False,
        )
    ]

    # Call the function
    run_template("test_template", "v1", {"param1": "value1"})

    # Assertions
    mock_template_manager.return_value.read_template.assert_called_once_with("test_template", "v1")
    mock_dataflow.set_parameters_values.assert_called_once_with({"param1": "value1"})
    mock_dataflow.run.assert_called_once()


def test_run_template_no_parameters(mock_template_manager, mock_dataflow):
    # Mock template manager and dataflow
    mock_template_manager.return_value.read_template.return_value = {"mock": "template"}
    mock_dataflow.parameters = []

    # Call the function
    run_template("test_template", "v1")

    # Assertions
    mock_template_manager.return_value.read_template.assert_called_once_with("test_template", "v1")
    mock_dataflow.set_parameters_values.assert_not_called()
    mock_dataflow.run.assert_called_once()


def test_run_template_from_command_line(mock_template_manager, mock_dataflow, monkeypatch):
    # Mock template manager and dataflow
    mock_template_manager.return_value.read_template.return_value = {"mock": "template"}
    mock_dataflow.parameters = [
        ParameterSpec(
            name="param1",
            type="string",
            description="Test parameter",
            default="default_value",
            required=False,
        )
    ]

    # Mock command-line arguments
    monkeypatch.setattr(
        "sys.argv",
        ["run.py", "--template", "test_template", "--version", "v1", "--param1", "value1"],
    )

    # Call the function
    run_template_from_command_line()

    # Assertions
    mock_template_manager.return_value.read_template.assert_called_once_with("test_template", "v1")
    mock_dataflow.set_parameters_values.assert_called_once_with({"param1": "value1"})
    mock_dataflow.run.assert_called_once()


def test_parse_parameter_values_from_specs():
    # Define parameter specs
    parameter_specs = [
        ParameterSpec(
            name="param1",
            type="string",
            description="Test parameter",
            default="default_value",
            required=False,
        ),
        ParameterSpec(
            name="param2",
            type="integer",
            description="Test integer parameter",
            default="10",
            required=True,
        ),
    ]

    # Define parameter values
    parameter_values = {"param1": "custom_value", "param2": "42"}

    # Call the function
    result = _parse_parameter_values_from_specs(parameter_specs, parameter_values)

    # Assertions
    assert result == {"param1": "custom_value", "param2": 42}


def test_parse_parameter_values_from_specs_missing_required():
    # Define parameter specs
    parameter_specs = [
        ParameterSpec(
            name="param1",
            type="string",
            description="Test parameter",
            default="default_value",
            required=False,
        ),
        ParameterSpec(
            name="param2",
            type="integer",
            description="Test integer parameter",
            default="",
            required=True,
        ),
    ]

    # Define parameter values
    parameter_values = {"param1": "custom_value"}

    # Call the function and expect an exception
    with pytest.raises(ValueError, match="Parameter param2 not found in the provided values"):
        _parse_parameter_values_from_specs(parameter_specs, parameter_values)
