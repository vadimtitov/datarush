from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Iterator, Type, TypeVar, get_type_hints

import pandas as pd
from pydantic import BaseModel

from datarush.core.types import ParameterSpec
from datarush.exceptions import UnknownTableError
from datarush.utils.jinja2 import model_validate_jinja2

LOG = logging.getLogger(__name__)


class Table:

    def __init__(self, name: str, df: pd.DataFrame) -> None:
        self.name = name
        self.df = df

    def copy(self) -> Table:
        return Table(self.name, self.df.copy())


class Tableset:

    def __init__(self, tables: list[Table]) -> None:
        self._table_map = {table.name: table for table in tables}

    def copy(self) -> Tableset:
        return Tableset(table.copy() for table in self._table_map.values())

    def get_df(self, name: str) -> pd.DataFrame:
        table = self._table_map.get(name)
        if not table:
            raise UnknownTableError(name)
        return table.df

    def set_df(self, name: str, df: pd.DataFrame) -> None:
        self._table_map[name] = Table(name, df)

    def __getitem__(self, key: str) -> Table:
        return self._table_map[key]

    def __setitem__(self, key: str, value: Table) -> None:
        self._table_map[key] = value

    def __delitem__(self, key: str) -> None:
        del self._table_map[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._table_map.keys())

    def __bool__(self) -> bool:
        return bool(self._table_map)


class Operation(ABC):

    is_enabled: bool = True
    advanced_mode: bool = False

    def __init__(self, model_dict: dict[str, Any], advanced_mode: bool = False) -> None:
        self._model_dict = model_dict
        self._template_context = {}
        self.advanced_mode = advanced_mode

    @property
    def model_dict(self) -> dict[str, Any]:
        """
        Get the model dictionary.

        Returns:
            dict[str, Any]: The model dictionary.
        """
        return self._model_dict

    @property
    def model(self) -> _TModel:
        """
        Get the model instance.
        """
        if not self.advanced_mode:
            return self.schema().model_validate(self.model_dict)

        return model_validate_jinja2(
            self.schema(), self.model_dict, context=self._template_context
        )

    @classmethod
    def schema(cls) -> Type[_TModel]:
        """
        Get the model schema/type used by this operation.

        Returns:
            Type[_TModel]: The model class type.
        """
        return get_type_hints(cls)["model"]

    def update_template_context(self, context: dict[str, Any]) -> None:
        """
        Update the template context for Jinja2 rendering.

        Args:
            context (dict[str, Any]): The new context to use.
        """
        self._template_context = context

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def title(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def description(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def operate(self, table_set: Tableset) -> Tableset:
        """
        Perform the operation on the given Tableset.

        Args:
            table_set (Tableset): The input Tableset.

        Returns:
            Tableset: The transformed Tableset.
        """
        raise NotImplementedError

    @abstractmethod
    def summary(self) -> str:
        """
        Provide a summary of the operation.

        Returns:
            str: A human-readable summary.
        """
        raise NotImplementedError


class Dataflow:

    def __init__(
        self,
        parameters: list[ParameterSpec] | None = None,
        operations: list[Operation] | None = None,
    ) -> None:
        self._current_tableset = Tableset([])
        self._parameters = parameters or []
        self._parameters_values = {}
        self._operations = operations or []

    @property
    def current_tableset(self) -> Tableset:
        return self._current_tableset

    @property
    def parameters(self) -> list[ParameterSpec]:
        return self._parameters

    @property
    def operations(self) -> list[Operation]:
        return self._operations

    def add_parameter(self, parameter: ParameterSpec) -> None:
        self._parameters.append(parameter)

    def remove_parameter(self, position: int) -> None:
        if position < 0 or position >= len(self.parameters):
            raise IndexError("position is out of range")
        self._parameters.pop(position)

    def set_parameter_value(self, name: str, value: Any) -> None:
        # TODO: make some validation for value
        if name not in [p.name for p in self.parameters]:
            raise KeyError(f"Parameter {name} not found")
        self._parameters_values[name] = value

    def set_parameters_values(self, values: dict[str, Any]) -> None:
        for name, value in values.items():
            self.set_parameter_value(name, value)

    def add_operation(self, operation: Operation) -> None:
        self._operations.append(operation)

    def move_operation(self, from_position: int, to_position: int) -> None:
        if from_position < 0 or from_position >= len(self.operations):
            raise IndexError("from_position is out of range")
        if to_position < 0 or to_position >= len(self.operations):
            raise IndexError("to_position is out of range")

        item = self._operations.pop(from_position)
        self._operations.insert(to_position, item)

    def remove_operation(self, position: int) -> None:
        if position < 0 or position >= len(self.operations):
            raise IndexError("position is out of range")
        self._operations.pop(position)

    def get_current_context(self) -> dict[str, Any]:
        """
        Get the current context for the dataflow.

        Returns:
            dict[str, Any]: The current context.
        """
        return {"parameters": self._parameters_values}

    def run(self) -> None:
        self._current_tableset = Tableset([])
        for operation in self.operations:
            if operation.is_enabled:
                context = self.get_current_context()
                operation.update_template_context(context)
                self._current_tableset = operation.operate(self._current_tableset)


_TModel = TypeVar("_TModel", bound=BaseModel)
