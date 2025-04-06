from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Iterator, Type, TypeVar, get_type_hints

import pandas as pd
import streamlit as st
from pydantic import BaseModel, ValidationError

from datarush.exceptions import UnknownTableError
from datarush.utils.types import model_dict_from_streamlit, model_validate_jinja2

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

    @classmethod
    def from_streamlit(
        cls, tableset: Tableset | None = None, key: int | str | None = None
    ) -> Operation | None:
        try:
            model_dict = model_dict_from_streamlit(
                cls.schema(),
                tableset=tableset,
                key=key,
            )
            return cls(model_dict)
        except ValidationError as e:
            return None

    def update_from_streamlit(
        self, tableset: Tableset | None = None, key: int | str | None = None
    ) -> bool:
        model_dict = model_dict_from_streamlit(
            self.schema(),
            tableset=tableset,
            key=key,
            current_model_dict=self.model_dict,
            advanced_mode=self.advanced_mode,
        )
        if self.model_dict == model_dict:
            return False

        if st.button("Update", f"operation_update_button_{key}"):
            self._model_dict = model_dict
            return True

        return False

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
        operations: list[Operation] | None = None,
    ) -> None:
        self._current_tableset = Tableset([])
        self._operations = operations or []

    @property
    def current_tableset(self) -> Tableset:
        return self._current_tableset

    @property
    def operations(self) -> list[Operation]:
        return self._operations

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

    def run(self) -> None:
        self._current_tableset = Tableset([])
        for operation in self.operations:
            if operation.is_enabled:
                operation.update_template_context(
                    {"bucket": "awesome", "object_key": "datasets/sample/test/data.csv"}
                )
                self._current_tableset = operation.operate(self._current_tableset)


def get_dataflow() -> Dataflow:
    if "dataflow" not in st.session_state:
        st.session_state["dataflow"] = Dataflow()
    return st.session_state["dataflow"]


def set_dataflow(dataflow: Dataflow) -> None:
    st.session_state["dataflow"] = dataflow


_TModel = TypeVar("_TModel", bound=BaseModel)
