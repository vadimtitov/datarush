from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Iterator, Type, TypeVar, get_type_hints

import pandas as pd
import streamlit as st
from pydantic import BaseModel, ValidationError
from streamlit.delta_generator import DeltaGenerator

from datarush.exceptions import UnknownTableError
from datarush.utils.types import model_from_streamlit

LOG = logging.getLogger(__name__)


class Process(ABC):

    def __init__(self, model: _TModel) -> None:
        self.model = model

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

    @classmethod
    def schema(cls) -> Type[_TModel]:
        return get_type_hints(cls)["model"]

    @classmethod
    def from_streamlit(
        cls, tableset: Tableset | None = None, key: int | str | None = None
    ) -> Operation | None:
        try:
            model = model_from_streamlit(cls.schema(), st=st, tableset=tableset, key=key)
            return cls(model)
        except ValidationError as e:
            return None

    def update_from_streamlit(
        self, tableset: Tableset | None = None, key: int | str | None = None
    ) -> bool:
        model = model_from_streamlit(self.schema(), st=st, tableset=tableset, key=key)
        if self.model == model:
            return False

        if st.button("Update", f"operation_update_button_{key}"):
            self.model = model
            return True

        return False


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


class Operation(Process):
    """
    Base class for operations applied to a Tableset using a specified model.
    """

    is_enabled: bool = True
    model: _TModel

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

    @classmethod
    def schema(cls) -> Type[_TModel]:
        """
        Get the model schema/type used by this operation.

        Returns:
            Type[_TModel]: The model class type.
        """
        return get_type_hints(cls)["model"]


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
                self._current_tableset = operation.operate(self._current_tableset)


def get_dataflow() -> Dataflow:
    if "dataflow" not in st.session_state:
        st.session_state["dataflow"] = Dataflow()
    return st.session_state["dataflow"]


_TModel = TypeVar("_TModel", bound=BaseModel)
