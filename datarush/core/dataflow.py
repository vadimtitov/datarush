from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Iterator, Type, TypeVar, get_type_hints

import pandas as pd
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
        cls, st: DeltaGenerator, tableset: Tableset | None = None, key: int | str | None = None
    ) -> Operation | None:
        try:
            model = model_from_streamlit(cls.schema(), st=st, tableset=tableset, key=key)
            return cls(model)
        except ValidationError as e:
            print(e)
            return None

    def update_from_streamlit(
        self, st: DeltaGenerator, tableset: Tableset | None = None, key: int | str | None = None
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
        self._table_map[name].df = df

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


class Source(Process):

    @abstractmethod
    def read(self) -> Table:
        raise NotImplementedError


class Operation(Process):

    is_enabled: bool = True
    model: _TModel

    @abstractmethod
    def operate(self, table_set: Tableset) -> Tableset:
        raise NotImplementedError

    @abstractmethod
    def summary(self) -> str:
        raise NotImplementedError

    def __init__(self, model: _TModel) -> None:
        self.model = model

    @classmethod
    def schema(cls) -> Type[_TModel]:
        return get_type_hints(cls)["model"]


class Sink(Process):

    @property
    @abstractmethod
    def table_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def write(self, table: Table) -> None:
        raise NotImplementedError


class Dataflow:

    def __init__(
        self,
        sources: list[Source] | None = None,
        operations: list[Operation] | None = None,
        sinks: list[Sink] = None,
    ) -> None:
        self._sources = sources or []
        self._operations = operations or []
        self._sinks = sinks or []

        self._original_tableset: Tableset | None = None
        self._transformed_tableset: Tableset | None = None

    @property
    def sources(self) -> list[Source]:
        return self._sources

    @property
    def operations(self) -> list[Operation]:
        return self._operations

    @property
    def sinks(self) -> list[Sink]:
        return self._sinks

    @property
    def original_tableset(self) -> Tableset:
        if not self.sources:
            raise ValueError("No sources")

        LOG.info("Reading data from sources")
        if self._original_tableset is None:
            self._original_tableset = Tableset(source.read() for source in self.sources)
            self._transformed_tableset = self._original_tableset.copy()
        return self._original_tableset

    @property
    def transformed_tableset(self) -> Tableset | None:
        return self._transformed_tableset

    def add_source(self, source: Source) -> None:
        self._sources.append(source)

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

    def add_sink(self, sink: Sink) -> None:
        self._sinks.append(sink)

    def write_sinks(self, tableset: Tableset) -> None:
        LOG.info("Writing data to sinks")
        for sink in self.sinks:
            table = tableset[sink.table_name]
            sink.write(table)

    def transform(self) -> Tableset:
        self._transformed_tableset = self._original_tableset.copy()

        for operation in self.operations:
            if operation.is_enabled:
                self._transformed_tableset = operation.operate(self._transformed_tableset)
        return self._transformed_tableset

    def run(self) -> None:
        LOG.info("Running dataflow")
        self.write_sinks(tableset=self.transform())


# _TSource = TypeVar("_TSource", bound=Source)
_TModel = TypeVar("_TModel", bound=BaseModel)
