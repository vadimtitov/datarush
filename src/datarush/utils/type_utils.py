"""Utilities for converting string values to typed Python objects."""

# flake8: noqa: D103

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any, Callable, Type, cast, get_args, get_origin

from datarush.core.types import ColumnStr, TableStr


def convert_to_type[T](value: str, to_type: type[T] | None) -> T:
    """Convert a string value to the specified type."""
    if is_string_enum(to_type):
        return to_type(value)  # type: ignore

    if to_type not in _TYPE_PARSERS:
        raise ValueError(f"Unsupported type {to_type} for conversion")
    parser = _TYPE_PARSERS[to_type]
    return cast(T, parser(value))


def is_string_enum(type_: Type | None) -> bool:
    """Check if the type is a string-based enum."""
    if type_ is None:
        return False
    origin_cls = get_origin(type_) or type_
    return issubclass(origin_cls, str) and issubclass(origin_cls, Enum)


def types_are_equal(type1: Type | None, type2: Type | None) -> bool:
    """Check if two generic types are structurally equal."""
    origin1, origin2 = get_origin(type1), get_origin(type2)
    args1, args2 = get_args(type1), get_args(type2)

    if origin1 is None or origin2 is None:
        return type1 is type2

    return origin1 == origin2 and args1 == args2


def _to_bool(value: str) -> bool:
    """Convert string to boolean."""
    if value == "True":
        return True
    if value == "False":
        return False
    raise ValueError(f"Cannot convert {value} to boolean")


_TYPE_PARSERS: dict[type, Callable[[str], Any]] = {
    bool: _to_bool,
    str: str,
    TableStr: str,
    ColumnStr: str,
    int: int,
    float: float,
    date: lambda v: datetime.fromisoformat(v).date(),
    datetime: datetime.fromisoformat,
    list[str]: lambda v: [item.strip().strip("'").strip('"') for item in v.strip("[]").split(",")],
    list[int]: lambda v: [int(item.strip()) for item in v.strip("[]").split(",")],
    list[float]: lambda v: [float(item.strip()) for item in v.strip("[]").split(",")],
}
