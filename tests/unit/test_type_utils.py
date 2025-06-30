from datetime import date, datetime
from enum import Enum
from typing import Literal

import pytest

from datarush.core.types import StringMap
from datarush.utils.type_utils import convert_to_type, is_string_enum, types_are_equal


# Mock Enum for testing
class MockEnum(str, Enum):
    OPTION_A = "option_a"
    OPTION_B = "option_b"


def test_convert_to_type_string():
    assert convert_to_type("test", str) == "test"


def test_convert_to_type_int():
    assert convert_to_type("42", int) == 42


def test_convert_to_type_float():
    assert convert_to_type("3.14", float) == 3.14


def test_convert_to_type_bool():
    assert convert_to_type("True", bool) is True
    assert convert_to_type("False", bool) is False
    with pytest.raises(ValueError):
        convert_to_type("not_a_bool", bool)


def test_convert_to_type_date():
    assert convert_to_type("2023-05-10", date) == date(2023, 5, 10)


def test_convert_to_type_datetime():
    assert convert_to_type("2023-05-10T15:30:00", datetime) == datetime(2023, 5, 10, 15, 30, 0)


def test_convert_to_type_list_of_strings():
    assert convert_to_type("['a', 'b', 'c']", list[str]) == ["a", "b", "c"]


def test_convert_to_type_list_of_ints():
    assert convert_to_type("[1, 2, 3]", list[int]) == [1, 2, 3]


def test_convert_to_type_list_of_floats():
    assert convert_to_type("[1.1, 2.2, 3.3]", list[float]) == [1.1, 2.2, 3.3]


def test_convert_to_type_enum():
    assert convert_to_type("option_a", MockEnum) == MockEnum.OPTION_A
    assert convert_to_type("option_b", MockEnum) == MockEnum.OPTION_B
    with pytest.raises(ValueError):
        convert_to_type("invalid_option", MockEnum)


def test_convert_to_type_string_map():
    assert convert_to_type("{'key1': 'value1', 'key2': 'value2'}", StringMap) == {
        "key1": "value1",
        "key2": "value2",
    }
    assert convert_to_type('{"key1": "value1", "key2": "value2"}', StringMap) == {
        "key1": "value1",
        "key2": "value2",
    }


def test_convert_to_type_literal():
    assert convert_to_type("A", Literal["A", "B"]) == "A"
    with pytest.raises(ValueError):
        convert_to_type("C", Literal["A", "B"])


def test_convert_to_type_pydantic_model():
    from pydantic import BaseModel

    class MyModel(BaseModel):
        name: str
        age: int

    model_instance = convert_to_type('{"name": "Alice", "age": 30}', MyModel)
    assert model_instance.name == "Alice"
    assert model_instance.age == 30

    with pytest.raises(ValueError):
        convert_to_type('{"name": "Alice"}', MyModel)  # Missing 'age' field


def test_convert_to_type_unsupported_type():
    class UnsupportedType:
        pass

    with pytest.raises(ValueError, match="Unsupported type"):
        convert_to_type("value", UnsupportedType)


def test_is_string_enum():
    assert is_string_enum(MockEnum) is True
    assert is_string_enum(str) is False
    assert is_string_enum(None) is False


def test_types_are_equal():
    assert types_are_equal(list[str], list[str]) is True
    assert types_are_equal(list[int], list[float]) is False
    assert types_are_equal(None, None) is True
    assert types_are_equal(str, int) is False
