import jinja2
import pytest
from pydantic import BaseModel, Field

from datarush.utils.jinja2 import model_validate_jinja2, render_jinja2_template


def test_render_jinja2_template():
    template_str = "Hello, {{ name }}!"
    context = {"name": "World"}
    result = render_jinja2_template(template_str, context)
    assert result == "Hello, World!"


def test_render_jinja2_template_with_missing_context():
    template_str = "Hello, {{ name }}!"
    context = {}  # Missing 'name'
    result = render_jinja2_template(template_str, context)
    assert result == "Hello, !"


def test_render_jinja2_template_with_complex_context():
    template_str = "The sum of {{ a }} and {{ b }} is {{ a + b }}."
    context = {"a": 5, "b": 3}
    result = render_jinja2_template(template_str, context)
    assert result == "The sum of 5 and 3 is 8."


def test_model_validate_jinja2():
    class TestModel(BaseModel):
        name: str
        age: int

    model_dict = {
        "name": "{{ first_name }} {{ last_name }}",
        "age": "{{ birth_year | int + 20 }}",
    }
    context = {
        "first_name": "John",
        "last_name": "Doe",
        "birth_year": "2000",
    }

    result = model_validate_jinja2(TestModel, model_dict, context)
    assert result.name == "John Doe"
    assert result.age == 2020


def test_model_validate_jinja2_with_defaults():
    class TestModel(BaseModel):
        name: str
        age: int = Field(default=30)

    model_dict = {
        "name": "{{ first_name }} {{ last_name }}",
    }
    context = {
        "first_name": "Jane",
        "last_name": "Smith",
    }

    result = model_validate_jinja2(TestModel, model_dict, context)
    assert result.name == "Jane Smith"
    assert result.age == 30  # Default value is used


def test_model_validate_jinja2_with_missing_context():
    class TestModel(BaseModel):
        name: str
        age: int

    model_dict = {
        "name": "{{ first_name }} {{ last_name }}",
        "age": "{{ birth_year | int + 20 }}",
    }
    context = {}  # Missing required context keys

    with pytest.raises(jinja2.exceptions.UndefinedError):
        model_validate_jinja2(TestModel, model_dict, context)


def test_model_validate_jinja2_with_invalid_type_conversion():
    class TestModel(BaseModel):
        age: int

    model_dict = {
        "age": "{{ invalid_int }}",
    }
    context = {"invalid_int": "not_an_int"}

    with pytest.raises(ValueError):
        model_validate_jinja2(TestModel, model_dict, context)
