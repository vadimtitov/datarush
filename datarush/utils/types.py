from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Type, TypeVar, get_args, get_origin

from pydantic import BaseModel
from pydantic_core import PydanticUndefined
from streamlit.delta_generator import DeltaGenerator
from streamlit_ace import st_ace  # Ensure you have the streamlit-ace library installed

from datarush.core.types import ContentType
from datarush.utils.jinja2 import render_jinja2_template

if TYPE_CHECKING:
    from datarush.core.dataflow import Tableset


def types_are_equal(type1: Type, type2: Type) -> bool:
    """Checks if two generic types (like list[str] and list[int]) are the same."""
    return get_origin(type1) == get_origin(type2) and get_args(type1) == get_args(type2)


_TModel = TypeVar("_TModel", bound=BaseModel)


def model_from_streamlit(
    schema: Type[_TModel],
    st: DeltaGenerator,
    tableset: Tableset | None = None,
    key: str | int | None = None,
    current_model: _TModel | None = None,
    advanced_mode: bool = False,
) -> _TModel:
    model_dict = {}

    for name, field in schema.model_fields.items():
        kwargs = {
            "label": field.title or name,
            "key": f"{schema.__name__}_{advanced_mode}_{name}_{key or ''}",
            "help": field.description,
        }
        default = field.default if field.default is not PydanticUndefined else None
        current_value = getattr(current_model, name) if current_model else None

        if advanced_mode:
            # value = convert_to_type(
            #     st.text_input(value=str(current_value), **kwargs), to_type=field.annotation
            # )
            st.write(kwargs["label"])
            raw_value = st_ace(
                str(current_value),
                language="django",
                theme="twilight",
                keybinding="vscode",
                min_lines=1,
                max_lines=5,
                show_gutter=False,
                auto_update=False,
                key=kwargs["key"],
            )
            value = render_jinja2_template(raw_value, context={})
            value = convert_to_type(value, to_type=field.annotation)

        elif name == "table":
            if tableset:
                options = list(tableset)
                index = options.index(current_value) if current_value is not None else 0
                value = st.selectbox(options=options, index=index, **kwargs)
            else:
                value = st.selectbox(
                    options=[current_value] if current_value is not None else [], **kwargs
                )

        elif name == "tables":
            if tableset:
                options = list(tableset)
                value = st.multiselect(options=options, default=current_value or None, **kwargs)
            else:
                value = st.multiselect(
                    options=current_value or [], default=current_value, **kwargs
                )

        elif name in ("column", "columns"):
            if tableset:
                relevant_tables = []
                table = model_dict.get("table")
                if table:
                    relevant_tables.append(table)

                tables = model_dict.get("tables")
                if tables:
                    relevant_tables.extend(tables)

                relevant_columns = sorted(
                    {col for name in relevant_tables for col in tableset.get_df(name).columns}
                )
                if name == "column":
                    index = (
                        relevant_columns.index(current_value) if current_value is not None else 0
                    )
                    value = st.selectbox(options=relevant_columns, index=index, **kwargs)
                else:
                    value = st.multiselect(
                        options=relevant_columns, default=current_value, **kwargs
                    )
            else:
                if name == "column":
                    value = st.selectbox(
                        options=[current_value] if current_value is not None else [], **kwargs
                    )
                else:
                    value = st.multiselect(
                        options=current_value or [], default=current_value, **kwargs
                    )

        elif _is_string_enum(field.annotation):
            value = st.selectbox(options=list(field.annotation), **kwargs)

        elif field.annotation is bytes:
            content_type: ContentType | None = model_dict.get("content_type")
            extension = content_type.extension() if content_type else None
            file = st.file_uploader("Choose File", type=extension, key=f"local_file_file_df_{key}")
            value = file.getvalue() if file else None

        elif field.annotation is bool:
            value = st.checkbox(
                value=current_value if current_value is not None else default, **kwargs
            )

        elif issubclass(field.annotation, str):
            value = st.text_input(
                value=current_value if current_value is not None else (default or ""), **kwargs
            )

        elif field.annotation is int:
            value = st.number_input(
                value=current_value if current_value is not None else default, step=1, **kwargs
            )

        elif field.annotation is float:
            value = st.number_input(
                value=current_value if current_value is not None else default, step=0.01, **kwargs
            )

        else:
            raise TypeError(f"Not supported type: {field.annotation}")

        if value is not None:
            model_dict[name] = value

    return schema.model_validate(model_dict)

    # text = st.text_input("Enter text")  # str
    # number = st.number_input("Enter number")  # int or float
    # slider = st.slider("Pick a value", 0, 100)  # int or float
    # checkbox = st.checkbox("Agree?")  # bool
    # radio = st.radio("Select an option", ["Option 1", "Option 2"])  # str
    # selectbox = st.selectbox("Choose", ["A", "B", "C"])  # str
    # multiselect = st.multiselect("Choose multiple", ["X", "Y", "Z"])  # List[str]
    # text_area = st.text_area("Enter multi-line text")  # str
    # submit = st.form_submit_button("Submit")


def _is_string_enum(type_: Type) -> bool:
    origin_cls = get_origin(type_) or type_
    return issubclass(origin_cls, str) and issubclass(origin_cls, Enum)


def convert_to_type(value: str, to_type: type[T]) -> T:
    """
    Convert a string value to the specified type using a dictionary of parsers.

    Args:
        value (str): The string to be converted.
        to_type (type): The target type to which the value should be converted.

    Returns:
        Any: The converted value in the target type.
    """
    if to_type not in TYPE_PARSERS:
        raise ValueError(f"Unsupported type {to_type} for conversion")
    parser = TYPE_PARSERS[to_type]
    return parser(value)


def _to_bool(value: str) -> bool:
    if value == "True":
        return True
    if value == "False":
        return False
    raise ValueError(f"Cannot convert {value} to boolean")


TYPE_PARSERS: dict[type, Callable[[str], Any]] = {
    bool: _to_bool,
    str: str,
    int: int,
    float: float,
    list[str]: lambda v: [item.strip().strip("'").strip('"') for item in v.strip("[]").split(",")],
    list[int]: lambda v: [int(item.strip()) for item in v.strip("[]").split(",")],
    list[float]: lambda v: [float(item.strip()) for item in v.strip("[]").split(",")],
}
