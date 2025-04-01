from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Type, TypeVar, get_args, get_origin

from pydantic import BaseModel
from pydantic_core import PydanticUndefined
from streamlit.delta_generator import DeltaGenerator

from datarush.core.types import ContentType

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
) -> _TModel:
    model_dict = {}

    for name, field in schema.model_fields.items():
        kwargs = {
            "label": field.title or name,
            "key": f"{schema.__name__}_{name}_{key or ''}",
            "help": field.description,
        }
        default = field.default if field.default is not PydanticUndefined else None
        current_value = getattr(current_model, name) if current_model else None

        if name == "table":
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
