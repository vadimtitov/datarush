import copy

import pandas as pd
import streamlit as st
from streamlit.delta_generator import DeltaGenerator

from datarush.core.dataflow import Dataflow, Source
from datarush.core.operations import OPERATION_TYPES, get_operation_type_by_title
from datarush.core.sources import SOURCE_TYPES, get_source_type_by_title


def _get_dataflow() -> Dataflow:
    if "dataflow" not in st.session_state:
        st.session_state["dataflow"] = Dataflow()
    return st.session_state["dataflow"]


def main():
    """Main function for DataRush UI."""
    st.set_page_config(layout="wide")

    pages = [
        st.Page(params_page, title="Parameters"),
        st.Page(operations_page, title="Operations"),
    ]

    main_page = st.navigation(pages, position="hidden")

    with st.sidebar:
        st.title("DataRush")

        for page in pages:
            st.page_link(page)

    main_page.run()


def params_page():
    st.subheader("Parameters")


def operations_page():
    left_section, right_section = st.columns([2, 2])

    with left_section:
        show_tables(left_section)

    with right_section:
        show_operations(right_section)
        show_add_operation_ui(right_section)

        if st.button("Run Operations"):
            _get_dataflow().transform()
            st.rerun()


# def show_sidebar_menu(options: list[str]) -> str:
#     selected = None
#     for option in options:
#         if st.sidebar.button(option, use_container_width=True):
#             selected = option

#     st.sidebar.markdown("---")

#     return selected or options[0]


def show_sources(section: DeltaGenerator) -> None:
    section.subheader("Data Sources")
    sources = _get_dataflow().sources
    for i, source in enumerate(sources):
        with section.expander(source.title, expanded=False):
            section.write(source.title)
            if source.update_from_streamlit(section, key=i):
                st.rerun()

    section.subheader("Add new source")
    source_type = get_source_type_by_title(
        section.selectbox("Source Type", [src.title for src in SOURCE_TYPES])
    )
    source = source_type.from_streamlit(section, key=len(sources))

    if source:
        if section.button("Add source"):
            _get_dataflow().add_source(source)
            st.rerun()


def show_sinks(section: DeltaGenerator) -> None:
    section.header("Data Sinks")
    # File Source Selection

    source = section.selectbox("Sink Type", ["Local File", "S3 File", "HTTP Request"])
    content_type = section.selectbox("Content Types", ["CSV", "JSON", "PARQUET"])


def show_tables(section: DeltaGenerator) -> None:
    section.subheader("Tables")

    dataflow = _get_dataflow()
    if not dataflow.sources:
        return

    tableset = dataflow.transformed_tableset or dataflow.original_tableset

    if not tableset:
        return

    # Dropdown to select loaded file
    show_original = section.checkbox("Show Original", key="show_original")

    table_selection = section.selectbox(
        "Select Table to Display",
        options=list(dataflow.original_tableset) if show_original else list(tableset),
    )

    if table_selection:
        if not show_original:  #  and table_selection in st.session_state["transformed_tables"]
            section.dataframe(tableset.get_df(table_selection), height=800)
        else:
            section.dataframe(dataflow.original_tableset.get_df(table_selection), height=800)


def show_operations(section: DeltaGenerator) -> None:
    """Displays and manages the list of operations."""
    st.subheader("Operations")
    to_remove = []

    operations = _get_dataflow().operations
    tableset = _get_dataflow().transformed_tableset or _get_dataflow().original_tableset

    # operation controls
    for i, op in enumerate(operations):
        with st.expander(op.summary(), expanded=False):
            cols = st.columns([1, 1, 1, 1])
            if cols[0].button("⬆", key=f"up_{i}") and i > 0:
                _get_dataflow().move_operation(i, i - 1)
                # st.rerun()
                return
            if cols[1].button("⬇", key=f"down_{i}") and i < len(operations) - 1:
                _get_dataflow().move_operation(i, i + 1)
                # st.rerun()
                return
            if cols[2].button("🗑️", key=f"remove_{i}"):
                to_remove.append(i)
            operations[i].is_enabled = cols[3].checkbox(
                "Enable", value=op.is_enabled, key=f"enable_{i}"
            )

            if op.update_from_streamlit(st, tableset, key=i):
                st.rerun()

    for index in reversed(to_remove):
        _get_dataflow().remove_operation(index)
        st.rerun()


def show_add_operation_ui(section: DeltaGenerator) -> None:
    """Provides UI for adding new operations."""
    with st.expander("Add New Operation", expanded=False):
        selected_op_title = st.selectbox(
            "Choose an operation to add", [cls.title for cls in OPERATION_TYPES]
        )
        if not selected_op_title:
            return

        operation_class = get_operation_type_by_title(selected_op_title)
        operation_future_index = len(_get_dataflow().operations)
        tableset = _get_dataflow().transformed_tableset or _get_dataflow().original_tableset

        new_operation = operation_class.from_streamlit(
            st=st, tableset=tableset, key=operation_future_index
        )

        if st.button("Add Operation"):
            if not new_operation:
                st.error("Fill in rqeuired operation params")
                return

            _get_dataflow().add_operation(new_operation)
            st.rerun()


if __name__ == "__main__":
    main()
