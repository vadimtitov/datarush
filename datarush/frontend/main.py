import json

import streamlit as st

from datarush.core.dataflow import Dataflow
from datarush.core.operations import OPERATION_TYPES, get_operation_type_by_title


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
        st.title("DataRush Template")

        for page in pages:
            st.page_link(page)

    main_page.run()


def params_page():
    st.subheader("Parameters")

    default_json = {"key": "value"}

    # Pre-populate the text area with JSON
    default_json_str = json.dumps(default_json, indent=4)
    json_input = st.text_area("Edit Input parameters here:", value=default_json_str, height=300)

    st.data_editor(data=default_json, key="data_editor")

    # Parse and display JSON
    if st.button("Validate JSON"):
        try:
            parsed_json = json.loads(json_input)
            st.success("Valid JSON!")
            st.json(parsed_json)
        except json.JSONDecodeError as e:
            st.error(f"Invalid JSON: {e}")


def operations_page():
    left_section, right_section = st.columns([2, 2])

    with left_section:
        show_tables()

    with right_section:
        show_operations()
        show_add_operation_ui()

        if st.button("Run Operations"):
            _get_dataflow().run()
            st.rerun()


def show_tables() -> None:
    st.subheader("Tables")
    dataflow = _get_dataflow()

    if not dataflow.current_tableset:
        st.write("No tables loaded, use operations to load tables")
        return

    selected_table_name = st.selectbox(
        "Select Table to Display",
        options=list(dataflow.current_tableset),
    )

    if selected_table_name:
        table = dataflow.current_tableset[selected_table_name]
        st.dataframe(table.df, height=800)


def show_operations() -> None:
    """Displays and manages the list of operations."""
    st.subheader("Operations")
    dataflow = _get_dataflow()

    to_remove = []
    # operation controls
    for i, op in enumerate(dataflow.operations):
        with st.expander(op.summary(), expanded=False):
            cols = st.columns([1, 1, 1, 1])
            if cols[0].button("⬆", key=f"up_{i}") and i > 0:
                dataflow.move_operation(i, i - 1)
                # st.rerun()
                return
            if cols[1].button("⬇", key=f"down_{i}") and i < len(dataflow.operations) - 1:
                dataflow.move_operation(i, i + 1)
                # st.rerun()
                return
            if cols[2].button("🗑️", key=f"remove_{i}"):
                to_remove.append(i)
            dataflow.operations[i].is_enabled = cols[3].checkbox(
                "Enable", value=op.is_enabled, key=f"enable_{i}"
            )

            if op.update_from_streamlit(st, dataflow.current_tableset, key=i):
                st.rerun()

    for index in reversed(to_remove):
        dataflow.remove_operation(index)
        st.rerun()


def show_add_operation_ui() -> None:
    """Provides UI for adding new operations."""
    dataflow = _get_dataflow()
    with st.expander("Add New Operation", expanded=False):
        selected_op_title = st.selectbox(
            "Choose an operation to add", [cls.title for cls in OPERATION_TYPES]
        )
        if not selected_op_title:
            return

        operation_class = get_operation_type_by_title(selected_op_title)
        operation_future_index = len(dataflow.operations)

        new_operation = operation_class.from_streamlit(
            st=st, tableset=dataflow.current_tableset, key=operation_future_index
        )

        if st.button("Add Operation"):
            if not new_operation:
                st.error("Fill in rqeuired operation params")
                return

            dataflow.add_operation(new_operation)
            st.rerun()


if __name__ == "__main__":
    main()
