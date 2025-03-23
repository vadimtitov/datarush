import streamlit as st

from datarush.core.dataflow import get_dataflow
from datarush.core.operations import OPERATION_TYPES, get_operation_type_by_title
from datarush.utils.misc import truncate


def operations_page():
    left_section, right_section = st.columns([2, 2])

    with left_section:
        show_tables()

    with right_section:
        show_operations()
        show_add_operation_ui()

        if st.button("Run Operations"):
            get_dataflow().run()
            st.rerun()


def show_tables() -> None:
    st.subheader("Tables")
    dataflow = get_dataflow()

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
    dataflow = get_dataflow()

    to_remove = []
    # operation controls
    for i, op in enumerate(dataflow.operations):
        with st.expander(truncate(op.summary(), n=80), expanded=False):
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

            if op.update_from_streamlit(dataflow.current_tableset, key=i):
                st.rerun()

    for index in reversed(to_remove):
        dataflow.remove_operation(index)
        st.rerun()


def show_add_operation_ui() -> None:
    """Provides UI for adding new operations."""
    dataflow = get_dataflow()
    with st.expander("Add New Operation", expanded=False):
        selected_op_title = st.selectbox(
            "Choose an operation to add", [cls.title for cls in OPERATION_TYPES]
        )
        if not selected_op_title:
            return

        operation_class = get_operation_type_by_title(selected_op_title)
        operation_future_index = len(dataflow.operations)

        new_operation = operation_class.from_streamlit(
            tableset=dataflow.current_tableset, key=operation_future_index
        )

        if st.button("Add Operation"):
            if not new_operation:
                st.error("Fill in rqeuired operation params")
                return

            dataflow.add_operation(new_operation)
            st.rerun()
