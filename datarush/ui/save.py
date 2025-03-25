import streamlit as st

from datarush.core.dataflow import get_dataflow
from datarush.core.templates import dataflow_to_template


def save_page():
    st.subheader("Template JSON")

    dataflow = get_dataflow()

    st.json(dataflow_to_template(dataflow))

    # Parse and display JSON
    if st.button("Save"):
        st.success("Template saved")
