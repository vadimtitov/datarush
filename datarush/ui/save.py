import streamlit as st

from datarush.core.dataflow import get_dataflow
from datarush.core.template_manager import dataflow_to_json


def save_page():
    st.subheader("Template JSON")

    dataflow = get_dataflow()

    st.json(dataflow_to_json(dataflow))

    # Parse and display JSON
    if st.button("Save"):
        st.success("Template saved")
