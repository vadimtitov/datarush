import json

import streamlit as st
from streamlit_ace import THEMES, st_ace

from datarush.core.templates import dataflow_to_template
from datarush.ui.state import get_dataflow


def save_page():
    st.subheader("Template JSON")

    dataflow = get_dataflow()

    template = dataflow_to_template(dataflow)

    content_ace = st_ace(
        json.dumps(template, indent=4),
        language="json",
        theme="twilight",
    )

    # Parse and display JSON
    if st.button("Save"):
        st.success("Template saved")

    st.data_editor({}, use_container_width=True)
