"""Page for saving the dataflow as a JSON template."""

import json

import streamlit as st
from streamlit_ace import st_ace

from datarush.core.templates import dataflow_to_template
from datarush.ui.state import get_dataflow


def raw_template_page() -> None:
    """Render the save page with template JSON viewer."""
    st.subheader("Template JSON")

    dataflow = get_dataflow()

    template = dataflow_to_template(dataflow)

    _ = st_ace(
        json.dumps(template, indent=4),
        language="json",
        theme="twilight",
    )
