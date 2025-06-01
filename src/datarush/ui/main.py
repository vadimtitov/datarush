"""Main streamlit entrypoint."""

import streamlit as st

from datarush.config import DatarushConfig, set_datarush_config
from datarush.ui import operations, parameters, raw_template, sidebar


def run_ui(config: DatarushConfig | None = None) -> None:
    """Run DataRush UI.

    Args:
        initialize: Optional callable to run exactly once before the UI starts.
        config: Optional injectable DatarushConfig to use.
            If not provided, the default configuration is loaded from environment variables.
    """
    st.set_page_config(layout="wide")

    if not st.session_state.get("session_initialized", False):
        set_datarush_config(config)
        st.session_state["session_initialized"] = True

    pages = [
        st.Page(parameters.parameters_page, title="Parameters"),
        st.Page(operations.operations_page, title="Operations"),
        st.Page(raw_template.raw_template_page, title="Raw"),
    ]

    main_page = st.navigation(pages, position="hidden")
    sidebar.sidebar_section(pages)
    main_page.run()
