"""Main streamlit entrypoint."""

from typing import Callable

import streamlit as st

from datarush.ui import operations, parameters, raw_template, sidebar


def run_ui(initialize: Callable[[], None] | None = None) -> None:
    """Run DataRush UI.

    Args:
        initialize: Optional callable to run exactly once before the UI starts.
            Can be used to set up the environment, e.g. load configuration.
    """
    if initialize and st.session_state.get("session_initialized", False) is False:
        initialize()
        st.session_state["session_initialized"] = True

    st.set_page_config(layout="wide")

    pages = [
        st.Page(parameters.parameters_page, title="Parameters"),
        st.Page(operations.operations_page, title="Operations"),
        st.Page(raw_template.raw_template_page, title="Raw"),
    ]

    main_page = st.navigation(pages, position="hidden")
    sidebar.sidebar_section(pages)
    main_page.run()
