"""Sidebar section for template selection, creation, and page navigation."""

import streamlit as st
from streamlit.navigation.page import StreamlitPage

from datarush.core.templates import (
    TemplateManager,
    dataflow_to_template,
    get_template_manager,
    template_to_dataflow,
)
from datarush.ui.state import get_dataflow, set_dataflow

OPTION_CREATE_NEW_TEMPLATE = "- Create New Template -"


def sidebar_section(pages: list[StreamlitPage]) -> None:
    """Render the sidebar with template and page controls."""
    st.sidebar.title("DataRush Template")
    st.sidebar.divider()
    template_manager = _get_template_manager()
    templates = template_manager.list_templates()

    param_template = _get_query_param("template")

    if param_template and param_template not in templates:
        raise ValueError(f"Template {param_template} not found in {templates}")

    options = [OPTION_CREATE_NEW_TEMPLATE] + templates
    selected_template = st.sidebar.selectbox(
        "Select Template",
        options=options,
        index=options.index(param_template or OPTION_CREATE_NEW_TEMPLATE),
    )

    if param_template and selected_template != param_template:
        _delete_query_param("version")

    if selected_template != OPTION_CREATE_NEW_TEMPLATE:
        versions = template_manager.list_template_versions(selected_template)

        param_version = _get_query_param("version")

        if param_version and param_version not in versions:
            raise ValueError(f"Version {param_version} not found in {versions}")

        index_version = versions.index(param_version) if param_version else 0

        selected_version = st.sidebar.selectbox(
            "Select Version", options=versions, index=index_version
        )

        if selected_template and selected_version:
            _set_query_param("template", selected_template)
            _set_query_param("version", selected_version)

            if st.sidebar.button("Load"):
                template = template_manager.read_template(selected_template, selected_version)
                dataflow = template_to_dataflow(template)
                set_dataflow(dataflow)

    else:
        with st.sidebar.expander("Create New Template", expanded=True):
            new_template_name = st.sidebar.text_input("New Template Name")
            new_template_version = st.sidebar.text_input("Version (x.y.z)", value="0.0.0")
            if new_template_name and new_template_version:
                if st.sidebar.button("Save New Template"):
                    template = dataflow_to_template(get_dataflow())
                    template_manager.write_template(
                        template, template_name=new_template_name, version=new_template_version
                    )
                    _set_query_param("template", new_template_name)
                    _set_query_param("version", new_template_version)

                    st.sidebar.success(
                        f"New template '{new_template_name}' created with version {new_template_version}"
                    )

    st.sidebar.divider()

    for page in pages:
        st.sidebar.page_link(page)


@st.cache_resource
def _get_template_manager() -> TemplateManager:
    """Get the cached template manager instance."""
    return get_template_manager()


def _get_query_param(param: str) -> str | None:
    """Retrieve a query parameter from the URL or session state."""
    value = st.query_params.get(param)

    if "query_params" not in st.session_state:
        st.session_state.query_params = {}

    if value:
        st.session_state.query_params[param] = value
        return value

    value = st.session_state.query_params.get(param)

    if value:
        st.query_params[param] = value
        return value

    return None


def _set_query_param(param: str, value: str) -> None:
    """Set a query parameter in both URL and session state."""
    st.query_params[param] = value

    if "query_params" not in st.session_state:
        st.session_state.query_params = {}

    st.session_state.query_params[param] = value


def _delete_query_param(param: str) -> None:
    """Delete a query parameter from the URL and session state."""
    if param in st.query_params:
        del st.query_params[param]
        st.session_state.query_params = st.query_params
