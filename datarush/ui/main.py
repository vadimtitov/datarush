import streamlit as st

from datarush.config import AppConfig
from datarush.core.dataflow import get_dataflow, set_dataflow
from datarush.core.templates import TemplateManager, dataflow_to_template, template_to_dataflow
from datarush.ui import operations, parameters


@st.cache_resource
def _get_template_manager() -> TemplateManager:
    return TemplateManager()


def main():
    """Main function for DataRush UI."""
    st.set_page_config(layout="wide")

    pages = [
        st.Page(parameters.parameters_page, title="Parameters"),
        st.Page(operations.operations_page, title="Operations"),
    ]

    main_page = st.navigation(pages, position="hidden")

    with st.sidebar:
        st.title("DataRush Template")
        st.divider()
        template_manager = _get_template_manager()
        templates = template_manager.list_templates()

        selected_template = st.selectbox(
            "Select Template", options=["- Create New Template -"] + templates
        )

        if selected_template != "- Create New Template -":
            versions = template_manager.list_template_versions(selected_template)
            selected_version = st.selectbox("Select Version", options=versions)

            if selected_template and selected_version:
                if st.button("Load"):
                    template = template_manager.read_template(selected_template, selected_version)
                    dataflow = template_to_dataflow(template)
                    set_dataflow(dataflow)

        else:
            with st.expander("Create New Template", expanded=True):
                new_template_name = st.text_input("New Template Name")
                new_template_version = st.text_input("Version (x.y.z)", value="0.0.0")
                if new_template_name and new_template_version:
                    if st.button("Save New Template"):
                        template = dataflow_to_template(get_dataflow())
                        template_manager.write_template(
                            template, template_name=new_template_name, version=new_template_version
                        )
                        st.success(
                            f"New template '{new_template_name}' created with version {new_template_version}"
                        )

        st.divider()

        for page in pages:
            st.page_link(page)

    main_page.run()


if __name__ == "__main__":
    AppConfig.validate()
    main()
