import streamlit as st

from datarush.config import AppConfig
from datarush.ui import operations, parameters, sidebar


def main():
    """Main function for DataRush UI."""
    st.set_page_config(layout="wide")

    pages = [
        st.Page(parameters.parameters_page, title="Parameters"),
        st.Page(operations.operations_page, title="Operations"),
    ]

    main_page = st.navigation(pages, position="hidden")
    sidebar.sidebar_section(pages)
    main_page.run()


if __name__ == "__main__":
    AppConfig.validate()
    main()
