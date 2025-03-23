import streamlit as st

from datarush.ui import operations, parameters


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

        for page in pages:
            st.page_link(page)

    main_page.run()


if __name__ == "__main__":
    main()
