import json

import streamlit as st


def parameters_page():
    st.subheader("Parameters")

    default_json = {"key": "value"}

    # Pre-populate the text area with JSON
    default_json_str = json.dumps(default_json, indent=4)
    json_input = st.text_area("Edit Input parameters here:", value=default_json_str, height=300)

    st.data_editor(data=default_json, key="data_editor")

    # Parse and display JSON
    if st.button("Validate JSON"):
        try:
            parsed_json = json.loads(json_input)
            st.success("Valid JSON!")
            st.json(parsed_json)
        except json.JSONDecodeError as e:
            st.error(f"Invalid JSON: {e}")
