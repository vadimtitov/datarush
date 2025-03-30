import json

import jinja2
import streamlit as st
from streamlit_ace import st_ace

from datarush.utils.jinja2 import render_jinja2_template


def parameters_page():
    st.subheader("Parameters")

    st.markdown("---")

    default_json = {"some_key": "some value"}
    jinja2_input = "{{ some_key }}"

    st.markdown("##### Mock JSON Input")
    json_input = st_ace(
        json.dumps(default_json, indent=4),
        language="json",
        theme="twilight",
        keybinding="vscode",
        min_lines=5,
        auto_update=True,
    )
    try:
        parameters = json.loads(json_input)
    except json.decoder.JSONDecodeError:
        parameters = {}
        st.error("Invalid JSON input")

    st.markdown("---")

    st.markdown("##### Jinja2 playground")
    left, right = st.columns(2)
    with left:
        st.write("Template")
        jinja2_input = st_ace(
            jinja2_input,
            placeholder="Jinja2 template",
            language="django",
            theme="twilight",
            keybinding="vscode",
            auto_update=True,
        )
    with right:
        st.write("Result")
        try:
            rendered_template = render_jinja2_template(jinja2_input, parameters)
            st.code(rendered_template)
        except jinja2.exceptions.TemplateSyntaxError:
            st.error("Invalid Jinja2 template syntax")
