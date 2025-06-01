"""
Run datarush UI.

Exported into a separate script because
streamlit command only accepts .py file as argument
"""

from datarush.ui import run_ui


if __name__ == "__main__":
    run_ui()
