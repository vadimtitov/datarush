import numpy as np
import pandas as pd

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.normalize_empty_values import NormalizeEmptyValues


def make_tableset(df):
    return Tableset([Table("departments", df.copy())])


def test_normalize_empty_values_all_columns_and_no_custom():
    df = pd.DataFrame(
        {
            "col1": [1, None, 3, float("nan"), 5],
            "col2": ["a", "", "c", None, "e"],
            "col3": [1.1, None, 3.3, np.nan, 5.5],
            "col4": ["", "valid", None, "", "NA"],
        }
    )
    expected = pd.DataFrame(
        {
            "col1": [1, np.nan, 3, np.nan, 5],
            "col2": ["a", np.nan, "c", np.nan, "e"],
            "col3": [1.1, np.nan, 3.3, np.nan, 5.5],
            "col4": [np.nan, "valid", np.nan, np.nan, np.nan],
        }
    )
    model = {"table": "departments", "columns": [], "custom_empty_values": ["NA"]}
    op = NormalizeEmptyValues(model)
    result = op.operate(make_tableset(df))
    result_df = result.get_df("departments")
    pd.testing.assert_frame_equal(result_df, expected, check_dtype=False)
