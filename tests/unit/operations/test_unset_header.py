import pandas as pd
import pandas.testing as pdt

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.unset_header import UnsetHeader


def test_unset_header_inplace():
    # Original DataFrame
    df = pd.DataFrame(
        columns=["name", "age"],
        data=[
            ["Alice", 30],
            ["Bob", 25],
        ],
    )
    tableset = Tableset([Table("people", df)])

    # Run operation
    model = {"table": "people"}
    operation = UnsetHeader(model)
    result = operation.operate(tableset)
    new_df = result.get_df("people")

    # Expected DataFrame after unsetting header
    expected_df = pd.DataFrame(
        data=[
            ["name", "age"],
            ["Alice", 30],
            ["Bob", 25],
        ]
    )
    expected_df.columns = [0, 1]

    # Compare actual and expected
    pdt.assert_frame_equal(new_df, expected_df, check_dtype=False)
