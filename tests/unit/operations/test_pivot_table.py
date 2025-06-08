import pandas as pd

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.pivot_table import PivotTable


def test_pivot_table_sum():
    df = pd.DataFrame(
        {
            "region": ["East", "East", "West", "West"],
            "category": ["A", "B", "A", "B"],
            "sales": [100, 150, 200, 250],
            "quantity": [1, 2, 3, 4],
        }
    )
    tableset = Tableset([Table("sales_data", df)])

    model = {
        "table": "sales_data",
        "index": ["region"],
        "columns": ["category"],
        "values": ["sales"],
        "aggfunc": "sum",
        "output_table": "pivoted",
    }

    op = PivotTable(model)
    result = op.operate(tableset)
    pivot_df = result.get_df("pivoted")

    # Columns will be MultiIndex if multiple values or columns
    assert "region" in pivot_df.columns
    assert ("sales", "A") in pivot_df.columns or "A" in pivot_df.columns


def test_pivot_table_mean_multiple_values():
    df = pd.DataFrame(
        {
            "group": ["G1", "G1", "G2", "G2"],
            "type": ["X", "Y", "X", "Y"],
            "val1": [10, 20, 30, 40],
            "val2": [1, 2, 3, 4],
        }
    )
    tableset = Tableset([Table("data", df)])

    model = {
        "table": "data",
        "index": ["group"],
        "columns": ["type"],
        "values": ["val1", "val2"],
        "aggfunc": "mean",
        "output_table": "pivoted_data",
    }

    op = PivotTable(model)
    result = op.operate(tableset)
    pivot_df = result.get_df("pivoted_data")

    assert "group" in pivot_df.columns
    assert ("val1", "X") in pivot_df.columns or "X" in pivot_df.columns
    assert ("val2", "Y") in pivot_df.columns or "Y" in pivot_df.columns
