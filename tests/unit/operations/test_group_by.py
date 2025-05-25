import pandas as pd

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.group_by import GroupBy


def test_groupby_sum():
    df = pd.DataFrame(
        {
            "category": ["A", "A", "B", "B"],
            "value": [1, 2, 3, 4],
        }
    )
    tableset = Tableset([Table("input", df)])

    model = {
        "table": "input",
        "group_by": ["category"],
        "aggregation_column": "value",
        "agg_func": "sum",
        "output_table": "result",
    }

    operation = GroupBy(model)
    result = operation.operate(tableset)
    grouped_df = result.get_df("result")

    assert grouped_df.shape == (2, 2)
    assert set(grouped_df["category"]) == {"A", "B"}
    assert grouped_df.loc[grouped_df["category"] == "A", "value"].iloc[0] == 3
    assert grouped_df.loc[grouped_df["category"] == "B", "value"].iloc[0] == 7


def test_groupby_count():
    df = pd.DataFrame(
        {
            "type": ["x", "x", "y", "z", "z", "z"],
            "value": [10, 20, 30, 40, 50, 60],
        }
    )
    tableset = Tableset([Table("data", df)])

    model = {
        "table": "data",
        "group_by": ["type"],
        "aggregation_column": "value",
        "agg_func": "count",
        "output_table": "counts",
    }

    operation = GroupBy(model)
    result = operation.operate(tableset)
    grouped_df = result.get_df("counts")

    assert grouped_df.shape == (3, 2)
    assert set(grouped_df["type"]) == {"x", "y", "z"}
    assert grouped_df.loc[grouped_df["type"] == "z", "value"].iloc[0] == 3
