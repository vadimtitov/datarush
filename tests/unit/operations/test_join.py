import pandas as pd

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.join import JoinTables


def test_inner_join():
    left_df = pd.DataFrame({"id": [1, 2, 3], "val_left": ["a", "b", "c"]})
    right_df = pd.DataFrame({"id": [2, 3, 4], "val_right": ["x", "y", "z"]})
    tableset = Tableset(
        [
            Table("left", left_df),
            Table("right", right_df),
        ]
    )

    model = {
        "left_table": "left",
        "right_table": "right",
        "left_on": "id",
        "right_on": "id",
        "join_type": "inner",
        "output_table": "joined",
    }

    operation = JoinTables(model)
    result = operation.operate(tableset)
    joined_df = result.get_df("joined")

    assert list(joined_df.columns) == ["id", "val_left", "val_right"]
    assert len(joined_df) == 2
    assert set(joined_df["id"]) == {2, 3}


def test_left_join():
    left_df = pd.DataFrame({"id": [1, 2], "val": ["x", "y"]})
    right_df = pd.DataFrame({"id": [2], "meta": ["z"]})
    tableset = Tableset(
        [
            Table("left", left_df),
            Table("right", right_df),
        ]
    )

    model = {
        "left_table": "left",
        "right_table": "right",
        "left_on": "id",
        "right_on": "id",
        "join_type": "left",
        "output_table": "joined",
    }

    operation = JoinTables(model)
    result = operation.operate(tableset)
    joined_df = result.get_df("joined")

    assert joined_df.shape == (2, 3)
    assert pd.isna(joined_df.iloc[0]["meta"])
