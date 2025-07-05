import pandas as pd
import pandas.testing as pdt

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.explode import Explode


def test_explode_multi_column():
    df = pd.DataFrame(
        {
            "name": ["Alice", "Bob"],
            "items": [["apple", "banana"], ["pear"]],
            "prices": [[1.0, 2.0], [3.0]],
        }
    )

    tableset = Tableset([Table("orders", df)])

    model = {
        "table": "orders",
        "columns": ["items", "prices"],
    }

    op = Explode(model)
    result = op.operate(tableset)

    expected = pd.DataFrame(
        {
            "name": ["Alice", "Alice", "Bob"],
            "items": ["apple", "banana", "pear"],
            "prices": [1.0, 2.0, 3.0],
        }
    )
    expected["prices"] = expected["prices"].astype(object)

    pdt.assert_frame_equal(result.get_df("orders"), expected)
