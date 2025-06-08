import pandas as pd
import pandas.testing as pdt

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.derive_column import DeriveColumn


def test_derive_column_from_jinja_template():
    df = pd.DataFrame(
        {
            "first_name": ["Alice", "Bob"],
            "last_name": ["Smith", "Jones"],
        }
    )
    tableset = Tableset([Table("people", df)])

    model = {
        "table": "people",
        "target_column": "full_name",
        "template": "{{ first_name }} {{ last_name }}",
    }

    op = DeriveColumn(model)
    result = op.operate(tableset)

    expected = df.copy()
    expected["full_name"] = ["Alice Smith", "Bob Jones"]

    pdt.assert_frame_equal(result.get_df("people"), expected)
