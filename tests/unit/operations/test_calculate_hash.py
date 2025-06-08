import hashlib

import pandas as pd
import pandas.testing as pdt
import pytest

from datarush.core.dataflow import Table, Tableset
from datarush.core.operations.transformations.calculate_hash import CalculateHash


@pytest.mark.parametrize("hash_func", ["md5", "sha1", "sha256", "blake2b"])
def test_calculate_hash(hash_func):
    df = pd.DataFrame(
        {
            "first": ["John", "Jane"],
            "last": ["Doe", "Smith"],
        }
    )
    tableset = Tableset([Table("people", df)])

    model = {
        "table": "people",
        "columns": ["first", "last"],
        "target_column": "id_hash",
        "hash_func": hash_func,
    }

    op = CalculateHash(model)
    result = op.operate(tableset)
    result_df = result.get_df("people")

    def expected_hash(row):
        combined = f"{row['first']}|{row['last']}"
        return getattr(hashlib, hash_func)(combined.encode("utf-8")).hexdigest()

    expected_df = df.copy()
    expected_df["id_hash"] = df.apply(expected_hash, axis=1)

    pdt.assert_frame_equal(result_df, expected_df)
