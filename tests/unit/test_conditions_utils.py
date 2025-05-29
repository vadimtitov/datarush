import pandas as pd
import pytest

from datarush.core.types import ConditionOperator, RowCondition, ValueType
from datarush.utils.conditions import match_conditions


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame(
        {
            "col1": [1, 2, 3, 4],
            "col2": ["apple", "banana", "cherry", "date"],
            "col3": [True, False, True, False],
        }
    )


@pytest.mark.parametrize(
    "conditions,combine,expected",
    [
        # Test EQ operator
        (
            [
                RowCondition(
                    column="col1",
                    operator=ConditionOperator.EQ,
                    value="2",
                    value_type=ValueType.INTEGER,
                )
            ],
            "and",
            [False, True, False, False],
        ),
        # Test LT operator
        (
            [
                RowCondition(
                    column="col1",
                    operator=ConditionOperator.LT,
                    value="3",
                    value_type=ValueType.INTEGER,
                )
            ],
            "and",
            [True, True, False, False],
        ),
        # Test LTE operator
        (
            [
                RowCondition(
                    column="col1",
                    operator=ConditionOperator.LTE,
                    value="3",
                    value_type=ValueType.INTEGER,
                )
            ],
            "and",
            [True, True, True, False],
        ),
        # Test GT operator
        (
            [
                RowCondition(
                    column="col1",
                    operator=ConditionOperator.GT,
                    value="2",
                    value_type=ValueType.INTEGER,
                )
            ],
            "and",
            [False, False, True, True],
        ),
        # Test GTE operator
        (
            [
                RowCondition(
                    column="col1",
                    operator=ConditionOperator.GTE,
                    value="2",
                    value_type=ValueType.INTEGER,
                )
            ],
            "and",
            [False, True, True, True],
        ),
        # Test REGEX operator
        (
            [
                RowCondition(
                    column="col2",
                    operator=ConditionOperator.REGEX,
                    value="^a",
                    value_type=ValueType.STRING,
                )
            ],
            "and",
            [True, False, False, False],
        ),
        # Test negated condition
        (
            [
                RowCondition(
                    column="col1",
                    operator=ConditionOperator.EQ,
                    value="2",
                    value_type=ValueType.INTEGER,
                    negate=True,
                )
            ],
            "and",
            [True, False, True, True],
        ),
        # Test multiple conditions with "and"
        (
            [
                RowCondition(
                    column="col1",
                    operator=ConditionOperator.GT,
                    value="1",
                    value_type=ValueType.INTEGER,
                ),
                RowCondition(
                    column="col3",
                    operator=ConditionOperator.EQ,
                    value="True",
                    value_type=ValueType.BOOLEAN,
                ),
            ],
            "and",
            [False, False, True, False],
        ),
        # Test multiple conditions with "or"
        (
            [
                RowCondition(
                    column="col1",
                    operator=ConditionOperator.LT,
                    value="2",
                    value_type=ValueType.INTEGER,
                ),
                RowCondition(
                    column="col3",
                    operator=ConditionOperator.EQ,
                    value="True",
                    value_type=ValueType.BOOLEAN,
                ),
            ],
            "or",
            [True, False, True, False],
        ),
        # Test empty conditions
        (
            [],
            "and",
            [True, True, True, True],
        ),
    ],
)
def test_match_conditions(sample_dataframe, conditions, combine, expected):
    result = match_conditions(sample_dataframe, conditions, combine)
    assert result.tolist() == expected
