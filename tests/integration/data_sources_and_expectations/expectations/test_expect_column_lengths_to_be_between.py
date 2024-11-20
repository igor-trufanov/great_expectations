from unittest.mock import ANY

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.test_canonical_expectations import (
    ALL_DATA_SOURCES,
    JUST_PANDAS_DATA_SOURCES,
)

NUM_COL = "my_nums"
STRING_COL = "my_strings"

DATA = pd.DataFrame(
    {
        NUM_COL: [1, 2, 1, None],
        STRING_COL: ["one", "two", "one", None],
    }
)


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_success_complete(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValueLengthsToBeBetween(column=NUM_COL, min_value=3, max_value=3)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {
        "element_count": 4,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "partial_unexpected_list": [],
        "missing_count": 1,
        "missing_percent": 25.0,
        "unexpected_percent_total": 0.0,
        "unexpected_percent_nonmissing": 0.0,
        "partial_unexpected_counts": [],
        "partial_unexpected_index_list": [],
        "unexpected_list": [],
        "unexpected_index_list": [],
        "unexpected_index_query": ANY,
    }


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValueLengthsToBeBetween(column=STRING_COL, min_value=3, max_value=3),
            id="strings",
        ),
        pytest.param(
            gxe.ExpectColumnValueLengthsToBeBetween(column=NUM_COL, min_value=3),
            id="no_max",
        ),
        pytest.param(
            gxe.ExpectColumnValueLengthsToBeBetween(column=NUM_COL, max_value=6),
            id="no_min",
        ),
        pytest.param(
            gxe.ExpectColumnValueLengthsToBeBetween(
                column=NUM_COL, min_value=1, max_value=5, strict_min=True, strict_max=True
            ),
            id="strict_bounds",
            marks=pytest.mark.xfail(strict=True),
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnValueLengthsToBeBetween
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValueLengthsToBeBetween(column=NUM_COL, min_value=6, max_value=8),
            id="bad_range",
        ),
        pytest.param(
            gxe.ExpectColumnValueLengthsToBeBetween(column=NUM_COL, min_value=3, strict_min=True),
            id="strict_min",
        ),
        pytest.param(
            gxe.ExpectColumnValueLengthsToBeBetween(column=NUM_COL, max_value=3, strict_max=True),
            id="strict_max",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnValueLengthsToBeBetween
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success
