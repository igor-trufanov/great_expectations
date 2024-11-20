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

COL_NAME = "my_col"

A_LOT_OF_NONES = [None] * 10
DATA = pd.DataFrame({COL_NAME: [0, 1, 1, 2, 2, 2, 3, 4, 4, 4, *A_LOT_OF_NONES]}, dtype="object")


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_success_complete_results(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnMostCommonValueToBeInSet(
        column=COL_NAME, value_set=[2, 4], ties_okay=True
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {"observed_value": [2, 4]}


@parameterize_batch_for_data_sources(
    data_source_configs=ALL_DATA_SOURCES,
    data=pd.DataFrame({COL_NAME: ["foo", "bar", "bar", "baz"]}),
)
def test_strings(batch_for_datasource: Batch) -> None:
    """Ensure the median is calculated as the mean of the two middle values."""
    expectation = gxe.ExpectColumnMostCommonValueToBeInSet(
        column=COL_NAME, value_set=["bar", "something_else"], ties_okay=True
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnMostCommonValueToBeInSet(
                column=COL_NAME, value_set=[2, 4, 100], ties_okay=True
            ),
            id="value_set_has_extra_values",
        ),
        pytest.param(
            # This test case surprises me
            gxe.ExpectColumnMostCommonValueToBeInSet(
                column=COL_NAME, value_set=[2], ties_okay=True
            ),
            id="value_set_is_subset",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnMostCommonValueToBeInSet
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnMostCommonValueToBeInSet(
                column=COL_NAME, value_set=[100, 101], ties_okay=True
            ),
            id="no_matches_to_value_set",
        ),
        pytest.param(
            # This feels like it conflicts with the 2 positive cases above
            gxe.ExpectColumnMostCommonValueToBeInSet(
                column=COL_NAME, value_set=[2, 100], ties_okay=False
            ),
            id="value_set_and_observed_have_partial_overlap",
        ),
        pytest.param(
            gxe.ExpectColumnMostCommonValueToBeInSet(
                column=COL_NAME, value_set=[2, 4], ties_okay=False
            ),
            id="ties_not_okay",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnMostCommonValueToBeInSet
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success
