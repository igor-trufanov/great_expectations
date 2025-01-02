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

DATA_WITH_NULLS = pd.DataFrame({COL_NAME: [1, 4, None]}, dtype="object")


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA_WITH_NULLS)
def test_success_complete_results(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnMeanToBeBetween(column=COL_NAME, min_value=2, max_value=3)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {"observed_value": 2.5}


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnMeanToBeBetween(column=COL_NAME),
            id="vacuous_success",
        ),
        pytest.param(
            gxe.ExpectColumnMeanToBeBetween(column=COL_NAME, min_value=2, max_value=3),
            id="min_and_max",
        ),
        pytest.param(
            gxe.ExpectColumnMeanToBeBetween(column=COL_NAME, min_value=2),
            id="just_min",
        ),
        pytest.param(
            gxe.ExpectColumnMeanToBeBetween(column=COL_NAME, max_value=3),
            id="just_max",
        ),
        pytest.param(
            gxe.ExpectColumnMeanToBeBetween(
                column=COL_NAME, min_value=2, max_value=3, strict_min=True, strict_max=True
            ),
            id="strict_min_and_max",
        ),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA_WITH_NULLS
)
def test_success(batch_for_datasource: Batch, expectation: gxe.ExpectColumnMeanToBeBetween) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnMeanToBeBetween(column=COL_NAME, min_value=4),
            id="just_min_fail",
        ),
        pytest.param(
            gxe.ExpectColumnMeanToBeBetween(
                column=COL_NAME, min_value=3, strict_min=True, max_value=2.5
            ),
            id="strict_min_fail",
        ),
        pytest.param(
            gxe.ExpectColumnMeanToBeBetween(column=COL_NAME, max_value=2),
            id="just_max_fail",
        ),
        pytest.param(
            gxe.ExpectColumnMeanToBeBetween(
                column=COL_NAME, min_value=1, max_value=2.5, strict_max=True
            ),
            id="strict_max_fail",
        ),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA_WITH_NULLS
)
def test_failure(batch_for_datasource: Batch, expectation: gxe.ExpectColumnMeanToBeBetween) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=pd.DataFrame({COL_NAME: []})
)
def test_no_data(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnMeanToBeBetween(
        column=COL_NAME, min_value=1, max_value=3, result_format=ResultFormat.SUMMARY
    )
    result = batch_for_datasource.validate(expectation)
    assert not result.success
    assert result.to_json_dict()["result"] == {"observed_value": None}
