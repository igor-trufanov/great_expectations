import datetime as dt

import pandas as pd

import great_expectations.expectations as gxe
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.test_canonical_expectations import (
    ALL_DATA_SOURCES,
)


@parameterize_batch_for_data_sources(
    data_source_configs=ALL_DATA_SOURCES,
    data=pd.DataFrame({"a": ["b", "c"]}),
)
def test_numeric_expectation_against_str_data_misconfiguration(batch_for_datasource) -> None:
    expectation = gxe.ExpectColumnStdevToBeBetween(column="a", min_value=0, max_value=1)
    result = batch_for_datasource.validate(expectation)
    assert not result.success
    assert "could not convert string to float" in str(result.exception_info)


@parameterize_batch_for_data_sources(
    data_source_configs=ALL_DATA_SOURCES,
    data=pd.DataFrame({"a": [1, 2]}),
)
def test_datetime_expectation_against_numeric_data_misconfiguration(batch_for_datasource) -> None:
    expectation = gxe.ExpectColumnMaxToBeBetween(
        column="a",
        min_value=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
        max_value=dt.datetime(2024, 1, 2, tzinfo=dt.timezone.utc),
    )
    result = batch_for_datasource.validate(expectation)
    assert not result.success
    assert "Could not parse" in str(
        result.exception_info
    ) and "into datetime representation" in str(result.exception_info)


@parameterize_batch_for_data_sources(
    data_source_configs=ALL_DATA_SOURCES,
    data=pd.DataFrame({"a": [1, 2]}),
)
def test_nonexistent_column_misconfiguration(batch_for_datasource) -> None:
    expectation = gxe.ExpectColumnMedianToBeBetween(column="b", min_value=5, max_value=10)
    result = batch_for_datasource.validate(expectation)
    assert not result.success
    assert 'The column "b" in BatchData does not exist' in str(result.exception_info)


@parameterize_batch_for_data_sources(
    data_source_configs=ALL_DATA_SOURCES,
    data=pd.DataFrame({"a": [1, 2]}),
)
def test_column_min_max_mismatch_misconfiguration(batch_for_datasource) -> None:
    expectation = gxe.ExpectColumnValuesToBeBetween(column="a", min_value=2, max_value=1)
    result = batch_for_datasource.validate(expectation)
    assert not result.success
    assert "min_value cannot be greater than max_value" in str(result.exception_info)


@parameterize_batch_for_data_sources(
    data_source_configs=ALL_DATA_SOURCES,
    data=pd.DataFrame({"a": [1, 2]}),
)
def test_column_min_max_missing_misconfiguration(batch_for_datasource) -> None:
    expectation = gxe.ExpectColumnValuesToBeBetween(column="a")
    result = batch_for_datasource.validate(expectation)
    assert not result.success
    assert "min_value and max_value cannot both be None" in str(result.exception_info)
