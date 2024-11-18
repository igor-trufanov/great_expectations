from datetime import datetime, timezone

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.compatibility.sqlalchemy import sqltypes
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.test_utils.data_source_config import PostgreSQLDatasourceTestConfig

COL_NAME = "arbitrary_column_name"


class TestTypeSupport:
    @parameterize_batch_for_data_sources(
        data_source_configs=[PostgreSQLDatasourceTestConfig()],
        data=pd.DataFrame({COL_NAME: [True]}),
    )
    def test_bools(self, batch_for_datasource: Batch) -> None:
        expectation = gxe.ExpectColumnDistinctValuesToEqualSet(column=COL_NAME, value_set=[True])
        result = batch_for_datasource.validate(expectation)
        assert result.success

    @parameterize_batch_for_data_sources(
        data_source_configs=[PostgreSQLDatasourceTestConfig()],
        data=pd.DataFrame({COL_NAME: ["foo", "bar"]}),
    )
    def test_strings(self, batch_for_datasource: Batch) -> None:
        expectation = gxe.ExpectColumnDistinctValuesToEqualSet(
            column=COL_NAME, value_set=["foo", "bar"]
        )
        result = batch_for_datasource.validate(expectation)
        assert result.success

    @parameterize_batch_for_data_sources(
        data_source_configs=[
            PostgreSQLDatasourceTestConfig(column_types={COL_NAME: sqltypes.INT}),
            PostgreSQLDatasourceTestConfig(column_types={COL_NAME: sqltypes.NUMERIC}),
            PostgreSQLDatasourceTestConfig(column_types={COL_NAME: sqltypes.FLOAT}),
        ],
        data=pd.DataFrame({COL_NAME: [2]}),
    )
    def test_numeric_type_comparisions(self, batch_for_datasource: Batch) -> None:
        expectation = gxe.ExpectColumnMinToBeBetween(column=COL_NAME, min_value=1, max_value=3)
        result = batch_for_datasource.validate(expectation)
        assert result.success

    @parameterize_batch_for_data_sources(
        data_source_configs=[
            PostgreSQLDatasourceTestConfig(column_types={COL_NAME: sqltypes.DATE}),
            PostgreSQLDatasourceTestConfig(column_types={COL_NAME: sqltypes.TIMESTAMP}),
        ],
        data=pd.DataFrame({COL_NAME: [datetime(2024, 1, 2)]}),  # noqa: DTZ001
    )
    def test_date_type_comparision(self, batch_for_datasource: Batch) -> None:
        expectation = gxe.ExpectColumnMinToBeBetween(
            column=COL_NAME,
            min_value=datetime(2024, 1, 1),  # noqa: DTZ001
            max_value=datetime(2024, 1, 3),  # noqa: DTZ001
        )
        result = batch_for_datasource.validate(expectation)
        assert result.success

    @pytest.mark.xfail(strict=True)
    @parameterize_batch_for_data_sources(
        data_source_configs=[
            PostgreSQLDatasourceTestConfig(column_types={COL_NAME: sqltypes.DATE}),
        ],
        data=pd.DataFrame({COL_NAME: [datetime(2024, 1, 2, tzinfo=timezone.utc)]}),
    )
    def test_timezones(self, batch_for_datasource: Batch) -> None:
        expectation = gxe.ExpectColumnMinToBeBetween(
            column=COL_NAME,
            min_value=datetime(2024, 1, 1, tzinfo=timezone.utc),
            max_value=datetime(2024, 1, 3, tzinfo=timezone.utc),
        )
        result = batch_for_datasource.validate(expectation)
        assert result.success

    @pytest.mark.xfail(strict=True)
    @parameterize_batch_for_data_sources(
        data_source_configs=[
            PostgreSQLDatasourceTestConfig(column_types={COL_NAME: sqltypes.DATETIME}),
        ],
        data=pd.DataFrame({COL_NAME: [datetime(2024, 1, 2)]}),  # noqa: DTZ001
    )
    def test_datetimes(self, batch_for_datasource: Batch) -> None:
        expectation = gxe.ExpectColumnMinToBeBetween(
            column=COL_NAME,
            min_value=datetime(2024, 1, 1),  # noqa: DTZ001
            max_value=datetime(2024, 1, 3),  # noqa: DTZ001
        )
        result = batch_for_datasource.validate(expectation)
        assert result.success
