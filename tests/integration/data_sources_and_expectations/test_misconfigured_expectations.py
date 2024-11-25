import datetime as dt

import pandas as pd

import great_expectations.expectations as gxe
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.test_utils.data_source_config import (
    BigQueryDatasourceTestConfig,
    DatabricksDatasourceTestConfig,
    MSSQLDatasourceTestConfig,
    MySQLDatasourceTestConfig,
    PandasDataFrameDatasourceTestConfig,
    PandasFilesystemCsvDatasourceTestConfig,
    PostgreSQLDatasourceTestConfig,
    SnowflakeDatasourceTestConfig,
    SparkFilesystemCsvDatasourceTestConfig,
    SqliteDatasourceTestConfig,
)
from tests.integration.test_utils.data_source_config.base import DataSourceTestConfig

pandas_data_frame_ds_test_config = PandasDataFrameDatasourceTestConfig()
pandas_filesystem_csv_ds_test_config = PandasFilesystemCsvDatasourceTestConfig()
spark_filesystem_csv_ds_test_config = SparkFilesystemCsvDatasourceTestConfig()
big_query_ds_test_config = BigQueryDatasourceTestConfig()
databricks_ds_test_config = DatabricksDatasourceTestConfig()
mssql_ds_test_config = MSSQLDatasourceTestConfig()
mysql_ds_test_config = MySQLDatasourceTestConfig()
postgresql_ds_test_config = PostgreSQLDatasourceTestConfig()
snowflake_ds_test_config = SnowflakeDatasourceTestConfig()
sqlite_ds_test_config = SqliteDatasourceTestConfig()

PANDAS_DATA_SOURCES: list[DataSourceTestConfig] = [
    pandas_data_frame_ds_test_config,
    pandas_filesystem_csv_ds_test_config,
]


SPARK_DATA_SOURCES: list[DataSourceTestConfig] = [
    spark_filesystem_csv_ds_test_config,
]

SQL_DATA_SOURCES: list[DataSourceTestConfig] = [
    big_query_ds_test_config,
    databricks_ds_test_config,
    mssql_ds_test_config,
    mysql_ds_test_config,
    postgresql_ds_test_config,
    snowflake_ds_test_config,
    sqlite_ds_test_config,
]

ALL_DATA_SOURCES: list[DataSourceTestConfig] = (
    PANDAS_DATA_SOURCES + SPARK_DATA_SOURCES + SQL_DATA_SOURCES
)


class TestNumericExpectationAgainstStrDataMisconfiguration:
    _DATA = pd.DataFrame({"a": ["b", "c"]})
    _EXPECTATION = gxe.ExpectColumnStdevToBeBetween(
        column="a",
        min_value=0,
        max_value=1,
        strict_min=True,
        strict_max=True,
    )

    @parameterize_batch_for_data_sources(
        data_source_configs=PANDAS_DATA_SOURCES,
        data=_DATA,
    )
    def test_pandas(self, batch_for_datasource) -> None:
        self._test_misconfiguration(
            batch_for_datasource=batch_for_datasource,
            exception_message="could not convert string to float",
        )

    @parameterize_batch_for_data_sources(
        data_source_configs=[big_query_ds_test_config],
        data=_DATA,
    )
    def test_bigquery(self, batch_for_datasource) -> None:
        self._test_misconfiguration(
            batch_for_datasource=batch_for_datasource,
            exception_message="No matching signature for operator * for argument types: FLOAT64, STRING",  # noqa: E501
        )

    @parameterize_batch_for_data_sources(
        data_source_configs=[mssql_ds_test_config],
        data=_DATA,
    )
    def test_mssql(self, batch_for_datasource) -> None:
        self._test_misconfiguration(
            batch_for_datasource=batch_for_datasource,
            exception_message="[SQL Server]Error converting data type varchar to float",
        )

    @parameterize_batch_for_data_sources(
        data_source_configs=[postgresql_ds_test_config],
        data=_DATA,
    )
    def test_postgresql(self, batch_for_datasource) -> None:
        self._test_misconfiguration(
            batch_for_datasource=batch_for_datasource,
            exception_message="(psycopg2.errors.UndefinedFunction) operator does not exist: numeric * character varying",  # noqa: E501
        )

    @parameterize_batch_for_data_sources(
        data_source_configs=[
            ds
            for ds in SQL_DATA_SOURCES
            if ds
            not in {
                # Ignored due to bug (currently not failing or raising an exception)
                sqlite_ds_test_config,
                mysql_ds_test_config,
                # Tested separately due to different error messages
                big_query_ds_test_config,
                mssql_ds_test_config,
                postgresql_ds_test_config,
            }
        ],
        data=_DATA,
    )
    def test_remaining_sql(self, batch_for_datasource) -> None:
        self._test_misconfiguration(
            batch_for_datasource=batch_for_datasource,
            exception_message="could not convert string to float",
        )

    @parameterize_batch_for_data_sources(
        data_source_configs=SPARK_DATA_SOURCES,
        data=_DATA,
    )
    def test_spark(self, batch_for_datasource) -> None:
        self._test_misconfiguration(
            batch_for_datasource=batch_for_datasource,
            exception_message="could not convert string to float",
        )

    def _test_misconfiguration(self, batch_for_datasource, exception_message: str) -> None:
        result = batch_for_datasource.validate(self._EXPECTATION)
        assert not result.success
        assert exception_message in str(result.exception_info)


class TestNonExistentColumnMisconfiguration:
    _DATA = pd.DataFrame({"a": [1, 2]})
    _EXPECTATION = gxe.ExpectColumnMedianToBeBetween(column="b", min_value=5, max_value=10)

    @parameterize_batch_for_data_sources(
        data_source_configs=PANDAS_DATA_SOURCES + SQL_DATA_SOURCES,
        data=_DATA,
    )
    def test_pandas_and_sql(self, batch_for_datasource) -> None:
        self._test_misconfiguration(
            batch_for_datasource=batch_for_datasource,
            exception_message='The column "b" in BatchData does not exist',
        )

    @parameterize_batch_for_data_sources(
        data_source_configs=SPARK_DATA_SOURCES,
        data=_DATA,
    )
    def test_spark(self, batch_for_datasource) -> None:
        self._test_misconfiguration(
            batch_for_datasource=batch_for_datasource,
            exception_message="[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `b` cannot be resolved",  # noqa: E501
        )

    def _test_misconfiguration(self, batch_for_datasource, exception_message: str) -> None:
        result = batch_for_datasource.validate(self._EXPECTATION)
        assert not result.success
        assert exception_message in str(result.exception_info)


@parameterize_batch_for_data_sources(
    data_source_configs=ALL_DATA_SOURCES,
    data=pd.DataFrame({"a": [1, 2]}),
)
def test_datetime_expectation_against_numeric_data_misconfiguration(batch_for_datasource) -> None:
    expectation = gxe.ExpectColumnMaxToBeBetween(
        column="a",
        min_value=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
        max_value=dt.datetime(2024, 1, 2, tzinfo=dt.timezone.utc),
        strict_max=True,
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
