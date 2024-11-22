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

# Basic types test data
INTEGER_COLUMN = "integers"
INTEGER_AND_NULL_COLUMN = "integers_and_nulls"
STRING_COLUMN = "strings"
BOOLEAN_COLUMN = "booleans"
NULL_COLUMN = "nulls"

# ALL_DATA_SOURCES: Sequence[DataSourceTestConfig] = [
#     BigQueryDatasourceTestConfig(column_types=COLUMN_TYPES),
#     DatabricksDatasourceTestConfig(column_types=COLUMN_TYPES),
#     MSSQLDatasourceTestConfig(column_types=COLUMN_TYPES),
#     MySQLDatasourceTestConfig(column_types=COLUMN_TYPES),
#     PandasDataFrameDatasourceTestConfig(),
#     PandasFilesystemCsvDatasourceTestConfig(),
#     PostgreSQLDatasourceTestConfig(column_types=COLUMN_TYPES),
#     SnowflakeDatasourceTestConfig(column_types=COLUMN_TYPES),
#     SparkFilesystemCsvDatasourceTestConfig(),
#     SqliteDatasourceTestConfig(column_types=COLUMN_TYPES),
# ]

# # Create a series of nulls with float dtype
# null_series = pd.Series([None, None, None, None, None], dtype="float64")

BASIC_DATA = pd.DataFrame(
    {
        INTEGER_COLUMN: [1, 2, 3, 4, 5],
        INTEGER_AND_NULL_COLUMN: [1, 2, 3, 4, None],
        STRING_COLUMN: ["a", "b", "c", "d", "e"],
        BOOLEAN_COLUMN: [True, False, True, False, True],
        NULL_COLUMN: pd.Series([None, None, None, None, None], dtype="float64"),
    }
)


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=BASIC_DATA)
def test_success_complete_pandas(batch_for_datasource: Batch) -> None:
    type_list = ["INTEGER", "int", "int64", "int32", "IntegerType"]
    expectation = gxe.ExpectColumnValuesToBeInTypeList(column=INTEGER_COLUMN, type_list=type_list)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"]["observed_value"] in type_list  # type: ignore


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column=INTEGER_COLUMN, type_list=["int"]),
            id="integer_types",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column=INTEGER_AND_NULL_COLUMN, type_list=["int", "float64"], mostly=0.8
            ),
            id="mostly",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column=STRING_COLUMN, type_list=["str"]),
            id="string_types",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column=BOOLEAN_COLUMN, type_list=["bool"]),
            id="boolean_types",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column=NULL_COLUMN, type_list=["float"]),
            id="null_float_types",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=BASIC_DATA)
def test_success(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToBeInTypeList,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(column=INTEGER_COLUMN, type_list=["str"]),
            id="wrong_type",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeInTypeList(
                column=INTEGER_AND_NULL_COLUMN, type_list=["int"], mostly=0.9
            ),
            id="mostly_threshold_not_met",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=BASIC_DATA)
def test_failure(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToBeInTypeList,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success
