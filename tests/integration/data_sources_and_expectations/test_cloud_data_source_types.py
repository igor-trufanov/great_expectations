from typing import Generator

import pandas as pd
import pytest
from sqlalchemy.orm import MappedColumn

from great_expectations.compatibility import snowflake
from great_expectations.compatibility.sqlalchemy import sqltypes
from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.expectations import ExpectColumnSumToBeBetween
from tests.integration.test_utils.data_source_config import SnowflakeDatasourceTestConfig
from tests.integration.test_utils.data_source_config.snowflake import SnowflakeBatchTestSetup


class TestSnowflakeTypes:
    """This set of tests ensures that we can run expectations against every data
    type supported by Snowflake.

    https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
    """

    COLUMN = "col_a"

    @pytest.mark.snowflake
    @pytest.mark.parametrize(
        "data_type",
        [
            snowflake.NUMBER,
            sqltypes.NUMERIC,
            sqltypes.INT,
            sqltypes.BIGINT,
            sqltypes.SMALLINT,
            sqltypes.FLOAT,
            sqltypes.DOUBLE,
            sqltypes.DOUBLE_PRECISION,
            sqltypes.REAL,
        ],
    )
    def test_numeric(self, data_type):
        batch = self.setup_batch(
            data=pd.DataFrame({self.COLUMN: [1, 2, 3, 4]}),
            column_type=data_type,
        )
        result = batch.validate(
            expect=ExpectColumnSumToBeBetween(
                column=self.COLUMN,
                min_value=9,
                max_value=11,
            )
        )
        assert result.success

    def test_string(self): ...

    def test_boolean(self): ...

    def test_date(self): ...

    def test_semi_structured(self): ...

    def test_geospatial(self): ...

    def test_vector(self): ...

    def setup_batch(self, data: pd.DataFrame, column_type: MappedColumn) -> Batch:
        # generate batch inside a closure so we can also clean it up on test exit
        def _batch_generator(
            data: pd.DataFrame, column_type: MappedColumn
        ) -> Generator[Batch, None, None]:
            batch_setup = SnowflakeBatchTestSetup(
                config=SnowflakeDatasourceTestConfig(column_types={self.COLUMN: column_type}),
                data=data,
                extra_data={},
            )
            batch_setup.setup()

            yield batch_setup.make_batch()

            batch_setup.teardown()

        return next(_batch_generator(data=data, column_type=column_type))


class TestPostgreSQLTypes: ...


class TestDatabricksSQLTypes: ...
