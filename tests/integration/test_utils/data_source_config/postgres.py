from typing import Dict, Type, Union

import pandas as pd
import pytest

from great_expectations.compatibility.sqlalchemy import POSTGRESQL_TYPES, TypeEngine
from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.test_utils.data_source_config.base import (
    BatchTestSetup,
    DataSourceTestConfig,
    SQLBatchTestSetup,
)


class PostgreSQLDatasourceTestConfig(DataSourceTestConfig):
    @property
    @override
    def label(self) -> str:
        return "postgresql"

    @property
    @override
    def pytest_mark(self) -> pytest.MarkDecorator:
        return pytest.mark.postgresql

    @override
    def create_batch_setup(
        self, data: pd.DataFrame, request: pytest.FixtureRequest
    ) -> BatchTestSetup:
        return PostgresBatchTestSetup(
            data=data,
            config=self,
        )


class PostgresBatchTestSetup(SQLBatchTestSetup[PostgreSQLDatasourceTestConfig]):
    @override
    @property
    def connection_string(self) -> str:
        return "postgresql+psycopg2://postgres@localhost:5432/test_ci"

    @override
    @property
    def schema(self) -> Union[str, None]:
        return "public"

    @override
    @property
    def inferrable_types_lookup(self) -> Dict[Type, TypeEngine]:
        return {
            str: POSTGRESQL_TYPES.VARCHAR,  # type: ignore[dict-item]
            int: POSTGRESQL_TYPES.INTEGER,  # type: ignore[dict-item]
            float: POSTGRESQL_TYPES.FLOAT,  # type: ignore[dict-item]
            bool: POSTGRESQL_TYPES.BOOLEAN,  # type: ignore[dict-item]
        }

    @override
    def make_batch(self) -> Batch:
        name = self._random_resource_name()
        return (
            self._context.data_sources.add_postgres(
                name=name, connection_string=self.connection_string
            )
            .add_table_asset(
                name=name,
                table_name=self.table_name,
                schema_name=self.schema,
            )
            .add_batch_definition_whole_table(name=name)
            .get_batch()
        )
