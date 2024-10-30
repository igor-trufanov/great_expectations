from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Type, Union

import pytest

from great_expectations.compatibility.pydantic import BaseSettings
from great_expectations.compatibility.snowflake import SNOWFLAKE_TYPES
from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.test_utils.data_source_config.base import (
    BatchTestSetup,
    DataSourceTestConfig,
    SQLBatchTestSetup,
)

if TYPE_CHECKING:
    import pandas as pd

    from great_expectations.compatibility.sqlalchemy import TypeEngine


class SnowflakeDatasourceTestConfig(DataSourceTestConfig):
    @property
    @override
    def label(self) -> str:
        return "snowflake"

    @property
    @override
    def pytest_mark(self) -> pytest.MarkDecorator:
        return pytest.mark.snowflake

    @override
    def create_batch_setup(
        self, data: pd.DataFrame, request: pytest.FixtureRequest
    ) -> BatchTestSetup:
        return SnowflakeBatchTestSetup(
            data=data,
            config=self,
        )


class SnowflakeConnectionConfig(BaseSettings):
    """This class retrieves these values from the environment.
    If you're testing locally, you can use your Snowflake creds
    and test against your own Snowflake account.
    """

    SNOWFLAKE_USER: str
    SNOWFLAKE_PW: str
    SNOWFLAKE_ACCOUNT: str
    SNOWFLAKE_DATABASE: str
    SNOWFLAKE_SCHEMA: str
    SNOWFLAKE_WAREHOUSE: str
    SNOWFLAKE_ROLE: str = "PUBLIC"

    @property
    def connection_string(self) -> str:
        return (
            f"snowflake://{self.SNOWFLAKE_USER}:{self.SNOWFLAKE_PW}"
            f"@{self.SNOWFLAKE_ACCOUNT}/{self.SNOWFLAKE_DATABASE}/{self.SNOWFLAKE_SCHEMA}"
            f"?warehouse={self.SNOWFLAKE_WAREHOUSE}&role={self.SNOWFLAKE_ROLE}"
        )


class SnowflakeBatchTestSetup(SQLBatchTestSetup[SnowflakeDatasourceTestConfig]):
    snowflake_config = SnowflakeConnectionConfig()  # type: ignore [call-arg]

    @override
    @property
    def connection_string(self) -> str:
        return self.snowflake_config.connection_string

    @override
    @property
    def schema(self) -> Union[str, None]:
        return self.snowflake_config.SNOWFLAKE_SCHEMA

    @override
    @property
    def inferrable_types_lookup(self) -> Dict[Type, TypeEngine]:
        return {
            str: SNOWFLAKE_TYPES.STRING,  # type: ignore[dict-item]
            int: SNOWFLAKE_TYPES.NUMBER,  # type: ignore[dict-item]
            float: SNOWFLAKE_TYPES.DEC,  # type: ignore[dict-item]
        }

    @override
    def make_batch(self) -> Batch:
        name = self._random_resource_name()
        return (
            self._context.data_sources.add_snowflake(
                name=name,
                account=self.snowflake_config.SNOWFLAKE_ACCOUNT,
                user=self.snowflake_config.SNOWFLAKE_USER,
                password=self.snowflake_config.SNOWFLAKE_PW,
                database=self.snowflake_config.SNOWFLAKE_DATABASE,
                schema=self.snowflake_config.SNOWFLAKE_SCHEMA,
                warehouse=self.snowflake_config.SNOWFLAKE_WAREHOUSE,
                role=self.snowflake_config.SNOWFLAKE_ROLE,
            )
            .add_table_asset(
                name=name,
                table_name=self.table_name,
            )
            .add_batch_definition_whole_table(name=name)
            .get_batch()
        )
